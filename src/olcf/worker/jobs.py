# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Job supervision (CONTRACTS sections 4.2, 4.3, 6; spec section 33).

Lifecycle of ``run_job``::

    validate -> idempotency (job_id, attempt) -> admission (concurrency, cores, memory) -> runtime light-check (jar hashes)
    -> workdir <state>/work/<job_id>/ (0700) -> write source (+sha256 check) + job.json (0600) + job.public.json (redacted)
    -> spawn <runtime>/venv/bin/python -m olcf.runtime_lib.runner job.json   (own process group, scrubbed env, stdout/stderr to files)
    -> supervise: log shipping, events, timeout, cancel (SIGTERM -> grace -> SIGKILL), exit classification
    -> final job_state (+ inline artifacts), secrets removed, workdir kept for ``job_retention_hours``.

Everything the agent needs to *adopt* a running job after an agent restart is persisted in ``agent-job.json`` (pid + create time,
deadline, cancel flags) and ``logship.json`` (confirmed log offsets); jobs are children of their own session so they survive
the agent. Terminal results that were not confirmed delivered are re-reported in every ``hello`` (``active_jobs``) and as
``job_state`` frames until the connection that carried them proved stable (or the controller acks with ``job_ack``).
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import re
import shutil
import signal
import subprocess
import threading
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Protocol

import psutil

from olcf.core.hostruntime import HOST_RUNTIME_ID
from olcf.core.redaction import redact_obj, redact_text
from olcf.core.timeutil import now_iso
from olcf.runtime.errors import RuntimeManagerError
from olcf.runtime.manager import RuntimeHandle, RuntimeManager
from olcf.worker import logship, procutil, sandbox
from olcf.worker.config import AgentConfig
from olcf.worker.layers import LayerManager
from olcf.worker.protocol import CommandError

log = logging.getLogger("olcf.worker.jobs")

INLINE_PER_ARTIFACT = 2 * 1024 * 1024
INLINE_TOTAL = 3 * 1024 * 1024
MAX_STATE_FRAME = 6 * 1024 * 1024
TERMINAL = ("succeeded", "failed", "cancelled", "timed_out")
SENSITIVE_FILES = ("job.json", "kernel-spec.json", "kernel-spec.connection.json")
DENY_FETCH = set(SENSITIVE_FILES) | {"agent-job.json", "logship.json"}
_ENV_ALLOW = ("LANG", "LC_ALL", "LC_CTYPE", "TZ", "SYSTEMROOT", "WINDIR", "COMSPEC", "PATHEXT", "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy",
              "https_proxy", "no_proxy", "SSL_CERT_FILE", "SSL_CERT_DIR", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE", "NODE_EXTRA_CA_CERTS")


class Outbound(Protocol):
    """What the managers need from the agent's connection (implemented by :class:`olcf.worker.agent.Agent`)."""

    def emit(self, ftype: str, payload: dict[str, Any], *, buffer: bool = False, ack: Callable[[bool], None] | None = None) -> bool:
        """Queue a frame. ``buffer=True`` keeps it across a disconnect; otherwise it is dropped (returns False) while offline.
        ``ack(ok)`` is called when the controller acknowledged the frame (``result{ref}`` -> True, ``error{ref}`` -> False)."""
        ...
    def connected(self) -> bool: ...
    def conn_gen(self) -> int: ...
    def outq_depth(self) -> int: ...


def parse_memory_mb(value: Any, default: int = 1024) -> int:
    """``"512m"`` / ``"2g"`` / ``"1024"`` (Spark sizes) -> MB."""
    if value is None:
        return default
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([kKmMgGtT]?)[bB]?\s*", str(value))
    if not m:
        return default
    n, unit = float(m.group(1)), m.group(2).lower()
    return max(1, int(n * {"k": 1 / 1024, "m": 1, "g": 1024, "t": 1024 * 1024, "": 1}[unit]))


@dataclass
class JobRecord:
    """Persisted job state (``agent-job.json``)."""

    job_id: str
    attempt: int
    kind: str
    name: str
    runtime_id: str
    workdir: str
    state: str = "starting"
    pid: int | None = None
    pid_create_time: float | None = None
    started_at: str | None = None
    finished_at: str | None = None
    exit_code: int | None = None
    deadline: float | None = None
    timeout_seconds: float | None = None
    cancel_requested: bool = False
    cancel_reason: str | None = None
    cancel_at: float | None = None
    cancel_grace: float = 10.0
    resources: dict[str, int] = field(default_factory=lambda: {"cores": 1, "memory_mb": 1024})
    result: dict[str, Any] | None = None
    failure: dict[str, Any] | None = None
    sent_gen: int | None = None  # connection generation that carried the terminal job_state
    sent_at: float | None = None
    delivered: bool = False
    events: list[dict[str, Any]] = field(default_factory=list)  # recent events (bounded) for re-send after reconnect
    inline_artifacts: bool = True

    @property
    def terminal(self) -> bool:
        return self.state in TERMINAL

    def brief(self) -> dict[str, Any]:
        """Reconcile entry (hello / heartbeat ``active_jobs``): small on purpose (the full result travels in ``job_state``)."""
        d: dict[str, Any] = {"job_id": self.job_id, "attempt": self.attempt, "pid": self.pid, "state": self.state, "started_at": self.started_at, "runtime_id": self.runtime_id}
        if self.terminal:
            d.update({"exit_code": self.exit_code, "finished_at": self.finished_at, "undelivered": not self.delivered})
        return d


def _atomic_json(path: Path, doc: Any, *, mode: int = 0o600) -> None:
    tmp = path.with_name(path.name + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, default=str)
    os.replace(tmp, path)


def shred(path: Path) -> None:
    """Overwrite then remove a file holding secrets (best effort; journaling filesystems may keep old blocks)."""
    try:
        size = path.stat().st_size
        with open(path, "r+b") as fh:
            fh.write(b"\0" * size)
            fh.flush()
            os.fsync(fh.fileno())
    except OSError:
        pass
    try:
        path.unlink()
    except OSError:
        pass


def safe_join(base: Path, rel: str) -> Path:
    """``base/rel`` refusing absolute paths and traversal (all file paths derived from payloads go through this)."""
    if not rel or os.path.isabs(rel) or ".." in Path(rel).parts or "\\" in rel and os.name != "nt":
        raise CommandError("bad_path", f"unsafe path {rel!r}", remediation="Paths must be relative and stay inside the job directory.")
    p = (base / rel).resolve()
    if not str(p).startswith(str(base.resolve()) + os.sep):
        raise CommandError("bad_path", f"path {rel!r} escapes the job directory")
    return p


class JobManager:
    """Owns every job process on this node."""

    def __init__(self, cfg: AgentConfig, runtimes: RuntimeManager, out: Outbound, *, extra_allocated=lambda: (0, 0), capacity=lambda: (1 << 30, 1 << 30),  # noqa: ANN001
                 layers: LayerManager | None = None) -> None:
        self.cfg, self.runtimes, self.out = cfg, runtimes, out
        self.layers = layers or LayerManager(cfg)
        self.jobs: dict[tuple[str, int], JobRecord] = {}
        self._lock = threading.RLock()
        self._threads: dict[tuple[str, int], threading.Thread] = {}
        self._shippers: dict[tuple[str, int], dict[str, logship.StreamShipper]] = {}
        self.draining = False
        self._extra_allocated = extra_allocated
        self._capacity = capacity
        self.work = cfg.path("work")
        self._stop = threading.Event()

    # ------------------------------------------------------------------ accounting
    def allocated(self) -> tuple[int, int]:
        with self._lock:
            act = [r for r in self.jobs.values() if not r.terminal]
        return sum(r.resources["cores"] for r in act), sum(r.resources["memory_mb"] for r in act)

    def active(self) -> list[JobRecord]:
        with self._lock:
            return [r for r in self.jobs.values() if not r.terminal]

    def snapshot(self, *, include_undelivered: bool = False) -> list[dict[str, Any]]:
        """Running jobs; with ``include_undelivered`` also terminal ones whose final ``job_state`` is not confirmed (``hello`` only: the
        terminal state follows immediately as a ``job_state`` frame, so the controller never mistakes them for running)."""
        with self._lock:
            return [r.brief() for r in self.jobs.values() if not r.terminal or (include_undelivered and not r.delivered)]

    # ------------------------------------------------------------------ run_job
    def run_job(self, payload: dict[str, Any]) -> dict[str, Any]:
        spec = payload.get("spec")
        if not isinstance(spec, dict):
            raise CommandError("bad_request", "run_job needs payload.spec (the job spec)")
        job_id = str(spec.get("job_id") or "")
        if not re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", job_id):
            raise CommandError("bad_request", f"invalid job_id {job_id!r}")
        attempt = int(spec.get("attempt") or 1)
        key = (job_id, attempt)
        with self._lock:
            existing = self.jobs.get(key)
            if existing is not None:
                return {"duplicate": True, "job_id": job_id, "attempt": attempt, "state": existing.state, "pid": existing.pid}
            other = [r for r in self.jobs.values() if r.job_id == job_id and not r.terminal]
            if other:
                raise CommandError("conflict", f"job {job_id} attempt {other[0].attempt} is still active on this node",
                                   remediation="Cancel the previous attempt (confirm the process is gone) before re-running.")
            if self.draining:
                raise CommandError("draining", "this node is draining and accepts no new jobs", remediation="Place the job on another node or undrain this one.")
            kind = str(spec.get("kind") or "notebook")
            res = self._requested_resources(payload, spec)
            self._admit(res)
            rt_id = str((spec.get("runtime") or {}).get("id") or payload.get("runtime_id") or "")
            if not rt_id:
                raise CommandError("bad_request", "job spec has no runtime.id")
            try:
                handle = self.runtimes.light_check(rt_id)
            except RuntimeManagerError as exc:
                raise CommandError("runtime_not_ready", f"runtime {rt_id} cannot be used on this node: {exc.message}", remediation=exc.remediation) from exc
            if rt_id == HOST_RUNTIME_ID:
                # the fingerprint of the interpreter that really runs the job (the controller only knows the catalog entry): result.json / the job record carry this one
                spec.setdefault("runtime", {}).update(id=rt_id, family=handle.family, fingerprint=handle.fingerprint, host_python=handle.summary())
            workdir = self.work / job_id
            if workdir.exists():
                self._clear_old_workdir(workdir, job_id)
            rec = JobRecord(job_id=job_id, attempt=attempt, kind=kind, name=str(spec.get("name") or job_id), runtime_id=rt_id, workdir=str(workdir), resources=res)
            self.jobs[key] = rec  # reserve the idempotency key + the resources before doing slow I/O
        try:
            # the environment's dependency layer (pip --target, hash-pinned) and jars; cached per node and dependency hash, so repeat jobs start immediately
            self.layers.apply(spec, handle.python)
            self._prepare_workdir(rec, spec, payload)
            proc = self._spawn(rec, spec, handle)
        except BaseException:
            with self._lock:
                self.jobs.pop(key, None)
            shutil.rmtree(workdir, ignore_errors=True)
            raise
        t = threading.Thread(target=self._supervise, args=(rec, proc, spec), name=f"job-{job_id}", daemon=True)
        with self._lock:
            self._threads[key] = t
        t.start()
        return {"job_id": job_id, "attempt": attempt, "state": "running", "pid": rec.pid, "workdir": str(workdir)}

    def _requested_resources(self, payload: dict[str, Any], spec: dict[str, Any]) -> dict[str, int]:
        r = payload.get("resources") or {}
        conf = (spec.get("spark") or {}).get("conf") or {}
        cores = int(r.get("cores") or conf.get("spark.driver.cores") or 1)
        mem = int(r.get("memory_mb") or parse_memory_mb(conf.get("spark.driver.memory"), 1024))
        return {"cores": max(1, cores), "memory_mb": max(64, mem)}

    def _admit(self, res: dict[str, int]) -> None:
        """Safety net behind the controller's scheduler: reject unsafe oversubscription (spec section 13)."""
        act = self.active()
        max_jobs = self.cfg.max_jobs or max(1, (psutil_cores()))
        if len(act) >= max_jobs:
            raise CommandError("resource_exhausted", f"node is at its concurrent job limit ({max_jobs})", remediation="Wait for running jobs to finish or raise --max-jobs.")
        cap_cores, cap_mem = self._capacity()
        x_cores, x_mem = self._extra_allocated()
        used_cores = sum(r.resources["cores"] for r in act) + x_cores
        used_mem = sum(r.resources["memory_mb"] for r in act) + x_mem
        if used_cores + res["cores"] > cap_cores or used_mem + res["memory_mb"] > cap_mem:
            raise CommandError("resource_exhausted", f"not enough free resources: need {res['cores']} cores/{res['memory_mb']} MB, "
                               f"free {max(0, cap_cores - used_cores)} cores/{max(0, cap_mem - used_mem)} MB",
                               details={"requested": res, "free": {"cores": cap_cores - used_cores, "memory_mb": cap_mem - used_mem}},
                               remediation="Let the scheduler place the job elsewhere, or lower the driver resources.")

    def _clear_old_workdir(self, workdir: Path, job_id: str) -> None:
        rec_file = workdir / "agent-job.json"
        try:
            prev = json.loads(rec_file.read_text())
            if prev.get("state") not in TERMINAL:
                raise CommandError("conflict", f"job directory of {job_id} belongs to a still-running attempt")
        except (OSError, ValueError):
            pass
        shutil.rmtree(workdir, ignore_errors=True)

    # ------------------------------------------------------------------ prepare + spawn
    def _prepare_workdir(self, rec: JobRecord, spec: dict[str, Any], payload: dict[str, Any]) -> None:
        wd = Path(rec.workdir)
        for sub in ("", "source", "tmp", "home", "spark-local"):
            (wd / sub).mkdir(parents=True, exist_ok=True)
            try:
                os.chmod(wd / sub, 0o700)
            except OSError:
                pass
        src = spec.get("source") or {}
        content = None
        if isinstance(payload.get("source"), dict):
            content = payload["source"]
        elif payload.get("source_content") is not None:
            content = {"content": payload["source_content"], "encoding": payload.get("source_encoding", "utf-8")}
        elif src.get("content") is not None:
            content = {"content": src["content"], "encoding": src.get("encoding", "utf-8")}
        rel = src.get("path")
        if content is not None:
            if not rel:
                rel = content.get("path") or "source/" + (src.get("filename") or {"ipynb": "notebook.ipynb", "py": "main.py", "fabric_git_source": "notebook-content.py", "sql": "query.sql", "jar": "app.jar"}.get(str(src.get("format")), "source.txt"))
                spec.setdefault("source", {})["path"] = rel
            data = _decode_content(content)
            digest = hashlib.sha256(data).hexdigest()
            expect = [str(x).lower() for x in (src.get("sha256"), content.get("sha256")) if x]
            if any(digest != x for x in expect):
                raise CommandError("source_hash_mismatch", "source content does not match spec.source.sha256", remediation="The capsule may have been altered in transit; resubmit.")
            target = safe_join(wd, rel)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
        for f in payload.get("files") or []:
            target = safe_join(wd, str(f["path"]))
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(_decode_content(f))
        # job.json: all of it is sensitive (storage_conf, secrets, callbacks) -> 0600, deleted at job end
        _atomic_json(wd / "job.json", spec, mode=0o600)
        pub = redact_obj({k: v for k, v in spec.items() if k not in ("secrets", "storage_conf", "callbacks")})
        pub["has_secrets"] = bool(spec.get("secrets"))
        _atomic_json(wd / "job.public.json", pub, mode=0o640)
        limits = spec.get("limits") or {}
        if limits.get("timeout_seconds"):
            rec.timeout_seconds = float(limits["timeout_seconds"])

    def _job_env(self, rec: JobRecord, handle: RuntimeHandle) -> dict[str, str]:
        return scrubbed_env(self.cfg, handle, Path(rec.workdir), {"OLCF_JOB_ID": rec.job_id, "OLCF_ATTEMPT": str(rec.attempt)})

    def _spawn(self, rec: JobRecord, spec: dict[str, Any], handle: RuntimeHandle) -> subprocess.Popen[bytes]:
        wd = Path(rec.workdir)
        cmd = handle.module_argv("olcf.runtime_lib.runner", str(wd / "job.json"))
        cmd = sandbox.wrap_command(cmd, self.cfg.sandbox_command, workdir=wd, runtime_dir=handle.root, state_dir=self.cfg.state_dir, home=wd / "home")
        env = self._job_env(rec, handle)
        out = open(wd / "stdout.log", "ab")  # noqa: SIM115 - closed right after spawn (child keeps its own copy)
        err = open(wd / "stderr.log", "ab")  # noqa: SIM115
        try:
            proc = subprocess.Popen(cmd, cwd=wd, env=env, stdin=subprocess.DEVNULL, stdout=out, stderr=err, **procutil.popen_kwargs())  # noqa: S603
        except OSError as exc:
            raise CommandError("spawn_failed", f"cannot start the job runner: {exc}", remediation="Check the runtime installation (verify/repair) and the sandbox command.") from exc
        finally:
            out.close()
            err.close()
        rec.pid = proc.pid
        try:
            rec.pid_create_time = psutil.Process(proc.pid).create_time()
        except psutil.Error:
            rec.pid_create_time = None
        rec.state = "running"
        rec.started_at = now_iso()
        if rec.timeout_seconds:
            rec.deadline = time.time() + rec.timeout_seconds + self.cfg.timeout_slack_seconds
        self._persist(rec)
        procutil.write_pidfile(wd / "job.pid", proc.pid, job_id=rec.job_id)
        self.out.emit("job_state", {"job_id": rec.job_id, "attempt": rec.attempt, "state": "running", "pid": rec.pid, "started_at": rec.started_at, "runtime_id": rec.runtime_id},
                      buffer=True)
        return proc

    def _persist(self, rec: JobRecord) -> None:
        try:
            _atomic_json(Path(rec.workdir) / "agent-job.json", asdict(rec), mode=0o600)
        except OSError:
            log.exception("cannot persist state of %s", rec.job_id)

    # ------------------------------------------------------------------ supervision
    def _secret_values(self, rec: JobRecord) -> list[str]:
        try:
            spec = json.loads((Path(rec.workdir) / "job.json").read_text())
        except (OSError, ValueError):
            return []
        vals = [str(v) for v in (spec.get("secrets") or {}).values()]
        vals += [str(v) for v in ((spec.get("storage_conf") or {}).get("env") or {}).values()]
        vals += [str(v) for k, v in ((spec.get("storage_conf") or {}).get("spark") or {}).items() if re.search(r"(?i)(secret|key|token|password|sas)", str(k))]
        cb = (spec.get("callbacks") or {}).get("job_token")
        return vals + ([str(cb)] if cb else [])

    def _make_shippers(self, rec: JobRecord) -> dict[str, logship.StreamShipper]:
        wd = Path(rec.workdir)
        saved = logship.load_ship_state(wd / "logship.json")
        secrets = self._secret_values(rec)
        spec_limits = {}
        try:
            spec_limits = json.loads((wd / "job.public.json").read_text()).get("limits") or {}
        except (OSError, ValueError):
            pass
        cap = int(spec_limits.get("max_output_bytes") or 50 * 1024 * 1024)

        def on_event(ev: dict[str, Any], off: int) -> None:
            self._on_event(rec, ev, off)

        out: dict[str, logship.StreamShipper] = {}
        for stream in ("stdout", "stderr"):
            st = saved.get(stream, {})
            out[stream] = logship.StreamShipper(rec.job_id, rec.attempt, stream, wd / f"{stream}.log", chunk_bytes=self.cfg.log_chunk_bytes, start_raw=int(st.get("raw", 0)),
                                                start_seq=int(st.get("seq", 0)), secret_values=secrets, max_bytes=cap, implicit_ack_seconds=self.cfg.implicit_log_ack_seconds,
                                                on_event=on_event if stream == "stdout" else None)
            out[stream].shipped_bytes = int(st.get("shipped_bytes", 0))
        return out

    def _on_event(self, rec: JobRecord, ev: dict[str, Any], offset: int) -> None:
        name = str(ev.pop("event", "event"))
        payload = {"job_id": rec.job_id, "attempt": rec.attempt, "event": name, "data": redact_obj(ev), "seq": offset}
        with self._lock:
            rec.events.append(payload)
            del rec.events[:-200]
        self.out.emit("job_event", payload, buffer=True)

    def _send_log(self, frame: dict[str, Any]) -> bool:
        if self.out.outq_depth() >= self.cfg.log_buffer_frames:
            return False
        job_id, stream, seq = frame["job_id"], frame["stream"], frame["seq"]

        def acked(ok: bool) -> None:  # the controller stored the chunk (or rejected it for good: either way do not resend)
            self.ack_log(job_id, stream, seq)

        return self.out.emit("job_log", frame, ack=acked)

    def _pump_all(self, rec: JobRecord, shippers: dict[str, logship.StreamShipper], *, finished: bool) -> bool:
        progressed = False
        gen = self.out.conn_gen()
        connected = self.out.connected()
        for sh in shippers.values():
            if connected:
                sh.on_reconnect(gen)
            while sh.pump(self._send_log, connected=connected, finished=finished):
                progressed = True
                if self.out.outq_depth() >= self.cfg.log_buffer_frames:
                    break
        return progressed

    def _supervise(self, rec: JobRecord, proc: subprocess.Popen[bytes] | None, spec: dict[str, Any] | None = None) -> None:
        key = (rec.job_id, rec.attempt)
        wd = Path(rec.workdir)
        shippers = self._make_shippers(rec)
        with self._lock:
            self._shippers[key] = shippers
        last_save = 0.0
        prev_gen = -1
        try:
            while not self._stop.is_set():
                rc = self._poll_exit(rec, proc)
                self._pump_all(rec, shippers, finished=rc is not None)
                gen = self.out.conn_gen()
                if gen != prev_gen and self.out.connected():
                    prev_gen = gen
                    self._resend_events(rec)
                if time.monotonic() - last_save > 1.0:
                    logship.save_ship_state(wd / "logship.json", shippers)
                    last_save = time.monotonic()
                if rc is not None:
                    break
                self._enforce(rec)
                time.sleep(0.1 if any(s.backlog() for s in shippers.values()) else 0.25)
            if self._stop.is_set() and self._poll_exit(rec, proc) is None:
                return  # agent shutting down with the job still running: it is adopted on the next start
            rc = self._poll_exit(rec, proc)
            if rc is not None:
                # We just watched the runner exit: reap whatever it left behind in its process group (e.g. a Spark driver JVM after
                # the runner was OOM-killed or had to be stopped hard). Never done for a pid we did not see alive (pid reuse).
                left = procutil.kill_group_remnants(rec.pid)
                if left:
                    log.warning("job %s: killed %d leftover process(es) of its process group", rec.job_id, left)
            quiet_until = time.monotonic() + 3.0
            while time.monotonic() < quiet_until and not self._stop.is_set():
                if self._pump_all(rec, shippers, finished=True):
                    quiet_until = time.monotonic() + 1.0
                elif all(s.backlog() == 0 for s in shippers.values()):
                    break
                else:
                    time.sleep(0.1)
            self._finalise(rec, rc if rc is not None else -1)
            logship.save_ship_state(wd / "logship.json", shippers)
            # keep confirming/resending logs until everything is acknowledged (or ~60 s after the end)
            end = time.monotonic() + 60
            while time.monotonic() < end and not self._stop.is_set():
                self._pump_all(rec, shippers, finished=True)
                if all(s.drained for s in shippers.values()):
                    break
                time.sleep(0.25)
            if all(s.drained for s in shippers.values()):
                for shp in shippers.values():  # everything is delivered: mask secrets in the raw spool too (defence in depth)
                    logship.redact_file(shp.path, shp.secrets)
                    shp.raw_pos = shp.path.stat().st_size if shp.path.exists() else 0
            logship.save_ship_state(wd / "logship.json", shippers)
        except BaseException:  # noqa: BLE001 - the supervisor must never die silently
            log.exception("supervisor of job %s crashed", rec.job_id)
            if not rec.terminal:
                self._finalise(rec, -1, crash=True)
        finally:
            with self._lock:
                self._shippers.pop(key, None)

    def _poll_exit(self, rec: JobRecord, proc: subprocess.Popen[bytes] | None) -> int | None:
        if proc is not None:
            return proc.poll()
        # adopted process (not our child): exit code is unknown, ``result.json`` carries the outcome
        if procutil.process_matches(rec.pid, rec.pid_create_time) is None:
            return -1000
        return None

    def _enforce(self, rec: JobRecord) -> None:
        now = time.time()
        if not rec.cancel_requested and rec.deadline and now > rec.deadline:
            log.warning("job %s exceeded its timeout (%ss)", rec.job_id, rec.timeout_seconds)
            self.cancel_job(rec.job_id, rec.cancel_grace, reason="timeout", attempt=rec.attempt)
        if rec.cancel_requested and rec.cancel_at is not None and now > rec.cancel_at + rec.cancel_grace and rec.pid:
            log.warning("job %s ignored SIGTERM for %ss: SIGKILL", rec.job_id, rec.cancel_grace)
            rec.cancel_at = None  # escalate once
            procutil.signal_group(rec.pid, getattr(signal, "SIGKILL", signal.SIGTERM))
            self._persist(rec)

    def cancel_job(self, job_id: str, grace_seconds: float | None = None, *, reason: str = "cancelled", attempt: int | None = None, force: bool = False) -> dict[str, Any]:
        """Idempotent: SIGTERM the process group now, SIGKILL after ``grace_seconds`` (default ``kill_grace_seconds``)."""
        with self._lock:
            cands = [r for (j, a), r in self.jobs.items() if j == job_id and (attempt is None or a == attempt)]
        if not cands:
            return {"job_id": job_id, "found": False, "state": "unknown"}
        rec = max(cands, key=lambda r: r.attempt)
        if rec.terminal:
            rec.delivered = rec.delivered or reason == "cancelled"  # the controller knows it is over: stop re-reporting
            return {"job_id": job_id, "found": True, "state": rec.state}
        grace = float(grace_seconds) if grace_seconds is not None else self.cfg.kill_grace_seconds
        if not rec.cancel_requested:
            rec.cancel_requested, rec.cancel_reason, rec.cancel_at, rec.cancel_grace = True, reason, time.time(), grace
            self._persist(rec)
            if rec.pid:
                procutil.signal_group(rec.pid, getattr(signal, "SIGKILL", signal.SIGTERM) if force else signal.SIGTERM)
        elif force and rec.pid:
            procutil.signal_group(rec.pid, getattr(signal, "SIGKILL", signal.SIGTERM))
        return {"job_id": job_id, "found": True, "state": "cancelling", "reason": rec.cancel_reason}

    # ------------------------------------------------------------------ finalisation
    def _read_result(self, rec: JobRecord) -> dict[str, Any] | None:
        try:
            return json.loads((Path(rec.workdir) / "result.json").read_text())
        except (OSError, ValueError):
            return None

    def _tail(self, path: Path, n: int = 4000) -> str:
        try:
            with open(path, "rb") as fh:
                fh.seek(0, os.SEEK_END)
                fh.seek(max(0, fh.tell() - n))
                return redact_text(fh.read().decode("utf-8", "replace"))
        except OSError:
            return ""

    def classify_exit(self, rec: JobRecord, rc: int, result: dict[str, Any] | None) -> tuple[str, dict[str, Any] | None]:
        """Decide the terminal state + failure record from exit code, result.json and the cancel/timeout flags."""
        failure = (result or {}).get("failure")
        cat = (failure or {}).get("category")
        if result and result.get("state") == "succeeded" and rc in (0, -1000):
            return "succeeded", None

        def reported(base: dict[str, Any]) -> dict[str, Any]:
            """The agent's own verdict (timeout / cancel) wins over whatever error the dying runner produced while being stopped
            (typically a Py4J error from the JVM going away); the runner's message is kept as ``details`` for debugging."""
            if failure and failure.get("category") == base["category"]:
                return failure
            if failure:
                base["details"] = {"runner_failure": {k: failure.get(k) for k in ("category", "message")}}
            return base

        if rec.cancel_reason == "timeout" or cat == "timeout" or rc == 124:
            return "timed_out", reported({"category": "timeout", "message": f"job exceeded its timeout of {rec.timeout_seconds}s", "remediation": "Increase limits.timeout_seconds or optimise the job."})
        if rec.cancel_requested or cat == "cancellation" or rc in (143, -signal.SIGTERM):
            return "cancelled", reported({"category": "cancellation", "message": "job cancelled by the controller"})
        if failure:
            return "failed", failure
        wd = Path(rec.workdir)
        tail = self._tail(wd / "stderr.log")
        if rc == -getattr(signal, "SIGKILL", 9) or rc == -9:  # Windows has no signal.SIGKILL
            return "failed", {"category": "resource", "message": "the job process was killed by SIGKILL (most likely the kernel OOM killer)", "remediation": "Reduce driver memory/data volume or give the node more RAM.", "details": {"stderr_tail": tail}}
        msg = f"runner exited with status {rc} without writing a result" if rc != -1000 else "the job process disappeared (agent restart) without writing a result"
        return "failed", {"category": "internal", "message": msg, "remediation": "See the job's stderr.log; report a bug if this is unexpected.", "details": {"stderr_tail": tail}}

    def _artifacts(self, rec: JobRecord, result: dict[str, Any] | None) -> list[dict[str, Any]]:
        """Describe the job's artifacts; small ones travel inline (base64) in the final ``job_state``.

        The control channel limit for one agent frame is 8 MiB, so at most ``INLINE_TOTAL`` raw bytes (2 MiB per artifact) are inlined;
        the rest carry ``truncated: true`` + size + sha256 and can be read with the ``fetch_artifact`` command."""
        out: list[dict[str, Any]] = []
        wd = Path(rec.workdir)
        budget = INLINE_TOTAL
        for a in (result or {}).get("artifacts") or []:
            try:
                p = safe_join(wd, str(a["path"]))
            except (CommandError, KeyError):
                continue
            if not p.is_file() or p.name in DENY_FETCH:
                continue
            size = p.stat().st_size
            entry: dict[str, Any] = {"name": a.get("name", p.name), "path": a["path"], "media_type": a.get("media_type", "application/octet-stream"), "size": size}
            h = hashlib.sha256()
            with open(p, "rb") as fh:
                for block in iter(lambda: fh.read(1 << 20), b""):
                    h.update(block)
            entry["sha256"] = h.hexdigest()
            if size <= INLINE_PER_ARTIFACT and size <= budget:
                entry["data_b64"] = base64.b64encode(p.read_bytes()).decode("ascii")
                budget -= size
            else:
                entry["truncated"] = True
            out.append(entry)
        return out

    def _finalise(self, rec: JobRecord, rc: int, *, crash: bool = False) -> None:
        result = self._read_result(rec)
        state, failure = self.classify_exit(rec, rc, result)
        if crash:
            state, failure = "failed", {"category": "internal", "message": "agent supervisor crashed while supervising the job"}
        rec.exit_code = None if rc in (-1000, -1) else rc
        rec.finished_at = now_iso()
        rec.failure = failure
        rec.result = {k: v for k, v in (result or {}).items() if k != "failure"} or None
        art = self._artifacts(rec, result) if rec.inline_artifacts else []
        if rec.result is not None:
            rec.result["artifacts"] = art or rec.result.get("artifacts", [])
        rec.state = state
        # remove secrets from disk first, then report (a crash between the two never leaves secrets behind)
        self._scrub(rec)
        self._persist(rec)
        self._send_terminal(rec)
        log.info("job %s attempt %d finished: %s", rec.job_id, rec.attempt, state)

    def _scrub(self, rec: JobRecord) -> None:
        wd = Path(rec.workdir)
        for name in SENSITIVE_FILES:
            shred(wd / name)
        shutil.rmtree(wd / "pip-layer", ignore_errors=True)
        shutil.rmtree(wd / "spark-local", ignore_errors=True)
        shutil.rmtree(wd / "tmp", ignore_errors=True)
        (wd / "job.pid").unlink(missing_ok=True)

    def _terminal_payload(self, rec: JobRecord) -> dict[str, Any]:
        d = {"job_id": rec.job_id, "attempt": rec.attempt, "state": rec.state, "exit_code": rec.exit_code, "result": rec.result, "failure": rec.failure,
             "finished_at": rec.finished_at, "started_at": rec.started_at, "runtime_id": rec.runtime_id}
        if rec.result and rec.result.get("spark"):
            d["spark"] = rec.result["spark"]
        # hard guard for the controller's 8 MiB frame limit: shed inline artifact data first, then an oversized exit value
        if len(json.dumps(d, default=str)) > MAX_STATE_FRAME and d.get("result"):
            res = dict(d["result"])
            res["artifacts"] = [{k: v for k, v in a.items() if k != "data_b64"} | {"truncated": True} for a in res.get("artifacts") or []]
            d["result"] = res
            if len(json.dumps(d, default=str)) > MAX_STATE_FRAME:
                ev = json.dumps(res.get("exit_value"), default=str)
                res["exit_value"] = {"truncated": True, "size_bytes": len(ev), "preview": ev[:2000]}
        return d

    def _send_terminal(self, rec: JobRecord) -> None:
        def acked(ok: bool) -> None:
            if ok and not rec.delivered:
                rec.delivered = True
                self._persist(rec)

        ok = self.out.emit("job_state", self._terminal_payload(rec), buffer=True, ack=acked)
        if ok and self.out.connected():
            rec.sent_gen, rec.sent_at = self.out.conn_gen(), time.monotonic()

    def tick(self) -> None:
        """Called ~1/s by the agent: confirm delivery of terminal states carried by a connection that stayed up."""
        now = time.monotonic()
        gen = self.out.conn_gen()
        with self._lock:
            recs = [r for r in self.jobs.values() if r.terminal and not r.delivered]
        for r in recs:
            if r.sent_gen == gen and r.sent_at is not None and now - r.sent_at >= 10.0 and self.out.connected():
                r.delivered = True
                self._persist(r)

    def ack(self, job_id: str, attempt: int | None = None) -> None:
        with self._lock:
            for (j, a), r in self.jobs.items():
                if j == job_id and (attempt is None or a == attempt) and r.terminal:
                    r.delivered = True

    def ack_log(self, job_id: str, stream: str, seq: int) -> None:
        with self._lock:
            for (j, _a), shp in self._shippers.items():
                if j == job_id and stream in shp:
                    shp[stream].ack(seq)

    def on_connected(self) -> None:
        """After ``hello``: re-report terminal states that are not confirmed (the supervisors re-send events and logs themselves)."""
        with self._lock:
            pending = [r for r in self.jobs.values() if r.terminal and not r.delivered]
        for r in pending:
            self._send_terminal(r)

    def _resend_events(self, rec: JobRecord) -> None:
        for ev in list(rec.events)[-50:]:
            self.out.emit("job_event", ev, buffer=True)

    # ------------------------------------------------------------------ adoption / lifecycle
    def adopt_orphans(self) -> list[str]:
        """Re-attach to jobs started by a previous agent process (pid + create time must still match)."""
        adopted: list[str] = []
        if not self.work.is_dir():
            return adopted
        for d in sorted(self.work.iterdir()):
            f = d / "agent-job.json"
            if not f.is_file():
                continue
            try:
                doc = json.loads(f.read_text())
                known = {k: doc[k] for k in JobRecord.__dataclass_fields__ if k in doc}
                rec = JobRecord(**known)
            except (OSError, ValueError, TypeError):
                continue
            rec.workdir = str(d)
            key = (rec.job_id, rec.attempt)
            self.jobs[key] = rec
            if rec.terminal:
                continue
            alive = procutil.process_matches(rec.pid, rec.pid_create_time)
            if alive is None:
                self._finalise(rec, -1000)
                continue
            log.info("adopting running job %s (pid %s)", rec.job_id, rec.pid)
            t = threading.Thread(target=self._supervise, args=(rec, None), name=f"job-{rec.job_id}", daemon=True)
            self._threads[key] = t
            t.start()
            adopted.append(rec.job_id)
        return adopted

    def cleanup_old(self) -> int:
        """Delete workdirs of delivered terminal jobs older than the retention window."""
        n = 0
        horizon = time.time() - self.cfg.job_retention_hours * 3600
        with self._lock:
            old = [(k, r) for k, r in self.jobs.items() if r.terminal and r.delivered]
        for k, r in old:
            wd = Path(r.workdir)
            try:
                if wd.exists() and wd.stat().st_mtime < horizon:
                    shutil.rmtree(wd, ignore_errors=True)
                    with self._lock:
                        self.jobs.pop(k, None)
                    n += 1
            except OSError:
                continue
        return n

    def stop(self, *, kill_jobs: bool, grace: float = 5.0) -> None:
        """Agent shutdown: leave jobs running (adopted on restart) or terminate them."""
        if kill_jobs:
            for r in self.active():
                self.cancel_job(r.job_id, grace, attempt=r.attempt)
            end = time.monotonic() + grace + 5
            while time.monotonic() < end and self.active():
                time.sleep(0.2)
        self._stop.set()
        for t in list(self._threads.values()):
            t.join(timeout=3)

    # ------------------------------------------------------------------ artifacts on demand
    def fetch_artifact(self, job_id: str, path: str, offset: int = 0, length: int = 4 * 1024 * 1024) -> dict[str, Any]:
        """Paged read of a file below the job directory (never the sensitive spec files)."""
        wd = self.work / job_id
        p = safe_join(wd, path)
        if p.name in DENY_FETCH or not p.is_file():
            raise CommandError("not_found", f"no such artifact {path!r} for job {job_id}")
        length = max(1, min(int(length), 8 * 1024 * 1024))
        size = p.stat().st_size
        with open(p, "rb") as fh:
            fh.seek(max(0, int(offset)))
            data = fh.read(length)
        return {"job_id": job_id, "path": path, "size": size, "offset": int(offset), "length": len(data), "eof": int(offset) + len(data) >= size,
                "data_b64": base64.b64encode(data).decode("ascii")}


def scrubbed_env(cfg: AgentConfig, handle: RuntimeHandle, workdir: Path, extra: dict[str, str] | None = None) -> dict[str, str]:
    """Allow-listed environment for every process that runs user code (jobs and kernels).

    Nothing of the agent's own environment leaks in except a short allow-list (locale, proxies, CA bundles) and the names the
    administrator listed in ``env_passthrough``: in particular never the enrolment token, ``OLCF_*`` settings, cloud credentials or
    ``PYTHONPATH``. The runtime contributes JAVA_HOME/SPARK_HOME/PYSPARK_PYTHON/PATH. HOME/TMPDIR point into the 0700 job directory."""
    env = {k: os.environ[k] for k in (*_ENV_ALLOW, *cfg.env_passthrough) if k in os.environ}
    env.update(handle.env([cfg.path("runtimes", "_jdk", str(m)) for m in (8, 11, 17, 21)]))
    env.update({"HOME": str(workdir / "home"), "TMPDIR": str(workdir / "tmp"), "TEMP": str(workdir / "tmp"), "TMP": str(workdir / "tmp"), "OLCF_JOB_DIR": str(workdir),
                "PYTHONUNBUFFERED": "1", "SPARK_LOCAL_DIRS": str(workdir / "spark-local"), "MPLBACKEND": "Agg"})
    env.update(extra or {})
    return env


def psutil_cores() -> int:
    return psutil.cpu_count(logical=True) or 1


def _decode_content(item: dict[str, Any]) -> bytes:
    raw = item.get("content")
    if raw is None:
        raise CommandError("bad_request", "file entry has no content")
    enc = str(item.get("encoding") or "utf-8").lower()
    if enc == "base64":
        try:
            return base64.b64decode(raw, validate=True)
        except ValueError as exc:
            raise CommandError("bad_request", "invalid base64 content") from exc
    return str(raw).encode("utf-8")
