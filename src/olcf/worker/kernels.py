# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Interactive notebook kernels (CONTRACTS 4.2 ``start_kernel`` ... ``kernel_complete``, 4.4 notebook sessions).

One session = one ``ipykernel`` process running the runtime's ``olcf.runtime_lib.kernel_host`` (OLCF bootstrap preloaded: lazy
``spark``, ``display``, ``notebookutils``, ``%%sql`` ... magics). The agent talks to it with ``jupyter_client`` over loopback ZeroMQ
(connection file 0600 in the session directory, ``127.0.0.1`` only, HMAC-signed messages) and relays every message to the controller as a
``kernel_event`` frame::

    {"session_id", "channel": "iopub"|"shell"|"status", "msg_type", "parent_msg_id", "content"}

* ``iopub``  - ``stream``, ``display_data``, ``update_display_data``, ``execute_result``, ``execute_input``, ``error``, ``clear_output``;
  ``content`` is the Jupyter message content (nbformat output shape) and ``parent_msg_id`` is the *controller's* ``msg_id`` of the
  ``kernel_execute`` it belongs to.
* ``shell``  - ``execute_reply`` (``status``, ``execution_count``, ``ename``/``evalue``/``traceback`` on error).
* ``status`` - ``msg_type`` ``status`` (``content.state``: starting|idle|busy|dead, ``detail``), ``spark`` (``application_id``, ``ui_url``,
  ``app_name``: the kernel's SparkSession started) or ``event`` (any other ``@@OLCF@@`` event of the kernel, e.g. ``spark_error``).

Safety: output frames are bounded (``MAX_EVENT_BYTES``; larger payloads are replaced by a note), text outputs are scrubbed of the session's
secret values and credential patterns, the kernel gets the same scrubbed environment as batch jobs, the session directory is 0700 and
the spec file (which can hold storage credentials) is shredded when the session ends. Kernels are children of the agent
(``JPY_PARENT_PID``) and exit when it dies; after an agent restart the controller learns from ``hello`` that the session is gone.
An optional ``idle_timeout_seconds`` (start payload) shuts idle kernels down on the node as well (the controller culls too).

Threading rule (FINALQA-2): ZeroMQ sockets and the ``jupyter_client`` ``Session`` (HMAC bookkeeping, ``digest_history``) are **not** thread-safe.
Every socket of a session's client is therefore touched by exactly one thread, the session's *I/O thread* (``_io_loop``): it receives the
shell and iopub messages and runs the sends (``execute``, ``complete``, ``kernel_info``) that the command threads hand to it with ``_call``.
Two threads on one socket corrupted multipart frames (``Invalid Signature: b'{}'``), aborted libzmq (``Context was terminated``) or lost replies.
"""

from __future__ import annotations

import concurrent.futures
import json
import logging
import os
import queue
import re
import shutil
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import zmq

from olcf.core.redaction import redact_text
from olcf.core.timeutil import now_iso
from olcf.runtime.errors import RuntimeManagerError
from olcf.runtime.manager import RuntimeManager
from olcf.worker import procutil, sandbox
from olcf.worker.config import AgentConfig
from olcf.worker.jobs import Outbound, parse_memory_mb, safe_join, scrubbed_env, shred
from olcf.worker.layers import LayerManager
from olcf.worker.protocol import CommandError

log = logging.getLogger("olcf.worker.kernels")

MAX_EVENT_BYTES = 4 * 1024 * 1024
_EVENT_RE = re.compile(r"^@@OLCF@@ (\{.*\})\s*$")
_ID_RE = re.compile(r"[A-Za-z0-9_.-]{1,80}")


class Scrubber:
    """Removes exact secret values and credential-looking patterns from (nested) strings."""

    def __init__(self, secrets: list[str]) -> None:
        self.secrets = sorted({s for s in secrets if len(s) >= 4}, key=len, reverse=True)

    def text(self, s: str) -> str:
        for v in self.secrets:
            if v in s:
                s = s.replace(v, "[REDACTED]")
        return redact_text(s)

    def obj(self, o: Any) -> Any:
        if isinstance(o, str):
            return self.text(o)
        if isinstance(o, list):
            return [self.obj(x) for x in o]
        if isinstance(o, dict):
            return {k: (v if k in ("image/png", "image/jpeg", "image/gif") else self.obj(v)) for k, v in o.items()}
        return o


def bound_content(content: dict[str, Any]) -> dict[str, Any]:
    """Replace over-sized outputs by a short note so one cell can never exhaust the control channel."""
    try:
        size = len(json.dumps(content, default=str))
    except (TypeError, ValueError):
        return {"text": "[olcf: output could not be serialised]"}
    if size <= MAX_EVENT_BYTES:
        return content
    note = f"[olcf: output of {size} bytes exceeds the {MAX_EVENT_BYTES} byte limit of the control channel and was dropped]"
    if "text" in content:
        return {**content, "text": note}
    return {"data": {"text/plain": note}, "metadata": {}}


@dataclass
class Session:
    """An interactive session on a worker: id, runtime, working directory and granted resources."""
    session_id: str
    runtime_id: str
    dir: Path
    resources: dict[str, int]
    scrub: Scrubber
    state: str = "starting"
    detail: str = ""
    pid: int | None = None
    create_time: float | None = None
    km: Any = None
    kc: Any = None
    spark: dict[str, Any] | None = None
    started_at: str = field(default_factory=now_iso)
    last_activity: float = field(default_factory=time.monotonic)
    idle_timeout: float | None = None
    msg_map: dict[str, str] = field(default_factory=dict)  # jupyter msg_id -> controller msg_id
    waiters: dict[str, queue.Queue[dict[str, Any]]] = field(default_factory=dict)
    threads: list[threading.Thread] = field(default_factory=list)
    io_jobs: queue.Queue[tuple[Any, concurrent.futures.Future[Any]]] = field(default_factory=queue.Queue)  # sends handed to the I/O thread
    io_thread: threading.Thread | None = None
    iopub_waiters: dict[str, queue.Queue[dict[str, Any]]] = field(default_factory=dict)
    restarting: bool = False
    stop: threading.Event = field(default_factory=threading.Event)
    busy: int = 0
    closing: bool = False
    files: list[Any] = field(default_factory=list)  # kernel.out/kernel.err handles: KernelManager re-uses them on restart, so they live as long as the session
    lock: threading.RLock = field(default_factory=threading.RLock)
    out_pos: int = 0

    def brief(self) -> dict[str, Any]:
        return {"session_id": self.session_id, "runtime_id": self.runtime_id, "pid": self.pid, "state": self.state, "detail": self.detail, "spark": self.spark,
                "started_at": self.started_at}


class KernelService:
    """Starts and relays notebook kernels."""

    def __init__(self, cfg: AgentConfig, runtimes: RuntimeManager, out: Outbound, layers: LayerManager | None = None) -> None:
        self.cfg, self.runtimes, self.out = cfg, runtimes, out
        self.layers = layers or LayerManager(cfg)
        self.sessions: dict[str, Session] = {}
        self._lock = threading.RLock()
        self.root = cfg.path("kernels")
        self.extra_allocated = lambda: (0, 0)  # set by the agent: (cores, memory_mb) used by jobs and daemons
        self.capacity = lambda: (1 << 30, 1 << 30)
        self.draining = False

    # ------------------------------------------------------------------ queries
    def snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            return [s.brief() for s in self.sessions.values()]

    def allocated(self) -> tuple[int, int]:
        with self._lock:
            ss = [s for s in self.sessions.values() if not s.closing]
        return sum(s.resources["cores"] for s in ss), sum(s.resources["memory_mb"] for s in ss)

    def _get(self, session_id: str) -> Session:
        with self._lock:
            s = self.sessions.get(session_id)
        if s is None:
            raise CommandError("no_such_session", f"kernel session {session_id!r} does not exist on this node", remediation="Start a new session (the node may have restarted).")
        return s

    # ------------------------------------------------------------------ commands
    def start(self, payload: dict[str, Any], *, extra_used: tuple[int, int] = (0, 0)) -> dict[str, Any]:
        session_id = str(payload.get("session_id") or "")
        if not _ID_RE.fullmatch(session_id):
            raise CommandError("bad_request", f"invalid session_id {session_id!r}")
        spec = payload.get("spec")
        if not isinstance(spec, dict):
            raise CommandError("bad_request", "start_kernel needs payload.spec (a job spec with runtime.id)")
        with self._lock:
            if session_id in self.sessions and not self.sessions[session_id].closing:
                s = self.sessions[session_id]
                return {**s.brief(), "duplicate": True}
        if self.draining:
            raise CommandError("draining", "this node is draining and accepts no new sessions")
        rt = str((spec.get("runtime") or {}).get("id") or payload.get("runtime_id") or "")
        if not rt:
            raise CommandError("bad_request", "spec.runtime.id is required")
        r = payload.get("resources") or {}
        conf = (spec.get("spark") or {}).get("conf") or {}
        res = {"cores": max(1, int(r.get("cores") or conf.get("spark.driver.cores") or 1)),
               "memory_mb": max(64, int(r.get("memory_mb") or parse_memory_mb(conf.get("spark.driver.memory"), 1024)))}
        cap_c, cap_m = self.capacity()
        x_c, x_m = self.extra_allocated()
        a_c, a_m = self.allocated()
        if a_c + x_c + res["cores"] > cap_c or a_m + x_m + res["memory_mb"] > cap_m:
            raise CommandError("resource_exhausted", f"not enough free resources for a session: need {res['cores']} cores/{res['memory_mb']} MB",
                               remediation="Close idle sessions or place the session on another node.")
        try:
            handle = self.runtimes.light_check(rt)
        except RuntimeManagerError as exc:
            raise CommandError("runtime_not_ready", f"runtime {rt} cannot be used on this node: {exc.message}", remediation=exc.remediation) from exc
        sdir = self.root / session_id
        if sdir.exists():
            shutil.rmtree(sdir, ignore_errors=True)
        for sub in ("", "tmp", "home", "spark-local"):
            (sdir / sub).mkdir(parents=True, exist_ok=True)
            try:
                os.chmod(sdir / sub, 0o700)
            except OSError:
                pass
        secrets = [str(v) for v in (spec.get("secrets") or {}).values()] + [str(v) for v in ((spec.get("storage_conf") or {}).get("env") or {}).values()]
        secrets += [str(v) for k, v in ((spec.get("storage_conf") or {}).get("spark") or {}).items() if re.search(r"(?i)(secret|key|token|password|sas)", str(k))]
        try:
            self.layers.apply(spec, handle.python)   # the session's environment layer + jars (same cache as batch jobs)
        except BaseException:
            shutil.rmtree(sdir, ignore_errors=True)
            raise
        s = Session(session_id, rt, sdir, res, Scrubber(secrets), idle_timeout=float(payload["idle_timeout_seconds"]) if payload.get("idle_timeout_seconds") else None)
        options = {"eager_spark": bool(payload.get("eager_spark", True)), "workdir": str(sdir)}
        fd = os.open(sdir / "kernel-spec.json", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump({"spec": spec, "options": options}, fh, default=str)
        with self._lock:
            self.sessions[session_id] = s
        self._status(s, "starting", "launching kernel")
        try:
            self._launch(s, handle)
        except BaseException:
            self._teardown(s, "failed to start")
            raise
        return s.brief()

    def _launch(self, s: Session, handle: Any) -> None:
        from jupyter_client import KernelManager
        from jupyter_client.kernelspec import KernelSpec

        cmd = handle.module_argv("olcf.runtime_lib.kernel_host", "-f", "{connection_file}", "--spec", str(s.dir / "kernel-spec.json"))
        cmd = sandbox.wrap_command(cmd, self.cfg.sandbox_command, workdir=s.dir, runtime_dir=handle.root, state_dir=self.cfg.state_dir, home=s.dir / "home") if self.cfg.sandbox_command else cmd
        # interactive kernels render figures inline (display_data PNG); batch jobs keep the non-interactive Agg backend of scrubbed_env
        env = scrubbed_env(self.cfg, handle, s.dir, {"OLCF_SESSION_ID": s.session_id, "MPLBACKEND": "module://matplotlib_inline.backend_inline"})
        km = KernelManager()
        km.ip = "127.0.0.1"
        km.connection_file = str(s.dir / "kernel.json")
        km._kernel_spec = KernelSpec(argv=cmd, display_name="OLCF PySpark", language="python", env={})  # noqa: SLF001 - bypass kernelspec discovery (supported pattern)
        out_f = open(s.dir / "kernel.out", "ab")  # noqa: SIM115 - handed to the child
        err_f = open(s.dir / "kernel.err", "ab")  # noqa: SIM115
        # The handles must stay open for the whole session: jupyter_client replays the same launch arguments on restart_kernel().
        s.files = [out_f, err_f]
        try:
            km.start_kernel(cwd=str(s.dir), env=env, stdout=out_f, stderr=err_f)
        except Exception as exc:  # noqa: BLE001
            raise CommandError("spawn_failed", f"cannot start the kernel: {exc}", remediation="Check the runtime (verify/repair) and the sandbox command.") from exc
        s.km = km
        s.pid = getattr(km.provisioner, "pid", None) if getattr(km, "provisioner", None) else None
        if s.pid is None and getattr(km, "kernel", None) is not None:
            s.pid = km.kernel.pid
        try:
            import psutil

            s.create_time = psutil.Process(s.pid).create_time() if s.pid else None
        except Exception:  # noqa: BLE001
            s.create_time = None
        kc = km.client()
        kc.start_channels()
        s.kc = kc
        try:
            kc.wait_for_ready(timeout=self.cfg.ready_timeout_seconds)
        except RuntimeError as exc:
            tail = redact_text(self._tail(s.dir / "kernel.err", 3000))
            raise CommandError("kernel_timeout", f"the kernel did not become ready within {self.cfg.ready_timeout_seconds:.0f}s: {exc}\n{tail}",
                               remediation="See the kernel log (collect_diagnostics); the runtime may lack ipykernel.") from exc
        for name, fn in (("io", self._io_loop), ("log", self._log_loop), ("watch", self._watch_loop)):
            t = threading.Thread(target=fn, args=(s,), name=f"kernel-{s.session_id}-{name}", daemon=True)
            if name == "io":
                s.io_thread = t
            s.threads.append(t)
            t.start()
        self._status(s, "idle", "kernel ready")

    @staticmethod
    def _tail(p: Path, n: int) -> str:
        try:
            with open(p, "rb") as fh:
                fh.seek(0, os.SEEK_END)
                fh.seek(max(0, fh.tell() - n))
                return fh.read().decode("utf-8", "replace")
        except OSError:
            return ""

    def execute(self, payload: dict[str, Any]) -> dict[str, Any]:
        s = self._get(str(payload.get("session_id") or ""))
        if s.state == "dead" or s.kc is None:
            raise CommandError("kernel_dead", f"kernel session {s.session_id} is not running", remediation="Restart the session.")
        code = payload.get("code")
        if not isinstance(code, str):
            raise CommandError("bad_request", "kernel_execute needs payload.code (string)")
        self._check_not_restarting(s)
        ctl_id = str(payload.get("msg_id") or "")
        store_history, stop_on_error = bool(payload.get("store_history", True)), bool(payload.get("stop_on_error", True))

        def send() -> str:
            # runs on the I/O thread: the id is registered before any reply can be read, and the socket has one user
            jid = s.kc.execute(code, store_history=store_history, allow_stdin=False, stop_on_error=stop_on_error)
            with s.lock:
                s.msg_map[jid] = ctl_id or jid
                if len(s.msg_map) > 2000:
                    for k in list(s.msg_map)[:500]:
                        s.msg_map.pop(k, None)
                s.busy += 1
            return jid

        jid = self._call(s, send)
        s.last_activity = time.monotonic()
        return {"session_id": s.session_id, "msg_id": ctl_id or jid, "accepted": True}

    def interrupt(self, payload: dict[str, Any]) -> dict[str, Any]:
        s = self._get(str(payload.get("session_id") or ""))
        if s.km is None:
            raise CommandError("kernel_dead", "kernel is not running")
        s.km.interrupt_kernel()
        return {"session_id": s.session_id, "interrupted": True}

    def restart(self, payload: dict[str, Any]) -> dict[str, Any]:
        s = self._get(str(payload.get("session_id") or ""))
        if s.km is None:
            raise CommandError("kernel_dead", "kernel is not running")
        with s.lock:
            if s.restarting or s.closing:
                raise CommandError("kernel_busy", "the kernel is already restarting or shutting down", remediation="Wait for the restart to finish.")
            s.restarting = True
        try:
            self._status(s, "starting", "restarting kernel")
            with s.lock:
                s.msg_map.clear()
                s.busy = 0
                s.spark = None
            try:
                s.km.restart_kernel(now=True)  # the manager's own (control) socket; the client's sockets stay with the I/O thread
                prov = getattr(s.km, "provisioner", None)
                s.pid = getattr(prov, "pid", s.pid)
                try:
                    import psutil

                    s.create_time = psutil.Process(s.pid).create_time() if s.pid else None  # the watch loop compares pid + start time
                except Exception:  # noqa: BLE001
                    s.create_time = None
                self._wait_ready(s, self.cfg.ready_timeout_seconds)
            except Exception as exc:  # noqa: BLE001
                self._status(s, "dead", f"restart failed: {exc}")
                raise CommandError("kernel_restart_failed", f"kernel restart failed: {exc}") from exc
            s.last_activity = time.monotonic()
            self._status(s, "idle", "kernel restarted")
        finally:
            s.restarting = False
        return {"session_id": s.session_id, "pid": s.pid, "state": "idle"}

    def _check_not_restarting(self, s: Session) -> None:
        if s.restarting:
            raise CommandError("kernel_restarting", f"kernel session {s.session_id} is restarting", remediation="Retry in a moment.")

    def _call(self, s: Session, fn: Any, timeout: float = 30.0) -> Any:
        """Run ``fn`` on the session's I/O thread (the only thread that may touch the client's sockets) and return its result."""
        if threading.current_thread() is s.io_thread:
            return fn()
        if s.stop.is_set() or s.io_thread is None or not s.io_thread.is_alive():
            raise CommandError("kernel_dead", f"kernel session {s.session_id} is not running", remediation="Restart the session.")
        fut: concurrent.futures.Future[Any] = concurrent.futures.Future()
        s.io_jobs.put((fn, fut))
        try:
            return fut.result(timeout=timeout)
        except concurrent.futures.TimeoutError as exc:
            fut.cancel()
            raise CommandError("timeout", "the kernel session did not accept the request in time") from exc

    def _wait_ready(self, s: Session, timeout: float) -> None:
        """After a restart: ask ``kernel_info`` until the new kernel answers on shell *and* publishes on iopub (the same handshake as
        ``KernelClient.wait_for_ready``, but through the I/O thread so no second thread reads the sockets)."""
        deadline = time.monotonic() + timeout
        while True:
            shell_q: queue.Queue[dict[str, Any]] = queue.Queue()
            idle_q: queue.Queue[dict[str, Any]] = queue.Queue()

            def ask(shell_q: queue.Queue[dict[str, Any]] = shell_q, idle_q: queue.Queue[dict[str, Any]] = idle_q) -> str:
                jid = s.kc.kernel_info()
                with s.lock:
                    s.waiters[jid] = shell_q
                    s.iopub_waiters[jid] = idle_q
                return jid

            jid = self._call(s, ask)
            try:
                try:
                    shell_q.get(timeout=max(0.1, min(2.0, deadline - time.monotonic())))
                    idle_q.get(timeout=1.0)  # the kernel's `status: idle` for that request proves the iopub subscription is connected
                    return
                except queue.Empty:
                    pass
            finally:
                with s.lock:
                    s.waiters.pop(jid, None)
                    s.iopub_waiters.pop(jid, None)
            if procutil.process_matches(s.pid, s.create_time) is None:
                raise RuntimeError("the kernel died before replying to kernel_info" + self._hint(s))
            if time.monotonic() > deadline:
                raise RuntimeError(f"the kernel did not answer within {timeout:.0f}s")

    def shutdown(self, payload: dict[str, Any]) -> dict[str, Any]:
        sid = str(payload.get("session_id") or "")
        with self._lock:
            s = self.sessions.get(sid)
        if s is None:
            return {"session_id": sid, "found": False, "state": "dead"}
        self._teardown(s, "shutdown requested")
        return {"session_id": sid, "found": True, "state": "dead"}

    def complete(self, payload: dict[str, Any]) -> dict[str, Any]:
        s = self._get(str(payload.get("session_id") or ""))
        if s.kc is None or s.state == "dead":
            raise CommandError("kernel_dead", "kernel is not running")
        code = str(payload.get("code") or "")
        cursor = int(payload.get("cursor") if payload.get("cursor") is not None else len(code))
        self._check_not_restarting(s)
        q: queue.Queue[dict[str, Any]] = queue.Queue()

        def send() -> str:
            jid = s.kc.complete(code, cursor)
            with s.lock:
                s.waiters[jid] = q  # registered on the I/O thread, before the reply can be read
            return jid

        jid = self._call(s, send)
        try:
            msg = q.get(timeout=float(payload.get("timeout") or 10.0))
        except queue.Empty as exc:
            raise CommandError("timeout", "the kernel did not answer the completion request in time (it is busy)") from exc
        finally:
            with s.lock:
                s.waiters.pop(jid, None)
        c = msg.get("content", {})
        return {"session_id": s.session_id, "status": c.get("status"), "matches": c.get("matches", []), "cursor_start": c.get("cursor_start"), "cursor_end": c.get("cursor_end"),
                "metadata": c.get("metadata", {})}

    # ------------------------------------------------------------------ relays (threads)
    def _event(self, s: Session, channel: str, msg_type: str, content: dict[str, Any], parent: str | None = None) -> None:
        payload = {"session_id": s.session_id, "channel": channel, "msg_type": msg_type, "parent_msg_id": parent, "content": s.scrub.obj(bound_content(content))}
        self.out.emit("kernel_event", payload)

    def _status(self, s: Session, state: str, detail: str = "") -> None:
        s.state, s.detail = state, detail
        self._event(s, "status", "status", {"state": state, "detail": detail})

    def _io_loop(self, s: Session) -> None:
        """The only thread that touches the client's sockets: runs queued sends, relays iopub and shell messages."""
        kc = s.kc
        shell, iopub = kc.shell_channel, kc.iopub_channel
        poller = zmq.Poller()
        poller.register(shell.socket, zmq.POLLIN)
        poller.register(iopub.socket, zmq.POLLIN)
        while not s.stop.is_set():
            while True:
                try:
                    fn, fut = s.io_jobs.get_nowait()
                except queue.Empty:
                    break
                if not fut.set_running_or_notify_cancel():
                    continue
                try:
                    fut.set_result(fn())
                except BaseException as exc:  # noqa: BLE001 - handed to the caller
                    fut.set_exception(exc)
            try:
                ready = dict(poller.poll(25))
            except zmq.ZMQError:
                if s.stop.is_set():
                    return
                time.sleep(0.1)
                continue
            for chan, handler in ((iopub, self._on_iopub), (shell, self._on_shell)):
                if chan.socket not in ready:
                    continue
                try:
                    msg = chan.get_msg(timeout=0)
                except queue.Empty:
                    continue
                except Exception as exc:  # noqa: BLE001 - a frame that does not verify is dropped, never fatal for the session
                    if s.stop.is_set():
                        return
                    log.warning("session %s: dropped an unreadable kernel message: %s", s.session_id, exc)
                    continue
                handler(s, msg)
        # fail what was queued after the last pass so that no caller waits for its timeout
        while True:
            try:
                _fn, fut = s.io_jobs.get_nowait()
            except queue.Empty:
                break
            fut.cancel()

    def _on_iopub(self, s: Session, msg: dict[str, Any]) -> None:
        mtype = msg.get("msg_type") or msg.get("header", {}).get("msg_type")
        content = msg.get("content", {})
        parent = msg.get("parent_header", {}).get("msg_id")
        with s.lock:
            ctl = s.msg_map.get(parent or "", parent)
            iowaiter = s.iopub_waiters.get(parent or "")
        if mtype == "status":
            state = content.get("execution_state")
            if iowaiter is not None and state == "idle":
                iowaiter.put(msg)
                return
            with s.lock:
                known = (parent or "") in s.msg_map
            if known and state in ("busy", "idle") and s.state != "dead":
                if state == "idle":
                    with s.lock:
                        s.busy = max(0, s.busy - 1)
                    s.last_activity = time.monotonic()
                s.state = state
                self._event(s, "status", "status", {"state": state, "detail": ""}, ctl)
            return
        if mtype in ("stream", "display_data", "update_display_data", "execute_result", "execute_input", "error", "clear_output"):
            self._event(s, "iopub", mtype, content, ctl)

    def _on_shell(self, s: Session, msg: dict[str, Any]) -> None:
        parent = msg.get("parent_header", {}).get("msg_id")
        mtype = msg.get("msg_type")
        with s.lock:
            waiter = s.waiters.get(parent or "")
            ctl = s.msg_map.get(parent or "", parent)
        if waiter is not None:
            waiter.put(msg)
            return
        if mtype == "execute_reply":
            c = msg.get("content", {})
            self._event(s, "shell", "execute_reply", {k: c.get(k) for k in ("status", "execution_count", "ename", "evalue", "traceback") if k in c}, ctl)

    def _log_loop(self, s: Session) -> None:
        """Tail ``kernel.out``: @@OLCF@@ events become ``status`` events (Spark app info is also kept for ``hello``)."""
        path = s.dir / "kernel.out"
        pos = 0
        buf = b""
        while not s.stop.is_set():
            try:
                with open(path, "rb") as fh:
                    fh.seek(pos)
                    data = fh.read(64 * 1024)
            except OSError:
                data = b""
            if not data:
                s.stop.wait(0.5)
                continue
            pos += len(data)
            buf += data
            *lines, buf = buf.split(b"\n")
            if len(buf) > 1 << 20:
                buf = b""
            for raw in lines:
                m = _EVENT_RE.match(raw.decode("utf-8", "replace"))
                if not m:
                    continue
                try:
                    ev = json.loads(m.group(1))
                except ValueError:
                    continue
                name = str(ev.pop("event", "event"))
                if name == "spark_app_started":
                    s.spark = {k: ev.get(k) for k in ("application_id", "ui_url", "app_name", "master")}
                    self._event(s, "status", "spark", dict(s.spark))
                elif name != "kernel_ready":
                    self._event(s, "status", "event", {"event": name, **ev})

    def _watch_loop(self, s: Session) -> None:
        """Detect a dead kernel process (OOM kill, crash) and the idle timeout."""
        while not s.stop.wait(1.0):
            if s.closing:
                return
            alive = procutil.process_matches(s.pid, s.create_time) is not None if s.pid else False
            if not alive and s.state != "starting" and not s.restarting:
                self._status(s, "dead", "the kernel process exited unexpectedly" + self._hint(s))
                s.stop.set()
                return
            if s.idle_timeout and s.busy == 0 and time.monotonic() - s.last_activity > s.idle_timeout:
                log.info("session %s idle for %.0fs: shutting down", s.session_id, s.idle_timeout)
                self._teardown(s, f"idle timeout of {s.idle_timeout:.0f}s")
                return

    def _hint(self, s: Session) -> str:
        tail = redact_text(self._tail(s.dir / "kernel.err", 600)).strip()
        return f": {tail[-300:]}" if tail else ""

    # ------------------------------------------------------------------ teardown / lifecycle
    def _teardown(self, s: Session, reason: str) -> None:
        with s.lock:
            if s.closing:
                return
            s.closing = True
        s.stop.set()
        io = s.io_thread
        if io is not None and io is not threading.current_thread():
            io.join(timeout=5.0)  # the sockets are closed only after their single user has stopped
        try:
            if s.kc is not None:
                s.kc.stop_channels()
        except Exception:  # noqa: BLE001, S110
            pass
        try:
            if s.km is not None:
                s.km.shutdown_kernel(now=True)
        except Exception:  # noqa: BLE001, S110
            pass
        if s.pid:
            procutil.terminate_tree(s.pid, grace=2.0, create_time=s.create_time)
        for fh in s.files:
            try:
                fh.close()
            except OSError:
                pass
        s.files = []
        for name in ("kernel-spec.json", "kernel.json"):
            shred(s.dir / name)
        shutil.rmtree(s.dir / "tmp", ignore_errors=True)
        shutil.rmtree(s.dir / "spark-local", ignore_errors=True)
        s.state, s.detail = "dead", reason
        self._event(s, "status", "status", {"state": "dead", "detail": reason})
        with self._lock:
            self.sessions.pop(s.session_id, None)

    def stop_all(self) -> None:
        with self._lock:
            ss = list(self.sessions.values())
        for s in ss:
            self._teardown(s, "agent shutting down")

    def cleanup_stale(self) -> int:
        """Remove session directories left by a previous agent process (kernels die with the agent)."""
        n = 0
        if self.root.is_dir():
            for d in self.root.iterdir():
                if d.is_dir() and d.name not in self.sessions:
                    shutil.rmtree(d, ignore_errors=True)
                    n += 1
        return n

    def read_file(self, session_id: str, rel: str, limit: int = 64 * 1024) -> str:
        p = safe_join(self.root / session_id, rel)
        return redact_text(p.read_text(encoding="utf-8", errors="replace")[-limit:])
