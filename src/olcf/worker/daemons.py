# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Spark Standalone Master / Worker daemons (CONTRACTS section 4.2 ``start_spark_master`` ... ``stop_spark_worker``).

The agent starts the *real* JVM daemons with the runtime's own ``spark-class`` launcher::

    $SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master --host H --port P --webui-port W --properties-file F
    $SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker spark://H:P --cores N --memory Mm --work-dir D ...

(``spark-class`` ``exec``s the JVM, so the pid we record *is* the JVM.) There is exactly one master and one worker daemon per
``(role, runtime_id)`` on a node: standalone clusters are per runtime and are never mixed (CONTRACTS 4.5).

Per daemon directory ``<state>/daemons/<role>-<runtime_id>/``::

    daemon.json      desired parameters + pid/create_time (written atomically; used to adopt after an agent restart)
    conf/            private SPARK_CONF_DIR (spark-defaults.conf 0600: may carry spark.authenticate.secret)
    logs/daemon.log  stdout+stderr of the JVM (log4j console appender), secrets redacted when read back through diagnostics
    work/            worker work dir (executor stdout/stderr, app dirs) unless the controller supplies ``work_dir``

Health: the master answers ``GET /json/`` on its web UI (``status: ALIVE``, the worker list), the worker answers ``GET /json/``
on its own web UI. A supervisor thread polls both; a daemon that dies unexpectedly is restarted with exponential backoff
(at most ``MAX_RESTARTS`` per ``RESTART_WINDOW`` seconds, then it is reported ``failed``) and every transition is sent as a
``daemon_state`` frame. ``stop_*`` is SIGTERM to the process group, SIGKILL after a grace period.

Lifetime: the daemons are children in their own session (so one ``stop`` reaches the JVM and anything it forked). By default the agent
stops them when it exits (SIGTERM / SIGINT / ``shutdown``; ``AgentConfig.keep_daemons_on_exit`` unset), so a stopped agent never leaves orphan
Master / Worker JVMs behind. Operators who restart agents in a rolling fashion and want the cluster to stay up set
``keep_daemons_on_exit = true`` (``agent.toml``) or pass ``--keep-daemons-on-exit``: the daemons then survive and are adopted from
``daemon.json`` by the next agent process.

Memory: the daemon JVM heap is ``cfg.daemon_memory`` (``SPARK_DAEMON_MEMORY``); the worker's *offered* cores/memory
(``--cores/--memory``) are reported to the job manager as allocated so the node is not oversubscribed.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import psutil

from olcf.core.redaction import redact_text
from olcf.core.timeutil import now_iso
from olcf.runtime.errors import RuntimeManagerError
from olcf.runtime.manager import RuntimeHandle, RuntimeManager
from olcf.worker import procutil
from olcf.worker.config import AgentConfig
from olcf.worker.jobs import Outbound
from olcf.worker.protocol import CommandError

log = logging.getLogger("olcf.worker.daemons")

MAX_RESTARTS = 5
RESTART_WINDOW = 600.0
MASTER_CLASS = "org.apache.spark.deploy.master.Master"
WORKER_CLASS = "org.apache.spark.deploy.worker.Worker"
_SECRET_CONF = ("secret", "password", "token", "key", "sas")


def _is_secret_key(k: str) -> bool:
    kl = k.lower()
    return any(s in kl for s in _SECRET_CONF)


def _connect_host(host: str) -> str:
    return "127.0.0.1" if host in ("", "0.0.0.0", "::") else host  # noqa: S104


_NO_PROXY_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def http_json(url: str, timeout: float = 2.0) -> dict[str, Any] | None:
    """GET ``url`` and parse JSON; ``None`` on any failure (the daemon is simply not healthy yet)."""
    try:  # daemons are polled on their own address: bypass any HTTP(S)_PROXY from the environment
        with _NO_PROXY_OPENER.open(url, timeout=timeout) as resp:  # noqa: S310 - http(s) URL built from controller parameters
            return json.loads(resp.read().decode("utf-8", "replace"))
    except (urllib.error.URLError, OSError, ValueError):
        return None


@dataclass
class Daemon:
    """One supervised daemon (desired state + runtime state)."""

    role: str  # master | worker
    runtime_id: str
    params: dict[str, Any]
    dir: Path
    state: str = "starting"  # starting|running|restarting|stopping|stopped|failed
    detail: str = ""
    pid: int | None = None
    create_time: float | None = None
    url: str | None = None  # master: spark://h:p ; worker: its web UI
    ui_url: str | None = None
    restarts: list[float] = field(default_factory=list)
    stop_requested: bool = False
    next_restart_at: float = 0.0
    backoff: float = 1.0
    proc: subprocess.Popen[bytes] | None = None
    extra: dict[str, Any] = field(default_factory=dict)
    ui_port: int | None = None  # effective web UI port (explicit, or chosen at first launch and kept across restarts)

    @property
    def key(self) -> str:
        return f"{self.role}:{self.runtime_id}"

    def brief(self) -> dict[str, Any]:
        d: dict[str, Any] = {"role": self.role, "runtime_id": self.runtime_id, "pid": self.pid, "url": self.url, "ui_url": self.ui_url, "state": self.state, "detail": self.detail,
                             "ui_port": self.ui_port}
        if self.role == "master":
            d.update({"port": self.params.get("port"), "host": self.params.get("host")})
        else:
            d.update({"master_url": self.params.get("master_url"), "cores": self.params.get("cores"), "memory_mb": self.params.get("memory_mb"), "labels": self.params.get("labels"),
                      "host": self.params.get("host")})
        d.update(self.extra)
        return d


class DaemonManager:
    """Starts, supervises and stops the Spark standalone daemons of this node."""

    def __init__(self, cfg: AgentConfig, runtimes: RuntimeManager, out: Outbound, *, worker_id: str | None = None) -> None:
        self.cfg, self.runtimes, self.out = cfg, runtimes, out
        #: The controller-assigned id of this node. Exported to the daemons as ``OLCF_WORKER_ID`` so that executors (children of the Spark Worker JVM) can
        #: tell a job which *node* they run on - the host name alone is ambiguous when several agents share one machine or address.
        self.worker_id = worker_id
        self.daemons: dict[str, Daemon] = {}
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.root = cfg.path("daemons")

    # ------------------------------------------------------------------ public commands
    def start_master(self, payload: dict[str, Any]) -> dict[str, Any]:
        rt = self._runtime_id(payload)
        host = str(payload.get("host") or self.cfg.advertise_host or "127.0.0.1")
        port = int(payload.get("port") or 7077)
        ui_port = int(payload.get("ui_port") or 8080)
        params = {"host": host, "port": port, "ui_port": ui_port, "conf": dict(payload.get("conf") or {})}
        return self._start("master", rt, params)

    def start_worker(self, payload: dict[str, Any]) -> dict[str, Any]:
        rt = self._runtime_id(payload)
        master_url = str(payload.get("master_url") or "")
        if not master_url.startswith("spark://"):
            raise CommandError("bad_request", "start_spark_worker needs master_url like spark://host:7077")
        cores = max(1, int(payload.get("cores") or self.cfg.max_cores or psutil.cpu_count() or 1))
        memory_mb = max(256, int(payload.get("memory_mb") or 1024))
        params = {"master_url": master_url, "cores": cores, "memory_mb": memory_mb, "labels": dict(payload.get("labels") or {}),
                  "work_dir": payload.get("work_dir"), "port": payload.get("port"), "ui_port": payload.get("ui_port"),
                  "host": str(payload.get("host") or self.cfg.advertise_host or "127.0.0.1"), "conf": dict(payload.get("conf") or {})}
        return self._start("worker", rt, params)

    def stop(self, role: str, payload: dict[str, Any]) -> dict[str, Any]:
        rt = str(payload.get("runtime_id") or "")
        grace = float(payload.get("grace_seconds") or 15.0)
        with self._lock:
            d = self.daemons.get(f"{role}:{rt}") if rt else next((x for x in self.daemons.values() if x.role == role), None)
        if d is None:
            return {"role": role, "runtime_id": rt, "state": "stopped", "found": False}
        self._stop_daemon(d, grace)
        return {"role": role, "runtime_id": d.runtime_id, "state": d.state, "found": True}

    # ------------------------------------------------------------------ queries
    def snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            return [d.brief() for d in self.daemons.values() if d.state not in ("stopped",)]

    def allocated(self) -> tuple[int, int]:
        """(cores, memory_mb) the worker daemons offer to Spark: unavailable for jobs' drivers."""
        with self._lock:
            ws = [d for d in self.daemons.values() if d.role == "worker" and d.state in ("starting", "running", "restarting")]
        return sum(int(d.params["cores"]) for d in ws), sum(int(d.params["memory_mb"]) for d in ws)

    def components(self) -> list[dict[str, Any]]:
        out = []
        with self._lock:
            ds = list(self.daemons.values())
        for d in ds:
            status = {"running": "ok", "starting": "degraded", "restarting": "degraded", "stopping": "degraded", "stopped": "unknown", "failed": "down"}.get(d.state, "unknown")
            out.append({"name": f"spark-{d.role}:{d.runtime_id}", "status": status, "detail": d.detail or d.state})
        return out

    def log_tail(self, role: str, runtime_id: str, n: int = 8000) -> str:
        p = self.root / f"{role}-{runtime_id}" / "logs" / "daemon.log"
        try:
            with open(p, "rb") as fh:
                fh.seek(0, os.SEEK_END)
                fh.seek(max(0, fh.tell() - n))
                return redact_text(fh.read().decode("utf-8", "replace"))
        except OSError:
            return ""

    # ------------------------------------------------------------------ start / launch
    def _runtime_id(self, payload: dict[str, Any]) -> str:
        rt = str(payload.get("runtime_id") or "")
        if not rt:
            raise CommandError("bad_request", "runtime_id is required")
        return rt

    def _start(self, role: str, rt: str, params: dict[str, Any]) -> dict[str, Any]:
        try:
            handle = self.runtimes.light_check(rt)
        except RuntimeManagerError as exc:
            raise CommandError("runtime_not_ready", f"runtime {rt} cannot be used on this node: {exc.message}", remediation=exc.remediation) from exc
        key = f"{role}:{rt}"
        with self._lock:
            old = self.daemons.get(key)
        if old is not None and old.state in ("starting", "running", "restarting") and old.params == params and self._alive(old):
            return {**old.brief(), "duplicate": True}
        if old is not None and old.state not in ("stopped", "failed"):
            self._stop_daemon(old, 15.0)  # desired parameters changed: restart with the new ones
        d = Daemon(role=role, runtime_id=rt, params=params, dir=self.root / f"{role}-{rt}")
        with self._lock:
            self.daemons[key] = d
        self._prepare_dirs(d)
        self._launch(d, handle)
        self._wait_ready(d)
        self._ensure_thread()
        return d.brief()

    def _prepare_dirs(self, d: Daemon) -> None:
        for sub in ("", "conf", "logs", "work", "local"):
            (d.dir / sub).mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(d.dir, 0o700)
        except OSError:
            pass
        p = d.params
        conf: dict[str, Any] = {"spark.worker.cleanup.enabled": "true"} if d.role == "worker" else {}
        conf.update(p.get("conf") or {})
        text = "".join(f"{k} {v}\n" for k, v in sorted(conf.items()))
        f = d.dir / "conf" / "spark-defaults.conf"
        fd = os.open(f, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
        (d.dir / "conf" / "spark-env.sh").write_text("# generated by the OLCF worker agent; intentionally empty (environment comes from the agent)\n", encoding="utf-8")

    def _command(self, d: Daemon, handle: RuntimeHandle) -> tuple[list[str], dict[str, Any]]:
        p = d.params
        if not handle.has_spark:
            raise CommandError("runtime_no_spark", f"runtime {handle.id} has no Spark, it cannot host a Spark {d.role}", remediation="Choose a Spark runtime (fabric-*/olcf-native).")
        launcher = handle.spark_home / "bin" / ("spark-class.cmd" if os.name == "nt" else "spark-class")
        if not launcher.is_file():
            raise CommandError("runtime_broken", f"{launcher} is missing: the runtime installation is damaged", remediation=f"Run: olcf-worker runtime repair {handle.id}")
        props = str(d.dir / "conf" / "spark-defaults.conf")
        if d.role == "master":
            host = p["host"]
            if procutil.port_in_use(host, int(p["port"])):
                raise CommandError("port_in_use", f"master port {p['port']} is already in use on {host}", remediation="Choose another port or stop the process that owns it.")
            if procutil.port_in_use(host, int(p["ui_port"])):
                raise CommandError("port_in_use", f"master web UI port {p['ui_port']} is already in use", remediation="Choose another ui_port.")
            args = [MASTER_CLASS, "--host", host, "--port", str(p["port"]), "--webui-port", str(p["ui_port"]), "--properties-file", props]
            d.ui_port = int(p["ui_port"])
            d.url = f"spark://{host}:{p['port']}"
            d.ui_url = f"http://{host}:{p['ui_port']}"
        else:
            ui = int(p.get("ui_port") or d.ui_port or procutil.free_port(p["host"]))
            d.ui_port = ui
            work = Path(p.get("work_dir") or d.dir / "work")
            work.mkdir(parents=True, exist_ok=True)
            args = [WORKER_CLASS, "--host", p["host"], "--cores", str(p["cores"]), "--memory", f"{p['memory_mb']}m", "--work-dir", str(work), "--webui-port", str(ui),
                    "--properties-file", props]
            if p.get("port"):
                args += ["--port", str(p["port"])]
            args.append(p["master_url"])
            d.url = f"http://{p['host']}:{ui}"
            d.ui_url = d.url
        env = {k: os.environ[k] for k in ("LANG", "LC_ALL", "TZ", "SYSTEMROOT", "WINDIR", "COMSPEC", "PATHEXT", "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "no_proxy",
                                          *self.cfg.env_passthrough) if k in os.environ}
        env.update(handle.env([self.cfg.path("runtimes", "_jdk", str(m)) for m in (8, 11, 17, 21)]))
        env.update({"SPARK_CONF_DIR": str(d.dir / "conf"), "SPARK_LOG_DIR": str(d.dir / "logs"), "SPARK_PID_DIR": str(d.dir), "SPARK_DAEMON_MEMORY": self.cfg.daemon_memory,
                    "SPARK_LOCAL_IP": _connect_host(p["host"]), "SPARK_PUBLIC_DNS": p["host"], "SPARK_NO_DAEMONIZE": "1", "SPARK_LOCAL_DIRS": str(d.dir / "local"),
                    "HOME": str(d.dir)})
        if self.worker_id:
            env["OLCF_WORKER_ID"] = str(self.worker_id)
        return [str(launcher), *args], env

    def _launch(self, d: Daemon, handle: RuntimeHandle) -> None:
        cmd, env = self._command(d, handle)
        logf = open(d.dir / "logs" / "daemon.log", "ab")  # noqa: SIM115 - handed to the child, closed below
        try:
            proc = subprocess.Popen(cmd, cwd=d.dir, env=env, stdin=subprocess.DEVNULL, stdout=logf, stderr=subprocess.STDOUT, **procutil.popen_kwargs())  # noqa: S603
        except OSError as exc:
            raise CommandError("spawn_failed", f"cannot launch the Spark {d.role}: {exc}", remediation="Check JAVA_HOME and the runtime (verify/repair).") from exc
        finally:
            logf.close()
        d.proc, d.pid = proc, proc.pid
        try:
            d.create_time = psutil.Process(proc.pid).create_time()
        except psutil.Error:
            d.create_time = None
        d.state, d.detail, d.stop_requested = "starting", "launched", False
        self._persist(d)
        log.info("started spark %s (runtime %s) pid %s", d.role, d.runtime_id, d.pid)

    def _alive(self, d: Daemon) -> bool:
        if d.proc is not None:
            return d.proc.poll() is None
        return procutil.process_matches(d.pid, d.create_time) is not None

    def _persist(self, d: Daemon) -> None:
        doc = {"role": d.role, "runtime_id": d.runtime_id, "ui_port": d.ui_port, "params": {k: v for k, v in d.params.items() if k != "conf"}, "conf_keys": sorted(d.params.get("conf") or {}),
               "pid": d.pid, "create_time": d.create_time, "url": d.url, "ui_url": d.ui_url, "state": d.state, "updated_at": now_iso()}
        tmp = d.dir / "daemon.json.tmp"
        tmp.write_text(json.dumps(doc), encoding="utf-8")
        os.replace(tmp, d.dir / "daemon.json")

    # ------------------------------------------------------------------ health
    def _health(self, d: Daemon) -> tuple[bool, str, dict[str, Any]]:
        """(healthy, detail, extra) from the daemon's own web UI JSON."""
        doc = http_json(f"http://{_connect_host(d.params['host'])}:{d.ui_port}/json/")
        if doc is None:
            return False, "web UI not answering", {}
        if d.role == "master":
            ok = str(doc.get("status", "")).upper() == "ALIVE"
            workers = doc.get("workers") or []
            alive = [w for w in workers if str(w.get("state", "")).upper() == "ALIVE"]
            return ok, f"status {doc.get('status')}, {len(alive)} worker(s) alive", {"alive_workers": len(alive), "cores": doc.get("cores"), "cores_used": doc.get("coresused")}
        # the worker id looks like worker-20260101120000-10.0.0.5-41234: the last component is its RPC port
        wid = str(doc.get("id") or "")
        port = int(wid.rsplit("-", 1)[-1]) if wid.rsplit("-", 1)[-1].isdigit() else None
        return True, f"serving, master {doc.get('masterurl') or d.params['master_url']}", {"port": port, "worker_id": wid, "cores_used": doc.get("coresused")}

    def _wait_ready(self, d: Daemon) -> None:
        end = time.monotonic() + self.cfg.ready_timeout_seconds
        while time.monotonic() < end:
            if not self._alive(d):
                tail = self.log_tail(d.role, d.runtime_id, 3000)
                self._set_state(d, "failed", "the daemon exited during startup")
                raise CommandError("daemon_failed", f"the Spark {d.role} exited during startup:\n{tail}", remediation="Check the daemon log (collect_diagnostics), JAVA_HOME and port availability.")
            ok, detail, extra = self._health(d)
            if ok:
                d.extra = extra
                self._set_state(d, "running", detail)
                return
            time.sleep(0.4)
        tail = self.log_tail(d.role, d.runtime_id, 3000)
        self._stop_daemon(d, 5.0)
        raise CommandError("daemon_timeout", f"the Spark {d.role} did not become healthy within {self.cfg.ready_timeout_seconds:.0f}s:\n{tail}",
                           remediation="Check memory/ports on the node and the daemon log.")

    def _set_state(self, d: Daemon, state: str, detail: str = "") -> None:
        changed = d.state != state or d.detail != detail
        d.state, d.detail = state, detail
        self._persist(d)
        if changed:
            self.out.emit("daemon_state", {"role": d.role, "runtime_id": d.runtime_id, "state": state, "detail": detail, "pid": d.pid, "url": d.url, "ui_url": d.ui_url}, buffer=True)

    # ------------------------------------------------------------------ supervision
    def _ensure_thread(self) -> None:
        with self._lock:
            if self._thread is None or not self._thread.is_alive():
                self._stop.clear()
                self._thread = threading.Thread(target=self._loop, name="daemon-supervisor", daemon=True)
                self._thread.start()

    def _loop(self) -> None:
        while not self._stop.wait(2.0):
            with self._lock:
                ds = [d for d in self.daemons.values() if d.state in ("running", "starting", "restarting")]
            for d in ds:
                try:
                    self._check(d)
                except Exception:  # noqa: BLE001 - supervision must continue
                    log.exception("daemon supervision of %s failed", d.key)

    def _check(self, d: Daemon) -> None:
        if d.stop_requested:
            return
        if not self._alive(d):
            now = time.monotonic()
            if d.state != "restarting":  # just noticed the exit: schedule the relaunch
                d.next_restart_at = now + d.backoff
                self._set_state(d, "restarting", f"process exited, restarting in {d.backoff:.0f}s")
                d.backoff = min(30.0, d.backoff * 2)
                return
            if now < d.next_restart_at:
                return
            # every *launch attempt* counts against the budget, so a daemon that dies during startup again and again ends up "failed"
            # instead of being retried forever
            d.restarts = [t for t in d.restarts if now - t < RESTART_WINDOW]
            if len(d.restarts) >= MAX_RESTARTS:
                self._set_state(d, "failed", f"exited {MAX_RESTARTS} times within {RESTART_WINDOW:.0f}s, giving up")
                return
            d.restarts.append(now)
            try:
                handle = self.runtimes.light_check(d.runtime_id)
                self._launch(d, handle)
                self._wait_ready(d)
                d.backoff = 1.0
            except (CommandError, RuntimeManagerError) as exc:
                self._set_state(d, "restarting", f"restart failed: {str(exc)[:200]}")
                d.next_restart_at = time.monotonic() + d.backoff
                d.backoff = min(30.0, d.backoff * 2)
            return
        ok, detail, extra = self._health(d)
        d.extra = extra
        if ok:
            if d.state != "running" or d.detail != detail:
                self._set_state(d, "running", detail)
        elif d.state == "running":
            self._set_state(d, "running", f"degraded: {detail}")

    def _stop_daemon(self, d: Daemon, grace: float) -> None:
        d.stop_requested = True
        self._set_state(d, "stopping", "stop requested")
        if d.pid:
            procutil.terminate_tree(d.pid, grace=grace, create_time=d.create_time)
        if d.proc is not None:
            try:
                d.proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                pass
        self._set_state(d, "stopped", "stopped")
        d.pid = None

    # ------------------------------------------------------------------ agent lifecycle
    def adopt(self) -> list[str]:
        """Re-attach to daemons of a previous agent process (pid + create time match); relaunch those that are gone but were wanted."""
        out: list[str] = []
        if not self.root.is_dir():
            return out
        for dd in sorted(self.root.iterdir()):
            doc = procutil.read_pidfile(dd / "daemon.json")
            if not doc or doc.get("state") in ("stopped", "stopping", "failed"):
                continue
            params = dict(doc.get("params") or {})
            params["conf"] = self._read_conf(dd)
            d = Daemon(role=doc["role"], runtime_id=doc["runtime_id"], params=params, dir=dd, pid=doc.get("pid"), create_time=doc.get("create_time"),
                       url=doc.get("url"), ui_url=doc.get("ui_url"), ui_port=doc.get("ui_port"))
            if procutil.process_matches(d.pid, d.create_time) is not None:
                d.state, d.detail = "running", "adopted after agent restart"
                with self._lock:
                    self.daemons[d.key] = d
                out.append(d.key)
            else:
                d.state, d.detail = "restarting", "process gone while the agent was down"
                d.next_restart_at = 0.0
                with self._lock:
                    self.daemons[d.key] = d
                out.append(d.key)
        if out:
            self._ensure_thread()
        return out

    def _read_conf(self, dd: Path) -> dict[str, str]:
        conf: dict[str, str] = {}
        try:
            for line in (dd / "conf" / "spark-defaults.conf").read_text(encoding="utf-8").splitlines():
                k, _, v = line.partition(" ")
                if k and k != "spark.worker.cleanup.enabled":
                    conf[k] = v
        except OSError:
            pass
        return conf

    def stop_all(self, *, kill: bool, grace: float = 10.0) -> None:
        """Agent shutdown: stop the supervision thread; terminate the daemons only when ``kill`` (otherwise they keep running)."""
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        if kill:
            with self._lock:
                ds = list(self.daemons.values())
            # workers first: they deregister cleanly while the master is still up
            for d in sorted(ds, key=lambda x: x.role != "worker"):
                if d.state not in ("stopped",):
                    self._stop_daemon(d, grace)

    def purge(self, role: str, runtime_id: str) -> None:
        shutil.rmtree(self.root / f"{role}-{runtime_id}", ignore_errors=True)
