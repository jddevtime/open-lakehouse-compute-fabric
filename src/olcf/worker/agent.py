# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The agent core: control-channel client, hello/reconcile, heartbeat and the command dispatcher (CONTRACTS section 4.2).

Connection model
----------------
One outbound WebSocket (``wss`` pinned to the CA from enrolment, client certificate for mTLS, ``Authorization: Bearer <worker token>``)
runs inside an asyncio loop on the main thread. Everything that blocks (jobs, daemons, kernels, installs, probes) lives in threads that
talk to the loop only through :meth:`Agent.emit`:

* ``emit`` appends a frame to a bounded queue; the sender task drains it while connected. ``buffer=True`` frames (``job_state``,
  ``job_event``, ``daemon_state``) survive a disconnect and are delivered after the next ``hello``; the others (``job_log``,
  ``kernel_event``, ``heartbeat``, command results) are dropped while offline - logs are re-sent from the on-disk spool, kernel events and
  heartbeats are only meaningful live.
* The controller acknowledges every agent frame that carries an ``id`` with ``result{ref=id}`` (``error{ref}`` = rejected). The agent uses
  these acks as the *explicit delivery confirmation* for ``job_log`` chunks and terminal ``job_state`` frames (callbacks passed to ``emit``).
* ``hello`` (sent on **every** (re)connect, carries an ``id`` and is answered with ``result{data: {heartbeat_seconds, drain, server_time}}``)
  reports the full state: active jobs (plus finished-but-unconfirmed ones), kernels, Spark daemons, runtimes, inventory. Right after the
  acknowledged hello the agent re-reports unconfirmed terminal states; the log shippers rewind to their oldest unconfirmed chunk.
* Reconnect: exponential backoff ``backoff_min`` -> ``backoff_max`` with jitter, forever. Auth failures (HTTP 401/403, close code 4401/4403)
  are logged loudly (revoked node / bad token) and retried at the maximum interval.

Commands
--------
``ping, run_job, cancel_job, start_spark_master, stop_spark_master, start_spark_worker, stop_spark_worker, start_kernel, kernel_execute,
kernel_interrupt, kernel_restart, kernel_shutdown, kernel_complete, probe, install_runtime, drain, collect_diagnostics, shutdown`` plus the
additive ``fetch_artifact``. Each is answered with ``result{ref=id}``; a repeated command ``id`` is answered from a small result cache (the
controller may retry after a timeout) and never executed twice. Unexpected exceptions become ``internal`` errors with the stack in the agent log.

Lifecycle
---------
On start the agent adopts orphaned jobs and daemons of a previous process. On SIGINT/SIGTERM/SIGHUP it stops accepting work. Spark Master/Worker
daemons it started are stopped with the agent (unless ``keep_daemons_on_exit``: a rolling restart that must keep the cluster up sets it
and the next agent adopts them); running jobs are left running (and adopted on the next start) unless ``stop_children_on_exit`` (default
for ``--embedded``); kernels always die with the agent. State files: ``agent.pid``, ``agent-status.json`` (read by ``olcf-worker status``), ``drain.json``.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import hashlib
import json
import logging
import os
import random
import shutil
import signal
import ssl
import threading
import time
import urllib.parse
from collections import OrderedDict, deque
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import psutil
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidStatus

from olcf.core.errors import OlcfError
from olcf.core.timeutil import now_iso
from olcf.runtime.errors import RuntimeManagerError
from olcf.runtime.manager import ManagerConfig, RuntimeManager
from olcf.runtime.signing import TrustStore
from olcf.version import WORKER_PROTOCOL_VERSION, __version__
from olcf.worker import diagnostics, identity, inventory, procutil, protocol
from olcf.worker.config import AgentConfig
from olcf.worker.daemons import DaemonManager
from olcf.worker.identity import Identity
from olcf.worker.jobs import JobManager
from olcf.worker.kernels import KernelService
from olcf.worker.layers import LayerManager
from olcf.worker.metrics import MetricsCollector
from olcf.worker.probes import ProbeSuite
from olcf.worker.protocol import CommandError, make_frame

log = logging.getLogger("olcf.worker.agent")

MAX_QUEUE = 20000
MAX_FRAME_FROM_CONTROLLER = 32 * 1024 * 1024  # controller sends up to 24 MiB (run_job with the job source)
#: Frames that need no controller acknowledgement (no ``id``).
ANON_TYPES = frozenset({"heartbeat", "kernel_event", "daemon_state", "job_event", "result", "pong"})
AUTH_CLOSE_CODES = (4401, 4403)


@dataclass
class _Out:
    frame: dict[str, Any]
    buffer: bool
    ack: Callable[[bool], None] | None


class AgentError(OlcfError):
    """The worker agent failed (code ``agent_error``)."""
    code = "agent_error"
    status = 500


class Agent:
    """One agent process: owns the managers and the control channel."""

    def __init__(self, cfg: AgentConfig, ident: Identity, *, runtimes: RuntimeManager | None = None) -> None:
        self.cfg = cfg.resolve()
        self.ident = ident
        self.cfg.ensure_dirs()
        self.runtimes = runtimes or RuntimeManager(self._manager_config())
        self.metrics = MetricsCollector(self.cfg.state_dir)
        self.skew = protocol.ClockSkew()
        self.layers = LayerManager(self.cfg, fetch_file=self._download_env_jar)
        self.jobs = JobManager(self.cfg, self.runtimes, self, extra_allocated=self._jobs_extra, capacity=self.capacity, layers=self.layers)
        self.daemons = DaemonManager(self.cfg, self.runtimes, self, worker_id=ident.worker_id)
        self.kernels = KernelService(self.cfg, self.runtimes, self, self.layers)
        self.kernels.extra_allocated = self._kernels_extra
        self.kernels.capacity = self.capacity
        self.probes = ProbeSuite(state_dir=self.cfg.state_dir, runtimes=self.runtimes, ca_pem=ident.ca_pem, skew=lambda: self.skew.seconds)
        # outbound queue + delivery tracking
        self._q: deque[_Out] = deque()
        self._qlock = threading.Lock()
        self._acks: dict[str, Callable[[bool], None]] = {}
        self._connected = False
        self._conn_gen = 0
        self._hello_wait: tuple[str, asyncio.Future[dict[str, Any]]] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._wake: asyncio.Event | None = None
        self._stop: asyncio.Event | None = None
        self._stopping = False
        self._stop_kill: bool | None = None
        self._stop_daemons: bool | None = None
        self._exit_code = 0
        self._hb_interval = float(ident.heartbeat_seconds or cfg.heartbeat_seconds)
        self._hb_kick: asyncio.Event | None = None
        self._pool = ThreadPoolExecutor(max_workers=16, thread_name_prefix="olcf-cmd")
        self._rpc: dict[str, concurrent.futures.Future[dict[str, Any]]] = {}  # frames whose answer carries data (renew_cert): id -> future of the result payload
        self._credentials_plan: dict[str, Any] | None = None  # the hello acknowledgement's `credentials` block (when to renew)
        self._renew_lock = threading.Lock()
        self._last_renewal: dict[str, Any] | None = None
        self._last_renewal_error: str | None = None
        self._cmd_cache: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._cmd_inflight: set[str] = set()
        self._cmd_tasks: set[asyncio.Task[None]] = set()
        self._started = time.time()
        self._last_hello: str | None = None
        self._last_error: str | None = None
        self._rtt_ms: float | None = None
        self._ws: Any = None
        self._runtimes_cache: tuple[float, list[dict[str, Any]]] = (0.0, [])
        self._housekeeper: threading.Thread | None = None
        self._handlers: dict[str, Callable[[dict[str, Any]], Any]] = {
            "run_job": self.jobs.run_job,
            "cancel_job": self._cmd_cancel_job,
            "start_spark_master": self.daemons.start_master,
            "stop_spark_master": lambda p: self.daemons.stop("master", p),
            "start_spark_worker": self.daemons.start_worker,
            "stop_spark_worker": lambda p: self.daemons.stop("worker", p),
            "start_kernel": self.kernels.start,
            "kernel_execute": self.kernels.execute,
            "kernel_interrupt": self.kernels.interrupt,
            "kernel_restart": self.kernels.restart,
            "kernel_shutdown": self.kernels.shutdown,
            "kernel_complete": self.kernels.complete,
            "probe": self._cmd_probe,
            "install_runtime": self._cmd_install_runtime,
            "drain": self._cmd_drain,
            "collect_diagnostics": self._cmd_diagnostics,
            "shutdown": self._cmd_shutdown,
            "fetch_artifact": self._cmd_fetch_artifact,
            "renew_credentials": self._cmd_renew_credentials,
        }
        self._load_drain()

    # ================================================================== configuration helpers
    def _manager_config(self) -> ManagerConfig:
        trust = TrustStore.from_dir(self.cfg.runtime_trust_dir) if self.cfg.runtime_trust_dir else None
        return ManagerConfig(state_dir=self.cfg.state_dir, trust=trust, maven_repositories=list(self.cfg.maven_repositories) or None, pip_index_url=self.cfg.pip_index_url,
                             pip_find_links=list(self.cfg.pip_find_links), host_python=self.cfg.host_python)

    def capacity(self) -> tuple[int, int]:
        """(cores, memory_mb) this agent may hand out: ``--max-cores/--max-memory-mb`` or the machine (85 % of RAM)."""
        cores = self.cfg.max_cores or psutil.cpu_count(logical=True) or 1
        mem = self.cfg.max_memory_mb or int(psutil.virtual_memory().total / 1024**2 * 0.85)
        return int(cores), int(mem)

    def _jobs_extra(self) -> tuple[int, int]:
        d, k = self.daemons.allocated(), self.kernels.allocated()
        return d[0] + k[0], d[1] + k[1]

    def _kernels_extra(self) -> tuple[int, int]:
        d, j = self.daemons.allocated(), self.jobs.allocated()
        return d[0] + j[0], d[1] + j[1]

    # ================================================================== Outbound interface (used by the managers)
    def connected(self) -> bool:
        return self._connected

    def conn_gen(self) -> int:
        return self._conn_gen

    def outq_depth(self) -> int:
        return len(self._q)

    def emit(self, ftype: str, payload: dict[str, Any], *, buffer: bool = False, ack: Callable[[bool], None] | None = None) -> bool:
        if not self._connected and not buffer:
            return False
        frame = make_frame(ftype, payload, anon=ftype in ANON_TYPES and ack is None)
        return self._enqueue(frame, buffer, ack)

    def _enqueue(self, frame: dict[str, Any], buffer: bool, ack: Callable[[bool], None] | None) -> bool:
        with self._qlock:
            if len(self._q) >= MAX_QUEUE:
                if not buffer:
                    return False
                for i, item in enumerate(self._q):  # make room by dropping the oldest droppable frame (never a terminal job_state)
                    if item.frame["type"] != "job_state":
                        del self._q[i]
                        break
            self._q.append(_Out(frame, buffer, ack))
        self._poke()
        return True

    def _poke(self) -> None:
        loop, wake = self._loop, self._wake
        if loop is not None and wake is not None and not loop.is_closed():
            with contextlib.suppress(RuntimeError):
                loop.call_soon_threadsafe(wake.set)

    # ================================================================== run
    def run(self) -> int:
        """Blocking entry point: returns the process exit code (0 on clean shutdown)."""
        self._guard_single_instance()
        try:
            return asyncio.run(self._main())
        finally:
            with contextlib.suppress(OSError):
                (self.cfg.state_dir / "agent.pid").unlink()
            self._write_status(final=True)

    def _guard_single_instance(self) -> None:
        pf = self.cfg.state_dir / "agent.pid"
        doc = procutil.read_pidfile(pf)
        if doc and doc.get("pid") != os.getpid() and procutil.process_matches(doc.get("pid"), doc.get("create_time")) is not None:
            raise AgentError(f"another agent (pid {doc['pid']}) already uses the state directory {self.cfg.state_dir}",
                             remediation="Stop it with `olcf-worker leave --stop-only` or use a different --state-dir.")
        procutil.write_pidfile(pf, os.getpid(), url=self.ident.url, worker_id=self.ident.worker_id)

    async def _main(self) -> int:
        self._loop = asyncio.get_running_loop()
        self._wake, self._stop, self._hb_kick = asyncio.Event(), asyncio.Event(), asyncio.Event()
        self._install_signal_handlers()
        log.info("agent %s starting as worker %s (protocol %s), state dir %s", __version__, self.ident.worker_id, WORKER_PROTOCOL_VERSION, self.cfg.state_dir)
        await self._loop.run_in_executor(None, self._startup)
        backoff = self.cfg.backoff_min
        try:
            while not self._stop.is_set():
                started = time.monotonic()
                try:
                    await self._connect_once()
                    self._last_error = None
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001 - never leave the loop
                    self._last_error = f"{type(exc).__name__}: {exc}"
                    level = logging.ERROR if self._is_auth_failure(exc) else logging.WARNING
                    log.log(level, "control channel lost: %s", self._last_error)
                    if self._is_auth_failure(exc):
                        backoff = self.cfg.backoff_max
                        log.error("the controller rejected this node's credentials (revoked node, expired token or wrong certificate): "
                                  "re-enrol with a new token (`olcf-worker run --token ...`); retrying every %.0fs", backoff)
                self._set_connected(False)
                if self._stop.is_set():
                    break
                if time.monotonic() - started > 30:
                    backoff = self.cfg.backoff_min  # a healthy session resets the backoff
                delay = backoff * (0.8 + 0.4 * random.random())  # noqa: S311 - jitter only
                log.info("reconnecting in %.1fs", delay)
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._stop.wait(), delay)
                backoff = min(self.cfg.backoff_max, backoff * 2)
        finally:
            await self._loop.run_in_executor(None, self._shutdown_components)
        return self._exit_code

    def _install_signal_handlers(self) -> None:
        def handler(signum: int, _frame: Any = None) -> None:
            log.info("signal %s received: shutting down", signum)
            self.request_stop(f"signal {signum}")

        for name in ("SIGINT", "SIGTERM", "SIGHUP", "SIGBREAK"):
            sig = getattr(signal, name, None)
            if sig is not None:
                with contextlib.suppress(ValueError, OSError):  # not in the main thread (tests) -> caller stops explicitly
                    signal.signal(sig, handler)

    def request_stop(self, reason: str = "requested", *, kill_children: bool | None = None, stop_daemons: bool | None = None) -> None:
        """Thread-safe: leave the connection loop and shut down.

        ``kill_children`` (jobs *and* daemons) and ``stop_daemons`` (daemons only, wins for them) override the configured defaults
        (``stop_children_on_exit`` for jobs, ``keep_daemons_on_exit`` for Spark daemons); ``None`` keeps the configuration.
        """
        log.info("stop requested: %s", reason)
        if not self._stopping or kill_children is not None or stop_daemons is not None:  # a repeated plain signal must not undo an explicit choice
            self._stop_kill = kill_children
            self._stop_daemons = stop_daemons if stop_daemons is not None else kill_children
        self._stopping = True
        loop, stop = self._loop, self._stop
        if loop is not None and stop is not None:
            with contextlib.suppress(RuntimeError):
                loop.call_soon_threadsafe(stop.set)

    def _startup(self) -> None:
        self.kernels.cleanup_stale()
        adopted = self.jobs.adopt_orphans()
        daemons = self.daemons.adopt()
        if adopted or daemons:
            log.info("adopted %d job(s) and %d daemon(s) from a previous agent process", len(adopted), len(daemons))
        self._housekeeper = threading.Thread(target=self._housekeeping, name="olcf-housekeeping", daemon=True)
        self._housekeeper.start()

    def _housekeeping(self) -> None:
        last_clean = 0.0
        while not self._stopping:
            try:
                self.jobs.tick()
                if time.monotonic() - last_clean > 600:
                    last_clean = time.monotonic()
                    self.jobs.cleanup_old()
            except Exception:  # noqa: BLE001
                log.exception("housekeeping failed")
            time.sleep(1.0)

    def _shutdown_components(self) -> None:
        kill = self._stop_kill if self._stop_kill is not None else bool(self.cfg.stop_children_on_exit)
        kill_daemons = self._stop_daemons if self._stop_daemons is not None else not bool(self.cfg.keep_daemons_on_exit)
        log.info("shutting down (stop jobs: %s, stop spark daemons: %s)", kill, kill_daemons)
        self.kernels.stop_all()
        self.jobs.stop(kill_jobs=kill, grace=min(5.0, self.cfg.kill_grace_seconds))
        self.daemons.stop_all(kill=kill_daemons, grace=min(10.0, self.cfg.kill_grace_seconds))
        self._pool.shutdown(wait=False, cancel_futures=True)

    # ================================================================== connection
    def _is_auth_failure(self, exc: BaseException) -> bool:
        if isinstance(exc, InvalidStatus):
            return exc.response.status_code in (401, 403)
        if isinstance(exc, ConnectionClosed) and exc.rcvd is not None:
            return exc.rcvd.code in AUTH_CLOSE_CODES
        return False

    def _ws_url_and_ssl(self) -> tuple[str, ssl.SSLContext | None]:
        url = identity.channel_url_for(self.ident.url or "http://127.0.0.1", self.ident.channel_url)
        u = urllib.parse.urlparse(url)
        if u.scheme == "wss":
            p = identity.identity_paths(self.cfg.state_dir)
            ca = self.ident.ca_pem or (self.cfg.ca_pem.read_text() if self.cfg.ca_pem else None)
            return url, identity.tls_context(ca, cert_file=p["cert"], key_file=p["key"])
        loopback = (u.hostname or "") in ("127.0.0.1", "localhost", "::1")
        if not (loopback or self.cfg.embedded or self.cfg.insecure_ws or str(self.ident.url).startswith("http://")):
            raise AgentError(f"refusing to use the unencrypted control channel {url!r} for a remote https controller",
                             remediation="Use a wss:// channel URL (https controller + CA) or pass insecure_ws=true for lab setups.")
        if not loopback:
            log.warning("control channel %s is not encrypted (ws://); use wss with a CA for production", url)
        return url, None

    async def _connect_once(self) -> None:
        url, sslctx = self._ws_url_and_ssl()
        headers = {"Authorization": f"Bearer {self.ident.worker_token}", "User-Agent": f"olcf-worker/{__version__}"}
        log.info("connecting to %s", url)
        async with connect(url, additional_headers=headers, ssl=sslctx, max_size=MAX_FRAME_FROM_CONTROLLER, ping_interval=self.cfg.ws_ping_seconds, ping_timeout=self.cfg.ws_ping_seconds,
                           open_timeout=15, close_timeout=5, compression=None) as ws:
            self._ws = ws
            try:
                await self._session(ws)
            finally:
                self._ws = None

    async def _session(self, ws: Any) -> None:
        assert self._loop and self._stop and self._wake and self._hb_kick
        self._conn_gen += 1
        gen = self._conn_gen
        with self._qlock:  # volatile frames of the dead connection are meaningless now; buffered ones stay
            self._q = deque(x for x in self._q if x.buffer)
        hello = await self._loop.run_in_executor(None, self._hello_payload)
        frame = make_frame("hello", hello)
        fut: asyncio.Future[dict[str, Any]] = self._loop.create_future()
        self._hello_wait = (frame["id"], fut)
        await ws.send(protocol.encode(frame))
        reader = asyncio.create_task(self._reader(ws), name="olcf-reader")
        try:
            res = await asyncio.wait_for(fut, 20.0)
        except TimeoutError as exc:
            reader.cancel()
            raise AgentError("the controller did not acknowledge hello within 20 s") from exc
        if not res.get("ok", True):
            reader.cancel()
            raise AgentError(f"hello rejected by the controller: {res.get('error')}")
        data = res.get("data") or {}
        self._apply_hello_result(data)
        self._last_hello = now_iso()
        log.info("connected (generation %d); controller says drain=%s, heartbeat=%ss", gen, data.get("drain"), data.get("heartbeat_seconds"))
        self._set_connected(True)
        self.jobs.on_connected()
        tasks = [reader, asyncio.create_task(self._sender(ws), name="olcf-sender"), asyncio.create_task(self._heartbeat_loop(), name="olcf-heartbeat"),
                 asyncio.create_task(self._stop.wait(), name="olcf-stopwait"), asyncio.create_task(self._renewal_loop(gen), name="olcf-renewal")]
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                if t.cancelled():
                    continue
                exc = t.exception()
                if exc is not None:
                    raise exc
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._set_connected(False)
            self._fail_acks()

    def _set_connected(self, value: bool) -> None:
        self._connected = value
        if value:
            self._poke()
        self._write_status()

    def _fail_acks(self) -> None:
        with self._qlock:
            self._acks.clear()  # shippers rewind on the next connection; terminal states are re-sent from the job records
        for fid, fut in list(self._rpc.items()):  # an exchange in flight cannot be answered on a dead connection
            self._rpc.pop(fid, None)
            if not fut.done():
                fut.set_exception(AgentError("the control channel closed before the controller answered"))

    def _apply_hello_result(self, data: dict[str, Any]) -> None:
        hb = data.get("heartbeat_seconds")
        if isinstance(hb, (int, float)) and hb > 0:
            self._hb_interval = float(hb)
        if "server_time" in data:
            self.skew.observe(data["server_time"])
        if "drain" in data:  # the controller is authoritative for scheduling state
            self._set_drain(bool(data["drain"]), persist=True)
        creds = data.get("credentials")
        self._credentials_plan = creds if isinstance(creds, dict) else None

    async def _reader(self, ws: Any) -> None:
        assert self._loop
        async for raw in ws:
            try:
                f = protocol.decode(raw)
            except ValueError as exc:
                log.warning("dropping undecodable frame: %s", exc)
                continue
            self.skew.observe(f.get("ts"))
            ftype = f["type"]
            if ftype == "result":
                self._on_result(f)
            elif ftype == "error":
                self._on_error(f)
            elif ftype in protocol.COMMANDS or ftype == "fetch_artifact" or (f.get("id") and ftype not in protocol.NOTIFICATIONS and ftype not in ("log_ack", "job_ack", "bye")):
                # a frame that carries an id expects an answer: unknown types get a structured ``unsupported_command`` error from _on_command
                t = asyncio.create_task(self._on_command(f))
                self._cmd_tasks.add(t)
                t.add_done_callback(self._cmd_tasks.discard)
            elif ftype == "log_ack":  # additive (not sent by the current controller): explicit cumulative log ack
                p = f.get("payload") or {}
                self.jobs.ack_log(str(p.get("job_id")), str(p.get("stream", "stdout")), int(p.get("seq", -1)))
            elif ftype == "job_ack":
                p = f.get("payload") or {}
                self.jobs.ack(str(p.get("job_id")), p.get("attempt"))
            elif ftype == "bye":
                log.info("controller closed the session: %s", (f.get("payload") or {}).get("reason"))
                return
            else:
                log.debug("ignoring frame of type %r", ftype)

    def _on_result(self, f: dict[str, Any]) -> None:
        ref = f.get("ref") or (f.get("payload") or {}).get("ref")
        hw = self._hello_wait
        if hw and ref == hw[0]:
            if not hw[1].done():
                hw[1].set_result(f.get("payload") or {})
            return
        fut = self._rpc.pop(str(ref), None)
        if fut is not None:
            if not fut.done():
                fut.set_result(f.get("payload") or {})
            return
        with self._qlock:
            ack = self._acks.pop(str(ref), None)
        if ack:
            ok = bool((f.get("payload") or {}).get("ok", True))
            self._run_ack(ack, ok)

    def _on_error(self, f: dict[str, Any]) -> None:
        p = f.get("payload") or {}
        ref = f.get("ref") or p.get("ref")
        log.warning("controller reported a protocol error: %s: %s", p.get("code"), p.get("message"))
        hw = self._hello_wait
        if hw and ref == hw[0] and not hw[1].done():
            hw[1].set_result({"ok": False, "error": p})
            return
        fut = self._rpc.pop(str(ref), None) if ref else None
        if fut is not None and not fut.done():
            fut.set_result({"ok": False, "error": p})
            return
        with self._qlock:
            ack = self._acks.pop(str(ref), None) if ref else None
        if ack:  # rejected for good (e.g. not_your_job): never resend
            self._run_ack(ack, False)

    @staticmethod
    def _run_ack(ack: Callable[[bool], None], ok: bool) -> None:
        try:
            ack(ok)
        except Exception:  # noqa: BLE001
            log.exception("delivery callback failed")

    async def _sender(self, ws: Any) -> None:
        assert self._wake
        while True:
            await self._wake.wait()
            self._wake.clear()
            while True:
                with self._qlock:
                    item = self._q.popleft() if self._q else None
                if item is None:
                    break
                fid = item.frame.get("id")
                if item.ack is not None and fid:
                    with self._qlock:
                        self._acks[fid] = item.ack
                try:
                    await ws.send(protocol.encode(item.frame))
                except ConnectionClosed:
                    if item.buffer:
                        with self._qlock:
                            self._q.appendleft(item)
                    raise

    # ================================================================== hello / heartbeat
    def _runtime_list(self, max_age: float = 5.0) -> list[dict[str, Any]]:
        ts, cached = self._runtimes_cache
        if time.monotonic() - ts > max_age:
            try:
                cached = self.runtimes.list()
            except Exception:  # noqa: BLE001
                log.exception("cannot list runtimes")
            self._runtimes_cache = (time.monotonic(), cached)
        return cached

    def _hello_payload(self) -> dict[str, Any]:
        rts = self._runtime_list(0)
        cap = self.capacity()
        return {"worker_id": self.ident.worker_id, "agent_version": __version__, "protocol": WORKER_PROTOCOL_VERSION,
                "inventory": inventory.collect_inventory(self.cfg.state_dir, labels=self.cfg.labels, pool=self.cfg.pool, runtimes=rts, advertise_host=self.cfg.advertise_host),
                "active_jobs": self.jobs.snapshot(include_undelivered=True), "kernels": self.kernels.snapshot(), "spark_daemons": self.daemons.snapshot(), "runtimes": rts,
                "embedded": bool(self.cfg.embedded), "draining": self.jobs.draining, "capacity": {"cores": cap[0], "memory_mb": cap[1]},
                "allocated": {"cores": self.jobs.allocated()[0] + self._jobs_extra()[0], "memory_mb": self.jobs.allocated()[1] + self._jobs_extra()[1]},
                "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(self._started)), "pool": self.cfg.pool, "labels": self.cfg.labels,
                **({"cert": held} if (held := identity.cert_info(self.cfg.state_dir)) else {})}

    def _components(self, metrics: dict[str, Any], rts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        comps: list[dict[str, Any]] = [{"name": "agent", "status": "ok", "detail": f"olcf-worker {__version__}, up {time.time() - self._started:.0f}s"}]
        link_detail = "connected" + (f", rtt {self._rtt_ms:.0f} ms" if self._rtt_ms is not None else "")
        comps.append({"name": "control-plane-link", "status": "ok", "detail": link_detail})
        pct = float(metrics.get("disk_percent") or 0)
        comps.append({"name": "disk", "status": "down" if pct >= 97 else "degraded" if pct >= 90 else "ok", "detail": f"{metrics.get('disk_free_gb')} GB free ({pct:.0f}% used)"})
        skew = self.skew.seconds
        if skew is not None:
            comps.append({"name": "clock", "status": "ok" if abs(skew) <= 5 else "degraded", "detail": f"skew {skew:+.2f}s vs controller"})
        for r in rts:
            status = {"ready": "ok", "installing": "degraded", "retired": "unknown"}.get(r.get("state", ""), "down")
            comps.append({"name": f"runtime:{r['id']}", "status": status, "detail": r.get("state", "")})
        comps.extend(self.daemons.components())
        return comps

    def _heartbeat_payload(self) -> dict[str, Any]:
        m = self.metrics.collect()
        rts = self._runtime_list()
        return {"ts": now_iso(), "metrics": m, "runtimes": rts, "components": self._components(m, rts), "active_jobs": self.jobs.snapshot(), "spark_daemons": self.daemons.snapshot(),
                "kernels": self.kernels.snapshot(), "draining": self.jobs.draining, "capacity": dict(zip(("cores", "memory_mb"), self.capacity(), strict=True)),
                "allocated": dict(zip(("cores", "memory_mb"), self._total_allocated(), strict=True)), "clock_skew_seconds": self.skew.seconds}

    def _total_allocated(self) -> tuple[int, int]:
        j, e = self.jobs.allocated(), self._jobs_extra()
        return j[0] + e[0], j[1] + e[1]

    # ================================================================== credential renewal (CONTRACTS 4.2 "Credential renewal")
    def rpc(self, ftype: str, payload: dict[str, Any], timeout: float = 60.0) -> dict[str, Any]:
        """Send a frame whose acknowledgement carries data and wait for the controller's ``result`` payload (blocking: call from a worker thread, never the event loop)."""
        if not self._connected:
            raise AgentError("not connected to the control plane")
        frame = make_frame(ftype, payload)
        fut: concurrent.futures.Future[dict[str, Any]] = concurrent.futures.Future()
        self._rpc[frame["id"]] = fut
        if not self._enqueue(frame, False, None):
            self._rpc.pop(frame["id"], None)
            raise AgentError("the outbound queue is full")
        try:
            return fut.result(timeout)
        except concurrent.futures.TimeoutError:
            raise AgentError(f"the controller did not answer {ftype} within {timeout:.0f}s") from None
        finally:
            self._rpc.pop(frame["id"], None)

    def renew_credentials(self, reason: str = "scheduled", *, want_cert: bool | None = None) -> dict[str, Any]:
        """One ``renew_cert`` exchange: new key + CSR (the key never leaves the node), the controller's answer is applied atomically (new certificate for the *next* connection,
        rotated worker token). Raises :class:`CommandError` with the controller's code when it refuses (``renewal_not_due`` ...)."""
        from olcf.security.pki import PKI

        with self._renew_lock:
            sd = self.cfg.state_dir
            held = identity.cert_info(sd)
            if want_cert is None:
                want_cert = held is not None
            payload: dict[str, Any] = {"reason": reason}
            key_pem = None
            if want_cert:
                name = self.ident.name or "worker"
                payload["csr_pem"], key_pem = PKI.generate_csr(name, sans=[name])
            if held:
                payload["cert"] = held
            res = self.rpc("renew_cert", payload)
            if not res.get("ok", True):
                err = res.get("error") or {}
                self._last_renewal_error = f"{err.get('code')}: {err.get('message')}"
                raise CommandError(str(err.get("code") or "renewal_failed"), str(err.get("message") or "the controller refused the renewal"),
                                   remediation=err.get("remediation"), details=err.get("details"))
            data = res.get("data") or {}
            applied = identity.apply_renewal(sd, self.ident, data, key_pem)
            renewed = list(data.get("renewed") or applied)
            self._last_renewal = {"at": now_iso(), "renewed": renewed, "reason": reason, "cert_expires_at": data.get("cert_expires_at"), "token_expires_at": data.get("token_expires_at")}
            self._last_renewal_error = None
            log.info("credentials renewed (%s): certificate valid until %s", ", ".join(renewed) or "nothing", data.get("cert_expires_at"))
            self._write_status()
            return {"renewed": renewed, "cert_expires_at": data.get("cert_expires_at"), "token_expires_at": data.get("token_expires_at"), "renew_after": data.get("renew_after")}

    async def _renewal_loop(self, gen: int) -> None:
        """Per connection: wait until the controller's ``renew_after`` (plus jitter), renew over this channel, schedule the next one. Never ends (it would end the session)."""
        assert self._loop
        plan = self._credentials_plan or {}
        due = protocol.parse_ts(plan.get("renew_after")) if plan.get("auto_renew", True) else None
        failures = 0
        while True:
            if due is None:
                await asyncio.Event().wait()  # nothing to renew on this connection (old controller, automatic renewal off, no credentials to renew)
            delay = max(0.0, (due or 0.0) - time.time()) + random.random() * max(0.0, self.cfg.cert_renew_jitter_seconds)  # noqa: S311 - jitter only
            await asyncio.sleep(delay)
            if gen != self._conn_gen:
                await asyncio.Event().wait()
            try:
                res = await self._loop.run_in_executor(self._pool, self.renew_credentials, "scheduled")
                failures = 0
                due = protocol.parse_ts(res.get("renew_after")) if res.get("renew_after") else None
                if due is not None:
                    due = max(due, time.time() + 60.0)
            except asyncio.CancelledError:
                raise
            except CommandError as exc:
                if exc.code == "renewal_not_due":
                    nxt = protocol.parse_ts((exc.details or {}).get("renew_after"))
                    due = max(nxt or 0.0, time.time() + 60.0)
                elif exc.code == "auto_renew_disabled":
                    due = None
                elif exc.code == "renewal_too_soon":
                    due = time.time() + 60.0
                else:
                    failures += 1
                    log.warning("credential renewal refused: %s", exc.message)
                    due = time.time() + min(3600.0, self.cfg.cert_renew_retry_seconds * 2 ** (failures - 1))
            except Exception as exc:  # noqa: BLE001 - a failed renewal never ends the session; it is retried with a growing delay
                failures += 1
                self._last_renewal_error = f"{type(exc).__name__}: {exc}"
                log.warning("credential renewal failed: %s", exc)
                due = time.time() + min(3600.0, self.cfg.cert_renew_retry_seconds * 2 ** (failures - 1))

    def _cmd_renew_credentials(self, p: dict[str, Any]) -> dict[str, Any]:
        return self.renew_credentials("requested", want_cert=bool(p.get("certificate", True)))

    async def _heartbeat_loop(self) -> None:
        assert self._loop and self._hb_kick
        while True:
            payload = await self._loop.run_in_executor(None, self._heartbeat_payload)
            ws = self._ws
            lat = getattr(ws, "latency", None)
            if isinstance(lat, (int, float)) and lat > 0:
                self._rtt_ms = lat * 1000
            self.emit("heartbeat", payload)
            self._write_status()
            self._hb_kick.clear()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._hb_kick.wait(), self._hb_interval)

    def kick_heartbeat(self) -> None:
        loop, ev = self._loop, self._hb_kick
        self._runtimes_cache = (0.0, self._runtimes_cache[1])
        if loop is not None and ev is not None:
            with contextlib.suppress(RuntimeError):
                loop.call_soon_threadsafe(ev.set)

    # ================================================================== status file (olcf-worker status)
    def status_snapshot(self) -> dict[str, Any]:
        return {"pid": os.getpid(), "worker_id": self.ident.worker_id, "name": self.ident.name, "url": self.ident.url, "channel_url": self.ident.channel_url,
                "connected": self._connected, "connection_generation": self._conn_gen, "last_hello": self._last_hello, "last_error": self._last_error,
                "agent_version": __version__, "protocol": WORKER_PROTOCOL_VERSION, "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(self._started)),
                "updated_at": now_iso(), "draining": self.jobs.draining, "jobs": self.jobs.snapshot(include_undelivered=True), "daemons": self.daemons.snapshot(),
                "kernels": self.kernels.snapshot(), "clock_skew_seconds": self.skew.seconds, "queue_depth": len(self._q),
                "credentials": {"certificate": identity.cert_info(self.cfg.state_dir), "renew_after": (self._credentials_plan or {}).get("renew_after"),
                                "token_expires_at": (self._credentials_plan or {}).get("token_expires_at"), "last_renewal": self._last_renewal,
                                "last_error": self._last_renewal_error}}

    def _write_status(self, final: bool = False) -> None:
        try:
            doc = self.status_snapshot()
            if final:
                doc["connected"] = False
                doc["stopped"] = True
            p = self.cfg.state_dir / "agent-status.json"
            tmp = p.with_suffix(".tmp")
            tmp.write_text(json.dumps(doc, default=str), encoding="utf-8")
            os.replace(tmp, p)
        except OSError:
            pass

    def diagnostic_summary(self) -> dict[str, Any]:
        cfg = {k: str(v) for k, v in vars(self.cfg).items() if k not in ("token",)}
        return {"status": self.status_snapshot(), "config": cfg, "metrics": self.metrics.collect(), "runtimes": self._runtime_list(0), "capacity": self.capacity(),
                "inventory": inventory.collect_inventory(self.cfg.state_dir, labels=self.cfg.labels, pool=self.cfg.pool, advertise_host=self.cfg.advertise_host)}

    # ================================================================== drain
    def _drain_file(self) -> Path:
        return self.cfg.state_dir / "drain.json"

    def _load_drain(self) -> None:
        try:
            on = bool(json.loads(self._drain_file().read_text()).get("draining"))
        except (OSError, ValueError):
            on = False
        self.jobs.draining = self.kernels.draining = on

    def _set_drain(self, on: bool, *, persist: bool = True) -> None:
        self.jobs.draining = self.kernels.draining = on
        if persist:
            try:
                self._drain_file().write_text(json.dumps({"draining": on, "at": now_iso()}))
            except OSError:
                pass

    # ================================================================== command dispatcher
    async def _on_command(self, f: dict[str, Any]) -> None:
        assert self._loop
        cid, ctype = f.get("id"), f["type"]
        payload = f.get("payload") or {}
        if cid:
            if cid in self._cmd_cache:
                self._send_result(self._cmd_cache[cid])
                return
            if cid in self._cmd_inflight:
                return
            self._cmd_inflight.add(cid)
        t0 = time.monotonic()
        try:
            if ctype == "ping":
                frame = protocol.result_ok(cid, {"pong": now_iso(), "worker_id": self.ident.worker_id, "agent_version": __version__, "protocol": WORKER_PROTOCOL_VERSION})
            else:
                handler = self._handlers.get(ctype)
                if handler is None:
                    raise CommandError("unsupported_command", f"the agent does not implement command {ctype!r}", remediation="Upgrade the agent to the version of the control plane.")
                data = await self._loop.run_in_executor(self._pool, handler, payload)
                frame = protocol.result_ok(cid, data if isinstance(data, dict) else {"value": data})
        except OlcfError as exc:
            log.warning("command %s failed: %s", ctype, exc.message if hasattr(exc, "message") else exc)
            frame = protocol.result_err(cid, exc)
        except Exception as exc:  # noqa: BLE001
            log.exception("command %s crashed", ctype)
            frame = protocol.result_err(cid, exc)
        finally:
            if cid:
                self._cmd_inflight.discard(cid)
        log.debug("command %s took %.0f ms", ctype, (time.monotonic() - t0) * 1000)
        if cid:
            self._cmd_cache[cid] = frame
            while len(self._cmd_cache) > 256:
                self._cmd_cache.popitem(last=False)
        self._send_result(frame)

    def _send_result(self, frame: dict[str, Any]) -> None:
        self._enqueue(frame, False, None)

    # ---- individual commands
    def _cmd_cancel_job(self, p: dict[str, Any]) -> dict[str, Any]:
        job_id = str(p.get("job_id") or "")
        if not job_id:
            raise CommandError("bad_request", "cancel_job needs job_id")
        force = bool(p.get("force"))
        grace = 0.0 if force else p.get("grace_seconds")
        return self.jobs.cancel_job(job_id, grace, attempt=p.get("attempt"), force=force)

    def _cmd_drain(self, p: dict[str, Any]) -> dict[str, Any]:
        on = bool(p.get("on", True))
        self._set_drain(on)
        return {"draining": on, "running_jobs": len(self.jobs.active()), "kernels": len(self.kernels.snapshot())}

    def _cmd_shutdown(self, p: dict[str, Any]) -> dict[str, Any]:
        kill = p.get("stop_children")
        daemons = p.get("stop_daemons")
        threading.Timer(0.3, lambda: self.request_stop("shutdown command", kill_children=bool(kill) if kill is not None else None,
                                                      stop_daemons=bool(daemons) if daemons is not None else None)).start()
        return {"stopping": True}

    def _cmd_fetch_artifact(self, p: dict[str, Any]) -> dict[str, Any]:
        return self.jobs.fetch_artifact(str(p.get("job_id") or ""), str(p.get("path") or ""), int(p.get("offset") or 0), int(p.get("length") or 4 * 1024 * 1024))

    def _cmd_diagnostics(self, p: dict[str, Any]) -> dict[str, Any]:
        return diagnostics.collect(self.cfg.state_dir, self.diagnostic_summary(), job_ids=p.get("job_ids"), inline=bool(p.get("inline")))

    def _cmd_probe(self, p: dict[str, Any]) -> dict[str, Any]:
        checks = p.get("checks")
        if not checks:
            u = urllib.parse.urlparse(self.ident.url or "")
            checks = [{"kind": "disk", "name": "disk"}, {"kind": "clock", "name": "clock"}]
            if u.hostname:
                checks += [{"kind": "dns", "name": "control-plane-dns", "host": u.hostname}, {"kind": "tcp", "name": "control-plane-tcp", "host": u.hostname, "port": u.port or (443 if u.scheme == "https" else 80)}]
            checks += [{"kind": "runtime", "name": f"runtime:{r['id']}", "runtime_id": r["id"]} for r in self._runtime_list(0) if r.get("state") == "ready"]
        return self.probes.run(list(checks))

    # ---- install_runtime
    def _cmd_install_runtime(self, p: dict[str, Any]) -> dict[str, Any]:
        mode = str(p.get("mode") or "install")
        manifest = p.get("runtime_manifest") or {}
        rid = str(manifest.get("id") or p.get("runtime_id") or "")
        if mode not in ("install", "repair", "verify", "retire", "rollback"):
            raise CommandError("bad_request", f"unknown install_runtime mode {mode!r}")
        if not rid:
            raise CommandError("bad_request", "install_runtime needs runtime_manifest (or runtime_id)")
        progress = lambda step, detail: log.info("runtime %s %s: %s %s", rid, mode, step, detail)  # noqa: E731
        bundle_path: Path | None = None
        try:
            if p.get("offline_bundle") and mode in ("install", "repair"):
                bundle_path = self._download_bundle(p["offline_bundle"])
            if mode == "install":
                res = self.runtimes.import_bundle(bundle_path, progress=progress, force=bool(p.get("force"))) if bundle_path else self.runtimes.install(manifest, force=bool(p.get("force")), progress=progress)
            elif mode == "verify":
                res = self.runtimes.verify(rid, deep=bool(p.get("deep")))
                res["state"] = "ready"
            elif mode == "repair":
                if bundle_path:
                    res = self.runtimes.import_bundle(bundle_path, progress=progress, force=True)
                else:
                    res = self.runtimes.repair(rid, progress=progress)
            elif mode == "retire":
                res = self.runtimes.retire(rid, purge=bool(p.get("purge")))
            else:
                res = self.runtimes.rollback(rid)
        except RuntimeManagerError:
            self.kick_heartbeat()
            raise
        finally:
            if bundle_path is not None:
                with contextlib.suppress(OSError):
                    bundle_path.unlink()
        self.kick_heartbeat()
        out: dict[str, Any] = {"runtime_id": res.get("runtime_id", rid), "state": res.get("state", "ready"), "verified": mode != "retire", "mode": mode}
        out["fingerprint"] = res.get("fingerprint")
        out["detail"] = res.get("detail") or ("changed" if res.get("changed") else "unchanged" if mode == "install" else mode)
        out["result"] = {k: v for k, v in res.items() if k not in ("smoke", "steps")}
        if "steps" in res:
            out["steps"] = res["steps"]
        if mode == "retire":
            out["state"] = "retired"
        return out

    def _download_bundle(self, spec: dict[str, Any]) -> Path:
        """Fetch an offline bundle from the control plane (token only to the controller's own host), verify size + sha256."""
        url = str(spec.get("url") or "")
        u, c = urllib.parse.urlparse(url), urllib.parse.urlparse(self.ident.url or "")
        if u.scheme not in ("http", "https") or not u.hostname:
            raise CommandError("bad_request", f"offline_bundle.url is not an http(s) URL: {url!r}")
        same_host = (u.hostname, u.port or (443 if u.scheme == "https" else 80)) == (c.hostname, c.port or (443 if c.scheme == "https" else 80))
        headers = {"Authorization": f"Bearer {self.ident.worker_token}"} if same_host else {}
        ca = self.ident.ca_pem
        verify: ssl.SSLContext | bool = identity.tls_context(ca) if (ca and u.scheme == "https") else True
        dest_dir = self.cfg.path("cache", "bundles")
        dest_dir.mkdir(parents=True, exist_ok=True)
        name = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in str(spec.get("filename") or "bundle.tar.gz"))[:100]
        dest = dest_dir / f"{spec.get('bundle_id', 'bundle')}-{name}"
        h = hashlib.sha256()
        size = 0
        try:
            with httpx.stream("GET", url, headers=headers, verify=verify, timeout=httpx.Timeout(30.0, read=300.0), follow_redirects=False) as r, open(dest, "wb") as fh:
                if r.status_code != 200:
                    raise CommandError("bundle_download_failed", f"bundle download failed with HTTP {r.status_code}", remediation="Check that the bundle still exists on the control plane.")
                for chunk in r.iter_bytes(1 << 20):
                    h.update(chunk)
                    size += len(chunk)
                    fh.write(chunk)
        except httpx.HTTPError as exc:
            dest.unlink(missing_ok=True)
            raise CommandError("bundle_download_failed", f"cannot download the offline bundle: {exc}") from exc
        if spec.get("size") is not None and int(spec["size"]) != size:
            dest.unlink(missing_ok=True)
            raise CommandError("bundle_corrupt", f"bundle size {size} does not match the announced {spec['size']}")
        if spec.get("sha256") and h.hexdigest() != str(spec["sha256"]).lower():
            dest.unlink(missing_ok=True)
            raise CommandError("bundle_corrupt", "bundle sha256 does not match the control plane's record", remediation="Re-upload the bundle; never install unverified bundles.")
        return dest

    def _download_env_jar(self, sha256: str, dest: Path) -> None:
        """Fetch a jar that was uploaded to an environment (``jars.files``) from the control plane; the caller verifies the sha256."""
        base = (self.ident.url or "").rstrip("/")
        if not base:
            raise CommandError("jar_unavailable", "the agent does not know the control plane URL")
        ca = self.ident.ca_pem
        verify: ssl.SSLContext | bool = identity.tls_context(ca) if (ca and base.startswith("https")) else True
        headers = {"Authorization": f"Bearer {self.ident.worker_token}"}
        try:
            with httpx.stream("GET", f"{base}/v1/environments/jars/{sha256}", headers=headers, verify=verify, timeout=httpx.Timeout(30.0, read=300.0),
                              follow_redirects=False) as r, open(dest, "wb") as fh:
                if r.status_code != 200:
                    raise CommandError("jar_unavailable", f"environment jar download failed with HTTP {r.status_code}",
                                       remediation="Re-publish the environment (the control plane no longer holds the jar).")
                for chunk in r.iter_bytes(1 << 20):
                    fh.write(chunk)
        except httpx.HTTPError as exc:
            dest.unlink(missing_ok=True)
            raise CommandError("jar_unavailable", f"cannot download the environment jar: {exc}") from exc

    # ================================================================== helpers for tests / embedding
    def stop_blocking(self) -> None:
        self.request_stop("stop_blocking")


def wipe_state(state_dir: Path, *, keep_runtimes: bool = True) -> None:
    """Delete agent state (identity, work, logs, daemons...). Runtimes are kept unless asked otherwise (they are large and reusable)."""
    for child in state_dir.iterdir() if state_dir.is_dir() else []:
        if keep_runtimes and child.name == "runtimes":
            continue
        shutil.rmtree(child, ignore_errors=True) if child.is_dir() else child.unlink(missing_ok=True)


__all__ = ["Agent", "AgentError", "wipe_state"]
