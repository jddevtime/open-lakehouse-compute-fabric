# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""A fake worker agent for the controller tests: a real ``websockets`` client speaking CONTRACTS 4.2 against a live control plane.

It enrols through the real HTTP endpoints (``POST /v1/workers/enrollment-tokens`` + ``/enroll``), opens ``GET /v1/workers/ws`` with the worker token and then behaves
like an agent: ``hello`` (with active jobs / kernels / Spark daemons / runtimes), heartbeats, ``result`` for every command, job execution (``job_state``, ``job_log``
chunks, ``job_event spark_app_started``), cancellation, Spark master/worker daemons (a tiny HTTP server answers ``/json`` like a standalone master), notebook kernels
(``kernel_event`` frames, including the SQL result marker of ``run_spark_sql``), probes, drain, abrupt disconnects and reconnects (undelivered terminal states are
re-reported in ``hello.active_jobs`` and re-sent, exactly like the real agent).

Everything is configurable through :class:`JobPlan` (per job), ``fail_commands`` (reject command types), ``command_delay`` and the ``probe_*`` knobs. The agent runs in its own
thread with its own event loop so tests stay plain synchronous code.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import json
import re
import socket
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import httpx
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


# =============================================================================== fake Spark standalone master (HTTP /json)
class FakeSparkMaster:
    """What ``http://host:ui_port/json`` of a Spark Standalone master returns; workers register through ``start_spark_worker``."""

    def __init__(self, host: str, port: int, ui_port: int) -> None:
        self.host, self.port, self.ui_port = host, port, ui_port
        self.workers: dict[str, dict[str, Any]] = {}
        self.apps: list[dict[str, Any]] = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                if self.path == "/json":  # exactly like Spark 4's master UI: the slash-less path redirects (clients must ask for /json/)
                    self.send_response(302)
                    self.send_header("Location", "/json/")
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                elif self.path.rstrip("/") in ("/json", ""):
                    body = json.dumps(outer.document()).encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                elif self.path.startswith("/proxy-test"):
                    body = b'<html><head></head><body><a href="/jobs/">jobs</a></body></html>'
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                else:
                    self.send_response(404)
                    self.end_headers()

            def log_message(self, *args: Any) -> None:
                return

        self._srv = ThreadingHTTPServer(("127.0.0.1", ui_port), Handler)
        self._srv.daemon_threads = True
        self._thread = threading.Thread(target=self._srv.serve_forever, name=f"fake-spark-master-{ui_port}", daemon=True)
        self._thread.start()

    def document(self) -> dict[str, Any]:
        ws = list(self.workers.values())
        alive = [w for w in ws if w["state"] == "ALIVE"]
        return {
            "url": f"spark://{self.host}:{self.port}", "workers": ws, "aliveworkers": len(alive), "cores": sum(w["cores"] for w in alive), "coresused": 0,
            "memory": sum(w["memory"] for w in alive), "memoryused": 0, "activeapps": self.apps, "completedapps": [], "status": "ALIVE",
        }

    def stop(self) -> None:
        with contextlib.suppress(Exception):
            self._srv.shutdown()
            self._srv.server_close()


# =============================================================================== job behaviour
@dataclass
class JobPlan:
    """How the fake agent runs one job."""

    outcome: str = "succeed"  # succeed | fail | exit | hang | crash (never report; the process vanishes)
    duration: float = 0.3
    logs: list[str] = field(default_factory=lambda: ["fake job started\n", "fake job done\n"])
    stderr: list[str] = field(default_factory=list)
    result: dict[str, Any] = field(default_factory=dict)
    failure: dict[str, Any] | None = None
    exit_code: int | None = None
    spark_app: bool = False
    events: list[tuple[str, dict[str, Any]]] = field(default_factory=list)  # extra job_event frames (name, data)
    on_start: Callable[[dict[str, Any]], None] | None = None
    #: files left in the job directory: name -> bytes. Like the real agent, files up to ``inline_limit`` travel base64-encoded in the terminal job_state;
    #: larger ones are flagged ``truncated`` and served by the ``fetch_artifact`` command.
    artifacts: dict[str, bytes] = field(default_factory=dict)
    inline_limit: int = 2 * 1024 * 1024
    bad_sha: set[str] = field(default_factory=set)  # artifact names whose reported sha256 is wrong


@dataclass
class JobRec:
    spec: dict[str, Any]
    attempt: int
    plan: JobPlan
    state: str = "running"
    task: asyncio.Task[None] | None = None
    cancel: asyncio.Event = field(default_factory=asyncio.Event)
    release: asyncio.Event = field(default_factory=asyncio.Event)
    terminal_payload: dict[str, Any] | None = None
    delivered: bool = False
    seqs: dict[str, int] = field(default_factory=lambda: {"stdout": 0, "stderr": 0})
    cancel_force: bool = False


class FakeAgent:
    def __init__(self, stack: Any, name: str, *, cores: int = 8, memory_mb: int = 16384, pool: str | None = None, labels: dict[str, str] | None = None,
                 runtimes: tuple[str, ...] = ("olcf-rt-2.0.0",), heartbeat: float | None = None, address: str = "127.0.0.1", acks: bool = True,
                 capacity: tuple[int, int] | None = None, csr: bool = True) -> None:
        self.stack, self.name = stack, name
        self.send_csr = csr  # enrol with a certificate signing request (like the real agent) and keep the issued machine certificate
        self.cores, self.memory_mb, self.pool, self.labels = cores, memory_mb, pool, labels or {}
        self.runtimes = list(runtimes)
        self.capacity = capacity  # (cores, memory_mb) the agent advertises as hello.capacity (--max-cores/--max-memory-mb); None = not advertised
        self.heartbeat_seconds = heartbeat
        self.address = address
        self.worker_id: str | None = None
        self.token: str | None = None
        self.channel_url: str | None = None
        self.job_plan: Callable[[dict[str, Any]], JobPlan] = lambda spec: JobPlan()
        self.fail_commands: dict[str, str] = {}
        self.fail_times: dict[str, int] = {}  # reject only the next N commands of a type (absent = always)
        self.command_delay: dict[str, float] = {}
        self.lose_result_of: set[str] = set()  # run the command, then cut the connection instead of answering (once per entry): the result frame is lost in transit
        self.probe_fail: set[str] = set()
        self.sql_results: dict[str, dict[str, Any]] = {}
        self.kernel_fail_code: set[str] = set()
        self.auto_reconnect = False
        self.send_acks = acks
        # observed state ------------------------------------------------------------------------------------------------------------------------------
        self.commands: list[dict[str, Any]] = []
        self.hello_acks: list[dict[str, Any]] = []
        self.jobs: dict[str, JobRec] = {}
        self.kernels: dict[str, dict[str, Any]] = {}
        self.masters: dict[str, FakeSparkMaster] = {}
        self.spark_workers: dict[str, dict[str, Any]] = {}
        self.draining = False
        self.artifact_files: dict[tuple[str, str], bytes] = {}
        self.fetch_calls: list[dict[str, Any]] = []
        self.ghost_jobs: list[dict[str, Any]] = []  # jobs reported in hello/heartbeats that the controller never started
        self.connections = 0
        self.close_code: int | None = None
        self.close_reason: str = ""
        self.rejected = False
        self.errors_received: list[dict[str, Any]] = []
        # credentials (CONTRACTS 4.2 "Credential renewal"): the machine certificate and key, what renewals changed
        self.key_pem: str | None = None
        self.cert_pem: str | None = None
        self.ca_pem: str | None = None
        self.renewals: list[dict[str, Any]] = []  # every renew_cert exchange: {"request": payload, "result": result payload}
        self.previous_tokens: list[str] = []  # worker tokens this agent held before a rotation
        self.report_cert = True  # advertise the held certificate in hello (like the real agent)
        self._rpc: dict[str, Any] = {}
        self._tasks: set[Any] = set()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._ws: Any = None
        self._stop = threading.Event()
        self._connected = threading.Event()
        self._main_future: Any = None

    # ------------------------------------------------------------------------------------------------------------------------------ enrolment
    def enroll(self, *, token_pool: str | None = None, replace: bool = False) -> FakeAgent:
        h = self.stack.admin
        r = self.stack.http.post("/v1/workers/enrollment-tokens", headers=h, json={"name": self.name, "pool": token_pool or self.pool, "ttl_seconds": 600, "labels": self.labels,
                                                                                      "replace_existing": replace})
        assert r.status_code in (200, 201), r.text
        body: dict[str, Any] = {
            "token": r.json()["token"], "name": self.name, "hostname": f"{self.name}.test", "inventory": self.inventory(), "agent_version": "fake-1.0", "protocol": "1.0",
            "labels": {}, "pool": None}
        if self.send_csr:
            from olcf.security.pki import PKI

            csr_pem, key_pem = PKI.generate_csr(self.name, sans=[self.name])
            body["csr_pem"] = csr_pem
        r = self.stack.http.post("/v1/workers/enroll", json=body)
        assert r.status_code in (200, 201), r.text
        d = r.json()
        self.worker_id, self.token, self.channel_url = d["worker_id"], d["worker_token"], d["channel_url"]
        self.ca_pem = d.get("ca_pem")
        if self.send_csr and d.get("client_cert_pem"):
            self.cert_pem, self.key_pem = d["client_cert_pem"], key_pem
        self.heartbeat_seconds = self.heartbeat_seconds or float(d.get("heartbeat_seconds") or 1.0)
        return self

    def inventory(self) -> dict[str, Any]:
        return {"cores": self.cores, "memory_mb": self.memory_mb, "disk_gb": 200, "os": "linux", "arch": "x86_64", "hostname": f"{self.name}.test", "address": self.address,
                "python": "3.12"}

    # ------------------------------------------------------------------------------------------------------------------------------ lifecycle
    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is not None and self._thread is not None and self._thread.is_alive():
            return self._loop
        ready = threading.Event()

        def run() -> None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop
            ready.set()
            try:
                loop.run_forever()
            finally:
                for _ in range(5):
                    pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
                    if not pending:
                        break
                    for t in pending:
                        t.cancel()
                    with contextlib.suppress(BaseException):
                        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                loop.close()

        self._thread = threading.Thread(target=run, name=f"fake-agent-{self.name}", daemon=True)
        self._thread.start()
        ready.wait(5)
        assert self._loop is not None
        return self._loop

    def start(self, wait: float = 15.0, *, auto_reconnect: bool = False) -> FakeAgent:
        """Open the channel (the in-memory agent state - jobs, kernels, daemons - survives :meth:`drop` + :meth:`start`)."""
        assert self.token, "enroll() first"
        self.auto_reconnect = auto_reconnect
        self._stop.clear()
        self._connected.clear()
        loop = self._ensure_loop()
        self._main_future = asyncio.run_coroutine_threadsafe(self._main(), loop)
        if wait:
            self.wait_connected(wait)
        return self

    def wait_connected(self, timeout: float = 15.0) -> None:
        if not self._connected.wait(timeout):
            raise AssertionError(f"fake agent {self.name} did not connect (close_code={self.close_code} {self.close_reason})")

    @property
    def connected(self) -> bool:
        return self._connected.is_set()

    def stop(self) -> None:
        """Disconnect and terminate the agent thread (job tasks included)."""
        self._stop.set()
        loop = self._loop
        if loop is not None and loop.is_running():
            with contextlib.suppress(Exception):
                asyncio.run_coroutine_threadsafe(self._shutdown(), loop).result(5)
            loop.call_soon_threadsafe(loop.stop)
        if self._thread:
            self._thread.join(timeout=10)
        for m in self.masters.values():
            m.stop()
        self.masters.clear()

    def drop(self) -> None:
        """Abruptly kill the TCP connection (no close handshake): the controller sees a vanished worker."""
        loop, ws = self._loop, self._ws
        if loop is None or ws is None:
            return

        def _abort() -> None:
            with contextlib.suppress(Exception):
                ws.transport.abort()

        loop.call_soon_threadsafe(_abort)
        self._connected.clear()
        fut = getattr(self, "_main_future", None)
        if fut is not None and not self.auto_reconnect:
            with contextlib.suppress(Exception):
                fut.result(10)

    def reconnect(self, wait: float = 15.0) -> None:
        """Open a fresh connection after :meth:`drop` keeping all in-memory state."""
        self.start(wait, auto_reconnect=self.auto_reconnect)

    async def _shutdown(self) -> None:
        if self._ws is not None:
            with contextlib.suppress(Exception):
                await self._ws.close()

    async def _main(self) -> None:
        backoff = 0.2
        while not self._stop.is_set():
            try:
                async with connect(self.channel_url, additional_headers={"Authorization": f"Bearer {self.token}"}, max_size=32 * 1024 * 1024, open_timeout=10,
                                   ping_interval=None) as ws:
                    self._ws = ws
                    self.connections += 1
                    backoff = 0.2
                    await self._session(ws)
            except ConnectionClosed as exc:
                self._record_close(exc)
            except Exception as exc:  # noqa: BLE001
                self.close_reason = f"{type(exc).__name__}: {exc}"
            finally:
                self._ws = None
                self._connected.clear()
            if self._stop.is_set() or not self.auto_reconnect:
                return
            await asyncio.sleep(backoff)
            backoff = min(2.0, backoff * 2)

    def _record_close(self, exc: ConnectionClosed) -> None:
        rcvd = exc.rcvd
        if rcvd is not None:
            self.close_code, self.close_reason = rcvd.code, rcvd.reason
        else:
            self.close_code, self.close_reason = 1006, "connection lost"

    # ------------------------------------------------------------------------------------------------------------------------------ session
    def _frame(self, type_: str, payload: dict[str, Any], *, id_: bool = False, ref: str | None = None) -> str:
        f: dict[str, Any] = {"v": 1, "type": type_, "ts": _now(), "payload": payload}
        if id_ and self.send_acks:
            f["id"] = uuid.uuid4().hex
        if ref:
            f["ref"] = ref
        return json.dumps(f)

    async def _send(self, type_: str, payload: dict[str, Any], *, id_: bool = False, ref: str | None = None) -> None:
        ws = self._ws
        if ws is None:
            return
        await ws.send(self._frame(type_, payload, id_=id_, ref=ref))

    def _active_jobs(self, *, undelivered: bool) -> list[dict[str, Any]]:
        out = []
        for jid, r in self.jobs.items():
            if r.state == "running":
                out.append({"job_id": jid, "pid": 4242, "state": "running", "attempt": r.attempt})
            elif undelivered and not r.delivered:
                out.append({"job_id": jid, "pid": 4242, "state": r.state, "attempt": r.attempt})
        return out + list(self.ghost_jobs)

    def _daemons(self) -> list[dict[str, Any]]:
        out = []
        for rid, m in self.masters.items():
            out.append({"role": "master", "runtime_id": rid, "pid": 5000, "url": f"spark://{m.host}:{m.port}", "state": "running", "port": m.port, "ui_port": m.ui_port})
        for rid, w in self.spark_workers.items():
            out.append({"role": "worker", "runtime_id": rid, "pid": 5001, "state": "running", "master_url": w["master_url"], "cores": w["cores"], "memory_mb": w["memory_mb"]})
        return out

    def _runtime_states(self) -> list[dict[str, Any]]:
        # ``olcf-host-python`` is reported the way a real agent reports it (family/python/no_spark), the others like installed Spark runtimes
        return [({"id": r, "state": "ready", "verified": True, "fingerprint": f"sha256:fake-{r}-{self.name}", "family": "host-python", "python": "3.13.5", "no_spark": True,
                  "engines": ["python"]} if r == "olcf-host-python" else {"id": r, "state": "ready", "verified": True, "fingerprint": f"sha256:fake-{r}"}) for r in self.runtimes]

    def _metrics(self) -> dict[str, Any]:
        return {"cpu_percent": 12.5, "load1": 0.4, "mem_total_mb": self.memory_mb, "mem_available_mb": self.memory_mb * 0.7, "disk_total_gb": 200, "disk_free_gb": 150,
                "net_rx_bps": 10.0, "net_tx_bps": 10.0}

    async def _session(self, ws: Any) -> None:
        hello = {"worker_id": self.worker_id, "agent_version": "fake-1.0", "protocol": "1.0", "inventory": self.inventory(), "active_jobs": self._active_jobs(undelivered=True),
                 "kernels": [{"session_id": sid, "pid": 4343, "state": "idle"} for sid in self.kernels], "spark_daemons": self._daemons(), "runtimes": self._runtime_states(),
                 "embedded": False}
        if self.capacity:
            hello["capacity"] = {"cores": self.capacity[0], "memory_mb": self.capacity[1]}
        if self.cert_pem and self.report_cert:
            hello["cert"] = self.cert_info()
        hello_id = uuid.uuid4().hex
        await ws.send(json.dumps({"v": 1, "type": "hello", "id": hello_id, "ts": _now(), "payload": hello}))
        early: list[dict[str, Any]] = []  # commands may legitimately arrive before the hello ack (hello listeners run first on the controller)
        while True:
            ack = json.loads(await asyncio.wait_for(ws.recv(), 10))
            if ack["type"] == "result" and ack.get("ref") == hello_id:
                break
            early.append(ack)
        assert ack["payload"]["ok"], ack
        self.hello_acks.append(ack["payload"]["data"])
        self.draining = bool(ack["payload"]["data"].get("drain"))
        hb_seconds = self.heartbeat_seconds or float(ack["payload"]["data"].get("heartbeat_seconds") or 1.0)
        self._connected.set()
        hb = asyncio.create_task(self._heartbeats(hb_seconds))
        for f in early:
            await self._on_frame(f)
        for r in list(self.jobs.values()):  # re-report unconfirmed terminal states after hello (like the real agent)
            if r.terminal_payload is not None and not r.delivered:
                await self._send("job_state", r.terminal_payload, id_=True)
                r.delivered = True
        try:
            async for raw in ws:
                try:
                    f = json.loads(raw)
                except ValueError:
                    continue
                await self._on_frame(f)
        finally:
            hb.cancel()

    async def _heartbeats(self, every: float) -> None:
        while True:
            await asyncio.sleep(every)
            with contextlib.suppress(Exception):
                await self._send("heartbeat", {"ts": _now(), "metrics": self._metrics(), "runtimes": self._runtime_states(),
                                               "components": [{"name": "agent", "status": "ok", "detail": ""}], "active_jobs": self._active_jobs(undelivered=False),
                                               "spark_daemons": self._daemons()})

    # ------------------------------------------------------------------------------------------------------------------------------ commands
    async def _on_frame(self, f: dict[str, Any]) -> None:
        t = f.get("type")
        if t == "error":
            self.errors_received.append(f.get("payload") or {})
            return
        if t == "result":
            fut = self._rpc.pop(str(f.get("ref") or ""), None)  # the answer to a renew_cert request
            if fut is not None and not fut.done():
                fut.set_result(f.get("payload") or {})
            return  # (other) acks of our own frames
        if t == "renew_credentials":  # it awaits a result frame that this very loop must read: run it beside the loop
            task = asyncio.create_task(self._run_command(f))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
            return
        await self._run_command(f)

    async def _run_command(self, f: dict[str, Any]) -> None:
        t = f.get("type")
        fid = f.get("id")
        payload = f.get("payload") or {}
        self.commands.append({"type": t, "payload": payload, "id": fid, "at": time.time()})
        delay = self.command_delay.get(str(t), 0.0)
        if delay:
            await asyncio.sleep(delay)
        if t in self.fail_commands and self.fail_times.get(str(t), 1 << 30) > 0:
            if str(t) in self.fail_times:
                self.fail_times[str(t)] -= 1
            if fid:
                await self._send("result", {"ok": False, "error": {"code": "injected", "message": self.fail_commands[t]}}, ref=fid)
            return
        handler = getattr(self, f"_cmd_{t}", None)
        if handler is None:
            if fid:
                await self._send("result", {"ok": False, "error": {"code": "unsupported", "message": f"unknown command {t}"}}, ref=fid)
            return
        try:
            data = await handler(payload)
            ok, err = True, None
        except _Reject as exc:
            data, ok, err = None, False, {"code": exc.code, "message": str(exc)}
        if str(t) in self.lose_result_of:
            self.lose_result_of.discard(str(t))
            if self._ws is not None:
                self._ws.transport.abort()
            return
        if fid:
            p: dict[str, Any] = {"ok": ok}
            if ok:
                p["data"] = data if data is not None else {}
            else:
                p["error"] = err
            await self._send("result", p, ref=fid)

    async def _cmd_ping(self, p: dict[str, Any]) -> dict[str, Any]:
        return {"pong": True, "time": _now()}

    async def _cmd_drain(self, p: dict[str, Any]) -> dict[str, Any]:
        self.draining = bool(p.get("on"))
        return {"drain": self.draining}

    async def _cmd_shutdown(self, p: dict[str, Any]) -> dict[str, Any]:
        return {}

    async def _cmd_collect_diagnostics(self, p: dict[str, Any]) -> dict[str, Any]:
        return {"files": []}

    async def _cmd_install_runtime(self, p: dict[str, Any]) -> dict[str, Any]:
        rid = (p.get("runtime_manifest") or {}).get("id") or p.get("runtime_id")
        if rid and rid not in self.runtimes and p.get("mode", "install") in ("install", "repair"):
            self.runtimes.append(rid)
        return {"runtime_id": rid, "state": "ready", "verified": True}

    async def _cmd_probe(self, p: dict[str, Any]) -> dict[str, Any]:
        checks = []
        for c in p.get("checks") or []:
            name = c.get("name") or c.get("kind")
            ok = name not in self.probe_fail and c.get("kind") not in self.probe_fail
            item: dict[str, Any] = {"name": name, "kind": c.get("kind"), "ok": ok, "detail": "ok" if ok else "injected failure", "ms": 1.5}
            if c.get("kind") == "spark_master" and ok:
                m = next((m for m in self.masters.values()), None)
                if m is not None:
                    item["data"] = m.document()
            checks.append(item)
        return {"ok": all(c["ok"] for c in checks), "ms": 3.0, "checks": checks}

    # ---- jobs
    async def _cmd_run_job(self, p: dict[str, Any]) -> dict[str, Any]:
        spec = p["spec"]
        jid, attempt = spec["job_id"], int(spec.get("attempt") or 1)
        cur = self.jobs.get(jid)
        if cur is not None and cur.attempt == attempt and cur.state == "running":
            return {"job_id": jid, "attempt": attempt, "duplicate": True}
        if self.draining:
            raise _Reject("draining", "the node is draining")
        plan = self.job_plan(spec)
        rec = JobRec(spec=spec, attempt=attempt, plan=plan)
        rec.payload = p  # type: ignore[attr-defined]
        self.jobs[jid] = rec
        rec.task = asyncio.get_running_loop().create_task(self._run_job(jid, rec))
        return {"job_id": jid, "attempt": attempt, "pid": 4242}

    async def _log(self, jid: str, rec: JobRec, stream: str, text: str) -> None:
        seq = rec.seqs[stream]
        rec.seqs[stream] += 1
        await self._send("job_log", {"job_id": jid, "stream": stream, "seq": seq, "data": text, "attempt": rec.attempt}, id_=True)

    async def _run_job(self, jid: str, rec: JobRec) -> None:
        plan = rec.plan
        try:
            await self._send("job_state", {"job_id": jid, "state": "running", "attempt": rec.attempt}, id_=True)
            if plan.outcome == "crash":
                await asyncio.sleep(0.3)
                rec.state, rec.delivered = "lost", True  # the process vanishes silently (no further job_state, no longer listed in heartbeats)
                return
            if plan.on_start:
                plan.on_start(rec.spec)
            if plan.spark_app and (rec.spec.get("spark") or {}).get("master"):
                await self._send("job_event", {"job_id": jid, "event": "spark_app_started", "data": {
                    "application_id": f"app-{jid[-8:]}-0000", "ui_url": "http://127.0.0.1:1/never", "app_name": (rec.spec.get("spark") or {}).get("app_name")}})
            for ev in plan.events:  # (name, data) or (name, data, seq): events with a seq are delivered twice, like an agent re-sending after a reconnect
                ev_name, ev_data = ev[0], ev[1]
                body = {"job_id": jid, "event": ev_name, "data": ev_data, "attempt": rec.attempt}
                if len(ev) > 2:
                    body["seq"] = ev[2]
                await self._send("job_event", body)
                if len(ev) > 2:
                    await self._send("job_event", body)
            for line in plan.logs:
                await self._log(jid, rec, "stdout", line)
            for line in plan.stderr:
                await self._log(jid, rec, "stderr", line)
            if plan.outcome == "hang":
                waiters = [asyncio.ensure_future(rec.cancel.wait()), asyncio.ensure_future(rec.release.wait())]
                try:
                    await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
                finally:
                    for w in waiters:
                        w.cancel()
            else:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(rec.cancel.wait(), plan.duration)
            if rec.cancel.is_set():
                self._finish(rec, {"job_id": jid, "state": "cancelled", "attempt": rec.attempt, "exit_code": -15, "result": {}})
            elif plan.outcome == "succeed":
                result: dict[str, Any] = {"rows": 3, **plan.result}
                if plan.artifacts:
                    result["artifacts"] = self._describe_artifacts(jid, plan)
                self._finish(rec, {"job_id": jid, "state": "succeeded", "attempt": rec.attempt, "exit_code": 0, "result": result})
            else:
                failure = plan.failure or {"category": "user-code", "message": "the job failed", "traceback": "Traceback (fake)"}
                self._finish(rec, {"job_id": jid, "state": "failed", "attempt": rec.attempt, "exit_code": plan.exit_code if plan.exit_code is not None else 1,
                                   "result": {}, "failure": failure})
            if self._ws is not None and rec.terminal_payload is not None:
                try:
                    await self._send("job_state", rec.terminal_payload, id_=True)
                    rec.delivered = True
                except Exception:  # noqa: BLE001
                    rec.delivered = False  # re-reported after the next hello
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            rec.delivered = False

    def _describe_artifacts(self, jid: str, plan: JobPlan) -> list[dict[str, Any]]:
        out = []
        for name, data in plan.artifacts.items():
            self.artifact_files[(jid, name)] = data
            digest = hashlib.sha256(data).hexdigest()
            entry: dict[str, Any] = {"name": name, "path": name, "media_type": "application/octet-stream", "size": len(data),
                                     "sha256": ("0" * 64) if name in plan.bad_sha else digest}
            if len(data) <= plan.inline_limit:
                entry["data_b64"] = base64.b64encode(data).decode("ascii")
            else:
                entry["truncated"] = True
            out.append(entry)
        return out

    async def _cmd_fetch_artifact(self, p: dict[str, Any]) -> dict[str, Any]:
        self.fetch_calls.append(dict(p))
        data = self.artifact_files.get((p["job_id"], p["path"]))
        if data is None:
            raise _Reject("not_found", f"no such artifact {p['path']!r}")
        off, length = int(p.get("offset") or 0), int(p.get("length") or 4 * 1024 * 1024)
        chunk = data[off:off + length]
        return {"job_id": p["job_id"], "path": p["path"], "size": len(data), "offset": off, "length": len(chunk), "eof": off + len(chunk) >= len(data),
                "data_b64": base64.b64encode(chunk).decode("ascii")}

    @staticmethod
    def _finish(rec: JobRec, payload: dict[str, Any]) -> None:
        rec.state = payload["state"]
        rec.terminal_payload = payload

    async def _cmd_cancel_job(self, p: dict[str, Any]) -> dict[str, Any]:
        rec = self.jobs.get(p.get("job_id", ""))
        if rec is None or rec.state != "running":
            return {"job_id": p.get("job_id"), "state": rec.state if rec else "unknown"}
        if rec.plan.outcome == "hang" and p.get("force"):
            rec.cancel_force = True
        rec.cancel.set()
        return {"job_id": p["job_id"], "state": "cancelling"}

    # ---- spark daemons
    async def _cmd_start_spark_master(self, p: dict[str, Any]) -> dict[str, Any]:
        rid = p["runtime_id"]
        old = self.masters.pop(rid, None)
        if old:
            old.stop()
        try:
            m = FakeSparkMaster(p.get("host") or self.address, int(p["port"]), int(p["ui_port"]))
        except OSError as exc:
            raise _Reject("port_in_use", f"cannot bind the master UI port {p.get('ui_port')}: {exc}") from exc
        self.masters[rid] = m
        return {"pid": 5000, "url": f"spark://{m.host}:{m.port}", "ui_url": f"http://{m.host}:{m.ui_port}"}

    async def _cmd_stop_spark_master(self, p: dict[str, Any]) -> dict[str, Any]:
        m = self.masters.pop(p["runtime_id"], None)
        if m:
            m.stop()
        return {"stopped": bool(m)}

    async def _cmd_start_spark_worker(self, p: dict[str, Any]) -> dict[str, Any]:
        rid = p["runtime_id"]
        entry = {"master_url": p["master_url"], "cores": int(p["cores"]), "memory_mb": int(p["memory_mb"])}
        self.spark_workers[rid] = entry
        wid = f"worker-{self.name}-{rid}"
        # register with whichever fake master (on any agent) owns that URL
        for m in self.stack.fake_masters():
            if p["master_url"] == f"spark://{m.host}:{m.port}":
                m.workers[wid] = {"id": wid, "host": self.address, "port": 7078, "state": "ALIVE", "cores": entry["cores"], "coresused": 0, "memory": entry["memory_mb"],
                                  "memoryused": 0, "webuiaddress": f"http://{self.address}:8081"}
        return {"pid": 5001, "port": 7078}

    async def _cmd_stop_spark_worker(self, p: dict[str, Any]) -> dict[str, Any]:
        rid = p["runtime_id"]
        w = self.spark_workers.pop(rid, None)
        wid = f"worker-{self.name}-{rid}"
        for m in self.stack.fake_masters():
            m.workers.pop(wid, None)
        return {"stopped": bool(w)}

    # ---- kernels
    async def _cmd_start_kernel(self, p: dict[str, Any]) -> dict[str, Any]:
        sid = p["session_id"]
        if self.draining:
            raise _Reject("draining", "the node is draining")
        self.kernels[sid] = {"spec": p.get("spec"), "count": 0, "payload": p}
        await self._kernel_event(sid, "status", "status", {"state": "idle", "detail": "kernel ready"})
        if p.get("eager_spark"):
            await self._kernel_event(sid, "status", "spark", {"application_id": f"app-{sid[-8:]}-0001", "ui_url": "http://127.0.0.1:1/never", "app_name": f"olcf-session-{sid}"})
        return {"session_id": sid, "pid": 4343}

    async def _kernel_event(self, sid: str, channel: str, msg_type: str, content: dict[str, Any], parent: str | None = None) -> None:
        pl: dict[str, Any] = {"session_id": sid, "channel": channel, "msg_type": msg_type, "content": content}
        if parent:
            pl["parent_msg_id"] = parent
        await self._send("kernel_event", pl)

    async def _cmd_kernel_execute(self, p: dict[str, Any]) -> dict[str, Any]:
        sid = p["session_id"]
        k = self.kernels.get(sid)
        if k is None:
            raise _Reject("no_such_kernel", f"no kernel for session {sid}")
        msg_id, code = p["msg_id"], p.get("code") or ""
        asyncio.get_running_loop().create_task(self._execute(sid, k, msg_id, code))
        return {"msg_id": msg_id}

    async def _execute(self, sid: str, k: dict[str, Any], msg_id: str, code: str) -> None:
        k["count"] += 1
        n = k["count"]
        k["interrupt"] = asyncio.Event()
        await self._kernel_event(sid, "status", "status", {"state": "busy"}, msg_id)
        await asyncio.sleep(0.05)
        m = re.search(r"spark\.sql\(_olcf_json\.loads\((\".*\")\)\)\n", code)
        if m:
            sql = json.loads(json.loads(m.group(1)))
            res = self.sql_results.get(sql) or {"columns": [{"name": "x", "type": "int"}], "rows": [[1], [2], [3]]}
            if sql in self.kernel_fail_code:
                await self._kernel_event(sid, "iopub", "error", {"ename": "AnalysisException", "evalue": "table not found", "traceback": ["AnalysisException: table not found"]}, msg_id)
                await self._kernel_event(sid, "shell", "execute_reply", {"status": "error", "execution_count": n, "ename": "AnalysisException", "evalue": "table not found"}, msg_id)
            else:
                text = "@@OLCF_SQL_RESULT@@" + json.dumps(res) + "\n"
                await self._kernel_event(sid, "iopub", "stream", {"name": "stdout", "text": text}, msg_id)
                await self._kernel_event(sid, "shell", "execute_reply", {"status": "ok", "execution_count": n}, msg_id)
        elif "raise" in code:
            await self._kernel_event(sid, "iopub", "error", {"ename": "ValueError", "evalue": "boom", "traceback": ["ValueError: boom"]}, msg_id)
            await self._kernel_event(sid, "shell", "execute_reply", {"status": "error", "execution_count": n, "ename": "ValueError", "evalue": "boom"}, msg_id)
        elif "sleep-forever" in code:
            await k["interrupt"].wait()
            await self._kernel_event(sid, "iopub", "error", {"ename": "KeyboardInterrupt", "evalue": "", "traceback": ["KeyboardInterrupt"]}, msg_id)
            await self._kernel_event(sid, "shell", "execute_reply", {"status": "error", "execution_count": n, "ename": "KeyboardInterrupt", "evalue": ""}, msg_id)
        else:
            await self._kernel_event(sid, "iopub", "stream", {"name": "stdout", "text": f"out:{code}\n"}, msg_id)
            await self._kernel_event(sid, "iopub", "execute_result", {"data": {"text/plain": str(n)}, "metadata": {}, "execution_count": n}, msg_id)
            await self._kernel_event(sid, "shell", "execute_reply", {"status": "ok", "execution_count": n}, msg_id)
        await self._kernel_event(sid, "status", "status", {"state": "idle"}, msg_id)

    async def _cmd_kernel_interrupt(self, p: dict[str, Any]) -> dict[str, Any]:
        k = self.kernels.get(p["session_id"])
        if k and k.get("interrupt") is not None:
            k["interrupt"].set()
        return {}

    async def _cmd_kernel_restart(self, p: dict[str, Any]) -> dict[str, Any]:
        sid = p["session_id"]
        if sid not in self.kernels:
            raise _Reject("no_such_kernel", f"no kernel for session {sid}")
        self.kernels[sid]["count"] = 0
        await self._kernel_event(sid, "status", "status", {"state": "starting", "detail": "restarting"})
        await self._kernel_event(sid, "status", "status", {"state": "idle", "detail": "kernel restarted"})
        return {}

    async def _cmd_kernel_shutdown(self, p: dict[str, Any]) -> dict[str, Any]:
        self.kernels.pop(p["session_id"], None)
        return {}

    async def _cmd_kernel_complete(self, p: dict[str, Any]) -> dict[str, Any]:
        code, cur = p.get("code") or "", int(p.get("cursor") or 0)
        return {"matches": ["print", "property"], "cursor_start": max(0, cur - 1), "cursor_end": cur, "status": "ok", "code": code}

    # ------------------------------------------------------------------------------------------------------------------------------ credentials
    def cert_info(self) -> dict[str, Any]:
        """What the agent tells the controller about its machine certificate: ``{"serial": hex, "not_after": ISO}``."""
        from cryptography import x509

        c = x509.load_pem_x509_certificate((self.cert_pem or "").encode())
        return {"serial": format(c.serial_number, "x"), "not_after": c.not_valid_after_utc.strftime("%Y-%m-%dT%H:%M:%SZ")}

    async def _renew(self, *, reason: str = "scheduled", csr: bool = True, apply: bool = True, timeout: float = 20.0, csr_pem: str | None = None) -> dict[str, Any]:
        """Send ``renew_cert`` and wait for the controller's ``result``; with ``apply`` keep the new certificate/key/token for the next connection (like the agent)."""
        from olcf.security.pki import PKI

        payload: dict[str, Any] = {"reason": reason}
        key_pem = None
        if csr_pem is not None:
            payload["csr_pem"] = csr_pem
        elif csr:
            payload["csr_pem"], key_pem = PKI.generate_csr(self.name, sans=[self.name])
        if self.cert_pem:
            payload["cert"] = self.cert_info()
        fid = uuid.uuid4().hex
        fut: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._rpc[fid] = fut
        assert self._ws is not None, "not connected"
        await self._ws.send(json.dumps({"v": 1, "type": "renew_cert", "id": fid, "ts": _now(), "payload": payload}))
        try:
            res = await asyncio.wait_for(fut, timeout)
        finally:
            self._rpc.pop(fid, None)
        self.renewals.append({"request": payload, "result": res})
        if apply and res.get("ok"):
            d = res.get("data") or {}
            if d.get("cert_pem") and key_pem:
                self.cert_pem, self.key_pem = d["cert_pem"], key_pem
            if d.get("worker_token"):
                self.previous_tokens.append(self.token or "")
                self.token = d["worker_token"]  # the next connection authenticates with it
        return res

    def renew(self, *, reason: str = "scheduled", csr: bool = True, apply: bool = True, timeout: float = 20.0, csr_pem: str | None = None) -> dict[str, Any]:
        """Run a ``renew_cert`` exchange on the live channel (from the test thread); returns the ``result`` payload (``ok`` + ``data`` | ``error``)."""
        loop = self._loop
        assert loop is not None and self.connected, "the agent must be connected"
        return asyncio.run_coroutine_threadsafe(self._renew(reason=reason, csr=csr, apply=apply, timeout=timeout, csr_pem=csr_pem), loop).result(timeout + 5)

    async def _cmd_renew_credentials(self, p: dict[str, Any]) -> dict[str, Any]:
        """The operator asked this node to renew its credentials now: run the exchange and report what changed."""
        res = await self._renew(reason="requested")
        if not res.get("ok"):
            err = res.get("error") or {}
            raise _Reject(str(err.get("code") or "renewal_failed"), str(err.get("message") or "renewal failed"))
        d = res.get("data") or {}
        return {"renewed": d.get("renewed") or [], "cert_expires_at": d.get("cert_expires_at"), "token_expires_at": d.get("token_expires_at")}

    # ------------------------------------------------------------------------------------------------------------------------------ test helpers
    def commands_of(self, type_: str) -> list[dict[str, Any]]:
        return [c for c in list(self.commands) if c["type"] == type_]

    def command_types(self) -> list[str]:
        return [c["type"] for c in list(self.commands)]

    def run_job_specs(self) -> list[dict[str, Any]]:
        return [c["payload"]["spec"] for c in self.commands_of("run_job")]

    def finish_hung(self, job_id: str, outcome: str = "succeed") -> None:
        """Let a 'hang' job end on its own with the given outcome (the plan decides)."""
        loop = self._loop
        rec = self.jobs[job_id]
        rec.plan.outcome = outcome
        assert loop is not None
        loop.call_soon_threadsafe(rec.release.set)


class _Reject(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def wait_until(fn: Callable[[], Any], timeout: float = 20.0, interval: float = 0.05, message: str = "condition") -> Any:
    """Poll ``fn`` until it returns a truthy value (returned) or fail with ``message`` after ``timeout`` seconds."""
    end = time.monotonic() + timeout
    last: Any = None
    while time.monotonic() < end:
        try:
            last = fn()
        except AssertionError as exc:
            last = exc
        else:
            if last:
                return last
        time.sleep(interval)
    raise AssertionError(f"timed out after {timeout:.0f}s waiting for {message} (last: {last!r})")


def wait_for_agent(fn: Callable[[], Any], log: Any, timeout: float, interval: float, message: str) -> Any:
    """:func:`wait_until` for an agent process: a timeout carries the tail of the agent's own log, so a CI failure says why it never came online."""
    try:
        return wait_until(fn, timeout, interval, message)
    except AssertionError as exc:
        try:
            tail = open(log, "rb").read()[-14000:].decode("utf-8", "replace")  # noqa: SIM115, PTH123 - best effort
        except OSError as err:
            tail = f"(no agent log: {err})"
        raise AssertionError(f"{exc}\n--- agent log (last 14000 bytes) ---\n{tail}") from None


def http_get(url: str, **kw: Any) -> httpx.Response:
    return httpx.get(url, timeout=5, **kw)
