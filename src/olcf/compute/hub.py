# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The worker hub: enrollment, the agent WebSocket channel, request/response correlation and liveness.

Responsibilities (spec sections 13, 27; CONTRACTS 4.2)
------------------------------------------------------
* **Enrollment** - single-use enrollment tokens, CSR signing with the built-in CA, worker principal + worker token.
* **Channel** - ``GET /v1/workers/ws``: bearer worker token (and the client certificate CN when
  ``tls.require_client_cert_for_workers``), certificate revocation list, one connection per worker (a newer connection
  replaces an older one), strict frame validation, per-worker bounded send queue (backpressure), idle watchdog.
* **Requests** - ``request()``/``request_async()`` send a command and wait for ``result{ref}`` with a timeout. A worker that is
  connected to *another* API replica is reached through the ``worker_commands`` outbox table so the leader can schedule onto
  any worker regardless of which replica terminates its socket.
* **State** - ``hello`` is the reconcile point: inventory/runtimes/daemons are stored and registered listeners (jobs,
  sessions, Spark clusters) adopt or repair whatever the agent reports. Heartbeats refresh metrics and liveness; per-node
  ring buffers keep the last 120 samples.
* **Logs** - ``job_log`` chunks go straight to the log store (disk), never accumulated in memory.

Only socket-bound state lives in memory; everything the scheduler needs is persisted in the ``workers`` table.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import threading
import time
import urllib.parse
from collections import deque
from collections.abc import Callable
from typing import Any

from fastapi import APIRouter, Request, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field
from sqlalchemy import select, update

from olcf.compute import protocol as P
from olcf.compute import schema as C
from olcf.compute.errors import WorkerBusy, WorkerCommandFailed, WorkerOffline, WorkerTimeout
from olcf.compute.logstore import LogStore
from olcf.compute.registry import HISTORY_SAMPLES, ComputeRegistry, assess_health, summarize_metrics
from olcf.core.context import AppContext
from olcf.core.errors import BadRequest, Conflict, OlcfError, Unauthenticated
from olcf.core.ids import new_id
from olcf.core.states import WorkerState
from olcf.core.timeutil import from_iso, iso_in, now_iso
from olcf.db import schema as S
from olcf.db.util import fetch_all, fetch_one, insert_row, new_etag
from olcf.security.rbac import Principal

log = logging.getLogger("olcf.compute.hub")

SEND_QUEUE_MAX = 512
HELLO_TIMEOUT_SECONDS = 15.0
OUTBOX_POLL_SECONDS = 0.25
PERSIST_EVERY_SECONDS = 2.0
CLOSE_PROTOCOL, CLOSE_UNAUTH, CLOSE_FORBIDDEN, CLOSE_TIMEOUT, CLOSE_REPLACED = 4400, 4401, 4403, 4408, 4409
RESERVED_LABEL_PREFIX = "olcf.io/"

Listener = Callable[[str, Any, P.Frame], None]


# =========================================================================== enrollment models
class EnrollmentTokenCreate(BaseModel):
    """Request body that creates a single-use enrollment token, optionally fixing the worker's name, pool and labels."""
    name: str | None = Field(None, max_length=128, description="Suggested worker name (the agent may override it)")
    ttl_seconds: int = Field(3600, ge=60, le=7 * 86400)
    pool: str | None = None
    labels: dict[str, str] = Field(default_factory=dict)
    replace_existing: bool = Field(False, description="Allow re-enrolling a worker with the same name (rotates its credentials)")


class EnrollRequest(BaseModel):
    """Request body an agent sends to exchange an enrollment token for a worker token (and a client certificate, when it sends a certificate request).
    """
    token: str
    name: str | None = Field(None, max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
    hostname: str | None = Field(None, max_length=255)
    csr_pem: str | None = Field(None, max_length=16384)
    inventory: dict[str, Any] = Field(default_factory=dict)
    agent_version: str = "unknown"
    protocol: str = P.WORKER_PROTOCOL_VERSION
    labels: dict[str, str] = Field(default_factory=dict)
    pool: str | None = None
    embedded: bool = False


def ws_base(ctx: AppContext, request: Request | None) -> str:
    if ctx.settings.server.public_url:
        base = ctx.settings.server.public_url.rstrip("/")
    elif request is not None and request.headers.get("host"):
        base = f"{request.url.scheme}://{request.headers['host']}"
    else:
        base = ctx.settings.base_url
    return base.replace("https://", "wss://", 1).replace("http://", "ws://", 1)


class WorkerConnection:
    """One live agent channel. All coroutine methods run on the hub's event loop."""

    def __init__(self, hub: WorkerHub, worker_id: str, name: str, websocket: WebSocket, ip: str | None) -> None:
        self.hub, self.worker_id, self.name, self.ws, self.ip = hub, worker_id, name, websocket, ip
        self.send_q: asyncio.Queue[P.Frame | None] = asyncio.Queue(maxsize=SEND_QUEUE_MAX)
        self.pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self.closed = False
        self.connected_at = time.time()
        self.last_rx = time.monotonic()
        self.last_persist = 0.0
        self.verified_jobs: deque[str] = deque(maxlen=512)
        self.history: dict[str, list[Any]] = {"cpu": [], "mem": [], "ts": []}
        self.sender: asyncio.Task[None] | None = None
        self.token_id: str | None = None  # the worker-token row this connection authenticated with

    async def send(self, frame: P.Frame, timeout: float = 10.0) -> None:
        if self.closed:
            raise WorkerOffline(f"worker '{self.name}' is not connected")
        try:
            await asyncio.wait_for(self.send_q.put(frame), timeout)
        except TimeoutError:
            raise WorkerBusy(f"worker '{self.name}' is not draining its send queue ({SEND_QUEUE_MAX} frames queued)") from None

    async def request(self, frame: P.Frame, timeout: float) -> dict[str, Any]:
        assert frame.id
        fut: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self.pending[frame.id] = fut
        try:
            await self.send(frame, min(timeout, 10.0))
            return await asyncio.wait_for(fut, timeout)
        except TimeoutError:
            raise WorkerTimeout(f"worker '{self.name}' did not answer '{frame.type}' within {timeout:.0f}s") from None
        finally:
            self.pending.pop(frame.id, None)

    async def _sender_loop(self) -> None:
        try:
            while True:
                frame = await self.send_q.get()
                if frame is None:
                    return
                text = frame.encode()
                if len(text) > P.MAX_CONTROLLER_FRAME_BYTES:
                    fut = self.pending.get(frame.id or "")
                    if fut and not fut.done():
                        fut.set_exception(BadRequest("command payload too large for the worker channel; stage it as an artifact"))
                    continue
                await self.ws.send_text(text)
        except Exception:  # noqa: BLE001 - socket died; the receive loop notices and cleans up
            self.closed = True

    def fail_pending(self, exc: Exception) -> None:
        for fut in list(self.pending.values()):
            if not fut.done():
                fut.set_exception(exc)
        self.pending.clear()


class WorkerHub:
    """Owns agent connections of *this* API replica and the cross-replica command outbox."""

    def __init__(self, ctx: AppContext, registry: ComputeRegistry, logs: LogStore) -> None:
        self.ctx, self.registry, self.logs = ctx, registry, logs
        self._conns: dict[str, WorkerConnection] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread: int | None = None
        self._listeners: dict[str, list[Listener]] = {}
        self._hello_listeners: list[Callable[[dict[str, Any], P.Hello], None]] = []
        self._inventory_listeners: list[Callable[[str], None]] = []
        self._tasks: list[asyncio.Task[Any]] = []
        self._lock = threading.RLock()
        self.job_owner_lookup: Callable[[str], str | None] | None = None
        self.credentials: Any = None  # WorkerCredentials (set by the compute module): certificate/token renewal, expiry, rotation grace
        self.stats = {"frames_in": 0, "frames_out": 0, "protocol_errors": 0, "connections": 0, "rejected": 0}

    # ------------------------------------------------------------------ lifecycle
    def start(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        """Bind to the event loop and start the outbox poller. Safe to call again (no-op when bound)."""
        if self._loop is not None:
            return
        try:
            self._loop = loop or asyncio.get_running_loop()
        except RuntimeError:
            return  # bound lazily on the first websocket
        self._loop_thread = threading.get_ident() if loop is None else None  # thread that runs the loop (blocking requests are refused there)
        self._tasks.append(self._loop.create_task(self._outbox_loop()))

    def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        self._tasks.clear()
        loop = self._loop
        if loop is not None:
            for conn in list(self._conns.values()):
                try:
                    asyncio.run_coroutine_threadsafe(self._close_conn(conn, 1001, "controller shutting down"), loop)
                except RuntimeError:
                    pass

    async def _ensure_bound(self) -> None:
        if self._loop is None:
            self._loop = asyncio.get_running_loop()
            self._loop_thread = threading.get_ident()
            self._tasks.append(self._loop.create_task(self._outbox_loop()))

    # ------------------------------------------------------------------ registration
    def on(self, type_: str, fn: Listener) -> None:
        """Register a *synchronous* handler (executed in a worker thread) for an agent frame type."""
        self._listeners.setdefault(type_, []).append(fn)

    def on_hello(self, fn: Callable[[dict[str, Any], P.Hello], None]) -> None:
        self._hello_listeners.append(fn)

    def on_inventory_change(self, fn: Callable[[str], None]) -> None:
        """``fn(worker_id)`` runs when a worker's advertised runtimes changed (heartbeat) - hello listeners cover connects."""
        self._inventory_listeners.append(fn)

    # ------------------------------------------------------------------ inspection
    def is_connected(self, worker_id: str) -> bool:
        c = self._conns.get(worker_id)
        return bool(c and not c.closed)

    def connected_ids(self) -> set[str]:
        return {k for k, c in self._conns.items() if not c.closed}

    # ------------------------------------------------------------------ requests
    def request(self, worker_id: str, type_: str, payload: dict[str, Any] | None = None, *, timeout: float = 30.0) -> Any:
        """Send a command to a worker and return the agent's ``data``. Blocking; never call from the event-loop thread."""
        frame = P.command(type_, payload)
        conn = self._conns.get(worker_id)
        loop = self._loop
        if conn is not None and loop is not None and not conn.closed:
            if self._loop_thread is not None and threading.get_ident() == self._loop_thread:
                raise RuntimeError("WorkerHub.request() blocks; use request_async() inside the event loop")
            fut = asyncio.run_coroutine_threadsafe(conn.request(frame, timeout), loop)
            try:
                res = fut.result(timeout + 10)
            except concurrent.futures.TimeoutError:
                fut.cancel()
                raise WorkerTimeout(f"worker {worker_id} did not answer '{type_}' within {timeout:.0f}s") from None
            self.stats["frames_out"] += 1
            return self._unwrap(res, type_, worker_id)
        return self._request_via_outbox(worker_id, type_, payload or {}, timeout)

    async def request_async(self, worker_id: str, type_: str, payload: dict[str, Any] | None = None, *, timeout: float = 30.0) -> Any:
        conn = self._conns.get(worker_id)
        if conn is None or conn.closed:
            return await asyncio.to_thread(self._request_via_outbox, worker_id, type_, payload or {}, timeout)
        res = await conn.request(P.command(type_, payload), timeout)
        self.stats["frames_out"] += 1
        return self._unwrap(res, type_, worker_id)

    def notify(self, worker_id: str, type_: str, payload: dict[str, Any] | None = None) -> None:
        """Fire-and-forget command (the agent's ``result`` is ignored)."""
        try:
            self.request(worker_id, type_, payload, timeout=10.0)
        except OlcfError as exc:
            log.debug("notify %s -> %s failed: %s", type_, worker_id, exc.message)

    @staticmethod
    def _unwrap(res: dict[str, Any], type_: str, worker_id: str) -> Any:
        if res.get("ok", False):
            return res.get("data")
        err = res.get("error")
        msg = err.get("message") if isinstance(err, dict) else str(err)
        raise WorkerCommandFailed(f"worker {worker_id} rejected '{type_}': {msg}", details={"worker_id": worker_id, "command": type_, "error": err})

    # ------------------------------------------------------------------ cross-replica outbox
    def _request_via_outbox(self, worker_id: str, type_: str, payload: dict[str, Any], timeout: float) -> Any:
        row = self.registry.get_worker(worker_id)
        node = row.get("connected_node")
        fresh = row.get("last_seen_at") and (time.time() - from_iso(row["last_seen_at"]).timestamp()) < self.ctx.settings.compute.worker_offline_after_seconds
        if not row.get("connected") or not node or node == self.ctx.node_id or not fresh:
            raise WorkerOffline(f"worker '{row['name']}' is not connected", remediation="check the agent service on that machine and its network path to the control plane")
        cid = new_id("cmd")
        with self.ctx.db.begin() as conn:
            insert_row(conn, C.worker_commands, {"id": cid, "worker_id": worker_id, "node_id": node, "type": type_, "payload": payload,
                                                 "timeout_seconds": float(timeout), "state": "pending", "result": None, "error": None,
                                                 "created_at": now_iso(), "expires_at": iso_in(timeout + 30)})
        deadline = time.monotonic() + timeout + 5
        c = C.worker_commands.c
        while time.monotonic() < deadline:
            with self.ctx.db.connect() as conn:
                r = fetch_one(conn, select(c.state, c.result, c.error).where(c.id == cid))
            if r and r["state"] == "done":
                return self._unwrap(r["result"] or {"ok": True}, type_, worker_id)
            if r and r["state"] == "failed":
                err = r["error"] or {}
                code = err.get("code")
                msg = err.get("message", "command failed")
                if code == "worker_timeout":
                    raise WorkerTimeout(msg)
                if code == "worker_offline":
                    raise WorkerOffline(msg)
                raise WorkerCommandFailed(msg, details=err)
            time.sleep(0.05)
        with self.ctx.db.begin() as conn:
            conn.execute(update(C.worker_commands).where(c.id == cid, c.state == "pending").values(state="failed", error={"code": "worker_timeout", "message": "no replica delivered the command in time"}))
        raise WorkerTimeout(f"command '{type_}' to worker {worker_id} timed out (cross-replica delivery)")

    async def _outbox_loop(self) -> None:
        purge_at = 0.0
        while True:
            try:
                await asyncio.sleep(OUTBOX_POLL_SECONDS)
                if not self._conns:
                    continue
                rows = await asyncio.to_thread(self._claim_outbox)
                for r in rows:
                    self._tasks.append(asyncio.create_task(self._deliver(r)))
                self._tasks = [t for t in self._tasks if not t.done()]
                if time.monotonic() > purge_at:
                    purge_at = time.monotonic() + 60
                    await asyncio.to_thread(self._purge_outbox)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                log.exception("worker outbox loop failed")
                await asyncio.sleep(1.0)

    def _claim_outbox(self) -> list[dict[str, Any]]:
        c = C.worker_commands.c
        with self.ctx.db.begin() as conn:
            rows = fetch_all(conn, select(C.worker_commands).where(c.node_id == self.ctx.node_id, c.state == "pending").order_by(c.created_at).limit(50))
            claimed = []
            for r in rows:
                if conn.execute(update(C.worker_commands).where(c.id == r["id"], c.state == "pending").values(state="sent")).rowcount:
                    claimed.append(r)
        return claimed

    def _purge_outbox(self) -> None:
        with self.ctx.db.begin() as conn:
            conn.execute(C.worker_commands.delete().where(C.worker_commands.c.expires_at < now_iso()))

    async def _deliver(self, r: dict[str, Any]) -> None:
        conn = self._conns.get(r["worker_id"])
        try:
            if conn is None or conn.closed:
                raise WorkerOffline(f"worker {r['worker_id']} is no longer connected to node {self.ctx.node_id}")
            res = await conn.request(P.command(r["type"], r["payload"]), float(r["timeout_seconds"]))
            vals: dict[str, Any] = {"state": "done", "result": res}
        except OlcfError as exc:
            vals = {"state": "failed", "error": exc.to_dict()}
        except Exception as exc:  # noqa: BLE001
            vals = {"state": "failed", "error": {"code": "internal", "message": f"{type(exc).__name__}: {exc}"}}
        await asyncio.to_thread(self._finish_outbox, r["id"], vals)

    def _finish_outbox(self, cid: str, vals: dict[str, Any]) -> None:
        with self.ctx.db.begin() as conn:
            conn.execute(update(C.worker_commands).where(C.worker_commands.c.id == cid).values(**vals))

    # ------------------------------------------------------------------ closing
    async def _close_conn(self, conn: WorkerConnection, code: int, reason: str) -> None:
        if conn.closed and conn.sender is None:
            return
        conn.closed = True
        conn.fail_pending(WorkerOffline(f"worker '{conn.name}' disconnected: {reason}"))
        try:
            await conn.ws.close(code=code, reason=reason[:120])
        except Exception:  # noqa: BLE001
            pass

    def disconnect(self, worker_id: str, code: int = CLOSE_FORBIDDEN, reason: str = "closed by controller") -> None:
        """Close a worker's channel (thread-safe)."""
        conn, loop = self._conns.get(worker_id), self._loop
        if conn is not None and loop is not None:
            try:
                asyncio.run_coroutine_threadsafe(self._close_conn(conn, code, reason), loop)
            except RuntimeError:
                pass

    # ================================================================== the websocket endpoint
    async def handle_websocket(self, ws: WebSocket) -> None:
        await self._ensure_bound()
        ctx = self.ctx
        await ws.accept()
        ip = ws.client.host if ws.client else None
        try:
            worker = await asyncio.to_thread(self._authenticate, ws)
        except Unauthenticated as exc:
            self.stats["rejected"] += 1
            log.warning("worker channel rejected from %s: %s", ip, exc.message)
            await ws.close(code=CLOSE_UNAUTH, reason=exc.message[:120])
            return
        except OlcfError as exc:
            self.stats["rejected"] += 1
            log.warning("worker channel forbidden from %s: %s", ip, exc.message)
            await ws.close(code=CLOSE_FORBIDDEN, reason=exc.message[:120])
            return
        wid, name = worker["id"], worker["name"]
        conn = WorkerConnection(self, wid, name, ws, ip)
        conn.token_id = worker.get("_auth_token_id")
        # ---- hello must come first
        try:
            raw = await asyncio.wait_for(ws.receive_text(), HELLO_TIMEOUT_SECONDS)
            frame = P.decode(raw)
            if frame.type != "hello":
                raise P.ProtocolError("the first frame must be 'hello'", fatal=True)
            hello = P.parse_payload(frame)
            assert isinstance(hello, P.Hello)
            if hello.worker_id and hello.worker_id != wid:
                await asyncio.to_thread(ctx.audit.record, None, "worker.identity_mismatch", resource_type="worker", resource_id=wid, outcome="failure",
                                        details={"claimed": hello.worker_id, "ip": ip})
                raise P.ProtocolError("hello.worker_id does not match the authenticated worker", code="identity_mismatch", fatal=True)
            major = hello.protocol.split(".")[0]
            if major != P.WORKER_PROTOCOL_VERSION.split(".")[0]:
                raise P.ProtocolError(f"unsupported protocol {hello.protocol}; controller speaks {P.WORKER_PROTOCOL_VERSION}", code="unsupported_protocol", fatal=True)
        except TimeoutError:
            await ws.close(code=CLOSE_TIMEOUT, reason="hello timeout")
            return
        except P.ProtocolError as exc:
            self.stats["protocol_errors"] += 1
            await ws.close(code=CLOSE_PROTOCOL if exc.code != "identity_mismatch" else CLOSE_FORBIDDEN, reason=exc.message[:120])
            return
        except WebSocketDisconnect:
            return
        # ---- single connection per worker: the newest wins
        old = self._conns.get(wid)
        if old is not None:
            await self._close_conn(old, CLOSE_REPLACED, "replaced by a newer connection")
        self._conns[wid] = conn
        self.stats["connections"] += 1
        conn.sender = asyncio.create_task(conn._sender_loop())
        try:
            row = await asyncio.to_thread(self._process_hello, worker, hello, ip, conn)
            if frame.id:
                ack: dict[str, Any] = {"worker_id": wid, "name": name, "protocol": P.WORKER_PROTOCOL_VERSION, "heartbeat_seconds": ctx.settings.compute.worker_heartbeat_seconds,
                                       "server_time": now_iso(), "drain": row["scheduling"] != "active"}
                if self.credentials is not None:
                    ack["credentials"] = await asyncio.to_thread(self.credentials.hello_block, row)  # when to renew the certificate / rotate the token
                await conn.send(P.result_frame(frame.id, True, ack))
            idle_limit = max(60.0, 6 * ctx.settings.compute.worker_heartbeat_seconds)
            while not conn.closed:
                try:
                    raw = await asyncio.wait_for(ws.receive_text(), idle_limit)
                except TimeoutError:
                    log.warning("worker %s idle for %.0fs; closing channel", name, idle_limit)
                    await self._close_conn(conn, CLOSE_TIMEOUT, "idle")
                    break
                conn.last_rx = time.monotonic()
                self.stats["frames_in"] += 1
                try:
                    f = P.decode(raw)
                    await self._dispatch(conn, f)
                except P.ProtocolError as exc:
                    self.stats["protocol_errors"] += 1
                    log.warning("protocol error from worker %s: %s", name, exc.message)
                    if exc.fatal:
                        await self._close_conn(conn, CLOSE_PROTOCOL, exc.message)
                        break
                    await conn.send(P.error_frame(exc.code, exc.message, exc.ref))
        except WebSocketDisconnect:
            pass
        except RuntimeError as exc:  # starlette raises RuntimeError on receive after close
            log.debug("websocket of %s closed: %s", name, exc)
        except Exception:  # noqa: BLE001
            log.exception("worker channel for %s failed", name)
        finally:
            conn.closed = True
            conn.fail_pending(WorkerOffline(f"worker '{name}' disconnected"))
            if conn.sender:
                conn.sender.cancel()
            if self._conns.get(wid) is conn:
                self._conns.pop(wid, None)
                await asyncio.to_thread(self._process_disconnect, wid, name)

    # ------------------------------------------------------------------ authentication
    def _authenticate(self, ws: WebSocket) -> dict[str, Any]:
        ctx = self.ctx
        auth = ws.headers.get("authorization", "")
        if not auth.lower().startswith("bearer "):
            raise Unauthenticated("missing worker token (Authorization: Bearer olcf_wrk_...)")
        token = auth[7:].strip()
        row = ctx.tokens.verify_opaque(token, kinds=("worker",))
        wid = (row.get("meta") or {}).get("worker_id")
        worker = None
        if wid:
            with ctx.db.connect() as conn:
                worker = fetch_one(conn, select(C.workers).where(C.workers.c.id == wid))
        creds = self.credentials
        if worker is None or not (creds.auth_accepts(worker, row["id"]) if creds is not None else worker.get("token_id") == row["id"]):
            raise Unauthenticated("worker token is no longer valid for any worker (rotated or removed)")
        if worker["state"] == WorkerState.REVOKED.value or worker.get("revoked_at"):
            raise OlcfError("worker has been revoked")
        pr = ctx.maybe_service("identity").get(row["principal_id"])
        if not pr or not pr["is_active"]:
            raise OlcfError("worker principal is disabled")
        serial = self._check_client_cert(ws, worker)
        if creds is not None:
            try:
                creds.on_authenticated(worker, row["id"], serial)
            except Exception:  # noqa: BLE001 - bookkeeping of a rotation never blocks a valid connection
                log.exception("credential rotation bookkeeping failed for worker %s", worker["name"])
        worker["_auth_token_id"] = row["id"]
        return worker

    def _client_cert(self, ws: WebSocket) -> str | None:
        tls = (ws.scope.get("extensions") or {}).get("tls") or {}
        chain = tls.get("client_cert_chain")
        if chain:
            return chain[0]
        if self.ctx.settings.server.trusted_proxies > 0:  # reverse proxy terminated mTLS and forwards the verified cert
            hdr = ws.headers.get("x-client-cert")
            if hdr:
                return urllib.parse.unquote(hdr)
        return None

    def revoked_serials(self) -> set[str]:
        with self.ctx.db.connect() as conn:
            row = fetch_one(conn, select(S.kv).where(S.kv.c.key == "pki.revoked"))
        return set((row or {}).get("value") or [])

    def revoke_serial(self, serial: str) -> None:
        serials = self.revoked_serials() | {serial}
        with self.ctx.db.begin() as conn:
            n = conn.execute(update(S.kv).where(S.kv.c.key == "pki.revoked").values(value=sorted(serials), updated_at=now_iso())).rowcount
            if not n:
                conn.execute(S.kv.insert().values(key="pki.revoked", value=sorted(serials), updated_at=now_iso()))

    def _check_client_cert(self, ws: WebSocket, worker: dict[str, Any]) -> str | None:
        """Verify the presented client certificate (CN, validity, CRL); returns its serial, or ``None`` when none was presented."""
        required = self.ctx.settings.tls.require_client_cert_for_workers
        pem = self._client_cert(ws)
        if pem is None:
            if required:
                raise OlcfError("a client certificate is required on the worker channel (tls.require_client_cert_for_workers)")
            return None
        ok, cn_or_reason = self.ctx.pki.verify_client_cert(pem)
        if not ok:
            raise OlcfError(f"client certificate rejected: {cn_or_reason}")
        if cn_or_reason != worker["id"]:
            raise OlcfError("client certificate CN does not match the worker identity")
        from cryptography import x509

        serial = format(x509.load_pem_x509_certificate(pem.encode()).serial_number, "x")
        if serial in self.revoked_serials():
            raise OlcfError("client certificate has been revoked")
        return serial

    # ------------------------------------------------------------------ hello / heartbeat / disconnect (threads)
    def _process_hello(self, worker: dict[str, Any], hello: P.Hello, ip: str | None, conn: WorkerConnection) -> dict[str, Any]:
        reg = self.registry
        inv = P.normalize_inventory(hello.inventory)
        if not inv.get("address") and ip:
            inv["address"] = ip
        cap = (hello.model_extra or {}).get("capacity")  # --max-cores/--max-memory-mb: the planner must never promise more than the agent hands out
        if isinstance(cap, dict) and cap.get("cores") and cap.get("memory_mb"):
            inv["capacity"] = {"cores": int(cap["cores"]), "memory_mb": int(cap["memory_mb"])}
        runtimes = [r.model_dump() for r in hello.runtimes]
        row = reg.get_worker(worker["id"])
        previous = row["state"]
        hist = row.get("history") or {}
        conn.history = {"cpu": list(hist.get("cpu", [])), "mem": list(hist.get("mem", [])), "ts": list(hist.get("ts", []))}
        reg.update_worker(
            worker["id"], agent_version=hello.agent_version, protocol=hello.protocol, hostname=inv.get("hostname") or row.get("hostname"),
            address=inv.get("address") or row.get("address"), os=inv.get("os"), arch=inv.get("arch"), inventory=inv, embedded=bool(hello.embedded or row.get("embedded")),
            runtimes=runtimes or row.get("runtimes") or [], spark_daemons=[d.model_dump() for d in hello.spark_daemons],
            active_jobs=[a.model_dump() for a in hello.active_jobs], connected=True, connected_node=self.ctx.node_id,
            last_connected_at=now_iso(), last_seen_at=now_iso(),
        )
        if runtimes:
            self.sync_runtime_installs(worker["id"], runtimes)
        changed = reg.set_worker_state(worker["id"], WorkerState.ONLINE.value, reason="agent connected", only_from=("pending", "offline", "degraded"))
        if changed and previous == WorkerState.PENDING.value:
            log.info("worker %s connected for the first time", worker["name"])
        if self.credentials is not None:
            try:
                self.credentials.observe_hello(row, (hello.model_extra or {}).get("cert"))
            except Exception:  # noqa: BLE001
                log.exception("certificate expiry bookkeeping failed for worker %s", worker["name"])
        for fn in self._hello_listeners:
            try:
                fn(reg.get_worker(worker["id"]), hello)
            except Exception:  # noqa: BLE001
                log.exception("hello listener %s failed for worker %s", getattr(fn, "__name__", fn), worker["name"])
        return reg.get_worker(worker["id"])

    def _process_heartbeat(self, conn: WorkerConnection, hb: P.Heartbeat) -> None:
        now = time.monotonic()
        m = hb.metrics.model_dump(exclude_none=True)
        summary = summarize_metrics(m)
        conn.history["cpu"].append(summary["cpu_percent"])
        conn.history["mem"].append(summary["mem_percent"])
        conn.history["ts"].append(now_iso())
        for k in conn.history:
            if len(conn.history[k]) > HISTORY_SAMPLES:
                del conn.history[k][: len(conn.history[k]) - HISTORY_SAMPLES]
        if now - conn.last_persist < PERSIST_EVERY_SECONDS:
            return
        conn.last_persist = now
        vals: dict[str, Any] = {"metrics": m, "history": conn.history, "last_seen_at": now_iso(), "connected": True, "connected_node": self.ctx.node_id}
        inventory_changed = False
        row = self.registry.get_worker(conn.worker_id)
        notes = dict(row.get("notes") or {})
        if hb.ts:
            try:
                skew = time.time() - from_iso(hb.ts).timestamp()
                notes["clock_skew_seconds"] = round(skew, 2)
            except ValueError:
                pass
        if notes != (row.get("notes") or {}):
            vals["notes"] = notes
        if hb.runtimes is not None:
            rt = [r.model_dump() for r in hb.runtimes]
            if rt != (row.get("runtimes") or []):
                vals["runtimes"] = rt
                self.sync_runtime_installs(conn.worker_id, rt)
                inventory_changed = True
        if hb.components is not None:
            vals["components"] = [c.model_dump() for c in hb.components]
        if hb.active_jobs is not None:
            vals["active_jobs"] = [a.model_dump() for a in hb.active_jobs]
        if hb.spark_daemons is not None:
            vals["spark_daemons"] = [d.model_dump() for d in hb.spark_daemons]
        merged = {**row, **vals}
        state, reasons = assess_health(merged, time.time(), heartbeat_s=self.ctx.settings.compute.worker_heartbeat_seconds,
                                       offline_after_s=self.ctx.settings.compute.worker_offline_after_seconds)
        self.registry.update_worker(conn.worker_id, **vals)
        if inventory_changed:
            for fn in self._inventory_listeners:
                try:
                    fn(conn.worker_id)
                except Exception:  # noqa: BLE001
                    log.exception("inventory listener %s failed for worker %s", getattr(fn, "__name__", fn), conn.worker_id)
        if state in (WorkerState.ONLINE.value, WorkerState.DEGRADED.value) and row["state"] != state and row["state"] != WorkerState.REVOKED.value:
            self.registry.set_worker_state(conn.worker_id, state, reason="; ".join(reasons) or "heartbeat healthy")

    def _process_disconnect(self, worker_id: str, name: str) -> None:
        try:
            row = self.registry.get_worker(worker_id)
            if row.get("connected_node") == self.ctx.node_id:
                self.registry.update_worker(worker_id, connected=False, last_disconnected_at=now_iso())
            log.info("worker %s disconnected", name)
        except OlcfError:
            pass

    def sync_runtime_installs(self, worker_id: str, runtimes: list[dict[str, Any]]) -> None:
        """Mirror the agent-reported runtime states into ``runtime_installs`` (per-node install status for the catalog)."""
        t = C.runtime_installs.c
        now = now_iso()
        with self.ctx.db.begin() as conn:
            have = {r["runtime_id"]: r for r in fetch_all(conn, select(C.runtime_installs).where(t.worker_id == worker_id))}
            seen = set()
            for r in runtimes:
                rid = r.get("id")
                if not rid:
                    continue
                seen.add(rid)
                vals = {"state": r.get("state", "ready"), "verified": bool(r.get("verified")), "fingerprint": r.get("fingerprint"), "detail": r.get("detail"), "updated_at": now}
                if rid in have:
                    cur = have[rid]
                    if (cur["state"], cur["verified"], cur["fingerprint"], cur["detail"]) != (vals["state"], vals["verified"], vals["fingerprint"], vals["detail"]):
                        conn.execute(update(C.runtime_installs).where(t.worker_id == worker_id, t.runtime_id == rid).values(**vals))
                else:
                    insert_row(conn, C.runtime_installs, {"runtime_id": rid, "worker_id": worker_id, **vals})
            for rid in set(have) - seen:
                conn.execute(C.runtime_installs.delete().where(t.worker_id == worker_id, t.runtime_id == rid))

    # ------------------------------------------------------------------ frame dispatch
    async def _dispatch(self, conn: WorkerConnection, frame: P.Frame) -> None:
        t = frame.type
        if t == "hello":  # a second hello on a live channel re-runs reconciliation (agent restarted its state)
            hello = P.parse_payload(frame)
            await asyncio.to_thread(self._process_hello, self.registry.get_worker(conn.worker_id), hello, conn.ip, conn)  # type: ignore[arg-type]
        elif t not in P.AGENT_TYPES:
            raise P.ProtocolError(f"unknown frame type '{t}'", code="unknown_type", ref=frame.id)
        elif t in ("result", "pong"):
            payload = P.parse_payload(frame)
            fut = conn.pending.get(frame.ref or "")
            if fut is not None and not fut.done():
                fut.set_result(payload.model_dump() if isinstance(payload, BaseModel) else dict(payload))
            return
        elif t == "renew_cert":
            await self._renew_cert(conn, frame)
            return
        elif t == "heartbeat":
            hb = P.parse_payload(frame)
            await asyncio.to_thread(self._process_heartbeat, conn, hb)  # type: ignore[arg-type]
        elif t == "job_log":
            msg = P.parse_payload(frame)
            assert isinstance(msg, P.JobLogMsg)
            if len(msg.data.encode("utf-8", "replace")) > P.MAX_LOG_CHUNK_BYTES * 2:
                raise P.ProtocolError("job_log chunk exceeds 64 KiB", code="chunk_too_large", ref=frame.id)
            if msg.job_id not in conn.verified_jobs:
                owner = await asyncio.to_thread(self._job_owner, msg.job_id)
                if owner is not None and owner != conn.worker_id:
                    raise P.ProtocolError("job_log for a job that is not assigned to this worker", code="not_your_job", ref=frame.id)
                conn.verified_jobs.append(msg.job_id)
            self.logs.append(msg.job_id, msg.stream, msg.seq, msg.data)
        else:
            msg = P.parse_payload(frame)
            for fn in self._listeners.get(t, []):
                try:
                    await asyncio.to_thread(fn, conn.worker_id, msg, frame)
                except OlcfError as exc:
                    log.warning("listener for %s from %s rejected: %s", t, conn.name, exc.message)
                except Exception:  # noqa: BLE001
                    log.exception("listener for %s from %s failed", t, conn.name)
        if frame.id and t != "hello":
            await conn.send(P.result_frame(frame.id, True))

    async def _renew_cert(self, conn: WorkerConnection, frame: P.Frame) -> None:
        """``renew_cert``: the answer carries data (new certificate / token) or a structured refusal; it is always a ``result`` for the frame's ``id``."""
        req = P.parse_payload(frame)
        assert isinstance(req, P.RenewCert)
        if self.credentials is None:
            err = {"code": "unsupported", "message": "this controller does not renew worker credentials"}
            if frame.id:
                await conn.send(P.result_frame(frame.id, False, error=err))
            return
        try:
            data = await asyncio.to_thread(self.credentials.renew, conn.worker_id, conn.token_id, req.model_dump(exclude_none=True), ip=conn.ip, connected_at=conn.connected_at)
        except OlcfError as exc:
            if frame.id:
                await conn.send(P.result_frame(frame.id, False, error=exc.to_dict()))
            return
        except Exception as exc:  # noqa: BLE001
            log.exception("renew_cert of worker %s failed", conn.name)
            if frame.id:
                await conn.send(P.result_frame(frame.id, False, error={"code": "internal", "message": f"{type(exc).__name__} while renewing credentials"}))
            return
        if frame.id:
            await conn.send(P.result_frame(frame.id, True, data))

    def _job_owner(self, job_id: str) -> str | None:
        fn = self.job_owner_lookup
        return fn(job_id) if fn else None

    # ================================================================== liveness (leader loop calls this)
    def liveness_tick(self) -> list[dict[str, Any]]:
        """Recompute online/degraded/offline for every worker row from persisted heartbeats. Returns rows that went offline."""
        c = self.ctx.settings.compute
        now = time.time()
        went_offline: list[dict[str, Any]] = []
        for row in self.registry.list_workers(include_revoked=False):
            if row["state"] in (WorkerState.REVOKED.value,):
                continue
            state, reasons = assess_health(row, now, heartbeat_s=c.worker_heartbeat_seconds, offline_after_s=c.worker_offline_after_seconds)
            if state == WorkerState.PENDING.value:
                continue
            if state == WorkerState.OFFLINE.value:
                if row["state"] != WorkerState.OFFLINE.value:
                    self.registry.update_worker(row["id"], connected=False, last_disconnected_at=row.get("last_disconnected_at") or now_iso())
                    if self.registry.set_worker_state(row["id"], state, reason="; ".join(reasons)):
                        went_offline.append(row)
            elif state != row["state"]:
                self.registry.set_worker_state(row["id"], state, reason="; ".join(reasons))
        return went_offline

    def snapshot(self) -> dict[str, Any]:
        return {"connected": sorted(self.connected_ids()), "stats": dict(self.stats)}


# =========================================================================== enrollment (service functions)
class EnrollmentService:
    """Enrollment-token issuing and worker enrollment."""

    def __init__(self, ctx: AppContext, registry: ComputeRegistry) -> None:
        self.ctx, self.registry = ctx, registry

    def create_token(self, principal: Principal, body: EnrollmentTokenCreate, *, base_url: str) -> dict[str, Any]:
        if body.pool and not self.registry.pool_exists(body.pool):
            raise BadRequest(f"unknown worker pool '{body.pool}'")
        for k in body.labels:
            if not k or len(k) > 63:
                raise BadRequest("label keys must be 1-63 characters")
        token, rec = self.ctx.tokens.create_enrollment(principal.id, ttl_seconds=body.ttl_seconds, meta={
            "name": body.name, "pool": body.pool, "labels": body.labels, "replace_existing": body.replace_existing, "created_by": principal.name})
        cmd = f"olcf-worker run --url {base_url} --token {token}" + (f" --name {body.name}" if body.name else "") + (f" --pool {body.pool}" if body.pool else "")
        if body.labels:
            cmd += " --labels " + ",".join(f"{k}={v}" for k, v in sorted(body.labels.items()))
        self.ctx.audit.record(principal, "worker.enrollment_token_created", resource_type="enrollment_token", resource_id=rec["id"],
                              details={"pool": body.pool, "ttl_seconds": body.ttl_seconds, "name": body.name})
        return {"token": token, "token_id": rec["id"], "expires_at": rec["expires_at"], "command": cmd, "url": base_url}

    def enroll(self, req: EnrollRequest, *, ip: str | None, channel_base: str) -> dict[str, Any]:
        ctx = self.ctx
        ctx.limiter.check(f"enroll:{ip}", limit=30)
        if req.protocol.split(".")[0] != P.WORKER_PROTOCOL_VERSION.split(".")[0]:
            raise BadRequest(f"unsupported agent protocol {req.protocol}", details={"supported": P.WORKER_PROTOCOL_VERSION})
        tok = ctx.tokens.consume_enrollment(req.token)  # raises Unauthenticated; single use
        meta = tok.get("meta") or {}
        inv = P.normalize_inventory(req.inventory)
        name = req.name or meta.get("name") or (req.hostname or inv.get("hostname") or "worker").split(".")[0]
        name = "".join(ch if (ch.isalnum() or ch in "._-") else "-" for ch in name)[:128] or "worker"
        pool = meta.get("pool") or req.pool or "default"
        if not self.registry.pool_exists(pool):
            raise BadRequest(f"unknown worker pool '{pool}'")
        labels = {k: str(v) for k, v in req.labels.items() if not k.startswith(RESERVED_LABEL_PREFIX)}
        labels.update({k: str(v) for k, v in (meta.get("labels") or {}).items()})  # admin labels win
        identity = ctx.service("identity")
        now = now_iso()
        with ctx.db.connect() as conn:
            existing = fetch_one(conn, select(C.workers).where(C.workers.c.name == name))
        if existing and not meta.get("replace_existing"):
            raise Conflict(f"a worker named '{name}' is already enrolled", remediation="choose another --name, or revoke the old worker, or issue the enrollment token with replace_existing")
        if existing:
            worker_id = existing["id"]
            old_pr = existing.get("principal_id")
            if old_pr:
                ctx.tokens.revoke_all_for(old_pr, kinds=("worker",))
            if existing.get("cert_serial"):
                self.hub_revoke_serial(existing["cert_serial"])
            pending = ((existing.get("credentials") or {}).get("rotation") or {}).get("prev_serial")
            if pending:
                self.hub_revoke_serial(pending)  # a superseded certificate whose rotation was still open
            principal_id = old_pr or identity.create_principal(f"worker:{worker_id}", kind="worker", source="system", display_name=name)["id"]
        else:
            worker_id = new_id("wrk")
            principal_id = identity.create_principal(f"worker:{worker_id}", kind="worker", source="system", display_name=name)["id"]
        cert_pem = serial = None
        creds = self.credentials()
        if req.csr_pem:
            try:
                cert_pem, serial = ctx.pki.sign_worker_csr(req.csr_pem, worker_id, days=creds.cert_lifetime_days() if creds else 90)
            except Exception as exc:  # noqa: BLE001
                raise BadRequest(f"invalid certificate signing request: {exc}") from exc
        wtoken, trow = ctx.tokens.create_worker_token(principal_id, meta={"worker_id": worker_id}, ttl_seconds=creds.token_ttl_seconds() if creds else None)
        values = {
            "name": name, "hostname": req.hostname or inv.get("hostname"), "address": inv.get("address"), "state": WorkerState.PENDING.value,
            "scheduling": "active", "pool": pool, "labels": labels, "embedded": req.embedded, "agent_version": req.agent_version, "protocol": req.protocol,
            "os": inv.get("os"), "arch": inv.get("arch"), "inventory": inv, "principal_id": principal_id, "token_id": trow["id"], "cert_serial": serial,
            "connected": False, "connected_node": None, "revoked_at": None, "updated_at": now, "etag": new_etag(),
            **(creds.enrolment_values(cert_pem=cert_pem, token_row=trow) if creds else {}),
        }
        with ctx.db.begin() as conn:
            if existing:
                conn.execute(update(C.workers).where(C.workers.c.id == worker_id).values(**values))
            else:
                insert_row(conn, C.workers, {"id": worker_id, "metrics": {}, "history": {}, "runtimes": [], "components": [], "spark_daemons": [], "active_jobs": [],
                                             "notes": {}, "last_seen_at": None, "last_connected_at": None, "last_disconnected_at": None, "enrolled_at": now,
                                             "created_at": now, **values})
        actor = Principal(id=principal_id, name=f"worker:{worker_id}", kind="worker", token_scopes=["worker"])
        ctx.audit.record(actor, "worker.enrolled", resource_type="worker", resource_id=worker_id, ip=ip,
                         details={"name": name, "pool": pool, "hostname": values["hostname"], "agent_version": req.agent_version, "enrollment_token": tok["id"],
                                  "replaced": bool(existing), "issued_by": meta.get("created_by")})
        ctx.events.publish("worker.enrolled", {"worker_id": worker_id, "name": name, "pool": pool, "replaced": bool(existing)}, subject=worker_id)
        return {
            "worker_id": worker_id, "name": name, "worker_token": wtoken, "ca_pem": ctx.pki.ca_pem, "client_cert_pem": cert_pem,
            "channel_url": f"{channel_base}/v1/workers/ws", "heartbeat_seconds": ctx.settings.compute.worker_heartbeat_seconds,
            "protocol": P.WORKER_PROTOCOL_VERSION, "pool": pool, "server_time": now,
        }

    def credentials(self) -> Any:
        """The credential-lifecycle service (policy: certificate lifetime, token max age); ``None`` when the compute service is not installed."""
        compute = self.ctx.maybe_service("compute")
        return getattr(compute, "credentials", None) if compute is not None else None

    # the hub owns the CRL; the enrollment service reaches it through the compute service
    def hub_revoke_serial(self, serial: str) -> None:
        compute = self.ctx.maybe_service("compute")
        if compute is not None:
            compute.hub.revoke_serial(serial)


# =========================================================================== routes
router = APIRouter(tags=["workers"])


def _hub(websocket_or_request: Any) -> WorkerHub:
    ctx: AppContext = websocket_or_request.app.state.ctx
    return ctx.service("compute").hub


@router.websocket("/workers/ws")
async def worker_channel(websocket: WebSocket) -> None:
    """Agent channel (authentication is performed inside, with the worker token from the ``Authorization`` header)."""
    await _hub(websocket).handle_websocket(websocket)


__all__ = ["EnrollRequest", "EnrollmentService", "EnrollmentTokenCreate", "WorkerConnection", "WorkerHub", "ws_base", "router"]
