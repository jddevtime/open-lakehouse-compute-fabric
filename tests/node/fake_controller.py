# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""A fake controller: a real ``websockets`` server speaking the agent protocol (CONTRACTS 4.2) the way ``olcf.compute.hub`` does.

* authenticates ``Authorization: Bearer <token>`` (token -> worker id map), several workers may be connected at once,
* ``hello`` must be the first frame and is answered with ``result{ref, ok, data:{heartbeat_seconds, drain, server_time}}``,
* every other agent frame that carries an ``id`` is acknowledged with ``result{ref=id}`` (like the hub); ``reject_log_jobs`` answers
  ``job_log`` of the listed jobs with ``error{ref}`` instead, ``ack_enabled=False`` disables acks (tests the implicit-ack path),
* records everything: frames, hellos, heartbeats, de-duplicated log chunks per ``(job_id, stream, seq)`` (like the log store), job states,
  job events, kernel events, daemon states,
* ``request(worker, type, payload)`` sends a command and waits for the agent's ``result``,
* ``drop(worker)`` aborts the TCP connection, ``stop()``/``start()`` restart the server on the *same port* (controller restart).

It also contains :class:`FakeEnrollServer`, an HTTP server implementing ``POST /v1/workers/enroll`` with the real :class:`olcf.security.pki.PKI`.
"""

# ruff: noqa: S107
from __future__ import annotations

import asyncio
import http.server
import json
import threading
import time
import uuid
from collections import defaultdict
from collections.abc import Callable
from datetime import UTC, datetime
from http import HTTPStatus
from pathlib import Path
from typing import Any

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class _Conn:
    def __init__(self, ws: Any, worker_id: str) -> None:
        self.ws, self.worker_id = ws, worker_id
        self.pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self.closed = False


class FakeController:
    def __init__(self, tokens: dict[str, str] | None = None, *, heartbeat_seconds: float = 1.0, host: str = "127.0.0.1", port: int = 0) -> None:
        self.tokens = tokens or {"tok-1": "wrk_1"}
        self.heartbeat_seconds = heartbeat_seconds
        self.host, self.port = host, port
        self.ack_enabled = True
        self.reject_log_jobs: set[str] = set()
        self.drain: dict[str, bool] = defaultdict(bool)
        self.hello_delay = 0.0
        # recorded state (all guarded by self.lock)
        self.lock = threading.RLock()
        self.frames: list[tuple[str, dict[str, Any]]] = []
        self.hellos: list[tuple[str, dict[str, Any]]] = []
        self.heartbeats: dict[str, dict[str, Any]] = {}
        self.logs: dict[tuple[str, str], dict[int, str]] = defaultdict(dict)
        self.log_dups = 0
        self.job_states: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self.job_events: list[dict[str, Any]] = []
        self.kernel_events: list[dict[str, Any]] = []
        self.daemon_states: list[dict[str, Any]] = []
        self.connections: dict[str, _Conn] = {}
        self.connect_count: dict[str, int] = defaultdict(int)
        self.auth_failures = 0
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._server: Any = None

    # ------------------------------------------------------------------ lifecycle
    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def start(self) -> FakeController:
        if self._loop is None:
            self._loop = asyncio.new_event_loop()
            self._thread = threading.Thread(target=self._loop.run_forever, name="fake-controller", daemon=True)
            self._thread.start()
        asyncio.run_coroutine_threadsafe(self._start_server(), self._loop).result(15)
        return self

    async def _start_server(self) -> None:
        last: Exception | None = None
        for _ in range(50):  # the port may linger for a moment after a restart
            try:
                self._server = await serve(self._handler, self.host, self.port, process_request=self._auth, max_size=32 * 1024 * 1024, ping_interval=None, compression=None)
                break
            except OSError as exc:
                last = exc
                await asyncio.sleep(0.1)
        else:
            raise RuntimeError(f"cannot bind {self.host}:{self.port}: {last}")
        self.port = self._server.sockets[0].getsockname()[1]

    def stop(self, *, close_loop: bool = False) -> None:
        """Stop serving and drop every connection (a controller restart when followed by :meth:`start`)."""
        if self._loop is None or self._server is None:
            return
        fut = asyncio.run_coroutine_threadsafe(self._stop_server(), self._loop)
        fut.result(15)
        if close_loop:
            self._loop.call_soon_threadsafe(self._loop.stop)
            if self._thread:
                self._thread.join(5)
            self._loop = None

    async def _stop_server(self) -> None:
        srv, self._server = self._server, None
        for c in list(self.connections.values()):
            c.closed = True
            tr = getattr(c.ws, "transport", None)
            if tr is not None:
                tr.abort()
        self.connections.clear()
        srv.close()
        await srv.wait_closed()

    def restart(self) -> None:
        self.stop()
        self.start()

    def __enter__(self) -> FakeController:
        return self.start()

    def __exit__(self, *exc: object) -> None:
        self.stop(close_loop=True)

    # ------------------------------------------------------------------ server side
    def _auth(self, connection: Any, request: Any) -> Any:
        auth = request.headers.get("Authorization", "")
        token = auth[7:].strip() if auth.lower().startswith("bearer ") else ""
        if token not in self.tokens:
            self.auth_failures += 1
            return connection.respond(HTTPStatus.UNAUTHORIZED, "invalid worker token\n")
        return None

    async def _handler(self, ws: Any) -> None:
        auth = ws.request.headers.get("Authorization", "")
        wid = self.tokens[auth[7:].strip()]
        try:
            raw = await asyncio.wait_for(ws.recv(), 15)
        except (TimeoutError, ConnectionClosed):
            return
        frame = json.loads(raw)
        if frame.get("type") != "hello":
            await ws.close(4400, "first frame must be hello")
            return
        with self.lock:
            self.frames.append((wid, frame))
            self.hellos.append((wid, frame["payload"]))
        if self.hello_delay:
            await asyncio.sleep(self.hello_delay)
        conn = _Conn(ws, wid)
        old = self.connections.get(wid)
        if old is not None:
            old.closed = True
            try:
                await old.ws.close(4409, "replaced by a newer connection")
            except Exception:  # noqa: BLE001, S110
                pass
        self.connections[wid] = conn
        self.connect_count[wid] += 1
        await self._send(conn, {"v": 1, "type": "result", "ref": frame["id"], "ts": _now(),
                                "payload": {"ok": True, "data": {"worker_id": wid, "heartbeat_seconds": self.heartbeat_seconds, "server_time": _now(), "drain": self.drain[wid]}}})
        try:
            async for raw in ws:
                await self._on_frame(conn, json.loads(raw))
        except ConnectionClosed:
            pass
        finally:
            conn.closed = True
            for fut in conn.pending.values():
                if not fut.done():
                    fut.set_exception(ConnectionError("agent disconnected"))
            if self.connections.get(wid) is conn:
                self.connections.pop(wid, None)

    async def _send(self, conn: _Conn, frame: dict[str, Any]) -> None:
        await conn.ws.send(json.dumps(frame))

    async def _on_frame(self, conn: _Conn, f: dict[str, Any]) -> None:
        wid, t, p = conn.worker_id, f.get("type"), f.get("payload") or {}
        with self.lock:
            self.frames.append((wid, f))
            if t == "heartbeat":
                self.heartbeats[wid] = p
            elif t == "job_log":
                key = (p["job_id"], p["stream"])
                if p["seq"] in self.logs[key]:
                    self.log_dups += 1
                self.logs[key][p["seq"]] = p["data"]
            elif t == "job_state":
                self.job_states[p["job_id"]].append(p)
            elif t == "job_event":
                self.job_events.append(p)
            elif t == "kernel_event":
                self.kernel_events.append(p)
            elif t == "daemon_state":
                self.daemon_states.append(p)
        if t in ("result", "pong"):
            fut = conn.pending.get(f.get("ref") or "")
            if fut is not None and not fut.done():
                fut.set_result(p)
            return
        fid = f.get("id")
        if fid and self.ack_enabled:
            if t == "job_log" and p.get("job_id") in self.reject_log_jobs:
                await self._send(conn, {"v": 1, "type": "error", "ref": fid, "ts": _now(), "payload": {"code": "not_your_job", "message": "job_log for a job that is not assigned to this worker"}})
            else:
                await self._send(conn, {"v": 1, "type": "result", "ref": fid, "ts": _now(), "payload": {"ok": True}})

    # ------------------------------------------------------------------ test API
    def connected(self, worker_id: str | None = None) -> bool:
        wid = worker_id or next(iter(self.tokens.values()))
        c = self.connections.get(wid)
        return c is not None and not c.closed

    def wait_connected(self, worker_id: str | None = None, timeout: float = 30.0) -> None:
        self.wait_for(lambda: self.connected(worker_id), timeout, f"worker {worker_id or 'any'} to connect")

    def wait_for(self, fn: Callable[[], Any], timeout: float = 30.0, what: str = "condition", interval: float = 0.05) -> Any:
        end = time.monotonic() + timeout
        while True:
            val = fn()
            if val:
                return val
            if time.monotonic() > end:
                raise AssertionError(f"timed out after {timeout:g}s waiting for {what}")
            time.sleep(interval)

    def request(self, worker_id: str, ftype: str, payload: dict[str, Any] | None = None, timeout: float = 30.0) -> dict[str, Any]:
        """Send a command; returns the agent's ``result`` payload (``{"ok": ..., "data"|"error": ...}``)."""
        assert self._loop is not None
        return asyncio.run_coroutine_threadsafe(self._request(worker_id, ftype, payload or {}, timeout), self._loop).result(timeout + 5)

    async def _request(self, worker_id: str, ftype: str, payload: dict[str, Any], timeout: float) -> dict[str, Any]:
        conn = self.connections.get(worker_id)
        if conn is None or conn.closed:
            raise ConnectionError(f"worker {worker_id} is not connected")
        fid = uuid.uuid4().hex
        fut: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        conn.pending[fid] = fut
        await self._send(conn, {"v": 1, "type": ftype, "id": fid, "ts": _now(), "payload": payload})
        try:
            return await asyncio.wait_for(fut, timeout)
        finally:
            conn.pending.pop(fid, None)

    def ok(self, worker_id: str, ftype: str, payload: dict[str, Any] | None = None, timeout: float = 30.0) -> Any:
        """Like :meth:`request` but asserts success and returns ``data``."""
        res = self.request(worker_id, ftype, payload, timeout)
        assert res.get("ok"), f"{ftype} failed: {res.get('error')}"
        return res.get("data")

    def send_raw(self, worker_id: str, frame: dict[str, Any]) -> None:
        assert self._loop is not None

        async def go() -> None:
            conn = self.connections[worker_id]
            await self._send(conn, frame)

        asyncio.run_coroutine_threadsafe(go(), self._loop).result(5)

    def drop(self, worker_id: str) -> None:
        """Abort the TCP connection of a worker without a close handshake."""
        assert self._loop is not None

        async def go() -> None:
            c = self.connections.get(worker_id)
            if c is not None:
                tr = getattr(c.ws, "transport", None)
                if tr is not None:
                    tr.abort()

        asyncio.run_coroutine_threadsafe(go(), self._loop).result(5)

    def log_text(self, job_id: str, stream: str = "stdout") -> str:
        with self.lock:
            chunks = self.logs.get((job_id, stream), {})
            return "".join(chunks[s] for s in sorted(chunks))

    def log_seqs(self, job_id: str, stream: str = "stdout") -> list[int]:
        with self.lock:
            return sorted(self.logs.get((job_id, stream), {}))

    def final_state(self, job_id: str) -> dict[str, Any] | None:
        with self.lock:
            for p in reversed(self.job_states.get(job_id, [])):
                if p["state"] in ("succeeded", "failed", "cancelled", "timed_out"):
                    return p
        return None

    def frames_of(self, ftype: str, worker_id: str | None = None) -> list[dict[str, Any]]:
        with self.lock:
            return [f for w, f in self.frames if f.get("type") == ftype and (worker_id is None or w == worker_id)]

    def events_named(self, name: str, job_id: str | None = None) -> list[dict[str, Any]]:
        with self.lock:
            return [e for e in self.job_events if e["event"] == name and (job_id is None or e["job_id"] == job_id)]


# ------------------------------------------------------------------------------------------------ enrolment
class FakeEnrollServer:
    """``POST /v1/workers/enroll`` with a real CA: validates a single-use token, signs the CSR, returns the contract's response."""

    def __init__(self, pki_dir: Path, *, token: str = "olcf_enr_testtoken0001", channel_url: str = "ws://127.0.0.1:1/v1/workers/ws") -> None:
        from olcf.security.pki import PKI

        self.pki = PKI(pki_dir)
        self.token, self.used = token, False
        self.channel_url = channel_url
        self.requests: list[dict[str, Any]] = []
        outer = self

        class H(http.server.BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
                pass

            def do_POST(self) -> None:  # noqa: N802
                body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))) or b"{}")
                outer.requests.append(body)
                if self.path != "/v1/workers/enroll":
                    return self._reply(404, {"error": "not found"})
                if body.get("token") != outer.token or outer.used:
                    return self._reply(403, {"error": "token invalid or already used"})
                outer.used = True
                wid = "wrk_" + uuid.uuid4().hex[:10]
                cert, _serial = outer.pki.sign_worker_csr(body["csr_pem"], wid)
                self._reply(200, {"worker_id": wid, "worker_token": "olcf_wrk_" + uuid.uuid4().hex, "ca_pem": outer.pki.ca_pem, "client_cert_pem": cert,
                                  "channel_url": outer.channel_url, "heartbeat_seconds": 5})

            def _reply(self, code: int, doc: dict[str, Any]) -> None:
                data = json.dumps(doc).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        self._srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.port = self._srv.server_address[1]
        self.url = f"http://127.0.0.1:{self.port}"
        threading.Thread(target=self._srv.serve_forever, daemon=True).start()

    def close(self) -> None:
        self._srv.shutdown()
        self._srv.server_close()
