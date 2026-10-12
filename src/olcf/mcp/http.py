# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Streamable HTTP transport of the MCP endpoint (``/mcp``) plus the management routes under ``/v1/mcp``.

* ``POST /mcp``   one JSON-RPC message (or, for protocol 2025-03-26 sessions, a batch).  Requests are answered with
  ``application/json``; a ``tools/call`` carrying a ``progressToken`` is answered as ``text/event-stream`` with progress
  notifications (keep-alive for long queries) followed by the response.  Notifications and responses get ``202``.
* ``DELETE /mcp`` ends the session.  ``GET /mcp`` answers ``405`` (this server never initiates messages).
* Bearer authentication on every request (a platform token with scope ``mcp`` / ``mcp:write``, or a user access token); ``Origin`` is
  validated against DNS rebinding; sessions are bound to the principal that created them.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any
from urllib.parse import urlsplit

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from starlette.concurrency import run_in_threadpool

from olcf.api.deps import Authed, Ctx, Pagination, _client_ip, authenticate, bearer_token
from olcf.core.context import AppContext
from olcf.core.errors import OlcfError, Unauthenticated
from olcf.mcp import protocol
from olcf.mcp.protocol import INVALID_REQUEST, SERVER_ERROR, RpcError
from olcf.mcp.service import McpService, Reply
from olcf.security.rbac import Principal

public = APIRouter(tags=["mcp"])
api = APIRouter(tags=["mcp"])
LOOPBACK = {"localhost", "127.0.0.1", "::1", "[::1]"}
NO_STORE = {"Cache-Control": "no-store"}


def _fail(status: int, code: int, message: str, *, data: Any = None, headers: dict[str, str] | None = None) -> JSONResponse:
    return JSONResponse(RpcError(code, message, data).body(None), status_code=status, headers={**NO_STORE, **(headers or {})})


def _hostname(netloc: str) -> str:
    if netloc.startswith("["):
        return netloc.split("]")[0] + "]"
    return netloc.rsplit(":", 1)[0] if netloc.count(":") == 1 else netloc


def _setting_list(ctx: AppContext, key: str) -> list[str]:
    try:
        v = ctx.service("settings").get(key)
        return [str(x) for x in v] if isinstance(v, list) else []
    except Exception:  # noqa: BLE001
        return []


def origin_problem(ctx: AppContext, request: Request) -> str | None:
    """DNS-rebinding / cross-site protection.  Returns a reason when the request must be refused."""
    cfg = ctx.settings.server
    host = (request.headers.get("host") or "").lower()
    public_host = urlsplit(cfg.public_url).netloc.lower() if cfg.public_url else ""
    allowed_hosts = LOOPBACK | ({public_host, _hostname(public_host)} if public_host else set()) | {h.lower() for h in _setting_list(ctx, "mcp.allowed_hosts")}
    if cfg.host in ("127.0.0.1", "localhost", "::1") and _hostname(host) not in allowed_hosts and host not in allowed_hosts:
        return f"the Host '{host[:100]}' is not allowed on a loopback-only server (DNS rebinding protection); set server.public_url or the mcp.allowed_hosts setting"
    origin = request.headers.get("origin")
    if origin is None:
        return None                                   # not a browser: bearer authentication is the control
    if origin == "null":
        return "Origin 'null' is not allowed"
    parts = urlsplit(origin)
    allowed_origins = {o.rstrip("/").lower() for o in [*cfg.cors_origins, *_setting_list(ctx, "mcp.allowed_origins")]}
    if cfg.public_url:
        pu = urlsplit(cfg.public_url)
        allowed_origins.add(f"{pu.scheme}://{pu.netloc}".lower())
    if origin.rstrip("/").lower() in allowed_origins or "*" in allowed_origins:
        return None
    if parts.netloc.lower() == host and (cfg.host not in ("127.0.0.1", "localhost", "::1") or _hostname(host) in LOOPBACK):
        return None                                   # same-origin (and, on loopback-only servers, only for loopback hosts)
    return f"Origin '{origin[:100]}' is not allowed"


def _authenticate(ctx: AppContext, request: Request) -> Principal:
    token = bearer_token(request)
    if not token:
        raise Unauthenticated("authentication required", remediation="send 'Authorization: Bearer <platform token with the mcp scope>'")
    principal = authenticate(ctx, token, ip=_client_ip(request, ctx))
    principal.correlation_id = getattr(request.state, "correlation_id", None)
    return principal


def _event(body: dict[str, Any]) -> str:
    return f"event: message\ndata: {json.dumps(body, default=str, separators=(',', ':'))}\n\n"


async def _read_body(request: Request, limit: int) -> bytes | None:
    cl = request.headers.get("content-length")
    if cl and cl.isdigit() and int(cl) > limit:
        return None
    chunks, total = [], 0
    async for part in request.stream():
        total += len(part)
        if total > limit:
            return None
        chunks.append(part)
    return b"".join(chunks)


def _prepare(svc: McpService, principal: Principal, msg: Any, session_header: str | None, version_header: str | None, ip: str | None) -> tuple[dict[str, Any] | None, str]:
    """Resolve the session for ``msg`` (None for ``initialize``) and check the protocol-version header against it."""
    is_init = isinstance(msg, dict) and msg.get("method") == "initialize"
    if is_init:
        return None, protocol.LATEST_VERSION
    if not session_header:
        raise RpcError(INVALID_REQUEST, "missing Mcp-Session-Id header: send initialize first and include the session id on every later request", http_status=400)
    session = svc.get_session(session_header, principal)
    if version_header and version_header not in protocol.SUPPORTED_VERSIONS:
        raise RpcError(INVALID_REQUEST, f"unsupported MCP-Protocol-Version '{version_header[:40]}'", {"supported": list(protocol.SUPPORTED_VERSIONS)}, http_status=400)
    if version_header and version_header != session["protocol_version"]:
        raise RpcError(INVALID_REQUEST, f"MCP-Protocol-Version {version_header} does not match the negotiated version {session['protocol_version']}", http_status=400)
    return session, session["protocol_version"]


@public.post("/mcp")
async def mcp_post(request: Request) -> Response:
    ctx: AppContext = request.app.state.ctx
    svc: McpService = ctx.service("mcp")
    if not svc.enabled():
        return _fail(503, SERVER_ERROR, "the MCP endpoint is disabled (setting mcp.enabled)")
    problem = origin_problem(ctx, request)
    if problem:
        return _fail(403, SERVER_ERROR, problem)
    try:
        principal = await run_in_threadpool(_authenticate, ctx, request)
        svc.check_access(principal)
    except Unauthenticated as exc:
        return _fail(401, SERVER_ERROR, exc.message, data={"remediation": exc.remediation}, headers={"WWW-Authenticate": 'Bearer realm="olcf-mcp"'})
    except OlcfError as exc:
        return _fail(403 if exc.status == 403 else exc.status, SERVER_ERROR, exc.message, data={"remediation": exc.remediation})
    ctype = request.headers.get("content-type", "").split(";")[0].strip().lower()
    if ctype != "application/json":
        return _fail(415, INVALID_REQUEST, "Content-Type must be application/json")
    raw = await _read_body(request, svc.cfg("mcp.max_request_bytes", 1_048_576))
    if raw is None:
        return _fail(413, INVALID_REQUEST, "request body is too large")
    ip = _client_ip(request, ctx)
    sid_header, ver_header = request.headers.get(protocol.SESSION_HEADER), request.headers.get(protocol.VERSION_HEADER)
    try:
        body = protocol.parse_body(raw)
        if isinstance(body, list):
            return await run_in_threadpool(_batch, svc, principal, body, sid_header, ver_header, ip)
        protocol.classify(body)
        session, version = await run_in_threadpool(_prepare, svc, principal, body, sid_header, ver_header, ip)
        if (isinstance(body, dict) and body.get("method") == "tools/call" and "id" in body and "text/event-stream" in request.headers.get("accept", "")
                and isinstance(((body.get("params") or {}).get("_meta") or {}).get("progressToken"), (str, int))):
            return _sse_call(svc, principal, body, session, ip)
        reply = await run_in_threadpool(lambda: svc.handle(principal, body, session=session, ip=ip))
    except RpcError as exc:
        return _fail(exc.http_status or (400 if exc.code in (protocol.PARSE_ERROR, INVALID_REQUEST) else 200), exc.code, exc.message, data=exc.data)
    return _reply_response(reply)


def _reply_response(reply: Reply) -> Response:
    headers = dict(NO_STORE)
    if reply.session_id:
        headers[protocol.SESSION_HEADER] = reply.session_id
    if reply.body is None:
        return Response(status_code=202, headers=headers)
    return JSONResponse(reply.body, status_code=reply.status, headers=headers)


def _batch(svc: McpService, principal: Principal, items: list[Any], sid_header: str | None, ver_header: str | None, ip: str | None) -> Response:
    """JSON-RPC batches exist only in protocol 2025-03-26; later versions removed them."""
    if not items:
        return _fail(400, INVALID_REQUEST, "empty batch")
    try:
        session, version = _prepare(svc, principal, {"method": "batch"}, sid_header, ver_header, ip)
    except RpcError as exc:
        return _fail(exc.http_status or 400, exc.code, exc.message, data=exc.data)
    if version != "2025-03-26":
        return _fail(400, INVALID_REQUEST, f"JSON-RPC batches are not supported by protocol {version}")
    if len(items) > 50:
        return _fail(400, INVALID_REQUEST, "at most 50 messages per batch")
    out: list[dict[str, Any]] = []
    for m in items:
        try:
            if isinstance(m, dict) and m.get("method") == "initialize":
                raise RpcError(INVALID_REQUEST, "initialize must not be part of a batch")
            r = svc.handle(principal, m, session=session, ip=ip)
            if r.body is not None:
                out.append(r.body)
        except RpcError as exc:
            out.append(exc.body(m.get("id") if isinstance(m, dict) else None))
    if not out:
        return Response(status_code=202, headers=NO_STORE)
    return JSONResponse(out, headers=NO_STORE)


def _sse_call(svc: McpService, principal: Principal, body: dict[str, Any], session: dict[str, Any] | None, ip: str | None) -> StreamingResponse:
    token = body["params"]["_meta"]["progressToken"]
    interval = max(1, svc.cfg("mcp.progress_interval_seconds", 5))

    async def gen():  # noqa: ANN202
        loop = asyncio.get_running_loop()
        fut = loop.run_in_executor(None, lambda: svc.handle(principal, body, session=session, ip=ip))
        n = 1
        yield _event(protocol.notification("notifications/progress", {"progressToken": token, "progress": n, "message": "started"}))
        while True:
            done, _ = await asyncio.wait({fut}, timeout=interval)
            if done:
                break
            n += 1
            yield _event(protocol.notification("notifications/progress", {"progressToken": token, "progress": n, "message": f"still running ({(n - 1) * interval}s)"}))
        yield _event(fut.result().body or {})

    return StreamingResponse(gen(), media_type="text/event-stream", headers={**NO_STORE, "X-Accel-Buffering": "no"})


@public.delete("/mcp")
async def mcp_delete(request: Request) -> Response:
    ctx: AppContext = request.app.state.ctx
    svc: McpService = ctx.service("mcp")
    problem = origin_problem(ctx, request)
    if problem:
        return _fail(403, SERVER_ERROR, problem)
    try:
        principal = await run_in_threadpool(_authenticate, ctx, request)
    except OlcfError as exc:
        return _fail(401, SERVER_ERROR, exc.message, headers={"WWW-Authenticate": 'Bearer realm="olcf-mcp"'})
    sid = request.headers.get(protocol.SESSION_HEADER)
    if not sid:
        return _fail(400, INVALID_REQUEST, "missing Mcp-Session-Id header")
    closed = await run_in_threadpool(svc.close_session, sid, principal)
    return Response(status_code=204 if closed else 404, headers=NO_STORE)


@public.get("/mcp")
async def mcp_get(request: Request) -> Response:
    return Response(status_code=405, headers={**NO_STORE, "Allow": "POST, DELETE"}, content=json.dumps(RpcError(SERVER_ERROR, "this server does not open server-to-client streams; use POST").body(None)),
                    media_type="application/json")


# ----------------------------------------------------------------------------- management API (/v1/mcp)
@api.get("/info")
def info(request: Request, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Endpoint URL, tools visible to you, limits and ready-to-paste client configuration (Claude Desktop and HTTP clients)."""
    return ctx.service("mcp").info(str(request.base_url).rstrip("/"), principal)


@api.get("/calls")
def calls(principal: Authed, ctx: Ctx, page: Pagination, tool: str | None = Query(None, max_length=64), status: str | None = Query(None, pattern="^(ok|error|denied)$"),
          principal_id: str | None = None, mine: bool = True) -> dict[str, Any]:
    """Audit trail of MCP calls: yours, or everyone's with ``audit.read`` (``mine=false``).  Arguments are redacted."""
    items, nxt = ctx.service("mcp").list_calls(principal, tool=tool, status=status, principal_id=principal_id, mine=mine, limit=page.limit, cursor=page.cursor)
    return {"items": items, "next_cursor": nxt}
