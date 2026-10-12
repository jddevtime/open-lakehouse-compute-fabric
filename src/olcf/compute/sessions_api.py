# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""REST + WebSocket API of notebook sessions (CONTRACTS 4.4).

REST: ``POST /v1/sessions`` ``GET /v1/sessions`` ``GET|DELETE /v1/sessions/{id}`` ``POST /v1/sessions/{id}/interrupt|restart|execute|complete`` and
``GET /v1/sessions/{id}/events`` (long-poll fallback for clients without WebSocket support).

WebSocket ``GET /v1/sessions/{id}/ws`` (``Authorization: Bearer`` or ``?access_token=``), JSON text messages:

client -> server  ``{"op":"execute","cell_id":"c1","code":"..."}``, ``{"op":"interrupt"}``, ``{"op":"restart"}``, ``{"op":"complete","code":"..","cursor":3,"req":"r1"}``, ``{"op":"ping"}``
server -> client  ``{"type":"status","state":"starting|idle|busy|dead","detail":".."}``, ``{"type":"output","cell_id":"c1","output":{nbformat output}}``,
                  ``{"type":"reply","cell_id":"c1","status":"ok|error|aborted","execution_count":3}``, ``{"type":"spark","application_id":"..","ui_url":"/v1/spark-applications/../ui/"}``,
                  ``{"type":"complete","req":"r1","matches":[..],"cursor_start":0,"cursor_end":3}``, ``{"type":"clear_output","cell_id":"c1","wait":false}``,
                  ``{"type":"error","op":"execute","code":"conflict","message":".."}``, ``{"type":"pong"}``.

On (re)connect the server sends ``status`` (and ``spark``) followed by the replay of the newest 500 outputs, then streams live. Several browsers may attach to one
session. Closing the socket never stops the kernel; ``DELETE`` or the idle timeout does. Close codes: 4401 unauthenticated, 4403 forbidden, 4404 unknown session,
4410 session ended (after the final ``status: dead`` was delivered).
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

from olcf.api.deps import Authed, Ctx, Pagination, StreamGuard, authenticate
from olcf.compute.sessions import SessionCreate
from olcf.core.errors import BadRequest, NotFound, OlcfError, Unauthenticated

log = logging.getLogger("olcf.compute.sessions_api")
router = APIRouter(tags=["sessions"])
MAX_CLIENT_MESSAGE = 5 * 1024 * 1024
MAX_CONCURRENT_OPS = 8


class ExecuteBody(BaseModel):
    """Request body that runs one cell of code in a session."""
    cell_id: str = Field(min_length=1, max_length=64)
    code: str


class CompleteBody(BaseModel):
    """Request body that asks the kernel for completions at a cursor position."""
    code: str
    cursor: int | None = None


def event_to_message(ev: dict[str, Any], *, spark_ui: str | None = None) -> dict[str, Any] | None:
    """One ``session_events`` row -> the browser message of CONTRACTS 4.4 (``None`` for rows the browser does not receive)."""
    d = ev.get("data") or {}
    kind = ev["kind"]
    base = {"seq": ev["seq"], "ts": ev["ts"]}
    if kind == "output":
        return {"type": "output", "cell_id": ev.get("cell_id"), "output": d.get("output") or {}, **base}
    if kind == "reply":
        return {"type": "reply", "cell_id": ev.get("cell_id"), **{k: v for k, v in d.items() if k in ("status", "execution_count", "ename", "evalue", "traceback")}, **base}
    if kind == "clear":
        return {"type": "clear_output", "cell_id": ev.get("cell_id"), "wait": bool(d.get("wait")), **base}
    if kind == "spark":
        return {"type": "spark", "application_id": d.get("application_id"), "app_name": d.get("app_name"), "ui_url": spark_ui, **base}
    if kind == "status":
        if d.get("state"):
            return {"type": "status", "state": d["state"], "detail": d.get("detail") or "", **base}
        return {"type": "event", "event": d.get("event"), "data": {k: v for k, v in d.items() if k != "event"}, **base}
    return None


def _ui_of(svc: Any, session_id: str) -> str | None:
    return ((svc.public(svc.get_row(session_id)).get("spark")) or {}).get("ui")


# ---------------------------------------------------------------------------------------------------------- REST
@router.post("", status_code=201)
def create_session(body: SessionCreate, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    svc = ctx.service("sessions")
    return svc.public(svc.create(principal, body))


@router.get("")
def list_sessions(principal: Authed, ctx: Ctx, page: Pagination, workspace_id: str | None = None, state: str | None = Query(None, pattern="^(starting|idle|busy|dead)$"),
                  mine: bool = True, include_hidden: bool = False) -> dict[str, Any]:
    svc = ctx.service("sessions")
    rows, nxt = svc.list(principal, workspace_id=workspace_id, state=state, mine=mine, include_hidden=include_hidden, limit=page.limit, cursor=page.cursor)
    return {"items": [svc.public(r) for r in rows], "next_cursor": nxt}


@router.get("/{session_id}")
def get_session(session_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    svc = ctx.service("sessions")
    return svc.public(svc.authorize(principal, session_id))


@router.delete("/{session_id}")
def delete_session(session_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    return ctx.service("sessions").delete(principal, session_id)


@router.post("/{session_id}/interrupt")
def interrupt_session(session_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    return ctx.service("sessions").interrupt(principal, session_id)


@router.post("/{session_id}/restart")
def restart_session(session_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    return ctx.service("sessions").restart(principal, session_id)


@router.post("/{session_id}/execute", status_code=202)
def execute_in_session(session_id: str, body: ExecuteBody, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Queue code for execution; read the outputs from ``/events`` (or the WebSocket). Returns the ``msg_id``."""
    return ctx.service("sessions").execute(principal, session_id, body.cell_id, body.code)


@router.post("/{session_id}/complete")
def complete_in_session(session_id: str, body: CompleteBody, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    return ctx.service("sessions").complete(principal, session_id, body.code, body.cursor)


@router.get("/{session_id}/events")
async def session_events(session_id: str, principal: Authed, ctx: Ctx, after_seq: int = Query(0, ge=0), limit: int = Query(200, ge=1, le=1000),
                         wait: float = Query(0.0, ge=0.0, le=30.0), replay: bool = False) -> dict[str, Any]:
    """Poll the relay: messages after ``after_seq`` (optionally long-polling up to ``wait`` seconds). ``replay=true`` starts with the last 500 outputs."""
    svc = ctx.service("sessions")
    row = await asyncio.to_thread(svc.authorize, principal, session_id)
    ui = await asyncio.to_thread(_ui_of, svc, session_id)
    loop = asyncio.get_running_loop()
    ev = asyncio.Event()
    unsub = svc.subscribe(session_id, loop, ev)
    try:
        if replay and after_seq == 0:
            head = await asyncio.to_thread(svc.head_seq, session_id)
            rows = await asyncio.to_thread(svc.replay, session_id, head)
            after_seq = head
        else:
            rows = await asyncio.to_thread(svc.events_after, session_id, after_seq, limit)
            deadline = loop.time() + wait
            while not rows and wait and loop.time() < deadline:
                try:
                    await asyncio.wait_for(ev.wait(), min(0.5, max(0.01, deadline - loop.time())))
                except TimeoutError:
                    pass
                ev.clear()
                rows = await asyncio.to_thread(svc.events_after, session_id, after_seq, limit)
    finally:
        unsub()
    msgs = [m for m in (event_to_message(r, spark_ui=ui) for r in rows) if m is not None]
    last = max([after_seq] + [r["seq"] for r in rows])
    fresh = await asyncio.to_thread(svc.get_row, session_id)
    return {"items": msgs, "next_seq": last, "state": fresh["state"], "detail": fresh["state_reason"], "execution_count": fresh["execution_count"], "workspace_id": row["workspace_id"]}


# ---------------------------------------------------------------------------------------------------------- WebSocket
async def _send(ws: WebSocket, msg: dict[str, Any]) -> None:
    await ws.send_text(json.dumps(msg, default=str, separators=(",", ":")))


@router.websocket("/{session_id}/ws")
async def session_ws(ws: WebSocket, session_id: str) -> None:
    ctx = ws.app.state.ctx
    svc = ctx.service("sessions")
    await ws.accept()
    token = bearer_token_ws(ws)
    try:
        if not token:
            raise Unauthenticated("authentication required")
        principal = await asyncio.to_thread(authenticate, ctx, token, ip=ws.client.host if ws.client else None)
        await asyncio.to_thread(svc.authorize, principal, session_id)
    except Unauthenticated as exc:
        await ws.close(code=4401, reason=exc.message[:120])
        return
    except NotFound:
        await ws.close(code=4404, reason="unknown session")
        return
    except OlcfError as exc:
        await ws.close(code=4403, reason=exc.message[:120])
        return

    loop = asyncio.get_running_loop()
    wake = asyncio.Event()
    unsub = svc.subscribe(session_id, loop, wake)
    ops: set[asyncio.Task[None]] = set()
    try:
        head = await asyncio.to_thread(svc.head_seq, session_id)
        replay = await asyncio.to_thread(svc.replay, session_id, head)
        pub = await asyncio.to_thread(lambda: svc.public(svc.get_row(session_id)))
        await _send(ws, {"type": "status", "state": pub["state"], "detail": pub["state_reason"] or "", "execution_count": pub["execution_count"]})
        if pub.get("spark"):
            await _send(ws, {"type": "spark", "application_id": pub["spark"]["application_id"], "ui_url": pub["spark"]["ui"], "app_name": pub["spark"].get("app_name")})
        ui = (pub.get("spark") or {}).get("ui")
        for r in replay:
            m = event_to_message(r, spark_ui=ui)
            if m is not None:
                m["replay"] = True
                await _send(ws, m)
        await _send(ws, {"type": "replay_done", "count": len(replay), "seq": head})
        holder = {"principal": principal}  # refreshed by the watcher: an open socket must not outlive the account, the token or the role
        tail = asyncio.create_task(_tail(ws, svc, session_id, head, wake, ui))
        reader = asyncio.create_task(_read_ops(ws, svc, holder, session_id, ops))
        watcher = asyncio.create_task(_watch(ctx, svc, StreamGuard(ctx, None, principal, interval=5.0, token=token), holder, session_id))
        done, pending = await asyncio.wait({tail, reader, watcher}, return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
        for t in done:
            exc = t.exception()
            if exc and not isinstance(exc, (WebSocketDisconnect, asyncio.CancelledError)):
                log.debug("session websocket ended: %r", exc)
        if watcher in done and not watcher.cancelled() and watcher.exception() is None:
            code, reason = watcher.result()
            await ws.close(code=code, reason=reason)
        elif tail in done and not tail.cancelled() and tail.exception() is None:
            await ws.close(code=4410, reason="session ended")
    except WebSocketDisconnect:
        pass
    except Exception:  # noqa: BLE001
        log.exception("session websocket for %s failed", session_id)
        try:
            await ws.close(code=1011, reason="internal error")
        except Exception:  # noqa: BLE001, S110
            pass
    finally:
        unsub()
        for t in ops:
            t.cancel()


def bearer_token_ws(ws: WebSocket) -> str | None:
    auth = ws.headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return ws.query_params.get("access_token")


async def _tail(ws: WebSocket, svc: Any, session_id: str, last_seq: int, wake: asyncio.Event, ui: str | None) -> None:
    """Stream new relay rows; returns when the session is dead and everything was delivered."""
    while True:
        rows = await asyncio.to_thread(svc.events_after, session_id, last_seq, 200)
        for r in rows:
            last_seq = r["seq"]
            if r["kind"] == "spark":
                ui = await asyncio.to_thread(_ui_of, svc, session_id) or ui
            m = event_to_message(r, spark_ui=ui)
            if m is not None:
                await _send(ws, m)
        if rows:
            continue
        row = await asyncio.to_thread(svc.get_row, session_id)
        if row["state"] == "dead":
            return
        try:
            await asyncio.wait_for(wake.wait(), 0.5)  # cross-replica events are found by this poll
        except TimeoutError:
            pass
        wake.clear()


async def _watch(ctx: Any, svc: Any, guard: StreamGuard, holder: dict[str, Any], session_id: str) -> tuple[int, str]:
    """Returns the WebSocket close ``(code, reason)`` once the connection's credential or its right to use the session is gone; keeps ``holder`` current meanwhile."""
    while True:
        await asyncio.sleep(1.0)
        fresh = await guard.current()
        if fresh is None:
            return 4401, "credentials no longer valid"
        if fresh is not holder["principal"]:
            try:
                await asyncio.to_thread(svc.authorize, fresh, session_id)
            except NotFound:
                return 4404, "unknown session"
            except OlcfError:
                return 4403, "access to this session was withdrawn"
            holder["principal"] = fresh


async def _read_ops(ws: WebSocket, svc: Any, holder: dict[str, Any], session_id: str, ops: set[asyncio.Task[None]]) -> None:
    while True:
        text = await ws.receive_text()
        if len(text) > MAX_CLIENT_MESSAGE:
            await _send(ws, {"type": "error", "code": "bad_request", "message": "message too large"})
            continue
        try:
            msg = json.loads(text)
            if not isinstance(msg, dict):
                raise ValueError("not an object")
        except ValueError:
            await _send(ws, {"type": "error", "code": "bad_request", "message": "messages must be JSON objects"})
            continue
        op = msg.get("op")
        if op == "ping":
            await _send(ws, {"type": "pong"})
            continue
        if op not in ("execute", "interrupt", "restart", "complete"):
            await _send(ws, {"type": "error", "code": "bad_request", "message": f"unknown op {op!r}", "op": op})
            continue
        if len([t for t in ops if not t.done()]) >= MAX_CONCURRENT_OPS:
            await _send(ws, {"type": "error", "code": "too_many_requests", "message": "too many operations in flight", "op": op})
            continue
        task = asyncio.create_task(_run_op(ws, svc, holder["principal"], session_id, op, msg))
        ops.add(task)
        task.add_done_callback(ops.discard)


async def _run_op(ws: WebSocket, svc: Any, principal: Any, session_id: str, op: str, msg: dict[str, Any]) -> None:
    try:
        if op == "execute":
            cell, code = msg.get("cell_id"), msg.get("code")
            if not isinstance(cell, str) or not isinstance(code, str):
                raise BadRequest("execute needs string 'cell_id' and 'code'")
            r = await asyncio.to_thread(svc.execute, principal, session_id, cell, code)
            await _send(ws, {"type": "accepted", "cell_id": cell, "msg_id": r["msg_id"]})
        elif op == "interrupt":
            await asyncio.to_thread(svc.interrupt, principal, session_id)
        elif op == "restart":
            await asyncio.to_thread(svc.restart, principal, session_id)
        elif op == "complete":
            code = msg.get("code")
            if not isinstance(code, str):
                raise BadRequest("complete needs a string 'code'")
            cursor = msg.get("cursor")
            res = await asyncio.to_thread(svc.complete, principal, session_id, code, int(cursor) if isinstance(cursor, int) else None)
            await _send(ws, {"type": "complete", "req": msg.get("req"), "matches": res.get("matches", []), "cursor_start": res.get("cursor_start"), "cursor_end": res.get("cursor_end"),
                             "metadata": res.get("metadata", {})})
    except OlcfError as exc:
        await _send(ws, {"type": "error", "op": op, "code": exc.code, "message": exc.message, "remediation": exc.remediation, "req": msg.get("req"), "cell_id": msg.get("cell_id")})
    except WebSocketDisconnect:
        raise
    except Exception:  # noqa: BLE001
        log.exception("session op %s failed", op)
        try:
            await _send(ws, {"type": "error", "op": op, "code": "internal", "message": "internal error", "req": msg.get("req")})
        except Exception:  # noqa: BLE001, S110
            pass

