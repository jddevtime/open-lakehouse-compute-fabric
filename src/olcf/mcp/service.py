# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The ``mcp`` service: protocol sessions, dispatch of JSON-RPC methods, per-call authorisation, audit and result-size control.

Everything transport-independent lives here (the HTTP layer and the deep health check both call :meth:`McpService.handle`), so what
the health check proves is what a real agent gets.
"""

from __future__ import annotations

import json
import logging
import secrets
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, func, insert, select, update

from olcf.core.errors import Forbidden, OlcfError, TooManyRequests
from olcf.core.ids import ulid
from olcf.core.redaction import redact_obj, redact_text
from olcf.core.timeutil import from_iso, iso_in, now_iso
from olcf.db.util import fetch_all, fetch_one, paginate
from olcf.mcp import prompts, protocol, resources
from olcf.mcp.protocol import (
    INTERNAL_ERROR,
    INVALID_PARAMS,
    INVALID_REQUEST,
    METHOD_NOT_FOUND,
    SERVER_ERROR,
    RpcError,
    Tool,
)
from olcf.mcp.schema import mcp_calls, mcp_sessions
from olcf.mcp.tools import Env, build_tools
from olcf.security.rbac import Binding, Principal
from olcf.version import __version__

if TYPE_CHECKING:
    from olcf.core.context import AppContext

log = logging.getLogger("olcf.mcp")
SERVER_NAME = "olcf"
INSTRUCTIONS = (
    "Open Lakehouse Compute Fabric (OLCF). Explore with list_workspaces -> list_lakehouses -> list_tables -> describe_table, then answer questions with run_sql "
    "(read-only DuckDB; reference tables as <lakehouse>.<table>). Prefer aggregating in SQL over pulling rows, check the 'truncated' flag, and use :name parameters for "
    "values. For questions about where data comes from and what depends on it use the intel_* tools (intel_where_does_this_come_from, intel_impact_analysis, intel_lineage, "
    "intel_staleness; start with intel_search_assets): they report a confidence for every link and list the gaps that make a trace incomplete, so quote both. "
    "You can only see what the person or token behind this connection may see."
)
MAX_ARG_AUDIT = 4000


@dataclass
class Reply:
    """Result of handling one JSON-RPC message: the response body (``None`` for notifications) plus HTTP details."""

    body: dict[str, Any] | None
    session_id: str | None = None
    status: int = 200


class McpService:
    """Implements the MCP tools and request handling on top of the application services, applying the caller's permissions to every tool call.
    """
    def __init__(self, ctx: AppContext) -> None:
        self.ctx = ctx
        self.tools: list[Tool] = build_tools()
        self._by_name = {t.name: t for t in self.tools}

    # ------------------------------------------------------------------ settings
    def cfg(self, key: str, default: int) -> int:
        try:
            v = self.ctx.service("settings").get(key)
            return int(v) if v is not None else default
        except Exception:  # noqa: BLE001
            return default

    def enabled(self) -> bool:
        try:
            v = self.ctx.service("settings").get("mcp.enabled")
            return True if v is None else bool(v)
        except Exception:  # noqa: BLE001
            return True

    # ------------------------------------------------------------------ access
    def check_access(self, principal: Principal) -> None:
        """The credential must be allowed to use the endpoint at all (``mcp.use`` for some workspace, through its token scopes)."""
        vis = principal.visible_workspaces("mcp.use")
        if vis is not None and not vis:
            raise Forbidden("this credential may not use the MCP endpoint", remediation="use a platform token with the 'mcp' scope (or 'mcp:write'); the account itself needs a role "
                            "that includes mcp.use, e.g. Contributor", details={"permission": "mcp.use"})

    def _available(self, tool: Tool) -> bool:
        """Whether the module a tool depends on is installed: ``Tool.service`` is a platform service (``"intelligence"``) or, dotted, an optional package the service carries
        (``"intelligence.governance"`` is the governance package attached to the Intelligence service, which stays ``None`` when it is not installed)."""
        if not tool.service:
            return True
        name, _, attr = tool.service.partition(".")
        svc = self.ctx.maybe_service(name)
        return svc is not None and (not attr or getattr(svc, attr, None) is not None)

    def visible_tools(self, principal: Principal) -> list[Tool]:
        out = []
        for t in self.tools:
            if not self._available(t):
                continue
            if all((v := principal.visible_workspaces(p)) is None or v for p in t.needs):
                out.append(t)
        return out

    # ------------------------------------------------------------------ sessions
    def create_session(self, principal: Principal, version: str, client: dict[str, Any], caps: dict[str, Any]) -> str:
        sid = "mcp_" + secrets.token_urlsafe(24)
        idle = self.cfg("mcp.session_idle_minutes", 60) * 60
        with self.ctx.db.begin() as conn:
            conn.execute(insert(mcp_sessions).values(
                id=sid, principal_id=principal.id, token_id=principal.token_id, protocol_version=version, client_name=str(client.get("name") or "")[:128] or None,
                client_version=str(client.get("version") or "")[:64] or None, client_capabilities=redact_obj(caps) if isinstance(caps, dict) else {},
                created_at=now_iso(), last_seen_at=now_iso(), expires_at=iso_in(idle)))
        return sid

    def get_session(self, sid: str, principal: Principal) -> dict[str, Any]:
        """The live session ``sid`` owned by ``principal``; anything else is reported as unknown (404) so ids cannot be probed."""
        with self.ctx.db.connect() as conn:
            row = fetch_one(conn, select(mcp_sessions).where(mcp_sessions.c.id == sid))
        if row is None or row["principal_id"] != principal.id or row["closed_at"] is not None or from_iso(row["expires_at"]).timestamp() < time.time():
            raise RpcError(SERVER_ERROR, "unknown or expired session", {"remediation": "send a new initialize request"}, http_status=404)
        idle = self.cfg("mcp.session_idle_minutes", 60) * 60
        with self.ctx.db.begin() as conn:
            conn.execute(update(mcp_sessions).where(mcp_sessions.c.id == sid).values(last_seen_at=now_iso(), expires_at=iso_in(idle)))
        return row

    def close_session(self, sid: str, principal: Principal) -> bool:
        with self.ctx.db.begin() as conn:
            n = conn.execute(update(mcp_sessions).where(mcp_sessions.c.id == sid, mcp_sessions.c.principal_id == principal.id, mcp_sessions.c.closed_at.is_(None))
                             .values(closed_at=now_iso())).rowcount
        return bool(n)

    def purge(self) -> dict[str, int]:
        """Delete expired / closed sessions and calls older than ``mcp.calls_retention_days``."""
        from datetime import UTC, datetime, timedelta

        cutoff = (datetime.now(UTC) - timedelta(days=self.cfg("mcp.calls_retention_days", 90))).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        with self.ctx.db.begin() as conn:
            s = conn.execute(delete(mcp_sessions).where((mcp_sessions.c.expires_at < now_iso()) | (mcp_sessions.c.closed_at.is_not(None)))).rowcount or 0
            c = conn.execute(delete(mcp_calls).where(mcp_calls.c.created_at < cutoff)).rowcount or 0
        return {"sessions": int(s), "calls": int(c)}

    def active_sessions(self, principal: Principal | None = None) -> int:
        stmt = select(func.count()).select_from(mcp_sessions).where(mcp_sessions.c.closed_at.is_(None), mcp_sessions.c.expires_at > now_iso())
        if principal is not None:
            stmt = stmt.where(mcp_sessions.c.principal_id == principal.id)
        with self.ctx.db.connect() as conn:
            return int(conn.execute(stmt).scalar() or 0)

    # ------------------------------------------------------------------ audit
    def audit(self, principal: Principal, session: dict[str, Any] | None, method: str, name: str | None, args: Any, status: str, *, workspace_id: str | None = None,
              error: tuple[str, str] | None = None, started: float | None = None, result_bytes: int | None = None, truncated: bool = False, ip: str | None = None) -> None:
        red: Any = None
        if args:
            red = redact_obj(args)
            if len(json.dumps(red, default=str)) > MAX_ARG_AUDIT:
                red = {"_truncated": True, "keys": sorted(red)[:50] if isinstance(red, dict) else None, "preview": json.dumps(red, default=str)[:MAX_ARG_AUDIT // 2]}
        try:
            with self.ctx.db.begin() as conn:
                conn.execute(insert(mcp_calls).values(
                    id=f"mcl_{ulid()}", session_id=(session or {}).get("id"), principal_id=principal.id, principal_name=principal.name, token_id=principal.token_id,
                    client_name=(session or {}).get("client_name"), workspace_id=workspace_id, method=method, tool=(name or "")[:64] or None, arguments=red, status=status,
                    error_code=error[0][:64] if error else None, error_message=redact_text(error[1])[:1000] if error else None,
                    elapsed_ms=None if started is None else int((time.perf_counter() - started) * 1000), result_bytes=result_bytes, truncated="true" if truncated else None,
                    ip=principal.ip or ip, created_at=now_iso()))
        except Exception:  # noqa: BLE001 - the call already happened; never turn an audit hiccup into a failed call, but be loud about it
            log.exception("could not record MCP call audit row")

    # ------------------------------------------------------------------ dispatch
    def handle(self, principal: Principal, msg: dict[str, Any], *, session: dict[str, Any] | None, ip: str | None = None, record: bool = True) -> Reply:
        kind = protocol.classify(msg)
        if kind == "response":
            return Reply(None, status=202)
        if kind == "notification":
            return Reply(None, status=202)               # initialized / cancelled / progress: nothing to do (calls are not cancellable mid-flight)
        method, req_id = msg["method"], msg["id"]
        params = msg.get("params") or {}
        if isinstance(params, list):
            return Reply(RpcError(INVALID_PARAMS, "params must be an object").body(req_id))
        started = time.perf_counter()
        try:
            if method != "ping" and method != "initialize":
                self.ctx.limiter.check(f"mcp:{principal.id}", limit=self.cfg("mcp.rate_limit_per_minute", 120), window=60.0)
        except TooManyRequests as exc:
            if record:
                self.audit(principal, session, method, None, None, "denied", error=("rate_limited", exc.message), started=started, ip=ip)
            return Reply(RpcError(SERVER_ERROR, exc.message, {"retry_after_seconds": (exc.details or {}).get("retry_after_seconds")}, http_status=429).body(req_id), status=429)
        try:
            if method == "initialize":
                return self._initialize(principal, req_id, params, ip, record)
            if method.startswith("server/"):          # newer-protocol capability probes (server/discover): tell the client this is a handshake-era server
                raise RpcError(METHOD_NOT_FOUND, f"method '{method[:100]}' is not supported; use the initialize handshake")
            if session is None and method != "ping":
                raise RpcError(INVALID_REQUEST, "no session: send initialize first and pass the Mcp-Session-Id header", http_status=400)
            if method == "ping":
                return Reply(protocol.ok(req_id, {}))
            env = Env(self.ctx, principal, (session or {}).get("id"))
            version = (session or {}).get("protocol_version", protocol.LATEST_VERSION)
            if method == "tools/list":
                return Reply(protocol.ok(req_id, {"tools": [t.descriptor(version) for t in self.visible_tools(principal)]}))
            if method == "tools/call":
                return Reply(protocol.ok(req_id, self.call_tool(env, session, params, version, ip, record)))
            if method == "resources/list":
                return Reply(protocol.ok(req_id, resources.list_resources(env, params.get("cursor"))))
            if method == "resources/templates/list":
                return Reply(protocol.ok(req_id, {"resourceTemplates": resources.TEMPLATES}))
            if method == "resources/read":
                return Reply(protocol.ok(req_id, self._audited(env, session, "resources/read", str(params.get("uri", "")), params, ip, record,
                                                              lambda: resources.read_resource(env, self._need_str(params, "uri")))))
            if method == "prompts/list":
                return Reply(protocol.ok(req_id, {"prompts": prompts.PROMPTS}))
            if method == "prompts/get":
                return Reply(protocol.ok(req_id, self._audited(env, session, "prompts/get", str(params.get("name", "")), params, ip, record,
                                                              lambda: prompts.get_prompt(env, self._need_str(params, "name"), params.get("arguments") or {}))))
            raise RpcError(METHOD_NOT_FOUND, f"method '{method[:100]}' is not supported")
        except RpcError as exc:
            return Reply(exc.body(req_id), status=exc.http_status or 200)
        except OlcfError as exc:           # service errors raised by resources/prompts surface as JSON-RPC errors
            return Reply(RpcError(INVALID_PARAMS if exc.status < 500 else INTERNAL_ERROR, exc.message, {"code": exc.code, "remediation": exc.remediation}).body(req_id))
        except Exception:  # noqa: BLE001
            log.exception("MCP method %s failed", method)
            return Reply(RpcError(INTERNAL_ERROR, "internal error").body(req_id))

    @staticmethod
    def _need_str(params: dict[str, Any], key: str) -> str:
        v = params.get(key)
        if not isinstance(v, str) or not v:
            raise RpcError(INVALID_PARAMS, f"'{key}' is required")
        return v

    def _initialize(self, principal: Principal, req_id: Any, params: dict[str, Any], ip: str | None, record: bool) -> Reply:
        started = time.perf_counter()
        client = params.get("clientInfo")
        caps = params.get("capabilities")
        if not isinstance(params.get("protocolVersion"), str) or not isinstance(client, dict) or not isinstance(caps, dict):
            raise RpcError(INVALID_PARAMS, "initialize needs protocolVersion, capabilities and clientInfo")
        version = protocol.negotiate(params["protocolVersion"])
        sid = self.create_session(principal, version, client, caps)
        result = {"protocolVersion": version,
                  "capabilities": {"tools": {"listChanged": False}, "resources": {"subscribe": False, "listChanged": False}, "prompts": {"listChanged": False}},
                  "serverInfo": {"name": SERVER_NAME, "title": "Open Lakehouse Compute Fabric", "version": __version__}, "instructions": INSTRUCTIONS}
        if version < "2025-06-18":
            result["serverInfo"].pop("title")
        if record:
            self.audit(principal, {"id": sid, "client_name": str(client.get("name") or "")[:128]}, "initialize", None, {"clientInfo": client, "protocolVersion": params["protocolVersion"]},
                       "ok", started=started, ip=ip)
        return Reply(protocol.ok(req_id, result), session_id=sid)

    def _audited(self, env: Env, session: dict[str, Any] | None, method: str, name: str, args: Any, ip: str | None, record: bool, fn: Any) -> Any:
        started = time.perf_counter()
        try:
            out = fn()
        except RpcError as exc:
            if record:
                self.audit(env.principal, session, method, name, args, "error", error=(str(exc.code), exc.message), started=started, ip=ip)
            raise
        except OlcfError as exc:
            if record:
                self.audit(env.principal, session, method, name, args, "denied" if isinstance(exc, Forbidden) else "error", error=(exc.code, exc.message), started=started, ip=ip)
            raise
        if record:
            ws = next(iter(env.workspace_touched)) if len(env.workspace_touched) == 1 else None
            self.audit(env.principal, session, method, name, args, "ok", workspace_id=ws, started=started, result_bytes=len(json.dumps(out, default=str)), ip=ip)
        return out

    # ------------------------------------------------------------------ tools/call
    def call_tool(self, env: Env, session: dict[str, Any] | None, params: dict[str, Any], version: str, ip: str | None, record: bool) -> dict[str, Any]:
        started = time.perf_counter()
        name = params.get("name")
        args = params.get("arguments")
        if not isinstance(name, str) or name not in self._by_name:
            raise RpcError(INVALID_PARAMS, f"unknown tool '{str(name)[:100]}'", {"tools": [t.name for t in self.visible_tools(env.principal)]})
        tool = self._by_name[name]
        if not self._available(tool):
            raise RpcError(INVALID_PARAMS, f"unknown tool '{name}'", {"tools": [t.name for t in self.visible_tools(env.principal)]})
        if args is None:
            args = {}
        if not isinstance(args, dict):
            raise RpcError(INVALID_PARAMS, "arguments must be an object")

        def fail(err: OlcfError | None, code: str, message: str, remediation: str | None, status: str, details: Any = None) -> dict[str, Any]:
            if record:
                self.audit(env.principal, session, "tools/call", name, args, status, error=(code, message), started=started, ip=ip,
                           workspace_id=next(iter(env.workspace_touched)) if len(env.workspace_touched) == 1 else None)
            body = {"error": {"code": code, "message": message, **({"remediation": remediation} if remediation else {}), **({"details": details} if details else {})}}
            return self._result(body, version, is_error=True)

        if not all((v := env.principal.visible_workspaces(p)) is None or v for p in tool.needs):
            return fail(None, "forbidden", f"this credential may not use the tool '{name}'",
                        "write tools need a token with the 'mcp:write' scope and a role that can submit jobs" if tool.write else "the account or token lacks the needed permission", "denied")
        problems = protocol.validate_arguments(tool.schema, args)
        if problems:
            return fail(None, "invalid_arguments", "; ".join(problems[:6]), f"see the inputSchema of {name} (tools/list)", "error")
        try:
            data = tool.fn(env, args)
        except Forbidden as exc:
            return fail(exc, exc.code, exc.message, exc.remediation, "denied", exc.details)
        except OlcfError as exc:
            return fail(exc, exc.code, exc.message, exc.remediation, "error", exc.details)
        except Exception as exc:  # noqa: BLE001
            log.exception("MCP tool %s crashed", name)
            return fail(None, "internal", f"the tool failed unexpectedly ({type(exc).__name__})", None, "error")
        data, trimmed = self._fit(data, self.cfg("mcp.max_result_bytes", 262_144))
        result = self._result(data, version)
        if record:
            self.audit(env.principal, session, "tools/call", name, args, "ok", started=started, ip=ip, result_bytes=len(result["content"][0]["text"]), truncated=trimmed,
                       workspace_id=next(iter(env.workspace_touched)) if len(env.workspace_touched) == 1 else None)
        return result

    @staticmethod
    def _result(data: dict[str, Any], version: str, *, is_error: bool = False) -> dict[str, Any]:
        text = json.dumps(data, default=str, ensure_ascii=False, separators=(",", ":"))
        out: dict[str, Any] = {"content": [{"type": "text", "text": text}], "isError": is_error}
        if version >= "2025-06-18":
            out["structuredContent"] = json.loads(text)
        return out

    @staticmethod
    def _fit(data: dict[str, Any], cap: int) -> tuple[dict[str, Any], bool]:
        """Shrink the largest row/list payload until the JSON fits ``cap`` bytes (and say so), rather than failing or flooding the model."""
        def size() -> int:
            return len(json.dumps(data, default=str, ensure_ascii=False, separators=(",", ":")).encode())

        if size() <= cap:
            return data, False

        def lists() -> list[tuple[list[Any], dict[str, Any], str]]:
            found = [(v, data, k) for k, v in data.items() if isinstance(v, list) and len(v) > 1]
            for t in data.get("tiles") or []:
                if isinstance(t, dict) and isinstance(t.get("rows"), list) and len(t["rows"]) > 1:
                    found.append((t["rows"], t, "rows"))
            return found

        for _ in range(40):
            cands = lists()
            if not cands:
                break
            biggest, owner, key = max(cands, key=lambda c: len(json.dumps(c[0], default=str)))
            del biggest[max(1, len(biggest) // 2):]
            if key == "rows" and "row_count" in owner and owner is data:
                owner["row_count"] = len(biggest)
            owner["truncated"] = True
            data["truncated"] = True
            data["note"] = "the result was cut to fit the size limit; narrow the query (WHERE / LIMIT / fewer columns) or aggregate in SQL"
            if size() <= cap:
                return data, True
        raise RpcError(INTERNAL_ERROR, "the result is larger than the size limit and cannot be reduced", {"limit_bytes": cap})

    # ------------------------------------------------------------------ views
    def info(self, base_url: str, principal: Principal) -> dict[str, Any]:
        url = (self.ctx.settings.server.public_url or base_url).rstrip("/") + "/mcp"
        tools = [{"name": t.name, "title": t.title, "description": t.description, "write": t.write} for t in self.visible_tools(principal)]
        return {"endpoint": url, "transport": "streamable-http", "protocol_versions": list(protocol.SUPPORTED_VERSIONS), "server": {"name": SERVER_NAME, "version": __version__},
                "enabled": self.enabled(), "auth": {"type": "bearer", "scopes": {"mcp": "read tools (catalog, SQL, reports, lineage and impact)", "mcp:write": "also submit and cancel notebook jobs"},
                                                    "header": "Authorization: Bearer <platform token>"},
                "tools": tools, "your_active_sessions": self.active_sessions(principal),
                "limits": {"rate_limit_per_minute": self.cfg("mcp.rate_limit_per_minute", 120), "max_result_bytes": self.cfg("mcp.max_result_bytes", 262_144),
                           "max_rows": self.cfg("mcp.max_rows", 5000), "sql_timeout_seconds": self.cfg("mcp.sql_timeout_seconds", 30)},
                "client_config": {
                    "claude_desktop": {"mcpServers": {"olcf": {"command": "python", "args": ["-m", "olcf.mcp.stdio", "--url", url], "env": {"OLCF_MCP_TOKEN": "<paste a platform token with the mcp scope>"}}}},
                    "http_clients": {"type": "http", "url": url, "headers": {"Authorization": "Bearer <paste a platform token with the mcp scope>"}},
                }}

    def list_calls(self, principal: Principal, *, tool: str | None, status: str | None, principal_id: str | None, mine: bool, limit: int, cursor: str | None) -> tuple[list[dict[str, Any]], str | None]:
        stmt = select(mcp_calls)
        all_ok = principal.can("audit.read")
        if mine or not all_ok:
            stmt = stmt.where(mcp_calls.c.principal_id == principal.id)
        elif principal_id:
            stmt = stmt.where(mcp_calls.c.principal_id == principal_id)
        if tool:
            stmt = stmt.where(mcp_calls.c.tool == tool)
        if status:
            stmt = stmt.where(mcp_calls.c.status == status)
        with self.ctx.db.connect() as conn:
            return paginate(conn, stmt, mcp_calls, limit=limit, cursor=cursor, descending=True)

    def stats(self) -> dict[str, Any]:
        with self.ctx.db.connect() as conn:
            rows = fetch_all(conn, select(mcp_calls.c.status, func.count().label("n")).group_by(mcp_calls.c.status))
        return {r["status"]: int(r["n"]) for r in rows}

    # ------------------------------------------------------------------ health
    def deep_check(self) -> tuple[bool, list[dict[str, Any]]]:
        """In-process run of the real protocol path: initialize -> tools/list -> tools/call -> resources/list -> close (nothing is recorded in the audit trail)."""
        steps: list[dict[str, Any]] = []
        probe = Principal(id="usr_mcp_probe", name="mcp-health-probe", kind="service", bindings=[Binding("PlatformAdmin")], token_scopes=["mcp"])
        state: dict[str, Any] = {"session": None, "id": 0}

        def rpc(method: str, params: dict[str, Any]) -> dict[str, Any]:
            state["id"] += 1
            r = self.handle(probe, {"jsonrpc": "2.0", "id": state["id"], "method": method, "params": params}, session=state["session"], record=False)
            if r.body is None or "error" in r.body:
                raise RuntimeError(json.dumps((r.body or {}).get("error"))[:300])
            if method == "initialize":
                state["session"] = {"id": r.session_id, "protocol_version": r.body["result"]["protocolVersion"], "client_name": "olcf-health"}
            return r.body["result"]

        def step(name: str, fn: Any) -> bool:
            t0 = time.perf_counter()
            try:
                detail = fn()
                ok = True
            except Exception as exc:  # noqa: BLE001
                detail, ok = f"{type(exc).__name__}: {str(exc)[:300]}", False
            steps.append({"name": name, "ok": ok, "detail": detail, "ms": round((time.perf_counter() - t0) * 1000, 1)})
            return ok

        def initialize() -> str:
            res = rpc("initialize", {"protocolVersion": protocol.LATEST_VERSION, "capabilities": {}, "clientInfo": {"name": "olcf-health", "version": "1"}})
            return f"protocol {res['protocolVersion']}, session created"

        def tools_list() -> str:
            return f"{len(rpc('tools/list', {})['tools'])} tools listed"

        def tools_call() -> str:
            res = rpc("tools/call", {"name": "list_workspaces", "arguments": {}})
            if res.get("isError"):
                raise RuntimeError(res["content"][0]["text"][:200])
            return "list_workspaces executed with authorisation"

        def resources_list() -> str:
            return f"{len(rpc('resources/list', {})['resources'])} resources on the first page"

        ok = step("initialize", initialize)
        if ok:
            ok = step("tools/list", tools_list) and ok
            ok = step("tools/call list_workspaces", tools_call) and ok
            ok = step("resources/list", resources_list) and ok
            with self.ctx.db.begin() as conn:
                conn.execute(delete(mcp_sessions).where(mcp_sessions.c.id == state["session"]["id"]))
            steps.append({"name": "session cleanup", "ok": True, "detail": "probe session removed", "ms": 0.0})
        return ok, steps
