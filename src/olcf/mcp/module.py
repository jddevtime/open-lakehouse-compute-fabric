# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""MCP module: settings, the ``mcp`` service, the ``/mcp`` endpoint (public router), ``/v1/mcp/*`` routes, health check and housekeeping."""

from __future__ import annotations

import logging
import threading
from typing import Any

from olcf.core.context import AppContext, Module
from olcf.core.errors import BadRequest
from olcf.core.health import ComponentHealth, HealthCheck, Status

log = logging.getLogger("olcf.mcp")


def _bounded(lo: int, hi: int):  # noqa: ANN202
    def check(v: Any) -> int:
        if not isinstance(v, int) or isinstance(v, bool) or not lo <= v <= hi:
            raise BadRequest(f"must be an integer between {lo} and {hi}")
        return v

    return check


def _bool(v: Any) -> bool:
    if not isinstance(v, bool):
        raise BadRequest("must be true or false")
    return v


def _str_list(v: Any) -> list[str]:
    if not isinstance(v, list) or not all(isinstance(x, str) and 0 < len(x) <= 200 for x in v):
        raise BadRequest("must be a list of strings")
    return v


INT_SETTINGS: list[tuple[str, str, int, tuple[int, int]]] = [
    ("mcp.session_idle_minutes", "Idle time after which an MCP session expires", 60, (1, 10_080)),
    ("mcp.rate_limit_per_minute", "MCP requests per principal per minute", 120, (1, 100_000)),
    ("mcp.max_result_bytes", "Largest tool result returned to an agent (rows are cut to fit)", 262_144, (4096, 8_388_608)),
    ("mcp.max_request_bytes", "Largest MCP request body", 1_048_576, (4096, 33_554_432)),
    ("mcp.max_rows", "Most rows run_sql may return", 5000, (1, 100_000)),
    ("mcp.sql_timeout_seconds", "Default run_sql timeout", 30, (1, 600)),
    ("mcp.max_sample_rows", "Most rows sample_rows may return", 100, (1, 1000)),
    ("mcp.max_list_items", "Most items list/search tools return", 500, (1, 5000)),
    ("mcp.max_report_rows", "Most rows per tile run_report returns", 500, (1, 10_000)),
    ("mcp.calls_retention_days", "Days of MCP call audit rows kept", 90, (1, 3650)),
    ("mcp.progress_interval_seconds", "Progress notification interval for streamed tool calls", 5, (1, 60)),
]


class McpModule(Module):
    """Module that registers the Model Context Protocol endpoint and its service."""
    name = "mcp"

    def __init__(self, ctx: AppContext) -> None:
        super().__init__()
        from olcf.mcp import http

        self.ctx = ctx
        self.public_routers = [(http.public, "")]
        self.routers = [(http.api, "/mcp")]
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._housekeeping, name="olcf-mcp-housekeeping", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _housekeeping(self) -> None:
        while not self._stop.wait(3600):
            try:
                if self.ctx.leader.is_leader():
                    res = self.ctx.service("mcp").purge()
                    if res["sessions"] or res["calls"]:
                        log.info("MCP housekeeping purged %s", res)
            except Exception:  # noqa: BLE001
                log.exception("MCP housekeeping failed")


def install(ctx: AppContext) -> McpModule:
    from olcf.mcp.service import McpService

    settings = ctx.service("settings")
    settings.register("mcp.enabled", description="Serve the MCP (AI agent) endpoint at /mcp", default=True, validator=_bool)
    settings.register("mcp.allowed_hosts", description="Extra Host header values accepted when the server is bound to loopback only (DNS rebinding protection)", default=[], validator=_str_list)
    settings.register("mcp.allowed_origins", description="Browser origins allowed to call /mcp (in addition to server.cors_origins and server.public_url)", default=[], validator=_str_list)
    for key, desc, default, (lo, hi) in INT_SETTINGS:
        settings.register(key, description=desc, default=default, validator=_bounded(lo, hi))
    svc = McpService(ctx)
    ctx.register_service("mcp", svc)

    def quick() -> ComponentHealth:
        if not svc.enabled():
            return ComponentHealth("mcp", Status.UNKNOWN, "disabled (setting mcp.enabled=false)", category="data")
        return ComponentHealth("mcp", Status.OK, f"serving {len(svc.tools)} tools; {svc.active_sessions()} active session(s)", category="data",
                               labels={"tools": len(svc.tools), "sessions": svc.active_sessions(), "calls": svc.stats()})

    def deep() -> ComponentHealth:
        if not svc.enabled():
            return ComponentHealth("mcp", Status.UNKNOWN, "disabled (setting mcp.enabled=false)", category="data", mode="deep")
        ok, steps = svc.deep_check()
        failed = [s["name"] for s in steps if not s["ok"]]
        return ComponentHealth("mcp", Status.OK if ok else Status.DOWN, "initialize, tools/list, tools/call and resources/list passed" if ok else f"failed: {', '.join(failed)}",
                               category="data", steps=steps, mode="deep")

    ctx.health.register(HealthCheck(name="mcp", category="data", description="MCP endpoint (deep: runs the real protocol path in-process)", quick=quick, deep=deep, ttl=30.0))
    return McpModule(ctx)
