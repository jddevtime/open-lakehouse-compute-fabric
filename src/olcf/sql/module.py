# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""SQL module: settings, the ``sql`` and ``reports`` services, item hooks, health checks and the history purge thread."""

from __future__ import annotations

import logging
import threading
from typing import Any

from olcf.core.context import AppContext, Module
from olcf.core.errors import BadRequest
from olcf.core.health import ComponentHealth, HealthCheck, Status

log = logging.getLogger("olcf.sql")


def _bounded(lo: int, hi: int):  # noqa: ANN202
    def check(v: Any) -> int:
        if not isinstance(v, int) or isinstance(v, bool) or not lo <= v <= hi:
            raise BadRequest(f"must be an integer between {lo} and {hi}")
        return v

    return check


SETTINGS: list[tuple[str, str, int, tuple[int, int]]] = [
    ("sql.default_max_rows", "Rows returned by default for an interactive statement", 10_000, (1, 1_000_000)),
    ("sql.max_rows_cap", "Largest max_rows a caller may request (use exports for more)", 100_000, (1, 1_000_000)),
    ("sql.default_timeout_seconds", "Default statement timeout", 60, (1, 3600)),
    ("sql.max_timeout_seconds", "Largest statement timeout a caller may request", 600, (1, 86_400)),
    ("sql.duckdb_memory_mb", "Memory limit of one embedded DuckDB query", 1024, (64, 262_144)),
    ("sql.duckdb_threads", "Threads used by one embedded DuckDB query", 2, (1, 64)),
    ("sql.max_concurrent_queries", "Embedded statements running at once (others wait, then get 429)", 4, (1, 256)),
    ("sql.max_file_scan_mb", "Most data a read_csv/read_parquet/read_json call may load", 256, (1, 65_536)),
    ("sql.export_max_rows", "Most rows an export streams", 5_000_000, (1, 1_000_000_000)),
    ("sql.history_retention_days", "Days of query history kept", 30, (1, 3650)),
    ("sql.history_max_rows", "Most query history rows kept", 100_000, (1000, 100_000_000)),
    ("sql.report_cache_ttl_seconds", "How long report tile results are cached", 60, (0, 86_400)),
    ("sql.report_parallelism", "Report tiles run at once", 4, (1, 16)),
]


class SqlModule(Module):
    """Module that registers the SQL, saved query and report routes and services."""
    name = "sql"

    def __init__(self, ctx: AppContext) -> None:
        super().__init__()
        from olcf.sql import api

        self.ctx = ctx
        self.routers = [(api.router, "")]
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        sql = self.ctx.service("sql")
        try:
            if self.ctx.leader.is_leader() or self.ctx.leader.acquire():
                n = sql.history.fail_orphans()
                if n:
                    log.info("marked %d interrupted statement(s) from before the restart", n)
        except Exception:  # noqa: BLE001
            log.exception("could not clean orphaned query history rows")
        self._stop.clear()
        self._thread = threading.Thread(target=self._housekeeping, name="olcf-sql-housekeeping", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _housekeeping(self) -> None:
        """Leader only: purge expired history hourly."""
        while not self._stop.wait(3600):
            try:
                if self.ctx.leader.is_leader():
                    res = self.ctx.service("sql").history.purge()
                    if res["expired"] or res["trimmed"]:
                        log.info("query history purged: %s", res)
            except Exception:  # noqa: BLE001
                log.exception("query history purge failed")


def _health(ctx: AppContext, engine: str) -> None:
    sql = ctx.service("sql")
    name = f"sql:{engine}"

    def quick() -> ComponentHealth:
        eng = sql.duckdb if engine == "duckdb" else sql.spark
        ok, detail = eng.available()
        if engine == "spark" and not ok:
            return ComponentHealth(name, Status.UNKNOWN, f"Spark SQL engine not available: {detail}", category="data")
        return ComponentHealth(name, Status.OK if ok else Status.DOWN, detail if ok else f"engine unavailable: {detail}", category="data")

    def deep() -> ComponentHealth:
        ok, steps = sql.deep_check(engine)
        failed = [s["name"] for s in steps if not s["ok"]]
        if engine == "spark" and failed == ["compute service"]:
            return ComponentHealth(name, Status.UNKNOWN, "Spark SQL engine not available: compute service not running", category="data", steps=steps, mode="deep")
        return ComponentHealth(name, Status.OK if ok else Status.DOWN, "all probe steps passed" if ok else f"failed: {', '.join(failed)}", category="data", steps=steps, mode="deep")

    ctx.health.register(HealthCheck(name=name, category="data", description=f"SQL engine '{engine}' (deep: SELECT 1 and a real table read)", quick=quick, deep=deep, ttl=30.0))


def install(ctx: AppContext) -> SqlModule:
    from olcf.sql import reports
    from olcf.sql.service import SqlService

    settings = ctx.service("settings")
    for key, desc, default, (lo, hi) in SETTINGS:
        settings.register(key, description=desc, default=default, validator=_bounded(lo, hi))
    sql = SqlService(ctx)
    ctx.register_service("sql", sql)
    ctx.register_service("reports", reports.ReportService(ctx, sql))
    ctx.register_item_hooks("sql_query", validate=reports.validate_sql_query, remap_ids=reports.remap_sql_query_ids)
    ctx.register_item_hooks("report", validate=reports.validate_report, remap_ids=reports.remap_report_ids)
    for engine in ("duckdb", "spark"):
        _health(ctx, engine)
    return SqlModule(ctx)
