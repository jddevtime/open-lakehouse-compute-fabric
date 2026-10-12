# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""REST API of the SQL service and reports (mounted under ``/v1``).

Results are typed and capped (``max_rows``, ``truncated``); anything bigger goes through the streaming ``/sql/export``.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field
from starlette.concurrency import iterate_in_threadpool

from olcf.api.deps import Authed, Ctx, Pagination
from olcf.core.errors import BadRequest
from olcf.sql.model import SqlRequest

router = APIRouter(tags=["sql"])
MAX_SQL = 1_000_000


class ExecuteBody(BaseModel):
    """Request body for executing SQL: the statement, the engine (DuckDB or Spark), the workspace and limits."""
    model_config = ConfigDict(extra="forbid")
    sql: str = Field(min_length=1, max_length=MAX_SQL)
    engine: str = Field("duckdb", pattern="^(duckdb|spark)$")
    workspace_id: str | None = None
    lakehouse: str | None = Field(None, description="Default lakehouse (id or name) for unqualified table names")
    parameters: dict[str, Any] = Field(default_factory=dict)
    max_rows: int | None = Field(None, ge=1)
    timeout_seconds: int | None = Field(None, ge=1)
    query_id: str | None = Field(None, pattern="^[A-Za-z0-9_-]{8,64}$", description="Client-chosen id so the statement can be cancelled while it runs")

    def request(self, source: str = "api") -> SqlRequest:
        return SqlRequest(sql=self.sql, engine=self.engine, workspace_id=self.workspace_id, lakehouse=self.lakehouse, parameters=self.parameters,
                          max_rows=self.max_rows, timeout_seconds=self.timeout_seconds, query_id=self.query_id, source=source)


class RunSavedBody(BaseModel):
    """Request body for running a saved query with parameters and optional row and time limits."""
    model_config = ConfigDict(extra="forbid")
    parameters: dict[str, Any] = Field(default_factory=dict)
    max_rows: int | None = Field(None, ge=1)
    timeout_seconds: int | None = Field(None, ge=1)
    query_id: str | None = Field(None, pattern="^[A-Za-z0-9_-]{8,64}$")


class ExportBody(ExecuteBody):
    """Request body for exporting a query result as CSV, JSON, NDJSON or Parquet."""
    format: str = Field("csv", pattern="^(csv|json|ndjson|parquet)$")
    filename: str | None = Field(None, max_length=120)


class ReportRunBody(BaseModel):
    """Request body for running a report with filters, optionally bypassing cached results."""
    model_config = ConfigDict(extra="forbid")
    filters: dict[str, Any] = Field(default_factory=dict)
    refresh: bool = False


def _sql(ctx: Any):  # noqa: ANN202
    return ctx.service("sql")


@router.post("/sql/execute")
def execute(body: ExecuteBody, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Run one statement and return typed columns and at most ``max_rows`` rows (``truncated`` tells if there were more)."""
    return _sql(ctx).execute(principal, body.request()).to_dict()


@router.post("/sql/explain")
def explain(body: ExecuteBody, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    return _sql(ctx).explain(principal, body.request())


@router.post("/sql/queries/{item_id}/run")
def run_saved(item_id: str, body: RunSavedBody, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Run a saved ``sql_query`` item with named parameters."""
    return _sql(ctx).run_saved(principal, item_id, body.parameters, max_rows=body.max_rows, timeout_seconds=body.timeout_seconds, query_id=body.query_id).to_dict()


@router.post("/sql/cancel/{query_id}")
def cancel(query_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    return _sql(ctx).cancel(principal, query_id)


@router.get("/sql/running")
def running(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    return {"items": _sql(ctx).running(principal)}


@router.get("/sql/history")
def history(principal: Authed, ctx: Ctx, page: Pagination, workspace_id: str | None = None, mine: bool = True,
            status: str | None = Query(None, pattern="^(ok|error|cancelled|timeout|denied|running)$"), source: str | None = Query(None, max_length=24)) -> dict[str, Any]:
    """Query history: your own statements, or (``mine=false``) everyone's where you administer the workspace / hold ``audit.read``."""
    items, nxt = _sql(ctx).history.list(principal, workspace_id=workspace_id, mine=mine, status=status, source=source, limit=page.limit, cursor=page.cursor)
    return {"items": items, "next_cursor": nxt}


@router.get("/sql/history/{query_id}")
def history_item(query_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    return _sql(ctx).history.get(principal, query_id)


@router.get("/sql/schema")
def schema(principal: Authed, ctx: Ctx, workspace_id: str | None = None, lakehouse: str | None = None) -> dict[str, Any]:
    """Lakehouses, tables and columns the caller may query, with the exact reference to type in SQL."""
    return _sql(ctx).schema(principal, workspace_id, lakehouse)


@router.get("/sql/engines")
def engines(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    return {"items": _sql(ctx).engines(), "limits": _sql(ctx).limits()}


@router.post("/sql/export")
def export(body: ExportBody, principal: Authed, ctx: Ctx) -> StreamingResponse:
    """Stream the full result as CSV / JSON / NDJSON / Parquet (never buffered whole; capped by ``sql.export_max_rows``)."""
    if body.max_rows is not None:
        raise BadRequest("max_rows does not apply to exports; the server limit sql.export_max_rows is used")
    ctype, name, chunks = _sql(ctx).open_export(principal, body.request("export"), body.format, body.filename)
    return StreamingResponse(iterate_in_threadpool(chunks), media_type=ctype, headers={"Content-Disposition": f'attachment; filename="{name}"', "Cache-Control": "no-store"})


@router.post("/reports/{report_id}/run")
def run_report(report_id: str, body: ReportRunBody, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Run every tile of a report with the given filter values; a failing tile reports its own error."""
    return ctx.service("reports").run(principal, report_id, body.filters, refresh=body.refresh)


@router.get("/reports")
def list_reports(principal: Authed, ctx: Ctx, workspace_id: str | None = None) -> dict[str, Any]:
    return {"items": ctx.service("reports").list_reports(principal, workspace_id)}
