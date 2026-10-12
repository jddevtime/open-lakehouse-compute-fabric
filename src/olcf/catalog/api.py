# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""REST API of the data catalog (mounted under ``/v1``): catalogs, tables, search, statistics and lineage."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query, Response
from pydantic import BaseModel, ConfigDict, Field

from olcf.api.deps import Authed, Ctx, IfMatch, Pagination, etag_header
from olcf.core.errors import BadRequest

router = APIRouter(tags=["catalog"])


def _svc(ctx: Any):  # noqa: ANN202
    return ctx.service("catalog")


class UpdateEntry(BaseModel):
    """Request body that edits the human-written parts of a catalog entry: description, tags and per-column descriptions."""
    model_config = ConfigDict(extra="forbid")
    description: str | None = None
    tags: list[str] | None = None
    column_descriptions: dict[str, str | None] | None = None


class LineageBody(BaseModel):
    """Request body that records lineage edges explicitly: source and target datasets, how the data moved, and optional column mappings."""
    model_config = ConfigDict(extra="forbid")
    workspace_id: str
    sources: list[dict[str, Any]] = Field(min_length=1)
    targets: list[dict[str, Any]] = Field(min_length=1)
    via: dict[str, Any] | None = None
    columns: list[dict[str, Any]] | None = None
    column_status: str | None = None


class ParseBody(BaseModel):
    """Request body of the SQL lineage parser: a statement, its dialect and default lakehouse, and whether to record the edges it finds."""
    model_config = ConfigDict(extra="forbid")
    workspace_id: str
    sql: str = Field(min_length=1, max_length=200_000)
    dialect: str = "duckdb"
    default_lakehouse: str | None = None
    record: bool = False


@router.get("/catalogs")
def list_catalogs(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Catalogs (a label on lakehouses, default ``default``) the caller can see, with their lakehouses and table counts."""
    return {"items": _svc(ctx).list_catalogs(principal)}


@router.get("/catalog/tables")
def list_tables(principal: Authed, ctx: Ctx, page: Pagination, workspace_id: str | None = None, lakehouse_id: str | None = None,
                catalog: str | None = None, format: str | None = Query(None, pattern="^(delta|iceberg|parquet)$"), tag: str | None = None,
                q: str | None = Query(None, max_length=200), include_missing: bool = False, include: str | None = Query(None, pattern="^columns$")) -> dict[str, Any]:
    items, nxt = _svc(ctx).list_tables(lakehouse_id, principal=principal, workspace_id=workspace_id, catalog=catalog, fmt=format, tag=tag, q=q,
                                       include_missing=include_missing, limit=page.limit, cursor=page.cursor, columns=include == "columns")
    return {"items": items, "next_cursor": nxt}


@router.get("/catalog/tables/{entry_id}")
def get_table(entry_id: str, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    out = _svc(ctx).get(principal, entry_id)
    response.headers["ETag"] = etag_header(out["etag"])
    return out


@router.patch("/catalog/tables/{entry_id}")
def update_table(entry_id: str, body: UpdateEntry, principal: Authed, ctx: Ctx, response: Response, if_match: IfMatch = None) -> dict[str, Any]:
    """Describe / tag a table and its columns.  Send ``If-Match`` with the ETag from the last read to avoid lost updates."""
    out = _svc(ctx).update_entry(principal, entry_id, description=body.description, tags=body.tags,
                                 column_descriptions={k: (v or "") for k, v in body.column_descriptions.items()} if body.column_descriptions is not None else None,
                                 if_match=if_match)
    response.headers["ETag"] = etag_header(out["etag"])
    return out


@router.get("/catalog/tables/{entry_id}/stats")
def table_stats(entry_id: str, principal: Authed, ctx: Ctx, compute: bool = False) -> dict[str, Any]:
    return _svc(ctx).stats(principal, entry_id, compute=compute)


@router.post("/catalog/tables/{entry_id}/stats/refresh")
def refresh_stats(entry_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Recompute row count, size and per-column min/max/null counts from the Delta log or Parquet footers (no data scan)."""
    return _svc(ctx).compute_stats(principal, entry_id)


@router.post("/catalog/sync")
def sync_workspace(principal: Authed, ctx: Ctx, workspace_id: str = Query(...)) -> dict[str, Any]:
    """Re-discover every lakehouse of a workspace."""
    principal.require("lakehouse.maintain", workspace_id=workspace_id)
    svc = _svc(ctx)
    return {"lakehouses": [svc.sync_lakehouse(lh, principal=principal) for lh in ctx.service("storage").list_lakehouses(workspace_id)]}


@router.get("/catalog/search")
def search(principal: Authed, ctx: Ctx, q: str = Query(..., min_length=1, max_length=200), kind: list[str] | None = Query(None),
           workspace_id: str | None = None, limit: int = Query(25, ge=1, le=100)) -> dict[str, Any]:
    """Ranked search over tables, columns and saved queries / reports / notebooks."""
    return {"items": _svc(ctx).search(principal, q, kinds=kind, workspace_id=workspace_id, limit=limit), "query": q}


@router.get("/catalog/lineage")
def get_lineage(principal: Authed, ctx: Ctx, dataset: str = Query(..., min_length=1), direction: str = Query("both", pattern="^(up|down|both)$"),
                depth: int = Query(3, ge=1, le=10), column: str | None = None) -> dict[str, Any]:
    """Lineage graph around a dataset (``olcf://ws/lakehouse/Tables/t``, a ``cat_`` entry id, or an item id).  Pass ``column`` to trace one column upstream."""
    svc = _svc(ctx)
    return svc.lineage(principal, svc.resolve_dataset(principal, dataset), direction=direction, depth=depth, column=column)


@router.post("/catalog/lineage", status_code=201)
def record_lineage(body: LineageBody, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Record lineage reported by a notebook, pipeline or external tool (needs ``lakehouse.write`` in the workspace)."""
    ws = ctx.service("workspaces").resolve(body.workspace_id)
    edges = _svc(ctx).record_lineage(principal, workspace_id=ws["id"], sources=body.sources, targets=body.targets, via=body.via, columns=body.columns,
                                     column_status=body.column_status)
    ctx.audit.record(principal, "catalog.lineage_recorded", resource_type="workspace", resource_id=ws["id"], workspace_id=ws["id"], details={"edges": len(edges)})
    return {"edges": edges}


@router.post("/catalog/lineage/parse")
def parse_lineage(body: ParseBody, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Derive lineage from SQL text (table level always, column level only where provable); ``record=true`` stores the edges."""
    ws = ctx.service("workspaces").resolve(body.workspace_id)
    svc = _svc(ctx)
    default_id = None
    if body.default_lakehouse:
        default_id = svc.default_lakehouse(principal, ws["id"], body.default_lakehouse).id
    out = svc.analyze_sql(principal, ws["id"], body.sql, dialect=body.dialect, default_lakehouse_id=default_id)
    if body.record:
        if not any(s["target_datasets"] for s in out["statements"]):
            raise BadRequest("none of the statements writes a table, so there is no lineage to record")
        out["edges"] = svc.record_lineage_from_sql(principal, ws["id"], body.sql, dialect=body.dialect, default_lakehouse_id=default_id, via={"kind": "api", "name": "lineage/parse"})
    return out
