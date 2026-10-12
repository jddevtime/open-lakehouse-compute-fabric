# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""REST API of the Intelligence core (mounted under ``/v1``): assets, lineage, origin, impact, staleness, documentation, history, sampling, resolve, edges,
snapshots and the overview. Discovery (sources, scans, coverage ...) and governance (``/governance/*``) add their own routes from their packages.

Visibility: an asset outside the caller's workspaces answers ``404`` exactly like one that does not exist. Reads need ``intelligence.read``, ``PATCH`` needs
``governance.curate``, ``/sample`` needs ``intelligence.sample`` in the asset's workspace and ``POST /inbox/requeue`` needs ``intelligence.manage``.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import Any

from fastapi import APIRouter, Depends, Query, Response
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.exc import DBAPIError

from olcf.api.deps import Authed, Ctx, IfMatch, Pagination, etag_header
from olcf.core.errors import Unavailable
from olcf.intelligence import model as M

log = logging.getLogger("olcf.intelligence")


def _storage_guard() -> Iterator[None]:
    """Turn a failing Intelligence database (missing table, locked or unreachable store) into ``503 unavailable`` instead of an unhandled ``500``."""
    try:
        yield
    except DBAPIError as exc:
        log.warning("intelligence storage error: %s", str(exc.orig or exc)[:300])
        raise Unavailable("the Intelligence graph store is unavailable", remediation="check the 'intelligence' health check and the database; other platform features are unaffected") from exc


router = APIRouter(tags=["intelligence"], dependencies=[Depends(_storage_guard)])

_CONF = "^(verified|derived|inferred|unknown)$"


def _svc(ctx: Any):  # noqa: ANN202
    return ctx.service("intelligence")


def _types(values: list[str] | None) -> list[str] | None:
    out = [t.strip() for v in values or [] for t in v.split(",") if t.strip()]
    return out or None


class UpdateAsset(BaseModel):
    """Request body that edits the human-written parts of an asset: its description (an empty text hands the field back to the scans) and its tags."""
    model_config = ConfigDict(extra="forbid")
    description: str | None = Field(None, max_length=8000)
    tags: list[str] | None = None


class ResolveBody(BaseModel):
    """Request body of ``POST /intelligence/resolve``: the asset type, any known aliases (``{kind: value}``), an optional name and the scope that qualifies a partial name."""
    model_config = ConfigDict(extra="forbid")
    type: str = Field(..., min_length=1, max_length=32)
    aliases: dict[str, str] = Field(default_factory=dict)
    name: str | None = Field(None, max_length=512)
    scope: dict[str, str] = Field(default_factory=dict)
    create: bool = False


@router.get("/intelligence/summary")
def summary(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Counts of assets, edges by confidence, open gaps and the latest scan per source, limited to what the caller may see."""
    return _svc(ctx).summary(principal)


@router.get("/intelligence/assets")
def list_assets(principal: Authed, ctx: Ctx, page: Pagination, q: str | None = Query(None, max_length=200), type: str | None = Query(None, max_length=200),
                workspace_id: str | None = None, domain: str | None = None, classification: str | None = None, certification: str | None = None, owner: str | None = None,
                tag: str | None = Query(None, max_length=64), status: str | None = Query(None, pattern="^(active|removed|placeholder|any)$")) -> dict[str, Any]:
    """Assets the caller can see (workspaces, lakehouses, tables, columns, notebooks, pipelines, models, reports ...), oldest first. ``q`` matches name, path, description and aliases.

    With governance installed every item has a compact ``governance`` object. ``truncated_filter: true`` appears when a governance filter matched more assets than the query can carry.
    """
    meta: dict[str, Any] = {}
    items, nxt = _svc(ctx).list_assets(principal, q=q, type=type, workspace_id=workspace_id, domain=domain, classification=classification, certification=certification,
                                       owner=owner, tag=tag, status=status, limit=page.limit, cursor=page.cursor, meta=meta)
    out: dict[str, Any] = {"items": items, "next_cursor": nxt}
    if meta.get("truncated_filter"):
        out["truncated_filter"] = True
    return out


@router.get("/intelligence/assets/{asset_id}")
def get_asset(asset_id: str, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    """One asset with aliases, parent, child count, lineage edge counts and open gaps. The ``ETag`` header is the value to send back as ``If-Match``."""
    out = _svc(ctx).get_asset(principal, asset_id)
    response.headers["ETag"] = etag_header(out["etag"])
    return out


@router.patch("/intelligence/assets/{asset_id}")
def update_asset(asset_id: str, body: UpdateAsset, principal: Authed, ctx: Ctx, response: Response, if_match: IfMatch = None) -> dict[str, Any]:
    """Describe / tag an asset (needs ``governance.curate``). Send ``If-Match`` with the ETag of the last read to avoid lost updates."""
    out = _svc(ctx).update_asset(principal, asset_id, description=body.description, tags=body.tags, if_match=if_match)
    response.headers["ETag"] = etag_header(out["etag"])
    return out


@router.get("/intelligence/assets/{asset_id}/children")
def children(asset_id: str, principal: Authed, ctx: Ctx, page: Pagination, type: str | None = Query(None, max_length=200),
             status: str | None = Query(None, pattern="^(active|removed|placeholder|any)$")) -> dict[str, Any]:
    """Child assets (tables of a lakehouse, columns of a table ...), by name."""
    items, nxt = _svc(ctx).children(principal, asset_id, type=type, status=status, limit=page.limit, cursor=page.cursor)
    return {"items": items, "next_cursor": nxt}


@router.get("/intelligence/assets/{asset_id}/lineage")
def lineage(asset_id: str, principal: Authed, ctx: Ctx, direction: str = Query("both", pattern="^(up|down|both)$"), depth: int = Query(3, ge=1, le=10),
            level: str = Query("table", pattern="^(table|column)$"), min_confidence: str | None = Query(None, pattern=_CONF), types: list[str] | None = Query(None),
            column: str | None = Query(None, max_length=256), include_removed: bool = False) -> dict[str, Any]:
    """Lineage around an asset. Table level runs through the processes (notebooks, pipelines, queries) between datasets; column level follows ``TRANSFORMS`` edges
    (``via`` names the process). ``depth`` counts dataset hops (1..10); the answer is cut at the node cap and says ``truncated``. ``types`` keeps only those node
    types and bridges the rest; ``min_confidence`` keeps edges at least that strong; ``column`` selects one column (implies column level)."""
    return _svc(ctx).lineage(principal, asset_id, direction=direction, depth=depth, level=level, min_confidence=min_confidence, types=_types(types), column=column,
                             include_removed=include_removed)


@router.get("/intelligence/assets/{asset_id}/origin")
def origin(asset_id: str, principal: Authed, ctx: Ctx, depth: int = Query(10, ge=1, le=10), min_confidence: str | None = Query(None, pattern=_CONF),
           level: str = Query("table", pattern="^(table|column)$"), column: str | None = Query(None, max_length=256)) -> dict[str, Any]:
    """Where does this come from: the upstream roots, every path from them (source first) and the confidence of each path (its weakest edge)."""
    return _svc(ctx).origin(principal, asset_id, depth=depth, min_confidence=min_confidence, level="column" if column else level, column=column)


@router.get("/intelligence/assets/{asset_id}/impact")
def impact(asset_id: str, principal: Authed, ctx: Ctx, depth: int = Query(5, ge=1, le=10), min_confidence: str | None = Query(None, pattern=_CONF)) -> dict[str, Any]:
    """What breaks if this changes: downstream consumers grouped by type (tables, models, measures, reports, visuals), with owners when governance data exists."""
    return _svc(ctx).impact(principal, asset_id, depth=depth, min_confidence=min_confidence)


@router.get("/intelligence/assets/{asset_id}/staleness")
def staleness(asset_id: str, principal: Authed, ctx: Ctx, depth: int = Query(5, ge=1, le=10), stale_after_hours: float | None = Query(None, gt=0, le=87600)) -> dict[str, Any]:
    """Is the data fresh? Last observed write, the outcome of the run that wrote it, and the most upstream stale producer (the root cause)."""
    return _svc(ctx).staleness(principal, asset_id, depth=depth, stale_after_hours=stale_after_hours)


@router.get("/intelligence/assets/{asset_id}/documentation", response_model=None)
def documentation(asset_id: str, principal: Authed, ctx: Ctx, format: str = Query("json", pattern="^(json|markdown)$")) -> Any:
    """Generated documentation (Markdown): description, schema, governance facts, lineage summary, freshness and schema changes. ``format=markdown`` returns the text itself."""
    out = _svc(ctx).documentation(principal, asset_id)
    if format == "markdown":
        return PlainTextResponse(out["markdown"], media_type="text/markdown; charset=utf-8")
    return out


@router.get("/intelligence/assets/{asset_id}/history")
def history(asset_id: str, principal: Authed, ctx: Ctx, page: Pagination, schema_only: bool = False) -> dict[str, Any]:
    """Attribute versions of an asset, newest first, with what changed (columns added / removed / retyped). ``schema_only`` keeps the schema changes."""
    return _svc(ctx).history(principal, asset_id, limit=page.limit, cursor=page.cursor, schema_only=schema_only)


@router.get("/intelligence/assets/{asset_id}/sample")
def sample(asset_id: str, principal: Authed, ctx: Ctx, rows: int | None = Query(None, ge=1, le=100), columns: list[str] | None = Query(None)) -> dict[str, Any]:
    """Sample rows of a table, fetched on demand under the sampling policy of its workspace (masked, limited, audited; values are never stored). Needs ``intelligence.sample``."""
    return _svc(ctx).sample(principal, asset_id, rows=rows, columns=columns)


@router.post("/intelligence/resolve")
def resolve(body: ResolveBody, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Which asset does this reference mean? Resolves aliases, then the name qualified by the scope (``workspace_id``, ``lakehouse_id``, ``lakehouse``, ``table`` ...).
    With ``create`` a placeholder is made when nothing matches (needs ``intelligence.manage``)."""
    ref = M.AssetRef.make(body.type, body.aliases, body.name, body.scope)
    return _svc(ctx).resolve_for(principal, ref, create=body.create)


@router.post("/intelligence/inbox/requeue")
def requeue_inbox(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Queue every event the Intelligence inbox gave up on (status ``failed``) for another set of attempts (needs ``intelligence.manage``). Failed rows are also
    retried once automatically after ``intelligence.inbox_retry_after_minutes``. Returns ``{"requeued": n, "inbox": {pending, done, failed, skipped, ...}}``."""
    return _svc(ctx).requeue_inbox(principal)


@router.get("/intelligence/edges/{edge_id}")
def get_edge(edge_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """One lineage edge with all of its evidence (who saw it, where, when) and the history of its confidence."""
    return _svc(ctx).edge_detail(principal, edge_id)


@router.get("/intelligence/snapshots")
def snapshots(principal: Authed, ctx: Ctx, page: Pagination, source_id: str | None = None, status: str | None = Query(None, pattern="^(running|succeeded|partial|failed|cancelled)$"),
              include_retired: bool = False) -> dict[str, Any]:
    """Metadata snapshots (scans) of the sources the caller can see, newest first; the ids feed ``/intelligence/snapshots/diff``. The closing scan of a deleted source is hidden
    unless ``include_retired=true``."""
    items, nxt = _svc(ctx).snapshots(principal, source_id=source_id, status=status, limit=page.limit, cursor=page.cursor, include_retired=include_retired)
    return {"items": items, "next_cursor": nxt}


@router.get("/intelligence/snapshots/diff")
def snapshot_diff(principal: Authed, ctx: Ctx, from_: str = Query(..., alias="from"), to: str = Query(...), limit: int = Query(500, ge=1, le=2000)) -> dict[str, Any]:
    """Assets and edges added, removed and changed between two scans of one source (columns added / removed / retyped are listed per table)."""
    return _svc(ctx).diff(principal, from_, to, limit=limit)


@router.get("/intelligence/unused")
def unused(principal: Authed, ctx: Ctx, page: Pagination, days: int | None = Query(None, ge=0, le=3650), type: str | None = Query(None, max_length=200),
           workspace_id: str | None = None) -> dict[str, Any]:
    """Tables and views nothing was seen reading within ``days`` days (no downstream consumer observed by a scan or at run time)."""
    return _svc(ctx).unused(principal, days=days, types=_types([type]) if type else None, workspace_id=workspace_id, limit=page.limit, cursor=page.cursor)
