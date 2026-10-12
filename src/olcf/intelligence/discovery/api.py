# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""REST API of Intelligence discovery (mounted under ``/v1``).

====================================================  ===============================================================================================
Route                                                 Purpose (permission)
====================================================  ===============================================================================================
``GET    /intelligence/sources``                      list the sources you can see (``intelligence.read``)
``POST   /intelligence/sources``                      create a source; credentials are write-only (``intelligence.manage``)
``POST   /intelligence/sources/test``                 test a source configuration before saving it (``intelligence.manage``)
``GET    /intelligence/sources/{id}``                 one source (``config`` only for managers)
``PATCH  /intelligence/sources/{id}``                 change it; ``If-Match`` supported (``intelligence.manage``)
``DELETE /intelligence/sources/{id}``                 delete it and mark its assets removed (``intelligence.manage``)
``POST   /intelligence/sources/{id}/test``            ordered connectivity steps with remediation (``intelligence.manage``)
``POST   /intelligence/sources/{id}/scan``            start a full or incremental scan, ``202`` (``intelligence.manage``)
``GET    /intelligence/scans``                        scans of visible sources, newest first
``GET    /intelligence/scans/{id}``                   one scan with its gap counts
``POST   /intelligence/scans/{id}/cancel``            stop a running scan (``intelligence.manage``)
``GET    /intelligence/gaps``                         what discovery could not read (open by default)
``GET    /intelligence/coverage``                     assets and gaps per source, schedule, sampling state
``POST   /intelligence/import/scanner``               import a saved Power BI scanner result (``intelligence.manage``)
``GET    /intelligence/assets/{id}/suggestions``      suggested sensitive-data classifications from column names / types
``GET    /intelligence/assets/{id}/sampling-policy``  the sampling policy that applies to an asset
``GET    /intelligence/sampling-policies``            list sampling policies you manage
``PUT    /intelligence/sampling-policies``            create or replace the policy of a scope (``intelligence.manage``)
``DELETE /intelligence/sampling-policies/{id}``       remove a policy
====================================================  ===============================================================================================

Row sampling itself is ``GET /intelligence/assets/{id}/sample`` (Intelligence core); it answers ``403 policy_denied`` with the reason until a policy allows it.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query, Request, Response
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, ConfigDict, Field

from olcf.api.deps import Authed, Ctx, IfMatch, Pagination, etag_header
from olcf.core.errors import BadRequest
from olcf.intelligence.discovery import scanner_import
from olcf.intelligence.discovery.schema import MASKING_MODES

router = APIRouter(tags=["intelligence-discovery"])


def _d(ctx: Any):  # noqa: ANN202
    return ctx.service("intelligence").discovery


class SourceCreate(BaseModel):
    """Request body of ``POST /intelligence/sources``. ``credentials`` / ``sampling_credentials`` are write-only: ``{client_secret}`` or ``{private_key_pem, certificate_pem}``."""
    model_config = ConfigDict(extra="forbid")
    name: str = Field(..., min_length=1, max_length=128)
    kind: str = Field(..., min_length=1, max_length=24)
    config: dict[str, Any] = Field(default_factory=dict)
    schedule: dict[str, Any] | None = None
    enabled: bool = True
    credentials: dict[str, Any] | None = None
    sampling_credentials: dict[str, Any] | None = None


class SourceUpdate(BaseModel):
    """Request body of ``PATCH /intelligence/sources/{id}``: only the fields present change; ``config`` is merged (a ``null`` value removes a key)."""
    model_config = ConfigDict(extra="forbid")
    name: str | None = Field(None, min_length=1, max_length=128)
    enabled: bool | None = None
    config: dict[str, Any] | None = None
    schedule: dict[str, Any] | None = None
    credentials: dict[str, Any] | None = None
    sampling_credentials: dict[str, Any] | None = None


class SourceTest(BaseModel):
    """Request body of ``POST /intelligence/sources/test``: a configuration to test without saving it. Credentials are used in memory only."""
    model_config = ConfigDict(extra="forbid")
    name: str | None = Field(None, max_length=128)
    kind: str = Field(..., min_length=1, max_length=24)
    config: dict[str, Any] = Field(default_factory=dict)
    credentials: dict[str, Any] | None = None
    sampling_credentials: dict[str, Any] | None = None


class ScanRequest(BaseModel):
    """Request body of ``POST /intelligence/sources/{id}/scan``. ``workspace_ids`` limits the scan (a limited full scan never removes anything)."""
    model_config = ConfigDict(extra="forbid")
    mode: str = Field("incremental", pattern="^(full|incremental)$")
    workspace_ids: list[str] | None = Field(None, max_length=1000)


class PolicyPut(BaseModel):
    """Request body of ``PUT /intelligence/sampling-policies``. ``max_rows`` 0 turns sampling off for the scope; the narrowest scope (asset, source, workspace, global) wins."""
    model_config = ConfigDict(extra="forbid")
    scope_type: str = Field(..., pattern="^(global|workspace|source|asset)$")
    scope_id: str | None = Field(None, max_length=128)
    max_rows: int = Field(..., ge=0, le=100, strict=True)
    masking: str = Field("partial", pattern="^(" + "|".join(MASKING_MODES) + ")$")
    exclude_classified: bool = True
    enabled: bool = True


# ---------------------------------------------------------------------------------------------------------------- sources
@router.get("/intelligence/sources")
def list_sources(principal: Authed, ctx: Ctx, kind: str | None = Query(None, max_length=24)) -> dict[str, Any]:
    """Discovery sources the caller can see. Credentials are never returned; ``has_credentials`` says whether any are stored."""
    return {"items": _d(ctx).list_sources(principal, kind=kind)}


@router.post("/intelligence/sources", status_code=201)
def create_source(body: SourceCreate, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    """Create a source (kinds: local, catalog_mirror, fabric, scanner_import). Send Fabric credentials as write-only ``credentials`` (and optionally ``sampling_credentials`` for a
    separate sampling identity); they are stored in the secret store and never shown again."""
    out = _d(ctx).create_source(principal, body.model_dump(exclude_none=True))
    response.headers["ETag"] = etag_header(out["etag"])
    return out


@router.post("/intelligence/sources/test")
def test_unsaved(body: SourceTest, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Run the connectivity steps for a configuration that has not been saved. The result lists each step with ``ok``, ``detail``, ``remediation`` and ``duration_ms``; steps after the
    first failure are reported ``skipped``."""
    return _d(ctx).test_config(principal, body.model_dump(exclude_none=True))


@router.get("/intelligence/sources/{source_id}")
def get_source(source_id: str, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    """One source by id or name. The ``ETag`` header is the value for ``If-Match``."""
    out = _d(ctx).get_source(principal, source_id)
    response.headers["ETag"] = etag_header(out["etag"])
    return out


@router.patch("/intelligence/sources/{source_id}")
def update_source(source_id: str, body: SourceUpdate, principal: Authed, ctx: Ctx, response: Response, if_match: IfMatch = None) -> dict[str, Any]:
    """Change a source. Sending ``credentials`` replaces the stored secret; ``config`` is merged key by key."""
    out = _d(ctx).update_source(principal, source_id, {k: v for k, v in body.model_dump(exclude_unset=True).items() if v is not None}, if_match)
    response.headers["ETag"] = etag_header(out["etag"])
    return out


@router.delete("/intelligence/sources/{source_id}")
def delete_source(source_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Delete a source (not the built-in local source, not while it is scanning). Assets only it reported are marked ``removed``; history is kept."""
    return _d(ctx).delete_source(principal, source_id)


@router.post("/intelligence/sources/{source_id}/test")
def test_source(source_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Run the ordered connectivity steps of a saved source (a Fabric source has eight, ending in the optional sampling identity)."""
    return _d(ctx).test_source(principal, source_id)


@router.post("/intelligence/sources/{source_id}/scan", status_code=202)
def scan_source(source_id: str, principal: Authed, ctx: Ctx, body: ScanRequest | None = None, wait: bool = False) -> dict[str, Any]:
    """Start a scan. It runs in the background (``202`` with the scan id to poll at ``GET /intelligence/scans/{id}``); ``wait=true`` holds the request until it ends.
    ``409`` when a scan of the source is already running."""
    mode, ids = (body.mode, body.workspace_ids) if body is not None else ("incremental", None)
    return _d(ctx).scan(principal, source_id, mode=mode, workspace_ids=ids, wait=wait)


# ---------------------------------------------------------------------------------------------------------------- scans and gaps
@router.get("/intelligence/scans")
def list_scans(principal: Authed, ctx: Ctx, page: Pagination, source_id: str | None = None,
               status: str | None = Query(None, pattern="^(running|succeeded|partial|failed|cancelled)$")) -> dict[str, Any]:
    """Scans of the sources the caller can see, newest first."""
    items, nxt = _d(ctx).list_scans(principal, source_id=source_id, status=status, limit=page.limit, cursor=page.cursor)
    return {"items": items, "next_cursor": nxt}


@router.get("/intelligence/scans/{scan_id}")
def get_scan(scan_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """One scan: status, statistics (including the removal sweep of a succeeded full scan) and the number of gaps by kind."""
    return _d(ctx).get_scan(principal, scan_id)


@router.post("/intelligence/scans/{scan_id}/cancel", status_code=202)
def cancel_scan(scan_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Ask a running scan to stop; it ends ``cancelled`` after the batch in progress. ``409`` if it already ended."""
    return _d(ctx).cancel_scan(principal, scan_id)


@router.get("/intelligence/gaps")
def list_gaps(principal: Authed, ctx: Ctx, page: Pagination, source_id: str | None = None, scan_id: str | None = None, asset_id: str | None = None,
              kind: str | None = Query(None, max_length=32), resolved: bool = False) -> dict[str, Any]:
    """Gaps: what discovery or analysis could not read (permission denied, API unavailable, dynamic references ...). Open ones by default; each says how to fix it."""
    items, nxt = _d(ctx).list_gaps(principal, source_id=source_id, scan_id=scan_id, kind=kind, asset_id=asset_id, resolved=resolved, limit=page.limit, cursor=page.cursor)
    return {"items": items, "next_cursor": nxt}


@router.get("/intelligence/coverage")
def coverage(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Per source: active assets by type, open gaps by kind, last scan, next scan due; plus whether any sampling policy is enabled."""
    return _d(ctx).coverage(principal)


# ---------------------------------------------------------------------------------------------------------------- scanner import
@router.post("/intelligence/import/scanner", status_code=202)
async def import_scanner(request: Request, principal: Authed, ctx: Ctx, name: str | None = Query(None, max_length=128), tenant_id: str | None = Query(None, max_length=128),
                         workspace_id: str | None = Query(None, max_length=64), scan: bool = True, wait: bool = False) -> dict[str, Any]:
    """Import a saved Power BI admin scanner result (the JSON body of ``GET /admin/workspaces/scanResult/{scanId}``) as a ``scanner_import`` source and scan it. The body is
    validated and refused if it looks like it contains a credential. Importing again under the same ``name`` replaces the stored document."""
    limit = scanner_import.MAX_IMPORT_BYTES
    declared = request.headers.get("content-length")
    if declared and declared.isdigit() and int(declared) > limit:
        raise BadRequest(f"the scanner result is larger than {limit // (1024 * 1024)} MB", remediation="export fewer workspaces per file")
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > limit:
            raise BadRequest(f"the scanner result is larger than {limit // (1024 * 1024)} MB", remediation="export fewer workspaces per file")
    return await run_in_threadpool(_d(ctx).import_scanner, principal, bytes(body), name=name, tenant_id=tenant_id, workspace_id=workspace_id, scan=scan, wait=wait)


# ---------------------------------------------------------------------------------------------------------------- suggestions and sampling policies
@router.get("/intelligence/assets/{asset_id}/suggestions")
def suggestions(asset_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Suggested sensitive-data classifications for a table or column (from names and types; no data is read). Accepting one is a governance action."""
    return _d(ctx).suggest(principal, asset_id)


@router.get("/intelligence/assets/{asset_id}/sampling-policy")
def asset_policy(asset_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """The sampling policy that applies to an asset (``allowed: false`` with the reason when sampling is off - the default)."""
    return _d(ctx).effective_policy(principal, asset_id)


@router.get("/intelligence/sampling-policies")
def list_policies(principal: Authed, ctx: Ctx, scope_type: str | None = Query(None, pattern="^(global|workspace|source|asset)$")) -> dict[str, Any]:
    """Sampling policies the caller manages. With none, nothing is ever sampled."""
    return {"items": _d(ctx).list_policies(principal, scope_type=scope_type)}


@router.put("/intelligence/sampling-policies")
def put_policy(body: PolicyPut, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Create or replace the sampling policy of one scope: ``max_rows`` (0..100), ``masking`` (none|hash|redact|partial) and whether classified / sensitive columns are excluded."""
    return _d(ctx).put_policy(principal, scope_type=body.scope_type, scope_id=body.scope_id, max_rows=body.max_rows, masking=body.masking, exclude_classified=body.exclude_classified,
                              enabled=body.enabled)


@router.delete("/intelligence/sampling-policies/{policy_id}")
def delete_policy(policy_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Remove a sampling policy (the scope falls back to the next wider policy, and finally to off)."""
    return _d(ctx).delete_policy(principal, policy_id)
