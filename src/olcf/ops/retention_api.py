# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""HTTP API of retention and housekeeping (``/v1/admin/retention``).

``GET  /admin/retention``            status: effective settings (change them with ``PUT /v1/admin/settings/retention.<name>``), last run, next run
``POST /admin/retention/run``        run now; ``dry_run`` (default **true**) reports what would be deleted. A real run needs ``admin.settings``
``GET  /admin/retention/runs``       report history; ``/runs/{id}`` one report
``GET  /admin/retention/audit``      verify the audit hash chain including its retention anchors (needs ``audit.read``)

Dry runs need ``admin.diagnostics``; deleting data needs ``admin.settings`` (PlatformAdmin).
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query
from pydantic import BaseModel, ConfigDict, Field

from olcf.api.deps import Authed, Ctx

router = APIRouter(tags=["retention"])


class RunBody(BaseModel):
    """Request body for running retention: a dry-run switch (on by default) and an optional subset of steps."""
    model_config = ConfigDict(extra="forbid")
    dry_run: bool = True
    only: list[str] | None = Field(None, description="subset of steps (events, audit, metrics, jobs, job_logs, recycle_bin, tokens, idempotency, nonces, operations, support_bundles, retention_runs)")


@router.get("/admin/retention")
def retention_status(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("admin.diagnostics")
    out = ctx.service("retention").status()
    if not principal.can("audit.read"):
        out.pop("audit", None)
    return out


@router.post("/admin/retention/run")
def retention_run(body: RunBody, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("admin.diagnostics" if body.dry_run else "admin.settings")
    return ctx.service("retention").run(principal, dry_run=body.dry_run, only=body.only)


@router.get("/admin/retention/runs")
def retention_runs(principal: Authed, ctx: Ctx, limit: int = Query(20, ge=1, le=200)) -> dict[str, Any]:
    principal.require("admin.diagnostics")
    return {"items": ctx.service("retention").runs(limit)}


@router.get("/admin/retention/runs/{rid}")
def retention_get_run(rid: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("admin.diagnostics")
    return ctx.service("retention").get_run(rid)


@router.get("/admin/retention/audit")
def retention_audit(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("audit.read")
    return ctx.service("retention").verify_audit()
