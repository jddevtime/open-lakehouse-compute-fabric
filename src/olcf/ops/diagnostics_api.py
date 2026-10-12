# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""HTTP API of diagnostics and support bundles (permission ``admin.diagnostics``).

``POST /admin/diagnostics``                        run the suite (``?wait`` default true; options in the body); ``GET`` returns the last report
                                                  (always 200: ``last_run`` is ``null`` and ``checks`` empty until the first run)
``GET|POST /admin/support-bundle``                 list / create a sanitized, leak-scanned support bundle (opt-in sections in the body)
``GET|DELETE /admin/support-bundle/{id}``, ``GET .../download``
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict, Field

from olcf.api.deps import Authed, Ctx

router = APIRouter(tags=["diagnostics"])


class DiagnosticsBody(BaseModel):
    """Request body for running diagnostics: a subset of checks, a real Spark smoke job and per-worker probes."""
    model_config = ConfigDict(extra="forbid")
    only: list[str] | None = Field(None, description="subset of: platform, database, health, auth, storage, compute, network, time, certificates, logs, config")
    spark: bool = Field(False, description="run a real distributed Spark smoke job (needs compute.manage)")
    worker_probes: bool = Field(False, description="run the per-node probe matrix on every worker")
    network_time: bool = Field(False, description="compare the clock with an HTTPS endpoint's Date header")
    deep_health: bool = True


class BundleBody(BaseModel):
    """Request body for creating a support bundle: whether to pseudonymise addresses and whether to include logs and configuration."""
    model_config = ConfigDict(extra="forbid")
    sanitize: bool = Field(True, description="pseudonymise IP addresses, e-mail addresses and host names")
    include_logs: bool = True
    include_config: bool = True
    include_diagnostics: bool = True
    include_health: bool = True
    include_audit: bool = Field(False, description="OPT-IN: recent audit records")
    audit_limit: int = Field(500, ge=1, le=5000)
    include_jobs: bool = Field(False, description="OPT-IN: recent job states/errors (never source or parameters)")
    job_limit: int = Field(100, ge=1, le=1000)
    include_worker_diagnostics: bool = Field(False, description="OPT-IN: every connected worker's own redacted diagnostics archive")
    log_tail_kb: int = Field(1024, ge=16, le=8192)


@router.post("/admin/diagnostics")
def run_diagnostics(body: DiagnosticsBody, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("admin.diagnostics")
    return ctx.service("diagnostics").run(principal, only=body.only, spark=body.spark, worker_probes=body.worker_probes, network_time=body.network_time, deep_health=body.deep_health)


@router.get("/admin/diagnostics")
def last_diagnostics(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("admin.diagnostics")
    rep = ctx.service("diagnostics").last()
    if rep is None:
        # not an error: nothing has been run yet (a 404 here only produced console noise in the UI and spurious errors in scripts)
        return {"last_run": None, "id": None, "status": "unknown", "summary": {"ok": 0, "warn": 0, "fail": 0, "skipped": 0}, "checks": [],
                "message": "diagnostics have not been run yet", "remediation": "POST /v1/admin/diagnostics"}
    return {**rep, "last_run": rep.get("started_at")}


@router.get("/admin/support-bundle")
def list_bundles(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("admin.diagnostics")
    return {"items": ctx.service("support").list()}


@router.post("/admin/support-bundle", status_code=201)
def create_bundle(body: BundleBody, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("admin.diagnostics")
    if body.include_audit:
        principal.require("audit.read")  # the opt-in audit section is an audit export
    return ctx.service("support").create(principal, body.model_dump())


@router.get("/admin/support-bundle/{bid}")
def get_bundle(bid: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("admin.diagnostics")
    return ctx.service("support").public(ctx.service("support").get(bid))


@router.get("/admin/support-bundle/{bid}/download")
def download_bundle(bid: str, principal: Authed, ctx: Ctx) -> FileResponse:
    principal.require("admin.diagnostics")
    svc = ctx.service("support")
    rec = svc.get(bid)
    ctx.audit.record(principal, "admin.support_bundle_downloaded", resource_type="support_bundle", resource_id=bid)
    return FileResponse(svc.file_path(rec), media_type="application/zip", filename=rec["filename"], headers={"Cache-Control": "no-store"})


@router.delete("/admin/support-bundle/{bid}", status_code=204)
def delete_bundle(bid: str, principal: Authed, ctx: Ctx) -> None:
    principal.require("admin.diagnostics")
    ctx.service("support").delete(principal, bid)
