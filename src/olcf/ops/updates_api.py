# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""HTTP API of platform updates (``/v1/admin/updates``; permission ``admin.update``, trust-store changes need ``admin.settings`` = PlatformAdmin).

``GET /admin/updates``                       status (installed, channel, available, hand-off, maintenance) + history
``GET|PUT /admin/updates/config``            channel, feed, proxy/CA, maintenance windows, trust threshold
``POST /admin/updates/check``                check the feed (signature-verified)
``POST /admin/updates/import``               offline bundle upload (tar/tar.gz/tar.zst with a signed manifest.json)
``GET|POST|DELETE /admin/updates/trust``     Ed25519 trust store (who may sign releases)
``POST /admin/updates/maintenance``          manually drain / resume compute
``GET /admin/updates/{id}``, ``POST .../preflight|stage|apply|cancel|rollback``
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, File, UploadFile
from pydantic import BaseModel, ConfigDict, Field

from olcf.api.deps import Authed, Ctx
from olcf.ops.common import run_operation

router = APIRouter(tags=["updates"])


def _svc(ctx):  # noqa: ANN001, ANN202
    return ctx.service("updates")


class CheckBody(BaseModel):
    """Request body for checking for updates on a channel."""
    model_config = ConfigDict(extra="forbid")
    channel: str | None = None


class ConfigBody(BaseModel):
    """Request body for changing the update channel, feed URL and proxy."""
    model_config = ConfigDict(extra="forbid")
    channel: str | None = None
    feed_url: str | None = Field(None, max_length=2048)
    proxy: str | None = Field(None, max_length=512)
    ca_bundle: str | None = Field(None, max_length=1024)
    allow_insecure_http: bool | None = None
    threshold: int | None = None
    maintenance_windows: list[dict[str, Any]] | None = None
    drain_timeout_seconds: int | None = Field(None, ge=0, le=86400)
    auto_check: bool | None = None
    check_interval_hours: int | None = Field(None, ge=1, le=720)
    restart_command: list[str] | None = None
    fetch_previous: bool | None = None
    dry_run_max_rows: int | None = Field(None, ge=0)


class TrustBody(BaseModel):
    """Request body for trusting an Ed25519 public key to sign update manifests."""
    model_config = ConfigDict(extra="forbid")
    public_key: str = Field(max_length=200, description="base64 raw Ed25519 public key")
    name: str = Field(max_length=100)


class ApplyBody(BaseModel):
    """Request body for applying a staged update: a confirmation, an override of the maintenance window and whether to start the hand-off.
    """
    model_config = ConfigDict(extra="forbid")
    confirm: str
    override_window: bool = False
    execute: bool = Field(False, description="also start the upgrade hand-off (python -m olcf.ops.upgrade apply) after preparation")


class MaintenanceBody(BaseModel):
    """Request body for switching maintenance mode on or off."""
    model_config = ConfigDict(extra="forbid")
    on: bool


@router.get("/admin/updates")
def status(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("admin.update")
    return {**_svc(ctx).status(), "items": _svc(ctx).list(30)}


@router.get("/admin/updates/config")
def get_config(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("admin.update")
    return _svc(ctx).get_config()


@router.put("/admin/updates/config")
def put_config(body: ConfigBody, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("admin.update")
    return _svc(ctx).set_config(principal, body.model_dump(exclude_unset=True))


@router.post("/admin/updates/check")
def check(body: CheckBody, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("admin.update")
    return _svc(ctx).check(principal, body.channel)


@router.post("/admin/updates/import", status_code=201)
def import_bundle(principal: Authed, ctx: Ctx, file: UploadFile = File(...)) -> dict[str, Any]:  # noqa: B008
    principal.require("admin.update")
    return _svc(ctx).import_bundle(principal, file.file, file.filename or "bundle")


@router.get("/admin/updates/trust")
def trust_list(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("admin.update")
    return {"items": _svc(ctx).trust_list(), "threshold": _svc(ctx).get_config()["threshold"]}


@router.post("/admin/updates/trust", status_code=201)
def trust_add(body: TrustBody, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("admin.settings")  # deciding who may ship code to the control plane is a PlatformAdmin matter
    return _svc(ctx).trust_add(principal, body.public_key, body.name)


@router.delete("/admin/updates/trust/{keyid}", status_code=204)
def trust_revoke(keyid: str, principal: Authed, ctx: Ctx) -> None:
    principal.require("admin.settings")
    _svc(ctx).trust_revoke(principal, keyid)


@router.post("/admin/updates/maintenance")
def maintenance(body: MaintenanceBody, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("admin.update")
    svc = _svc(ctx)
    out = svc.drain(principal) if body.on else svc.undrain(principal)
    ctx.audit.record(principal, "admin.maintenance", details={"on": body.on})
    return {**out, "maintenance": svc.maintenance_state()}


@router.get("/admin/updates/{uid}")
def get_update(uid: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("admin.update")
    return _svc(ctx).public(_svc(ctx).get(uid))


@router.post("/admin/updates/{uid}/stage")
def stage(uid: str, principal: Authed, ctx: Ctx, wait: bool = False) -> dict[str, Any]:
    principal.require("admin.update")
    svc = _svc(ctx)
    svc.get(uid)
    return run_operation(ctx, "update.stage", principal, lambda p: svc.public(svc.stage(uid, p)), wait=wait)


@router.post("/admin/updates/{uid}/preflight")
def preflight(uid: str, principal: Authed, ctx: Ctx, wait: bool = False) -> dict[str, Any]:
    principal.require("admin.update")
    svc = _svc(ctx)
    svc.get(uid)
    return run_operation(ctx, "update.preflight", principal, lambda p: svc.preflight(uid, p), wait=wait)


@router.post("/admin/updates/{uid}/apply", status_code=202)
def apply(uid: str, body: ApplyBody, principal: Authed, ctx: Ctx, wait: bool = False) -> dict[str, Any]:
    principal.require("admin.update")
    return _svc(ctx).start_apply(principal, uid, confirm=body.confirm, override_window=body.override_window, execute=body.execute, wait=wait)


@router.post("/admin/updates/{uid}/cancel")
def cancel(uid: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("admin.update")
    return _svc(ctx).cancel(principal, uid)


@router.post("/admin/updates/{uid}/rollback")
def rollback(uid: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("admin.update")
    return _svc(ctx).rollback(principal, uid)
