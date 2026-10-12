# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""HTTP API of backup / restore / disaster recovery (all under ``/v1/admin/backup``; permission ``admin.backup``).

``GET|POST /admin/backup``                          list / start a backup (async operation; ``?wait=true`` blocks)
``GET|PUT /admin/backup/config``                    schedule, retention, targets, include_keys, write-only passphrase
``GET|POST /admin/backup/targets`` ...              off-host targets (local, s3, sftp, lakehouse) + ``/test``
``POST /admin/backup/import``                       upload a backup file made elsewhere
``POST /admin/backup/retention``                    apply retention now (``dry_run``)
``GET|DELETE /admin/backup/{id}``, ``/download``    inspect / delete / download the (encrypted) file
``POST /admin/backup/{id}/validate|restore-test``   verify / prove restorability on a throw-away database
``GET /admin/backup/{id}/prerequisites``            what a restore needs
``POST /admin/backup/{id}/restore``                 restore into the live database (requires ``confirm: "RESTORE"``)
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, File, Query, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict, Field

from olcf.api.deps import Authed, Ctx
from olcf.ops.common import run_operation

router = APIRouter(tags=["backup"])


def _svc(ctx):  # noqa: ANN001, ANN202
    return ctx.service("backup")


class BackupStart(BaseModel):
    """Request body for starting a backup: whether to include keys, a one-off passphrase and the targets to copy to."""
    model_config = ConfigDict(extra="forbid")
    include_keys: bool | None = Field(None, description="include the CA/signing/master keys (needs a passphrase)")
    passphrase: str | None = Field(None, max_length=1024, description="one-off passphrase; defaults to the configured one, else the platform master key")
    targets: list[str] | None = Field(None, description="target names; defaults to the configured ones")


class ConfigPut(BaseModel):
    """Request body for changing the backup schedule, retention and restore-test settings."""
    model_config = ConfigDict(extra="forbid")
    schedule: dict[str, Any] | None = None
    retention: dict[str, Any] | None = None
    restore_test: dict[str, Any] | None = None
    include_keys: bool | None = None
    targets: list[str] | None = None
    passphrase: str | None = Field(None, max_length=1024, description="write-only; stored in the secret store")


class TargetBody(BaseModel):
    """Request body for adding a backup target: name, type and configuration."""
    model_config = ConfigDict(extra="forbid")
    name: str
    type: str
    config: dict[str, Any] = Field(default_factory=dict)
    secret: dict[str, Any] | str | None = Field(None, description="credentials; stored in the secret store, never returned")
    secret_id: str | None = None
    enabled: bool = True
    retention: dict[str, Any] = Field(default_factory=dict)


class TargetPatch(BaseModel):
    """Request body for changing a backup target's configuration, secret or enabled state."""
    model_config = ConfigDict(extra="forbid")
    config: dict[str, Any] | None = None
    secret: dict[str, Any] | str | None = None
    enabled: bool | None = None
    retention: dict[str, Any] | None = None


class PassphraseBody(BaseModel):
    """Request body that carries an optional passphrase."""
    model_config = ConfigDict(extra="forbid")
    passphrase: str | None = Field(None, max_length=1024)


class RestoreBody(BaseModel):
    """Request body for restoring a backup: a confirmation string, an optional passphrase and whether to restore keys."""
    model_config = ConfigDict(extra="forbid")
    confirm: str
    passphrase: str | None = Field(None, max_length=1024)
    restore_keys: bool = Field(False, description="also write key/PKI files contained in the backup into the data directory (overwrites)")


@router.get("/admin/backup")
def list_backups(principal: Authed, ctx: Ctx, limit: int = Query(100, ge=1, le=500)) -> dict[str, Any]:
    principal.require("admin.backup")
    return {"items": _svc(ctx).list(limit), "health": _svc(ctx).health()}


@router.post("/admin/backup", status_code=202)
def start_backup(body: BackupStart, principal: Authed, ctx: Ctx, wait: bool = False) -> dict[str, Any]:
    principal.require("admin.backup")
    out = _svc(ctx).start_backup(principal, include_keys=body.include_keys, passphrase=body.passphrase, targets=body.targets, wait=wait)
    if wait:
        out["backup"] = _svc(ctx).public(_svc(ctx).get(out["backup_id"]))
    return out


@router.get("/admin/backup/config")
def get_config(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("admin.backup")
    return _svc(ctx).public_config()


@router.put("/admin/backup/config")
def put_config(body: ConfigPut, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("admin.backup")
    data = body.model_dump(exclude_unset=True)
    passphrase = data.pop("passphrase", None)
    return _svc(ctx).set_config(principal, {k: v for k, v in data.items() if v is not None}, passphrase)


@router.get("/admin/backup/targets")
def list_targets(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("admin.backup")
    return {"items": [_svc(ctx).public_target(t) for t in _svc(ctx).list_targets()], "types": ["local", "s3", "sftp", "lakehouse"]}


@router.post("/admin/backup/targets", status_code=201)
def create_target(body: TargetBody, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("admin.backup")
    return _svc(ctx).create_target(principal, body.model_dump())


@router.get("/admin/backup/targets/{ref}")
def get_target(ref: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("admin.backup")
    return _svc(ctx).public_target(_svc(ctx).get_target(ref))


@router.patch("/admin/backup/targets/{ref}")
def patch_target(ref: str, body: TargetPatch, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("admin.backup")
    return _svc(ctx).update_target(principal, ref, body.model_dump(exclude_unset=True))


@router.delete("/admin/backup/targets/{ref}", status_code=204)
def delete_target(ref: str, principal: Authed, ctx: Ctx) -> None:
    principal.require("admin.backup")
    _svc(ctx).delete_target(principal, ref)


@router.post("/admin/backup/targets/{ref}/test")
def test_target(ref: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("admin.backup")
    res = _svc(ctx).test_target(ref)
    ctx.audit.record(principal, "admin.backup_target_tested", resource_type="backup_target", resource_id=ref, outcome="success" if res["ok"] else "failure")
    return res


@router.get("/admin/backup/targets/{ref}/files")
def target_files(ref: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("admin.backup")
    from olcf.ops import backup_targets as T

    return {"items": T.build_target(ctx, _svc(ctx).get_target(ref)).list()}


@router.post("/admin/backup/import", status_code=201)
def import_backup(principal: Authed, ctx: Ctx, file: UploadFile = File(...)) -> dict[str, Any]:  # noqa: B008
    principal.require("admin.backup")
    return _svc(ctx).import_file(principal, file.file, file.filename or "upload.olcfbak")


@router.post("/admin/backup/retention")
def run_retention(principal: Authed, ctx: Ctx, dry_run: bool = True) -> dict[str, Any]:
    principal.require("admin.backup")
    rep = _svc(ctx).apply_retention(dry_run=dry_run)
    ctx.audit.record(principal, "admin.backup_retention", details={"dry_run": dry_run, "local": len(rep["local"])})
    return {"dry_run": dry_run, **rep}


@router.get("/admin/backup/{bid}")
def get_backup(bid: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("admin.backup")
    return _svc(ctx).public(_svc(ctx).get(bid))


@router.delete("/admin/backup/{bid}")
def delete_backup(bid: str, principal: Authed, ctx: Ctx, everywhere: bool = True) -> dict[str, Any]:
    principal.require("admin.backup")
    return _svc(ctx).delete(principal, bid, everywhere=everywhere)


@router.get("/admin/backup/{bid}/download")
def download_backup(bid: str, principal: Authed, ctx: Ctx) -> FileResponse:
    principal.require("admin.backup")
    svc = _svc(ctx)
    rec = svc.get(bid)
    path = svc.file_path(rec)
    ctx.audit.record(principal, "admin.backup_downloaded", resource_type="backup", resource_id=bid, details={"size": rec.get("size_bytes")})
    return FileResponse(path, media_type="application/octet-stream", filename=rec["filename"], headers={"Cache-Control": "no-store"})


@router.get("/admin/backup/{bid}/prerequisites")
def prerequisites(bid: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("admin.backup")
    return _svc(ctx).prerequisites(bid)


@router.post("/admin/backup/{bid}/validate")
def validate(bid: str, body: PassphraseBody, principal: Authed, ctx: Ctx, wait: bool = False) -> dict[str, Any]:
    principal.require("admin.backup")
    svc = _svc(ctx)
    svc.get(bid)
    return run_operation(ctx, "backup.validate", principal, lambda p: svc.validate(principal, bid, body.passphrase), wait=wait)


@router.post("/admin/backup/{bid}/restore-test")
def restore_test(bid: str, body: PassphraseBody, principal: Authed, ctx: Ctx, wait: bool = False) -> dict[str, Any]:
    principal.require("admin.backup")
    svc = _svc(ctx)
    svc.get(bid)
    return run_operation(ctx, "backup.restore_test", principal, lambda p: svc.restore_test(principal, bid, body.passphrase), wait=wait)


@router.post("/admin/backup/{bid}/restore", status_code=202)
def restore(bid: str, body: RestoreBody, principal: Authed, ctx: Ctx, wait: bool = False) -> dict[str, Any]:
    principal.require("admin.backup")
    return _svc(ctx).start_restore(principal, bid, passphrase=body.passphrase, confirm=body.confirm, restore_keys=body.restore_keys, wait=wait)
