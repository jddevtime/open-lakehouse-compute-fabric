# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""REST API of the notebook service (mounted under ``/v1``).

``GET/PUT /notebooks/{id}/content`` (cells with stable ids, ETag / If-Match), ``/parameters``, ``/versions`` (+ restore), ``/diff?a=&b=``,
``/export?format=ipynb|py|fabric|html``, ``POST /workspaces/{ws}/notebooks/import`` (multipart), ``POST /notebooks/{id}/run``, ``/session`` and ``GET /runs``.

Every handler authorises with the workspace of the *stored* item (never a client supplied id) and returns plain, secret-free JSON.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, File, Query, Response, UploadFile
from pydantic import BaseModel, ConfigDict
from starlette.concurrency import run_in_threadpool

from olcf.api.deps import Authed, Ctx, IdempotencyKey, IfMatch, etag_header
from olcf.core.errors import BadRequest
from olcf.core.redaction import redact_obj
from olcf.notebooks import params
from olcf.notebooks.htmlexport import CSP
from olcf.notebooks.service import ContentPut, NotebookService, RunRequest, SessionRequest

router = APIRouter(tags=["notebooks"])


class _Out(BaseModel):
    model_config = ConfigDict(extra="allow")


class NotebookContent(_Out):
    """A notebook as the API returns it: item identity, version and the notebook document."""
    id: str
    workspace_id: str
    name: str
    version: int
    etag: str
    cells: list[dict[str, Any]]
    metadata: dict[str, Any]
    settings: dict[str, Any]
    parameters: dict[str, Any]


class ImportResult(_Out):
    """Result of importing notebooks into a workspace: the items and how many were created and overwritten."""
    workspace_id: str
    items: list[dict[str, Any]]
    created: int
    overwritten: int
    skipped: int


class DiffResult(_Out):
    """Result of comparing two versions of a notebook: whether they are identical and the differences."""
    item_id: str
    a: int
    b: int
    identical: bool
    summary: dict[str, int]
    cells: list[dict[str, Any]]
    text: str


def svc(ctx: Ctx) -> NotebookService:
    return ctx.service("notebooks")


# ------------------------------------------------------------------------------ content
@router.get("/notebooks/{item_id}/content", response_model=NotebookContent)
def get_content(item_id: str, principal: Authed, ctx: Ctx, response: Response, outputs: bool = True) -> dict[str, Any]:
    s = svc(ctx)
    row = s.item(principal, item_id, "item.read")
    response.headers["ETag"] = etag_header(row["etag"])
    return s.content(row, outputs=outputs)


@router.put("/notebooks/{item_id}/content", response_model=NotebookContent)
def put_content(item_id: str, body: ContentPut, principal: Authed, ctx: Ctx, response: Response, if_match: IfMatch = None) -> dict[str, Any]:
    s = svc(ctx)
    row = s.item(principal, item_id, "item.update")
    new = s.put_content(principal, row, body, if_match)
    response.headers["ETag"] = etag_header(new["etag"])
    return s.content(new)


@router.get("/notebooks/{item_id}/parameters")
def get_parameters(item_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """The parameters cell as a typed list (defaults inferred from literal assignments)."""
    s = svc(ctx)
    row = s.item(principal, item_id, "item.read")
    return s.detect_parameters(s.load(row)).to_dict()


class InjectBody(BaseModel):
    """Request body for previewing parameter injection into a notebook. Unknown fields are rejected."""
    model_config = ConfigDict(extra="forbid")
    parameters: dict[str, Any] = {}


@router.post("/notebooks/{item_id}/parameters/preview")
def preview_injection(item_id: str, body: InjectBody, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Validate ``parameters`` against the notebook and return the injected cell exactly as the runner will create it (nothing is saved or run)."""
    s = svc(ctx)
    row = s.item(principal, item_id, "item.read")
    nb = s.load(row)
    det = s.detect_parameters(nb)
    values = params.coerce_parameters(det.parameters, body.parameters, allow_undeclared=True, require=True)
    injected = params.inject_parameters(nb.nb, values)
    cell = next((c for c in injected["cells"] if "injected-parameters" in (c.get("metadata") or {}).get("tags", [])), None)
    return {"parameters": values, "cell": cell, "cell_index": None if cell is None else injected["cells"].index(cell)}


# ------------------------------------------------------------------------------ versions / diff
@router.get("/notebooks/{item_id}/versions")
def list_versions(item_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    s = svc(ctx)
    row = s.item(principal, item_id, "item.read", include_deleted=True)
    return {"items": s.versions(row), "next_cursor": None}


@router.get("/notebooks/{item_id}/versions/{version}")
def get_version(item_id: str, version: int, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    s = svc(ctx)
    row = s.item(principal, item_id, "item.read", include_deleted=True)
    n, defn = s.version_definition(row, version)
    d = s.validate_definition(defn)
    return {"item_id": row["id"], "version": n, "current": n == row["version"], "nbformat": d["nbformat"]["nbformat"], "metadata": d["nbformat"]["metadata"],
            "cells": d["nbformat"]["cells"], "settings": d.get("settings") or {}}


@router.post("/notebooks/{item_id}/versions/{version}/restore", response_model=NotebookContent)
def restore_version(item_id: str, version: int, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    s = svc(ctx)
    row = s.item(principal, item_id, "item.update")
    new = s.restore(principal, row, version)
    response.headers["ETag"] = etag_header(new["etag"])
    return s.content(new)


@router.get("/notebooks/{item_id}/diff", response_model=DiffResult)
def diff(item_id: str, principal: Authed, ctx: Ctx, a: str | None = Query(None, description="version number, 'previous' (default) or 'current'"),
         b: str | None = Query(None, description="version number or 'current' (default)"), context: int = Query(3, ge=0, le=50)) -> dict[str, Any]:
    """Per-cell unified diff between two saved versions; outputs and execution counts are ignored."""
    s = svc(ctx)
    row = s.item(principal, item_id, "item.read", include_deleted=True)
    return s.diff(row, a, b, context=context)


# ------------------------------------------------------------------------------ export / import
@router.get("/notebooks/{item_id}/export")
def export(item_id: str, principal: Authed, ctx: Ctx, format: str = Query("ipynb", pattern="^(ipynb|py|fabric|fabric_git_source|html)$"),
           version: str | None = None, outputs: bool = True) -> Response:
    s = svc(ctx)
    row = s.item(principal, item_id, "item.read")
    data, media, filename = s.export(row, format, version=version, outputs=outputs)
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}  # the foundation middleware adds X-Content-Type-Options: nosniff
    if media.startswith("text/html"):
        headers["Content-Security-Policy"] = CSP
        headers["Content-Disposition"] = f'inline; filename="{filename}"'
    ctx.audit.record(principal, "notebook.exported", resource_type="notebook", resource_id=row["id"], workspace_id=row["workspace_id"], details={"format": format})
    return Response(content=data, media_type=media, headers=headers)


@router.post("/workspaces/{ws}/notebooks/import", status_code=201, response_model=ImportResult)
async def import_notebooks(ws: str, principal: Authed, ctx: Ctx, file: list[UploadFile] | None = File(None), files: list[UploadFile] | None = File(None),
                           on_conflict: str = Query("rename", pattern="^(rename|skip|overwrite)$"), format: str | None = Query(None, pattern="^(auto|ipynb|py|fabric|fabric_git_source)$"),
                           folder: str = Query("", max_length=256), strip_outputs: bool = False) -> dict[str, Any]:
    """Import ``.ipynb``, plain ``.py`` (percent), Fabric Git ``notebook-content.py`` or a ``.zip`` of several (multipart field ``file`` or ``files``)."""
    wsrow = ctx.service("workspaces").resolve(ws)
    principal.require("item.create", workspace_id=wsrow["id"])
    uploads = [*(file or []), *(files or [])]
    if not uploads:
        raise BadRequest("send at least one file as multipart field 'file' or 'files'")
    limit = ctx.settings.server.request_body_limit_mb * 1024 * 1024
    blobs: list[tuple[str, bytes]] = []
    total = 0
    for u in uploads:
        data = await u.read()
        total += len(data)
        if total > limit:
            raise BadRequest("upload too large")
        blobs.append((u.filename or "notebook.ipynb", data))
    s = svc(ctx)

    def work() -> dict[str, Any]:
        items = s.read_import_files(blobs, fmt=format)
        return s.import_notebooks(principal, wsrow["id"], items, on_conflict=on_conflict, folder=folder, strip_outputs=strip_outputs)

    res = await run_in_threadpool(work)
    ctx.audit.record(principal, "notebook.imported", resource_type="workspace", resource_id=wsrow["id"], workspace_id=wsrow["id"],
                     details={"created": res["created"], "overwritten": res["overwritten"], "skipped": res["skipped"]})
    return res


# ------------------------------------------------------------------------------ execution
@router.post("/notebooks/{item_id}/run", status_code=202)
def run_notebook(item_id: str, body: RunRequest, principal: Authed, ctx: Ctx, idem: IdempotencyKey = None) -> dict[str, Any]:
    """Submit the notebook as a batch job (kind ``notebook``); returns the job record (poll ``GET /v1/jobs/{id}``)."""
    s = svc(ctx)
    row = s.item(principal, item_id, "item.read")
    if idem and not body.idempotency_key:
        body = body.model_copy(update={"idempotency_key": idem})
    return s.run(principal, row, body)


@router.post("/notebooks/{item_id}/session", status_code=201)
def start_session(item_id: str, principal: Authed, ctx: Ctx, body: SessionRequest | None = None) -> dict[str, Any]:
    """Start an interactive session bound to the notebook (runtime, environment and default lakehouse from its settings)."""
    s = svc(ctx)
    row = s.item(principal, item_id, "item.read")
    return s.start_session(principal, row, body or SessionRequest())


@router.get("/notebooks/{item_id}/runs")
def list_runs(item_id: str, principal: Authed, ctx: Ctx, limit: int = Query(50, ge=1, le=200), cursor: str | None = None, state: str | None = None) -> dict[str, Any]:
    s = svc(ctx)
    row = s.item(principal, item_id, "item.read")
    return redact_obj(s.runs(principal, row, limit=limit, cursor=cursor, state=[x for x in (state or "").split(",") if x] or None))


@router.get("/notebooks/{item_id}/pip")
def pip_check(item_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Notebook-time package installs found in the notebook and whether the effective ``%pip`` policy (block|declared|allow) accepts them."""
    s = svc(ctx)
    row = s.item(principal, item_id, "item.read")
    nb = s.load(row)
    env_ref = nb.settings.get("environment_id")
    declared: list[str] = []
    env_policy = None
    if env_ref:
        spec = ctx.service("environments").resolve(s.ctx.service("items").resolve(row["workspace_id"], env_ref, "environment")["id"], workspace_id=row["workspace_id"])
        declared, env_policy = list(spec.get("requirements") or []), spec.get("pip_policy")
    policy = s.effective_pip_policy(nb.settings, env_policy)
    cmds = s.pip_commands(nb.nb)
    try:
        s.check_pip_policy(nb.nb, policy, declared)
        verdict: dict[str, Any] = {"allowed": True}
    except Exception as exc:  # noqa: BLE001 - report the policy decision instead of failing
        verdict = {"allowed": False, "message": getattr(exc, "message", str(exc)), "violations": getattr(exc, "details", {}).get("violations", [])}
    return {"policy": policy, "declared_requirements": declared, "commands": cmds, **verdict}
