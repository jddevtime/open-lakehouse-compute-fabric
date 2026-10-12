# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""REST API of Git sync (``/workspaces/{ws}/git/...``) and deployment promotion (``/deployment-pipelines``, ``/deployments``).

Permissions: connect / disconnect / switch / create branch need ``workspace.update``; ``status`` / ``branches`` / ``commit`` need ``workspace.export``; ``pull`` needs
``workspace.import`` (and per-item ``item.create|update|delete`` is enforced for every applied unit); using a connection's secret needs ``connection.use``.  A
deployment needs ``workspace.export`` on the source and ``workspace.import`` on the target stage; pipelines are visible to principals who can read one of their
workspaces and managed by principals who can update all of them.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query, Response
from pydantic import BaseModel

from olcf.api.deps import Authed, Ctx, IfMatch, Pagination, etag_header
from olcf.notebooks.gitsync import (
    BranchCreate,
    BranchSwitch,
    CommitRequest,
    ConnectRequest,
    GitService,
    PullRequest,
)
from olcf.notebooks.promotion import DeploymentService, DeployRequest, PipelineCreate, PipelineUpdate

router = APIRouter(tags=["git-sync"])


class GitChange(BaseModel):
    """One changed unit in a Git status: unit, kind, item type and name."""
    unit: str
    kind: str
    type: str
    name: str
    item_id: str | None = None
    direction: str
    local: str
    remote: str


class GitStatus(BaseModel):
    """Git status of a workspace: whether it is connected, the repository, branch and folder, and the pending changes."""
    connected: bool
    repo_url: str | None = None
    branch: str | None = None
    folder: str | None = None
    notebook_format: str | None = None
    connection: str | None = None
    synced_commit: str | None = None
    remote_commit: str | None = None
    remote_branch_exists: bool | None = None
    fetched: bool | None = None
    in_sync: bool | None = None
    counts: dict[str, int] = {}
    changes: list[GitChange] = []
    skipped_paths: list[str] = []


def git(ctx: Ctx) -> GitService:
    return ctx.service("git")


@router.get("/workspaces/{ws}/git/capabilities")
def capabilities(ws: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """``{available, path, version, backend, remediation, platform, connected}``: whether Git sync can work on this control plane (no git, or only the macOS
    installer shim, means ``available: false`` and the other Git endpoints answer 503 ``git_unavailable``)."""
    return git(ctx).capabilities(principal, ws)


@router.post("/workspaces/{ws}/git/connect", response_model=GitStatus, status_code=201)
def connect(ws: str, body: ConnectRequest, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Connect the workspace to ``repo_url`` / ``branch`` / ``folder`` (credentials through a ``git`` connection; never in the URL)."""
    return git(ctx).connect(principal, ws, body)


@router.post("/workspaces/{ws}/git/disconnect")
def disconnect(ws: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Disconnect and delete the local clone; items and the repository are untouched."""
    return git(ctx).disconnect(principal, ws)


@router.get("/workspaces/{ws}/git/status", response_model=GitStatus)
def status(ws: str, principal: Authed, ctx: Ctx, fetch: bool = Query(True, description="fetch the remote branch first")) -> dict[str, Any]:
    """Changed / added / deleted units vs the last synchronised commit and what the remote changed (``direction``: outgoing, incoming, conflict)."""
    return git(ctx).status(principal, ws, fetch=fetch)


@router.post("/workspaces/{ws}/git/commit")
def commit(ws: str, body: CommitRequest, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Commit and push the outgoing changes.  409 when the remote has changes to pull first, 422 when the secret scan rejects the commit."""
    return git(ctx).commit(principal, ws, body)


@router.post("/workspaces/{ws}/git/pull")
def pull(ws: str, principal: Authed, ctx: Ctx, body: PullRequest | None = None) -> dict[str, Any]:
    """Update the workspace from the repository; conflicting units need ``resolution`` / ``resolutions`` = ``ours`` | ``theirs``."""
    return git(ctx).pull(principal, ws, body)


@router.get("/workspaces/{ws}/git/branches")
def branches(ws: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    return git(ctx).branches(principal, ws)


@router.post("/workspaces/{ws}/git/branches", status_code=201)
def create_branch(ws: str, body: BranchCreate, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    return git(ctx).create_branch(principal, ws, body)


@router.put("/workspaces/{ws}/git/branch", response_model=GitStatus)
def switch_branch(ws: str, body: BranchSwitch, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Point the workspace at another branch (content is not changed; status / pull show the differences)."""
    return git(ctx).switch_branch(principal, ws, body)


# ------------------------------------------------------------------------------ deployment promotion
def dep(ctx: Ctx) -> DeploymentService:
    return ctx.service("deployments")


@router.post("/deployment-pipelines", status_code=201)
def create_pipeline(body: PipelineCreate, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    row = dep(ctx).create_pipeline(principal, body)
    response.headers["ETag"] = etag_header(row["etag"])
    return DeploymentService.public(row)


@router.get("/deployment-pipelines")
def list_pipelines(principal: Authed, ctx: Ctx, page: Pagination) -> dict[str, Any]:
    rows, nxt = dep(ctx).list_pipelines(principal, limit=page.limit, cursor=page.cursor)
    return {"items": [DeploymentService.public(r) for r in rows], "next_cursor": nxt}


@router.get("/deployment-pipelines/{pid}")
def get_pipeline(pid: str, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    row = dep(ctx).get(principal, pid)
    response.headers["ETag"] = etag_header(row["etag"])
    return DeploymentService.public(row)


@router.patch("/deployment-pipelines/{pid}")
def update_pipeline(pid: str, body: PipelineUpdate, principal: Authed, ctx: Ctx, response: Response, if_match: IfMatch = None) -> dict[str, Any]:
    row = dep(ctx).update_pipeline(principal, pid, body, if_match)
    response.headers["ETag"] = etag_header(row["etag"])
    return DeploymentService.public(row)


@router.delete("/deployment-pipelines/{pid}", status_code=204)
def delete_pipeline(pid: str, principal: Authed, ctx: Ctx) -> None:
    dep(ctx).delete_pipeline(principal, pid)


@router.post("/deployment-pipelines/{pid}/deploy", status_code=201)
def deploy(pid: str, body: DeployRequest, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Promote one stage to the next (``dry_run`` plans without writing).  The record lists every unit created / updated / unchanged / deleted."""
    return dep(ctx).deploy(principal, pid, body)


@router.get("/deployment-pipelines/{pid}/deployments")
def list_deployments(pid: str, principal: Authed, ctx: Ctx, limit: int = Query(50, ge=1, le=200)) -> dict[str, Any]:
    return {"items": dep(ctx).deployments(principal, pid, limit=limit)}


@router.get("/deployments/{did}")
def get_deployment(did: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    return dep(ctx).deployment(principal, did)
