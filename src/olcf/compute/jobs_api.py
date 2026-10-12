# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""REST API for jobs, their logs/output/artifacts and Spark applications (CONTRACTS 4.1, spec 13).

``/v1/jobs``
    ``POST`` submit (``Idempotency-Key`` header or ``idempotency_key`` in the body), ``GET`` list (state/workspace/kind/item/origin/queue/since/until/text, keyset
    pagination), ``GET /{id}`` (timeline, attempts, hashes, application id), ``POST /{id}/cancel``, ``POST /{id}/retry``, ``GET /{id}/logs`` (paged by *byte* offset),
    ``GET /{id}/logs/stream`` (SSE, ``Last-Event-ID`` = byte offset), ``GET /{id}/output`` (paged download of the executed notebook / result),
    ``GET /{id}/artifacts`` and ``GET|POST|PUT /{id}/artifacts/{name}`` (the *agent upload* endpoint, see below).

Artifact delivery (agent -> controller, documented in CONTRACTS addenda)
    The runner leaves ``output.ipynb`` and other artifacts in the job's work dir. The NODE agent inlines small files (base64) in the terminal ``job_state`` and the
    controller stores them (:mod:`olcf.compute.artifacts`); larger ones are pulled afterwards with the ``fetch_artifact`` command. A runner may also push a file with
    ``POST /v1/jobs/{job_id}/artifacts/{name}`` using the **worker token of the job's driver node** (or the job-scoped token); the body is the raw file. Only the driver
    worker of the *current attempt* (or the attempt's job token) may upload, names are restricted to ``[A-Za-z0-9._-]{1,128}`` and the size is capped by the job's
    ``limits.max_output_bytes`` (hard cap ``compute.max_artifact_bytes``).

``/v1/spark-applications``
    List/get of applications known from the Spark masters/Livy (one row per application, linked to the job or session). Spark UIs are reachable only through the
    authenticated proxies in :mod:`olcf.compute.spark_ui`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import mimetypes
import os
import tempfile
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Query, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict
from sqlalchemy import select

from olcf.api.deps import Authed, AuthedStream, Ctx, IdempotencyKey, Pagination, StreamGuard
from olcf.compute import artifacts as artifacts_mod
from olcf.compute import schema as C
from olcf.compute.errors import ArtifactTooLarge
from olcf.compute.jobs import TERMINAL
from olcf.core.errors import BadRequest, Forbidden, NotFound
from olcf.core.models import JobSubmission
from olcf.core.timeutil import age_seconds
from olcf.db.util import fetch_one, paginate
from olcf.security.rbac import Principal

log = logging.getLogger("olcf.compute.jobs_api")

jobs_router = APIRouter(tags=["jobs"])
apps_router = APIRouter(tags=["spark"])

_ARTIFACT_NAME = artifacts_mod.ARTIFACT_NAME
HARD_ARTIFACT_CAP = artifacts_mod.HARD_ARTIFACT_CAP
DEFAULT_ARTIFACT_CAP = artifacts_mod.DEFAULT_ARTIFACT_CAP
OUTPUT_NAME = "output.ipynb"
_LATE_UPLOAD_SECONDS = 600.0


class JobOut(BaseModel):
    """The public representation of a job returned by the jobs API."""
    model_config = ConfigDict(extra="allow")
    id: str
    kind: str
    state: str
    workspace_id: str
    queue: str
    attempt: int


def _svc(ctx: Any) -> Any:
    return ctx.service("compute")


# ---------------------------------------------------------------------------------------------------------- submit / read
@jobs_router.post("", status_code=202, response_model=JobOut)
def submit_job(body: JobSubmission, response: Response, principal: Authed, ctx: Ctx, idem: IdempotencyKey = None) -> dict[str, Any]:
    """Validate and queue a job. Replays of the same idempotency key return the original job."""
    if idem and not body.idempotency_key:
        body = body.model_copy(update={"idempotency_key": idem})
    jobs = _svc(ctx).jobs
    rec = jobs.submit(principal, body)
    response.headers["Location"] = f"/v1/jobs/{rec.id}"
    return jobs.public(jobs.get_row(rec.id), detail=False)


@jobs_router.get("")
def list_jobs(principal: Authed, ctx: Ctx, page: Pagination, workspace_id: str | None = None, state: list[str] = Query(default_factory=list), kind: str | None = None,
              item_id: str | None = None, origin: str | None = None, queue: str | None = None, since: str | None = None, until: str | None = None,
              text: str | None = Query(None, max_length=200), parent_run_id: str | None = None) -> dict[str, Any]:
    states = [s.strip() for item in state for s in item.split(",") if s.strip()]
    jobs = _svc(ctx).jobs
    rows, nxt = jobs.list(principal, workspace_id=workspace_id, state=states or None, kind=kind, item_id=item_id, origin=origin, queue=queue, since=since, until=until,
                          text=text, parent_run_id=parent_run_id, limit=page.limit, cursor=page.cursor)
    return {"items": [jobs.public(r) for r in rows], "next_cursor": nxt}


@jobs_router.get("/stats")
def job_stats(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("compute.read")
    jobs = _svc(ctx).jobs
    return {"counts": jobs.counts(), "queues": jobs.queue_stats()}


@jobs_router.get("/{job_id}", response_model=JobOut)
def get_job(job_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    jobs = _svc(ctx).jobs
    return jobs.public(jobs.authorize(principal, job_id, "job.read"), detail=True)


@jobs_router.get("/{job_id}/timeline")
def job_timeline(job_id: str, principal: Authed, ctx: Ctx, limit: int = Query(500, ge=1, le=5000)) -> dict[str, Any]:
    jobs = _svc(ctx).jobs
    jobs.authorize(principal, job_id, "job.read")
    return {"items": jobs.timeline(job_id, limit), "next_cursor": None}


@jobs_router.post("/{job_id}/cancel", response_model=JobOut)
def cancel_job(job_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    jobs = _svc(ctx).jobs
    jobs.cancel(principal, job_id)
    return jobs.public(jobs.get_row(job_id))


@jobs_router.post("/{job_id}/retry", status_code=202, response_model=JobOut)
def retry_job(job_id: str, response: Response, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    jobs = _svc(ctx).jobs
    rec = jobs.retry(principal, job_id)
    response.headers["Location"] = f"/v1/jobs/{rec.id}"
    return jobs.public(jobs.get_row(rec.id))


# ---------------------------------------------------------------------------------------------------------- logs
_STREAMS = "^(stdout|stderr|combined)$"


@jobs_router.get("/{job_id}/logs")
def job_logs(job_id: str, principal: Authed, ctx: Ctx, offset: int = Query(0, ge=0), limit: int = Query(65536, ge=1, le=1024 * 1024),
             stream: str = Query("combined", pattern=_STREAMS), tail: bool = False) -> dict[str, Any]:
    """One page of the log. ``offset``/``next_offset`` are byte offsets; ``tail=true`` returns the last ``limit`` bytes."""
    jobs = _svc(ctx).jobs
    jobs.authorize(principal, job_id, "job.read")
    return jobs.logs_page(job_id, offset, limit, stream, tail)


@jobs_router.get("/{job_id}/logs/stream")
async def stream_job_logs(request: Request, job_id: str, principal: AuthedStream, ctx: Ctx, offset: int | None = Query(None, ge=0),
                          stream: str = Query("combined", pattern=_STREAMS)) -> StreamingResponse:
    """Server-Sent Events: ``event: log`` (``id`` = next byte offset), ``event: state`` on every state change, ``event: end`` when finished and drained."""
    jobs = _svc(ctx).jobs
    await asyncio.to_thread(jobs.authorize, principal, job_id, "job.read")
    start = offset if offset is not None else int(request.headers.get("last-event-id") or 0)
    page_size = 64 * 1024
    guard = StreamGuard(ctx, request, principal)

    async def gen() -> AsyncIterator[str]:
        pos = start
        last_state: str | None = None
        idle = 0
        yield "retry: 2000\n\n"
        while True:
            if await request.is_disconnected():
                return
            fresh = await guard.current()  # the credential and the right to read this job are re-validated while the stream lives
            if fresh is None:
                yield "event: end\ndata: {\"reason\": \"credentials no longer valid\"}\n\n"
                return
            if fresh is not principal:
                try:
                    await asyncio.to_thread(jobs.authorize, fresh, job_id, "job.read")
                except (Forbidden, NotFound):
                    yield "event: end\ndata: {\"reason\": \"access to this job was withdrawn\"}\n\n"
                    return
            row = await asyncio.to_thread(jobs.get_row, job_id)
            if row["state"] != last_state:
                last_state = row["state"]
                yield f"event: state\ndata: {json.dumps({'state': row['state'], 'reason': row['state_reason'], 'attempt': row['attempt']})}\n\n"
            progressed = False
            while True:
                page = await asyncio.to_thread(jobs.logs_page, job_id, pos, page_size, stream)
                if page["data"]:
                    pos = page["next_offset"]
                    progressed = True
                    yield f"id: {pos}\nevent: log\ndata: {json.dumps({'offset': page['offset'], 'next_offset': pos, 'data': page['data']})}\n\n"
                if not page["data"] or page["next_offset"] >= page["size"]:
                    break
            if row["state"] in TERMINAL and not progressed:
                yield f"event: end\ndata: {json.dumps({'state': row['state'], 'next_offset': pos})}\n\n"
                return
            idle = 0 if progressed else idle + 1
            if idle >= 15:
                idle = 0
                yield ": keep-alive\n\n"
            await asyncio.sleep(0.5)

    return StreamingResponse(gen(), media_type="text/event-stream", headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"})


# ---------------------------------------------------------------------------------------------------------- artifacts
def artifact_dir(ctx: Any, job_id: str) -> Path:
    return artifacts_mod.artifact_dir(ctx.settings.path("jobs"), job_id)


def artifact_path(ctx: Any, job_id: str, name: str) -> Path:
    return artifacts_mod.artifact_path(ctx.settings.path("jobs"), job_id, name)


def _may_upload(ctx: Any, principal: Principal, row: dict[str, Any]) -> bool:
    """Only the driver worker of the current attempt, or that attempt's job token, may upload."""
    if row["state"] in TERMINAL and (age_seconds(row.get("finished_at")) or 0.0) > _LATE_UPLOAD_SECONDS:
        return False
    with ctx.db.connect() as conn:
        if principal.kind == "worker" and row.get("driver_worker_id"):
            w = fetch_one(conn, select(C.workers.c.principal_id, C.workers.c.state).where(C.workers.c.id == row["driver_worker_id"]))
            if w and w["principal_id"] == principal.id and w["state"] != "revoked":
                return True
        if principal.token_id and "job" in principal.token_scopes:
            a = fetch_one(conn, select(C.job_attempts.c.id).where(C.job_attempts.c.job_id == row["id"], C.job_attempts.c.attempt == row["attempt"],
                                                                  C.job_attempts.c.token_id == principal.token_id))
            if a:
                return True
    return False


async def _receive(ctx: Any, request: Request, dest: Path, cap: int) -> int:
    """Stream the request body to ``dest`` (atomic rename) enforcing ``cap`` without ever holding the file in memory."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=dest.parent, prefix=".upload-")
    size = 0
    try:
        with os.fdopen(fd, "wb") as f:
            async for chunk in request.stream():
                size += len(chunk)
                if size > cap:
                    raise ArtifactTooLarge(f"artifact exceeds the limit of {cap} bytes")
                await asyncio.to_thread(f.write, chunk)
        os.replace(tmp, dest)
        return size
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


@jobs_router.post("/{job_id}/artifacts/{name}", status_code=201)
@jobs_router.put("/{job_id}/artifacts/{name}", status_code=201, include_in_schema=False)
async def upload_artifact(request: Request, job_id: str, name: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    jobs = _svc(ctx).jobs
    row = await asyncio.to_thread(jobs.get_row, job_id)
    if not await asyncio.to_thread(_may_upload, ctx, principal, row):
        raise Forbidden("only the driver worker (or the job token) of a live attempt may upload artifacts")
    dest = artifact_path(ctx, job_id, name)
    limits = (row.get("spec") or {}).get("limits") or {}
    cap = min(HARD_ARTIFACT_CAP, int(limits.get("max_output_bytes") or 0) or DEFAULT_ARTIFACT_CAP, int(_svc(ctx).registry.setting("compute.max_artifact_bytes", HARD_ARTIFACT_CAP)))
    cap = max(cap, 1024 * 1024)
    size = await _receive(ctx, request, dest, cap)
    jobs.add_event(job_id, "artifact_uploaded", {"name": name, "bytes": size, "attempt": row["attempt"]})
    return {"name": name, "size": size, "job_id": job_id}


def _artifacts(ctx: Any, row: dict[str, Any]) -> list[dict[str, Any]]:
    d = artifact_dir(ctx, row["id"])
    reported = {a.get("name"): a for a in ((row.get("result") or {}).get("artifacts") or []) if isinstance(a, dict)}
    out = []
    if d.is_dir():
        for p in sorted(d.iterdir()):
            if p.is_file() and not p.name.startswith(".") and _ARTIFACT_NAME.match(p.name):
                st = p.stat()
                out.append({"name": p.name, "size": st.st_size, "media_type": (reported.get(p.name) or {}).get("media_type") or mimetypes.guess_type(p.name)[0] or "application/octet-stream",
                            "uploaded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(st.st_mtime))})
    return out


@jobs_router.get("/{job_id}/artifacts")
def list_artifacts(job_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    jobs = _svc(ctx).jobs
    row = jobs.authorize(principal, job_id, "job.read")
    return {"items": _artifacts(ctx, row), "next_cursor": None}


def _serve_file(path: Path, *, offset: int, limit: int, media_type: str, filename: str | None) -> StreamingResponse:
    size = path.stat().st_size
    if offset > size:
        raise BadRequest("offset is beyond the end of the file", details={"size": size})
    end = min(size, offset + limit)

    def body():  # noqa: ANN202
        with path.open("rb") as f:
            f.seek(offset)
            left = end - offset
            while left > 0:
                chunk = f.read(min(256 * 1024, left))
                if not chunk:
                    break
                left -= len(chunk)
                yield chunk

    headers = {"X-OLCF-Size": str(size), "X-OLCF-Offset": str(offset), "X-OLCF-Next-Offset": str(end) if end < size else "", "Content-Length": str(end - offset),
               "Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"}
    if filename:
        headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    return StreamingResponse(body(), media_type=media_type, headers=headers)


@jobs_router.get("/{job_id}/artifacts/{name}")
def download_artifact(job_id: str, name: str, principal: Authed, ctx: Ctx, offset: int = Query(0, ge=0), limit: int = Query(64 * 1024 * 1024, ge=1, le=1024 * 1024 * 1024)) -> StreamingResponse:
    jobs = _svc(ctx).jobs
    jobs.authorize(principal, job_id, "job.read")
    p = artifact_path(ctx, job_id, name)
    if not p.is_file():
        raise NotFound(f"job {job_id} has no artifact '{name}'")
    return _serve_file(p, offset=offset, limit=limit, media_type="application/octet-stream", filename=name)


@jobs_router.get("/{job_id}/output")
def job_output(job_id: str, principal: Authed, ctx: Ctx, offset: int = Query(0, ge=0), limit: int = Query(8 * 1024 * 1024, ge=1, le=256 * 1024 * 1024),
               name: str = OUTPUT_NAME) -> StreamingResponse:
    """The executed notebook (``output.ipynb``) or the named artifact, paged by byte offset (``X-OLCF-Next-Offset`` is empty on the last page).

    Without an uploaded artifact, a finished job answers with its ``result.json`` essentials (``exit_value``, ``result``, ``failure``) so API clients always get the outcome.
    """
    jobs = _svc(ctx).jobs
    row = jobs.authorize(principal, job_id, "job.read")
    p = artifact_path(ctx, job_id, name)
    if p.is_file():
        return _serve_file(p, offset=offset, limit=limit, media_type="application/x-ipynb+json" if name.endswith(".ipynb") else (mimetypes.guess_type(name)[0] or "application/octet-stream"),
                           filename=None)
    if row["state"] not in TERMINAL:
        raise NotFound("the job has not produced output yet", details={"state": row["state"]})
    doc = json.dumps({"job_id": job_id, "state": row["state"], "exit_value": row["exit_value"], "result": row["result"], "failure": row["failure"]}, default=str, indent=2).encode()
    scratch = artifact_dir(ctx, job_id).parent / "result-summary.json"
    scratch.parent.mkdir(parents=True, exist_ok=True)
    if not scratch.exists() or scratch.stat().st_size != len(doc):
        scratch.write_bytes(doc)
    return _serve_file(scratch, offset=offset, limit=limit, media_type="application/json", filename=None)


# ---------------------------------------------------------------------------------------------------------- spark applications
def _app_out(a: dict[str, Any]) -> dict[str, Any]:
    out = {k: a[k] for k in ("id", "application_id", "cluster_id", "workspace_id", "job_id", "session_id", "name", "state", "cores", "memory_per_executor_mb", "executors",
                             "driver_worker_id", "started_at", "finished_at", "last_seen_at", "created_at")}
    out["ui"] = f"/v1/spark-applications/{a['id']}/ui/" if a.get("ui_url") or a.get("cluster_id") else None
    return out


def authorize_app(principal: Principal, app: dict[str, Any]) -> None:
    if app.get("workspace_id"):
        principal.require("job.read", workspace_id=app["workspace_id"])
    else:  # an application of an externally managed cluster that OLCF cannot attribute to a workspace
        principal.require("compute.read")


def get_app_row(ctx: Any, app_id: str) -> dict[str, Any]:
    with ctx.db.connect() as conn:
        row = fetch_one(conn, select(C.spark_apps).where((C.spark_apps.c.id == app_id) | (C.spark_apps.c.application_id == app_id)).limit(1))
    if not row:
        raise NotFound(f"spark application {app_id} not found")
    return row


@apps_router.get("")
def list_spark_applications(principal: Authed, ctx: Ctx, page: Pagination, workspace_id: str | None = None, state: str | None = None, cluster_id: str | None = None,
                            job_id: str | None = None) -> dict[str, Any]:
    t = C.spark_apps.c
    stmt = select(C.spark_apps)
    if workspace_id:
        principal.require("job.read", workspace_id=workspace_id)
        stmt = stmt.where(t.workspace_id == workspace_id)
    else:
        vis = principal.visible_workspaces("job.read")
        if vis is not None:
            if not vis:
                return {"items": [], "next_cursor": None}
            stmt = stmt.where(t.workspace_id.in_(sorted(vis)))
    for col, val in ((t.state, state), (t.cluster_id, cluster_id), (t.job_id, job_id)):
        if val:
            stmt = stmt.where(col == val)
    with ctx.db.connect() as conn:
        rows, nxt = paginate(conn, stmt, C.spark_apps, limit=page.limit, cursor=page.cursor, descending=True)
    return {"items": [_app_out(r) for r in rows], "next_cursor": nxt}


@apps_router.get("/{app_id}")
def get_spark_application(app_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    row = get_app_row(ctx, app_id)
    authorize_app(principal, row)
    out = _app_out(row)
    if row.get("job_id"):
        with ctx.db.connect() as conn:
            j = fetch_one(conn, select(C.jobs.c.id, C.jobs.c.name, C.jobs.c.state).where(C.jobs.c.id == row["job_id"]))
        out["job"] = j
    return out


__all__ = ["apps_router", "artifact_dir", "artifact_path", "authorize_app", "get_app_row", "jobs_router"]
