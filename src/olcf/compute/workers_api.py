# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""REST API for the fleet: workers, worker pools, queues, providers and Spark clusters (CONTRACTS 4.2/4.6, spec 13/14).

* Workers: enrollment tokens, enrollment (agent-facing), list/get/patch/delete, ``cordon | uncordon | drain | revoke``, labels, pool assignment.
* Pools / queues / providers: CRUD with ``If-Match`` ETags. Providers hold only *non-secret* configuration; credentials are referenced by ``secret_id``.
* Clusters: standalone Spark clusters can be inspected, started, refreshed and stopped by administrators.

Every handler authorises server-side (``compute.read`` / ``compute.manage``), writes an audit record for each mutation and publishes events
(``worker.cordoned``, ``worker.draining``, ``worker.drained``, ``worker.revoked`` ...). Revoking a worker is *complete*: its tokens are revoked,
its certificate serial enters the CRL, its principal is deactivated, its state becomes ``revoked`` and its channel is closed with code 4403.
"""

from __future__ import annotations

import logging
import re
import threading
from typing import Any

from fastapi import APIRouter, Query, Request, Response
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select, update

from olcf.api.deps import Authed, Ctx, IdempotencyKey, IfMatch, Pagination, check_if_match, etag_header
from olcf.compute import schema as C
from olcf.compute.hub import EnrollmentService, EnrollmentTokenCreate, EnrollRequest, ws_base
from olcf.compute.registry import (
    ACTIVE_JOB_STATES,
    ACTIVE_SESSION_STATES,
    ComputeRegistry,
    public_worker,
    worker_allocation_summary,
)
from olcf.core.context import AppContext
from olcf.core.errors import BadRequest, Conflict, NotFound, OlcfError
from olcf.core.timeutil import now_iso
from olcf.db import schema as S
from olcf.db.util import fetch_all, new_etag, paginate
from olcf.security.rbac import Principal

log = logging.getLogger("olcf.compute.workers_api")
_LABEL_KEY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,62}$")
_SECRETISH = re.compile(r"(?i)(password|passwd|secret(?!_id|_ref)|token(?!_url|_endpoint)|api[_-]?key|private[_-]?key|credential)")


# =========================================================================== models
class _Out(BaseModel):
    model_config = ConfigDict(extra="allow")


class WorkerOut(_Out):
    """The public representation of a worker: identity, connection and scheduling state, pool, labels, inventory and agent version."""
    id: str
    name: str
    state: str
    scheduling: str
    pool: str
    labels: dict[str, str] = Field(default_factory=dict)
    etag: str


class RenewBody(BaseModel):
    """Request body of ``POST /v1/workers/{id}/renew``: which credentials the node should renew now (default both)."""
    certificate: bool = True
    token: bool = True


class PoolOut(_Out):
    """The public representation of a worker pool, including whether it is reserved and for which workspaces."""
    id: str
    name: str
    reserved: bool
    workspace_ids: list[str]
    etag: str


class QueueOut(_Out):
    """The public representation of a job queue and its limits."""
    id: str
    name: str
    etag: str


class ProviderOut(_Out):
    """A compute provider. ``id`` .. ``etag`` and the capability fields are visible to every ``runtime.read`` user; the rest (``config``, ``secret_id``, ``last_discovery``,
    timestamps) only to ``compute.read`` holders - for everybody else those keys are absent."""

    id: str
    name: str
    type: str
    enabled: bool
    is_default: bool
    etag: str
    live_verified: bool | None = None
    execution: str | None = None
    capabilities: list[Any] = Field(default_factory=list)
    builtin: bool = False
    config: dict[str, Any] | None = Field(None, description="provider configuration (never contains secrets); operators only")
    secret_id: str | None = Field(None, description="id of the stored credential (the value is never returned); operators only")
    last_discovery: dict[str, Any] | None = Field(None, description="result of the last discovery; operators only")
    created_at: str | None = None
    updated_at: str | None = None


class WorkerPatch(BaseModel):
    """Request body that changes a worker's labels, pool or name."""
    labels: dict[str, str] | None = None
    pool: str | None = None
    name: str | None = Field(None, pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


class ReasonBody(BaseModel):
    """Optional free-text reason recorded with an operator action such as cordon or drain."""
    reason: str = Field("", max_length=500)


class PoolBody(BaseModel):
    """Request body that creates or changes a worker pool: description, reserved flag, the workspaces a reserved pool admits, and labels."""
    name: str | None = None
    description: str | None = None
    reserved: bool | None = None
    workspace_ids: list[str] | None = None
    labels: dict[str, str] | None = None


class QueueBody(BaseModel):
    """Request body that creates or changes a queue: concurrency, core and memory limits, queue length and wait limits, per-workspace limits and fair-share weights.
    """
    name: str | None = None
    description: str | None = None
    max_concurrency: int | None = Field(None, ge=1)
    max_cores: int | None = Field(None, ge=1)
    max_memory_mb: int | None = Field(None, ge=256)
    max_queued: int | None = Field(None, ge=1)
    max_queue_wait_seconds: int | None = Field(None, ge=1)
    workspace_limits: dict[str, dict[str, int]] | None = None
    fair_share_weights: dict[str, float] | None = None
    allowed_pools: list[str] | None = None
    default_priority: int | None = Field(None, ge=0, le=100)
    paused: bool | None = None


class ProviderBody(BaseModel):
    """Request body that creates or changes a compute provider instance: its type, configuration, secret reference and whether it is enabled or the default.
    """
    name: str | None = Field(None, pattern=r"^[a-z0-9][a-z0-9-]{0,62}$")
    type: str | None = None
    config: dict[str, Any] | None = None
    secret_id: str | None = None
    enabled: bool | None = None
    is_default: bool | None = None


class ClusterStart(BaseModel):
    """Request body that starts a Spark cluster for a runtime in a pool on a provider."""
    runtime_id: str
    pool: str = "default"
    provider: str = "standalone"


# =========================================================================== business logic
def node_allocations(ctx: AppContext) -> tuple[dict[str, tuple[int, int]], dict[str, set[int]]]:
    with ctx.db.connect() as conn:
        jobs = fetch_all(conn, select(C.jobs.c.driver_worker_id, C.jobs.c.resources_granted).where(C.jobs.c.state.in_(ACTIVE_JOB_STATES)))
        sess = fetch_all(conn, select(C.sessions.c.worker_id, C.sessions.c.resources).where(C.sessions.c.state.in_(ACTIVE_SESSION_STATES)))
        clusters = fetch_all(conn, select(C.clusters.c.spark_workers).where(C.clusters.c.state.in_(("up", "degraded", "starting"))))
    return worker_allocation_summary(jobs, sess, clusters)


class WorkerAdmin:
    """Mutations on workers with audit + events. Registered on the compute service as ``admin``."""

    def __init__(self, ctx: AppContext, registry: ComputeRegistry, hub: Any, providers: Any) -> None:
        self.ctx, self.registry, self.hub, self.providers = ctx, registry, hub, providers
        self._guard = threading.Lock()

    # ---- helpers
    def _audit(self, principal: Principal, action: str, worker: dict[str, Any], **details: Any) -> None:
        self.ctx.audit.record(principal, action, resource_type="worker", resource_id=worker["id"], details={"name": worker["name"], **details})

    def busy(self, worker_id: str) -> dict[str, int]:
        """What still runs on the node: jobs, kernels and Spark executors of running applications."""
        with self.ctx.db.connect() as conn:
            jobs = len(conn.execute(select(C.jobs.c.id).where(C.jobs.c.driver_worker_id == worker_id, C.jobs.c.state.in_(ACTIVE_JOB_STATES))).all())
            sess = len(conn.execute(select(C.sessions.c.id).where(C.sessions.c.worker_id == worker_id, C.sessions.c.state.in_(ACTIVE_SESSION_STATES))).all())
            apps = fetch_all(conn, select(C.spark_apps.c.executors).where(C.spark_apps.c.state.in_(("RUNNING", "WAITING"))))
        executors = sum(1 for a in apps for e in (a["executors"] or []) if isinstance(e, dict) and e.get("worker") == worker_id and int(e.get("cores") or 0) > 0)
        return {"jobs": jobs, "sessions": sess, "executors": executors}

    def get(self, ref: str) -> dict[str, Any]:
        return self.registry.find_worker(ref)

    # ---- scheduling state
    def set_scheduling(self, principal: Principal, ref: str, new: str, reason: str = "") -> dict[str, Any]:
        principal.require("compute.manage")
        w = self.get(ref)
        if w["state"] == "revoked":
            raise Conflict("the worker is revoked")
        notes = dict(w.get("notes") or {})
        notes.pop("drained_at", None)
        if new in ("draining", "cordoned"):
            notes["scheduling_reason"] = reason
        else:
            notes.pop("scheduling_reason", None)
        updated = self.registry.update_worker(w["id"], scheduling=new, notes=notes)
        if w["connected"] and (new == "draining" or w["scheduling"] == "draining"):
            try:
                self.hub.request(w["id"], "drain", {"on": new == "draining"}, timeout=10.0)
            except OlcfError as exc:  # an agent that misses it learns the state on reconnect (hello reconcile reads `scheduling` from the DB)
                log.info("drain notification to %s failed: %s", w["name"], exc.message)
        event = {"cordoned": "worker.cordoned", "draining": "worker.draining", "active": "worker.uncordoned"}[new]
        self.ctx.events.publish(event, {"worker_id": w["id"], "name": w["name"], "reason": reason, "busy": self.busy(w["id"])}, subject=w["id"])
        verb = {"cordoned": "cordon", "draining": "drain", "active": "uncordon"}[new]
        self._audit(principal, f"worker.{verb}", w, reason=reason)
        if new == "active":
            self.ctx.maybe_service("compute").scheduler.wake()  # capacity returned
        return updated

    def drain_tick(self) -> None:
        """Leader hook: a draining node with no remaining work is *drained* (event once) and its Spark workers are released."""
        for w in self.registry.list_workers(include_revoked=False):
            if w["scheduling"] != "draining" or (w.get("notes") or {}).get("drained_at"):
                continue
            if any(self.busy(w["id"]).values()):
                continue
            notes = dict(w.get("notes") or {})
            notes["drained_at"] = now_iso()
            self.registry.update_worker(w["id"], notes=notes)
            for p in self.providers.all():
                rel = getattr(p, "release_node", None)
                if callable(rel):
                    try:
                        rel(w["id"])
                    except Exception:  # noqa: BLE001
                        log.exception("release_node failed for %s", w["name"])
            self.ctx.events.publish("worker.drained", {"worker_id": w["id"], "name": w["name"]}, subject=w["id"])

    # ---- labels / pool / name
    def patch(self, principal: Principal, ref: str, body: WorkerPatch, if_match: str | None) -> dict[str, Any]:
        principal.require("compute.manage")
        w = self.get(ref)
        check_if_match(if_match, w["etag"])
        vals: dict[str, Any] = {}
        if body.labels is not None:
            vals["labels"] = self.validate_labels(body.labels)
        if body.pool is not None:
            if not self.registry.pool_exists(body.pool):
                raise BadRequest(f"unknown worker pool '{body.pool}'")
            vals["pool"] = body.pool
        if body.name is not None and body.name != w["name"]:
            vals["name"] = body.name
        if not vals:
            return w
        try:
            new = self.registry.update_worker(w["id"], **vals)
        except Exception as exc:  # noqa: BLE001
            if "UNIQUE" in str(exc).upper() or "duplicate" in str(exc).lower():
                raise Conflict(f"a worker named '{body.name}' already exists") from exc
            raise
        self._audit(principal, "worker.updated", w, changes={k: v for k, v in vals.items()})
        if "pool" in vals:
            self.ctx.maybe_service("compute").scheduler.wake()
        return new

    @staticmethod
    def validate_labels(labels: dict[str, str]) -> dict[str, str]:
        out = {}
        for k, v in labels.items():
            if not _LABEL_KEY.match(k):
                raise BadRequest(f"invalid label key '{k}'")
            if len(str(v)) > 63:
                raise BadRequest(f"label '{k}' value is longer than 63 characters")
            out[k] = str(v)
        return out

    # ---- revoke / delete
    def revoke(self, principal: Principal, ref: str, reason: str = "") -> dict[str, Any]:
        principal.require("compute.manage")
        w = self.get(ref)
        if w["state"] == "revoked":
            return w
        pid = w.get("principal_id")
        revoked_tokens = self.ctx.tokens.revoke_all_for(pid, kinds=("worker",)) if pid else 0
        if w.get("cert_serial"):
            self.hub.revoke_serial(w["cert_serial"])
        pending = ((w.get("credentials") or {}).get("rotation") or {}).get("prev_serial")
        if pending:
            self.hub.revoke_serial(pending)  # a superseded certificate whose rotation had not closed yet
        if pid:
            with self.ctx.db.begin() as conn:
                conn.execute(update(S.principals).where(S.principals.c.id == pid).values(is_active=False))
            self.ctx.authz.invalidate(pid)
        with self.ctx.db.begin() as conn:
            conn.execute(update(C.workers).where(C.workers.c.id == w["id"]).values(state="revoked", revoked_at=now_iso(), connected=False, scheduling="cordoned",
                                                                                  updated_at=now_iso(), etag=new_etag()))
        self.hub.disconnect(w["id"], 4403, "worker revoked")
        self.ctx.events.publish("worker.revoked", {"worker_id": w["id"], "name": w["name"], "reason": reason}, subject=w["id"])
        self._audit(principal, "worker.revoked", w, reason=reason, tokens_revoked=revoked_tokens, certificate_revoked=bool(w.get("cert_serial")))
        self.ctx.maybe_service("compute").scheduler.wake()
        return self.registry.get_worker(w["id"])

    def delete(self, principal: Principal, ref: str) -> None:
        principal.require("compute.manage")
        w = self.get(ref)
        if w["state"] not in ("revoked", "pending", "offline"):
            raise Conflict("only revoked, never-connected or offline workers can be removed", remediation="drain and revoke the worker first")
        if any(self.busy(w["id"]).values()):
            raise Conflict("the worker still has jobs or sessions", details=self.busy(w["id"]))
        if w["state"] != "revoked":
            self.revoke(principal, w["id"], "removed")
        with self.ctx.db.begin() as conn:
            conn.execute(C.runtime_installs.delete().where(C.runtime_installs.c.worker_id == w["id"]))
            conn.execute(C.workers.delete().where(C.workers.c.id == w["id"]))
        self._audit(principal, "worker.deleted", w)


# =========================================================================== routers
workers_router = APIRouter(tags=["workers"])
pools_router = APIRouter(tags=["workers"])
queues_router = APIRouter(tags=["queues"])
providers_router = APIRouter(tags=["providers"])
clusters_router = APIRouter(tags=["clusters"])


def _svc(ctx: Any) -> Any:
    return ctx.service("compute")


def _worker_out(ctx: AppContext, row: dict[str, Any], alloc: dict[str, tuple[int, int]] | None = None) -> dict[str, Any]:
    out = public_worker(row)
    out["credentials"] = _svc(ctx).credentials.describe(row)
    a = (alloc if alloc is not None else node_allocations(ctx)[0]).get(row["id"], (0, 0))
    out["resources"].update({"cores_allocated": a[0], "memory_allocated_mb": a[1]})
    return out


# ---- enrollment -------------------------------------------------------------------------------------------------
@workers_router.post("/enrollment-tokens", status_code=201)
def create_enrollment_token(body: EnrollmentTokenCreate, request: Request, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("compute.manage")
    base = ctx.settings.server.public_url or f"{request.url.scheme}://{request.headers.get('host', 'localhost')}"
    return EnrollmentService(ctx, _svc(ctx).registry).create_token(principal, body, base_url=base.rstrip("/"))


@workers_router.post("/enroll", status_code=201)
def enroll(body: EnrollRequest, request: Request, ctx: Ctx) -> dict[str, Any]:
    """Agent-facing, unauthenticated: the single-use enrollment token in the body is the credential (rate limited)."""
    ip = request.client.host if request.client else None
    return EnrollmentService(ctx, _svc(ctx).registry).enroll(body, ip=ip, channel_base=ws_base(ctx, request).rstrip("/"))


# ---- workers ------------------------------------------------------------------------------------------------------
@workers_router.get("")
def list_workers(principal: Authed, ctx: Ctx, page: Pagination, state: str | None = None, pool: str | None = None, scheduling: str | None = None,
                 label: list[str] = Query(default_factory=list, description="key=value (repeatable)"), q: str | None = None) -> dict[str, Any]:
    principal.require("compute.read")
    wants = {}
    for item in label:
        k, _, v = item.partition("=")
        wants[k] = v
    stmt = select(C.workers)
    for col, val in ((C.workers.c.state, state), (C.workers.c.pool, pool), (C.workers.c.scheduling, scheduling)):
        if val:
            stmt = stmt.where(col == val)
    if q:
        stmt = stmt.where(C.workers.c.name.ilike(f"%{q.replace('%', '').replace('_', '')}%"))
    with ctx.db.connect() as conn:
        rows, nxt = paginate(conn, stmt, C.workers, limit=page.limit, cursor=page.cursor)
    if wants:
        rows = [r for r in rows if all(str((r.get("labels") or {}).get(k)) == v if v else k in (r.get("labels") or {}) for k, v in wants.items())]
    alloc = node_allocations(ctx)[0]
    return {"items": [_worker_out(ctx, r, alloc) for r in rows], "next_cursor": nxt}


@workers_router.get("/{worker_id}", response_model=WorkerOut)
def get_worker(worker_id: str, response: Response, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("compute.read")
    row = _svc(ctx).registry.find_worker(worker_id)
    response.headers["ETag"] = etag_header(row["etag"])
    out = _worker_out(ctx, row)
    out["busy"] = _svc(ctx).admin.busy(row["id"])
    out["history"] = row.get("history") or {}  # the last <=120 heartbeat samples: cpu, mem (percent) and ts
    return out


@workers_router.patch("/{worker_id}", response_model=WorkerOut)
def patch_worker(worker_id: str, body: WorkerPatch, response: Response, principal: Authed, ctx: Ctx, if_match: IfMatch = None) -> dict[str, Any]:
    row = _svc(ctx).admin.patch(principal, worker_id, body, if_match)
    response.headers["ETag"] = etag_header(row["etag"])
    return _worker_out(ctx, row)


@workers_router.put("/{worker_id}/labels", response_model=WorkerOut)
def put_labels(worker_id: str, body: dict[str, str], principal: Authed, ctx: Ctx, if_match: IfMatch = None) -> dict[str, Any]:
    return _worker_out(ctx, _svc(ctx).admin.patch(principal, worker_id, WorkerPatch(labels=body), if_match))


class PoolAssign(BaseModel):
    """Request body of ``PUT /v1/workers/{id}/pool``: the name of the pool the worker joins."""
    pool: str | None = Field(None, description="name of the pool the worker joins")


@workers_router.put("/{worker_id}/pool", response_model=WorkerOut)
def put_pool(worker_id: str, body: PoolAssign, principal: Authed, ctx: Ctx, if_match: IfMatch = None) -> dict[str, Any]:
    if "pool" not in body.model_fields_set:
        raise BadRequest("body must be {'pool': '<name>'}")
    return _worker_out(ctx, _svc(ctx).admin.patch(principal, worker_id, WorkerPatch(pool=body.pool), if_match))


def _sched(action: str, new: str):  # noqa: ANN202
    def handler(worker_id: str, principal: Authed, ctx: Ctx, body: ReasonBody | None = None) -> dict[str, Any]:
        return _worker_out(ctx, _svc(ctx).admin.set_scheduling(principal, worker_id, new, (body.reason if body else "")))
    handler.__name__ = f"worker_{action}"
    return handler


for _a, _n in (("cordon", "cordoned"), ("drain", "draining"), ("uncordon", "active")):
    workers_router.add_api_route(f"/{{worker_id}}/{_a}", _sched(_a, _n), methods=["POST"], response_model=WorkerOut, name=f"worker_{_a}")


@workers_router.post("/{worker_id}/revoke", response_model=WorkerOut)
def revoke_worker(worker_id: str, principal: Authed, ctx: Ctx, body: ReasonBody | None = None) -> dict[str, Any]:
    return _worker_out(ctx, _svc(ctx).admin.revoke(principal, worker_id, body.reason if body else ""))


@workers_router.post("/{worker_id}/renew")
def renew_worker(worker_id: str, principal: Authed, ctx: Ctx, body: RenewBody | None = None) -> dict[str, Any]:
    """Make the node renew its machine certificate and/or worker token now (it must be connected). The exchange itself runs over the node's authenticated channel."""
    body = body or RenewBody()
    res = _svc(ctx).credentials.request_renewal(principal, worker_id, certificate=body.certificate, token=body.token)
    return {"renewed": res["renewed"], "worker": _worker_out(ctx, _svc(ctx).registry.get_worker(res["worker_id"]))}


@workers_router.delete("/{worker_id}", status_code=204)
def delete_worker(worker_id: str, principal: Authed, ctx: Ctx) -> None:
    _svc(ctx).admin.delete(principal, worker_id)


# ---- pools ----------------------------------------------------------------------------------------------------------
@pools_router.get("")
def list_pools(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("compute.read")
    return {"items": _svc(ctx).registry.list_pools(), "next_cursor": None}


@pools_router.post("", status_code=201, response_model=PoolOut)
def create_pool(body: PoolBody, response: Response, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("compute.manage")
    if not body.name:
        raise BadRequest("'name' is required")
    row = _svc(ctx).registry.create_pool(body.name, description=body.description or "", reserved=bool(body.reserved), workspace_ids=body.workspace_ids, labels=body.labels)
    ctx.audit.record(principal, "pool.created", resource_type="worker_pool", resource_id=row["id"], details={"name": row["name"], "reserved": row["reserved"]})
    response.headers["ETag"] = etag_header(row["etag"])
    return row


@pools_router.get("/{pool}", response_model=PoolOut)
def get_pool(pool: str, response: Response, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("compute.read")
    row = _svc(ctx).registry.get_pool(pool)
    response.headers["ETag"] = etag_header(row["etag"])
    return row


@pools_router.patch("/{pool}", response_model=PoolOut)
def patch_pool(pool: str, body: PoolBody, response: Response, principal: Authed, ctx: Ctx, if_match: IfMatch = None) -> dict[str, Any]:
    principal.require("compute.manage")
    reg = _svc(ctx).registry
    cur = reg.get_pool(pool)
    check_if_match(if_match, cur["etag"])
    row = reg.update_pool(pool, body.model_dump(exclude_unset=True, exclude={"name"}))
    ctx.audit.record(principal, "pool.updated", resource_type="worker_pool", resource_id=row["id"], details=body.model_dump(exclude_unset=True))
    _svc(ctx).scheduler.wake()
    response.headers["ETag"] = etag_header(row["etag"])
    return row


@pools_router.delete("/{pool}", status_code=204)
def delete_pool(pool: str, principal: Authed, ctx: Ctx) -> None:
    principal.require("compute.manage")
    row = _svc(ctx).registry.delete_pool(pool)
    ctx.audit.record(principal, "pool.deleted", resource_type="worker_pool", resource_id=row["id"], details={"name": row["name"]})


# ---- queues -------------------------------------------------------------------------------------------------------
def _queue_out(ctx: AppContext, row: dict[str, Any], stats: dict[str, dict[str, Any]]) -> dict[str, Any]:
    s = stats.get(row["name"]) or {}
    oldest = s.get("oldest_queued_at")
    wait = None
    if oldest:
        from olcf.core.timeutil import age_seconds

        wait = round(age_seconds(oldest) or 0, 1)
    return {**row, "depth": s.get("depth", 0), "running": s.get("running", 0), "oldest_wait_seconds": wait or 0}


@queues_router.get("")
def list_queues(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("compute.read")
    svc = _svc(ctx)
    stats = svc.jobs.queue_stats()
    return {"items": [_queue_out(ctx, q, stats) for q in svc.registry.list_queues()], "next_cursor": None}


@queues_router.post("", status_code=201, response_model=QueueOut)
def create_queue(body: QueueBody, response: Response, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("compute.manage")
    if not body.name:
        raise BadRequest("'name' is required")
    row = _svc(ctx).registry.create_queue(body.name, body.model_dump(exclude_unset=True, exclude={"name"}, exclude_none=False))
    ctx.audit.record(principal, "queue.created", resource_type="queue", resource_id=row["id"], details={"name": row["name"]})
    response.headers["ETag"] = etag_header(row["etag"])
    return row


@queues_router.get("/{queue}", response_model=QueueOut)
def get_queue(queue: str, response: Response, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("compute.read")
    svc = _svc(ctx)
    row = svc.registry.get_queue(queue)
    response.headers["ETag"] = etag_header(row["etag"])
    return _queue_out(ctx, row, svc.jobs.queue_stats())


@queues_router.patch("/{queue}", response_model=QueueOut)
def patch_queue(queue: str, body: QueueBody, response: Response, principal: Authed, ctx: Ctx, if_match: IfMatch = None) -> dict[str, Any]:
    principal.require("compute.manage")
    svc = _svc(ctx)
    cur = svc.registry.get_queue(queue)
    check_if_match(if_match, cur["etag"])
    row = svc.registry.update_queue(queue, body.model_dump(exclude={"name"}), fields_set=set(body.model_fields_set) - {"name"})
    ctx.audit.record(principal, "queue.updated", resource_type="queue", resource_id=row["id"], details=body.model_dump(exclude_unset=True))
    svc.scheduler.wake()
    response.headers["ETag"] = etag_header(row["etag"])
    return row


@queues_router.post("/{queue}/pause", response_model=QueueOut)
@queues_router.post("/{queue}/resume", response_model=QueueOut, include_in_schema=False)
def pause_queue(queue: str, request: Request, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("compute.manage")
    paused = request.url.path.endswith("/pause")
    svc = _svc(ctx)
    row = svc.registry.update_queue(queue, {"paused": paused}, fields_set={"paused"})
    ctx.audit.record(principal, "queue.paused" if paused else "queue.resumed", resource_type="queue", resource_id=row["id"], details={"name": row["name"]})
    svc.scheduler.wake()
    return row


@queues_router.delete("/{queue}", status_code=204)
def delete_queue(queue: str, principal: Authed, ctx: Ctx) -> None:
    principal.require("compute.manage")
    svc = _svc(ctx)
    q = svc.registry.get_queue(queue)
    with ctx.db.connect() as conn:
        busy = conn.execute(select(C.jobs.c.id).where(C.jobs.c.queue == q["name"], C.jobs.c.state.in_(("accepted", "validating", "queued", "provisioning", "running", "cancel_requested"))).limit(1)).first()
    if busy:
        raise Conflict(f"queue '{q['name']}' still has active jobs")
    svc.registry.delete_queue(queue)
    ctx.audit.record(principal, "queue.deleted", resource_type="queue", resource_id=q["id"], details={"name": q["name"]})


# ---- providers -----------------------------------------------------------------------------------------------------
def _provider_out(ctx: AppContext, row: dict[str, Any], types: dict[str, dict[str, Any]]) -> dict[str, Any]:
    t = types.get(row["type"]) or {}
    return {**{k: row[k] for k in ("id", "name", "type", "config", "secret_id", "enabled", "is_default", "last_discovery", "etag", "created_at", "updated_at")},
            "live_verified": t.get("live_verified"), "execution": t.get("execution"), "capabilities": t.get("capabilities", []), "builtin": row["name"] in ("standalone", "local")}


def _provider_view(ctx: AppContext, principal: Principal, row: dict[str, Any], types: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Full record for operators (``compute.read``); everybody else with ``runtime.read`` (any authenticated user with a role) gets the public part only:
    enough to choose a provider in a job form, without configuration, secret reference or discovery details."""
    out = _provider_out(ctx, row, types)
    if principal.can("compute.read"):
        return out
    return {k: out[k] for k in ("id", "name", "type", "enabled", "is_default", "etag", "live_verified", "execution", "capabilities", "builtin")}


def _check_config(cfg: dict[str, Any]) -> None:
    def walk(o: Any, path: str) -> None:
        if isinstance(o, dict):
            for k, v in o.items():
                if _SECRETISH.search(str(k)):
                    raise BadRequest(f"provider configuration must not contain secrets (key '{path}{k}')", remediation="store the credential with POST /v1/secrets and pass its id as 'secret_id'")
                walk(v, f"{path}{k}.")
        elif isinstance(o, list):
            for i in o:
                walk(i, path)
    walk(cfg, "")


@providers_router.get("/types")
def provider_types(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("runtime.read")
    return {"items": _svc(ctx).providers.types(), "next_cursor": None}


@providers_router.get("")
def list_providers(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("runtime.read")
    svc = _svc(ctx)
    types = {t["type"]: t for t in svc.providers.types()}
    return {"items": [_provider_view(ctx, principal, r, types) for r in svc.registry.list_providers()], "next_cursor": None}


@providers_router.post("", status_code=201, response_model=ProviderOut)
def create_provider(body: ProviderBody, response: Response, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("compute.manage")
    if not body.name or not body.type:
        raise BadRequest("'name' and 'type' are required")
    svc = _svc(ctx)
    cfg = dict(body.config or {})
    _check_config(cfg)
    if body.secret_id:
        ctx.secrets.get_meta(body.secret_id)  # NotFound if absent
    svc.providers.instantiate(body.type, body.name, cfg, body.secret_id)  # validates (BadRequest/Unsupported)
    from olcf.core.ids import new_id

    now = now_iso()
    row = {"id": new_id("prv"), "name": body.name, "type": body.type, "config": cfg, "secret_id": body.secret_id, "enabled": body.enabled is not False, "is_default": False,
           "last_discovery": None, "etag": new_etag(), "created_at": now, "updated_at": now}
    from sqlalchemy.exc import IntegrityError

    try:
        with ctx.db.begin() as conn:
            conn.execute(C.providers.insert().values(**row))
    except IntegrityError:
        raise Conflict(f"a provider named '{body.name}' already exists") from None
    if body.is_default:
        _set_default(ctx, row["id"])
    ctx.audit.record(principal, "provider.created", resource_type="provider", resource_id=row["id"], details={"name": body.name, "type": body.type})
    response.headers["ETag"] = etag_header(row["etag"])
    types = {t["type"]: t for t in svc.providers.types()}
    return _provider_out(ctx, svc.registry.get_provider_row(row["id"]), types)


def _set_default(ctx: AppContext, provider_id: str) -> None:
    with ctx.db.begin() as conn:
        conn.execute(update(C.providers).values(is_default=False))
        conn.execute(update(C.providers).where(C.providers.c.id == provider_id).values(is_default=True, updated_at=now_iso()))


@providers_router.get("/{provider}", response_model=ProviderOut)
def get_provider(provider: str, response: Response, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("runtime.read")
    svc = _svc(ctx)
    row = svc.registry.get_provider_row(provider)
    response.headers["ETag"] = etag_header(row["etag"])
    return _provider_view(ctx, principal, row, {t["type"]: t for t in svc.providers.types()})


@providers_router.patch("/{provider}", response_model=ProviderOut)
def patch_provider(provider: str, body: ProviderBody, response: Response, principal: Authed, ctx: Ctx, if_match: IfMatch = None) -> dict[str, Any]:
    principal.require("compute.manage")
    svc = _svc(ctx)
    cur = svc.registry.get_provider_row(provider)
    check_if_match(if_match, cur["etag"])
    vals: dict[str, Any] = {}
    cfg = cur["config"] if body.config is None else dict(body.config)
    sid = cur["secret_id"] if "secret_id" not in body.model_fields_set else body.secret_id
    if body.config is not None:
        _check_config(cfg)
    if body.config is not None or "secret_id" in body.model_fields_set:
        if sid:
            ctx.secrets.get_meta(sid)
        svc.providers.instantiate(cur["type"], cur["name"], cfg, sid)
        vals.update(config=cfg, secret_id=sid)
    if body.enabled is not None:
        vals["enabled"] = body.enabled
    if vals:
        vals.update(updated_at=now_iso(), etag=new_etag())
        with ctx.db.begin() as conn:
            conn.execute(update(C.providers).where(C.providers.c.id == cur["id"]).values(**vals))
    if body.is_default is True:
        _set_default(ctx, cur["id"])
    elif body.is_default is False and cur["is_default"]:
        with ctx.db.begin() as conn:
            conn.execute(update(C.providers).where(C.providers.c.id == cur["id"]).values(is_default=False, updated_at=now_iso(), etag=new_etag()))
    svc.providers.invalidate(cur["name"])
    ctx.audit.record(principal, "provider.updated", resource_type="provider", resource_id=cur["id"], details={"name": cur["name"], "fields": sorted(body.model_fields_set)})
    svc.scheduler.wake()
    row = svc.registry.get_provider_row(cur["id"])
    response.headers["ETag"] = etag_header(row["etag"])
    return _provider_out(ctx, row, {t["type"]: t for t in svc.providers.types()})


@providers_router.delete("/{provider}", status_code=204)
def delete_provider(provider: str, principal: Authed, ctx: Ctx) -> None:
    principal.require("compute.manage")
    svc = _svc(ctx)
    row = svc.registry.get_provider_row(provider)
    if row["name"] in ("standalone", "local"):
        raise Conflict("built-in providers cannot be deleted (disable them instead)")
    with ctx.db.connect() as conn:
        busy = conn.execute(select(C.jobs.c.id).where(C.jobs.c.provider == row["name"], C.jobs.c.state.in_(("queued", "provisioning", "running", "cancel_requested"))).limit(1)).first()
    if busy:
        raise Conflict(f"provider '{row['name']}' still has active jobs")
    with ctx.db.begin() as conn:
        conn.execute(C.providers.delete().where(C.providers.c.id == row["id"]))
    svc.providers.invalidate(row["name"])
    ctx.audit.record(principal, "provider.deleted", resource_type="provider", resource_id=row["id"], details={"name": row["name"]})


@providers_router.post("/{provider}/discover")
def discover_provider(provider: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Run the provider's own discovery now (capabilities, nodes, runtimes, connectivity) and remember the result."""
    principal.require("compute.manage")
    svc = _svc(ctx)
    row = svc.registry.get_provider_row(provider)
    prov = svc.providers.from_row(row)
    d = prov.discover().to_dict()
    with ctx.db.begin() as conn:
        conn.execute(update(C.providers).where(C.providers.c.id == row["id"]).values(last_discovery={**d, "at": now_iso()}))
    return d


# ---- clusters ----------------------------------------------------------------------------------------------------------
def _cluster_out(c: dict[str, Any]) -> dict[str, Any]:
    ms = c.get("last_status") or {}
    return {"id": c["id"], "provider": c["provider"], "runtime_id": c["runtime_id"], "pool": c["pool"], "family": c["family"], "state": c["state"], "detail": c["detail"],
            "master": {"url": c["master_url"], "worker_id": c["master_worker_id"], "state": ms.get("state"), "alive_workers": ms.get("alive_workers"), "cores_total": ms.get("cores_total"),
                       "cores_used": ms.get("cores_used"), "memory_total_mb": ms.get("memory_total_mb"), "memory_used_mb": ms.get("memory_used_mb")},
            "workers": {wid: {k: w.get(k) for k in ("cores", "memory_mb", "state", "host")} for wid, w in (c.get("spark_workers") or {}).items()},
            "last_status_at": c["last_status_at"], "etag": c["etag"], "created_at": c["created_at"], "updated_at": c["updated_at"]}


@clusters_router.get("")
def list_clusters(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("compute.read")
    with ctx.db.connect() as conn:
        rows = fetch_all(conn, select(C.clusters).order_by(C.clusters.c.created_at))
    return {"items": [_cluster_out(c) for c in rows], "next_cursor": None}


@clusters_router.get("/{cluster_id}")
def get_cluster(cluster_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("compute.read")
    with ctx.db.connect() as conn:
        rows = fetch_all(conn, select(C.clusters).where(C.clusters.c.id == cluster_id))
    if not rows:
        raise NotFound(f"cluster {cluster_id} not found")
    return _cluster_out(rows[0])


@clusters_router.post("", status_code=202)
def start_cluster(body: ClusterStart, principal: Authed, ctx: Ctx, idem: IdempotencyKey = None) -> dict[str, Any]:  # noqa: ARG001
    principal.require("compute.manage")
    svc = _svc(ctx)
    prov = svc.providers.get(body.provider)
    if not hasattr(prov, "ensure_cluster"):
        raise BadRequest(f"provider '{body.provider}' has no managed clusters")
    svc.runtimes.get_row(body.runtime_id)
    ops = ctx.service("operations")
    oid = ops.create("cluster.start", principal.id, f"starting {body.runtime_id} cluster in pool {body.pool}")

    def run() -> None:
        try:
            cl = prov.ensure_cluster(body.runtime_id, body.pool)
            ops.update(oid, state="succeeded", progress=100, message=f"cluster {cl['id']} is {cl['state']}", result={"cluster_id": cl["id"], "state": cl["state"]})
        except OlcfError as exc:
            ops.update(oid, state="failed", progress=100, message=exc.message, error=exc.to_dict())
        except Exception as exc:  # noqa: BLE001
            log.exception("cluster start failed")
            ops.update(oid, state="failed", progress=100, message=str(exc), error={"code": "internal", "message": str(exc)})

    threading.Thread(target=run, name="olcf-cluster-start", daemon=True).start()
    ctx.audit.record(principal, "cluster.start", resource_type="cluster", resource_id=f"{body.runtime_id}/{body.pool}", details=body.model_dump())
    return {"operation_id": oid}


@clusters_router.post("/{cluster_id}/stop")
def stop_cluster(cluster_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("compute.manage")
    svc = _svc(ctx)
    with ctx.db.connect() as conn:
        rows = fetch_all(conn, select(C.clusters).where(C.clusters.c.id == cluster_id))
    if not rows:
        raise NotFound(f"cluster {cluster_id} not found")
    prov = svc.providers.get(rows[0]["provider"])
    if not hasattr(prov, "stop_cluster"):
        raise BadRequest(f"provider '{rows[0]['provider']}' has no managed clusters")
    out = prov.stop_cluster(cluster_id)
    ctx.audit.record(principal, "cluster.stop", resource_type="cluster", resource_id=cluster_id, details={"runtime_id": rows[0]["runtime_id"], "pool": rows[0]["pool"]})
    return _cluster_out(out)


@clusters_router.post("/{cluster_id}/refresh")
def refresh_cluster(cluster_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("compute.read")
    svc = _svc(ctx)
    with ctx.db.connect() as conn:
        rows = fetch_all(conn, select(C.clusters).where(C.clusters.c.id == cluster_id))
    if not rows:
        raise NotFound(f"cluster {cluster_id} not found")
    prov = svc.providers.get(rows[0]["provider"])
    if not hasattr(prov, "refresh_cluster"):
        raise BadRequest(f"provider '{rows[0]['provider']}' has no managed clusters")
    return _cluster_out(prov.refresh_cluster(rows[0]))
