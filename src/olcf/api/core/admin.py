# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Health, secrets, audit, events (SSE), operations, settings and platform info."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import sys
from collections.abc import AsyncIterator
from typing import Any

from fastapi import APIRouter, Query, Request
from fastapi.responses import PlainTextResponse, StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import select, update

from olcf.api.deps import Authed, AuthedStream, Ctx, StreamGuard
from olcf.core.context import AppContext
from olcf.core.errors import BadRequest, NotFound
from olcf.core.health import ComponentHealth, HealthCheck, Status
from olcf.core.ids import new_id
from olcf.core.timeutil import now_iso
from olcf.db import schema as S
from olcf.db.migrate import current_versions
from olcf.db.util import fetch_one
from olcf.version import __version__

router = APIRouter(tags=["admin"])
public_router = APIRouter(tags=["health"])


# ===================================================================== services
class OperationService:
    """Long-running operation tracker (``/v1/operations/{id}``)."""

    def __init__(self, ctx: AppContext) -> None:
        self.ctx = ctx

    def create(self, kind: str, principal_id: str | None = None, message: str = "") -> str:
        oid = new_id("evt").replace("evt_", "ops_")
        now = now_iso()
        with self.ctx.db.begin() as conn:
            conn.execute(S.operations.insert().values(id=oid, kind=kind, state="running", progress=0, message=message, created_by=principal_id, created_at=now, updated_at=now))
        return oid

    def update(self, oid: str, *, state: str | None = None, progress: int | None = None, message: str | None = None, result: Any = None, error: Any = None) -> None:
        vals: dict[str, Any] = {"updated_at": now_iso()}
        for k, v in (("state", state), ("progress", progress), ("message", message), ("result", result), ("error", error)):
            if v is not None:
                vals[k] = v
        with self.ctx.db.begin() as conn:
            conn.execute(update(S.operations).where(S.operations.c.id == oid).values(**vals))

    def get(self, oid: str) -> dict[str, Any]:
        with self.ctx.db.connect() as conn:
            row = fetch_one(conn, select(S.operations).where(S.operations.c.id == oid))
        if not row:
            raise NotFound("operation not found")
        return row


class KVSettings:
    """Small typed settings store backed by the ``kv`` table (``settings.<key>``)."""

    def __init__(self, ctx: AppContext) -> None:
        self.ctx = ctx
        self.registry: dict[str, dict[str, Any]] = {}

    def register(self, key: str, *, description: str, default: Any = None, validator=None, secret_free: bool = True) -> None:
        self.registry[key] = {"description": description, "default": default, "validator": validator}

    def get(self, key: str, default: Any = None) -> Any:
        with self.ctx.db.connect() as conn:
            row = fetch_one(conn, select(S.kv).where(S.kv.c.key == f"settings.{key}"))
        if row:
            return row["value"]
        reg = self.registry.get(key)
        return reg["default"] if reg and default is None else default

    def set(self, key: str, value: Any) -> None:
        reg = self.registry.get(key)
        if reg is None:
            raise BadRequest(f"unknown setting '{key}'", details={"known": sorted(self.registry)})
        if reg["validator"]:
            value = reg["validator"](value)
        k = f"settings.{key}"
        with self.ctx.db.begin() as conn:
            n = conn.execute(update(S.kv).where(S.kv.c.key == k).values(value=value, updated_at=now_iso())).rowcount
            if not n:
                conn.execute(S.kv.insert().values(key=k, value=value, updated_at=now_iso()))

    def all(self) -> dict[str, Any]:
        return {k: {"value": self.get(k), "description": v["description"], "default": v["default"]} for k, v in sorted(self.registry.items())}


# ===================================================================== built-in health checks
def register_health_checks(ctx: AppContext) -> None:
    def db_quick() -> ComponentHealth:
        ms = ctx.db.ping()
        return ComponentHealth("database", Status.OK if ms < 500 else Status.DEGRADED, f"{ctx.db.dialect} reachable ({ms:.1f} ms)", labels={"dialect": ctx.db.dialect})

    def db_deep() -> ComponentHealth:
        probe = f"health.probe.{os.getpid()}"
        with ctx.db.begin() as conn:
            conn.execute(S.kv.delete().where(S.kv.c.key == probe))
            conn.execute(S.kv.insert().values(key=probe, value={"t": now_iso()}, updated_at=now_iso()))
        with ctx.db.begin() as conn:
            got = fetch_one(conn, select(S.kv).where(S.kv.c.key == probe))
            conn.execute(S.kv.delete().where(S.kv.c.key == probe))
        ok = bool(got)
        vers = current_versions(ctx.db)
        return ComponentHealth("database", Status.OK if ok else Status.DOWN, f"write/read/delete round-trip {'ok' if ok else 'FAILED'}; migrations {vers}",
                               steps=[{"step": "write", "ok": True}, {"step": "read-back", "ok": ok}, {"step": "delete", "ok": True}], labels={"migrations": vers})

    ctx.health.register(HealthCheck("database", "control-plane", "Metadata database (SQLite/PostgreSQL)", db_quick, db_deep, ttl=5, critical=True))

    def secrets_quick() -> ComponentHealth:
        ok, detail = ctx.secrets.health()
        return ComponentHealth("secrets", Status.OK if ok else Status.DOWN, detail, labels={"provider": ctx.secrets.default})

    def secrets_deep() -> ComponentHealth:
        name = f"health-probe-{os.getpid()}"
        meta = ctx.secrets.create(name, "probe-value", created_by="usr_system")
        try:
            ok = ctx.secrets.resolve(meta["id"]) == "probe-value"
        finally:
            ctx.secrets.delete(meta["id"])
        return ComponentHealth("secrets", Status.OK if ok else Status.DOWN, f"create/resolve/delete round-trip {'ok' if ok else 'FAILED'}", labels={"provider": ctx.secrets.default})

    ctx.health.register(HealthCheck("secrets", "control-plane", "Secret store and master key", secrets_quick, secrets_deep, ttl=15, critical=True))

    def audit_quick() -> ComponentHealth:
        rows = ctx.audit.query(limit=1)
        return ComponentHealth("audit", Status.OK, f"append-only log; latest {rows[0]['ts'] if rows else 'n/a'}")

    def audit_deep() -> ComponentHealth:
        res = ctx.audit.verify()
        return ComponentHealth("audit", Status.OK if res["ok"] else Status.DOWN, f"hash chain {'verified' if res['ok'] else 'BROKEN'} over {res['checked']} records", labels=res)

    ctx.health.register(HealthCheck("audit", "control-plane", "Tamper-evident audit log", audit_quick, audit_deep, ttl=30))

    def leader_quick() -> ComponentHealth:
        holder = ctx.leader.current_holder()
        mine = ctx.leader.is_leader()
        return ComponentHealth("leader", Status.OK if holder else Status.DEGRADED, f"leader={holder['holder'] if holder else 'none'} term={holder['term'] if holder else '-'}; this node {'IS' if mine else 'is not'} leader",
                               labels={"node": ctx.node_id, "is_leader": mine})

    ctx.health.register(HealthCheck("leader", "control-plane", "Scheduler leader lease (HA)", leader_quick, None, ttl=3))

    def disk_quick() -> ComponentHealth:
        u = shutil.disk_usage(ctx.settings.data_dir)
        free_pct = u.free / u.total * 100
        st = Status.OK if free_pct > 15 else Status.DEGRADED if free_pct > 5 else Status.DOWN
        return ComponentHealth("data-disk", st, f"{u.free / 1e9:.1f} GB free of {u.total / 1e9:.1f} GB ({free_pct:.0f}% free)", labels={"free_bytes": u.free, "total_bytes": u.total})

    ctx.health.register(HealthCheck("data-disk", "control-plane", "Data directory disk space", disk_quick, None, ttl=20))

    def pki_quick() -> ComponentHealth:
        import datetime as dt

        left = ctx.pki.ca_cert.not_valid_after_utc - dt.datetime.now(dt.UTC)
        return ComponentHealth("pki", Status.OK if left.days > 60 else Status.DEGRADED, f"CA {ctx.pki.ca_fingerprint()[:16]}…, {left.days} days remaining")

    ctx.health.register(HealthCheck("pki", "security", "Built-in certificate authority", pki_quick, None, ttl=60))

    def api_quick() -> ComponentHealth:
        return ComponentHealth("api", Status.OK, f"v{__version__} up {ctx.uptime:.0f}s on {ctx.node_id}", labels={"version": __version__, "node": ctx.node_id, "tls": ctx.settings.tls.enabled})

    ctx.health.register(HealthCheck("api", "control-plane", "REST API gateway", api_quick, None, ttl=2, critical=True))

    from olcf.api.core import rotation

    rotation.register_health(ctx)

    from olcf.security import runtime_trust

    runtime_trust.register_health(ctx)


# ===================================================================== routes: health
@public_router.get("/health")
def liveness() -> dict[str, Any]:
    return {"status": "ok", "version": __version__}


@public_router.get("/health/ready")
def readiness(ctx: Ctx) -> Any:
    """Readiness: 503 when the database does not answer or when production trusts the public development runtime-signing key (``runtime-trust`` is ``down``).

    The body of a 503 is the same fixed document in both cases: the probe is unauthenticated, so it says *that* the node is not ready, never *why* (the reason is in
    ``GET /v1/admin/health``, ``olcf health`` and ``olcf status``). Liveness (``GET /v1/health``) is unaffected, so an orchestrator keeps the process but stops routing to it.
    """
    from fastapi.responses import JSONResponse

    try:
        ctx.db.ping()
    except Exception:  # noqa: BLE001
        return JSONResponse({"status": "unavailable"}, status_code=503)
    try:
        trust_down = ctx.health.run("runtime-trust").status is Status.DOWN
    except KeyError:  # not registered (a stripped-down context): nothing to enforce
        trust_down = False
    if trust_down:
        return JSONResponse({"status": "unavailable"}, status_code=503)
    return {"status": "ready", "node": ctx.node_id, "leader": ctx.leader.is_leader()}


@router.get("/admin/health")
def health_snapshot(principal: Authed, ctx: Ctx, force: bool = False) -> dict[str, Any]:
    principal.require("admin.health")
    return ctx.health.snapshot(force=force)


@router.get("/admin/health/checks")
def health_checks(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("admin.health")
    return {"items": ctx.health.describe()}


@router.post("/admin/health/checks/{name}/test")
def health_deep_test(name: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """The generic 'Test connectivity' button: runs the component's active end-to-end test."""
    principal.require("admin.health")
    try:
        res = ctx.health.run(name, deep=True, force=True)
    except KeyError:
        raise NotFound(f"no such health check '{name}'", details={"available": ctx.health.names()}) from None
    ctx.audit.record(principal, "admin.health_test", resource_type="health_check", resource_id=name, outcome="success" if res.status is Status.OK else "failure")
    return res.to_dict()


@router.get("/admin/info")
def info(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("admin.health")
    return {
        "product": "Open Lakehouse Compute Fabric", "version": __version__, "author": "Justin Dodrill", "node": ctx.node_id,
        "python": sys.version.split()[0], "platform": sys.platform, "uptime_seconds": round(ctx.uptime, 1),
        "database": {"dialect": ctx.db.dialect, "migrations": current_versions(ctx.db)},
        "leader": ctx.leader.is_leader(), "tls": ctx.settings.tls.enabled, "base_url": ctx.settings.base_url,
        "modules": [m.name for m in ctx.modules], "services": sorted(ctx.services), "environment": ctx.settings.environment,
    }


# ===================================================================== routes: secrets
class SecretCreate(BaseModel):
    """Request body of ``POST /v1/secrets``: the secret value (a string or a structured document), its provider and the scope it belongs to.
    """
    name: str = Field(min_length=1, max_length=128)
    value: str | dict[str, Any]
    provider: str | None = None
    scope_type: str = "org"
    scope_id: str = ""
    meta: dict[str, Any] = Field(default_factory=dict)


class SecretRotate(BaseModel):
    """Request body that replaces a secret's value; the secret keeps its id, so nothing that references it needs editing."""
    value: str | dict[str, Any]


def _secret_scope_perm(principal, scope_type: str, scope_id: str) -> None:
    if scope_type == "workspace":
        principal.require("secret.manage", workspace_id=scope_id)
    elif scope_type == "org":
        principal.require("secret.manage")
    else:
        raise BadRequest("secrets may be scoped to 'org' or 'workspace'")


@router.post("/secrets", status_code=201)
def create_secret(body: SecretCreate, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    _secret_scope_perm(principal, body.scope_type, body.scope_id)
    meta = ctx.secrets.create(body.name, body.value, provider=body.provider, scope_type=body.scope_type, scope_id=body.scope_id, created_by=principal.id, meta=body.meta)
    ctx.audit.record(principal, "secret.created", resource_type="secret", resource_id=meta["id"], workspace_id=body.scope_id or None, details={"name": body.name, "provider": meta["provider"]})
    return meta


@router.get("/secrets")
def list_secrets(principal: Authed, ctx: Ctx, scope_type: str | None = None, scope_id: str | None = None) -> dict[str, Any]:
    """Secret *metadata* only. There is deliberately no endpoint that returns a secret value."""
    items = ctx.secrets.list(scope_type=scope_type, scope_id=scope_id)
    items = [s for s in items if principal.can("secret.manage", **({"workspace_id": s["scope_id"]} if s["scope_type"] == "workspace" else {}))]
    return {"items": items}


@router.post("/secrets/{sid}/rotate")
def rotate_secret(sid: str, body: SecretRotate, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    meta = ctx.secrets.get_meta(sid)
    _secret_scope_perm(principal, meta["scope_type"], meta["scope_id"])
    out = ctx.secrets.rotate(sid, body.value)
    ctx.audit.record(principal, "secret.rotated", resource_type="secret", resource_id=sid, workspace_id=meta["scope_id"] or None, details={"version": out["version"]})
    return out


@router.delete("/secrets/{sid}", status_code=204)
def delete_secret(sid: str, principal: Authed, ctx: Ctx) -> None:
    meta = ctx.secrets.get_meta(sid)
    _secret_scope_perm(principal, meta["scope_type"], meta["scope_id"])
    with ctx.db.connect() as conn:
        used = conn.execute(select(S.connections.c.id).where(S.connections.c.secret_id == sid, S.connections.c.deleted_at.is_(None)).limit(1)).first()
    if used:
        raise BadRequest("secret is referenced by a connection; detach it first")
    ctx.secrets.delete(sid)
    ctx.audit.record(principal, "secret.deleted", resource_type="secret", resource_id=sid, workspace_id=meta["scope_id"] or None)


# ===================================================================== routes: audit
@router.get("/audit")
def audit_query(principal: Authed, ctx: Ctx, limit: int = Query(100, ge=1, le=1000), before_seq: int | None = None, action: str | None = None,
                actor: str | None = None, workspace_id: str | None = None, resource_id: str | None = None, since: str | None = None,
                until: str | None = None, outcome: str | None = None) -> dict[str, Any]:
    if workspace_id:
        principal.require("audit.read", workspace_id=workspace_id) if principal.can("audit.read", workspace_id=workspace_id) else principal.require("audit.read")
    else:
        principal.require("audit.read")
    rows = ctx.audit.query(limit=limit, before_seq=before_seq, action=action, actor=actor, workspace_id=workspace_id, resource_id=resource_id, since=since, until=until, outcome=outcome)
    return {"items": rows, "next_before_seq": rows[-1]["seq"] if len(rows) == limit else None}


@router.get("/audit/export")
def audit_export(principal: Authed, ctx: Ctx, format: str = Query("jsonl", pattern="^(jsonl|cef)$")) -> StreamingResponse:
    principal.require("audit.read")
    ctx.audit.record(principal, "audit.exported", details={"format": format})

    def gen():
        for r in ctx.audit.iter_all():
            yield (json.dumps(r, default=str) if format == "jsonl" else ctx.audit.to_cef(r)) + "\n"

    return StreamingResponse(gen(), media_type="application/x-ndjson" if format == "jsonl" else "text/plain")


@router.get("/audit/verify")
def audit_verify(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("audit.read")
    return ctx.audit.verify()


# ===================================================================== routes: events
@router.get("/events")
def list_events(principal: Authed, ctx: Ctx, after_seq: int = 0, limit: int = Query(100, ge=1, le=1000), type: str | None = None, subject: str | None = None,
                descending: bool = False) -> dict[str, Any]:
    visible = principal.visible_workspaces("events.read")
    rows = ctx.events.list(after_seq=after_seq, limit=limit, type_prefix=type, workspace_ids=visible, subject=subject, descending=descending)
    return {"items": rows, "head_seq": ctx.events.head_seq()}


@router.get("/events/stream")
async def stream_events(request: Request, principal: AuthedStream, ctx: Ctx, type: str | None = None, after_seq: int | None = None) -> StreamingResponse:
    """Server-Sent Events. Supports ``Last-Event-ID`` and polls the shared event table so any replica can serve it (visibility is recomputed on every pass)."""
    guard = StreamGuard(ctx, request, principal)
    last = after_seq if after_seq is not None else int(request.headers.get("last-event-id") or ctx.events.head_seq())
    wake = asyncio.Event()
    loop = asyncio.get_running_loop()
    unsub = ctx.events.subscribe(lambda _e: loop.call_soon_threadsafe(wake.set))

    async def gen() -> AsyncIterator[str]:
        nonlocal last
        try:
            yield "retry: 3000\n\n"
            idle = 0.0
            while True:
                if await request.is_disconnected():
                    return
                now_principal = await guard.current()  # disabled account / revoked or expired token: end the stream; withdrawn role: narrow it
                if now_principal is None:
                    yield "event: end\ndata: {\"reason\": \"credentials no longer valid\"}\n\n"
                    return
                visible = now_principal.visible_workspaces("events.read")
                rows = await asyncio.to_thread(ctx.events.list, after_seq=last, limit=200, type_prefix=type, workspace_ids=visible)
                if rows:
                    for r in rows:
                        last = r["seq"]
                        yield f"id: {r['seq']}\nevent: {r['type']}\ndata: {json.dumps(r, default=str)}\n\n"
                    idle = 0.0
                else:
                    idle += 1
                    if idle >= 15:
                        yield ": keep-alive\n\n"
                        idle = 0.0
                try:
                    await asyncio.wait_for(wake.wait(), timeout=1.0)
                except TimeoutError:
                    pass
                wake.clear()
        finally:
            unsub()

    return StreamingResponse(gen(), media_type="text/event-stream", headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"})


# ===================================================================== routes: operations & settings
@router.get("/operations/{oid}")
def get_operation(oid: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    op = ctx.service("operations").get(oid)
    if op["created_by"] != principal.id:
        principal.require("admin.health")
    return op


@router.get("/admin/settings")
def list_settings(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("admin.settings")
    return {"settings": ctx.service("settings").all()}


class SettingPut(BaseModel):
    """Body of ``PUT /admin/settings/{key}``: ``{"value": <any JSON>}``; the setting's validator (``GET /admin/settings``) decides what is acceptable."""

    value: Any = Field(None, description="the new value (any JSON; validated per setting key)")


@router.put("/admin/settings/{key}")
def put_setting(key: str, body: SettingPut, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("admin.settings")
    if "value" not in body.model_fields_set:
        raise BadRequest("body must be {'value': ...}")
    ctx.service("settings").set(key, body.value)
    ctx.audit.record(principal, "config.changed", resource_type="setting", resource_id=key, details={"value": body.value})
    return {"key": key, "value": ctx.service("settings").get(key)}


@router.get("/admin/ports", response_class=PlainTextResponse)
def ports(principal: Authed, ctx: Ctx) -> str:
    principal.require("admin.health")
    c = ctx.settings.compute
    r = c.port_ranges()
    return (
        f"API/UI              tcp/{ctx.settings.server.port}\n"
        f"Spark master RPC    tcp/{r['master_rpc'][0]}-{r['master_rpc'][1]} (one per runtime family)\n"
        f"Spark master UI     tcp/{r['master_ui'][0]}-{r['master_ui'][1]}\n"
        f"Spark worker RPC    tcp/{r['worker_rpc'][0]}-{r['worker_rpc'][1]} (one daemon per runtime family and node)\n"
        f"Spark worker UI     tcp/{r['worker_ui'][0]}-{r['worker_ui'][1]}\n"
        f"Driver UI range     tcp/{r['driver_ui'][0]}-{r['driver_ui'][1]}\n"
        f"Driver RPC range    tcp/{c.driver_port_range[0]}-{c.driver_port_range[1]}\n"
        f"BlockManager range  tcp/{c.block_manager_port_range[0]}-{c.block_manager_port_range[1]}\n"
    )


# ===================================================================== routes: key rotation (admin.keys; see olcf.api.core.rotation)
def _include_rotation() -> None:
    from olcf.api.core import rotation

    router.include_router(rotation.router)


_include_rotation()
