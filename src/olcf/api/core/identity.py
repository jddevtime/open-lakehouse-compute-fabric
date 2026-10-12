# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Identity: local users, login, tokens, role bindings, RBAC catalog."""

from __future__ import annotations

import time
from typing import Any

from fastapi import APIRouter, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import delete, select, update

from olcf.api.deps import Authed, Ctx, Pagination, bearer_token, client_ip
from olcf.core.context import AppContext
from olcf.core.errors import BadRequest, Conflict, Forbidden, NotFound, Unauthenticated
from olcf.core.ids import new_id
from olcf.core.models import Role, ScopeType
from olcf.core.timeutil import from_iso, now_iso
from olcf.db import schema as S
from olcf.db.util import fetch_all, fetch_one, insert_row, paginate
from olcf.security.passwords import generate_password, hash_password, verify_password
from olcf.security.rbac import PERMISSIONS, ROLE_PERMISSIONS, TOKEN_SCOPES, Principal

router = APIRouter(tags=["identity"])

_PUBLIC_COLS = ("id", "kind", "name", "display_name", "email", "is_active", "must_change_password", "source", "created_at", "updated_at", "last_login_at")


def public_principal(row: dict[str, Any]) -> dict[str, Any]:
    return {k: row.get(k) for k in _PUBLIC_COLS}


# ----------------------------------------------------------------------------- service
class IdentityService:
    """Principals and role bindings: creating users and service accounts, binding and unbinding roles (including the resync of bindings that an identity provider owns), local login, refresh-token rotation with reuse detection, and password changes.
    """
    def __init__(self, ctx: AppContext) -> None:
        self.ctx = ctx

    def get_by_name(self, name: str) -> dict[str, Any] | None:
        with self.ctx.db.connect() as conn:
            return fetch_one(conn, select(S.principals).where(S.principals.c.name == name))

    def get(self, pid: str) -> dict[str, Any] | None:
        with self.ctx.db.connect() as conn:
            return fetch_one(conn, select(S.principals).where(S.principals.c.id == pid))

    def create_principal(self, name: str, *, kind: str = "user", password: str | None = None, display_name: str | None = None,
                         email: str | None = None, source: str = "local", external_id: str | None = None,
                         must_change_password: bool = False, attrs: dict[str, Any] | None = None) -> dict[str, Any]:
        if self.get_by_name(name):
            raise Conflict(f"principal '{name}' already exists")
        if password is not None and len(password) < self.ctx.settings.auth.password_min_length:
            raise BadRequest(f"password must be at least {self.ctx.settings.auth.password_min_length} characters")
        now = now_iso()
        row = {
            "id": new_id("usr"), "kind": kind, "name": name, "display_name": display_name or name, "email": email,
            "password_hash": hash_password(password) if password else None, "is_active": True,
            "must_change_password": must_change_password, "source": source, "external_id": external_id,
            "attrs": attrs or {}, "created_at": now, "updated_at": now, "last_login_at": None,
        }
        with self.ctx.db.begin() as conn:
            insert_row(conn, S.principals, row)
        return row

    def bind(self, principal_id: str, role: str, *, scope_type: str = "org", scope_id: str = "", source: str = "manual",
             created_by: str | None = None) -> dict[str, Any]:
        if role not in ROLE_PERMISSIONS:
            raise BadRequest(f"unknown role '{role}'", details={"roles": sorted(ROLE_PERMISSIONS)})
        if scope_type not in {s.value for s in ScopeType}:
            raise BadRequest(f"unknown scope type '{scope_type}'")
        if scope_type == "org":
            scope_id = ""
        elif not scope_id:
            raise BadRequest("scope_id is required for non-org scopes")
        t = S.role_bindings.c
        with self.ctx.db.begin() as conn:
            existing = fetch_one(conn, select(S.role_bindings).where(t.principal_id == principal_id, t.role == role, t.scope_type == scope_type, t.scope_id == scope_id))
            if existing:
                return existing
            row = {"id": new_id("usr").replace("usr_", "bnd_"), "principal_id": principal_id, "role": role, "scope_type": scope_type,
                   "scope_id": scope_id, "source": source, "created_at": now_iso(), "created_by": created_by}
            insert_row(conn, S.role_bindings, row)
        self.ctx.authz.invalidate(principal_id)
        return row

    def unbind(self, binding_id: str) -> bool:
        with self.ctx.db.begin() as conn:
            row = fetch_one(conn, select(S.role_bindings).where(S.role_bindings.c.id == binding_id))
            if not row:
                return False
            conn.execute(delete(S.role_bindings).where(S.role_bindings.c.id == binding_id))
        self.ctx.authz.invalidate(row["principal_id"])
        return True

    def sync_bindings(self, principal_id: str, source: str, wanted: list[tuple[str, str, str]]) -> None:
        """Make the bindings owned by ``source`` (e.g. ``oidc:entra``) exactly equal to ``wanted`` [(role, scope_type, scope_id)]."""
        t = S.role_bindings.c
        with self.ctx.db.begin() as conn:
            have = fetch_all(conn, select(S.role_bindings).where(t.principal_id == principal_id, t.source == source))
            have_keys = {(r["role"], r["scope_type"], r["scope_id"]): r["id"] for r in have}
            want = {(r, st, "" if st == "org" else sid) for r, st, sid in wanted}
            for key, bid in have_keys.items():
                if key not in want:
                    conn.execute(delete(S.role_bindings).where(S.role_bindings.c.id == bid))
            for role, st, sid in want:
                if (role, st, sid) not in have_keys:
                    insert_row(conn, S.role_bindings, {"id": new_id("usr").replace("usr_", "bnd_"), "principal_id": principal_id, "role": role,
                                                       "scope_type": st, "scope_id": sid, "source": source, "created_at": now_iso(), "created_by": None})
        self.ctx.authz.invalidate(principal_id)

    def login(self, username: str, password: str, *, ip: str | None = None) -> dict[str, Any]:
        ctx = self.ctx
        if not ctx.settings.auth.local_login_enabled:
            raise Forbidden("local login is disabled; use your organisation's identity provider")
        ctx.limiter.check(f"login:{ip}", limit=ctx.settings.auth.login_rate_limit_per_minute)
        ctx.limiter.check(f"login-user:{username.lower()}", limit=10)
        row = self.get_by_name(username)
        ok = verify_password(password, row["password_hash"] if row else None)
        if not row or not ok or not row["is_active"] or row["kind"] not in ("user", "service"):
            ctx.audit.record(None, "auth.login_failed", resource_type="principal", resource_id=username, outcome="failure", ip=ip,
                             details={"reason": "bad credentials or disabled"})
            raise Unauthenticated("invalid username or password")
        ctx.limiter.reset(f"login-user:{username.lower()}")
        with ctx.db.begin() as conn:
            conn.execute(update(S.principals).where(S.principals.c.id == row["id"]).values(last_login_at=now_iso()))
        principal = ctx.authz.principal_from_row(row)
        principal.ip = ip
        family = new_id("tok")  # one login session: its refresh-token chain and its access tokens share it (see refresh() / logout())
        access, ttl = ctx.tokens.issue_access_token(row, family=family)
        refresh, _ = ctx.tokens.create_refresh(row["id"], meta={"ip": ip, "family": family})
        ctx.audit.record(principal, "auth.login", resource_type="principal", resource_id=row["id"], ip=ip)
        return {"access_token": access, "token_type": "Bearer", "expires_in": ttl, "refresh_token": refresh,
                "must_change_password": bool(row["must_change_password"]), "principal": public_principal(row)}

    def refresh(self, refresh_token: str) -> dict[str, Any]:
        ctx = self.ctx
        try:
            row = ctx.tokens.verify_opaque(refresh_token, kinds=("refresh",), touch=False)
        except Unauthenticated:
            self._detect_reuse(refresh_token)
            raise
        if not ctx.tokens.revoke(row["id"]):  # rotation: each refresh token is single use (the atomic update also settles two simultaneous refreshes)
            raise Unauthenticated("token revoked")
        prow = self.get(row["principal_id"])
        if not prow or not prow["is_active"]:
            raise Unauthenticated("principal disabled")
        meta = dict(row.get("meta") or {})
        family = meta.setdefault("family", row["id"])  # tokens issued before families existed start one with their own id
        access, ttl = ctx.tokens.issue_access_token(prow, family=family)
        new_refresh, _ = ctx.tokens.create_refresh(prow["id"], meta=meta)
        return {"access_token": access, "token_type": "Bearer", "expires_in": ttl, "refresh_token": new_refresh}

    def _detect_reuse(self, refresh_token: str) -> None:
        """A refresh token that was already rotated (or logged out) is presented again: either the legitimate client lost a response (harmless, handled by the grace
        window) or somebody holds a copy of a stolen token. Standard countermeasure: end the whole login session - revoke the family's live refresh token and every
        access token minted for it - so the thief and the owner both have to sign in again. Audited as ``auth.refresh_reuse_detected``."""
        ctx = self.ctx
        row = ctx.tokens.inspect_opaque(refresh_token, kinds=("refresh",))
        if not row or not row["revoked_at"]:
            return  # unknown / expired token: nothing to conclude
        if time.time() - from_iso(row["revoked_at"]).timestamp() <= ctx.tokens.REUSE_GRACE_SECONDS:
            return
        family = (row.get("meta") or {}).get("family") or row["id"]
        if ctx.tokens.is_revoked(None, family):
            return  # the session was already ended (by an earlier reuse or a logout)
        n = ctx.tokens.revoke_family(family, row["principal_id"])
        prow = self.get(row["principal_id"])
        ctx.audit.record(ctx.authz.principal_from_row(prow) if prow else None, "auth.refresh_reuse_detected", resource_type="principal", resource_id=row["principal_id"],
                         outcome="failure", details={"family": family, "refresh_revoked": n})

    def change_password(self, principal_id: str, new_password: str, *, must_change: bool = False) -> None:
        if len(new_password) < self.ctx.settings.auth.password_min_length:
            raise BadRequest(f"password must be at least {self.ctx.settings.auth.password_min_length} characters")
        with self.ctx.db.begin() as conn:
            conn.execute(update(S.principals).where(S.principals.c.id == principal_id).values(
                password_hash=hash_password(new_password), must_change_password=must_change, updated_at=now_iso()))
        self.ctx.tokens.revoke_all_for(principal_id, ("refresh",))


def bootstrap_admin(ctx: AppContext, *, password: str | None = None, quiet: bool = False) -> tuple[dict[str, Any], str] | None:
    """Create the local bootstrap PlatformAdmin when no user principal exists. Returns (row, password)."""
    svc: IdentityService = ctx.service("identity")
    with ctx.db.connect() as conn:
        n = conn.execute(select(S.principals.c.id).where(S.principals.c.kind == "user").limit(1)).first()
    if n:
        return None
    import os

    pw = password or os.environ.get("OLCF_BOOTSTRAP_ADMIN_PASSWORD") or generate_password()
    row = svc.create_principal(ctx.settings.auth.bootstrap_admin, kind="user", password=pw, display_name="Bootstrap Administrator",
                               source="bootstrap", must_change_password=False)
    svc.bind(row["id"], Role.PLATFORM_ADMIN.value, created_by="usr_system", source="system")
    ctx.audit.record(None, "identity.user_created", resource_type="principal", resource_id=row["id"], details={"bootstrap": True})
    return row, pw


# ----------------------------------------------------------------------------- API models
class LoginRequest(BaseModel):
    """Request body of a local password login."""
    username: str = Field(min_length=1, max_length=128)
    password: str = Field(min_length=1, max_length=1024)


class RefreshRequest(BaseModel):
    """Request body that exchanges a refresh token for a new access token and a rotated refresh token."""
    refresh_token: str


class LogoutRequest(BaseModel):
    """Both parts are optional: the access token may come from the ``Authorization`` header, the refresh token from the body."""

    refresh_token: str | None = None


class UserCreate(BaseModel):
    """Request body that creates a user or a service account, optionally with initial role bindings and a forced password change at first sign-in.
    """
    name: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9._@+-]+$")
    kind: str = Field("user", pattern="^(user|service)$")
    password: str | None = None
    must_change_password: bool = Field(False, description="Force the user to choose a new password at first sign-in (the session can then call only POST /identity/password)")
    display_name: str | None = None
    email: str | None = None
    roles: list[dict[str, str]] = Field(default_factory=list, description="[{role, scope_type, scope_id}]")


class UserUpdate(BaseModel):
    """Request body that changes the display name, e-mail address or active flag of an account."""
    display_name: str | None = None
    email: str | None = None
    is_active: bool | None = None


class PasswordChange(BaseModel):
    """Request body of ``POST /v1/identity/password``; the current password is required unless an administrator resets someone else's."""
    current_password: str | None = None
    new_password: str = Field(min_length=1, max_length=1024)


class BindingCreate(BaseModel):
    """Request body that grants a role to a principal at organisation or workspace scope."""
    principal_id: str
    role: str
    scope_type: str = "org"
    scope_id: str = ""


class TokenCreate(BaseModel):
    """Request body that mints a personal or service access token with limited scopes and a lifetime in days."""
    name: str = Field(min_length=1, max_length=128)
    scopes: list[str] = Field(default_factory=lambda: ["*"])
    ttl_days: int | None = Field(90, ge=1, le=3650)
    principal_id: str | None = None  # admins may mint tokens for service principals


def _svc(ctx: AppContext) -> IdentityService:
    return ctx.service("identity")


def _require_can_grant(principal: Principal, role: str, scope_type: str = "org", scope_id: str = "") -> None:
    """No privilege escalation: a caller may only hand out platform-level roles (PlatformAdmin, SecurityAdmin, RuntimeAdmin) whose permissions it holds
    itself, so a SecurityAdmin cannot create PlatformAdmins. Who may grant the other roles is decided by the route's own permission check."""
    if not principal.covers_role(role, scope_type, scope_id):
        raise Forbidden(f"you cannot grant or remove the role '{role}': it carries permissions you do not hold yourself",
                        remediation="ask a platform administrator (a role grant must be covered by the grantor's own permissions)")


def _require_outranks(principal: Principal, ctx: AppContext, target_id: str) -> None:
    """Acting on another account (credentials, activation, tokens) requires holding at least that account's permissions."""
    if target_id == principal.id:
        return
    if not principal.outranks(list(ctx.authz.bindings_for(target_id))):
        raise Forbidden("this account holds permissions you do not hold yourself, so you cannot change its credentials or state",
                        remediation="ask a platform administrator")


# ----------------------------------------------------------------------------- routes
@router.post("/identity/login")
def login(body: LoginRequest, request: Request, ctx: Ctx) -> dict[str, Any]:
    return _svc(ctx).login(body.username, body.password, ip=client_ip(request, ctx))


@router.post("/identity/refresh")
def refresh(body: RefreshRequest, ctx: Ctx) -> dict[str, Any]:
    return _svc(ctx).refresh(body.refresh_token)


@router.post("/identity/logout", status_code=204)
def logout(request: Request, ctx: Ctx, body: LogoutRequest | None = None) -> None:
    """End the login session: the refresh token (if sent) is revoked and so is every access token of the session - the one in the ``Authorization`` header
    immediately, on every node that shares the database (``revoked_tokens``). Takes no credentials of its own: whatever proves itself valid is revoked."""
    principal_id: str | None = None
    family: str | None = None
    token = bearer_token(request)
    if token and ctx.tokens.looks_like_local_jwt(token):
        try:
            claims = ctx.tokens.verify_access_token(token)
            principal_id, family = claims["sub"], claims.get("fam")
            if claims.get("jti"):
                ctx.tokens.revoke_access(claims["jti"], principal_id=principal_id, expires_at=claims.get("exp"), reason="logout")
        except Unauthenticated:
            pass
    if body and body.refresh_token:
        try:
            row = ctx.tokens.verify_opaque(body.refresh_token, kinds=("refresh",), touch=False)
            ctx.tokens.revoke(row["id"])
            if principal_id in (None, row["principal_id"]):
                principal_id = row["principal_id"]
                family = family or (row.get("meta") or {}).get("family") or row["id"]
        except Unauthenticated:
            pass
    if principal_id and family:
        ctx.tokens.revoke_family(family, principal_id, reason="logout")
    prow = _svc(ctx).get(principal_id) if principal_id else None
    if prow:
        p = ctx.authz.principal_from_row(prow)
        p.ip = client_ip(request, ctx)
        ctx.audit.record(p, "auth.logout", resource_type="principal", resource_id=prow["id"], ip=p.ip)


@router.get("/identity/me")
def me(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    row = _svc(ctx).get(principal.id) or {}
    perms = sorted(p for p in PERMISSIONS if _safe_can(principal, p))
    return {"principal": public_principal(row) if row else principal.summary(), "bindings": [b.__dict__ for b in principal.bindings],
            "token_scopes": principal.token_scopes, "org_permissions": perms}


def _safe_can(principal, perm: str) -> bool:
    try:
        return principal.can(perm)
    except KeyError:
        return False


@router.post("/identity/password", status_code=204)
def change_password(body: PasswordChange, principal: Authed, ctx: Ctx) -> None:
    row = _svc(ctx).get(principal.id)
    if not row or not row["password_hash"]:
        raise BadRequest("this account does not use a local password")
    if not verify_password(body.current_password or "", row["password_hash"]):
        raise Forbidden("current password is incorrect")
    if verify_password(body.new_password, row["password_hash"]):
        raise BadRequest("the new password must differ from the current one")
    _svc(ctx).change_password(principal.id, body.new_password)
    ctx.audit.record(principal, "identity.password_changed", resource_type="principal", resource_id=principal.id)


@router.get("/identity/users")
def list_users(principal: Authed, ctx: Ctx, page: Pagination, kind: str | None = None, q: str | None = None) -> dict[str, Any]:
    principal.require("identity.read")
    stmt = select(S.principals)
    if kind:
        stmt = stmt.where(S.principals.c.kind == kind)
    if q:
        stmt = stmt.where(S.principals.c.name.like(f"%{q}%"))
    with ctx.db.connect() as conn:
        rows, nxt = paginate(conn, stmt, S.principals, limit=page.limit, cursor=page.cursor)
    return {"items": [public_principal(r) for r in rows], "next_cursor": nxt}


@router.post("/identity/users", status_code=201)
def create_user(body: UserCreate, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("identity.manage")
    for r in body.roles:
        _require_can_grant(principal, r.get("role", ""), r.get("scope_type", "org"), r.get("scope_id", ""))
    svc = _svc(ctx)
    if body.kind == "user" and not body.password:
        raise BadRequest("password is required for user accounts (service principals use tokens)")
    if body.must_change_password and body.kind != "user":
        raise BadRequest("must_change_password applies to user accounts (service principals have no password)")
    row = svc.create_principal(body.name, kind=body.kind, password=body.password, display_name=body.display_name, email=body.email,
                               must_change_password=body.must_change_password)
    for r in body.roles:
        svc.bind(row["id"], r["role"], scope_type=r.get("scope_type", "org"), scope_id=r.get("scope_id", ""), created_by=principal.id)
    ctx.audit.record(principal, "identity.user_created", resource_type="principal", resource_id=row["id"],
                     details={"name": body.name, "kind": body.kind, "must_change_password": body.must_change_password})
    return public_principal(row)


@router.get("/identity/users/{user_id}")
def get_user(user_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    if user_id != principal.id:
        principal.require("identity.read")
    row = _svc(ctx).get(user_id)
    if not row:
        raise NotFound("principal not found")
    with ctx.db.connect() as conn:
        binds = fetch_all(conn, select(S.role_bindings).where(S.role_bindings.c.principal_id == user_id))
    return {**public_principal(row), "bindings": binds}


@router.patch("/identity/users/{user_id}")
def update_user(user_id: str, body: UserUpdate, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("identity.manage")
    _require_outranks(principal, ctx, user_id)
    vals = {k: v for k, v in body.model_dump().items() if v is not None}
    if not vals:
        raise BadRequest("nothing to update")
    if vals.get("is_active") is False and user_id == principal.id:
        raise BadRequest("you cannot disable your own account")
    with ctx.db.begin() as conn:
        n = conn.execute(update(S.principals).where(S.principals.c.id == user_id).values(**vals, updated_at=now_iso())).rowcount
    if not n:
        raise NotFound("principal not found")
    if vals.get("is_active") is False:
        ctx.tokens.revoke_all_for(user_id)
    ctx.authz.invalidate(user_id)
    ctx.audit.record(principal, "identity.user_updated", resource_type="principal", resource_id=user_id, details=vals)
    return public_principal(_svc(ctx).get(user_id) or {})


@router.post("/identity/users/{user_id}/reset-password")
def reset_password(user_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("identity.manage")
    _require_outranks(principal, ctx, user_id)
    row = _svc(ctx).get(user_id)
    if not row or row["kind"] != "user":
        raise NotFound("user not found")
    pw = generate_password()
    _svc(ctx).change_password(user_id, pw, must_change=True)
    ctx.audit.record(principal, "identity.password_changed", resource_type="principal", resource_id=user_id, details={"reset": True})
    return {"temporary_password": pw, "must_change_password": True}


@router.get("/roles")
def list_roles(principal: Authed) -> dict[str, Any]:
    return {"roles": [{"name": r.value, "permissions": sorted(ROLE_PERMISSIONS[r])} for r in Role], "permissions": PERMISSIONS,
            "token_scopes": {k: sorted(v) for k, v in TOKEN_SCOPES.items()}}


@router.get("/identity/bindings")
def list_bindings(principal: Authed, ctx: Ctx, principal_id: str | None = None, scope_type: str | None = None, scope_id: str | None = None) -> dict[str, Any]:
    if principal_id != principal.id:
        if scope_type == "workspace" and scope_id:
            principal.require("workspace.members.manage", workspace_id=scope_id)
        else:
            principal.require("identity.read")
    stmt = select(S.role_bindings, S.principals.c.name.label("principal_name")).join(S.principals, S.principals.c.id == S.role_bindings.c.principal_id)
    if principal_id:
        stmt = stmt.where(S.role_bindings.c.principal_id == principal_id)
    if scope_type:
        stmt = stmt.where(S.role_bindings.c.scope_type == scope_type)
    if scope_id:
        stmt = stmt.where(S.role_bindings.c.scope_id == scope_id)
    with ctx.db.connect() as conn:
        return {"items": fetch_all(conn, stmt.order_by(S.role_bindings.c.id))}


@router.post("/identity/bindings", status_code=201)
def create_binding(body: BindingCreate, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    if body.scope_type == "workspace":
        principal.require("workspace.members.manage", workspace_id=body.scope_id)
        if body.role in (Role.PLATFORM_ADMIN, Role.SECURITY_ADMIN, Role.RUNTIME_ADMIN):
            raise Forbidden("organisation roles cannot be granted at workspace scope")
    else:
        principal.require("identity.manage")
    _require_can_grant(principal, body.role, body.scope_type, body.scope_id)
    if not _svc(ctx).get(body.principal_id):
        raise NotFound("principal not found")
    row = _svc(ctx).bind(body.principal_id, body.role, scope_type=body.scope_type, scope_id=body.scope_id, created_by=principal.id)
    ctx.audit.record(principal, "identity.binding_changed", resource_type="principal", resource_id=body.principal_id,
                     workspace_id=body.scope_id if body.scope_type == "workspace" else None,
                     details={"op": "grant", "role": body.role, "scope_type": body.scope_type, "scope_id": body.scope_id})
    return row


@router.delete("/identity/bindings/{binding_id}", status_code=204)
def delete_binding(binding_id: str, principal: Authed, ctx: Ctx) -> None:
    with ctx.db.connect() as conn:
        row = fetch_one(conn, select(S.role_bindings).where(S.role_bindings.c.id == binding_id))
    if not row:
        raise NotFound("binding not found")
    if row["scope_type"] == "workspace":
        principal.require("workspace.members.manage", workspace_id=row["scope_id"])
    else:
        principal.require("identity.manage")
    if row["principal_id"] != principal.id:  # giving up one's own role is always allowed; taking away someone else's needs the same standing as granting it
        _require_can_grant(principal, row["role"], row["scope_type"], row["scope_id"])
    if row["role"] == Role.PLATFORM_ADMIN and row["principal_id"] == principal.id:
        with ctx.db.connect() as conn:
            admins = conn.execute(select(S.role_bindings.c.id).where(S.role_bindings.c.role == Role.PLATFORM_ADMIN.value)).all()
        if len(admins) <= 1:
            raise BadRequest("refusing to remove the last PlatformAdmin binding")
    _svc(ctx).unbind(binding_id)
    ctx.audit.record(principal, "identity.binding_changed", resource_type="principal", resource_id=row["principal_id"],
                     workspace_id=row["scope_id"] if row["scope_type"] == "workspace" else None,
                     details={"op": "revoke", "role": row["role"], "scope_type": row["scope_type"], "scope_id": row["scope_id"]})


@router.get("/identity/tokens")
def list_tokens(principal: Authed, ctx: Ctx, all: bool = Query(False)) -> dict[str, Any]:
    stmt = select(S.api_tokens).where(S.api_tokens.c.kind == "pat", S.api_tokens.c.revoked_at.is_(None))
    if all:
        principal.require("identity.manage")
    else:
        stmt = stmt.where(S.api_tokens.c.principal_id == principal.id)
    with ctx.db.connect() as conn:
        rows = fetch_all(conn, stmt.order_by(S.api_tokens.c.id.desc()))
    return {"items": [{k: r[k] for k in ("id", "principal_id", "name", "prefix", "scopes", "created_at", "expires_at", "last_used_at")} for r in rows]}


@router.post("/identity/tokens", status_code=201)
def create_token(body: TokenCreate, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    target = body.principal_id or principal.id
    if target != principal.id:
        principal.require("identity.manage")
        if not _svc(ctx).get(target):
            raise NotFound("principal not found")
        _require_outranks(principal, ctx, target)
    bad = [s for s in body.scopes if s not in TOKEN_SCOPES]
    if bad:
        raise BadRequest(f"unknown token scope(s): {bad}", details={"scopes": sorted(TOKEN_SCOPES)})
    if "*" in body.scopes and target == principal.id and "*" not in principal.token_scopes:
        raise Forbidden("a restricted token cannot mint an unrestricted token")
    # F01: a credential can only hand out what it holds itself - a read-only or "mcp:write" token must not be able to mint a "write" (or "job") token for the same account
    wanted: set[str] = set()
    for scope in body.scopes:
        wanted |= TOKEN_SCOPES[scope]
    held = principal.effective_permissions()
    if "*" not in held and ("*" in wanted or not wanted <= held):
        raise Forbidden("a token cannot mint a token with scopes that exceed its own", details={"missing": sorted(wanted - held) if "*" not in wanted else ["*"]},
                        remediation="sign in (or use a token) with the scopes you want to hand out")
    plaintext, rec = ctx.tokens.create_pat(target, body.name, scopes=body.scopes, ttl_days=body.ttl_days)
    ctx.audit.record(principal, "auth.token_created", resource_type="token", resource_id=rec["id"], details={"for": target, "scopes": body.scopes})
    return {"token": plaintext, "id": rec["id"], "prefix": rec["prefix"], "scopes": rec["scopes"], "expires_at": rec["expires_at"],
            "note": "Store this token now; it cannot be shown again."}


@router.delete("/identity/tokens/{token_id}", status_code=204)
def revoke_token(token_id: str, principal: Authed, ctx: Ctx) -> None:
    with ctx.db.connect() as conn:
        row = fetch_one(conn, select(S.api_tokens).where(S.api_tokens.c.id == token_id))
    if not row:
        raise NotFound("token not found")
    if row["principal_id"] != principal.id:
        principal.require("identity.manage")
    ctx.tokens.revoke(token_id)
    ctx.audit.record(principal, "auth.token_revoked", resource_type="token", resource_id=token_id)
