# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""HTTP API of identity federation (mounted under ``/v1`` by :mod:`olcf.security.federation`).

Administration (``identity.federation.manage``)
    ``GET|POST /identity/providers``, ``GET|PATCH|DELETE /identity/providers/{id}``, ``POST /identity/providers/{id}/test``,
    ``POST /identity/providers/{id}/preview-mapping``, ``GET|PUT /identity/local-login``

Public (the credential is in the request itself)
    ``GET /identity/oidc/providers``, ``GET /identity/oidc/{provider}/login``, ``GET /identity/oidc/{provider}/callback``,
    ``POST /identity/oidc/exchange``, ``POST /identity/ldap/{provider}/login``
"""

from __future__ import annotations

import re
from typing import Any

from fastapi import APIRouter, Query, Request, Response
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select, update

from olcf.api.deps import Authed, Ctx, IfMatch, check_if_match, client_ip, etag_header
from olcf.core.errors import BadRequest, Conflict, Forbidden, NotFound, ValidationFailed
from olcf.core.ids import new_id
from olcf.core.timeutil import now_iso
from olcf.db import schema as S
from olcf.db.util import fetch_all, fetch_one, new_etag
from olcf.security import federation_mapping as M
from olcf.security import federation_oidc as O
from olcf.security import schema as SEC

router = APIRouter(tags=["identity-federation"])
NAME_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")
PRIVILEGED_ROLES = {"PlatformAdmin", "SecurityAdmin"}


def _svc(ctx):  # noqa: ANN001, ANN202
    return ctx.service("federation")


# ----------------------------------------------------------------------------- models
class ProviderCreate(BaseModel):
    """Request body for adding an identity provider of type OIDC, Entra or LDAP."""
    model_config = ConfigDict(extra="forbid")
    name: str
    type: str = Field(pattern="^(oidc|entra|ldap)$")
    display_name: str | None = Field(None, max_length=255)
    enabled: bool = True
    issuer: str | None = Field(None, max_length=512)
    tenant: str | None = Field(None, max_length=255, description="Entra tenant id / domain / organizations / common")
    client_id: str | None = Field(None, max_length=255)
    client_secret: str | None = Field(None, max_length=4096, description="Stored in the secret store; never returned")
    config: dict[str, Any] = Field(default_factory=dict)


class ProviderPatch(BaseModel):
    """Request body for changing an identity provider's configuration; only the fields that are set change."""
    model_config = ConfigDict(extra="forbid")
    display_name: str | None = Field(None, max_length=255)
    enabled: bool | None = None
    issuer: str | None = Field(None, max_length=512)
    tenant: str | None = None
    client_id: str | None = Field(None, max_length=255)
    client_secret: str | None = Field(None, max_length=4096)
    config: dict[str, Any] | None = None


class TestRequest(BaseModel):
    """Request body for testing an identity provider, optionally with a client-credentials token request."""
    client_credentials: bool = False
    scope: str | None = None


class ExchangeRequest(BaseModel):
    """Request body for exchanging a one-time login code for OLCF tokens."""
    code: str = Field(min_length=10, max_length=256)
    ui_verifier: str | None = Field(None, max_length=256)


class LdapLogin(BaseModel):
    """Request body for logging in through an LDAP identity provider."""
    username: str = Field(min_length=1, max_length=256)
    password: str = Field(min_length=1, max_length=1024)


class LocalLoginBody(BaseModel):
    """Request body that switches local username and password login on or off."""
    enabled: bool


class MappingPreview(BaseModel):
    """Request body for previewing which roles a set of claims would map to."""
    claims: dict[str, Any]


class IdentityProviderOut(BaseModel):
    """An identity provider as shown to administrators: configuration without any secret (``has_client_secret`` says whether one is stored)."""

    id: str
    name: str
    type: str
    display_name: str | None = None
    enabled: bool
    issuer: str | None = None
    client_id: str | None = None
    has_client_secret: bool = False
    config: dict[str, Any] = Field(default_factory=dict)
    last_test: dict[str, Any] | None = Field(None, description="{ok, at, steps[]} of the last connectivity test; null until tested or after a change")
    etag: str
    created_at: str | None = None
    updated_at: str | None = None


def public_provider(row: dict[str, Any]) -> dict[str, Any]:
    return {"id": row["id"], "name": row["name"], "type": row["type"], "display_name": row["display_name"], "enabled": bool(row["enabled"]),
            "issuer": row["issuer"], "client_id": row["client_id"], "has_client_secret": bool(row["client_secret_id"]), "config": row["config"],
            "last_test": row.get("last_test"), "etag": row["etag"], "created_at": row["created_at"], "updated_at": row["updated_at"]}


# ----------------------------------------------------------------------------- validation
def _normalise(body_type: str, name: str, issuer: str | None, tenant: str | None, client_id: str | None, config: dict[str, Any], *, principal) -> tuple[str | None, dict[str, Any]]:  # noqa: ANN001
    """Validate a provider definition offline; returns ``(issuer, normalised config)``. Raises ``ValidationFailed``/``BadRequest``."""
    try:
        cfg = M.ProviderConfig.model_validate({**config, **({"tenant": tenant} if tenant else {})})
    except Exception as exc:  # noqa: BLE001 - pydantic ValidationError
        errs = exc.errors() if callable(getattr(exc, "errors", None)) else [{"msg": str(exc)}]
        raise ValidationFailed("invalid provider configuration", details={"errors": [{"loc": [str(x) for x in e.get("loc", [])], "msg": str(e.get("msg"))} for e in errs]}) from exc
    out = cfg.model_dump(exclude_none=True, exclude_defaults=True)
    allow_http = cfg.allow_insecure_http
    if body_type == "ldap":
        if cfg.ldap is None:
            raise BadRequest("type 'ldap' requires config.ldap")
        issuer = None
    else:
        if not client_id:
            raise BadRequest("client_id is required")
        if body_type == "entra":
            t = cfg.tenant
            if not t:
                raise BadRequest("type 'entra' requires 'tenant' (tenant id, domain, organizations or common)")
            authority = (cfg.authority_host or O.DEFAULT_ENTRA_AUTHORITY).rstrip("/")
            issuer = f"{authority}/{t}/v2.0"
            if t.lower() in O.MULTI_TENANT_AUTHORITIES and not cfg.allowed_tenants:
                raise BadRequest("a multi-tenant authority needs config.allowed_tenants (an empty allow-list would accept no tenant)")
            for tid in cfg.allowed_tenants:
                if not O.GUID_RE.match(tid):
                    raise BadRequest(f"allowed_tenants entries must be tenant GUIDs, got {tid!r}")
        else:
            if not issuer:
                raise BadRequest("type 'oidc' requires 'issuer'")
        O.validate_endpoint_url(issuer or "", allow_http=allow_http, what="issuer")
        for k in ("discovery_url", "jwks_uri", "graph_base_url"):
            if getattr(cfg, k):
                O.validate_endpoint_url(str(getattr(cfg, k)), allow_http=allow_http, what=k)
    # privilege-escalation guard: a mapping hands out roles to whoever the IdP says, so the caller must hold every organisation-level role it maps
    # (a SecurityAdmin cannot map a group to PlatformAdmin; only a PlatformAdmin may). Workspace-scoped rules follow workspace.members.manage rules.
    uncovered = sorted({r.role for r in cfg.all_rules() if r.scope_type == "org" and not principal.covers_role(r.role)})
    if uncovered:
        raise Forbidden(f"you cannot map identities to {', '.join(uncovered)}: the role carries permissions you do not hold yourself", remediation="ask a PlatformAdmin")
    return issuer, out


def _row(ctx, ref: str) -> dict[str, Any]:  # noqa: ANN001
    t = SEC.identity_providers.c
    with ctx.db.connect() as conn:
        row = fetch_one(conn, select(SEC.identity_providers).where((t.id == ref) | (t.name == ref)))
    if not row:
        raise NotFound(f"identity provider '{ref}' not found")
    return row


# ----------------------------------------------------------------------------- admin routes
@router.get("/identity/providers")
def list_providers(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("identity.federation.manage")
    with ctx.db.connect() as conn:
        rows = fetch_all(conn, select(SEC.identity_providers).order_by(SEC.identity_providers.c.name))
    return {"items": [public_provider(r) for r in rows], "next_cursor": None, "local_login": _svc(ctx).local_login_enabled()}


@router.post("/identity/providers", status_code=201, response_model=IdentityProviderOut)
def create_provider(body: ProviderCreate, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    principal.require("identity.federation.manage")
    if not NAME_RE.match(body.name):
        raise ValidationFailed("provider name must be lower-case letters, digits and '-' (max 63)")
    issuer, cfg = _normalise(body.type, body.name, body.issuer, body.tenant, body.client_id, body.config, principal=principal)
    with ctx.db.connect() as conn:
        if fetch_one(conn, select(SEC.identity_providers.c.id).where(SEC.identity_providers.c.name == body.name)):
            raise Conflict(f"identity provider '{body.name}' already exists")
    secret_id = None
    if body.client_secret:
        secret_id = ctx.secrets.create(f"idp-{body.name}-client-secret", body.client_secret, created_by=principal.id,
                                       meta={"kind": "idp_client_secret", "provider": body.name})["id"]
    now = now_iso()
    row = {"id": new_id("idp"), "name": body.name, "type": body.type, "display_name": body.display_name or body.name, "enabled": body.enabled, "issuer": issuer,
           "client_id": body.client_id, "client_secret_id": secret_id, "config": cfg, "last_test": None, "etag": new_etag(), "created_by": principal.id,
           "created_at": now, "updated_at": now}
    with ctx.db.begin() as conn:
        conn.execute(SEC.identity_providers.insert().values(**row))
    _svc(ctx).invalidate()
    ctx.audit.record(principal, "config.changed", resource_type="identity_provider", resource_id=row["id"], details={"op": "create", "name": body.name, "type": body.type, "issuer": issuer})
    response.headers["ETag"] = etag_header(row["etag"])
    return public_provider(row)


@router.get("/identity/providers/{provider_id}", response_model=IdentityProviderOut)
def get_provider(provider_id: str, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    principal.require("identity.federation.manage")
    row = _row(ctx, provider_id)
    response.headers["ETag"] = etag_header(row["etag"])
    return public_provider(row)


@router.patch("/identity/providers/{provider_id}", response_model=IdentityProviderOut)
def patch_provider(provider_id: str, body: ProviderPatch, principal: Authed, ctx: Ctx, response: Response, if_match: IfMatch = None) -> dict[str, Any]:
    principal.require("identity.federation.manage")
    row = _row(ctx, provider_id)
    check_if_match(if_match, row["etag"])
    changes = body.model_dump(exclude_unset=True)
    merged_cfg = {**row["config"], **(changes.get("config") or {})} if "config" in changes and changes["config"] is not None else row["config"]
    issuer, cfg = _normalise(row["type"], row["name"], changes.get("issuer", row["issuer"]) if row["type"] == "oidc" else None,
                             changes.get("tenant"), changes.get("client_id", row["client_id"]), merged_cfg, principal=principal)
    vals: dict[str, Any] = {"config": cfg, "issuer": issuer, "updated_at": now_iso(), "etag": new_etag(), "last_test": None}
    for k in ("display_name", "enabled", "client_id"):
        if k in changes and changes[k] is not None:
            vals[k] = changes[k]
    if changes.get("client_secret"):
        if row["client_secret_id"]:
            ctx.secrets.rotate(row["client_secret_id"], changes["client_secret"])
        else:
            vals["client_secret_id"] = ctx.secrets.create(f"idp-{row['name']}-client-secret", changes["client_secret"], created_by=principal.id,
                                                          meta={"kind": "idp_client_secret", "provider": row["name"]})["id"]
    with ctx.db.begin() as conn:
        conn.execute(update(SEC.identity_providers).where(SEC.identity_providers.c.id == row["id"]).values(**vals))
    svc = _svc(ctx)
    svc.invalidate()
    if vals.get("enabled") is False:
        _revoke_sessions(ctx, row["name"])
    audit_detail = {"op": "update", "name": row["name"], "fields": sorted(k for k in changes if k != "client_secret") + (["client_secret"] if changes.get("client_secret") else [])}
    ctx.audit.record(principal, "config.changed", resource_type="identity_provider", resource_id=row["id"], details=audit_detail)
    new = _row(ctx, row["id"])
    response.headers["ETag"] = etag_header(new["etag"])
    return public_provider(new)


def _revoke_sessions(ctx, name: str) -> int:  # noqa: ANN001
    """When a provider is disabled/removed its users must not keep refreshing sessions: revoke their refresh tokens."""
    n = 0
    t = S.principals.c
    with ctx.db.connect() as conn:
        ids = [r["id"] for r in fetch_all(conn, select(S.principals.c.id).where(t.source.in_([f"oidc:{name}", f"ldap:{name}"])))]
    for pid in ids:
        n += ctx.tokens.revoke_all_for(pid, ("refresh",))
    return n


@router.delete("/identity/providers/{provider_id}", status_code=204)
def delete_provider(provider_id: str, principal: Authed, ctx: Ctx) -> None:
    principal.require("identity.federation.manage")
    row = _row(ctx, provider_id)
    svc = _svc(ctx)
    if not svc.local_login_enabled():
        # deleting could remove the only way in: re-run the lock-out analysis as if this provider were gone
        others = [p for p in svc.providers() if p.id != row["id"]]
        svc_check = svc.lockout_check
        ok, reasons = svc_check()
        still = any(p.allow_login_enabled() for p in others)
        if not still or not ok:
            raise Conflict("refusing to delete the last usable provider while local login is disabled", details={"reasons": reasons},
                           remediation="re-enable local login first (PUT /v1/identity/local-login)")
    _revoke_sessions(ctx, row["name"])
    with ctx.db.begin() as conn:
        conn.execute(SEC.identity_providers.delete().where(SEC.identity_providers.c.id == row["id"]))
    if row["client_secret_id"]:
        try:
            ctx.secrets.delete(row["client_secret_id"])
        except Exception:  # noqa: BLE001
            pass
    svc.invalidate()
    ctx.audit.record(principal, "config.changed", resource_type="identity_provider", resource_id=row["id"], details={"op": "delete", "name": row["name"]})


@router.post("/identity/providers/{provider_id}/test")
def test_provider(provider_id: str, principal: Authed, ctx: Ctx, body: TestRequest | None = None) -> dict[str, Any]:
    principal.require("identity.federation.manage")
    row = _row(ctx, provider_id)
    svc = _svc(ctx)
    spec = O.ProviderSpec.from_row(row)
    body = body or TestRequest()
    res = svc.test_provider(spec, client_credentials=body.client_credentials, scope=body.scope)
    ctx.audit.record(principal, "admin.health_test", resource_type="identity_provider", resource_id=row["id"], outcome="success" if res["ok"] else "failure")
    return res


@router.post("/identity/providers/{provider_id}/preview-mapping")
def preview_mapping(provider_id: str, body: MappingPreview, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Dry-run: which bindings would these claims receive? Lets administrators check mappings before real users log in."""
    principal.require("identity.federation.manage")
    row = _row(ctx, provider_id)
    svc = _svc(ctx)
    spec = O.ProviderSpec.from_row(row)
    cfg = svc.parsed(spec)
    claims = dict(body.claims)
    sp = M.find_service_mapping(cfg, claims) if M.is_service_token(claims) else None
    bindings, skipped = M.evaluate(cfg, claims, svc._resolve_ws, service_match=sp, extra_rules=cfg.ldap.group_rules if cfg.ldap else None)  # noqa: SLF001
    return {"bindings": [{"role": r, "scope_type": st, "scope_id": sid} for r, st, sid in bindings], "skipped": skipped,
            "service_principal": sp.name if sp else None, "would_login": bool(bindings) or cfg.allow_login_without_roles}


@router.get("/identity/local-login")
def get_local_login(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("identity.federation.manage")
    svc = _svc(ctx)
    ok, reasons = svc.lockout_check()
    return {"enabled": svc.local_login_enabled(), "can_disable": ok, "blockers": reasons, "break_glass_env": "OLCF_BREAKGLASS_LOCAL_LOGIN"}


@router.put("/identity/local-login")
def put_local_login(body: LocalLoginBody, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("identity.federation.manage")
    return _svc(ctx).set_local_login(body.enabled, principal)


# ----------------------------------------------------------------------------- public routes
@router.get("/identity/oidc/providers")
def public_providers(ctx: Ctx) -> dict[str, Any]:
    """What the login page needs: which sign-in buttons to show. No secrets, no configuration."""
    svc = _svc(ctx)
    items = [{"name": p.name, "display_name": p.display_name, "type": p.type,
              "login_url": f"/v1/identity/oidc/{p.name}/login" if p.type != "ldap" else None,
              "password_login_url": f"/v1/identity/ldap/{p.name}/login" if p.type == "ldap" else None}
             for p in svc.providers() if p.allow_login_enabled()]
    return {"items": items, "local_login": svc.local_login_enabled()}


@router.get("/identity/oidc/{provider}/login", status_code=302)
def oidc_login(provider: str, request: Request, ctx: Ctx, redirect_to: str | None = Query(None, max_length=1100),
               ui_challenge: str | None = Query(None, max_length=128)) -> RedirectResponse:
    res = _svc(ctx).begin_login(provider, redirect_to, ui_challenge, client_ip(request, ctx))
    resp = RedirectResponse(res.location, status_code=302)
    resp.set_cookie(res.cookie_name or "", res.cookie_value or "", max_age=600, path="/v1/identity/oidc", httponly=True, samesite="lax",
                    secure=ctx.settings.base_url.startswith("https://"))
    return resp


@router.get("/identity/oidc/{provider}/callback", status_code=302)
def oidc_callback(provider: str, request: Request, ctx: Ctx, code: str | None = Query(None, max_length=4096), state: str | None = Query(None, max_length=512),
                  error: str | None = Query(None, max_length=128)) -> RedirectResponse:
    svc = _svc(ctx)
    binding = request.cookies.get(svc.cookie_name(state)) if state else None
    res = svc.complete_login(provider, code=code, state=state, error=error, binding=binding, ip=client_ip(request, ctx))
    resp = RedirectResponse(res.location, status_code=302)
    resp.delete_cookie(res.cookie_name or svc.cookie_name(state or ""), path="/v1/identity/oidc")
    return resp


@router.post("/identity/oidc/exchange")
def oidc_exchange(body: ExchangeRequest, request: Request, ctx: Ctx) -> dict[str, Any]:
    return _svc(ctx).exchange_code(body.code, body.ui_verifier, client_ip(request, ctx))


@router.post("/identity/ldap/{provider}/login")
def ldap_login(provider: str, body: LdapLogin, request: Request, ctx: Ctx) -> dict[str, Any]:
    return _svc(ctx).ldap_login(provider, body.username, body.password, client_ip(request, ctx))
