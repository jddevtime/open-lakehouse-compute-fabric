# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Identity federation: OIDC / OAuth2 (generic + Microsoft Entra) and LDAP bridge logins, external bearer validation (spec 22, 31).

Module path ``olcf.security.federation`` (fixed by ``olcf.api.modules.MODULES``); ``install(ctx)`` registers the ``federation`` service:

``federation.authenticate_bearer(token) -> Principal | None``
    Called by the foundation ``authenticate`` for any bearer token that is not a platform token. Returns ``None`` (HTTP 401) for everything that
    cannot be proven: unknown issuer, bad signature/algorithm, wrong audience or tenant, expired, unmapped identities. See
    :mod:`olcf.security.federation_oidc` for the validation rules and :mod:`olcf.security.federation_mapping` for role mapping.

Interactive login (authorization code + PKCE, server side)
----------------------------------------------------------
1. ``GET /v1/identity/oidc/{provider}/login?redirect_to=/path`` creates a login request (random ``state`` and ``nonce``, PKCE S256 verifier;
   only hashes of state/browser-binding are stored) and redirects to the IdP. A random **browser-binding cookie** (HttpOnly, SameSite=Lax) is
   set; the callback only succeeds in the same browser (login CSRF / state fixation defence). ``redirect_to`` is validated against an allow-list
   of origins (open-redirect defence) and the IdP ``redirect_uri`` is always built from the *configured* public URL, never from the Host header.
2. ``GET /v1/identity/oidc/{provider}/callback`` consumes the state exactly once, exchanges the code **server side** (client secret from the secret
   store), validates the ``id_token`` (signature, issuer, audience, expiry, nonce, tenant, ``at_hash``), JIT-provisions the principal
   (``source="oidc:<provider>"``, keyed by the IdP subject - never by name, so an IdP user called ``admin`` cannot become the local admin),
   syncs role bindings from the mapping rules and redirects back to the UI with ``#olcf_login_code=<one-time code>`` (a fragment is never sent to
   servers or logged).
3. ``POST /v1/identity/oidc/exchange {code, ui_verifier?}`` turns the one-time code (60 s, single use, optionally bound to a UI-side PKCE challenge)
   into OLCF access + refresh tokens. No token ever travels in a URL.

LDAP / AD: see :mod:`olcf.security.ldap_bridge` (``POST /v1/identity/ldap/{provider}/login``).

Local login can be disabled once an external administrator exists (``PUT /v1/identity/local-login``): the call is refused when it would lock the
organisation out (no active federated PlatformAdmin whose provider is enabled). Break-glass: start the control plane with
``OLCF_BREAKGLASS_LOCAL_LOGIN=1`` to force local login back on.

HA: every piece of state (providers, login requests, one-time codes) is in the database; JWKS/discovery caches are per process and refresh
themselves. Health check ``identity-providers``: quick = last stored test results, deep = live test of every enabled provider.
"""

from __future__ import annotations

import hmac
import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urlencode, urlsplit, urlunsplit

import jwt
from sqlalchemy import select, update

from olcf.core.context import AppContext, Module
from olcf.core.errors import (
    BadRequest,
    Conflict,
    Forbidden,
    NotFound,
    OlcfError,
    TooManyRequests,
    Unauthenticated,
    Unavailable,
)
from olcf.core.health import ComponentHealth, HealthCheck, Status
from olcf.core.timeutil import from_iso, iso_in, now_iso
from olcf.db import schema as S
from olcf.db.util import fetch_all, fetch_one
from olcf.security import federation_mapping as M
from olcf.security import federation_oidc as O
from olcf.security import schema as SEC
from olcf.security.rbac import Principal

log = logging.getLogger("olcf.federation")

LOGIN_REQUEST_TTL = 600
LOGIN_CODE_TTL = 60
KV_LOCAL_LOGIN = "identity.local_login_enabled"
BREAKGLASS_ENV = "OLCF_BREAKGLASS_LOCAL_LOGIN"


def _origin(url: str) -> str:
    p = urlsplit(url)
    return f"{p.scheme}://{p.netloc}".lower()


@dataclass
class LoginRedirect:
    """Where to send the browser to start an external login: the URL and an optional cookie to set."""
    location: str
    cookie_name: str | None = None
    cookie_value: str | None = None


class FederationService:
    """Provider repository + login flows + bearer validation. Registered as ``ctx.service("federation")``."""

    def __init__(self, ctx: AppContext, *, http: O.Http | None = None) -> None:
        self.ctx = ctx
        self.http = http or O.Http(ctx.settings)
        self.discovery = O.DiscoveryCache(self.http)
        self.jwks = O.JwksCache(self.http)
        self.validator = O.TokenValidator(self.jwks, self.discovery)
        self._providers: tuple[float, list[O.ProviderSpec]] | None = None
        self._plock = threading.Lock()
        self._val_cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self._fail_throttle: dict[str, float] = {}
        #: ``auth.local_login_enabled`` from the config file/environment: an upper bound (the KV switch can only disable further)
        self.file_local_login = bool(ctx.settings.auth.local_login_enabled)
        self.provider_cache_ttl = 5.0
        self.bearer_cache_seconds = 20.0

    # ================================================================== provider repository
    def invalidate(self) -> None:
        with self._plock:
            self._providers = None
        self.discovery.invalidate()
        self.jwks.invalidate()

    def providers(self, *, include_disabled: bool = False) -> list[O.ProviderSpec]:
        now = time.monotonic()
        with self._plock:
            cached = self._providers
        if cached is None or now - cached[0] > self.provider_cache_ttl:
            with self.ctx.db.connect() as conn:
                rows = fetch_all(conn, select(SEC.identity_providers).order_by(SEC.identity_providers.c.name))
            cached = (now, [O.ProviderSpec.from_row(r) for r in rows])
            with self._plock:
                self._providers = cached
        return [p for p in cached[1] if include_disabled or p.enabled]

    def spec_by_name(self, name: str, *, include_disabled: bool = False) -> O.ProviderSpec:
        for p in self.providers(include_disabled=include_disabled):
            if p.name == name or p.id == name:
                return p
        raise NotFound(f"identity provider '{name}' not found")

    def parsed(self, spec: O.ProviderSpec) -> M.ProviderConfig:
        return M.ProviderConfig.model_validate(spec.config)

    def client_secret(self, spec: O.ProviderSpec) -> str | None:
        if not spec.client_secret_id:
            return None
        val = self.ctx.secrets.resolve(spec.client_secret_id)
        if isinstance(val, dict):
            val = val.get("client_secret") or val.get("value")
        return str(val) if val else None

    # ================================================================== local login switch
    def local_login_enabled(self) -> bool:
        if os.environ.get(BREAKGLASS_ENV) == "1":
            return True
        if not self.file_local_login:
            return False
        with self.ctx.db.connect() as conn:
            row = fetch_one(conn, select(S.kv).where(S.kv.c.key == KV_LOCAL_LOGIN))
        return True if row is None else bool(row["value"])

    def apply_local_login_flag(self) -> bool:
        """Push the effective flag into ``ctx.settings.auth.local_login_enabled`` (read by the foundation's ``IdentityService.login``)."""
        eff = self.local_login_enabled()
        self.ctx.settings.auth.local_login_enabled = eff
        return eff

    def lockout_check(self) -> tuple[bool, list[str]]:
        """May local login be disabled? Needs >=1 active federated PlatformAdmin whose provider is enabled and allows interactive login."""
        reasons: list[str] = []
        usable = {p.name for p in self.providers() if p.allow_login_enabled()}
        if not usable:
            reasons.append("no enabled identity provider allows interactive login")
        pt, rb = S.principals.c, S.role_bindings.c
        stmt = (select(S.principals.c.id, S.principals.c.name, S.principals.c.source)
                .join(S.role_bindings, rb.principal_id == pt.id)
                .where(rb.role == "PlatformAdmin", rb.scope_type == "org", pt.is_active.is_(True), (pt.source.like("oidc:%")) | (pt.source.like("ldap:%"))))
        with self.ctx.db.connect() as conn:
            admins = fetch_all(conn, stmt)
        ok_admins = [a for a in admins if a["source"].split(":", 1)[1] in usable]
        if not ok_admins:
            reasons.append("no active federated PlatformAdmin exists (log in through the identity provider at least once with an administrator mapping first)")
        return (not reasons), reasons

    def set_local_login(self, enabled: bool, principal: Principal) -> dict[str, Any]:
        if not enabled:
            ok, reasons = self.lockout_check()
            if not ok:
                raise Conflict("refusing to disable local login: it would lock the organisation out", details={"reasons": reasons},
                               remediation="configure a provider, sign in with a federated administrator, then retry")
        with self.ctx.db.begin() as conn:
            n = conn.execute(update(S.kv).where(S.kv.c.key == KV_LOCAL_LOGIN).values(value=enabled, updated_at=now_iso())).rowcount
            if not n:
                conn.execute(S.kv.insert().values(key=KV_LOCAL_LOGIN, value=enabled, updated_at=now_iso()))
        eff = self.apply_local_login_flag()
        self.ctx.audit.record(principal, "config.changed", resource_type="setting", resource_id="identity.local_login", details={"enabled": enabled, "effective": eff})
        return {"enabled": enabled, "effective": eff, "break_glass_env": BREAKGLASS_ENV}

    # ================================================================== principals + bindings
    def _resolve_ws(self, ref: str) -> str | None:
        try:
            return self.ctx.service("workspaces").resolve(ref)["id"]
        except OlcfError:
            return None

    def _find_principal(self, source: str, subject: str) -> dict[str, Any] | None:
        t = S.principals.c
        with self.ctx.db.connect() as conn:
            return fetch_one(conn, select(S.principals).where(t.source == source, t.external_id == subject))

    def _provision(self, source: str, subject: str, *, kind: str, name: str, email: str | None, display_name: str | None, attrs: dict[str, Any],
                   create: bool) -> dict[str, Any] | None:
        """Find the principal for (source, subject) or create it. Never matches by name; a name clash gets a subject-derived suffix."""
        row = self._find_principal(source, subject)
        now = now_iso()
        if row is not None:
            changes: dict[str, Any] = {}
            if email and row.get("email") != email:
                changes["email"] = email
            if display_name and row.get("display_name") != display_name:
                changes["display_name"] = display_name
            merged = {**(row.get("attrs") or {}), **attrs}
            if merged != (row.get("attrs") or {}):
                changes["attrs"] = merged
            if changes:
                with self.ctx.db.begin() as conn:
                    conn.execute(update(S.principals).where(S.principals.c.id == row["id"]).values(**changes, updated_at=now))
                row = {**row, **changes}
            return row
        if not create:
            return None
        ident = self.ctx.service("identity")
        suffix = ""
        for attempt in range(3):
            try:
                return ident.create_principal(f"{name}{suffix}", kind=kind, display_name=display_name or name, email=email, source=source,
                                              external_id=subject, attrs=attrs)
            except Conflict:
                again = self._find_principal(source, subject)  # a concurrent first login created it
                if again is not None:
                    return again
                suffix = "#" + O.sha256_hex(f"{subject}{attempt}")[:8]
            except Exception:  # noqa: BLE001 - unique-name race surfaces as a DB error
                again = self._find_principal(source, subject)
                if again is not None:
                    return again
                suffix = "#" + O.sha256_hex(f"{subject}{attempt}")[:8]
        raise Conflict("could not create the federated principal")

    def _sync(self, row: dict[str, Any], source: str, bindings: list[M.Binding], extra_hash: list[str] | None = None) -> None:
        h = M.mapping_hash(bindings, extra_hash)
        fed = (row.get("attrs") or {}).get("federation") or {}
        if fed.get("mapping_hash") == h:
            return
        self.ctx.service("identity").sync_bindings(row["id"], source, bindings)
        attrs = {**(row.get("attrs") or {}), "federation": {**fed, "mapping_hash": h, "synced_at": now_iso()}}
        with self.ctx.db.begin() as conn:
            conn.execute(update(S.principals).where(S.principals.c.id == row["id"]).values(attrs=attrs))
        row["attrs"] = attrs
        self.ctx.audit.record(None, "identity.binding_changed", resource_type="principal", resource_id=row["id"],
                              details={"op": "federation_sync", "source": source, "bindings": [list(b) for b in bindings]})

    @staticmethod
    def _overage(claims: dict[str, Any]) -> bool:
        names = claims.get("_claim_names")
        return bool(claims.get("hasgroups")) or (isinstance(names, dict) and "groups" in names)

    @staticmethod
    def _safe_claims(claims: dict[str, Any]) -> dict[str, Any]:
        keep = ("iss", "sub", "oid", "tid", "azp", "appid", "aud", "exp", "iat", "idtyp", "scp", "roles", "preferred_username", "upn", "email", "name")
        return {k: claims[k] for k in keep if k in claims}

    # ================================================================== bearer validation
    def authenticate_bearer(self, token: str) -> Principal | None:
        if not token or token.count(".") != 2 or len(token) > 16384:
            return None
        try:
            claims = jwt.decode(token, options={"verify_signature": False})
        except jwt.PyJWTError:
            return None
        iss = claims.get("iss")
        if not isinstance(iss, str):
            return None
        cands = [p for p in self.providers() if p.type in ("oidc", "entra") and p.cfg("allow_bearer", True) and p.issuer_matches(iss, claims)]
        if not cands:
            return None  # unknown issuer: never cause any outbound request
        cache_key = O.sha256_hex(token)
        hit = self._val_cache.get(cache_key)
        now = time.monotonic()
        last: Exception | None = None
        for spec in cands:
            try:
                if hit and hit[0] > now and hit[1]["provider"] == spec.name:
                    info = hit[1]
                else:
                    v = self.validator.validate_access_token(spec, token)
                    info = self._map_access_token(spec, v.claims)
                    if info is None:
                        last = O.TokenRejected("no_mapping", "identity has no role mapping")
                        continue
                    exp = float(v.claims.get("exp", 0)) - time.time()
                    self._val_cache[cache_key] = (now + max(0.0, min(self.bearer_cache_seconds, exp)), info)
                    if len(self._val_cache) > 5000:
                        self._val_cache = {k: v2 for k, v2 in self._val_cache.items() if v2[0] > now}
                with self.ctx.db.connect() as conn:
                    prow = fetch_one(conn, select(S.principals).where(S.principals.c.id == info["principal_id"]))
                if not prow or not prow["is_active"]:
                    return None
                return self.ctx.authz.principal_from_row(prow, token_scopes=info["token_scopes"], claims=info["claims"])
            except O.TokenRejected as exc:
                last = exc
            except OlcfError as exc:  # IdP unreachable etc.: cannot validate -> treat as unauthenticated, but log the cause
                last = exc
                log.warning("bearer validation against provider %s failed: %s", spec.name, exc.message)
        self._log_failure(iss, last)
        return None

    def _log_failure(self, iss: str, err: Exception | None) -> None:
        reason = getattr(err, "reason", type(err).__name__ if err else "unknown")
        key = f"{iss}|{reason}"
        now = time.monotonic()
        if now - self._fail_throttle.get(key, 0.0) < 60.0:
            return
        if len(self._fail_throttle) > 1000:
            self._fail_throttle.clear()
        self._fail_throttle[key] = now
        self.ctx.audit.record(None, "auth.login_failed", resource_type="federated_token", outcome="failure", details={"issuer": iss[:200], "reason": reason})

    def _map_access_token(self, spec: O.ProviderSpec, claims: dict[str, Any]) -> dict[str, Any] | None:
        cfg = self.parsed(spec)
        service = M.is_service_token(claims)
        sp = M.find_service_mapping(cfg, claims) if service else None
        subject = str((claims.get("oid") or claims.get("sub")) if service else (claims.get(spec.subject_claim) or claims.get("sub")) or "")
        if not subject:
            return None
        overage = self._overage(claims)
        flat = dict(claims)
        if cfg.groups_claim != "groups" and cfg.groups_claim in claims:
            flat["groups"] = claims[cfg.groups_claim]
        if cfg.roles_claim != "roles" and cfg.roles_claim in claims:
            flat["roles"] = claims[cfg.roles_claim]
        if overage and cfg.deny_on_overage and any((r.claim or "") == "groups" for r in cfg.role_mappings):
            raise O.TokenRejected("group_overage", "token has too many groups to be carried in the JWT (group overage)")
        bindings, skipped = M.evaluate(cfg, flat, self._resolve_ws, service_match=sp)
        if skipped:
            log.info("mapping skipped for provider %s: %s", spec.name, skipped)
        if service and sp is None and not bindings:
            return None  # fail closed: unknown application
        if not bindings and not cfg.allow_login_without_roles:
            return None
        # delegated scopes (scp) may narrow the token
        token_scopes = ["*"]
        scp = M.claim_values(claims, "scp")
        if service and sp is not None and sp.token_scopes:
            token_scopes = list(sp.token_scopes)
        elif scp and cfg.scope_map:
            token_scopes = sorted({cfg.scope_map[s] for s in scp if s in cfg.scope_map})
            if not token_scopes:
                return None  # token carries scopes, none of which grants anything here
        source = f"oidc:{spec.name}"
        if service:
            label = (sp.name if sp and sp.name else None) or str(claims.get("azp") or claims.get("appid") or subject)
            name, kind = M.sanitize_name(f"sp:{spec.name}:{label}"), "service"
            display, email = label, None
        else:
            uname = str(claims.get(cfg.username_claim or "") or claims.get("preferred_username") or claims.get("upn") or claims.get("email") or subject)
            name, kind = M.sanitize_name(f"{spec.name}:{uname}"), "user"
            display, email = claims.get(cfg.display_name_claim), claims.get(cfg.email_claim)
        attrs = {"federation": {"provider": spec.name, "subject": subject, "tenant": claims.get("tid"), "app_id": claims.get("azp") or claims.get("appid"),
                                "groups_overage": overage}}
        create = cfg.auto_provision if not service else True  # a service principal is only reached after an explicit mapping matched
        row = self._provision(source, subject, kind=kind, name=name, email=email if isinstance(email, str) else None,
                              display_name=display if isinstance(display, str) else None, attrs=attrs, create=create)
        if row is None:
            return None
        if not row["is_active"]:
            return None
        self._sync(row, source, bindings, token_scopes)
        return {"principal_id": row["id"], "token_scopes": token_scopes, "claims": self._safe_claims(claims), "provider": spec.name}

    # ================================================================== interactive login: begin
    def redirect_uri(self, spec: O.ProviderSpec) -> str:
        return f"{self.ctx.settings.base_url}/v1/identity/oidc/{quote(spec.name)}/callback"

    def allowed_return_origins(self, spec: O.ProviderSpec) -> set[str]:
        s = self.ctx.settings
        origins = {_origin(s.base_url)}
        origins.update(_origin(o) for o in s.server.cors_origins)
        origins.update(_origin(o) for o in spec.cfg("allowed_return_origins", []) or [])
        return origins

    def safe_return_url(self, value: str | None, spec: O.ProviderSpec) -> str:
        """Open-redirect defence: same-site relative path, or an absolute URL whose origin is explicitly allowed. Anything else -> BadRequest."""
        if not value:
            return "/"
        v = value.strip()
        if len(v) > 1024 or any(ord(c) < 32 or ord(c) == 127 for c in v) or "\\" in v:
            raise BadRequest("redirect_to is not acceptable")
        from urllib.parse import unquote

        decoded = unquote(v)
        if "\\" in decoded or any(ord(c) < 32 for c in decoded):
            raise BadRequest("redirect_to is not acceptable")
        if v.startswith("/"):
            if v.startswith("//") or decoded.startswith("//") or decoded.startswith("/\\"):
                raise BadRequest("redirect_to must be a same-site path")
            parts = urlsplit(v)
            if parts.scheme or parts.netloc:
                raise BadRequest("redirect_to must be a same-site path")
            return urlunsplit(("", "", parts.path, parts.query, ""))
        parts = urlsplit(v)
        if parts.scheme not in ("http", "https") or not parts.netloc or parts.username or parts.password:
            raise BadRequest("redirect_to must be a path or an allowed origin")
        if _origin(v) not in self.allowed_return_origins(spec):
            raise BadRequest("redirect_to origin is not allowed", remediation="add it to the provider's allowed_return_origins")
        return urlunsplit((parts.scheme, parts.netloc, parts.path or "/", parts.query, ""))

    def begin_login(self, provider: str, redirect_to: str | None, ui_challenge: str | None, ip: str | None) -> LoginRedirect:
        ctx = self.ctx
        ctx.limiter.check(f"oidc-login:{ip}", limit=30)
        spec = self.spec_by_name(provider)
        cfg = self.parsed(spec)
        if spec.type not in ("oidc", "entra") or not cfg.allow_login:
            raise NotFound(f"identity provider '{provider}' does not support interactive login")
        if ui_challenge is not None and not (43 <= len(ui_challenge) <= 128):
            raise BadRequest("ui_challenge must be a base64url S256 challenge")
        return_to = self.safe_return_url(redirect_to, spec)
        disc = self.discovery.get(spec)
        auth_ep = spec.cfg("authorization_endpoint") or disc.endpoint("authorization_endpoint")
        if not auth_ep:
            raise Unavailable("the provider's discovery document has no authorization_endpoint")
        O.validate_endpoint_url(str(auth_ep), allow_http=spec.allow_http, what="authorization_endpoint")
        state, nonce, binding = O.random_token(32), O.random_token(24), O.random_token(32)
        verifier, challenge = O.new_pkce()
        from olcf.security.secrets import seal

        blob, kid = seal(ctx.secrets.master, verifier.encode()) if ctx.secrets.master else (verifier.encode(), "plain")
        now = now_iso()
        with ctx.db.begin() as conn:
            conn.execute(SEC.oidc_auth_requests.delete().where(SEC.oidc_auth_requests.c.expires_at < iso_in(-3600)))
            pending = conn.execute(select(SEC.oidc_auth_requests.c.state_hash).where(SEC.oidc_auth_requests.c.consumed_at.is_(None),
                                                                                    SEC.oidc_auth_requests.c.expires_at > now).limit(20001)).all()
            if len(pending) > 20000:
                raise TooManyRequests("too many pending logins")
            conn.execute(SEC.oidc_auth_requests.insert().values(
                state_hash=O.sha256_hex(state), provider_id=spec.id, nonce=nonce, verifier_enc=blob, verifier_kid=kid, binding_hash=O.sha256_hex(binding),
                redirect_to=return_to, ui_challenge=ui_challenge, created_at=now, expires_at=iso_in(LOGIN_REQUEST_TTL)))
        params = {"response_type": "code", "client_id": spec.client_id or "", "redirect_uri": self.redirect_uri(spec), "scope": " ".join(spec.scopes),
                  "state": state, "nonce": nonce, "code_challenge": challenge, "code_challenge_method": "S256"}
        if spec.is_entra:
            params["response_mode"] = "query"
        sep = "&" if "?" in str(auth_ep) else "?"
        return LoginRedirect(f"{auth_ep}{sep}{urlencode(params)}", self.cookie_name(state), binding)

    @staticmethod
    def cookie_name(state: str) -> str:
        return "olcf_oidc_" + O.sha256_hex(state)[:16]

    # ================================================================== interactive login: callback
    def _consume_request(self, state: str) -> dict[str, Any] | None:
        h = O.sha256_hex(state)
        t = SEC.oidc_auth_requests.c
        with self.ctx.db.begin() as conn:
            row = fetch_one(conn, select(SEC.oidc_auth_requests).where(t.state_hash == h))
            if not row:
                return None
            n = conn.execute(update(SEC.oidc_auth_requests).where(t.state_hash == h, t.consumed_at.is_(None)).values(consumed_at=now_iso())).rowcount
            if n != 1:
                return None  # replay
        return row

    def _fail_redirect(self, return_to: str, code: str) -> str:
        parts = urlsplit(return_to)
        return urlunsplit((parts.scheme, parts.netloc, parts.path or "/", parts.query, urlencode({"olcf_login_error": code})))

    def complete_login(self, provider: str, *, code: str | None, state: str | None, error: str | None, binding: str | None, ip: str | None) -> LoginRedirect:
        ctx = self.ctx
        ctx.limiter.check(f"oidc-callback:{ip}", limit=60)
        if not state or len(state) > 256:
            raise BadRequest("missing or malformed state")
        row = self._consume_request(state)
        cname = self.cookie_name(state)
        if row is None:
            ctx.audit.record(None, "auth.login_failed", resource_type="identity_provider", resource_id=provider, outcome="failure", ip=ip,
                             details={"reason": "unknown_or_replayed_state"})
            raise BadRequest("the login request is unknown, expired or was already used", remediation="start the sign-in again")
        spec = self.spec_by_name(provider, include_disabled=True)
        if row["provider_id"] != spec.id or from_iso(row["expires_at"]).timestamp() < time.time():
            raise BadRequest("the login request does not belong to this provider or has expired")
        if not binding or not hmac.compare_digest(O.sha256_hex(binding), row["binding_hash"]):
            ctx.audit.record(None, "auth.login_failed", resource_type="identity_provider", resource_id=spec.name, outcome="failure", ip=ip,
                             details={"reason": "browser_binding_mismatch"})
            raise BadRequest("the login was not started in this browser", remediation="start the sign-in again from the OLCF login page")
        return_to = row["redirect_to"]
        if not spec.enabled:
            return LoginRedirect(self._fail_redirect(return_to, "provider_disabled"), cname, None)
        if error:
            safe = error if error in ("access_denied", "login_required", "consent_required", "interaction_required") else "idp_error"
            ctx.audit.record(None, "auth.login_failed", resource_type="identity_provider", resource_id=spec.name, outcome="failure", ip=ip, details={"reason": safe})
            return LoginRedirect(self._fail_redirect(return_to, safe), cname, None)
        if not code or len(code) > 4096:
            return LoginRedirect(self._fail_redirect(return_to, "missing_code"), cname, None)
        try:
            principal_row = self._login_with_code(spec, row, code, ip)
        except O.TokenRejected as exc:
            ctx.audit.record(None, "auth.login_failed", resource_type="identity_provider", resource_id=spec.name, outcome="failure", ip=ip, details={"reason": exc.reason})
            return LoginRedirect(self._fail_redirect(return_to, exc.reason), cname, None)
        except OlcfError as exc:
            log.warning("login via %s failed: %s", spec.name, exc.message)
            ctx.audit.record(None, "auth.login_failed", resource_type="identity_provider", resource_id=spec.name, outcome="failure", ip=ip, details={"reason": exc.code})
            return LoginRedirect(self._fail_redirect(return_to, exc.code), cname, None)
        one_time = O.random_token(32)
        with ctx.db.begin() as conn:
            conn.execute(SEC.oidc_login_codes.delete().where(SEC.oidc_login_codes.c.expires_at < iso_in(-3600)))
            conn.execute(SEC.oidc_login_codes.insert().values(code_hash=O.sha256_hex(one_time), principal_id=principal_row["id"], provider_id=spec.id,
                                                              ui_challenge=row.get("ui_challenge"), created_at=now_iso(), expires_at=iso_in(LOGIN_CODE_TTL)))
        parts = urlsplit(return_to)
        frag = urlencode({"olcf_login_code": one_time, "provider": spec.name})
        return LoginRedirect(urlunsplit((parts.scheme, parts.netloc, parts.path or "/", parts.query, frag)), cname, None)

    def _login_with_code(self, spec: O.ProviderSpec, req: dict[str, Any], code: str, ip: str | None) -> dict[str, Any]:
        ctx = self.ctx
        cfg = self.parsed(spec)
        disc = self.discovery.get(spec)
        token_ep = spec.cfg("token_endpoint") or disc.endpoint("token_endpoint")
        if not token_ep:
            raise Unavailable("the provider's discovery document has no token_endpoint")
        from olcf.security.secrets import unseal

        verifier = (unseal(ctx.secrets.master, req["verifier_enc"], req["verifier_kid"]) if req["verifier_kid"] != "plain" else req["verifier_enc"]).decode()
        resp = O.post_token(self.http, spec, str(token_ep),
                            {"grant_type": "authorization_code", "code": code, "redirect_uri": self.redirect_uri(spec), "code_verifier": verifier},
                            self.client_secret(spec))
        id_token = resp.get("id_token")
        if not isinstance(id_token, str):
            raise O.TokenRejected("no_id_token", "the provider returned no id_token")
        access = resp.get("access_token") if isinstance(resp.get("access_token"), str) else None
        v = self.validator.validate_id_token(spec, id_token, nonce=req["nonce"], access_token=access)
        claims = dict(v.claims)
        if cfg.require_email_verified and claims.get("email_verified") is not True:
            raise O.TokenRejected("email_not_verified", "the provider did not verify the e-mail address")
        overage = self._overage(claims)
        if overage and cfg.resolve_group_overage and access:
            claims["groups"] = self._graph_groups(spec, cfg, access)
        elif overage and cfg.deny_on_overage:
            raise O.TokenRejected("group_overage", "too many groups for the token (group overage)")
        if cfg.groups_claim != "groups" and cfg.groups_claim in claims:
            claims["groups"] = claims[cfg.groups_claim]
        if cfg.roles_claim != "roles" and cfg.roles_claim in claims:
            claims["roles"] = claims[cfg.roles_claim]
        bindings, skipped = M.evaluate(cfg, claims, self._resolve_ws)
        if skipped:
            log.info("mapping skipped for provider %s: %s", spec.name, skipped)
        if not bindings and not cfg.allow_login_without_roles:
            raise O.TokenRejected("no_role_mapping", "your identity has no role mapping in OLCF")
        subject = str(claims.get(spec.subject_claim) or claims.get("sub"))
        uname = str(claims.get(cfg.username_claim or "") or claims.get("preferred_username") or claims.get("upn") or claims.get("email") or subject)
        email, display = claims.get(cfg.email_claim), claims.get(cfg.display_name_claim)
        source = f"oidc:{spec.name}"
        attrs = {"federation": {"provider": spec.name, "subject": subject, "tenant": claims.get("tid"), "groups_overage": overage}}
        row = self._provision(source, subject, kind="user", name=M.sanitize_name(f"{spec.name}:{uname}"), email=email if isinstance(email, str) else None,
                              display_name=display if isinstance(display, str) else None, attrs=attrs, create=cfg.auto_provision)
        if row is None:
            raise O.TokenRejected("not_provisioned", "automatic provisioning is disabled and the account does not exist")
        if not row["is_active"]:
            raise O.TokenRejected("account_disabled", "the account is disabled")
        self._sync(row, source, bindings)
        with ctx.db.begin() as conn:
            conn.execute(update(S.principals).where(S.principals.c.id == row["id"]).values(last_login_at=now_iso()))
        p = ctx.authz.principal_from_row(row)
        p.ip = ip
        ctx.audit.record(p, "auth.login", resource_type="principal", resource_id=row["id"], ip=ip, details={"provider": spec.name, "method": "oidc"})
        return row

    def _graph_groups(self, spec: O.ProviderSpec, cfg: M.ProviderConfig, access_token: str) -> list[str]:
        """Resolve Entra group overage server side via Microsoft Graph with the user's own access token (needs scope ``User.Read``)."""
        base = (cfg.graph_base_url or "https://graph.microsoft.com").rstrip("/")
        url: str | None = f"{base}/v1.0/me/transitiveMemberOf/microsoft.graph.group?$select=id&$top=999"
        out: list[str] = []
        host = urlsplit(base).netloc
        for _ in range(10):
            if not url:
                break
            if urlsplit(url).netloc != host:  # never follow a nextLink to another host with the user's token
                raise O.TokenRejected("graph_error", "Graph pagination left the configured host")
            data, _ = self.http.get_json(url, allow_http=spec.allow_http, what="Microsoft Graph", headers={"Authorization": f"Bearer {access_token}"})
            out.extend(str(g.get("id")) for g in data.get("value", []) if g.get("id"))
            url = data.get("@odata.nextLink")
        return out[:5000]

    # ================================================================== one-time code exchange
    def exchange_code(self, code: str, ui_verifier: str | None, ip: str | None) -> dict[str, Any]:
        ctx = self.ctx
        ctx.limiter.check(f"oidc-exchange:{ip}", limit=30)
        h = O.sha256_hex(code)
        t = SEC.oidc_login_codes.c
        with ctx.db.connect() as conn:
            row = fetch_one(conn, select(SEC.oidc_login_codes).where(t.code_hash == h))
        bad = Unauthenticated("the login code is invalid, expired or was already used")
        if not row or row["consumed_at"] or from_iso(row["expires_at"]).timestamp() < time.time():
            raise bad
        if row["ui_challenge"]:
            if not ui_verifier or not hmac.compare_digest(O.pkce_challenge(ui_verifier), row["ui_challenge"]):
                raise bad  # the code is NOT burned: an interceptor without the verifier must not be able to invalidate the victim's code
        with ctx.db.begin() as conn:
            n = conn.execute(update(SEC.oidc_login_codes).where(t.code_hash == h, t.consumed_at.is_(None)).values(consumed_at=now_iso())).rowcount
        if n != 1:
            raise bad
        prow = ctx.service("identity").get(row["principal_id"])
        if not prow or not prow["is_active"]:
            raise Unauthenticated("principal disabled")
        spec = next((p for p in self.providers(include_disabled=True) if p.id == row["provider_id"]), None)
        return self._issue_tokens(prow, spec, ip)

    def _issue_tokens(self, prow: dict[str, Any], spec: O.ProviderSpec | None, ip: str | None) -> dict[str, Any]:
        from olcf.api.core.identity import public_principal

        ctx = self.ctx
        access, ttl = ctx.tokens.issue_access_token(prow)
        refresh, rec = ctx.tokens.create_refresh(prow["id"], meta={"ip": ip, "idp": spec.name if spec else None})
        ttl_cfg = spec.cfg("session_ttl_seconds") if spec else None
        if ttl_cfg:
            with ctx.db.begin() as conn:
                conn.execute(update(S.api_tokens).where(S.api_tokens.c.id == rec["id"]).values(expires_at=iso_in(min(int(ttl_cfg), ctx.tokens.refresh_ttl))))
        return {"access_token": access, "token_type": "Bearer", "expires_in": ttl, "refresh_token": refresh, "must_change_password": False,
                "principal": public_principal(prow)}

    # ================================================================== LDAP login
    def ldap_login(self, provider: str, username: str, password: str, ip: str | None) -> dict[str, Any]:
        from olcf.security.ldap_bridge import LdapBridge

        ctx = self.ctx
        ctx.limiter.check(f"ldap-login:{ip}", limit=ctx.settings.auth.login_rate_limit_per_minute)
        ctx.limiter.check(f"ldap-login-user:{provider}:{username.lower()}", limit=10)
        spec = self.spec_by_name(provider)
        if spec.type != "ldap":
            raise NotFound(f"identity provider '{provider}' is not an LDAP provider")
        cfg = self.parsed(spec)
        if cfg.ldap is None or not cfg.allow_login:
            raise NotFound("LDAP login is not configured for this provider")
        bridge = LdapBridge(cfg.ldap, self.client_secret(spec))
        try:
            ident = bridge.authenticate(username, password)
        except Unauthenticated:
            ctx.audit.record(None, "auth.login_failed", resource_type="identity_provider", resource_id=spec.name, outcome="failure", ip=ip,
                             details={"reason": "bad credentials", "method": "ldap"})
            raise
        claims = {"groups": ident.groups, "username": ident.username, "email": ident.email}
        bindings, _ = M.evaluate(cfg, claims, self._resolve_ws, extra_rules=cfg.ldap.group_rules)
        if not bindings and not cfg.allow_login_without_roles:
            ctx.audit.record(None, "auth.login_failed", resource_type="identity_provider", resource_id=spec.name, outcome="failure", ip=ip,
                             details={"reason": "no_role_mapping", "method": "ldap"})
            raise Forbidden("your directory groups have no role mapping in OLCF")
        source = f"ldap:{spec.name}"
        row = self._provision(source, ident.external_id, kind="user", name=M.sanitize_name(f"{spec.name}:{ident.username}"), email=ident.email,
                              display_name=ident.display_name, attrs={"federation": {"provider": spec.name, "dn": ident.dn}}, create=cfg.auto_provision)
        if row is None or not row["is_active"]:
            raise Forbidden("the account is not provisioned or is disabled")
        self._sync(row, source, bindings)
        with ctx.db.begin() as conn:
            conn.execute(update(S.principals).where(S.principals.c.id == row["id"]).values(last_login_at=now_iso()))
        p = ctx.authz.principal_from_row(row)
        p.ip = ip
        ctx.audit.record(p, "auth.login", resource_type="principal", resource_id=row["id"], ip=ip, details={"provider": spec.name, "method": "ldap"})
        return self._issue_tokens(row, spec, ip)

    # ================================================================== provider test
    def test_provider(self, spec: O.ProviderSpec, *, client_credentials: bool = False, scope: str | None = None, persist: bool = True) -> dict[str, Any]:
        steps: list[dict[str, Any]] = []

        def step(name: str, fn: Any) -> Any:
            t0 = time.perf_counter()
            try:
                out = fn()
                steps.append({"name": name, "ok": True, "detail": out if isinstance(out, str) else "ok", "ms": round((time.perf_counter() - t0) * 1000, 1)})
                return out
            except Exception as exc:  # noqa: BLE001
                msg = getattr(exc, "message", None) or str(exc)
                rem = getattr(exc, "remediation", None)
                steps.append({"name": name, "ok": False, "detail": f"{type(exc).__name__}: {msg}", "remediation": rem,
                              "ms": round((time.perf_counter() - t0) * 1000, 1)})
                return None

        cfg = self.parsed(spec)
        if spec.type == "ldap":
            from olcf.security.ldap_bridge import LdapBridge

            def ldap_steps() -> str:
                if cfg.ldap is None:
                    raise BadRequest("ldap block missing from config")
                bridge = LdapBridge(cfg.ldap, self.client_secret(spec))
                steps.extend(bridge.test())
                return "ok"

            try:
                ldap_steps()
            except Exception as exc:  # noqa: BLE001
                steps.append({"name": "ldap", "ok": False, "detail": f"{type(exc).__name__}: {getattr(exc, 'message', exc)}", "ms": 0})
        else:
            disc = step("discovery", lambda: (self.discovery.get(spec, force=True), f"issuer {spec.issuer or spec.tenant}")[1])
            if disc is not None:
                d = self.discovery.get(spec)

                def jwks_step() -> str:
                    uri = str(spec.cfg("jwks_uri") or d.endpoint("jwks_uri"))
                    ks = self.jwks.get(uri, allow_http=spec.allow_http, force=True)
                    n = len(ks.keys) + len(ks.anonymous)
                    if n == 0:
                        raise Unavailable("the JWKS contains no usable signing keys")
                    return f"{n} signing key(s): {', '.join(sorted(ks.keys))[:200]}"

                step("jwks", jwks_step)
                step("endpoints", lambda: "authorization_endpoint=" + ("yes" if d.endpoint("authorization_endpoint") else "no") + ", token_endpoint=" + ("yes" if d.endpoint("token_endpoint") else "no"))
                if client_credentials:
                    def cc() -> str:
                        secret = self.client_secret(spec)
                        if not secret:
                            raise BadRequest("no client secret is stored for this provider")
                        sc = scope or (f"{spec.client_id}/.default" if spec.is_entra else "")
                        form = {"grant_type": "client_credentials"}
                        if sc:
                            form["scope"] = sc
                        resp = O.post_token(self.http, spec, str(spec.cfg("token_endpoint") or d.endpoint("token_endpoint")), form, secret)
                        tok = resp.get("access_token")
                        if not isinstance(tok, str):
                            raise O.TokenRejected("no_access_token", "no access_token in the response")
                        info = f"client credentials accepted; expires_in={resp.get('expires_in')}"
                        if tok.count(".") == 2:
                            _h, claims = self.validator._verify_signature(spec, tok)  # noqa: SLF001 - signature + expiry only, audience may be another API
                            info += f"; JWT signature verified (aud={claims.get('aud')})"
                        return info

                    step("client_credentials", cc)
        ok = all(s["ok"] for s in steps) and bool(steps)
        result = {"ok": ok, "at": now_iso(), "steps": steps, "provider": spec.name}
        if persist:
            with self.ctx.db.begin() as conn:
                conn.execute(update(SEC.identity_providers).where(SEC.identity_providers.c.id == spec.id).values(last_test=result))
            self.invalidate_providers_only()
        return result

    def invalidate_providers_only(self) -> None:
        with self._plock:
            self._providers = None

    # ================================================================== health
    def health_quick(self) -> ComponentHealth:
        with self.ctx.db.connect() as conn:
            rows = fetch_all(conn, select(SEC.identity_providers))
        enabled = [r for r in rows if r["enabled"]]
        local = self.local_login_enabled()
        labels = {"providers": len(rows), "enabled": len(enabled), "local_login": local}
        if not enabled:
            return ComponentHealth("identity-providers", Status.OK, "no external identity provider configured (local accounts only)", labels=labels)
        bad = [r["name"] for r in enabled if r.get("last_test") and not r["last_test"].get("ok")]
        untested = [r["name"] for r in enabled if not r.get("last_test")]
        if bad:
            return ComponentHealth("identity-providers", Status.DEGRADED if local else Status.DOWN, f"provider test failing: {', '.join(bad)}", labels={**labels, "failing": bad})
        if untested:
            return ComponentHealth("identity-providers", Status.UNKNOWN, f"provider(s) not tested yet: {', '.join(untested)}", labels={**labels, "untested": untested})
        return ComponentHealth("identity-providers", Status.OK, f"{len(enabled)} provider(s) healthy at last test", labels=labels)

    def health_deep(self) -> ComponentHealth:
        steps: list[dict[str, Any]] = []
        bad = []
        for spec in self.providers():
            res = self.test_provider(spec)
            steps.append({"name": spec.name, "ok": res["ok"], "steps": res["steps"]})
            if not res["ok"]:
                bad.append(spec.name)
        if not steps:
            return ComponentHealth("identity-providers", Status.OK, "no external identity provider configured", steps=[])
        local = self.local_login_enabled()
        st = Status.OK if not bad else (Status.DEGRADED if local else Status.DOWN)
        return ComponentHealth("identity-providers", st, "all providers reachable" if not bad else f"failing: {', '.join(bad)}", steps=steps)


class FederationModule(Module):
    """Module that registers the external identity provider (OIDC, Entra, LDAP) routes and service."""
    name = "federation"

    def __init__(self, ctx: AppContext, svc: FederationService) -> None:
        super().__init__()
        from olcf.security import federation_api

        self.ctx, self.svc = ctx, svc
        self.routers = [(federation_api.router, "")]
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="olcf-federation", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _run(self) -> None:
        interval = float(os.environ.get("OLCF_FEDERATION_TICK_SECONDS", "10"))
        while not self._stop.wait(interval):
            try:
                self.svc.apply_local_login_flag()
                if self.ctx.leader.is_leader():
                    self.tick_provider_tests()
            except Exception:  # noqa: BLE001
                log.exception("federation housekeeping failed")

    def tick_provider_tests(self) -> int:
        """Leader only: keep ``last_test`` fresh so the ``identity-providers`` health check reflects reality without anyone clicking Test."""
        every = float(self.ctx.maybe_service("settings").get("identity.provider_test_interval_seconds") or 600) if self.ctx.maybe_service("settings") else 600.0
        n = 0
        for spec in self.svc.providers():
            with self.ctx.db.connect() as conn:
                row = fetch_one(conn, select(SEC.identity_providers.c.last_test).where(SEC.identity_providers.c.id == spec.id))
            at = (row or {}).get("last_test") or {}
            if at.get("at") and time.time() - from_iso(at["at"]).timestamp() < every:
                continue
            self.svc.test_provider(spec)
            n += 1
        return n


def install(ctx: AppContext) -> FederationModule:
    svc = FederationService(ctx)
    ctx.register_service("federation", svc)
    settings = ctx.maybe_service("settings")
    if settings is not None:
        settings.register("identity.provider_test_interval_seconds", description="Seconds between automatic identity provider tests (leader)", default=600,
                          validator=lambda v: max(30, int(v)))
    ctx.health.register(HealthCheck("identity-providers", "security", "External identity providers (OIDC / Entra / LDAP): discovery, JWKS, optional client credentials",
                                    svc.health_quick, svc.health_deep, ttl=15))
    try:
        svc.apply_local_login_flag()
    except Exception:  # noqa: BLE001 - tables may not exist yet when the context was built without migrations
        log.debug("local login flag not applied at install", exc_info=True)
    return FederationModule(ctx, svc)
