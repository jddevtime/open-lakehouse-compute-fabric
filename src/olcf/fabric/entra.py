# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Microsoft Entra ID (v2.0 and v1.0 tokens): inbound JWT validation and outbound client-credentials token acquisition.

Inbound - :class:`EntraValidator`
    Validates a bearer token that a Fabric notebook obtained for the OLCF API registration:

    * the header ``alg`` must be ``RS256`` (``none`` / HMAC are rejected before any key lookup);
    * the ``tid`` claim must be in the **tenant allow-list** (an empty list rejects everything - fail closed) and ``iss`` must be exactly the v2.0 or v1.0 issuer *of that
      tenant*;
    * the signing key comes from the tenant's **JWKS** (cached; an unknown ``kid`` triggers one rate-limited refresh, so key rotation works without restarts);
    * ``aud`` must be one of the configured audiences (an App ID URI ``api://<guid>`` and the bare client id are treated as equivalent);
    * ``exp`` / ``nbf`` / ``iat`` are checked with a clock-skew leeway;
    * claims ``tid`` / ``oid`` / ``sub`` / ``appid`` (v1) or ``azp`` (v2) / ``roles`` / ``scp`` / ``upn`` are returned as :class:`EntraIdentity`.

Outbound - :class:`TokenBroker`
    Acquires app-only tokens with the **broker** identity (a service principal stored as an OLCF ``onelake`` connection secret): client secret or certificate
    (``private_key_pem`` + ``certificate_pem`` -> JWT client assertion). Tokens are cached until shortly before expiry. Errors are translated to the AADSTS code and a
    remediation. Secrets never appear in messages or logs.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
import threading
import time
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

import httpx
import jwt
from cryptography import x509
from cryptography.hazmat.primitives import serialization

from olcf.fabric.errors import TokenInvalid
from olcf.fabric.http import Network, make_client

_GUID = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
V1_ISSUER = "https://sts.windows.net/{tid}/"


@dataclass(frozen=True)
class EntraIdentity:
    """What a validated Entra token says about the caller (claims only; the platform decides what that identity may do)."""

    tenant_id: str
    object_id: str
    subject: str
    app_id: str | None
    roles: tuple[str, ...]
    scopes: tuple[str, ...]
    issuer: str
    audience: str
    version: str
    upn: str | None = None
    name: str | None = None
    expires_at: float = 0.0
    raw: dict[str, Any] = field(default_factory=dict, compare=False, repr=False)

    @property
    def is_app_only(self) -> bool:
        """``idtyp=app`` (v2) / no ``scp`` and ``roles`` or ``oid == sub`` (v1): a service principal rather than a signed-in user."""
        return self.raw.get("idtyp") == "app" or (not self.scopes and (bool(self.roles) or self.object_id == self.subject))

    def claims(self) -> dict[str, Any]:
        return {"tid": self.tenant_id, "oid": self.object_id, "sub": self.subject, "appid": self.app_id, "roles": list(self.roles), "scp": list(self.scopes),
                "iss": self.issuer, "aud": self.audience, "ver": self.version, "upn": self.upn, "name": self.name}


def _equivalent_audiences(audiences: Iterable[str]) -> set[str]:
    out: set[str] = set()
    for a in audiences:
        a = a.strip()
        if not a:
            continue
        out.add(a)
        if _GUID.match(a):
            out.add(f"api://{a}")
        elif a.lower().startswith("api://") and _GUID.match(a[6:]):
            out.add(a[6:])
    return out


class EntraValidator:
    """Validate Entra access tokens against a tenant allow-list, audience list and the tenants' JWKS."""

    def __init__(self, *, allowed_tenants: Iterable[str], audiences: Iterable[str], network: Network | None = None, clock_skew_seconds: int = 120,
                 jwks_ttl_seconds: int = 3600, jwks_min_refresh_seconds: float = 20.0, now: Callable[[], float] = time.time,
                 client_factory: Callable[[], httpx.Client] | None = None) -> None:
        self.allowed_tenants = {t.strip().lower() for t in allowed_tenants if t and t.strip()}
        self.audiences = _equivalent_audiences(audiences)
        self.network = network or Network()
        self.skew = int(clock_skew_seconds)
        self.jwks_ttl = int(jwks_ttl_seconds)
        self.min_refresh = float(jwks_min_refresh_seconds)
        self._now = now
        self._client_factory = client_factory or (lambda: make_client(self.network))
        self._lock = threading.Lock()
        #: tenant -> {"keys": {kid: PyJWK}, "fetched": ts, "attempted": ts}
        self._jwks: dict[str, dict[str, Any]] = {}

    # ------------------------------------------------------------------ public
    def issuers_for(self, tid: str) -> set[str]:
        return {f"{self.network.authority_host}/{tid}/v2.0", V1_ISSUER.format(tid=tid), f"https://login.microsoftonline.com/{tid}/v2.0"}

    def jwks_url(self, tid: str) -> str:
        return f"{self.network.authority_host}/{tid}/discovery/v2.0/keys"

    def validate(self, token: str) -> EntraIdentity:
        try:
            header = jwt.get_unverified_header(token)
            unverified = jwt.decode(token, options={"verify_signature": False, "verify_exp": False, "verify_aud": False})
        except jwt.PyJWTError as exc:
            raise TokenInvalid("the bearer token is not a well-formed JWT", remediation="send an Entra ID access token for the OLCF API registration") from exc
        if header.get("alg") != "RS256":
            raise TokenInvalid(f"token algorithm '{header.get('alg')}' is not accepted (RS256 only)")
        kid = header.get("kid")
        tid = str(unverified.get("tid") or "").lower()
        if not self.allowed_tenants:
            raise TokenInvalid("no Entra tenant is allowed for the Fabric integration", remediation="configure allowed tenants (PUT /v1/integrations/fabric) or settings.fabric.allowed_tenants")
        if not tid or tid not in self.allowed_tenants:
            raise TokenInvalid("the token's tenant is not allowed", details={"tid": tid or None}, remediation="add the Fabric tenant id to the allowed tenants of the Fabric integration")
        if str(unverified.get("iss") or "") not in self.issuers_for(tid):
            raise TokenInvalid("the token issuer does not match its tenant")
        if not self.audiences:
            raise TokenInvalid("no audience is configured for the Fabric integration", remediation="configure the audience (App ID URI) of the OLCF API registration")
        key = self._signing_key(tid, kid)
        try:
            claims = jwt.decode(token, key=key.key, algorithms=["RS256"], audience=list(self.audiences), issuer=list(self.issuers_for(tid)), leeway=self.skew,
                                options={"require": ["exp", "iss", "aud"], "verify_exp": True, "verify_nbf": True, "verify_iat": False})
        except jwt.ExpiredSignatureError as exc:
            raise TokenInvalid("the token has expired", remediation="acquire a fresh token (and check clock skew between the Fabric session and the control plane)") from exc
        except jwt.InvalidAudienceError as exc:
            raise TokenInvalid("the token audience is not accepted", remediation="request the token for the OLCF API audience configured in the Fabric integration") from exc
        except jwt.PyJWTError as exc:
            raise TokenInvalid(f"the token is invalid: {type(exc).__name__}") from exc
        aud = claims.get("aud")
        return EntraIdentity(
            tenant_id=tid, object_id=str(claims.get("oid") or ""), subject=str(claims.get("sub") or ""),
            app_id=claims.get("appid") or claims.get("azp"), roles=tuple(str(r) for r in claims.get("roles") or []),
            scopes=tuple(str(claims.get("scp") or "").split()), issuer=str(claims["iss"]),
            audience=str(aud[0] if isinstance(aud, list) else aud), version=str(claims.get("ver") or ""),
            upn=claims.get("upn") or claims.get("preferred_username"), name=claims.get("name"), expires_at=float(claims["exp"]),
            raw={k: v for k, v in claims.items() if k in ("idtyp", "xms_mirid", "acr", "amr", "appidacr", "azpacr")})

    # ------------------------------------------------------------------ jwks
    def _signing_key(self, tid: str, kid: str | None) -> jwt.PyJWK:
        if not kid:
            raise TokenInvalid("the token header has no key id (kid)")
        now = self._now()
        with self._lock:
            entry = self._jwks.get(tid)
            fresh = entry is not None and now - entry["fetched"] < self.jwks_ttl
            if fresh and kid in entry["keys"]:
                return entry["keys"][kid]
            # unknown kid or stale cache: refresh, but not more often than min_refresh (a flood of bogus kids must not become a flood of fetches)
            can_refresh = entry is None or now - entry["attempted"] >= self.min_refresh
            if can_refresh:
                self._refresh(tid, now)
            entry = self._jwks.get(tid)
            if entry is not None and kid in entry["keys"]:
                return entry["keys"][kid]
        raise TokenInvalid("the token was signed with an unknown key", remediation="the tenant's signing keys could not be matched; retry shortly or check connectivity to the Entra JWKS endpoint")

    def _refresh(self, tid: str, now: float) -> None:
        prev = self._jwks.get(tid)
        placeholder = {"keys": dict(prev["keys"]) if prev else {}, "fetched": prev["fetched"] if prev else 0.0, "attempted": now}
        self._jwks[tid] = placeholder
        try:
            with self._client_factory() as client:
                resp = client.get(self.jwks_url(tid))
            resp.raise_for_status()
            doc = resp.json()
            keys = {}
            for jwk in doc.get("keys", []):
                if jwk.get("kty") == "RSA" and jwk.get("kid") and jwk.get("use", "sig") == "sig":
                    keys[jwk["kid"]] = jwt.PyJWK.from_dict(jwk, algorithm="RS256")
            if keys:
                self._jwks[tid] = {"keys": keys, "fetched": now, "attempted": now}
        except (httpx.HTTPError, ValueError, jwt.PyJWTError):
            # keep serving the previous key set; the caller turns "kid not found" into a clear rejection
            return

    def cached_tenants(self) -> dict[str, int]:
        return {t: len(e["keys"]) for t, e in self._jwks.items()}


# ===================================================================================== outbound tokens
class EntraTokenError(Exception):
    """Token acquisition failed. ``code`` is the AADSTS code when Entra sent one; ``remediation`` says what to check."""

    def __init__(self, message: str, *, code: str | None = None, remediation: str | None = None, status: int | None = None) -> None:
        super().__init__(message)
        self.code, self.remediation, self.status = code, remediation, status


_AADSTS_HINTS = {
    "AADSTS7000215": "The client secret is wrong or was copied as the secret *id*. Create a new secret and update the broker connection.",
    "AADSTS7000222": "The client secret expired. Create a new secret and update the broker connection.",
    "AADSTS700016": "The application (client) id was not found in this tenant. Check client_id and tenant_id of the broker connection.",
    "AADSTS90002": "The tenant was not found. Check tenant_id of the broker connection.",
    "AADSTS700027": "The certificate assertion was rejected: upload the matching public certificate to the app registration.",
    "AADSTS700024": "Client assertion is not within its valid time range: check the clock of the control plane.",
    "AADSTS50034": "The account does not exist in the tenant.",
    "AADSTS65001": "Admin consent is missing for the requested scope.",
}


class TokenBroker:
    """App-only token acquisition for the broker service principal (client secret or certificate), with an in-memory cache."""

    def __init__(self, *, tenant_id: str, client_id: str, secret: dict[str, Any] | str, network: Network | None = None, now: Callable[[], float] = time.time,
                 client_factory: Callable[[], httpx.Client] | None = None) -> None:
        if not tenant_id or not client_id:
            raise EntraTokenError("the broker identity needs tenant_id and client_id", remediation="set tenant_id and client_id on the broker connection")
        self.tenant_id, self.client_id = tenant_id, client_id
        self._secret = {"client_secret": secret} if isinstance(secret, str) else dict(secret or {})
        self.network = network or Network()
        self._now = now
        self._client_factory = client_factory or (lambda: make_client(self.network))
        self._cache: dict[str, tuple[str, float]] = {}
        self._lock = threading.Lock()

    @property
    def token_endpoint(self) -> str:
        return f"{self.network.authority_host}/{self.tenant_id}/oauth2/v2.0/token"

    def _assertion(self) -> str:
        pem_key = self._secret.get("private_key_pem")
        pem_cert = self._secret.get("certificate_pem")
        if not pem_key or not pem_cert:
            raise EntraTokenError("the broker secret has neither client_secret nor private_key_pem + certificate_pem",
                                  remediation="store {client_secret} or {private_key_pem, certificate_pem} in the broker connection's secret")
        cert = x509.load_pem_x509_certificate(pem_cert.encode())
        thumb = base64.urlsafe_b64encode(hashlib.sha256(cert.public_bytes(serialization.Encoding.DER)).digest()).decode().rstrip("=")
        now = int(self._now())
        claims = {"aud": self.token_endpoint, "iss": self.client_id, "sub": self.client_id, "jti": str(uuid.uuid4()), "nbf": now - 5, "exp": now + 300}
        return jwt.encode(claims, pem_key, algorithm="RS256", headers={"x5t#S256": thumb})

    def get_token(self, scope: str) -> str:
        """An access token for ``scope`` (for example ``https://api.fabric.microsoft.com/.default`` or ``https://storage.azure.com/.default``)."""
        with self._lock:
            hit = self._cache.get(scope)
            if hit and hit[1] - 60 > self._now():
                return hit[0]
        form = {"grant_type": "client_credentials", "client_id": self.client_id, "scope": scope}
        if self._secret.get("client_secret"):
            form["client_secret"] = str(self._secret["client_secret"])
        else:
            form["client_assertion_type"] = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
            form["client_assertion"] = self._assertion()
        try:
            with self._client_factory() as client:
                resp = client.post(self.token_endpoint, data=form, headers={"Accept": "application/json"})
        except httpx.HTTPError as exc:
            raise EntraTokenError(f"cannot reach the Entra token endpoint: {type(exc).__name__}", remediation="check DNS, proxy and CA settings (POST /v1/integrations/fabric/test)") from None
        try:
            doc = resp.json()
        except ValueError:
            doc = {}
        if resp.status_code != 200 or "access_token" not in doc:
            desc = str(doc.get("error_description") or doc.get("error") or f"HTTP {resp.status_code}")
            m = re.search(r"AADSTS\d+", desc)
            code = m.group(0) if m else None
            raise EntraTokenError(f"Entra rejected the token request: {desc.splitlines()[0][:300]}", code=code, status=resp.status_code,
                                  remediation=_AADSTS_HINTS.get(code or "", "Check tenant_id, client_id and the credential of the broker connection."))
        tok = str(doc["access_token"])
        with self._lock:
            self._cache[scope] = (tok, self._now() + float(doc.get("expires_in", 3600)))
        return tok

    def decode_unverified(self, token: str) -> dict[str, Any]:
        """Claims of a token we just obtained (for diagnostics output: ``appid``, ``tid``, ``roles``; never the token itself)."""
        try:
            return jwt.decode(token, options={"verify_signature": False, "verify_exp": False, "verify_aud": False})
        except jwt.PyJWTError:
            return {}


def summarise_token(claims: dict[str, Any]) -> str:
    keep = {k: claims.get(k) for k in ("tid", "appid", "azp", "aud", "roles") if claims.get(k) is not None}
    return json.dumps(keep, default=str)
