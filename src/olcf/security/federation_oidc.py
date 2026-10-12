# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""OIDC / OAuth2 client primitives for identity federation (spec section 22).

Why this module exists
----------------------
Everything that touches an external identity provider's *wire protocol* lives here, separated from the platform glue in
:mod:`olcf.security.federation`: endpoint discovery, JWKS retrieval with caching and key-rotation handling, PKCE, the token
endpoint, and - most importantly - strict JWT validation.

Security properties (each one has an attack test in ``tests/security/test_federation_attacks.py``)
-------------------------------------------------------------------------------------------------
* **Algorithm allow-list.** Only asymmetric algorithms are ever accepted (RS*/PS*/ES*/EdDSA). ``none`` and every ``HS*`` algorithm are rejected
  *before* a key is looked up, so the classic "sign with HS256 using the public key as the HMAC secret" confusion attack cannot work. The
  algorithm used to verify comes from the allow-list intersected with the *key's* type, never from attacker-controlled data alone.
* **Keys come only from the provider's own JWKS** (``jwks_uri`` of the discovery document or an operator override). ``jku``/``x5u``/``jwk``/``x5c``
  header members are ignored.
* **Issuer first.** The (unverified) ``iss`` claim is used only to *select* configured providers; an unknown issuer never triggers any network call.
* ``exp`` is mandatory; ``nbf``/``iat`` are checked with a small, bounded clock skew. ``aud`` must intersect the configured audiences; for ID
  tokens ``azp`` is enforced when several audiences are present, ``nonce`` and ``at_hash`` are checked.
* **Tenant allow-list** (Microsoft Entra): the ``tid`` claim must be in the configured list, and the issuer must embed that same tenant id. A
  multi-tenant authority (``organizations``/``common``) with an empty allow-list accepts *nothing*.
* **Key rotation.** An unknown ``kid`` triggers one JWKS refresh (rate limited, so garbage tokens cannot turn the control plane into a request
  amplifier); a failed refresh falls back to the stale key set for a bounded time.
* Outbound requests: HTTPS only (``allow_insecure_http`` is an explicit per-provider opt-in for development), no redirects, no credentials in
  URLs, bounded response size and time, enterprise CA bundle / proxy honoured.
"""

from __future__ import annotations

import base64
import hashlib
import ipaddress
import json
import logging
import re
import secrets as _secrets
import threading
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

import httpx
import jwt
from jwt.algorithms import get_default_algorithms

from olcf.core.config import Settings
from olcf.core.errors import BadRequest, Unauthenticated, Unavailable

log = logging.getLogger("olcf.federation")

#: Asymmetric algorithms that may ever be used to verify a federated token. HS* / none are deliberately absent.
ASYMMETRIC_ALGS = ("RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES384", "ES512", "EdDSA")
DEFAULT_ENTRA_AUTHORITY = "https://login.microsoftonline.com"
ENTRA_V1_ISSUER = "https://sts.windows.net/{tid}/"
GUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
MULTI_TENANT_AUTHORITIES = ("common", "organizations", "consumers")
MAX_RESPONSE_BYTES = 2 * 1024 * 1024
_BLOCKED_HOSTS = {"169.254.169.254", "metadata.google.internal", "fd00:ec2::254"}


class TokenRejected(Unauthenticated):
    """A federated token failed validation. ``reason`` is a stable machine-readable code (never includes token contents)."""

    code = "unauthenticated"

    def __init__(self, reason: str, message: str | None = None) -> None:
        super().__init__(message or f"token rejected: {reason}")
        self.reason = reason


# ----------------------------------------------------------------------------- PKCE / random helpers
def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


def new_pkce() -> tuple[str, str]:
    """Return ``(code_verifier, S256 code_challenge)`` (RFC 7636): 64 random bytes -> 86 character verifier."""
    verifier = b64url(_secrets.token_bytes(64))
    return verifier, pkce_challenge(verifier)


def pkce_challenge(verifier: str) -> str:
    return b64url(hashlib.sha256(verifier.encode("ascii")).digest())


def sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def random_token(nbytes: int = 32) -> str:
    return b64url(_secrets.token_bytes(nbytes))


def left_half_hash(value: str, alg: str) -> str:
    """``at_hash`` / ``c_hash`` computation (OIDC core 3.1.3.6): left half of the hash chosen by the id_token's alg size."""
    bits = {"256": hashlib.sha256, "384": hashlib.sha384, "512": hashlib.sha512}.get(alg[-3:], hashlib.sha256)
    digest = bits(value.encode("ascii")).digest()
    return b64url(digest[: len(digest) // 2])


# ----------------------------------------------------------------------------- provider configuration view
@dataclass
class ProviderSpec:
    """Typed, validated view of an ``identity_providers`` row. All policy lives in ``config`` (JSON) so the schema stays stable."""

    id: str
    name: str
    type: str
    issuer: str | None
    client_id: str | None
    client_secret_id: str | None
    enabled: bool
    display_name: str
    config: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> ProviderSpec:
        return cls(id=row["id"], name=row["name"], type=row["type"], issuer=row.get("issuer"), client_id=row.get("client_id"),
                   client_secret_id=row.get("client_secret_id"), enabled=bool(row.get("enabled")), display_name=row.get("display_name") or row["name"],
                   config=dict(row.get("config") or {}))

    # --- generic knobs
    def cfg(self, key: str, default: Any = None) -> Any:
        v = self.config.get(key)
        return default if v is None else v

    @property
    def allow_http(self) -> bool:
        return bool(self.cfg("allow_insecure_http", False))

    @property
    def is_entra(self) -> bool:
        return self.type == "entra"

    @property
    def authority(self) -> str:
        return str(self.cfg("authority_host", DEFAULT_ENTRA_AUTHORITY)).rstrip("/")

    @property
    def tenant(self) -> str | None:
        """Entra tenant segment of the authority (GUID, domain, or ``organizations``/``common``/``consumers``)."""
        t = self.cfg("tenant")
        if t:
            return str(t)
        if self.is_entra and self.issuer:
            parts = urlsplit(self.issuer).path.strip("/").split("/")
            return parts[0] if parts and parts[0] else None
        return None

    @property
    def multi_tenant(self) -> bool:
        return self.is_entra and (self.tenant or "").lower() in MULTI_TENANT_AUTHORITIES

    @property
    def allowed_tenants(self) -> list[str]:
        """Tenant ids (lower-case) whose tokens are accepted. Single-tenant Entra providers default to their own tenant GUID."""
        explicit = [str(t).lower() for t in self.cfg("allowed_tenants", []) or []]
        if explicit:
            return explicit
        t = self.tenant
        if self.is_entra and t and GUID_RE.match(t):
            return [t.lower()]
        return []

    @property
    def audiences(self) -> list[str]:
        """Accepted ``aud`` values for bearer access tokens (default: client id and ``api://<client id>``)."""
        explicit = [str(a) for a in self.cfg("allowed_audiences", []) or []]
        if explicit:
            return explicit
        if self.client_id:
            return [self.client_id, f"api://{self.client_id}"]
        return []

    @property
    def allowed_algs(self) -> list[str]:
        algs = [a for a in (self.cfg("allowed_algs", None) or list(ASYMMETRIC_ALGS)) if a in ASYMMETRIC_ALGS]
        return algs or ["RS256"]

    @property
    def scopes(self) -> list[str]:
        scopes = self.cfg("scopes", None) or ["openid", "profile", "email"]
        return [str(s) for s in scopes]

    @property
    def discovery_url(self) -> str:
        override = self.cfg("discovery_url")
        if override:
            return str(override)
        if self.is_entra:
            return f"{self.authority}/{self.tenant}/v2.0/.well-known/openid-configuration"
        return str(self.issuer or "").rstrip("/") + "/.well-known/openid-configuration"

    def allow_login_enabled(self) -> bool:
        """Can users sign in interactively through this provider right now?"""
        return bool(self.enabled and self.cfg("allow_login", True) and self.type in ("oidc", "entra", "ldap"))

    @property
    def clock_skew(self) -> int:
        return max(0, min(int(self.cfg("clock_skew_seconds", 60)), 300))

    @property
    def subject_claim(self) -> str:
        return str(self.cfg("subject_claim", "oid" if self.is_entra else "sub"))

    def issuer_matches(self, iss: str, claims: dict[str, Any]) -> bool:
        """Does ``iss`` (from an *unverified* token) belong to this provider? Selection only - validation re-checks everything."""
        if not iss:
            return False
        if self.is_entra:
            tid = str(claims.get("tid") or "").lower()
            if not tid:
                return False
            if iss == f"{self.authority}/{tid}/v2.0":
                return True
            return bool(self.cfg("accept_v1_tokens", False)) and iss == ENTRA_V1_ISSUER.format(tid=tid)
        return iss == self.issuer


def validate_endpoint_url(url: str, *, allow_http: bool, what: str) -> str:
    """Reject non-HTTPS / credential-bearing / cloud-metadata endpoints. Raises :class:`BadRequest`."""
    try:
        parts = urlsplit(url)
    except ValueError as exc:
        raise BadRequest(f"{what}: not a valid URL") from exc
    if parts.scheme not in ("https", "http") or not parts.hostname:
        raise BadRequest(f"{what}: must be an absolute http(s) URL")
    if parts.scheme == "http" and not allow_http:
        raise BadRequest(f"{what}: plain http is not allowed", remediation="use https, or set config.allow_insecure_http=true for development only")
    if parts.username or parts.password:
        raise BadRequest(f"{what}: credentials in URLs are not allowed")
    host = parts.hostname.lower()
    if host in _BLOCKED_HOSTS:
        raise BadRequest(f"{what}: host {host} is blocked")
    try:
        ip = ipaddress.ip_address(host)
        if ip.is_link_local or ip.is_multicast or ip.is_unspecified:
            raise BadRequest(f"{what}: address {host} is blocked")
    except ValueError:
        pass
    return url


# ----------------------------------------------------------------------------- HTTP
class Http:
    """Outbound HTTP for IdP traffic: enterprise CA bundle + proxy honoured, no redirects, bounded body and time."""

    def __init__(self, settings: Settings, *, timeout: float = 10.0, transport: httpx.BaseTransport | None = None) -> None:
        self.settings, self.timeout, self.transport = settings, timeout, transport

    def _client(self) -> httpx.Client:
        kwargs: dict[str, Any] = {"timeout": self.timeout, "follow_redirects": False, "trust_env": True}
        ca = self.settings.tls.ca_bundle
        if ca:
            import ssl

            kwargs["verify"] = ssl.create_default_context(cafile=ca)
        if self.transport is not None:
            kwargs["transport"] = self.transport
        return httpx.Client(**kwargs)

    def request(self, method: str, url: str, *, allow_http: bool, what: str, **kw: Any) -> httpx.Response:
        validate_endpoint_url(url, allow_http=allow_http, what=what)
        try:
            with self._client() as c, c.stream(method, url, **kw) as resp:
                body = bytearray()
                for chunk in resp.iter_bytes():
                    body.extend(chunk)
                    if len(body) > MAX_RESPONSE_BYTES:
                        raise Unavailable(f"{what}: response larger than {MAX_RESPONSE_BYTES} bytes")
                return httpx.Response(resp.status_code, headers=resp.headers, content=bytes(body), request=resp.request)
        except httpx.HTTPError as exc:
            raise Unavailable(f"{what}: {type(exc).__name__}: {exc}") from exc

    def get_json(self, url: str, *, allow_http: bool, what: str, headers: dict[str, str] | None = None) -> tuple[dict[str, Any], httpx.Response]:
        resp = self.request("GET", url, allow_http=allow_http, what=what, headers={"Accept": "application/json", **(headers or {})})
        if resp.status_code != 200:
            raise Unavailable(f"{what}: HTTP {resp.status_code}")
        try:
            data = json.loads(resp.content)
        except ValueError as exc:
            raise Unavailable(f"{what}: response is not JSON") from exc
        if not isinstance(data, dict):
            raise Unavailable(f"{what}: response is not a JSON object")
        return data, resp


# ----------------------------------------------------------------------------- discovery + JWKS caches
@dataclass
class Discovery:
    """A cached OIDC discovery document with the time it was fetched."""
    doc: dict[str, Any]
    fetched_at: float

    def endpoint(self, name: str) -> str | None:
        v = self.doc.get(name)
        return str(v) if v else None


class DiscoveryCache:
    """Per-provider discovery documents. A wrong ``issuer`` in the document (mix-up attack) is an error, never accepted."""

    def __init__(self, http: Http, ttl: float = 3600.0) -> None:
        self.http, self.ttl = http, ttl
        self._cache: dict[tuple[str, str], Discovery] = {}
        self._lock = threading.Lock()

    def invalidate(self, provider_id: str | None = None) -> None:
        with self._lock:
            for k in [k for k in self._cache if provider_id is None or k[0] == provider_id]:
                self._cache.pop(k, None)

    def get(self, spec: ProviderSpec, *, force: bool = False) -> Discovery:
        key = (spec.id, spec.discovery_url)
        with self._lock:
            hit = self._cache.get(key)
        if hit and not force and time.monotonic() - hit.fetched_at < self.ttl:
            return hit
        try:
            doc, _ = self.http.get_json(spec.discovery_url, allow_http=spec.allow_http, what="discovery document")
        except Unavailable:
            if hit and time.monotonic() - hit.fetched_at < 24 * 3600:
                log.warning("discovery refresh failed for provider %s; using stale document", spec.name)
                return hit
            raise
        self._check(spec, doc)
        disc = Discovery(doc, time.monotonic())
        with self._lock:
            self._cache[key] = disc
        return disc

    @staticmethod
    def _check(spec: ProviderSpec, doc: dict[str, Any]) -> None:
        iss = str(doc.get("issuer") or "")
        if spec.is_entra and spec.multi_tenant:
            if "{tenantid}" not in iss and iss != spec.issuer:
                raise Unavailable("discovery document issuer does not look like a multi-tenant Entra authority")
        elif not spec.cfg("skip_issuer_check_in_discovery", False) and iss != (spec.issuer if not spec.is_entra else f"{spec.authority}/{spec.tenant}/v2.0"):
            raise Unavailable("discovery document issuer does not match the configured issuer", remediation="check the issuer URL (exact match, including any trailing slash)")
        for name in ("jwks_uri",):
            if not doc.get(name):
                raise Unavailable(f"discovery document has no {name}")
        for name in ("jwks_uri", "token_endpoint", "authorization_endpoint", "userinfo_endpoint"):
            if doc.get(name):
                validate_endpoint_url(str(doc[name]), allow_http=spec.allow_http, what=name)


@dataclass
class KeySet:
    """A cached set of signing keys of an identity provider, indexed by key id, with fetch time and time-to-live."""
    keys: dict[str, jwt.PyJWK]
    anonymous: list[jwt.PyJWK]
    fetched_at: float
    ttl: float
    raw_count: int = 0


class JwksCache:
    """JWKS per ``jwks_uri`` with TTL, rate-limited forced refresh on unknown ``kid`` and stale-if-error."""

    def __init__(self, http: Http, *, default_ttl: float = 3600.0, min_refresh_seconds: float = 15.0, max_stale_seconds: float = 24 * 3600.0) -> None:
        self.http = http
        self.default_ttl, self.min_refresh, self.max_stale = default_ttl, min_refresh_seconds, max_stale_seconds
        self._sets: dict[str, KeySet] = {}
        self._last_forced: dict[str, float] = {}
        self._lock = threading.Lock()
        self.fetches = 0  # observable by tests / diagnostics

    def invalidate(self, uri: str | None = None) -> None:
        with self._lock:
            if uri is None:
                self._sets.clear()
                self._last_forced.clear()
            else:
                self._sets.pop(uri, None)
                self._last_forced.pop(uri, None)

    def _fetch(self, uri: str, allow_http: bool) -> KeySet:
        data, resp = self.http.get_json(uri, allow_http=allow_http, what="JWKS")
        keys: dict[str, jwt.PyJWK] = {}
        anonymous: list[jwt.PyJWK] = []
        raw = data.get("keys")
        if not isinstance(raw, list):
            raise Unavailable("JWKS has no 'keys' array")
        for jwk in raw:
            if not isinstance(jwk, dict) or jwk.get("use") == "enc" or jwk.get("kty") not in ("RSA", "EC", "OKP"):
                continue
            try:
                pj = jwt.PyJWK.from_dict(jwk)
            except Exception as exc:  # noqa: BLE001 - skip unusable keys (e.g. unsupported curve) rather than failing the whole set
                log.debug("skipping unusable JWK kid=%s: %s", jwk.get("kid"), type(exc).__name__)
                continue
            kid = jwk.get("kid")
            if kid:
                keys[str(kid)] = pj
            else:
                anonymous.append(pj)
        ttl = self.default_ttl
        m = re.search(r"max-age=(\d+)", resp.headers.get("cache-control", ""))
        if m:
            ttl = float(max(60, min(int(m.group(1)), 86400)))
        with self._lock:
            self.fetches += 1
        return KeySet(keys, anonymous, time.monotonic(), ttl, raw_count=len(raw))

    def get(self, uri: str, *, allow_http: bool, force: bool = False) -> KeySet:
        with self._lock:
            ks = self._sets.get(uri)
        if ks and not force and time.monotonic() - ks.fetched_at < ks.ttl:
            return ks
        try:
            new = self._fetch(uri, allow_http)
        except Unavailable:
            if ks and time.monotonic() - ks.fetched_at < self.max_stale:
                log.warning("JWKS refresh failed for %s; continuing with stale keys", uri)
                return ks
            raise
        with self._lock:
            self._sets[uri] = new
        return new

    def key_for(self, uri: str, kid: str | None, *, allow_http: bool, kty: str | None = None, min_refresh: float | None = None) -> jwt.PyJWK:
        """Return the verification key for ``kid``; refresh once (rate limited) when it is unknown (key rotation)."""
        ks = self.get(uri, allow_http=allow_http)
        key = self._pick(ks, kid, kty)
        if key is not None:
            return key
        now = time.monotonic()
        with self._lock:
            last = self._last_forced.get(uri, 0.0)
            allowed = now - last >= (self.min_refresh if min_refresh is None else min_refresh)
            if allowed:
                self._last_forced[uri] = now
        if allowed:
            ks = self.get(uri, allow_http=allow_http, force=True)
            key = self._pick(ks, kid, kty)
            if key is not None:
                return key
        raise TokenRejected("unknown_key", "signing key not found in the provider's JWKS")

    @staticmethod
    def _pick(ks: KeySet, kid: str | None, kty: str | None) -> jwt.PyJWK | None:
        if kid:
            k = ks.keys.get(kid)
            return k if k is not None and (kty is None or k.key_type == kty) else None
        # no kid in the token header: only acceptable when it is unambiguous
        cands = [k for k in [*ks.keys.values(), *ks.anonymous] if kty is None or k.key_type == kty]
        return cands[0] if len(cands) == 1 else None


# ----------------------------------------------------------------------------- validation
_ALG_KTY = {"RS": "RSA", "PS": "RSA", "ES": "EC", "Ed": "OKP"}


def alg_key_type(alg: str) -> str | None:
    return _ALG_KTY.get(alg[:2])


@dataclass
class Validated:
    """A token that passed validation: its claims, header and the provider that issued it."""
    claims: dict[str, Any]
    header: dict[str, Any]
    provider: ProviderSpec


class TokenValidator:
    """Strict JWT validation against a provider (see module docstring for the threat model)."""

    def __init__(self, jwks: JwksCache, discovery: DiscoveryCache) -> None:
        self.jwks, self.discovery = jwks, discovery
        self._algs = get_default_algorithms()

    # --- header / signature
    def _verify_signature(self, spec: ProviderSpec, token: str) -> tuple[dict[str, Any], dict[str, Any]]:
        try:
            header = jwt.get_unverified_header(token)
        except jwt.PyJWTError as exc:
            raise TokenRejected("malformed", "token is not a well-formed JWT") from exc
        alg = header.get("alg")
        if not isinstance(alg, str) or alg not in spec.allowed_algs:
            # covers "none", "HS256/384/512" and anything unexpected: rejected before any key is considered
            raise TokenRejected("alg_not_allowed", f"signature algorithm {alg!r} is not accepted")
        kty = alg_key_type(alg)
        kid = header.get("kid")
        if kid is not None and not isinstance(kid, str):
            raise TokenRejected("malformed", "invalid kid")
        disc = self.discovery.get(spec)
        jwks_uri = str(spec.cfg("jwks_uri") or disc.endpoint("jwks_uri"))
        key = self.jwks.key_for(jwks_uri, kid, allow_http=spec.allow_http, kty=kty, min_refresh=float(spec.cfg("jwks_min_refresh_seconds", 15.0)))
        jwk_alg = key._jwk_data.get("alg") if hasattr(key, "_jwk_data") else None  # noqa: SLF001
        if jwk_alg and jwk_alg != alg:
            raise TokenRejected("alg_mismatch", "token algorithm does not match the key's declared algorithm")
        try:
            claims = jwt.decode(token, key=key.key, algorithms=[alg], leeway=spec.clock_skew,
                                options={"require": ["exp"], "verify_aud": False, "verify_iss": False, "verify_exp": True, "verify_nbf": True, "verify_iat": False})
        except jwt.ExpiredSignatureError as exc:
            raise TokenRejected("expired", "token has expired") from exc
        except jwt.ImmatureSignatureError as exc:
            raise TokenRejected("not_yet_valid", "token is not valid yet") from exc
        except jwt.InvalidSignatureError as exc:
            raise TokenRejected("bad_signature", "token signature is invalid") from exc
        except jwt.PyJWTError as exc:
            raise TokenRejected("invalid", f"token is invalid ({type(exc).__name__})") from exc
        if not isinstance(claims, dict):
            raise TokenRejected("malformed", "claims are not an object")
        return header, claims

    @staticmethod
    def _aud_list(claims: dict[str, Any]) -> list[str]:
        aud = claims.get("aud")
        if isinstance(aud, str):
            return [aud]
        if isinstance(aud, list):
            return [str(a) for a in aud]
        return []

    def _check_issuer_and_tenant(self, spec: ProviderSpec, claims: dict[str, Any]) -> None:
        iss = claims.get("iss")
        if not isinstance(iss, str) or not iss:
            raise TokenRejected("bad_issuer", "token has no issuer")
        if not spec.issuer_matches(iss, claims):
            raise TokenRejected("bad_issuer", "token issuer is not accepted by this provider")
        if spec.is_entra:
            tid = str(claims.get("tid") or "").lower()
            if not tid or tid not in spec.allowed_tenants:
                raise TokenRejected("tenant_not_allowed", "token tenant is not in the allow-list")
            if tid not in iss.lower():  # issuer must embed the same tenant (defence against forged tid)
                raise TokenRejected("bad_issuer", "token issuer and tenant disagree")

    def validate_access_token(self, spec: ProviderSpec, token: str) -> Validated:
        """Validate a bearer access token (JWT) presented to the OLCF API."""
        header, claims = self._verify_signature(spec, token)
        self._check_issuer_and_tenant(spec, claims)
        auds = self._aud_list(claims)
        if not set(auds) & set(spec.audiences):
            raise TokenRejected("bad_audience", "token audience is not accepted")
        return Validated(claims, header, spec)

    def validate_id_token(self, spec: ProviderSpec, token: str, *, nonce: str, access_token: str | None = None) -> Validated:
        """Validate the ``id_token`` of the authorization-code flow (OIDC core 3.1.3.7)."""
        header, claims = self._verify_signature(spec, token)
        self._check_issuer_and_tenant(spec, claims)
        auds = self._aud_list(claims)
        if spec.client_id not in auds:
            raise TokenRejected("bad_audience", "id_token was not issued for this client")
        if len(auds) > 1 and claims.get("azp") != spec.client_id:
            raise TokenRejected("bad_audience", "id_token azp does not match the client")
        got_nonce = claims.get("nonce")
        if not isinstance(got_nonce, str) or not _secrets.compare_digest(got_nonce, nonce):
            raise TokenRejected("bad_nonce", "id_token nonce does not match the login request")
        if not claims.get("sub"):
            raise TokenRejected("malformed", "id_token has no subject")
        at_hash = claims.get("at_hash")
        if at_hash and access_token and not _secrets.compare_digest(str(at_hash), left_half_hash(access_token, str(header.get("alg", "RS256")))):
            raise TokenRejected("bad_at_hash", "access token does not match id_token at_hash")
        return Validated(claims, header, spec)


# ----------------------------------------------------------------------------- token endpoint
def client_auth(spec: ProviderSpec, secret: str | None) -> tuple[dict[str, str], tuple[str, str] | None]:
    """Return ``(extra form fields, basic-auth tuple)`` for the configured ``token_endpoint_auth_method``."""
    method = str(spec.cfg("token_endpoint_auth_method", "client_secret_post" if spec.is_entra else "client_secret_basic"))
    if method == "none" or secret is None:  # public client (PKCE only)
        return {"client_id": spec.client_id or ""}, None
    if method == "client_secret_basic":
        from urllib.parse import quote

        return {}, (quote(spec.client_id or "", safe=""), quote(secret, safe=""))
    if method == "client_secret_post":
        return {"client_id": spec.client_id or "", "client_secret": secret}, None
    raise BadRequest(f"unsupported token_endpoint_auth_method {method!r}", details={"supported": ["client_secret_basic", "client_secret_post", "none"]})


def post_token(http: Http, spec: ProviderSpec, endpoint: str, form: dict[str, str], secret: str | None) -> dict[str, Any]:
    extra, basic = client_auth(spec, secret)
    resp = http.request("POST", endpoint, allow_http=spec.allow_http, what="token endpoint", data={**form, **extra}, auth=basic,
                        headers={"Accept": "application/json"})
    try:
        data = json.loads(resp.content or b"{}")
    except ValueError:
        data = {}
    if resp.status_code != 200 or not isinstance(data, dict) or "error" in data:
        err = data.get("error") if isinstance(data, dict) else None
        # IdP error descriptions can contain internal detail; surface only the standard error code
        raise TokenRejected("token_endpoint_error", f"token endpoint refused the request ({err or 'HTTP ' + str(resp.status_code)})")
    return data
