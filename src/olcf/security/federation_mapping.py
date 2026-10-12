# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Claim -> role-binding mapping and the provider configuration schema for identity federation.

Why this module exists
----------------------
OIDC, Entra and LDAP differ in *how* identities are proven but share one question: "which OLCF roles does this identity get?". The answer is
a list of **mapping rules** configured per provider, evaluated against the validated claims (or LDAP attributes / group DNs) on every login and,
for bearer tokens, whenever the claim set changes. The result feeds ``IdentityService.sync_bindings(principal, "oidc:<provider>", wanted)`` so
bindings owned by a provider always equal what the IdP says *now*, and manually granted bindings are never touched.

Rule semantics
--------------
``{"claim": "groups", "value": "<exact>" | "pattern": "<regex, fullmatch>", "role": "Contributor", "scope_type": "workspace", "workspace": "analytics"}``

* ``claim`` is a dotted path into the claims (``realm_access.roles`` for Keycloak). A string value is a single candidate; a list contributes
  every element; the OAuth ``scp``/``scope`` claim is split on spaces.
* Exactly one of ``value`` / ``pattern`` (full-match regular expression; both rule and candidate are length capped) selects the claim values.
* ``scope_type`` is ``org`` (default) or ``workspace``. A workspace rule names the workspace (``workspace``, resolved when bindings are
  synced) or gives ``scope_id`` directly. Rules whose workspace does not exist are skipped and reported - never widened to org scope.
* Service principals (Entra ``idtyp=app`` tokens, ``gty=client-credentials``) are mapped by ``service_principals`` entries:
  ``{"match": {"oid": "...", "azp": "..."}, "roles": [rule-without-claim, ...]}``. A service token is accepted only if an entry matches
  (fail closed); ``roles`` / ``groups`` claim rules apply additionally.

Group overage (Microsoft Entra)
-------------------------------
Entra omits the ``groups`` claim when a user belongs to more than ~200 groups (JWT) and instead emits ``_claim_names`` / ``_claim_sources``
(or ``hasgroups``). Behaviour here:

* **Interactive login**: if ``resolve_group_overage`` is on and the token response includes a Graph-capable access token (scope ``User.Read``),
  the groups are fetched from Microsoft Graph (``transitiveMemberOf``) server-side; otherwise ``groups`` is treated as empty.
* **Bearer tokens**: the API only holds a token issued for *OLCF*, not for Graph, so overage cannot be resolved (that would need an on-behalf-of
  exchange). The groups are treated as empty and the principal is flagged ``groups_overage=true``; with ``deny_on_overage`` the token is refused.
* **Recommendation**: use **app roles** (``roles`` claim) or restrict the groups claim to "groups assigned to the application" in Entra, which
  avoids overage entirely. This is the documented, supported configuration.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from olcf.core.models import Role

_MAX_PATTERN = 256
_MAX_CANDIDATE = 512


class MappingRule(BaseModel):
    """One claim -> role rule."""

    model_config = ConfigDict(extra="forbid")

    claim: str | None = Field(None, max_length=128)
    value: str | None = Field(None, max_length=_MAX_CANDIDATE)
    pattern: str | None = Field(None, max_length=_MAX_PATTERN)
    role: str
    scope_type: Literal["org", "workspace"] = "org"
    workspace: str | None = Field(None, max_length=128)
    scope_id: str | None = Field(None, max_length=64)

    @field_validator("role")
    @classmethod
    def _role(cls, v: str) -> str:
        if v not in {r.value for r in Role}:
            raise ValueError(f"unknown role {v!r}; known: {sorted(r.value for r in Role)}")
        return v

    @field_validator("pattern")
    @classmethod
    def _pattern(cls, v: str | None) -> str | None:
        if v is not None:
            try:
                re.compile(v)
            except re.error as exc:
                raise ValueError(f"invalid regular expression: {exc}") from exc
        return v

    def model_post_init(self, __ctx: Any) -> None:
        if self.scope_type == "workspace" and not (self.workspace or self.scope_id):
            raise ValueError("a workspace rule needs 'workspace' (name) or 'scope_id'")
        if self.scope_type == "org" and (self.workspace or self.scope_id):
            raise ValueError("an org rule must not name a workspace")


class ClaimRule(MappingRule):
    """Rule evaluated against a claim: requires ``claim`` and exactly one of ``value`` / ``pattern``."""

    def model_post_init(self, __ctx: Any) -> None:
        super().model_post_init(__ctx)
        if not self.claim:
            raise ValueError("'claim' is required")
        if (self.value is None) == (self.pattern is None):
            raise ValueError("exactly one of 'value' or 'pattern' is required")


class SpMatch(BaseModel):
    """The claims (object id, authorised party or application id) that identify a service principal in a mapping."""
    model_config = ConfigDict(extra="forbid")
    oid: str | None = None
    azp: str | None = None
    appid: str | None = None
    sub: str | None = None
    client_id: str | None = None

    def model_post_init(self, __ctx: Any) -> None:
        if not any(v for v in self.model_dump().values()):
            raise ValueError("a service principal match needs at least one of oid/azp/appid/sub/client_id")


class ServicePrincipalMapping(BaseModel):
    """Maps an external service principal, matched by claims, to a name and a list of role rules."""
    model_config = ConfigDict(extra="forbid")
    name: str | None = Field(None, max_length=96, pattern=r"^[A-Za-z0-9._@+-]+$")
    match: SpMatch
    roles: list[MappingRule] = Field(default_factory=list, max_length=50)
    token_scopes: list[str] | None = None


class LdapConfig(BaseModel):
    """LDAP / Active Directory bridge settings (type ``ldap``)."""

    model_config = ConfigDict(extra="forbid")
    server_uri: str = Field(max_length=512)  # ldaps://dc.example.com:636 or ldap://... with start_tls
    start_tls: bool = False
    ca_file: str | None = None
    bind_dn: str | None = Field(None, max_length=512)  # service account used for searching; password = client secret
    user_base_dn: str = Field(max_length=512)
    user_filter: str = Field("(&(objectClass=person)(sAMAccountName={username}))", max_length=512)
    username_attr: str = "sAMAccountName"
    email_attr: str = "mail"
    display_name_attr: str = "displayName"
    group_base_dn: str | None = None
    group_filter: str = Field("(member={user_dn})", max_length=512)
    member_of_attr: str | None = "memberOf"
    nested_groups: bool = False  # AD: LDAP_MATCHING_RULE_IN_CHAIN
    timeout_seconds: float = Field(10.0, ge=1, le=60)
    #: rules evaluated against ``{"groups": [group CNs + DNs], "username": ..., "email": ...}``; use claim="groups"
    group_rules: list[ClaimRule] = Field(default_factory=list, max_length=200)
    allow_insecure_ldap: bool = False  # plain ldap:// without StartTLS - never in production


class ProviderConfig(BaseModel):
    """Everything configurable per provider beyond issuer/client (``extra=forbid``: unknown keys are errors, never ignored)."""

    model_config = ConfigDict(extra="forbid")

    # --- transport / discovery
    allow_insecure_http: bool = False
    authority_host: str | None = None  # Entra authority override (sovereign clouds, tests)
    tenant: str | None = None
    allowed_tenants: list[str] = Field(default_factory=list)
    allowed_audiences: list[str] = Field(default_factory=list)
    allowed_algs: list[str] = Field(default_factory=list)
    scopes: list[str] = Field(default_factory=list)
    discovery_url: str | None = None
    jwks_uri: str | None = None
    token_endpoint_auth_method: Literal["client_secret_basic", "client_secret_post", "none"] | None = None
    clock_skew_seconds: int = Field(60, ge=0, le=300)
    jwks_min_refresh_seconds: float = Field(15.0, ge=0, le=3600)
    accept_v1_tokens: bool = False
    # --- identity mapping
    subject_claim: str | None = None
    username_claim: str | None = None
    email_claim: str = "email"
    display_name_claim: str = "name"
    groups_claim: str = "groups"
    roles_claim: str = "roles"
    role_mappings: list[ClaimRule] = Field(default_factory=list, max_length=500)
    service_principals: list[ServicePrincipalMapping] = Field(default_factory=list, max_length=200)
    scope_map: dict[str, str] = Field(default_factory=dict, description="delegated scope (scp) -> OLCF token scope (read|write|mcp|fabric|*)")
    # --- policy
    auto_provision: bool = True
    allow_login: bool = True
    allow_bearer: bool = True
    allow_login_without_roles: bool = False
    require_email_verified: bool = False
    deny_on_overage: bool = False
    resolve_group_overage: bool = False
    graph_base_url: str | None = None
    allowed_return_origins: list[str] = Field(default_factory=list)
    session_ttl_seconds: int | None = Field(None, ge=300, le=14 * 86400)
    ldap: LdapConfig | None = None

    @field_validator("allowed_algs")
    @classmethod
    def _algs(cls, v: list[str]) -> list[str]:
        from olcf.security.federation_oidc import ASYMMETRIC_ALGS

        bad = [a for a in v if a not in ASYMMETRIC_ALGS]
        if bad:
            raise ValueError(f"algorithms {bad} are not allowed (only asymmetric algorithms: {list(ASYMMETRIC_ALGS)})")
        return v

    @field_validator("scope_map")
    @classmethod
    def _scope_map(cls, v: dict[str, str]) -> dict[str, str]:
        from olcf.security.rbac import TOKEN_SCOPES

        bad = {k: s for k, s in v.items() if s not in TOKEN_SCOPES or s in ("worker", "job", "fabric-internal")}
        if bad:
            raise ValueError(f"unknown or reserved token scopes in scope_map: {bad}")
        return v

    def all_rules(self) -> list[MappingRule]:
        rules: list[MappingRule] = list(self.role_mappings)
        for sp in self.service_principals:
            rules.extend(sp.roles)
        if self.ldap:
            rules.extend(self.ldap.group_rules)
        return rules


# ----------------------------------------------------------------------------- evaluation
Binding = tuple[str, str, str]  # (role, scope_type, scope_id)


def claim_values(claims: dict[str, Any], path: str) -> list[str]:
    """Candidate string values of ``path`` (dotted) in ``claims``; ``scp``/``scope`` are space separated."""
    cur: Any = claims
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            # keys that themselves contain dots (e.g. URN-style claims) get a second chance as a flat key
            if isinstance(claims, dict) and path in claims:
                cur = claims[path]
                break
            return []
    if isinstance(cur, str):
        return cur.split() if path in ("scp", "scope") else [cur]
    if isinstance(cur, (list, tuple)):
        return [str(x) for x in cur if isinstance(x, (str, int))]
    return []


def rule_matches(rule: ClaimRule, claims: dict[str, Any]) -> bool:
    values = claim_values(claims, rule.claim or "")
    if rule.value is not None:
        return rule.value in values
    pat = re.compile(rule.pattern or "")
    return any(len(v) <= _MAX_CANDIDATE and pat.fullmatch(v) for v in values)


def _scope(rule: MappingRule, resolve_ws: Callable[[str], str | None], skipped: list[str]) -> Binding | None:
    if rule.scope_type == "org":
        return (rule.role, "org", "")
    sid = rule.scope_id
    if not sid and rule.workspace:
        sid = resolve_ws(rule.workspace)
    if not sid:
        skipped.append(f"workspace {rule.workspace or rule.scope_id!r} not found for role {rule.role}")
        return None
    return (rule.role, "workspace", sid)


def evaluate(cfg: ProviderConfig, claims: dict[str, Any], resolve_ws: Callable[[str], str | None], *, service_match: ServicePrincipalMapping | None = None,
            extra_rules: list[ClaimRule] | None = None) -> tuple[list[Binding], list[str]]:
    """Compute the wanted bindings. Returns ``(bindings, skipped_reasons)``; the binding list is sorted and de-duplicated."""
    out: set[Binding] = set()
    skipped: list[str] = []
    for rule in [*cfg.role_mappings, *(extra_rules or [])]:
        if isinstance(rule, ClaimRule) and rule_matches(rule, claims):
            b = _scope(rule, resolve_ws, skipped)
            if b:
                out.add(b)
    if service_match is not None:
        for rule in service_match.roles:
            b = _scope(rule, resolve_ws, skipped)
            if b:
                out.add(b)
    return sorted(out), skipped


def find_service_mapping(cfg: ProviderConfig, claims: dict[str, Any]) -> ServicePrincipalMapping | None:
    """First ``service_principals`` entry whose every ``match`` member equals the claim (case-insensitive for GUIDs)."""
    for sp in cfg.service_principals:
        want = {k: v for k, v in sp.match.model_dump().items() if v}
        have = {"oid": claims.get("oid"), "azp": claims.get("azp"), "appid": claims.get("appid"), "sub": claims.get("sub"),
                "client_id": claims.get("client_id") or claims.get("cid")}
        if want and all(str(have.get(k) or "").lower() == str(v).lower() for k, v in want.items()):
            return sp
    return None


def mapping_hash(bindings: list[Binding], token_scopes: list[str] | None = None) -> str:
    return hashlib.sha256(json.dumps([bindings, token_scopes or []], sort_keys=True).encode()).hexdigest()[:24]


def is_service_token(claims: dict[str, Any]) -> bool:
    """Application (non-interactive) token? Entra: ``idtyp=app``; OAuth client credentials: ``gty``; Entra v1: ``appidacr`` without a user."""
    if claims.get("idtyp") == "app":
        return True
    gty = claims.get("gty")
    if gty == "client-credentials" or (isinstance(gty, list) and "client-credentials" in gty):
        return True
    return False


def sanitize_name(raw: str, *, limit: int = 100) -> str:
    # ':' is allowed on purpose: federated names are namespaced ``<provider>:<name>`` and locally created names can never contain ':'
    cleaned = re.sub(r"[^A-Za-z0-9._@+:#-]", "_", raw.strip())[:limit]
    return cleaned or "user"
