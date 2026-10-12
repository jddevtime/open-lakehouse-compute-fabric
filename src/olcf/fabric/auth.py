# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Authentication of Fabric callers and the *Fabric principal*.

Three kinds of bearer token reach the adapter:

* a **platform token** (``olcf_pat_...`` or a local JWT) - resolved by the foundation ``authenticate``; a token with scope ``fabric`` is limited to
  ``integration.fabric.submit`` plus job read/cancel in the workspaces the principal is bound to;
* an **Entra access token** (a Fabric notebook that acquired a token for the OLCF API registration) - validated here by :class:`olcf.fabric.entra.EntraValidator`
  (tenant allow-list, audience, JWKS signature) and mapped to a platform principal ``fabric:<tid>:<oid>`` of kind ``fabric``;
* anything else is offered to the optional ``federation`` service (``ctx.maybe_service("federation")``) through the foundation ``authenticate``.

The mapped Entra principal receives the ``ServicePrincipal`` role **only** in the OLCF workspaces the administrator mapped for its tenant, and its tokens carry the
``fabric`` scope, so it cannot reach any other workspace or API (``tests/fabric/test_e2e_security.py`` proves this).
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from typing import Annotated, Any

from fastapi import Depends, Request

from olcf.api.deps import Ctx, authenticate, bearer_token, client_ip
from olcf.core.context import AppContext
from olcf.core.errors import OlcfError, Unauthenticated
from olcf.core.models import Role
from olcf.fabric.config import FabricConfig
from olcf.fabric.entra import EntraIdentity, EntraValidator
from olcf.fabric.errors import TenantNotAllowed, TokenInvalid
from olcf.fabric.http import Network
from olcf.security.rbac import Principal

log = logging.getLogger("olcf.fabric.auth")
BINDING_SOURCE = "fabric"
FABRIC_SCOPES = ["fabric"]


class FabricAuth:
    """Authenticates callers of the Fabric endpoints."""

    def __init__(self, ctx: AppContext, config: Callable[[], FabricConfig], network: Callable[[FabricConfig], Network]) -> None:
        self.ctx = ctx
        self._config = config
        self._network = network
        self._lock = threading.Lock()
        self._validator: tuple[str, EntraValidator] | None = None
        #: tests inject a client factory (local JWKS server / proxy double)
        self.client_factory: Callable[[], Any] | None = None

    # ------------------------------------------------------------------ validator
    def validator(self, cfg: FabricConfig | None = None) -> EntraValidator | None:
        cfg = cfg or self._config()
        if not cfg.allowed_tenants or not cfg.audiences:
            return None
        net = self._network(cfg)
        fingerprint = repr((sorted(cfg.allowed_tenants), sorted(cfg.audiences), net.describe()))
        with self._lock:
            if self._validator is None or self._validator[0] != fingerprint:
                self._validator = (fingerprint, EntraValidator(allowed_tenants=cfg.allowed_tenants, audiences=cfg.audiences, network=net, client_factory=self.client_factory))
            return self._validator[1]

    # ------------------------------------------------------------------ principals
    def principal_for(self, ident: EntraIdentity, cfg: FabricConfig) -> Principal:
        """Map a validated Entra identity to a platform principal and (re)sync its bindings to the mapped workspaces."""
        if ident.tenant_id.lower() not in cfg.allowed_tenants:
            raise TenantNotAllowed(f"tenant {ident.tenant_id} is not allowed", remediation="add the tenant to allowed_tenants (PUT /v1/integrations/fabric)")
        ident_svc = self.ctx.service("identity")
        name = f"fabric:{ident.tenant_id.lower()}:{ident.object_id.lower()}"
        row = ident_svc.get_by_name(name)
        if row is None:
            display = ident.name or ident.upn or ident.app_id or ident.object_id
            row = ident_svc.create_principal(name, kind="fabric", display_name=f"Fabric: {display}"[:120], source="entra", external_id=ident.object_id,
                                             attrs={"tenant_id": ident.tenant_id, "app_id": ident.app_id, "upn": ident.upn, "app_only": ident.is_app_only})
        elif not row["is_active"]:
            raise Unauthenticated("this Fabric principal is disabled")
        wanted = [(Role.SERVICE_PRINCIPAL.value, "workspace", self._olcf_workspace_id(m.olcf_workspace))
                  for m in cfg.workspace_map if not m.tenant_id or m.tenant_id.lower() == ident.tenant_id.lower()]
        wanted = [w for w in wanted if w[2]]
        ident_svc.sync_bindings(row["id"], BINDING_SOURCE, wanted)
        return self.ctx.authz.principal_from_row(row, token_scopes=list(FABRIC_SCOPES), claims=ident.claims())

    def _olcf_workspace_id(self, ref: str) -> str:
        try:
            return self.ctx.service("workspaces").resolve(ref)["id"]
        except OlcfError:
            log.warning("fabric workspace_map refers to unknown OLCF workspace %r", ref)
            return ""

    # ------------------------------------------------------------------ entry point
    def authenticate(self, token: str, *, ip: str | None = None) -> Principal:
        if token.startswith("olcf_") or self.ctx.tokens.looks_like_local_jwt(token):
            return authenticate(self.ctx, token, ip=ip)
        cfg = self._config()
        val = self.validator(cfg)
        entra_error: Exception | None = None
        if val is not None and cfg.enabled:
            try:
                ident = val.validate(token)
                principal = self.principal_for(ident, cfg)
                principal.ip = ip
                return principal
            except TokenInvalid as exc:
                entra_error = exc
        # not an Entra token for this integration: let the federation service (if any) have a go
        if self.ctx.maybe_service("federation") is not None:
            try:
                return authenticate(self.ctx, token, ip=ip)
            except OlcfError:
                if entra_error is not None:
                    raise entra_error from None
                raise
        if entra_error is not None:
            raise entra_error
        raise TokenInvalid("the bearer token was not accepted", remediation=("configure allowed_tenants and audiences for the Fabric integration "
                                                                           "(PUT /v1/integrations/fabric) or use a platform token with scope 'fabric'"))


def get_fabric_principal(request: Request, ctx: Ctx) -> Principal:
    token = bearer_token(request)
    if not token:
        raise Unauthenticated("authentication required", remediation="send 'Authorization: Bearer <token>' (a platform token, or an Entra token for the OLCF API)")
    svc = ctx.maybe_service("fabric")
    if svc is None:
        raise Unauthenticated("the Fabric integration is not installed")
    principal = svc.auth.authenticate(token, ip=client_ip(request, ctx))
    principal.correlation_id = getattr(request.state, "correlation_id", None)
    return principal


FabricCaller = Annotated[Principal, Depends(get_fabric_principal)]

__all__ = ["BINDING_SOURCE", "FABRIC_SCOPES", "FabricAuth", "FabricCaller", "get_fabric_principal"]
