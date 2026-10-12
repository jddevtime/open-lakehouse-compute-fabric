# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The Fabric integration configuration document (``fabric_config``, ``PUT /v1/integrations/fabric``).

The administrator decides *who* may submit (Entra tenants and audiences), *where* their work lands (Fabric workspace -> OLCF workspace mapping), *which identity* the
control plane uses against Fabric/OneLake (the broker connection, an OLCF ``onelake`` connection whose secret holds the service principal), and *what the notebooks may
do* (default runtime and policy). A capsule can only narrow these settings, never widen them: ``allow_pip_install`` and ``allow_shell`` come from here, not from the
notebook.

The broker secret is referenced by connection id and is only ever read server-side.
"""

from __future__ import annotations

import re
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from olcf.core.errors import ValidationFailed

_GUID = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
DEFAULT_FAMILY = "fabric-2.0-compatible"


class _C(BaseModel):
    model_config = ConfigDict(extra="forbid")


class WorkspaceMapping(_C):
    """A Fabric workspace and the OLCF workspace its notebooks run in."""

    fabric_workspace_id: str
    olcf_workspace: str = Field(description="OLCF workspace id or name")
    #: restrict to one Entra tenant (default: any allowed tenant)
    tenant_id: str | None = None
    label: str | None = None

    @field_validator("fabric_workspace_id")
    @classmethod
    def _guid(cls, v: str) -> str:
        if not _GUID.match(v.strip()):
            raise ValueError("the Fabric workspace id must be a GUID")
        return v.strip().lower()


class LakehouseMapping(_C):
    """Run against an existing OLCF lakehouse instead of the Fabric one (for example a local copy of the data)."""

    fabric_workspace_id: str | None = None
    fabric_lakehouse_id: str | None = None
    fabric_lakehouse_name: str | None = None
    olcf_lakehouse: str = Field(description="OLCF lakehouse item id or name (in the mapped workspace)")


class NetworkConfig(_C):
    """Outbound network settings of the Fabric adapter: proxy, CA bundle, timeout, whether to trust proxy environment variables and the authority host.
    """
    proxy: str | None = None
    ca_bundle: str | None = None
    timeout_seconds: float | None = Field(default=None, ge=1, le=120)
    trust_env: bool = True
    authority_host: str | None = None
    fabric_api: str | None = None
    onelake_dfs: str | None = None


class DefaultPolicy(_C):
    """Default scheduling policy applied to Fabric-originated jobs that do not set their own: queue, priority, timeout, retries and retention.
    """
    queue: str = "default"
    priority: int = Field(50, ge=0, le=100)
    timeout_seconds: int | None = Field(default=None, ge=1)
    retries: int = Field(0, ge=0, le=10)
    retention_days: int | None = Field(default=None, ge=0)
    #: ``%pip install`` / pip from notebook code. Off by default: the runtime has no package index policy for Fabric notebooks yet (see fabric-assumptions.md).
    allow_pip_install: bool = False
    #: shell escapes (``!cmd``, ``%%sh``, ``os.system``)
    allow_shell: bool = False
    #: the capsule may request at most these resources (``None`` = platform defaults only)
    max_executors: int | None = Field(default=None, ge=1)
    max_executor_memory_mb: int | None = Field(default=None, ge=256)


class SecretGrant(_C):
    """Lets notebooks read one Key Vault secret reference through ``notebookutils.credentials.getSecret``: ``(vault, name)`` -> an OLCF secret."""

    vault: str
    name: str
    secret_id: str


class FabricConfig(_C):
    """Persisted integration settings. Everything defaults to the closed/strict value."""

    enabled: bool = True
    allowed_tenants: list[str] = Field(default_factory=list)
    audiences: list[str] = Field(default_factory=list)
    workspace_map: list[WorkspaceMapping] = Field(default_factory=list)
    lakehouse_map: list[LakehouseMapping] = Field(default_factory=list)
    #: id of the OLCF ``onelake`` connection holding the broker service principal (``{client_secret}`` or certificate material)
    broker_connection: str | None = None
    default_runtime: dict[str, Any] = Field(default_factory=lambda: {"family": DEFAULT_FAMILY})
    default_policy: DefaultPolicy = Field(default_factory=DefaultPolicy)
    network: NetworkConfig = Field(default_factory=NetworkConfig)
    secret_grants: list[SecretGrant] = Field(default_factory=list)
    #: create an OLCF OneLake lakehouse item the first time a mapped Fabric lakehouse is referenced (needs ``broker_connection``)
    auto_create_lakehouses: bool = False
    #: reject OneLake references to Fabric workspaces that are not mapped
    enforce_data_scope: bool = True
    pull_timeout_seconds: int = Field(120, ge=5, le=900)
    #: assumptions about the Fabric notebook session that the SDK honours (overridable because Microsoft behaviour may differ per tenant/runtime)
    sdk_hints: dict[str, Any] = Field(default_factory=lambda: {
        "definition_formats": ["fabricGitSource", "ipynb"],
        "token_audiences": {"storage": "storage", "pbi": "pbi", "kusto": "kusto", "keyvault": "keyvault"},
    })

    @field_validator("allowed_tenants")
    @classmethod
    def _tenants(cls, v: list[str]) -> list[str]:
        out = []
        for t in v:
            t = t.strip().lower()
            if not _GUID.match(t):
                raise ValueError(f"'{t}' is not an Entra tenant id (GUID)")
            out.append(t)
        return sorted(set(out))

    @field_validator("audiences")
    @classmethod
    def _aud(cls, v: list[str]) -> list[str]:
        out = [a.strip() for a in v if a and a.strip()]
        for a in out:
            if not (_GUID.match(a) or a.startswith(("api://", "https://"))):
                raise ValueError(f"audience '{a}' must be a client id GUID or an App ID URI (api://... / https://...)")
        return sorted(set(out))

    def workspace_for(self, fabric_workspace_id: str | None, tenant_id: str | None = None) -> WorkspaceMapping | None:
        if not fabric_workspace_id:
            return None
        fid = fabric_workspace_id.strip().lower()
        for m in self.workspace_map:
            if m.fabric_workspace_id == fid and (not m.tenant_id or not tenant_id or m.tenant_id.lower() == tenant_id.lower()):
                return m
        return None

    def grant_for(self, vault: str, name: str) -> SecretGrant | None:
        from olcf.runtime_lib.notebookutils.naming import normalise_vault

        want = (normalise_vault(vault), name)
        for g in self.secret_grants:
            if (normalise_vault(g.vault), g.name) == want:
                return g
        return None


def parse_config(doc: dict[str, Any] | None) -> FabricConfig:
    """Validate a stored/submitted document; pydantic errors become a ``ValidationFailed`` with field paths."""
    try:
        return FabricConfig.model_validate(doc or {})
    except ValueError as exc:  # pydantic.ValidationError subclasses ValueError
        errs = getattr(exc, "errors", None)
        details = [{"field": ".".join(str(p) for p in e["loc"]), "message": e["msg"]} for e in errs()] if callable(errs) else [{"message": str(exc)}]
        raise ValidationFailed("the Fabric integration configuration is invalid", details={"errors": details}) from exc


__all__ = ["DEFAULT_FAMILY", "DefaultPolicy", "FabricConfig", "LakehouseMapping", "NetworkConfig", "SecretGrant", "WorkspaceMapping", "parse_config"]
