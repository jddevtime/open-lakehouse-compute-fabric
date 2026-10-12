# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Data plane of the adapter: turn the Fabric references of a capsule into OLCF lakehouses (and so into Spark storage settings).

A notebook reaches data through the Fabric default lakehouse (``/lakehouse/default``, ``Files/...``, ``Tables/...``), through OneLake URLs
(``abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/...``) or through logical names. This module resolves each *referenced lakehouse* to an OLCF
``lakehouse`` item, in this order:

1. an administrator mapping (``lakehouse_map``): run against an existing OLCF lakehouse - for example an on-prem copy of the data;
2. an existing item whose ``definition.fabric`` names the same Fabric workspace and lakehouse ids (or the same name in the mapped workspace);
3. a new OneLake item created from the capsule's default-lakehouse information, when ``auto_create_lakehouses`` is on and a broker connection exists.

The OneLake item uses the broker connection, so ``StorageService.spark_conf`` yields the ABFS OAuth settings for ``onelake.dfs.fabric.microsoft.com`` and the Spark
session can read and write OneLake directly. A reference that cannot be attached is reported *before* anything runs - the alternative is a job that dies on its first
read with an authentication error.

Data-scope rule (``enforce_data_scope``): OneLake references with a workspace *id* must name a mapped Fabric workspace. Paths computed at run time cannot be checked
statically; the broker service principal's permissions in Fabric are the real boundary (documented in ``docs/guides/fabric-assumptions.md``).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from olcf.core.context import AppContext
from olcf.core.errors import Conflict, NotFound, ValidationFailed
from olcf.core.models import DataRef, ItemCreate, ItemType, LakehouseRef
from olcf.fabric.config import FabricConfig
from olcf.fabric.errors import WorkspaceNotAllowed
from olcf.security.rbac import Principal
from olcf.storage.providers.onelake import ONELAKE_HOST, build_onelake_root

_GUID = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")


@dataclass
class FabricLakehouse:
    """A lakehouse as the capsule names it (ids and/or names)."""

    workspace_id: str | None = None
    workspace_name: str | None = None
    lakehouse_id: str | None = None
    lakehouse_name: str | None = None
    role: str = "read"
    origin: str = "context"

    def key(self) -> tuple[str, str]:
        return ((self.workspace_id or self.workspace_name or "").lower(), (self.lakehouse_id or self.lakehouse_name or "").lower())

    def label(self) -> str:
        return f"{self.workspace_name or self.workspace_id or '?'}/{self.lakehouse_name or self.lakehouse_id or '?'}"


@dataclass
class DataPlan:
    """The resolved data plan of a job: default lakehouse, data references, the lakehouses involved, those created on demand and the canonical references recorded for audit.
    """
    default_lakehouse: LakehouseRef | None = None
    data_refs: list[DataRef] = field(default_factory=list)
    lakehouses: list[dict[str, Any]] = field(default_factory=list)  # {item_id, name, workspace_id, provider, source, fabric}
    created: list[str] = field(default_factory=list)
    resolved: list[dict[str, Any]] = field(default_factory=list)  # canonical references for the audit trail
    problems: list[dict[str, Any]] = field(default_factory=list)
    default_item: dict[str, Any] | None = None

    def to_public(self) -> dict[str, Any]:
        return {"lakehouses": self.lakehouses, "created": self.created, "resolved": self.resolved, "problems": self.problems}


def references_from(context: dict[str, Any], logical: list[dict[str, Any]], analysis: dict[str, Any] | None, *, current_workspace_id: str | None) -> list[FabricLakehouse]:
    """Collect every Fabric lakehouse the capsule refers to: the default lakehouse, logical references, OneLake URLs found by the analyzer."""
    refs: dict[tuple[str, str], FabricLakehouse] = {}

    def add(ref: FabricLakehouse) -> None:
        if not (ref.lakehouse_id or ref.lakehouse_name):
            return
        cur = refs.get(ref.key())
        if cur is None:
            refs[ref.key()] = ref
        else:
            cur.workspace_id = cur.workspace_id or ref.workspace_id
            cur.workspace_name = cur.workspace_name or ref.workspace_name
            cur.lakehouse_id = cur.lakehouse_id or ref.lakehouse_id
            cur.lakehouse_name = cur.lakehouse_name or ref.lakehouse_name
            if ref.role == "default":
                cur.role = "default"

    dl_id = context.get("defaultLakehouseId")
    dl_name = context.get("defaultLakehouseName")
    if dl_id or dl_name:
        add(FabricLakehouse(workspace_id=context.get("defaultLakehouseWorkspaceId") or current_workspace_id, workspace_name=context.get("defaultLakehouseWorkspaceName") or context.get("currentWorkspaceName"),
                            lakehouse_id=dl_id, lakehouse_name=dl_name, role="default", origin="context"))
    for item in logical:
        if not isinstance(item, dict) or item.get("kind") not in ("lakehouse", "onelake"):
            continue
        add(FabricLakehouse(workspace_id=item.get("fabricWorkspaceId") or item.get("workspaceId"), workspace_name=item.get("workspaceName"),
                            lakehouse_id=item.get("fabricLakehouseId") or item.get("lakehouseId"), lakehouse_name=item.get("name") or item.get("lakehouseName"),
                            role="default" if item.get("role") == "default" else "read", origin="capsule"))
    for p in ((analysis or {}).get("data_references") or {}).get("paths", []):
        if p.get("kind") != "onelake":
            continue
        if (p.get("item_type") or "").lower() not in ("", "lakehouse"):
            continue
        ws = p.get("workspace")
        item = p.get("item")
        add(FabricLakehouse(workspace_id=ws if p.get("workspace_is_id") else None, workspace_name=None if p.get("workspace_is_id") else ws,
                            lakehouse_id=item if p.get("item_is_id") else None, lakehouse_name=None if p.get("item_is_id") else item, origin="onelake-url"))
    return list(refs.values())


class DataPlane:
    """Resolves referenced Fabric lakehouses against the OLCF catalogue."""

    def __init__(self, ctx: AppContext) -> None:
        self.ctx = ctx

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def onelake_root(ref: FabricLakehouse) -> str:
        ws = ref.workspace_id or ref.workspace_name
        if ref.lakehouse_id:
            item = ref.lakehouse_id
        elif ref.lakehouse_name:
            item = ref.lakehouse_name if ref.lakehouse_name.lower().endswith(".lakehouse") else f"{ref.lakehouse_name}.Lakehouse"
        else:
            raise ValidationFailed("the Fabric lakehouse has neither an id nor a name")
        if not ws:
            raise ValidationFailed(f"the Fabric workspace of lakehouse '{ref.label()}' is unknown")
        return build_onelake_root(ws, item)

    def _in_scope(self, cfg: FabricConfig, ref: FabricLakehouse, tenant_id: str | None, context: dict[str, Any]) -> None:
        if not cfg.enforce_data_scope:
            return
        mapped = {m.fabric_workspace_id for m in cfg.workspace_map if not m.tenant_id or not tenant_id or m.tenant_id.lower() == tenant_id.lower()}
        if ref.workspace_id:
            if ref.workspace_id.lower() not in mapped:
                raise WorkspaceNotAllowed(f"the notebook reads Fabric workspace {ref.workspace_id}, which is not mapped for this integration",
                                          remediation="map the workspace (PUT /v1/integrations/fabric workspace_map) or turn off enforce_data_scope",
                                          details={"fabric_workspace_id": ref.workspace_id})
            return
        if ref.workspace_name:
            known = {str(context.get(k) or "").lower() for k in ("currentWorkspaceName", "defaultLakehouseWorkspaceName")} - {""}
            if ref.workspace_name.lower() not in known:
                raise WorkspaceNotAllowed(f"the notebook reads Fabric workspace '{ref.workspace_name}' by name; only the notebook's own workspaces can be verified by name",
                                          remediation="address the workspace by id (abfss://<workspace-guid>@onelake...) or turn off enforce_data_scope")

    def _existing(self, olcf_ws_id: str, ref: FabricLakehouse, cfg: FabricConfig) -> tuple[dict[str, Any] | None, str]:
        items = self.ctx.service("items")
        for m in cfg.lakehouse_map:
            if m.fabric_workspace_id and ref.workspace_id and m.fabric_workspace_id.lower() != ref.workspace_id.lower():
                continue
            hit = ((m.fabric_lakehouse_id and ref.lakehouse_id and m.fabric_lakehouse_id.lower() == ref.lakehouse_id.lower())
                   or (m.fabric_lakehouse_name and ref.lakehouse_name and m.fabric_lakehouse_name.lower() == ref.lakehouse_name.lower()))
            if hit:
                try:
                    return items.resolve(olcf_ws_id, m.olcf_lakehouse, "lakehouse"), "mapping"
                except NotFound:
                    raise ValidationFailed(f"the lakehouse mapping for '{ref.label()}' points to '{m.olcf_lakehouse}', which does not exist in the OLCF workspace",
                                           remediation="fix lakehouse_map in the integration settings") from None
        rows, _ = items.list(olcf_ws_id, type_="lakehouse", limit=500)
        for r in rows:
            fab = (r.get("definition") or {}).get("fabric") or {}
            if ref.lakehouse_id and fab.get("lakehouse_id", "").lower() == ref.lakehouse_id.lower() and (not ref.workspace_id or fab.get("workspace_id", "").lower() == ref.workspace_id.lower()):
                return r, "existing"
        if ref.lakehouse_name:
            bare = ref.lakehouse_name.removesuffix(".Lakehouse")
            for r in rows:
                fab = (r.get("definition") or {}).get("fabric") or {}
                if r["name"].lower() == bare.lower() and (not fab or not ref.workspace_id or fab.get("workspace_id", "").lower() == ref.workspace_id.lower()):
                    return r, "existing-by-name"
        return None, ""

    def _create(self, cfg: FabricConfig, olcf_ws_id: str, ref: FabricLakehouse, actor: Principal) -> dict[str, Any]:
        if not cfg.broker_connection:
            raise ValidationFailed(f"the Fabric lakehouse '{ref.label()}' is not attached to OLCF and no broker connection is configured",
                                   remediation="set broker_connection (an OLCF 'onelake' connection) and auto_create_lakehouses, or map the lakehouse to an existing one (lakehouse_map)")
        conn = self.ctx.service("connections").get(cfg.broker_connection)
        if conn["type"] != "onelake":
            raise ValidationFailed(f"the broker connection '{conn['name']}' has type '{conn['type']}', not 'onelake'")
        if not (ref.workspace_id or ref.workspace_name):
            raise ValidationFailed(f"cannot attach '{ref.label()}': the Fabric workspace is unknown")
        root = self.onelake_root(ref)
        base = (ref.lakehouse_name or "").removesuffix(".Lakehouse")
        name = base if _NAME.match(base) else (ref.lakehouse_id or "lakehouse")
        fab = {"workspace_id": ref.workspace_id or ref.workspace_name, "lakehouse_id": ref.lakehouse_id or ref.lakehouse_name}
        items = self.ctx.service("items")
        for candidate in (name, f"{name}-{(ref.workspace_id or 'ws')[:8]}"):
            try:
                row = items.create(actor, olcf_ws_id, ItemCreate(
                    type=ItemType.LAKEHOUSE, name=candidate, description=f"Fabric lakehouse {ref.label()} (created by the Fabric integration)",
                    tags=["fabric", "onelake"], definition={"storage": {"connection_id": conn["id"], "root_uri": root}, "fabric": fab}))
                self.ctx.audit.record(actor, "fabric.lakehouse.attached", resource_type="lakehouse", resource_id=row["id"], workspace_id=olcf_ws_id,
                                      details={"fabric_workspace_id": fab["workspace_id"], "fabric_lakehouse_id": fab["lakehouse_id"], "root": root})
                return row
            except Conflict:
                continue
        raise Conflict(f"an item named '{name}' already exists in the OLCF workspace and is not the Fabric lakehouse '{ref.label()}'",
                       remediation="map the lakehouse explicitly (lakehouse_map) or rename the existing item")

    # ------------------------------------------------------------------ plan
    def plan(self, cfg: FabricConfig, *, olcf_workspace_id: str, tenant_id: str | None, context: dict[str, Any], logical: list[dict[str, Any]],
             analysis: dict[str, Any] | None, current_workspace_id: str | None, actor: Principal, dry_run: bool = False) -> DataPlan:
        """Resolve every referenced lakehouse. ``dry_run`` reports what would be attached without creating items (used by ``/validate``)."""
        plan = DataPlan()
        for ref in references_from(context, logical, analysis, current_workspace_id=current_workspace_id):
            self._in_scope(cfg, ref, tenant_id, context)
            row, how = self._existing(olcf_workspace_id, ref, cfg)
            if row is None:
                if not cfg.auto_create_lakehouses:
                    plan.problems.append({"lakehouse": ref.label(), "role": ref.role, "reason": "not attached to OLCF and auto_create_lakehouses is off",
                                          "remediation": "attach it (create a lakehouse item with definition.fabric) or enable auto_create_lakehouses with a broker connection, "
                                                         "or map it to an existing lakehouse with lakehouse_map"})
                    continue
                if dry_run:
                    plan.lakehouses.append({"item_id": None, "name": ref.lakehouse_name or ref.lakehouse_id, "workspace_id": olcf_workspace_id, "source": "would-create", "fabric": ref.label()})
                    continue
                try:
                    row = self._create(cfg, olcf_workspace_id, ref, actor)
                except ValidationFailed as exc:
                    plan.problems.append({"lakehouse": ref.label(), "role": ref.role, "reason": exc.message, "remediation": exc.remediation})
                    continue
                how = "created"
                plan.created.append(row["id"])
            fab = (row.get("definition") or {}).get("fabric")
            plan.lakehouses.append({"item_id": row["id"], "name": row["name"], "workspace_id": row["workspace_id"], "source": how, "fabric": ref.label()})
            plan.resolved.append({"fabric": ref.label(), "olcf_lakehouse": row["id"], "how": how, "root": (row.get("definition") or {}).get("storage", {}).get("root_uri")})
            role = "default" if ref.role == "default" else "read"
            plan.data_refs.append(DataRef(kind="lakehouse", ref=row["id"], role=role))
            if role == "default":
                plan.default_item = row
                plan.default_lakehouse = LakehouseRef(lakehouse_id=row["id"], fabric_workspace_id=(fab or {}).get("workspace_id") or ref.workspace_id,
                                                      fabric_lakehouse_id=(fab or {}).get("lakehouse_id") or ref.lakehouse_id)
        return plan

    def default_lakehouse_info(self, item: dict[str, Any] | None) -> dict[str, Any] | None:
        """The ``AnalysisOptions.lakehouse`` block for the resolved default lakehouse: ``local`` (path rewritable) or ``remote``."""
        if not item:
            return None
        st = self.ctx.maybe_service("storage")
        if st is None:
            return {"kind": "remote", "root_path": None, "name": item["name"]}
        lh = st.get_lakehouse(item)
        if lh.provider_id == "local":
            from olcf.storage.providers.local import uri_to_path

            return {"kind": "local", "root_path": str(uri_to_path(lh.root_uri)), "name": item["name"]}
        return {"kind": "remote", "root_path": None, "name": item["name"]}


__all__ = ["DataPlan", "DataPlane", "FabricLakehouse", "ONELAKE_HOST", "references_from"]
