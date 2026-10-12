# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The OneLake data plane: a Fabric lakehouse the capsule refers to becomes an OLCF lakehouse item whose Spark configuration is ABFS OAuth against OneLake, with the
resolution order, the data-scope guard and the auto-create switch."""

from __future__ import annotations

import json
from typing import Any

import pytest

from olcf.core.errors import ValidationFailed
from olcf.fabric.errors import WorkspaceNotAllowed
from olcf.fabric.onelake import DataPlane, FabricLakehouse, references_from
from olcf.fabric.service import FabricService
from olcf.storage.providers.onelake import ONELAKE_HOST
from tests.fabric.fabric_fakes import (
    BROKER_CLIENT_ID,
    BROKER_SECRET,
    FABRIC_WS,
    LAKEHOUSE,
    OTHER_FABRIC_WS,
    TENANT,
    FakeFabricWorld,
)
from tests.fabric.fabric_stack import FabricStack

LH2 = "33333333-3333-4333-8333-333333333333"


@pytest.fixture()
def env(stack: FabricStack, world: FakeFabricWorld) -> dict[str, Any]:
    ws = stack.workspace()
    secret = stack.http.post("/v1/secrets", headers=stack.admin, json={"name": "broker-" + ws[-6:], "value": {"client_secret": BROKER_SECRET}}).json()
    conn = stack.http.post("/v1/connections", headers=stack.admin, json={"name": "onelake-" + ws[-6:], "type": "onelake", "shared": True, "secret_id": secret["id"],
                                                                         "config": {"workspace": FABRIC_WS, "lakehouse": LAKEHOUSE, "auth": "service_principal", "tenant_id": TENANT,
                                                                                    "client_id": BROKER_CLIENT_ID, "endpoint": world.base}}).json()
    stack.put_fabric(allowed_tenants=[TENANT], workspace_map=[{"fabric_workspace_id": FABRIC_WS, "olcf_workspace": ws}], broker_connection=conn["id"], auto_create_lakehouses=True,
                     network=world.network_config())
    stack.service_principal(ws)
    row = stack.ctx.service("identity").get(stack.last_principal_id)
    actor = FabricService._internal(stack.ctx.authz.principal_from_row(row), ws)          # noqa: SLF001 - the identity the adapter acts with
    return {"ws": ws, "conn": conn["id"], "stack": stack, "actor": actor}


def plan(env: dict[str, Any], *, context: dict[str, Any] | None = None, logical: list | None = None, analysis: dict | None = None, dry_run: bool = False, **cfg: Any):
    st: FabricStack = env["stack"]
    if cfg:
        base = st.http.get("/v1/integrations/fabric", headers=st.admin).json()["config"]
        base.update(cfg)
        st.put_fabric(**base)
    ctxn = {"currentWorkspaceId": FABRIC_WS, "currentWorkspaceName": "Sales", "defaultLakehouseId": LAKEHOUSE, "defaultLakehouseName": "bronze", "defaultLakehouseWorkspaceId": FABRIC_WS}
    ctxn.update(context or {})
    return DataPlane(st.ctx).plan(st.fabric.config(), olcf_workspace_id=env["ws"], tenant_id=TENANT, context=ctxn, logical=logical or [], analysis=analysis, current_workspace_id=FABRIC_WS,
                                  dry_run=dry_run, actor=env["actor"])


def test_default_lakehouse_is_attached_and_spark_gets_abfs_oauth(env: dict[str, Any]) -> None:
    st: FabricStack = env["stack"]
    p = plan(env)
    assert len(p.created) == 1 and p.default_item and p.problems == []
    item = p.default_item
    fab = item["definition"]["fabric"]
    assert fab == {"workspace_id": FABRIC_WS, "lakehouse_id": LAKEHOUSE}
    assert item["definition"]["storage"]["root_uri"] == f"abfss://{FABRIC_WS}@{ONELAKE_HOST}/{LAKEHOUSE}"
    assert p.default_lakehouse.fabric_workspace_id == FABRIC_WS and p.default_lakehouse.fabric_lakehouse_id == LAKEHOUSE

    conf = st.ctx.service("storage").spark_conf(item)
    h = "spark.hadoop."
    assert conf[f"{h}fs.azure.account.auth.type.{ONELAKE_HOST}"] == "OAuth"
    assert conf[f"{h}fs.azure.account.oauth.provider.type.{ONELAKE_HOST}"].endswith("ClientCredsTokenProvider")
    assert conf[f"{h}fs.azure.account.oauth2.client.id.{ONELAKE_HOST}"] == BROKER_CLIENT_ID
    assert conf[f"{h}fs.azure.account.oauth2.client.secret.{ONELAKE_HOST}"] == BROKER_SECRET
    assert conf[f"{h}fs.azure.account.oauth2.client.endpoint.{ONELAKE_HOST}"].endswith(f"/{TENANT}/oauth2/token")
    assert all(ONELAKE_HOST in k for k in conf if "fs.azure.account" in k)                       # nothing for any other host
    # the descriptor that goes into the job spec carries NO credential, the storage_conf (0600, deleted with the job) does
    desc = st.ctx.service("storage").job_lakehouse_descriptor(item)
    assert BROKER_SECRET not in json.dumps(desc) and desc["fabric"] == fab and desc["root_uri"].startswith("abfss://")
    assert st.ctx.service("storage").job_storage_conf(item)["spark"][f"{h}fs.azure.account.oauth2.client.secret.{ONELAKE_HOST}"] == BROKER_SECRET


def test_the_secret_is_never_part_of_the_item_or_public_listings(env: dict[str, Any]) -> None:
    st: FabricStack = env["stack"]
    plan(env)
    listing = st.http.get("/v1/lakehouses", headers=st.admin, params={"workspace_id": env["ws"]}).text
    assert BROKER_SECRET not in listing and "client_secret" not in listing.lower().replace("client_secret_name", "")
    assert BROKER_SECRET not in st.http.get(f"/v1/connections/{env['conn']}", headers=st.admin).text


def test_second_plan_reuses_the_attached_item(env: dict[str, Any]) -> None:
    first = plan(env)
    again = plan(env)
    assert again.created == [] and again.default_item["id"] == first.default_item["id"] and again.lakehouses[0]["source"] == "existing"


def test_resolution_by_name_and_additional_lakehouses_from_onelake_urls(env: dict[str, Any]) -> None:
    analysis = {"data_references": {"paths": [{"kind": "onelake", "workspace": FABRIC_WS, "workspace_is_id": True, "item": "silver", "item_is_id": False, "item_type": "Lakehouse",
                                               "area": "Tables", "path": "t", "uri": "abfss://x"}]}}
    p = plan(env, analysis=analysis)
    names = sorted(x["name"] for x in p.lakehouses)
    assert names == ["bronze", "silver"]
    silver = next(x for x in p.lakehouses if x["name"] == "silver")
    assert silver["source"] == "created"
    root = next(r["root"] for r in p.resolved if r["fabric"].endswith("silver"))
    assert root == f"abfss://{FABRIC_WS}@{ONELAKE_HOST}/silver.Lakehouse"
    assert [d.role for d in p.data_refs] == ["default", "read"] or sorted(d.role for d in p.data_refs) == ["default", "read"]


def test_logical_references_from_the_capsule(env: dict[str, Any]) -> None:
    p = plan(env, context={"defaultLakehouseId": None, "defaultLakehouseName": None},
             logical=[{"kind": "lakehouse", "role": "default", "fabricLakehouseId": LH2, "name": "curated", "fabricWorkspaceId": FABRIC_WS}])
    assert p.default_item["name"] == "curated" and p.default_item["definition"]["fabric"]["lakehouse_id"] == LH2


def test_mapping_to_an_existing_olcf_lakehouse_wins(env: dict[str, Any]) -> None:
    st: FabricStack = env["stack"]
    r = st.http.post(f"/v1/workspaces/{env['ws']}/lakehouses", headers=st.admin, json={"name": "local_bronze", "default_format": "delta"})
    assert r.status_code == 201, r.text
    p = plan(env, lakehouse_map=[{"fabric_workspace_id": FABRIC_WS, "fabric_lakehouse_id": LAKEHOUSE, "olcf_lakehouse": "local_bronze"}])
    assert p.created == [] and p.default_item["name"] == "local_bronze" and p.lakehouses[0]["source"] == "mapping"
    info = DataPlane(st.ctx).default_lakehouse_info(p.default_item)
    assert info and info["kind"] == "local" and "local_bronze" in info["root_path"]


def test_a_mapping_to_a_missing_lakehouse_is_reported(env: dict[str, Any]) -> None:
    with pytest.raises(ValidationFailed) as ei:
        plan(env, lakehouse_map=[{"fabric_workspace_id": FABRIC_WS, "fabric_lakehouse_id": LAKEHOUSE, "olcf_lakehouse": "does-not-exist"}])
    assert ei.value.remediation


def test_dry_run_creates_nothing(env: dict[str, Any]) -> None:
    st: FabricStack = env["stack"]
    p = plan(env, dry_run=True)
    assert p.created == [] and p.lakehouses[0]["source"] == "would-create"
    rows = st.http.get(f"/v1/workspaces/{env['ws']}/lakehouses", headers=st.admin).json()["items"]
    assert not any((r.get("fabric") or {}).get("lakehouse_id") == LAKEHOUSE for r in rows)


def test_without_auto_create_the_missing_lakehouse_is_a_problem_with_remediation(env: dict[str, Any]) -> None:
    p = plan(env, auto_create_lakehouses=False)
    assert p.created == [] and p.default_item is None and "auto_create_lakehouses" in p.problems[0]["reason"] and p.problems[0]["remediation"]


def test_unmapped_workspace_is_refused_by_the_data_scope_guard(env: dict[str, Any]) -> None:
    other = {"kind": "onelake", "workspace": OTHER_FABRIC_WS, "workspace_is_id": True, "item": LH2, "item_is_id": True, "item_type": "Lakehouse", "area": "Files", "path": "x", "uri": "abfss://y"}
    with pytest.raises(WorkspaceNotAllowed) as ei:
        plan(env, analysis={"data_references": {"paths": [other]}})
    assert OTHER_FABRIC_WS in str(ei.value)
    p = plan(env, analysis={"data_references": {"paths": [other]}}, enforce_data_scope=False)           # an administrator may widen the scope on purpose
    assert any(x["fabric"].startswith(OTHER_FABRIC_WS) or OTHER_FABRIC_WS in x["fabric"] for x in p.lakehouses)


def test_by_name_references_to_foreign_workspaces_cannot_be_verified(env: dict[str, Any]) -> None:
    named = {"kind": "onelake", "workspace": "SomeoneElsesWorkspace", "workspace_is_id": False, "item": "x", "item_is_id": False, "item_type": "Lakehouse", "area": "Files", "path": "", "uri": "abfss://z"}
    with pytest.raises(WorkspaceNotAllowed):
        plan(env, analysis={"data_references": {"paths": [named]}})


def test_non_lakehouse_items_and_other_schemes_are_ignored() -> None:
    wh = {"kind": "onelake", "workspace": FABRIC_WS, "workspace_is_id": True, "item": "w", "item_is_id": False, "item_type": "Warehouse", "uri": "abfss://w"}
    refs = references_from({}, [], {"data_references": {"paths": [wh, {"kind": "s3", "uri": "s3://b/x"}, {"kind": "relative", "path": "x"}]}}, current_workspace_id=FABRIC_WS)
    assert refs == []


def test_references_are_deduplicated_and_merged() -> None:
    ctx = {"defaultLakehouseId": LAKEHOUSE, "defaultLakehouseName": "bronze", "defaultLakehouseWorkspaceId": FABRIC_WS}
    refs = references_from(ctx, [{"kind": "lakehouse", "fabricLakehouseId": LAKEHOUSE, "name": "bronze", "fabricWorkspaceId": FABRIC_WS, "role": "default"}], None, current_workspace_id=FABRIC_WS)
    assert len(refs) == 1 and refs[0].role == "default"
    assert FabricLakehouse(workspace_id="A", lakehouse_id="B").key() == ("a", "b")


def test_onelake_root_spellings() -> None:
    assert DataPlane.onelake_root(FabricLakehouse(workspace_id="ws", lakehouse_name="Sales")) == f"abfss://ws@{ONELAKE_HOST}/Sales.Lakehouse"
    assert DataPlane.onelake_root(FabricLakehouse(workspace_id="ws", lakehouse_name="Sales.Lakehouse")) == f"abfss://ws@{ONELAKE_HOST}/Sales.Lakehouse"
    assert DataPlane.onelake_root(FabricLakehouse(workspace_id="ws", lakehouse_id="guid")) == f"abfss://ws@{ONELAKE_HOST}/guid"
    with pytest.raises(ValidationFailed):
        DataPlane.onelake_root(FabricLakehouse(lakehouse_name="x"))
