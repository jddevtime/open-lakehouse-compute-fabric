# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""A run submitted from a Microsoft Fabric notebook is tied to the Fabric assets the tenant scan discovers (the promise of the product: OLCF compute *extends* Fabric).

The path under test is the real one, nothing in the middle is replaced:

    simulated Fabric notebook session (fake ``notebookutils``) -> the stdlib-only SDK ``olcf_fabric.run_remote()`` -> signed capsule -> ``POST /v1/integrations/fabric/submit`` ->
    adapter plan (tenant, workspace map, default lakehouse attached) -> capsule job on a real worker agent (Spark-less look-alike runtime) -> in-job lineage probe ->
    ``dataset.read`` / ``dataset.written`` -> Intelligence inbox -> ``runtime.facts_from_event`` -> verified edges

and next to it the Intelligence **Fabric source** scanning the very same Fabric tenant (``FakeFabricWorld``, a real HTTP server reproducing the documented Fabric REST / OneLake / Power
BI scanner shapes). The notebook run reads ``bronze_orders`` and writes ``silver_orders`` of the Fabric lakehouse ``lh_sales`` (an OLCF lakehouse that
records the Fabric mapping, ``definition.fabric``) with table names built at run time, so static analysis finds nothing: every edge asserted here can only come from the run.

Both orders are covered: ``scan_first`` (the tenant scan has created the notebook and the tables, the run attaches to them) and ``events_first`` (the run creates placeholders keyed by the
Fabric aliases, the later scan promotes them in place).
"""

from __future__ import annotations

import io
import json
import sys
from collections.abc import Iterator
from typing import Any

import pytest
from sqlalchemy import select

from olcf.core.config import Settings
from olcf.intelligence.schema import asset_aliases, assets, edges, gaps
from olcf.storage.providers.local import uri_to_path
from tests.fabric.fabric_fakes import (
    API_AUDIENCE,
    BROKER_SECRET,
    FABRIC_WS,
    LAKEHOUSE,
    TENANT,
    FakeFabricWorld,
    FakeNotebookUtils,
    allow_private_endpoints,
)
from tests.fabric.fabric_support import SUBMIT_CELL, ipynb, notebook_context
from tests.intelligence.system import fabric_scenario as sc
from tests.intelligence.system.conftest import RUNTIME_ID, LivePlatform, build_live_platform

NOTEBOOK_NAME = "nb_bronze_to_silver"

#: Python (deltalake) body of the Fabric notebook as it runs on OLCF compute: the table names are computed, so a static reading of it finds no table.
NB_BODY = '''import os
from pathlib import Path

import pyarrow.compute as pc
from deltalake import DeltaTable, write_deltalake
from olcf.runtime_lib import paths

tables = Path(paths.resolve("Tables/bronze_orders", for_="local")).parent
src_name = next(n for n in sorted(os.listdir(tables)) if n.startswith("bronze_"))
dst_name = "silver_" + "orders"
rows = DeltaTable(str(tables / src_name)).to_pyarrow_table()
rows = rows.filter(pc.greater(rows["amount"], 0.0))
write_deltalake(str(tables / dst_name), rows, mode="overwrite")
print("written", dst_name, rows.num_rows)
'''


class FabricRun:
    """What the module-scoped scenario observed, shared by the tests."""

    platform: LivePlatform
    world: FakeFabricWorld
    order: str
    source_id: str
    olcf_workspace: str
    lakehouse_item: str
    job: dict[str, Any]
    submission: dict[str, Any]
    scan: dict[str, Any]
    inbox: list[dict[str, Any]]


def _write_local_table(root: str, name: str) -> None:
    import pyarrow as pa
    from deltalake import write_deltalake

    rows = pa.table({"order_id": [1, 2, 3], "customer_id": [10, 20, 30], "amount": [5.0, 0.0, 7.5], "channel": ["web", "web", "shop"]})
    write_deltalake(str(uri_to_path(root) / "Tables" / name), rows, mode="overwrite")


def _inbox_for(p: LivePlatform, job_id: str) -> list[dict[str, Any]]:
    from olcf.intelligence.schema import inbox

    with p.ctx.db.connect() as c:
        return [dict(r._mapping) for r in c.execute(select(inbox).order_by(inbox.c.seq)) if (r.payload or {}).get("process", {}).get("job_id") == job_id]


def _names(p: LivePlatform) -> dict[str, str]:
    with p.ctx.db.connect() as c:
        return {r.id: r.name for r in c.execute(select(assets.c.id, assets.c.name))}


def _edges(p: LivePlatform, *types_: str) -> list[dict[str, Any]]:
    with p.ctx.db.connect() as c:
        return [dict(r._mapping) for r in c.execute(select(edges).where(edges.c.status == "active", edges.c.type.in_(types_)))]


def _aliases(p: LivePlatform, asset_id: str) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    with p.ctx.db.connect() as c:
        for r in c.execute(select(asset_aliases.c.kind, asset_aliases.c.value).where(asset_aliases.c.asset_id == asset_id)):
            out.setdefault(r.kind, set()).add(r.value)
    return out


@pytest.fixture(scope="module", params=["scan_first", "events_first"])
def fabric_run(request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory) -> Iterator[FabricRun]:
    world = FakeFabricWorld().start()
    sc.populate(world, denied_workspace=False)
    # the Fabric notebook the run claims to be: its stored definition computes its table names, so the tenant scan derives no edge for it
    world.add_notebook(FABRIC_WS, sc.NB_BRONZE_SILVER, ipynb=ipynb([("code", NB_BODY)]))

    def settings(s: Settings) -> None:
        s.intelligence.scan_on_start = False                       # the only scan is the one the test asks for

    mp = pytest.MonkeyPatch()
    try:
        with build_live_platform(tmp_path_factory.mktemp(request.param), install_sample=False, agent=True, setup=settings, runtime_family="fabric-2.0-compatible") as p:
            r = FabricRun()
            r.platform, r.world, r.order = p, world, request.param
            allow_private_endpoints(p.ctx)
            ws = p.post("/v1/workspaces", {"name": "fabric-target"}, expect=201)
            r.olcf_workspace = ws["id"]

            # --- the Fabric integration: tenant allow-list, workspace map; the platform identity a Fabric notebook uses
            secret = p.post("/v1/secrets", {"name": "fabric-broker", "value": {"client_secret": BROKER_SECRET}}, expect=201)
            conn = p.post("/v1/connections", {"name": "fabric-broker", "type": "onelake", "shared": True, "secret_id": secret["id"],
                                              "config": {"workspace": FABRIC_WS, "lakehouse": LAKEHOUSE, "auth": "service_principal", "tenant_id": world.tenant,
                                                         "client_id": world.client_id, "endpoint": world.base}}, expect=201)
            assert p.http.put("/v1/integrations/fabric", headers=p.admin, json={
                "allowed_tenants": [TENANT], "audiences": [API_AUDIENCE], "workspace_map": [{"fabric_workspace_id": FABRIC_WS, "olcf_workspace": ws["id"]}],
                "broker_connection": conn["id"], "network": world.network_config(),
                "default_runtime": {"family": "fabric-2.0-compatible", "runtime_id": RUNTIME_ID, "engine": "python"}}).status_code == 200
            sp = p.post("/v1/identity/users", {"name": "fabric-sp", "kind": "service", "roles": [{"role": "ServicePrincipal", "scope_type": "workspace", "scope_id": ws["id"]}]}, expect=201)
            tok = p.post("/v1/identity/tokens", {"name": "t", "principal_id": sp["id"], "scopes": ["fabric"]}, expect=201)["token"]

            # --- lh_sales: the OLCF lakehouse that records which Fabric lakehouse it is (definition.fabric), holding the data the notebook reads
            lh = p.post(f"/v1/workspaces/{ws['id']}/lakehouses", {"name": "lh_sales", "fabric": {"workspace_id": FABRIC_WS, "lakehouse_id": LAKEHOUSE}}, expect=201)
            r.lakehouse_item = lh["id"]
            _write_local_table(lh["root_uri"], "bronze_orders")

            # --- the Intelligence source over the same tenant, its Fabric workspace mapped to the OLCF workspace
            body = {"name": "Contoso Fabric", "kind": "fabric", "credentials": {"client_secret": BROKER_SECRET},
                    "config": {"tenant_id": TENANT, "metadata_identity": {"tenant_id": TENANT, "client_id": world.client_id}, "network": world.discovery_network_config(),
                               "workspace_map": {FABRIC_WS: ws["id"]}}}
            r.source_id = p.post("/v1/intelligence/sources", body, expect=201)["id"]

            def scan() -> dict[str, Any]:
                doc = p.post(f"/v1/intelligence/sources/{r.source_id}/scan", {"mode": "full"}, expect=202, wait="true")
                return dict(doc.get("scan", doc))

            if request.param == "scan_first":
                r.scan = scan()

            # --- a Fabric notebook session runs the cell that submits the notebook to OLCF
            ctx = notebook_context(currentNotebookName=NOTEBOOK_NAME, currentNotebookId=sc.NB_BRONZE_SILVER, currentWorkspaceId=FABRIC_WS, currentWorkspaceName="Sales",
                                   defaultLakehouseName="lh_sales", defaultLakehouseId=LAKEHOUSE, defaultLakehouseWorkspaceId=FABRIC_WS, defaultLakehouseWorkspaceName="Sales")
            document = ipynb([("code", NB_BODY), ("code", SUBMIT_CELL)])
            nbu = FakeNotebookUtils(context=ctx, definitions={NOTEBOOK_NAME: {None: document, "ipynb": document}})
            mp.setitem(sys.modules, "notebookutils", nbu)  # type: ignore[arg-type]
            import olcf_fabric as olcf

            olcf.config.reset_settings()
            olcf.configure(endpoint=p.http.base_url.__str__().rstrip("/"), token=tok, tenant_id=TENANT, allow_insecure_http=True)
            run = olcf.run_remote(wait=False)
            run.wait(stream_logs=False, out=io.StringIO())
            assert run.state == "succeeded", (run.data.get("error"), (run.data.get("job") or {}).get("failure"))
            r.submission = p.get(f"/v1/integrations/fabric/submissions/{run.submission_id}", who={"Authorization": f"Bearer {tok}"})
            r.job = p.get(f"/v1/jobs/{run.job_id}")
            p.drain()
            r.inbox = _inbox_for(p, run.job_id)
            if request.param == "events_first":
                r.scan = scan()
            p.drain()
            yield r
            olcf.config.reset_settings()
    finally:
        mp.undo()
        world.stop()


def test_the_run_came_through_the_real_adapter_path_and_carries_the_fabric_identifiers(fabric_run: FabricRun) -> None:
    j, sub = fabric_run.job, fabric_run.submission
    assert j["state"] == "succeeded", j.get("failure")
    assert j["kind"] == "capsule" and j["origin"]["kind"] == "fabric" and j["item_id"] is None          # a Fabric run has no OLCF notebook item: nothing to key the process by
    fab = j["origin"]["fabric"] if "fabric" in j["origin"] else j["origin"]["details"]["fabric"]
    assert (fab["tenant_id"], fab["workspace_id"], fab["notebook_id"]) == (TENANT, FABRIC_WS, sc.NB_BRONZE_SILVER)       # stored at submission, nothing to add
    assert sub["state"] == "succeeded" and sub["notebook_id"] == sc.NB_BRONZE_SILVER


def test_the_events_carry_the_fabric_identifiers_additively(fabric_run: FabricRun) -> None:
    by_type = {r["type"]: r["payload"] for r in fabric_run.inbox}
    assert {"dataset.read", "dataset.written"} <= set(by_type), sorted(by_type)
    for t in ("dataset.read", "dataset.written"):
        proc = by_type[t]["process"]
        assert proc["fabric"] == {"tenant_id": TENANT, "workspace_id": FABRIC_WS, "item_id": sc.NB_BRONZE_SILVER, "item_name": NOTEBOOK_NAME, "item_type": "Notebook",
                                  "run_id": proc["fabric"].get("run_id")}
        assert proc["kind"] == "notebook" and proc["job_id"] == fabric_run.job["id"] and proc["item_id"] is None                  # the existing keys are untouched
        for d in by_type[t]["datasets"]:
            assert d["lakehouse_id"] == fabric_run.lakehouse_item and d["fabric"] == {"workspace_id": FABRIC_WS, "lakehouse_id": LAKEHOUSE}
    assert {d["name"] for d in by_type["dataset.read"]["datasets"]} == {"bronze_orders"} and {d["name"] for d in by_type["dataset.written"]["datasets"]} == {"silver_orders"}
    assert all(r["status"] == "done" and r["error"] is None for r in fabric_run.inbox)


def test_the_runtime_edges_connect_the_tenant_discovered_notebook_to_the_tenant_discovered_tables(fabric_run: FabricRun) -> None:
    p = fabric_run.platform
    assert fabric_run.scan["status"] == "succeeded", fabric_run.scan
    notebook, bronze, silver = p.asset("notebook", NOTEBOOK_NAME), p.asset("table", "bronze_orders"), p.asset("table", "silver_orders")
    # these ARE the assets of the tenant scan: the Fabric aliases are on them, and they belong to the Fabric source
    assert f"{TENANT}/{FABRIC_WS}/{sc.NB_BRONZE_SILVER}" in _aliases(p, notebook)["fabric"]
    assert f"{TENANT}/{FABRIC_WS}/{LAKEHOUSE}/bronze_orders" in _aliases(p, bronze)["fabric"] and f"{TENANT}/{FABRIC_WS}/{LAKEHOUSE}/silver_orders" in _aliases(p, silver)["fabric"]
    with p.ctx.db.connect() as c:
        rows = {r.id: r for r in c.execute(select(assets.c.id, assets.c.status, assets.c.source_id, assets.c.workspace_id).where(assets.c.id.in_([notebook, bronze, silver])))}
    assert {r.status for r in rows.values()} == {"active"} and {r.source_id for r in rows.values()} == {fabric_run.source_id}
    assert {r.workspace_id for r in rows.values()} == {fabric_run.olcf_workspace}
    runtime = {(e["type"], e["src_id"], e["dst_id"]): e for e in _edges(p, "READS_FROM", "WRITES_TO") if e["confidence"] == "verified"}
    assert ("READS_FROM", notebook, bronze) in runtime and ("WRITES_TO", notebook, silver) in runtime, runtime.keys()
    for key in (("READS_FROM", notebook, bronze), ("WRITES_TO", notebook, silver)):
        detail = p.get(f"/v1/intelligence/edges/{runtime[key]['id']}")
        rt = [ev for ev in detail["evidence"] if ev["kind"] == "runtime"]
        assert rt and all(ev["confidence"] == "verified" and ev["ref"] == fabric_run.job["id"] for ev in rt), detail["evidence"]
        assert any(ev["location"].get("fabric_item") == sc.NB_BRONZE_SILVER and ev["location"].get("fabric_workspace") == FABRIC_WS and ev["location"].get("job_id") == fabric_run.job["id"]
                   for ev in rt), rt
    # static analysis of the Fabric notebook found nothing (names built at run time): these edges exist because the run happened
    assert not [e for e in _edges(p, "READS_FROM", "WRITES_TO") if e["src_id"] == notebook and e["confidence"] == "derived"]


def test_no_job_level_process_asset_and_no_duplicate_assets_were_made(fabric_run: FabricRun) -> None:
    p = fabric_run.platform
    with p.ctx.db.connect() as c:
        job_assets = [(r.type, r.name) for r in c.execute(select(assets.c.type, assets.c.name).where(assets.c.type.in_(["spark_job", "script"])))]
        dups = [(r.type, r.name) for r in c.execute(select(assets.c.type, assets.c.name).where(assets.c.status != "removed", assets.c.type.in_(["notebook", "table", "lakehouse"])))]
        open_gaps = [g.kind for g in c.execute(select(gaps.c.kind).where(gaps.c.resolved_at.is_(None)))]
        placeholders = c.execute(select(assets.c.name).where(assets.c.status == "placeholder")).all()
    assert job_assets == []                                                                 # no 'olcf-job:<id>' spark_job / script next to the real notebook
    assert len(dups) == len(set(dups)), dups                                                # the notebook, the tables and the lakehouse exist once each
    assert "unresolved_reference" not in open_gaps and placeholders == []
    if fabric_run.order == "events_first":
        stats = fabric_run.scan["stats"]["apply"]
        assert stats["assets_created"] > 0                                                  # the scan created what the run could not know (columns, items nothing ran) ...
        # ... and promoted, in place, the placeholders the run had created for the notebook and the two tables
        assert {"nb_bronze_to_silver", "bronze_orders", "silver_orders"} <= {n for n in _names(p).values()}


def test_lineage_runs_through_the_fabric_notebook_from_the_tenant_tables_to_the_model(fabric_run: FabricRun) -> None:
    """The point of the link: the runtime edge is one link of the chain the Fabric scan drew (silver -> Orders model table -> semantic model), so impact crosses it."""
    p = fabric_run.platform
    impact = {i["name"] for group in p.impact(p.asset("table", "bronze_orders"))["by_type"].values() for i in group["items"]}
    assert {"silver_orders", "Sales Model"} <= impact, impact
    up = p.lineage(p.asset("table", "silver_orders"), direction="up", depth=2)
    assert {"nb_bronze_to_silver", "bronze_orders"} <= {n["name"] for n in up["nodes"]}
    origin = p.origin(p.asset("report", "Sales Report"))
    assert any(NOTEBOOK_NAME in [nodes_by_id["name"] for nodes_by_id in [{n["id"]: n for n in origin["nodes"]}[i] for i in x["nodes"]]] for x in origin["paths"])


def test_replaying_the_events_changes_nothing(fabric_run: FabricRun) -> None:
    p = fabric_run.platform
    before = {e["id"] for e in _edges(p, "READS_FROM", "WRITES_TO")}
    count = len(_names(p))
    for r in fabric_run.inbox:
        if r["type"] in ("dataset.read", "dataset.written"):
            p.ctx.events.publish(r["type"], {k: v for k, v in r["payload"].items() if k != "_meta"}, workspace_id=r["workspace_id"], subject=fabric_run.job["id"])
    p.drain()
    assert {e["id"] for e in _edges(p, "READS_FROM", "WRITES_TO")} == before and len(_names(p)) == count


def test_without_the_notebook_guid_the_run_is_matched_by_name_and_the_edges_are_only_inferred(fabric_run: FabricRun) -> None:
    """The documented fallback: an event that names the Fabric workspace and the notebook's name but not its GUID (an old or partial capsule) is attached to the notebook of that name in the
    OLCF workspace the Fabric workspace maps to - and says so: ``inferred`` edges, evidence marked ``attribution: name``."""
    p = fabric_run.platform
    other = p.asset("notebook", "nb_silver_to_gold")
    payload = {"workspace_id": fabric_run.olcf_workspace, "engine": "python", "mode": None, "statement_hash": None, "at": "2026-03-04T05:06:07.000Z",
               "process": {"kind": "notebook", "item_id": None, "job_id": "job_by_name", "query_id": None, "name": "nb_silver_to_gold (Fabric)", "run_id": None,
                           "fabric": {"workspace_id": FABRIC_WS, "item_name": "nb_silver_to_gold", "item_type": "Notebook"}},
               "datasets": [{"kind": "table", "lakehouse_id": fabric_run.lakehouse_item, "name": "bronze_orders", "path": None, "uri": None, "format": "delta",
                             "fabric": {"workspace_id": FABRIC_WS, "lakehouse_id": LAKEHOUSE}}]}
    p.ctx.events.publish("dataset.read", payload, workspace_id=fabric_run.olcf_workspace, subject="job_by_name")
    p.drain()
    bronze = p.asset("table", "bronze_orders")             # a table the gold notebook's own code does not read: the edge below can only come from the event
    by_name = [e for e in _edges(p, "READS_FROM") if e["src_id"] == other and e["dst_id"] == bronze and e["confidence"] == "inferred"]
    assert len(by_name) == 1, [(e["confidence"], e["src_id"]) for e in _edges(p, "READS_FROM")]
    ev = p.get(f"/v1/intelligence/edges/{by_name[0]['id']}")["evidence"]
    assert any(x["kind"] == "runtime" and x["confidence"] == "inferred" and x["location"].get("attribution") == "name" for x in ev), ev
    assert len([n for n in _names(p).values() if n == "nb_silver_to_gold"]) == 1               # attached to the discovered notebook, no copy of it
    assert json.dumps(ev).count(BROKER_SECRET) == 0


def test_the_other_fabric_notebook_is_not_confused_with_the_one_that_ran(fabric_run: FabricRun) -> None:
    p = fabric_run.platform
    other = p.asset("notebook", "nb_silver_to_gold")
    writers_of_bronze = {e["src_id"] for e in _edges(p, "WRITES_TO") if e["dst_id"] == p.asset("table", "bronze_orders")}
    assert other not in writers_of_bronze
