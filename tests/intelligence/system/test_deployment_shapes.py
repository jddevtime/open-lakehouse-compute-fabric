# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""The platform in every deployment shape it is sold in, each one a real application built from the real module list.

1. **Without Intelligence** - the platform is whole: its API answers 404 for ``/v1/intelligence/*`` and ``/v1/governance/*``, the OpenAPI document does not list them, so the UI
   (which asks ``hasRoute`` before it shows the *Intelligence* group) hides the navigation; the MCP endpoint offers no ``intel_*`` / ``governance_*`` tool, the sample installs
   without an ``intelligence`` section, SQL and the catalog (which publish the events Intelligence would consume) work unchanged.
2. **Intelligence with the platform and no Fabric** - everything works from the local lakehouse and the imported Power BI scan; a Fabric source cannot be created without an
   identity, and one that cannot authenticate fails with a step-by-step explanation without touching the graph.
3. **Fabric only** - a service-principal source against the Fabric test server (``tests/fabric/fabric_fakes.py``, which reproduces the documented REST behaviour): the connection test passes every step, a scan reads workspaces, lakehouse, tables, notebooks, a pipeline, a semantic model and a report and links them, the workspace
   the principal may not read is a gap and not a failure, an incremental scan analyses nothing when nothing changed and only the changed notebook when one did.
4. **Fabric and on-premises together** - an OLCF notebook that reads and writes the same OneLake paths the Fabric notebooks use; each table is ONE asset (aliases of both worlds)
   whichever side is scanned first, and lineage and impact cross the boundary (the OLCF notebook writes, the Fabric semantic model reads).

Database parity: nothing here depends on SQLite (the tests use the application's own SQLAlchemy layer); PostgreSQL itself is not exercised by this suite.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select

from olcf.api.modules import MODULES
from olcf.intelligence import model as M
from olcf.intelligence.schema import asset_aliases, assets, edges, gaps
from tests.fabric.fabric_fakes import (
    BROKER_CLIENT_ID,
    BROKER_SECRET,
    FABRIC_WS,
    LAKEHOUSE,
    TENANT,
    FakeFabricWorld,
    allow_private_endpoints,
)
from tests.intelligence.system import fabric_scenario as sc
from tests.intelligence.system.conftest import Platform, build_platform

ROOT = Path(__file__).resolve().parents[3]
STATIC_JS = ROOT / "src" / "olcf" / "web" / "static" / "js"
WITHOUT_INTELLIGENCE = [m for m in MODULES if m != "olcf.intelligence.module"]
WITHOUT_FABRIC = [m for m in MODULES if m != "olcf.fabric.module"]
FABRIC_ONLY = ["olcf.api.core.module", "olcf.intelligence.module"]
WRONG_SECRET = "wrong-secret-" + "0123456789"      # built at run time so no credential-shaped literal sits in the source (secret-scan gate)
INTEL_ROUTES = ("/v1/intelligence/summary", "/v1/intelligence/assets", "/v1/intelligence/sources", "/v1/intelligence/snapshots", "/v1/governance/domains",
                "/v1/governance/certifications", "/v1/governance/glossary/terms")


def chains(doc: dict[str, Any]) -> list[list[tuple[str, str]]]:
    nodes = {n["id"]: n for n in doc["nodes"]}
    return [[(nodes[i]["type"], nodes[i]["name"]) for i in p["nodes"]] for p in doc["paths"]]


def fabric_source_body(world: FakeFabricWorld, *, secret: str = BROKER_SECRET, name: str = "Contoso Fabric") -> dict[str, Any]:
    cfg = {"tenant_id": TENANT, "metadata_identity": {"tenant_id": TENANT, "client_id": BROKER_CLIENT_ID}, "network": world.discovery_network_config()}
    return {"name": name, "kind": "fabric", "config": cfg, "credentials": {"client_secret": secret}}


def has_route(tmp_path: Path, spec: dict[str, Any], path: str, method: str = "get") -> bool:
    """Ask the browser client's real ``hasRoute`` (``web/static/js/api/client.js``, run under Node) about an OpenAPI document - the question the navigation asks."""
    node = shutil.which("node")
    if node is None:
        pytest.skip("node is not installed")
    pkg = tmp_path / "pkg"
    for rel in ("api/client.js", "core/util.js", "core/store.js"):
        dst = pkg / "js" / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(STATIC_JS / rel, dst)
    (pkg / "package.json").write_text('{"type": "module"}')
    (pkg / "spec.json").write_text(json.dumps(spec))
    (pkg / "ask.mjs").write_text(
        "import { readFileSync } from 'node:fs';\n"
        "const store = () => ({ m: new Map(), getItem(k) { return this.m.has(k) ? this.m.get(k) : null; }, setItem(k, v) { this.m.set(k, String(v)); }, removeItem(k) { this.m.delete(k); } });\n"
        "globalThis.window = { localStorage: store(), sessionStorage: store() };\n"
        "const doc = JSON.parse(readFileSync('spec.json', 'utf8'));\n"
        "globalThis.fetch = async (url) => { if (String(url) !== '/v1/openapi.json') throw new Error('unexpected ' + url); return new Response(JSON.stringify(doc), { status: 200 }); };\n"
        "const { hasRoute } = await import('./js/api/client.js');\n"
        f"console.log(JSON.stringify(await hasRoute({json.dumps(path)}, {json.dumps(method)})));\n")
    r = subprocess.run([node, "ask.mjs"], cwd=pkg, capture_output=True, text=True, timeout=60, check=False)
    assert r.returncode == 0, r.stdout + r.stderr
    return bool(json.loads(r.stdout.strip().splitlines()[-1]))


# ================================================================================================================ 1. the platform without Intelligence
@pytest.fixture(scope="module")
def bare(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Platform]:
    with build_platform(tmp_path_factory.mktemp("bare"), WITHOUT_INTELLIGENCE, install_sample=True) as p:
        yield p


def test_without_the_module_every_intelligence_route_is_a_plain_404(bare: Platform) -> None:
    for path in INTEL_ROUTES:
        r = bare.http.get(path, headers=bare.admin)
        assert r.status_code == 404, (path, r.status_code, r.text[:200])
        assert r.json()["error"]["code"] == "not_found"
    # a write to a route that does not exist is answered exactly as for any other unknown path of the platform (the GET-only static fallback makes that a 405, not a 404)
    unknown = bare.http.post("/v1/no-such-module/x", json={}, headers=bare.admin)
    for path in ("/v1/intelligence/sources", "/v1/intelligence/resolve", "/v1/governance/domains"):
        r = bare.http.post(path, json={"name": "x", "kind": "local"}, headers=bare.admin)
        assert (r.status_code, r.json()["error"]["code"]) == (unknown.status_code, unknown.json()["error"]["code"]) and r.status_code in (404, 405)
    assert bare.ctx.maybe_service("intelligence") is None
    assert not any(m.name == "intelligence" for m in bare.ctx.modules)


def test_without_the_module_the_openapi_document_and_therefore_the_navigation_do_not_offer_it(bare: Platform, tmp_path: Path) -> None:
    spec = bare.http.get("/v1/openapi.json").json()
    assert [p for p in spec["paths"] if p.startswith(("/v1/intelligence", "/v1/governance"))] == []
    assert len(spec["paths"]) > 200                                                         # ... and the rest of the platform is there
    assert has_route(tmp_path, spec, "/v1/intelligence/summary") is False                   # the browser client's own check: the Intelligence group is filtered out of the sidebar
    assert has_route(tmp_path, spec, "/v1/workspaces") is True
    shell = (STATIC_JS / "shell.js").read_text()
    assert "hasRoute('/v1/intelligence/summary', 'get')" in shell and "navGroups.filter(([g]) => g !== 'group.intelligence')" in shell


def test_without_the_module_mcp_offers_no_intelligence_or_governance_tools(bare: Platform) -> None:
    tools = [t["name"] for t in bare.mcp().result("tools/list")["tools"]]
    assert tools and not [t for t in tools if t.startswith(("intel_", "governance_"))]
    assert "get_lineage" in tools                                                           # the platform's own (catalog) lineage tool is not Intelligence
    err = bare.mcp().error("tools/call", {"name": "intel_summary", "arguments": {}})
    assert err["code"] in (-32602, -32601) or "unknown" in err["message"].lower()


def test_without_the_module_health_sample_sql_and_catalog_are_untouched(bare: Platform) -> None:
    health = bare.get("/v1/admin/health")
    assert "intelligence" not in {c["name"] for c in health["components"]} and {c["name"] for c in health["components"]} >= {"api", "audit"}
    assert "intelligence" not in bare.sample and not [c for c in bare.sample["created"] if str(c).startswith("intelligence")]     # type: ignore[attr-defined]
    assert all("ntelligence" not in str(step) for step in bare.sample.get("try_next", []))                                             # type: ignore[attr-defined]
    rows = bare.post("/v1/sql/execute", {"sql": "select count(*) from gold_daily_sales", "workspace_id": bare.sample["workspace_id"]})["rows"]                  # type: ignore[attr-defined]
    assert rows[0][0] > 0
    sync = bare.post("/v1/catalog/sync", None, workspace_id=bare.sample["workspace_id"])                                                                    # type: ignore[attr-defined]
    assert sync["lakehouses"]                                                               # the event it publishes has no subscriber and nobody is the wiser
    again = bare.http.post("/v1/sample/install", json={"size": "small"}, headers=bare.admin)
    assert again.status_code in (200, 201) and "intelligence" not in again.json()


# ================================================================================================================ 2. Intelligence, no Fabric
@pytest.fixture(scope="module")
def nofabric(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Platform]:
    with build_platform(tmp_path_factory.mktemp("nofabric"), WITHOUT_FABRIC, install_sample=True) as p:
        yield p


def test_with_no_fabric_configured_intelligence_works_from_the_local_lakehouse(nofabric: Platform) -> None:
    p = nofabric
    kinds = {s["kind"] for s in p.get("/v1/intelligence/sources")["items"]}
    assert kinds == {"local", "scanner_import"}                                              # no Fabric source until someone adds one
    assert p.scan()["status"] == "succeeded"
    assert p.get("/v1/intelligence/summary")["assets"]["by_type"].get("tenant", 0) == 0
    visual = p.asset("visual", "Revenue (card)")
    assert any(c[0] == ("file", "Files/raw/sales") for c in chains(p.origin(visual)))        # a visual traces to the raw files without any Fabric asset
    assert p.get("/v1/intelligence/assets", q="fact_sales")["items"]
    assert [x for x in p.get("/v1/intelligence/assets", type="workspace")["items"] if x.get("source_kind") == "fabric"] == []


def test_a_fabric_source_cannot_be_created_without_an_identity(nofabric: Platform) -> None:
    for body in ({"name": "F", "kind": "fabric", "config": {}}, {"name": "F", "kind": "fabric", "config": {"tenant_id": TENANT}}):
        r = nofabric.http.post("/v1/intelligence/sources", json=body, headers=nofabric.admin)
        assert r.status_code == 422, r.text
        err = r.json()["error"]
        assert err["code"] == "validation_failed" and "metadata_identity" in err["message"] and err["remediation"]
    assert {s["kind"] for s in nofabric.get("/v1/intelligence/sources")["items"]} == {"local", "scanner_import"}


@pytest.fixture(scope="module")
def world() -> Iterator[FakeFabricWorld]:
    w = FakeFabricWorld().start()
    sc.populate(w)
    try:
        yield w
    finally:
        w.stop()


def test_a_fabric_source_that_cannot_authenticate_fails_by_step_and_leaves_the_graph_alone(nofabric: Platform, world: FakeFabricWorld) -> None:
    p = nofabric
    allow_private_endpoints(p.ctx)                           # the fake tenant listens on 127.0.0.1
    before = p.get("/v1/intelligence/summary")["assets"]["by_type"]
    sid = p.post("/v1/intelligence/sources", fabric_source_body(world, secret=WRONG_SECRET, name="Wrong secret"), expect=201)["id"]
    test = p.post(f"/v1/intelligence/sources/{sid}/test")
    assert test["ok"] is False
    steps = {s["name"]: s for s in test["steps"]}
    assert steps["resolve credentials"]["ok"] is True and steps["acquire Fabric token"]["ok"] is False
    assert steps["acquire Fabric token"].get("remediation") or steps["acquire Fabric token"].get("detail")
    assert WRONG_SECRET not in json.dumps(test) and BROKER_SECRET not in json.dumps(test)
    scan = p.post(f"/v1/intelligence/sources/{sid}/scan", {"mode": "full"}, expect=202, wait="true")
    scan = scan.get("scan", scan)
    assert scan["status"] == "failed", scan
    after = p.get("/v1/intelligence/summary")["assets"]["by_type"]
    # nothing but the root the source itself names (the tenant, known from its configuration before any call) was written
    assert {k: v - before.get(k, 0) for k, v in after.items() if v != before.get(k, 0)} == {"tenant": 1}
    assert p.scan()["status"] == "succeeded"                                                     # the other sources are unaffected
    p.http.delete(f"/v1/intelligence/sources/{sid}", headers=p.admin)


# ================================================================================================================ 3. Fabric only
@pytest.fixture(scope="module")
def fabric(tmp_path_factory: pytest.TempPathFactory, world: FakeFabricWorld) -> Iterator[tuple[Platform, str, dict[str, Any]]]:
    with build_platform(tmp_path_factory.mktemp("fabric"), FABRIC_ONLY) as p:
        allow_private_endpoints(p.ctx)
        sid = p.post("/v1/intelligence/sources", fabric_source_body(world), expect=201)["id"]
        test = p.post(f"/v1/intelligence/sources/{sid}/test")
        doc = p.post(f"/v1/intelligence/sources/{sid}/scan", {"mode": "full"}, expect=202, wait="true")
        yield p, sid, {"test": test, "scan": doc.get("scan", doc)}


def test_the_service_principal_source_passes_every_connection_step(fabric: tuple[Platform, str, dict[str, Any]]) -> None:
    _, _, run = fabric
    test = run["test"]
    assert test["ok"] is True
    assert [s["name"] for s in test["steps"]] == ["resolve credentials", "acquire Fabric token", "list workspaces", "acquire Power BI token", "scanner permission probe",
                                                 "item read probe", "definition read probe", "sampling identity probe"]
    assert all(s["ok"] for s in test["steps"]), test["steps"]


def test_the_secret_never_appears_in_any_answer_or_stored_row(fabric: tuple[Platform, str, dict[str, Any]]) -> None:
    p, sid, run = fabric
    blob = json.dumps([p.get("/v1/intelligence/sources"), p.get(f"/v1/intelligence/sources/{sid}"), run, p.get("/v1/intelligence/gaps"), p.get("/v1/intelligence/summary")])
    assert BROKER_SECRET not in blob and "client_secret" not in json.dumps(p.get(f"/v1/intelligence/sources/{sid}")["config"])
    with p.ctx.db.connect() as c:
        stored = json.dumps([dict(r._mapping) for r in c.execute(select(assets))] + [dict(r._mapping) for r in c.execute(select(gaps))], default=str)
    assert BROKER_SECRET not in stored


def test_the_scan_reads_every_kind_of_fabric_item_and_the_denied_workspace_is_a_gap(fabric: tuple[Platform, str, dict[str, Any]]) -> None:
    p, _, run = fabric
    scan = run["scan"]
    assert scan["status"] == "partial"                                                       # the denied workspace: some of the tenant, not all of it, and it says so
    assert scan["stats"]["apply"]["errors"] == 0 and scan["stats"]["counters"]["workspaces_listed"] == 2
    by_type = p.get("/v1/intelligence/summary")["assets"]["by_type"]
    for kind in ("tenant", "workspace", "lakehouse", "table", "notebook", "pipeline", "activity", "semantic_model", "model_table", "measure", "report", "page", "visual", "column"):
        assert by_type.get(kind, 0) >= 1, (kind, by_type)
    assert by_type["table"] == 3 and by_type["notebook"] == 2 and by_type["measure"] == 3 and by_type["visual"] == 2
    open_gaps = [g for g in p.get("/v1/intelligence/gaps")["items"] if not g.get("resolved_at")]
    denied = [g for g in open_gaps if g["kind"] == "permission_denied"]
    assert [g["subject"] for g in denied] == ["Finance"] and "403" in denied[0]["detail"] and denied[0]["remediation"]
    assert [g for g in open_gaps if g["kind"] == "unresolved_reference"] == []
    assert p.asset("workspace", "Finance")                                                   # listed, so it exists in the graph; its content is what is missing
    assert [a for a in p.get("/v1/intelligence/assets", q="lh_finance")["items"]] == []


def test_lineage_runs_across_fabric_items_from_the_visual_back_to_bronze(fabric: tuple[Platform, str, dict[str, Any]]) -> None:
    p, _, _ = fabric
    full = [("table", "bronze_orders"), ("notebook", "nb_bronze_to_silver"), ("table", "silver_orders"), ("notebook", "nb_silver_to_gold"), ("table", "gold_revenue"),
            ("model_table", "Revenue"), ("measure", "Total Revenue"), ("visual", "Revenue by day")]
    doc = p.origin(p.asset("visual", "Revenue by day"))
    assert full in chains(doc)
    assert {r["asset"]["name"] for r in doc["roots"] if r["asset"]["type"] == "table"} == {"bronze_orders"}
    assert {x["confidence"] for x in doc["paths"]} <= {"derived", "inferred"}                # parsed code and definitions, not observed runs
    report = chains(p.origin(p.asset("report", "Sales Report")))
    assert [*full[:5], ("model_table", "Revenue"), ("semantic_model", "Sales Model"), ("report", "Sales Report")] in report
    assert [("table", "bronze_orders"), ("notebook", "nb_bronze_to_silver"), ("table", "silver_orders"), ("model_table", "Orders"), ("semantic_model", "Sales Model"),
            ("report", "Sales Report")] in report
    impact = p.impact(p.asset("table", "bronze_orders"))
    names = {i["name"] for group in impact["by_type"].values() for i in group["items"]}
    assert {"silver_orders", "gold_revenue", "Total Revenue", "Revenue by day", "Sales Report", "Sales Model"} <= names


def test_column_lineage_follows_the_measure_down_to_the_bronze_column(fabric: tuple[Platform, str, dict[str, Any]]) -> None:
    p, _, _ = fabric
    doc = p.origin(p.asset("measure", "Total Revenue"), level="column")
    assert {r["asset"]["name"] for r in doc["roots"]} == {"amount"}
    assert any(c[0] == ("column", "amount") and c[-3:] == [("column", "revenue"), ("column", "revenue"), ("measure", "Total Revenue")] for c in chains(doc)), chains(doc)


def test_the_pipeline_contains_and_executes_the_notebooks_it_runs(fabric: tuple[Platform, str, dict[str, Any]]) -> None:
    p, _, _ = fabric
    assert p.asset("pipeline", "pl_daily_refresh")
    with p.ctx.db.connect() as c:
        names = {r.id: r.name for r in c.execute(select(assets.c.id, assets.c.name))}
        rows = [(r.type, names[r.src_id], names[r.dst_id]) for r in c.execute(select(edges.c.type, edges.c.src_id, edges.c.dst_id).where(edges.c.status == "active"))]
    assert {("CONTAINS", "pl_daily_refresh", "Bronze to silver"), ("CONTAINS", "pl_daily_refresh", "Silver to gold")} <= set(rows)
    assert ("EXECUTES", "Bronze to silver", "nb_bronze_to_silver") in rows and ("EXECUTES", "Silver to gold", "nb_silver_to_gold") in rows
    assert ("DEPENDS_ON", "Silver to gold", "Bronze to silver") in rows                        # the order the pipeline runs them in


def test_an_incremental_scan_analyses_nothing_when_nothing_changed_and_only_the_changed_notebook_when_one_did(fabric: tuple[Platform, str, dict[str, Any]], world: FakeFabricWorld) -> None:
    p, sid, _ = fabric
    again = p.post(f"/v1/intelligence/sources/{sid}/scan", {"mode": "incremental"}, expect=202, wait="true")
    again = again.get("scan", again)
    apply = again["stats"]["apply"]
    assert again["status"] == "succeeded" and apply["analyzed"] == 0 and apply["analysis_skipped"] == 6
    assert apply["assets_created"] == apply["assets_updated"] == apply["edges_created"] == apply["edges_updated"] == 0
    assert [g["subject"] for g in p.get("/v1/intelligence/gaps")["items"] if g["kind"] == "permission_denied" and not g.get("resolved_at")] == ["Finance"]   # still visible
    changed = sc._nb('spark.sql("INSERT OVERWRITE TABLE gold_revenue SELECT ordered_date AS day, SUM(amount) AS revenue, COUNT(*) AS orders FROM silver_orders GROUP BY ordered_date")\n')
    world.add_notebook(FABRIC_WS, sc.NB_SILVER_GOLD, ipynb=changed)
    third = p.post(f"/v1/intelligence/sources/{sid}/scan", {"mode": "incremental"}, expect=202, wait="true")
    third = third.get("scan", third)
    assert third["status"] == "succeeded" and third["stats"]["apply"]["analyzed"] == 1 and third["stats"]["apply"]["analysis_skipped"] == 5
    assert third["stats"]["apply"]["assets_created"] == 0


# ================================================================================================================ 4. Fabric and on-premises together
SILVER_URI = f"abfss://{FABRIC_WS}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE}/Tables/silver_orders"
GOLD_URI = f"abfss://{FABRIC_WS}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE}/Tables/gold_revenue"
OLCF_NOTEBOOK = {"nbformat": 4, "nbformat_minor": 5, "metadata": {"kernelspec": {"name": "python3", "display_name": "Python 3", "language": "python"}},
                 "cells": [{"cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [],
                            "source": f'df = spark.read.format("delta").load("{SILVER_URI}")\n'
                                      f'df.groupBy("ordered_date").sum("amount").write.format("delta").mode("overwrite").save("{GOLD_URI}")\n'}]}


@pytest.fixture(scope="module", params=["fabric_first", "notebook_first"])
def unified(request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory) -> Iterator[tuple[Platform, str, dict[str, Any]]]:
    """The full platform with the sample, a Fabric source and an OLCF notebook on the same OneLake paths; ``notebook_first`` scans the notebook before Fabric is known."""
    w = FakeFabricWorld().start()
    sc.populate(w, denied_workspace=False)
    try:
        with build_platform(tmp_path_factory.mktemp(request.param), None, install_sample=True) as p:
            allow_private_endpoints(p.ctx)
            def import_notebook() -> None:
                r = p.http.post(f"/v1/workspaces/{p.sample['workspace_id']}/notebooks/import", headers=p.admin,                       # type: ignore[attr-defined]
                                files={"file": ("olcf_publish_gold.ipynb", json.dumps(OLCF_NOTEBOOK).encode(), "application/json")})
                assert r.status_code == 201, r.text

            def add_fabric() -> dict[str, Any]:
                sid = p.post("/v1/intelligence/sources", fabric_source_body(w), expect=201)["id"]
                doc = p.post(f"/v1/intelligence/sources/{sid}/scan", {"mode": "full"}, expect=202, wait="true")
                return dict(doc.get("scan", doc))

            if request.param == "fabric_first":
                fabric_scan = add_fabric()
                import_notebook()
                p.scan()
            else:
                import_notebook()
                p.scan()
                with p.ctx.db.connect() as c:                                               # before Fabric is known the two tables are placeholders
                    waiting = {r.name for r in c.execute(select(assets.c.name).where(assets.c.status == "placeholder", assets.c.type == "table"))}
                assert {"silver_orders", "gold_revenue"} <= waiting
                fabric_scan = add_fabric()
            yield p, request.param, fabric_scan
    finally:
        w.stop()


def _aliases(p: Platform, asset_id: str) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    with p.ctx.db.connect() as c:
        for r in c.execute(select(asset_aliases.c.kind, asset_aliases.c.value).where(asset_aliases.c.asset_id == asset_id)):
            out.setdefault(r.kind, set()).add(r.value)
    return out


@pytest.mark.parametrize(("name", "uri"), [("silver_orders", SILVER_URI), ("gold_revenue", GOLD_URI)])
def test_one_onelake_table_is_one_asset_with_the_aliases_of_both_worlds(unified: tuple[Platform, str, dict[str, Any]], name: str, uri: str) -> None:
    p, _, _ = unified
    table = p.asset("table", name)                                                          # exactly one active asset of that name
    aliases = _aliases(p, table)
    assert {"fabric", "onelake_path", "delta_location", "qname"} <= set(aliases)
    assert M.normalize_path(uri) in {a for a in aliases["onelake_path"]}                    # the path the OLCF notebook wrote is the path Fabric reported
    assert any(v.startswith(f"{TENANT}/{FABRIC_WS}/") for v in aliases["fabric"])
    with p.ctx.db.connect() as c:
        rows = c.execute(select(assets.c.status, assets.c.source_id).where(assets.c.name == name, assets.c.type == "table")).all()
        gaps_open = c.execute(select(gaps.c.id).where(gaps.c.asset_id == table, gaps.c.kind == "unresolved_reference", gaps.c.resolved_at.is_(None))).all()
    assert [r.status for r in rows if r.status != "removed"] == ["active"]                  # a placeholder that was merged is gone, none was left behind
    assert gaps_open == []


def test_the_olcf_notebook_edges_attach_to_the_fabric_tables(unified: tuple[Platform, str, dict[str, Any]]) -> None:
    p, order, fabric_scan = unified
    notebook, silver, gold = p.asset("notebook", "olcf_publish_gold"), p.asset("table", "silver_orders"), p.asset("table", "gold_revenue")
    with p.ctx.db.connect() as c:
        rows = {(r.type, r.src_id, r.dst_id) for r in c.execute(select(edges.c.type, edges.c.src_id, edges.c.dst_id).where(edges.c.status == "active"))}
    assert ("READS_FROM", notebook, silver) in rows and ("WRITES_TO", notebook, gold) in rows
    names = {}
    with p.ctx.db.connect() as c:
        names = {r.id: r.name for r in c.execute(select(assets.c.id, assets.c.name))}
    readers = {names[s] for t, s, d in rows if t == "READS_FROM" and d == silver}
    writers = {names[s] for t, s, d in rows if t == "WRITES_TO" and d == gold}
    assert readers == {"nb_silver_to_gold", "Orders", "olcf_publish_gold"} and writers == {"nb_silver_to_gold", "olcf_publish_gold"}
    merged = fabric_scan["stats"]["apply"]["placeholders_merged"]
    if order == "notebook_first":                                                           # the scan that discovers the real tables turns the waiting placeholders into them
        assert merged >= 2
    else:
        assert merged == 0


def test_lineage_and_impact_cross_the_boundary(unified: tuple[Platform, str, dict[str, Any]]) -> None:
    p, _, _ = unified
    report = p.asset("report", "Sales Report")
    paths = chains(p.origin(report))
    assert [("table", "bronze_orders"), ("notebook", "nb_bronze_to_silver"), ("table", "silver_orders"), ("notebook", "olcf_publish_gold"), ("table", "gold_revenue"),
            ("model_table", "Revenue"), ("semantic_model", "Sales Model"), ("report", "Sales Report")] in paths            # an OLCF notebook writes, a Fabric semantic model reads
    down = p.lineage(p.asset("notebook", "olcf_publish_gold"), direction="down", depth=10)
    assert {"gold_revenue", "Revenue", "Sales Model", "Sales Report"} <= {n["name"] for n in down["nodes"]}
    impact = {i["name"] for group in p.impact(p.asset("notebook", "olcf_publish_gold"))["by_type"].values() for i in group["items"]}
    assert {"gold_revenue", "Sales Report", "Total Revenue"} <= impact
    up = p.lineage(p.asset("table", "silver_orders"), direction="up", depth=10)
    assert {"nb_bronze_to_silver", "bronze_orders"} <= {n["name"] for n in up["nodes"]}
    # the gold table of the OLCF notebook is the one the Fabric notebook writes too: both producers are listed on the one asset
    producers = {n["name"] for n in p.lineage(p.asset("table", "gold_revenue"), direction="up", depth=1)["nodes"] if n["type"] == "notebook"}
    assert {"nb_silver_to_gold", "olcf_publish_gold"} <= producers
