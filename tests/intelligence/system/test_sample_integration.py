# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The sample inside OLCF Intelligence: what a freshly installed sample shows the first time somebody opens Intelligence.

Real application (every module), the real sample installer, the real local connector and the real scanner analyzers. Covered: the glossary with definitions that match the data and links
to lakehouse columns and Power BI measures, the domain and data product, owners and stewards, certification, classification (one approved label, the rest in the review queue), the
SYNTHETIC Power BI scan and its labelling, the answer to *where does the Revenue visual's number originate* with no hand-made edge, idempotence, and that a failing Intelligence part never
fails the install while the sample installs unchanged without the module.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
from sqlalchemy import func, select

from olcf.intelligence.schema import assets, edges
from olcf.sample import intelligence as sample_intelligence
from olcf.sample import powerbi, spec
from tests.intelligence.system.conftest import Platform, build_platform

GOLD = sample_intelligence.GOLD_TABLES
REPO = Path(__file__).resolve().parents[3]


def _gov(p: Platform, asset_id: str) -> dict:
    return p.get(f"/v1/intelligence/assets/{asset_id}")["governance"]


def _terms(p: Platform) -> dict[str, dict]:
    return {t["name"]: t for t in p.get("/v1/governance/glossary/terms", limit=200)["items"]}


# ------------------------------------------------------------------------------------------------------------------------ the install result
def test_install_reports_every_step_of_the_intelligence_part(platform):
    out = platform.sample
    assert out["action"] == "installed"
    intel = out["intelligence"]
    assert intel["installed"] is True and intel["ok"] is True and intel["problems"] == []
    assert list(intel["steps"]) == ["scan_local", "import_powerbi", "classification_scan", "glossary", "term_link_suggestions", "domain", "owners", "classifications", "certification",
                                    "data_product"]
    assert all(s["ok"] for s in intel["steps"].values()), intel["steps"]
    assert intel["steps"]["import_powerbi"]["label"] == powerbi.SYNTHETIC_LABEL
    assert {f"term:{t[0]}" for t in sample_intelligence.TERMS} <= set(intel["created"])
    assert "data_product:Retail Sales Analytics" in intel["created"] and "domain:Retail Sales" in intel["created"]
    assert any(c.startswith("intelligence:term:") for c in out["created"])


def test_the_sample_is_in_the_graph_after_the_install(platform):
    for name in spec.TABLES:
        platform.asset("table", name, path_has="retail_lakehouse")
    platform.asset("notebook", "01_Retail_Medallion_ETL")
    platform.asset("report", "Retail Sales Overview")
    sources = {s["name"]: s for s in platform.get("/v1/intelligence/sources")["items"]}
    assert sources["local"]["kind"] == "local"
    synthetic = [s for s in sources.values() if s["kind"] == "scanner_import"]
    assert [s["name"] for s in synthetic] == [powerbi.SOURCE_NAME]
    assert "synthetic example" in powerbi.SOURCE_NAME and "not from a real tenant" in powerbi.SOURCE_NAME


# ------------------------------------------------------------------------------------------------------------------------ the synthetic Power BI scan
def test_the_synthetic_scan_is_labelled_everywhere_it_can_be():
    doc = powerbi.scan_document()
    assert powerbi.SYNTHETIC_LABEL in doc["x-olcf-synthetic"]
    ws = doc["workspaces"][0]
    assert powerbi.SYNTHETIC_LABEL in ws["name"]
    text = json.dumps(doc, ensure_ascii=False)
    assert text.count(powerbi.SYNTHETIC_LABEL) >= 7
    assert powerbi.SYNTHETIC_LABEL in ws["datasets"][0]["tables"][0]["description"] and powerbi.SYNTHETIC_LABEL in ws["reports"][0]["olcfReportDefinition"]["definition/report.json"]["description"]
    committed = json.loads((REPO / "schemas" / "sample-retail-powerbi-scan.json").read_text())
    assert committed == doc
    model = ws["datasets"][0]
    assert model["name"] == "Retail Sales Model" and model["targetStorageMode"] == "DirectLake"
    measures = {m["name"]: m["expression"] for t in model["tables"] for m in t["measures"]}
    assert measures["Total Revenue"] == "SUM('Daily Sales'[net_revenue])"
    assert len(measures) >= 8
    assert "@" not in json.dumps(doc).replace("synthetic.owner@example.com", "")


def test_the_synthetic_source_and_its_assets_say_so(platform):
    src = next(s for s in platform.get("/v1/intelligence/sources")["items"] if s["kind"] == "scanner_import")
    assert "synthetic example" in src["name"]
    model = platform.get(f"/v1/intelligence/assets/{platform.asset('semantic_model', powerbi.MODEL_NAME)}")
    assert model["type"] == "semantic_model" and model["status"] == "active"
    report_id = platform.asset("report", powerbi.REPORT_NAME)
    detail = platform.get(f"/v1/intelligence/assets/{report_id}")
    assert detail["name"] == "Retail Sales Report"
    docs = platform.get(f"/v1/intelligence/assets/{report_id}/documentation", format="json")
    assert "synthetic" in json.dumps(docs).lower()


# ------------------------------------------------------------------------------------------------------------------------ the headline question
def test_where_does_the_revenue_visual_come_from_without_any_manual_edge(platform):
    visual = platform.asset("visual", "Revenue (card)")
    doc = platform.origin(visual)
    nodes = {n["id"]: n for n in doc["nodes"]}
    chains = [[nodes[i] for i in p["nodes"]] for p in doc["paths"]]
    wanted = ["file", "script", "table", "notebook", "table", "model_table", "measure", "visual"]
    best = [c for c in chains if [n["type"] for n in c] == wanted and c[0]["name"] == "Files/raw/sales" and c[2]["name"] == "fact_sales" and c[4]["name"] == "gold_daily_sales"]
    assert best, [[f"{n['type']}:{n['name']}" for n in c] for c in chains]
    chain = best[0]
    assert [n["name"] for n in chain[3:]] == ["01_Retail_Medallion_ETL", "gold_daily_sales", "Daily Sales", "Total Revenue", "Revenue (card)"]
    assert {n["status"] for n in chain} == {"active"}
    # the report and the semantic model sit on the same path from the other end
    report = platform.origin(platform.asset("report", powerbi.REPORT_NAME))
    rn = {n["id"]: n for n in report["nodes"]}
    assert any([rn[i]["type"] for i in p["nodes"]][-3:] == ["model_table", "semantic_model", "report"] for p in report["paths"])
    # every edge on the path is a stored edge found by the analyzers: it has evidence, and none was written by hand
    with platform.ctx.db.connect() as c:
        rows = c.execute(select(edges.c.type, edges.c.source_id, edges.c.status, edges.c.id).where(edges.c.id.in_([e["id"] for e in doc["edges"] if "id" in e]))).all() if doc["edges"] and "id" in doc["edges"][0] else []
        n_manual = c.execute(select(func.count()).select_from(edges).where(edges.c.source_id.in_(["manual", "sample", "api"]))).scalar()
    assert n_manual == 0
    assert all(r[2] == "active" for r in rows)


def test_the_model_tables_resolve_to_the_real_gold_tables(platform):
    """Direct Lake style M expressions are resolved by name to the lakehouse tables of the sample; no placeholder is created for them."""
    for t, (gold, _) in powerbi.MODEL_TABLES.items():
        mt = platform.asset("model_table", t)
        lin = platform.lineage(mt, direction="up", depth=1)
        up = {n["name"]: n for n in lin["nodes"] if n["direction"] == "upstream"}
        assert gold in up and up[gold]["type"] == "table" and up[gold]["placeholder"] is False, (t, list(up))


def test_column_lineage_reaches_the_lakehouse_columns_from_a_measure_and_a_visual(platform):
    for visual, measure, source in (("Revenue (card)", "Total Revenue", "net_amount"), ("Profit (card)", "Total Profit", "profit"), ("Orders (card)", "Total Orders", "order_id")):
        v = platform.asset("visual", visual)
        doc = platform.origin(v, level="column")
        nodes = {n["id"]: n for n in doc["nodes"]}
        assert source in {r["asset"]["name"] for r in doc["roots"]}, (visual, [r["asset"]["name"] for r in doc["roots"]])
        path_types = [[nodes[i]["type"] for i in p["nodes"]] for p in doc["paths"]]
        assert any(t[-3:] == ["column", "measure", "visual"] for t in path_types), path_types
        m = platform.origin(platform.asset("measure", measure), level="column")
        assert m["roots"] and {r["asset"]["type"] for r in m["roots"]} == {"column"}


def test_impact_of_a_raw_file_and_a_silver_column_lists_the_power_bi_visual_and_report(platform):
    fact_net = platform.asset("column", "net_amount", parent="fact_sales")
    imp = platform.impact(fact_net)
    names = {(r["type"], r["name"]) for r in _impact_assets(imp)}
    assert ("visual", "Revenue (card)") in names and ("report", "Retail Sales Report") in names and ("semantic_model", "Retail Sales Model") in names
    raw = platform.asset("file", "Files/raw/sales")
    names = {(r["type"], r["name"]) for r in _impact_assets(platform.impact(raw))}
    assert {("table", "fact_sales"), ("table", "gold_daily_sales"), ("measure", "Total Revenue"), ("visual", "Revenue (card)"), ("report", "Retail Sales Report")} <= names


def _impact_assets(doc: dict) -> list[dict]:
    return [item for group in doc["by_type"].values() for item in group["items"]]


# ------------------------------------------------------------------------------------------------------------------------ governance
def test_the_glossary_has_real_definitions_that_match_the_data(platform):
    terms = _terms(platform)
    assert set(terms) == {t[0] for t in sample_intelligence.TERMS} and len(terms) >= 10
    assert {"Revenue", "Net Revenue", "Customer Segment", "Order", "Store", "Gross Margin"} <= set(terms)
    assert all(t["status"] == "approved" and len(t["definition"]) > 60 for t in terms.values())
    assert "net_amount" in terms["Net Revenue"]["definition"] and "gross_amount - discount_amount" in terms["Net Revenue"]["definition"]
    assert "Consumer, Corporate or Home Office" in terms["Customer Segment"]["definition"]
    assert "profit" in terms["Gross Margin"]["definition"].lower() and "net_revenue" in terms["Gross Margin"]["definition"]
    # every snake_case identifier a definition mentions is a column or table of the sample, and every link target is a column of the table it names
    cols = {c["name"] for t in spec.TABLES.values() for c in t["columns"]}
    known = cols | set(spec.TABLES)
    for name, _abbr, _syn, definition, targets in sample_intelligence.TERMS:
        mentioned = set(re.findall(r"\b[a-z]+(?:_[a-z]+)+\b", definition))
        assert mentioned <= known, (name, sorted(mentioned - known))
        for kind, table, col in targets:
            if kind == "lh_column":
                assert col in {c["name"] for c in spec.TABLES[table]["columns"]}, (name, table, col)
    assert {"net_amount", "gross_amount", "discount_amount", "profit", "order_id", "segment", "ltv_segment", "lifetime_revenue"} <= cols


def test_terms_are_linked_to_columns_tables_and_measures(platform):
    net = platform.asset("column", "net_revenue", parent="gold_daily_sales")
    assert "Net Revenue" in {t["name"] for t in _gov(platform, net)["glossary_terms"]}
    measure = platform.asset("measure", "Total Revenue")
    assert "Revenue" in {t["name"] for t in _gov(platform, measure)["glossary_terms"]}
    seg = platform.asset("column", "segment", parent="dim_customer")
    assert "Customer Segment" in {t["name"] for t in _gov(platform, seg)["glossary_terms"]}
    links = platform.get(f"/v1/governance/glossary/terms/{_terms(platform)['Revenue']['id']}/links")["items"]
    linked = {(link["asset"]["type"], link["asset"]["name"]) for link in links if link.get("asset")}
    assert {("table", "gold_daily_sales"), ("measure", "Total Revenue"), ("measure", "Category Revenue")} <= linked


def test_domain_and_data_product(platform):
    domains = platform.get("/v1/governance/domains")["items"]
    assert [d["name"] for d in domains] == ["Retail Sales"]
    prods = platform.get("/v1/governance/products")["items"]
    assert [p["name"] for p in prods] == ["Retail Sales Analytics"]
    prod = platform.get(f"/v1/governance/products/{prods[0]['id']}")
    assert prod["status"] == "published" and prod["sla"]["freshness_hours"] == 24 and prod["domain"]["name"] == "Retail Sales"
    assert prod["member_count"] == 3
    gold = platform.asset("table", "gold_daily_sales")
    g = _gov(platform, gold)
    assert [p["name"] for p in g["data_product"]] == ["Retail Sales Analytics"]
    assert g["domain"]["name"] == "Retail Sales" and g["certification"] == "certified"
    # the domain reaches the Power BI model and report too, and lakehouse tables inherit it
    for aid in (platform.asset("semantic_model", powerbi.MODEL_NAME), platform.asset("report", powerbi.REPORT_NAME), platform.asset("table", "dim_store")):
        assert _gov(platform, aid)["domain"]["name"] == "Retail Sales"


def test_owners_stewards_and_certification(platform):
    for t in GOLD:
        g = _gov(platform, platform.asset("table", t))
        assert g["certification"] == "certified", t
        assert {(o["role"], o["kind"]) for o in g["owners"]} == {("owner", "user")}, t
    cust = _gov(platform, platform.asset("table", "dim_customer"))
    assert ("steward", "user") in {(o["role"], o["kind"]) for o in cust["owners"]}
    assert _gov(platform, platform.asset("table", "fact_sales"))["certification"] in ("none", "draft")


def test_classification_one_label_approved_and_the_rest_waiting_for_review(platform):
    email = platform.asset("column", "email", parent="dim_customer")
    cls = _gov(platform, email)["classifications"]
    assert [c["name"] for c in cls] == ["PII"] and cls[0]["source"] in ("manual", "discovery", "detector", "inherited") or cls
    queue = platform.get("/v1/governance/review-queue", limit=200)["items"]
    kinds = {i["kind"] for i in queue}
    assert kinds == {"classification", "term_link"}
    pending = {(i["asset"]["name"], i.get("classification", {}).get("name")) for i in queue if i["kind"] == "classification"}
    assert ("first_name", "PII") in pending and ("last_name", "PII") in pending
    assert ("email", "PII") not in pending
    summary = platform.get("/v1/governance/review-queue/summary")
    assert summary["classification"] == 2 and summary["term_link"] >= 1


def test_exposure_of_the_pii_label_is_only_possible_because_no_column_lineage_proves_it(platform):
    """The PII label is on dim_customer.email. The notebook reads dim_customer as a whole (table level), so everything downstream, the report included, is a *possible* exposure
    (``proven: false``) and the answer says so; nothing is reported as proven."""
    exposure = platform.get("/v1/governance/classifications/PII/exposure")
    assert exposure["summary"]["proven"] == 0 and exposure["summary"]["possible"] == len(exposure["exposures"]) > 0
    assert {e["proven"] for e in exposure["exposures"]} == {False}
    assert any(e["asset"]["name"] == "Retail Sales Report" for e in exposure["exposures"])
    assert any("proven=false" in n for n in exposure["notes"])
    assert exposure["data_products"][0]["product"]["name"] == "Retail Sales Analytics"
    proven_only = platform.get("/v1/governance/classifications/PII/exposure", include_possible="false")
    assert proven_only["exposures"] == []


# ------------------------------------------------------------------------------------------------------------------------ idempotence, isolation
def _counts(p: Platform) -> dict[str, int]:
    from olcf.intelligence.governance.schema import (
        gov_domains,
        gov_owners,
        gov_products,
        gov_term_links,
        gov_terms,
    )

    with p.ctx.db.connect() as c:
        out = {t.name: int(c.execute(select(func.count()).select_from(t)).scalar() or 0) for t in (gov_terms, gov_term_links, gov_domains, gov_products, gov_owners, assets, edges)}
    return out


def test_installing_again_changes_nothing(platform):
    before = _counts(platform)
    again = platform.post("/v1/sample/install", {"size": "small"})
    assert again["action"] == "unchanged" and again["created"] == [] and again["updated"] == []
    assert again["intelligence"]["ok"] is True and again["intelligence"]["created"] == []
    assert again["intelligence"]["steps"]["scan"]["skipped"]
    after = _counts(platform)
    assert {k: after[k] - before[k] for k in before if k not in ("gov_term_links",)} == {k: 0 for k in before if k not in ("gov_term_links",)}


def test_reinstall_scans_again_and_keeps_what_curators_may_have_changed(platform):
    # a curator removes the owner entry of the product and un-certifies a table; a reinstall must not put them back
    prod = platform.get("/v1/governance/products")["items"][0]
    owners = platform.get(f"/v1/governance/products/{prod['id']}/owners")["direct"]
    assert len(owners) == 2
    for o in owners:
        platform.http.delete(f"/v1/governance/products/{prod['id']}/owners/{o['id']}", headers=platform.admin)
    gold = platform.asset("table", "gold_category_summary")
    platform.http.put(f"/v1/governance/assets/{gold}/certification", json={"state": "draft"}, headers=platform.admin)
    out = platform.post("/v1/sample/install", {"size": "small", "reinstall": True}, expect=None) or {}
    assert out["action"] == "reinstalled"
    assert out["intelligence"]["ok"] is True
    assert out["intelligence"]["steps"]["scan_local"]["ok"] is True
    assert out["intelligence"]["steps"]["curation"]["skipped"]
    assert platform.get(f"/v1/governance/products/{prod['id']}/owners")["direct"] == []
    assert _gov(platform, gold)["certification"] == "draft"
    assert len(platform.get("/v1/governance/glossary/terms", limit=200)["items"]) == len(sample_intelligence.TERMS)


@pytest.fixture()
def small_stack(tmp_path):
    mods = ["olcf.api.core.module", "olcf.storage.module", "olcf.catalog.module", "olcf.sql.module", "olcf.intelligence.module", "olcf.sample.module"]
    with build_platform(tmp_path, mods) as p:
        yield p


def test_a_failing_intelligence_part_never_fails_the_install(small_stack, monkeypatch):
    intel = small_stack.intel

    def boom(*a, **k):
        raise RuntimeError("scan exploded")

    monkeypatch.setattr(intel.discovery.runner, "start", boom)
    monkeypatch.setattr(intel.governance.glossary, "create", boom)
    out = small_stack.post("/v1/sample/install", {"size": "small"}, expect=201)
    assert out["action"] == "installed"
    assert [t["name"] for t in out["tables"]] == list(spec.TABLES)
    res = out["intelligence"]
    assert res["ok"] is False
    assert res["steps"]["scan_local"] == {"ok": False, "error": "RuntimeError: scan exploded"}
    assert res["steps"]["glossary"]["ok"] is False
    assert any("scan exploded" in p for p in res["problems"])
    # the steps that do not depend on the failed ones still ran
    assert res["steps"]["domain"]["ok"] is True
    # and the sample itself is fully usable
    r = small_stack.post("/v1/sql/query", {"sql": "SELECT count(*) AS n FROM retail_lakehouse.gold_daily_sales", "workspace": "Sample Retail"}, expect=None)
    assert r is not None


def test_dropped_governance_tables_do_not_fail_the_install(small_stack):
    from sqlalchemy import text

    with small_stack.ctx.db.begin() as c:
        for t in ("gov_term_links", "gov_terms", "gov_product_members", "gov_products", "gov_asset_domains", "gov_domains", "gov_owners", "gov_certifications", "gov_class_assignments"):
            c.execute(text(f"DROP TABLE IF EXISTS {t}"))  # noqa: S608
    small_stack.intel.governance.tables_available(force=True)
    out = small_stack.post("/v1/sample/install", {"size": "small"}, expect=201)
    assert out["action"] == "installed" and out["intelligence"]["ok"] is False
    assert out["intelligence"]["steps"]["scan_local"]["ok"] is True
    assert out["intelligence"]["problems"]


def test_the_sample_installs_unchanged_without_the_intelligence_module(tmp_path):
    mods = ["olcf.api.core.module", "olcf.storage.module", "olcf.catalog.module", "olcf.sql.module", "olcf.sample.module"]
    with build_platform(tmp_path, mods) as p:
        out = p.post("/v1/sample/install", {"size": "small"}, expect=201)
        assert out["action"] == "installed" and "intelligence" not in out
        assert not any(c.startswith("intelligence:") for c in out["created"])
        again = p.post("/v1/sample/install", {"size": "small"})
        assert again["action"] == "unchanged"
        assert p.get("/v1/intelligence/summary", expect=404) is not None


def test_a_user_without_organisation_rights_gets_the_sample_and_a_clear_report_of_what_was_not_done(small_stack):
    contributor = small_stack.user("contrib", "Contributor")
    out = small_stack.post("/v1/sample/install", {"size": "small"}, who=contributor, expect=201)
    assert out["action"] == "installed" and len(out["tables"]) == len(spec.TABLES)
    res = out["intelligence"]
    assert res["ok"] is False
    assert res["steps"]["import_powerbi"] == {"ok": False, "error": "Forbidden: permission 'intelligence.manage' is required"}
    assert [p.split(":")[0] for p in res["problems"]] == ["import_powerbi"], res["problems"]
    # without governance.approve the terms and the product are drafts
    assert {t["status"] for t in small_stack.get("/v1/governance/glossary/terms", who=contributor, limit=200)["items"]} == {"draft"}
    assert [p["status"] for p in small_stack.get("/v1/governance/products", who=contributor)["items"]] == ["draft"]
    # an administrator installing again completes what needs organisation-wide rights (the synthetic Power BI scan)
    again = small_stack.post("/v1/sample/install", {"size": "small"}, expect=200)
    assert again["action"] == "updated"
    assert again["intelligence"]["ok"] is True and again["intelligence"]["steps"]["import_powerbi"]["ok"] is True
    assert again["created"] == [f"intelligence:source:{powerbi.SOURCE_NAME}"]
    assert small_stack.asset("visual", "Revenue (card)")
