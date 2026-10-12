# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The whole platform as one system: install the sample, let Intelligence scan it, and ask the questions people ask, through every surface.

Real application (every module), the real sample, the real analyzers: nothing here is a stub. Covered: lineage from a Power BI visual to the raw files, column lineage for every measure of
the synthetic model, impact analysis of a raw file and of a silver column, staleness and origin, the glossary on the same assets, classification suggestions -> review -> approval ->
exposure (the report is returned *proven* once a labelled column provably feeds it), the documentation endpoint, the MCP tools ``intel_where_does_this_come_from`` and
``governance_exposure`` over the MCP transport, the ``olcf intelligence`` commands through click's CliRunner and the SDK.
"""

from __future__ import annotations

import pytest

from olcf.sample import powerbi

MEASURES = [(t, m) for t, (_, ms) in powerbi.MODEL_TABLES.items() for m, _ in ms]
SOURCE_COLUMNS = {
    "Total Revenue": {"net_amount"}, "Gross Revenue": {"gross_amount"}, "Total Profit": {"profit"}, "Total Orders": {"order_id"}, "Average Order Value": {"net_amount", "order_id"},
    "Gross Margin %": {"net_amount", "profit"}, "Discount Amount": {"gross_amount", "net_amount"}, "Category Revenue": {"net_amount"}, "Category Profit": {"profit"},
    "Average Lifetime Revenue": {"net_amount"}, "Customers": {"customer_id"},
}


def _chains(doc: dict) -> list[list[tuple[str, str]]]:
    nodes = {n["id"]: n for n in doc["nodes"]}
    return [[(nodes[i]["type"], nodes[i]["name"]) for i in p["nodes"]] for p in doc["paths"]]


# ------------------------------------------------------------------------------------------------------------------------ lineage
def test_the_revenue_visual_traces_to_the_raw_files_through_every_layer(platform):
    visual = platform.asset("visual", "Revenue (card)")
    chains = _chains(platform.origin(visual))
    full = ("file", "Files/raw/sales"), ("script", "sample generator"), ("table", "fact_sales"), ("notebook", "01_Retail_Medallion_ETL"), ("table", "gold_daily_sales"), \
        ("model_table", "Daily Sales"), ("measure", "Total Revenue"), ("visual", "Revenue (card)")
    assert list(full) in chains, chains
    # the verdict of the whole path is the weakest link, and it is not "verified" (code was parsed, not observed)
    doc = platform.origin(visual)
    assert {p["confidence"] for p in doc["paths"]} <= {"derived", "inferred"}
    assert doc["has_upstream"] is True and doc["truncated"] is False


def test_the_report_and_the_model_have_the_same_sources(platform):
    report = platform.origin(platform.asset("report", powerbi.REPORT_NAME))
    model = platform.origin(platform.asset("semantic_model", powerbi.MODEL_NAME))
    raw = {r["asset"]["name"] for r in report["roots"] if r["asset"]["type"] == "file"}
    assert raw == {"Files/raw/sales", "Files/raw/customers", "Files/raw/products", "Files/raw/stores"} == {r["asset"]["name"] for r in model["roots"] if r["asset"]["type"] == "file"}
    assert any(c[-3:] == [("model_table", "Daily Sales"), ("semantic_model", "Retail Sales Model"), ("report", "Retail Sales Report")] for c in _chains(report))


@pytest.mark.parametrize(("table", "measure"), MEASURES)
def test_column_lineage_from_every_measure_reaches_the_silver_columns(platform, table, measure):
    doc = platform.origin(platform.asset("measure", measure, parent=table), level="column")
    real = {r["asset"]["name"] for r in doc["roots"] if "fact_sales" in (r["asset"].get("path") or "")}
    assert real == SOURCE_COLUMNS[measure], (measure, [(r["asset"]["name"], r["asset"].get("path")) for r in doc["roots"]])
    chains = _chains(doc)
    assert chains and all(c[-1] == ("measure", measure) and {t for t, _ in c} == {"column", "measure"} for c in chains)
    assert all(len(c) >= 3 for c in chains)           # silver column -> gold column -> model column -> measure (at least)


def test_column_lineage_of_columns_has_the_transformation_text(platform):
    gold = platform.asset("column", "net_revenue", parent="gold_daily_sales")
    doc = platform.lineage(gold, direction="up", level="column")
    edge = next(e for e in doc["edges"] if e["type"] == "TRANSFORMS")
    assert "net_amount" in (edge.get("transformation") or "").lower() or "sum" in (edge.get("transformation") or "").lower()
    model_col = platform.asset("column", "net_revenue", parent="Daily Sales")
    up = platform.lineage(model_col, direction="up", level="column")
    assert {n["name"] for n in up["nodes"] if n["direction"] == "upstream"} >= {"net_revenue", "net_amount"}


def test_impact_of_a_raw_file_and_of_a_silver_column_lists_the_visual_and_the_report(platform):
    for asset in (platform.asset("file", "Files/raw/sales"), platform.asset("column", "net_amount", parent="fact_sales")):
        doc = platform.impact(asset)
        found = {(i["type"], i["name"]) for g in doc["by_type"].values() for i in g["items"]}
        assert {("visual", "Revenue (card)"), ("report", "Retail Sales Report"), ("semantic_model", "Retail Sales Model"), ("measure", "Total Revenue")} <= found
        assert doc["truncated"] is False and doc["total"] == sum(g["count"] for g in doc["by_type"].values())
    # the REST impact of a column is the impact of its table (it over-approximates: every measure of the model is listed); the column-level lineage is the precise answer
    down = platform.lineage(platform.asset("column", "profit", parent="fact_sales"), direction="down", level="column", depth=10)
    reached = {(n["type"], n["name"]) for n in down["nodes"] if n["direction"] == "downstream"}
    assert {("measure", "Total Profit"), ("visual", "Profit (card)"), ("visual", "Gross margin (card)")} <= reached
    assert ("measure", "Total Orders") not in reached and ("visual", "Orders (card)") not in reached and ("visual", "Revenue by day (line)") not in reached


def test_mcp_column_impact_lists_the_dependent_columns_precisely_and_the_consumers_of_the_table(platform):
    out = platform.mcp().call("intel_impact_analysis", asset="fact_sales.net_amount", type="column", depth=8)
    deps = {d["name"] for d in out["dependent_columns"]}
    assert {"net_revenue"} <= deps and "orders" not in deps
    assert any(c["name"] == "Retail Sales Report" for c in out["consumers"])
    assert any("table-level" in n for n in out["notes"])


def test_staleness_and_origin_of_a_gold_table(platform):
    gold = platform.asset("table", "gold_daily_sales")
    doc = platform.get(f"/v1/intelligence/assets/{gold}/staleness")
    assert doc["asset"]["name"] == "gold_daily_sales"
    assert any(k for k in doc)          # the document is not empty
    origin = platform.origin(gold)
    assert any(r["asset"]["name"] == "Files/raw/sales" for r in origin["roots"])


def test_the_documentation_endpoint_describes_the_asset_with_lineage_and_governance(platform):
    gold = platform.asset("table", "gold_daily_sales")
    text = platform.http.get(f"/v1/intelligence/assets/{gold}/documentation", params={"format": "markdown"}, headers=platform.admin)
    assert text.status_code == 200 and text.headers["content-type"].startswith("text/markdown")
    md = text.text
    for needle in ("# gold_daily_sales", "**Certification:** certified", "Retail Sales Analytics", "Glossary terms:** Revenue", "| `net_revenue` | double | Sum of net_amount |", "## Lineage", "01_Retail_Medallion_ETL"):
        assert needle in md, needle
    as_json = platform.get(f"/v1/intelligence/assets/{gold}/documentation", format="json")
    assert as_json["name"] == "gold_daily_sales" and as_json["asset_id"] == gold and as_json["markdown"] == md


# ------------------------------------------------------------------------------------------------------------------------ glossary and classification
def test_the_glossary_term_of_a_metric_is_on_the_column_and_found_by_its_synonym(platform):
    net = platform.asset("column", "net_revenue", parent="gold_daily_sales")
    detail = platform.get(f"/v1/governance/assets/{net}")
    assert [(t["name"], t["link_status"], t["source"]) for t in detail["terms"]] == [("Net Revenue", "approved", "manual")]
    search = platform.get("/v1/governance/glossary/search", q="net sales")["items"]          # "Net Sales" is a synonym of Net Revenue
    assert search[0]["term"]["name"] == "Net Revenue"
    measure = platform.get(f"/v1/governance/assets/{platform.asset('measure', 'Total Revenue')}")
    assert [t["name"] for t in measure["terms"]] == ["Revenue"]


def test_suggestions_review_approval_and_exposure_reach_the_report_when_a_labelled_column_feeds_it(platform):
    net = platform.asset("column", "net_amount", parent="fact_sales")
    # 1. a suggestion for a column whose NAME says it is personal data is waiting in the review queue
    queue = platform.get("/v1/governance/review-queue", kind="classification", limit=100)["items"]
    first = next(i for i in queue if i["asset"]["name"] == "first_name")
    assert first["classification"]["name"] == "PII" and first["evidence"]["rule"].startswith("name:")
    # 2. a curator approves it through the review endpoint
    done = platform.post(f"/v1/governance/review-queue/{first['id']}/approve", {"comment": "synthetic customer names"})
    assert done["decision"] == "approved" and done["asset_id"] == first["asset"]["id"]
    assert {c["name"] for c in platform.get(f"/v1/intelligence/assets/{first['asset']['id']}")["governance"]["classifications"]} == {"PII"}
    # 3. a column that provably feeds the reports gets a label by hand
    platform.post(f"/v1/governance/assets/{net}/classifications", {"classification": "Confidential", "comment": "revenue is business-sensitive"}, expect=201)
    try:
        default = platform.get("/v1/governance/classifications/Confidential/exposure")
        by_name = {e["asset"]["name"]: e for e in default["exposures"]}
        # the OLCF report reads the column through SQL (derived, column level): proven
        overview = by_name["Retail Sales Overview"]
        assert overview["kind"] == "report" and overview["proven"] is True and overview["precision"] == "contains" and overview["confidence_floor"] == "derived"
        assert by_name["Revenue by month"]["proven"] is True and by_name["Revenue by month"]["precision"] == "column"
        # the Power BI model columns are matched to the lakehouse columns BY NAME (the scanner result has no sourceColumn): inferred, so with the default threshold (derived) the
        # report is only a possible exposure and the answer says so
        pbi = by_name["Retail Sales Report"]
        assert pbi["kind"] == "report" and pbi["proven"] is False and pbi["confidence_floor"] == "inferred"
        wider = platform.get("/v1/governance/classifications/Confidential/exposure", min_confidence="inferred")
        w = {e["asset"]["name"]: e for e in wider["exposures"]}
        assert w["Retail Sales Report"]["proven"] is True and w["Retail Sales Report"]["precision"] == "contains" and w["Retail Sales Report"]["confidence_floor"] == "inferred"
        assert [e["asset"]["name"] for e in wider["exposures"] if e["kind"] == "report" and e["proven"]].count("Retail Sales Report") == 1
        visuals = {e["asset"]["name"] for e in wider["exposures"] if e["kind"] == "visual" and e["proven"]}
        assert {"Revenue (card)", "Gross margin (card)", "Revenue by day (line)", "Average order value (card)"} <= visuals
        assert "Orders (card)" not in visuals and "Profit (card)" not in visuals          # these do not use net_amount
        path = [n["name"] for n in w["Revenue (card)"]["path"]["nodes"]]
        assert path[0] == "net_amount" and path[-1] == "Revenue (card)" and "Total Revenue" in path
        assert wider["summary"]["reports"] == 2 and wider["data_products"][0]["product"]["name"] == "Retail Sales Analytics"
    finally:
        platform.http.delete(f"/v1/governance/assets/{net}/classifications/Confidential", headers=platform.admin)


# ------------------------------------------------------------------------------------------------------------------------ the surfaces: MCP, CLI, SDK
def test_mcp_where_does_this_come_from_over_the_mcp_transport(platform):
    mcp = platform.mcp()
    tools = {t["name"] for t in mcp.result("tools/list")["tools"]}
    assert {"intel_where_does_this_come_from", "governance_exposure", "intel_lineage", "intel_impact_analysis"} <= tools
    out = mcp.call("intel_where_does_this_come_from", asset="Revenue (card)", type="visual")
    chains = [p["chain"] for p in out["paths"]]
    assert any(c[0].endswith("Files/raw/sales") and c[-1].endswith("Revenue (card)") and any("Total Revenue" in x for x in c) and any("gold_daily_sales" in x for x in c) for c in chains), chains
    assert out["complete"] is True or "truncated" in out
    assert "Files/raw/sales" in out["summary"] and out["paths"][0]["confidence"] in ("derived", "inferred", "verified")
    col = mcp.call("intel_where_does_this_come_from", asset="Total Revenue", type="measure")
    assert col["paths"] and {o["name"] for o in col["origins"]} >= {"Files/raw/sales"}


def test_mcp_governance_exposure_returns_the_report_for_a_label_that_provably_reaches_it(platform):
    col = platform.asset("column", "profit", parent="fact_sales")
    platform.post(f"/v1/governance/assets/{col}/classifications", {"classification": "Financial"}, expect=201)
    try:
        mcp = platform.mcp()
        out = mcp.call("governance_exposure", classification="Financial", min_confidence="inferred", max_items=100)
        reports = {e["name"]: e for e in out["exposures"] if e["type"] == "report"}
        assert reports["Retail Sales Report"]["proven"] is True and reports["Retail Sales Report"]["confidence"] == "inferred"
        visuals = {e["name"] for e in out["exposures"] if e["type"] == "visual" and e["proven"]}
        assert {"Profit (card)", "Gross margin (card)", "Revenue by category (bar)"} >= {"Profit (card)", "Gross margin (card)"} and {"Profit (card)", "Gross margin (card)"} <= visuals
        assert "Orders (card)" not in visuals and "Revenue (card)" not in visuals
        assert out["counts"]["reports"] >= 1 and out["counts"]["proven"] >= 1 and out["labelled_sources"][0]["name"] == "profit"
        # a label that reaches measures nobody displays does not make the report exposed
        col2 = platform.asset("column", "gross_amount", parent="fact_sales")
        platform.post(f"/v1/governance/assets/{col2}/classifications", {"classification": "Restricted"}, expect=201)
        try:
            out2 = mcp.call("governance_exposure", classification="Restricted", min_confidence="inferred", max_items=100)
            proven = {(e["type"], e["name"]) for e in out2["exposures"] if e["proven"]}
            assert ("measure", "Gross Revenue") in proven and ("measure", "Discount Amount") in proven
            assert ("report", "Retail Sales Report") not in proven
        finally:
            platform.http.delete(f"/v1/governance/assets/{col2}/classifications/Restricted", headers=platform.admin)
        # an agent token reads; it cannot change anything
        r = platform.http.post(f"/v1/governance/assets/{col}/classifications", json={"classification": "PII"}, headers=platform.agent("agent_ro", "Contributor"))
        assert r.status_code in (401, 403), r.text
    finally:
        platform.http.delete(f"/v1/governance/assets/{col}/classifications/Financial", headers=platform.admin)


def test_cli_lineage_where_from_and_impact_through_the_command_tree(platform):
    cli = platform.cli()
    out = cli.run("intelligence", "lineage", "gold_daily_sales", "--direction", "up", "--depth", "3").stdout
    assert "gold_daily_sales" in out and "01_Retail_Medallion_ETL" in out
    where = cli.run("intelligence", "where-from", "Total Revenue", "--type", "measure").stdout
    assert "Files/raw/sales" in where and "Total Revenue" in where
    doc = cli.json("intelligence", "lineage", "gold_daily_sales", "--direction", "down", "--depth", "4")
    assert {n["name"] for n in doc["nodes"]} >= {"Daily Sales", "Total Revenue", "Revenue (card)"}
    impact = cli.run("intelligence", "impact", "fact_sales", "--depth", "8").stdout
    assert "Retail Sales Report" in impact


def test_sdk_lineage_origin_and_impact(platform):
    client = platform.sdk()
    visual = platform.asset("visual", "Revenue (card)")
    origin = client.intelligence.origin(visual)
    assert {r["asset"]["name"] for r in origin["roots"] if r["asset"]["type"] == "file"} >= {"Files/raw/sales"}
    lin = client.intelligence.lineage(platform.asset("table", "gold_daily_sales"), direction="down", depth=4)
    assert "Revenue (card)" in {n["name"] for n in lin["nodes"]}
    impact = client.intelligence.impact(platform.asset("table", "fact_sales"), depth=8)
    assert any(i["name"] == "Retail Sales Report" for g in impact["by_type"].values() for i in g["items"])
    assert client.intelligence.get_asset(visual)["name"] == "Revenue (card)"
