# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Column-level lineage rooted at a measure or a visual, and the table-level lineage of a measure.

Regression: ``level=column`` only started at columns (a measure or visual root answered with its table-level edges or nothing), and a measure had no edge to the model table it belongs to,
so "where does the number in this visual come from?" stopped at the measure unless the caller already knew the source column.
"""

from __future__ import annotations

from olcf.intelligence import model as M


def _build(intel):
    ws = intel.workspace("bi")["id"]
    b = intel.b.__class__(ws)
    lh = b.lakehouse("itm_lh", "gold_lh")
    sales = b.table(lh, "sales", [("amount", "double"), ("day", "date")])
    amount = b.column(sales, "amount", "double")
    model = M.AssetFact(M.AssetRef.make("semantic_model", {"olcf_item": "itm_sm"}), "model", workspace_id=ws)
    mtable = M.AssetFact(M.AssetRef.make("model_table", {"pbi": "g/m/sales"}, "Sales"), "Sales", parent=model.ref, workspace_id=ws)
    mcol = M.AssetFact(M.AssetRef.make("column", {"pbi": "g/m/sales/amount"}, "amount"), "amount", parent=mtable.ref, workspace_id=ws)
    measure = M.AssetFact(M.AssetRef.make("measure", {"pbi": "g/m/sales/total"}, "Total"), "Total", parent=mtable.ref, workspace_id=ws)
    report = M.AssetFact(M.AssetRef.make("report", {"olcf_item": "itm_rp"}), "report", workspace_id=ws)
    visual = M.AssetFact(M.AssetRef.make("visual", {"pbi": "g/r/card"}, "Card"), "Card", parent=report.ref, workspace_id=ws)
    nb = b.notebook("itm_nb", "etl")
    edges = [
        b.edge(nb, sales, "WRITES_TO"),
        b.edge(mtable, sales, "READS_FROM"),
        b.edge(mcol, amount, "TRANSFORMS", via=nb),
        b.edge(mcol, measure, "TRANSFORMS"),
        b.edge(measure, mtable, "DEPENDS_ON"),
        b.edge(model, mtable, "DEPENDS_ON"),
        b.edge(report, model, "DEPENDS_ON"),
        b.edge(visual, measure, "DISPLAYS"),
    ]
    intel.scan(M.Facts([lh, sales, amount, model, mtable, mcol, measure, report, visual, nb], edges))
    return {n: intel.asset_id(n) for n in ("Total", "Card", "Sales", "report", "model")}, ws


def test_a_measure_and_a_visual_are_roots_of_column_lineage(intel):
    ids, _ = _build(intel)
    vis = intel.get(f"/v1/intelligence/assets/{ids['Card']}/lineage", params={"direction": "up", "level": "column", "depth": 5}, headers=intel.admin).json()
    names = {n["name"]: n["direction"] for n in vis["nodes"]}
    assert names["Card"] == "root" and names["Total"] == "upstream"
    assert [e["type"] for e in vis["edges"] if e["type"] == "DISPLAYS"] == ["DISPLAYS"]
    origin = intel.get(f"/v1/intelligence/assets/{ids['Total']}/origin", params={"level": "column"}, headers=intel.admin).json()
    assert origin["nodes"] and any(r["asset"]["name"] == "amount" for r in origin["roots"])


def test_the_table_level_lineage_of_a_measure_reaches_its_model_table_and_the_gold_table(intel):
    ids, _ = _build(intel)
    doc = intel.get(f"/v1/intelligence/assets/{ids['Card']}/origin", headers=intel.admin).json()
    nodes = {n["id"]: n for n in doc["nodes"]}
    chains = [[(nodes[i]["type"], nodes[i]["name"]) for i in p["nodes"]] for p in doc["paths"]]
    assert [("notebook", "etl"), ("table", "sales"), ("model_table", "Sales"), ("measure", "Total"), ("visual", "Card")] in chains or any(
        c[-4:] == [("table", "sales"), ("model_table", "Sales"), ("measure", "Total"), ("visual", "Card")] for c in chains), chains
    impact = intel.get(f"/v1/intelligence/assets/{intel.asset_id('sales')}/impact", headers=intel.admin).json()
    reached = {(i["type"], i["name"]) for g in impact["by_type"].values() for i in g["items"]}
    assert {("model_table", "Sales"), ("measure", "Total"), ("visual", "Card"), ("report", "report"), ("semantic_model", "model")} <= reached
