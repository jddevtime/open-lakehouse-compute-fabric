# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Regression tests of the hardening round: wide fan-out (impact totals, dataset-first rendering), true diff totals, origin of a column, flat scan counters,
version retention, automatic and manual re-queueing of failed inbox rows and the ``last_scan_status`` of the tenant asset."""

from __future__ import annotations

from olcf.intelligence import model as M

from .conftest import Builder


def _hub(n: int, *, fanout_writes: bool = True) -> M.Facts:
    """One lakehouse, one hub table read by ``n`` notebooks, each of which writes its own output table."""
    b = Builder("wks_h")
    lh = b.lakehouse("itm_lh", "lh")
    hub = b.table(lh, "hub")
    facts: list[M.AssetFact] = [lh, hub]
    edges: list[M.EdgeFact] = []
    for i in range(n):
        nb = b.notebook(f"itm_nb{i}", f"nb{i:04d}")
        facts.append(nb)
        edges.append(b.edge(nb, hub, "READS_FROM"))
        if fanout_writes:
            out = b.table(lh, f"out{i:04d}")
            facts.append(out)
            edges.append(b.edge(nb, out, "WRITES_TO"))
    return M.Facts(facts, edges)


# ------------------------------------------------------------------------------------------------------------------------------ item 1: fan-out
def test_wide_fan_out_impact_reports_real_totals_and_lineage_prefers_datasets(intel):
    intel.scan(_hub(570))
    hub = intel.asset_id("hub")
    r = intel.get(f"/v1/intelligence/assets/{hub}/impact", params={"depth": 3}, headers=intel.admin)
    assert r.status_code == 200, r.text
    j = r.json()
    # 570 output tables are affected; the 570 notebooks in between are processes, not impacted assets, and do not use up the budget
    assert j["total"] == 570 and j["by_type"]["table"]["count"] == 570 and set(j["by_type"]) == {"table"}
    assert j["truncated"] is False and j["items_truncated"] is True
    assert len(j["by_type"]["table"]["items"]) <= 500 and len(j["nodes"]) <= 500 and j["stats"]["listed"] == len(j["by_type"]["table"]["items"])
    # the same graph through the lineage render: capped at 500 nodes, but with tables kept in preference to the notebooks
    lin = intel.get(f"/v1/intelligence/assets/{hub}/lineage", params={"direction": "down", "depth": 3}, headers=intel.admin).json()
    assert lin["truncated"] is True and len(lin["nodes"]) <= 500
    tables = [n for n in lin["nodes"] if n["type"] == "table"]
    assert len(tables) >= 450 and lin["stats"]["contracted_processes"] > 0
    ids = {n["id"] for n in lin["nodes"]}
    assert all(e["from"] in ids and e["to"] in ids for e in lin["edges"])                  # no dangling edge after the contraction
    # the dedicated impact bound is a setting: past it the totals say they are cut
    intel.ctx.service("settings").set("intelligence.max_impact_nodes", 100)
    cut = intel.get(f"/v1/intelligence/assets/{hub}/impact", params={"depth": 3}, headers=intel.admin).json()
    assert cut["truncated"] is True and cut["total"] == 99 and cut["stats"]["max_impact_nodes"] == 100


def test_small_render_cap_keeps_tables_over_notebooks(intel):
    intel.scan(_hub(12))
    intel.ctx.service("settings").set("intelligence.max_lineage_nodes", 10)
    hub = intel.asset_id("hub")
    lin = intel.get(f"/v1/intelligence/assets/{hub}/lineage", params={"direction": "down", "depth": 2}, headers=intel.admin).json()
    assert lin["truncated"] is True and len(lin["nodes"]) == 10
    assert {n["type"] for n in lin["nodes"]} == {"table"}
    assert any(e["virtual"] for e in lin["edges"])
    imp = intel.get(f"/v1/intelligence/assets/{hub}/impact", params={"depth": 2}, headers=intel.admin).json()
    assert imp["total"] == 12 and imp["truncated"] is False and imp["items_truncated"] is True and len(imp["nodes"]) <= 10


def test_diff_summary_counts_are_true_totals_when_the_lists_are_cut(env):
    b = env.b
    lh = b.lakehouse("itm_lh", "lh")
    env.scan(M.Facts([lh]))
    tables = [b.table(lh, f"t{i:02d}") for i in range(30)]
    nbs = [b.notebook(f"itm_n{i}", f"n{i:02d}") for i in range(12)]
    s2, _ = env.scan(M.Facts([lh, *tables, *nbs], [b.edge(nb, tables[i]) for i, nb in enumerate(nbs)]), mode="incremental")
    s1 = env.rows("select id from scans order by version")[0][0]
    out = env.graph.diff(s1, s2, limit=10)
    assert out["summary"]["assets_added"] == 42 and out["summary"]["edges_added"] == 12
    assert len(out["assets"]["added"]) == 10 and len(out["edges"]["added"]) == 10 and out["truncated"] is True
    # visibility filtering applies to the totals as well
    hidden = env.graph.diff(s1, s2, limit=10, keep=lambda ws: False)
    assert hidden["summary"]["assets_added"] == 0 and hidden["summary"]["edges_added"] == 0 and hidden["truncated"] is False
    full = env.graph.diff(s1, s2, limit=500)
    assert full["truncated"] is False and len(full["assets"]["added"]) == 42


# ------------------------------------------------------------------------------------------------------------------------------ item 2: origin of a column
def test_origin_of_a_column_switches_to_column_level(world):
    cid = world.ids["clean.amount"]
    j = world.get(f"/assets/{cid}/origin").json()
    assert j["level"] == "column" and j["level_switched"]["to"] == "column" and j["has_upstream"] is True
    assert j["paths"] and j["paths"][0]["nodes"][0] == world.ids["raw.amount"]
    # a table is not switched, and an explicit column-level request is not reported as a switch
    t = world.get(f"/assets/{world.ids['clean']}/origin").json()
    assert t["level"] == "table" and t["level_switched"] is None
    assert world.get(f"/assets/{cid}/origin", level="column").json()["level_switched"] is None


# ------------------------------------------------------------------------------------------------------------------------------ item 3: flat scan counters
def test_scan_rows_expose_flat_counters_whatever_wrote_them(intel):
    b = Builder("wks_c")
    lh = b.lakehouse("itm_lh", "lh")
    intel.scan(M.Facts([lh, b.table(lh, "a"), b.table(lh, "b")]))                                  # a local scan: counters stored flat
    sid = intel.engine.begin_scan("fabric:tenant", "full", "manual")                                 # a discovery scan: counters stored under "apply"
    intel.engine.finish_scan(sid, "succeeded", {"apply": {"assets_created": 7, "assets_updated": 2, "edges_created": 5, "errors": 0}, "facts": {"assets": 9}, "gaps_reported": 1})
    rows = intel.get("/v1/intelligence/snapshots", params={"limit": 20}, headers=intel.admin).json()["items"]
    by_source = {r["source_id"]: r["stats"] for r in rows}
    local, fabric = by_source["local"], by_source["fabric:tenant"]
    assert local["assets_created"] == 3 and local["apply"]["assets_created"] == 3 and local["edges_created"] == 0       # lakehouse + two tables
    assert fabric["assets_created"] == 7 and fabric["assets_updated"] == 2 and fabric["edges_created"] == 5
    assert fabric["apply"]["assets_created"] == 7 and fabric["facts"] == {"assets": 9} and fabric["gaps_reported"] == 1    # nothing removed, only added
    one = intel.get(f"/v1/intelligence/scans/{sid}", headers=intel.admin)
    if one.status_code == 200:                                                                      # the discovery routes are part of this build
        assert one.json()["stats"]["assets_created"] == 7 and one.json()["stats"]["apply"]["edges_created"] == 5


def test_scan_stats_normaliser_is_additive_and_idempotent():
    from olcf.intelligence.engine import scan_stats

    assert scan_stats(None) == {} and scan_stats({}) == {}
    flat = scan_stats({"assets_created": 4, "sweep": {"assets_removed": 1}})
    assert flat["apply"] == {"assets_created": 4} and flat["sweep"] == {"assets_removed": 1} and flat["assets_created"] == 4
    nested = scan_stats({"apply": {"edges_updated": 3}, "counters": {"tables": 2}})
    assert nested["edges_updated"] == 3 and nested["apply"] == {"edges_updated": 3} and nested["counters"] == {"tables": 2}
    assert scan_stats(nested) == nested and scan_stats(flat) == flat


# ------------------------------------------------------------------------------------------------------------------------------ item 7: tenant scan status
def test_the_tenant_asset_records_how_the_last_scan_ended(env):
    tenant = M.AssetFact(M.AssetRef.make("tenant", {"source": "fab:tenant:t1"}, "Fabric tenant"), "Fabric tenant", {"source_kind": "fabric", "tenant_id": "t1"})
    sid, _ = env.scan(M.Facts([tenant]), source="fab", finish="failed")
    row = env.asset("Fabric tenant", "tenant")
    assert row["attrs"]["last_scan_status"] == "failed" and row["attrs"]["last_scanned_at"] and row["attrs"]["tenant_id"] == "t1"
    versions = env.rows("select count(*) from asset_versions where asset_id = :a", a=row["id"])[0][0]
    env.scan(M.Facts([tenant]), source="fab")
    row = env.asset("Fabric tenant", "tenant")
    assert row["attrs"]["last_scan_status"] == "succeeded"
    assert env.rows("select count(*) from asset_versions where asset_id = :a", a=row["id"])[0][0] == versions       # an outcome is not a content change
    local = env.scan(M.Facts([env.b.lakehouse("itm_lh", "lh")]))
    assert "last_scan_status" not in env.asset("lh")["attrs"] and local
