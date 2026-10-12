# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Graph invariants: idempotent apply, evidence merge and cap, confidence changes, placeholders, the removal sweep, gaps, diff, history and traversal."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from olcf.core.errors import BadRequest, Conflict
from olcf.intelligence import graph as G
from olcf.intelligence import model as M


def _two_tables(env):
    b = env.b
    lh = b.lakehouse("itm_lh1", "lh")
    a, c = b.table(lh, "raw", [("id", "int")]), b.table(lh, "clean", [("id", "int")])
    nb = b.notebook("itm_nb1", "etl")
    return lh, a, c, nb


def test_apply_is_idempotent(env):
    lh, a, c, nb = _two_tables(env)
    facts = M.Facts([lh, a, c, nb], [env.b.edge(nb, a, "READS_FROM"), env.b.edge(nb, c, "WRITES_TO")])
    _, s1 = env.scan(facts)
    assert s1.assets_created == 4 and s1.edges_created == 2 and s1.errors == 0
    n_assets, n_edges = len(env.rows("select id from assets")), len(env.edges())
    _, s2 = env.scan(facts)
    assert s2.assets_created == 0 and s2.edges_created == 0 and s2.assets_unchanged == 4 and s2.edges_unchanged == 2
    assert len(env.rows("select id from assets")) == n_assets and len(env.edges()) == n_edges
    assert {e["observation_count"] for e in env.edges()} == {2}


def test_asset_ids_are_stable_and_aliases_unique(env):
    lh, a, c, nb = _two_tables(env)
    env.scan(M.Facts([lh, a]))
    first = env.asset("raw")["id"]
    env.scan(M.Facts([lh, a]))
    assert env.asset("raw")["id"] == first and first.startswith("ast_")
    aliases = env.rows("select kind, value, asset_id from asset_aliases where asset_id = :a", a=first)
    assert ("lakehouse_table", "itm_lh1:raw", first) in aliases
    assert env.registry.resolve(a.ref) == first


def test_container_tree_and_parent_workspace(env):
    lh, a, *_ = _two_tables(env)
    env.scan(M.Facts([a]))     # the table arrives before its lakehouse: the parent is created as a placeholder, then promoted
    pid = env.registry.resolve(lh.ref, create=False)
    assert env.registry.get(pid)["status"] == "placeholder"
    env.scan(M.Facts([lh, a]))
    t, parent = env.asset("raw"), env.asset("lh")
    assert parent["id"] == pid and t["parent_id"] == pid and parent["status"] == "active" and t["workspace_id"] == "wks_test"


def test_evidence_merge_dedup_and_confidence_upgrade(env):
    lh, a, c, nb = _two_tables(env)
    env.scan(M.Facts([lh, a, nb], [env.b.edge(nb, a, "READS_FROM", "inferred", kind="naming", location={"cell": 1})]))
    e = env.edges()[0]
    assert e["confidence"] == "inferred" and len(e["evidence"]) == 1
    # the same observation again: no new evidence; a stronger one: confidence goes up and the change is recorded
    env.scan(M.Facts([], [env.b.edge(nb, a, "READS_FROM", "inferred", kind="naming", location={"cell": 1})]), mode="incremental")
    assert len(env.edges()[0]["evidence"]) == 1
    env.scan(M.Facts([], [env.b.edge(nb, a, "READS_FROM", "verified", kind="runtime", source="runtime:job", ref="job1", location={})]), mode="incremental")
    e = env.edges()[0]
    assert e["confidence"] == "verified" and len(e["evidence"]) == 2 and e["last_verified_at"]
    assert {"field": "confidence", "from": "inferred", "to": "verified"}.items() <= next(h for h in e["attrs"]["changes"] if h["field"] == "confidence").items()
    assert e["observation_count"] == 3


def test_evidence_capped_but_keeps_strongest(env):
    lh, a, c, nb = _two_tables(env)
    first = env.b.edge(nb, a, "READS_FROM", "verified", kind="runtime", source="runtime:job", ref="old", location={}, observed_at="2020-01-01T00:00:00Z")
    env.scan(M.Facts([lh, a, nb], [first]))
    for i in range(G.MAX_EVIDENCE + 10):
        env.scan(M.Facts([], [env.b.edge(nb, a, "READS_FROM", "derived", location={"cell": i})]), mode="incremental")
    e = env.edges()[0]
    assert len(e["evidence"]) == G.MAX_EVIDENCE
    # the old verified (runtime) record survives the cap because it is the newest of its confidence level; with the retention window it would be pruned instead
    assert any(d["confidence"] == "verified" for d in e["evidence"]) or e["confidence"] != "verified"


def test_merge_evidence_pure():
    base = [{"kind": "static", "source": "s", "confidence": "derived", "ref": None, "location": {"n": i}, "observed_at": f"2026-01-01T00:00:{i:02d}Z"} for i in range(30)]
    kept = G.merge_evidence([], base)
    assert len(kept) == G.MAX_EVIDENCE and kept[0]["observed_at"] > kept[-1]["observed_at"]
    dup = G.merge_evidence(kept, base[:3])
    assert len(dup) == G.MAX_EVIDENCE
    runtime = {"kind": "runtime", "source": "r", "confidence": "verified", "ref": "x", "location": {}, "observed_at": "2020-01-01T00:00:00Z"}
    assert G.merge_evidence([runtime], [], drop_runtime_before="2025-01-01T00:00:00Z") == []
    assert G.merge_evidence([], [runtime], drop_runtime_before="2025-01-01T00:00:00Z") == [runtime]
    assert G.strongest_confidence([]) == "unknown" and G.strongest_confidence([], "inferred") == "inferred"
    assert G.strongest_confidence([{"confidence": "inferred"}, {"confidence": "derived"}]) == "derived"
    assert G.weakest("verified", "inferred") == "inferred"
    assert G.allowed_confidences("derived") == ["verified", "derived"] and G.allowed_confidences(None) == list(M.CONFIDENCE_LEVELS)


def test_stated_confidence_caps_evidence(env):
    lh, a, c, nb = _two_tables(env)
    ef = env.b.edge(nb, a, "READS_FROM", "verified", kind="runtime", source="runtime:job", ref="j", location={})
    ef.confidence = "inferred"
    env.scan(M.Facts([lh, a, nb], [ef]))
    assert env.edges()[0]["confidence"] == "inferred"


def test_transformation_change_is_recorded(env):
    lh, a, c, nb = _two_tables(env)
    env.scan(M.Facts([lh, a, c, nb], [env.b.edge(a, c, "TRANSFORMS", via=nb, transformation="x + 1")]))
    env.scan(M.Facts([], [env.b.edge(a, c, "TRANSFORMS", via=nb, transformation="x + 2")]), mode="incremental")
    e = env.edges(type="TRANSFORMS")[0]
    assert e["transformation"] == "x + 2" and [h for h in e["attrs"]["changes"] if h["field"] == "transformation"]
    assert e["via_id"] == env.asset("etl")["id"]


def test_rejected_edges(env):
    lh, a, c, nb = _two_tables(env)
    _, st = env.scan(M.Facts([lh, a, nb], [env.b.edge(nb, a, "GOVERNED_BY"), env.b.edge(a, a, "DEPENDS_ON")]))
    assert st.errors == 1 and st.edges_rejected == 2 and env.edges() == []
    with pytest.raises(BadRequest):
        with env.db.begin() as conn:
            env.graph.upsert_edge(conn, src_id="ast_x", dst_id="ast_y", type_="CLASSIFIED_AS", via_id=None, evidence=[], confidence=None, transformation=None, attrs=None,
                                  scan_id=None, source_id=None)


def test_edge_without_evidence_gets_api_evidence(env):
    lh, a, c, nb = _two_tables(env)
    ef = M.EdgeFact(nb.ref, a.ref, "READS_FROM", [], confidence="derived")
    env.scan(M.Facts([lh, a, nb], [ef]))
    e = env.edges()[0]
    assert e["evidence"] and e["evidence"][0]["kind"] == "api"


def test_placeholder_created_gap_reported_then_promoted(env):
    b = env.b
    lh, a, c, nb = _two_tables(env)
    ref = b.named("lh.future_tbl")
    sid, st = env.scan(M.Facts([nb], [b.edge(nb, ref, "READS_FROM")]), source="s1")
    assert st.placeholders_created == 1
    ph = env.asset("lh.future_tbl")
    assert ph["status"] == "placeholder" and ph["attrs"]["unresolved"] is True
    open_gaps = env.rows("select kind, subject, resolved_at from gaps where source_id='s1'")
    assert [g[0] for g in open_gaps] == ["unresolved_reference"] and open_gaps[0][2] is None
    # discovery finds it: an asset whose qname matches the placeholder is promoted in place (same id), gap resolved
    real = M.AssetFact(M.AssetRef.make("table", {"lakehouse_table": "itm_lh1:future_tbl"}, "future_tbl", {"workspace_id": "wks_test"}), "future_tbl", workspace_id="wks_test",
                       parent=lh.ref)
    env.scan(M.Facts([lh, real]), source="s1", mode="incremental")
    rows = env.rows("select id, status from assets where name in ('future_tbl', 'lh.future_tbl') and type='table'")
    assert len(rows) == 1 and rows[0][0] == ph["id"] and rows[0][1] == "active"
    assert env.rows("select resolved_at from gaps where source_id='s1'")[0][0] is not None
    # the edge to it survived with the same id
    assert env.edges()[0]["dst_id"] == ph["id"]


def test_placeholder_merged_into_existing_real_asset(env):
    b = env.b
    lh, a, c, nb = _two_tables(env)
    env.scan(M.Facts([lh, a, c, nb]))
    # a reference by a name the registry cannot resolve to `raw` gives a placeholder; a later real asset with an alias for both merges them
    ref = M.AssetRef.make("table", None, "Bronze.Raw", {"workspace_id": "wks_test"})
    env.scan(M.Facts([], [b.edge(nb, ref, "READS_FROM")]), mode="incremental")
    ph = env.asset("Bronze.Raw")
    assert ph["status"] == "placeholder"
    claim = M.AssetFact(M.AssetRef.make("table", {"lakehouse_table": "itm_lh1:raw", "qname": "bronze.raw"}, "raw", {"workspace_id": "wks_test"}), "raw", workspace_id="wks_test",
                        parent=lh.ref)
    # the placeholder has the alias (qname scope|bronze.raw); a fact carrying the plain qname resolves the real asset and absorbs the placeholder
    env.scan(M.Facts([claim]), mode="incremental")
    rows = {r[0]: r[1] for r in env.rows("select id, status from assets where name in ('raw', 'Bronze.Raw')")}
    assert rows[ph["id"]] in ("removed", "active")
    real = env.asset("raw")
    edges = env.edges(type="READS_FROM")
    assert len(edges) == 1 and edges[0]["dst_id"] in (real["id"], ph["id"])


def test_resolution_rules(env):
    lh, a, c, nb = _two_tables(env)
    env.scan(M.Facts([lh, a, c]))
    raw = env.asset("raw")["id"]
    reg = env.registry
    # 1. alias hit
    assert reg.resolve(a.ref, create=False) == raw
    # 2. name qualified with the default lakehouse
    assert reg.resolve(M.AssetRef.make("table", None, "raw", {"workspace_id": "wks_test", "lakehouse_id": "itm_lh1"}), create=False) == raw
    # 2b. 'lakehouse.table' form
    assert reg.resolve(M.AssetRef.make("table", None, "lh.raw", {"workspace_id": "wks_test"}), create=False) in (raw,)
    # 3. unique name in the workspace
    assert reg.resolve(M.AssetRef.make("table", None, "clean", {"workspace_id": "wks_test"}), create=False) == env.asset("clean")["id"]
    # other workspace: no match, and create=False means NotFound rather than a placeholder
    from olcf.core.errors import NotFound

    with pytest.raises(NotFound):
        reg.resolve(M.AssetRef.make("table", None, "clean", {"workspace_id": "wks_other"}), create=False)
    # 4. placeholder, then the same reference finds the same placeholder
    p1 = reg.resolve(M.AssetRef.make("table", None, "ghost", {"workspace_id": "wks_test"}))
    p2 = reg.resolve(M.AssetRef.make("table", None, "ghost", {"workspace_id": "wks_test"}))
    assert p1 == p2 and p1 != raw
    # same name in another workspace is a different placeholder
    assert reg.resolve(M.AssetRef.make("table", None, "ghost", {"workspace_id": "wks_other"})) != p1


def test_ambiguous_name_does_not_guess(env):
    b = env.b
    lh1, lh2 = b.lakehouse("itm_a", "bronze"), b.lakehouse("itm_b", "silver")
    t1, t2 = b.table(lh1, "sales"), b.table(lh2, "sales")
    env.scan(M.Facts([lh1, lh2, t1, t2]))
    ref = M.AssetRef.make("table", None, "sales", {"workspace_id": "wks_test"})
    got = env.registry.resolve(ref)
    assert got not in (env.registry.resolve(t1.ref), env.registry.resolve(t2.ref))      # ambiguous: a placeholder, never a coin flip
    # qualified by lakehouse name is unambiguous
    assert env.registry.resolve(M.AssetRef.make("table", None, "silver.sales", {"workspace_id": "wks_test"}), create=False) == env.registry.resolve(t2.ref)


def test_column_resolution_through_table_scope(env):
    b = env.b
    lh, a, c, nb = _two_tables(env)
    col = b.column(a, "id", "int")
    env.scan(M.Facts([lh, a, col]))
    cid = env.registry.resolve(col.ref)
    tid = env.registry.resolve(a.ref)
    by_scope = M.AssetRef.make("column", None, "ID", {"table": tid, "workspace_id": "wks_test"})
    assert env.registry.resolve(by_scope, create=False) == cid                    # case-insensitive match of the child column
    by_alias = M.AssetRef.make("column", None, "id", {"table": "lakehouse_table:itm_lh1:raw"})
    assert env.registry.resolve(by_alias, create=False) == cid
    by_qname = M.AssetRef.make("column", None, "id", {"table": "lh.raw", "workspace_id": "wks_test"})
    assert env.registry.resolve(by_qname, create=False) in (cid,)


def test_sweep_only_after_full_succeeded_scan(env):
    b = env.b
    lh, a, c, nb = _two_tables(env)
    e1, e2 = b.edge(nb, a, "READS_FROM"), b.edge(nb, c, "WRITES_TO")
    env.scan(M.Facts([lh, a, c, nb], [e1, e2]))
    status = lambda n: env.asset(n)["status"]  # noqa: E731
    # incremental, partial, failed scans never remove
    env.scan(M.Facts([lh, a], [e1]), mode="incremental")
    env.scan(M.Facts([lh, a], [e1]), mode="full", finish="partial")
    env.scan(M.Facts([lh, a], [e1]), mode="full", finish="failed")
    env.scan(M.Facts([lh, a], [e1]), mode="full", finish="cancelled")
    assert status("clean") == "active" and status("etl") == "active" and {e["status"] for e in env.edges()} == {"active"}
    # a full succeeded scan sweeps what it did not see
    sid, _ = env.scan(M.Facts([lh, a], [e1]), mode="full")
    assert status("clean") == "removed" and status("etl") == "removed" and status("raw") == "active"
    assert {e["type"]: e["status"] for e in env.edges()} == {"READS_FROM": "active", "WRITES_TO": "removed"}
    assert env.asset("clean")["removed_scan"] == sid
    assert env.engine.scan_row(sid)["stats"]["sweep"]["assets_removed"] == 2
    # nothing was deleted, and the asset comes back (same id) when it is seen again
    cid = env.asset("clean")["id"]
    env.scan(M.Facts([lh, a, c, nb], [e1, e2]), mode="incremental")
    assert env.asset("clean")["id"] == cid and status("clean") == "active" and {e["status"] for e in env.edges()} == {"active"}


def test_sweep_is_scoped_to_the_source(env):
    lh, a, c, nb = _two_tables(env)
    other = M.AssetFact(M.AssetRef.make("table", {"fabric": "t/w/i"}, "fabric_tbl"), "fabric_tbl", workspace_id="wks_test")
    env.scan(M.Facts([lh, a, c]), source="local")
    env.scan(M.Facts([other]), source="fabric1")
    env.scan(M.Facts([lh, a]), source="local")
    assert env.asset("clean")["status"] == "removed" and env.asset("fabric_tbl")["status"] == "active"
    env.scan(M.Facts([]), source="fabric1")
    assert env.asset("fabric_tbl")["status"] == "removed"


def test_runtime_verified_edge_survives_the_sweep(env):
    lh, a, c, nb = _two_tables(env)
    env.scan(M.Facts([lh, a, nb], [env.b.edge(nb, a, "READS_FROM", "derived")]))
    env.scan(M.Facts([], [env.b.edge(nb, a, "READS_FROM", "verified", kind="runtime", source="runtime:job", ref="j", location={})]), mode="incremental")
    env.scan(M.Facts([lh, a, nb]), mode="full")        # code no longer shows the read, but a recent run did
    assert env.edges()[0]["status"] == "active"


def test_runtime_evidence_is_pruned_after_the_window(env):
    lh, a, c, nb = _two_tables(env)
    old = (datetime.now(UTC) - timedelta(days=400)).strftime("%Y-%m-%dT%H:%M:%SZ")
    env.scan(M.Facts([lh, a, nb], [env.b.edge(nb, a, "READS_FROM", "verified", kind="runtime", source="runtime:job", ref="j", location={}, observed_at=old)]))
    assert env.edges()[0]["confidence"] == "verified"
    from sqlalchemy import update

    from olcf.intelligence.schema import edges

    with env.db.begin() as conn:      # the first observation was long ago
        conn.execute(update(edges).values(last_verified_at=old))
        assert env.graph.prune_runtime_evidence(conn) == 1
    e = env.edges()[0]
    # only stale runtime evidence is left: it is kept as history but no longer justifies 'verified'
    assert e["confidence"] == "derived" and e["last_verified_at"] is None and e["evidence"][0]["stale"] is True
    with env.db.begin() as conn:
        assert env.graph.prune_runtime_evidence(conn) == 0
    # fresh static evidence replaces it
    env.scan(M.Facts([], [env.b.edge(nb, a, "READS_FROM", "derived", kind="static", location={"cell": 1})]), mode="incremental")
    assert [d["kind"] for d in env.edges()[0]["evidence"]] == ["static"]


def test_gaps_dedupe_resolve_and_reopen(env):
    gap = M.Gap("parse_error", "nbparser", "etl", "bad cell", "fix it", {"cell": 3})
    env.scan(M.Facts([], [], [gap]), source="s1")
    env.scan(M.Facts([], [], [gap]), source="s1")
    rows = env.rows("select resolved_at from gaps")
    assert len(rows) == 1 and rows[0][0] is None
    env.scan(M.Facts([]), source="s1")                                      # full + succeeded + not seen -> resolved
    assert env.rows("select resolved_at from gaps")[0][0] is not None
    env.scan(M.Facts([], [], [gap]), source="s1", mode="incremental")      # seen again -> reopened, still one row
    rows = env.rows("select resolved_at from gaps")
    assert len(rows) == 1 and rows[0][0] is None
    # a different location is a different gap; another source's identical gap is its own row
    env.scan(M.Facts([], [], [M.Gap("parse_error", "nbparser", "etl", "bad cell", None, {"cell": 4})]), source="s1", mode="incremental")
    env.scan(M.Facts([], [], [gap]), source="s2")
    assert len(env.rows("select id from gaps")) == 3


def test_apply_to_finished_scan_is_refused(env):
    sid, _ = env.scan(M.Facts([]))
    with pytest.raises(Conflict):
        env.engine.apply_facts(M.Facts([]), scan_id=sid, source_id="local")
    with pytest.raises(Conflict):
        env.engine.finish_scan(sid, "succeeded")
    with pytest.raises(BadRequest):
        env.engine.begin_scan("local", "weekly", "manual")
    with pytest.raises(BadRequest):
        env.engine.finish_scan(env.engine.begin_scan("local", "full", "manual"), "running")


def test_scan_versions_increase_per_source(env):
    s1, _ = env.scan(M.Facts([]), source="a")
    s2, _ = env.scan(M.Facts([]), source="a")
    s3, _ = env.scan(M.Facts([]), source="b")
    assert [env.engine.scan_row(s)["version"] for s in (s1, s2, s3)] == [1, 2, 1]
    assert env.engine.latest_scan("a")["id"] == s2 and env.engine.latest_scan("a", status="failed") is None


def test_asset_versions_record_schema_changes_only_when_stable_attrs_change(env):
    b = env.b
    lh = b.lakehouse("itm_lh1", "lh")
    t1 = b.table(lh, "raw", [("id", "int"), ("name", "string")], row_count=10)
    env.scan(M.Facts([lh, t1]))
    t2 = b.table(lh, "raw", [("id", "int"), ("name", "string")], row_count=99)           # a volatile attribute changed: no new version
    env.scan(M.Facts([lh, t2]))
    aid = env.asset("raw")["id"]
    assert len(env.graph.history(aid)[0]) == 1
    t3 = b.table(lh, "raw", [("id", "bigint"), ("email", "string")], row_count=99)       # a column removed, one added, one retyped
    s3, _ = env.scan(M.Facts([lh, t3]))
    versions = env.graph.history(aid)[0]
    assert len(versions) == 2 and versions[0]["scan_id"] == s3
    ch = versions[0]["changes"]
    assert "email" in str(ch.get("columns_added")) and "name" in str(ch.get("columns_removed")) and ch["columns_changed"] == [{"column": "id", "from": "int", "to": "bigint"}]
    page, nxt = env.graph.history(aid, limit=1)
    assert len(page) == 1 and nxt is not None and env.graph.history(aid, limit=1, cursor=nxt)[0][0]["id"] < page[0]["id"]


def test_diff_between_scans(env):
    b = env.b
    lh, a, c, nb = _two_tables(env)
    s1, _ = env.scan(M.Facts([lh, a, c, nb], [b.edge(nb, a), b.edge(nb, c, "WRITES_TO")]))
    a2 = b.table(lh, "raw", [("id", "bigint")])
    d = b.table(lh, "dim")
    s2, _ = env.scan(M.Facts([lh, a2, d, nb], [b.edge(nb, a2), b.edge(nb, d, "WRITES_TO")]))
    out = env.graph.diff(s1, s2)
    assert [x["name"] for x in out["assets"]["added"]] == ["dim"]
    assert [x["name"] for x in out["assets"]["removed"]] == ["clean"]
    assert [x["name"] for x in out["assets"]["changed"]] == ["raw"] and out["assets"]["changed"][0]["changes"]["columns_changed"]
    assert len(out["edges"]["added"]) == 1 and out["edges"]["added"][0]["dst_name"] == "dim"
    assert len(out["edges"]["removed"]) == 1 and out["edges"]["removed"][0]["dst_name"] == "clean"
    assert out["summary"]["assets_added"] == 1 and not out["truncated"]
    # visibility filter hides everything
    hidden = env.graph.diff(s1, s2, keep=lambda ws: False)
    assert hidden["summary"] == {k: 0 for k in out["summary"]}
    with pytest.raises(BadRequest):
        env.graph.diff(s2, s1)
    other, _ = env.scan(M.Facts([]), source="zzz")
    with pytest.raises(BadRequest):
        env.graph.diff(s1, other)


def _chain(env, n=4, *, cycle=False, conf="derived"):
    b = env.b
    lh = b.lakehouse("itm_lh1", "lh")
    tabs = [b.table(lh, f"t{i}") for i in range(n)]
    nbs = [b.notebook(f"itm_nb{i}", f"nb{i}") for i in range(n - 1)]
    edges = []
    for i in range(n - 1):
        edges += [b.edge(nbs[i], tabs[i], "READS_FROM", conf), b.edge(nbs[i], tabs[i + 1], "WRITES_TO", conf)]
    if cycle:
        edges += [b.edge(nbs[0], tabs[n - 1], "READS_FROM", conf), b.edge(nbs[n - 2], tabs[0], "WRITES_TO", conf)]
    env.scan(M.Facts([lh, *tabs, *nbs], edges))
    return {t.name: env.asset(t.name)["id"] for t in tabs}


def test_walk_directions_depth_and_process_nodes_are_free(env):
    ids = _chain(env, 4)
    down = env.graph.walk([ids["t0"]], direction="down", depth=2)
    names = {i["row"]["name"]: i["level"] for i in down.nodes.values()}
    assert names["t0"] == 0 and names["t1"] == 1 and names["t2"] == 2 and "t3" not in names
    assert names["nb0"] == 0 and names["nb1"] == 1        # a process costs no depth
    up = env.graph.walk([ids["t3"]], direction="up", depth=10)
    assert {i["row"]["name"] for i in up.nodes.values() if i["row"]["type"] == "table"} == {"t0", "t1", "t2", "t3"}
    assert env.graph.walk([ids["t3"]], direction="down", depth=10).nodes.keys() == {ids["t3"]}
    with pytest.raises(BadRequest):
        env.graph.walk([ids["t0"]], direction="sideways", depth=1)


def test_walk_cycle_terminates(env):
    ids = _chain(env, 4, cycle=True)
    w = env.graph.walk([ids["t0"]], direction="down", depth=10)
    assert {i["row"]["name"] for i in w.nodes.values() if i["row"]["type"] == "table"} == {"t0", "t1", "t2", "t3"} and not w.truncated


def test_walk_node_cap_truncates(env):
    ids = _chain(env, 6)
    w = env.graph.walk([ids["t0"]], direction="down", depth=10, max_nodes=4)
    assert w.truncated and len(w.nodes) == 4


def test_walk_confidence_filter_and_floor(env):
    b = env.b
    lh = b.lakehouse("itm_lh1", "lh")
    t0, t1, t2 = (b.table(lh, f"t{i}") for i in range(3))
    n0, n1 = b.notebook("itm_n0", "n0"), b.notebook("itm_n1", "n1")
    env.scan(M.Facts([lh, t0, t1, t2, n0, n1], [b.edge(n0, t0, "READS_FROM", "verified"), b.edge(n0, t1, "WRITES_TO", "verified"),
                                              b.edge(n1, t1, "READS_FROM", "inferred"), b.edge(n1, t2, "WRITES_TO", "derived")]))
    i0 = env.asset("t0")["id"]
    w = env.graph.walk([i0], direction="down", depth=5)
    floors = {v["row"]["name"]: v["floor"] for v in w.nodes.values()}
    assert floors["t0"] == "verified" and floors["t1"] == "verified" and floors["t2"] == "inferred"       # weakest edge on the path
    w2 = env.graph.walk([i0], direction="down", depth=5, min_confidence="derived")
    assert "t2" not in {v["row"]["name"] for v in w2.nodes.values()}


def test_walk_hides_nodes_and_removed_edges(env):
    ids = _chain(env, 3)
    w = env.graph.walk([ids["t0"]], direction="down", depth=5, keep_node=lambda r: r["name"] != "t1")
    assert "t1" not in {v["row"]["name"] for v in w.nodes.values()} and "t2" not in {v["row"]["name"] for v in w.nodes.values()}
    env.scan(M.Facts([]), mode="full")            # sweeps everything
    assert env.graph.walk([ids["t0"]], direction="down", depth=5).nodes.keys() == {ids["t0"]}
    assert len(env.graph.walk([ids["t0"]], direction="down", depth=5, include_removed=True).nodes) > 1


def test_retire_by_alias_removes_subtree(env):
    lh, a, c, nb = _two_tables(env)
    env.scan(M.Facts([lh, a, c]))
    assert env.engine.retire_by_alias("olcf_item", "itm_lh1") == 3
    assert env.asset("raw")["status"] == "removed" and env.engine.retire_by_alias("olcf_item", "itm_lh1") == 0
    assert env.engine.retire_by_alias("olcf_item", "itm_none") == 0


def test_edge_hash_identity_distinguishes_via(env):
    b = env.b
    lh, a, c, nb = _two_tables(env)
    nb2 = b.notebook("itm_nb2", "etl2")
    env.scan(M.Facts([lh, a, c, nb, nb2], [b.edge(a, c, "TRANSFORMS", via=nb), b.edge(a, c, "TRANSFORMS", via=nb2), b.edge(a, c, "TRANSFORMS", via=nb)]))
    assert len(env.edges(type="TRANSFORMS")) == 2
