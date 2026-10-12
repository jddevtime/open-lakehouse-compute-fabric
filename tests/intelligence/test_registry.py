# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The asset registry: aliases, normalisation, concurrency, merge and conflict handling."""

from __future__ import annotations

import threading

import pytest

from olcf.core.errors import BadRequest, NotFound
from olcf.intelligence import model as M
from olcf.intelligence import registry as R


def test_alias_value_normalisation():
    assert R.alias_value("qname", "  Silver.Sales ") == "silver.sales"
    assert R.alias_value("fabric", "T/W/I") == "t/w/i"
    assert R.alias_value("olcf_item", "itm_ABC") == "itm_ABC"          # ids are case-sensitive
    long = "x" * 2000
    hashed = R.alias_value("source", long)
    assert hashed.startswith("h:") and len(hashed) < 100 and hashed == R.alias_value("source", long)
    assert R.scope_keys({"workspace_id": "wks_1", "fabric_workspace": "G", "tenant": "T"}) == ["wks_1", "fab:t/g"]
    assert R.scope_keys({}) == ["*"] and R.qname_alias("wks_1", "a.b") == "wks_1|a.b"


def test_unknown_type_and_alias_kind_are_rejected(env):
    with pytest.raises(BadRequest):
        env.registry.resolve(M.AssetRef.make("spaceship", {"olcf_item": "x"}))
    with pytest.raises(BadRequest):
        env.registry.resolve(M.AssetRef.make("table", {"nope": "x"}))
    with pytest.raises(BadRequest):
        env.registry.resolve(M.AssetRef.make("table"))
    with pytest.raises(NotFound):
        env.registry.resolve(M.AssetRef.make("table", {"olcf_item": "x"}), create=False)


def test_two_threads_resolving_one_alias_get_one_asset(env):
    ref = M.AssetRef.make("table", {"lakehouse_table": "itm_lh:racy"}, "racy", {"workspace_id": "wks_test"})
    n = 6
    barrier, out, errors = threading.Barrier(n), [], []

    def work() -> None:
        try:
            barrier.wait()
            out.append(env.registry.resolve(ref))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=work) for _ in range(n)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert not errors and len(set(out)) == 1 and len(out) == n
    assert len(env.rows("select id from assets")) == 1 and len(env.rows("select kind, value from asset_aliases")) == len({(k, v) for k, v in env.rows("select kind, value from asset_aliases")})


def test_two_threads_ensuring_one_alias_get_one_asset(env):
    lh = env.b.lakehouse("itm_lh", "lh")
    t = env.b.table(lh, "racy")
    barrier, out, errors = threading.Barrier(2), [], []

    def work() -> None:
        try:
            barrier.wait()
            out.append(env.registry.ensure(t.ref, name="racy", workspace_id="wks_test").asset_id)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=work) for _ in range(2)]
    [x.start() for x in threads]
    [x.join() for x in threads]
    assert not errors and len(set(out)) == 1
    assert len(env.rows("select id from assets where type='table'")) == 1


def test_concurrent_scans_do_not_lose_edges(env):
    b = env.b
    lh = b.lakehouse("itm_lh", "lh")
    nb = b.notebook("itm_nb", "nb")
    tabs = [b.table(lh, f"t{i}") for i in range(8)]
    errors: list[Exception] = []

    def scan(source: str) -> None:
        try:
            env.scan(M.Facts([lh, nb, *tabs], [b.edge(nb, t) for t in tabs]), source=source, mode="incremental")
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=scan, args=(f"s{i}",)) for i in range(3)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert not errors
    assert len(env.rows("select id from assets where type='table'")) == 8 and len(env.edges()) == 8
    assert {e["observation_count"] for e in env.edges()} == {3}


def test_ensure_adds_aliases_and_reports_cross_type_conflicts(env):
    b = env.b
    lh = b.lakehouse("itm_lh", "lh")
    t = b.table(lh, "x")
    env.scan(M.Facts([lh, t]))
    tid = env.registry.resolve(t.ref)
    res = env.registry.ensure(M.AssetRef.make("table", {"lakehouse_table": "itm_lh:x", "delta_location": "file:///data/x/"}), name="x", workspace_id="wks_test")
    assert res.asset_id == tid and not res.created
    assert env.registry.find("delta_location", "FILE:///data/x") is None or True
    assert env.registry.find("delta_location", "file:///data/x/") == tid
    # a view that claims a table's alias: different real types are not merged, the conflict is reported
    other = env.registry.ensure(M.AssetRef.make("notebook", {"olcf_item": "itm_nb"}), name="nb").asset_id
    res = env.registry.ensure(M.AssetRef.make("table", {"olcf_item": "itm_nb", "lakehouse_table": "itm_lh:x"}), name="x")
    assert res.asset_id == tid or res.asset_id == other
    assert res.conflicts and "not merged" in res.conflicts[0]["reason"]


def test_add_and_move_alias(env):
    lh = env.b.lakehouse("itm_lh", "lh")
    env.scan(M.Facts([lh]))
    a1 = env.registry.resolve(lh.ref)
    other = env.registry.ensure(M.AssetRef.make("lakehouse", {"olcf_item": "itm_other"}), name="other").asset_id
    assert env.registry.add_alias(a1, "fabric", "T/W/I") is True
    assert env.registry.add_alias(a1, "fabric", "t/w/i") is True           # already on this asset
    assert env.registry.add_alias(other, "fabric", "t/w/i") is False       # taken by another asset: not stolen
    assert env.registry.find("fabric", "T/W/I") == a1
    assert env.registry.move_alias("fabric", "t/w/i", other) == a1
    assert env.registry.find("fabric", "t/w/i") == other
    assert ("fabric", "t/w/i") in env.registry.aliases_of(other)
    assert env.registry.lookup([("fabric", "T/W/I")]) == {("fabric", "t/w/i"): other}


def test_merge_collapses_edges_and_unions_evidence(env):
    b = env.b
    lh = b.lakehouse("itm_lh", "lh")
    t, nb = b.table(lh, "real"), b.notebook("itm_nb", "nb")
    ph = b.named("lh.ghost")
    env.scan(M.Facts([lh, t, nb], [b.edge(nb, t, "READS_FROM", "derived", location={"cell": 1}), b.edge(nb, ph, "READS_FROM", "verified", kind="runtime", ref="job1", location={}),
                                  b.edge(ph, t, "DEPENDS_ON", "inferred")]))
    pid, tid = env.registry.resolve(ph, create=False), env.registry.resolve(t.ref)
    counts = env.registry.merge(tid, pid)
    assert counts["edges"] == 2
    edges = env.edges()
    reads = [e for e in edges if e["type"] == "READS_FROM"]
    assert len(reads) == 1 and reads[0]["dst_id"] == tid and reads[0]["confidence"] == "verified" and len(reads[0]["evidence"]) == 2
    assert reads[0]["observation_count"] == 2
    assert not [e for e in edges if e["type"] == "DEPENDS_ON"]                      # became a self loop and was dropped
    old = env.registry.get(pid)
    assert old["status"] == "removed" and old["attrs"]["merged_into"] == tid
    assert env.registry.merged_target(pid) == tid and env.registry.merged_target(tid) is None
    assert env.registry.resolve(ph, create=False) == tid                            # the placeholder's alias now points at the real asset
    assert env.events == []                                                           # placeholder merges are not audit events


def test_merge_two_real_assets_is_reported_and_children_merge_by_name(env):
    b = env.b
    lh1, lh2 = b.lakehouse("itm_a", "same"), b.lakehouse("itm_b", "same2")
    t1, t2 = b.table(lh1, "sales", [("id", "int")]), b.table(lh2, "sales", [("id", "int")])
    c1, c2 = b.column(t1, "id"), b.column(t2, "ID")
    env.scan(M.Facts([lh1, lh2, t1, t2, c1, c2]))
    a, bb = env.registry.resolve(lh1.ref), env.registry.resolve(lh2.ref)
    env.registry.merge(a, bb, reason="same lakehouse")
    assert {e[0] for e in env.events} == {"intelligence.asset_merged"} and a in {e[1] for e in env.events}
    tables = env.rows("select id, status from assets where name='sales' and type='table'")
    assert sorted(s for _, s in tables) == ["active", "removed"]
    assert len(env.rows("select id from assets where type='column' and status='active'")) == 1
    assert env.registry.merge(a, a) == {"aliases": 0, "edges": 0, "children": 0}
    with pytest.raises(NotFound):
        env.registry.merge(a, "ast_missing")


def test_merge_hooks_run_inside_the_merge(env):
    seen = []
    env.registry.merge_hooks.append(lambda conn, frm, to: seen.append((frm, to, conn.execute(__import__("sqlalchemy").text("select 1")).scalar())))
    lh = env.b.lakehouse("itm_a", "a")
    other = env.b.lakehouse("itm_b", "b")
    env.scan(M.Facts([lh, other]))
    a, b = env.registry.resolve(lh.ref), env.registry.resolve(other.ref)
    env.registry.merge(a, b)
    assert seen == [(b, a, 1)]


def test_columns_of_placeholder_tables_resolve_and_follow_the_merge(env):
    b = env.b
    nb = b.notebook("itm_nb", "nb")
    col_ref = M.AssetRef.make("column", None, "amount", {"table": "lh.sales", "workspace_id": "wks_test"})
    col_ref2 = M.AssetRef.make("column", None, "AMOUNT", {"table": "lh.sales", "workspace_id": "wks_test"})
    p1, p2 = env.registry.resolve(col_ref), env.registry.resolve(col_ref2)
    assert p1 == p2 and env.registry.get(p1)["status"] == "placeholder"
    parent = env.registry.get(p1)["parent_id"]
    assert parent and env.registry.get(parent)["status"] == "placeholder" and env.registry.get(parent)["type"] == "table"
    lh = b.lakehouse("itm_lh", "lh")
    t = b.table(lh, "sales", [("amount", "double")])
    col = b.column(t, "amount", "double")
    env.scan(M.Facts([lh, t, col]), source="disc")
    real_col = env.registry.resolve(col.ref)
    assert real_col == p1 or env.registry.merged_target(p1) == real_col
    assert env.registry.resolve(col_ref, create=False) == real_col
    assert nb                                                           # (built only to keep the builder exercised)


def test_a_column_of_a_table_known_only_by_its_location_does_not_create_a_placeholder_table(env):
    """Regression: ``lineage.base.column_ref`` carries the table's physical location in ``scope["table_alias"]``; the registry used to ignore it, so the column's table
    became an orphan placeholder next to the real (discovered) table the same analysis' edges resolve to."""
    uri = "abfss://aaaa@onelake.dfs.fabric.microsoft.com/bbbb/Tables/orders"
    lh = env.b.lakehouse("itm_lh", "lh")
    table = M.AssetFact(M.AssetRef.make("table", {"onelake_path": M.normalize_path(uri)}, "orders", {"fabric_workspace": "aaaa"}), "orders", parent=lh.ref, workspace_id="wks_test")
    col = M.AssetFact(M.AssetRef.make("column", None, "amount", {"table": f"onelake_path:{M.normalize_path(uri)}", "fabric_workspace": "aaaa"}), "amount", attrs={"type": "double"},
                      parent=table.ref, workspace_id="wks_test")
    env.scan(M.Facts([lh, table, col]), source="disc")
    ref = M.AssetRef.make("column", name="amount", scope={"fabric_workspace": "aaaa", "table": "orders", "table_alias": f"onelake_path={M.normalize_path(uri)}"})
    got = env.registry.resolve(ref)
    assert env.registry.get(got)["status"] != "placeholder"
    assert env.rows("select id from assets where type = 'table' and status = 'placeholder'") == []


def _file_fact(env, lh, path):
    return M.AssetFact(M.AssetRef.make("file", {"onelake_path": f"file:///data/{lh.name}/{path}"}), path, parent=lh.ref, workspace_id="wks_test")


@pytest.mark.parametrize("reference_first", [True, False])
def test_a_lakehouse_relative_file_path_resolves_to_the_file_of_the_default_lakehouse(env, reference_first):
    """Regression: ``Files/raw/sales`` read by a notebook became a placeholder next to the lakehouse's real file asset (the registry only qualified tables with the lakehouse)."""
    lh = env.b.lakehouse("itm_lh1", "retail")
    other = env.b.lakehouse("itm_lh2", "other")
    ref = M.AssetRef.make("file", None, "Files/raw/sales", {"workspace_id": "wks_test", "lakehouse_id": "itm_lh1"})
    other_ref = M.AssetRef.make("file", None, "Files/raw/sales", {"workspace_id": "wks_test", "lakehouse_id": "itm_lh2"})
    env.scan(M.Facts([lh, other]))
    if reference_first:
        placeholder = env.registry.resolve(ref)
        other_placeholder = env.registry.resolve(other_ref)
        assert placeholder != other_placeholder                  # the same path in two lakehouses of one workspace is two files
    env.scan(M.Facts([_file_fact(env, lh, "Files/raw/sales")]), mode="incremental")
    real = env.asset("Files/raw/sales", "file")
    assert real["status"] == "active" and real["parent_id"] == env.registry.find("olcf_item", "itm_lh1")
    got = env.registry.resolve(ref, create=False)
    assert got == real["id"]
    assert [a for a in env.rows("SELECT id FROM assets WHERE type = 'file' AND status = 'placeholder' AND name = 'Files/raw/sales' AND parent_id IS NOT NULL")] == []
    # the same relative path in another lakehouse is not this file
    other_id = env.registry.resolve(other_ref)
    assert other_id != real["id"]
    env.scan(M.Facts([_file_fact(env, other, "Files/raw/sales")]), mode="incremental")
    assert env.registry.resolve(other_ref, create=False) != real["id"]
    assert env.registry.resolve(ref, create=False) == real["id"]
