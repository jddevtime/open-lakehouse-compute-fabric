# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Review finding F04: tenant / workspace / item / table identities are authoritative; a qualified name is only a hint.

Two remote Fabric workspaces mapped to ONE OLCF workspace, each with a lakehouse called ``Silver`` that holds an ``orders`` table, must stay two lakehouses, two tables, two sets
of columns and two lineages. The facts below have exactly the shape of the Fabric connector (``discovery/fabric.py``): the Fabric aliases plus the local workspace and the remote
scope on the reference."""

from __future__ import annotations

from typing import Any

from olcf.intelligence import model as M
from olcf.intelligence.model import AssetFact, AssetRef, fabric_alias

LOCAL = "wks_local"


def fabric_workspace(tenant: str, gid: str, lh_guid: str, lh_name: str = "Silver", tables: dict[str, list[str]] | None = None, notebook: str | None = None,
                     local: str = LOCAL) -> M.Facts:
    """What the Fabric connector emits for one remote workspace: its lakehouse, tables with columns and an optional notebook that reads ``orders`` by name."""
    tables = tables if tables is not None else {"orders": ["id", "amount"]}
    scope = {"fabric_workspace": gid, "tenant": tenant}
    lh_ref = AssetRef.make("lakehouse", {"fabric": fabric_alias(tenant, gid, lh_guid)}, lh_name, scope)
    facts = M.Facts([AssetFact(lh_ref, lh_name, {"source_kind": "fabric"}, workspace_id=local, path=f"{gid} / {lh_name}")])
    for t, cols in tables.items():
        t_alias = f"{fabric_alias(tenant, gid, lh_guid)}/{t.lower()}"
        tref = AssetRef.make("table", {"fabric": t_alias, "delta_location": f"abfss://{gid}@onelake/{lh_guid}/tables/{t}", "onelake_path": f"abfss://{gid}@onelake/{lh_guid}/tables/{t}"}, t,
                             {"fabric_workspace": gid, "lakehouse": lh_name, "tenant": tenant})
        facts.assets.append(AssetFact(tref, t, {"columns": [{"name": c, "type": "string"} for c in cols]}, parent=lh_ref, workspace_id=local))
        for c in cols:
            cref = AssetRef.make("column", {"fabric_column": f"{t_alias}:{c}"}, c, {"fabric_workspace": gid, "table": f"fabric:{t_alias}", "table_type": "table", "tenant": tenant})
            facts.assets.append(AssetFact(cref, c, {"data_type": "string"}, parent=tref, workspace_id=local))
    if notebook:
        nb = AssetRef.make("notebook", {"fabric": fabric_alias(tenant, gid, notebook)}, f"nb_{gid}", scope)
        facts.assets.append(AssetFact(nb, f"nb_{gid}", {}, workspace_id=local))
        # the notebook reads "Silver.orders" by name, in the scope of ITS remote workspace
        read = AssetRef.make("table", None, f"{lh_name}.orders", {"workspace_id": local, "fabric_workspace": gid, "tenant": tenant})
        facts.edges.append(M.EdgeFact(nb, read, "READS_FROM", [M.Evidence("static", "test", "derived", location={"cell": 1})]))
    return facts


def tables_named(env, name: str) -> list[dict[str, Any]]:
    from sqlalchemy import select

    from olcf.intelligence.schema import assets

    with env.db.connect() as c:
        return [dict(r._mapping) for r in c.execute(select(assets).where(assets.c.name == name, assets.c.type == "table", assets.c.status != "removed").order_by(assets.c.id))]


def test_two_remote_workspaces_mapped_to_one_local_workspace_stay_distinct(env):
    env.scan(fabric_workspace("tenant1", "ws-a", "lh-a", notebook="nb-a"), source="src-ws-a")
    env.scan(fabric_workspace("tenant1", "ws-b", "lh-b", notebook="nb-b"), source="src-ws-b")
    orders = tables_named(env, "orders")
    assert len(orders) == 2 and orders[0]["id"] != orders[1]["id"]
    lakehouses = {r["parent_id"] for r in orders}
    assert len(lakehouses) == 2 and None not in lakehouses                                # each table keeps its own lakehouse (the second scan used to re-parent the first)
    for o in orders:
        cols = env.rows("select name from assets where parent_id = :p and type = 'column' order by name", p=o["id"])
        assert [c[0] for c in cols] == ["amount", "id"]
    assert env.rows("select count(*) from assets where type = 'column'")[0][0] == 4
    # lineage: each notebook reads ITS table, selected by the remote scope of the reference
    for gid in ("ws-a", "ws-b"):
        nb = env.asset(f"nb_{gid}", "notebook")["id"]
        read = [e for e in env.edges(src_id=nb, type="READS_FROM")]
        assert len(read) == 1
        target = env.rows("select attrs from assets where id = :i", i=read[0]["dst_id"])
        tr = next(o for o in orders if o["id"] == read[0]["dst_id"])
        assert tr["attrs"].get("columns") and env.rows("select value from asset_aliases where asset_id = :i and kind = 'fabric'", i=tr["id"])[0][0].startswith(f"tenant1/{gid}/")
        assert target
    assert len({e["dst_id"] for e in env.edges(type="READS_FROM")}) == 2
    # the two assets each carry exactly one Fabric identity
    for o in orders:
        assert len(env.rows("select value from asset_aliases where asset_id = :i and kind = 'fabric'", i=o["id"])) == 1


def test_a_reference_that_only_knows_the_local_name_is_ambiguous_not_the_first_table(env):
    env.scan(fabric_workspace("tenant1", "ws-a", "lh-a"), source="src-ws-a")
    env.scan(fabric_workspace("tenant1", "ws-b", "lh-b"), source="src-ws-b")
    by_scope = env.registry.resolve(AssetRef.make("table", None, "Silver.orders", {"workspace_id": LOCAL, "fabric_workspace": "ws-b", "tenant": "tenant1"}), create=False)
    assert env.rows("select value from asset_aliases where asset_id = :i and kind = 'fabric'", i=by_scope)[0][0].startswith("tenant1/ws-b/")
    plain = env.registry.resolve_ex(AssetRef.make("table", None, "Silver.orders", {"workspace_id": LOCAL}), create=True)
    assert plain.placeholder and plain.created                                         # unresolved on purpose: two tables answer to that name
    assert plain.asset_id not in {o["id"] for o in tables_named(env, "orders") if o["status"] != "placeholder"}


def test_the_conflict_is_reported_as_a_gap_and_rescans_are_stable(env):
    env.scan(fabric_workspace("tenant1", "ws-a", "lh-a"), source="src-ws-a")
    env.scan(fabric_workspace("tenant1", "ws-b", "lh-b"), source="src-ws-b")
    first = {o["id"]: o["parent_id"] for o in tables_named(env, "orders")}
    kinds = [r[0] for r in env.rows("select kind from gaps where resolved_at is null")]
    assert "identity_conflict" in kinds
    for _ in range(2):                                                                   # rescanning in either order changes nothing
        env.scan(fabric_workspace("tenant1", "ws-b", "lh-b"), source="src-ws-b")
        env.scan(fabric_workspace("tenant1", "ws-a", "lh-a"), source="src-ws-a")
    assert {o["id"]: o["parent_id"] for o in tables_named(env, "orders")} == first
    # a display-name change of the remote lakehouse keeps every id
    env.scan(fabric_workspace("tenant1", "ws-a", "lh-a", lh_name="Gold"), source="src-ws-a")
    assert {o["id"]: o["parent_id"] for o in tables_named(env, "orders")} == first
    assert env.rows("select count(*) from assets where type = 'lakehouse'")[0][0] == 2


def test_cross_tenant_mappings_stay_distinct(env):
    env.scan(fabric_workspace("tenant1", "ws-a", "lh-a"), source="src-ws-a")
    env.scan(fabric_workspace("tenant2", "ws-c", "lh-c"), source="src-ws-c")
    orders = tables_named(env, "orders")
    assert len(orders) == 2
    tenants = sorted(env.rows("select value from asset_aliases where kind = 'fabric' and asset_id = :i", i=o["id"])[0][0].split("/")[0] for o in orders)
    assert tenants == ["tenant1", "tenant2"]


def test_the_same_remote_table_seen_with_and_without_a_tenant_is_one_asset(env):
    env.scan(fabric_workspace("tenant1", "ws-a", "lh-a"), source="src-ws-a")
    # a local lakehouse that records its Fabric mapping knows the table without the tenant ("*")
    scope = {"fabric_workspace": "ws-a"}
    star = AssetRef.make("table", {"fabric": f"{fabric_alias(None, 'ws-a', 'lh-a')}/orders"}, "orders", scope)
    env.scan(M.Facts([AssetFact(star, "orders", {"note": "from the local mapping"}, workspace_id=LOCAL)]), source="local")
    assert len(tables_named(env, "orders")) == 1


def test_a_local_table_and_its_fabric_twin_still_merge_by_name_when_nothing_conflicts(env):
    b = env.b
    lh = b.lakehouse("itm_lh", "Silver", LOCAL)
    local = b.table(lh, "orders")
    env.scan(M.Facts([lh, local]), source="local")
    env.scan(fabric_workspace("tenant1", "ws-a", "lh-a"), source="src-ws-a")
    # the local lakehouse has an OLCF item id, the Fabric one a Fabric id: different authoritative kinds say nothing against each other, the shared name joins them
    assert len(tables_named(env, "orders")) == 1


def test_identity_conflicts_are_diagnosed_for_assets_conflated_by_older_builds(env):
    env.scan(fabric_workspace("tenant1", "ws-a", "lh-a"), source="src-ws-a")                # one source per remote workspace: a full scan sweeps its own source only
    env.scan(fabric_workspace("tenant1", "ws-b", "lh-b", tables={"payments": ["id"]}), source="src-ws-b")
    a = tables_named(env, "orders")[0]["id"]
    pay = tables_named(env, "payments")[0]
    # reproduce the damage an older build did: a second authoritative identity attached to the first table
    other = f"{fabric_alias('tenant1', 'ws-b', 'lh-b')}/orders"
    env.registry.add_alias(a, "fabric", other)
    rep = env.registry.identity_conflicts()
    assert [r["asset_id"] for r in rep] == [a]
    assert sorted(rep[0]["kinds"]["fabric"]) == sorted(["tenant1/ws-a/lh-a/orders", other.lower()])
    assert rep[0]["name"] == "orders" and rep[0]["type"] == "table" and pay["id"] not in {r["asset_id"] for r in rep}
    sid = env.engine.begin_scan("fab", "incremental", "manual")
    assert env.engine.report_identity_conflicts(sid, "fab") == 1
    assert env.engine.report_identity_conflicts(sid, "fab") == 1                           # idempotent: the same gap, not a second row
    env.engine.finish_scan(sid, "succeeded", {})
    gaps = env.rows("select detail from gaps where kind = 'identity_conflict' and asset_id = :a and resolved_at is null", a=a)
    assert len(gaps) == 1 and "tenant1/ws-b/lh-b/orders" in gaps[0][0]


def test_a_known_tenant_next_to_an_unknown_one_is_not_a_conflict(env):
    env.scan(fabric_workspace("tenant1", "ws-a", "lh-a"), source="src-ws-a")
    a = tables_named(env, "orders")[0]["id"]
    env.registry.add_alias(a, "fabric", "*/ws-a/lh-a/orders")
    assert env.registry.identity_conflicts() == []
