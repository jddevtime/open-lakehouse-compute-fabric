# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Review findings F09 and F10 (engine side).

F09: the analysis cache key covers everything that decides what lineage a definition resolves to - not only the definition's etag. A workspace whose default lakehouse changes,
a table whose schema changes, a renamed container, a different Fabric mapping or semantic-model binding re-analyses a definition whose text is byte-identical; volatile context
that the analyzers do not read never does.

F10: an analysis that could not read the whole definition (parse error, analyzer exception, analysis cap, an unsupported construct that made known edges vanish) is *incomplete*:
the engine reports ``ApplyStats.incomplete`` / ``blocking_gaps``, keeps the last known edges flagged stale, and retires only after a complete analysis.
"""

from __future__ import annotations

from typing import Any

import pytest

from olcf.intelligence import model as M
from olcf.intelligence.engine import ApplyStats, context_fingerprint, is_blocking_gap, scan_stats
from olcf.intelligence.queries import _edge_out

WS = "wks_test"


class Analyzer:
    """A context-sensitive stand-in for the lineage analyzers: reads each table named in the payload, resolved in the default lakehouse of the context (as the real ones do)."""

    def __init__(self) -> None:
        self.calls = 0
        self.gaps: list[M.Gap] = []
        self.fail: Exception | None = None
        self.drop_edges = False

    def load(self) -> tuple[Any, str]:
        return self.analyze, "1"

    def analyze(self, defn: M.DefinitionFact) -> M.Facts:
        self.calls += 1
        if self.fail:
            raise self.fail
        lh = (defn.context.get("default_lakehouse") or {})
        scope = {"workspace_id": WS, **({"lakehouse_id": lh["id"]} if lh.get("id") else {}), **({"lakehouse": lh["name"]} if lh.get("name") else {})}
        edges = [] if self.drop_edges else [M.EdgeFact(defn.ref, M.AssetRef.make("table", None, n, scope), "READS_FROM", [M.Evidence("static", "fake@1", "derived", location={"cell": i})])
                                           for i, n in enumerate(defn.payload["reads"])]
        return M.Facts([M.AssetFact(defn.ref, defn.name, workspace_id=WS)], edges, list(self.gaps))


@pytest.fixture()
def world(env):
    """Two lakehouses with an ``orders`` table each, a notebook definition and a fake analyzer."""
    b = env.b
    la, lb = b.lakehouse("itm_lha", "lhA"), b.lakehouse("itm_lhb", "lhB")
    ta, tb = b.table(la, "orders"), b.table(lb, "orders")
    env.scan(M.Facts([la, lb, ta, tb, b.table(la, "people")]), source="catalog", mode="incremental")
    an = Analyzer()
    env.engine.analyzer_loader = an.load
    env.an = an
    return env


def defn(reads=("orders",), etag="e1", **ctx: Any) -> M.DefinitionFact:
    context = {"workspace_id": WS, "default_lakehouse": {"id": "itm_lha", "name": "lhA"}, **ctx}
    return M.DefinitionFact("notebook", M.AssetRef.make("notebook", {"olcf_item": "itm_nb"}), "etl", {"reads": list(reads)}, etag, context)


def analyze(env, d, *, source="local", finish="succeeded", mode="full", force=False):
    sid = env.engine.begin_scan(source, mode, "manual")
    st = env.engine.analyze_and_apply(d, scan_id=sid, source_id=source, force=force)
    if finish:
        env.engine.finish_scan(sid, finish, st.as_dict())
    return sid, st


def targets(env, status="active") -> dict[str, str]:
    """``{lakehouse item: edge status}`` of the READS_FROM edges of the notebook (each target table is named by its lakehouse's olcf_item)."""
    out = {}
    for e in env.edges(type="READS_FROM"):
        if e["status"] == status:
            parent = env.rows("select a.name from assets t join assets a on a.id = t.parent_id where t.id = :i", i=e["dst_id"])
            out[parent[0][0] if parent else "?"] = e["status"]
    return out


# ----------------------------------------------------------------------------------------------------------------------------------- F09
def test_unchanged_definition_and_context_is_skipped(world):
    analyze(world, defn())
    _, st = analyze(world, defn())
    assert world.an.calls == 1 and st.analysis_skipped == 1


def test_changing_the_workspace_default_lakehouse_refreshes_the_edges_without_editing_the_query(world):
    analyze(world, defn())
    assert targets(world) == {"lhA": "active"}
    _, st = analyze(world, defn(default_lakehouse={"id": "itm_lhb", "name": "lhB"}))          # same text, same etag, the workspace now defaults to lhB
    assert world.an.calls == 2 and st.analyzed == 1 and st.analysis_skipped == 0 and not st.incomplete
    assert targets(world) == {"lhB": "active"} and targets(world, "removed") == {"lhA": "removed"}


def test_a_renamed_container_a_schema_a_mapping_or_a_model_binding_reanalyse(world):
    analyze(world, defn(schemas={"lha.orders": ["id"]}, semantic_models={"Sales": "g1"}, tenant="t1", fabric_workspace="w1"))
    base = world.an.calls
    steps = [
        {"default_lakehouse": {"id": "itm_lha", "name": "lhA_renamed"}},                         # the container was renamed
        {"schemas": {"lha.orders": ["id", "amount"]}},                                           # a column was added to a table the definition reads
        {"semantic_models": {"Sales": "g2"}},                                                    # the model name now points at another GUID
        {"tenant": "t2"},                                                                        # another tenant ...
        {"fabric_workspace": "w2"},                                                              # ... or another remote workspace is mapped here
        {"dialect": "tsql"},
        {"parameters": {"env": "prod"}},
        {"known_notebooks": {"other": "itm_x"}},
    ]
    ctx: dict[str, Any] = {"schemas": {"lha.orders": ["id"]}, "semantic_models": {"Sales": "g1"}, "tenant": "t1", "fabric_workspace": "w1"}
    for i, change in enumerate(steps, 1):
        ctx = {**ctx, **change}
        _, st = analyze(world, defn(**ctx))
        assert world.an.calls == base + i and st.analyzed == 1, change
        _, again = analyze(world, defn(**ctx))                                                   # and the result is cached again under the new context
        assert world.an.calls == base + i and again.analysis_skipped == 1, change


def test_volatile_context_never_forces_a_reanalysis(world):
    analyze(world, defn(location={"item": "itm_nb"}, scanned_at="2026-01-01T00:00:00Z", trace_id="a"))
    for extra in ({"location": {"item": "itm_nb", "cell": 9}}, {"scanned_at": "2026-02-02T00:00:00Z"}, {"trace_id": "b", "request": {"n": 1}}):
        _, st = analyze(world, defn(**extra))
        assert st.analysis_skipped == 1
    assert world.an.calls == 1


def test_only_the_schemas_of_tables_the_definition_mentions_count(world):
    analyze(world, defn(schemas={"lha.orders": ["id"], "lha.people": ["id", "name"]}))
    _, st = analyze(world, defn(schemas={"lha.orders": ["id"], "lha.people": ["id", "name", "email"], "lha.invoices": ["x"]}))   # nobody here reads people or invoices
    assert st.analysis_skipped == 1 and world.an.calls == 1
    _, st = analyze(world, defn(schemas={"lha.orders": ["id", "amount"], "lha.people": ["id"]}))
    assert st.analyzed == 1 and world.an.calls == 2


def test_the_fingerprint_is_deterministic_and_normalised():
    a = defn(schemas={"A.Orders": ["id"]}, semantic_models={"Sales": "G1"})
    b = defn(semantic_models={"sales": "g1"}, schemas={"a.orders": ["id"]})
    assert context_fingerprint(a, "1") == context_fingerprint(b, "1") != context_fingerprint(a, "2")
    assert context_fingerprint(defn(), "1") != context_fingerprint(defn(etag="e1", dialect="duckdb"), "1")


def test_the_fingerprint_covers_every_context_key_the_analyzers_read():
    from dataclasses import fields

    from olcf.intelligence.engine import LINEAGE_CONTEXT_KEYS
    from olcf.intelligence.lineage.base import LineageContext

    read = {f.name for f in fields(LineageContext)} - {"process", "location"}
    assert read <= set(LINEAGE_CONTEXT_KEYS), f"a context key the analyzers read is missing from the cache fingerprint: {sorted(read - set(LINEAGE_CONTEXT_KEYS))}"


def test_analyses_cached_before_the_fingerprint_existed_are_analysed_once_more(world):
    analyze(world, defn())
    from sqlalchemy import text

    with world.db.begin() as c:
        c.execute(text("update analysis_cache set context_hash = null"))
    _, st = analyze(world, defn())
    assert st.analyzed == 1 and world.an.calls == 2
    _, st = analyze(world, defn())
    assert st.analysis_skipped == 1


def test_migration_3_adds_the_column_to_an_existing_database(tmp_path):
    from sqlalchemy import inspect, text

    from olcf.db.engine import Database
    from olcf.intelligence import schema

    db = Database(f"sqlite:///{tmp_path / 'old.db'}")
    with db.begin() as c:
        c.execute(text("create table analysis_cache (asset_id varchar(32) not null, kind varchar(32) not null, analyzer_version varchar(32) not null, etag varchar(128), "
                       "analyzed_at varchar(32) not null, edge_count integer not null, touched json not null, primary key (asset_id, kind))"))
        c.execute(text("insert into analysis_cache values ('ast_x', 'notebook', '1', 'e', '2026-01-01T00:00:00Z', 0, '{}')"))
        schema._analysis_context_hash(c)
        schema._analysis_context_hash(c)                                                           # idempotent
        assert "context_hash" in {col["name"] for col in inspect(c).get_columns("analysis_cache")}
        assert c.execute(text("select context_hash from analysis_cache")).scalar() is None


# ----------------------------------------------------------------------------------------------------------------------------------- F10
def test_blocking_gap_kinds():
    g = M.Gap
    assert is_blocking_gap(g("parse_error", "sql", "q", "bad"))
    assert is_blocking_gap(g("unsupported_construct", "analysis", "analysis", "more than 200000 edges: the remaining relationships were not recorded"))
    assert is_blocking_gap(g("unsupported_construct", "notebook", "nb", "the notebook has 7000 cells; only the first 6000 were analysed"))
    assert is_blocking_gap(g("unsupported_construct", "sql", "q", "the SQL text is larger than 2000000 characters and was not analysed"))
    assert not is_blocking_gap(g("unsupported_construct", "sql", "q", "PIVOT is not modelled"))
    assert not is_blocking_gap(g("dynamic_reference", "py", "q", "table name built at run time"))
    assert not is_blocking_gap(g("parse_error", "sql", "q", "bad", None, {"informational": True}))


def test_apply_stats_carry_the_incomplete_flag_flat_and_nested():
    a, b = ApplyStats(), ApplyStats(blocking_gaps=2, incomplete=True)
    a.add(b)
    a.add(ApplyStats())
    assert a.blocking_gaps == 2 and a.incomplete is True
    d = scan_stats(a.as_dict())
    assert d["blocking_gaps"] == 2 and d["incomplete"] is True and d["apply"]["incomplete"] is True


def valid_then(world, *, drop=False):
    _, st = analyze(world, defn(("orders", "people")))
    assert st.blocking_gaps == 0 and not st.incomplete and len(targets(world)) == 1
    return {e["id"] for e in world.edges(type="READS_FROM")}


def test_a_parse_error_keeps_the_last_known_lineage_and_reports_partial_data(world):
    before = valid_then(world)
    world.an.gaps = [M.Gap("parse_error", "sql-analyzer", "etl", "syntax error near FROM", "fix the query", {"cell": 1})]
    world.an.drop_edges = True
    sid, st = analyze(world, defn(("orders", "people"), etag="e2"), finish="succeeded")     # even a runner that sweeps anyway cannot lose them
    assert st.incomplete and st.blocking_gaps == 1 and st.analyzed == 1
    now = world.edges(type="READS_FROM")
    assert {e["id"] for e in now} == before and {e["status"] for e in now} == {"active"}
    for e in now:
        stale = e["attrs"]["stale"]
        assert stale["since_scan"] == sid and stale["last_confirmed_scan"] != sid and "parse_error" in stale["reason"]
        assert e["evidence"] and e["last_seen_scan"] == sid
    assert [g[0] for g in world.rows("select kind from gaps where kind = 'parse_error' and resolved_at is null")] == ["parse_error"]
    assert _edge_out(now[0])["stale"] is True


def test_an_incomplete_analysis_is_repeated_by_the_next_scan_even_with_the_same_etag(world):
    valid_then(world)
    world.an.gaps = [M.Gap("parse_error", "sql-analyzer", "etl", "syntax error", None, {"cell": 1})]
    world.an.drop_edges = True
    analyze(world, defn(etag="e2"))
    calls = world.an.calls
    _, st = analyze(world, defn(etag="e2"))
    assert world.an.calls == calls + 1 and st.incomplete and st.analysis_skipped == 0


def test_repairing_the_definition_retires_only_what_really_went(world):
    before = valid_then(world)
    world.an.gaps = [M.Gap("parse_error", "sql-analyzer", "etl", "syntax error", None, {"cell": 1})]
    world.an.drop_edges = True
    analyze(world, defn(("orders", "people"), etag="e2"))
    world.an.gaps, world.an.drop_edges = [], False
    _, st = analyze(world, defn(("orders",), etag="e3"))                                       # the repaired definition no longer reads people
    assert not st.incomplete and st.blocking_gaps == 0
    by_status = {e["id"]: e for e in world.edges(type="READS_FROM")}
    assert set(by_status) == before
    kept = [e for e in by_status.values() if e["status"] == "active"]
    gone = [e for e in by_status.values() if e["status"] == "removed"]
    assert len(kept) == 1 and len(gone) == 1
    assert "stale" not in kept[0]["attrs"]                                                        # observed again: no longer last-known-good only
    assert world.rows("select count(*) from gaps where kind = 'parse_error' and resolved_at is null")[0][0] == 0


def test_an_analyzer_exception_keeps_the_lineage_and_is_blocking(world):
    before = valid_then(world)
    world.an.fail = RuntimeError("boom")
    sid, st = analyze(world, defn(("orders", "people"), etag="e2"))
    assert st.incomplete and st.blocking_gaps == 1 and st.errors == 1
    now = world.edges(type="READS_FROM")
    assert {e["id"] for e in now} == before and all(e["status"] == "active" and e["attrs"]["stale"]["reason"].startswith("the analyzer raised") for e in now)
    assert world.asset("etl")["status"] == "active"
    world.an.fail = None
    _, st = analyze(world, defn(("orders", "people"), etag="e2"))
    assert not st.incomplete and all("stale" not in e["attrs"] for e in world.edges(type="READS_FROM"))


def test_a_cap_hit_is_blocking(world):
    valid_then(world)
    world.an.gaps = [M.Gap("unsupported_construct", "notebook", "analysis", "more than 200000 edges: the remaining relationships were not recorded", None, {})]
    world.an.drop_edges = True
    _, st = analyze(world, defn(etag="e2"))
    assert st.incomplete and st.blocking_gaps == 1
    assert {e["status"] for e in world.edges(type="READS_FROM")} == {"active"}


def test_an_unsupported_construct_blocks_only_when_it_made_known_edges_vanish(world):
    gap = M.Gap("unsupported_construct", "sql", "etl", "PIVOT is not modelled", None, {"cell": 2})
    world.an.gaps = [gap]
    _, first = analyze(world, defn())                                                          # nothing was known before: informational, the scan stays complete
    assert not first.incomplete and first.blocking_gaps == 0
    _, same = analyze(world, defn(etag="e2"))                                                  # the same edges are found again
    assert not same.incomplete
    world.an.drop_edges = True
    _, lost = analyze(world, defn(etag="e3"))                                                  # the construct now hides the edges that used to be found
    assert lost.incomplete and lost.blocking_gaps == 1
    assert {e["status"] for e in world.edges(type="READS_FROM")} == {"active"}


def test_a_gap_free_analysis_that_finds_fewer_edges_retires_them(world):
    valid_then(world)
    world.an.drop_edges = True
    _, st = analyze(world, defn(etag="e2"))
    assert not st.incomplete and {e["status"] for e in world.edges(type="READS_FROM")} == {"removed"}


def test_informational_gaps_never_block(world):
    valid_then(world)
    world.an.gaps = [M.Gap("parse_error", "sql", "etl", "optional hint", None, {"informational": True})]
    world.an.drop_edges = True
    _, st = analyze(world, defn(etag="e2"))
    assert not st.incomplete and st.blocking_gaps == 0
