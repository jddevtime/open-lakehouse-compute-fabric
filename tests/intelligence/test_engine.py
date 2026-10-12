# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The engine: scans, the analysis cache (etag skip, keep-alive, retirement), failure isolation and bookkeeping."""

from __future__ import annotations

from typing import Any

import pytest

from olcf.core.errors import BadRequest
from olcf.intelligence import model as M
from olcf.intelligence.engine import Engine, gap_hash


class FakeAnalyzer:
    """Stands in for ``olcf.intelligence.lineage.engine`` (the real analyzers are another module's code); produces a read of each table named in the payload."""

    def __init__(self, env, version: str = "1") -> None:
        self.env, self.version, self.calls = env, version, 0
        self.fail: Exception | None = None

    def load(self):
        return self.analyze, self.version

    def analyze(self, defn: M.DefinitionFact) -> M.Facts:
        self.calls += 1
        if self.fail:
            raise self.fail
        b = self.env.b
        reads = [M.AssetRef.make("table", None, n, {"workspace_id": "wks_test"}) for n in defn.payload.get("reads", [])]
        gaps = [M.Gap("dynamic_reference", "fake", defn.name, d, None, {"cell": 1}) for d in defn.payload.get("gaps", [])]
        return M.Facts([M.AssetFact(defn.ref, defn.name, workspace_id="wks_test")], [b.edge(defn.ref, r, "READS_FROM", "derived", location={"cell": i}) for i, r in enumerate(reads)], gaps)


def _defn(env, reads, etag="e1", gaps=()):
    return M.DefinitionFact("notebook", M.AssetRef.make("notebook", {"olcf_item": "itm_nb"}), "etl", {"reads": list(reads), "gaps": list(gaps)}, etag, {"workspace_id": "wks_test"})


@pytest.fixture()
def fake(env):
    f = FakeAnalyzer(env)
    env.engine.analyzer_loader = f.load
    return f


def _analyze(env, defn, *, mode="full", finish="succeeded", source="local", force=False):
    sid = env.engine.begin_scan(source, mode, "manual")
    st = env.engine.analyze_and_apply(defn, scan_id=sid, source_id=source, force=force)
    if finish:
        env.engine.finish_scan(sid, finish, st.as_dict())
    return sid, st


def test_analysis_creates_facts_and_cache(env, fake):
    _, st = _analyze(env, _defn(env, ["a", "b"], gaps=["dyn"]))
    assert st.analyzed == 1 and st.edges_created == 2 and st.gaps == 1 and st.errors == 0
    cache = env.rows("select kind, etag, analyzer_version, edge_count from analysis_cache")
    assert cache == [("notebook", "e1", "1", 2)]
    assert len(env.edges()) == 2 and env.asset("etl")["status"] == "active"


def test_unchanged_etag_is_skipped_and_kept_alive_through_a_full_scan(env, fake):
    _analyze(env, _defn(env, ["a", "b"], gaps=["dyn"]))
    s2, st = _analyze(env, _defn(env, ["a", "b"], gaps=["dyn"]))
    assert fake.calls == 1 and st.analysis_skipped == 1 and st.analyzed == 0
    # a full scan that skipped the analysis must not sweep what the analysis produced
    assert env.asset("etl")["status"] == "active" and {e["status"] for e in env.edges()} == {"active"}
    assert env.rows("select kind, resolved_at from gaps order by kind") == [("dynamic_reference", None), ("unresolved_reference", None), ("unresolved_reference", None)]
    assert {e["last_seen_scan"] for e in env.edges()} == {s2}
    # placeholders created by the analysis are kept too (they were touched)
    assert {r[0] for r in env.rows("select status from assets where type='table'")} == {"placeholder"}


def test_changed_etag_or_version_reanalyses_and_retires_what_is_gone(env, fake):
    _analyze(env, _defn(env, ["a", "b"], gaps=["dyn"]))
    _, st = _analyze(env, _defn(env, ["a"], etag="e2"))
    assert fake.calls == 2 and st.analyzed == 1
    by_dst = {env.rows("select name from assets where id=:i", i=e["dst_id"])[0][0]: e["status"] for e in env.edges()}
    assert by_dst == {"a": "active", "b": "removed"}
    open_ = sorted(r[0] + ":" + (r[1] or "") for r in env.rows("select kind, subject from gaps where resolved_at is null"))
    assert open_ == ["unresolved_reference:a"]          # the dynamic-reference gap and the reference to 'b' are gone with the code that caused them
    assert env.rows("select etag from analysis_cache") == [("e2",)]
    fake.version = "2"
    _, st = _analyze(env, _defn(env, ["a"], etag="e2"))
    assert fake.calls == 3 and st.analyzed == 1                                          # new analyzer version: same etag is analysed again
    _, st = _analyze(env, _defn(env, ["a"], etag="e2"), force=True)
    assert fake.calls == 4
    _, st = _analyze(env, _defn(env, ["a"], etag=None))                                  # no etag: always analysed
    assert fake.calls == 5


def test_analyzer_exception_becomes_a_gap(env, fake):
    fake.fail = RuntimeError("boom")
    _, st = _analyze(env, _defn(env, ["a"]))
    assert st.errors == 1 and st.analyzed == 0
    g = env.rows("select kind, subject from gaps")
    assert g == [("parse_error", "etl")]
    assert env.rows("select 1 from analysis_cache") == []


def test_missing_lineage_package_is_a_not_inspected_gap(env):
    def missing():
        raise ModuleNotFoundError("No module named 'olcf.intelligence.lineage.engine'", name="olcf.intelligence.lineage.engine")

    env.engine.analyzer_loader = missing
    _, st = _analyze(env, _defn(env, ["a"]))
    assert st.errors == 1 and env.rows("select kind from gaps") == [("not_inspected",)]

    def other():
        raise ModuleNotFoundError("No module named 'sqlglot'", name="sqlglot")

    env.engine.analyzer_loader = other
    with pytest.raises(ModuleNotFoundError):
        _analyze(env, _defn(env, ["a"]))


def test_a_bad_fact_does_not_lose_the_rest_of_the_batch(env):
    b = env.b
    lh = b.lakehouse("itm_lh", "lh")
    bad = M.AssetFact(M.AssetRef.make("spaceship", {"olcf_item": "x"}), "bad")
    good = b.table(lh, "good")
    _, st = env.scan(M.Facts([bad, lh, good], [M.EdgeFact(M.AssetRef.make("table", {"nope": "x"}), good.ref, "READS_FROM", [])]))
    assert st.errors == 2 and st.assets_created == 2 and "spaceship" in st.error_samples[0]
    assert env.asset("good")["status"] == "active"


def test_stats_helpers():
    from olcf.intelligence.engine import ApplyStats

    a, b = ApplyStats(assets_created=1), ApplyStats(assets_created=2, errors=1)
    b.error("x")
    a.add(b)
    d = a.as_dict()
    assert d["assets_created"] == 3 and d["errors"] == 2 and d["error_samples"] == ["x"]
    assert gap_hash("s", "k", "sub", "d", {"a": 1}) == gap_hash("s", "k", "sub", "d", {"a": 1}) != gap_hash("s", "k", "sub", "d", {"a": 2})


class _Audit:
    def __init__(self) -> None:
        self.rows: list[tuple[Any, ...]] = []

    def record(self, principal, action, **kw):
        self.rows.append((action, kw.get("resource_id"), kw.get("details")))


class _Events:
    def __init__(self) -> None:
        self.rows: list[tuple[str, dict[str, Any]]] = []

    def publish(self, type_, payload, **kw):
        self.rows.append((type_, payload))


def test_scan_bookkeeping_audit_and_events(env):
    audit, events = _Audit(), _Events()
    eng = Engine(env.db, env.registry, env.graph, audit=audit, events=events)
    sid = eng.begin_scan("fabric1", "full", "manual")
    row = eng.finish_scan(sid, "succeeded", {"x": 1})
    assert [a[0] for a in audit.rows] == ["intelligence.scan_started", "intelligence.scan_finished"]
    assert events.rows[0][0] == "intelligence.scan.finished" and events.rows[0][1]["scan_id"] == sid and events.rows[0][1]["status"] == "succeeded"
    assert row["stats"]["x"] == 1 and "sweep" in row["stats"]
    rt = eng.begin_scan("runtime", "incremental", "runtime")
    eng.finish_scan(rt, "succeeded")
    assert len(audit.rows) == 2 and len(events.rows) == 1                 # the synthetic run-time scan is silent
    # a failing audit never fails the scan
    audit.record = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("audit down"))
    s2 = eng.begin_scan("fabric1", "full", "manual")
    assert eng.finish_scan(s2, "failed", error="x" * 5000)["status"] == "failed"
    assert len(eng.scan_row(s2)["error"]) == 2000
    with pytest.raises(BadRequest):
        eng.begin_scan("", "full", "manual")


def test_runtime_scans_do_not_report_unresolved_references(env):
    b = env.b
    nb = b.notebook("itm_nb", "nb")
    ref = b.named("ghost")
    env.scan(M.Facts([nb], [b.edge(nb, ref)]), source="runtime", mode="incremental")
    assert env.rows("select id from gaps") == []
    env.scan(M.Facts([], [b.edge(nb, ref)]), source="disc", mode="incremental")
    assert [g[0] for g in env.rows("select kind from gaps")] == ["unresolved_reference"]
