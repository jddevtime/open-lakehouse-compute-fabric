# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``facts_from_event``: platform events -> Intelligence facts. Pure (no database, no app); the SQL analyzer is replaced by a fake that returns known facts."""

from __future__ import annotations

import copy
import importlib.util
import random
import types
from typing import Any

import pytest

from olcf.core import dataset_events as dsev
from olcf.intelligence import model as M
from olcf.intelligence import runtime as R

WS, LH, NB, JOB, RUN = "wks_1", "itm_lh1", "itm_nb1", "job_1", "run_1"
AT = "2026-03-04T05:06:07.000Z"


def proc(kind: str = "notebook", **kw: Any) -> dict[str, Any]:
    return {**dsev.process_descriptor(kind), **kw}


def table(name: str, lh: str | None = LH) -> dict[str, Any]:
    return dsev.dataset_descriptor("table", lakehouse_id=lh, name=name)


def flow(process: dict[str, Any], datasets: list[Any], mode: str | None = None, **extra: Any) -> dict[str, Any]:
    return {"workspace_id": WS, "process": process, "datasets": datasets, "engine": "spark", "mode": mode, "statement_hash": None, "at": AT, **extra}


def aliases(ref: M.AssetRef) -> dict[str, str]:
    return ref.alias_map


# ------------------------------------------------------------------------------------------------- dataset.read / dataset.written
def test_notebook_read_and_write_edges_are_verified_with_runtime_evidence():
    p = proc("notebook", item_id=NB, job_id=JOB, name="Nightly")
    read = R.facts_from_event("dataset.read", flow(p, [table("sales"), table("customers")]))
    assert read is not None and read.assets == [] and read.gaps == []                      # the notebook is an item: discovery owns its asset
    nb = M.AssetRef.make("notebook", {"olcf_item": NB}, name="Nightly", scope={"workspace_id": WS})
    assert [(e.src, e.type, e.confidence) for e in read.edges] == [(nb, "READS_FROM", "verified")] * 2
    sales = M.AssetRef.make("table", {"lakehouse_table": f"{LH}:sales"}, name="sales", scope={"workspace_id": WS, "lakehouse_id": LH})
    assert read.edges[0].dst == sales
    ev = read.edges[0].evidence
    assert len(ev) == 1 and (ev[0].kind, ev[0].source, ev[0].confidence, ev[0].ref, ev[0].observed_at) == ("runtime", "runtime:job", "verified", JOB, AT)
    assert ev[0].location == {"workspace_id": WS, "item_id": NB, "job_id": JOB, "engine": "spark", "process": "Nightly"}
    write = R.facts_from_event("dataset.written", flow(p, [table("gold")], mode="overwrite"))
    assert write is not None and [(e.type, e.attrs, e.confidence) for e in write.edges] == [("WRITES_TO", {"mode": "overwrite"}, "verified")]
    assert write.edges[0].src == nb


def test_duplicate_unnamed_and_malformed_datasets_are_skipped():
    facts = R.facts_from_event("dataset.read", flow(proc(item_id=NB), [table("a"), table("a"), {"kind": "table"}, "junk", None, {"kind": "view", "name": "v"}, 5, table("b")]))
    assert facts is not None and [aliases(e.dst)["lakehouse_table"] for e in facts.edges] == [f"{LH}:a", f"{LH}:b"]
    assert R.facts_from_event("dataset.read", flow(proc(item_id=NB), [])) is None
    assert R.facts_from_event("dataset.read", flow(proc(item_id=NB), [{"kind": "table"}])) is None
    assert R.facts_from_event("dataset.read", {**flow(proc(item_id=NB), [table("a")]), "datasets": "a"}) is None


def test_tables_without_a_known_lakehouse_have_no_alias_and_resolve_by_name():
    facts = R.facts_from_event("dataset.read", flow(proc(item_id=NB), [table("Silver.sales", lh=None)]))
    assert facts is not None
    ref = facts.edges[0].dst
    assert ref.type == "table" and ref.aliases == () and ref.name == "Silver.sales" and ref.scope_map == {"workspace_id": WS}


def test_file_datasets_use_path_aliases():
    ds = [dsev.dataset_descriptor("file", lakehouse_id=LH, path="Files/raw/a.csv", format="csv"),
          dsev.dataset_descriptor("file", uri="abfss://ws@onelake.dfs.fabric.microsoft.com/LH/Files/Raw/", format="parquet"),
          dsev.dataset_descriptor("file", uri="s3://bucket/Key/Part.parquet"),
          dsev.dataset_descriptor("file", path="Files/bare.csv"),
          dsev.dataset_descriptor("file")]
    facts = R.facts_from_event("dataset.written", flow(proc("pipeline", item_id="itm_pl", run_id=RUN, name="p/copy"), ds, mode="append"))
    assert facts is not None and len(facts.edges) == 4
    a, b, c, d = (e.dst for e in facts.edges)
    assert all(x.type == "file" for x in (a, b, c, d))
    assert aliases(a) == {"source": f"olcf-file:{LH}:Files/raw/a.csv"} and a.name == "a.csv"
    assert aliases(b) == {"onelake_path": "abfss://ws@onelake.dfs.fabric.microsoft.com/lh/files/raw"}
    assert aliases(c) == {"delta_location": "s3://bucket/key/part.parquet"}
    assert aliases(d) == {"delta_location": "Files/bare.csv"}
    assert facts.edges[0].evidence[0].source == "runtime:pipeline" and facts.edges[0].evidence[0].ref == RUN


def test_process_identity_variants():
    sha = dsev.statement_hash("SELECT * FROM lh1.sales")
    # ad hoc SQL: the statement is the asset, wherever it ran; the runtime owns (names) it
    adhoc = proc("sql_query", query_id="qry_1")
    f = R.facts_from_event("dataset.read", flow(adhoc, [table("sales")], engine="duckdb", statement_hash=sha))
    assert f is not None and len(f.assets) == 1
    a = f.assets[0]
    assert a.ref == M.AssetRef.make("sql_query", {"query": f"sha1:{sha}"}, name=f"SQL {sha[:10]}", scope={"workspace_id": WS})
    assert a.name == f"SQL {sha[:10]}" and a.attrs == {"engine": "duckdb", "statement_hash": sha, "observed": "runtime"} and a.workspace_id == WS
    assert f.edges[0].src == a.ref and f.edges[0].evidence[0].source == "runtime:sql" and f.edges[0].evidence[0].ref == "qry_1"
    assert f.edges[0].evidence[0].location["statement_hash"] == sha
    # a "sha1:" prefixed hash is the same asset
    g = R.facts_from_event("dataset.read", flow(adhoc, [table("sales")], engine="duckdb", statement_hash="sha1:" + sha.upper()))
    assert g is not None and g.assets[0].ref == a.ref
    # no hash: identified by the query id
    h = R.facts_from_event("dataset.read", flow(adhoc, [table("sales")], engine="duckdb"))
    assert h is not None and aliases(h.assets[0].ref) == {"source": "olcf-query:qry_1"}
    # nothing identifiable: no facts
    assert R.facts_from_event("dataset.read", flow(proc("sql_query"), [table("sales")])) is None
    # saved query / report tile: item-backed, not owned
    q = R.facts_from_event("dataset.read", flow(proc("sql_query", item_id="itm_q", query_id="qry_2"), [table("sales")], statement_hash=sha))
    assert q is not None and q.assets == [] and q.edges[0].src == M.AssetRef.make("sql_query", {"olcf_item": "itm_q"}, name=None, scope={"workspace_id": WS})
    r = R.facts_from_event("dataset.read", flow(proc("report", item_id="itm_r", name="tile:n"), [table("sales")]))
    assert r is not None and r.edges[0].src.type == "report" and aliases(r.edges[0].src) == {"olcf_item": "itm_r"}
    # item-less job: the runtime owns an asset keyed by the job id
    j = R.facts_from_event("dataset.written", flow(proc("job", job_id=JOB, name="nightly-script"), [table("t")], mode="create"))
    assert j is not None and aliases(j.assets[0].ref) == {"source": f"olcf-job:{JOB}"}
    assert j.assets[0].name == "nightly-script" and j.assets[0].ref.type == "spark_job" and j.assets[0].attrs == {"engine": "spark", "observed": "runtime"}
    # regression: an item-less job that ran on the python engine is a script, not a Spark job (same rule as the item-backed ones below)
    py = R.facts_from_event("dataset.written", flow(proc("job", job_id=JOB, name="py-job"), [table("t")], mode="create", engine="python"))
    assert py is not None and py.assets[0].ref.type == "script" and py.assets[0].attrs == {"engine": "python", "observed": "runtime"} and py.edges[0].src.type == "script"
    # a Spark job definition vs a python script item
    spark_job = R.facts_from_event("dataset.read", flow(proc("spark_job", item_id="itm_sj", job_id=JOB), [table("t")]))
    script = R.facts_from_event("dataset.read", flow(proc("job", item_id="itm_sc", job_id=JOB), [table("t")], engine="python"))
    assert spark_job is not None and spark_job.edges[0].src.type == "spark_job" and script is not None and script.edges[0].src.type == "script"
    # a pipeline needs its item
    assert R.facts_from_event("dataset.read", flow(proc("pipeline", run_id=RUN), [table("t")])) is None
    assert R.facts_from_event("dataset.read", {**flow(proc("job"), [table("t")]), "process": "nope"}) is None


def test_mode_is_only_kept_on_written_edges():
    f = R.facts_from_event("dataset.read", flow(proc(item_id=NB), [table("a")], mode="append"))
    assert f is not None and f.edges[0].attrs == {}


# ------------------------------------------------------------------------------------------------- dataset.statement
class FakeAnalyzer:
    """Replaces ``olcf.intelligence.lineage.engine``; records the definitions it was given and returns what the test configured."""

    def __init__(self) -> None:
        self.calls: list[M.DefinitionFact] = []
        self.result = M.Facts()
        self.error: Exception | None = None

    def analyze_definition(self, defn: M.DefinitionFact) -> M.Facts:
        self.calls.append(defn)
        if self.error is not None:
            raise self.error
        return copy.deepcopy(self.result)


@pytest.fixture()
def analyzer(monkeypatch):
    fake = FakeAnalyzer()
    mod = types.ModuleType("olcf.intelligence.lineage.engine")
    mod.analyze_definition = fake.analyze_definition  # type: ignore[attr-defined]
    monkeypatch.setitem(__import__("sys").modules, "olcf.intelligence.lineage.engine", mod)
    return fake


def statement(process: dict[str, Any], sql: str = "INSERT INTO gold SELECT id, amount FROM sales", **extra: Any) -> dict[str, Any]:
    return {"workspace_id": WS, "process": process, "sql": sql, "dialect": "spark", "default_lakehouse_id": LH, "statement_hash": dsev.statement_hash(sql) if isinstance(sql, str) else None, "at": AT, **extra}


def t_ref(name: str) -> M.AssetRef:
    return M.AssetRef.make("table", {"lakehouse_table": f"{LH}:{name}"}, name=name, scope={"workspace_id": WS, "lakehouse_id": LH})


def c_ref(tname: str, col: str) -> M.AssetRef:
    return M.AssetRef.make("column", {"lakehouse_column": f"{LH}:{tname}:{col}"}, name=col, scope={"workspace_id": WS, "lakehouse_id": LH, "table": tname})


def test_statement_edges_are_upgraded_to_runtime_evidence(analyzer):
    sql = "INSERT INTO gold SELECT id, amount FROM sales"
    p = proc("sql_query", query_id="qry_9")
    static = lambda conf, snippet="x": [M.Evidence("code", "sql-analyzer", conf, ref="etag1", location={"line": 1, "col": 8}, snippet=snippet)]  # noqa: E731
    analyzer.result = M.Facts(
        assets=[M.AssetFact(t_ref("sales"), "sales", workspace_id=WS)],
        edges=[M.EdgeFact(t_ref("sales"), t_ref("gold"), "TRANSFORMS", static("derived"), confidence="derived", transformation="SUM(amount)"),
               M.EdgeFact(c_ref("sales", "amount"), c_ref("gold", "amount"), "DERIVES_FROM", static("inferred"), confidence="inferred"),
               M.EdgeFact(c_ref("sales", "id"), c_ref("gold", "id"), "DERIVES_FROM", static("unknown"), confidence="unknown"),
               M.EdgeFact(c_ref("sales", "x"), c_ref("gold", "x"), "DERIVES_FROM", static("verified"), confidence="verified"),
               M.EdgeFact(M.AssetRef.make("sql_query", {"query": "sha1:other"}), t_ref("sales"), "READS_FROM", static("derived"), confidence="derived"),
               M.EdgeFact(M.AssetRef.make("sql_query", {"query": "sha1:other"}), t_ref("gold"), "WRITES_TO", [], confidence=None)],
        gaps=[M.Gap("unresolved_reference", "sql-analyzer", "missing", "table not found")])
    facts = R.facts_from_event("dataset.statement", statement(p, sql))
    assert facts is not None
    # the analyzer saw a SQL definition with the right context
    (defn,) = analyzer.calls
    sha = dsev.statement_hash(sql)
    assert defn.kind == "sql_query" and defn.payload == {"sql": sql, "engine": "spark", "lakehouse_id": LH} and defn.etag is None
    assert defn.ref == M.AssetRef.make("sql_query", {"query": f"sha1:{sha}"}, name=f"SQL {sha[:10]}", scope={"workspace_id": WS})
    assert defn.context["default_lakehouse"] == {"id": LH, "name": None} and defn.context["dialect"] == "spark" and defn.context["workspace_id"] == WS
    assert defn.context["source_id"] == "runtime" and defn.context["item_type"] == "sql_query"
    # the owned process asset first, then what the analyzer found
    assert facts.assets[0].ref == defn.ref and facts.assets[1].ref == t_ref("sales")
    confs = [(e.type, e.confidence) for e in facts.edges]
    assert confs == [("TRANSFORMS", "verified"), ("DERIVES_FROM", "inferred"), ("DERIVES_FROM", "unknown"), ("DERIVES_FROM", "verified"), ("READS_FROM", "verified"), ("WRITES_TO", "verified")]
    first = facts.edges[0]
    assert first.transformation == "SUM(amount)"
    (ev,) = first.evidence
    assert (ev.kind, ev.source, ev.confidence, ev.ref, ev.location, ev.snippet, ev.observed_at) == ("runtime", "runtime:sql", "verified", "qry_9", {"line": 1, "col": 8}, "x", AT)
    assert facts.edges[1].evidence[0].confidence == "inferred" and facts.edges[2].evidence[0].confidence == "unknown"
    last = facts.edges[5].evidence
    assert len(last) == 1 and last[0].kind == "runtime" and last[0].confidence == "verified", "an edge without evidence gets runtime evidence of the statement"
    assert [g.kind for g in facts.gaps] == ["unresolved_reference"]


def test_item_backed_processes_keep_their_discovered_name(analyzer):
    nb_ref = M.AssetRef.make("notebook", {"olcf_item": NB}, name="job-name", scope={"workspace_id": WS})
    analyzer.result = M.Facts(assets=[M.AssetFact(nb_ref, "job-name", workspace_id=WS), M.AssetFact(t_ref("sales"), "sales", workspace_id=WS)],
                              edges=[M.EdgeFact(t_ref("sales"), t_ref("gold"), "TRANSFORMS", [M.Evidence("code", "a", "derived")], confidence="derived")])
    facts = R.facts_from_event("dataset.statement", statement(proc("notebook", item_id=NB, job_id=JOB, name="job-name")))
    assert facts is not None
    assert [a.ref for a in facts.assets] == [t_ref("sales")], "the analyzer's fact for the notebook itself would rename it to the job's name"
    (defn,) = analyzer.calls
    assert defn.context["item_id"] == NB and defn.context["item_type"] == "notebook" and defn.context["item_name"] == "job-name"
    assert facts.edges[0].evidence[0].source == "runtime:job" and facts.edges[0].evidence[0].ref == JOB


def test_statement_dialect_is_validated(analyzer):
    R.facts_from_event("dataset.statement", statement(proc("sql_query", query_id="q"), dialect="duckdb"))
    R.facts_from_event("dataset.statement", statement(proc("sql_query", query_id="q"), dialect="cobol"))
    assert [d.payload["engine"] for d in analyzer.calls] == ["duckdb", "spark"]


def test_analyzer_failure_becomes_a_parse_error_gap(analyzer):
    analyzer.error = RuntimeError("boom " + "x" * 500)
    facts = R.facts_from_event("dataset.statement", statement(proc("sql_query", query_id="qry_3")))
    assert facts is not None and facts.edges == [] and len(facts.assets) == 1
    (gap,) = facts.gaps
    assert gap.kind == "parse_error" and gap.source == "runtime:statement" and gap.location == {"query_id": "qry_3"}
    assert gap.detail.startswith("the statement could not be analysed: RuntimeError: boom") and len(gap.detail) < 300
    assert gap.kind in M.GAP_KINDS


def test_a_missing_analyzer_module_is_also_just_a_gap(monkeypatch):
    import sys

    monkeypatch.setitem(sys.modules, "olcf.intelligence.lineage.engine", None)       # import raises ImportError
    facts = R.facts_from_event("dataset.statement", statement(proc("sql_query", query_id="qry_4")))
    assert facts is not None and [g.kind for g in facts.gaps] == ["parse_error"] and facts.edges == []


@pytest.mark.parametrize("sql", [None, "", "   \n", 5, ["select 1"]])
def test_statements_without_sql_text_give_nothing(analyzer, sql):
    assert R.facts_from_event("dataset.statement", statement(proc("sql_query", query_id="q"), sql=sql)) is None
    assert analyzer.calls == []


def test_statement_with_an_unidentifiable_process_gives_nothing(analyzer):
    assert R.facts_from_event("dataset.statement", statement(proc("pipeline"))) is None
    assert analyzer.calls == []


@pytest.mark.skipif(importlib.util.find_spec("olcf.intelligence.lineage.engine") is None, reason="the lineage engine module is not part of this checkout yet")
def test_real_analyzer_end_to_end():
    facts = R.facts_from_event("dataset.statement", statement(proc("sql_query", query_id="qry_real"), "INSERT INTO gold SELECT id, amount FROM sales"))
    assert facts is not None
    reads = {e.dst for e in facts.edges if e.type == "READS_FROM"}
    writes = {e.dst for e in facts.edges if e.type == "WRITES_TO"}
    assert reads and writes
    assert all(ev.kind == "runtime" for e in facts.edges for ev in e.evidence)
    assert all(e.confidence == "verified" for e in facts.edges if e.type in ("READS_FROM", "WRITES_TO"))


# ------------------------------------------------------------------------------------------------- dataset.schema_changed
def schema_event(**over: Any) -> dict[str, Any]:
    return {"workspace_id": WS, "lakehouse_id": LH, "table": "sales", "version": "7", "at": AT,
            "columns_added": [{"name": "region", "type": "string", "nullable": True}],
            "columns_removed": [{"name": "old", "type": "long", "nullable": False}],
            "columns_changed": [{"name": "amount", "from": "double", "to": "decimal(10,2)", "nullable_from": True, "nullable_to": False}],
            "columns": [{"name": "id", "type": "long", "nullable": True}, {"name": "amount", "type": "decimal(10,2)", "nullable": False}, {"name": "region", "type": "string", "nullable": True}],
            **over}


def test_schema_change_updates_the_table_and_its_column_assets():
    facts = R.facts_from_event("dataset.schema_changed", schema_event())
    assert facts is not None and facts.edges == [] and facts.gaps == []
    tbl, added, changed, removed = facts.assets
    assert tbl.ref == t_ref("sales") and tbl.name == "sales" and tbl.workspace_id == WS
    assert tbl.attrs == {"columns": [{"name": "id", "type": "long", "nullable": True}, {"name": "amount", "type": "decimal(10,2)", "nullable": False},
                                     {"name": "region", "type": "string", "nullable": True}], "schema_changed_at": AT}
    assert tbl.parent == M.AssetRef.make("lakehouse", {"olcf_item": LH}, scope={"workspace_id": WS})
    assert (added.ref, added.attrs, added.parent) == (c_ref("sales", "region"), {"type": "string", "nullable": True}, tbl.ref)
    assert (changed.ref, changed.attrs) == (c_ref("sales", "amount"), {"type": "decimal(10,2)", "nullable": False})
    assert (removed.ref, removed.attrs) == (c_ref("sales", "old"), {"type": "long", "dropped": True})


def test_schema_change_without_the_full_list_or_with_junk_entries():
    facts = R.facts_from_event("dataset.schema_changed", schema_event(columns=None, columns_added=[{"name": ""}, "x", None, {"name": "ok", "type": "int"}], columns_removed=None, columns_changed="no"))
    assert facts is not None and "columns" not in facts.assets[0].attrs
    assert [a.name for a in facts.assets] == ["sales", "ok"]
    big = R.facts_from_event("dataset.schema_changed", schema_event(columns=[{"name": f"c{i}", "type": "int"} for i in range(900)]))
    assert big is not None and len(big.assets[0].attrs["columns"]) == R._MAX_COLUMNS
    assert R.facts_from_event("dataset.schema_changed", schema_event(table=None)) is None
    assert R.facts_from_event("dataset.schema_changed", schema_event(lakehouse_id="")) is None


# ------------------------------------------------------------------------------------------------- pipelines and the rest
@pytest.mark.parametrize("state", ["succeeded", "failed", "cancelled"])
def test_pipeline_run_terminal_states_update_the_pipeline_asset(state):
    f = R.facts_from_event(f"pipeline.run.{state}", {"workspace_id": WS, "run_id": RUN, "pipeline_id": "itm_pl", "pipeline": "Daily load", "trigger": "manual"})
    assert f is not None and f.edges == []
    (a,) = f.assets
    assert a.ref == M.AssetRef.make("pipeline", {"olcf_item": "itm_pl"}, name="Daily load", scope={"workspace_id": WS}) and a.attrs == {"last_run_state": state} and a.name == "Daily load"


@pytest.mark.parametrize(("event", "payload"), [
    ("pipeline.run.started", {"workspace_id": WS, "pipeline_id": "itm_pl", "pipeline": "x"}),
    ("pipeline.run.queued", {"workspace_id": WS, "pipeline_id": "itm_pl", "pipeline": "x"}),
    ("pipeline.run.succeeded", {"workspace_id": WS, "pipeline": "x"}),
    ("pipeline.run.succeeded", {"workspace_id": WS, "pipeline_id": "itm_pl"}),
    ("job.state_changed", {"job_id": JOB, "to": "succeeded", "name": "n", "item_id": NB, "workspace_id": WS}),
    ("workspace.created", {"id": WS}),
    ("dataset.unknown", {}),
    ("", {}),
])
def test_other_events_give_nothing(event, payload):
    assert R.facts_from_event(event, payload) is None


def test_handles_matches_the_events_it_translates():
    for t in R.RUNTIME_EVENT_TYPES:
        assert R.handles(t)
    assert R.handles("pipeline.run.cancelled") and R.handles("pipeline.run.anything")
    assert not R.handles("workspace.created") and not R.handles("dataset.other")


def test_non_dict_payloads_give_nothing():
    for payload in (None, [], "x", 5):
        for t in R.RUNTIME_EVENT_TYPES:
            assert R.facts_from_event(t, payload) is None  # type: ignore[arg-type]


def test_translation_is_deterministic_and_does_not_modify_the_payload():
    payload = flow(proc("notebook", item_id=NB, job_id=JOB, name="n"), [table("a"), dsev.dataset_descriptor("file", lakehouse_id=LH, path="Files/x")], mode="append")
    before = copy.deepcopy(payload)
    a = R.facts_from_event("dataset.written", payload)
    b = R.facts_from_event("dataset.written", payload)
    assert payload == before and a == b


def test_the_translation_module_stays_pure():
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(R))
    imported = {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module} | {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
    assert not {m for m in imported if m.split(".")[0] in ("sqlalchemy", "requests", "httpx", "socket", "time", "datetime", "random", "os", "olcf") and not m.startswith(("olcf.intelligence.model", "olcf.core.dataset_events"))}


# ------------------------------------------------------------------------------------------------- Fabric-originated runs
TENANT = "11111111-1111-4111-8111-111111111111"
FWS = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
FNB = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
FLH = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
FAB = {"tenant_id": TENANT, "workspace_id": FWS, "item_id": FNB, "item_name": "nb_load", "item_type": "Notebook", "run_id": "run-0001"}
FLB = {"workspace_id": FWS, "lakehouse_id": FLH}


def fab_table(name: str) -> dict[str, Any]:
    return dsev.dataset_descriptor("table", lakehouse_id=LH, name=name, fabric=FLB)


def test_a_fabric_notebook_run_is_the_notebook_the_tenant_scan_discovered():
    facts = R.facts_from_event("dataset.read", flow(proc("notebook", job_id=JOB, name="nb_load (Fabric)", fabric=FAB), [fab_table("Orders")]))
    assert facts is not None and facts.assets == [] and facts.gaps == []                    # discovery owns the notebook: the runtime creates no asset
    (edge,) = facts.edges
    assert edge.confidence == "verified" and edge.type == "READS_FROM"
    assert edge.src.type == "notebook" and aliases(edge.src) == {"fabric": M.fabric_alias(TENANT, FWS, FNB)} and edge.src.name == "nb_load"
    assert edge.src.scope_map == {"workspace_id": WS, "fabric_workspace": FWS, "tenant": TENANT}
    ev = edge.evidence[0]
    assert (ev.kind, ev.source, ev.confidence, ev.ref) == ("runtime", "runtime:job", "verified", JOB)
    assert ev.location["fabric_item"] == FNB and ev.location["fabric_workspace"] == FWS and ev.location["fabric_tenant"] == TENANT and ev.location["fabric_run"] == "run-0001"


def test_the_fabric_notebook_never_becomes_a_job_level_asset():
    facts = R.facts_from_event("dataset.written", flow(proc("notebook", job_id=JOB, name="nb", fabric=FAB), [fab_table("silver")], mode="overwrite"))
    assert facts is not None and "olcf-job" not in str(facts) and facts.edges[0].attrs == {"mode": "overwrite"}


def test_a_table_of_a_fabric_lakehouse_carries_the_aliases_the_scan_gives_it():
    facts = R.facts_from_event("dataset.read", flow(proc("notebook", job_id=JOB, name="nb", fabric=FAB), [fab_table("Orders")]))
    assert facts is not None
    dst = facts.edges[0].dst
    assert aliases(dst) == {"fabric": f"{TENANT}/{FWS}/{FLH}/orders", "onelake_path": f"abfss://{FWS}@onelake.dfs.fabric.microsoft.com/{FLH}/tables/orders", "lakehouse_table": f"{LH}:Orders"}
    assert [k for k, _ in dst.aliases][0] == "fabric"                                           # AssetRef sorts its aliases; the tenant scan's own identity is the first looked up
    assert dst.name == "Orders"


def test_without_the_tenant_the_table_still_gets_its_onelake_path_but_no_fabric_alias():
    fab = {k: v for k, v in FAB.items() if k != "tenant_id"}
    facts = R.facts_from_event("dataset.read", flow(proc("notebook", job_id=JOB, name="nb", fabric=fab), [fab_table("orders")]))
    assert facts is not None
    assert set(aliases(facts.edges[0].dst)) == {"onelake_path", "lakehouse_table"}
    assert facts.edges[0].confidence == "inferred" and facts.edges[0].src.alias_map == {}   # no tenant: the alias cannot be built, so the notebook is matched by name only


def test_a_nested_or_dotted_table_name_maps_to_the_onelake_folder():
    ds = dsev.dataset_descriptor("table", lakehouse_id=LH, name="sales.orders", fabric=FLB)
    facts = R.facts_from_event("dataset.read", flow(proc("notebook", job_id=JOB, name="nb", fabric=FAB), [ds]))
    assert facts is not None and aliases(facts.edges[0].dst)["onelake_path"].endswith(f"/{FLH}/tables/sales/orders")


def test_a_file_of_a_fabric_lakehouse_gets_its_onelake_path():
    ds = dsev.dataset_descriptor("file", lakehouse_id=LH, path="Files/raw/a.csv", fabric=FLB)
    facts = R.facts_from_event("dataset.read", flow(proc("notebook", job_id=JOB, name="nb", fabric=FAB), [ds]))
    assert facts is not None
    assert aliases(facts.edges[0].dst) == {"onelake_path": f"abfss://{FWS}@onelake.dfs.fabric.microsoft.com/{FLH}/files/raw/a.csv", "source": f"olcf-file:{LH}:Files/raw/a.csv"}


def test_the_fallback_by_name_is_explicit_and_inferred():
    """No notebook GUID (and no tenant): the notebook is matched by name inside the Fabric workspace's OLCF workspace, and every edge says it was a name match."""
    fab = {"workspace_id": FWS, "item_name": "nb_load", "item_type": "Notebook"}
    facts = R.facts_from_event("dataset.read", flow(proc("notebook", job_id=JOB, name="nb_load (Fabric)", fabric=fab), [table("orders")]))
    assert facts is not None and facts.assets == []
    (edge,) = facts.edges
    assert edge.confidence == "inferred" and edge.src.name == "nb_load" and edge.src.alias_map == {} and edge.src.scope_map["fabric_workspace"] == FWS
    ev = edge.evidence[0]
    assert ev.confidence == "inferred" and ev.location["attribution"] == "name" and ev.location["fabric_workspace"] == FWS


def test_a_fabric_block_that_names_nothing_falls_back_to_the_ordinary_process():
    facts = R.facts_from_event("dataset.read", flow(proc("notebook", job_id=JOB, name="job-name", fabric={"tenant_id": TENANT, "item_type": "Notebook"}), [table("orders")]))
    assert facts is not None and aliases(facts.assets[0].ref) == {"source": f"olcf-job:{JOB}"}      # unchanged behaviour for an event without usable Fabric ids
    plain = R.facts_from_event("dataset.read", flow(proc("notebook", job_id=JOB, name="job-name"), [table("orders")]))
    assert plain is not None and plain.assets[0].ref == facts.assets[0].ref


def test_an_olcf_item_and_a_fabric_notebook_are_one_asset_when_both_are_known():
    facts = R.facts_from_event("dataset.read", flow(proc("notebook", item_id=NB, job_id=JOB, name="nb", fabric=FAB), [fab_table("orders")]))
    assert facts is not None
    assert aliases(facts.edges[0].src) == {"fabric": M.fabric_alias(TENANT, FWS, FNB), "olcf_item": NB}


def test_events_without_fabric_blocks_are_translated_exactly_as_before():
    p = proc("notebook", item_id=NB, job_id=JOB, name="Nightly")
    facts = R.facts_from_event("dataset.read", flow(p, [table("sales")]))
    assert facts is not None and facts.edges[0].dst == M.AssetRef.make("table", {"lakehouse_table": f"{LH}:sales"}, name="sales", scope={"workspace_id": WS, "lakehouse_id": LH})
    assert facts.edges[0].evidence[0].location == {"workspace_id": WS, "item_id": NB, "job_id": JOB, "engine": "spark", "process": "Nightly"}


def test_a_hostile_fabric_block_cannot_inject_aliases_of_other_kinds():
    """The block is reduced to GUIDs and a tenant by ``dsev.fabric_process_block`` / ``fabric_lakehouse_block`` before it is used; junk gives the ordinary process."""
    bad = {"tenant_id": "x/../y", "workspace_id": "../../etc", "item_id": "id; drop", "item_name": "n" * 1000}
    facts = R.facts_from_event("dataset.read", flow(proc("notebook", job_id=JOB, name="nb", fabric=bad), [dsev.dataset_descriptor("table", lakehouse_id=LH, name="t", fabric={"workspace_id": "../", "lakehouse_id": "x"})]))
    assert facts is not None
    assert not [v for e in facts.edges for k, v in (*e.src.aliases, *e.dst.aliases) if k in ("fabric", "onelake_path") and ("drop" in v or "etc" in v or ".." in v)]
    assert "onelake_path" not in aliases(facts.edges[0].dst)


def test_a_statement_of_a_fabric_run_resolves_bare_tables_in_the_fabric_default_lakehouse(analyzer):
    t_bare = M.AssetRef.make("table", {"lakehouse_table": f"{LH}:orders"}, name="orders", scope={"workspace_id": WS, "lakehouse_id": LH})
    analyzer.result = M.Facts(assets=[M.AssetFact(t_bare, "orders", workspace_id=WS)],
                              edges=[M.EdgeFact(t_bare, t_ref("gold"), "TRANSFORMS", [M.Evidence("code", "a", "derived")], confidence="derived")])
    pay = statement(proc("notebook", job_id=JOB, name="nb", fabric=FAB), "INSERT INTO gold SELECT * FROM orders", default_lakehouse_fabric=FLB)
    facts = R.facts_from_event("dataset.statement", pay)
    assert facts is not None
    (defn,) = analyzer.calls
    assert defn.context["tenant"] == TENANT and defn.context["fabric_workspace"] == FWS and defn.context["default_lakehouse"]["fabric_id"] == FLH
    assert facts.assets == [] or all(a.ref.type != "notebook" for a in facts.assets)
    bare = next(a.ref for a in facts.assets if a.ref.name == "orders")                      # the asset fact and the edge name the table the same way: no second asset
    assert aliases(bare).get("fabric") == f"{TENANT}/{FWS}/{FLH}/orders" and facts.edges[0].src == bare
    assert "onelake_path" in aliases(bare) and aliases(bare)["lakehouse_table"] == f"{LH}:orders"
    assert facts.edges[0].confidence == "verified" and facts.edges[0].evidence[-1].confidence == "verified"


# ------------------------------------------------------------------------------------------------- fuzz: junk never raises
KEYS = ["workspace_id", "process", "datasets", "engine", "mode", "statement_hash", "at", "sql", "dialect", "default_lakehouse_id", "lakehouse_id", "table", "columns_added",
        "columns_removed", "columns_changed", "columns", "pipeline_id", "pipeline", "kind", "name", "item_id", "job_id", "query_id", "run_id", "path", "uri"]


def junk(rng: random.Random, depth: int = 0) -> Any:
    """A random JSON-ish value (deterministic for a seeded ``rng``)."""
    choice = rng.randrange(9 if depth < 3 else 5)
    if choice == 0:
        return None
    if choice == 1:
        return rng.choice([True, False])
    if choice == 2:
        return rng.choice([0, -1, 7, 10**12])
    if choice == 3:
        return rng.choice(["", " ", "x", "table", "file", "a.b.c", "sha1:" + "a" * 40, "SELECT * FROM t", "\x00", "é" * 5, "notebook", "pipeline"])
    if choice == 4:
        return rng.choice([1.5, -0.0, 1e300])
    if choice in (5, 6):
        return [junk(rng, depth + 1) for _ in range(rng.randrange(4))]
    return {rng.choice(KEYS): junk(rng, depth + 1) for _ in range(rng.randrange(6))}


def test_arbitrary_payloads_never_raise(analyzer):
    rng = random.Random(20260304)
    events = [*R.RUNTIME_EVENT_TYPES, "pipeline.run.failed", "other"]
    for _ in range(3000):
        payload = {rng.choice(KEYS): junk(rng) for _ in range(rng.randrange(1, 10))}
        result = R.facts_from_event(rng.choice(events), payload)
        assert result is None or isinstance(result, M.Facts)


def test_arbitrary_datasets_never_raise():
    rng = random.Random(7)
    kinds = ["notebook", "sql_query", "pipeline", "job", "spark_job", "report", "weird"]
    for _ in range(2000):
        datasets = [{k: junk(rng) for k in rng.sample(["kind", "lakehouse_id", "name", "path", "uri", "format"], rng.randrange(1, 6))} for _ in range(rng.randrange(5))]
        for d in datasets:
            if rng.random() < 0.6:
                d["kind"] = rng.choice(["table", "file"])
        result = R.facts_from_event("dataset.read", flow(proc(rng.choice(kinds), item_id=NB, job_id=JOB, query_id="q", run_id=RUN), datasets))
        assert result is None or all(e.type == "READS_FROM" for e in result.edges)
