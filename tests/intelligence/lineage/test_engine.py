# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Engine entry point: dispatch over every definition kind, unknown kinds, analyzer failures, etag stamping and the process asset guarantee."""

from __future__ import annotations

from typing import Any

import pytest

from olcf.intelligence.lineage import engine
from olcf.intelligence.lineage.engine import ANALYZER_VERSION, ANALYZERS, analyze_definition
from olcf.intelligence.model import DEFINITION_KINDS, AssetRef, DefinitionFact, Facts
from tests.intelligence.lineage.support import (
    edges,
    gap_kinds,
    gaps,
    load_json,
    load_parts,
)

WS = "11111111-1111-4111-8111-111111111111"
CONTEXT: dict[str, Any] = {"item_id": "itm_x", "item_name": "X", "fabric_workspace": WS, "tenant": "t1", "default_lakehouse": {"name": "lh", "id": "itm_lh"}}


def dfn(kind: str, payload: Any, *, ref: AssetRef | None = None, etag: str | None = "e1", **ctx: Any) -> DefinitionFact:
    """A definition fact of ``kind`` with the standard test context."""
    return DefinitionFact(kind=kind, ref=ref or AssetRef.make("notebook", {"olcf_item": "itm_x"}, "X"), name="X", payload=payload, etag=etag, context={**CONTEXT, **ctx})


PAYLOADS: dict[str, Any] = {
    "notebook": lambda: load_json("notebooks", "fabric_medallion.ipynb"),
    "pipeline_olcf": lambda: load_json("pipelines", "olcf_daily_load.json"),
    "pipeline_fabric": lambda: load_json("pipelines", "fabric_sales_load.json"),
    "sql_query": lambda: {"sql": "SELECT a FROM t", "engine": "duckdb"},
    "report_olcf": lambda: {"tiles": [{"id": "a", "title": "A", "sql": "SELECT a FROM t"}]},
    "spark_job_olcf": lambda: {"code": "spark.read.table('t')"},
    "semantic_model": lambda: load_parts("models", "retail"),
    "report_pbi": lambda: load_parts("reports", "pbir_retail"),
    "scanner_result": lambda: load_json("scanner", "scan_result.json"),
}


def test_every_definition_kind_has_an_analyzer_and_a_test_payload() -> None:
    assert set(ANALYZERS) == set(DEFINITION_KINDS) == set(PAYLOADS)


def test_analyzer_version_is_a_string_shared_with_the_evidence_source() -> None:
    assert isinstance(ANALYZER_VERSION, str) and ANALYZER_VERSION
    f = analyze_definition(dfn("sql_query", PAYLOADS["sql_query"]()))
    assert {ev.source for e in f.edges for ev in e.evidence} == {f"sql-analyzer@{ANALYZER_VERSION}"}


@pytest.mark.parametrize("kind", sorted(PAYLOADS))
def test_dispatch_produces_facts_for_every_kind(kind: str) -> None:
    f = analyze_definition(dfn(kind, PAYLOADS[kind]()))
    assert isinstance(f, Facts) and f.assets and f.edges
    assert not any(g.kind == "parse_error" for g in f.gaps), gaps(f)


@pytest.mark.parametrize("kind", sorted(PAYLOADS))
def test_every_edge_evidence_is_stamped_with_the_etag(kind: str) -> None:
    f = analyze_definition(dfn(kind, PAYLOADS[kind](), etag="etag-42"))
    assert f.edges
    assert {ev.ref for e in f.edges for ev in e.evidence} == {"etag-42"}


def test_no_etag_leaves_evidence_refs_alone() -> None:
    f = analyze_definition(dfn("sql_query", PAYLOADS["sql_query"](), etag=None))
    assert {ev.ref for e in f.edges for ev in e.evidence} == {None}


def test_existing_evidence_ref_is_not_overwritten() -> None:
    f = analyze_definition(dfn("scanner_result", PAYLOADS["scanner_result"](), etag="etag-42"))
    refs = {ev.ref for e in f.edges for ev in e.evidence}
    assert refs <= {"etag-42"} | {r for r in refs if r}
    assert None not in refs


@pytest.mark.parametrize("kind", ["nope", "", "NOTEBOOK", "report"])
def test_unknown_kind_is_a_not_inspected_gap(kind: str) -> None:
    f = analyze_definition(dfn(kind, {}))
    assert gaps(f) == [("not_inspected", "X")] and not f.edges and not f.assets
    assert f.gaps[0].source == "lineage-engine" or "lineage" in f.gaps[0].source
    assert "has no lineage analyzer" in f.gaps[0].detail and all(k in f.gaps[0].detail for k in DEFINITION_KINDS)


def test_non_string_kind_does_not_raise() -> None:
    bad = DefinitionFact(kind=None, ref=AssetRef.make("notebook", {"olcf_item": "itm_x"}, "X"), name="X", payload={}, context=dict(CONTEXT))  # type: ignore[arg-type]
    assert gap_kinds(analyze_definition(bad)) == {"not_inspected"}


def test_analyzer_exception_becomes_a_parse_error_naming_the_analyzer(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(payload: Any, ctx: Any) -> Facts:
        raise ValueError("kaboom " + "x" * 500)

    for kind, (name, _) in list(ANALYZERS.items()):
        monkeypatch.setitem(ANALYZERS, kind, (name, boom))
        f = analyze_definition(dfn(kind, {}))
        assert gaps(f) == [("parse_error", "X")], kind
        gap = f.gaps[0]
        assert gap.source == name and name in gap.detail and "ValueError" in gap.detail and "kaboom" in gap.detail
        assert len(gap.detail) < 400
        assert not f.edges


def test_analyzer_names_are_distinct() -> None:
    names = [name for name, _ in ANALYZERS.values()]
    assert len(set(names)) == len(names)
    assert all(n.endswith("-analyzer") for n in names)


def test_process_asset_is_added_when_the_analyzer_did_not_emit_it(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(ANALYZERS, "notebook", ("notebook-analyzer", lambda p, c: Facts()))
    ref = AssetRef.make("notebook", {"olcf_item": "itm_x"}, "X")
    f = analyze_definition(dfn("notebook", {}, ref=ref))
    assert len(f.assets) == 1 and f.assets[0].ref == ref and f.assets[0].name == "X"
    assert f.assets[0].attrs == {"definition_kind": "notebook", "analyzer_version": ANALYZER_VERSION}


def test_process_asset_is_not_duplicated_when_emitted() -> None:
    ref = AssetRef.make("notebook", {"olcf_item": "itm_x"}, "X")
    f = analyze_definition(dfn("notebook", {"cells": []}, ref=ref))
    assert [a.ref.type for a in f.assets] == ["notebook"] and len(f.assets) == 1


def test_the_definition_ref_is_the_process_of_the_analysis() -> None:
    ref = AssetRef.make("notebook", {"olcf_item": "itm_x"}, "Custom name")
    f = analyze_definition(dfn("notebook", {"cells": [{"cell_type": "code", "source": "spark.read.table('a').write.saveAsTable('b')"}]}, ref=ref))
    assert edges(f, "READS_FROM", "WRITES_TO") == {"READS_FROM notebook:Custom name -> table:a [derived]", "WRITES_TO notebook:Custom name -> table:b [derived]"}
    assert {e.src for e in f.edges if e.type in ("READS_FROM", "WRITES_TO")} == {ref}


class TestSqlQueryPayload:
    """The ``sql_query`` kind accepts ``{"sql", "engine", "lakehouse_id"}`` or a bare string."""

    def run(self, payload: Any, **ctx: Any) -> Facts:
        """Analyse a sql_query definition."""
        return analyze_definition(dfn("sql_query", payload, ref=AssetRef.make("sql_query", {"olcf_item": "itm_q"}, "Q"), **{"tenant": None, "fabric_workspace": None, **ctx}))

    def test_object_and_bare_string_agree(self) -> None:
        a = self.run({"sql": "SELECT a FROM t"})
        b = self.run("SELECT a FROM t")
        assert edges(a) == edges(b) == {"READS_FROM sql_query:Q -> table:t [derived]"}

    def test_engine_selects_the_dialect(self) -> None:
        spark = self.run({"sql": "SELECT `a` FROM `lh`.`t`", "engine": "spark"})
        duck = self.run({"sql": "SELECT `a` FROM `lh`.`t`", "engine": "duckdb"})
        assert not spark.gaps and edges(spark) == {"READS_FROM sql_query:Q -> table:lh.t [derived]"}
        assert gap_kinds(duck) == {"parse_error"}

    def test_lakehouse_id_changes_the_scope(self) -> None:
        own = self.run({"sql": "SELECT a FROM t"})
        other = self.run({"sql": "SELECT a FROM t", "lakehouse_id": "itm_other"})
        scope = lambda f: dict(next(e.dst for e in f.edges if e.type == "READS_FROM").scope_map)  # noqa: E731
        assert scope(own) == {"lakehouse": "lh", "lakehouse_id": "itm_lh"}
        assert scope(other) == {"lakehouse_id": "itm_other"}

    def test_malformed_payloads(self) -> None:
        bad_payloads: list[Any] = [None, 5, [], {"engine": "spark"}, {"sql": 5}]
        for bad in bad_payloads:
            f = self.run(bad)
            assert gap_kinds(f) == {"parse_error"} and not f.edges, bad
            assert f.gaps[0].source == "sql-analyzer"


def test_default_lakehouse_and_item_context_reach_the_analyzer() -> None:
    f = analyze_definition(dfn("spark_job_olcf", {"code": "spark.read.table('t')"}, default_lakehouse={"name": "other", "id": "itm_o"}, tenant=None, fabric_workspace=None))
    assert dict(next(iter(f.edges)).dst.scope_map) == {"lakehouse": "other", "lakehouse_id": "itm_o"}


def test_pbi_kinds_use_the_workspace_and_item_of_the_context() -> None:
    guid = "22222222-2222-4222-8222-222222222222"
    f = analyze_definition(dfn("semantic_model", PAYLOADS["semantic_model"](), ref=AssetRef.make("semantic_model", {"pbi": f"{WS}/{guid}"}, "Retail"), item_id=guid))
    table = next(a.ref for a in f.assets if a.ref.type == "model_table")
    assert table.alias_map["pbi"].startswith(f"{WS}/{guid}/")


def test_empty_context_is_accepted() -> None:
    d = DefinitionFact(kind="sql_query", ref=AssetRef.make("sql_query", {"olcf_item": "itm_q"}, "Q"), name="Q", payload="SELECT a FROM t", context={})
    assert edges(analyze_definition(d)) == {"READS_FROM sql_query:Q -> table:t [derived]"}


def test_engine_module_exports() -> None:
    assert set(engine.__all__) == {"ANALYZERS", "ANALYZER_VERSION", "analyze_definition"}
