# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Cross-cutting guarantees of the analyzers: determinism, malformed input, hard caps, speed, purity (no I/O, no code execution) and a seeded mutation fuzz.

Every analysis must return :class:`~olcf.intelligence.model.Facts` without raising, whatever the input; broken input shows up as gaps.
"""

from __future__ import annotations

import builtins
import hashlib
import json
import os
import random
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest

from olcf.intelligence.lineage import base, notebook, report_pbi, scanner, semantic_model, tmdl
from olcf.intelligence.lineage import pipeline_common as pcommon
from olcf.intelligence.lineage.base import LineageContext
from olcf.intelligence.lineage.dax import extract_refs
from olcf.intelligence.lineage.engine import ANALYZERS, analyze_definition
from olcf.intelligence.lineage.mcode import analyze_m
from olcf.intelligence.lineage.notebook import analyze_notebook
from olcf.intelligence.lineage.pyspark import analyze_python
from olcf.intelligence.lineage.sql import analyze_sql
from olcf.intelligence.model import (
    ASSET_TYPES,
    CONFIDENCE_LEVELS,
    DEFINITION_KINDS,
    EDGE_TYPES,
    EVIDENCE_KINDS,
    GAP_KINDS,
    AssetRef,
    DefinitionFact,
    Facts,
)
from olcf.sample import notebooks as sample_notebooks
from olcf.sample import reports as sample_reports
from tests.intelligence.lineage.support import (
    CORPUS,
    LH,
    gap_kinds,
    gaps,
    join_chain,
    load_json,
    load_parts,
    load_text,
)

REPO = Path(__file__).resolve().parents[3]
WS = "11111111-1111-4111-8111-111111111111"
CONTEXT: dict[str, Any] = {"item_id": "itm_x", "item_name": "X", "fabric_workspace": WS, "tenant": "t1", "default_lakehouse": dict(LH)}


def definition(kind: str, payload: Any, **ctx: Any) -> DefinitionFact:
    """A definition fact of ``kind`` carrying ``payload``."""
    return DefinitionFact(kind=kind, ref=AssetRef.make("notebook", {"olcf_item": "itm_x"}, "X"), name="X", payload=payload, etag="e1", context={**CONTEXT, **ctx})


def payloads() -> dict[str, Any]:
    """One realistic payload per definition kind (fresh objects, analyzers must not mutate them)."""
    return {
        "notebook": load_json("notebooks", "fabric_medallion.ipynb"),
        "pipeline_olcf": load_json("pipelines", "olcf_daily_load.json"),
        "pipeline_fabric": load_json("pipelines", "fabric_sales_load.json"),
        "sql_query": {"sql": "SELECT a, sum(b) AS s FROM t JOIN u USING (k) GROUP BY a", "engine": "duckdb"},
        "report_olcf": sample_reports.report_definition("itm_lh"),
        "spark_job_olcf": {"language": "python", "code": "df = spark.read.table('a')\ndf.write.saveAsTable('b')", "main_file": "Files/m.py", "args": ["Files/in"]},
        "semantic_model": load_parts("models", "retail"),
        "report_pbi": load_parts("reports", "pbir_retail"),
        "scanner_result": load_json("scanner", "scan_result.json"),
    }


def run_direct(kind: str, payload: Any) -> Facts:
    """Call the analyzer of ``kind`` itself (the engine would turn an escaping exception into a gap, hiding the bug) and check the result."""
    d = definition(kind, payload)
    f = ANALYZERS[kind][1](payload, LineageContext.from_definition(d))
    check_wellformed(f)
    return f


def check_wellformed(f: Facts) -> None:
    """Structural invariants every result must satisfy, whatever the input."""
    assert isinstance(f, Facts)
    for a in f.assets:
        assert isinstance(a.ref, AssetRef) and a.ref.type in ASSET_TYPES, a.ref
        assert isinstance(a.name, str)
    for e in f.edges:
        assert e.type in EDGE_TYPES, e.type
        assert isinstance(e.src, AssetRef) and isinstance(e.dst, AssetRef)
        assert e.src.type in ASSET_TYPES and e.dst.type in ASSET_TYPES
        assert e.effective_confidence() in CONFIDENCE_LEVELS
        assert e.evidence, "an edge without evidence"
        for ev in e.evidence:
            assert ev.kind in EVIDENCE_KINDS and ev.confidence in CONFIDENCE_LEVELS and ev.source
            assert ev.snippet is None or len(ev.snippet) <= 400
    for g in f.gaps:
        assert g.kind in GAP_KINDS, g.kind
        assert isinstance(g.subject, str) and isinstance(g.detail, str) and g.detail


# ================================================================================================================================ determinism
def digest(f: Facts) -> str:
    """Hash of the full content of a result (references, attributes, evidence, gaps, order)."""
    return hashlib.sha256(repr(f).encode("utf-8")).hexdigest()


@pytest.mark.parametrize("kind", sorted(DEFINITION_KINDS))
def test_same_input_gives_identical_output(kind: str) -> None:
    a = analyze_definition(definition(kind, payloads()[kind]))
    b = analyze_definition(definition(kind, payloads()[kind]))
    assert repr(a) == repr(b) and a.edges


@pytest.mark.parametrize("kind", sorted(DEFINITION_KINDS))
def test_analysis_does_not_modify_its_input(kind: str) -> None:
    payload = payloads()[kind]
    before = json.dumps(payload, sort_keys=True, default=str)
    analyze_definition(definition(kind, payload))
    assert json.dumps(payload, sort_keys=True, default=str) == before


def test_sample_notebooks_are_deterministic_per_cell_order() -> None:
    for name, nb in sample_notebooks.notebooks().items():
        one = analyze_notebook(nb, context=dict(CONTEXT, item_name=name))
        two = analyze_notebook(json.loads(json.dumps(nb)), context=dict(CONTEXT, item_name=name))
        assert repr(one) == repr(two), name


DIGEST_SCRIPT = """
import hashlib, json, sys
sys.path.insert(0, {repo!r})
from tests.intelligence.lineage.test_robustness import payloads, definition, digest
from olcf.intelligence.lineage.engine import analyze_definition
out = {{}}
for kind, payload in sorted(payloads().items()):
    out[kind] = digest(analyze_definition(definition(kind, payload)))
print(json.dumps(out, sort_keys=True))
"""


def test_output_does_not_depend_on_the_hash_seed() -> None:
    """Sets and dicts are never iterated in hash order: two interpreters with different ``PYTHONHASHSEED`` produce byte-identical facts."""
    results = []
    for seed in ("1", "424242"):
        env = {**os.environ, "PYTHONHASHSEED": seed}
        proc = subprocess.run([sys.executable, "-I", "-c", DIGEST_SCRIPT.format(repo=str(REPO))], capture_output=True, text=True, env=env, check=True, timeout=300, cwd=str(REPO))  # noqa: S603
        results.append(json.loads(proc.stdout.strip().splitlines()[-1]))
    assert results[0] == results[1]
    assert set(results[0]) == set(DEFINITION_KINDS)


# ================================================================================================================================ malformed input
BAD_INPUTS: list[Any] = [
    None, 0, 1.5, True, "", " ", "{", "}", "[]", "null", '{"cells": 5}', "\x00\x01\x02", "\ud800 lone surrogate", b"\xff\xfe\x00", b"", [], [1, 2, 3], [None], {}, {"a": None},
    {"cells": 5}, {"cells": [None, 1, "x", {}, []]}, {"activities": 5}, {"activities": [None, 5, "x"]}, {"tiles": [None]}, {"tables": "x"}, {"model": []},
    {"code": 5, "args": 5}, {"sql": None}, {"definition/model.tmdl": None}, {"definition/model.tmdl": 5, "definition.pbir": []}, {"value": [{"workspaces": 5}]}, {"workspaces": [None, 1]},
]


def case_id(text: str) -> str:
    """A short, stable test id for a (possibly huge or unprintable) input text."""
    return f"len{len(text)}-" + hashlib.md5(text.encode("utf-8", "surrogatepass"), usedforsecurity=False).hexdigest()[:6]


def deep(n: int, leaf: Any = 1) -> Any:
    """A dict nested ``n`` levels deep (iteratively built)."""
    out = leaf
    for _ in range(n):
        out = {"a": out, "activities": [out], "cells": [out], "tiles": [out], "children": out}
    return out


@pytest.mark.parametrize("kind", sorted(DEFINITION_KINDS))
def test_malformed_payloads_never_raise_and_are_wellformed(kind: str) -> None:
    for bad in BAD_INPUTS:
        check_wellformed(analyze_definition(definition(kind, bad)))
        run_direct(kind, bad)


@pytest.mark.parametrize("kind", sorted(DEFINITION_KINDS))
def test_non_container_payloads_are_reported_as_gaps_not_silence(kind: str) -> None:
    for bad in (None, 0, 1.5, True, b"\xff\xfe\x00", [1, 2, 3], "\x00\x01\x02"):
        f = analyze_definition(definition(kind, bad))
        assert f.gaps and not f.edges, (kind, bad)


@pytest.mark.parametrize("kind", sorted(DEFINITION_KINDS))
def test_deeply_nested_payloads_do_not_overflow_the_stack(kind: str) -> None:
    for depth in (50, 400):
        check_wellformed(analyze_definition(definition(kind, deep(depth))))
        run_direct(kind, deep(depth))


def test_deeply_nested_json_text_is_a_gap() -> None:
    text = "[" * 100_000 + "]" * 100_000
    for kind in ("notebook", "semantic_model", "report_pbi"):
        f = analyze_definition(definition(kind, text))
        check_wellformed(f)
        assert f.gaps


def test_odd_contexts_are_accepted() -> None:
    odd: list[Any] = [None, {}, {"item_id": 5}, {"default_lakehouse": "x"}, {"default_lakehouse": {"name": 5}}, {"parameters": 5, "known_notebooks": "x"}, {"semantic_models": []},
                      {"fabric_workspace": "not-a-guid", "tenant": 5}, {"location": "x"}]
    for ctx in odd:
        for kind, payload in payloads().items():
            f = analyze_definition(DefinitionFact(kind=kind, ref=AssetRef.make("notebook", {"olcf_item": "itm_x"}, "X"), name="X", payload=payload, context=ctx or {}))
            check_wellformed(f)
    nb = payloads()["notebook"]
    for ctx in odd:
        check_wellformed(analyze_notebook(nb, context=ctx))
        check_wellformed(analyze_python("spark.read.table('t')", context=ctx))
        check_wellformed(analyze_sql("select 1 from t", dialect="spark", context=ctx))


@pytest.mark.parametrize("text", ["", "(", "SELECT", "SELECT * FROM", "SELECT 1 +", "WITH a AS (", "INSERT INTO", "MERGE INTO t USING", "SELECT '", "SELECT \"", "/* unclosed",
                                  "-- only a comment", "SELECT " + "(" * 5000, "SELECT " + "a," * 20_000 + "a FROM t", "SELECT * FROM " + "(SELECT 1 FROM t) AS x JOIN " * 300 + "(SELECT 2) y",
                                  "SELECT \x00 FROM t", "\ud800", "SELECT 'é' FROM `ü`", "CREATE TABLE t (", "UPDATE t SET", "DELETE FROM", "SELECT 1; " * 6000,
                                  join_chain("JOIN", 59), join_chain("LEFT JOIN", 59, " (SELECT 1)")],
                         ids=case_id)
@pytest.mark.parametrize("dialect", ["spark", "duckdb", "tsql"])
def test_hostile_sql_never_raises(text: str, dialect: str) -> None:
    check_wellformed(analyze_sql(text, dialect=dialect, context={"default_lakehouse": dict(LH)}))


@pytest.mark.parametrize("text", ["", "(", "def f(:", "x = ", "spark.read.table(", "for i in range(", "'''unclosed", "\tindent", "if x:\n  y\n z", "lambda:" * 3000, "x = " + "(" * 3000 + "1" + ")" * 3000,
                                  "x = " + "[" * 2000 + "]" * 2000, "a" * 100_000, "\x00", "\ud800", "spark.read.table('t')\n" * 50_000, "f'{" * 500, "def f():\n" + " return f()\n" * 10, "class A(A): pass",
                                  "def f(): return f()\nf()", "x = x\n", "while True:\n    spark.read.table('t')\n"], ids=case_id)
def test_hostile_python_never_raises(text: str) -> None:
    check_wellformed(analyze_python(text, context={"default_lakehouse": dict(LH)}))


@pytest.mark.parametrize("text", ["", "let", "let in", "let a = in a", "let a = (", "let a = \"unclosed in a", "//", "/*", "[", "{", "let a = " + "(" * 3000 + "1" + ")" * 3000 + " in a",
                                  "let " + ", ".join(f"s{i} = s{i + 1}" for i in range(5000)) + ", s5000 = 1 in s0", "x" * 200_000, "\x00", "\ud800", "#\"", "let a = #table(1, 2) in a",
                                  "section S; shared a = 1;", "let Source = Sql.Database(\"s\", \"d\") in Source{[Schema=\"dbo\",Item=\"t\"]}[Data] meta"],
                         ids=case_id)
def test_hostile_m_never_raises(text: str) -> None:
    res = analyze_m(text, ctx=LineageContext())
    assert res.sources is not None


@pytest.mark.parametrize("text", ["", "(", "SUM(", "'T'[", "[", "VAR x = ", "RETURN", "\"unclosed", "'unclosed", "SUM(" * 3000 + ")" * 3000, "x" * 600_000, "\x00", "\ud800", "T[" * 5000], ids=case_id)
def test_hostile_dax_never_raises(text: str) -> None:
    refs = extract_refs(text)
    assert refs is not None


def test_hostile_tmdl_never_raises() -> None:
    cases = ["", "table", "table T\n\tcolumn", "\t\t\t\tcolumn x", "table 'unclosed\n", "expression E = ```\nunterminated", "measure M = " + "(" * 5000, "\x00", "\ud800",
             "table T\n" + "\tcolumn c\n" * 50_000, "relationship\n\tfromColumn: a\n", "createOrReplace\n\tdatabase D\n"]
    for text in cases:
        check_wellformed(semantic_model.analyze_semantic_model({"definition/model.tmdl": text, "definition/tables/T.tmdl": text}, context=CONTEXT))


# ================================================================================================================================ caps
def many_reads(n: int) -> dict[str, Any]:
    """A notebook with ``n`` single-statement cells, each reading its own table."""
    return {"cells": [{"cell_type": "code", "source": f"spark.read.table('t{i}')"} for i in range(n)], "metadata": {}, "nbformat": 4}


def test_edge_cap_stops_recording_and_says_so(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(base, "MAX_EDGES", 5)
    f = analyze_notebook(many_reads(40), context=CONTEXT)
    assert len([e for e in f.edges if e.type == "READS_FROM"]) == 5
    assert ("unsupported_construct", "analysis") in gaps(f) and "more than 5 edges" in f.gaps[-1].detail or any("more than 5 edges" in g.detail for g in f.gaps)


def test_gap_cap_keeps_a_marker_instead_of_dropping_silently(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(base, "MAX_GAPS", 4)
    nb = {"cells": [{"cell_type": "code", "source": f"spark.read.table(name_{i})"} for i in range(30)], "metadata": {}, "nbformat": 4}
    f = analyze_notebook(nb, context=CONTEXT)
    assert len(f.gaps) == 5
    assert f.gaps[-1].kind == "unsupported_construct" and "more than 4 distinct gaps" in f.gaps[-1].detail


def test_oversized_json_text_is_a_gap_for_every_json_text_analyzer(monkeypatch: pytest.MonkeyPatch) -> None:
    for module in (notebook, semantic_model, report_pbi):
        monkeypatch.setattr(module, "MAX_JSON_BYTES", 50)
    text = json.dumps(payloads()["notebook"])
    f = analyze_notebook(text, context=CONTEXT)
    assert gaps(f) == [("unsupported_construct", "notebook")] and "larger than 50 bytes" in f.gaps[0].detail
    g = semantic_model.analyze_semantic_model(text, context=CONTEXT)
    assert gap_kinds(g) == {"parse_error"} and "larger than 50 bytes" in g.gaps[0].detail
    h = report_pbi.analyze_report_pbi(text, context=CONTEXT)
    assert gap_kinds(h) == {"unsupported_construct"} and "larger than 50 bytes" in h.gaps[0].detail
    assert not (f.edges or g.edges or h.edges)


def test_part_cap_for_models_and_reports(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(semantic_model, "MAX_PARTS", 3)
    monkeypatch.setattr(report_pbi, "MAX_PARTS", 3)
    model = semantic_model.analyze_semantic_model(load_parts("models", "retail"), context=CONTEXT)
    assert any("more than 3 parts" in g.detail for g in model.gaps)
    rep = report_pbi.analyze_report_pbi(load_parts("reports", "pbir_retail"), context=CONTEXT)
    assert any("more than 3 parts" in g.detail for g in rep.gaps)
    full = semantic_model.analyze_semantic_model(load_parts("models", "retail"), context=CONTEXT)
    assert full.gaps == model.gaps  # deterministic


def test_scanner_workspace_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(scanner, "MAX_PARTS", 1)
    doc = load_json("scanner", "scan_result.json")
    second = json.loads(json.dumps(doc["workspaces"][0]))
    second["id"] = "99999999-9999-4999-8999-999999999999"
    doc["workspaces"].append(second)
    f = scanner.analyze_scanner_result(doc, context={"tenant": "t1"})
    assert any("more than 1 workspaces" in g.detail for g in f.gaps)


def test_pipeline_activity_and_nesting_caps(monkeypatch: pytest.MonkeyPatch) -> None:
    from tests.intelligence.lineage.test_pipelines import act, olcf, one

    monkeypatch.setattr(pcommon, "MAX_ACTIVITIES", 3)
    f = olcf(one(*[act(f"a{i}", "notebook", notebook="x") for i in range(10)]))
    assert any("more than 3 activities" in g.detail for g in f.gaps)
    monkeypatch.setattr(pcommon, "MAX_ACTIVITIES", 5000)
    inner = act("leaf", "notebook", notebook="x")
    for i in range(15):
        inner = {"name": f"c{i}", "type": "ForEach", "typeProperties": {"activities": [inner]}}
    g = olcf(one(inner))
    assert any("nested deeper than" in x.detail for x in g.gaps)


def test_m_and_tmdl_and_dax_caps(monkeypatch: pytest.MonkeyPatch) -> None:
    from olcf.intelligence.lineage import dax, mcode

    monkeypatch.setattr(mcode, "MAX_M_CHARS", 20)
    r = analyze_m('let Source = Sql.Database("s", "d") in Source', ctx=LineageContext())
    assert r.problems and "longer than 20 characters" in r.problems[0] and not r.sources
    monkeypatch.setattr(mcode, "MAX_M_CHARS", 1_000_000)
    monkeypatch.setattr(mcode, "MAX_M_STEPS", 5)
    chain = "let\n" + ",\n".join(f"s{i} = " + ('Sql.Database("s","d")' if i == 0 else f"s{i - 1}") for i in range(30)) + "\nin s29"
    r2 = analyze_m(chain, ctx=LineageContext())
    assert r2.problems and not r2.sources
    monkeypatch.setattr(tmdl, "MAX_TMDL_BYTES", 10)
    m = semantic_model.analyze_semantic_model({"definition/tables/T.tmdl": "table T\n  column a\n"}, context=CONTEXT)
    assert m.gaps and "larger than 10 bytes" in " ".join(g.detail for g in m.gaps)
    monkeypatch.setattr(dax, "MAX_DAX_CHARS", 10)
    assert extract_refs("SUM('Sales'[Amount]) + 1").problems


def test_python_step_and_size_caps_are_gaps(monkeypatch: pytest.MonkeyPatch) -> None:
    from olcf.intelligence.lineage import pyspark

    monkeypatch.setattr(pyspark, "MAX_STEPS", 300)
    src = "for i in range(150):\n    for j in range(150):\n        x = i + j\nspark.read.table('t')\n"
    f = analyze_python(src, context=CONTEXT)
    assert any(g.kind == "unsupported_construct" for g in f.gaps)


def test_sql_statement_and_size_caps_are_gaps(monkeypatch: pytest.MonkeyPatch) -> None:
    from olcf.intelligence.lineage import sql

    monkeypatch.setattr(sql, "MAX_STATEMENTS", 3)
    f = analyze_sql("SELECT a FROM t1; SELECT a FROM t2; SELECT a FROM t3; SELECT a FROM t4; SELECT a FROM t5", dialect="spark", context=CONTEXT)
    assert len([e for e in f.edges if e.type == "READS_FROM"]) == 3
    assert any("more than 3 statements" in g.detail for g in f.gaps)


# ================================================================================================================================ performance
def sql_call(tail: str) -> str:
    """A ``spark.sql`` call over ``SELECT a, b FROM <tail>`` (fixture text, never executed)."""
    return f"spark.sql('SELECT a, b FROM {tail}')"  # noqa: S608


def insert_stmt(i: int) -> str:
    """One INSERT ... SELECT with a join, writing ``gold_<i % 50>``."""
    return f"INSERT INTO gold_{i % 50} SELECT a, sum(b) AS b FROM silver_{i % 80} s JOIN dim_{i % 9} d ON d.k = s.k GROUP BY a;"  # noqa: S608


def big_notebook(cells: int, target_bytes: int) -> dict[str, Any]:
    """A realistic-looking notebook of ``cells`` code cells totalling roughly ``target_bytes`` of source (reads, writes, SQL, pandas, helper functions, comments)."""
    per_cell = target_bytes // cells
    out: list[dict[str, Any]] = []
    for i in range(cells):
        body = [
            f"df_{i} = spark.read.table('silver_{i % 40}')",
            f"agg_{i} = df_{i}.groupBy('k').agg(F.sum('v').alias('total'), F.count('*').alias('n'))",
            f"agg_{i}.write.mode('overwrite').saveAsTable('gold_{i % 25}')" if i % 3 == 0 else f"print(agg_{i}.count())",
        ]
        if i % 5 == 0:
            body.append(sql_call(f"gold_{i % 25} g JOIN dim_{i % 7} d ON d.id = g.id WHERE a > {i}"))
        text = "\n".join(body) + "\n"
        pad = max(0, per_cell - len(text))
        text += "".join(f"# {'padding comment line ' * 3}{j}\n" for j in range(pad // 70 + 1))
        out.append({"cell_type": "code", "source": text, "metadata": {}, "outputs": [], "execution_count": None})
    return {"cells": out, "metadata": {"kernelspec": {"name": "synapse_pyspark"}}, "nbformat": 4, "nbformat_minor": 5}


def test_five_megabyte_two_thousand_cell_notebook_is_analysed_in_seconds() -> None:
    nb = big_notebook(2000, 5_000_000)
    size = sum(len(c["source"]) for c in nb["cells"])
    assert size >= 4_500_000
    t0 = time.perf_counter()
    f = analyze_notebook(nb, context=CONTEXT)
    elapsed = time.perf_counter() - t0
    assert elapsed < 15, f"analysis took {elapsed:.1f}s"
    check_wellformed(f)
    reads = {e.dst.name for e in f.edges if e.type == "READS_FROM"}
    writes = {e.dst.name for e in f.edges if e.type == "WRITES_TO"}
    assert {f"silver_{i}" for i in range(40)} <= reads and {f"gold_{i}" for i in range(25)} <= writes
    assert not [g for g in f.gaps if g.kind == "parse_error"]


def test_five_megabyte_notebook_as_json_text_through_the_engine() -> None:
    nb = big_notebook(2000, 5_000_000)
    text = json.dumps(nb)
    assert len(text) > 5_000_000
    t0 = time.perf_counter()
    f = analyze_definition(definition("notebook", text))
    assert time.perf_counter() - t0 < 20
    assert f.edges


def test_large_sql_script_is_fast() -> None:
    script = "\n".join(insert_stmt(i) for i in range(3000))
    t0 = time.perf_counter()
    f = analyze_sql(script, dialect="spark", context=CONTEXT)
    elapsed = time.perf_counter() - t0
    assert elapsed < 30, f"{elapsed:.1f}s"
    check_wellformed(f)
    assert len({e.dst.name for e in f.edges if e.type == "WRITES_TO"}) == 50


def test_long_m_step_chain_and_wide_model_are_fast() -> None:
    chain = "let\n" + ",\n".join(f"s{i} = " + ('Sql.Database("srv", "db"){[Schema="dbo",Item="t"]}[Data]' if i == 0 else f"Table.AddColumn(s{i - 1}, \"c{i}\", each [a] + {i})") for i in range(1200)) + "\nin s1199"
    t0 = time.perf_counter()
    r = analyze_m(chain, ctx=LineageContext())
    assert time.perf_counter() - t0 < 10
    assert r.sources
    tables = "".join(f"table T{i}\n" + "".join(f"\tcolumn c{j}\n\t\tdataType: string\n\t\tsourceColumn: c{j}\n" for j in range(40)) + "\n" for i in range(150))
    t1 = time.perf_counter()
    f = semantic_model.analyze_semantic_model({"definition/model.tmdl": "model M\n", "definition/tables/all.tmdl": tables}, context=CONTEXT)
    assert time.perf_counter() - t1 < 10
    assert len([a for a in f.assets if a.ref.type == "model_table"]) == 150


# ================================================================================================================================ purity
class Forbidden(Exception):
    """Raised by the patched I/O entry points."""


def forbid(*_a: Any, **_k: Any) -> Any:
    """Stand-in for an I/O call that analysis must never make."""
    raise Forbidden("analysis attempted I/O or code execution")


def test_analysis_performs_no_io_and_executes_nothing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    marker = tmp_path / "executed"
    hostile_nb = {
        "cells": [
            {"cell_type": "code", "source": f"import os\nos.system('touch {marker}')\nexec(\"open('{marker}', 'w').write('x')\")\n__import__('subprocess').run(['touch', '{marker}'])\n"
                                            f"open('{marker}', 'w').write('x')\nspark.read.table('safe')\n"},
            {"cell_type": "code", "source": f"%run /etc/passwd\n!touch {marker}\n%sh touch {marker}\n"},
            {"cell_type": "code", "source": f"import subprocess\nsubprocess.run('touch {marker}', shell=True)\n" + sql_call("t")},
        ],
        "metadata": {}, "nbformat": 4,
    }
    work = {kind: payload for kind, payload in payloads().items()}
    work["notebook"] = hostile_nb
    # warm up lazily imported code so only analysis-time I/O is seen, then forbid everything
    expected = {kind: repr(analyze_definition(definition(kind, payload))) for kind, payload in work.items()}
    for sample in sample_notebooks.notebooks().values():
        analyze_notebook(sample, context=CONTEXT)
    real_open = builtins.open
    monkeypatch.setattr(builtins, "open", forbid)
    monkeypatch.setattr("io.open", forbid)
    monkeypatch.setattr(os, "system", forbid)
    monkeypatch.setattr(os, "popen", forbid)
    monkeypatch.setattr(os, "listdir", forbid)
    monkeypatch.setattr(os, "scandir", forbid)
    monkeypatch.setattr(subprocess, "Popen", forbid)
    monkeypatch.setattr(socket, "socket", forbid)
    monkeypatch.setattr(socket, "create_connection", forbid)
    monkeypatch.setattr(socket, "getaddrinfo", forbid)
    monkeypatch.setattr(Path, "open", forbid)
    monkeypatch.setattr(Path, "read_text", forbid)
    monkeypatch.setattr(Path, "read_bytes", forbid)
    monkeypatch.setattr(Path, "write_text", forbid)
    monkeypatch.setattr(Path, "iterdir", forbid)
    got = {kind: repr(analyze_definition(definition(kind, payload))) for kind, payload in work.items()}
    monkeypatch.setattr(builtins, "open", real_open)
    assert got == expected
    assert not marker.exists()
    # the hostile notebook still produced its (static) lineage
    assert "safe" in got["notebook"]


def test_analyzer_sources_import_no_io_modules_and_call_no_dynamic_code_builtins() -> None:
    """Static guard (AST, not text): the analyzer package imports nothing that can reach the network, processes or files and never calls ``eval`` / ``exec`` / ``open`` / ``compile``."""
    import ast

    forbidden_modules = {"socket", "subprocess", "requests", "httpx", "urllib3", "http", "ftplib", "smtplib", "sqlite3", "duckdb", "os", "shutil", "tempfile", "glob", "pickle", "marshal",
                         "importlib", "ctypes", "multiprocessing", "asyncio", "threading"}
    forbidden_calls = {"eval", "exec", "open", "compile", "__import__", "input"}
    seen = 0
    for path in sorted((REPO / "src" / "olcf" / "intelligence" / "lineage").glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        seen += 1
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert alias.name.split(".")[0] not in forbidden_modules, f"{path.name} imports {alias.name}"
            elif isinstance(node, ast.ImportFrom) and node.module:
                top = node.module.split(".")[0]
                assert top not in forbidden_modules, f"{path.name} imports from {node.module}"
                assert not (node.module == "urllib.request" or node.module.startswith("urllib.request")), f"{path.name} imports urllib.request"
            elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                assert node.func.id not in forbidden_calls, f"{path.name} calls {node.func.id}() at line {node.lineno}"
    assert seen >= 18


# ================================================================================================================================ mutation fuzz
VOCAB = ["(", ")", "[", "]", "{", "}", '"', "'", "`", "\n", "\t", " ", ";", ",", ".", "=", "==", "*", "/*", "*/", "--", "//", "#", "\\", "\x00", "\u00e9", "\u202e", "\ud800",
         "let", "in", "each", "SELECT", "FROM", "JOIN", "WITH", "AS", "UNION", "def ", "lambda", "import ", "return", "null", "true", "{}", "[]", "[[", "((", "%run", "%%sql", "!", "$", "{{", "f'", '"""']


def mutate_text(text: str, rng: random.Random) -> str:
    """Apply 1-4 random edits (delete, insert token, duplicate, swap, truncate, replace character) to ``text``."""
    for _ in range(rng.randint(1, 4)):
        if not text:
            text = rng.choice(VOCAB)
            continue
        op = rng.randrange(7)
        i = rng.randrange(len(text))
        j = min(len(text), i + rng.randint(1, 40))
        if op == 0:
            text = text[:i] + text[j:]
        elif op == 1:
            text = text[:i] + rng.choice(VOCAB) + text[i:]
        elif op == 2:
            text = text[:j] + text[i:j] + text[j:]
        elif op == 3:
            k = rng.randrange(len(text))
            a, b = sorted((i, k))
            text = text[:a] + text[b:b + 20] + text[a + 20:b] + text[a:a + 20] + text[b + 20:]
        elif op == 4:
            text = text[: rng.randint(0, len(text))]
        elif op == 5:
            text = text[:i] + rng.choice(VOCAB) + text[i + 1:]
        else:
            text = text[:i] + text[i:j].upper() + text[j:]
    return text


def mutate_tree(obj: Any, rng: random.Random, depth: int = 0) -> Any:
    """Replace a random node of a JSON-like tree by an unexpected value (wrong type, null, empty, huge) or mutate a string leaf; returns a mutated copy."""
    if isinstance(obj, dict) and obj:
        out = dict(obj)
        key = rng.choice(sorted(out, key=str))
        roll = rng.random()
        if roll < 0.35 or depth > 6:
            out[key] = rng.choice([None, 5, "", "x", [], {}, [None], True, 1.5, {"a": [1]}, "\x00", -1, 10**30])
        elif roll < 0.45:
            del out[key]
        else:
            out[key] = mutate_tree(out[key], rng, depth + 1)
        return out
    if isinstance(obj, list) and obj:
        out_l = list(obj)
        i = rng.randrange(len(out_l))
        if rng.random() < 0.3 or depth > 6:
            out_l[i] = rng.choice([None, 5, "", [], {}, "x"])
        elif rng.random() < 0.2:
            del out_l[i]
        else:
            out_l[i] = mutate_tree(out_l[i], rng, depth + 1)
        return out_l
    if isinstance(obj, str):
        return mutate_text(obj, rng)
    return rng.choice([None, "x", 0, obj])


FUZZ_ROUNDS = 40


def test_fuzz_json_and_text_payloads_never_raise() -> None:
    """Structural and textual mutations of every kind's realistic payload: the engine always returns well-formed facts."""
    rng = random.Random(20260101)
    for kind, payload in sorted(payloads().items()):
        for _ in range(FUZZ_ROUNDS):
            mutated = mutate_tree(payload, rng)
            run_direct(kind, mutated)
        text = json.dumps(payload) if not isinstance(payload, str) else payload
        for _ in range(FUZZ_ROUNDS // 2):
            run_direct(kind, mutate_text(text, rng))


def test_fuzz_source_languages_never_raise() -> None:
    rng = random.Random(7)
    etl = "\n".join("".join(c["source"]) if isinstance(c["source"], list) else c["source"] for c in sample_notebooks.etl_notebook()["cells"] if c["cell_type"] == "code")
    sqls = [q["definition"]["sql"] for q in sample_reports.saved_queries("itm_lh")]
    m_files = sorted((CORPUS / "mcode").glob("*.m"))[:25]
    dax_exprs = ["CALCULATE(SUM('Sales'[Amount]), FILTER(ALL('Date'), 'Date'[Year] = 2025))", "VAR x = [Total] RETURN DIVIDE(x, COUNTROWS('Sales'))", "SUMX(RELATEDTABLE('Sales'), [Amount] * 2)"]
    for _ in range(FUZZ_ROUNDS * 2):
        check_wellformed(analyze_python(mutate_text(etl, rng), context=CONTEXT))
        check_wellformed(analyze_sql(mutate_text(rng.choice(sqls), rng), dialect=rng.choice(["spark", "duckdb", "tsql"]), context=CONTEXT))
        analyze_m(mutate_text(rng.choice(m_files).read_text(encoding="utf-8"), rng), ctx=LineageContext())
        extract_refs(mutate_text(rng.choice(dax_exprs), rng))


def test_fuzz_model_and_report_parts_never_raise() -> None:
    rng = random.Random(99)
    model = load_parts("models", "retail")
    report = load_parts("reports", "pbir_retail")
    legacy = load_parts("reports", "legacy_retail")
    for _ in range(FUZZ_ROUNDS):
        for parts, kind in ((model, "semantic_model"), (report, "report_pbi"), (legacy, "report_pbi")):
            mutated = dict(parts)
            for _ in range(rng.randint(1, 3)):
                key = rng.choice(sorted(mutated))
                roll = rng.random()
                if roll < 0.15:
                    del mutated[key]
                elif roll < 0.25:
                    mutated[key + "x"] = mutated[key]
                else:
                    mutated[key] = mutate_text(mutated[key], rng)
            run_direct(kind, mutated)


def test_fuzz_is_reproducible() -> None:
    """The fuzzer itself is deterministic (same seed, same mutations), so a failure can always be replayed."""
    a = [mutate_text(load_text("mcode", sorted(p.name for p in (CORPUS / "mcode").glob("*.m"))[3]), random.Random(5)) for _ in range(2)]
    assert a[0] == a[1]
