# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Notebook analyzer: cell dispatch, magics, cross-cell state, parameters, default lakehouse, malformed documents, and the OLCF compatibility corpus notebooks."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from olcf.intelligence.lineage.base import MAX_CELLS, PyContext
from olcf.intelligence.lineage.notebook import (
    analyze_notebook,
    cell_language,
    cell_source,
    split_cell_magic,
    strip_line_magics,
)
from olcf.intelligence.model import Facts
from tests.intelligence.lineage.support import (
    LH,
    NB_PROCESS,
    asset_labels,
    edges,
    gap_kinds,
    gap_text,
    gaps,
    load_json,
)

CASES = Path(__file__).resolve().parents[2] / "corpus" / "cases"


def code(src: str, **meta: Any) -> dict[str, Any]:
    """A code cell."""
    return {"cell_type": "code", "metadata": meta, "source": src}


def run(*cells: Any, meta: dict[str, Any] | None = None, **ctx: Any) -> Facts:
    """Analyse a notebook made of the given cells (strings become code cells) with the test lakehouse as default."""
    doc = {"cells": [c if isinstance(c, dict) else code(c) for c in cells], "metadata": meta or {}}
    return analyze_notebook(doc, context=PyContext(default_lakehouse=dict(LH), process=NB_PROCESS, **ctx))


# ------------------------------------------------------------------------------------------------------------------------ cell helpers
def test_cell_source_joins_line_lists_and_strings() -> None:
    assert cell_source({"source": ["a\n", "b"]}) == "a\nb"
    assert cell_source({"source": "x = 1"}) == "x = 1"
    assert cell_source({}) == ""


def test_split_cell_magic_keeps_the_body_and_lowercases_the_name() -> None:
    assert split_cell_magic("%%SQL -o res\nselect 1") == ("sql", "-o res", "select 1")
    assert split_cell_magic("x = 1") == (None, "", "x = 1")


def test_cell_language_reads_fabric_cell_metadata() -> None:
    assert cell_language({"metadata": {"microsoft": {"language": "sparksql"}}}) == "sparksql"
    assert cell_language({"metadata": {}}) in ("python", "")


def test_strip_line_magics_keeps_line_numbers_and_blanks_shell_lines() -> None:
    src = "%pip install x\nimport os\nfiles = !ls /tmp\nz = (1\n    % 2)\ny = f'{z!r}'\n!echo hi\n"
    out, found = strip_line_magics(src)
    assert out.count("\n") == src.count("\n")
    assert [m.name for m in found] == ["pip", "!", "!"]
    assert [m.line for m in found] == [1, 3, 7]
    assert "% 2" in out and "{z!r}" in out
    compile(out, "<cell>", "exec")


def test_strip_line_magics_keeps_the_expression_of_time_magics() -> None:
    out, _ = strip_line_magics("%timeit -n 1 -r2 spark.table('t').count()\n%time spark.table('u')\n")
    assert out.splitlines() == ["spark.table('t').count()", "spark.table('u')"]


# ---------------------------------------------------------------------------------------------------------------------------- cross-cell
def test_variables_functions_and_temp_views_are_shared_between_cells() -> None:
    f = run(
        "t = 'orders'\ndef load(name):\n    return spark.table(name)\n",
        "df = load(t)\ndf.createOrReplaceTempView('v_orders')\n",
        "spark.sql('SELECT order_id FROM v_orders').write.saveAsTable('out')\n",
    )
    assert edges(f, "READS_FROM", "WRITES_TO") == {"READS_FROM notebook:nb -> table:orders [derived]", "WRITES_TO notebook:nb -> table:out [derived]"}


def test_a_later_cell_sees_the_schema_learned_from_an_earlier_ddl() -> None:
    f = run("%%sql\nCREATE TABLE silver.t (id INT, v STRING)", "%%sql\nINSERT INTO gold.t2 SELECT * FROM silver.t")
    assert edges(f, "TRANSFORMS") == {"TRANSFORMS col:silver.t.id -> col:gold.t2.id [inferred]", "TRANSFORMS col:silver.t.v -> col:gold.t2.v [inferred]"}  # target schema unknown: same names assumed


def test_evidence_locations_name_the_cell_and_line() -> None:
    f = run("x = 1\n\n\n\ny = spark.table('far')", {"cell_type": "raw", "source": "spark.table('raw')"}, {"cell_type": "markdown", "source": "spark.table('md')"})
    assert edges(f) == {"READS_FROM notebook:nb -> table:far [derived]"}
    assert f.edges[0].evidence[0].location == {"cell": 0, "line": 5}


def test_markdown_raw_cells_comments_and_strings_create_no_lineage() -> None:
    f = run({"cell_type": "markdown", "source": "select * from md_table"}, "# spark.table('commented')\ns = 'spark.table(\"in_string\")'\n")
    assert not f.edges and not f.gaps


# --------------------------------------------------------------------------------------------------------------------------------- magics
def test_sql_cell_magic_is_analysed_as_sql() -> None:
    f = run("%%sql\nINSERT INTO a SELECT x FROM b", "%%sparksql\nSELECT y FROM c")
    assert edges(f, "READS_FROM", "WRITES_TO") == {"READS_FROM notebook:nb -> table:b [derived]", "WRITES_TO notebook:nb -> table:a [derived]", "READS_FROM notebook:nb -> table:c [derived]"}


def test_sql_cell_with_fabric_language_metadata_is_sql() -> None:
    f = run(code("select * from t2", microsoft={"language": "sparksql"}))
    assert edges(f, "READS_FROM") == {"READS_FROM notebook:nb -> table:t2 [derived]"}


def test_multi_statement_sql_cell_chains_statements() -> None:
    f = run("%%sql\nCREATE TABLE x AS SELECT a FROM y;\nINSERT INTO z SELECT a FROM x")
    assert edges(f, "WRITES_TO") == {"WRITES_TO notebook:nb -> table:x [derived]", "WRITES_TO notebook:nb -> table:z [derived]"}
    assert "TRANSFORMS col:y.a -> col:x.a [derived]" in edges(f, "TRANSFORMS")


def test_python_cell_magics_are_analysed_as_python() -> None:
    f = run("%%pyspark\nspark.table('a')", "%%time\nspark.table('b')", "%time spark.table('c')")
    assert edges(f) == {f"READS_FROM notebook:nb -> table:{t} [derived]" for t in "abc"}


def test_shell_cells_and_bang_lines_are_skipped_without_gaps() -> None:
    f = run("%%bash\nls /tmp", "%pip install x\n!ls\nspark.table('a')", "%%html\n<b>x</b>")
    assert edges(f) == {"READS_FROM notebook:nb -> table:a [derived]"}
    assert not f.gaps


def test_unsupported_languages_give_a_gap_and_no_edges() -> None:
    f = run("%%scala\nspark.table(\"ignored\")", code("val x = 1", microsoft={"language": "scala"}), "%%sparkr\nx <- 1")
    assert not f.edges
    assert gaps(f) == [("unsupported_construct", "cell 0"), ("unsupported_construct", "cell 1"), ("unsupported_construct", "cell 2")]
    assert "Scala" in gap_text(f) and "SparkR" in gap_text(f)


def test_unknown_cell_magic_is_a_gap_not_a_guess() -> None:
    f = run("%%weird\nspark.table('x')")
    assert not f.edges
    assert gaps(f) == [("unsupported_construct", "cell 0")]
    assert "%%weird" in gap_text(f)


@pytest.mark.parametrize(
    ("cell", "target"),
    [
        ("%run helper", "helper"),
        ("%run ./Folder/helper", "Folder/helper"),
        ("%run 'My Notebook' {'a': 1}", "My Notebook"),
        ("%run -b nb2", "nb2"),
        ("notebookutils.notebook.run('child', 60)", "child"),
        ("mssparkutils.notebook.run('child2')", "child2"),
        ("n = 'dyn' + str(1)\nnotebookutils.notebook.run(n)", "dyn1"),
    ],
)
def test_notebook_calls_become_depends_on_edges(cell: str, target: str) -> None:
    f = run(cell)
    assert edges(f) == {f"DEPENDS_ON notebook:nb -> notebook:{target} [derived]"}


def test_run_multiple_lists_every_notebook() -> None:
    f = run("notebookutils.notebook.runMultiple({'activities': [{'name': 'a', 'path': 'nbA'}, {'name': 'b', 'path': 'nbB'}]})")
    assert edges(f) == {"DEPENDS_ON notebook:nb -> notebook:nbA [derived]", "DEPENDS_ON notebook:nb -> notebook:nbB [derived]"}


@pytest.mark.parametrize("cell", ["%run $nb", "%run {nb}", "%run"])
def test_dynamic_run_targets_are_gaps(cell: str) -> None:
    f = run(cell)
    assert not f.edges
    assert gap_kinds(f) == {"dynamic_reference"}


def test_known_notebooks_resolve_the_called_item() -> None:
    f = run("%run helper", known_notebooks={"helper": "itm_helper"})
    (e,) = f.edges
    assert e.dst.alias_map == {"olcf_item": "itm_helper"}


# ---------------------------------------------------------------------------------------------------------------------------- parameters
def test_names_from_a_parameters_cell_are_inferred_everything_else_derived() -> None:
    f = run(code("src = 'orders'", tags=["parameters"]), "spark.table(src).write.saveAsTable('fixed')\nspark.table('literal').write.saveAsTable('fixed2')")
    assert edges(f, "READS_FROM", "WRITES_TO") == {
        "READS_FROM notebook:nb -> table:orders [inferred]",
        "WRITES_TO notebook:nb -> table:fixed [derived]",
        "READS_FROM notebook:nb -> table:literal [derived]",
        "WRITES_TO notebook:nb -> table:fixed2 [derived]",
    }


def test_context_parameters_override_the_cell_defaults() -> None:
    f = run(code("src = 'orders'", tags=["parameters"]), "spark.table(src)", parameters={"src": "customers"})
    assert edges(f) == {"READS_FROM notebook:nb -> table:customers [inferred]"}


def test_edges_below_a_parameterised_table_are_inferred_too() -> None:
    f = run(code("src_table = 'orders'", tags=["parameters"]), "spark.table(src_table).select('id').write.saveAsTable('out')")
    assert edges(f, "TRANSFORMS") == {"TRANSFORMS col:orders.id -> col:out.id [inferred]"}


# ----------------------------------------------------------------------------------------------------------------------- default lakehouse
def lakehouse_scope(f: Facts) -> dict[str, str]:
    """Scope of the first edge's target."""
    return dict(f.edges[0].dst.scope)


def test_configure_replaces_the_default_lakehouse_completely() -> None:
    f = run('%%configure\n{"defaultLakehouse": {"name": "other", "id": "11111111-2222-4333-8444-555555555555", "workspaceId": "66666666-7777-4888-8999-000000000000"}}', "spark.table('t1')")
    assert lakehouse_scope(f) == {"lakehouse": "other", "fabric_lakehouse": "11111111-2222-4333-8444-555555555555", "fabric_workspace": "66666666-7777-4888-8999-000000000000"}


def test_configure_after_other_cells_is_a_gap() -> None:
    f = run("spark.table('a')", '%%configure\n{"defaultLakehouse": {"name": "other"}}')
    assert gaps(f) == [("unsupported_construct", "cell 1")]


@pytest.mark.parametrize("body", ["{bad json", '{"defaultLakehouse": 5}'])
def test_invalid_configure_is_a_parse_error(body: str) -> None:
    f = run("%%configure\n" + body, "spark.table('a')")
    assert gap_kinds(f) == {"parse_error"}
    assert edges(f) == {"READS_FROM notebook:nb -> table:a [derived]"}


def test_fabric_metadata_sets_the_default_lakehouse_when_the_context_has_none() -> None:
    doc = load_json("notebooks", "fabric_medallion.ipynb")
    f = analyze_notebook(doc, context={"item_id": "11111111-1111-4111-8111-111111111111", "item_name": "Medallion"})
    scope = dict(f.edges[0].dst.scope)
    assert scope == {"lakehouse": "silver", "fabric_lakehouse": "aaaaaaaa-0000-4000-8000-000000000001", "fabric_workspace": "bbbbbbbb-0000-4000-8000-000000000002"}


def test_context_default_lakehouse_wins_over_notebook_metadata() -> None:
    meta = {"dependencies": {"lakehouse": {"default_lakehouse": "aaaaaaaa-0000-4000-8000-000000000001", "default_lakehouse_name": "meta_lh",
                                           "default_lakehouse_workspace_id": "bbbbbbbb-0000-4000-8000-000000000002"}}}
    f = run("spark.table('t1')", meta=meta)
    assert lakehouse_scope(f) == {"lakehouse": "lh", "lakehouse_id": "itm_lh"}


# --------------------------------------------------------------------------------------------------------------------------- malformed
def test_syntax_error_in_one_cell_does_not_stop_the_others() -> None:
    f = run("def f(:\n  pass", "spark.table('ok').write.saveAsTable('okout')")
    assert ("parse_error", "cell 0") in gaps(f)
    assert edges(f, "READS_FROM", "WRITES_TO") == {"READS_FROM notebook:nb -> table:ok [derived]", "WRITES_TO notebook:nb -> table:okout [derived]"}
    assert next(g for g in f.gaps if g.kind == "parse_error").location == {"cell": 0, "line": 1}


@pytest.mark.parametrize("doc", ["not json", "[]", "{}", {"cells": 5}, None, 7])
def test_non_notebook_documents_give_one_parse_error(doc: Any) -> None:
    f = analyze_notebook(doc, context=PyContext(process=NB_PROCESS))
    assert gap_kinds(f) == {"parse_error"} and not f.edges


def test_notebook_given_as_a_json_string_is_parsed() -> None:
    f = analyze_notebook(json.dumps({"cells": [code("spark.table('a')")]}), context=PyContext(default_lakehouse=dict(LH), process=NB_PROCESS))
    assert edges(f) == {"READS_FROM notebook:nb -> table:a [derived]"}


def test_legacy_worksheets_layout_is_read() -> None:
    doc = {"worksheets": [{"cells": [{"cell_type": "code", "source": ["spark.table('legacy')"]}]}]}
    f = analyze_notebook(doc, context=PyContext(default_lakehouse=dict(LH), process=NB_PROCESS))
    assert edges(f) == {"READS_FROM notebook:nb -> table:legacy [derived]"}


def test_notebook_asset_is_created_when_no_process_is_given() -> None:
    f = analyze_notebook({"cells": []}, context={"item_id": "itm_x", "item_name": "My NB"})
    assert asset_labels(f, "notebook") == {"notebook:My NB"}
    (a,) = f.assets
    assert a.ref.alias_map == {"olcf_item": "itm_x"} and a.attrs["cells"] == 0


def test_too_many_cells_are_cut_with_a_gap() -> None:
    cells = [code(f"spark.table('t{i}')") for i in range(MAX_CELLS + 5)]
    f = analyze_notebook({"cells": cells}, context=PyContext(default_lakehouse=dict(LH), process=NB_PROCESS))
    assert gaps(f, "unsupported_construct") == [("unsupported_construct", "notebook")]
    assert len(edges(f, "READS_FROM")) == MAX_CELLS
    assert "READS_FROM notebook:nb -> table:t0 [derived]" in edges(f)


# ---------------------------------------------------------------------------------------------------------------------------- fixtures
def test_fabric_medallion_notebook_exact_lineage() -> None:
    doc = load_json("notebooks", "fabric_medallion.ipynb")
    f = analyze_notebook(doc, context={"item_id": "11111111-1111-4111-8111-111111111111", "item_name": "Medallion", "fabric_workspace": "bbbbbbbb-0000-4000-8000-000000000002"})
    assert edges(f) == {
        "DEPENDS_ON notebook:Medallion -> notebook:Shared/helpers [derived]",
        "DEPENDS_ON notebook:Medallion -> notebook:Validate Orders [derived]",
        "READS_FROM notebook:Medallion -> table:bronze.raw_orders [inferred]",
        "READS_FROM notebook:Medallion -> table:silver.orders_clean [derived]",
        "TRANSFORMS col:bronze.raw_orders.customer_id -> col:silver.orders_clean.customer_id [inferred]",
        "TRANSFORMS col:bronze.raw_orders.order_id -> col:silver.orders_clean.order_id [inferred]",
        "TRANSFORMS col:bronze.raw_orders.price -> col:silver.orders_clean.revenue [inferred]",
        "TRANSFORMS col:bronze.raw_orders.qty -> col:silver.orders_clean.revenue [inferred]",
        "TRANSFORMS col:silver.orders_clean.customer_id -> col:gold.customer_revenue.customer_id [inferred]",
        "TRANSFORMS col:silver.orders_clean.revenue -> col:gold.customer_revenue.revenue [inferred]",
        "WRITES_TO notebook:Medallion -> table:gold.customer_revenue [derived]",
        "WRITES_TO notebook:Medallion -> table:silver.orders_clean [derived]",
    }
    assert gaps(f) == [("unsupported_construct", "cell 6")]
    assert all(e.evidence[0].location["item"] == "11111111-1111-4111-8111-111111111111" for e in f.edges)


#: the exact READS_FROM / WRITES_TO / DEPENDS_ON edges and gaps of the OLCF compatibility corpus notebooks
CORPUS_EXPECTED: dict[str, tuple[set[str], list[tuple[str, str]]]] = {
    "delta_merge": ({"READS_FROM notebook:nb -> file:Files/orders.csv [derived]", "READS_FROM notebook:nb -> table:order_status [derived]",
                     "WRITES_TO notebook:nb -> table:order_status [derived]"}, []),
    "etl_orders": ({"READS_FROM notebook:nb -> file:Files/orders.csv [inferred]", "READS_FROM notebook:nb -> file:Files/out/orders_clean [inferred]",
                    "WRITES_TO notebook:nb -> file:Files/out/orders_clean [inferred]"}, []),
    "joins": ({"READS_FROM notebook:nb -> file:Files/customers.csv [derived]", "READS_FROM notebook:nb -> file:Files/orders.csv [derived]"}, [("unresolved_reference", "country")]),
    "magics": ({"DEPENDS_ON notebook:nb -> notebook:helpers [derived]"}, []),
    "multi_lakehouse": ({"READS_FROM notebook:nb -> file:Files/curated/totals.json [derived]", "READS_FROM notebook:nb -> file:Files/orders.csv [derived]",
                         "WRITES_TO notebook:nb -> file:Files/curated/totals.json [derived]"}, []),
    "parameters": (set(), []),
    "pipeline_invocation": ({"DEPENDS_ON notebook:nb -> notebook:child_load [derived]", "DEPENDS_ON notebook:nb -> notebook:child_validate [derived]"}, []),
    "spark_sql": ({"READS_FROM notebook:nb -> file:Files/customers.csv [derived]", "READS_FROM notebook:nb -> file:Files/orders.csv [derived]"},
                  [("unsupported_construct", "Files/orders.csv")]),
    "udfs": ({"READS_FROM notebook:nb -> file:Files/customers.csv [derived]"}, []),
    "windows": ({"READS_FROM notebook:nb -> file:Files/sales.csv [derived]"}, []),
}


@pytest.mark.parametrize("case", sorted(CORPUS_EXPECTED))
def test_compatibility_corpus_notebooks(case: str) -> None:
    doc = json.loads((CASES / case / "notebook.ipynb").read_text(encoding="utf-8"))
    f = analyze_notebook(doc, context=PyContext(default_lakehouse=dict(LH), process=NB_PROCESS))
    want_edges, want_gaps = CORPUS_EXPECTED[case]
    assert edges(f, "READS_FROM", "WRITES_TO", "DEPENDS_ON") == want_edges
    assert gaps(f) == want_gaps


def test_every_corpus_notebook_is_covered() -> None:
    assert {p.name for p in CASES.iterdir() if (p / "notebook.ipynb").exists()} == set(CORPUS_EXPECTED)
