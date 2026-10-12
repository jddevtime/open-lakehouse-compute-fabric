# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""PySpark / Python analyzer: exact edges, columns, confidence and gaps for a corpus of programs, plus the honesty rules (dynamic names, unknown expressions, caps)."""

# ruff: noqa: S608  (the programs under test build SQL text on purpose)

from __future__ import annotations

import textwrap

import pytest

from olcf.intelligence.lineage.base import PyContext
from olcf.intelligence.lineage.pyspark import analyze_python
from tests.intelligence.lineage.support import LH, NB_PROCESS, edges, find, gap_kinds, gap_text, gaps

CASES = [
    pytest.param(
        {},
        '''from pyspark.sql import functions as F, Window

df = spark.read.table("orders")
out = df.withColumn("net", F.col("amt") - F.col("disc")).select("id", "net").withColumnRenamed("net", "net_amt")
out.write.mode("overwrite").saveAsTable("gold.orders_net")
''',
        {"READS_FROM notebook:nb -> table:orders [derived]",
         "TRANSFORMS col:orders.amt -> col:gold.orders_net.net_amt [derived] | (amt - disc)",
         "TRANSFORMS col:orders.disc -> col:gold.orders_net.net_amt [derived] | (amt - disc)",
         "TRANSFORMS col:orders.id -> col:gold.orders_net.id [derived]",
         "WRITES_TO notebook:nb -> table:gold.orders_net [derived]"},
        set(),
        id="read_select_rename_write",
    ),
    pytest.param(
        {},
        '''
tbl = "orders"
df = spark.sql(f"SELECT id, amt FROM {tbl} WHERE amt > 0")
df.write.saveAsTable("clean")
''',
        {"READS_FROM notebook:nb -> table:orders [derived]",
         "TRANSFORMS col:orders.amt -> col:clean.amt [derived]",
         "TRANSFORMS col:orders.id -> col:clean.id [derived]",
         "WRITES_TO notebook:nb -> table:clean [derived]"},
        set(),
        id="spark_sql_with_literal_variable_in_fstring",
    ),
    pytest.param(
        {},
        '''
import sys
tbl = sys.argv[1]
df = spark.table(f"{tbl}_raw")
df.write.saveAsTable("clean")
''',
        {"WRITES_TO notebook:nb -> table:clean [derived]"},
        {"dynamic_reference", "unsupported_construct"},
        id="dynamic_table_name_from_argv",
    ),
    pytest.param(
        {},
        '''from pyspark.sql import functions as F, Window

w = Window.partitionBy("cust").orderBy("dt")
df = spark.table("orders").withColumn("rn", F.row_number().over(w)).withColumn("run", F.sum("amt").over(w))
df.write.saveAsTable("ranked")
''',
        {"READS_FROM notebook:nb -> table:orders [derived]",
         "TRANSFORMS col:orders.amt -> col:ranked.run [derived] | sum(amt) OVER (PARTITION BY cust ORDER BY dt)",
         "TRANSFORMS col:orders.cust -> col:ranked.rn [derived] | row_number() OVER (PARTITION BY cust ORDER BY dt)",
         "TRANSFORMS col:orders.cust -> col:ranked.run [derived] | sum(amt) OVER (PARTITION BY cust ORDER BY dt)",
         "TRANSFORMS col:orders.dt -> col:ranked.rn [derived] | row_number() OVER (PARTITION BY cust ORDER BY dt)",
         "TRANSFORMS col:orders.dt -> col:ranked.run [derived] | sum(amt) OVER (PARTITION BY cust ORDER BY dt)",
         "WRITES_TO notebook:nb -> table:ranked [derived]"},
        {"unsupported_construct"},
        id="window_functions",
    ),
    pytest.param(
        {"orders": ["id", "cust", "dt", "amt"]},
        '''from pyspark.sql import functions as F, Window

w = Window.partitionBy("cust").orderBy("dt")
df = spark.table("orders").withColumn("rn", F.row_number().over(w))
df.write.saveAsTable("ranked")
''',
        {"READS_FROM notebook:nb -> table:orders [derived]",
         "TRANSFORMS col:orders.amt -> col:ranked.amt [derived]",
         "TRANSFORMS col:orders.cust -> col:ranked.cust [derived]",
         "TRANSFORMS col:orders.cust -> col:ranked.rn [derived] | row_number() OVER (PARTITION BY cust ORDER BY dt)",
         "TRANSFORMS col:orders.dt -> col:ranked.dt [derived]",
         "TRANSFORMS col:orders.dt -> col:ranked.rn [derived] | row_number() OVER (PARTITION BY cust ORDER BY dt)",
         "TRANSFORMS col:orders.id -> col:ranked.id [derived]",
         "WRITES_TO notebook:nb -> table:ranked [derived]"},
        set(),
        id="window_functions_known_schema",
    ),
    pytest.param(
        {},
        '''from pyspark.sql import functions as F, Window

a = spark.table("a"); b = spark.table("b")
j = a.join(b, a.id == b.id, "left").select(a.id, b.id.alias("bid"), a.v, b.v.alias("bv"))
j.write.saveAsTable("joined")
''',
        {"READS_FROM notebook:nb -> table:a [derived]",
         "READS_FROM notebook:nb -> table:b [derived]",
         "TRANSFORMS col:a.id -> col:joined.id [derived]",
         "TRANSFORMS col:a.v -> col:joined.v [derived]",
         "TRANSFORMS col:b.id -> col:joined.bid [derived]",
         "TRANSFORMS col:b.v -> col:joined.bv [derived]",
         "WRITES_TO notebook:nb -> table:joined [derived]"},
        set(),
        id="join_with_alias_collision",
    ),
    pytest.param(
        {},
        '''from pyspark.sql import functions as F, Window

g = spark.table("sales").groupBy("region").agg(F.sum("amt").alias("total"), F.countDistinct("cust").alias("n"))
g.write.format("delta").mode("overwrite").save("Tables/by_region")
''',
        {"READS_FROM notebook:nb -> table:sales [derived]",
         "TRANSFORMS col:sales.amt -> col:by_region.total [derived] | sum(amt)",
         "TRANSFORMS col:sales.cust -> col:by_region.n [derived] | countdistinct(cust)",
         "TRANSFORMS col:sales.region -> col:by_region.region [derived]",
         "WRITES_TO notebook:nb -> table:by_region [derived]"},
        set(),
        id="groupby_agg_path_write",
    ),
    pytest.param(
        {},
        '''
df = spark.read.format("csv").option("header", True).load("Files/raw/orders.csv")
df.write.mode("append").format("delta").saveAsTable("bronze_orders")
''',
        {"READS_FROM notebook:nb -> file:Files/raw/orders.csv [derived]",
         "WRITES_TO notebook:nb -> table:bronze_orders [derived]"},
        {"unsupported_construct"},
        id="csv_file_to_table_unknown_columns",
    ),
    pytest.param(
        {},
        '''
from delta.tables import DeltaTable
t = DeltaTable.forName(spark, "gold.daily")
s = spark.table("silver.sales")
t.alias("t").merge(s.alias("s"), "t.id = s.id").whenMatchedUpdate(set={"v": "s.v"}).execute()
''',
        {"READS_FROM notebook:nb -> table:silver.sales [derived]",
         "TRANSFORMS col:silver.sales.v -> col:gold.daily.v [derived] | s.v",
         "WRITES_TO notebook:nb -> table:gold.daily [derived]"},
        set(),
        id="delta_merge_for_name",
    ),
    pytest.param(
        {},
        '''
import pandas as pd
d = pd.read_csv("/lakehouse/default/Files/x.csv")
d.to_csv("/lakehouse/default/Files/y.csv")
''',
        {"READS_FROM notebook:nb -> file:Files/x.csv [derived]",
         "WRITES_TO notebook:nb -> file:Files/y.csv [derived]"},
        set(),
        id="pandas_csv_roundtrip",
    ),
    pytest.param(
        {},
        '''
for name in ["a", "b"]:
    spark.table(name).write.saveAsTable(name + "_copy")
''',
        {"READS_FROM notebook:nb -> table:a [derived]",
         "READS_FROM notebook:nb -> table:b [derived]",
         "WRITES_TO notebook:nb -> table:a_copy [derived]",
         "WRITES_TO notebook:nb -> table:b_copy [derived]"},
        {"unsupported_construct"},
        id="loop_over_literal_list",
    ),
    pytest.param(
        {},
        '''
def load(n):
    return spark.table(n).select("id")
x = load("t1")
x.write.saveAsTable("t1_ids")
''',
        {"READS_FROM notebook:nb -> table:t1 [derived]",
         "TRANSFORMS col:t1.id -> col:t1_ids.id [derived]",
         "WRITES_TO notebook:nb -> table:t1_ids [derived]"},
        set(),
        id="function_inlining",
    ),
    pytest.param(
        {},
        '''
# spark.table("in_comment")
"""docstring mentions spark.table("in_docstring")"""
x = spark.table("real")
''',
        {"READS_FROM notebook:nb -> table:real [derived]"},
        set(),
        id="comments_and_docstrings_ignored",
    ),
    pytest.param(
        {},
        '''
from pyspark.sql.functions import udf
@udf("int")
def f(v): return v + 1
df = spark.table("t").withColumn("w", f("v")).select("id", "w")
df.write.saveAsTable("o")
''',
        {"READS_FROM notebook:nb -> table:t [derived]",
         "TRANSFORMS col:t.id -> col:o.id [derived]",
         "TRANSFORMS col:t.v -> col:o.w [derived] | f(v)",
         "WRITES_TO notebook:nb -> table:o [derived]"},
        set(),
        id="udf_with_string_argument",
    ),
    pytest.param(
        {},
        '''from pyspark.sql import functions as F, Window

from pyspark.sql.types import IntegerType
f = F.udf(lambda x: x + 1, IntegerType())
df = spark.table("t").withColumn("w", f(F.col("v"))).select("id", "w")
df.write.saveAsTable("o")
''',
        {"READS_FROM notebook:nb -> table:t [derived]",
         "TRANSFORMS col:t.id -> col:o.id [derived]",
         "TRANSFORMS col:t.v -> col:o.w [derived] | udf(v)",
         "WRITES_TO notebook:nb -> table:o [derived]"},
        set(),
        id="udf_lambda",
    ),
    pytest.param(
        {},
        '''
df = spark.table("t")
df.createOrReplaceTempView("tv")
spark.sql("CREATE TABLE o AS SELECT id FROM tv")
''',
        {"READS_FROM notebook:nb -> table:t [derived]",
         "TRANSFORMS col:t.id -> col:o.id [derived]",
         "WRITES_TO notebook:nb -> table:o [derived]"},
        set(),
        id="temp_view_then_sql",
    ),
    pytest.param(
        {},
        '''
df = spark.table("t").selectExpr("id", "amt * 2 as dbl")
df.write.saveAsTable("o")
''',
        {"READS_FROM notebook:nb -> table:t [derived]",
         "TRANSFORMS col:t.amt -> col:o.dbl [derived] | amt * 2",
         "TRANSFORMS col:t.id -> col:o.id [derived]",
         "WRITES_TO notebook:nb -> table:o [derived]"},
        set(),
        id="select_expr_strings",
    ),
    pytest.param(
        {},
        '''
name = compute_name()
spark.table(name).write.saveAsTable("o")
''',
        {"WRITES_TO notebook:nb -> table:o [derived]"},
        {"dynamic_reference", "unsupported_construct"},
        id="unknown_table_name_from_function",
    ),
    pytest.param(
        {},
        '''
x = spark.table("t"
''',
        set(),
        {"parse_error"},
        id="syntax_error",
    ),
    pytest.param(
        {},
        '''
df = spark.table("t").withColumn("w", mystery(1)).select("id", "w")
df.write.saveAsTable("o")
''',
        {"READS_FROM notebook:nb -> table:t [derived]",
         "TRANSFORMS col:t.id -> col:o.id [derived]",
         "WRITES_TO notebook:nb -> table:o [derived]"},
        {"unsupported_construct"},
        id="unknown_function_in_column",
    ),
    pytest.param(
        {},
        '''from pyspark.sql import functions as F, Window

df = spark.table("t").withColumn("a", F.lit(1)).withColumn("b", F.current_timestamp()).select("id", "a", "b")
df.write.saveAsTable("o")
''',
        {"READS_FROM notebook:nb -> table:t [derived]",
         "TRANSFORMS col:t.id -> col:o.id [derived]",
         "WRITES_TO notebook:nb -> table:o [derived]"},
        set(),
        id="literal_columns_have_no_origin_and_no_gap",
    ),
    pytest.param(
        {},
        '''
df = spark.table("t").rdd.map(lambda r: r).toDF(["a", "b"])
df.write.saveAsTable("o")
''',
        {"READS_FROM notebook:nb -> table:t [derived]",
         "WRITES_TO notebook:nb -> table:o [derived]"},
        {"unsupported_construct"},
        id="rdd_roundtrip",
    ),
    pytest.param(
        {},
        '''
pdf = spark.table("t").toPandas()
spark.createDataFrame(pdf).write.saveAsTable("o")
''',
        {"READS_FROM notebook:nb -> table:t [derived]",
         "WRITES_TO notebook:nb -> table:o [derived]"},
        {"unsupported_construct"},
        id="topandas_roundtrip",
    ),
    pytest.param(
        {},
        '''
df = spark.table("t").select([c for c in ["x", "y"]])
df.write.saveAsTable("o")
''',
        {"READS_FROM notebook:nb -> table:t [derived]",
         "TRANSFORMS col:t.x -> col:o.x [derived]",
         "TRANSFORMS col:t.y -> col:o.y [derived]",
         "WRITES_TO notebook:nb -> table:o [derived]"},
        set(),
        id="select_list_comprehension",
    ),
    pytest.param(
        {},
        '''
df = spark.table("t").transform(lambda d: d.select("id"))
df.write.saveAsTable("o")
''',
        {"READS_FROM notebook:nb -> table:t [derived]",
         "TRANSFORMS col:t.id -> col:o.id [derived]",
         "WRITES_TO notebook:nb -> table:o [derived]"},
        set(),
        id="transform_lambda",
    ),
    pytest.param(
        {},
        '''from pyspark.sql import functions as F, Window

cols = ["a", "b"]
df = spark.table("t").select(*cols).withColumn("c", F.col("a") + F.col("b"))
df.write.saveAsTable("o")
''',
        {"READS_FROM notebook:nb -> table:t [derived]",
         "TRANSFORMS col:t.a -> col:o.a [derived]",
         "TRANSFORMS col:t.a -> col:o.c [derived] | (a + b)",
         "TRANSFORMS col:t.b -> col:o.b [derived]",
         "TRANSFORMS col:t.b -> col:o.c [derived] | (a + b)",
         "WRITES_TO notebook:nb -> table:o [derived]"},
        set(),
        id="star_args_select",
    ),
    pytest.param(
        {},
        '''
df = spark.table("a").unionByName(spark.table("b"))
df.write.saveAsTable("o")
''',
        {"READS_FROM notebook:nb -> table:a [derived]",
         "READS_FROM notebook:nb -> table:b [derived]",
         "WRITES_TO notebook:nb -> table:o [derived]"},
        {"unsupported_construct"},
        id="union_by_name_unknown_schema",
    ),
    pytest.param(
        {},
        '''
import os
env = os.environ["ENV"]
spark.sql(f"INSERT INTO {env}_gold SELECT id FROM silver")
''',
        {"READS_FROM notebook:nb -> table:silver [derived]"},
        {"dynamic_reference"},
        id="f_string_with_unknown_hole_in_table_name",
    ),
    pytest.param(
        {},
        '''
spark.sql("CREATE TABLE a AS SELECT x FROM s1")
spark.sql("INSERT INTO b SELECT y FROM s2")
''',
        {"READS_FROM notebook:nb -> table:s1 [derived]",
         "READS_FROM notebook:nb -> table:s2 [derived]",
         "TRANSFORMS col:s1.x -> col:a.x [derived]",
         "TRANSFORMS col:s2.y -> col:b.y [inferred]",
         "WRITES_TO notebook:nb -> table:a [derived]",
         "WRITES_TO notebook:nb -> table:b [derived]"},
        set(),
        id="spark_sql_multi_statement_in_one_call",
    ),
    pytest.param(
        {},
        '''
root = get_root()
spark.table("t").write.format("delta").save(root + "/Tables/x")
''',
        {"READS_FROM notebook:nb -> table:t [derived]",
         "WRITES_TO notebook:nb -> table:x [inferred]"},
        {"dynamic_reference", "unsupported_construct"},
        id="write_to_path_with_unknown_root",
    ),
    pytest.param(
        {},
        '''from pyspark.sql import functions as F, Window

spark.table("s").select("a").write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("tgt")
''',
        {"READS_FROM notebook:nb -> table:s [derived]",
         "TRANSFORMS col:s.a -> col:tgt.a [derived]",
         "WRITES_TO notebook:nb -> table:tgt [derived]"},
        set(),
        id="save_as_table_with_format_options",
    ),
    pytest.param(
        {},
        '''from pyspark.sql import functions as F, Window

spark.table("s").select("a", "b").write.insertInto("tgt")
''',
        {"READS_FROM notebook:nb -> table:s [derived]",
         "TRANSFORMS col:s.a -> col:tgt.a [inferred]",
         "TRANSFORMS col:s.b -> col:tgt.b [inferred]",
         "WRITES_TO notebook:nb -> table:tgt [derived]"},
        set(),
        id="insert_into_by_position",
    ),
    pytest.param(
        {},
        '''from pyspark.sql import functions as F, Window

spark.table("s").select("a", "b").write.partitionBy("a").mode("overwrite").saveAsTable("tgt")
''',
        {"READS_FROM notebook:nb -> table:s [derived]",
         "TRANSFORMS col:s.a -> col:tgt.a [derived]",
         "TRANSFORMS col:s.b -> col:tgt.b [derived]",
         "WRITES_TO notebook:nb -> table:tgt [derived]"},
        set(),
        id="overwrite_partition_by",
    ),
]


def _run(src: str, **kw: object):  # type: ignore[no-untyped-def]
    return analyze_python(textwrap.dedent(src), context=PyContext(default_lakehouse=dict(LH), process=NB_PROCESS, **kw))  # type: ignore[arg-type]


@pytest.mark.parametrize(("schemas", "source", "expected_edges", "expected_gaps"), CASES)
def test_program_corpus(schemas: dict[str, list[str]], source: str, expected_edges: set[str], expected_gaps: set[str]) -> None:
    facts = _run(source, schemas=schemas)
    assert edges(facts, tr=True) == expected_edges
    assert gap_kinds(facts) == expected_gaps


def test_sql_inside_python_reports_python_line_and_sql_line() -> None:
    facts = _run('''
        x = 1
        spark.sql("""
        CREATE TABLE o AS
        SELECT a FROM t
        """)
    ''')
    ev = find(facts, "WRITES_TO", "notebook:nb", "table:o").evidence[0]
    assert ev.kind == "static" and ev.source == "pyspark-analyzer@1"
    assert ev.location["line"] == 3 and ev.location["sql_line"] == 2


def test_function_that_is_never_called_is_analysed_speculatively_at_inferred_confidence() -> None:
    facts = _run('''
        def never_called():
            spark.table("spec").select("a").write.saveAsTable("spec_out")
        def called():
            spark.table("real").select("a").write.saveAsTable("real_out")
        called()
    ''')
    got = edges(facts, "READS_FROM", "WRITES_TO")
    assert got == {
        "READS_FROM notebook:nb -> table:real [derived]", "WRITES_TO notebook:nb -> table:real_out [derived]",
        "READS_FROM notebook:nb -> table:spec [inferred]", "WRITES_TO notebook:nb -> table:spec_out [inferred]",
    }
    assert not gap_kinds(facts), "speculative analysis must not add gaps"


def test_recursion_and_deep_call_chains_terminate() -> None:
    facts = _run('''
        def rec(n):
            return rec(n - 1)
        rec(3)
        spark.table("t").select("a").write.saveAsTable("o")
    ''')
    assert "READS_FROM notebook:nb -> table:t [derived]" in edges(facts)


def test_branches_that_assign_different_names_make_the_name_dynamic() -> None:
    facts = _run('''
        if cond:
            t = "a"
        else:
            t = "b"
        spark.table(t).select("x").write.saveAsTable("o")
    ''')
    assert not {e for e in edges(facts) if e.startswith("READS_FROM")}
    assert gaps(facts, "dynamic_reference") == [("dynamic_reference", "t")]


def test_branches_that_assign_the_same_name_keep_it() -> None:
    facts = _run('''
        if cond:
            t = "a"
        else:
            t = "a"
        spark.table(t).select("x").write.saveAsTable("o")
    ''')
    assert "READS_FROM notebook:nb -> table:a [derived]" in edges(facts)


def test_names_taken_from_a_parameter_cell_are_inferred_not_derived() -> None:
    from olcf.intelligence.lineage.notebook import analyze_notebook

    nb = {"cells": [
        {"cell_type": "code", "metadata": {"tags": ["parameters"]}, "source": 'src_table = "orders"\n'},
        {"cell_type": "code", "metadata": {}, "source": 'spark.table(src_table).select("id").write.saveAsTable("out")\n'},
    ]}
    facts = analyze_notebook(nb, context=PyContext(default_lakehouse=dict(LH), process=NB_PROCESS))
    assert edges(facts, "READS_FROM", "WRITES_TO") == {"READS_FROM notebook:nb -> table:orders [inferred]", "WRITES_TO notebook:nb -> table:out [derived]"}
    assert edges(facts, "TRANSFORMS") == {"TRANSFORMS col:orders.id -> col:out.id [inferred]"}


def test_duckdb_in_memory_tables_are_not_lakehouse_tables() -> None:
    facts = _run('''
        import duckdb
        duckdb.sql("CREATE TABLE o AS SELECT a FROM 'Files/x.parquet'")
        con = duckdb.connect()
        df = con.execute("select a from read_csv('Files/y.csv')").df()
    ''')
    assert edges(facts) == {"READS_FROM notebook:nb -> file:Files/x.parquet [derived]", "READS_FROM notebook:nb -> file:Files/y.csv [derived]"}


def test_onelake_url_read_resolves_to_the_table_and_notebook_call_to_a_dependency() -> None:
    facts = _run('''
        import notebookutils
        notebookutils.notebook.run("child_nb")
        df = spark.read.parquet("abfss://w@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Tables/sales")
    ''', known_notebooks={"child_nb": "itm_child"})
    assert edges(facts) == {"DEPENDS_ON notebook:nb -> notebook:child_nb [derived]", "READS_FROM notebook:nb -> table:sales [derived]"}


def test_loops_are_bounded_and_do_not_hang() -> None:
    import time

    start = time.monotonic()
    facts = _run('''
        for i in range(100000):
            for j in range(100000):
                spark.table("t")
        while True:
            spark.table("u")
    ''')
    assert time.monotonic() - start < 20
    assert {"READS_FROM notebook:nb -> table:t [derived]", "READS_FROM notebook:nb -> table:u [derived]"} <= edges(facts)


def test_step_budget_and_size_caps_end_in_a_gap(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("olcf.intelligence.lineage.pyspark.MAX_STEPS", 200)
    facts = _run('''
        for i in range(50):
            for j in range(50):
                x = i + j
        spark.table("never_reached").write.saveAsTable("o")
    ''')
    assert "analysis budget was exhausted" in gap_text(facts)
    monkeypatch.setattr("olcf.intelligence.lineage.pyspark.MAX_PY_CHARS", 40)
    big = _run('spark.table("some_table_with_a_long_name").write.saveAsTable("o")')
    assert not big.edges and "larger than 40 characters" in gap_text(big)


def test_pathological_nesting_is_a_parse_error_not_a_crash() -> None:
    facts = analyze_python("x = " + "(" * 500 + "1" + ")" * 500, context=PyContext(process=NB_PROCESS))
    assert gap_kinds(facts) == {"parse_error"} and not facts.edges


def test_non_text_source_is_a_parse_error_gap() -> None:
    facts = analyze_python(None, context=PyContext(process=NB_PROCESS))  # type: ignore[arg-type]
    assert gaps(facts) == [("parse_error", "python")]


def test_without_a_process_the_code_becomes_a_script_asset_keyed_by_its_hash() -> None:
    src = 'spark.table("t").write.saveAsTable("o")'
    facts = analyze_python(src, context=PyContext(default_lakehouse=dict(LH)))
    scripts = [a for a in facts.assets if a.ref.type == "script"]
    assert len(scripts) == 1 and scripts[0].ref.alias_map["query"].startswith("sha1:")
    assert {e.src.type for e in facts.edges if e.type in ("READS_FROM", "WRITES_TO")} == {"script"}


def test_syntax_warnings_of_the_analysed_code_do_not_leak() -> None:
    import warnings

    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter("always")
        f = analyze_python("path = '\\c'\nspark.read.table('t')\n", context=PyContext(default_lakehouse=dict(LH), process=NB_PROCESS))
    assert not seen
    assert "READS_FROM notebook:nb -> table:t [derived]" in edges(f)
