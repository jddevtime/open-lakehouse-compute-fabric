# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""SQL analyzer: exact edges, columns, confidence and gaps for a corpus of statements (Spark, DuckDB and T-SQL), plus the honesty rules.

Every case lists the complete set of edges the analyzer must produce (so an extra, wrong edge fails as surely as a missing one) and the set of gap kinds.
"""

from __future__ import annotations

import pytest

from olcf.intelligence.lineage.base import SqlContext
from olcf.intelligence.lineage.sql import analyze_sql, query_outputs, split_statements
from olcf.intelligence.model import AssetRef
from tests.intelligence.lineage.support import (
    LH,
    NB_PROCESS,
    asset_labels,
    edges,
    find,
    gap_kinds,
    gap_text,
    gaps,
    join_chain,
)

CASES = [
    pytest.param(
        "CREATE TABLE gold AS SELECT o.id, o.amt * 2 AS dbl, c.name FROM orders o JOIN customers c ON c.id = o.cid",
        "spark", {},
        {"READS_FROM notebook:nb -> table:customers [derived]",
         "READS_FROM notebook:nb -> table:orders [derived]",
         "TRANSFORMS col:customers.name -> col:gold.name [derived] | c.name",
         "TRANSFORMS col:orders.amt -> col:gold.dbl [derived] | o.amt * 2",
         "TRANSFORMS col:orders.id -> col:gold.id [derived] | o.id",
         "WRITES_TO notebook:nb -> table:gold [derived]"},
        set(),
        id="ctas_join_alias",
    ),
    pytest.param(
        "WITH a AS (SELECT id, amt FROM orders), b AS (SELECT id, amt + 1 AS amt1 FROM a) INSERT INTO t SELECT id, amt1 AS total FROM b",
        "spark", {},
        {"READS_FROM notebook:nb -> table:orders [derived]",
         "TRANSFORMS col:orders.amt -> col:t.total [inferred] | amt + 1",
         "TRANSFORMS col:orders.id -> col:t.id [inferred]",
         "WRITES_TO notebook:nb -> table:t [derived]"},
        set(),
        id="cte_chain_insert",
    ),
    pytest.param(
        "INSERT INTO t SELECT id, SUM(amt) OVER (PARTITION BY cust ORDER BY dt) AS running FROM orders",
        "spark", {},
        {"READS_FROM notebook:nb -> table:orders [derived]",
         "TRANSFORMS col:orders.amt -> col:t.running [inferred] | SUM(amt) OVER (PARTITION BY cust ORDER BY dt)",
         "TRANSFORMS col:orders.cust -> col:t.running [inferred] | SUM(amt) OVER (PARTITION BY cust ORDER BY dt)",
         "TRANSFORMS col:orders.dt -> col:t.running [inferred] | SUM(amt) OVER (PARTITION BY cust ORDER BY dt)",
         "TRANSFORMS col:orders.id -> col:t.id [inferred]",
         "WRITES_TO notebook:nb -> table:t [derived]"},
        set(),
        id="window_function",
    ),
    pytest.param(
        "CREATE TABLE x AS SELECT * FROM orders",
        "spark", {},
        {"READS_FROM notebook:nb -> table:orders [derived]",
         "WRITES_TO notebook:nb -> table:x [derived]"},
        {"unsupported_construct"},
        id="star_unknown_schema",
    ),
    pytest.param(
        "CREATE TABLE x AS SELECT * FROM orders",
        "spark", {"orders": ["id", "amt"]},
        {"READS_FROM notebook:nb -> table:orders [derived]",
         "TRANSFORMS col:orders.amt -> col:x.amt [derived]",
         "TRANSFORMS col:orders.id -> col:x.id [derived]",
         "WRITES_TO notebook:nb -> table:x [derived]"},
        set(),
        id="star_known_schema",
    ),
    pytest.param(
        "CREATE TABLE out AS SELECT * EXCEPT (secret) FROM people",
        "spark", {"people": ["id", "name", "secret"]},
        {"READS_FROM notebook:nb -> table:people [derived]",
         "TRANSFORMS col:people.id -> col:out.id [derived]",
         "TRANSFORMS col:people.name -> col:out.name [derived]",
         "WRITES_TO notebook:nb -> table:out [derived]"},
        set(),
        id="star_except_known_schema",
    ),
    pytest.param(
        "CREATE TABLE out AS SELECT a.*, b.name FROM a JOIN b ON a.k = b.k",
        "spark", {"a": ["k", "v"]},
        {"READS_FROM notebook:nb -> table:a [derived]",
         "READS_FROM notebook:nb -> table:b [derived]",
         "TRANSFORMS col:a.k -> col:out.k [derived]",
         "TRANSFORMS col:a.v -> col:out.v [derived]",
         "TRANSFORMS col:b.name -> col:out.name [derived] | b.name",
         "WRITES_TO notebook:nb -> table:out [derived]"},
        set(),
        id="star_qualified_with_schema",
    ),
    pytest.param(
        "CREATE TABLE x AS SELECT t.id AS id, u.id AS id FROM t JOIN u ON 1 = 1",
        "spark", {},
        {"READS_FROM notebook:nb -> table:t [derived]",
         "READS_FROM notebook:nb -> table:u [derived]",
         "TRANSFORMS col:t.id -> col:x.id [derived] | t.id",
         "TRANSFORMS col:u.id -> col:x.id [derived] | u.id",
         "WRITES_TO notebook:nb -> table:x [derived]"},
        set(),
        id="alias_collision_same_output_name",
    ),
    pytest.param(
        "CREATE TABLE x AS SELECT id, amt FROM a JOIN b ON a.k = b.k",
        "spark", {},
        {"READS_FROM notebook:nb -> table:a [derived]",
         "READS_FROM notebook:nb -> table:b [derived]",
         "WRITES_TO notebook:nb -> table:x [derived]"},
        {"unresolved_reference"},
        id="ambiguous_unqualified_columns",
    ),
    pytest.param(
        "CREATE TABLE out AS SELECT id, name FROM a JOIN b USING (id)",
        "spark", {"a": ["id", "name"], "b": ["id", "city"]},
        {"READS_FROM notebook:nb -> table:a [derived]",
         "READS_FROM notebook:nb -> table:b [derived]",
         "TRANSFORMS col:a.id -> col:out.id [derived]",
         "TRANSFORMS col:a.name -> col:out.name [derived]",
         "TRANSFORMS col:b.id -> col:out.id [derived]",
         "WRITES_TO notebook:nb -> table:out [derived]"},
        set(),
        id="join_using_known_schemas",
    ),
    pytest.param(
        "INSERT OVERWRITE TABLE tgt SELECT id, v FROM src",
        "spark", {},
        {"READS_FROM notebook:nb -> table:src [derived]",
         "TRANSFORMS col:src.id -> col:tgt.id [inferred]",
         "TRANSFORMS col:src.v -> col:tgt.v [inferred]",
         "WRITES_TO notebook:nb -> table:tgt [derived]"},
        set(),
        id="insert_overwrite",
    ),
    pytest.param(
        "INSERT INTO t (a, b) SELECT x, y + 1 FROM src",
        "spark", {},
        {"READS_FROM notebook:nb -> table:src [derived]",
         "TRANSFORMS col:src.x -> col:t.a [derived]",
         "TRANSFORMS col:src.y -> col:t.b [derived] | y + 1",
         "WRITES_TO notebook:nb -> table:t [derived]"},
        set(),
        id="insert_with_column_list",
    ),
    pytest.param(
        "INSERT INTO t (a, b) VALUES (1, 2)",
        "spark", {},
        {"WRITES_TO notebook:nb -> table:t [derived]"},
        set(),
        id="insert_values_only_writes",
    ),
    pytest.param(
        "MERGE INTO tgt t USING src s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.v = s.v + 1 WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)",
        "spark", {},
        {"READS_FROM notebook:nb -> table:src [derived]",
         "TRANSFORMS col:src.id -> col:tgt.id [derived] | s.id",
         "TRANSFORMS col:src.v -> col:tgt.v [derived] | s.v + 1 | s.v",
         "WRITES_TO notebook:nb -> table:tgt [derived]"},
        set(),
        id="merge_update_insert",
    ),
    pytest.param(
        "MERGE INTO tgt USING (SELECT id, v FROM src) s ON tgt.id = s.id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *",
        "spark", {"tgt": ["id", "v"]},
        {"READS_FROM notebook:nb -> table:src [derived]",
         "TRANSFORMS col:src.id -> col:tgt.id [derived]",
         "TRANSFORMS col:src.v -> col:tgt.v [derived]",
         "WRITES_TO notebook:nb -> table:tgt [derived]"},
        set(),
        id="merge_star_with_subquery_source",
    ),
    pytest.param(
        "MERGE INTO tgt t USING src s ON t.id = s.id WHEN MATCHED AND s.del = 1 THEN DELETE",
        "spark", {},
        {"READS_FROM notebook:nb -> table:src [derived]",
         "WRITES_TO notebook:nb -> table:tgt [derived]"},
        set(),
        id="merge_delete_only",
    ),
    pytest.param(
        "-- orders in comment\nSELECT 1; /* from fake_table */ CREATE TABLE z AS SELECT a FROM real_table",
        "spark", {},
        {"READS_FROM notebook:nb -> table:real_table [derived]",
         "TRANSFORMS col:real_table.a -> col:z.a [derived]",
         "WRITES_TO notebook:nb -> table:z [derived]"},
        set(),
        id="comments_naming_tables_are_ignored",
    ),
    pytest.param(
        "CREATE TABLE tgt AS SELECT \"Id\", `Amt` FROM `Orders`",
        "spark", {},
        {"READS_FROM notebook:nb -> table:orders [derived]",
         "TRANSFORMS col:orders.amt -> col:tgt.amt [derived] | `Amt`",
         "WRITES_TO notebook:nb -> table:tgt [derived]"},
        set(),
        id="spark_backtick_and_double_quoted",
    ),
    pytest.param(
        "SELECT [Id] FROM [dbo].[Orders]; INSERT INTO [dbo].[t2] SELECT [Id] AS [X] FROM [dbo].[Orders]",
        "tsql", {},
        {"READS_FROM notebook:nb -> table:dbo.orders [derived]",
         "TRANSFORMS col:dbo.orders.id -> col:dbo.t2.x [inferred] | [Id]",
         "WRITES_TO notebook:nb -> table:dbo.t2 [derived]"},
        set(),
        id="tsql_brackets",
    ),
    pytest.param(
        "SELECT id, v INTO #tmp FROM src; INSERT INTO final SELECT id, v FROM #tmp",
        "tsql", {},
        {"READS_FROM notebook:nb -> table:src [derived]",
         "TRANSFORMS col:src.id -> col:final.id [inferred]",
         "TRANSFORMS col:src.v -> col:final.v [inferred]",
         "WRITES_TO notebook:nb -> table:final [derived]"},
        set(),
        id="tsql_select_into_temp_chain",
    ),
    pytest.param(
        "SELECT a.x, b.y INTO dbo.res FROM dbo.A a JOIN dbo.B b ON a.k = b.k\nGO\nINSERT INTO dbo.log SELECT x FROM dbo.res",
        "tsql", {},
        {"READS_FROM notebook:nb -> table:dbo.a [derived]",
         "READS_FROM notebook:nb -> table:dbo.b [derived]",
         "READS_FROM notebook:nb -> table:dbo.res [derived]",
         "TRANSFORMS col:dbo.a.x -> col:dbo.res.x [derived] | a.x",
         "TRANSFORMS col:dbo.b.y -> col:dbo.res.y [derived] | b.y",
         "TRANSFORMS col:dbo.res.x -> col:dbo.log.x [inferred]",
         "WRITES_TO notebook:nb -> table:dbo.log [derived]",
         "WRITES_TO notebook:nb -> table:dbo.res [derived]"},
        set(),
        id="tsql_go_batches",
    ),
    pytest.param(
        "create table T as select ID, Amt from ORDERS",
        "spark", {},
        {"READS_FROM notebook:nb -> table:orders [derived]",
         "TRANSFORMS col:orders.amt -> col:t.amt [derived]",
         "TRANSFORMS col:orders.id -> col:t.id [derived]",
         "WRITES_TO notebook:nb -> table:t [derived]"},
        set(),
        id="case_insensitive_names",
    ),
    pytest.param(
        "CREATE TABLE x AS SELECT id FROM orders UNION ALL SELECT id FROM returns",
        "spark", {},
        {"READS_FROM notebook:nb -> table:orders [derived]",
         "READS_FROM notebook:nb -> table:returns [derived]",
         "TRANSFORMS col:orders.id -> col:x.id [derived]",
         "TRANSFORMS col:returns.id -> col:x.id [derived]",
         "WRITES_TO notebook:nb -> table:x [derived]"},
        set(),
        id="union_all",
    ),
    pytest.param(
        "CREATE VIEW v AS SELECT id, amt FROM orders",
        "spark", {},
        {"READS_FROM notebook:nb -> table:orders [derived]",
         "TRANSFORMS col:orders.amt -> col:v.amt [derived]",
         "TRANSFORMS col:orders.id -> col:v.id [derived]",
         "WRITES_TO notebook:nb -> view:v [derived]"},
        set(),
        id="view",
    ),
    pytest.param(
        "CREATE TABLE x AS SELECT myudf(a) AS u FROM t",
        "spark", {},
        {"READS_FROM notebook:nb -> table:t [derived]",
         "TRANSFORMS col:t.a -> col:x.u [derived] | MYUDF(a)",
         "WRITES_TO notebook:nb -> table:x [derived]"},
        set(),
        id="sql_function_call",
    ),
    pytest.param(
        "CREATE TABLE x AS SELECT CASE WHEN a.s > 1 THEN b.v ELSE 0 END AS r FROM a JOIN b ON a.k = b.k",
        "spark", {},
        {"READS_FROM notebook:nb -> table:a [derived]",
         "READS_FROM notebook:nb -> table:b [derived]",
         "TRANSFORMS col:a.s -> col:x.r [derived] | CASE WHEN a.s > 1 THEN b.v ELSE 0 END",
         "TRANSFORMS col:b.v -> col:x.r [derived] | CASE WHEN a.s > 1 THEN b.v ELSE 0 END",
         "WRITES_TO notebook:nb -> table:x [derived]"},
        set(),
        id="case_expression",
    ),
    pytest.param(
        "CREATE TEMP VIEW tv AS SELECT id, amt FROM orders; CREATE TABLE out AS SELECT id, amt * 2 AS d FROM tv",
        "spark", {},
        {"READS_FROM notebook:nb -> table:orders [derived]",
         "TRANSFORMS col:orders.amt -> col:out.d [derived] | amt * 2",
         "TRANSFORMS col:orders.id -> col:out.id [derived]",
         "WRITES_TO notebook:nb -> table:out [derived]"},
        set(),
        id="temp_view_chain",
    ),
    pytest.param(
        "CREATE TABLE out AS SELECT id, (SELECT max(x) FROM other o WHERE o.k = t.k) AS mx FROM t",
        "spark", {},
        {"READS_FROM notebook:nb -> table:other [derived]",
         "READS_FROM notebook:nb -> table:t [derived]",
         "TRANSFORMS col:other.x -> col:out.mx [derived] | (SELECT MAX(x) FROM other AS o WHERE o.k = t.k) <- MAX(x)",
         "TRANSFORMS col:t.id -> col:out.id [derived]",
         "WRITES_TO notebook:nb -> table:out [derived]"},
        set(),
        id="correlated_subquery",
    ),
    pytest.param(
        "CREATE TABLE out AS SELECT id, c FROM t LATERAL VIEW explode(arr) e AS c",
        "spark", {},
        {"READS_FROM notebook:nb -> table:t [derived]",
         "TRANSFORMS col:t.arr -> col:out.c [derived] | EXPLODE(arr)",
         "TRANSFORMS col:t.id -> col:out.id [derived]",
         "WRITES_TO notebook:nb -> table:out [derived]"},
        set(),
        id="lateral_view_explode",
    ),
    pytest.param(
        "UPDATE tgt SET v = 1 WHERE id IN (SELECT id FROM src)",
        "spark", {},
        {"READS_FROM notebook:nb -> table:src [derived]",
         "WRITES_TO notebook:nb -> table:tgt [derived]"},
        set(),
        id="update_and_in_subquery",
    ),
    pytest.param(
        "DELETE FROM tgt WHERE id IN (SELECT id FROM src)",
        "spark", {},
        {"READS_FROM notebook:nb -> table:src [derived]",
         "WRITES_TO notebook:nb -> table:tgt [derived]"},
        set(),
        id="delete_with_subquery",
    ),
    pytest.param(
        "CREATE TABLE y LIKE orders",
        "spark", {},
        {"WRITES_TO notebook:nb -> table:y [derived]"},
        set(),
        id="create_like_writes_only",
    ),
    pytest.param(
        "DROP TABLE old; USE other_db; SELECT * FROM t",
        "spark", {},
        {"READS_FROM notebook:nb -> table:other_db.t [derived]"},
        {"unsupported_construct"},
        id="use_database",
    ),
    pytest.param(
        "SELEC bad syntax here ((",
        "spark", {},
        set(),
        {"parse_error"},
        id="parse_error",
    ),
    pytest.param(
        "",
        "spark", {},
        set(),
        set(),
        id="empty_text",
    ),
    pytest.param(
        "CREATE TABLE x AS SELECT a FROM ${db}.orders",
        "spark", {},
        {"WRITES_TO notebook:nb -> table:x [derived]"},
        {"dynamic_reference"},
        id="dynamic_database_placeholder_read",
    ),
    pytest.param(
        "INSERT INTO ${db}.tgt SELECT a FROM orders",
        "spark", {},
        {"READS_FROM notebook:nb -> table:orders [derived]"},
        {"dynamic_reference"},
        id="dynamic_database_placeholder_write",
    ),
    pytest.param(
        "SELECT a FROM IDENTIFIER(:t)",
        "spark", {},
        set(),
        {"dynamic_reference"},
        id="identifier_function_dynamic",
    ),
    pytest.param(
        "CREATE TABLE x AS SELECT a, count(*) c FROM orders GROUP BY a HAVING count(*) > 1",
        "spark", {},
        {"READS_FROM notebook:nb -> table:orders [derived]",
         "TRANSFORMS col:orders.a -> col:x.a [derived]",
         "WRITES_TO notebook:nb -> table:x [derived]"},
        set(),
        id="group_by_having",
    ),
    pytest.param(
        "COPY INTO tgt FROM 'abfss://c@acct.dfs.core.windows.net/p/' FILEFORMAT = PARQUET",
        "spark", {},
        {"READS_FROM notebook:nb -> file:abfss://c@acct.dfs.core.windows.net/p/ [derived]",
         "WRITES_TO notebook:nb -> table:tgt [derived]"},
        set(),
        id="copy_into_from_storage",
    ),
    pytest.param(
        "CREATE TABLE x AS SELECT * FROM parquet.`Files/raw/a`",
        "spark", {},
        {"READS_FROM notebook:nb -> file:Files/raw/a [derived]",
         "WRITES_TO notebook:nb -> table:x [derived]"},
        {"unsupported_construct"},
        id="spark_parquet_path_reader",
    ),
    pytest.param(
        "SELECT * FROM delta.`Tables/sales`",
        "spark", {},
        {"READS_FROM notebook:nb -> table:sales [derived]"},
        {"unsupported_construct"},
        id="spark_delta_path_reader",
    ),
    pytest.param(
        "CREATE TABLE out AS SELECT * FROM read_csv('Files/raw/*.csv')",
        "duckdb", {},
        {"READS_FROM notebook:nb -> file:Files/raw/*.csv [derived]",
         "WRITES_TO notebook:nb -> table:out [derived]"},
        {"unsupported_construct"},
        id="duckdb_read_csv_star",
    ),
    pytest.param(
        "CREATE TABLE out AS SELECT a, b FROM read_parquet('Files/p/x.parquet')",
        "duckdb", {},
        {"READS_FROM notebook:nb -> file:Files/p/x.parquet [derived]",
         "TRANSFORMS col:files/p/x.parquet.a -> col:out.a [derived]",
         "TRANSFORMS col:files/p/x.parquet.b -> col:out.b [derived]",
         "WRITES_TO notebook:nb -> table:out [derived]"},
        set(),
        id="duckdb_read_parquet_columns",
    ),
    pytest.param(
        "CREATE TABLE r AS SELECT a, b FROM 'Files/p/x.parquet'",
        "duckdb", {},
        {"READS_FROM notebook:nb -> file:Files/p/x.parquet [derived]",
         "TRANSFORMS col:files/p/x.parquet.a -> col:r.a [derived]",
         "TRANSFORMS col:files/p/x.parquet.b -> col:r.b [derived]",
         "WRITES_TO notebook:nb -> table:r [derived]"},
        set(),
        id="duckdb_file_literal",
    ),
    pytest.param(
        "CACHE TABLE c AS SELECT id FROM src",
        "spark", {},
        {"READS_FROM notebook:nb -> table:src [derived]"},
        set(),
        id="cache_table_as",
    ),
    pytest.param(
        "CREATE TABLE out AS SELECT id FROM src TIMESTAMP AS OF '2024-01-01'",
        "spark", {},
        {"READS_FROM notebook:nb -> table:src [derived]",
         "TRANSFORMS col:src.id -> col:out.id [derived]",
         "WRITES_TO notebook:nb -> table:out [derived]"},
        set(),
        id="time_travel_same_table",
    ),
    pytest.param(
        "CREATE OR REPLACE TABLE out USING DELTA AS SELECT id, name FROM src WHERE x IN (SELECT y FROM filt)",
        "spark", {},
        {"READS_FROM notebook:nb -> table:filt [derived]",
         "READS_FROM notebook:nb -> table:src [derived]",
         "TRANSFORMS col:src.id -> col:out.id [derived]",
         "TRANSFORMS col:src.name -> col:out.name [derived]",
         "WRITES_TO notebook:nb -> table:out [derived]"},
        set(),
        id="ctas_using_delta_with_in_subquery",
    ),
    pytest.param(
        "SELECT upper(name) AS n, count(*) OVER (PARTITION BY region) c FROM cust",
        "spark", {},
        {"READS_FROM notebook:nb -> table:cust [derived]"},
        set(),
        id="select_only_has_no_writes",
    ),
    pytest.param(
        "CREATE TABLE o AS SELECT 1 AS one, current_date() AS d, 'x' AS s FROM src",
        "spark", {},
        {"READS_FROM notebook:nb -> table:src [derived]",
         "WRITES_TO notebook:nb -> table:o [derived]"},
        set(),
        id="literals_have_no_origin",
    ),
    pytest.param(
        "CREATE TABLE o AS SELECT transform(arr, x -> x + k) AS t FROM src",
        "spark", {},
        {"READS_FROM notebook:nb -> table:src [derived]",
         "TRANSFORMS col:src.arr -> col:o.t [derived] | TRANSFORM(arr, x -> x + k)",
         "TRANSFORMS col:src.k -> col:o.t [derived] | TRANSFORM(arr, x -> x + k)",
         "WRITES_TO notebook:nb -> table:o [derived]"},
        set(),
        id="lambda_function",
    ),
    pytest.param(
        "CREATE TABLE o AS SELECT a FROM (SELECT a, b FROM inner_t) q WHERE q.b > 1",
        "spark", {},
        {"READS_FROM notebook:nb -> table:inner_t [derived]",
         "TRANSFORMS col:inner_t.a -> col:o.a [derived]",
         "WRITES_TO notebook:nb -> table:o [derived]"},
        set(),
        id="derived_table",
    ),
]


@pytest.mark.parametrize(("sql", "dialect", "schemas", "expected_edges", "expected_gaps"), CASES)
def test_statement_corpus(sql: str, dialect: str, schemas: dict[str, list[str]], expected_edges: set[str], expected_gaps: set[str]) -> None:
    facts = analyze_sql(sql, dialect=dialect, context=SqlContext(default_lakehouse=dict(LH), process=NB_PROCESS, schemas=schemas))
    assert edges(facts, tr=True) == expected_edges
    assert gap_kinds(facts) == expected_gaps


def _run(sql: str, dialect: str = "spark", **kw: object):  # type: ignore[no-untyped-def]
    return analyze_sql(sql, dialect=dialect, context=SqlContext(default_lakehouse=dict(LH), process=NB_PROCESS, **kw))  # type: ignore[arg-type]


def test_gap_details_name_the_object_and_the_reason() -> None:
    star = _run("CREATE TABLE x AS SELECT * FROM orders")
    assert gaps(star) == [("unsupported_construct", "orders")]
    assert "SELECT * without known schema" in gap_text(star)
    amb = _run("CREATE TABLE x AS SELECT id FROM a JOIN b ON a.k = b.k")
    assert gaps(amb) == [("unresolved_reference", "id")]
    assert "ambiguous" in gap_text(amb)
    dyn = _run("CREATE TABLE x AS SELECT a FROM ${db}.orders")
    assert gaps(dyn) == [("dynamic_reference", "${db}.orders")]
    bad = _run("CREATE TABLE ((")
    assert gap_kinds(bad) == {"parse_error"} and not bad.edges


def test_dynamic_reference_never_yields_an_edge_to_a_guessed_table() -> None:
    for sql in ("SELECT a FROM ${db}.orders", "INSERT INTO ${env}.tgt SELECT a FROM orders", "SELECT a FROM :schema.orders"):
        facts = _run(sql)
        names = {e.dst.name for e in facts.edges}
        assert not names & {"db.orders", "env.tgt", "schema.orders", "orders.orders"}, sql
        assert "dynamic_reference" in gap_kinds(facts)


def test_without_a_process_each_statement_becomes_a_query_asset_keyed_by_its_text() -> None:
    facts = analyze_sql("SELECT a FROM t; CREATE TABLE z AS SELECT a FROM t", dialect="spark", context=SqlContext(default_lakehouse=dict(LH)))
    queries = [a for a in facts.assets if a.ref.type == "sql_query"]
    assert len(queries) == 2
    aliases = [a.ref.alias_map["query"] for a in queries]
    assert all(x.startswith("sha1:") and len(x) == 45 for x in aliases) and aliases[0] != aliases[1]
    assert {e.src.type for e in facts.edges if e.type in ("READS_FROM", "WRITES_TO")} == {"sql_query"}
    again = analyze_sql("SELECT  a\nFROM t;\nCREATE TABLE z AS SELECT a FROM t", dialect="spark", context=SqlContext(default_lakehouse=dict(LH)))
    assert [a.ref.alias_map["query"] for a in again.assets if a.ref.type == "sql_query"] == aliases, "whitespace must not change the statement identity"


def test_evidence_carries_statement_number_line_and_analyzer_version() -> None:
    facts = _run("SELECT 1;\n\nCREATE TABLE z AS SELECT a FROM t")
    e = find(facts, "WRITES_TO", "notebook:nb", "table:z")
    ev = e.evidence[0]
    assert ev.kind == "static" and ev.source == "sql-analyzer@1"
    assert ev.location["statement"] == 2 and ev.location["line"] == 3
    assert ev.snippet and ev.snippet.startswith("CREATE TABLE z")


def test_same_table_read_twice_is_one_edge_with_evidence_of_both() -> None:
    facts = _run("CREATE TABLE a AS SELECT x FROM t; CREATE TABLE b AS SELECT y FROM t")
    reads = [e for e in facts.edges if e.type == "READS_FROM" and e.dst.name == "t"]
    assert len(reads) == 1 and len(reads[0].evidence) == 2
    assert {ev.location["statement"] for ev in reads[0].evidence} == {1, 2}


def test_tables_in_different_lakehouses_are_different_references() -> None:
    facts = _run("CREATE TABLE gold.out AS SELECT id FROM bronze.customers UNION ALL SELECT id FROM silver.customers")
    reads = sorted(e.dst.name or "" for e in facts.edges if e.type == "READS_FROM")
    assert reads == ["bronze.customers", "silver.customers"]


def test_schema_lookup_uses_default_lakehouse_prefix_but_never_strips_other_schemas() -> None:
    schemas = {"lh.orders": ["id", "amt"]}
    ok = _run("CREATE TABLE x AS SELECT * FROM orders", schemas=schemas)
    assert "TRANSFORMS col:orders.id -> col:x.id [derived]" in edges(ok)
    other = _run("CREATE TABLE x AS SELECT * FROM other_lh.orders", schemas=schemas)
    assert not [e for e in other.edges if e.type == "TRANSFORMS"] and gap_kinds(other) == {"unsupported_construct"}


def test_schema_learned_from_create_table_ddl_expands_a_later_star() -> None:
    facts = _run("CREATE TABLE s (id INT, name STRING); CREATE TABLE d AS SELECT * FROM s")
    assert {"TRANSFORMS col:s.id -> col:d.id [derived]", "TRANSFORMS col:s.name -> col:d.name [derived]"} <= edges(facts)
    assert not gap_kinds(facts)


def test_explicit_target_columns_are_derived_and_positional_ones_inferred() -> None:
    explicit = edges(_run("INSERT INTO t (a, b) SELECT x, y FROM src"))
    positional = edges(_run("INSERT INTO t SELECT x, y FROM src"))
    assert "TRANSFORMS col:src.x -> col:t.a [derived]" in explicit
    assert "TRANSFORMS col:src.x -> col:t.x [inferred]" in positional


def test_split_statements_respects_strings_comments_and_go() -> None:
    assert split_statements("select ';' as a; -- ; c\nselect 2; /* ; */ select 3", "spark") == [("select ';' as a", 1), ("select 2", 2), ("select 3", 2)]
    assert split_statements("select 1\nGO\nselect 2\ngo 5\nselect 3", "tsql") == [("select 1", 1), ("select 2", 3), ("select 3", 5)]
    assert split_statements("select 1;; ;select 2", "spark") == [("select 1", 1), ("select 2", 1)]
    assert split_statements("", "spark") == [] and split_statements("  \n-- only a comment", "spark") == []


def test_non_text_input_and_unknown_dialect_are_handled() -> None:
    bad = analyze_sql(123, context=SqlContext())  # type: ignore[arg-type]
    assert gaps(bad) == [("parse_error", "sql")] and not bad.edges
    weird = analyze_sql("SELECT a FROM t", dialect="no-such-dialect", context=SqlContext(process=NB_PROCESS))
    assert edges(weird) == {"READS_FROM notebook:nb -> table:t [derived]"}


def test_statement_count_and_size_caps_produce_a_gap_not_a_hang(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("olcf.intelligence.lineage.sql.MAX_STATEMENTS", 3)
    facts = _run(";".join(f"CREATE TABLE t{i} AS SELECT a FROM s{i}" for i in range(6)))  # noqa: S608
    assert gaps(facts, "unsupported_construct") and "more than 3 statements" in gap_text(facts)
    assert {e.dst.name for e in facts.edges if e.type == "WRITES_TO"} == {"t0", "t1", "t2"}
    monkeypatch.setattr("olcf.intelligence.lineage.sql.MAX_SQL_CHARS", 20)
    big = _run("CREATE TABLE a AS SELECT x FROM some_long_table_name")
    assert not big.edges and "larger than 20 characters" in gap_text(big)


def test_query_outputs_lists_columns_with_their_sources() -> None:
    from olcf.intelligence.lineage.base import LineageContext

    out = query_outputs("select a, b + c as d from t", dialect="duckdb", context=LineageContext(default_lakehouse=dict(LH)))
    assert [(name, sorted(col for _t, col in origins)) for name, origins in out.columns] == [("a", ["a"]), ("d", ["b", "c"])]
    assert out.complete and [t.name for t in out.tables] == ["t"]
    star = query_outputs("select * from t", dialect="duckdb", context=LineageContext(default_lakehouse=dict(LH)))
    assert star.columns == [] and not star.complete


def test_asset_for_process_is_not_invented_when_the_caller_supplies_it() -> None:
    facts = _run("CREATE TABLE z AS SELECT a FROM t")
    assert not asset_labels(facts, "sql_query") and isinstance(NB_PROCESS, AssetRef)


@pytest.mark.parametrize("dialect", ["duckdb", "spark", "tsql"])
@pytest.mark.parametrize("text", ["SELEC broken", "foo", "1 + 1", "x = 5"])
def test_text_that_is_not_a_statement_is_a_parse_error_not_silence(dialect: str, text: str) -> None:
    """A misspelt keyword parses as a bare expression; it must produce a gap rather than an empty, silent result."""
    f = analyze_sql(text, dialect=dialect, context=SqlContext(default_lakehouse=dict(LH)))
    assert gap_kinds(f) == {"parse_error"} and not f.edges
    assert "not a SQL statement" in gap_text(f)
    out = query_outputs(text, dialect=dialect, context=SqlContext(default_lakehouse=dict(LH)), process=None)
    assert gap_kinds(out.facts) == {"parse_error"} and out.columns == []


def test_valid_non_lineage_statements_stay_silent() -> None:
    ctx = SqlContext(default_lakehouse=dict(LH))
    for text in ("SET x = 1", "DROP TABLE x", "TRUNCATE TABLE t", "SELECT 1", "SELECT 1;; SELECT 2", "COMMIT"):
        assert not analyze_sql(text, dialect="spark", context=ctx).gaps, text


def test_bare_join_chain_is_reported_not_parsed() -> None:
    """sqlglot backtracks exponentially over JOINs without ON / USING; a long chain is a gap (instantly), not a stalled analysis."""
    import time

    ctx = SqlContext(default_lakehouse=dict(LH))
    for kind in ("JOIN", "LEFT JOIN", "INNER JOIN"):
        sql = join_chain(kind, 59)
        t0 = time.perf_counter()
        f = analyze_sql(sql, dialect="spark", context=ctx)
        assert time.perf_counter() - t0 < 2
        assert gap_kinds(f) == {"unsupported_construct"}
        assert "without ON / USING" in gap_text(f) and not f.edges


def test_long_join_chains_with_conditions_or_cross_joins_are_analysed() -> None:
    ctx = SqlContext(default_lakehouse=dict(LH))
    on = analyze_sql(join_chain("JOIN", 79, " ON t{i}.k = t0.k"), dialect="spark", context=ctx)
    assert len([e for e in on.edges if e.type == "READS_FROM"]) == 80
    using = analyze_sql(join_chain("LEFT JOIN", 39, " USING (k)"), dialect="spark", context=ctx)
    assert len([e for e in using.edges if e.type == "READS_FROM"]) == 40
    cross = analyze_sql(join_chain("CROSS JOIN", 39), dialect="spark", context=ctx)
    assert len([e for e in cross.edges if e.type == "READS_FROM"]) == 40
    few = analyze_sql(join_chain("JOIN", 8), dialect="spark", context=ctx)
    assert len([e for e in few.edges if e.type == "READS_FROM"]) == 9
