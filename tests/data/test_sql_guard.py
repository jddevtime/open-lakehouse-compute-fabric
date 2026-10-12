# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""SQL parsing, classification, read-only enforcement and typed parameter binding (pure, no database)."""

from __future__ import annotations

import datetime as dt

import pytest

from olcf.core.errors import BadRequest, Unsupported
from olcf.sql import guard as g


def parse(sql, dialect="duckdb", **params):
    st = g.parse_statement(sql, dialect, param_names=set(params))
    st, warnings = g.bind_parameters(st, params, None)
    return st, warnings


def rendered(sql, dialect="duckdb", **params):
    st, _ = parse(sql, dialect, **params)
    return g.render(st.ast, dialect, st.explain_prefix)


@pytest.mark.parametrize("sql,kind", [
    ("SELECT 1", "select"), ("WITH a AS (SELECT 1) SELECT * FROM a", "select"), ("VALUES (1), (2)", "select"), ("FROM t", "select"),
    ("SELECT 1 UNION ALL SELECT 2", "select"), ("DESCRIBE t", "describe"), ("SUMMARIZE t", "summarize"), ("SHOW TABLES", "show"),
    ("EXPLAIN SELECT * FROM t", "explain"), ("EXPLAIN ANALYZE SELECT 1", "explain"),
])
def test_reads_are_classified_read_only(sql, kind):
    st = g.parse_statement(sql)
    assert st.kind == kind and st.read_only
    g.require_read_only(st, "duckdb")


@pytest.mark.parametrize("sql,kind", [
    ("INSERT INTO t VALUES (1)", "insert"), ("CREATE TABLE a AS SELECT 1", "ctas"), ("CREATE VIEW v AS SELECT 1", "create_view"),
    ("UPDATE t SET a = 1", "update"), ("DELETE FROM t", "delete"), ("MERGE INTO t USING s ON t.a = s.a WHEN MATCHED THEN DELETE", "merge"),
    ("DROP TABLE t", "ddl"), ("CREATE TABLE t (a INT)", "ddl"), ("ALTER TABLE t ADD COLUMN b INT", "ddl"),
])
def test_writes_are_rejected_on_duckdb_with_guidance(sql, kind):
    st = g.parse_statement(sql)
    assert st.kind == kind and not st.read_only
    with pytest.raises(Unsupported) as e:
        g.require_read_only(st, "duckdb")
    assert "read-only" in str(e.value) and "spark" in e.value.remediation.lower()
    g.require_read_only(st, "spark") if kind != "ddl" else None   # Spark accepts writes (authorisation happens in the service)


@pytest.mark.parametrize("sql", [
    "COPY t TO 'x.csv'", "ATTACH 'x.db'", "INSTALL httpfs", "SET enable_external_access = true", "PRAGMA database_list", "CALL x()",
    "SELECT * FROM query('select 1')", "SELECT * FROM glob('*')", "SELECT * FROM read_text('/etc/passwd')", "SELECT * FROM read_blob('x')",
    "SELECT * FROM read_xlsx('x.xlsx')", "SELECT getenv('HOME')", "SELECT * FROM delta_scan('/x')",
    "SELECT * FROM duckdb_settings()", "SELECT current_setting('home_directory')", "SELECT * FROM duckdb_extensions()", "SELECT * FROM pragma_database_list()",
])
def test_dangerous_statements_and_functions_are_refused(sql):
    with pytest.raises((Unsupported, BadRequest)):
        st = g.parse_statement(sql)
        g.require_read_only(st, "duckdb")


def test_parse_errors_empty_multi_statement_and_size():
    for bad, msg in [("", "empty"), ("   ", "empty"), ("SELEC ((", "cannot parse"), ("SELECT 1; SELECT 2", "exactly one"), ("x" * (g.MAX_SQL_CHARS + 1), "limited")]:
        with pytest.raises(BadRequest, match=msg):
            g.parse_statement(bad)
    assert g.parse_statement("SELECT 1;").kind == "select"          # a trailing semicolon is fine
    with pytest.raises(BadRequest, match="positional"):
        g.parse_statement("SELECT * FROM t WHERE a = ?")


def test_named_parameters_are_bound_as_typed_literals():
    assert rendered("SELECT * FROM t WHERE region = :r AND qty > $q LIMIT :n", r="e'u; DROP TABLE t; --", q=5, n=10) == \
        "SELECT * FROM t WHERE region = 'e''u; DROP TABLE t; --' AND qty > 5 LIMIT 10"
    assert rendered("SELECT :a, :b, :c, :d", a=None, b=True, c=-3, d=2.5) == "SELECT NULL, TRUE, -3, 2.5"
    assert "CAST('2024-01-31' AS DATE)" in rendered("SELECT * FROM t WHERE d >= :d", d=dt.date(2024, 1, 31))
    assert "CAST('2024-01-31 10:00:00' AS TIMESTAMP)" in rendered("SELECT * FROM t WHERE d >= :d", d=dt.datetime(2024, 1, 31, 10))
    assert rendered("SELECT * FROM t WHERE x IN :ids", ids=[1, 2, 3]) == "SELECT * FROM t WHERE x IN (1, 2, 3)"
    assert rendered("SELECT * FROM t WHERE x IN :ids", dialect="spark", ids=["a", "b"]) == "SELECT * FROM t WHERE x IN ('a', 'b')"


def test_markers_inside_strings_comments_casts_and_quoted_identifiers_are_left_alone():
    sql = "SELECT ':r' AS a, \"x:r\" AS b, a::int AS c, ':r'||:r AS d -- :r\n/* :r */ FROM t WHERE region = :r"
    out = rendered(sql, r="eu")
    assert "':r'" in out and '"x:r"' in out and "CAST(a AS INT)" in out and "'eu'" in out
    assert out.count("'eu'") == 2


def test_unknown_dollar_parameter_is_an_error_but_colon_is_left_for_the_parser():
    with pytest.raises(BadRequest, match="no value"):
        g.parse_statement("SELECT $missing")
    st = g.parse_statement("SELECT {'a': b} FROM t")           # struct literal colon is not a parameter
    assert st.kind == "select"


def test_unused_and_misplaced_parameters():
    st, warnings = parse("SELECT 1", unused=1)
    assert warnings == ["parameter 'unused' is not used by the statement"]
    with pytest.raises(BadRequest, match="only an expression"):
        parse("SELECT * FROM :tbl", tbl="t")


def test_parameter_type_coercion_and_validation():
    assert g.coerce_param("a", "5", "int") == 5 and g.coerce_param("a", 5, "number") == 5 and g.coerce_param("a", "true", "boolean") is True
    assert g.coerce_param("a", "2024-02-03", "date") == dt.date(2024, 2, 3)
    assert g.coerce_param("a", "2024-02-03T04:05:06Z", "timestamp").year == 2024
    assert g.coerce_param("a", [1, "2"], "int") == [1, 2]
    for bad, t in [("x", "int"), (1.5, "int"), ("maybe", "boolean"), ("2024", "date"), ("zz", "timestamp"), (float("inf"), "number"), ({"a": 1}, None), ("x" * 20000, "string")]:
        with pytest.raises(BadRequest):
            g.coerce_param("a", bad, t)
    with pytest.raises(BadRequest):
        g.coerce_param("a", list(range(2000)), "int")


def test_specs_defaults_missing_and_unknown():
    specs = g.validate_param_specs([{"name": "region", "type": "string", "default": "eu"}, {"name": "n", "type": "int"}])
    assert g.resolve_parameter_values({"n": 3}, specs) == {"n": 3, "region": "eu"}
    with pytest.raises(BadRequest, match="missing"):
        g.resolve_parameter_values({}, specs)
    with pytest.raises(BadRequest, match="unknown"):
        g.resolve_parameter_values({"n": 1, "zzz": 2}, specs)
    for bad in ["x", [{"name": "1bad"}], [{"name": "a"}, {"name": "a"}], [{"name": "a", "type": "blob"}], [{"name": "a", "type": "int", "default": "x"}]]:
        with pytest.raises(BadRequest):
            g.validate_param_specs(bad)
    with pytest.raises(BadRequest):
        g.resolve_parameter_values({"bad name": 1}, None)


def test_explicit_null_default_makes_a_parameter_optional():
    specs = g.validate_param_specs([{"name": "region", "type": "string", "default": None}, {"name": "n", "type": "int", "required": False}, {"name": "m", "type": "int"}])
    assert [s["required"] for s in specs] == [False, False, True]
    assert g.resolve_parameter_values({"m": 1}, specs) == {"m": 1, "region": None, "n": None}


def test_parse_error_names_a_parameter_that_has_no_value():
    with pytest.raises(BadRequest, match="named parameter"):
        g.parse_statement("SELECT :nope")


def test_find_uses_tables_files_and_ctes():
    st = g.parse_statement("""WITH s AS (SELECT * FROM lh.sales) SELECT * FROM s JOIN dim d ON d.k = s.k
                              JOIN read_csv('Files/raw/a.csv', header=true, delim=';') c ON 1=1 JOIN read_parquet(['Files/a.parquet','Files/b.parquet']) p ON 1=1""")
    uses = g.find_uses(st.ast)
    assert sorted((u.kind, u.parts or tuple(u.paths)) for u in uses) == sorted([("table", ("lh", "sales")), ("table", ("dim",)), ("file", ("Files/raw/a.csv",)),
                                                                                  ("file", ("Files/a.parquet", "Files/b.parquet"))])
    csv = next(u for u in uses if u.fmt == "csv")
    assert csv.fmt == "csv" and csv.options == {"header": True, "delim": ";"} and csv.alias == "c"
    with pytest.raises(BadRequest, match="literal path"):
        g.find_uses(g.parse_statement("SELECT * FROM read_csv(some_col)").ast)
    three = g.find_uses(g.parse_statement("SELECT * FROM ws.lh.t").ast)[0]
    assert three.parts == ("ws", "lh", "t") and three.display == "ws.lh.t"
