# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""Review F02: the read-only SQL surface must not reach host capabilities.

Spark ``REFLECT`` / ``JAVA_METHOD`` / ``TRY_REFLECT`` call arbitrary JVM static methods, ``SELECT TRANSFORM(..) USING '<cmd>'`` runs a program, ``format.`path```
table references read arbitrary paths, ``${env:NAME}`` is substituted by Spark into the statement text before parsing. A Viewer holding only ``sql.execute`` reached all of
them. The guard now rejects them *in planning* - before a kernel or session exists - for every role, on the Spark engine, and keeps the DuckDB engine's equivalents
(file readers, ``COPY`` / ``ATTACH`` / ``INSTALL`` / ``LOAD`` / ``PRAGMA``) closed too. The pure-guard tests need no database; the service tests count the calls the
(fake) compute service receives: a rejected statement must produce none.
"""

# ruff: noqa: S608 - the statements below are attack strings, not queries built from input

from __future__ import annotations

import itertools
import random
from typing import Any

import pyarrow as pa
import pytest

from olcf.core.errors import BadRequest, Unsupported
from olcf.sql import guard as g

REJECTED = (Unsupported, BadRequest)


def plan_spark(sql: str, **params: Any) -> g.Statement:
    """Everything ``SqlService.plan`` does to a Spark statement before it talks to an engine."""
    st = g.parse_statement(sql, "spark", param_names=set(params))
    st, _ = g.bind_parameters(st, params, None)
    g.require_read_only(st, "spark")
    g.check_final_sql(g.render(st.ast, "spark", st.explain_prefix), "spark")
    return st


# ------------------------------------------------------------------------------------------------------------------ the three reproduced statements
@pytest.mark.parametrize("sql", [
    "SELECT reflect('java.lang.System', 'getProperty', 'java.version')",
    "SELECT java_method('java.lang.System', 'getProperty', 'java.version')",
    "SELECT TRANSFORM('hello') USING 'cat' AS (value)",
])
def test_the_statements_of_the_review_are_refused(sql):
    with pytest.raises(REJECTED):
        plan_spark(sql)


# ------------------------------------------------------------------------------------------------------------------ denied constructs (Spark)
DENIED_SPARK = [
    # reflection and function indirection
    "SELECT try_reflect('java.lang.Runtime', 'exec', 'id')",
    "SELECT call_function('reflect', 'java.lang.System', 'getProperty', 'java.version')",
    "SELECT call_function('java_method', 'a', 'b', 'c')",
    "SELECT * FROM t WHERE x IN (SELECT reflect('a', 'b', 'c'))",
    "SELECT (SELECT java_method('a', 'b', 'c')) AS v",
    "WITH w AS (SELECT reflect('a', 'b', 'c') AS r) SELECT * FROM w",
    "SELECT * FROM t LATERAL VIEW explode(array(reflect('a','b','c'))) e AS x",
    "SELECT CASE WHEN 1 = 1 THEN reflect('a','b','c') END",
    "SELECT lower(reflect('a','b','c'))",
    "SELECT * FROM (SELECT * FROM (SELECT reflect('a','b','c') AS r) a) b",
    "SELECT 1 UNION ALL SELECT reflect('a','b','c')",
    "SELECT max(reflect('a','b','c')) OVER (PARTITION BY x) FROM t",
    "SELECT a FROM t GROUP BY a HAVING reflect('a','b','c') = 'x'",
    "SELECT a FROM t ORDER BY reflect('a','b','c')",
    "SELECT * FROM t JOIN u ON reflect('a','b','c') = u.k",
    "SELECT from_protobuf(b, 'Msg', '/etc/descriptor.desc') FROM t",
    "SELECT http_request('x') ",
    "SELECT ai_query('model', 'prompt')",
    "SELECT * FROM read_files('/etc')",
    "SELECT IDENTIFIER('reflect')('a', 'b', 'c')",
    # script transformation
    "SELECT TRANSFORM(a, b) USING 'cat' AS (x STRING, y STRING) FROM t",
    "SELECT TRANSFORM(a, b) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' USING '/bin/sh -c id' AS (x) FROM t",
    "SELECT transform(a) USING 'python3 -c print(1)' FROM t",
    "FROM t MAP a, b USING 'cat' AS (x, y)",
    "FROM t REDUCE a, b USING 'cat' AS (x, y)",
    "SELECT * FROM (SELECT TRANSFORM(a) USING 'cat' AS (v) FROM t) s",
    "WITH w AS (SELECT TRANSFORM(a) USING 'cat' AS (v) FROM t) SELECT * FROM w",
    # path-style data source references: read arbitrary paths with the kernel's identity
    "SELECT * FROM parquet.`/etc/passwd`",
    "SELECT * FROM csv.`/etc/passwd`",
    "SELECT * FROM text.`/etc/passwd`",
    "SELECT * FROM json.`s3a://bucket/key`",
    "SELECT * FROM delta.`/tmp/x`",
    "SELECT * FROM orc.`file.orc`",
    "SELECT * FROM binaryFile.`/etc`",
    "SELECT * FROM `parquet`.`relative.parquet`",
    "SELECT * FROM PARQUET.`/etc/passwd`",
    "SELECT * FROM lh.t JOIN parquet.`/etc/passwd` p ON 1 = 1",
    "SELECT * FROM t WHERE x IN (SELECT a FROM csv.`/etc/passwd`)",
    "WITH w AS (SELECT * FROM text.`/etc/passwd`) SELECT * FROM w",
    "SELECT * FROM `/etc/passwd`",
    "SELECT * FROM lh.`../../etc/passwd`",
    "SELECT * FROM IDENTIFIER('parquet.`/etc/passwd`')",
    "SELECT * FROM identifier('t')",
    "DESCRIBE parquet.`/etc/passwd`",
    # not queries at all
    "ADD JAR /tmp/evil.jar",
    "ADD FILE /etc/passwd",
    "LIST FILE",
    "SET spark.sql.shuffle.partitions=1",
    "RESET",
    "CREATE FUNCTION f AS 'com.evil.Udf' USING JAR '/tmp/evil.jar'",
    "CALL system.drop_everything()",
    "INSERT OVERWRITE LOCAL DIRECTORY '/tmp/out' SELECT 1",
    "CREATE TABLE lh.t USING parquet LOCATION '/etc/x' AS SELECT 1",
    "CREATE TABLE lh.t USING parquet LOCATION '/etc/x' AS SELECT reflect('a','b','c')",
    "INSERT INTO lh.t SELECT reflect('a','b','c')",
    "INSERT INTO lh.t SELECT TRANSFORM(a) USING 'cat' AS (v) FROM t",
    "CREATE TABLE lh.t AS SELECT * FROM parquet.`/etc/passwd`",
    "MERGE INTO lh.t USING (SELECT reflect('a','b','c') AS k) s ON t.k = s.k WHEN MATCHED THEN DELETE",
    "EXPLAIN SELECT reflect('a','b','c')",
    "EXPLAIN SELECT TRANSFORM(a) USING 'cat' AS (v) FROM t",
    # variable substitution is performed by Spark on the statement text
    "SELECT '${env:HOME}'",
    "SELECT * FROM t WHERE k = '${system:user.dir}'",
    "SELECT '${sparkconf:spark.hadoop.fs.s3a.secret.key}'",
    "SELECT 1 /* ${env:HOME} */",
]


@pytest.mark.parametrize("sql", DENIED_SPARK)
def test_spark_host_capabilities_are_refused_in_planning(sql):
    with pytest.raises(REJECTED):
        plan_spark(sql)


def test_parameter_values_cannot_smuggle_variable_substitution():
    # the value is bound as a literal - and Spark would substitute ${env:..} in it, so the final text is checked
    with pytest.raises(REJECTED):
        plan_spark("SELECT * FROM t WHERE k = :k", k="${env:AWS_SECRET_ACCESS_KEY}")


# ------------------------------------------------------------------------------------------------------------------ still allowed
ALLOWED_SPARK = [
    "SELECT 1",
    "SELECT a, count(*) AS n, sum(b) AS s, avg(b), min(b), max(b) FROM lh.t GROUP BY a HAVING count(*) > 1 ORDER BY n DESC LIMIT 10",
    "SELECT * FROM lh.t",
    "SELECT * FROM lh.sch.t",
    "SELECT * FROM ws.lh.t",
    "SELECT * FROM `my lake`.`my-table`",
    "WITH a AS (SELECT * FROM lh.t), b AS (SELECT x FROM a) SELECT * FROM b",
    "SELECT * FROM lh.t JOIN lh.u USING (id)",
    "SELECT * FROM lh.t x JOIN lh.u y ON x.id = y.id",
    "SELECT /*+ BROADCAST(u) */ * FROM lh.t JOIN lh.u ON t.id = u.id",
    "SELECT row_number() OVER (PARTITION BY a ORDER BY b) AS rn, lag(b) OVER (ORDER BY a), sum(b) OVER (PARTITION BY a) FROM lh.t",
    "SELECT * FROM lh.t WHERE a IN (SELECT a FROM lh.u) AND EXISTS (SELECT 1 FROM lh.v WHERE v.id = t.id)",
    "SELECT transform(arr, x -> x + 1) AS inc, filter(arr, x -> x > 1), aggregate(arr, 0, (acc, x) -> acc + x), exists(arr, x -> x > 2) FROM lh.t",
    "SELECT explode(arr) FROM lh.t",
    "SELECT * FROM lh.t LATERAL VIEW explode(arr) e AS x",
    "SELECT * FROM lh.t LATERAL VIEW OUTER posexplode(arr) e AS pos, x",
    "SELECT * FROM range(10)",
    "SELECT * FROM explode(array(1, 2, 3))",
    "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS v(id, s)",
    "SELECT input_file_name(), current_user(), current_date(), current_timestamp(), uuid(), version()",
    "SELECT from_json(j, 'a INT'), to_json(named_struct('a', 1)), schema_of_json('{\"a\":1}'), from_csv(c, 'a INT'), get_json_object(j, '$.a') FROM lh.t",
    "SELECT aes_encrypt('x', '0000111122223333'), md5('x'), sha2('x', 256), hash(a), crc32('x') FROM lh.t",
    "SELECT date_trunc('month', d), datediff(d, current_date()), add_months(d, 1), to_date(s), to_timestamp(s), date_format(d, 'yyyy') FROM lh.t",
    "SELECT regexp_extract(s, '(a+)', 1), split(s, ','), concat_ws('-', a, b), lpad(s, 5, '0'), 'reflect' AS word, 'java_method' AS w2 FROM lh.t",
    "SELECT percentile_approx(b, 0.5), approx_count_distinct(a), collect_list(a), first(a), stddev(b) FROM lh.t",
    "SELECT CAST(a AS BIGINT), try_cast(s AS INT), CASE WHEN a > 1 THEN 'x' ELSE 'y' END, coalesce(a, 0), nvl(a, 1), nullif(a, 0), assert_true(true) FROM lh.t",
    "SELECT * FROM lh.t TABLESAMPLE (10 PERCENT)",
    "SELECT * FROM lh.t VERSION AS OF 3",
    "SELECT a FROM lh.t UNION ALL SELECT a FROM lh.u INTERSECT SELECT a FROM lh.v",
    "SELECT * FROM lh.t JOIN lh.u USING (id) JOIN (SELECT id, max(x) AS m FROM lh.w GROUP BY id) q USING (id)",
    "SELECT stack(2, 'a', 1, 'b', 2) AS (k, v)",
    "SELECT * FROM lh.t WHERE `transform` = 1 AND reflect_count > 2",   # names that merely look like the denied ones
    "SELECT reflect FROM lh.t",                                          # a column called reflect is not a call
    "DESCRIBE lh.t",
    "EXPLAIN SELECT * FROM lh.t",
    "SELECT * FROM lh.t WHERE s = 'it''s a $ {not-a-substitution' OR 1 = 1",
]


@pytest.mark.parametrize("sql", ALLOWED_SPARK)
def test_ordinary_spark_queries_are_still_accepted(sql):
    plan_spark(sql)


# ------------------------------------------------------------------------------------------------------------------ fuzz-ish obfuscation
_DANGEROUS_CALLS = [
    "reflect('java.lang.System', 'getProperty', 'java.version')",
    "java_method('java.lang.System', 'getProperty', 'java.version')",
    "try_reflect('java.lang.System', 'getProperty', 'java.version')",
    "call_function('reflect', 'a', 'b', 'c')",
]


def _case_variants(name: str, rng: random.Random) -> list[str]:
    mixed = "".join(c.upper() if rng.random() < 0.5 else c.lower() for c in name)
    return [name, name.upper(), name.title(), mixed, f"`{name}`", f"`{name.upper()}`"]


_SPACINGS = ["", " ", "  ", "\n", "\t", "\r\n", "/* c */", "/**/", " /* a */ ", "-- c\n", "/* reflect( */"]


def _wrap(expr: str) -> list[str]:
    return [
        f"SELECT {expr}",
        f"SELECT 1 AS a, {expr} AS b FROM lh.t",
        f"SELECT * FROM lh.t WHERE x = {expr}",
        f"SELECT * FROM (SELECT {expr} AS r) s",
        f"SELECT * FROM (SELECT * FROM (SELECT {expr} AS r) s1) s2",
        f"WITH c AS (SELECT {expr} AS r) SELECT * FROM c",
        f"WITH c AS (SELECT 1 AS k), d AS (SELECT {expr} AS r FROM c) SELECT * FROM d",
        f"SELECT /*+ BROADCAST(t) */ {expr} FROM lh.t",
        f"/* leading */ SELECT {expr} -- trailing",
        f"SELECT * FROM lh.t WHERE x IN (SELECT {expr})",
        f"SELECT * FROM lh.t WHERE EXISTS (SELECT 1 WHERE {expr} IS NOT NULL)",
        f"SELECT count(*) FROM lh.t GROUP BY {expr}",
        f"SELECT {expr}\n  FROM lh.t\n LIMIT 1;",
    ]


def test_obfuscated_reflection_is_refused_everywhere():
    rng = random.Random(20261008)
    seen = 0
    for call in _DANGEROUS_CALLS:
        name, rest = call.split("(", 1)
        for variant in _case_variants(name, rng):
            for gap in rng.sample(_SPACINGS, 5):
                for sql in _wrap(f"{variant}{gap}({rest}"):
                    seen += 1
                    with pytest.raises(REJECTED):
                        plan_spark(sql)
    assert seen > 1000


def test_obfuscated_script_transform_is_refused_everywhere():
    rng = random.Random(7)
    heads = ["TRANSFORM", "transform", "Transform", "`transform`", "TrAnSfOrM"]
    usings = ["USING", "using", "Using", "USING /* x */", "\nUSING\n", "USING\t"]
    cmds = ["'cat'", "'/bin/sh -c id'", "'python3 -c \"print(1)\"'", "\"cat\""]
    seen = 0
    for head, using, cmd in itertools.product(heads, usings, cmds):
        gap = rng.choice(_SPACINGS)
        inner = f"{head}{gap}(a, b) {using} {cmd} AS (x STRING, y STRING)"
        for sql in (f"SELECT {inner} FROM lh.t", f"SELECT * FROM (SELECT {inner} FROM lh.t) s", f"WITH c AS (SELECT {inner} FROM lh.t) SELECT * FROM c",
                    f"SELECT /*+ x */ {inner} FROM lh.t WHERE a > 1", f"/* hi */ SELECT {inner} FROM lh.t -- end"):
            seen += 1
            with pytest.raises(REJECTED):
                plan_spark(sql)
    assert seen > 500


def test_obfuscated_path_tables_are_refused_everywhere():
    fmts = ["parquet", "PARQUET", "`parquet`", "csv", "Json", "text", "orc", "delta", "binaryFile", "avro"]
    paths = ["`/etc/passwd`", "`file:///etc/passwd`", "`s3a://b/k`", "`../x`", "`relative.parquet`", "`/tmp/a b`"]
    for fmt, path in itertools.product(fmts, paths):
        for sql in (f"SELECT * FROM {fmt}.{path}", f"SELECT * FROM lh.t JOIN {fmt}.{path} q ON 1=1", f"SELECT * FROM (SELECT * FROM {fmt}.{path}) s",
                    f"WITH c AS (SELECT * FROM {fmt}.{path}) SELECT * FROM c", f"SELECT * FROM lh.t WHERE a IN (SELECT a FROM {fmt}.{path})",
                    f"SELECT * FROM {fmt} . {path}", f"SELECT * FROM {fmt}./**/{path}"):
            with pytest.raises(REJECTED):
                plan_spark(sql)


def test_allowed_queries_survive_the_same_wrapping():
    benign = ["upper(name)", "transform(arr, x -> x + 1)", "coalesce(a, 0)", "input_file_name()", "'reflect'", "count(*) OVER ()"]
    for expr in benign:
        for sql in _wrap(expr):
            plan_spark(sql)


# ------------------------------------------------------------------------------------------------------------------ unparsable Spark SQL fails closed
@pytest.mark.parametrize("sql", [
    "SELECT reflect(", "SELECT * FROM", "SELECT TRANSFORM(a) USING FROM t", "SELECT 1; SELECT reflect('a','b','c')", "SELECT $$ reflect('a','b','c') $$",
    "SELEC reflect('a','b','c')", "SELECT 1 /* unterminated reflect('a','b','c')", "SELECT 'unterminated reflect('a','b','c')",
])
def test_unparsable_or_multiple_spark_statements_fail_closed(sql):
    with pytest.raises(REJECTED):
        plan_spark(sql)


# ------------------------------------------------------------------------------------------------------------------ DuckDB: the analogous capabilities
DENIED_DUCKDB = [
    "SELECT * FROM read_csv('/etc/passwd')", "SELECT * FROM read_csv_auto('/etc/passwd')", "SELECT * FROM read_parquet('/etc/x.parquet')", "SELECT * FROM read_json('/etc/passwd')",
    "SELECT * FROM read_text('/etc/passwd')", "SELECT * FROM read_blob('/etc/passwd')", "SELECT * FROM glob('/etc/*')", "SELECT * FROM '/etc/passwd.csv'",
    "SELECT * FROM 'https://example.com/x.parquet'", "SELECT * FROM \"/etc/passwd.csv\"", "SELECT * FROM parquet_metadata('/etc/x.parquet')",
    "SELECT * FROM parquet_schema('/etc/x.parquet')", "SELECT * FROM parquet_file_metadata('/etc/x.parquet')", "SELECT * FROM parquet_kv_metadata('/etc/x.parquet')",
    "SELECT * FROM sniff_csv('/etc/passwd')", "SELECT * FROM read_ndjson('/etc/passwd')", "SELECT * FROM delta_scan('/x')", "SELECT * FROM iceberg_scan('/x')",
    "SELECT * FROM query('SELECT 1')", "SELECT * FROM query_table('t')", "SELECT getenv('HOME')", "SELECT current_setting('home_directory')",
    "SELECT * FROM duckdb_settings()", "SELECT * FROM duckdb_secrets()", "SELECT * FROM pragma_database_list()", "SELECT * FROM duckdb_extensions()",
    "COPY (SELECT 1) TO '/tmp/x.csv'", "COPY t FROM '/etc/passwd'", "ATTACH '/tmp/x.db' AS x", "DETACH x", "INSTALL httpfs", "LOAD httpfs", "FORCE INSTALL httpfs",
    "PRAGMA database_list", "SET enable_external_access = true", "RESET enable_external_access", "CALL pragma_version()", "EXPORT DATABASE '/tmp/x'",
    "IMPORT DATABASE '/tmp/x'", "CHECKPOINT", "USE other", "CREATE SECRET s (TYPE s3)", "CREATE MACRO m() AS 1", "VACUUM", "SELECT * FROM test_all_types()",
    "SELECT * FROM t, LATERAL read_csv('/etc/passwd')",
    "WITH c AS (SELECT * FROM read_text('/etc/passwd')) SELECT * FROM c",
    "SELECT * FROM (SELECT * FROM glob('/*')) s",
    "SELECT (SELECT content FROM read_text('/etc/passwd'))",
    "SELECT * FROM lh.t WHERE a IN (SELECT 1 FROM read_csv('/etc/passwd'))",
    "SELECT * FROM READ_TEXT('/etc/passwd')", "select * from Read_Blob('/etc/passwd')", "SELECT * FROM \"read_text\"('/etc/passwd')",
]


ALLOWED_DUCKDB = [
    "SELECT 1", "SELECT * FROM lh.t", "WITH a AS (SELECT * FROM lh.t) SELECT count(*) FROM a", "SELECT * FROM range(10)", "SELECT * FROM generate_series(1, 3)",
    "SELECT unnest([1, 2, 3])", "SELECT * FROM read_csv('Files/raw/x.csv', header = true)", "SELECT * FROM read_parquet('olcf://ws/lh/Files/x.parquet')",
    "SELECT * FROM read_json('Files/raw/e.json')", "DESCRIBE lh.t", "SUMMARIZE lh.t", "SELECT row_number() OVER (ORDER BY a) FROM lh.t",
    "SELECT a, sum(b) FILTER (WHERE b > 1) FROM lh.t GROUP BY ALL", "SELECT * FROM lh.t WHERE a IN (SELECT a FROM lh.u)", "FROM lh.t SELECT a",
    "SELECT json_extract(j, '$.a'), strftime(d, '%Y'), list_sort(l), regexp_matches(s, 'a') FROM lh.t",
]


@pytest.mark.parametrize("sql", ALLOWED_DUCKDB)
def test_ordinary_duckdb_queries_are_still_accepted(sql):
    st = g.parse_statement(sql, "duckdb")
    g.require_read_only(st, "duckdb")


# ------------------------------------------------------------------------------------------------------------------ service level: nothing reaches the engine
class FakeCompute:
    """Counts what reaches the pooled Spark session (= what would allocate / use a kernel)."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def run_spark_sql(self, principal, workspace_id, sql, *, lakehouse=None, limit=1000, timeout=60):  # noqa: ANN001, ANN201
        self.calls.append(sql)
        return {"columns": [{"name": "n", "type": "bigint"}], "rows": [[1]], "truncated": False, "stats": {}}


@pytest.fixture()
def env(dc):
    ws = dc.workspace("wsq")
    lh = dc.lakehouse("wsq", "lhq")
    dc.write_table(lh["id"], "customers", pa.table({"customer_id": pa.array([1, 2, 3], pa.int64()), "region": ["eu", "us", "us"]}))
    fake = FakeCompute()
    dc.ctx.register_service("compute", fake)
    return {
        "ws": ws, "lh": lh, "fake": fake,
        "viewer": dc.user("viewer-q", "Viewer", workspace_id=ws["id"]),
        "viewer_pat": dc.user("viewer-pat", "Viewer", workspace_id=ws["id"], scopes=["read"]),
        "mcp_pat": dc.user("mcp-pat", "Contributor", workspace_id=ws["id"], scopes=["mcp"]),
        "contrib": dc.user("contrib-q", "Contributor", workspace_id=ws["id"]),
    }


def run(dc, env, sql, who, engine="spark", **extra):
    return dc.post("/v1/sql/execute", json={"sql": sql, "engine": engine, "workspace_id": env["ws"]["id"], "lakehouse": "lhq", **extra}, headers=env[who] if who in env else who)


SERVICE_DENIED = [
    "SELECT reflect('java.lang.System', 'getProperty', 'java.version')",
    "SELECT java_method('java.lang.System', 'getProperty', 'java.version')",
    "SELECT TRANSFORM('hello') USING 'cat' AS (value)",
    "SELECT * FROM parquet.`/etc/passwd`",
    "SELECT '${env:HOME}'",
    "SELECT * FROM lhq.customers WHERE region = (SELECT try_reflect('java.lang.System', 'getenv', 'HOME'))",
]


def test_spark_host_capabilities_never_reach_a_kernel(dc, env):
    """Viewer, read-only token, MCP token and Contributor alike: planning refuses, and the compute service is never asked (no kernel allocated)."""
    for who in ("viewer", "viewer_pat", "mcp_pat", "contrib"):
        for sql in SERVICE_DENIED:
            r = run(dc, env, sql, who)
            assert r.status_code in (400, 403, 422), (who, sql, r.status_code, r.text)
            assert "java.version" not in r.text and "root:" not in r.text
    assert env["fake"].calls == [], "a rejected statement must not allocate or use a kernel"


def test_rejection_is_actionable_and_ordinary_queries_still_run(dc, env):
    r = run(dc, env, "SELECT reflect('java.lang.System', 'getProperty', 'java.version')", "viewer")
    assert r.status_code == 422
    err = r.json()["error"]
    assert "reflect" in err["message"].lower() and err["remediation"]
    ok = run(dc, env, "SELECT region, count(*) AS n FROM lhq.customers GROUP BY region ORDER BY n DESC", "viewer")
    assert ok.status_code == 200, ok.text
    assert len(env["fake"].calls) == 1 and "reflect" not in env["fake"].calls[0]


def test_writers_get_the_same_function_rules(dc, env):
    for sql in ("INSERT INTO lhq.customers SELECT reflect('a','b','c'), 'x'", "CREATE TABLE lhq.gold AS SELECT TRANSFORM(region) USING 'cat' AS (r) FROM lhq.customers",
                "CREATE TABLE lhq.g2 USING parquet LOCATION '/etc/x' AS SELECT 1"):
        r = run(dc, env, sql, "contrib")
        assert r.status_code in (400, 422), (sql, r.status_code, r.text)
    assert env["fake"].calls == []
    assert run(dc, env, "CREATE TABLE lhq.gold AS SELECT region, count(*) AS n FROM lhq.customers GROUP BY region", "contrib").status_code == 200


def test_variable_substitution_through_a_parameter_is_refused(dc, env):
    r = run(dc, env, "SELECT * FROM lhq.customers WHERE region = :r", "viewer", parameters={"r": "${env:AWS_SECRET_ACCESS_KEY}"})
    assert r.status_code in (400, 422) and env["fake"].calls == []


def test_duckdb_equivalents_fail_in_planning(dc, env):
    leaked = []
    for sql in DENIED_DUCKDB:
        r = run(dc, env, sql, "viewer", engine="duckdb")
        if r.status_code not in (400, 403, 404, 422) or "root:" in r.text:
            leaked.append((sql, r.status_code, r.text[:200]))
    assert not leaked, leaked


def test_duckdb_lakehouse_tables_and_files_still_work(dc, env):
    ws = env["ws"]["id"]
    r = run(dc, env, "SELECT region, count(*) AS n FROM lhq.customers GROUP BY region ORDER BY region", "viewer", engine="duckdb")
    assert r.status_code == 200 and r.json()["rows"] == [["eu", 1], ["us", 2]]
    assert ws


# ------------------------------------------------------------------------------------------------------------------ check_spark_sql: the session-command class, strict or not
SESSION_COMMANDS = ["ADD JAR /tmp/evil.jar", "ADD FILE /etc/passwd", "ADD ARCHIVE /tmp/a.zip", "LIST FILE", "LIST JAR", "RESET", "RESET spark.sql.shuffle.partitions", "CACHE TABLE lh.t",
                    "CACHE LAZY TABLE lh.t", "UNCACHE TABLE lh.t", "CLEAR CACHE", "REFRESH TABLE lh.t", "SET spark.x=1", "USE other", "CALL system.rollback('lh.t')", "OPTIMIZE lh.t",
                    "MSCK REPAIR TABLE lh.t", "RESTORE TABLE lh.t TO VERSION AS OF 1", "SELECT 1; ADD JAR /tmp/evil.jar", "  /* c */ -- c\n RESET x"]


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("sql", SESSION_COMMANDS)
def test_check_spark_sql_refuses_session_commands_regardless_of_strict(sql, strict):
    with pytest.raises(REJECTED):
        g.check_spark_sql(sql, strict=strict)


@pytest.mark.parametrize("sql", ["INSERT INTO lh.t VALUES (1)", "INSERT INTO lh.t SELECT * FROM lh.u", "CREATE TABLE lh.t2 AS SELECT * FROM lh.t", "CREATE TABLE lh.t3 (x INT) USING delta",
                                 "CREATE OR REPLACE TEMP VIEW v AS SELECT 1", "MERGE INTO lh.t USING lh.u ON t.a = u.a WHEN MATCHED THEN DELETE", "UPDATE lh.t SET a = 1", "DELETE FROM lh.t WHERE a = 1",
                                 "DROP TABLE lh.t", "ALTER TABLE lh.t ADD COLUMNS (y INT)", "TRUNCATE TABLE lh.t"])
def test_check_spark_sql_keeps_the_dml_and_ddl_a_writer_may_send(sql):
    st = g.check_spark_sql(sql, strict=False)
    assert st is not None and not st.read_only and st.kind in g.SPARK_ALLOWED_KINDS
    with pytest.raises(REJECTED):  # ... and a caller who may not write still gets nothing but reads
        g.check_spark_sql(sql, strict=True)


@pytest.mark.parametrize("sql", ["SHOW TABLES", "SHOW DATABASES", "DESCRIBE lh.t", "EXPLAIN SELECT 1", "(SELECT 1)", *ALLOWED_SPARK[:6]])
def test_check_spark_sql_keeps_reads_for_everybody(sql):
    for strict in (False, True):
        st = g.check_spark_sql(sql, strict=strict)
        assert st is not None and st.read_only


def test_check_spark_sql_fails_closed_on_text_it_cannot_read():
    """Review C01: no unparsed-text fallback - several statements and text the parser cannot read are refused for a writer exactly as for a reader."""
    for strict in (False, True):
        for sql in ("SELECT 1; SELECT 2", "SELECT 1; CREATE FUNCTION f AS 'a.B' USING JAR '/tmp/x.jar'", "SELECT 1; SET x=1", "VACUUM lh.t; SELECT 1"):
            with pytest.raises(REJECTED):
                g.check_spark_sql(sql, strict=strict)


# ====================================================================================================================================================================
# Review C01: Spark SQL functions are an ALLOWLIST, and nothing the parser cannot classify reaches a kernel (for any role)
# ====================================================================================================================================================================
def _allowed_functions() -> frozenset[str]:
    # imported lazily: a missing module must fail these tests, not the whole file
    from olcf.sql.spark_functions import SPARK_ALLOWED_FUNCTIONS

    return SPARK_ALLOWED_FUNCTIONS


UNKNOWN_FUNCTIONS = [
    "SELECT my_company_udf(a) FROM lh.t",
    "SELECT brand_new_builtin_2031(a, b) FROM lh.t",
    "SELECT * FROM lh.t WHERE registered_udf(a) = 1",
    "SELECT a FROM lh.t GROUP BY a HAVING secret_agg(b) > 1",
    "SELECT sum(my_udf(b)) OVER (PARTITION BY a) FROM lh.t",
    "SELECT upper(lower(some_python_udf(a))) FROM lh.t",
    "SELECT transform(arr, x -> udf_in_lambda(x)) FROM lh.t",
    "WITH w AS (SELECT custom_fn(a) AS c FROM lh.t) SELECT * FROM w",
    "SELECT * FROM lh.t JOIN lh.u ON udf_cmp(t.a, u.a)",
    "SELECT * FROM lh.t LATERAL VIEW custom_udtf(a) e AS x",
    "SELECT * FROM custom_table_function(1)",
    "INSERT INTO lh.t SELECT custom_fn(a) FROM lh.u",
    "CREATE TABLE lh.t2 AS SELECT custom_fn(a) AS c FROM lh.u",
    "UPDATE lh.t SET a = custom_fn(a)",
    "DELETE FROM lh.t WHERE custom_fn(a)",
    "MERGE INTO lh.t USING lh.u ON t.k = u.k WHEN MATCHED THEN UPDATE SET a = custom_fn(u.a)",
    "EXPLAIN SELECT custom_fn(a) FROM lh.t",
    "SELECT `custom_fn`(a) FROM lh.t",
    "SELECT CUSTOM_FN(a) FROM lh.t",
]

QUALIFIED_FUNCTIONS = [
    "SELECT mydb.my_udf(x) FROM lh.t",
    "SELECT default.f(x) FROM lh.t",
    "SELECT spark_catalog.default.f(x) FROM lh.t",
    "SELECT `mydb`.`my_udf`(x) FROM lh.t",
    "SELECT default.upper(x) FROM lh.t",              # even a built-in name: a qualified call is a catalog lookup, never a built-in
    "SELECT default.reflect('a', 'b', 'c')",
    "SELECT * FROM lh.t WHERE mydb.check_fn(a)",
    "INSERT INTO lh.t SELECT mydb.f(a) FROM lh.u",
    "SELECT * FROM mydb.range(3)",
    "SELECT * FROM lh.t LATERAL VIEW mydb.udtf(a) e AS x",
]


@pytest.mark.parametrize("sql", UNKNOWN_FUNCTIONS)
def test_c01_unknown_functions_are_refused(sql):
    with pytest.raises(Unsupported) as ei:
        plan_spark(sql)
    assert ei.value.remediation, "the refusal says what to do instead"


@pytest.mark.parametrize("sql", QUALIFIED_FUNCTIONS)
def test_c01_qualified_functions_are_refused(sql):
    with pytest.raises(Unsupported):
        plan_spark(sql)


@pytest.mark.parametrize("sql", [*UNKNOWN_FUNCTIONS[:4], *QUALIFIED_FUNCTIONS[:3]])
@pytest.mark.parametrize("strict", [False, True])
def test_c01_the_kernel_door_applies_the_same_function_rules(sql, strict):
    with pytest.raises(Unsupported):
        g.check_spark_sql(sql, strict=strict)


CREATE_FUNCTIONS = [
    "CREATE FUNCTION f AS 'com.evil.Udf'",
    "CREATE FUNCTION f AS 'com.evil.Udf' USING JAR '/tmp/evil.jar'",
    "CREATE OR REPLACE FUNCTION f AS 'com.evil.Udf'",
    "CREATE TEMPORARY FUNCTION f AS 'com.evil.Udf'",
    "CREATE OR REPLACE TEMPORARY FUNCTION f AS 'com.evil.Udf' USING JAR '/tmp/evil.jar'",
    "CREATE FUNCTION IF NOT EXISTS mydb.f AS 'com.evil.Udf'",
    "CREATE FUNCTION f(a INT) RETURNS INT RETURN a + 1",
    "CREATE TEMPORARY FUNCTION f(a INT) RETURNS INT RETURN a + 1",
    "CREATE PROCEDURE p() BEGIN SELECT 1; END",
    "DROP FUNCTION f",                                  # shared kernel state: a writer must not drop a registered function either
    "DROP TEMPORARY FUNCTION f",
    "DROP FUNCTION IF EXISTS mydb.f",
]


@pytest.mark.parametrize("sql", CREATE_FUNCTIONS)
def test_c01_create_function_is_refused_everywhere(sql):
    with pytest.raises(REJECTED):
        plan_spark(sql)
    for strict in (False, True):
        with pytest.raises(REJECTED):
            g.check_spark_sql(sql, strict=strict)


MULTI_STATEMENT = [
    "SELECT 1; SELECT 2",
    "SELECT 1;\nSELECT 2;",
    "INSERT INTO lh.t SELECT 1; INSERT INTO lh.t SELECT 2",
    "SELECT * FROM lh.t; DROP TABLE lh.t",
    "DROP TABLE lh.a; DROP TABLE lh.b",
    "SELECT 1;; SELECT 2",
    "CREATE TABLE lh.t2 AS SELECT 1; SELECT 2",
]

#: text the parser cannot classify as one complete statement (a writer used to get these through a first-keyword test)
UNPARSABLE_WRITER_TEXT = [
    "SELECT * FROM",
    "INSERT INTO lh.t VALUES (1",
    "DELETE FROM",
    "CREATE TABLE lh.t (",
    "SELECT unknown_udf(",
    "UPDATE lh.t SET a =",
    "MERGE INTO lh.t USING",
    "DROP",
    "SELECT 1 FROM lh.t WHERE",
]


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("sql", MULTI_STATEMENT + UNPARSABLE_WRITER_TEXT)
def test_c01_check_spark_sql_has_no_unparsed_text_fallback(sql, strict):
    with pytest.raises(REJECTED):
        g.check_spark_sql(sql, strict=strict)  # strict or not: never None, never "judged fragment by fragment"


def test_c01_check_spark_sql_always_returns_a_parsed_statement():
    for sql in ("SELECT 1", "INSERT INTO lh.t VALUES (1)", "SHOW TABLES", "DESCRIBE lh.t", "DROP TABLE lh.t"):
        st = g.check_spark_sql(sql, strict=False)
        assert isinstance(st, g.Statement) and st.kind in g.SPARK_ALLOWED_KINDS


@pytest.mark.parametrize("sql", MULTI_STATEMENT + UNPARSABLE_WRITER_TEXT)
def test_c01_planning_refuses_unparsable_and_multiple_statements(sql):
    with pytest.raises(REJECTED):
        plan_spark(sql)


@pytest.mark.parametrize("who", ["viewer", "contrib", "admin"])
def test_c01_unparsable_or_multiple_statements_never_reach_a_kernel_for_any_role(dc, env, who):
    """The service path: viewer, Contributor (writer) and an OrgAdmin all get a refusal and the (fake) compute service is never asked."""
    env["admin"] = dc.admin
    for sql in MULTI_STATEMENT + UNPARSABLE_WRITER_TEXT:
        r = run(dc, env, sql, who)
        assert r.status_code in (400, 403, 422), (who, sql, r.status_code, r.text)
    assert env["fake"].calls == []


@pytest.mark.parametrize("who", ["viewer", "contrib", "admin"])
def test_c01_custom_and_qualified_functions_never_reach_a_kernel_for_any_role(dc, env, who):
    env["admin"] = dc.admin
    for sql in ("SELECT my_company_udf(region) FROM lhq.customers", "SELECT mydb.my_udf(region) FROM lhq.customers", "CREATE FUNCTION f AS 'com.evil.Udf'",
                "INSERT INTO lhq.customers SELECT my_company_udf(customer_id), region FROM lhq.customers"):
        r = run(dc, env, sql, who)
        assert r.status_code in (400, 403, 422), (who, sql, r.status_code, r.text)
    assert env["fake"].calls == []


def test_c01_allowlist_and_denylist_are_disjoint():
    allowed = _allowed_functions()
    assert isinstance(allowed, frozenset) and len(allowed) > 300
    assert not allowed & g.SPARK_DENIED_FUNCTIONS, sorted(allowed & g.SPARK_DENIED_FUNCTIONS)
    assert not [n for n in allowed if n.startswith(g.SPARK_DENIED_FUNCTION_PREFIXES)]
    assert all(n == n.lower() and n.replace("_", "").isalnum() for n in allowed), "names are lowercase identifiers"
    for table_fn in g.SPARK_ALLOWED_TABLE_FUNCTIONS:
        assert table_fn in allowed, f"table function {table_fn} must also be a known function"


@pytest.mark.parametrize("name", sorted(g.SPARK_DENIED_FUNCTIONS))
def test_c01_known_dangerous_functions_keep_their_explicit_message(name):
    with pytest.raises(Unsupported) as ei:
        plan_spark(f"SELECT {name}('a', 'b', 'c')")
    assert "not allowed" in str(ei.value) and "approved" not in str(ei.value)


@pytest.mark.parametrize("name", ["read_files", "http_get", "ai_query", "remote_thing", "READ_KAFKA"])
def test_c01_denied_prefixes_keep_their_explicit_message(name):
    with pytest.raises(Unsupported) as ei:
        plan_spark(f"SELECT {name}('x')")
    assert "not allowed" in str(ei.value)


#: function-call syntax names: plain ``name(args)``; the rest of the allowlist is exercised by the sweep below
POSITIVE_FUNCTIONS = [
    "abs", "acos", "add_months", "aggregate", "any_value", "approx_count_distinct", "approx_percentile", "array", "array_agg", "array_contains", "array_distinct", "array_except",
    "array_intersect", "array_join", "array_max", "array_min", "array_position", "array_remove", "array_repeat", "array_size", "array_sort", "array_union", "arrays_zip", "ascii",
    "assert_true", "avg", "base64", "bin", "bit_and", "bit_count", "bool_and", "btrim", "cardinality", "ceil", "char_length", "coalesce", "collect_list", "collect_set", "concat",
    "concat_ws", "contains", "conv", "corr", "cos", "count", "count_if", "covar_pop", "crc32", "cume_dist", "current_catalog", "current_database", "current_date", "current_timestamp",
    "current_timezone", "current_user", "date_add", "date_diff", "date_format", "date_sub", "date_trunc", "datediff", "day", "dayofweek", "decode", "dense_rank", "element_at",
    "elt", "encode", "endswith", "exp", "explode", "explode_outer", "factorial", "filter", "first", "first_value", "flatten", "floor", "forall", "format_number", "format_string",
    "from_csv", "from_json", "from_unixtime", "from_utc_timestamp", "from_xml", "get_json_object", "greatest", "grouping_id", "hash", "hex", "hour", "hypot", "ifnull", "initcap", "inline",
    "input_file_name", "instr", "isnan", "isnotnull", "isnull", "json_array_length", "json_object_keys", "json_tuple", "kurtosis", "lag", "last", "last_day", "last_value", "lead",
    "least", "left", "length", "levenshtein", "ln", "locate", "log", "log10", "lower", "lpad", "ltrim", "make_date", "make_timestamp", "map", "map_concat", "map_entries",
    "map_filter", "map_from_arrays", "map_keys", "map_values", "mask", "max", "max_by", "md5", "mean", "median", "min", "min_by", "minute", "mod", "mode", "month",
    "months_between", "named_struct", "nanvl", "next_day", "nth_value", "ntile", "nullif", "nvl", "nvl2", "octet_length", "overlay", "parse_json", "parse_url", "percent_rank",
    "percentile", "percentile_approx", "pmod", "posexplode", "position", "pow", "quarter", "radians", "rand", "randn", "rank", "regexp_extract", "regexp_extract_all",
    "regexp_replace", "repeat", "replace", "reverse", "right", "rint", "round", "row_number", "rpad", "rtrim", "schema_of_json", "second", "sentences", "sequence", "sha", "sha1",
    "sha2", "shuffle", "sign", "sin", "size", "skewness", "slice", "sort_array", "soundex", "space", "spark_partition_id", "split", "split_part", "sqrt", "stack", "startswith",
    "std", "stddev", "stddev_samp", "str_to_map", "struct", "substr", "substring", "substring_index", "sum", "tan", "to_csv", "to_date", "to_json", "to_timestamp",
    "to_unix_timestamp", "to_utc_timestamp", "to_xml", "transform", "transform_keys", "transform_values", "translate", "trim", "trunc", "try_add", "try_avg",
    "try_divide", "try_element_at", "try_parse_json", "try_sum", "typeof", "ucase", "unbase64", "unhex", "unix_timestamp", "upper", "uuid", "var_pop", "var_samp", "variance",
    "variant_get", "version", "weekofyear", "width_bucket", "xxhash64", "year", "zip_with",
]


def test_c01_the_positive_corpus_is_in_the_allowlist():
    missing = sorted(set(POSITIVE_FUNCTIONS) - _allowed_functions())
    assert not missing, missing


@pytest.mark.parametrize("name", POSITIVE_FUNCTIONS)
def test_c01_allowed_functions_are_accepted(name):
    for args in ("a", "a, b", "a, b, c", ""):  # sqlglot builds typed nodes with a fixed arity for many built-ins: try the usual ones
        try:
            st = g.parse_statement(f"SELECT {name}({args}) FROM lh.t", "spark")
        except BadRequest:
            continue
        assert st.kind == "select"
        return
    pytest.fail(f"{name}() does not parse with any usual arity: use a dedicated sweep query for it")


def test_c01_parser_internal_errors_are_a_clean_refusal():
    """sqlglot raises ValueError / IndexError (not SqlglotError) for some malformed calls: that must be a BadRequest like any other parse failure, for every role."""
    for sql in ("SELECT date_trunc() FROM lh.t", "SELECT date_trunc(,) FROM lh.t"):
        for strict in (False, True):
            with pytest.raises(REJECTED):
                g.check_spark_sql(sql, strict=strict)


#: Ordinary, realistic queries: sqlglot normalises many of these into typed nodes (Sum, Cast, TryCast, Case, If, Coalesce, TimestampTrunc, TsOrDsAdd, ...) whose internal name is not
#: the Spark spelling; none of them may be mistaken for an unapproved function.
SWEEP_QUERIES = [
    "SELECT region, sum(amount) AS s, count(*) AS n, count(DISTINCT customer_id) AS d, avg(amount), min(amount), max(amount) FROM lh.orders GROUP BY region",
    "SELECT CAST(a AS INT), CAST(a AS STRING), a::bigint, TRY_CAST(a AS DOUBLE), CAST(a AS DECIMAL(10, 2)), CAST(d AS DATE), CAST(ts AS TIMESTAMP) FROM lh.t",
    "SELECT CASE WHEN a > 1 THEN 'x' WHEN a > 0 THEN 'y' ELSE 'z' END, if(a > 1, 1, 0), coalesce(a, 0), nvl(a, 1), ifnull(a, 2), nullif(a, 0), nvl2(a, 1, 2) FROM lh.t",
    "SELECT upper(s), lower(s), ucase(s), lcase(s), trim(s), ltrim(s), rtrim(s), length(s), char_length(s), substring(s, 1, 2), substr(s, 2), left(s, 2), right(s, 2) FROM lh.t",
    "SELECT concat(a, b), a || b, concat_ws('-', a, b), replace(s, 'a', 'b'), regexp_replace(s, 'a', 'b'), regexp_extract(s, '(a)', 1), s RLIKE 'x', s LIKE 'x%' FROM lh.t",
    "SELECT split(s, ','), lpad(s, 5, '0'), rpad(s, 5, '0'), repeat(s, 2), reverse(s), instr(s, 'a'), locate('a', s), initcap(s), translate(s, 'a', 'b'), format_number(a, 2) FROM lh.t",
    "SELECT year(d), month(d), day(d), dayofweek(d), hour(ts), minute(ts), second(ts), quarter(d), weekofyear(d), last_day(d) FROM lh.t",
    "SELECT date_trunc('month', ts), trunc(d, 'MM'), date_add(d, 1), date_sub(d, 1), datediff(d, e), add_months(d, 1), months_between(d, e), next_day(d, 'Mon') FROM lh.t",
    "SELECT to_date(s), to_date(s, 'yyyy-MM-dd'), to_timestamp(s), unix_timestamp(), unix_timestamp(s), from_unixtime(1), date_format(d, 'yyyy'), make_date(2020, 1, 1) FROM lh.t",
    "SELECT current_date(), current_date, current_timestamp(), current_timestamp, now(), localtimestamp(), current_timezone(), extract(year FROM d), extract(month FROM ts) FROM lh.t",
    "SELECT d + INTERVAL 1 DAY, d - INTERVAL '2' MONTH, ts + make_interval(0, 0, 0, 1) FROM lh.t",
    "SELECT abs(a), ceil(a), ceiling(a), floor(a), round(a, 2), bround(a), sqrt(a), pow(a, 2), power(a, 2), exp(a), ln(a), log(a), log10(a), log2(a), mod(a, 2), a % 2, a DIV 2, pmod(a, 2) FROM lh.t",
    "SELECT greatest(a, b), least(a, b), sign(a), signum(a), sin(a), cos(a), tan(a), rand(), randn(), rand(1), hash(a), xxhash64(a), md5(s), sha1(s), sha2(s, 256), crc32(s) FROM lh.t",
    "SELECT a & b, a | b, a ^ b, ~a, shiftleft(a, 1), shiftright(a, 1), bit_count(a), bit_and(a), bit_or(a), a <=> b, a IS NULL, a IS NOT NULL, a BETWEEN 1 AND 2, a IN (1, 2) FROM lh.t",
    "SELECT array(1, 2, 3), array_contains(arr, 1), size(arr), element_at(arr, 1), arr[0], m['k'], map('a', 1), map_keys(m), map_values(m), sort_array(arr), array_distinct(arr), flatten(arrs) FROM lh.t",
    "SELECT transform(arr, x -> x + 1), filter(arr, x -> x > 1), aggregate(arr, 0, (a, b) -> a + b), reduce(arr, 0, (a, b) -> a + b), exists(arr, x -> x > 1), forall(arr, x -> x > 0), zip_with(a, b, (x, y) -> x + y) FROM lh.t",
    "SELECT named_struct('a', 1, 'b', 2), struct(a, b), s.a, from_json(j, 'a INT'), to_json(s), get_json_object(j, '$.a'), json_tuple(j, 'a'), schema_of_json(j) FROM lh.t",
    "SELECT row_number() OVER (PARTITION BY a ORDER BY b), rank() OVER (ORDER BY b), dense_rank() OVER (ORDER BY b), lag(b, 1) OVER (ORDER BY a), lead(b) OVER (ORDER BY a), ntile(4) OVER (ORDER BY a) FROM lh.t",
    "SELECT sum(b) OVER (PARTITION BY a ORDER BY c ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), first_value(b) OVER (ORDER BY c), last(b) OVER (ORDER BY c), percent_rank() OVER (ORDER BY b) FROM lh.t",
    "SELECT collect_list(a), collect_set(a), array_agg(a), first(a), last(a), first(a, true), max_by(a, b), min_by(a, b), stddev(a), stddev_pop(a), variance(a), var_pop(a), corr(a, b), mean(a) FROM lh.t",
    "SELECT percentile(a, 0.5), percentile_approx(a, 0.5), approx_count_distinct(a), median(a), mode(a), any_value(a), count_if(a > 1), bool_and(a), bool_or(a), every(a), some(a), any(a) FROM lh.t",
    "SELECT sum(a) FILTER (WHERE b > 1), count(*) FILTER (WHERE b IS NULL) FROM lh.t",
    "SELECT a, grouping(a), grouping_id(a, b), sum(c) FROM lh.t GROUP BY ROLLUP (a, b)",
    "SELECT a, sum(c) FROM lh.t GROUP BY CUBE (a, b)",
    "SELECT a, sum(c) FROM lh.t GROUP BY a GROUPING SETS ((a), ())",
    "SELECT * FROM lh.t LATERAL VIEW explode(arr) e AS x LATERAL VIEW OUTER posexplode(arr2) p AS pos, y",
    "SELECT explode(arr), posexplode(arr), inline(arr_of_structs), stack(2, 'a', 1, 'b', 2) FROM lh.t",
    "SELECT * FROM range(10), range(1, 10, 2)",
    "SELECT * FROM explode(array(1, 2)), posexplode(array(1)), inline(array(struct(1, 2))), stack(1, 2, 3), json_tuple('{\"a\":1}', 'a'), sequence(1, 3)",
    "SELECT sequence(1, 10), sequence(to_date('2020-01-01'), to_date('2020-01-31'), INTERVAL 1 DAY), uuid(), version(), typeof(a), monotonically_increasing_id(), spark_partition_id() FROM lh.t",
    "SELECT current_user(), user(), current_database(), current_catalog(), input_file_name(), assert_true(a > 0), raise_error('x') FROM lh.t",
    "SELECT base64(s), unbase64(s), hex(s), unhex(s), encode(s, 'utf-8'), decode(b, 'utf-8'), aes_encrypt(s, k), aes_decrypt(b, k), url_encode(s), url_decode(s), parse_url(s, 'HOST') FROM lh.t",
    "SELECT parse_json(s), try_parse_json(s), variant_get(v, '$.a', 'int'), try_variant_get(v, '$.a', 'int'), is_variant_null(v), schema_of_variant(v), to_variant_object(s) FROM lh.t",
    "SELECT from_csv(s, 'a INT'), to_csv(st), schema_of_csv(s), from_xml(s, 'a INT'), to_xml(st), schema_of_xml(s) FROM lh.t",
    "SELECT try_add(a, b), try_subtract(a, b), try_multiply(a, b), try_divide(a, b), try_element_at(arr, 1), try_to_number(s, '99'), try_to_timestamp(s), try_sum(a), try_avg(a) FROM lh.t",
    "SELECT * FROM lh.t WHERE a IN (SELECT a FROM lh.u) AND EXISTS (SELECT 1 FROM lh.v WHERE v.k = t.k) AND NOT EXISTS (SELECT 1 FROM lh.w)",
    "SELECT * FROM lh.a JOIN lh.b USING (id) LEFT JOIN lh.c ON a.id = c.id AND coalesce(c.x, 0) > 0 WHERE upper(a.s) LIKE 'A%' ORDER BY a.id DESC NULLS LAST LIMIT 5",
    "WITH x AS (SELECT a, row_number() OVER (ORDER BY b) AS rn FROM lh.t) SELECT * FROM x WHERE rn <= 10 UNION ALL SELECT a, 0 FROM lh.u",
    "SELECT /*+ BROADCAST(u) */ t.*, u.x FROM lh.t JOIN lh.u ON t.id = u.id",
    "SELECT * FROM lh.t TABLESAMPLE (10 PERCENT)",
    "SELECT * FROM lh.t PIVOT (sum(v) FOR k IN ('a', 'b'))",
    "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS v(id, s)",
    "INSERT INTO lh.t SELECT upper(s), CAST(a AS INT), coalesce(b, 0) FROM lh.u",
    "INSERT OVERWRITE TABLE lh.t SELECT a, sum(b) FROM lh.u GROUP BY a",
    "CREATE TABLE lh.t2 USING delta AS SELECT a, date_trunc('day', ts) AS d, count(*) AS n FROM lh.t GROUP BY 1, 2",
    "CREATE OR REPLACE TEMP VIEW v AS SELECT a, sum(b) AS s FROM lh.t GROUP BY a",
    "CREATE TABLE lh.t3 (id BIGINT, s STRING, ts TIMESTAMP) USING delta PARTITIONED BY (ts)",
    "UPDATE lh.t SET a = coalesce(a, 0) + 1, s = upper(s) WHERE b > 0",
    "DELETE FROM lh.t WHERE a IS NULL OR length(s) = 0",
    "MERGE INTO lh.t USING (SELECT * FROM lh.u) s ON t.k = s.k WHEN MATCHED THEN UPDATE SET a = s.a WHEN NOT MATCHED THEN INSERT (k, a) VALUES (s.k, s.a)",
    "ALTER TABLE lh.t ADD COLUMNS (x INT)",
    "DROP TABLE IF EXISTS lh.t",
    "TRUNCATE TABLE lh.t",
    "SHOW TABLES", "DESCRIBE lh.t", "EXPLAIN SELECT upper(s) FROM lh.t",
]


@pytest.mark.parametrize("sql", SWEEP_QUERIES)
def test_c01_ordinary_queries_are_not_mistaken_for_unapproved_functions(sql):
    for strict in (False, True):
        try:
            st = g.check_spark_sql(sql, strict=strict)
        except Unsupported as exc:
            if strict and "only read-only statements" in str(exc):
                continue  # a write, correctly refused for a caller without sql.write
            pytest.fail(f"{sql!r} rejected: {exc}")
        assert isinstance(st, g.Statement)
