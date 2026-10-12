# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Review F02 at the kernel door: ``ComputeService.run_spark_sql`` refuses host capabilities (and, for callers without ``sql.write``, anything that is not a single
read) *before* a pooled SQL session is looked up or created - so a Viewer can never allocate a kernel for such a statement, whichever API got it there."""

from __future__ import annotations

import pytest
from sqlalchemy import func, select

from olcf.core.errors import BadRequest, Unsupported

PW = "Correct-Horse-9-Battery"


def _session_count(st) -> int:
    from olcf.compute import schema as C

    with st.ctx.db.connect() as conn:
        return int(conn.execute(select(func.count()).select_from(C.sessions)).scalar_one())


def _principal(st, ws: str, role: str):
    from olcf.api.deps import authenticate

    h = st.admin if role == "OrgAdmin" else st.user(role, ws)
    return authenticate(st.ctx, h["Authorization"].split(" ", 1)[1])


@pytest.mark.parametrize("sql", [
    "SELECT reflect('java.lang.System', 'getProperty', 'java.version')",
    "SELECT java_method('java.lang.System', 'getProperty', 'java.version')",
    "SELECT TRANSFORM('hello') USING 'cat' AS (value)",
    "SELECT * FROM parquet.`/etc/passwd`",
    "SELECT '${env:HOME}'",
    "SELECT 1; SELECT 2",
])
def test_viewer_statements_are_refused_before_any_session_exists(fleet, sql):
    st = fleet
    ws = st.workspace()
    viewer = _principal(st, ws, "Viewer")
    before = _session_count(st)
    with pytest.raises((Unsupported, BadRequest)):
        st.compute.run_spark_sql(viewer, ws, sql, limit=10, timeout=5)
    assert _session_count(st) == before


def test_a_viewer_cannot_smuggle_a_write_behind_a_with_clause(fleet):
    from olcf.core.errors import Forbidden

    st = fleet
    ws = st.workspace()
    viewer = _principal(st, ws, "Viewer")
    before = _session_count(st)
    with pytest.raises(Forbidden):  # the old first-word test called ``WITH .. INSERT`` a read
        st.compute.run_spark_sql(viewer, ws, "WITH w AS (SELECT 1 AS a) INSERT INTO lh.t SELECT * FROM w", limit=10, timeout=5)
    assert _session_count(st) == before


def test_a_writer_is_refused_host_capabilities_too(fleet):
    st = fleet
    ws = st.workspace()
    contrib = _principal(st, ws, "Contributor")
    before = _session_count(st)
    for sql in ("SELECT reflect('a', 'b', 'c')", "INSERT INTO lh.t SELECT TRANSFORM(a) USING 'cat' AS (v) FROM s", "SELECT * FROM csv.`/etc/passwd`"):
        with pytest.raises((Unsupported, BadRequest)):
            st.compute.run_spark_sql(contrib, ws, sql, limit=10, timeout=5)
    assert _session_count(st) == before


# ------------------------------------------------------------------------------------------------------------------ session commands (ADD JAR, RESET, CACHE TABLE ...)
#: sqlglot hands these back as a generic ``Command`` or a bare node, which the guard used to classify as "unsupported, not read-only": a caller with ``sql.write`` then passed the
#: permission check and the text reached the shared kernel.  The guard now refuses the whole class for every caller of ``ComputeService.run_spark_sql``.
SESSION_COMMANDS = [
    "ADD JAR /tmp/evil.jar",
    "ADD FILE /etc/passwd",
    "ADD ARCHIVE /tmp/a.zip",
    "LIST FILE",
    "RESET",
    "RESET spark.sql.shuffle.partitions",
    "CACHE TABLE lh.t",
    "UNCACHE TABLE lh.t",
    "CLEAR CACHE",
    "REFRESH TABLE lh.t",
    "SET spark.sql.shuffle.partitions=1",
    "CALL system.rollback('lh.t')",
    "OPTIMIZE lh.t",
    "SELECT 1; ADD JAR /tmp/evil.jar",
    "/* innocent */ RESET spark.hadoop.fs.s3a.secret.key",
]


@pytest.mark.parametrize("role", ["OrgAdmin", "Contributor", "Viewer"])
def test_session_commands_never_reach_a_kernel_whoever_asks(fleet, role):
    st = fleet
    ws = st.workspace()
    caller = _principal(st, ws, role)  # one login per role: the login endpoint is rate limited
    before = _session_count(st)
    for sql in SESSION_COMMANDS:
        # C01: no unparsed fallback - a command the parser can classify is Unsupported, text it cannot read (``RESET x``) or several statements is a BadRequest; all carry a remediation
        with pytest.raises((Unsupported, BadRequest)) as ei:
            st.compute.run_spark_sql(caller, ws, sql, limit=10, timeout=5)
        assert ei.value.remediation, f"{sql}: the refusal says what to send instead"
        assert _session_count(st) == before, f"{sql}: no pooled session may be created for a refused statement"


def test_writers_keep_the_dml_and_ddl_the_guard_allows_and_everybody_keeps_show(fleet):
    from olcf.core.errors import Forbidden

    st = fleet
    ws = st.workspace()
    a = st.agent("sql-guard-writer")
    ok = {"columns": [{"name": "n", "type": "int"}], "rows": [[1]]}
    statements = ["insert into t values (1)", "create table t2 as select 1 as n", "create table t3 (x int) using delta", "merge into t using s on t.id = s.id when matched then delete",
                  "update t set x = 1", "delete from t where x = 2", "drop table t3", "show tables", "describe t", "select 1"]
    for sql in statements:
        a.sql_results[sql] = ok
    st.wait_worker(a)
    admin = _principal(st, ws, "OrgAdmin")
    contributor = _principal(st, ws, "Contributor")
    viewer = _principal(st, ws, "Viewer")
    for who in (admin, contributor):
        for sql in statements:
            assert st.compute.run_spark_sql(who, ws, sql, limit=10, timeout=30)["rows"] == [[1]], sql
    # a viewer reads (SELECT, SHOW, DESCRIBE) but still cannot write
    for sql in ("select 1", "show tables", "describe t"):
        assert st.compute.run_spark_sql(viewer, ws, sql, limit=10, timeout=30)["rows"] == [[1]], sql
    for sql in ("insert into t values (1)", "drop table t3"):
        with pytest.raises(Forbidden):
            st.compute.run_spark_sql(viewer, ws, sql, limit=10, timeout=30)


# ------------------------------------------------------------------------------------------------------------------ review C01: no unparsed-text fallback, function allowlist
#: text the parser cannot classify as exactly one supported statement: a writer used to be waved through (first-keyword test) - now every role is refused, before any session exists
UNCLASSIFIABLE = [
    "SELECT 1; SELECT 2",
    "INSERT INTO lh.t SELECT 1; INSERT INTO lh.t SELECT 2",
    "SELECT * FROM",
    "INSERT INTO lh.t VALUES (1",
    "CREATE TABLE lh.t (",
    "DELETE FROM",
    "SELECT unknown_udf(",
    "SELECT 1; DROP TABLE lh.t",
]

#: unapproved, user-defined and qualified functions, and function creation
UNAPPROVED = [
    "SELECT my_company_udf(a) FROM lh.t",
    "SELECT mydb.my_udf(a) FROM lh.t",
    "SELECT default.f(a) FROM lh.t",
    "INSERT INTO lh.t SELECT my_company_udf(a) FROM lh.u",
    "CREATE FUNCTION f AS 'com.evil.Udf'",
    "CREATE TEMPORARY FUNCTION f AS 'com.evil.Udf' USING JAR '/tmp/evil.jar'",
]


@pytest.mark.parametrize("role", ["OrgAdmin", "Contributor", "Viewer"])
def test_c01_unclassifiable_and_unapproved_statements_never_reach_a_kernel_whoever_asks(fleet, role):
    st = fleet
    ws = st.workspace()
    caller = _principal(st, ws, role)
    before = _session_count(st)
    for sql in (*UNCLASSIFIABLE, *UNAPPROVED):
        with pytest.raises((Unsupported, BadRequest)):
            st.compute.run_spark_sql(caller, ws, sql, limit=10, timeout=5)
        assert _session_count(st) == before, f"{sql}: no pooled session may be created for a refused statement"


def test_c01_approved_functions_still_run_for_writers_and_readers(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("sql-guard-c01")
    sql = "select upper(s), cast(a as int), coalesce(b, 0), count(*) over () from t"
    a.sql_results[sql] = {"columns": [{"name": "n", "type": "int"}], "rows": [[1]]}
    st.wait_worker(a)
    for role in ("OrgAdmin", "Contributor", "Viewer"):
        assert st.compute.run_spark_sql(_principal(st, ws, role), ws, sql, limit=10, timeout=30)["rows"] == [[1]]
