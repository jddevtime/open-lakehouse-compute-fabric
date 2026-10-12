# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""SQL service end to end through the REST API: DuckDB execution, read-only enforcement, limits, cancel/timeout, history,
authorisation across workspaces, files / Iceberg registration, Spark delegation (fake compute service), exports, deep
health and saved queries.  No Spark/JVM is started."""

from __future__ import annotations

import csv
import io
import json
import re
import threading
import time
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


def sql(dc, body: dict[str, Any], headers=None, expect: int = 200) -> dict[str, Any]:
    r = dc.post("/v1/sql/execute", json=body, headers=headers or dc.admin)
    assert r.status_code == expect, r.text
    return r.json()


@pytest.fixture()
def env(dc):
    """ws1/lh1 (customers, sales[6000 rows], Files/raw/*) + ws2/lh2 (secret table) + viewers."""
    ws1, ws2 = dc.workspace("ws1"), dc.workspace("ws2")
    lh1, lh2 = dc.lakehouse("ws1", "lh1"), dc.lakehouse("ws2", "lh2")
    dc.write_table(lh1["id"], "customers", pa.table({"customer_id": pa.array([1, 2, 3], pa.int64()), "region": ["eu", "us", "us"], "name": ["a", "=b", "c"]}))
    n = 6000
    dc.write_table(lh1["id"], "sales", pa.table({"order_id": pa.array(range(n), pa.int64()), "customer_id": pa.array([i % 3 + 1 for i in range(n)], pa.int64()),
                                                  "amount": pa.array([float(i % 100) for i in range(n)])}))
    dc.write_table(lh2["id"], "secrets", pa.table({"k": ["top"], "v": [42]}))
    h = dc.admin
    csv_body = b"id,city,pop\n1,Paris,2100000\n2,Berlin,3600000\n3,Rome,2800000\n"
    assert dc.put(f"/v1/lakehouses/{lh1['id']}/files/content", params={"path": "Files/raw/cities.csv"}, content=csv_body, headers=h).status_code == 201
    buf = io.BytesIO()
    pq.write_table(pa.table({"x": [1, 2, 3, 4], "y": ["a", "b", "c", "d"]}), buf)
    assert dc.put(f"/v1/lakehouses/{lh1['id']}/files/content", params={"path": "Files/raw/p/part-0.parquet"}, content=buf.getvalue(), headers=h).status_code == 201
    assert dc.put(f"/v1/lakehouses/{lh1['id']}/files/content", params={"path": "Files/raw/e.json"}, content=b'{"a":1,"b":"x"}\n{"a":2,"b":"y"}\n', headers=h).status_code == 201
    v1 = dc.user("viewer1", "Viewer", workspace_id=ws1["id"])
    contrib1 = dc.user("contrib1", "Contributor", workspace_id=ws1["id"])
    return {"ws1": ws1, "ws2": ws2, "lh1": lh1, "lh2": lh2, "viewer": v1, "contrib": contrib1}


# ----------------------------------------------------------------------------- basics
def test_select_typed_columns_params_and_stats(dc, env):
    r = sql(dc, {"sql": "SELECT c.region, count(*) AS n, sum(s.amount) AS total, max(s.order_id) AS last FROM lh1.sales s JOIN lh1.customers c USING (customer_id) "
                        "WHERE s.amount >= :min GROUP BY c.region ORDER BY c.region", "lakehouse": env["lh1"]["id"], "parameters": {"min": 50}})
    assert [c["name"] for c in r["columns"]] == ["region", "n", "total", "last"]
    assert [c["kind"] for c in r["columns"]] == ["string", "integer", "float", "integer"]
    assert r["rows"][0][0] == "eu" and r["rows"][1][0] == "us" and sum(row[1] for row in r["rows"]) == 3000
    assert r["truncated"] is False and r["row_count"] == 2 and r["engine"] == "duckdb" and r["statement"] == "select"
    assert r["stats"]["rows_returned"] == 2 and r["stats"]["tables"] == ["lh1.sales", "lh1.customers"] or sorted(r["stats"]["tables"]) == ["lh1.customers", "lh1.sales"]
    assert r["query_id"].startswith("qry_")


def test_three_part_names_and_workspace_qualified_names(dc, env):
    for ref in ("lh1.customers", "ws1.lh1.customers", "lh1.dbo.customers"):
        body = {"sql": f"SELECT count(*) AS n FROM {ref}", "workspace_id": env["ws1"]["id"]}  # noqa: S608
        r = dc.post("/v1/sql/execute", json=body, headers=dc.admin)
        if ref.endswith("dbo.customers"):
            assert r.status_code in (200, 404), r.text   # a non-schema lakehouse may refuse schema qualification, never crash
        else:
            assert r.status_code == 200 and r.json()["rows"] == [[3]], r.text
    # the lakehouse alone sets the workspace; no workspace_id needed
    assert sql(dc, {"sql": "SELECT count(*) FROM customers", "lakehouse": "lh1", "workspace_id": env["ws1"]["id"]})["rows"] == [[3]]


def test_missing_workspace_id_gets_an_actionable_message(dc, env):
    """FIX Q: workspace_id stays optional (statements without tables work), but a table name cannot be resolved without it - the error says what to send."""
    assert sql(dc, {"sql": "SELECT 1 AS one"})["rows"] == [[1]]
    for ref in ("customers", "lh1.customers"):
        r = dc.post("/v1/sql/execute", json={"sql": f"SELECT * FROM {ref}"}, headers=dc.admin)  # noqa: S608
        err = r.json()["error"]
        assert r.status_code == 400 and "workspace_id" in err["message"] and ref in err["message"], r.text
        assert "workspace_id" in err["remediation"] and "<workspace>.<lakehouse>.<table>" in err["remediation"]
        assert err["details"]["missing"] == "workspace_id"
    # the fully qualified form needs no workspace_id
    assert sql(dc, {"sql": "SELECT count(*) FROM ws1.lh1.customers"})["rows"] == [[3]]
    # and with the workspace the same statement works
    assert sql(dc, {"sql": "SELECT count(*) FROM lh1.customers", "workspace_id": env["ws1"]["id"]})["rows"] == [[3]]


def test_unknown_table_gives_a_did_you_mean(dc, env):
    r = dc.post("/v1/sql/execute", json={"sql": "SELECT * FROM lh1.custmers", "lakehouse": env["lh1"]["id"]}, headers=dc.admin)
    assert r.status_code == 404 and "customers" in json.dumps(r.json())


def test_sql_errors_are_400_not_500(dc, env):
    for bad in ("SELEC 1", "SELECT nope FROM lh1.customers", "SELECT 1; SELECT 2", ""):
        r = dc.post("/v1/sql/execute", json={"sql": bad or " ", "lakehouse": env["lh1"]["id"]}, headers=dc.admin)
        assert r.status_code in (400, 422), (bad, r.status_code, r.text)
        assert r.json()["error"]["message"]


def test_truncation_flag_and_row_cap_warning(dc, env):
    r = sql(dc, {"sql": "SELECT order_id FROM lh1.sales ORDER BY order_id", "lakehouse": "lh1", "workspace_id": env["ws1"]["id"], "max_rows": 100})
    assert r["row_count"] == 100 and r["truncated"] is True and r["rows"][-1] == [99]
    full = sql(dc, {"sql": "SELECT order_id FROM lh1.sales", "lakehouse": "lh1", "workspace_id": env["ws1"]["id"], "max_rows": 6000})
    assert full["row_count"] == 6000 and full["truncated"] is False
    dc.ctx.service("settings").set("sql.max_rows_cap", 50)
    capped = sql(dc, {"sql": "SELECT order_id FROM lh1.sales", "lakehouse": "lh1", "workspace_id": env["ws1"]["id"], "max_rows": 5000})
    assert capped["row_count"] == 50 and capped["truncated"] and any("server limit" in w for w in capped["warnings"])


def test_parameters_are_typed_literals_never_text(dc, env):
    body = {"lakehouse": "lh1", "workspace_id": env["ws1"]["id"]}
    r = sql(dc, {**body, "sql": "SELECT count(*) FROM lh1.customers WHERE region = :r", "parameters": {"r": "us' OR '1'='1"}})
    assert r["rows"] == [[0]]
    r = sql(dc, {**body, "sql": "SELECT count(*) FROM lh1.customers WHERE name = :n", "parameters": {"n": "'; DROP TABLE x; --"}})
    assert r["rows"] == [[0]]
    r = sql(dc, {**body, "sql": "SELECT :d::DATE + 1 AS d, :f AS f, :b AS b", "parameters": {"d": "2024-02-28", "f": 1.5, "b": True}})
    assert r["rows"] == [["2024-02-29", 1.5, True]]
    missing = dc.post("/v1/sql/execute", json={**body, "sql": "SELECT :nope"}, headers=dc.admin)
    assert missing.status_code == 400 and "nope" in missing.text
    extra = dc.post("/v1/sql/execute", json={**body, "sql": "SELECT 1", "parameters": {"zzz": 1}}, headers=dc.admin)
    assert extra.status_code == 200 and extra.json()["warnings"] == ["parameter 'zzz' is not used by the statement"]


# ----------------------------------------------------------------------------- read-only
@pytest.mark.parametrize("stmt", [
    "INSERT INTO lh1.customers VALUES (9,'x','y')", "UPDATE lh1.customers SET region='x'", "DELETE FROM lh1.customers",
    "CREATE TABLE lh1.t AS SELECT 1", "CREATE TABLE t (a int)", "DROP TABLE lh1.customers", "ALTER TABLE lh1.customers ADD COLUMN z int",
    "TRUNCATE lh1.customers", "COPY lh1.customers TO '/tmp/x.csv'", "ATTACH '/tmp/x.db' AS x", "INSTALL httpfs", "LOAD httpfs",
    "PRAGMA database_list", "SET memory_limit='100GB'", "CALL pragma_version()", "EXPORT DATABASE '/tmp/x'",
    "SELECT * FROM read_text('/etc/passwd')", "SELECT * FROM glob('/*')", "SELECT * FROM read_csv('/etc/passwd')",
    "SELECT * FROM read_parquet('http://example.com/x.parquet')", "SELECT * FROM read_csv('s3://bucket/x.csv')", "SELECT * FROM '/etc/passwd'",
    "SELECT getenv('HOME')", "SELECT * FROM duckdb_settings()", "SELECT current_setting('home_directory')",
])
def test_duckdb_never_writes_or_reaches_outside(dc, env, stmt):
    before = sql(dc, {"sql": "SELECT count(*) FROM lh1.customers", "lakehouse": "lh1", "workspace_id": env["ws1"]["id"]})["rows"]
    r = dc.post("/v1/sql/execute", json={"sql": stmt, "lakehouse": "lh1", "workspace_id": env["ws1"]["id"]}, headers=dc.admin)
    assert r.status_code in (400, 403, 404, 422), (stmt, r.status_code, r.text)
    assert "root:" not in r.text
    after = sql(dc, {"sql": "SELECT count(*) FROM lh1.customers", "lakehouse": "lh1", "workspace_id": env["ws1"]["id"]})["rows"]
    assert before == after == [[3]]


def test_write_error_is_actionable(dc, env):
    r = dc.post("/v1/sql/execute", json={"sql": "INSERT INTO lh1.customers VALUES (4,'x','y')", "lakehouse": "lh1", "workspace_id": env["ws1"]["id"]}, headers=dc.admin)
    assert r.status_code == 422
    err = r.json()["error"]
    assert "read-only" in err["message"] and "spark" in err["remediation"]


# ----------------------------------------------------------------------------- files
def test_read_csv_parquet_json_from_files_area(dc, env):
    b = {"lakehouse": "lh1", "workspace_id": env["ws1"]["id"]}
    r = sql(dc, {**b, "sql": "SELECT city, pop FROM read_csv('Files/raw/cities.csv') WHERE pop > 2500000 ORDER BY pop"})
    assert r["rows"] == [["Rome", 2800000], ["Berlin", 3600000]]
    r = sql(dc, {**b, "sql": "SELECT sum(x) AS s, count(*) AS n FROM read_parquet('Files/raw/p/part-0.parquet')"})
    assert r["rows"] == [[10, 4]]
    r = sql(dc, {**b, "sql": "SELECT count(*) FROM read_parquet('Files/raw/p/*.parquet')"})
    assert r["rows"] == [[4]]
    r = sql(dc, {**b, "sql": "SELECT a, b FROM read_json('Files/raw/e.json') ORDER BY a"})
    assert r["rows"] == [[1, "x"], [2, "y"]]
    r = sql(dc, {**b, "sql": "SELECT count(*) FROM read_csv('olcf://ws1/lh1/Files/raw/cities.csv', header=true)"})
    assert r["rows"] == [[3]]
    # files and tables can be joined
    r = sql(dc, {**b, "sql": "SELECT count(*) FROM read_csv('Files/raw/cities.csv') c JOIN lh1.customers u ON u.customer_id = c.id"})
    assert r["rows"] == [[3]]


@pytest.mark.parametrize("path", ["Files/../Tables/customers/part", "Files/raw/../../..", "/etc/passwd", "Files/../../../../etc/passwd", "Tables/customers", "olcf://ws2/lh2/Files/x.csv",
                                  "Files/raw/%2e%2e/x", "file:///etc/passwd", "~/x"])
def test_file_reader_traversal_and_foreign_paths_are_refused(dc, env, path):
    r = dc.post("/v1/sql/execute", json={"sql": f"SELECT * FROM read_csv('{path}')", "lakehouse": "lh1", "workspace_id": env["ws1"]["id"]}, headers=dc.admin)  # noqa: S608
    assert r.status_code in (400, 403, 404, 422), (path, r.status_code, r.text)
    assert "root:" not in r.text


def test_file_scan_limit(dc, env):
    dc.ctx.service("settings").set("sql.max_file_scan_mb", 1)
    big = ("a,b\n" + "1,xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n" * 20000).encode()
    assert dc.put(f"/v1/lakehouses/{env['lh1']['id']}/files/content", params={"path": "Files/raw/big.csv"}, content=big, headers=dc.admin).status_code == 201
    r = dc.post("/v1/sql/execute", json={"sql": "SELECT count(*) FROM read_csv('Files/raw/big.csv')", "lakehouse": "lh1", "workspace_id": env["ws1"]["id"]}, headers=dc.admin)
    assert r.status_code == 422 and "file-scan limit" in r.text and "remediation" in r.text


# ----------------------------------------------------------------------------- authorisation
def test_cross_workspace_access_is_denied_and_not_leaked(dc, env):
    v = env["viewer"]
    ok = sql(dc, {"sql": "SELECT count(*) FROM lh1.customers", "workspace_id": env["ws1"]["id"]}, headers=v)
    assert ok["rows"] == [[3]]
    for body in ({"sql": "SELECT * FROM lh2.secrets", "workspace_id": env["ws2"]["id"]},
                 {"sql": "SELECT * FROM ws2.lh2.secrets", "workspace_id": env["ws1"]["id"]},
                 {"sql": "SELECT * FROM lh2.secrets", "lakehouse": env["lh2"]["id"]},
                 {"sql": "SELECT * FROM lh1.customers c, ws2.lh2.secrets s", "workspace_id": env["ws1"]["id"], "lakehouse": "lh1"}):
        r = dc.post("/v1/sql/execute", json=body, headers=v)
        assert r.status_code in (403, 404), (body, r.status_code, r.text)
        body_doc = r.json()
        assert "rows" not in body_doc and "columns" not in body_doc, "no result set may come back for a statement that was denied"
        # ids in the answer (wks_<ULID>, correlation ids) are random and may contain any digits, e.g. "42": look for the secret as a token, never as a substring
        scrubbed = re.sub(r"\b[a-z]{2,5}_[0-9A-Za-z]{16,}\b", "<id>", r.text.replace(body_doc["error"]["correlation_id"], "<cid>"))
        assert not re.search(r"(?<![0-9A-Za-z_])top(?![0-9A-Za-z_])", scrubbed), scrubbed
        assert not re.search(r"(?<![0-9A-Za-z_.-])42(?![0-9A-Za-z_])", scrubbed), scrubbed
    # a principal with no workspace at all
    nobody = dc.user("nobody", "Viewer", workspace_id=env["ws2"]["id"])
    assert dc.post("/v1/sql/execute", json={"sql": "SELECT * FROM lh1.customers", "workspace_id": env["ws1"]["id"]}, headers=nobody).status_code in (403, 404)


def test_schema_endpoint_lists_only_readable_lakehouses_with_columns(dc, env):
    v = env["viewer"]
    s = dc.get("/v1/sql/schema", headers=v).json()
    assert [lh["name"] for lh in s["lakehouses"]] == ["lh1"]
    tables = {t["name"]: t for t in s["lakehouses"][0]["tables"]}
    assert set(tables) == {"customers", "sales"} and tables["customers"]["reference"] == "lh1.customers"
    assert [c["name"] for c in tables["customers"]["columns"]] == ["customer_id", "region", "name"]
    assert sorted(lh["name"] for lh in dc.get("/v1/sql/schema", headers=dc.admin).json()["lakehouses"]) == ["lh1", "lh2"]
    assert dc.get("/v1/sql/schema", params={"workspace_id": env["ws2"]["id"]}, headers=v).status_code == 403


def test_token_scope_without_sql_execute_is_denied(dc, env):
    h = dc.user("scoped", "Contributor", workspace_id=env["ws1"]["id"], scopes=["mcp"])   # mcp scope includes sql.execute
    assert sql(dc, {"sql": "SELECT 1", "workspace_id": env["ws1"]["id"]}, headers=h)["rows"] == [[1]]
    h2 = dc.user("scoped2", "Contributor", workspace_id=env["ws1"]["id"], scopes=["fabric"])
    assert dc.post("/v1/sql/execute", json={"sql": "SELECT 1", "workspace_id": env["ws1"]["id"]}, headers=h2).status_code == 403


# ----------------------------------------------------------------------------- timeout / cancel / concurrency
SLOW = "SELECT sum(i * 1.0) FROM range(0, 100000000000) t(i)"


def test_timeout_interrupts_the_statement(dc, env):
    t0 = time.perf_counter()
    r = dc.post("/v1/sql/execute", json={"sql": SLOW, "lakehouse": "lh1", "workspace_id": env["ws1"]["id"], "timeout_seconds": 1}, headers=dc.admin)
    assert r.status_code == 408 and r.json()["error"]["code"] == "query_timeout"
    assert time.perf_counter() - t0 < 20
    h = dc.get("/v1/sql/history", params={"status": "timeout"}, headers=dc.admin).json()["items"]
    assert len(h) == 1 and h[0]["error_code"] == "query_timeout"
    # the engine is reusable afterwards
    assert sql(dc, {"sql": "SELECT 1", "workspace_id": env["ws1"]["id"]})["rows"] == [[1]]


def test_cancel_a_running_statement(dc, env):
    out: dict[str, Any] = {}

    def run() -> None:
        out["r"] = dc.post("/v1/sql/execute", json={"sql": SLOW, "query_id": "qry_cancel_me_1", "workspace_id": env["ws1"]["id"], "timeout_seconds": 120}, headers=dc.admin)

    t = threading.Thread(target=run)
    t.start()
    try:
        for _ in range(100):
            if any(q["query_id"] == "qry_cancel_me_1" for q in dc.get("/v1/sql/running", headers=dc.admin).json()["items"]):
                break
            time.sleep(0.1)
        else:
            pytest.fail("statement never showed up as running")
        # someone else cannot cancel it
        assert dc.post("/v1/sql/cancel/qry_cancel_me_1", headers=env["viewer"]).status_code == 404
        assert dc.post("/v1/sql/cancel/qry_cancel_me_1", headers=dc.admin).json()["cancelling"] is True
    finally:
        t.join(timeout=30)
    assert not t.is_alive()
    assert out["r"].status_code == 499 and out["r"].json()["error"]["code"] == "query_cancelled"
    assert dc.get("/v1/sql/history/qry_cancel_me_1", headers=dc.admin).json()["status"] == "cancelled"
    assert dc.post("/v1/sql/cancel/qry_cancel_me_1", headers=dc.admin).status_code == 404


def test_concurrency_limit_returns_429(dc, env):
    dc.ctx.service("settings").set("sql.max_concurrent_queries", 1)
    svc = dc.ctx.service("sql")
    svc._slots = threading.BoundedSemaphore(1)
    svc._slots.acquire()   # a statement is "running"
    try:
        r = dc.post("/v1/sql/execute", json={"sql": "SELECT 1", "workspace_id": env["ws1"]["id"], "timeout_seconds": 1}, headers=dc.admin)
        assert r.status_code == 429, r.text
    finally:
        svc._slots.release()
    assert sql(dc, {"sql": "SELECT 1", "workspace_id": env["ws1"]["id"]})["rows"] == [[1]]


def test_memory_limit_is_enforced_with_a_clear_error(dc, env):
    dc.ctx.service("settings").set("sql.duckdb_memory_mb", 64)
    r = dc.post("/v1/sql/execute", json={"sql": "SELECT count(*) FROM (SELECT i, repeat('x', 1000) s FROM range(0, 400000) t(i) ORDER BY s DESC, i)", "workspace_id": env["ws1"]["id"],
                                         "timeout_seconds": 60}, headers=dc.admin)
    assert r.status_code in (422, 400), r.text
    assert r.json()["error"]["code"] in ("unsupported", "bad_request")


# ----------------------------------------------------------------------------- history
def test_history_records_every_outcome_and_respects_visibility(dc, env):
    v, c = env["viewer"], env["contrib"]
    sql(dc, {"sql": "SELECT count(*) FROM lh1.customers", "workspace_id": env["ws1"]["id"]}, headers=v)
    dc.post("/v1/sql/execute", json={"sql": "SELECT nope FROM lh1.customers", "workspace_id": env["ws1"]["id"]}, headers=v)
    sql(dc, {"sql": "SELECT 'password=hunter2hunter2' AS pw, 1 AS a WHERE 'Bearer abcdefghijklmnop' <> ''", "workspace_id": env["ws1"]["id"]}, headers=c)
    mine = dc.get("/v1/sql/history", headers=v).json()["items"]
    assert [m["status"] for m in mine] == ["error", "ok"] and mine[1]["row_count"] == 1 and mine[1]["tables"] == ["lh1.customers"] and mine[1]["engine"] == "duckdb"
    assert mine[0]["error_message"] and mine[0]["principal_id"] == mine[1]["principal_id"]
    # other users' rows are invisible to a viewer, and indistinguishable from "missing"
    assert dc.get("/v1/sql/history", params={"mine": "false"}, headers=v).json()["items"] == mine
    allrows = dc.get("/v1/sql/history", params={"mine": "false", "workspace_id": env["ws1"]["id"]}, headers=dc.admin).json()["items"]
    assert len(allrows) == 3
    secret_row = next(r for r in allrows if "pw" in r["sql_text"])
    assert "hunter2hunter2" not in secret_row["sql_text"] and "abcdefghijklmnop" not in secret_row["sql_text"] and "REDACTED" in secret_row["sql_text"]
    other = next(r for r in allrows if r["principal_id"] != mine[0]["principal_id"])
    assert dc.get(f"/v1/sql/history/{other['id']}", headers=v).status_code == 404
    assert dc.get(f"/v1/sql/history/{mine[0]['id']}", headers=v).status_code == 200
    # pagination + filters
    page = dc.get("/v1/sql/history", params={"limit": 1, "mine": "false", "workspace_id": env["ws1"]["id"]}, headers=dc.admin).json()
    assert len(page["items"]) == 1 and page["next_cursor"]
    assert len(dc.get("/v1/sql/history", params={"status": "error", "mine": "false", "workspace_id": env["ws1"]["id"]}, headers=dc.admin).json()["items"]) == 1


def test_history_retention_purges_old_and_trims_excess(dc, env):
    from sqlalchemy import update

    from olcf.sql.schema import query_history
    for i in range(6):
        sql(dc, {"sql": f"SELECT {i}", "workspace_id": env["ws1"]["id"]})
    with dc.ctx.db.begin() as conn:
        conn.execute(update(query_history).where(query_history.c.sql_text.in_(["SELECT 0", "SELECT 1"])).values(started_at="2020-01-01T00:00:00.000000Z"))
    hs = dc.ctx.service("sql").history
    res = hs.purge()
    assert res["expired"] == 2 and res["trimmed"] == 0
    dc.ctx.service("settings").set("sql.history_max_rows", 1000)
    # trim: pretend the cap is 3 by calling the store directly with a patched setting reader
    hs._setting = lambda key, default: 3 if key == "sql.history_max_rows" else default   # type: ignore[method-assign]
    res = hs.purge()
    assert res["trimmed"] == 1
    left = dc.get("/v1/sql/history", params={"limit": 50}, headers=dc.admin).json()["items"]
    assert sorted(r["sql_text"] for r in left) == ["SELECT 3", "SELECT 4", "SELECT 5"]   # newest survive


def test_orphaned_running_rows_are_failed_on_restart(dc, env):
    svc = dc.ctx.service("sql")
    from olcf.security.rbac import Binding, Principal
    p = Principal(id="prn_x", name="x", kind="user", bindings=[Binding("PlatformAdmin")])
    svc.history.start("qry_orphan_000001", p, engine="duckdb", source="api", sql="SELECT 1", workspace_id=None, lakehouse_id=None, item_id=None, kind="select")
    assert svc.history.fail_orphans() >= 1
    row = dc.get("/v1/sql/history/qry_orphan_000001", headers=dc.admin).json()
    assert row["status"] == "error" and row["error_code"] == "interrupted"


# ----------------------------------------------------------------------------- explain / engines
def test_explain_and_engine_catalogue(dc, env):
    r = dc.post("/v1/sql/explain", json={"sql": "SELECT region, count(*) FROM lh1.customers GROUP BY region", "workspace_id": env["ws1"]["id"]}, headers=dc.admin).json()
    assert r["engine"] == "duckdb" and r["tables"] == ["lh1.customers"] and "GROUP" in r["plan"].upper()
    e = dc.get("/v1/sql/engines", headers=dc.admin).json()
    by = {x["id"]: x for x in e["items"]}
    assert by["duckdb"]["available"] and by["duckdb"]["default"] and by["duckdb"]["read_only"] and by["duckdb"]["capabilities"]["writes"] is False
    assert by["spark"]["available"] is False and e["limits"]["max_rows_cap"] == 100000


# ----------------------------------------------------------------------------- spark delegation
class FakeCompute:
    """Stands in for the compute service: records calls, answers like a Spark SQL session."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.cancelled: list[str] = []

    def run_spark_sql(self, principal, workspace_id, sql, *, lakehouse=None, limit=1000, timeout=60):  # noqa: ANN001, ANN201
        self.calls.append({"principal": principal.id, "workspace_id": workspace_id, "sql": sql, "lakehouse": lakehouse, "limit": limit, "timeout": timeout})
        if "boom" in sql:
            raise RuntimeError("executor lost")
        return {"columns": [{"name": "n", "type": "bigint"}, {"name": "d", "type": "date"}, {"name": "s", "type": "string"}],
                "rows": [[1, "2024-01-02", "a"], [2, None, None]], "truncated": False, "stats": {"jobs": 1}}


def test_spark_engine_is_unavailable_without_compute_with_guidance(dc, env):
    r = dc.post("/v1/sql/execute", json={"sql": "SELECT 1", "engine": "spark", "workspace_id": env["ws1"]["id"]}, headers=dc.admin)
    assert r.status_code == 503 and "compute" in r.json()["error"]["remediation"]
    ok, steps = dc.ctx.service("sql").deep_check("spark")
    assert ok is False and steps[0]["name"] == "compute service"


def test_spark_select_and_write_delegate_with_authorisation_lineage_and_catalog_refresh(dc, env):
    fake = FakeCompute()
    dc.ctx.register_service("compute", fake)
    ws = env["ws1"]["id"]
    r = sql(dc, {"sql": "SELECT n, d, s FROM lh1.customers", "engine": "spark", "workspace_id": ws, "lakehouse": "lh1", "max_rows": 50}, headers=env["viewer"])
    assert r["engine"] == "spark" and r["rows"] == [[1, "2024-01-02", "a"], [2, None, None]] and [c["name"] for c in r["columns"]] == ["n", "d", "s"]
    assert fake.calls[0]["limit"] == 50 and fake.calls[0]["workspace_id"] == ws and fake.calls[0]["lakehouse"]
    # writes: viewer lacks sql.write; cross-workspace read still denied
    w = {"sql": "CREATE TABLE lh1.gold AS SELECT region, count(*) AS n FROM lh1.customers GROUP BY region", "engine": "spark", "workspace_id": ws, "lakehouse": "lh1"}
    denied = dc.post("/v1/sql/execute", json=w, headers=env["viewer"])
    assert denied.status_code == 403 and len(fake.calls) == 1
    assert dc.post("/v1/sql/execute", json={"sql": "SELECT * FROM lh2.secrets", "engine": "spark", "workspace_id": ws, "lakehouse": "lh1"}, headers=env["viewer"]).status_code in (403, 404)
    assert len(fake.calls) == 1
    # a contributor may write; the engine saw the statement, history is recorded, lineage lh1.customers -> lh1.gold exists
    done = sql(dc, w, headers=env["contrib"])
    assert done["statement"] in ("ctas", "create_table_as") or "create" in done["statement"] and len(fake.calls) == 2
    lin = dc.get("/v1/catalog/lineage", params={"dataset": f"{env['lh1']['id']}/Tables/customers", "direction": "down"}, headers=dc.admin).json()
    assert any(n.get("name") == "gold" for n in lin["nodes"]), lin
    h = dc.get("/v1/sql/history", params={"mine": "false", "workspace_id": ws}, headers=dc.admin).json()["items"]
    assert {x["engine"] for x in h} == {"spark"} and any(x["statement_kind"] and x["statement_kind"] != "select" for x in h)
    # engine failure surfaces as 503 with a remediation, and the history row ends in error
    r = dc.post("/v1/sql/execute", json={"sql": "SELECT 'boom'", "engine": "spark", "workspace_id": ws}, headers=dc.admin)
    assert r.status_code == 503 and r.json()["error"]["remediation"]
    ok, steps = dc.ctx.service("sql").deep_check("spark")
    assert ok and steps[-1]["detail"].startswith("SELECT 1")
    assert dc.ctx.health.run("sql:spark", deep=True).status.value == "ok"


# ----------------------------------------------------------------------------- exports
def export(dc, body, headers=None, expect=200):
    r = dc.post("/v1/sql/export", json=body, headers=headers or dc.admin)
    assert r.status_code == expect, r.text
    return r


def test_export_csv_json_ndjson_parquet_stream_the_full_result(dc, env):
    b = {"sql": "SELECT order_id, amount FROM lh1.sales ORDER BY order_id", "workspace_id": env["ws1"]["id"], "lakehouse": "lh1", "max_rows": None}
    r = export(dc, {**{k: v for k, v in b.items() if k != "max_rows"}, "format": "csv", "filename": "sales out"})
    assert r.headers["content-type"].startswith("text/csv") and 'filename="sales_out.csv"' in r.headers["content-disposition"]
    rows = list(csv.reader(io.StringIO(r.content.decode("utf-8-sig"))))
    assert rows[0] == ["order_id", "amount"] and len(rows) == 6001 and rows[-1][0] == "5999"
    r = export(dc, {**{k: v for k, v in b.items() if k != "max_rows"}, "format": "json"})
    data = r.json()
    assert len(data) == 6000 and data[0] == {"order_id": 0, "amount": 0.0}
    r = export(dc, {**{k: v for k, v in b.items() if k != "max_rows"}, "format": "ndjson"})
    lines = r.content.decode().splitlines()
    assert len(lines) == 6000 and json.loads(lines[-1])["order_id"] == 5999
    r = export(dc, {**{k: v for k, v in b.items() if k != "max_rows"}, "format": "parquet"})
    t = pq.read_table(io.BytesIO(r.content))
    assert t.num_rows == 6000 and t.schema.names == ["order_id", "amount"] and t.schema.field("order_id").type == pa.int64()
    hist = dc.get("/v1/sql/history", params={"source": "export"}, headers=dc.admin).json()["items"]
    assert len(hist) == 4 and all(h["status"] == "ok" and h["row_count"] == 6000 for h in hist)


def test_export_csv_neutralises_formulas_and_streams_a_large_result(dc, env):
    r = export(dc, {"sql": "SELECT name FROM lh1.customers ORDER BY customer_id", "workspace_id": env["ws1"]["id"], "lakehouse": "lh1", "format": "csv"})
    cells = [row[0] for row in csv.reader(io.StringIO(r.content.decode("utf-8-sig")))][1:]
    assert cells == ["a", "'=b", "c"]
    big = export(dc, {"sql": "SELECT i, 'row ' || i AS s FROM range(0, 400000) t(i)", "workspace_id": env["ws1"]["id"], "format": "csv"})
    assert big.content.count(b"\n") == 400001
    dc.ctx.service("settings").set("sql.export_max_rows", 1000)
    capped = export(dc, {"sql": "SELECT i FROM range(0, 400000) t(i)", "workspace_id": env["ws1"]["id"], "format": "ndjson"})
    assert len(capped.content.splitlines()) == 1000
    assert dc.get("/v1/sql/history", params={"source": "export"}, headers=dc.admin).json()["items"][0]["truncated"] is True


def test_export_is_authorised_and_validated(dc, env):
    export(dc, {"sql": "SELECT * FROM lh2.secrets", "workspace_id": env["ws2"]["id"], "format": "csv"}, headers=env["viewer"], expect=403)
    export(dc, {"sql": "SELECT 1", "workspace_id": env["ws1"]["id"], "format": "xlsx"}, expect=422)
    export(dc, {"sql": "DELETE FROM lh1.customers", "workspace_id": env["ws1"]["id"], "lakehouse": "lh1", "format": "csv"}, expect=422)
    export(dc, {"sql": "SELECT 1", "workspace_id": env["ws1"]["id"], "format": "csv", "max_rows": 5}, expect=400)


# ----------------------------------------------------------------------------- deep health
def test_deep_health_runs_a_real_table_read(dc, env):
    h = dc.ctx.health.run("sql:duckdb", deep=True)
    assert h.status.value == "ok", h.to_dict()
    names = [s["name"] for s in h.steps]
    assert names == ["select 1", "delta table read", "sample table read"] and all(s["ok"] for s in h.steps)
    assert "3 rows" in h.steps[1]["detail"]
    q = dc.ctx.health.run("sql:duckdb", force=True)
    assert q.status.value == "ok" and "1." in q.detail
    sp = dc.ctx.health.run("sql:spark", force=True)
    assert sp.status.value == "unknown" and "compute" in sp.detail
    assert {"sql:duckdb", "sql:spark"} <= set(dc.ctx.health.names())


# ----------------------------------------------------------------------------- saved queries
def create_query(dc, ws, name, definition, headers=None, expect=201):
    r = dc.post(f"/v1/workspaces/{ws}/items", json={"type": "sql_query", "name": name, "definition": definition}, headers=headers or dc.admin)
    assert r.status_code == expect, r.text
    return r.json()


def test_saved_query_validation_and_run_with_typed_parameters(dc, env):
    ws = env["ws1"]["id"]
    q = create_query(dc, ws, "by region", {"sql": "SELECT region, count(*) AS n FROM lh1.customers WHERE customer_id >= :minid AND region = coalesce(:region, region) GROUP BY region ORDER BY 1",
                                           "engine": "duckdb", "lakehouse_id": env["lh1"]["id"],
                                           "parameters": [{"name": "minid", "type": "int", "default": 1}, {"name": "region", "type": "string", "default": None}]})
    r = dc.post(f"/v1/sql/queries/{q['id']}/run", json={}, headers=env["viewer"])
    assert r.status_code == 200 and r.json()["rows"] == [["eu", 1], ["us", 2]], r.text
    r = dc.post(f"/v1/sql/queries/{q['id']}/run", json={"parameters": {"minid": "3", "region": "us"}}, headers=env["viewer"])
    assert r.json()["rows"] == [["us", 1]]
    bad = dc.post(f"/v1/sql/queries/{q['id']}/run", json={"parameters": {"minid": "not a number"}}, headers=env["viewer"])
    assert bad.status_code == 400 and "minid" in bad.text
    unknown = dc.post(f"/v1/sql/queries/{q['id']}/run", json={"parameters": {"zzz": 1}}, headers=env["viewer"])
    assert unknown.status_code == 400
    hist = dc.get("/v1/sql/history", params={"source": "saved_query"}, headers=env["viewer"]).json()["items"]
    assert len(hist) == 2 and all(h["item_id"] == q["id"] for h in hist)
    # definition validation
    create_query(dc, ws, "bad2", {"sql": "SELEC 1"}, expect=422)
    create_query(dc, ws, "bad3", {"sql": "SELECT 1", "engine": "oracle"}, expect=422)
    create_query(dc, ws, "bad4", {"sql": "SELECT :a", "parameters": [{"name": "a", "type": "blob"}]}, expect=422)
    create_query(dc, ws, "bad5", {"sql": "SELECT 1", "bogus": True}, expect=422)
    # a saved query of another workspace cannot be run by someone without item.read there
    outsider = dc.user("outsider", "Viewer", workspace_id=env["ws2"]["id"])
    assert dc.post(f"/v1/sql/queries/{q['id']}/run", json={}, headers=outsider).status_code in (403, 404)


def test_saved_query_can_be_a_spark_write_definition_but_runs_through_authorisation(dc, env):
    ws = env["ws1"]["id"]
    q = create_query(dc, ws, "build gold", {"sql": "CREATE TABLE lh1.gold2 AS SELECT * FROM lh1.customers", "engine": "spark", "lakehouse_id": env["lh1"]["id"]})
    dc.ctx.register_service("compute", FakeCompute())
    assert dc.post(f"/v1/sql/queries/{q['id']}/run", json={}, headers=env["viewer"]).status_code == 403
    assert dc.post(f"/v1/sql/queries/{q['id']}/run", json={}, headers=env["contrib"]).status_code == 200


# ----------------------------------------------------------------------------- iceberg + delta time travel registration
def test_iceberg_table_is_queryable(dc, tmp_path):
    pytest.importorskip("pyiceberg")
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField, StringType

    dc.workspace("wsi")
    lh = dc.lakehouse("wsi", "lhi")
    root = Path(dc.ctx.service("storage").get_lakehouse(lh["id"]).root_uri.removeprefix("file://"))
    cat = SqlCatalog("t", uri=f"sqlite:///{tmp_path}/cat.db", warehouse=f"file://{tmp_path}/wh")
    cat.create_namespace("db")
    tbl = cat.create_table("db.ev", schema=Schema(NestedField(1, "id", LongType(), required=True), NestedField(2, "kind", StringType())), location=f"file://{root}/Tables/ev")
    tbl.append(pa.table({"id": pa.array([1, 2, 3], pa.int64()), "kind": ["a", "b", "a"]}, schema=pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("kind", pa.string())])))
    r = sql(dc, {"sql": "SELECT kind, count(*) AS n FROM lhi.ev GROUP BY kind ORDER BY kind", "lakehouse": "lhi", "workspace_id": lh["workspace_id"]})
    assert r["rows"] == [["a", 2], ["b", 1]]


def test_delta_table_sees_new_commits_and_partition_pruning_still_correct(dc, env):
    lh = env["lh1"]
    dc.write_table(lh["id"], "events", pa.table({"d": ["2024-01", "2024-01", "2024-02"], "v": pa.array([1, 2, 3], pa.int64())}), partition_by=["d"])
    assert sql(dc, {"sql": "SELECT sum(v) FROM lh1.events WHERE d = '2024-02'", "lakehouse": "lh1", "workspace_id": env["ws1"]["id"]})["rows"] == [[3]]
    dc.write_table(lh["id"], "events", pa.table({"d": ["2024-02"], "v": pa.array([10], pa.int64())}), mode="append")
    assert sql(dc, {"sql": "SELECT sum(v) FROM lh1.events WHERE d = '2024-02'", "lakehouse": "lh1", "workspace_id": env["ws1"]["id"]})["rows"] == [[13]]
