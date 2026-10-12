# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""Platform events published by the SQL service for runtime lineage: ``dataset.read`` / ``dataset.written`` / ``dataset.statement``.

The real event bus is used with a capturing subscriber, so the assertions are on the exact payloads other modules (Intelligence) would receive. Failure injection proves
the SQL path is unaffected when publishing breaks. No JVM/Spark is started (the Spark engine is the same fake compute service as in ``test_sql_service``).
"""

from __future__ import annotations

import hashlib
import logging
import re
import time
from typing import Any

import pyarrow as pa
import pytest

from olcf.core import dataset_events as dsev

ISO = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d")


class Capture:
    """Subscribes to the real event bus and keeps every event (published or not persisted) in order."""

    def __init__(self, ctx: Any) -> None:
        self.ctx = ctx
        self.events: list[dict[str, Any]] = []
        self.unsubscribe = ctx.events.subscribe(self.events.append)

    def of(self, *types: str) -> list[dict[str, Any]]:
        return [e for e in self.events if e["type"] in types]

    def types(self) -> list[str]:
        return [e["type"] for e in self.events if e["type"].startswith("dataset.")]

    def clear(self) -> None:
        self.events.clear()


@pytest.fixture()
def env(dc):
    ws1, ws2 = dc.workspace("ws1"), dc.workspace("ws2")
    lh1, lh2 = dc.lakehouse("ws1", "lh1"), dc.lakehouse("ws2", "lh2")
    dc.write_table(lh1["id"], "customers", pa.table({"customer_id": pa.array([1, 2, 3], pa.int64()), "region": ["eu", "us", "us"]}))
    dc.write_table(lh1["id"], "sales", pa.table({"order_id": pa.array([1, 2, 3, 4], pa.int64()), "customer_id": pa.array([1, 1, 2, 3], pa.int64()),
                                                  "amount": pa.array([1.0, 2.0, 3.0, 4.0])}))
    dc.write_table(lh2["id"], "secrets", pa.table({"k": ["top"]}))
    csv_body = b"id,city\n1,Paris\n2,Berlin\n"
    assert dc.put(f"/v1/lakehouses/{lh1['id']}/files/content", params={"path": "Files/raw/cities.csv"}, content=csv_body, headers=dc.admin).status_code == 201
    return {"ws1": ws1, "ws2": ws2, "lh1": lh1, "lh2": lh2, "contrib": dc.user("contrib1", "Contributor", workspace_id=ws1["id"])}


@pytest.fixture()
def cap(dc):
    c = Capture(dc.ctx)
    yield c
    c.unsubscribe()


def run(dc, body: dict[str, Any], headers=None, expect: int = 200) -> dict[str, Any]:
    r = dc.post("/v1/sql/execute", json=body, headers=headers or dc.admin)
    assert r.status_code == expect, r.text
    return r.json()


def sha1_of(sql: str) -> str:
    return hashlib.sha1(" ".join(sql.split()).rstrip(";").strip().encode()).hexdigest()  # noqa: S324


# ------------------------------------------------------------------------------------------------- exact payloads (DuckDB reads)
def test_duckdb_select_publishes_read_and_statement_with_exact_payloads(dc, env, cap):
    ws, lh = env["ws1"]["id"], env["lh1"]["id"]
    sql = "SELECT c.region, count(*) AS n FROM lh1.sales s JOIN lh1.customers c USING (customer_id) GROUP BY c.region"
    r = run(dc, {"sql": sql, "workspace_id": ws, "lakehouse": lh, "query_id": "qry_events_0001"})
    assert r["rows"] and r["query_id"] == "qry_events_0001"
    assert cap.types() == ["dataset.read", "dataset.statement"]       # nothing is written; the order is read, then the statement
    read, stmt = cap.events[-2], cap.events[-1]
    process = {"kind": "sql_query", "item_id": None, "job_id": None, "query_id": "qry_events_0001", "name": None, "run_id": None}
    assert read["workspace_id"] == ws and read["subject"] == "qry_events_0001"
    at = read["payload"].pop("at")
    assert ISO.match(at)
    assert read["payload"] == {
        "workspace_id": ws, "process": process, "engine": "duckdb", "mode": None, "statement_hash": sha1_of(sql),
        "datasets": [{"kind": "table", "lakehouse_id": lh, "name": "sales", "path": None, "uri": None, "format": None},
                     {"kind": "table", "lakehouse_id": lh, "name": "customers", "path": None, "uri": None, "format": None}],
    }
    assert stmt["workspace_id"] == ws and stmt["subject"] == "qry_events_0001"
    assert ISO.match(stmt["payload"].pop("at"))
    assert stmt["payload"] == {"workspace_id": ws, "process": process, "sql": sql, "dialect": "duckdb", "default_lakehouse_id": lh, "statement_hash": sha1_of(sql)}


def test_events_follow_history_and_only_reads_and_writes_are_persisted(dc, env, cap):
    ws, lh = env["ws1"]["id"], env["lh1"]["id"]
    run(dc, {"sql": "SELECT count(*) FROM lh1.customers", "workspace_id": ws, "lakehouse": lh, "query_id": "qry_events_0002"})
    hist = dc.get("/v1/sql/history/qry_events_0002", headers=dc.admin).json()
    assert hist["status"] == "ok"
    stored = [e["type"] for e in dc.ctx.events.list(type_prefix="dataset.", limit=100)]
    assert stored == ["dataset.read"], "the SQL text of dataset.statement must stay out of the persisted event table (webhooks / GET /v1/events / SSE read it)"
    assert cap.types() == ["dataset.read", "dataset.statement"]
    api = dc.get("/v1/events", params={"type": "dataset."}, headers=dc.admin).json()["items"]
    assert [e["type"] for e in api] == ["dataset.read"]


def test_statement_hash_ignores_whitespace_and_trailing_semicolon_and_text_is_redacted_and_clipped(dc, env, cap):
    ws = env["ws1"]["id"]
    a = run(dc, {"sql": "SELECT count(*) FROM lh1.customers", "workspace_id": ws})
    b = run(dc, {"sql": "SELECT   count(*)\n  FROM lh1.customers ;", "workspace_id": ws})
    assert a["rows"] == b["rows"]
    reads = cap.of("dataset.read")
    assert reads[0]["payload"]["statement_hash"] == reads[1]["payload"]["statement_hash"] == sha1_of("SELECT count(*) FROM lh1.customers")
    cap.clear()
    secret = "SELECT 'password=hunter2hunter2' AS leaked, count(*) FROM lh1.customers"
    run(dc, {"sql": secret, "workspace_id": ws})
    (stmt,) = cap.of("dataset.statement")
    assert "hunter2hunter2" not in stmt["payload"]["sql"] and "REDACTED" in stmt["payload"]["sql"]
    assert cap.of("dataset.read")[0]["payload"]["statement_hash"] == sha1_of(secret)     # the identity is of the statement as written
    cap.clear()
    long_sql = "SELECT '" + "x" * 30_000 + "' AS filler, count(*) FROM lh1.customers"
    run(dc, {"sql": long_sql, "workspace_id": ws})
    (stmt,) = cap.of("dataset.statement")
    assert len(stmt["payload"]["sql"]) == dsev.MAX_STATEMENT_CHARS


def test_file_reads_are_reported_as_files_with_their_lakehouse(dc, env, cap):
    ws, lh = env["ws1"]["id"], env["lh1"]["id"]
    r = run(dc, {"sql": "SELECT * FROM read_csv('Files/raw/cities.csv', header=true)", "workspace_id": ws, "lakehouse": lh})
    assert len(r["rows"]) == 2
    (read,) = cap.of("dataset.read")
    assert read["payload"]["datasets"] == [{"kind": "file", "lakehouse_id": lh, "name": None, "path": "Files/raw/cities.csv", "uri": None, "format": None}]
    assert cap.of("dataset.statement")        # the statement is still announced: the analyzer decides what it means


def test_statements_without_datasets_publish_nothing(dc, env, cap):
    run(dc, {"sql": "SELECT 1 AS one", "workspace_id": env["ws1"]["id"]})
    assert cap.types() == []


# ------------------------------------------------------------------------------------------------- failures publish nothing
def test_failed_statements_publish_nothing(dc, env, cap):
    ws, lh = env["ws1"]["id"], env["lh1"]["id"]
    assert dc.post("/v1/sql/execute", json={"sql": "SELEC 1", "workspace_id": ws}, headers=dc.admin).status_code == 400
    assert dc.post("/v1/sql/execute", json={"sql": "SELECT * FROM lh1.no_such_table", "workspace_id": ws}, headers=dc.admin).status_code == 404
    runtime_error = dc.post("/v1/sql/execute", json={"sql": "SELECT cast(region AS integer) FROM lh1.customers", "workspace_id": ws, "lakehouse": lh}, headers=dc.admin)
    assert runtime_error.status_code >= 400
    assert dc.post("/v1/sql/execute", json={"sql": "SELECT * FROM lh2.secrets", "workspace_id": env["ws2"]["id"]},
                   headers=dc.user("viewer-x", "Viewer", workspace_id=ws)).status_code in (403, 404)
    assert cap.types() == []
    hist = dc.get("/v1/sql/history", params={"mine": "false", "workspace_id": ws}, headers=dc.admin).json()["items"]
    assert any(h["status"] == "error" for h in hist)


# ------------------------------------------------------------------------------------------------- saved queries, reports, export
def test_saved_query_and_report_tiles_name_their_items(dc, env, cap):
    ws, lh = env["ws1"], env["lh1"]
    q = dc.post(f"/v1/workspaces/{ws['id']}/items", json={"type": "sql_query", "name": "regions", "definition": {
        "sql": "SELECT region, count(*) AS n FROM lh1.customers GROUP BY region", "engine": "duckdb", "lakehouse_id": lh["id"]}}, headers=dc.admin).json()
    assert dc.post(f"/v1/sql/queries/{q['id']}/run", json={"query_id": "qry_saved_0001"}, headers=dc.admin).status_code == 200
    read = cap.of("dataset.read")[-1]["payload"]
    assert read["process"] == {"kind": "sql_query", "item_id": q["id"], "job_id": None, "query_id": read["process"]["query_id"], "name": None, "run_id": None}
    cap.clear()
    rep = dc.post(f"/v1/workspaces/{ws['id']}/items", json={"type": "report", "name": "overview", "definition": {
        "title": "Overview", "tiles": [{"id": "n", "title": "Customers", "type": "kpi", "lakehouse_id": lh["id"], "width": 3, "height": 1, "sql": "SELECT count(*) AS n FROM lh1.customers"}]}},
        headers=dc.admin).json()
    assert dc.post(f"/v1/reports/{rep['id']}/run", json={}, headers=dc.admin).status_code == 200
    (read,) = [e for e in cap.of("dataset.read")]
    assert read["payload"]["process"]["kind"] == "report" and read["payload"]["process"]["item_id"] == rep["id"] and read["payload"]["process"]["name"] == "tile:n"
    assert read["payload"]["datasets"][0]["name"] == "customers"


def test_streaming_export_publishes_after_the_stream_completes(dc, env, cap):
    ws, lh = env["ws1"]["id"], env["lh1"]["id"]
    r = dc.post("/v1/sql/export", json={"sql": "SELECT * FROM lh1.sales", "workspace_id": ws, "lakehouse": lh, "format": "csv"}, headers=dc.admin)
    assert r.status_code == 200 and r.text.count("\n") >= 4
    (read,) = cap.of("dataset.read")
    assert read["payload"]["datasets"][0]["name"] == "sales" and read["payload"]["process"]["kind"] == "sql_query"


# ------------------------------------------------------------------------------------------------- Spark: write modes
class FakeCompute:
    """Stands in for the compute service: answers like a Spark SQL session."""

    def run_spark_sql(self, principal, workspace_id, sql, *, lakehouse=None, limit=1000, timeout=60):  # noqa: ANN001, ANN201, ARG002
        return {"columns": [{"name": "n", "type": "bigint"}], "rows": [[1]], "truncated": False, "stats": {}}


@pytest.mark.parametrize(("sql", "mode", "reads"), [
    ("CREATE TABLE lh1.gold AS SELECT region, count(*) AS n FROM lh1.customers GROUP BY region", "create", ["customers"]),
    ("CREATE OR REPLACE TABLE lh1.gold AS SELECT * FROM lh1.customers", "overwrite", ["customers"]),
    ("INSERT INTO lh1.gold SELECT * FROM lh1.customers", "append", ["customers"]),
    ("INSERT OVERWRITE lh1.gold SELECT * FROM lh1.customers", "overwrite", ["customers"]),
    ("MERGE INTO lh1.gold g USING lh1.customers c ON g.customer_id = c.customer_id WHEN MATCHED THEN UPDATE SET g.region = c.region", "merge", ["customers"]),
    ("UPDATE lh1.gold SET region = 'x' WHERE customer_id = 1", "update", []),
    ("DELETE FROM lh1.gold WHERE customer_id = 1", "delete", []),
])
def test_spark_writes_publish_written_with_the_mode(dc, env, cap, sql, mode, reads):
    dc.ctx.register_service("compute", FakeCompute())
    ws, lh = env["ws1"]["id"], env["lh1"]["id"]
    run(dc, {"sql": sql, "engine": "spark", "workspace_id": ws, "lakehouse": lh, "query_id": "qry_spark_0001"}, headers=env["contrib"])
    written = cap.of("dataset.written")
    assert len(written) == 1
    p = written[0]["payload"]
    assert p["engine"] == "spark" and p["mode"] == mode and p["statement_hash"] == sha1_of(sql)
    assert p["datasets"] == [{"kind": "table", "lakehouse_id": lh, "name": "gold", "path": None, "uri": None, "format": None}]
    assert p["process"]["query_id"] == "qry_spark_0001" and p["workspace_id"] == ws
    assert [d["name"] for e in cap.of("dataset.read") for d in e["payload"]["datasets"]] == reads
    (stmt,) = cap.of("dataset.statement")
    assert stmt["payload"]["dialect"] == "spark" and stmt["payload"]["default_lakehouse_id"] == lh and stmt["payload"]["sql"] == sql


def test_ddl_without_data_flow_publishes_no_written_event(dc, env, cap):
    dc.ctx.register_service("compute", FakeCompute())
    ws, lh = env["ws1"]["id"], env["lh1"]["id"]
    run(dc, {"sql": "ALTER TABLE lh1.customers ADD COLUMNS (z INT)", "engine": "spark", "workspace_id": ws, "lakehouse": lh}, headers=dc.admin)
    assert "dataset.written" not in cap.types()


# ------------------------------------------------------------------------------------------------- failure injection
def _break_publish(dc, monkeypatch):
    bus = dc.ctx.events
    real = bus.publish
    attempts: list[str] = []

    def publish(type_, payload=None, **kw):
        if type_.startswith("dataset."):
            attempts.append(type_)
            raise RuntimeError("event store is down")
        return real(type_, payload, **kw)

    monkeypatch.setattr(bus, "publish", publish)
    return attempts


def test_a_publisher_that_raises_changes_nothing_for_the_query(dc, env, monkeypatch, caplog):
    ws, lh = env["ws1"]["id"], env["lh1"]["id"]
    body = {"sql": "SELECT region, count(*) AS n FROM lh1.customers GROUP BY region ORDER BY region", "workspace_id": ws, "lakehouse": lh}
    baseline = run(dc, {**body, "query_id": "qry_baseline_01"})
    dsev.reset_emission_state()
    attempts = _break_publish(dc, monkeypatch)
    with caplog.at_level(logging.DEBUG, logger="olcf.sql"):
        broken = run(dc, {**body, "query_id": "qry_broken_0001"})
        again = run(dc, {**body, "query_id": "qry_broken_0002"})
    # same result, same status code, same stats shape, history 'ok'
    assert broken["rows"] == baseline["rows"] == again["rows"] and broken["columns"] == baseline["columns"] and broken["truncated"] == baseline["truncated"]
    assert set(broken["stats"]) == set(baseline["stats"])
    assert dc.get("/v1/sql/history/qry_broken_0001", headers=dc.admin).json()["status"] == "ok"
    # the emission was attempted (read + statement per run) and every failure was swallowed and counted
    assert attempts == ["dataset.read", "dataset.statement"] * 2
    assert dsev.emission_failures() == {"dataset.read": 2, "dataset.statement": 2}
    # logged as a warning once per kind, then at debug level only
    warnings = [r for r in caplog.records if r.name == "olcf.sql" and r.levelno == logging.WARNING and "could not be published" in r.getMessage()]
    assert sorted(w.getMessage().split(" could not")[0] for w in warnings) == ["dataset.read", "dataset.statement"]
    assert any(r.levelno == logging.DEBUG and "could not be published" in r.getMessage() for r in caplog.records)


def test_emission_cost_is_small_and_bounded_even_when_the_publisher_fails(dc, env, monkeypatch):
    ws, lh = env["ws1"]["id"], env["lh1"]["id"]
    _break_publish(dc, monkeypatch)
    svc = dc.ctx.service("sql")
    plan_holder: dict[str, Any] = {}
    real_emit = svc._emit_dataset_events

    def timed(plan):
        t0 = time.perf_counter()
        real_emit(plan)
        plan_holder["seconds"] = time.perf_counter() - t0

    monkeypatch.setattr(svc, "_emit_dataset_events", timed)
    run(dc, {"sql": "SELECT count(*) FROM lh1.customers", "workspace_id": ws, "lakehouse": lh})
    assert plan_holder["seconds"] < 0.25      # building three payloads and failing three times: no waiting on anything


def test_a_failing_subscriber_does_not_affect_the_query(dc, env, cap):
    def bad(_evt):
        raise RuntimeError("subscriber crashed")

    dc.ctx.events.subscribe(bad)
    r = run(dc, {"sql": "SELECT count(*) FROM lh1.customers", "workspace_id": env["ws1"]["id"]})
    assert r["rows"] == [[3]] and cap.types() == ["dataset.read", "dataset.statement"]


# ------------------------------------------------------------------------------------------------- Fabric identifiers (additive blocks)
TENANT = "11111111-1111-4111-8111-111111111111"
FWS = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
FNB = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
FLH = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"


def test_the_fabric_blocks_are_added_only_when_given_and_existing_keys_are_untouched():
    plain = dsev.process_descriptor("notebook", item_id="itm_1", job_id="job_1", name="n")
    assert "fabric" not in plain and "fabric" not in dsev.dataset_descriptor("table", name="t")
    block = dsev.fabric_process_block({"tenant_id": TENANT.upper(), "workspace_id": FWS.upper(), "notebook_id": FNB, "notebook_name": "  nb \n load ", "run_id": "run-0001", "extra": "x"})
    assert block == {"tenant_id": TENANT, "workspace_id": FWS, "item_id": FNB, "item_name": "nb load", "item_type": "Notebook", "run_id": "run-0001"}
    with_fab = dsev.process_descriptor("notebook", job_id="job_1", fabric=block)
    assert {k: v for k, v in with_fab.items() if k != "fabric"} == {k: v for k, v in dsev.process_descriptor("notebook", job_id="job_1").items()} and with_fab["fabric"] == block
    assert dsev.dataset_descriptor("table", name="t", fabric={"workspace_id": FWS, "lakehouse_id": FLH})["fabric"] == {"workspace_id": FWS, "lakehouse_id": FLH}


@pytest.mark.parametrize("raw", [None, "x", [], {}, {"tenant_id": TENANT}, {"workspace_id": "not-a-guid", "notebook_id": "nope"}, {"run_id": "bad run id with spaces"}])
def test_a_fabric_process_block_with_nothing_usable_is_none(raw):
    assert dsev.fabric_process_block(raw) is None


def test_values_that_are_not_what_they_claim_to_be_are_dropped_from_the_block():
    block = dsev.fabric_process_block({"tenant_id": "not a tenant", "workspace_id": FWS, "notebook_id": "42", "notebook_name": "nb", "run_id": "x" * 500})
    assert block == {"workspace_id": FWS, "item_name": "nb", "item_type": "Notebook"}


@pytest.mark.parametrize("raw", [None, {}, {"workspace_id": FWS}, {"lakehouse_id": FLH}, {"workspace_id": "Sales", "lakehouse_id": "lh_sales"}, "x"])
def test_a_fabric_lakehouse_block_needs_both_guids(raw):
    assert dsev.fabric_lakehouse_block(raw) is None


def test_a_fabric_lakehouse_block_is_lower_cased():
    assert dsev.fabric_lakehouse_block({"workspace_id": FWS.upper(), "lakehouse_id": FLH.upper(), "x": 1}) == {"workspace_id": FWS, "lakehouse_id": FLH}

