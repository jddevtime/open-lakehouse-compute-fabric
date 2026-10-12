# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Reports: definition validation, parallel tile execution, filters, per-tile errors, caching, authorisation, lineage."""

from __future__ import annotations

import datetime as dt
import threading
import time
from typing import Any

import pyarrow as pa
import pytest


@pytest.fixture()
def rp(dc):
    ws1, ws2 = dc.workspace("ws1"), dc.workspace("ws2")
    lh1, lh2 = dc.lakehouse("ws1", "lh1"), dc.lakehouse("ws2", "lh2")
    days = [dt.date(2024, 1, 1) + dt.timedelta(days=i) for i in range(60)]
    n = len(days)
    dc.write_table(lh1["id"], "sales", pa.table({
        "order_date": pa.array(days, pa.date32()), "category": ["toys" if i % 2 else "books" for i in range(n)],
        "channel": ["web" if i % 3 else "store" for i in range(n)], "amount": pa.array([float(i + 1) for i in range(n)])}))
    dc.write_table(lh2["id"], "secrets", pa.table({"v": [1]}))
    return {"ws1": ws1, "ws2": ws2, "lh1": lh1, "lh2": lh2, "viewer": dc.user("rv", "Viewer", workspace_id=ws1["id"]),
            "other": dc.user("ro", "Viewer", workspace_id=ws2["id"])}


def definition(lh: dict[str, Any], **over: Any) -> dict[str, Any]:
    base = {
        "title": "Sales overview", "refresh_seconds": None,
        "filters": [
            {"name": "period", "label": "Period", "type": "date_range", "default": {"from": "2024-01-01", "to": "2024-02-29"}},
            {"name": "category", "label": "Category", "type": "select", "default": None, "options_sql": "SELECT DISTINCT category FROM lh1.sales ORDER BY 1", "options_lakehouse_id": lh["id"]},
        ],
        "tiles": [
            {"id": "revenue", "title": "Revenue", "type": "kpi", "lakehouse_id": lh["id"], "width": 3, "height": 1,
             "sql": "SELECT sum(amount) AS revenue FROM lh1.sales WHERE order_date BETWEEN :period_from AND :period_to AND category = coalesce(:category, category)"},
            {"id": "by_cat", "title": "By category", "type": "bar", "lakehouse_id": lh["id"], "x": "category", "y": "revenue", "width": 6, "height": 3,
             "sql": "SELECT category, sum(amount) AS revenue FROM lh1.sales WHERE order_date BETWEEN :period_from AND :period_to GROUP BY category ORDER BY category"},
            {"id": "monthly", "title": "Monthly", "type": "line", "lakehouse_id": lh["id"], "x": "month", "y": "revenue", "width": 6, "height": 3,
             "sql": "SELECT date_trunc('month', order_date) AS month, sum(amount) AS revenue FROM lh1.sales WHERE order_date BETWEEN :period_from AND :period_to "
                    "AND category = coalesce(:category, category) GROUP BY 1 ORDER BY 1"},
        ],
    }
    base.update(over)
    return base


def make(dc, ws, name, defn, headers=None, expect=201):
    r = dc.post(f"/v1/workspaces/{ws['id']}/items", json={"type": "report", "name": name, "definition": defn}, headers=headers or dc.admin)
    assert r.status_code == expect, r.text
    return r.json()


def run(dc, rid, body=None, headers=None, expect=200):
    r = dc.post(f"/v1/reports/{rid}/run", json=body or {}, headers=headers or dc.admin)
    assert r.status_code == expect, r.text
    return r.json()


def test_run_returns_typed_tiles_filters_and_timing(dc, rp):
    rep = make(dc, rp["ws1"], "overview", definition(rp["lh1"]))
    out = run(dc, rep["id"])
    assert out["errors"] == 0 and out["partial"] is False and out["report"]["title"] == "Sales overview"
    tiles = {t["id"]: t for t in out["tiles"]}
    assert list(tiles) == ["revenue", "by_cat", "monthly"]
    assert tiles["revenue"]["columns"][0]["name"] == "revenue" and tiles["revenue"]["columns"][0]["kind"] == "float"
    assert tiles["revenue"]["rows"] == [[float(sum(range(1, 61)))]]
    assert [r[0] for r in tiles["by_cat"]["rows"]] == ["books", "toys"] and tiles["by_cat"]["view"]["x"] == "category"
    assert [r[0][:7] for r in tiles["monthly"]["rows"]] == ["2024-01", "2024-02"]
    assert all(isinstance(t["elapsed_ms"], int) and t["error"] is None for t in out["tiles"])
    f = {x["name"]: x for x in out["filters"]}
    assert f["period"]["value"] == {"from": "2024-01-01", "to": "2024-02-29"} and f["category"]["options"] == ["books", "toys"]


def test_filters_change_results_and_are_validated(dc, rp):
    rep = make(dc, rp["ws1"], "overview", definition(rp["lh1"]))
    out = run(dc, rep["id"], {"filters": {"period": {"from": "2024-01-01", "to": "2024-01-10"}, "category": "toys"}})
    rev = out["tiles"][0]["rows"][0][0]
    assert rev == float(sum(i + 1 for i in range(10) if i % 2))          # toys on odd indices within the first 10 days
    for bad, code in [({"nope": 1}, 400), ({"category": "cars"}, 400), ({"category": ["toys", "books"]}, 400), ({"period": {"from": "2024-03-01", "to": "2024-01-01"}}, 400),
                      ({"period": "yesterday"}, 400), ({"period": {"from": "garbage", "to": "2024-01-01"}}, 400)]:
        assert dc.post(f"/v1/reports/{rep['id']}/run", json={"filters": bad}, headers=dc.admin).status_code == code, bad


def test_a_failing_tile_does_not_fail_the_report(dc, rp):
    d = definition(rp["lh1"])
    d["tiles"].append({"id": "broken", "title": "Broken", "type": "table", "lakehouse_id": rp["lh1"]["id"], "sql": "SELECT nope FROM lh1.sales"})
    d["tiles"].append({"id": "missing", "title": "Missing table", "type": "table", "lakehouse_id": rp["lh1"]["id"], "sql": "SELECT * FROM lh1.not_a_table"})
    rep = make(dc, rp["ws1"], "partial", d)
    out = run(dc, rep["id"])
    assert out["errors"] == 2 and out["partial"] is True
    tiles = {t["id"]: t for t in out["tiles"]}
    assert tiles["revenue"]["error"] is None and tiles["revenue"]["rows"]
    assert tiles["broken"]["error"]["code"] == "bad_request" and tiles["broken"]["rows"] == []
    assert tiles["missing"]["error"]["code"] == "not_found"


def test_tile_on_a_lakehouse_the_caller_cannot_read_errors_only_that_tile(dc, rp):
    d = definition(rp["lh1"])
    d["tiles"].append({"id": "foreign", "title": "Foreign", "type": "table", "lakehouse_id": rp["lh2"]["id"], "sql": "SELECT * FROM lh2.secrets"})
    rep = make(dc, rp["ws1"], "foreign", d)
    out = run(dc, rep["id"], headers=rp["viewer"])
    t = {x["id"]: x for x in out["tiles"]}
    assert t["foreign"]["error"]["code"] in ("forbidden", "not_found") and t["foreign"]["rows"] == [] and t["revenue"]["error"] is None
    assert "secrets" not in str(t["foreign"]["rows"])
    # an admin of everything sees the tile
    assert run(dc, rep["id"])["errors"] == 0


def test_report_access_requires_item_read(dc, rp):
    rep = make(dc, rp["ws1"], "private", definition(rp["lh1"]))
    assert dc.post(f"/v1/reports/{rep['id']}/run", json={}, headers=rp["other"]).status_code in (403, 404)
    assert run(dc, rep["id"], headers=rp["viewer"])["errors"] == 0
    assert dc.post("/v1/reports/itm_doesnotexist/run", json={}, headers=dc.admin).status_code == 404
    nb = dc.post(f"/v1/workspaces/{rp['ws1']['id']}/items", json={"type": "sql_query", "name": "q", "definition": {"sql": "SELECT 1"}}, headers=dc.admin).json()
    assert dc.post(f"/v1/reports/{nb['id']}/run", json={}, headers=dc.admin).status_code == 400
    assert [r["name"] for r in dc.get("/v1/reports", headers=rp["viewer"]).json()["items"]] == ["private"]
    assert dc.get("/v1/reports", headers=rp["other"]).json()["items"] == []


def test_results_are_cached_per_filter_values_and_refresh_bypasses(dc, rp):
    rep = make(dc, rp["ws1"], "cached", definition(rp["lh1"]))
    first = run(dc, rep["id"])
    assert first["cache"]["hits"] == 0
    second = run(dc, rep["id"])
    assert second["cache"]["hits"] == 3 and all(t["from_cache"] for t in second["tiles"]) and second["tiles"][0]["rows"] == first["tiles"][0]["rows"]
    other = run(dc, rep["id"], {"filters": {"category": "toys"}})
    assert other["tiles"][0]["from_cache"] is False
    forced = run(dc, rep["id"], {"refresh": True})
    assert forced["cache"]["hits"] == 0
    # changing the report definition invalidates the cache (the key carries the item version/etag)
    cur = dc.get(f"/v1/items/{rep['id']}", headers=dc.admin)
    assert cur.headers["ETag"]
    d2 = definition(rp["lh1"])
    d2["tiles"][0]["sql"] = "SELECT 1 AS revenue"
    r = dc.put(f"/v1/items/{rep['id']}", json={"definition": d2}, headers={**dc.admin, "If-Match": cur.headers["ETag"]})
    assert r.status_code == 200, r.text
    assert run(dc, rep["id"])["tiles"][0]["rows"] == [[1]]
    # cached data is never served to someone who may not read the tiles' tables
    d3 = definition(rp["lh1"])
    d3["tiles"].append({"id": "foreign", "title": "F", "type": "table", "lakehouse_id": rp["lh2"]["id"], "sql": "SELECT * FROM lh2.secrets"})
    rep3 = make(dc, rp["ws1"], "cached2", d3)
    assert run(dc, rep3["id"])["errors"] == 0                                    # admin primes the cache
    again = run(dc, rep3["id"], headers=rp["viewer"])
    assert {t["id"]: t for t in again["tiles"]}["foreign"]["error"] is not None


def test_tiles_run_in_parallel_with_a_bound(dc, rp):
    d = definition(rp["lh1"])
    d["tiles"] = [{"id": f"t{i}", "title": f"T{i}", "type": "kpi", "lakehouse_id": rp["lh1"]["id"], "sql": f"SELECT {i} AS v"} for i in range(8)]
    d["filters"] = []
    rep = make(dc, rp["ws1"], "parallel", d)
    svc = dc.ctx.service("sql")
    real = svc.execute
    live = {"now": 0, "max": 0}
    lock = threading.Lock()

    def slow(principal, req):  # noqa: ANN001, ANN202
        with lock:
            live["now"] += 1
            live["max"] = max(live["max"], live["now"])
        try:
            time.sleep(0.3)
            return real(principal, req)
        finally:
            with lock:
                live["now"] -= 1

    svc.execute = slow  # type: ignore[method-assign]
    try:
        t0 = time.perf_counter()
        out = run(dc, rep["id"], {"refresh": True})
        wall = time.perf_counter() - t0
    finally:
        svc.execute = real  # type: ignore[method-assign]
    assert out["errors"] == 0 and [t["rows"][0][0] for t in out["tiles"]] == list(range(8))     # order preserved
    assert 2 <= live["max"] <= 4 and wall < 8 * 0.3                                           # parallel, bounded by sql.report_parallelism (4)


def test_report_definition_validation_lists_every_problem(dc, rp):
    lh = rp["lh1"]
    bad = definition(lh, filters=[{"name": "1x", "type": "select"}, {"name": "d", "type": "date_range", "default": {"from": "x", "to": "y"}}, {"name": "d", "type": "weird"}],
                     tiles=[{"id": "a", "type": "pie", "sql": "SELECT 1", "lakehouse_id": lh["id"]},
                            {"id": "a", "type": "kpi", "sql": "DELETE FROM lh1.sales", "lakehouse_id": lh["id"]},
                            {"id": "c", "type": "kpi", "sql": "SELECT :undeclared", "lakehouse_id": lh["id"], "width": 99},
                            {"id": "d", "type": "kpi", "sql": "SELECT 1", "engine": "oracle"}])
    r = dc.post(f"/v1/workspaces/{rp['ws1']['id']}/items", json={"type": "report", "name": "bad", "definition": bad}, headers=dc.admin)
    assert r.status_code == 422
    problems = " | ".join(r.json()["error"]["details"]["problems"])
    for needle in ("filters[0]", "default must be", "type must be one of", "duplicate id 'a'", "must be read-only", "width must be", "lakehouse_id is required", "engine must be one of"):
        assert needle in problems, (needle, problems)
    for d in ({"title": "x", "tiles": []}, {"tiles": [{"id": "a", "type": "kpi", "sql": "SELECT 1", "lakehouse_id": lh["id"]}]}, {"title": "x", "bogus": 1, "tiles": []}):
        assert dc.post(f"/v1/workspaces/{rp['ws1']['id']}/items", json={"type": "report", "name": "bad", "definition": d}, headers=dc.admin).status_code == 422
    ok = make(dc, rp["ws1"], "ok", definition(lh))
    assert ok["definition"]["tiles"][0]["engine"] == "duckdb" and ok["definition"]["filters"][0]["type"] == "date_range"


def test_running_a_report_records_lineage_to_the_report(dc, rp):
    rep = make(dc, rp["ws1"], "lineage", definition(rp["lh1"]))
    run(dc, rep["id"])
    g = dc.get("/v1/catalog/lineage", params={"dataset": f"{rp['lh1']['id']}/Tables/sales", "direction": "down"}, headers=dc.admin).json()
    assert any(n.get("kind") == "report" and n.get("item_id") == rep["id"] for n in g["nodes"]), g
    up = dc.get("/v1/catalog/lineage", params={"dataset": rep["id"], "direction": "up"}, headers=dc.admin).json()
    assert any(n.get("name") == "sales" for n in up["nodes"]), up
