# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Catalog: discovery sync, search, annotations with ETags, statistics, lineage recording/graph/column trace, RBAC."""

from __future__ import annotations

import pyarrow as pa
import pytest


@pytest.fixture()
def cat(dc):
    ws = dc.workspace("ws1")
    lh = dc.lakehouse("ws1", "lh1")
    dc.write_table(lh["id"], "customers", pa.table({"customer_id": pa.array([1, 2, 3], pa.int64()), "region": ["eu", "us", "us"], "name": ["a", "b", "c"]}))
    dc.write_table(lh["id"], "sales", pa.table({"order_id": pa.array([1, 2, 3, 4], pa.int64()), "customer_id": pa.array([1, 1, 2, 3], pa.int64()),
                                                "amount": [10.0, 20.0, None, 40.0]}), partition_by=["customer_id"])
    r = dc.post(f"/v1/lakehouses/{lh['id']}/refresh", headers=dc.admin)
    assert r.status_code == 200, r.text
    return ws, lh, r.json()


def entries(dc, **params):
    return dc.get("/v1/catalog/tables", params=params, headers=dc.admin).json()


def test_sync_discovers_tables_with_schema_and_stats(dc, cat):
    ws, lh, res = cat
    assert sorted(res["added"]) == ["customers", "sales"] and res["missing"] == [] and res["table_count"] == 2
    items = entries(dc, lakehouse_id=lh["id"], include="columns")["items"]
    sales = next(i for i in items if i["name"] == "sales")
    assert sales["format"] == "delta" and sales["partition_columns"] == ["customer_id"] and sales["catalog"] == "default"
    assert [(c["name"], c["type"]) for c in sales["columns"]] == [("order_id", "long"), ("customer_id", "long"), ("amount", "double")]
    assert sales["canonical_uri"] == "olcf://ws1/lh1/Tables/sales" and sales["lakehouse"] == "lh1" and sales["workspace"] == "ws1"
    detail = dc.get(f"/v1/catalog/tables/{sales['id']}", headers=dc.admin)
    assert detail.headers["ETag"] and detail.json()["stats"]["row_count"] == 4 and detail.json()["stats"]["source"] == "delta_log"
    # idempotent: a second sync updates, never duplicates
    again = dc.post(f"/v1/lakehouses/{lh['id']}/refresh", headers=dc.admin).json()
    assert sorted(again["updated"]) == ["customers", "sales"] and again["added"] == []
    assert len(entries(dc)["items"]) == 2


def test_vanished_tables_are_marked_missing_and_come_back(dc, cat):
    ws, lh, _ = cat
    assert dc.delete(f"/v1/lakehouses/{lh['id']}/tables/customers", headers=dc.admin).status_code == 200
    assert [i["name"] for i in entries(dc)["items"]] == ["sales"]
    miss = entries(dc, include_missing=True)["items"]
    assert {i["name"]: i["status"] for i in miss} == {"customers": "missing", "sales": "active"}
    dc.write_table(lh["id"], "customers", pa.table({"customer_id": [1]}))
    dc.ctx.service("catalog").sync_lakehouse(dc.ctx.service("storage").get_lakehouse(lh["id"]))
    assert {i["name"]: i["status"] for i in entries(dc)["items"]} == {"customers": "active", "sales": "active"}


def test_describe_and_tag_with_etags_survive_resync(dc, cat):
    ws, lh, _ = cat
    cust = next(i for i in entries(dc)["items"] if i["name"] == "customers")
    h = dc.admin
    g = dc.get(f"/v1/catalog/tables/{cust['id']}", headers=h)
    etag = g.headers["ETag"]
    r = dc.patch(f"/v1/catalog/tables/{cust['id']}", json={"description": "Customer master", "tags": ["PII", "gold", "pii"], "column_descriptions": {"region": "Sales region"}},
                 headers={**h, "If-Match": etag})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["description"] == "Customer master" and body["tags"] == ["gold", "pii"] and body["etag"] != etag.strip('"')
    assert next(c for c in body["columns"] if c["name"] == "region")["description"] == "Sales region"
    # stale ETag -> 412, unknown column -> 400, empty -> 400, bad tags -> 400
    assert dc.patch(f"/v1/catalog/tables/{cust['id']}", json={"description": "x"}, headers={**h, "If-Match": etag}).status_code == 412
    cur = dc.get(f"/v1/catalog/tables/{cust['id']}", headers=h).headers["ETag"]
    assert dc.patch(f"/v1/catalog/tables/{cust['id']}", json={"column_descriptions": {"nope": "x"}}, headers={**h, "If-Match": cur}).status_code == 400
    assert dc.patch(f"/v1/catalog/tables/{cust['id']}", json={}, headers=h).status_code == 400
    assert dc.patch(f"/v1/catalog/tables/{cust['id']}", json={"tags": [1]}, headers=h).status_code in (400, 422)
    # human annotations survive re-discovery
    dc.post(f"/v1/lakehouses/{lh['id']}/refresh", headers=h)
    again = dc.get(f"/v1/catalog/tables/{cust['id']}", headers=h).json()
    assert again["description"] == "Customer master" and again["tags"] == ["gold", "pii"]
    assert next(c for c in again["columns"] if c["name"] == "region")["description"] == "Sales region"
    assert [i["name"] for i in entries(dc, tag="pii")["items"]] == ["customers"] and entries(dc, tag="nothing")["items"] == []


def test_search_ranks_name_over_column_over_description(dc, cat):
    ws, lh, _ = cat
    cust = next(i for i in entries(dc)["items"] if i["name"] == "customers")
    dc.patch(f"/v1/catalog/tables/{cust['id']}", json={"description": "who buys things", "tags": ["crm"]}, headers=dc.admin)
    s = lambda q, **p: dc.get("/v1/catalog/search", params={"q": q, **p}, headers=dc.admin).json()["items"]  # noqa: E731
    top = s("customers", kind="table")
    assert top[0]["name"] == "customers" and top[0]["score"] >= 100 and top[0]["canonical_uri"].endswith("/Tables/customers")
    assert [r["name"] for r in s("customer_id", kind="table")] == ["customers", "sales"]       # matched via column names
    cols = s("amount", kind="column")
    assert cols[0]["kind"] == "column" and cols[0]["name"] == "sales.amount" and cols[0]["type"] == "double"
    assert s("crm", kind="table")[0]["matched"] == ["tag"] and s("buys", kind="table")[0]["matched"] == ["description"]
    assert s("sales region_nonexistent") == []                        # every term must match
    assert s("CUSTOM")[0]["name"] == "customers"                      # case-insensitive prefix
    assert dc.get("/v1/catalog/search", params={"q": "x", "kind": "bogus"}, headers=dc.admin).status_code == 400
    assert dc.get("/v1/catalog/search", params={"q": "   "}, headers=dc.admin).status_code in (400, 422)
    assert s("100%", kind="table") == [] and s("cu_tomers", kind="table") == []    # LIKE wildcards are escaped


def test_search_includes_items(dc, cat):
    ws, lh, _ = cat
    r = dc.post("/v1/workspaces/ws1/items", json={"type": "sql_query", "name": "revenue by region", "description": "monthly revenue",
                                                  "definition": {"sql": "SELECT 1", "engine": "duckdb", "lakehouse_id": lh["id"], "parameters": []}}, headers=dc.admin)
    assert r.status_code == 201, r.text
    found = dc.get("/v1/catalog/search", params={"q": "revenue", "kind": "item"}, headers=dc.admin).json()["items"]
    assert found and found[0]["kind"] == "sql_query" and found[0]["name"] == "revenue by region"


def test_column_stats_from_the_delta_log(dc, cat):
    ws, lh, _ = cat
    sales = next(i for i in entries(dc)["items"] if i["name"] == "sales")
    base = dc.get(f"/v1/catalog/tables/{sales['id']}/stats", headers=dc.admin).json()
    assert base["row_count"] == 4 and base["column_stats"] is None and base["num_files"] == 3
    st = dc.post(f"/v1/catalog/tables/{sales['id']}/stats/refresh", headers=dc.admin).json()
    cs = st["column_stats"]
    assert cs["amount"]["min"] == 10.0 and cs["amount"]["max"] == 40.0 and cs["amount"]["null_count"] == 1
    assert cs["order_id"]["min"] == 1 and cs["order_id"]["max"] == 4
    # new data -> new version -> stale column stats are dropped until recomputed
    dc.write_table(lh["id"], "sales", pa.table({"order_id": pa.array([9], pa.int64()), "customer_id": pa.array([9], pa.int64()), "amount": [99.0]}), mode="append")
    dc.post(f"/v1/lakehouses/{lh['id']}/refresh", headers=dc.admin)
    after = dc.get(f"/v1/catalog/tables/{sales['id']}/stats", headers=dc.admin).json()
    assert after["row_count"] == 5 and after["column_stats"] is None and after["version"] != st["version"]
    assert dc.get(f"/v1/catalog/tables/{sales['id']}/stats", params={"compute": "true"}, headers=dc.admin).json()["column_stats"]["amount"]["max"] == 99.0


def test_catalogs_listing_and_default_lakehouse_resolution(dc, cat):
    ws, lh, _ = cat
    c = dc.get("/v1/catalogs", headers=dc.admin).json()["items"]
    assert c[0]["name"] == "default" and c[0]["table_count"] == 2 and c[0]["lakehouses"][0]["name"] == "lh1"
    svc = dc.ctx.service("catalog")
    principal = dc.ctx.authz  # noqa: F841
    from olcf.api.deps import authenticate

    p = authenticate(dc.ctx, dc.admin["Authorization"].split()[1])
    assert svc.default_lakehouse(p, ws["id"]).id == lh["id"]                     # the only lakehouse
    assert svc.default_lakehouse(p, ws["id"], "lh1").id == lh["id"]
    dc.lakehouse("ws1", "lh2", catalog="finance")
    from olcf.core.errors import BadRequest

    with pytest.raises(BadRequest) as e:
        svc.default_lakehouse(p, ws["id"])
    assert "lh1" in str(e.value) and "lh2" in str(e.value) and e.value.remediation
    assert svc.default_lakehouse(p, ws["id"], catalog="finance").name == "lh2"
    assert {x["name"] for x in dc.get("/v1/catalogs", headers=dc.admin).json()["items"]} == {"default", "finance"}


# ------------------------------------------------------------------------------------------------ lineage
def tbl(lh, name):
    return {"kind": "table", "lakehouse_id": lh["id"], "name": name}


def lineage(dc, dataset, **params):
    r = dc.get("/v1/catalog/lineage", params={"dataset": dataset, **params}, headers=dc.admin)
    assert r.status_code == 200, r.text
    return r.json()


def test_record_lineage_graph_dedup_and_column_trace(dc, cat):
    ws, lh, _ = cat
    h = dc.admin
    svc = dc.ctx.service("catalog")
    # silver <- bronze (complete mapping), gold <- silver (partial: one column unknown)
    body = {"workspace_id": ws["id"], "sources": [tbl(lh, "bronze")], "targets": [tbl(lh, "silver")], "via": {"kind": "notebook", "name": "etl", "ref": "itm_nb1"},
            "columns": [{"target": "amount", "sources": [{"source": 0, "column": "amt"}], "transform": "direct"}], "column_status": "complete"}
    r = dc.post("/v1/catalog/lineage", json=body, headers=h)
    assert r.status_code == 201 and len(r.json()["edges"]) == 1
    dc.post("/v1/catalog/lineage", json=body, headers=h)                       # same run again -> same edge, run_count bumped
    r2 = dc.post("/v1/catalog/lineage", json={"workspace_id": ws["id"], "sources": [tbl(lh, "silver")], "targets": [tbl(lh, "gold")], "via": {"kind": "job", "ref": "job_1"},
                                              "columns": [{"target": "total", "sources": [{"source": 0, "column": "amount"}], "transform": "aggregate"},
                                                          {"target": "mystery", "sources": [], "transform": "constant"}], "column_status": "partial"}, headers=h)
    assert r2.status_code == 201
    g = lineage(dc, "olcf://ws1/lh1/Tables/gold", direction="up", depth=5)
    assert {n["key"].split(":")[-1] for n in g["nodes"]} == {"gold", "silver", "bronze"} and len(g["edges"]) == 2 and g["truncated"] is False
    assert next(n for n in g["nodes"] if n["root"])["name"] == "gold" and next(n for n in g["nodes"] if n["name"] == "bronze")["lakehouse"] == "lh1"
    first = next(e for e in g["edges"] if e["via"]["kind"] == "notebook")
    assert first["run_count"] == 2 and first["column_lineage"]["status"] == "complete" and first["via"]["name"] == "etl"
    down = lineage(dc, "olcf://ws1/lh1/Tables/bronze", direction="down", depth=1)
    assert [n["name"] for n in down["nodes"]] == ["bronze", "silver"] or {n["name"] for n in down["nodes"]} == {"bronze", "silver"}
    assert len(lineage(dc, "olcf://ws1/lh1/Tables/silver", direction="both")["edges"]) == 2
    # proven column trace and honest gaps
    t = lineage(dc, "olcf://ws1/lh1/Tables/gold", direction="up", depth=5, column="total")["column_trace"]
    assert t["status"] == "complete" and [h_["from"] for h_ in t["paths"][0]["hops"]] == [{"dataset": f"table:{lh['id']}:silver", "column": "amount"},
                                                                                     {"dataset": f"table:{lh['id']}:bronze", "column": "amt"}]
    t2 = lineage(dc, "olcf://ws1/lh1/Tables/gold", direction="up", column="unmapped")["column_trace"]
    assert t2["status"] == "unknown" and t2["paths"] == [] and "partial" in t2["unresolved"][0]["reason"]
    const = lineage(dc, "olcf://ws1/lh1/Tables/gold", direction="up", column="mystery")["column_trace"]
    assert const["paths"][0]["origin"]["transform"] == "constant"
    # depth limiting
    assert len(lineage(dc, "olcf://ws1/lh1/Tables/gold", direction="up", depth=1)["edges"]) == 1
    # dataset by catalog entry id and by lakehouse-id path
    sales = next(i for i in entries(dc)["items"] if i["name"] == "sales")
    assert lineage(dc, sales["id"])["dataset"]["name"] == "sales"
    assert lineage(dc, f"{lh['id']}/Tables/gold", direction="up")["dataset"]["name"] == "gold"
    assert svc is not None


def test_lineage_validation_and_self_edges(dc, cat):
    ws, lh, _ = cat
    h = dc.admin
    post = lambda **kw: dc.post("/v1/catalog/lineage", json={"workspace_id": ws["id"], **kw}, headers=h)  # noqa: E731
    assert post(sources=[{"kind": "weird"}], targets=[tbl(lh, "x")]).status_code == 400
    assert post(sources=[{"kind": "table", "name": "x"}], targets=[tbl(lh, "x")]).status_code == 400
    assert post(sources=[tbl(lh, "a")], targets=[tbl(lh, "b")], via={"kind": "teleport"}).status_code == 400
    assert post(sources=[tbl(lh, "a")], targets=[tbl(lh, "b")], column_status="great").status_code == 400
    assert post(sources=[tbl(lh, "a")], targets=[tbl(lh, "a")]).json()["edges"] == []           # self edge ignored
    ext = post(sources=[{"kind": "external", "uri": "s3://raw/landing/"}, {"kind": "file", "lakehouse_id": lh["id"], "path": "Files/raw/a.csv"}], targets=[tbl(lh, "b")])
    assert ext.status_code == 201 and len(ext.json()["edges"]) == 2
    g = lineage(dc, "olcf://ws1/lh1/Tables/b", direction="up")
    assert {n["kind"] for n in g["nodes"]} == {"table", "external", "file"}
    assert next(n for n in g["nodes"] if n["kind"] == "file")["canonical_uri"] == "olcf://ws1/lh1/Files/raw/a.csv"
    assert dc.get("/v1/catalog/lineage", params={"dataset": "nonsense"}, headers=h).status_code == 400
    assert dc.get("/v1/catalog/lineage", params={"dataset": "olcf://ws1/lh1/Tables/b", "direction": "sideways"}, headers=h).status_code == 422


def test_sql_lineage_parse_and_record(dc, cat):
    ws, lh, _ = cat
    h = dc.admin
    sql = "CREATE TABLE gold AS SELECT c.region, SUM(s.amount) AS revenue, COUNT(*) AS orders FROM sales s JOIN customers c ON c.customer_id = s.customer_id GROUP BY c.region"
    r = dc.post("/v1/catalog/lineage/parse", json={"workspace_id": ws["id"], "sql": sql, "default_lakehouse": "lh1"}, headers=h)
    assert r.status_code == 200, r.text
    st = r.json()["statements"][0]
    assert st["statement"] == "ctas" and st["status"] == "complete"
    assert sorted(d["name"] for d in st["source_datasets"]) == ["customers", "sales"] and st["target_datasets"] == [tbl(lh, "gold")]
    assert {c["target"]: c["transform"] for c in st["columns"]} == {"region": "direct", "revenue": "aggregate", "orders": "aggregate"}
    assert "edges" not in r.json()
    rec = dc.post("/v1/catalog/lineage/parse", json={"workspace_id": ws["id"], "sql": sql, "default_lakehouse": "lh1", "record": True}, headers=h).json()
    assert len(rec["edges"]) == 2 and all(e["column_status"] == "complete" for e in rec["edges"])
    t = lineage(dc, "olcf://ws1/lh1/Tables/gold", direction="up", column="revenue")["column_trace"]
    assert t["status"] == "complete" and t["paths"][0]["origin"] == {"dataset": f"table:{lh['id']}:sales", "column": "amount"}
    # lakehouse-qualified names resolve, read-only SQL records nothing, bad SQL is a 400
    q = dc.post("/v1/catalog/lineage/parse", json={"workspace_id": ws["id"], "sql": "SELECT * FROM lh1.sales"}, headers=h).json()["statements"][0]
    assert q["source_datasets"] == [tbl(lh, "sales")] and q["target_datasets"] == []
    assert dc.post("/v1/catalog/lineage/parse", json={"workspace_id": ws["id"], "sql": "SELECT 1", "record": True}, headers=h).status_code == 400
    assert dc.post("/v1/catalog/lineage/parse", json={"workspace_id": ws["id"], "sql": "SELEC (("}, headers=h).status_code == 400
    # unresolved names become external datasets instead of being dropped or guessed
    resp = dc.post("/v1/catalog/lineage/parse", json={"workspace_id": ws["id"], "sql": "INSERT INTO gold SELECT * FROM somewhere.other", "default_lakehouse": "lh1"}, headers=h)
    assert resp.status_code == 200, resp.text
    ext = resp.json()["statements"][0]
    assert ext["source_datasets"] == [{"kind": "external", "name": "somewhere.other"}] and ext["status"] == "unknown"


def test_rbac_and_workspace_isolation(dc, cat):
    ws, lh, _ = cat
    other = dc.workspace("ws2")
    viewer = dc.user("vic", "Viewer", workspace_id=ws["id"])
    stranger = dc.user("sam", "Viewer", workspace_id=other["id"])
    contributor = dc.user("carl", "Contributor", workspace_id=ws["id"])
    cust = next(i for i in entries(dc)["items"] if i["name"] == "customers")
    # viewers read ...
    assert [i["name"] for i in dc.get("/v1/catalog/tables", headers=viewer).json()["items"]] == ["customers", "sales"]
    assert dc.get(f"/v1/catalog/tables/{cust['id']}", headers=viewer).status_code == 200
    # ... but cannot annotate, record lineage or sync
    assert dc.patch(f"/v1/catalog/tables/{cust['id']}", json={"description": "x"}, headers=viewer).status_code == 403
    assert dc.post("/v1/catalog/lineage", json={"workspace_id": ws["id"], "sources": [tbl(lh, "a")], "targets": [tbl(lh, "b")]}, headers=viewer).status_code == 403
    assert dc.post("/v1/catalog/sync", params={"workspace_id": ws["id"]}, headers=viewer).status_code == 403
    assert dc.patch(f"/v1/catalog/tables/{cust['id']}", json={"description": "by contributor"}, headers=contributor).status_code == 200
    # another workspace sees nothing and cannot fetch by id (authorised against the entry's stored workspace)
    assert dc.get("/v1/catalog/tables", headers=stranger).json()["items"] == []
    assert dc.get(f"/v1/catalog/tables/{cust['id']}", headers=stranger).status_code == 403
    assert dc.get("/v1/catalog/search", params={"q": "customers"}, headers=stranger).json()["items"] == []
    assert dc.get(f"/v1/catalog/tables/{cust['id']}/stats", headers=stranger).status_code == 403
    assert dc.get("/v1/catalog/lineage", params={"dataset": "olcf://ws1/lh1/Tables/customers"}, headers=stranger).status_code == 403
    assert dc.get("/v1/catalogs", headers=stranger).json()["items"] == []
    # lineage edges of ws1 are invisible to the stranger even when the dataset key is known
    dc.post("/v1/catalog/lineage", json={"workspace_id": ws["id"], "sources": [tbl(lh, "a")], "targets": [tbl(lh, "b")]}, headers=dc.admin)
    svc = dc.ctx.service("catalog")
    from olcf.api.deps import authenticate

    p = authenticate(dc.ctx, stranger["Authorization"].split()[1])
    assert svc.lineage(p, tbl(lh, "b"))["edges"] == []
    assert dc.get("/v1/catalog/tables/cat_nonexistent", headers=dc.admin).status_code == 404


def test_deleting_a_lakehouse_removes_its_catalog_entries(dc, cat):
    ws, lh, _ = cat
    assert len(entries(dc)["items"]) == 2
    assert dc.delete(f"/v1/items/{lh['id']}", headers=dc.admin).status_code in (200, 204)
    assert entries(dc)["items"] == []
