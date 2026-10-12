# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""sqlglot lineage extraction: table level always, column level only where provable."""

from __future__ import annotations

import pytest

from olcf.catalog.lineage import TableRef, analyze

SCHEMAS = {
    ("fact_sales",): {"order_id": "bigint", "customer_id": "bigint", "product_id": "bigint", "amount": "double", "qty": "int", "order_ts": "timestamp"},
    ("dim_customer",): {"customer_id": "bigint", "region": "varchar", "name": "varchar"},
    ("gold_sales",): {"region": "varchar", "revenue": "double", "orders": "bigint"},
}


def schema_for(ref: TableRef):
    return SCHEMAS.get(ref.lowered()[-1:])


def one(sql, **kw):
    out = analyze(sql, schema_for=schema_for, **kw)
    assert len(out) == 1
    return out[0]


def cols(res):
    return {c.target: (sorted((res.sources[i].dotted, col) for i, col in c.sources), c.transform) for c in res.columns}


def test_plain_select_is_table_level_only():
    r = one("SELECT * FROM lh.fact_sales f JOIN lh.dim_customer c ON c.customer_id = f.customer_id")
    assert r.statement == "select" and r.status == "not_applicable" and r.targets == [] and r.columns == []
    assert sorted(s.dotted for s in r.sources) == ["lh.dim_customer", "lh.fact_sales"]


def test_ctas_with_cte_join_aggregate_is_fully_traced():
    r = one("""CREATE TABLE gold_sales AS
               WITH s AS (SELECT customer_id, amount * qty AS rev FROM fact_sales WHERE amount > 0)
               SELECT c.region AS region, SUM(s.rev) AS revenue, COUNT(*) AS orders
               FROM s JOIN dim_customer c ON c.customer_id = s.customer_id GROUP BY c.region""")
    assert r.statement == "ctas" and [t.dotted for t in r.targets] == ["gold_sales"] and r.status == "complete"
    assert sorted(s.dotted for s in r.sources) == ["dim_customer", "fact_sales"]      # CTE name 's' is not a table
    c = cols(r)
    assert c["region"] == ([("dim_customer", "region")], "direct")
    assert c["revenue"] == ([("fact_sales", "amount"), ("fact_sales", "qty")], "aggregate")
    assert c["orders"] == ([], "aggregate")


def test_select_star_is_expanded_when_schema_known_and_unknown_otherwise():
    known = one("CREATE TABLE copy1 AS SELECT * FROM dim_customer")
    assert known.status == "complete" and set(cols(known)) == {"customer_id", "region", "name"}
    unknown = one("CREATE TABLE copy2 AS SELECT * FROM mystery_table")
    assert unknown.status == "unknown" and unknown.columns == [] and any("SELECT *" in n for n in unknown.notes)


def test_partial_when_some_columns_cannot_be_resolved():
    r = one("CREATE TABLE x AS SELECT f.amount AS a, customer_id AS ambiguous FROM unknown_a f JOIN unknown_b g ON f.k = g.k")
    c = {x.target: x for x in r.columns}
    assert c["a"].resolved and c["a"].transform == "direct"
    assert not c["ambiguous"].resolved and c["ambiguous"].transform == "unresolved"
    assert r.status == "partial"


def test_insert_positional_mapping_uses_target_schema():
    r = one("INSERT INTO gold_sales SELECT c.region, SUM(f.amount), COUNT(*) FROM fact_sales f JOIN dim_customer c USING (customer_id) GROUP BY 1")
    assert r.statement == "insert" and [t.dotted for t in r.targets] == ["gold_sales"]
    assert [c.target for c in r.columns] == ["region", "revenue", "orders"]    # mapped onto the target's declared columns
    explicit = one("INSERT INTO gold_sales (region, revenue) SELECT region, 1 FROM dim_customer")
    assert cols(explicit)["revenue"] == ([], "constant")
    assert one("INSERT INTO t VALUES (1, 2)").status == "not_applicable"


def test_union_traces_both_branches():
    r = one("CREATE TABLE u AS SELECT customer_id FROM fact_sales UNION ALL SELECT customer_id FROM dim_customer")
    assert cols(r)["customer_id"] == ([("dim_customer", "customer_id"), ("fact_sales", "customer_id")], "direct")


def test_view_merge_update_delete():
    v = one("CREATE VIEW v AS SELECT region FROM dim_customer")
    assert v.statement == "create_view" and v.status == "complete"
    m = one("MERGE INTO gold_sales t USING (SELECT 1) s ON t.region = 'x' WHEN MATCHED THEN UPDATE SET t.revenue = 1")
    assert m.statement == "merge" and [t.dotted for t in m.targets] == ["gold_sales"]
    m2 = one("MERGE INTO gold_sales t USING dim_customer s ON t.region = s.region WHEN MATCHED THEN UPDATE SET t.orders = s.customer_id "
             "WHEN NOT MATCHED THEN INSERT (region, revenue) VALUES (s.region, 0)", dialect="spark")
    assert [s.dotted for s in m2.sources] == ["dim_customer"] and m2.status == "complete"
    assert cols(m2)["orders"] == ([("dim_customer", "customer_id")], "direct") and cols(m2)["revenue"] == ([], "constant")
    u = one("UPDATE gold_sales SET revenue = revenue * 2 WHERE region IN (SELECT region FROM dim_customer)")
    assert u.statement == "update" and [s.dotted for s in u.sources] == ["dim_customer"]
    d = one("DELETE FROM gold_sales WHERE region IN (SELECT region FROM dim_customer)")
    assert d.statement == "delete" and [t.dotted for t in d.targets] == ["gold_sales"] and [s.dotted for s in d.sources] == ["dim_customer"]


def test_file_sources_and_multi_statement():
    r = analyze("CREATE TABLE raw AS SELECT * FROM read_parquet('Files/raw/a.parquet'); SELECT 1", dialect="duckdb")
    assert len(r) == 2
    assert r[0].sources[0].kind == "file" and r[0].sources[0].uri == "Files/raw/a.parquet"
    assert r[1].statement == "select"


def test_dialects_and_errors():
    s = one("CREATE TABLE t AS SELECT region FROM dim_customer", dialect="spark")
    assert s.status == "complete"
    with pytest.raises(ValueError, match="cannot parse"):
        analyze("SELEC broken ((")
    assert analyze("SET x = 1")[0].statement in ("other", "select")


def test_to_dict_is_json_ready():
    import json

    d = one("CREATE TABLE gold_sales AS SELECT region, COUNT(*) AS orders FROM dim_customer GROUP BY region").to_dict()
    json.dumps(d)
    assert d["columns"][0]["sources"] == [{"source": "dim_customer", "column": "region"}]
