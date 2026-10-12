# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The shipped sample workspace (``olcf.sample``) analysed end to end: its notebooks and its saved SQL queries.

These are the contents a new user sees first, so the exact lineage they get is pinned here (read-only use of ``olcf.sample``).
"""

from __future__ import annotations

from typing import Any

import pytest

from olcf.intelligence.lineage.engine import analyze_definition
from olcf.intelligence.lineage.notebook import analyze_notebook
from olcf.intelligence.model import AssetRef, DefinitionFact, Facts
from olcf.sample import notebooks as sample_notebooks
from olcf.sample import reports as sample_reports
from tests.intelligence.lineage.support import (
    edges,
    gap_kinds,
    gaps,
    lab,
)

LH = {"name": "retail_lakehouse", "id": "itm_lh"}


def nb(name: str, **ctx: Any) -> Facts:
    """Analyse a sample notebook (by its key in ``notebooks()``) with the sample lakehouse as default."""
    return analyze_notebook(sample_notebooks.notebooks()[name], context={"item_id": "itm_nb", "item_name": name, "default_lakehouse": dict(LH), **ctx})


ETL_TRANSFORMS = [
    "col:dim_customer.loyalty_tier -> col:silver_sales_enriched.loyalty_tier",
    "col:dim_customer.segment -> col:silver_sales_enriched.customer_segment",
    "col:dim_product.brand -> col:silver_sales_enriched.brand",
    "col:dim_product.category -> col:gold_category_summary.category",
    "col:dim_product.category -> col:silver_sales_enriched.category",
    "col:dim_product.product_name -> col:silver_sales_enriched.product_name",
    "col:dim_product.subcategory -> col:silver_sales_enriched.subcategory",
    "col:dim_store.region -> col:silver_sales_enriched.region",
    "col:dim_store.store_name -> col:silver_sales_enriched.store_name",
    "col:files/raw/sales.customer_id -> col:silver_sales_enriched.customer_id",
    "col:files/raw/sales.order_date -> col:silver_sales_enriched.month",
    "col:files/raw/sales.order_date -> col:silver_sales_enriched.order_date",
    "col:files/raw/sales.order_date -> col:silver_sales_enriched.year",
    "col:files/raw/sales.order_ts -> col:silver_sales_enriched.order_ts",
    "col:files/raw/sales.product_id -> col:silver_sales_enriched.product_id",
    "col:files/raw/sales.store_id -> col:silver_sales_enriched.store_id",
    "col:silver_sales_enriched.customer_id -> col:gold_customer_ltv.customer_id",
    "col:silver_sales_enriched.customer_id -> col:gold_customer_ltv.ltv_segment",
    "col:silver_sales_enriched.customer_id -> col:gold_daily_sales.customers",
    "col:silver_sales_enriched.gross_amount -> col:gold_daily_sales.gross_revenue",
    "col:silver_sales_enriched.net_amount -> col:gold_category_summary.margin_pct",
    "col:silver_sales_enriched.net_amount -> col:gold_category_summary.net_revenue",
    "col:silver_sales_enriched.net_amount -> col:gold_category_summary.revenue_rank",
    "col:silver_sales_enriched.net_amount -> col:gold_category_summary.revenue_share_pct",
    "col:silver_sales_enriched.net_amount -> col:gold_customer_ltv.avg_order_value",
    "col:silver_sales_enriched.net_amount -> col:gold_customer_ltv.lifetime_revenue",
    "col:silver_sales_enriched.net_amount -> col:gold_customer_ltv.ltv_segment",
    "col:silver_sales_enriched.net_amount -> col:gold_daily_sales.avg_order_value",
    "col:silver_sales_enriched.net_amount -> col:gold_daily_sales.net_revenue",
    "col:silver_sales_enriched.order_date -> col:gold_customer_ltv.first_order_date",
    "col:silver_sales_enriched.order_date -> col:gold_customer_ltv.last_order_date",
    "col:silver_sales_enriched.order_date -> col:gold_customer_ltv.recency_days",
    "col:silver_sales_enriched.order_date -> col:gold_daily_sales.sale_date",
    "col:silver_sales_enriched.order_id -> col:gold_category_summary.orders",
    "col:silver_sales_enriched.order_id -> col:gold_customer_ltv.avg_order_value",
    "col:silver_sales_enriched.order_id -> col:gold_customer_ltv.orders",
    "col:silver_sales_enriched.order_id -> col:gold_daily_sales.avg_order_value",
    "col:silver_sales_enriched.order_id -> col:gold_daily_sales.orders",
    "col:silver_sales_enriched.profit -> col:gold_category_summary.margin_pct",
    "col:silver_sales_enriched.profit -> col:gold_category_summary.profit",
    "col:silver_sales_enriched.profit -> col:gold_customer_ltv.lifetime_profit",
    "col:silver_sales_enriched.profit -> col:gold_daily_sales.profit",
    "col:silver_sales_enriched.quantity -> col:gold_category_summary.units",
    "col:silver_sales_enriched.quantity -> col:gold_customer_ltv.units",
    "col:silver_sales_enriched.quantity -> col:gold_daily_sales.units",
    "col:silver_sales_enriched.year -> col:gold_category_summary.revenue_rank",
    "col:silver_sales_enriched.year -> col:gold_category_summary.revenue_share_pct",
    "col:silver_sales_enriched.year -> col:gold_category_summary.year",
]


def test_sample_notebook_set() -> None:
    assert list(sample_notebooks.notebooks()) == ["00_README", "01_Retail_Medallion_ETL", "02_Retail_Explore_Python"]


def test_readme_notebook_has_no_lineage_and_no_gaps() -> None:
    f = nb("00_README")
    assert not f.edges and not f.gaps and [a.ref.type for a in f.assets] == ["notebook"]


def test_etl_notebook_table_level_lineage() -> None:
    f = nb("01_Retail_Medallion_ETL")
    n = "notebook:01_Retail_Medallion_ETL"
    assert edges(f, "READS_FROM", "WRITES_TO") == {
        f"READS_FROM {n} -> file:Files/raw/sales [derived]",
        f"READS_FROM {n} -> table:dim_customer [derived]",
        f"READS_FROM {n} -> table:dim_product [derived]",
        f"READS_FROM {n} -> table:dim_store [derived]",
        f"READS_FROM {n} -> table:gold_category_summary [derived]",
        f"READS_FROM {n} -> table:gold_customer_ltv [derived]",
        f"READS_FROM {n} -> table:gold_daily_sales [derived]",
        f"READS_FROM {n} -> table:silver_sales_enriched [derived]",
        f"WRITES_TO {n} -> table:gold_category_summary [derived]",
        f"WRITES_TO {n} -> table:gold_customer_ltv [derived]",
        f"WRITES_TO {n} -> table:gold_daily_sales [derived]",
        f"WRITES_TO {n} -> table:silver_sales_enriched [derived]",
    }


def test_etl_notebook_column_lineage_is_exact() -> None:
    f = nb("01_Retail_Medallion_ETL")
    got = sorted(f"{lab(e.src)} -> {lab(e.dst)}" for e in f.edges if e.type == "TRANSFORMS")
    assert got == ETL_TRANSFORMS
    assert {e.effective_confidence() for e in f.edges} == {"derived"}


def test_etl_notebook_key_column_paths() -> None:
    """Spot checks of the semantics behind the exact list: joins, window functions, CASE over a ranking, aggregates over a formatted SQL template."""
    f = nb("01_Retail_Medallion_ETL")
    pairs = {(lab(e.src), lab(e.dst)) for e in f.edges if e.type == "TRANSFORMS"}
    # a USING join keeps the left (bronze) key; the dimension's own copy is not a source
    assert ("col:files/raw/sales.customer_id", "col:silver_sales_enriched.customer_id") in pairs
    assert ("col:dim_customer.customer_id", "col:silver_sales_enriched.customer_id") not in pairs
    # alias in a select
    assert ("col:dim_customer.segment", "col:silver_sales_enriched.customer_segment") in pairs
    # LTV decile is a window over lifetime revenue (net_amount) with customer_id as tie breaker
    assert ("col:silver_sales_enriched.net_amount", "col:gold_customer_ltv.ltv_segment") in pairs
    assert ("col:silver_sales_enriched.customer_id", "col:gold_customer_ltv.ltv_segment") in pairs
    # rank() OVER (PARTITION BY year ORDER BY net_revenue)
    assert ("col:silver_sales_enriched.year", "col:gold_category_summary.revenue_rank") in pairs
    assert ("col:silver_sales_enriched.net_amount", "col:gold_category_summary.revenue_rank") in pairs
    # a column that only filters (min_amount / run_date parameters) is not a lineage source of anything
    assert not any(dst.endswith(".sale_id") for _, dst in pairs)


def test_etl_notebook_reports_the_partly_known_silver_schema() -> None:
    f = nb("01_Retail_Medallion_ETL")
    assert gaps(f) == [("unsupported_construct", "silver_sales_enriched")]
    assert f.gaps[0].location["cell"] == 7
    assert "only partly known" in f.gaps[0].detail


def test_explore_notebook_reads_are_inferred_with_a_dynamic_reference_gap_each() -> None:
    f = nb("02_Retail_Explore_Python")
    n = "notebook:02_Retail_Explore_Python"
    tables = ["dim_customer", "dim_date", "dim_product", "dim_store", "fact_sales", "fact_web_events", "gold_category_summary", "gold_customer_ltv", "gold_daily_sales"]
    assert edges(f) == {f"READS_FROM {n} -> table:{t} [inferred]" for t in tables}
    assert sorted(s for k, s in gaps(f) if k == "dynamic_reference") == sorted(f"<?>/{t}" for t in tables)
    assert gap_kinds(f) == {"dynamic_reference"}
    assert {g.location["cell"] for g in f.gaps} == {4}


def test_notebooks_through_the_engine_with_the_manifest_settings() -> None:
    by_name = {m["name"]: m for m in sample_notebooks.manifest()}
    for name, item in by_name.items():
        d = DefinitionFact(kind="notebook", ref=AssetRef.make("notebook", {"olcf_item": f"itm_{name[:2]}"}, name), name=name, payload=item["definition"]["nbformat"], etag="v1",
                           context={"item_id": f"itm_{name[:2]}", "item_name": name, "default_lakehouse": item["definition"]["settings"]["default_lakehouse"]})
        f = analyze_definition(d)
        assert f.assets and all(g.kind != "parse_error" for g in f.gaps)
    etl = by_name["01_Retail_Medallion_ETL"]
    f = analyze_definition(DefinitionFact(kind="notebook", ref=AssetRef.make("notebook", {"olcf_item": "itm_01"}, "etl"), name="etl", payload=etl["definition"]["nbformat"],
                                          context={"item_id": "itm_01", "item_name": "etl", "default_lakehouse": etl["definition"]["settings"]["default_lakehouse"]}))
    assert sum(1 for e in f.edges if e.type == "TRANSFORMS") == len(ETL_TRANSFORMS)


QUERY_TABLES = {
    "Revenue by month": {"dim_date", "fact_sales"},
    "Top products in a category": {"dim_product", "fact_sales"},
    "Customer value segments": {"dim_customer", "gold_customer_ltv"},
    "Daily sales, last N days": {"gold_daily_sales"},
    "Web funnel by device": {"fact_web_events"},
    "Store performance ranking": {"dim_store", "fact_sales"},
    "Channel mix by quarter": {"dim_date", "fact_sales"},
    "Discount impact on margin": {"fact_sales"},
    "Raw files reconcile with the table": {"fact_sales", "Files/raw/sales/*.csv"},
}


@pytest.mark.parametrize("query", sample_reports.saved_queries("itm_lh"), ids=lambda q: q["name"])
def test_saved_query_reads_exactly_its_tables(query: dict[str, Any]) -> None:
    d = query["definition"]
    f = analyze_definition(DefinitionFact(kind="sql_query", ref=AssetRef.make("sql_query", {"olcf_item": "itm_q"}, query["name"]), name=query["name"], payload=d, etag="v1",
                                          context={"item_id": "itm_q", "item_name": query["name"], "default_lakehouse": dict(LH)}))
    assert not f.gaps
    assert {e.type for e in f.edges} == {"READS_FROM"}
    read = {(e.dst.name or "").removeprefix("retail_lakehouse.") for e in f.edges}
    assert read == QUERY_TABLES[query["name"]]
    assert {e.effective_confidence() for e in f.edges} == {"derived"}


def test_saved_queries_cover_the_sample_set() -> None:
    assert {q["name"] for q in sample_reports.saved_queries("itm_lh")} == set(QUERY_TABLES)


def test_saved_query_file_read_is_a_file_dataset() -> None:
    q = next(q for q in sample_reports.saved_queries("itm_lh") if q["name"].startswith("Raw files"))
    f = analyze_definition(DefinitionFact(kind="sql_query", ref=AssetRef.make("sql_query", {"olcf_item": "itm_q"}, "q"), name="q", payload=q["definition"],
                                          context={"item_id": "itm_q", "default_lakehouse": dict(LH)}))
    assert {e.dst.type for e in f.edges} == {"table", "file"}
