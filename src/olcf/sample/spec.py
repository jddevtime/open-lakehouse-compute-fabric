# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The sample dataset's single source of truth: tables, columns, descriptions, partitioning, sizes.

``schemas/sample-retail.schema.json`` is generated from this module (``python -m olcf.sample.generate --write-schemas``) and a test fails
when the committed file drifts.  The installer uses the same descriptions to annotate the data catalog.
"""

from __future__ import annotations

from typing import Any

VERSION = 4                      # bump when the generated data or its schema changes (re-install rewrites the tables)
SEED = 20260101
WORKSPACE = "Sample Retail"
LAKEHOUSE = "retail_lakehouse"
START_DATE = "2023-01-01"
END_DATE = "2025-12-31"
AS_OF = END_DATE                 # "today" of the dataset

#: size -> row targets
SIZES: dict[str, dict[str, int]] = {
    "standard": {"customers": 2000, "products": 200, "stores": 25, "sales_lines": 80_000, "web_events": 50_000},
    "small": {"customers": 300, "products": 60, "stores": 8, "sales_lines": 6_000, "web_events": 4_000},
}

ARROW_TYPES = {"long": "int64", "int": "int32", "double": "float64", "string": "string", "date": "date32", "timestamp": "timestamp[us, tz=UTC]", "boolean": "bool"}


def col(name: str, type_: str, desc: str, nullable: bool = False) -> dict[str, Any]:
    return {"name": name, "type": type_, "nullable": nullable, "description": desc}


TABLES: dict[str, dict[str, Any]] = {
    "dim_customer": {
        "layer": "silver", "kind": "dimension", "tags": ["retail", "dimension", "pii-synthetic"], "primary_key": ["customer_id"], "partition_by": [],
        "description": "Customers of the retail company (synthetic people; no real personal data).",
        "columns": [
            col("customer_id", "long", "Surrogate key"), col("first_name", "string", "Given name"), col("last_name", "string", "Family name"),
            col("email", "string", "Contact e-mail (example.com, synthetic)"), col("city", "string", "Home city"), col("state", "string", "US state code"),
            col("country", "string", "ISO country code"), col("segment", "string", "Consumer, Corporate or Home Office"), col("signup_date", "date", "Date the account was created"),
            col("loyalty_tier", "string", "Bronze, Silver, Gold or Platinum"), col("is_active", "boolean", "Placed an order in the last 6 months of the dataset"),
        ],
    },
    "dim_product": {
        "layer": "silver", "kind": "dimension", "tags": ["retail", "dimension"], "primary_key": ["product_id"], "partition_by": [],
        "description": "Products with category hierarchy, cost and list price.",
        "columns": [
            col("product_id", "long", "Surrogate key"), col("sku", "string", "Stock keeping unit"), col("product_name", "string", "Display name"),
            col("category", "string", "Top level category"), col("subcategory", "string", "Second level category"), col("brand", "string", "Brand"),
            col("unit_cost", "double", "What a unit costs the company"), col("list_price", "double", "Standard selling price"), col("launch_date", "date", "First day on sale"),
        ],
    },
    "dim_store": {
        "layer": "silver", "kind": "dimension", "tags": ["retail", "dimension"], "primary_key": ["store_id"], "partition_by": [],
        "description": "Physical stores plus the online store (format = Online).",
        "columns": [
            col("store_id", "long", "Surrogate key"), col("store_name", "string", "Store name"), col("city", "string", "City"), col("state", "string", "US state code"),
            col("region", "string", "East, West, Central, South or Online"), col("format", "string", "Flagship, Mall, Outlet or Online"),
            col("opened_date", "date", "Opening date"), col("sq_ft", "int", "Selling space in square feet (0 for Online)"),
        ],
    },
    "dim_date": {
        "layer": "silver", "kind": "dimension", "tags": ["retail", "dimension", "calendar"], "primary_key": ["date_key"], "partition_by": [],
        "description": "Calendar dimension covering the whole dataset period (three years).",
        "columns": [
            col("date_key", "int", "yyyymmdd"), col("date", "date", "Calendar date"), col("year", "int", "Year"), col("quarter", "int", "Quarter 1-4"), col("month", "int", "Month 1-12"),
            col("month_name", "string", "January ... December"), col("day", "int", "Day of month"), col("day_of_week", "int", "1 = Monday ... 7 = Sunday"),
            col("day_name", "string", "Monday ... Sunday"), col("week_of_year", "int", "ISO week number"), col("is_weekend", "boolean", "Saturday or Sunday"),
            col("is_holiday", "boolean", "US federal holiday (fixed-date approximation)"),
        ],
    },
    "fact_sales": {
        "layer": "silver", "kind": "fact", "tags": ["retail", "fact", "sales"], "primary_key": ["sale_id"], "partition_by": ["year", "month"],
        "description": "One row per order line. Partitioned by year and month of the order date.",
        "columns": [
            col("sale_id", "long", "Order line id"), col("order_id", "long", "Order id (an order has 1-5 lines)"), col("order_date", "date", "Date of the order"),
            col("order_ts", "timestamp", "Time of the order (UTC)"), col("date_key", "int", "Foreign key to dim_date"), col("year", "int", "Partition column: order year"),
            col("month", "int", "Partition column: order month"), col("customer_id", "long", "Foreign key to dim_customer"), col("product_id", "long", "Foreign key to dim_product"),
            col("store_id", "long", "Foreign key to dim_store (the online store for web and mobile orders)"), col("channel", "string", "store, web or mobile"),
            col("quantity", "int", "Units sold"), col("unit_price", "double", "Price per unit before discount"), col("discount_pct", "double", "Discount percentage applied (0-25)"),
            col("gross_amount", "double", "quantity * unit_price"), col("discount_amount", "double", "gross_amount * discount_pct / 100"),
            col("net_amount", "double", "Revenue: gross_amount - discount_amount"), col("cost_amount", "double", "quantity * unit cost"), col("profit", "double", "net_amount - cost_amount"),
            col("payment_method", "string", "card, cash, paypal or gift_card"),
        ],
    },
    "fact_web_events": {
        "layer": "silver", "kind": "fact", "tags": ["retail", "fact", "web", "clickstream"], "primary_key": ["event_id"], "partition_by": [],
        "description": "Web and mobile clickstream events (sessions, page views, funnel steps).",
        "columns": [
            col("event_id", "long", "Event id"), col("event_ts", "timestamp", "Event time (UTC)"), col("event_date", "date", "Event date"), col("session_id", "string", "Visit identifier"),
            col("customer_id", "long", "Known customer, null for anonymous visitors", True), col("event_type", "string", "page_view, search, add_to_cart, checkout_start or purchase"),
            col("page", "string", "home, category, product, search, cart or checkout"), col("product_id", "long", "Product the event refers to, if any", True),
            col("device", "string", "desktop, mobile or tablet"), col("browser", "string", "Browser family"), col("referrer", "string", "direct, search, social, email or ads"),
            col("country", "string", "ISO country code"), col("duration_ms", "long", "Time spent on the event"),
        ],
    },
    "gold_daily_sales": {
        "layer": "gold", "kind": "aggregate", "tags": ["retail", "gold", "sales"], "primary_key": ["sale_date"], "partition_by": [],
        "description": "Daily sales KPIs, built from fact_sales by notebook 01_Retail_Medallion_ETL.",
        "columns": [
            col("sale_date", "date", "Day"), col("orders", "long", "Distinct orders"), col("units", "long", "Units sold"), col("customers", "long", "Distinct customers"),
            col("gross_revenue", "double", "Sum of gross_amount"), col("net_revenue", "double", "Sum of net_amount"), col("profit", "double", "Sum of profit"),
            col("avg_order_value", "double", "net_revenue / orders"),
        ],
    },
    "gold_category_summary": {
        "layer": "gold", "kind": "aggregate", "tags": ["retail", "gold", "category"], "primary_key": ["category", "year"], "partition_by": [],
        "description": "Revenue, profit and share per product category and year.",
        "columns": [
            col("category", "string", "Product category"), col("year", "int", "Year"), col("orders", "long", "Distinct orders"), col("units", "long", "Units sold"),
            col("net_revenue", "double", "Revenue"), col("profit", "double", "Profit"), col("margin_pct", "double", "profit / net_revenue * 100"),
            col("revenue_share_pct", "double", "Share of the year's total revenue"), col("revenue_rank", "int", "Rank within the year by revenue (1 = best)"),
        ],
    },
    "gold_customer_ltv": {
        "layer": "gold", "kind": "aggregate", "tags": ["retail", "gold", "customer", "ltv"], "primary_key": ["customer_id"], "partition_by": [],
        "description": "Lifetime value and recency per customer (as of the end of the dataset).",
        "columns": [
            col("customer_id", "long", "Customer"), col("first_order_date", "date", "First purchase"), col("last_order_date", "date", "Latest purchase"),
            col("orders", "long", "Orders placed"), col("units", "long", "Units bought"), col("lifetime_revenue", "double", "Total net revenue"),
            col("lifetime_profit", "double", "Total profit"), col("avg_order_value", "double", "lifetime_revenue / orders"),
            col("recency_days", "int", "Days between last_order_date and the dataset end"), col("ltv_segment", "string", "Top 10%, Upper 40% or Lower 50% by lifetime revenue"),
        ],
    },
}

RAW_FILES = {
    "Files/raw/customers/customers.csv": "dim_customer",
    "Files/raw/products/products.csv": "dim_product",
    "Files/raw/stores/stores.csv": "dim_store",
    # fact_sales is exported one CSV per year: Files/raw/sales/sales_<year>.csv
}


def table_order() -> list[str]:
    return list(TABLES)


def schema_document() -> dict[str, Any]:
    """The JSON document stored as ``schemas/sample-retail.schema.json``."""
    return {
        "$comment": "Generated by `python -m olcf.sample.generate --write-schemas` from olcf.sample.spec. Do not edit by hand.",
        "name": "sample-retail", "version": VERSION, "seed": SEED, "workspace": WORKSPACE, "lakehouse": LAKEHOUSE, "period": {"start": START_DATE, "end": END_DATE},
        "sizes": SIZES,
        "tables": [{"name": n, "layer": t["layer"], "kind": t["kind"], "description": t["description"], "tags": t["tags"], "primary_key": t["primary_key"],
                    "partition_by": t["partition_by"], "columns": [{"name": c["name"], "type": c["type"], "nullable": c["nullable"], "description": c["description"]} for c in t["columns"]]}
                   for n, t in TABLES.items()],
        "raw_files": [*RAW_FILES, "Files/raw/sales/sales_<year>.csv"],
    }
