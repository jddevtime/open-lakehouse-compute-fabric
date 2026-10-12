# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Saved SQL queries and the *Retail Sales Overview* report of the sample lakehouse."""

# ruff: noqa: S608  (the f-strings only interpolate the constant sample lakehouse name; user values are bound parameters)

from __future__ import annotations

from typing import Any

from olcf.sample import spec

LH = spec.LAKEHOUSE
PLACEHOLDER = "<lakehouse-id>"
REPORT_NAME = "Retail Sales Overview"


def saved_queries(lakehouse_id: str = PLACEHOLDER) -> list[dict[str, Any]]:
    """``[{name, description, definition}]`` ready to be created as ``sql_query`` items (all DuckDB, all runnable with their defaults)."""
    q = [
        ("Revenue by month", "Monthly revenue, profit and orders for one year.", f"""SELECT d.month, d.month_name, round(sum(f.net_amount), 2) AS revenue, round(sum(f.profit), 2) AS profit,
       count(DISTINCT f.order_id) AS orders
FROM {LH}.fact_sales f
JOIN {LH}.dim_date d ON d.date_key = f.date_key
WHERE d.year = :year
GROUP BY d.month, d.month_name
ORDER BY d.month""", [{"name": "year", "type": "int", "default": 2025}]),
        ("Top products in a category", "Best selling products of a category by revenue, with margin.", f"""SELECT p.product_name, p.brand, sum(f.quantity) AS units, round(sum(f.net_amount), 2) AS revenue,
       round(100.0 * sum(f.profit) / sum(f.net_amount), 1) AS margin_pct
FROM {LH}.fact_sales f
JOIN {LH}.dim_product p ON p.product_id = f.product_id
WHERE p.category = :category
GROUP BY p.product_name, p.brand
ORDER BY revenue DESC
LIMIT :top_n""", [{"name": "category", "type": "string", "default": "Electronics"}, {"name": "top_n", "type": "int", "default": 10}]),
        ("Customer value segments", "Customers by lifetime-value segment and customer segment, from the gold_customer_ltv table.", f"""SELECT l.ltv_segment, c.segment AS customer_segment, count(*) AS customers,
       round(avg(l.lifetime_revenue), 2) AS avg_lifetime_revenue, round(avg(l.avg_order_value), 2) AS avg_order_value, round(avg(l.recency_days), 0) AS avg_recency_days
FROM {LH}.gold_customer_ltv l
JOIN {LH}.dim_customer c ON c.customer_id = l.customer_id
GROUP BY l.ltv_segment, c.segment
ORDER BY l.ltv_segment, c.segment""", []),
        ("Daily sales, last N days", "Net revenue and orders per day for the most recent days in the data.", f"""SELECT sale_date, orders, net_revenue, profit
FROM {LH}.gold_daily_sales
WHERE sale_date > (SELECT max(sale_date) FROM {LH}.gold_daily_sales) - :days
ORDER BY sale_date""", [{"name": "days", "type": "int", "default": 30}]),
        ("Web funnel by device", "Sessions that reached each funnel step, by device, with the conversion rate.", f"""SELECT device, sessions, carts, checkouts, purchases, round(100.0 * purchases / sessions, 2) AS conversion_pct
FROM (
  SELECT device, count(DISTINCT session_id) AS sessions,
         count(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN session_id END) AS carts,
         count(DISTINCT CASE WHEN event_type = 'checkout_start' THEN session_id END) AS checkouts,
         count(DISTINCT CASE WHEN event_type = 'purchase' THEN session_id END) AS purchases
  FROM {LH}.fact_web_events
  GROUP BY device)
ORDER BY sessions DESC""", []),
        ("Store performance ranking", "Revenue and margin per store, ranked; optionally limited to one region.", f"""SELECT rank() OVER (ORDER BY sum(f.net_amount) DESC) AS rank, s.store_name, s.region, s.format,
       round(sum(f.net_amount), 2) AS revenue, round(100.0 * sum(f.profit) / sum(f.net_amount), 1) AS margin_pct
FROM {LH}.fact_sales f
JOIN {LH}.dim_store s ON s.store_id = f.store_id
WHERE s.region = coalesce(:region, s.region)
GROUP BY s.store_name, s.region, s.format
ORDER BY rank""", [{"name": "region", "type": "string", "default": None}]),
        ("Channel mix by quarter", "Revenue per sales channel and quarter with each channel's share of the quarter.", f"""SELECT d.year, d.quarter, f.channel, round(sum(f.net_amount), 2) AS revenue,
       round(100.0 * sum(f.net_amount) / sum(sum(f.net_amount)) OVER (PARTITION BY d.year, d.quarter), 1) AS share_pct
FROM {LH}.fact_sales f
JOIN {LH}.dim_date d ON d.date_key = f.date_key
GROUP BY d.year, d.quarter, f.channel
ORDER BY d.year, d.quarter, f.channel""", []),
        ("Discount impact on margin", "How deeper discounts change units, revenue and margin.", f"""SELECT discount_pct, count(*) AS lines, sum(quantity) AS units, round(sum(net_amount), 2) AS revenue,
       round(100.0 * sum(profit) / sum(net_amount), 1) AS margin_pct
FROM {LH}.fact_sales
GROUP BY discount_pct
ORDER BY discount_pct""", []),
        ("Raw files reconcile with the table", "Compares the raw CSV export in Files/ with the fact_sales Delta table.", f"""SELECT (SELECT count(*) FROM read_csv('Files/raw/sales/*.csv')) AS csv_lines,
       (SELECT count(*) FROM {LH}.fact_sales) AS table_lines,
       (SELECT round(sum(net_amount), 2) FROM read_csv('Files/raw/sales/*.csv')) AS csv_revenue,
       (SELECT round(sum(net_amount), 2) FROM {LH}.fact_sales) AS table_revenue""", []),
    ]
    return [{"name": name, "description": desc, "definition": {"sql": sql, "engine": "duckdb", "lakehouse_id": lakehouse_id, "parameters": params}} for name, desc, sql, params in q]


def report_definition(lakehouse_id: str = PLACEHOLDER) -> dict[str, Any]:
    """The *Retail Sales Overview* dashboard (CONTRACTS section 10 ``report`` item)."""
    where = "f.order_date BETWEEN :period_from AND :period_to"
    cat = "p.category = coalesce(:category, p.category)"
    join_p = f"JOIN {LH}.dim_product p ON p.product_id = f.product_id"

    def tile(id_: str, title: str, type_: str, sql: str, w: int, h: int, **view: Any) -> dict[str, Any]:
        return {"id": id_, "title": title, "type": type_, "sql": sql, "lakehouse_id": lakehouse_id, "engine": "duckdb", "width": w, "height": h, **view}

    return {
        "title": REPORT_NAME,
        "refresh_seconds": None,
        "filters": [
            {"name": "period", "label": "Period", "type": "date_range", "default": {"from": "2025-01-01", "to": "2025-12-31"}},
            {"name": "category", "label": "Category", "type": "select", "default": None, "options_sql": f"SELECT DISTINCT category FROM {LH}.dim_product ORDER BY 1",
             "options_lakehouse_id": lakehouse_id},
        ],
        "tiles": [
            tile("revenue", "Revenue", "kpi", f"SELECT round(sum(f.net_amount), 2) AS revenue FROM {LH}.fact_sales f {join_p} WHERE {where} AND {cat}", 3, 1, format="currency"),
            tile("orders", "Orders", "kpi", f"SELECT count(DISTINCT f.order_id) AS orders FROM {LH}.fact_sales f {join_p} WHERE {where} AND {cat}", 3, 1, format="integer"),
            tile("customers", "Customers", "kpi", f"SELECT count(DISTINCT f.customer_id) AS customers FROM {LH}.fact_sales f {join_p} WHERE {where} AND {cat}", 3, 1, format="integer"),
            tile("aov", "Average order value", "kpi", f"SELECT round(sum(f.net_amount) / count(DISTINCT f.order_id), 2) AS avg_order_value FROM {LH}.fact_sales f {join_p} WHERE {where} AND {cat}",
                 3, 1, format="currency"),
            tile("revenue_by_month", "Revenue by month", "line", f"SELECT strftime(date_trunc('month', f.order_date), '%Y-%m') AS month, round(sum(f.net_amount), 2) AS revenue, "
                 f"round(sum(f.profit), 2) AS profit FROM {LH}.fact_sales f {join_p} WHERE {where} AND {cat} GROUP BY 1 ORDER BY 1", 6, 3, x="month", y="revenue", series="profit"),
            tile("revenue_by_category", "Revenue by category", "bar", f"SELECT p.category, round(sum(f.net_amount), 2) AS revenue, round(sum(f.profit), 2) AS profit "
                 f"FROM {LH}.fact_sales f {join_p} WHERE {where} GROUP BY p.category ORDER BY revenue DESC", 6, 3, x="category", y="revenue"),
            tile("channel_mix", "Channel mix", "donut", f"SELECT f.channel, round(sum(f.net_amount), 2) AS revenue FROM {LH}.fact_sales f {join_p} WHERE {where} AND {cat} "
                 f"GROUP BY f.channel ORDER BY revenue DESC", 4, 3, x="channel", y="revenue"),
            tile("top_products", "Top 10 products", "table", f"SELECT p.product_name, p.category, sum(f.quantity) AS units, round(sum(f.net_amount), 2) AS revenue, "
                 f"round(100.0 * sum(f.profit) / sum(f.net_amount), 1) AS margin_pct FROM {LH}.fact_sales f {join_p} WHERE {where} AND {cat} "
                 f"GROUP BY p.product_name, p.category ORDER BY revenue DESC LIMIT 10", 8, 3),
            tile("revenue_by_store", "Top 10 stores", "bar", f"SELECT s.store_name, round(sum(f.net_amount), 2) AS revenue FROM {LH}.fact_sales f {join_p} "
                 f"JOIN {LH}.dim_store s ON s.store_id = f.store_id WHERE {where} AND {cat} GROUP BY s.store_name ORDER BY revenue DESC LIMIT 10", 6, 3, x="store_name", y="revenue"),
            tile("region_by_weekday", "Revenue by region and weekday", "heatmap", f"SELECT s.region, d.day_name AS weekday, round(sum(f.net_amount), 2) AS revenue "
                 f"FROM {LH}.fact_sales f {join_p} JOIN {LH}.dim_store s ON s.store_id = f.store_id JOIN {LH}.dim_date d ON d.date_key = f.date_key "
                 f"WHERE {where} AND {cat} GROUP BY s.region, d.day_name, d.day_of_week ORDER BY s.region, d.day_of_week", 6, 3, x="weekday", y="region", series="revenue"),
        ],
    }
