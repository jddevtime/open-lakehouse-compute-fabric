# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The notebooks of the sample lakehouse, built with ``nbformat`` (deterministic: fixed cell ids, no outputs).

* ``00_README`` - a notebook-style markdown tour of the lakehouse (no code cells run anything).
* ``01_Retail_Medallion_ETL`` - PySpark + Delta: bronze CSV -> silver (``saveAsTable``) -> gold (Spark SQL + Delta ``MERGE``), ``DESCRIBE HISTORY``.
* ``02_Retail_Explore_Python`` - deltalake + DuckDB + pandas (+ matplotlib charts when it is installed); no Spark needed.

:func:`manifest` returns the item records the installer creates (``name``, ``description``, ``tags``, ``definition``).
"""

# ruff: noqa: S608  (notebook text only; nothing here executes SQL)

from __future__ import annotations

import textwrap
from typing import Any

import nbformat
from nbformat import v4

from olcf.core.hostruntime import HOST_FAMILY
from olcf.sample import spec
from olcf.sample.generate import GOLD_SQL, gold_sql

LH = spec.LAKEHOUSE
SILVER = "silver_sales_enriched"


def _md(cid: str, text: str) -> Any:
    c = v4.new_markdown_cell(textwrap.dedent(text).strip("\n"))
    c["id"] = cid
    return c


def _code(cid: str, text: str, tags: list[str] | None = None) -> Any:
    c = v4.new_code_cell(textwrap.dedent(text).strip("\n"))
    c["id"] = cid
    if tags:
        c["metadata"]["tags"] = tags
    return c


def _notebook(cells: list[Any], kernel: str) -> dict[str, Any]:
    nb = v4.new_notebook(cells=cells)
    nb["metadata"] = {
        "kernelspec": {"name": "python3", "display_name": "Python 3 (ipykernel)", "language": "python"},
        "language_info": {"name": "python"},
        "olcf": {"sample": spec.WORKSPACE, "kernel": kernel},
    }
    nb["nbformat"], nb["nbformat_minor"] = 4, 5
    nbformat.validate(nb)
    return dict(nb)


# ----------------------------------------------------------------------------- 00 README
def readme_notebook() -> dict[str, Any]:
    rows = "\n".join(f"| `{n}` | {t['layer']} | {t['description']} |" for n, t in spec.TABLES.items())
    cells = [
        _md("readme-title", f"""
            # {spec.WORKSPACE}

            A synthetic retail company with three years of trading ({spec.START_DATE} to {spec.END_DATE}); nothing here is real data. The workspace was created by
            *Install sample* and can be re-installed at any time (it is idempotent; use *reinstall* to restore the original data).
            """),
        _md("readme-tables", f"""
            ## The `{LH}` lakehouse

            | table | layer | description |
            |---|---|---|
            {rows}

            Raw CSV exports live below `Files/raw/` (customers, products, stores and one `sales_<year>.csv` per year).
            """),
        _md("readme-sql", f"""
            ## Query it

            * **SQL editor** - pick `{LH}` and run, for example, `SELECT * FROM {LH}.gold_daily_sales ORDER BY sale_date DESC LIMIT 10`.
              The saved queries in this workspace take typed parameters (`:year`, `:category`, `:days` ...).
            * **Report** - open *Retail Sales Overview* and change the period or the category filter.
            * **MCP** - connect Claude Desktop (see the MCP guide) and ask "which category grew fastest in 2025?"; the agent uses `describe_table` and `run_sql`.
            """),
        _md("readme-notebooks", """
            ## Notebooks

            1. `01_Retail_Medallion_ETL` - PySpark + Delta Lake: bronze CSV to a silver table, then gold aggregates with Spark SQL and a Delta `MERGE`. Needs a Spark runtime.
               Parameters: `run_date` (the as-of date) and `min_amount` (drop smaller order lines).
            2. `02_Retail_Explore_Python` - the same data with `deltalake`, DuckDB, pandas and matplotlib. No Spark required.
            """),
        _md("readme-model", """
            ## Data model

            `fact_sales` (one row per order line, partitioned by year and month) joins to `dim_customer`, `dim_product`, `dim_store` and `dim_date`.
            `fact_web_events` is the clickstream (page views, searches, cart and checkout steps, purchases). The `gold_*` tables are aggregates of `fact_sales`
            and are what the report reads. Lineage between them is recorded in the catalog.
            """),
    ]
    return _notebook(cells, "none")


# ----------------------------------------------------------------------------- 01 ETL (PySpark)
def etl_notebook() -> dict[str, Any]:
    templates = {n: gold_sql(n, "spark").strip("\n") for n in GOLD_SQL}
    sql_defs = "\n".join(f'    "{n}": """\n{q}\n""",' for n, q in templates.items())
    gold_cell = (f"GOLD_SQL = {{\n{sql_defs}\n}}\n\n\ndef gold(name):\n"
                 f'    return spark.sql(GOLD_SQL[name].format(fact="{SILVER}", product="dim_product", as_of=run_date))')
    cells = [
        _md("etl-title", f"""
            # Retail medallion ETL (PySpark + Delta)

            **bronze** raw CSV (`Files/raw`) -> **silver** `{SILVER}` (typed, filtered, enriched, partitioned by year and month) -> **gold** daily, category and customer
            aggregates. The default lakehouse of this notebook is `{LH}`; table names are relative to it. Run it with the Spark runtime of the compute fabric.
            """),
        _md("etl-params-md", """
            ## Parameters

            The next cell is tagged `parameters`: a job run or a pipeline activity can override these values.
            """),
        _code("etl-params", """
            run_date = "2025-12-31"   # process order lines up to and including this date (also the as-of date of recency_days)
            min_amount = 0.0          # drop order lines whose net_amount is below this value
            """, tags=["parameters"]),
        _code("etl-setup", """
            from pyspark.sql import functions as F

            print(f"Spark {spark.version}; run_date={run_date}; min_amount={min_amount}")
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            """),
        _md("etl-bronze-md", "## Bronze: read the raw files\n\nCSV exports as delivered by the source systems; types are inferred, nothing is cleaned yet."),
        _code("etl-bronze", """
            def read_csv(path):
                return spark.read.option("header", True).option("inferSchema", True).csv(path)

            bronze_sales = read_csv("Files/raw/sales/")
            print("bronze sales lines:", bronze_sales.count())
            bronze_sales.printSchema()
            """),
        _md("etl-silver-md", f"""
            ## Silver: `{SILVER}`

            * cast the date and timestamp columns, derive the partition columns `year` and `month`
            * apply the `run_date` and `min_amount` parameters, drop duplicate lines
            * enrich with customer, product and store attributes from the conformed dimension tables
            """),
        _code("etl-silver", f"""
            customers = spark.table("dim_customer").select("customer_id", F.col("segment").alias("customer_segment"), "loyalty_tier")
            products = spark.table("dim_product").select("product_id", "product_name", "category", "subcategory", "brand")
            stores = spark.table("dim_store").select("store_id", "store_name", "region")

            silver = (
                bronze_sales
                .withColumn("order_date", F.to_date("order_date"))
                .withColumn("order_ts", F.to_timestamp("order_ts"))
                .withColumn("year", F.year("order_date").cast("int"))
                .withColumn("month", F.month("order_date").cast("int"))
                .filter((F.col("order_date") <= F.lit(run_date).cast("date")) & (F.col("net_amount") >= F.lit(float(min_amount))))
                .dropDuplicates(["sale_id"])
                .join(customers, "customer_id", "left")
                .join(products, "product_id", "left")
                .join(stores, "store_id", "left")
            )

            (silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
                   .partitionBy("year", "month").saveAsTable("{SILVER}"))
            print("silver rows:", spark.table("{SILVER}").count())
            """),
        _md("etl-gold-md", """
            ## Gold: aggregates with Spark SQL

            The queries are plain SQL templates; `{fact}` and `{product}` are the table names and `{as_of}` the run date. `gold_category_summary` and `gold_customer_ltv`
            are rebuilt; `gold_daily_sales` is upserted with a Delta `MERGE` so that re-running a window only touches the days it contains.
            """),
        _code("etl-gold-sql", gold_cell),
        _code("etl-gold-rebuild", """
            for name in ("gold_category_summary", "gold_customer_ltv"):
                (gold(name).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(name))
                print(name, spark.table(name).count(), "rows")
            """),
        _code("etl-gold-merge", """
            daily = gold("gold_daily_sales")
            if spark.catalog.tableExists("gold_daily_sales"):
                daily.createOrReplaceTempView("daily_updates")
                spark.sql(\"\"\"
                    MERGE INTO gold_daily_sales AS t
                    USING daily_updates AS s
                    ON t.sale_date = s.sale_date
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *
                \"\"\")
            else:
                daily.write.format("delta").saveAsTable("gold_daily_sales")
            print("gold_daily_sales:", spark.table("gold_daily_sales").count(), "rows")
            """),
        _md("etl-history-md", "## Table history\n\nEvery write above is a commit in the Delta log: version, operation and metrics."),
        _code("etl-history", """
            for table in ("gold_daily_sales", "gold_category_summary", "gold_customer_ltv"):
                print(table)
                spark.sql(f"DESCRIBE HISTORY {table}").select("version", "timestamp", "operation", "operationMetrics").show(5, truncate=80)
            """),
        _md("etl-checks-md", "## Sanity checks\n\nThe gold layer must reconcile with the silver layer; the notebook fails loudly if it does not."),
        _code("etl-checks", f"""
            silver_total = spark.table("{SILVER}").agg(F.round(F.sum("net_amount"), 2)).first()[0]
            gold_total = spark.table("gold_daily_sales").filter(F.col("sale_date") <= F.lit(run_date).cast("date")).agg(F.round(F.sum("net_revenue"), 2)).first()[0]
            category_total = spark.table("gold_category_summary").agg(F.round(F.sum("net_revenue"), 2)).first()[0]
            print(f"silver {{silver_total}}  gold daily {{gold_total}}  gold category {{category_total}}")
            assert abs(silver_total - gold_total) < 1.0, "gold_daily_sales does not reconcile with silver"
            assert abs(silver_total - category_total) < 1.0, "gold_category_summary does not reconcile with silver"
            assert spark.table("{SILVER}").filter("customer_id IS NULL OR product_id IS NULL").count() == 0, "null keys in silver"
            print("ok")
            """),
    ]
    return _notebook(cells, "pyspark")


# ----------------------------------------------------------------------------- 02 Explore (no Spark)
def explore_notebook() -> dict[str, Any]:
    cells = [
        _md("exp-title", f"""
            # Explore the retail lakehouse with Python (no Spark)

            Reads the Delta tables of `{LH}` with `deltalake`, queries them with DuckDB, shapes the results with pandas and plots with matplotlib.
            Runs on any OLCF Python runtime, or on a laptop: set `RETAIL_TABLES_ROOT` to a folder produced by `python -m olcf.sample.generate --out DIR` (`DIR/Tables`).

            Needs `deltalake`, `duckdb` and `pandas` (part of OLCF itself). Charts need `matplotlib`; without it the notebook still runs and shows the tables, and the cell below says how to add it
            (`pip install "olcf[plots]"` for the OLCF installation, or add `matplotlib` to the notebook's Environment item).
            """),
        _code("exp-setup", """
            import os
            from pathlib import Path

            import duckdb
            import pandas as pd
            from deltalake import DeltaTable

            try:
                import matplotlib

                matplotlib.use(os.environ.get("MPLBACKEND") or "Agg")
                import matplotlib.pyplot as plt
            except ImportError:  # charts are optional: every cell still shows its table
                plt = None
                print("matplotlib is not installed here, so charts are skipped and tables are shown instead.")
                print("To get the charts: pip install 'olcf[plots]' (or add matplotlib to the notebook's Environment).")

            pd.set_option("display.width", 140)
            pd.set_option("display.max_columns", 20)
            """),
        _md("exp-root-md", "## Locate the tables\n\n`RETAIL_TABLES_ROOT` wins; otherwise the OLCF runtime resolves `Tables/` of the default lakehouse; the Fabric mount path is the last fallback."),
        _code("exp-root", """
            def tables_root() -> Path:
                override = os.environ.get("RETAIL_TABLES_ROOT")
                if override:
                    return Path(override)
                try:
                    from olcf.runtime_lib import paths

                    return Path(paths.resolve("Tables/fact_sales", for_="local")).parent
                except Exception:
                    return Path("/lakehouse/default/Tables")


            ROOT = tables_root()
            print("tables:", ROOT)
            """),
        _code("exp-load", """
            def table(name, version=None):
                return DeltaTable(str(ROOT / name), version=version)


            con = duckdb.connect()
            for name in ("fact_sales", "fact_web_events", "dim_customer", "dim_product", "dim_store", "dim_date",
                         "gold_daily_sales", "gold_category_summary", "gold_customer_ltv"):
                con.register(name, table(name).to_pyarrow_dataset())

            con.sql("SELECT 'fact_sales' AS table_name, count(*) AS row_count FROM fact_sales UNION ALL "
                    "SELECT 'fact_web_events', count(*) FROM fact_web_events").df()
            """),
        _md("exp-schema-md", "## What is in `fact_sales`?\n\nDelta keeps the schema, the partitioning and a commit log."),
        _code("exp-schema", """
            fact = table("fact_sales")
            print("version:", fact.version(), " partitioned by:", fact.metadata().partition_columns)
            print(fact.to_pyarrow_dataset().schema)
            fact.history(3)
            """),
        _md("exp-month-md", "## Revenue and profit by month"),
        _code("exp-month", """
            monthly = con.sql(\"\"\"
                SELECT date_trunc('month', sale_date) AS month, sum(net_revenue) AS revenue, sum(profit) AS profit
                FROM gold_daily_sales GROUP BY 1 ORDER BY 1
            \"\"\").df()

            if plt is not None:
                fig, ax = plt.subplots(figsize=(10, 3.5))
                ax.plot(monthly["month"], monthly["revenue"], label="revenue")
                ax.plot(monthly["month"], monthly["profit"], label="profit")
                ax.set_title("Monthly revenue and profit")
                ax.legend()
                fig.autofmt_xdate()
                plt.show()
            monthly.tail()
            """),
        _md("exp-cat-md", "## Category mix by year"),
        _code("exp-cat", """
            cat = con.sql("SELECT category, year, net_revenue FROM gold_category_summary").df().pivot(index="category", columns="year", values="net_revenue")
            cat = cat.sort_values(cat.columns[-1])
            if plt is not None:
                fig, ax = plt.subplots(figsize=(8, 3.5))
                height = 0.8 / len(cat.columns)
                for i, year in enumerate(cat.columns):
                    ax.barh([j + i * height for j in range(len(cat))], cat[year], height=height, label=str(year))
                ax.set_yticks([j + 0.4 - height / 2 for j in range(len(cat))], list(cat.index))
                ax.set_title("Net revenue by category and year")
                ax.legend()
                plt.show()
            cat.round(0)
            """),
        _md("exp-funnel-md", "## Web funnel\n\nSessions that reached each step of the visit, by device."),
        _code("exp-funnel", """
            funnel = con.sql(\"\"\"
                SELECT device,
                       count(DISTINCT session_id) AS sessions,
                       count(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN session_id END) AS carts,
                       count(DISTINCT CASE WHEN event_type = 'checkout_start' THEN session_id END) AS checkouts,
                       count(DISTINCT CASE WHEN event_type = 'purchase' THEN session_id END) AS purchases
                FROM fact_web_events GROUP BY device ORDER BY sessions DESC
            \"\"\").df()
            funnel["conversion_pct"] = (100 * funnel["purchases"] / funnel["sessions"]).round(2)
            funnel
            """),
        _md("exp-ltv-md", "## Customer value\n\nLifetime value by segment, joined to the customer dimension."),
        _code("exp-ltv", """
            ltv = con.sql(\"\"\"
                SELECT l.ltv_segment, c.segment AS customer_segment, count(*) AS customers, round(avg(l.lifetime_revenue), 2) AS avg_lifetime_revenue
                FROM gold_customer_ltv l JOIN dim_customer c USING (customer_id)
                GROUP BY ALL ORDER BY 1, 2
            \"\"\").df()
            pivot = ltv.pivot(index="ltv_segment", columns="customer_segment", values="avg_lifetime_revenue")
            if plt is not None:
                fig, ax = plt.subplots(figsize=(8, 3.5))
                width = 0.8 / len(pivot.columns)
                for i, segment in enumerate(pivot.columns):
                    ax.bar([j + i * width for j in range(len(pivot))], pivot[segment], width=width, label=segment)
                ax.set_xticks([j + 0.4 - width / 2 for j in range(len(pivot))], list(pivot.index))
                ax.set_title("Average lifetime revenue by value and customer segment")
                ax.legend()
                plt.show()
            ltv
            """),
        _md("exp-travel-md", "## Time travel\n\nThe first commit of a table is still readable: `DeltaTable(path, version=0)`."),
        _code("exp-travel", """
            current = table("gold_daily_sales")
            first = table("gold_daily_sales", version=0)
            print(f"current version {current.version()}: {current.to_pyarrow_table().num_rows} rows; version 0: {first.to_pyarrow_table().num_rows} rows")
            """),
        _code("exp-check", """
            total_sales = con.sql("SELECT round(sum(net_amount), 2) FROM fact_sales").fetchone()[0]
            total_gold = con.sql("SELECT round(sum(net_revenue), 2) FROM gold_daily_sales").fetchone()[0]
            assert abs(total_sales - total_gold) < 1.0, (total_sales, total_gold)
            print(f"revenue {total_sales:,.2f} reconciles with gold_daily_sales")
            """),
    ]
    return _notebook(cells, "python")


# ----------------------------------------------------------------------------- manifest
def notebooks() -> dict[str, dict[str, Any]]:
    return {"00_README": readme_notebook(), "01_Retail_Medallion_ETL": etl_notebook(), "02_Retail_Explore_Python": explore_notebook()}


def manifest() -> list[dict[str, Any]]:
    """Notebook item records: ``[{name, description, tags, definition}]``; ``definition`` follows CONTRACTS section 10.

    ``02_Retail_Explore_Python`` asks for the ``host-python`` runtime family: it needs ``deltalake``, ``duckdb`` and ``pandas`` (matplotlib optional), which the workers'
    own interpreter provides by definition, whereas an installed Spark runtime (e.g. ``fabric-2.0-compatible``) is not guaranteed to carry them.
    """
    nbs = notebooks()
    default_lh = {"name": LH}
    return [
        {"name": "00_README", "description": "Tour of the sample retail workspace.", "tags": ["sample", "retail", "docs"],
         "definition": {"nbformat": nbs["00_README"], "settings": {"engine": "python", "default_lakehouse": default_lh}}},
        {"name": "01_Retail_Medallion_ETL", "description": "PySpark + Delta medallion pipeline: bronze CSV to silver table to gold aggregates (Spark SQL, MERGE, DESCRIBE HISTORY).",
         "tags": ["sample", "retail", "spark", "etl"],
         "definition": {"nbformat": nbs["01_Retail_Medallion_ETL"], "settings": {"engine": "spark", "runtime": {"family": "fabric-2.0-compatible"}, "default_lakehouse": default_lh}}},
        {"name": "02_Retail_Explore_Python", "description": "Explore the lakehouse with deltalake, DuckDB, pandas and matplotlib (no Spark).", "tags": ["sample", "retail", "python", "eda"],
         "definition": {"nbformat": nbs["02_Retail_Explore_Python"], "settings": {"engine": "python", "runtime": {"family": HOST_FAMILY}, "default_lakehouse": default_lh}}},
    ]
