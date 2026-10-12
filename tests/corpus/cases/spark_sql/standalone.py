# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Standalone equivalent of the spark_sql notebook (plain PySpark)."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def main(roots: dict[str, str], params: dict[str, Any], spark: Any) -> dict[str, Any]:
    from pyspark.sql import functions as F

    files = Path(roots["default"]) / "Files"
    country = params.get("country", "DE")
    (spark.read.option("header", True).csv(str(files / "orders.csv")).withColumn("qty", F.col("qty").cast("int")).withColumn("price", F.col("price").cast("double"))
     .createOrReplaceTempView("orders"))
    spark.read.option("header", True).csv(str(files / "customers.csv")).createOrReplaceTempView("customers")
    spark.sql("CREATE OR REPLACE TEMP VIEW paid AS SELECT * FROM orders WHERE status = 'paid' AND customer_id IS NOT NULL")
    by_country = spark.sql(f""" 
        WITH j AS (SELECT c.country, o.qty, o.price FROM paid o JOIN customers c USING (customer_id))
        SELECT country, COUNT(*) AS n, ROUND(SUM(qty * price), 2) AS revenue FROM j WHERE country = '{country}' GROUP BY country
    """).collect()  # noqa: S608 - fixed test literal
    ranked = spark.sql("SELECT customer_id, RANK() OVER (ORDER BY qty DESC) AS r FROM paid").orderBy("customer_id", "r").collect()
    return {"by_country": {r["country"]: [r["n"], r["revenue"]] for r in by_country}, "ranked": [[r["customer_id"], r["r"]] for r in ranked]}
