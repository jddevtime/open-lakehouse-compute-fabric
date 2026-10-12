# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Standalone equivalent of the joins notebook (plain PySpark)."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def main(roots: dict[str, str], params: dict[str, Any], spark: Any) -> dict[str, Any]:
    from pyspark.sql import functions as F

    files = Path(roots["default"]) / "Files"
    orders = (spark.read.option("header", True).csv(str(files / "orders.csv")).withColumn("qty", F.col("qty").cast("int")).where(F.col("customer_id").isNotNull()))
    customers = spark.read.option("header", True).csv(str(files / "customers.csv"))
    inner = orders.join(customers, "customer_id", "inner")
    left = orders.join(customers, "customer_id", "left")
    anti = orders.join(customers, "customer_id", "left_anti")
    bcast = orders.join(F.broadcast(customers), "customer_id").groupBy("country").agg(F.sum("qty").alias("qty"))
    a, b = orders.alias("a"), orders.alias("b")
    same_sku = a.join(b, (F.col("a.sku") == F.col("b.sku")) & (F.col("a.order_id") < F.col("b.order_id"))).count()
    return {"inner": sorted(r["order_id"] for r in inner.collect()), "left_null_names": sorted(r["order_id"] for r in left.where(F.col("name").isNull()).collect()),
            "anti": sorted(r["customer_id"] for r in anti.select("customer_id").distinct().collect()), "qty_by_country": {r["country"]: r["qty"] for r in bcast.collect()},
            "same_sku_pairs": same_sku}
