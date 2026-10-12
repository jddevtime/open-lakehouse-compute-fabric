# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Standalone equivalent of the etl_orders notebook: plain PySpark, no Fabric API. ``main(roots, params, spark)`` returns the result the notebook exits with."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def main(roots: dict[str, str], params: dict[str, Any], spark: Any) -> dict[str, Any]:
    from pyspark.sql import functions as F

    root = Path(roots["default"])
    min_qty = params.get("min_qty", 1)
    raw = spark.read.option("header", True).csv(str(root / "Files" / "orders.csv"))
    clean = (raw.where(F.col("customer_id").isNotNull())
             .withColumn("qty", F.col("qty").cast("int"))
             .withColumn("price", F.col("price").cast("double"))
             .withColumn("order_date", F.to_date("order_date"))
             .where(F.col("status") == "paid")
             .where(F.col("qty") >= min_qty)
             .withColumn("revenue", F.round(F.col("qty") * F.col("price"), 2)))
    out = str(root / "Files" / "out" / "orders_clean")
    clean.write.mode("overwrite").parquet(out)
    back = spark.read.parquet(out)
    agg = back.groupBy("customer_id").agg(F.round(F.sum("revenue"), 2).alias("revenue")).collect()
    return {"rows": back.count(), "revenue_by_customer": {r["customer_id"]: r["revenue"] for r in agg},
            "months": sorted(r[0] for r in back.select(F.date_format("order_date", "yyyy-MM")).distinct().collect())}
