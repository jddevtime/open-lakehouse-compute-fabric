# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Standalone equivalent of the windows notebook (plain PySpark)."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def main(roots: dict[str, str], params: dict[str, Any], spark: Any) -> dict[str, Any]:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    sales = spark.read.option("header", True).csv(str(Path(roots["default"]) / "Files" / "sales.csv")).withColumn("amount", F.col("amount").cast("int"))
    by_region = Window.partitionBy("region").orderBy(F.col("amount").desc(), "rep", "month")
    running = Window.partitionBy("region", "rep").orderBy("month").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    ordered = Window.partitionBy("rep").orderBy("month")
    w = (sales.withColumn("rn", F.row_number().over(by_region)).withColumn("rank", F.rank().over(Window.partitionBy("region").orderBy(F.col("amount").desc())))
         .withColumn("running", F.sum("amount").over(running)).withColumn("prev", F.lag("amount", 1).over(ordered)).withColumn("delta", F.col("amount") - F.col("prev")))
    top = {r["region"]: [r["rep"], r["month"], r["amount"]] for r in w.where("rn = 1").collect()}
    running_end = {r["rep"]: r["running"] for r in w.where("month = '2026-02'").collect()}
    deltas = {r["rep"]: r["delta"] for r in w.where("month = '2026-02'").collect()}
    ranks = sorted([r["region"], r["rep"], r["month"], r["rank"]] for r in w.collect())
    return {"top": top, "running_end": running_end, "deltas": deltas, "ranks": ranks}
