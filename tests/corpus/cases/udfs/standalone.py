# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Standalone equivalent of the udfs notebook (plain PySpark)."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def main(roots: dict[str, str], params: dict[str, Any], spark: Any) -> dict[str, Any]:
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType, StringType

    @F.udf(returnType=StringType())
    def initials(name):
        return None if name is None else name[:1].upper() + "."

    def region_of(country):
        return {"US": "AMER", "DE": "EMEA", "FR": "EMEA"}.get(country, "OTHER")

    spark.udf.register("region_of", region_of, StringType())
    spark.udf.register("name_len", lambda s: len(s) if s else 0, IntegerType())
    df = spark.read.option("header", True).csv(str(Path(roots["default"]) / "Files" / "customers.csv"))
    df.createOrReplaceTempView("customers")
    a = df.withColumn("initials", initials("name")).orderBy("customer_id").collect()
    b = spark.sql("select customer_id, region_of(country) as region, name_len(name) as n from customers order by customer_id").collect()
    return {"initials": {r["customer_id"]: r["initials"] for r in a}, "region": {r["customer_id"]: r["region"] for r in b}, "name_len": {r["customer_id"]: r["n"] for r in b}}
