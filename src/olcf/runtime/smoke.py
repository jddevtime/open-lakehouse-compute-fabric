# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Runtime smoke test, executed *inside* a runtime venv: ``python -I -m olcf.runtime.smoke --mode spark ...``.

The runtime manager runs this after every install/repair (and on ``verify --deep``) before writing the ``VERIFIED``
marker. It prints a single JSON line ``{"ok": bool, "steps": [{"name","ok","detail","ms"}]}`` and exits non-zero on failure.

Steps (``--mode spark``): imports -> pyspark version matches the manifest -> start a real local Spark session through
the same code path jobs use (:func:`olcf.runtime_lib.spark.create_session`) -> a shuffle job -> Parquet round-trip ->
(optional) pandas/Arrow conversion -> **Delta write / read / MERGE / history round-trip when ``--delta``** (the Delta
jars were resolved at install time). ``--mode import`` only checks that Python and the listed modules import.
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
import time
import traceback
from pathlib import Path
from typing import Any


def _run(steps: list[dict[str, Any]], name: str, fn) -> bool:  # noqa: ANN001
    t0 = time.monotonic()
    try:
        detail = fn()
        steps.append({"name": name, "ok": True, "detail": str(detail or ""), "ms": round((time.monotonic() - t0) * 1000, 1)})
        return True
    except Exception as exc:  # noqa: BLE001 - every failure becomes a step result
        steps.append({"name": name, "ok": False, "detail": f"{type(exc).__name__}: {exc}\n{traceback.format_exc()[-1500:]}",
                      "ms": round((time.monotonic() - t0) * 1000, 1)})
        return False


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="olcf.runtime.smoke")
    ap.add_argument("--mode", choices=["spark", "import", "none"], default="spark")
    ap.add_argument("--workdir", default=None)
    ap.add_argument("--expect-spark", default=None)
    ap.add_argument("--imports", default="")
    ap.add_argument("--delta", action="store_true")
    ns = ap.parse_args(argv)
    steps: list[dict[str, Any]] = []
    work = Path(ns.workdir or tempfile.mkdtemp(prefix="olcf-smoke-"))
    _run(steps, "python", lambda: sys.version.split()[0])
    for mod in [m for m in ns.imports.split(",") if m]:
        _run(steps, f"import:{mod}", lambda mod=mod: __import__(mod).__name__)
    _run(steps, "import:olcf.runtime_lib", lambda: __import__("olcf.runtime_lib.runner", fromlist=["x"]).__name__)
    ok = all(s["ok"] for s in steps)
    if ns.mode == "spark" and ok:
        ok = _spark_steps(steps, work, ns.expect_spark, ns.delta)
    print(json.dumps({"ok": ok, "steps": steps}))
    return 0 if ok else 1


def _spark_steps(steps: list[dict[str, Any]], work: Path, expect: str | None, delta: bool) -> bool:
    holder: dict[str, Any] = {}

    def version() -> str:
        import pyspark

        if expect and pyspark.__version__ != expect:
            raise RuntimeError(f"pyspark {pyspark.__version__} installed but the manifest pins {expect}")
        return pyspark.__version__

    if not _run(steps, "pyspark-version", version):
        return False

    def start() -> str:
        from olcf.runtime_lib import spark as sp

        spec = {"name": "olcf-smoke", "spark": {"master": "local[1]", "app_name": "olcf-smoke", "conf": {
            "spark.driver.memory": "512m", "spark.sql.shuffle.partitions": "2", "spark.ui.enabled": "false",
            "spark.sql.warehouse.dir": (work / "warehouse").as_uri(), "spark.driver.bindAddress": "127.0.0.1",
            "spark.driver.host": "127.0.0.1", "spark.local.dir": str(work / "local")}}}
        holder["spark"] = sp.create_session(spec)
        return f"Spark {holder['spark'].version}, app {holder['spark'].sparkContext.applicationId}"

    if not _run(steps, "start-session", start):
        return False
    spark = holder["spark"]
    good = True

    def shuffle() -> str:
        total = spark.range(0, 1000).selectExpr("sum(id) as s").collect()[0]["s"]
        assert total == 499500, total
        grouped = spark.range(0, 1000).selectExpr("id % 7 as k").groupBy("k").count().orderBy("k").collect()
        assert len(grouped) == 7 and sum(r["count"] for r in grouped) == 1000
        return "sum + groupBy shuffle ok"

    good &= _run(steps, "shuffle-job", shuffle)

    def parquet() -> str:
        p = (work / "pq").as_uri()
        spark.range(0, 50).write.mode("overwrite").parquet(p)
        n = spark.read.parquet(p).count()
        assert n == 50
        return "parquet write/read ok"

    good &= _run(steps, "parquet-roundtrip", parquet)

    def pandas_arrow() -> str:
        try:
            import pandas  # noqa: F401
            import pyarrow  # noqa: F401
        except ImportError:
            return "pandas/pyarrow not installed (skipped)"
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        pdf = spark.range(0, 10).toPandas()
        assert len(pdf) == 10
        back = spark.createDataFrame(pdf).count()
        assert back == 10
        return "toPandas/createDataFrame ok"

    good &= _run(steps, "pandas-arrow", pandas_arrow)
    if delta:
        def delta_roundtrip() -> str:
            p = (work / "delta_tbl").as_uri()
            spark.range(0, 10).selectExpr("id", "cast(id as string) as v").write.format("delta").mode("overwrite").save(p)
            spark.sql(f"CREATE TABLE IF NOT EXISTS smoke_delta USING delta LOCATION '{p}'")
            src = spark.createDataFrame([(5, "five-updated"), (100, "new")], ["id", "v"])
            src.createOrReplaceTempView("src")
            spark.sql("MERGE INTO smoke_delta t USING src s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.v = s.v WHEN NOT MATCHED THEN INSERT *")
            rows = {r["id"]: r["v"] for r in spark.table("smoke_delta").collect()}
            assert rows[5] == "five-updated" and rows[100] == "new" and len(rows) == 11, rows
            hist = spark.sql("DESCRIBE HISTORY smoke_delta").collect()
            assert len(hist) >= 2
            return f"delta write/read/MERGE ok ({len(hist)} commits)"

        good &= _run(steps, "delta-roundtrip", delta_roundtrip)
    try:
        spark.stop()
    except Exception:  # noqa: BLE001, S110
        pass
    return good


if __name__ == "__main__":
    raise SystemExit(main())
