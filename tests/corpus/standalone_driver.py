#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Runs the ``standalone.py`` equivalents of the corpus cases.

Pure Python / delta-rs cases run in this interpreter; Spark cases run in ONE subprocess of the cached Spark runtime (one JVM for all of them, under the sandbox Spark lock)::

    python standalone_driver.py --spark etl_orders joins ...      # child mode: prints  @@RESULT@@ {"case": {...}}
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
if str(ROOT) not in sys.path:  # run as a script (child process): the repository root makes ``tests.*`` importable
    sys.path.insert(0, str(ROOT))

from tests.corpus.corpus_defs import CASES, CASES_DIR, standalone_engine  # noqa: E402
from tests.corpus.corpus_support import SMALL_JVM, materialise, spark_lock, spark_runtime  # noqa: E402

MARK = "@@RESULT@@ "


def load(name: str):
    spec = importlib.util.spec_from_file_location(f"corpus_standalone_{name}", CASES_DIR / name / "standalone.py")
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


def run_one(name: str, spark: Any = None) -> dict[str, Any]:
    case = CASES[name]
    with tempfile.TemporaryDirectory(prefix=f"corpus-{name}-") as tmp:
        roots = materialise(CASES_DIR / name, case.extra_lakehouses, Path(tmp))
        out = load(name).main(roots, dict(case.parameters), spark)
    return json.loads(json.dumps(out, sort_keys=True))     # what the notebook's exit value looks like after a JSON round trip


def spark_session() -> Any:
    from pyspark.sql import SparkSession

    b = SparkSession.builder.master("local[1]").appName("olcf-corpus-standalone")
    for k, v in SMALL_JVM.items():
        b = b.config(k, v)
    s = b.getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    return s


def child(names: list[str]) -> None:
    spark = spark_session()
    try:
        out = {n: run_one(n, spark) for n in names}
    finally:
        spark.stop()
    print(MARK + json.dumps(out, sort_keys=True))


def run_all(names: list[str] | None = None) -> dict[str, dict[str, Any]]:
    names = list(names or CASES)
    got: dict[str, dict[str, Any]] = {}
    for n in [n for n in names if standalone_engine(CASES[n]) != "spark"]:
        got[n] = run_one(n)
    sparks = [n for n in names if standalone_engine(CASES[n]) == "spark"]
    if sparks:
        _, python = spark_runtime()
        with spark_lock():
            proc = subprocess.run([str(python), str(Path(__file__).resolve()), "--spark", *sparks], capture_output=True, text=True, timeout=600, check=False,
                                  env={"PATH": "/usr/bin:/bin:/usr/local/bin", "HOME": tempfile.gettempdir(), "LANG": "C.UTF-8",
                                       "PYSPARK_PYTHON": str(python)})  # workers must run the driver's interpreter, not whatever python3 the PATH finds
        line = next((ln for ln in proc.stdout.splitlines() if ln.startswith(MARK)), None)
        if proc.returncode != 0 or line is None:
            raise RuntimeError(f"the Spark standalone run failed ({proc.returncode}):\n{proc.stderr[-4000:]}\n{proc.stdout[-1000:]}")
        got.update(json.loads(line[len(MARK):]))
    return got


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--spark", nargs="+")
    ap.add_argument("names", nargs="*")
    a = ap.parse_args()
    if a.spark:
        child(a.spark)
    else:
        print(json.dumps(run_all(a.names or None), indent=2, sort_keys=True))
