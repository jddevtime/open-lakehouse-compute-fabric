#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Regenerate the signed, hash-pinned runtime manifests under ``src/olcf/runtime/manifests/``.

Needs network access to PyPI (and ``uv`` on PATH). Usage::

    python scripts/lock_runtime.py --all                    # all three families, signed with the dev key
    python scripts/lock_runtime.py olcf-rt-2.0.0 --key my-org.key --out /tmp/manifests
    python scripts/lock_runtime.py --all --index-url https://pypi.org/pypi

What it does (see ``olcf/runtime/locktool.py``): resolve top-level requirements universally with ``uv``, look up the
sha256 of every release file installable on linux x86_64/aarch64, macOS arm64/x86_64 and Windows for the target
Python, write them as ``packages`` pins, then sign the manifest with Ed25519. Commit the resulting JSON; the diff
is the audit trail of every dependency change. Maven jars are *not* hashed here (Maven Central is only needed on the
machine that installs the runtime): their hashes are recorded in each node's ``lock.json`` at first resolution.

Author: Justin Dodrill.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from olcf.runtime import locktool  # noqa: E402
from olcf.runtime.signing import load_private_key, sign_manifest_dict  # noqa: E402

# Spark's own jars (hadoop-client-api/runtime are shaded, spark-*, scala, slf4j, log4j ...) are never re-downloaded.
_PROVIDED = [
    "org.apache.hadoop:hadoop-client-*", "org.apache.hadoop:hadoop-common", "org.apache.hadoop:hadoop-hdfs*", "org.apache.hadoop:hadoop-auth",
    "org.apache.hadoop:hadoop-annotations", "org.apache.hadoop:hadoop-yarn-*", "org.apache.hadoop:hadoop-mapreduce-*",
    "org.apache.spark:*", "org.scala-lang:*", "org.scala-lang.modules:*", "org.slf4j:*", "org.apache.logging.log4j:*", "log4j:*",
    "commons-logging:commons-logging", "javax.servlet:*", "jakarta.servlet:*",
]
_APPROVED_CONF = [
    "spark.executor.memory", "spark.executor.cores", "spark.executor.instances", "spark.executor.memoryOverhead", "spark.driver.memory",
    "spark.driver.cores", "spark.driver.maxResultSize", "spark.cores.max", "spark.default.parallelism", "spark.sql.*", "spark.dynamicAllocation.*",
    "spark.serializer", "spark.kryoserializer.*", "spark.sql.shuffle.partitions", "spark.memory.*", "spark.speculation*", "spark.task.*",
    "spark.hadoop.fs.*", "spark.hadoop.parquet.*", "spark.hadoop.mapreduce.*", "spark.databricks.delta.*", "spark.delta.*", "spark.pyspark.*",
    "spark.executorEnv.*", "spark.ui.*", "spark.eventLog.*", "spark.files.*", "spark.network.timeout", "spark.rpc.*",
]
_SPARK_DEFAULTS = {"spark.sql.session.timeZone": "UTC", "spark.sql.adaptive.enabled": "true", "spark.sql.parquet.compression.codec": "zstd",
                   "spark.ui.enabled": "true", "spark.sql.ansi.enabled": "false"}
_COMMON_TOP = ["ipykernel>=6.29", "jupyter_client>=8.6", "nbformat>=5.10", "requests>=2.32", "packaging", "psutil>=5.9"]

SPECS: dict[str, dict] = {
    "olcf-rt-2.0.0": {
        "id": "olcf-rt-2.0.0", "version": "2.0.0", "family": "fabric-2.0-compatible",
        "title": "OLCF Runtime 2.0 (Fabric Runtime 2.0 Compatible): Spark 4.1.1, Delta 4.2.0, Java 21, Scala 2.13, Python 3.13",
        "description": "Open-source runtime aligned to the Spark 4.1 / Delta 4.2 / Java 21 / Scala 2.13 / Python 3.13 family. Compatible with, not identical to, Microsoft Fabric Runtime 2.0.",
        "spark": {"version": "4.1.1"}, "delta": {"version": "4.2.0"}, "iceberg": {"version": "1.11.0"},
        "scala": "2.13", "java": {"major": 21, "supported": [17, 21]}, "python": {"version": "3.13"},
        "os": ["linux", "macos", "windows"], "arch": ["x86_64", "aarch64"],
        "jars": [
            {"coordinate": "io.delta:delta-spark_2.13:4.2.0", "group": "core"},
            {"coordinate": "io.delta:delta-storage:4.2.0", "group": "core"},
            {"coordinate": "org.apache.hadoop:hadoop-azure:${hadoop.version}", "group": "core"},
            {"coordinate": "org.apache.hadoop:hadoop-aws:${hadoop.version}", "group": "s3", "optional": True},
            {"coordinate": "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0", "group": "iceberg", "optional": True},
        ],
        "provided_jars": _PROVIDED, "spark_conf_defaults": _SPARK_DEFAULTS, "approved_spark_conf": _APPROVED_CONF,
        "verify": {"smoke": "spark", "imports": ["pyspark", "delta", "pandas", "pyarrow", "ipykernel", "jupyter_client", "nbformat"]},
        "build_top_level": ["setuptools>=75", "wheel"],
        # pandas is held below 3 until PySpark declares pandas-3 support; bump deliberately and re-run the smoke test.
        "top_level": ["pyspark[sql]==4.1.1", "delta-spark==4.2.0", "pandas>=2.2,<3", "pyarrow>=15", "numpy>=1.26", "deltalake>=1.0", *_COMMON_TOP],
    },
    "olcf-rt-1.3.0": {
        "id": "olcf-rt-1.3.0", "version": "1.3.0", "family": "fabric-1.3-compatible",
        "title": "OLCF Runtime 1.3 (Fabric Runtime 1.3 Compatible, legacy): Spark 3.5.5, Delta 3.2.0, Java 11, Scala 2.12, Python 3.11",
        "description": "Legacy runtime aligned to Spark 3.5 / Delta 3.2 / Java 11 / Scala 2.12 / Python 3.11 for notebooks that have not moved to the 2.0 family.",
        "spark": {"version": "3.5.5"}, "delta": {"version": "3.2.0"}, "iceberg": {"version": "1.11.0"},
        "scala": "2.12", "java": {"major": 11, "supported": [8, 11, 17]}, "python": {"version": "3.11"},
        "os": ["linux", "macos", "windows"], "arch": ["x86_64", "aarch64"],
        "jars": [
            {"coordinate": "io.delta:delta-spark_2.12:3.2.0", "group": "core"},
            {"coordinate": "io.delta:delta-storage:3.2.0", "group": "core"},
            {"coordinate": "org.apache.hadoop:hadoop-azure:${hadoop.version}", "group": "core"},
            {"coordinate": "org.apache.hadoop:hadoop-aws:${hadoop.version}", "group": "s3", "optional": True},
            {"coordinate": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0", "group": "iceberg", "optional": True},
        ],
        "provided_jars": _PROVIDED, "spark_conf_defaults": _SPARK_DEFAULTS, "approved_spark_conf": _APPROVED_CONF,
        "verify": {"smoke": "spark", "imports": ["pyspark", "delta", "pandas", "pyarrow", "ipykernel", "jupyter_client", "nbformat"]},
        "build_top_level": ["setuptools>=65,<81", "wheel"],
        "top_level": ["pyspark[sql]==3.5.5", "delta-spark==3.2.0", "pandas>=1.5,<2.3", "pyarrow>=11,<18", "numpy>=1.23,<2", *_COMMON_TOP],
    },
    "olcf-rt-native-1.0.0": {
        "id": "olcf-rt-native-1.0.0", "version": "1.0.0", "family": "olcf-native",
        "title": "OLCF Native Runtime 1.0: Spark 4.1.1, Delta 4.2.0 + DuckDB, Polars, delta-rs, PyIceberg",
        "description": "OLCF-native runtime that advances independently of Microsoft's cadence: the 2.0 baseline plus additional engines and libraries.",
        "spark": {"version": "4.1.1"}, "delta": {"version": "4.2.0"}, "iceberg": {"version": "1.11.0"},
        "scala": "2.13", "java": {"major": 21, "supported": [17, 21]}, "python": {"version": "3.13"},
        "os": ["linux", "macos", "windows"], "arch": ["x86_64", "aarch64"],
        "jars": [
            {"coordinate": "io.delta:delta-spark_2.13:4.2.0", "group": "core"},
            {"coordinate": "io.delta:delta-storage:4.2.0", "group": "core"},
            {"coordinate": "org.apache.hadoop:hadoop-azure:${hadoop.version}", "group": "core"},
            {"coordinate": "org.apache.hadoop:hadoop-aws:${hadoop.version}", "group": "s3", "optional": True},
            {"coordinate": "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0", "group": "iceberg"},
        ],
        "provided_jars": _PROVIDED, "spark_conf_defaults": _SPARK_DEFAULTS, "approved_spark_conf": _APPROVED_CONF,
        "verify": {"smoke": "spark", "imports": ["pyspark", "delta", "duckdb", "polars", "deltalake", "pyiceberg", "pandas", "pyarrow", "ipykernel"]},
        "build_top_level": ["setuptools>=75", "wheel"],
        "top_level": ["pyspark[sql]==4.1.1", "delta-spark==4.2.0", "pandas>=2.2,<3", "pyarrow>=15", "numpy>=1.26", "deltalake>=1.0", "duckdb>=1.1",
                      "polars>=1.0", "pyiceberg>=0.9", *_COMMON_TOP],
    },
}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("ids", nargs="*", help=f"runtime ids ({', '.join(SPECS)})")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--out", default=str(ROOT / "src" / "olcf" / "runtime" / "manifests"))
    ap.add_argument("--key", default=str(ROOT / "src" / "olcf" / "runtime" / "keys" / "olcf-dev.key"), help="Ed25519 private key (PEM) used to sign")
    ap.add_argument("--index-url", default="https://pypi.org/pypi", help="PyPI JSON API root used to look up hashes")
    ap.add_argument("--no-sign", action="store_true")
    ns = ap.parse_args()
    ids = list(SPECS) if ns.all else ns.ids
    if not ids:
        ap.error("give runtime ids or --all")
    out = Path(ns.out)
    out.mkdir(parents=True, exist_ok=True)
    for rid in ids:
        if rid not in SPECS:
            ap.error(f"unknown runtime id {rid}")
        print(f"== locking {rid}", flush=True)
        manifest = locktool.build_manifest(SPECS[rid], index=ns.index_url, progress=lambda s: print("  ", s, flush=True))
        if not ns.no_sign:
            manifest = sign_manifest_dict(manifest, load_private_key(Path(ns.key)))
        (out / f"{rid}.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
        print(f"   wrote {out / (rid + '.json')} ({len(manifest['packages'])} packages)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
