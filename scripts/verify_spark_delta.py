#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Verify Delta Lake (and optionally Iceberg) on an installed OLCF runtime.

    The Delta / Iceberg / Hadoop-cloud jars come from Maven Central, so the Delta code paths
    (``spark.build_spark_conf`` extension + catalog generation, jar linking into ``pyspark/jars``, lakehouse table registration)
    are covered by unit tests (``tests/node/test_runtime_lib.py``) and by the Delta-free Spark integration tests
    (``tests/node/test_spark_cluster.py``) without network access. Run THIS script on a machine with Internet access (Python 3.11+, Java 17 or 21)
    before relying on Delta in production, and attach its output to the release record.

What it does (every step prints PASS/FAIL; the exit status is the number of failed checks, capped at 125):

1. installs a signed bundled runtime (default ``olcf-rt-2.0.0``: Spark 4.0 + Delta + Iceberg) with the production
   :class:`olcf.runtime.manager.RuntimeManager` (venv + ``pip --require-hashes`` + Maven jars checked against the pinned sha256),
2. checks that the Delta jars are linked into ``pyspark/jars`` and recorded in ``lock.json``,
3. runs a job through the real runner (``python -m olcf.runtime_lib.runner job.json``) against a local ``file://`` lakehouse:
   Delta create / append / overwrite / MERGE / UPDATE / DELETE / time travel / ``DESCRIBE HISTORY`` / schema evolution / OPTIMIZE-VACUUM
   dry-run, ``saveAsTable`` into the default lakehouse and re-registration of ``Tables/<name>`` as an external table in a *second*
   job (the Fabric default-lakehouse semantics), relative ``Files/`` and ``Tables/`` paths, and the Iceberg extension when present,
4. verifies the on-disk layout (``_delta_log`` JSON commits, checkpoint-free small table, parquet part files).

Usage::

    python scripts/verify_spark_delta.py [--runtime olcf-rt-2.0.0] [--state-dir DIR] [--keep] [--with-jdk] [--skip-install]
                                         [--maven-repository URL] [--index-url URL] [--iceberg]

``--skip-install`` reuses an already installed runtime in ``--state-dir`` (default: a fresh temporary directory, removed on exit
unless ``--keep``). Needs: network to PyPI + Maven Central (or mirrors via the options), ~1.5 GB disk, ~2 GB RAM.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
import textwrap
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from olcf.runtime.manager import ManagerConfig, RuntimeManager  # noqa: E402

PASSED: list[str] = []
FAILED: list[tuple[str, str]] = []


def check(name: str, ok: bool, detail: str = "") -> bool:
    (PASSED if ok else FAILED).append(name if ok else (name, detail))  # type: ignore[arg-type]
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f" - {detail}" if detail and not ok else ""))
    return ok


JOB_1 = textwrap.dedent('''
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    results = {}
    # 1. relative paths resolve against the default lakehouse (Files/ and Tables/)
    people = spark.createDataFrame([(1, "ada", 36), (2, "grace", 45), (3, "linus", 28)], ["id", "name", "age"])
    people.write.format("delta").mode("overwrite").save("Tables/people")
    people.write.mode("overwrite").option("header", True).csv("Files/exports/people_csv")
    results["tables_people_rows"] = spark.read.format("delta").load("Tables/people").count()
    results["files_csv_rows"] = spark.read.option("header", True).csv("Files/exports/people_csv").count()

    # 2. append / overwrite / schema evolution
    spark.createDataFrame([(4, "ken", 80)], ["id", "name", "age"]).write.format("delta").mode("append").save("Tables/people")
    spark.createDataFrame([(5, "dennis", 70, "NJ")], ["id", "name", "age", "state"]).write.format("delta").mode("append").option("mergeSchema", "true").save("Tables/people")
    results["after_append"] = spark.read.format("delta").load("Tables/people").count()
    results["columns"] = spark.read.format("delta").load("Tables/people").columns

    # 3. MERGE / UPDATE / DELETE through the Python API and through SQL
    tbl = DeltaTable.forPath(spark, spark.conf.get("spark.sql.warehouse.dir") + "/people")
    upd = spark.createDataFrame([(2, "grace hopper", 46, "NY"), (9, "barbara", 50, "MA")], ["id", "name", "age", "state"])
    (tbl.alias("t").merge(upd.alias("s"), "t.id = s.id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    tbl.update("id = 1", {"age": F.lit(37)})
    tbl.delete("id = 3")
    results["after_dml"] = sorted(r["id"] for r in tbl.toDF().collect())
    results["grace"] = tbl.toDF().filter("id = 2").first()["name"]

    # 4. history + time travel
    hist = tbl.history().select("version", "operation").orderBy("version").collect()
    results["history_ops"] = [r["operation"] for r in hist]
    results["v0_rows"] = spark.read.format("delta").option("versionAsOf", 0).load("Tables/people").count()

    # 5. saveAsTable lands in the default lakehouse database as a managed-location Delta table
    spark.range(10).withColumn("sq", F.col("id") * F.col("id")).write.format("delta").mode("overwrite").saveAsTable("squares")
    results["squares"] = spark.sql("select count(*) c, sum(sq) s from squares").first().asDict()
    results["current_db"] = spark.catalog.currentDatabase()

    # 6. SQL DDL / DML against the Delta catalog
    spark.sql("create table if not exists sql_made (k int, v string) using delta")
    spark.sql("insert into sql_made values (1,'a'),(2,'b')")
    spark.sql("update sql_made set v = 'bb' where k = 2")
    results["sql_made"] = [tuple(r) for r in spark.sql("select * from sql_made order by k").collect()]
    results["describe_detail_format"] = spark.sql("describe detail sql_made").first()["format"]

    # 7. maintenance commands accepted by the Delta extension
    spark.sql("optimize sql_made")
    spark.sql("vacuum sql_made dry run")
    results["extensions"] = spark.conf.get("spark.sql.extensions")
    results["catalog"] = spark.conf.get("spark.sql.catalog.spark_catalog")
    EXIT_VALUE = results
''')

JOB_2 = textwrap.dedent('''
    # a NEW job (new JVM): tables written by job 1 must be visible by name again (external-table registration of Tables/*)
    rows = spark.sql("select count(*) c from people").first()["c"]
    squares = spark.table("squares").count()
    names = sorted(t.name for t in spark.catalog.listTables())
    EXIT_VALUE = {"people": rows, "squares": squares, "tables": names, "db": spark.catalog.currentDatabase()}
''')

JOB_ICEBERG = textwrap.dedent('''
    spark.conf.set("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.ice.type", "hadoop")
    spark.conf.set("spark.sql.catalog.ice.warehouse", spark.conf.get("spark.sql.warehouse.dir") + "/../iceberg-wh")
    spark.sql("create table ice.db.t (id int, v string) using iceberg")
    spark.sql("insert into ice.db.t values (1,'x'),(2,'y')")
    EXIT_VALUE = {"rows": spark.sql("select count(*) c from ice.db.t").first()["c"], "snapshots": spark.sql("select count(*) c from ice.db.t.snapshots").first()["c"]}
''')


def run_job(rt_root: Path, runtime_id: str, workdir: Path, name: str, source: str, lakehouse: dict, extra_spark: dict | None = None, timeout: int = 900) -> dict:
    """Run one job exactly like the agent does: ``<venv>/python -I -m olcf.runtime_lib.runner job.json`` in a scrubbed environment."""
    wd = workdir / name
    (wd / "source").mkdir(parents=True, exist_ok=True)
    (wd / "source" / "main.py").write_text(source)
    spec = {"schema": "olcf.jobspec/1", "job_id": name, "attempt": 1, "kind": "spark_job", "engine": "pyspark", "name": name, "source": {"format": "py", "path": "source/main.py"},
            "parameters": {}, "runtime": {"id": runtime_id}, "limits": {"timeout_seconds": timeout},
            "spark": {"master": "local[2]", "conf": {"spark.driver.memory": "1g", "spark.ui.enabled": "false", "spark.sql.shuffle.partitions": "4", **(extra_spark or {})}},
            "default_lakehouse": lakehouse, "lakehouses": [], "env": {}, "secrets": {}, "context": {}}
    (wd / "job.json").write_text(json.dumps(spec))
    mgr = RuntimeManager(ManagerConfig(state_dir=rt_root))
    handle = mgr.light_check(runtime_id)
    env = {**handle.env([rt_root / "runtimes" / "_jdk" / str(m) for m in (17, 21)]), "OLCF_JOB_DIR": str(wd), "HOME": str(wd), "TMPDIR": str(wd), "PYTHONUNBUFFERED": "1"}
    proc = subprocess.run([str(handle.python), "-I", "-m", "olcf.runtime_lib.runner", str(wd / "job.json")], cwd=wd, env=env, capture_output=True, text=True, timeout=timeout + 60, check=False)  # noqa: S603
    try:
        result = json.loads((wd / "result.json").read_text())
    except (OSError, ValueError):
        result = {"state": "failed", "failure": {"message": "no result.json written"}}
    result["_returncode"] = proc.returncode
    result["_stdout_tail"] = proc.stdout[-3000:]
    result["_stderr_tail"] = proc.stderr[-3000:]
    return result


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--runtime", default="olcf-rt-2.0.0")
    ap.add_argument("--state-dir", type=Path)
    ap.add_argument("--keep", action="store_true")
    ap.add_argument("--skip-install", action="store_true")
    ap.add_argument("--with-jdk", action="store_true")
    ap.add_argument("--maven-repository", action="append")
    ap.add_argument("--index-url")
    ap.add_argument("--iceberg", action="store_true", help="also exercise the Iceberg extension (runtime must contain the Iceberg jar)")
    a = ap.parse_args()

    state = a.state_dir or Path(tempfile.mkdtemp(prefix="olcf-verify-delta-"))
    state.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    try:
        mgr = RuntimeManager(ManagerConfig(state_dir=state, maven_repositories=a.maven_repository, pip_index_url=a.index_url, with_jdk=a.with_jdk))
        if not a.skip_install:
            print(f"installing {a.runtime} into {state} (several minutes; downloads Spark, pyspark deps and the Maven jars)...")
            try:
                res = mgr.install(a.runtime, progress=lambda s, d: print(f"  [{s}] {d}"))
                check("runtime installs (signature, hashes, jars, smoke)", True)
                print(f"  fingerprint {res.get('fingerprint')}")
            except Exception as exc:  # noqa: BLE001
                check("runtime installs (signature, hashes, jars, smoke)", False, str(exc))
                return min(len(FAILED), 125)
        h = mgr.light_check(a.runtime)
        jars = [j.get("coordinate", "") for j in h.lock.get("jars", [])]
        check("lock.json lists the delta-spark jar", any("delta-spark" in j for j in jars), str(jars))
        linked = {p.name for p in (h.spark_home / "jars").glob("*.jar")}
        check("delta jars are linked into pyspark/jars", any(n.startswith("delta-spark") for n in linked) and any(n.startswith("delta-storage") for n in linked))
        check("runtime verify (hashes + smoke)", bool(mgr.verify(a.runtime, deep=True)))

        lake = state / "lakehouse"
        for sub in ("Files", "Tables"):
            (lake / sub).mkdir(parents=True, exist_ok=True)
        lh = {"id": "lh-verify", "name": "verify_lh", "root_uri": lake.as_uri(), "files_uri": (lake / "Files").as_uri(), "tables_uri": (lake / "Tables").as_uri(), "format": "delta"}
        work = state / "jobs"

        print("\nrunning job 1 (Delta DML, time travel, SQL, saveAsTable)...")
        r1 = run_job(state, a.runtime, work, "delta1", JOB_1, lh)
        ok = check("job 1 succeeded", r1.get("state") == "succeeded", json.dumps(r1.get("failure") or r1["_stderr_tail"])[:1500])
        if ok:
            v = r1["exit_value"]
            check("relative Tables/ and Files/ paths resolve against the default lakehouse", v["tables_people_rows"] == 3 and v["files_csv_rows"] == 3, str(v))
            check("append + schema evolution (mergeSchema)", v["after_append"] == 5 and "state" in v["columns"], str(v))
            check("MERGE / UPDATE / DELETE", v["after_dml"] == [1, 2, 4, 5, 9] and v["grace"] == "grace hopper", str(v["after_dml"]))
            check("history contains the DML operations", {"MERGE", "UPDATE", "DELETE"} <= set(v["history_ops"]), str(v["history_ops"]))
            check("time travel versionAsOf=0", v["v0_rows"] == 3, str(v["v0_rows"]))
            check("saveAsTable lands in the lakehouse database", v["squares"] == {"c": 10, "s": 285} and v["current_db"] == "verify_lh", str((v["squares"], v["current_db"])))
            check("SQL DDL/DML on Delta", v["sql_made"] == [(1, "a"), (2, "bb")] and v["describe_detail_format"] == "delta", str(v["sql_made"]))
            check("Delta extension + catalog configured by the runtime", "DeltaSparkSessionExtension" in v["extensions"] and v["catalog"].endswith("DeltaCatalog"), str((v["extensions"], v["catalog"])))
        log = lake / "Tables" / "people" / "_delta_log"
        commits = sorted(p.name for p in log.glob("*.json")) if log.is_dir() else []
        check("on-disk _delta_log has one JSON commit per write", len(commits) >= 6, str(commits))
        check("parquet data files exist", bool(list((lake / "Tables" / "people").glob("*.parquet"))))

        print("\nrunning job 2 (new JVM: tables must be re-registered by name)...")
        r2 = run_job(state, a.runtime, work, "delta2", JOB_2, lh)
        ok2 = check("job 2 succeeded", r2.get("state") == "succeeded", json.dumps(r2.get("failure") or r2["_stderr_tail"])[:1500])
        if ok2:
            v2 = r2["exit_value"]
            check("Tables/* are registered as external tables in the next job", v2["people"] == 5, str(v2))
            check("squares / people / sql_made visible by name", {"people", "squares", "sql_made"} <= set(v2["tables"]), str(v2["tables"]))

        if a.iceberg:
            print("\nrunning the Iceberg job...")
            r3 = run_job(state, a.runtime, work, "ice1", JOB_ICEBERG, lh)
            ok3 = check("Iceberg job succeeded", r3.get("state") == "succeeded", json.dumps(r3.get("failure") or r3["_stderr_tail"])[:1500])
            if ok3:
                check("Iceberg table written and snapshot recorded", r3["exit_value"] == {"rows": 2, "snapshots": 1}, str(r3["exit_value"]))
    finally:
        if not a.keep and not a.state_dir:
            shutil.rmtree(state, ignore_errors=True)
        elif a.keep or a.state_dir:
            print(f"\nstate kept in {state}")
    print(f"\n{len(PASSED)} checks passed, {len(FAILED)} failed in {time.time() - t0:.0f}s")
    for name, detail in FAILED:
        print(f"  FAILED: {name}\n          {detail[:600]}")
    return min(len(FAILED), 125)


if __name__ == "__main__":
    sys.exit(main())
