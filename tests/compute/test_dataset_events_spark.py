# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Runtime lineage with REAL PySpark 4.1.1 (the cached test runtime, local master, parquet tables - no Delta jars).

A child Python process of the runtime installs the probe, runs a small PySpark program and prints ``@@OLCF@@`` events; the control plane's ``JobService`` turns them into
``dataset.*`` platform events, and the pure translation produces facts. Serialised with every other JVM test through ``/tmp/olcf-spark.lock``.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any

import pytest

from olcf.core import dataset_events as dsev
from olcf.intelligence import runtime as intel_runtime
from tests.spark_locking import spark_lock_held

ROOT = Path(__file__).resolve().parents[2]
pytestmark = [pytest.mark.spark, pytest.mark.timeout(600)]

PROGRAM = """
import json, os, sys
from olcf.runtime_lib import lineage_probe
spec = json.loads(sys.argv[1])
lineage_probe.install(spec)                      # before pyspark is imported: the post-import hook patches it on import
from pyspark.sql import SparkSession
spark = (SparkSession.builder.master("local[1]").appName("probe-e2e").config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
         .config("spark.sql.warehouse.dir", os.path.join(spec["root"], "warehouse")).config("spark.driver.bindAddress", "127.0.0.1").getOrCreate())
root = spec["root"]
spark.sql("CREATE DATABASE IF NOT EXISTS Silver")
df = spark.range(10).withColumnRenamed("id", "k")
df.write.mode("overwrite").saveAsTable("Silver.src")                                   # written (table, overwrite)
t = spark.table("Silver.src")                                                          # read (table)
t.write.mode("overwrite").parquet(root + "/Files/out/a")                               # written (file, overwrite)
spark.read.parquet(root + "/Files/out/a").count()                                      # read (file)
spark.read.format("parquet").load(root + "/Files/out/a")                               # read (file, format via .format)
spark.sql("CREATE TABLE Silver.dst USING parquet AS SELECT k FROM Silver.src")         # statement
spark.sql("INSERT INTO Silver.dst SELECT k FROM Silver.src WHERE k > 5")               # statement
t.write.insertInto("Silver.dst")                                                       # written (table, append)
try:
    t.write.mode("errorifexists").parquet(root + "/Files/out/a")                       # fails: the path exists -> nothing may be reported
except Exception as exc:
    print("expected failure:", type(exc).__name__)
spark.sql("SHOW TABLES IN Silver").collect()                                           # housekeeping: not reported
spark.stop()
os._exit(0)
"""


@pytest.fixture(scope="module")
def runtime_python() -> Path:
    from tests.node.runtime_fixture import CACHE, ensure_test_runtime

    _, rid = ensure_test_runtime(lambda m: print(m, file=sys.stderr))          # built BEFORE the lock is taken (building runs smoke tests under the same lock)
    py = CACHE / "state" / "runtimes" / rid / "venv" / "bin" / "python"
    assert py.exists(), py
    return py


def run_program(py: Path, root: Path) -> list[dict[str, Any]]:
    spec = {"root": str(root), "default_lakehouse": {"id": "itm_silver", "name": "Silver", "root_uri": str(root)}, "lakehouses": [], "lakehouse_ids": ["itm_silver"]}
    env = {k: v for k, v in os.environ.items() if k not in ("PYTHONPATH", "OLCF_DATASET_EVENTS")}
    env["PYTHONPATH"] = str(ROOT / "src")
    with spark_lock_held():
        proc = subprocess.run([str(py), "-c", textwrap.dedent(PROGRAM), json.dumps(spec)], capture_output=True, text=True, timeout=420, env=env, cwd=str(root), check=False)  # noqa: S603
    assert proc.returncode == 0, proc.stdout[-2000:] + proc.stderr[-4000:]
    assert "expected failure" in proc.stdout
    return [json.loads(line[len("@@OLCF@@ "):]) for line in proc.stdout.splitlines() if line.startswith("@@OLCF@@ ")]


def test_real_pyspark_reads_and_writes_become_platform_events_and_facts(fleet, runtime_python, tmp_path):
    root = tmp_path / "lh"
    (root / "Files").mkdir(parents=True)
    runner_events = run_program(runtime_python, root)
    kinds = [e["event"] for e in runner_events]
    assert "dataset_events_truncated" not in kinds

    # --- what the probe saw, in order
    def brief(e: dict[str, Any]) -> tuple:
        d = (e.get("datasets") or [{}])[0]
        return e["event"], d.get("kind"), d.get("name") or d.get("path"), e.get("mode")

    got = [brief(e) for e in runner_events if e["event"] != "dataset_statement"]
    assert got == [("dataset_written", "table", "src", "overwrite"), ("dataset_read", "table", "src", None), ("dataset_written", "file", "Files/out/a", "overwrite"),
                   ("dataset_read", "file", "Files/out/a", None), ("dataset_written", "table", "dst", "append")], got
    # the failed errorifexists write reported nothing (the only file write is the first one), nor did SHOW TABLES
    stmts = [e["sql"] for e in runner_events if e["event"] == "dataset_statement"]
    assert stmts == ["CREATE TABLE Silver.dst USING parquet AS SELECT k FROM Silver.src", "INSERT INTO Silver.dst SELECT k FROM Silver.src WHERE k > 5"]
    assert all(e["datasets"][0].get("lakehouse_id") == "itm_silver" for e in runner_events if e["event"] in ("dataset_read", "dataset_written") and e["datasets"][0]["kind"] == "table")

    # --- the control plane republishes them
    st = fleet
    seen: list[dict[str, Any]] = []
    unsub = st.ctx.events.subscribe(lambda e: seen.append(e) if e["type"].startswith("dataset.") else None)
    row = {"id": "job_spark_e2e", "workspace_id": "wks_e2e", "item_id": "itm_notebook", "name": "probe-e2e", "kind": "notebook", "engine": "spark", "attempt": 1,
           "spec": {"lakehouse_ids": ["itm_silver"], "default_lakehouse": {"id": "itm_silver"}}}
    try:
        for e in runner_events:
            if e["event"] in ("dataset_read", "dataset_written", "dataset_statement"):
                assert st.compute.jobs.forward_dataset_event(row, e["event"], {k: v for k, v in e.items() if k != "event"})
    finally:
        unsub()
    types = [e["type"] for e in seen]
    assert types.count("dataset.statement") == 2 and types.count("dataset.read") == 2 and types.count("dataset.written") == 3
    assert all(e["workspace_id"] == "wks_e2e" and e["subject"] == "job_spark_e2e" and e["payload"]["process"]["item_id"] == "itm_notebook" for e in seen)
    assert not [e for e in st.ctx.events.list(type_prefix="dataset.", limit=100) if e["type"] == "dataset.statement" and e["payload"].get("job_id") == "job_spark_e2e"]

    # --- and Intelligence's pure translation accepts every one of them
    for e in seen:
        if e["type"] == "dataset.statement":
            continue                   # the analyzer is exercised by tests/intelligence
        facts = intel_runtime.facts_from_event(e["type"], e["payload"])
        assert facts is not None and facts.edges and all(x.confidence == "verified" for x in facts.edges)
        assert facts.edges[0].src.type == "notebook"
    assert dsev.statement_hash(stmts[0])             # the same hash function Intelligence uses for ad-hoc statements


SESSION_PROGRAM = """
import json, os, sys
spec = json.loads(sys.argv[1])
from olcf.runtime_lib import spark as rl_spark          # the production session factory: it starts the probe after the lakehouse set-up
spark = rl_spark.create_session(spec)
spark.range(3).write.mode("overwrite").saveAsTable("fresh")                        # the default lakehouse database is active: Silver.fresh
spark.range(3).write.mode("overwrite").parquet("Files/out/b")                      # relative path: resolved against the lakehouse root by the session
spark.read.parquet("Files/out/b").count()
spark.sql("SELECT count(*) FROM fresh").collect()
spark.stop()
os._exit(0)
"""


def test_the_production_session_factory_starts_the_probe_after_its_own_setup(fleet, runtime_python, tmp_path):
    root = tmp_path / "lhs"
    (root / "Tables").mkdir(parents=True)
    (root / "Files").mkdir()
    spec = {"name": "probe-session", "job_id": "job_sess", "spark": {"master": "local[1]", "conf": {"spark.ui.enabled": "false", "spark.driver.bindAddress": "127.0.0.1",
                                                                                                     "spark.sql.shuffle.partitions": "1"}},
            "default_lakehouse": {"id": "itm_silver", "name": "Silver", "root_uri": root.as_uri(), "tables_uri": (root / "Tables").as_uri(), "format": "parquet"},
            "lakehouses": [], "lakehouse_ids": ["itm_silver"]}
    env = {k: v for k, v in os.environ.items() if k not in ("PYTHONPATH", "OLCF_DATASET_EVENTS")}
    env["PYTHONPATH"] = str(ROOT / "src")
    with spark_lock_held():
        proc = subprocess.run([str(runtime_python), "-c", textwrap.dedent(SESSION_PROGRAM), json.dumps(spec)], capture_output=True, text=True, timeout=420, env=env,  # noqa: S603
                              cwd=str(tmp_path), check=False)
    assert proc.returncode == 0, proc.stdout[-2000:] + proc.stderr[-4000:]
    events = [json.loads(line[len("@@OLCF@@ "):]) for line in proc.stdout.splitlines() if line.startswith("@@OLCF@@ ")]
    names = [e["event"] for e in events]
    assert "spark_app_started" in names and "lakehouse_ready" in names
    lineage = [e for e in events if e["event"].startswith("dataset_")]
    assert [e["event"] for e in lineage] == ["dataset_written", "dataset_written", "dataset_read", "dataset_statement"], lineage
    assert lineage[0]["datasets"] == [{"kind": "table", "lakehouse_id": "itm_silver", "name": "fresh"}] and lineage[0]["mode"] == "overwrite"
    assert lineage[1]["datasets"][0]["path"] == "Files/out/b" and lineage[1]["datasets"][0]["lakehouse_id"] == "itm_silver" and lineage[1]["datasets"][0]["format"] == "parquet"
    assert lineage[2]["datasets"][0]["path"] == "Files/out/b"
    assert lineage[3]["sql"] == "SELECT count(*) FROM fresh" and lineage[3]["default_lakehouse_id"] == "itm_silver"
    assert names.index("lakehouse_ready") < names.index("dataset_written"), "the session's own CREATE DATABASE / USE statements are not user lineage"
