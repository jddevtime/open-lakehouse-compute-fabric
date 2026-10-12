# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Spark on the Compute page: the smoke test dialog and a Spark Standalone cluster with one worker on every node (JVMs, serialised through the sandbox lock).

The sandbox agents are three processes on one 2-core VM, so the machines share an address and host name; "spread over three nodes" is therefore shown by the cluster's
registered workers (one per agent) and by an application that really ran against ``spark://master``, not by executor host names.
"""

from __future__ import annotations

import re
import time

import pytest
from playwright.sync_api import expect

pytestmark = [pytest.mark.spark, pytest.mark.usefixtures("spark_exclusive")]

SCRIPT = """import json, os, time
def where(i):
    time.sleep(1.0)
    return os.getpid()
sc = spark.sparkContext
pids = sorted(set(sc.parallelize(range(8), 8).map(where).collect()))
print('E2E-MASTER', sc.master)
print('E2E-EXECUTOR-PROCESSES', len(pids))
"""


def test_smoke_test_dialog(page, goto, api, has_runtime, snap):
    if not has_runtime:
        pytest.skip("no cached Spark runtime")
    goto(page, "/compute")
    page.get_by_role("tab", name=re.compile("^Spark clusters")).click()
    page.get_by_role("button", name="Spark smoke test").click()
    dlg = page.get_by_role("dialog")
    expect(dlg).to_contain_text("Spark smoke test")
    dlg.get_by_label("Executors").fill("2")
    dlg.get_by_role("button", name="Run smoke test").click()
    banner = dlg.locator(".banner").first
    expect(banner).to_contain_text(re.compile("Spark smoke test (passed|failed|timed_out|cancelled)"), timeout=280_000)
    expect(banner).to_contain_text("Spark smoke test passed")
    job_id = dlg.get_by_role("link", name=re.compile(r"^job_")).inner_text()
    job = api("GET", f"/v1/jobs/{job_id}")
    assert job["state"] == "succeeded"
    expect(dlg).to_contain_text(re.compile(r"8 tasks, sum 499500"))
    snap(page, "compute-spark-smoke")
    page.keyboard.press("Escape")


def test_standalone_cluster_has_a_worker_on_every_node(page, goto, api, has_runtime, snap):
    if not has_runtime:
        pytest.skip("no cached Spark runtime")
    ws = api("POST", "/v1/workspaces", json={"name": "spark-e2e-" + str(int(time.time()))})["id"]
    job = api("POST", "/v1/jobs", json={"kind": "spark_job", "workspace_id": ws, "name": "e2e-distributed", "source": {"format": "py", "content": SCRIPT, "filename": "e2e.py"},
                                          "resources": {"driver_cores": 1, "driver_memory_mb": 512, "executor_cores": 1, "executor_memory_mb": 512, "min_executors": 2, "max_executors": 2},
                                          "policy": {"timeout_seconds": 240}})
    end = time.time() + 240
    rec = job
    while time.time() < end:
        rec = api("GET", f"/v1/jobs/{job['id']}")
        if rec["state"] in ("succeeded", "failed", "cancelled", "timed_out"):
            break
        time.sleep(2)
    assert rec["state"] == "succeeded", rec.get("failure")
    assert rec["provider"] == "standalone"
    # the job page shows the log of the real application
    goto(page, f"/jobs/{job['id']}")
    expect(page.get_by_text(re.compile(r"E2E-MASTER spark://"))).to_be_visible(timeout=30_000)
    # the cluster on the Compute page: three registered workers, one per node
    goto(page, "/compute")
    page.get_by_role("tab", name=re.compile("^Spark clusters")).click()
    card = page.locator("article, .panel, .card").filter(has_text="Spark workers on").first
    expect(card).to_be_visible()
    for n in ("node-1", "node-2", "node-3"):
        expect(card.get_by_text(n, exact=True)).to_be_visible()
    expect(card).to_contain_text(re.compile(r"spark://"))
    expect(card.get_by_text("Up", exact=True)).to_be_visible()
    topo = api("GET", "/v1/compute/topology")
    cl = next(c for c in topo["clusters"] if c["state"] == "up")
    assert cl["master"]["alive_workers"] >= 3 and len(cl["workers"]) >= 3
    snap(page, "compute-spark-cluster")
