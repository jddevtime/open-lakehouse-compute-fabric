# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Queued ``engine=python`` jobs follow the workers' runtime inventory (CONTRACTS addendum L4): a job bound to a Spark runtime that no online worker has is re-bound to the
host Python (``runtime_fallback`` event, ``rebound: true``) when the inventory changes or on a scheduler pass; a job that pinned a runtime id is never re-bound."""

from __future__ import annotations

import time

from tests.compute.fake_agent import JobPlan, wait_until

HOST = "olcf-host-python"
SPARK_RT = "olcf-rt-2.0.0"


def _events(st, job_id: str) -> list[dict]:
    return st.http.get(f"/v1/jobs/{job_id}/timeline", headers=st.admin).json()["items"]


def _fallbacks(st, job_id: str) -> list[dict]:
    return [e for e in _events(st, job_id) if (e["event"] or "") == "runtime_fallback"]


def _host_ready(st) -> None:
    wait_until(lambda: [r for r in st.http.get("/v1/runtimes", headers=st.admin).json()["items"] if r["id"] == HOST][0]["ready_nodes"], 15, 0.2, "the host runtime to be ready")


def test_job_queued_before_any_worker_is_rebound_when_a_host_python_worker_connects(fleet):
    st = fleet
    ws = st.workspace()
    job = st.submit(ws)  # nobody is online: bound to the default Spark runtime, waits
    assert st.job(job["id"])["runtime_id"] == SPARK_RT and not _fallbacks(st, job["id"])
    a = st.agent("rb-host", runtimes=(HOST,))
    a.job_plan = lambda spec: JobPlan(duration=0.2)
    st.wait_worker(a)
    done = st.wait_job(job["id"], timeout=40)
    assert done["state"] == "succeeded" and done["runtime_id"] == HOST, done
    fb = _fallbacks(st, job["id"])
    assert len(fb) == 1 and fb[0]["data"]["rebound"] is True and fb[0]["data"]["requested"] == SPARK_RT and fb[0]["data"]["using"] == HOST
    assert SPARK_RT in fb[0]["reason"] and "re-bound" in fb[0]["reason"]
    assert a.run_job_specs()[0]["runtime"]["id"] == HOST
    audit = st.http.get("/v1/audit", headers=st.admin, params={"action": "job.runtime_rebound", "resource_id": job["id"]}).json()["items"]
    assert audit and audit[0]["details"]["to"] == HOST


def test_job_is_rebound_when_the_only_node_with_its_runtime_disappears(fleet):
    st = fleet
    ws = st.workspace()
    spark = st.agent("rb-spark", runtimes=(SPARK_RT,))
    host = st.agent("rb-onlyhost", runtimes=(HOST,))
    host.job_plan = lambda spec: JobPlan(duration=0.2)
    for x in (spark, host):
        st.wait_worker(x)
    _host_ready(st)
    assert st.http.post(f"/v1/workers/{spark.worker_id}/cordon", headers=st.admin, json={}).status_code == 200
    job = st.submit(ws)
    time.sleep(3.5)  # several scheduler passes: the Spark runtime is still online somewhere (cordoned, but present), so the job keeps its binding
    j = st.job(job["id"])
    assert j["state"] == "queued" and j["runtime_id"] == SPARK_RT and not _fallbacks(st, job["id"]), j
    assert st.http.post(f"/v1/workers/{spark.worker_id}/revoke", headers=st.admin, json={}).status_code in (200, 204)
    done = st.wait_job(job["id"], timeout=40)
    assert done["state"] == "succeeded" and done["runtime_id"] == HOST
    fb = _fallbacks(st, job["id"])
    assert len(fb) == 1 and fb[0]["data"]["rebound"] is True and fb[0]["data"]["trigger"] in ("scheduler pass", "worker runtimes changed", "worker connected")


def test_a_pinned_runtime_is_never_rebound(fleet):
    st = fleet
    ws = st.workspace()
    host = st.agent("rb-pin-host", runtimes=(HOST,))
    st.wait_worker(host)
    _host_ready(st)
    job = st.submit(ws, runtime={"runtime_id": SPARK_RT})
    time.sleep(3.5)
    j = st.job(job["id"])
    assert j["state"] == "queued" and j["runtime_id"] == SPARK_RT and not _fallbacks(st, job["id"]), j
    # a fresh hello does not change that
    other = st.agent("rb-pin-host2", runtimes=(HOST,))
    st.wait_worker(other)
    time.sleep(2.0)
    assert st.job(job["id"])["runtime_id"] == SPARK_RT and not _fallbacks(st, job["id"])
    st.http.post(f"/v1/jobs/{job['id']}/cancel", headers=st.admin)


def test_a_spark_job_is_never_rebound_and_the_setting_switches_rebinding_off(fleet):
    st = fleet
    ws = st.workspace()
    host = st.agent("rb-off-host", runtimes=(HOST,))
    st.wait_worker(host)
    _host_ready(st)
    spark_job = st.submit(ws, engine="spark", resources={"driver_cores": 1, "driver_memory_mb": 1024, "executor_cores": 1, "executor_memory_mb": 1024, "min_executors": 1})
    assert st.http.put("/v1/admin/settings/compute.host_python_fallback", headers=st.admin, json={"value": False}).status_code == 200
    try:
        py_job = st.submit(ws)
        time.sleep(3.5)
        assert st.job(py_job["id"])["runtime_id"] == SPARK_RT and not _fallbacks(st, py_job["id"])
        assert st.job(spark_job["id"])["runtime_id"] != HOST and not _fallbacks(st, spark_job["id"])
    finally:
        assert st.http.put("/v1/admin/settings/compute.host_python_fallback", headers=st.admin, json={"value": True}).status_code == 200
    done = st.wait_job(py_job["id"], timeout=40)  # switched back on: the next pass re-binds it and it runs
    assert done["runtime_id"] == HOST and len(_fallbacks(st, py_job["id"])) == 1
    assert st.job(spark_job["id"])["runtime_id"] != HOST
    st.http.post(f"/v1/jobs/{spark_job['id']}/cancel", headers=st.admin)
