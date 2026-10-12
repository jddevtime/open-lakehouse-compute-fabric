# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Topology document, node/fleet tests, Spark smoke test, health checks and published events (CONTRACTS 4.6)."""

from __future__ import annotations

import time

from tests.compute.fake_agent import JobPlan, wait_until


def _topo(st) -> dict:
    r = st.http.get("/v1/compute/topology", headers=st.admin)
    assert r.status_code == 200, r.text
    return r.json()


def _events(st, kind: str) -> list[dict]:
    return st.http.get("/v1/events", headers=st.admin, params={"type": kind, "limit": 200}).json()["items"]


def _check(st, name: str, deep: bool = False) -> dict:
    if deep:
        r = st.http.post(f"/v1/admin/health/checks/{name}/test", headers=st.admin)
        assert r.status_code == 200, r.text
        return r.json()
    snap = st.http.get("/v1/admin/health", headers=st.admin, params={"force": True}).json()
    comps = snap.get("components") or snap.get("items") or []
    return next(c for c in comps if c["name"] == name)


def test_topology_document_is_complete_for_an_empty_fleet(fleet):
    st = fleet
    t = _topo(st)
    for key in ("generated_at", "summary", "control_plane", "clusters", "nodes", "queues"):
        assert key in t
    for key in ("nodes_total", "nodes_online", "cores_total", "cores_used", "memory_total_mb", "memory_used_mb", "jobs_running", "jobs_queued", "executors"):
        assert key in t["summary"]
    assert t["summary"]["nodes_online"] == 0 and t["summary"]["cores_total"] == 0
    cp = t["control_plane"]
    assert cp["nodes"] and cp["nodes"][0]["leader"] is True and cp["nodes"][0]["node_id"]
    assert {c["name"] for c in cp["components"]} >= {"scheduler", "worker-fleet", "runtimes"}
    assert any(q["name"] == "default" for q in t["queues"])
    assert st.http.get("/v1/compute/topology").status_code == 401


def test_topology_with_nodes_jobs_and_metric_history(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("topo-a", cores=8, memory_mb=16384, labels={"site": "dc1"})
    b = st.agent("topo-b", cores=4, memory_mb=8192)
    for x in (a, b):
        x.job_plan = lambda spec: JobPlan(outcome="hang")
        st.wait_worker(x)
    job = st.submit(ws, resources={"driver_cores": 2, "driver_memory_mb": 2048})
    st.wait_job(job["id"], "running")
    wait_until(lambda: len(_topo(st)["nodes"][0]["history"]["cpu"]) >= 3, 20, 0.5, "metric history samples")
    t = _topo(st)
    assert t["summary"]["nodes_online"] == 2 and t["summary"]["cores_total"] == 12 and t["summary"]["jobs_running"] == 1
    assert t["summary"]["cores_used"] >= 2
    nodes = {n["name"]: n for n in t["nodes"] if n["name"] in ("topo-a", "topo-b")}
    na = nodes["topo-a"]
    assert na["state"] == "online" and na["scheduling"] == "active" and na["pool"] == "default" and na["labels"]["site"] == "dc1"
    assert na["resources"]["cores"] == 8 and na["resources"]["memory_mb"] == 16384
    assert na["metrics"]["cpu_percent"] == 12.5 and 25 < na["metrics"]["mem_percent"] < 35
    assert len(na["history"]["cpu"]) == len(na["history"]["mem"]) == len(na["history"]["ts"]) <= 120
    assert {c["name"] for c in na["components"]} >= {"agent", "control-plane-link"}
    assert {r["id"] for r in na["runtimes"]} == {"olcf-rt-2.0.0"} and na["runtimes"][0]["verified"] is True
    running_on = [n for n in nodes.values() if n["running_jobs"]]
    assert len(running_on) == 1 and running_on[0]["running_jobs"][0]["job_id"] == job["id"]
    assert running_on[0]["resources"]["cores_allocated"] >= 2
    a.finish_hung(job["id"], "succeed")
    b.finish_hung(job["id"], "succeed") if job["id"] in b.jobs else None
    st.wait_job(job["id"])


def test_history_ring_buffer_is_capped_at_120_samples(fleet):
    st = fleet
    a = st.agent("topo-ring", heartbeat=0.05)
    st.wait_worker(a)
    wait_until(lambda: len(st.worker(a.worker_id)["history"].get("cpu", [])) >= 100, 30, 0.5, "100 samples")
    time.sleep(8)  # > 120 heartbeats in total
    h = st.worker(a.worker_id)["history"]
    assert len(h["cpu"]) <= 120 and len(h["mem"]) <= 120 and len(h["ts"]) <= 120


def test_worker_test_probe_suite_runs_on_the_node(fleet):
    st = fleet
    a = st.agent("probe-a")
    st.wait_worker(a)
    r = st.http.post(f"/v1/workers/{a.worker_id}/test", headers=st.admin)
    assert r.status_code == 200, r.text
    d = r.json()
    assert d["ok"] is True
    names = [s["name"] for s in d["steps"]]
    assert names[0] == "agent-link" and {"dns:control-plane", "tcp:control-plane", "http:control-plane", "disk", "clock", "runtime:olcf-rt-2.0.0"} <= set(names)
    assert all({"name", "ok", "detail", "ms"} <= set(s) for s in d["steps"])
    probe = a.commands_of("probe")[-1]["payload"]["checks"]
    assert any(c["kind"] == "tcp" and c["port"] == st.settings.server.port for c in probe)
    clock = next(c for c in probe if c["kind"] == "clock")
    assert clock["server_time"] and clock["max_skew_seconds"] > 0
    a.probe_fail.add("clock")
    d = st.http.post(f"/v1/workers/{a.worker_id}/test", headers=st.admin).json()
    assert d["ok"] is False and [s["name"] for s in d["steps"] if not s["ok"]] == ["clock"]
    assert st.worker(a.worker_id)["notes"]["last_test"]["failed"] == ["clock"]
    # an unreachable agent fails at the first step, with a remediation
    a.drop()
    st.wait_worker(a, "offline", timeout=30)
    d = st.http.post(f"/v1/workers/{a.worker_id}/test", headers=st.admin).json()
    assert d["ok"] is False and d["steps"][0]["name"] == "agent-link" and not d["steps"][0]["ok"]
    assert st.http.post("/v1/workers/wrk_nope/test", headers=st.admin).status_code == 404


def test_compute_test_returns_a_matrix(fleet):
    st = fleet
    a, b = st.agent("mx-a"), st.agent("mx-b")
    b.probe_fail.add("disk")
    for x in (a, b):
        st.wait_worker(x)
    d = st.http.post("/v1/compute/test", headers=st.admin).json()
    assert d["ok"] is False
    assert {"mx-a", "mx-b"} <= set(d["nodes"])
    cols = d["matrix"]["checks"]
    assert "disk" in cols and "agent-link" in cols
    rows = {r["node"]: r for r in d["matrix"]["rows"]}
    assert rows["mx-a"]["ok"] is True and rows["mx-b"]["ok"] is False
    assert rows["mx-b"]["results"]["disk"]["ok"] is False and rows["mx-a"]["results"]["disk"]["ok"] is True
    ws = st.workspace()
    assert st.http.post("/v1/compute/test", headers=st.user("Viewer", ws)).status_code == 403


def _smoke(st, body=None, headers=None, wait=60):
    """Start the (asynchronous) smoke test and poll it until it is finished: returns (POST response, final report)."""
    r = st.http.post("/v1/compute/spark-smoke", headers=headers or st.admin, json=body or {"timeout_seconds": 60})
    assert r.status_code == 202, r.text
    doc = r.json()
    final = wait_until(lambda: (d := st.http.get(doc["poll_url"], headers=headers or st.admin).json())["finished"] and d, wait, 0.2, "the smoke test to finish")
    return r, final


def test_spark_smoke_reports_the_nodes_that_hosted_executors(fleet):
    st = fleet
    a, b = st.agent("smoke-a"), st.agent("smoke-b")
    plan = JobPlan(spark_app=True, duration=0.3, events=[
        ("executor_hosts", {"hosts": [{"host": "smoke-a.test", "tasks": 4}, {"host": "smoke-b.test", "tasks": 4}]}),
        ("smoke_result", {"sum": 499500, "tasks": 8, "master": "spark://127.0.0.1:7077", "applicationId": "app-smoke-0001"})])
    for x in (a, b):
        x.job_plan = lambda spec, plan=plan: plan
        st.wait_worker(x)
    r, d = _smoke(st)
    assert d["ok"] is True and d["state"] == "succeeded" and d["tasks"] == 8 and d["sum"] == 499500
    assert d["distributed"] is True and {n["name"] for n in d["nodes_with_executors"]} == {"smoke-a", "smoke-b"}
    assert d["id"] == d["job_id"] == r.json()["id"]
    specs = [s for x in (a, b) for s in x.run_job_specs()]
    assert len(specs) == 1 and specs[0]["workspace"]["name"] == "system" and specs[0]["spark"]["master"].startswith("spark://")
    ws = st.workspace()
    assert st.http.post("/v1/compute/spark-smoke", headers=st.user("Contributor", ws)).status_code == 403


def test_spark_smoke_failure_is_reported(fleet):
    st = fleet
    a = st.agent("smokef-a")
    a.job_plan = lambda spec: JobPlan(outcome="fail", failure={"category": "engine", "message": "no executors"})
    st.wait_worker(a)
    _r, d = _smoke(st, {"timeout_seconds": 30})
    assert d["ok"] is False and d["state"] == "failed" and d["failure"]["message"] == "no executors"
    assert d["distributed"] is False and d["note"]


def test_health_checks_scheduler_fleet_runtimes_and_spark_master(fleet):
    st = fleet
    ws = st.workspace()
    names = {c["name"] for c in st.http.get("/v1/admin/health/checks", headers=st.admin).json()["items"]}
    assert {"scheduler", "worker-fleet", "runtimes"} <= names
    assert _check(st, "worker-fleet")["status"] == "degraded"  # nothing enrolled
    assert _check(st, "scheduler")["status"] == "ok"
    a, b = st.agent("hc-a"), st.agent("hc-b")
    for x in (a, b):
        x.job_plan = lambda spec: JobPlan(duration=0.2)
        st.wait_worker(x)
    wait_until(lambda: _check(st, "worker-fleet")["status"] == "ok", 15, 0.5, "healthy fleet")
    assert _check(st, "runtimes")["status"] == "ok"
    deep = _check(st, "worker-fleet", deep=True)
    assert deep["status"] == "ok" and deep["steps"] and all(s["ok"] for s in deep["steps"])
    sched_deep = _check(st, "scheduler", deep=True)
    assert [s["step"] for s in sched_deep["steps"]] == ["read-fleet-state", "dry-run-placement"] and sched_deep["status"] == "ok"
    # a Spark cluster adds a spark-master:<runtime> check
    st.wait_job(st.submit(ws, engine="spark")["id"], timeout=60)
    st.compute.topology_service.sync_runtime_checks()
    names = {c["name"] for c in st.http.get("/v1/admin/health/checks", headers=st.admin).json()["items"]}
    assert "spark-master:olcf-rt-2.0.0" in names
    quick = _check(st, "spark-master:olcf-rt-2.0.0")
    assert quick["status"] == "ok", quick
    deep = _check(st, "spark-master:olcf-rt-2.0.0", deep=True)
    assert deep["status"] == "ok" and deep["steps"]
    # one node going away degrades the fleet; none left takes it down
    a.drop()
    st.wait_worker(a, "offline", timeout=30)
    assert _check(st, "worker-fleet")["status"] == "degraded"
    b.drop()
    st.wait_worker(b, "offline", timeout=30)
    assert _check(st, "worker-fleet")["status"] == "down"
    assert _check(st, "runtimes")["status"] in ("unknown", "degraded", "down")


def test_state_events_are_published(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("ev-a")
    a.job_plan = lambda spec: JobPlan(duration=0.2)
    st.wait_worker(a)
    job = st.submit(ws)
    st.wait_job(job["id"])
    jev = [e for e in _events(st, "job.state_changed") if e["subject"] == job["id"]]
    path = [e["payload"]["to"] for e in sorted(jev, key=lambda e: e["seq"])]
    assert path[0] == "accepted" and path[-1] == "succeeded" and "running" in path
    assert all(e["workspace_id"] == ws for e in jev)
    kinds = {e["type"] for e in _events(st, "worker.")}
    assert {"worker.enrolled", "worker.online"} <= kinds
    sid = st.http.post("/v1/sessions", headers=st.admin, json={"workspace_id": ws, "engine": "python"}).json()["id"]
    wait_until(lambda: st.http.get(f"/v1/sessions/{sid}", headers=st.admin).json()["state"] == "idle", 15, message="idle")
    st.http.delete(f"/v1/sessions/{sid}", headers=st.admin)
    sk = {e["type"] for e in _events(st, "session.") if e["subject"] == sid}
    assert {"session.started", "session.ended"} <= sk or {"session.started"} <= sk, sk
    # a viewer of the workspace sees the workspace's job events, but nobody else's
    viewer = st.user("Viewer", ws)
    ev = st.http.get("/v1/events", headers=viewer, params={"type": "job.state_changed", "limit": 200}).json()["items"]
    assert ev and all(e["workspace_id"] == ws for e in ev)
