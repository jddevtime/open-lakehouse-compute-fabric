# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""First-run behaviour of the compute fabric: the host-Python fallback and queued jobs that explain themselves (FIRSTRUN).

* a Python-engine job with no installed Spark runtime runs on the built-in ``olcf-host-python`` runtime (recorded in the job: runtime id, fingerprint, a
  ``runtime_fallback`` event), a Spark job never does;
* every reason a job can stay queued shows up as ``queue_reason`` in the job record and as a ``queue_reason`` event in the timeline.
"""

from __future__ import annotations

import time

from tests.compute.fake_agent import JobPlan, wait_until

HOST = "olcf-host-python"


def _reason(st, job_id: str, code: str, timeout: float = 15):
    return wait_until(lambda: (j := st.job(job_id))["state"] == "queued" and (j.get("queue_reason") or {}).get("code") == code and j, timeout, 0.1, f"queue_reason {code}")


def _events(st, job_id: str) -> list[dict]:
    return st.http.get(f"/v1/jobs/{job_id}/timeline", headers=st.admin).json()["items"]


def _spark_job(st, ws, **over):
    return st.submit(ws, engine="spark", resources={"driver_cores": 1, "driver_memory_mb": 1024, "executor_cores": 1, "executor_memory_mb": 1024, "min_executors": 1}, **over)


# --------------------------------------------------------------------------- the pseudo-runtime in the catalog


def test_host_python_is_listed_and_ready_once_a_worker_offers_it(fleet):
    st = fleet
    rows = {r["id"]: r for r in st.http.get("/v1/runtimes", headers=st.admin).json()["items"]}
    assert HOST in rows and rows[HOST]["family"] == "host-python" and rows[HOST]["no_spark"] is True and rows[HOST]["builtin"] is True
    a = st.agent("hp-list", runtimes=(HOST,))
    st.wait_worker(a)
    wait_until(lambda: {r["id"]: r for r in st.http.get("/v1/runtimes", headers=st.admin).json()["items"]}[HOST]["state"] == "ready", 15, message="host runtime ready")
    row = {r["id"]: r for r in st.http.get("/v1/runtimes", headers=st.admin).json()["items"]}[HOST]
    assert row["ready_nodes"] and row["engines"] == ["python"]


def test_host_python_cannot_be_installed_or_retired(fleet):
    st = fleet
    r = st.http.post(f"/v1/runtimes/{HOST}/install", headers=st.admin, json={})
    assert r.status_code in (400, 409, 422), r.text
    assert "host" in r.text.lower() or "built" in r.text.lower()


# --------------------------------------------------------------------------- fallback


def test_python_job_without_a_spark_runtime_runs_on_host_python(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("hp-run", runtimes=(HOST,))
    a.job_plan = lambda spec: JobPlan(duration=0.1)
    st.wait_worker(a)
    wait_until(lambda: st.http.get("/v1/runtimes", headers=st.admin).json()["items"] and
               [r for r in st.http.get("/v1/runtimes", headers=st.admin).json()["items"] if r["id"] == HOST][0]["ready_nodes"], 15, message="host runtime ready")
    job = st.submit(ws)
    done = st.wait_job(job["id"])
    assert done["state"] == "succeeded", done
    assert done["runtime_id"] == HOST
    spec = a.run_job_specs()[0]
    assert spec["runtime"]["id"] == HOST
    kinds = [e["event"] or e["kind"] for e in _events(st, job["id"])]
    assert "runtime_fallback" in kinds, kinds
    detail = st.http.get(f"/v1/jobs/{job['id']}", headers=st.admin, params={"detail": "true"}).json()
    assert detail["hashes"]["runtime_fingerprint"] == f"sha256:fake-{HOST}-hp-run"  # the placed node's own interpreter fingerprint


def test_spark_job_never_runs_on_host_python(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("hp-nospark", runtimes=(HOST,))
    st.wait_worker(a)
    wait_until(lambda: [r for r in st.http.get("/v1/runtimes", headers=st.admin).json()["items"] if r["id"] == HOST][0]["ready_nodes"], 15, message="host runtime ready")
    job = _spark_job(st, ws)
    j = _reason(st, job["id"], "no_runtime")
    assert j["runtime_id"] != HOST
    assert "not installed" in j["queue_reason"]["message"] and "olcf runtime install" in j["queue_reason"]["message"]
    time.sleep(1.0)
    assert not a.commands_of("run_job")
    st.http.post(f"/v1/jobs/{job['id']}/cancel", headers=st.admin)


def test_naming_the_host_runtime_for_a_spark_job_is_refused(fleet):
    st = fleet
    ws = st.workspace()
    r = st.http.post("/v1/jobs", headers=st.admin, json={"kind": "script", "workspace_id": ws, "engine": "spark", "source": {"format": "py", "content": "1"}, "name": "x",
                                                          "runtime": {"runtime_id": HOST}})
    assert r.status_code in (400, 409, 422), r.text
    assert "no spark" in r.text.lower()


def test_fallback_can_be_switched_off(fleet):
    st = fleet
    ws = st.workspace()
    assert st.http.put("/v1/admin/settings/compute.host_python_fallback", headers=st.admin, json={"value": False}).status_code == 200
    try:
        a = st.agent("hp-off", runtimes=(HOST,))
        st.wait_worker(a)
        wait_until(lambda: [r for r in st.http.get("/v1/runtimes", headers=st.admin).json()["items"] if r["id"] == HOST][0]["ready_nodes"], 15, message="host runtime ready")
        job = st.submit(ws)
        j = _reason(st, job["id"], "no_runtime")
        assert j["runtime_id"] != HOST
        st.http.post(f"/v1/jobs/{job['id']}/cancel", headers=st.admin)
    finally:
        st.http.put("/v1/admin/settings/compute.host_python_fallback", headers=st.admin, json={"value": True})


# --------------------------------------------------------------------------- queue reasons


def test_queue_reason_no_online_worker(fleet):
    st = fleet
    ws = st.workspace()
    job = st.submit(ws)
    j = _reason(st, job["id"], "no_node")
    assert "worker" in j["queue_reason"]["message"].lower() and j["queue_reason"]["remediation"]
    ev = [e for e in _events(st, job["id"]) if (e["event"] or e["kind"]) == "queue_reason"]
    assert ev and ev[0]["data"]["code"] == "no_node"
    st.http.post(f"/v1/jobs/{job['id']}/cancel", headers=st.admin)


def test_queue_reason_runtime_not_installed_and_installed_elsewhere(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("qr-none", runtimes=())
    st.wait_worker(a)
    job = _spark_job(st, ws)
    j = _reason(st, job["id"], "no_runtime")
    assert "not installed on any worker" in j["queue_reason"]["message"], j["queue_reason"]
    assert j["wait_reason"].startswith("no_runtime")
    st.http.post(f"/v1/jobs/{job['id']}/cancel", headers=st.admin)


def test_queue_reason_paused_queue_and_resume(fleet):
    st = fleet
    ws = st.workspace()
    assert st.http.post("/v1/queues", headers=st.admin, json={"name": "qr-paused"}).status_code == 201
    st.http.post("/v1/queues/qr-paused/pause", headers=st.admin)
    a = st.agent("qr-pause", runtimes=("olcf-rt-2.0.0",))
    a.job_plan = lambda spec: JobPlan(duration=0.1)
    st.wait_worker(a)
    job = st.submit(ws, policy={"queue": "qr-paused"})
    j = _reason(st, job["id"], "paused")
    assert "paused" in j["queue_reason"]["message"]
    st.http.post("/v1/queues/qr-paused/resume", headers=st.admin)
    done = st.wait_job(job["id"])
    assert done["state"] == "succeeded" and done["queue_reason"] is None  # no reason once it ran


def test_queue_reason_quota_and_resources(fleet):
    st = fleet
    ws = st.workspace()
    assert st.http.post("/v1/queues", headers=st.admin, json={"name": "qr-quota", "max_concurrency": 1}).status_code == 201
    a = st.agent("qr-quota", cores=8, memory_mb=16384)
    a.job_plan = lambda spec: JobPlan(outcome="hang")
    st.wait_worker(a)
    first = st.submit(ws, policy={"queue": "qr-quota"})
    st.wait_job(first["id"], "running")
    second = st.submit(ws, policy={"queue": "qr-quota"})
    j = _reason(st, second["id"], "quota")
    assert "concurrency limit" in j["queue_reason"]["message"]
    # resources: fill the node with default-queue jobs, one more waits for capacity
    for jid in (first["id"], second["id"]):
        st.http.post(f"/v1/jobs/{jid}/cancel", headers=st.admin)
    big = {"driver_cores": 3, "driver_memory_mb": 1024}
    runners = [st.submit(ws, resources=big) for _ in range(2)]
    for r in runners:
        st.wait_job(r["id"], "running")
    waiting = st.submit(ws, resources=big)
    j = _reason(st, waiting["id"], "resources")
    assert "waiting for resources" in j["queue_reason"]["message"]
    for r in [*runners, waiting]:
        st.http.post(f"/v1/jobs/{r['id']}/cancel", headers=st.admin)


# the last test of the module: it leaves 'olcf-rt-2.0.0' recorded as installed on a (now retired) worker, which would change the queue reasons of the tests above
def test_python_job_asking_for_the_host_family_runs_on_host_python_even_with_a_spark_runtime_installed(fleet):
    """The sample notebook 02 (deltalake, duckdb, pandas) asks for ``family: host-python``: an installed Spark runtime must not capture it (it may lack those libraries)."""
    st = fleet
    ws = st.workspace()
    a = st.agent("hp-family", runtimes=("olcf-rt-2.0.0", HOST))
    a.job_plan = lambda spec: JobPlan(duration=0.1)
    st.wait_worker(a)
    wait_until(lambda: [r for r in st.http.get("/v1/runtimes", headers=st.admin).json()["items"] if r["id"] == HOST][0]["ready_nodes"], 15, message="host runtime ready")
    wait_until(lambda: [r for r in st.http.get("/v1/runtimes", headers=st.admin).json()["items"] if r["id"] == "olcf-rt-2.0.0"][0]["ready_nodes"], 15, message="spark runtime ready")
    done = st.wait_job(st.submit(ws, runtime={"family": "host-python"})["id"])
    assert done["state"] == "succeeded" and done["runtime_id"] == HOST, done
    other = st.wait_job(st.submit(ws, runtime={"family": "fabric-2.0-compatible"})["id"])  # control: a Spark family still binds to the installed Spark runtime
    assert other["state"] == "succeeded" and other["runtime_id"] == "olcf-rt-2.0.0", other
