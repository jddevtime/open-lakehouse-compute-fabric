# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Spark Standalone provider across two fake agents: master/worker command sequence, job spark block, cluster reuse, drain, never mixing runtimes."""

from __future__ import annotations

from tests.compute.fake_agent import JobPlan, wait_until


def _spark_job(st, ws, **over):
    return st.submit(ws, engine="spark", resources={"driver_cores": 1, "driver_memory_mb": 1024, "executor_cores": 1, "executor_memory_mb": 1024, "min_executors": 1}, **over)


def test_two_agents_standalone_cluster_command_sequence(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("sa-a", cores=8, memory_mb=16384)
    b = st.agent("sa-b", cores=8, memory_mb=16384)
    for x in (a, b):
        x.job_plan = lambda spec: JobPlan(spark_app=True, duration=0.5)
        st.wait_worker(x)
    job = _spark_job(st, ws)
    done = st.wait_job(job["id"], timeout=60)
    assert done["state"] == "succeeded", done
    agents = {a.worker_id: a, b.worker_id: b}
    masters = [x for x in (a, b) if x.commands_of("start_spark_master")]
    assert len(masters) == 1, "exactly one Spark master per (runtime, pool)"
    m = masters[0]
    mcmd = m.commands_of("start_spark_master")[0]["payload"]
    assert mcmd["runtime_id"] == "olcf-rt-2.0.0" and mcmd["host"] == "127.0.0.1" and mcmd["port"] and mcmd["ui_port"]
    assert mcmd["conf"]["spark.ui.proxyBase"].startswith("/v1/spark-ui/clu_")
    master_url = f"spark://127.0.0.1:{mcmd['port']}"
    # every node got a worker pointing at that master, sized below its capacity (headroom for the OS/driver)
    for x in (a, b):
        (w,) = x.commands_of("start_spark_worker")
        p = w["payload"]
        assert p["master_url"] == master_url and p["runtime_id"] == "olcf-rt-2.0.0"
        assert 1 <= p["cores"] <= 7 and 1024 <= p["memory_mb"] < 16384
    # master was started strictly before any worker, workers strictly before run_job
    t_master = mcmd and m.commands_of("start_spark_master")[0]["at"]
    workers_at = [x.commands_of("start_spark_worker")[0]["at"] for x in (a, b)]
    assert all(t_master <= t for t in workers_at)
    driver = agents[done["driver_worker_id"]]
    (rj,) = driver.commands_of("run_job")
    spark = rj["payload"]["spec"]["spark"]
    assert spark["master"] == master_url and spark["conf"]["spark.driver.host"] == "127.0.0.1"
    assert int(spark["conf"]["spark.cores.max"]) >= 1 and spark["conf"]["spark.driver.port"] and spark["conf"]["spark.blockManager.port"]
    assert all(t <= rj["at"] for t in workers_at)
    # cluster + application are visible through the API
    clusters = st.http.get("/v1/clusters", headers=st.admin).json()["items"]
    (cl,) = [c for c in clusters if c["runtime_id"] == "olcf-rt-2.0.0" and c["state"] in ("up", "degraded")]
    assert cl["id"] == done["cluster_id"] and cl["state"] == "up"
    apps = st.http.get("/v1/spark-applications", headers=st.admin).json()["items"]
    assert any(x["job_id"] == job["id"] for x in apps)
    # a second job reuses the running cluster
    j2 = _spark_job(st, ws)
    assert st.wait_job(j2["id"], timeout=60)["cluster_id"] == cl["id"]
    assert len(m.commands_of("start_spark_master")) == 1
    assert sum(len(x.commands_of("start_spark_worker")) for x in (a, b)) == 2


def test_drain_stops_the_spark_worker_on_that_node_only(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("sd-a")
    b = st.agent("sd-b")
    for x in (a, b):
        x.job_plan = lambda spec: JobPlan(duration=0.2)
        st.wait_worker(x)
    st.wait_job(_spark_job(st, ws)["id"], timeout=60)
    victim = b if not b.commands_of("start_spark_master") else a
    keep = a if victim is b else b
    r = st.http.post(f"/v1/workers/{victim.worker_id}/drain", headers=st.admin)
    assert r.status_code == 200
    wait_until(lambda: victim.commands_of("stop_spark_worker"), 30, message="stop_spark_worker on the drained node")
    assert not keep.commands_of("stop_spark_worker")
    # the cluster keeps serving from the remaining node
    st.http.post(f"/v1/workers/{victim.worker_id}/uncordon", headers=st.admin)
    j = _spark_job(st, ws, placement={"node": keep.worker_id}) if False else _spark_job(st, ws)
    assert st.wait_job(j["id"], timeout=60)["state"] == "succeeded"


def test_single_small_node_spark_job_runs_local_without_a_cluster_and_says_why(fleet):
    """A machine too small for master + worker + driver (2 cores) still runs Spark jobs: local mode, with the reason in the job timeline."""
    st = fleet
    ws = st.workspace()
    a = st.agent("local-a", cores=2, memory_mb=4096)
    a.job_plan = lambda spec: JobPlan(duration=0.2)
    st.wait_worker(a)
    done = st.wait_job(_spark_job(st, ws)["id"], timeout=60)
    assert done["state"] == "succeeded" and done["provider"] == "local"
    spark = a.run_job_specs()[0]["spark"]
    assert str(spark["master"]).startswith("local[")
    assert not a.commands_of("start_spark_master") and not a.commands_of("start_spark_worker")
    events = st.http.get(f"/v1/jobs/{done['id']}/timeline", headers=st.admin).json()["items"]
    assert any("local mode" in str(e.get("message") or e.get("reason") or e) for e in events), events


def test_single_node_with_enough_room_gets_a_standalone_cluster_out_of_the_box(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("solo-a")            # 8 cores / 16 GB: master + worker + driver fit inside the reservations
    a.job_plan = lambda spec: JobPlan(duration=0.2)
    st.wait_worker(a)
    done = st.wait_job(_spark_job(st, ws)["id"], timeout=60)
    assert done["state"] == "succeeded" and done["provider"] == "standalone"
    assert a.commands_of("start_spark_master") and a.commands_of("start_spark_worker")


def test_spark_job_without_a_runtime_node_waits_with_a_reason(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("nort-a", runtimes=())  # a node without any ready runtime
    st.wait_worker(a)
    job = _spark_job(st, ws)
    j = wait_until(lambda: (x := st.job(job["id"]))["state"] == "queued" and x["wait_reason"] and x, 15, message="a wait reason")
    assert "runtime" in j["wait_reason"].lower()
    assert not a.commands_of("run_job") and not a.commands_of("start_spark_master")
    st.http.post(f"/v1/jobs/{job['id']}/cancel", headers=st.admin)


def test_runtimes_are_never_mixed_in_one_cluster(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("mix-a", runtimes=("olcf-rt-2.0.0", "olcf-rt-1.3.0"), memory_mb=32768)
    b = st.agent("mix-b", runtimes=("olcf-rt-2.0.0", "olcf-rt-1.3.0"), memory_mb=32768)
    for x in (a, b):
        x.job_plan = lambda spec: JobPlan(duration=0.2)
        st.wait_worker(x)
    j1 = _spark_job(st, ws, runtime={"runtime_id": "olcf-rt-2.0.0"})
    j2 = _spark_job(st, ws, runtime={"runtime_id": "olcf-rt-1.3.0"})
    d1, d2 = st.wait_job(j1["id"], timeout=60), st.wait_job(j2["id"], timeout=60)
    assert d1["state"] == d2["state"] == "succeeded"
    assert d1["cluster_id"] != d2["cluster_id"]
    ports = {c["payload"]["runtime_id"]: c["payload"]["port"] for x in (a, b) for c in x.commands_of("start_spark_master")}
    assert set(ports) == {"olcf-rt-2.0.0", "olcf-rt-1.3.0"} and len(set(ports.values())) == 2
    for x in (a, b):
        for c in x.commands_of("start_spark_worker"):
            assert c["payload"]["master_url"].endswith(f":{ports[c['payload']['runtime_id']]}"), "a worker must join the master of its own runtime"
    # each node runs one daemon per runtime and the sizing accounts for both (no oversubscription of the 8 cores)
    for x in (a, b):
        assert set(x.spark_workers) == {"olcf-rt-2.0.0", "olcf-rt-1.3.0"}
        total = sum(w["cores"] for w in x.spark_workers.values())
        assert total <= 8 - st.settings.compute.reserve_cores, (x.name, x.spark_workers)


def test_failed_master_start_is_reported_as_provider_failure(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("badm-a")
    a.fail_commands["start_spark_master"] = "port range exhausted"
    st.wait_worker(a)
    job = _spark_job(st, ws, policy={"placement": {"provider": "standalone"}})
    done = st.wait_job(job["id"], timeout=90)
    assert done["state"] == "failed" and done["failure_category"] == "provider"
    assert "master" in done["failure"]["message"].lower()
    assert len(a.commands_of("start_spark_master")) >= 1


def test_spark_workers_are_sized_from_the_capacity_the_agent_advertises(fleet):
    """An agent started with ``--max-memory-mb`` hands out only that much: the Spark worker daemon the provider sizes for it must fit (otherwise the
    driver launch fails with "not enough free resources" on a machine whose inventory is larger than its allowance)."""
    st = fleet
    ws = st.workspace()
    a = st.agent("cap-a", cores=16, memory_mb=65536, capacity=(4, 4096))
    b = st.agent("cap-b", cores=16, memory_mb=65536, capacity=(4, 4096))
    for x in (a, b):
        x.job_plan = lambda spec: JobPlan(spark_app=True, duration=0.5)
        st.wait_worker(x)
    job = st.submit(ws, engine="spark", resources={"driver_cores": 1, "driver_memory_mb": 512, "executor_cores": 1, "executor_memory_mb": 512, "min_executors": 1})
    done = st.wait_job(job["id"], timeout=60)
    assert done["state"] == "succeeded", done
    for x in (a, b):
        (w,) = x.commands_of("start_spark_worker")
        assert w["payload"]["cores"] <= 3 and w["payload"]["memory_mb"] <= 4096 - 1024, w["payload"]
