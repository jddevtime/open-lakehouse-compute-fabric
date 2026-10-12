# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""FIX regressions: ``compute.reserve_cores`` is a live setting (B) and a recovered cluster does not keep a stale "N/? workers alive" detail (C)."""

from __future__ import annotations

import time

from tests.compute.fake_agent import JobPlan


def _spark_job(st, ws):
    return st.submit(ws, engine="spark", resources={"driver_cores": 1, "driver_memory_mb": 1024, "executor_cores": 1, "executor_memory_mb": 1024, "min_executors": 1})


def test_reserve_cores_is_live_for_the_planner(fleet):
    st = fleet
    a = st.agent("rc-a", cores=8, memory_mb=16384)
    st.wait_worker(a)
    reg = st.compute.registry

    def usable() -> int:
        (v,) = [n for n in reg.node_views({}, {}) if n.name == "rc-a"]
        return v.usable_cores

    assert usable() == 8 - st.settings.compute.reserve_cores
    r = st.http.put("/v1/admin/settings/compute.reserve_cores", headers=st.admin, json={"value": 5})
    assert r.status_code == 200, r.text
    time.sleep(reg.RESERVE_CORES_CACHE_SECONDS + 0.3)  # the planner reads through a short cache
    assert usable() == 3
    # invalid values are refused by the settings validator and change nothing
    for bad in (-1, 65, "many", True, 1.5):
        assert st.http.put("/v1/admin/settings/compute.reserve_cores", headers=st.admin, json={"value": bad}).status_code == 400, bad
    # the provider that sizes Spark workers sees the same value
    prov = st.compute.providers.get("standalone")
    assert prov._nodeview(reg.node_infos()[0]).reserve_cores == 5
    st.http.put("/v1/admin/settings/compute.reserve_cores", headers=st.admin, json={"value": st.settings.compute.reserve_cores})
    time.sleep(reg.RESERVE_CORES_CACHE_SECONDS + 0.3)
    assert usable() == 8 - st.settings.compute.reserve_cores


def test_cluster_detail_is_cleared_when_the_workers_have_registered(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("cd-a", cores=8, memory_mb=16384)
    a.job_plan = lambda spec: JobPlan(spark_app=True, duration=0.3)
    st.wait_worker(a)
    done = st.wait_job(_spark_job(st, ws)["id"], timeout=60)
    assert done["state"] == "succeeded"
    prov = st.compute.providers.get("standalone")
    (cl,) = [c for c in prov.clusters() if c["id"] == done["cluster_id"]]
    # simulate the transient seen during start-up: workers not yet registered
    cl = prov._set_state(cl, "degraded", "0/? workers alive")
    assert cl["state"] == "degraded" and cl["detail"] == "0/? workers alive"
    cl = prov.refresh_cluster(cl)
    assert cl["state"] == "up" and cl["detail"] == "", cl
    topo = st.http.get("/v1/compute/topology", headers=st.admin).json()
    (tc,) = [c for c in topo["clusters"] if c["id"] == cl["id"]]
    assert tc["detail"] == "" and tc["master"]["alive_workers"] >= 1
    # the same state with a new detail replaces the old one, and "up" never keeps one
    cl = prov._set_state(cl, "degraded", "1/2 workers alive")
    assert cl["detail"] == "1/2 workers alive"
    assert prov._set_state(cl, "up", "")["detail"] == ""
