# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Queue behaviour through the live API: pause/resume, ordering of a 500-job backlog, concurrency limits, max_queued, priorities and workspace limits."""

from __future__ import annotations

import time

from tests.compute.fake_agent import JobPlan, wait_until


def _queue(st, name: str, **body) -> dict:
    r = st.http.post("/v1/queues", headers=st.admin, json={"name": name, **body})
    assert r.status_code == 201, r.text
    return r.json()


def _set(st, key: str, value) -> None:
    assert st.http.put(f"/v1/admin/settings/{key}", headers=st.admin, json={"value": value}).status_code == 200


def test_500_job_backlog_is_dispatched_by_priority_then_fifo(fleet):
    st = fleet
    ws = st.workspace()
    _set(st, "compute.aging_interval_seconds", 1_000_000)  # no aging: the order must be exactly priority desc, then submission order
    _queue(st, "bulk", max_concurrency=1)
    assert st.http.post("/v1/queues/bulk/pause", headers=st.admin).json()["paused"] is True
    prios = (10, 50, 90)
    order: list[tuple[int, int]] = []  # (index, priority) in submission order
    ids: dict[str, int] = {}
    t0 = time.time()
    for i in range(500):
        p = prios[(i * 7) % 3]
        j = st.submit(ws, name=f"bulk-{i}", policy={"queue": "bulk", "priority": p})
        ids[j["id"]] = i
        order.append((i, p))
    submit_s = time.time() - t0
    q = st.http.get("/v1/queues/bulk", headers=st.admin).json()
    assert q["depth"] == 500 and q["running"] == 0, q
    a = st.agent("q-order", cores=2, memory_mb=8192)  # 1 usable core -> one job at a time
    a.job_plan = lambda spec: JobPlan(duration=0.0, logs=[])
    st.wait_worker(a)
    time.sleep(1.0)
    assert not a.commands_of("run_job"), "a paused queue must not dispatch"
    st.http.post("/v1/queues/bulk/resume", headers=st.admin)
    n = 40
    wait_until(lambda: len(a.commands_of("run_job")) >= n, 120, 0.2, f"{n} dispatches")
    st.http.post("/v1/queues/bulk/pause", headers=st.admin)
    got = [ids[c["payload"]["spec"]["job_id"]] for c in a.commands_of("run_job")][:n]
    expected = [i for i, p in sorted(order, key=lambda x: (-x[1], x[0]))][:n]
    assert got == expected, (got, expected)
    assert submit_s < 120
    # clean up the backlog (queued jobs cancel immediately)
    for jid in list(ids):
        st.http.post(f"/v1/jobs/{jid}/cancel", headers=st.admin)
    wait_until(lambda: st.http.get("/v1/queues/bulk", headers=st.admin).json()["depth"] == 0, 120, 0.5, "the backlog to drain")


def test_queue_concurrency_limit(fleet):
    st = fleet
    ws = st.workspace()
    _queue(st, "limited", max_concurrency=2)
    a = st.agent("q-conc", cores=16, memory_mb=32768)
    a.job_plan = lambda spec: JobPlan(outcome="hang")
    st.wait_worker(a)
    jobs = [st.submit(ws, policy={"queue": "limited"})["id"] for _ in range(5)]
    wait_until(lambda: len(a.commands_of("run_job")) == 2, 15, message="two jobs running")
    time.sleep(1.0)
    assert len(a.commands_of("run_job")) == 2
    q = st.http.get("/v1/queues/limited", headers=st.admin).json()
    assert q["running"] == 2 and q["depth"] == 3
    first = a.run_job_specs()[0]["job_id"]
    a.finish_hung(first, "succeed")
    wait_until(lambda: len(a.commands_of("run_job")) == 3, 15, message="the next job to start after a slot freed")
    for j in jobs:
        st.http.post(f"/v1/jobs/{j}/cancel", headers=st.admin)
    for j in jobs:
        st.wait_job(j)


def test_max_queued_rejects_with_429(fleet):
    st = fleet
    ws = st.workspace()
    _queue(st, "tiny", max_queued=2)
    st.http.post("/v1/queues/tiny/pause", headers=st.admin)
    ok = [st.submit(ws, policy={"queue": "tiny"})["id"] for _ in range(2)]
    r = st.http.post("/v1/jobs", headers=st.admin, json={"kind": "script", "workspace_id": ws, "engine": "python", "source": {"format": "py", "content": "1"}, "policy": {"queue": "tiny"}})
    assert r.status_code == 429, r.text
    for j in ok:
        st.http.post(f"/v1/jobs/{j}/cancel", headers=st.admin)


def test_queue_wait_limit_times_out_waiting_jobs(fleet):
    st = fleet
    ws = st.workspace()
    _queue(st, "impatient", max_queue_wait_seconds=2)
    j = st.submit(ws, policy={"queue": "impatient"})  # no worker in the fleet
    done = st.wait_job(j["id"], timeout=60)
    assert done["state"] == "timed_out" and "queue" in done["state_reason"].lower()


def test_workspace_limit_in_queue(fleet):
    st = fleet
    ws1, ws2 = st.workspace(), st.workspace()
    _queue(st, "wsl", workspace_limits={ws1: {"max_concurrency": 1}})
    a = st.agent("q-wsl", cores=16, memory_mb=32768)
    a.job_plan = lambda spec: JobPlan(outcome="hang")
    st.wait_worker(a)
    j1 = [st.submit(ws1, policy={"queue": "wsl"})["id"] for _ in range(3)]
    j2 = st.submit(ws2, policy={"queue": "wsl"})["id"]
    wait_until(lambda: len(a.commands_of("run_job")) == 2, 15, message="one job of ws1 and the job of ws2")
    time.sleep(0.8)
    started = {c["payload"]["spec"]["workspace"]["id"] for c in a.commands_of("run_job")}
    assert started == {ws1, ws2} and len(a.commands_of("run_job")) == 2
    for j in [*j1, j2]:
        st.http.post(f"/v1/jobs/{j}/cancel", headers=st.admin)
        st.wait_job(j)


def test_infeasible_request_after_node_loss_fails_after_grace(fleet):
    st = fleet
    ws = st.workspace()
    _set(st, "compute.infeasible_grace_seconds", 2)
    try:
        a = st.agent("q-big", cores=32, memory_mb=65536)
        st.wait_worker(a)
        j = st.submit(ws, resources={"driver_cores": 16, "driver_memory_mb": 8192})
        st.wait_job(j["id"])
        a.stop()
        st.http.post(f"/v1/workers/{a.worker_id}/cordon", headers=st.admin)
        st.http.post(f"/v1/workers/{a.worker_id}/revoke", headers=st.admin)
        small = st.agent("q-small", cores=2, memory_mb=4096)
        st.wait_worker(small)
        r = st.http.post("/v1/jobs", headers=st.admin, json={"kind": "script", "workspace_id": ws, "engine": "python", "source": {"format": "py", "content": "1"},
                                                              "resources": {"driver_cores": 16, "driver_memory_mb": 8192}})
        assert r.status_code in (409, 422, 400)  # cannot ever fit: refused at submission (fail closed)
    finally:
        _set(st, "compute.infeasible_grace_seconds", 300)
