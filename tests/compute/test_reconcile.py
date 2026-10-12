# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Disconnects, agent restarts and reconciliation on ``hello`` (CONTRACTS 4.2): jobs are never lost silently and never run twice."""

from __future__ import annotations

import time

from tests.compute.fake_agent import JobPlan, wait_until


def _set(st, key: str, value) -> None:
    r = st.http.put(f"/v1/admin/settings/{key}", headers=st.admin, json={"value": value})
    assert r.status_code == 200, r.text


def test_disconnect_mid_job_agent_keeps_running_job_survives(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("rec-keep")
    a.job_plan = lambda spec: JobPlan(outcome="hang")
    st.wait_worker(a)
    job = st.submit(ws)
    st.wait_job(job["id"], "running")
    a.drop()
    st.wait_worker(a, "offline", timeout=30)
    assert st.job(job["id"])["state"] == "running", "a short disconnect must not fail the job (worker-lost grace)"
    a.reconnect()
    st.wait_worker(a, "online")
    assert a.hello_acks and a.connections == 2
    time.sleep(1.0)
    assert st.job(job["id"])["state"] == "running"
    a.finish_hung(job["id"], "succeed")
    assert st.wait_job(job["id"])["state"] == "succeeded"
    assert len(a.commands_of("run_job")) == 1, "the job must never be started a second time"


def test_job_finishing_while_disconnected_is_reported_after_hello(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("rec-term")
    a.job_plan = lambda spec: JobPlan(outcome="hang")
    st.wait_worker(a)
    job = st.submit(ws)
    st.wait_job(job["id"], "running")
    a.drop()
    a.finish_hung(job["id"], "succeed")  # finishes while the channel is down: the terminal state stays unconfirmed in the agent
    wait_until(lambda: a.jobs[job["id"]].state == "succeeded", 10, message="the job to end at the agent")
    assert a.jobs[job["id"]].delivered is False
    a.reconnect()
    done = st.wait_job(job["id"])
    assert done["state"] == "succeeded"
    ev = [e for e in st.job(job["id"])["timeline"] if e["kind"] == "event"]
    assert not any(e["event"] == "agent_lost_job" for e in ev)


def test_agent_restart_losing_the_job_fails_it_as_provider_failure(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("rec-lost")
    a.job_plan = lambda spec: JobPlan(outcome="hang")
    st.wait_worker(a)
    job = st.submit(ws)
    st.wait_job(job["id"], "running")
    time.sleep(2.5)  # the controller trusts a 'missing' job only once run_job was acknowledged for a moment
    a.drop()
    a.jobs.clear()  # the agent process restarted: nothing survived
    a.reconnect()
    done = st.wait_job(job["id"])
    assert done["state"] == "failed"
    assert done["failure_category"] == "provider"
    assert any(e["event"] == "agent_lost_job" for e in st.job(job["id"])["timeline"] if e["kind"] == "event")


def test_idempotent_job_is_replayed_after_confirmed_loss(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("rec-replay")
    a.job_plan = lambda spec: JobPlan(outcome="hang")
    st.wait_worker(a)
    job = st.submit(ws, policy={"retries": 1, "retry_backoff_seconds": 0, "idempotent": True})
    st.wait_job(job["id"], "running")
    time.sleep(2.5)  # the controller trusts a 'missing' job only once run_job was acknowledged for a moment
    a.drop()
    a.jobs.clear()
    a.job_plan = lambda spec: JobPlan()
    a.reconnect()
    done = st.wait_job(job["id"], timeout=40)
    assert done["state"] == "succeeded" and done["attempt"] == 2
    assert [c["payload"]["spec"]["attempt"] for c in a.commands_of("run_job")] == [1, 2]


def test_non_idempotent_job_is_not_replayed_after_loss(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("rec-noreplay")
    a.job_plan = lambda spec: JobPlan(outcome="hang")
    st.wait_worker(a)
    job = st.submit(ws, policy={"retries": 2, "retry_backoff_seconds": 0})
    st.wait_job(job["id"], "running")
    time.sleep(2.5)  # the controller trusts a 'missing' job only once run_job was acknowledged for a moment
    a.drop()
    a.jobs.clear()
    a.reconnect()
    done = st.wait_job(job["id"])
    assert done["state"] == "failed" and done["attempt"] == 1
    time.sleep(1)
    assert len(a.commands_of("run_job")) == 1


def test_worker_gone_beyond_grace_fails_the_job(fleet):
    st = fleet
    ws = st.workspace()
    _set(st, "compute.worker_lost_grace_seconds", 2)
    try:
        a = st.agent("rec-gone")
        a.job_plan = lambda spec: JobPlan(outcome="hang")
        st.wait_worker(a)
        job = st.submit(ws)
        st.wait_job(job["id"], "running")
        a.drop()
        a.stop()
        done = st.wait_job(job["id"], timeout=60)
        assert done["state"] == "failed" and done["failure_category"] in ("provider", "worker-lost", "infrastructure"), done
    finally:
        _set(st, "compute.worker_lost_grace_seconds", 180)


def test_running_job_missing_from_heartbeats_is_failed(fleet):
    st = fleet
    ws = st.workspace()
    _set(st, "compute.missing_report_grace_seconds", 3)
    try:
        a = st.agent("rec-missing")
        a.job_plan = lambda spec: JobPlan(outcome="crash")
        st.wait_worker(a)
        job = st.submit(ws)
        done = st.wait_job(job["id"], timeout=60)
        assert done["state"] == "failed"
    finally:
        _set(st, "compute.missing_report_grace_seconds", 30)


def test_unknown_process_reported_by_an_agent_is_cancelled(fleet):
    st = fleet
    a = st.agent("rec-orphan", start=False)
    a.ghost_jobs = [{"job_id": "job_01GHOSTGHOSTGHOSTGHOSTGHOS", "pid": 99, "state": "running", "attempt": 1}]
    a.start()
    st.wait_worker(a)
    cmd = wait_until(lambda: a.commands_of("cancel_job"), 10, message="cancel_job for the orphan")
    assert cmd[0]["payload"]["job_id"] == "job_01GHOSTGHOSTGHOSTGHOSTGHOS" and cmd[0]["payload"]["force"] is True
    assert "orphans" in (st.worker(a.worker_id).get("notes") or {})


def test_run_job_is_idempotent_at_the_agent_for_the_same_attempt(fleet):
    """The agent answers 'ok' to a repeated run_job of (job, attempt) without starting a second process (agent-side contract, exercised via the fake)."""
    st = fleet
    ws = st.workspace()
    a = st.agent("rec-dup")
    a.job_plan = lambda spec: JobPlan(outcome="hang")
    st.wait_worker(a)
    job = st.submit(ws)
    st.wait_job(job["id"], "running")
    payload = a.commands_of("run_job")[0]["payload"]
    res = st.compute.hub.request(a.worker_id, "run_job", payload, timeout=10)
    assert res.get("duplicate") is True
    assert len(a.jobs) == 1
    a.finish_hung(job["id"], "succeed")
    st.wait_job(job["id"])
