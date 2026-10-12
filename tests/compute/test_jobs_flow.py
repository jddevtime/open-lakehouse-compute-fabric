# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""End-to-end job flows against a live control plane with fake agents: enroll -> connect -> submit -> logs -> terminal states."""

from __future__ import annotations

import json

from tests.compute.fake_agent import JobPlan, wait_until


def test_enroll_connect_submit_logs_succeed(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("flow-a")
    w = st.wait_worker(a)
    assert w["name"] == "flow-a" and w["state"] == "online" and w["inventory"]["cores"] == 8
    assert a.hello_acks and a.hello_acks[0]["worker_id"] == a.worker_id

    job = st.submit(ws)
    assert job["state"] in ("accepted", "validating", "queued", "provisioning", "running", "succeeded")
    done = st.wait_job(job["id"])
    assert done["state"] == "succeeded", done
    assert done["driver_worker_id"] == a.worker_id
    assert done["runtime_id"] == "olcf-rt-2.0.0"
    logs = st.logs(job["id"])
    assert "fake job started" in logs and "fake job done" in logs
    # the agent received a CONTRACTS 4.3 job spec and the source
    (cmd,) = a.commands_of("run_job")
    spec = cmd["payload"]["spec"]
    assert spec["schema"] == "olcf.jobspec/1" and spec["job_id"] == job["id"] and spec["attempt"] == 1
    assert spec["callbacks"]["job_token"].startswith("olcf_") and spec["limits"]["timeout_seconds"] > 0
    assert cmd["payload"]["source"]["content"] == "print('hi')"
    # timeline contains the full state path
    detail = st.job(job["id"])
    states = [e["to"] for e in detail["timeline"] if e.get("kind") == "state"]
    for s in ("accepted", "validating", "queued", "provisioning", "running", "succeeded"):
        assert s in states, states


def test_job_failure_is_classified_and_reported(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("flow-fail")
    a.job_plan = lambda spec: JobPlan(outcome="fail", failure={"category": "user-code", "message": "NameError: x", "traceback": "tb"})
    st.wait_worker(a)
    j = st.wait_job(st.submit(ws)["id"])
    assert j["state"] == "failed"
    assert j["failure"]["category"] == "user-code" and "NameError" in j["failure"]["message"]
    assert j["failure_category"] == "user-code"
    assert j["attempt"] == 1  # user-code failures are not retried


def test_cancel_running_job(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("flow-cancel")
    a.job_plan = lambda spec: JobPlan(outcome="hang")
    st.wait_worker(a)
    job = st.submit(ws)
    st.wait_job(job["id"], "running")
    r = st.http.post(f"/v1/jobs/{job['id']}/cancel", headers=st.admin)
    assert r.status_code in (200, 202), r.text
    done = st.wait_job(job["id"])
    assert done["state"] == "cancelled"
    wait_until(lambda: a.commands_of("cancel_job"), 5, message="cancel_job at the agent")
    assert a.commands_of("cancel_job")[0]["payload"]["job_id"] == job["id"]


def test_cancel_queued_job_never_reaches_an_agent(fleet):
    st = fleet
    ws = st.workspace()
    job = st.submit(ws)  # no worker at all: stays queued
    wait_until(lambda: st.job(job["id"])["state"] == "queued", 10, message="queued")
    wait_until(lambda: st.job(job["id"])["wait_reason"], 10, message="the scheduler to say why the job waits")  # set by the first planning pass after queueing
    r = st.http.post(f"/v1/jobs/{job['id']}/cancel", headers=st.admin)
    assert r.status_code in (200, 202), r.text
    assert st.wait_job(job["id"])["state"] == "cancelled"


def test_job_timeout(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("flow-timeout")
    a.job_plan = lambda spec: JobPlan(outcome="hang")
    st.wait_worker(a)
    job = st.submit(ws, policy={"timeout_seconds": 2})
    done = st.wait_job(job["id"], timeout=60)
    assert done["state"] == "timed_out", done
    assert done["failure_category"] == "timeout"
    assert a.commands_of("cancel_job"), "the controller must ask the agent to stop the job"


def test_idempotent_submit_returns_the_same_job(fleet):
    st = fleet
    ws = st.workspace()
    st.agent("flow-idem")
    j1 = st.submit(ws, idem="same-key", name="idem-job")
    j2 = st.submit(ws, idem="same-key", name="idem-job")
    assert j1["id"] == j2["id"]
    r = st.http.post("/v1/jobs", headers={**st.admin, "Idempotency-Key": "same-key"},
                     json={"kind": "script", "workspace_id": ws, "engine": "python", "source": {"format": "py", "content": "print(2)"}})
    assert r.status_code == 409, r.text  # same key, different request
    st.wait_job(j1["id"])
    assert len([c for c in st.compute.hub.snapshot()["connected"]]) >= 1


def test_logs_paging_and_sse_stream(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("flow-logs")
    a.job_plan = lambda spec: JobPlan(logs=[f"line {i}\n" for i in range(50)], stderr=["warn\n"], duration=0.5)
    st.wait_worker(a)
    job = st.submit(ws)
    events: list[tuple[str, str]] = []
    with st.http.stream("GET", f"/v1/jobs/{job['id']}/logs/stream", headers=st.admin, timeout=30) as r:
        assert r.status_code == 200
        name = ""
        for line in r.iter_lines():
            if line.startswith("event:"):
                name = line[6:].strip()
            elif line.startswith("data:"):
                events.append((name, line[5:].strip()))
                if name == "end":
                    break
    kinds = {k for k, _ in events}
    assert {"log", "end"} <= kinds
    text = "".join(json.loads(d).get("data", "") for k, d in events if k == "log")
    assert "line 49" in text
    full = st.logs(job["id"])
    assert full.count("line ") == 50
    r = st.http.get(f"/v1/jobs/{job['id']}/logs", headers=st.admin, params={"offset": 0, "limit": 20})
    d = r.json()
    assert len(d["data"].encode()) <= 20 and d["next_offset"] <= 20 and not d["eof"]
    st.wait_job(job["id"])


def test_retry_of_failed_job_creates_a_new_job(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("flow-retry")
    a.job_plan = lambda spec: JobPlan(outcome="fail")
    st.wait_worker(a)
    j = st.wait_job(st.submit(ws)["id"])
    assert j["state"] == "failed"
    a.job_plan = lambda spec: JobPlan()
    r = st.http.post(f"/v1/jobs/{j['id']}/retry", headers=st.admin)
    assert r.status_code in (200, 201, 202), r.text
    new = r.json()
    assert new["id"] != j["id"]
    assert st.wait_job(new["id"])["state"] == "succeeded"


def test_automatic_retry_after_infrastructure_failure(fleet):
    """policy.retries + a provider/infra failure (the agent refuses run_job) re-queues until the node works again."""
    st = fleet
    ws = st.workspace()
    a = st.agent("flow-autoretry")
    a.fail_commands["run_job"] = "disk full"
    a.fail_times["run_job"] = 1
    st.wait_worker(a)
    job = st.submit(ws, policy={"retries": 2, "retry_backoff_seconds": 0})
    done = st.wait_job(job["id"], timeout=60)
    assert done["state"] == "succeeded" and done["attempt"] == 2, done
    assert len(a.commands_of("run_job")) == 2
    assert [c["payload"]["spec"]["attempt"] for c in a.commands_of("run_job")] == [1, 2]


def test_redelivered_job_events_are_deduplicated_by_seq(fleet):
    """NODE addenda N3: ``job_event.seq`` is the dedupe id; agents re-send events after reconnecting."""
    st = fleet
    ws = st.workspace()
    a = st.agent("flow-evdup")
    a.job_plan = lambda spec: JobPlan(events=[("custom_a", {"n": 1}, 100), ("custom_b", {"n": 2}, 250), ("custom_c", {"n": 3})], duration=0.5)
    st.wait_worker(a)
    done = st.wait_job(st.submit(ws)["id"])
    assert done["state"] == "succeeded"
    ev = [e["event"] for e in st.job(done["id"])["timeline"] if e.get("event")]
    assert ev.count("custom_a") == 1 and ev.count("custom_b") == 1
    assert ev.count("custom_c") == 1  # no seq: delivered once, kept as is
