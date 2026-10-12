# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``POST /v1/compute/spark-smoke`` is asynchronous: 202 + a job id and a poll URL, a fast actionable 409 when no Spark runtime is ready anywhere, a bounded duration
(the ``olcf-spark-smoke`` job is cancelled on timeout, also when it never left the queue), cancellation, ``olcf compute smoke`` and the SDK helpers.

Fake agents only: the real-cluster run is ``test_spark_smoke_real.py`` (marked ``spark``).
"""

from __future__ import annotations

import json
import re
import time
from typing import Any

from tests.compute.fake_agent import JobPlan, wait_until

SMOKE = "/v1/compute/spark-smoke"
SMOKE_EVENTS = [("executor_hosts", {"hosts": [{"host": "sa-a.test", "tasks": 4}, {"host": "sa-b.test", "tasks": 4}]}),
                ("smoke_result", {"sum": 499500, "tasks": 8, "master": "spark://127.0.0.1:7077", "applicationId": "app-smoke-async"})]


def _smoke_jobs(st: Any, *states: str) -> list[dict[str, Any]]:
    """Every job the smoke test created (system workspace), newest first, optionally only in the given states."""
    from sqlalchemy import select

    from olcf.compute import schema as C

    with st.ctx.db.connect() as conn:
        rows = [dict(r._mapping) for r in conn.execute(select(C.jobs.c.id, C.jobs.c.name, C.jobs.c.state).where(C.jobs.c.name == "olcf-spark-smoke").order_by(C.jobs.c.created_at.desc()))]
    return [j for j in rows if not states or j["state"] in states]


def _poll(st: Any, doc: dict[str, Any]) -> dict[str, Any]:
    r = st.http.get(doc["poll_url"], headers=st.admin)
    assert r.status_code == 200, r.text
    return r.json()


def _hold_agent(st: Any, name: str, outcome: str = "hang", **kw: Any) -> Any:
    a = st.agent(name, **kw)
    a.job_plan = lambda spec: JobPlan(outcome=outcome, duration=600.0, spark_app=True, events=list(SMOKE_EVENTS))
    st.wait_worker(a)
    return a


# ------------------------------------------------------------------------------------------------------------------------- asynchronous contract
def test_post_returns_202_at_once_with_an_id_and_a_poll_url(fleet):
    st = fleet
    a, b = st.agent("sa-a"), st.agent("sa-b")
    plan = JobPlan(spark_app=True, duration=2.5, events=SMOKE_EVENTS)
    for x in (a, b):
        x.job_plan = lambda spec, plan=plan: plan
        st.wait_worker(x)
    t0 = time.monotonic()
    r = st.http.post(SMOKE, headers=st.admin, json={"timeout_seconds": 60})
    took = time.monotonic() - t0
    assert r.status_code == 202, r.text
    assert took < 2.0, f"the request must not wait for the job ({took:.1f} s)"
    doc = r.json()
    assert doc["id"] == doc["job_id"] and doc["id"].startswith("job_") and doc["poll_url"] == f"{SMOKE}/{doc['id']}"
    assert r.headers["location"] == doc["poll_url"] and int(r.headers["retry-after"]) >= 1
    assert doc["finished"] is False and doc["ok"] is None and doc["timeout_seconds"] == 60 and doc["deadline_at"]
    assert doc["phase"] in ("queued", "starting", "running") and doc["message"]
    seen = [doc["phase"]]
    final = wait_until(lambda: (seen.append((d := _poll(st, doc))["phase"]) or d["finished"]) and d, 30, 0.2, "the smoke test to finish")
    assert final["ok"] is True and final["state"] == "succeeded" and final["phase"] == "done"
    assert final["distributed"] is True and {n["name"] for n in final["nodes_with_executors"]} == {"sa-a", "sa-b"} and final["sum"] == 499500
    assert final["timed_out"] is False and final["elapsed_seconds"] > 0
    assert "running" in seen or "starting" in seen, seen  # it was observably in progress while the request was long gone
    assert _poll(st, doc)["finished"] is True  # a finished report stays readable


def test_poll_is_limited_to_smoke_jobs_and_to_permitted_callers(fleet):
    st = fleet
    a = st.agent("sa-rbac")
    st.wait_worker(a)
    ws = st.workspace()
    contributor, viewer = st.user("Contributor", ws), st.user("Viewer", ws)
    assert st.http.post(SMOKE, headers=contributor, json={}).status_code == 403
    assert st.http.post(SMOKE, json={}).status_code == 401
    other = st.submit(ws)  # an ordinary job is not a smoke test
    assert st.http.get(f"{SMOKE}/{other['id']}", headers=st.admin).status_code == 404
    assert st.http.get(f"{SMOKE}/job_nope", headers=st.admin).status_code == 404
    assert st.http.get(f"{SMOKE}/{other['id']}", headers=viewer).status_code in (403, 404)
    assert st.http.get(f"{SMOKE}/{other['id']}").status_code == 401
    assert st.http.post(f"{SMOKE}/job_nope/cancel", headers=contributor).status_code == 403  # permission first, whatever the id
    assert st.http.post(f"{SMOKE}/{other['id']}/cancel", headers=st.admin).status_code == 404  # an ordinary job cannot be stopped through this route
    st.http.post(f"/v1/jobs/{other['id']}/cancel", headers=st.admin)


# ------------------------------------------------------------------------------------------------------------------------- fail fast
def test_409_when_no_worker_is_online_and_no_job_is_created(fleet):
    st = fleet
    before = len(_smoke_jobs(st))
    r = st.http.post(SMOKE, headers=st.admin, json={})
    assert r.status_code == 409, r.text
    err = r.json()["error"]
    assert err["code"] == "no_spark_runtime" and "no worker" in err["message"].lower()
    assert "olcf worker enroll-token" in err["remediation"] and "olcf runtime install" in err["remediation"]
    assert len(_smoke_jobs(st)) == before


def test_409_when_online_workers_have_no_ready_spark_runtime(fleet):
    st = fleet
    host_only = st.agent("sa-host", runtimes=("olcf-host-python",))  # Python only: nothing for a Spark job
    st.wait_worker(host_only)
    r = st.http.post(SMOKE, headers=st.admin, json={})
    assert r.status_code == 409 and r.json()["error"]["code"] == "no_spark_runtime"
    d = r.json()["error"]["details"]
    assert d["online_workers"] == 1 and d["spark_ready_workers"] == 0 and "olcf-host-python" not in d.get("spark_runtimes", [])
    assert "no Spark runtime is ready" in r.json()["error"]["message"] and "olcf runtime install" in r.json()["error"]["remediation"]
    spark = st.agent("sa-spark", runtimes=("olcf-rt-2.0.0",))
    st.wait_worker(spark)
    spark.job_plan = lambda spec: JobPlan(spark_app=True, events=SMOKE_EVENTS)
    # a runtime that is not the requested one
    bad = st.http.post(SMOKE, headers=st.admin, json={"runtime_id": "olcf-rt-9.9.9"})
    assert bad.status_code == 409 and "olcf-rt-9.9.9" in bad.json()["error"]["message"]
    # a cordoned node does not count, a pool without a Spark node does not count
    assert st.http.post(f"/v1/workers/{spark.worker_id}/cordon", headers=st.admin, json={}).status_code == 200
    cordoned = st.http.post(SMOKE, headers=st.admin, json={})
    assert cordoned.status_code == 409 and cordoned.json()["error"]["details"]["spark_ready_workers"] == 0
    assert st.http.post(f"/v1/workers/{spark.worker_id}/uncordon", headers=st.admin, json={}).status_code == 200
    ok = st.http.post(SMOKE, headers=st.admin, json={"runtime_id": "olcf-rt-2.0.0"})
    assert ok.status_code == 202, ok.text
    wait_until(lambda: _poll(st, ok.json())["finished"], 30, 0.2, "the smoke test to finish")


def test_second_smoke_while_one_is_running_is_refused_with_the_running_id(fleet):
    st = fleet
    a = _hold_agent(st, "sa-dup")
    first = st.http.post(SMOKE, headers=st.admin, json={"timeout_seconds": 60})
    assert first.status_code == 202
    again = st.http.post(SMOKE, headers=st.admin, json={})
    assert again.status_code == 409 and again.json()["error"]["code"] == "smoke_already_running"
    assert again.json()["error"]["details"]["id"] == first.json()["id"] and again.json()["error"]["details"]["poll_url"] == first.json()["poll_url"]
    assert st.http.post(f"{SMOKE}/{first.json()['id']}/cancel", headers=st.admin).status_code == 200
    wait_until(lambda: _poll(st, first.json())["finished"], 30, 0.2, "the cancelled smoke test to finish")
    free = st.http.post(SMOKE, headers=st.admin, json={"timeout_seconds": 30})
    assert free.status_code == 202  # free again
    wait_until(lambda: a.run_job_specs(), 15, 0.2, "the smoke job to start")  # let the cluster come up before it is stopped: nothing may be left half-started
    st.http.post(f"{SMOKE}/{free.json()['id']}/cancel", headers=st.admin)
    wait_until(lambda: _poll(st, free.json())["finished"], 30, 0.3, "the cancelled smoke test to finish")


# ------------------------------------------------------------------------------------------------------------------------- bounded duration
def test_a_running_smoke_that_hangs_is_cancelled_at_the_deadline(fleet):
    st = fleet
    a = _hold_agent(st, "sa-hang")
    r = st.http.post(SMOKE, headers=st.admin, json={"timeout_seconds": 10})
    assert r.status_code == 202
    doc = r.json()
    wait_until(lambda: a.run_job_specs(), 15, 0.2, "the smoke job to start on the agent")
    final = wait_until(lambda: (d := _poll(st, doc))["finished"] and d, 40, 0.3, "the smoke test to be stopped at its deadline")
    assert final["ok"] is False and final["timed_out"] is True and final["state"] in ("cancelled", "timed_out")
    assert "timed out" in final["message"].lower() and "10" in final["message"]
    assert not _smoke_jobs(st, "queued", "running", "provisioning", "cancel_requested", "accepted", "validating"), "no smoke job is left behind"
    assert a.commands_of("cancel_job"), "the running job was cancelled on the agent"
    rec = [e for e in st.compute.jobs.timeline(doc["id"]) if e["event"] == "spark_smoke_timeout" or "smoke" in (e["reason"] or "")]
    assert rec, "the timeline says why the job was cancelled"
    audit = st.http.get("/v1/audit", headers=st.admin, params={"action": "compute.spark_smoke_timeout", "resource_id": doc["id"]}).json()["items"]
    assert audit and audit[0]["details"]["timeout_seconds"] == 10


def test_a_smoke_that_never_leaves_the_queue_is_cancelled_too(fleet):
    st = fleet
    # the only node is fully occupied by another job: the smoke job is accepted (the node is big enough) but stays queued
    small = st.agent("sa-small", cores=4, memory_mb=4096)
    small.job_plan = lambda spec: JobPlan(outcome="hang", duration=600.0)
    st.wait_worker(small)
    hog = st.submit(st.workspace(), resources={"driver_cores": 3, "driver_memory_mb": 1024})
    wait_until(lambda: small.run_job_specs(), 15, 0.2, "the occupying job to start")
    n_started = len(small.run_job_specs())
    r = st.http.post(SMOKE, headers=st.admin, json={"timeout_seconds": 10})
    assert r.status_code == 202, r.text
    doc = r.json()
    mid = _poll(st, doc)
    assert mid["finished"] is False and mid["phase"] == "queued"
    final = wait_until(lambda: (d := _poll(st, doc))["finished"] and d, 40, 0.3, "the queued smoke test to be stopped at its deadline")
    assert final["timed_out"] is True and final["ok"] is False and final["state"] == "cancelled"
    assert "queue" in final["message"].lower()
    assert len(small.run_job_specs()) == n_started, "the smoke job never reached the agent"
    assert not _smoke_jobs(st, "queued", "accepted", "validating")
    st.http.post(f"/v1/jobs/{hog['id']}/cancel", headers=st.admin)


def test_cancel_endpoint_stops_the_job(fleet):
    st = fleet
    a = _hold_agent(st, "sa-cancel")
    r = st.http.post(SMOKE, headers=st.admin, json={"timeout_seconds": 120})
    wait_until(lambda: a.run_job_specs(), 15, 0.2, "the smoke job to start")
    c = st.http.post(f"{SMOKE}/{r.json()['id']}/cancel", headers=st.admin)
    assert c.status_code == 200, c.text
    final = wait_until(lambda: (d := _poll(st, r.json()))["finished"] and d, 30, 0.3, "the cancelled smoke test to finish")
    assert final["state"] == "cancelled" and final["ok"] is False and final["timed_out"] is False
    assert st.http.post(f"{SMOKE}/{r.json()['id']}/cancel", headers=st.admin).status_code == 200  # idempotent


# ------------------------------------------------------------------------------------------------------------------------- CLI and SDK
def _cli(st: Any, *args: str) -> Any:
    from click.testing import CliRunner

    from olcf.cli.main import cli

    env = {"OLCF_URL": st.url, "OLCF_TOKEN": st.admin["Authorization"].split()[1], "NO_COLOR": "1", "NO_PROXY": "127.0.0.1,localhost", "OLCF_CONFIG_DIR": str(st.root / "cli-config")}
    return CliRunner().invoke(cli, list(args), env=env, catch_exceptions=False)


def test_cli_compute_smoke_waits_with_progress_and_reports(fleet):
    st = fleet
    a = st.agent("sa-cli")
    a.job_plan = lambda spec: JobPlan(spark_app=True, duration=2.0, events=SMOKE_EVENTS)
    st.wait_worker(a)
    res = _cli(st, "--json", "compute", "smoke", "--timeout", "60")
    assert res.exit_code == 0, res.output
    doc = json.loads(res.stdout)
    assert doc["finished"] is True and doc["ok"] is True and doc["state"] == "succeeded" and doc["tasks"] == 8 and doc["poll_url"].startswith(SMOKE)
    human = _cli(st, "compute", "smoke", "--timeout", "60")
    assert human.exit_code == 0, human.output
    assert "Spark smoke test" in human.output and "passed" in human.output and "application:" in human.output and "queued:" in human.output
    # --no-wait: print the id and how to follow it, return at once
    t0 = time.monotonic()
    nowait = _cli(st, "--json", "compute", "smoke", "--no-wait")
    assert nowait.exit_code == 0 and time.monotonic() - t0 < 5
    started = json.loads(nowait.stdout)
    assert started["finished"] is False and started["poll_url"] == f"{SMOKE}/{started['id']}"
    wait_until(lambda: _poll(st, started)["finished"], 30, 0.2, "the background smoke test to finish")
    status = _cli(st, "--json", "compute", "smoke", "--status", started["id"])
    assert status.exit_code == 0 and json.loads(status.stdout)["finished"] is True


def test_cli_compute_smoke_fails_fast_and_exits_nonzero(fleet):
    st = fleet
    res = _cli(st, "compute", "smoke")
    assert res.exit_code == 6, res.output  # 409 no_spark_runtime = "conflict / precondition" in the CLI exit-code table
    assert "no Spark runtime" in res.output or "no worker" in res.output.lower()
    assert "olcf runtime install" in res.output  # the remediation is shown


def test_cli_waiting_smoke_that_times_out_exits_nonzero(fleet):
    st = fleet
    _hold_agent(st, "sa-cli-hang")
    res = _cli(st, "--json", "compute", "smoke", "--server-timeout", "10", "--timeout", "60")
    doc = json.loads(res.stdout)
    assert res.exit_code == 10 and doc["timed_out"] is True and doc["finished"] is True  # 10 = the awaited job did not succeed


def test_cli_client_side_wait_timeout_exits_9_like_every_other_wait_and_not_11(fleet):
    """The CLI table: 9 = a --wait ran out of time (the work may still be running), 11 = prerequisite missing.  The smoke test keeps running on the controller."""
    st = fleet
    from olcf.cli.state import EXIT_PREREQ, EXIT_TIMEOUT

    assert (EXIT_TIMEOUT, EXIT_PREREQ) == (9, 11)
    _hold_agent(st, "sa-cli-client-timeout")
    res = _cli(st, "compute", "smoke", "--server-timeout", "60", "--timeout", "2")
    assert res.exit_code == 9, res.output
    assert "--status" in res.output and "--cancel" in res.output  # the hint says how to follow or stop it
    # it is still running: a second start is refused (409 -> exit 6); then stop it
    again = _cli(st, "compute", "smoke", "--no-wait")
    assert again.exit_code == 6, again.output
    smoke_id = re.search(r"job_[0-9A-Za-z]+", res.output).group(0)  # type: ignore[union-attr]
    assert _cli(st, "compute", "smoke", "--cancel", smoke_id).exit_code == 0
    wait_until(lambda: _poll(st, {"poll_url": f"{SMOKE}/{smoke_id}"})["finished"], 40, 0.3, "the cancelled smoke test to finish")


def test_sdk_start_status_wait_cancel(fleet):
    st = fleet
    from olcf.sdk import Client

    c = Client(st.url, token=st.admin["Authorization"].split()[1], max_retries=0)
    a = st.agent("sa-sdk")
    a.job_plan = lambda spec: JobPlan(spark_app=True, duration=1.0, events=SMOKE_EVENTS)
    st.wait_worker(a)
    started = c.compute.spark_smoke(timeout_seconds=60)
    assert started["finished"] is False and started["id"].startswith("job_") and started["poll_url"].endswith(started["id"])
    seen: list[str] = []
    final = c.compute.spark_smoke_wait(started["id"], timeout=60, interval=0.2, on_progress=lambda d: seen.append(d["phase"]))
    assert final["ok"] is True and seen and final["finished"] is True
    assert c.compute.spark_smoke_status(started["id"])["state"] == "succeeded"
    again = c.compute.spark_smoke(timeout_seconds=60)
    cancelled = c.compute.spark_smoke_cancel(again["id"])
    assert cancelled["id"] == again["id"]
    assert c.compute.spark_smoke_wait(again["id"], timeout=60, interval=0.2)["finished"] is True
