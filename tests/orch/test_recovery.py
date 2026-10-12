# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Durability: the dispatcher is killed in the middle of runs and a new one (restart or failover) must continue from the database alone.

``Inproc.restart_engine(crash=True)`` behaves like ``kill -9``: the dispatcher thread stops, in-flight handles are dropped and the old instance never writes again.
Nothing may be executed twice unless the activity is declared safe to repeat, and nothing may stay stuck.
"""

from __future__ import annotations

import time
from typing import Any

from sqlalchemy import select, update

from olcf.pipelines.schema import activity_runs as A
from tests.orch import probes
from tests.orch.orch_support import Inproc, wait_until


def P(name: str, *deps: Any, type_: str = "t_probe", policy: dict[str, Any] | None = None, **props: Any) -> dict[str, Any]:
    a: dict[str, Any] = {"name": name, "type": type_, "typeProperties": props}
    if deps:
        a["dependsOn"] = list(deps)
    if policy:
        a["policy"] = policy
    return a


def running(ip: Inproc, run_id: str, name: str) -> dict[str, Any] | None:
    a = ip.acts(run_id).get(name)
    return a[0] if a and a[0]["state"] == "running" else None


def test_async_activity_is_adopted_not_restarted(ip: Inproc) -> None:
    p = ip.pipeline("adopt", [P("W", type_="t_async", gate="g1", value="done"), P("After", "W", value="@activity('W').output.value")])
    run = ip.run(p)
    wait_until(lambda: running(ip, run["id"], "W"), 10, 0.03, "W running")
    wait_until(lambda: ip.act(run["id"], "W") and _state_polls(ip, run["id"], "W") >= 2, 10, 0.03, "a couple of polls")
    ip.restart_engine(crash=True)
    time.sleep(0.4)
    assert ip.get(run["id"])["state"] == "running"
    probes.gate("g1").set()
    done = ip.wait(run["id"])
    assert done["state"] == "succeeded"
    assert len(probes.STARTS) == 1                                         # start() ran once; the new dispatcher only polled
    assert _state_polls(ip, run["id"], "W") >= 3 and ip.act(run["id"], "After")["output"]["value"] == "done"


def _state_polls(ip: Inproc, run_id: str, name: str) -> int:
    row = ip.act(run_id, name)
    with ip.ctx.db.connect() as conn:
        sd = conn.execute(select(A.c.state_data).where(A.c.id == row["id"])).scalar() or {}
    return int(sd.get("polls", 0))


def test_interrupted_blocking_activity_is_failed_not_repeated(ip: Inproc) -> None:
    p = ip.pipeline("interrupted", [P("B", gate="g2"), P("After", "B"), P("Handler", {"activity": "B", "conditions": ["failed"]}, value="cleanup")])
    run = ip.run(p)
    wait_until(lambda: probes.calls("B"), 10, 0.03, "B started")
    ip.restart_engine(crash=True)
    done = ip.wait(run["id"])
    acts = ip.acts(run["id"])
    assert acts["B"][0]["state"] == "failed" and acts["B"][0]["error"]["code"] == "interrupted", acts["B"][0]["error"]
    assert acts["After"][0]["state"] == "skipped" and acts["Handler"][0]["state"] == "succeeded"
    assert done["state"] == "succeeded"                                    # the failure was handled
    assert len(probes.calls("B")) == 1
    probes.gate("g2").set()                                                # the orphaned thread of the dead dispatcher finishing later must not change anything
    time.sleep(0.4)
    assert ip.act(run["id"], "B")["state"] == "failed" and ip.act(run["id"], "B")["error"]["code"] == "interrupted"


def test_safe_to_repeat_activities_run_again_as_the_same_attempt(ip: Inproc) -> None:
    p = ip.pipeline("safe", [P("S", type_="t_probe_safe", gate="g3", value="s"), P("I", gate="g4", value="i", policy={"idempotent": True})])
    run = ip.run(p)
    wait_until(lambda: probes.calls("S") and probes.calls("I"), 10, 0.03, "both started")
    ip.restart_engine(crash=True)
    wait_until(lambda: len(probes.calls("S")) == 2 and len(probes.calls("I")) == 2, 10, 0.03, "both executed again")
    probes.gate("g3").set(), probes.gate("g4").set()
    done = ip.wait(run["id"])
    assert done["state"] == "succeeded", ip.acts(run["id"])
    for n in ("S", "I"):
        again = probes.calls(n)[1]
        assert again["attempt"] == 1 and again["resuming"] is True        # same attempt number, flagged as a resumption
        assert ip.act(run["id"], n)["attempt"] == 1


def test_queued_runs_wait_through_a_crash(ip: Inproc) -> None:
    p = ip.pipeline("queued", [P("A", sleep=0.1)], settings={"concurrency": 1})
    ip.engine.crash()                                                        # no dispatcher at all while the runs are created
    runs = [ip.run(p) for _ in range(3)]
    time.sleep(0.3)
    assert [ip.get(r["id"])["state"] for r in runs] == ["queued"] * 3
    ip.restart_engine(crash=False)
    assert [ip.wait(r["id"])["state"] for r in runs] == ["succeeded"] * 3
    assert probes.ACTIVE["max"] == 1


def test_retry_wait_and_wait_activities_survive_a_crash(ip: Inproc) -> None:
    p = ip.pipeline("timers", [P("R", failTimes=1, policy={"retries": 1, "retryBackoffSeconds": 1.2}), {"name": "W", "type": "wait", "dependsOn": ["R"], "typeProperties": {"waitTimeInSeconds": 1.0}}])
    t0 = time.monotonic()
    run = ip.run(p)
    wait_until(lambda: (a := ip.acts(run["id"]).get("R")) and a[0]["state"] == "retry_wait", 10, 0.03, "R in retry_wait")
    ip.restart_engine(crash=True)
    wait_until(lambda: (a := ip.acts(run["id"]).get("W")) and a[0]["state"] == "running", 10, 0.03, "W running")
    ip.restart_engine(crash=True)
    done = ip.wait(run["id"])
    assert done["state"] == "succeeded" and time.monotonic() - t0 >= 2.0
    assert [c["attempt"] for c in probes.calls("R")] == [1, 2]


def test_for_each_continues_exactly_where_it_stopped(ip: Inproc) -> None:
    p = ip.pipeline("foreach-crash", [{"name": "F", "type": "for_each", "typeProperties": {"items": "@range(0, 4)", "isSequential": True, "collect": "W", "activities": [
        P("W", type_="t_probe_safe", value="@item()", gate="@concat('fe', string(item()))")]}}])
    # gates are strings: open the first two iterations up front
    probes.gate("fe0").set(), probes.gate("fe1").set()
    run = ip.run(p)
    wait_until(lambda: any(c["value"] == 2 for c in probes.calls("W")), 10, 0.03, "iteration 2 started")
    assert [c["value"] for c in probes.calls("W")] == [0, 1, 2]
    ip.restart_engine(crash=True)
    wait_until(lambda: len([c for c in probes.calls("W") if c["value"] == 2]) == 2, 10, 0.03, "iteration 2 executed again")
    probes.gate("fe2").set(), probes.gate("fe3").set()
    done = ip.wait(run["id"])
    assert done["state"] == "succeeded"
    values = [c["value"] for c in probes.calls("W")]
    assert values.count(0) == 1 and values.count(1) == 1 and values.count(2) == 2 and values.count(3) == 1       # finished iterations were not run again
    assert [r["value"] for r in ip.act(run["id"], "F")["output"]["results"]] == [0, 1, 2, 3]


def test_child_run_is_not_started_twice_after_a_crash(ip: Inproc) -> None:
    ip.pipeline("kid", [P("K", type_="t_async", gate="g5", value="kid-done")])
    p = ip.pipeline("mom", [{"name": "C", "type": "child_pipeline", "typeProperties": {"pipeline": "kid"}}])
    run = ip.run(p)
    wait_until(lambda: probes.STARTS, 10, 0.03, "child activity started")
    ip.restart_engine(crash=True)
    time.sleep(0.5)
    probes.gate("g5").set()
    assert ip.wait(run["id"])["state"] == "succeeded"
    assert len(ip.svc.children(ip.admin, run["id"])) == 1 and len(probes.STARTS) == 1


def test_callback_received_while_the_dispatcher_was_down_is_applied(ip: Inproc) -> None:
    p = ip.pipeline("cb-down", [{"name": "Approve", "type": "webhook_wait", "typeProperties": {"timeoutSeconds": 120}}])
    run = ip.run(p)
    act = wait_until(lambda: running(ip, run["id"], "Approve"), 10, 0.03)
    with ip.ctx.db.connect() as conn:
        token = conn.execute(select(A.c.state_data).where(A.c.id == act["id"])).scalar()["token"]
    ip.engine.crash()
    ip.svc.deliver_callback(act["id"], token, b'{"output": {"ok": true}}')     # the API replica accepts the callback although no dispatcher runs
    time.sleep(0.2)
    assert ip.get(run["id"])["state"] == "running"
    ip.restart_engine(crash=False)
    assert ip.wait(run["id"])["state"] == "succeeded"
    assert ip.act(run["id"], "Approve")["output"]["output"] == {"ok": True}


def test_a_lease_taken_over_by_someone_else_fences_the_old_writer(ip: Inproc) -> None:
    """Even without a crash: if another dispatcher took the attempt over (lease token changed), the original thread's result is discarded."""
    p = ip.pipeline("fenced", [P("B", gate="g6", value="late")])
    run = ip.run(p)
    act = wait_until(lambda: running(ip, run["id"], "B"), 10, 0.03)
    with ip.ctx.db.begin() as conn:
        conn.execute(update(A).where(A.c.id == act["id"]).values(lease_token="someone-else"))  # noqa: S106
    probes.gate("g6").set()                                                  # the original thread now finishes with a success it must not be able to record
    done = ip.wait(run["id"])
    b = ip.act(run["id"], "B")
    assert b["state"] == "failed" and b["error"]["code"] == "interrupted" and done["state"] == "failed"
    assert b["output"] in (None, {})


def test_adoption_of_a_cancelling_run_finishes_the_cancel(ip: Inproc) -> None:
    p = ip.pipeline("cancel-crash", [P("A", type_="t_async", gate="g7")])
    run = ip.run(p)
    wait_until(lambda: running(ip, run["id"], "A"), 10, 0.03)
    ip.engine.crash()
    ip.svc.cancel(ip.admin, run["id"], "operator cancelled")
    ip.restart_engine(crash=False)
    assert ip.wait(run["id"])["state"] == "cancelled"
    assert ip.act(run["id"], "A")["state"] == "cancelled"
