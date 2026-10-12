# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Pipeline engine semantics: dependency conditions, retries, timeouts, containers, variables, outputs, cancellation, resume, concurrency policies, child runs, callbacks.

These tests run the real engine in-process (SQLite) with test-only activity types from ``probes.py``; nothing here starts a worker, Spark or a JVM.
"""

from __future__ import annotations

import time
from typing import Any

import pytest

from olcf.core.errors import Conflict, NotFound, ValidationFailed
from tests.orch import probes
from tests.orch.orch_support import Inproc, wait_until


def P(name: str, *deps: Any, policy: dict[str, Any] | None = None, type_: str = "t_probe", **props: Any) -> dict[str, Any]:
    a: dict[str, Any] = {"name": name, "type": type_, "typeProperties": props}
    if deps:
        a["dependsOn"] = list(deps)
    if policy:
        a["policy"] = policy
    return a


def on(name: str, *conds: str) -> dict[str, Any]:
    return {"activity": name, "conditions": list(conds)}


def states(acts: dict[str, list[dict[str, Any]]]) -> dict[str, str]:
    return {k: v[0]["state"] for k, v in acts.items()}


# ------------------------------------------------------------------------------------------------------------------------------ basics
def test_linear_run_with_parameters_variables_and_outputs(ip: Inproc) -> None:
    run, acts = ip.execute(
        [P("A", value="@pipeline().parameters.n"),
         {"name": "S", "type": "set_variable", "dependsOn": ["A"], "typeProperties": {"variable": "v", "value": "@concat('x', string(activity('A').output.value))"}},
         P("B", "S", value="@variables('v')")],
        {"n": 7}, parameters={"n": {"type": "int", "default": 1}}, variables={"v": {"type": "string", "default": "init"}},
        outputs={"result": "@activity('B').output.value", "var": "@variables('v')", "run": "@pipeline().RunId"})
    assert run["state"] == "succeeded", run
    assert states(acts) == {"A": "succeeded", "S": "succeeded", "B": "succeeded"}
    assert acts["A"][0]["output"]["value"] == 7
    assert run["outputs"]["result"] == "x7" and run["outputs"]["var"] == "x7" and run["outputs"]["run"] == run["id"]
    assert run["variables"]["v"] == "x7"
    assert run["started_at"] and run["finished_at"] and run["duration_seconds"] is not None
    assert run["parameters"] == {"n": 7}


def test_parameter_defaults_coercion_and_rejection(ip: Inproc) -> None:
    p = ip.pipeline("params", [P("A", value="@pipeline().parameters.d")], parameters={"d": {"type": "string", "default": "dflt"}, "n": {"type": "int"}, "f": {"type": "bool", "default": False}})
    with pytest.raises(ValidationFailed) as ei:
        ip.run(p, {})
    assert "n" in str(ei.value)                              # missing required parameter named
    with pytest.raises(ValidationFailed):
        ip.run(p, {"n": "not a number"})
    with pytest.raises(ValidationFailed):
        ip.run(p, {"n": 1, "unknown": 2})
    run = ip.wait(ip.run(p, {"n": "5"})["id"])                # numeric strings are coerced
    assert run["state"] == "succeeded" and run["parameters"] == {"d": "dflt", "n": 5, "f": False}


def test_run_creation_is_idempotent_per_key(ip: Inproc) -> None:
    p = ip.pipeline("idem", [P("A")])
    r1 = ip.run(p, idempotency_key="k1")
    r2 = ip.run(p, idempotency_key="k1")
    r3 = ip.run(p, idempotency_key="k2")
    assert r1["id"] == r2["id"] != r3["id"]
    ip.wait(r1["id"]), ip.wait(r3["id"])
    assert len(probes.calls("A")) == 2


def test_empty_pipeline_succeeds(ip: Inproc) -> None:
    run, acts = ip.execute([])
    assert run["state"] == "succeeded" and not acts


def test_run_requires_a_pipeline_item_and_permission(ip: Inproc) -> None:
    nb = ip.item("notebook", "nb1", {"cells": []})
    with pytest.raises(Exception) as ei:
        ip.svc.run(ip.admin, nb["workspace_id"], nb["id"], {})
    assert "pipeline" in str(ei.value).lower()
    with pytest.raises(NotFound):
        ip.svc.run(ip.admin, ip.ws_id, "no-such-pipeline", {})


# ------------------------------------------------------------------------------------------------------------------------------ dependency conditions
def test_unhandled_failure_fails_the_run_and_skips_dependents(ip: Inproc) -> None:
    run, acts = ip.execute([P("A", fail=True), P("B", "A"), P("C", "B")])
    assert run["state"] == "failed"
    assert states(acts) == {"A": "failed", "B": "skipped", "C": "skipped"}
    assert acts["A"][0]["error"]["code"] == "boom" and "boom" in acts["A"][0]["error"]["message"]
    assert run["error"]["message"] and "A" in run["error"]["message"]
    assert probes.calls("B") == []


def test_failure_handler_makes_the_run_succeed(ip: Inproc) -> None:
    run, acts = ip.execute([P("A", fail=True), P("Handle", on("A", "failed"), value="handled"), P("Ok", "A")])
    assert run["state"] == "succeeded", run
    assert states(acts) == {"A": "failed", "Handle": "succeeded", "Ok": "skipped"}


def test_failure_is_handled_when_a_dependent_waits_for_failed_or_completed(ip: Inproc) -> None:
    run, acts = ip.execute([P("Bad", fail=True), P("Fin", on("Bad", "completed"))])
    assert acts["Bad"][0]["state"] == "failed" and acts["Fin"][0]["state"] == "succeeded"
    assert run["state"] == "succeeded"


def test_skipped_condition_and_propagation(ip: Inproc) -> None:
    run, acts = ip.execute([P("Bad", fail=True), P("Sk", "Bad"), P("OnSkip", on("Sk", "skipped")), P("Down", "Sk"), P("Handled", on("Bad", "failed"))])
    assert acts["Sk"][0]["state"] == "skipped"
    assert acts["OnSkip"][0]["state"] == "succeeded"
    assert acts["Down"][0]["state"] == "skipped"
    assert run["state"] == "succeeded"


def test_join_requires_every_condition(ip: Inproc) -> None:
    run, acts = ip.execute([P("A"), P("B", fail=True), P("C", "A", "B"), P("D", "A", on("B", "failed")), P("H", on("B", "failed"))])
    assert states(acts) == {"A": "succeeded", "B": "failed", "C": "skipped", "D": "succeeded", "H": "succeeded"}


def test_independent_branches_run_in_parallel(ip: Inproc) -> None:
    probes.gate("go")
    p = ip.pipeline("par", [P("A", gate="go"), P("B", gate="go"), P("C", gate="go"), P("D", "A", "B", "C")])
    run = ip.run(p)
    wait_until(lambda: probes.ACTIVE["now"] == 3, 10, 0.02, "three branches running at once")
    probes.gate("go").set()
    assert ip.wait(run["id"])["state"] == "succeeded"
    assert probes.ACTIVE["max"] == 3


def test_max_parallel_activities_setting(ip: Inproc) -> None:
    run, _ = ip.execute([P(f"T{i}", sleep=0.15) for i in range(6)], settings={"maxParallelActivities": 2})
    assert run["state"] == "succeeded" and probes.ACTIVE["max"] == 2


# ------------------------------------------------------------------------------------------------------------------------------ retries and timeouts
def test_retries_until_success_and_record_attempts(ip: Inproc) -> None:
    run, acts = ip.execute([P("A", failTimes=2, policy={"retries": 3, "retryBackoffSeconds": 0})])
    a = acts["A"][0]
    assert run["state"] == "succeeded" and a["attempt"] == 3 and a["output"]["attempt"] == 3
    assert [c["attempt"] for c in probes.calls("A")] == [1, 2, 3]
    assert [x.get("state") for x in (a["attempts"] or [])][:2] == ["failed", "failed"]


def test_retries_exhausted(ip: Inproc) -> None:
    run, acts = ip.execute([P("A", fail=True, policy={"retries": 2, "retryBackoffSeconds": 0}), P("B", "A")])
    assert run["state"] == "failed" and acts["A"][0]["state"] == "failed" and acts["A"][0]["attempt"] == 3
    assert len(probes.calls("A")) == 3 and acts["B"][0]["state"] == "skipped"


def test_non_retryable_errors_are_not_retried(ip: Inproc) -> None:
    run, acts = ip.execute([P("A", fail=True, retryable=False, policy={"retries": 5, "retryBackoffSeconds": 0}),
                            {"name": "F", "type": "fail", "dependsOn": [on("A", "failed")], "typeProperties": {"message": "custom stop", "errorCode": "E42"}, "policy": {"retries": 3}}])
    assert len(probes.calls("A")) == 1
    assert acts["F"][0]["state"] == "failed" and acts["F"][0]["error"]["code"] == "E42" and acts["F"][0]["error"]["message"] == "custom stop"
    assert run["state"] == "failed" and "custom stop" in run["error"]["message"]


def test_retry_backoff_grows_exponentially(ip: Inproc) -> None:
    run, _ = ip.execute([P("A", failTimes=2, policy={"retries": 2, "retryBackoffSeconds": 0.3, "backoffMultiplier": 2})])
    assert run["state"] == "succeeded"
    t = [c["at"] for c in probes.calls("A")]
    assert t[1] - t[0] >= 0.28 and t[2] - t[1] >= 0.55, [round(x - t[0], 2) for x in t]


def test_activity_timeout_fails_the_attempt(ip: Inproc) -> None:
    started = time.monotonic()
    run, acts = ip.execute([P("Slow", sleep=30, policy={"timeoutSeconds": 1})])
    a = acts["Slow"][0]
    assert run["state"] == "failed" and a["state"] == "failed" and "timed out" in a["error"]["message"].lower()
    assert a["error"]["code"].lower() in ("timeout", "timed_out", "activitytimeout")
    assert time.monotonic() - started < 15


def test_pipeline_timeout_fails_the_run(ip: Inproc) -> None:
    run, acts = ip.execute([P("Slow", sleep=30)], settings={"timeoutSeconds": 1})
    assert run["state"] == "failed" and "timeout" in (run["error"]["message"] or "").lower() and run["error"]["code"] == "run_timeout"
    assert acts["Slow"][0]["state"] in ("cancelled", "failed")


# ------------------------------------------------------------------------------------------------------------------------------ control flow
def test_if_runs_exactly_one_branch(ip: Inproc) -> None:
    defn = [{"name": "I", "type": "if", "typeProperties": {"expression": "@greater(pipeline().parameters.n, 5)",
                                                         "ifTrueActivities": [P("Big")], "ifFalseActivities": [P("Small")]}}, P("After", "I")]
    big, acts = ip.execute(defn, {"n": 9}, parameters={"n": {"type": "int"}})
    assert big["state"] == "succeeded" and "Big" in acts and "Small" not in acts and acts["I"][0]["output"]["branch"] is True and acts["After"][0]["state"] == "succeeded"
    small, acts = ip.execute(defn, {"n": 1}, parameters={"n": {"type": "int"}})
    assert "Small" in acts and "Big" not in acts and acts["I"][0]["output"]["branch"] is False


def test_if_failure_inside_a_branch_fails_the_container(ip: Inproc) -> None:
    run, acts = ip.execute([{"name": "I", "type": "if", "typeProperties": {"expression": "@true", "ifTrueActivities": [P("Bad", fail=True)]}}, P("After", "I")])
    assert run["state"] == "failed" and acts["I"][0]["state"] == "failed" and acts["After"][0]["state"] == "skipped"


def test_if_requires_a_boolean(ip: Inproc) -> None:
    run, acts = ip.execute([{"name": "I", "type": "if", "typeProperties": {"expression": "@concat('a','b')", "ifTrueActivities": [P("X")]}}])
    assert run["state"] == "failed" and "boolean" in acts["I"][0]["error"]["message"]


def test_switch_cases_default_and_case_values(ip: Inproc) -> None:
    sw = {"name": "S", "type": "switch", "typeProperties": {"on": "@pipeline().parameters.m", "cases": [
        {"value": "a", "activities": [P("CaseA")]}, {"value": "b", "activities": [P("CaseB")]}], "defaultActivities": [P("Dflt")]}}
    for value, ran in (("a", "CaseA"), ("b", "CaseB"), ("zzz", "Dflt")):
        run, acts = ip.execute([sw], {"m": value}, parameters={"m": {"type": "string"}})
        assert run["state"] == "succeeded" and {n for n in acts if n in ("CaseA", "CaseB", "Dflt")} == {ran}, (value, list(acts))


def test_for_each_collects_results_in_order_and_exposes_item(ip: Inproc) -> None:
    run, acts = ip.execute([{"name": "F", "type": "for_each", "typeProperties": {"items": "@pipeline().parameters.xs", "batchCount": 3, "collect": "W", "activities": [
        P("W", value="@concat(string(itemIndex()), ':', string(item()))", sleep=0.05)]}}, P("After", "F", value="@activity('F').output.iterations")],
        {"xs": ["a", "b", "c", "d", "e"]}, parameters={"xs": {"type": "array"}})
    assert run["state"] == "succeeded"
    out = acts["F"][0]["output"]
    assert out["iterations"] == 5 and out["succeeded"] == 5 and out["results"] == [{"value": f"{i}:{x}", "attempt": 1} for i, x in enumerate("abcde")]
    assert len(acts["W"]) == 5 and acts["After"][0]["output"]["value"] == 5


def test_for_each_parallelism_is_bounded_by_batch_count(ip: Inproc) -> None:
    run, _ = ip.execute([{"name": "F", "type": "for_each", "typeProperties": {"items": "@range(0, 8)", "batchCount": 3, "activities": [P("W", sleep=0.1)]}}])
    assert run["state"] == "succeeded" and probes.ACTIVE["max"] == 3
    probes.reset()
    run, _ = ip.execute([{"name": "F", "type": "for_each", "typeProperties": {"items": "@range(0, 4)", "isSequential": True, "activities": [P("W", sleep=0.05)]}}])
    assert run["state"] == "succeeded" and probes.ACTIVE["max"] == 1
    order = [c["at"] for c in probes.calls("W")]
    assert order == sorted(order)


def test_for_each_empty_and_invalid_items(ip: Inproc) -> None:
    with pytest.raises(ValidationFailed):          # a malformed expression is rejected when the pipeline is saved, not at run time
        ip.pipeline("bad-items", [{"name": "F", "type": "for_each", "typeProperties": {"items": "@createArray()", "activities": [P("W")]}}])
    run, acts = ip.execute([{"name": "F", "type": "for_each", "typeProperties": {"items": [], "activities": [P("W")]}}, P("After", "F")])
    assert run["state"] == "succeeded" and "W" not in acts and acts["F"][0]["output"]["iterations"] == 0
    run, acts = ip.execute([{"name": "F", "type": "for_each", "typeProperties": {"items": "@parameters('s')", "activities": [P("W")]}}], {"s": "text"}, parameters={"s": {"type": "string"}})
    assert run["state"] == "failed" and "array" in acts["F"][0]["error"]["message"]


def test_for_each_failure_semantics(ip: Inproc) -> None:
    body = [P("W", value="@item()", fail="@equals(item(), 2)")]
    run, acts = ip.execute([{"name": "F", "type": "for_each", "typeProperties": {"items": "@range(0, 4)", "isSequential": True, "continueOnFailure": True, "activities": body}}])
    assert run["state"] == "failed" and acts["F"][0]["state"] == "failed"
    assert acts["F"][0]["output"]["failed"] == 1 and acts["F"][0]["output"]["succeeded"] == 3 and len(probes.calls("W")) == 4
    probes.reset()
    run, acts = ip.execute([{"name": "F", "type": "for_each", "typeProperties": {"items": "@range(0, 4)", "isSequential": True, "continueOnFailure": False, "activities": body}}])
    assert run["state"] == "failed" and len(probes.calls("W")) == 3        # iterations after the failing one never start


def test_until_loops_until_the_condition_holds(ip: Inproc) -> None:
    run, acts = ip.execute([
        {"name": "U", "type": "until", "typeProperties": {"expression": "@greaterOrEquals(length(variables('seen')), 3)", "maxIterations": 10, "activities": [
            {"name": "Add", "type": "set_variable", "typeProperties": {"variable": "seen", "mode": "append", "value": "@string(length(variables('seen')))"}}]}}],
        variables={"seen": {"type": "array", "default": []}})
    assert run["state"] == "succeeded", run
    assert run["variables"]["seen"] == ["0", "1", "2"] and acts["U"][0]["output"]["iterations"] == 3


def test_until_max_iterations_guard(ip: Inproc) -> None:
    run, acts = ip.execute([{"name": "U", "type": "until", "typeProperties": {"expression": "@false", "maxIterations": 3, "activities": [P("W")]}}])
    assert run["state"] == "failed" and "maxIterations" in acts["U"][0]["error"]["message"] and len(probes.calls("W")) == 3


def test_nested_containers(ip: Inproc) -> None:
    inner = {"name": "In", "type": "if", "typeProperties": {"expression": "@equals(mod(item(), 2), 0)", "ifTrueActivities": [P("Even", value="@item()")], "ifFalseActivities": [P("Odd", value="@item()")]}}
    run, acts = ip.execute([{"name": "F", "type": "for_each", "typeProperties": {"items": "@range(0, 4)", "batchCount": 2, "activities": [inner]}}])
    assert run["state"] == "succeeded" and len(acts["Even"]) == 2 and len(acts["Odd"]) == 2
    assert sorted(a["output"]["value"] for a in acts["Even"]) == [0, 2]


def test_variables_append_and_type_errors(ip: Inproc) -> None:
    with pytest.raises(ValidationFailed):          # literal of the wrong type: caught when saving
        ip.pipeline("bad-literal", [{"name": "S", "type": "set_variable", "typeProperties": {"variable": "n", "value": "not a number"}}], variables={"n": {"type": "int"}})
    run, acts = ip.execute([{"name": "S", "type": "set_variable", "typeProperties": {"variable": "n", "value": "@concat('not', 'a number')"}}], variables={"n": {"type": "int"}})
    assert run["state"] == "failed" and "int" in acts["S"][0]["error"]["message"]
    run, acts = ip.execute([{"name": "S", "type": "set_variable", "typeProperties": {"variable": "s", "mode": "append", "value": 1}}], variables={"s": {"type": "string"}})
    assert run["state"] == "failed" and "append" in acts["S"][0]["error"]["message"]


def test_wait_activity_waits(ip: Inproc) -> None:
    t0 = time.monotonic()
    run, acts = ip.execute([{"name": "W", "type": "wait", "typeProperties": {"waitTimeInSeconds": 0.6}}, P("After", "W")])
    assert run["state"] == "succeeded" and time.monotonic() - t0 >= 0.55 and acts["W"][0]["output"]["waitedSeconds"] == 0.6


def test_expression_errors_fail_the_activity_with_a_precise_message(ip: Inproc) -> None:
    run, acts = ip.execute([P("A", value="@div(1, pipeline().parameters.z)")], {"z": 0}, parameters={"z": {"type": "int"}})
    assert run["state"] == "failed" and "division by zero" in acts["A"][0]["error"]["message"]
    assert acts["A"][0]["error"]["category"] in ("expression", "configuration")
    assert probes.calls("A") == []


# ------------------------------------------------------------------------------------------------------------------------------ cancellation
def test_cancel_running_run(ip: Inproc) -> None:
    p = ip.pipeline("cancel-me", [P("A", gate="never"), P("B", "A")])
    run = ip.run(p)
    wait_until(lambda: probes.calls("A"), 10, 0.02, "A to start")
    out = ip.svc.cancel(ip.admin, run["id"], "stop it")
    assert out["state"] in ("cancel_requested", "cancelled")
    done = ip.wait(run["id"])
    acts = ip.acts(run["id"])
    assert done["state"] == "cancelled" and "stop it" in (done["cancel_reason"] or "")
    assert acts["A"][0]["state"] == "cancelled" and "B" not in acts or acts["B"][0]["state"] in ("skipped", "cancelled")
    assert probes.calls("B") == []
    probes.gate("never").set()


def test_cancel_interrupts_wait_and_webhook_wait_promptly(ip: Inproc) -> None:
    """A cancelled run must not sit in cancel_requested for the whole cancel grace period just because it is waiting."""
    p = ip.pipeline("cancel-waiters", [{"name": "W", "type": "wait", "typeProperties": {"waitTimeInSeconds": 3600}},
                                       {"name": "H", "type": "webhook_wait", "typeProperties": {"timeoutSeconds": 3600}}])
    run = ip.run(p)
    wait_until(lambda: {a["state"] for name, v in ip.acts(run["id"]).items() for a in v} == {"running"} and len(ip.acts(run["id"])) == 2, 10, 0.05, "both waiters to run")
    ip.svc.cancel(ip.admin, run["id"], "enough waiting")
    done = ip.wait(run["id"], timeout=10)
    acts = ip.acts(run["id"])
    assert done["state"] == "cancelled" and acts["W"][0]["state"] == "cancelled" and acts["H"][0]["state"] == "cancelled"


def test_cancel_queued_run_and_idempotent_cancel(ip: Inproc) -> None:
    p = ip.pipeline("one-at-a-time", [P("A", gate="hold")], settings={"concurrency": 1, "queuePolicy": "queue"})
    first, second = ip.run(p), ip.run(p)
    wait_until(lambda: probes.calls("A"), 10, 0.02, "first to start")
    assert ip.get(second["id"])["state"] == "queued"
    ip.svc.cancel(ip.admin, second["id"])
    assert ip.wait(second["id"])["state"] == "cancelled"
    ip.svc.cancel(ip.admin, second["id"])                         # cancelling a finished run is a no-op, not an error
    probes.gate("hold").set()
    assert ip.wait(first["id"])["state"] == "succeeded"
    assert len(probes.calls("A")) == 1


def test_cancel_propagates_to_for_each_iterations(ip: Inproc) -> None:
    p = ip.pipeline("cancel-foreach", [{"name": "F", "type": "for_each", "typeProperties": {"items": "@range(0, 6)", "batchCount": 3, "activities": [P("W", gate="never2")]}}])
    run = ip.run(p)
    wait_until(lambda: len(probes.calls("W")) == 3, 10, 0.02, "three iterations")
    ip.svc.cancel(ip.admin, run["id"])
    assert ip.wait(run["id"])["state"] == "cancelled"
    assert len(probes.calls("W")) == 3
    assert {a["state"] for a in ip.acts(run["id"])["W"]} == {"cancelled"}


# ------------------------------------------------------------------------------------------------------------------------------ resume / re-run
def _flaky_pipeline(ip: Inproc, name: str = "flaky") -> dict[str, Any]:
    return ip.pipeline(name, [P("A", value="a"), P("B", "A", fail="@pipeline().parameters.breakB"), P("C", "B", value="c"), P("D", "A", value="d")],
                       parameters={"breakB": {"type": "bool", "default": True}})


def test_resume_from_failure_reuses_successful_activities(ip: Inproc) -> None:
    p = _flaky_pipeline(ip)
    first = ip.wait(ip.run(p)["id"])
    assert first["state"] == "failed"
    assert (len(probes.calls("A")), len(probes.calls("D")), len(probes.calls("C"))) == (1, 1, 0)
    # fix the failure cause (a data fix, not a parameter) by editing the pipeline so B passes - the resume keeps the first run's definition snapshot by default,
    # so resume with the latest definition
    ip.ctx.service("items").update(ip.admin, p["id"], __import__("olcf.core.models", fromlist=["ItemUpdate"]).ItemUpdate(
        definition={"schema": "olcf.pipeline/1", "parameters": {"breakB": {"type": "bool", "default": True}},
                    "activities": [P("A", value="a"), P("B", "A", fail=False), P("C", "B", value="c"), P("D", "A", value="d")]}), None)
    second = ip.svc.resume(ip.admin, first["id"], latest_definition=True)
    assert second["resumed_from"] == first["id"] and second["group_id"] == first["group_id"] and second["id"] != first["id"]
    done = ip.wait(second["id"])
    acts = ip.acts(done["id"])
    assert done["state"] == "succeeded", (done, states(acts))
    assert acts["A"][0]["reused_from"] and acts["D"][0]["reused_from"]
    assert not acts["B"][0].get("reused_from") and not acts["C"][0].get("reused_from")
    assert len(probes.calls("A")) == 1 and len(probes.calls("D")) == 1                 # not executed again
    assert len(probes.calls("B")) == 2 and len(probes.calls("C")) == 1
    assert acts["A"][0]["output"]["value"] == "a"                                       # the reused result is still available to expressions


def test_resume_with_rerun_list_and_full_rerun(ip: Inproc) -> None:
    p = ip.pipeline("rerun", [P("A", value="a"), P("B", "A", value="b"), P("C", "B", value="c")])
    first = ip.wait(ip.run(p)["id"])
    assert first["state"] == "succeeded"
    r2 = ip.wait(ip.svc.resume(ip.admin, first["id"], rerun=["B"])["id"])
    assert r2["state"] == "succeeded"
    assert len(probes.calls("A")) == 1 and len(probes.calls("B")) == 2 and len(probes.calls("C")) == 2     # B and everything depending on it ran again
    r3 = ip.wait(ip.svc.resume(ip.admin, first["id"], reuse=False)["id"])
    assert r3["state"] == "succeeded" and len(probes.calls("A")) == 2


def test_changed_parameters_disable_reuse(ip: Inproc) -> None:
    p = ip.pipeline("reparam", [P("A", value="@pipeline().parameters.x")], parameters={"x": {"type": "string", "default": "one"}})
    first = ip.wait(ip.run(p)["id"])
    second = ip.wait(ip.svc.resume(ip.admin, first["id"], parameters={"x": "two"})["id"])
    assert second["state"] == "succeeded" and len(probes.calls("A")) == 2
    assert ip.act(second["id"], "A")["output"]["value"] == "two"


def test_resume_of_an_active_run_is_refused(ip: Inproc) -> None:
    p = ip.pipeline("busy", [P("A", gate="busy")])
    run = ip.run(p)
    wait_until(lambda: probes.calls("A"), 10, 0.02)
    with pytest.raises(Exception) as ei:
        ip.svc.resume(ip.admin, run["id"])
    assert "finished" in str(ei.value).lower() or "active" in str(ei.value).lower() or "running" in str(ei.value).lower()
    probes.gate("busy").set()
    ip.wait(run["id"])


# ------------------------------------------------------------------------------------------------------------------------------ concurrency policies
def test_concurrency_queue_policy_runs_serially(ip: Inproc) -> None:
    p = ip.pipeline("serial", [P("A", sleep=0.2)], settings={"concurrency": 1, "queuePolicy": "queue"})
    runs = [ip.run(p) for _ in range(3)]
    done = [ip.wait(r["id"]) for r in runs]
    assert [d["state"] for d in done] == ["succeeded"] * 3 and probes.ACTIVE["max"] == 1
    starts = [d["started_at"] for d in done]
    assert starts == sorted(starts)                                     # FIFO


def test_concurrency_skip_policy(ip: Inproc) -> None:
    p = ip.pipeline("skipper", [P("A", gate="skipgate")], settings={"concurrency": 1, "queuePolicy": "skip"})
    first = ip.run(p)
    wait_until(lambda: probes.calls("A"), 10, 0.02)
    second = ip.wait(ip.run(p)["id"])
    assert second["state"] == "skipped" and "concurrency" in (second["state_reason"] or "").lower()
    probes.gate("skipgate").set()
    assert ip.wait(first["id"])["state"] == "succeeded" and len(probes.calls("A")) == 1


def test_concurrency_cancel_previous_policy(ip: Inproc) -> None:
    p = ip.pipeline("latest-wins", [P("A", gate="lw")], settings={"concurrency": 1, "queuePolicy": "cancel_previous"})
    first = ip.run(p)
    wait_until(lambda: probes.calls("A"), 10, 0.02)
    second = ip.run(p)
    assert ip.wait(first["id"])["state"] == "cancelled"
    wait_until(lambda: len(probes.calls("A")) == 2, 10, 0.02, "the newer run to start")
    probes.gate("lw").set()
    assert ip.wait(second["id"])["state"] == "succeeded"


def test_concurrency_limit_n(ip: Inproc) -> None:
    p = ip.pipeline("two-at-a-time", [P("A", sleep=0.25)], settings={"concurrency": 2})
    runs = [ip.run(p) for _ in range(5)]
    assert all(ip.wait(r["id"])["state"] == "succeeded" for r in runs) and probes.ACTIVE["max"] == 2


# ------------------------------------------------------------------------------------------------------------------------------ child pipelines
def test_child_pipeline_outputs_and_graph(ip: Inproc) -> None:
    child = ip.pipeline("child", [P("Work", value="@pipeline().parameters.who")], parameters={"who": {"type": "string"}}, outputs={"greeting": "@concat('hi ', activity('Work').output.value)"})
    run, acts = ip.execute([{"name": "Call", "type": "child_pipeline", "typeProperties": {"pipeline": "child", "parameters": {"who": "world"}}},
                            P("After", "Call", value="@activity('Call').output.outputs.greeting")])
    assert run["state"] == "succeeded", run
    assert acts["After"][0]["output"]["value"] == "hi world"
    kids = ip.svc.children(ip.admin, run["id"])
    assert len(kids) == 1 and kids[0]["pipeline_id"] == child["id"] and kids[0]["parent_run_id"] == run["id"] and kids[0]["root_run_id"] == run["id"] and kids[0]["depth"] == 1


def test_child_failure_fails_the_parent_activity(ip: Inproc) -> None:
    ip.pipeline("child-bad", [P("Boom", fail=True)])
    run, acts = ip.execute([{"name": "Call", "type": "child_pipeline", "typeProperties": {"pipeline": "child-bad"}}])
    assert run["state"] == "failed" and "child run" in acts["Call"][0]["error"]["message"]
    assert acts["Call"][0]["error"]["details"]["child_run_id"]


def test_child_recursion_is_refused(ip: Inproc) -> None:
    ip.pipeline("loopy", [{"name": "Again", "type": "child_pipeline", "typeProperties": {"pipeline": "loopy"}}])
    p = ip.item("pipeline", "loopy-root", {"schema": "olcf.pipeline/1", "activities": [{"name": "Go", "type": "child_pipeline", "typeProperties": {"pipeline": "loopy"}}]})
    run = ip.wait(ip.run(p)["id"], timeout=30)
    assert run["state"] == "failed"
    assert "recurs" in str(ip.act(run["id"], "Go")["error"]).lower() or "depth" in str(ip.act(run["id"], "Go")["error"]).lower() or "cycle" in str(ip.act(run["id"], "Go")["error"]).lower()


def test_cancel_parent_cancels_child(ip: Inproc) -> None:
    ip.pipeline("child-wait", [P("A", gate="childgate")])
    p = ip.pipeline("parent", [{"name": "Call", "type": "child_pipeline", "typeProperties": {"pipeline": "child-wait"}}])
    run = ip.run(p)
    wait_until(lambda: probes.calls("A"), 10, 0.02)
    ip.svc.cancel(ip.admin, run["id"])
    assert ip.wait(run["id"])["state"] == "cancelled"
    kid = ip.svc.children(ip.admin, run["id"])[0]
    assert ip.wait(kid["id"])["state"] == "cancelled"


# ------------------------------------------------------------------------------------------------------------------------------ webhook callback
def test_webhook_wait_completes_on_callback_and_rejects_bad_tokens(ip: Inproc) -> None:
    p = ip.pipeline("approval", [{"name": "Approve", "type": "webhook_wait", "typeProperties": {"timeoutSeconds": 60}}, P("After", "Approve", value="@activity('Approve').output.output.decision")])
    run = ip.run(p)
    act = wait_until(lambda: (a := ip.acts(run["id"]).get("Approve", [None])[0]) and a["state"] == "running" and a, 10, 0.05, "Approve to run")
    with ip.ctx.db.connect() as conn:
        from sqlalchemy import select

        from olcf.pipelines.schema import activity_runs as A
        state = conn.execute(select(A.c.state_data).where(A.c.id == act["id"])).scalar()
    assert state["callback_url"].endswith(f"/v1/hooks/pipelines/callbacks/{act['id']}/{state['token']}")
    with pytest.raises(NotFound):
        ip.svc.deliver_callback(act["id"], "wrong-token", b'{"output": {"decision": "no"}}')
    with pytest.raises(NotFound):
        ip.svc.deliver_callback("act_doesnotexist", state["token"], b"{}")
    assert ip.get(run["id"])["state"] == "running"
    assert ip.svc.deliver_callback(act["id"], state["token"], b'{"output": {"decision": "yes"}}')["status"] == "accepted"
    done = ip.wait(run["id"])
    assert done["state"] == "succeeded" and ip.act(run["id"], "After")["output"]["value"] == "yes"
    assert ip.svc.deliver_callback(act["id"], state["token"], b'{"output": {"decision": "yes"}}')["duplicate"] is True       # a retried delivery is acknowledged
    with pytest.raises(Conflict):
        ip.svc.deliver_callback(act["id"], state["token"], b'{"output": {"decision": "other"}}')


def test_webhook_wait_failure_callback_and_timeout(ip: Inproc) -> None:
    p = ip.pipeline("approval-fail", [{"name": "Approve", "type": "webhook_wait", "typeProperties": {"timeoutSeconds": 60}}])
    run = ip.run(p)
    act = wait_until(lambda: (a := ip.acts(run["id"]).get("Approve", [None])[0]) and a["state"] == "running" and a, 10, 0.05)
    from sqlalchemy import select

    from olcf.pipelines.schema import activity_runs as A
    def callback_token() -> str | None:  # the row turns "running" first; the activity saves its token a moment later (visible only under load)
        with ip.ctx.db.connect() as conn:
            return (conn.execute(select(A.c.state_data).where(A.c.id == act["id"])).scalar() or {}).get("token")

    token = wait_until(callback_token, 10, 0.05)
    ip.svc.deliver_callback(act["id"], token, b'{"error": {"message": "rejected by bob", "errorCode": "Rejected"}}')
    done = ip.wait(run["id"])
    assert done["state"] == "failed" and ip.act(run["id"], "Approve")["error"]["code"] == "Rejected"
    run2 = ip.wait(ip.run(ip.pipeline("approval-timeout", [{"name": "Approve", "type": "webhook_wait", "typeProperties": {"timeoutSeconds": 1}}]))["id"])
    assert run2["state"] == "failed" and "timed out" in str(ip.act(run2["id"], "Approve")["error"]).lower()


# ------------------------------------------------------------------------------------------------------------------------------ graph, io, events
def test_run_graph_has_nodes_edges_timings_and_critical_path(ip: Inproc) -> None:
    run, _ = ip.execute([P("A", sleep=0.1), P("B", "A", sleep=0.2), P("C", "A", sleep=0.05), P("D", "B", "C"),
                         {"name": "F", "type": "for_each", "dependsOn": ["D"], "typeProperties": {"items": "@range(0, 3)", "activities": [P("W", sleep=0.02)]}}])
    g = ip.svc.graph(ip.admin, run["id"])
    nodes = {n["id"]: n for n in g["nodes"]}
    assert set(nodes) == {"A", "B", "C", "D", "F", "F/W"}
    assert all(n["state"] == "succeeded" for n in nodes.values()) and nodes["F/W"]["instances"] == 3 and nodes["F/W"]["parent"] == "F"
    assert nodes["B"]["duration_ms"] >= 190 and nodes["A"]["started_at"] <= nodes["B"]["started_at"]
    edges = {(e["from"], e["to"]) for e in g["edges"]}
    assert {("A", "B"), ("A", "C"), ("B", "D"), ("C", "D"), ("D", "F")} <= edges
    assert g["counts"]["succeeded"] == 8 and len(g["timeline"]) == 8
    assert g["critical_path_ms"] >= 300                                    # A -> B -> D -> F is the longest chain
    assert g["run"]["id"] == run["id"]


def test_secrets_are_redacted_in_inputs_outputs_and_errors(ip: Inproc) -> None:
    secret = "supersecret-token-value-1234567890"  # noqa: S105
    run, acts = ip.execute([P("A", value={"Authorization": f"Bearer {secret}", "password": "hunter2hunter2", "note": "fine"}),
                            P("B", "A", fail=True, secret=f"password={secret}")])
    blob = repr(acts) + repr(run) + repr(ip.svc.graph(ip.admin, run["id"], include_io=True))
    assert secret not in blob and "hunter2hunter2" not in blob
    assert "fine" in blob
    # logs of the engine do not carry it either
    assert acts["A"][0]["output"]["value"]["note"] == "fine"


def test_secure_output_is_withheld(ip: Inproc) -> None:
    run, acts = ip.execute([P("A", value="visible?", policy={"secureOutput": True}), P("B", "A", value="@activity('A').output.value")])
    assert acts["A"][0]["output"] == {"secure": True}
    assert acts["B"][0]["output"]["value"] == "visible?"                   # downstream activities still receive it


def test_events_metrics_and_lineage(ip: Inproc) -> None:
    good, _ = ip.execute([P("A")])
    bad, _ = ip.execute([P("A", fail=True)])
    ev = {e["type"] for e in ip.ctx.events.list(after_seq=0, limit=500, type_prefix="pipeline.")}
    assert {"pipeline.run.queued", "pipeline.run.started", "pipeline.run.succeeded", "pipeline.run.failed"} <= ev
    text = ip.metrics.render_prometheus()
    assert 'olcf_pipeline_runs_total{pipeline=' in text and 'state="succeeded"' in text and 'state="failed"' in text
    assert "olcf_pipeline_run_duration_seconds_bucket" in text and good["id"] != bad["id"]
    assert "olcf_pipeline_activity" in text


def test_leader_only_execution(ip_idle: Inproc) -> None:
    x = ip_idle
    x.ctx.leader.release()
    x.start()
    p = x.pipeline("standby", [P("A")])
    run = x.run(p)
    time.sleep(0.6)
    assert x.get(run["id"])["state"] == "queued" and probes.calls("A") == []
    x.ctx.leader.acquire()
    assert x.wait(run["id"])["state"] == "succeeded"


def test_definition_snapshot_is_immutable_for_a_run(ip: Inproc) -> None:
    p = ip.pipeline("snap", [P("A", gate="snapgate"), P("B", "A", value="old")])
    run = ip.run(p)
    wait_until(lambda: probes.calls("A"), 10, 0.02)
    from olcf.core.models import ItemUpdate
    ip.ctx.service("items").update(ip.admin, p["id"], ItemUpdate(definition={"schema": "olcf.pipeline/1", "activities": [P("A", gate="snapgate"), P("B", "A", value="NEW")]}), None)
    probes.gate("snapgate").set()
    ip.wait(run["id"])
    assert ip.act(run["id"], "B")["output"]["value"] == "old"
    assert ip.wait(ip.run(p)["id"])["state"] == "succeeded" and probes.calls("B")[-1]["value"] == "NEW"
