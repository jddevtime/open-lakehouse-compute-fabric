# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""HTTP end-to-end tests of the orchestration API against a live control plane (uvicorn in a thread; foundation + compute + pipelines + observability modules) with CTRL's fake
worker agents: items, validation, runs, graph, cancel / resume, job-backed activities, triggers, schedules, public hooks, webhooks, alerts, metrics and authorisation.
With ``OLCF_TEST_POSTGRES_URL`` set the same tests run on PostgreSQL."""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from typing import Any

from tests.orch.orch_support import JobPlan, OrchStack, wait_until


def A(name: str, type_: str, *deps: Any, policy: dict[str, Any] | None = None, **props: Any) -> dict[str, Any]:
    a: dict[str, Any] = {"name": name, "type": type_, "typeProperties": props}
    if deps:
        a["dependsOn"] = list(deps)
    if policy:
        a["policy"] = policy
    return a


def sign(secret: str, body: bytes, ts: int | None = None) -> dict[str, str]:
    ts = int(time.time()) if ts is None else ts
    mac = hmac.new(secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
    return {"X-OLCF-Timestamp": str(ts), "X-OLCF-Signature": "sha256=" + mac, "Content-Type": "application/json"}


# ------------------------------------------------------------------------------------------------------------------------------ discovery + validation
def test_activity_catalogue_schema_and_validation(stack: OrchStack) -> None:
    h = stack.admin
    types = stack.http.get("/v1/pipelines/activity-types", headers=h).json()["items"]
    names = {t["type"] for t in types}
    assert {"wait", "fail", "set_variable", "if", "switch", "for_each", "until", "child_pipeline", "webhook_wait", "http", "sql", "copy", "notebook", "spark_job", "script"} <= names
    assert all(t["title"] and t["description"] and t["properties_schema"] for t in types)
    schema = stack.http.get("/v1/pipelines/schema", headers=h).json()
    assert schema["properties"]["activities"] and "$schema" in schema
    ok = stack.http.post("/v1/pipelines/validate", headers=h, json={"definition": {"schema": "olcf.pipeline/1", "activities": [A("W", "wait", waitTimeInSeconds=1)]}}).json()
    assert ok["valid"] is True and ok["errors"] == [] and ok["graph"]["nodes"]
    bad = stack.http.post("/v1/pipelines/validate", headers=h, json={"definition": {"schema": "olcf.pipeline/1", "activities": [A("W", "wait", "Missing", waitTimeInSeconds=1), A("W", "wait", waitTimeInSeconds=1)]}}).json()
    assert bad["valid"] is False and {e["code"] for e in bad["errors"]} == {"duplicate_name", "unknown_dependency"} and all(e["path"] for e in bad["errors"])
    unknown = stack.http.post("/v1/pipelines/validate", headers=h, json={"definition": {"schema": "olcf.pipeline/1", "activities": [A("X", "nonsense")]}}).json()
    assert unknown["valid"] is False and unknown["errors"][0]["path"].startswith("activities[0]")
    yaml_text = "schema: olcf.pipeline/1\nactivities:\n  - name: A\n    type: wait\n    typeProperties: {waitTimeInSeconds: 1}\n"
    assert stack.http.post("/v1/pipelines/validate", headers=h, json={"text": yaml_text, "format": "yaml"}).json()["valid"] is True
    assert stack.http.post("/v1/pipelines/validate", headers=h, json={"text": "{not json", "format": "json"}).json()["valid"] is False
    assert stack.http.get("/v1/pipelines/schema").status_code == 401


def test_pipeline_item_lifecycle_through_the_items_api(stack: OrchStack, ws: str) -> None:
    h = stack.admin
    p = stack.pipeline(ws, "lifecycle", [A("W", "wait", waitTimeInSeconds=0)])
    assert p["type"] == "pipeline" and p["definition"]["activities"][0]["name"] == "W"
    bad = stack.http.post(f"/v1/workspaces/{ws}/items", headers=h, json={"type": "pipeline", "name": "broken", "definition": {"schema": "olcf.pipeline/1", "activities": [A("X", "no_such_type")]}})
    assert bad.status_code == 422 and "activities[0]" in bad.text                              # the definition is validated on save, with the path of the problem
    g = stack.http.get(f"/v1/pipelines/{p['id']}/definition", headers=h)
    assert g.status_code == 200 and g.json()["validation"]["valid"] is True and g.json()["graph"]["nodes"][0]["name"] == "W"
    y = stack.http.get(f"/v1/pipelines/{p['id']}/definition", headers=h, params={"format": "yaml"})
    assert y.status_code == 200 and "activities:" in y.text and y.headers["content-type"].startswith("application/yaml")
    got = stack.http.get(f"/v1/items/{p['id']}", headers=h)
    et = got.headers["ETag"]
    upd = {"definition": {"schema": "olcf.pipeline/1", "activities": [A("W", "wait", waitTimeInSeconds=0), A("F", "fail", "W", message="x")]}}
    stale = stack.http.put(f"/v1/items/{p['id']}", headers={**h, "If-Match": '"stale"'}, json=upd)
    assert stale.status_code in (409, 412)
    ok = stack.http.put(f"/v1/items/{p['id']}", headers={**h, "If-Match": et}, json=upd)
    assert ok.status_code == 200 and len(ok.json()["definition"]["activities"]) == 2 and ok.headers["ETag"] != et
    invalid = stack.http.put(f"/v1/items/{p['id']}", headers=h, json={"definition": {"schema": "olcf.pipeline/1", "activities": [A("F", "fail", "Nope", message="x")]}})
    assert invalid.status_code == 422 and "unknown_dependency" in invalid.text
    assert stack.http.get(f"/v1/pipelines/{ws}/definition", headers=h).status_code in (404, 400)       # not a pipeline id
    assert stack.http.delete(f"/v1/items/{p['id']}", headers=h).status_code == 204
    assert stack.http.post(f"/v1/pipelines/{p['id']}/runs", headers=h, json={}).status_code in (404, 409)         # a deleted pipeline cannot be run


# ------------------------------------------------------------------------------------------------------------------------------ runs
def test_run_lifecycle_graph_activities_and_listing(stack: OrchStack, ws: str) -> None:
    h = stack.admin
    p = stack.pipeline(ws, "run-api", [A("A", "set_variable", variable="v", value="@pipeline().parameters.who"), A("B", "wait", "A", waitTimeInSeconds=0)],
                       parameters={"who": {"type": "string", "default": "world"}}, variables={"v": {"type": "string", "default": ""}}, outputs={"hello": "@concat('hi ', variables('v'))"})
    r = stack.http.post(f"/v1/pipelines/{p['id']}/runs", headers={**h, "Idempotency-Key": "run-api-1"}, json={"parameters": {"who": "orch"}})
    assert r.status_code == 202 and r.headers["Location"] == f"/v1/pipeline-runs/{r.json()['id']}"
    run_id = r.json()["id"]
    again = stack.http.post(f"/v1/pipelines/{p['id']}/runs", headers={**h, "Idempotency-Key": "run-api-1"}, json={"parameters": {"who": "orch"}})
    assert again.json()["id"] == run_id                                                         # replay with the same key returns the original run
    done = stack.wait_run(run_id)
    assert done["state"] == "succeeded" and done["outputs"] == {"hello": "hi orch"} and done["pipeline_id"] == p["id"] and done["parameters"] == {"who": "orch"}
    graph = stack.http.get(f"/v1/pipeline-runs/{run_id}/graph", headers=h).json()
    assert {n["name"] for n in graph["nodes"]} == {"A", "B"} and [(e["from"], e["to"]) for e in graph["edges"]] == [("A", "B")]
    assert graph["run"]["id"] == run_id and [t["node"] for t in graph["timeline"]] == ["A", "B"]
    assert all(n["state"] == "succeeded" for n in graph["nodes"]) and graph["critical_path_ms"] is not None and graph["counts"] == {"succeeded": 2}
    acts = stack.http.get(f"/v1/pipeline-runs/{run_id}/activities", headers=h, params={"include_io": "true"}).json()["items"]
    assert [a["name"] for a in acts] == ["A", "B"] and acts[0]["input"] is not None and acts[0]["state"] == "succeeded"
    one = stack.http.get(f"/v1/pipeline-runs/{run_id}/activities/{acts[0]['id']}", headers=h)
    assert one.status_code == 200 and one.json()["name"] == "A"
    assert stack.http.get(f"/v1/pipeline-runs/{run_id}/children", headers=h).json()["items"] == []
    lst = stack.http.get("/v1/pipeline-runs", headers=h, params={"pipeline_id": p["id"], "state": "succeeded"}).json()
    assert [x["id"] for x in lst["items"]] == [run_id]
    per = stack.http.get(f"/v1/pipelines/{p['id']}/runs", headers=h).json()
    assert [x["id"] for x in per["items"]] == [run_id]
    assert stack.http.get("/v1/pipeline-runs/run_nope", headers=h).status_code == 404
    bad = stack.http.post(f"/v1/pipelines/{p['id']}/runs", headers=h, json={"parameters": {"who": 5, "unknown": 1}})
    assert bad.status_code in (400, 422)                                                        # parameters are validated against the declaration
    rr = stack.http.post(f"/v1/pipeline-runs/{run_id}/rerun", headers=h, json={})
    assert rr.status_code == 202 and rr.json()["id"] != run_id and rr.json()["group_id"] == done["group_id"]
    assert stack.wait_run(rr.json()["id"])["state"] == "succeeded"


def test_cancel_and_resume_through_the_api(stack: OrchStack, ws: str) -> None:
    h = stack.admin
    p = stack.pipeline(ws, "cancel-api", [A("Slow", "wait", waitTimeInSeconds=60), A("After", "wait", "Slow", waitTimeInSeconds=0)])
    run_id = stack.start_run(p["id"])["id"]
    wait_until(lambda: stack.run(run_id)["state"] == "running", 15, 0.1, "the run to start")
    c = stack.http.post(f"/v1/pipeline-runs/{run_id}/cancel", headers=h, json={"reason": "operator said so"})
    assert c.status_code == 202
    done = stack.wait_run(run_id)
    assert done["state"] == "cancelled" and "operator said so" in (done["state_reason"] or "")
    assert stack.http.post(f"/v1/pipeline-runs/{run_id}/cancel", headers=h, json={}).status_code in (200, 202)       # idempotent
    f = stack.pipeline(ws, "resume-api", [A("Ok", "wait", waitTimeInSeconds=0), A("Bad", "fail", "Ok", message="boom", errorCode="E_BOOM")])
    failed = stack.wait_run(stack.start_run(f["id"])["id"])
    assert failed["state"] == "failed"
    res = stack.http.post(f"/v1/pipeline-runs/{failed['id']}/resume", headers={**h, "Idempotency-Key": "resume-1"}, json={})
    assert res.status_code == 202 and res.json()["group_id"] == failed["group_id"]
    again = stack.http.post(f"/v1/pipeline-runs/{failed['id']}/resume", headers={**h, "Idempotency-Key": "resume-1"}, json={})
    assert again.json()["id"] == res.json()["id"]
    second = stack.wait_run(res.json()["id"])
    assert second["state"] == "failed" and stack.activities(second["id"])["Ok"]["state"] == "succeeded"
    assert stack.http.post(f"/v1/pipeline-runs/{run_id}/resume", headers=h, json={}).status_code in (202, 409)


def test_child_pipeline_runs_are_linked(stack: OrchStack, ws: str) -> None:
    child = stack.pipeline(ws, "child-api", [A("C", "set_variable", variable="x", value="@pipeline().parameters.n")], parameters={"n": {"type": "int", "default": 1}}, variables={"x": {"type": "int", "default": 0}},
                           outputs={"doubled": "@mul(variables('x'), 2)"})
    parent = stack.pipeline(ws, "parent-api", [A("Call", "child_pipeline", pipeline="child-api", parameters={"n": 21})], outputs={"r": "@activity('Call').output.outputs.doubled"})
    run = stack.wait_run(stack.start_run(parent["id"])["id"])
    assert run["state"] == "succeeded" and run["outputs"] == {"r": 42}, run
    kids = stack.http.get(f"/v1/pipeline-runs/{run['id']}/children", headers=stack.admin).json()["items"]
    assert len(kids) == 1 and kids[0]["pipeline_id"] == child["id"] and kids[0]["parent_run_id"] == run["id"] and kids[0]["state"] == "succeeded"
    graph = stack.http.get(f"/v1/pipeline-runs/{run['id']}/graph", headers=stack.admin).json()
    call = next(n for n in graph["nodes"] if n["name"] == "Call")
    assert call["child_runs"] == [{"run_id": kids[0]["id"], "pipeline": "child-api", "state": "succeeded"}]


# ------------------------------------------------------------------------------------------------------------------------------ job-backed activities (fake worker agent)
def test_script_activity_runs_a_job_on_a_worker_and_passes_results(fleet: OrchStack, ws: str) -> None:
    agent = fleet.agents[0]
    agent.job_plan = lambda spec: JobPlan(duration=0.3, result={"rows": 7, "label": "done"}, logs=["hello from the fake worker\n"])
    p = fleet.pipeline(ws, "script-api", [A("S", "script", code="print('hi')", parameters={"n": "@pipeline().parameters.n"}, resources={"driver_cores": 1, "driver_memory_mb": 512}),
                                          A("After", "set_variable", "S", variable="v", value="@string(activity('S').output.state)")],
                       parameters={"n": {"type": "int", "default": 1}}, variables={"v": {"type": "string", "default": ""}}, outputs={"state": "@variables('v')", "rows": "@activity('S').output.result.rows"})
    run = fleet.wait_run(fleet.start_run(p["id"], {"n": 5})["id"], timeout=60)
    assert run["state"] == "succeeded", (run["error"], {k: v["error"] for k, v in fleet.activities(run["id"]).items()})
    s = fleet.activities(run["id"])["S"]
    out = s["output"]
    assert out["state"] == "succeeded" and out["jobId"] and out["result"] == {"rows": 7, "label": "done"}
    assert run["outputs"] == {"state": "succeeded", "rows": 7}
    job = fleet.job(out["jobId"])
    assert job["origin"]["kind"] == "pipeline" and job["state"] == "succeeded" and job["parameters"] == {"n": 5} and job["kind"] == "script"
    assert s["job_ids"] == [out["jobId"]] and s["links"]["jobs"] == [f"/v1/jobs/{out['jobId']}"] and "hello from the fake worker" in fleet.logs(out["jobId"])
    (cmd,) = [c for c in agent.commands_of("run_job") if c["payload"]["spec"]["job_id"] == out["jobId"]]
    assert cmd["payload"]["source"]["content"] == "print('hi')" and cmd["payload"]["spec"]["parameters"] == {"n": 5}


def test_job_failure_retry_and_cancellation_propagate(fleet: OrchStack, ws: str) -> None:
    agent = fleet.agents[0]
    calls: list[int] = []

    def plan(spec: dict[str, Any]) -> JobPlan:
        calls.append(1)
        if len(calls) == 1:
            return JobPlan(outcome="fail", duration=0.1, failure={"category": "user_code", "message": "NameError: boom", "retryable": True}, exit_code=1)
        return JobPlan(duration=0.2)

    agent.job_plan = plan
    run = fleet.execute(ws, [A("S", "script", code="raise SystemExit(1)", policy={"retries": 2, "retryBackoffSeconds": 0})], timeout=60)
    assert run["run"]["state"] == "succeeded" and run["acts"]["S"]["attempt"] == 2 and len(calls) == 2
    assert len(run["acts"]["S"]["attempts"]) == 2 and run["acts"]["S"]["attempts"][0]["state"] == "failed"
    agent.job_plan = lambda spec: JobPlan(outcome="fail", duration=0.1, failure={"category": "user_code", "message": "always", "retryable": False}, exit_code=2)
    bad = fleet.execute(ws, [A("S", "script", code="x", policy={"retries": 3, "retryBackoffSeconds": 0}), A("N", "wait", "S", waitTimeInSeconds=0)], timeout=60)
    assert bad["run"]["state"] == "failed" and bad["acts"]["S"]["state"] == "failed" and bad["acts"]["N"]["state"] == "skipped"
    assert "always" in json.dumps(bad["acts"]["S"]["error"])
    agent.job_plan = lambda spec: JobPlan(outcome="hang")
    p = fleet.pipeline(ws, "cancel-job", [A("S", "script", code="while True: pass")])
    run_id = fleet.start_run(p["id"])["id"]
    job_id = wait_until(lambda: (a := fleet.activities(run_id).get("S")) and a["job_ids"] and a["job_ids"][0], 20, 0.1, "the job to be submitted")
    wait_until(lambda: fleet.job(job_id)["state"] == "running", 20, 0.1, "the job to run")
    assert fleet.http.post(f"/v1/pipeline-runs/{run_id}/cancel", headers=fleet.admin, json={}).status_code == 202
    assert fleet.wait_run(run_id)["state"] == "cancelled"
    assert fleet.wait_job(job_id)["state"] == "cancelled"                                      # cancelling the run cancels the job it started


def test_script_without_a_worker_waits_in_the_queue_then_runs(stack: OrchStack, ws: str) -> None:
    stack.retire_agents()
    p = stack.pipeline(ws, "queued-job", [A("S", "script", code="print(1)")])
    run_id = stack.start_run(p["id"])["id"]
    job_id = wait_until(lambda: (a := stack.activities(run_id).get("S")) and a["job_ids"] and a["job_ids"][0], 20, 0.1, "the job to be submitted")
    time.sleep(1)
    assert stack.job(job_id)["state"] in ("queued", "accepted", "validating") and stack.run(run_id)["state"] == "running"
    agent = stack.agent("late-agent")
    stack.wait_worker(agent)
    try:
        assert stack.wait_run(run_id, timeout=60)["state"] == "succeeded"
    finally:
        stack.retire_agents()


def test_activity_for_a_missing_item_fails_clearly(fleet: OrchStack, ws: str) -> None:
    run = fleet.execute(ws, [A("N", "notebook", notebook="no-such-notebook", policy={"retries": 2, "retryBackoffSeconds": 0})])
    assert run["run"]["state"] == "failed" and run["acts"]["N"]["error"]["category"] == "configuration" and run["acts"]["N"]["attempt"] == 1
    assert "no-such-notebook" in run["acts"]["N"]["error"]["message"]


# ------------------------------------------------------------------------------------------------------------------------------ webhook_wait callbacks over HTTP
def test_webhook_wait_callback_over_http(stack: OrchStack, ws: str) -> None:
    p = stack.pipeline(ws, "callback-api", [A("Wait", "webhook_wait", timeoutSeconds=120), A("After", "set_variable", "Wait", variable="v", value="@string(activity('Wait').output.output.answer)")],
                       variables={"v": {"type": "string", "default": ""}}, outputs={"v": "@variables('v')"})
    run_id = stack.start_run(p["id"])["id"]
    act = wait_until(lambda: (a := stack.activities(run_id).get("Wait")) and a["state"] == "running" and a.get("callback_url") and a, 20, 0.1, "the waiter to run")
    url = act["callback_url"]
    assert url.startswith(stack.settings.base_url) and f"/v1/hooks/pipelines/callbacks/{act['id']}/" in url
    path = url[len(stack.settings.base_url):]
    anon = stack.http                                                                           # the callback needs no bearer token: the token in the path is the credential
    assert anon.post(path + "x", json={"output": {"answer": "no"}}).status_code == 404
    assert anon.post(f"/v1/hooks/pipelines/callbacks/act_nope/{path.rsplit('/', 1)[1]}", json={}).status_code == 404
    assert anon.post(path, content=b"x" * (300 * 1024)).status_code == 413
    assert anon.post(path, content=b"not json", headers={"Content-Type": "application/json"}).status_code == 400
    assert stack.run(run_id)["state"] == "running"
    ok = anon.post(path, json={"output": {"answer": "yes"}})
    assert ok.status_code == 202 and ok.json()["status"] == "accepted"
    done = stack.wait_run(run_id)
    assert done["state"] == "succeeded" and done["outputs"] == {"v": "yes"}
    dup = anon.post(path, json={"output": {"answer": "yes"}})
    assert dup.status_code == 202 and dup.json()["duplicate"] is True
    assert anon.post(path, json={"output": {"answer": "different"}}).status_code == 409
    finished = stack.activities(run_id)["Wait"]
    assert "callback_url" not in finished                                                       # only shown while the activity waits


def test_callback_url_is_hidden_from_users_who_cannot_run_pipelines(stack: OrchStack, ws: str) -> None:
    p = stack.pipeline(ws, "callback-hidden", [A("Wait", "webhook_wait", timeoutSeconds=60)])
    run_id = stack.start_run(p["id"])["id"]
    wait_until(lambda: (a := stack.activities(run_id).get("Wait")) and a["state"] == "running" and a.get("callback_url") and a, 20, 0.1, "the waiter to run")
    viewer = stack.user("Viewer", ws)
    seen = stack.http.get(f"/v1/pipeline-runs/{run_id}/activities", headers=viewer)
    assert seen.status_code == 200 and "callback_url" not in seen.text
    assert stack.http.post(f"/v1/pipeline-runs/{run_id}/cancel", headers=viewer, json={}).status_code == 403
    stack.http.post(f"/v1/pipeline-runs/{run_id}/cancel", headers=stack.admin, json={})
    assert stack.wait_run(run_id)["state"] == "cancelled"


# ------------------------------------------------------------------------------------------------------------------------------ triggers + schedules over HTTP
def test_webhook_trigger_over_http_with_signature_replay_size_and_rate_limit(stack: OrchStack, ws: str) -> None:
    h = stack.admin
    stack.pipeline(ws, "hook-target", [A("A", "set_variable", variable="v", value="@pipeline().parameters.n")], parameters={"n": {"type": "int", "default": 0}}, variables={"v": {"type": "int", "default": 0}},
                   outputs={"n": "@variables('v')"})
    created = stack.http.post("/v1/triggers", headers=h, json={"workspace_id": ws, "name": "inbound", "type": "webhook", "target": {"type": "pipeline", "ref": "hook-target"},
                                                              "config": {"payload_mapping": {"n": "$.count"}, "rate_limit_per_minute": 6}})
    assert created.status_code == 201, created.text
    t = created.json()
    secret, url = t["credentials"]["secret"], t["credentials"]["url"]
    assert url.endswith(f"/v1/hooks/pipelines/{t['id']}") and secret not in json.dumps({k: v for k, v in t.items() if k != "credentials"})
    path = url[len(stack.settings.base_url):]
    body = json.dumps({"count": 9}).encode()
    anon = stack.http
    assert anon.post(path, content=body).status_code == 401                                     # unsigned
    assert anon.post(path, content=body, headers=sign("wrong-secret-wrong-secret-0000", body)).status_code == 401
    assert anon.post(path, content=body, headers=sign(secret, body, ts=int(time.time()) - 3600)).status_code == 401
    signed = sign(secret, body)
    r = anon.post(path, content=body, headers=signed)
    assert r.status_code == 202 and r.json()["status"] == "accepted"
    assert stack.wait_run(r.json()["run_id"])["outputs"] == {"n": 9}
    assert anon.post(path, content=body, headers=signed).status_code == 409                    # replay
    big = b"x" * (2 * 1024 * 1024)
    assert anon.post(path, content=big, headers=sign(secret, big)).status_code == 413
    assert anon.post(path + "zzz", content=body, headers=signed).status_code == 404
    codes = []
    for i in range(10):
        payload = json.dumps({"count": i}).encode()
        codes.append(anon.post(path, content=payload, headers=sign(secret, payload, ts=int(time.time()) - i)).status_code)
    assert 429 in codes and codes[0] == 202
    fires = stack.http.get(f"/v1/triggers/{t['id']}/runs", headers=h).json()["items"]
    assert fires and {f["status"] for f in fires} <= {"started", "failed", "skipped"}
    rot = stack.http.post(f"/v1/triggers/{t['id']}/rotate-secret", headers=h).json()
    assert rot["secret"] != secret
    assert stack.http.post(f"/v1/triggers/{t['id']}/disable", headers=h).json()["enabled"] is False
    assert anon.post(path, content=body, headers=sign(rot["secret"], body, ts=int(time.time()) - 20)).status_code == 404


def test_trigger_crud_pause_run_now_and_authorisation(stack: OrchStack, ws: str) -> None:
    h = stack.admin
    stack.pipeline(ws, "crud-target", [A("A", "wait", waitTimeInSeconds=0)])
    base = {"workspace_id": ws, "name": "t1", "type": "webhook", "target": {"type": "pipeline", "ref": "crud-target"}}
    for bad in ({"name": ""}, {"type": "nope"}, {"target": {"type": "pipeline", "ref": "missing"}}, {"config": {"auth": "magic"}}, {"workspace_id": None}):
        assert stack.http.post("/v1/triggers", headers=h, json={**base, **bad}).status_code in (400, 404, 422)
    t = stack.http.post("/v1/triggers", headers=h, json=base).json()
    assert stack.http.get(f"/v1/triggers/{t['id']}", headers=h).json()["name"] == "t1"
    assert [x["id"] for x in stack.http.get("/v1/triggers", headers=h, params={"workspace_id": ws, "type": "webhook"}).json()["items"]] == [t["id"]]
    assert stack.http.patch(f"/v1/triggers/{t['id']}", headers=h, json={"name": "t2"}).json()["name"] == "t2"
    now = stack.http.post(f"/v1/triggers/{t['id']}/run-now", headers=h)
    assert now.status_code == 202 and stack.wait_run(now.json()["run_id"])["state"] == "succeeded"
    paused = stack.http.post(f"/v1/triggers/{t['id']}/pause", headers=h, json={"minutes": 30})
    assert paused.status_code == 200 and paused.json()["paused_until"]
    assert stack.http.post(f"/v1/triggers/{t['id']}/pause", headers=h, json={}).status_code == 422
    assert stack.http.post(f"/v1/triggers/{t['id']}/pause", headers=h, json={"minutes": -5}).status_code == 422
    assert stack.http.post(f"/v1/triggers/{t['id']}/resume", headers=h).json()["paused_until"] is None
    viewer, contrib = stack.user("Viewer", ws), stack.user("Contributor", ws)
    assert stack.http.get(f"/v1/triggers/{t['id']}", headers=viewer).status_code == 200
    assert stack.http.post(f"/v1/triggers/{t['id']}/run-now", headers=viewer).status_code == 403
    assert stack.http.patch(f"/v1/triggers/{t['id']}", headers=viewer, json={"name": "x"}).status_code == 403
    assert stack.http.delete(f"/v1/triggers/{t['id']}", headers=viewer).status_code == 403
    assert stack.http.post("/v1/triggers", headers=viewer, json={**base, "name": "v"}).status_code == 403
    assert stack.http.post(f"/v1/triggers/{t['id']}/run-now", headers=contrib).status_code == 202
    outsider = stack.user("Contributor", stack.workspace())
    assert stack.http.get(f"/v1/triggers/{t['id']}", headers=outsider).status_code in (403, 404)
    assert stack.http.delete(f"/v1/triggers/{t['id']}", headers=h).status_code == 204
    assert stack.http.get(f"/v1/triggers/{t['id']}", headers=h).status_code == 404


def test_schedule_endpoints(stack: OrchStack, ws: str) -> None:
    h = stack.admin
    stack.pipeline(ws, "sched-target", [A("A", "set_variable", variable="v", value="@trigger().scheduledTime")], variables={"v": {"type": "string", "default": ""}}, outputs={"at": "@variables('v')"})
    prev = stack.http.post("/v1/schedules/preview", headers=h, json={"cron": "30 9 * * 1-5", "timezone": "America/New_York", "count": 3, "after": "2026-03-06T00:00:00Z"})
    assert prev.status_code == 200 and prev.json()["timezone"] == "America/New_York"
    items = prev.json()["items"]
    assert [i["utc"][:19] for i in items] == ["2026-03-06T14:30:00", "2026-03-09T13:30:00", "2026-03-10T13:30:00"] and items[1]["local"].startswith("2026-03-09T09:30:00")        # DST starts 8 March
    for bad in ({"cron": "* * *"}, {"cron": "0 0 31 2 *"}, {"cron": "0 * * * *", "timezone": "Mars/Base"}, {"cron": "0 * * * *", "count": 1000}, {}):
        assert stack.http.post("/v1/schedules/preview", headers=h, json=bad).status_code in (400, 422)
    made = stack.http.post("/v1/schedules", headers=h, json={"workspace_id": ws, "name": "nightly", "definition": {"cron": "0 3 * * *", "timezone": "UTC", "target": {"type": "pipeline", "ref": "sched-target"}}})
    assert made.status_code == 201, made.text
    s = made.json()
    assert s["enabled"] is True and len(s["next_fire_times"]) == 3 and s["trigger"]["type"] == "schedule"
    assert stack.http.post("/v1/schedules", headers=h, json={"workspace_id": ws, "name": "bad", "definition": {"cron": "nope", "target": {"type": "pipeline", "ref": "sched-target"}}}).status_code == 422
    assert [x["id"] for x in stack.http.get("/v1/schedules", headers=h, params={"workspace_id": ws}).json()["items"]] == [s["id"]]
    assert stack.http.get(f"/v1/schedules/{s['id']}/next", headers=h, params={"count": 2}).json()["items"]
    dis = stack.http.post(f"/v1/schedules/{s['id']}/disable", headers=h).json()
    assert dis["enabled"] is False
    assert stack.http.post(f"/v1/schedules/{s['id']}/enable", headers=h).json()["enabled"] is True
    assert stack.http.post(f"/v1/schedules/{s['id']}/pause", headers=h, json={"minutes": 10}).json()["paused_until"]
    assert stack.http.post(f"/v1/schedules/{s['id']}/resume", headers=h).json()["paused_until"] is None
    now = stack.http.post(f"/v1/schedules/{s['id']}/run-now", headers=h)
    assert now.status_code == 202
    run = stack.wait_run(now.json()["run_id"])
    assert run["state"] == "succeeded" and run["outputs"]["at"]
    runs = stack.http.get(f"/v1/schedules/{s['id']}/runs", headers=h).json()["items"]
    assert runs and runs[0]["run_id"] == run["id"]
    viewer = stack.user("Viewer", ws)
    assert stack.http.get(f"/v1/schedules/{s['id']}", headers=viewer).status_code == 200
    assert stack.http.post(f"/v1/schedules/{s['id']}/run-now", headers=viewer).status_code == 403
    assert stack.http.post(f"/v1/schedules/{s['id']}/disable", headers=viewer).status_code == 403
    assert stack.http.post("/v1/schedules/preview", json={"cron": "* * * * *"}).status_code == 401
    assert stack.http.delete(f"/v1/items/{s['id']}", headers=h).status_code == 204
    assert stack.http.get(f"/v1/schedules/{s['id']}", headers=h).status_code == 404


# ------------------------------------------------------------------------------------------------------------------------------ Fabric import endpoint
FABRIC_DOC = {"name": "Copy from Fabric", "properties": {"activities": [
    {"name": "Wait1", "type": "Wait", "typeProperties": {"waitTimeInSeconds": 1}, "dependsOn": []},
    {"name": "Hook", "type": "WebActivity", "dependsOn": [{"activity": "Wait1", "dependencyConditions": ["Succeeded"]}],
     "typeProperties": {"url": "https://example.com/api", "method": "POST", "body": {"a": 1}}},
    {"name": "Odd", "type": "SomethingFabricOnly", "dependsOn": [{"activity": "Hook", "dependencyConditions": ["Completed"]}], "typeProperties": {"x": 1}},
], "parameters": {"p": {"type": "string", "defaultValue": "d"}}}}


def test_import_fabric_endpoint(stack: OrchStack, ws: str) -> None:
    h = stack.admin
    r = stack.http.post("/v1/pipelines/import-fabric", headers=h, json={"document": FABRIC_DOC})
    assert r.status_code == 200, r.text
    res = r.json()
    assert res["validation"]["valid"] is True and res["report"]["unsupported"] and res["report"]["unsupported"][0]["name"] == "Odd" and res["created"] is None
    types = {a["name"]: a["type"] for a in res["definition"]["activities"]}
    assert types["Wait1"] == "wait" and types["Hook"] == "http"
    strict = stack.http.post("/v1/pipelines/import-fabric", headers=h, json={"document": FABRIC_DOC, "on_unsupported": "error"})
    assert strict.status_code == 422 and "SomethingFabricOnly" in strict.text
    made = stack.http.post("/v1/pipelines/import-fabric", headers=h, json={"document": FABRIC_DOC, "workspace_id": ws, "create": True, "name": "imported-1"})
    assert made.status_code == 200 and made.json()["created"]["name"] == "imported-1"
    item = stack.http.get(f"/v1/items/{made.json()['created']['id']}", headers=h).json()
    assert item["type"] == "pipeline" and item["definition"]["activities"]
    assert stack.http.post("/v1/pipelines/import-fabric", headers=h, json={"document": FABRIC_DOC, "workspace_id": ws, "create": True, "name": "imported-1"}).status_code == 409
    assert stack.http.post("/v1/pipelines/import-fabric", headers=h, json={}).status_code == 400
    assert stack.http.post("/v1/pipelines/import-fabric", headers=h, json={"document": FABRIC_DOC, "item_map": {"a": 1}}).status_code == 422
    viewer = stack.user("Viewer", ws)
    assert stack.http.post("/v1/pipelines/import-fabric", headers=viewer, json={"document": FABRIC_DOC, "workspace_id": ws, "create": True, "name": "nope"}).status_code == 403
    assert stack.http.post("/v1/pipelines/import-fabric", json={"document": FABRIC_DOC}).status_code == 401


# ------------------------------------------------------------------------------------------------------------------------------ authorisation of runs
def test_run_and_item_authorisation(stack: OrchStack, ws: str) -> None:
    p = stack.pipeline(ws, "authz-api", [A("A", "wait", waitTimeInSeconds=0)])
    viewer, contrib, operator = stack.user("Viewer", ws), stack.user("Contributor", ws), stack.user("Operator", ws)
    other = stack.user("Contributor", stack.workspace())
    assert stack.http.post(f"/v1/pipelines/{p['id']}/runs", headers=viewer, json={}).status_code == 403
    assert stack.http.post(f"/v1/pipelines/{p['id']}/runs", headers=other, json={}).status_code in (403, 404)
    assert stack.http.post(f"/v1/pipelines/{p['id']}/runs", json={}).status_code == 401
    run_id = stack.http.post(f"/v1/pipelines/{p['id']}/runs", headers=contrib, json={}).json()["id"]
    assert stack.http.post(f"/v1/pipelines/{p['id']}/runs", headers=operator, json={}).status_code == 202              # operators may run pipelines
    stack.wait_run(run_id)
    for who, ok in ((viewer, True), (contrib, True), (operator, True), (other, False)):
        for path in (f"/v1/pipeline-runs/{run_id}", f"/v1/pipeline-runs/{run_id}/graph", f"/v1/pipeline-runs/{run_id}/activities", f"/v1/pipeline-runs/{run_id}/children"):
            got = stack.http.get(path, headers=who).status_code
            assert (got == 200) if ok else got in (403, 404), (path, got)
    assert stack.http.post(f"/v1/pipeline-runs/{run_id}/rerun", headers=viewer, json={}).status_code == 403
    assert stack.http.post(f"/v1/pipeline-runs/{run_id}/resume", headers=other, json={}).status_code in (403, 404)
    listed = stack.http.get("/v1/pipeline-runs", headers=other).json()["items"]
    assert run_id not in [x["id"] for x in listed]                                                # lists are filtered to the caller's workspaces
    assert stack.http.get("/v1/pipeline-runs", headers=viewer, params={"workspace_id": ws}).status_code == 200
    assert stack.http.get("/v1/pipeline-runs").status_code == 401
