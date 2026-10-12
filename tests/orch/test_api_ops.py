# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""HTTP end-to-end tests of outbound webhooks, alerts and metrics against a live control plane with real background loops (dispatcher, audit bridge, alert evaluation, sampler) and
CTRL's fake worker agents. Receivers are real local HTTP servers. With ``OLCF_TEST_POSTGRES_URL`` set the same tests run on PostgreSQL."""

from __future__ import annotations

import json
import time
from typing import Any

import pytest

from olcf.observability.metrics import parse_prometheus
from olcf.pipelines.webhooks import verify_signature
from tests.orch.orch_support import JobPlan, OrchStack, wait_until


def A(name: str, type_: str, *deps: Any, **props: Any) -> dict[str, Any]:
    a: dict[str, Any] = {"name": name, "type": type_, "typeProperties": props}
    if deps:
        a["dependsOn"] = list(deps)
    return a


@pytest.fixture(autouse=True)
def fast_webhooks(stack: OrchStack) -> Any:
    st = stack.ctx.service("settings")
    old = {k: st.get(k) for k in ("webhooks.backoff_base_seconds",)}
    st.set("webhooks.backoff_base_seconds", 0.2)
    yield
    for k, v in old.items():
        st.set(k, v)


def hook_body(url: str, events: list[str] | None = None, **kw: Any) -> dict[str, Any]:
    return {"name": kw.pop("name", "api-hook"), "url": url, "event_types": events or ["pipeline.run.*"], **kw}


# ------------------------------------------------------------------------------------------------------------------------------ webhooks API
def test_webhook_subscription_crud_over_http(stack: OrchStack, ws: str, httpd: Any) -> None:
    h = stack.admin
    cat = stack.http.get("/v1/webhooks/event-types", headers=h).json()
    assert {"job.state_changed", "pipeline.run.failed", "security.login_failed", "item.created"} <= {e["type"] for e in cat["items"]} and "X-OLCF-Signature" in cat["delivery"]["headers"]
    created = stack.http.post("/v1/webhooks", headers=h, json=hook_body(httpd.url + "/crud?token=URLSECRET123", workspace_id=ws, headers={"X-Team": "data"}))
    assert created.status_code == 201, created.text
    hook = created.json()
    assert hook["secret"].startswith("whsec_") and hook["has_secret"] is True and "URLSECRET123" not in json.dumps(hook) and hook["event_types"] == ["pipeline.run.*"]
    etag = created.headers["ETag"]
    got = stack.http.get(f"/v1/webhooks/{hook['id']}", headers=h)
    assert got.status_code == 200 and "secret" not in got.json() and got.headers["ETag"] == etag
    assert [x["id"] for x in stack.http.get("/v1/webhooks", headers=h, params={"workspace_id": ws}).json()["items"]] == [hook["id"]]
    assert stack.http.patch(f"/v1/webhooks/{hook['id']}", headers={**h, "If-Match": '"stale"'}, json={"name": "x"}).status_code in (409, 412)
    upd = stack.http.patch(f"/v1/webhooks/{hook['id']}", headers={**h, "If-Match": etag}, json={"name": "renamed", "event_types": ["job.*", "pipeline.*"], "max_attempts": 3})
    assert upd.status_code == 200 and upd.json()["name"] == "renamed" and upd.json()["max_attempts"] == 3
    rot = stack.http.post(f"/v1/webhooks/{hook['id']}/rotate-secret", headers=h).json()
    assert rot["secret"] != hook["secret"]
    given = stack.http.post(f"/v1/webhooks/{hook['id']}/rotate-secret", headers=h, json={"secret": "caller-chosen-secret-0123456789"}).json()
    assert given["secret"] == "caller-chosen-secret-0123456789"
    for bad in ({"url": "http://169.254.169.254/latest"}, {"url": "ftp://x"}, {"event_types": []}, {"event_types": ["webhook.test"]}, {"max_attempts": 99}, {"headers": {"Authorization": "x"}}, {"name": ""}):
        r = stack.http.post("/v1/webhooks", headers=h, json={**hook_body(httpd.url + "/x"), **bad})
        assert r.status_code == 422, (bad, r.status_code, r.text)
    assert stack.http.post("/v1/webhooks", json=hook_body(httpd.url + "/x")).status_code == 401
    assert stack.http.delete(f"/v1/webhooks/{hook['id']}", headers=h).status_code == 204
    assert stack.http.get(f"/v1/webhooks/{hook['id']}", headers=h).status_code == 404


def test_events_are_delivered_signed_and_logged(stack: OrchStack, ws: str, httpd: Any) -> None:
    h = stack.admin
    hook = stack.http.post("/v1/webhooks", headers=h, json=hook_body(httpd.url + "/events", ["pipeline.run.succeeded", "pipeline.run.failed"], workspace_id=ws)).json()
    ok = stack.pipeline(ws, "evt-ok", [A("A", "wait", waitTimeInSeconds=0)])
    bad = stack.pipeline(ws, "evt-bad", [A("F", "fail", message="intentional")])
    run_ok = stack.wait_run(stack.start_run(ok["id"])["id"])
    run_bad = stack.wait_run(stack.start_run(bad["id"])["id"])
    assert run_ok["state"] == "succeeded" and run_bad["state"] == "failed"
    wait_until(lambda: len(httpd.to("/events")) >= 2, 30, 0.2, "both events to be delivered")
    got = {hit.json()["type"]: hit for hit in httpd.to("/events")}
    assert set(got) == {"pipeline.run.succeeded", "pipeline.run.failed"}                       # started / queued events are not subscribed
    for typ, hit in got.items():
        assert verify_signature(hook["secret"], hit.headers["x-olcf-timestamp"], hit.body, hit.headers["x-olcf-signature"])
        env = hit.json()
        assert env["workspace_id"] == ws and env["data"]["run_id"] in (run_ok["id"], run_bad["id"]) and hit.headers["x-olcf-event"] == typ and hit.headers["x-olcf-attempt"] == "1"
    assert got["pipeline.run.failed"].json()["data"]["pipeline"] == "evt-bad"
    log = wait_until(lambda: (d := stack.http.get(f"/v1/webhooks/{hook['id']}/deliveries", headers=h).json()["items"]) and all(x["state"] == "succeeded" for x in d) and len(d) == 2 and d, 30, 0.2,
                     "deliveries to be recorded as succeeded")
    detail = stack.http.get(f"/v1/webhooks/deliveries/{log[0]['id']}", headers=h).json()
    assert detail["payload"]["type"] in got and detail["attempt_log"][0]["status"] == 200 and detail["attempts"] == 1
    assert hook["secret"] not in json.dumps(detail)
    only_failed = stack.http.get(f"/v1/webhooks/{hook['id']}/deliveries", headers=h, params={"event_type": "pipeline.run.failed"}).json()["items"]
    assert len(only_failed) == 1 and only_failed[0]["event_type"] == "pipeline.run.failed"
    assert stack.http.get(f"/v1/webhooks/{hook['id']}/deliveries", headers=h, params={"state": "dead"}).json()["items"] == []


def test_retries_dead_letter_and_redelivery_over_http(stack: OrchStack, ws: str, httpd: Any) -> None:
    h = stack.admin
    httpd.on("/flaky", 500, 503, 200)
    httpd.on("/gone-bad", 400)
    flaky = stack.http.post("/v1/webhooks", headers=h, json=hook_body(httpd.url + "/flaky", ["pipeline.run.succeeded"], workspace_id=ws, max_attempts=5, name="flaky")).json()
    dead = stack.http.post("/v1/webhooks", headers=h, json=hook_body(httpd.url + "/gone-bad", ["pipeline.run.succeeded"], workspace_id=ws, max_attempts=3, name="dead")).json()
    p = stack.pipeline(ws, "evt-retry", [A("A", "wait", waitTimeInSeconds=0)])
    stack.wait_run(stack.start_run(p["id"])["id"])
    d = wait_until(lambda: (x := stack.http.get(f"/v1/webhooks/{flaky['id']}/deliveries", headers=h).json()["items"]) and x[0]["state"] == "succeeded" and x[0], 40, 0.25, "the flaky delivery to succeed")
    assert d["attempts"] == 3 and [e["status"] for e in stack.http.get(f"/v1/webhooks/deliveries/{d['id']}", headers=h).json()["attempt_log"]] == [500, 503, 200]
    assert [x.headers["x-olcf-attempt"] for x in httpd.to("/flaky")] == ["1", "2", "3"] and len({x.headers["x-olcf-delivery"] for x in httpd.to("/flaky")}) == 1
    dd = wait_until(lambda: (x := stack.http.get(f"/v1/webhooks/{dead['id']}/deliveries", headers=h, params={"state": "dead"}).json()["items"]) and x[0], 40, 0.25, "the permanent failure to become dead")
    assert dd["attempts"] == 1 and len(httpd.to("/gone-bad")) == 1                              # 4xx: not retried
    httpd.on("/gone-bad", 200)
    re = stack.http.post(f"/v1/webhooks/deliveries/{dd['id']}/redeliver", headers=h)
    assert re.status_code == 202 and re.json()["state"] == "pending"
    wait_until(lambda: stack.http.get(f"/v1/webhooks/deliveries/{dd['id']}", headers=h).json()["state"] == "succeeded", 30, 0.25, "the redelivery to succeed")
    assert stack.http.post(f"/v1/webhooks/deliveries/{dd['id']}/redeliver", headers=h).status_code in (202, 400, 409)


def test_test_delivery_endpoint_reports_outcome(stack: OrchStack, ws: str, httpd: Any) -> None:
    h = stack.admin
    hook = stack.http.post("/v1/webhooks", headers=h, json=hook_body(httpd.url + "/probe", workspace_id=ws, name="probe")).json()
    t = stack.http.post(f"/v1/webhooks/{hook['id']}/test", headers=h)
    assert t.status_code == 200 and t.json()["state"] == "succeeded" and t.json()["is_test"] is True and t.json()["payload"]["type"] == "webhook.test"
    assert verify_signature(hook["secret"], httpd.to("/probe")[0].headers["x-olcf-timestamp"], httpd.to("/probe")[0].body, httpd.to("/probe")[0].headers["x-olcf-signature"])
    httpd.on("/probe", 410)
    failed = stack.http.post(f"/v1/webhooks/{hook['id']}/test", headers=h).json()
    assert failed["state"] == "dead" and "410" in failed["last_error"]
    assert stack.http.get(f"/v1/webhooks/{hook['id']}", headers=h).json()["enabled"] is True          # a manual test never disables; only real deliveries do


def test_security_events_reach_subscribers_via_the_audit_bridge(stack: OrchStack, httpd: Any) -> None:
    h = stack.admin
    hook = stack.http.post("/v1/webhooks", headers=h, json=hook_body(httpd.url + "/sec", ["security.*"], name="sec")).json()
    time.sleep(1.2)                                                                             # a subscription only sees events after its creation
    assert stack.http.post("/v1/identity/login", json={"username": "admin", "password": "definitely-wrong-password"}).status_code == 401
    hit = wait_until(lambda: [x for x in httpd.to("/sec") if x.json()["type"] == "security.login_failed"], 30, 0.25, "security.login_failed to be delivered")[0]
    env = hit.json()
    assert env["data"]["source"] == "audit" and env["data"]["resource_id"] and "definitely-wrong-password" not in hit.body.decode()
    assert verify_signature(hook["secret"], hit.headers["x-olcf-timestamp"], hit.body, hit.headers["x-olcf-signature"])


def test_webhook_authorisation(stack: OrchStack, ws: str, httpd: Any) -> None:
    h = stack.admin
    mine = stack.http.post("/v1/webhooks", headers=h, json=hook_body(httpd.url + "/a", workspace_id=ws, name="ws-hook")).json()
    org = stack.http.post("/v1/webhooks", headers=h, json=hook_body(httpd.url + "/b", name="org-hook")).json()
    viewer, contrib, wsadmin = stack.user("Viewer", ws), stack.user("Contributor", ws), stack.user("WorkspaceAdmin", ws)
    outsider = stack.user("WorkspaceAdmin", stack.workspace())
    for who in (viewer, contrib):
        assert stack.http.post("/v1/webhooks", headers=who, json=hook_body(httpd.url + "/x", workspace_id=ws)).status_code == 403
        assert stack.http.get(f"/v1/webhooks/{mine['id']}", headers=who).status_code in (403, 404)
        assert stack.http.get("/v1/webhooks", headers=who).json()["items"] == []
    created = stack.http.post("/v1/webhooks", headers=wsadmin, json=hook_body(httpd.url + "/wa", workspace_id=ws, name="by-wsadmin"))
    assert created.status_code == 201
    assert stack.http.post("/v1/webhooks", headers=wsadmin, json=hook_body(httpd.url + "/wa2", name="org-by-wsadmin")).status_code == 403        # organisation-wide needs an org-level grant
    seen = {x["id"] for x in stack.http.get("/v1/webhooks", headers=wsadmin).json()["items"]}
    assert mine["id"] in seen and org["id"] not in seen
    assert stack.http.get(f"/v1/webhooks/{mine['id']}", headers=outsider).status_code in (403, 404)
    assert stack.http.delete(f"/v1/webhooks/{mine['id']}", headers=outsider).status_code in (403, 404)
    assert stack.http.post(f"/v1/webhooks/{org['id']}/test", headers=wsadmin).status_code in (403, 404)
    assert stack.http.get("/v1/webhooks/event-types").status_code == 401


# ------------------------------------------------------------------------------------------------------------------------------ alerts API
def test_alert_defaults_rule_types_and_channel_crud(stack: OrchStack, ws: str, httpd: Any) -> None:
    h = stack.admin
    types = {t["type"]: t for t in stack.http.get("/v1/alerts/rule-types", headers=h).json()["items"]}
    assert set(types) >= {"failed_jobs", "failed_pipelines", "queue_saturation", "worker_loss", "disk_pressure", "runtime_mismatch", "auth_expiry", "control_plane_health", "fabric_handshake_stale"}
    assert types["failed_jobs"]["params"]["threshold"]["default"] == 5
    rules = stack.http.get("/v1/alerts/rules", headers=h).json()["items"]
    builtin = {r["builtin_key"]: r for r in rules if r["builtin"]}
    assert len(builtin) >= 9 and builtin["worker-lost"]["severity"] == "critical" and builtin["fabric-handshake-stale"]["enabled"] is False
    assert stack.http.delete(f"/v1/alerts/rules/{builtin['failed-jobs']['id']}", headers=h).status_code == 400
    ch = stack.http.post("/v1/alerts/channels", headers=h, json={"name": "ops-hook", "type": "webhook", "config": {"url": httpd.url + "/chan?key=CHANSECRET1"}, "secret": "channel-secret-0123456789"})
    assert ch.status_code == 201 and "CHANSECRET1" not in ch.text and ch.json()["has_secret"] is True and "channel-secret" not in ch.text
    test = stack.http.post(f"/v1/alerts/channels/{ch.json()['id']}/test", headers=h).json()
    assert test["ok"] is True and httpd.to("/chan")[0].json()["status"] == "test"
    assert verify_signature("channel-secret-0123456789", httpd.to("/chan")[0].headers["x-olcf-timestamp"], httpd.to("/chan")[0].body, httpd.to("/chan")[0].headers["x-olcf-signature"], tolerance_seconds=None)
    assert stack.http.post("/v1/alerts/channels", headers=h, json={"name": "bad", "type": "webhook", "config": {"url": "http://169.254.169.254/"}}).status_code == 422
    assert stack.http.post("/v1/alerts/channels", headers=h, json={"name": "mail", "type": "email", "config": {"to": ["x@example.com"]}}).status_code == 201
    chans = stack.http.get("/v1/alerts/channels", headers=h).json()["items"]
    assert {c["name"] for c in chans} >= {"ops-hook", "mail"}
    assert stack.http.patch(f"/v1/alerts/channels/{ch.json()['id']}", headers=h, json={"enabled": False}).json()["enabled"] is False
    assert stack.http.delete(f"/v1/alerts/channels/{ch.json()['id']}", headers=h).status_code == 204


def test_failed_jobs_rule_fires_notifies_acks_silences_and_resolves(fleet: OrchStack, ws: str, httpd: Any) -> None:
    h = fleet.admin
    agent = fleet.agents[0]
    agent.job_plan = lambda spec: JobPlan(outcome="fail", duration=0.1, failure={"category": "user_code", "message": "boom", "retryable": False}, exit_code=1)
    ch = fleet.http.post("/v1/alerts/channels", headers=h, json={"name": "jobs-hook", "type": "webhook", "config": {"url": httpd.url + "/jobs-alert"}}).json()
    rule = fleet.http.post("/v1/alerts/rules", headers=h, json={"name": "too many failed jobs", "type": "failed_jobs", "severity": "critical", "workspace_id": ws, "channels": [ch["id"]],
                                                                  "params": {"threshold": 2, "window_seconds": 300, "interval_seconds": 5}, "for_seconds": 0, "repeat_seconds": 3600})
    assert rule.status_code == 201, rule.text
    rid = rule.json()["id"]
    j1 = fleet.submit(ws, source="raise SystemExit(1)")
    fleet.wait_job(j1["id"], "failed")
    assert fleet.http.post(f"/v1/alerts/rules/{rid}/test", headers=h).json()["firing"] is False        # one failure: below the threshold
    j2 = fleet.submit(ws, source="raise SystemExit(1)")
    fleet.wait_job(j2["id"], "failed")
    assert fleet.http.post(f"/v1/alerts/rules/{rid}/test", headers=h).json()["firing"] is True
    assert fleet.http.get("/v1/alerts", headers=h, params={"rule_id": rid}).json()["items"] == []     # testing never creates alerts
    ev = fleet.http.post("/v1/alerts/evaluate", headers=h)
    assert ev.status_code == 200
    alerts = fleet.http.get("/v1/alerts", headers=h, params={"rule_id": rid, "state": "firing"}).json()["items"]
    assert len(alerts) == 1
    a = alerts[0]
    assert a["severity"] == "critical" and a["workspace_id"] == ws and a["value"] == 2.0 and "2 job(s) failed" in a["summary"] and set(a["details"]["job_ids"]) == {j1["id"], j2["id"]}
    assert a["details"]["failure_categories"] == {"user-code": 2}
    hit = wait_until(lambda: httpd.to("/jobs-alert"), 20, 0.2, "the alert notification")[0]
    body = hit.json()
    assert body["status"] == "firing" and body["alert"]["id"] == a["id"] and body["rule"]["name"] == "too many failed jobs" and hit.headers["x-olcf-event"] == "alert.firing"
    fleet.http.post("/v1/alerts/evaluate", headers=h)
    assert len(fleet.http.get("/v1/alerts", headers=h, params={"rule_id": rid}).json()["items"]) == 1 and len(httpd.to("/jobs-alert")) == 1          # de-duplicated, no re-notification
    acked = fleet.http.post(f"/v1/alerts/{a['id']}/ack", headers=h).json()
    assert acked["acknowledged_at"] and fleet.http.get(f"/v1/alerts/{a['id']}", headers=h).json()["acknowledged_by"]
    assert fleet.http.post(f"/v1/alerts/{a['id']}/silence", headers=h, json={"minutes": 5}).json()["silenced_until"]
    assert fleet.http.post(f"/v1/alerts/rules/{rid}/silence", headers=h, json={"minutes": 5}).json()["silenced_until"]
    assert fleet.http.post(f"/v1/alerts/{a['id']}/silence", headers=h, json={"minutes": -1}).status_code == 422
    # a rule change takes effect at the next evaluation: raising the threshold resolves the alert and says so
    fleet.http.patch(f"/v1/alerts/rules/{rid}", headers=h, json={"params": {"threshold": 5}})
    fleet.http.post("/v1/alerts/evaluate", headers=h)
    res = fleet.http.get(f"/v1/alerts/{a['id']}", headers=h).json()
    assert res["state"] == "resolved" and res["resolved_at"]
    viewer = fleet.user("Viewer", ws)
    assert fleet.http.get("/v1/alerts", headers=viewer, params={"rule_id": rid}).status_code == 200
    assert fleet.http.post(f"/v1/alerts/{a['id']}/ack", headers=viewer).status_code == 403
    assert fleet.http.post("/v1/alerts/rules", headers=viewer, json={"name": "x", "type": "failed_jobs", "workspace_id": ws}).status_code == 403
    assert fleet.http.post("/v1/alerts/evaluate", headers=viewer).status_code == 403
    assert fleet.http.get("/v1/alerts").status_code == 401
    fleet.http.delete(f"/v1/alerts/rules/{rid}", headers=h)


def test_worker_loss_and_control_plane_rules(stack: OrchStack, ws: str) -> None:
    h = stack.admin
    stack.retire_agents()
    agent = stack.agent("doomed-" + str(int(time.time())))
    stack.wait_worker(agent)
    rule = stack.http.post("/v1/alerts/rules", headers=h, json={"name": "worker gone", "type": "worker_loss", "severity": "critical", "params": {"grace_seconds": 10, "interval_seconds": 5}}).json()
    stack.http.post("/v1/alerts/evaluate", headers=h)
    assert stack.http.get("/v1/alerts", headers=h, params={"rule_id": rule["id"]}).json()["items"] == []
    agent.stop()
    a = wait_until(lambda: (stack.http.post("/v1/alerts/evaluate", headers=h), x := stack.http.get("/v1/alerts", headers=h, params={"rule_id": rule["id"], "state": "firing"}).json()["items"])[1] and x[0],
                   60, 1.0, "the worker loss alert")
    assert agent.name in a["summary"] and a["labels"]["worker"] == agent.name
    stack.http.post(f"/v1/workers/{agent.worker_id}/revoke", headers=h, json={})
    stack.http.post("/v1/alerts/evaluate", headers=h)
    assert stack.http.get(f"/v1/alerts/{a['id']}", headers=h).json()["state"] == "resolved"        # a revoked worker is not "lost"
    disk = stack.http.post("/v1/alerts/rules", headers=h, json={"name": "disk full (test)", "type": "disk_pressure", "params": {"used_percent": 1, "include_control_plane": True}}).json()
    stack.http.post("/v1/alerts/evaluate", headers=h)
    firing = stack.http.get("/v1/alerts", headers=h, params={"rule_id": disk["id"], "state": "firing"}).json()["items"]
    assert any(x["instance_key"] == "control-plane" for x in firing)
    stack.http.delete(f"/v1/alerts/rules/{disk['id']}", headers=h)
    stack.http.delete(f"/v1/alerts/rules/{rule['id']}", headers=h)


# ------------------------------------------------------------------------------------------------------------------------------ metrics, health and OTLP
def test_prometheus_endpoint_round_trip_and_route_labels(stack: OrchStack, ws: str) -> None:
    h = stack.admin
    p = stack.pipeline(ws, "metrics-p", [A("A", "wait", waitTimeInSeconds=0)])
    run = stack.wait_run(stack.start_run(p["id"])["id"])
    stack.http.get(f"/v1/pipeline-runs/{run['id']}", headers=h)
    stack.http.get("/v1/pipeline-runs/run_does_not_exist", headers=h)
    assert stack.http.get("/v1/metrics").status_code == 401
    assert stack.http.get("/v1/metrics", headers=stack.user("Viewer", ws)).status_code == 403
    r = stack.http.get("/v1/metrics", headers=h)
    assert r.status_code == 200 and r.headers["content-type"].startswith("text/plain; version=0.0.4") and r.headers["cache-control"] == "no-store"
    m = parse_prometheus(r.text)
    reqs = {(lab["route"], lab["status"]): v for lab, v in m["olcf_api_requests_total"] if lab["method"] == "GET"}
    assert reqs[("/v1/pipeline-runs/{run_id}", "200")] >= 1 and reqs[("/v1/pipeline-runs/{run_id}", "404")] >= 1       # route templates, not raw paths
    assert not any("run_01" in lab["route"] for lab, _ in m["olcf_api_requests_total"])
    assert any(lab["route"] == "/v1/metrics" and lab["status"] == "401" for lab, _ in m["olcf_api_requests_total"])
    assert m["olcf_api_request_duration_seconds_count"] and m["olcf_is_leader"][0][1] == 1.0 and m["olcf_build_info"][0][0]["version"]
    assert sum(v for lab, v in m["olcf_pipeline_runs_total"] if lab.get("state") == "succeeded") >= 1 and any(n.startswith("olcf_pipeline_activity") for n in m)
    assert "olcf_pipeline_runs" in m and "olcf_jobs" in m and "olcf_alerts_firing" in m
    assert 'password' not in r.text.lower()


def test_series_snapshot_names_and_otlp_test(stack: OrchStack, ws: str, httpd: Any) -> None:
    h = stack.admin
    obs = next(m for m in stack.ctx.modules if m.name == "observability")
    obs.sample_once()
    names = stack.http.get("/v1/metrics/names", headers=h, params={"prefix": "olcf_"}).json()["items"]
    assert "olcf_uptime_seconds" in names and "olcf_api_requests_total" in names
    s = stack.http.get("/v1/metrics/series", headers=h, params={"name": "olcf_uptime_seconds", "since": "10m"}).json()
    assert s["series"] and s["series"][0]["points"] and s["step"] >= 15 and s["retention_days"] > 0
    f = stack.http.get("/v1/metrics/series", headers=h, params={"name": "olcf_api_requests_total", "labels": "status=200", "since": "10m"}).json()
    assert f["cumulative"] is True and all(x["labels"].get("status") == "200" for x in f["series"])
    assert stack.http.get("/v1/metrics/series", headers=h, params={"name": "olcf_uptime_seconds", "since": "now", "until": "1h"}).status_code == 400
    assert stack.http.get("/v1/metrics/series", headers=h, params={"name": "olcf_uptime_seconds", "since": "garbage"}).status_code == 400
    assert stack.http.get("/v1/metrics/series", headers=h, params={"name": "olcf_uptime_seconds", "labels": "nokeyvalue"}).status_code == 400
    assert stack.http.get("/v1/metrics/series", headers=h).status_code == 422
    snap = stack.http.get("/v1/metrics/snapshot", headers=h).json()
    assert any(m["name"] == "olcf_uptime_seconds" for m in snap["metrics"])
    st = stack.ctx.service("settings")
    assert stack.http.post("/v1/metrics/otlp/test", headers=h).json() == {"enabled": False, "endpoint": ""}
    st.set("observability.otlp.endpoint", httpd.url)
    try:
        res = stack.http.post("/v1/metrics/otlp/test", headers=h).json()
        assert res["enabled"] is True and res["metrics"] == {"ok": True}
        sent = httpd.to("/v1/metrics")[0].json()["resourceMetrics"][0]
        assert {a["key"] for a in sent["resource"]["attributes"]} >= {"service.name", "service.instance.id"}
        assert stack.http.post("/v1/metrics/otlp/test", headers=stack.user("Contributor", ws)).status_code == 403
    finally:
        st.set("observability.otlp.endpoint", "")


def test_health_checks_report_engine_and_metrics(stack: OrchStack) -> None:
    h = stack.admin
    snap = stack.http.get("/v1/admin/health", headers=h, params={"force": "true"}).json()
    comps = {c["name"]: c for c in snap["components"]}
    assert comps["pipelines-engine"]["status"] == "ok" and comps["metrics"]["status"] in ("ok", "degraded")
    assert "pipelines-engine" in {c["name"] for c in stack.http.get("/v1/admin/health/checks", headers=h).json()["items"]}
    deep = stack.http.post("/v1/admin/health/checks/pipelines-engine/test", headers=h).json()
    assert deep["status"] == "ok" and deep["steps"], deep
    deep_m = stack.http.post("/v1/admin/health/checks/metrics/test", headers=h).json()
    assert deep_m["status"] == "ok" and [s["name"] for s in deep_m["steps"]] == ["registry round trip", "series store write", "OTLP export"]
    assert stack.http.get("/v1/admin/health", headers=stack.user("Viewer", None)).status_code == 403
    info = stack.http.get("/v1/admin/info", headers=h).json()
    assert {"pipelines", "observability"} <= set(info["modules"]) and {"pipelines", "triggers", "webhooks", "metrics", "alerts"} <= set(info["services"])
