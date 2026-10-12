# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""Alerting (spec 28): rule validation, evaluation state machine (pending / firing / resolved, de-duplication, repeat, acknowledge, silence), channels, notifications to a real
local HTTP receiver, built-in rules and authorisation. Compute-backed rule types are exercised in test_api_e2e.py (they need the compute tables)."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from sqlalchemy import update

from olcf.core.errors import BadRequest, Forbidden, NotFound, ValidationFailed
from olcf.core.health import ComponentHealth, HealthCheck, Status
from olcf.core.timeutil import to_iso
from olcf.observability.alerts import DEFAULT_RULES, RULE_TYPES, validate_params
from olcf.pipelines.schema import pipeline_runs as R
from olcf.pipelines.webhooks import verify_signature
from tests.orch.orch_support import FakeClock, Inproc


@pytest.fixture()
def al(ip: Inproc) -> Inproc:
    ip.clock = FakeClock(datetime.now(UTC).isoformat())                                      # type: ignore[attr-defined]  # runs finish in real time, so start at the real "now"
    ip.set_clock(ip.clock)                                                                   # type: ignore[attr-defined]
    return ip


def fail_runs(ip: Inproc, n: int, ws: str | None = None) -> list[str]:
    ids = []
    for _ in range(n):
        p = ip.pipeline("bad-" + uuid.uuid4().hex[:8], [{"name": "x", "type": "t_probe", "typeProperties": {"fail": True}, "policy": {"retries": 0}}], ws=ws)
        run = ip.wait(ip.run(p)["id"])
        assert run["state"] == "failed"
        ids.append(run["id"])
    with ip.ctx.db.begin() as conn:                                                           # runs finish in real time; the alert clock is fake - align them
        conn.execute(update(R).where(R.c.id.in_(ids)).values(finished_at=to_iso(ip.clock())))        # type: ignore[attr-defined]
    return ids


def evaluate(ip: Inproc) -> dict[str, Any]:
    return ip.alerts.evaluate_all(wait=True, force=True)


def alerts(ip: Inproc, **kw: Any) -> list[dict[str, Any]]:
    return ip.alerts.list_alerts(ip.admin, **kw)["items"]


def advance(ip: Inproc, seconds: float) -> None:
    ip.clock.advance(seconds)                                                                # type: ignore[attr-defined]


def pipeline_rule(ip: Inproc, **kw: Any) -> dict[str, Any]:
    body = {"name": "pipelines failing", "type": "failed_pipelines", "severity": "critical", "params": {"threshold": 2, "window_seconds": 3600}, "for_seconds": 0, "repeat_seconds": 600}
    body.update(kw)
    return ip.alerts.create_rule(ip.admin, body)


def webhook_channel(ip: Inproc, url: str, **kw: Any) -> dict[str, Any]:
    return ip.alerts.create_channel(ip.admin, {"name": "ops", "type": "webhook", "config": {"url": url}, **kw})


# ------------------------------------------------------------------------------------------------------------------------------ rule validation
def test_rule_types_are_documented() -> None:
    assert set(RULE_TYPES) == {"failed_jobs", "failed_pipelines", "queue_saturation", "worker_loss", "disk_pressure", "runtime_mismatch", "auth_expiry", "control_plane_health", "fabric_handshake_stale"}
    for t, spec in RULE_TYPES.items():
        assert spec["title"] and spec["description"] and spec["params"], t
        assert validate_params(t, {})                                                         # every type is valid with its defaults


def test_param_validation() -> None:
    assert validate_params("failed_jobs", {"threshold": 3})["window_seconds"] == 900
    for bad in ({"threshold": 0}, {"threshold": 1.5}, {"threshold": True}, {"threshold": "3"}, {"nope": 1}, {"group_by": "galaxy"}, {"window_seconds": 10}, {"interval_seconds": 1}):
        with pytest.raises(ValidationFailed):
            validate_params("failed_jobs", bad)
    with pytest.raises(ValidationFailed):
        validate_params("flying_pigs", {})
    with pytest.raises(ValidationFailed):
        validate_params("failed_jobs", ["not", "a", "dict"])
    with pytest.raises(ValidationFailed):
        validate_params("auth_expiry", {"kinds": "pat"})
    assert validate_params("failed_jobs", None)["threshold"] == 5


def test_rule_crud_and_validation(al: Inproc) -> None:
    ip = al
    rule = pipeline_rule(ip, workspace_id=ip.ws_id)
    assert rule["workspace_id"] == ip.ws_id and rule["enabled"] is True and rule["params"]["threshold"] == 2 and rule["etag"]
    upd = ip.alerts.update_rule(ip.admin, rule["id"], {"severity": "info", "params": {"threshold": 4}, "name": "renamed"})
    assert upd["severity"] == "info" and upd["params"]["threshold"] == 4 and upd["params"]["window_seconds"] == 3600 and upd["etag"] != rule["etag"]
    assert ip.alerts.update_rule(ip.admin, rule["id"], {"enabled": False})["params"]["threshold"] == 4          # an update without params keeps them
    with pytest.raises(ValidationFailed):
        ip.alerts.update_rule(ip.admin, rule["id"], {"type": "failed_jobs"})
    for bad in ({"name": ""}, {"type": "nope"}, {"severity": "meh"}, {"for_seconds": -1}, {"for_seconds": True}, {"repeat_seconds": 5}, {"channels": ["chn_missing"]}, {"channels": "x"},
                {"type": "worker_loss", "workspace_id": ip.ws_id}, {"labels": "x"}, {"params": {"threshold": 0}}):
        with pytest.raises(ValidationFailed):
            pipeline_rule(ip, **bad)
    ip.alerts.delete_rule(ip.admin, rule["id"])
    with pytest.raises(NotFound):
        ip.alerts.get_rule(rule["id"])
    assert rule["id"] not in [r["id"] for r in ip.alerts.list_rules(ip.admin)]


def test_default_rules_are_created_once_and_protected(al: Inproc) -> None:
    ip = al
    n = ip.alerts.ensure_default_rules()
    assert n == len(DEFAULT_RULES) >= 9 and ip.alerts.ensure_default_rules() == 0
    rules = {r["builtin_key"]: r for r in ip.alerts.list_rules(ip.admin)}
    assert rules["worker-lost"]["severity"] == "critical" and rules["fabric-handshake-stale"]["enabled"] is False and rules["failed-jobs"]["enabled"] is True
    with pytest.raises(BadRequest):
        ip.alerts.delete_rule(ip.admin, rules["failed-jobs"]["id"])
    with pytest.raises(ValidationFailed):
        ip.alerts.update_rule(ip.admin, rules["failed-jobs"]["id"], {"type": "failed_pipelines"})
    ip.alerts.update_rule(ip.admin, rules["failed-jobs"]["id"], {"enabled": False, "params": {"threshold": 9}})
    assert ip.alerts.ensure_default_rules() == 0                                              # edits survive
    assert next(r for r in ip.alerts.list_rules(ip.admin) if r["builtin_key"] == "failed-jobs")["params"]["threshold"] == 9


# ------------------------------------------------------------------------------------------------------------------------------ state machine
def test_pending_firing_dedupe_repeat_and_resolve(al: Inproc, httpd: Any) -> None:
    ip = al
    ch = webhook_channel(ip, httpd.url + "/alert", secret="alert-signing-secret-0123456789")
    rule = pipeline_rule(ip, channels=[ch["id"]], for_seconds=60, repeat_seconds=600, workspace_id=ip.ws_id)
    events: list[dict[str, Any]] = []
    ip.ctx.events.subscribe(events.append)
    fail_runs(ip, 1)
    assert evaluate(ip)["conditions"] == 0 and alerts(ip) == []                                # below the threshold: nothing happens
    fail_runs(ip, 1)
    s = evaluate(ip)
    assert s["conditions"] == 1 and s["fired"] == 0
    [a] = alerts(ip)
    assert a["state"] == "pending" and a["fired_at"] is None and a["rule_name"] == "pipelines failing" and a["severity"] == "critical" and "2 pipeline run(s) failed" in a["summary"] and httpd.hits == []
    advance(ip, 30)
    assert evaluate(ip)["fired"] == 0 and alerts(ip)[0]["state"] == "pending"                  # "for" not yet elapsed
    advance(ip, 31)
    s = evaluate(ip)
    assert s["fired"] == 1
    [a] = alerts(ip)
    assert a["state"] == "firing" and a["fired_at"] and a["notify_count"] == 1 and a["details"]["count"] == 2 and len(a["details"]["run_ids"]) == 2 and a["fingerprint"]
    [hit] = httpd.to("/alert")
    body = hit.json()
    assert body["status"] == "firing" and body["alert"]["id"] == a["id"] and body["rule"]["name"] == "pipelines failing" and body["alert"]["severity"] == "critical"
    assert hit.headers["x-olcf-event"] == "alert.firing" and verify_signature("alert-signing-secret-0123456789", hit.headers["x-olcf-timestamp"], hit.body, hit.headers["x-olcf-signature"], tolerance_seconds=None)
    assert any(e["type"] == "alert.firing" and e["payload"]["alert_id"] == a["id"] and e["workspace_id"] == ip.ws_id for e in events)
    assert a["details"]["notifications"][0]["ok"] is True and a["details"]["notifications"][0]["type"] in ("webhook", "log")
    # de-duplication: evaluating again neither creates a second alert nor notifies again
    for _ in range(3):
        evaluate(ip)
    assert len(alerts(ip)) == 1 and len(httpd.to("/alert")) == 1
    # repeat notification only after repeat_seconds
    advance(ip, 590)
    evaluate(ip)
    assert len(httpd.to("/alert")) == 1
    advance(ip, 20)
    evaluate(ip)
    assert len(httpd.to("/alert")) == 2 and alerts(ip)[0]["notify_count"] == 2
    # acknowledged alerts stop repeating
    acked = ip.alerts.acknowledge(ip.admin, a["id"])
    assert acked["acknowledged_at"] and acked["acknowledged_by"] == ip.admin.id
    advance(ip, 2000)
    evaluate(ip)
    assert len(httpd.to("/alert")) == 2 and any(e["type"] == "alert.acknowledged" for e in events)
    # more failures keep updating the same alert (value follows the count)
    fail_runs(ip, 1)
    evaluate(ip)
    assert len(alerts(ip)) == 1 and alerts(ip)[0]["value"] == 3
    # the failures age out of the window: the alert resolves and says so
    advance(ip, 3700)
    s = evaluate(ip)
    assert s["resolved"] == 1
    [a] = alerts(ip)
    assert a["state"] == "resolved" and a["resolved_at"] and a["duration_seconds"] > 3000
    assert [h.json()["status"] for h in httpd.to("/alert")][-1] == "resolved" and any(e["type"] == "alert.resolved" for e in events)
    assert alerts(ip, state="firing") == [] and len(alerts(ip, state="resolved")) == 1 and alerts(ip, state="pending,firing") == []
    with pytest.raises(BadRequest):
        ip.alerts.acknowledge(ip.admin, a["id"])
    # a new episode creates a new history row
    fail_runs(ip, 2)
    evaluate(ip)
    advance(ip, 61)
    evaluate(ip)
    assert len(alerts(ip)) == 2 and len(alerts(ip, rule_id=rule["id"], state="firing")) == 1
    ip.ctx.service("metrics")
    text = ip.metrics.render_prometheus()
    assert 'olcf_alerts_fired_total{rule_type="failed_pipelines",severity="critical"} 2' in text


def test_a_condition_that_clears_before_the_for_duration_never_notifies(al: Inproc, httpd: Any) -> None:
    ip = al
    ch = webhook_channel(ip, httpd.url + "/quiet")
    pipeline_rule(ip, channels=[ch["id"]], for_seconds=300, params={"threshold": 1, "window_seconds": 60})
    fail_runs(ip, 1)
    evaluate(ip)
    assert alerts(ip)[0]["state"] == "pending"
    advance(ip, 120)                                                                         # the failure leaves the 60 s window
    evaluate(ip)
    [a] = alerts(ip)
    assert a["state"] == "resolved" and a["fired_at"] is None and httpd.hits == []


def test_resolve_after_seconds_debounces_flapping(al: Inproc) -> None:
    ip = al
    pipeline_rule(ip, params={"threshold": 1, "window_seconds": 60, "resolve_after_seconds": 300})
    fail_runs(ip, 1)
    evaluate(ip)
    assert alerts(ip)[0]["state"] == "firing"
    advance(ip, 120)                                                                         # condition gone, but not for long enough
    evaluate(ip)
    assert alerts(ip)[0]["state"] == "firing"
    advance(ip, 301)
    evaluate(ip)
    assert alerts(ip)[0]["state"] == "resolved"


def test_silencing_a_rule_and_an_alert_suppresses_notifications_but_keeps_state(al: Inproc, httpd: Any) -> None:
    ip = al
    ch = webhook_channel(ip, httpd.url + "/silent")
    rule = pipeline_rule(ip, channels=[ch["id"]], params={"threshold": 1, "window_seconds": 172800}, repeat_seconds=60)
    ip.alerts.silence_rule(ip.admin, rule["id"], 3600)
    assert ip.alerts.get_rule(rule["id"])["silenced_until"]
    fail_runs(ip, 1)
    evaluate(ip)
    [a] = alerts(ip)
    assert a["state"] == "firing" and httpd.hits == []                                       # firing, but quiet
    advance(ip, 3601)                                                                        # the silence expires: the next repeat notifies
    evaluate(ip)
    assert len(httpd.to("/silent")) == 1
    ip.alerts.silence(ip.admin, a["id"], 7200)
    advance(ip, 120)
    evaluate(ip)
    assert len(httpd.to("/silent")) == 1 and ip.alerts.get_alert(ip.admin, a["id"])["silenced_until"]
    ip.alerts.silence(ip.admin, a["id"], None)                                               # lifting
    advance(ip, 120)
    evaluate(ip)
    assert len(httpd.to("/silent")) == 2


def test_disabled_and_deleted_rules_resolve_their_alerts(al: Inproc) -> None:
    ip = al
    r1 = pipeline_rule(ip, name="r1", params={"threshold": 1, "window_seconds": 3600})
    r2 = pipeline_rule(ip, name="r2", params={"threshold": 1, "window_seconds": 3600})
    fail_runs(ip, 1)
    evaluate(ip)
    assert {a["rule_id"] for a in alerts(ip, state="firing")} == {r1["id"], r2["id"]}
    ip.alerts.update_rule(ip.admin, r1["id"], {"enabled": False})
    evaluate(ip)
    assert {a["rule_id"] for a in alerts(ip, state="firing")} == {r2["id"]}
    ip.alerts.delete_rule(ip.admin, r2["id"])
    evaluate(ip)
    assert alerts(ip, state="firing,pending") == [] and {a["state"] for a in alerts(ip)} == {"resolved"}


def test_per_rule_interval_is_respected_unless_forced(al: Inproc) -> None:
    ip = al
    pipeline_rule(ip, params={"threshold": 1, "interval_seconds": 120})
    assert ip.alerts.evaluate_all(wait=True)["rules"] == 1
    assert ip.alerts.evaluate_all(wait=True)["rules"] == 0                                    # too soon
    advance(ip, 121)
    assert ip.alerts.evaluate_all(wait=True)["rules"] == 1
    assert ip.alerts.evaluate_all(wait=True, force=True)["rules"] == 1


def test_a_failing_rule_does_not_stop_the_others_or_resolve_alerts(al: Inproc) -> None:
    ip = al
    good = pipeline_rule(ip, params={"threshold": 1, "window_seconds": 3600})
    fail_runs(ip, 1)
    evaluate(ip)
    assert alerts(ip)[0]["state"] == "firing"
    broken = ip.alerts.create_rule(ip.admin, {"name": "broken", "type": "worker_loss"})

    def boom(rule: dict[str, Any], params: dict[str, Any]) -> list[Any]:
        raise RuntimeError("database says no password=hunter2hunter2")

    ip.alerts.evaluators._worker_loss = boom                                                 # type: ignore[method-assign]
    s = evaluate(ip)
    err = ip.alerts.get_rule(broken["id"])["last_error"]
    assert s["errors"] == 1 and "RuntimeError" in err and "hunter2hunter2" not in err and ip.alerts.get_rule(good["id"])["last_error"] is None
    assert alerts(ip)[0]["state"] == "firing"                                                # an evaluation error is not a recovery
    assert ip.alerts.stats()["last_run"]["errors"] == 1


def test_rule_test_endpoint_does_not_touch_state(al: Inproc) -> None:
    ip = al
    rule = pipeline_rule(ip, params={"threshold": 1, "window_seconds": 3600})
    fail_runs(ip, 1)
    res = ip.alerts.test_rule(ip.admin, rule["id"])
    assert res["firing"] is True and res["conditions"][0]["summary"] and alerts(ip) == []


def test_workspace_scoped_rules_only_count_their_workspace(al: Inproc) -> None:
    ip = al
    other = ip.workspace()["id"]
    scoped = pipeline_rule(ip, name="scoped", workspace_id=other, params={"threshold": 1, "window_seconds": 3600})
    fail_runs(ip, 2)                                                                          # failures happen in the default workspace
    evaluate(ip)
    assert alerts(ip, rule_id=scoped["id"]) == []
    fail_runs(ip, 1, ws=other)
    evaluate(ip)
    [a] = alerts(ip, rule_id=scoped["id"])
    assert a["workspace_id"] == other and a["details"]["count"] == 1
    grouped = pipeline_rule(ip, name="grouped", params={"threshold": 1, "window_seconds": 3600, "group_by": "pipeline"})
    evaluate(ip)
    assert len(alerts(ip, rule_id=grouped["id"])) == 3                                        # one alert per failing pipeline


# ------------------------------------------------------------------------------------------------------------------------------ other evaluators runnable here
def test_control_plane_health_rule(al: Inproc) -> None:
    ip = al
    state = {"status": Status.DOWN}
    ip.ctx.health.register(HealthCheck("t_component", "test", "test check", lambda: ComponentHealth("t_component", state["status"], f"state is {state['status'].value} password=hunter2hunter2", category="test"), ttl=0))
    ip.alerts.create_rule(ip.admin, {"name": "health", "type": "control_plane_health", "params": {"components": ["t_component"]}})
    evaluate(ip)
    [a] = alerts(ip)
    assert a["instance_key"] == "t_component" and a["state"] == "firing" and "hunter2hunter2" not in a["summary"] + str(a["details"])       # health details are redacted
    state["status"] = Status.OK
    evaluate(ip)
    assert alerts(ip)[0]["state"] == "resolved"
    state["status"] = Status.DEGRADED
    evaluate(ip)
    assert alerts(ip, state="firing") == []                                                  # degraded is below the default min_status
    ip.alerts.create_rule(ip.admin, {"name": "health-degraded", "type": "control_plane_health", "params": {"components": ["t_component"], "min_status": "degraded"}})
    evaluate(ip)
    assert len(alerts(ip, state="firing")) == 1


def test_auth_expiry_rule(al: Inproc) -> None:
    ip = al
    soon, _ = ip.ctx.tokens.create_pat(ip.admin.id, "ci-token", ttl_days=2)
    ip.ctx.tokens.create_pat(ip.admin.id, "long-lived", ttl_days=300)
    del soon
    ip.alerts.create_rule(ip.admin, {"name": "expiry", "type": "auth_expiry", "params": {"within_days": 7, "check_certificates": False}})
    evaluate(ip)
    [a] = alerts(ip)
    assert "ci-token" in a["summary"] and 1.5 < a["value"] < 2.1 and a["instance_key"].startswith("token:") and a["state"] == "firing"
    for bad in ({"kinds": ["pat"], "within_days": -1},):
        with pytest.raises(ValidationFailed):
            ip.alerts.create_rule(ip.admin, {"name": "x", "type": "auth_expiry", "params": bad})


def test_auth_expiry_covers_worker_machine_certificates(al: Inproc) -> None:
    """Worker certificates inside ``worker_certificate_days`` raise one alert per node; revoked nodes and far-away expiries do not; the builtin critical rule uses 7 days."""
    from datetime import UTC, datetime, timedelta

    from olcf.compute import schema as C
    from olcf.core.timeutil import to_iso
    from olcf.db.util import insert_row

    ip = al
    now = datetime.now(UTC)

    def worker(name: str, days: float, *, revoked: bool = False) -> None:
        with ip.ctx.db.begin() as conn:
            insert_row(conn, C.workers, {
                "id": f"wrk_{name}", "name": name, "state": "offline" if revoked else "online", "scheduling": "active", "pool": "default", "labels": {}, "embedded": False,
                "inventory": {}, "metrics": {}, "history": {}, "runtimes": [], "components": [], "spark_daemons": [], "active_jobs": [], "notes": {}, "connected": False,
                "enrolled_at": to_iso(now), "etag": "e", "created_at": to_iso(now), "updated_at": to_iso(now), "cert_expires_at": to_iso(now + timedelta(days=days)),
                "revoked_at": to_iso(now) if revoked else None})

    worker("soon", 12)
    worker("critical", 3)
    worker("far", 80)
    worker("gone", 2, revoked=True)
    ip.alerts.create_rule(ip.admin, {"name": "worker-certs", "type": "auth_expiry", "params": {"kinds": [], "check_certificates": False, "worker_certificate_days": 30}})
    evaluate(ip)
    firing = {a["instance_key"]: a for a in alerts(ip, state="firing")}
    assert set(firing) == {"worker-cert:wrk_soon", "worker-cert:wrk_critical"}
    assert "machine certificate of worker 'critical'" in firing["worker-cert:wrk_critical"]["summary"] and 2.5 < firing["worker-cert:wrk_critical"]["value"] < 3.1
    builtin = next(r for r in DEFAULT_RULES if r["builtin_key"] == "worker-certificate-critical")
    assert builtin["severity"] == "critical" and builtin["params"]["worker_certificate_days"] == 7 and validate_params("auth_expiry", builtin["params"])["kinds"] == []
    with pytest.raises(ValidationFailed):
        validate_params("auth_expiry", {"worker_certificate_days": -1})


# ------------------------------------------------------------------------------------------------------------------------------ channels + notification delivery
def test_channel_validation_and_public_view(al: Inproc, httpd: Any) -> None:
    ip = al
    ch = webhook_channel(ip, httpd.url + "/x?token=SUPERSECRETTOKEN", secret="channel-secret-0123456789", config=None) if False else ip.alerts.create_channel(
        ip.admin, {"name": "w", "type": "webhook", "config": {"url": httpd.url + "/x?token=SUPERSECRETTOKEN", "headers": {"X-Team": "ops"}}, "secret": "channel-secret-0123456789"})
    pub = ip.alerts.public_channel(ch)
    assert "SUPERSECRETTOKEN" not in str(pub) and pub["has_secret"] is True and "channel-secret" not in str(pub) and pub["config"]["headers"] == {"X-Team": "ops"}
    for bad in ({"type": "pigeon"}, {"type": "webhook", "config": {"url": "http://169.254.169.254/"}}, {"type": "webhook", "config": {"url": httpd.url, "extra": 1}}, {"type": "webhook", "config": {}},
                {"type": "webhook", "config": {"url": httpd.url, "headers": {"Authorization": "x"}}}, {"type": "email", "config": {"to": []}}, {"type": "email", "config": {"to": ["not-an-address"]}},
                {"type": "email", "config": {"to": ["a@b.c"], "x": 1}}, {"type": "log", "secret": "x" * 20}, {"type": "email", "config": {"to": ["a@b.c"]}, "secret": "x" * 20}, {"name": ""}):
        body = {"name": "c", "type": "log", "config": {}, **bad}
        with pytest.raises(ValidationFailed):
            ip.alerts.create_channel(ip.admin, body)
    em = ip.alerts.create_channel(ip.admin, {"name": "mail", "type": "email", "config": {"to": ["oncall@example.com"]}})
    assert em["config"]["to"] == ["oncall@example.com"]
    upd = ip.alerts.update_channel(ip.admin, em["id"], {"enabled": False, "config": {"to": ["a@example.com", "b@example.com"]}})
    assert upd["enabled"] is False and len(upd["config"]["to"]) == 2
    ip.alerts.delete_channel(ip.admin, ch["id"])
    with pytest.raises(NotFound):
        ip.alerts.get_channel(ch["id"])
    assert ch["id"] not in [c["id"] for c in ip.alerts.list_channels(ip.admin)]
    with pytest.raises(ValidationFailed):                                                      # rules cannot reference deleted channels
        pipeline_rule(ip, channels=[ch["id"]])


def test_channel_test_delivery_and_failure_reporting(al: Inproc, httpd: Any) -> None:
    ip = al
    ch = ip.alerts.create_channel(ip.admin, {"name": "w", "type": "webhook", "config": {"url": httpd.url + "/t"}, "secret": "channel-secret-0123456789"})
    res = ip.alerts.test_channel(ip.admin, ch["id"])
    assert res["ok"] is True and res["detail"] == "HTTP 200"
    [hit] = httpd.to("/t")
    assert hit.json()["status"] == "test" and hit.json()["rule"]["name"] == "Test notification" and verify_signature("channel-secret-0123456789", hit.headers["x-olcf-timestamp"], hit.body, hit.headers["x-olcf-signature"], tolerance_seconds=None)
    httpd.on("/t", 500)
    bad = ip.alerts.test_channel(ip.admin, ch["id"])
    assert bad["ok"] is False and "500" in bad["detail"] and len(httpd.to("/t")) == 2        # a test is a single attempt
    ip.alerts.update_channel(ip.admin, ch["id"], {"config": {"url": httpd.url + "/t"}})
    ip.ctx.service("settings").set("network.ssrf.allow", [])
    blocked = ip.alerts.test_channel(ip.admin, ch["id"])
    assert blocked["ok"] is False and "blocked" in blocked["detail"] and len(httpd.to("/t")) == 2
    ip.ctx.service("settings").set("network.ssrf.allow", ["127.0.0.0/8"])
    log_ch = ip.alerts.create_channel(ip.admin, {"name": "l", "type": "log"})
    assert ip.alerts.test_channel(ip.admin, log_ch["id"])["ok"] is True
    mail = ip.alerts.create_channel(ip.admin, {"name": "m", "type": "email", "config": {"to": ["a@example.com"]}})
    r = ip.alerts.test_channel(ip.admin, mail["id"])
    assert r["ok"] is False and "SMTP is not configured" in r["detail"]


def test_a_failing_channel_is_recorded_and_other_channels_still_notify(al: Inproc, httpd: Any) -> None:
    ip = al
    httpd.on("/down", 400)                                                                    # 4xx: not retried by the notifier
    good = webhook_channel(ip, httpd.url + "/up")
    bad = webhook_channel(ip, httpd.url + "/down")
    pipeline_rule(ip, channels=[bad["id"], good["id"]], params={"threshold": 1, "window_seconds": 3600})
    fail_runs(ip, 1)
    evaluate(ip)
    [a] = alerts(ip)
    notes = {n["channel"]: n for n in a["details"]["notifications"]}
    assert notes[good["id"]]["ok"] is True and notes[bad["id"]]["ok"] is False and "400" in notes[bad["id"]]["detail"]
    assert len(httpd.to("/up")) == 1 and len(httpd.to("/down")) == 1
    assert 'olcf_alert_notifications_total{channel_type="webhook",result="error"} 1' in ip.metrics.render_prometheus()


def test_webhook_channel_retries_server_errors(al: Inproc, httpd: Any) -> None:
    ip = al
    httpd.on("/flaky", 503, 200)
    ch = webhook_channel(ip, httpd.url + "/flaky")
    res = ip.alerts.notifier.send(ip.alerts.get_channel(ch["id"]), {"status": "firing", "alert": {"severity": "info", "summary": "s"}, "rule": {"name": "r"}}, attempts=3)
    assert res["ok"] is True and res["attempts"] == 2 and len(httpd.to("/flaky")) == 2


def test_alert_payloads_are_redacted(al: Inproc) -> None:
    ip = al
    p = ip.pipeline("leaky", [{"name": "F", "type": "fail", "typeProperties": {"message": "connect failed password=hunter2hunter2 token=abcdef0123456789abcdef", "errorCode": "E1"}}])  # gitleaks:allow
    run = ip.wait(ip.run(p)["id"])
    assert run["state"] == "failed"
    with ip.ctx.db.begin() as conn:
        conn.execute(update(R).where(R.c.id == run["id"]).values(finished_at=to_iso(ip.clock())))       # type: ignore[attr-defined]
        conn.execute(update(R).where(R.c.id == run["id"]).values(state_reason="connect failed password=hunter2hunter2"))
    pipeline_rule(ip, params={"threshold": 1, "window_seconds": 3600})
    evaluate(ip)
    text = str(alerts(ip))
    assert "last_reason" in text and "hunter2hunter2" not in text


# ------------------------------------------------------------------------------------------------------------------------------ authorisation
def test_alert_authorisation(al: Inproc) -> None:
    ip = al
    ident = ip.ctx.service("identity")
    row = ident.create_principal("alert-viewer", kind="user", password="correct horse battery staple")
    ident.bind(row["id"], "Viewer", scope_type="workspace", scope_id=ip.ws_id)
    viewer = ip.ctx.authz.principal_from_row(row)
    row2 = ident.create_principal("alert-manager", kind="user", password="correct horse battery staple")
    ident.bind(row2["id"], "WorkspaceAdmin", scope_type="workspace", scope_id=ip.ws_id)
    manager = ip.ctx.authz.principal_from_row(row2)
    rule = pipeline_rule(ip, workspace_id=ip.ws_id, params={"threshold": 1, "window_seconds": 3600})
    platform = ip.alerts.create_rule(ip.admin, {"name": "platform", "type": "worker_loss"})
    with pytest.raises(Forbidden):
        ip.alerts.create_rule(viewer, {"name": "x", "type": "failed_pipelines", "workspace_id": ip.ws_id})
    with pytest.raises(Forbidden):
        ip.alerts.update_rule(viewer, rule["id"], {"enabled": False})
    with pytest.raises(Forbidden):
        ip.alerts.create_channel(viewer, {"name": "c", "type": "log"})
    assert ip.alerts.create_rule(manager, {"name": "mine", "type": "failed_pipelines", "workspace_id": ip.ws_id})["owner_id"] == manager.id        # workspace admins manage workspace rules
    with pytest.raises(Forbidden):
        ip.alerts.create_rule(manager, {"name": "platform2", "type": "worker_loss"})                  # platform-wide rules need an organisation-level grant
    with pytest.raises(Forbidden):
        ip.alerts.silence_rule(manager, platform["id"], 60)
    fail_runs(ip, 1)
    evaluate(ip)
    [a] = alerts(ip, state="firing")
    assert [x["id"] for x in ip.alerts.list_alerts(viewer)["items"]] == [a["id"]]                       # may read workspace alerts
    with pytest.raises(Forbidden):
        ip.alerts.acknowledge(viewer, a["id"])
    assert ip.alerts.acknowledge(manager, a["id"])["acknowledged_by"] == manager.id
    other = ip.workspace()["id"]
    outsider_row = ident.create_principal("alert-outsider", kind="user", password="correct horse battery staple")
    ident.bind(outsider_row["id"], "Viewer", scope_type="workspace", scope_id=other)
    outsider = ip.ctx.authz.principal_from_row(outsider_row)
    assert ip.alerts.list_alerts(outsider)["items"] == []
    with pytest.raises(NotFound):
        ip.alerts.get_alert(outsider, a["id"])
    assert rule["id"] in [r["id"] for r in ip.alerts.list_rules(viewer)] and rule["id"] not in [r["id"] for r in ip.alerts.list_rules(outsider)]


def test_history_retention_and_pagination(al: Inproc) -> None:
    ip = al
    pipeline_rule(ip, params={"threshold": 1, "window_seconds": 60})
    for _ in range(3):
        fail_runs(ip, 1)
        evaluate(ip)
        advance(ip, 120)
        evaluate(ip)
        advance(ip, 1)
    assert len(alerts(ip, state="resolved")) == 3
    page1 = ip.alerts.list_alerts(ip.admin, limit=2)
    assert len(page1["items"]) == 2 and page1["next_cursor"]
    page2 = ip.alerts.list_alerts(ip.admin, limit=2, cursor=page1["next_cursor"])
    assert len(page2["items"]) == 1 and {a["id"] for a in page1["items"]}.isdisjoint(a["id"] for a in page2["items"])
    assert ip.alerts.purge() == 0
    advance(ip, 91 * 86400)
    assert ip.alerts.purge() == 3 and alerts(ip) == []
