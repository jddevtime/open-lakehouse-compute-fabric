# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""Outbound event webhooks (spec 24): subscriptions, signing, delivery against a real local HTTP receiver, retries with exponential backoff, dead letters, SSRF protection."""

from __future__ import annotations

import json
import socket
from datetime import timedelta
from typing import Any

import pytest

from olcf.core.errors import BadRequest, NotFound, ValidationFailed
from olcf.pipelines.webhooks import AuditEventBridge, canonical_body, matches, verify_signature
from tests.orch.orch_support import FakeClock, Inproc


def _real_now() -> str:
    from datetime import UTC, datetime

    return datetime.now(UTC).isoformat()


@pytest.fixture()
def ipw(ip_idle: Inproc) -> Inproc:
    ip_idle.clock = FakeClock(_real_now())     # type: ignore[attr-defined]   # events carry real timestamps, so the fake clock starts at the real "now"
    ip_idle.set_clock(ip_idle.clock)           # type: ignore[attr-defined]
    ip_idle.hooks.fanout()                     # seeds the event cursor like start() does
    ip_idle.ctx.service("settings").set("webhooks.backoff_base_seconds", 10)
    ip_idle.hooks.start = lambda: None         # type: ignore[method-assign]   # loops are driven by the tests
    return ip_idle


def subscribe(ip: Inproc, url: str, events: list[str] | None = None, **kw: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    ip.clock.set(_real_now())                                               # type: ignore[attr-defined]   # subscriptions only see events published after they were created
    return ip.hooks.create(ip.admin, {"name": kw.pop("name", "hook"), "url": url, "event_types": events or ["job.*"], **kw})


def emit(ip: Inproc, type_: str = "job.state_changed", ws: str | None = None, **payload: Any) -> dict[str, Any]:
    evt = ip.ctx.events.publish(type_, {"job_id": "job_1", "to": "failed", **payload}, workspace_id=ws if ws is not None else ip.ws_id, subject="job_1", persist=True)
    return evt if isinstance(evt, dict) else {"type": type_}


def drain(ip: Inproc, n: int = 1) -> list[dict[str, int]]:
    return [ip.hooks.tick() for _ in range(n)]


def deliveries(ip: Inproc, hook: dict[str, Any], **kw: Any) -> list[dict[str, Any]]:
    return ip.hooks.deliveries(ip.admin, hook["id"], limit=200, **kw)["items"]


def closed_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


# ------------------------------------------------------------------------------------------------------------------------------ subscriptions
def test_pattern_matching() -> None:
    assert matches(["job.*"], "job.state_changed") and not matches(["job.*"], "pipeline.run.failed")
    assert matches(["*"], "anything.at.all") and matches(["pipeline.run.failed", "item.*"], "pipeline.run.failed")
    assert not matches(["pipeline.run.failed"], "pipeline.run.failed.extra")


def test_create_returns_the_secret_once_and_never_shows_it_again(ipw: Inproc, httpd: Any) -> None:
    row, shown = subscribe(ipw, httpd.url + "/h?token=abc123", ["job.*", "pipeline.*"])
    assert shown["secret"].startswith("whsec_") and len(shown["secret"]) > 30
    pub = ipw.hooks.public(row)
    assert shown["secret"] not in json.dumps(pub) and "secret" not in pub and pub["has_secret"] is True and "abc123" not in pub["url"] and pub["event_types"] == ["job.*", "pipeline.*"]
    assert shown["secret"] not in json.dumps(ipw.hooks.public(ipw.hooks.get(row["id"])))
    row2, shown2 = subscribe(ipw, httpd.url + "/h2", secret="my-own-secret-0123456789")
    assert shown2 == {} and ipw.hooks.public(row2)["has_secret"]
    rot = ipw.hooks.rotate_secret(ipw.admin, row["id"])
    assert rot["secret"] != shown["secret"]


def test_validation_of_subscriptions(ipw: Inproc, httpd: Any) -> None:
    ip = ipw
    base = {"name": "h", "url": httpd.url + "/x", "event_types": ["job.*"]}
    for bad in ({"name": ""}, {"event_types": []}, {"event_types": ["Job.*"]}, {"event_types": ["webhook.test"]}, {"url": "ftp://x/y"}, {"url": "http://169.254.169.254/latest/meta-data"},
                {"url": "http://user:pw@example.com/"}, {"max_attempts": 99}, {"max_attempts": 0}, {"max_attempts": "many"}, {"timeout_seconds": 0}, {"timeout_seconds": "fast"}, {"secret": "short"}, {"headers": {"Authorization": "Bearer x"}},
                {"headers": {"X-OLCF-Event": "spoof"}}, {"headers": {"Bad Name": "x"}}, {"headers": {"X-A": "line\nbreak"}}):
        with pytest.raises(ValidationFailed, match=".+"):
            ip.hooks.create(ip.admin, {**base, **bad})
            raise AssertionError(f"accepted {bad}")


def test_ssrf_guard_blocks_private_targets_unless_allow_listed(ipw: Inproc, httpd: Any) -> None:
    ip = ipw
    ip.ctx.service("settings").set("network.ssrf.allow", [])
    for url in (httpd.url + "/x", "http://localhost:9/x", "http://10.0.0.5/x", "http://[::1]/x", "http://192.168.1.1/x", "http://169.254.169.254/x"):
        with pytest.raises(ValidationFailed) as ei:
            ip.hooks.create(ip.admin, {"name": "h", "url": url, "event_types": ["*"]})
        assert "url rejected" in str(ei.value), url
    ip.ctx.service("settings").set("network.ssrf.allow", ["127.0.0.0/8"])
    assert ip.hooks.create(ip.admin, {"name": "ok", "url": httpd.url + "/x", "event_types": ["*"]})[0]["id"]
    ip.ctx.service("settings").set("network.ssrf.deny", ["127.0.0.0/8"])
    with pytest.raises(ValidationFailed):                                                    # an explicit deny wins over the allow list
        ip.hooks.create(ip.admin, {"name": "denied", "url": httpd.url + "/x", "event_types": ["*"]})
    ip.ctx.service("settings").set("network.ssrf.deny", [])


def test_ssrf_is_rechecked_at_delivery_time(ipw: Inproc, httpd: Any) -> None:
    ip = ipw
    hook, _ = subscribe(ip, httpd.url + "/h")
    emit(ip)
    ip.ctx.service("settings").set("network.ssrf.allow", [])                                 # the operator removes the allow rule after the subscription was created
    drain(ip)
    [d] = deliveries(ip, hook)
    assert d["state"] == "dead" and "blocked" in d["last_error"] and httpd.hits == []        # permanent, never reached the server
    ip.ctx.service("settings").set("network.ssrf.allow", ["127.0.0.0/8"])


# ------------------------------------------------------------------------------------------------------------------------------ delivery
def test_delivery_is_signed_and_carries_the_event(ipw: Inproc, httpd: Any) -> None:
    ip = ipw
    hook, shown = subscribe(ip, httpd.url + "/hook", ["job.*"])
    emit(ip, "job.state_changed", reason="boom", password="should-not-leak-0123456789")
    emit(ip, "pipeline.run.failed")                                                          # not subscribed
    r = drain(ip)[0]
    assert r["enqueued"] == 1 and r["attempted"] == 1
    [hit] = httpd.to("/hook")
    h = hit.headers
    assert h["x-olcf-event"] == "job.state_changed" and h["x-olcf-attempt"] == "1" and h["content-type"] == "application/json"
    assert h["x-olcf-delivery"].startswith("whd_") and h["x-olcf-event-id"]
    assert verify_signature(shown["secret"], h["x-olcf-timestamp"], hit.body, h["x-olcf-signature"], now=ip.clock().timestamp())     # type: ignore[attr-defined]
    assert not verify_signature("another-secret-another-secret", h["x-olcf-timestamp"], hit.body, h["x-olcf-signature"], tolerance_seconds=None)
    assert not verify_signature(shown["secret"], h["x-olcf-timestamp"], hit.body + b" ", h["x-olcf-signature"], tolerance_seconds=None)         # tampering
    assert not verify_signature(shown["secret"], h["x-olcf-timestamp"], hit.body, h["x-olcf-signature"], now=ip.clock().timestamp() + 3600)   # type: ignore[attr-defined]   # replay window
    env = hit.json()
    assert env["type"] == "job.state_changed" and env["id"] == h["x-olcf-event-id"] and env["workspace_id"] == ip.ws_id and env["api_version"] and env["data"]["reason"] == "boom"
    assert "should-not-leak" not in hit.body.decode()                                        # payloads are redacted
    assert hit.body == canonical_body(env)
    [d] = deliveries(ip, hook)
    assert d["state"] == "succeeded" and d["attempts"] == 1 and d["last_status_code"] == 200 and d["delivered_at"] and d["attempt_log"][0]["status"] == 200
    assert ip.hooks.get(hook["id"])["last_status"] == "succeeded"


def test_fanout_scoping_and_exactly_once(ipw: Inproc, httpd: Any) -> None:
    ip = ipw
    other_ws = ip.workspace()["id"]
    a, _ = subscribe(ip, httpd.url + "/a", ["*"], workspace_id=ip.ws_id, name="ws-hook")
    b, _ = subscribe(ip, httpd.url + "/b", ["job.*"], name="org-hook")
    emit(ip, "job.x", ws=ip.ws_id)
    emit(ip, "job.y", ws=other_ws)
    drain(ip, 3)
    assert sorted(h.json()["type"] for h in httpd.to("/a")) == ["job.x"]                      # workspace-scoped hook never sees other workspaces
    assert sorted(h.json()["type"] for h in httpd.to("/b")) == ["job.x", "job.y"]            # organisation-wide hook sees all matching events
    assert ip.hooks.fanout() == 0                                                            # nothing is enqueued twice
    n = len(httpd.hits)
    drain(ip, 3)
    assert len(httpd.hits) == n
    assert len(deliveries(ip, a)) == 1 and len(deliveries(ip, b)) == 2


def test_events_before_the_subscription_are_not_delivered(ipw: Inproc, httpd: Any) -> None:
    ip = ipw
    emit(ip, "job.before")
    ip.clock.advance(5)                                                                      # type: ignore[attr-defined]
    hook, _ = subscribe(ip, httpd.url + "/h")
    drain(ip)
    assert httpd.hits == [] and deliveries(ip, hook) == []
    emit(ip, "job.after")
    drain(ip)
    assert [h.json()["type"] for h in httpd.hits] == ["job.after"]


# ------------------------------------------------------------------------------------------------------------------------------ retries / dead letters
def test_retries_with_exponential_backoff_then_success(ipw: Inproc, httpd: Any) -> None:
    ip = ipw
    httpd.on("/flaky", 500, 503, 200)
    hook, _ = subscribe(ip, httpd.url + "/flaky", max_attempts=5)
    emit(ip)
    drain(ip)
    [d] = deliveries(ip, hook)
    assert d["state"] == "retrying" and d["attempts"] == 1 and d["last_status_code"] == 500 and "HTTP 500" in d["last_error"]
    delay1 = (ip.clock() - ip.clock()).total_seconds()                                       # placeholder to keep the arithmetic below readable
    row = ip.hooks.get_delivery(ip.admin, d["id"])
    delay1 = (_t(row["next_attempt_at"]) - ip.clock()).total_seconds()
    assert 8.0 <= delay1 <= 12.0                                                             # base 10 s +-20 %
    drain(ip)
    assert len(httpd.hits) == 1                                                              # not retried before it is due
    ip.clock.advance(delay1 + 0.1)                                                           # type: ignore[attr-defined]
    drain(ip)
    row = ip.hooks.get_delivery(ip.admin, d["id"])
    assert row["state"] == "retrying" and row["attempts"] == 2
    delay2 = (_t(row["next_attempt_at"]) - ip.clock()).total_seconds()
    assert 16.0 <= delay2 <= 24.0                                                            # doubled
    ip.clock.advance(delay2 + 0.1)                                                           # type: ignore[attr-defined]
    drain(ip)
    row = ip.hooks.get_delivery(ip.admin, d["id"])
    assert row["state"] == "succeeded" and row["attempts"] == 3 and [e["status"] for e in row["attempt_log"]] == [500, 503, 200]
    assert [h.headers["x-olcf-attempt"] for h in httpd.to("/flaky")] == ["1", "2", "3"]
    assert len({h.headers["x-olcf-delivery"] for h in httpd.to("/flaky")}) == 1 and len({h.headers["x-olcf-event-id"] for h in httpd.to("/flaky")}) == 1   # same identifiers every attempt
    assert ip.hooks.get(hook["id"])["consecutive_failures"] == 0


def _t(iso: str) -> Any:
    from olcf.core.timeutil import from_iso

    return from_iso(iso)


def test_retry_after_header_is_honoured(ipw: Inproc, httpd: Any) -> None:
    ip = ipw
    httpd.on("/busy", (429, "slow down", {"Retry-After": "90"}), 200)
    hook, _ = subscribe(ip, httpd.url + "/busy")
    emit(ip)
    drain(ip)
    [d] = deliveries(ip, hook)
    assert d["state"] == "retrying" and (_t(ip.hooks.get_delivery(ip.admin, d["id"])["next_attempt_at"]) - ip.clock()).total_seconds() >= 89


def test_exhausted_retries_go_to_the_dead_letter_queue_and_can_be_redelivered(ipw: Inproc, httpd: Any) -> None:
    ip = ipw
    httpd.on("/down", 502)
    hook, _ = subscribe(ip, httpd.url + "/down", max_attempts=3)
    emit(ip)
    for _ in range(6):
        drain(ip)
        ip.clock.advance(60)                                                                 # type: ignore[attr-defined]
    [d] = deliveries(ip, hook)
    assert d["state"] == "dead" and d["attempts"] == 3 and len(httpd.hits) == 3 and all(e["status"] == 502 for e in d["attempt_log"])
    assert [x["id"] for x in deliveries(ip, hook, state="dead")] == [d["id"]] and deliveries(ip, hook, state="succeeded") == []
    assert ip.hooks.get(hook["id"])["consecutive_failures"] == 1 and ip.hooks.get(hook["id"])["last_status"] == "dead"
    httpd.on("/down", 200)                                                                   # the receiver recovers
    again = ip.hooks.redeliver(ip.admin, d["id"])
    assert again["state"] == "pending" and again["attempts"] == 0
    drain(ip)
    assert ip.hooks.get_delivery(ip.admin, d["id"])["state"] == "succeeded"
    with pytest.raises(BadRequest):                                                          # only finished deliveries can be re-queued... a pending one cannot
        ip.hooks.redeliver(ip.admin, ip.hooks.redeliver(ip.admin, d["id"])["id"])


@pytest.mark.parametrize(("status", "retries"), [(400, False), (401, False), (404, False), (422, False), (301, False), (408, True), (425, True), (429, True), (500, True), (504, True)])
def test_which_statuses_are_retried(ipw: Inproc, httpd: Any, status: int, retries: bool) -> None:
    ip = ipw
    httpd.on("/s", status)
    hook, _ = subscribe(ip, httpd.url + "/s", max_attempts=4)
    emit(ip)
    drain(ip)
    [d] = deliveries(ip, hook)
    assert d["state"] == ("retrying" if retries else "dead"), (status, d["state"])


def test_gone_disables_the_subscription(ipw: Inproc, httpd: Any) -> None:
    ip = ipw
    httpd.on("/gone", 410)
    hook, _ = subscribe(ip, httpd.url + "/gone")
    emit(ip)
    drain(ip)
    row = ip.hooks.get(hook["id"])
    assert row["enabled"] is False and "410" in row["disabled_reason"] and deliveries(ip, hook)[0]["state"] == "dead"
    emit(ip)
    drain(ip)
    assert len(httpd.hits) == 1
    ip.hooks.update(ip.admin, hook["id"], {"enabled": True})
    assert ip.hooks.get(hook["id"])["enabled"] is True and ip.hooks.get(hook["id"])["disabled_reason"] is None


def test_repeated_permanent_failures_disable_the_subscription(ipw: Inproc, httpd: Any) -> None:
    ip = ipw
    ip.ctx.service("settings").set("webhooks.disable_after_dead", 2)
    httpd.on("/bad", 400)
    hook, _ = subscribe(ip, httpd.url + "/bad")
    for _ in range(3):
        emit(ip)
        drain(ip)
    row = ip.hooks.get(hook["id"])
    assert row["enabled"] is False and "consecutive" in row["disabled_reason"]
    assert len(httpd.hits) == 2


def test_a_backlog_for_a_broken_endpoint_cannot_push_the_failure_counter_past_the_limit(ipw: Inproc, httpd: Any) -> None:
    """FIX O: many deliveries are attempted in one pass (concurrently); once the subscription is disabled the others make no request and leave the counter alone."""
    ip = ipw
    ip.ctx.service("settings").set("webhooks.disable_after_dead", 3)
    httpd.on("/bad", 400)
    hook, _ = subscribe(ip, httpd.url + "/bad")
    for _ in range(20):
        emit(ip)
    ip.hooks.fanout()
    ip.hooks.deliver_due()
    row = ip.hooks.get(hook["id"])
    assert row["enabled"] is False and "consecutive" in row["disabled_reason"]
    assert row["consecutive_failures"] == 3, row["consecutive_failures"]          # exactly the limit, not the size of the backlog
    assert len(httpd.hits) < 20                                                    # attempts already on the wire may finish, but the rest of the backlog is never sent
    # nothing is left queued for the disabled subscription, and further passes are silent
    states = {d["state"] for d in deliveries(ip, hook)}
    assert states <= {"dead"}, states
    hits, failures = len(httpd.hits), row["consecutive_failures"]
    emit(ip)
    drain(ip, 3)
    assert len(httpd.hits) == hits and ip.hooks.get(hook["id"])["consecutive_failures"] == failures


def test_a_manually_disabled_webhook_stops_retrying_and_counting(ipw: Inproc, httpd: Any) -> None:
    ip = ipw
    httpd.on("/flaky", 503)
    hook, _ = subscribe(ip, httpd.url + "/flaky", max_attempts=5)
    emit(ip)
    drain(ip)
    assert [d["state"] for d in deliveries(ip, hook)] == ["retrying"]
    hits = len(httpd.hits)
    ip.hooks.update(ip.admin, hook["id"], {"enabled": False})
    ip.clock.advance(hours=2)                                           # type: ignore[attr-defined]   # past every backoff
    drain(ip, 3)
    assert len(httpd.hits) == hits                                                 # no retry noise against the endpoint
    assert [d["state"] for d in deliveries(ip, hook)] == ["dead"]
    assert ip.hooks.get(hook["id"])["consecutive_failures"] == 0


def test_connection_errors_and_timeouts_are_retried(ipw: Inproc, httpd: Any) -> None:
    ip = ipw
    dead_port = closed_port()
    ip.ctx.service("settings").set("network.ssrf.allow", ["127.0.0.0/8"])
    hook, _ = subscribe(ip, f"http://127.0.0.1:{dead_port}/x", max_attempts=2)
    emit(ip)
    drain(ip)
    [d] = deliveries(ip, hook)
    assert d["state"] == "retrying" and d["last_status_code"] is None and d["last_error"]
    httpd.on("/slow", ("sleep", 2.5, 200))
    slow, _ = subscribe(ip, httpd.url + "/slow", timeout_seconds=1, name="slow")
    emit(ip)
    drain(ip)
    [d2] = deliveries(ip, slow)
    assert d2["state"] == "retrying" and "timed out" in d2["last_error"]


def test_redirects_are_not_followed(ipw: Inproc, httpd: Any) -> None:
    ip = ipw
    httpd.on("/redir", (302, "", {"Location": "http://169.254.169.254/latest/meta-data"}))
    hook, _ = subscribe(ip, httpd.url + "/redir")
    emit(ip)
    drain(ip)
    [d] = deliveries(ip, hook)
    assert d["state"] == "dead" and d["last_status_code"] == 302 and len(httpd.hits) == 1


def test_response_bodies_are_truncated_and_redacted_in_the_log(ipw: Inproc, httpd: Any) -> None:
    ip = ipw
    httpd.on("/leaky", (500, "oops password=hunter2hunter2 " + "x" * 50_000, {}))
    hook, _ = subscribe(ip, httpd.url + "/leaky")
    emit(ip)
    drain(ip)
    [d] = deliveries(ip, hook)
    assert "hunter2hunter2" not in d["last_error"] and len(d["last_error"]) < 1200


# ------------------------------------------------------------------------------------------------------------------------------ test delivery, CRUD, leadership
def test_test_delivery_endpoint(ipw: Inproc, httpd: Any) -> None:
    ip = ipw
    hook, shown = subscribe(ip, httpd.url + "/t", ["job.*"])
    d = ip.hooks.test(ip.admin, hook["id"])
    assert d["state"] == "succeeded" and d["is_test"] is True and d["event_type"] == "webhook.test" and d["attempts"] == 1
    [hit] = httpd.to("/t")
    assert hit.json()["type"] == "webhook.test" and verify_signature(shown["secret"], hit.headers["x-olcf-timestamp"], hit.body, hit.headers["x-olcf-signature"], tolerance_seconds=None)
    httpd.on("/t", 500)
    bad = ip.hooks.test(ip.admin, hook["id"])
    assert bad["state"] == "dead" and "HTTP 500" in bad["last_error"]                       # a test never retries and never counts against the subscription's health
    assert ip.hooks.get(hook["id"])["consecutive_failures"] == 0


def test_update_delete_and_authorization(ipw: Inproc, httpd: Any) -> None:
    ip = ipw
    hook, _ = subscribe(ip, httpd.url + "/u", ["job.*"])
    upd = ip.hooks.update(ip.admin, hook["id"], {"event_types": ["pipeline.*", "job.*"], "name": "renamed", "max_attempts": 2, "headers": {"X-Team": "data"}})
    for bad in ({"max_attempts": 0}, {"timeout_seconds": 100}, {"timeout_seconds": "x"}):
        with pytest.raises(ValidationFailed):
            ip.hooks.update(ip.admin, hook["id"], bad)
    assert upd["event_types"] == ["job.*", "pipeline.*"] and upd["name"] == "renamed" and upd["etag"] != hook["etag"] and upd["headers"] == {"X-Team": "data"}
    emit(ip)
    ip.hooks.delete(ip.admin, hook["id"])                                                    # pending deliveries are dropped with the subscription
    drain(ip)
    assert httpd.hits == []
    with pytest.raises(NotFound):
        ip.hooks.get(hook["id"])
    # a user without webhook.manage cannot even tell that the subscription exists
    ident = ip.ctx.service("identity")
    row = ident.create_principal("viewer1", kind="user", password="correct horse battery staple")
    ident.bind(row["id"], "Viewer", scope_type="workspace", scope_id=ip.ws_id)
    viewer = ip.ctx.authz.principal_from_row(row)
    h2, _ = subscribe(ip, httpd.url + "/v", ["*"], workspace_id=ip.ws_id)
    with pytest.raises(NotFound):
        ip.hooks.get_authorized(viewer, h2["id"])
    with pytest.raises(Exception):  # noqa: B017 - Forbidden
        ip.hooks.create(viewer, {"name": "x", "url": httpd.url + "/x", "event_types": ["*"], "workspace_id": ip.ws_id})
    assert ip.hooks.list(viewer) == []


def test_dispatcher_runs_on_the_leader_only(ipw: Inproc, httpd: Any) -> None:
    ip = ipw
    hook, _ = subscribe(ip, httpd.url + "/l")
    emit(ip)
    ip.ctx.leader.release()
    from olcf.pipelines.common import LeaderLoop

    ran: list[int] = []
    loop = LeaderLoop(ip.ctx, "t", lambda: ran.append(1), 0.05, ip.hooks.loop.log)
    loop.start()
    import time

    time.sleep(0.3)
    loop.stop()
    assert ran == []                                                                         # a follower never runs the loop body
    ip.ctx.leader.acquire()
    loop.start()
    time.sleep(0.3)
    loop.stop()
    assert ran
    assert httpd.hits == []                                                                  # (nothing was delivered by the generic loop; the dispatcher is exercised via tick())
    drain(ip)
    assert len(httpd.hits) == 1 and hook["id"]


def test_large_payloads_are_truncated_in_the_envelope(ipw: Inproc, httpd: Any) -> None:
    ip = ipw
    hook, _ = subscribe(ip, httpd.url + "/big")
    emit(ip, blob="x" * 300_000)
    drain(ip)
    [hit] = httpd.to("/big")
    env = hit.json()
    assert env["data"].get("truncated") is True and len(hit.body) < 200_000 and "hint" in env["data"]
    assert hook["id"]


def test_housekeeping_purges_old_finished_deliveries(ipw: Inproc, httpd: Any) -> None:
    ip = ipw
    hook, _ = subscribe(ip, httpd.url + "/p")
    emit(ip)
    drain(ip)
    assert ip.hooks.purge() == 0
    ip.clock.advance(0)                                                                      # type: ignore[attr-defined]
    ip.clock.t = ip.clock.t + timedelta(days=15)                                             # type: ignore[attr-defined]
    assert ip.hooks.purge() == 1 and deliveries(ip, hook) == []
    assert ip.hooks.stats()["deliveries_by_state"] == {}


# ------------------------------------------------------------------------------------------------------------------------------ audit bridge: security.* and fabric.*
def test_audit_records_become_security_and_fabric_events(ipw: Inproc, httpd: Any) -> None:
    ip = ipw
    bridge = AuditEventBridge(ip.ctx)
    bridge.start()                                                                           # seeds the cursor at "now": history is not replayed
    hook, _ = subscribe(ip, httpd.url + "/sec", ["security.*", "fabric.*"])
    ip.ctx.audit.record(None, "auth.login_failed", resource_type="principal", resource_id="mallory", outcome="failure", ip="203.0.113.9", details={"reason": "bad password"})
    ip.ctx.audit.record(ip.admin, "identity.binding_changed", resource_type="principal", resource_id="usr_x", details={"role": "Admin"})
    ip.ctx.audit.record(ip.admin, "fabric.submission.rejected", resource_type="fabric_submission", resource_id="sub_1", outcome="failure", details={"reason": "bad signature"})
    ip.ctx.audit.record(ip.admin, "item.created", resource_type="notebook", resource_id="itm_1")      # not security relevant
    ip.ctx.audit.record(ip.admin, "queue.paused", resource_type="queue", resource_id="q1", outcome="denied")
    assert bridge.tick() >= 4                                                                # the 4 records above (+ the subscription's own secret / webhook audit records)
    assert bridge.tick() == 0                                                                # exactly once
    drain(ip, 2)
    got = sorted(h.json()["type"] for h in httpd.to("/sec") if h.json()["type"] not in ("security.secret_changed", "security.webhook_changed"))
    assert got == ["fabric.submission.rejected", "security.access_denied", "security.login_failed", "security.role_changed"]
    failed = next(h.json() for h in httpd.to("/sec") if h.json()["type"] == "security.login_failed")
    assert failed["data"]["ip"] == "203.0.113.9" and failed["data"]["resource_id"] == "mallory" and failed["data"]["source"] == "audit"
    assert bridge.event_type_for({"action": "fabric.handshake", "outcome": "failure"}) == "fabric.handshake.failed"
    assert bridge.event_type_for({"action": "fabric.handshake", "outcome": "success"}) == "fabric.handshake"
    assert hook["id"]
