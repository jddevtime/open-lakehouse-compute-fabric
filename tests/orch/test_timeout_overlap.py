# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""A timed-out activity attempt never overlaps its retry and never causes a duplicate effect (review finding F13).

The engine used to conclude a timed-out attempt (scheduling its retry) *before* it signalled cancellation, so a non-cooperative activity, an HTTP request that was
already accepted by the server, or a job whose cancel was still in flight could still commit while the retry ran.  Contract tested here:

* the attempt is cancelled and the engine waits - up to ``policy.cancelGraceSeconds`` - until it has actually stopped; only then is it recorded and maybe retried;
* work that ran past its deadline, or whose outcome cannot be known (``timeout_uncertain``), is replayed only when it is idempotent; the operator is told;
* HTTP: a request that was never sent is always repeatable; one that was sent but not answered is not repeated for POST / PUT / PATCH / DELETE unless it carries an
  ``Idempotency-Key`` (or ``policy.idempotent``); GET is always repeatable.
"""

from __future__ import annotations

import socket
import time
from typing import Any

import pytest

from tests.orch import probes
from tests.orch.orch_support import Inproc
from tests.orch.test_engine import P


@pytest.fixture(autouse=True)
def _no_orphans_outlive_the_test() -> Any:
    """Non-cooperative probes keep running after their attempt was abandoned (that is the point); let them end before the next test resets the shared probe state."""
    yield
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline and any(s["end"] is None for s in list(probes.SPANS) if s["name"] in {"S", "U", "C"}):
        time.sleep(0.05)


def A(name: str, type_: str, *deps: Any, policy: dict[str, Any] | None = None, **props: Any) -> dict[str, Any]:  # noqa: N802
    return P(name, *deps, policy=policy, type_=type_, **props)


def overlaps(spans: list[dict[str, Any]]) -> bool:
    """True when any two attempt spans (monotonic start/end) of the same activity overlap."""
    done = sorted((s for s in spans if s["end"] is not None), key=lambda s: s["start"])
    return any(b["start"] < a["end"] - 0.02 for a, b in zip(done, done[1:], strict=False))


def attempts_of(act: dict[str, Any]) -> list[dict[str, Any]]:
    return list(act.get("attempts") or [])


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ------------------------------------------------------------------------------------------------------------- the reviewer's scenario (engine level)
def test_non_cooperative_activity_is_not_retried_while_or_after_it_runs_when_not_idempotent(ip: Inproc) -> None:
    """timeoutSeconds=1, retries=1, idempotent=false, an activity that ignores cancellation and takes 2 s: one attempt, one effect, no overlap."""
    t0 = time.monotonic()
    run, acts = ip.execute([A("S", "t_stubborn", sleep=2, policy={"timeoutSeconds": 1, "retries": 1, "retryBackoffSeconds": 0, "idempotent": False})], timeout=30)
    a = acts["S"][0]
    time.sleep(0.3)
    assert run["state"] == "failed" and a["state"] == "failed"
    assert len(probes.SPANS) == 1, "the retry must not start while - or because - the original attempt was still running"
    assert len(probes.EFFECTS) == 1, "the effect must happen exactly once"
    assert a["error"]["category"] == "timeout" and a["error"]["retryable"] is False
    assert a["error"]["details"]["lateOutcome"] == "succeeded" and a["error"]["details"]["timedOut"] is True
    assert a["attempt"] == 1
    assert time.monotonic() - t0 >= 1.9, "the engine waited for the attempt to stop instead of concluding it at the deadline"
    assert not overlaps(probes.SPANS)


def test_an_idempotent_retry_starts_only_after_the_previous_attempt_has_stopped(ip: Inproc) -> None:
    """Before the fix attempt 2 started ~1 s in while attempt 1 ran until 2 s (0.95 s overlap, reproduced by the reviewer); now it starts after attempt 1 ended."""
    run, acts = ip.execute([A("S", "t_stubborn", sleep=2, stubbornAttempts=1, policy={"timeoutSeconds": 1, "retries": 1, "retryBackoffSeconds": 0, "idempotent": True})], timeout=30)
    assert run["state"] == "succeeded", run
    spans = sorted(probes.SPANS, key=lambda s: s["start"])
    assert [s["attempt"] for s in spans] == [1, 2]
    assert spans[1]["start"] >= spans[0]["end"] - 0.02, f"attempt 2 started at {spans[1]['start']:.2f} while attempt 1 ran until {spans[0]['end']:.2f}"
    assert not overlaps(probes.SPANS)
    assert acts["S"][0]["attempt"] == 2 and acts["S"][0]["error"] is None


def test_cooperative_activity_stops_at_the_deadline_and_is_retried_without_overlap(ip: Inproc) -> None:
    run, acts = ip.execute([A("C", "t_probe", sleep=30, policy={"timeoutSeconds": 1, "retries": 1, "retryBackoffSeconds": 0})], timeout=30)
    a = acts["C"][0]
    assert run["state"] == "failed" and a["attempt"] == 2 and a["error"]["category"] == "timeout"
    calls = probes.calls("C")
    assert [c["attempt"] for c in calls] == [1, 2]
    assert probes.ACTIVE["max"] == 1, "never two attempts of the activity at the same time"
    assert [x["state"] for x in attempts_of(a)] == ["failed", "failed"]


def test_grace_period_expiry_marks_the_outcome_uncertain_and_does_not_retry_non_idempotent_work(ip: Inproc) -> None:
    events: list[dict[str, Any]] = []
    ip.ctx.events.subscribe(events.append)
    run, acts = ip.execute([A("U", "t_stubborn", sleep=4, policy={"timeoutSeconds": 1, "cancelGraceSeconds": 1, "retries": 2, "retryBackoffSeconds": 0})], timeout=30)
    a = acts["U"][0]
    assert run["state"] == "failed" and a["state"] == "failed"
    err = a["error"]
    assert err["category"] == "timeout_uncertain" and err["code"] == "timeout_uncertain" and err["retryable"] is False
    assert err["details"]["outcome"] == "unknown" and err["details"]["attemptStillRunning"] is True and "resume" in err["details"]["operatorAction"]
    assert "/v1/pipeline-runs/" in err["details"]["operatorAction"] and "/v1/pipelines/runs" not in err["details"]["operatorAction"]  # the route that exists
    time.sleep(2.5)                                           # the orphaned thread finishes and tries to report: it must not change the record
    assert len(probes.SPANS) == 1 and len(probes.EFFECTS) == 1
    again = ip.act(run["id"], "U")
    assert again["state"] == "failed" and again["error"]["category"] == "timeout_uncertain"
    unc = [e for e in events if e["type"] == "pipeline.activity.outcome_uncertain"]
    assert len(unc) == 1 and unc[0]["payload"]["activity"] == "U" and unc[0]["payload"]["retried"] is False
    assert any(e["type"] == "pipeline.activity.failed" for e in events)


def test_idempotent_uncertain_attempt_is_retried_and_the_operator_is_still_told(ip: Inproc) -> None:
    events: list[dict[str, Any]] = []
    ip.ctx.events.subscribe(events.append)
    run, acts = ip.execute([A("U", "t_stubborn", sleep=4, stubbornAttempts=1, policy={"timeoutSeconds": 1, "cancelGraceSeconds": 0.5, "retries": 1, "retryBackoffSeconds": 0, "idempotent": True})], timeout=30)
    assert run["state"] == "succeeded", run
    assert acts["U"][0]["attempt"] == 2
    assert [e["payload"]["retried"] for e in events if e["type"] == "pipeline.activity.outcome_uncertain"] == [True]


def test_the_default_grace_is_configurable_per_engine(ip: Inproc) -> None:
    ip.engine.timeout_cancel_grace = 0.5
    run, acts = ip.execute([A("U", "t_stubborn", sleep=3, policy={"timeoutSeconds": 1, "retries": 1, "retryBackoffSeconds": 0})], timeout=30)
    assert acts["U"][0]["error"]["category"] == "timeout_uncertain" and run["state"] == "failed" and len(probes.SPANS) == 1


def test_cancel_grace_policy_is_validated_and_part_of_the_schema(ip: Inproc) -> None:
    from olcf.core.errors import ValidationFailed

    with pytest.raises(ValidationFailed):
        ip.pipeline("bad-grace", [A("S", "t_stubborn", policy={"cancelGraceSeconds": -1})])
    ok = ip.pipeline("ok-grace", [A("S", "t_stubborn", policy={"cancelGraceSeconds": 0})])
    assert ok["id"]


# ------------------------------------------------------------------------------------------------------------- jobs (asynchronous activities)
def test_job_retry_waits_for_the_previous_job_to_reach_a_terminal_state(ip: Inproc) -> None:
    """The first job's cancel takes 1.5 s to be acknowledged: attempt 2 (a new job) may only start after the first job is terminal."""
    run, acts = ip.execute([A("J", "t_job", runFor=60, cancelDelay=1.5, stubbornAttempts=1, policy={"timeoutSeconds": 1, "retries": 1, "retryBackoffSeconds": 0})], timeout=40)
    spans = sorted(probes.SPANS, key=lambda s: s["start"])
    assert [s["attempt"] for s in spans] == [1, 2]
    assert spans[0]["end"] is not None and spans[1]["start"] >= spans[0]["end"] - 0.02, "attempt 2 started before job 1 was terminal"
    assert probes.CANCELS and probes.CANCELS[0]["attempt"] == 1
    assert run["state"] == "succeeded" and acts["J"][0]["attempt"] == 2


def test_job_whose_cancel_is_never_acknowledged_ends_uncertain_and_is_not_resubmitted(ip: Inproc) -> None:
    run, acts = ip.execute([A("J", "t_job", runFor=60, cancelDelay=60, policy={"timeoutSeconds": 1, "cancelGraceSeconds": 1, "retries": 3, "retryBackoffSeconds": 0})], timeout=40)
    a = acts["J"][0]
    assert run["state"] == "failed" and a["error"]["category"] == "timeout_uncertain" and a["error"]["retryable"] is False
    assert len(probes.SPANS) == 1, "no second job may be submitted while the first one may still be running"
    assert len(probes.CANCELS) >= 1


# ------------------------------------------------------------------------------------------------------------- HTTP
HTTP_POLICY = {"timeoutSeconds": 1, "retries": 1, "retryBackoffSeconds": 0}


def test_post_that_is_accepted_but_not_answered_before_the_timeout_is_not_repeated(ip: Inproc, httpd: Any) -> None:
    """The server accepts the POST and commits it 2 s later although the pipeline gave up after 1 s: exactly one commit, one request, no retry."""
    httpd.on("/orders", ("commit_after", 2.0, 201))
    run, acts = ip.execute([A("Post", "http", url=httpd.url + "/orders", method="POST", body={"n": 1}, policy=HTTP_POLICY)], timeout=30)
    a = acts["Post"][0]
    assert run["state"] == "failed" and a["state"] == "failed"
    assert a["error"]["category"] == "timeout_uncertain" and a["error"]["retryable"] is False
    assert a["error"]["details"]["requestSent"] is True and a["error"]["details"]["outcome"] == "unknown"
    time.sleep(2.5)                                    # let the server finish its slow commit
    assert len(httpd.to("/orders")) == 1, "the request must not be sent a second time"
    assert len(httpd.committed("/orders")) == 1, "the effect happens once"


@pytest.mark.parametrize("method", ["PUT", "PATCH", "DELETE"])
def test_other_non_idempotent_methods_are_not_repeated_either(ip: Inproc, httpd: Any, method: str) -> None:
    httpd.on("/thing", ("commit_after", 1.6, 200))
    run, acts = ip.execute([A("M", "http", url=httpd.url + "/thing", method=method, policy=HTTP_POLICY)], timeout=30)
    assert acts["M"][0]["error"]["category"] == "timeout_uncertain" and run["state"] == "failed"
    time.sleep(1.5)
    assert len(httpd.to("/thing")) == 1 and len(httpd.committed("/thing")) == 1


def test_post_with_an_idempotency_key_is_retried_with_the_same_key(ip: Inproc, httpd: Any) -> None:
    httpd.on("/orders", ("commit_after", 2.0, 201), 201)
    run, acts = ip.execute([A("Post", "http", url=httpd.url + "/orders", method="POST", body={"n": 1}, headers={"Idempotency-Key": "order-42"}, policy=HTTP_POLICY)], timeout=30)
    assert run["state"] == "succeeded" and acts["Post"][0]["attempt"] == 2
    keys = [h.headers.get("idempotency-key") for h in httpd.to("/orders")]
    assert keys == ["order-42", "order-42"], "a retry repeats the very same key so that the server can de-duplicate it"


def test_idempotency_key_property_generates_a_key_that_is_stable_across_attempts(ip: Inproc, httpd: Any) -> None:
    httpd.on("/orders", ("commit_after", 2.0, 201), 201)
    run, acts = ip.execute([A("Post", "http", url=httpd.url + "/orders", method="POST", body={"n": 1}, idempotencyKey=True, policy=HTTP_POLICY)], timeout=30)
    assert run["state"] == "succeeded"
    keys = [h.headers.get("idempotency-key") for h in httpd.to("/orders")]
    assert len(keys) == 2 and keys[0] == keys[1] and keys[0].startswith("olcf-")


def test_post_with_policy_idempotent_is_retried(ip: Inproc, httpd: Any) -> None:
    httpd.on("/orders", ("commit_after", 2.0, 201), 201)
    run, _ = ip.execute([A("Post", "http", url=httpd.url + "/orders", method="POST", body={"n": 1}, policy={**HTTP_POLICY, "idempotent": True})], timeout=30)
    assert run["state"] == "succeeded" and len(httpd.to("/orders")) == 2


def test_get_that_times_out_is_aborted_promptly_and_retried(ip: Inproc, httpd: Any) -> None:
    httpd.on("/slow", ("sleep", 2.0, 200), 200)
    t0 = time.monotonic()
    run, acts = ip.execute([A("G", "http", url=httpd.url + "/slow", policy=HTTP_POLICY)], timeout=30)
    assert run["state"] == "succeeded" and acts["G"][0]["attempt"] == 2
    assert time.monotonic() - t0 < 1.9 + 1.0, "the blocked read is aborted at the deadline instead of waiting for the slow server"


def test_the_pipeline_timeout_aborts_a_blocked_http_read_promptly(ip: Inproc, httpd: Any) -> None:
    httpd.on("/slow", ("commit_after", 3.0, 200))
    t0 = time.monotonic()
    run, acts = ip.execute([A("P", "http", url=httpd.url + "/slow", method="POST", body={}, policy={"timeoutSeconds": 1, "retries": 0})], timeout=30)
    assert acts["P"][0]["error"]["category"] == "timeout_uncertain"
    assert time.monotonic() - t0 < 2.5, "the attempt stopped at the deadline: the engine did not have to wait for its grace period"
    assert run["state"] == "failed"


def test_requests_that_were_never_sent_are_always_retried_even_for_post(ip: Inproc) -> None:
    port = free_port()                                  # nothing listens here: connection refused, i.e. provably nothing was sent
    run, acts = ip.execute([A("Refused", "http", url=f"http://127.0.0.1:{port}/x", method="POST", body={}, policy={"retries": 2, "retryBackoffSeconds": 0})], timeout=30)
    a = acts["Refused"][0]
    assert run["state"] == "failed" and a["attempt"] == 3, "a failure that proves nothing was sent is retried"
    assert a["error"]["category"] == "network" and a["error"]["details"]["requestSent"] is False


def test_connection_reset_after_the_request_was_sent_is_uncertain_for_post_but_retried_for_get(ip: Inproc) -> None:
    import threading

    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(8)
    accepted: list[bytes] = []

    def serve() -> None:
        srv.settimeout(0.2)
        end = time.monotonic() + 15
        while time.monotonic() < end:
            try:
                c, _ = srv.accept()
            except OSError:
                continue
            c.settimeout(2)
            try:
                accepted.append(c.recv(65536))   # read the request, then hang up without answering
            except OSError:
                pass
            c.close()

    th = threading.Thread(target=serve, daemon=True)
    th.start()
    url = f"http://127.0.0.1:{srv.getsockname()[1]}/x"
    try:
        run, acts = ip.execute([A("Post", "http", url=url, method="POST", body={"a": 1}, policy={"retries": 2, "retryBackoffSeconds": 0}),
                                A("Get", "http", url=url, policy={"retries": 2, "retryBackoffSeconds": 0})], timeout=30)
    finally:
        srv.close()
    post, get = acts["Post"][0], acts["Get"][0]
    assert post["attempt"] == 1 and post["error"]["category"] == "network_uncertain" and post["error"]["retryable"] is False
    assert get["attempt"] == 3 and get["error"]["category"] == "network"
    assert sum(b.startswith(b"POST") for b in accepted) == 1, "the POST was delivered once and never replayed"


def test_server_errors_are_not_replayed_for_post_unless_safe(ip: Inproc, httpd: Any) -> None:
    httpd.on("/p500", 500)
    httpd.on("/p503", 503, 200)
    httpd.on("/k500", 500, 200)
    httpd.on("/k503", 503, 200)
    httpd.on("/get503", 503, 200)
    httpd.on("/p429", 429, 200)
    run, acts = ip.execute([
        A("Plain", "http", url=httpd.url + "/p500", method="POST", body={}, policy={"retries": 2, "retryBackoffSeconds": 0}),
        A("Busy", "http", url=httpd.url + "/p503", method="POST", body={}, policy={"retries": 2, "retryBackoffSeconds": 0}),
        A("Keyed", "http", url=httpd.url + "/k500", method="POST", body={}, headers={"Idempotency-Key": "k-1"}, policy={"retries": 2, "retryBackoffSeconds": 0}),
        A("KeyedBusy", "http", url=httpd.url + "/k503", method="POST", body={}, headers={"Idempotency-Key": "k-2"}, policy={"retries": 2, "retryBackoffSeconds": 0}),
        A("GetBusy", "http", url=httpd.url + "/get503", policy={"retries": 2, "retryBackoffSeconds": 0}),
        A("Throttled", "http", url=httpd.url + "/p429", method="POST", body={}, policy={"retries": 2, "retryBackoffSeconds": 0}),
    ], timeout=30)
    assert acts["Plain"][0]["attempt"] == 1 and acts["Plain"][0]["state"] == "failed"
    assert "retrySuppressed" in acts["Plain"][0]["error"]["details"]
    # a gateway can answer 503 after the upstream committed the request: for an unsafe, un-keyed POST it is uncertain and sent exactly once
    assert acts["Busy"][0]["state"] == "failed" and acts["Busy"][0]["attempt"] == 1 and len(httpd.to("/p503")) == 1
    assert acts["Busy"][0]["error"]["retryable"] is False and "retrySuppressed" in acts["Busy"][0]["error"]["details"]
    assert acts["KeyedBusy"][0]["state"] == "succeeded" and acts["KeyedBusy"][0]["attempt"] == 2, "an explicitly idempotent request keeps its 503 retry"
    assert acts["GetBusy"][0]["state"] == "succeeded" and acts["GetBusy"][0]["attempt"] == 2, "a safe method keeps its 503 retry"
    assert acts["Throttled"][0]["state"] == "succeeded" and acts["Throttled"][0]["attempt"] == 2, "429 is a known rejection"
    assert acts["Keyed"][0]["state"] == "succeeded" and acts["Keyed"][0]["attempt"] == 2
    assert len(httpd.to("/p500")) == 1 and run["state"] == "failed"


def test_activity_read_timeout_of_a_post_is_uncertain(ip: Inproc, httpd: Any) -> None:
    """``timeoutSeconds`` of the http activity itself (the socket timeout) fires before the engine's: still no replay of the POST."""
    httpd.on("/orders", ("commit_after", 2.0, 201))
    run, acts = ip.execute([A("Post", "http", url=httpd.url + "/orders", method="POST", body={}, timeoutSeconds=1, policy={"retries": 2, "retryBackoffSeconds": 0})], timeout=30)
    a = acts["Post"][0]
    assert a["error"]["category"] == "timeout_uncertain" and a["attempt"] == 1 and run["state"] == "failed"
    time.sleep(1.5)
    assert len(httpd.to("/orders")) == 1 and len(httpd.committed("/orders")) == 1


# ------------------------------------------------------------------------------------------------------------- unchanged behaviour
def test_run_cancel_still_cancels_a_running_activity(ip: Inproc) -> None:
    p = ip.pipeline("cancel-me", [A("S", "t_probe", sleep=30)])
    r = ip.run(p)
    from tests.orch.orch_support import wait_until

    wait_until(lambda: probes.calls("S"), 10, 0.05, "the activity to start")
    ip.svc.cancel(ip.admin, r["id"])
    final = ip.wait(r["id"], timeout=20)
    assert final["state"] == "cancelled" and ip.act(r["id"], "S")["state"] == "cancelled"


def test_the_documented_resume_route_is_the_one_the_api_serves() -> None:
    """The engine docstring, CONTRACTS and the operator message all name POST /v1/pipeline-runs/{id}/resume; the wrong spelling must not come back."""
    from pathlib import Path

    from olcf.pipelines import api, engine, module

    root = Path(__file__).resolve().parents[2]
    assert "/v1/pipeline-runs/{id}/resume" in (engine.__doc__ or "")
    for text in ((engine.__doc__ or ""), Path(engine.__file__).read_text(), (root / "docs" / "dev" / "CONTRACTS.md").read_text()):
        assert "/v1/pipelines/runs/" not in text
    routes = {(m, r.path) for r in api.runs_router.routes for m in getattr(r, "methods", set())}
    assert any(m == "POST" and p.endswith("/{run_id}/resume") for m, p in routes)
    assert "(api.runs_router, \"/pipeline-runs\")" in Path(module.__file__).read_text()  # mounted under /v1/pipeline-runs
