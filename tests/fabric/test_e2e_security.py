# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""End to end rejection paths: tampering, replay, expiry, wrong tenant / workspace, oversize, unsupported API, failures and isolation of the Fabric principal.

Each test drives the real SDK pieces (challenge, canonical JSON, HMAC) against the real control plane; the capsule is built by hand only where a test must break it.
"""

from __future__ import annotations

import copy
import datetime as dt
import io
import json
from typing import Any

import pytest

from olcf_fabric import capsule as cap
from olcf_fabric import fabric as fab
from olcf_fabric._canonical import sign_capsule
from olcf_fabric.client import get_client
from olcf_fabric.errors import (
    AuthenticationError,
    CompatibilityError,
    RemoteExecutionError,
    ServerError,
    SubmissionRejected,
)
from tests.fabric.fabric_fakes import BROKER_SECRET, FABRIC_WS, OTHER_FABRIC_WS, OTHER_TENANT, TENANT
from tests.fabric.fabric_support import E2E, SUBMIT_CELL, ipynb, notebook_context

OK_NB = ipynb([("code", "n = 1", ["parameters"]), ("code", "print('ok', n)\nnotebookutils.notebook.exit('done')"), ("code", SUBMIT_CELL)])


def build_parts(s: Any, text: str, **params: Any) -> dict[str, Any]:
    """The pre-signature request body, assembled as ``run_remote`` does."""
    ctxn = fab.normalise_context(fab.runtime_context())
    src = cap.source_block(mode="inline", fmt="ipynb", content=text, notebook_id=ctxn.get("currentNotebookId"), notebook_name=ctxn.get("currentNotebookName"),
                           workspace_id=ctxn.get("currentWorkspaceId"), workspace_name=ctxn.get("currentWorkspaceName"), original=("ipynb", text), excluded=[])
    return cap.request_parts(source=src, context={"normalized": ctxn, "raw": fab.raw_context(fab.runtime_context())}, runtime={"family": "fabric-2.0-compatible", "constraints": {}, "sparkConf": {}},
                             parameters=dict(params), data_references={"resolved": [], "logical": []}, policy={}, identity={"tenantId": TENANT, "userId": ctxn.get("userId")}, secret_refs=[])


def signed_capsule(s: Any, text: str = OK_NB, **params: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    parts = build_parts(s, text, **params)
    ch = get_client().challenge()
    return cap.build_signed(parts, ch, idempotency_key=None, correlation_id=None), ch


def resign(signed: dict[str, Any], ch: dict[str, Any]) -> dict[str, Any]:
    signed["signature"] = sign_capsule(signed["capsule"], ch["key"], ch["keyId"])
    return signed


def submit_error(signed: dict[str, Any]) -> ServerError:
    with pytest.raises(ServerError) as ei:
        get_client().submit(signed)
    return ei.value


def fabric_jobs(e2e: E2E) -> int:
    r = e2e.st.http.get("/v1/jobs", headers=e2e.st.admin, params={"limit": 500})
    items = r.json().get("items", r.json()) if r.status_code == 200 else []
    return sum(1 for j in items if (j.get("origin") or {}).get("kind") == "fabric")


# ------------------------------------------------------------------------------------------------ integrity and replay
def test_tampered_capsule_is_rejected_and_nothing_runs(e2e: E2E, session: Any) -> None:
    session.make(notebook=OK_NB)
    before = fabric_jobs(e2e)
    signed, _ = signed_capsule(session, n=1)
    signed["capsule"]["parameters"]["n"] = 999                      # changed after signing
    err = submit_error(signed)
    assert err.status == 400 and err.code == "capsule_signature_invalid"
    signed2, _ = signed_capsule(session)
    signed2["capsule"]["source"]["content"] += "\nimport os; os.system('id')\n"   # source changed after signing
    assert submit_error(signed2).code == "capsule_signature_invalid"
    signed3, _ = signed_capsule(session)
    signed3["signature"]["mac"] = "0" * 64
    assert submit_error(signed3).code == "capsule_signature_invalid"
    signed4, _ = signed_capsule(session)
    signed4["signature"]["keyId"] = "k-unknown"
    assert submit_error(signed4).status == 400
    assert fabric_jobs(e2e) == before
    rows = e2e.st.http.get("/v1/integrations/fabric/submissions", headers=e2e.st.admin, params={"state": "rejected"}).json()["items"]
    assert any("capsule_signature_invalid" in (r.get("error") or "") for r in rows)       # audited and listed, with the reason


def test_wrong_key_signature_is_rejected(e2e: E2E, session: Any) -> None:
    session.make(notebook=OK_NB)
    signed, ch = signed_capsule(session)
    other = get_client().challenge()                                # a key issued for another nonce
    signed["signature"] = sign_capsule(signed["capsule"], other["key"], ch["keyId"])
    assert submit_error(signed).code == "capsule_signature_invalid"


def test_exact_resend_is_idempotent_and_a_modified_resigned_capsule_is_a_replay(e2e: E2E, session: Any) -> None:
    session.make(notebook=OK_NB)
    signed, ch = signed_capsule(session, n=5)
    first = get_client().submit(copy.deepcopy(signed))
    again = get_client().submit(copy.deepcopy(signed))
    assert again["id"] == first["id"] and again.get("job_id") == first.get("job_id") and again.get("replayed") is True
    forged = copy.deepcopy(signed)                                  # attacker holding the key edits and re-signs with the SAME nonce
    forged["capsule"]["parameters"]["n"] = 6
    resign(forged, ch)
    err = submit_error(forged)
    assert err.status == 409 and err.code == "capsule_replay"
    sub = e2e.st.http.get(f"/v1/integrations/fabric/submissions/{first['id']}", headers=e2e.headers).json()
    assert sub["parameters"] == {"n": 5}                            # the original run is untouched
    other_id = copy.deepcopy(signed)                                 # same nonce, new submission id and idempotency key
    other_id["capsule"]["submissionId"] = "sub_01ZZZZZZZZZZZZZZZZZZZZZZZZ"
    other_id["capsule"]["idempotencyKey"] = "another-key"
    resign(other_id, ch)
    assert submit_error(other_id).status in (400, 409)


def test_nonce_is_single_use_across_a_fresh_submission_id(e2e: E2E, session: Any) -> None:
    session.make(notebook=OK_NB)
    a, _ = signed_capsule(session)
    get_client().submit(a)
    b, ch_b = signed_capsule(session)
    b["capsule"]["security"]["nonce"] = a["capsule"]["security"]["nonce"]   # try to reuse the first nonce
    resign(b, ch_b)
    assert submit_error(b).status in (400, 409)


def test_expired_and_future_timestamps_are_rejected(e2e: E2E, session: Any) -> None:
    session.make(notebook=OK_NB)
    max_age = e2e.st.settings.fabric.capsule_max_age_seconds
    for shift in (-(max_age + 120), max_age + 120):
        signed, ch = signed_capsule(session)
        created = cap.parse_iso(signed["capsule"]["createdAt"]) + dt.timedelta(seconds=shift)
        signed["capsule"]["createdAt"] = cap.iso(created)
        resign(signed, ch)
        err = submit_error(signed)
        assert err.status == 400 and err.code == "capsule_expired", (shift, err)


def test_unknown_nonce_is_rejected(e2e: E2E, session: Any) -> None:
    session.make(notebook=OK_NB)
    signed, ch = signed_capsule(session)
    signed["capsule"]["security"]["nonce"] = "n" * 32
    resign(signed, ch)
    assert submit_error(signed).status in (400, 409)


# ------------------------------------------------------------------------------------------------ tenant / workspace
def test_wrong_tenant_claim_is_rejected(e2e: E2E, session: Any) -> None:
    s = session.make(notebook=OK_NB)
    s.olcf.configure(endpoint=e2e.url, token=e2e.token, tenant_id=OTHER_TENANT, allow_insecure_http=True)
    with pytest.raises(SubmissionRejected) as ei:
        s.olcf.run_remote(wait=False)
    assert ei.value.code == "fabric_tenant_not_allowed" and ei.value.status == 403


def test_entra_token_from_another_tenant_is_not_authenticated(e2e: E2E, session: Any) -> None:
    s = session.make(notebook=OK_NB)
    bad = e2e.world.mint(tid=OTHER_TENANT)
    s.olcf.configure(endpoint=e2e.url, token=bad, allow_insecure_http=True)
    with pytest.raises(AuthenticationError):
        s.olcf.run_remote(wait=False)
    wrong_aud = e2e.world.mint(aud="api://someone-else")
    s.olcf.configure(endpoint=e2e.url, token=wrong_aud, allow_insecure_http=True)
    with pytest.raises(AuthenticationError):
        s.olcf.run_remote(wait=False)
    s.olcf.configure(endpoint=e2e.url, token=e2e.world.mint(exp_in=-120), allow_insecure_http=True)
    with pytest.raises(AuthenticationError):
        s.olcf.run_remote(wait=False)


def test_entra_token_caller_runs_a_notebook_and_is_confined_to_the_mapped_workspace(e2e: E2E, session: Any) -> None:
    s = session.make(notebook=OK_NB)
    token = e2e.world.mint(oid="11111111-1111-4111-8111-111111111111", appid="22222222-2222-4222-8222-222222222222")
    s.olcf.configure(endpoint=e2e.url, token=token, allow_insecure_http=True)
    run = s.olcf.run_remote(wait=False)
    run.wait(stream_logs=False, out=io.StringIO())
    assert run.state == "succeeded"
    hdr = {"Authorization": f"Bearer {token}"}
    http = e2e.st.http
    assert http.get(f"/v1/integrations/fabric/submissions/{run.submission_id}", headers=hdr).status_code == 200
    assert http.get(f"/v1/integrations/fabric/submissions/{run.submission_id}/logs", headers=hdr).status_code == 200
    # not allowed: submitting jobs directly, secrets, administration, other workspaces
    # an Entra token is only understood by the adapter's own endpoints; the rest of the API does not accept it at all
    assert http.get(f"/v1/jobs/{run.job_id}", headers=hdr).status_code == 401
    assert http.get("/v1/secrets", headers=hdr).status_code == 401
    assert http.put("/v1/integrations/fabric", headers=hdr, json={}).status_code == 403
    assert http.post("/v1/integrations/fabric/signing/rotate", headers=hdr).status_code == 403


def test_platform_token_principal_is_confined_too(e2e: E2E, session: Any) -> None:
    s = session.make(notebook=OK_NB)
    s.olcf.run_remote(wait=False)
    http, h = e2e.st.http, e2e.headers
    submit_body = {"kind": "notebook", "workspace_id": e2e.ws, "source": {"format": "py", "content": "print(1)"}, "engine": "python"}
    assert http.post("/v1/jobs", headers=h, json=submit_body).status_code == 403              # no job.submit: only the adapter may create jobs
    secrets = http.get("/v1/secrets", headers=h)
    assert secrets.status_code in (401, 403) or secrets.json()["items"] == []                # metadata listing is filtered by secret.manage
    assert http.put("/v1/integrations/fabric", headers=h, json={}).status_code == 403
    assert http.get("/v1/integrations/fabric/metrics", headers=h).status_code == 403
    lonely = e2e.st.service_principal(e2e.st.workspace())          # bound to a workspace no Fabric workspace maps to
    r = http.post("/v1/integrations/fabric/challenge", headers=lonely)
    assert r.status_code == 200                                      # may ask for a challenge ...
    signed = cap.build_signed(build_parts(session, OK_NB), r.json(), idempotency_key=None, correlation_id=None)   # ... and sign with its own key
    r = http.post("/v1/integrations/fabric/submit", headers=lonely, json=signed)
    assert r.status_code == 403                                      # ... but may not submit into a workspace it has no binding for
    own = e2e.st.http.get("/v1/integrations/fabric/submissions", headers=h).json()["items"]
    assert own, "the e2e principal sees its workspace's submissions"
    assert http.get(f"/v1/integrations/fabric/submissions/{own[0]['id']}", headers=lonely).status_code in (403, 404)
    admin_job = own[0].get("job_id")
    if admin_job:
        assert http.get(f"/v1/jobs/{admin_job}", headers=lonely).status_code in (403, 404)
        assert http.post(f"/v1/jobs/{admin_job}/cancel", headers=lonely).status_code in (403, 404)


def test_wrong_fabric_workspace_is_rejected(e2e: E2E, session: Any) -> None:
    s = session.make(notebook=OK_NB, context={"currentWorkspaceId": OTHER_FABRIC_WS})
    with pytest.raises(SubmissionRejected) as ei:
        s.olcf.run_remote(wait=False)
    assert ei.value.code == "fabric_workspace_not_allowed" and ei.value.status == 403
    assert ei.value.remediation
    assert FABRIC_WS != OTHER_FABRIC_WS


# ------------------------------------------------------------------------------------------------ size
def test_oversized_capsule_is_refused_early(e2e: E2E, session: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(e2e.st.settings.fabric, "capsule_max_bytes", 40_000)
    big = ipynb([("code", "x = '" + "a" * 120_000 + "'"), ("code", SUBMIT_CELL)])
    s = session.make(notebook=big)
    before = fabric_jobs(e2e)
    with pytest.raises(SubmissionRejected) as ei:
        s.olcf.run_remote(wait=False)
    assert ei.value.status == 413 and ei.value.code == "capsule_too_large"
    assert fabric_jobs(e2e) == before


# ------------------------------------------------------------------------------------------------ secrets
def test_capsule_with_a_secret_is_refused_and_not_stored(e2e: E2E, session: Any) -> None:
    secret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"  # noqa: S105 - the documented AWS example secret
    nb = ipynb([("code", f"aws_secret_access_key = '{secret}'\nprint('x')"), ("code", SUBMIT_CELL)])
    s = session.make(notebook=nb)
    with pytest.raises(SubmissionRejected) as ei:
        s.olcf.run_remote(wait=False)
    assert ei.value.code == "capsule_contains_secret"
    assert secret not in json.dumps(ei.value.details) and secret not in str(ei.value)
    rows = e2e.st.http.get("/v1/integrations/fabric/submissions", headers=e2e.st.admin, params={"state": "rejected"}).json()["items"]
    assert secret not in json.dumps(rows)
    known = ipynb([("code", f"client_secret = '{BROKER_SECRET}'")])      # the broker's own secret, known to the server, is caught even without a pattern
    s = session.make(notebook=known)
    with pytest.raises(SubmissionRejected) as ei:
        s.olcf.run_remote(wait=False)
    assert ei.value.code == "capsule_contains_secret" and BROKER_SECRET not in json.dumps(ei.value.details) + str(ei.value)
    pem = ipynb([("code", "k = '''-----BEGIN RSA PRIVATE KEY-----\nMIIBOgIBAAJBAKj34GkxFhD90vcNLYLInFEX6Ppy1tPf9Cnzj4p4WGeKLs1Pt8Qu\n-----END RSA PRIVATE KEY-----'''")])  # gitleaks:allow
    s = session.make(notebook=pem)
    with pytest.raises(SubmissionRejected) as ei:
        s.olcf.run_remote(wait=False)
    assert ei.value.code == "capsule_contains_secret"


# ------------------------------------------------------------------------------------------------ unsupported API
@pytest.mark.parametrize("code,rule_hint", [
    ("import sempy.fabric as fabric\ndf = fabric.read_table('x')", "sempy"),
    ("notebookutils.nonexistent.doThing()", "notebookutils"),
    ("import subprocess\nsubprocess.run('ls', shell=True)", "shell"),
])
def test_unsupported_api_fails_before_execution(e2e: E2E, session: Any, code: str, rule_hint: str) -> None:
    s = session.make(notebook=ipynb([("code", code), ("code", SUBMIT_CELL)]))
    before = fabric_jobs(e2e)
    try:
        s.olcf.run_remote(wait=False)
    except CompatibilityError as exc:
        assert exc.report and exc.report.get("blockers"), exc.report
        assert exc.remediation or any(b.get("remediation") for b in exc.report["blockers"])
    else:
        pytest.fail("expected a CompatibilityError")
    assert fabric_jobs(e2e) == before


def test_shell_calls_and_pip_are_blocked_by_policy(e2e: E2E, session: Any) -> None:
    s = session.make(notebook=ipynb([("code", "%pip install requests"), ("code", SUBMIT_CELL)]))
    with pytest.raises(CompatibilityError) as ei:
        s.olcf.run_remote(wait=False)
    assert any("pip" in json.dumps(b).lower() for b in ei.value.report["blockers"])


# ------------------------------------------------------------------------------------------------ failures
FAILING = ipynb([("code", "print('before the failure')"), ("code", "def boom():\n    raise ValueError('kaboom 42')\nboom()"), ("code", "print('never')"), ("code", SUBMIT_CELL)])


def test_failing_notebook_raises_with_cell_and_logs(e2e: E2E, session: Any) -> None:
    s = session.make(notebook=FAILING)
    buf = io.StringIO()
    with pytest.raises(RemoteExecutionError) as ei:
        run = s.olcf.run_remote(wait=False)
        run.wait(stream_logs=True, out=buf)
        s.olcf.finish(run)
    exc = ei.value
    assert exc.state == "failed" and "kaboom 42" in str(exc)
    assert exc.failure.get("cell") == 1 or exc.failure.get("cell") == 2 or "cell" in str(exc)
    assert "before the failure" in buf.getvalue() and "never" not in buf.getvalue()
    sub = e2e.st.http.get(f"/v1/integrations/fabric/submissions/{exc.submission_id}", headers=e2e.headers).json()
    assert sub["state"] == "failed" and sub["failure_category"]
    assert not s.nbu.exits                                           # notebookutils.notebook.exit was NOT called on failure


def test_pipeline_failure_propagates_to_the_activity(e2e: E2E, session: Any) -> None:
    ctx = {"isForPipeline": True, "parentRunId": "pipe-run-77", "rootRunId": "pipe-run-77", "currentRunId": "act-run-3"}
    s = session.make(notebook=FAILING, context=ctx)
    with pytest.raises(RemoteExecutionError):
        s.olcf.run_remote(stream_logs=False)                           # the exception is what makes the pipeline activity fail
    sub = e2e.st.http.get("/v1/integrations/fabric/submissions", headers=e2e.headers, params={"limit": 1}).json()["items"][0]
    assert sub["state"] == "failed"
    job = e2e.st.http.get(f"/v1/jobs/{sub['job_id']}", headers=e2e.st.admin).json()
    fabric_ctx = job["origin"]["details"]["fabric"]
    assert fabric_ctx["context"]["isForPipeline"] is True and fabric_ctx["context"]["parentRunId"] == "pipe-run-77"
    assert notebook_context()["isForPipeline"] is False


def test_pipeline_success_hands_the_exit_value_to_the_pipeline(e2e: E2E, session: Any) -> None:
    ctx = {"isForPipeline": True, "parentRunId": "pipe-run-78"}
    s = session.make(notebook=OK_NB, context=ctx)
    with pytest.raises(type(s.nbu).NotebookExit):
        s.olcf.run_remote(stream_logs=False)
    assert s.nbu.exits == ["done"]


def test_cancel_a_running_submission(e2e: E2E, session: Any) -> None:
    nb = ipynb([("code", "import time\nfor _ in range(120):\n    time.sleep(0.5)"), ("code", SUBMIT_CELL)])
    s = session.make(notebook=nb)
    run = s.olcf.run_remote(wait=False)
    from tests.fabric.fabric_stack import wait_until
    wait_until(lambda: s.olcf.status(run)["state"] in ("running", "provisioning", "queued"), 20, 0.3, "the run to start")
    s.olcf.cancel(run)
    wait_until(lambda: s.olcf.status(run)["state"] in ("cancelled", "cancelling", "failed"), 40, 0.5, "cancellation")
