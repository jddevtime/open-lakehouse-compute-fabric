# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Review F01: a credential derived for a job / kernel must never be broader than the credential that caused it.

The derived credential (``scopes=["job"]``) used to *replace* the submitting token's permission ceiling, so a Contributor holding a restricted ``mcp:write`` PAT could run
a job whose callback token held ``secret.use`` and ``lakehouse.write``. These tests drive the whole path (REST submit -> scheduler -> fake agent -> the credential the agent
receives -> HTTP with that credential) and assert the credential is the intersection of: the persisted ceiling of the submitting credential, the principal's *current* role
bindings and the minimum job permissions; that it is locked to the workspace of the job; and that it dies with its parent credential, its attempt and its job.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from tests.compute.fake_agent import JobPlan, wait_until

PW = "Correct-Horse-9-Battery"


def _hdr(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _mk_user(st: Any, roles: list[dict[str, str]], name: str | None = None) -> tuple[str, str]:
    name = name or "u" + uuid.uuid4().hex[:8]
    r = st.http.post("/v1/identity/users", headers=st.admin, json={"name": name, "password": PW, "roles": roles})
    assert r.status_code == 201, r.text
    return name, r.json()["id"]


def _login(st: Any, name: str) -> str:
    r = st.http.post("/v1/identity/login", json={"username": name, "password": PW})
    assert r.status_code == 200, r.text
    return r.json()["access_token"]


def _pat(st: Any, jwt_token: str, scopes: list[str], expect: int = 201) -> dict[str, Any]:
    r = st.http.post("/v1/identity/tokens", headers=_hdr(jwt_token), json={"name": "t-" + uuid.uuid4().hex[:5], "scopes": scopes})
    assert r.status_code == expect, r.text
    return r.json()


def _ws_role(role: str, ws: str) -> dict[str, str]:
    return {"role": role, "scope_type": "workspace", "scope_id": ws}


def _secret(st: Any, ws: str) -> str:
    r = st.http.post("/v1/secrets", headers=st.admin, json={"name": "s-" + uuid.uuid4().hex[:6], "value": "hunter2-hunter2", "scope_type": "workspace", "scope_id": ws})
    assert r.status_code == 201, r.text
    return r.json()["id"]


def _hang_agent(st: Any, name: str):
    a = st.agent(name)
    a.job_plan = lambda spec: JobPlan(outcome="hang")
    st.wait_worker(a)
    return a


def _running_job_token(st: Any, agent: Any, ws: str, headers: dict[str, str]) -> tuple[str, str]:
    job = st.submit(ws, headers)
    st.wait_job(job["id"], "running")
    spec = [s for s in agent.run_job_specs() if s["job_id"] == job["id"]][0]
    return job["id"], spec["callbacks"]["job_token"]


def _child(st: Any, ws: str, token: str, **over: Any):
    body = {"kind": "script", "workspace_id": ws, "engine": "python", "source": {"format": "py", "content": "print(1)"}, **over}
    return st.http.post("/v1/jobs", headers=_hdr(token), json=body)


def _principal(st: Any, token: str):
    from olcf.api.deps import authenticate

    return authenticate(st.ctx, token)


@pytest.fixture()
def env(fleet):
    st = fleet
    agent = _hang_agent(st, "cred-" + uuid.uuid4().hex[:6])
    return st, agent


def test_restricted_pat_cannot_use_secrets_or_write_through_a_job(env):
    st, agent = env
    ws = st.workspace()
    sid = _secret(st, ws)
    name, _ = _mk_user(st, [_ws_role("Contributor", ws)])
    pat = _pat(st, _login(st, name), ["mcp:write"])["token"]  # may submit jobs; may NOT use secrets or write data
    _job_id, tok = _running_job_token(st, agent, ws, _hdr(pat))
    p = _principal(st, tok)
    assert p.can("job.read", workspace_id=ws) and p.can("job.submit", workspace_id=ws)  # the ceiling still allows what the PAT allowed
    assert not p.can("secret.use", workspace_id=ws)
    assert not p.can("lakehouse.write", workspace_id=ws)
    assert not p.can("connection.use", workspace_id=ws)
    # the exploit of the review: a child job that references a secret hands the secret value to attacker-controlled code
    denied = _child(st, ws, tok, secret_refs={"LOOT": sid})
    assert denied.status_code == 403, denied.text
    assert _child(st, ws, tok).status_code == 202  # what the restricted PAT may do, the job may do
    # the PAT itself is refused too (baseline) - the derived credential is not *more* capable than its parent
    assert _child(st, ws, pat, secret_refs={"LOOT": sid}).status_code == 403


def test_unrestricted_contributor_keeps_the_documented_job_capabilities(env):
    st, agent = env
    ws = st.workspace()
    sid = _secret(st, ws)
    name, _ = _mk_user(st, [_ws_role("Contributor", ws)])
    jwt_token = _login(st, name)
    _job_id, tok = _running_job_token(st, agent, ws, _hdr(jwt_token))
    p = _principal(st, tok)
    for perm in ("job.read", "job.submit", "job.cancel", "workspace.read", "item.read", "lakehouse.read", "lakehouse.write", "secret.use", "connection.use", "sql.execute"):
        assert p.can(perm, workspace_id=ws), perm
    for perm in ("secret.manage", "workspace.update", "workspace.members.manage", "item.create", "identity.manage", "platform.admin", "admin.settings", "compute.manage"):
        assert not p.can(perm, workspace_id=ws), perm  # job scope never carried these
    # just-in-time secrets: a job may submit children that reference the secrets of its own workspace
    assert _child(st, ws, tok, secret_refs={"OK": sid}).status_code == 202


def test_a_viewer_cannot_gain_anything_through_a_job_it_could_not_submit(env):
    st, agent = env
    ws = st.workspace()
    contrib, uid = _mk_user(st, [_ws_role("Contributor", ws)])
    _job_id, tok = _running_job_token(st, agent, ws, _hdr(_login(st, contrib)))
    # the submitter is demoted to Viewer while the job runs: the credential follows the CURRENT bindings
    for b in st.http.get("/v1/identity/bindings", headers=st.admin, params={"principal_id": uid}).json()["items"]:
        assert st.http.delete(f"/v1/identity/bindings/{b['id']}", headers=st.admin).status_code == 204
    assert st.http.post("/v1/identity/bindings", headers=st.admin, json={"principal_id": uid, **_ws_role("Viewer", ws)}).status_code == 201
    st.ctx.authz.invalidate()
    p = _principal(st, tok)
    assert p.can("job.read", workspace_id=ws)
    assert not p.can("job.submit", workspace_id=ws) and not p.can("secret.use", workspace_id=ws) and not p.can("lakehouse.write", workspace_id=ws)


def test_job_credential_is_bound_to_the_workspace_of_the_job(env):
    st, agent = env
    ws_a, ws_b = st.workspace(), st.workspace()
    sid_b = _secret(st, ws_b)
    name, _ = _mk_user(st, [_ws_role("Contributor", ws_a), _ws_role("Contributor", ws_b)])
    jwt_token = _login(st, name)
    _job_id, tok = _running_job_token(st, agent, ws_a, _hdr(jwt_token))
    p = _principal(st, tok)
    assert p.can("job.submit", workspace_id=ws_a)
    for perm in ("job.submit", "job.read", "secret.use", "lakehouse.write", "workspace.read"):
        assert not p.can(perm, workspace_id=ws_b), perm  # the submitter may, the job's credential may not
    assert p.visible_workspaces("workspace.read") == {ws_a}
    assert _child(st, ws_b, tok).status_code in (403, 404)
    assert _child(st, ws_b, tok, secret_refs={"X": sid_b}).status_code in (403, 404)
    assert _child(st, ws_b, jwt_token).status_code == 202  # the user themselves still can


def test_org_level_contributor_job_credential_is_still_workspace_bound(env):
    st, agent = env
    ws, other = st.workspace(), st.workspace()
    name, _ = _mk_user(st, [{"role": "Contributor", "scope_type": "org", "scope_id": ""}])
    _job_id, tok = _running_job_token(st, agent, ws, _hdr(_login(st, name)))
    p = _principal(st, tok)
    assert p.can("job.submit", workspace_id=ws) and not p.can("job.submit", workspace_id=other)
    assert not p.can("secret.use") and not p.can("lakehouse.write")  # no workspace given: an org-wide question is not a job question


def test_derived_credential_dies_with_a_revoked_parent_pat(env):
    st, agent = env
    ws = st.workspace()
    name, _ = _mk_user(st, [_ws_role("Contributor", ws)])
    pat = _pat(st, _login(st, name), ["mcp:write"])
    _job_id, tok = _running_job_token(st, agent, ws, _hdr(pat["token"]))
    assert st.http.get("/v1/identity/me", headers=_hdr(tok)).status_code == 200
    assert st.http.delete(f"/v1/identity/tokens/{pat['id']}", headers=_hdr(_login(st, name))).status_code == 204
    assert st.http.get("/v1/identity/me", headers=_hdr(tok)).status_code == 401  # the job is still running, its credential is not
    assert _child(st, ws, tok).status_code == 401


def test_derived_credential_dies_when_the_login_session_is_revoked(env):
    st, agent = env
    ws = st.workspace()
    name, _ = _mk_user(st, [_ws_role("Contributor", ws)])
    login = st.http.post("/v1/identity/login", json={"username": name, "password": PW}).json()
    _job_id, tok = _running_job_token(st, agent, ws, _hdr(login["access_token"]))
    assert st.http.get("/v1/identity/me", headers=_hdr(tok)).status_code == 200
    out = st.http.post("/v1/identity/logout", headers=_hdr(login["access_token"]), json={"refresh_token": login.get("refresh_token")})
    assert out.status_code in (200, 204), out.text
    assert st.http.get("/v1/identity/me", headers=_hdr(tok)).status_code == 401


def test_derived_credential_is_refused_for_a_finished_job_and_for_a_stale_attempt(env):
    st, agent = env
    ws = st.workspace()
    name, _ = _mk_user(st, [_ws_role("Contributor", ws)])
    job_id, tok = _running_job_token(st, agent, ws, _hdr(_login(st, name)))
    assert st.http.get(f"/v1/jobs/{job_id}", headers=_hdr(tok)).status_code == 200
    agent.finish_hung(job_id, "succeed")
    st.wait_job(job_id)
    wait_until(lambda: st.http.get(f"/v1/jobs/{job_id}", headers=_hdr(tok)).status_code == 401, 10, message="the job credential to end with the job")


def test_session_kernel_credential_cannot_exceed_the_creating_credential(env, monkeypatch):
    from olcf.security import rbac

    monkeypatch.setitem(rbac.TOKEN_SCOPES, "sess-limited", {"session.use", "job.submit", "job.read", "workspace.read", "item.read", "lakehouse.read"})  # kernels yes, secrets / writes no
    st, agent = env
    ws, other = st.workspace(), st.workspace()
    sid = _secret(st, ws)
    name, uid = _mk_user(st, [_ws_role("Contributor", ws), _ws_role("Contributor", other)])
    pat, _rec = st.ctx.tokens.create_pat(uid, "limited-kernel", scopes=["sess-limited"])
    sess = st.http.post("/v1/sessions", headers=_hdr(pat), json={"workspace_id": ws, "engine": "python"})
    assert sess.status_code == 201, sess.text
    session_id = sess.json()["id"]
    wait_until(lambda: st.http.get(f"/v1/sessions/{session_id}", headers=st.admin).json()["state"] == "idle", 20, 0.1, "kernel")
    tok = agent.kernels[session_id]["spec"]["callbacks"]["job_token"]
    p = _principal(st, tok)
    assert p.can("job.submit", workspace_id=ws) and p.can("lakehouse.read", workspace_id=ws)
    assert not p.can("secret.use", workspace_id=ws) and not p.can("lakehouse.write", workspace_id=ws) and not p.can("connection.use", workspace_id=ws)
    assert not p.can("job.submit", workspace_id=other)
    assert _child(st, ws, tok, secret_refs={"X": sid}).status_code == 403
    # revoking the PAT that opened the kernel ends the kernel's credential
    assert st.http.delete(f"/v1/identity/tokens/{_rec['id']}", headers=st.admin).status_code == 204
    assert st.http.get("/v1/identity/me", headers=_hdr(tok)).status_code == 401


def test_session_kernel_credential_of_an_unrestricted_user_is_job_scoped_and_workspace_bound(env):
    st, agent = env
    ws, other = st.workspace(), st.workspace()
    name, _ = _mk_user(st, [_ws_role("Contributor", ws), _ws_role("Contributor", other)])
    jwt_token = _login(st, name)
    sess = st.http.post("/v1/sessions", headers=_hdr(jwt_token), json={"workspace_id": ws, "engine": "python"})
    assert sess.status_code == 201, sess.text
    session_id = sess.json()["id"]
    wait_until(lambda: st.http.get(f"/v1/sessions/{session_id}", headers=st.admin).json()["state"] == "idle", 20, 0.1, "kernel")
    tok = agent.kernels[session_id]["spec"]["callbacks"]["job_token"]
    p = _principal(st, tok)
    assert p.can("secret.use", workspace_id=ws) and p.can("lakehouse.write", workspace_id=ws)
    assert not p.can("job.submit", workspace_id=other) and not p.can("secret.manage", workspace_id=ws)
    # killing the session ends the credential
    assert st.http.delete(f"/v1/sessions/{session_id}", headers=_hdr(jwt_token)).status_code in (200, 202, 204)
    wait_until(lambda: st.http.get("/v1/identity/me", headers=_hdr(tok)).status_code == 401, 10, message="the kernel credential to end with the session")


def test_a_restricted_token_cannot_mint_a_broader_token(env):
    st, _agent = env
    ws = st.workspace()
    name, _ = _mk_user(st, [_ws_role("Contributor", ws)])
    jwt_token = _login(st, name)
    pat = _pat(st, jwt_token, ["mcp:write"])["token"]
    for scopes in (["write"], ["job"], ["*"], ["mcp:write", "write"]):
        r = st.http.post("/v1/identity/tokens", headers=_hdr(pat), json={"name": "esc", "scopes": scopes})
        assert r.status_code == 403, (scopes, r.text)
    assert st.http.post("/v1/identity/tokens", headers=_hdr(pat), json={"name": "narrower", "scopes": ["mcp"]}).status_code == 201
    assert st.http.post("/v1/identity/tokens", headers=_hdr(jwt_token), json={"name": "ok", "scopes": ["write"]}).status_code == 201


# ------------------------------------------------------------------------------------------------------------------------- the token of a launch that never happened
def test_a_token_minted_for_a_failed_launch_is_recorded_and_unusable_at_once(env, monkeypatch):
    """``job_attempts.token_id`` used to be written only after ``provider.submit`` returned, so a launch whose submit raised left a live credential (TTL: job timeout + 1 h) that
    no attempt row pointed at: nothing could list it for revocation.  The id is recorded before the spec is built and the token is revoked as soon as the launch fails."""
    from sqlalchemy import select

    from olcf.compute import schema as C
    from olcf.compute.errors import ProviderError
    from olcf.compute.providers.agent import AgentExecutedProvider
    from olcf.db import schema as S

    st, _agent = env
    ws = st.workspace()
    seen: list[str] = []

    def failing_submit(self, req):  # noqa: ANN001, ANN202
        seen.append(req.spec["callbacks"]["job_token"])
        # at this very moment the token is already listed on the attempt row ...
        with st.ctx.db.connect() as conn:
            rec = conn.execute(select(C.job_attempts.c.token_id).where(C.job_attempts.c.job_id == req.job["id"], C.job_attempts.c.attempt == req.job["attempt"])).scalar_one()
        assert rec, "the token id must be on the attempt row before provider.submit is called"
        raise ProviderError("the provider refused the job")

    monkeypatch.setattr(AgentExecutedProvider, "submit", failing_submit)
    job = st.submit(ws)
    done = st.wait_job(job["id"], ("failed", "cancelled", "timed_out", "succeeded"), timeout=60)
    assert done["state"] == "failed" and seen, done
    assert "could not start the job" in str(done.get("failure"))
    with st.ctx.db.connect() as conn:
        attempts = conn.execute(select(C.job_attempts.c.attempt, C.job_attempts.c.token_id).where(C.job_attempts.c.job_id == job["id"])).all()
        revoked = {r[0]: r[1] for r in conn.execute(select(S.api_tokens.c.id, S.api_tokens.c.revoked_at))}
    assert attempts and all(tid for _a, tid in attempts), attempts
    assert all(revoked.get(tid) for _a, tid in attempts), "every token minted for a failed launch is revoked"
    for tok in seen:  # ... and the plaintext no longer authenticates
        assert st.http.get(f"/v1/jobs/{job['id']}", headers=_hdr(tok)).status_code == 401


def test_a_requeued_launch_revokes_the_token_of_the_abandoned_attempt_and_the_next_attempt_gets_a_working_one(env, monkeypatch):
    """``WorkerOffline`` at hand-off re-queues the job without ending the attempt (nothing was started), which used to leave that attempt's credential alive as well."""
    from olcf.compute.errors import WorkerOffline
    from olcf.compute.providers.agent import AgentExecutedProvider

    st, agent = env
    ws = st.workspace()
    real = AgentExecutedProvider.submit
    tokens: list[str] = []

    def flaky(self, req):  # noqa: ANN001, ANN202
        if req.job["workspace_id"] != ws:  # the control plane is shared by the whole module: queued work of earlier tests must neither be failed nor counted here
            return real(self, req)
        tokens.append(req.spec["callbacks"]["job_token"])
        if len(tokens) == 1:
            raise WorkerOffline("the driver worker dropped its connection")
        return real(self, req)

    monkeypatch.setattr(AgentExecutedProvider, "submit", flaky)
    job = st.submit(ws)
    st.wait_job(job["id"], "running", timeout=60)
    assert len(tokens) == 2 and tokens[0] != tokens[1]
    assert st.http.get(f"/v1/jobs/{job['id']}", headers=_hdr(tokens[0])).status_code == 401, "the abandoned attempt's credential is dead"
    assert st.http.get(f"/v1/jobs/{job['id']}", headers=_hdr(tokens[1])).status_code == 200, "the attempt that really started keeps a working credential"
    agent.finish_hung(job["id"], "succeed")
