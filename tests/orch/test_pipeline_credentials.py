# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Review F01, pipeline side: a job started by a pipeline run (also by a *nested* child pipeline) keeps the permission ceiling of the credential that started the run, and
a run started with a derived (job / kernel) credential keeps that credential's workspace binding and dies with its origin."""

from __future__ import annotations

import uuid

import pytest

from tests.compute.fake_agent import JobPlan, wait_until
from tests.orch.orch_support import OrchStack
from tests.orch.test_api_e2e import A

PW = "Correct-Horse-9-Battery"
LIMITED = "pipe-limited"
LIMITED_PERMS = {"pipeline.run", "job.submit", "job.read", "job.cancel", "workspace.read", "item.read", "lakehouse.read"}  # runs pipelines and jobs; no secrets, no writes


def _user(st: OrchStack, ws: str) -> tuple[str, dict[str, str]]:
    name = "u" + uuid.uuid4().hex[:8]
    r = st.http.post("/v1/identity/users", headers=st.admin, json={"name": name, "password": PW, "roles": [{"role": "Contributor", "scope_type": "workspace", "scope_id": ws}]})
    assert r.status_code == 201, r.text
    login = st.http.post("/v1/identity/login", json={"username": name, "password": PW}).json()
    return r.json()["id"], {"Authorization": f"Bearer {login['access_token']}"}


def _principal(st: OrchStack, token: str):
    from olcf.api.deps import authenticate

    return authenticate(st.ctx, token)


@pytest.fixture()
def limited_scope(monkeypatch):
    from olcf.security import rbac

    monkeypatch.setitem(rbac.TOKEN_SCOPES, LIMITED, set(LIMITED_PERMS))


def test_nested_pipeline_job_credential_keeps_the_ceiling_of_the_starting_token(fleet: OrchStack, limited_scope) -> None:
    st = fleet
    agent = st.agents[0]
    agent.job_plan = lambda spec: JobPlan(outcome="hang")
    ws = st.workspace()
    uid, _jwt = _user(st, ws)
    pat, _rec = st.ctx.tokens.create_pat(uid, "limited", scopes=[LIMITED])
    child = st.pipeline(ws, "child", [A("S", "script", code="print('hi')")])
    parent = st.pipeline(ws, "parent", [A("C", "child_pipeline", pipeline=child["id"], waitOnCompletion=False)])
    run = st.start_run(parent["id"], headers={"Authorization": f"Bearer {pat}"})
    wait_until(lambda: any(s.get("workspace", {}).get("id") == ws for s in agent.run_job_specs()), 30, 0.1, "the nested pipeline's job to reach the agent")
    spec = [s for s in agent.run_job_specs() if s["workspace"]["id"] == ws][0]
    p = _principal(st, spec["callbacks"]["job_token"])
    assert p.can("job.submit", workspace_id=ws) and p.can("job.read", workspace_id=ws)
    assert not p.can("secret.use", workspace_id=ws) and not p.can("lakehouse.write", workspace_id=ws) and not p.can("connection.use", workspace_id=ws)
    assert run["id"]


def test_pipeline_job_of_an_unrestricted_contributor_keeps_the_documented_job_capabilities(fleet: OrchStack) -> None:
    st = fleet
    agent = st.agents[0]
    agent.job_plan = lambda spec: JobPlan(outcome="hang")
    ws = st.workspace()
    _uid, jwt_headers = _user(st, ws)
    pl = st.pipeline(ws, "plain", [A("S", "script", code="print('hi')")])
    st.start_run(pl["id"], headers=jwt_headers)
    wait_until(lambda: any(s["workspace"]["id"] == ws for s in agent.run_job_specs()), 30, 0.1, "the job to reach the agent")
    spec = [s for s in agent.run_job_specs() if s["workspace"]["id"] == ws][0]
    p = _principal(st, spec["callbacks"]["job_token"])
    assert p.can("secret.use", workspace_id=ws) and p.can("lakehouse.write", workspace_id=ws) and p.can("job.submit", workspace_id=ws)
    assert not p.can("job.submit", workspace_id=st.workspace())


def test_a_run_started_by_a_job_credential_stays_bound_and_dies_with_its_origin(fleet: OrchStack) -> None:
    from olcf.pipelines.activities.base import ActivityError

    st = fleet
    agent = st.agents[0]
    agent.job_plan = lambda spec: JobPlan(outcome="hang")
    ws, other = st.workspace(), st.workspace()
    uid, _ = _user(st, ws)
    pat, rec = st.ctx.tokens.create_pat(uid, "full", scopes=["*"])
    job = st.submit(ws, {"Authorization": f"Bearer {pat}"})
    st.wait_job(job["id"], "running")
    token = [s for s in agent.run_job_specs() if s["job_id"] == job["id"]][0]["callbacks"]["job_token"]
    derived = _principal(st, token)
    assert derived.workspace_lock == ws
    item = st.pipeline(ws, "tgt", [A("W", "wait", waitTimeInSeconds=0)])
    engine = st.ctx.service("pipelines").engine
    items = st.ctx.service("items")
    run = engine.create_run(derived, items.get(item["id"]), {}, {"type": "manual"})
    p = engine.principal_for(engine.run_row(run["id"]))
    assert p.workspace_lock == ws and not p.can("job.submit", workspace_id=other) and p.can("job.submit", workspace_id=ws)
    # the ceiling document is internal: the run's public trigger does not expose it
    assert "principal_credential" not in st.run(run["id"])["trigger"]
    # revoking the PAT that started the job ends the run's principal too
    assert st.http.delete(f"/v1/identity/tokens/{rec['id']}", headers=st.admin).status_code == 204
    engine._principals.clear()  # noqa: SLF001 - the 5 s principal cache
    with pytest.raises(ActivityError):
        engine.principal_for(engine.run_row(run["id"]))
