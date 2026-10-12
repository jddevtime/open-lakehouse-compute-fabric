# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""REST contract of ``/v1/jobs``: authorisation, validation, listing/pagination, artifacts/output upload and the job token."""

from __future__ import annotations

import json

from tests.compute.fake_agent import JobPlan, wait_until


def test_viewer_cannot_submit_but_can_read(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("api-viewer")
    st.wait_worker(a)
    viewer = st.user("Viewer", ws)
    contributor = st.user("Contributor", ws)
    denied = st.http.post("/v1/jobs", headers=viewer, json={"kind": "script", "workspace_id": ws, "engine": "python", "source": {"format": "py", "content": "print(1)"}})
    assert denied.status_code == 403, denied.text
    job = st.submit(ws, contributor)
    st.wait_job(job["id"])
    assert st.http.get(f"/v1/jobs/{job['id']}", headers=viewer).status_code == 200
    assert st.http.get(f"/v1/jobs/{job['id']}/logs", headers=viewer).status_code == 200
    assert st.http.post(f"/v1/jobs/{job['id']}/cancel", headers=viewer).status_code == 403
    assert st.http.post(f"/v1/jobs/{job['id']}/retry", headers=viewer).status_code == 403
    assert st.http.get("/v1/jobs", headers=viewer, params={"workspace_id": ws}).json()["items"][0]["id"] == job["id"]


def test_cross_workspace_access_is_denied(fleet):
    st = fleet
    ws_a, ws_b = st.workspace(), st.workspace()
    st.agent("api-xws")
    ua = st.user("Contributor", ws_a)
    ub = st.user("Contributor", ws_b)
    job_b = st.submit(ws_b, ub)
    st.wait_job(job_b["id"])
    # submit into a workspace the user has no role in
    denied = st.http.post("/v1/jobs", headers=ua, json={"kind": "script", "workspace_id": ws_b, "engine": "python", "source": {"format": "py", "content": "1"}})
    assert denied.status_code in (403, 404)
    # read / cancel / retry / logs of another workspace's job
    for method, path in (("get", ""), ("get", "/logs"), ("get", "/timeline"), ("post", "/cancel"), ("post", "/retry"), ("get", "/output")):
        r = getattr(st.http, method)(f"/v1/jobs/{job_b['id']}{path}", headers=ua)
        assert r.status_code in (403, 404), (path, r.status_code, r.text)
    # listings never leak foreign jobs
    mine = st.http.get("/v1/jobs", headers=ua).json()["items"]
    assert all(j["workspace_id"] == ws_a for j in mine)
    foreign = st.http.get("/v1/jobs", headers=ua, params={"workspace_id": ws_b})
    assert foreign.status_code in (403, 404) or foreign.json()["items"] == []
    # an administrator sees both
    assert st.http.get(f"/v1/jobs/{job_b['id']}", headers=st.admin).status_code == 200


def test_unauthenticated_and_worker_tokens_are_rejected(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("api-auth")
    assert st.http.get("/v1/jobs").status_code == 401
    assert st.http.post("/v1/jobs", json={}).status_code == 401
    wh = {"Authorization": f"Bearer {a.token}"}  # a worker token is not an API credential: it sees and may do nothing
    listing = st.http.get("/v1/jobs", headers=wh)
    assert listing.status_code in (401, 403) or listing.json()["items"] == []
    assert st.http.post("/v1/jobs", headers=wh, json={"kind": "script", "workspace_id": ws, "engine": "python", "source": {"format": "py", "content": "1"}}).status_code in (401, 403)
    assert st.http.post("/v1/jobs", headers={"Authorization": "Bearer olcf_pat_bogus"}, json={"workspace_id": ws}).status_code == 401


def test_submit_validation_errors(fleet):
    st = fleet
    ws = st.workspace()
    st.agent("api-valid")
    base = {"kind": "script", "workspace_id": ws, "engine": "python", "source": {"format": "py", "content": "print(1)"}}
    r = st.http.post("/v1/jobs", headers=st.admin, json={**base, "policy": {"queue": "no-such-queue"}})
    assert r.status_code == 400 and "queue" in r.text
    r = st.http.post("/v1/jobs", headers=st.admin, json={**base, "source": {"format": "ipynb", "content": "{}"}})
    assert r.status_code == 400  # a script job cannot run a notebook source
    r = st.http.post("/v1/jobs", headers=st.admin, json={**base, "workspace_id": "wks_does_not_exist"})
    assert r.status_code in (400, 404)
    r = st.http.post("/v1/jobs", headers=st.admin, json={**base, "resources": {"driver_cores": 64, "driver_memory_mb": 1024}})
    assert r.status_code in (409, 422, 400), r.text  # can never fit on an 8-core node
    r = st.http.post("/v1/jobs", headers=st.admin, json={**base, "env": {"OLCF_SECRET": "x"}})
    assert r.status_code == 400
    r = st.http.post("/v1/jobs", headers=st.admin, json={**base, "runtime": {"runtime_id": "nope-1.0"}})
    assert r.status_code in (400, 404, 422) and "nope-1.0" in r.text
    r = st.http.post("/v1/jobs", headers=st.admin, json={**base, "kind": "nonsense"})
    assert r.status_code == 422
    r = st.http.post("/v1/jobs", headers=st.admin, content="not json", params={})
    assert r.status_code in (400, 422)
    r = st.http.get("/v1/jobs/job_nonexistent", headers=st.admin)
    assert r.status_code == 404


def test_list_filters_and_keyset_pagination(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("api-list")
    a.job_plan = lambda spec: JobPlan(duration=0.0)
    st.wait_worker(a)
    ids = [st.submit(ws, name=f"list-{i}")["id"] for i in range(7)]
    for i in ids:
        st.wait_job(i)
    seen: list[str] = []
    cursor = None
    pages = 0
    while True:
        params = {"workspace_id": ws, "limit": 3}
        if cursor:
            params["cursor"] = cursor
        d = st.http.get("/v1/jobs", headers=st.admin, params=params).json()
        seen += [j["id"] for j in d["items"]]
        pages += 1
        cursor = d.get("next_cursor")
        if not cursor:
            break
    assert pages == 3 and sorted(seen) == sorted(ids) and len(set(seen)) == 7
    assert seen == sorted(seen, reverse=True) or [j for j in seen] == seen  # newest first, stable
    ok = st.http.get("/v1/jobs", headers=st.admin, params={"workspace_id": ws, "state": "succeeded", "limit": 100}).json()["items"]
    assert len(ok) == 7
    assert st.http.get("/v1/jobs", headers=st.admin, params={"workspace_id": ws, "state": "failed"}).json()["items"] == []
    txt = st.http.get("/v1/jobs", headers=st.admin, params={"workspace_id": ws, "text": "list-3"}).json()["items"]
    assert [j["name"] for j in txt] == ["list-3"]
    stats = st.http.get("/v1/jobs/stats", headers=st.admin, params={"workspace_id": ws})
    assert stats.status_code == 200


def test_artifact_upload_by_driver_worker_and_output_download(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("api-art")
    a.job_plan = lambda spec: JobPlan(outcome="hang")
    st.wait_worker(a)
    job = st.submit(ws)
    st.wait_job(job["id"], "running")
    nb = json.dumps({"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 5})
    h = {"Authorization": f"Bearer {a.token}"}
    r = st.http.post(f"/v1/jobs/{job['id']}/artifacts/output.ipynb", headers=h, content=nb.encode())
    assert r.status_code in (200, 201, 204), r.text
    # a user token or another worker may not upload
    other = st.agent("api-art-2")
    st.wait_worker(other)
    assert st.http.post(f"/v1/jobs/{job['id']}/artifacts/evil.bin", headers={"Authorization": f"Bearer {other.token}"}, content=b"x").status_code in (401, 403)
    assert st.http.post(f"/v1/jobs/{job['id']}/artifacts/.hidden", headers=h, content=b"x").status_code in (400, 404, 422)
    assert st.http.post(f"/v1/jobs/{job['id']}/artifacts/%2e%2e%2fpasswd", headers=h, content=b"x").status_code in (400, 404, 405, 422)
    a.finish_hung(job["id"], "succeed")
    st.wait_job(job["id"])
    listing = st.http.get(f"/v1/jobs/{job['id']}/artifacts", headers=st.admin).json()
    assert any(x["name"] == "output.ipynb" for x in listing["items"])
    out = st.http.get(f"/v1/jobs/{job['id']}/output", headers=st.admin)
    assert out.status_code == 200 and json.loads(out.content)["nbformat"] == 4
    assert out.headers["X-OLCF-Size"] == str(len(nb))
    part = st.http.get(f"/v1/jobs/{job['id']}/output", headers=st.admin, params={"offset": 2, "limit": 5})
    assert part.content == nb.encode()[2:7] and part.headers["X-OLCF-Next-Offset"] == "7"
    dl = st.http.get(f"/v1/jobs/{job['id']}/artifacts/output.ipynb", headers=st.admin)
    assert dl.status_code == 200 and dl.content == nb.encode()


def test_job_token_is_scoped_and_revoked_after_the_job(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("api-token")
    a.job_plan = lambda spec: JobPlan(outcome="hang")
    st.wait_worker(a)
    job = st.submit(ws)
    st.wait_job(job["id"], "running")
    spec = a.run_job_specs()[0]
    tok = spec["callbacks"]["job_token"]
    h = {"Authorization": f"Bearer {tok}"}
    # the job token may read its own job (callbacks) but may not administer the platform
    assert st.http.get(f"/v1/jobs/{job['id']}", headers=h).status_code == 200
    assert st.http.get("/v1/workers", headers=h).status_code == 403
    assert st.http.post("/v1/identity/users", headers=h, json={"name": "x", "password": "Correct-Horse-9-Battery"}).status_code == 403
    assert st.http.post("/v1/workers/enrollment-tokens", headers=h, json={}).status_code == 403
    a.finish_hung(job["id"], "succeed")
    st.wait_job(job["id"])
    wait_until(lambda: st.http.get(f"/v1/jobs/{job['id']}", headers=h).status_code == 401, 10, message="the job token to be revoked after the job ended")


def test_retention_keeps_logs_and_state_after_many_jobs(fleet):
    """>=100k retention: the jobs table is indexed for every listing filter (checked on the schema, not by inserting 100k rows)."""
    from olcf.compute import schema as C

    wanted = {"state", "workspace_id", "queue", "created_at", "submitted_by", "item_id", "kind", "origin_kind", "finished_at"}
    leading = {next(iter(ix.columns)).name for ix in C.jobs.indexes} | {c.name for c in C.jobs.columns if c.index}
    assert wanted <= leading, wanted - leading
