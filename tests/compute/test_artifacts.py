# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Job artifacts: inline delivery in the terminal ``job_state``, paged ``fetch_artifact`` for large files, validation, and the HTTP upload endpoint."""

from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path
from typing import Any

import pytest

from olcf.compute import artifacts as A
from olcf.core.errors import BadRequest
from tests.compute.fake_agent import JobPlan, wait_until

JOB = "job_01HZZ9ABCDEFGHJKMNPQRSTVWX"
NB = json.dumps({"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}).encode()


def b64(b: bytes) -> str:
    return base64.b64encode(b).decode()


# ------------------------------------------------------------------------------------------------ pure
def test_ingest_inline_stores_verifies_and_strips_payloads(tmp_path: Path) -> None:
    data = b"hello artifact"
    sha = hashlib.sha256(data).hexdigest()
    res, pending = A.ingest_inline(tmp_path, JOB, {"rows": 1, "artifacts": [
        {"name": "ok.txt", "path": "ok.txt", "size": len(data), "sha256": sha, "data_b64": b64(data)},
        {"name": "bad-sha.txt", "path": "bad-sha.txt", "size": len(data), "sha256": "0" * 64, "data_b64": b64(data)},
        {"name": "bad-size.txt", "path": "bad-size.txt", "size": 3, "data_b64": b64(data)},
        {"name": "bad-b64.txt", "path": "bad-b64.txt", "data_b64": "!!!not base64!!!"},
        {"name": "../escape", "path": "../../escape", "data_b64": b64(data)},
        {"name": "..", "path": "", "data_b64": b64(data)},
        {"name": "big.bin", "path": "sub/big.bin", "size": 99, "truncated": True},
        {"path": "dir/derived-name.txt", "data_b64": b64(data)},
        "not-a-dict",
    ]})
    by = {a.get("name") or a.get("path"): a for a in res["artifacts"]}
    assert res["rows"] == 1 and "data_b64" not in json.dumps(res)
    assert by["ok.txt"]["stored"] is True and (tmp_path / JOB / "artifacts" / "ok.txt").read_bytes() == data
    assert by["bad-sha.txt"]["stored"] is False and "sha256" in by["bad-sha.txt"]["error"] and not (tmp_path / JOB / "artifacts" / "bad-sha.txt").exists()
    assert "size mismatch" in by["bad-size.txt"]["error"] and "error" in by["bad-b64.txt"]
    assert by[".."]["error"] == "invalid artifact name" and by[".."].get("stored") is None
    assert by["escape"]["stored"] is True and (tmp_path / JOB / "artifacts" / "escape").exists()  # a bad name falls back to the last path component, still confined
    assert by["derived-name.txt"]["stored"] is True  # a bad/missing name falls back to the file name of the path
    assert [p["name"] for p in pending] == ["big.bin"] and pending[0]["pending"] is True
    assert [p for p in tmp_path.rglob("escape")] == [tmp_path / JOB / "artifacts" / "escape"]  # nothing was written outside the artifact directory
    assert not list((tmp_path / JOB / "artifacts").glob(".ingest-*"))  # no temp files left behind
    assert A.ingest_inline(tmp_path, JOB, None) == ({}, []) and A.ingest_inline(tmp_path, JOB, {"artifacts": "x"})[1] == []


def test_artifact_paths_are_confined(tmp_path: Path) -> None:
    for bad in ("../x", "a/b", ".hidden", "", "x" * 129, "a\\b", "a\x00b"):
        with pytest.raises(BadRequest):
            A.artifact_path(tmp_path, JOB, bad)
    with pytest.raises(BadRequest):
        A.artifact_dir(tmp_path, "../other")
    assert A.artifact_path(tmp_path, JOB, "output.ipynb").parent == (tmp_path / JOB / "artifacts").resolve()


def test_fetch_remote_pages_verifies_and_cleans_up(tmp_path: Path) -> None:
    data = os.urandom(10_000)
    sha = hashlib.sha256(data).hexdigest()
    calls: list[dict[str, Any]] = []

    def agent(cmd: str, p: dict[str, Any]) -> dict[str, Any]:
        calls.append(p)
        chunk = data[p["offset"]:p["offset"] + 4096]
        return {"offset": p["offset"], "data_b64": b64(chunk), "eof": p["offset"] + len(chunk) >= len(data)}

    A.FETCH_CHUNK = 4096  # exercise paging with a tiny page
    try:
        info = A.fetch_remote(tmp_path, JOB, {"name": "o.bin", "path": "sub/o.bin", "size": len(data), "sha256": sha}, agent, cap=1 << 20)
    finally:
        A.FETCH_CHUNK = 4 * 1024 * 1024
    assert info["bytes"] == len(data) and [c["offset"] for c in calls] == [0, 4096, 8192] and calls[0]["path"] == "sub/o.bin"
    assert (tmp_path / JOB / "artifacts" / "o.bin").read_bytes() == data
    for entry, msg in (({"name": "x.bin", "size": len(data), "sha256": "0" * 64}, "sha256"), ({"name": "y.bin", "size": 5}, "size"), ({"name": "z.bin", "size": 10**9}, "limit")):
        with pytest.raises(ValueError, match=msg):
            A.fetch_remote(tmp_path, JOB, entry, agent, cap=1 << 20)
        assert not (tmp_path / JOB / "artifacts" / entry["name"]).exists()
    assert not list((tmp_path / JOB / "artifacts").glob(".fetch-*"))


# ------------------------------------------------------------------------------------------------ through the control plane
def run(st: Any, plan: JobPlan, **submit: Any) -> tuple[Any, dict[str, Any]]:
    a = st.agent()
    a.job_plan = lambda spec: plan
    st.wait_worker(a)
    ws = st.workspace()
    done = st.wait_job(st.submit(ws, **submit)["id"], timeout=60)
    return a, done


def test_inline_notebook_output_is_available_when_the_job_finishes(fleet: Any) -> None:
    st = fleet
    a, done = run(st, JobPlan(artifacts={"output.ipynb": NB, "metrics.json": b'{"rows": 3}'}))
    assert done["state"] == "succeeded"
    r = st.http.get(f"/v1/jobs/{done['id']}/output", headers=st.admin)
    assert r.status_code == 200 and r.content == NB and r.headers["X-OLCF-Next-Offset"] == ""
    names = {x["name"]: x for x in st.http.get(f"/v1/jobs/{done['id']}/artifacts", headers=st.admin).json()["items"]}
    assert set(names) == {"output.ipynb", "metrics.json"} and names["metrics.json"]["size"] == 11
    assert st.http.get(f"/v1/jobs/{done['id']}/artifacts/metrics.json", headers=st.admin).content == b'{"rows": 3}'
    # paging by byte offset
    p1 = st.http.get(f"/v1/jobs/{done['id']}/output", headers=st.admin, params={"limit": 10})
    assert p1.content == NB[:10] and p1.headers["X-OLCF-Next-Offset"] == "10"
    # the database keeps metadata only - never the base64 payload
    detail = st.job(done["id"])
    assert "data_b64" not in json.dumps(detail) and detail["result"]["artifacts"][0]["stored"] is True
    assert not a.fetch_calls, "small artifacts must not trigger fetch_artifact"


def test_large_artifacts_are_pulled_with_fetch_artifact(fleet: Any) -> None:
    st = fleet
    big = os.urandom(9 * 1024 * 1024 + 123)
    a, done = run(st, JobPlan(artifacts={"output.ipynb": NB, "big.parquet": big}, inline_limit=1024))
    assert done["state"] == "succeeded"
    got = wait_until(lambda: (r := st.http.get(f"/v1/jobs/{done['id']}/artifacts/big.parquet", headers=st.admin)).status_code == 200 and r, 60, 0.2, "the large artifact to be fetched")
    assert hashlib.sha256(got.content).hexdigest() == hashlib.sha256(big).hexdigest()
    assert [c["offset"] for c in a.fetch_calls if c["path"] == "big.parquet"] == [0, 4 * 1024 * 1024, 8 * 1024 * 1024]
    ev = [e for e in st.job(done["id"])["timeline"] if e.get("event") == "artifact_fetched" or e.get("kind") == "event"]
    assert any("artifact_fetched" in json.dumps(e) for e in ev)
    assert st.http.get(f"/v1/jobs/{done['id']}/output", headers=st.admin).content == NB


def test_damaged_artifacts_never_fail_the_job(fleet: Any) -> None:
    st = fleet
    a, done = run(st, JobPlan(artifacts={"good.txt": b"fine", "bad.txt": b"tampered", "large-bad.bin": b"x" * 5000}, bad_sha={"bad.txt", "large-bad.bin"}, inline_limit=1000))
    assert done["state"] == "succeeded"
    wait_until(lambda: "artifact_failed" in json.dumps(st.job(done["id"])["timeline"]), 20, 0.2, "artifact_failed event")
    listed = {x["name"] for x in st.http.get(f"/v1/jobs/{done['id']}/artifacts", headers=st.admin).json()["items"]}
    assert listed == {"good.txt"}
    assert st.http.get(f"/v1/jobs/{done['id']}/artifacts/bad.txt", headers=st.admin).status_code == 404


# ------------------------------------------------------------------------------------------------ HTTP upload endpoint
def test_upload_endpoint_authorisation(fleet: Any) -> None:
    st = fleet
    ws = st.workspace()
    first, second = st.agent(), st.agent()
    for x in (first, second):
        x.job_plan = lambda spec: JobPlan(outcome="hang")
        st.wait_worker(x)
    j = st.submit(ws)
    running = st.wait_job(j["id"], "running")
    a, other = (first, second) if running["driver_worker_id"] == first.worker_id else (second, first)
    url = f"/v1/jobs/{j['id']}/artifacts/output.ipynb"
    drv = {"Authorization": f"Bearer {a.token}"}
    assert st.http.post(url, headers=drv, content=NB).status_code == 201
    assert st.http.get(url, headers=st.admin).content == NB
    assert st.http.post(url, headers={"Authorization": f"Bearer {other.token}"}, content=b"x").status_code == 403  # not the driver node
    assert st.http.post(url, headers=st.admin, content=b"x").status_code == 403  # not even an administrator: only the executing side uploads
    assert st.http.post(url).status_code == 401
    assert st.http.post(f"/v1/jobs/{j['id']}/artifacts/..%2Fevil", headers=drv, content=b"x").status_code in (400, 404, 405)
    assert st.http.post(f"/v1/jobs/{j['id']}/artifacts/.hidden", headers=drv, content=b"x").status_code == 400
    # the job-scoped token the runner holds (CONTRACTS 4.3 callbacks.job_token) works too, and only for this job
    job_token = a.run_job_specs()[0]["callbacks"]["job_token"]
    assert st.http.post(f"/v1/jobs/{j['id']}/artifacts/extra.txt", headers={"Authorization": f"Bearer {job_token}"}, content=b"more").status_code == 201
    j2 = st.submit(ws)
    st.wait_job(j2["id"], "running")
    assert st.http.post(f"/v1/jobs/{j2['id']}/artifacts/extra.txt", headers={"Authorization": f"Bearer {job_token}"}, content=b"more").status_code == 403
    for jid in (j["id"], j2["id"]):
        st.http.post(f"/v1/jobs/{jid}/cancel", headers=st.admin)
        st.wait_job(jid)
