# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""Seams between the notebooks (ITEMS) and compute (CTRL) packages, against a live control plane with fake worker agents: the pip policy reaches the node,
environment variables and layers reach jobs *and* sessions, an explicit dependency lock is validated, env jars are served to nodes."""

from __future__ import annotations

import copy
from typing import Any

from tests.items.items_support import nb_json
from tests.items.wheelbuilder import build_wheel


def item(st: Any, ws: str, type_: str, name: str, definition: dict[str, Any]) -> dict[str, Any]:
    r = st.http.post(f"/v1/workspaces/{ws}/items", headers=st.admin, json={"type": type_, "name": name, "definition": definition})
    assert r.status_code == 201, r.text
    return r.json()


def published_env(st: Any, ws: str, tmp_path: Any, name: str = "libs", **extra: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    d = tmp_path / f"wh-{name}"
    build_wheel(d, "olcf-seam-lib", "1.0")
    assert st.http.put("/v1/admin/settings/environments.wheelhouses", headers=st.admin, json={"value": [str(d)]}).status_code == 200
    env = item(st, ws, "environment", name, {"python": {"requirements": ["olcf-seam-lib"], "index": {"no_index": True, "find_links": [str(d)]}}, **extra})
    pub = st.http.post(f"/v1/environments/{env['id']}/publish", headers=st.admin, json={})
    assert pub.status_code == 201, pub.text
    return env, pub.json()


def run_nb(st: Any, ws: str, name: str, settings: dict[str, Any]) -> dict[str, Any]:
    a = st.agent(f"seam-{name}")
    st.wait_worker(a)
    n = item(st, ws, "notebook", name, {"nbformat": nb_json([("code", "x = 1")]), "settings": {"engine": "python", **settings}})
    r = st.http.post(f"/v1/notebooks/{n['id']}/run", headers=st.admin, json={})
    assert r.status_code == 202, r.text
    assert st.wait_job(r.json()["id"])["state"] == "succeeded"
    return a.commands_of("run_job")[-1]["payload"]["spec"]


def test_pip_policy_reaches_the_node_as_allow_pip_install(fleet):
    st = fleet
    ws = st.workspace()
    assert run_nb(st, ws, "declared", {})["policy"]["allow_pip_install"] is False                       # default policy 'declared': nothing outside the lock
    st.retire_agents()
    assert run_nb(st, ws, "open", {"pip_policy": "allow"})["policy"]["allow_pip_install"] is True
    st.retire_agents()
    assert run_nb(st, ws, "closed", {"pip_policy": "block"})["policy"]["allow_pip_install"] is False


def test_administrator_cap_beats_a_job_policy(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("seam-cap")
    st.wait_worker(a)
    body = {"kind": "script", "workspace_id": ws, "engine": "python", "source": {"format": "py", "content": "print(1)"}, "policy": {"allow_pip_install": True}}
    assert st.http.put("/v1/admin/settings/environments.pip_policy_max", headers=st.admin, json={"value": "declared"}).status_code == 200
    try:
        r = st.http.post("/v1/jobs", headers=st.admin, json=body)
        assert r.status_code in (403, 422) and "pip" in r.text.lower(), r.text
    finally:
        assert st.http.put("/v1/admin/settings/environments.pip_policy_max", headers=st.admin, json={"value": "allow"}).status_code == 200
    ok = st.http.post("/v1/jobs", headers=st.admin, json=body)
    assert ok.status_code == 202, ok.text
    assert st.wait_job(ok.json()["id"])["state"] == "succeeded"
    assert a.commands_of("run_job")[-1]["payload"]["spec"]["policy"]["allow_pip_install"] is True


def test_job_with_environment_gets_variables_layer_and_lock(fleet, tmp_path):
    st = fleet
    ws = st.workspace()
    a = st.agent("seam-env")
    st.wait_worker(a)
    env, pub = published_env(st, ws, tmp_path, variables={"MODE": "batch"})
    r = st.http.post("/v1/jobs", headers=st.admin, json={"kind": "script", "workspace_id": ws, "engine": "python", "source": {"format": "py", "content": "print(1)"},
                                                         "runtime": {"environment_id": env["id"]}, "env": {"EXTRA": "1"}})
    assert r.status_code == 202, r.text
    assert st.wait_job(r.json()["id"])["state"] == "succeeded"
    spec = a.commands_of("run_job")[-1]["payload"]["spec"]
    assert spec["env"]["MODE"] == "batch" and spec["env"]["EXTRA"] == "1"                      # environment variables merged under the explicit ones
    assert spec["runtime"]["environment"]["layer"]["key"].startswith("sha256:") and spec["runtime"]["dependency_lock"]["lock_hash"] == pub["lock"]["lock_hash"]


def test_explicit_dependency_lock_is_validated_and_gets_a_layer(fleet, tmp_path):
    st = fleet
    ws = st.workspace()
    a = st.agent("seam-lock")
    st.wait_worker(a)
    env, pub = published_env(st, ws, tmp_path, name="src-env")
    lock = pub["lock"]
    body = {"kind": "script", "workspace_id": ws, "engine": "python", "source": {"format": "py", "content": "print(1)"}}
    r = st.http.post("/v1/jobs", headers=st.admin, json={**body, "runtime": {"dependency_lock": lock}})
    assert r.status_code == 202, r.text
    assert st.wait_job(r.json()["id"])["state"] == "succeeded"
    spec = a.commands_of("run_job")[-1]["payload"]["spec"]
    assert spec["runtime"]["environment"]["layer"]["key"].startswith("sha256:") and spec["runtime"]["dependency_lock"]["lock_hash"] == lock["lock_hash"]
    # a lock whose content was changed after hashing is refused before anything is queued
    bad = copy.deepcopy(lock)
    bad["python"]["packages"][0]["hashes"] = ["0" * 64]
    r = st.http.post("/v1/jobs", headers=st.admin, json={**body, "runtime": {"dependency_lock": bad}})
    assert r.status_code in (400, 422), r.text


def test_session_gets_the_environment_variables_layer_and_pip_policy(fleet, tmp_path):
    st = fleet
    ws = st.workspace()
    a = st.agent("seam-ses")
    st.wait_worker(a)
    env, _pub = published_env(st, ws, tmp_path, name="ses-env", variables={"MODE": "interactive"})
    n = item(st, ws, "notebook", "ses-nb", {"nbformat": nb_json([("code", "x = 1")]), "settings": {"engine": "python", "environment_id": env["id"], "pip_policy": "allow"}})
    r = st.http.post(f"/v1/notebooks/{n['id']}/session", headers=st.admin, json={})
    assert r.status_code == 201, r.text
    from tests.compute.fake_agent import wait_until

    wait_until(lambda: a.commands_of("start_kernel"), 15, 0.1, "start_kernel")
    spec = a.commands_of("start_kernel")[0]["payload"]["spec"]
    assert spec["env"]["MODE"] == "interactive" and spec["runtime"]["environment"]["layer"]["key"].startswith("sha256:")
    assert spec["policy"]["allow_pip_install"] is True
    # the controller also accepts the environment directly on a session request
    r2 = st.http.post("/v1/sessions", headers=st.admin, json={"workspace_id": ws, "engine": "python", "environment_id": env["id"]})
    assert r2.status_code == 201, r2.text


def test_env_jar_endpoint_serves_content_addressed_jars_to_workers_and_admins_only(live):
    import hashlib

    st = live
    jar = b"PK\x03\x04 fake jar"
    sha = hashlib.sha256(jar).hexdigest()
    d = st.settings.path("artifacts", "env-jars")
    d.mkdir(parents=True, exist_ok=True)
    (d / f"{sha}.jar").write_bytes(jar)
    r = st.http.get(f"/v1/environments/jars/{sha}", headers=st.admin)
    assert r.status_code == 200 and r.content == jar and r.headers["x-content-sha256"] == sha
    assert st.http.get(f"/v1/environments/jars/{'0' * 64}", headers=st.admin).status_code == 404
    assert st.http.get("/v1/environments/jars/not-a-hash", headers=st.admin).status_code == 404
    assert st.http.get(f"/v1/environments/jars/{sha}").status_code == 401
