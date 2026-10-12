# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""End to end against a live control plane with fake worker agents: notebook run with a published environment, Spark job definition run, notebook session."""

from __future__ import annotations

import json
from typing import Any

from tests.items.items_support import nb_json
from tests.items.wheelbuilder import build_wheel


def item(st: Any, ws: str, type_: str, name: str, definition: dict[str, Any]) -> dict[str, Any]:
    r = st.http.post(f"/v1/workspaces/{ws}/items", headers=st.admin, json={"type": type_, "name": name, "definition": definition})
    assert r.status_code == 201, r.text
    return r.json()


def wheelhouse(st: Any, tmp_path: Any) -> Any:
    d = tmp_path / "wheelhouse"
    build_wheel(d, "olcf-e2e-lib", "3.1.4")
    r = st.http.put("/v1/admin/settings/environments.wheelhouses", headers=st.admin, json={"value": [str(d)]})
    assert r.status_code == 200, r.text
    return d


def test_notebook_run_with_published_environment_reaches_the_agent(fleet, tmp_path):
    st = fleet
    wh = wheelhouse(st, tmp_path)
    ws = st.workspace()
    a = st.agent("items-nb")
    st.wait_worker(a)
    env = item(st, ws, "environment", "libs", {"python": {"requirements": ["olcf-e2e-lib"], "index": {"no_index": True, "find_links": [str(wh)]}}, "variables": {"MODE": "batch"}})
    pub = st.http.post(f"/v1/environments/{env['id']}/publish", headers=st.admin, json={})
    assert pub.status_code == 201, pub.text
    lock_hash = pub.json()["lock"]["lock_hash"]
    nb = nb_json([("code", "region = 'eu'\nlimit = 5"), ("code", "print(region, limit)")])
    nb["cells"][0]["metadata"] = {"tags": ["parameters"]}
    n = item(st, ws, "notebook", "report-nb", {"nbformat": nb, "settings": {"environment_id": env["id"]}})
    r = st.http.post(f"/v1/notebooks/{n['id']}/run", headers=st.admin, json={"parameters": {"region": "us", "limit": "7"}})
    assert r.status_code == 202, r.text
    job = r.json()
    done = st.wait_job(job["id"])
    assert done["state"] == "succeeded", done
    assert done["kind"] == "notebook" and done["item_id"] == n["id"] and done["parameters"] == {"region": "us", "limit": 7}  # typed against the notebook's parameter cell
    (cmd,) = a.commands_of("run_job")
    spec = cmd["payload"]["spec"]
    assert spec["parameters"] == {"region": "us", "limit": 7}
    assert spec["runtime"]["dependency_lock"]["lock_hash"] == lock_hash
    assert spec["runtime"]["environment"]["lock_hash"] == lock_hash and spec["runtime"]["environment"]["item_id"] == env["id"]
    assert spec["env"]["MODE"] == "batch"
    # listed under the notebook's runs
    runs = st.http.get(f"/v1/notebooks/{n['id']}/runs", headers=st.admin).json()["items"]
    assert [x["id"] for x in runs] == [job["id"]]
    # an invalid parameter type is rejected before anything is queued
    bad = st.http.post(f"/v1/notebooks/{n['id']}/run", headers=st.admin, json={"parameters": {"limit": "many"}})
    assert bad.status_code == 422


def test_spark_job_definition_run_reaches_the_agent(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("items-sj")
    st.wait_worker(a)
    sj = item(st, ws, "spark_job_definition", "etl", {"language": "python", "code": "print('go')", "args": ["--day", "2026-01-01"], "conf": {"spark.sql.shuffle.partitions": 4}, "retries": 1,
                                                       "timeout_seconds": 900})
    r = st.http.post(f"/v1/spark-job-definitions/{sj['id']}/run", headers=st.admin, json={})
    assert r.status_code == 202, r.text
    done = st.wait_job(r.json()["id"])
    assert done["state"] == "succeeded" and done["kind"] == "spark_job"
    (cmd,) = a.commands_of("run_job")
    spec = cmd["payload"]["spec"]
    assert spec["kind"] == "spark_job" and spec["limits"]["timeout_seconds"] == 900
    assert "spark.sql.shuffle.partitions" in json.dumps(spec)
    assert "--day" in json.dumps(cmd["payload"]) and "print('go')" in json.dumps(cmd["payload"])


def test_notebook_session_starts_a_kernel(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("items-ses")
    st.wait_worker(a)
    n = item(st, ws, "notebook", "interactive", {"nbformat": nb_json([("code", "x = 1")])})
    r = st.http.post(f"/v1/notebooks/{n['id']}/session", headers=st.admin, json={})
    assert r.status_code == 201, r.text
    s = r.json()
    assert s["id"].startswith("ses_")
    from tests.compute.fake_agent import wait_until

    wait_until(lambda: a.commands_of("start_kernel"), 15, 0.1, "start_kernel")
    assert s["id"] in a.kernels or a.commands_of("start_kernel")[0]["payload"]["session_id"] == s["id"]
