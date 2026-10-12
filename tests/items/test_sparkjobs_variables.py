# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""Spark job definitions (validation, normalisation, run) and variable sets (typed values, per-stage overrides, secrets by reference, injection)."""

from __future__ import annotations

import copy

import pytest

from olcf.core.errors import NotFound, ValidationFailed
from olcf.notebooks import sparkjobs as sj
from olcf.notebooks import variables as var
from tests.items.items_support import nb_json


# ------------------------------------------------------------------------------------------------ spark job definitions
def test_definition_normalisation_is_idempotent_and_aliases_do_not_duplicate():
    d = sj.normalise_definition({"language": "python", "code": "print(1)", "args": ["--a", "1"], "conf": {"spark.sql.shuffle.partitions": 7, "spark.x": True}, "timeout_seconds": 90, "retries": 2,
                                 "reference_files": ["Files/libs/h.py"], "settings": {"default_lakehouse": {"id": "lak_x"}}})
    assert sj.normalise_definition(copy.deepcopy(d)) == d
    assert d["settings"]["runtime"]["spark_conf"] == {"spark.sql.shuffle.partitions": "7", "spark.x": "true"}
    assert d["settings"]["policy"]["timeout_seconds"] == 90 and d["settings"]["policy"]["retries"] == 2
    assert d["settings"]["engine"] == "spark" and d["settings"]["default_lakehouse"] == {"lakehouse_id": "lak_x"}
    assert "conf" not in d and "retries" not in d and "timeout_seconds" not in d


@pytest.mark.parametrize("bad", [
    {"language": "cobol", "code": "x"},
    {"language": "python"},  # neither main_file nor code
    {"language": "python", "code": "x = 1", "main_file": "Files/a.py"},  # both
    {"language": "python", "code": "def broken(:\n"},
    {"language": "python", "main_file": "Files/a.txt"},
    {"language": "python", "main_file": "../../etc/passwd.py"},
    {"language": "python", "main_file": "file:///etc/passwd.py"},
    {"language": "python", "main_file": "/abs/a.py"},
    {"language": "scala", "main_file": "Files/a.jar"},  # no main_class
    {"language": "scala", "main_class": "a.b.C"},  # no jar
    {"language": "java", "main_file": "Files/a.jar", "main_class": "bad class name"},
    {"language": "scala", "main_file": "Files/a.jar", "main_class": "a.B", "code": "print(1)"},
    {"language": "python", "code": "x", "main_class": "a.B"},
    {"language": "python", "code": "x", "args": [1]},
    {"language": "python", "code": "x", "args": ["a\x00b"]},
    {"language": "python", "code": "x", "conf": {"notspark": 1}},
    {"language": "python", "code": "x", "retries": 99},
    {"language": "python", "code": "x", "timeout_seconds": 0},
    {"language": "python", "code": "x", "settings": {"engine": "duckdb"}},
    {"language": "python", "code": "x", "unknown_key": 1},
    {"language": "python", "code": "x", "reference_files": ["gopher://x/y"]},
])
def test_invalid_definitions_are_rejected(bad):
    with pytest.raises(ValidationFailed):
        sj.normalise_definition(bad)


def test_scala_and_java_jar_jobs_validate():
    d = sj.normalise_definition({"language": "scala", "main_file": "abfss://c@acct.dfs.core.windows.net/jars/app.jar", "main_class": "com.acme.Main", "args": ["x"]})
    assert d["main_class"] == "com.acme.Main" and d["main_file"].endswith("app.jar")
    assert sj.normalise_definition({"language": "java", "main_file": "olcf://ws/lh/Files/app.jar", "main_class": "a.B"})["language"] == "java"


def test_spark_job_item_api_and_run(ic):
    ws = ic.workspace()
    lh = ic.post(f"/v1/workspaces/{ws['id']}/lakehouses", json={"name": "lh"}, headers=ic.admin).json()
    r = ic.post(f"/v1/workspaces/{ws['id']}/items", json={"type": "spark_job_definition", "name": "bad", "definition": {"language": "python"}}, headers=ic.admin)
    assert r.status_code == 422
    it = ic.item(ws["id"], "spark_job_definition", "etl", {"language": "python", "code": "print(dt)", "args": ["--x"], "conf": {"spark.sql.shuffle.partitions": 4}, "timeout_seconds": 600, "retries": 1,
                                                          "settings": {"default_lakehouse": {"id": lh["id"]}}})
    v = ic.get(f"/v1/spark-job-definitions/{it['id']}/validate", headers=ic.admin).json()
    sub = v["submission"]
    assert sub["kind"] == "spark_job" and sub["item_id"] == it["id"] and sub["default_lakehouse"]["lakehouse_id"] == lh["id"]
    assert sub["runtime"]["spark_conf"] == {"spark.sql.shuffle.partitions": "4"} and sub["policy"]["timeout_seconds"] == 600 and sub["policy"]["retries"] == 1
    r = ic.post(f"/v1/spark-job-definitions/{it['id']}/run", json={"parameters": {"dt": "2026-01-01"}, "conf": {"spark.executor.memory": "2g"}, "tags": {"a": "b"}}, headers={**ic.admin, "Idempotency-Key": "sj-1"})
    assert r.status_code == 202, r.text
    job = r.json()
    assert job["kind"] == "spark_job" and job["parameters"] == {"dt": "2026-01-01"} and job["item_id"] == it["id"]
    again = ic.post(f"/v1/spark-job-definitions/{it['id']}/run", json={"parameters": {"dt": "2026-01-01"}, "conf": {"spark.executor.memory": "2g"}, "tags": {"a": "b"}},
                    headers={**ic.admin, "Idempotency-Key": "sj-1"})
    assert again.json()["id"] == job["id"]
    assert [x["id"] for x in ic.get(f"/v1/spark-job-definitions/{it['id']}/runs", headers=ic.admin).json()["items"]] == [job["id"]]
    assert ic.post(f"/v1/spark-job-definitions/{it['id']}/run", json={"parameters": {"bad name": 1}}, headers=ic.admin).status_code == 422
    assert ic.post(f"/v1/spark-job-definitions/{it['id']}/run", json={"conf": {"x": 1}}, headers=ic.admin).status_code == 422
    viewer = ic.user("vera", "Viewer", workspace_id=ws["id"])
    assert ic.post(f"/v1/spark-job-definitions/{it['id']}/run", json={}, headers=viewer).status_code == 403
    assert ic.get(f"/v1/spark-job-definitions/{it['id']}/validate", headers=viewer).status_code == 200
    # a notebook id is not a spark job definition
    nb = ic.item(ws["id"], "notebook", "n", {"nbformat": nb_json([("code", "1")])})
    assert ic.post(f"/v1/spark-job-definitions/{nb['id']}/run", json={}, headers=ic.admin).status_code == 404


# ------------------------------------------------------------------------------------------------ variable sets
def make_secret(ic, name="vs-secret", value="not-a-real-secret-value"):
    r = ic.post("/v1/secrets", json={"name": name, "value": value}, headers=ic.admin)
    assert r.status_code in (200, 201), r.text
    return r.json()["id"]


def vset(ic, ws, name="vars", **defn):
    base = {"stages": ["dev", "test", "prod"], "default_stage": "dev", "variables": [
        {"name": "container", "type": "string", "default": "raw-dev", "overrides": {"test": "raw-test", "prod": "raw"}},
        {"name": "retries", "type": "integer", "default": 1, "overrides": {"prod": 5}},
        {"name": "dry_run", "type": "boolean", "default": True, "overrides": {"prod": False}},
        {"name": "tables", "type": "array", "default": ["a"]},
        {"name": "limits", "type": "object", "default": {"rows": 10}},
        {"name": "ratio", "type": "number", "default": 0.5},
    ]}
    base.update(defn)
    return ic.item(ws["id"], "variable_set", name, base)


def test_resolve_by_stage_with_typed_values(ic):
    ws = ic.workspace()
    vs = vset(ic, ws)
    got = {s: ic.get(f"/v1/variable-sets/{vs['id']}/resolve?stage={s}", headers=ic.admin).json() for s in ("dev", "test", "prod")}
    assert got["dev"]["values"]["container"] == "raw-dev" and got["test"]["values"]["container"] == "raw-test" and got["prod"]["values"]["container"] == "raw"
    assert got["prod"]["values"]["retries"] == 5 and got["dev"]["values"]["retries"] == 1
    assert got["prod"]["values"]["dry_run"] is False and got["dev"]["values"]["dry_run"] is True
    assert got["prod"]["values"]["tables"] == ["a"] and got["prod"]["values"]["limits"] == {"rows": 10} and got["prod"]["values"]["ratio"] == 0.5
    assert got["prod"]["stage"] == "prod" and got["prod"]["stages"] == ["dev", "test", "prod"]
    src = {v["name"]: v["source"] for v in got["prod"]["variables"]}
    assert src["container"] == "override" and src["tables"] == "default"
    assert ic.get(f"/v1/variable-sets/{vs['id']}/resolve?stage=qa", headers=ic.admin).status_code == 422
    assert ic.get(f"/v1/variable-sets/{vs['id']}/resolve?stage=a%20b", headers=ic.admin).status_code == 422


def test_stage_precedence_explicit_workspace_default(ic):
    ws = ic.workspace()
    vs = vset(ic, ws)
    url = f"/v1/variable-sets/{vs['id']}/resolve"
    assert ic.get(url, headers=ic.admin).json()["stage"] == "dev"  # the set's default stage
    assert ic.put(f"/v1/workspaces/{ws['id']}/stage", json={"stage": "prod"}, headers=ic.admin).status_code == 200
    assert ic.get(f"/v1/workspaces/{ws['id']}/stage", headers=ic.admin).json()["stage"] == "prod"
    assert ic.get(url, headers=ic.admin).json()["values"]["container"] == "raw"  # the workspace's active stage
    assert ic.get(url + "?stage=test", headers=ic.admin).json()["values"]["container"] == "raw-test"  # explicit beats workspace
    assert ic.put(f"/v1/workspaces/{ws['id']}/stage", json={"stage": None}, headers=ic.admin).status_code == 200
    assert ic.get(url, headers=ic.admin).json()["stage"] == "dev"
    assert ic.put(f"/v1/workspaces/{ws['id']}/stage", json={"stage": "bad stage!"}, headers=ic.admin).status_code == 422
    viewer = ic.user("vv", "Viewer", workspace_id=ws["id"])
    assert ic.put(f"/v1/workspaces/{ws['id']}/stage", json={"stage": "test"}, headers=viewer).status_code == 403
    assert ic.get(f"/v1/workspaces/{ws['id']}/stage", headers=viewer).status_code == 200


def test_secrets_are_references_only(ic):
    ws = ic.workspace()
    sid = make_secret(ic)
    vs = vset(ic, ws, variables=[{"name": "api_key", "type": "secret", "default": sid}, {"name": "plain", "type": "string", "default": "x"}])
    res = ic.get(f"/v1/variable-sets/{vs['id']}/resolve", headers=ic.admin).json()
    assert res["secrets"] == {"api_key": sid} and "api_key" not in res["values"]
    assert "not-a-real-secret-value" not in str(res)
    d = next(v for v in res["variables"] if v["name"] == "api_key")
    assert d["secret_ref"] == sid and "value" not in d
    # a secret variable must hold a secret id; plain values and credential-looking values are refused
    for bad in ({"name": "api_key", "type": "secret", "default": "hunter2hunter2"},
                {"name": "db_password", "type": "string", "default": "hunter2hunter2"},
                {"name": "token", "type": "string", "overrides": {"prod": "abc"}},
                {"name": "conn", "type": "string", "default": "postgresql://user:pa55w0rd@db/x"},
                {"name": "k", "type": "string", "default": "AKIAABCDEFGHIJKLMNOP"}):  # gitleaks:allow
        r = ic.post(f"/v1/workspaces/{ws['id']}/items", json={"type": "variable_set", "name": "bad", "definition": {"variables": [bad]}}, headers=ic.admin)
        assert r.status_code == 422, (bad, r.text)
    # a dangling secret reference is reported by the service (check_secrets) but the REST view stays a pure read
    ghost = vset(ic, ws, name="ghost", variables=[{"name": "k", "type": "secret", "default": "sec_00000000000000000000000000"}])
    with pytest.raises(ValidationFailed):
        ic.ctx.service("variables").resolve(ghost["id"], "dev", workspace_id=ws["id"], check_secrets=True)


@pytest.mark.parametrize("bad", [
    {"stages": []}, {"stages": ["a", "a"]}, {"stages": ["bad stage"]}, {"default_stage": "nope"},
    {"variables": [{"name": "1x"}]}, {"variables": [{"name": "a"}, {"name": "a"}]}, {"variables": [{"name": "a", "type": "money"}]},
    {"variables": [{"name": "a", "type": "integer", "default": "1"}]}, {"variables": [{"name": "a", "type": "integer", "default": True}]},
    {"variables": [{"name": "a", "type": "string", "overrides": {"qa": "x"}}]}, {"variables": [{"name": "a", "extra": 1}]}, {"unknown": 1},
])
def test_invalid_variable_sets(bad):
    with pytest.raises(ValidationFailed):
        var.normalise_definition(bad)


def test_required_variable_without_value_for_the_stage(ic):
    ws = ic.workspace()
    vs = vset(ic, ws, variables=[{"name": "must", "type": "string", "required": True, "overrides": {"prod": "x"}}])
    assert ic.get(f"/v1/variable-sets/{vs['id']}/resolve?stage=prod", headers=ic.admin).status_code == 200
    r = ic.get(f"/v1/variable-sets/{vs['id']}/resolve?stage=dev", headers=ic.admin)
    assert r.status_code == 422 and r.json()["error"]["details"]["missing"] == ["must"]


def test_substitute_placeholders_keep_types_and_fail_closed():
    values = {"n": 5, "s": "x", "l": [1, 2]}
    out = var.substitute({"a": "${var:n}", "b": "pre-${var:s}-post", "c": ["${var:l}", 1]}, values, secrets={"k": "sec_abc"})
    assert out == {"a": 5, "b": "pre-x-post", "c": [[1, 2], 1]}
    with pytest.raises(ValidationFailed):
        var.substitute({"d": "${var:unknown}"}, values, secrets={})
    with pytest.raises(ValidationFailed):  # a secret never becomes text
        var.substitute({"d": "x ${var:k}"}, values, secrets={"k": "sec_abc"})


def test_variable_service_is_registered_for_other_modules_and_checks_authz(ic):
    ws, other = ic.workspace("a"), ic.workspace("b")
    vs = vset(ic, ws)
    svc = ic.ctx.service("variables")
    assert svc.resolve(vs["id"], "prod")["values"]["container"] == "raw"
    assert svc.resolve("vars", "test", workspace_id=ws["id"])["values"]["container"] == "raw-test"  # by name inside a workspace
    with pytest.raises(NotFound):
        svc.resolve(vs["id"], "prod", workspace_id=other["id"])  # id of another workspace
    with pytest.raises(NotFound):
        svc.resolve("vars", "prod")  # a name needs a workspace
    outsider = ic.user("olga", "Contributor", workspace_id=other["id"])
    assert ic.get(f"/v1/variable-sets/{vs['id']}/resolve", headers=outsider).status_code in (403, 404)
    assert svc.substitute({"x": "${var:container}"}, vs["id"], "prod")["x"] == "raw"


def test_variable_set_feeds_notebook_parameters_and_secret_refs(ic):
    ws = ic.workspace()
    sid = make_secret(ic)
    vs = vset(ic, ws, variables=[{"name": "container", "type": "string", "default": "raw-dev", "overrides": {"prod": "raw"}}, {"name": "api_key", "type": "secret", "default": sid},
                                 {"name": "not_a_param", "type": "string", "default": "ignored"}])
    nb = nb_json([("code", "container = 'x'\nretries = 1"), ("code", "print(container)")])
    nb["cells"][0]["metadata"] = {"tags": ["parameters"]}
    it = ic.item(ws["id"], "notebook", "nb", {"nbformat": nb, "settings": {"variable_set": "vars"}})
    # variables with the name of a declared parameter become its value for the chosen stage; an explicit parameter wins
    r = ic.post(f"/v1/notebooks/{it['id']}/run", json={"stage": "prod", "parameters": {"retries": 3}}, headers=ic.admin)
    assert r.status_code == 202, r.text
    job = r.json()
    assert job["parameters"] == {"container": "raw", "retries": 3}
    assert job["notes"]["variable_set"] == {"id": vs["id"], "name": "vars", "stage": "prod"}
    full = ic.get(f"/v1/jobs/{job['id']}", headers=ic.admin).json()
    assert "not-a-real-secret-value" not in str(full)
    r2 = ic.post(f"/v1/notebooks/{it['id']}/run", json={"parameters": {"container": "explicit"}}, headers=ic.admin)
    assert r2.json()["parameters"]["container"] == "explicit"
    assert ic.post(f"/v1/notebooks/{it['id']}/run", json={"stage": "nowhere"}, headers=ic.admin).status_code == 422
    # the same for Spark job definitions
    sjd = ic.item(ws["id"], "spark_job_definition", "sj", {"language": "python", "code": "print(container)", "settings": {"variable_set": "vars"}})
    r3 = ic.post(f"/v1/spark-job-definitions/{sjd['id']}/run", json={"stage": "prod"}, headers=ic.admin)
    assert r3.status_code == 202 and r3.json()["parameters"]["container"] == "raw"
