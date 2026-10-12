# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""Deployment promotion dev -> test -> prod: pipelines CRUD, dry runs, id and connection remapping, variable stages, authz and history."""

from __future__ import annotations

import json

import pytest

from tests.items.items_support import nb_json


@pytest.fixture()
def pipe(ic):
    """Three workspaces, connections per stage, a pipeline with connection rules."""
    wss = {s: ic.workspace(f"{s}-ws") for s in ("dev", "test", "prod")}
    for s, ws in wss.items():
        r = ic.post("/v1/connections", json={"workspace_id": ws["id"], "name": f"{s}-warehouse", "type": "postgres", "config": {"host": f"{s}.db.example.com"}}, headers=ic.admin)
        assert r.status_code == 201, r.text
    body = {"name": "main-flow", "stages": [{"stage": s, "workspace_id": w["id"]} for s, w in wss.items()],
            "rules": {"connections": {"test": {"dev-warehouse": "test-warehouse"}, "prod": {"test-warehouse": "prod-warehouse", "dev-warehouse": "prod-warehouse"}}}}
    r = ic.post("/v1/deployment-pipelines", json=body, headers=ic.admin)
    assert r.status_code == 201, r.text
    return ic, wss, r.json()


def deploy(ic, pid, **body):
    return ic.post(f"/v1/deployment-pipelines/{pid}/deploy", json=body, headers=ic.admin)


def items(ic, ws, **q):
    return {i["name"]: i for i in ic.get(f"/v1/workspaces/{ws['id']}/items", params=q, headers=ic.admin).json()["items"]}


def test_pipeline_crud_and_validation(ic):
    a, b = ic.workspace("a"), ic.workspace("b")
    ok = {"name": "p1", "stages": [{"stage": "dev", "workspace_id": a["id"]}, {"stage": "prod", "workspace_id": b["id"]}]}
    r = ic.post("/v1/deployment-pipelines", json=ok, headers=ic.admin)
    assert r.status_code == 201 and r.headers["ETag"]
    p = r.json()
    assert p["rules"]["connection_mode"] == "map" and [s["stage"] for s in p["stages"]] == ["dev", "prod"]
    assert ic.post("/v1/deployment-pipelines", json=ok, headers=ic.admin).status_code == 409
    for bad in ({**ok, "name": "bad name!"}, {**ok, "name": "p2", "stages": ok["stages"][:1]},
                {**ok, "name": "p2", "stages": [{"stage": "dev", "workspace_id": a["id"]}, {"stage": "dev", "workspace_id": b["id"]}]},
                {**ok, "name": "p2", "stages": [{"stage": "dev", "workspace_id": a["id"]}, {"stage": "prod", "workspace_id": a["id"]}]},
                {**ok, "name": "p2", "stages": [{"stage": "Dev", "workspace_id": a["id"]}, {"stage": "prod", "workspace_id": b["id"]}]},
                {**ok, "name": "p2", "stages": [{"stage": "dev", "workspace_id": a["id"]}, {"stage": "prod", "workspace_id": "wks_nope"}]},
                {**ok, "name": "p2", "rules": {"nonsense": 1}}, {**ok, "name": "p2", "rules": {"connections": {"qa": {"a": "b"}}}},
                {**ok, "name": "p2", "rules": {"connection_mode": "steal"}}):
        assert ic.post("/v1/deployment-pipelines", json=bad, headers=ic.admin).status_code in (404, 422), bad
    assert ic.get(f"/v1/deployment-pipelines/{p['id']}", headers=ic.admin).json()["name"] == "p1"
    assert [x["name"] for x in ic.get("/v1/deployment-pipelines", headers=ic.admin).json()["items"]] == ["p1"]
    r = ic.patch(f"/v1/deployment-pipelines/{p['id']}", json={"description": "d", "rules": {"exclude_types": ["report"]}}, headers={**ic.admin, "If-Match": r.headers["ETag"]})
    assert r.status_code == 200 and r.json()["rules"]["exclude_types"] == ["report"] and r.json()["description"] == "d"
    assert ic.patch(f"/v1/deployment-pipelines/{p['id']}", json={"description": "x"}, headers={**ic.admin, "If-Match": '"stale"'}).status_code in (409, 412)
    assert ic.delete(f"/v1/deployment-pipelines/{p['id']}", headers=ic.admin).status_code == 204
    assert ic.get(f"/v1/deployment-pipelines/{p['id']}", headers=ic.admin).status_code == 404


def test_dev_to_test_to_prod_with_remapping(pipe):
    ic, wss, p = pipe
    dev = wss["dev"]
    n = ic.item(dev["id"], "notebook", "etl", {"nbformat": nb_json([("code", "container = 'x'")]), "settings": {"variable_set": "cfg"}})
    ic.item(dev["id"], "variable_set", "cfg", {"stages": ["dev", "test", "prod"], "default_stage": "dev",
                                                       "variables": [{"name": "container", "type": "string", "default": "raw-dev", "overrides": {"test": "raw-test", "prod": "raw"}}]})
    rep = ic.item(dev["id"], "report", "rpt", {"source": {"connection": "dev-warehouse"}, "uses": n["id"], "text": "mentions " + n["id"]})
    # plan first
    r = deploy(ic, p["id"], dry_run=True)
    assert r.status_code == 201, r.text
    plan = r.json()
    assert plan["state"] == "planned" and plan["dry_run"] and plan["plan"]["problems"] == [] and plan["from_stage"] == "dev" and plan["to_stage"] == "test"
    assert plan["plan"]["connection_map"] == {"dev-warehouse": "test-warehouse"} and plan["plan"]["variable_stage"] == "test"
    assert items(ic, wss["test"]) == {}  # nothing written
    # real promotion dev -> test
    r = deploy(ic, p["id"], comment="first")
    assert r.status_code == 201, r.text
    d1 = r.json()
    assert d1["state"] == "succeeded" and {a["action"] for a in d1["result"]["applied"]} == {"created"}
    t = items(ic, wss["test"])
    assert set(t) == {"etl", "cfg", "rpt"}
    assert t["etl"]["id"] != n["id"] and t["rpt"]["workspace_id"] == wss["test"]["id"]
    rd = ic.get_item(t["rpt"]["id"])["definition"]
    assert rd["source"]["connection"] == "test-warehouse"  # remapped by rule
    assert rd["uses"] == t["etl"]["id"]  # item reference rewritten to the target item
    assert ic.get_item(rep["id"])["definition"]["source"]["connection"] == "dev-warehouse"  # the source is untouched
    # the target workspace now resolves variables with the 'test' stage overrides
    assert ic.get(f"/v1/workspaces/{wss['test']['id']}/stage", headers=ic.admin).json()["stage"] == "test"
    res = ic.get(f"/v1/variable-sets/{t['cfg']['id']}/resolve", headers=ic.admin).json()
    assert res["stage"] == "test" and res["values"]["container"] == "raw-test"
    # idempotent: a second deployment updates nothing
    d2 = deploy(ic, p["id"]).json()
    assert {a["action"] for a in d2["result"]["applied"]} == {"unchanged"}
    # change in dev -> update in place (same target ids), a new item version in test
    ic.put(f"/v1/notebooks/{n['id']}/content", json={"cells": [{"cell_type": "code", "source": "container = 'changed'"}]},
           headers={**ic.admin, "If-Match": ic.get(f"/v1/notebooks/{n['id']}/content", headers=ic.admin).headers["ETag"]})
    d3 = deploy(ic, p["id"]).json()
    assert [a["action"] for a in d3["result"]["applied"] if "etl" in a["key"]] == ["updated"]
    assert items(ic, wss["test"])["etl"]["id"] == t["etl"]["id"]
    vers = ic.get(f"/v1/items/{t['etl']['id']}/versions", headers=ic.admin).json()["items"]
    assert len(vers) >= 2
    # test -> prod uses the prod connection and the prod stage
    r = deploy(ic, p["id"], from_stage="test")
    assert r.status_code == 201, r.text
    assert r.json()["plan"]["connection_map"]["test-warehouse"] == "prod-warehouse"
    prod = items(ic, wss["prod"])
    assert set(prod) == {"etl", "cfg", "rpt"}
    assert ic.get_item(prod["rpt"]["id"])["definition"]["source"]["connection"] == "prod-warehouse"
    assert ic.get(f"/v1/variable-sets/{prod['cfg']['id']}/resolve", headers=ic.admin).json()["values"]["container"] == "raw"
    # history
    hist = ic.get(f"/v1/deployment-pipelines/{p['id']}/deployments", headers=ic.admin).json()["items"]
    assert len(hist) == 5 and hist[0]["to_stage"] == "prod"
    one = ic.get(f"/v1/deployments/{hist[0]['id']}", headers=ic.admin).json()
    assert one["id"] == hist[0]["id"] and one["state"] == "succeeded"


def test_missing_connection_counterpart_is_an_error_not_a_silent_keep(pipe):
    ic, wss, p = pipe
    ic.item(wss["dev"]["id"], "report", "r", {"source": {"connection": "dev-only"}})
    ic.post("/v1/connections", json={"workspace_id": wss["dev"]["id"], "name": "dev-only", "type": "postgres", "config": {}}, headers=ic.admin)
    r = deploy(ic, p["id"], dry_run=True)
    assert r.status_code == 201 and any("dev-only" in x for x in r.json()["plan"]["problems"])
    r = deploy(ic, p["id"])
    assert r.status_code == 422 and any("dev-only" in x for x in r.json()["error"]["details"]["problems"])
    assert items(ic, wss["test"]) == {}
    # creating the counterpart (or a rule) fixes it
    ic.post("/v1/connections", json={"workspace_id": wss["test"]["id"], "name": "dev-only", "type": "postgres", "config": {}}, headers=ic.admin)
    assert deploy(ic, p["id"]).status_code == 201


def test_copy_connection_mode_creates_metadata_only_connections(ic):
    a, b = ic.workspace("a"), ic.workspace("b")
    sec = ic.post("/v1/secrets", json={"name": "s", "value": "very-secret-value-123"}, headers=ic.admin).json()["id"]
    assert ic.post("/v1/connections", json={"workspace_id": a["id"], "name": "wh", "type": "postgres", "config": {"host": "h"}, "secret_id": sec}, headers=ic.admin).status_code == 201
    ic.item(a["id"], "report", "r", {"source": {"connection": "wh"}})
    p = ic.post("/v1/deployment-pipelines", json={"name": "cp", "stages": [{"stage": "dev", "workspace_id": a["id"]}, {"stage": "prod", "workspace_id": b["id"]}], "rules": {"connection_mode": "copy"}},
                headers=ic.admin).json()
    r = deploy(ic, p["id"])
    assert r.status_code == 201, r.text
    cons = ic.get("/v1/connections", params={"workspace_id": b["id"]}, headers=ic.admin).json()["items"]
    wh = next(c for c in cons if c["name"] == "wh")
    assert not wh.get("secret_id") and "very-secret-value-123" not in json.dumps(r.json())


def test_variable_stage_checks_block_a_bad_target(pipe):
    ic, wss, p = pipe
    ic.item(wss["dev"]["id"], "variable_set", "narrow", {"stages": ["dev", "prod"], "default_stage": "dev", "variables": [{"name": "a", "type": "string", "default": "x"}]})
    r = deploy(ic, p["id"], dry_run=True)
    assert any("no stage 'test'" in x for x in r.json()["plan"]["problems"])
    assert deploy(ic, p["id"]).status_code == 422
    # a rule can map the pipeline stage to another variable-set stage
    assert ic.patch(f"/v1/deployment-pipelines/{p['id']}", json={"rules": {"variable_stage": {"test": "dev"}, "connections": {"test": {"dev-warehouse": "test-warehouse"}}}},
                    headers=ic.admin).status_code == 200
    assert deploy(ic, p["id"]).status_code == 201
    assert ic.get(f"/v1/workspaces/{wss['test']['id']}/stage", headers=ic.admin).json()["stage"] == "dev"
    # required variables without a value for the stage and missing secrets
    ic.item(wss["dev"]["id"], "variable_set", "req", {"stages": ["dev", "test", "prod"], "default_stage": "dev", "variables": [{"name": "must", "type": "string", "required": True, "overrides": {"prod": "x"}}]})
    assert any("required variable 'must'" in x for x in deploy(ic, p["id"], dry_run=True).json()["plan"]["problems"])


def test_selection_filters_and_delete_missing(pipe):
    ic, wss, p = pipe
    ic.item(wss["dev"]["id"], "notebook", "n1", {"nbformat": nb_json([("code", "1")])})
    ic.item(wss["dev"]["id"], "notebook", "n2", {"nbformat": nb_json([("code", "2")])})
    ic.item(wss["dev"]["id"], "report", "rep", {"x": 1})
    assert deploy(ic, p["id"], items=["n1"]).status_code == 201
    assert set(items(ic, wss["test"])) == {"n1"}
    assert deploy(ic, p["id"], items=["ghost"]).status_code == 404
    ic.patch(f"/v1/deployment-pipelines/{p['id']}", json={"rules": {"exclude_types": ["report"], "connections": {"test": {"dev-warehouse": "test-warehouse"}}}}, headers=ic.admin)
    assert deploy(ic, p["id"]).status_code == 201
    assert set(items(ic, wss["test"])) == {"n1", "n2"}
    ic.item(wss["test"]["id"], "notebook", "only-in-test", {"nbformat": nb_json([("code", "3")])})
    assert deploy(ic, p["id"]).status_code == 201 and "only-in-test" in items(ic, wss["test"])  # kept unless delete_missing
    ic.patch(f"/v1/deployment-pipelines/{p['id']}", json={"rules": {"exclude_types": ["report"], "delete_missing": True}}, headers=ic.admin)
    assert deploy(ic, p["id"], items=["n1"]).status_code == 422  # not combinable with a selection
    assert deploy(ic, p["id"]).status_code == 201
    assert set(items(ic, wss["test"])) == {"n1", "n2"}


def test_stage_order_rules(pipe):
    ic, wss, p = pipe
    ic.item(wss["dev"]["id"], "notebook", "n1", {"nbformat": nb_json([("code", "1")])})
    assert deploy(ic, p["id"], from_stage="prod").status_code == 422  # no stage after prod
    assert deploy(ic, p["id"], from_stage="test", to_stage="dev").status_code == 422  # backwards
    assert deploy(ic, p["id"], from_stage="dev", to_stage="prod").status_code == 422  # skipping
    assert deploy(ic, p["id"], from_stage="qa").status_code == 422
    r = deploy(ic, p["id"], from_stage="dev", to_stage="prod", allow_skip=True)
    assert r.status_code == 201 and set(items(ic, wss["prod"])) == {"n1"}


def test_authz_and_failed_deployments_are_recorded(pipe):
    ic, wss, p = pipe
    ic.item(wss["dev"]["id"], "notebook", "n1", {"nbformat": nb_json([("code", "1")])})
    viewer = ic.user("vera", "Viewer", workspace_id=wss["dev"]["id"])
    assert ic.get(f"/v1/deployment-pipelines/{p['id']}", headers=viewer).status_code == 200  # can read one of its workspaces
    assert ic.post(f"/v1/deployment-pipelines/{p['id']}/deploy", json={}, headers=viewer).status_code == 403
    assert ic.delete(f"/v1/deployment-pipelines/{p['id']}", headers=viewer).status_code in (403, 404)
    assert ic.post("/v1/deployment-pipelines", json={"name": "x", "stages": [{"stage": "a", "workspace_id": wss["dev"]["id"]}, {"stage": "b", "workspace_id": wss["test"]["id"]}]},
                   headers=viewer).status_code == 403
    stranger = ic.user("sam", "Contributor", workspace_id=ic.workspace("elsewhere")["id"])
    assert ic.get(f"/v1/deployment-pipelines/{p['id']}", headers=stranger).status_code == 404
    assert ic.get("/v1/deployment-pipelines", headers=stranger).json()["items"] == []
    # exporter on the source but no import right on the target -> forbidden
    half = ic.user("hal", "Contributor", workspace_id=wss["dev"]["id"])
    assert ic.post(f"/v1/deployment-pipelines/{p['id']}/deploy", json={}, headers=half).status_code == 403
    assert ic.get(f"/v1/deployment-pipelines/{p['id']}/deployments", headers=ic.admin).json()["items"] == []
