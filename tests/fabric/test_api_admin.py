# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Administration endpoints of the adapter: configuration, readiness, diagnostics, analyze, rules, snippet, authorisation."""

from __future__ import annotations

import json

from tests.fabric.fabric_fakes import API_AUDIENCE, FABRIC_WS, TENANT, FakeFabricWorld
from tests.fabric.fabric_stack import FabricStack

BASE = "/v1/integrations/fabric"


def test_get_integration_reports_readiness_before_configuration(stack: FabricStack) -> None:
    r = stack.http.get(BASE, headers=stack.admin)
    assert r.status_code == 200, r.text
    doc = r.json()
    ids = {i["id"]: i for i in doc["readiness"]}
    assert ids["workspace_map"]["ok"] is False and ids["workspace_map"]["remediation"]
    assert ids["tenants"]["required"] is False
    assert doc["config"]["enforce_data_scope"] is True and doc["config"]["default_policy"]["allow_pip_install"] is False
    assert doc["counters"]["submissions_total"] == 0


def test_put_configuration_validates_references(stack: FabricStack) -> None:
    bad = stack.http.put(BASE, headers=stack.admin, json={"workspace_map": [{"fabric_workspace_id": FABRIC_WS, "olcf_workspace": "does-not-exist"}]})
    assert bad.status_code == 422 and "does not exist" in bad.text
    bad = stack.http.put(BASE, headers=stack.admin, json={"allowed_tenants": ["not-a-guid"]})
    assert bad.status_code == 422
    bad = stack.http.put(BASE, headers=stack.admin, json={"network": {"fabric_api": "http://evil.example.com"}})
    assert bad.status_code == 422 and "https" in bad.text
    bad = stack.http.put(BASE, headers=stack.admin, json={"network": {"proxy": "http://user:pw@proxy.corp:3128"}})
    assert bad.status_code == 422 and "credentials" in bad.text
    bad = stack.http.put(BASE, headers=stack.admin, json={"surprise": 1})
    assert bad.status_code == 422
    ws = stack.workspace()
    ok = stack.put_fabric(allowed_tenants=[TENANT], audiences=[API_AUDIENCE], workspace_map=[{"fabric_workspace_id": FABRIC_WS, "olcf_workspace": ws}])
    ids = {i["id"]: i for i in ok["readiness"]}
    assert ids["workspace_map"]["ok"] and ids["tenants"]["ok"] and ids["audiences"]["ok"]
    audit = stack.http.get("/v1/audit", headers=stack.admin, params={"action": "fabric.config.updated"}).json()
    assert any(e["action"] == "fabric.config.updated" for e in audit["items"])


def test_config_endpoints_need_the_fabric_permissions(stack: FabricStack) -> None:
    ws = stack.workspace()
    sp = stack.service_principal(ws)
    assert stack.http.get(BASE, headers=sp).status_code == 403
    assert stack.http.put(BASE, headers=sp, json={}).status_code == 403
    assert stack.http.post(f"{BASE}/test", headers=sp).status_code == 403
    assert stack.http.get(f"{BASE}/metrics", headers=sp).status_code == 403
    assert stack.http.get(BASE).status_code == 401


def test_analyze_pasted_and_uploaded_content(stack: FabricStack) -> None:
    nb = {"nbformat": 4, "nbformat_minor": 5, "metadata": {}, "cells": [{"cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [],
                                                                      "source": "import sempy.fabric as f\ndf = spark.range(3)\nnotebookutils.fs.ls('Files/')"}]}
    r = stack.http.post(f"{BASE}/analyze", headers=stack.admin, json={"content": json.dumps(nb), "format": "ipynb"})
    assert r.status_code == 200, r.text
    rep = r.json()
    assert rep["blocked"] and any(b["rule"].startswith("FAB-") for b in rep["blockers"])
    assert rep["analyzer_version"] and rep["ruleset_version"] and set(rep["counts"]) == {"native-portable", "compatibility-shim", "deterministic-rewrite", "warning", "fabric-only-fallback", "unsupported"}
    up = stack.http.post(f"{BASE}/analyze", headers=stack.admin, files={"file": ("nb.ipynb", json.dumps(nb).encode(), "application/json")})
    assert up.status_code == 200, up.text
    assert up.json()["blocked"]
    assert stack.http.post(f"{BASE}/analyze", headers=stack.admin, json={"content": "not a notebook {", "format": "ipynb"}).status_code in (400, 422)


def test_compat_rules_are_versioned(stack: FabricStack) -> None:
    doc = stack.http.get(f"{BASE}/compat/rules", headers=stack.admin).json()
    assert doc["ruleset_version"] and len(doc["rules"]) >= 60
    assert {r["class"] for r in doc["rules"]} >= {"unsupported", "compatibility-shim", "deterministic-rewrite", "warning"}


def test_snippet_is_ready_to_paste(stack: FabricStack) -> None:
    doc = stack.http.get(f"{BASE}/snippet", headers=stack.admin, params={"workspace": FABRIC_WS}).json()
    code = doc["snippet"]
    compile(code.replace("%pip", "#%pip"), "snippet", "exec")
    assert "olcf.run_remote()" in code and "olcf.configure(" in code and stack.url in code
    assert "token=" not in code or "paste" in code
    assert stack.http.get(f"{BASE}/snippet", headers=stack.admin, params={"auth": "client_credentials"}).json()["snippet"].count("client_id") >= 1


def test_diagnostics_against_the_fake_tenant(stack: FabricStack, world: FakeFabricWorld) -> None:
    ws = stack.workspace()
    secret = stack.http.post("/v1/secrets", headers=stack.admin, json={"name": "broker", "value": {"client_secret": world.client_secret}}).json()
    conn = stack.http.post("/v1/connections", headers=stack.admin, json={"name": "fabric-broker", "type": "onelake", "shared": True, "secret_id": secret["id"],
                                                                         "config": {"workspace": FABRIC_WS, "lakehouse": "x", "auth": "service_principal", "tenant_id": world.tenant,
                                                                                    "client_id": world.client_id}})
    assert conn.status_code == 201, conn.text
    stack.put_fabric(allowed_tenants=[world.tenant], audiences=[API_AUDIENCE], broker_connection=conn.json()["id"], network=world.network_config(),
                     workspace_map=[{"fabric_workspace_id": FABRIC_WS, "olcf_workspace": ws}])
    res = stack.http.post(f"{BASE}/test", headers=stack.admin)
    assert res.status_code == 200, res.text
    steps = {s["name"]: s for s in res.json()["steps"]}
    assert list(steps) == ["DNS", "TLS", "proxy", "Entra token", "Fabric API", "OneLake", "inbound path"]
    for name in ("Entra token", "Fabric API", "OneLake"):
        assert steps[name]["ok"], steps[name]
    assert all({"name", "ok", "detail", "ms", "remediation"} <= set(s) for s in steps.values())
    assert world.client_secret not in res.text
    # a missing tenant setting is diagnosed with a remediation
    world.fabric_status = 403
    try:
        steps = {s["name"]: s for s in stack.http.post(f"{BASE}/test", headers=stack.admin).json()["steps"]}
        assert not steps["Fabric API"]["ok"] and "Service principals can use Fabric APIs" in (steps["Fabric API"]["remediation"] or "")
    finally:
        world.fabric_status = 200
    health = stack.http.get("/v1/admin/health", headers=stack.admin).json()
    names = {c["name"] for c in (health.get("components") or health.get("items") or [])}
    assert {"fabric-adapter", "fabric-auth", "onelake"} <= names, names
