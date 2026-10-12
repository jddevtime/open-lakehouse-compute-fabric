# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S106 - a throw-away test password
"""``runtime.read`` is a baseline permission: any authenticated principal with a role binding at any scope may read the runtime catalog / provider list
(job and session forms need it); everything else keeps its normal scoping."""

from __future__ import annotations

from olcf.security.rbac import Binding, Principal


def test_baseline_permission_unit():
    ws_viewer = Principal(id="u", name="u", bindings=[Binding("Viewer", "workspace", "ws_1")])
    assert ws_viewer.can("runtime.read") and not ws_viewer.can("runtime.manage") and not ws_viewer.can("compute.read") and not ws_viewer.can("workspace.create")
    assert not Principal(id="n", name="n", bindings=[]).can("runtime.read")                         # no binding at all: no access
    assert not Principal(id="t", name="t", bindings=ws_viewer.bindings, token_scopes=["mcp"]).can("runtime.read")  # token scopes still narrow
    assert Principal(id="t", name="t", bindings=ws_viewer.bindings, token_scopes=["read"]).can("runtime.read")


def test_workspace_viewer_reads_runtimes_and_providers_but_not_operator_data(ic):
    ws = ic.workspace("rt")["id"]
    viewer = ic.user("vw", "Viewer", workspace_id=ws)
    for path in ("/v1/runtimes", "/v1/runtimes/matrix", "/v1/providers", "/v1/providers/types"):
        r = ic.get(path, headers=viewer)
        assert r.status_code == 200, (path, r.text)
    prov = ic.get("/v1/providers", headers=viewer).json()["items"]
    assert {p["name"] for p in prov} >= {"standalone", "local"}
    assert all("config" not in p and "secret_id" not in p for p in prov)                           # public view only
    admin_prov = ic.get("/v1/providers", headers=ic.admin).json()["items"]
    assert all("config" in p for p in admin_prov)
    # operator data and management stay closed
    assert ic.get("/v1/workers", headers=viewer).status_code == 403
    assert ic.get("/v1/clusters", headers=viewer).status_code == 403
    assert ic.post("/v1/providers", json={"name": "x", "type": "local"}, headers=viewer).status_code == 403
    # an authenticated user without any binding gets nothing
    ident = ic.ctx.service("identity")
    ident.create_principal("nobody", kind="user", password="correct horse battery staple")
    tok = ic.post("/v1/identity/login", json={"username": "nobody", "password": "correct horse battery staple"}).json()["access_token"]
    assert ic.get("/v1/runtimes", headers={"Authorization": f"Bearer {tok}"}).status_code == 403
