# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Foundation smoke tests: auth, workspaces, items, ETags, secrets hygiene, export/import."""

from __future__ import annotations


def test_login_workspace_item_flow(client):
    h = client.admin
    w = client.post("/v1/workspaces", json={"name": "demo"}, headers=h)
    assert w.status_code == 201
    i = client.post("/v1/workspaces/demo/items", json={"type": "notebook", "name": "nb1", "definition": {"cells": []}}, headers=h)
    assert i.status_code == 201
    iid = i.json()["id"]
    bad = client.put(f"/v1/items/{iid}", json={"definition": {"cells": [1]}}, headers={**h, "If-Match": "bogus"})
    assert bad.status_code == 412
    good = client.put(f"/v1/items/{iid}", json={"definition": {"cells": [1]}}, headers={**h, "If-Match": i.json()["etag"]})
    assert good.status_code == 200 and good.json()["version"] == 2


def test_unauthenticated_and_forbidden(client):
    assert client.get("/v1/workspaces").status_code == 401
    r = client.post("/v1/identity/users", json={"name": "bob", "password": "correct horse battery", "roles": [{"role": "Viewer"}]}, headers=client.admin)
    assert r.status_code == 201
    tok = client.post("/v1/identity/login", json={"username": "bob", "password": "correct horse battery"}).json()["access_token"]
    bh = {"Authorization": f"Bearer {tok}"}
    assert client.post("/v1/workspaces", json={"name": "nope"}, headers=bh).status_code == 403
    assert client.get("/v1/audit", headers=bh).status_code == 403


def test_secrets_never_leak(client):
    h = client.admin
    s = client.post("/v1/secrets", json={"name": "k", "value": "super-secret-value"}, headers=h).json()
    assert "super-secret-value" not in client.get("/v1/secrets", headers=h).text
    assert client.post("/v1/connections", json={"name": "x", "type": "s3", "config": {"secret_key": "abc"}}, headers=h).status_code == 422
    ok = client.post("/v1/connections", json={"name": "x", "type": "s3", "config": {"bucket": "b"}, "secret_id": s["id"]}, headers=h)
    assert ok.status_code == 201 and ok.json()["has_secret"] is True
    assert "super-secret-value" not in ok.text


def test_export_import_roundtrip(client):
    h = client.admin
    client.post("/v1/workspaces", json={"name": "src"}, headers=h)
    client.post("/v1/workspaces/src/items", json={"type": "notebook", "name": "n", "definition": {"a": 1}}, headers=h)
    z = client.get("/v1/workspaces/src/export", headers=h)
    assert z.status_code == 200
    imp = client.post("/v1/workspaces/import?new_name=dst", files={"file": ("x.zip", z.content)}, headers=h)
    assert imp.status_code == 201 and imp.json()["items_created"] == ["n"]


def test_audit_chain_verifies_and_detects_tamper(client):
    h = client.admin
    client.post("/v1/workspaces", json={"name": "a1"}, headers=h)
    assert client.get("/v1/audit/verify", headers=h).json()["ok"] is True
    from sqlalchemy import text

    with client.ctx.db.begin() as conn:
        conn.execute(text("UPDATE audit_log SET action='tampered' WHERE seq=(SELECT MIN(seq) FROM audit_log)"))
    assert client.get("/v1/audit/verify", headers=h).json()["ok"] is False
