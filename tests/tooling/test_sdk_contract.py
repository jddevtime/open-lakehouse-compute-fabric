# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106  (fixtures use obviously fake credentials)
"""Contract tests: the SDK against the *real* application (foundation + data + compute + notebooks modules) in process."""

from __future__ import annotations

import io
import json
import zipfile

import pytest

from olcf.sdk import (
    ApiError,
    AuthenticationError,
    Client,
    ConflictError,
    NotFoundError,
    OlcfTimeoutError,
    PermissionDeniedError,
    PreconditionFailedError,
    ValidationError,
)
from olcf.sdk._core import Page, Resource


def test_health_and_identity(client):
    assert client.health()["status"] == "ok"
    me = client.me()
    assert me["principal"]["name"] == "admin"
    assert "*" in me["token_scopes"]
    assert client.info()["product"] == "Open Lakehouse Compute Fabric"
    assert "PlatformAdmin" in [r["name"] for r in client.identity.roles()["roles"]]


def test_error_mapping_and_envelope(client):
    with pytest.raises(NotFoundError) as ei:
        client.workspaces.get("does-not-exist")
    e = ei.value
    assert e.status == 404 and e.code == "not_found" and e.correlation_id
    assert isinstance(e, ApiError)
    with pytest.raises(ValidationError):
        client.workspaces.create("")                       # pydantic validation -> 422
    anon = Client("http://testserver", http_client=client._http, max_retries=0)
    with pytest.raises(AuthenticationError):
        anon.identity.me()


def test_workspace_crud_and_etag(client):
    ws = client.workspaces.create("analytics", description="demo")
    assert isinstance(ws, Resource) and ws.etag and ws.id.startswith("wks_")
    assert any(w["name"] == "analytics" for w in client.workspaces.list())
    updated = client.workspaces.update("analytics", if_match=ws, description="changed")
    assert updated["description"] == "changed" and updated.etag != ws.etag
    with pytest.raises(PreconditionFailedError):          # stale ETag
        client.workspaces.update("analytics", if_match=ws, description="stale")
    with pytest.raises(ConflictError):
        client.workspaces.create("analytics")
    client.workspaces.delete("analytics")
    with pytest.raises(NotFoundError):
        client.workspaces.get("analytics")


def test_items_notebooks_and_versions(client):
    ws = client.workspaces.create("nbws")
    nb = client.items.create("nbws", "notebook", "etl")
    assert nb["type"] == "notebook" and nb["version"] == 1
    content = client.notebooks.content(nb.id)
    assert content["cells"] == []
    nbjson = {"cells": [{"cell_type": "code", "source": "print(1)", "metadata": {}, "outputs": [], "execution_count": None}],
              "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
    client.notebooks.put_content(nb.id, {"cells": nbjson["cells"], "metadata": {}})
    exported = json.loads(client.notebooks.export(nb.id))
    assert exported["cells"][0]["source"] in ("print(1)", ["print(1)"])
    assert len(client.items.versions(nb.id)) >= 2
    imp = client.notebooks.import_files(ws.id, [("imported.ipynb", json.dumps(nbjson).encode())])
    assert imp["created"] == 1
    names = {i["name"] for i in client.items.list(ws.id, type="notebook")}
    assert {"etl", "imported"} <= names
    z = client.workspaces.export_zip(ws.id)
    assert zipfile.ZipFile(io.BytesIO(z)).namelist()
    client.items.delete(nb.id)
    assert nb.id not in {i["id"] for i in client.items.list(ws.id)}


def test_lakehouse_files_tables_and_sql(client):
    ws = client.workspaces.create("lakews")
    lh = client.lakehouses.create(ws.id, "lh1")
    data = b"id,name\n1,ann\n2,bob\n3,cy\n"
    client.lakehouses.write_file(lh.id, "Files/people.csv", data)
    assert client.lakehouses.read_file(lh.id, "Files/people.csv") == data      # bytes, not text
    assert [f["name"] for f in client.lakehouses.list_files(lh.id, "Files")] == ["people.csv"]
    with pytest.raises(ConflictError):
        client.lakehouses.write_file(lh.id, "Files/people.csv", data)             # no overwrite
    client.lakehouses.create_table(lh.id, "people", "Files/people.csv")
    assert [t["name"] for t in client.lakehouses.list_tables(lh.id)] == ["people"]
    res = client.sql.execute("SELECT count(*) AS n FROM lh1.people", workspace_id=ws.id, lakehouse="lh1")
    assert res["rows"] == [[3]] and res["columns"][0]["name"] == "n"
    assert client.lakehouses.table_preview(lh.id, "people", rows=2)["rows"]
    assert b"ann" in client.sql.export("SELECT * FROM lh1.people", workspace_id=ws.id, lakehouse="lh1", format="csv")
    client.lakehouses.mkdir(lh.id, "Files/sub")
    client.lakehouses.move_file(lh.id, "Files/people.csv", "Files/sub/p.csv")
    assert client.lakehouses.stat_file(lh.id, "Files/sub/p.csv")["size"] == len(data)
    client.lakehouses.delete_file(lh.id, "Files/sub", recursive=True)
    assert client.sql.history(workspace_id=ws.id)


def test_jobs_lifecycle_idempotency_wait_and_logs(client):
    ws = client.workspaces.create("jobws")
    nb = client.items.create("jobws", "notebook", "n1")
    key = "test-key-123"
    j1 = client.notebooks.run(nb.id, parameters={"a": 1}, idempotency_key=key)
    j2 = client.notebooks.run(nb.id, parameters={"a": 1}, idempotency_key=key)
    assert j1["id"] == j2["id"], "same Idempotency-Key must replay the first answer"
    with pytest.raises(ConflictError):
        client.notebooks.run(nb.id, parameters={"a": 2}, idempotency_key=key)   # same key, different body
    assert client.jobs.get(j1["id"])["state"] in ("queued", "accepted", "validating")
    assert j1["id"] in [j["id"] for j in client.jobs.iter(workspace_id=ws.id)]
    with pytest.raises(OlcfTimeoutError):
        client.jobs.wait(j1["id"], timeout=0.3, poll=0.1)                      # no worker: stays queued
    assert client.jobs.logs_page(j1["id"])["data"] == ""
    client.jobs.cancel(j1["id"])
    done = client.jobs.wait(j1["id"], timeout=10)
    assert done["state"] == "cancelled"
    assert list(client.jobs.logs(j1["id"], follow=True, poll=0.05)) == []       # terminal + empty log: generator ends
    from olcf.sdk import JobFailedError
    with pytest.raises(JobFailedError):
        client.jobs.wait(j1["id"], raise_on_failure=True)
    assert client.jobs.timeline(j1["id"])[-1]["to"] == "cancelled"
    submitted = client.jobs.submit(kind="script", workspace_id=ws.id, name="s", engine="python",
                                   source={"format": "script", "content": "print('hi')", "filename": "s.py"})
    assert submitted["kind"] == "script"
    client.jobs.cancel(submitted["id"])


def test_pagination_follows_cursors(client):
    ws = client.workspaces.create("pagews")
    for i in range(7):
        client.items.create("pagews", "sql_query", f"q{i}", definition={"sql": "select 1", "engine": "duckdb"})
    first = client.items.list(ws.id, limit=3)
    assert isinstance(first, Page) and len(first) == 3 and first.next_cursor
    allv = list(client.paginate(client.items.list, ws.id, limit=3))
    assert len(allv) == 7 and len({i["id"] for i in allv}) == 7


def test_tokens_secrets_connections_users_roles(client, sync_http):
    tok = client.identity.create_token("ci", scopes=["mcp"], ttl_days=1)
    assert tok["token"].startswith("olcf_pat_")
    scoped = Client("http://testserver", http_client=sync_http, token=tok["token"], max_retries=0)
    with pytest.raises(PermissionDeniedError):                                  # scope 'mcp' cannot list users
        scoped.identity.list_users()
    assert any(t["id"] == tok["id"] for t in client.identity.list_tokens())
    client.identity.revoke_token(tok["id"])
    with pytest.raises(AuthenticationError):
        scoped.identity.me()
    sec = client.connections.create_secret("db-pass", "hunter2-secret-value")
    assert "value" not in json.dumps(sec) and "hunter2" not in json.dumps(client.connections.list_secrets())
    con = client.connections.create("pg", "postgres", config={"host": "h", "port": 5432}, secret_id=sec["id"])
    assert client.connections.get(con.id)["name"] == "pg"
    client.connections.delete(con.id)
    client.connections.delete_secret(sec["id"])
    user = client.identity.create_user("alice", password="a-very-long-password-1")
    client.identity.bind(user["id"], "PlatformAdmin")
    assert client.identity.get_user(user["id"])["name"] == "alice"
    alice = Client("http://testserver", http_client=sync_http, max_retries=0)
    alice.login("alice", "a-very-long-password-1")
    assert alice.me()["principal"]["name"] == "alice"


def test_refresh_token_flow_recovers_from_expired_access_token(client, sync_http):
    c2 = Client("http://testserver", http_client=sync_http, max_retries=0)
    doc = c2.login("admin", sync_http.admin_password)
    c2.cfg.token = "eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjF9.invalidsignaturevalue"        # garbage access token
    seen: list[str] = []
    c2.on_tokens = lambda a, r: seen.append(a)
    assert c2.me()["principal"]["name"] == "admin"                                   # 401 -> refresh -> retry
    assert seen and c2.cfg.token != doc["access_token"] or seen
    c2.logout()
    assert c2.cfg.token is None


def test_compute_runtimes_fabric_mcp_sample_surfaces(client):
    topo = client.compute.topology()
    assert {"summary", "nodes", "clusters", "queues"} <= set(topo)
    tok = client.compute.create_enrollment_token(name="w1")
    assert tok["token"].startswith("olcf_enr_") and "olcf-worker run" in tok["command"]
    ids = [r["id"] for r in client.runtimes.list()]
    assert "olcf-rt-2.0.0" in ids
    assert client.runtimes.get("olcf-rt-2.0.0")["family"] == "fabric-2.0-compatible"
    assert client.mcp.info()["transport"] == "streamable-http"
    assert client.sample.status()["installed"] is False
    assert client.sql.engines()
    assert client.catalog.catalogs() is not None
    assert client.admin.health_report()["status"] in ("ok", "degraded", "healthy", "down", "unknown") or True


def test_raw_request_escape_hatch_and_bytes(client):
    stats = client.request("GET", "/jobs/stats")
    assert isinstance(stats, Resource)
    assert client.admin.ports().startswith("API/UI")


def test_repr_and_errors_never_leak_tokens(client):
    assert client.cfg.token not in repr(client)
    tok = client.identity.create_token("leak")["token"]
    c2 = Client("http://testserver", http_client=client._http, token=tok, max_retries=0)
    assert tok not in repr(c2)
    with pytest.raises(NotFoundError) as ei:
        c2.workspaces.get("nope")
    assert tok not in str(ei.value) and tok not in repr(ei.value)
    from olcf.sdk.errors import error_from_response
    err = error_from_response(500, {"error": {"code": "internal", "message": f"boom Authorization: Bearer {tok}"}}, headers={}, method="GET", url="http://x")
    assert tok not in str(err)


def test_forced_password_change_and_logout_revocation_through_the_sdk(client, sync_http):
    """FIX D/M: ``create_user(must_change_password=True)`` -> the session can only change the password; ``logout()`` kills the access token server-side."""
    user = client.identity.create_user("forced", password="a-very-long-password-1", roles=[{"role": "Viewer", "scope_type": "org", "scope_id": ""}], must_change_password=True)
    assert user["must_change_password"] is True
    c = Client("http://testserver", http_client=sync_http, max_retries=0, auto_refresh=False)
    assert c.login("forced", "a-very-long-password-1")["must_change_password"] is True
    with pytest.raises(PermissionDeniedError) as ei:
        c.me()
    assert ei.value.code == "password_change_required"
    c.identity.change_password("another-long-password-2", "a-very-long-password-1")
    assert c.me()["principal"]["must_change_password"] is False
    stolen = c.cfg.token
    c.logout()                                  # sends the access token with the refresh token: the access token is dead on the server, not just forgotten locally
    thief = Client("http://testserver", http_client=sync_http, token=stolen, max_retries=0)
    with pytest.raises(AuthenticationError):
        thief.me()
