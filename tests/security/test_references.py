# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Spec 35.4: insecure direct object references *inside request bodies*.

``test_authorization`` attacks every route with the other tenant's ids in the URL. This module puts tenant B's ids (items, lakehouses, connections, secrets, jobs,
pipelines) into the **bodies and definitions** that tenant A's users send, and into the places where one object points at another (job specs, triggers, schedules,
report tiles, lakehouse -> connection, connection -> secret, item versions). Nothing of B may be read, executed against, or attached.
"""

from __future__ import annotations

import pytest

from tests.security.secenv import CANARY_B, SECRET_B

CLIENT_ERRORS = (400, 403, 404, 409, 422)


@pytest.fixture(scope="module")
def env(two_tenants):
    return two_tenants


def a(env):
    return env.tenants["A"]


def b(env):
    return env.tenants["B"]


def submit(env, who, **spec):
    base = {"kind": "script", "workspace_id": a(env).ws, "name": "ref-attack", "engine": "python", "source": {"format": "py", "content": "print(1)", "filename": "x.py"}}
    return env.c.post("/v1/jobs", json={**base, **spec}, headers=env.headers(who))


@pytest.mark.parametrize("who", ["a_admin", "a_contrib"])
def test_job_cannot_reference_tenant_b_objects(env, who):
    B = b(env)
    attempts = {
        "secret_refs": {"secret_refs": {"STOLEN": B.ids["sid"]}},
        "item_id": {"item_id": B.ids["item_id"], "source": None},
        "default_lakehouse": {"default_lakehouse": {"lakehouse_id": B.ids["lh_id"]}},
        "data_refs_read": {"data_refs": [{"kind": "lakehouse", "ref": B.ids["lh_id"], "role": "read"}]},
        "data_refs_write": {"data_refs": [{"kind": "lakehouse", "ref": B.ids["lh_id"], "role": "write"}]},
        "artifact_uri": {"source": {"format": "py", "artifact_uri": "olcf://tenant-b/lh_b/Files/secret.txt", "filename": "x.py"}},
        "parent_run": {"parent_run_id": B.ids["job_id"]},
    }
    for label, spec in attempts.items():
        r = submit(env, who, **spec)
        assert r.status_code in CLIENT_ERRORS, (label, r.status_code, r.text[:300])
        assert CANARY_B not in r.text and SECRET_B not in r.text, label
    jobs = env.c.get("/v1/jobs", params={"workspace_id": a(env).ws}, headers=env.admin).json()["items"]
    assert not [j for j in jobs if j["name"] == "ref-attack" and j["state"] not in ("failed", "cancelled")], "no attack job may be accepted"


def test_job_from_foreign_workspace_id_is_refused(env):
    r = env.c.post("/v1/jobs", json={"kind": "script", "workspace_id": b(env).ws, "name": "x", "engine": "python", "source": {"format": "py", "content": "print(1)", "filename": "x.py"}},
                   headers=env.headers("a_admin"))
    assert r.status_code == 403


def test_connection_cannot_adopt_a_foreign_secret(env):
    """A connection stores a ``secret_id``; naming B's secret would let A's tenant use (and with a test/browse call exfiltrate) it."""
    A, B = a(env), b(env)
    for who in ("a_admin", "a_contrib"):
        r = env.c.post("/v1/connections", json={"workspace_id": A.ws, "name": f"adopt-{who}", "type": "s3", "config": {"bucket": "b"}, "secret_id": B.ids["sid"]}, headers=env.headers(who))
        assert r.status_code in CLIENT_ERRORS, (who, r.status_code, r.text[:300])
    r = env.c.patch(f"/v1/connections/{A.ids['cid']}", json={"secret_id": B.ids["sid"]}, headers=env.headers("a_admin"))
    assert r.status_code in CLIENT_ERRORS, (r.status_code, r.text[:300])
    cur = env.c.get(f"/v1/connections/{A.ids['cid']}", headers=env.admin).json()
    assert cur["secret_id"] == A.ids["sid"]


def test_secret_values_never_come_back_through_any_reference(env):
    B = b(env)
    for who in ("a_admin", "a_contrib", "a_viewer"):
        for path in (f"/v1/secrets/{B.ids['sid']}", f"/v1/secrets/{B.ids['sid']}/value", f"/v1/connections/{B.ids['cid']}"):
            r = env.c.get(path, headers=env.headers(who))
            assert r.status_code in CLIENT_ERRORS + (405,), (who, path, r.status_code)
            assert SECRET_B not in r.text


def test_lakehouse_cannot_use_foreign_connection(env):
    A, B = a(env), b(env)
    for who in ("a_admin", "a_contrib"):
        r = env.c.post(f"/v1/workspaces/{A.ws}/lakehouses", json={"name": f"fc_{who[2:]}", "connection_id": B.ids["cid"]}, headers=env.headers(who))
        assert r.status_code == 403, (who, r.status_code, r.text[:300])
    # an existing lakehouse cannot be re-pointed at it either
    r = env.c.put(f"/v1/items/{A.ids['lh_id']}", json={"definition": {"storage": {"connection_id": B.ids["cid"]}}}, headers=env.headers("a_admin"))
    assert r.status_code in CLIENT_ERRORS, (r.status_code, r.text[:300])


def test_triggers_and_schedules_cannot_target_foreign_items(env):
    A, B = a(env), b(env)
    h = env.headers("a_admin")
    for typ, ref in (("pipeline", B.ids["pipeline_id"]), ("notebook", B.ids["item_id"])):
        r = env.c.post("/v1/triggers", json={"workspace_id": A.ws, "name": f"t-{typ}", "type": "webhook", "target": {"type": typ, "ref": ref}}, headers=h)
        assert r.status_code in CLIENT_ERRORS, (typ, r.status_code, r.text[:300])
    r = env.c.post("/v1/triggers", json={"workspace_id": B.ws, "name": "t-direct", "type": "webhook", "target": {"type": "pipeline", "ref": B.ids["pipeline_id"]}}, headers=h)
    assert r.status_code == 403
    r = env.c.post("/v1/triggers", json={"workspace_id": A.ws, "name": "t-store", "type": "storage_event", "target": {"type": "notebook", "ref": A.ids["item_id"]},
                                         "config": {"lakehouse": B.ids["lh_id"], "path": "Files"}}, headers=h)
    assert r.status_code in CLIENT_ERRORS, (r.status_code, r.text[:300])
    r = env.c.post("/v1/triggers", json={"workspace_id": A.ws, "name": "t-dep", "type": "dependency", "target": {"type": "notebook", "ref": A.ids["item_id"]},
                                         "config": {"upstream": {"pipeline": B.ids["pipeline_id"]}}}, headers=h)
    assert r.status_code in CLIENT_ERRORS, (r.status_code, r.text[:300])
    r = env.c.post("/v1/schedules", json={"workspace_id": A.ws, "name": "s-foreign", "definition": {"cron": "0 * * * *", "timezone": "UTC", "target": {"type": "notebook", "ref": B.ids["item_id"]}}}, headers=h)
    if r.status_code in (200, 201):  # a schedule item may be stored, but it must never fire against B's notebook
        run = env.c.post(f"/v1/schedules/{r.json().get('id') or r.json().get('item_id')}/run-now", headers=h)
        assert run.status_code in CLIENT_ERRORS + (200, 202)
        assert CANARY_B not in run.text
    jobs_b = env.c.get("/v1/jobs", params={"workspace_id": B.ws}, headers=env.admin).json()["items"]
    assert all(j["id"] == B.ids["job_id"] for j in jobs_b), "no run may have been created in tenant B"


def test_report_tiles_cannot_query_foreign_lakehouses(env):
    A, B = a(env), b(env)
    h = env.headers("a_admin")
    tile = {"title": "t", "lakehouse_id": B.ids["lh_id"], "sql": "select 1 as x", "visual": "table"}
    r = env.c.post(f"/v1/workspaces/{A.ws}/items", json={"type": "report", "name": "xr", "definition": {"tiles": [tile]}}, headers=h)
    if r.status_code != 201:
        assert r.status_code in CLIENT_ERRORS
        return
    run = env.c.post(f"/v1/reports/{r.json()['id']}/run", json={}, headers=h)
    assert CANARY_B not in run.text
    if run.status_code == 200:
        assert run.json().get("errors", 0) >= 1, "the foreign tile must fail"


def test_sql_name_resolution_stays_in_the_callers_workspace(env):
    A, B = a(env), b(env)
    h = env.headers("a_contrib")
    for sql in ("SELECT * FROM lh_b.default.t", "SELECT * FROM tenant_b.lh_b.t", f"SELECT * FROM '{B.ids['lh_id']}'"):  # noqa: S608 - test-built statement from constants, no user input
        r = env.c.post("/v1/sql/execute", json={"sql": sql, "lakehouse": "lh_a", "workspace_id": A.ws}, headers=h)
        assert r.status_code in CLIENT_ERRORS + (200,), (sql, r.status_code)
        assert CANARY_B not in r.text
    # naming B's lakehouse id as the "default lakehouse" of a query in A is harmless for ``select 1`` but must never expose B's files or tables
    for sql in ("select * from read_csv('Files/secret.txt', header=false)", "select * from read_parquet('Tables/t/*.parquet')"):
        r = env.c.post("/v1/sql/execute", json={"sql": sql, "lakehouse": B.ids["lh_id"], "workspace_id": A.ws}, headers=h)
        assert r.status_code in CLIENT_ERRORS, (sql, r.status_code, r.text[:200])
        assert CANARY_B not in r.text
    r = env.c.post("/v1/sql/execute", json={"sql": "select 1", "lakehouse": "lh_b", "workspace_id": B.ws}, headers=h)
    assert r.status_code == 403


def test_item_create_with_foreign_ids_in_definitions_is_inert(env):
    """Definitions may hold arbitrary ids (they are data), but nothing may *resolve* them with the creator's authority on B: running them must fail."""
    A, B = a(env), b(env)
    h = env.headers("a_admin")
    pl = {"activities": [{"name": "run", "type": "notebook", "typeProperties": {"notebook": B.ids["item_id"]}}]}
    r = env.c.post(f"/v1/workspaces/{A.ws}/items", json={"type": "pipeline", "name": "pl-foreign", "definition": pl}, headers=h)
    if r.status_code != 201:
        assert r.status_code in CLIENT_ERRORS
        return
    run = env.c.post(f"/v1/pipelines/{r.json()['id']}/runs", json={"parameters": {}}, headers=h)
    assert run.status_code in CLIENT_ERRORS + (200, 201, 202), run.text[:300]
    body = run.text
    assert CANARY_B not in body
    jobs_b = env.c.get("/v1/jobs", params={"workspace_id": B.ws}, headers=env.admin).json()["items"]
    assert all(j["id"] == B.ids["job_id"] for j in jobs_b), "a pipeline of A must never start work in tenant B"


def test_item_versions_and_revert_stay_inside_the_item(env):
    A, B = a(env), b(env)
    h = env.headers("a_admin")
    r = env.c.get(f"/v1/items/{B.ids['item_id']}/versions", headers=h)
    assert r.status_code == 403
    r = env.c.post(f"/v1/items/{A.ids['item_id']}/versions/999/revert", headers=h)
    assert r.status_code in (404, 409, 422)


def test_moving_or_copying_an_item_into_a_foreign_workspace_is_refused(env):
    A, B = a(env), b(env)
    h = env.headers("a_admin")
    r = env.c.put(f"/v1/items/{A.ids['item_id']}", json={"workspace_id": B.ws, "name": "moved"}, headers=h)
    assert r.status_code in CLIENT_ERRORS + (200,)
    moved = env.c.get(f"/v1/items/{A.ids['item_id']}", headers=env.admin).json()
    assert moved["workspace_id"] == A.ws
    r = env.c.post(f"/v1/workspaces/{B.ws}/items", json={"type": "notebook", "name": "planted", "definition": {}}, headers=h)
    assert r.status_code == 403


def test_idempotency_keys_do_not_cross_tenants(env):
    """Replaying B's idempotency key as A must not return B's response."""
    A, B = a(env), b(env)
    body_b = {"kind": "script", "workspace_id": B.ws, "name": f"idem-{CANARY_B}", "engine": "python", "source": {"format": "py", "content": "print(1)", "filename": "x.py"},
              "idempotency_key": "shared-key-123"}
    first = env.c.post("/v1/jobs", json=body_b, headers=env.admin)
    assert first.status_code in (200, 201, 202), first.text[:200]
    body_a = {**body_b, "workspace_id": A.ws, "name": "idem-a"}
    r = env.c.post("/v1/jobs", json=body_a, headers=env.headers("a_admin"))
    assert CANARY_B not in r.text
    if r.status_code in (200, 201, 202):
        assert r.json()["workspace_id"] == A.ws


def test_deployment_pipeline_cannot_bridge_to_a_foreign_workspace(env):
    """Promotion copies items (and connections) between workspaces: it must need export on the source and import on the target, at creation and at deploy time."""
    A, B = a(env), b(env)
    h = env.headers("a_admin")
    for stages in ([{"stage": "dev", "workspace_id": A.ws}, {"stage": "prod", "workspace_id": B.ws}], [{"stage": "dev", "workspace_id": B.ws}, {"stage": "prod", "workspace_id": A.ws}]):
        r = env.c.post("/v1/deployment-pipelines", json={"name": "bridge", "stages": stages}, headers=h)
        assert r.status_code == 403, r.text[:300]
        if r.status_code == 201:
            d = env.c.post(f"/v1/deployment-pipelines/{r.json()['id']}/deploy", json={}, headers=h)
            assert d.status_code in CLIENT_ERRORS, (d.status_code, d.text[:300])
            assert CANARY_B not in d.text
    names = {i["name"] for i in env.c.get(f"/v1/workspaces/{A.ws}/items", headers=env.admin).json()["items"]}
    assert not [n for n in names if CANARY_B in n], "tenant B's items must not have been copied into A"
