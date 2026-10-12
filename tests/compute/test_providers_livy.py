# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Livy-based providers (``external`` with ``livy_url`` and ``fabric``) against a Livy/Entra test server, driven through the public APIs."""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Iterator
from typing import Any

import pytest

from olcf.compute.providers.livy import flatten_source
from olcf.core.errors import Unsupported, ValidationFailed
from tests.compute.fake_agent import wait_until
from tests.compute.fake_services import FakeLivy


@pytest.fixture()
def livy() -> Iterator[FakeLivy]:
    srv = FakeLivy()
    try:
        yield srv
    finally:
        srv.close()


def register(st: Any, ptype: str, config: dict[str, Any], *, secret: dict[str, Any] | None = None, name: str | None = None) -> str:
    name = name or f"p-{uuid.uuid4().hex[:8]}"
    body: dict[str, Any] = {"name": name, "type": ptype, "config": config}
    if secret is not None:
        r = st.http.post("/v1/secrets", headers=st.admin, json={"name": f"s-{name}", "value": secret})
        assert r.status_code == 201, r.text
        body["secret_id"] = r.json()["id"]
    r = st.http.post("/v1/providers", headers=st.admin, json=body)
    assert r.status_code == 201, r.text
    return name


def unregister(st: Any, name: str) -> None:
    st.http.delete(f"/v1/providers/{name}", headers=st.admin)


@pytest.fixture()
def ext(stack: Any, livy: FakeLivy) -> Iterator[str]:
    name = register(stack, "external", {"livy_url": livy.url})
    try:
        yield name
    finally:
        unregister(stack, name)


def spark_job(st: Any, ws: str, provider: str, **over: Any) -> dict[str, Any]:
    over.setdefault("engine", "spark")
    over.setdefault("policy", {"placement": {"provider": provider}})
    return st.submit(ws, **over)


# --------------------------------------------------------------------------------------------- flatten_source
def test_flatten_script_with_parameters_and_sql_and_notebook() -> None:
    spec = {"source": {"format": "py"}, "parameters": {"n": 5, "name": "x"}}
    text = flatten_source(spec, b"print(n)")
    assert "n = 5" in text and "name = 'x'" in text and text.rstrip().endswith("print(n)")
    # F09: an invalid parameter name is refused (as the notebook path does), never silently dropped
    with pytest.raises(ValidationFailed, match="bad-key"):
        flatten_source({"source": {"format": "py"}, "parameters": {"bad-key": 1}}, b"print(1)")
    sql = flatten_source({"source": {"format": "sql"}}, b"select 1; select 'a;b' ")
    assert sql.count("spark.sql(") == 2  # F10: the lexical splitter keeps the ';' inside the quoted literal
    nb = {"cells": [{"cell_type": "markdown", "source": "# t"}, {"cell_type": "code", "source": ["x = 1\n", "print(x)"]},
                    {"cell_type": "code", "source": "%%sql\nselect 1"}, {"cell_type": "code", "source": "%%pyspark\ny = 2"}]}
    out = flatten_source({"source": {"format": "ipynb"}}, json.dumps(nb).encode())
    assert "# --- cell 1 ---" in out and "x = 1\nprint(x)" in out and "spark.sql('select 1').show(truncate=False)" in out and "y = 2" in out and "# t" not in out


def test_flatten_rejects_unsupported_constructs() -> None:
    nb = {"cells": [{"cell_type": "code", "source": "%pip install foo"}]}
    with pytest.raises(Unsupported, match="magic"):
        flatten_source({"source": {"format": "ipynb"}}, json.dumps(nb).encode())
    with pytest.raises(Unsupported, match="valid JSON"):
        flatten_source({"source": {"format": "ipynb"}}, b"{not json")
    with pytest.raises(Unsupported, match="cannot be executed"):
        flatten_source({"source": {"format": "jar"}}, b"x")


# --------------------------------------------------------------------------------------------- external / livy
def test_external_livy_job_runs_end_to_end(stack: Any, livy: FakeLivy, ext: str) -> None:
    ws = stack.workspace()
    livy.output = "rows=42"
    j = spark_job(stack, ws, ext, source="print(n)", parameters={"n": 7})
    done = stack.wait_job(j["id"], timeout=60)
    assert done["state"] == "succeeded", done
    assert done["provider"] == ext and done["driver_worker_id"] is None
    assert done["application_id"] and done["application_id"].startswith("application_")
    # exactly one session, one statement, carrying the flattened source with the parameter header; the session is deleted afterwards
    creates = livy.calls("POST", r"/sessions$")
    assert len(creates) == 1 and creates[0][3]["kind"] == "pyspark" and creates[0][3]["name"] == f"olcf-{j['id']}-a1"
    stm = livy.calls("POST", r"/statements$")
    assert len(stm) == 1 and "n = 7" in stm[0][3]["code"] and "print(n)" in stm[0][3]["code"]
    wait_until(lambda: livy.calls("DELETE", r"/sessions/\d+$"), 10, message="session deletion")
    assert all(r[2].get("X-Requested-By") == "olcf" for r in livy.requests)
    # the Livy log is stored as the job log
    logs = stack.logs(j["id"])
    assert "livy: session booting" in logs and "driver: hello" in logs


def test_external_livy_statement_error_fails_the_job(stack: Any, livy: FakeLivy, ext: str) -> None:
    livy.outcome = "error"
    ws = stack.workspace()
    done = stack.wait_job(spark_job(stack, ws, ext)["id"], timeout=60)
    assert done["state"] == "failed"
    assert "ValueError" in done["failure"]["message"] and "boom" in done["failure"]["message"]
    wait_until(lambda: livy.calls("DELETE", r"/sessions/\d+$"), 10, message="session cleanup after failure")


def test_external_livy_cancel_deletes_the_remote_session(stack: Any, livy: FakeLivy, ext: str) -> None:
    livy.outcome = "hang"
    ws = stack.workspace()
    j = spark_job(stack, ws, ext)
    stack.wait_job(j["id"], "running", timeout=60)
    r = stack.http.post(f"/v1/jobs/{j['id']}/cancel", headers=stack.admin)
    assert r.status_code in (200, 202), r.text
    assert stack.wait_job(j["id"], timeout=60)["state"] == "cancelled"
    assert livy.calls("DELETE", r"/sessions/\d+$"), "cancel must delete the Livy session"
    assert not livy.sessions


def test_external_livy_survives_transient_503_on_reads(stack: Any, livy: FakeLivy, ext: str) -> None:
    livy.fail_requests, livy.fail_method = 2, "GET"  # the client retries GET/DELETE on 429/502/503/504 with bounded backoff
    ws = stack.workspace()
    assert stack.wait_job(spark_job(stack, ws, ext)["id"], timeout=60)["state"] == "succeeded"


def test_external_livy_503_on_the_create_post_is_never_replayed(stack: Any, livy: FakeLivy) -> None:
    """A gateway can answer 503 after the upstream committed the create (review C02): the session POST is sent once and left to reconciliation."""
    name = register(stack, "external", {"livy_url": livy.url, "submission_quiesce_seconds": 3600})
    try:
        livy.fail_requests, livy.fail_method = 1, "POST"
        ws = stack.workspace()
        j = spark_job(stack, ws, name)
        wait_until(lambda: livy.calls("POST", r"/sessions$"), timeout=30)
        time.sleep(3)  # several monitor ticks: a replay would show up as a second (successful) POST
        assert len(livy.calls("POST", r"/sessions$")) == 1
        assert stack.http.get(f"/v1/jobs/{j['id']}", headers=stack.admin).json()["state"] not in ("succeeded", "failed")
        stack.http.post(f"/v1/jobs/{j['id']}/cancel", headers=stack.admin)
    finally:
        unregister(stack, name)


def test_external_livy_batch_for_artifact_sources(stack: Any, livy: FakeLivy, ext: str) -> None:
    ws = stack.workspace()
    r = stack.http.post("/v1/jobs", headers=stack.admin, json={
        "kind": "spark_job", "workspace_id": ws, "engine": "spark", "name": "batch-job", "source": {"format": "jar", "artifact_uri": "abfss://c@acct.dfs.core.windows.net/app.jar", "main_class": "com.example.Main", "args": ["--n", "3"]},
        "policy": {"placement": {"provider": ext}}})
    assert r.status_code == 202, r.text
    done = stack.wait_job(r.json()["id"], timeout=60)
    assert done["state"] == "succeeded", done
    (b,) = livy.calls("POST", r"/batches$")
    assert b[3]["file"].endswith("app.jar") and b[3]["className"] == "com.example.Main" and b[3]["args"] == ["--n", "3"]
    assert not livy.calls("POST", r"/sessions$")


def test_external_livy_bearer_token_comes_from_the_secret_store(stack: Any) -> None:
    srv = FakeLivy(require_bearer="s3cret-bearer")
    name = register(stack, "external", {"livy_url": srv.url}, secret={"bearer_token": "s3cret-bearer"})
    try:
        ws = stack.workspace()
        assert stack.wait_job(spark_job(stack, ws, name)["id"], timeout=60)["state"] == "succeeded"
        assert all(r[2].get("Authorization") == "Bearer s3cret-bearer" for r in srv.requests)
        # the secret value never appears in the provider API output
        listing = stack.http.get(f"/v1/providers/{name}", headers=stack.admin).text
        assert "s3cret-bearer" not in listing
    finally:
        unregister(stack, name)
        srv.close()


def test_external_livy_discover_and_unreachable(stack: Any, livy: FakeLivy, ext: str) -> None:
    livy.add_foreign_session("someone-elses")
    d = stack.http.post(f"/v1/providers/{ext}/discover", headers=stack.admin).json()
    assert d["ok"] and "1 sessions" in d["detail"] and d["execution"] == "remote"
    row = stack.http.get(f"/v1/providers/{ext}", headers=stack.admin).json()
    assert row["last_discovery"]["ok"] is True
    dead = FakeLivy()
    url = dead.url
    dead.close()
    name = register(stack, "external", {"livy_url": url})
    try:
        d2 = stack.http.post(f"/v1/providers/{name}/discover", headers=stack.admin).json()
        assert d2["ok"] is False and d2["detail"]
    finally:
        unregister(stack, name)


def test_livy_reconcile_lists_only_olcf_applications(stack: Any, livy: FakeLivy, ext: str) -> None:
    livy.add_foreign_session("olcf-job_ABC123", "busy")
    livy.add_foreign_session("not-ours", "busy")
    prov = stack.compute.providers.get(ext)
    found = prov.reconcile()
    assert [(s.job_id, s.state) for s in found] == [("job_ABC123", "running")]
    assert found[0].native["kind"] == "session"


# --------------------------------------------------------------------------------------------- fabric
def fabric_config(livy: FakeLivy) -> dict[str, Any]:
    return {"workspace_id": "11111111-aaaa", "lakehouse_id": "22222222-bbbb", "tenant_id": "tenant-x", "client_id": "app-1", "api_base": livy.url, "token_url": livy.url + "/token"}


def test_fabric_job_uses_the_lakehouse_livy_path_and_entra_client_credentials(stack: Any) -> None:
    srv = FakeLivy(require_bearer="entra-jwt", token="entra-jwt")  # noqa: S106
    name = register(stack, "fabric", fabric_config(srv), secret={"client_secret": "shh"})
    try:
        ws = stack.workspace()
        for _ in range(2):
            assert stack.wait_job(spark_job(stack, ws, name)["id"], timeout=60)["state"] == "succeeded"
        paths = {r[1] for r in srv.requests if r[0] == "POST"}
        assert "/v1/workspaces/11111111-aaaa/lakehouses/22222222-bbbb/livyapi/versions/2023-12-01/sessions" in paths
        assert len(srv.token_requests) == 1, "the Entra token is cached across jobs"
        form = srv.token_requests[0]
        assert "grant_type=client_credentials" in form and "client_id=app-1" in form and "client_secret=shh" in form and "scope=" in form
        assert not any("shh" in json.dumps([r[1], r[3]], default=str) for r in srv.requests if r[1] != "/token")
        row = stack.http.get(f"/v1/providers/{name}", headers=stack.admin).json()
        assert row["type"] == "fabric" and row["live_verified"] is False and row["execution"] == "remote"
    finally:
        unregister(stack, name)
        srv.close()


def test_fabric_accepts_a_preissued_access_token(stack: Any) -> None:
    srv = FakeLivy(require_bearer="preissued")
    cfg = fabric_config(srv)
    cfg.pop("token_url")
    name = register(stack, "fabric", cfg, secret={"access_token": "preissued"})
    try:
        d = stack.http.post(f"/v1/providers/{name}/discover", headers=stack.admin).json()
        assert d["ok"], d
        assert not srv.token_requests
    finally:
        unregister(stack, name)
        srv.close()


def test_fabric_rejected_credentials_surface_as_provider_error(stack: Any) -> None:
    srv = FakeLivy(require_bearer="other")
    name = register(stack, "fabric", fabric_config(srv), secret={"client_secret": "shh"})
    try:
        d = stack.http.post(f"/v1/providers/{name}/discover", headers=stack.admin).json()
        assert d["ok"] is False and "401" in d["detail"]
    finally:
        unregister(stack, name)
        srv.close()


# --------------------------------------------------------------------------------------------- provider registration API
def test_provider_api_validation_and_lifecycle(stack: Any, livy: FakeLivy) -> None:
    h = stack.admin
    post = lambda body: stack.http.post("/v1/providers", headers=h, json=body)  # noqa: E731
    # secrets never go into the config; *_url keys are fine
    r = post({"name": "bad-cfg", "type": "external", "config": {"livy_url": livy.url, "password": "x"}})
    assert r.status_code == 400 and "secret" in r.text.lower()
    r = post({"name": "bad-nested", "type": "external", "config": {"livy_url": livy.url, "auth": {"api_key": "x"}}})
    assert r.status_code == 400
    assert post({"name": "x1", "type": "nope", "config": {}}).status_code == 400
    assert post({"name": "x2", "type": "external", "config": {}}).status_code == 400  # needs master_url/livy_url
    assert post({"name": "x3", "type": "external", "config": {"master_url": "yarn://rm:8032"}}).status_code in (400, 422, 501)
    assert post({"name": "x4", "type": "external", "config": {"livy_url": "ftp://x"}}).status_code == 400
    assert post({"name": "x5", "type": "fabric", "config": {"workspace_id": "w"}}).status_code == 400
    assert post({"name": "x6", "type": "kubernetes", "config": {}}).status_code == 400
    assert post({"name": "x7", "type": "external", "config": {"livy_url": livy.url}, "secret_id": "sec_missing"}).status_code == 404
    # create / duplicate / default exclusivity / disable / delete
    a = post({"name": "ext-a", "type": "external", "config": {"livy_url": livy.url}, "is_default": True})
    b = post({"name": "ext-b", "type": "external", "config": {"livy_url": livy.url}})
    assert a.status_code == 201 and b.status_code == 201 and a.json()["is_default"] is True
    assert post({"name": "ext-a", "type": "external", "config": {"livy_url": livy.url}}).status_code == 409
    assert stack.http.patch("/v1/providers/ext-b", headers=h, json={"is_default": True}).status_code == 200
    rows = {p["name"]: p for p in stack.http.get("/v1/providers", headers=h).json()["items"]}
    assert rows["ext-b"]["is_default"] and not rows["ext-a"]["is_default"]
    assert sum(1 for p in rows.values() if p["is_default"]) == 1
    # a restore of the original default keeps the invariant
    assert stack.http.patch("/v1/providers/standalone", headers=h, json={"is_default": True}).status_code == 200
    # an update that makes the config invalid is rejected and leaves the row alone
    assert stack.http.patch("/v1/providers/ext-a", headers=h, json={"config": {"livy_url": "nope"}}).status_code == 400
    assert stack.http.get("/v1/providers/ext-a", headers=h).json()["config"]["livy_url"] == livy.url
    # a disabled provider cannot receive jobs
    assert stack.http.patch("/v1/providers/ext-a", headers=h, json={"enabled": False}).status_code == 200
    ws = stack.workspace()
    r = stack.http.post("/v1/jobs", headers=h, json={"kind": "script", "workspace_id": ws, "engine": "spark", "source": {"format": "py", "content": "1"},
                                                     "policy": {"placement": {"provider": "ext-a"}}})
    assert r.status_code in (400, 404, 409, 422), r.text
    # built-ins cannot be deleted; custom ones can
    assert stack.http.delete("/v1/providers/standalone", headers=h).status_code == 409
    assert stack.http.delete("/v1/providers/ext-a", headers=h).status_code == 204
    assert stack.http.delete("/v1/providers/ext-b", headers=h).status_code == 204
    assert stack.http.get("/v1/providers/ext-a", headers=h).status_code == 404
    # RBAC: viewers may list providers (runtime.read is a baseline permission) but only see the public part - no configuration, secret reference or discovery
    # data; operators see everything but cannot change providers
    body = {"name": "nope", "type": "external", "config": {"livy_url": livy.url}}
    v = stack.user("Viewer")
    r = stack.http.get("/v1/providers", headers=v)
    assert r.status_code == 200 and r.json()["items"] and all("config" not in p and "secret_id" not in p for p in r.json()["items"])
    assert all("config" in p for p in stack.http.get("/v1/providers", headers=stack.admin).json()["items"])
    assert stack.http.get("/v1/workers", headers=v).status_code == 403
    assert stack.http.post("/v1/providers", headers=v, json=body).status_code == 403
    o = stack.user("Operator")
    assert stack.http.get("/v1/providers", headers=o).status_code == 200
    assert stack.http.post("/v1/providers", headers=o, json=body).status_code == 403


def test_provider_in_use_cannot_be_deleted(stack: Any, livy: FakeLivy, ext: str) -> None:
    livy.outcome = "hang"
    ws = stack.workspace()
    j = spark_job(stack, ws, ext)
    stack.wait_job(j["id"], "running", timeout=60)
    assert stack.http.delete(f"/v1/providers/{ext}", headers=stack.admin).status_code == 409
    stack.http.post(f"/v1/jobs/{j['id']}/cancel", headers=stack.admin)
    stack.wait_job(j["id"], timeout=60)


def test_provider_types_report_honest_verification_status(stack: Any) -> None:
    types = {t["type"]: t for t in stack.http.get("/v1/providers/types", headers=stack.admin).json()["items"]}
    assert {"standalone", "local", "external", "kubernetes", "fabric"} <= set(types)
    assert types["kubernetes"]["live_verified"] is False and types["fabric"]["live_verified"] is False
    assert types["standalone"]["live_verified"] is True and types["kubernetes"]["execution"] == "remote"


# --------------------------------------------------------------------------------------------- review F11 / F12 end to end
def test_f11_entry_point_comes_from_the_normalised_spec_not_from_job_parameters(stack: Any, livy: FakeLivy, ext: str) -> None:
    ws = stack.workspace()
    jar = "abfss://c@acct.dfs.core.windows.net/app.jar"
    # the legacy spelling (entry point smuggled through ``parameters``) is no longer honoured: the jar has no class, which is refused up front
    r = stack.http.post("/v1/jobs", headers=stack.admin, json={"kind": "spark_job", "workspace_id": ws, "engine": "spark", "name": "legacy", "source": {"format": "jar", "artifact_uri": jar},
                                                              "parameters": {"main_class": "com.example.Legacy"}, "policy": {"placement": {"provider": ext}}})
    if r.status_code == 202:
        assert stack.wait_job(r.json()["id"], timeout=60)["state"] == "failed"
    else:
        assert r.status_code in (400, 422), r.text
    assert not livy.calls("POST", r"/batches$")


def test_f12_user_spark_conf_reaches_the_livy_session_and_platform_keys_are_refused(stack: Any, livy: FakeLivy, ext: str) -> None:
    ws = stack.workspace()
    j = spark_job(stack, ws, ext, source="print(1)", runtime={"spark_conf": {"spark.sql.shuffle.partitions": "7"}})
    assert stack.wait_job(j["id"], timeout=60)["state"] == "succeeded"
    (create,) = livy.calls("POST", r"/sessions$")
    assert create[3].get("conf", {}).get("spark.sql.shuffle.partitions") == "7"
    assert not any(k in (create[3].get("conf") or {}) for k in ("spark.driver.host", "spark.driver.port", "spark.blockManager.port"))
    # platform-owned driver/network keys are refused at submit time, exactly as for the agent provider
    r = stack.http.post("/v1/jobs", headers=stack.admin, json={"kind": "spark_job", "workspace_id": ws, "engine": "spark", "name": "bad", "source": {"format": "py", "content": "1"},
                                                              "runtime": {"spark_conf": {"spark.driver.host": "evil"}}, "policy": {"placement": {"provider": ext}}})
    assert r.status_code == 403, r.text
    assert len(livy.calls("POST", r"/sessions$")) == 1


def test_f04_a_crash_after_the_create_request_is_recovered_by_name_and_never_replayed(stack: Any, livy: FakeLivy, ext: str) -> None:
    """The control plane dies after Livy accepted the session but before the handle was saved. Recovery must not queue the job again (a second session), it must
    find the one that exists by its attempt name and carry on."""
    prov = stack.compute.providers.get(ext)
    real_submit = prov.submit

    def crash_after_create(req: Any) -> Any:
        real_submit(req)
        raise SystemExit("simulated control-plane crash before the provider handle was saved")

    prov.submit = crash_after_create  # type: ignore[method-assign]
    ws = stack.workspace()
    try:
        j = spark_job(stack, ws, ext, source="print(1)")
        row = wait_until(lambda: (r := stack.compute.jobs.get_row(j["id"])) and (r["provider_handle"] or {}).get("intent_at") and r, 30, 0.1, "the intent to be recorded")
        wait_until(lambda: len(livy.calls("POST", r"/sessions$")) == 1, 10, 0.05, "Livy to receive the create request")
        row = stack.compute.jobs.get_row(j["id"])
        assert row["state"] == "provisioning" and not row["provider_handle"].get("sent")
    finally:
        prov.submit = real_submit  # type: ignore[method-assign]
    assert stack.http.put("/v1/admin/settings/compute.stuck_state_seconds", headers=stack.admin, json={"value": 1}).status_code == 200
    try:
        time.sleep(1.5)
        stack.compute.jobs.recover()
        done = stack.wait_job(j["id"], timeout=60)
        assert done["state"] == "succeeded", done
        assert len(livy.calls("POST", r"/sessions$")) == 1, "the session was found by name, not created a second time"
    finally:
        stack.http.put("/v1/admin/settings/compute.stuck_state_seconds", headers=stack.admin, json={"value": 300})
