# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""Discovery sources over REST: CRUD, write-only credentials, authorisation (read vs manage), visibility, scans / gaps / coverage routes."""

from __future__ import annotations

import json
from typing import Any

import pytest
from sqlalchemy import select

from olcf.intelligence.discovery.schema import discovery_sources
from olcf.security.rbac import SYSTEM_PRINCIPAL
from tests.intelligence.discovery.conftest import people_table

SECRET = "Zq8~super-secret-client-value-0123456789"
TENANT = "11111111-1111-4111-8111-111111111111"
CLIENT = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"


def fabric_body(name: str = "tenant-a", **extra: Any) -> dict[str, Any]:
    return {"name": name, "kind": "fabric", "config": {"tenant_id": TENANT, "metadata_identity": {"tenant_id": TENANT, "client_id": CLIENT}}, "credentials": {"client_secret": SECRET}, **extra}


def everything_stored(disc: Any) -> str:
    """The source rows and the audit log as text (to prove a secret is in neither)."""
    with disc.ctx.db.connect() as c:
        rows = json.dumps([{k: (v.hex() if isinstance(v, bytes) else v) for k, v in dict(r._mapping).items()} for r in c.execute(select(discovery_sources))], default=str)
    return rows + json.dumps(disc.ctx.audit.query(limit=500), default=str)


def test_create_fabric_source_stores_the_secret_in_the_secret_store_and_never_returns_it(disc):
    r = disc.api("POST", "/sources", json=fabric_body())
    assert r.status_code == 201, r.text
    body = r.json()
    assert SECRET not in r.text and body["has_credentials"] is True and body["kind"] == "fabric" and body["enabled"] is True
    assert "secret_ref" not in body and body["config"]["metadata_identity"] == {"tenant_id": TENANT, "client_id": CLIENT}
    row = disc.d.sources.get(body["id"])
    assert row["secret_ref"].startswith("sec_") and SECRET not in everything_stored(disc)
    assert disc.ctx.secrets.resolve(row["secret_ref"]) == {"client_secret": SECRET}
    for path in (f"/sources/{body['id']}", "/sources"):
        assert SECRET not in disc.api("GET", path).text
    assert disc.ctx.audit.query(action="intelligence.source_created")[0]["details"]["has_secret"] is True
    d = disc.api("DELETE", f"/sources/{body['id']}")
    assert d.status_code == 200 and d.json()["deleted"] is True
    with pytest.raises(Exception):  # noqa: B017, PT011 - the secret is gone with the source
        disc.ctx.secrets.resolve(row["secret_ref"])


def test_sampling_identity_is_separate_and_its_secret_is_owned_by_the_source(disc):
    body = fabric_body()
    body["config"]["sampling_identity"] = {"tenant_id": TENANT, "client_id": "ffffffff-ffff-4fff-8fff-ffffffffffff"}
    body["sampling_credentials"] = {"client_secret": "Qy7~sampling-secret-9876543210"}
    r = disc.api("POST", "/sources", json=body)
    assert r.status_code == 201, r.text
    out = r.json()
    assert out["has_sampling_identity"] is True and "Qy7" not in r.text and "sampling-secret" not in everything_stored(disc)
    row = disc.d.sources.get(out["id"])
    sid = row["config"]["sampling_identity"]["secret_id"]
    assert sid.startswith("sec_") and sid != row["secret_ref"] and row["config"]["sampling_identity"]["owned"] is True
    # replacing the sampling credentials rotates the owned secret
    r2 = disc.api("PATCH", f"/sources/{out['id']}", json={"sampling_credentials": {"client_secret": "Qy7~rotated-secret-1111111111"}})
    assert r2.status_code == 200
    new_sid = disc.d.sources.get(out["id"])["config"]["sampling_identity"]["secret_id"]
    assert new_sid != sid
    with pytest.raises(Exception):  # noqa: B017, PT011
        disc.ctx.secrets.resolve(sid)
    # removing the sampling identity deletes its owned secret and turns Fabric sampling off
    r3 = disc.api("PATCH", f"/sources/{out['id']}", json={"config": {"sampling_identity": None}})
    assert r3.status_code == 200 and r3.json()["has_sampling_identity"] is False
    with pytest.raises(Exception):  # noqa: B017, PT011
        disc.ctx.secrets.resolve(new_sid)


def test_validation_errors_are_field_level_and_say_how_to_fix_them(disc):
    assert disc.api("POST", "/sources", json={"name": "x", "kind": "nope"}).status_code == 400
    no_cred = fabric_body()
    no_cred.pop("credentials")
    r = disc.api("POST", "/sources", json=no_cred)
    assert r.status_code == 422 and "credentials" in r.text
    bad = fabric_body()
    bad["credentials"] = {"client_secret": SECRET, "private_key_pem": "x"}
    assert disc.api("POST", "/sources", json=bad).status_code == 422
    assert SECRET not in disc.api("POST", "/sources", json=bad).text
    assert disc.api("POST", "/sources", json={"name": "l", "kind": "local", "credentials": {"client_secret": "x"}}).status_code == 422
    assert disc.api("POST", "/sources", json={"name": "l", "kind": "local", "config": {"nope": 1}}).status_code == 422
    assert disc.api("POST", "/sources", json={"name": "l", "kind": "local", "schedule": {"incremental_seconds": 5}}).status_code == 422
    assert disc.api("POST", "/sources", json={"name": "l", "kind": "local", "surprise": 1}).status_code == 422
    assert disc.api("POST", "/sources", json=fabric_body("c", config={"tenant_id": TENANT, "metadata_identity": {"connection_id": "con_missing"}}, credentials=None)).status_code in (400, 422)
    ok = disc.api("POST", "/sources", json={"name": "dup", "kind": "local"})
    assert ok.status_code == 201
    assert disc.api("POST", "/sources", json={"name": "dup", "kind": "local"}).status_code == 409


def test_patch_merges_config_checks_if_match_and_keeps_the_kind(disc):
    r = disc.api("POST", "/sources", json={"name": "scoped", "kind": "local", "config": {"max_files_per_path": 10}})
    sid, etag = r.json()["id"], r.headers["ETag"]
    p = disc.api("PATCH", f"/sources/{sid}", json={"config": {"profile_tables": False}, "schedule": {"incremental_seconds": 120}}, who={**disc.admin, "If-Match": etag})
    assert p.status_code == 200 and p.json()["config"] == {"max_files_per_path": 10, "profile_tables": False} and p.json()["schedule"] == {"incremental_seconds": 120}
    assert p.json()["version"] == 2
    stale = disc.api("PATCH", f"/sources/{sid}", json={"enabled": False}, who={**disc.admin, "If-Match": etag})
    assert stale.status_code == 412
    assert disc.api("PATCH", f"/sources/{sid}", json={"kind": "fabric"}).status_code == 422
    assert disc.api("PATCH", f"/sources/{sid}", json={"config": {"max_files_per_path": None}}).json()["config"] == {"profile_tables": False}
    assert disc.api("PATCH", f"/sources/{sid}", json={"name": "local"}).status_code == 409                       # names are unique
    assert disc.ctx.audit.query(action="intelligence.source_updated")[0]["details"]["fields"]


def test_the_default_local_source_exists_cannot_be_deleted_and_install_is_idempotent(disc):
    src = disc.api("GET", "/sources/local").json()
    assert src["system"] is True and src["enabled"] and src["kind"] == "local"
    assert disc.api("DELETE", "/sources/local").status_code == 409
    again = disc.d.ensure_default_source()
    assert again["id"] == src["id"] and len([s for s in disc.d.sources.list() if s["system_key"]]) == 1
    assert disc.api("PATCH", "/sources/local", json={"enabled": False}).json()["enabled"] is False


def test_unsaved_configuration_can_be_tested_and_nothing_is_stored(disc):
    r = disc.api("POST", "/sources/test", json={"kind": "local", "config": {"workspace_ids": ["missing"]}})
    assert r.status_code == 200 and r.json()["ok"] is False and r.json()["failed_step"] == "workspaces"
    assert len(disc.d.sources.list()) == 1
    r2 = disc.api("POST", "/sources/test", json={"kind": "fabric", "config": fabric_body()["config"]})
    assert r2.json()["ok"] is False and r2.json()["failed_step"] == "resolve credentials" and "no credentials" in r2.json()["steps"][0]["detail"]
    assert len(disc.d.sources.list()) == 1 and SECRET not in everything_stored(disc)


def test_endpoint_overrides_need_the_fabric_integration_permission(disc):
    sec = disc.intel.user("sec1", "SecurityAdmin")                           # holds intelligence.manage but not integration.fabric.manage
    body = fabric_body("with-net", config={"tenant_id": TENANT, "metadata_identity": {"tenant_id": TENANT, "client_id": CLIENT}, "network": {"fabric_api": "https://api.fabric.example"}})
    denied = disc.api("POST", "/sources", who=sec, json=body)
    assert denied.status_code == 403 and "integration.fabric.manage" in denied.text
    assert disc.api("POST", "/sources", who=sec, json=fabric_body("plain")).status_code == 201
    assert disc.api("POST", "/sources", json=body).status_code == 201                                       # the platform administrator may
    http = fabric_body("plain-http", config={"tenant_id": TENANT, "metadata_identity": {"tenant_id": TENANT, "client_id": CLIENT}, "network": {"fabric_api": "http://evil.example"}})
    assert disc.api("POST", "/sources", json=http).status_code == 422                                       # plain http only for loopback
    proxy = fabric_body("proxy", config={"tenant_id": TENANT, "metadata_identity": {"tenant_id": TENANT, "client_id": CLIENT}, "network": {"proxy": "http://u:p@proxy:3128"}})
    assert disc.api("POST", "/sources", json=proxy).status_code == 422


def test_reading_and_managing_are_separate_permissions(disc):
    disc.lakehouse("alpha", "lh1")
    disc.write("people", people_table())
    disc.scan()
    other = disc.intel.workspace("beta")
    viewer_alpha = disc.intel.user("va", "Viewer", workspace_id=disc.ws["id"])
    viewer_beta = disc.intel.user("vb", "Viewer", workspace_id=other["id"])
    viewer_org = disc.intel.user("vo", "Viewer")
    nobody = disc.intel.user("nobody", None)
    wsadmin = disc.intel.user("wa", "WorkspaceAdmin", workspace_id=disc.ws["id"])
    # an organisation-wide reader sees everything but only a manager sees the configuration
    org = disc.api("GET", "/sources", who=viewer_org).json()["items"]
    assert [s["name"] for s in org] == ["local"] and org[0]["config"] is None and org[0]["schedule"] == {}
    mine = disc.api("GET", "/sources", who=viewer_alpha).json()["items"]
    assert [s["name"] for s in mine] == ["local"]                                                            # the source has assets in alpha
    assert disc.api("GET", "/sources", who=viewer_beta).json()["items"] == []                               # ... and none in beta
    assert disc.api("GET", "/sources/local", who=viewer_beta).status_code == 404                              # hidden, not forbidden
    assert disc.api("GET", "/sources", who=nobody).status_code == 403
    assert disc.api("GET", "/sources/local").json()["config"] == {}                                           # the manager sees the configuration
    for who in (viewer_org, viewer_alpha, wsadmin):                                                           # managing sources is organisation-wide
        assert disc.api("POST", "/sources", who=who, json={"name": "n", "kind": "local"}).status_code == 403
        assert disc.api("POST", "/sources/local/scan", who=who).status_code == 403
        assert disc.api("POST", "/sources/local/test", who=who).status_code == 403
        assert disc.api("PATCH", "/sources/local", who=who, json={"enabled": False}).status_code == 403
        assert disc.api("DELETE", "/sources/local", who=who).status_code == 403
        assert disc.api("POST", "/import/scanner", who=who, content=b"{}").status_code == 403
    # scans, gaps and coverage follow the same visibility
    sid = disc.d.sources.find("local")["id"]
    assert disc.api("GET", "/scans", who=viewer_alpha).json()["items"]
    assert disc.api("GET", "/scans", who=viewer_beta).json()["items"] == []
    scan_id = disc.api("GET", "/scans").json()["items"][0]["id"]
    assert disc.api("GET", f"/scans/{scan_id}", who=viewer_alpha).status_code == 200
    assert disc.api("GET", f"/scans/{scan_id}", who=viewer_beta).status_code == 404
    assert disc.api("POST", f"/scans/{scan_id}/cancel", who=viewer_org).status_code == 403
    cov = disc.api("GET", "/coverage", who=viewer_alpha).json()
    assert [c["source_id"] for c in cov["sources"]] == [sid] and cov["sources"][0]["assets"]["by_type"]["table"] == 1
    assert disc.api("GET", "/coverage", who=viewer_beta).json()["sources"] == []
    assert disc.api("GET", "/gaps", who=nobody).status_code == 403


def test_scans_routes_and_run_with_wait(disc):
    disc.lakehouse()
    disc.write("people", people_table())
    r = disc.api("POST", "/sources/local/scan", json={"mode": "full"}, params={"wait": "true"})
    assert r.status_code == 202 and r.json()["status"] == "succeeded", r.text
    sid = r.json()["id"]
    one = disc.api("GET", f"/scans/{sid}").json()
    assert one["gaps"] == {"total": 0, "by_kind": {}} and one["stats"]["facts"]["assets"] >= 8
    listing = disc.api("GET", "/scans", params={"source_id": one["source_id"], "status": "succeeded", "limit": 1}).json()
    assert listing["items"][0]["id"] == sid
    assert disc.api("GET", "/scans", params={"status": "bogus"}).status_code == 422
    assert disc.api("POST", "/sources/local/scan", json={"mode": "weekly"}).status_code == 422
    assert disc.api("POST", "/sources/nope/scan").status_code == 404
    snap = disc.intel.get("/v1/intelligence/snapshots", headers=disc.admin).json()["items"]
    assert any(s["id"] == sid for s in snap)                                                                  # the core's snapshot list sees discovery scans too


def test_gaps_route_lists_open_gaps_with_filters(disc):
    disc.lakehouse()
    disc.write("people", people_table())
    disc.d.sources.update(SYSTEM_PRINCIPAL, disc.source()["id"], {"config": {"max_tables_per_lakehouse": 1}})
    disc.write("zeta", people_table(2))
    row = disc.scan()
    assert row["status"] == "partial"
    items = disc.api("GET", "/gaps", params={"scan_id": row["id"], "kind": "not_inspected"}).json()["items"]
    assert items and items[0]["remediation"] and items[0]["source_id"] == disc.source()["id"] and items[0]["origin"]
    assert disc.api("GET", "/gaps", params={"kind": "permission_denied"}).json()["items"] == []
    assert disc.api("GET", "/gaps", params={"resolved": "true"}).json()["items"] == []
    assert len(disc.api("GET", "/gaps", params={"limit": 1}).json()["items"]) == 1


def test_coverage_reports_sources_gaps_and_sampling_state(disc):
    disc.lakehouse()
    disc.write("people", people_table())
    disc.scan()
    cov = disc.api("GET", "/coverage").json()
    assert cov["sampling"]["enabled"] is False and set(cov["connectors"]) >= {"local", "fabric", "scanner_import", "catalog_mirror"}
    local = cov["sources"][0]
    assert local["kind"] == "local" and local["assets"]["by_type"]["column"] == 5 and local["last_scan"]["status"] == "succeeded" and local["running"] is False
    assert local["next_due_at"] and local["open_gaps"]["total"] == 0


def test_health_check_reports_discovery(disc):
    h = disc.d.health()
    assert h.name == "intelligence-discovery" and h.status.value in ("ok", "degraded") and h.labels["sources"] == 1
    assert disc.d.status()["sampling"] == {"policies": 0, "any_enabled": False}
    assert "intelligence-discovery" in disc.ctx.health.names()
    assert disc.ctx.health.run("intelligence-discovery", force=True).name == "intelligence-discovery"


def test_retired_sources_leave_no_snapshot_in_the_default_list(disc):
    disc.lakehouse()
    disc.write("people", people_table())
    ext = disc.api("POST", "/sources", json={"name": "mirror", "kind": "catalog_mirror", "config": {}}).json()
    scan = disc.scan(ext["id"])
    assert disc.api("DELETE", f"/sources/{ext['id']}").status_code == 200
    base = "/v1/intelligence/snapshots"

    def ids(**params: Any) -> list[dict[str, Any]]:
        r = disc.intel.get(base, params=params, headers=disc.admin)
        assert r.status_code == 200, r.text
        return r.json()["items"]

    default = ids(source_id=ext["id"])
    assert [s["id"] for s in default] == [scan["id"]] and all(s["trigger"] != "retired" for s in default)       # the real scan stays, the retirement does not show
    full = ids(source_id=ext["id"], include_retired=True)
    assert len(full) == 2 and {s["trigger"] for s in full} == {"manual", "retired"}
    retired = next(s for s in full if s["trigger"] == "retired")
    assert retired["stats"]["retired_source"] is True and retired["status"] == "succeeded"
    assert retired["id"] not in {s["id"] for s in ids()} and retired["id"] in {s["id"] for s in ids(include_retired=True)}
    assert disc.intel.get(base, params={"include_retired": "maybe"}, headers=disc.admin).status_code == 422


def test_testing_an_unsaved_source_accepts_a_name(disc):
    body = {"name": "draft", "kind": "local", "config": {}}
    r = disc.api("POST", "/sources/test", json=body)
    assert r.status_code == 200, r.text
    assert r.json()["ok"] is True and r.json()["kind"] == "local"
    assert disc.api("POST", "/sources/test", json={**body, "bogus": 1}).status_code == 422                 # unknown fields are still refused
    assert [s["name"] for s in disc.api("GET", "/sources").json()["items"]] == ["local"]                    # nothing was saved
