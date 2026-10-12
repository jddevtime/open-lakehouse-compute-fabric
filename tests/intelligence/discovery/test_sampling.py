# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""Sampling: off by default, the policy matrix (scope precedence, 0..100 clamp, masking modes, classified-column exclusion), authorisation of policies and samples, the audit record
without values, and the Fabric reader injection point."""

from __future__ import annotations

import json
import logging
from typing import Any

import pyarrow as pa
import pytest

from olcf.intelligence import model as M
from olcf.intelligence.discovery import sampling as S
from tests.intelligence.discovery.conftest import people_table

SENSITIVE_VALUES = ["user0@example.com", "100-45-6789"]


@pytest.fixture()
def world(disc):
    disc.lakehouse()
    disc.write("people", people_table(6))
    big = pa.table({"id": pa.array(range(150), pa.int64()), "city": pa.array([f"city-{i}" for i in range(150)]), "amount": pa.array([float(i) for i in range(150)])})
    disc.write("big", big)
    disc.scan()
    disc.people = disc.asset("people", "table")
    disc.bigt = disc.asset("big", "table")
    disc.wsadmin = disc.intel.user("wa_sampler", "WorkspaceAdmin", workspace_id=disc.ws["id"])
    return disc


def sample(d: Any, asset: dict[str, Any], who: dict[str, str] | None = None, **params: Any) -> Any:
    return d.intel.c.get(f"/v1/intelligence/assets/{asset['id']}/sample", params=params, headers=who or d.admin)


def put(d: Any, who: dict[str, str] | None = None, **body: Any) -> Any:
    body.setdefault("scope_type", "global")
    return d.api("PUT", "/sampling-policies", who=who, json=body)


# ------------------------------------------------------------------------------------------------ pure masking
def test_mask_modes():
    assert S.mask_value("alice@example.com", "none") == "alice@example.com" and S.mask_value(None, "redact") is None
    assert S.mask_value("secret", "redact") == "***" and S.mask_value(12, "redact") == "***"
    h1, h2 = S.mask_value("alice", "hash", salt=b"s", column="c"), S.mask_value("alice", "hash", salt=b"s", column="c")
    assert h1 == h2 and h1.startswith("h:") and "alice" not in h1
    assert h1 != S.mask_value("alice", "hash", salt=b"other", column="c") and h1 != S.mask_value("alice", "hash", salt=b"s", column="d") and h1 != S.mask_value("bob", "hash", salt=b"s", column="c")
    assert S.mask_value("alice@example.com", "partial") == "a" + "*" * 15 + "m" and S.mask_value("abcd", "partial") == "****"
    assert S.mask_value(12345, "partial") == "1****" and S.mask_value(-98.5, "partial") == "-9***" and S.mask_value(True, "partial") == "***"
    import datetime as dt

    assert S.mask_value(dt.date(2024, 5, 17), "partial") == "2024-**-**" and S.mask_value(dt.datetime(1999, 1, 2, 3), "partial") == "1999-**-**"
    assert S.mask_value(b"abc", "partial") == "<3 bytes>"
    with pytest.raises(ValueError, match="unknown masking"):
        S.mask_value("x", "wibble")


def test_none_masking_clips_long_values_and_serialises_complex_ones():
    long = S.mask_value("x" * 1000, "none")
    assert len(long) == S.MAX_CELL_CHARS and long.endswith("…")
    import decimal

    assert json.loads(json.dumps(S.mask_value({"a": [1, 2]}, "none"))) is not None
    assert json.dumps(S.mask_value(decimal.Decimal("1.5"), "none")) in ("1.5", '"1.5"')


# ------------------------------------------------------------------------------------------------ off by default
def test_sampling_is_off_by_default_even_for_platform_admin(world):
    r = sample(world, world.people)
    assert r.status_code == 403 and r.json()["error"]["code"] == "policy_denied" and "disabled" in r.json()["error"]["message"]
    assert r.json()["error"]["details"]["reason"] == "sampling_disabled" and "sampling-policies" in r.json()["error"]["remediation"]
    pol = world.api("GET", f"/assets/{world.people['id']}/sampling-policy").json()
    assert pol["allowed"] is False and pol["max_rows"] == 0 and pol["scope"] == "none"
    assert world.api("GET", "/sampling-policies").json() == {"items": []}
    assert world.d.sampling.policies.any_enabled() is False
    denied = world.ctx.audit.query(action="intelligence.sample_read")
    assert denied and all(a["outcome"] == "denied" for a in denied)


def test_a_zero_row_or_disabled_policy_is_an_explicit_off(world):
    assert put(world, max_rows=0).status_code == 200
    r = sample(world, world.bigt)
    assert r.status_code == 403 and "allows 0 rows" in r.json()["error"]["message"]
    assert put(world, max_rows=10, enabled=False).status_code == 200
    r2 = sample(world, world.bigt)
    assert r2.status_code == 403 and "disabled" in r2.json()["error"]["message"]
    assert world.d.sampling.policies.any_enabled() is False


# ------------------------------------------------------------------------------------------------ the policy matrix
@pytest.mark.parametrize("max_rows, asked, expected", [(1, 100, 1), (7, 100, 7), (7, 3, 3), (100, 100, 100), (100, None, 10), (5, None, 5)])
def test_row_count_is_the_minimum_of_request_default_and_policy(world, max_rows, asked, expected):
    assert put(world, max_rows=max_rows, masking="none").status_code == 200
    r = sample(world, world.bigt, **({"rows": asked} if asked else {}))
    assert r.status_code == 200, r.text
    assert r.json()["row_count"] == expected == len(r.json()["rows"]) and r.json()["max_rows"] == max_rows


@pytest.mark.parametrize("bad", [-1, 101, 1000, True, "5", 2.5, None])
def test_policy_rows_outside_0_to_100_are_refused(world, bad):
    r = put(world, max_rows=bad)
    assert r.status_code == 422, r.text
    assert world.api("GET", "/sampling-policies").json()["items"] == []


def test_invalid_policy_bodies(world):
    assert put(world, max_rows=5, masking="shuffle").status_code == 422
    assert put(world, scope_type="workspace", scope_id=None, max_rows=5).status_code == 422
    assert put(world, scope_type="galaxy", max_rows=5).status_code == 422
    assert world.api("DELETE", "/sampling-policies/smp_missing").status_code == 404


def test_masking_modes_over_the_api(world):
    put(world, max_rows=3, masking="none", exclude_classified=False)
    plain = sample(world, world.people, columns="city", rows=3).json()
    assert [r["city"] for r in plain["rows"]] == ["Oslo", "Rome", None] and plain["masking"] == "none"
    put(world, max_rows=3, masking="redact")
    assert [r["city"] for r in sample(world, world.people, columns="city", rows=3).json()["rows"]] == ["***", "***", None]
    put(world, max_rows=3, masking="partial")
    assert [r["city"] for r in sample(world, world.people, columns="city", rows=3).json()["rows"]] == ["****", "****", None]
    put(world, max_rows=3, masking="hash")
    a = sample(world, world.people, columns="city", rows=3).json()["rows"]
    b = sample(world, world.people, columns="city", rows=3).json()["rows"]
    assert a == b and a[0]["city"].startswith("h:") and "Oslo" not in json.dumps(a) and a[2]["city"] is None


def test_hash_salt_is_persisted_so_hashes_are_stable_across_service_instances(world):
    put(world, max_rows=2, masking="hash")
    one = sample(world, world.people, columns="city", rows=2).json()["rows"]
    world.d.sampling._salt = None                                                                         # as after a restart: the salt is read back from the store
    assert sample(world, world.people, columns="city", rows=2).json()["rows"] == one


def test_scope_precedence_asset_over_source_over_workspace_over_global(world):
    ws, src = world.ws["id"], world.source("local")["id"]
    eff = lambda: world.d.sampling.policies.effective(workspace_id=ws, source_id=src, asset_id=world.bigt["id"])  # noqa: E731
    assert eff().scope == "none"
    put(world, max_rows=40, masking="none")
    assert (eff().scope, eff().max_rows) == ("global", 40)
    assert put(world, scope_type="workspace", scope_id=ws, max_rows=30, masking="redact").status_code == 200
    assert (eff().scope, eff().max_rows, eff().masking) == (f"workspace:{ws}", 30, "redact")
    assert put(world, scope_type="source", scope_id=src, max_rows=20).status_code == 200
    assert (eff().scope, eff().max_rows) == (f"source:{src}", 20)
    asset_policy = put(world, scope_type="asset", scope_id=world.bigt["id"], max_rows=0)
    assert asset_policy.status_code == 200 and eff().allowed is False and eff().scope == f"asset:{world.bigt['id']}"
    assert sample(world, world.bigt).status_code == 403                                                    # the asset row switches it off although everything wider allows it
    other = sample(world, world.people, columns="city")
    assert other.status_code == 200 and other.json()["policy_scope"] == f"source:{src}" and other.json()["max_rows"] == 20
    world.api("DELETE", f"/sampling-policies/{asset_policy.json()['id']}")
    assert eff().scope == f"source:{src}" and sample(world, world.bigt).status_code == 200
    for p in world.api("GET", "/sampling-policies").json()["items"]:
        world.api("DELETE", f"/sampling-policies/{p['id']}")
    assert eff().scope == "none" and sample(world, world.bigt).status_code == 403


def test_policy_put_replaces_the_row_of_the_same_scope(world):
    first = put(world, max_rows=5, masking="redact").json()
    second = put(world, max_rows=9, masking="none").json()
    assert first["id"] == second["id"] and second["max_rows"] == 9 and second["etag"] != first["etag"]
    assert len(world.api("GET", "/sampling-policies").json()["items"]) == 1
    assert [p["scope_type"] for p in world.api("GET", "/sampling-policies", params={"scope_type": "global"}).json()["items"]] == ["global"]
    assert world.api("GET", "/sampling-policies", params={"scope_type": "asset"}).json()["items"] == []
    assert world.api("GET", "/sampling-policies", params={"scope_type": "bogus"}).status_code == 422
    cfg = world.ctx.audit.query(action="config.changed")
    assert any(a["details"].get("scope") == "global" and a["details"].get("max_rows") == 9 for a in cfg)


# ------------------------------------------------------------------------------------------------ classified columns
def test_sensitive_columns_are_excluded_by_default(world):
    put(world, max_rows=3, masking="none")
    r = sample(world, world.people, rows=3)
    assert r.status_code == 200, r.text
    out = r.json()
    assert {c["name"] for c in out["columns"]} == {"id", "amount", "city"} and set(out["excluded_columns"]) == {"email", "ssn"}
    assert not any(v in r.text for v in SENSITIVE_VALUES) and all(set(row) == {"id", "amount", "city"} for row in out["rows"])
    asked = sample(world, world.people, columns="email")
    assert asked.status_code == 400 and asked.json()["error"]["details"]["reason"] == "all_columns_excluded"
    mixed = sample(world, world.people, columns=["email", "id"], rows=2).json()
    assert [c["name"] for c in mixed["columns"]] == ["id"] and "email" in mixed["excluded_columns"]


def test_turning_exclusion_off_shows_them_still_masked(world):
    put(world, max_rows=2, masking="partial", exclude_classified=False)
    out = sample(world, world.people, rows=2).json()
    assert "email" in {c["name"] for c in out["columns"]} and out["excluded_columns"] == []
    assert out["rows"][0]["email"] != "user0@example.com" and out["rows"][0]["email"].startswith("u*")
    assert out["rows"][0]["ssn"].startswith("1") and "100-45-6789" not in json.dumps(out)


def test_governance_classified_columns_are_excluded_through_the_provider(world):
    put(world, max_rows=3, masking="none")
    seen: list[str] = []
    world.d.sampling.add_classified_provider(lambda asset_id: (seen.append(asset_id), {"CITY"})[1])
    out = sample(world, world.people, rows=3).json()
    assert "city" not in {c["name"] for c in out["columns"]} and "city" in out["excluded_columns"] and world.people["id"] in seen


def test_a_failing_classification_lookup_blocks_the_sample_instead_of_weakening_it(world):
    put(world, max_rows=3, masking="none")

    def boom(_: str) -> set[str]:
        raise RuntimeError("governance db down")

    world.d.sampling.add_classified_provider(boom)
    r = sample(world, world.people)
    assert r.status_code == 422 and "classification lookup failed" in r.json()["error"]["message"] and "governance db down" not in r.text


def test_sampling_one_column_asset(world):
    put(world, max_rows=3, masking="none")
    col_city = next(a for a in world.assets(type="column", name="city") if a["parent_id"] == world.people["id"])
    ok = sample(world, col_city, rows=3)
    assert ok.status_code == 200 and [c["name"] for c in ok.json()["columns"]] == ["city"] and ok.json()["rows"][0] == {"city": "Oslo"}
    col_email = next(a for a in world.assets(type="column", name="email") if a["parent_id"] == world.people["id"])
    r = sample(world, col_email)
    assert r.status_code == 403 and r.json()["error"]["details"]["reason"] == "column_excluded"


def test_things_that_are_not_tables_cannot_be_sampled(world):
    put(world, max_rows=3)
    r = sample(world, world.asset("lh1", "lakehouse"))
    assert r.status_code == 400 and r.json()["error"]["details"]["reason"] == "not_sampleable"
    assert sample(world, {"id": "ast_doesnotexist"}).status_code == 404


# ------------------------------------------------------------------------------------------------ authorisation
def test_who_can_sample_and_who_can_set_policy(world):
    ws, src = world.ws["id"], world.source("local")["id"]
    viewer = world.intel.user("v_sampler", "Viewer", workspace_id=ws)
    outsider = world.intel.user("o_sampler", "WorkspaceAdmin", workspace_id=world.intel.workspace("beta")["id"])
    assert put(world, max_rows=3, masking="none").status_code == 200
    assert sample(world, world.bigt, who=world.wsadmin).status_code == 200                                 # intelligence.sample in the workspace
    assert sample(world, world.bigt, who=viewer).status_code == 403                                        # may read the asset, but sampling is its own permission
    assert sample(world, world.bigt, who=outsider).status_code == 404                                      # cannot even see it
    # policies: org-wide scopes need org-wide manage; a workspace administrator manages the policies of that workspace only
    assert put(world, who=world.wsadmin, max_rows=1).status_code == 403
    assert put(world, who=world.wsadmin, scope_type="source", scope_id=src, max_rows=1).status_code in (403, 404)
    assert put(world, who=world.wsadmin, scope_type="workspace", scope_id=ws, max_rows=2, masking="redact").status_code == 200
    assert put(world, who=world.wsadmin, scope_type="asset", scope_id=world.bigt["id"], max_rows=1).status_code == 200
    assert put(world, who=outsider, scope_type="workspace", scope_id=ws, max_rows=2).status_code in (403, 404)
    assert put(world, who=viewer, scope_type="workspace", scope_id=ws, max_rows=2).status_code == 403
    listed = world.api("GET", "/sampling-policies", who=world.wsadmin).json()["items"]
    assert {p["scope_type"] for p in listed} == {"workspace", "asset"}                                      # not the global row
    assert world.api("GET", "/sampling-policies", who=viewer).status_code == 403
    gid = next(p["id"] for p in world.api("GET", "/sampling-policies").json()["items"] if p["scope_type"] == "global")
    assert world.api("DELETE", f"/sampling-policies/{gid}", who=world.wsadmin).status_code == 403
    assert sample(world, world.bigt, who=world.wsadmin).json()["row_count"] == 1                           # the asset policy now rules


# ------------------------------------------------------------------------------------------------ audit and secrecy
def test_the_audit_record_holds_counts_and_names_but_never_values(world, caplog):
    caplog.set_level(logging.DEBUG)
    put(world, max_rows=3, masking="none", exclude_classified=False)
    r = sample(world, world.people, rows=3)
    assert r.status_code == 200
    values = [str(v) for row in r.json()["rows"] for v in row.values() if isinstance(v, str)] + SENSITIVE_VALUES
    records = [a for a in world.ctx.audit.query(action="intelligence.sample_read") if a["outcome"] == "success"]
    assert len(records) == 1                                                                                # recorded once, by the Intelligence service
    blob = json.dumps(records, default=str)
    assert not any(v in blob for v in values), blob
    assert records[0]["details"]["rows"] == 3 and records[0]["details"]["masking"] == "none"
    assert not any(v in caplog.text for v in ("user0@example.com", "100-45-6789", "Oslo"))
    # the service itself records (with names only) when it is called directly
    from olcf.security.rbac import SYSTEM_PRINCIPAL

    direct = world.d.sampling.sample(SYSTEM_PRINCIPAL, world.people["id"], 2)
    assert direct["row_count"] == 2 and "not stored" in direct["note"]
    d = [a for a in world.ctx.audit.query(action="intelligence.sample_read") if a["outcome"] == "success"][0]
    assert d["details"]["columns"] and "user0@example.com" not in json.dumps(d, default=str)


def test_samples_are_not_kept_anywhere(world):
    from sqlalchemy import select

    from olcf.intelligence.discovery.schema import discovery_sources, sampling_policies
    from olcf.intelligence.schema import assets, edges, gaps, scans

    put(world, max_rows=3, masking="none", exclude_classified=False)
    sample(world, world.people, rows=3)
    dump = ""
    with world.ctx.db.connect() as c:
        for t in (assets, edges, gaps, scans, discovery_sources, sampling_policies):
            dump += json.dumps([[str(v) for v in r] for r in c.execute(select(t))])
    assert "user0@example.com" not in dump and "100-45-6789" not in dump


# ------------------------------------------------------------------------------------------------ Fabric reader injection
def fabric_table(world: Any) -> dict[str, Any]:
    body = {"name": "fab", "kind": "fabric", "config": {"tenant_id": "11111111-1111-4111-8111-111111111111", "metadata_identity": {"tenant_id": "11111111-1111-4111-8111-111111111111",
            "client_id": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"}, "sampling_identity": {"tenant_id": "11111111-1111-4111-8111-111111111111", "client_id": "ffffffff-ffff-4fff-8fff-ffffffffffff"}},
            "credentials": {"client_secret": "Zq8~meta-secret-0123456789"}, "sampling_credentials": {"client_secret": "Qy7~sampling-secret-9876543210"}}
    src = world.api("POST", "/sources", json=body)
    assert src.status_code == 201, src.text
    sid = src.json()["id"]
    lh = M.AssetFact(M.AssetRef.make("lakehouse", {"fabric": "t/ws1/lh1"}), "fab-lh", workspace_id=world.ws["id"])
    tab = M.AssetFact(M.AssetRef.make("table", {"fabric": "t/ws1/lh1/orders"}, "orders"), "orders", parent=lh.ref, workspace_id=world.ws["id"],
                      attrs={"source_kind": "fabric", "location": "abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Tables/orders", "columns": [{"name": "id", "type": "bigint"}, {"name": "note", "type": "string"}]})
    scan_id = world.engine.begin_scan(sid, "full", "manual")
    stats = world.engine.apply_facts(M.Facts([lh, tab]), scan_id=scan_id, source_id=sid)
    world.engine.finish_scan(scan_id, "succeeded", stats.as_dict())
    return {"source": sid, "asset": world.asset("orders", "table")}


def test_fabric_tables_are_read_only_with_the_sampling_identity_through_the_injected_reader(world):
    fab = fabric_table(world)
    put(world, max_rows=5, masking="none")
    calls: list[tuple[Any, ...]] = []

    def reader(source, identity, attrs, rows, columns):
        calls.append((source["id"], identity["client_id"], identity.get("client_secret"), attrs["location"], rows, columns))
        return pa.table({"id": [1, 2, 3], "note": ["a", "b", "c"]}).slice(0, rows)

    world.d.sampling.fabric_reader = reader
    r = sample(world, fab["asset"], rows=2)
    assert r.status_code == 200, r.text
    assert r.json()["rows"] == [{"id": 1, "note": "a"}, {"id": 2, "note": "b"}] and calls[0][0] == fab["source"]
    assert calls[0][1] == "ffffffff-ffff-4fff-8fff-ffffffffffff" and calls[0][2] == "Qy7~sampling-secret-9876543210" and calls[0][4] == 2    # the sampling identity, not the metadata one
    assert "Qy7~sampling-secret" not in r.text and "Zq8~meta" not in json.dumps(world.ctx.audit.query(limit=500), default=str)


def test_fabric_read_failures_are_reported_without_leaking_details(world):
    fab = fabric_table(world)
    put(world, max_rows=5, masking="none")

    def broken(*_: Any) -> Any:
        raise RuntimeError("401 from OneLake for Qy7~sampling-secret-9876543210")

    world.d.sampling.fabric_reader = broken
    r = sample(world, fab["asset"])
    assert r.status_code == 422 and r.json()["error"]["details"]["reason"] == "read_failed" and "RuntimeError" in r.json()["error"]["message"]
    assert "Qy7~sampling-secret" not in r.text and "OneLake data access role" in r.json()["error"]["remediation"]


def test_fabric_sampling_needs_a_sampling_identity(world):
    fab = fabric_table(world)
    put(world, max_rows=5)
    assert world.api("PATCH", f"/sources/{fab['source']}", json={"config": {"sampling_identity": None}}).status_code == 200
    world.d.sampling.fabric_reader = lambda *a: pa.table({"id": [1]})
    r = sample(world, fab["asset"])
    assert r.status_code == 422 and r.json()["error"]["details"]["reason"] == "no_sampling_identity" and "metadata discovery never reads data" in r.json()["error"]["message"]


def test_the_default_fabric_reader_refuses_certificates_and_missing_locations():
    from olcf.core.errors import Unsupported

    with pytest.raises(Unsupported, match="client secret"):
        S.fabric_delta_reader({}, {"client_id": "c", "tenant_id": "t"}, {"location": "abfss://x"}, 1, [])
    with pytest.raises(Unsupported, match="no OneLake location"):
        S.fabric_delta_reader({}, {"client_id": "c", "tenant_id": "t", "client_secret": "s"}, {}, 1, [])


def test_tables_of_other_sources_have_no_reader(world):
    put(world, max_rows=5)
    f = M.Facts([M.AssetFact(M.AssetRef.make("table", {"fabric": "t/ws1/ext"}, "ext_t"), "ext_t", workspace_id=world.ws["id"], attrs={"source_kind": "catalog_mirror"})])
    world.intel.scan(f, source=world.source("local")["id"], mode="incremental")
    r = sample(world, world.asset("ext_t", "table"))
    assert r.status_code == 422 and r.json()["error"]["details"]["reason"] == "not_sampleable"
