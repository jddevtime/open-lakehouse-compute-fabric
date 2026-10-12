# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The discovery additions of the Fabric REST client against the emulation: paging, item definitions (long-running operations), the lakehouse tables API and its documented
limitation, the Power BI admin scanner calls, OneLake listing / reading, throttling and the give-up threshold for long ``Retry-After`` values.

Contract tests against ``FakeFabricWorld``."""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import pytest

from olcf.fabric.entra import TokenBroker
from olcf.fabric.http import Network
from olcf.fabric.rest import FabricApiError, FabricRestClient, IncompleteInventory
from tests.fabric.fabric_fakes import (
    BROKER_CLIENT_ID,
    BROKER_SECRET,
    FABRIC_WS,
    LAKEHOUSE,
    OTHER_FABRIC_WS,
    TENANT,
    FakeFabricWorld,
    pbi_dataset,
    pbi_table,
    scanner_workspace,
)

MODEL = "ffffffff-0000-4000-8000-0000000000f1"


@pytest.fixture()
def w() -> Iterator[FakeFabricWorld]:
    world = FakeFabricWorld().start()
    try:
        yield world
    finally:
        world.stop()


def client(world: FakeFabricWorld, **kw: Any) -> tuple[FabricRestClient, list[float]]:
    net = Network.from_config(world.discovery_network_config())
    sleeps: list[float] = []
    broker = TokenBroker(tenant_id=TENANT, client_id=BROKER_CLIENT_ID, secret=BROKER_SECRET, network=net)
    return FabricRestClient(broker, net, sleep=sleeps.append, **kw), sleeps


# ------------------------------------------------------------------------------------------------ workspaces and items
def test_iter_workspaces_follows_continuation_tokens_without_a_page_limit_of_20(w):
    w.workspaces = [{"id": f"{i:08x}-0000-4000-8000-000000000000", "displayName": f"ws{i}", "type": "Workspace"} for i in range(45)]
    w.workspaces_page_size = 2
    c, _ = client(w)
    got = list(c.iter_workspaces())
    assert [x["displayName"] for x in got] == [f"ws{i}" for i in range(45)]
    assert len([1 for _, p in w.calls if p == "/v1/workspaces"]) == 23


def test_iter_workspaces_has_a_runaway_guard_that_says_so(w):
    w.workspaces = [{"id": f"{i:08x}-0000-4000-8000-000000000000", "displayName": f"ws{i}", "type": "Workspace"} for i in range(10)]
    w.workspaces_page_size = 1
    c, _ = client(w)
    got: list[dict[str, Any]] = []
    with pytest.raises(IncompleteInventory) as stopped:
        for ws in c.iter_workspaces(max_pages=3):
            got.append(ws)
    assert len(got) == 3 and stopped.value.reason == "max_pages" and stopped.value.pages == 3        # what was received was delivered, then the listing says it is not the whole inventory


def test_list_items_pages_and_filters_by_type(w):
    for i in range(5):
        w.add_item(FABRIC_WS, f"{i:08x}-0000-4000-8000-0000000000aa", f"item{i}", "Lakehouse" if i % 2 == 0 else "Notebook")
    w.items_page_size = 2
    c, _ = client(w)
    assert [i["displayName"] for i in c.list_items(FABRIC_WS)] == [f"item{i}" for i in range(5)]
    assert [i["displayName"] for i in c.list_items(FABRIC_WS, item_type="Notebook")] == ["item1", "item3"]
    assert c.list_items(OTHER_FABRIC_WS) == []


def test_a_workspace_without_permission_is_a_403_with_remediation(w):
    w.workspace_denied.add(FABRIC_WS)
    c, _ = client(w)
    with pytest.raises(FabricApiError) as err:
        c.list_items(FABRIC_WS)
    assert err.value.status == 403 and "Service principals can use Fabric APIs" in (err.value.remediation or "")
    assert BROKER_SECRET not in str(err.value)


def test_a_denied_application_gets_403_from_every_discovery_endpoint(w):
    w.denied_apps.add(BROKER_CLIENT_ID)
    c, _ = client(w)
    for call in (lambda: c.list_items(FABRIC_WS), lambda: c.powerbi_modified_workspaces("2026-01-01T00:00:00Z"), lambda: c.list_lakehouse_tables(FABRIC_WS, LAKEHOUSE)):
        with pytest.raises(FabricApiError) as err:
            call()
        assert err.value.status == 403
    assert {a for a, *_ in w.app_calls} <= {BROKER_CLIENT_ID}


# ------------------------------------------------------------------------------------------------ definitions
def test_get_item_definition_runs_the_long_running_operation_and_decodes_text_parts(w):
    w.add_item(FABRIC_WS, MODEL, "m", "SemanticModel", parts={".platform": json.dumps({"metadata": {"type": "SemanticModel", "displayName": "m"}}), "definition/model.tmdl": "table t\n",
                                                                "StaticResources/logo.png": bytes([0x89, 0x50, 0x4E, 0x47, 0xFF, 0xFE])})
    c, sleeps = client(w)
    d = c.get_item_definition(FABRIC_WS, MODEL)
    assert d.parts["definition/model.tmdl"] == "table t\n" and d.platform == {"metadata": {"type": "SemanticModel", "displayName": "m"}} and d.binary_parts == 1
    assert "StaticResources/logo.png" not in d.parts
    assert len(sleeps) >= 3                                                                                   # Retry-After then two Running polls
    assert ("POST", f"/v1/workspaces/{FABRIC_WS}/items/{MODEL}/getDefinition") in w.calls and any(p.endswith("/result") for _, p in w.calls)


def test_get_item_definition_errors(w):
    w.add_item(FABRIC_WS, MODEL, "m", "SemanticModel", parts={"a": "x"})
    c, _ = client(w)
    w.item_denied.add((FABRIC_WS, MODEL))
    with pytest.raises(FabricApiError) as denied:
        c.get_item_definition(FABRIC_WS, MODEL)
    assert denied.value.status == 403
    w.item_denied.clear()
    w.item_definition_status[(FABRIC_WS, MODEL)] = 400
    with pytest.raises(FabricApiError) as unsupported:
        c.get_item_definition(FABRIC_WS, MODEL)
    assert unsupported.value.status == 400 and unsupported.value.error_code == "OperationNotSupportedForItem"
    w.item_definition_status.clear()
    with pytest.raises(FabricApiError) as missing:
        c.get_item_definition(FABRIC_WS, "00000000-0000-4000-8000-000000000000")
    assert missing.value.status == 404
    w.fail_operation = True
    with pytest.raises(FabricApiError, match="could not be produced"):
        c.get_item_definition(FABRIC_WS, MODEL)


def test_decode_item_definition_rejects_malformed_documents():
    with pytest.raises(FabricApiError, match="no definition parts"):
        FabricRestClient.decode_item_definition({})
    with pytest.raises(FabricApiError, match="payloadType"):
        FabricRestClient.decode_item_definition({"definition": {"parts": [{"path": "a", "payload": "eA==", "payloadType": "Link"}]}})
    ok = FabricRestClient.decode_item_definition({"definition": {"parts": [{"path": ".platform", "payload": "bm90IGpzb24=", "payloadType": "InlineBase64"}]}})
    assert ok.platform is None and ok.parts[".platform"] == "not json"


# ------------------------------------------------------------------------------------------------ lakehouse tables
def test_list_lakehouse_tables_uses_the_data_key_and_pages(w):
    for n in ("a", "b", "c"):
        w.add_lakehouse_table(FABRIC_WS, LAKEHOUSE, n, schema=[("id", "long")])
    w.tables_page_size = 2
    c, _ = client(w)
    got = c.list_lakehouse_tables(FABRIC_WS, LAKEHOUSE)
    assert [t["name"] for t in got] == ["a", "b", "c"] and got[0]["format"] == "delta" and got[0]["location"].startswith("abfss://")


def test_tables_api_limitations_surface_as_distinguishable_errors(w):
    w.add_lakehouse_table(FABRIC_WS, LAKEHOUSE, "a")
    c, _ = client(w)
    w.schema_enabled_lakehouses.add(LAKEHOUSE)
    with pytest.raises(FabricApiError) as schema:
        c.list_lakehouse_tables(FABRIC_WS, LAKEHOUSE)
    assert schema.value.status == 400 and schema.value.error_code == "UnsupportedOperationForSchemasEnabledLakehouse"
    w.schema_enabled_lakehouses.clear()
    w.tables_api_status = 501
    with pytest.raises(FabricApiError) as nf:
        c.list_lakehouse_tables(FABRIC_WS, LAKEHOUSE)
    assert nf.value.status == 501
    w.tables_api_status = 200
    with pytest.raises(FabricApiError) as missing:
        c.list_lakehouse_tables(FABRIC_WS, "00000000-0000-4000-8000-000000000000")
    assert missing.value.status == 404


# ------------------------------------------------------------------------------------------------ OneLake
def test_onelake_list_and_read(w):
    w.add_lakehouse_table(FABRIC_WS, LAKEHOUSE, "orders", schema=[("id", "long")], commits=3)
    w.add_lakehouse_table(FABRIC_WS, LAKEHOUSE, "other", schema=[("id", "long")])
    c, _ = client(w)
    top = c.onelake_list(FABRIC_WS, f"{LAKEHOUSE}/Tables")
    assert {(e["name"].rsplit("/", 1)[-1], e["isDirectory"]) for e in top} == {("orders", "true"), ("other", "true")}
    log = c.onelake_list(FABRIC_WS, f"{LAKEHOUSE}/Tables/orders/_delta_log")
    assert [e["name"].rsplit("/", 1)[-1] for e in log] == [f"{v:020d}.json" for v in range(3)] and all(e["isDirectory"] == "false" for e in log)
    deep = c.onelake_list(FABRIC_WS, f"{LAKEHOUSE}/Tables/orders", recursive=True)
    assert any(e["name"].endswith("00000000000000000002.json") for e in deep)
    body = c.onelake_read(FABRIC_WS, f"{LAKEHOUSE}/Tables/orders/_delta_log/{0:020d}.json")
    assert b"metaData" in body and b"schemaString" in body
    with pytest.raises(FabricApiError, match="larger than 10 bytes"):
        c.onelake_read(FABRIC_WS, f"{LAKEHOUSE}/Tables/orders/_delta_log/{0:020d}.json", max_bytes=10)
    with pytest.raises(FabricApiError) as nope:
        c.onelake_read(FABRIC_WS, f"{LAKEHOUSE}/Tables/orders/_delta_log/{9:020d}.json")
    assert nope.value.status == 404
    w.onelake_status = 403
    with pytest.raises(FabricApiError) as denied:
        c.onelake_read(FABRIC_WS, f"{LAKEHOUSE}/Tables/orders/_delta_log/{0:020d}.json")
    assert denied.value.status == 403


def test_onelake_listing_pages_through_x_ms_continuation(w):
    for i in range(7):
        w.onelake_files[f"{FABRIC_WS}/{LAKEHOUSE}/Files/f{i}.csv"] = b"x"
    w.onelake_page_size = 3
    c, _ = client(w)
    assert len(c.onelake_list(FABRIC_WS, f"{LAKEHOUSE}/Files")) == 7


# ------------------------------------------------------------------------------------------------ Power BI admin scanner
def scan_world(w: FakeFabricWorld) -> None:
    ds = pbi_dataset("d1d1d1d1-0000-4000-8000-000000000001", "m", [pbi_table("t", [("c", "Int64")])])
    w.pbi_workspaces = {FABRIC_WS: scanner_workspace(FABRIC_WS, "Sales", datasets=[ds]), OTHER_FABRIC_WS: scanner_workspace(OTHER_FABRIC_WS, "Finance")}


def test_modified_workspaces_filters_by_timestamp_and_sends_the_documented_query(w):
    w.pbi_modified = {FABRIC_WS: "2026-03-01T00:00:00Z", OTHER_FABRIC_WS: "2026-01-01T00:00:00Z"}
    c, _ = client(w)
    assert c.powerbi_modified_workspaces("2026-02-01T00:00:00Z") == [FABRIC_WS]
    assert sorted(c.powerbi_modified_workspaces()) == sorted([FABRIC_WS, OTHER_FABRIC_WS])
    assert w.pbi_modified_calls[0] == {"modifiedSince": ["2026-02-01T00:00:00Z"], "excludePersonalWorkspaces": ["True"]}
    c.powerbi_modified_workspaces("2026-02-01T00:00:00Z", exclude_personal=False)
    assert w.pbi_modified_calls[-1]["excludePersonalWorkspaces"] == ["False"]


def test_scan_start_status_and_result_round_trip(w):
    scan_world(w)
    c, _ = client(w)
    info = c.powerbi_scan_workspaces([FABRIC_WS, OTHER_FABRIC_WS])
    assert info["id"] and info["status"] == "NotStarted"
    assert w.pbi_scan_requests == [[FABRIC_WS, OTHER_FABRIC_WS]] and w.pbi_getinfo_queries[0]["datasetExpressions"] == ["True"]
    with pytest.raises(FabricApiError) as early:
        c.powerbi_scan_result(info["id"])
    assert early.value.status == 404                                                                          # not available before the scan succeeded
    states = [c.powerbi_scan_status(info["id"])["status"] for _ in range(3)]
    assert states[0] == "Running" and states[-1] == "Succeeded"
    result = c.powerbi_scan_result(info["id"])
    assert [x["name"] for x in result["workspaces"]] == ["Sales", "Finance"] and "datasourceInstances" in result


def test_scan_flags_are_sent_as_booleans_and_unscannable_workspaces_are_left_out(w):
    scan_world(w)
    w.pbi_unscannable.add(OTHER_FABRIC_WS)
    c, _ = client(w)
    info = c.powerbi_scan_workspaces([FABRIC_WS, OTHER_FABRIC_WS], lineage=False, artifact_users=False)
    assert w.pbi_getinfo_queries[0]["lineage"] == ["False"] and w.pbi_getinfo_queries[0]["getArtifactUsers"] == ["False"] and w.pbi_getinfo_queries[0]["datasourceDetails"] == ["True"]
    for _ in range(3):
        c.powerbi_scan_status(info["id"])
    assert [x["id"] for x in c.powerbi_scan_result(info["id"])["workspaces"]] == [FABRIC_WS]


def test_scan_batch_size_is_validated_locally_and_by_the_service(w):
    c, _ = client(w)
    with pytest.raises(FabricApiError, match="between 1 and 100"):
        c.powerbi_scan_workspaces([])
    with pytest.raises(FabricApiError, match="between 1 and 100"):
        c.powerbi_scan_workspaces([f"{i:08x}-0000-4000-8000-000000000000" for i in range(101)])
    assert w.pbi_scan_requests == []


def test_unknown_scan_ids_are_404(w):
    c, _ = client(w)
    for call in (c.powerbi_scan_status, c.powerbi_scan_result):
        with pytest.raises(FabricApiError) as err:
            call("deadbeef")
        assert err.value.status == 404


def test_admin_api_refusal_carries_the_status(w):
    w.pbi_status = 403
    c, _ = client(w)
    with pytest.raises(FabricApiError) as err:
        c.powerbi_modified_workspaces("2026-01-01T00:00:00Z")
    assert err.value.status == 403
    w.pbi_status = 401
    with pytest.raises(FabricApiError) as err2:
        c.powerbi_scan_workspaces([FABRIC_WS])
    assert err2.value.status == 401


def test_concurrency_limit_answers_429_when_too_many_scans_are_unfinished(w):
    scan_world(w)
    w.pbi_concurrent_limit = 2
    c, _ = client(w, max_attempts=1)
    c.powerbi_scan_workspaces([FABRIC_WS])
    c.powerbi_scan_workspaces([FABRIC_WS])
    with pytest.raises(FabricApiError) as err:
        c.powerbi_scan_workspaces([FABRIC_WS])
    assert err.value.status == 429 and w.pbi_peak_concurrent == 2


# ------------------------------------------------------------------------------------------------ throttling
def test_throttled_requests_sleep_for_retry_after_and_succeed(w):
    w.add_item(FABRIC_WS, MODEL, "m", "Lakehouse")
    w.throttle_paths = {"/items": 2}
    w.throttle_retry_after = "7"
    c, sleeps = client(w)
    assert [i["displayName"] for i in c.list_items(FABRIC_WS)] == ["m"]
    assert sleeps == [7.0, 7.0]


def test_a_long_retry_after_is_raised_instead_of_slept_when_a_threshold_is_set(w):
    w.pbi_hourly_quota = 0
    w.pbi_quota_retry_after = "3600"
    scan_world(w)
    c, sleeps = client(w)
    c.give_up_retry_after = 120.0
    with pytest.raises(FabricApiError) as err:
        c.powerbi_scan_workspaces([FABRIC_WS])
    assert err.value.status == 429 and err.value.retry_after == 3600.0 and sleeps == []
    short = client(w)[0]
    short.give_up_retry_after = 4000.0
    with pytest.raises(FabricApiError):
        short.powerbi_scan_workspaces([FABRIC_WS])                                                              # below the threshold it keeps retrying, then gives up after max_attempts


def test_the_default_behaviour_caps_the_wait_at_the_documented_maximum(w):
    w.pbi_hourly_quota = 0
    w.pbi_quota_retry_after = "3600"
    scan_world(w)
    c, sleeps = client(w, max_attempts=2)
    with pytest.raises(FabricApiError):
        c.powerbi_scan_workspaces([FABRIC_WS])
    assert sleeps and max(sleeps) <= 60.0


def test_tokens_use_the_right_audience_per_api(w):
    w.add_item(FABRIC_WS, MODEL, "m", "Lakehouse")
    w.add_lakehouse_table(FABRIC_WS, LAKEHOUSE, "a", schema=[("id", "long")])
    scan_world(w)
    w.pbi_modified = {FABRIC_WS: "2026-03-01T00:00:00Z"}
    c, _ = client(w)
    c.list_items(FABRIC_WS)
    c.powerbi_modified_workspaces("2026-01-01T00:00:00Z")
    c.onelake_list(FABRIC_WS, f"{LAKEHOUSE}/Tables")
    scopes = {f["scope"] for f in w.token_requests}
    assert scopes == {"https://api.fabric.microsoft.com/.default", "https://analysis.windows.net/powerbi/api/.default", "https://storage.azure.com/.default"}
