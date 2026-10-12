# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""Importing a saved Power BI admin scanner result: validation, credential refusal, per-workspace splitting, the same scan machinery as every other connector, and a round trip
through the emulated admin API."""

from __future__ import annotations

import copy
import json
from typing import Any

import pytest

from olcf.core.errors import BadRequest, ValidationFailed
from olcf.intelligence.discovery import scanner_import as si
from olcf.intelligence.model import DefinitionFact, Facts
from olcf.security.rbac import SYSTEM_PRINCIPAL
from tests.fabric.fabric_fakes import (
    FABRIC_WS,
    OTHER_FABRIC_WS,
    FakeFabricWorld,
    pbi_dataset,
    pbi_report,
    pbi_table,
    scanner_workspace,
)

DS1 = "d1d1d1d1-0000-4000-8000-000000000001"
REP1 = "e1e1e1e1-0000-4000-8000-000000000001"
SRC1 = "5a5a5a5a-0000-4000-8000-000000000001"
SRC2 = "5a5a5a5a-0000-4000-8000-000000000002"


def document(two: bool = True) -> dict[str, Any]:
    """A deterministic scanner result (the fake builders embed random user ids, so the result is built once per variant)."""
    if True not in _DOCS:
        _DOCS[True] = _build(True)
    doc = copy.deepcopy(_DOCS[True])
    if not two:                                                                                           # the same Sales workspace, so its hash is identical in both variants
        doc["workspaces"] = doc["workspaces"][:1]
    return doc


_DOCS: dict[bool, dict[str, Any]] = {}


def _build(two: bool) -> dict[str, Any]:
    ds = pbi_dataset(DS1, "Sales model", [pbi_table("sales", [("id", "Int64"), ("amount", "Double")], measures=[("Total", "SUM(sales[amount])")],
                                                     source_expression='let s = Sql.Database("srv", "db"), t = s{[Schema="dbo",Item="sales"]}[Data] in t')], datasource_ids=[SRC1])
    wss = [scanner_workspace(FABRIC_WS, "Sales", datasets=[ds], reports=[pbi_report(REP1, "Sales report", DS1)])]
    if two:
        wss.append(scanner_workspace(OTHER_FABRIC_WS, "Finance", datasets=[pbi_dataset("d2d2d2d2-0000-4000-8000-000000000002", "Ledger", [pbi_table("ledger", [("k", "Int64")])],
                                                                                      datasource_ids=[SRC2])]))
    return {"workspaces": wss, "datasourceInstances": [{"datasourceType": "Sql", "connectionDetails": {"server": "srv", "database": "db"}, "datasourceId": SRC1, "gatewayId": ""},
                                                       {"datasourceType": "Sql", "connectionDetails": {"server": "srv2", "database": "db2"}, "datasourceId": SRC2}],
            "misconfiguredDatasourceInstances": []}


class Recorder:
    version = "t1"

    def __init__(self) -> None:
        self.calls: list[DefinitionFact] = []

    def __call__(self, defn: DefinitionFact) -> Facts:
        self.calls.append(defn)
        return Facts()


@pytest.fixture()
def recorder(disc):
    r = Recorder()
    disc.engine.analyzer_loader = lambda: (r, r.version)
    return r


def post(disc: Any, doc: Any, who: dict[str, str] | None = None, **params: Any) -> Any:
    raw = doc if isinstance(doc, (bytes, str)) else json.dumps(doc)
    return disc.intel.c.post("/v1/intelligence/import/scanner", content=raw if isinstance(raw, bytes) else raw.encode(), params=params, headers=who or disc.admin)


# ------------------------------------------------------------------------------------------------ pure validation
def test_valid_documents_are_accepted_in_all_forms():
    doc = document()
    assert si.validate_scan_document(doc)["workspaces"][0]["id"] == FABRIC_WS
    assert si.validate_scan_document(json.dumps(doc))["workspaces"][1]["name"] == "Finance"
    assert si.validate_scan_document(json.dumps(doc).encode("utf-8-sig"))["datasourceInstances"]


@pytest.mark.parametrize("raw, message", [
    (b"not json", "not valid JSON"), (b"[1, 2]", "'workspaces' array"), (b'{"workspaces": []}', "no workspaces"), (b'{"workspaces": [{"name": "x"}]}', "no 'id'"),
    (b'{"workspaces": [1]}', "no 'id'"), (b'{"workspaces": [{"id": "a"}], "datasourceInstances": {}}', "must be an array"),
])
def test_invalid_documents_are_refused_with_a_message(raw, message):
    with pytest.raises((BadRequest, ValidationFailed), match=message):
        si.validate_scan_document(raw)


@pytest.mark.parametrize("leak", [
    'Server=srv;Database=db;User Id=u;Password=hunter2secret;', '"client_secret": "abcdef-very-secret"', "Authorization: Bearer abcdefghijklmnopqrstuvwxyz0123",
    ".".join(["eyJhbGciOiJSUzI1NiJ9", "eyJzdWIiOiIxMjM0NTY3ODkwIn0", "signaturepart"]),  # a JWT-shaped value assembled at run time: no token-shaped literal in the source
    "-----BEGIN RSA PRIVATE KEY-----",
])
def test_documents_that_look_like_they_contain_credentials_are_refused_without_echoing_them(leak):
    doc = document(two=False)
    doc["workspaces"][0]["description"] = leak
    with pytest.raises(ValidationFailed) as err:
        si.validate_scan_document(doc)
    assert "credential" in err.value.message and leak not in err.value.message and not any(leak in str(v) for v in (err.value.details or {}).values())


def test_size_and_workspace_limits(monkeypatch):
    monkeypatch.setattr(si, "MAX_IMPORT_BYTES", 200)
    with pytest.raises(BadRequest, match="limit is"):
        si.validate_scan_document(json.dumps(document()).encode())
    monkeypatch.setattr(si, "MAX_IMPORT_BYTES", 50 * 1024 * 1024)
    monkeypatch.setattr(si, "MAX_IMPORT_WORKSPACES", 1)
    with pytest.raises(ValidationFailed, match="limit is 1"):
        si.validate_scan_document(document())


def test_split_gives_each_workspace_only_the_data_sources_it_uses():
    parts = si.split_scan_result(document())
    assert [p["workspaces"][0]["name"] for p in parts] == ["Sales", "Finance"]
    assert [d["datasourceId"] for d in parts[0]["datasourceInstances"]] == [SRC1] and [d["datasourceId"] for d in parts[1]["datasourceInstances"]] == [SRC2]
    blob, sha = si.pack(document())
    assert si.unpack(blob) == json.loads(json.dumps(document())) and len(sha) == 64 and si.pack(document())[1] == sha


# ------------------------------------------------------------------------------------------------ import through the API
def test_import_creates_a_source_stores_the_document_and_scans_it_like_any_connector(disc, recorder):
    r = post(disc, document(), name="Tenant export", tenant_id="11111111-1111-4111-8111-111111111111", wait="true")
    assert r.status_code == 202, r.text
    out = r.json()
    assert out["workspaces"] == 2 and out["datasource_instances"] == 2 and len(out["sha256"]) == 64
    assert out["scan"]["status"] == "succeeded" and out["scan"]["mode"] == "full" and out["scan"]["trigger"] == "import"
    src = out["source"]
    assert src["kind"] == "scanner_import" and src["import"]["stored"] is True and src["import"]["sha256"] == out["sha256"] and "payload" not in json.dumps(src)
    assert {a["name"] for a in disc.assets(type="external", source_id=src["id"])} == {"Power BI scan of workspace Sales", "Power BI scan of workspace Finance"}
    assert sorted(c.kind for c in recorder.calls) == ["scanner_result", "scanner_result"]
    call = next(c for c in recorder.calls if c.payload["workspaces"][0]["name"] == "Sales")
    assert call.context["source"] == "fabric" and call.context["fabric_workspace"] == FABRIC_WS and call.context["tenant"] == "11111111-1111-4111-8111-111111111111"
    assert [d["datasourceId"] for d in call.payload["datasourceInstances"]] == [SRC1] and call.etag == next(c.etag for c in recorder.calls if c is call)


def test_reimporting_the_same_document_skips_analysis_and_a_smaller_one_removes_vanished_workspaces(disc, recorder):
    first = post(disc, document(), name="export", wait="true").json()
    assert len(recorder.calls) == 2
    again = post(disc, document(), name="export", wait="true").json()
    assert again["source"]["id"] == first["source"]["id"] and again["scan"]["stats"]["apply"]["analysis_skipped"] == 2 and len(recorder.calls) == 2
    smaller = post(disc, document(two=False), name="export", wait="true").json()
    assert smaller["scan"]["status"] == "succeeded" and smaller["scan"]["stats"]["sweep"]["assets_removed"] == 1
    assert disc.asset("Power BI scan of workspace Finance", "external", status="removed")
    assert disc.asset("Power BI scan of workspace Sales", "external")["status"] == "active"
    changed = document(two=False)
    changed["workspaces"][0]["reports"][0]["name"] = "Renamed"
    post(disc, changed, name="export", wait="true")
    assert len(recorder.calls) == 3                                                                      # only the changed workspace is analysed again


def test_import_without_scanning_and_scanning_later(disc, recorder):
    out = post(disc, document(two=False), name="later", scan="false").json()
    assert out["scan"] is None and recorder.calls == []
    row = disc.scan(out["source"]["id"], mode="incremental")                                               # first scan of any source is full
    assert row["mode"] == "full" and row["status"] == "succeeded" and len(recorder.calls) == 1
    inc = disc.scan(out["source"]["id"], mode="incremental")
    assert inc["mode"] == "incremental" and inc["stats"]["apply"]["analysis_skipped"] == 1


def test_import_errors_over_http(disc):
    r = post(disc, b"not json")
    assert r.status_code == 400 and "not valid JSON" in r.text
    leaky = document(two=False)
    leaky["workspaces"][0]["description"] = "Server=a;Password=hunter2secret;"
    r2 = post(disc, leaky)
    assert r2.status_code == 422 and "hunter2secret" not in r2.text and "credential" in r2.text
    assert disc.d.sources.list(kind="scanner_import") == []                                                # nothing was created by the refused import
    assert post(disc, document(), who=disc.intel.user("viewer-x", "Viewer")).status_code == 403
    local_clash = post(disc, document(two=False), name="local")
    assert local_clash.status_code == 409


def test_oversized_bodies_are_refused_before_they_are_parsed(disc, monkeypatch):
    monkeypatch.setattr(si, "MAX_IMPORT_BYTES", 300)
    r = post(disc, document())
    assert r.status_code == 400 and "larger than" in r.text


def test_the_import_source_has_a_two_step_connectivity_test(disc):
    src = disc.d.sources.create(SYSTEM_PRINCIPAL, name="empty", kind="scanner_import")
    out = disc.d.test_source(SYSTEM_PRINCIPAL, src["id"])
    assert out["ok"] is False and out["failed_step"] == "stored document" and out["steps"][1]["skipped"] is True and "POST" in out["steps"][0]["remediation"]
    post(disc, document(two=False), name="empty", scan="false")
    ok = disc.d.test_source(SYSTEM_PRINCIPAL, src["id"])
    assert ok["ok"] is True and "1 workspace" in ok["steps"][1]["detail"]
    with pytest.raises(Exception, match="no scanner result"):
        disc.d.runner.start(SYSTEM_PRINCIPAL, disc.d.sources.create(SYSTEM_PRINCIPAL, name="nothing", kind="scanner_import")["id"])


def test_deleting_an_import_source_retires_its_assets(disc, recorder):
    out = post(disc, document(), name="gone", wait="true").json()
    assert disc.assets(type="external", source_id=out["source"]["id"])
    d = disc.api("DELETE", f"/sources/{out['source']['id']}")
    assert d.status_code == 200 and d.json()["retired"]["assets_removed"] == 2
    assert all(a["status"] == "removed" for a in disc.assets(type="external", source_id=out["source"]["id"]))


def test_round_trip_through_the_emulated_admin_api(disc, recorder):
    """Fetch a scan result from the emulated Power BI admin API (as an administrator running the scan elsewhere would), save it, import it, and get the same
    definitions the Fabric connector produces for those workspaces."""
    from olcf.fabric.entra import TokenBroker
    from olcf.fabric.http import Network
    from olcf.fabric.rest import FabricRestClient
    from tests.fabric.fabric_fakes import BROKER_CLIENT_ID, BROKER_SECRET, TENANT

    world = FakeFabricWorld().start()
    try:
        doc = document()
        world.pbi_workspaces = {w["id"]: w for w in doc["workspaces"]}
        world.pbi_datasources = doc["datasourceInstances"]
        net = Network.from_config(world.discovery_network_config())
        client = FabricRestClient(TokenBroker(tenant_id=TENANT, client_id=BROKER_CLIENT_ID, secret={"client_secret": BROKER_SECRET}, network=net), net, sleep=lambda s: None)
        info = client.powerbi_scan_workspaces([FABRIC_WS, OTHER_FABRIC_WS])
        for _ in range(10):
            if client.powerbi_scan_status(info["id"]).get("status", "").lower() == "succeeded":
                break
        saved = json.dumps(client.powerbi_scan_result(info["id"]))
    finally:
        world.stop()
    out = post(disc, saved, name="saved-by-admin", wait="true").json()
    assert out["scan"]["status"] == "succeeded" and out["workspaces"] == 2
    expected = {d["workspaces"][0]["id"] for d in si.split_scan_result(json.loads(saved))}
    assert {c.context["fabric_workspace"] for c in recorder.calls} == expected == {FABRIC_WS, OTHER_FABRIC_WS}
