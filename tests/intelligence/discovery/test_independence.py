# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Discovery sources are independent of each other: nothing needs a Fabric tenant, and a scanner import works on an installation that has no workspace content at all."""

from __future__ import annotations

import json
from typing import Any

import pytest

from olcf.fabric.entra import TokenBroker
from olcf.intelligence.model import DefinitionFact, Facts
from olcf.security.rbac import SYSTEM_PRINCIPAL
from tests.intelligence.discovery.conftest import people_table
from tests.intelligence.discovery.test_scanner_import import document, post


class Recorder:
    version = "t1"

    def __init__(self) -> None:
        self.calls: list[DefinitionFact] = []

    def __call__(self, defn: DefinitionFact) -> Facts:
        self.calls.append(defn)
        return Facts()


TENANT_WORDS = ("tenant", "fabric", "power bi", "entra")


def test_without_a_fabric_source_the_local_source_scans_fully_and_everything_is_healthy(disc, monkeypatch):
    # any attempt to reach Entra / Fabric would be a defect: nothing is configured to
    def no_network(self: Any, *a: Any, **k: Any) -> Any:
        raise AssertionError("a Fabric token was requested although no Fabric source exists")

    monkeypatch.setattr(TokenBroker, "get_token", no_network)
    disc.lakehouse()
    disc.write("people", people_table())
    disc.notebook("etl", "df = spark.table('people')")
    row = disc.scan()
    assert row["status"] == "succeeded" and row["error"] is None
    assert disc.ctx.maybe_service("fabric") is None                                                       # the Fabric module is not even installed in this deployment
    sources = disc.api("GET", "/sources").json()["items"]
    assert [s["kind"] for s in sources] == ["local"]
    st = disc.d.status()
    assert st["kinds"] == ["local"] and "fabric" in st["connectors"] and "scanner_import" in st["connectors"]     # available to add, not required
    assert st["failed"] == [] and st["stale"] == []
    health = disc.d.health()
    assert "fabric" not in health.detail.lower() and "tenant" not in health.detail.lower()
    assert "last scan failed" not in health.detail and "no recent scan" not in health.detail            # (only the scheduler, deliberately not started in the test environment, is reported)
    for g in disc.gaps(open_only=False):
        text = json.dumps([g["subject"], g["detail"], g["remediation"]]).lower()
        assert not any(w in text for w in TENANT_WORDS), g
    assert not disc.assets(type="tenant")
    coverage = disc.api("GET", "/coverage").json()
    assert [c["kind"] for c in coverage["sources"]] == ["local"] and coverage["sources"][0]["open_gaps"]["total"] == 0
    summary = disc.intel.get("/v1/intelligence/summary", headers=disc.admin)
    assert summary.status_code == 200
    # adding a Fabric source later is just another source; the local one is unaffected
    body = {"name": "later", "kind": "fabric", "config": {"tenant_id": "11111111-1111-4111-8111-111111111111",
                                                          "metadata_identity": {"tenant_id": "11111111-1111-4111-8111-111111111111", "client_id": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"}},
            "credentials": {"client_secret": "Zq8~not-used-0123456789"}}
    assert disc.api("POST", "/sources", json=body).status_code == 201
    assert disc.scan()["status"] == "succeeded"


def test_an_installation_with_no_workspaces_scans_cleanly(disc):
    row = disc.scan()
    assert row["status"] == "succeeded" and disc.gaps() == []
    assert disc.d.health().labels["failed"] == []


def test_a_scanner_import_only_installation_works(disc):
    rec = Recorder()
    disc.engine.analyzer_loader = lambda: (rec, rec.version)
    # no workspace, no lakehouse, no notebook: the local source is switched off and only an imported scanner result exists
    assert disc.api("PATCH", "/sources/local", json={"enabled": False}).status_code == 200
    out = post(disc, document(), name="tenant export", wait="true").json()
    assert out["scan"]["status"] == "succeeded", out
    assert {a["name"] for a in disc.assets(type="external", source_id=out["source"]["id"])} == {"Power BI scan of workspace Sales", "Power BI scan of workspace Finance"}
    assert sorted(c.kind for c in rec.calls) == ["scanner_result", "scanner_result"]
    # the local source holds nothing and did not scan; the import source is a first-class source
    assert disc.assets(type="lakehouse") == [] and disc.assets(type="table") == []
    assert {s["kind"] for s in disc.api("GET", "/sources").json()["items"]} == {"local", "scanner_import"}
    st = disc.d.status()
    assert st["failed"] == [] and st["stale"] == []
    cov = {c["kind"]: c for c in disc.api("GET", "/coverage").json()["sources"]}
    assert cov["scanner_import"]["last_scan"]["status"] == "succeeded" and sum(cov["scanner_import"]["assets"]["by_type"].values()) >= 2
    # the source can say what it holds
    assert disc.d.test_source(SYSTEM_PRINCIPAL, out["source"]["id"])["ok"] is True


def test_the_import_source_works_next_to_a_local_installation_without_touching_it(disc):
    disc.lakehouse()
    disc.write("people", people_table())
    disc.scan()
    before = {a["id"] for a in disc.assets(status="active")}
    post(disc, document(two=False), name="export", wait="true")
    assert before <= {a["id"] for a in disc.assets(status="active")}
    again = disc.scan()
    assert again["status"] == "succeeded" and not [a for a in disc.assets(status="removed")]


@pytest.mark.parametrize("kind", ["local", "catalog_mirror"])
def test_kinds_that_need_no_tenant_are_created_without_credentials(disc, kind):
    r = disc.api("POST", "/sources", json={"name": f"x-{kind}", "kind": kind, "config": {}})
    assert r.status_code == 201 and r.json()["has_credentials"] is False
