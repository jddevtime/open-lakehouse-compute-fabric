# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""The Fabric connector against ``FakeFabricWorld`` (a real HTTP server that reproduces the documented Fabric / OneLake / Power BI admin behaviour).

These are CONTRACT tests against the documented shapes of the APIs.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import pytest
from sqlalchemy import select

from olcf.fabric.entra import EntraTokenError, TokenBroker
from olcf.intelligence.discovery.schema import discovery_sources
from olcf.intelligence.model import DefinitionFact, Facts
from olcf.intelligence.schema import assets as assets_t
from olcf.intelligence.schema import gaps as gaps_t
from olcf.intelligence.schema import scans as scans_t
from olcf.security.rbac import SYSTEM_PRINCIPAL
from tests.fabric.fabric_fakes import (
    BROKER_CLIENT_ID,
    BROKER_SECRET,
    FABRIC_WS,
    LAKEHOUSE,
    NOTEBOOK,
    OTHER_FABRIC_WS,
    SAMPLING_CLIENT_ID,
    SAMPLING_SECRET,
    TENANT,
    FakeFabricWorld,
    allow_private_endpoints,
    pbi_dataset,
    pbi_report,
    pbi_table,
    scanner_workspace,
)

OTHER_LH = "eeeeeeee-0000-4000-8000-0000000000e1"
MODEL = "ffffffff-0000-4000-8000-0000000000f1"
DS1 = "d1d1d1d1-0000-4000-8000-000000000001"
NB_JSON = json.dumps({"nbformat": 4, "nbformat_minor": 5, "metadata": {"language_info": {"name": "python"}}, "cells": [{"cell_type": "code", "metadata": {}, "source": ["df = spark.read.table('orders')\n"],
                                                                                                                       "outputs": [], "execution_count": None}]})


class Recorder:
    version = "t1"

    def __init__(self) -> None:
        self.calls: list[DefinitionFact] = []

    def __call__(self, defn: DefinitionFact) -> Facts:
        self.calls.append(defn)
        return Facts()


class Fx:
    """A started emulated tenant, a registered fabric source and shortcuts."""

    def __init__(self, disc: Any, world: FakeFabricWorld) -> None:
        self.disc, self.world = disc, world
        self.sleeps: list[float] = []
        self.src: dict[str, Any] | None = None

    def create(self, name: str = "tenant", sampling: bool = False, **config: Any) -> dict[str, Any]:
        cfg = {"tenant_id": TENANT, "metadata_identity": {"tenant_id": TENANT, "client_id": BROKER_CLIENT_ID}, "network": self.world.discovery_network_config(), **config}
        body: dict[str, Any] = {"name": name, "kind": "fabric", "config": cfg, "credentials": {"client_secret": BROKER_SECRET}}
        if sampling:
            cfg["sampling_identity"] = {"tenant_id": TENANT, "client_id": SAMPLING_CLIENT_ID}
            body["sampling_credentials"] = {"client_secret": SAMPLING_SECRET}
        r = self.disc.api("POST", "/sources", json=body)
        assert r.status_code == 201, r.text
        self.src = r.json()
        return self.src

    def scan(self, mode: str = "full", **kw: Any) -> dict[str, Any]:
        assert self.src is not None
        return self.disc.scan(self.src["id"], mode, **kw)

    def gaps(self, kind: str | None = None) -> list[dict[str, Any]]:
        assert self.src is not None
        return [g for g in self.disc.gaps(self.src["id"]) if kind is None or g["kind"] == kind]

    def assets(self, type_: str) -> list[dict[str, Any]]:
        assert self.src is not None
        return self.disc.assets(type=type_, source_id=self.src["id"], status="active")

    def test(self) -> dict[str, Any]:
        assert self.src is not None
        r = self.disc.api("POST", f"/sources/{self.src['id']}/test")
        assert r.status_code == 200, r.text
        return r.json()

    def paths(self, fragment: str) -> list[str]:
        return [p for _, p in self.world.calls if fragment in p]


def populate(world: FakeFabricWorld) -> None:
    world.workspaces = [{"id": FABRIC_WS, "displayName": "Sales", "type": "Workspace"}, {"id": OTHER_FABRIC_WS, "displayName": "Finance", "type": "Workspace"}]
    world.add_item(FABRIC_WS, LAKEHOUSE, "lh_sales", "Lakehouse")
    world.add_item(FABRIC_WS, NOTEBOOK, "nb_load", "Notebook")
    world.add_item(FABRIC_WS, MODEL, "Sales model", "SemanticModel", parts={".platform": "{}", "definition/model.tmdl": "table sales\n\tcolumn id\n"})
    world.add_item(OTHER_FABRIC_WS, OTHER_LH, "lh_fin", "Lakehouse")
    world.add_item(FABRIC_WS, "99999999-0000-4000-8000-000000000001", "Some eventstream", "Eventstream")
    world.add_lakehouse_table(FABRIC_WS, LAKEHOUSE, "orders", schema=[("id", "long"), ("email", "string"), ("amount", "double")], commits=3)
    world.add_lakehouse_table(FABRIC_WS, LAKEHOUSE, "customers", schema=[("customer_id", "long"), ("first_name", "string")])
    world.add_lakehouse_table(OTHER_FABRIC_WS, OTHER_LH, "ledger", schema=[("k", "long"), ("salary", "double")])
    world.add_notebook(FABRIC_WS, NOTEBOOK, ipynb=NB_JSON)
    ds = pbi_dataset(DS1, "Sales model", [pbi_table("sales", [("id", "Int64")], measures=[("Total", "SUM(sales[id])")])])
    world.pbi_workspaces = {FABRIC_WS: scanner_workspace(FABRIC_WS, "Sales", datasets=[ds], reports=[pbi_report("e1e1e1e1-0000-4000-8000-000000000001", "Sales report", DS1)]),
                            OTHER_FABRIC_WS: scanner_workspace(OTHER_FABRIC_WS, "Finance")}
    world.pbi_modified = {FABRIC_WS: "2999-01-01T00:00:00Z", OTHER_FABRIC_WS: "2000-01-01T00:00:00Z"}
    world.throttle_retry_after = "0"


@pytest.fixture()
def fx(disc):
    world = FakeFabricWorld().start()
    populate(world)
    allow_private_endpoints(disc.ctx)                       # the fake tenant listens on 127.0.0.1
    world.extra_clients[SAMPLING_CLIENT_ID] = SAMPLING_SECRET
    rec = Recorder()
    disc.engine.analyzer_loader = lambda: (rec, rec.version)
    f = Fx(disc, world)
    f.rec = rec  # type: ignore[attr-defined]
    disc.d.runner.sleep = f.sleeps.append
    yield f
    disc.d.runner.sleep = None
    world.stop()


# ------------------------------------------------------------------------------------------------ the full scan
def test_full_scan_models_tenant_workspaces_items_tables_columns_definitions_and_scanner_results(fx):
    fx.create()
    row = fx.scan()
    assert row["status"] == "succeeded", (row["status"], row["error"], [g["detail"] for g in fx.gaps()])
    assert [a["name"] for a in fx.assets("tenant")] == [f"Microsoft Fabric tenant {TENANT}"]
    assert {a["name"] for a in fx.assets("workspace")} == {"Sales", "Finance"}
    assert {a["name"] for a in fx.assets("lakehouse")} == {"lh_sales", "lh_fin"}
    assert {a["name"] for a in fx.assets("notebook")} == {"nb_load"} and {a["name"] for a in fx.assets("semantic_model")} == {"Sales model"}
    tables = {a["name"]: a for a in fx.assets("table")}
    assert set(tables) == {"orders", "customers", "ledger"}
    assert [c["name"] for c in tables["orders"]["attrs"]["columns"]] == ["id", "email", "amount"] and tables["orders"]["attrs"]["delta_version"] == 2
    cols = {a["name"] for a in fx.assets("column") if fx.disc.asset_parent(a["id"]) == tables["orders"]["id"]}
    assert cols == {"id", "email", "amount"}
    assert tables["orders"]["attrs"]["sensitive_columns"] == ["email"] and tables["ledger"]["attrs"]["sensitive_columns"] == ["salary"]
    assert fx.disc.asset_parent(tables["orders"]["id"]) == next(a["id"] for a in fx.assets("lakehouse") if a["name"] == "lh_sales")
    assert {c.kind for c in fx.rec.calls} == {"notebook", "semantic_model", "scanner_result"} and sum(1 for c in fx.rec.calls if c.kind == "scanner_result") == 2
    nb = next(c for c in fx.rec.calls if c.kind == "notebook")
    assert nb.context["fabric_workspace"] == FABRIC_WS and nb.context["tenant"] == TENANT and nb.context["default_lakehouse"] == {"name": "lh_sales"} and nb.context["source"] == "fabric"
    assert "orders" in json.dumps(nb.payload)
    assert [g["kind"] for g in fx.gaps()] == [] or all(g["kind"] == "unsupported_construct" for g in fx.gaps())
    assert row["stats"]["counters"]["workspaces_scanned"] == 2 and row["stats"]["counters"]["tables"] == 3 and row["stats"]["counters"]["items_unmodelled"] == 1


def test_the_scanner_is_called_with_the_documented_query_and_at_most_100_workspaces(fx):
    fx.create()
    fx.scan()
    assert fx.world.pbi_scan_requests == [[FABRIC_WS, OTHER_FABRIC_WS]]
    q = fx.world.pbi_getinfo_queries[0]
    assert q == {"lineage": ["True"], "datasourceDetails": ["True"], "datasetSchema": ["True"], "datasetExpressions": ["True"], "getArtifactUsers": ["True"]}
    assert fx.world.pbi_peak_concurrent == 1
    fx.world.app_calls.clear()
    fx.scan()
    assert {a for a, *_ in fx.world.app_calls} == {BROKER_CLIENT_ID}


def test_scanner_batches_are_limited_to_the_configured_size_and_run_concurrently_up_to_the_limit(fx):
    extra = [f"{i:08x}-0000-4000-8000-000000000000" for i in range(1, 8)]
    fx.world.workspaces += [{"id": g, "displayName": f"ws{i}", "type": "Workspace"} for i, g in enumerate(extra)]
    fx.create()
    fx.disc.d.runner.limits = lambda: {"scanner_batch_size": 3}                                                  # type: ignore[method-assign]
    row = fx.scan()
    assert row["status"] in ("succeeded", "partial")
    assert [len(b) for b in fx.world.pbi_scan_requests] == [3, 3, 3] and sum(len(b) for b in fx.world.pbi_scan_requests) == 9
    assert fx.world.pbi_peak_concurrent <= 16


def test_a_second_full_scan_skips_unchanged_definitions_and_a_changed_one_is_analysed_again(fx):
    fx.create()
    fx.scan()
    first = len(fx.rec.calls)
    row2 = fx.scan()
    assert len(fx.rec.calls) == first and row2["stats"]["apply"]["analysis_skipped"] == first
    fx.world.add_notebook(FABRIC_WS, NOTEBOOK, ipynb=NB_JSON.replace("orders", "customers"))
    fx.scan()
    assert len(fx.rec.calls) == first + 1 and fx.rec.calls[-1].kind == "notebook"


def test_a_full_scan_removes_what_vanished_and_an_incomplete_one_does_not(fx):
    fx.create()
    fx.scan()
    assert {a["name"] for a in fx.assets("table")} == {"orders", "customers", "ledger"}
    fx.world.lakehouse_tables[(FABRIC_WS, LAKEHOUSE)] = [t for t in fx.world.lakehouse_tables[(FABRIC_WS, LAKEHOUSE)] if t["name"] != "customers"]
    row = fx.scan()
    assert row["status"] == "succeeded" and row["stats"]["sweep"]["assets_removed"] >= 2                         # the table and its columns
    assert "customers" not in {a["name"] for a in fx.assets("table")}
    removed = fx.disc.assets(type="table", name="customers", status="removed")
    assert len(removed) == 1
    # a workspace the principal can no longer read is a permission gap, the scan is partial, and nothing of it is swept
    fx.world.workspace_denied.add(OTHER_FABRIC_WS)
    part = fx.scan()
    assert part["status"] == "partial" and "sweep" not in part["stats"]
    assert "ledger" in {a["name"] for a in fx.assets("table")}
    assert {a["name"] for a in fx.assets("workspace")} == {"Sales", "Finance"}


def test_incremental_scan_revisits_only_modified_workspaces_and_never_sweeps(fx):
    fx.create()
    fx.scan()
    fx.world.calls.clear()
    fx.world.pbi_modified_calls.clear()
    fx.world.pbi_scan_requests.clear()
    row = fx.scan("incremental")
    assert row["mode"] == "incremental" and row["status"] == "succeeded" and "sweep" not in row["stats"]
    assert fx.paths(f"/workspaces/{OTHER_FABRIC_WS}/") == [] and fx.paths(f"/workspaces/{FABRIC_WS}/items")
    assert fx.world.pbi_scan_requests == [[FABRIC_WS]] and len(fx.world.pbi_modified_calls) == 1 and "modifiedSince" in fx.world.pbi_modified_calls[0]
    assert "ledger" in {a["name"] for a in fx.assets("table")}                                                   # untouched, not swept
    # a workspace that appears after the last scan is picked up although it is not 'modified'
    fx.world.workspaces.append({"id": "77777777-0000-4000-8000-000000000007", "displayName": "New", "type": "Workspace"})
    fx.world.pbi_modified.pop(FABRIC_WS)
    fx.scan("incremental")
    assert "New" in {a["name"] for a in fx.assets("workspace")}


def test_incremental_scan_reads_everything_when_the_modified_api_is_not_allowed(fx):
    fx.create()
    fx.scan()
    fx.world.pbi_status = 403
    row = fx.scan("incremental")
    kinds = {g["kind"] for g in fx.gaps()}
    assert row["status"] == "partial" and "permission_denied" in kinds
    assert any("tenant settings" in (g["remediation"] or "").lower() or "Tenant settings" in (g["remediation"] or "") for g in fx.gaps("permission_denied"))
    assert "ledger" in {a["name"] for a in fx.assets("table")}                                                     # item-level metadata did not need the admin API


# ------------------------------------------------------------------------------------------------ permission gaps
def test_a_workspace_without_access_is_a_permission_gap_and_the_rest_is_still_discovered(fx):
    fx.world.workspace_denied.add(OTHER_FABRIC_WS)
    fx.create()
    row = fx.scan()
    assert row["status"] == "partial"
    g = fx.gaps("permission_denied")
    assert any("Finance" in x["subject"] for x in g) and all(x["remediation"] for x in g)
    assert {a["name"] for a in fx.assets("table")} == {"orders", "customers"} and "Finance" in {a["name"] for a in fx.assets("workspace")}


def test_a_notebook_the_principal_may_not_read_is_a_gap_not_a_failure(fx):
    fx.world.item_denied.add((FABRIC_WS, MODEL))
    fx.create()
    row = fx.scan()
    assert row["status"] == "partial"
    gap = next(g for g in fx.gaps("permission_denied") if "Sales model" in g["subject"])
    assert "Contributor" in gap["remediation"] and gap["location"]["fabric_item"] == MODEL
    assert {c.kind for c in fx.rec.calls} >= {"notebook", "scanner_result"} and "semantic_model" not in {c.kind for c in fx.rec.calls}


def test_the_power_bi_admin_scanner_refused_is_one_gap_with_the_tenant_setting_to_enable(fx):
    fx.world.pbi_status = 403
    fx.create()
    row = fx.scan()
    assert row["status"] == "partial"
    g = fx.gaps("permission_denied")
    assert len(g) == 1 and g[0]["subject"] == "admin/workspaces/getInfo" and "Service principals can access read-only admin APIs" in g[0]["remediation"]
    assert len(fx.assets("table")) == 3 and not any(c.kind == "scanner_result" for c in fx.rec.calls)


def test_no_access_at_all_fails_the_scan_with_a_clear_message_and_sweeps_nothing(fx):
    fx.create()
    fx.scan()
    fx.world.fabric_status = 403
    row = fx.scan()
    assert row["status"] == "failed" and "workspaces of the tenant cannot be listed" in row["error"] and "Service principals can use Fabric APIs" in row["error"]
    assert BROKER_SECRET not in json.dumps(row, default=str)
    assert len(fx.assets("table")) == 3


def test_a_denied_application_gets_a_permission_gap_for_every_workspace_and_no_tables(fx):
    fx.world.denied_apps.add(BROKER_CLIENT_ID)
    fx.create()
    row = fx.scan()
    assert row["status"] == "partial" and not fx.assets("table")
    assert {g["subject"] for g in fx.gaps("permission_denied")} >= {"Sales", "Finance"}


def test_tables_api_missing_for_a_schema_enabled_lakehouse_falls_back_to_onelake(fx):
    fx.world.schema_enabled_lakehouses.add(LAKEHOUSE)
    fx.create()
    row = fx.scan()
    assert row["status"] == "succeeded", [g["detail"] for g in fx.gaps()]                                       # the fallback is informational, it must not block sweeps
    assert {a["name"] for a in fx.assets("table")} == {"orders", "customers", "ledger"}
    info = [g for g in fx.gaps("api_unavailable") if g["location"].get("informational")]
    assert info and "schemas enabled" in info[0]["detail"]
    assert next(a for a in fx.assets("table") if a["name"] == "orders")["attrs"]["columns"]


def test_a_table_whose_delta_log_cannot_be_read_is_reported_without_columns(fx):
    del fx.world.onelake_files[f"{FABRIC_WS}/{LAKEHOUSE}/Tables/customers/_delta_log/{0:020d}.json"]
    fx.create()
    row = fx.scan()
    t = next(a for a in fx.assets("table") if a["name"] == "customers")
    assert t["attrs"]["columns"] == [] and row["status"] in ("succeeded", "partial")
    assert any("customers" in g["subject"] for g in fx.gaps())


def test_onelake_permission_loss_makes_tables_columnless_with_permission_gaps(fx):
    fx.world.onelake_status = 403
    fx.create()
    row = fx.scan()
    assert row["status"] == "partial"
    g = [x for x in fx.gaps("permission_denied") if "Delta log" in x["detail"]]
    assert len(g) == 3 and "OneLake" in g[0]["remediation"]
    assert all(a["attrs"]["columns"] == [] for a in fx.assets("table"))


# ------------------------------------------------------------------------------------------------ throttling and quota
def test_throttling_is_retried_after_retry_after_and_the_scan_still_succeeds(fx):
    fx.world.throttle_paths = {"/items": 2, "getInfo": 2}
    fx.world.throttle_retry_after = "3"
    fx.create()
    row = fx.scan()
    assert row["status"] == "succeeded", (row["error"], [g["detail"] for g in fx.gaps()])
    assert fx.sleeps.count(3.0) >= 4 and sum(1 for p in fx.paths("/items") if p.endswith("/items")) >= 3
    assert fx.world.throttle_paths == {"/items": 0, "getInfo": 0}


def test_a_scanner_quota_that_would_need_a_long_wait_ends_the_scanner_phase_with_one_gap(fx):
    fx.world.pbi_hourly_quota = 0
    fx.world.pbi_quota_retry_after = "3600"
    fx.create()
    row = fx.scan()
    assert row["status"] == "partial"
    g = fx.gaps("api_unavailable")
    assert len(g) == 1 and "quota is exhausted" in g[0]["detail"] and "500 getInfo requests per hour" in g[0]["remediation"]
    assert 3600.0 not in fx.sleeps and len(fx.assets("table")) == 3                                              # it did not sleep for an hour, and the rest was discovered
    assert not any(c.kind == "scanner_result" for c in fx.rec.calls)


def test_the_local_hourly_budget_is_enforced_before_the_server_says_no(fx):
    fx.create()
    fx.disc.d.runner.limits = lambda: {"getinfo_requests_per_hour": 1, "scanner_batch_size": 1, "max_wait_seconds": 5.0}   # type: ignore[method-assign]
    row = fx.scan()
    assert len(fx.world.pbi_scan_requests) == 1 and row["status"] == "partial"
    assert any("quota is exhausted" in g["detail"] for g in fx.gaps("api_unavailable"))


def test_scanner_workspaces_that_come_back_empty_are_not_inspected_gaps(fx):
    fx.world.pbi_unscannable.add(OTHER_FABRIC_WS)
    fx.create()
    row = fx.scan()
    assert row["status"] == "partial" and any("returned nothing for workspace 'Finance'" in g["detail"] for g in fx.gaps("not_inspected"))


def test_a_scan_that_never_finishes_times_out_with_a_gap(fx):
    fx.world.pbi_scan_stuck = True
    fx.create()
    fx.disc.d.runner.limits = lambda: {"scan_timeout_seconds": 0.0, "scan_poll_seconds": 0.0}   # type: ignore[method-assign]
    row = fx.scan()
    assert row["status"] == "partial" and any("did not finish" in g["detail"] for g in fx.gaps("api_unavailable"))


# ------------------------------------------------------------------------------------------------ configuration of the scan
def test_workspace_ids_item_types_and_switches_narrow_the_scan(fx):
    fx.create(workspace_ids=["Sales"], item_types=["Lakehouse"], definitions=False, scanner={"enabled": False})
    row = fx.scan()
    assert row["status"] == "succeeded"
    assert {a["name"] for a in fx.assets("workspace")} == {"Sales"} and {a["name"] for a in fx.assets("lakehouse")} == {"lh_sales"} and not fx.assets("notebook")
    assert fx.rec.calls == [] and fx.world.pbi_scan_requests == []                                              # type: ignore[attr-defined]
    fx.disc.api("PATCH", f"/sources/{fx.src['id']}", json={"config": {"tables": False}})
    fx.scan()
    assert fx.disc.assets(type="table", status="removed")


def test_a_configured_workspace_that_is_not_visible_is_a_gap(fx):
    fx.create(workspace_ids=["Sales", "Nowhere"])
    row = fx.scan()
    assert row["status"] == "partial" and any(g["subject"] == "Nowhere" for g in fx.gaps("not_inspected"))


def test_request_level_workspace_filter_limits_the_scan_and_makes_a_full_scan_partial(fx):
    fx.create()
    row = fx.scan(workspace_filter=["Sales"])
    assert row["status"] == "partial" and "sweep" not in row["stats"]
    assert {a["name"] for a in fx.assets("workspace")} == {"Sales"}


def test_workspace_map_attaches_olcf_workspaces(fx):
    olcf_ws = fx.disc.intel.workspace("mapped")["id"]
    fx.create(workspace_map={FABRIC_WS: olcf_ws})
    fx.scan()
    assert next(a for a in fx.assets("lakehouse") if a["name"] == "lh_sales")["workspace_id"] == olcf_ws
    assert next(a for a in fx.assets("lakehouse") if a["name"] == "lh_fin")["workspace_id"] is None


def test_workspace_paging_is_followed(fx):
    fx.world.workspaces_page_size = 1
    fx.world.items_page_size = 1
    fx.world.tables_page_size = 1
    fx.create()
    assert fx.scan()["status"] == "succeeded" and len(fx.assets("table")) == 3 and len(fx.assets("workspace")) == 2


# ------------------------------------------------------------------------------------------------ cancellation
def test_cancelling_during_a_throttle_wait_ends_the_scan_cancelled_without_sweeping(fx):
    fx.create()
    fx.scan()
    before = {a["id"] for a in fx.assets("table")}
    fx.world.lakehouse_tables[(FABRIC_WS, LAKEHOUSE)] = []                                                        # would be swept by a complete scan
    fx.world.throttle_paths = {"/items": 1}
    fx.world.throttle_retry_after = "2"

    def cancel_during_sleep(seconds: float) -> None:
        fx.sleeps.append(seconds)
        for sid in fx.disc.d.runner.active_scan_ids():
            fx.disc.d.runner.cancel(SYSTEM_PRINCIPAL, sid)

    fx.disc.d.runner.sleep = cancel_during_sleep
    row = fx.scan()
    assert row["status"] == "cancelled" and "sweep" not in row["stats"]
    assert {a["id"] for a in fx.assets("table")} == before


# ------------------------------------------------------------------------------------------------ the connectivity test
STEPS = ["resolve credentials", "acquire Fabric token", "list workspaces", "acquire Power BI token", "scanner permission probe", "item read probe", "definition read probe",
         "sampling identity probe"]


def failing_step(out: dict[str, Any]) -> dict[str, Any]:
    return next(s for s in out["steps"] if s["name"] == out["failed_step"])


def test_all_eight_steps_pass_against_a_healthy_tenant_with_a_sampling_identity(fx):
    fx.create(sampling=True)
    out = fx.test()
    assert out["ok"] is True and [s["name"] for s in out["steps"]] == STEPS and all(s["ok"] and not s["skipped"] for s in out["steps"])
    assert "2 workspace(s) visible" in out["steps"][2]["detail"] and "no data was read" in out["steps"][7]["detail"]
    assert BROKER_SECRET not in json.dumps(out) and SAMPLING_SECRET not in json.dumps(out)


def test_without_a_sampling_identity_the_last_step_is_skipped_not_failed(fx):
    fx.create()
    out = fx.test()
    assert out["ok"] is True and out["steps"][7]["skipped"] is True and "no sampling identity" in out["steps"][7]["detail"]


@pytest.mark.parametrize("step_index, break_world, remediation_fragment", [
    (1, lambda w: setattr(w, "client_secret", "someone-rotated-it"), "Create a new secret"),
    (2, lambda w: setattr(w, "fabric_status", 403), "Service principals can use Fabric APIs"),
    (2, lambda w: setattr(w, "workspaces", []), "at least one workspace"),
    (4, lambda w: setattr(w, "pbi_status", 403), "Service principals can access read-only admin APIs"),
    (5, lambda w: w.workspace_denied.add(FABRIC_WS), "add the service principal to that workspace"),
    (6, lambda w: (w.fabric_items.__setitem__(FABRIC_WS, [i for i in w.fabric_items[FABRIC_WS] if i["type"] != "Notebook"]), w.item_denied.add((FABRIC_WS, MODEL))), "Contributor"),
    (7, lambda w: setattr(w, "onelake_status", 403), "OneLake data access role"),
])
def test_each_failing_step_stops_the_test_and_says_how_to_fix_it(fx, step_index, break_world, remediation_fragment):
    fx.create(sampling=True)
    break_world(fx.world)
    out = fx.test()
    assert out["ok"] is False and out["failed_step"] == STEPS[step_index], out
    failed = failing_step(out)
    assert remediation_fragment in (failed["remediation"] or ""), failed
    after = out["steps"][step_index + 1:]
    assert all(s["skipped"] for s in after) and [s["name"] for s in out["steps"]] == STEPS
    assert all(s["ok"] for s in out["steps"][:step_index])
    assert BROKER_SECRET not in json.dumps(out) and "someone-rotated-it" not in json.dumps(out)


def test_step_1_fails_when_the_stored_secret_is_gone(fx):
    src = fx.create()
    row = fx.disc.d.sources.get(src["id"])
    fx.disc.ctx.secrets.delete(row["secret_ref"])
    out = fx.test()
    assert out["failed_step"] == STEPS[0] and out["steps"][0]["remediation"] and all(s["skipped"] for s in out["steps"][1:])


def test_step_4_fails_when_the_power_bi_token_is_refused(fx, monkeypatch):
    fx.create()
    real = TokenBroker.get_token

    def get_token(self: Any, scope: str, *a: Any, **kw: Any) -> str:
        if "powerbi" in scope:
            raise EntraTokenError("AADSTS500011: resource principal not found", remediation="grant the app registration the Power BI Service API permission")
        return real(self, scope, *a, **kw)

    monkeypatch.setattr(TokenBroker, "get_token", get_token)
    out = fx.test()
    assert out["failed_step"] == STEPS[3] and "Power BI" in failing_step(out)["remediation"] and all(s["skipped"] for s in out["steps"][4:])


def test_the_scanner_steps_are_skipped_when_the_scanner_is_turned_off(fx):
    fx.create(scanner={"enabled": False})
    out = fx.test()
    assert out["ok"] is True and out["steps"][3]["skipped"] and out["steps"][4]["skipped"] and not out["steps"][5]["skipped"]


def test_a_wrong_sampling_secret_fails_only_step_8(fx):
    fx.create(sampling=True)
    fx.world.extra_clients[SAMPLING_CLIENT_ID] = "rotated-elsewhere"
    out = fx.test()
    assert out["failed_step"] == STEPS[7] and all(s["ok"] for s in out["steps"][:7]) and "sampling identity" in failing_step(out)["detail"]


def test_the_metadata_and_sampling_identities_are_used_separately(fx):
    fx.create(sampling=True)
    fx.world.token_requests.clear()
    assert fx.test()["ok"] is True
    by_client: dict[str, set[str]] = {}
    for form in fx.world.token_requests:
        by_client.setdefault(form["client_id"], set()).add(form["scope"].rsplit("/.default", 1)[0])
    assert set(by_client) == {BROKER_CLIENT_ID, SAMPLING_CLIENT_ID}
    assert by_client[SAMPLING_CLIENT_ID] == {"https://storage.azure.com"}                                      # the sampling identity only ever asks for OneLake data access
    assert "https://api.fabric.microsoft.com" in by_client[BROKER_CLIENT_ID]
    fx.world.token_requests.clear()
    fx.scan()
    assert {f["client_id"] for f in fx.world.token_requests} == {BROKER_CLIENT_ID}                              # scanning never uses the sampling identity


def test_a_source_can_be_tested_before_it_is_saved_without_storing_anything(fx):
    body = {"kind": "fabric", "config": {"tenant_id": TENANT, "metadata_identity": {"tenant_id": TENANT, "client_id": BROKER_CLIENT_ID}, "network": fx.world.discovery_network_config()},
            "credentials": {"client_secret": BROKER_SECRET}}
    r = fx.disc.api("POST", "/sources/test", json=body)
    assert r.status_code == 200, r.text
    assert r.json()["ok"] is True and BROKER_SECRET not in r.text
    assert fx.disc.api("GET", "/sources").json()["items"][0]["kind"] != "fabric"


# ------------------------------------------------------------------------------------------------ secrets never leak
def dump_everything(disc: Any) -> str:
    out: list[Any] = []
    with disc.ctx.db.connect() as c:
        for t in (discovery_sources, gaps_t, scans_t, assets_t):
            out.extend(dict(r._mapping) for r in c.execute(select(t)))
    return json.dumps(out, default=str) + json.dumps(disc.ctx.audit.query(limit=1000), default=str)


def test_secrets_never_appear_in_rows_logs_audit_responses_or_error_messages(fx, caplog):
    caplog.set_level(logging.DEBUG)
    src = fx.create(sampling=True)
    texts: list[str] = [json.dumps(src)]
    texts.append(json.dumps(fx.test()))
    texts.append(json.dumps(fx.scan(), default=str))
    fx.world.pbi_status = 403
    fx.world.workspace_denied.add(OTHER_FABRIC_WS)
    texts.append(json.dumps(fx.scan(), default=str))
    fx.world.fabric_status = 403
    texts.append(json.dumps(fx.scan(), default=str))
    for path in (f"/sources/{src['id']}", "/sources", "/gaps", "/scans", "/coverage", f"/sources/{src['id']}/scans"):
        texts.append(fx.disc.api("GET", path).text)
    blob = "\n".join(texts) + dump_everything(fx.disc) + caplog.text
    for secret in (BROKER_SECRET, SAMPLING_SECRET):
        assert secret not in blob
    assert "Bearer eyJ" not in blob


def test_an_upstream_error_that_echoes_the_secret_is_scrubbed(fx, monkeypatch):
    fx.create()
    real = TokenBroker.get_token

    def leaky(self: Any, scope: str, *a: Any, **kw: Any) -> str:
        raise EntraTokenError(f"AADSTS7000215: Invalid client secret provided: {BROKER_SECRET}")

    fx.test()                                                                                                       # resolving the identity teaches the process the secret value
    monkeypatch.setattr(TokenBroker, "get_token", leaky)
    out = fx.test()
    assert out["ok"] is False and BROKER_SECRET not in json.dumps(out)
    monkeypatch.setattr(TokenBroker, "get_token", real)


def test_credentials_are_write_only_and_the_source_view_says_only_that_they_exist(fx):
    src = fx.create(sampling=True)
    assert src["has_credentials"] is True and src["has_sampling_identity"] is True
    again = fx.disc.api("GET", f"/sources/{src['id']}").json()
    assert "credentials" not in again and "client_secret" not in json.dumps(again) and BROKER_SECRET not in json.dumps(again)
