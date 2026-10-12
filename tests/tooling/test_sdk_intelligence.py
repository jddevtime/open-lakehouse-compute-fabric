# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""``client.intelligence`` (sync and async) against the real application: every wrapper is exercised end to end over the in-process transport on a populated asset graph,
including pagination iterators, ETags, error mapping, visibility (hidden existence) and the write-only handling of credentials."""

from __future__ import annotations

import json

import httpx
import pytest

from olcf.intelligence import model as M
from olcf.sdk import (
    AsyncClient,
    BadRequestError,
    ConflictError,
    NotFoundError,
    PermissionDeniedError,
    PreconditionFailedError,
    ValidationError,
)
from olcf.sdk._core import Page, Resource
from tests.intelligence.conftest import Builder
from tests.intelligence.surfaces_support import Stack, intel_stack


@pytest.fixture(scope="module")
def stack(tmp_path_factory):
    with intel_stack(tmp_path_factory.mktemp("sdk_intel")) as st:
        yield st


@pytest.fixture()
def api(stack: Stack):
    return stack.sdk().intelligence


# ------------------------------------------------------------------------------------------------------------------------------ assets and graph
def test_summary_and_asset_listing(stack, api):
    s = api.summary()
    assert isinstance(s, Resource) and s["assets"]["by_type"]["table"] == 7 and s["edges"]["by_confidence"]["verified"] == 1 and s["organisation_wide"] is True
    page = api.list_assets(type="table", limit=3)
    assert isinstance(page, Page) and len(page) == 3 and page.next_cursor
    assert {a["type"] for a in page} == {"table"}
    assert [a["name"] for a in api.list_assets(q="SALES_report")] == ["sales_report"]
    assert {a["name"] for a in api.list_assets(type=["notebook", "report"])} == {"etl", "agg", "beta_nb", "sales_report"}
    assert {a["name"] for a in api.list_assets(workspace_id=stack.world.ws2["id"], type="table")} == {"secret", "secret_out"}
    with pytest.raises(BadRequestError):
        api.list_assets(type="spaceship")
    assert list(api.list_assets(domain="finance")) == []          # governance filters work; nothing sits in a domain called finance yet


def test_iter_assets_follows_cursors(api):
    every = list(api.iter_assets(limit=4))
    assert len(every) == 19 and len({a["id"] for a in every}) == 19
    tables = list(api.iter_assets(type="table", limit=2))
    assert len(tables) == 7
    kids = list(api.iter_children(next(a["id"] for a in every if a["name"] == "lakehouse_a"), limit=2))
    assert {k["name"] for k in kids} == {"raw", "clean", "mart", "lonely", "fresh"}
    hist = list(api.iter_history(next(a["id"] for a in every if a["name"] == "raw"), limit=1))
    assert len(hist) == 2 and hist[0]["changes"]["columns_added"] == ["region"]


def test_get_asset_etag_and_update_with_if_match(stack):
    api = stack.sdk("curator_org").intelligence
    a = api.get_asset(stack.ids["lonely"])
    assert a.etag and a["etag"] == a.etag and a["child_count"] == 0 and a["parent"]["name"] == "lakehouse_a"
    updated = api.update_asset(a["id"], a, description="Orphan table", tags=["tmp"])
    assert updated["description"] == "Orphan table" and updated["tags"] == ["tmp"] and updated.etag != a.etag
    with pytest.raises(PreconditionFailedError):
        api.update_asset(a["id"], a, description="stale write")
    again = api.update_asset(a["id"], updated, description="")      # an empty text hands the field back to the scans
    assert again["description"] == ""
    assert stack.sdk("alpha").intelligence.get_asset(a["id"])["name"] == "lonely"
    with pytest.raises(PermissionDeniedError):
        stack.sdk("alpha").intelligence.update_asset(a["id"], description="viewers cannot curate")


def test_lineage_origin_impact_staleness(stack, api):
    ids = stack.ids
    g = api.lineage(ids["clean"], direction="both", depth=3)
    assert g["root"]["name"] == "clean" and g["level"] == "table" and not g["truncated"]
    assert {n["name"] for n in g["nodes"]} >= {"etl", "raw", "agg", "mart", "beta_nb"}
    assert all({"from", "to", "confidence"} <= set(e) for e in g["edges"])
    up = api.lineage(ids["clean"], direction="up", depth=1)
    assert {n["name"] for n in up["nodes"] if n["direction"] != "root"} == {"etl", "raw", "crm_system"}      # depth counts dataset hops: the process and the datasets it reads
    tables_only = api.lineage(ids["sales_report"], direction="up", depth=5, types=["table"])
    assert {n["type"] for n in tables_only["nodes"]} == {"table", "report"} or {n["type"] for n in tables_only["nodes"]} <= {"table", "report", "source_system"}
    strong = api.lineage(ids["sales_report"], direction="up", depth=5, min_confidence="derived")
    assert all(e["confidence"] in ("verified", "derived") for e in strong["edges"])
    col = api.lineage(ids["clean.amount"], level="column")
    assert col["level"] == "column" and [e["transformation"] for e in col["edges"]] == ["amount * 1.0"]
    assert api.lineage(ids["clean"], column="amount")["level"] == "column"
    o = api.origin(ids["sales_report"])
    assert {r["asset"]["name"] for r in o["roots"]} == {"raw", "crm_system"} and o["paths"][0]["confidence"] == "inferred"
    oc = api.origin(ids["clean.amount"], level="column")
    assert oc["roots"][0]["asset"]["name"] == "amount" and oc["roots"][0]["best_confidence"] == "derived"
    imp = api.impact(ids["raw"])
    assert imp["total"] == 5 and set(imp["by_type"]) == {"table", "semantic_model", "report"}
    assert api.impact(ids["raw"], depth=1)["total"] < imp["total"]
    st = api.staleness(ids["clean"])
    assert st["stale"] is False and st["threshold_hours"] == 24.0 and st["nodes"][0]["asset"]["name"] == "clean"
    assert api.staleness(ids["clean"], stale_after_hours=48)["threshold_hours"] == 48


def test_documentation_history_edge_and_resolve(stack, api):
    ids = stack.ids
    doc = api.documentation(ids["clean"])
    assert doc["name"] == "clean" and "## Schema" in doc["markdown"]
    md = api.documentation_markdown(ids["clean"])
    assert isinstance(md, str) and md == doc["markdown"]
    h = api.history(ids["raw"])
    assert [x["changes"].get("columns_added") for x in h] == [["region"], None] and h.extra["asset_id"] == ids["raw"]
    assert len(api.history(ids["raw"], schema_only=True)) == 1
    edge_id = api.lineage(ids["clean.amount"], level="column")["edges"][0]["id"]
    e = api.get_edge(edge_id)
    assert e["id"] == edge_id and e["evidence"] and e["confidence"] == "derived"
    r = api.resolve("table", name="clean", scope={"workspace_id": stack.world.ws1["id"], "lakehouse": "lakehouse_a"})
    assert r["asset"]["id"] == ids["clean"] and r["created"] is False
    assert api.resolve("table", aliases={"lakehouse_table": "itm_lh1:clean"})["asset"]["id"] == ids["clean"]
    with pytest.raises(NotFoundError):
        api.resolve("table", name="never_seen", scope={"workspace_id": stack.world.ws1["id"]})
    with pytest.raises(NotFoundError):
        api.get_asset("ast_NOPE")


def test_snapshots_diff_unused_and_sampling_default(stack, api):
    snaps = api.snapshots()
    assert [s["version"] for s in snaps] == [2, 1]
    d = api.diff(snaps[1]["id"], snaps[0]["id"])
    assert d["summary"]["assets_added"] == 1 and d["assets"]["added"][0]["name"] == "fresh"
    assert d["assets"]["changed"][0]["changes"]["columns_added"] == ["region"]
    assert len(list(api.iter_snapshots(limit=1))) == 2
    u = api.unused(days=0, type="table")
    assert isinstance(u, Page) and u.extra["days"] == 0 and "lonely" in {x["name"] for x in u}
    assert "lonely" in {x["name"] for x in api.iter_unused(days=0, limit=2)}
    pol = api.asset_sampling_policy(stack.ids["clean"])
    assert pol["allowed"] is False and pol["scope"] == "none"
    with pytest.raises(PermissionDeniedError) as ei:
        api.sample(stack.ids["clean"])
    assert ei.value.code == "policy_denied" and ei.value.remediation and "sampling-policies" in ei.value.remediation
    assert api.suggestions(stack.ids["clean"])["suggestions"] == []


def test_visibility_is_enforced_by_the_server(stack):
    alpha, beta = stack.sdk("alpha").intelligence, stack.sdk("beta").intelligence
    with pytest.raises(NotFoundError):
        alpha.get_asset(stack.ids["secret"])
    assert beta.get_asset(stack.ids["secret"])["name"] == "secret"
    assert "secret" not in {a["name"] for a in alpha.iter_assets()}
    with pytest.raises(PermissionDeniedError):
        stack.sdk("none").intelligence.summary()
    with pytest.raises(PermissionDeniedError):
        alpha.create_source("nope", "local")


# ------------------------------------------------------------------------------------------------------------------------------ discovery
def test_sources_crud_and_write_only_credentials(stack):
    api = stack.sdk().intelligence
    local = [s for s in api.list_sources() if s["kind"] == "local"]
    assert local and local[0]["system"] is True
    created = api.create_source("mirror", "catalog_mirror", config={"workspace_ids": []}, schedule={"incremental_seconds": 3600})
    assert created["name"] == "mirror" and created.etag and created["has_credentials"] is False
    got = api.get_source("mirror")
    assert got["id"] == created["id"] and api.get_source(created["id"])["name"] == "mirror"
    with pytest.raises(ConflictError):
        api.create_source("mirror", "catalog_mirror")
    with pytest.raises(PreconditionFailedError):
        api.update_source("mirror", "bogus-etag", enabled=False)
    upd = api.update_source("mirror", got, enabled=False)
    assert upd["enabled"] is False and upd.etag != got.etag
    assert api.list_sources(kind="catalog_mirror")[0]["name"] == "mirror"
    # a Fabric source: credentials go in, only a flag comes out
    secret = "Zx9~sdk-test-secret-must-never-come-back"
    fab = api.create_source("tenant", "fabric", config={"tenant_id": "11111111-1111-4111-8111-111111111111", "metadata_identity": {"tenant_id": "11111111-1111-4111-8111-111111111111",
                                                                                                                                 "client_id": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"}},
                            credentials={"client_secret": secret})
    assert fab["has_credentials"] is True and secret not in json.dumps(fab) and secret not in json.dumps(api.get_source("tenant")) and secret not in json.dumps(api.list_sources())
    with pytest.raises(ValidationError):
        api.create_source("bad", "fabric", config={})                          # needs a metadata identity
    assert api.delete_source("mirror") is not None
    with pytest.raises(NotFoundError):
        api.get_source("mirror")
    api.delete_source("tenant")
    with pytest.raises(PermissionDeniedError):
        stack.sdk("alpha").intelligence.create_source("viewer-source", "local")


def test_connectivity_tests_report_steps_and_remediation(stack):
    api = stack.sdk().intelligence
    ok = api.test_config("catalog_mirror", config={})
    assert ok["ok"] is True and all(s["ok"] for s in ok["steps"])
    bad = api.test_config("fabric", config={"tenant_id": "11111111-1111-4111-8111-111111111111", "metadata_identity": {"tenant_id": "11111111-1111-4111-8111-111111111111",
                                                                                                                         "client_id": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"},
                                           "network": {"timeout_seconds": 2}}, credentials={"client_secret": "nope"})
    assert bad["ok"] is False and bad["failed_step"] and any(s["skipped"] for s in bad["steps"])
    failing = next(s for s in bad["steps"] if not s["ok"] and not s["skipped"])
    assert failing["remediation"] and "nope" not in json.dumps(bad)
    local = next(s for s in api.list_sources() if s["kind"] == "local")
    assert api.test_source(local["id"])["ok"] is True


def test_scan_lifecycle_gaps_coverage_and_policies(stack):
    admin = stack.sdk()
    api = admin.intelligence
    ws = admin.workspaces.create("sdk-scan-ws")
    lh = admin.lakehouses.create(ws.id, "sdklh")
    svc = stack.ctx.service("storage")
    import pyarrow as pa

    svc.tables.write_table(svc.get_lakehouse(lh.id), "people", pa.table({"id": pa.array([1, 2, 3], pa.int64()), "email": ["a@x.org", "b@x.org", "c@x.org"]}))
    scan = api.scan_source("local", mode="full", wait=True)
    assert scan["status"] in ("succeeded", "partial") and scan["mode"] == "full"
    final = api.wait_scan(scan["id"], timeout=60)
    assert final["status"] in ("succeeded", "partial")
    assert api.get_scan(scan["id"])["id"] == scan["id"]
    assert scan["id"] in {s["id"] for s in api.iter_scans(source_id=api.get_source("local")["id"], limit=1)}
    assert all(s["status"] == "succeeded" for s in api.list_scans(status="succeeded"))
    tbl = next(a for a in api.iter_assets(q="people", type="table"))
    sugg = api.suggestions(tbl["id"])
    assert any("email" in json.dumps(x).lower() for x in sugg["suggestions"])
    cov = api.coverage()
    assert any(s["kind"] == "local" and s["assets"]["active"] > 0 for s in cov["sources"]) and cov["sampling"]["enabled"] is False and "fabric" in cov["connectors"]
    gaps = api.list_gaps()
    assert isinstance(gaps, Page) and list(api.iter_gaps(limit=1)) == list(gaps)
    # sampling stays off until a policy allows it, then is masked and bounded
    with pytest.raises(PermissionDeniedError):
        api.sample(tbl["id"])
    pol = api.put_sampling_policy("workspace", 2, scope_id=ws.id, masking="redact")
    assert pol["max_rows"] == 2 and pol["masking"] == "redact" and pol["scope_type"] == "workspace"
    assert api.list_sampling_policies(scope_type="workspace")[0]["id"] == pol["id"]
    eff = api.asset_sampling_policy(tbl["id"])
    assert eff["allowed"] is True and eff["max_rows"] == 2
    s = api.sample(tbl["id"], rows=5, columns=["id"])
    assert s["row_count"] <= 2 and [c["name"] for c in s["columns"]] == ["id"]
    assert api.put_sampling_policy("workspace", 0, scope_id=ws.id)["id"] == pol["id"]       # replaced in place: 0 turns it off
    assert api.asset_sampling_policy(tbl["id"])["allowed"] is False
    api.delete_sampling_policy(pol["id"])
    assert api.list_sampling_policies() == []
    with pytest.raises(NotFoundError):
        api.delete_sampling_policy(pol["id"])


def test_cancel_scan_of_a_finished_scan_conflicts_and_unknown_ids_are_404(stack):
    api = stack.sdk().intelligence
    done = api.list_scans(limit=1)[0]
    with pytest.raises(ConflictError):
        api.cancel_scan(done["id"])
    with pytest.raises(NotFoundError):
        api.get_scan("scn_NOPE")
    with pytest.raises(NotFoundError):
        api.scan_source("no-such-source")


def test_import_scanner_accepts_bytes_text_and_dict_and_refuses_credentials(stack):
    from tests.fabric.fabric_fakes import FABRIC_WS, pbi_dataset, pbi_table, scanner_workspace

    api = stack.sdk().intelligence
    doc = {"workspaces": [scanner_workspace(FABRIC_WS, "Imported", datasets=[pbi_dataset("d1d1d1d1-0000-4000-8000-000000000001", "Imported model", [pbi_table("t", [("c", "Int64")])])])],
           "datasourceInstances": []}
    res = api.import_scanner(doc, name="sdk-import", wait=True)
    assert res["workspaces"] == 1 and res["source"]["kind"] == "scanner_import" and res["scan"]["status"] in ("succeeded", "partial")
    again = api.import_scanner(json.dumps(doc).encode(), name="sdk-import", scan=False)
    assert again["scan"] is None and again["source"]["id"] == res["source"]["id"]
    assert api.import_scanner(json.dumps(doc), name="sdk-import-2", scan=False)["source"]["name"] == "sdk-import-2"
    poisoned = json.loads(json.dumps(doc))
    poisoned["workspaces"][0]["connectionString"] = "Server=x;Password=hunter2hunter2;"
    with pytest.raises(ValidationError, match="credential"):
        api.import_scanner(poisoned, name="sdk-poisoned")
    with pytest.raises((BadRequestError, ValidationError)):
        api.import_scanner(b"{not json", name="sdk-bad")


# ------------------------------------------------------------------------------------------------------------------------------ async client
@pytest.fixture()
async def async_api(tmp_path):
    from olcf.api.app import create_app
    from olcf.core.config import load_settings
    from tests.intelligence.conftest import INTEL_MODULES

    s = load_settings(tmp_path / "data", environment="test")
    s.compute.embedded_worker = False
    app = create_app(s, module_names=INTEL_MODULES)
    async with app.router.lifespan_context(app):
        pw = app.state.ctx.settings.path("secrets", "bootstrap-admin.txt").read_text().split("password=")[1].strip()
        svc = app.state.ctx.service("intelligence")
        b = Builder("wks_async")
        lh = b.lakehouse("itm_a", "lh")
        raw, clean = b.table(lh, "raw", [("id", "int")]), b.table(lh, "clean", [("id", "int")])
        nb = b.notebook("itm_n", "load")
        sid = svc.engine.begin_scan("local", "full", "manual")
        stats = svc.engine.apply_facts(M.Facts([lh, raw, clean, nb], [b.edge(nb, raw, "READS_FROM"), b.edge(nb, clean, "WRITES_TO", "verified", kind="runtime", source="runtime:job", ref="j1", location={})]),
                                       scan_id=sid, source_id="local")
        svc.engine.finish_scan(sid, "succeeded", stats.as_dict())
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://testserver") as http:
            c = AsyncClient("http://testserver", http_client=http, max_retries=0)
            await c.login("admin", pw)
            yield c


async def test_async_intelligence_reads_iterators_and_errors(async_api):
    api = async_api.intelligence
    s = await api.summary()
    assert s["assets"]["by_type"]["table"] == 2
    names = [a["name"] async for a in api.iter_assets(limit=2)]
    assert sorted(names) == sorted(["lh", "raw", "clean", "load"])
    clean = (await api.list_assets(q="clean", type="table"))[0]
    g = await api.lineage(clean["id"], direction="up")
    assert {n["name"] for n in g["nodes"]} >= {"load", "raw"}
    o = await api.origin(clean["id"])
    assert o["roots"][0]["asset"]["name"] == "raw"
    imp = await api.impact((await api.list_assets(q="raw", type="table"))[0]["id"])
    assert imp["total"] >= 1
    assert "# clean" in await api.documentation_markdown(clean["id"])
    with pytest.raises(NotFoundError):
        await api.get_asset("ast_NOPE")
    snaps = [x async for x in api.iter_snapshots()]
    assert len(snaps) == 1
    scans = await api.list_scans()
    assert (await api.wait_scan(scans[0]["id"], timeout=5))["status"] == "succeeded"
    pol = await api.put_sampling_policy("global", 3)
    assert pol["max_rows"] == 3
    await api.delete_sampling_policy(pol["id"])
    assert [x async for x in api.iter_children(next(a for a in await api.list_assets(type="lakehouse"))["id"], limit=1)]
    assert [g async for g in api.iter_gaps()] == []


def test_requeue_inbox(api):
    res = api.requeue_inbox()
    assert res["requeued"] == 0 and {"pending", "failed"} <= set(res["inbox"])
