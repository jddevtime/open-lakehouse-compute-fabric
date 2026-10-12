# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The ``catalog_mirror`` connector: a read-only mirror of ``catalog_entries`` and ``lineage_edges`` (READS_FROM / WRITES_TO / column TRANSFORMS, confidence ``derived``)."""

from __future__ import annotations

from typing import Any

import pyarrow as pa
from sqlalchemy import func, select

from olcf.catalog.schema import catalog_entries, lineage_edges
from olcf.security.rbac import SYSTEM_PRINCIPAL
from tests.intelligence.discovery.conftest import people_table


def record(disc: Any, **body: Any) -> Any:
    r = disc.intel.post("/v1/catalog/lineage", json={"workspace_id": disc.ws["id"], **body}, headers=disc.admin)
    assert r.status_code == 201, r.text
    return r.json()


def mirror(disc: Any, **config: Any) -> dict[str, Any]:
    return disc.d.sources.create(SYSTEM_PRINCIPAL, name="catalog", kind="catalog_mirror", config=config)


def counts(disc: Any) -> tuple[int, int]:
    with disc.ctx.db.connect() as c:
        return (int(c.execute(select(func.count()).select_from(catalog_entries)).scalar() or 0), int(c.execute(select(func.count()).select_from(lineage_edges)).scalar() or 0))


def test_catalog_lineage_is_mirrored_as_derived_metadata_edges(disc):
    disc.lakehouse()
    lh = disc.lh["id"]
    disc.write("raw", people_table())
    disc.write("clean", people_table())
    nb = disc.notebook("etl", "x = 1")
    record(disc, sources=[{"kind": "table", "lakehouse_id": lh, "name": "raw"}], targets=[{"kind": "table", "lakehouse_id": lh, "name": "clean"}],
           via={"kind": "notebook", "ref": nb["id"], "name": "etl"}, columns=[{"target": "amount", "sources": [{"source": 0, "column": "amount"}], "transform": "amount * 2"}])
    disc.api("PATCH", "/sources/local", json={"config": {"mirror_catalog_lineage": False}})        # the standalone kind is under test here, not the local source's built-in mirror
    disc.scan()                                                                                      # the local scan reports the tables; the mirror then adds what the catalog knows
    src = mirror(disc)
    before = counts(disc)
    row = disc.scan(src["id"])
    assert row["status"] == "succeeded" and row["stats"]["counters"]["catalog_edges"] >= 1
    assert counts(disc) == before                                                                    # read-only: the catalog tables were not written
    raw, clean = disc.asset("raw", "table"), disc.asset("clean", "table")
    edges = {(e["type"], e["src_id"], e["dst_id"]): e for e in disc.edges(source_id=src["id"])}
    process = disc.asset("etl", "notebook")
    reads, writes = edges[("READS_FROM", process["id"], raw["id"])], edges[("WRITES_TO", process["id"], clean["id"])]
    for e in (reads, writes):
        assert e["confidence"] == "derived" and e["evidence"][0]["kind"] == "metadata" and e["evidence"][0]["source"] == "catalog-mirror"
    col = [e for e in disc.edges(type="TRANSFORMS", source_id=src["id"])]
    assert len(col) == 1 and col[0]["via_id"] == process["id"] and col[0]["transformation"] == "amount * 2" and col[0]["confidence"] == "derived"
    assert {raw["id"], clean["id"]} == {col[0]["src_id"] and disc.asset_parent(col[0]["src_id"]), col[0]["dst_id"] and disc.asset_parent(col[0]["dst_id"])}


def test_catalog_descriptions_tags_and_columns_are_mirrored_without_a_local_scan(disc):
    disc.lakehouse()
    disc.write("raw", people_table())
    assert disc.intel.post("/v1/catalog/sync", params={"workspace_id": disc.ws["id"]}, headers=disc.admin).status_code == 200
    entry = disc.intel.get("/v1/catalog/tables", params={"workspace_id": disc.ws["id"]}, headers=disc.admin).json()["items"][0]
    patch = disc.intel.patch(f"/v1/catalog/tables/{entry['id']}", json={"description": "People master data", "tags": ["pii", "gold"], "column_descriptions": {"city": "Home city"}},
                             headers=disc.admin)
    assert patch.status_code == 200, patch.text
    src = mirror(disc)
    row = disc.scan(src["id"])
    assert row["status"] == "succeeded" and row["stats"]["counters"]["catalog_tables"] == 1
    table = disc.asset("raw", "table")
    assert table["description"] == "People master data" and sorted(table["attrs"]["source_tags"]) == ["gold", "pii"] and table["attrs"]["source_kind"] == "catalog"
    assert [c["name"] for c in table["attrs"]["columns"]] == ["id", "email", "ssn", "amount", "city"]
    assert table["source_id"] == src["id"] and table["status"] == "active"
    # a later local scan finds the same asset through the shared aliases instead of creating a second one
    disc.scan()
    assert len(disc.assets(name="raw", type="table")) == 1


def test_unidentifiable_datasets_become_gaps_not_failures(disc):
    disc.lakehouse()
    disc.write("raw", people_table())
    record(disc, sources=[{"kind": "table", "lakehouse_id": disc.lh["id"], "name": "raw"}], targets=[{"kind": "external", "name": "warehouse.sales"}], via={"kind": "job", "name": "nightly"})
    src = mirror(disc)
    row = disc.scan(src["id"])
    assert row["status"] == "succeeded"
    ext = disc.asset("warehouse.sales", "external")
    assert [e for e in disc.edges(type="WRITES_TO") if e["dst_id"] == ext["id"]]
    script = disc.asset("nightly", "script")
    assert script["workspace_id"] == disc.ws["id"]


def test_incremental_mirror_only_reads_recent_rows(disc):
    disc.lakehouse()
    lh = disc.lh["id"]
    disc.write("a", pa.table({"id": pa.array([1], pa.int64())}))
    disc.write("b", pa.table({"id": pa.array([1], pa.int64())}))
    disc.write("c", pa.table({"id": pa.array([1], pa.int64())}))
    record(disc, sources=[{"kind": "table", "lakehouse_id": lh, "name": "a"}], targets=[{"kind": "table", "lakehouse_id": lh, "name": "b"}], via={"kind": "job", "name": "j1"})
    src = mirror(disc)
    first = disc.scan(src["id"])
    assert first["stats"]["counters"]["catalog_edges"] == 1
    import time

    time.sleep(0.05)
    record(disc, sources=[{"kind": "table", "lakehouse_id": lh, "name": "b"}], targets=[{"kind": "table", "lakehouse_id": lh, "name": "c"}], via={"kind": "job", "name": "j2"})
    inc = disc.scan(src["id"], mode="incremental")
    assert inc["mode"] == "incremental" and inc["stats"]["counters"]["catalog_edges"] == 1 and "sweep" not in inc["stats"]


def test_the_mirror_can_be_scoped_and_its_connectivity_is_tested(disc):
    disc.lakehouse("alpha", "lh1")
    other = disc.intel.workspace("beta")
    src = mirror(disc, workspace_ids=[other["id"]])
    out = disc.d.test_source(SYSTEM_PRINCIPAL, src["id"])
    assert out["ok"] is True and [s["name"] for s in out["steps"]] == ["catalog module", "catalog tables"]
    row = disc.scan(src["id"])
    assert row["status"] == "succeeded" and row["stats"]["facts"]["assets"] == 0                       # nothing in beta


# ------------------------------------------------------------------------------------------------ the local source mirrors recorded lineage by itself
def recorded_flow(disc: Any) -> dict[str, Any]:
    disc.lakehouse()
    lh = disc.lh["id"]
    disc.write("raw", people_table())
    disc.write("clean", people_table())
    nb = disc.notebook("etl", "x = 1")
    record(disc, sources=[{"kind": "table", "lakehouse_id": lh, "name": "raw"}], targets=[{"kind": "table", "lakehouse_id": lh, "name": "clean"}],
           via={"kind": "notebook", "ref": nb["id"], "name": "etl"}, columns=[{"target": "amount", "sources": [{"source": 0, "column": "amount"}], "transform": "amount * 2"}])
    return nb


def test_the_default_local_source_also_mirrors_the_catalogs_recorded_lineage(disc):
    recorded_flow(disc)
    row = disc.scan()                                                                                    # only the default local source exists
    assert row["status"] == "succeeded" and row["stats"]["counters"]["catalog_edges"] == 1
    local = disc.source("local")
    raw, clean, process = disc.asset("raw", "table"), disc.asset("clean", "table"), disc.asset("etl", "notebook")
    flows = {(e["type"], e["src_id"], e["dst_id"]): e for e in disc.edges(source_id=local["id"])}
    for key in (("READS_FROM", process["id"], raw["id"]), ("WRITES_TO", process["id"], clean["id"])):
        assert key in flows and flows[key]["confidence"] == "derived" and flows[key]["evidence"][0]["source"] == "catalog-mirror"
    col = disc.edges(type="TRANSFORMS", source_id=local["id"])
    assert len(col) == 1 and col[0]["transformation"] == "amount * 2" and col[0]["via_id"] == process["id"]
    assert not disc.assets(type="table", name="raw", status="removed") and len(disc.assets(name="raw", type="table")) == 1       # no duplicate tables from the mirror
    assert len([s for s in disc.d.sources.list()]) == 1                                                 # no second source was needed
    again = disc.scan()
    assert again["status"] == "succeeded" and len(disc.assets(name="etl", type="notebook")) == 1


def test_mirror_catalog_lineage_can_be_switched_off_and_is_validated(disc):
    recorded_flow(disc)
    r = disc.api("PATCH", "/sources/local", json={"config": {"mirror_catalog_lineage": False}})
    assert r.status_code == 200, r.text
    assert r.json()["config"]["mirror_catalog_lineage"] is False
    row = disc.scan()
    assert "catalog_edges" not in row["stats"]["counters"] and not disc.edges(type="WRITES_TO")
    bad = disc.api("PATCH", "/sources/local", json={"config": {"mirror_catalog_lineage": "yes"}})
    assert bad.status_code == 422 and "true or false" in bad.text


def test_the_standalone_catalog_mirror_kind_still_works_next_to_the_local_source(disc):
    recorded_flow(disc)
    disc.api("PATCH", "/sources/local", json={"config": {"mirror_catalog_lineage": False}})
    disc.scan()
    src = mirror(disc)
    row = disc.scan(src["id"])
    assert row["status"] == "succeeded" and row["stats"]["counters"]["catalog_edges"] == 1
    assert {e["type"] for e in disc.edges(source_id=src["id"])} >= {"READS_FROM", "WRITES_TO", "TRANSFORMS"}
