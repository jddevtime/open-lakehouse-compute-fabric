# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Iceberg discovery, metadata, time travel and graceful degradation, using a table created by a real pyiceberg SQL catalog."""

from __future__ import annotations

import time
from pathlib import Path

import pyarrow as pa
import pytest

pyiceberg = pytest.importorskip("pyiceberg")


@pytest.fixture()
def ice(dc, tmp_path):
    """Lakehouse ``lhi`` with an Iceberg table ``Tables/sales`` (3 snapshots, partitioned by region) made by PyIceberg."""
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.types import LongType, NestedField, StringType

    dc.workspace("ws1")
    lh = dc.lakehouse("ws1", "lhi")
    root = Path(dc.ctx.service("storage").get_lakehouse(lh["id"]).root_uri.removeprefix("file://"))
    cat = SqlCatalog("t", uri=f"sqlite:///{tmp_path}/cat.db", warehouse=f"file://{tmp_path}/wh")
    cat.create_namespace("db")
    schema = Schema(NestedField(1, "id", LongType(), required=True), NestedField(2, "region", StringType()), NestedField(3, "amount", LongType()))
    spec = PartitionSpec(PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="region"))
    tbl = cat.create_table("db.sales", schema=schema, partition_spec=spec, location=f"file://{root}/Tables/sales")
    snaps = []
    for rows in ([(1, "eu", 10), (2, "us", 20)], [(3, "eu", 30)], [(4, "us", 40), (5, "us", 50)]):
        arrow_schema = pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("region", pa.string()), pa.field("amount", pa.int64())])
        tbl.append(pa.table({"id": [r[0] for r in rows], "region": [r[1] for r in rows], "amount": [r[2] for r in rows]}, schema=arrow_schema))
        tbl = cat.load_table("db.sales")
        snaps.append((tbl.current_snapshot().snapshot_id, int(time.time() * 1000)))
        time.sleep(0.02)
    return lh, snaps


def test_iceberg_table_is_discovered_and_described(dc, ice):
    lh, snaps = ice
    h = dc.admin
    items = dc.get(f"/v1/lakehouses/{lh['id']}/tables", headers=h).json()["items"]
    assert [(t["name"], t["format"]) for t in items] == [("sales", "iceberg")]
    d = dc.get(f"/v1/lakehouses/{lh['id']}/tables/sales", headers=h).json()
    assert d["format"] == "iceberg" and d["format_version"] in (1, 2)
    assert [(c["name"], c["type"], c["nullable"]) for c in d["schema"]] == [("id", "long", False), ("region", "string", True), ("amount", "long", True)]
    assert d["partition_columns"] == ["region"] and d["partition_spec"][0]["transform"] == "identity"
    assert d["current_snapshot_id"] == snaps[-1][0] and len(d["snapshots"]) == 3 and d["num_rows"] == 5
    assert [s["snapshot_id"] for s in d["snapshots"]] == [s[0] for s in snaps]
    assert d["snapshots"][0]["operation"] == "append" and d["snapshots"][1]["parent_id"] == snaps[0][0]
    hist = dc.get(f"/v1/lakehouses/{lh['id']}/tables/sales/history", headers=h).json()["items"]
    assert [x["snapshot_id"] for x in hist] == [s[0] for s in reversed(snaps)]


def test_iceberg_preview_and_time_travel(dc, ice):
    lh, snaps = ice
    h = dc.admin
    p = f"/v1/lakehouses/{lh['id']}/tables/sales/preview"
    cur = dc.get(p, headers=h).json()
    assert len(cur["rows"]) == 5
    assert {c["name"] for c in cur["columns"]} == {"id", "region", "amount"}
    first = dc.get(p, params={"snapshot_id": snaps[0][0]}, headers=h).json()
    assert sorted(r[0] for r in first["rows"]) == [1, 2]
    second = dc.get(p, params={"snapshot_id": snaps[1][0]}, headers=h).json()
    assert sorted(r[0] for r in second["rows"]) == [1, 2, 3]
    # as_of resolves to the newest snapshot at/before the instant: far future -> current, 1999 -> error
    future = dc.get(p, params={"as_of": "2999-01-01T00:00:00Z"}, headers=h)
    assert future.status_code == 200 and len(future.json()["rows"]) == 5
    assert dc.get(p, params={"snapshot_id": 12345}, headers=h).status_code == 400
    assert dc.get(p, params={"as_of": "1999-01-01T00:00:00Z"}, headers=h).status_code == 400
    assert dc.get(p, params={"as_of": "garbage"}, headers=h).status_code == 400
    cols = dc.get(p, params={"columns": "id,amount", "rows": 2}, headers=h).json()
    assert [c["name"] for c in cols["columns"]] == ["id", "amount"] and len(cols["rows"]) == 2


def test_iceberg_writes_are_refused_with_guidance(dc, ice):
    lh, _ = ice
    h = dc.admin
    for call in (dc.post(f"/v1/lakehouses/{lh['id']}/tables/sales/optimize", json={}, headers=h),
                 dc.post(f"/v1/lakehouses/{lh['id']}/tables/sales/vacuum", json={}, headers=h)):
        assert call.status_code in (400, 409, 422, 501), call.text
        assert "iceberg" in call.text.lower() or "delta" in call.text.lower()


def test_metadata_is_available_without_pyiceberg(dc, ice, monkeypatch):
    lh, snaps = ice
    import olcf.storage.iceberg as ice_mod

    monkeypatch.setattr(ice_mod, "pyiceberg_available", lambda: False)
    real_import = __builtins__["__import__"] if isinstance(__builtins__, dict) else __builtins__.__import__

    def fake_import(name, *a, **k):
        if name == "pyiceberg" or name.startswith("pyiceberg."):
            raise ImportError("simulated: pyiceberg not installed")
        return real_import(name, *a, **k)

    monkeypatch.setattr("builtins.__import__", fake_import)
    h = dc.admin
    d = dc.get(f"/v1/lakehouses/{lh['id']}/tables/sales", headers=h)
    assert d.status_code == 200 and d.json()["pyiceberg_available"] is False and len(d.json()["schema"]) == 3
    r = dc.get(f"/v1/lakehouses/{lh['id']}/tables/sales/preview", headers=h)
    assert r.status_code == 503, r.text
    body = r.json()
    msg = str(body)
    assert "PyIceberg is not installed" in msg and "olcf[iceberg]" in msg


def test_iceberg_metadata_helpers(ice, dc):
    lh, snaps = ice
    from olcf.storage import iceberg as ice_mod

    svc = dc.ctx.service("storage")
    fs = svc.filesystem(svc.get_lakehouse(lh["id"]))
    meta = ice_mod.find_metadata_file(fs, "Tables/sales")
    assert meta and meta.endswith(".metadata.json") and ice_mod.is_iceberg_dir(fs, "Tables/sales")
    assert not ice_mod.is_iceberg_dir(fs, "Tables")
    s = ice_mod.summarize(ice_mod.read_metadata(fs, meta))
    assert s["current_snapshot_id"] == snaps[-1][0]
    assert ice_mod.snapshot_for(s, snapshot_id=snaps[0][0]) == snaps[0][0]
    assert ice_mod.snapshot_for(s) is None
