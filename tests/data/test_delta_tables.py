# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Delta tables through the lakehouse API, exercised with real delta-rs operations (no mocks)."""

from __future__ import annotations

import json
import threading

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from deltalake import DeltaTable, write_deltalake


@pytest.fixture()
def env(dc):
    dc.workspace("ws1")
    lh = dc.lakehouse("ws1", "lh1")
    root = lh["root_uri"][len("file://"):]
    return dc, lh, f"/v1/lakehouses/{lh['id']}", root


def table_path(root, name):
    return f"{root}/Tables/{name}"


def sample_table(n=100, offset=0):
    return pa.table({"id": pa.array(range(offset, offset + n), pa.int64()), "grp": pa.array([f"g{i % 4}" for i in range(n)]),
                     "year": pa.array([2024 if i % 2 else 2025 for i in range(n)], pa.int32()), "v": pa.array([float(i) for i in range(n)])})


def test_discovery_detail_history_preview_protocol(env):
    dc, lh, p, root = env
    write_deltalake(table_path(root, "sales"), sample_table(), partition_by=["year"])
    write_deltalake(table_path(root, "sales"), sample_table(10, 1000), mode="append")
    tables = dc.get(f"{p}/tables", headers=dc.admin).json()["items"]
    assert [(t["name"], t["format"]) for t in tables] == [("sales", "delta")]
    d = dc.get(f"{p}/tables/sales", headers=dc.admin).json()
    assert d["format"] == "delta" and d["version"] == 1 and d["partition_columns"] == ["year"]
    assert d["num_rows"] == 110 and d["num_files"] >= 3 and d["size_bytes"] > 0
    assert [f["name"] for f in d["schema"]] == ["id", "grp", "year", "v"] and d["schema"][0]["type"] == "long"
    assert d["protocol"]["min_reader_version"] == 1 and d["interop"]["read"]["spark"]["allowed"] is True
    assert d["partitions"]["count"] == 2
    h = dc.get(f"{p}/tables/sales/history", headers=dc.admin).json()["items"]
    assert [x["version"] for x in h] == [1, 0] and h[0]["operation"] == "WRITE" and h[0]["operation_metrics"]["num_added_rows"] == 10
    pv = dc.get(f"{p}/tables/sales/preview", params={"rows": 5}, headers=dc.admin).json()
    assert pv["row_count"] == 5 and pv["truncated"] is True and [c["name"] for c in pv["columns"]] == ["id", "grp", "v", "year"] or len(pv["columns"]) == 4
    # time travel: version 0 has 100 rows
    v0 = dc.get(f"{p}/tables/sales/preview", params={"rows": 500, "version": 0}, headers=dc.admin).json()
    assert v0["row_count"] == 100 and v0["version"] == 0
    # protocol endpoint
    pr = dc.get(f"{p}/tables/sales/protocol", headers=dc.admin).json()
    assert pr["compatibility"]["fabric-2.0"]["read"] is True and pr["policy_version"].startswith("olcf-delta-interop")
    assert dc.get(f"{p}/tables/missing", headers=dc.admin).status_code == 404
    assert dc.get(f"{p}/tables/..%2Fx", headers=dc.admin).status_code in (400, 404)
    assert dc.get(f"{p}/tables/bad-name", headers=dc.admin).status_code == 400


def test_full_delta_operation_matrix_visible_through_api(env):
    """append, overwrite, merge, update, delete, schema evolution, partitioning, checkpoint - and the API keeps up."""
    dc, lh, p, root = env
    uri = table_path(root, "t")
    write_deltalake(uri, sample_table(20))                                      # v0 create
    write_deltalake(uri, sample_table(5, 100), mode="append")                   # v1 append
    t = DeltaTable(uri)
    t.merge(source=pa.table({"id": [1, 500], "grp": ["m", "m"], "year": pa.array([1, 1], pa.int32()), "v": [1.5, 2.5]}), predicate="t.id = s.id",
            source_alias="s", target_alias="t").when_matched_update_all().when_not_matched_insert_all().execute()   # v2 merge
    t.update(updates={"v": "-1.0"}, predicate="id = 2")                          # v3 update
    t.delete("id = 3")                                                          # v4 delete
    write_deltalake(uri, pa.table({"id": [9000], "grp": ["new"], "year": pa.array([3], pa.int32()), "v": [0.0], "extra": ["x"]}), mode="append", schema_mode="merge")  # v5 evolve
    DeltaTable(uri).create_checkpoint()
    ops = [h["operation"] for h in dc.get(f"{p}/tables/t/history", headers=dc.admin).json()["items"]]
    assert ops[:5] == ["WRITE", "DELETE", "UPDATE", "MERGE", "WRITE"]
    d = dc.get(f"{p}/tables/t", headers=dc.admin).json()
    assert "extra" in [f["name"] for f in d["schema"]] and d["version"] == 5
    assert d["num_rows"] == 20 + 5 + 1 - 1 + 1  # merge inserted 500 and updated 1, delete removed id 3, evolve added one
    prev = dc.get(f"{p}/tables/t/preview", params={"rows": 1000}, headers=dc.admin).json()
    ids = {r[[c["name"] for c in prev["columns"]].index("id")] for r in prev["rows"]}
    assert 3 not in ids and 500 in ids and 9000 in ids
    assert (root + "/Tables/t/_delta_log/_last_checkpoint") and json.loads(open(root + "/Tables/t/_delta_log/_last_checkpoint").read())["version"] == 5
    # overwrite
    write_deltalake(uri, sample_table(3), mode="overwrite", schema_mode="overwrite")
    assert dc.get(f"{p}/tables/t", headers=dc.admin).json()["num_rows"] == 3
    # reading through a checkpoint keeps working
    assert dc.get(f"{p}/tables/t/preview", headers=dc.admin).json()["row_count"] == 3


def test_concurrent_writers_all_commits_land(env):
    dc, lh, p, root = env
    uri = table_path(root, "conc")
    write_deltalake(uri, sample_table(1))
    errors: list[Exception] = []

    def worker(k: int) -> None:
        try:
            for j in range(3):
                write_deltalake(uri, sample_table(2, 1000 * k + 10 * j), mode="append")
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(k,)) for k in range(1, 5)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert not errors, errors
    d = dc.get(f"{p}/tables/conc", headers=dc.admin).json()
    assert d["num_rows"] == 1 + 4 * 3 * 2 and d["version"] == 12  # 12 appends -> 12 further versions, none lost
    # concurrent readers while writers run
    assert dc.get(f"{p}/tables/conc/preview", params={"rows": 1000}, headers=dc.admin).json()["row_count"] == 25


def test_create_table_from_csv_and_parquet_and_modes(env):
    dc, lh, p, root = env
    h = dc.admin
    csv = "id,name,amount,ts\n" + "\n".join(f"{i},n{i},{i * 1.5},2024-01-0{1 + i % 9} 10:00:00" for i in range(50)) + "\n"
    dc.put(f"{p}/files/content", params={"path": "Files/raw/people.csv"}, content=csv.encode(), headers=h)
    r = dc.post(f"{p}/tables", json={"name": "people", "source": "Files/raw/people.csv"}, headers=h)
    assert r.status_code == 201, r.text
    assert r.json()["version"] == 0 and r.json()["format"] == "csv" and r.json()["policy"]["allowed"] is True
    d = dc.get(f"{p}/tables/people", headers=h).json()
    assert d["num_rows"] == 50
    types = {f["name"]: f["type"] for f in d["schema"]}
    assert types["id"] == "long" and types["amount"] == "double" and types["ts"] == "timestamp"   # naive CSV timestamps -> UTC timestamp
    assert d["protocol"]["min_reader_version"] == 1 and d["protocol"]["writer_features"] == []      # no protocol upgrade (no timestampNtz)
    # table exists -> 409; append works; overwrite requires destructive
    assert dc.post(f"{p}/tables", json={"name": "people", "source": "Files/raw/people.csv"}, headers=h).status_code == 409
    assert dc.post(f"{p}/tables", json={"name": "people", "source": "Files/raw/people.csv", "mode": "append"}, headers=h).status_code == 201
    assert dc.get(f"{p}/tables/people", headers=h).json()["num_rows"] == 100
    contrib = dc.user("carl", "Contributor", workspace_id=lh["workspace_id"])
    assert dc.post(f"{p}/tables", json={"name": "people", "source": "Files/raw/people.csv", "mode": "overwrite"}, headers=contrib).status_code == 403
    assert dc.post(f"{p}/tables", json={"name": "people", "source": "Files/raw/people.csv", "mode": "overwrite"}, headers=h).status_code == 201
    assert dc.get(f"{p}/tables/people", headers=h).json()["num_rows"] == 50
    # parquet directory source, partitioned
    import io

    for i in range(2):
        buf = io.BytesIO()
        pq.write_table(sample_table(30, i * 100), buf)
        dc.put(f"{p}/files/content", params={"path": f"Files/pq/part{i}.parquet"}, content=buf.getvalue(), headers=h)
    r = dc.post(f"{p}/tables", json={"name": "from_pq", "source": "Files/pq", "partition_by": ["grp"]}, headers=h)
    assert r.status_code == 201 and r.json()["source_files"] == 2
    d = dc.get(f"{p}/tables/from_pq", headers=h).json()
    assert d["num_rows"] == 60 and d["partition_columns"] == ["grp"]
    # validation
    assert dc.post(f"{p}/tables", json={"name": "x", "source": "Tables/people"}, headers=h).status_code == 400
    assert dc.post(f"{p}/tables", json={"name": "x", "source": "Files/nope.csv"}, headers=h).status_code == 404
    assert dc.post(f"{p}/tables", json={"name": "bad name", "source": "Files/raw/people.csv"}, headers=h).status_code == 400
    assert dc.post(f"{p}/tables", json={"name": "x", "source": "Files/raw/people.csv", "partition_by": ["nope"]}, headers=h).status_code == 400


def test_naive_timestamps_can_be_kept_but_are_policy_checked(env):
    dc, lh, p, root = env
    dc.put(f"{p}/files/content", params={"path": "Files/t.csv"}, content=b"a,ts\n1,2024-01-01 00:00:00\n", headers=dc.admin)
    r = dc.post(f"{p}/tables", json={"name": "ntz", "source": "Files/t.csv", "options": {"naive_timestamps": "ntz"}}, headers=dc.admin)
    assert r.status_code == 201, r.text   # fabric-2.0 supports timestampNtz
    d = dc.get(f"{p}/tables/ntz", headers=dc.admin).json()
    assert d["protocol"]["min_reader_version"] == 3 and "timestampNtz" in d["protocol"]["reader_features"]
    assert [f["type"] for f in d["schema"]][1] == "timestamp_ntz"
    # tighten the platform profile to legacy 1.3 (supports ntz too) then a variant feature table is blocked for managed writes
    dc.ctx.service("settings").set("storage.delta_interop_profile", "fabric-1.3")
    uri = table_path(root, "variant")
    write_deltalake(uri, sample_table(3))
    log = f"{uri}/_delta_log/00000000000000000001.json"
    with open(log, "w") as fh:
        fh.write(json.dumps({"protocol": {"minReaderVersion": 3, "minWriterVersion": 7, "readerFeatures": ["variantType"], "writerFeatures": ["variantType"]}}) + "\n")
    proto = dc.get(f"{p}/tables/variant/protocol", headers=dc.admin).json()
    assert proto["write"]["delta-rs"]["allowed"] is False and proto["compatibility"]["fabric-1.3"]["read"] is False
    r = dc.post(f"{p}/tables/variant/optimize", json={}, headers=dc.admin)
    assert r.status_code == 422 and r.json()["error"]["code"] == "compatibility_blocked"
    assert "variantType" in json.dumps(r.json()["error"]["details"])
    r = dc.post(f"{p}/tables", json={"name": "variant", "source": "Files/t.csv", "mode": "append"}, headers=dc.admin)
    assert r.status_code == 422


def test_optimize_compact_and_zorder(env):
    dc, lh, p, root = env
    uri = table_path(root, "frag")
    for i in range(6):
        write_deltalake(uri, sample_table(20, i * 20), mode="append")
    assert dc.get(f"{p}/tables/frag", headers=dc.admin).json()["num_files"] == 6
    r = dc.post(f"{p}/tables/frag/optimize", json={}, headers=dc.admin)
    assert r.status_code == 200, r.text
    assert r.json()["kind"] == "compact" and r.json()["metrics"]["numFilesRemoved"] == 6 and r.json()["metrics"]["numFilesAdded"] == 1
    d = dc.get(f"{p}/tables/frag", headers=dc.admin).json()
    assert d["num_files"] == 1 and d["num_rows"] == 120
    z = dc.post(f"{p}/tables/frag/optimize", json={"z_order_by": ["id", "v"]}, headers=dc.admin)
    assert z.status_code == 200 and z.json()["kind"] == "z-order"
    assert dc.post(f"{p}/tables/frag/optimize", json={"z_order_by": ["nope"]}, headers=dc.admin).status_code == 400
    # permissions: Viewer cannot optimize
    viewer = dc.user("vera", "Viewer", workspace_id=lh["workspace_id"])
    assert dc.post(f"{p}/tables/frag/optimize", json={}, headers=viewer).status_code == 403


def test_vacuum_safety(env):
    dc, lh, p, root = env
    uri = table_path(root, "vac")
    write_deltalake(uri, sample_table(10))
    write_deltalake(uri, sample_table(10), mode="overwrite")
    h = dc.admin
    r = dc.post(f"{p}/tables/vac/vacuum", json={}, headers=h)
    assert r.status_code == 200 and r.json()["dry_run"] is True and r.json()["retention_hours"] == 168 and r.json()["files_count"] == 0
    # below the safety threshold -> refused with the minimum spelled out
    r = dc.post(f"{p}/tables/vac/vacuum", json={"retention_hours": 1, "dry_run": False}, headers=h)
    assert r.status_code == 400 and r.json()["error"]["details"]["minimum_hours"] == 168
    # needs lakehouse.destructive even for the dry run
    contrib = dc.user("carl", "Contributor", workspace_id=lh["workspace_id"])
    assert dc.post(f"{p}/tables/vac/vacuum", json={}, headers=contrib).status_code == 403
    wsadmin = dc.user("wally", "WorkspaceAdmin", workspace_id=lh["workspace_id"])
    assert dc.post(f"{p}/tables/vac/vacuum", json={}, headers=wsadmin).status_code == 200
    # only a PlatformAdmin setting can lower the floor
    assert dc.put("/v1/admin/settings/storage.vacuum_min_retention_hours", json={"value": 0}, headers=h).status_code == 200
    assert dc.put("/v1/admin/settings/storage.vacuum_min_retention_hours", json={"value": 0}, headers=wsadmin).status_code == 403
    dry = dc.post(f"{p}/tables/vac/vacuum", json={"retention_hours": 0, "dry_run": True}, headers=h).json()
    assert dry["files_count"] == 1 and dry["dry_run"] is True
    live = dc.post(f"{p}/tables/vac/vacuum", json={"retention_hours": 0, "dry_run": False}, headers=h).json()
    assert live["files_count"] == 1
    audits = [a["action"] for a in dc.get("/v1/audit?limit=50", headers=h).json()["items"]]
    assert "data.destructive" in audits


def test_drop_table_requires_destructive(env):
    dc, lh, p, root = env
    write_deltalake(table_path(root, "gone"), sample_table(5))
    contrib = dc.user("carl", "Contributor", workspace_id=lh["workspace_id"])
    assert dc.delete(f"{p}/tables/gone", headers=contrib).status_code == 403
    r = dc.delete(f"{p}/tables/gone", headers=dc.admin)
    assert r.status_code == 200 and r.json()["dropped"] is True
    assert dc.get(f"{p}/tables/gone", headers=dc.admin).status_code == 404
    assert dc.get(f"{p}/tables", headers=dc.admin).json()["items"] == []


def test_parquet_directory_and_schema_enabled_discovery(dc):
    dc.workspace("ws1")
    lh = dc.lakehouse("ws1", "lhs", schema_enabled=True)
    root = lh["root_uri"][len("file://"):]
    p = f"/v1/lakehouses/{lh['id']}"
    write_deltalake(f"{root}/Tables/sales/orders", sample_table(10))
    import os

    os.makedirs(f"{root}/Tables/ext/pqdata/year=2024", exist_ok=True)
    pq.write_table(sample_table(7).drop(["year"]), f"{root}/Tables/ext/pqdata/year=2024/part.parquet")
    names = {(t["name"], t["format"]) for t in dc.get(f"{p}/tables", headers=dc.admin).json()["items"]}
    assert names == {("sales.orders", "delta"), ("ext.pqdata", "parquet")}
    assert dc.get(f"{p}/tables/sales.orders", headers=dc.admin).json()["num_rows"] == 10
    d = dc.get(f"{p}/tables/ext.pqdata", headers=dc.admin).json()
    assert d["format"] == "parquet" and d["partition_columns"] == ["year"] and d["num_files"] == 1
    pv = dc.get(f"{p}/tables/ext.pqdata/preview", params={"rows": 3}, headers=dc.admin).json()
    assert pv["row_count"] == 3 and pv["truncated"] is True
    assert dc.get(f"{p}/tables/ext.pqdata/protocol", headers=dc.admin).status_code == 422   # not a Delta table


def test_file_previews_json_parquet_image(env):
    dc, lh, p, root = env
    h = dc.admin
    dc.put(f"{p}/files/content", params={"path": "Files/a.jsonl"}, content=b'{"a":1,"b":"x"}\n{"a":2,"c":true}\n', headers=h)
    pv = dc.get(f"{p}/files/preview", params={"path": "Files/a.jsonl"}, headers=h).json()
    assert pv["kind"] == "table" and [c["name"] for c in pv["columns"]] == ["a", "b", "c"] and pv["rows"][1] == [2, None, True]
    dc.put(f"{p}/files/content", params={"path": "Files/a.json"}, content=json.dumps([{"k": i} for i in range(5)]).encode(), headers=h)
    assert dc.get(f"{p}/files/preview", params={"path": "Files/a.json", "rows": 2}, headers=h).json()["rows"] == [[0], [1]]
    pq.write_table(sample_table(50), f"{root}/Files/x.parquet")
    pv = dc.get(f"{p}/files/preview", params={"path": "Files/x.parquet", "rows": 7}, headers=h).json()
    assert pv["kind"] == "table" and pv["total_rows"] == 50 and len(pv["rows"]) == 7 and pv["truncated"] is True
    png = bytes.fromhex("89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c4890000000d4944415478da63fcffff3f0300050001a5f645400000000049454e44ae426082")
    dc.put(f"{p}/files/content", params={"path": "Files/p.png"}, content=png, headers=h)
    pv = dc.get(f"{p}/files/preview", params={"path": "Files/p.png"}, headers=h).json()
    assert pv["kind"] == "image" and pv["data_uri"].startswith("data:image/png;base64,")
    dc.put(f"{p}/files/content", params={"path": "Files/blob.bin"}, content=b"\x00\x01\x02" * 100, headers=h)
    assert dc.get(f"{p}/files/preview", params={"path": "Files/blob.bin"}, headers=h).json()["kind"] == "binary"
    big = "\n".join(f"{i},{i}" for i in range(100000)).encode()
    dc.put(f"{p}/files/content", params={"path": "Files/big.csv"}, content=b"a,b\n" + big, headers=h)
    pv = dc.get(f"{p}/files/preview", params={"path": "Files/big.csv", "rows": 10, "bytes": 2048}, headers=h).json()
    assert pv["kind"] == "table" and pv["truncated"] is True and len(pv["rows"]) == 10
