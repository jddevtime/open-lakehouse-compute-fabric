# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Standalone equivalent of the delta_merge notebook using delta-rs (the ``deltalake`` package), so the golden does not depend on Spark or the Delta jars."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any


def main(roots: dict[str, str], params: dict[str, Any], spark: Any = None) -> dict[str, Any]:
    import pyarrow as pa
    from deltalake import DeltaTable, write_deltalake

    root = Path(roots["default"])
    with open(root / "Files" / "orders.csv", newline="") as fh:
        rows = [(int(r["order_id"]), r["status"]) for r in csv.DictReader(fh)]
    schema = pa.schema([("order_id", pa.int32()), ("status", pa.string())])
    table_path = str(root / "Tables" / "order_status")
    write_deltalake(table_path, pa.Table.from_pylist([{"order_id": a, "status": b} for a, b in rows], schema=schema), mode="overwrite")
    updates = pa.Table.from_pylist([{"order_id": 3, "status": "paid"}, {"order_id": 9, "status": "paid"}], schema=schema)
    (DeltaTable(table_path).merge(source=updates, predicate="t.order_id = u.order_id", source_alias="u", target_alias="t")
     .when_matched_update({"status": "u.status"}).when_not_matched_insert_all().execute())
    void = pa.Table.from_pylist([{"order_id": 1, "status": "void"}], schema=schema)
    DeltaTable(table_path).merge(source=void, predicate="t.order_id = u.order_id", source_alias="u", target_alias="t").when_matched_update({"status": "u.status"}).execute()
    dt = DeltaTable(table_path)
    out = sorted(dt.to_pyarrow_table().to_pylist(), key=lambda r: r["order_id"])
    return {"rows": [[r["order_id"], r["status"]] for r in out], "versions": len(dt.history())}
