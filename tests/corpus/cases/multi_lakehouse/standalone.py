# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Standalone equivalent of the multi_lakehouse notebook: plain Python over two directories."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any


def main(roots: dict[str, str], params: dict[str, Any], spark: Any = None) -> dict[str, Any]:
    bronze, silver = Path(roots["default"]), Path(roots["silver"])
    with open(bronze / "Files" / "orders.csv", newline="") as fh:
        rows = list(csv.DictReader(fh))
    totals: dict[str, float] = {}
    for r in rows:
        if r["status"] == "paid" and r["customer_id"]:
            totals[r["customer_id"]] = round(totals.get(r["customer_id"], 0) + int(r["qty"]) * float(r["price"]), 2)
    out = silver / "Files" / "curated"
    out.mkdir(parents=True, exist_ok=True)
    (out / "totals.json").write_text(json.dumps(totals, sort_keys=True))
    return {"lakehouses": ["bronze", "silver"], "default": "bronze", "totals": json.loads((out / "totals.json").read_text()), "silver_ls": sorted(p.name for p in out.iterdir())}
