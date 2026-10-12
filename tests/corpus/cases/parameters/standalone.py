# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Standalone equivalent of the parameters notebook: defaults overridden by the run parameters."""

from __future__ import annotations

import datetime
from typing import Any


def main(roots: dict[str, str], params: dict[str, Any], spark: Any = None) -> dict[str, Any]:
    p = {"run_date": "2026-01-01", "limit": 10, "dry_run": True, "ratio": 0.5, "tags": []}
    p.update(params)
    d = datetime.date.fromisoformat(p["run_date"])
    return {"run_date": p["run_date"], "weekday": d.strftime("%A"), "limit": p["limit"], "limit_type": type(p["limit"]).__name__, "dry_run": p["dry_run"], "ratio": p["ratio"],
            "tags": p["tags"], "first": list(range(p["limit"]))}
