# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Standalone equivalent of the pipeline_invocation notebook: the child notebooks are plain functions."""

from __future__ import annotations

from typing import Any


def child_load(batch: int) -> dict[str, Any]:
    return {"batch": batch, "rows": batch * 10}


def main(roots: dict[str, str], params: dict[str, Any], spark: Any = None) -> dict[str, Any]:
    batch = params.get("batch", 1)
    child = child_load(batch)
    return {"batch": batch, "child": child, "ok": child["rows"] == batch * 10}
