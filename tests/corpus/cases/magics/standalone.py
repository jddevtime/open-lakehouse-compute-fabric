# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Standalone equivalent of the magics notebook: the %run-ed helper notebook is a plain module-level definition."""

from __future__ import annotations

from typing import Any


def tax(amount: float, rate: float = 0.2) -> float:
    return round(amount * (1 + rate), 2)


def main(roots: dict[str, str], params: dict[str, Any], spark: Any = None) -> dict[str, Any]:
    prices = [10, 20.5, 99.99]
    return {"values": [tax(p) for p in prices], "constant": 42}
