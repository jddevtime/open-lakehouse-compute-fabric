# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Run the large-tenant workload of ``test_performance`` in a FRESH process and print its memory figures as one JSON line.

A process-wide peak (``ru_maxrss``) never goes down, so measured inside the pytest process it would include everything that ran before (other modules, other tests).  This
module is started as ``python -m tests.intelligence.system.memory_probe``: build the platform, load the 2000-table graph, run the repeated queries, report ``peak_bytes``
(``None`` when the platform has no peak figure) and ``grown_bytes`` (RSS growth over 100 repeated queries after a warm-up).
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import psutil

from tests.intelligence.system.conftest import build_platform
from tests.intelligence.system.test_performance import LAYERS, _build
from tests.peak_memory import peak_rss_bytes


def measure() -> dict[str, int | None]:
    """Load the graph, query it repeatedly and return the peak and the growth in bytes."""
    with tempfile.TemporaryDirectory(prefix="olcf-memprobe-") as d, build_platform(Path(d), ["olcf.api.core.module", "olcf.intelligence.module"]) as p:
        svc = p.intel
        source = next(s["id"] for s in p.get("/v1/intelligence/sources")["items"] if s["kind"] == "local")
        scan = svc.engine.begin_scan(source, "incremental", "manual")
        stats = svc.engine.apply_facts(_build(), scan_id=scan, source_id=source)
        assert not stats.errors, stats.error_samples
        svc.engine.finish_scan(scan, "succeeded", stats.as_dict())
        tail, hub = p.asset("table", f"t_{LAYERS - 1:02d}_001"), p.asset("table", "t_00_000")
        for _ in range(3):                                                                  # warm-up: caches and allocator arenas settle
            p.lineage(tail, direction="up", depth=10)
            p.lineage(hub, direction="down", depth=10)
        before = psutil.Process().memory_info().rss
        for _ in range(25):
            p.lineage(tail, direction="up", depth=10)
            p.lineage(hub, direction="down", depth=10)
            p.get("/v1/intelligence/assets", q="t_05_0")
            p.get("/v1/intelligence/summary")
        grown = psutil.Process().memory_info().rss - before
        return {"peak_bytes": peak_rss_bytes(), "grown_bytes": grown}


if __name__ == "__main__":
    print(json.dumps(measure()))  # noqa: T201
