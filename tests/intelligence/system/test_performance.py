# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Performance and size sanity of the asset graph at the scale of a large tenant: 2000 tables, 20000 columns, about 5000 edges, built by the real engine in one scan.

The graph is a layered pipeline (11 layers of 130 tables, every table of a layer written by a notebook that reads two tables of the layer before, so a depth-10 walk has many paths)
plus one hub table read by 570 notebooks that each write a table of their own (a node cap and wide fan-out).

**Bounds.** The numbers below were measured on the development VM (SQLite, one process, ``MEASURED``) and every bound is at least five times the measurement, rounded up to a round
figure, so a slow shared runner passes and an accidentally quadratic query does not. They are sanity bounds, not benchmarks. SQLite only: the same suite against PostgreSQL has
not been run (``tests/intelligence`` has a ``postgres`` parametrisation for the storage layer but this scale test is not part of it).

======================================  ==========  =======
what                                    measured    bound
======================================  ==========  =======
load 23871 assets + 5040 edges (apply)  about 105 s  600 s
lineage up, depth 10, 121 nodes         0.07 s      2 s
lineage down from the hub (capped)      0.11 s      2 s
origin (depth 10, path cap)             0.24 s      3 s
impact of the hub                       0.12 s      2 s
search (50 hits)                        0.05 s      1 s
summary                                 0.02 s      1 s
diff of a small scan                    0.04 s      1 s
diff of the loading scan (everything)   see test    10 s
peak RSS, fresh process, load+queries   about 160 MB 1.5 GB
======================================  ==========  =======
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import func, select

from olcf.intelligence import model as M
from olcf.intelligence.queries import MAX_PATHS
from olcf.intelligence.schema import assets, edges
from tests.intelligence.conftest import Builder
from tests.intelligence.system.conftest import Platform, build_platform

pytestmark = pytest.mark.timeout(1500)

LAYERS, WIDTH, FAN, COLS = 11, 130, 570, 10
TABLES = LAYERS * WIDTH + FAN
NOTEBOOKS = (LAYERS - 1) * WIDTH + FAN
EDGES = (LAYERS - 1) * WIDTH * 3 + FAN * 2
LOAD_BOUND, WALK_BOUND, ORIGIN_BOUND, SEARCH_BOUND, DIFF_BOUND, FULL_DIFF_BOUND, RSS_BOUND = 600.0, 2.0, 3.0, 1.0, 1.0, 10.0, 2.0 * 1024 ** 3
CAP = 500


@dataclass
class Big:
    p: Platform
    source_id: str
    empty_scan: str
    load_scan: str
    load_seconds: float
    stats: dict[str, Any]


def _build() -> M.Facts:
    b = Builder(None)
    lh = b.lakehouse("itm_perf", "perf_lh")
    out: list[M.AssetFact] = [lh]
    tabs: dict[tuple[int, int], M.AssetFact] = {}

    def table(name: str) -> M.AssetFact:
        t = b.table(lh, name, [(f"c{k}", "double") for k in range(COLS)])
        out.append(t)
        out.extend(b.column(t, f"c{k}", "double") for k in range(COLS))
        return t

    for layer in range(LAYERS):
        for i in range(WIDTH):
            tabs[layer, i] = table(f"t_{layer:02d}_{i:03d}")
    fan = [table(f"fan_{i:04d}") for i in range(FAN)]
    links: list[M.EdgeFact] = []
    for layer in range(1, LAYERS):
        for i in range(WIDTH):
            nb = b.notebook(f"itm_nb_{layer}_{i}", f"nb_{layer:02d}_{i:03d}")
            out.append(nb)
            links += [b.edge(nb, tabs[layer - 1, i], "READS_FROM"), b.edge(nb, tabs[layer - 1, (i + 1) % WIDTH], "READS_FROM"), b.edge(nb, tabs[layer, i], "WRITES_TO")]
    for i, t in enumerate(fan):
        nb = b.notebook(f"itm_nbf_{i}", f"nbf_{i:04d}")
        out.append(nb)
        links += [b.edge(nb, tabs[0, 0], "READS_FROM"), b.edge(nb, t, "WRITES_TO")]
    return M.Facts(out, links)


@pytest.fixture(scope="module")
def big(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Big]:
    with build_platform(tmp_path_factory.mktemp("perf"), ["olcf.api.core.module", "olcf.intelligence.module"]) as p:
        svc = p.intel
        source = next(s["id"] for s in p.get("/v1/intelligence/sources")["items"] if s["kind"] == "local")
        empty = svc.engine.begin_scan(source, "incremental", "manual")
        svc.engine.finish_scan(empty, "succeeded", {})
        facts = _build()
        scan = svc.engine.begin_scan(source, "incremental", "manual")
        t0 = time.perf_counter()
        stats = svc.engine.apply_facts(facts, scan_id=scan, source_id=source)
        seconds = time.perf_counter() - t0
        assert not stats.errors, stats.error_samples
        svc.engine.finish_scan(scan, "succeeded", stats.as_dict())
        yield Big(p, source, empty, scan, seconds, stats.as_dict())


def _timed(fn: Any, bound: float, what: str) -> Any:
    t0 = time.perf_counter()
    out = fn()
    took = time.perf_counter() - t0
    assert took < bound, f"{what} took {took:.2f} s (bound {bound} s)"
    return out


def _names(doc: dict[str, Any], type_: str | None = None) -> set[str]:
    return {n["name"] for n in doc["nodes"] if type_ is None or n["type"] == type_}


# ------------------------------------------------------------------------------------------------------------------------ size and load
def test_the_graph_has_the_size_the_suite_promises_and_it_loaded_in_time(big: Big) -> None:
    p = big.p
    assert (big.stats["assets_created"], big.stats["edges_created"]) == (1 + TABLES * (1 + COLS) + NOTEBOOKS, EDGES)
    summary = p.get("/v1/intelligence/summary")["assets"]
    assert summary["by_type"]["table"] == TABLES == 2000 and summary["by_type"]["column"] == TABLES * COLS == 20000 and summary["by_type"]["notebook"] == NOTEBOOKS
    with p.ctx.db.connect() as c:
        assert c.execute(select(func.count()).select_from(edges).where(edges.c.status == "active")).scalar_one() == EDGES == 5040
        assert c.execute(select(func.count()).select_from(assets).where(assets.c.status == "placeholder")).scalar_one() == 0
    assert big.load_seconds < LOAD_BOUND, f"loading took {big.load_seconds:.0f} s"


# ------------------------------------------------------------------------------------------------------------------------ lineage
def test_depth_10_lineage_reaches_the_first_layer_within_the_bound(big: Big) -> None:
    p = big.p
    tail = p.asset("table", f"t_{LAYERS - 1:02d}_000")
    r = _timed(lambda: p.http.get(f"/v1/intelligence/assets/{tail}/lineage", params={"direction": "up", "depth": 10}, headers=p.admin), WALK_BOUND, "lineage up depth 10")
    assert r.status_code == 200
    doc = r.json()
    assert doc["truncated"] is False and doc["stats"]["max_nodes"] == CAP
    layers = {n["name"][2:4] for n in doc["nodes"] if n["type"] == "table"}
    assert layers == {f"{k:02d}" for k in range(LAYERS)}                                  # every layer from the tail back to the sources
    assert {n["name"] for n in doc["nodes"] if n["type"] == "table" and n["name"].startswith("t_00_")} == {f"t_00_{i:03d}" for i in range(11)}   # a window of widening width
    assert len(r.content) < 300_000                                                       # an answer is a few hundred KB at most
    assert {e["confidence"] for e in doc["edges"]} == {"derived"}


def test_a_wide_fan_out_is_cut_at_the_node_cap_and_says_so(big: Big) -> None:
    p = big.p
    hub = p.asset("table", "t_00_000")
    doc = _timed(lambda: p.lineage(hub, direction="down", depth=10), WALK_BOUND, "lineage down from the hub")
    assert doc["truncated"] is True
    assert len(doc["nodes"]) <= CAP == doc["stats"]["max_nodes"] and len(doc["nodes"]) >= CAP - 1
    ids = {n["id"] for n in doc["nodes"]}
    assert all(e["from"] in ids and e["to"] in ids for e in doc["edges"])                 # a cut graph has no dangling edge
    # the other answers built on the walk carry the flag too (never a silent partial answer)
    # impact counts with a bounded walk of its own (intelligence.max_impact_nodes), so its totals are real numbers and only the listed items are cut at the render cap
    impact = _timed(lambda: p.impact(hub), WALK_BOUND, "impact of the hub")
    assert impact["truncated"] is False and impact["items_truncated"] is True
    assert impact["total"] > CAP and sum(g["count"] for g in impact["by_type"].values()) == impact["total"] and len(impact["nodes"]) <= CAP
    origin = p.origin(p.asset("table", f"t_{LAYERS - 1:02d}_000"))
    assert origin["truncated"] is True and len(origin["paths"]) == MAX_PATHS            # 2^10 diamond paths: the path list is capped, and says so


def test_the_cap_is_a_setting_and_a_larger_one_returns_the_whole_fan_out(big: Big) -> None:
    p = big.p
    hub = p.asset("table", "t_00_000")
    r = p.http.put("/v1/admin/settings/intelligence.max_lineage_nodes", json={"value": 6000}, headers=p.admin)
    assert r.status_code == 200, r.text
    try:
        doc = _timed(lambda: p.lineage(hub, direction="down", depth=1), WALK_BOUND * 2, "lineage down, cap 6000")
        assert doc["truncated"] is False and doc["stats"]["max_nodes"] == 6000
        assert {f"fan_{i:04d}" for i in range(FAN)} <= _names(doc, "table")
        impact = p.impact(hub, depth=1)
        assert impact["truncated"] is False and impact["total"] >= FAN
    finally:
        p.http.put("/v1/admin/settings/intelligence.max_lineage_nodes", json={"value": CAP}, headers=p.admin)
    assert p.lineage(hub, direction="down", depth=1)["stats"]["max_nodes"] == CAP


def test_column_lineage_of_a_wide_table_is_bounded(big: Big) -> None:
    p = big.p
    doc = _timed(lambda: p.lineage(p.asset("table", "t_05_010"), direction="up", level="column", depth=10), WALK_BOUND, "column lineage")
    assert len(doc["nodes"]) <= CAP and doc["stats"]["max_nodes"] == CAP                   # no TRANSFORMS edges were reported in this graph: the columns are roots without paths


# ------------------------------------------------------------------------------------------------------------------------ search and summary
def test_search_summary_and_paging_stay_fast_on_2000_tables(big: Big) -> None:
    p = big.p
    hits = _timed(lambda: p.get("/v1/intelligence/assets", q="t_05_0", type="table"), SEARCH_BOUND, "search")["items"]
    assert len(hits) == 50 and all("t_05_0" in h["name"] for h in hits)
    assert _timed(lambda: p.get("/v1/intelligence/assets", q="fan_0123", type="table"), SEARCH_BOUND, "exact search")["items"][0]["name"] == "fan_0123"
    summary = _timed(lambda: p.get("/v1/intelligence/summary"), SEARCH_BOUND, "summary")
    assert summary["assets"]["total"] >= TABLES * (1 + COLS)
    seen: list[str] = []
    cursor: str | None = None
    t0 = time.perf_counter()
    while True:
        page = p.get("/v1/intelligence/assets", type="table", limit=200, cursor=cursor)
        seen += [a["id"] for a in page["items"]]
        cursor = page.get("next_cursor")
        if not cursor:
            break
    assert time.perf_counter() - t0 < SEARCH_BOUND * 10
    assert len(seen) == len(set(seen)) == TABLES                                            # every table exactly once through the cursor


# ------------------------------------------------------------------------------------------------------------------------ diffs
def test_the_diff_of_the_loading_scan_is_bounded_and_flagged(big: Big) -> None:
    """Everything is new in this diff: the lists are cut at ``limit`` and the answer is flagged. (The summary counts are of what was fetched, ``limit + 1`` at most - see the
    report: they are not totals when ``truncated`` is true.)"""
    p = big.p
    doc = _timed(lambda: p.get("/v1/intelligence/snapshots/diff", **{"from": big.empty_scan, "to": big.load_scan}, limit=500), FULL_DIFF_BOUND, "full diff")
    assert doc["truncated"] is True and len(doc["assets"]["added"]) == 500 and len(doc["edges"]["added"]) == 500
    assert doc["summary"]["assets_added"] >= 500 and doc["summary"]["assets_removed"] == 0


def test_a_small_scan_on_a_large_graph_has_an_exact_small_diff(big: Big) -> None:
    p, svc, b = big.p, big.p.intel, Builder(None)
    lh = b.lakehouse("itm_perf", "perf_lh")
    changed = b.table(lh, "t_03_005", [(f"c{k}", "double") for k in range(COLS)] + [("extra", "string")])
    new_tables = [b.table(lh, f"delta_{i:02d}", [(f"c{k}", "double") for k in range(COLS)]) for i in range(20)]
    new_columns = [b.column(t, f"c{k}", "double") for t in new_tables for k in range(COLS)]
    notebooks = [b.notebook(f"itm_nbd_{i}", f"nbd_{i:02d}") for i in range(20)]
    reader = M.AssetRef.make("table", {"lakehouse_table": "itm_perf:t_00_001"}, "t_00_001")
    links = [x for i, nb in enumerate(notebooks) for x in (b.edge(nb, reader, "READS_FROM"), b.edge(nb, new_tables[i], "WRITES_TO"))]
    sid = svc.engine.begin_scan(big.source_id, "incremental", "manual")
    t0 = time.perf_counter()
    stats = svc.engine.apply_facts(M.Facts([lh, changed, *new_tables, *new_columns, *notebooks], links), scan_id=sid, source_id=big.source_id)
    svc.engine.finish_scan(sid, "succeeded", stats.as_dict())
    assert time.perf_counter() - t0 < 60 and not stats.errors
    doc = _timed(lambda: p.get("/v1/intelligence/snapshots/diff", **{"from": big.load_scan, "to": sid}), DIFF_BOUND, "small diff")
    assert doc["summary"] == {"assets_added": 20 + 200 + 20, "assets_removed": 0, "assets_changed": 1, "edges_added": 40, "edges_removed": 0, "edges_changed": 0}
    assert doc["truncated"] is False
    entry = doc["assets"]["changed"][0]
    assert entry["name"] == "t_03_005" and entry["changes"]["columns_added"] == ["extra"]


# ------------------------------------------------------------------------------------------------------------------------ memory
ROOT = Path(__file__).resolve().parents[3]


def _run_memory_probe() -> dict[str, Any]:
    """The workload in a fresh interpreter: its peak RSS is the workload's, not the high-water mark of everything pytest ran before (see ``memory_probe``)."""
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(ROOT / "src"), str(ROOT / "sdk" / "fabric"), str(ROOT)])}
    r = subprocess.run([sys.executable, "-m", "tests.intelligence.system.memory_probe"], capture_output=True, text=True, env=env, cwd=str(ROOT), timeout=1200, check=False)  # noqa: S603
    assert r.returncode == 0, f"the memory probe failed (exit {r.returncode}):\n{r.stderr[-3000:]}"
    return json.loads(r.stdout.strip().splitlines()[-1])  # type: ignore[no-any-return]


def test_memory_stays_bounded_and_repeated_queries_do_not_grow_it() -> None:
    out = _run_memory_probe()
    grown = out["grown_bytes"]
    assert grown < 150 * 1024 ** 2, f"RSS grew by {grown / 1024 ** 2:.0f} MB over 100 repeated queries"
    peak = out["peak_bytes"]
    if peak is None:
        pytest.skip(f"no peak-RSS figure on {sys.platform}: neither the resource module nor psutil's peak_wset is available (the growth assertion above did run)")
    assert peak < RSS_BOUND, f"peak RSS {peak / 1024 ** 2:.0f} MB"
