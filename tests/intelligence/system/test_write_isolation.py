# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Review item R01: a heavy scan must not hold up the platform's other writers (event publication, job and session state) on the default SQLite store.

SQLite has one writer. Before the fix a scan that committed one transaction after another starved every other writer: no transaction was longer than about 50 ms, yet while a 4,000
asset scan ran, one of 29 event publishes took 4.2 seconds (the waiting writer polls from a busy handler that sleeps up to 100 ms, and the scan re-took the lock within microseconds).
The graph writer now pauses after ``intelligence.write_batch_max_ms`` of continuous writing and the event bus persists with a bounded wait.

**Bounds** (SQLite, one process, the real application and engine; measured on the development VM): while a scan of about 3,400 assets and 800 edges is applied by the engine, a probe
thread publishes an event and writes a core row every 20 ms.

=================================  ===========  =======
what                               measured     bound
=================================  ===========  =======
event publish, worst of the run    about 0.3 s  2.5 s
core row write (kv), worst         about 0.4 s  3.0 s
scan completes, no errors          yes          yes
=================================  ===========  =======

**Residual limit.** On a shared SQLite file the platform's other writers still wait, for up to ``write_batch_max_ms + write_yield_ms`` (about 0.35 s by default), behind a busy
graph writer, and a scan takes about 40 % longer than unpaced because of the pauses. The full answer is a separate store or process for the graph, or PostgreSQL, which has no
single-writer lock; this test does not claim more than the stated bounds.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator

import pytest

from olcf.db import schema as S
from olcf.intelligence import model as M
from tests.intelligence.conftest import Builder
from tests.intelligence.system.conftest import Platform, build_platform

pytestmark = pytest.mark.timeout(600)

# Loose enough for a loaded shared CI runner (one run measured 1.15 s), tight against starvation: before the pacing a publish waited for the whole scan (tens of seconds).
PUBLISH_BOUND, WRITE_BOUND = 2.5, 3.0
LAKEHOUSES, TABLES_EACH, COLS = 8, 100, 3


@pytest.fixture(scope="module")
def platform(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Platform]:
    with build_platform(tmp_path_factory.mktemp("isolation"), ["olcf.api.core.module", "olcf.intelligence.module"]) as p:
        yield p


def _facts() -> M.Facts:
    b = Builder(None)
    facts = M.Facts()
    for i in range(LAKEHOUSES):
        lh = b.lakehouse(f"itm_iso_{i}", f"iso{i}")
        facts.assets.append(lh)
        prev = None
        for t in range(TABLES_EACH):
            tb = b.table(lh, f"t{t}", [(f"c{c}", "string") for c in range(COLS)])
            facts.assets.append(tb)
            facts.assets.extend(b.column(tb, f"c{c}") for c in range(COLS))
            if prev is not None:
                facts.edges.append(b.edge(tb, prev, "READS_FROM"))
            prev = tb
    return facts


def test_event_publication_and_core_writes_are_not_starved_by_a_heavy_scan(platform: Platform) -> None:
    ctx, svc = platform.ctx, platform.intel
    source = next(s["id"] for s in platform.get("/v1/intelligence/sources")["items"] if s["kind"] == "local")
    facts = _facts()
    publish: list[float] = []
    writes: list[float] = []
    stop = threading.Event()

    def probe() -> None:
        n = 0
        while not stop.is_set():
            n += 1
            t0 = time.perf_counter()
            ctx.events.publish("probe.heavy_scan", {"n": n})
            publish.append(time.perf_counter() - t0)
            t0 = time.perf_counter()
            with ctx.db.begin() as conn:
                conn.execute(S.kv.delete().where(S.kv.c.key == "probe.heavy_scan"))
                conn.execute(S.kv.insert().values(key="probe.heavy_scan", value={"n": n}, updated_at="2026-01-01T00:00:00Z"))
            writes.append(time.perf_counter() - t0)
            time.sleep(0.02)

    thread = threading.Thread(target=probe, name="probe")
    thread.start()
    time.sleep(0.2)
    scan = svc.engine.begin_scan(source, "incremental", "manual")
    t0 = time.perf_counter()
    stats = svc.engine.apply_facts(facts, scan_id=scan, source_id=source)
    seconds = time.perf_counter() - t0
    svc.engine.finish_scan(scan, "succeeded", stats.as_dict())
    stop.set()
    thread.join()
    ctx.events.flush_deferred()

    assert not stats.errors, stats.error_samples
    assert stats.assets_created == LAKEHOUSES * (1 + TABLES_EACH * (1 + COLS)) and stats.edges_created == LAKEHOUSES * (TABLES_EACH - 1)
    assert svc.engine.pacer.pauses > 0, "the scan was long enough to be paced"
    assert len(publish) > 20, f"the probe ran {len(publish)} times during a {seconds:.1f} s scan"
    assert max(publish) < PUBLISH_BOUND, f"worst publish {max(publish):.2f} s (bound {PUBLISH_BOUND} s) during a {seconds:.1f} s scan"
    assert max(writes) < WRITE_BOUND, f"worst core write {max(writes):.2f} s (bound {WRITE_BOUND} s) during a {seconds:.1f} s scan"
    stored = ctx.events.list(type_prefix="probe.heavy_scan", limit=1000)
    assert len(stored) == len(publish)                                                        # nothing was lost: every probe event is stored once the persister has caught up
