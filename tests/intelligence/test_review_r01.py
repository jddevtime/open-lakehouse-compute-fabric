# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Review item R01 (resource boundary), unit level: the event bus does not wait unboundedly for the SQLite write lock, the graph writer paces itself, and the settings that bound
both exist. The end-to-end bound with a heavy scan is in ``system/test_write_isolation.py``."""

from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest

from olcf.core import events as ev
from olcf.core.events import EventBus
from olcf.db.engine import Database
from olcf.intelligence import model as M
from olcf.intelligence import settings as rt
from olcf.intelligence.engine import WritePacer


@pytest.fixture()
def disk(tmp_path: Path) -> Database:
    db = Database(f"sqlite:///{tmp_path / 'r01.db'}")
    db.migrate()
    return db


def hold_writer(db: Database, seconds: float) -> threading.Thread:
    """Hold the SQLite write lock for ``seconds`` (what a long graph transaction does)."""
    started = threading.Event()

    def run() -> None:
        with db.begin_serialized("intelligence.graph"):
            started.set()
            time.sleep(seconds)

    t = threading.Thread(target=run)
    t.start()
    assert started.wait(5)
    return t


# --------------------------------------------------------------------------------------------------------------------------------- event bus
def test_publish_returns_within_the_persist_budget_while_a_writer_holds_the_lock(disk: Database) -> None:
    bus = EventBus(disk)
    seen: list[dict] = []
    bus.subscribe(seen.append)
    holder = hold_writer(disk, 3.0)
    t0 = time.perf_counter()
    evt = bus.publish("probe.locked", {"n": 1})
    took = time.perf_counter() - t0
    assert took < ev.PERSIST_BUDGET + 1.5, f"publish took {took:.2f} s while the writer lock was held"   # far below the 3 s hold, with room for a loaded CI runner
    assert [e["type"] for e in seen] == ["probe.locked"] and "seq" not in evt             # subscribers have it at once; the persister fills the seq in later
    holder.join()
    assert bus.flush_deferred(10) and evt["seq"] and bus.deferred_total == 1
    assert [e["type"] for e in bus.list(type_prefix="probe.")] == ["probe.locked"]


def test_events_published_behind_a_deferred_one_keep_the_publish_order(disk: Database) -> None:
    bus = EventBus(disk)
    holder = hold_writer(disk, 1.0)
    for i in range(5):
        bus.publish(f"probe.order.{i}", {})
    holder.join()
    assert bus.flush_deferred(10)
    bus.publish("probe.order.5", {})
    assert [e["type"] for e in bus.list(type_prefix="probe.order.")] == [f"probe.order.{i}" for i in range(6)]
    assert bus.dropped == 0


def test_an_uncontended_publish_is_persisted_inline_with_its_seq(disk: Database) -> None:
    bus = EventBus(disk)
    evt = bus.publish("probe.inline", {})
    assert evt["seq"] and bus.deferred_total == 0 and bus.head_seq() == evt["seq"]
    with disk.connect() as c:                                                            # the bounded wait did not leak into the pooled connection
        assert c.exec_driver_sql("PRAGMA busy_timeout").scalar() == 30000


def test_a_database_error_is_still_swallowed_and_logged(tmp_path: Path) -> None:
    db = Database(f"sqlite:///{tmp_path / 'nomigrate.db'}")                              # no events table
    bus = EventBus(db)
    evt = bus.publish("probe.broken", {})
    assert "seq" not in evt and bus.deferred_total == 0


# --------------------------------------------------------------------------------------------------------------------------------- pacing
def test_the_pacer_pauses_after_max_rows_and_resets(disk: Database) -> None:
    pacer = WritePacer(disk, lambda: (5, 0, 20.0))
    for _ in range(4):
        pacer.after_unit(time.monotonic())
    assert pacer.pauses == 0
    t0 = time.perf_counter()
    pacer.after_unit(time.monotonic())
    assert pacer.pauses == 1 and time.perf_counter() - t0 >= 0.018
    for _ in range(4):
        pacer.after_unit(time.monotonic())
    assert pacer.pauses == 1                                                             # a new window started after the pause


def test_the_pacer_pauses_after_max_ms_of_continuous_writing(disk: Database) -> None:
    pacer = WritePacer(disk, lambda: (0, 60.0, 10.0))
    began = time.monotonic()
    for _ in range(3):
        time.sleep(0.03)
        pacer.after_unit(began)                                                          # the unit "began" 30 ms, 60 ms, 90 ms ago: the window passes 60 ms on the second
        began = time.monotonic() - 0.0
    assert pacer.pauses >= 1


def test_an_idle_thread_starts_a_new_window(disk: Database) -> None:
    pacer = WritePacer(disk, lambda: (3, 0, 10.0))
    pacer.after_unit(time.monotonic())
    pacer.after_unit(time.monotonic())
    time.sleep(0.05)                                                                     # longer than the pause: the old window is forgotten
    pacer.after_unit(time.monotonic())
    pacer.after_unit(time.monotonic())
    assert pacer.pauses == 0


@pytest.mark.parametrize("limits", [(0, 0, 100.0), (5, 5.0, 0.0)])
def test_the_pacer_is_off_when_its_limits_are_zero(disk: Database, limits: tuple[int, float, float]) -> None:
    pacer = WritePacer(disk, lambda: limits)
    for _ in range(50):
        pacer.after_unit(time.monotonic() - 1.0)
    assert pacer.pauses == 0


def test_the_pacer_never_pauses_on_postgres() -> None:
    class Pg:
        is_sqlite = False

    pacer = WritePacer(Pg(), lambda: (1, 1.0, 50.0))                                    # type: ignore[arg-type]
    t0 = time.perf_counter()
    for _ in range(10):
        pacer.after_unit(time.monotonic() - 1.0)
    assert pacer.pauses == 0 and time.perf_counter() - t0 < 0.2


def test_the_engine_paces_its_units_and_a_failing_settings_read_does_not_matter(env) -> None:
    if not env.db.is_sqlite:
        pytest.skip("pacing is a SQLite measure")
    eng = env.engine
    eng.pacer.enabled = True
    eng.pacer.limits = lambda: (10, 0, 5.0)
    b = env.b
    lh = b.lakehouse("itm_p", "p")
    env.scan(M.Facts([lh, *[b.table(lh, f"t{i}") for i in range(40)]]))
    assert eng.pacer.pauses >= 3
    eng.pacer.limits = lambda: 1 / 0                                                      # type: ignore[assignment,return-value]
    eng.pacer._cached = (0.0, (0, 0.0, 0.0))                                              # a stale cache is refreshed, the failure keeps the previous values
    env.scan(M.Facts([b.table(lh, "after")]))


def test_the_write_settings_are_registered_with_sane_bounds() -> None:
    keys = {k: (d, b) for k, _, d, b in rt.SETTINGS}
    for key, default in rt.WRITE_PACING:
        assert key in keys and keys[key][0] == default
    assert keys["intelligence.write_batch_max_rows"][0] > 0 and keys["intelligence.write_yield_ms"][0] >= 100


# --------------------------------------------------------------------------------------------------------------------------------- bounded maintenance transactions
def _big_graph(env, n: int = 12):
    b = env.b
    lh = b.lakehouse("itm_big", "big")
    tabs = [b.table(lh, f"t{i}") for i in range(n)]
    nb = b.notebook("itm_bignb", "nb")
    edges = [b.edge(nb, t, "READS_FROM") for t in tabs]
    return lh, tabs, nb, edges


def test_the_removal_sweep_runs_in_bounded_transactions_and_removes_exactly_what_it_did_before(env, monkeypatch) -> None:
    import olcf.intelligence.engine as eng_mod

    monkeypatch.setattr(eng_mod, "SWEEP_CHUNK", 4)
    lh, tabs, nb, edges = _big_graph(env)
    env.scan(M.Facts([lh, nb, *tabs], edges, [M.Gap("dynamic_reference", "t", "s", f"d{i}", None, {"i": i}) for i in range(9)]))
    sizes: list[int] = []
    real = env.graph.sweep

    def spy(conn, source_id, scan_id, *, limit=None):
        out = real(conn, source_id, scan_id, limit=limit)
        sizes.append(max(out.values()))
        return out

    monkeypatch.setattr(env.graph, "sweep", spy)
    sid, _ = env.scan(M.Facts([lh]))                                                    # the next full scan sees only the lakehouse
    row = env.engine.scan_row(sid)
    assert row["stats"]["sweep"] == {"assets_removed": 13, "edges_removed": 12, "gaps_resolved": 9}
    assert max(sizes) <= 4 and len(sizes) >= 4                                           # several bounded statements, none over the chunk
    assert env.rows("select count(*) from assets where status = 'active'")[0][0] == 1
    assert env.rows("select count(*) from edges where status = 'active'")[0][0] == 0
    assert env.rows("select count(*) from gaps where resolved_at is null")[0][0] == 0


def test_retiring_a_whole_tree_is_done_in_chunks(env, monkeypatch) -> None:
    import olcf.intelligence.engine as eng_mod

    monkeypatch.setattr(eng_mod, "RETIRE_CHUNK", 5)
    lh, tabs, nb, edges = _big_graph(env)
    env.scan(M.Facts([lh, *tabs, *[env.b.column(t, "id") for t in tabs]]))
    assert env.engine.retire_by_alias("olcf_item", "itm_big") == 1 + 12 + 12
    assert env.rows("select count(*) from assets where status = 'removed'")[0][0] == 25
    assert env.engine.retire_by_alias("olcf_item", "itm_missing") == 0


def test_runtime_evidence_pruning_runs_in_batches(env) -> None:
    from datetime import UTC, datetime, timedelta

    from sqlalchemy import update

    from olcf.intelligence.schema import edges as edges_t

    lh, tabs, nb, edges = _big_graph(env, 7)
    old = (datetime.now(UTC) - timedelta(days=400)).strftime("%Y-%m-%dT%H:%M:%SZ")
    env.scan(M.Facts([lh, nb, *tabs], [env.b.edge(nb, t, "READS_FROM", "verified", kind="runtime", source="runtime:job", ref="j", location={}, observed_at=old) for t in tabs]))
    with env.db.begin() as conn:
        conn.execute(update(edges_t).values(last_verified_at=old))
    assert env.engine.prune_runtime_evidence(batch=3) == 7
    assert {e["confidence"] for e in env.edges()} == {"derived"} and env.engine.prune_runtime_evidence(batch=3) == 0


def test_version_trimming_is_batched_and_calls_the_pause_between_batches(env) -> None:
    from olcf.intelligence.schema import asset_versions

    lh, tabs, nb, edges = _big_graph(env, 6)
    env.scan(M.Facts([lh, *tabs]))
    ids = [r[0] for r in env.rows("select id from assets where type = 'table'")]
    with env.db.begin() as conn:
        for aid in ids:
            for k in range(8):
                conn.execute(asset_versions.insert().values(asset_id=aid, scan_id=None, attrs_hash=f"h{k}", attrs={"k": k}, changes={}, created_at=f"2026-01-0{k + 1}T00:00:00Z"))
    calls: list[int] = []
    deleted = env.graph.purge_versions(3, batch=2, after_batch=lambda: calls.append(1))
    assert deleted == 6 * (9 - 3) and len(calls) == 3                                    # one initial version per table + 8, trimmed to 3 each; 6 assets in 3 batches
    assert env.rows("select count(*) from asset_versions")[0][0] == 6 * 3 + 1                 # plus the lakehouse's single version
    assert env.engine.trim_versions(3) == 0
