# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Failure isolation: whatever breaks inside Intelligence, jobs, SQL and pipelines keep working, with identical results and sane timing.

The application is the live one of ``build_live_platform`` (every module, a real worker agent, real runner processes), the workloads are real:

* a Python job that reads ``fact_sales`` and writes a new Delta table (``EXIT_VALUE`` carries the result),
* a SQL statement over ``fact_sales`` (DuckDB),
* a pipeline with a copy activity (table to table).

Each failure is injected in turn - the inbox tables dropped, the inbox writer raising, the inbox writer hanging, the graph raising while events are applied, the Intelligence
background loops killed, an unrelated event subscriber hanging for 5 seconds - and the same three workloads run. They must succeed, return what the healthy baseline returned
and not take noticeably longer. Then the failure is repaired and the tests state precisely what the system recovered and what it lost:

======================  =====================================================================  ==============================================================
failure                 while it lasts                                                         after the repair
======================  =====================================================================  ==============================================================
inbox tables dropped    events are not recorded (counted, writer backs off, health degraded)   new events are recorded again; events of the outage are LOST
inbox writer raising    same                                                                   same
inbox writer hanging    publishers wait at most the bus budget once, then not at all           queued events are recorded when it returns
graph raising           events wait in the inbox, retried (3 attempts), then marked ``failed`` backlog within the attempts drains; ``failed`` rows stay failed
loops killed            events wait in the inbox                                               the whole backlog drains when the loop is back
hanging subscriber      nothing (the bus gives it a lane of its own)                           its backlog is delivered when it returns
======================  =====================================================================  ==============================================================
"""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pytest
from sqlalchemy import func, select, text

from olcf.core import events as bus_mod
from olcf.core.health import Status
from olcf.intelligence import ingest
from olcf.intelligence.schema import inbox
from tests.intelligence.system.conftest import LivePlatform, build_live_platform, wait_until

JOB = '''from pathlib import Path

import pyarrow.compute as pc
from deltalake import DeltaTable, write_deltalake
from olcf.runtime_lib import paths

tables = Path(paths.resolve("Tables/fact_sales", for_="local")).parent
data = DeltaTable(str(tables / "fact_sales")).to_pyarrow_table(columns=["store_id", "net_amount"])
write_deltalake(str(tables / "iso_job_TAG"), data, mode="overwrite")
EXIT_VALUE = {"rows": data.num_rows, "net": round(pc.sum(data["net_amount"]).as_py(), 2)}
'''
SQL = "SELECT count(*) AS n, round(sum(net_amount), 2) AS s FROM retail_lakehouse.fact_sales"
SLACK = 6.0              # seconds a workload may take longer than in the healthy baseline (a worker start, a slow CI disk)


@pytest.fixture(scope="module")
def live(tmp_path_factory: pytest.TempPathFactory) -> Iterator[LivePlatform]:
    with build_live_platform(tmp_path_factory.mktemp("isolation"), install_sample=True, agent=True) as p:
        yield p


def _mod(p: LivePlatform) -> Any:
    return next(m for m in p.ctx.modules if m.name == "intelligence")


def run_workloads(p: LivePlatform, tag: str) -> dict[str, Any]:
    """Run the three workloads; returns their results and wall-clock seconds."""
    ws = p.sample["workspace_id"]                                              # type: ignore[attr-defined]
    out: dict[str, Any] = {"seconds": {}}
    t = time.monotonic()
    r = p.post("/v1/sql/execute", {"sql": SQL, "workspace_id": ws, "lakehouse": "retail_lakehouse"}, expect=200)
    out["sql"] = r["rows"]
    out["seconds"]["sql"] = time.monotonic() - t
    t = time.monotonic()
    job = p.wait_job(p.submit(JOB.replace("TAG", tag), workspace_id=ws, lakehouse="retail_lakehouse", name=f"iso-job-{tag}")["id"])
    assert job["state"] == "succeeded", job.get("failure")
    out["job"] = job["exit_value"]
    out["seconds"]["job"] = time.monotonic() - t
    t = time.monotonic()
    pipe = p.post(f"/v1/workspaces/{ws}/items", {"type": "pipeline", "name": f"iso-pipe-{tag}", "definition": {"schema": "olcf.pipeline/1", "activities": [
        {"name": "Copy", "type": "copy", "typeProperties": {"source": {"lakehouse": "retail_lakehouse", "table": "dim_store"},
                                                           "sink": {"lakehouse": "retail_lakehouse", "table": f"iso_pipe_{tag}", "mode": "overwrite"}}}]}}, expect=201)
    run = p.post(f"/v1/pipelines/{pipe['id']}/runs", {"parameters": {}}, expect=202)
    done = wait_until(lambda: (x := p.get(f"/v1/pipeline-runs/{run['id']}"))["state"] in ("succeeded", "failed", "cancelled", "timed_out") and x, 60, 0.1, "the pipeline run")
    assert done["state"] == "succeeded", done
    out["pipeline"] = p.post("/v1/sql/execute", {"sql": f"SELECT count(*) AS n FROM retail_lakehouse.iso_pipe_{tag}", "workspace_id": ws, "lakehouse": "retail_lakehouse"},  # noqa: S608
                             expect=200)["rows"]
    out["seconds"]["pipeline"] = time.monotonic() - t
    return out


@pytest.fixture(scope="module")
def baseline(live: LivePlatform) -> dict[str, Any]:
    return run_workloads(live, "base")


def assert_same_as_baseline(got: dict[str, Any], base: dict[str, Any]) -> None:
    assert got["sql"] == base["sql"] and got["job"] == base["job"] and got["pipeline"] == base["pipeline"], (got, base)
    for k, v in got["seconds"].items():
        assert v <= base["seconds"][k] + SLACK, f"{k} took {v:.1f}s, baseline {base['seconds'][k]:.1f}s"


def _job_edges(p: LivePlatform, tag: str) -> list[str]:
    """Types of the verified edges the runtime recorded for the job of ``tag`` (empty when the graph never heard of it)."""
    try:
        asset = p.asset("script", f"iso-job-{tag}", status=None)
    except AssertionError:
        return []
    return sorted(e["type"] for e in p.lineage(asset, direction="both", depth=1)["edges"] if asset in (e["src"], e["dst"]) and e["confidence"] == "verified")


def _inbox_count(p: LivePlatform, **where: Any) -> int:
    with p.ctx.db.connect() as c:
        stmt = select(func.count()).select_from(inbox)
        for k, v in where.items():
            stmt = stmt.where(inbox.c[k] == v)
        return int(c.execute(stmt).scalar() or 0)


def _heal_writer(p: LivePlatform) -> None:
    """Forget the writer's back-off (production: 30 s after three failures in a row) so the next test starts with a writer that records."""
    w = _mod(p).writer
    w._paused_until, w._consecutive = 0.0, 0


def _health(p: LivePlatform) -> Any:
    return p.ctx.health.run("intelligence", force=True)


@contextmanager
def loops_stopped(p: LivePlatform) -> Iterator[None]:
    """Kill Intelligence's background work (inbox loop and the discovery / governance loops); restart it afterwards."""
    mod = _mod(p)
    mod.loop.stop()
    for child in reversed(mod.children):
        if hasattr(child, "stop"):
            child.stop()
    try:
        yield
    finally:
        mod.loop.start()
        for child in mod.children:
            if hasattr(child, "start"):
                child.start()


class _Raising:
    """Stands in for a database handle: every use raises (or hangs), like a broken or stalled inbox table."""

    def __init__(self, hang: threading.Event | None = None) -> None:
        self.hang = hang

    def begin(self, *a: Any, **k: Any) -> Any:
        if self.hang is not None:
            self.hang.wait(5)
        raise RuntimeError("the inbox is broken")

    connect = begin


# ============================================================================================================================ baseline
def test_baseline_the_healthy_system(live: LivePlatform, baseline: dict[str, Any]) -> None:
    assert baseline["sql"][0][0] > 0 and baseline["job"]["rows"] == baseline["sql"][0][0] and baseline["pipeline"][0][0] > 0
    live.drain()
    assert _job_edges(live, "base") == ["READS_FROM", "WRITES_TO"]


# ============================================================================================================================ inbox tables dropped
def test_inbox_tables_dropped(live: LivePlatform, baseline: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ingest, "_BACKOFF_SECONDS", 0.3)                       # the production back-off is 30 s; the behaviour is the same
    mod = _mod(live)
    failures_before = mod.writer.failures
    with live.ctx.db.begin() as c:
        c.execute(text("DROP TABLE inbox"))
    try:
        got = run_workloads(live, "dropped")
        assert_same_as_baseline(got, baseline)
        assert mod.writer.failures > failures_before and "inbox" in (mod.writer.last_error or "")
        health = _health(live)
        assert health.status in (Status.DEGRADED, Status.DOWN) and health.status != Status.OK       # the check itself cannot read the missing table: it says so, it does not crash
        assert live.http.get("/v1/health/ready").status_code == 200              # the platform itself stays ready: Intelligence is not a critical component
        with pytest.raises(Exception, match="inbox"):                           # one pass fails ...
            mod.processor.tick()
        assert mod.loop._thread is not None and mod.loop._thread.is_alive()     # ... the loop that calls it logs and keeps going
    finally:
        inbox.create(live.ctx.db.engine, checkfirst=True)                       # the repair: the table is back, empty
    _heal_writer(live)
    after = run_workloads(live, "after_drop")
    assert_same_as_baseline(after, baseline)
    live.drain()
    assert _job_edges(live, "dropped") == []                                    # LOST: nothing recorded those events and the inbox is the only copy
    assert _job_edges(live, "after_drop") == ["READS_FROM", "WRITES_TO"]        # recovered: new runs are recorded again


# ============================================================================================================================ inbox raising / hanging
def test_inbox_writer_raising(live: LivePlatform, baseline: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ingest, "_BACKOFF_SECONDS", 0.3)
    mod = _mod(live)
    real = mod.writer.db
    mod.writer.db = _Raising()
    try:
        got = run_workloads(live, "raising")
        assert_same_as_baseline(got, baseline)
        assert mod.writer.failures > 0
    finally:
        mod.writer.db = real
    _heal_writer(live)
    assert_same_as_baseline(run_workloads(live, "after_raise"), baseline)
    live.drain()
    assert _job_edges(live, "raising") == [] and _job_edges(live, "after_raise") == ["READS_FROM", "WRITES_TO"]


def test_inbox_writer_hanging_for_five_seconds(live: LivePlatform, baseline: dict[str, Any]) -> None:
    mod = _mod(live)
    real, release = mod.writer.db, threading.Event()
    mod.writer.db = _Raising(release)                                          # begin() waits 5 s, then raises
    try:
        t = time.monotonic()
        for i in range(10):
            live.ctx.events.publish("job.state_changed", {"n": i})
        assert time.monotonic() - t < bus_mod.WAIT_BUDGET + 1.0                  # ten publishes, one budget at most - not 10 x 5 s
        got = run_workloads(live, "hanging")
        assert_same_as_baseline(got, baseline)
    finally:
        mod.writer.db = real                                                     # first the repair, then let the stuck call return: the events queued behind it are recorded
        release.set()
        wait_until(lambda: mod.writer.db is not None and mod.writer.written > 0, 5, 0.05, "the writer to record again")
        time.sleep(0.5)
        _heal_writer(live)


# ============================================================================================================================ graph raising
def test_graph_raising_backlog_within_the_attempts_is_drained_after_the_repair(live: LivePlatform, baseline: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    mod = _mod(live)
    with loops_stopped(live):                                                   # deterministic: the test decides when the processor runs
        live.drain()
        engine = mod.processor.engine

        def broken(*a: Any, **k: Any) -> Any:
            raise RuntimeError("the graph is unavailable")

        monkeypatch.setattr(engine, "apply_facts", broken)
        got = run_workloads(live, "graph")
        assert_same_as_baseline(got, baseline)
        pending = _inbox_count(live, status="pending")
        assert pending > 0
        mod.processor.tick()                                                    # attempt 1: still pending, retried later
        mod.processor.tick()                                                    # attempt 2
        assert _inbox_count(live, status="pending") > 0 and _inbox_count(live, status="failed") == 0
        assert "the graph is unavailable" in (mod.processor.last_error or "")
        monkeypatch.undo()                                                      # the repair, before the third attempt
        mod.processor.tick()
        assert _inbox_count(live, status="pending") == 0 and _inbox_count(live, status="failed") == 0
    assert _job_edges(live, "graph") == ["READS_FROM", "WRITES_TO"]             # recovered: the whole backlog was applied


def test_graph_raising_longer_than_the_attempts_marks_rows_failed_and_they_stay_failed(live: LivePlatform, baseline: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    """What is lost: ``failed`` is final. The health check says so (degraded, N rows failed) and the rows keep their error, but nothing re-queues them."""
    mod = _mod(live)
    with loops_stopped(live):
        live.drain()
        monkeypatch.setattr(mod.processor.engine, "apply_facts", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("the graph is unavailable")))
        got = run_workloads(live, "graph_long")
        assert_same_as_baseline(got, baseline)
        for _ in range(ingest.MAX_ATTEMPTS):
            mod.processor.tick()
        monkeypatch.undo()
        failed = _inbox_count(live, status="failed")
        assert failed > 0
        mod.processor.tick()                                                    # repaired - and still failed
        assert _inbox_count(live, status="failed") == failed
        health = _health(live)
        assert health.status == Status.DEGRADED and f"{failed} inbox row(s) failed" in health.detail
        assert _job_edges(live, "graph_long") == []
        with live.ctx.db.begin() as c:                                           # operator action: put the rows back (documented recovery for this case)
            c.execute(inbox.update().where(inbox.c.status == "failed").values(status="pending", attempts=0, error=None))
        mod.processor.tick()
    assert _job_edges(live, "graph_long") == ["READS_FROM", "WRITES_TO"] and _inbox_count(live, status="failed") == 0


# ============================================================================================================================ scheduler (background loops) killed
def test_background_loops_killed_the_backlog_drains_on_restart(live: LivePlatform, baseline: dict[str, Any]) -> None:
    with loops_stopped(live):
        live.drain()
        got = run_workloads(live, "loops")
        assert_same_as_baseline(got, baseline)
        assert _inbox_count(live, status="pending") > 0                          # recorded, waiting
        time.sleep(2.5)                                                          # longer than the poll interval: nothing processes it
        assert _inbox_count(live, status="pending") > 0 and _job_edges(live, "loops") == []
        assert _health(live).detail.count("pending") >= 1
    wait_until(lambda: _inbox_count(live, status="pending") == 0, 30, 0.25, "the restarted loop to drain the backlog")
    assert _job_edges(live, "loops") == ["READS_FROM", "WRITES_TO"]


# ============================================================================================================================ hanging subscriber
def test_a_subscriber_hanging_for_five_seconds_does_not_block_publishers(live: LivePlatform, baseline: dict[str, Any]) -> None:
    release = threading.Event()
    calls: list[str] = []

    def hanging(evt: dict[str, Any]) -> None:
        calls.append(evt["type"])
        release.wait(5)                                                          # "sleeps 5 s" on every event until released

    off = live.ctx.events.subscribe(hanging)
    try:
        t = time.monotonic()
        for i in range(25):
            live.ctx.events.publish("probe.event", {"i": i})
        publish_seconds = time.monotonic() - t
        assert publish_seconds < bus_mod.WAIT_BUDGET + 1.0, publish_seconds       # not 25 x 5 s
        got = run_workloads(live, "hang")
        assert_same_as_baseline(got, baseline)
        live.drain()
        assert _job_edges(live, "hang") == ["READS_FROM", "WRITES_TO"]            # Intelligence, on its own lane, never noticed
        assert len(calls) < 25                                                    # the hung subscriber really was stuck on its first event
    finally:
        release.set()
        off()
    wait_until(lambda: len(calls) >= 25, 30, 0.1, "the stuck subscriber to catch up")


def test_a_raising_foreign_subscriber_changes_nothing(live: LivePlatform, baseline: dict[str, Any]) -> None:
    def boom(_e: dict[str, Any]) -> None:
        raise RuntimeError("someone else's subscriber is broken")

    off = live.ctx.events.subscribe(boom)
    try:
        assert_same_as_baseline(run_workloads(live, "boom"), baseline)
        live.drain()
        assert _job_edges(live, "boom") == ["READS_FROM", "WRITES_TO"]
    finally:
        off()


# ============================================================================================================================ intelligence disabled at runtime
def test_the_workloads_do_not_import_intelligence(live: LivePlatform) -> None:
    """Compute, SQL and pipelines publish ordinary events and are unaware of Intelligence: no import of ``olcf.intelligence`` from them."""
    import ast
    import pathlib

    root = pathlib.Path(bus_mod.__file__).resolve().parents[1]
    offenders = []
    for pkg in ("compute", "sql", "pipelines", "jobs", "notebooks", "storage", "catalog", "worker", "runtime_lib"):
        for f in (root / pkg).rglob("*.py"):
            tree = ast.parse(f.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                names = [a.name for a in node.names] if isinstance(node, ast.Import) else [node.module or ""] if isinstance(node, ast.ImportFrom) and node.level == 0 else []
                if any(n == "olcf.intelligence" or n.startswith("olcf.intelligence.") for n in names):
                    offenders.append(f"{f.relative_to(root)}: {names}")
    assert offenders == [], offenders
