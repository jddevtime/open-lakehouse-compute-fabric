# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The event inbox: writer isolation, the processor (translation, runtime scan, deletions, retry, retention), the leader loop and the health check."""

from __future__ import annotations

import sys
import time
import types
from typing import Any

import pytest
from sqlalchemy import insert, select, text

from olcf.core.dataset_events import dataset_descriptor, process_descriptor
from olcf.core.health import Status
from olcf.intelligence import ingest
from olcf.intelligence import model as M
from olcf.intelligence.schema import edges as edges_t
from olcf.intelligence.schema import gaps as gaps_t
from olcf.intelligence.schema import inbox
from tests.intelligence.conftest import INTEL_MODULES, Builder


def modules_of(ctx):
    return next(m for m in ctx.modules if m.name == "intelligence")


@pytest.fixture()
def ing(intel):
    """The app with the background inbox loop stopped (tests drive ``tick()`` themselves) and a workspace + discovered graph."""
    mod = modules_of(intel.ctx)
    mod.loop.stop()
    intel.mod, intel.proc, intel.writer = mod, mod.processor, mod.writer
    ws = intel.workspace("alpha")
    b = Builder(ws["id"])
    lh = b.lakehouse("itm_lh", "lh")
    src, dst = b.table(lh, "src"), b.table(lh, "dst")
    nb = b.notebook("itm_nb", "etl")
    intel.scan(M.Facts([lh, src, dst, nb], [b.edge(nb, src, "READS_FROM", "derived"), b.edge(nb, dst, "WRITES_TO", "derived")]))
    intel.ws, intel.b = ws, b
    return intel


def flow_event(kind: str, *, tables: list[str], item: str = "itm_nb", job: str = "job_1", lakehouse: str = "itm_lh", name: str = "etl", **extra: Any) -> dict[str, Any]:
    return {"process": process_descriptor("notebook", item_id=item, job_id=job, name=name),
            "datasets": [dataset_descriptor("table", lakehouse_id=lakehouse, name=t) for t in tables], "at": "2026-10-09T10:00:00Z", **extra}


def rows(c, **where):
    with c.ctx.db.connect() as conn:
        stmt = select(inbox).order_by(inbox.c.seq)
        for k, v in where.items():
            stmt = stmt.where(inbox.c[k] == v)
        return [dict(r._mapping) for r in conn.execute(stmt)]


def edge_between(c, src: str, dst: str, type_: str) -> dict[str, Any] | None:
    reg = c.service.registry
    s, d = reg.find("olcf_item", src), reg.find("lakehouse_table", dst)
    with c.ctx.db.connect() as conn:
        r = conn.execute(select(edges_t).where(edges_t.c.src_id == s, edges_t.c.dst_id == d, edges_t.c.type == type_)).mappings().first()
    return dict(r) if r else None


# ------------------------------------------------------------------------------------------------------------------------------ writer
def test_writer_stores_only_the_events_intelligence_wants(ing):
    ev = ing.ctx.events
    ev.publish("dataset.written", flow_event("w", tables=["dst"]), workspace_id=ing.ws["id"], subject="job_1")
    ev.publish("job.state_changed", {"to": "succeeded"}, subject="job_1")
    ev.publish("pipeline.run.failed", {"pipeline": "p"}, subject="run_9")
    ev.publish("auth.login", {"user": "x"})
    ev.publish("datasets.unrelated", {})
    got = rows(ing)
    assert [r["type"] for r in got if r["type"] != "workspace.created"] == ["dataset.written", "job.state_changed", "pipeline.run.failed"]
    first = next(r for r in got if r["type"] == "dataset.written")
    assert first["status"] == "pending" and first["workspace_id"] == ing.ws["id"] and first["payload"]["_meta"]["subject"] == "job_1" and first["payload"]["_meta"]["event_id"].startswith("evt_")
    assert ingest.wanted("dataset.read") and ingest.wanted("pipeline.run.succeeded") and ingest.wanted("item.deleted") and not ingest.wanted("auth.login") and not ingest.wanted("jobs.created")
    assert ing.writer.written >= 3 and ing.writer.failures == 0


def test_writer_is_off_when_intelligence_is_disabled(ing):
    ing.ctx.settings.intelligence.enabled = False
    before = len(rows(ing))
    ing.ctx.events.publish("dataset.read", flow_event("r", tables=["src"]))
    assert len(rows(ing)) == before


def test_a_broken_writer_never_reaches_the_publisher(ing):
    class Boom:
        def begin(self):
            raise RuntimeError("database is on fire")

    w = ingest.InboxWriter(Boom())
    evt = {"type": "dataset.written", "payload": {}, "id": "evt_1", "ts": "now", "subject": None, "workspace_id": None}
    for _ in range(5):
        w(evt)                                   # must not raise
    assert w.failures == 5 and "on fire" in (w.last_error or "") and w.written == 0
    # after three failures in a row it backs off: it stops touching the database (so a dead table is not paid for on every event)
    calls = []

    class Counting(Boom):
        def begin(self):
            calls.append(1)
            return super().begin()

    w2 = ingest.InboxWriter(Counting())
    for _ in range(10):
        w2(evt)
    assert len(calls) == 3 and w2.failures == 10
    w2._paused_until = 0.0                       # the back-off ends: it tries again
    w2(evt)
    assert len(calls) == 4
    # a subscriber that is not even a valid event does not raise either
    ingest.InboxWriter(Boom())({})
    ingest.InboxWriter(Boom())({"type": None})


def test_a_failing_subscriber_does_not_break_event_publication(ing):
    ing.ctx.events.subscribe(lambda e: (_ for _ in ()).throw(RuntimeError("other subscriber")))
    evt = ing.ctx.events.publish("dataset.read", flow_event("r", tables=["src"]), subject="job_7")
    assert evt["seq"] and rows(ing, type="dataset.read")


# ------------------------------------------------------------------------------------------------------------------------------ processor
def test_processor_turns_runtime_events_into_verified_edges(ing):
    ev = ing.ctx.events
    ev.publish("dataset.read", flow_event("r", tables=["src"]), workspace_id=ing.ws["id"], subject="job_1")
    ev.publish("dataset.written", flow_event("w", tables=["dst", "brand_new"], mode="append"), workspace_id=ing.ws["id"], subject="job_1")
    assert edge_between(ing, "itm_nb", "itm_lh:dst", "WRITES_TO")["confidence"] == "derived"
    out = ing.proc.tick()
    assert out["applied"] == 2 and out["failed"] == 0
    read, write = edge_between(ing, "itm_nb", "itm_lh:src", "READS_FROM"), edge_between(ing, "itm_nb", "itm_lh:dst", "WRITES_TO")
    assert read["confidence"] == "verified" and write["confidence"] == "verified"
    assert write["attrs"]["mode"] == "append" and write["last_verified_at"]
    kinds = {e["kind"] for e in write["evidence"]}
    assert kinds == {"runtime", "static"} and next(e for e in write["evidence"] if e["kind"] == "runtime")["ref"] == "job_1"
    assert edge_between(ing, "itm_nb", "itm_lh:brand_new", "WRITES_TO")["confidence"] == "verified"              # a dataset nothing has discovered yet
    created = ing.service.registry.find("lakehouse_table", "itm_lh:brand_new")
    assert created and ing.service.registry.get(created)["status"] in ("active", "placeholder")
    assert {r["status"] for r in rows(ing) if r["type"].startswith("dataset.")} == {"done"}
    # the upgrade shows in the API: the edge detail lists both pieces of evidence and the confidence change
    detail = ing.get(f"/v1/intelligence/edges/{write['id']}", headers=ing.admin).json()
    assert detail["confidence"] == "verified" and any(c["field"] == "confidence" and c["from"] == "derived" for c in detail["changes"])


def test_runtime_scan_is_one_per_day_and_never_sweeps_other_sources(ing):
    ing.ctx.events.publish("dataset.read", flow_event("r", tables=["src"]), workspace_id=ing.ws["id"], subject="job_1")
    ing.proc.tick()
    first = ing.proc.runtime_scan()
    ing.ctx.events.publish("dataset.read", flow_event("r", tables=["src"], job="job_2"), workspace_id=ing.ws["id"], subject="job_2")
    ing.proc.tick()
    assert ing.proc.runtime_scan() == first and len(ing.service.snapshots(_admin(ing), source_id="runtime", status="running")[0]) == 1
    # a restart reuses today's open scan
    fresh = ingest.InboxProcessor(ing.ctx, ing.engine)
    assert fresh.runtime_scan() == first
    # a full scan of the local source that no longer shows the read does not remove the runtime-verified edge
    ing.scan(M.Facts([]), source="local", mode="full")
    assert edge_between(ing, "itm_nb", "itm_lh:src", "READS_FROM")["status"] == "active"
    ing.proc.close()
    assert ing.engine.scan_row(first)["status"] == "succeeded"
    assert ing.proc.runtime_scan() != first                                       # a new day's scan is opened on demand


def _admin(c):
    from olcf.security.rbac import Binding, Principal

    return Principal(id="usr_admin", name="admin", bindings=[Binding("PlatformAdmin")])


def test_day_rollover_closes_yesterdays_scan(ing, monkeypatch):
    first = ing.proc.runtime_scan()
    monkeypatch.setattr(ingest, "now_iso", lambda: "2099-01-01T00:00:00.000000Z")
    second = ing.proc.runtime_scan()
    assert second != first and ing.engine.scan_row(first)["status"] == "succeeded" and ing.engine.scan_row(second)["status"] == "running"


def test_unknown_malformed_and_empty_events_are_skipped_not_failed(ing):
    ev = ing.ctx.events
    ev.publish("dataset.read", {"nothing": "useful"})
    ev.publish("dataset.written", {"process": {"kind": "notebook"}, "datasets": "not a list"})
    ev.publish("job.state_changed", {"to": "running"}, subject="job_3")
    ev.publish("item.created", {"type": "notebook", "name": "n"}, subject="itm_x")
    ing.proc.tick()
    relevant = [r for r in rows(ing) if r["type"] in ("dataset.read", "dataset.written", "job.state_changed", "item.created")]
    assert {r["status"] for r in relevant} == {"skipped"} and all(r["attempts"] == 1 for r in relevant)
    assert ing.proc.failed == 0


def test_without_a_translator_rows_are_skipped_with_a_reason(ing, monkeypatch):
    monkeypatch.setattr(ingest.InboxProcessor, "_translator", staticmethod(lambda: None))
    ing.ctx.events.publish("dataset.read", flow_event("r", tables=["src"]))
    ing.proc.tick()
    row = rows(ing, type="dataset.read")[0]
    assert row["status"] == "skipped" and "not installed" in row["error"]


def test_a_poison_event_is_retried_then_failed_and_the_queue_moves_on(ing, monkeypatch):
    calls = {"n": 0}

    def translate(etype, payload):
        if payload.get("poison"):
            calls["n"] += 1
            raise ValueError("cannot translate")
        from olcf.intelligence.runtime import facts_from_event

        return facts_from_event(etype, payload)

    monkeypatch.setattr(ingest.InboxProcessor, "_translator", staticmethod(lambda: translate))
    ing.ctx.events.publish("dataset.read", {"poison": True})
    ing.ctx.events.publish("dataset.read", flow_event("r", tables=["src"]), workspace_id=ing.ws["id"], subject="job_1")
    for _ in range(4):
        ing.proc.tick()
    got = {r["payload"].get("poison", False): r for r in rows(ing, type="dataset.read")}
    assert got[True]["status"] == "failed" and got[True]["attempts"] == ingest.MAX_ATTEMPTS and "cannot translate" in got[True]["error"] and calls["n"] == ingest.MAX_ATTEMPTS
    assert got[False]["status"] == "done"
    assert ing.proc.failed == 1 and "ValueError" in (ing.proc.last_error or "")
    assert edge_between(ing, "itm_nb", "itm_lh:src", "READS_FROM")["confidence"] == "verified"


def test_a_failing_row_is_attempted_once_per_pass_not_burnt_back_to_back(ing, monkeypatch):
    """Regression (system integration): one pass used to re-select the failing row until it had used all its attempts, so a graph that was unavailable for a moment turned every
    event of that moment into a final ``failed`` row. The retries are now spaced by the poll interval."""
    broken = {"on": True}

    def translate(etype, payload):
        if broken["on"]:
            raise RuntimeError("the graph is unavailable")
        from olcf.intelligence.runtime import facts_from_event

        return facts_from_event(etype, payload)

    monkeypatch.setattr(ingest.InboxProcessor, "_translator", staticmethod(lambda: translate))
    ing.ctx.events.publish("dataset.read", flow_event("r", tables=["src"]), workspace_id=ing.ws["id"], subject="job_1")
    ing.proc.tick()
    got = rows(ing, type="dataset.read")
    assert [(r["status"], r["attempts"]) for r in got] == [("pending", 1)]
    ing.proc.tick()
    assert [(r["status"], r["attempts"]) for r in rows(ing, type="dataset.read")] == [("pending", 2)]
    broken["on"] = False                                    # the repair arrives before the last attempt: nothing was lost
    ing.proc.tick()
    assert [r["status"] for r in rows(ing, type="dataset.read")] == ["done"]
    assert edge_between(ing, "itm_nb", "itm_lh:src", "READS_FROM")["confidence"] == "verified"


def test_statement_events_use_the_analyzer_and_lift_confidence(ing, monkeypatch):
    stub = types.ModuleType("olcf.intelligence.lineage.engine")
    seen = {}

    def analyze_definition(defn):
        seen["sql"], seen["kind"] = defn.payload["sql"], defn.kind
        b = ing.b
        tgt = M.AssetRef.make("table", {"lakehouse_table": "itm_lh:dst"})
        src = M.AssetRef.make("table", {"lakehouse_table": "itm_lh:src"})
        return M.Facts([], [b.edge(defn.ref, src, "READS_FROM", "derived", location={"stmt": 1}),
                            M.EdgeFact(src, tgt, "TRANSFORMS", [M.Evidence("static", "sql", "inferred")], via=defn.ref, transformation="x")], [])

    stub.analyze_definition, stub.ANALYZER_VERSION = analyze_definition, "t"
    monkeypatch.setitem(sys.modules, "olcf.intelligence.lineage.engine", stub)
    ing.ctx.events.publish("dataset.statement", {"sql": "insert into dst select x from src", "dialect": "spark",
                                                 "process": process_descriptor("sql_query", query_id="q1", name="ad hoc")}, workspace_id=ing.ws["id"], subject="q1")
    ing.proc.tick()
    assert seen == {"sql": "insert into dst select x from src", "kind": "sql_query"}
    q = ing.service.list_assets(_admin(ing), type="sql_query")[0]
    assert len(q) == 1 and q[0]["type"] == "sql_query"
    with ing.ctx.db.connect() as conn:
        es = {r["type"]: dict(r) for r in conn.execute(select(edges_t).where(edges_t.c.source_id == "runtime")).mappings()}
    assert es["READS_FROM"]["confidence"] == "verified" and es["READS_FROM"]["evidence"][0]["kind"] == "runtime"
    assert es["TRANSFORMS"]["confidence"] == "inferred"                      # a guess stays a guess


def test_statement_with_a_crashing_analyzer_becomes_a_gap(ing, monkeypatch):
    stub = types.ModuleType("olcf.intelligence.lineage.engine")
    stub.analyze_definition = lambda defn: (_ for _ in ()).throw(RuntimeError("parser bug"))
    monkeypatch.setitem(sys.modules, "olcf.intelligence.lineage.engine", stub)
    ing.ctx.events.publish("dataset.statement", {"sql": "select 1", "process": process_descriptor("sql_query", query_id="q2", name="q")}, workspace_id=ing.ws["id"], subject="q2")
    ing.proc.tick()
    assert rows(ing, type="dataset.statement")[0]["status"] == "done"
    with ing.ctx.db.connect() as conn:
        assert conn.execute(select(gaps_t.c.kind).where(gaps_t.c.origin == "runtime:statement")).scalar() == "parse_error"


def test_schema_change_events_create_history(ing):
    cols = [{"name": "id", "type": "int"}, {"name": "extra", "type": "string"}]
    ing.ctx.events.publish("dataset.schema_changed", {"dataset": dataset_descriptor("table", lakehouse_id="itm_lh", name="dst"), "columns": cols, "added": ["extra"], "workspace_id": ing.ws["id"]},
                           workspace_id=ing.ws["id"], subject="itm_lh")
    ing.proc.tick()
    # whatever shape the translator accepts, the event is consumed (done or skipped) and never fails
    assert rows(ing, type="dataset.schema_changed")[0]["status"] in ("done", "skipped")


def test_pipeline_run_events_are_consumed(ing):
    ing.ctx.events.publish("pipeline.run.failed", {"pipeline_id": "itm_pipe", "run_id": "run_1", "name": "nightly"}, workspace_id=ing.ws["id"], subject="run_1")
    ing.proc.tick()
    assert rows(ing, type="pipeline.run.failed")[0]["status"] in ("done", "skipped")


def test_deletion_events_retire_assets(ing):
    ev = ing.ctx.events
    ev.publish("item.deleted", {"type": "notebook", "name": "etl"}, workspace_id=ing.ws["id"], subject="itm_nb")
    out = ing.proc.tick()
    assert out["failed"] == 0
    assert ing.service.registry.get(ing.service.registry.find("olcf_item", "itm_nb"))["status"] == "removed"
    assert ing.service.registry.get(ing.service.registry.find("lakehouse_table", "itm_lh:src"))["status"] == "active"
    ev.publish("lakehouse.deleted", {"name": "lh"}, workspace_id=ing.ws["id"], subject="itm_lh")
    ing.proc.tick()
    for alias in ("itm_lh:src", "itm_lh:dst"):
        assert ing.service.registry.get(ing.service.registry.find("lakehouse_table", alias))["status"] == "removed"           # children go with the lakehouse
    # a deletion event for something never discovered is harmless
    ev.publish("item.deleted", {}, subject="itm_unknown")
    ing.proc.tick()
    assert rows(ing, type="item.deleted")[-1]["status"] == "done"
    # restored by the next scan (items are soft-deleted in OLCF)
    lh = ing.b.lakehouse("itm_lh", "lh")
    ing.scan(M.Facts([lh, ing.b.table(lh, "src")]), mode="incremental")
    assert ing.service.registry.get(ing.service.registry.find("lakehouse_table", "itm_lh:src"))["status"] == "active"


def test_a_deletion_event_without_a_subject_is_skipped(ing):
    ing.ctx.events.publish("item.deleted", {"name": "x"})
    ing.proc.tick()
    row = rows(ing, type="item.deleted")[0]
    assert row["status"] == "skipped" and "subject" in row["error"]


def test_purge_applies_the_retention_window(ing):
    old, recent = "2020-01-01T00:00:00.000000Z", time.strftime("%Y-%m-%dT%H:%M:%S.000000Z", time.gmtime())
    with ing.ctx.db.begin() as conn:
        conn.execute(insert(inbox), [
            {"type": "dataset.read", "payload": {}, "received_at": old, "attempts": 1, "status": "done"},
            {"type": "dataset.read", "payload": {}, "received_at": old, "attempts": 1, "status": "failed"},
            {"type": "dataset.read", "payload": {}, "received_at": old, "attempts": 0, "status": "pending"},
            {"type": "dataset.read", "payload": {}, "received_at": recent, "attempts": 1, "status": "done"},
        ])
        conn.execute(text("delete from inbox where type='workspace.created'"))
    res = ing.proc.purge()
    assert res["inbox_rows"] == 3                      # 'pending' that is older than twice the window is stuck, so it goes too
    assert [r["status"] for r in rows(ing)] == ["done"]
    ing.ctx.service("settings").set("intelligence.retention_days", 3650)
    with ing.ctx.db.begin() as conn:
        conn.execute(insert(inbox).values(type="dataset.read", payload={}, received_at=old, attempts=1, status="done"))
    assert ing.proc.purge()["inbox_rows"] == 0


def test_housekeeping_prunes_expired_runtime_evidence(ing):
    ing.ctx.events.publish("dataset.read", flow_event("r", tables=["src"]), workspace_id=ing.ws["id"], subject="job_1")
    ing.proc.tick()
    e = edge_between(ing, "itm_nb", "itm_lh:src", "READS_FROM")
    old = "2020-01-01T00:00:00.000000Z"
    with ing.ctx.db.begin() as conn:
        ev = [{**d, "observed_at": old} if d["kind"] == "runtime" else d for d in e["evidence"]]
        conn.execute(edges_t.update().where(edges_t.c.id == e["id"]).values(last_verified_at=old, evidence=ev))
    ing.proc._housekeeping()
    after = edge_between(ing, "itm_nb", "itm_lh:src", "READS_FROM")
    assert after["confidence"] == "derived" and {d["kind"] for d in after["evidence"]} == {"static"} and after["last_verified_at"] is None


# ------------------------------------------------------------------------------------------------------------------------------ leader loop and end to end
def test_leader_loop_only_runs_on_the_leader(ing):
    calls = []
    state = {"leader": False}

    class Ctx:
        class leader:  # noqa: N801
            @staticmethod
            def is_leader():
                return state["leader"]

    loop = ingest.LeaderLoop(Ctx, "t", lambda: calls.append(1), 0.05)
    loop.start()
    loop.start()                                               # idempotent
    time.sleep(0.3)
    assert calls == []
    state["leader"] = True
    deadline = time.monotonic() + 3
    while not calls and time.monotonic() < deadline:
        time.sleep(0.05)
    loop.stop()
    n = len(calls)
    assert n >= 1
    time.sleep(0.2)
    assert len(calls) == n and loop.tick_now() is None
    loop.stop()                                                # stopping twice is fine


def test_a_crashing_loop_function_keeps_the_loop_alive(ing):
    calls = []

    def fn():
        calls.append(1)
        raise RuntimeError("tick failed")

    class Ctx:
        class leader:  # noqa: N801
            @staticmethod
            def is_leader():
                return True

    loop = ingest.LeaderLoop(Ctx, "t2", fn, 0.05)
    loop.start()
    deadline = time.monotonic() + 3
    while len(calls) < 3 and time.monotonic() < deadline:
        time.sleep(0.05)
    loop.stop()
    assert len(calls) >= 3


@pytest.fixture()
def fast_app(settings):
    from fastapi.testclient import TestClient

    from olcf.api.app import create_app

    settings.intelligence.inbox_poll_seconds = 0.2
    app = create_app(settings, module_names=INTEL_MODULES)
    with TestClient(app):
        yield app.state.ctx


def test_events_flow_end_to_end_through_the_background_loop(fast_app):
    ctx = fast_app
    svc = ctx.service("intelligence")
    ctx.events.publish("dataset.written", flow_event("w", tables=["out"]), workspace_id="wks_x", subject="job_5")
    reg = svc.registry
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if reg.find("lakehouse_table", "itm_lh:out"):
            break
        time.sleep(0.2)
    aid = reg.find("lakehouse_table", "itm_lh:out")
    assert aid, "the loop did not process the event"
    with ctx.db.connect() as conn:
        assert conn.execute(text("select count(*) from edges where dst_id=:d and type='WRITES_TO' and confidence='verified'"), {"d": aid}).scalar() == 1
    assert svc.inbox_stats()["pending"] == 0


def test_a_disabled_module_records_nothing_and_does_not_start_its_loop(settings):
    from fastapi.testclient import TestClient

    from olcf.api.app import create_app

    settings.intelligence.enabled = False
    app = create_app(settings, module_names=INTEL_MODULES)
    with TestClient(app):
        ctx = app.state.ctx
        mod = modules_of(ctx)
        assert mod.loop._thread is None
        ctx.events.publish("dataset.read", flow_event("r", tables=["src"]))
        with ctx.db.connect() as conn:
            assert conn.execute(select(inbox.c.seq)).first() is None
        comp = ctx.health.run("intelligence", force=True)
        assert comp.status is Status.UNKNOWN and "disabled" in comp.detail


# ------------------------------------------------------------------------------------------------------------------------------ health
def test_health_check_quick_and_deep(ing):
    quick, deep = ingest.health_check(ing.ctx, ing.service, ing.writer, ing.proc)
    q = quick()
    assert q.status is Status.OK and q.category == "data" and q.labels["inbox"]["failed"] == 0 and q.labels["last_scans"][0]["source_id"] == "local"
    d = deep()
    assert d.status is Status.OK and [s["step"] for s in d.steps] == ["inbox write", "inbox read-back", "graph query"] and all(s["ok"] for s in d.steps)
    assert not rows(ing, type="intelligence.probe")                       # the probe row is removed again


def test_health_degrades_on_failures_and_lag(ing):
    ing.proc.failed = 1
    with ing.ctx.db.begin() as conn:
        conn.execute(insert(inbox).values(type="dataset.read", payload={}, received_at="2020-01-01T00:00:00.000000Z", attempts=3, status="failed", error="x"))
        conn.execute(insert(inbox).values(type="dataset.read", payload={}, received_at="2020-01-01T00:00:00.000000Z", attempts=0, status="pending"))
    ing.writer.failures, ing.writer.last_error = 2, "OperationalError: locked"
    q = ingest.health_check(ing.ctx, ing.service, ing.writer, ing.proc)[0]()
    assert q.status is Status.DEGRADED
    for needle in ("1 inbox row(s) failed", "2 event(s) could not be recorded", "oldest waiting event"):
        assert needle in q.detail
    ing.ctx.settings.intelligence.inbox_lag_warn = 0
    assert "events waiting" in ingest.health_check(ing.ctx, ing.service, ing.writer, ing.proc)[0]().detail


def test_health_reports_a_failed_scan_and_the_disabled_state(ing):
    sid = ing.engine.begin_scan("fabric_x", "full", "manual")
    ing.engine.finish_scan(sid, "failed", error="boom")
    q = ingest.health_check(ing.ctx, ing.service, ing.writer, ing.proc)[0]()
    assert q.status is Status.DEGRADED and "last scan failed for fabric_x" in q.detail
    ing.ctx.settings.intelligence.enabled = False
    assert ingest.health_check(ing.ctx, ing.service, ing.writer, ing.proc)[0]().status is Status.UNKNOWN


def test_deep_health_is_down_when_the_graph_or_inbox_is_broken(ing, monkeypatch):
    quick, deep = ingest.health_check(ing.ctx, ing.service, ing.writer, ing.proc)
    monkeypatch.setattr(ing.service.graph, "walk", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("graph broken")))
    d = deep()
    assert d.status is Status.DOWN and "graph query" in d.detail and d.steps[-1]["ok"] is False
    monkeypatch.undo()
    real = ing.ctx.db.begin

    def broken():
        raise RuntimeError("no writes")

    monkeypatch.setattr(ing.ctx.db, "begin", broken)
    d = deep()
    monkeypatch.setattr(ing.ctx.db, "begin", real)
    assert d.status is Status.DOWN and d.steps[0] == {"step": "inbox write", "ok": False, "error": "no writes"}


def test_health_is_registered_and_visible_through_the_api(intel):
    r = intel.get("/v1/admin/health", headers=intel.admin).json()
    comp = next(c for c in r["components"] if c["name"] == "intelligence")
    assert comp["status"] == "ok" and comp["category"] == "data" and comp["labels"]["writer"]["failures"] == 0


# ------------------------------------------------------------------------------------------------------------------------------ hardening: retry, requeue, history trimming
def _age(c, **where):
    """Make the matching inbox rows look like they were last processed an hour ago."""
    old = "2020-01-01T00:00:00.000000Z"
    with c.ctx.db.begin() as conn:
        stmt = inbox.update().values(updated_at=old)
        for k, v in where.items():
            stmt = stmt.where(inbox.c[k] == v)
        conn.execute(stmt)


def test_failed_rows_are_retried_once_after_the_configured_delay(ing, monkeypatch):
    calls = {"n": 0}

    def translate(etype, payload):
        calls["n"] += etype == "dataset.read"
        if etype == "dataset.read" and calls["n"] <= ingest.MAX_ATTEMPTS:             # the first three attempts fail (the row goes to failed), the automatic retry works
            raise RuntimeError("the graph was unavailable")
        from olcf.intelligence.runtime import facts_from_event

        return facts_from_event(etype, payload)

    monkeypatch.setattr(ingest.InboxProcessor, "_translator", staticmethod(lambda: translate))
    ing.ctx.events.publish("dataset.read", flow_event("r", tables=["src"]), workspace_id=ing.ws["id"], subject="job_1")
    for _ in range(4):
        ing.proc.tick()
    row = rows(ing, type="dataset.read")[0]
    assert row["status"] == "failed" and row["attempts"] == ingest.MAX_ATTEMPTS and row["updated_at"]
    assert ing.proc._retry_failed() == 0                                          # failed just now: not due yet
    _age(ing, status="failed")
    assert ing.proc._retry_failed() == 1 and ing.proc.requeued == 1
    assert rows(ing, type="dataset.read")[0]["status"] == "pending"
    ing.proc.tick()
    row = rows(ing, type="dataset.read")[0]
    assert row["status"] == "done" and row["error"] is None
    assert edge_between(ing, "itm_nb", "itm_lh:src", "READS_FROM")["confidence"] == "verified"


def test_a_row_that_fails_its_retry_is_not_retried_again_and_zero_disables(ing, monkeypatch):
    from olcf.intelligence.runtime import facts_from_event

    def translate(etype, payload):
        if etype == "dataset.read":
            raise ValueError("always")
        return facts_from_event(etype, payload)

    monkeypatch.setattr(ingest.InboxProcessor, "_translator", staticmethod(lambda: translate))
    ing.ctx.events.publish("dataset.read", {"x": 1})
    for _ in range(3):
        ing.proc.tick()
    ing.ctx.service("settings").set("intelligence.inbox_retry_after_minutes", 0)
    _age(ing, status="failed")
    assert ing.proc._retry_failed() == 0                                          # 0 turns the automatic retry off
    assert rows(ing, type="dataset.read")[0]["status"] == "failed"
    ing.ctx.service("settings").set("intelligence.inbox_retry_after_minutes", 30)
    assert ing.proc._retry_failed() == 1
    ing.proc.tick()
    row = rows(ing, type="dataset.read")[0]
    assert row["status"] == "failed" and row["attempts"] > ingest.MAX_ATTEMPTS
    _age(ing, status="failed")
    assert ing.proc._retry_failed() == 0                                          # once only: the operator endpoint is the way from here


def test_requeue_endpoint_queues_every_failed_row(ing, monkeypatch):
    state = {"broken": True}

    def translate(etype, payload):
        if state["broken"] and etype == "dataset.read":
            raise RuntimeError("down")
        from olcf.intelligence.runtime import facts_from_event

        return facts_from_event(etype, payload)

    monkeypatch.setattr(ingest.InboxProcessor, "_translator", staticmethod(lambda: translate))
    for _ in range(2):
        ing.ctx.events.publish("dataset.read", flow_event("r", tables=["src"]), workspace_id=ing.ws["id"], subject="job_1")
    for _ in range(4):
        ing.proc.tick()
    assert [r["status"] for r in rows(ing, type="dataset.read")] == ["failed", "failed"]
    viewer = ing.user("inbox_viewer", "Viewer")
    assert ing.post("/v1/intelligence/inbox/requeue", headers=viewer).status_code == 403
    assert ing.post("/v1/intelligence/inbox/requeue").status_code == 401
    state["broken"] = False
    r = ing.post("/v1/intelligence/inbox/requeue", headers=ing.admin)
    assert r.status_code == 200, r.text
    j = r.json()
    assert j["requeued"] == 2 and j["inbox"]["failed"] == 0 and j["inbox"]["pending"] >= 2
    ing.proc.tick()
    assert [r["status"] for r in rows(ing, type="dataset.read")] == ["done", "done"]
    again = ing.post("/v1/intelligence/inbox/requeue", headers=ing.admin).json()
    assert again["requeued"] == 0
    with ing.ctx.db.connect() as conn:
        from olcf.db import schema as S

        n = conn.execute(select(S.audit_log.c.id).where(S.audit_log.c.action == "intelligence.inbox_requeued")).fetchall()
    assert len(n) == 2


def test_housekeeping_trims_long_attribute_histories_and_survives_a_failing_step(ing, monkeypatch):
    from olcf.intelligence.schema import asset_versions

    aid = ing.asset_id("src")
    with ing.ctx.db.begin() as conn:
        conn.execute(asset_versions.insert(), [{"asset_id": aid, "scan_id": None, "attrs_hash": f"h{i}", "attrs": {"i": i}, "changes": {}, "created_at": "2026-01-01T00:00:00.000000Z"}
                                               for i in range(40)])
    ing.ctx.service("settings").set("intelligence.max_versions_per_asset", 10)
    out = ing.proc._housekeeping()
    assert out["versions"] >= 30
    with ing.ctx.db.connect() as conn:
        left = conn.execute(select(asset_versions.c.attrs_hash).where(asset_versions.c.asset_id == aid).order_by(asset_versions.c.id)).scalars().all()
    assert len(left) == 10 and left[-1] == "h39"                                  # the newest versions stay
    monkeypatch.setattr(ing.proc, "purge", lambda: (_ for _ in ()).throw(RuntimeError("purge broke")))
    out = ing.proc._housekeeping()                                                # one step failing never stops the others
    assert out["inbox_rows"] == 0 and out["versions"] == 0
