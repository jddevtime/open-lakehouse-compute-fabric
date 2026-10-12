# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Review finding F05: an asset merge must audit itself without opening a second SQLite writer inside the graph write transaction.

Everything here runs the real wiring (``Database`` on an on-disk SQLite file, the platform ``AuditLog``, the registry installed by the module) and asserts latency, the
number of audit records, the verifiability of the hash chain and the behaviour of nested, concurrent and rolled-back merges."""

from __future__ import annotations

import threading
import time

import pytest

from olcf.intelligence import model as M

from .conftest import Builder

PROMPT = 1.0                      # seconds: a merge of real assets on on-disk SQLite must finish well inside this


def _notebook(registry, item: str, name: str, **kw):
    return registry.ensure(M.AssetRef.make("notebook", {"olcf_item": item}, name), name=name, workspace_id="wks_x", **kw).asset_id


def _merges(ctx):
    return ctx.audit.query(action="intelligence.asset_merged", limit=500)


@pytest.fixture()
def reg(intel):
    return intel.service.registry


def test_merging_two_real_assets_is_prompt_and_audited_once(intel, reg):
    a, b = _notebook(reg, "itm_a", "nb_a"), _notebook(reg, "itm_b", "nb_b")
    before = len(_merges(intel.ctx))
    t0 = time.perf_counter()
    reg.merge(a, b, reason="test")
    took = time.perf_counter() - t0
    assert took < PROMPT, f"the merge took {took:.2f}s (a second writer inside the graph transaction blocks until the busy timeout)"
    rows = _merges(intel.ctx)
    assert len(rows) == before + 1
    rec = rows[0]
    assert rec["resource_id"] == a and rec["details"]["merged"] == b and rec["details"]["reason"] == "test"
    assert intel.ctx.audit.verify()["ok"] is True
    assert reg.find("olcf_item", "itm_b") == a                                      # the aliases moved


def test_nested_child_merges_each_leave_one_record(intel, reg):
    b = Builder("wks_x")
    lh1, lh2 = b.lakehouse("itm_lh1", "lh"), b.lakehouse("itm_lh2", "lh")
    ids = {}
    for tag, lh in (("1", lh1), ("2", lh2)):
        ids[f"lh{tag}"] = reg.ensure(lh.ref, name="lh", workspace_id="wks_x").asset_id
        for t in ("orders", "items"):
            fact = b.table(lh, t)
            ids[f"{t}{tag}"] = reg.ensure(fact.ref, name=t, workspace_id="wks_x", parent_id=ids[f"lh{tag}"]).asset_id
    before = len(_merges(intel.ctx))
    t0 = time.perf_counter()
    counts = reg.merge(ids["lh1"], ids["lh2"], reason="same lakehouse")
    assert time.perf_counter() - t0 < PROMPT
    assert counts["children"] == 2
    rows = _merges(intel.ctx)[: len(_merges(intel.ctx)) - before]
    assert sorted(r["details"]["merged"] for r in rows) == sorted([ids["lh2"], ids["orders2"], ids["items2"]])      # one record per merge, the lakehouse and its two tables
    assert len({r["id"] for r in rows}) == 3 and intel.ctx.audit.verify()["ok"] is True


def test_a_rolled_back_merge_leaves_neither_a_record_nor_a_notification(intel, reg):
    a, b = _notebook(reg, "itm_ra", "nb_ra"), _notebook(reg, "itm_rb", "nb_rb")
    seen: list[tuple[str, str]] = []
    reg.on_event = lambda action, asset, details: seen.append((action, asset))
    before = len(_merges(intel.ctx))
    with pytest.raises(RuntimeError), intel.ctx.db.begin_serialized("intelligence.graph") as conn:
        reg.merge(a, b, conn=conn)
        raise RuntimeError("the unit failed after the merge")
    reg.discard_events()                                                          # the caller owned the transaction, so the caller drops what it queued (the Engine does)
    assert len(_merges(intel.ctx)) == before and seen == []
    assert reg.get(b)["status"] != "removed" and reg.find("olcf_item", "itm_rb") == b
    reg.merge(a, b)                                                               # and a later, successful merge still reports exactly once
    assert len(_merges(intel.ctx)) == before + 1 and seen == [("intelligence.asset_merged", a)]


def test_notifications_are_delivered_after_the_commit_never_inside_it(intel, reg):
    a, b = _notebook(reg, "itm_na", "nb_na"), _notebook(reg, "itm_nb", "nb_nb")
    state: dict[str, object] = {}

    def on_event(action, asset, details):
        state["merged_visible"] = reg.get(b)["status"] == "removed"             # a fresh connection: only sees the merge if it has been committed
        t0 = time.perf_counter()
        with intel.ctx.db.begin() as conn:                                      # a writer opened from the notification must not block (no transaction is open any more)
            conn.exec_driver_sql("SELECT 1")
        intel.ctx.events.publish("intelligence.test.during_merge", {})
        state["write_seconds"] = time.perf_counter() - t0

    reg.on_event = on_event
    reg.merge(a, b)
    assert state["merged_visible"] is True and state["write_seconds"] < PROMPT       # type: ignore[operator]


def test_concurrent_merges_each_audit_once_and_publishes_are_not_stalled(intel, reg):
    pairs = [(_notebook(reg, f"itm_c{i}a", f"c{i}a"), _notebook(reg, f"itm_c{i}b", f"c{i}b")) for i in range(6)]
    before = len(_merges(intel.ctx))
    latencies: list[float] = []
    stop = threading.Event()

    def publisher() -> None:
        while not stop.is_set():
            t0 = time.perf_counter()
            intel.ctx.events.publish("job.state_changed", {"to": "running"}, subject="job_probe")
            latencies.append(time.perf_counter() - t0)
            time.sleep(0.01)

    pub = threading.Thread(target=publisher)
    pub.start()
    errors: list[Exception] = []

    def work(p: tuple[str, str]) -> None:
        try:
            intel.service.registry.merge(*p, reason="concurrent")
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=work, args=(p,)) for p in pairs]
    t0 = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join(30)
    elapsed = time.perf_counter() - t0
    stop.set()
    pub.join(10)
    assert not errors and elapsed < 5.0
    rows = _merges(intel.ctx)[: len(_merges(intel.ctx)) - before]
    assert sorted(r["details"]["merged"] for r in rows) == sorted(b for _, b in pairs)
    assert intel.ctx.audit.verify()["ok"] is True
    assert latencies and max(latencies) < 1.0, f"event publication stalled for {max(latencies):.2f}s during merges"
