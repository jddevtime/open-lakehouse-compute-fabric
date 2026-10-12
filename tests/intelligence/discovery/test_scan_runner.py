# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The scan runner: admission and the one-scan-per-source lease, cancellation (this process and "another replica"), failure outcomes, lease takeover, reaping, the scheduler."""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from datetime import timedelta
from typing import Any

import pytest
from sqlalchemy import update

from olcf.core.errors import BadRequest, Conflict, NotFound
from olcf.core.timeutil import iso_in, to_iso, utcnow
from olcf.intelligence.discovery import base, register_builtin_connectors
from olcf.intelligence.discovery.base import ConnectorFailure, ScanContext
from olcf.intelligence.discovery.schema import discovery_sources
from olcf.intelligence.model import AssetFact, AssetRef, Facts
from olcf.security.rbac import SYSTEM_PRINCIPAL


def asset(name: str) -> Facts:
    return Facts(assets=[AssetFact(AssetRef.make("external", {"source": f"gate:{name}"}, name), name, {"source_kind": "test"})])


class Gate:
    """A connector (registered in place of ``local``) whose progress the test controls."""

    kind = "local"
    label = "test gate"

    def __init__(self) -> None:
        self.first_done = threading.Event()
        self.release = threading.Event()
        self.names = ["a", "b"]
        self.fail_with: Exception | None = None
        self.gap: tuple[str, bool] | None = None
        self.runs = 0

    def validate_config(self, config: Any) -> dict[str, Any]:
        return dict(config)

    def test(self, config: Any, secrets: Any, *, app: Any = None) -> list[Any]:
        return []

    def discover(self, ctx: ScanContext) -> Iterator[Any]:
        self.runs += 1
        ctx.state["runs"] = self.runs
        yield asset(self.names[0])
        self.first_done.set()
        if self.gap:
            ctx.record_gap(self.gap[0], "something", "could not read something", "fix it", location={"informational": True} if self.gap[1] else None)
        while not self.release.is_set():
            ctx.check_cancelled()
            time.sleep(0.01)
        if self.fail_with:
            raise self.fail_with
        for n in self.names[1:]:
            ctx.check_cancelled()
            yield asset(n)


@pytest.fixture()
def gate(disc):
    g = Gate()
    base.register_connector("local", lambda: g, replace=True)
    yield g
    g.release.set()
    disc.d.runner.shutdown(5)
    register_builtin_connectors()


def wait_for(cond, timeout: float = 10.0) -> None:  # noqa: ANN001
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return
        time.sleep(0.02)
    raise AssertionError("condition not met in time")


def test_a_second_scan_of_a_running_source_is_a_conflict_that_names_the_running_scan(disc, gate):
    first = disc.d.runner.start(SYSTEM_PRINCIPAL, "local", mode="full")
    assert first["status"] == "running"
    assert gate.first_done.wait(5)
    with pytest.raises(Conflict) as err:
        disc.d.runner.start(SYSTEM_PRINCIPAL, "local", mode="full")
    assert first["id"] in err.value.message and err.value.details["scan_id"] == first["id"]
    r = disc.api("POST", "/sources/local/scan", json={"mode": "full"})
    assert r.status_code == 409 and first["id"] in r.text
    assert disc.source()["running"] if "running" in disc.source() else disc.d.sources.is_leased(disc.source())
    gate.release.set()
    wait_for(lambda: disc.engine.scan_row(first["id"])["status"] == "succeeded")
    # the instant the closed scan is visible a new scan is admitted: the lease claim treats a lease on a finished scan as free, so there is no refusal window
    assert disc.d.runner.start(SYSTEM_PRINCIPAL, "local", mode="full", wait=True)["status"] == "succeeded"
    wait_for(lambda: not disc.d.sources.is_leased(disc.source()))


def test_concurrent_starts_admit_exactly_one_scan_and_the_losers_name_it(disc, gate):
    results: list[Any] = []
    barrier = threading.Barrier(8)

    def go() -> None:
        barrier.wait()
        try:
            results.append(disc.d.runner.start(SYSTEM_PRINCIPAL, "local", mode="full"))
        except Conflict as exc:
            results.append(exc)

    threads = [threading.Thread(target=go) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    winners = [r for r in results if isinstance(r, dict)]
    losers = [r for r in results if isinstance(r, Conflict)]
    assert len(winners) == 1 and len(losers) == 7
    assert all(winners[0]["id"] in e.message and e.details["scan_id"] == winners[0]["id"] for e in losers)
    assert len(disc.scans(disc.source()["id"])) == 1                                                    # the losers never opened a scan row
    gate.release.set()
    wait_for(lambda: disc.engine.scan_row(winners[0]["id"])["status"] == "succeeded")


def test_a_lease_on_a_finished_scan_is_free_even_before_it_is_released(disc, gate):
    gate.release.set()
    row = disc.scan()
    src = disc.source()
    # simulate the gap between "scan closed" and "lease released" (or a crash right there): the lease still points at the finished scan
    with disc.ctx.db.begin() as c:
        c.execute(update(discovery_sources).where(discovery_sources.c.id == src["id"]).values(lock_scan_id=row["id"], lock_holder="x", lock_expires_at=to_iso(utcnow() + timedelta(hours=1))))
    assert disc.d.sources.acquire(src["id"], "scn_probe") is None
    disc.d.sources.release(src["id"], "scn_probe")
    # but a lease on a RUNNING scan, or one still under its pre-scan token, is never taken
    assert disc.d.sources.acquire(src["id"], "scn_a") is None
    held = disc.d.sources.acquire(src["id"], "scn_b")
    assert held is not None and held["lock_scan_id"] == "scn_a"
    disc.d.sources.release(src["id"], "scn_a")


def test_cancel_stops_between_batches_keeps_what_was_applied_and_never_sweeps(disc, gate):
    # an earlier complete scan saw both assets; a cancelled full scan that only saw the first must not remove the second
    gate.release.set()
    done = disc.scan()
    assert done["status"] == "succeeded"
    gate.release.clear()
    gate.first_done.clear()
    running = disc.d.runner.start(SYSTEM_PRINCIPAL, "local", mode="full")
    assert gate.first_done.wait(5)
    out = disc.d.runner.cancel(SYSTEM_PRINCIPAL, running["id"])
    assert out["cancel_requested"] is True
    wait_for(lambda: disc.engine.scan_row(running["id"])["status"] != "running")
    row = disc.engine.scan_row(running["id"])
    assert row["status"] == "cancelled" and "sweep" not in row["stats"] and row["finished_at"]
    assert disc.asset("b", "external")["status"] == "active"
    src = disc.source()
    assert src["last_status"] == "cancelled" and not disc.d.sources.is_leased(src) and src["cancel_scan_id"] is None
    with pytest.raises(Conflict, match="already cancelled"):
        disc.d.runner.cancel(SYSTEM_PRINCIPAL, running["id"])


def test_cancel_flag_written_by_another_replica_is_observed(disc, gate):
    running = disc.d.runner.start(SYSTEM_PRINCIPAL, "local", mode="full")
    assert gate.first_done.wait(5)
    assert disc.d.sources.request_cancel(disc.source()["id"], running["id"]) is True      # what the API on another node does: one column on the source row
    wait_for(lambda: disc.engine.scan_row(running["id"])["status"] == "cancelled", timeout=10)


def test_cancel_via_api_returns_202_and_hides_nothing_from_readers(disc, gate):
    r = disc.api("POST", "/sources/local/scan", json={"mode": "full"})
    assert r.status_code == 202 and r.json()["status"] == "running"
    sid = r.json()["id"]
    assert gate.first_done.wait(5)
    c = disc.api("POST", f"/scans/{sid}/cancel")
    assert c.status_code == 202 and c.json()["cancel_requested"] is True
    wait_for(lambda: disc.api("GET", f"/scans/{sid}").json()["status"] == "cancelled")
    assert disc.api("POST", f"/scans/{sid}/cancel").status_code == 409


def test_full_scan_sweep_removes_what_a_complete_scan_no_longer_sees(disc, gate):
    gate.release.set()
    disc.scan()
    assert {a["name"] for a in disc.assets(type="external", status="active")} == {"a", "b"}
    gate.names = ["a"]
    row = disc.scan()
    assert row["stats"]["sweep"]["assets_removed"] == 1 and disc.asset("b", "external", status="removed")
    gate.names = ["a", "b"]
    disc.scan()
    assert disc.asset("b", "external")["status"] == "active"                                  # it comes back as the same asset


def test_blocking_and_informational_gaps_decide_between_partial_and_succeeded(disc, gate):
    gate.release.set()
    gate.gap = ("permission_denied", False)
    row = disc.scan()
    assert row["status"] == "partial" and row["stats"]["blocking_gaps"] == 1 and "sweep" not in row["stats"]
    gap = [g for g in disc.gaps() if g["kind"] == "permission_denied"][0]
    assert gap["remediation"] == "fix it" and gap["origin"].startswith("discovery:")
    gate.gap = ("not_inspected", True)
    row2 = disc.scan()
    assert row2["status"] == "succeeded" and row2["stats"]["gaps_reported"] == 1 and "sweep" in row2["stats"]
    assert not [g for g in disc.gaps() if g["kind"] == "permission_denied"]                    # a clean sweep resolved the old gap


def test_connector_failure_is_a_failed_scan_with_a_message_not_a_stack_trace(disc, gate):
    gate.release.set()
    gate.fail_with = ConnectorFailure("the credentials were rejected", "rotate the secret")
    row = disc.scan()
    assert row["status"] == "failed" and "credentials were rejected" in row["error"] and "rotate the secret" in row["error"] and "Traceback" not in row["error"]
    assert disc.source()["last_status"] == "failed" and not disc.d.sources.is_leased(disc.source())
    assert disc.asset("a", "external")["status"] == "active"                                    # whatever was applied before the failure stays; nothing swept


def test_unexpected_errors_are_redacted(disc, gate):
    gate.release.set()
    gate.fail_with = RuntimeError("request failed: client_secret=Zx9~topsecretvalue and Bearer abcdefghijklmnopqrstuvwxyz")
    row = disc.scan()
    assert row["status"] == "failed" and "topsecretvalue" not in row["error"] and "abcdefghijklmnopqrstuvwxyz" not in row["error"] and "RuntimeError" in row["error"]


def test_connector_checkpoint_is_stored_only_for_scans_that_completed(disc, gate):
    gate.release.set()
    disc.scan()
    assert disc.source()["state"] == {"runs": 1}
    gate.fail_with = RuntimeError("boom")
    disc.scan(mode="incremental")
    assert disc.source()["state"] == {"runs": 1}                                              # a failed scan does not move the checkpoint
    gate.fail_with = None
    disc.scan(mode="incremental")
    assert disc.source()["state"]["runs"] == 3


def test_admission_errors(disc, gate):
    gate.release.set()
    with pytest.raises(BadRequest, match="mode must be"):
        disc.d.runner.start(SYSTEM_PRINCIPAL, "local", mode="sometimes")
    with pytest.raises(NotFound):
        disc.d.runner.start(SYSTEM_PRINCIPAL, "src_missing")
    disc.d.sources.update(SYSTEM_PRINCIPAL, disc.source()["id"], {"enabled": False})
    with pytest.raises(Conflict, match="disabled"):
        disc.d.runner.start(SYSTEM_PRINCIPAL, "local")
    assert disc.api("POST", "/sources/local/scan").status_code == 409


def test_a_lease_whose_owner_vanished_is_taken_over_and_the_abandoned_scan_is_closed(disc, gate):
    gate.release.set()
    sid = disc.source()["id"]
    dead = disc.engine.begin_scan(sid, "full", "manual")                                        # a scan row left "running" by a process that died ...
    assert disc.d.sources.acquire(sid, dead) is None
    disc.d.sources.rebind(sid, dead, dead)
    with pytest.raises(Conflict):
        disc.d.runner.start(SYSTEM_PRINCIPAL, "local", wait=True)                              # ... still holds a live lease: refused
    from sqlalchemy import update

    from olcf.intelligence.discovery.schema import discovery_sources
    with disc.ctx.db.begin() as c:                                                              # its heartbeat stopped long ago
        c.execute(update(discovery_sources).where(discovery_sources.c.id == sid).values(lock_expires_at="2000-01-01T00:00:00.000000Z"))
    fresh = disc.d.runner.start(SYSTEM_PRINCIPAL, "local", mode="full", wait=True)
    assert fresh["status"] == "succeeded"
    old = disc.engine.scan_row(dead)
    assert old["status"] == "failed" and "interrupted" in old["error"]


def test_reap_closes_abandoned_running_scans_without_a_live_lease(disc, gate):
    sid = disc.source()["id"]
    dead = disc.engine.begin_scan(sid, "full", "schedule")
    assert disc.d.runner.reap() == 1
    row = disc.engine.scan_row(dead)
    assert row["status"] == "failed" and "interrupted" in row["error"]
    assert disc.d.runner.reap() == 0


def test_in_process_concurrency_limit(disc, gate):
    from olcf.intelligence import settings as rt

    assert rt.get(disc.ctx, "intelligence.max_concurrent_scans", 2) == 2
    other = disc.d.sources.create(SYSTEM_PRINCIPAL, name="second", kind="local")
    third = disc.d.sources.create(SYSTEM_PRINCIPAL, name="third", kind="local")
    disc.d.runner.start(SYSTEM_PRINCIPAL, "local")
    disc.d.runner.start(SYSTEM_PRINCIPAL, other["id"])
    with pytest.raises(Conflict, match="limit is 2"):
        disc.d.runner.start(SYSTEM_PRINCIPAL, third["id"])
    assert len(disc.d.runner.active_scan_ids()) == 2
    gate.release.set()
    wait_for(lambda: not disc.d.runner.active_scan_ids())


def test_scans_are_audited_through_the_engine(disc, gate):
    gate.release.set()
    row = disc.scan()
    started = [r for r in disc.ctx.audit.query(action="intelligence.scan_started") if r["resource_id"] == row["id"]]
    finished = [r for r in disc.ctx.audit.query(action="intelligence.scan_finished") if r["resource_id"] == row["id"]]
    assert started and finished and finished[0]["details"]["status"] == "succeeded"


# ------------------------------------------------------------------------------------------------ the scheduler
def test_scheduler_starts_due_scans_and_respects_intervals_and_failures(disc, gate):
    gate.release.set()
    sched = disc.d.scheduler
    src = disc.source()
    assert sched.due(src) == "full"                                                             # never scanned: a full scan first
    out = sched.tick()
    assert len(out["started"]) == 1 and out["started"][0]["trigger"] == "schedule"
    wait_for(lambda: not disc.d.runner.active_scan_ids())
    assert sched.due(disc.source()) is None                                                     # just scanned
    from sqlalchemy import update

    from olcf.intelligence.discovery.schema import discovery_sources
    with disc.ctx.db.begin() as c:
        c.execute(update(discovery_sources).where(discovery_sources.c.id == src["id"]).values(last_incremental_at="2000-01-01T00:00:00.000000Z", last_full_at="2000-01-01T00:00:00.000000Z"))
    assert sched.due(disc.source()) == "full"                                                   # the full interval (1 day) has passed
    with disc.ctx.db.begin() as c:
        c.execute(update(discovery_sources).where(discovery_sources.c.id == src["id"]).values(last_full_at=iso_in(-3600), last_incremental_at=iso_in(-3000)))
    assert sched.due(disc.source()) == "incremental"
    disc.d.sources.update(SYSTEM_PRINCIPAL, src["id"], {"schedule": {"incremental_seconds": 0, "full_seconds": 0}})
    assert sched.due(disc.source()) is None                                                     # 0 = only on demand
    disc.d.sources.update(SYSTEM_PRINCIPAL, src["id"], {"schedule": {"incremental_seconds": 60}, "enabled": False})
    assert sched.due(disc.source()) is None                                                     # disabled sources are never scheduled


def test_scheduler_backs_off_after_a_failure_and_skips_leased_sources(disc, gate):
    gate.release.set()
    gate.fail_with = RuntimeError("down")
    sched = disc.d.scheduler
    sched.tick()
    wait_for(lambda: not disc.d.runner.active_scan_ids())
    src = disc.source()
    assert src["last_status"] == "failed" and sched.due(src) is None                            # retried no sooner than the back-off
    assert sched.tick()["started"] == []


def test_scheduler_startup_scan_and_scan_on_start_switch(disc, gate):
    gate.release.set()
    sched = disc.d.scheduler
    disc.ctx.settings.intelligence.scan_on_start = False
    assert sched.due(disc.source()) is None and sched.next_due(disc.source()) is None          # no automatic first scan when switched off
    assert sched.tick()["started"] == []
    disc.scan()                                                                                  # a manual first scan, then the cadence applies
    assert disc.source()["last_full_at"]
    disc.ctx.settings.intelligence.scan_on_start = True


def test_scheduler_does_not_run_in_the_test_environment(disc):
    assert disc.ctx.settings.environment == "test" and disc.d.scheduler.running is False


def test_a_holder_whose_lease_was_taken_over_cannot_finish_or_sweep(disc, gate):
    """R03: a stalled scan whose lease another process has taken over ends ``failed`` (nothing swept), not ``succeeded``."""
    gate.release.set()
    sid = disc.source()["id"]
    original = gate.discover

    def discover_then_lose_the_lease(ctx: ScanContext) -> Iterator[Any]:
        yield from original(ctx)
        with disc.ctx.db.begin() as c:                                           # "another replica" took the expired lease over
            c.execute(update(discovery_sources).where(discovery_sources.c.id == sid).values(lock_scan_id="someone-else", lock_holder="replica-2", lock_expires_at=iso_in(300)))

    gate.discover = discover_then_lose_the_lease                                  # type: ignore[method-assign]
    row = disc.scan(mode="full")
    assert row["status"] == "failed" and "lease" in (row["error"] or "") and "sweep" not in row["stats"]
