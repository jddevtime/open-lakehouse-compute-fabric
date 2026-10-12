# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106, F811
"""Incremental Fabric scans: the durable per-workspace retry set (review F06), a valid ``modifiedSince`` and the shared request budget (F07), incomplete inventories (F08).

Contract tests against ``FakeFabricWorld`` and a test clock. The documented window of *Get Modified Workspaces*
(30 minutes to 30 days) is enforced by the test server, so the tests show that the connector honours the documentation.
"""

from __future__ import annotations

import datetime as dt
import threading
from typing import Any

import pytest
from sqlalchemy import select, update

from olcf.fabric.rest import FabricRestClient
from olcf.intelligence.discovery import fabric as F
from olcf.intelligence.discovery.ratelimit import SharedWindowLimit
from olcf.intelligence.discovery.scans import ScanRunner
from olcf.intelligence.discovery.schema import discovery_sources
from tests.fabric.fabric_fakes import FABRIC_WS, OTHER_FABRIC_WS
from tests.intelligence.discovery.test_fabric_connector import Fx, fx  # noqa: F401  (the fixture)

T0 = dt.datetime(2026, 3, 1, 12, 0, 0, tzinfo=dt.UTC)


class Clock:
    """The scan clock (``runner.now``) and the emulated service's clock (``world.pbi_now``)."""

    def __init__(self, start: dt.datetime = T0) -> None:
        self.t = start
        self.skew = dt.timedelta(0)           # how far the *service's* clock is behind ours

    def iso(self) -> str:
        return self.t.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    def service_now(self) -> dt.datetime:
        return self.t - self.skew

    def advance(self, **kw: float) -> None:
        self.t += dt.timedelta(**kw)


@pytest.fixture()
def clock(fx):  # noqa: F811
    c = Clock()
    fx.disc.d.runner.now = c.iso
    fx.world.pbi_now = c.service_now
    fx.clock = c
    yield c
    fx.disc.d.runner.now = None


def state_of(f: Fx) -> dict[str, Any]:
    with f.disc.ctx.db.connect() as c:
        return dict(c.execute(select(discovery_sources.c.state).where(discovery_sources.c.id == f.src["id"])).scalar_one())


def set_state(f: Fx, **changes: Any) -> None:
    st = state_of(f)
    st.update(changes)
    with f.disc.ctx.db.begin() as c:
        c.execute(update(discovery_sources).where(discovery_sources.c.id == f.src["id"]).values(state=st))


def reset_world_traffic(f: Fx) -> None:
    f.world.calls.clear()
    f.world.pbi_modified_calls.clear()
    f.world.pbi_scan_requests.clear()
    f.world.pbi_modified_ages.clear()


def quiet(f: Fx) -> None:
    """No workspace is reported as modified."""
    f.world.pbi_modified = {FABRIC_WS: "2000-01-01T00:00:00.000Z", OTHER_FABRIC_WS: "2000-01-01T00:00:00.000Z"}


def tables(f: Fx) -> set[str]:
    return {a["name"] for a in f.assets("table")}


# ------------------------------------------------------------------------------------------------ F06: durable retry set
def test_a_workspace_that_failed_is_retried_after_access_returns_although_nothing_changed(fx, clock):
    fx.create()
    fx.world.workspace_denied.add(OTHER_FABRIC_WS)
    first = fx.scan()
    assert first["status"] == "partial" and "ledger" not in tables(fx)
    retry = state_of(fx)["retry"]
    assert list(retry) == [OTHER_FABRIC_WS.lower()] and retry[OTHER_FABRIC_WS.lower()]["attempts"] == 1 and retry[OTHER_FABRIC_WS.lower()]["kind"] == "permission_denied"
    # access is restored; the workspace's content did not change and the modified list is empty
    fx.world.workspace_denied.discard(OTHER_FABRIC_WS)
    quiet(fx)
    clock.advance(minutes=10)
    reset_world_traffic(fx)
    row = fx.scan("incremental")
    assert row["status"] == "succeeded", [g["detail"] for g in fx.gaps()]
    assert "ledger" in tables(fx)                                                    # retried and repaired
    assert fx.paths(f"/workspaces/{FABRIC_WS}/items") == []                          # the healthy workspace was not read again
    assert state_of(fx)["retry"] == {}


def test_retries_back_off_and_are_never_abandoned(fx, clock):
    fx.create()
    fx.world.workspace_denied.add(OTHER_FABRIC_WS)
    fx.scan()
    key = OTHER_FABRIC_WS.lower()
    first = state_of(fx)["retry"][key]
    quiet(fx)
    clock.advance(minutes=1)                                                         # inside the first back-off (5 minutes)
    reset_world_traffic(fx)
    fx.scan("incremental")
    assert fx.paths(f"/workspaces/{OTHER_FABRIC_WS}/") == [] and state_of(fx)["retry"][key]["attempts"] == 1
    clock.advance(minutes=5)
    fx.scan("incremental")                                                           # due: tried (and fails again)
    second = state_of(fx)["retry"][key]
    assert fx.paths(f"/workspaces/{OTHER_FABRIC_WS}/") and second["attempts"] == 2 and second["first_failure"] == first["first_failure"]
    gap_s = (dt.datetime.fromisoformat(second["next_attempt"].replace("Z", "+00:00")) - dt.datetime.fromisoformat(second["last_attempt"].replace("Z", "+00:00"))).total_seconds()
    assert gap_s == 600                                                              # doubled
    for _ in range(12):                                                              # many failures: capped, still there
        clock.advance(hours=7)
        fx.scan("incremental")
    last = state_of(fx)["retry"][key]
    assert last["attempts"] >= 10
    assert (dt.datetime.fromisoformat(last["next_attempt"].replace("Z", "+00:00")) - dt.datetime.fromisoformat(last["last_attempt"].replace("Z", "+00:00"))).total_seconds() == F.RETRY_CAP


def test_the_retry_set_survives_a_restart(fx, clock):
    fx.create()
    fx.world.workspace_denied.add(OTHER_FABRIC_WS)
    fx.scan()
    assert OTHER_FABRIC_WS.lower() in state_of(fx)["retry"]                          # it is in the database, not in a process
    fresh = ScanRunner(fx.disc.ctx, fx.disc.d, fx.disc.d.sources)                    # a new process: no memory of the previous scan
    fresh.now, fresh.sleep = clock.iso, fx.sleeps.append
    fx.disc.d.runner = fresh
    fx.world.workspace_denied.discard(OTHER_FABRIC_WS)
    quiet(fx)
    clock.advance(minutes=10)
    assert fx.scan("incremental")["status"] == "succeeded" and "ledger" in tables(fx)


def test_a_scanner_phase_failure_is_retried_too(fx, clock):
    fx.create()
    fx.world.pbi_hourly_quota = 0                                                    # getInfo answers 429 with an hour-long Retry-After: the scanner phase gives up
    first = fx.scan()
    assert first["status"] == "partial" and any(g["kind"] == "api_unavailable" for g in fx.gaps())
    assert not any(c.kind == "scanner_result" for c in fx.rec.calls)
    retry = state_of(fx)["retry"]
    assert set(retry) == {FABRIC_WS.lower(), OTHER_FABRIC_WS.lower()}
    assert all(dt.datetime.fromisoformat(e["next_attempt"].replace("Z", "+00:00")) - clock.t >= dt.timedelta(minutes=59) for e in retry.values())     # waits out the quota
    fx.world.pbi_hourly_quota = None
    quiet(fx)
    clock.advance(minutes=30)                                                        # not yet due
    reset_world_traffic(fx)
    fx.scan("incremental")
    assert fx.world.pbi_scan_requests == [] and len(state_of(fx)["retry"]) == 2
    clock.advance(minutes=40)
    row = fx.scan("incremental")
    assert row["status"] == "succeeded", [g["detail"] for g in fx.gaps()]
    assert fx.world.pbi_scan_requests and sum(1 for c in fx.rec.calls if c.kind == "scanner_result") == 2 and state_of(fx)["retry"] == {}


def test_a_failure_inside_an_incremental_scan_joins_the_retry_set_and_a_vanished_workspace_leaves_it(fx, clock):
    fx.create()
    fx.scan()
    assert state_of(fx)["retry"] == {}
    clock.advance(minutes=10)
    fx.world.pbi_modified[FABRIC_WS] = clock.iso()
    fx.world.workspace_denied.add(FABRIC_WS)
    row = fx.scan("incremental")
    assert row["status"] == "partial" and FABRIC_WS.lower() in state_of(fx)["retry"]
    fx.world.workspaces = [w for w in fx.world.workspaces if w["id"] != FABRIC_WS]                       # the workspace is gone
    fx.world.workspace_denied.discard(FABRIC_WS)
    clock.advance(minutes=30)
    quiet(fx)
    fx.scan("incremental")
    assert FABRIC_WS.lower() not in state_of(fx)["retry"]


def test_a_cut_off_workspace_item_list_is_a_retry_not_a_silent_prefix(fx, clock, monkeypatch):
    fx.create()
    fx.world.items_page_size = 1
    monkeypatch.setattr(FabricRestClient, "DEFAULT_MAX_PAGES", 1)
    row = fx.scan()
    assert row["status"] == "partial" and set(state_of(fx)["retry"]) >= {FABRIC_WS.lower()}


# ------------------------------------------------------------------------------------------------ F07: modifiedSince
@pytest.mark.parametrize("minutes", [1, 5, 15, 60])
def test_every_modified_since_is_inside_the_documented_window_for_short_and_long_intervals(fx, clock, minutes):
    fx.create()
    fx.scan()
    quiet(fx)
    reset_world_traffic(fx)
    for _ in range(4):
        clock.advance(minutes=minutes)
        row = fx.scan("incremental")
        assert row["status"] == "succeeded", [g["detail"] for g in fx.gaps()]
        assert not [g for g in fx.gaps() if "modified-workspaces" in g["detail"]]
    ages = fx.world.pbi_modified_ages
    assert len(ages) == 4 and len(fx.world.pbi_modified_calls) == 4                              # one valid question per scan: no rejected value, no retry
    assert all(30 * 60 <= a <= 30 * 86400 for a in ages)
    assert fx.paths(f"/workspaces/{OTHER_FABRIC_WS}/") == [] and fx.paths(f"/workspaces/{FABRIC_WS}/items") == []     # and never a fallback to reading everything


def test_a_change_the_service_reports_late_is_eventually_scanned(fx, clock):
    fx.create()
    fx.scan()
    quiet(fx)
    clock.advance(minutes=1)
    fx.world.pbi_modified[OTHER_FABRIC_WS] = clock.iso()                                         # changed now ...
    fx.world.pbi_visible_at[OTHER_FABRIC_WS] = clock.t + dt.timedelta(minutes=24)                # ... but reported by the service only 24 minutes later
    fx.world.add_lakehouse_table(OTHER_FABRIC_WS, "eeeeeeee-0000-4000-8000-0000000000e1", "late_table", schema=[("k", "long")])
    seen = False
    for _ in range(8):                                                                           # scans every 5 minutes
        clock.advance(minutes=5)
        fx.scan("incremental")
        if "late_table" in tables(fx):
            seen = True
            break
    assert seen, "the late-reported change was never picked up"


def test_a_service_clock_behind_ours_is_corrected_with_one_retry(fx, clock):
    fx.create()
    fx.scan()
    quiet(fx)
    clock.skew = dt.timedelta(minutes=20)                                                        # our clock is 20 minutes ahead of the service's
    clock.advance(minutes=5)
    reset_world_traffic(fx)
    row = fx.scan("incremental")
    assert row["status"] == "succeeded", [g["detail"] for g in fx.gaps()]
    assert len(fx.world.pbi_modified_calls) == 2 and fx.world.pbi_modified_ages[0] < 30 * 60 <= fx.world.pbi_modified_ages[1]
    assert fx.paths(f"/workspaces/{OTHER_FABRIC_WS}/") == []                                     # still no full read


def test_a_service_clock_ahead_of_ours_is_fine(fx, clock):
    fx.create()
    fx.scan()
    quiet(fx)
    clock.skew = -dt.timedelta(minutes=20)
    clock.advance(minutes=5)
    reset_world_traffic(fx)
    assert fx.scan("incremental")["status"] == "succeeded" and len(fx.world.pbi_modified_calls) == 1


def test_the_thirty_day_boundary(fx, clock):
    fx.create()
    fx.scan()
    quiet(fx)
    reset_world_traffic(fx)
    # a watermark 29 days old is still inside the window: one valid question, no full read
    set_state(fx, pbi_watermark=F._iso(clock.t - dt.timedelta(days=29)))
    row = fx.scan("incremental")
    assert row["status"] == "succeeded" and len(fx.world.pbi_modified_calls) == 1 and fx.world.pbi_modified_ages[0] <= 30 * 86400
    assert fx.paths(f"/workspaces/{OTHER_FABRIC_WS}/") == []
    # older than the window: the API cannot be asked, everything is read once (informational), and the watermark is fresh again afterwards
    reset_world_traffic(fx)
    set_state(fx, pbi_watermark=F._iso(clock.t - dt.timedelta(days=30)))
    row = fx.scan("incremental")
    assert row["status"] == "succeeded" and fx.world.pbi_modified_calls == [] and fx.paths(f"/workspaces/{OTHER_FABRIC_WS}/")
    reset_world_traffic(fx)
    clock.advance(minutes=45)
    fx.scan("incremental")
    assert len(fx.world.pbi_modified_calls) == 1 and fx.paths(f"/workspaces/{OTHER_FABRIC_WS}/") == []        # no perpetual full scan


def test_a_future_watermark_is_clamped_into_the_window(fx, clock):
    fx.create()
    fx.scan()
    quiet(fx)
    set_state(fx, pbi_watermark=F._iso(clock.t + dt.timedelta(hours=3)))                          # wall clock stepped backwards
    reset_world_traffic(fx)
    assert fx.scan("incremental")["status"] == "succeeded"
    assert len(fx.world.pbi_modified_calls) == 1 and 30 * 60 <= fx.world.pbi_modified_ages[0] <= 30 * 86400


def test_the_request_budget_is_respected_and_a_spent_budget_defers_without_a_full_scan(fx, clock):
    fx.create()
    fx.disc.d.runner.limits = lambda: {"modified_requests_per_hour": 2}                           # type: ignore[method-assign]
    fx.scan()
    quiet(fx)
    reset_world_traffic(fx)
    marks = []
    for _ in range(3):
        clock.advance(minutes=3)
        row = fx.scan("incremental")
        assert row["status"] == "succeeded"
        marks.append(state_of(fx)["pbi_watermark"])
    assert len(fx.world.pbi_modified_calls) == 2                                                  # the third scan did not ask
    assert marks[2] == marks[1]                                                                   # and did not move the watermark: the next scan asks from the same point
    assert any("budget" in g["detail"] for g in fx.gaps())
    assert fx.paths(f"/workspaces/{OTHER_FABRIC_WS}/") == []
    clock.advance(minutes=61)                                                                     # the window slides
    fx.scan("incremental")
    assert len(fx.world.pbi_modified_calls) == 3


def test_new_workspaces_are_still_scanned_while_the_budget_is_spent(fx, clock):
    fx.create()
    fx.disc.d.runner.limits = lambda: {"modified_requests_per_hour": 1}                           # type: ignore[method-assign]
    fx.scan()
    quiet(fx)
    clock.advance(minutes=2)
    fx.scan("incremental")                                                                        # uses the only slot
    fx.world.workspaces.append({"id": "77777777-0000-4000-8000-000000000007", "displayName": "Brand new", "type": "Workspace"})
    clock.advance(minutes=2)
    fx.scan("incremental")
    assert "Brand new" in {a["name"] for a in fx.assets("workspace")}


def test_the_budget_is_shared_between_sources_of_one_tenant(fx, clock):
    fx.disc.d.runner.limits = lambda: {"modified_requests_per_hour": 1}                           # type: ignore[method-assign]
    fx.create("one")
    fx.scan()
    quiet(fx)
    clock.advance(minutes=2)
    reset_world_traffic(fx)
    fx.scan("incremental")
    assert len(fx.world.pbi_modified_calls) == 1
    other = Fx(fx.disc, fx.world)
    other.sleeps = fx.sleeps
    other.create("two")
    other.scan()
    clock.advance(minutes=2)
    reset_world_traffic(fx)
    other.scan("incremental")
    assert fx.world.pbi_modified_calls == []                                                      # the second source found the tenant's budget spent


def test_a_transient_modified_api_failure_defers_instead_of_reading_everything(fx, clock):
    fx.create()
    fx.scan()
    quiet(fx)
    clock.advance(minutes=45)
    before = state_of(fx)["pbi_watermark"]
    fx.world.pbi_modified_failures = 10
    reset_world_traffic(fx)
    row = fx.scan("incremental")
    assert row["status"] == "partial" and fx.paths(f"/workspaces/{OTHER_FABRIC_WS}/") == [] and state_of(fx)["pbi_watermark"] == before
    fx.world.pbi_modified_failures = 0
    clock.advance(minutes=10)
    row = fx.scan("incremental")
    assert row["status"] == "succeeded" and state_of(fx)["pbi_watermark"] != before


def test_the_watermark_is_kept_behind_the_scan_start_by_the_documented_minimum(fx, clock):
    fx.create()
    fx.scan()
    wm = dt.datetime.fromisoformat(state_of(fx)["pbi_watermark"].replace("Z", "+00:00"))
    assert clock.t - wm >= dt.timedelta(minutes=30)


class _Ctx:
    """A database-less context: the in-memory budget."""
    db = None


def test_the_shared_budget_slides_and_is_exact_under_contention(disc):
    now = {"t": 1_000_000.0}
    a = SharedWindowLimit(disc.ctx, "t1", 3, window=3600, clock=lambda: now["t"])
    b = SharedWindowLimit(disc.ctx, "t1", 3, window=3600, clock=lambda: now["t"])                  # another replica
    assert [a.reserve(), b.reserve(), a.reserve()] == [0.0, 0.0, 0.0]
    wait = b.reserve()
    assert 3590 < wait <= 3600 and a.used() == 3
    now["t"] += 3601
    assert a.reserve() == 0.0 and b.used() == 1
    got: list[float] = []
    contended = [SharedWindowLimit(disc.ctx, "t2", 5, window=3600, clock=lambda: now["t"]) for _ in range(16)]
    threads = [threading.Thread(target=lambda lim=lim: got.append(lim.reserve())) for lim in contended]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sum(1 for w in got if w == 0.0) == 5 and contended[0].used() == 5                      # never more than the limit, whatever the interleaving
    mem = SharedWindowLimit(_Ctx(), "mem-only", 1, clock=lambda: now["t"])
    assert mem.reserve() == 0.0 and mem.reserve() > 0


# ------------------------------------------------------------------------------------------------ F08: incomplete inventories
def test_a_cut_off_item_listing_makes_the_full_scan_partial_and_removes_nothing(fx, clock, monkeypatch):
    fx.create()
    assert fx.scan()["status"] == "succeeded"
    known = {a["name"] for a in fx.assets("notebook")} | tables(fx)
    assert {"nb_load", "orders"} <= known
    fx.world.items_page_size = 1
    monkeypatch.setattr(FabricRestClient, "DEFAULT_MAX_PAGES", 1)
    row = fx.scan()
    assert row["status"] == "partial" and "sweep" not in row["stats"]
    assert any(g["kind"] == "api_unavailable" and "incomplete" in g["detail"] for g in fx.gaps())
    assert {a["name"] for a in fx.assets("notebook")} | tables(fx) >= known                      # nothing unseen was removed
    assert not fx.disc.assets(status="removed", source_id=fx.src["id"])


def test_a_cut_off_workspace_listing_is_partial_keeps_what_arrived_and_forgets_nothing(fx, clock, monkeypatch):
    fx.create()
    fx.scan()
    known_before = set(state_of(fx)["workspaces"])
    fx.world.workspaces_page_size = 1
    monkeypatch.setattr(FabricRestClient, "DEFAULT_MAX_PAGES", 1)
    row = fx.scan()
    assert row["status"] == "partial" and "sweep" not in row["stats"]
    assert known_before <= set(state_of(fx)["workspaces"])
    assert not fx.disc.assets(status="removed", source_id=fx.src["id"])


def test_a_cut_off_table_listing_is_partial_and_keeps_known_tables(fx, clock, monkeypatch):
    fx.create()
    fx.scan()
    fx.world.tables_page_size = 1
    monkeypatch.setattr(FabricRestClient, "DEFAULT_MAX_PAGES", 1)
    row = fx.scan()
    assert row["status"] == "partial" and "sweep" not in row["stats"]
    assert {"orders", "customers"} <= tables(fx)
    assert FABRIC_WS.lower() in state_of(fx)["retry"]


def test_a_cut_off_onelake_listing_is_a_blocking_gap(fx, clock, monkeypatch):
    fx.create()
    fx.world.onelake_page_size = 1
    monkeypatch.setattr(FabricRestClient, "DEFAULT_MAX_ONELAKE_PAGES", 1)
    row = fx.scan()
    assert row["status"] == "partial"
    assert any("Delta log" in g["detail"] for g in fx.gaps())
    assert not fx.disc.assets(status="removed", source_id=fx.src["id"])


def test_complete_listings_still_sweep(fx, clock):
    fx.create()
    fx.scan()
    fx.world.items_page_size = 1                                                                  # paging works, nothing is cut off
    assert fx.scan()["status"] == "succeeded"
