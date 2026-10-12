# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The event bus delivers to in-process subscribers without ever holding a publisher for long: a slow, hung or raising subscriber is isolated (system-integration finding).

Before the fix subscribers were called inline, so one subscriber that slept 5 seconds made every ``publish`` (a job state change, a SQL statement, a pipeline run) take 5 seconds.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Any

import pytest

from olcf.core import events as ev
from olcf.core.events import EventBus
from olcf.db.engine import Database


@pytest.fixture()
def bus(tmp_path: Path) -> EventBus:
    db = Database(f"sqlite:///{tmp_path / 'bus.db'}")
    db.migrate()
    return EventBus(db)


def test_fast_subscribers_still_look_synchronous(bus: EventBus) -> None:
    got: list[str] = []
    bus.subscribe(lambda e: got.append(e["type"]))
    for i in range(50):
        bus.publish(f"t.{i}", {})
        assert got[-1] == f"t.{i}"                       # delivered by the time publish returns
    assert got == [f"t.{i}" for i in range(50)]          # in order


def test_a_hanging_subscriber_does_not_block_publishers(bus: EventBus) -> None:
    release = threading.Event()
    seen: list[str] = []
    fast: list[str] = []
    bus.subscribe(lambda e: (seen.append(e["type"]), release.wait(5)))        # hangs "for 5 seconds" on its first event
    bus.subscribe(lambda e: fast.append(e["type"]))
    t0 = time.monotonic()
    bus.publish("first", {})
    first = time.monotonic() - t0
    assert first < ev.WAIT_BUDGET + 0.5                                       # waited at most the budget, not 5 s
    t1 = time.monotonic()
    for i in range(20):
        bus.publish(f"later.{i}", {})
    assert time.monotonic() - t1 < 1.0                                        # the stalled subscriber is no longer waited for at all
    assert fast == ["first", *[f"later.{i}" for i in range(20)]]              # the healthy subscriber saw everything, immediately
    release.set()                                                             # recovery: the queued events are delivered, in order
    deadline = time.monotonic() + 5
    while len(seen) < 21 and time.monotonic() < deadline:
        time.sleep(0.02)
    assert seen == ["first", *[f"later.{i}" for i in range(20)]]


def test_a_raising_subscriber_is_isolated_and_events_are_persisted(bus: EventBus) -> None:
    def boom(_e: dict[str, Any]) -> None:
        raise RuntimeError("subscriber bug")

    got: list[str] = []
    bus.subscribe(boom)
    bus.subscribe(lambda e: got.append(e["type"]))
    evt = bus.publish("x.y", {"a": 1})
    assert got == ["x.y"] and evt["seq"]
    assert [e["type"] for e in bus.list()] == ["x.y"]


def test_publishing_from_inside_a_subscriber_never_waits_or_deadlocks(bus: EventBus) -> None:
    got: list[str] = []

    def chain(e: dict[str, Any]) -> None:
        if e["type"] == "a":
            bus.publish("b", {})

    bus.subscribe(chain)
    bus.subscribe(lambda e: got.append(e["type"]))
    t0 = time.monotonic()
    bus.publish("a", {})
    assert time.monotonic() - t0 < ev.WAIT_BUDGET
    deadline = time.monotonic() + 3
    while got != ["a", "b"] and time.monotonic() < deadline:
        time.sleep(0.01)
    assert got == ["a", "b"]


def test_a_stuck_subscriber_has_a_bounded_backlog(bus: EventBus, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ev, "LANE_CAPACITY", 5)
    monkeypatch.setattr(ev, "WAIT_BUDGET", 0.05)
    release = threading.Event()
    seen: list[str] = []
    bus.subscribe(lambda e: (seen.append(e["type"]), release.wait(5)))
    for i in range(30):
        bus.publish(f"e.{i}", {})
    release.set()
    deadline = time.monotonic() + 5
    while seen[-1:] != ["e.29"] and time.monotonic() < deadline:
        time.sleep(0.02)
    assert seen[0] == "e.0" and seen[-1] == "e.29" and len(seen) <= 7          # the oldest queued events were dropped, the newest are kept


def test_unsubscribe_stops_delivery(bus: EventBus) -> None:
    got: list[str] = []
    off = bus.subscribe(lambda e: got.append(e["type"]))
    bus.publish("one", {})
    off()
    bus.publish("two", {})
    assert got == ["one"]
