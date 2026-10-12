# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Test-only pipeline activity types (registered as plugins) that make engine behaviour observable and controllable.

``t_probe``       blocking: records every execution, can fail the first N attempts, sleep (cancellation-aware), block on a named gate, and tracks concurrency.
``t_probe_safe``  the same, declared safe to execute again after a crash (``resumable``).
``t_async``       asynchronous (start / poll): records starts, completes after N polls or when its gate opens; its state survives a control-plane crash.
``t_stubborn``    blocking and NON-cooperative: sleeps through cancellation, then performs its (counted) side effect; every attempt is recorded as a ``SPANS`` entry so tests can prove
                  that attempts never overlap.  ``stubbornAttempts`` limits how many attempts misbehave (later attempts return at once).
``t_job``         asynchronous, models an external job whose cancel request takes ``cancelDelay`` seconds to take effect (``terminal_at`` of the span); ``cancel()`` is recorded.
"""

from __future__ import annotations

import threading
import time
from typing import Any

from olcf.pipelines.activities.base import Activity, ActivityCancelled, ActivityError, X, obj

LOCK = threading.Lock()
SPANS: list[dict[str, Any]] = []     # one entry per attempt of t_stubborn / t_job: {name, attempt, start, end, effect}
EFFECTS: list[dict[str, Any]] = []   # every committed side effect of t_stubborn
CANCELS: list[dict[str, Any]] = []   # every call of t_job.cancel()
CALLS: list[dict[str, Any]] = []
STARTS: list[dict[str, Any]] = []
GATES: dict[str, threading.Event] = {}
ACTIVE = {"now": 0, "max": 0}


def reset() -> None:
    with LOCK:
        CALLS.clear()
        SPANS.clear()
        EFFECTS.clear()
        CANCELS.clear()
        STARTS.clear()
        GATES.clear()
        ACTIVE.update(now=0, max=0)


def gate(name: str) -> threading.Event:
    with LOCK:
        return GATES.setdefault(name, threading.Event())


def calls(name: str | None = None) -> list[dict[str, Any]]:
    with LOCK:
        return [c for c in CALLS if name is None or c["name"] == name]


class Probe(Activity):
    type = "t_probe"
    mode = "blocking"
    title = "Test probe"
    description = "Test-only activity."
    properties_schema = obj({"value": {}, "failTimes": X({"type": "integer"}), "fail": X({"type": "boolean"}), "sleep": X({"type": "number"}), "gate": {"type": "string"},
                             "retryable": X({"type": "boolean"}), "secret": {}})

    def execute(self, actx: Any) -> dict[str, Any]:
        p = actx.props
        with LOCK:
            CALLS.append({"name": actx.name, "attempt": actx.attempt, "run": actx.run_id, "value": p.get("value"), "at": time.monotonic(), "resuming": actx.resuming})
            ACTIVE["now"] += 1
            ACTIVE["max"] = max(ACTIVE["max"], ACTIVE["now"])
        try:
            if p.get("gate"):
                ev = gate(p["gate"])
                while not ev.is_set():
                    if actx.sleep(0.02):
                        raise ActivityCancelled()
            if p.get("sleep") and actx.sleep(float(p["sleep"])):
                raise ActivityCancelled()
            if p.get("fail") or actx.attempt <= int(p.get("failTimes") or 0):
                raise ActivityError(f"boom (attempt {actx.attempt})", category="test", code="boom", retryable=p.get("retryable", True))
            return {"value": p.get("value"), "attempt": actx.attempt}
        finally:
            with LOCK:
                ACTIVE["now"] -= 1


class SafeProbe(Probe):
    type = "t_probe_safe"
    resumable = True


class AsyncProbe(Activity):
    type = "t_async"
    mode = "async"
    title = "Test async probe"
    description = "Test-only asynchronous activity."
    properties_schema = obj({"value": {}, "ticks": {"type": "integer"}, "gate": {"type": "string"}})

    def start(self, actx: Any) -> None:
        with LOCK:
            STARTS.append({"name": actx.name, "run": actx.run_id, "attempt": actx.attempt})
        actx.save_state(polls=0, started=True)

    def poll(self, actx: Any) -> dict[str, Any] | None:
        if actx.is_cancelled():
            raise ActivityCancelled()
        n = int(actx.state.get("polls", 0)) + 1
        actx.save_state(polls=n)
        g = actx.props.get("gate")
        if g and not gate(g).is_set():
            return None
        if not g and n < int(actx.props.get("ticks") or 1):
            return None
        return {"value": actx.props.get("value"), "polls": n}

    def poll_interval(self, actx: Any) -> float:
        return 0.05


class StubbornProbe(Activity):
    """Ignores cancellation completely (a thread that is stuck in a driver call): sleeps ``sleep`` seconds, then commits one side effect."""

    type = "t_stubborn"
    mode = "blocking"
    title = "Test stubborn probe"
    description = "Test-only activity that does not cooperate with cancellation."
    properties_schema = obj({"sleep": {"type": "number"}, "stubbornAttempts": {"type": "integer"}, "value": {}})

    def execute(self, actx: Any) -> dict[str, Any]:
        p = actx.props
        span = {"name": actx.name, "attempt": actx.attempt, "start": time.monotonic(), "end": None, "effect": False}
        with LOCK:
            SPANS.append(span)
            CALLS.append({"name": actx.name, "attempt": actx.attempt, "run": actx.run_id, "at": span["start"], "resuming": actx.resuming})
            ACTIVE["now"] += 1
            ACTIVE["max"] = max(ACTIVE["max"], ACTIVE["now"])
        try:
            if actx.attempt <= int(p.get("stubbornAttempts") or 99):
                time.sleep(float(p.get("sleep") or 0))   # deliberately NOT actx.sleep(): the cancellation flag is ignored
            with LOCK:
                EFFECTS.append({"name": actx.name, "attempt": actx.attempt, "at": time.monotonic()})
                span["effect"] = True
            return {"value": p.get("value"), "attempt": actx.attempt}
        finally:
            with LOCK:
                span["end"] = time.monotonic()
                ACTIVE["now"] -= 1


class JobProbe(Activity):
    """An external job: the cancel request is acknowledged only ``cancelDelay`` seconds later (the job is terminal from then on)."""

    type = "t_job"
    mode = "async"
    title = "Test job probe"
    description = "Test-only asynchronous activity with a delayed cancel."
    properties_schema = obj({"cancelDelay": {"type": "number"}, "runFor": {"type": "number"}, "value": {}, "stubbornAttempts": {"type": "integer"}})

    def start(self, actx: Any) -> None:
        span = {"name": actx.name, "attempt": actx.attempt, "start": time.monotonic(), "end": None, "effect": False}
        with LOCK:
            SPANS.append(span)
            STARTS.append({"name": actx.name, "run": actx.run_id, "attempt": actx.attempt})
        actx.save_state(started_at=time.time(), attempt=actx.attempt, cancel_at=None)  # state_data survives retries: reset per attempt

    def poll(self, actx: Any) -> dict[str, Any] | None:
        p = actx.props
        st = actx.state
        now = time.time()
        with LOCK:
            span = next((x for x in reversed(SPANS) if x["name"] == actx.name and x["attempt"] == actx.attempt), None)
        cancel_at = st.get("cancel_at")
        if cancel_at is not None and now >= cancel_at + float(p.get("cancelDelay") or 0):
            if span is not None and span["end"] is None:
                span["end"] = time.monotonic()
            raise ActivityCancelled()
        if actx.attempt <= int(p.get("stubbornAttempts") or 99) and now < st["started_at"] + float(p.get("runFor") or 0):
            return None
        if span is not None and span["end"] is None:
            span["end"] = time.monotonic()
            span["effect"] = True
        return {"value": p.get("value"), "attempt": actx.attempt}

    def cancel(self, actx: Any) -> None:
        with LOCK:
            CANCELS.append({"name": actx.name, "attempt": actx.attempt, "at": time.monotonic()})
        if actx.state.get("cancel_at") is None:
            try:
                actx.save_state(cancel_at=time.time())
            except Exception:  # noqa: BLE001
                pass

    def poll_interval(self, actx: Any) -> float:
        return 0.05


PROBES = [Probe, SafeProbe, AsyncProbe, StubbornProbe, JobProbe]
