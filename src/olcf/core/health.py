# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Component health registry.

Every service registers one or more :class:`HealthCheck` objects. The UI "traffic light"
panels, ``GET /v1/admin/health`` and the "Test connectivity" buttons all use this registry:

* ``quick`` - cheap, passive, safe to call every few seconds (cached for ``ttl`` seconds)
* ``deep``  - active end-to-end test (opens sockets, reads/writes storage, runs a Spark smoke test...)
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from olcf.core.timeutil import now_iso


class Status(StrEnum):
    """Health status of a component: ``ok``, ``unknown``, ``degraded`` or ``down``, with a ``rank`` used to take the worst of several."""
    OK = "ok"
    DEGRADED = "degraded"
    DOWN = "down"
    UNKNOWN = "unknown"

    @property
    def rank(self) -> int:
        return {"ok": 0, "unknown": 1, "degraded": 2, "down": 3}[self.value]


@dataclass
class ComponentHealth:
    """Result of one health check: component name, status, detail, latency, category, check time, labels and optional sub-steps of a deep test.
    """
    name: str
    status: Status
    detail: str = ""
    latency_ms: float | None = None
    category: str = "platform"
    checked_at: str = field(default_factory=now_iso)
    labels: dict[str, Any] = field(default_factory=dict)
    #: Optional structured sub-results (e.g. one entry per step of a deep test).
    steps: list[dict[str, Any]] = field(default_factory=list)
    mode: str = "quick"

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status.value,
            "detail": self.detail,
            "latency_ms": None if self.latency_ms is None else round(self.latency_ms, 2),
            "category": self.category,
            "checked_at": self.checked_at,
            "labels": self.labels,
            "steps": self.steps,
            "mode": self.mode,
        }


CheckFn = Callable[[], ComponentHealth]


@dataclass
class HealthCheck:
    """Definition of a health check: a quick function, an optional deep function, a cache time-to-live and whether the component is critical to overall health.
    """
    name: str
    category: str
    description: str
    quick: CheckFn
    deep: CheckFn | None = None
    ttl: float = 5.0
    critical: bool = False  # critical components down => overall "down"


def timed(name: str, category: str, fn: Callable[[], tuple[Status, str] | tuple[Status, str, dict[str, Any]]], *, mode: str = "quick") -> ComponentHealth:
    """Run ``fn`` and wrap its ``(status, detail[, labels])`` result, capturing exceptions."""
    start = time.perf_counter()
    try:
        res = fn()
        status, detail = res[0], res[1]
        labels = res[2] if len(res) > 2 else {}  # type: ignore[misc]
    except Exception as exc:  # noqa: BLE001 - a failing check must never raise
        status, detail, labels = Status.DOWN, f"{type(exc).__name__}: {exc}", {}
    return ComponentHealth(
        name=name, status=status, detail=detail, category=category, labels=labels, mode=mode,
        latency_ms=(time.perf_counter() - start) * 1000.0,
    )


class HealthRegistry:
    """Registry of health checks. It runs them, caches quick results for each check's time-to-live and combines them into an overall status.
    """
    def __init__(self) -> None:
        self._checks: dict[str, HealthCheck] = {}
        self._cache: dict[str, tuple[float, ComponentHealth]] = {}
        self._lock = threading.RLock()

    def register(self, check: HealthCheck) -> None:
        with self._lock:
            self._checks[check.name] = check

    def unregister(self, name: str) -> None:
        with self._lock:
            self._checks.pop(name, None)
            self._cache.pop(name, None)

    def names(self) -> list[str]:
        with self._lock:
            return sorted(self._checks)

    def describe(self) -> list[dict[str, Any]]:
        with self._lock:
            return [
                {"name": c.name, "category": c.category, "description": c.description, "deep": c.deep is not None, "critical": c.critical}
                for c in sorted(self._checks.values(), key=lambda c: (c.category, c.name))
            ]

    def run(self, name: str, *, deep: bool = False, force: bool = False) -> ComponentHealth:
        with self._lock:
            check = self._checks.get(name)
        if check is None:
            raise KeyError(name)
        if deep and check.deep is not None:
            result = self._invoke(check.deep, check, "deep")
            with self._lock:
                self._cache[name] = (time.monotonic(), result)
            return result
        with self._lock:
            cached = self._cache.get(name)
        if cached and not force and (time.monotonic() - cached[0]) < check.ttl:
            return cached[1]
        result = self._invoke(check.quick, check, "quick")
        with self._lock:
            self._cache[name] = (time.monotonic(), result)
        return result

    @staticmethod
    def _invoke(fn: CheckFn, check: HealthCheck, mode: str) -> ComponentHealth:
        start = time.perf_counter()
        try:
            res = fn()
        except Exception as exc:  # noqa: BLE001
            res = ComponentHealth(check.name, Status.DOWN, f"{type(exc).__name__}: {exc}")
        res.name = check.name
        res.category = res.category if res.category != "platform" else check.category
        res.mode = mode
        if res.latency_ms is None:
            res.latency_ms = (time.perf_counter() - start) * 1000.0
        return res

    def snapshot(self, *, force: bool = False) -> dict[str, Any]:
        results = [self.run(n, force=force) for n in self.names()]
        overall = Status.OK
        with self._lock:
            critical = {n for n, c in self._checks.items() if c.critical}
        for r in results:
            if r.status is Status.DOWN and r.name in critical:
                overall = Status.DOWN
                break
            if r.status.rank > overall.rank and r.status is not Status.UNKNOWN:
                overall = Status.DEGRADED if r.status is not Status.DOWN or r.name not in critical else Status.DOWN
        return {"status": overall.value, "checked_at": now_iso(), "components": [r.to_dict() for r in results]}
