# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""A request budget shared by every discovery source and every control-plane replica that talks to the same remote API.

The Power BI admin API documents hard hourly limits per tenant (30 requests per hour for *Get Modified Workspaces*). Several Fabric sources can point at one tenant and several
replicas can run scans, so a per-process counter would not keep the tenant under the limit. :class:`SharedWindowLimit` keeps a sliding window of request times in one row of the
platform's ``kv`` table and reserves a slot with an optimistic compare-and-swap (the row's ``updated_at`` is the version), so two processes can never both take the last slot.

Without a database (pure unit tests) the budget lives in memory. Time is injectable for tests; it must be wall-clock (epoch seconds), because it is compared across processes.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError

from olcf.core.timeutil import now_iso
from olcf.db.schema import kv
from olcf.db.util import fetch_one

_MEMORY: dict[str, list[float]] = {}
_MEMORY_LOCK = threading.Lock()
_ATTEMPTS = 12


class SharedWindowLimit:
    """At most ``limit`` reservations per ``window`` seconds for one ``key`` across all processes sharing the database."""

    def __init__(self, ctx: Any, key: str, limit: int, *, window: float = 3600.0, clock: Callable[[], float] = time.time) -> None:
        self.ctx, self.key, self.limit, self.window, self.clock = ctx, f"ratelimit:{key}"[:191], max(1, int(limit)), float(window), clock

    def reserve(self) -> float:
        """Take one slot now (returns ``0.0``) or, when the window is full, take nothing and return the seconds until a slot frees up."""
        if self.ctx is None or getattr(self.ctx, "db", None) is None:
            return self._memory()
        for _ in range(_ATTEMPTS):
            now = self.clock()
            with self.ctx.db.connect() as c:
                row = fetch_one(c, select(kv).where(kv.c.key == self.key))
            calls = [float(t) for t in ((row or {}).get("value") or {}).get("calls", []) if now - float(t) < self.window]
            if len(calls) >= self.limit:
                return max(0.0, min(calls) + self.window - now)
            doc = {"calls": [*calls, now]}
            try:
                with self.ctx.db.begin() as c:
                    if row is None:
                        c.execute(kv.insert().values(key=self.key, value=doc, updated_at=now_iso()))
                    elif not c.execute(update(kv).where(kv.c.key == self.key, kv.c.updated_at == row["updated_at"]).values(value=doc, updated_at=now_iso())).rowcount:
                        continue                                   # another process changed the row between our read and write: read again
            except IntegrityError:
                continue                                           # another process created the row first
            return 0.0
        return self.window / self.limit                           # contention that never settled: behave as "full" for a short while rather than exceed the limit

    def used(self) -> int:
        """Reservations currently inside the window (diagnostics)."""
        now = self.clock()
        if self.ctx is None or getattr(self.ctx, "db", None) is None:
            with _MEMORY_LOCK:
                return len([t for t in _MEMORY.get(self.key, []) if now - t < self.window])
        with self.ctx.db.connect() as c:
            row = fetch_one(c, select(kv).where(kv.c.key == self.key))
        return len([t for t in ((row or {}).get("value") or {}).get("calls", []) if now - float(t) < self.window])

    def _memory(self) -> float:
        with _MEMORY_LOCK:
            now = self.clock()
            calls = [t for t in _MEMORY.get(self.key, []) if now - t < self.window]
            if len(calls) >= self.limit:
                _MEMORY[self.key] = calls
                return max(0.0, min(calls) + self.window - now)
            _MEMORY[self.key] = [*calls, now]
            return 0.0
