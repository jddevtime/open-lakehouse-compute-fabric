# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Small in-process sliding-window limiter (login throttling, public webhook endpoints, ...).

The key space is attacker-controlled (client addresses, user names, trigger ids), so the limiter must not grow without bound:

* keys whose last hit is older than their window are worthless and are swept out periodically (every ``sweep_interval`` seconds, piggy-backed on ``check``);
* the table never holds more than ``max_keys`` keys: beyond that the least recently used keys are evicted (an evicted key simply starts a fresh window).
  Eviction is the last resort; with the default of 10 000 keys the table stays in the low megabytes even under a flood of random keys.

The limiter is per process (see CONTRACTS S8: the effective limit is ``limit x nodes``).
"""

from __future__ import annotations

import threading
import time
from collections import OrderedDict, deque

from olcf.core.errors import TooManyRequests


class RateLimiter:
    """Sliding-window rate limiter keyed by an arbitrary string. It holds a bounded number of keys, evicting the least recently used, and sweeps idle ones periodically.
    """
    def __init__(self, *, max_keys: int = 10_000, sweep_interval: float = 30.0) -> None:
        if max_keys < 1:
            raise ValueError("max_keys must be at least 1")
        self.max_keys = max_keys
        self.sweep_interval = sweep_interval
        #: key -> (hit times, window of the last check); ordered least -> most recently used
        self._hits: OrderedDict[str, tuple[deque[float], float]] = OrderedDict()
        self._lock = threading.Lock()
        self._last_sweep = time.monotonic()

    def check(self, key: str, *, limit: int, window: float = 60.0) -> None:
        now = time.monotonic()
        with self._lock:
            if now - self._last_sweep >= self.sweep_interval:
                self._sweep(now)
            q = self._hits[key][0] if key in self._hits else deque()
            while q and now - q[0] > window:
                q.popleft()
            if len(q) >= limit:
                self._hits[key] = (q, window)
                self._hits.move_to_end(key)
                raise TooManyRequests("too many requests, slow down", details={"retry_after_seconds": int(window - (now - q[0])) + 1})
            q.append(now)
            self._hits[key] = (q, window)
            self._hits.move_to_end(key)
            while len(self._hits) > self.max_keys:
                self._hits.popitem(last=False)

    def reset(self, key: str) -> None:
        with self._lock:
            self._hits.pop(key, None)

    def __len__(self) -> int:
        """Number of keys currently tracked (diagnostics and tests)."""
        with self._lock:
            return len(self._hits)

    def _sweep(self, now: float) -> None:
        self._last_sweep = now
        for k in [k for k, (q, window) in self._hits.items() if not q or now - q[-1] > window]:
            del self._hits[k]
