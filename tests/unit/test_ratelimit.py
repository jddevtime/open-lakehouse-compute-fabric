# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""FIX N: the in-memory rate limiter keeps working as a sliding window and can no longer grow without bound."""

from __future__ import annotations

import pytest

from olcf.core import ratelimit
from olcf.core.errors import TooManyRequests
from olcf.core.ratelimit import RateLimiter


class Clock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


@pytest.fixture()
def clock(monkeypatch):
    c = Clock()
    monkeypatch.setattr(ratelimit.time, "monotonic", c)
    return c


def test_sliding_window_semantics_are_unchanged(clock):
    rl = RateLimiter()
    for _ in range(3):
        rl.check("k", limit=3, window=60)
    with pytest.raises(TooManyRequests) as ei:
        rl.check("k", limit=3, window=60)
    assert 1 <= ei.value.details["retry_after_seconds"] <= 61
    rl.check("other", limit=3, window=60)       # keys are independent
    clock.t += 61
    rl.check("k", limit=3, window=60)           # the window slid
    rl.reset("k")
    for _ in range(3):
        rl.check("k", limit=3, window=60)


def test_idle_keys_are_swept(clock):
    rl = RateLimiter(max_keys=1000, sweep_interval=30)
    for i in range(500):
        rl.check(f"ip:{i}", limit=5, window=60)
    assert len(rl) == 500
    clock.t += 120                                # every window expired
    rl.check("fresh", limit=5, window=60)         # the next check triggers the sweep
    assert len(rl) == 1


def test_table_is_bounded_under_a_flood_of_random_keys(clock):
    rl = RateLimiter(max_keys=100, sweep_interval=1e9)   # sweeping disabled: only the hard cap protects
    for i in range(10_000):
        rl.check(f"flood:{i}", limit=5, window=3600)
    assert len(rl) == 100


def test_eviction_drops_the_least_recently_used_key_not_a_busy_one(clock):
    rl = RateLimiter(max_keys=3, sweep_interval=1e9)
    rl.check("busy", limit=2, window=3600)
    rl.check("busy", limit=2, window=3600)
    for i in range(10):
        rl.check(f"noise:{i}", limit=2, window=3600)
        with pytest.raises(TooManyRequests):      # touching "busy" keeps it the most recently used key
            rl.check("busy", limit=2, window=3600)
    assert len(rl) == 3
    with pytest.raises(TooManyRequests):
        rl.check("busy", limit=2, window=3600)


def test_invalid_size():
    with pytest.raises(ValueError):
        RateLimiter(max_keys=0)
