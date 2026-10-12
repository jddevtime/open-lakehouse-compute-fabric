# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The browser-test harness (review finding F12): Spark master port blocks never overlap, and a control plane that dies at startup fails the fixture at once with its log tail."""

from __future__ import annotations

import random
import subprocess
import sys
import time
from pathlib import Path

import pytest

from olcf.core.config import ComputeSettings
from tests.e2e.stack import (
    MASTER_PORT_SPAN,
    MASTER_PORT_WINDOW,
    E2EStack,
    StackFailure,
    allocate_master_ports,
    log_tail,
    port_is_free,
)


def _settings(rpc: int, ui: int, span: int) -> ComputeSettings:
    """The production validator: it rejects overlapping ranges over the FULL configured spans, including the fixed worker/driver ranges."""
    return ComputeSettings(spark_master_port_base=rpc, spark_master_port_span=span, spark_master_ui_port_base=ui, spark_master_ui_port_span=span)


@pytest.mark.parametrize("seed", range(300))
def test_allocated_blocks_are_disjoint_and_valid_for_the_production_validator(seed: int) -> None:
    rpc, ui = allocate_master_ports(rng=random.Random(seed), is_free=lambda _p: True)
    assert abs(rpc - ui) >= MASTER_PORT_SPAN, (rpc, ui)
    lo, hi = MASTER_PORT_WINDOW
    for base in (rpc, ui):
        assert lo <= base and base + MASTER_PORT_SPAN - 1 <= hi
    ranges = _settings(rpc, ui, MASTER_PORT_SPAN).port_ranges()
    assert ranges["master_rpc"][1] < ranges["master_ui"][0] or ranges["master_ui"][1] < ranges["master_rpc"][0]


def test_adjacent_free_ports_are_still_split_into_disjoint_blocks() -> None:
    """The review's trigger: free ports fewer than 16 apart. Only one narrow stretch is free, so the two blocks must sit back to back."""
    free = set(range(30000, 30000 + 2 * MASTER_PORT_SPAN))
    rpc, ui = allocate_master_ports(rng=random.Random(1), is_free=free.__contains__, window=(29996, 30030), attempts=20000)
    assert {rpc, ui} == {30000, 30000 + MASTER_PORT_SPAN}
    _settings(rpc, ui, MASTER_PORT_SPAN)  # the validator accepts adjacent blocks


def test_a_busy_port_inside_a_block_rejects_the_block() -> None:
    busy = {31005}
    for seed in range(100):
        rpc, ui = allocate_master_ports(rng=random.Random(seed), is_free=lambda p: p not in busy)
        assert all(not (b <= 31005 < b + MASTER_PORT_SPAN) for b in (rpc, ui))


def test_allocation_failure_is_explicit() -> None:
    with pytest.raises(RuntimeError, match="no two free blocks"):
        allocate_master_ports(rng=random.Random(0), is_free=lambda _p: False, attempts=10)
    with pytest.raises(ValueError, match="cannot hold"):
        allocate_master_ports(span=10, window=(100, 110))


def test_real_allocation_binds_free_ports() -> None:
    rpc, ui = allocate_master_ports()
    assert all(port_is_free(b + i) for b in (rpc, ui) for i in range(MASTER_PORT_SPAN))


def test_the_old_independent_choice_can_overlap_but_the_allocator_cannot() -> None:
    """Two single free ports 5 apart with the default span of 16 are rejected by the production validator; the harness no longer builds that."""
    with pytest.raises(ValueError, match="overlap"):
        ComputeSettings(spark_master_port_base=30000, spark_master_ui_port_base=30005)


def test_log_tail_returns_the_last_lines_and_handles_missing_files(tmp_path: Path) -> None:
    log = tmp_path / "cp.log"
    log.write_text("\n".join(f"line {i}" for i in range(100)))
    tail = log_tail(log, lines=3)
    assert tail.splitlines() == ["line 97", "line 98", "line 99"]
    assert "unreadable" in log_tail(tmp_path / "missing.log")
    (tmp_path / "empty.log").write_text("")
    assert "empty" in log_tail(tmp_path / "empty.log")


def test_a_control_plane_that_exits_at_startup_fails_immediately_with_its_log_tail(tmp_path: Path) -> None:
    st = E2EStack(tmp_path, port=1)
    st.log = tmp_path / "control-plane.log"
    with open(st.log, "ab") as out:
        st.proc = subprocess.Popen([sys.executable, "-c", "import sys; print('ValueError: the master_rpc and master_ui port ranges overlap'); sys.exit(3)"], stdout=out, stderr=subprocess.STDOUT)  # noqa: S603
    st.http = type("NeverCalled", (), {"get": staticmethod(lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("must not poll a dead server")))})()  # type: ignore[assignment]
    started = time.monotonic()
    st.proc.wait(timeout=30)
    with pytest.raises(StackFailure, match=r"(?s)exited with status 3.*port ranges overlap"):
        st.wait_control_plane(timeout=90)
    assert time.monotonic() - started < 30, "must not wait for the 90 s timeout"
