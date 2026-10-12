# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The e2e stacks clean up their child processes without ``/proc`` (review finding F17: macOS has none).

Real child processes are started with a marker on their command line, exactly like the fixtures' stacks (their data directory is the marker).
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
import time
from pathlib import Path

import psutil
import pytest

from tests.e2e import procscan

#: starts a grandchild as well: a stack's agent has children (kernels, JVMs) that also carry the directory on their command line
PARENT = textwrap.dedent(
    """
    import subprocess, sys, time
    subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)", sys.argv[1]])
    time.sleep(120)
    """
)


def start_tree(marker: str) -> subprocess.Popen[bytes]:
    p = subprocess.Popen([sys.executable, "-c", PARENT, marker])  # noqa: S603
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if len(procscan.find(marker)) >= 2:
            return p
        time.sleep(0.05)
    p.kill()
    raise AssertionError("the child tree did not start")


def test_reap_kills_the_whole_tree_and_only_it(tmp_path: Path) -> None:
    mine, other = str(tmp_path / "stack-a"), str(tmp_path / "stack-b")
    a, b = start_tree(mine), start_tree(other)
    try:
        assert procscan.reap(mine, term_rounds=2, pause=1.0) == []
        a.wait(timeout=10)  # reaped by us (the test is its parent): no zombie left either
        assert procscan.find(mine) == []
        assert len(procscan.find(other)) >= 2, "a neighbouring stack must not be touched"
        assert b.poll() is None
    finally:
        procscan.reap(other, term_rounds=1, pause=0.5)
        b.wait(timeout=10)


def test_nothing_to_reap_is_fine_and_the_caller_is_never_a_candidate(tmp_path: Path) -> None:
    assert procscan.reap(str(tmp_path / "nobody"), term_rounds=1, pause=0.1) == []
    me = psutil.Process()
    marker = me.cmdline()[-1] if me.cmdline() else ""
    if marker:
        assert me.pid not in [p.pid for p in procscan.find(marker)]


def test_cleanup_does_not_walk_proc(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """On macOS ``Path('/proc').iterdir()`` raises FileNotFoundError: no code path of the helper may do that."""
    real = Path.iterdir

    def guarded(self: Path):  # type: ignore[no-untyped-def]
        if str(self) == "/proc":
            raise FileNotFoundError("/proc does not exist on this platform")
        return real(self)

    monkeypatch.setattr(Path, "iterdir", guarded)
    tree = start_tree(str(tmp_path / "stack-c"))
    try:
        assert procscan.reap(str(tmp_path / "stack-c"), term_rounds=2, pause=1.0) == []
    finally:
        tree.wait(timeout=10)


def test_assert_gone_names_the_survivors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    marker = str(tmp_path / "stack-d")
    tree = start_tree(marker)
    try:
        monkeypatch.setattr(procscan, "reap", lambda *a, **k: procscan.find(marker))  # a process that cannot be killed
        with pytest.raises(AssertionError, match="survived teardown"):
            procscan.assert_gone(marker)
        monkeypatch.undo()
    finally:
        procscan.reap(marker, term_rounds=1, pause=0.5)
        tree.wait(timeout=10)
