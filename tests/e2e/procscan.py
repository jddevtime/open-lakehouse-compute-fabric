# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Portable cleanup of the processes a test stack leaves behind (review finding F17).

The e2e fixtures used to walk ``/proc`` to find processes whose command line contains the stack's directory; ``/proc`` does not exist on macOS, so the teardown of a
first-run stack died with ``FileNotFoundError`` (and left uncertainty about whether the children were gone).  This uses ``psutil`` (a dependency of the project, with a
backend for Linux, macOS and Windows) and is scoped by a marker: only processes whose command line contains it - i.e. those the fixture started - are touched.
"""

from __future__ import annotations

import contextlib
import time
from collections.abc import Iterable

import psutil


def find(marker: str, *, exclude: Iterable[int] = ()) -> list[psutil.Process]:
    """Live processes (not zombies, not the caller, not ``exclude``) whose command line contains ``marker``."""
    skip = {psutil.Process().pid, *exclude}
    out: list[psutil.Process] = []
    for p in psutil.process_iter(["pid", "cmdline", "status"]):
        try:
            if p.info["pid"] in skip or p.info["status"] == psutil.STATUS_ZOMBIE:
                continue
            if any(marker in part for part in (p.info["cmdline"] or ())):
                out.append(p)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue  # gone meanwhile, or another user's process (which the fixture did not start)
    return out


def _signal_all(procs: list[psutil.Process], kill: bool) -> None:
    for p in procs:
        with contextlib.suppress(psutil.NoSuchProcess, psutil.AccessDenied):
            p.kill() if kill else p.terminate()


def reap(marker: str, *, term_rounds: int = 3, pause: float = 1.0, kill_wait: float = 10.0) -> list[psutil.Process]:
    """Terminate everything carrying ``marker``; escalate to kill; return what is *still* alive afterwards (the caller asserts that this is empty)."""
    for _ in range(term_rounds):
        found = find(marker)
        if not found:
            return []
        _signal_all(found, kill=False)
        psutil.wait_procs(found, timeout=pause)
    found = find(marker)
    _signal_all(found, kill=True)
    deadline = time.monotonic() + kill_wait
    while time.monotonic() < deadline:
        found = find(marker)
        if not found:
            return []
        time.sleep(0.2)
    return found


def assert_gone(marker: str, **kw: float) -> None:
    left = reap(marker, **kw)  # type: ignore[arg-type]
    assert not left, "processes of the test stack survived teardown: " + ", ".join(f"{p.pid}" for p in left)
