# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Fixtures for the compute-fabric (CTRL) tests (the helpers live in :mod:`tests.compute.ctrl_stack`, which tests import by qualified name).

``stack`` is module-scoped (one server, one database per test module) and fast: the scheduler ticks every 200 ms and heartbeats are 1 s. The database is SQLite by
default; with ``OLCF_TEST_POSTGRES_URL`` set the same tests run against PostgreSQL (each stack gets its own schema, dropped afterwards). The fixtures never need Spark.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from tests.compute.ctrl_stack import BACKENDS, Stack, wait_until


@pytest.fixture(scope="module", params=BACKENDS)
def stack(request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory) -> Iterator[Stack]:
    st = Stack(tmp_path_factory.mktemp(f"stack-{request.param}"), request.param).start()
    try:
        yield st
    finally:
        st.stop()


@pytest.fixture()
def fleet(stack: Stack) -> Iterator[Stack]:
    """The module stack with an empty fleet at the start and at the end of the test."""
    stack.retire_agents()
    wait_until(lambda: not [w for w in stack.http.get("/v1/workers", headers=stack.admin).json()["items"] if w["state"] not in ("revoked", "offline", "pending")], 30, 0.2,
               "previous workers to leave the fleet")
    try:
        yield stack
    finally:
        stack.retire_agents()


@pytest.fixture()
def make_stack(tmp_path: Path) -> Iterator[Callable[..., Stack]]:
    """Factory for tests that need their own control plane (special settings, restarts)."""
    made: list[Stack] = []

    def _make(**tweaks: Any) -> Stack:
        st = Stack(tmp_path / f"s{len(made)}", tweaks.pop("backend", "sqlite"), **tweaks).start()
        made.append(st)
        return st

    yield _make
    for st in made:
        st.stop()
