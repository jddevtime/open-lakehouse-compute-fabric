# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Fixtures of the orchestration (ORCH) tests (the helpers live in :mod:`tests.orch.orch_support`, which tests import by qualified name)."""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from tests.orch.orch_support import BACKENDS, Inproc, JobPlan, OrchStack


@pytest.fixture()
def httpd() -> Iterator[Any]:
    from tests.orch import httpd as _h

    s = _h.Server()
    try:
        yield s
    finally:
        s.stop()


@pytest.fixture()
def ip(tmp_path: Path) -> Iterator[Inproc]:
    """In-process control plane with the engine running (SQLite, or PostgreSQL via ``OLCF_TEST_POSTGRES_URL`` is covered by the ``stack`` tests)."""
    x = Inproc(tmp_path).start()
    try:
        yield x
    finally:
        x.stop()


@pytest.fixture()
def ip_idle(tmp_path: Path) -> Iterator[Inproc]:
    """In-process control plane with *nothing* running (tests drive ticks themselves)."""
    x = Inproc(tmp_path)
    try:
        yield x
    finally:
        x.stop()


@pytest.fixture(scope="module", params=BACKENDS)
def stack(request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory) -> Iterator[OrchStack]:
    st = OrchStack(tmp_path_factory.mktemp(f"orch-{request.param}"), request.param).start()
    try:
        yield st
    finally:
        st.stop()


@pytest.fixture()
def fleet(stack: OrchStack) -> Iterator[OrchStack]:
    """The module stack with exactly one fake worker online (jobs succeed after ~0.3 s unless ``stack.agents[0].job_plan`` says otherwise)."""
    stack.retire_agents()
    agent = stack.agent("orch-agent-" + uuid.uuid4().hex[:6])
    stack.wait_worker(agent)
    agent.job_plan = lambda spec: JobPlan()
    try:
        yield stack
    finally:
        stack.retire_agents()


@pytest.fixture()
def ws(stack: OrchStack) -> str:
    return stack.workspace()


@pytest.fixture()
def make_stack(tmp_path: Path) -> Iterator[Callable[..., OrchStack]]:
    made: list[OrchStack] = []

    def _make(**tweaks: Any) -> OrchStack:
        st = OrchStack(tmp_path / f"s{len(made)}", tweaks.pop("backend", "sqlite"), **tweaks).start()
        made.append(st)
        return st

    yield _make
    for st in made:
        with contextlib.suppress(Exception):
            st.stop()
