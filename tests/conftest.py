# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Shared pytest fixtures for the OLCF test-suite."""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
os.environ.setdefault("OLCF_WORKER_FAULTHANDLER_SECONDS", "45")  # an agent that hangs at start-up writes its thread stacks to its log (shown by the start-up timeout)
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "sdk" / "fabric"))


@pytest.fixture(scope="module")
def spark_lock() -> Iterator[None]:
    """Serialise anything that starts a JVM: the sandbox is small and shared (see docs/dev/CONTRACTS.md section 0).

    Module scope, not session scope: the lock is released when the module is done, so a module whose own fixture takes the same lock (the node cluster tests) does not
    wait for it until the end of the run when everything is collected in one pytest process.
    """
    from tests.spark_locking import spark_lock_held

    with spark_lock_held():
        yield


@pytest.fixture()
def settings(tmp_path):
    from olcf.core.config import load_settings

    s = load_settings(tmp_path / "data", environment="test")
    s.compute.embedded_worker = False
    return s


@pytest.fixture()
def ctx(settings):
    """A fully built AppContext with foundation services registered (no feature modules)."""
    from olcf.api.app import install_modules
    from olcf.core.context import build_context

    c = build_context(settings)
    c.modules = install_modules(c, ["olcf.api.core.module"])
    yield c
    c.db.dispose()


@pytest.fixture()
def make_app(settings):
    """Factory: ``make_app(modules=[...])`` returns (app, TestClient-ready) for the given module list."""
    from olcf.api.app import create_app

    def _make(module_names=None):
        return create_app(settings, module_names=module_names or ["olcf.api.core.module"])

    return _make


@pytest.fixture()
def client(make_app):
    """TestClient (lifespan started) with the foundation module only; ``client.admin`` holds auth headers."""
    from fastapi.testclient import TestClient

    app = make_app()
    with TestClient(app) as c:
        pw = (app.state.ctx.settings.path("secrets", "bootstrap-admin.txt")).read_text().split("password=")[1].strip()
        r = c.post("/v1/identity/login", json={"username": "admin", "password": pw})
        assert r.status_code == 200, r.text
        c.admin = {"Authorization": f"Bearer {r.json()['access_token']}"}  # type: ignore[attr-defined]
        c.ctx = app.state.ctx  # type: ignore[attr-defined]
        yield c


def _reorder_modules(items: list, order: str) -> list:
    """Reorder whole test modules (tests inside a module keep their order, so module-scoped fixtures still work).

    ``OLCF_TEST_ORDER=reverse`` or ``shuffle:<seed>`` exposes helper imports that only work in one collection order (review finding F15).
    """
    import random

    groups: dict[str, list] = {}
    for it in items:
        groups.setdefault(it.nodeid.split("::", 1)[0], []).append(it)
    names = list(groups)
    if order == "reverse":
        names.reverse()
    elif order.startswith("shuffle:"):
        random.Random(order.split(":", 1)[1]).shuffle(names)
    else:
        raise pytest.UsageError(f"OLCF_TEST_ORDER must be 'reverse' or 'shuffle:<seed>', got {order!r}")
    return [it for n in names for it in groups[n]]


def pytest_collection_modifyitems(config, items):
    skip_live = pytest.mark.skip(reason="live external systems are never exercised by default (use -m live)")
    if "live" not in (config.getoption("-m") or ""):
        for item in items:
            if "live" in item.keywords:
                item.add_marker(skip_live)
    order = os.environ.get("OLCF_TEST_ORDER", "").strip()
    if order:
        items[:] = _reorder_modules(items, order)


def postgres_url() -> str | None:
    return os.environ.get("OLCF_TEST_POSTGRES_URL")
