# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""Fixtures of the ITEMS tests: a real app (foundation + storage + compute + notebooks modules) behind a TestClient.  No JVM / Spark is ever started; the compute
controller runs without workers, so submitted jobs stay queued (that is what the run endpoints are asserted on).  Helpers live in :mod:`tests.items.items_support`."""

from __future__ import annotations

import pytest

from tests.items.items_support import BASE_MODULES, WIDE_MODULES, _client, present


@pytest.fixture()
def ic(settings):
    from olcf.api.app import create_app

    app = create_app(settings, module_names=present(BASE_MODULES))
    yield from _client(app)


@pytest.fixture()
def wide(settings):
    """Every module that is present (pipelines, sql, catalog, sample ...): used by the full-workspace round trip and the sample compatibility test."""
    from olcf.api.app import create_app

    app = create_app(settings, module_names=present(WIDE_MODULES))
    yield from _client(app)


# ---------------------------------------------------------------------------------------------------------------- live stack with fake worker agents (CTRL fixtures)
@pytest.fixture(scope="module")
def live(tmp_path_factory):
    """A live control plane (uvicorn thread) with the foundation + storage + compute + notebooks modules; ``live.agent()`` enrols a fake worker agent."""
    import asyncio
    import threading

    import httpx
    import uvicorn

    from tests.compute import ctrl_stack as h

    class ItemsStack(h.Stack):
        def start(self):  # noqa: ANN202
            from olcf.api.app import create_app

            self.app = create_app(self.settings, module_names=present(BASE_MODULES))
            cfg = uvicorn.Config(self.app, host=self.settings.server.host, port=self.settings.server.port, log_level="warning", ws="websockets-sansio", lifespan="on", access_log=False)
            self.server = uvicorn.Server(cfg)
            self._thread = threading.Thread(target=lambda: asyncio.run(self.server.serve()), name="olcf-items-test-server", daemon=True)
            self._thread.start()
            h.wait_until(lambda: self.server.started, 60, 0.1, "the control plane to start")
            self.url = self.settings.base_url
            self.http = httpx.Client(base_url=self.url, timeout=60)
            pw = self.settings.path("secrets", "bootstrap-admin.txt").read_text().split("password=")[1].strip()
            self.admin = self.login("admin", pw)
            return self

    st = ItemsStack(tmp_path_factory.mktemp("items-live")).start()
    try:
        yield st
    finally:
        st.stop()


@pytest.fixture()
def fleet(live):
    live.retire_agents()
    try:
        yield live
    finally:
        live.retire_agents()
