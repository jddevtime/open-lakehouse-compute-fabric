# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Fixtures for the Fabric adapter tests."""

from __future__ import annotations

import sys
import types
from collections.abc import Iterator
from typing import Any

import pytest

from tests.fabric.fabric_fakes import (
    API_AUDIENCE,
    BROKER_SECRET,
    FABRIC_WS,
    LAKEHOUSE,
    TENANT,
    FakeFabricWorld,
    FakeNotebookUtils,
)
from tests.fabric.fabric_stack import FAKE_RT, FabricStack
from tests.fabric.fabric_support import E2E, notebook_context


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "live: needs a live Microsoft Fabric / Entra / OneLake tenant (never run in CI)")


@pytest.fixture(scope="module")
def world() -> Iterator[FakeFabricWorld]:
    w = FakeFabricWorld().start()
    try:
        yield w
    finally:
        w.stop()


@pytest.fixture(scope="module")
def stack(tmp_path_factory: pytest.TempPathFactory) -> Iterator[FabricStack]:
    st = FabricStack(tmp_path_factory.mktemp("fabric-stack")).start()
    try:
        yield st
    finally:
        st.stop()


@pytest.fixture(scope="session")
def e2e(tmp_path_factory: pytest.TempPathFactory) -> Iterator[E2E]:
    world = FakeFabricWorld().start()
    st = FabricStack(tmp_path_factory.mktemp("fabric-e2e")).start()
    try:
        sd = tmp_path_factory.mktemp("fabric-agent") / "state"
        sd.mkdir()
        st.install_runtime(FAKE_RT, state_dir=sd)
        st.start_agent(sd)
        st.wait_agent_ready(FAKE_RT)
        ws = st.workspace("fabric-target")
        sp = st.service_principal(ws)
        secret = st.http.post("/v1/secrets", headers=st.admin, json={"name": "fabric-broker", "value": {"client_secret": BROKER_SECRET}}).json()
        conn = st.http.post("/v1/connections", headers=st.admin, json={"name": "fabric-broker", "type": "onelake", "shared": True, "secret_id": secret["id"],
                                                                      "config": {"workspace": FABRIC_WS, "lakehouse": LAKEHOUSE, "auth": "service_principal", "tenant_id": world.tenant,
                                                                                 "client_id": world.client_id, "endpoint": world.base}}).json()
        st.put_fabric(allowed_tenants=[TENANT], audiences=[API_AUDIENCE], workspace_map=[{"fabric_workspace_id": FABRIC_WS, "olcf_workspace": ws}], broker_connection=conn["id"],
                      network=world.network_config(), default_runtime={"family": "fabric-2.0-compatible", "runtime_id": FAKE_RT, "engine": "python"})
        yield E2E(st, world, ws, sp, sp["Authorization"].split()[1], broker_secret_id=secret["id"], extra={"conn": conn["id"]})
    finally:
        st.stop()
        world.stop()


@pytest.fixture()
def session(e2e: E2E, monkeypatch: pytest.MonkeyPatch) -> Iterator[types.SimpleNamespace]:
    """A simulated Fabric notebook session: fake ``notebookutils`` in ``sys.modules`` and the real SDK configured against the e2e control plane."""
    import olcf_fabric as olcf

    def make(*, notebook: str, git: str | None = None, context: dict[str, Any] | None = None, **cfg: Any) -> types.SimpleNamespace:
        ctx = notebook_context(**(context or {}))
        defs: dict[str, dict[str | None, str]] = {ctx["currentNotebookName"]: {None: notebook, "ipynb": notebook}}
        if git:
            defs[ctx["currentNotebookName"]]["fabricGitSource"] = git
        nbu = FakeNotebookUtils(context=ctx, definitions=defs)
        monkeypatch.setitem(sys.modules, "notebookutils", nbu)  # type: ignore[arg-type]
        olcf.configure(endpoint=e2e.url, token=e2e.token, tenant_id=TENANT, allow_insecure_http=True, **cfg)
        return types.SimpleNamespace(nbu=nbu, olcf=olcf, ctx=ctx)

    olcf.config.reset_settings()
    yield types.SimpleNamespace(make=make, olcf=olcf)
    olcf.config.reset_settings()
