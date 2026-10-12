# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Fixtures for the security suite: mock IdP, federated control plane, browser-flow simulation."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from tests.security.fedutil import entra_body  # noqa: E402
from tests.security.mock_idp import MockIdp  # noqa: E402


@pytest.fixture()
def idp():
    server = MockIdp().start()
    yield server
    server.stop()


@pytest.fixture()
def fed_settings(settings):
    settings.server.public_url = "http://olcf.test:8470"
    return settings


@pytest.fixture()
def fed(fed_settings):
    """Control plane with foundation + federation; ``fed.admin`` auth headers, ``fed.ctx`` the context."""
    from olcf.api.app import create_app

    app = create_app(fed_settings, module_names=["olcf.api.core.module", "olcf.security.federation"])
    with TestClient(app, base_url="http://olcf.test:8470") as c:
        pw = app.state.ctx.settings.path("secrets", "bootstrap-admin.txt").read_text().split("password=")[1].strip()
        r = c.post("/v1/identity/login", json={"username": "admin", "password": pw})
        assert r.status_code == 200, r.text
        c.admin = {"Authorization": f"Bearer {r.json()['access_token']}"}  # type: ignore[attr-defined]
        c.ctx = app.state.ctx  # type: ignore[attr-defined]
        c.fed = app.state.ctx.service("federation")  # type: ignore[attr-defined]
        c.admin_password = pw  # type: ignore[attr-defined]
        yield c


@pytest.fixture()
def entra(fed, idp):
    """An Entra provider named ``entra`` plus a workspace ``analytics`` for the mappings."""
    assert fed.post("/v1/workspaces", json={"name": "analytics"}, headers=fed.admin).status_code == 201
    r = fed.post("/v1/identity/providers", json=entra_body(idp), headers=fed.admin)
    assert r.status_code == 201, r.text
    return r.json()




@pytest.fixture(scope="module")
def two_tenants(tmp_path_factory):
    """The full control plane with tenants A and B (module scoped: building it takes a few seconds)."""
    from olcf.api.app import create_app
    from olcf.core.config import load_settings
    from tests.security.secenv import build_env

    s = load_settings(tmp_path_factory.mktemp("sec") / "data", environment="test")
    s.compute.embedded_worker = False
    app = create_app(s)
    with TestClient(app) as c:
        env = build_env(c, app.state.ctx)
        env.app = app  # type: ignore[attr-defined]
        env.spec = c.get("/v1/openapi.json").json()  # type: ignore[attr-defined]
        assert not env.failed_seeds, env.failed_seeds
        yield env


@pytest.fixture(scope="module")
def lenient_tenants(tmp_path_factory):
    """Like ``two_tenants`` but unhandled server exceptions become HTTP 500 responses (the fuzz tests count them instead of aborting)."""
    from olcf.api.app import create_app
    from olcf.core.config import load_settings
    from tests.security.secenv import build_env

    s = load_settings(tmp_path_factory.mktemp("fuzz") / "data", environment="test")
    s.compute.embedded_worker = False
    app = create_app(s)
    with TestClient(app, raise_server_exceptions=False) as c:
        env = build_env(c, app.state.ctx)
        env.app = app  # type: ignore[attr-defined]
        env.spec = c.get("/v1/openapi.json").json()  # type: ignore[attr-defined]
        assert not env.failed_seeds, env.failed_seeds
        yield env
