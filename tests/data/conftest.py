# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""Fixtures for the DATA work package: an app with core + storage + catalog + sql + mcp + sample modules.

No JVM/Spark is started anywhere in these tests.  ``dc`` is a TestClient with ``dc.admin`` auth headers and helpers to
create users (with arbitrary roles / token scopes), workspaces and lakehouses.
"""

from __future__ import annotations

import importlib
from typing import Any

import pytest

ALL_DATA_MODULES = [
    "olcf.api.core.module", "olcf.storage.module", "olcf.catalog.module", "olcf.sql.module", "olcf.mcp.module", "olcf.sample.module",
]


def available_modules() -> list[str]:
    out = []
    for m in ALL_DATA_MODULES:
        try:
            importlib.import_module(m)
            out.append(m)
        except ModuleNotFoundError as exc:
            if exc.name and (m == exc.name or m.startswith(exc.name + ".")):
                continue
            raise
    return out


class DataClient:
    """Thin helper around a TestClient used by the data tests."""

    def __init__(self, client: Any) -> None:
        self.c = client
        self.ctx = client.ctx
        self.admin = client.admin

    def __getattr__(self, name: str) -> Any:
        return getattr(self.c, name)

    def user(self, name: str, role: str = "Viewer", *, workspace_id: str | None = None, scopes: list[str] | None = None) -> dict[str, str]:
        """Create a user with ``role`` (org scope, or workspace scope when ``workspace_id``) and return auth headers.

        With ``scopes`` the credential is a platform token (PAT) limited to those scopes, as MCP agents use."""
        ident = self.ctx.service("identity")
        row = ident.create_principal(name, kind="user", password="correct horse battery staple")
        ident.bind(row["id"], role, scope_type="workspace" if workspace_id else "org", scope_id=workspace_id or "")
        if scopes:
            token, _ = self.ctx.tokens.create_pat(row["id"], f"{name}-pat", scopes=scopes, ttl_days=1)
            return {"Authorization": f"Bearer {token}"}
        r = self.c.post("/v1/identity/login", json={"username": name, "password": "correct horse battery staple"})
        assert r.status_code == 200, r.text
        return {"Authorization": f"Bearer {r.json()['access_token']}"}

    def workspace(self, name: str = "ws1") -> dict[str, Any]:
        r = self.c.post("/v1/workspaces", json={"name": name}, headers=self.admin)
        assert r.status_code == 201, r.text
        return r.json()

    def lakehouse(self, ws: str, name: str = "lh1", **kw: Any) -> dict[str, Any]:
        r = self.c.post(f"/v1/workspaces/{ws}/lakehouses", json={"name": name, **kw}, headers=self.admin)
        assert r.status_code == 201, r.text
        return r.json()

    def write_table(self, lakehouse_id: str, name: str, data: Any, **kw: Any) -> dict[str, Any]:
        """Write an Arrow table as a Delta table straight through the storage service (no HTTP)."""
        svc = self.ctx.service("storage")
        return svc.tables.write_table(svc.get_lakehouse(lakehouse_id), name, data, **kw)


@pytest.fixture()
def data_app(settings):
    from olcf.api.app import create_app

    return create_app(settings, module_names=available_modules())


@pytest.fixture()
def dc(data_app):
    from fastapi.testclient import TestClient

    with TestClient(data_app) as c:
        pw = data_app.state.ctx.settings.path("secrets", "bootstrap-admin.txt").read_text().split("password=")[1].strip()
        r = c.post("/v1/identity/login", json={"username": "admin", "password": pw})
        assert r.status_code == 200, r.text
        c.admin = {"Authorization": f"Bearer {r.json()['access_token']}"}  # type: ignore[attr-defined]
        c.ctx = data_app.state.ctx  # type: ignore[attr-defined]
        yield DataClient(c)
