# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""Support code of the ITEMS tests (import by qualified name: ``from tests.items.items_support import nb_json``); the fixtures built on it are in ``conftest.py``. About the stack: a real app (foundation + storage + compute + notebooks modules) behind a TestClient.  No JVM / Spark is ever started; the compute
controller runs without workers, so submitted jobs stay queued (that is what the run endpoints are asserted on)."""

from __future__ import annotations

import importlib
from typing import Any

BASE_MODULES = ["olcf.api.core.module", "olcf.storage.module", "olcf.compute.module", "olcf.notebooks.module"]
WIDE_MODULES = ["olcf.api.core.module", "olcf.storage.module", "olcf.catalog.module", "olcf.sql.module", "olcf.compute.module", "olcf.notebooks.module",
                "olcf.pipelines.module", "olcf.sample.module"]


def present(mods: list[str]) -> list[str]:
    out = []
    for m in mods:
        try:
            importlib.import_module(m)
            out.append(m)
        except ModuleNotFoundError as exc:
            if exc.name and (m == exc.name or m.startswith(exc.name + ".")):
                continue
            raise
    return out


class ItemsClient:
    """A TestClient with the admin credential and helpers to create workspaces, items and users."""

    def __init__(self, client: Any, ctx: Any) -> None:
        self.c, self.ctx = client, ctx
        self.admin: dict[str, str] = client.admin

    def __getattr__(self, name: str) -> Any:
        return getattr(self.c, name)

    def user(self, name: str, role: str = "Viewer", *, workspace_id: str | None = None) -> dict[str, str]:
        ident = self.ctx.service("identity")
        row = ident.create_principal(name, kind="user", password="correct horse battery staple")
        ident.bind(row["id"], role, scope_type="workspace" if workspace_id else "org", scope_id=workspace_id or "")
        r = self.c.post("/v1/identity/login", json={"username": name, "password": "correct horse battery staple"})
        assert r.status_code == 200, r.text
        return {"Authorization": f"Bearer {r.json()['access_token']}"}

    def workspace(self, name: str = "ws1", **body: Any) -> dict[str, Any]:
        r = self.c.post("/v1/workspaces", json={"name": name, **body}, headers=self.admin)
        assert r.status_code == 201, r.text
        return r.json()

    def item(self, ws: str, type_: str, name: str, definition: dict[str, Any] | None = None, **body: Any) -> dict[str, Any]:
        r = self.c.post(f"/v1/workspaces/{ws}/items", json={"type": type_, "name": name, "definition": definition or {}, **body}, headers=self.admin)
        assert r.status_code == 201, r.text
        return r.json()

    def get_item(self, item_id: str) -> dict[str, Any]:
        r = self.c.get(f"/v1/items/{item_id}", headers=self.admin)
        assert r.status_code == 200, r.text
        return r.json()


def _client(app: Any):  # noqa: ANN202
    from fastapi.testclient import TestClient

    with TestClient(app) as c:
        pw = app.state.ctx.settings.path("secrets", "bootstrap-admin.txt").read_text().split("password=")[1].strip()
        r = c.post("/v1/identity/login", json={"username": "admin", "password": pw})
        assert r.status_code == 200, r.text
        c.admin = {"Authorization": f"Bearer {r.json()['access_token']}"}  # type: ignore[attr-defined]
        yield ItemsClient(c, app.state.ctx)


def nb_json(cells: list[tuple[str, str]], **meta: Any) -> dict[str, Any]:
    """A minimal valid nbformat 4.5 notebook (``[(cell_type, source), ...]``)."""
    out = []
    for i, (ct, src) in enumerate(cells):
        cell: dict[str, Any] = {"id": f"c{i:02d}", "cell_type": ct, "metadata": {}, "source": src}
        if ct == "code":
            cell.update(execution_count=None, outputs=[])
        out.append(cell)
    return {"nbformat": 4, "nbformat_minor": 5, "metadata": {"kernelspec": {"name": "python3", "display_name": "Python 3", "language": "python"}, **meta}, "cells": out}
