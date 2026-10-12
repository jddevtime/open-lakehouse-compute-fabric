# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""Fixtures of the discovery suite: the Intelligence app (``intel``) plus helpers to build local lakehouses with real Delta tables, run scans synchronously and read the graph.

No JVM / Spark is started anywhere. The scheduler loop does not run in the ``test`` environment, so nothing changes the graph unless a test asks for it.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest
from sqlalchemy import select

from olcf.intelligence.schema import assets, edges, gaps, scans
from olcf.security.rbac import SYSTEM_PRINCIPAL


def people_table(n: int = 6) -> pa.Table:
    """A table with obviously sensitive columns (e-mail, ssn), a numeric and a text column."""
    return pa.table({"id": pa.array(range(n), pa.int64()), "email": pa.array([f"user{i}@example.com" for i in range(n)]),
                     "ssn": pa.array([f"{100 + i:03d}-45-{6789 + i:04d}" for i in range(n)]), "amount": pa.array([float(i) * 1.5 for i in range(n)]),
                     "city": pa.array(["Oslo", "Rome", None, "Lima", "Kyiv", "Pune"][:n])})


class Disc:
    """Helpers around the Intelligence client for discovery tests."""

    def __init__(self, intel: Any) -> None:
        self.intel = intel
        self.ctx = intel.ctx
        self.admin = intel.admin
        self.svc = intel.service
        self.d = intel.service.discovery
        self.engine = intel.engine
        self.ws: dict[str, Any] | None = None
        self.lh: dict[str, Any] | None = None

    # ---- platform data
    def lakehouse(self, ws_name: str = "alpha", lh_name: str = "lh1") -> dict[str, Any]:
        self.ws = self.intel.workspace(ws_name)
        self.lh = self.intel.lakehouse(self.ws["id"], lh_name)
        return self.lh

    def write(self, name: str, table: pa.Table, lakehouse_id: str | None = None, **kw: Any) -> None:
        svc = self.ctx.service("storage")
        svc.tables.write_table(svc.get_lakehouse(lakehouse_id or self.lh["id"]), name, table, **kw)  # type: ignore[index]

    def notebook(self, name: str, code: str, ws_id: str | None = None) -> dict[str, Any]:
        body = {"type": "notebook", "name": name, "definition": {"cells": [{"cell_type": "code", "source": code}], "language": "python"}}
        r = self.intel.post(f"/v1/workspaces/{ws_id or self.ws['id']}/items", json=body, headers=self.admin)  # type: ignore[index]
        assert r.status_code == 201, r.text
        return r.json()

    # ---- scans
    def source(self, ref: str = "local") -> dict[str, Any]:
        return self.d.sources.find(ref)

    def scan(self, ref: str = "local", mode: str = "full", **kw: Any) -> dict[str, Any]:
        """Run a scan synchronously and return the final ``scans`` row."""
        return self.d.runner.start(SYSTEM_PRINCIPAL, ref, mode=mode, trigger=kw.pop("trigger", "manual"), wait=True, **kw)

    # ---- graph reads
    def assets(self, **where: Any) -> list[dict[str, Any]]:
        stmt = select(assets)
        for k, v in where.items():
            stmt = stmt.where(assets.c[k] == v)
        with self.ctx.db.connect() as c:
            return [dict(r._mapping) for r in c.execute(stmt.order_by(assets.c.id))]

    def asset(self, name: str, type_: str | None = None, status: str = "active") -> dict[str, Any]:
        where: dict[str, Any] = {"name": name, "status": status}
        if type_:
            where["type"] = type_
        rows = self.assets(**where)
        assert rows, f"no {status} asset {type_ or ''} {name}: {[(a['type'], a['name'], a['status']) for a in self.assets()]}"
        return rows[0]

    def asset_parent(self, asset_id: str) -> str | None:
        return next(a["parent_id"] for a in self.assets(id=asset_id))

    def edges(self, **where: Any) -> list[dict[str, Any]]:
        stmt = select(edges)
        for k, v in where.items():
            stmt = stmt.where(edges.c[k] == v)
        with self.ctx.db.connect() as c:
            return [dict(r._mapping) for r in c.execute(stmt.order_by(edges.c.id))]

    def gaps(self, source_id: str | None = None, open_only: bool = True) -> list[dict[str, Any]]:
        stmt = select(gaps)
        if source_id:
            stmt = stmt.where(gaps.c.source_id == source_id)
        if open_only:
            stmt = stmt.where(gaps.c.resolved_at.is_(None))
        with self.ctx.db.connect() as c:
            return [dict(r._mapping) for r in c.execute(stmt.order_by(gaps.c.id))]

    def scans(self, source_id: str) -> list[dict[str, Any]]:
        with self.ctx.db.connect() as c:
            return [dict(r._mapping) for r in c.execute(select(scans).where(scans.c.source_id == source_id).order_by(scans.c.version))]

    def api(self, method: str, path: str, who: dict[str, str] | None = None, **kw: Any) -> Any:
        return self.intel.c.request(method, f"/v1/intelligence{path}", headers=who or self.admin, **kw)


@pytest.fixture()
def disc(intel):
    return Disc(intel)
