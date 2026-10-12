# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""Fixtures of the Intelligence suite.

* ``env`` - a bare ``Database`` + ``AssetRegistry`` + ``GraphStore`` + ``Engine`` (no HTTP, no app): fast graph-invariant tests.
* ``intel`` - a TestClient over core + storage + catalog + sql + intelligence (``intel.admin`` auth headers, ``intel.user(...)``, ``intel.workspace()``,
  ``intel.engine`` / ``intel.service``), the Intelligence counterpart of the ``dc`` data fixture.  No JVM / Spark is started anywhere.
* ``Builder`` - small helpers that make facts (lakehouse, table with columns, notebook, edges) the way a connector or analyzer would.
"""

from __future__ import annotations

import os
from typing import Any

import pytest

from olcf.intelligence import model as M

INTEL_MODULES = ["olcf.api.core.module", "olcf.storage.module", "olcf.catalog.module", "olcf.sql.module", "olcf.intelligence.module"]
PASSWORD = "correct horse battery staple"


class Builder:
    """Makes facts like a connector would. ``ws`` is the OLCF workspace id the assets belong to."""

    def __init__(self, ws: str | None = "wks_test") -> None:
        self.ws = ws

    def lakehouse(self, item: str, name: str, ws: str | None = None) -> M.AssetFact:
        """A lakehouse asset identified by its OLCF item id."""
        return M.AssetFact(M.AssetRef.make("lakehouse", {"olcf_item": item}), name, workspace_id=ws or self.ws, path=name)

    def table(self, lh: M.AssetFact, name: str, columns: list[tuple[str, str]] | None = None, **attrs: Any) -> M.AssetFact:
        """A table of ``lh`` (alias ``lakehouse_table``), with optional column list in its attributes."""
        item = dict(lh.ref.aliases)["olcf_item"]
        a: dict[str, Any] = dict(attrs)
        if columns:
            a["columns"] = [{"name": n, "type": t} for n, t in columns]
        return M.AssetFact(M.AssetRef.make("table", {"lakehouse_table": f"{item}:{name}"}, name), name, attrs=a, parent=lh.ref, workspace_id=lh.workspace_id,
                           path=f"{lh.name}/{name}")

    def column(self, table: M.AssetFact, name: str, type_: str = "string") -> M.AssetFact:
        """A column of a lakehouse table (alias ``lakehouse_column``), child of the table."""
        tv = dict(table.ref.aliases)["lakehouse_table"]
        return M.AssetFact(M.AssetRef.make("column", {"lakehouse_column": f"{tv}:{name}"}, name), name, attrs={"type": type_}, parent=table.ref, workspace_id=table.workspace_id)

    def notebook(self, item: str, name: str, ws: str | None = None) -> M.AssetFact:
        """A notebook asset (a process)."""
        return M.AssetFact(M.AssetRef.make("notebook", {"olcf_item": item}), name, workspace_id=ws or self.ws)

    def named(self, name: str, type_: str = "table", ws: str | None = None, **scope: str) -> M.AssetRef:
        """A reference by (qualified) name that resolves through the workspace / default-lakehouse scope, or becomes a placeholder."""
        sc = {"workspace_id": ws or self.ws or "", **scope}
        return M.AssetRef.make(type_, None, name, {k: v for k, v in sc.items() if v})

    def edge(self, src: Any, dst: Any, type_: str = "READS_FROM", confidence: str = "derived", *, kind: str = "static", source: str = "test", ref: str | None = None,
             location: dict[str, Any] | None = None, via: Any = None, transformation: str | None = None, observed_at: str | None = None) -> M.EdgeFact:
        """An edge with one piece of evidence; ``src`` / ``dst`` / ``via`` may be facts or references."""

        def r(x: Any) -> Any:
            return x.ref if isinstance(x, M.AssetFact) else x

        return M.EdgeFact(r(src), r(dst), type_, [M.Evidence(kind, source, confidence, ref=ref, location=location or {"cell": 1}, observed_at=observed_at)],
                          via=r(via) if via is not None else None, transformation=transformation)


class Env:
    """Registry + graph + engine over one database."""

    def __init__(self, db: Any) -> None:
        from olcf.intelligence.engine import Engine
        from olcf.intelligence.graph import GraphStore
        from olcf.intelligence.registry import AssetRegistry

        self.db = db
        self.events: list[tuple[str, str, dict[str, Any]]] = []
        self.registry = AssetRegistry(db, on_event=lambda a, i, d: self.events.append((a, i, d)))
        self.graph = GraphStore(db)
        self.engine = Engine(db, self.registry, self.graph)
        self.engine.pacer.enabled = False            # graph-invariant tests do not need the SQLite writer pauses (test_review_r01 turns them on where it tests them)
        self.b = Builder()

    def scan(self, facts: M.Facts | None = None, *, source: str = "local", mode: str = "full", finish: str | None = "succeeded") -> tuple[str, Any]:
        sid = self.engine.begin_scan(source, mode, "manual")
        stats = self.engine.apply_facts(facts, scan_id=sid, source_id=source) if facts is not None else None
        if finish:
            self.engine.finish_scan(sid, finish, stats.as_dict() if stats else {})
        return sid, stats

    def rows(self, sql: str, **params: Any) -> list[tuple[Any, ...]]:
        from sqlalchemy import text

        with self.db.connect() as c:
            return [tuple(r) for r in c.execute(text(sql), params)]

    def asset(self, name: str, type_: str | None = None) -> dict[str, Any]:
        from sqlalchemy import select

        from olcf.intelligence.schema import assets

        stmt = select(assets).where(assets.c.name == name)
        if type_:
            stmt = stmt.where(assets.c.type == type_)
        with self.db.connect() as c:
            rows = [dict(r._mapping) for r in c.execute(stmt.order_by(assets.c.id))]
        assert rows, f"no asset named {name}"
        return rows[0]

    def edges(self, **where: Any) -> list[dict[str, Any]]:
        from sqlalchemy import select

        from olcf.intelligence.schema import edges

        stmt = select(edges)
        for k, v in where.items():
            stmt = stmt.where(edges.c[k] == v)
        with self.db.connect() as c:
            return [dict(r._mapping) for r in c.execute(stmt.order_by(edges.c.id))]


def _pg_params() -> list[Any]:
    pg = os.environ.get("OLCF_TEST_POSTGRES_URL")
    return ["sqlite", pytest.param("postgres", marks=[pytest.mark.postgres, pytest.mark.skipif(not pg, reason="OLCF_TEST_POSTGRES_URL not set")])]


@pytest.fixture(params=_pg_params())
def env(request, tmp_path):
    """Registry + graph + engine over a fresh SQLite database (and, when OLCF_TEST_POSTGRES_URL is set, over a fresh PostgreSQL database)."""
    import uuid

    from olcf.db.engine import Database
    from olcf.intelligence import schema  # noqa: F401  (registers the migration stream)

    cleanup = None
    if request.param == "postgres":
        pytest.importorskip("psycopg")
        from sqlalchemy import create_engine, text
        from sqlalchemy.engine import make_url

        base = make_url(os.environ["OLCF_TEST_POSTGRES_URL"])
        admin = create_engine(base.set(drivername="postgresql+psycopg"), isolation_level="AUTOCOMMIT")
        name = f"olcf_i_{uuid.uuid4().hex[:10]}"
        with admin.connect() as c:
            c.execute(text(f'CREATE DATABASE "{name}"'))
        url = base.set(database=name).render_as_string(hide_password=False)

        def cleanup() -> None:
            with admin.connect() as c:
                c.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
            admin.dispose()
    else:
        url = f"sqlite:///{tmp_path / 'intel.db'}"
    db = Database(url)
    db.migrate()
    yield Env(db)
    db.dispose()
    if cleanup:
        cleanup()


class IntelClient:
    """TestClient wrapper with user / workspace helpers (``intel.admin`` are the administrator's headers)."""

    def __init__(self, client: Any) -> None:
        self.c = client
        self.ctx = client.ctx
        self.admin = client.admin
        self.service = self.ctx.service("intelligence")
        self.engine = self.service.engine
        self.b = Builder()
        self._n = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self.c, name)

    def user(self, name: str, role: str | None = "Viewer", *, workspace_id: str | None = None, scopes: list[str] | None = None) -> dict[str, str]:
        """Create a user with ``role`` (``None``: no binding at all) at org or workspace scope and return its auth headers (a scoped PAT with ``scopes``)."""
        ident = self.ctx.service("identity")
        row = ident.create_principal(name, kind="user", password=PASSWORD)
        if role:
            ident.bind(row["id"], role, scope_type="workspace" if workspace_id else "org", scope_id=workspace_id or "")
        if scopes:
            token, _ = self.ctx.tokens.create_pat(row["id"], f"{name}-pat", scopes=scopes, ttl_days=1)
            return {"Authorization": f"Bearer {token}"}
        r = self.c.post("/v1/identity/login", json={"username": name, "password": PASSWORD})
        assert r.status_code == 200, r.text
        return {"Authorization": f"Bearer {r.json()['access_token']}"}

    def workspace(self, name: str = "ws1") -> dict[str, Any]:
        r = self.c.post("/v1/workspaces", json={"name": name}, headers=self.admin)
        assert r.status_code == 201, r.text
        return r.json()

    def scan(self, facts: M.Facts, *, source: str = "local", mode: str = "full", finish: str | None = "succeeded") -> str:
        sid = self.engine.begin_scan(source, mode, "manual")
        stats = self.engine.apply_facts(facts, scan_id=sid, source_id=source)
        assert not stats.errors, stats.error_samples
        if finish:
            self.engine.finish_scan(sid, finish, stats.as_dict())
        return sid

    def lakehouse(self, ws: str, name: str = "lh1", **kw: Any) -> dict[str, Any]:
        r = self.c.post(f"/v1/workspaces/{ws}/lakehouses", json={"name": name, **kw}, headers=self.admin)
        assert r.status_code == 201, r.text
        return r.json()

    def registry_id(self, ref: M.AssetRef) -> str:
        return self.service.registry.resolve(ref, create=False)

    def asset_id(self, name: str, type_: str | None = None) -> str:
        r = self.c.get("/v1/intelligence/assets", params={"q": name, **({"type": type_} if type_ else {}), "limit": 200}, headers=self.admin)
        assert r.status_code == 200, r.text
        hits = [a for a in r.json()["items"] if a["name"] == name]
        assert hits, f"no asset {name}: {r.text[:300]}"
        return hits[0]["id"]


@pytest.fixture()
def intel_app(settings):
    from olcf.api.app import create_app

    return create_app(settings, module_names=INTEL_MODULES)


@pytest.fixture()
def intel(intel_app):
    from fastapi.testclient import TestClient

    with TestClient(intel_app) as c:
        pw = intel_app.state.ctx.settings.path("secrets", "bootstrap-admin.txt").read_text().split("password=")[1].strip()
        r = c.post("/v1/identity/login", json={"username": "admin", "password": pw})
        assert r.status_code == 200, r.text
        c.admin = {"Authorization": f"Bearer {r.json()['access_token']}"}  # type: ignore[attr-defined]
        c.ctx = intel_app.state.ctx  # type: ignore[attr-defined]
        yield IntelClient(c)


class World:
    """A populated two-workspace graph plus callers with different visibility (built once per module by the ``world`` fixture; tests must not mutate it)."""

    def __init__(self, client: IntelClient) -> None:
        self.c = client
        self.admin = client.admin
        self.ids: dict[str, str] = {}
        self.ws1: dict[str, Any] = {}
        self.ws2: dict[str, Any] = {}
        self.h: dict[str, dict[str, str]] = {}
        self.scans: dict[str, str] = {}

    def get(self, path: str, who: str = "admin", **params: Any) -> Any:
        return self.c.get(f"/v1/intelligence{path}", params=params, headers=self.h.get(who, self.admin) if who != "admin" else self.admin)


def build_world(client: IntelClient) -> World:
    """Populate: workspace alpha (raw -> etl -> clean -> agg -> mart -> sales_model -> sales_report, column lineage raw.amount -> clean.amount), workspace beta (a
    notebook that reads alpha's clean and a private table) and an organisation-level external source."""
    w = World(client)
    w.ws1, w.ws2 = client.workspace("alpha"), client.workspace("beta")
    a, b = w.ws1["id"], w.ws2["id"]
    bd1, bd2 = Builder(a), Builder(b)
    lh1, lh2 = bd1.lakehouse("itm_lh1", "lakehouse_a"), bd2.lakehouse("itm_lh2", "lakehouse_b")
    raw = bd1.table(lh1, "raw", [("id", "int"), ("amount", "double")])
    clean = bd1.table(lh1, "clean", [("id", "int"), ("amount", "double")])
    mart = bd1.table(lh1, "mart", [("total", "double")])
    lonely = bd1.table(lh1, "lonely", [("x", "int")])
    cols = {f"{t.name}.{c}": bd1.column(t, c) for t in (raw, clean) for c in ("id", "amount")}
    etl, agg = bd1.notebook("itm_nb1", "etl"), bd1.notebook("itm_nb2", "agg")
    model = M.AssetFact(M.AssetRef.make("semantic_model", {"olcf_item": "itm_sm1"}), "sales_model", workspace_id=a)
    report = M.AssetFact(M.AssetRef.make("report", {"olcf_item": "itm_rp1"}), "sales_report", workspace_id=a)
    ext = M.AssetFact(M.AssetRef.make("source_system", {"source": "crm:main"}), "crm_system", workspace_id=None)
    secret = bd2.table(lh2, "secret", [("pii", "string")])
    out2 = bd2.table(lh2, "secret_out")
    xnb = bd2.notebook("itm_nb9", "beta_nb")
    runtime = dict(kind="runtime", source="runtime:job", ref="job_1", location={})
    edges = [
        bd1.edge(etl, raw, "READS_FROM", "derived"), bd1.edge(etl, clean, "WRITES_TO", "verified", **runtime),
        bd1.edge(agg, clean, "READS_FROM", "derived"), bd1.edge(agg, mart, "WRITES_TO", "derived"),
        bd1.edge(model, mart, "DEPENDS_ON", "derived", kind="definition"), bd1.edge(report, model, "DEPENDS_ON", "inferred", kind="definition"),
        bd1.edge(etl, ext, "READS_FROM", "inferred", kind="naming"),
        bd1.edge(cols["raw.amount"], cols["clean.amount"], "TRANSFORMS", "derived", via=etl, transformation="amount * 1.0"),
        bd1.edge(cols["raw.id"], cols["clean.id"], "TRANSFORMS", "derived", via=etl, transformation="id"),
        bd2.edge(xnb, clean.ref, "READS_FROM", "derived"), bd2.edge(xnb, out2, "WRITES_TO", "derived"), bd2.edge(xnb, secret, "READS_FROM", "derived"),
    ]
    w.scans["s1"] = client.scan(M.Facts([lh1, lh2, raw, clean, mart, lonely, *cols.values(), etl, agg, model, report, ext, secret, out2, xnb], edges), source="local")
    # a second scan changes `raw`'s schema and adds a table: history and diff
    raw2 = bd1.table(lh1, "raw", [("id", "bigint"), ("amount", "double"), ("region", "string")])
    fresh = bd1.table(lh1, "fresh", [("y", "int")])
    w.scans["s2"] = client.scan(M.Facts([lh1, raw2, fresh]), source="local", mode="incremental")
    for n in ("raw", "clean", "mart", "lonely", "secret", "secret_out", "fresh", "etl", "agg", "sales_model", "sales_report", "crm_system", "beta_nb"):
        w.ids[n] = client.asset_id(n)
    w.ids["lakehouse_a"] = client.asset_id("lakehouse_a")
    gap = M.Gap("dynamic_reference", "pyspark", "etl", "table name built at run time", "use a literal table name", {"cell": 4})
    client.engine.record_gaps(w.scans["s1"], [gap], "local", asset_id=w.ids["etl"])
    w.ids["clean.amount"] = client.registry_id(cols["clean.amount"].ref)
    w.ids["raw.amount"] = client.registry_id(cols["raw.amount"].ref)
    w.h = {
        "alpha": client.user("v_alpha", "Viewer", workspace_id=a),
        "beta": client.user("v_beta", "Viewer", workspace_id=b),
        "org": client.user("v_org", "Viewer"),
        "none": client.user("v_none", None),
        "curator": client.user("c_alpha", "Contributor", workspace_id=a),
        "curator_org": client.user("c_org", "Contributor"),
        "agent": client.user("agent", "Contributor", scopes=["mcp"]),
        "wsadmin": client.user("wa_alpha", "WorkspaceAdmin", workspace_id=a),
    }
    return w


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    from fastapi.testclient import TestClient

    from olcf.api.app import create_app
    from olcf.core.config import load_settings

    s = load_settings(tmp_path_factory.mktemp("intel_world") / "data", environment="test")
    s.compute.embedded_worker = False
    app = create_app(s, module_names=INTEL_MODULES)
    with TestClient(app) as c:
        pw = app.state.ctx.settings.path("secrets", "bootstrap-admin.txt").read_text().split("password=")[1].strip()
        r = c.post("/v1/identity/login", json={"username": "admin", "password": pw})
        assert r.status_code == 200, r.text
        c.admin = {"Authorization": f"Bearer {r.json()['access_token']}"}  # type: ignore[attr-defined]
        c.ctx = app.state.ctx  # type: ignore[attr-defined]
        yield build_world(IntelClient(c))
