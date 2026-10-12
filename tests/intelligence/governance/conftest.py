# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""Fixtures of the governance suite.

``gw`` is a module-scoped world: the Intelligence app with governance installed, two workspaces (alpha, beta), a lineage graph built the way a connector would build it, users with
different permissions, and thin request helpers. Tests in a module share it, so every test creates the governance objects it needs under names of its own (the world itself is
never mutated, except for objects created in the fixture). ``fresh`` builds the same world per test for tests that count things or change settings.

The lineage (workspace alpha): ``customers`` -etl-> ``customers_clean`` -agg-> ``mart_sales`` <- ``sales_model`` <- ``sales_report``; ``orders`` -agg-> ``mart_sales``; column lineage
``customers.email`` -> ``customers_clean.email`` -> visual ``email_card`` (page of ``sales_report``). Workspace beta: ``beta_src.secret_col`` -> alpha's ``alpha_ingest.secret_col`` through
a notebook of beta (so a caller who sees only alpha sees a source it cannot see).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from olcf.intelligence import model as M
from tests.intelligence.conftest import INTEL_MODULES, Builder, IntelClient

OLD = "2020-01-01T00:00:00.000000Z"


def _stamp(delta_hours: float = 0.0) -> str:
    return (datetime.now(UTC) - timedelta(hours=delta_hours)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class Gov:
    """The world plus request helpers (``gw.get("/glossary/terms", "viewer_alpha")`` etc.)."""

    def __init__(self, client: IntelClient) -> None:
        self.c = client
        self.admin = client.admin
        self.ctx = client.ctx
        self.svc = client.service
        self.gov = client.service.governance
        self.ids: dict[str, str] = {}
        self.h: dict[str, dict[str, str]] = {}
        self.ws: dict[str, dict[str, Any]] = {}
        self.scans: dict[str, str] = {}
        self.n = 0
        self.facts: dict[str, M.AssetFact] = {}

    # ------------------------------------------------------------------------------------------------------------------------ requests
    def headers(self, who: str) -> dict[str, str]:
        return self.admin if who == "admin" else self.h[who]

    def req(self, method: str, path: str, who: str = "admin", **kw: Any) -> Any:
        headers = {**self.headers(who), **kw.pop("headers", {})}
        return self.c.request(method, f"/v1{path}", headers=headers, **kw)

    def get(self, path: str, who: str = "admin", **params: Any) -> Any:
        return self.req("GET", f"/governance{path}", who, params=params)

    def post(self, path: str, body: Any = None, who: str = "admin", **kw: Any) -> Any:
        return self.req("POST", f"/governance{path}", who, json=body if body is not None else {}, **kw)

    def put(self, path: str, body: Any, who: str = "admin", **kw: Any) -> Any:
        return self.req("PUT", f"/governance{path}", who, json=body, **kw)

    def patch(self, path: str, body: Any, who: str = "admin", **kw: Any) -> Any:
        return self.req("PATCH", f"/governance{path}", who, json=body, **kw)

    def delete(self, path: str, who: str = "admin", **kw: Any) -> Any:
        return self.req("DELETE", f"/governance{path}", who, **kw)

    def intel(self, path: str, who: str = "admin", **params: Any) -> Any:
        return self.req("GET", f"/intelligence{path}", who, params=params)

    def ok(self, r: Any, status: int = 200) -> Any:
        assert r.status_code == status, f"expected {status}, got {r.status_code}: {r.text[:600]}"
        return r.json()

    def code(self, r: Any) -> str:
        return r.json().get("error", {}).get("code", "")

    def uniq(self, prefix: str = "x") -> str:
        self.n += 1
        return f"{prefix}_{self.n}_{id(self) % 9973}"

    def aid(self, name: str) -> str:
        return self.ids[name]

    # ------------------------------------------------------------------------------------------------------------------------ shortcuts
    def term(self, name: str | None = None, who: str = "admin", **body: Any) -> dict[str, Any]:
        return self.ok(self.post("/glossary/terms", {"name": name or self.uniq("term"), **body}, who), 201)

    def domain(self, name: str | None = None, **body: Any) -> dict[str, Any]:
        return self.ok(self.post("/domains", {"name": name or self.uniq("dom"), **body}), 201)

    def product(self, name: str | None = None, **body: Any) -> dict[str, Any]:
        return self.ok(self.post("/products", {"name": name or self.uniq("prod"), **body}), 201)

    def tick(self) -> dict[str, Any]:
        """Run the governance background pass once (the loop does not run in tests)."""
        return self.gov.tick()

    def inbox(self) -> None:
        """Drain the Intelligence inbox (events -> graph facts)."""
        self.svc.inbox_processor.tick()

    def rescan(self, facts: M.Facts, mode: str = "incremental") -> str:
        return self.c.scan(facts, source="local", mode=mode)

    def classify(self, asset: str, label: str, who: str = "admin") -> Any:
        return self.post(f"/assets/{self.aid(asset)}/classifications", {"classification": label}, who)

    def alter_table(self, name: str, columns: list[tuple[str, str]]) -> None:
        """Rescan one table of alpha with a different column list (an incremental scan: nothing else changes) and drain the inbox."""
        f = self.facts[name]
        changed = M.AssetFact(f.ref, f.name, attrs={**f.attrs, "columns": [{"name": n, "type": t} for n, t in columns]}, parent=f.parent, workspace_id=f.workspace_id, path=f.path)
        self.rescan(M.Facts([changed], []), mode="incremental")
        self.inbox()

    def events(self, type_: str, **where: Any) -> list[dict[str, Any]]:
        """Persisted platform events of ``type_`` (oldest first)."""
        from sqlalchemy import select

        from olcf.db import schema as S

        with self.ctx.db.connect() as c:
            rows = [dict(r._mapping) for r in c.execute(select(S.events).where(S.events.c.type == type_).order_by(S.events.c.seq))]
        return [r for r in rows if all(r["payload"].get(k) == v for k, v in where.items())]

    def audit(self, action: str) -> list[dict[str, Any]]:
        return self.ok(self.req("GET", "/audit", params={"action": action, "limit": 200}))["items"]

    def class_id(self, name: str) -> str:
        for c in self.ok(self.get("/classifications"))["items"]:
            if c["name"] == name:
                return c["id"]
        raise AssertionError(name)


def build_gov(client: IntelClient) -> Gov:
    w = Gov(client)
    w.ws["alpha"], w.ws["beta"] = client.workspace("alpha"), client.workspace("beta")
    a, b = w.ws["alpha"]["id"], w.ws["beta"]["id"]
    bd1, bd2 = Builder(a), Builder(b)
    lh = bd1.lakehouse("itm_gl1", "lakehouse_a")
    lh_b = bd2.lakehouse("itm_gl2", "lakehouse_b")
    customers = bd1.table(lh, "customers", [("customer_id", "int"), ("email", "string"), ("full_name", "string"), ("region", "string")], last_modified=_stamp(1))
    customers.description = "Master data of customers across all regions"
    orders = bd1.table(lh, "orders", [("order_id", "int"), ("customer_id", "int"), ("amount", "double")], last_modified=OLD)
    clean = bd1.table(lh, "customers_clean", [("customer_id", "int"), ("email", "string"), ("region", "string")], last_modified=_stamp(2))
    clean.description = "Customers with normalised e-mail addresses"
    mart = bd1.table(lh, "mart_sales", [("total", "double"), ("region", "string")], last_modified=_stamp(3))
    mart.description = "x"
    dump = bd1.table(lh, "raw_dump", [("blob", "string")])
    ingest = bd1.table(lh, "alpha_ingest", [("secret_col", "string")], last_modified=_stamp(1))
    ingest.description = "Rows received from beta"
    cols: dict[str, M.AssetFact] = {}
    for t in (customers, orders, clean, mart, dump, ingest):
        for c in dict(t.attrs.get("columns") and [(x["name"], x["type"]) for x in t.attrs["columns"]] or []):
            cols[f"{t.name}.{c}"] = bd1.column(t, c)
    # the e-mail column carries a detector suggestion, the way discovery leaves it
    em = cols["customers.email"]
    em.attrs["suggested_classifications"] = [{"classification": "email", "confidence": 0.95, "rule": "email-pattern", "evidences": ["name", "pattern"], "category": "pii"}]
    cols["customers.full_name"].attrs["suggested_classifications"] = [{"classification": "person_name", "confidence": 0.6, "rule": "name-heuristic", "evidences": ["name"], "category": "pii"}]
    etl, agg = bd1.notebook("itm_gn1", "etl"), bd1.notebook("itm_gn2", "agg")
    model = M.AssetFact(M.AssetRef.make("semantic_model", {"olcf_item": "itm_gsm1"}), "sales_model", workspace_id=a)
    report = M.AssetFact(M.AssetRef.make("report", {"olcf_item": "itm_grp1"}), "sales_report", workspace_id=a)
    page = M.AssetFact(M.AssetRef.make("page", {"pbi": "ws/rp1/page1"}, "page1"), "page1", parent=report.ref, workspace_id=a)
    visual = M.AssetFact(M.AssetRef.make("visual", {"pbi": "ws/rp1/page1/v1"}, "email_card"), "email_card", parent=page.ref, workspace_id=a)
    src_b = bd2.table(lh_b, "beta_src", [("secret_col", "string")])
    col_b = bd2.column(src_b, "secret_col")
    nb_b = bd2.notebook("itm_gn9", "beta_nb")
    runtime = dict(kind="runtime", source="runtime:job", ref="job_g1", location={})
    edges = [
        bd1.edge(etl, customers, "READS_FROM", "derived"), bd1.edge(etl, clean, "WRITES_TO", "verified", **runtime),
        bd1.edge(agg, orders, "READS_FROM", "derived"), bd1.edge(agg, clean, "READS_FROM", "derived"), bd1.edge(agg, mart, "WRITES_TO", "derived"),
        bd1.edge(model, mart, "DEPENDS_ON", "derived", kind="definition"), bd1.edge(report, model, "DEPENDS_ON", "inferred", kind="definition"),
        bd1.edge(cols["customers.email"], cols["customers_clean.email"], "TRANSFORMS", "derived", via=etl, transformation="lower(email)"),
        bd1.edge(cols["customers.customer_id"], cols["customers_clean.customer_id"], "TRANSFORMS", "derived", via=etl),
        bd1.edge(cols["customers.region"], cols["customers_clean.region"], "TRANSFORMS", "derived", via=etl),
        bd1.edge(cols["customers_clean.region"], cols["mart_sales.region"], "TRANSFORMS", "derived", via=agg),
        bd1.edge(visual, cols["customers_clean.email"], "DISPLAYS", "derived", kind="definition"),
        bd2.edge(nb_b, src_b, "READS_FROM", "derived"), bd2.edge(nb_b, ingest.ref, "WRITES_TO", "derived"),
        bd2.edge(col_b, cols["alpha_ingest.secret_col"], "TRANSFORMS", "derived", via=nb_b),
    ]
    facts = [lh, lh_b, customers, orders, clean, mart, dump, ingest, *cols.values(), etl, agg, model, report, page, visual, src_b, col_b, nb_b]
    w.facts = {f.name: f for f in facts if f.ref.type != "column"}
    w.scans["s1"] = client.scan(M.Facts(facts, edges), source="local")
    for n in ("customers", "orders", "customers_clean", "mart_sales", "raw_dump", "alpha_ingest", "etl", "agg", "sales_model", "sales_report", "page1", "email_card", "beta_src", "beta_nb",
              "lakehouse_a", "lakehouse_b"):
        w.ids[n] = client.asset_id(n)
    for key, ref in cols.items():
        w.ids[key] = client.registry_id(ref.ref)
    w.ids["beta_src.secret_col"] = client.registry_id(col_b.ref)
    w.h = {
        "viewer_alpha": client.user("g_va", "Viewer", workspace_id=a),
        "viewer_beta": client.user("g_vb", "Viewer", workspace_id=b),
        "viewer_org": client.user("g_vo", "Viewer"),
        "nobody": client.user("g_none", None),
        "cur_alpha": client.user("g_ca", "Contributor", workspace_id=a),
        "cur_beta": client.user("g_cb", "Contributor", workspace_id=b),
        "cur_org": client.user("g_co", "Contributor"),
        "cur_org2": client.user("g_co2", "Contributor"),
        "appr_alpha": client.user("g_aa", "WorkspaceAdmin", workspace_id=a),
        "lead_org": client.user("g_lead", "WorkspaceAdmin"),
        "lead_org2": client.user("g_lead2", "WorkspaceAdmin"),
        "secadmin": client.user("g_sec", "SecurityAdmin"),
        "agent": client.user("g_agent", "Contributor", scopes=["read"]),
    }
    return w


@pytest.fixture(scope="module")
def gw(tmp_path_factory):
    from fastapi.testclient import TestClient

    from olcf.api.app import create_app
    from olcf.core.config import load_settings

    s = load_settings(tmp_path_factory.mktemp("gov_world") / "data", environment="test")
    s.compute.embedded_worker = False
    app = create_app(s, module_names=INTEL_MODULES)
    with TestClient(app) as c:
        pw = app.state.ctx.settings.path("secrets", "bootstrap-admin.txt").read_text().split("password=")[1].strip()
        r = c.post("/v1/identity/login", json={"username": "admin", "password": pw})
        assert r.status_code == 200, r.text
        c.admin = {"Authorization": f"Bearer {r.json()['access_token']}"}  # type: ignore[attr-defined]
        c.ctx = app.state.ctx  # type: ignore[attr-defined]
        yield build_gov(IntelClient(c))


@pytest.fixture()
def fresh(tmp_path):
    """The same world, built for this test alone (for tests that count things or change settings)."""
    from fastapi.testclient import TestClient

    from olcf.api.app import create_app
    from olcf.core.config import load_settings

    s = load_settings(tmp_path / "data", environment="test")
    s.compute.embedded_worker = False
    app = create_app(s, module_names=INTEL_MODULES)
    with TestClient(app) as c:
        pw = app.state.ctx.settings.path("secrets", "bootstrap-admin.txt").read_text().split("password=")[1].strip()
        r = c.post("/v1/identity/login", json={"username": "admin", "password": pw})
        assert r.status_code == 200, r.text
        c.admin = {"Authorization": f"Bearer {r.json()['access_token']}"}  # type: ignore[attr-defined]
        c.ctx = app.state.ctx  # type: ignore[attr-defined]
        yield build_gov(IntelClient(c))


def _setting(self: Gov, key: str, value: Any) -> None:
    r = self.c.put(f"/v1/admin/settings/{key}", json={"value": value}, headers=self.admin)
    assert r.status_code == 200, r.text


Gov.setting = _setting  # type: ignore[attr-defined]
