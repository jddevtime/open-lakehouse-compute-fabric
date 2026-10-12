# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Governance observes only and is optional: nothing on the compute / execution path references it, it stays on its allowed layers, and a missing governance schema never breaks
Intelligence, the registry, scans or the platform."""

from __future__ import annotations

import ast
from pathlib import Path

from olcf.intelligence.governance import schema as GS

SRC = Path(__file__).resolve().parents[3] / "src" / "olcf"
GOV = SRC / "intelligence" / "governance"
#: where code runs jobs, SQL, pipelines, notebooks: none of it may know that governance exists
EXECUTION_PACKAGES = ("compute", "sql", "catalog", "storage", "pipelines", "notebooks", "fabric", "runtime_lib", "worker", "scheduler")


def _imports(path: Path) -> list[tuple[int, str]]:
    out = []
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            out += [(node.lineno, a.name) for a in node.names]
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            out.append((node.lineno, node.module))
    return out


def test_nothing_on_the_execution_path_mentions_governance():
    scanned = 0
    for pkg in EXECUTION_PACKAGES:
        for f in sorted((SRC / pkg).rglob("*.py")) if (SRC / pkg).is_dir() else []:
            scanned += 1
            text = f.read_text(encoding="utf-8")
            assert "intelligence.governance" not in text and "maybe_service(\"governance\")" not in text, f
            assert not [m for _, m in _imports(f) if m.startswith("olcf.intelligence")], f
    assert scanned > 100


def test_governance_stays_on_its_allowed_layers():
    allowed = ("olcf.core", "olcf.db", "olcf.security", "olcf.intelligence", "olcf.api.deps")
    bad = []
    for f in sorted(GOV.glob("*.py")):
        bad += [f"{f.name}:{line} {m}" for line, m in _imports(f) if m.startswith("olcf.") and not m.startswith(allowed)]
    assert not bad, bad


def test_governance_never_calls_the_compute_side():
    """Reading is all governance does: no submit / run / execute call exists in its sources."""
    banned = ("submit_job", "run_sql", "execute_sql", "ctx.service(\"compute\")", "ctx.service(\"sql\")", "service(\"pipelines\")", "subprocess", "os.system")
    for f in sorted(GOV.glob("*.py")):
        text = f.read_text(encoding="utf-8")
        for word in banned:
            assert word not in text, f"{f.name} contains {word}"


def test_core_files_do_not_import_the_governance_package():
    for name in ("module", "schema", "registry", "graph", "engine", "service", "queries", "ingest", "api", "settings"):
        text = (SRC / "intelligence" / f"{name}.py").read_text(encoding="utf-8")
        assert "from olcf.intelligence.governance" not in text and "import olcf.intelligence.governance" not in text, name


def test_policy_modes_cannot_enforce(gw):
    # the single place a mode is validated refuses anything but observe, and every stored policy is observe
    from olcf.intelligence.governance.policies import MODES

    assert MODES == ("observe",)
    assert {p["mode"] for p in gw.ok(gw.get("/policies"))["items"]} <= {"observe"}


# ============================================================================================================================ missing tables
def drop_governance_tables(gw):
    with gw.gov.db.begin() as c:
        for tb in reversed(GS.GOVERNANCE_TABLES):
            c.exec_driver_sql(f'DROP TABLE IF EXISTS "{tb.name}"')
    gw.gov.tables_available(force=True)


def test_intelligence_keeps_working_without_governance_tables(fresh):
    gw = fresh
    aid = gw.aid("customers_clean")
    drop_governance_tables(gw)
    assert gw.gov.tables_available() is False
    # reads of the core: plain documents without the governance block
    doc = gw.ok(gw.intel(f"/assets/{aid}"))
    assert doc["name"] == "customers_clean" and "governance" not in doc
    assert gw.ok(gw.intel("/assets", q="customers"))["items"]
    assert gw.ok(gw.intel(f"/assets/{aid}/lineage", direction="both", depth=3))["nodes"]
    assert gw.ok(gw.intel(f"/assets/{aid}/impact", depth=3))
    assert gw.ok(gw.intel(f"/assets/{aid}/documentation"))
    # filters that need governance say so (503 with a remediation), they do not return a wrong list
    r = gw.intel("/assets", domain="anything")
    assert r.status_code == 503 and gw.code(r) == "unavailable" and "migrate" in r.json()["error"]["remediation"]
    # governance routes: 503 everywhere, never 500
    for method, path in (("get", "/summary"), ("get", "/glossary/terms"), ("get", "/domains"), ("get", "/products"), ("get", "/classifications"), ("get", "/review-queue"),
                         ("get", "/changes"), ("get", "/policies"), ("get", "/violations"), ("get", "/certifications"), ("get", f"/assets/{aid}"), ("get", f"/assets/{aid}/quality"),
                         ("get", f"/assets/{aid}/owners"), ("get", f"/assets/{aid}/graph")):
        resp = getattr(gw, method)(path)
        assert resp.status_code == 503, (path, resp.status_code, resp.text[:200])
        assert resp.json()["error"]["code"] == "unavailable"
    assert gw.post("/glossary/terms", {"name": "x"}).status_code == 503
    assert gw.post("/classifications/scan", {}).status_code == 503
    # a scan, the event hook and the background pass survive
    gw.alter_table("customers_clean", [("customer_id", "int"), ("email", "string"), ("region", "string"), ("extra", "string")])
    out = gw.tick()
    assert isinstance(out, dict)
    assert gw.ok(gw.intel(f"/assets/{aid}"))["name"] == "customers_clean"
    # a merge does not need the governance schema
    gw.svc.registry.merge(gw.aid("orders"), gw.aid("raw_dump"), reason="isolation")
    # health says what is wrong; the platform stays healthy
    h = gw.ok(gw.req("GET", "/admin/health"))
    comp = next(c for c in (h.get("components") or h.get("checks") or []) if c["name"] == "intelligence-governance")
    assert comp["status"] in ("degraded", "DEGRADED") and "missing" in comp["detail"] and "migrate" in comp["detail"]


def test_governance_recovers_when_the_tables_come_back(fresh):
    gw = fresh
    drop_governance_tables(gw)
    assert gw.get("/summary").status_code == 503
    with gw.ctx.db.begin() as c:        # the schema was never applied: the operator runs 'olcf db migrate'
        c.exec_driver_sql("DELETE FROM schema_migrations WHERE stream = 'governance'")
    gw.ctx.db.migrate()
    gw.gov.tables_available(force=True)
    assert gw.ok(gw.get("/summary"))["health"]["tables_available"] is True
    gw.gov.classifications.ensure_builtins()
    assert gw.ok(gw.get("/classifications"))["items"]


def test_scan_without_governance_tables_does_not_raise(fresh):
    gw = fresh
    drop_governance_tables(gw)
    for _ in range(2):
        gw.alter_table("orders", [("order_id", "int")])
    assert gw.gov.on_event({"type": "intelligence.scan.finished", "payload": {"status": "succeeded", "scan_id": "scn_x", "source_id": "local"}}) is None
    assert gw.gov.on_event({"type": "dataset.schema_changed", "payload": {"table": "t"}}) is None
    assert gw.gov.on_event({"type": "something.else"}) is None
    assert gw.gov.on_event({"type": "intelligence.scan.finished", "payload": None}) is None


def test_event_subscriber_only_enqueues(fresh):
    """The subscriber runs inside the publisher: it must be a cheap insert, with the work left to the background pass."""
    gw = fresh
    from sqlalchemy import func, select

    sig = GS.gov_signals
    with gw.gov.db.connect() as c:
        before = c.execute(select(func.count()).select_from(sig)).scalar()
    sid = gw.rescan(__import__("olcf.intelligence.model", fromlist=["Facts"]).Facts([], []), mode="incremental")
    assert sid
    with gw.gov.db.connect() as c:
        after = c.execute(select(func.count()).select_from(sig)).scalar()
    assert after == before + 1
