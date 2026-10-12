# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S608
"""Failure isolation and optionality (contract section 1, requirements 1 to 3).

* a broken subscriber, inbox or graph leaves queries, jobs and ordinary API calls working;
* an import-graph scan proves that no compute-side package imports ``olcf.intelligence`` or looks the service up;
* the platform works without the module, and the module works without compute and SQL.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pyarrow as pa
import pytest
from sqlalchemy import text

import olcf
from olcf.intelligence import model as M
from tests.intelligence.conftest import Builder

SRC = Path(olcf.__file__).resolve().parent
#: packages that must stay ignorant of Intelligence (contract 1.2; ``olcf.jobs`` / ``olcf.worker`` are listed for when they hold code)
ISOLATED = ["compute", "worker", "runtime_lib", "sql", "pipelines", "catalog", "storage", "notebooks", "fabric", "jobs"]
#: the MCP tool modules that serve Intelligence and governance (``olcf.mcp.intel_tools`` / ``gov_tools``): the only code outside the package that asks for the service by name
MCP_SURFACE = {"mcp/intel_tools.py", "mcp/gov_tools.py", "sample/installer.py", "sample/intelligence.py"}
BASE_MODULES = ["olcf.api.core.module", "olcf.storage.module", "olcf.catalog.module", "olcf.sql.module"]


# ------------------------------------------------------------------------------------------------------------------------------ import graph
def _references(path: Path) -> list[tuple[int, str]]:
    """Every place a module refers to ``olcf.intelligence``: imports, ``importlib`` calls and service look-ups by name."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    found: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found += [(node.lineno, a.name) for a in node.names if a.name == "olcf.intelligence" or a.name.startswith("olcf.intelligence.")]
        elif isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            if node.level == 0 and (mod == "olcf.intelligence" or mod.startswith("olcf.intelligence.")):
                found.append((node.lineno, mod))
            if node.level == 0 and mod == "olcf" and any(a.name == "intelligence" for a in node.names):
                found.append((node.lineno, "olcf.intelligence"))
        elif isinstance(node, ast.Call):
            fn = node.func
            name = fn.attr if isinstance(fn, ast.Attribute) else getattr(fn, "id", "")
            first = node.args[0] if node.args else None
            if isinstance(first, ast.Constant) and isinstance(first.value, str):
                if name in ("import_module", "__import__") and first.value.startswith("olcf.intelligence"):
                    found.append((node.lineno, first.value))
                if name in ("service", "maybe_service", "register_service") and first.value == "intelligence":
                    found.append((node.lineno, f"{name}('intelligence')"))
    return found


def _files(pkg: str) -> list[Path]:
    return sorted((SRC / pkg).rglob("*.py")) if (SRC / pkg).is_dir() else []


def test_no_compute_side_package_depends_on_intelligence():
    scanned = 0
    offenders: list[str] = []
    for pkg in ISOLATED:
        for f in _files(pkg):
            scanned += 1
            offenders += [f"{f.relative_to(SRC.parent)}:{line} {what}" for line, what in _references(f)]
    assert scanned > 100, "the scan found suspiciously few files"
    assert not offenders, "compute-side packages must not import or look up Intelligence:\n" + "\n".join(offenders)
    for pkg in ("compute", "sql", "catalog", "storage", "pipelines", "notebooks", "fabric", "runtime_lib"):
        assert _files(pkg), f"package {pkg} has no sources: the scan would be vacuous"


def test_only_registration_lists_name_the_module_outside_its_package():
    """Everywhere else in the source tree, only ``api/modules.py`` and ``db/migrate.py`` (string registration lists) may mention the package as code."""
    allowed_strings = {"api/modules.py", "db/migrate.py"}
    for f in sorted(SRC.rglob("*.py")):
        rel = f.relative_to(SRC).as_posix()
        if rel.startswith("intelligence/"):
            continue
        refs = _references(f)
        if rel in MCP_SURFACE:        # the MCP tools of Intelligence and the sample installer look the service up by name (only while it is installed) but never import the package
            refs = [r for r in refs if r[1] != "maybe_service('intelligence')"]
        assert not refs, f"{rel} references olcf.intelligence in code: {refs}"
        if rel in allowed_strings:
            assert "olcf.intelligence" in f.read_text(encoding="utf-8")


def test_intelligence_core_stays_on_its_allowed_layers():
    allowed = ("olcf.core", "olcf.db", "olcf.security", "olcf.intelligence", "olcf.api.deps")      # + the HTTP dependency helpers every router uses
    bad: list[str] = []
    for name in ("module", "schema", "registry", "graph", "engine", "service", "queries", "ingest", "api", "settings", "model", "runtime"):
        f = SRC / "intelligence" / f"{name}.py"
        tree = ast.parse(f.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            mods = [a.name for a in node.names] if isinstance(node, ast.Import) else [node.module or ""] if isinstance(node, ast.ImportFrom) and node.level == 0 else []
            bad += [f"{name}.py:{node.lineno} {m}" for m in mods if m.startswith("olcf.") and not m.startswith(allowed)]
    assert not bad, bad


# ------------------------------------------------------------------------------------------------------------------------------ optionality
def test_platform_works_without_the_module(settings):
    from fastapi.testclient import TestClient

    from olcf.api.app import create_app

    app = create_app(settings, module_names=BASE_MODULES)
    with TestClient(app) as c:
        ctx = app.state.ctx
        pw = ctx.settings.path("secrets", "bootstrap-admin.txt").read_text().split("password=")[1].strip()
        admin = {"Authorization": "Bearer " + c.post("/v1/identity/login", json={"username": "admin", "password": pw}).json()["access_token"]}
        assert ctx.maybe_service("intelligence") is None
        ws = c.post("/v1/workspaces", json={"name": "plain"}, headers=admin)
        assert ws.status_code == 201
        assert c.post(f"/v1/workspaces/{ws.json()['id']}/lakehouses", json={"name": "lh"}, headers=admin).status_code == 201
        assert c.get("/v1/intelligence/summary", headers=admin).status_code == 404
        assert "intelligence" not in ctx.health.names()
        assert not [k for k in ctx.service("settings").registry if k.startswith("intelligence.")]
        assert c.get("/v1/catalog/tables", headers=admin).status_code in (200, 404)
        assert c.get("/v1/admin/health", headers=admin).status_code == 200


def test_module_works_without_compute_or_sql(settings):
    from fastapi.testclient import TestClient

    from olcf.api.app import create_app

    app = create_app(settings, module_names=["olcf.api.core.module", "olcf.storage.module", "olcf.catalog.module", "olcf.intelligence.module"])
    with TestClient(app) as c:
        ctx = app.state.ctx
        pw = ctx.settings.path("secrets", "bootstrap-admin.txt").read_text().split("password=")[1].strip()
        admin = {"Authorization": "Bearer " + c.post("/v1/identity/login", json={"username": "admin", "password": pw}).json()["access_token"]}
        assert ctx.maybe_service("intelligence") is not None and ctx.maybe_service("compute") is None and ctx.maybe_service("sql") is None
        ws = c.post("/v1/workspaces", json={"name": "alpha"}, headers=admin).json()
        b = Builder(ws["id"])
        svc = ctx.service("intelligence")
        lh = b.lakehouse("itm_lh", "lh")
        nb = b.notebook("itm_nb", "nb")
        a, d = b.table(lh, "a"), b.table(lh, "d")
        sid = svc.engine.begin_scan("local", "full", "manual")
        stats = svc.engine.apply_facts(M.Facts([lh, a, d, nb], [b.edge(nb, a), b.edge(nb, d, "WRITES_TO")]), scan_id=sid, source_id="local")
        svc.engine.finish_scan(sid, "succeeded", stats.as_dict())
        aid = svc.registry.find("lakehouse_table", "itm_lh:d")
        lin = c.get(f"/v1/intelligence/assets/{aid}/lineage", headers=admin)
        assert lin.status_code == 200 and {n["name"] for n in lin.json()["nodes"]} == {"d", "nb", "a"}
        # the SQL-dependent routes answer cleanly instead of failing
        denied = c.get(f"/v1/intelligence/assets/{aid}/sample", headers=admin)          # sampling is opt-in: refused by policy (or unsupported without discovery), never a crash
        assert denied.status_code in (403, 422) and "sampl" in denied.json()["error"]["message"].lower()
        assert c.get("/v1/admin/health", headers=admin).status_code == 200


# ------------------------------------------------------------------------------------------------------------------------------ broken Intelligence, working platform
def _ready_sql(intel):
    ws = intel.workspace("iso")
    lh = intel.lakehouse(ws["id"], "lh1")
    storage = intel.ctx.service("storage")
    storage.tables.write_table(storage.get_lakehouse(lh["id"]), "nums", pa.table({"n": pa.array([1, 2, 3], pa.int64())}))
    return ws["id"], lh["id"]


def _count(intel, ws, lh, expect=200):
    r = intel.post("/v1/sql/execute", json={"sql": "SELECT count(*) AS c FROM lh1.nums", "workspace_id": ws, "lakehouse": lh}, headers=intel.admin)
    assert r.status_code == expect, r.text
    return r.json()


def test_sql_and_events_survive_a_broken_inbox_and_graph(intel):
    ws, lh = _ready_sql(intel)
    baseline = _count(intel, ws, lh)
    assert baseline["rows"] and "3" in str(baseline["rows"])
    mod = next(m for m in intel.ctx.modules if m.name == "intelligence")
    # 1. the inbox table is gone: every event the writer sees fails to store
    with intel.ctx.db.begin() as conn:
        conn.execute(text("DROP TABLE inbox"))
    for _ in range(5):
        assert _count(intel, ws, lh)["rows"] == baseline["rows"]
    assert mod.writer.failures >= 3 and "inbox" in (mod.writer.last_error or "").lower()
    evt = intel.ctx.events.publish("dataset.read", {"datasets": []})
    assert evt["seq"], "publishing still works and persists the event"
    # 2. the graph tables are gone as well
    with intel.ctx.db.begin() as conn:
        for t in ("edges", "assets", "asset_aliases", "scans", "gaps"):
            conn.execute(text(f"DROP TABLE {t}"))
    assert _count(intel, ws, lh)["rows"] == baseline["rows"]
    assert intel.post("/v1/workspaces", json={"name": "still-creating"}, headers=intel.admin).status_code == 201          # item / workspace events keep flowing
    assert intel.get("/v1/workspaces", headers=intel.admin).status_code == 200
    # Intelligence itself reports the problem instead of taking anything down with it
    down = intel.get("/v1/intelligence/summary", headers=intel.admin)
    assert down.status_code == 503 and down.json()["error"]["code"] == "unavailable" and "unaffected" in down.json()["error"]["remediation"]
    assert intel.get("/v1/intelligence/assets", headers=intel.admin).status_code == 503
    health = intel.get("/v1/admin/health", headers=intel.admin)
    assert health.status_code == 200
    comp = next(c for c in health.json()["components"] if c["name"] == "intelligence")
    assert comp["status"] in ("degraded", "down", "unknown")
    # the background loop survives a failing tick (it logs and tries again); a manual tick may raise, the loop wrapper catches it
    with pytest.raises(Exception):  # noqa: B017, PT011 - any database error is fine here
        mod.processor.tick()
    assert mod.loop._thread is not None and mod.loop._thread.is_alive()


def test_a_broken_subscriber_object_does_not_affect_publishers(intel):
    ws, lh = _ready_sql(intel)
    mod = next(m for m in intel.ctx.modules if m.name == "intelligence")

    def boom(evt):
        raise RuntimeError("intelligence subscriber exploded")

    mod._unsubscribe()
    intel.ctx.events.subscribe(boom)
    assert _count(intel, ws, lh)["rows"]
    assert intel.post("/v1/workspaces", json={"name": "after-boom"}, headers=intel.admin).status_code == 201


def test_a_slow_inbox_write_failure_is_counted_not_raised(intel, monkeypatch):
    ws, lh = _ready_sql(intel)
    mod = next(m for m in intel.ctx.modules if m.name == "intelligence")
    monkeypatch.setattr(mod.writer, "db", type("Dead", (), {"begin": staticmethod(lambda: (_ for _ in ()).throw(TimeoutError("locked")))})())
    assert _count(intel, ws, lh)["rows"]
    assert mod.writer.failures >= 1


def test_jobs_run_with_a_broken_inbox_and_graph(tmp_path, monkeypatch):
    """A real control plane with fake agents (the compute test stack) and the Intelligence module installed: jobs succeed with its tables dropped."""
    import olcf.api.app as appmod
    from tests.compute.ctrl_stack import Stack

    real = appmod.create_app

    def with_intelligence(settings, module_names=None, **kw):
        return real(settings, module_names=[*(module_names or []), "olcf.storage.module", "olcf.catalog.module", "olcf.sql.module", "olcf.intelligence.module"], **kw)

    monkeypatch.setattr(appmod, "create_app", with_intelligence)
    st = Stack(tmp_path / "jobstack", "sqlite").start()
    try:
        assert st.ctx.maybe_service("intelligence") is not None
        ws = st.workspace()
        agent = st.agent("iso-agent")
        st.wait_worker(agent)
        ok = st.wait_job(st.submit(ws)["id"])
        assert ok["state"] == "succeeded"
        with st.ctx.db.begin() as conn:
            for t in ("inbox", "edges", "assets", "asset_aliases", "scans", "gaps", "analysis_cache", "asset_versions"):
                conn.execute(text(f"DROP TABLE {t}"))
        for _ in range(2):
            job = st.wait_job(st.submit(ws)["id"])
            assert job["state"] == "succeeded", job
        assert "fake job done" in st.logs(job["id"])
    finally:
        st.stop()


def test_a_healthy_job_still_feeds_the_inbox(tmp_path, monkeypatch):
    import olcf.api.app as appmod
    from tests.compute.ctrl_stack import Stack

    real = appmod.create_app
    monkeypatch.setattr(appmod, "create_app", lambda settings, module_names=None, **kw: real(settings, module_names=[*(module_names or []), "olcf.intelligence.module"], **kw))
    st = Stack(tmp_path / "jobstack2", "sqlite").start()
    try:
        ws = st.workspace()
        st.wait_worker(st.agent("iso-agent-2"))
        assert st.wait_job(st.submit(ws)["id"])["state"] == "succeeded"
        with st.ctx.db.connect() as conn:
            types = {r[0] for r in conn.execute(text("select type from inbox"))}
        assert "job.state_changed" in types
    finally:
        st.stop()
