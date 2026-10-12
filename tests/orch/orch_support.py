# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Support code of the orchestration (ORCH) tests (import by qualified name: ``from tests.orch.orch_support import Inproc``).

``stack`` is a *live* control plane (uvicorn in a thread; foundation + compute + pipelines + observability modules) with CTRL's fake worker agents, exactly like the compute tests
(:mod:`tests.compute.ctrl_stack` is imported and extended, not modified). SQLite by default; with ``OLCF_TEST_POSTGRES_URL`` set the stack tests also run on PostgreSQL.

``ctxs`` / ``make_ctx`` build an in-process :class:`AppContext` (foundation + pipelines + observability, no HTTP server and no compute module) for the service-level tests that
need a controllable clock or direct database access. Nothing here starts Spark or a JVM.
"""

from __future__ import annotations

import asyncio
import contextlib
import threading
import uuid
from pathlib import Path
from typing import Any

import httpx

from tests.compute import ctrl_stack as CTRL  # noqa: N812 - CTRL's stack helpers, extended here
from tests.compute.fake_agent import JobPlan, free_port

BACKENDS: list[str] = CTRL.BACKENDS
wait_until = CTRL.wait_until
FakeAgent = CTRL.FakeAgent

MODULE_NAMES = ["olcf.api.core.module", "olcf.compute.module", "olcf.pipelines.module", "olcf.observability.module"]
TERMINAL = ("succeeded", "failed", "cancelled", "skipped")


class OrchStack(CTRL.Stack):
    """CTRL's :class:`Stack` with the orchestration modules installed and pipeline helpers."""

    def start(self) -> OrchStack:
        import uvicorn

        from olcf.api.app import create_app

        self.app = create_app(self.settings, module_names=MODULE_NAMES)
        cfg = uvicorn.Config(self.app, host=self.settings.server.host, port=self.settings.server.port, log_level="warning", ws="websockets-sansio", lifespan="on", access_log=False)
        self.server = uvicorn.Server(cfg)

        def run() -> None:
            asyncio.run(self.server.serve())

        self._thread = threading.Thread(target=run, name="olcf-test-server", daemon=True)
        self._thread.start()
        wait_until(lambda: self.server.started, 60, 0.1, "the control plane to start")
        self.url = self.settings.base_url
        self.http = httpx.Client(base_url=self.url, timeout=30)
        pw = self.settings.path("secrets", "bootstrap-admin.txt").read_text().split("password=")[1].strip()
        self.admin = self.login("admin", pw)
        self.ctx.service("settings").set("network.ssrf.allow", ["127.0.0.0/8"])
        return self

    # ------------------------------------------------------------------ pipelines
    def item(self, ws: str, type_: str, name: str, definition: dict[str, Any], expect: int = 201) -> dict[str, Any]:
        r = self.http.post(f"/v1/workspaces/{ws}/items", headers=self.admin, json={"type": type_, "name": name, "definition": definition})
        assert r.status_code == expect, r.text
        return r.json()

    def pipeline(self, ws: str, name: str, activities: list[dict[str, Any]], **extra: Any) -> dict[str, Any]:
        return self.item(ws, "pipeline", name, {"schema": "olcf.pipeline/1", "activities": activities, **extra})

    def start_run(self, pipeline_id: str, parameters: dict[str, Any] | None = None, *, headers: dict[str, str] | None = None, expect: int = 202, **body: Any) -> dict[str, Any]:
        r = self.http.post(f"/v1/pipelines/{pipeline_id}/runs", headers=headers or self.admin, json={"parameters": parameters or {}, **body})
        assert r.status_code == expect, r.text
        return r.json()

    def run(self, run_id: str) -> dict[str, Any]:
        r = self.http.get(f"/v1/pipeline-runs/{run_id}", headers=self.admin)
        assert r.status_code == 200, r.text
        return r.json()

    def wait_run(self, run_id: str, states: tuple[str, ...] = TERMINAL, timeout: float = 30) -> dict[str, Any]:
        return wait_until(lambda: (x := self.run(run_id))["state"] in states and x, timeout, 0.1, f"run {run_id} to reach {states}")

    def activities(self, run_id: str, include_io: bool = True) -> dict[str, dict[str, Any]]:
        r = self.http.get(f"/v1/pipeline-runs/{run_id}/activities", headers=self.admin, params={"include_io": include_io})
        assert r.status_code == 200, r.text
        out: dict[str, dict[str, Any]] = {}
        for a in r.json()["items"]:
            out.setdefault(a["name"], a)
        return out

    def execute(self, ws: str, activities: list[dict[str, Any]], parameters: dict[str, Any] | None = None, timeout: float = 30, **extra: Any) -> dict[str, Any]:
        """Create a pipeline, run it, wait for the end; returns ``{"run": ..., "acts": {name: activity}}``."""

        p = self.pipeline(ws, "p-" + uuid.uuid4().hex[:8], activities, **extra)
        run = self.wait_run(self.start_run(p["id"], parameters)["id"], timeout=timeout)
        return {"run": run, "acts": self.activities(run["id"]), "pipeline": p}


def _install_probes(registry: Any) -> None:
    from tests.orch import probes

    probes.reset()
    for cls in probes.PROBES:
        registry.register(cls, source="test")


class FakeClock:
    """Injectable clock (aware UTC datetimes) for triggers, webhooks, alerts and series; ``advance`` moves time without sleeping."""

    def __init__(self, start: str = "2026-03-01T00:00:30+00:00") -> None:
        from datetime import datetime

        self.t = datetime.fromisoformat(start)

    def __call__(self) -> Any:
        return self.t

    def set(self, iso: str) -> None:
        from datetime import datetime

        self.t = datetime.fromisoformat(iso)

    def advance(self, seconds: float = 0, *, minutes: float = 0, hours: float = 0) -> Any:
        from datetime import timedelta

        self.t = self.t + timedelta(seconds=seconds, minutes=minutes, hours=hours)
        return self.t


class Inproc:
    """An in-process control plane (foundation + storage + catalog + sql + pipelines + observability, no HTTP server, no compute module) for deterministic service-level tests.

    Only the engine is started on demand (``start()``); trigger / webhook / alert loops are driven by calling their ``tick`` methods so tests control time.
    """

    def __init__(self, root: Path, backend: str = "sqlite") -> None:
        from olcf.api.app import install_modules
        from olcf.api.core.identity import bootstrap_admin
        from olcf.core.config import load_settings
        from olcf.core.context import build_context
        from olcf.core.models import WorkspaceCreate

        self.root = root
        self.settings = load_settings(root / "data", environment="test")
        self.settings.compute.embedded_worker = False
        self.ctx = build_context(self.settings)
        self.ctx.modules = install_modules(self.ctx, ["olcf.api.core.module", "olcf.storage.module", "olcf.catalog.module", "olcf.sql.module", "olcf.pipelines.module", "olcf.observability.module"])
        self.pipes = next(m for m in self.ctx.modules if m.name == "pipelines")
        self.obs = next(m for m in self.ctx.modules if m.name == "observability")
        self.ctx.leader.acquire()
        created = bootstrap_admin(self.ctx)
        assert created
        self.admin = self.ctx.authz.principal_from_row(created[0])
        self.ctx.service("settings").set("network.ssrf.allow", ["127.0.0.0/8"])
        _install_probes(self.pipes.engine.registry)
        self.pipes.engine.tick_seconds = 0.05
        self._WorkspaceCreate = WorkspaceCreate
        self.ws_id = self.workspace()["id"]

    # ------------------------------------------------------------------ plumbing
    @property
    def engine(self) -> Any:
        return self.pipes.engine

    @property
    def svc(self) -> Any:
        return self.pipes.service

    @property
    def triggers(self) -> Any:
        return self.pipes.triggers

    @property
    def hooks(self) -> Any:
        return self.pipes.webhooks

    @property
    def alerts(self) -> Any:
        return self.ctx.service("alerts")

    @property
    def metrics(self) -> Any:
        return self.ctx.service("metrics")

    def lakehouse(self, name: str = "lh1", ws: str | None = None) -> Any:
        """Create a lakehouse item (local storage under the test data directory); returns the resolved :class:`Lakehouse`."""
        item = self.item("lakehouse", name, {}, ws)
        return self.ctx.service("storage").get_lakehouse(item["id"])

    @property
    def storage(self) -> Any:
        return self.ctx.service("storage")

    def set_clock(self, clock: Any) -> None:
        """Drive triggers, webhooks and alerts from ``clock`` (a callable returning an aware UTC datetime)."""
        self.triggers._clock = clock
        self.hooks._clock = clock
        self.alerts._clock = clock
        self.obs.series._clock = clock

    def start(self) -> Inproc:
        self.engine.start()
        return self

    def restart_engine(self, *, crash: bool = True) -> Any:
        """Replace the dispatcher by a new instance (after a simulated crash by default): the new one must adopt everything from the database."""
        from olcf.pipelines.engine import PipelineEngine

        old = self.engine
        old.crash() if crash else old.stop()
        new = PipelineEngine(self.ctx, old.registry, tick_seconds=0.05)
        self.pipes.engine = new
        self.svc.engine = new
        self.triggers.engine = new
        new.start()
        return new

    def stop(self) -> None:
        with contextlib.suppress(Exception):
            self.engine.stop(timeout=5)
        with contextlib.suppress(Exception):
            self.hooks.stop()
        self.ctx.db.dispose()

    # ------------------------------------------------------------------ helpers
    def workspace(self, name: str | None = None) -> dict[str, Any]:

        return self.ctx.service("workspaces").create(self.admin, self._WorkspaceCreate(name=name or "ws-" + uuid.uuid4().hex[:8]))

    def item(self, type_: str, name: str, definition: dict[str, Any], ws: str | None = None) -> dict[str, Any]:
        from olcf.core.models import ItemCreate

        return self.ctx.service("items").create(self.admin, ws or self.ws_id, ItemCreate(type=type_, name=name, definition=definition))

    def pipeline(self, name: str, activities: list[dict[str, Any]], ws: str | None = None, **extra: Any) -> dict[str, Any]:
        return self.item("pipeline", name, {"schema": "olcf.pipeline/1", "activities": activities, **extra}, ws)

    def run(self, pipeline: dict[str, Any], parameters: dict[str, Any] | None = None, **kw: Any) -> dict[str, Any]:
        return self.svc.run(self.admin, pipeline["workspace_id"], pipeline["id"], parameters or {}, **kw)

    def get(self, run_id: str) -> dict[str, Any]:
        return self.svc.get_run(self.admin, run_id)

    def wait(self, run_id: str, states: tuple[str, ...] = TERMINAL, timeout: float = 20.0) -> dict[str, Any]:
        return wait_until(lambda: (r := self.get(run_id))["state"] in states and r, timeout, 0.03, f"run {run_id} to reach {states}")

    def acts(self, run_id: str) -> dict[str, list[dict[str, Any]]]:
        out: dict[str, list[dict[str, Any]]] = {}
        for a in self.svc.activities(self.admin, run_id, include_io=True):
            out.setdefault(a["name"], []).append(a)
        return out

    def act(self, run_id: str, name: str, index: int = 0) -> dict[str, Any]:
        return self.acts(run_id)[name][index]

    def execute(self, activities: list[dict[str, Any]], args: dict[str, Any] | None = None, timeout: float = 20.0, **extra: Any) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:

        p = self.pipeline("p-" + uuid.uuid4().hex[:8], activities, **extra)
        run = self.wait(self.run(p, args)["id"], timeout=timeout)
        return run, self.acts(run["id"])


__all__ = ["BACKENDS", "FakeAgent", "FakeClock", "Inproc", "JobPlan", "MODULE_NAMES", "OrchStack", "TERMINAL", "free_port", "wait_until"]
