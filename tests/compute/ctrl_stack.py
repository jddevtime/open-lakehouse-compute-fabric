# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Support code of the compute-fabric (CTRL) tests: the :class:`Stack` (a *live* control plane, uvicorn in a thread) plus fake worker agents.

Import it by its qualified name (``from tests.compute.ctrl_stack import Stack``); ``conftest.py`` only registers the fixtures built on it.

``stack`` is module-scoped (one server, one database per test module) and fast: the scheduler ticks every 200 ms and heartbeats are 1 s. The database is SQLite by
default; with ``OLCF_TEST_POSTGRES_URL`` set the same tests run against PostgreSQL (each stack gets its own schema, dropped afterwards). The fixtures never need Spark.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import threading
import time
import uuid
from pathlib import Path
from typing import Any

import httpx

from tests.compute.fake_agent import FakeAgent, FakeSparkMaster, free_port, wait_until  # noqa: E402

PG_URL = os.environ.get("OLCF_TEST_POSTGRES_URL")
BACKENDS = ["sqlite"] + (["postgres"] if PG_URL else [])


class Stack:
    """A running control plane and helpers to talk to it."""

    def __init__(self, root: Path, backend: str = "sqlite", **tweaks: Any) -> None:
        from olcf.core.config import load_settings

        self.root, self.backend = root, backend
        self.settings = load_settings(root / "data", environment="test")
        s = self.settings
        s.compute.embedded_worker = False
        s.compute.scheduler_interval_seconds = 0.2
        s.compute.worker_heartbeat_seconds = 1.0
        s.compute.worker_offline_after_seconds = 4.0
        s.compute.spark_master_port_base = free_port()
        s.compute.spark_master_ui_port_base = free_port()
        s.server.host, s.server.port = "127.0.0.1", free_port()
        self._pg_schema: str | None = None
        if backend == "postgres":
            s.db.url = self._pg_url()
        for k, v in tweaks.items():
            obj: Any = s
            *path, leaf = k.split("__")
            for part in path:
                obj = getattr(obj, part)
            setattr(obj, leaf, v)
        self.agents: list[FakeAgent] = []
        self.server: Any = None
        self._thread: threading.Thread | None = None
        self.http: httpx.Client = None  # type: ignore[assignment]
        self.admin: dict[str, str] = {}
        self.app: Any = None

    # ------------------------------------------------------------------ database
    def _pg_url(self) -> str:
        from sqlalchemy import create_engine, text

        assert PG_URL
        self._pg_schema = "olcf_t_" + uuid.uuid4().hex[:10]
        eng = create_engine(PG_URL)
        with eng.begin() as c:
            c.execute(text(f'CREATE SCHEMA "{self._pg_schema}"'))
        eng.dispose()
        sep = "&" if "?" in PG_URL else "?"
        return f"{PG_URL}{sep}options=-csearch_path%3D{self._pg_schema}"

    def _drop_pg(self) -> None:
        if not self._pg_schema:
            return
        from sqlalchemy import create_engine, text

        with contextlib.suppress(Exception):
            eng = create_engine(PG_URL)  # type: ignore[arg-type]
            with eng.begin() as c:
                c.execute(text(f'DROP SCHEMA "{self._pg_schema}" CASCADE'))
            eng.dispose()

    # ------------------------------------------------------------------ lifecycle
    def start(self) -> Stack:
        import uvicorn

        from olcf.api.app import create_app

        self.app = create_app(self.settings, module_names=["olcf.api.core.module", "olcf.compute.module"])
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
        return self

    def stop(self) -> None:
        for a in list(self.agents):
            with contextlib.suppress(Exception):
                a.stop()
        self.agents.clear()
        if self.http is not None:
            self.http.close()
        if self.server is not None:
            self.server.should_exit = True
        if self._thread is not None:
            self._thread.join(timeout=30)
        self._drop_pg()

    @property
    def ctx(self) -> Any:
        return self.app.state.ctx

    @property
    def compute(self) -> Any:
        return self.ctx.service("compute")

    # ------------------------------------------------------------------ identity helpers
    def login(self, name: str, password: str) -> dict[str, str]:
        r = self.http.post("/v1/identity/login", json={"username": name, "password": password})
        assert r.status_code == 200, r.text
        return {"Authorization": f"Bearer {r.json()['access_token']}"}

    def workspace(self, name: str | None = None, **body: Any) -> str:
        name = name or "ws-" + uuid.uuid4().hex[:8]
        r = self.http.post("/v1/workspaces", headers=self.admin, json={"name": name, **body})
        assert r.status_code == 201, r.text
        return r.json()["id"]

    def user(self, role: str, workspace_id: str | None = None, name: str | None = None) -> dict[str, str]:
        """Create a user with ``role`` (at workspace scope when ``workspace_id`` is given) and return its auth headers."""
        name = name or f"{role.lower()}-{uuid.uuid4().hex[:6]}"
        scope = {"scope_type": "workspace", "scope_id": workspace_id} if workspace_id else {"scope_type": "org", "scope_id": ""}
        r = self.http.post("/v1/identity/users", headers=self.admin, json={"name": name, "password": "Correct-Horse-9-Battery", "roles": [{"role": role, **scope}]})
        assert r.status_code == 201, r.text
        return self.login(name, "Correct-Horse-9-Battery")

    # ------------------------------------------------------------------ agents
    def agent(self, name: str | None = None, *, start: bool = True, **kw: Any) -> FakeAgent:
        a = FakeAgent(self, name or "fa-" + uuid.uuid4().hex[:6], **kw)
        a.enroll()
        self.agents.append(a)
        if start:
            a.start()
        return a

    def fake_masters(self) -> list[FakeSparkMaster]:
        return [m for a in list(self.agents) for m in list(a.masters.values())]

    def worker(self, worker_id: str) -> dict[str, Any]:
        r = self.http.get(f"/v1/workers/{worker_id}", headers=self.admin)
        assert r.status_code == 200, r.text
        return r.json()

    def wait_worker(self, agent: FakeAgent, state: str = "online", timeout: float = 20) -> dict[str, Any]:
        return wait_until(lambda: (w := self.worker(agent.worker_id))["state"] == state and w, timeout, 0.1, f"worker {agent.name} to be {state}")  # type: ignore[arg-type]

    def retire_agents(self) -> None:
        """Remove every agent of this stack from the fleet (so the next test starts with an empty fleet)."""
        for a in list(self.agents):
            with contextlib.suppress(Exception):
                a.stop()
            if a.worker_id:
                with contextlib.suppress(Exception):
                    self.http.post(f"/v1/workers/{a.worker_id}/revoke", headers=self.admin, json={})
        self.agents.clear()

    # ------------------------------------------------------------------ jobs
    def submit(self, ws: str, headers: dict[str, str] | None = None, *, source: str = "print('hi')", expect: int = 202, idem: str | None = None, **over: Any) -> dict[str, Any]:
        body: dict[str, Any] = {"kind": "script", "workspace_id": ws, "engine": "python", "source": {"format": "py", "content": source}, "name": "t-" + uuid.uuid4().hex[:6]}
        body.update(over)
        h = dict(headers or self.admin)
        if idem:
            h["Idempotency-Key"] = idem
        r = self.http.post("/v1/jobs", headers=h, json=body)
        assert r.status_code == expect, f"{r.status_code} {r.text}"
        return r.json()

    def job(self, job_id: str, headers: dict[str, str] | None = None) -> dict[str, Any]:
        r = self.http.get(f"/v1/jobs/{job_id}", headers=headers or self.admin)
        assert r.status_code == 200, r.text
        return r.json()

    def wait_job(self, job_id: str, states: tuple[str, ...] | str = ("succeeded", "failed", "cancelled", "timed_out"), timeout: float = 30) -> dict[str, Any]:
        want = (states,) if isinstance(states, str) else states
        return wait_until(lambda: (j := self.job(job_id))["state"] in want and j, timeout, 0.1, f"job {job_id} to reach {want}")

    def logs(self, job_id: str) -> str:
        out, off = "", 0
        for _ in range(100):
            r = self.http.get(f"/v1/jobs/{job_id}/logs", headers=self.admin, params={"offset": off, "limit": 65536})
            assert r.status_code == 200, r.text
            d = r.json()
            out += d["data"]
            if d.get("eof") or d["next_offset"] == off:
                break
            off = d["next_offset"]
        return out


__all__ = ["BACKENDS", "FakeAgent", "Stack", "time", "wait_until"]
