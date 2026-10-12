# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""Fixtures of the Intelligence *system* suite: the real application with every module, the real analyzers and connectors, no stubs.

* ``platform`` (module scope) - all modules of ``olcf.api.modules.MODULES`` (no embedded worker, no Spark), the administrator logged in, the sample installed (``size=small``) and
  therefore scanned by the installer. :class:`Platform` has the small helpers the tests share (``get`` / ``post``, ``asset`` lookup, ``lineage`` / ``origin`` / ``impact`` documents, SDK clients).
* ``build_platform(path, modules)`` - the same for any list of modules, used by the deployment-shape tests.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import httpx
import pytest
from click.testing import CliRunner
from fastapi.testclient import TestClient
from sqlalchemy import select

from olcf.api.app import create_app
from olcf.core.config import load_settings
from olcf.sdk import Client
from tests.intelligence.surfaces_support import McpClient

ROOT = Path(__file__).resolve().parents[3]
PASSWORD = "correct horse battery staple"
RUNTIME_ID = "olcf-rt-fake-intel-1"
_COUNTER = itertools.count(1)


class Cli:
    """Runs ``olcf`` commands in-process (click's CliRunner) with an SDK client bound to the in-process transport, as the user whose headers were given (default: the administrator)."""

    def __init__(self, platform: Platform, who: dict[str, str] | None = None) -> None:
        self.p, self.who = platform, who or platform.admin
        self.runner = CliRunner()

    def run(self, *args: str, expect: int | None = 0) -> Any:
        import olcf.cli.state as cli_state
        from olcf.cli.main import cli

        token = self.who["Authorization"].split(" ", 1)[1]
        mp = pytest.MonkeyPatch()
        mp.setenv("NO_COLOR", "1")
        mp.setenv("OLCF_CONFIG_DIR", tempfile.mkdtemp())
        for var in ("OLCF_URL", "OLCF_TOKEN"):
            mp.delenv(var, raising=False)
        mp.setattr(cli_state, "Client", lambda *a, **k: Client("http://testserver", http_client=self.p.http, token=token, max_retries=0))
        try:
            res = self.runner.invoke(cli, list(args), catch_exceptions=False, prog_name="olcf")
        finally:
            mp.undo()
        if expect is not None:
            assert res.exit_code == expect, f"olcf {' '.join(args)} -> exit {res.exit_code}, expected {expect}\n{res.output}"
        return res

    def json(self, *args: str, expect: int | None = 0) -> Any:
        return json.loads(self.run("--json", *args, expect=expect).stdout)


class Platform:
    """The running application plus the helpers the system tests share."""

    def __init__(self, app: Any, http: TestClient, token: str) -> None:
        self.app, self.http, self.token = app, http, token
        self.admin = {"Authorization": f"Bearer {token}"}

    @property
    def ctx(self) -> Any:
        return self.app.state.ctx

    @property
    def intel(self) -> Any:
        return self.ctx.service("intelligence")

    # ---- HTTP
    def get(self, path: str, who: dict[str, str] | None = None, expect: int | None = 200, **params: Any) -> Any:
        r = self.http.get(path, params={k: v for k, v in params.items() if v is not None}, headers=who or self.admin)
        if expect is not None:
            assert r.status_code == expect, f"GET {path} {params} -> {r.status_code} {r.text[:400]}"
        return r.json() if r.content else None

    def post(self, path: str, body: Any = None, who: dict[str, str] | None = None, expect: int | None = 200, **params: Any) -> Any:
        r = self.http.post(path, params=params, json=body if body is not None else {}, headers=who or self.admin)
        if expect is not None:
            assert r.status_code == expect, f"POST {path} -> {r.status_code} {r.text[:400]}"
        return r.json() if r.content else None

    def sdk(self, token: str | None = None) -> Client:
        return Client("http://testserver", http_client=self.http, token=token or self.token, max_retries=0)

    def user(self, name: str, role: str | None = "Viewer", *, workspace_id: str | None = None) -> dict[str, str]:
        """A new user with one role binding (org level, or in ``workspace_id``) and its auth headers."""
        ident = self.ctx.service("identity")
        row = ident.create_principal(name, kind="user", password=PASSWORD)
        if role:
            ident.bind(row["id"], role, scope_type="workspace" if workspace_id else "org", scope_id=workspace_id or "")
        r = self.http.post("/v1/identity/login", json={"username": name, "password": PASSWORD})
        assert r.status_code == 200, r.text
        return {"Authorization": f"Bearer {r.json()['access_token']}"}

    def agent(self, name: str, role: str | None = "Viewer", *, workspace_id: str | None = None, scopes: tuple[str, ...] = ("mcp",)) -> dict[str, str]:
        """Headers of a scoped personal access token (default scope ``mcp``) of a new user with one role binding."""
        ident = self.ctx.service("identity")
        row = ident.create_principal(name, kind="user", password=PASSWORD)
        if role:
            ident.bind(row["id"], role, scope_type="workspace" if workspace_id else "org", scope_id=workspace_id or "")
        token, _ = self.ctx.tokens.create_pat(row["id"], f"{name}-pat", scopes=list(scopes), ttl_days=1)
        return {"Authorization": f"Bearer {token}"}

    def mcp(self, headers: dict[str, str] | None = None) -> McpClient:
        client = McpClient(self.http, headers or self.agent(f"agent{next(_COUNTER)}", "Contributor"))
        client.init()
        return client

    def cli(self, who: dict[str, str] | None = None) -> Cli:
        return Cli(self, who)

    # ---- the graph
    def asset(self, type_: str, name: str, parent: str | None = None, path_has: str | None = None, status: str | None = "active") -> str:
        """The id of the one asset of that type and name (``status`` ``None``: whatever its status; optionally under a parent of that name / with ``path_has`` in its path)."""
        from olcf.intelligence.schema import assets

        a = assets.c
        with self.ctx.db.connect() as c:
            stmt = select(a).where(a.type == type_, a.name == name, *([a.status == status] if status else [a.status != "removed"]))
            rows = [dict(r._mapping) for r in c.execute(stmt)]
            if parent is not None:
                names = {r["id"]: r["name"] for r in c.execute(select(a.id, a.name)).mappings()} if rows else {}
                rows = [r for r in rows if r["parent_id"] and names.get(r["parent_id"]) == parent]
        if path_has:
            rows = [r for r in rows if path_has in (r["path"] or "")]
        assert len(rows) == 1, f"{len(rows)} assets {type_} '{name}' (parent={parent}, path_has={path_has}): {[r['path'] for r in rows]}"
        return str(rows[0]["id"])

    def drain(self) -> None:
        """Process the inbox now (the background loop would do it within a few seconds)."""
        mod = next(m for m in self.ctx.modules if m.name == "intelligence")
        mod.processor.tick()

    def scan(self, kind: str = "local", mode: str = "incremental", **body: Any) -> dict[str, Any]:
        """Run a scan of the (first) source of that kind to the end (``wait=true``) and return the scan document."""
        src = next(s for s in self.get("/v1/intelligence/sources")["items"] if s["kind"] == kind)
        doc = self.post(f"/v1/intelligence/sources/{src['id']}/scan", {"mode": mode, **body}, expect=202, wait="true")
        return doc["scan"] if isinstance(doc, dict) and isinstance(doc.get("scan"), dict) else doc

    def lineage(self, asset_id: str, **params: Any) -> dict[str, Any]:
        return self.get(f"/v1/intelligence/assets/{asset_id}/lineage", **params)

    def origin(self, asset_id: str, **params: Any) -> dict[str, Any]:
        return self.get(f"/v1/intelligence/assets/{asset_id}/origin", **params)

    def impact(self, asset_id: str, **params: Any) -> dict[str, Any]:
        return self.get(f"/v1/intelligence/assets/{asset_id}/impact", **params)


@contextmanager
def build_platform(path: Path, modules: list[str] | None = None, *, install_sample: bool = False, setup: Any = None) -> Iterator[Platform]:
    """Build the application under ``path`` with ``modules`` (``None``: every module), log the administrator in and optionally install the small sample."""
    s = load_settings(path / "data", environment="test")
    s.compute.embedded_worker = False
    if setup is not None:
        setup(s)
    app = create_app(s, module_names=modules)
    with TestClient(app) as c:
        pw = app.state.ctx.settings.path("secrets", "bootstrap-admin.txt").read_text().split("password=")[1].strip()
        r = c.post("/v1/identity/login", json={"username": "admin", "password": pw})
        assert r.status_code == 200, r.text
        p = Platform(app, c, r.json()["access_token"])
        if install_sample:
            r = c.post("/v1/sample/install", json={"size": "small"}, headers=p.admin)
            assert r.status_code == 201, r.text
            p.sample = r.json()  # type: ignore[attr-defined]
        yield p


@pytest.fixture(scope="module")
def platform(tmp_path_factory) -> Iterator[Platform]:
    with build_platform(tmp_path_factory.mktemp("system"), None, install_sample=True) as p:
        yield p


def wait_until(fn: Any, timeout: float = 30.0, interval: float = 0.1, what: str = "condition") -> Any:
    """Poll ``fn`` until it returns something truthy (exceptions count as 'not yet'); fail with ``what`` and the last value after ``timeout`` seconds."""
    end = time.monotonic() + timeout
    last: Any = None
    while time.monotonic() < end:
        try:
            last = fn()
            if last:
                return last
        except Exception as exc:  # noqa: BLE001
            last = exc
        time.sleep(interval)
    raise AssertionError(f"timed out waiting for {what} (last: {last!r})")


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class LivePlatform(Platform):
    """A platform served by a real uvicorn (all modules) with a real worker agent subprocess, so jobs run in real runner processes (no Spark: the runtime directory is the NODE
    package's own look-alike, ``tests.node.fakes.make_fake_runtime``). ``http`` is an ``httpx.Client`` instead of the in-process TestClient; everything else is the same."""

    agent_proc: subprocess.Popen[bytes] | None = None

    def submit(self, source: str, *, workspace_id: str, kind: str = "script", lakehouse: str | None = None, **over: Any) -> dict[str, Any]:
        body: dict[str, Any] = {"kind": kind, "workspace_id": workspace_id, "engine": "python", "source": {"format": "py", "content": source}, "name": "sys-" + str(next(_COUNTER)),
                                "runtime": {"runtime_id": RUNTIME_ID}}
        if lakehouse:
            body["default_lakehouse"] = {"name": lakehouse}
        body.update(over)
        return self.post("/v1/jobs", body, expect=202)

    def wait_job(self, job_id: str, timeout: float = 120) -> dict[str, Any]:
        return wait_until(lambda: (j := self.get(f"/v1/jobs/{job_id}"))["state"] in ("succeeded", "failed", "cancelled", "timed_out") and j, timeout, 0.25, f"job {job_id} to finish")

    def logs(self, job_id: str) -> str:
        return str(self.get(f"/v1/jobs/{job_id}/logs", limit=65536)["data"])


@contextmanager
def build_live_platform(path: Path, *, install_sample: bool = True, agent: bool = True, setup: Any = None, runtime_family: str = "olcf-native") -> Iterator[LivePlatform]:
    """The whole application (every module) behind a real HTTP server, the sample installed and a real worker agent online (``agent=False`` for no worker).

    ``setup(settings)`` may change the settings before the application is built; ``runtime_family`` is the family of the look-alike runtime the agent offers (the Fabric adapter asks
    for ``fabric-2.0-compatible``).
    """
    import uvicorn

    s = load_settings(path / "data", environment="test")
    s.compute.embedded_worker = False
    if setup is not None:
        setup(s)
    s.compute.scheduler_interval_seconds = 0.2
    s.compute.worker_heartbeat_seconds = 1.0
    s.compute.worker_offline_after_seconds = 6.0
    s.compute.spark_master_port_base = _free_port()
    s.compute.spark_master_ui_port_base = _free_port()
    s.server.host, s.server.port = "127.0.0.1", _free_port()
    app = create_app(s)
    server = uvicorn.Server(uvicorn.Config(app, host=s.server.host, port=s.server.port, log_level="warning", ws="websockets-sansio", lifespan="on", access_log=False))
    thread = threading.Thread(target=lambda: asyncio.run(server.serve()), name="olcf-system-test-server", daemon=True)
    thread.start()
    proc: subprocess.Popen[bytes] | None = None
    http: httpx.Client | None = None
    try:
        wait_until(lambda: server.started, 90, 0.1, "the control plane to start")
        http = httpx.Client(base_url=s.base_url, timeout=60)
        pw = s.path("secrets", "bootstrap-admin.txt").read_text().split("password=")[1].strip()
        r = http.post("/v1/identity/login", json={"username": "admin", "password": pw})
        assert r.status_code == 200, r.text
        p = LivePlatform(app, http, r.json()["access_token"])  # type: ignore[arg-type]
        if install_sample:
            r = http.post("/v1/sample/install", json={"size": "small"}, headers=p.admin)
            assert r.status_code == 201, r.text
            p.sample = r.json()  # type: ignore[attr-defined]
        if agent:
            sys.path.insert(0, str(ROOT))
            from tests.node import fakes

            assert http.put("/v1/admin/settings/compute.allow_unsigned_runtimes", headers=p.admin, json={"value": True}).status_code == 200
            manifest = json.loads((ROOT / "src/olcf/runtime/manifests/olcf-rt-native-1.0.0.json").read_text())
            manifest.update({"id": RUNTIME_ID, "family": runtime_family, "version": "1.0.0-fake"})
            manifest.pop("signature", None)
            r = http.post("/v1/runtimes", headers=p.admin, json={"manifest": manifest})
            assert r.status_code == 201, r.text
            state = path / "agent" / "state"
            state.mkdir(parents=True)
            fakes.make_fake_runtime(state, RUNTIME_ID, family=runtime_family)
            r = http.post("/v1/workers/enrollment-tokens", headers=p.admin, json={"name": "system-agent", "ttl_seconds": 600})
            assert r.status_code in (200, 201), r.text
            env = {k: v for k, v in os.environ.items() if k != "OLCF_WORKER_TOKEN"}
            env["PYTHONPATH"] = os.pathsep.join(os.path.abspath(x) for x in (env.get("PYTHONPATH") or str(ROOT / "src")).split(os.pathsep) if x)
            env["OLCF_WORKER_TOKEN"] = r.json()["token"]
            cmd = [sys.executable, "-m", "olcf.worker.main", "run", "--url", s.base_url, "--name", "system-agent", "--state-dir", str(state)]
            with open(path / "agent" / "agent.log", "ab") as out:
                proc = subprocess.Popen(cmd, stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, env=env, start_new_session=True, cwd=str(path / "agent"))  # noqa: S603
            p.agent_proc = proc
            wait_until(lambda: any(w["state"] == "online" for w in p.get("/v1/workers")["items"]), 90, 0.5, "the worker agent to come online")
            wait_until(lambda: RUNTIME_ID in p.ctx.service("compute").registry.node_infos()[0].ready_runtimes, 60, 0.5, "the agent to report the runtime ready")
        yield p
    finally:
        if proc is not None and proc.poll() is None:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(proc.pid, signal.SIGTERM)
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                with contextlib.suppress(ProcessLookupError, PermissionError):
                    os.killpg(proc.pid, signal.SIGKILL)
        if http is not None:
            http.close()
        server.should_exit = True
        thread.join(timeout=30)
