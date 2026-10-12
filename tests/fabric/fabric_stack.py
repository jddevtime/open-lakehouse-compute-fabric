# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""A live control plane (uvicorn in a thread; foundation + storage + compute + fabric modules) and, optionally, a real worker agent subprocess with a fake or real runtime."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parents[2]
MODULES = ["olcf.api.core.module", "olcf.storage.module", "olcf.compute.module", "olcf.fabric.module"]
FAKE_RT = "olcf-rt-fake-fabric-1"
FABRIC_FAMILY = "fabric-2.0-compatible"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_until(fn: Any, timeout: float = 30.0, interval: float = 0.1, what: str = "condition") -> Any:
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


def wait_for_agent(fn: Any, log: Any, timeout: float, interval: float, what: str) -> Any:
    """:func:`wait_until` for an agent process: a timeout carries the tail of the agent's own log, so a CI failure says why it never came online."""
    try:
        return wait_until(fn, timeout, interval, what)
    except AssertionError as exc:
        try:
            tail = Path(log).read_bytes()[-14000:].decode("utf-8", "replace")
        except OSError as err:
            tail = f"(no agent log: {err})"
        raise AssertionError(f"{exc}\n--- agent log (last 14000 bytes) ---\n{tail}") from None


class FabricStack:
    def __init__(self, root: Path, **tweaks: Any) -> None:
        from olcf.core.config import load_settings

        self.root = root
        self.settings = load_settings(root / "data", environment="test")
        s = self.settings
        s.compute.embedded_worker = False
        s.compute.scheduler_interval_seconds = 0.2
        s.compute.worker_heartbeat_seconds = 1.0
        s.compute.worker_offline_after_seconds = 6.0
        s.compute.spark_master_port_base = free_port()
        s.compute.spark_master_ui_port_base = free_port()
        s.server.host, s.server.port = "127.0.0.1", free_port()
        for k, v in tweaks.items():
            obj: Any = s
            *path, leaf = k.split("__")
            for part in path:
                obj = getattr(obj, part)
            setattr(obj, leaf, v)
        self.server: Any = None
        self._thread: threading.Thread | None = None
        self.http: httpx.Client = None  # type: ignore[assignment]
        self.admin: dict[str, str] = {}
        self.app: Any = None
        self.agent: RealAgent | None = None

    # ------------------------------------------------------------------ lifecycle
    def start(self) -> FabricStack:
        import uvicorn

        from olcf.api.app import create_app

        self.app = create_app(self.settings, module_names=MODULES)
        cfg = uvicorn.Config(self.app, host=self.settings.server.host, port=self.settings.server.port, log_level="warning", ws="websockets-sansio", lifespan="on", access_log=False)
        self.server = uvicorn.Server(cfg)
        self._thread = threading.Thread(target=lambda: asyncio.run(self.server.serve()), name="olcf-fabric-test-server", daemon=True)
        self._thread.start()
        wait_until(lambda: self.server.started, 60, 0.1, "the control plane to start")
        self.url = self.settings.base_url
        self.http = httpx.Client(base_url=self.url, timeout=60)
        pw = self.settings.path("secrets", "bootstrap-admin.txt").read_text().split("password=")[1].strip()
        r = self.http.post("/v1/identity/login", json={"username": "admin", "password": pw})
        assert r.status_code == 200, r.text
        self.admin = {"Authorization": f"Bearer {r.json()['access_token']}"}
        return self

    def stop(self) -> None:
        if self.agent is not None:
            self.agent.stop()
        if self.http is not None:
            self.http.close()
        if self.server is not None:
            self.server.should_exit = True
        if self._thread is not None:
            self._thread.join(timeout=30)

    @property
    def ctx(self) -> Any:
        return self.app.state.ctx

    @property
    def fabric(self) -> Any:
        return self.ctx.service("fabric")

    # ------------------------------------------------------------------ identity / data helpers
    def workspace(self, name: str | None = None) -> str:
        r = self.http.post("/v1/workspaces", headers=self.admin, json={"name": name or "ws-" + uuid.uuid4().hex[:8]})
        assert r.status_code == 201, r.text
        return r.json()["id"]

    def service_principal(self, workspace_id: str | None, *, scopes: list[str] | None = None, role: str = "ServicePrincipal", name: str | None = None) -> dict[str, str]:
        """A service principal (bound at ``workspace_id`` or org-wide when ``None``) and a platform token headers for it (default scope ``fabric``)."""
        name = name or "fab-sp-" + uuid.uuid4().hex[:6]
        scope = {"scope_type": "workspace", "scope_id": workspace_id} if workspace_id else {"scope_type": "org", "scope_id": ""}
        r = self.http.post("/v1/identity/users", headers=self.admin, json={"name": name, "kind": "service", "roles": [{"role": role, **scope}]})
        assert r.status_code == 201, r.text
        pid = r.json()["id"]
        self.last_principal_id = pid
        t = self.http.post("/v1/identity/tokens", headers=self.admin, json={"name": "t", "principal_id": pid, "scopes": scopes or ["fabric"]})
        assert t.status_code == 201, t.text
        return {"Authorization": f"Bearer {t.json()['token']}"}

    def put_fabric(self, **cfg: Any) -> dict[str, Any]:
        r = self.http.put("/v1/integrations/fabric", headers=self.admin, json=cfg)
        assert r.status_code == 200, r.text
        return r.json()

    # ------------------------------------------------------------------ compute
    def install_runtime(self, runtime_id: str = FAKE_RT, family: str = FABRIC_FAMILY, *, fake: bool = True, state_dir: Path | None = None) -> str:
        """Register a runtime in the catalog; with ``fake`` also build a Spark-less runtime directory the agent accepts."""
        self.http.put("/v1/admin/settings/compute.allow_unsigned_runtimes", headers=self.admin, json={"value": True})
        base = json.loads((ROOT / "src/olcf/runtime/manifests/olcf-rt-2.0.0.json").read_text())
        base.update({"id": runtime_id, "family": family, "version": "2.0.0-fake"})
        base.pop("signature", None)
        r = self.http.post("/v1/runtimes", headers=self.admin, json={"manifest": base})
        assert r.status_code == 201, r.text
        if fake and state_dir is not None:
            sys.path.insert(0, str(ROOT))
            from tests.node import fakes

            fakes.make_fake_runtime(state_dir, runtime_id, family=family)
        return runtime_id

    def start_agent(self, state_dir: Path, name: str = "fab-agent-1") -> RealAgent:
        self.agent = RealAgent(self, name, state_dir).start()
        wait_for_agent(lambda: next((w for w in self.http.get("/v1/workers", headers=self.admin).json()["items"] if w["name"] == name and w["state"] == "online"), None),
                       self.agent.log, 90, 0.5, "the agent to come online")
        return self.agent

    def wait_agent_ready(self, runtime_id: str) -> None:
        wait_until(lambda: runtime_id in self.ctx.service("compute").registry.node_infos()[0].ready_runtimes, 60, 0.5, "the agent to report the runtime ready")


class RealAgent:
    """The production worker agent as a subprocess (the NODE package's ``olcf.worker.main``)."""

    def __init__(self, st: FabricStack, name: str, state_dir: Path) -> None:
        self.st, self.name, self.state_dir = st, name, state_dir
        self.proc: subprocess.Popen[bytes] | None = None
        self.log = state_dir.parent / f"{name}.log"

    def start(self) -> RealAgent:
        r = self.st.http.post("/v1/workers/enrollment-tokens", headers=self.st.admin, json={"name": self.name, "ttl_seconds": 600})
        assert r.status_code in (200, 201), r.text
        env = {k: v for k, v in os.environ.items() if k != "OLCF_WORKER_TOKEN"}
        env["PYTHONPATH"] = os.pathsep.join(os.path.abspath(p) for p in (env.get("PYTHONPATH") or str(ROOT / "src")).split(os.pathsep) if p)
        env["OLCF_WORKER_TOKEN"] = r.json()["token"]
        cmd = [sys.executable, "-m", "olcf.worker.main", "run", "--url", self.st.url, "--name", self.name, "--state-dir", str(self.state_dir)]
        with open(self.log, "ab") as out:
            self.proc = subprocess.Popen(cmd, stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, env=env, start_new_session=True, cwd=str(self.state_dir.parent))  # noqa: S603
        return self

    def stop(self) -> None:
        p = self.proc
        if p is None or p.poll() is not None:
            return
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(p.pid, signal.SIGTERM)
        try:
            p.wait(timeout=30)
        except subprocess.TimeoutExpired:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(p.pid, signal.SIGKILL)
            p.wait(timeout=10)
