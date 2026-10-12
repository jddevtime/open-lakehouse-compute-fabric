# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""A REAL OLCF stack for browser tests: the full control plane (all modules) in a uvicorn subprocess plus N production worker agents.

Nothing here is mocked. The control plane is started with ``python -m uvicorn olcf.api.app:create_app --factory`` exactly as an operator would, workers
enrol with real enrollment tokens and (when the cached Spark test runtime from ``tests/node/runtime_fixture.py`` exists) share that verified runtime so
notebooks and SQL really execute. Usable as a library (``E2EStack``) from pytest and as a command line dev server::

    PYTHONPATH=src python -m tests.e2e.stack --data /tmp/olcf-ui --port 8470 --agents 3
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import random
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import httpx

from tests.e2e import procscan

ROOT = Path(__file__).resolve().parents[2]
RUNTIME_CACHE = Path(os.environ.get("OLCF_TEST_RUNTIME_DIR", "/tmp/olcf-test-runtime"))  # noqa: S108
RUNTIME_ID = "olcf-rt-test-4.1.1"
FABRIC_FAMILY = "fabric-2.0-compatible"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


#: Window the Spark master port blocks are drawn from: clear of every other range ``ComputeSettings`` reserves (driver UI 4040-4056, driver/block-manager/worker 41000-41399) and of the 7077/18080 defaults.
MASTER_PORT_WINDOW = (24000, 40900)
#: Ports per master role. One runtime family is used, but a few spare ports make a lost race for the first port harmless.
MASTER_PORT_SPAN = 8


def port_is_free(port: int) -> bool:
    """True when ``port`` can be bound on the loopback interface right now."""
    with socket.socket() as s:
        try:
            s.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


def allocate_master_ports(span: int = MASTER_PORT_SPAN, *, rng: random.Random | None = None, is_free: Any = port_is_free,
                          window: tuple[int, int] = MASTER_PORT_WINDOW, attempts: int = 200) -> tuple[int, int]:
    """Pick the ``(rpc_base, ui_base)`` of two disjoint blocks of ``span`` consecutive free ports inside ``window``.

    ``ComputeSettings`` validates that no two Spark roles overlap over their FULL configured span, so two independently chosen free ports (spans of 16) can collide; the
    blocks returned here never touch each other, never leave ``window`` and every port in them was free when checked.
    """
    r = rng or random.Random()
    lo, hi = window
    if span < 1 or hi - lo + 1 < 2 * span:
        raise ValueError(f"the window {window} cannot hold two blocks of {span} ports")
    for _ in range(attempts):
        rpc, ui = r.randint(lo, hi - span + 1), r.randint(lo, hi - span + 1)
        if abs(rpc - ui) < span:
            continue
        if all(is_free(b + i) for b in (rpc, ui) for i in range(span)):
            return rpc, ui
    raise RuntimeError(f"no two free blocks of {span} consecutive ports in {lo}-{hi} after {attempts} attempts")


def log_tail(path: Path, lines: int = 40, limit: int = 6000) -> str:
    """The last ``lines`` lines of a log file (at most ``limit`` characters), for error messages."""
    try:
        text = Path(path).read_text(errors="replace")
    except OSError as exc:
        return f"<{path} unreadable: {exc}>"
    return "\n".join(text.splitlines()[-lines:])[-limit:] or f"<{path} is empty>"


class StackFailure(RuntimeError):
    """A stack process died or never came up: waiting longer cannot help, so :func:`wait_until` re-raises it immediately."""


def wait_until(fn: Any, timeout: float = 30.0, interval: float = 0.25, what: str = "condition") -> Any:
    end = time.monotonic() + timeout
    last: Any = None
    while time.monotonic() < end:
        try:
            last = fn()
            if last:
                return last
        except StackFailure:
            raise
        except Exception as exc:  # noqa: BLE001
            last = exc
        time.sleep(interval)
    raise AssertionError(f"timed out waiting for {what} (last: {last!r})")


def _env() -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if k != "OLCF_WORKER_TOKEN"}
    env["PYTHONPATH"] = os.pathsep.join([str(ROOT / "src"), str(ROOT / "sdk" / "fabric"), str(ROOT)])
    return env


class Agent:
    """One production worker agent subprocess."""

    def __init__(self, stack: E2EStack, name: str, labels: str = "", pool: str | None = None, with_runtime: bool = True) -> None:
        self.stack, self.name, self.labels, self.pool, self.with_runtime = stack, name, labels, pool, with_runtime
        self.state_dir = stack.root / "agents" / name / "state"
        self.proc: subprocess.Popen[bytes] | None = None
        self.worker_id: str | None = None

    def start(self) -> Agent:
        sd = self.state_dir
        (sd / "runtimes").mkdir(parents=True, exist_ok=True)
        src = RUNTIME_CACHE / "state" / "runtimes"
        if self.with_runtime and (src / RUNTIME_ID).exists():
            link = sd / "runtimes" / RUNTIME_ID
            if not link.exists():
                os.symlink(src / RUNTIME_ID, link)
            st = src / "state.json"
            if st.exists() and not (sd / "runtimes" / "state.json").exists():
                (sd / "runtimes" / "state.json").write_text(st.read_text())
        r = self.stack.http.post("/v1/workers/enrollment-tokens", headers=self.stack.admin, json={"name": self.name, "ttl_seconds": 600})
        assert r.status_code in (200, 201), r.text
        env = _env()
        env["OLCF_WORKER_TOKEN"] = r.json()["token"]
        cmd = [sys.executable, "-m", "olcf.worker.main", "run", "--url", self.stack.url, "--name", self.name, "--state-dir", str(sd)]
        if self.with_runtime and (RUNTIME_CACHE / "trust").exists():
            cmd += ["--runtime-trust-dir", str(RUNTIME_CACHE / "trust")]  # the cached runtime is signed with the NODE package's test key
        if self.labels:
            cmd += ["--labels", self.labels]
        if self.pool:
            cmd += ["--pool", self.pool]
        log = sd.parent / "agent.log"
        with open(log, "ab") as out:
            self.proc = subprocess.Popen(cmd, stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, env=env, start_new_session=True, cwd=str(sd.parent))  # noqa: S603
        return self

    def stop(self) -> None:
        p = self.proc
        if p is None or p.poll() is not None:
            return
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(p.pid, signal.SIGTERM)
        try:
            p.wait(timeout=25)
        except subprocess.TimeoutExpired:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(p.pid, signal.SIGKILL)
            p.wait(timeout=10)


class E2EStack:
    """Control plane subprocess + agents + an authenticated httpx client for arranging test data."""

    def __init__(self, root: Path, port: int | None = None) -> None:
        self.root = Path(root)
        self.port = port or free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self.proc: subprocess.Popen[bytes] | None = None
        self.agents: list[Agent] = []
        self.http: httpx.Client = None  # type: ignore[assignment]
        self.admin: dict[str, str] = {}
        self.username = "admin"
        self.password = ""
        self.log = self.root / "control-plane.log"

    # ------------------------------------------------------------------ lifecycle
    def start(self) -> E2EStack:
        self.root.mkdir(parents=True, exist_ok=True)
        data = self.root / "data"
        env = _env()
        rpc_base, ui_base = allocate_master_ports()
        env.update({
            "OLCF_DATA_DIR": str(data), "OLCF_ENVIRONMENT": "development", "OLCF_SERVER__HOST": "127.0.0.1", "OLCF_SERVER__PORT": str(self.port),
            "OLCF_COMPUTE__EMBEDDED_WORKER": "false", "OLCF_COMPUTE__SCHEDULER_INTERVAL_SECONDS": "0.5", "OLCF_COMPUTE__WORKER_HEARTBEAT_SECONDS": "2",
            "OLCF_COMPUTE__WORKER_OFFLINE_AFTER_SECONDS": "10", "OLCF_COMPUTE__SPARK_MASTER_PORT_BASE": str(rpc_base),
            "OLCF_COMPUTE__SPARK_MASTER_PORT_SPAN": str(MASTER_PORT_SPAN), "OLCF_COMPUTE__SPARK_MASTER_UI_PORT_BASE": str(ui_base),
            "OLCF_COMPUTE__SPARK_MASTER_UI_PORT_SPAN": str(MASTER_PORT_SPAN), "OLCF_LOG_LEVEL": "WARNING",
            # the sandbox agents have 2 cores each; Spark Standalone needs 2 *usable* cores per node, so the OS reservation is released (a real fleet keeps the default)
            "OLCF_COMPUTE__RESERVE_CORES": os.environ.get("OLCF_E2E_RESERVE_CORES", "0"),
        })
        with open(self.log, "ab") as out:
            self.proc = subprocess.Popen([sys.executable, "-m", "uvicorn", "olcf.api.app:create_app", "--factory", "--host", "127.0.0.1", "--port", str(self.port), "--log-level", "warning",
                                          "--ws", "websockets-sansio"], stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, env=env, cwd=str(ROOT), start_new_session=True)  # noqa: S603
        self.http = httpx.Client(base_url=self.url, timeout=60)
        self.wait_control_plane(90)
        pwf = data / "secrets" / "bootstrap-admin.txt"
        self.password = pwf.read_text().split("password=")[1].strip()
        r = self.http.post("/v1/identity/login", json={"username": self.username, "password": self.password})
        assert r.status_code == 200, r.text
        self.admin = {"Authorization": f"Bearer {r.json()['access_token']}"}
        return self

    def wait_control_plane(self, timeout: float = 90.0) -> None:
        """Wait for ``/v1/health``; when the server process exits first (invalid settings, port in use, import error) fail AT ONCE with the tail of its log."""
        end = time.monotonic() + timeout
        last: Any = None
        while time.monotonic() < end:
            rc = self.proc.poll() if self.proc is not None else None
            if rc is not None:
                raise StackFailure(f"the control plane exited with status {rc} before it answered /v1/health; last lines of {self.log}:\n{log_tail(self.log)}")
            try:
                if self.http.get("/v1/health").status_code == 200:
                    return
            except httpx.HTTPError as exc:
                last = exc
            time.sleep(0.25)
        raise StackFailure(f"the control plane did not answer /v1/health within {timeout:.0f}s (last: {last!r}); last lines of {self.log}:\n{log_tail(self.log)}")

    def stop(self) -> None:
        for a in self.agents:
            a.stop()
        self.agents.clear()
        if self.http is not None:
            self.http.close()
        p = self.proc
        if p is not None and p.poll() is None:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(p.pid, signal.SIGTERM)
            try:
                p.wait(timeout=30)
            except subprocess.TimeoutExpired:
                with contextlib.suppress(ProcessLookupError, PermissionError):
                    os.killpg(p.pid, signal.SIGKILL)
                p.wait(timeout=10)
        self.reap()

    def reap(self) -> None:
        """Kill what the agents leave behind (Spark master/worker JVMs, kernel hosts run detached from the agent) by looking for this stack's directory on their command line."""
        procscan.assert_gone(str(self.root), term_rounds=2, pause=2.0)

    # ------------------------------------------------------------------ helpers
    def api(self, method: str, path: str, **kw: Any) -> Any:
        r = self.http.request(method, path, headers=self.admin, **kw)
        if r.status_code >= 400:
            raise AssertionError(f"{method} {path} -> {r.status_code} {r.text[:500]}")
        return r.json() if r.content else None

    def has_runtime_cache(self) -> bool:
        return (RUNTIME_CACHE / "state" / "runtimes" / RUNTIME_ID).exists() and (RUNTIME_CACHE / "manifest.json").exists()

    def register_runtime(self) -> str | None:
        """Register the cached, verified Spark runtime in the catalog (unsigned test manifest allowed by an explicit admin setting)."""
        if not self.has_runtime_cache():
            return None
        self.http.put("/v1/admin/settings/compute.allow_unsigned_runtimes", headers=self.admin, json={"value": True})
        manifest = json.loads((RUNTIME_CACHE / "manifest.json").read_text())
        manifest.update({"family": FABRIC_FAMILY})
        manifest.pop("signature", None)
        r = self.http.post("/v1/runtimes", headers=self.admin, json={"manifest": manifest})
        if r.status_code not in (200, 201, 409):
            raise AssertionError(r.text)
        return RUNTIME_ID

    def add_agent(self, name: str, **kw: Any) -> Agent:
        a = Agent(self, name, **kw).start()
        self.agents.append(a)
        return a

    def wait_nodes_online(self, n: int, timeout: float = 120) -> list[dict[str, Any]]:
        def ok() -> Any:
            for a in self.agents:
                if a.proc is not None and a.proc.poll() is not None:
                    raise StackFailure(f"agent {a.name} exited with status {a.proc.returncode}; last lines of {a.state_dir.parent / 'agent.log'}:\n{log_tail(a.state_dir.parent / 'agent.log')}")
            items = self.http.get("/v1/workers", headers=self.admin).json()["items"]
            online = [w for w in items if w["state"] == "online"]
            return online if len(online) >= n else None

        return wait_until(ok, timeout, 0.5, f"{n} workers online")


def main() -> None:
    ap = argparse.ArgumentParser(description="Run a real OLCF stack for UI development")
    ap.add_argument("--data", default="/tmp/olcf-ui-dev")  # noqa: S108
    ap.add_argument("--port", type=int, default=8470)
    ap.add_argument("--agents", type=int, default=0)
    ap.add_argument("--fresh", action="store_true")
    a = ap.parse_args()
    root = Path(a.data)
    if a.fresh and root.exists():
        shutil.rmtree(root)
    st = E2EStack(root, a.port).start()
    st.register_runtime()
    for i in range(a.agents):
        st.add_agent(f"node-{i + 1}", labels=f"site=lab,rack=r{i % 3 + 1}")
    print(json.dumps({"url": st.url, "username": st.username, "password": st.password}), flush=True)  # noqa: T201
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass
    finally:
        st.stop()


if __name__ == "__main__":
    main()
