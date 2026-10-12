# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Fixtures for the tooling tests: an in-process control plane (foundation + data + compute + notebooks modules), a live
subprocess server, and an isolated CLI configuration directory."""

from __future__ import annotations

import contextlib
import os
import shutil
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[2]
MODULES = [
    "olcf.api.core.module", "olcf.storage.module", "olcf.catalog.module", "olcf.sql.module", "olcf.compute.module",
    "olcf.notebooks.module", "olcf.mcp.module", "olcf.sample.module",
]


@pytest.fixture(autouse=True)
def _isolated_env(tmp_path, monkeypatch):
    """Never touch the developer's real ~/.olcf or CLI config; never use a proxy for loopback."""
    monkeypatch.setenv("OLCF_CONFIG_DIR", str(tmp_path / "cli-config"))
    for var in ("OLCF_URL", "OLCF_TOKEN", "OLCF_CONTEXT", "OLCF_DATA_DIR", "OLCF_USE_KEYRING"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("NO_COLOR", "1")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def cli_env(base: Path, data_dir: Path) -> dict[str, str]:
    """Environment for CLI subprocesses: private config dir, absolute PYTHONPATH, no proxy for loopback, no inherited OLCF_* settings."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("OLCF_") and k != "_OLCF_COMPLETE"}
    env.update({
        "PYTHONPATH": str(ROOT / "src"), "OLCF_CONFIG_DIR": str(base / "cli-config"), "OLCF_DATA_DIR": str(data_dir),
        "NO_COLOR": "1", "NO_PROXY": "127.0.0.1,localhost", "no_proxy": "127.0.0.1,localhost", "PYTHONUNBUFFERED": "1",
    })
    return env


@pytest.fixture()
def app(tmp_path):
    """A real application (SQLite, no embedded worker) with the foundation, data, compute and notebooks modules."""
    from olcf.api.app import create_app
    from olcf.core.config import load_settings

    s = load_settings(tmp_path / "data", environment="test")
    s.compute.embedded_worker = False
    return create_app(s, module_names=MODULES)


def admin_password(app) -> str:
    return app.state.ctx.settings.path("secrets", "bootstrap-admin.txt").read_text().split("password=")[1].strip()


@pytest.fixture()
def sync_http(app):
    """Starlette's TestClient is an ``httpx.Client`` wired to the ASGI app (lifespan started)."""
    from fastapi.testclient import TestClient

    with TestClient(app) as c:
        c.admin_password = admin_password(app)  # type: ignore[attr-defined]
        c.app = app  # type: ignore[attr-defined]
        yield c


@pytest.fixture()
def client(sync_http):
    """Authenticated SDK client against the in-process app."""
    from olcf.sdk import Client

    c = Client("http://testserver", http_client=sync_http, max_retries=0)
    c.login("admin", sync_http.admin_password)
    yield c


@pytest.fixture()
async def aclient(app):
    """Authenticated async SDK client through ``httpx.ASGITransport`` (lifespan driven by hand: the transport does not run it)."""
    from olcf.sdk import AsyncClient

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as http:
            c = AsyncClient("http://testserver", http_client=http, max_retries=0)
            await c.login("admin", admin_password(app))
            yield c


# --------------------------------------------------------------------------- live server (real process)
class LiveServer:
    """A real ``olcf serve`` process plus helpers to run the CLI against it (in process via CliRunner, or as a subprocess)."""

    def __init__(self, data_dir: Path, port: int, proc: subprocess.Popen[bytes], env: dict[str, str]) -> None:
        self.data_dir, self.port, self.proc, self.env = data_dir, port, proc, env
        self.url = f"http://127.0.0.1:{port}"

    @property
    def admin_password(self) -> str:
        return (self.data_dir / "secrets" / "bootstrap-admin.txt").read_text().split("password=")[1].strip()

    def invoke(self, *args: str, input: str | None = None, env: dict[str, str] | None = None):  # noqa: A002
        """Run ``olcf <args>`` in process (fast); returns the click ``Result`` (``.output`` is stdout, ``.exit_code``)."""
        from click.testing import CliRunner

        from olcf.cli.main import cli

        runner = CliRunner()
        return runner.invoke(cli, list(args), input=input, env={**self.env, **(env or {})}, catch_exceptions=False)

    def json(self, *args: str, input: str | None = None):
        import json

        res = self.invoke("--json", *args, input=input)
        assert res.exit_code == 0, f"olcf --json {' '.join(args)} -> {res.exit_code}\n{res.output}"
        return json.loads(res.stdout)

    def run(self, *args: str, input: str | None = None, env: dict[str, str] | None = None, timeout: float = 120) -> subprocess.CompletedProcess[str]:
        """Run ``python -m olcf.cli.main <args>`` as a real process (exit codes, environment handling)."""
        return subprocess.run([sys.executable, "-m", "olcf.cli.main", *args], env={**self.env, **(env or {})}, input=input, capture_output=True, text=True, timeout=timeout, check=False)


def _wait_healthy(url: str, proc: subprocess.Popen[bytes], log: Path) -> None:
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError("olcf serve exited early:\n" + log.read_text()[-3000:])
        with contextlib.suppress(httpx.HTTPError):
            if httpx.get(url + "/v1/health", timeout=2, trust_env=False).status_code == 200:
                return
        time.sleep(0.3)
    raise RuntimeError("olcf serve did not become healthy:\n" + log.read_text()[-3000:])


def _stop(proc: subprocess.Popen[bytes]) -> None:
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)


@pytest.fixture(scope="module")
def live_server(tmp_path_factory) -> Iterator[LiveServer]:
    """``olcf init`` + ``olcf serve`` as real subprocesses (embedded worker off, so no Spark); one per test module, SIGTERM at the end.

    The CLI context of the throw-away installation lives in a private ``OLCF_CONFIG_DIR`` so the developer's own contexts are never touched.
    """
    base = tmp_path_factory.mktemp("live")
    data = base / "data"
    port = free_port()
    env = cli_env(base, data)
    py = sys.executable
    subprocess.run([py, "-m", "olcf.cli.main", "-q", "--data-dir", str(data), "init", "--port", str(port)], env=env, check=True, capture_output=True, timeout=180)
    logp = base / "serve.log"
    log = open(logp, "wb")  # noqa: SIM115
    proc = subprocess.Popen([py, "-m", "olcf.cli.main", "--data-dir", str(data), "serve", "--port", str(port), "--no-embedded-worker"], env=env, stdout=log, stderr=subprocess.STDOUT)
    srv = LiveServer(data, port, proc, env)
    try:
        _wait_healthy(srv.url, proc, logp)
        yield srv
    finally:
        _stop(proc)
        log.close()


@pytest.fixture(scope="session")
def shellcheck() -> str | None:
    return shutil.which("shellcheck")
