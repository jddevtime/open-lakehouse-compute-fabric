# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""A FIRST-RUN stack for tests: exactly what a new user gets from ``olcf quickstart --no-runtime --no-open`` on an empty data directory.

No Spark runtime, no pre-enrolled agents, nothing arranged by the test: the server, its embedded worker (so only the built-in host-Python runtime), the sample
lakehouse and the credentials come from the shipped commands.  ``E2EStack``-compatible attributes (``url``, ``username``, ``password``, ``http``, ``admin``,
``api``) let the browser helpers of ``tests/e2e`` drive it unchanged.
"""

from __future__ import annotations

import contextlib
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import httpx

from tests.e2e import procscan
from tests.e2e.stack import ROOT, free_port, wait_until


class FirstRunStack:
    """``olcf quickstart`` in a throw-away data directory and CLI configuration directory."""

    def __init__(self, root: Path, port: int | None = None) -> None:
        self.root = Path(root)
        self.data = self.root / "data"
        self.port = port or free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self.username = "admin"
        self.password = ""
        self.quickstart: subprocess.CompletedProcess[str] | None = None
        self.http: httpx.Client = None  # type: ignore[assignment]
        self.admin: dict[str, str] = {}

    # ------------------------------------------------------------------ the shipped CLI
    def env(self, **extra: str) -> dict[str, str]:
        env = {k: v for k, v in os.environ.items() if not k.startswith("OLCF_") and k != "_OLCF_COMPLETE"}
        env.update({"PYTHONPATH": os.pathsep.join([str(ROOT / "src"), str(ROOT / "sdk" / "fabric")]), "OLCF_CONFIG_DIR": str(self.root / "cli-config"), "OLCF_DATA_DIR": str(self.data),
                    "NO_COLOR": "1", "NO_PROXY": "127.0.0.1,localhost", "no_proxy": "127.0.0.1,localhost", "PYTHONUNBUFFERED": "1"})
        env.update(extra)
        return env

    def cli(self, *args: str, timeout: float = 300, check: bool = False, input: str | None = None) -> subprocess.CompletedProcess[str]:  # noqa: A002
        res = subprocess.run([sys.executable, "-m", "olcf.cli.main", *args], env=self.env(), capture_output=True, text=True, timeout=timeout, input=input, check=False, cwd=str(ROOT))  # noqa: S603
        if check and res.returncode != 0:
            raise AssertionError(f"olcf {' '.join(args)} -> {res.returncode}\n{res.stdout}\n{res.stderr}")
        return res

    # ------------------------------------------------------------------ lifecycle
    def start(self) -> FirstRunStack:
        self.root.mkdir(parents=True, exist_ok=True)
        self.quickstart = self.cli("quickstart", "--no-runtime", "--no-open", "--port", str(self.port), timeout=600)
        if self.quickstart.returncode != 0:
            raise AssertionError(f"quickstart failed ({self.quickstart.returncode})\n{self.quickstart.stdout}\n{self.quickstart.stderr}")
        self.password = (self.data / "secrets" / "bootstrap-admin.txt").read_text().split("password=")[1].strip()
        self.http = httpx.Client(base_url=self.url, timeout=120, trust_env=False)
        r = self.http.post("/v1/identity/login", json={"username": self.username, "password": self.password})
        assert r.status_code == 200, r.text
        self.admin = {"Authorization": f"Bearer {r.json()['access_token']}"}
        return self

    def stop(self) -> None:
        if self.http is not None:
            self.http.close()
        with contextlib.suppress(Exception):
            self.cli("stop", "--force", timeout=90)
        self.reap()

    def reap(self) -> None:
        """Anything still carrying this stack's directory on its command line (the embedded worker, kernels) is killed; fails if a process survives."""
        procscan.assert_gone(str(self.root))

    # ------------------------------------------------------------------ REST helpers
    def api(self, method: str, path: str, **kw: Any) -> Any:
        r = self.http.request(method, path, headers=self.admin, **kw)
        if r.status_code >= 400:
            raise AssertionError(f"{method} {path} -> {r.status_code} {r.text[:500]}")
        return r.json() if r.content else None

    def wait_job(self, job_id: str, timeout: float = 240) -> dict[str, Any]:
        return wait_until(lambda: (j := self.api("GET", f"/v1/jobs/{job_id}"))["state"] in ("succeeded", "failed", "cancelled", "timed_out") and j, timeout, 0.5, f"job {job_id}")
