# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Embedded worker: single-node mode is a cluster of one (CONTRACTS 4.2/6/11).

When ``settings.compute.embedded_worker`` is true the control plane enrols *itself* as a worker and keeps one agent child process alive::

    python -m olcf.worker.main run --embedded --url <control plane> --state-dir <data_dir>/workers/<name> [--name N] [--advertise-host H]

* The enrollment token is created internally (``EnrollmentService`` with the system principal, ``replace_existing`` so a lost identity can be re-enrolled) and handed to the child
  through ``$OLCF_WORKER_TOKEN`` (never argv, so it does not show up in the process list). With a valid existing identity no token is created at all.
* The supervisor waits until the control plane answers ``GET /v1/health`` (the server is only listening after startup finished), starts the child, restarts it with exponential
  back-off (1 s -> 60 s, reset after a stable minute) when it dies, and on :meth:`stop` terminates it (SIGTERM, then SIGKILL after a grace period; the embedded agent stops its
  own Spark daemons and kernels).
* If the agent package is not importable this is a no-op with a clear warning (the controller still works with remote workers).
* Child output goes to ``<data_dir>/logs/embedded-worker.log`` (size-capped by rotation on restart).
"""

from __future__ import annotations

import importlib.util
import json
import logging
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import httpx
import psutil
from sqlalchemy import select

from olcf.compute import schema as C
from olcf.compute.hub import EnrollmentService, EnrollmentTokenCreate
from olcf.compute.registry import ComputeRegistry
from olcf.core.context import AppContext
from olcf.core.errors import OlcfError
from olcf.db.util import fetch_one
from olcf.security.rbac import SYSTEM_PRINCIPAL
from olcf.worker import procutil

log = logging.getLogger("olcf.compute.embedded")
MAX_LOG_BYTES = 20 * 1024 * 1024


def embedded_name() -> str:
    host = socket.gethostname().split(".")[0] or "localhost"
    return "".join(ch if (ch.isalnum() or ch in "._-") else "-" for ch in host)[:100] or "localhost"


class EmbeddedWorker:
    """The worker agent that ``olcf serve`` runs next to the control plane: starts it as a subprocess (or adopts a running one), enrolls it and restarts it if it dies.
    """
    def __init__(self, ctx: AppContext, registry: ComputeRegistry, *, name: str | None = None) -> None:
        self.ctx, self.registry = ctx, registry
        self.name = name or embedded_name()
        self.state_dir: Path = ctx.settings.path("workers", self.name)
        self.log_path: Path = ctx.settings.path("logs", "embedded-worker.log")
        self._proc: subprocess.Popen[bytes] | None = None
        self._adopted: psutil.Process | None = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self.restarts = 0
        self.last_exit: int | None = None
        self.started_at: float | None = None
        self.enabled = False
        self.note = ""

    # ------------------------------------------------------------------ public
    @property
    def pid(self) -> int | None:
        p = self._proc
        if p is not None and p.poll() is None:
            return p.pid
        return self._adopted.pid if self._adopted is not None else None

    def status(self) -> dict[str, Any]:
        return {"enabled": self.enabled, "name": self.name, "pid": self.pid, "restarts": self.restarts, "last_exit_code": self.last_exit, "state_dir": str(self.state_dir),
                "note": self.note, "uptime_seconds": round(time.time() - self.started_at, 1) if self.started_at and self.pid else None}

    def start(self) -> bool:
        """Start the supervisor. Returns False (with a warning) when the embedded worker is disabled or the agent is unavailable."""
        if not self.ctx.settings.compute.embedded_worker:
            self.note = "disabled by settings.compute.embedded_worker"
            return False
        if importlib.util.find_spec("olcf.worker.main") is None:
            self.note = "the worker agent module (olcf.worker.main) is not importable"
            log.warning("embedded worker NOT started: %s; enrol workers manually (POST /v1/workers/enrollment-tokens)", self.note)
            return False
        if self._thread and self._thread.is_alive():
            return True
        self.enabled = True
        self._stop.clear()
        self._thread = threading.Thread(target=self._supervise, name="olcf-embedded-worker", daemon=True)
        self._thread.start()
        return True

    def stop(self, timeout: float = 20.0) -> None:
        self._stop.set()
        self._terminate(timeout)
        if self._thread:
            self._thread.join(timeout=timeout + 5)

    # ------------------------------------------------------------------ enrollment
    def _identity_valid(self) -> bool:
        f = self.state_dir / "identity.json"
        if not f.is_file():
            return False
        try:
            ident = json.loads(f.read_text(encoding="utf-8"))
            wid = ident["worker_id"]
        except (OSError, ValueError, KeyError):
            return False
        with self.ctx.db.connect() as conn:
            row = fetch_one(conn, select(C.workers.c.id, C.workers.c.state, C.workers.c.revoked_at).where(C.workers.c.id == wid, C.workers.c.name == self.name))
        return bool(row and row["state"] != "revoked" and not row["revoked_at"])

    def _new_token(self) -> str:
        svc = EnrollmentService(self.ctx, self.registry)
        body = EnrollmentTokenCreate(name=self.name, ttl_seconds=600, replace_existing=True, pool="default", labels={"olcf.io/embedded": "true"})
        return svc.create_token(SYSTEM_PRINCIPAL, body, base_url=self.ctx.settings.base_url)["token"]

    # ------------------------------------------------------------------ supervision
    def _adoptable(self) -> psutil.Process | None:
        """An embedded agent that outlived a previous control-plane process (agents run in their own session): supervise it instead of starting a second one."""
        try:
            doc = json.loads((self.state_dir / "agent.pid").read_text(encoding="utf-8"))
            p = psutil.Process(int(doc["pid"]))
            ct = doc.get("create_time")
            if p.status() == psutil.STATUS_ZOMBIE or (ct is not None and abs(p.create_time() - float(ct)) > 1.0):
                return None
            return p if any("olcf.worker.main" in part or "olcf-worker" in part for part in p.cmdline()) else None
        except (OSError, ValueError, KeyError, psutil.Error):
            return None

    def _wait_for_api(self, timeout: float = 180.0) -> bool:
        url = self.ctx.settings.base_url.rstrip("/") + "/v1/health"
        deadline = time.monotonic() + timeout
        while not self._stop.is_set() and time.monotonic() < deadline:
            try:
                if httpx.get(url, timeout=2.0, verify=False).status_code == 200:  # noqa: S501 - loopback readiness probe of our own server (no data is sent)
                    return True
            except httpx.HTTPError:
                pass
            self._stop.wait(0.5)
        return False

    def _command(self) -> list[str]:
        cmd = [sys.executable, "-m", "olcf.worker.main", "run", "--embedded", "--url", self.ctx.settings.base_url, "--name", self.name, "--state-dir", str(self.state_dir)]
        adv = self.ctx.settings.compute.advertise_host
        if adv:
            cmd += ["--advertise-host", adv]
        if self.ctx.settings.tls.enabled:
            ca = self.ctx.pki.ca_pem
            ca_file = self.state_dir / "controller-ca.pem"
            self.state_dir.mkdir(parents=True, exist_ok=True)
            ca_file.write_text(ca, encoding="utf-8")
            cmd += ["--ca-pem", str(ca_file)]
        return cmd

    def _spawn(self) -> subprocess.Popen[bytes]:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(self.state_dir, 0o700)
        env = dict(os.environ)
        env.pop("OLCF_WORKER_TOKEN", None)
        # the agent trusts the same runtime signing keys as this control plane (the development key only outside production, see runtime.trust.*)
        from olcf.runtime.signing import resolve_dev_key_policy

        t = self.ctx.settings.runtime.trust
        env["OLCF_RUNTIME_ALLOW_DEV_KEY"] = "1" if resolve_dev_key_policy(t.allow_development_key, self.ctx.settings.environment) else "0"
        if self.ctx.settings.runtime.trust.dir and "OLCF_RUNTIME_TRUST_DIR" not in env:
            env["OLCF_RUNTIME_TRUST_DIR"] = self.ctx.settings.runtime.trust.dir
        if env.get("PYTHONPATH"):  # the child runs in its state dir: relative import roots (``PYTHONPATH=src``) must keep pointing where they pointed for us
            env["PYTHONPATH"] = os.pathsep.join(os.path.abspath(x) if x else x for x in env["PYTHONPATH"].split(os.pathsep))
        cmd = self._command()
        if not self._identity_valid():
            env["OLCF_WORKER_TOKEN"] = self._new_token()
            if (self.state_dir / "identity.json").exists():
                cmd.append("--re-enroll")
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        if self.log_path.exists() and self.log_path.stat().st_size > MAX_LOG_BYTES:
            self.log_path.replace(self.log_path.with_suffix(".log.1"))
        out = open(self.log_path, "ab")  # noqa: SIM115 - handed to the child
        try:
            proc = subprocess.Popen(cmd, stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, env=env, cwd=str(self.state_dir), **procutil.popen_kwargs())  # noqa: S603 - fixed argv, no shell
        finally:
            out.close()
        self.started_at = time.time()
        log.info("embedded worker '%s' started (pid %s)", self.name, proc.pid)
        return proc

    def _supervise(self) -> None:
        backoff = 1.0
        if not self._wait_for_api():
            log.error("embedded worker: the control plane did not become ready; giving up")
            self.note = "control plane not ready"
            return
        while not self._stop.is_set():
            started = time.monotonic()
            orphan = self._adoptable()
            if orphan is not None:
                log.info("embedded worker: adopting the running agent (pid %s)", orphan.pid)
                self.note = f"adopted running agent pid {orphan.pid}"
                self._adopted = orphan
                while not self._stop.is_set() and orphan.is_running() and orphan.status() != psutil.STATUS_ZOMBIE:
                    self._stop.wait(1.0)
                self._adopted = None
                if self._stop.is_set():
                    return
                self.restarts += 1
                continue
            try:
                with self._lock:
                    self._proc = self._spawn()
                proc = self._proc
            except (OSError, OlcfError) as exc:
                self.note = f"cannot start the agent: {exc}"
                log.error("embedded worker could not be started: %s", exc)
                if self._stop.wait(backoff):
                    return
                backoff = min(60.0, backoff * 2)
                continue
            while not self._stop.is_set():
                try:
                    rc = proc.wait(timeout=0.5)
                    break
                except subprocess.TimeoutExpired:
                    continue
            else:
                return
            self.last_exit = rc
            if self._stop.is_set():
                return
            self.restarts += 1
            lived = time.monotonic() - started
            backoff = 1.0 if lived > 60 else min(60.0, backoff * 2)
            log.warning("embedded worker exited with code %s after %.0fs; restarting in %.0fs", rc, lived, backoff)
            self.note = f"restarted after exit code {rc}"
            if self._stop.wait(backoff):
                return

    def _terminate(self, timeout: float) -> None:
        orphan = self._adopted
        if orphan is not None:
            try:
                procutil.signal_group(orphan.pid, signal.SIGTERM)  # portable: killpg on POSIX, the process tree on Windows
                orphan.wait(timeout)
            except (ProcessLookupError, PermissionError, psutil.Error):
                pass
        with self._lock:
            proc = self._proc
        if proc is None or proc.poll() is not None:
            return
        try:
            procutil.signal_group(proc.pid, signal.SIGTERM)  # the child leads its own session/process group (POSIX) / is a process tree (Windows)
        except (ProcessLookupError, PermissionError):
            proc.terminate()
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            log.warning("embedded worker did not stop within %.0fs; killing it", timeout)
            try:
                procutil.signal_group(proc.pid, getattr(signal, "SIGKILL", signal.SIGTERM))  # SIGKILL does not exist on Windows (the helper maps it to kill())
            except (ProcessLookupError, PermissionError):
                proc.kill()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                log.error("embedded worker (pid %s) could not be killed", proc.pid)
