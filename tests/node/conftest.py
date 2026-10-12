# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Fixtures for the NODE tests: fake controller, agent harness, fake runtime state dirs and the real (cached) Spark runtime."""

# ruff: noqa: S107
from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

from olcf.worker.agent import Agent  # noqa: E402
from olcf.worker.config import AgentConfig  # noqa: E402
from olcf.worker.identity import Identity  # noqa: E402
from tests.node.fake_controller import FakeController  # noqa: E402
from tests.node.fakes import make_fake_runtime  # noqa: E402

FAKE_RT = "olcf-rt-fake-1"


class AgentHandle:
    """One agent running in a background thread of the test process (its own asyncio loop, real sockets, real child processes)."""

    def __init__(self, cfg: AgentConfig, ident: Identity) -> None:
        self.cfg, self.ident = cfg, ident
        self.agent = Agent(cfg, ident)
        self.exit_code: int | None = None
        self.error: BaseException | None = None
        self.thread = threading.Thread(target=self._run, name=f"agent-{ident.worker_id}", daemon=True)

    def _run(self) -> None:
        try:
            self.exit_code = self.agent.run()
        except BaseException as exc:  # noqa: BLE001
            self.error = exc

    def start(self) -> AgentHandle:
        self.thread.start()
        return self

    def stop(self, *, kill_children: bool = True, timeout: float = 90.0) -> None:
        if self.thread.is_alive():
            self.agent.request_stop("test teardown", kill_children=kill_children)
            self.thread.join(timeout)
        assert not self.thread.is_alive(), "agent thread did not stop"
        if self.error:
            raise self.error


def make_cfg(state_dir: Path, **over) -> AgentConfig:
    cfg = AgentConfig(state_dir=state_dir, backoff_min=0.2, backoff_max=1.0, heartbeat_seconds=1.0, ws_ping_seconds=10.0, kill_grace_seconds=2.0, timeout_slack_seconds=2.0,
                      max_cores=8, max_memory_mb=64000, daemon_memory="256m", ready_timeout_seconds=120.0, implicit_log_ack_seconds=2.0, log_level="WARNING")
    for k, v in over.items():
        setattr(cfg, k, v)
    return cfg.resolve()


def make_ident(ctl: FakeController, worker_id: str = "wrk_1", token: str = "tok-1", name: str = "node-1") -> Identity:
    return Identity(worker_id=worker_id, worker_token=token, channel_url=f"ws://127.0.0.1:{ctl.port}/v1/workers/ws", url=ctl.url, name=name, heartbeat_seconds=1.0)


@pytest.fixture(autouse=True)
def _quiet_logs() -> None:
    logging.getLogger("olcf").setLevel(logging.WARNING)


@pytest.fixture()
def controller() -> Iterator[FakeController]:
    with FakeController() as ctl:
        yield ctl


@pytest.fixture()
def fake_state(tmp_path: Path) -> Path:
    """An agent state dir holding the fake runtime ``olcf-rt-fake-1``."""
    sd = tmp_path / "state"
    sd.mkdir()
    make_fake_runtime(sd, FAKE_RT)
    return sd


@pytest.fixture()
def start_agent(controller: FakeController, fake_state: Path) -> Iterator[Callable[..., AgentHandle]]:
    """Factory: ``start_agent(**cfg_overrides)`` starts an agent against ``controller`` and waits until it is connected."""
    handles: list[AgentHandle] = []

    def factory(*, state_dir: Path | None = None, wait: bool = True, worker_id: str = "wrk_1", token: str = "tok-1", **over) -> AgentHandle:
        cfg = make_cfg(state_dir or fake_state, **over)
        h = AgentHandle(cfg, make_ident(controller, worker_id, token)).start()
        handles.append(h)
        if wait:
            controller.wait_connected(worker_id, 60)
        return h

    yield factory
    for h in handles:
        try:
            h.stop(kill_children=True)
        except Exception as exc:  # noqa: BLE001
            print(f"agent teardown problem: {exc}", file=sys.stderr)


@pytest.fixture(scope="session")
def node_runtime() -> tuple[Path, str]:
    """The real, cached runtime (pyspark 4.1.1) under /tmp/olcf-test-runtime; built on first use (several minutes)."""
    from tests.node.runtime_fixture import CACHE, ensure_test_runtime

    mgr, rid = ensure_test_runtime(lambda m: print(m, file=sys.stderr))
    return CACHE / "state" / "runtimes", rid


@pytest.fixture(scope="module")
def spark_exclusive(node_runtime: tuple[Path, str]) -> Iterator[None]:  # noqa: ARG001 - depends on node_runtime: the runtime is built BEFORE the lock is taken
    """Hold ``flock /tmp/olcf-spark.lock`` (the sandbox-wide JVM serialisation lock) for the duration of one test module.

    ``node_runtime`` is resolved first on purpose: building/syncing the runtime runs smoke tests under the very same lock, so
    taking the lock first would deadlock. Unlike the session-wide ``spark_lock`` of ``tests/conftest.py`` this releases the lock
    when the module is done so other builders' Spark tests are not blocked for the rest of the session.
    """
    from tests.spark_locking import spark_lock_held

    with spark_lock_held():
        yield


def job_spec(job_id: str, source: str, *, kind: str = "script", fmt: str = "py", runtime: str = FAKE_RT, attempt: int = 1, **over) -> dict:
    """A minimal valid job spec + the payload the controller's agent provider sends (``{"spec", "source"}``)."""
    import hashlib

    spec = {"schema": "olcf.jobspec/1", "job_id": job_id, "attempt": attempt, "kind": kind, "engine": "python", "name": job_id,
            "workspace": {"id": "wks_1", "name": "w"}, "source": {"format": fmt, "path": "source/main.py" if fmt == "py" else "source/notebook.ipynb"},
            "parameters": {}, "spark": {"master": "local[1]", "conf": {}}, "runtime": {"id": runtime, "family": "olcf-native"}, "limits": {"timeout_seconds": 120},
            "env": {}, "secrets": {}, "context": {}}
    spec.update(over)
    payload = {"spec": spec, "source": {"path": spec["source"]["path"], "encoding": "utf8", "content": source, "sha256": hashlib.sha256(source.encode()).hexdigest()}}
    return payload


def wait_final(ctl: FakeController, job_id: str, timeout: float = 90.0) -> dict:
    return ctl.wait_for(lambda: ctl.final_state(job_id), timeout, f"final state of {job_id}")


def pgrep(pattern: str) -> list[str]:
    out = subprocess.run(["pgrep", "-f", pattern], capture_output=True, text=True, check=False)  # noqa: S603, S607
    return [p for p in out.stdout.split() if p and int(p) != os.getpid()]


__all__ = ["AgentHandle", "FAKE_RT", "job_spec", "make_cfg", "make_ident", "pgrep", "shutil", "time", "wait_final"]
