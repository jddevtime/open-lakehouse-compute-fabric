# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""A stopped agent must not leave Spark Master / Worker JVMs behind (REAL agent process, REAL JVM daemons).

The agent runs as a separate ``python -m olcf.worker.main run`` process against the fake controller; the controller asks it to start a Spark Master
and a Spark Worker (real ``org.apache.spark.deploy`` JVMs from the cached runtime). Then the agent receives SIGTERM / SIGINT / a ``shutdown`` command:

* default (``keep_daemons_on_exit`` false): both JVMs - and everything in their process groups - are gone when the agent has exited;
* ``--keep-daemons-on-exit`` (rolling restarts): the JVMs survive and a second agent adopts them; they are then stopped explicitly.

Serialised with the other JVM tests through ``/tmp/olcf-spark.lock`` (fixture ``spark_exclusive``).
"""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import psutil
import pytest

from olcf.worker import identity as ident_mod
from olcf.worker import procutil
from olcf.worker.daemons import http_json
from tests.node.conftest import ROOT, make_ident
from tests.node.fake_controller import FakeController
from tests.node.runtime_fixture import CACHE

pytestmark = [pytest.mark.spark, pytest.mark.timeout(600)]

W = "wrk_sd"


class AgentProc:
    """One real agent process plus the state directory it owns."""

    def __init__(self, base: Path, runtimes: Path, ctl: FakeController, *extra: str) -> None:
        self.state = base / "state"
        self.state.mkdir(exist_ok=True)
        if not (self.state / "runtimes").exists():
            (self.state / "runtimes").symlink_to(runtimes, target_is_directory=True)
        ident_mod.save_identity(self.state, make_ident(ctl, W, "tok-sd", "node-sd"))
        env = {**os.environ, "PYTHONPATH": str(ROOT / "src")}
        env.pop("OLCF_WORKER_TOKEN", None)
        self.log = open(base / "agent.log", "ab")  # noqa: SIM115
        self.proc = subprocess.Popen(  # noqa: S603
            [sys.executable, "-m", "olcf.worker.main", "run", "--state-dir", str(self.state), "--max-cores", "4", "--max-memory-mb", "6000", "--daemon-memory", "256m",
             "--advertise-host", "127.0.0.1", "--runtime-trust-dir", str(CACHE / "trust"), "--log-level", "WARNING", *extra],
            env=env, stdout=self.log, stderr=self.log, cwd=str(ROOT))

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(60)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        self.log.close()


def _alive(pid: int) -> bool:
    try:
        return psutil.Process(pid).status() != psutil.STATUS_ZOMBIE
    except psutil.Error:
        return False


def _wait_gone(pids: list[int], timeout: float) -> list[int]:
    end = time.monotonic() + timeout
    while time.monotonic() < end and any(_alive(p) for p in pids):
        time.sleep(0.3)
    return [p for p in pids if _alive(p)]


def _kill_all(pids: list[int]) -> None:
    for pid in pids:
        if _alive(pid):
            procutil.terminate_tree(pid, grace=3.0, create_time=None)


@pytest.fixture()
def box(node_runtime, spark_exclusive, tmp_path):
    runtimes_dir, rid = node_runtime
    ctl = FakeController(tokens={"tok-sd": W}).start()
    started: list[int] = []
    agents: list[AgentProc] = []
    try:
        yield ctl, runtimes_dir, rid, tmp_path, started, agents
    finally:
        for a in agents:
            a.stop()
        _kill_all(started)  # last resort so a failing test never leaves JVMs on the shared sandbox
        ctl.stop(close_loop=True)
        shutil.rmtree(tmp_path, ignore_errors=True)
        left = _wait_gone(started, 20)
        assert not left, f"leaked daemon JVMs {left}"


def _start_cluster_daemons(ctl: FakeController, rid: str, started: list[int]) -> tuple[int, int]:
    port, ui = procutil.free_port(), procutil.free_port()
    m = ctl.ok(W, "start_spark_master", {"runtime_id": rid, "host": "127.0.0.1", "port": port, "ui_port": ui}, timeout=240)
    started.append(m["pid"])
    w = ctl.ok(W, "start_spark_worker", {"runtime_id": rid, "master_url": f"spark://127.0.0.1:{port}", "cores": 1, "memory_mb": 512, "host": "127.0.0.1"}, timeout=240)
    started.append(w["pid"])
    ctl.wait_for(lambda: [x for x in (http_json(f"http://127.0.0.1:{ui}/json/", 3) or {}).get("workers", []) if x["state"] == "ALIVE"], 60, "worker registered")
    assert all(_alive(p) for p in started)
    return m["pid"], w["pid"]


@pytest.mark.parametrize("how", ["SIGTERM", "SIGINT", "shutdown-command"])
def test_agent_exit_stops_the_spark_daemons_it_started(box, how):
    ctl, runtimes_dir, rid, tmp, started, agents = box
    agent = AgentProc(tmp, runtimes_dir, ctl)
    agents.append(agent)
    ctl.wait_connected(W, 90)
    master, worker = _start_cluster_daemons(ctl, rid, started)
    # the executors' parent (the Worker JVM) carries the node id, so jobs can tell which node an executor belongs to
    assert psutil.Process(worker).environ().get("OLCF_WORKER_ID") == W
    if how == "shutdown-command":
        assert ctl.ok(W, "shutdown", {})["stopping"] is True
    else:
        agent.proc.send_signal(getattr(signal, how))
    assert agent.proc.wait(90) == 0
    left = _wait_gone([master, worker], 30)
    assert not left, f"Spark daemons {left} outlived the agent that started them ({how})"
    java = [p.pid for p in psutil.process_iter(["name", "cmdline"]) if "java" in (p.info["name"] or "") and any(str(tmp) in c for c in p.info["cmdline"] or [])
            and p.status() != psutil.STATUS_ZOMBIE]
    assert not java, f"stray JVMs of this node: {java}"


def test_keep_daemons_on_exit_opt_out_for_rolling_restarts(box):
    ctl, runtimes_dir, rid, tmp, started, agents = box
    agent = AgentProc(tmp, runtimes_dir, ctl, "--keep-daemons-on-exit")
    agents.append(agent)
    ctl.wait_connected(W, 90)
    master, worker = _start_cluster_daemons(ctl, rid, started)
    agent.proc.send_signal(signal.SIGTERM)
    assert agent.proc.wait(90) == 0
    time.sleep(2)
    assert _alive(master) and _alive(worker), "with --keep-daemons-on-exit the JVMs must survive the agent"
    # the next agent adopts them (no relaunch: same pids) and its own SIGTERM - default policy - then stops them
    ctl.wait_for(lambda: not ctl.connected(W), 30, "old agent disconnected")
    agent2 = AgentProc(tmp, runtimes_dir, ctl)
    agents.append(agent2)
    ctl.wait_connected(W, 90)
    adopted = ctl.wait_for(lambda: {d["role"]: d["pid"] for d in (ctl.heartbeats.get(W) or {}).get("spark_daemons", []) if d.get("state") == "running"}, 60, "adopted daemons")
    assert adopted == {"master": master, "worker": worker}
    agent2.proc.send_signal(signal.SIGTERM)
    assert agent2.proc.wait(90) == 0
    assert not _wait_gone([master, worker], 30)
