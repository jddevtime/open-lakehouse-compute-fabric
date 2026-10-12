# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``POST /v1/compute/spark-smoke`` against a REAL standalone cluster: the production controller, TWO production worker agents and real Spark JVMs.

Topology (all on this machine, so both agents share the address ``127.0.0.1`` - the report must say so honestly)::

    control plane (uvicorn)  <--ws--  agent A  (Spark Master + Worker JVMs, executors)
                             <--ws--  agent B  (Spark Worker JVM, executors)

The smoke job goes through the normal job path (scheduler -> standalone provider -> runner with the runtime bootstrap), so the application's master must be the
``spark://`` URL of the standalone cluster and executors must be hosted by both agents. A regression to a script that builds its own ``SparkSession`` would show
up as ``local[*]`` and ``distributed: false``.

Serialised with every other JVM test through ``/tmp/olcf-spark.lock``; the cached runtime comes from ``tests/node/runtime_fixture.py``.
"""

from __future__ import annotations

import contextlib
import json
import os
import signal
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import psutil
import pytest

from tests.compute.fake_agent import wait_until
from tests.spark_locking import spark_lock_held

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

pytestmark = [pytest.mark.spark, pytest.mark.timeout(900)]


class RealAgent:
    """A production ``olcf.worker.main run`` process enrolled at the control plane, sharing the cached real runtime."""

    def __init__(self, st: Any, name: str, state_dir: Path, runtimes: Path, trust: Path) -> None:
        self.st, self.name, self.state_dir = st, name, state_dir
        state_dir.mkdir(parents=True)
        (state_dir / "runtimes").symlink_to(runtimes, target_is_directory=True)
        self.trust = trust
        self.log = state_dir.parent / f"{name}.log"
        self.proc: subprocess.Popen[bytes] | None = None

    def start(self) -> RealAgent:
        r = self.st.http.post("/v1/workers/enrollment-tokens", headers=self.st.admin, json={"name": self.name, "ttl_seconds": 600})
        assert r.status_code in (200, 201), r.text
        env = {k: v for k, v in os.environ.items() if k != "OLCF_WORKER_TOKEN"}
        env["PYTHONPATH"] = os.pathsep.join(os.path.abspath(p) for p in (env.get("PYTHONPATH") or str(ROOT / "src")).split(os.pathsep) if p)
        env["OLCF_WORKER_TOKEN"] = r.json()["token"]
        cmd = [sys.executable, "-m", "olcf.worker.main", "run", "--url", self.st.url, "--name", self.name, "--state-dir", str(self.state_dir), "--max-cores", "4",
               "--max-memory-mb", "3500", "--daemon-memory", "256m", "--advertise-host", "127.0.0.1", "--runtime-trust-dir", str(self.trust), "--log-level", "WARNING"]
        with open(self.log, "ab") as out:
            self.proc = subprocess.Popen(cmd, stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, env=env, start_new_session=True, cwd=str(self.state_dir.parent))  # noqa: S603
        return self

    def stop(self) -> None:
        p = self.proc
        if p is None or p.poll() is not None:
            return
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(p.pid, signal.SIGTERM)  # the agent stops the Spark daemons it started (default policy); the group kill also reaches stragglers
        try:
            p.wait(timeout=60)
        except subprocess.TimeoutExpired:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(p.pid, signal.SIGKILL)
            p.wait(timeout=10)


def _spark_jvms(scope: str) -> list[int]:
    out = []
    for p in psutil.process_iter(["name", "cmdline", "status"]):
        try:
            if p.info["status"] != psutil.STATUS_ZOMBIE and "java" in (p.info["name"] or "") and any(scope in c for c in p.info["cmdline"] or []):
                out.append(p.pid)
        except psutil.Error:
            continue
    return out


@pytest.fixture(scope="module")
def cluster2(tmp_path_factory: pytest.TempPathFactory) -> Iterator[tuple[Any, list[RealAgent]]]:
    from tests.compute.ctrl_stack import Stack
    from tests.node.runtime_fixture import CACHE, ensure_test_runtime

    mgr, rid = ensure_test_runtime(lambda m: print(m, file=sys.stderr))  # built BEFORE the lock is taken: building runs smoke tests under the same lock
    with spark_lock_held():
        base = tmp_path_factory.mktemp("smoke-real")
        st = Stack(base / "stack", "sqlite").start()
        agents: list[RealAgent] = []
        try:
            assert st.http.put("/v1/admin/settings/compute.allow_unsigned_runtimes", headers=st.admin, json={"value": True}).status_code == 200
            # this machine may have only 2 CPUs: without this each agent would keep one core for itself and offer 1 usable core, too few for a Spark Standalone node
            # (this is also the live-setting path of FIX item B: the planner reads the value without a restart)
            assert st.http.put("/v1/admin/settings/compute.reserve_cores", headers=st.admin, json={"value": 0}).status_code == 200
            manifest = json.loads((CACHE / "state" / "runtimes" / rid / "manifest.json").read_text())
            manifest.pop("signature", None)
            r = st.http.post("/v1/runtimes", headers=st.admin, json={"manifest": manifest})
            assert r.status_code == 201, r.text
            for name in ("smoke-a", "smoke-b"):
                agents.append(RealAgent(st, name, base / name / "state", CACHE / "state" / "runtimes", CACHE / "trust").start())
            for a in agents:
                wait_until(lambda a=a: next((w for w in st.http.get("/v1/workers", headers=st.admin).json()["items"] if w["name"] == a.name and w["state"] == "online"), None),
                           120, 0.5, f"{a.name} to come online")
            wait_until(lambda: len(infos := st.compute.registry.node_infos()) == 2 and all(rid in n.ready_runtimes for n in infos), 120, 0.5,
                       "both agents to report the runtime ready")
            st.runtime_id = rid
            yield st, agents
        finally:
            for a in agents:
                a.stop()
            st.stop()
            wait_until(lambda: not _spark_jvms(str(base)), 60, 0.5, "all Spark JVMs of this module to exit")


def _smoke(st: Any, body: dict[str, Any]) -> dict[str, Any]:
    """Start the smoke test (202 + poll URL) and poll it to the end."""
    r = st.http.post("/v1/compute/spark-smoke", headers=st.admin, json=body)
    assert r.status_code == 202, r.text
    doc = r.json()
    end = time.monotonic() + float(body.get("timeout_seconds", 240)) + 60
    while not doc["finished"] and time.monotonic() < end:
        time.sleep(1.0)
        doc = st.http.get(doc["poll_url"], headers=st.admin).json()
    assert doc["finished"], doc
    return doc


def test_smoke_runs_on_the_standalone_cluster_with_executors_on_both_agents(cluster2: tuple[Any, list[RealAgent]]) -> None:
    st, agents = cluster2
    d = _smoke(st, {"runtime_id": st.runtime_id, "executors": 2, "partitions": 4, "timeout_seconds": 120})
    if not (d["ok"] is True and d["state"] == "succeeded"):
        pytest.fail("smoke job did not succeed: " + json.dumps(d, default=str, indent=1))
    # through the job path: the master is the standalone cluster, never local[*]
    if not d["master_is_cluster"]:  # name the planner's reason in the failure
        tl = st.http.get(f"/v1/jobs/{d['job_id']}/timeline", headers=st.admin).json()["items"]
        pytest.fail("the smoke job did not run on the standalone cluster: " + json.dumps({"result": d, "timeline": [{k: v for k, v in e.items() if v not in (None, {}, "")} for e in tl]}, default=str, indent=1)[:6000])
    assert d["master"].startswith("spark://") and d["master_is_cluster"] is True and d["provider"] == "standalone", d
    assert d["sum"] == 499500
    assert d["total_cores"] >= 2 and d["partitions"] >= 2 * d["total_cores"] and d["tasks"] == d["partitions"]  # mapPartitions, at least two waves per core
    # per-node participation: BOTH agents hosted executors that ran tasks
    names = {n["name"] for n in d["nodes_with_executors"]}
    assert names == {"smoke-a", "smoke-b"}, d["nodes_with_executors"]
    for n in d["nodes_with_executors"]:
        assert n["tasks"] and n["executors"], n
    assert sum(n["tasks"] for n in d["nodes_with_executors"]) == d["tasks"]
    assert d["distributed"] is True
    # honesty: both agents run on this machine and share 127.0.0.1 -> the report says so instead of claiming multi-machine distribution
    assert d["single_address"] is True and "share the address 127.0.0.1" in d["note"]
    # executors report the node id they belong to (agents on one host cannot be told apart by host name)
    assert {e["node"] for e in d["executors"]} == {n["worker_id"] for n in d["nodes_with_executors"]}


def test_a_script_that_builds_its_own_session_is_not_distributed(cluster2: tuple[Any, list[RealAgent]]) -> None:
    """The defect that was reported: a job whose script builds its own ``SparkSession`` gets ``local[*]`` and must never be reported as distributed."""
    from olcf.compute import topology

    st, _ = cluster2
    legacy = topology.SMOKE_SCRIPT.replace("sc = spark.sparkContext", 'from pyspark.sql import SparkSession\nspark = SparkSession.builder.appName("legacy").getOrCreate()\nsc = spark.sparkContext')
    orig = topology.SMOKE_SCRIPT
    topology.SMOKE_SCRIPT = legacy
    try:
        d = _smoke(st, {"runtime_id": st.runtime_id, "executors": 2, "timeout_seconds": 300})
    finally:
        topology.SMOKE_SCRIPT = orig
    assert d["state"] == "succeeded" and d["master_is_cluster"] is False and d["distributed"] is False, d
    assert "no Spark standalone cluster was used" in d["note"]
