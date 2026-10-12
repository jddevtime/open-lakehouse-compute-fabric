# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""REAL Spark integration: two worker agents form a standalone cluster and run real jobs (JVMs, sockets, kernels).

Topology (all on this machine, driven by a fake controller that speaks the hub protocol)::

    FakeController  <--ws--  agent A (wrk_a): Spark Master (:free) + Worker 1 (1 core / 512 MB) + job drivers / kernels
                    <--ws--  agent B (wrk_b): Spark Worker 2 (1 core / 512 MB)

Everything is the production code path: the runtime is the cached real pyspark 4.1.1 venv installed by the production
``RuntimeManager``; the daemons are the real ``org.apache.spark.deploy.{master,worker}`` JVMs started through ``spark-class``;
jobs are real ``olcf.runtime_lib.runner`` processes (PySpark driver JVM + executor JVMs on the workers); notebooks run in a real
ipykernel. Resource discipline: every JVM heap is 256-512 MB, the module holds ``flock /tmp/olcf-spark.lock`` while it runs and
tears everything down (verified with ``pgrep``) at the end.

Not covered here (no Delta jars / network in the sandbox): Delta Lake reads/writes -> see ``scripts/verify_spark_delta.py``.
"""

from __future__ import annotations

import json
import shutil
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import psutil
import pytest

from olcf.worker.daemons import http_json
from tests.node.conftest import AgentHandle, job_spec, make_cfg, make_ident, wait_final
from tests.node.fake_controller import FakeController
from tests.node.runtime_fixture import CACHE

pytestmark = [pytest.mark.spark, pytest.mark.timeout(900)]

A, B = "wrk_a", "wrk_b"
SPARK_CONF = {
    "spark.driver.memory": "512m", "spark.executor.memory": "512m", "spark.executor.cores": "1", "spark.cores.max": "2",
    "spark.ui.enabled": "false", "spark.sql.shuffle.partitions": "4", "spark.default.parallelism": "4", "spark.network.timeout": "120s",
}


def _driver_jvms() -> list[int]:
    """Pids of live driver JVMs (a JVM may vanish between listing and inspection, hence the guard)."""
    out = []
    for p in _java_procs():
        try:
            if any("SparkSubmit" in c for c in p.cmdline()):
                out.append(p.pid)
        except psutil.Error:
            continue
    return out


@dataclass
class Cluster:
    ctl: FakeController
    a: AgentHandle
    b: AgentHandle
    rid: str
    master_url: str
    master_ui: str
    worker_ui: dict[str, str] = field(default_factory=dict)

    def spec(self, job_id: str, src: str, *, kind: str = "spark_job", fmt: str = "py", conf: dict | None = None, **over) -> dict:
        spark = {"master": self.master_url, "driver_host": "127.0.0.1", "conf": {**SPARK_CONF, **(conf or {})}}
        over.setdefault("spark", spark)
        return job_spec(job_id, src, kind=kind, fmt=fmt, runtime=self.rid, engine="pyspark", **over)

    def run(self, job_id: str, src: str, agent: str = A, **kw) -> dict:
        return self.ctl.ok(agent, "run_job", self.spec(job_id, src, **kw))

    def master_json(self) -> dict:
        return http_json(self.master_ui + "/json/", timeout=3) or {}

    def wait_idle(self, timeout: float = 60.0) -> None:
        """No application left on the master (executors released), so the next test starts from a clean cluster."""
        self.ctl.wait_for(lambda: self.master_json().get("activeapps") == [] and all(w.get("coresused") == 0 for w in self.master_json().get("workers", [])),
                          timeout, "cluster to become idle")

    def wait_app(self, job_id: str, timeout: float = 120.0) -> dict:
        self.ctl.wait_for(lambda: self.ctl.events_named("spark_app_started", job_id), timeout, f"spark_app_started of {job_id}")
        return self.ctl.events_named("spark_app_started", job_id)[0]


#: Directories of the running ``cluster`` fixture. Process scans below only look at processes that belong to THIS test (their command line, working directory or
#: environment names the fixture's directory): the sandbox is shared, other test runs have their own JVMs, which this module must neither count nor kill.
_SCOPE: list[str] = []


def _in_scope(p: psutil.Process) -> bool:
    if not _SCOPE:
        return True
    try:
        hay = [" ".join(p.cmdline()), p.cwd(), *p.environ().values()]
    except psutil.Error:
        return False
    return any(sc in h for sc in _SCOPE for h in hay)


def _java_procs() -> list[psutil.Process]:
    out = []
    for p in psutil.process_iter(["name", "cmdline", "status"]):
        try:
            # a zombie is a dead process whose parent (this test process, for daemons started by an agent instance we stopped on purpose)
            # has not reaped it yet - not a leak
            if p.info["status"] != psutil.STATUS_ZOMBIE and "java" in (p.info["name"] or "") and any("org.apache.spark" in c for c in p.info["cmdline"] or []) and _in_scope(p):
                out.append(p)
        except psutil.Error:
            continue
    return out


def _scoped(needle: str) -> list[int]:
    """Pids of this test's processes whose command line contains ``needle``."""
    out = []
    for p in psutil.process_iter(["cmdline", "status"]):
        try:
            if p.info["status"] != psutil.STATUS_ZOMBIE and any(needle in c for c in p.info["cmdline"] or []) and _in_scope(p):
                out.append(p.pid)
        except psutil.Error:
            continue
    return out


@pytest.fixture(scope="module")
def cluster(node_runtime, spark_exclusive, tmp_path_factory):
    runtimes_dir, rid = node_runtime
    base = tmp_path_factory.mktemp("spark-cluster")
    _SCOPE.append(str(base))
    ctl = FakeController(tokens={"tok-a": A, "tok-b": B}).start()
    handles: list[AgentHandle] = []
    cl: Cluster | None = None
    try:
        for name, wid, tok in (("a", A, "tok-a"), ("b", B, "tok-b")):
            sd = base / name
            sd.mkdir()
            (sd / "runtimes").symlink_to(runtimes_dir, target_is_directory=True)  # share the cached, verified runtime
            cfg = make_cfg(sd, max_cores=4, max_memory_mb=6000, daemon_memory="256m", ready_timeout_seconds=180.0, runtime_trust_dir=CACHE / "trust",
                           advertise_host="127.0.0.1", max_jobs=2, kill_grace_seconds=5.0)
            handles.append(AgentHandle(cfg, make_ident(ctl, wid, tok, f"node-{name}")).start())
        ctl.wait_connected(A, 60)
        ctl.wait_connected(B, 60)
        from olcf.worker.procutil import free_port

        port, ui = free_port(), free_port()
        m = ctl.ok(A, "start_spark_master", {"runtime_id": rid, "host": "127.0.0.1", "port": port, "ui_port": ui}, timeout=240)
        assert m["state"] == "running", m
        murl = f"spark://127.0.0.1:{port}"
        cl = Cluster(ctl, handles[0], handles[1], rid, murl, f"http://127.0.0.1:{ui}")
        for wid in (A, B):
            w = ctl.ok(wid, "start_spark_worker", {"runtime_id": rid, "master_url": murl, "cores": 1, "memory_mb": 512, "host": "127.0.0.1"}, timeout=240)
            assert w["state"] == "running", w
            cl.worker_ui[wid] = w["ui_url"]
        ctl.wait_for(lambda: len([w for w in cl.master_json().get("workers", []) if w["state"] == "ALIVE"]) == 2, 90, "both workers registered with the master")
        yield cl
    finally:
        # cl.a / cl.b, not `handles`: test_d2 replaces agent B by a new instance that adopted the Worker JVM, and that one must be stopped (and its daemons killed)
        for h in {id(x): x for x in [*handles, *( [cl.a, cl.b] if cl is not None else [] )]}.values():
            try:
                h.stop(kill_children=True, timeout=120)
            except Exception as exc:  # noqa: BLE001
                print(f"agent teardown problem: {exc}", file=sys.stderr)
        ctl.stop(close_loop=True)
        shutil.rmtree(base, ignore_errors=True)
        end = time.monotonic() + 30
        while _java_procs() and time.monotonic() < end:
            time.sleep(0.5)
        left = _java_procs()
        _SCOPE.clear()
        desc = []
        for p in left:
            try:
                desc.append((p.pid, next((c for c in p.cmdline() if "deploy." in c), "?"), p.ppid()))
            except psutil.Error:
                desc.append((p.pid, "gone", None))
        for p in left:  # last resort so a failed run never leaves JVMs behind on the shared box
            p.kill()
        assert not left, f"leaked Spark JVMs (pid, main class, ppid): {desc}"


# --------------------------------------------------------------------------------------------- cluster formation
def test_cluster_is_up_and_reported(cluster):
    mj = cluster.master_json()
    assert mj["status"] == "ALIVE" and len(mj["workers"]) == 2
    assert {w["cores"] for w in mj["workers"]} == {1} and all(w["memory"] == 512 for w in mj["workers"])
    # each agent reports its own daemons in the heartbeat; A hosts master + worker, B only a worker
    def daemons_of(wid):
        return lambda: {(d["role"], d["state"]) for d in (cluster.ctl.heartbeats.get(wid) or {}).get("spark_daemons", [])}

    cluster.ctl.wait_for(lambda: daemons_of(A)() == {("master", "running"), ("worker", "running")}, 30, "agent A heartbeat to show master+worker running")
    cluster.ctl.wait_for(lambda: daemons_of(B)() == {("worker", "running")}, 30, "agent B heartbeat to show its worker running")
    hb_a = cluster.ctl.heartbeats[A]
    assert {c["name"].split(":")[0] for c in hb_a["components"]} >= {"spark-master", "spark-worker"}
    assert hb_a["allocated"]["cores"] == 1 and hb_a["allocated"]["memory_mb"] == 512  # worker offer is carved out of the node
    # daemons are real JVMs with bounded heaps
    jv = _java_procs()
    assert len(jv) == 3
    for p in jv:
        assert "-Xmx256m" in p.cmdline() or any(c.startswith("-Xmx") for c in p.cmdline()), p.cmdline()[:8]
    # states were announced as frames
    states = {(f["role"], f["state"]) for f in cluster.ctl.daemon_states}
    assert {("master", "running"), ("worker", "running")} <= states


# --------------------------------------------------------------------------------------------- (a) script job with shuffle
def test_a_script_job_with_shuffle_and_exit_value(cluster):
    src = """
from pyspark.sql import functions as F
df = spark.range(0, 40000, 1, 4).withColumn("k", F.col("id") % 7)
rows = df.groupBy("k").agg(F.count("*").alias("c"), F.sum("id").alias("s")).orderBy("k").collect()
print("groups", len(rows), flush=True)
EXIT_VALUE = {"groups": len(rows), "count": sum(r["c"] for r in rows), "sum": sum(r["s"] for r in rows), "master": spark.sparkContext.master}
"""
    cluster.run("a1", src)
    fin = wait_final(cluster.ctl, "a1", 300)
    assert fin["state"] == "succeeded", fin.get("failure")
    ev = fin["result"]["exit_value"]
    assert ev["groups"] == 7 and ev["count"] == 40000 and ev["sum"] == sum(range(40000)) and ev["master"] == cluster.master_url
    assert fin["exit_code"] == 0 and fin["spark"]["application_id"].startswith("app-") and fin["spark"]["master"] == cluster.master_url
    cluster.ctl.wait_for(lambda: "groups 7" in cluster.ctl.log_text("a1"), 30, "driver output in the shipped log")
    app = cluster.ctl.events_named("spark_app_started", "a1")
    assert app and app[0]["data"]["application_id"] == fin["spark"]["application_id"]
    cluster.wait_idle()
    # the finished application is recorded by the master
    done = [x for x in cluster.master_json().get("completedapps", []) if x["id"] == fin["spark"]["application_id"]]
    assert done and done[0]["state"] in ("FINISHED", "KILLED")


def test_a2_failing_spark_job_is_classified(cluster):
    src = "spark.sql('select * from definitely_missing_table_xyz').collect()\n"
    cluster.run("a2", src, conf={"spark.cores.max": "1"})
    fin = wait_final(cluster.ctl, "a2", 300)
    assert fin["state"] == "failed" and fin["failure"]["category"] == "catalog", fin["failure"]
    assert "definitely_missing_table_xyz" in fin["failure"]["message"] + (fin["failure"].get("traceback") or "")
    cluster.wait_idle()


# --------------------------------------------------------------------------------------------- (e) executors on BOTH agents
def test_e_executors_run_on_both_agents(cluster):
    src = """
import time
from pyspark.sql import functions as F
df = spark.range(0, 100000, 1, 4).withColumn("k", F.col("id") % 5)
n = df.groupBy("k").count().count()
print("computed", n, flush=True)
time.sleep(12)   # keep the application (and its executors) alive so the test can inspect the cluster
EXIT_VALUE = n
"""
    cluster.run("e1", src)
    cluster.wait_app("e1")

    def both_busy():
        ws = cluster.master_json().get("workers", [])
        return len(ws) == 2 and all(w["coresused"] == 1 for w in ws)

    cluster.ctl.wait_for(both_busy, 120, "one executor core in use on EACH of the two workers")
    mj = cluster.master_json()
    app = mj["activeapps"][0]
    assert app["cores"] == 2 and app["state"] == "RUNNING"
    # ask each worker's own web UI which executors it hosts: one per agent, both belonging to the same application
    per_worker = {}
    for wid, ui in cluster.worker_ui.items():
        doc = cluster.ctl.wait_for(lambda ui=ui: (http_json(ui + "/json/", 3) or {}).get("executors"), 60, f"executors on {wid}")
        per_worker[wid] = doc
        assert len(doc) == 1 and doc[0]["appid"] == app["id"] and doc[0]["memory"] == 512
    assert per_worker[A][0]["appid"] == per_worker[B][0]["appid"] == app["id"]  # one application, executors on two different agents
    # the executors really are JVMs below the right agent's worker daemon (process tree of each agent)
    execs = [p for p in psutil.process_iter(["cmdline"]) if any("CoarseGrainedExecutorBackend" in c for c in (p.info["cmdline"] or []))]
    assert len(execs) == 2
    for p in execs:
        assert any(c.startswith("-Xmx512") or c == "-Xmx512M" for c in p.cmdline()), p.cmdline()[:12]
    parents = set()
    for p in execs:
        anc = {a.pid for a in p.parents()}
        for wid, h in ((A, cluster.a), (B, cluster.b)):
            w = [d for d in h.agent.daemons.snapshot() if d["role"] == "worker"][0]
            if w["pid"] in anc:
                parents.add(wid)
    assert parents == {A, B}, "executors must descend from the Worker daemons of BOTH agents"
    fin = wait_final(cluster.ctl, "e1", 300)
    assert fin["state"] == "succeeded" and fin["result"]["exit_value"] == 5
    cluster.wait_idle()
    assert all(w["coresused"] == 0 for w in cluster.master_json()["workers"])


# --------------------------------------------------------------------------------------------- (b) notebook job in a real kernel
NOTEBOOK = {"nbformat": 4, "nbformat_minor": 5, "metadata": {"kernelspec": {"name": "python3", "display_name": "Python 3", "language": "python"}}, "cells": [
    {"cell_type": "markdown", "metadata": {}, "source": "# demo"},
    {"cell_type": "code", "metadata": {"tags": ["parameters"]}, "execution_count": None, "outputs": [], "source": "n = 3\nlabel = 'default'\n"},
    {"cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [],
     "source": "from pyspark.sql import functions as F\nres = spark.range(n * 1000).withColumn('k', F.col('id') % 5).groupBy('k').count().collect()\nprint(sorted(r['count'] for r in res))\ndisplay(spark.range(3))\n"},
    {"cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [],
     "source": "print('label=' + label, 'n=' + str(n))\nnotebookutils.notebook.exit({'label': label, 'n': n, 'total': sum(r['count'] for r in res)})\nprint('never printed')\n"},
]}


def test_b_notebook_job_with_parameters_cell_in_a_real_kernel(cluster):
    cluster.run("b1", json.dumps(NOTEBOOK), kind="notebook", fmt="ipynb", parameters={"n": 4, "label": "from-test"}, conf={"spark.cores.max": "1"})
    fin = wait_final(cluster.ctl, "b1", 400)
    assert fin["state"] == "succeeded", fin.get("failure")
    assert fin["result"]["exit_value"] == {"label": "from-test", "n": 4, "total": 4000}  # the injected parameters overrode the defaults
    arts = {a["name"]: a for a in fin["result"]["artifacts"]}
    assert "output.ipynb" in arts and arts["output.ipynb"].get("data_b64"), arts.keys()
    import base64

    out = json.loads(base64.b64decode(arts["output.ipynb"]["data_b64"]))
    tags = [c["metadata"].get("tags") for c in out["cells"] if c["cell_type"] == "code"]
    assert ["injected-parameters"] in tags  # papermill-compatible injection after the parameters cell
    code_cells = [c for c in out["cells"] if c["cell_type"] == "code"]
    text = "".join(o.get("text", "") for c in code_cells for o in c["outputs"] if o["output_type"] == "stream")
    assert "[800, 800, 800, 800, 800]" in text and "label=from-test n=4" in text and "never printed" not in text
    assert any(o["output_type"] == "display_data" for o in code_cells[2]["outputs"])  # display(df) produced rich output
    assert fin["result"]["metrics"]["exit_called"] is True
    cluster.ctl.wait_for(lambda: cluster.ctl.events_named("spark_app_started", "b1"), 10, "spark app event from the kernel")
    cluster.wait_idle()


def test_b2_notebook_failure_reports_cell_and_category(cluster):
    nb = json.loads(json.dumps(NOTEBOOK))
    nb["cells"][2]["source"] = "spark.sql('select * from nope_missing_tbl').show()\n"
    cluster.run("b2", json.dumps(nb), kind="notebook", fmt="ipynb", spark={"master": "local[1]", "conf": {"spark.driver.memory": "512m", "spark.ui.enabled": "false"}})
    fin = wait_final(cluster.ctl, "b2", 300)
    assert fin["state"] == "failed"
    f = fin["failure"]
    assert f["category"] == "catalog" and f["cell"] == 2, f  # index in the ORIGINAL notebook (injected cells do not shift it)
    assert "nope_missing_tbl" in (f["message"] + (f.get("traceback") or ""))


# --------------------------------------------------------------------------------------------- (c) cancel and timeout
BUSY = "spark.range(0, 10**12, 1, 2).selectExpr('max(id % 1000)').collect()\n"  # long scan; max() cannot overflow (ANSI sum() does)


def test_c_cancel_running_spark_job_releases_the_cluster(cluster):
    cluster.run("c1", BUSY)
    cluster.wait_app("c1")
    cluster.ctl.wait_for(lambda: cluster.master_json().get("activeapps"), 60, "application registered with the master")
    assert _driver_jvms() and _scoped("CoarseGrainedExecutorBackend"), "the process scans must see this test's own driver and executor JVMs (else the checks below prove nothing)"
    pid = cluster.ctl.wait_for(lambda: next((j["pid"] for j in (cluster.ctl.heartbeats.get(A) or {}).get("active_jobs", []) if j["job_id"] == "c1"), None), 20)
    t0 = time.monotonic()
    res = cluster.ctl.ok(A, "cancel_job", {"job_id": "c1", "grace_seconds": 20})
    assert res["found"] and res["state"] == "cancelling"
    fin = wait_final(cluster.ctl, "c1", 120)
    assert fin["state"] == "cancelled" and fin["failure"]["category"] == "cancellation", fin
    assert time.monotonic() - t0 < 90
    assert not psutil.pid_exists(pid) or psutil.Process(pid).status() == psutil.STATUS_ZOMBIE
    cluster.wait_idle(90)  # the application was unregistered and its executors released
    cluster.ctl.wait_for(lambda: not _scoped("CoarseGrainedExecutorBackend"), 60, "executor JVMs to exit")
    # the driver JVM (child of the runner) is gone too
    assert not _driver_jvms()


def test_c2_timeout_of_a_spark_job(cluster):
    cluster.run("c2", BUSY, limits={"timeout_seconds": 45})
    cluster.wait_app("c2")
    fin = wait_final(cluster.ctl, "c2", 200)
    assert fin["state"] == "timed_out" and fin["failure"]["category"] == "timeout", json.dumps(fin, default=str)[:3000]
    cluster.wait_idle(90)
    assert not _driver_jvms()


def test_c3_force_cancel_kills_driver_jvm(cluster):
    cluster.run("c3", BUSY)
    cluster.wait_app("c3")
    cluster.ctl.ok(A, "cancel_job", {"job_id": "c3", "force": True})
    fin = wait_final(cluster.ctl, "c3", 60)
    assert fin["state"] == "cancelled"
    # after SIGKILL of the whole group the master notices the lost driver and frees the executors
    cluster.wait_idle(120)
    assert not _driver_jvms()


# --------------------------------------------------------------------------------------------- (d) reconnect + reconcile
def test_d_controller_restart_reconciles_jobs_and_daemons(cluster):
    src = """
import time
n = spark.range(1000).count()
print("before-restart", n, flush=True)
time.sleep(25)
print("after-restart", flush=True)
EXIT_VALUE = {"n": n}
"""
    ctl = cluster.ctl
    cluster.run("d1", src, conf={"spark.cores.max": "1"})
    cluster.wait_app("d1")
    ctl.wait_for(lambda: "before-restart" in ctl.log_text("d1"), 120, "output before the restart")
    pid = next(j["pid"] for j in ctl.heartbeats[A]["active_jobs"] if j["job_id"] == "d1")
    hellos = {A: len([h for w, h in ctl.hellos if w == A]), B: len([h for w, h in ctl.hellos if w == B])}
    ctl.restart()  # the controller process dies and comes back on the same port
    for wid in (A, B):
        ctl.wait_for(lambda wid=wid: len([h for w, h in ctl.hellos if w == wid]) > hellos[wid] and ctl.connected(wid), 60, f"{wid} to reconnect")
    ha = [h for w, h in ctl.hellos if w == A][-1]
    hb = [h for w, h in ctl.hellos if w == B][-1]
    # hello reconcile: the running job (same pid) and every daemon are reported exactly as they are
    job = next(j for j in ha["active_jobs"] if j["job_id"] == "d1")
    assert job["pid"] == pid and job["state"] == "running"
    assert {(d["role"], d["state"]) for d in ha["spark_daemons"]} == {("master", "running"), ("worker", "running")}
    assert {(d["role"], d["state"]) for d in hb["spark_daemons"]} == {("worker", "running")}
    assert any(d["role"] == "master" and d["url"] == cluster.master_url for d in ha["spark_daemons"])
    assert hb["active_jobs"] == [] and ha["runtimes"][0]["id"] == cluster.rid and ha["runtimes"][0]["state"] == "ready"
    assert ha["allocated"]["cores"] >= 1  # worker offer + the job driver
    # nothing was lost: the job finishes, its output is complete and ordered, events are not duplicated
    fin = wait_final(ctl, "d1", 300)
    assert fin["state"] == "succeeded" and fin["result"]["exit_value"] == {"n": 1000}
    ctl.wait_for(lambda: "after-restart" in ctl.log_text("d1"), 30, "output after the restart")
    text = ctl.log_text("d1")
    assert text.index("before-restart") < text.index("after-restart") and text.count("before-restart") == 1
    seqs = ctl.log_seqs("d1")
    assert seqs == list(range(len(seqs)))
    keyed = {(e["event"], e.get("seq")) for e in ctl.job_events if e["job_id"] == "d1"}
    assert ("spark_app_started", next(e["seq"] for e in ctl.events_named("spark_app_started", "d1"))) in keyed  # dedupe id present on every re-delivery
    cluster.wait_idle()


def test_d2_agent_restart_keeps_cluster_and_adopts_daemons(cluster):
    """Restart agent B (the Worker-only node): its Spark Worker JVM keeps running and is re-adopted, no re-registration needed."""
    ctl = cluster.ctl
    before = next(d for d in cluster.b.agent.daemons.snapshot() if d["role"] == "worker")
    ids_before = {w["id"] for w in cluster.master_json()["workers"]}
    cluster.b.stop(kill_children=False, timeout=120)
    assert psutil.pid_exists(before["pid"])
    n = len([1 for w, _ in ctl.hellos if w == B])
    cfg = cluster.b.cfg
    new = AgentHandle(cfg, make_ident(ctl, B, "tok-b", "node-b")).start()
    cluster.b = new
    ctl.wait_for(lambda: len([1 for w, _ in ctl.hellos if w == B]) > n and ctl.connected(B), 90, "agent B to reconnect")
    hello = [h for w, h in ctl.hellos if w == B][-1]
    worker = next(d for d in hello["spark_daemons"] if d["role"] == "worker")
    assert worker["pid"] == before["pid"] and worker["state"] == "running"
    assert {w["id"] for w in cluster.master_json()["workers"]} == ids_before  # same worker ids: no churn in the cluster
    # and the adopted daemon is controllable again
    cluster.wait_idle()


# --------------------------------------------------------------------------------------------- (f) probes against the live cluster
def test_f_probe_run(cluster):
    ctl = cluster.ctl
    checks = [
        {"kind": "spark_master", "url": cluster.master_ui + "/json/", "name": "master-ui"},
        {"kind": "spark_master", "master_url": cluster.master_url, "name": "master-rpc"},
        {"kind": "tcp", "host": "127.0.0.1", "port": int(cluster.master_url.rsplit(":", 1)[1]), "name": "master-tcp"},
        {"kind": "runtime", "runtime_id": cluster.rid, "name": "runtime"},
        {"kind": "disk", "name": "disk"},
        {"kind": "clock", "name": "clock"},
        {"kind": "storage", "uri": str(Path(cluster.a.cfg.state_dir) / "work"), "write": True, "name": "work-dir"},
    ]
    res = ctl.ok(A, "probe", {"checks": checks}, timeout=120)
    by = {c["name"]: c for c in res["checks"]}
    assert res["ok"], {k: v["detail"] for k, v in by.items() if not v["ok"]}
    assert by["master-ui"]["data"]["status"] == "ALIVE" and "2 worker(s)" in by["master-ui"]["detail"]
    assert by["runtime"]["data"]["fingerprint"]
    # from node B the master is reachable too (cluster networking), and a dead port is reported precisely
    res_b = ctl.ok(B, "probe", {"checks": [{"kind": "spark_master", "master_url": cluster.master_url, "name": "m"}, {"kind": "tcp", "host": "127.0.0.1", "port": 9, "timeout": 1, "name": "dead"}]})
    assert [c["ok"] for c in res_b["checks"]] == [True, False] and res_b["ok"] is False
    # the default probe (no checks) also runs everything applicable to this node, including the installed runtime
    default = ctl.ok(A, "probe", {}, timeout=120)
    assert {"runtime:" + cluster.rid, "disk", "clock"} <= {c["name"] for c in default["checks"]}


# --------------------------------------------------------------------------------------------- interactive kernel
def test_interactive_kernel_session(cluster):
    ctl = cluster.ctl
    spec = cluster.spec("kern1", "", kind="notebook", fmt="ipynb", spark={"master": "local[1]", "conf": {"spark.driver.memory": "512m", "spark.ui.enabled": "false"}},
                        secrets={"K_SECRET": "kernel-secret-value-42"})["spec"]
    sid = "sess-1"
    info = ctl.ok(A, "start_kernel", {"session_id": sid, "spec": spec, "eager_spark": False}, timeout=180)
    assert info["session_id"] == sid and info["pid"]
    ctl.wait_for(lambda: any(e["channel"] == "status" and e["content"].get("state") == "idle" for e in ctl.kernel_events if e["session_id"] == sid), 120, "kernel idle")

    def run(code, msg_id, timeout=240):
        ctl.ok(A, "kernel_execute", {"session_id": sid, "code": code, "msg_id": msg_id})
        return ctl.wait_for(lambda: next((e for e in ctl.kernel_events if e["channel"] == "shell" and e["msg_type"] == "execute_reply" and e["parent_msg_id"] == msg_id), None),
                            timeout, f"execute_reply of {msg_id}")

    def outputs(msg_id):
        return [e for e in ctl.kernel_events if e["parent_msg_id"] == msg_id and e["channel"] == "iopub"]

    r = run("x = 21 * 2\nx", "m1")
    assert r["content"]["status"] == "ok"
    res = [e for e in outputs("m1") if e["msg_type"] == "execute_result"]
    assert res and res[0]["content"]["data"]["text/plain"] == "42"
    r = run("print('secret is', __import__('os').environ['K_SECRET'])", "m2")
    streams = "".join(e["content"]["text"] for e in outputs("m2") if e["msg_type"] == "stream")
    assert "kernel-secret-value-42" not in streams and "secret is" in streams  # scrubbed on the node before it leaves
    r = run("spark.range(10).selectExpr('sum(id) as s').collect()[0]['s']", "m3")
    assert r["content"]["status"] == "ok"
    assert [e for e in outputs("m3") if e["msg_type"] == "execute_result"][0]["content"]["data"]["text/plain"] == "45"
    sp = [e for e in ctl.kernel_events if e["session_id"] == sid and e["channel"] == "status" and e["msg_type"] == "spark"]
    assert sp and sp[0]["content"]["application_id"].startswith(("local-", "app-"))
    r = run("%%sql\nselect 1 as one, 'a' as b", "m4")
    assert r["content"]["status"] == "ok" and any(e["msg_type"] in ("display_data", "execute_result") for e in outputs("m4"))
    r = run("1/0", "m5")
    assert r["content"]["status"] == "error" and r["content"]["ename"] == "ZeroDivisionError"
    comp = ctl.ok(A, "kernel_complete", {"session_id": sid, "code": "spark.rang", "cursor": 10})
    assert comp["status"] == "ok"
    ctl.ok(A, "kernel_execute", {"session_id": sid, "code": "import time\nprint('sleeping', flush=True)\ntime.sleep(120)", "msg_id": "m6"})
    ctl.wait_for(lambda: any(e["parent_msg_id"] == "m6" and e["msg_type"] == "stream" and "sleeping" in e["content"]["text"] for e in ctl.kernel_events), 30, "cell to be running")
    ctl.ok(A, "kernel_interrupt", {"session_id": sid})
    r6 = ctl.wait_for(lambda: next((e for e in ctl.kernel_events if e["msg_type"] == "execute_reply" and e["parent_msg_id"] == "m6"), None), 60, "interrupted cell reply")
    assert r6["content"]["status"] == "error" and r6["content"]["ename"] == "KeyboardInterrupt"
    hb = ctl.wait_for(lambda: next((h for h in [ctl.heartbeats.get(A)] if h and any(k["session_id"] == sid for k in h.get("kernels", []))), None), 15)
    assert hb
    kpid = info["pid"]
    assert ctl.ok(A, "kernel_shutdown", {"session_id": sid})["state"] == "dead"
    ctl.wait_for(lambda: not psutil.pid_exists(kpid) or psutil.Process(kpid).status() == psutil.STATUS_ZOMBIE, 30, "kernel process to exit")
    ctl.wait_for(lambda: not _driver_jvms(), 30, "the kernel's driver JVM to exit with it")
    assert not (Path(cluster.a.cfg.state_dir) / "kernels" / sid / "kernel-spec.json").exists()  # credentials shredded
    assert ctl.request(A, "kernel_execute", {"session_id": sid, "code": "1"})["ok"] is False


def test_zz_no_orphans_between_tests(cluster):
    """After all jobs: only the three daemon JVMs remain."""
    cluster.wait_idle()
    ctl = cluster.ctl
    ctl.wait_for(lambda: len(_java_procs()) == 3, 60, "only daemon JVMs left")
    assert _scoped("CoarseGrainedExecutorBackend") + _scoped("SparkSubmit") == []
    assert _scoped("olcf.runtime_lib") == []  # no runner / kernel host of this test is left behind
