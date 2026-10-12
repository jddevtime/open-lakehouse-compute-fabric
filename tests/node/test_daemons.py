# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Spark daemon management (:mod:`olcf.worker.daemons`) tested with a *fake* ``spark-class`` (real processes, real sockets, real
health polling, no JVM).

What this proves: command line + environment construction, private conf dir and permissions, readiness by the daemon's own
``/json/`` endpoint, duplicate / changed-parameter starts, port conflicts, graceful and forced stop, crash supervision with backoff
and give-up, adoption after an agent restart, allocation accounting, ``daemon_state`` frames and environment scrubbing. What it does
**not** prove: that the real JVM Master/Worker accept these arguments - that is covered by the ``spark`` integration tests
(``test_spark_cluster.py``) that start real daemons from the cached pyspark runtime.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import psutil
import pytest

from olcf.runtime.manager import ManagerConfig, RuntimeManager
from olcf.worker import daemons as dmod
from olcf.worker import procutil
from olcf.worker.daemons import DaemonManager
from olcf.worker.protocol import CommandError
from tests.node.conftest import make_cfg
from tests.node.fakes import make_fake_runtime

pytestmark = pytest.mark.skipif(os.name == "nt", reason="fake spark-class is a posix script")

RT = "olcf-rt-fake-spark"


class Out:
    """Minimal Outbound that records frames."""

    def __init__(self) -> None:
        self.frames: list[tuple[str, dict]] = []
        self.up = True

    def emit(self, ftype, payload, *, buffer=False, ack=None):  # noqa: ANN001, ANN201, ARG002
        self.frames.append((ftype, payload))
        return True

    def connected(self):  # noqa: ANN201
        return self.up

    def conn_gen(self):  # noqa: ANN201
        return 1

    def outq_depth(self):  # noqa: ANN201
        return 0


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "agent-secret-must-not-leak")
    monkeypatch.setenv("OLCF_WORKER_TOKEN", "agent-token-must-not-leak")
    state = tmp_path / "state"
    state.mkdir()
    make_fake_runtime(state, RT, fake_spark=True)
    cfg = make_cfg(state, ready_timeout_seconds=30.0, daemon_memory="192m")
    out = Out()
    mgr = DaemonManager(cfg, RuntimeManager(ManagerConfig(state_dir=state)), out)
    yield mgr, out, state
    mgr.stop_all(kill=True, grace=3.0)
    for p in psutil.process_iter(["cmdline"]):
        try:
            if any("spark-class" in c for c in p.info["cmdline"] or []) and str(state) in " ".join(p.cmdline()):
                p.kill()
        except psutil.Error:
            pass


def gone(pid: int) -> bool:
    """Dead or a zombie (the test process is the parent of daemons the *first* manager started and cannot reap them)."""
    try:
        return not psutil.pid_exists(pid) or psutil.Process(pid).status() == psutil.STATUS_ZOMBIE
    except psutil.Error:
        return True


def inv(state: Path, role: str) -> dict:
    return json.loads((state / "daemons" / f"{role}-{RT}" / "logs" / "fake-invocation.json").read_text())


def mport() -> tuple[int, int]:
    return procutil.free_port(), procutil.free_port()


def start_master(mgr, **kw):
    port, ui = mport()
    res = mgr.start_master({"runtime_id": RT, "host": "127.0.0.1", "port": port, "ui_port": ui, **kw})
    return res, port, ui


def test_master_start_builds_the_real_command_line(env):
    mgr, out, state = env
    res, port, ui = start_master(mgr, conf={"spark.authenticate": "true", "spark.authenticate.secret": "master-conf-secret"})
    assert res["state"] == "running" and res["url"] == f"spark://127.0.0.1:{port}" and res["ui_url"] == f"http://127.0.0.1:{ui}" and res["pid"]
    rec = inv(state, "master")
    argv = rec["argv"]
    assert argv[0] == "org.apache.spark.deploy.master.Master"
    assert argv[argv.index("--host") + 1] == "127.0.0.1" and argv[argv.index("--port") + 1] == str(port) and argv[argv.index("--webui-port") + 1] == str(ui)
    assert argv[argv.index("--properties-file") + 1].endswith("conf/spark-defaults.conf")
    e = rec["env"]
    assert e["SPARK_DAEMON_MEMORY"] == "192m" and e["SPARK_NO_DAEMONIZE"] == "1" and e["SPARK_LOCAL_IP"] == "127.0.0.1"
    assert e["SPARK_HOME"].endswith("site-packages/pyspark") and e["JAVA_HOME"]
    assert e["AWS_SECRET_ACCESS_KEY"] is None and e["OLCF_WORKER_TOKEN"] is None  # the agent's secrets never reach daemons
    assert "authenticate.secret master-conf-secret" in rec["props"]
    f = state / "daemons" / f"master-{RT}" / "conf" / "spark-defaults.conf"
    assert (f.stat().st_mode & 0o077) == 0  # may carry spark.authenticate.secret
    assert ((state / "daemons" / f"master-{RT}").stat().st_mode & 0o077) == 0
    assert any(t == "daemon_state" and p["state"] == "running" and p["role"] == "master" for t, p in out.frames)
    doc = json.loads((state / "daemons" / f"master-{RT}" / "daemon.json").read_text())
    assert doc["pid"] == res["pid"] and doc["state"] == "running" and "conf" not in doc["params"] and doc["conf_keys"] == ["spark.authenticate", "spark.authenticate.secret"]
    assert "master-conf-secret" not in json.dumps(doc)  # the pid/parameter file never stores conf values


def test_worker_start_command_and_allocation(env):
    mgr, out, state = env
    _, port, ui = start_master(mgr)
    wres = mgr.start_worker({"runtime_id": RT, "master_url": f"spark://127.0.0.1:{port}", "cores": 2, "memory_mb": 768, "host": "127.0.0.1", "labels": {"pool": "a"}})
    assert wres["state"] == "running" and wres["cores"] == 2 and wres["memory_mb"] == 768 and wres["ui_port"]
    argv = inv(state, "worker")["argv"]
    assert argv[0] == "org.apache.spark.deploy.worker.Worker" and argv[-1] == f"spark://127.0.0.1:{port}"
    assert argv[argv.index("--cores") + 1] == "2" and argv[argv.index("--memory") + 1] == "768m"
    assert argv[argv.index("--work-dir") + 1].endswith(f"worker-{RT}/work")
    assert "spark.worker.cleanup.enabled true" in inv(state, "worker")["props"]
    assert wres["port"] == 41234  # parsed from the worker id served at /json/
    assert mgr.allocated() == (2, 768)  # the offered capacity is unavailable for job drivers
    comps = {c["name"]: c["status"] for c in mgr.components()}
    assert comps[f"spark-master:{RT}"] == "ok" and comps[f"spark-worker:{RT}"] == "ok"
    assert {(d["role"], d["state"]) for d in mgr.snapshot()} == {("master", "running"), ("worker", "running")}
    mgr.stop("worker", {"runtime_id": RT, "grace_seconds": 3})
    assert mgr.allocated() == (0, 0)


def test_worker_ui_port_is_kept_across_restarts(env):
    mgr, _, state = env
    _, port, _ = start_master(mgr)
    w = mgr.start_worker({"runtime_id": RT, "master_url": f"spark://127.0.0.1:{port}", "cores": 1, "memory_mb": 256})
    ui = w["ui_port"]
    d = mgr.daemons[f"worker:{RT}"]
    d.proc.kill()
    d.proc.wait(10)
    mgr._check(d)  # noqa: SLF001 - schedules the restart
    assert d.state == "restarting"
    d.next_restart_at = 0
    mgr._check(d)  # noqa: SLF001
    assert d.state == "running" and d.ui_port == ui


def test_duplicate_start_and_parameter_change(env):
    mgr, _, state = env
    res, port, ui = start_master(mgr)
    again = mgr.start_master({"runtime_id": RT, "host": "127.0.0.1", "port": port, "ui_port": ui})
    assert again["duplicate"] is True and again["pid"] == res["pid"]
    assert inv(state, "master")["launch"] == 1
    # same daemon, different parameters -> restart with the new ones
    port2, ui2 = mport()
    changed = mgr.start_master({"runtime_id": RT, "host": "127.0.0.1", "port": port2, "ui_port": ui2})
    assert changed["pid"] != res["pid"] and changed["url"].endswith(str(port2)) and "duplicate" not in changed
    assert not psutil.pid_exists(res["pid"]) or psutil.Process(res["pid"]).status() == psutil.STATUS_ZOMBIE


def test_validation_errors(env):
    mgr, _, state = env
    with pytest.raises(CommandError) as ei:
        mgr.start_master({})
    assert ei.value.code == "bad_request"
    with pytest.raises(CommandError) as ei:
        mgr.start_worker({"runtime_id": RT, "master_url": "http://x:1"})
    assert ei.value.code == "bad_request"
    with pytest.raises(CommandError) as ei:
        mgr.start_master({"runtime_id": "olcf-rt-ghost"})
    assert ei.value.code == "runtime_not_ready"
    make_fake_runtime(state, "olcf-rt-nospark")
    # a runtime whose lock has no pyspark cannot host daemons
    lock = json.loads((state / "runtimes" / "olcf-rt-nospark" / "lock.json").read_text())
    lock["spark"] = {}
    (state / "runtimes" / "olcf-rt-nospark" / "lock.json").write_text(json.dumps(lock))
    with pytest.raises(CommandError) as ei:
        mgr.start_master({"runtime_id": "olcf-rt-nospark", "port": 1, "ui_port": 2})
    assert ei.value.code in ("runtime_no_spark", "runtime_not_ready")


def test_port_in_use_is_reported_before_launch(env):
    mgr, _, state = env
    import socket

    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    s.listen(1)
    busy = s.getsockname()[1]
    try:
        with pytest.raises(CommandError) as ei:
            mgr.start_master({"runtime_id": RT, "host": "127.0.0.1", "port": busy, "ui_port": procutil.free_port()})
        assert ei.value.code == "port_in_use" and str(busy) in ei.value.message
        with pytest.raises(CommandError) as ei:
            mgr.start_master({"runtime_id": RT, "host": "127.0.0.1", "port": procutil.free_port(), "ui_port": busy})
        assert ei.value.code == "port_in_use"
    finally:
        s.close()
    assert not (state / "daemons" / f"master-{RT}" / "logs" / "fake-invocation.json").exists()  # nothing was launched


def test_daemon_that_dies_at_startup_is_reported_with_its_log(env):
    mgr, _, state = env
    d = state / "daemons" / f"master-{RT}"
    d.mkdir(parents=True)
    (d / "fake-mode").write_text("crash")
    with pytest.raises(CommandError) as ei:
        start_master(mgr)
    assert ei.value.code == "daemon_failed" and "fake crash" in ei.value.message
    assert mgr.daemons[f"master:{RT}"].state == "failed"


def test_daemon_that_never_becomes_healthy_times_out_and_is_stopped(tmp_path):
    state = tmp_path / "state"
    state.mkdir()
    make_fake_runtime(state, RT, fake_spark=True)
    cfg = make_cfg(state, ready_timeout_seconds=3.0)
    mgr = DaemonManager(cfg, RuntimeManager(ManagerConfig(state_dir=state)), Out())
    d = state / "daemons" / f"master-{RT}"
    d.mkdir(parents=True)
    (d / "fake-mode").write_text("dead")  # listens, but answers status DEAD
    try:
        with pytest.raises(CommandError) as ei:
            start_master(mgr)
        assert ei.value.code == "daemon_timeout"
        pid = mgr.daemons[f"master:{RT}"].pid
        assert pid is None or not psutil.pid_exists(pid) or psutil.Process(pid).status() == psutil.STATUS_ZOMBIE
    finally:
        mgr.stop_all(kill=True, grace=2.0)


def test_stop_graceful_forced_and_unknown(env):
    mgr, out, state = env
    res, *_ = start_master(mgr)
    r = mgr.stop("master", {"runtime_id": RT, "grace_seconds": 5})
    assert r == {"role": "master", "runtime_id": RT, "state": "stopped", "found": True}
    assert not psutil.pid_exists(res["pid"]) or psutil.Process(res["pid"]).status() == psutil.STATUS_ZOMBIE
    assert [p["state"] for t, p in out.frames if t == "daemon_state" and p["role"] == "master"][-2:] == ["stopping", "stopped"]
    assert mgr.snapshot() == []  # stopped daemons are not reported as present
    assert mgr.stop("worker", {"runtime_id": "nope"})["found"] is False
    # a daemon that ignores SIGTERM is killed after the grace period
    (state / "daemons" / f"master-{RT}" / "fake-mode").write_text("ignore-term")
    res2, *_ = start_master(mgr)
    t0 = time.monotonic()
    mgr.stop("master", {"runtime_id": RT, "grace_seconds": 1})
    assert time.monotonic() - t0 < 15
    assert not psutil.pid_exists(res2["pid"]) or psutil.Process(res2["pid"]).status() == psutil.STATUS_ZOMBIE


def test_crash_is_supervised_with_restart_and_backoff(env, monkeypatch):
    mgr, out, state = env
    res, port, ui = start_master(mgr)
    d = mgr.daemons[f"master:{RT}"]
    os_pid = res["pid"]
    psutil.Process(os_pid).kill()
    d.proc.wait(10)
    mgr._check(d)  # noqa: SLF001
    assert d.state == "restarting" and "restarting in" in d.detail
    assert d.backoff == 2.0
    d.next_restart_at = 0
    mgr._check(d)  # noqa: SLF001
    assert d.state == "running" and d.pid != os_pid
    assert inv(state, "master")["launch"] == 2
    states = [p["state"] for t, p in out.frames if t == "daemon_state" and p["role"] == "master"]
    assert "restarting" in states and states[-1] == "running"


def test_restart_storm_ends_in_failed(env, monkeypatch):
    """A daemon that keeps dying during startup is retried a bounded number of times, then reported ``failed`` (not retried forever)."""
    mgr, out, state = env
    monkeypatch.setattr(dmod, "MAX_RESTARTS", 2)
    start_master(mgr)
    d = mgr.daemons[f"master:{RT}"]
    (state / "daemons" / f"master-{RT}" / "fake-mode").write_text("crash")
    d.proc.kill()
    d.proc.wait(10)
    for _ in range(10):
        mgr._check(d)  # noqa: SLF001
        if d.state == "failed":
            break
        d.next_restart_at = 0
    launches = int((state / "daemons" / f"master-{RT}" / "fake-launches").read_text())
    assert launches == 1 + 2  # the first start plus exactly MAX_RESTARTS relaunch attempts
    assert d.state == "failed" and "giving up" in d.detail
    assert any(t == "daemon_state" and p["state"] == "failed" for t, p in out.frames)
    comps = {c["name"]: c["status"] for c in mgr.components()}
    assert comps[f"spark-master:{RT}"] == "down"


def test_supervisor_thread_restarts_a_crashed_daemon(env):
    mgr, _, state = env
    res, *_ = start_master(mgr)
    psutil.Process(res["pid"]).kill()
    d = mgr.daemons[f"master:{RT}"]
    d.proc.wait(10)
    end = time.monotonic() + 40
    while time.monotonic() < end and not (d.state == "running" and d.pid != res["pid"]):
        time.sleep(0.5)
    assert d.state == "running" and d.pid != res["pid"]


def test_degraded_health_is_reported_not_restarted(env):
    mgr, out, state = env
    start_master(mgr)
    d = mgr.daemons[f"master:{RT}"]
    pid = d.pid
    d.ui_port = procutil.free_port()  # the web UI no longer answers where we look (simulates a wedged UI)
    mgr._check(d)  # noqa: SLF001
    assert d.state == "running" and d.detail.startswith("degraded") and d.pid == pid


def test_adoption_after_agent_restart(env):
    mgr, out, state = env
    res, port, ui = start_master(mgr, conf={"spark.x": "1"})
    w = mgr.start_worker({"runtime_id": RT, "master_url": f"spark://127.0.0.1:{port}", "cores": 1, "memory_mb": 256})
    mgr.stop_all(kill=False)  # agent exits, daemons keep running
    assert psutil.pid_exists(res["pid"]) and psutil.pid_exists(w["pid"])
    cfg = mgr.cfg
    mgr2 = DaemonManager(cfg, RuntimeManager(ManagerConfig(state_dir=state)), Out())
    try:
        adopted = mgr2.adopt()
        assert sorted(adopted) == [f"master:{RT}", f"worker:{RT}"]
        m = mgr2.daemons[f"master:{RT}"]
        assert m.pid == res["pid"] and m.state == "running" and m.params["conf"] == {"spark.x": "1"} and m.ui_port == ui
        assert mgr2.allocated() == (1, 256)
        # stopping an adopted daemon (not our child) works through pid + create time
        r = mgr2.stop("worker", {"runtime_id": RT, "grace_seconds": 5})
        assert r["state"] == "stopped" and gone(w["pid"])
    finally:
        mgr2.stop_all(kill=True, grace=3.0)


def test_daemon_gone_while_agent_was_down_is_relaunched(env):
    mgr, out, state = env
    res, *_ = start_master(mgr)
    mgr.stop_all(kill=False)
    psutil.Process(res["pid"]).kill()
    end = time.monotonic() + 10
    while time.monotonic() < end and psutil.pid_exists(res["pid"]) and psutil.Process(res["pid"]).status() != psutil.STATUS_ZOMBIE:
        time.sleep(0.1)
    mgr2 = DaemonManager(mgr.cfg, RuntimeManager(ManagerConfig(state_dir=state)), Out())
    try:
        assert mgr2.adopt() == [f"master:{RT}"]
        d = mgr2.daemons[f"master:{RT}"]
        assert d.state == "restarting"
        mgr2._check(d)  # noqa: SLF001
        assert d.state == "running" and d.pid != res["pid"]
    finally:
        mgr2.stop_all(kill=True, grace=3.0)


def test_stopped_daemons_are_not_adopted(env):
    mgr, _, state = env
    start_master(mgr)
    mgr.stop("master", {"runtime_id": RT, "grace_seconds": 3})
    mgr2 = DaemonManager(mgr.cfg, RuntimeManager(ManagerConfig(state_dir=state)), Out())
    assert mgr2.adopt() == []


def test_stop_all_kill_workers_before_master(env):
    mgr, out, state = env
    _, port, _ = start_master(mgr)
    mgr.start_worker({"runtime_id": RT, "master_url": f"spark://127.0.0.1:{port}", "cores": 1, "memory_mb": 256})
    mgr.stop_all(kill=True, grace=3.0)
    stopped = [p["role"] for t, p in out.frames if t == "daemon_state" and p["state"] == "stopped"]
    assert stopped == ["worker", "master"]


def test_log_tail_is_redacted(env):
    mgr, _, state = env
    start_master(mgr)
    log = state / "daemons" / f"master-{RT}" / "logs" / "daemon.log"
    with open(log, "a") as fh:
        fh.write("Authorization: Bearer abcdefghijklmnopqrstuvwxyz0123456789\n")
    tail = mgr.log_tail("master", RT)
    assert "fake spark-class" in tail and "abcdefghijklmnopqrstuvwxyz0123456789" not in tail
    assert mgr.log_tail("master", "nope") == ""


def test_http_json_helper():
    assert dmod.http_json("http://127.0.0.1:1/json/", timeout=0.5) is None
