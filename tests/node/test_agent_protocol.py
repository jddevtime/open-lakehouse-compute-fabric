# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Agent <-> controller protocol tests (CONTRACTS 4.2) against the fake hub, using the fake (non-Spark) runtime.

Covers: hello/heartbeat contents, ping, unknown command, command-id idempotency, drain (command + controller flag),
auth-failure retry, probe, diagnostics, install_runtime validation, shutdown, fetch_artifact confinement and the
enrolment + ``olcf-worker status/leave`` CLI. Job execution and delivery semantics are in ``test_agent_jobs.py``.
"""

# ruff: noqa: S106
from __future__ import annotations

import json
import time
import uuid
import zipfile

import pytest

from olcf.version import WORKER_PROTOCOL_VERSION
from olcf.worker import identity as ident_mod
from olcf.worker import main as worker_main
from tests.node.conftest import FAKE_RT, job_spec, wait_final
from tests.node.fake_controller import FakeEnrollServer

W = "wrk_1"


def test_hello_contents(controller, start_agent, fake_state):
    start_agent(pool="p1", labels={"zone": "a"})
    wid, hello = controller.hellos[-1]
    assert wid == W and hello["worker_id"] == W
    assert hello["protocol"] == WORKER_PROTOCOL_VERSION
    assert hello["embedded"] is False and hello["draining"] is False
    assert hello["pool"] == "p1" and hello["labels"] == {"zone": "a"}
    inv = hello["inventory"]
    assert inv["cores"] >= 1 and inv["memory_mb"] > 0 and inv["os"] and inv["hostname"]
    assert hello["capacity"]["cores"] == 8 and hello["capacity"]["memory_mb"] == 64000
    assert hello["allocated"] == {"cores": 0, "memory_mb": 0}
    assert hello["active_jobs"] == [] and hello["kernels"] == [] and hello["spark_daemons"] == []
    assert [r["id"] for r in hello["runtimes"]] == [FAKE_RT, "olcf-host-python"]  # the built-in host-Python runtime is always reported last
    assert hello["runtimes"][0]["state"] == "ready"
    json.dumps(hello)  # secrets must never be part of it
    assert "tok-1" not in json.dumps(hello)


def test_heartbeat_contents(controller, start_agent):
    start_agent()
    hb = controller.wait_for(lambda: controller.heartbeats.get(W), 15, "heartbeat")
    assert hb["metrics"]["cpu_percent"] is not None or "cpu_percent" in hb["metrics"]
    names = {c["name"] for c in hb["components"]}
    assert {"agent", "control-plane-link", "disk", f"runtime:{FAKE_RT}"} <= names
    assert hb["capacity"] == {"cores": 8, "memory_mb": 64000} and hb["draining"] is False
    assert hb["active_jobs"] == [] and hb["spark_daemons"] == []
    # heartbeats keep coming at the controller's interval
    n0 = len(controller.frames_of("heartbeat"))
    controller.wait_for(lambda: len(controller.frames_of("heartbeat")) >= n0 + 2, 15, "two more heartbeats")


def test_every_frame_has_the_envelope(controller, start_agent):
    start_agent()
    controller.wait_for(lambda: controller.heartbeats.get(W), 15)
    for f in controller.frames_of("heartbeat") + controller.frames_of("hello"):
        assert f["v"] == 1 and f["ts"] and isinstance(f["payload"], dict)
    assert all(f.get("id") for f in controller.frames_of("hello"))
    assert not any(f.get("id") for f in controller.frames_of("heartbeat"))  # anonymous: nothing to acknowledge


def test_ping_and_unknown_command(controller, start_agent):
    start_agent()
    data = controller.ok(W, "ping")
    assert data["worker_id"] == W and data["pong"] and data["protocol"] == WORKER_PROTOCOL_VERSION
    res = controller.request(W, "frobnicate", {})
    assert res["ok"] is False and res["error"]["code"] == "unsupported_command"
    res = controller.request(W, "run_job", {})
    assert res["ok"] is False and res["error"]["code"] == "bad_request"
    res = controller.request(W, "run_job", {"spec": {"job_id": "../evil", "runtime": {"id": FAKE_RT}}})
    assert res["ok"] is False and res["error"]["code"] == "bad_request"
    controller.ok(W, "ping")  # the agent is still healthy


def test_command_id_idempotency(controller, start_agent):
    start_agent()
    payload = job_spec("idem1", "print('once')\nEXIT_VALUE = 1\n")
    cid = uuid.uuid4().hex
    frame = {"v": 1, "type": "run_job", "id": cid, "ts": "2026-01-01T00:00:00Z", "payload": payload}
    controller.send_raw(W, frame)
    controller.wait_for(lambda: [f for f in controller.frames_of("result") if f.get("ref") == cid], 15, "first result")
    controller.send_raw(W, frame)  # a retransmission of the same command (e.g. after a controller-side timeout)
    controller.wait_for(lambda: len([f for f in controller.frames_of("result") if f.get("ref") == cid]) >= 2, 15, "replayed result")
    results = [f["payload"] for f in controller.frames_of("result") if f.get("ref") == cid]
    assert results[0] == results[1] and results[0]["ok"] and "duplicate" not in results[0]["data"]
    wait_final(controller, "idem1")
    assert controller.log_text("idem1").count("once") == 1  # executed once
    # a *new* command id for the same (job, attempt) is answered as a duplicate, never executed twice
    again = controller.ok(W, "run_job", payload)
    assert again["duplicate"] is True and again["state"] == "succeeded"


def test_drain_command_and_controller_flag(controller, start_agent, fake_state):
    start_agent()
    d = controller.ok(W, "drain", {"on": True})
    assert d["draining"] is True
    assert json.loads((fake_state / "drain.json").read_text())["draining"] is True
    res = controller.request(W, "run_job", job_spec("d1", "print(1)"))
    assert res["ok"] is False and res["error"]["code"] == "draining"
    controller.ok(W, "drain", {"on": False})
    controller.ok(W, "run_job", job_spec("d2", "print(2)"))
    assert wait_final(controller, "d2")["state"] == "succeeded"
    # the controller's flag in the hello result is authoritative after a reconnect
    controller.drain[W] = True
    n = len(controller.hellos)
    controller.drop(W)
    controller.wait_for(lambda: len(controller.hellos) > n and controller.connected(W), 20, "reconnect")
    assert controller.hellos[-1][1]["draining"] is False  # what the agent reported (its own state at that moment)
    res = controller.request(W, "run_job", job_spec("d3", "print(3)"))
    assert res["ok"] is False and res["error"]["code"] == "draining"
    controller.drain[W] = False
    controller.drop(W)
    controller.wait_for(lambda: len(controller.hellos) > n + 1 and controller.connected(W), 20, "second reconnect")
    controller.ok(W, "run_job", job_spec("d4", "print(4)"))
    assert wait_final(controller, "d4")["state"] == "succeeded"


def test_drain_survives_agent_restart(controller, start_agent, fake_state):
    h = start_agent()
    controller.ok(W, "drain", {"on": True})
    controller.drain[W] = True  # keep the controller consistent with the agent
    h.stop()
    n = len(controller.hellos)
    start_agent()
    controller.wait_for(lambda: len(controller.hellos) > n, 20, "second hello")
    assert controller.hellos[-1][1]["draining"] is True


def test_auth_failure_is_retried_until_token_is_valid(controller, start_agent):
    controller.tokens.pop("tok-1")
    controller.tokens["other"] = "wrk_other"
    h = start_agent(wait=False)
    controller.wait_for(lambda: controller.auth_failures >= 2, 20, "repeated rejected connects")
    assert not controller.connected(W) and h.thread.is_alive()  # keeps trying, does not crash or spin
    controller.tokens["tok-1"] = W
    controller.wait_connected(W, 30)
    assert h.agent.status_snapshot()["connected"]


def test_status_file(controller, start_agent, fake_state):
    start_agent()
    st = controller.wait_for(lambda: json.loads((fake_state / "agent-status.json").read_text()) if (fake_state / "agent-status.json").exists() else None, 10)
    assert st["worker_id"] == W and st["connected"] is True and st["pid"]
    assert (fake_state / "agent.pid").exists()
    assert "token" not in json.dumps(st).lower() or "tok-1" not in json.dumps(st)


def test_single_instance_guard(controller, fake_state):
    """A pid file of a *different live process* blocks a second agent; a stale one (other create time = pid reuse) does not."""
    import os
    import subprocess
    import sys

    from olcf.worker import procutil
    from olcf.worker.agent import Agent, AgentError
    from tests.node.conftest import make_cfg, make_ident

    other = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])  # noqa: S603
    try:
        procutil.write_pidfile(fake_state / "agent.pid", other.pid)
        with pytest.raises(AgentError, match="already uses"):
            Agent(make_cfg(fake_state), make_ident(controller)).run()
        assert other.poll() is None  # the guard never touches the other process
        doc = procutil.read_pidfile(fake_state / "agent.pid")
        (fake_state / "agent.pid").write_text(json.dumps({"pid": other.pid, "create_time": doc["create_time"] - 1000}))
        Agent(make_cfg(fake_state), make_ident(controller))._guard_single_instance()  # noqa: SLF001
        assert procutil.read_pidfile(fake_state / "agent.pid")["pid"] == os.getpid()
    finally:
        other.kill()
        other.wait(10)


def test_probe_default_and_custom(controller, start_agent):
    start_agent()
    data = controller.ok(W, "probe", {})
    kinds = {c["kind"] for c in data["checks"]}
    assert {"disk", "clock", "dns", "tcp"} <= kinds
    byname = {c["name"]: c for c in data["checks"]}
    assert byname["control-plane-tcp"]["ok"] and byname["disk"]["ok"]
    assert byname["clock"]["ok"], byname["clock"]
    assert f"runtime:{FAKE_RT}" in byname and byname[f"runtime:{FAKE_RT}"]["ok"] is False  # the look-alike runtime is not a signed install
    data = controller.ok(W, "probe", {"checks": [{"kind": "tcp", "host": "127.0.0.1", "port": controller.port, "name": "hub"}, {"kind": "tcp", "host": "127.0.0.1", "port": 1, "name": "nothing", "timeout": 1}]})
    assert data["ok"] is False and [c["ok"] for c in data["checks"]] == [True, False]


def test_collect_diagnostics_is_redacted(controller, start_agent, fake_state):
    start_agent()
    controller.ok(W, "run_job", job_spec("diag1", "print('hello diag')\n", secrets={"S": "diag-secret-value-123"}))
    wait_final(controller, "diag1")
    data = controller.ok(W, "collect_diagnostics", {"job_ids": ["diag1"]})
    assert data
    zips = list((fake_state / "diagnostics").glob("diag-*.zip"))
    assert zips
    with zipfile.ZipFile(zips[-1]) as zf:
        blob = b"".join(zf.read(n) for n in zf.namelist())
        names = zf.namelist()
    assert b"tok-1" not in blob and b"diag-secret-value-123" not in blob
    assert not any(n.rsplit("/", 1)[-1] in ("job.json", "kernel-spec.json", "identity.json", "client.key") for n in names)
    assert "summary.json" in names and any(n.endswith("job.public.json") for n in names)


def test_install_runtime_validation(controller, start_agent):
    start_agent()
    res = controller.request(W, "install_runtime", {"mode": "teleport", "runtime_id": FAKE_RT})
    assert res["ok"] is False and res["error"]["code"] == "bad_request"
    res = controller.request(W, "install_runtime", {"mode": "verify"})
    assert res["ok"] is False and res["error"]["code"] == "bad_request"
    res = controller.request(W, "install_runtime", {"mode": "verify", "runtime_id": "olcf-rt-ghost"})
    assert res["ok"] is False and res["error"]["code"]  # not installed -> structured failure, agent keeps running
    res = controller.request(W, "install_runtime", {"mode": "install", "runtime_manifest": {"id": "olcf-rt-bogus", "family": "olcf-native"}})
    assert res["ok"] is False  # unsigned / invalid manifest is refused (fail closed)
    controller.ok(W, "ping")


def test_shutdown_command_stops_the_agent(controller, start_agent):
    h = start_agent()
    assert controller.ok(W, "shutdown", {})["stopping"] is True
    h.thread.join(30)
    assert not h.thread.is_alive() and h.exit_code == 0


def test_fetch_artifact_confinement(controller, start_agent):
    start_agent()
    src = "import os\nopen('out.txt', 'w').write('artifact-body')\nopen('sub.bin', 'wb').write(os.urandom(5000))\n"
    controller.ok(W, "run_job", job_spec("art1", src))
    assert wait_final(controller, "art1")["state"] == "succeeded"
    d = controller.ok(W, "fetch_artifact", {"job_id": "art1", "path": "out.txt"})
    import base64

    assert base64.b64decode(d["data_b64"]) == b"artifact-body" and d["eof"] is True and d["size"] == 13
    d = controller.ok(W, "fetch_artifact", {"job_id": "art1", "path": "sub.bin", "offset": 4000, "length": 2000})
    assert d["length"] == 1000 and d["eof"] is True and d["offset"] == 4000
    for bad in ("../../../etc/passwd", "/etc/passwd", "job.json", "agent-job.json", "missing.txt"):
        res = controller.request(W, "fetch_artifact", {"job_id": "art1", "path": bad})
        assert res["ok"] is False, bad
    res = controller.request(W, "fetch_artifact", {"job_id": "../art1", "path": "out.txt"})
    assert res["ok"] is False


# --------------------------------------------------------------------------------------------- enrolment + CLI
def test_enrollment_writes_identity_0600(tmp_path):
    from tests.node.conftest import make_cfg

    srv = FakeEnrollServer(tmp_path / "pki")
    try:
        state = tmp_path / "state"
        cfg = make_cfg(state, url=srv.url, token="olcf_enr_testtoken0001", name="node-x")
        ident = ident_mod.enroll(cfg, {"hostname": "node-x", "cpu_count": 2})
        assert ident.worker_id.startswith("wrk_") and ident.worker_token and ident.ca_pem
        loaded = ident_mod.load_identity(state)
        assert loaded is not None and loaded.worker_id == ident.worker_id and loaded.worker_token == ident.worker_token
        paths = ident_mod.identity_paths(state)
        for name, p in paths.items():
            if p.exists() and ("key" in name or "identity" in name or "token" in name):
                assert (p.stat().st_mode & 0o077) == 0, f"{name} is group/world accessible"
        assert "worker_token" not in json.dumps(loaded.redacted()) or ident.worker_token not in json.dumps(loaded.redacted())
        # a used (single-use) token cannot enrol again
        with pytest.raises(Exception):  # noqa: B017, PT011
            ident_mod.enroll(make_cfg(tmp_path / "state2", url=srv.url, token="olcf_enr_testtoken0001"), {})
        ident_mod.delete_identity(state)
        assert ident_mod.load_identity(state) is None
    finally:
        srv.close()


def test_enrollment_wrong_token(tmp_path):
    from tests.node.conftest import make_cfg

    srv = FakeEnrollServer(tmp_path / "pki")
    try:
        with pytest.raises(Exception) as ei:  # noqa: PT011
            ident_mod.enroll(make_cfg(tmp_path / "s", url=srv.url, token="olcf_enr_wrong"), {})
        assert "olcf_enr_wrong" not in str(ei.value)  # secrets are not echoed
        assert ident_mod.load_identity(tmp_path / "s") is None
    finally:
        srv.close()


@pytest.fixture
def agent_process(controller, fake_state):
    """A real ``python -m olcf.worker.main run`` process (its own pid, SIGTERM handling, pid file) against the fake controller."""
    import os
    import subprocess
    import sys

    from tests.node.conftest import ROOT, make_ident

    ident_mod.save_identity(fake_state, make_ident(controller))
    env = {**os.environ, "PYTHONPATH": str(ROOT / "src")}
    env.pop("OLCF_WORKER_TOKEN", None)
    log = open(fake_state.parent / "agent-proc.log", "wb")  # noqa: SIM115
    proc = subprocess.Popen([sys.executable, "-m", "olcf.worker.main", "run", "--state-dir", str(fake_state), "--max-cores", "8", "--max-memory-mb", "64000",
                             "--log-level", "WARNING"], env=env, stdout=log, stderr=log, cwd=str(ROOT))  # noqa: S603
    try:
        controller.wait_connected(W, 60)
        yield proc
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(30)
            except subprocess.TimeoutExpired:
                proc.kill()
        log.close()


def test_cli_status_while_running(agent_process, controller, fake_state, capsys):
    controller.wait_for(lambda: controller.heartbeats.get(W), 15)
    assert worker_main.main(["status", "--state-dir", str(fake_state), "--json"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["agent_running"] is True and out["identity"]["worker_id"] == W and out["agent_pid"] == agent_process.pid
    assert "tok-1" not in json.dumps(out)
    assert worker_main.main(["status", "--state-dir", str(fake_state)]) == 0
    text = capsys.readouterr().out
    assert W in text and "tok-1" not in text


def test_cli_status_when_not_enrolled(tmp_path, capsys):
    assert worker_main.main(["status", "--state-dir", str(tmp_path / "empty")]) == 1
    out = capsys.readouterr().out
    assert "enrolled  : no" in out and "not running" in out


def test_cli_leave_dry_run_stop_only_then_full(agent_process, controller, fake_state, capsys):
    import psutil

    controller.ok(W, "run_job", job_spec("leave-job", "import time\nprint('up', flush=True)\ntime.sleep(300)\n"))
    pid = controller.wait_for(lambda: next((j["pid"] for j in controller.hellos[-1][1]["active_jobs"] if j["job_id"] == "leave-job"), None) or
                              next((j["pid"] for j in (controller.heartbeats.get(W) or {}).get("active_jobs", []) if j["job_id"] == "leave-job"), None), 20, "job pid")
    assert psutil.pid_exists(pid)
    # dry run changes nothing
    assert worker_main.main(["leave", "--state-dir", str(fake_state)]) == 0
    out = capsys.readouterr().out
    assert "dry run" in out and "terminate job leave-job" in out
    assert agent_process.poll() is None and ident_mod.load_identity(fake_state) is not None
    # --stop-only: the agent exits, the job keeps running (it is adopted by the next agent), identity stays
    assert worker_main.main(["leave", "--state-dir", str(fake_state), "--yes", "--stop-only"]) == 0
    agent_process.wait(60)
    assert psutil.pid_exists(pid) and ident_mod.load_identity(fake_state) is not None
    # full leave: jobs are terminated, identity removed, runtimes kept, operator told which worker to revoke
    assert worker_main.main(["leave", "--state-dir", str(fake_state), "--yes", "--purge"]) == 0
    out = capsys.readouterr().out
    assert W in out and "Revoke" in out
    end = time.monotonic() + 15
    while psutil.pid_exists(pid) and time.monotonic() < end:
        try:
            if psutil.Process(pid).status() == psutil.STATUS_ZOMBIE:
                break
        except psutil.Error:
            break
        time.sleep(0.2)
    assert not psutil.pid_exists(pid) or psutil.Process(pid).status() == psutil.STATUS_ZOMBIE
    assert ident_mod.load_identity(fake_state) is None
    assert (fake_state / "runtimes" / FAKE_RT).exists()
    assert not (fake_state / "work").exists() or not any((fake_state / "work").iterdir())
