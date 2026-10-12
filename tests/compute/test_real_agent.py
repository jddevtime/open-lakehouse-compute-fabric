# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""CTRL <-> NODE integration: the controller against the *production* worker agent and runner (``olcf.worker.main``), no fake agent in between.

The agent runs as a real subprocess against a runtime directory that looks like an installed runtime but needs no Spark (``tests.node.fakes.make_fake_runtime``,
the NODE package's own test double), so jobs run in real runner processes and kernels in a real ``ipykernel``. Everything here is what the architect's final
integration will exercise with a full runtime; Spark itself is covered by the NODE and DATA suites.
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
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import psutil
import pytest

from olcf.core.timeutil import from_iso, to_iso
from tests.compute.fake_agent import wait_for_agent, wait_until

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
fakes = pytest.importorskip("tests.node.fakes", reason="the NODE test doubles are part of the repository")

RT = "olcf-rt-fake-1"


class RealAgent:
    def __init__(self, st: Any, name: str, state_dir: Path) -> None:
        self.st, self.name, self.state_dir = st, name, state_dir
        self.proc: subprocess.Popen[bytes] | None = None
        self.log = state_dir.parent / f"{name}.log"

    def start(self) -> RealAgent:
        r = self.st.http.post("/v1/workers/enrollment-tokens", headers=self.st.admin, json={"name": self.name, "ttl_seconds": 600})
        assert r.status_code in (200, 201), r.text
        env = {k: v for k, v in os.environ.items() if k != "OLCF_WORKER_TOKEN"}
        env["PYTHONPATH"] = os.pathsep.join(os.path.abspath(p) for p in (env.get("PYTHONPATH") or str(ROOT / "src")).split(os.pathsep) if p)
        env["OLCF_WORKER_TOKEN"] = r.json()["token"]
        cmd = [sys.executable, "-m", "olcf.worker.main", "run", "--url", self.st.url, "--name", self.name, "--state-dir", str(self.state_dir)]
        with open(self.log, "ab") as out:
            self.proc = subprocess.Popen(cmd, stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, env=env, start_new_session=True, cwd=str(self.state_dir.parent))  # noqa: S603
        return self

    def stop(self) -> None:
        p = self.proc
        if p is None or p.poll() is not None:
            return
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(p.pid, signal.SIGTERM)
        try:
            p.wait(timeout=20)
        except subprocess.TimeoutExpired:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(p.pid, signal.SIGKILL)
            p.wait(timeout=10)


@pytest.fixture(scope="module")
def real(make_stack_module: Any, tmp_path_factory: pytest.TempPathFactory) -> Iterator[tuple[Any, RealAgent]]:
    st = make_stack_module
    assert st.http.put("/v1/admin/settings/compute.allow_unsigned_runtimes", headers=st.admin, json={"value": True}).status_code == 200
    base = json.loads((ROOT / "src/olcf/runtime/manifests/olcf-rt-native-1.0.0.json").read_text())
    base.update({"id": RT, "family": "olcf-native", "version": "1.0.0-fake"})
    base.pop("signature", None)
    r = st.http.post("/v1/runtimes", headers=st.admin, json={"manifest": base})
    assert r.status_code == 201, r.text
    sd = tmp_path_factory.mktemp("real-agent") / "state"
    sd.mkdir()
    fakes.make_fake_runtime(sd, RT)
    (sd / "agent.toml").write_text("cert_renew_jitter_seconds = 0\n")  # the credential-renewal test must not wait for the fleet-spreading delay
    agent = RealAgent(st, "real-1", sd).start()
    try:
        w = wait_for_agent(lambda: next((x for x in st.http.get("/v1/workers", headers=st.admin).json()["items"] if x["name"] == "real-1" and x["state"] == "online"), None),
                           agent.log, 90, 0.5, "the real agent to come online")
        wait_until(lambda: RT in st.compute.registry.node_infos()[0].ready_runtimes, 30, 0.5, "the agent to report the runtime as ready")
        assert w["inventory"]["protocol"] == "1.0"
        yield st, agent
    finally:
        agent.stop()
        pids = [agent.proc.pid] if agent.proc else []
        wait_until(lambda: not any(psutil.pid_exists(p) and psutil.Process(p).status() != psutil.STATUS_ZOMBIE for p in pids), 20, 0.2, "the agent to exit")


@pytest.fixture(scope="module")
def make_stack_module(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Any]:
    from tests.compute.ctrl_stack import Stack

    st = Stack(tmp_path_factory.mktemp("real-stack"), "sqlite").start()
    try:
        yield st
    finally:
        st.stop()


def submit(st: Any, ws: str, source: str, **over: Any) -> dict[str, Any]:
    over.setdefault("runtime", {"runtime_id": RT})
    return st.submit(ws, source=source, **over)


def test_python_job_runs_in_a_real_runner(real: Any) -> None:
    st, _agent = real
    ws = st.workspace()
    src = "import os, sys\nprint('hello from', os.environ.get('GREETING'))\nprint('to stderr', file=sys.stderr)\nEXIT_VALUE = {'rows': 3, 'ok': True}\n"
    j = submit(st, ws, src, env={"GREETING": "olcf"})
    done = st.wait_job(j["id"], timeout=90)
    assert done["state"] == "succeeded", done
    assert done["runtime_id"] == RT and done["driver_worker_id"]
    assert done["exit_value"] == {"rows": 3, "ok": True}
    logs = st.logs(j["id"])
    assert "hello from olcf" in logs and "to stderr" in logs
    assert "olcf_pat_" not in logs and "job_token" not in logs  # no credentials in the user-visible log
    assert done["application_id"] is None or isinstance(done["application_id"], str)


def test_user_code_failure_is_classified_with_traceback(real: Any) -> None:
    st, _agent = real
    ws = st.workspace()
    done = st.wait_job(submit(st, ws, "x = 1\nundefined_name + x\n")["id"], timeout=90)
    assert done["state"] == "failed" and done["failure"]["category"] == "user-code"
    assert "NameError" in json.dumps(done["failure"])
    assert done["attempt"] == 1


def test_dependency_failure_is_classified_by_rules(real: Any) -> None:
    st, _agent = real
    ws = st.workspace()
    done = st.wait_job(submit(st, ws, "import definitely_not_a_module_xyz\n")["id"], timeout=90)
    assert done["state"] == "failed" and done["failure"]["category"] in ("dependency", "user-code"), done["failure"]


def test_cancel_kills_the_runner_process_tree(real: Any) -> None:
    st, _agent = real
    ws = st.workspace()
    j = submit(st, ws, "import os, time\nprint('pid', os.getpid(), flush=True)\ntime.sleep(300)\n")
    st.wait_job(j["id"], "running", timeout=60)
    pid = int(wait_until(lambda: next((ln.split()[1] for ln in st.logs(j["id"]).splitlines() if ln.startswith("pid ")), None), 30, 0.3, "the runner to print its pid"))
    assert psutil.pid_exists(pid)
    assert st.http.post(f"/v1/jobs/{j['id']}/cancel", headers=st.admin).status_code in (200, 202)
    assert st.wait_job(j["id"], timeout=60)["state"] == "cancelled"
    wait_until(lambda: not psutil.pid_exists(pid) or psutil.Process(pid).status() == psutil.STATUS_ZOMBIE, 20, 0.2, "the job process to be gone")


def test_timeout_is_enforced_by_the_controller_and_the_agent(real: Any) -> None:
    st, _agent = real
    ws = st.workspace()
    j = submit(st, ws, "import time\ntime.sleep(300)\n", policy={"timeout_seconds": 3})
    done = st.wait_job(j["id"], timeout=90)
    assert done["state"] == "timed_out" and done["failure_category"] == "timeout"


def test_notebook_job_returns_output_notebook_as_an_artifact(real: Any) -> None:
    st, _agent = real
    ws = st.workspace()
    nb = {"nbformat": 4, "nbformat_minor": 5, "metadata": {"kernelspec": {"name": "python3", "display_name": "Python 3", "language": "python"}},
          "cells": [{"cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [], "source": "print('nb cell')\nEXIT_VALUE = 7"}]}
    r = st.http.post("/v1/jobs", headers=st.admin, json={"kind": "notebook", "workspace_id": ws, "engine": "python", "runtime": {"runtime_id": RT},
                                                         "source": {"format": "ipynb", "content": json.dumps(nb)}})
    assert r.status_code == 202, r.text
    done = st.wait_job(r.json()["id"], timeout=120)
    assert done["state"] == "succeeded", done
    out = st.http.get(f"/v1/jobs/{done['id']}/output", headers=st.admin)
    assert out.status_code == 200
    doc = out.json()
    assert doc["nbformat"] == 4 and any("nb cell" in "".join(o.get("text", "")) for c in doc["cells"] for o in c.get("outputs", []))
    assert "data_b64" not in json.dumps(done)


def test_secrets_reach_the_job_only_just_in_time(real: Any) -> None:
    st, _agent = real
    ws = st.workspace()
    sec = st.http.post("/v1/secrets", headers=st.admin, json={"name": "real-sec", "value": "s3cr3t-value-123"}).json()
    j = submit(st, ws, "import os\nprint('len', len(os.environ['MY_SECRET']))\nprint('value', os.environ['MY_SECRET'])\n", secret_refs={"MY_SECRET": sec["id"]})
    done = st.wait_job(j["id"], timeout=90)
    assert done["state"] == "succeeded"
    logs = st.logs(j["id"])
    assert "len 16" in logs and "s3cr3t-value-123" not in logs  # redacted on the way into the log store
    assert "s3cr3t-value-123" not in json.dumps(done) and "s3cr3t-value-123" not in json.dumps(st.job(j["id"])["timeline"])


def test_interactive_session_on_a_real_kernel(real: Any) -> None:
    st, _agent = real
    ws = st.workspace()
    r = st.http.post("/v1/sessions", headers=st.admin, json={"workspace_id": ws, "engine": "python", "runtime": {"runtime_id": RT}})
    assert r.status_code == 201, r.text
    sid = r.json()["id"]
    try:
        wait_until(lambda: st.http.get(f"/v1/sessions/{sid}", headers=st.admin).json()["state"] == "idle", 90, 0.5, "the kernel to become idle")
        st.http.post(f"/v1/sessions/{sid}/execute", headers=st.admin, json={"cell_id": "k1", "code": "a = 40\nprint('answer', a + 2)\na + 2"})
        seen: list[dict[str, Any]] = []
        pos = 0

        def got_reply() -> bool:
            nonlocal pos
            d = st.http.get(f"/v1/sessions/{sid}/events", headers=st.admin, params={"after_seq": pos, "wait": 1.0}, timeout=30).json()
            pos = d["next_seq"]
            seen.extend(d["items"])
            return any(m["type"] == "reply" and m.get("cell_id") == "k1" for m in seen)

        wait_until(got_reply, 60, 0.0, "the reply to the first cell")
        outs = [m["output"] for m in seen if m["type"] == "output"]
        assert any(o["output_type"] == "stream" and "answer 42" in o["text"] for o in outs)
        assert any(o["output_type"] == "execute_result" and o["data"]["text/plain"] == "42" for o in outs)
        # state persists between cells; errors come back as error outputs
        st.http.post(f"/v1/sessions/{sid}/execute", headers=st.admin, json={"cell_id": "k2", "code": "a * 2"})
        st.http.post(f"/v1/sessions/{sid}/execute", headers=st.admin, json={"cell_id": "k3", "code": "1/0"})
        wait_until(lambda: got_reply() and {m.get("cell_id") for m in seen if m["type"] == "reply"} >= {"k1", "k2", "k3"}, 60, 0.0, "replies of cells 2 and 3")
        outs = [(m.get("cell_id"), m["output"]) for m in seen if m["type"] == "output"]
        assert any(c == "k2" and o["output_type"] == "execute_result" and o["data"]["text/plain"] == "80" for c, o in outs)
        assert any(c == "k3" and o["output_type"] == "error" and o["ename"] == "ZeroDivisionError" for c, o in outs)
        # completion through the real kernel
        c = st.http.post(f"/v1/sessions/{sid}/complete", headers=st.admin, json={"code": "prin", "cursor": 4})
        assert c.status_code == 200 and "print" in json.dumps(c.json())
    finally:
        d = st.http.delete(f"/v1/sessions/{sid}", headers=st.admin)
        assert d.status_code == 200, d.text
    last = wait_until(lambda: (s := st.http.get(f"/v1/sessions/{sid}", headers=st.admin).json())["state"] == "dead" and s, 30, 0.3, f"the session to end ({d.text})")
    assert last["state_reason"], last


def test_agent_restart_mid_job_adopts_the_job_and_delivers_the_result(real: Any) -> None:
    """The reconcile path with the production agent: the agent process is stopped while a job runs, restarted from its state dir, adopts the job and reports it."""
    st, agent = real
    ws = st.workspace()
    j = submit(st, ws, "import time\nprint('phase1', flush=True)\ntime.sleep(12)\nprint('phase2', flush=True)\nEXIT_VALUE = 'survived'\n")
    st.wait_job(j["id"], "running", timeout=60)
    wait_until(lambda: "phase1" in st.logs(j["id"]), 30, 0.3, "the job to start printing")
    agent.stop()  # SIGTERM: running jobs are left running (only --embedded stops its children)
    wait_until(lambda: next(w for w in st.http.get("/v1/workers", headers=st.admin).json()["items"] if w["name"] == "real-1")["state"] == "offline", 30, 0.3, "the worker to go offline")
    assert st.job(j["id"])["state"] == "running", "an offline node does not fail its job inside the worker-lost grace period"
    cmd = [sys.executable, "-m", "olcf.worker.main", "run", "--url", st.url, "--name", "real-1", "--state-dir", str(agent.state_dir)]
    env = {k: v for k, v in os.environ.items() if k != "OLCF_WORKER_TOKEN"}
    env["PYTHONPATH"] = os.pathsep.join(os.path.abspath(p) for p in (env.get("PYTHONPATH") or str(ROOT / "src")).split(os.pathsep) if p)
    with open(agent.log, "ab") as out:
        agent.proc = subprocess.Popen(cmd, stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, env=env, start_new_session=True, cwd=str(agent.state_dir.parent))  # noqa: S603
    done = st.wait_job(j["id"], timeout=90)
    assert done["state"] == "succeeded" and done["exit_value"] == "survived", done
    assert done["attempt"] == 1, "the job must not be re-run after an agent restart"
    logs = st.logs(j["id"])
    assert logs.count("phase1") == 1 and "phase2" in logs


def test_credentials_are_renewed_over_the_channel_by_the_real_agent(real: Any) -> None:
    """The production agent end to end: a certificate inside its renewal window and a token past the max age are renewed by the agent on its own timer (CSR over the
    authenticated channel), the new files are swapped in atomically, the superseded credentials are revoked once the node reconnects with the new ones, and the operator
    command does the same on demand."""
    import ssl

    from olcf.worker import identity
    from tests.compute.test_worker_credentials import _put_setting, _rejected

    st, agent = real
    sd = agent.state_dir
    w = next(x for x in st.http.get("/v1/workers", headers=st.admin).json()["items"] if x["name"] == "real-1")
    wid = w["id"]
    reg = st.compute.registry

    def held() -> dict[str, Any]:
        return {"token": json.loads((sd / "identity.json").read_text())["worker_token"], "cert": identity.cert_info(sd), "key": (sd / "client.key").read_text()}

    before = held()
    assert before["cert"] and reg.get_worker(wid)["cert_serial"] == before["cert"]["serial"], "the controller issued exactly the certificate the agent holds"
    assert 89 < (from_iso(w["cert_expires_at"]).timestamp() - time.time()) / 86400 < 91
    reconnects = {"n": 0}

    def reconnect() -> None:
        reconnects["n"] += 1
        st.compute.hub.disconnect(wid, 1012, "test: reconnect")  # a service-restart close: the agent reconnects at once (not an authentication failure)
        wait_until(lambda: not reg.get_worker(wid)["connected"], 20, 0.1, "the channel to close")
        wait_until(lambda: reg.get_worker(wid)["connected"] and reg.get_worker(wid)["state"] == "online", 60, 0.2, "the agent to reconnect")

    try:
        # ---- automatic: certificate inside the window, token older than the new max age
        _put_setting(st, "compute.worker_token_max_age_days", 30)
        reg.update_worker(wid, cert_expires_at=to_iso(datetime.now(UTC) + timedelta(days=5)))
        reconnect()  # the hello acknowledgement now says: renew immediately
        wait_until(lambda: reg.get_worker(wid)["cert_serial"] != before["cert"]["serial"], 60, 0.3, "the agent to renew its certificate on its own")
        # the agent rewrites identity.json (token) first and swaps client.key / client.crt right after (identity.py): wait for the whole swap, not just the first file
        after = wait_until(lambda: (h := held())["token"] != before["token"] and h["cert"] and h["cert"]["serial"] == reg.get_worker(wid)["cert_serial"] and not list(sd.glob("*.next")) and h,
                           30, 0.3, "the agent to store the rotated token and swap in the new certificate pair")
        assert after["cert"]["serial"] == reg.get_worker(wid)["cert_serial"] and after["key"] != before["key"]
        assert not list(sd.glob("*.next")), "no half-finished swap is left behind"
        ssl.create_default_context().load_cert_chain(str(sd / "client.crt"), str(sd / "client.key"))  # the new certificate and the new key belong together
        assert (sd / "client.key").stat().st_mode & 0o077 == 0 and (sd / "identity.json").stat().st_mode & 0o077 == 0
        assert 29 < (from_iso(reg.get_worker(wid)["token_expires_at"]).timestamp() - time.time()) / 86400 < 31
        status = json.loads((sd / "agent-status.json").read_text())
        assert status["credentials"]["last_renewal"]["reason"] == "scheduled" and set(status["credentials"]["last_renewal"]["renewed"]) == {"certificate", "token"}
        # the superseded credentials work until the node comes back with the new ones, and are revoked then
        assert before["cert"]["serial"] not in st.compute.hub.revoked_serials() and not _rejected(st, before["token"])
        reconnect()
        wait_until(lambda: before["cert"]["serial"] in st.compute.hub.revoked_serials(), 30, 0.3, "the old certificate to be revoked")
        wait_until(lambda: _rejected(st, before["token"]), 30, 0.3, "the old token to be revoked")
        assert after["cert"]["serial"] not in st.compute.hub.revoked_serials()
        assert reg.get_worker(wid)["state"] == "online"
        # ---- on demand: `olcf worker renew` through the same exchange
        r = st.http.post(f"/v1/workers/{wid}/renew", headers=st.admin, json={})
        assert r.status_code == 200, r.text
        assert set(r.json()["renewed"]) == {"certificate", "token"}
        again = held()
        assert again["cert"]["serial"] == reg.get_worker(wid)["cert_serial"] != after["cert"]["serial"] and again["token"] != after["token"]
        assert json.loads((sd / "agent-status.json").read_text())["credentials"]["last_renewal"]["reason"] == "requested"
        reconnect()
        wait_until(lambda: _rejected(st, after["token"]), 30, 0.3, "the previous token to be revoked")
        # jobs still run with the renewed credentials
        ws = st.workspace()
        done = st.wait_job(submit(st, ws, "print('after renewal')\n")["id"], timeout=90)
        assert done["state"] == "succeeded", done
    finally:
        _put_setting(st, "compute.worker_token_max_age_days", 0)


# keep last: a failed verification marks the runtime not-ready on the node, which would hold back every later job
def test_runtime_verify_goes_through_the_real_runtime_manager(real: Any) -> None:
    st, _agent = real
    res = st.http.post(f"/v1/runtimes/{RT}/verify", headers=st.admin, json={"worker_ids": ["real-1"]})
    assert res.status_code == 202, res.text
    op = wait_until(lambda: (o := st.http.get(f"/v1/operations/{res.json()['operation_id']}", headers=st.admin).json())["state"] in ("succeeded", "failed") and o, 120, 0.5, "verify")
    node = op["result"]["nodes"][0]
    # the stand-in runtime is neither signed nor installed by the manager, so the real manager refuses it: an explicit, per-node, explained failure (fail closed)
    assert op["state"] == "failed" and node["name"] == "real-1" and node["ok"] is False
    assert "failed verification" in node["error"] and node["details"]["error"]["code"] == "runtime_verify_failed"
