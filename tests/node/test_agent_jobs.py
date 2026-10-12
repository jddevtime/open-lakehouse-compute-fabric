# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Job execution and delivery semantics of the worker agent (real runner processes, fake runtime without Spark, fake hub).

Covers: success/failure/exit value, failure classification, idempotency, conflicts, admission control, cancel (graceful,
escalation, force, unknown job), timeout, source integrity, secret handling (not on disk after the job, not in logs), ordered
log streaming across reconnects, implicit vs explicit acks, rejected logs, terminal-state redelivery, controller restart while a
job runs, orphan adoption after an agent restart, and job-kind coverage (sql-less kinds that need no JVM: script, shell, notebook
parsing errors).
"""

# ruff: noqa: S105, S106
from __future__ import annotations

import base64
import json
import os
import time
from pathlib import Path

import psutil
import pytest

from tests.node.conftest import job_spec, wait_final

W = "wrk_1"
LONG = "import time\nprint('started', flush=True)\ntime.sleep(300)\n"


def start_job(ctl, job_id, src, **over):
    return ctl.ok(W, "run_job", job_spec(job_id, src, **over))


def job_pid(ctl, job_id):
    return ctl.wait_for(lambda: next((j["pid"] for j in (ctl.heartbeats.get(W) or {}).get("active_jobs", []) if j["job_id"] == job_id and j.get("pid")), None) or
                        next((j["pid"] for _, h in ctl.hellos[-1:] for j in h["active_jobs"] if j["job_id"] == job_id and j.get("pid")), None), 20, f"pid of {job_id}")


def wait_log(ctl, job_id, needle, timeout=30.0, stream="stdout"):
    ctl.wait_for(lambda: needle in ctl.log_text(job_id, stream), timeout, f"{needle!r} in {job_id} {stream}")


def gone(pid, timeout=15.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        try:
            if not psutil.pid_exists(pid) or psutil.Process(pid).status() == psutil.STATUS_ZOMBIE:
                return True
        except psutil.Error:
            return True
        time.sleep(0.1)
    return False


# --------------------------------------------------------------------------------------------- outcomes
def test_success_with_exit_value_logs_and_events(controller, start_agent, fake_state):
    start_agent()
    src = "import sys\nprint('hello stdout')\nprint('hello stderr', file=sys.stderr)\nEXIT_VALUE = {'rows': 3, 'ok': True}\n"
    res = start_job(controller, "ok1", src, parameters={"x": 1})
    assert res["state"] == "running" and res["pid"] and res["attempt"] == 1
    fin = wait_final(controller, "ok1")
    assert fin["state"] == "succeeded" and fin["exit_code"] == 0 and fin["failure"] is None
    assert fin["result"]["exit_value"] == {"rows": 3, "ok": True}
    assert fin["runtime_id"] == "olcf-rt-fake-1" and fin["attempt"] == 1 and fin["started_at"] and fin["finished_at"]
    wait_log(controller, "ok1", "hello stdout")
    wait_log(controller, "ok1", "hello stderr", stream="stderr")
    assert "@@OLCF@@" not in controller.log_text("ok1")  # protocol lines are events, not log text
    assert {e["event"] for e in controller.job_events if e["job_id"] == "ok1"} >= {"runner_started", "runner_finished"}
    assert all(e.get("seq") is not None for e in controller.job_events if e["job_id"] == "ok1")
    # the job is no longer reported as running
    hb = controller.wait_for(lambda: controller.heartbeats.get(W), 5)
    controller.wait_for(lambda: all(j["job_id"] != "ok1" for j in controller.heartbeats[W]["active_jobs"]), 10, "job gone from heartbeats")
    assert hb


def test_notebook_exit_via_notebookutils_in_script(controller, start_agent):
    start_agent()
    start_job(controller, "exit1", "notebookutils.notebook.exit({'answer': 42})\nprint('not reached')\n")
    fin = wait_final(controller, "exit1")
    assert fin["state"] == "succeeded" and fin["result"]["exit_value"] == {"answer": 42}
    assert "not reached" not in controller.log_text("exit1")


def test_parameters_are_visible_to_scripts(controller, start_agent):
    start_agent()
    start_job(controller, "par1", "print('p=', PARAMETERS['n'], n)\nEXIT_VALUE = n * 2\n", parameters={"n": 21})
    fin = wait_final(controller, "par1")
    assert fin["result"]["exit_value"] == 42
    wait_log(controller, "par1", "p= 21 21")


@pytest.mark.parametrize(("name", "src", "category"), [
    ("f-user", "raise ValueError('boom')\n", "user-code"),
    ("f-dep", "import module_that_does_not_exist_abc\n", "dependency"),
    ("f-exit", "import sys\nsys.exit(3)\n", "user-code"),
    ("f-syntax", "def broken(:\n", "user-code"),
    ("f-storage", "raise OSError('AbfsRestOperationException: container not found')\n", "storage"),
])
def test_failures_are_classified(controller, start_agent, name, src, category):
    start_agent()
    start_job(controller, name, src)
    fin = wait_final(controller, name)
    assert fin["state"] == "failed" and fin["exit_code"] == 1
    assert fin["failure"]["category"] == category, fin["failure"]
    assert fin["failure"]["message"] and fin["failure"]["remediation"]


def test_failure_has_traceback_and_stderr_logged(controller, start_agent):
    start_agent()
    start_job(controller, "tb1", "def f():\n    raise KeyError('missing-key')\nf()\n")
    fin = wait_final(controller, "tb1")
    assert fin["failure"]["category"] == "user-code"
    assert "KeyError" in (fin["failure"].get("traceback") or fin["failure"]["message"])


def test_shell_script_job(controller, start_agent):
    if os.name == "nt":
        pytest.skip("posix shell")
    start_agent()
    ctl = controller
    payload = job_spec("sh1", "echo from-shell\nexit 0\n", fmt="script")
    payload["spec"]["source"]["path"] = "source/run.sh"
    payload["source"]["path"] = "source/run.sh"
    ctl.ok(W, "run_job", payload)
    assert wait_final(ctl, "sh1")["state"] == "succeeded"
    wait_log(ctl, "sh1", "from-shell")
    payload = job_spec("sh2", "echo bad >&2\nexit 7\n", fmt="script")
    payload["spec"]["source"]["path"] = "source/run.sh"
    payload["source"]["path"] = "source/run.sh"
    ctl.ok(W, "run_job", payload)
    assert wait_final(ctl, "sh2")["state"] == "failed"


def test_unsupported_notebook_content_is_a_compatibility_failure(controller, start_agent):
    start_agent()
    nb = {"nbformat": 3, "cells": []}
    payload = job_spec("nb-bad", json.dumps(nb), kind="notebook", fmt="ipynb")
    controller.ok(W, "run_job", payload)
    fin = wait_final(controller, "nb-bad")
    assert fin["state"] == "failed" and fin["failure"]["category"] == "compatibility"


def test_unknown_runtime_is_refused_before_spawning(controller, start_agent):
    start_agent()
    res = controller.request(W, "run_job", job_spec("rt-missing", "print(1)", runtime="olcf-rt-nope"))
    assert res["ok"] is False and res["error"]["code"] == "runtime_not_ready"
    res = controller.request(W, "run_job", {"spec": {**job_spec("rt-none", "print(1)")["spec"], "runtime": {}}})
    assert res["ok"] is False and res["error"]["code"] == "bad_request"


def test_source_hash_mismatch_is_rejected(controller, start_agent, fake_state):
    start_agent()
    payload = job_spec("hash1", "print('x')\n")
    payload["source"]["content"] = "print('tampered')\n"  # sha256 no longer matches
    res = controller.request(W, "run_job", payload)
    assert res["ok"] is False and res["error"]["code"] == "source_hash_mismatch"
    assert not (fake_state / "work" / "hash1").exists()  # nothing left behind
    payload = job_spec("hash2", "print('x')\n")
    payload["spec"]["source"]["sha256"] = "0" * 64
    assert controller.request(W, "run_job", payload)["error"]["code"] == "source_hash_mismatch"


def test_source_path_traversal_is_rejected(controller, start_agent):
    start_agent()
    payload = job_spec("trav1", "print('x')\n")
    payload["spec"]["source"]["path"] = "../../escape.py"
    payload["source"]["path"] = "../../escape.py"
    res = controller.request(W, "run_job", payload)
    assert res["ok"] is False
    payload = job_spec("trav2", "print('x')\n")
    payload["files"] = [{"path": "/etc/olcf-escape", "content": "x"}]
    assert controller.request(W, "run_job", payload)["ok"] is False
    assert not Path("/etc/olcf-escape").exists()


def test_base64_source_and_extra_files(controller, start_agent):
    start_agent()
    code = "import pathlib\nEXIT_VALUE = pathlib.Path('data/in.txt').read_text()\n"
    payload = job_spec("b64", code)
    payload["files"] = [{"path": "data/in.txt", "encoding": "base64", "content": base64.b64encode(b"from-file").decode()}]
    controller.ok(W, "run_job", payload)
    assert wait_final(controller, "b64")["result"]["exit_value"] == "from-file"


# --------------------------------------------------------------------------------------------- idempotency, conflicts, admission
def test_duplicate_and_conflicting_attempts(controller, start_agent):
    start_agent()
    first = start_job(controller, "dup1", LONG)
    again = controller.ok(W, "run_job", job_spec("dup1", LONG))
    assert again["duplicate"] is True and again["pid"] == first["pid"]
    res = controller.request(W, "run_job", job_spec("dup1", LONG, attempt=2))
    assert res["ok"] is False and res["error"]["code"] == "conflict"  # attempt 1 still alive on this node
    controller.ok(W, "cancel_job", {"job_id": "dup1"})
    assert wait_final(controller, "dup1")["state"] == "cancelled"
    # once the old attempt is over a retry attempt is accepted
    controller.ok(W, "run_job", job_spec("dup1", "print('retry')\n", attempt=2))
    fin = wait_final(controller, "dup1")
    controller.wait_for(lambda: any(p["attempt"] == 2 and p["state"] == "succeeded" for p in controller.job_states["dup1"]), 30, "attempt 2 final")
    assert fin


def test_admission_control_concurrent_job_limit(controller, start_agent):
    start_agent(max_jobs=1)
    start_job(controller, "adm1", LONG)
    res = controller.request(W, "run_job", job_spec("adm2", "print(1)"))
    assert res["ok"] is False and res["error"]["code"] == "resource_exhausted"
    controller.ok(W, "cancel_job", {"job_id": "adm1", "force": True})
    wait_final(controller, "adm1")
    start_job(controller, "adm3", "print(3)\n")
    assert wait_final(controller, "adm3")["state"] == "succeeded"


def test_admission_control_cores_and_memory(controller, start_agent):
    start_agent(max_cores=2, max_memory_mb=2048, max_jobs=10)
    p = job_spec("res1", LONG)
    p["resources"] = {"cores": 2, "memory_mb": 1500}
    controller.ok(W, "run_job", p)
    p2 = job_spec("res2", LONG)
    p2["resources"] = {"cores": 1, "memory_mb": 256}
    res = controller.request(W, "run_job", p2)
    assert res["ok"] is False and res["error"]["code"] == "resource_exhausted"
    assert res["error"]["details"]["requested"] == {"cores": 1, "memory_mb": 256} and res["error"]["details"]["free"]["cores"] == 0
    hb = controller.wait_for(lambda: (controller.heartbeats.get(W) or {}).get("allocated", {}).get("cores") == 2 and controller.heartbeats[W], 10, "allocation in heartbeat")
    assert hb["allocated"] == {"cores": 2, "memory_mb": 1500} and hb["capacity"] == {"cores": 2, "memory_mb": 2048}
    controller.ok(W, "cancel_job", {"job_id": "res1", "force": True})
    wait_final(controller, "res1")


# --------------------------------------------------------------------------------------------- cancel / timeout
def test_cancel_graceful(controller, start_agent):
    start_agent()
    start_job(controller, "can1", LONG)
    wait_log(controller, "can1", "started")
    pid = job_pid(controller, "can1")
    res = controller.ok(W, "cancel_job", {"job_id": "can1"})
    assert res["found"] is True and res["state"] == "cancelling"
    fin = wait_final(controller, "can1")
    assert fin["state"] == "cancelled" and fin["failure"]["category"] == "cancellation"
    assert gone(pid)
    # idempotent
    again = controller.ok(W, "cancel_job", {"job_id": "can1"})
    assert again["found"] is True and again["state"] == "cancelled"


def test_cancel_unknown_and_missing_id(controller, start_agent):
    start_agent()
    assert controller.ok(W, "cancel_job", {"job_id": "nope"}) == {"job_id": "nope", "found": False, "state": "unknown"}
    res = controller.request(W, "cancel_job", {})
    assert res["ok"] is False and res["error"]["code"] == "bad_request"


STUBBORN = ("import signal, time\nsignal.signal(signal.SIGTERM, signal.SIG_IGN)\nprint('stubborn up', flush=True)\n"
            "while True:\n    time.sleep(0.2)\n")


def test_cancel_escalates_to_sigkill_after_grace(controller, start_agent):
    start_agent()
    start_job(controller, "stub1", STUBBORN)
    wait_log(controller, "stub1", "stubborn up")
    pid = job_pid(controller, "stub1")
    t0 = time.monotonic()
    controller.ok(W, "cancel_job", {"job_id": "stub1", "grace_seconds": 1})
    fin = wait_final(controller, "stub1", 60)
    assert fin["state"] == "cancelled" and time.monotonic() - t0 < 40
    assert fin["exit_code"] == -9 and gone(pid)


def test_force_cancel_is_immediate(controller, start_agent):
    start_agent()
    start_job(controller, "force1", STUBBORN)
    wait_log(controller, "force1", "stubborn up")
    pid = job_pid(controller, "force1")
    controller.ok(W, "cancel_job", {"job_id": "force1", "force": True})
    fin = wait_final(controller, "force1", 30)
    assert fin["state"] == "cancelled" and fin["exit_code"] == -9 and gone(pid)


def test_cancel_kills_the_whole_process_tree(controller, start_agent, tmp_path):
    start_agent()
    marker = tmp_path / "grandchild.pid"
    src = (f"import subprocess, sys, time\nc = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(300)'])\n"
           f"open({str(marker)!r}, 'w').write(str(c.pid))\nprint('tree up', flush=True)\ntime.sleep(300)\n")
    start_job(controller, "tree1", src)
    wait_log(controller, "tree1", "tree up")
    gc = int(controller.wait_for(lambda: marker.read_text() if marker.exists() and marker.read_text() else None, 15, "grandchild pid"))
    assert psutil.pid_exists(gc)
    controller.ok(W, "cancel_job", {"job_id": "tree1", "force": True})
    wait_final(controller, "tree1")
    assert gone(gc), "grandchild process survived the cancel"


def test_leftover_processes_of_a_finished_job_are_reaped(controller, start_agent, tmp_path):
    start_agent()
    marker = tmp_path / "leftover.pid"
    src = (f"import subprocess, sys\nc = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(300)'])\n"
           f"open({str(marker)!r}, 'w').write(str(c.pid))\nEXIT_VALUE = 'parent-done'\n")  # the runner exits, its child would live on
    start_job(controller, "left1", src)
    fin = wait_final(controller, "left1")
    assert fin["state"] == "succeeded" and fin["result"]["exit_value"] == "parent-done"
    assert gone(int(marker.read_text())), "a process started by the job survived the job"


def test_timeout(controller, start_agent):
    start_agent()
    start_job(controller, "to1", LONG, limits={"timeout_seconds": 3})
    t0 = time.monotonic()
    fin = wait_final(controller, "to1", 60)
    assert fin["state"] == "timed_out" and fin["failure"]["category"] == "timeout"
    assert 2 < time.monotonic() - t0 < 40
    assert fin["failure"]["remediation"]


def test_timeout_of_a_job_that_ignores_sigterm(controller, start_agent):
    start_agent(kill_grace_seconds=1.0)
    start_job(controller, "to2", STUBBORN, limits={"timeout_seconds": 2})
    fin = wait_final(controller, "to2", 60)
    assert fin["state"] == "timed_out", fin


# --------------------------------------------------------------------------------------------- secrets
def test_secrets_never_reach_disk_after_the_job_nor_logs(controller, start_agent, fake_state):
    start_agent()
    secret = "s3cr3t-token-value-ABC123"
    src = "import os\nprint('token=' + os.environ['MY_SECRET'])\nprint('plain env:', os.environ.get('PLAIN'))\nEXIT_VALUE = len(os.environ['MY_SECRET'])\n"
    start_job(controller, "sec1", src, secrets={"MY_SECRET": secret}, env={"PLAIN": "visible"},
              storage_conf={"spark": {"fs.azure.account.key.x": "storage-key-value-999"}, "env": {"AWS_SECRET_ACCESS_KEY": "aws-secret-value-777"}})
    fin = wait_final(controller, "sec1")
    assert fin["state"] == "succeeded" and fin["result"]["exit_value"] == len(secret)  # the job did see the secret
    wait_log(controller, "sec1", "plain env: visible")
    log = controller.log_text("sec1")
    assert secret not in log and "token=" in log
    wd = fake_state / "work" / "sec1"
    assert not (wd / "job.json").exists()
    # the job printed its own secret: the raw spool is masked as soon as the output is fully delivered
    controller.wait_for(lambda: secret.encode() not in (wd / "stdout.log").read_bytes(), 20, "raw stdout.log redacted after delivery")
    blob = b""
    for p in wd.rglob("*"):
        if p.is_file():
            blob += p.read_bytes()
    for s in (secret, "storage-key-value-999", "aws-secret-value-777"):
        assert s.encode() not in blob, f"{s} left on disk"
    pub = json.loads((wd / "job.public.json").read_text())
    assert "secrets" not in pub and "storage_conf" not in pub and pub["has_secrets"] is True
    assert json.dumps(controller.frames_of("job_state")).count(secret) == 0
    assert all(secret not in json.dumps(f) for f in controller.frames_of("job_event"))


def test_job_environment_is_scrubbed(controller, start_agent, monkeypatch):
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "agent-env-secret-xyz")
    monkeypatch.setenv("OLCF_WORKER_TOKEN", "agent-token-xyz")
    monkeypatch.setenv("PYTHONPATH", "/definitely/not/here")
    start_agent()
    start_job(controller, "env1", "import os, json\nEXIT_VALUE = {k: v for k, v in os.environ.items()}\n")
    env = wait_final(controller, "env1")["result"]["exit_value"]
    assert "AWS_SECRET_ACCESS_KEY" not in env and "OLCF_WORKER_TOKEN" not in env
    assert "/definitely/not/here" not in env.get("PYTHONPATH", "")
    assert env["OLCF_JOB_ID"] == "env1" and env["HOME"].endswith("/home") and env["OLCF_JOB_DIR"].endswith("/env1")
    assert "agent-env-secret-xyz" not in json.dumps(env) and "agent-token-xyz" not in json.dumps(env)


def test_workdir_permissions(controller, start_agent, fake_state):
    if os.name == "nt":
        pytest.skip("posix permissions")
    start_agent()
    start_job(controller, "perm1", "print(1)\n", secrets={"A": "perm-secret-value"})
    wait_final(controller, "perm1")
    wd = fake_state / "work" / "perm1"
    assert (wd.stat().st_mode & 0o077) == 0
    assert ((wd / "home").stat().st_mode & 0o077) == 0
    assert ((wd / "agent-job.json").stat().st_mode & 0o077) == 0


# --------------------------------------------------------------------------------------------- log streaming
def test_logs_are_ordered_and_complete_across_reconnects(controller, start_agent):
    start_agent()
    src = "import time\nfor i in range(40):\n    print(f'line-{i:03d}', flush=True)\n    time.sleep(0.15)\n"
    start_job(controller, "log1", src)
    wait_log(controller, "log1", "line-005")
    controller.drop(W)
    n = controller.connect_count[W]
    controller.wait_for(lambda: controller.connect_count[W] > n or controller.connected(W) and controller.connect_count[W] >= 2, 30, "reconnect")
    wait_log(controller, "log1", "line-020")
    controller.drop(W)
    fin = wait_final(controller, "log1", 90)
    assert fin["state"] == "succeeded"
    controller.wait_for(lambda: "line-039" in controller.log_text("log1"), 30, "last line")
    text = controller.log_text("log1")
    assert text == "".join(f"line-{i:03d}\n" for i in range(40))  # no gaps, no reordering, no duplicates after dedupe by (job, stream, seq)
    seqs = controller.log_seqs("log1")
    assert seqs == list(range(len(seqs)))


def test_large_output_is_chunked_within_limits(controller, start_agent):
    start_agent()
    src = "line = 'x' * 99\nfor i in range(6000):\n    print(line)\n"  # ~600 KB
    start_job(controller, "big1", src)
    wait_final(controller, "big1")
    controller.wait_for(lambda: len(controller.log_text("big1")) >= 6000 * 100, 30, "all output")
    frames = [f["payload"] for f in controller.frames_of("job_log") if f["payload"]["job_id"] == "big1"]
    assert max(len(f["data"].encode()) for f in frames) <= 64 * 1024
    assert controller.log_text("big1") == ("x" * 99 + "\n") * 6000
    assert all("attempt" in f and "offset" in f for f in frames)


def test_implicit_acks_when_controller_sends_none(controller, start_agent):
    controller.ack_enabled = False  # a controller that never acknowledges frames
    start_agent(implicit_log_ack_seconds=1.0)
    start_job(controller, "imp1", "print('implicit')\n")
    fin = wait_final(controller, "imp1")
    assert fin["state"] == "succeeded"
    wait_log(controller, "imp1", "implicit")
    time.sleep(3.5)  # the connection stayed up past the implicit window: chunks count as delivered
    n = controller.connect_count[W]
    dups0 = controller.log_dups
    controller.drop(W)
    controller.wait_for(lambda: controller.connect_count[W] > n, 30, "reconnect")
    time.sleep(2.0)
    assert controller.log_dups == dups0  # nothing re-sent


def test_unconfirmed_logs_are_resent_after_reconnect(controller, start_agent):
    controller.ack_enabled = False
    start_agent(implicit_log_ack_seconds=60.0)
    start_job(controller, "resend1", "print('keep me')\n")
    wait_final(controller, "resend1")
    wait_log(controller, "resend1", "keep me")
    n = controller.connect_count[W]
    controller.drop(W)
    controller.wait_for(lambda: controller.connect_count[W] > n, 30, "reconnect")
    controller.wait_for(lambda: controller.log_dups >= 1, 20, "re-sent chunk (deduplicated by the store)")
    assert controller.log_text("resend1").count("keep me") == 1


def test_rejected_logs_are_not_retried_forever(controller, start_agent):
    controller.reject_log_jobs.add("rej1")
    start_agent()
    start_job(controller, "rej1", "print('nobody wants me')\n")
    fin = wait_final(controller, "rej1")
    assert fin["state"] == "succeeded"
    time.sleep(3)
    sent = [f for f in controller.frames_of("job_log") if f["payload"]["job_id"] == "rej1"]
    assert 1 <= len(sent) <= 2, f"{len(sent)} resends of a rejected log"
    start_job(controller, "rej2", "print('fine')\n")  # other jobs are unaffected
    wait_final(controller, "rej2")
    wait_log(controller, "rej2", "fine")


# --------------------------------------------------------------------------------------------- terminal state delivery / reconcile
def test_terminal_state_is_redelivered_until_acknowledged(controller, start_agent):
    controller.ack_enabled = False
    start_agent()
    start_job(controller, "term1", "EXIT_VALUE = 'v'\n")
    wait_final(controller, "term1")
    n = controller.connect_count[W]
    controller.drop(W)
    controller.wait_for(lambda: controller.connect_count[W] > n, 30, "reconnect 1")
    hello = controller.hellos[-1][1]
    entry = next(j for j in hello["active_jobs"] if j["job_id"] == "term1")
    assert entry["state"] == "succeeded" and entry["undelivered"] is True and entry["exit_code"] == 0
    controller.wait_for(lambda: len([p for p in controller.job_states["term1"] if p["state"] == "succeeded"]) >= 2, 20, "terminal job_state re-sent")
    # the controller starts acknowledging: the next reconnect delivers once more, after that the job is forgotten
    controller.ack_enabled = True
    n = controller.connect_count[W]
    controller.drop(W)
    controller.wait_for(lambda: controller.connect_count[W] > n, 30, "reconnect 2")
    controller.wait_for(lambda: len([p for p in controller.job_states["term1"] if p["state"] == "succeeded"]) >= 3, 20, "job_state delivered and acked")
    time.sleep(0.5)
    n = controller.connect_count[W]
    controller.drop(W)
    controller.wait_for(lambda: controller.connect_count[W] > n, 30, "reconnect 3")
    assert all(j["job_id"] != "term1" for j in controller.hellos[-1][1]["active_jobs"])


def test_job_finishing_while_disconnected_is_reported_after_reconnect(controller, start_agent):
    start_agent()
    start_job(controller, "off1", "import time\nprint('begin', flush=True)\ntime.sleep(4)\nprint('end', flush=True)\nEXIT_VALUE = 'done-offline'\n")
    wait_log(controller, "off1", "begin")
    controller.stop()  # controller down for the whole remaining runtime of the job
    time.sleep(6)
    assert controller.final_state("off1") is None
    controller.start()
    controller.wait_connected(W, 60)
    hello = controller.hellos[-1][1]
    assert any(j["job_id"] == "off1" for j in hello["active_jobs"])
    fin = wait_final(controller, "off1", 30)
    assert fin["state"] == "succeeded" and fin["result"]["exit_value"] == "done-offline"
    wait_log(controller, "off1", "end")
    assert controller.log_text("off1") == "begin\nend\n"


def test_running_job_survives_controller_restart_and_is_reconciled(controller, start_agent):
    start_agent()
    src = "import time\nfor i in range(25):\n    print(f'tick-{i:02d}', flush=True)\n    time.sleep(0.2)\nEXIT_VALUE = 'survived'\n"
    start_job(controller, "rst1", src)
    wait_log(controller, "rst1", "tick-02")
    pid = job_pid(controller, "rst1")
    hellos = len(controller.hellos)
    controller.restart()
    controller.wait_for(lambda: len(controller.hellos) > hellos and controller.connected(W), 40, "reconnect after restart")
    entry = next(j for j in controller.hellos[-1][1]["active_jobs"] if j["job_id"] == "rst1")
    assert entry["pid"] == pid and entry["state"] == "running"  # same process: it kept running while the hub was away
    fin = wait_final(controller, "rst1", 60)
    assert fin["state"] == "succeeded" and fin["result"]["exit_value"] == "survived"
    controller.wait_for(lambda: "tick-24" in controller.log_text("rst1"), 30, "last tick")
    assert controller.log_text("rst1") == "".join(f"tick-{i:02d}\n" for i in range(25))


# --------------------------------------------------------------------------------------------- agent restart / adoption
def test_orphan_job_is_adopted_after_agent_restart(controller, start_agent):
    h = start_agent()
    start_job(controller, "adopt1", "import time\nprint('adoptable', flush=True)\nfor i in range(300):\n    print(i, flush=True)\n    time.sleep(1)\n")
    wait_log(controller, "adopt1", "adoptable")
    pid = job_pid(controller, "adopt1")
    h.stop(kill_children=False)
    assert psutil.pid_exists(pid)  # the job outlives the agent
    n = len(controller.hellos)
    start_agent()
    controller.wait_for(lambda: len(controller.hellos) > n, 30, "hello of the new agent")
    entry = next(j for j in controller.hellos[-1][1]["active_jobs"] if j["job_id"] == "adopt1")
    assert entry["pid"] == pid and entry["state"] == "running"
    seq0 = len(controller.log_seqs("adopt1"))
    controller.wait_for(lambda: len(controller.log_seqs("adopt1")) > seq0, 20, "log streaming resumed by the new agent")
    controller.ok(W, "cancel_job", {"job_id": "adopt1", "force": True})
    fin = wait_final(controller, "adopt1", 30)
    assert fin["state"] == "cancelled" and gone(pid)


def test_job_that_finished_while_the_agent_was_down(controller, start_agent):
    h = start_agent()
    start_job(controller, "down1", "import time\nprint('x', flush=True)\ntime.sleep(2)\nEXIT_VALUE = 'finished-while-down'\n")
    wait_log(controller, "down1", "x")
    h.stop(kill_children=False)
    time.sleep(4.5)
    start_agent()
    fin = wait_final(controller, "down1", 30)
    assert fin["state"] == "succeeded" and fin["result"]["exit_value"] == "finished-while-down"


def test_job_that_died_while_the_agent_was_down_is_failed_not_lost(controller, start_agent):
    h = start_agent()
    start_job(controller, "dead1", LONG)
    pid = job_pid(controller, "dead1")
    h.stop(kill_children=False)
    psutil.Process(pid).kill()
    assert gone(pid)
    start_agent()
    fin = wait_final(controller, "dead1", 30)
    assert fin["state"] == "failed" and fin["failure"]["category"] in ("internal", "resource")
    assert "disappeared" in fin["failure"]["message"] or "killed" in fin["failure"]["message"]


def test_agent_stop_kills_children_when_asked(controller, start_agent):
    h = start_agent()
    start_job(controller, "kill1", LONG)
    pid = job_pid(controller, "kill1")
    h.stop(kill_children=True)
    assert gone(pid, 20)


# --------------------------------------------------------------------------------------------- retention
def test_old_delivered_workdirs_are_cleaned(controller, start_agent, fake_state):
    h = start_agent(job_retention_hours=0.0001)
    start_job(controller, "old1", "print(1)\n")
    wait_final(controller, "old1")
    wd = fake_state / "work" / "old1"
    controller.wait_for(lambda: (wd / "agent-job.json").exists(), 5)
    old = time.time() - 3600
    os.utime(wd, (old, old))
    h.agent.jobs.ack("old1")
    assert h.agent.jobs.cleanup_old() == 1
    assert not wd.exists()
