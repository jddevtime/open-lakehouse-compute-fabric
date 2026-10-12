# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Kernel hosting through the agent (real ipykernel processes, real ZeroMQ, fake runtime without Spark).

The Spark side of kernels (SparkSession in the kernel, ``%%sql`` against Spark, ``spark_app_started`` events) is covered by
``test_spark_cluster.py::test_interactive_kernel_session``; here the kernel *service* is tested: lifecycle, event relay, scrubbing,
interrupt/restart/complete, admission, idle culling, crash detection and clean-up, without needing a JVM.
"""

from __future__ import annotations

import threading
import time

import psutil

from tests.node.conftest import job_spec

W = "wrk_1"
SECRET = "kernel-secret-value-42"  # noqa: S105


def kspec(**over):
    sp = job_spec("kern", "", kind="notebook", fmt="ipynb")["spec"]
    sp.update(over)
    return sp


def start(ctl, sid="s1", **extra):
    spec = extra.pop("spec", None) or kspec(secrets={"K_SECRET": SECRET})
    return ctl.ok(W, "start_kernel", {"session_id": sid, "spec": spec, "eager_spark": False, **extra}, timeout=120)


def events(ctl, sid, **flt):
    return [e for e in list(ctl.kernel_events) if e["session_id"] == sid and all(e.get(k) == v for k, v in flt.items())]


def run(ctl, sid, code, mid, timeout=60.0):
    ctl.ok(W, "kernel_execute", {"session_id": sid, "code": code, "msg_id": mid})
    reply = ctl.wait_for(lambda: next((e for e in events(ctl, sid, msg_type="execute_reply") if e["parent_msg_id"] == mid), None), timeout, f"reply to {mid}")
    # The reply travels on the shell channel and the output on iopub, and Jupyter does not order the two: the reply can be seen first.  The request's own "idle" status is
    # the last iopub message of the request, so once it is here every stream/result/error of the cell is too.  (Best effort: a cell that kills its kernel never reports idle.)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if any(e["parent_msg_id"] == mid and e["content"].get("state") == "idle" for e in events(ctl, sid, channel="status", msg_type="status")):
            break
        time.sleep(0.02)
    return reply


def stream_text(ctl, sid, mid):
    return "".join(e["content"]["text"] for e in events(ctl, sid, msg_type="stream") if e["parent_msg_id"] == mid)


def alive(pid):
    try:
        return psutil.pid_exists(pid) and psutil.Process(pid).status() != psutil.STATUS_ZOMBIE
    except psutil.Error:
        return False


def test_lifecycle_events_and_outputs(controller, start_agent, fake_state):
    start_agent()
    info = start(controller)
    assert info["session_id"] == "s1" and info["pid"] and alive(info["pid"])
    controller.wait_for(lambda: any(e["content"].get("state") == "idle" for e in events(controller, "s1", channel="status", msg_type="status")), 60, "idle")
    states = [e["content"]["state"] for e in events(controller, "s1", channel="status", msg_type="status")]
    assert states[0] == "starting" and "idle" in states
    r = run(controller, "s1", "a = 6 * 7\na", "m1")
    assert r["content"]["status"] == "ok" and r["channel"] == "shell"
    res = [e for e in events(controller, "s1", msg_type="execute_result") if e["parent_msg_id"] == "m1"]
    assert res and res[0]["channel"] == "iopub" and res[0]["content"]["data"]["text/plain"] == "42"
    r = run(controller, "s1", "print('hello'); import sys; print('err', file=sys.stderr)", "m2")
    out = events(controller, "s1", msg_type="stream")
    assert {(e["content"]["name"], e["content"]["text"].strip()) for e in out if e["parent_msg_id"] == "m2"} == {("stdout", "hello"), ("stderr", "err")}
    r = run(controller, "s1", "1/0", "m3")
    assert r["content"]["status"] == "error" and r["content"]["ename"] == "ZeroDivisionError"
    err = [e for e in events(controller, "s1", msg_type="error") if e["parent_msg_id"] == "m3"]
    assert err and err[0]["content"]["ename"] == "ZeroDivisionError" and err[0]["content"]["traceback"]
    # state persists between cells, idle/busy are reported around each execution
    run(controller, "s1", "b = a + 1", "m4")
    run(controller, "s1", "print(b)", "m5")
    assert stream_text(controller, "s1", "m5").strip() == "43"
    assert any(e["content"]["state"] == "busy" for e in events(controller, "s1", channel="status", msg_type="status"))
    hb = controller.wait_for(lambda: next((h for h in [controller.heartbeats.get(W)] if h and h.get("kernels")), None), 10)
    assert hb["kernels"][0]["session_id"] == "s1"
    assert {c["session_id"] for c in controller.hellos[-1][1]["kernels"]} == set()  # none at hello time
    # directory layout: private, connection file 0600, loopback only
    kd = fake_state / "kernels" / "s1"
    assert (kd.stat().st_mode & 0o077) == 0
    spec_file = kd / "kernel-spec.json"
    assert spec_file.exists() and (spec_file.stat().st_mode & 0o077) == 0  # contains credentials while the session lives
    assert controller.ok(W, "kernel_shutdown", {"session_id": "s1"})["state"] == "dead"
    assert not alive(info["pid"]) or controller.wait_for(lambda: not alive(info["pid"]), 20)
    assert not (kd / "kernel-spec.json").exists()  # credentials shredded
    assert controller.ok(W, "kernel_shutdown", {"session_id": "s1"}) == {"session_id": "s1", "found": False, "state": "dead"}  # idempotent


def test_secrets_are_scrubbed_before_leaving_the_node(controller, start_agent):
    start_agent()
    start(controller)
    run(controller, "s1", "import os\nprint('value:', os.environ['K_SECRET'])\nos.environ['K_SECRET']", "m1")
    text = stream_text(controller, "s1", "m1")
    assert SECRET not in text and "value:" in text
    assert SECRET not in str(events(controller, "s1")), "secret leaked in some kernel_event"
    run(controller, "s1", "print('Authorization: Bearer abcdefghijklmnopqrstuvwxyz0123456789')", "m2")
    assert "abcdefghijklmnopqrstuvwxyz0123456789" not in stream_text(controller, "s1", "m2")


def test_environment_of_the_kernel_is_scrubbed(controller, start_agent, monkeypatch):
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "agent-env-secret")
    start_agent()
    start(controller)
    run(controller, "s1", "import os\nprint('AWS' , 'AWS_SECRET_ACCESS_KEY' in os.environ, 'OLCF_WORKER_TOKEN' in os.environ, os.environ['HOME'])", "m1")
    out = stream_text(controller, "s1", "m1")
    assert "AWS False False" in out and "/kernels/s1/home" in out


def test_interrupt_restart_and_complete(controller, start_agent):
    start_agent()
    info = start(controller)
    run(controller, "s1", "keep = 1", "m0")
    controller.ok(W, "kernel_execute", {"session_id": "s1", "code": "import time\nprint('sleeping', flush=True)\ntime.sleep(120)", "msg_id": "m1"})
    controller.wait_for(lambda: "sleeping" in stream_text(controller, "s1", "m1"), 30, "cell running")
    controller.ok(W, "kernel_interrupt", {"session_id": "s1"})
    r = controller.wait_for(lambda: next((e for e in events(controller, "s1", msg_type="execute_reply") if e["parent_msg_id"] == "m1"), None), 30, "interrupted reply")
    assert r["content"]["ename"] == "KeyboardInterrupt"
    assert run(controller, "s1", "print(keep)", "m2")["content"]["status"] == "ok"  # the kernel (and its state) survived the interrupt
    comp = controller.ok(W, "kernel_complete", {"session_id": "s1", "code": "ke", "cursor": 2})
    assert comp["status"] == "ok" and "keep" in comp["matches"] and comp["cursor_start"] == 0
    # restart: new interpreter process or fresh state, session id unchanged
    res = controller.ok(W, "kernel_restart", {"session_id": "s1"}, timeout=60)
    assert res["state"] == "idle"
    r = run(controller, "s1", "print('keep' in dir())", "m3")
    assert stream_text(controller, "s1", "m3").strip() == "False"
    assert info["pid"]


def test_restart_twice_interrupt_while_running_and_work_after_each_restart(controller, start_agent):
    """FINALQA-2: restart used to read the client's sockets from two threads (``Invalid Signature: b'{}'``, libzmq abort); it must be repeatable."""
    start_agent()
    pids = [start(controller)["pid"]]
    for n in (1, 2):
        run(controller, "s1", f"state = {n}\nimport os\nprint('pid', os.getpid(), flush=True)", f"a{n}")
        # a cell that is still running when the restart arrives
        controller.ok(W, "kernel_execute", {"session_id": "s1", "code": "import time\nprint('busy', flush=True)\ntime.sleep(120)", "msg_id": f"slow{n}"})
        controller.wait_for(lambda n=n: "busy" in stream_text(controller, "s1", f"slow{n}"), 30, "cell running")
        controller.ok(W, "kernel_interrupt", {"session_id": "s1"})
        r = controller.wait_for(lambda n=n: next((e for e in events(controller, "s1", msg_type="execute_reply") if e["parent_msg_id"] == f"slow{n}"), None), 30, "interrupted")
        assert r["content"]["ename"] == "KeyboardInterrupt"
        assert run(controller, "s1", "print(state)", f"b{n}")["content"]["status"] == "ok" and stream_text(controller, "s1", f"b{n}").strip() == str(n)
        controller.ok(W, "kernel_execute", {"session_id": "s1", "code": "import time\nprint('again', flush=True)\ntime.sleep(120)", "msg_id": f"tail{n}"})
        controller.wait_for(lambda n=n: "again" in stream_text(controller, "s1", f"tail{n}"), 30, "second cell running")
        res = controller.ok(W, "kernel_restart", {"session_id": "s1"}, timeout=90)  # restart *during* an execution
        assert res["state"] == "idle" and res["pid"] and res["pid"] != pids[-1] and not alive(pids[-1])
        pids.append(res["pid"])
        assert run(controller, "s1", "print('state' in dir())", f"c{n}")["content"]["status"] == "ok"
        assert stream_text(controller, "s1", f"c{n}").strip() == "False"            # fresh interpreter
        assert run(controller, "s1", "import os\nprint(os.getpid())", f"d{n}")["content"]["status"] == "ok"
        assert stream_text(controller, "s1", f"d{n}").strip() == str(res["pid"])     # the new process is the one we talk to
        comp = controller.ok(W, "kernel_complete", {"session_id": "s1", "code": "pri", "cursor": 3})
        assert comp["status"] == "ok" and "print" in comp["matches"]
    assert len(set(pids)) == 3
    # the watch loop must still recognise the restarted process (pid and start time are re-recorded): the session does not turn "dead" a few seconds later
    time.sleep(3.5)
    assert run(controller, "s1", "print('alive')", "z1")["content"]["status"] == "ok"
    assert not [e for e in events(controller, "s1", channel="status", msg_type="status") if e["content"].get("state") == "dead"]


def test_commands_racing_a_restart_never_corrupt_the_session(controller, start_agent):
    """Executes / completions issued from several threads while the kernel restarts are either served or refused cleanly (``kernel_restarting``)."""
    start_agent()
    start(controller)
    stop = threading.Event()
    seen: list[str] = []

    def hammer(i: int) -> None:
        n = 0
        while not stop.is_set():
            n += 1
            res = controller.request(W, "kernel_execute", {"session_id": "s1", "code": f"print({i}, {n})", "msg_id": f"h{i}_{n}"}) if i % 2 else \
                controller.request(W, "kernel_complete", {"session_id": "s1", "code": "pri", "cursor": 3, "timeout": 5})
            if not res["ok"]:
                seen.append(res["error"]["code"])
            time.sleep(0.01)

    threads = [threading.Thread(target=hammer, args=(i,), daemon=True) for i in range(4)]
    for t in threads:
        t.start()
    try:
        for _ in range(3):
            assert controller.ok(W, "kernel_restart", {"session_id": "s1"}, timeout=90)["state"] == "idle"
    finally:
        stop.set()
        for t in threads:
            t.join(10)
    assert set(seen) <= {"kernel_restarting", "timeout"}, seen
    assert run(controller, "s1", "print(21 * 2)", "final")["content"]["status"] == "ok" and stream_text(controller, "s1", "final").strip() == "42"
    assert "Invalid Signature" not in " ".join(str(e["content"]) for e in events(controller, "s1"))


def test_many_cells_in_a_row_keep_their_output_attribution(controller, start_agent):
    """The message-id map is filled before the first reply can be read: every cell's stream arrives under its own id."""
    start_agent()
    start(controller)
    for i in range(40):
        controller.ok(W, "kernel_execute", {"session_id": "s1", "code": f"print('n{i}')", "msg_id": f"q{i}"})
    controller.wait_for(lambda: all(any(e["parent_msg_id"] == f"q{i}" for e in events(controller, "s1", msg_type="execute_reply")) for i in range(40)), 60, "all replies")
    for i in range(40):
        assert stream_text(controller, "s1", f"q{i}").strip() == f"n{i}"


def test_busy_kernel_completion_times_out_cleanly(controller, start_agent):
    start_agent()
    start(controller)
    controller.ok(W, "kernel_execute", {"session_id": "s1", "code": "import time\nprint('b', flush=True)\ntime.sleep(8)", "msg_id": "m1"})
    controller.wait_for(lambda: "b" in stream_text(controller, "s1", "m1"), 30)
    res = controller.request(W, "kernel_complete", {"session_id": "s1", "code": "x", "timeout": 1})
    assert res["ok"] is False and res["error"]["code"] == "timeout"
    controller.ok(W, "kernel_interrupt", {"session_id": "s1"})


def test_duplicate_start_and_validation(controller, start_agent):
    start_agent()
    a = start(controller)
    b = controller.ok(W, "start_kernel", {"session_id": "s1", "spec": kspec()})
    assert b["duplicate"] is True and b["pid"] == a["pid"]
    for bad in ({"session_id": "../x", "spec": kspec()}, {"session_id": "ok", "spec": "not-a-dict"}, {"session_id": "ok2", "spec": {**kspec(), "runtime": {}}}):
        res = controller.request(W, "start_kernel", bad)
        assert res["ok"] is False and res["error"]["code"] == "bad_request", bad
    res = controller.request(W, "start_kernel", {"session_id": "ghost", "spec": kspec(runtime={"id": "olcf-rt-nope"})})
    assert res["ok"] is False and res["error"]["code"] == "runtime_not_ready"
    res = controller.request(W, "kernel_execute", {"session_id": "nope", "code": "1"})
    assert res["ok"] is False
    res = controller.request(W, "kernel_execute", {"session_id": "s1", "code": 5})
    assert res["ok"] is False and res["error"]["code"] == "bad_request"


def test_admission_and_drain(controller, start_agent):
    start_agent(max_cores=1, max_memory_mb=1500)
    start(controller, "s1")
    res = controller.request(W, "start_kernel", {"session_id": "s2", "spec": kspec(), "resources": {"cores": 1, "memory_mb": 256}})
    assert res["ok"] is False and res["error"]["code"] == "resource_exhausted"
    hb = controller.wait_for(lambda: (controller.heartbeats.get(W) or {}).get("allocated", {}).get("cores") == 1 and controller.heartbeats[W], 10)
    assert hb["allocated"]["cores"] == 1
    controller.ok(W, "kernel_shutdown", {"session_id": "s1"})
    controller.ok(W, "drain", {"on": True})
    res = controller.request(W, "start_kernel", {"session_id": "s3", "spec": kspec()})
    assert res["ok"] is False and res["error"]["code"] == "draining"


def test_idle_timeout_culls_the_session(controller, start_agent):
    start_agent()
    info = start(controller, idle_timeout_seconds=3)
    run(controller, "s1", "1", "m1")
    controller.wait_for(lambda: any(e["content"].get("state") == "dead" for e in events(controller, "s1", channel="status", msg_type="status")), 40, "idle cull")
    controller.wait_for(lambda: not alive(info["pid"]), 20, "kernel process gone")
    dead = [e for e in events(controller, "s1", channel="status", msg_type="status") if e["content"].get("state") == "dead"][0]
    assert "idle" in dead["content"]["detail"]


def test_kernel_crash_is_detected(controller, start_agent):
    start_agent()
    info = start(controller)
    run(controller, "s1", "1", "m1")
    psutil.Process(info["pid"]).kill()
    controller.wait_for(lambda: any(e["content"].get("state") == "dead" for e in events(controller, "s1", channel="status", msg_type="status")), 40, "dead status after a crash")
    res = controller.request(W, "kernel_execute", {"session_id": "s1", "code": "1"})
    assert res["ok"] is False and res["error"]["code"] == "kernel_dead"
    hb = controller.wait_for(lambda: controller.heartbeats.get(W), 5)
    controller.wait_for(lambda: not any(k["session_id"] == "s1" and k.get("state") != "dead" for k in controller.heartbeats[W].get("kernels", [])), 15)
    assert hb


def test_huge_output_is_bounded(controller, start_agent):
    start_agent()
    start(controller)
    run(controller, "s1", "print('x' * (6 * 1024 * 1024))", "m1", timeout=90)
    sizes = [len(str(e)) for e in events(controller, "s1")]
    assert max(sizes) < 5 * 1024 * 1024  # event content is capped (frame limit 8 MiB)


def test_kernels_die_with_the_agent_and_stale_dirs_are_removed(controller, start_agent, fake_state):
    h = start_agent()
    info = start(controller)
    kd = fake_state / "kernels" / "s1"
    assert kd.exists()
    h.stop(kill_children=True)
    assert not alive(info["pid"])
    # an agent crash (no clean shutdown) leaves a directory behind: the next agent start removes it
    kd.mkdir(parents=True, exist_ok=True)
    (kd / "kernel-spec.json").write_text('{"spec": {"secrets": {"A": "leftover-secret-value"}}}')
    start_agent()
    controller.wait_for(lambda: not kd.exists() or not (kd / "kernel-spec.json").exists(), 20, "stale kernel dir clean-up")
