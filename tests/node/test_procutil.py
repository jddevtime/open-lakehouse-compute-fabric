# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Unit tests for :mod:`olcf.worker.procutil` with real child processes (process groups, pid reuse protection, adoption)."""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import psutil
import pytest

from olcf.worker import procutil

posix_only = pytest.mark.skipif(os.name == "nt", reason="process groups are POSIX")

PARENT_WITH_GRANDCHILD = textwrap.dedent("""
    import subprocess, sys, time, os
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"])
    open(sys.argv[1], "w").write(str(child.pid))
    time.sleep(120)
""")

IGNORES_SIGTERM = textwrap.dedent("""
    import signal, time, sys
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    open(sys.argv[1], "w").write("ready")
    time.sleep(120)
""")


def wait_for(path: Path, timeout: float = 15.0) -> str:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if path.exists() and path.read_text():
            return path.read_text()
        time.sleep(0.05)
    raise AssertionError(f"{path} never appeared")


@pytest.fixture
def spawned():
    procs: list[subprocess.Popen] = []

    def spawn(code: str, *args: str) -> subprocess.Popen:
        p = subprocess.Popen([sys.executable, "-c", code, *args], **procutil.popen_kwargs())  # noqa: S603
        procs.append(p)
        return p

    yield spawn
    for p in procs:
        try:
            procutil.signal_group(p.pid, 9)
        except Exception:  # noqa: BLE001
            pass
        try:
            p.kill()
        except OSError:
            pass
        p.wait(timeout=10)


def test_pidfile_roundtrip(tmp_path):
    f = tmp_path / "x.pid"
    procutil.write_pidfile(f, os.getpid(), role="test")
    doc = procutil.read_pidfile(f)
    assert doc["pid"] == os.getpid() and doc["role"] == "test"
    assert abs(doc["create_time"] - psutil.Process().create_time()) < 0.01
    assert not list(tmp_path.glob("*.tmp"))
    assert procutil.read_pidfile(tmp_path / "nope.pid") is None
    (tmp_path / "bad.pid").write_text("garbage")
    assert procutil.read_pidfile(tmp_path / "bad.pid") is None


def test_process_matches_checks_create_time_and_cmdline():
    me = psutil.Process()
    assert procutil.process_matches(os.getpid(), me.create_time()) is not None
    assert procutil.process_matches(os.getpid(), me.create_time() - 100) is None  # a *different* process that reused the pid
    assert procutil.process_matches(os.getpid(), None, cmd_contains=Path(sys.executable).name[:3]) is not None
    assert procutil.process_matches(os.getpid(), me.create_time(), cmd_contains="definitely-not-in-cmdline-xyz") is None
    assert procutil.process_matches(None, None) is None
    assert procutil.process_matches(2**22 + 12345, None) is None


def test_zombie_is_not_a_match(spawned):
    p = spawned("pass")
    end = time.monotonic() + 10
    while time.monotonic() < end and psutil.Process(p.pid).status() != psutil.STATUS_ZOMBIE:
        time.sleep(0.05)
    assert procutil.process_matches(p.pid, None) is None  # exited but not reaped


@posix_only
def test_terminate_tree_kills_grandchildren(spawned, tmp_path):
    marker = tmp_path / "gc.pid"
    p = spawned(PARENT_WITH_GRANDCHILD, str(marker))
    gc = int(wait_for(marker))
    assert psutil.pid_exists(gc)
    assert procutil.terminate_tree(p.pid, grace=5.0)
    p.wait(timeout=5)
    time.sleep(0.2)
    assert not psutil.pid_exists(gc) or psutil.Process(gc).status() == psutil.STATUS_ZOMBIE


@posix_only
def test_terminate_tree_escalates_to_sigkill(spawned, tmp_path):
    marker = tmp_path / "ready"
    p = spawned(IGNORES_SIGTERM, str(marker))
    wait_for(marker)
    t0 = time.monotonic()
    assert procutil.terminate_tree(p.pid, grace=0.5)
    assert time.monotonic() - t0 < 5
    assert p.wait(timeout=5) == -9  # SIGKILL, because SIGTERM was ignored


def test_terminate_tree_on_gone_or_reused_pid(spawned):
    p = spawned("import time; time.sleep(60)")
    ct = psutil.Process(p.pid).create_time()
    assert procutil.terminate_tree(p.pid, grace=1.0, create_time=ct - 500)  # create time mismatch: not our process, untouched
    assert p.poll() is None
    assert procutil.terminate_tree(p.pid, grace=2.0, create_time=ct)
    p.wait(timeout=5)
    assert procutil.terminate_tree(p.pid, grace=1.0, create_time=ct)  # already gone -> True


def test_wait_pid_for_non_child(spawned):
    p = spawned("import time; time.sleep(0.6)")
    ct = psutil.Process(p.pid).create_time()
    assert procutil.wait_pid(p.pid, ct, timeout=0.05) is False  # still running
    p.wait(timeout=5)  # reap so it is not a zombie
    assert procutil.wait_pid(p.pid, ct, timeout=5) is True
    assert procutil.wait_pid(2**22 + 999, None, timeout=0.1) is True


@posix_only
def test_children_run_in_their_own_group(spawned):
    p = spawned("import time; time.sleep(30)")
    assert os.getpgid(p.pid) == p.pid != os.getpgid(0)


def test_ports():
    port = procutil.free_port()
    assert 1024 < port < 65536
    assert not procutil.port_in_use("127.0.0.1", port)
    import socket

    s = socket.socket()
    s.bind(("127.0.0.1", port))
    s.listen(1)
    try:
        assert procutil.port_in_use("127.0.0.1", port)
    finally:
        s.close()


@posix_only
def test_kill_group_remnants_reaps_children_of_a_dead_leader(spawned, tmp_path):
    marker = tmp_path / "left.pid"
    code = ("import subprocess, sys\nc = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(300)'])\n"
            "open(sys.argv[1], 'w').write(str(c.pid))\n")  # the leader exits at once, its child lives on in the same group
    p = spawned(code, str(marker))
    child = int(wait_for(marker))
    p.wait(timeout=10)
    assert psutil.pid_exists(child)
    assert procutil.kill_group_remnants(p.pid) == 1
    end = time.monotonic() + 10
    while time.monotonic() < end and psutil.pid_exists(child) and psutil.Process(child).status() != psutil.STATUS_ZOMBIE:
        time.sleep(0.05)
    assert not psutil.pid_exists(child) or psutil.Process(child).status() == psutil.STATUS_ZOMBIE
    assert procutil.kill_group_remnants(p.pid) == 0  # empty group: nothing to do, no error
    assert procutil.kill_group_remnants(None) == 0
