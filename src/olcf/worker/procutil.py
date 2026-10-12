# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Process supervision primitives: process groups, graceful termination, pid files that survive pid reuse, adoption.

POSIX children are started in their own session (``start_new_session``) so ``killpg`` reaches the whole tree (python runner ->
JVM driver -> python workers) and so they *survive an agent restart* (they are then adopted from the pid file). On Windows the
child gets ``CREATE_NEW_PROCESS_GROUP`` (graceful stop = ``CTRL_BREAK_EVENT``) and trees are terminated with ``psutil``.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import time
from pathlib import Path
from typing import Any

import psutil

IS_WINDOWS = os.name == "nt"


def popen_kwargs() -> dict[str, Any]:
    """Keyword arguments that detach a child into its own process group/session."""
    if IS_WINDOWS:
        return {"creationflags": getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)}
    return {"start_new_session": True}


def write_pidfile(path: Path, pid: int, **extra: Any) -> None:
    """Record ``pid`` with its create time (pid reuse protection)."""
    try:
        ct = psutil.Process(pid).create_time()
    except psutil.Error:
        ct = None
    doc = {"pid": pid, "create_time": ct, **extra}
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(doc), encoding="utf-8")
    os.replace(tmp, path)


def read_pidfile(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def process_matches(pid: int | None, create_time: float | None, cmd_contains: str | None = None) -> psutil.Process | None:
    """The live process for ``pid`` iff it is the *same* process that was recorded (create time + optional cmdline fragment)."""
    if not pid:
        return None
    try:
        p = psutil.Process(pid)
        if p.status() == psutil.STATUS_ZOMBIE:
            return None
        if create_time is not None and abs(p.create_time() - create_time) > 1.0:
            return None
        if cmd_contains and cmd_contains not in " ".join(p.cmdline()):
            return None
        return p
    except psutil.Error:
        return None


def _children(p: psutil.Process) -> list[psutil.Process]:
    try:
        return p.children(recursive=True)
    except psutil.Error:
        return []


def signal_group(pid: int, sig: int) -> None:
    """Send ``sig`` to the whole process group (POSIX) or the process tree (Windows)."""
    if IS_WINDOWS:
        try:
            p = psutil.Process(pid)
        except psutil.Error:
            return
        for proc in [*_children(p), p]:
            try:
                if sig == signal.SIGTERM:
                    proc.terminate()
                elif sig == getattr(signal, "SIGKILL", signal.SIGTERM):
                    proc.kill()
            except psutil.Error:
                pass
        return
    try:
        os.killpg(os.getpgid(pid), sig)
    except (ProcessLookupError, PermissionError):
        try:
            os.kill(pid, sig)
        except OSError:
            pass


def kill_group_remnants(pgid: int | None) -> int:
    """SIGKILL whatever is still alive in process group ``pgid`` (POSIX) and return how many processes were signalled.

    Job and daemon processes are started as session leaders, so their group id equals the leader's pid and the group outlives the
    leader as long as any member (a Spark driver JVM whose Python parent was OOM-killed, a forked helper...) is alive. Called right
    after the leader is gone, so that nothing a finished job started keeps running on the node. Safe when the group is already
    empty. No-op on Windows (no process groups; the Windows port needs a Job Object here - see docs).
    """
    if not pgid or IS_WINDOWS:
        return 0
    n = 0
    try:
        members = [p for p in psutil.process_iter(["pid", "status"]) if p.info["status"] != psutil.STATUS_ZOMBIE and _pgid_of(p.pid) == pgid]
        n = len(members)
        if n:
            os.killpg(pgid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError, OSError, psutil.Error):
        pass
    return n


def _pgid_of(pid: int) -> int | None:
    try:
        return os.getpgid(pid)
    except (ProcessLookupError, PermissionError, OSError):
        return None


def terminate_tree(pid: int, *, grace: float = 10.0, create_time: float | None = None) -> bool:
    """SIGTERM the group, wait ``grace`` seconds, then SIGKILL stragglers (children are collected first so reparented
    descendants cannot escape). Returns True when nothing is left running."""
    p = process_matches(pid, create_time)
    if p is None:
        return True
    tree = [p, *_children(p)]
    signal_group(pid, signal.SIGTERM)
    deadline = time.monotonic() + grace
    while time.monotonic() < deadline:
        if not any(_alive(x) for x in tree):
            return True
        time.sleep(0.1)
    signal_group(pid, getattr(signal, "SIGKILL", signal.SIGTERM))
    for x in tree:
        if _alive(x):
            try:
                x.kill()
            except psutil.Error:
                pass
    time.sleep(0.2)
    return not any(_alive(x) for x in tree)


def _alive(p: psutil.Process) -> bool:
    try:
        return p.is_running() and p.status() != psutil.STATUS_ZOMBIE
    except psutil.Error:
        return False


def wait_pid(pid: int, create_time: float | None, timeout: float | None = None, poll: float = 0.2) -> bool:
    """Wait for a (non-child) process to exit; True if it exited, False on timeout."""
    end = None if timeout is None else time.monotonic() + timeout
    while process_matches(pid, create_time) is not None:
        if end is not None and time.monotonic() > end:
            return False
        time.sleep(poll)
    return True


def free_port(host: str = "127.0.0.1") -> int:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host if host not in ("0.0.0.0", "") else "127.0.0.1", 0))  # noqa: S104
        return int(s.getsockname()[1])


def port_in_use(host: str, port: int) -> bool:
    """``True`` when something listens on ``host:port``: a connection succeeds, or (a listener whose accept queue is full silently drops connection attempts,
    which BSD/macOS does at once) the kernel refuses to bind the address."""
    import errno
    import socket

    probe = "127.0.0.1" if host in ("0.0.0.0", "") else host  # noqa: S104
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        if s.connect_ex((probe, port)) == 0:
            return True
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as b:
        b.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # a lingering TIME_WAIT connection is not "in use"
        try:
            b.bind((probe, port))
        except OSError as exc:
            return exc.errno == errno.EADDRINUSE
    return False
