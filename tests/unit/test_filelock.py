# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``olcf.core.filelock``: the one cross-platform file lock (POSIX ``flock``, Windows ``msvcrt`` byte lock). Review finding F06.

The POSIX path is exercised for real (threads and separate processes, including a holder that is killed). The Windows path cannot run on
this host, so it is driven through a faithful ``msvcrt`` stand-in that locks the byte at the current file position with a real, cross-process
POSIX record lock and refuses to lock a byte that does not exist - which is exactly what the helper has to get right on Windows
(rewind to offset 0, make sure byte 0 exists, non-blocking attempts, unlock at the same offset).
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import threading
import time
from pathlib import Path

import psutil
import pytest

from olcf.core import filelock
from olcf.core.filelock import FileLock, LockTimeout

ROOT = Path(__file__).resolve().parents[2]
posix_only = pytest.mark.skipif(os.name == "nt", reason="POSIX flock path")
windows_only = pytest.mark.skipif(os.name != "nt", reason="the real msvcrt branch")

#: Python source of an ``msvcrt`` look-alike (used in this process and in child processes): record locks on [pos, pos+n) of the fd.
FAKE_MSVCRT = textwrap.dedent('''
    import errno, fcntl, os, types
    m = types.ModuleType("msvcrt")
    m.LK_UNLCK, m.LK_LOCK, m.LK_NBLCK, m.LK_RLCK, m.LK_NBRLCK = 0, 1, 2, 3, 4
    m.calls = []
    def locking(fd, mode, nbytes):
        pos = os.lseek(fd, 0, os.SEEK_CUR)
        m.calls.append((mode, pos, nbytes))
        if os.fstat(fd).st_size < pos + nbytes:
            raise OSError(errno.EINVAL, "the byte to lock does not exist in the file")
        if mode == m.LK_UNLCK:
            fcntl.lockf(fd, fcntl.LOCK_UN, nbytes, pos, os.SEEK_SET)
        elif mode == m.LK_NBLCK:
            try:
                fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB, nbytes, pos, os.SEEK_SET)
            except OSError as exc:
                raise OSError(errno.EACCES, "locked by another process") from exc
        else:
            raise AssertionError(f"mode {mode} is not used by olcf.core.filelock")
    m.locking = locking
''')


def _run_child(code: str, *args: str, **kw) -> subprocess.Popen:
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(ROOT / "src"), os.environ.get("PYTHONPATH", "")])}
    return subprocess.Popen([sys.executable, "-P", "-c", textwrap.dedent(code), *args], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env, **kw)


def _wait_line(proc: subprocess.Popen, text: str, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    assert proc.stdout is not None
    while time.monotonic() < deadline:
        line = proc.stdout.readline()
        if text in line:
            return
        if proc.poll() is not None and not line:
            break
    raise AssertionError(f"child never printed {text!r}: {proc.stderr.read() if proc.stderr else ''}")


HOLDER = '''
    import sys, time
    {prelude}
    from olcf.core.filelock import FileLock
    lk = FileLock(sys.argv[1])
    lk.acquire(5)
    print("held", flush=True)
    time.sleep(120)
'''


# ------------------------------------------------------------------------------------------------------------------ real backend (POSIX)
@posix_only
def test_backend_is_fcntl_on_posix_and_the_module_imports_without_msvcrt() -> None:
    assert filelock.backend_name() == "fcntl"


def test_context_manager_creates_the_file_and_directory_and_releases(tmp_path: Path) -> None:
    p = tmp_path / "deep" / "dir" / "x.lock"
    lk = FileLock(p, timeout=1)
    with lk:
        assert lk.locked and p.exists()
        assert not FileLock(p).try_acquire()
    assert not lk.locked
    other = FileLock(p)
    assert other.try_acquire()
    other.release()
    other.release()  # idempotent


def test_not_reentrant_and_timeout_error_type(tmp_path: Path) -> None:
    lk = FileLock(tmp_path / "a.lock")
    lk.acquire()
    with pytest.raises(RuntimeError, match="already held"):
        lk.acquire()
    t0 = time.monotonic()
    with pytest.raises(LockTimeout, match="timed out after 0.3s"):
        FileLock(tmp_path / "a.lock", timeout=0.3).acquire()
    assert 0.25 < time.monotonic() - t0 < 5
    assert isinstance(LockTimeout("x"), TimeoutError)
    lk.release()
    FileLock(tmp_path / "a.lock", timeout=0).acquire()  # timeout=0 is a single try and now succeeds


def test_two_threads_exclude_each_other(tmp_path: Path) -> None:
    path = tmp_path / "t.lock"
    inside = 0
    peak = 0
    guard = threading.Lock()

    def work() -> None:
        nonlocal inside, peak
        for _ in range(5):
            with FileLock(path, timeout=30):
                with guard:
                    inside += 1
                    peak = max(peak, inside)
                time.sleep(0.01)
                with guard:
                    inside -= 1

    ts = [threading.Thread(target=work) for _ in range(4)]
    [t.start() for t in ts]
    [t.join(60) for t in ts]
    assert peak == 1 and inside == 0


def test_a_second_process_waits_then_acquires_when_the_holder_releases(tmp_path: Path) -> None:
    path = tmp_path / "p.lock"
    holder = _run_child(HOLDER.format(prelude=""), str(path))
    try:
        _wait_line(holder, "held")
        assert not FileLock(path).try_acquire()
        with pytest.raises(LockTimeout):
            FileLock(path, timeout=0.3).acquire()
        waiter_result: list[float] = []

        def wait() -> None:
            t0 = time.monotonic()
            with FileLock(path, timeout=30):
                waiter_result.append(time.monotonic() - t0)

        th = threading.Thread(target=wait)
        th.start()
        time.sleep(0.5)
        assert not waiter_result, "the waiter must block while the holder lives"
        holder.terminate()
        holder.wait(20)
        th.join(30)
        assert waiter_result and waiter_result[0] >= 0.4
    finally:
        holder.kill()
        holder.wait(10)


def test_a_crashed_holder_does_not_leave_an_unusable_lock(tmp_path: Path) -> None:
    """Killed (SIGKILL / TerminateProcess): no ``finally``/``__exit__`` runs, yet the next process takes the lock at once (the OS drops it with the dead process)."""
    path = tmp_path / "crash.lock"
    holder = _run_child(HOLDER.format(prelude=""), str(path))
    try:
        _wait_line(holder, "held")
        assert not FileLock(path).try_acquire()
        holder.kill()
        holder.wait(20)
        lk = FileLock(path, timeout=2)
        lk.acquire()
        lk.release()
    finally:
        holder.kill()
        holder.wait(10)
    assert path.exists(), "the lock file itself stays behind; only the handle lock matters"


@windows_only
def test_real_msvcrt_backend_is_selected_on_windows() -> None:
    assert filelock.backend_name() == "msvcrt"


# ------------------------------------------------------------------------------------------------------------------ Windows backend (stand-in msvcrt)
@pytest.fixture()
def windows_backend(monkeypatch: pytest.MonkeyPatch):
    """Run the helper on its ``msvcrt`` branch in this process: no fcntl, the stand-in msvcrt."""
    pytest.importorskip("fcntl", reason="the stand-in msvcrt is built on POSIX record locks")
    ns: dict = {}
    exec(FAKE_MSVCRT, ns)  # noqa: S102 - test-only stand-in module source
    fake = ns["m"]
    monkeypatch.setattr(filelock, "_fcntl", None)
    monkeypatch.setattr(filelock, "_msvcrt", fake)
    return fake


def test_windows_branch_creates_the_lockable_byte_and_locks_offset_zero(windows_backend, tmp_path: Path) -> None:
    fake = windows_backend
    assert filelock.backend_name() == "msvcrt"
    p = tmp_path / "w.lock"
    with FileLock(p, timeout=1):
        assert p.stat().st_size >= 1, "Windows can only lock bytes that exist"
    modes = [(c[0], c[1], c[2]) for c in fake.calls]
    assert modes == [(fake.LK_NBLCK, 0, 1), (fake.LK_UNLCK, 0, 1)], "one non-blocking lock of byte 0, unlocked at the same offset"


def test_windows_branch_unlock_rewinds_even_when_the_position_moved(windows_backend, tmp_path: Path) -> None:
    lk = FileLock(tmp_path / "w2.lock")
    lk.acquire()
    assert lk._fh is not None
    os.lseek(lk._fh.fileno(), 7, os.SEEK_SET)  # someone moved the position: unlock must still address byte 0
    lk.release()
    assert windows_backend.calls[-1] == (windows_backend.LK_UNLCK, 0, 1)
    again = FileLock(tmp_path / "w2.lock")
    assert again.try_acquire()
    again.release()


def test_windows_branch_try_acquire_reports_contention_without_leaking_handles(windows_backend, tmp_path: Path) -> None:
    p = tmp_path / "w3.lock"
    code = HOLDER.format(prelude="exec(open(sys.argv[2]).read()); import sys as _s; _s.modules['msvcrt'] = m; import olcf.core.filelock as f; f._fcntl = None; f._msvcrt = m")
    fake_src = tmp_path / "fake_msvcrt.py"
    fake_src.write_text(FAKE_MSVCRT)
    holder = _run_child(code, str(p), str(fake_src))
    try:
        _wait_line(holder, "held")
        before = psutil.Process().num_fds()                                  # portable (Linux, macOS); this test needs the POSIX record-lock stand-in anyway
        for _ in range(20):
            assert not FileLock(p).try_acquire()
        with pytest.raises(LockTimeout):
            FileLock(p, timeout=0.3).acquire()
        assert psutil.Process().num_fds() <= before, "failed attempts must close their handle"
        holder.kill()  # crash: the OS releases the byte lock
        holder.wait(20)
        lk = FileLock(p, timeout=5)
        lk.acquire()
        lk.release()
    finally:
        holder.kill()
        holder.wait(10)


def test_no_primitive_available_is_an_explicit_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(filelock, "_fcntl", None)
    monkeypatch.setattr(filelock, "_msvcrt", None)
    assert filelock.backend_name() == "none"
    with pytest.raises(RuntimeError, match="no file-locking primitive"):
        FileLock(tmp_path / "n.lock").try_acquire()


# ------------------------------------------------------------------------------------------------------------------ users of the helper
def test_runtime_lock_reports_timeout_as_install_failed_and_is_the_shared_implementation(tmp_path: Path) -> None:
    from olcf.runtime.errors import InstallFailed
    from olcf.runtime.locks import FileLock as RuntimeLock

    assert issubclass(RuntimeLock, FileLock)
    holder = FileLock(tmp_path / "r.lock")
    holder.acquire()
    try:
        with pytest.raises(InstallFailed, match="timed out waiting for runtime lock"), RuntimeLock(tmp_path / "r.lock", timeout=0.2):
            pass
    finally:
        holder.release()
    with RuntimeLock(tmp_path / "r.lock", timeout=1):
        pass


def test_layer_lock_times_out_with_layer_error_and_is_released_after_a_build(tmp_path: Path) -> None:
    from olcf.notebooks.layer import LayerError, ensure_layer

    layer = {"key": "sha256:abc", "packages": [{"name": "x", "version": "1", "hashes": ["00" * 32]}], "pip": {"no_index": True}}
    safe = "sha256-abc"
    held = FileLock(tmp_path / f".{safe}.lock")
    held.acquire()
    try:
        with pytest.raises(LayerError, match="timed out waiting for another process that is building layer"):
            ensure_layer(layer, tmp_path, timeout=0.3)
    finally:
        held.release()
    # a failing build (no such package offline) must still release the lock for the next caller
    with pytest.raises(LayerError):
        ensure_layer(layer, tmp_path, python=sys.executable, timeout=60)
    again = FileLock(tmp_path / f".{safe}.lock")
    assert again.try_acquire()
    again.release()


def test_layer_module_is_standard_library_plus_the_lock_helper_only() -> None:
    """``olcf.notebooks.layer`` runs inside runtime venvs (CONTRACTS section 12): stdlib + ``olcf.core.filelock`` only, and the helper is stdlib only."""
    import ast

    def imported(path: Path) -> set[str]:
        out: set[str] = set()
        for n in ast.walk(ast.parse(path.read_text())):
            if isinstance(n, ast.Import):
                out |= {a.name.split(".")[0] for a in n.names}
            elif isinstance(n, ast.ImportFrom) and n.module and n.level == 0:
                out.add(n.module if n.module.startswith("olcf") else n.module.split(".")[0])
        out.discard("__future__")
        return out

    layer_imports = imported(ROOT / "src/olcf/notebooks/layer.py")
    helper_imports = imported(ROOT / "src/olcf/core/filelock.py")
    third_party = {m for m in (layer_imports | helper_imports) if m not in sys.stdlib_module_names and not m.startswith("olcf")}
    assert not third_party, third_party
    assert {m for m in layer_imports if m.startswith("olcf")} == {"olcf.core.filelock"}
    assert not {m for m in helper_imports if m.startswith("olcf")}
    assert "fcntl" not in layer_imports and "fcntl" not in imported(ROOT / "src/olcf/notebooks/gitsync.py")
