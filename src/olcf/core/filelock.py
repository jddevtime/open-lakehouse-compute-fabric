# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Cross-process advisory file lock for POSIX and Windows.  **Standard library only** (runtime-venv safe).

Why this exists: three places (runtime installs, per-job dependency layers, Git sync) each needed an exclusive lock that is held across
processes, and two of them imported ``fcntl`` unconditionally, which does not exist on Windows - the application could not even start there.
This is the single implementation; nothing else in the code base should import ``fcntl`` or ``msvcrt`` for locking.

Semantics
---------
* POSIX: ``fcntl.flock`` on the whole file (per open file description, so two ``FileLock`` objects - in different threads or processes -
  exclude each other even inside one process).
* Windows: ``msvcrt.locking`` on **one byte at offset 0**.  Windows byte-range locks need the byte to exist and are relative to the file
  position, so the lock file is first given a length of at least one byte and the position is rewound before every lock/unlock call.
  ``LK_NBLCK`` is used for every attempt; waiting is done here (``LK_LOCK`` gives up after ten tries and raises) so a timeout is exact.
* The lock is tied to the open handle: a process that crashes or is killed releases it when the OS closes its handles, so a crashed
  holder never leaves a lock that cannot be taken (the lock *file* stays behind, which is harmless: only the handle lock matters).
* ``timeout=None`` waits forever; ``timeout=0`` is a single try.  Waiting uses a short sleep loop (no signals, so it works in any thread).

Use it as a context manager (``with FileLock(path, timeout=30):``) or call :meth:`acquire` / :meth:`try_acquire` / :meth:`release`.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from types import TracebackType
from typing import IO

try:  # POSIX
    import fcntl as _fcntl
except ImportError:  # Windows has no fcntl
    _fcntl = None  # type: ignore[assignment]

try:  # Windows only; absent on POSIX
    import msvcrt as _msvcrt
except ImportError:
    _msvcrt = None  # type: ignore[assignment]

_POLL_SECONDS = 0.05
_MAX_POLL_SECONDS = 0.25


class LockTimeout(TimeoutError):
    """The lock could not be acquired within the timeout (another process or thread holds it)."""


def backend_name() -> str:
    """``"fcntl"``, ``"msvcrt"`` or ``"none"`` - which primitive is available on this interpreter."""
    if _fcntl is not None:
        return "fcntl"
    if _msvcrt is not None:
        return "msvcrt"
    return "none"


def _try_lock(fh: IO[bytes]) -> bool:
    """One non-blocking attempt; True when the lock is now held by ``fh``."""
    if _fcntl is not None:
        try:
            _fcntl.flock(fh.fileno(), _fcntl.LOCK_EX | _fcntl.LOCK_NB)
        except OSError:
            return False
        return True
    if _msvcrt is not None:
        fd = fh.fileno()
        try:
            os.lseek(fd, 0, os.SEEK_SET)
            _msvcrt.locking(fd, _msvcrt.LK_NBLCK, 1)
        except OSError:
            return False
        return True
    raise RuntimeError("no file-locking primitive is available on this platform (neither fcntl nor msvcrt)")


def _unlock(fh: IO[bytes]) -> None:
    if _fcntl is not None:
        _fcntl.flock(fh.fileno(), _fcntl.LOCK_UN)
    elif _msvcrt is not None:
        fd = fh.fileno()
        os.lseek(fd, 0, os.SEEK_SET)
        _msvcrt.locking(fd, _msvcrt.LK_UNLCK, 1)


def _ensure_lockable_byte(fh: IO[bytes]) -> None:
    """Windows can only lock bytes that exist inside the file: make sure byte 0 does (a no-op when it is already there)."""
    if _fcntl is not None or _msvcrt is None:
        return
    fd = fh.fileno()
    if os.fstat(fd).st_size < 1:
        fh.write(b"\0")  # opened "a+b": appends, so a racing creator at worst leaves two bytes, which is harmless
        fh.flush()


class FileLock:
    """Exclusive cross-process lock on ``path`` (created, with its directory, when missing). Re-entrancy is *not* supported."""

    def __init__(self, path: str | Path, timeout: float | None = None) -> None:
        self.path = Path(path)
        self.timeout = timeout
        self._fh: IO[bytes] | None = None

    @property
    def locked(self) -> bool:
        """True while this object holds the lock."""
        return self._fh is not None

    def _open(self) -> IO[bytes]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fh = open(self.path, "a+b")  # noqa: SIM115 - held until release()
        try:
            _ensure_lockable_byte(fh)
        except BaseException:
            fh.close()
            raise
        return fh

    def try_acquire(self) -> bool:
        """Take the lock if it is free right now; return whether this object holds it afterwards."""
        if self._fh is not None:
            raise RuntimeError(f"FileLock {self.path} is already held by this object (not re-entrant)")
        fh = self._open()
        try:
            ok = _try_lock(fh)
        except BaseException:
            fh.close()
            raise
        if not ok:
            fh.close()
            return False
        self._fh = fh
        return True

    def acquire(self, timeout: float | None = None) -> None:
        """Wait up to ``timeout`` seconds (default: the constructor's; ``None`` = forever) and take the lock; raise :class:`LockTimeout`."""
        wait = self.timeout if timeout is None else timeout
        if self._fh is not None:
            raise RuntimeError(f"FileLock {self.path} is already held by this object (not re-entrant)")
        fh = self._open()
        deadline = None if wait is None else time.monotonic() + wait
        pause = _POLL_SECONDS
        try:
            if _fcntl is not None and deadline is None:
                _fcntl.flock(fh.fileno(), _fcntl.LOCK_EX)  # plain blocking call: no polling needed
            else:
                while not _try_lock(fh):
                    if deadline is not None and time.monotonic() >= deadline:
                        raise LockTimeout(f"timed out after {wait:g}s waiting for lock {self.path}")
                    time.sleep(pause if deadline is None else max(0.0, min(pause, deadline - time.monotonic())))
                    pause = min(_MAX_POLL_SECONDS, pause * 1.5)
        except BaseException:
            fh.close()
            raise
        self._fh = fh

    def release(self) -> None:
        """Release the lock (idempotent)."""
        fh, self._fh = self._fh, None
        if fh is None:
            return
        try:
            _unlock(fh)
        finally:
            fh.close()

    def __enter__(self) -> FileLock:
        self.acquire()
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None) -> None:
        self.release()

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"FileLock({str(self.path)!r}, locked={self.locked})"


def locked_path(path: str | Path, timeout: float | None = None) -> FileLock:
    """Convenience: ``with locked_path(p, 30): ...``."""
    return FileLock(path, timeout)


__all__ = ["FileLock", "LockTimeout", "backend_name", "locked_path"]
