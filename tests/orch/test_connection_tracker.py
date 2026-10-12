# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``ssrf.ConnectionTracker`` with real sockets (external review C04).

Closing a socket from the cancellation watcher while the owning thread is blocked in ``recv`` is racy: the descriptor can be released (and even reused) under the reader.
After a successful ``shutdown`` the watcher therefore returns and the thread that owns the I/O performs the normal cleanup.  A connection that finishes after the abort was
requested is closed by its own (connecting) thread before anything can be sent on it.
"""

from __future__ import annotations

import socket
import threading
import time
from collections.abc import Iterator
from typing import Any

import httpcore
import pytest
from httpcore._backends.sync import SyncStream

from olcf.pipelines import ssrf


class SpyStream(SyncStream):
    """A real socket stream that records which thread closed it."""

    def __init__(self, sock: socket.socket) -> None:
        super().__init__(sock)
        self.sock = sock
        self.closed_by: list[str] = []

    def close(self) -> None:
        self.closed_by.append(threading.current_thread().name)
        super().close()


class NoSocketStream(httpcore.NetworkStream):
    """A stream that exposes no usable socket (the fallback path)."""

    def __init__(self) -> None:
        self.closed = 0

    def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        return b""

    def write(self, buffer: bytes, timeout: float | None = None) -> None:
        pass

    def close(self) -> None:
        self.closed += 1

    def get_extra_info(self, info: str) -> Any:
        return None


@pytest.fixture()
def pair() -> Iterator[tuple[socket.socket, socket.socket]]:
    a, b = socket.socketpair()
    try:
        yield a, b
    finally:
        for s in (a, b):
            s.close()


def _tracked(tracker: ssrf.ConnectionTracker, sock: socket.socket) -> tuple[SpyStream, ssrf._TrackedStream]:
    spy = SpyStream(sock)
    return spy, ssrf._TrackedStream(spy, tracker)


def _blocked_reader(stream: httpcore.NetworkStream) -> tuple[threading.Thread, dict[str, Any]]:
    out: dict[str, Any] = {}
    started = threading.Event()

    def run() -> None:
        started.set()
        try:
            out["data"] = stream.read(4096, 30.0)
        except Exception as exc:  # noqa: BLE001
            out["error"] = exc
        out["at"] = time.monotonic()

    t = threading.Thread(target=run, name="io-owner", daemon=True)
    t.start()
    assert started.wait(5)
    time.sleep(0.2)  # let it enter recv
    return t, out


def test_abort_wakes_a_thread_blocked_in_recv(pair: tuple[socket.socket, socket.socket]) -> None:
    a, _b = pair
    tracker = ssrf.ConnectionTracker()
    _spy, stream = _tracked(tracker, a)
    tracker.add(stream)
    t, out = _blocked_reader(stream)
    assert t.is_alive(), "the reader must be blocked before the abort"
    t0 = time.monotonic()
    tracker.abort()
    t.join(5)
    assert not t.is_alive(), "shutdown must wake the blocked recv"
    assert out["at"] - t0 < 3 and out.get("data") == b""  # EOF (or a read error), long before the 30 s read timeout
    assert tracker.aborted is True


def test_the_watcher_does_not_close_the_descriptor_the_owner_does(pair: tuple[socket.socket, socket.socket]) -> None:
    a, b = pair
    tracker = ssrf.ConnectionTracker()
    spy, stream = _tracked(tracker, a)
    tracker.add(stream)
    fd = a.fileno()
    t, _out = _blocked_reader(stream)
    watcher = threading.Thread(target=tracker.abort, name="cancel-watcher")
    watcher.start()
    watcher.join(5)
    t.join(5)
    assert not watcher.is_alive() and not t.is_alive()
    assert spy.closed_by == [] and a.fileno() == fd != -1, "the watcher must not close the stream or release the descriptor"
    assert b.recv(1) == b"", "the peer sees the shutdown"
    stream.close()  # normal cleanup by the owner
    assert spy.closed_by == [threading.current_thread().name] and a.fileno() == -1


def test_a_connection_registered_after_abort_is_closed_before_anything_is_sent(pair: tuple[socket.socket, socket.socket]) -> None:
    a, b = pair
    tracker = ssrf.ConnectionTracker()
    tracker.abort()
    spy, stream = _tracked(tracker, a)
    with pytest.raises(httpcore.ConnectError):
        tracker.add(stream)  # called by the thread that owns the connection
    assert spy.closed_by == [threading.current_thread().name] and a.fileno() == -1
    assert tracker.connected is False, "nothing was sent, so the call may be repeated"
    b.settimeout(2)
    assert b.recv(16) == b"", "the peer got EOF and no request bytes"


def test_streams_without_a_usable_socket_keep_the_close_fallback() -> None:
    tracker = ssrf.ConnectionTracker()
    stream = NoSocketStream()
    tracker.add(stream)
    tracker.abort()
    assert stream.closed == 1


def test_a_failed_shutdown_falls_back_to_close(pair: tuple[socket.socket, socket.socket]) -> None:
    a, _b = pair
    tracker = ssrf.ConnectionTracker()
    spy, stream = _tracked(tracker, a)
    tracker.add(stream)
    a.close()  # shutdown on a closed socket raises OSError
    tracker.abort()
    assert len(spy.closed_by) == 1
