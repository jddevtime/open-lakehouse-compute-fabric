# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``procutil.port_in_use``: a busy port is reported even when its listener's accept queue is full (BSD/macOS drop further connection attempts silently)."""

from __future__ import annotations

import socket

from olcf.worker import procutil


def test_a_listener_with_a_full_accept_queue_is_still_in_use() -> None:
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    port = srv.getsockname()[1]
    clients: list[socket.socket] = []
    try:
        for _ in range(4):  # fill the queue; none is ever accepted
            c = socket.socket()
            c.setblocking(False)
            c.connect_ex(("127.0.0.1", port))
            clients.append(c)
        assert procutil.port_in_use("127.0.0.1", port)
    finally:
        for c in clients:
            c.close()
        srv.close()


def test_a_free_port_is_not_in_use() -> None:
    assert not procutil.port_in_use("127.0.0.1", procutil.free_port())
