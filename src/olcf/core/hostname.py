# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The host's fully qualified name, with a time limit.

``socket.getfqdn()`` asks the resolver about the machine's own name. On macOS a hostname the resolver cannot place (a laptop or a CI runner whose name ends in
``.local``) makes that call block for minutes, and the worker agent calls it while it starts. :func:`fqdn` runs it in a helper thread, waits a couple of seconds
and then settles for the plain hostname; the answer is remembered, so a slow resolver is asked at most once.
"""

from __future__ import annotations

import socket
import threading

_lock = threading.Lock()
_cached: str | None = None


def fqdn(timeout: float = 2.0) -> str:
    """``socket.getfqdn()``, or ``socket.gethostname()`` when the resolver does not answer within ``timeout`` seconds (the result is cached)."""
    global _cached
    with _lock:
        if _cached is None:
            box: list[str] = []
            worker = threading.Thread(target=lambda: box.append(socket.getfqdn()), name="olcf-fqdn", daemon=True)
            worker.start()
            worker.join(timeout)
            _cached = box[0] if box else socket.gethostname()
        return _cached
