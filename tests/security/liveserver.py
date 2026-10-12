# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Run a real uvicorn server (in a thread) for tests that need sockets: TLS, mTLS, WebSockets."""

from __future__ import annotations

import socket
import threading
import time
from typing import Any

import uvicorn


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class LiveServer:
    def __init__(self, app: Any, *, host: str = "127.0.0.1", port: int | None = None, **uvicorn_kwargs: Any) -> None:
        self.host, self.port = host, port or free_port()
        cfg = uvicorn.Config(app, host=host, port=self.port, log_level="warning", lifespan="on", access_log=False, **uvicorn_kwargs)
        self.server = uvicorn.Server(cfg)
        self.thread = threading.Thread(target=self.server.run, name="live-uvicorn", daemon=True)
        self.scheme = "https" if cfg.is_ssl else "http"

    @property
    def base_url(self) -> str:
        return f"{self.scheme}://{self.host}:{self.port}"

    def __enter__(self) -> LiveServer:
        self.thread.start()
        end = time.time() + 30
        while not self.server.started:
            if not self.thread.is_alive():
                raise RuntimeError("uvicorn exited during startup")
            if time.time() > end:
                raise RuntimeError("uvicorn did not start in 30 s")
            time.sleep(0.05)
        return self

    def __exit__(self, *exc: object) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=15)
