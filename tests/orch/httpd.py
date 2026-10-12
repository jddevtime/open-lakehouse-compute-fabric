# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""A tiny programmable HTTP server for the tests (webhook receivers, pipeline ``http`` targets). Runs on 127.0.0.1 in a daemon thread."""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


class Hit:
    def __init__(self, method: str, path: str, headers: dict[str, str], body: bytes) -> None:
        self.method, self.path, self.headers, self.body, self.at = method, path, headers, body, time.monotonic()

    def json(self) -> Any:
        return json.loads(self.body or b"null")


class Server:
    """``script`` maps a path (or ``"*"``) to a list of responses consumed in order (the last one repeats): ``int`` status, ``(status, body, headers)``, ``("sleep", seconds, status)``
    or ``("commit_after", seconds, status)`` - the server accepts the request, keeps working for ``seconds`` *without noticing that the client went away*, then commits the effect
    (``commits``: one ``Hit`` per commit, with the commit time in ``at``) and answers.  That is exactly the "accepted but unanswered" request of a timed-out POST."""

    def __init__(self) -> None:
        self.hits: list[Hit] = []
        self.commits: list[Hit] = []
        self.script: dict[str, list[Any]] = {}
        self.lock = threading.Lock()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def _handle(self) -> None:
                n = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(n) if n else b""
                hit = Hit(self.command, self.path, {k.lower(): v for k, v in self.headers.items()}, body)
                with owner.lock:
                    owner.hits.append(hit)
                    steps = owner.script.get(self.path.split("?")[0]) or owner.script.get("*") or [200]
                    step = steps.pop(0) if len(steps) > 1 else steps[0]
                status, payload, headers = 200, b'{"ok": true}', {"Content-Type": "application/json"}
                if isinstance(step, int):
                    status = step
                elif isinstance(step, tuple) and step and step[0] == "sleep":
                    time.sleep(float(step[1]))
                    status = int(step[2]) if len(step) > 2 else 200
                elif isinstance(step, tuple) and step and step[0] == "commit_after":
                    time.sleep(float(step[1]))
                    done = Hit(self.command, self.path, hit.headers, body)
                    with owner.lock:
                        owner.commits.append(done)
                    status = int(step[2]) if len(step) > 2 else 200
                elif isinstance(step, tuple):
                    status = step[0]
                    payload = step[1].encode() if isinstance(step[1], str) else (step[1] if isinstance(step[1], bytes) else json.dumps(step[1]).encode())
                    headers.update(step[2] if len(step) > 2 else {})
                try:
                    self.send_response(status)
                    for k, v in headers.items():
                        self.send_header(k, v)
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    if self.command != "HEAD":
                        self.wfile.write(payload)
                except (BrokenPipeError, ConnectionResetError):
                    pass

            do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = do_HEAD = do_OPTIONS = _handle  # noqa: N815

            def log_message(self, *args: Any) -> None:
                pass

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.httpd.daemon_threads = True
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, name="test-httpd", daemon=True)
        self.thread.start()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def on(self, path: str, *responses: Any) -> None:
        with self.lock:
            self.script[path] = list(responses)

    def committed(self, path: str) -> list[Hit]:
        with self.lock:
            return [h for h in self.commits if h.path.split("?")[0] == path]

    def to(self, path: str) -> list[Hit]:
        with self.lock:
            return [h for h in self.hits if h.path.split("?")[0] == path]

    def stop(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
