# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""A tiny PEP 503 package index (and a plain file server) with optional HTTP Basic authentication, served from a directory of wheels on 127.0.0.1."""

from __future__ import annotations

import base64
import contextlib
import hashlib
import re
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


class _Handler(BaseHTTPRequestHandler):
    server: IndexServer  # type: ignore[assignment]

    def log_message(self, *args) -> None:  # noqa: ANN002
        return

    def _authorised(self) -> bool:
        if not self.server.credentials:
            return True
        want = "Basic " + base64.b64encode(f"{self.server.credentials[0]}:{self.server.credentials[1]}".encode()).decode()
        got = self.headers.get("Authorization", "")
        self.server.seen_auth.append(got)
        return got == want

    def do_GET(self) -> None:  # noqa: N802
        self.server.requests.append(self.path)
        if not self._authorised():
            self.send_response(401)
            self.send_header("WWW-Authenticate", 'Basic realm="olcf-test"')
            self.end_headers()
            return
        root = self.server.root
        if self.path.startswith("/simple/"):
            name = re.sub(r"[-_.]+", "-", self.path[len("/simple/"):].strip("/")).lower()
            links = []
            for f in sorted(root.glob("*.whl")) + sorted(root.glob("*.tar.gz")):
                dist = re.sub(r"[-_.]+", "-", f.name.split("-")[0]).lower()
                if dist == name:
                    links.append(f'<a href="/files/{f.name}#sha256={hashlib.sha256(f.read_bytes()).hexdigest()}">{f.name}</a>')
            if not links:
                self.send_response(404)
                self.end_headers()
                return
            body = ("<!doctype html><html><body>" + "<br>".join(links) + "</body></html>").encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path.startswith("/files/"):
            f = root / Path(self.path[len("/files/"):]).name
            if f.is_file():
                data = f.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                return
        self.send_response(404)
        self.end_headers()


class IndexServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, root: Path, credentials: tuple[str, str] | None = None) -> None:
        super().__init__(("127.0.0.1", 0), _Handler)
        self.root, self.credentials = root, credentials
        self.requests: list[str] = []
        self.seen_auth: list[str] = []

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server_address[1]}"


@contextlib.contextmanager
def serve_index(root: Path, credentials: tuple[str, str] | None = None) -> Iterator[IndexServer]:
    srv = IndexServer(root, credentials)
    t = threading.Thread(target=srv.serve_forever, daemon=True, name="olcf-test-index")
    t.start()
    try:
        yield srv
    finally:
        srv.shutdown()
        srv.server_close()
        t.join(timeout=10)
