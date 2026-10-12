# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Test servers for the update feed: a static (optionally TLS) file server with a request log, a redirecting variant, and a forwarding HTTP proxy."""

from __future__ import annotations

import http.client
import ssl
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit


class FeedServer:
    def __init__(self, files: dict[str, bytes] | None = None, *, tls: tuple[str, str] | None = None, redirects: dict[str, str] | None = None) -> None:
        self.files = files if files is not None else {}
        self.redirects = redirects or {}
        self.log: list[str] = []
        outer = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):  # noqa: ANN002, ANN202
                pass

            def do_GET(self):  # noqa: N802, ANN202
                outer.log.append(self.path)
                if self.path in outer.redirects:
                    self.send_response(302)
                    self.send_header("Location", outer.redirects[self.path])
                    self.end_headers()
                    return
                body = outer.files.get(self.path)
                if body is None:
                    self.send_response(404)
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), H)
        if tls:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            ctx.load_cert_chain(*tls)
            self.httpd.socket = ctx.wrap_socket(self.httpd.socket, server_side=True)
        self.scheme = "https" if tls else "http"
        self.port = self.httpd.server_address[1]
        self.url = f"{self.scheme}://127.0.0.1:{self.port}"
        self._t = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def start(self) -> FeedServer:
        self._t.start()
        return self

    def stop(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()


class ForwardProxy:
    """Minimal HTTP forward proxy (absolute-URI GET) that records what it forwarded."""

    def __init__(self) -> None:
        self.seen: list[str] = []
        outer = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):  # noqa: ANN002, ANN202
                pass

            def do_GET(self):  # noqa: N802, ANN202
                outer.seen.append(self.path)
                u = urlsplit(self.path)
                conn = http.client.HTTPConnection(u.hostname, u.port, timeout=10)
                conn.request("GET", u.path + (f"?{u.query}" if u.query else ""))
                r = conn.getresponse()
                data = r.read()
                self.send_response(r.status)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}"
        self._t = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def start(self) -> ForwardProxy:
        self._t.start()
        return self

    def stop(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
