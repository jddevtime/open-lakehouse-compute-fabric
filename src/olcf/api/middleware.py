# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Pure-ASGI middleware (safe for streaming/SSE): correlation ids, security headers, request logging."""

from __future__ import annotations

import logging
import re
import time
import uuid

from starlette.types import ASGIApp, Message, Receive, Scope, Send

log = logging.getLogger("olcf.access")
_CID_OK = re.compile(r"^[A-Za-z0-9._-]{8,64}$")

CSP = (
    "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data: blob:; "
    "font-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'; object-src 'none'"
)


class CorrelationAndSecurityMiddleware:
    """ASGI middleware that assigns a correlation id to every request, adds the security response headers (and HSTS when TLS is on) and writes the access log.
    """
    def __init__(self, app: ASGIApp, *, hsts: bool = False, access_log: bool = True) -> None:
        self.app, self.hsts, self.access_log = app, hsts, access_log

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = {k.decode().lower(): v.decode() for k, v in scope.get("headers", [])}
        cid = headers.get("x-correlation-id", "")
        if not _CID_OK.match(cid):
            cid = uuid.uuid4().hex
        scope.setdefault("state", {})["correlation_id"] = cid
        start = time.perf_counter()
        status_holder = {"status": 0}
        path = scope.get("path", "")

        async def send_wrapper(message: Message) -> None:
            if message["type"] == "http.response.start":
                status_holder["status"] = message["status"]
                hdrs = [(k, v) for k, v in message.get("headers", []) if k.lower() != b"x-correlation-id"]
                present = {k.lower() for k, _ in hdrs}

                def add(name: bytes, value: bytes) -> None:
                    # a header the endpoint (or a static-file response) already set wins: duplicates of cache-control / x-content-type-options are
                    # ambiguous for proxies and browsers
                    if name not in present:
                        hdrs.append((name, value))
                        present.add(name)

                hdrs.append((b"x-correlation-id", cid.encode()))
                add(b"x-content-type-options", b"nosniff")
                add(b"referrer-policy", b"no-referrer")
                add(b"cross-origin-opener-policy", b"same-origin")
                if not path.startswith("/v1/"):  # UI documents
                    add(b"content-security-policy", CSP.encode())
                    add(b"x-frame-options", b"DENY")
                elif not path.startswith("/v1/docs"):
                    add(b"cache-control", b"no-store")
                if self.hsts:
                    add(b"strict-transport-security", b"max-age=31536000; includeSubDomains")
                message["headers"] = hdrs
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            if self.access_log and not path.startswith("/v1/events/stream"):
                log.info("%s %s -> %s %.1fms cid=%s", scope.get("method"), path, status_holder["status"], (time.perf_counter() - start) * 1000, cid)
