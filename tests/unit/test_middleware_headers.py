# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The security-header middleware never duplicates a header the endpoint already set."""

from __future__ import annotations

import asyncio
from typing import Any

from olcf.api.middleware import CorrelationAndSecurityMiddleware


def _run(path: str, app_headers: list[tuple[bytes, bytes]], **kw: Any) -> list[tuple[bytes, bytes]]:
    async def app(scope, receive, send):  # noqa: ANN001, ANN202, ARG001
        await send({"type": "http.response.start", "status": 200, "headers": app_headers})
        await send({"type": "http.response.body", "body": b""})

    sent: list[dict[str, Any]] = []

    async def send(m):  # noqa: ANN001, ANN202
        sent.append(m)

    async def receive():  # noqa: ANN202
        return {"type": "http.request", "body": b""}

    mw = CorrelationAndSecurityMiddleware(app, access_log=False, **kw)
    asyncio.run(mw({"type": "http", "path": path, "method": "GET", "headers": []}, receive, send))
    return sent[0]["headers"]


def _count(headers: list[tuple[bytes, bytes]], name: bytes) -> int:
    return sum(1 for k, _ in headers if k.lower() == name)


def test_endpoint_headers_are_not_duplicated():
    h = _run("/v1/things", [(b"Cache-Control", b"no-cache"), (b"X-Content-Type-Options", b"nosniff")], hsts=True)
    assert _count(h, b"cache-control") == 1 and dict(h)[b"Cache-Control"] == b"no-cache"
    assert _count(h, b"x-content-type-options") == 1
    assert _count(h, b"x-correlation-id") == 1 and _count(h, b"strict-transport-security") == 1


def test_defaults_are_added_once_when_the_endpoint_set_nothing():
    h = _run("/v1/things", [])
    assert _count(h, b"cache-control") == 1 and dict(h)[b"cache-control"] == b"no-store" and _count(h, b"x-content-type-options") == 1
    ui = _run("/index.html", [(b"x-frame-options", b"SAMEORIGIN")])
    assert _count(ui, b"x-frame-options") == 1 and _count(ui, b"content-security-policy") == 1
