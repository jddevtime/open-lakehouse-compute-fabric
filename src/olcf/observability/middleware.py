# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""API instrumentation: a *pure ASGI* middleware (no ``BaseHTTPMiddleware``, so streaming responses and SSE are never buffered or delayed) and the hook that installs it
without touching ``olcf/api/app.py``.

Installation trick
    ``Module.install(ctx)`` is not given the FastAPI app. FastAPI merges the ``lifespan`` of every included ``APIRouter`` into the application's lifespan and calls it with the
    application object, so the observability module ships an empty router with a lifespan (:func:`make_lifespan_router`). When the application starts, that lifespan wraps
    ``app.middleware_stack`` in :class:`ApiMetricsMiddleware` (and unwraps it on shutdown). The wrapper is the outermost layer, so it also sees requests rejected by the
    inner middleware. If the host application is started without lifespan (some test harnesses) the instrumentation is simply absent.

Metrics
    ``olcf_api_requests_total{method,route,status}``, ``olcf_api_request_duration_seconds{method,route}`` (time to the first response byte, so long-lived SSE streams do
    not distort latency), ``olcf_api_errors_total{route,class}`` (``4xx`` / ``5xx``), ``olcf_api_inflight_requests``. ``route`` is the matched route template; unmatched paths
    collapse to ``unmatched`` and id-like path segments to ``{id}`` so the label cardinality stays bounded.

Correlation
    ``X-Correlation-ID`` is honoured (or generated and injected into the request, so the inner middleware uses the same value), bound to the logging context, and a W3C
    ``traceparent`` continues / starts a trace; sampled requests are recorded as server spans for OTLP export.
"""

from __future__ import annotations

import logging
import re
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import APIRouter
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from olcf.observability import correlation
from olcf.observability.metrics import MetricsRegistry
from olcf.observability.otel import KIND_SERVER, Tracer

log = logging.getLogger("olcf.observability.middleware")
_CID_OK = re.compile(r"^[A-Za-z0-9._-]{8,64}$")
_ID_SEGMENT = re.compile(r"^(?:[a-z]{2,6}_[0-9A-Za-z]{10,}|[0-9a-fA-F]{16,}|\d+|[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F-]{23,})$")
MARK = "_olcf_api_metrics"


def route_label(scope: Scope) -> str:
    """Matched route template, else the path with id-like segments collapsed."""
    route = scope.get("route")
    path = getattr(route, "path_format", None) or getattr(route, "path", None)
    if isinstance(path, str) and path:
        # ``include_router(prefix=...)`` may leave only the router-relative template on the matched route; recover the full template from the concrete path.
        raw_path = str(scope.get("path") or "")
        try:
            tail = path.format(**(scope.get("path_params") or {}))
        except (KeyError, IndexError, ValueError):
            tail = ""
        if tail and tail != raw_path and raw_path.endswith(tail):
            return raw_path[: len(raw_path) - len(tail)].rstrip("/") + path
        return path
    raw = scope.get("path") or ""
    if not raw.startswith("/v1/"):
        return "ui" if raw and not raw.startswith("/v1") else "unmatched"
    parts = ["{id}" if _ID_SEGMENT.match(p) else p for p in raw.split("/")]
    return "unmatched:" + "/".join(parts)[:80]


class ApiMetricsMiddleware:
    """Pure-ASGI request instrumentation (see module docstring)."""

    def __init__(self, app: ASGIApp, registry: MetricsRegistry, tracer: Tracer | None = None) -> None:
        self.app, self.registry, self.tracer = app, registry, tracer
        setattr(self, MARK, True)
        self._requests = registry.counter("olcf_api_requests_total", "HTTP requests by method, route and status")
        self._errors = registry.counter("olcf_api_errors_total", "HTTP error responses by route and class")
        self._latency = registry.histogram("olcf_api_request_duration_seconds", "Time until the first response byte")
        self._inflight = registry.gauge("olcf_api_inflight_requests", "Requests being handled right now")

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope.get("headers", [])}
        cid = headers.get("x-correlation-id", "")
        if not _CID_OK.match(cid):
            cid = uuid.uuid4().hex
            scope["headers"] = [*scope.get("headers", []), (b"x-correlation-id", cid.encode())]
        tp = correlation.parse_traceparent(headers.get("traceparent"))
        sampled = bool(self.tracer and self.tracer.should_sample(tp[2] if tp else None))
        trace_id = tp[0] if tp else (correlation.new_trace_id() if sampled else None)
        span_id = correlation.new_span_id()
        method = scope.get("method", "GET")
        start = time.perf_counter()
        wall = time.time()
        state = {"status": 0, "done": False}
        self._inflight.inc(1.0)

        def record(status: int) -> None:
            if state["done"]:
                return
            state["done"] = True
            route = route_label(scope)
            dur = time.perf_counter() - start
            self._requests.inc(1.0, method=method, route=route, status=str(status))
            self._latency.observe(dur, method=method, route=route)
            if status >= 400:
                self._errors.inc(1.0, route=route, **{"class": f"{status // 100}xx"})
            if sampled and trace_id and self.tracer is not None:
                self.tracer.add(name=f"{method} {route}", trace_id=trace_id, span_id=span_id, parent_id=tp[1] if tp else None, start=wall, end=wall + dur, kind=KIND_SERVER,
                                attributes={"http.request.method": method, "http.route": route, "http.response.status_code": status, "olcf.correlation_id": cid},
                                error=f"HTTP {status}" if status >= 500 else None)

        async def send_wrapper(message: Message) -> None:
            if message["type"] == "http.response.start":
                state["status"] = int(message["status"])
                record(state["status"])
            await send(message)

        try:
            with correlation.bind(correlation_id=cid, trace_id=trace_id, span_id=span_id if trace_id else None):
                await self.app(scope, receive, send_wrapper)
        except BaseException:
            if not state["done"]:
                record(500)
            raise
        finally:
            self._inflight.dec(1.0)
            if not state["done"]:  # the app finished without sending a response (client went away)
                record(499)


def install_middleware(app: Any, registry: MetricsRegistry, tracer: Tracer | None = None) -> bool:
    """Wrap ``app.middleware_stack`` (idempotent). Returns True when the wrapper was added."""
    stack = getattr(app, "middleware_stack", None)
    if stack is None:
        build = getattr(app, "build_middleware_stack", None)
        if build is None:
            return False
        stack = app.middleware_stack = build()
    if getattr(stack, MARK, False):
        return False
    app.middleware_stack = ApiMetricsMiddleware(stack, registry, tracer)
    return True


def uninstall_middleware(app: Any) -> bool:
    stack = getattr(app, "middleware_stack", None)
    if stack is not None and getattr(stack, MARK, False):
        app.middleware_stack = stack.app
        return True
    return False


def make_lifespan_router(registry: MetricsRegistry, tracer: Tracer | None = None) -> APIRouter:
    """An empty router whose lifespan installs the middleware when the application starts (see module docstring)."""

    @asynccontextmanager
    async def lifespan(app: Any) -> AsyncIterator[None]:
        added = False
        try:
            added = install_middleware(app, registry, tracer)
        except Exception:  # noqa: BLE001 - instrumentation must never prevent startup
            log.exception("could not install the API metrics middleware")
        try:
            yield
        finally:
            if added:
                uninstall_middleware(app)

    return APIRouter(lifespan=lifespan)
