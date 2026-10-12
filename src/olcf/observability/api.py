# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""REST API of the metrics subsystem.

``GET /v1/metrics``
    Prometheus text exposition (format 0.0.4) of this replica: process-local metrics plus the database-derived gauges. Authenticate with a bearer token (a service principal
    with the ``Viewer`` role at organisation scope and a token of scope ``read`` is enough); there is no anonymous access.
``GET /v1/metrics/series?name=&labels=k=v,k2=v2&since=&until=&step=``
    Persisted time series for sparklines. ``since`` / ``until`` take an ISO-8601 time or a relative duration (``15m``, ``6h``, ``7d`` = that long ago). ``step`` is in seconds.
``GET /v1/metrics/names``, ``GET /v1/metrics/snapshot``
    Names of persisted series / a JSON dump of the live registry.
``POST /v1/metrics/otlp/test``
    Export once to the configured OTLP collector and report the outcome (platform administrators).
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import APIRouter, Query, Response

from olcf.api.deps import Authed, Ctx
from olcf.core.errors import BadRequest, Unavailable
from olcf.core.timeutil import from_iso, to_iso

router = APIRouter(tags=["metrics"])
PROM_TYPE = "text/plain; version=0.0.4; charset=utf-8"
_REL = re.compile(r"^-?(\d+(?:\.\d+)?)\s*(s|m|h|d|w)$")
_UNIT = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}


def parse_when(value: str | None, now: datetime, default: datetime | None = None) -> datetime:
    if value is None or value == "":
        if default is None:
            raise BadRequest("a time is required")
        return default
    v = value.strip()
    if v.lower() == "now":
        return now
    m = _REL.match(v)
    if m:
        return now - timedelta(seconds=float(m.group(1)) * _UNIT[m.group(2)])
    try:
        d = from_iso(v)
    except ValueError:
        raise BadRequest(f"cannot parse time '{value}': use ISO-8601 or a duration such as 15m, 6h, 7d") from None
    return d if d.tzinfo else d.replace(tzinfo=UTC)


def parse_labels(value: str | None) -> dict[str, str]:
    """``k=v,k2=v2`` or a JSON object."""
    if not value:
        return {}
    v = value.strip()
    if v.startswith("{"):
        try:
            obj = json.loads(v)
        except ValueError:
            raise BadRequest("labels must be 'k=v,k2=v2' or a JSON object") from None
        if not isinstance(obj, dict):
            raise BadRequest("labels must be an object")
        return {str(k): str(x) for k, x in obj.items()}
    out = {}
    for part in v.split(","):
        if not part.strip():
            continue
        k, sep, x = part.partition("=")
        if not sep or not k.strip():
            raise BadRequest(f"invalid label filter '{part}': expected key=value")
        out[k.strip()] = x.strip()
    return out


def _svc(ctx: Any) -> Any:
    svc = ctx.maybe_service("metrics")
    if svc is None:
        raise Unavailable("the metrics service is not running")
    return svc


@router.get("/metrics")
def prometheus(principal: Authed, ctx: Ctx) -> Response:
    """Prometheus scrape endpoint."""
    principal.require("events.read")
    svc = _svc(ctx)
    return Response(content=svc.render_prometheus(), media_type=PROM_TYPE)


@router.get("/metrics/series")
def series(principal: Authed, ctx: Ctx, name: str = Query(..., min_length=1, max_length=128), labels: str | None = None, since: str | None = "1h", until: str | None = "now",
           step: int | None = Query(None, ge=15, le=86400), max_points: int = Query(600, ge=10, le=5000)) -> dict[str, Any]:
    principal.require("events.read")
    svc = _svc(ctx)
    now = datetime.now(UTC)
    t_to = parse_when(until, now, now)
    t_from = parse_when(since, now, now - timedelta(hours=1))
    if t_from >= t_to:
        raise BadRequest("'since' must be earlier than 'until'")
    if (t_to - t_from) > timedelta(days=max(1, ctx.settings.retention.metric_days) + 1):
        t_from = t_to - timedelta(days=max(1, ctx.settings.retention.metric_days))
    out = svc.series.query(name, labels=parse_labels(labels), since=t_from, until=t_to, step=step, max_points=max_points)
    out["retention_days"] = ctx.settings.retention.metric_days
    return out


@router.get("/metrics/names")
def names(principal: Authed, ctx: Ctx, prefix: str | None = None, limit: int = Query(500, ge=1, le=2000)) -> dict[str, Any]:
    principal.require("events.read")
    return {"items": _svc(ctx).series.names(prefix, limit)}


@router.get("/metrics/snapshot")
def snapshot(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("events.read")
    svc = _svc(ctx)
    svc.run_collectors()
    return {"at": to_iso(datetime.now(UTC)), "metrics": svc.snapshot()}


@router.post("/metrics/otlp/test")
def otlp_test(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("platform.admin")
    exporter = ctx.maybe_service("otlp")
    if exporter is None:
        raise Unavailable("OTLP export is not available")
    res = exporter.export_once()
    ctx.audit.record(principal, "metrics.otlp_test", resource_type="otlp", resource_id="default", outcome="success" if not res.get("error") else "failure")
    return res
