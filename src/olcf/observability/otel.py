# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Optional OpenTelemetry export over OTLP/HTTP **JSON** (no OpenTelemetry SDK dependency, only ``httpx``).

Disabled until ``observability.otlp.endpoint`` is set (for example ``http://otel-collector:4318``). Then, every ``observability.otlp.interval_seconds``:

* **metrics** - the registry snapshot is POSTed to ``<endpoint>/v1/metrics`` (counters as cumulative monotonic sums, gauges, histograms with explicit bounds). Process-local
  metrics are exported by every replica (resource attribute ``service.instance.id`` = node id); database-derived (``scope="global"``) metrics only by the leader, so a
  backend never sums the same number once per replica.
* **traces** - spans recorded by :class:`Tracer` (API requests when sampled, pipeline runs and their activities) are POSTed to ``<endpoint>/v1/traces``.

Authentication headers are never put in settings: ``observability.otlp.headers_secret`` names a secret (string ``"k=v,k2=v2"`` or an object) in the secrets store.
The endpoint is subject to the SSRF guard (an in-cluster collector must be listed in ``network.ssrf.allow``). Export failures are counted
(``olcf_otlp_export_failures_total``) and never affect the control plane; spans that could not be sent stay in a bounded buffer for the next attempt.
"""

from __future__ import annotations

import contextlib
import logging
import random
import threading
import time
from collections import deque
from collections.abc import Iterator
from typing import Any

import httpx

from olcf.core.errors import BadRequest, OlcfError
from olcf.observability import correlation
from olcf.observability.metrics import Counter, Gauge, Histogram, MetricsRegistry
from olcf.pipelines import ssrf

log = logging.getLogger("olcf.observability.otel")

SCOPE_NAME = "olcf"
MAX_SPANS = 10000
KIND_INTERNAL, KIND_SERVER = 1, 2
STATUS_OK, STATUS_ERROR = 1, 2


def _attr(k: str, v: Any) -> dict[str, Any]:
    if isinstance(v, bool):
        val: dict[str, Any] = {"boolValue": v}
    elif isinstance(v, int):
        val = {"intValue": str(v)}
    elif isinstance(v, float):
        val = {"doubleValue": v}
    else:
        val = {"stringValue": str(v)}
    return {"key": str(k), "value": val}


def _attrs(d: dict[str, Any] | None) -> list[dict[str, Any]]:
    return [_attr(k, v) for k, v in (d or {}).items() if v is not None]


def _nano(t: float) -> str:
    return str(int(t * 1e9))


class Tracer:
    """Collects finished spans in a bounded buffer. ``sample_rate`` applies to *new* traces (API requests); spans of runs are always kept when export is on."""

    def __init__(self, *, sample_rate: float = 0.1, max_spans: int = MAX_SPANS) -> None:
        self.sample_rate = sample_rate
        self.enabled = False
        self._spans: deque[dict[str, Any]] = deque(maxlen=max_spans)
        self._lock = threading.Lock()
        self.dropped = 0

    def should_sample(self, parent_sampled: bool | None = None) -> bool:
        if not self.enabled:
            return False
        if parent_sampled is not None:
            return parent_sampled
        if self.sample_rate >= 1.0:
            return True
        if self.sample_rate <= 0.0:
            return False
        return random.random() < self.sample_rate  # noqa: S311 - sampling decision, not security relevant

    def add(self, *, name: str, trace_id: str, span_id: str, parent_id: str | None, start: float, end: float, attributes: dict[str, Any] | None = None, error: str | None = None,
            kind: int = KIND_INTERNAL) -> None:
        if not self.enabled:
            return
        span = {"traceId": trace_id, "spanId": span_id, "name": name[:200], "kind": kind, "startTimeUnixNano": _nano(start), "endTimeUnixNano": _nano(max(end, start)),
                "attributes": _attrs(attributes), "status": {"code": STATUS_ERROR, "message": error[:500]} if error else {"code": STATUS_OK}}
        if parent_id:
            span["parentSpanId"] = parent_id
        with self._lock:
            if len(self._spans) == self._spans.maxlen:
                self.dropped += 1
            self._spans.append(span)

    @contextlib.contextmanager
    def span(self, name: str, *, attributes: dict[str, Any] | None = None, kind: int = KIND_INTERNAL) -> Iterator[dict[str, str]]:
        """Time a block as a span of the current trace (a new trace when none is bound)."""
        trace_id = correlation.get("trace_id") or correlation.new_trace_id()
        parent = correlation.get("span_id")
        sid = correlation.new_span_id()
        start = time.time()
        err: str | None = None
        with correlation.bind(trace_id=trace_id, span_id=sid) as ctx:
            try:
                yield ctx
            except BaseException as exc:
                err = f"{type(exc).__name__}: {exc}"
                raise
            finally:
                self.add(name=name, trace_id=trace_id, span_id=sid, parent_id=parent, start=start, end=time.time(), attributes=attributes, error=err, kind=kind)

    def drain(self, limit: int = 2000) -> list[dict[str, Any]]:
        with self._lock:
            n = min(limit, len(self._spans))
            return [self._spans.popleft() for _ in range(n)]

    def requeue(self, spans: list[dict[str, Any]]) -> None:
        with self._lock:
            for s in reversed(spans):
                if len(self._spans) < (self._spans.maxlen or MAX_SPANS):
                    self._spans.appendleft(s)
                else:
                    self.dropped += 1

    def pending(self) -> int:
        with self._lock:
            return len(self._spans)


def build_metrics_payload(registry: MetricsRegistry, *, resource: dict[str, Any], leader: bool, now: float | None = None) -> dict[str, Any]:
    """OTLP JSON ``ExportMetricsServiceRequest`` for the registry (cumulative temporality, process start as the start time)."""
    t = _nano(now if now is not None else time.time())
    start = _nano(registry.process_start)
    metrics: list[dict[str, Any]] = []
    for m in registry.metrics():
        if m.scope == "global" and not leader:
            continue
        if isinstance(m, Counter):
            pts = [{"attributes": _attrs(dict(k)), "startTimeUnixNano": start, "timeUnixNano": t, "asDouble": v} for k, v in m.samples()]
            if pts:
                metrics.append({"name": m.name, "description": m.help, "sum": {"dataPoints": pts, "aggregationTemporality": 2, "isMonotonic": True}})
        elif isinstance(m, Gauge):
            pts = [{"attributes": _attrs(dict(k)), "timeUnixNano": t, "asDouble": v} for k, v in m.samples()]
            if pts:
                metrics.append({"name": m.name, "description": m.help, "gauge": {"dataPoints": pts}})
        elif isinstance(m, Histogram):
            pts = [{"attributes": _attrs(dict(k)), "startTimeUnixNano": start, "timeUnixNano": t, "count": str(n), "sum": total, "bucketCounts": [str(c) for c in counts],
                    "explicitBounds": list(m.buckets)} for k, counts, total, n in m.snapshot()]
            if pts:
                metrics.append({"name": m.name, "description": m.help, "histogram": {"dataPoints": pts, "aggregationTemporality": 2}})
    return {"resourceMetrics": [{"resource": {"attributes": _attrs(resource)}, "scopeMetrics": [{"scope": {"name": SCOPE_NAME}, "metrics": metrics}]}]}


def build_traces_payload(spans: list[dict[str, Any]], *, resource: dict[str, Any]) -> dict[str, Any]:
    return {"resourceSpans": [{"resource": {"attributes": _attrs(resource)}, "scopeSpans": [{"scope": {"name": SCOPE_NAME}, "spans": spans}]}]}


def parse_headers(value: Any) -> dict[str, str]:
    """``"a=b,c=d"`` or ``{"a": "b"}`` -> dict (invalid entries rejected; header injection impossible)."""
    out: dict[str, str] = {}
    items: list[tuple[str, str]] = []
    if isinstance(value, dict):
        items = [(str(k), str(v)) for k, v in value.items()]
    elif isinstance(value, str):
        for part in value.split(","):
            if part.strip():
                k, _, v = part.partition("=")
                items.append((k.strip(), v.strip()))
    for k, v in items:
        if not k or any(c in k + v for c in "\r\n") or not k.replace("-", "").replace("_", "").isalnum():
            raise BadRequest("OTLP header names must be alphanumeric and values must not contain line breaks")
        out[k] = v
    return out


class OtlpExporter:
    """Periodic exporter (see module docstring). ``export_once`` is also what the diagnostics API and the deep health check call."""

    def __init__(self, ctx: Any, registry: MetricsRegistry, tracer: Tracer, *, transport: httpx.BaseTransport | None = None) -> None:
        self.ctx, self.registry, self.tracer = ctx, registry, tracer
        self._transport = transport  # tests inject a mock transport; production always uses the SSRF-guarded one
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.last: dict[str, Any] = {}

    # ------------------------------------------------------------------------------------------------------------------------------ settings
    def register_settings(self) -> None:
        st = self.ctx.maybe_service("settings")
        if st is None or "observability.otlp.endpoint" in st.registry:
            return

        def endpoint(v: Any) -> Any:
            if v in (None, ""):
                return ""
            if not isinstance(v, str):
                raise BadRequest("must be an http(s) URL or empty to disable export")
            try:
                ssrf.split_url(v, ssrf.policy_from_ctx(self.ctx))
            except OlcfError as exc:
                raise BadRequest(f"invalid OTLP endpoint: {exc.message}") from None
            return v.rstrip("/")

        def num(lo: float, hi: float) -> Any:
            def v(x: Any) -> Any:
                if isinstance(x, bool) or not isinstance(x, (int, float)) or not lo <= x <= hi:
                    raise BadRequest(f"must be a number between {lo:g} and {hi:g}")
                return x

            return v

        def flag(v: Any) -> Any:
            if not isinstance(v, bool):
                raise BadRequest("must be true or false")
            return v

        st.register("observability.otlp.endpoint", description="OTLP/HTTP base URL of a collector (e.g. http://otel-collector:4318); empty disables export", default="", validator=endpoint)
        st.register("observability.otlp.headers_secret", description="Id of a secret holding the OTLP request headers ('k=v,k2=v2' or an object)", default="")
        st.register("observability.otlp.interval_seconds", description="Seconds between OTLP exports", default=30, validator=num(1, 3600))
        st.register("observability.otlp.timeout_seconds", description="Timeout of one OTLP request", default=10, validator=num(1, 120))
        st.register("observability.otlp.metrics", description="Export metrics over OTLP", default=True, validator=flag)
        st.register("observability.otlp.traces", description="Export traces over OTLP", default=True, validator=flag)
        st.register("observability.otlp.trace_sample_rate", description="Fraction of API requests that start a trace (runs are always traced)", default=0.1, validator=num(0, 1))
        st.register("observability.sample_interval_seconds", description="Seconds between persisted metric samples (series API)", default=15, validator=num(5, 3600))

    def _cfg(self, key: str, default: Any) -> Any:
        from olcf.pipelines.common import setting

        return setting(self.ctx, "observability.otlp." + key, default)

    @property
    def endpoint(self) -> str:
        return str(self._cfg("endpoint", "") or "").rstrip("/")

    @property
    def enabled(self) -> bool:
        return bool(self.endpoint)

    def refresh_tracer(self) -> None:
        self.tracer.enabled = self.enabled and bool(self._cfg("traces", True))
        self.tracer.sample_rate = float(self._cfg("trace_sample_rate", 0.1))

    def resource(self) -> dict[str, Any]:
        from olcf.version import __version__

        return {"service.name": "olcf-control-plane", "service.version": __version__, "service.instance.id": str(self.ctx.node_id)}

    def _headers(self) -> dict[str, str]:
        sid = str(self._cfg("headers_secret", "") or "")
        if not sid:
            return {}
        try:
            return parse_headers(self.ctx.secrets.resolve(sid))
        except OlcfError as exc:
            raise BadRequest(f"OTLP headers secret is unusable: {exc.message}") from None

    # ------------------------------------------------------------------------------------------------------------------------------ lifecycle
    def start(self) -> None:
        self.refresh_tracer()
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="olcf-otlp", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)
            self._thread = None

    def _run(self) -> None:
        last = 0.0
        while not self._stop.wait(1.0):
            try:
                self.refresh_tracer()
                if not self.enabled or time.monotonic() - last < float(self._cfg("interval_seconds", 30)):
                    continue
                last = time.monotonic()
                self.export_once()
            except Exception:  # noqa: BLE001
                log.exception("OTLP export loop failed")

    # ------------------------------------------------------------------------------------------------------------------------------ export
    def _post(self, client: httpx.Client, path: str, payload: dict[str, Any], headers: dict[str, str]) -> None:
        resp = client.post(self.endpoint + path, json=payload, headers={"Content-Type": "application/json", **headers})
        if resp.status_code >= 300:
            raise RuntimeError(f"collector answered HTTP {resp.status_code}: {resp.text[:200]}")

    def export_once(self) -> dict[str, Any]:
        """Send metrics and pending spans now. Returns ``{enabled, metrics: {ok, error}, traces: {ok, spans, error}}`` (never raises)."""
        res: dict[str, Any] = {"enabled": self.enabled, "endpoint": self.endpoint}
        if not self.enabled:
            return res
        try:
            headers = self._headers()
            policy = ssrf.policy_from_ctx(self.ctx)
            ssrf.validate_url(self.endpoint, policy)
            timeout = float(self._cfg("timeout_seconds", 10))
            client = httpx.Client(transport=self._transport, timeout=timeout, trust_env=False) if self._transport is not None else ssrf.guarded_client(policy, timeout=timeout)
        except OlcfError as exc:
            res["error"] = exc.message
            self._fail("setup")
            self.last = res
            return res
        resource = self.resource()
        with client:
            if self._cfg("metrics", True):
                try:
                    self.registry.run_collectors()
                    leader = bool(self.ctx.leader.is_leader())
                    self._post(client, "/v1/metrics", build_metrics_payload(self.registry, resource=resource, leader=leader), headers)
                    res["metrics"] = {"ok": True}
                except Exception as exc:  # noqa: BLE001
                    res["metrics"] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"[:300]}
                    self._fail("metrics")
            if self._cfg("traces", True):
                spans = self.tracer.drain()
                if spans:
                    try:
                        self._post(client, "/v1/traces", build_traces_payload(spans, resource=resource), headers)
                        res["traces"] = {"ok": True, "spans": len(spans)}
                    except Exception as exc:  # noqa: BLE001
                        self.tracer.requeue(spans)
                        res["traces"] = {"ok": False, "spans": len(spans), "error": f"{type(exc).__name__}: {exc}"[:300]}
                        self._fail("traces")
                else:
                    res["traces"] = {"ok": True, "spans": 0}
        self.last = res
        return res

    def _fail(self, signal: str) -> None:
        with contextlib.suppress(Exception):
            self.registry.counter("olcf_otlp_export_failures_total", "Failed OTLP exports").inc(1.0, signal=signal)
