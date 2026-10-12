# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""ORCH module: observability (metrics, Prometheus / OTLP export, alerting).

Services registered (CONTRACTS section 4.1)
    ``metrics``  :class:`~olcf.observability.metrics.MetricsRegistry` (``counter / gauge / histogram / record_sample``, ``render_prometheus``, ``.series`` store)
    ``alerts``   :class:`~olcf.observability.alerts.AlertService`
    ``otlp``     :class:`~olcf.observability.otel.OtlpExporter`
    ``tracer``   :class:`~olcf.observability.otel.Tracer`

Routes (under ``/v1``): ``/metrics``, ``/metrics/series|names|snapshot``, ``/metrics/otlp/test``, ``/alerts/...``.

Background work (all stopped by ``stop()``)
    * sampler (every replica): refreshes the database-derived gauges, persists the registry to ``metric_samples``; the *leader* additionally writes the global series,
      observes finished jobs / failure records, compacts and purges the series store;
    * alert evaluation (leader only, see :mod:`olcf.observability.alerts`);
    * OTLP exporter (every replica, only when ``observability.otlp.endpoint`` is set).

Health checks: ``metrics`` (sampler liveness and freshness; deep: a real write/read round trip and an OTLP export when configured).
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

from olcf.core.context import AppContext, Module
from olcf.core.health import ComponentHealth, HealthCheck, Status
from olcf.observability import alerts_api, correlation
from olcf.observability import api as metrics_api
from olcf.observability.alerts import AlertService
from olcf.observability.collectors import Collectors
from olcf.observability.metrics import MetricsRegistry, SeriesStore, parse_prometheus
from olcf.observability.middleware import make_lifespan_router
from olcf.observability.otel import OtlpExporter, Tracer
from olcf.pipelines.common import setting

log = logging.getLogger("olcf.observability")


class ObservabilityModule(Module):
    """Module that wires the metrics registry, time series store, collectors, exporter, tracer and alerts into the application and runs their background loop.
    """
    name = "observability"

    def __init__(self, ctx: AppContext, registry: MetricsRegistry, series: SeriesStore, collectors: Collectors, exporter: OtlpExporter, tracer: Tracer, alerts: AlertService) -> None:
        super().__init__()
        self.ctx, self.registry, self.series, self.collectors, self.exporter, self.tracer, self.alerts = ctx, registry, series, collectors, exporter, tracer, alerts
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.last_sample_at: float | None = None
        self.last_sample_rows = 0
        self.last_error: str | None = None
        self.routers = [(make_lifespan_router(registry, tracer), ""), (metrics_api.router, ""), (alerts_api.router, "/alerts")]

    # ------------------------------------------------------------------------------------------------------------------------------ lifecycle
    def start(self) -> None:
        correlation.install_log_filter()
        self.collectors.start()
        self.alerts.start()
        self.exporter.start()
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="olcf-metrics-sampler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        for step, fn in (("sampler", lambda: self._thread and self._thread.join(timeout=10)), ("alerts", self.alerts.stop), ("otlp", self.exporter.stop)):
            try:
                fn()
            except Exception:  # noqa: BLE001
                log.exception("observability %s failed to stop", step)
        correlation.uninstall_log_filter()

    # ------------------------------------------------------------------------------------------------------------------------------ sampler
    def _run(self) -> None:
        tick = 0
        while True:
            interval = float(setting(self.ctx, "observability.sample_interval_seconds", 15))
            if self._stop.wait(interval):
                return
            tick += 1
            try:
                self.sample_once(tick)
            except Exception as exc:  # noqa: BLE001
                self.last_error = f"{type(exc).__name__}: {exc}"
                log.exception("metrics sampling failed")

    def sample_once(self, tick: int = 1) -> int:
        """One sampling pass (also callable directly): collectors, leader-only observations, persistence, housekeeping."""
        leader = bool(self.ctx.leader.is_leader())
        if leader:
            self.collectors.observe_new()
        self.registry.run_collectors()
        rows = self.series.sample(leader=leader)
        self.last_sample_at = time.monotonic()
        self.last_sample_rows = rows
        self.last_error = None
        if leader and tick % 20 == 0:  # about every 5 minutes at the default interval
            self.series.compact()
            self.series.purge()
            self.alerts.purge()
            wh = self.ctx.maybe_service("webhooks")
            if wh is not None:
                wh.purge()
        return rows

    # ------------------------------------------------------------------------------------------------------------------------------ health
    def register_health(self) -> None:
        self.ctx.health.register(HealthCheck("metrics", "control-plane", "Metrics registry, series store and optional OTLP export", self._quick, self._deep, ttl=10))

    def _quick(self) -> ComponentHealth:
        interval = float(setting(self.ctx, "observability.sample_interval_seconds", 15))
        alive = bool(self._thread and self._thread.is_alive())
        if not alive:
            return ComponentHealth("metrics", Status.DOWN, "the metrics sampler is not running on this replica", category="control-plane")
        age = None if self.last_sample_at is None else time.monotonic() - self.last_sample_at
        labels = {"metrics": len(self.registry.names()), "last_sample_rows": self.last_sample_rows, "otlp": "enabled" if self.exporter.enabled else "disabled", "pending_spans": self.tracer.pending()}
        if self.last_error:
            return ComponentHealth("metrics", Status.DEGRADED, f"the last sampling pass failed: {self.last_error}", category="control-plane", labels=labels)
        if age is not None and age > max(60.0, interval * 4):
            return ComponentHealth("metrics", Status.DEGRADED, f"no sample was stored for {age:.0f} s (interval {interval:g} s)", category="control-plane", labels=labels)
        return ComponentHealth("metrics", Status.OK, f"{labels['metrics']} metrics registered; sampling every {interval:g} s", category="control-plane", labels=labels)

    def _deep(self) -> ComponentHealth:
        steps: list[dict[str, Any]] = []
        status = Status.OK

        def step(name: str, fn: Any) -> None:
            nonlocal status
            t0 = time.perf_counter()
            try:
                detail = fn()
                steps.append({"name": name, "ok": True, "detail": detail, "ms": round((time.perf_counter() - t0) * 1000, 1)})
            except Exception as exc:  # noqa: BLE001
                status = Status.DOWN
                steps.append({"name": name, "ok": False, "detail": f"{type(exc).__name__}: {exc}", "ms": round((time.perf_counter() - t0) * 1000, 1)})

        def roundtrip() -> str:
            c = self.registry.counter("olcf_health_probe_total", "Health probe counter")
            c.inc(1.0, probe="deep")
            parsed = parse_prometheus(self.registry.render_prometheus(collect=False))
            if not any(lbl.get("probe") == "deep" for lbl, _ in parsed.get("olcf_health_probe_total", [])):
                raise RuntimeError("probe counter missing from the Prometheus exposition")
            return "counter incremented and parsed back from the exposition"

        def store() -> str:
            n = self.sample_once()
            return f"sampled {n} rows; store holds {self.series.stats()['rows']} rows"

        def otlp() -> str:
            if not self.exporter.enabled:
                return "skipped: observability.otlp.endpoint is not set"
            res = self.exporter.export_once()
            bad = [k for k in ("metrics", "traces") if res.get(k, {}).get("ok") is False]
            if res.get("error") or bad:
                raise RuntimeError(res.get("error") or "; ".join(f"{k}: {res[k].get('error')}" for k in bad))
            return f"exported to {res['endpoint']}"

        step("registry round trip", roundtrip)
        step("series store write", store)
        step("OTLP export", otlp)
        return ComponentHealth("metrics", status, "deep check " + ("passed" if status is Status.OK else "failed"), category="control-plane", steps=steps, mode="deep")


def install(ctx: AppContext) -> ObservabilityModule:
    registry = MetricsRegistry()
    series = SeriesStore(ctx, registry)
    registry.series = series
    ctx.register_service("metrics", registry)
    collectors = Collectors(ctx, registry)
    collectors.install()
    tracer = Tracer()
    ctx.register_service("tracer", tracer)
    exporter = OtlpExporter(ctx, registry, tracer)
    exporter.register_settings()
    ctx.register_service("otlp", exporter)
    alerts = AlertService(ctx)
    alerts.register_settings()
    ctx.register_service("alerts", alerts)
    mod = ObservabilityModule(ctx, registry, series, collectors, exporter, tracer, alerts)
    mod.register_health()
    return mod
