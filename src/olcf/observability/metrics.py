# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""In-process metrics: :class:`MetricsRegistry` (counter / gauge / histogram with labels), Prometheus text exposition and the persistent time-series store behind
``GET /v1/metrics/series``.

The registry is registered as ``ctx.service("metrics")`` (CONTRACTS section 4.1). Other packages use only this surface and must tolerate its absence::

    m = ctx.maybe_service("metrics")
    m.counter("olcf_things_total", "help").inc(1.0, kind="x")      # labels are keyword arguments
    m.gauge("olcf_things", "help").set(3, queue="default")
    m.histogram("olcf_thing_seconds", "help").observe(0.25, kind="x")
    m.record_sample("olcf_job_queue_wait_seconds", 1.7, {"queue": "default"})   # observation of an existing metric, or an auto-created histogram

Thread safety
    Every metric owns a lock; a scrape takes consistent per-metric snapshots. Label sets per metric are capped (``max_series``, default 2000): new combinations beyond
    the cap are dropped and counted in ``olcf_metrics_dropped_series_total`` so a runaway label (a user id, a path) cannot exhaust memory.

Persistence (series)
    A sampler (:class:`SeriesStore`) writes the registry to ``metric_samples`` every ``observability.sample_interval_seconds``: counters and gauges as their current value,
    histograms as the mean/min/max of the observations of that interval plus a ``<name>_p95`` estimate. Process-local metrics are tagged with ``node=<node id>``;
    metrics marked ``scope="global"`` (derived from the shared database) are written once by the leader. A leader-only compaction folds raw rows into 5-minute and
    1-hour buckets and ``retention.metric_days`` bounds everything.
"""

from __future__ import annotations

import json
import logging
import math
import re
import threading
import time
from collections.abc import Callable, Iterable
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import delete, func, insert, select

from olcf.core.timeutil import from_iso, to_iso
from olcf.db.util import fetch_all
from olcf.observability.schema import metric_samples as MS

log = logging.getLogger("olcf.observability.metrics")

_NAME_RE = re.compile(r"[^a-zA-Z0-9_:]")
_LABEL_RE = re.compile(r"[^a-zA-Z0-9_]")
TIME_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0)
BYTE_BUCKETS = (1024.0, 16384.0, 262144.0, 1048576.0, 16777216.0, 134217728.0, 1073741824.0, 10737418240.0)
GENERIC_BUCKETS = (1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 5000.0, 10000.0, 100000.0)
CUMULATIVE_SUFFIXES = ("_total", "_count", "_sum")
LabelKey = tuple[tuple[str, str], ...]


def clean_name(name: str) -> str:
    n = _NAME_RE.sub("_", name)
    return n if n and not n[0].isdigit() else "_" + n


def clean_label(name: str) -> str:
    n = _LABEL_RE.sub("_", name)
    return n if n and not n[0].isdigit() else "_" + n


def label_key(labels: dict[str, Any] | None) -> LabelKey:
    if not labels:
        return ()
    return tuple(sorted((clean_label(str(k)), "" if v is None else str(v)) for k, v in labels.items()))


def _escape(v: str) -> str:
    return v.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _fmt_labels(key: LabelKey, extra: tuple[tuple[str, str], ...] = ()) -> str:
    items = [*key, *extra]
    if not items:
        return ""
    return "{" + ",".join(f'{k}="{_escape(v)}"' for k, v in items) + "}"


def _fmt(v: float) -> str:
    if math.isnan(v):
        return "NaN"
    if math.isinf(v):
        return "+Inf" if v > 0 else "-Inf"
    if v == int(v) and abs(v) < 1e15:
        return str(int(v))
    return repr(float(v))


def default_buckets(name: str) -> tuple[float, ...]:
    if name.endswith("_seconds") or "_seconds_" in name:
        return TIME_BUCKETS
    if name.endswith("_bytes"):
        return BYTE_BUCKETS
    return GENERIC_BUCKETS


class Metric:
    """Base of the three metric kinds. Series are keyed by their sorted label items."""

    kind = "untyped"

    def __init__(self, registry: MetricsRegistry, name: str, help_: str, *, scope: str = "node", max_series: int = 2000) -> None:
        self.registry, self.name, self.help, self.scope, self.max_series = registry, name, help_, scope, max_series
        self._lock = threading.Lock()
        self._series: dict[LabelKey, Any] = {}

    def _state(self, key: LabelKey) -> Any | None:
        st = self._series.get(key)
        if st is None:
            if len(self._series) >= self.max_series:
                self.registry._dropped(self.name)
                return None
            st = self._series[key] = self._new()
        return st

    def _new(self) -> Any:
        raise NotImplementedError

    def clear(self) -> None:
        with self._lock:
            self._series.clear()

    def remove(self, **labels: Any) -> None:
        with self._lock:
            self._series.pop(label_key(labels), None)

    def label_sets(self) -> list[dict[str, str]]:
        with self._lock:
            return [dict(k) for k in self._series]


class Counter(Metric):
    """A counter metric: a value per label set that only increases."""
    kind = "counter"

    def _new(self) -> list[float]:
        return [0.0]

    def inc(self, value: float = 1.0, /, **labels: Any) -> None:
        if value < 0 or math.isnan(value):
            return
        with self._lock:
            st = self._state(label_key(labels))
            if st is not None:
                st[0] += value

    def value(self, **labels: Any) -> float:
        with self._lock:
            st = self._series.get(label_key(labels))
            return st[0] if st else 0.0

    def samples(self) -> list[tuple[LabelKey, float]]:
        with self._lock:
            return [(k, v[0]) for k, v in self._series.items()]


class Gauge(Metric):
    """A gauge metric: a value per label set that can be set, raised and lowered."""
    kind = "gauge"

    def _new(self) -> list[float]:
        return [0.0]

    def set(self, value: float, /, **labels: Any) -> None:
        if math.isnan(value):
            return
        with self._lock:
            st = self._state(label_key(labels))
            if st is not None:
                st[0] = float(value)

    def inc(self, value: float = 1.0, /, **labels: Any) -> None:
        with self._lock:
            st = self._state(label_key(labels))
            if st is not None:
                st[0] += value

    def dec(self, value: float = 1.0, /, **labels: Any) -> None:
        self.inc(-value, **labels)

    def replace(self, values: Iterable[tuple[dict[str, Any], float]]) -> None:
        """Atomically replace *all* series (collectors: series that vanished from the source must vanish from the exposition)."""
        new = {label_key(lbl): [float(v)] for lbl, v in values}
        with self._lock:
            self._series = dict(list(new.items())[: self.max_series])

    def value(self, **labels: Any) -> float:
        with self._lock:
            st = self._series.get(label_key(labels))
            return st[0] if st else 0.0

    def samples(self) -> list[tuple[LabelKey, float]]:
        with self._lock:
            return [(k, v[0]) for k, v in self._series.items()]


class _HistState:
    __slots__ = ("count", "counts", "imax", "imin", "isum", "icount", "sum")

    def __init__(self, n: int) -> None:
        self.counts = [0] * n
        self.sum = 0.0
        self.count = 0
        self.isum = 0.0
        self.icount = 0
        self.imin = math.inf
        self.imax = -math.inf


class Histogram(Metric):
    """A histogram metric: observations counted into fixed buckets, with a sum and a count, per label set."""
    kind = "histogram"

    def __init__(self, registry: MetricsRegistry, name: str, help_: str, buckets: Iterable[float] | None = None, **kw: Any) -> None:
        super().__init__(registry, name, help_, **kw)
        b = sorted({float(x) for x in (buckets if buckets is not None else default_buckets(name)) if not math.isinf(float(x))})
        self.buckets: tuple[float, ...] = tuple(b)

    def _new(self) -> _HistState:
        return _HistState(len(self.buckets) + 1)

    def observe(self, value: float, /, **labels: Any) -> None:
        if math.isnan(value):
            return
        idx = len(self.buckets)
        for i, ub in enumerate(self.buckets):
            if value <= ub:
                idx = i
                break
        with self._lock:
            st = self._state(label_key(labels))
            if st is None:
                return
            st.counts[idx] += 1
            st.sum += value
            st.count += 1
            st.isum += value
            st.icount += 1
            st.imin = min(st.imin, value)
            st.imax = max(st.imax, value)

    def time(self, **labels: Any) -> Callable[[], None]:
        """``stop = h.time(kind="x"); ...; stop()`` observes the elapsed seconds."""
        t0 = time.perf_counter()
        return lambda: self.observe(time.perf_counter() - t0, **labels)

    def snapshot(self) -> list[tuple[LabelKey, list[int], float, int]]:
        """``[(labels, per-bucket counts (non-cumulative, len = buckets + 1), sum, count)]``."""
        with self._lock:
            return [(k, list(s.counts), s.sum, s.count) for k, s in self._series.items()]

    def take_interval(self) -> list[tuple[LabelKey, int, float, float, float, list[int]]]:
        """Consume the per-interval statistics: ``[(labels, n, mean, min, max, delta bucket counts)]`` (only series with new observations)."""
        out = []
        with self._lock:
            for k, s in self._series.items():
                if s.icount:
                    out.append((k, s.icount, s.isum / s.icount, s.imin, s.imax, list(s.counts)))
                    s.isum, s.icount, s.imin, s.imax = 0.0, 0, math.inf, -math.inf
        return out

    def quantile(self, q: float, /, **labels: Any) -> float | None:
        with self._lock:
            s = self._series.get(label_key(labels))
            counts = list(s.counts) if s else []
        return estimate_quantile(self.buckets, counts, q) if counts else None

    def count(self, **labels: Any) -> int:
        with self._lock:
            s = self._series.get(label_key(labels))
            return s.count if s else 0

    def total(self, **labels: Any) -> float:
        with self._lock:
            s = self._series.get(label_key(labels))
            return s.sum if s else 0.0


def estimate_quantile(buckets: tuple[float, ...], counts: list[int], q: float) -> float | None:
    """Linear interpolation inside the bucket that contains the quantile (the usual Prometheus ``histogram_quantile`` estimate)."""
    total = sum(counts)
    if total == 0:
        return None
    rank = q * total
    cum = 0.0
    lower = 0.0
    for i, c in enumerate(counts):
        if cum + c >= rank and c > 0:
            if i >= len(buckets):
                return buckets[-1] if buckets else None
            upper = buckets[i]
            return lower + (upper - lower) * ((rank - cum) / c)
        cum += c
        lower = buckets[i] if i < len(buckets) else lower
    return buckets[-1] if buckets else None


class MetricsRegistry:
    """Registry of named metrics. ``counter/gauge/histogram`` are get-or-create and idempotent (same name, same kind)."""

    #: the persistent :class:`SeriesStore` (attached by the observability module)
    series: Any = None

    def __init__(self, *, max_series: int = 2000, process_start: float | None = None) -> None:
        self._metrics: dict[str, Metric] = {}
        self._lock = threading.RLock()
        self._collectors: list[tuple[str, Callable[[], None]]] = []
        self.max_series = max_series
        self.process_start = process_start if process_start is not None else time.time()
        self._dropped_counts: dict[str, int] = {}

    # ------------------------------------------------------------------------------------------------------------------------------ creation
    def _get(self, cls: type[Metric], name: str, help_: str, **kw: Any) -> Any:
        name = clean_name(name)
        with self._lock:
            m = self._metrics.get(name)
            if m is not None:
                if not isinstance(m, cls):
                    raise TypeError(f"metric {name} is already registered as a {m.kind}, not a {cls.kind}")
                if help_ and not m.help:
                    m.help = help_
                return m
            m = cls(self, name, help_, max_series=self.max_series, **kw)
            self._metrics[name] = m
            return m

    def counter(self, name: str, help: str = "", *, scope: str = "node") -> Counter:  # noqa: A002 - matches the Prometheus client vocabulary
        return self._get(Counter, name, help, scope=scope)

    def gauge(self, name: str, help: str = "", *, scope: str = "node") -> Gauge:  # noqa: A002
        return self._get(Gauge, name, help, scope=scope)

    def histogram(self, name: str, help: str = "", buckets: Iterable[float] | None = None, *, scope: str = "node") -> Histogram:  # noqa: A002
        return self._get(Histogram, name, help, buckets=buckets, scope=scope)

    def get(self, name: str) -> Metric | None:
        with self._lock:
            return self._metrics.get(clean_name(name))

    def names(self) -> list[str]:
        with self._lock:
            return sorted(self._metrics)

    def remove(self, name: str) -> None:
        with self._lock:
            self._metrics.pop(clean_name(name), None)

    def clear(self) -> None:
        with self._lock:
            self._metrics.clear()

    def metrics(self) -> list[Metric]:
        with self._lock:
            return [self._metrics[k] for k in sorted(self._metrics)]

    def _dropped(self, name: str) -> None:
        with self._lock:
            self._dropped_counts[name] = self._dropped_counts.get(name, 0) + 1

    # ------------------------------------------------------------------------------------------------------------------------------ record_sample
    def record_sample(self, name: str, value: float, labels: dict[str, Any] | None = None, **_ignored: Any) -> None:
        """Record one measurement. Gauges are set, counters incremented, anything else (also unknown names) is observed in a histogram."""
        if value is None:
            return
        v = float(value)
        lab = dict(labels or {})
        m = self.get(name)
        if m is None:
            m = self.histogram(name, "")
        if isinstance(m, Gauge):
            m.set(v, **lab)
        elif isinstance(m, Counter):
            m.inc(v, **lab)
        else:
            m.observe(v, **lab)  # type: ignore[attr-defined]

    # ------------------------------------------------------------------------------------------------------------------------------ collectors
    def add_collector(self, name: str, fn: Callable[[], None]) -> None:
        """``fn`` refreshes gauges from an external source; it runs before every exposition / sampling pass (errors are logged and skipped)."""
        with self._lock:
            self._collectors = [(n, f) for n, f in self._collectors if n != name]
            self._collectors.append((name, fn))

    def run_collectors(self) -> None:
        with self._lock:
            cols = list(self._collectors)
        for name, fn in cols:
            try:
                fn()
            except Exception:  # noqa: BLE001
                log.debug("metrics collector %s failed", name, exc_info=True)

    # ------------------------------------------------------------------------------------------------------------------------------ exposition
    def render_prometheus(self, *, collect: bool = True) -> str:
        """Prometheus text format 0.0.4."""
        if collect:
            self.run_collectors()
        out: list[str] = []
        with self._lock:
            dropped = dict(self._dropped_counts)
        for m in self.metrics():
            if m.help:
                out.append(f"# HELP {m.name} {m.help.replace(chr(92), chr(92) * 2).replace(chr(10), ' ')}")
            out.append(f"# TYPE {m.name} {m.kind}")
            if isinstance(m, (Counter, Gauge)):
                for key, val in sorted(m.samples()):
                    out.append(f"{m.name}{_fmt_labels(key)} {_fmt(val)}")
            elif isinstance(m, Histogram):
                for key, counts, total, n in sorted(m.snapshot(), key=lambda t: t[0]):
                    cum = 0
                    for ub, c in zip(m.buckets, counts, strict=False):
                        cum += c
                        out.append(f"{m.name}_bucket{_fmt_labels(key, (('le', _fmt(ub)),))} {cum}")
                    out.append(f"{m.name}_bucket{_fmt_labels(key, (('le', '+Inf'),))} {n}")
                    out.append(f"{m.name}_sum{_fmt_labels(key)} {_fmt(total)}")
                    out.append(f"{m.name}_count{_fmt_labels(key)} {n}")
        out.append("# HELP olcf_metrics_dropped_series_total Label combinations dropped by the per-metric cardinality cap")
        out.append("# TYPE olcf_metrics_dropped_series_total counter")
        if dropped:
            for nm, n in sorted(dropped.items()):
                out.append(f'olcf_metrics_dropped_series_total{{metric="{_escape(nm)}"}} {n}')
        else:
            out.append("olcf_metrics_dropped_series_total 0")
        out.append("# HELP olcf_process_start_time_seconds Start time of this control-plane process")
        out.append("# TYPE olcf_process_start_time_seconds gauge")
        out.append(f"olcf_process_start_time_seconds {_fmt(self.process_start)}")
        return "\n".join(out) + "\n"

    def snapshot(self) -> list[dict[str, Any]]:
        """JSON-friendly dump (used by the OTLP exporter and diagnostics)."""
        out = []
        for m in self.metrics():
            if isinstance(m, Histogram):
                series = [{"labels": dict(k), "counts": c, "sum": s, "count": n} for k, c, s, n in m.snapshot()]
                out.append({"name": m.name, "kind": m.kind, "help": m.help, "buckets": list(m.buckets), "series": series})
            elif isinstance(m, (Counter, Gauge)):
                out.append({"name": m.name, "kind": m.kind, "help": m.help, "series": [{"labels": dict(k), "value": v} for k, v in m.samples()]})
        return out


# ---------------------------------------------------------------------------------------------------------------------------------------- prometheus parsing (tests / tooling)
_SAMPLE_RE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{(.*)\})?\s+(\S+)(?:\s+\d+)?$')
_LABEL_PAIR_RE = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')


def parse_prometheus(text: str) -> dict[str, list[tuple[dict[str, str], float]]]:
    """Parse text exposition back into ``{name: [(labels, value)]}`` (strict enough to catch malformed output)."""
    out: dict[str, list[tuple[dict[str, str], float]]] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = _SAMPLE_RE.match(line)
        if not m:
            raise ValueError(f"malformed sample line: {line!r}")
        labels = {k: v.replace('\\"', '"').replace("\\n", "\n").replace("\\\\", "\\") for k, v in _LABEL_PAIR_RE.findall(m.group(3) or "")}
        raw_v = m.group(4)
        val = {"+Inf": math.inf, "-Inf": -math.inf, "NaN": math.nan}.get(raw_v)
        out.setdefault(m.group(1), []).append((labels, val if val is not None else float(raw_v)))
    return out


# ---------------------------------------------------------------------------------------------------------------------------------------- series store
def _floor(dt: datetime, seconds: int) -> datetime:
    ts = int(dt.timestamp())
    return datetime.fromtimestamp(ts - ts % seconds, tz=UTC)


class SeriesStore:
    """Writes registry samples to ``metric_samples``, answers ``/v1/metrics/series`` queries, compacts and purges."""

    RAW = 15

    def __init__(self, ctx: Any, registry: MetricsRegistry, *, clock: Callable[[], datetime] | None = None) -> None:
        self.ctx, self.db, self.registry = ctx, ctx.db, registry
        self._clock = clock
        self._prev_hist: dict[tuple[str, LabelKey], list[int]] = {}
        self._lock = threading.Lock()

    def now(self) -> datetime:
        return self._clock() if self._clock is not None else datetime.now(UTC)

    # ------------------------------------------------------------------------------------------------------------------------------ write
    def sample(self, *, leader: bool) -> int:
        """Persist one sampling pass. Node-scoped metrics are always written (tagged ``node``), global ones only by the leader."""
        node = str(self.ctx.node_id)
        ts = to_iso(_floor(self.now(), self.RAW))
        rows: list[dict[str, Any]] = []

        def add(name: str, labels: dict[str, str], value: float, vmin: float | None = None, vmax: float | None = None, n: int = 1) -> None:
            if math.isnan(value) or math.isinf(value):
                return
            key = json.dumps(labels, sort_keys=True, separators=(",", ":"))[:512]
            rows.append({"ts": ts, "resolution": self.RAW, "name": name[:128], "labels_key": key, "labels": labels, "value": float(value),
                         "vmin": float(value if vmin is None else vmin), "vmax": float(value if vmax is None else vmax), "n": n})

        for m in self.registry.metrics():
            is_global = m.scope == "global"
            if is_global and not leader:
                continue
            extra = {} if is_global else {"node": node}
            if isinstance(m, (Counter, Gauge)):
                for key, val in m.samples():
                    add(m.name, {**dict(key), **extra}, val)
            elif isinstance(m, Histogram):
                for key, n, mean, lo, hi, counts in m.take_interval():
                    labels = {**dict(key), **extra}
                    add(m.name, labels, mean, lo, hi, n)
                    prev = self._prev_hist.get((m.name, key))
                    delta = [c - p for c, p in zip(counts, prev, strict=False)] if prev else counts
                    self._prev_hist[(m.name, key)] = counts
                    p95 = estimate_quantile(m.buckets, delta, 0.95)
                    if p95 is not None:
                        add(m.name + "_p95", labels, p95, p95, p95, n)
        if not rows:
            return 0
        with self.db.begin() as conn:
            for i in range(0, len(rows), 500):
                conn.execute(insert(MS), rows[i:i + 500])
        return len(rows)

    # ------------------------------------------------------------------------------------------------------------------------------ housekeeping
    def compact(self) -> dict[str, int]:
        """Fold raw rows older than 1 h into 5-minute buckets and 5-minute rows older than 24 h into 1-hour buckets (leader only)."""
        now = self.now()
        done = {"to_300": 0, "to_3600": 0}
        for src, dst, age in ((self.RAW, 300, timedelta(hours=1)), (300, 3600, timedelta(hours=24))):
            cutoff = to_iso(_floor(now - age, dst))
            while True:
                with self.db.begin() as conn:
                    rows = fetch_all(conn, select(MS).where(MS.c.resolution == src, MS.c.ts < cutoff).order_by(MS.c.ts.asc()).limit(20000))
                    if not rows:
                        break
                    buckets: dict[tuple[str, str, str], dict[str, Any]] = {}
                    for r in rows:
                        b = to_iso(_floor(from_iso(r["ts"]), dst))
                        k = (r["name"], r["labels_key"], b)
                        cum = r["name"].endswith(CUMULATIVE_SUFFIXES)
                        cur = buckets.get(k)
                        if cur is None:
                            buckets[k] = {"ts": b, "resolution": dst, "name": r["name"], "labels_key": r["labels_key"], "labels": r["labels"], "sum": r["value"] * r["n"], "n": r["n"],
                                          "vmin": r["vmin"], "vmax": r["vmax"], "last": r["value"], "cum": cum}
                        else:
                            cur["sum"] += r["value"] * r["n"]
                            cur["n"] += r["n"]
                            cur["vmin"] = min(x for x in (cur["vmin"], r["vmin"]) if x is not None) if r["vmin"] is not None or cur["vmin"] is not None else None
                            cur["vmax"] = max(x for x in (cur["vmax"], r["vmax"]) if x is not None) if r["vmax"] is not None or cur["vmax"] is not None else None
                            cur["last"] = r["value"]
                    ids = [r["id"] for r in rows]
                    for i in range(0, len(ids), 500):
                        conn.execute(delete(MS).where(MS.c.id.in_(ids[i:i + 500])))
                    out = [{"ts": b["ts"], "resolution": b["resolution"], "name": b["name"], "labels_key": b["labels_key"], "labels": b["labels"],
                            "value": b["last"] if b["cum"] else b["sum"] / max(1, b["n"]), "vmin": b["vmin"], "vmax": b["vmax"], "n": b["n"]} for b in buckets.values()]
                    for i in range(0, len(out), 500):
                        conn.execute(insert(MS), out[i:i + 500])
                    done["to_300" if dst == 300 else "to_3600"] += len(rows)
                if len(rows) < 20000:
                    break
        return done

    def purge(self, days: int | None = None) -> int:
        days = int(days if days is not None else self.ctx.settings.retention.metric_days)
        cutoff = to_iso(self.now() - timedelta(days=days))
        with self.db.begin() as conn:
            return conn.execute(delete(MS).where(MS.c.ts < cutoff)).rowcount

    # ------------------------------------------------------------------------------------------------------------------------------ read
    def names(self, prefix: str | None = None, limit: int = 500) -> list[str]:
        stmt = select(MS.c.name).distinct().order_by(MS.c.name).limit(limit)
        if prefix:
            stmt = stmt.where(MS.c.name.like(prefix.replace("%", "") + "%"))
        with self.db.connect() as conn:
            return [r[0] for r in conn.execute(stmt).all()]

    def query(self, name: str, *, labels: dict[str, str] | None = None, since: datetime | None = None, until: datetime | None = None, step: int | None = None,
              max_points: int = 600) -> dict[str, Any]:
        """Series of ``name`` as ``{"name", "step", "series": [{"labels", "points": [{"ts","value","min","max","last","n"}]}]}``.

        ``labels`` filters by equality (a series matches when it has all of the given labels). ``step`` (seconds) defaults to the smallest of
        15 / 60 / 300 / 900 / 3600 / 21600 s that keeps the result under ``max_points`` per series.
        """
        until = until or self.now()
        since = since or (until - timedelta(hours=1))
        if since >= until:
            return {"name": name, "step": step or self.RAW, "since": to_iso(since), "until": to_iso(until), "series": []}
        span = (until - since).total_seconds()
        if not step:
            for cand in (15, 60, 300, 900, 3600, 21600):
                if span / cand <= max_points:
                    step = cand
                    break
            else:
                step = 86400
        step = max(int(step), self.RAW)
        stmt = select(MS).where(MS.c.name == name, MS.c.ts >= to_iso(since), MS.c.ts <= to_iso(until)).order_by(MS.c.ts.asc()).limit(300000)
        with self.db.connect() as conn:
            rows = fetch_all(conn, stmt)
        cumulative = name.endswith(CUMULATIVE_SUFFIXES)
        groups: dict[str, dict[str, Any]] = {}
        for r in rows:
            lab = r["labels"] or {}
            if labels and any(str(lab.get(k)) != str(v) for k, v in labels.items()):
                continue
            g = groups.setdefault(r["labels_key"], {"labels": lab, "buckets": {}})
            b = to_iso(_floor(from_iso(r["ts"]), step))
            cur = g["buckets"].get(b)
            if cur is None:
                g["buckets"][b] = {"ts": b, "sum": r["value"] * r["n"], "n": r["n"], "min": r["vmin"] if r["vmin"] is not None else r["value"],
                                   "max": r["vmax"] if r["vmax"] is not None else r["value"], "last": r["value"]}
            else:
                cur["sum"] += r["value"] * r["n"]
                cur["n"] += r["n"]
                cur["min"] = min(cur["min"], r["vmin"] if r["vmin"] is not None else r["value"])
                cur["max"] = max(cur["max"], r["vmax"] if r["vmax"] is not None else r["value"])
                cur["last"] = r["value"]
        series = []
        for g in groups.values():
            pts = []
            for b in sorted(g["buckets"]):
                c = g["buckets"][b]
                pts.append({"ts": c["ts"], "value": c["last"] if cumulative else c["sum"] / max(1, c["n"]), "min": c["min"], "max": c["max"], "last": c["last"], "n": c["n"]})
            series.append({"labels": g["labels"], "points": pts})
        series.sort(key=lambda s: json.dumps(s["labels"], sort_keys=True))
        return {"name": name, "step": step, "since": to_iso(since), "until": to_iso(until), "cumulative": cumulative, "series": series}

    def stats(self) -> dict[str, Any]:
        with self.db.connect() as conn:
            n = conn.execute(select(func.count()).select_from(MS)).scalar() or 0
            oldest = conn.execute(select(func.min(MS.c.ts))).scalar()
            newest = conn.execute(select(func.max(MS.c.ts))).scalar()
        return {"rows": int(n), "oldest": oldest, "newest": newest}
