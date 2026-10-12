# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Database-derived metrics: gauges that describe the *whole* control plane (queue depth, workers, Spark applications, Fabric submissions, alerts, webhooks, ...).

Because the source of truth is the shared database, any replica can answer a scrape with the same numbers; the gauges are marked ``scope="global"`` so only the
leader persists them to the series store. Collection is throttled (``cache_seconds``) so a busy Prometheus does not turn into a database load.

:meth:`Collectors.observe_new` is the leader-only incremental part: it turns finished jobs, job attempts and failure records of the audit log into histogram
observations / counters exactly once, using durable cursors in the ``kv`` table (so a failover neither loses nor duplicates them).
"""

from __future__ import annotations

import logging
import threading
import time
from collections import Counter as CCounter
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import and_, func, select

from olcf.core.timeutil import from_iso, to_iso
from olcf.db import schema as S
from olcf.db.util import fetch_all
from olcf.observability.metrics import MetricsRegistry
from olcf.pipelines.common import kv_get, kv_set, kv_set_if_absent

log = logging.getLogger("olcf.observability.collectors")

JOB_CURSOR = "metrics.job_cursor"
ATTEMPT_CURSOR = "metrics.attempt_cursor"
AUDIT_CURSOR = "metrics.audit_cursor"
ACTIVE_JOB_STATES = ("accepted", "validating", "queued", "provisioning", "running", "cancel_requested")


class Collectors:
    """Registers a registry collector that refreshes the global gauges (see module docstring)."""

    def __init__(self, ctx: Any, registry: MetricsRegistry, *, cache_seconds: float = 5.0, clock: Any = None) -> None:
        self.ctx, self.db, self.m = ctx, ctx.db, registry
        self.cache_seconds = cache_seconds
        self._clock = clock
        self._last = 0.0
        self._lock = threading.Lock()

    def now(self) -> datetime:
        return self._clock() if self._clock is not None else datetime.now(UTC)

    def install(self) -> None:
        self.m.add_collector("olcf-db", self.collect)

    # ------------------------------------------------------------------------------------------------------------------------------ gauges
    def collect(self, force: bool = False) -> None:
        with self._lock:
            if not force and time.monotonic() - self._last < self.cache_seconds:
                return
            self._last = time.monotonic()
        for fn in (self._platform, self._jobs, self._workers, self._spark, self._fabric, self._pipelines, self._webhooks, self._alerts):
            try:
                fn()
            except Exception:  # noqa: BLE001 - one broken source must not blank the whole exposition
                log.debug("collector %s failed", fn.__name__, exc_info=True)

    def _g(self, name: str, help_: str) -> Any:
        return self.m.gauge(name, help_, scope="global")

    def _platform(self) -> None:
        from olcf.version import __version__

        self.m.gauge("olcf_build_info", "Control-plane version").replace([({"version": __version__, "node": str(self.ctx.node_id)}, 1.0)])
        self.m.gauge("olcf_uptime_seconds", "Seconds since this control-plane process started").set(float(self.ctx.uptime))
        try:
            leader = 1.0 if self.ctx.leader.is_leader() else 0.0
        except Exception:  # noqa: BLE001
            leader = 0.0
        self.m.gauge("olcf_is_leader", "1 when this replica currently holds the leader lease").set(leader)
        try:
            self.m.gauge("olcf_db_ping_milliseconds", "Database round-trip time").set(float(self.ctx.db.ping()))
        except Exception:  # noqa: BLE001
            self.m.gauge("olcf_db_ping_milliseconds", "Database round-trip time").set(-1.0)
        self._g("olcf_events_head_sequence", "Sequence number of the newest platform event").set(float(self.ctx.events.head_seq()))

    def _jobs(self) -> None:
        from olcf.compute.schema import jobs as J
        from olcf.compute.schema import queues as Q

        now = self.now()
        with self.db.connect() as conn:
            by_state = conn.execute(select(J.c.state, func.count()).group_by(J.c.state)).all()
            queued = fetch_all(conn, select(J.c.queue, J.c.queued_at).where(J.c.state == "queued"))
            running = conn.execute(select(J.c.queue, func.count()).where(J.c.state.in_(("running", "provisioning"))).group_by(J.c.queue)).all()
            queues = fetch_all(conn, select(Q.c.name, Q.c.max_queued, Q.c.max_concurrency, Q.c.paused))
        self._g("olcf_jobs", "Jobs by state (all time)").replace([({"state": s}, float(n)) for s, n in by_state])
        depth: dict[str, int] = {}
        oldest: dict[str, float] = {}
        for r in queued:
            depth[r["queue"]] = depth.get(r["queue"], 0) + 1
            if r["queued_at"]:
                oldest[r["queue"]] = max(oldest.get(r["queue"], 0.0), (now - from_iso(r["queued_at"])).total_seconds())
        run_by_q = {q: int(n) for q, n in running}
        names = {q["name"] for q in queues} | set(depth) | set(run_by_q)
        self._g("olcf_queue_depth", "Jobs waiting in the queue").replace([({"queue": q}, float(depth.get(q, 0))) for q in sorted(names)])
        self._g("olcf_queue_oldest_wait_seconds", "Age of the oldest queued job").replace([({"queue": q}, oldest.get(q, 0.0)) for q in sorted(names)])
        self._g("olcf_queue_running", "Jobs running or provisioning in the queue").replace([({"queue": q}, float(run_by_q.get(q, 0))) for q in sorted(names)])
        sat = []
        for q in queues:
            if q["max_queued"]:
                sat.append(({"queue": q["name"]}, depth.get(q["name"], 0) / q["max_queued"]))
        self._g("olcf_queue_saturation_ratio", "Queue depth divided by the queue's max_queued limit").replace(sat)
        self._g("olcf_queue_paused", "1 when the queue is paused").replace([({"queue": q["name"]}, 1.0 if q["paused"] else 0.0) for q in queues])

    def _workers(self) -> None:
        from olcf.compute.schema import workers as W

        with self.db.connect() as conn:
            rows = fetch_all(conn, select(W.c.name, W.c.pool, W.c.state, W.c.scheduling, W.c.metrics, W.c.inventory, W.c.last_seen_at).where(W.c.revoked_at.is_(None)))
        now = self.now()
        states = CCounter((r["pool"], r["state"]) for r in rows)
        self._g("olcf_workers", "Workers by pool and state").replace([({"pool": p, "state": s}, float(n)) for (p, s), n in sorted(states.items())])
        cpu, mem, disk, rx, tx, seen = [], [], [], [], [], []
        for r in rows:
            lab = {"worker": r["name"], "pool": r["pool"]}
            m = r["metrics"] or {}
            if m.get("cpu_percent") is not None:
                cpu.append((lab, float(m["cpu_percent"])))
            if m.get("mem_total_mb") and m.get("mem_available_mb") is not None:
                mem.append((lab, 100.0 * (1.0 - float(m["mem_available_mb"]) / float(m["mem_total_mb"]))))
            if m.get("disk_total_gb") and m.get("disk_free_gb") is not None:
                disk.append((lab, 100.0 * (1.0 - float(m["disk_free_gb"]) / float(m["disk_total_gb"]))))
            if m.get("net_rx_bps") is not None:
                rx.append((lab, float(m["net_rx_bps"])))
            if m.get("net_tx_bps") is not None:
                tx.append((lab, float(m["net_tx_bps"])))
            if r["last_seen_at"]:
                seen.append((lab, (now - from_iso(r["last_seen_at"])).total_seconds()))
        self._g("olcf_worker_cpu_percent", "Worker CPU utilisation (last heartbeat)").replace(cpu)
        self._g("olcf_worker_memory_used_percent", "Worker memory utilisation (last heartbeat)").replace(mem)
        self._g("olcf_worker_disk_used_percent", "Worker disk utilisation (last heartbeat)").replace(disk)
        self._g("olcf_worker_network_receive_bytes_per_second", "Worker network receive rate").replace(rx)
        self._g("olcf_worker_network_transmit_bytes_per_second", "Worker network transmit rate").replace(tx)
        self._g("olcf_worker_last_seen_seconds", "Seconds since the worker's last heartbeat").replace(seen)

    def _spark(self) -> None:
        from olcf.compute.schema import spark_apps as A

        with self.db.connect() as conn:
            rows = fetch_all(conn, select(A.c.state, A.c.cores, A.c.executors))
        by_state = CCounter(r["state"] for r in rows)
        self._g("olcf_spark_applications", "Spark applications by state").replace([({"state": s}, float(n)) for s, n in sorted(by_state.items())])
        live = [r for r in rows if r["state"] in ("RUNNING", "WAITING")]
        self._g("olcf_spark_executors", "Executors of running Spark applications").set(float(sum(len(r["executors"] or []) for r in live)))
        self._g("olcf_spark_cores", "Cores held by running Spark applications").set(float(sum(int(r["cores"] or 0) for r in live)))

    def _fabric(self) -> None:
        try:
            from olcf.fabric.schema import fabric_handshakes as H
            from olcf.fabric.schema import fabric_submissions as FS
        except ImportError:
            return
        hour = to_iso(self.now().replace(microsecond=0) - timedelta(hours=1))
        with self.db.connect() as conn:
            by_state = conn.execute(select(FS.c.state, func.count()).group_by(FS.c.state)).all()
            last_hour = conn.execute(select(func.count()).select_from(FS).where(FS.c.received_at >= hour)).scalar() or 0
            last_hs = conn.execute(select(func.max(H.c.received_at)).where(H.c.ok.is_(True))).scalar()
        self._g("olcf_fabric_submissions", "Fabric submissions by state (all time)").replace([({"state": s}, float(n)) for s, n in by_state])
        self._g("olcf_fabric_submissions_last_hour", "Fabric submissions received during the last hour").set(float(last_hour))
        if last_hs:
            self._g("olcf_fabric_last_handshake_age_seconds", "Seconds since the last successful Fabric handshake").set((self.now() - from_iso(last_hs)).total_seconds())

    def _pipelines(self) -> None:
        from olcf.pipelines.schema import triggers as T

        with self.db.connect() as conn:
            rows = conn.execute(select(T.c.type, T.c.enabled, func.count()).where(T.c.deleted_at.is_(None)).group_by(T.c.type, T.c.enabled)).all()
        self._g("olcf_pipeline_triggers", "Triggers by type and enabled flag").replace([({"type": t, "enabled": "true" if e else "false"}, float(n)) for t, e, n in rows])

    def _webhooks(self) -> None:
        svc = self.ctx.maybe_service("webhooks")
        if svc is None:
            return
        st = svc.stats()
        self._g("olcf_webhook_deliveries", "Webhook deliveries by state (retained window)").replace([({"state": s}, float(n)) for s, n in st["deliveries_by_state"].items()])
        self._g("olcf_webhook_oldest_due_lag_seconds", "How late the oldest due webhook delivery is").set(float(st["oldest_due_lag_seconds"] or 0.0))

    def _alerts(self) -> None:
        from olcf.pipelines.schema import alerts as AL

        with self.db.connect() as conn:
            rows = conn.execute(select(AL.c.severity, func.count()).where(AL.c.state == "firing").group_by(AL.c.severity)).all()
        have = {s: int(n) for s, n in rows}
        self._g("olcf_alerts_firing", "Firing alerts by severity").replace([({"severity": s}, float(have.get(s, 0))) for s in ("critical", "warning", "info")])

    # ------------------------------------------------------------------------------------------------------------------------------ incremental observations (leader only)
    def start(self) -> None:
        now = to_iso(self.now())
        kv_set_if_absent(self.ctx, JOB_CURSOR, now)
        kv_set_if_absent(self.ctx, ATTEMPT_CURSOR, now)
        with self.db.connect() as conn:
            head = conn.execute(select(func.max(S.audit_log.c.seq))).scalar() or 0
        kv_set_if_absent(self.ctx, AUDIT_CURSOR, int(head))

    def observe_new(self) -> dict[str, int]:
        """Observe jobs / attempts / audit failures that appeared since the last pass. Safe to call repeatedly; call only on the leader."""
        out = {"jobs": 0, "attempts": 0, "audit": 0}
        for key, fn in (("jobs", self._observe_jobs), ("attempts", self._observe_attempts), ("audit", self._observe_audit)):
            try:
                out[key] = fn()
            except Exception:  # noqa: BLE001
                log.debug("observation %s failed", key, exc_info=True)
        return out

    def _observe_jobs(self) -> int:
        from olcf.compute.schema import jobs as J

        cursor = kv_get(self.ctx, JOB_CURSOR, to_iso(self.now()))
        with self.db.connect() as conn:
            rows = fetch_all(conn, select(J.c.kind, J.c.state, J.c.queue, J.c.started_at, J.c.finished_at, J.c.failure_category)
                             .where(J.c.finished_at.is_not(None), J.c.finished_at > cursor).order_by(J.c.finished_at.asc()).limit(2000))
        if not rows:
            return 0
        done = self.m.counter("olcf_jobs_finished_total", "Jobs that reached a terminal state")
        runtime = self.m.histogram("olcf_job_runtime_seconds", "Time from job start to its terminal state")
        for r in rows:
            done.inc(1.0, kind=r["kind"], state=r["state"], queue=r["queue"])
            if r["started_at"]:
                runtime.observe(max(0.0, (from_iso(r["finished_at"]) - from_iso(r["started_at"])).total_seconds()), kind=r["kind"], state=r["state"])
            if r["state"] == "failed":
                self.m.counter("olcf_job_failures_total", "Failed jobs by failure category").inc(1.0, category=r["failure_category"] or "unknown", kind=r["kind"])
        kv_set(self.ctx, JOB_CURSOR, rows[-1]["finished_at"])
        return len(rows)

    def _observe_attempts(self) -> int:
        from olcf.compute.schema import job_attempts as JA
        from olcf.compute.schema import jobs as J

        cursor = kv_get(self.ctx, ATTEMPT_CURSOR, to_iso(self.now()))
        with self.db.connect() as conn:
            rows = fetch_all(conn, select(JA.c.created_at, JA.c.started_at, J.c.kind).select_from(JA.join(J, J.c.id == JA.c.job_id))
                             .where(and_(JA.c.started_at.is_not(None), JA.c.started_at > cursor)).order_by(JA.c.started_at.asc()).limit(2000))
        if not rows:
            return 0
        h = self.m.histogram("olcf_job_startup_seconds", "Time from provisioning to the job running on a worker")
        for r in rows:
            h.observe(max(0.0, (from_iso(r["started_at"]) - from_iso(r["created_at"])).total_seconds()), kind=r["kind"])
        kv_set(self.ctx, ATTEMPT_CURSOR, rows[-1]["started_at"])
        return len(rows)

    def _observe_audit(self) -> int:
        cursor = int(kv_get(self.ctx, AUDIT_CURSOR, 0) or 0)
        a = S.audit_log
        with self.db.connect() as conn:
            rows = fetch_all(conn, select(a.c.seq, a.c.action, a.c.outcome).where(a.c.seq > cursor).order_by(a.c.seq.asc()).limit(5000))
        if not rows:
            return 0
        auth = self.m.counter("olcf_auth_failures_total", "Failed authentication / authorisation events from the audit log")
        storage = self.m.counter("olcf_storage_failures_total", "Failed storage operations recorded in the audit log")
        n = 0
        for r in rows:
            if r["outcome"] in (None, "success"):
                continue
            if r["action"].startswith(("auth.", "identity.", "token.")):
                auth.inc(1.0, action=r["action"])
                n += 1
            elif r["action"].startswith(("storage.", "lakehouse.", "data.")):
                storage.inc(1.0, action=r["action"])
                n += 1
        kv_set(self.ctx, AUDIT_CURSOR, rows[-1]["seq"])
        return n
