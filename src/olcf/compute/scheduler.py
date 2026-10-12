# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The scheduler loop (leader only): turns database state into planner inputs, applies the plan, and runs maintenance.

All *decisions* are made by the pure functions in :mod:`olcf.compute.planner`; this module only gathers facts and acts:

1. every tick it checks leadership (a follower does nothing; on becoming leader it repairs transient state with
   ``JobService.recover`` and asks remote providers what they are running);
2. ``hub.liveness_tick()`` recomputes online/degraded/offline, providers refresh their clusters (``maintain``),
   ``JobService.maintain()`` enforces timeouts, cancellations, lost-worker grace and monitors remote providers;
3. queued jobs are converted to :class:`JobView`, planned against nodes, clusters, quotas and reservations, and each placement is
   *claimed* (queued -> provisioning, atomically) and handed to the launch pool. Jobs that cannot be placed get a human readable
   ``wait_reason`` (and unrunnable requests are failed after a grace period instead of waiting forever).

The loop sleeps for ``compute.scheduler_interval_seconds`` and wakes immediately when a job is submitted, finishes or a node
changes (``wake()``).
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

from sqlalchemy import select, update

from olcf.compute import schema as C
from olcf.compute.jobs import JobService
from olcf.compute.planner import (
    ClusterView,
    JobView,
    PlannerConfig,
    PlanResult,
    PoolView,
    QueueView,
    RunningView,
    infeasible_reason,
    plan,
)
from olcf.compute.providers import ProviderManager
from olcf.compute.registry import (
    ACTIVE_JOB_STATES,
    ACTIVE_SESSION_STATES,
    ComputeRegistry,
    worker_allocation_summary,
)
from olcf.compute.runtimes import RuntimeCatalog
from olcf.core.context import AppContext
from olcf.core.errors import OlcfError
from olcf.core.hostruntime import HOST_RUNTIME_ID
from olcf.core.timeutil import from_iso
from olcf.db.util import fetch_all

log = logging.getLogger("olcf.compute.scheduler")


def _epoch(value: str | None, default: float) -> float:
    if not value:
        return default
    try:
        return from_iso(value).timestamp()
    except ValueError:
        return default


def job_view(row: dict[str, Any], now: float) -> JobView:
    """Planner view of a queued ``jobs`` row."""
    res = row.get("resources_request") or {}
    pol = (row.get("spec") or {}).get("policy") or {}
    pl = pol.get("placement") or {}
    return JobView(
        id=row["id"], workspace_id=row["workspace_id"], queue=row["queue"], priority=int(row["priority"]), queued_at=_epoch(row.get("queued_at") or row["created_at"], now),
        runtime_id=row.get("runtime_id"), engine=row.get("engine") or "spark", driver_cores=int(res.get("driver_cores", 1)), driver_memory_mb=int(res.get("driver_memory_mb", 1024)),
        executor_cores=int(res.get("executor_cores", 1)), executor_memory_mb=int(res.get("executor_memory_mb", 1024)), min_executors=int(res.get("min_executors", 1)),
        max_executors=res.get("max_executors"), max_cores=res.get("max_cores"), dynamic_allocation=bool(res.get("dynamic_allocation")), pool=pl.get("pool"),
        labels={str(k): str(v) for k, v in (pl.get("labels") or {}).items()}, node=pl.get("node"), provider=row.get("provider") or pl.get("provider"),
        not_before=_epoch(row.get("not_before"), 0.0) if row.get("not_before") else None, flexible=bool((row.get("spec") or {}).get("resources_defaulted")))


def allocation_cores(g: dict[str, Any]) -> int:
    return int(g.get("driver_cores") or 0) + int(g.get("executor_cores_total") or 0)


def allocation_memory(g: dict[str, Any]) -> int:
    return int(g.get("driver_memory_mb") or 0) + int(g.get("executors") or 0) * int(g.get("executor_memory_mb") or 0)


class Scheduler:
    """Background leader loop. ``start()``/``stop()`` are called from ``Module.start/stop``."""

    def __init__(self, ctx: AppContext, registry: ComputeRegistry, hub: Any, jobs: JobService, providers: ProviderManager, runtimes: RuntimeCatalog) -> None:
        self.ctx, self.registry, self.hub, self.jobs, self.providers, self.runtimes = ctx, registry, hub, jobs, providers, runtimes
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._was_leader = False
        self._last_provider_maintain = 0.0
        self._last_stats: dict[str, Any] = {"ticks": 0, "last_tick": None, "errors": 0, "last_error": None, "placed_last": 0, "queued_last": 0, "leader": False}
        self.hooks: list[Any] = []  # leader-only callables run every tick before planning (drain completion, ...)
        self._infeasible_since: dict[str, float] = {}
        self._auto_install_asked: dict[str, float] = {}
        jobs.wake = self.wake
        hub.on_hello(lambda _w, _h: self.wake())

    # ------------------------------------------------------------------ lifecycle
    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="olcf-scheduler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread:
            self._thread.join(timeout=10)

    def wake(self) -> None:
        self._wake.set()

    @property
    def stats(self) -> dict[str, Any]:
        return dict(self._last_stats)

    def _run(self) -> None:
        interval = max(0.1, float(self.ctx.settings.compute.scheduler_interval_seconds))
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception as exc:  # noqa: BLE001 - the loop must survive anything
                log.exception("scheduler tick failed")
                self._last_stats["errors"] += 1
                self._last_stats["last_error"] = f"{type(exc).__name__}: {exc}"
            self._wake.wait(interval)
            self._wake.clear()

    # ------------------------------------------------------------------ one pass
    def tick(self, *, force_leader: bool = False) -> PlanResult | None:
        """One scheduling pass. Returns the plan applied (``None`` when not the leader)."""
        leader = force_leader or self.ctx.leader.is_leader()
        self._last_stats["leader"] = leader
        if not leader:
            self._was_leader = False
            return None
        if not self._was_leader:
            self._was_leader = True
            self._on_became_leader()
        now = time.time()
        went_offline = self.hub.liveness_tick()
        for w in went_offline:
            log.warning("worker %s went offline", w["name"])
        if now - self._last_provider_maintain > 3.0:
            self._last_provider_maintain = now
            self.providers.maintain_all()
        self.jobs.maintain()
        for hook in list(self.hooks):
            try:
                hook()
            except Exception:  # noqa: BLE001 - a hook must never stop scheduling
                log.exception("scheduler hook failed")
        result = self.plan_and_dispatch(now)
        self._last_stats.update(ticks=self._last_stats["ticks"] + 1, last_tick=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), placed_last=len(result.placements))
        return result

    def _on_became_leader(self) -> None:
        log.info("this node is now the scheduler leader; recovering state")
        try:
            counts = self.jobs.recover()
            if any(counts.values()):
                log.info("recovery: %s", counts)
            self._reconcile_remote()
        except Exception:  # noqa: BLE001
            log.exception("recovery after leader change failed")

    def _reconcile_remote(self) -> None:
        """Remote systems may still run applications of jobs that finished while no leader was watching: stop those (never the reverse)."""
        for name, apps in self.providers.reconcile_all().items():
            try:
                provider = self.providers.get(name)
            except OlcfError:
                continue
            if provider.execution != "remote":
                continue
            for a in apps:
                if not a.job_id:
                    continue
                try:
                    row = self.jobs.get_row(a.job_id)
                except OlcfError:
                    continue
                if row["state"] in ("succeeded", "failed", "cancelled", "timed_out") and a.state in ("running", "pending"):
                    from olcf.compute.providers.base import AppHandle

                    log.warning("stopping orphaned %s application of finished job %s", name, a.job_id)
                    try:
                        provider.cancel(AppHandle(name, a.job_id, int(row["attempt"]), native=dict(a.native or {})), force=True)
                    except OlcfError as exc:
                        log.warning("could not stop orphan of %s: %s", a.job_id, exc.message)

    # ------------------------------------------------------------------ inputs
    def planner_config(self) -> PlannerConfig:
        c = self.ctx.settings.compute
        r = self.registry
        return PlannerConfig(
            starvation_seconds=float(r.setting("compute.starvation_seconds", 300)), aging_interval_seconds=float(r.setting("compute.aging_interval_seconds", 60)),
            default_provider=str(r.setting("compute.default_provider", "auto") or "auto"), strategy=str(r.setting("compute.placement_strategy", "spread")),
            driver_reserve_fraction=float(r.setting("compute.driver_reserve_fraction", 0.25)), driver_port_range=tuple(c.driver_port_range),  # type: ignore[arg-type]
            block_manager_port_range=tuple(c.block_manager_port_range), remote_providers=self.providers.remote_names())

    def build_inputs(self, now: float, *, with_queued: bool = True, extra_workspaces: set[str] | None = None) -> dict[str, Any]:
        """Snapshot for the planner. ``with_queued=False`` skips the (possibly large) queue scan: used to place interactive sessions immediately."""
        db = self.ctx.db
        with db.connect() as conn:
            queued = fetch_all(conn, select(C.jobs).where(C.jobs.c.state == "queued").order_by(C.jobs.c.queued_at.asc()).limit(20000)) if with_queued else []
            active = fetch_all(conn, select(C.jobs).where(C.jobs.c.state.in_(ACTIVE_JOB_STATES)))
            sessions = fetch_all(conn, select(C.sessions).where(C.sessions.c.state.in_(ACTIVE_SESSION_STATES)))
            clusters = fetch_all(conn, select(C.clusters).where(C.clusters.c.state.in_(("up", "degraded", "starting"))))
            all_clusters = fetch_all(conn, select(C.clusters.c.pool, C.clusters.c.runtime_id, C.clusters.c.state))
        alloc, ports = worker_allocation_summary(active, sessions, clusters)
        nodes = self.registry.node_views(alloc, ports)
        pool_runtimes: dict[str, set[str]] = {}
        for c in all_clusters:
            if c["state"] != "stopped":
                pool_runtimes.setdefault(c["pool"], set()).add(c["runtime_id"])
        for q in queued:
            if q.get("runtime_id") and q["engine"] == "spark":
                pl = (((q.get("spec") or {}).get("policy") or {}).get("placement") or {}).get("pool")
                if pl:
                    pool_runtimes.setdefault(pl, set()).add(q["runtime_id"])
        sharing = {n.id: max(1, len(pool_runtimes.get(n.pool, set()))) for n in nodes}
        used: dict[tuple[str, str], list[int]] = {}
        for src in list(active) + [{"resources_granted": s.get("resources") or {}} for s in sessions]:
            g = src.get("resources_granted") or {}
            if g.get("cluster_runtime"):
                u = used.setdefault((g["cluster_runtime"], g["cluster_pool"]), [0, 0])
                u[0] += int(g.get("executor_cores_total") or 0)
                u[1] += int(g.get("executors") or 0) * int(g.get("executor_memory_mb") or 0)
        cviews = []
        for c in clusters:
            wc = {nid: int(w.get("cores", 0)) for nid, w in (c.get("spark_workers") or {}).items() if w.get("state") in ("running", "starting")}
            wm = {nid: int(w.get("memory_mb", 0)) for nid, w in (c.get("spark_workers") or {}).items() if w.get("state") in ("running", "starting")}
            ms = c.get("last_status") or {}
            u = used.get((c["runtime_id"], c["pool"]), [0, 0])
            cviews.append(ClusterView(c["runtime_id"], c["pool"], "standalone", c["id"], wc, wm, max(u[0], int(ms.get("cores_used") or 0)), max(u[1], int(ms.get("memory_used_mb") or 0))))
        running = [RunningView(a["workspace_id"], a["queue"], allocation_cores(a.get("resources_granted") or {}), allocation_memory(a.get("resources_granted") or {})) for a in active]
        queues = {}
        for q in self.registry.list_queues():
            queues[q["name"]] = QueueView(q["name"], q["max_concurrency"], q["max_cores"], q["max_memory_mb"], q.get("workspace_limits") or {},
                                          {k: float(v) for k, v in (q.get("fair_share_weights") or {}).items()}, tuple(q.get("allowed_pools") or ()), bool(q.get("paused")))
        pools = {p["name"]: PoolView(p["name"], bool(p["reserved"]), frozenset(p.get("workspace_ids") or [])) for p in self.registry.list_pools()}
        ws_quota = self._workspace_quotas({j["workspace_id"] for j in queued} | (extra_workspaces or set()))
        return {"queued": queued, "nodes": nodes, "clusters": cviews, "queues": queues, "running": running, "pools": pools, "ws_quota": ws_quota, "sharing": sharing}

    def _workspace_quotas(self, ids: set[str]) -> dict[str, dict[str, int]]:
        if not ids:
            return {}
        from olcf.db import schema as S

        with self.ctx.db.connect() as conn:
            rows = fetch_all(conn, select(S.workspaces.c.id, S.workspaces.c.quota).where(S.workspaces.c.id.in_(sorted(ids))))
        out: dict[str, dict[str, int]] = {}
        for r in rows:
            q = r.get("quota") or {}
            lim = {k: int(q[k]) for k in ("max_concurrency", "max_cores") if isinstance(q.get(k), int)}
            if lim:
                out[r["id"]] = lim
        return out

    # ------------------------------------------------------------------ plan + apply
    def plan_and_dispatch(self, now: float) -> PlanResult:
        inp = self.build_inputs(now)
        queued = inp["queued"]
        self._last_stats["queued_last"] = len(queued)
        if not queued:
            self._infeasible_since.clear()
            return PlanResult()
        cfg = self.planner_config()
        by_id = {r["id"]: r for r in queued}
        views = [job_view(r, now) for r in queued]
        result = plan(views, inp["nodes"], inp["clusters"], inp["queues"], inp["running"], inp["pools"], now=now, cfg=cfg, workspace_quotas=inp["ws_quota"], runtimes_per_node=inp["sharing"])
        for p in result.placements:
            row = by_id[p.job_id]
            wait = max(0.0, now - _epoch(row.get("queued_at") or row["created_at"], now))
            claimed = self.jobs.claim(p.job_id, p, queue_wait=wait)
            if claimed is not None:
                self.jobs.launch_async(p.job_id)
        self._record_waits(result, by_id, inp, cfg, now)
        return result

    def _record_waits(self, result: PlanResult, by_id: dict[str, dict[str, Any]], inp: dict[str, Any], cfg: PlannerConfig, now: float) -> None:
        placed = {p.job_id for p in result.placements}
        for aid in list(self._infeasible_since):
            if aid not in by_id or aid in placed:
                self._infeasible_since.pop(aid, None)
        grace = float(self.registry.setting("compute.infeasible_grace_seconds", 300))
        for b in result.blocked:
            row = by_id.get(b.job_id)
            if row is None:
                continue
            reason = f"{b.code}: {b.reason}"
            if b.code == "no_runtime" and row.get("runtime_id"):
                try:
                    msg, hint = self.runtimes.explain_missing(row["runtime_id"])
                    reason = f"no_runtime: {msg}; {hint}"
                except Exception:  # noqa: BLE001 - the explanation is best effort; the plain reason above stays
                    reason += f"; run POST /v1/runtimes/{row['runtime_id']}/install"
                if row["runtime_id"] != HOST_RUNTIME_ID:  # the built-in runtime is not installable
                    self._maybe_auto_install(row["runtime_id"], now)
            elif b.code == "no_node":
                reason += "; no worker is connected: restart the server (it embeds one) or enrol a machine (olcf worker enroll-token), then check `olcf worker list`"
            if b.code in ("resources", "constraints", "no_runtime", "no_node"):
                jv = job_view(row, now)
                why = infeasible_reason(jv, inp["nodes"], cfg) if b.code in ("resources", "constraints") else None
                if why:
                    since = self._infeasible_since.setdefault(row["id"], now)
                    reason = f"infeasible: {why}"
                    if now - since >= grace:
                        self._fail_infeasible(row, why)
                        continue
                else:
                    self._infeasible_since.pop(row["id"], None)
            if reason[:512] != (row.get("wait_reason") or ""):
                with self.ctx.db.begin() as conn:
                    conn.execute(update(C.jobs).where(C.jobs.c.id == row["id"], C.jobs.c.state == "queued").values(wait_reason=reason[:512]))
                # one timeline event per *kind* of wait (not per wording change): "why is my job still queued" is answerable from the timeline alone
                code = reason.split(":", 1)[0]
                if code != (row.get("wait_reason") or "").split(":", 1)[0]:
                    self.jobs.add_event(row["id"], "queue_reason", {"code": code, "message": reason[:500]}, reason=reason[:500])

    def _fail_infeasible(self, row: dict[str, Any], why: str) -> None:
        self.jobs.transition(row["id"], "failed", f"the request can never be satisfied: {why}"[:500], expect=("queued",),
                             fields={"failure": {"category": "resource", "message": why, "remediation": "reduce the resource request, relax the placement constraints or enrol a larger node"},
                                     "failure_category": "resource", "wait_reason": None})

    def _maybe_auto_install(self, runtime_id: str, now: float) -> None:
        if not self.registry.setting("compute.auto_install_runtimes", False):
            return
        if now - self._auto_install_asked.get(runtime_id, 0) < 900:
            return
        self._auto_install_asked[runtime_id] = now
        try:
            op = self.runtimes.request_install_if_missing(runtime_id)
            if op:
                log.info("auto-installing runtime %s (operation %s)", runtime_id, op)
        except OlcfError as exc:
            log.warning("auto-install of %s failed: %s", runtime_id, exc.message)


__all__ = ["Scheduler", "allocation_cores", "allocation_memory", "job_view"]
