# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""CTRL module: the compute fabric (workers, providers, scheduler, jobs, sessions, runtimes, topology).

``install(ctx)`` builds the object graph and registers the services other modules use (CONTRACTS 4.1):

``compute``   :class:`ComputeService` - ``.jobs``, ``.sessions``, ``.runtimes``, ``.hub``, ``.registry``, ``.scheduler``, ``.providers``, ``.admin``, ``.credentials``, ``.topology()``, ``.run_spark_sql(...)``
``jobs``      :class:`~olcf.compute.jobs.JobService` (``submit/get/wait/cancel/list/logs_page``)
``sessions``  :class:`~olcf.compute.sessions.SessionService`
``runtimes``  :class:`~olcf.compute.runtimes.RuntimeCatalog` (``resolve(RuntimeRequest)``)

Routers (all under ``/v1``): ``/workers`` (+ enroll, test), ``/workers/ws`` (agent channel), ``/worker-pools``, ``/queues``, ``/providers``, ``/clusters``, ``/jobs``,
``/spark-applications``, ``/spark-ui/{cluster}/...``, ``/sessions`` (+ ``/{id}/ws``), ``/runtimes``, ``/compute`` (topology, test, spark-smoke).

``Module.start()`` starts the hub, the (leader-only) scheduler thread, the replica heartbeat and the embedded worker; ``stop()`` reverses that. The scheduler also runs the
leader hooks ``WorkerAdmin.drain_tick`` and ``SessionService.maintain``.
"""

from __future__ import annotations

import logging
import threading
from typing import Any

from olcf.compute import hub as hub_mod
from olcf.compute import jobs_api, runtimes, sessions_api, spark_ui, topology, workers_api
from olcf.compute.credentials import WorkerCredentials
from olcf.compute.embedded import EmbeddedWorker
from olcf.compute.hub import WorkerHub
from olcf.compute.jobs import JobService
from olcf.compute.logstore import LogStore
from olcf.compute.providers import ProviderManager
from olcf.compute.providers.base import ProviderEnv
from olcf.compute.registry import ComputeRegistry
from olcf.compute.runtimes import RuntimeCatalog
from olcf.compute.scheduler import Scheduler
from olcf.compute.sessions import SessionService
from olcf.compute.topology import TopologyService
from olcf.compute.workers_api import WorkerAdmin
from olcf.core.context import AppContext, Module
from olcf.security.rbac import Principal

log = logging.getLogger("olcf.compute")


class ComputeService:
    """Facade registered as ``ctx.service("compute")`` (CONTRACTS 4.1)."""

    def __init__(self, ctx: AppContext, registry: ComputeRegistry, hub: WorkerHub, logs: LogStore, providers: ProviderManager, runtimes_: RuntimeCatalog, jobs: JobService,
                 sessions: SessionService, scheduler: Scheduler, admin: WorkerAdmin, topo: TopologyService, embedded: EmbeddedWorker) -> None:
        self.ctx, self.registry, self.hub, self.logs, self.providers, self.runtimes = ctx, registry, hub, logs, providers, runtimes_
        self.jobs, self.sessions, self.scheduler, self.admin, self.topology_service, self.embedded = jobs, sessions, scheduler, admin, topo, embedded
        self.credentials: WorkerCredentials  # certificate/token renewal and expiry (set by install())

    def topology(self) -> dict[str, Any]:
        """The control-panel topology document (CONTRACTS 4.6)."""
        return self.topology_service.topology()

    def run_spark_sql(self, principal: Principal, workspace_id: str, sql: str, *, lakehouse: Any = None, limit: int = 1000, timeout: float = 120) -> dict[str, Any]:
        """Run SQL with Spark through a pooled hidden session; returns ``{columns, rows, row_count, truncated, elapsed_ms, ...}`` (see :meth:`SessionService.run_spark_sql`)."""
        return self.sessions.run_spark_sql(principal, workspace_id, sql, lakehouse=lakehouse, limit=limit, timeout=timeout)


class ComputeModule(Module):
    """The module that installs the compute services (hub, scheduler, jobs, sessions, runtimes, providers) and runs their background loops on the leader.
    """
    name = "compute"

    def __init__(self, ctx: AppContext, service: ComputeService) -> None:
        super().__init__()
        self.ctx, self.service = ctx, service
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.routers = [
            (hub_mod.router, ""), (workers_api.workers_router, "/workers"), (topology.worker_test_router, "/workers"), (workers_api.pools_router, "/worker-pools"),
            (workers_api.queues_router, "/queues"), (workers_api.providers_router, "/providers"), (workers_api.clusters_router, "/clusters"),
            (jobs_api.jobs_router, "/jobs"), (jobs_api.apps_router, "/spark-applications"), (spark_ui.ui_router, ""), (sessions_api.router, "/sessions"),
            (runtimes.router, "/runtimes"), (topology.topology_router, "/compute"),
        ]

    # ------------------------------------------------------------------ lifecycle
    def start(self) -> None:
        s = self.service
        try:
            s.hub.start()
        except Exception:  # noqa: BLE001
            log.exception("worker hub could not be bound to the event loop (it binds on the first agent connection)")
        s.scheduler.start()
        self._stop.clear()
        self._thread = threading.Thread(target=self._housekeeping, name="olcf-compute-housekeeping", daemon=True)
        self._thread.start()
        try:
            s.embedded.start()
        except Exception:  # noqa: BLE001
            log.exception("embedded worker could not be started")

    def stop(self) -> None:
        s = self.service
        self._stop.set()
        for step, fn in (("embedded worker", s.embedded.stop), ("scheduler", s.scheduler.stop), ("hub", s.hub.stop), ("sessions", s.sessions.close), ("jobs", s.jobs.close),
                         ("providers", s.providers.close), ("runtime operations", s.runtimes.close)):
            try:
                fn()
            except Exception:  # noqa: BLE001
                log.exception("compute %s failed to stop", step)
        try:
            s.logs.close_all()
        except Exception:  # noqa: BLE001
            log.exception("log store failed to close")
        if self._thread:
            self._thread.join(timeout=5)

    def _housekeeping(self) -> None:
        """Every replica: publish liveness; refresh the per-runtime health checks."""
        tick = 0
        while not self._stop.is_set():
            try:
                self.service.topology_service.heartbeat()
                if tick % 12 == 0:
                    self.service.topology_service.sync_runtime_checks()
            except Exception:  # noqa: BLE001
                log.exception("compute housekeeping failed")
            tick += 1
            self._stop.wait(5.0)


def install(ctx: AppContext) -> ComputeModule:
    registry = ComputeRegistry(ctx)
    registry.register_settings()
    registry.ensure_default_pool()
    registry.ensure_default_queue()
    registry.ensure_builtin_providers()
    logs = LogStore(ctx.settings.path("jobs"))
    hub = WorkerHub(ctx, registry, logs)
    providers = ProviderManager(ProviderEnv(ctx, registry, hub, logs))
    catalog = RuntimeCatalog(ctx, registry, hub)
    catalog.load_builtin_manifests()
    catalog.ensure_host_runtime()
    jobs = JobService(ctx, registry, hub, logs, catalog, providers)
    sessions = SessionService(ctx, registry, hub, jobs, providers, catalog)
    scheduler = Scheduler(ctx, registry, hub, jobs, providers, catalog)
    sessions.scheduler = scheduler
    admin = WorkerAdmin(ctx, registry, hub, providers)
    topo = TopologyService(ctx, registry, hub, jobs, providers, catalog, sessions)
    topo.scheduler = scheduler
    embedded = EmbeddedWorker(ctx, registry)
    creds = WorkerCredentials(ctx, registry, hub)
    hub.credentials = creds
    # queued Python-engine jobs follow the workers' runtime inventory: on every hello, on a changed runtime advertisement and on every scheduler pass (leader only)
    hub.on_hello(lambda worker, hello: jobs.rebind_host_python("worker connected"))
    hub.on_inventory_change(lambda worker_id: jobs.rebind_host_python("worker runtimes changed"))
    scheduler.hooks += [admin.drain_tick, sessions.maintain, creds.sweep_tick, topo.smoke_tick, jobs.rebind_host_python]
    service = ComputeService(ctx, registry, hub, logs, providers, catalog, jobs, sessions, scheduler, admin, topo, embedded)
    service.credentials = creds
    ctx.register_service("compute", service)
    ctx.register_service("jobs", jobs)
    ctx.register_service("sessions", sessions)
    ctx.register_service("runtimes", catalog)
    topo.register_health()
    creds.register_health()
    return ComputeModule(ctx, service)
