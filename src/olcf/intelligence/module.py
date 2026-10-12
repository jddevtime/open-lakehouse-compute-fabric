# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Intelligence module: registers the ``intelligence`` service, its routes, the ``intelligence`` health check, the runtime settings, the event-bus subscriber
that fills the inbox and the leader-only loop that turns the inbox into graph facts; then loads the optional sub-packages.

``olcf.intelligence.discovery`` and ``olcf.intelligence.governance`` each expose ``install(ctx, service)``. They are optional: a package that is not there is
skipped (only a ``ModuleNotFoundError`` naming that exact package; any other import error propagates). What ``install`` returns, if anything with ``routers`` /
``public_routers`` / ``start`` / ``stop`` (a :class:`~olcf.core.context.Module`), is adopted by this module: its routers are mounted and it is started and
stopped with it. The module can be left out of ``create_app(module_names=...)`` entirely; nothing else imports it.
"""

from __future__ import annotations

import importlib
import logging
from typing import Any

from sqlalchemy import Connection

from olcf.core.context import AppContext, Module
from olcf.core.health import HealthCheck
from olcf.intelligence import settings as rt
from olcf.intelligence.engine import Engine
from olcf.intelligence.graph import GraphStore
from olcf.intelligence.ingest import InboxProcessor, InboxWriter, LeaderLoop, health_check
from olcf.intelligence.registry import AssetRegistry
from olcf.intelligence.service import IntelligenceService

log = logging.getLogger("olcf.intelligence")

OPTIONAL_PACKAGES = ("olcf.intelligence.discovery", "olcf.intelligence.governance")


class IntelligenceModule(Module):
    """The module that installs the Intelligence service, routes, health check and background loops."""
    name = "intelligence"

    def __init__(self, ctx: AppContext, service: IntelligenceService, writer: InboxWriter, processor: InboxProcessor, loop: LeaderLoop) -> None:
        super().__init__()
        from olcf.intelligence import api

        self.ctx, self.service, self.writer, self.processor, self.loop = ctx, service, writer, processor, loop
        self.routers = [(api.router, "")]
        self.children: list[Any] = []
        self._unsubscribe: Any = None

    def adopt(self, child: Any) -> None:
        """Take over the routers, start and stop of a sub-package's module object."""
        self.children.append(child)
        self.routers.extend(getattr(child, "routers", None) or [])
        self.public_routers.extend(getattr(child, "public_routers", None) or [])

    def subscribe(self) -> None:
        """Attach the inbox writer to the event bus."""
        if self.ctx.settings.intelligence.enabled and self._unsubscribe is None:
            self._unsubscribe = self.ctx.events.subscribe(self.writer)

    def start(self) -> None:
        """Start the inbox loop (when enabled) and the sub-packages."""
        if self.ctx.settings.intelligence.enabled:
            self.loop.start()
        for child in self.children:
            fn = getattr(child, "start", None)
            if fn:
                fn()

    def stop(self) -> None:
        """Stop the loops, close today's run-time scan and detach from the event bus."""
        for child in reversed(self.children):
            fn = getattr(child, "stop", None)
            if fn:
                try:
                    fn()
                except Exception:  # noqa: BLE001
                    log.exception("intelligence sub-package failed to stop")
        self.loop.stop()
        if self._unsubscribe is not None:
            self._unsubscribe()
            self._unsubscribe = None
        try:
            self.processor.close()
        except Exception:  # noqa: BLE001
            log.exception("could not close the run-time scan")


def _load_optional(ctx: AppContext, service: IntelligenceService, package: str) -> Any:
    """Import ``package`` and call its ``install(ctx, service)``; ``None`` when the package does not exist (any other import error is a real error)."""
    try:
        mod = importlib.import_module(package)
    except ModuleNotFoundError as exc:
        if exc.name == package:
            return None
        raise
    fn = getattr(mod, "install", None)
    return fn(ctx, service) if fn is not None else None


def install(ctx: AppContext) -> IntelligenceModule:
    """Build the registry, graph store, engine and service, wire audit / health / settings / event subscription and load the optional sub-packages."""
    settings = ctx.maybe_service("settings")
    if settings is not None:
        rt.register(settings)

    def audit_merge(conn: Connection, action: str, asset_id: str, details: dict[str, Any]) -> None:
        # written on the merge's own connection: opening a second writer inside the graph transaction would wait for the lock this very transaction holds
        ctx.audit.record(None, action, resource_type="asset", resource_id=asset_id, workspace_id=details.get("workspace_id"), details={"action": action, **details}, conn=conn)

    registry = AssetRegistry(ctx.db, audit=audit_merge)
    graph = GraphStore(ctx.db, verified_days=lambda: int(rt.get(ctx, "intelligence.verified_evidence_days", 90)),
                       max_nodes=lambda: int(rt.get(ctx, "intelligence.max_lineage_nodes", 500)))
    engine = Engine(ctx.db, registry, graph, audit=ctx.audit, events=ctx.events)
    engine.pacer.limits = lambda: (int(rt.get(ctx, *rt.WRITE_PACING[0])), float(rt.get(ctx, *rt.WRITE_PACING[1])), float(rt.get(ctx, *rt.WRITE_PACING[2])))
    service = IntelligenceService(ctx, registry, graph, engine)
    ctx.register_service("intelligence", service)

    writer = InboxWriter(ctx.db, enabled=lambda: ctx.settings.intelligence.enabled)
    processor = InboxProcessor(ctx, engine)
    loop = LeaderLoop(ctx, "intelligence-inbox", processor.tick, lambda: max(0.2, float(ctx.settings.intelligence.inbox_poll_seconds)), log)
    service.inbox_writer, service.inbox_processor = writer, processor
    quick, deep = health_check(ctx, service, writer, processor)
    ctx.health.register(HealthCheck(name="intelligence", category="data", description="Intelligence asset graph (inbox depth and lag, last scans, open gaps; deep: inbox and graph round trip)",
                                    quick=quick, deep=deep, ttl=15.0, critical=False))

    module = IntelligenceModule(ctx, service, writer, processor, loop)
    module.subscribe()
    for package in OPTIONAL_PACKAGES:
        child = _load_optional(ctx, service, package)
        if child is not None:
            module.adopt(child)
    return module
