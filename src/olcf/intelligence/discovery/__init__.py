# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Discovery for OLCF Intelligence: sources, connectors, scans, profiling, sensitive-data suggestions and opt-in sampling.

``install(ctx, service)`` is called by the Intelligence module (after the database migrations). It

* registers the built-in connectors (``local``, ``catalog_mirror``, ``fabric``, ``scanner_import``),
* creates ``service.discovery`` (:class:`~olcf.intelligence.discovery.service.DiscoveryService`: sources, scan runner, scheduler, sampling),
* registers the discovery runtime settings and the ``intelligence-discovery`` health check,
* creates the default enabled ``local`` source if it does not exist (idempotent), and
* returns a :class:`~olcf.core.context.Module` that mounts the discovery routes and starts / stops the scheduler with the application.

Nothing in this package starts Spark, a JVM or a worker: the local connector reads the Delta log with delta-rs, the Fabric connector speaks REST, and sampling uses delta-rs / DuckDB.
"""

from __future__ import annotations

from typing import Any

from olcf.core.context import AppContext, Module

__all__ = ["DiscoveryModule", "install", "register_builtin_connectors"]


def register_builtin_connectors() -> None:
    """Register the four built-in connector kinds (safe to call repeatedly)."""
    from olcf.intelligence.discovery import base
    from olcf.intelligence.discovery.catalog_mirror import KIND as MIRROR
    from olcf.intelligence.discovery.catalog_mirror import CatalogMirrorConnector
    from olcf.intelligence.discovery.fabric import FabricConnector
    from olcf.intelligence.discovery.local import KIND as LOCAL
    from olcf.intelligence.discovery.local import LocalConnector
    from olcf.intelligence.discovery.scanner_import import KIND as IMPORT
    from olcf.intelligence.discovery.scanner_import import ScannerImportConnector

    base.register_connector(LOCAL, LocalConnector, replace=True)
    base.register_connector(MIRROR, CatalogMirrorConnector, replace=True)
    base.register_connector("fabric", FabricConnector, replace=True)
    base.register_connector(IMPORT, ScannerImportConnector, replace=True)


class DiscoveryModule(Module):
    """The part of the Intelligence module that discovery contributes: its routes and the scheduler's lifecycle."""

    name = "intelligence-discovery"

    def __init__(self, discovery: Any) -> None:
        super().__init__()
        from olcf.intelligence.discovery import api

        self.discovery = discovery
        self.routers = [(api.router, "")]

    def start(self) -> None:
        """Create the default source if needed and start the scheduler."""
        self.discovery.start()

    def stop(self) -> None:
        """Stop the scheduler and cancel the scans of this process."""
        self.discovery.stop()


def install(ctx: AppContext, service: Any) -> DiscoveryModule:
    """Install discovery into the Intelligence ``service`` (see the module docstring)."""
    from olcf.core.health import HealthCheck
    from olcf.intelligence.discovery.service import DiscoveryService, register_settings

    settings = ctx.maybe_service("settings")
    if settings is not None:
        register_settings(settings)
    register_builtin_connectors()
    discovery = DiscoveryService(ctx, service)
    service.discovery = discovery
    discovery.ensure_default_source()
    ctx.health.register(HealthCheck(name="intelligence-discovery", category="data", description="Discovery sources, scan schedule, last scans and sampling state (database only)",
                                    quick=discovery.health, ttl=15.0, critical=False))
    return DiscoveryModule(discovery)
