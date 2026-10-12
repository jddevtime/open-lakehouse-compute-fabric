# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Registry of feature modules installed into the control plane.

Order matters only for ``start()``: earlier modules start first. Each entry is the import path of a module
exposing ``install(ctx) -> olcf.core.context.Module``. Missing modules are skipped with a warning so partial
checkouts still boot, but a release build must have all of them (checked by ``tests/unit/test_modules.py``).
"""

MODULES: list[str] = [
    "olcf.api.core.module",          # identity, workspaces, items, connections, secrets, audit, events, admin/health
    "olcf.security.federation",      # OIDC / Entra login + external bearer validation, identity providers API
    "olcf.storage.module",           # storage providers, lakehouses (files/tables), delta maintenance
    "olcf.catalog.module",           # logical catalog, lineage edges
    "olcf.sql.module",               # SQL engines (DuckDB/Spark), history, saved queries
    "olcf.compute.module",           # workers, pools, queues, providers, runtimes, jobs, sessions, spark apps/logs/ui
    "olcf.notebooks.module",         # notebook service, environments, spark job definitions, variable sets, git sync
    "olcf.pipelines.module",         # pipelines, triggers, webhooks, alerts
    "olcf.fabric.module",            # Microsoft Fabric Compute Extension adapter
    "olcf.intelligence.module",      # OLCF Intelligence: discovery, catalog graph, lineage, governance (optional, failure-isolated)
    "olcf.mcp.module",               # AI-agent endpoint (Model Context Protocol) + agent helpers
    "olcf.observability.module",     # metrics, prometheus/otel export, alert evaluation
    "olcf.ops.module",               # backup/restore, updates, diagnostics, support bundle, retention
    "olcf.sample.module",            # sample lakehouse, notebooks and dashboard installer
]
