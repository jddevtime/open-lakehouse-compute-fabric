# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Fabric module: registers the ``fabric`` service, the adapter routes and the health checks ``fabric-adapter``, ``fabric-auth`` and ``onelake``."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from olcf.core.context import AppContext, Module
from olcf.core.health import ComponentHealth, HealthCheck, Status, timed
from olcf.fabric import diagnostics as D
from olcf.fabric.config import FabricConfig
from olcf.fabric.service import FabricService

log = logging.getLogger("olcf.fabric")


class FabricModule(Module):
    """Module that registers the Fabric adapter: its routes, service and health checks."""
    name = "fabric"

    def __init__(self, ctx: AppContext, service: FabricService) -> None:
        super().__init__()
        from olcf.fabric import api

        self.ctx, self.service = ctx, service
        self.routers = [(api.router, "")]

    def start(self) -> None:
        self.service.start()

    def stop(self) -> None:
        self.service.stop()


def _handshake_age(svc: FabricService) -> tuple[float | None, dict[str, Any] | None]:
    hs = svc.last_handshake()
    if not hs:
        return None, None
    return (datetime.now(UTC) - datetime.fromisoformat(hs["received_at"])).total_seconds(), hs


def _inp(svc: FabricService, cfg: FabricConfig) -> D.DiagnosticsInput:
    broker, problem = svc.broker(cfg)
    return D.DiagnosticsInput(network=svc.network(cfg), broker=broker, broker_problem=problem, fabric_workspace_ids=[m.fabric_workspace_id for m in cfg.workspace_map],
                              allowed_tenants=cfg.allowed_tenants, audiences=cfg.audiences, public_url=svc.public_url(), last_handshake=svc.last_handshake(),
                              client_factory=svc.client_factory)


def register_health(ctx: AppContext, svc: FabricService) -> None:
    def adapter_quick() -> ComponentHealth:
        def run() -> tuple[Status, str, dict[str, Any]]:
            cfg = svc.config()
            if not cfg.enabled:
                return Status.UNKNOWN, "the Fabric integration is disabled", {}
            missing = [n for n, ok in (("workspace_map", bool(cfg.workspace_map)),) if not ok]
            age, hs = _handshake_age(svc)
            labels = {"workspaces_mapped": len(cfg.workspace_map), "last_handshake_age_s": None if age is None else int(age)}
            if missing:
                return Status.DEGRADED, "not configured: " + ", ".join(missing), labels
            if age is None:
                return Status.UNKNOWN, "configured; no Fabric notebook has connected yet", labels
            if hs and not hs["ok"]:
                return Status.DEGRADED, f"the last handshake ({int(age)} s ago) reported problems", labels
            if age > 7 * 86400:
                return Status.DEGRADED, f"the last handshake is {int(age // 86400)} days old", labels
            return Status.OK, f"configured; last handshake {int(age)} s ago", labels

        return timed("fabric-adapter", "integration", run)

    def adapter_deep() -> ComponentHealth:
        cfg = svc.config()
        res = D.run_diagnostics(_inp(svc, cfg))
        status = Status.OK if res["ok"] else (Status.DEGRADED if len(res["failed"]) < 3 else Status.DOWN)
        return ComponentHealth("fabric-adapter", status, res["detail"], category="integration", steps=res["steps"], mode="deep")

    def auth_quick() -> ComponentHealth:
        def run() -> tuple[Status, str, dict[str, Any]]:
            cfg = svc.config()
            if not cfg.allowed_tenants or not cfg.audiences:
                return Status.UNKNOWN, "no Entra tenant/audience configured (platform tokens with scope 'fabric' still work)", {}
            val = svc.auth.validator(cfg)
            return (Status.OK, f"{len(cfg.allowed_tenants)} tenant(s), {len(cfg.audiences)} audience(s); cached JWKS: {val.cached_tenants() if val else {}}", {}) if val else (Status.DEGRADED, "validator unavailable", {})

        return timed("fabric-auth", "integration", run)

    def auth_deep() -> ComponentHealth:
        cfg = svc.config()
        step = D._inbound(_inp(svc, cfg))  # noqa: SLF001 - same check the diagnostics run
        return ComponentHealth("fabric-auth", Status.OK if step["ok"] else Status.DOWN, step["detail"], category="integration", steps=[step], mode="deep")

    def onelake_quick() -> ComponentHealth:
        def run() -> tuple[Status, str, dict[str, Any]]:
            cfg = svc.config()
            broker, problem = svc.broker(cfg)
            if broker is None:
                return Status.UNKNOWN, f"no broker identity: {problem}", {}
            return Status.OK, f"broker identity {broker.client_id} configured", {"auto_create_lakehouses": cfg.auto_create_lakehouses}

        return timed("onelake", "integration", run)

    def onelake_deep() -> ComponentHealth:
        cfg = svc.config()
        inp = _inp(svc, cfg)
        steps = []
        token_step, token = D._entra_token(inp)  # noqa: SLF001
        steps.append(token_step)
        api_step, visible = D._fabric_api(inp, token)  # noqa: SLF001
        steps.append(api_step)
        steps.append(D._onelake(inp, visible))  # noqa: SLF001
        bad = [s["name"] for s in steps if not s["ok"]]
        return ComponentHealth("onelake", Status.OK if not bad else Status.DOWN, "reachable and authorised" if not bad else "failed at: " + ", ".join(bad), category="integration", steps=steps, mode="deep")

    ctx.health.register(HealthCheck("fabric-adapter", "integration", "Fabric adapter: configuration completeness and last handshake (deep: full diagnostics)", adapter_quick, adapter_deep, ttl=15))
    ctx.health.register(HealthCheck("fabric-auth", "integration", "Entra token validation (tenants, audiences, JWKS)", auth_quick, auth_deep, ttl=15))
    ctx.health.register(HealthCheck("onelake", "integration", "Broker identity and OneLake reachability", onelake_quick, onelake_deep, ttl=30))


def install(ctx: AppContext) -> FabricModule:
    svc = FabricService(ctx)
    ctx.register_service("fabric", svc)
    register_health(ctx, svc)
    return FabricModule(ctx, svc)
