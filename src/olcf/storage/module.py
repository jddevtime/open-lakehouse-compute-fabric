# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Storage module: registers ``storage`` service, lakehouse item hooks, connection testers, health checks and routes."""

from __future__ import annotations

from typing import Any

from olcf.core.context import AppContext, Module


class StorageModule(Module):
    """Module that registers the storage, lakehouse and table routes and services."""
    name = "storage"

    def __init__(self, ctx: AppContext) -> None:
        super().__init__()
        from olcf.storage import api

        self.ctx = ctx
        self.routers = [(api.router, "")]


def _validate_roots(value: Any) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(x, str) and x.startswith("/") for x in value):
        from olcf.core.errors import BadRequest

        raise BadRequest("storage.local_allowed_roots must be a list of absolute directory paths")
    return value


def install(ctx: AppContext) -> StorageModule:
    from olcf.storage import delta_policy, lakehouse, testers
    from olcf.storage.service import StorageService

    settings = ctx.service("settings")

    def hours(v: Any) -> int:
        from olcf.core.errors import BadRequest

        if not isinstance(v, int) or v < 0:
            raise BadRequest("must be a non-negative integer (hours)")
        return v

    def profile(v: Any) -> str:
        from olcf.core.errors import BadRequest

        if v not in delta_policy.PROFILES:
            raise BadRequest(f"unknown profile; choose one of {sorted(delta_policy.PROFILES)}")
        return v

    settings.register("storage.local_allowed_roots", description="Extra absolute directories where local lakehouses may live (PlatformAdmin)", default=[], validator=_validate_roots)
    settings.register("storage.vacuum_min_retention_hours", description="Lowest VACUUM retention OLCF accepts; the safe default is 168 (7 days)", default=168, validator=hours)
    settings.register("storage.delta_interop_profile", description="Delta interoperability profile enforced before managed writes", default=delta_policy.DEFAULT_PROFILE, validator=profile)

    svc = StorageService(ctx)
    ctx.register_service("storage", svc)
    lakehouse.install_hooks(ctx, svc.registry)
    testers.install_testers(ctx, svc, svc.registry)
    testers.install_health(ctx, svc, svc.registry)
    return StorageModule(ctx)
