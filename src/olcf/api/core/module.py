# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Foundation module: identity, workspaces, items, connections, secrets, audit, events, health."""

from __future__ import annotations

from olcf.api.core import admin, identity, workspaces
from olcf.core.context import AppContext, Module


class CoreModule(Module):
    """The module that installs the foundation services and routes (identity, workspaces, items, connections, secrets, audit, events, administration).
    """
    name = "core"

    def __init__(self, ctx: AppContext) -> None:
        super().__init__()
        self.ctx = ctx
        self.routers = [(identity.router, ""), (workspaces.router, ""), (admin.router, "")]
        self.public_routers = []
        self.v1_public = [(admin.public_router, "")]


def install(ctx: AppContext) -> CoreModule:
    ctx.register_service("identity", identity.IdentityService(ctx))
    ctx.register_service("workspaces", workspaces.WorkspaceService(ctx))
    ctx.register_service("items", workspaces.ItemService(ctx))
    ctx.register_service("connections", workspaces.ConnectionService(ctx))
    ctx.register_service("operations", admin.OperationService(ctx))
    ctx.register_service("settings", admin.KVSettings(ctx))
    admin.register_health_checks(ctx)
    return CoreModule(ctx)
