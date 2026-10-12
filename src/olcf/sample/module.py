# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Sample module: the ``sample`` service and the ``/v1/sample/*`` routes."""

from __future__ import annotations

from typing import Any

from olcf.core.context import AppContext, Module


class SampleService:
    """Programmatic access for other modules and the CLI: ``ctx.service("sample").install(principal, ...)``."""

    def __init__(self, ctx: AppContext) -> None:
        self.ctx = ctx

    def install(self, principal: Any, **kw: Any) -> dict[str, Any]:
        from olcf.sample import installer

        return installer.install_sample(self.ctx, principal, **kw)

    def status(self, principal: Any, **kw: Any) -> dict[str, Any]:
        from olcf.sample import installer

        return installer.status(self.ctx, principal, **kw)


class SampleModule(Module):
    """Module that registers the sample workspace installer routes."""
    name = "sample"

    def __init__(self, ctx: AppContext) -> None:
        super().__init__()
        from olcf.sample import api

        self.ctx = ctx
        self.routers = [(api.router, "")]


def install(ctx: AppContext) -> SampleModule:
    ctx.register_service("sample", SampleService(ctx))
    return SampleModule(ctx)
