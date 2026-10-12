# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Catalog module: registers the ``catalog`` service and its routes."""

from __future__ import annotations

from olcf.core.context import AppContext, Module


class CatalogModule(Module):
    """The module that installs the catalog service and its routes."""
    name = "catalog"

    def __init__(self, ctx: AppContext) -> None:
        super().__init__()
        from olcf.catalog import api

        self.ctx = ctx
        self.routers = [(api.router, "")]


def install(ctx: AppContext) -> CatalogModule:
    from olcf.catalog.service import CatalogService

    ctx.register_service("catalog", CatalogService(ctx))
    return CatalogModule(ctx)
