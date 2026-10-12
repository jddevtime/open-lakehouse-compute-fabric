# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""REST API of the sample lakehouse (mounted under ``/v1``): ``POST /sample/install`` and ``GET /sample/status``."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Response
from pydantic import BaseModel, ConfigDict, Field

from olcf.api.deps import Authed, Ctx
from olcf.sample import installer, spec

router = APIRouter(tags=["sample"])


class InstallBody(BaseModel):
    """Request body for installing the sample workspace: workspace and lakehouse names and whether to reinstall."""
    model_config = ConfigDict(extra="forbid")
    workspace_name: str = Field(spec.WORKSPACE, min_length=1, max_length=64, description="Workspace to create or reuse")
    lakehouse_name: str = Field(spec.LAKEHOUSE, min_length=1, max_length=64, description="Lakehouse to create or reuse (letters, digits, underscores)")
    reinstall: bool = Field(False, description="Rewrite the data and reset notebooks, queries and the report to their original definitions")
    size: str = Field("standard", pattern="^[a-z]+$", description="standard (about 80k order lines) or small (fast, for demos and tests)")


@router.post("/sample/install")
def install_sample(body: InstallBody, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    """Create the sample retail workspace (idempotent).  201 when something was created, 200 when it was already installed."""
    result = installer.install_sample(ctx, principal, workspace_name=body.workspace_name, lakehouse_name=body.lakehouse_name, reinstall=body.reinstall, size=body.size)
    response.status_code = 201 if result["action"] in ("installed", "reinstalled") else 200
    return result


@router.get("/sample/status")
def sample_status(principal: Authed, ctx: Ctx, workspace_name: str = spec.WORKSPACE, lakehouse_name: str = spec.LAKEHOUSE) -> dict[str, Any]:
    """Whether the sample is installed, which dataset version, and what it contains."""
    return installer.status(ctx, principal, workspace_name=workspace_name, lakehouse_name=lakehouse_name)
