# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""ITEMS module: ``install(ctx)`` registers the services and routes of notebooks, environments, Spark job definitions, variable sets, Git sync and promotion.

Services (``ctx.service(name)``):

* ``notebooks`` - :class:`olcf.notebooks.service.NotebookService` (parse / serialise / diff / versions / import / export / run / session);
* ``environments`` - :class:`olcf.notebooks.environments.EnvironmentService` (publish, ``resolve(item_id|lock) -> EnvironmentSpec`` used by the compute controller);
* ``spark_jobs`` - :class:`olcf.notebooks.sparkjobs.SparkJobService`;
* ``variables`` - :class:`olcf.notebooks.variables.VariableService` (``resolve(ref, stage, ...)`` for pipelines and notebook parameters);
* ``git`` - :class:`olcf.notebooks.gitsync.GitService`;
* ``deployments`` - :class:`olcf.notebooks.promotion.DeploymentService`.

Item hooks (validate / export / import) are registered for ``notebook``, ``environment``, ``spark_job_definition`` and ``variable_set``; a health check ``items``
reports whether the tools the package needs (``git``, ``pip``, ``nbformat``) work.
"""

from __future__ import annotations

from olcf.core.context import AppContext, Module
from olcf.core.health import ComponentHealth, HealthCheck, Status


class ItemsModule(Module):
    """Module that registers the notebook, environment, variable set, Spark job definition, Git sync and promotion routes."""
    name = "notebooks"

    def __init__(self, ctx: AppContext) -> None:
        super().__init__()
        from olcf.notebooks import api, env_api, git_api, sparkjobs, variables

        self.ctx = ctx
        self.routers = [(api.router, ""), (env_api.router, ""), (sparkjobs.router, ""), (variables.router, ""), (git_api.router, "")]


def install(ctx: AppContext) -> ItemsModule:
    from olcf.notebooks import (
        schema as _schema,  # noqa: F401 - registers tables (migration stream "notebooks")
    )
    from olcf.notebooks import workspace_io
    from olcf.notebooks.environments import EnvironmentService
    from olcf.notebooks.gitsync import GitService
    from olcf.notebooks.promotion import DeploymentService
    from olcf.notebooks.service import NotebookService
    from olcf.notebooks.sparkjobs import SparkJobService
    from olcf.notebooks.variables import VariableService

    envs = EnvironmentService(ctx)
    ctx.register_service("environments", envs)
    ctx.register_service("variables", VariableService(ctx))
    notebooks = NotebookService(ctx)
    ctx.register_service("notebooks", notebooks)
    sjobs = SparkJobService(ctx)
    ctx.register_service("spark_jobs", sjobs)
    git = GitService(ctx)
    ctx.register_service("git", git)
    ctx.register_service("deployments", DeploymentService(ctx))
    workspace_io.register_hooks(ctx)

    def quick() -> ComponentHealth:
        problems: list[str] = []
        labels: dict[str, object] = {}
        gh = git.health()
        labels.update(gh)
        if not gh["git_available"]:
            problems.append("git is not installed (Git sync is unavailable)")
        eh = envs.health()
        labels["pip_available"] = eh["pip_available"]
        if not eh["pip_available"]:
            problems.append("pip is not usable (environment publication is unavailable)")
        try:
            labels["nbformat"] = notebooks.health()["nbformat"]
        except Exception as exc:  # noqa: BLE001 - reported as a degraded check
            problems.append(f"nbformat: {exc}")
        return ComponentHealth("items", Status.DEGRADED if problems else Status.OK, "; ".join(problems) or "notebooks, environments and Git sync are available", labels=labels,
                               category="items")

    ctx.health.register(HealthCheck("items", "items", "Notebook / environment / Git sync tooling (nbformat, pip, git)", quick, ttl=30))
    return ItemsModule(ctx)
