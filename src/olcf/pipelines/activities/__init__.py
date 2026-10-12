# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Built-in pipeline activity types and the registry factory.

``build_registry()`` returns an :class:`~olcf.pipelines.activities.base.ActivityRegistry` containing the 14 built-in types (notebook, spark_job, sql, copy, script, http,
child_pipeline, wait, set_variable, if, for_each, switch, fail, webhook_wait) plus ``until`` and any third-party types found in the ``olcf.pipeline_activities``
entry-point group. A plugin that tries to replace a built-in type is rejected and reported through ``registry.rejected``.
"""

from __future__ import annotations

from olcf.pipelines.activities.base import (
    ACTIVITY_API_VERSION,
    ENTRY_POINT_GROUP,
    Activity,
    ActivityCancelled,
    ActivityContext,
    ActivityError,
    ActivityRegistry,
    Container,
    ValidationIssue,
)
from olcf.pipelines.activities.control import CONTROL_ACTIVITIES
from olcf.pipelines.activities.copy import COPY_ACTIVITIES
from olcf.pipelines.activities.http import HTTP_ACTIVITIES
from olcf.pipelines.activities.jobs import JOB_ACTIVITIES
from olcf.pipelines.activities.sql import SQL_ACTIVITIES

BUILTIN_ACTIVITIES: list[type[Activity]] = [*JOB_ACTIVITIES, *SQL_ACTIVITIES, *COPY_ACTIVITIES, *HTTP_ACTIVITIES, *CONTROL_ACTIVITIES]


def build_registry(*, plugins: bool = True) -> ActivityRegistry:
    """Create a registry with all built-in activity types and (optionally) the entry-point plugins."""
    reg = ActivityRegistry()
    for cls in BUILTIN_ACTIVITIES:
        reg.register(cls, source="builtin")
    if plugins:
        reg.load_entry_points()
    return reg


__all__ = [
    "ACTIVITY_API_VERSION", "BUILTIN_ACTIVITIES", "ENTRY_POINT_GROUP", "Activity", "ActivityCancelled", "ActivityContext", "ActivityError", "ActivityRegistry", "Container",
    "ValidationIssue", "build_registry",
]
