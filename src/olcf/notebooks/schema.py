# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Tables of the ITEMS package (migration stream ``notebooks``).

* ``environment_locks`` - the global, content-addressed store of published environment locks (the primary key *is* the lock hash, so a lock is
  immutable by construction: inserting the same content twice is a no-op, different content can never share a key).  Environment items only carry
  a pointer (``publications[].lock_hash``) so exports / git / version history stay small and a lock survives item restore.
* ``deployment_pipelines`` / ``deployments`` - dev -> test -> prod promotion definitions and their history.
"""

from __future__ import annotations

from sqlalchemy import JSON, Boolean, Column, Index, String, Table, Text

from olcf.core import ids as _ids
from olcf.db.migrate import Migration, create_tables, drop_tables, register
from olcf.db.schema import ID, TS, metadata

_ids.PREFIXES.setdefault("dpl", "deployment pipeline")
_ids.PREFIXES.setdefault("dep", "deployment")

environment_locks = Table(
    "environment_locks",
    metadata,
    Column("lock_hash", String(80), primary_key=True),  # "sha256:<hex>" of the canonical lock core
    Column("lock", JSON, nullable=False),
    Column("layer_key", String(80), nullable=False, index=True),
    Column("spec_hash", String(80)),
    Column("workspace_id", ID, index=True),  # first publisher (informational; locks are content, not workspace data)
    Column("created_at", TS, nullable=False),
    Column("created_by", ID),
)

deployment_pipelines = Table(
    "deployment_pipelines",
    metadata,
    Column("id", ID, primary_key=True),
    Column("name", String(128), nullable=False, unique=True),
    Column("description", Text, nullable=False, default=""),
    Column("stages", JSON, nullable=False, default=list),  # [{"stage": "dev", "workspace_id": "wks_..."}, ...] in promotion order
    Column("rules", JSON, nullable=False, default=dict),  # {"connections": {"<stage>": {"<name|id>": "<name|id>"}}, "variable_stage": {"<stage>": "<variable stage>"}}
    Column("owner_id", ID),
    Column("etag", String(64), nullable=False),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
    Column("deleted_at", TS),
)

deployments = Table(
    "deployments",
    metadata,
    Column("id", ID, primary_key=True),
    Column("pipeline_id", ID, nullable=False, index=True),
    Column("from_stage", String(32), nullable=False),
    Column("to_stage", String(32), nullable=False),
    Column("source_workspace_id", ID, nullable=False),
    Column("target_workspace_id", ID, nullable=False),
    Column("state", String(16), nullable=False, default="running"),  # running | succeeded | failed | planned
    Column("dry_run", Boolean, nullable=False, default=False),
    Column("plan", JSON, nullable=False, default=dict),
    Column("result", JSON, nullable=False, default=dict),
    Column("created_by", ID),
    Column("created_at", TS, nullable=False),
    Column("finished_at", TS),
    Index("ix_deployments_pipeline", "pipeline_id", "created_at"),
)

TABLES = [environment_locks, deployment_pipelines, deployments]

register(Migration("notebooks", 1, "environment locks, deployment pipelines", up=lambda c: create_tables(c, TABLES), down=lambda c: drop_tables(c, reversed(TABLES))))

__all__ = ["TABLES", "deployment_pipelines", "deployments", "environment_locks"]
