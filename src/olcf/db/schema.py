# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Core metadata schema (migration stream ``core``).

Other packages declare their own tables in ``olcf/<package>/schema.py`` against the same
``metadata`` object and register a migration stream (see ``olcf.db.migrate``).

Conventions
-----------
* Primary keys are prefixed ULID strings (``olcf.core.ids``).
* Timestamps are ISO-8601 UTC strings (``olcf.core.timeutil``).
* Flexible documents live in JSON columns; anything filtered/sorted on gets a real column.
* Soft delete uses ``deleted_at``; hard purge is a retention job.
"""

from __future__ import annotations

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    Index,
    Integer,
    LargeBinary,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
)

metadata = MetaData()

TS = String(32)
ID = String(64)
BigPK = BigInteger().with_variant(Integer, "sqlite")

schema_migrations = Table(
    "schema_migrations",
    metadata,
    Column("stream", String(64), primary_key=True),
    Column("version", Integer, primary_key=True),
    Column("description", String(255)),
    Column("applied_at", TS, nullable=False),
)

principals = Table(
    "principals",
    metadata,
    Column("id", ID, primary_key=True),
    Column("kind", String(16), nullable=False),  # user | service | worker | fabric
    Column("name", String(128), nullable=False, unique=True),
    Column("display_name", String(255)),
    Column("email", String(255)),
    Column("password_hash", String(255)),
    Column("is_active", Boolean, nullable=False, default=True),
    Column("must_change_password", Boolean, nullable=False, default=False),
    Column("source", String(64), nullable=False, default="local"),  # local | bootstrap | oidc:<provider> | system
    Column("external_id", String(255)),  # idp subject / object id
    Column("attrs", JSON, nullable=False, default=dict),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
    Column("last_login_at", TS),
    Index("ix_principals_external", "source", "external_id"),
)

role_bindings = Table(
    "role_bindings",
    metadata,
    Column("id", ID, primary_key=True),
    Column("principal_id", ID, nullable=False, index=True),
    Column("role", String(32), nullable=False),
    Column("scope_type", String(16), nullable=False, default="org"),
    Column("scope_id", String(64), nullable=False, default=""),  # '' for org scope
    Column("source", String(64), nullable=False, default="manual"),  # manual | oidc:<provider> | system
    Column("created_at", TS, nullable=False),
    Column("created_by", ID),
    UniqueConstraint("principal_id", "role", "scope_type", "scope_id", name="uq_role_binding"),
    Index("ix_role_bindings_scope", "scope_type", "scope_id"),
)

api_tokens = Table(
    "api_tokens",
    metadata,
    Column("id", ID, primary_key=True),
    Column("principal_id", ID, nullable=False, index=True),
    Column("kind", String(16), nullable=False),  # pat | refresh | enroll | worker
    Column("name", String(128)),
    Column("prefix", String(24), nullable=False, index=True),  # public lookup handle (not secret)
    Column("token_hash", String(128), nullable=False),
    Column("scopes", JSON, nullable=False, default=list),  # e.g. ["read"], ["mcp:read"], ["*"]
    Column("meta", JSON, nullable=False, default=dict),
    Column("created_at", TS, nullable=False),
    Column("expires_at", TS),
    Column("last_used_at", TS),
    Column("revoked_at", TS),
)

#: Access tokens (JWT) are stateless; a logout or a detected refresh-token reuse must still end them before ``exp``. Every node consults this table (through a
#: short in-memory cache, see ``TokenService``) so a revocation made on one node is honoured by all nodes that share the database. ``jti`` is either a token id or
#: ``fam:<family id>`` (every access token minted from one login session). Rows are useless after ``expires_at`` and are purged by the retention loop. Migration
#: stream ``core`` version 2 (not part of ``CORE_TABLES``, which is the frozen version-1 set).
revoked_tokens = Table(
    "revoked_tokens",
    metadata,
    Column("jti", String(96), primary_key=True),
    Column("principal_id", ID, index=True),
    Column("reason", String(64), nullable=False, default="logout"),
    Column("revoked_at", TS, nullable=False),
    Column("expires_at", TS, nullable=False, index=True),
)

workspaces = Table(
    "workspaces",
    metadata,
    Column("id", ID, primary_key=True),
    Column("name", String(64), nullable=False),
    Column("display_name", String(255)),
    Column("description", Text, nullable=False, default=""),
    Column("owner_id", ID),
    Column("tags", JSON, nullable=False, default=list),
    Column("settings", JSON, nullable=False, default=dict),
    Column("quota", JSON, nullable=False, default=dict),
    Column("git", JSON, nullable=False, default=dict),
    Column("etag", String(64), nullable=False),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
    Column("deleted_at", TS),
    Index("ix_workspaces_name", "name"),
)

items = Table(
    "items",
    metadata,
    Column("id", ID, primary_key=True),
    Column("workspace_id", ID, nullable=False, index=True),
    Column("type", String(32), nullable=False),
    Column("name", String(128), nullable=False),
    Column("folder", String(512), nullable=False, default=""),
    Column("description", Text, nullable=False, default=""),
    Column("owner_id", ID),
    Column("tags", JSON, nullable=False, default=list),
    Column("sensitivity", String(64)),
    Column("definition", JSON, nullable=False, default=dict),
    Column("version", Integer, nullable=False, default=1),
    Column("etag", String(64), nullable=False),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
    Column("deleted_at", TS),
    Index("ix_items_ws_type", "workspace_id", "type"),
    Index("ix_items_ws_name", "workspace_id", "name"),
)

item_versions = Table(
    "item_versions",
    metadata,
    Column("id", ID, primary_key=True),
    Column("item_id", ID, nullable=False, index=True),
    Column("version", Integer, nullable=False),
    Column("definition", JSON, nullable=False),
    Column("name", String(128)),
    Column("comment", String(512)),
    Column("created_at", TS, nullable=False),
    Column("created_by", ID),
    UniqueConstraint("item_id", "version", name="uq_item_version"),
)

connections = Table(
    "connections",
    metadata,
    Column("id", ID, primary_key=True),
    Column("workspace_id", ID, index=True),  # NULL => centrally shared connection
    Column("name", String(128), nullable=False),
    Column("type", String(32), nullable=False),  # local | s3 | minio | adls | onelake | jdbc | postgres | mysql | sqlserver | http | sftp ...
    Column("description", Text, nullable=False, default=""),
    Column("config", JSON, nullable=False, default=dict),  # metadata only - NEVER secret material
    Column("secret_id", ID),  # opaque reference into ``secrets``
    Column("shared", Boolean, nullable=False, default=False),
    Column("tags", JSON, nullable=False, default=list),
    Column("owner_id", ID),
    Column("etag", String(64), nullable=False),
    Column("last_test", JSON),  # {ok, at, detail, steps[]}
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
    Column("deleted_at", TS),
)

secrets = Table(
    "secrets",
    metadata,
    Column("id", ID, primary_key=True),
    Column("name", String(128), nullable=False),
    Column("provider", String(32), nullable=False, default="file"),
    Column("ciphertext", LargeBinary),  # envelope-encrypted (file provider); NULL for external vaults
    Column("external_ref", String(512)),  # path/key in an external vault
    Column("key_id", String(64)),
    Column("meta", JSON, nullable=False, default=dict),  # non-sensitive descriptors (kind, rotation info)
    Column("version", Integer, nullable=False, default=1),
    Column("scope_type", String(16), nullable=False, default="org"),
    Column("scope_id", String(64), nullable=False, default=""),
    Column("created_at", TS, nullable=False),
    Column("created_by", ID),
    Column("rotated_at", TS),
    Column("deleted_at", TS),
)

audit_log = Table(
    "audit_log",
    metadata,
    Column("seq", BigPK, primary_key=True, autoincrement=True),
    Column("id", ID, nullable=False, unique=True),
    Column("ts", TS, nullable=False, index=True),
    Column("actor_id", ID),
    Column("actor_name", String(128)),
    Column("actor_kind", String(16)),
    Column("action", String(96), nullable=False, index=True),
    Column("resource_type", String(48)),
    Column("resource_id", String(96)),
    Column("workspace_id", ID, index=True),
    Column("outcome", String(16), nullable=False, default="success"),
    Column("ip", String(64)),
    Column("correlation_id", String(64)),
    Column("details", JSON, nullable=False, default=dict),
    Column("prev_hash", String(64)),
    Column("hash", String(64)),
)

events = Table(
    "events",
    metadata,
    Column("seq", BigPK, primary_key=True, autoincrement=True),
    Column("id", ID, nullable=False, unique=True),
    Column("ts", TS, nullable=False, index=True),
    Column("type", String(96), nullable=False, index=True),
    Column("workspace_id", ID, index=True),
    Column("subject", String(128)),  # e.g. job id, worker id
    Column("payload", JSON, nullable=False, default=dict),
)

kv = Table(
    "kv",
    metadata,
    Column("key", String(191), primary_key=True),
    Column("value", JSON, nullable=False),
    Column("updated_at", TS, nullable=False),
)

idempotency_keys = Table(
    "idempotency_keys",
    metadata,
    Column("key", String(191), primary_key=True),
    Column("principal_id", ID, primary_key=True),
    Column("endpoint", String(191), primary_key=True),
    Column("request_hash", String(64), nullable=False),
    Column("status_code", Integer),
    Column("response", JSON),
    Column("created_at", TS, nullable=False),
    Column("expires_at", TS, nullable=False, index=True),
)

leader_leases = Table(
    "leader_leases",
    metadata,
    Column("name", String(96), primary_key=True),
    Column("holder", String(128), nullable=False),
    Column("term", Integer, nullable=False, default=1),
    Column("expires_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
)

operations = Table(
    "operations",
    metadata,
    Column("id", ID, primary_key=True),
    Column("kind", String(64), nullable=False),  # runtime.install | backup.run | restore.run | ...
    Column("state", String(16), nullable=False, default="running"),  # pending | running | succeeded | failed | cancelled
    Column("progress", Integer, nullable=False, default=0),
    Column("message", String(512)),
    Column("result", JSON),
    Column("error", JSON),
    Column("created_by", ID),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
)

CORE_TABLES = [
    principals, role_bindings, api_tokens, workspaces, items, item_versions, connections, secrets,
    audit_log, events, kv, idempotency_keys, leader_leases, operations,
]
