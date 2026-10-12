# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Discovery tables (migration stream ``intelligence_discovery``).

* ``discovery_sources`` - one row per place metadata is collected from: the platform itself (``local``), a mirror of the classic catalog (``catalog_mirror``), a Microsoft
  Fabric / Power BI tenant (``fabric``) or a saved Power BI scanner result (``scanner_import``). ``config`` holds only non-secret settings; credentials are *references* into
  the secret store (``secret_ref`` and the ``*_identity`` blocks of ``config``), never values. ``state`` is the connector's own checkpoint (for example the modified-workspaces
  watermark of an incremental Fabric scan, or Delta versions already seen); ``lock_*`` is the cross-replica "one running scan per source" lease; ``payload`` is the
  (compressed) saved scanner JSON of a ``scanner_import`` source.
* ``sampling_policies`` - the opt-in rules for sample rows and value profiling. There is **no default policy**: with no row (or ``max_rows = 0``) nothing is ever sampled.

Both tables are portable between SQLite and PostgreSQL (plain columns, JSON documents, no vendor operators).
"""

from __future__ import annotations

from sqlalchemy import JSON, Boolean, Column, Index, Integer, LargeBinary, String, Table, UniqueConstraint

from olcf.db.migrate import Migration, create_tables, drop_tables, register
from olcf.db.schema import ID, TS, metadata

SOURCE_KINDS: tuple[str, ...] = ("local", "fabric", "scanner_import", "catalog_mirror")
MASKING_MODES: tuple[str, ...] = ("none", "hash", "redact", "partial")
MAX_SAMPLE_ROWS = 100

discovery_sources = Table(
    "discovery_sources", metadata,
    Column("id", ID, primary_key=True),                                   # src_<ULID>
    Column("name", String(128), nullable=False),
    Column("kind", String(24), nullable=False),                           # local | fabric | scanner_import | catalog_mirror
    Column("enabled", Boolean, nullable=False, default=True),
    Column("system_key", String(48)),                                     # "local-default" for the source created at install; system sources cannot be deleted
    Column("config", JSON, nullable=False, default=dict),                 # non-secret settings; identities are references (secret_id / connection_id)
    Column("schedule", JSON, nullable=False, default=dict),               # {"incremental_seconds": int, "full_seconds": int}; 0 = never, absent = the platform default
    Column("secret_ref", ID),                                             # sec_ id of the metadata identity written through the API (a reference, not a value)
    Column("state", JSON, nullable=False, default=dict),                  # connector checkpoint (watermarks, versions already seen)
    Column("payload", LargeBinary),                                       # scanner_import: zlib-compressed scanResult JSON
    Column("payload_sha", String(64)),
    Column("last_scan_id", ID),
    Column("last_status", String(12)),                                    # running | succeeded | partial | failed | cancelled
    Column("last_scan_at", TS),                                           # when the last scan finished (or started, while running)
    Column("last_full_at", TS),                                           # start of the last full scan that was not failed / cancelled
    Column("last_incremental_at", TS),                                    # start of the last incremental scan that was not failed / cancelled
    Column("lock_scan_id", ID),                                           # the scan that holds the source lease
    Column("lock_holder", String(128)),                                   # node + thread that holds it
    Column("lock_expires_at", TS),                                        # lease renewed by a heartbeat while the scan runs; a stale lease can be taken over
    Column("cancel_scan_id", ID),                                         # a cancel request for this scan id (read by the running scan between batches)
    Column("version", Integer, nullable=False, default=1),                # increments on every configuration change
    Column("etag", String(64), nullable=False),
    Column("created_by", ID),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
    UniqueConstraint("name", name="uq_discovery_sources_name"),
    UniqueConstraint("system_key", name="uq_discovery_sources_system_key"),
    Index("ix_discovery_sources_kind", "kind", "enabled"),
)

sampling_policies = Table(
    "sampling_policies", metadata,
    Column("id", ID, primary_key=True),                                   # smp_<ULID>
    Column("scope_key", String(160), nullable=False),                     # "global" | "workspace:<id>" | "source:<id>" | "asset:<id>" (the narrowest scope wins)
    Column("workspace_id", ID),
    Column("source_id", ID),
    Column("asset_id", ID),
    Column("max_rows", Integer, nullable=False, default=0),               # 0..100; 0 disables sampling for the scope
    Column("masking", String(12), nullable=False, default="partial"),     # none | hash | redact | partial
    Column("exclude_classified", Boolean, nullable=False, default=True),  # never return columns that are classified (or look sensitive)
    Column("enabled", Boolean, nullable=False, default=True),
    Column("etag", String(64), nullable=False),
    Column("created_by", ID),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
    UniqueConstraint("scope_key", name="uq_sampling_policies_scope"),
    Index("ix_sampling_policies_ws", "workspace_id"),
    Index("ix_sampling_policies_asset", "asset_id"),
)

DISCOVERY_TABLES = [discovery_sources, sampling_policies]

register(Migration("intelligence_discovery", 1, "discovery sources and sampling policies", up=lambda c: create_tables(c, DISCOVERY_TABLES),
                   down=lambda c: drop_tables(c, reversed(DISCOVERY_TABLES))))
