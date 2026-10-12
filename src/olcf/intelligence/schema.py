# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Intelligence core tables (migration stream ``intelligence``).

* ``assets`` - one row per thing the platform knows about (workspace, lakehouse, table, column, notebook, report ...), identified by a stable ``ast_<ULID>``.
  An asset is never deleted: ``status`` moves ``active -> removed`` (history and lineage of dropped objects stay queryable); ``placeholder`` marks an object that
  was only *referenced* (a table named in code that no discovery scan has found yet) and is merged into the real asset when one shows up.
* ``asset_aliases`` - every other identifier of an asset (OLCF item id, catalog entry id, Fabric GUID, OneLake path, qualified name ...). ``(kind, value)`` is the
  primary key, so two sources that name the same object can only ever end up on one asset.
* ``asset_versions`` - one row per change of an asset's (non-volatile) attributes, scan by scan: the schema history.
* ``edges`` - typed, directed relationships with a confidence and capped, de-duplicated evidence; identity is ``edge_hash``.
* ``scans`` - one metadata snapshot of one source; ``version`` increases monotonically per source.
* ``gaps`` - what discovery or analysis could not do, reported as data.
* ``inbox`` - the durable queue between the platform event bus and the graph (events are appended cheaply, translated and applied by a leader-only loop).
* ``analysis_cache`` - per definition asset: analyzer version + etag + a fingerprint of the resolution context of the last analysis and the assets / edges / gaps it produced (lets an unchanged definition
  be skipped *and* kept alive in a full scan).

Search over JSON columns uses ``cast(col, String).like(...)``, never JSON operators, so every statement is portable between SQLite and PostgreSQL.
"""

from __future__ import annotations

from sqlalchemy import (
    JSON,
    Column,
    Connection,
    Index,
    Integer,
    String,
    Table,
    Text,
    UniqueConstraint,
    inspect,
)

from olcf.db.migrate import Migration, add_column, create_tables, drop_tables, register
from olcf.db.schema import ID, TS, BigPK, metadata

assets = Table(
    "assets", metadata,
    Column("id", ID, primary_key=True),                       # ast_<ULID>
    Column("type", String(32), nullable=False),               # one of model.ASSET_TYPES
    Column("name", String(512), nullable=False),
    Column("path", String(1024)),                             # human path, e.g. "Sample Retail / retail_lakehouse / fact_sales"
    Column("workspace_id", ID),                               # OLCF workspace; NULL = external / tenant level (needs an organisation-wide intelligence.read)
    Column("source_id", ID),                                  # the discovery source (or "runtime" / "catalog") that first saw it
    Column("parent_id", ID),                                  # the CONTAINS parent (lakehouse of a table, table of a column ...)
    Column("attrs", JSON, nullable=False, default=dict),
    Column("description", Text, nullable=False, default=""),
    Column("description_source", String(16), nullable=False, default="scan"),   # scan | manual (a manual description is never overwritten by a scan)
    Column("tags", JSON, nullable=False, default=list),       # manual tags (PATCH); tags reported by sources live in attrs.source_tags
    Column("status", String(16), nullable=False, default="active"),             # active | removed | placeholder
    Column("first_seen_scan", ID),
    Column("last_seen_scan", ID),
    Column("removed_scan", ID),
    Column("etag", String(32), nullable=False),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
    Index("ix_assets_ws_type", "workspace_id", "type"),
    Index("ix_assets_parent", "parent_id"),
    Index("ix_assets_source_status", "source_id", "status"),
    Index("ix_assets_name", "name"),
    Index("ix_assets_type_status", "type", "status"),
)

asset_aliases = Table(
    "asset_aliases", metadata,
    Column("kind", String(32), primary_key=True),
    Column("value", String(768), primary_key=True),           # values longer than 700 characters are stored as "h:<sha256>" (see registry.alias_value)
    Column("asset_id", ID, nullable=False),
    Column("created_at", TS, nullable=False),
    Index("ix_asset_aliases_asset", "asset_id"),
)

asset_versions = Table(
    "asset_versions", metadata,
    Column("id", BigPK, primary_key=True, autoincrement=True),
    Column("asset_id", ID, nullable=False),
    Column("scan_id", ID),
    Column("attrs_hash", String(64), nullable=False),
    Column("attrs", JSON, nullable=False, default=dict),
    Column("changes", JSON, nullable=False, default=dict),    # {"columns_added": [...], "columns_removed": [...], "columns_changed": [...], "attrs_changed": [...]}
    Column("created_at", TS, nullable=False),
    Index("ix_asset_versions_asset", "asset_id", "id"),
    Index("ix_asset_versions_scan", "scan_id"),
)

edges = Table(
    "edges", metadata,
    Column("id", ID, primary_key=True),                       # edg_<ULID>
    Column("edge_hash", String(64), nullable=False),
    Column("src_id", ID, nullable=False),
    Column("dst_id", ID, nullable=False),
    Column("type", String(24), nullable=False),               # one of model.STORED_EDGE_TYPES
    Column("via_id", ID),                                     # the process asset of a TRANSFORMS edge
    Column("confidence", String(12), nullable=False),         # verified | derived | inferred | unknown
    Column("transformation", Text),
    Column("attrs", JSON, nullable=False, default=dict),
    Column("evidence", JSON, nullable=False, default=list),   # newest first, at most 20
    Column("observation_count", Integer, nullable=False, default=1),
    Column("source_id", ID),
    Column("first_seen_scan", ID),
    Column("last_seen_scan", ID),
    Column("removed_scan", ID),
    Column("first_seen_at", TS, nullable=False),
    Column("last_seen_at", TS, nullable=False),
    Column("last_verified_at", TS),                           # newest runtime observation (drives "unused" and "last write observed")
    Column("status", String(16), nullable=False, default="active"),   # active | removed
    UniqueConstraint("edge_hash", name="uq_edges_hash"),
    Index("ix_edges_src", "src_id", "type"),
    Index("ix_edges_dst", "dst_id", "type"),
    Index("ix_edges_via", "via_id"),
    Index("ix_edges_source_status", "source_id", "status"),
)

scans = Table(
    "scans", metadata,
    Column("id", ID, primary_key=True),                       # scn_<ULID>
    Column("source_id", ID, nullable=False),
    Column("mode", String(12), nullable=False),               # full | incremental
    Column("trigger", String(24), nullable=False),            # manual | schedule | startup | runtime | api | import
    Column("status", String(12), nullable=False),             # running | succeeded | partial | failed | cancelled
    Column("version", Integer, nullable=False),               # monotonically increasing per source
    Column("principal_id", ID),
    Column("started_at", TS, nullable=False),
    Column("finished_at", TS),
    Column("stats", JSON, nullable=False, default=dict),
    Column("error", Text),
    UniqueConstraint("source_id", "version", name="uq_scans_source_version"),
    Index("ix_scans_source", "source_id", "version"),
    Index("ix_scans_status", "status"),
)

gaps = Table(
    "gaps", metadata,
    Column("id", BigPK, primary_key=True, autoincrement=True),
    Column("gap_hash", String(64), nullable=False),           # sha256 over (source, kind, subject, detail, location): the same gap seen again is one row
    Column("scan_id", ID),                                    # the scan that first recorded it
    Column("last_scan_id", ID),                               # the scan that saw it last
    Column("source_id", ID, nullable=False),
    Column("asset_id", ID),                                   # the asset it is about, when known
    Column("kind", String(32), nullable=False),               # one of model.GAP_KINDS
    Column("origin", String(64)),                             # the connector / analyzer that reported it (model.Gap.source)
    Column("subject", String(1024), nullable=False),
    Column("detail", Text, nullable=False, default=""),
    Column("remediation", Text),
    Column("location", JSON, nullable=False, default=dict),
    Column("first_seen_at", TS, nullable=False),
    Column("last_seen_at", TS, nullable=False),
    Column("resolved_at", TS),
    UniqueConstraint("source_id", "gap_hash", name="uq_gaps_source_hash"),
    Index("ix_gaps_asset", "asset_id"),
    Index("ix_gaps_source", "source_id", "resolved_at"),
)

inbox = Table(
    "inbox", metadata,
    Column("seq", BigPK, primary_key=True, autoincrement=True),
    Column("type", String(64), nullable=False),
    Column("workspace_id", ID),
    Column("payload", JSON, nullable=False, default=dict),
    Column("received_at", TS, nullable=False),
    Column("attempts", Integer, nullable=False, default=0),
    Column("status", String(12), nullable=False, default="pending"),   # pending | done | failed | skipped
    Column("error", Text),
    Column("updated_at", TS),                                          # when the row last left ``pending`` (done / skipped / failed) or was re-queued
    Index("ix_inbox_status", "status", "seq"),
    Index("ix_inbox_received", "received_at"),
)

analysis_cache = Table(
    "analysis_cache", metadata,
    Column("asset_id", ID, primary_key=True),
    Column("kind", String(32), primary_key=True),             # model.DEFINITION_KINDS
    Column("analyzer_version", String(32), nullable=False),
    Column("etag", String(128)),
    Column("context_hash", String(64)),                       # fingerprint of the lineage-relevant resolution context (engine.context_fingerprint); NULL = analysed before it existed
    Column("analyzed_at", TS, nullable=False),
    Column("edge_count", Integer, nullable=False, default=0),
    Column("touched", JSON, nullable=False, default=dict),    # {"assets": [...], "edges": [...], "gaps": [...]}: kept alive when the analysis is skipped
)

INTELLIGENCE_TABLES = [assets, asset_aliases, asset_versions, edges, scans, gaps, inbox, analysis_cache]

register(Migration("intelligence", 1, "initial intelligence schema: assets, aliases, versions, edges, scans, gaps, inbox, analysis cache",
                   up=lambda c: create_tables(c, INTELLIGENCE_TABLES), down=lambda c: drop_tables(c, reversed(INTELLIGENCE_TABLES))))


def _inbox_updated_at(conn: Connection) -> None:
    """intelligence 2: ``inbox.updated_at`` (the age of a failed row decides when it is retried); a fresh database already has it from version 1."""
    if "updated_at" not in {c["name"] for c in inspect(conn).get_columns("inbox")}:
        add_column(conn, "inbox", "updated_at VARCHAR(32)")


register(Migration("intelligence", 2, "inbox rows record when they were last processed (automatic retry of failed rows)", up=_inbox_updated_at))


def _analysis_context_hash(conn: Connection) -> None:
    """intelligence 3: ``analysis_cache.context_hash``. Rows analysed before it existed have ``NULL`` and are analysed once more by the next scan (safe invalidation);
    a fresh database already has the column from version 1."""
    if "context_hash" not in {c["name"] for c in inspect(conn).get_columns("analysis_cache")}:
        add_column(conn, "analysis_cache", "context_hash VARCHAR(64)")


register(Migration("intelligence", 3, "the analysis cache records the resolution context of an analysis (a changed default lakehouse, schema or mapping re-analyses the definition)",
                   up=_analysis_context_hash))
