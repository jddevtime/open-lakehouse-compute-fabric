# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Catalog tables (migration stream ``catalog``).

* ``catalog_entries`` - one row per discovered table (Delta / Iceberg / Parquet) of a lakehouse: columns, partitioning,
  description and tags.  Storage remains the source of truth for *physical* facts; the row is a searchable, annotatable
  projection that survives restarts and keeps human-written descriptions when a table is re-discovered.
* ``lineage_edges`` - a deduplicated edge ``source dataset -> target dataset`` recorded by whoever moved the data (SQL
  writes, notebooks, pipelines, the API).  Column mappings are stored only when they could be *proven*; otherwise the
  edge says ``partial`` or ``unknown`` explicitly rather than pretending.
* ``table_stats`` - the latest row count / size / per-column min-max-null statistics, derived from the Delta log
  (no data scan) or Parquet footers.
"""

from __future__ import annotations

from sqlalchemy import JSON, BigInteger, Column, Index, Integer, String, Table, Text, UniqueConstraint

from olcf.db.migrate import Migration, create_tables, drop_tables, register
from olcf.db.schema import ID, TS, metadata

catalog_entries = Table(
    "catalog_entries", metadata,
    Column("id", ID, primary_key=True),
    Column("workspace_id", ID, nullable=False),
    Column("lakehouse_id", ID, nullable=False),
    Column("catalog", String(64), nullable=False, default="default"),
    Column("schema_name", String(128), nullable=False, default=""),
    Column("name", String(256), nullable=False),            # table name as used in Tables/<name> ("schema.table" when schema-enabled)
    Column("format", String(16), nullable=False),           # delta | iceberg | parquet
    Column("location", String(1024), nullable=False, default=""),
    Column("columns", JSON, nullable=False, default=list),  # [{name, type, nullable, description?}]
    Column("partition_columns", JSON, nullable=False, default=list),
    Column("description", Text, nullable=False, default=""),
    Column("tags", JSON, nullable=False, default=list),
    Column("owner_id", ID),
    Column("status", String(16), nullable=False, default="active"),  # active | missing | unreadable
    Column("status_detail", String(512)),
    Column("version", String(64)),                          # delta version / iceberg snapshot id at last sync
    Column("last_modified", TS),
    Column("discovered_at", TS, nullable=False),
    Column("synced_at", TS, nullable=False),
    Column("etag", String(64), nullable=False),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
    UniqueConstraint("lakehouse_id", "name", name="uq_catalog_entries_lh_name"),
    Index("ix_catalog_entries_ws", "workspace_id"),
    Index("ix_catalog_entries_name", "name"),
)

table_stats = Table(
    "table_stats", metadata,
    Column("entry_id", ID, primary_key=True),
    Column("workspace_id", ID, nullable=False),
    Column("lakehouse_id", ID, nullable=False),
    Column("row_count", BigInteger),
    Column("size_bytes", BigInteger),
    Column("num_files", Integer),
    Column("version", String(64)),
    Column("column_stats", JSON),                           # {col: {min, max, null_count}} - null until computed for this version
    Column("source", String(32)),                           # delta_log | parquet_footers | iceberg_summary
    Column("computed_at", TS, nullable=False),
)

lineage_edges = Table(
    "lineage_edges", metadata,
    Column("id", ID, primary_key=True),
    Column("workspace_id", ID, nullable=False),
    Column("edge_hash", String(64), nullable=False),
    Column("source_key", String(512), nullable=False),
    Column("target_key", String(512), nullable=False),
    Column("source", JSON, nullable=False),                 # dataset descriptor
    Column("target", JSON, nullable=False),
    Column("via_kind", String(32), nullable=False, default="manual"),   # sql | notebook | job | pipeline | report | query | api | manual
    Column("via_name", String(256)),
    Column("via_ref", String(128)),                         # item id / job id / query id that produced the edge
    Column("column_status", String(16), nullable=False, default="unknown"),  # complete | partial | unknown | not_applicable
    Column("column_mappings", JSON, nullable=False, default=list),  # [{target, sources:[{column}], transform}]
    Column("notes", JSON, nullable=False, default=list),
    Column("run_count", Integer, nullable=False, default=1),
    Column("created_by", ID),
    Column("first_seen_at", TS, nullable=False),
    Column("last_seen_at", TS, nullable=False),
    UniqueConstraint("edge_hash", name="uq_lineage_edges_hash"),
    Index("ix_lineage_edges_target", "target_key"),
    Index("ix_lineage_edges_source", "source_key"),
    Index("ix_lineage_edges_ws", "workspace_id"),
)

CATALOG_TABLES = [catalog_entries, table_stats, lineage_edges]

register(Migration("catalog", 1, "initial catalog schema", up=lambda c: create_tables(c, CATALOG_TABLES),
                   down=lambda c: drop_tables(c, reversed(CATALOG_TABLES))))
