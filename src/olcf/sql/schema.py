# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""SQL service tables (migration stream ``sql``).

``query_history`` records every statement that reached the SQL service - who ran it, on which engine, what it touched
and how it ended.  SQL text is redacted and truncated before it is stored; rows are purged by retention settings.
"""

from __future__ import annotations

from sqlalchemy import JSON, BigInteger, Boolean, Column, Index, Integer, String, Table, Text

from olcf.db.migrate import Migration, create_tables, drop_tables, register
from olcf.db.schema import ID, TS, metadata

query_history = Table(
    "query_history", metadata,
    Column("id", ID, primary_key=True),                       # qry_<ulid> (also the cancel handle while running)
    Column("workspace_id", ID),
    Column("principal_id", ID, nullable=False),
    Column("principal_name", String(128)),
    Column("engine", String(16), nullable=False),
    Column("source", String(24), nullable=False, default="api"),  # api | saved_query | report | mcp | export | notebook
    Column("item_id", ID),                                    # saved query / report item that issued it
    Column("lakehouse_id", ID),
    Column("statement_kind", String(24)),
    Column("sql_text", Text, nullable=False),
    Column("sql_hash", String(64), nullable=False),
    Column("status", String(16), nullable=False),            # ok | error | cancelled | timeout | denied | running
    Column("error_code", String(64)),
    Column("error_message", String(1000)),
    Column("row_count", Integer),
    Column("truncated", Boolean, nullable=False, default=False),
    Column("elapsed_ms", BigInteger),
    Column("tables", JSON, nullable=False, default=list),
    Column("started_at", TS, nullable=False),
    Column("finished_at", TS),
    Index("ix_query_history_ws_started", "workspace_id", "started_at"),
    Index("ix_query_history_principal_started", "principal_id", "started_at"),
    Index("ix_query_history_started", "started_at"),
)

SQL_TABLES = [query_history]

register(Migration("sql", 1, "initial sql schema", up=lambda c: create_tables(c, SQL_TABLES), down=lambda c: drop_tables(c, SQL_TABLES)))
