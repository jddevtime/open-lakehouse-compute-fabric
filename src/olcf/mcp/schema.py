# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""MCP tables (migration stream ``mcp``): protocol sessions and the audit trail of every call."""

from __future__ import annotations

from sqlalchemy import JSON, BigInteger, Column, Index, String, Table

from olcf.db.migrate import Migration, create_tables, drop_tables, register
from olcf.db.schema import ID, TS, metadata

mcp_sessions = Table(
    "mcp_sessions", metadata,
    Column("id", String(64), primary_key=True),            # unguessable (mcp_<token>); sent as the Mcp-Session-Id header
    Column("principal_id", ID, nullable=False),            # a session is bound to the principal that initialised it
    Column("token_id", String(64)),
    Column("protocol_version", String(16), nullable=False),
    Column("client_name", String(128)),
    Column("client_version", String(64)),
    Column("client_capabilities", JSON, nullable=False, default=dict),
    Column("created_at", TS, nullable=False),
    Column("last_seen_at", TS, nullable=False),
    Column("expires_at", TS, nullable=False),
    Column("closed_at", TS),
    Index("ix_mcp_sessions_principal", "principal_id"),
    Index("ix_mcp_sessions_expires", "expires_at"),
)

mcp_calls = Table(
    "mcp_calls", metadata,
    Column("id", ID, primary_key=True),                    # mcl_<ulid>
    Column("session_id", String(64)),
    Column("principal_id", ID, nullable=False),
    Column("principal_name", String(128)),
    Column("token_id", String(64)),
    Column("client_name", String(128)),
    Column("workspace_id", ID),                            # set when the call touched exactly one workspace
    Column("method", String(48), nullable=False),          # initialize | tools/call | resources/read | prompts/get | ...
    Column("tool", String(64)),                            # tool / prompt name, or resource uri for resources/read
    Column("arguments", JSON),                             # redacted, size-capped
    Column("status", String(12), nullable=False),          # ok | error | denied
    Column("error_code", String(64)),
    Column("error_message", String(1000)),
    Column("elapsed_ms", BigInteger),
    Column("result_bytes", BigInteger),
    Column("truncated", String(5)),                        # "true" when the result was cut to fit the size limit
    Column("ip", String(64)),
    Column("created_at", TS, nullable=False),
    Index("ix_mcp_calls_created", "created_at"),
    Index("ix_mcp_calls_principal_created", "principal_id", "created_at"),
    Index("ix_mcp_calls_tool", "tool"),
    Index("ix_mcp_calls_status", "status"),
)

MCP_TABLES = [mcp_sessions, mcp_calls]

register(Migration("mcp", 1, "initial mcp schema", up=lambda c: create_tables(c, MCP_TABLES), down=lambda c: drop_tables(c, MCP_TABLES)))
