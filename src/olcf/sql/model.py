# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Request / result objects of the SQL service (plain dataclasses, no behaviour that needs the database)."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

ENGINES = ("duckdb", "spark")
QUERY_ID_RE = re.compile(r"^[A-Za-z0-9_-]{8,64}$")
PARAM_TYPES = ("string", "int", "number", "boolean", "date", "timestamp")


@dataclass
class SqlRequest:
    """One statement to run.

    ``workspace_id`` / ``lakehouse`` give the context for unqualified table names: ``lakehouse`` (id or name) is the default
    lakehouse and, when ``workspace_id`` is omitted, also determines it.  ``parameters`` are bound by name (``:name`` or
    ``$name`` in the SQL) as typed literals, never by string concatenation.
    """

    sql: str
    engine: str = "duckdb"
    workspace_id: str | None = None
    lakehouse: str | None = None
    parameters: dict[str, Any] = field(default_factory=dict)
    param_specs: list[dict[str, Any]] | None = None     # declared types/defaults (saved queries, reports)
    max_rows: int | None = None
    timeout_seconds: int | None = None
    query_id: str | None = None
    source: str = "api"
    item_id: str | None = None
    #: Caller-supplied tag such as "tile:revenue" (informational; not persisted and with no effect on authorisation or execution).
    label: str | None = None


@dataclass
class SqlResult:
    """Typed, size-capped result of a statement."""

    query_id: str
    engine: str
    statement: str
    columns: list[dict[str, Any]]
    rows: list[list[Any]]
    truncated: bool
    stats: dict[str, Any]
    warnings: list[str] = field(default_factory=list)

    @property
    def row_count(self) -> int:
        return len(self.rows)

    def to_dict(self) -> dict[str, Any]:
        return {"query_id": self.query_id, "engine": self.engine, "statement": self.statement, "columns": self.columns, "rows": self.rows,
                "row_count": len(self.rows), "truncated": self.truncated, "stats": self.stats, "warnings": self.warnings}
