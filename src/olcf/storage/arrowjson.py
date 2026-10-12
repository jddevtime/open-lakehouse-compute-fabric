# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Arrow -> JSON helpers used by previews, SQL results and report tiles.

JSON cannot carry every Arrow value: dates/timestamps become ISO strings, 64-bit integers (and whole decimals) outside the
JavaScript-safe range become strings, fractional decimals become numbers when they have at most 15 significant digits (else strings), bytes become hex (truncated), non-finite floats become ``null``.  Column
types are reported with a small, engine-neutral vocabulary so the UI can right-align numbers and format dates.
"""

from __future__ import annotations

import base64
import datetime as dt
import decimal
import math
from typing import Any

import pyarrow as pa

JS_SAFE_INT = 2**53 - 1
MAX_CELL_CHARS = 10_000


def _decimal(v: decimal.Decimal) -> Any:
    """Whole decimals follow the integer rule (number up to 2**53-1, else string); fractions with at most 15 significant digits are exact as
    a JSON number; anything wider stays a string so no precision is lost silently."""
    if not v.is_finite():
        return None
    sign, digits, exp = v.as_tuple()
    if isinstance(exp, int) and exp >= 0:
        i = int(v)
        return i if -JS_SAFE_INT <= i <= JS_SAFE_INT else str(i)
    return float(v) if len(digits) <= 15 else str(v)


def type_kind(t: pa.DataType) -> str:
    """Coarse UI category: integer|float|decimal|boolean|string|date|timestamp|time|binary|list|struct|map|other."""
    if pa.types.is_boolean(t):
        return "boolean"
    if pa.types.is_integer(t):
        return "integer"
    if pa.types.is_floating(t):
        return "float"
    if pa.types.is_decimal(t):
        return "integer" if t.scale == 0 else "decimal"      # e.g. DuckDB HUGEINT (sum of integers) arrives as decimal(38, 0)
    if pa.types.is_string(t) or pa.types.is_large_string(t) or getattr(pa.types, "is_string_view", lambda _: False)(t):
        return "string"
    if pa.types.is_date(t):
        return "date"
    if pa.types.is_timestamp(t):
        return "timestamp"
    if pa.types.is_time(t):
        return "time"
    if pa.types.is_binary(t) or pa.types.is_large_binary(t) or pa.types.is_fixed_size_binary(t):
        return "binary"
    if pa.types.is_list(t) or pa.types.is_large_list(t) or pa.types.is_fixed_size_list(t):
        return "list"
    if pa.types.is_struct(t):
        return "struct"
    if pa.types.is_map(t):
        return "map"
    return "other"


def column_descriptors(schema: pa.Schema) -> list[dict[str, Any]]:
    return [{"name": f.name, "type": str(f.type), "kind": type_kind(f.type), "nullable": f.nullable} for f in schema]


def jsonable(v: Any) -> Any:
    """Convert a Python value produced by ``Table.to_pylist()`` into something ``json.dumps`` accepts."""
    if v is None or isinstance(v, (bool, str)):
        return v if not isinstance(v, str) or len(v) <= MAX_CELL_CHARS else v[:MAX_CELL_CHARS] + "…"
    if isinstance(v, int):
        return v if -JS_SAFE_INT <= v <= JS_SAFE_INT else str(v)
    if isinstance(v, float):
        return v if math.isfinite(v) else None
    if isinstance(v, decimal.Decimal):
        return _decimal(v)
    if isinstance(v, dt.datetime):
        return v.isoformat()
    if isinstance(v, (dt.date, dt.time)):
        return v.isoformat()
    if isinstance(v, dt.timedelta):
        return v.total_seconds()
    if isinstance(v, (bytes, bytearray, memoryview)):
        b = bytes(v)
        return "0x" + b[:64].hex() + ("…" if len(b) > 64 else "")
    if isinstance(v, dict):
        return {str(k): jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        if v and isinstance(v[0], tuple) and len(v[0]) == 2:  # map type -> [(k, v), ...]
            return {str(k): jsonable(x) for k, x in v}
        return [jsonable(x) for x in v]
    return str(v)


def table_rows(table: pa.Table) -> list[list[Any]]:
    """Row-major, JSON-safe rows (list of lists, column order preserved)."""
    cols = [table.column(i).to_pylist() for i in range(table.num_columns)]
    return [[jsonable(c[r]) for c in cols] for r in range(table.num_rows)]


def table_records(table: pa.Table) -> list[dict[str, Any]]:
    names = table.column_names
    return [dict(zip(names, row, strict=True)) for row in table_rows(table)]


def image_data_uri(content: bytes, media_type: str) -> str:
    return f"data:{media_type};base64,{base64.b64encode(content).decode()}"
