# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Small query helpers shared by repositories."""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any

from sqlalchemy import Connection, Select, Table, delete, insert, select, update


def row(r: Any) -> dict[str, Any] | None:
    return None if r is None else dict(r._mapping)


def rows(rs: Any) -> list[dict[str, Any]]:
    return [dict(r._mapping) for r in rs]


def fetch_one(conn: Connection, stmt: Select[Any]) -> dict[str, Any] | None:
    return row(conn.execute(stmt).first())


def fetch_all(conn: Connection, stmt: Select[Any]) -> list[dict[str, Any]]:
    return rows(conn.execute(stmt))


def get_by_id(conn: Connection, table: Table, id_: str, *, include_deleted: bool = False) -> dict[str, Any] | None:
    stmt = select(table).where(table.c.id == id_)
    if not include_deleted and "deleted_at" in table.c:
        stmt = stmt.where(table.c.deleted_at.is_(None))
    return fetch_one(conn, stmt)


def insert_row(conn: Connection, table: Table, values: Mapping[str, Any]) -> None:
    conn.execute(insert(table).values(**values))


def update_row(conn: Connection, table: Table, id_: str, values: Mapping[str, Any], *, key: str = "id") -> int:
    return conn.execute(update(table).where(table.c[key] == id_).values(**values)).rowcount


def delete_row(conn: Connection, table: Table, id_: str, *, key: str = "id") -> int:
    return conn.execute(delete(table).where(table.c[key] == id_)).rowcount


def new_etag(payload: Any = None) -> str:
    import os

    h = hashlib.sha256(os.urandom(16))
    if payload is not None:
        h.update(json.dumps(payload, sort_keys=True, default=str).encode())
    return h.hexdigest()[:24]


# ----------------------------------------------------------------------------- keyset pagination
def encode_cursor(value: Sequence[Any]) -> str:
    return base64.urlsafe_b64encode(json.dumps(list(value)).encode()).decode().rstrip("=")


def decode_cursor(cursor: str | None) -> list[Any] | None:
    if not cursor:
        return None
    pad = "=" * (-len(cursor) % 4)
    try:
        return json.loads(base64.urlsafe_b64decode(cursor + pad))
    except Exception as exc:  # noqa: BLE001
        from olcf.core.errors import BadRequest

        raise BadRequest("invalid cursor") from exc


def paginate(
    conn: Connection,
    stmt: Select[Any],
    table: Table,
    *,
    limit: int = 50,
    cursor: str | None = None,
    descending: bool = False,
    key: str = "id",
) -> tuple[list[dict[str, Any]], str | None]:
    """Keyset-paginate ``stmt`` on ``table.<key>`` (ULIDs sort by creation time)."""
    limit = max(1, min(int(limit), 500))
    col = table.c[key]
    cur = decode_cursor(cursor)
    if cur:
        stmt = stmt.where(col < cur[0] if descending else col > cur[0])
    stmt = stmt.order_by(col.desc() if descending else col.asc()).limit(limit + 1)
    data = fetch_all(conn, stmt)
    next_cursor = None
    if len(data) > limit:
        data = data[:limit]
        next_cursor = encode_cursor([data[-1][key]])
    return data, next_cursor
