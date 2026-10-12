# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Query history: recording, listing, retention."""

from __future__ import annotations

import hashlib
import logging
import threading
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, func, insert, select, update

from olcf.core.errors import NotFound
from olcf.core.redaction import redact_text
from olcf.core.timeutil import now_iso
from olcf.db.util import fetch_all, fetch_one, paginate
from olcf.sql.schema import query_history

if TYPE_CHECKING:
    from olcf.core.context import AppContext
    from olcf.security.rbac import Principal

log = logging.getLogger("olcf.sql")
MAX_STORED_SQL = 20_000


class HistoryStore:
    """Persists one row per statement and enforces retention (age and a per-deployment row cap)."""

    def __init__(self, ctx: AppContext) -> None:
        self.ctx = ctx
        self._inserts = 0
        self._lock = threading.Lock()

    def _setting(self, key: str, default: int) -> int:
        try:
            v = self.ctx.service("settings").get(key)
            return int(v) if v is not None else default
        except Exception:  # noqa: BLE001
            return default

    def start(self, query_id: str, principal: Principal, *, engine: str, source: str, sql: str, workspace_id: str | None, lakehouse_id: str | None,
              item_id: str | None, kind: str | None) -> None:
        text = redact_text(sql)
        if len(text) > MAX_STORED_SQL:
            text = text[:MAX_STORED_SQL] + "\n-- (truncated)"
        with self.ctx.db.begin() as conn:
            conn.execute(insert(query_history).values(
                id=query_id, workspace_id=workspace_id, principal_id=principal.id, principal_name=principal.name, engine=engine, source=source, item_id=item_id,
                lakehouse_id=lakehouse_id, statement_kind=kind, sql_text=text, sql_hash=hashlib.sha256(sql.encode()).hexdigest(), status="running",
                truncated=False, tables=[], started_at=now_iso()))
        with self._lock:
            self._inserts += 1
            purge = self._inserts % 500 == 0
        if purge:
            try:
                self.purge()
            except Exception:  # noqa: BLE001
                log.exception("history purge failed")

    def finish(self, query_id: str, *, status: str, rows: int | None = None, truncated: bool = False, elapsed_ms: int | None = None,
               error_code: str | None = None, error_message: str | None = None, tables: list[str] | None = None, workspace_id: str | None = None,
               lakehouse_id: str | None = None, kind: str | None = None) -> None:
        values: dict[str, Any] = {"status": status, "row_count": rows, "truncated": truncated, "elapsed_ms": elapsed_ms, "finished_at": now_iso(),
                                  "error_code": error_code, "error_message": redact_text(error_message)[:1000] if error_message else None, "tables": tables or []}
        if workspace_id:
            values["workspace_id"] = workspace_id
        if lakehouse_id:
            values["lakehouse_id"] = lakehouse_id
        if kind:
            values["statement_kind"] = kind
        with self.ctx.db.begin() as conn:
            conn.execute(update(query_history).where(query_history.c.id == query_id).values(**values))

    def can_see_all(self, principal: Principal, workspace_id: str | None) -> bool:
        return principal.can("audit.read") or (workspace_id is not None and principal.can("workspace.update", workspace_id=workspace_id))

    def get(self, principal: Principal, query_id: str) -> dict[str, Any]:
        with self.ctx.db.connect() as conn:
            row = fetch_one(conn, select(query_history).where(query_history.c.id == query_id))
        if row is None or (row["principal_id"] != principal.id and not self.can_see_all(principal, row["workspace_id"])):
            raise NotFound(f"query '{query_id}' not found")  # same answer for "missing" and "not yours": no existence oracle
        return row

    def list(self, principal: Principal, *, workspace_id: str | None = None, mine: bool = True, status: str | None = None, source: str | None = None,
             limit: int = 50, cursor: str | None = None) -> tuple[list[dict[str, Any]], str | None]:
        stmt = select(query_history)
        if workspace_id:
            stmt = stmt.where(query_history.c.workspace_id == workspace_id)
        if mine or not self.can_see_all(principal, workspace_id):
            stmt = stmt.where(query_history.c.principal_id == principal.id)
        if status:
            stmt = stmt.where(query_history.c.status == status)
        if source:
            stmt = stmt.where(query_history.c.source == source)
        with self.ctx.db.connect() as conn:
            return paginate(conn, stmt, query_history, limit=limit, cursor=cursor, descending=True)

    def purge(self) -> dict[str, int]:
        """Delete rows older than ``sql.history_retention_days`` and trim to ``sql.history_max_rows`` (oldest first)."""
        days = self._setting("sql.history_retention_days", 30)
        cap = self._setting("sql.history_max_rows", 100_000)
        cutoff = (datetime.now(UTC) - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        with self.ctx.db.begin() as conn:
            old = conn.execute(delete(query_history).where(query_history.c.started_at < cutoff, query_history.c.status != "running")).rowcount or 0
            total = conn.execute(select(func.count()).select_from(query_history)).scalar() or 0
            trimmed = 0
            if total > cap:
                ids = [r[0] for r in conn.execute(select(query_history.c.id).where(query_history.c.status != "running").order_by(query_history.c.id.asc()).limit(total - cap))]
                if ids:
                    trimmed = conn.execute(delete(query_history).where(query_history.c.id.in_(ids))).rowcount or 0
        return {"expired": int(old), "trimmed": int(trimmed)}

    def fail_orphans(self) -> int:
        """Rows still ``running`` after a restart can never finish; mark them so they do not look alive forever."""
        with self.ctx.db.begin() as conn:
            n = conn.execute(update(query_history).where(query_history.c.status == "running").values(
                status="error", error_code="interrupted", error_message="the control plane restarted while this statement was running", finished_at=now_iso())).rowcount
        return int(n or 0)

    def stats(self, principal: Principal, workspace_id: str | None) -> list[dict[str, Any]]:
        stmt = select(query_history.c.engine, query_history.c.status, func.count().label("n")).group_by(query_history.c.engine, query_history.c.status)
        if workspace_id:
            stmt = stmt.where(query_history.c.workspace_id == workspace_id)
        if not self.can_see_all(principal, workspace_id):
            stmt = stmt.where(query_history.c.principal_id == principal.id)
        with self.ctx.db.connect() as conn:
            return fetch_all(conn, stmt)
