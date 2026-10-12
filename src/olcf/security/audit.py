# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Append-oriented, tamper-evident audit log (spec section 29).

Each record stores ``hash = SHA-256(prev_hash || canonical(record))`` forming a chain. The chain can be
verified with :meth:`AuditLog.verify` and records can be exported as JSONL or CEF for a SIEM.
"""

from __future__ import annotations

import hashlib
import json
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from sqlalchemy import Connection, select, text
from sqlalchemy.pool import StaticPool

from olcf.core.ids import new_id
from olcf.core.redaction import redact_obj
from olcf.core.timeutil import now_iso
from olcf.db import schema as S
from olcf.db.engine import Database
from olcf.db.util import fetch_all, fetch_one
from olcf.security.rbac import Principal

#: Canonical action names (non-exhaustive; modules may add ``<domain>.<verb>`` actions).
ACTIONS = [
    "auth.login", "auth.login_failed", "auth.logout", "auth.token_created", "auth.token_revoked",
    "identity.user_created", "identity.user_updated", "identity.binding_changed", "identity.password_changed",
    "secret.created", "secret.rotated", "secret.deleted", "config.changed",
    "workspace.created", "workspace.updated", "workspace.deleted", "workspace.restored", "workspace.exported", "workspace.imported",
    "item.created", "item.updated", "item.deleted", "item.restored", "connection.created", "connection.updated", "connection.deleted",
    "runtime.installed", "runtime.retired", "runtime.verified", "worker.enrolled", "worker.revoked", "worker.drained",
    "job.submitted", "job.cancelled", "data.destructive", "fabric.submission", "backup.created", "backup.restored", "update.applied",
    "intelligence.source_created", "intelligence.source_updated", "intelligence.source_deleted", "intelligence.source_tested",
    "intelligence.scan_started", "intelligence.scan_finished", "intelligence.asset_updated", "intelligence.asset_merged", "intelligence.sample_read", "intelligence.inbox_requeued",
    "governance.term_created", "governance.term_updated", "governance.term_deleted", "governance.term_linked", "governance.term_unlinked",
    "governance.domain_created", "governance.domain_updated", "governance.domain_deleted", "governance.domain_assigned",
    "governance.product_created", "governance.product_updated", "governance.product_deleted", "governance.product_members_changed",
    "governance.owner_added", "governance.owner_removed",
    "governance.classification_created", "governance.classification_updated", "governance.classification_deleted", "governance.classification_assigned",
    "governance.classification_removed", "governance.classification_reviewed", "governance.classification_scanned",
    "governance.certification_changed", "governance.certification_invalidated",
    "governance.change_requested", "governance.change_commented", "governance.change_decided", "governance.change_withdrawn", "governance.change_applied",
    "governance.policy_created", "governance.policy_updated", "governance.policy_deleted", "governance.policy_evaluated", "governance.violation_updated",
]


def _canonical(rec: dict[str, Any]) -> str:
    return json.dumps(rec, sort_keys=True, separators=(",", ":"), default=str)


class AuditLog:
    """Append-only audit log. When the hash chain is on, each record carries a hash that covers the previous record, so removal or alteration can be detected.
    """
    def __init__(self, db: Database, *, hash_chain: bool = True) -> None:
        self.db = db
        self.hash_chain = hash_chain
        self._lock = threading.Lock()

    def record(
        self,
        actor: Principal | None,
        action: str,
        *,
        resource_type: str | None = None,
        resource_id: str | None = None,
        workspace_id: str | None = None,
        outcome: str = "success",
        details: dict[str, Any] | None = None,
        ip: str | None = None,
        correlation_id: str | None = None,
        conn: Connection | None = None,
    ) -> dict[str, Any]:
        """Append one record. With ``conn`` (a connection inside a transaction that already holds the write lock, such as ``Database.begin_serialized``) the record
        is written *in that transaction*: it commits or rolls back with the caller's change and no second writer is opened (which would wait for the caller's own
        lock). The chain stays consistent because every other appender takes the database write lock before it reads the chain head."""
        rec = {
            "id": new_id("aud"), "ts": now_iso(),
            "actor_id": actor.id if actor else None, "actor_name": actor.name if actor else None,
            "actor_kind": actor.kind if actor else None,
            "action": action, "resource_type": resource_type, "resource_id": resource_id, "workspace_id": workspace_id,
            "outcome": outcome, "ip": ip or (actor.ip if actor else None),
            "correlation_id": correlation_id or (actor.correlation_id if actor else None),
            "details": redact_obj(details or {}),
        }
        if conn is not None:
            self._append(conn, rec)
        else:
            with self._lock, self._transaction() as own:
                self._append(own, rec)
        return rec

    @contextmanager
    def _transaction(self) -> Iterator[Connection]:
        """The appender's own transaction. File SQLite takes the write lock *before* the chain head is read (a deferred transaction reads a snapshot and then fails
        with ``SQLITE_BUSY_SNAPSHOT`` when another writer, including a connection-aware append, committed in between)."""
        if self.db.is_sqlite and not isinstance(self.db.engine.pool, StaticPool):
            with self.db.begin_serialized("audit.chain") as conn:
                yield conn
        else:
            with self.db.begin() as conn:
                yield conn

    def _append(self, conn: Connection, rec: dict[str, Any]) -> None:
        if self.hash_chain:
            if self.db.is_postgres:
                conn.execute(text("SELECT pg_advisory_xact_lock(727275)"))
            last = fetch_one(conn, select(S.audit_log.c.hash).order_by(S.audit_log.c.seq.desc()).limit(1))
            prev = (last or {}).get("hash") or ""
            rec["prev_hash"] = prev
            rec["hash"] = hashlib.sha256((prev + _canonical({k: v for k, v in rec.items() if k not in ("prev_hash", "hash")})).encode()).hexdigest()
        else:
            rec["prev_hash"] = rec["hash"] = None
        conn.execute(S.audit_log.insert().values(**rec))

    # ------------------------------------------------------------------ query / export
    def query(self, *, limit: int = 100, before_seq: int | None = None, action: str | None = None, actor: str | None = None,
              workspace_id: str | None = None, resource_id: str | None = None, since: str | None = None, until: str | None = None,
              outcome: str | None = None) -> list[dict[str, Any]]:
        stmt = select(S.audit_log).order_by(S.audit_log.c.seq.desc()).limit(max(1, min(limit, 5000)))
        t = S.audit_log.c
        if before_seq:
            stmt = stmt.where(t.seq < before_seq)
        if action:
            stmt = stmt.where(t.action.like(action.replace("*", "%")))
        if actor:
            stmt = stmt.where((t.actor_name == actor) | (t.actor_id == actor))
        if workspace_id:
            stmt = stmt.where(t.workspace_id == workspace_id)
        if resource_id:
            stmt = stmt.where(t.resource_id == resource_id)
        if since:
            stmt = stmt.where(t.ts >= since)
        if until:
            stmt = stmt.where(t.ts <= until)
        if outcome:
            stmt = stmt.where(t.outcome == outcome)
        with self.db.connect() as conn:
            return fetch_all(conn, stmt)

    def iter_all(self, batch: int = 1000) -> Iterator[dict[str, Any]]:
        last = 0
        while True:
            with self.db.connect() as conn:
                rows = fetch_all(conn, select(S.audit_log).where(S.audit_log.c.seq > last).order_by(S.audit_log.c.seq).limit(batch))
            if not rows:
                return
            yield from rows
            last = rows[-1]["seq"]

    def verify(self) -> dict[str, Any]:
        """Walk the chain and report the first inconsistency, if any."""
        prev = ""
        count = 0
        first = None
        for r in self.iter_all():
            count += 1
            if first is None:
                first = r["seq"]
                prev = r["prev_hash"] or ""  # chain may start after retention purge
            if not r["hash"]:
                prev = ""
                continue
            body = {k: v for k, v in r.items() if k not in ("seq", "prev_hash", "hash")}
            body["details"] = r["details"]
            expected = hashlib.sha256((prev + _canonical(body)).encode()).hexdigest()
            if r["prev_hash"] != prev or r["hash"] != expected:
                return {"ok": False, "checked": count, "broken_at_seq": r["seq"], "broken_id": r["id"]}
            prev = r["hash"]
        return {"ok": True, "checked": count, "head": prev}

    @staticmethod
    def to_cef(r: dict[str, Any]) -> str:
        def esc(v: Any) -> str:
            return str(v if v is not None else "").replace("\\", "\\\\").replace("|", "\\|").replace("=", "\\=").replace("\r", "\\r").replace("\n", "\\n")

        sev = 3 if r["outcome"] == "success" else 6
        ext = f"rt={esc(r['ts'])} suser={esc(r['actor_name'])} act={esc(r['action'])} outcome={esc(r['outcome'])} " \
              f"cs1Label=resource cs1={esc(r['resource_type'])}:{esc(r['resource_id'])} cs2Label=workspace cs2={esc(r['workspace_id'])} src={esc(r['ip'])}"
        return f"CEF:0|OpenLakehouse|OLCF|0.1|{esc(r['action'])}|{esc(r['action'])}|{sev}|{ext}"
