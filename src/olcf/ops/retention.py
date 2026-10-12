# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Retention and housekeeping (service ``retention``; runs on the **leader only**, or on demand through ``/v1/admin/retention``).

What is purged, by which setting (runtime overrides live in the settings store as ``retention.<name>``; the defaults come from the static
``settings.retention.*`` section of the configuration file):

=====================  =====================================================================  ====================
step                   data                                                                   setting (days)
=====================  =====================================================================  ====================
``events``             ``events`` rows (the platform event bus history)                       ``event_days``
``audit``              ``audit_log`` rows - archived to a gzip JSONL file and hash-anchored   ``audit_days`` (>= 30)
``metrics``            ``metric_samples``                                                     ``metric_days``
``jobs``               terminal jobs + events/attempts/Spark apps/files, via the compute svc   ``job_history_days``
``job_logs``           job log directories, via the compute service (never active jobs)       ``log_days``
``recycle_bin``        soft-deleted items (``ItemService.purge_deleted``), then soft-deleted  ``recycle_bin_days``
                       workspaces *that nothing else references any more*
``tokens``             expired / long-revoked ``api_tokens``; ``revoked_tokens`` whose access      ``token_grace_days``
                       token has expired (rows of logout / refresh-reuse revocations)
``idempotency``        expired ``idempotency_keys``                                           (expiry column)
``nonces``             expired ``fabric_nonces``, ``oidc_auth_requests``, ``oidc_login_codes``  (expiry column)
``operations``         running operations nobody updated for ``stale_operation_hours`` are    ``operations_days``
                       marked failed ("abandoned"); finished ones are deleted
``support_bundles``    generated support bundles                                              ``support_bundle_days``
``retention_runs``     this report table                                                      ``run_history_days``
=====================  =====================================================================  ====================

Audit and the hash chain: every audit record carries ``hash = SHA-256(prev_hash || canonical(record))``. Purging the oldest records would make
the chain start in the middle, which :meth:`olcf.security.audit.AuditLog.verify` has to trust (a truncation by an attacker would be
indistinguishable from retention). Therefore every purge (a) writes the removed records to an archive file (``<data>/archive/audit``) with its
SHA-256, (b) records an **anchor** ``(last_purged_seq, last_purged_hash)`` in ``audit_anchors`` in the same transaction as the delete, and (c)
always keeps the newest record. :meth:`RetentionService.verify_audit` verifies the remaining chain *and* that its first record links to an
anchor, so a prefix removed without an anchor is detected. Operators who need tamper-evidence against a database administrator should
periodically copy the ``head`` hash from ``GET /v1/admin/retention/audit`` to a system the database administrator cannot write to.

Every run - dry-run or real - leaves a report in ``retention_runs``. A dry run changes nothing (not even stale operations).
"""

from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import Table, delete, func, inspect, select, update

from olcf.core.context import AppContext
from olcf.core.errors import BadRequest, Conflict
from olcf.core.ids import new_id
from olcf.core.timeutil import from_iso, iso_in, now_iso
from olcf.db import schema as S
from olcf.db.engine import Database
from olcf.db.util import fetch_all, fetch_one
from olcf.ops import schema as O
from olcf.security.rbac import Principal

log = logging.getLogger("olcf.ops.retention")
BATCH = 5000
MIN_AUDIT_DAYS = 30

#: name -> (description, lo, hi, config attribute providing the default | literal default)
SETTING_DEFS: dict[str, tuple[str, int, int, str | int]] = {
    "enabled": ("Run the scheduled retention loop (1) or only on demand (0)", 0, 1, 1),
    "interval_hours": ("Hours between scheduled retention runs", 1, 720, 24),
    "event_days": ("Days of platform events kept", 1, 3650, "event_days"),
    "audit_days": (f"Days of audit records kept in the database (minimum {MIN_AUDIT_DAYS})", MIN_AUDIT_DAYS, 36500, "audit_days"),
    "audit_archive": ("Write purged audit records to a gzip JSONL archive before deleting (1) - disabling loses them", 0, 1, 1),
    "metric_days": ("Days of metric samples kept", 1, 3650, "metric_days"),
    "job_history_days": ("Days of finished job history kept (via the compute service)", 1, 3650, "job_history_days"),
    "log_days": ("Days of job log files kept (via the compute service)", 1, 3650, "log_days"),
    "recycle_bin_days": ("Days a deleted item/workspace stays restorable", 1, 3650, 30),
    "token_grace_days": ("Days an expired or revoked token row is kept (for clear error messages and forensics)", 1, 3650, 7),
    "operations_days": ("Days finished operations are kept", 1, 3650, 30),
    "stale_operation_hours": ("Running operations without progress for this long are marked failed (abandoned)", 1, 8760, 24),
    "support_bundle_days": ("Days generated support bundles are kept", 1, 3650, 14),
    "run_history_days": ("Days retention run reports are kept", 1, 3650, 90),
}


def _bounded(lo: int, hi: int):  # noqa: ANN202
    def check(v: Any) -> int:
        if not isinstance(v, int) or isinstance(v, bool) or not lo <= v <= hi:
            raise BadRequest(f"must be an integer between {lo} and {hi}")
        return v

    return check


STEP_ORDER = ["events", "audit", "metrics", "jobs", "job_logs", "recycle_bin", "tokens", "idempotency", "nonces", "operations", "support_bundles", "retention_runs"]
#: tables with a ``workspace_id`` column that must not block purging a deleted workspace (history only, no live references)
HISTORY_TABLES = {"audit_log", "events"}


def verify_audit_chain(db: Database) -> dict[str, Any]:
    """Chain verification that understands retention anchors (also used on restored databases by the backup restore-test)."""
    from olcf.security.audit import AuditLog

    base = AuditLog(db).verify()
    with db.connect() as conn:
        anchors = fetch_all(conn, select(O.audit_anchors).order_by(O.audit_anchors.c.last_purged_seq))
        first = fetch_one(conn, select(S.audit_log.c.seq, S.audit_log.c.prev_hash, S.audit_log.c.hash).order_by(S.audit_log.c.seq).limit(1))
    out = {**base, "anchors": len(anchors), "purged_records": sum(a["purged_count"] for a in anchors)}
    if first is None:
        return out
    out["first_seq"] = first["seq"]
    if not first["hash"]:
        return {**out, "note": "hash chain disabled for the oldest record"}
    prior = [a for a in anchors if a["last_purged_seq"] < first["seq"]]
    if first["prev_hash"]:
        link = prior[-1] if prior else None
        if link is None or link["last_purged_hash"] != first["prev_hash"]:
            return {**out, "ok": False, "reason": f"the chain starts at seq {first['seq']} whose predecessor is not covered by any retention anchor: "
                                                  "records before it were removed outside of retention"}
        out["anchored_to"] = {"seq": link["last_purged_seq"], "archive": link["archive"], "archive_sha256": link["archive_sha256"]}
    elif first["seq"] > 1 and not prior:
        out["note"] = "the chain starts at a genesis record but earlier sequence numbers are absent (chain was restarted or records removed)"
    return out


class RetentionService:
    """Applies the retention periods to events, audit records, metrics, jobs and logs, optionally archiving audit records first."""
    def __init__(self, ctx: AppContext) -> None:
        self.ctx = ctx
        self._lock = threading.Lock()
        self.archive_dir = ctx.settings.path("archive", "audit")

    # ------------------------------------------------------------------ settings
    def register_settings(self) -> None:
        reg = self.ctx.service("settings")
        for name, (desc, lo, hi, default) in SETTING_DEFS.items():
            dv = getattr(self.ctx.settings.retention, default) if isinstance(default, str) else default
            reg.register(f"retention.{name}", description=desc, default=dv, validator=_bounded(lo, hi))

    def setting(self, name: str) -> int:
        v = self.ctx.service("settings").get(f"retention.{name}")
        if not isinstance(v, int) or isinstance(v, bool):
            desc, lo, hi, default = SETTING_DEFS[name]
            return getattr(self.ctx.settings.retention, default) if isinstance(default, str) else default
        return v

    def effective(self) -> dict[str, int]:
        return {n: self.setting(n) for n in SETTING_DEFS}

    # ------------------------------------------------------------------ helpers
    def _tables(self) -> set[str]:
        return set(inspect(self.ctx.db.engine).get_table_names())

    def _table(self, name: str) -> Table | None:
        """A table of the shared metadata if its module is loaded *and* migrated in this database."""
        t = S.metadata.tables.get(name)
        return t if t is not None and name in self._present else None

    def _count(self, conn: Any, table: Table, *where: Any) -> int:
        return int(conn.execute(select(func.count()).select_from(table).where(*where)).scalar() or 0)

    def _purge(self, table: Table, where: Any, pk: str | None, dry: bool) -> tuple[int, int]:
        """Delete rows matching ``where`` in batches (by single-column primary key when there is one). Returns (candidates, deleted)."""
        with self.ctx.db.connect() as conn:
            cand = self._count(conn, table, where)
        if dry or not cand:
            return cand, 0
        deleted = 0
        if pk is None:
            with self.ctx.db.begin() as conn:
                deleted = conn.execute(delete(table).where(where)).rowcount or 0
            return cand, deleted
        col = table.c[pk]
        while True:
            with self.ctx.db.begin() as conn:
                ids = [r[0] for r in conn.execute(select(col).where(where).order_by(col).limit(BATCH)).all()]
                if not ids:
                    break
                deleted += conn.execute(delete(table).where(col.in_(ids))).rowcount or len(ids)
        return cand, deleted

    # ------------------------------------------------------------------ steps
    def _step_events(self, dry: bool) -> dict[str, Any]:
        cutoff = iso_in(-self.setting("event_days") * 86400)
        c, d = self._purge(S.events, S.events.c.ts < cutoff, "seq", dry)
        return {"cutoff": cutoff, "candidates": c, "deleted": d}

    def _step_metrics(self, dry: bool) -> dict[str, Any]:
        t = self._table("metric_samples")
        if t is None:
            return {"skipped": "observability module not installed"}
        cutoff = iso_in(-self.setting("metric_days") * 86400)
        c, d = self._purge(t, t.c.ts < cutoff, "id", dry)
        return {"cutoff": cutoff, "candidates": c, "deleted": d}

    def _step_tokens(self, dry: bool) -> dict[str, Any]:
        t = S.api_tokens
        cutoff = iso_in(-self.setting("token_grace_days") * 86400)
        c, d = self._purge(t, ((t.c.expires_at.is_not(None)) & (t.c.expires_at < cutoff)) | ((t.c.revoked_at.is_not(None)) & (t.c.revoked_at < cutoff)), "id", dry)
        # revoked access-token ids (logout / refresh-token reuse) are useless once the access token they block has expired
        now = now_iso()
        rc, rd = self._purge(S.revoked_tokens, S.revoked_tokens.c.expires_at < now, "jti", dry)
        return {"cutoff": cutoff, "candidates": c + rc, "deleted": d + rd, "revoked_access_tokens": {"cutoff": now, "candidates": rc, "deleted": rd}}

    def _step_idempotency(self, dry: bool) -> dict[str, Any]:
        now = now_iso()
        c, d = self._purge(S.idempotency_keys, S.idempotency_keys.c.expires_at < now, None, dry)
        return {"cutoff": now, "candidates": c, "deleted": d}

    def _step_nonces(self, dry: bool) -> dict[str, Any]:
        out: dict[str, Any] = {}
        # replay protection of capsule nonces must outlive the nonce's own expiry by a day; OIDC state/codes are single-use and short-lived
        for name, grace in (("fabric_nonces", 86400), ("oidc_auth_requests", 3600), ("oidc_login_codes", 3600)):
            t = self._table(name)
            if t is None:
                out[name] = {"skipped": "module not installed"}
                continue
            cutoff = iso_in(-grace)
            pk = list(t.primary_key.columns)
            c, d = self._purge(t, t.c.expires_at < cutoff, pk[0].name if len(pk) == 1 else None, dry)
            out[name] = {"cutoff": cutoff, "candidates": c, "deleted": d}
        return {"tables": out, "candidates": sum(v.get("candidates", 0) for v in out.values()), "deleted": sum(v.get("deleted", 0) for v in out.values())}

    def _step_operations(self, dry: bool) -> dict[str, Any]:
        t = S.operations
        stale_cut = iso_in(-self.setting("stale_operation_hours") * 3600)
        done_cut = iso_in(-self.setting("operations_days") * 86400)
        with self.ctx.db.connect() as conn:
            stale = [r["id"] for r in fetch_all(conn, select(t.c.id).where(t.c.state.in_(("pending", "running")), t.c.updated_at < stale_cut))]
        marked = 0
        if stale and not dry:
            err = {"code": "abandoned", "message": "no progress for too long: the node that ran this operation probably restarted or died"}
            with self.ctx.db.begin() as conn:
                marked = conn.execute(update(t).where(t.c.id.in_(stale), t.c.state.in_(("pending", "running"))).values(state="failed", error=err, message="abandoned", updated_at=now_iso())).rowcount or 0
        c, d = self._purge(t, t.c.state.in_(("succeeded", "failed", "cancelled")) & (t.c.updated_at < done_cut), "id", dry)
        return {"stale_cutoff": stale_cut, "stale_candidates": len(stale), "marked_failed": marked, "cutoff": done_cut, "candidates": c, "deleted": d}

    def _step_support_bundles(self, dry: bool) -> dict[str, Any]:
        sup = self.ctx.maybe_service("support")
        if sup is None:
            return {"skipped": "support bundle service not installed"}
        cutoff = iso_in(-self.setting("support_bundle_days") * 86400)
        with self.ctx.db.connect() as conn:
            ids = [r["id"] for r in fetch_all(conn, select(O.support_bundles.c.id).where(O.support_bundles.c.created_at < cutoff))]
        deleted = 0
        if not dry:
            for bid in ids:
                try:
                    sup.delete(None, bid)
                    deleted += 1
                except Exception:  # noqa: BLE001
                    log.exception("could not delete support bundle %s", bid)
        return {"cutoff": cutoff, "candidates": len(ids), "deleted": deleted}

    def _step_retention_runs(self, dry: bool) -> dict[str, Any]:
        cutoff = iso_in(-self.setting("run_history_days") * 86400)
        c, d = self._purge(O.retention_runs, O.retention_runs.c.started_at < cutoff, "id", dry)
        return {"cutoff": cutoff, "candidates": c, "deleted": d}

    def _step_jobs(self, dry: bool) -> dict[str, Any]:
        compute = self.ctx.maybe_service("compute")
        if compute is None:
            return {"skipped": "compute module not installed"}
        days = self.setting("job_history_days")
        cutoff = iso_in(-days * 86400)
        from olcf.compute import schema as C
        from olcf.compute.jobs import TERMINAL

        with self.ctx.db.connect() as conn:
            cand = self._count(conn, C.jobs, C.jobs.c.state.in_(TERMINAL), C.jobs.c.finished_at < cutoff)
        deleted = 0 if dry else int(compute.jobs.purge_retention(days=days) or 0)
        return {"cutoff": cutoff, "candidates": cand, "deleted": deleted}

    def _step_job_logs(self, dry: bool) -> dict[str, Any]:
        compute = self.ctx.maybe_service("compute")
        if compute is None:
            return {"skipped": "compute module not installed"}
        from olcf.compute import schema as C
        from olcf.compute.jobs import TERMINAL

        days = self.setting("log_days")
        epoch = time.time() - days * 86400
        with self.ctx.db.connect() as conn:
            active = {r[0] for r in conn.execute(select(C.jobs.c.id).where(C.jobs.c.state.not_in(TERMINAL))).all()}
        root = Path(compute.logs.root)
        candidates = 0
        if root.is_dir():
            for d in root.iterdir():
                try:
                    if d.is_dir() and d.name not in active and d.stat().st_mtime < epoch:
                        candidates += 1
                except OSError:
                    continue
        deleted = 0 if dry else int(compute.logs.purge_older_than(epoch, protect=active) or 0)
        return {"cutoff_epoch": int(epoch), "candidates": candidates, "deleted": deleted, "protected_active_jobs": len(active)}

    def _step_recycle_bin(self, dry: bool) -> dict[str, Any]:
        days = self.setting("recycle_bin_days")
        cutoff = iso_in(-days * 86400)
        items = self.ctx.service("items")
        with self.ctx.db.connect() as conn:
            item_c = self._count(conn, S.items, S.items.c.deleted_at.is_not(None), S.items.c.deleted_at < cutoff)
            ws_rows = fetch_all(conn, select(S.workspaces.c.id, S.workspaces.c.name).where(S.workspaces.c.deleted_at.is_not(None), S.workspaces.c.deleted_at < cutoff))
        item_d = 0 if dry else int(items.purge_deleted(days) or 0)
        ws_purged, blocked = [], []
        for w in ws_rows:
            refs = self._workspace_references(w["id"])
            if refs:
                blocked.append({"workspace_id": w["id"], "name": w["name"], "referenced_by": refs})
                continue
            if not dry:
                self._purge_workspace(w["id"])
            ws_purged.append(w["id"])
        return {"cutoff": cutoff, "items": {"candidates": item_c, "deleted": item_d}, "workspaces": {"candidates": len(ws_rows), "deleted": 0 if dry else len(ws_purged), "would_delete": len(ws_purged) if dry else None,
                                                                                                    "blocked": blocked},
                "candidates": item_c + len(ws_rows), "deleted": item_d + (0 if dry else len(ws_purged))}

    #: tables whose rows belong to a workspace and are removed together with it
    _OWNED = ("item_versions", "items", "connections", "role_bindings")

    def _workspace_references(self, ws_id: str) -> dict[str, int]:
        """Rows in *other* tables that still point at the workspace (jobs history, lakehouses, schedules ...). They block the purge."""
        refs: dict[str, int] = {}
        with self.ctx.db.connect() as conn:
            for name, t in S.metadata.tables.items():
                if name not in self._present or name in HISTORY_TABLES or name in self._OWNED or name == "workspaces" or "workspace_id" not in t.c:
                    continue
                n = self._count(conn, t, t.c.workspace_id == ws_id)
                if n:
                    refs[name] = n
        return refs

    def _purge_workspace(self, ws_id: str) -> None:
        with self.ctx.db.begin() as conn:
            item_ids = [r[0] for r in conn.execute(select(S.items.c.id).where(S.items.c.workspace_id == ws_id)).all()]
            for i in range(0, len(item_ids), 500):
                conn.execute(delete(S.item_versions).where(S.item_versions.c.item_id.in_(item_ids[i:i + 500])))
            conn.execute(delete(S.items).where(S.items.c.workspace_id == ws_id))
            secret_ids = [r[0] for r in conn.execute(select(S.connections.c.secret_id).where(S.connections.c.workspace_id == ws_id, S.connections.c.secret_id.is_not(None))).all()]
            conn.execute(delete(S.connections).where(S.connections.c.workspace_id == ws_id))
            conn.execute(delete(S.role_bindings).where(S.role_bindings.c.scope_type == "workspace", S.role_bindings.c.scope_id == ws_id))
            conn.execute(delete(S.workspaces).where(S.workspaces.c.id == ws_id, S.workspaces.c.deleted_at.is_not(None)))
        for sid in secret_ids:
            try:
                self.ctx.secrets.delete(sid)
            except Exception:  # noqa: BLE001 - already gone / shared with another connection
                log.debug("secret %s of a purged workspace could not be deleted", sid)

    # ------------------------------------------------------------------ audit
    def _step_audit(self, dry: bool) -> dict[str, Any]:
        t = S.audit_log
        days = self.setting("audit_days")
        cutoff = iso_in(-days * 86400)
        with self.ctx.db.connect() as conn:
            last_seq = conn.execute(select(func.max(t.c.seq))).scalar()
            first_keep = conn.execute(select(func.min(t.c.seq)).where(t.c.ts >= cutoff)).scalar()
            if last_seq is None:
                return {"cutoff": cutoff, "candidates": 0, "deleted": 0}
            boundary = min(first_keep, last_seq) if first_keep is not None else last_seq  # the newest record always stays (chain head)
            cand = self._count(conn, t, t.c.seq < boundary)
            head = fetch_one(conn, select(t.c.seq, t.c.hash).where(t.c.seq < boundary).order_by(t.c.seq.desc()).limit(1)) if cand else None
        res: dict[str, Any] = {"cutoff": cutoff, "candidates": cand, "deleted": 0, "kept_from_seq": boundary}
        if dry or not cand:
            return res
        archive = None
        if self.setting("audit_archive"):
            archive = self._archive_audit(boundary)
            res["archive"] = {"file": archive["file"], "sha256": archive["sha256"], "records": archive["records"]}
        deleted, anchor_id = 0, None
        lo_seq = 0
        while True:
            with self.ctx.db.begin() as conn:
                rows = conn.execute(select(t.c.seq, t.c.hash).where(t.c.seq < boundary, t.c.seq > lo_seq).order_by(t.c.seq).limit(BATCH)).all()
                if not rows:
                    break
                hi_seq, hi_hash = rows[-1][0], rows[-1][1]
                n = conn.execute(delete(t).where(t.c.seq > lo_seq, t.c.seq <= hi_seq)).rowcount or len(rows)
                deleted += n
                vals = {"anchored_at": now_iso(), "last_purged_seq": hi_seq, "last_purged_hash": hi_hash, "archive": archive["file"] if archive else None,
                        "archive_sha256": archive["sha256"] if archive else None}
                if anchor_id is None:
                    r = conn.execute(O.audit_anchors.insert().values(**vals, purged_count=n))
                    anchor_id = r.inserted_primary_key[0]
                else:
                    conn.execute(update(O.audit_anchors).where(O.audit_anchors.c.id == anchor_id).values(**vals, purged_count=O.audit_anchors.c.purged_count + n))
                lo_seq = hi_seq
        res["deleted"] = deleted
        res["anchor"] = {"id": anchor_id, "last_purged_seq": head["seq"] if head else None, "last_purged_hash": head["hash"] if head else None}
        self.ctx.audit.record(None, "admin.audit_purged", details={"records": deleted, "archive": archive["file"] if archive else None, "kept_from_seq": boundary})
        return res

    def _archive_audit(self, boundary: int) -> dict[str, Any]:
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        if os.name == "posix":
            os.chmod(self.archive_dir, 0o700)
        tmp = self.archive_dir / f".audit-{new_id('rtn')}.part"
        first = last = None
        n = 0
        h = hashlib.sha256()
        t = S.audit_log
        lo = 0
        with open(tmp, "wb") as raw:
            class _Tee:
                def write(self, b: bytes) -> int:
                    h.update(b)
                    return raw.write(b)

                def flush(self) -> None:
                    raw.flush()

            with gzip.GzipFile(fileobj=_Tee(), mode="wb", mtime=0) as gz:  # type: ignore[arg-type]
                while True:
                    with self.ctx.db.connect() as conn:
                        rows = fetch_all(conn, select(t).where(t.c.seq < boundary, t.c.seq > lo).order_by(t.c.seq).limit(BATCH))
                    if not rows:
                        break
                    for r in rows:
                        gz.write((json.dumps(r, sort_keys=True, default=str, separators=(",", ":")) + "\n").encode())
                        first = r["seq"] if first is None else first
                        last = r["seq"]
                        n += 1
                    lo = rows[-1]["seq"]
            raw.flush()
            os.fsync(raw.fileno())
        final = self.archive_dir / f"audit-{first:012d}-{last:012d}.jsonl.gz"
        os.replace(tmp, final)
        if os.name == "posix":
            os.chmod(final, 0o600)
        return {"file": final.name, "sha256": h.hexdigest(), "records": n, "first_seq": first, "last_seq": last}

    def verify_audit(self) -> dict[str, Any]:
        """Verify the remaining hash chain *and* its link to the retention anchors (detects a prefix removed without one)."""
        return verify_audit_chain(self.ctx.db)

    # ------------------------------------------------------------------ run
    def run(self, principal: Principal | None, *, dry_run: bool = False, only: list[str] | None = None, trigger: str = "manual") -> dict[str, Any]:
        steps = only or STEP_ORDER
        bad = [s for s in steps if s not in STEP_ORDER]
        if bad:
            raise BadRequest(f"unknown retention step(s) {bad}", details={"known": STEP_ORDER})
        if not self._lock.acquire(blocking=False):
            raise Conflict("a retention run is already in progress on this node")
        from olcf.core.leader import LeaderElector

        runlock = LeaderElector(self.ctx.db, "ops.retention.run", self.ctx.node_id, ttl=3600)
        try:
            if not runlock.acquire():
                raise Conflict("a retention run is in progress on another node")
            self._present = self._tables()
            rid = new_id("rtn")
            started = now_iso()
            with self.ctx.db.begin() as conn:
                conn.execute(O.retention_runs.insert().values(id=rid, dry_run=dry_run, started_at=started, report={"state": "running"}))
            report: dict[str, Any] = {"id": rid, "dry_run": dry_run, "trigger": trigger, "started_at": started, "settings": self.effective(), "steps": {}}
            error = None
            t0 = time.perf_counter()
            for name in STEP_ORDER:
                if name not in steps:
                    continue
                try:
                    report["steps"][name] = getattr(self, f"_step_{name}")(dry_run)
                except Exception as exc:  # noqa: BLE001 - one failing step must not stop the others
                    log.exception("retention step %s failed", name)
                    report["steps"][name] = {"error": f"{type(exc).__name__}: {getattr(exc, 'message', exc)}"[:300]}
                    error = "one or more steps failed"
            report["total_candidates"] = sum(int(s.get("candidates", 0) or 0) for s in report["steps"].values())
            report["total_deleted"] = sum(int(s.get("deleted", 0) or 0) for s in report["steps"].values())
            report["seconds"] = round(time.perf_counter() - t0, 2)
            report["finished_at"] = now_iso()
            with self.ctx.db.begin() as conn:
                conn.execute(update(O.retention_runs).where(O.retention_runs.c.id == rid).values(finished_at=report["finished_at"], report=report, error=error))
            if not dry_run or principal is not None:
                self.ctx.audit.record(principal, "admin.retention_run", resource_type="retention", resource_id=rid, outcome="failure" if error else "success",
                                      details={"dry_run": dry_run, "trigger": trigger, "deleted": report["total_deleted"], "candidates": report["total_candidates"]})
            return report
        finally:
            try:
                runlock.release()
            except Exception:  # noqa: BLE001
                pass
            self._lock.release()

    _present: set[str] = set()

    def runs(self, limit: int = 20) -> list[dict[str, Any]]:
        with self.ctx.db.connect() as conn:
            rows = fetch_all(conn, select(O.retention_runs).order_by(O.retention_runs.c.started_at.desc()).limit(limit))
        return rows

    def get_run(self, rid: str) -> dict[str, Any]:
        from olcf.core.errors import NotFound

        with self.ctx.db.connect() as conn:
            row = fetch_one(conn, select(O.retention_runs).where(O.retention_runs.c.id == rid))
        if not row:
            raise NotFound(f"retention run '{rid}' not found")
        return row

    def status(self) -> dict[str, Any]:
        last = next((r for r in self.runs(50) if not r["dry_run"] and r["finished_at"]), None)
        interval = self.setting("interval_hours")
        nxt = None
        if last and self.setting("enabled"):
            nxt = (from_iso(last["started_at"]).timestamp() + interval * 3600)
            nxt = datetime.fromtimestamp(nxt, UTC).isoformat()
        return {"enabled": bool(self.setting("enabled")), "interval_hours": interval, "settings": self.effective(), "last_run": {k: last[k] for k in ("id", "started_at", "finished_at", "error")} if last else None,
                "next_run_after": nxt, "archive_dir": str(self.archive_dir), "audit": self.verify_audit()}

    def tick(self, now: datetime | None = None) -> dict[str, Any]:
        """Leader loop hook: run when ``interval_hours`` have passed since the last real run."""
        if not self.setting("enabled"):
            return {}
        now = now or datetime.now(UTC)
        with self.ctx.db.connect() as conn:
            last = fetch_one(conn, select(O.retention_runs.c.started_at).where(O.retention_runs.c.dry_run.is_(False)).order_by(O.retention_runs.c.started_at.desc()).limit(1))
        if last and (now - from_iso(last["started_at"])).total_seconds() < self.setting("interval_hours") * 3600:
            return {}
        try:
            rep = self.run(None, trigger="schedule")
        except Conflict:
            return {}
        return {"run": rep["id"], "deleted": rep["total_deleted"]}


def install_retention(ctx: AppContext) -> RetentionService:
    svc = RetentionService(ctx)
    svc.register_settings()
    ctx.register_service("retention", svc)
    return svc
