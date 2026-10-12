# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Platform events -> graph, asynchronously and failure-isolated (contract sections 1.1 and 6).

* :class:`InboxWriter` is the event-bus subscriber. It appends the events Intelligence cares about to the durable ``inbox`` table and **never raises into the
  publisher**: an error is counted (``failures``) and, after a few in a row, the writer backs off for 30 seconds instead of paying for a broken table on every
  event. Nothing here is called by compute, SQL or pipelines - they publish ordinary events and are unaware of this module.
* :class:`InboxProcessor` drains the inbox in batches (leader only). Deletion events (`item.deleted`, `lakehouse.deleted`, `workspace.deleted`) retire the
  matching assets; every other row is translated by ``olcf.intelligence.runtime.facts_from_event(event_type, payload) -> Facts | None`` into verified facts and
  applied through the :class:`~olcf.intelligence.engine.Engine` under a synthetic ``runtime`` scan (one open scan per UTC day, mode ``incremental``, so a full
  scan of any other source never sweeps run-time edges). If the translator module is not installed rows are marked ``skipped``.
* Housekeeping: old inbox rows are purged after ``intelligence.retention_days``; expired run-time evidence is pruned; attribute histories are trimmed to
  ``intelligence.max_versions_per_asset``. A ``failed`` row is queued once more after ``intelligence.inbox_retry_after_minutes`` (0 = never), and an operator
  can re-queue all of them at any time (:func:`requeue_failed`, ``POST /intelligence/inbox/requeue``).
"""

from __future__ import annotations

import importlib
import logging
import threading
import time
from collections.abc import Callable
from typing import Any

from sqlalchemy import delete, func, insert, select, update

from olcf.core.health import ComponentHealth, Status
from olcf.core.timeutil import iso_in, now_iso
from olcf.db.util import fetch_all
from olcf.intelligence import settings as rt
from olcf.intelligence.engine import RUNTIME_SOURCE, ApplyStats, Engine
from olcf.intelligence.schema import gaps, inbox, scans

log = logging.getLogger("olcf.intelligence.ingest")

#: Event types the inbox accepts: exact names and prefixes.
INBOX_TYPES = frozenset({"job.state_changed", "item.created", "item.updated", "item.deleted", "lakehouse.created", "lakehouse.deleted", "workspace.created", "workspace.deleted"})
INBOX_PREFIXES = ("dataset.", "pipeline.run.")
MAX_ATTEMPTS = 3
_BACKOFF_AFTER = 3
_BACKOFF_SECONDS = 30.0


def requeue_failed(db: Any, *, older_than_minutes: float | None = None) -> int:
    """Put ``failed`` inbox rows back to ``pending`` and return how many.

    ``older_than_minutes`` is the automatic retry: only rows that failed that long ago and have not been retried automatically before (a row that fails the retry
    keeps ``attempts`` above :data:`MAX_ATTEMPTS`, so it is never picked again). Without it every failed row is re-queued with a fresh set of attempts (the
    operator's decision)."""
    stmt = update(inbox).where(inbox.c.status == "failed")
    if older_than_minutes is None:
        values: dict[str, Any] = {"attempts": 0}
    else:
        cut = iso_in(-float(older_than_minutes) * 60)
        stmt = stmt.where(inbox.c.attempts <= MAX_ATTEMPTS, func.coalesce(inbox.c.updated_at, inbox.c.received_at) < cut)
        values = {"attempts": MAX_ATTEMPTS}               # one more attempt: failing it takes the row past MAX_ATTEMPTS for good
    with db.begin() as conn:
        return int(conn.execute(stmt.values(status="pending", updated_at=now_iso(), **values)).rowcount or 0)


def wanted(event_type: str) -> bool:
    """True for the event types Intelligence consumes."""
    return event_type in INBOX_TYPES or event_type.startswith(INBOX_PREFIXES)


class InboxWriter:
    """Event-bus subscriber that appends relevant events to ``inbox``. Calling it never raises."""

    def __init__(self, db: Any, enabled: Callable[[], bool] | None = None) -> None:
        self.db = db
        self.enabled = enabled or (lambda: True)
        self.written = 0
        self.failures = 0
        self.last_error: str | None = None
        self._consecutive = 0
        self._paused_until = 0.0

    def __call__(self, evt: dict[str, Any]) -> None:
        try:
            etype = str(evt.get("type") or "")
            if not wanted(etype) or not self.enabled():
                return
            if self._paused_until and time.monotonic() < self._paused_until:
                self.failures += 1
                return
            payload = dict(evt.get("payload") or {})
            payload["_meta"] = {"event_id": evt.get("id"), "ts": evt.get("ts"), "subject": evt.get("subject")}
            with self.db.begin() as conn:
                conn.execute(insert(inbox).values(type=etype, workspace_id=evt.get("workspace_id"), payload=payload, received_at=now_iso(), attempts=0, status="pending"))
            self.written += 1
            self._consecutive = 0
        except Exception as exc:  # noqa: BLE001 - the publisher (jobs, SQL, pipelines) must never notice
            self.failures += 1
            self._consecutive += 1
            self.last_error = f"{type(exc).__name__}: {str(exc)[:200]}"
            if self._consecutive >= _BACKOFF_AFTER:
                self._paused_until = time.monotonic() + _BACKOFF_SECONDS
                self._consecutive = 0
            log.debug("could not append event to the intelligence inbox: %s", exc)


class LeaderLoop:
    """A daemon thread that calls ``fn`` every ``interval`` seconds while this replica holds the control-plane lease. ``tick_now`` runs one pass synchronously."""

    def __init__(self, ctx: Any, name: str, fn: Callable[[], Any], interval: float | Callable[[], float], logger: logging.Logger = log) -> None:
        self.ctx, self.name, self.fn, self.interval, self.log = ctx, name, fn, interval, logger
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the thread (idempotent)."""
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name=f"olcf-{self.name}", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 10.0) -> None:
        """Stop the thread and wait for it."""
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=timeout)
            self._thread = None

    def tick_now(self) -> Any:
        """Run one pass in the calling thread."""
        return self.fn()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                if self.ctx.leader.is_leader():
                    self.fn()
            except Exception:  # noqa: BLE001
                self.log.exception("%s loop failed", self.name)
            self._stop.wait(self.interval() if callable(self.interval) else self.interval)


class InboxProcessor:
    """Drains the inbox: translate, apply, mark. ``tick()`` is one pass (batches until the inbox is empty or a time budget is used)."""

    def __init__(self, ctx: Any, engine: Engine, *, batch_size: Callable[[], int] | None = None) -> None:
        self.ctx, self.engine = ctx, engine
        self.db = ctx.db
        self.batch_size = batch_size or (lambda: int(ctx.settings.intelligence.inbox_batch_size))
        self._scan_id: str | None = None
        self._scan_day: str | None = None
        self._acc = ApplyStats()
        self._last_housekeeping = 0.0
        self._last_retry_check = 0.0
        self.requeued = 0
        self._lock = threading.Lock()
        self.processed = 0
        self.applied = 0
        self.skipped = 0
        self.failed = 0
        self.last_error: str | None = None

    # ------------------------------------------------------------------------------------------------------------------------ translator
    @staticmethod
    def _translator() -> Callable[[str, dict[str, Any]], Any] | None:
        try:
            return importlib.import_module("olcf.intelligence.runtime").facts_from_event  # type: ignore[no-any-return]
        except ModuleNotFoundError as exc:
            if exc.name == "olcf.intelligence.runtime":
                return None
            raise

    # ------------------------------------------------------------------------------------------------------------------------ the synthetic runtime scan
    def runtime_scan(self) -> str:
        """The open ``runtime`` scan of today (UTC); yesterday's is closed ``succeeded`` and a new one begins. Survives restarts: an open scan of today is reused."""
        today = now_iso()[:10]
        if self._scan_id and self._scan_day == today:
            return self._scan_id
        if self._scan_id:
            self._close_scan()
        row = self.engine.latest_scan(RUNTIME_SOURCE, status="running")
        if row is not None and str(row["started_at"])[:10] == today:
            self._scan_id, self._scan_day = row["id"], today
            return row["id"]
        if row is not None:
            self.engine.finish_scan(row["id"], "succeeded", {"closed_by": "day rollover"})
        self._scan_id = self.engine.begin_scan(RUNTIME_SOURCE, "incremental", "runtime")
        self._scan_day = today
        self._acc = ApplyStats()
        return self._scan_id

    def _close_scan(self) -> None:
        if self._scan_id:
            try:
                self.engine.finish_scan(self._scan_id, "succeeded", self._acc.as_dict())
            except Exception:  # noqa: BLE001 - already closed by another replica
                log.debug("runtime scan %s could not be closed", self._scan_id, exc_info=True)
        self._scan_id, self._scan_day = None, None

    def close(self) -> None:
        """Close today's runtime scan (shutdown)."""
        with self._lock:
            self._close_scan()

    # ------------------------------------------------------------------------------------------------------------------------ processing
    def tick(self) -> dict[str, int]:
        """Process pending rows (several batches, at most ~5 seconds); returns the counters of this pass."""
        with self._lock:
            out = {"processed": 0, "applied": 0, "skipped": 0, "failed": 0}
            deadline = time.monotonic() + 5.0
            after = 0                                  # a row is attempted once per pass: its retries are spaced by the poll interval, not burnt back to back
            while time.monotonic() < deadline:
                n, after = self._batch(out, after)
                if n == 0:
                    break
            if time.monotonic() - self._last_retry_check > 60:
                self._last_retry_check = time.monotonic()
                self._retry_failed()
            if time.monotonic() - self._last_housekeeping > 3600:
                self._last_housekeeping = time.monotonic()
                self._housekeeping()
            return out

    def _batch(self, out: dict[str, int], after: int = 0) -> tuple[int, int]:
        """One batch of pending rows with ``seq > after``; returns ``(rows handled, highest seq handled)``."""
        size = max(1, self.batch_size())
        with self.db.connect() as c:
            rows = fetch_all(c, select(inbox).where(inbox.c.status == "pending", inbox.c.seq > after).order_by(inbox.c.seq).limit(size))
        if not rows:
            return 0, after
        translate = self._translator()
        done: list[int] = []
        skipped: list[tuple[int, str | None]] = []
        for r in rows:
            out["processed"] += 1
            try:
                verdict = self._handle(r, translate)
            except Exception as exc:  # noqa: BLE001 - one bad event never blocks the queue
                msg = f"{type(exc).__name__}: {str(exc)[:300]}"
                self.last_error = msg
                attempts = r["attempts"] + 1
                final = attempts >= MAX_ATTEMPTS
                with self.db.begin() as conn:
                    conn.execute(update(inbox).where(inbox.c.seq == r["seq"]).values(attempts=attempts, error=msg, status="failed" if final else "pending",
                                                                                      updated_at=now_iso()))
                if final:
                    out["failed"] += 1
                    self.failed += 1
                continue
            if verdict == "applied":
                done.append(r["seq"])
                out["applied"] += 1
            else:
                skipped.append((r["seq"], verdict if verdict not in ("skipped",) else None))
                out["skipped"] += 1
        with self.db.begin() as conn:
            for i in range(0, len(done), 300):
                conn.execute(update(inbox).where(inbox.c.seq.in_(done[i:i + 300])).values(status="done", error=None, attempts=inbox.c.attempts + 1, updated_at=now_iso()))
            for seq, why in skipped:
                conn.execute(update(inbox).where(inbox.c.seq == seq).values(status="skipped", error=why, attempts=inbox.c.attempts + 1, updated_at=now_iso()))
        self.processed += len(rows)
        self.applied += len(done)
        self.skipped += len(skipped)
        return len(rows), int(rows[-1]["seq"])

    def _handle(self, row: dict[str, Any], translate: Callable[[str, dict[str, Any]], Any] | None) -> str:
        etype = row["type"]
        payload = dict(row["payload"] or {})
        meta = payload.pop("_meta", None) or {}
        if etype in ("item.deleted", "lakehouse.deleted", "workspace.deleted"):
            subject = meta.get("subject")
            if not subject:
                return "no subject on the deletion event"
            kind = "olcf_workspace" if etype == "workspace.deleted" else "olcf_item"
            self.engine.retire_by_alias(kind, str(subject), scan_id=self.runtime_scan())
            return "applied"
        if translate is None:
            return "the run-time translator (olcf.intelligence.runtime) is not installed"
        if meta.get("subject") and "subject" not in payload:
            payload["_subject"] = meta["subject"]
        if "workspace_id" not in payload and row.get("workspace_id"):
            payload["workspace_id"] = row["workspace_id"]
        facts = translate(etype, payload)
        if facts is None or not (facts.assets or facts.edges or facts.gaps):
            return "skipped"
        scan_id = self.runtime_scan()
        stats = self.engine.apply_facts(facts, scan_id=scan_id, source_id=RUNTIME_SOURCE)
        self._acc.add(stats)
        if stats.errors and not (stats.assets_created or stats.assets_updated or stats.edges_created or stats.edges_updated or stats.edges_unchanged or stats.assets_unchanged):
            raise RuntimeError("; ".join(stats.error_samples[:3]) or "the facts could not be applied")
        return "applied"

    # ------------------------------------------------------------------------------------------------------------------------ housekeeping
    def _retry_failed(self) -> int:
        """Queue the ``failed`` rows that failed ``intelligence.inbox_retry_after_minutes`` ago once more (0 disables). Never raises."""
        try:
            minutes = float(rt.get(self.ctx, "intelligence.inbox_retry_after_minutes", 30))
            if minutes <= 0:
                return 0
            n = requeue_failed(self.db, older_than_minutes=minutes)
            if n:
                self.requeued += n
                log.info("re-queued %d failed inbox row(s) for one more attempt", n)
            return n
        except Exception:  # noqa: BLE001
            log.exception("re-queueing failed inbox rows failed")
            return 0

    def _housekeeping(self) -> dict[str, int]:
        """The hourly pass: purge old inbox rows, prune expired run-time evidence and trim long attribute histories. Each step is isolated from the others."""
        out = {"inbox_rows": 0, "pruned_edges": 0, "versions": 0}
        try:
            out["inbox_rows"] = self.purge()["inbox_rows"]
        except Exception:  # noqa: BLE001
            log.exception("intelligence inbox purge failed")
        try:
            out["pruned_edges"] = self.engine.prune_runtime_evidence()
            if out["pruned_edges"]:
                log.info("pruned expired run-time evidence from %d edge(s)", out["pruned_edges"])
        except Exception:  # noqa: BLE001
            log.exception("intelligence evidence pruning failed")
        try:
            out["versions"] = self.engine.trim_versions(int(rt.get(self.ctx, "intelligence.max_versions_per_asset", 200)))
            if out["versions"]:
                log.info("trimmed %d old attribute version(s)", out["versions"])
        except Exception:  # noqa: BLE001
            log.exception("intelligence history trimming failed")
        return out

    def purge(self) -> dict[str, int]:
        """Delete processed inbox rows older than ``intelligence.retention_days`` (and unprocessable ones after twice that)."""
        days = int(rt.get(self.ctx, "intelligence.retention_days", 30))
        cut, cut_pending = iso_in(-days * 86400), iso_in(-2 * days * 86400)
        with self.db.begin() as conn:
            done = conn.execute(delete(inbox).where(inbox.c.status.in_(("done", "skipped", "failed")), inbox.c.received_at < cut)).rowcount
            stuck = conn.execute(delete(inbox).where(inbox.c.received_at < cut_pending)).rowcount
        return {"inbox_rows": done + stuck}


def health_check(ctx: Any, service: Any, writer: InboxWriter, processor: InboxProcessor | None) -> tuple[Callable[[], ComponentHealth], Callable[[], ComponentHealth]]:
    """The quick and deep functions of the ``intelligence`` health check. ``quick`` only reads the local database (never the network)."""
    cfg = ctx.settings.intelligence

    def quick() -> ComponentHealth:
        if not cfg.enabled:
            return ComponentHealth("intelligence", Status.UNKNOWN, "disabled (intelligence.enabled = false): the graph is not updated", category="data")
        stats = service.inbox_stats()
        with ctx.db.connect() as c:
            last = []
            for sid in [r[0] for r in c.execute(select(scans.c.source_id).distinct())]:
                s = fetch_all(c, select(scans).where(scans.c.source_id == sid).order_by(scans.c.version.desc()).limit(1))
                if s:
                    last.append({"source_id": sid, "status": s[0]["status"], "finished_at": s[0]["finished_at"], "version": s[0]["version"]})
            open_gaps = int(c.execute(select(func.count()).select_from(gaps).where(gaps.c.resolved_at.is_(None))).scalar() or 0)
        lag = stats["oldest_pending_seconds"]
        problems = []
        if stats["pending"] > cfg.inbox_lag_warn:
            problems.append(f"{stats['pending']} events waiting")
        if lag is not None and lag > 300 and stats["pending"]:
            problems.append(f"oldest waiting event is {int(lag)} s old")
        if writer.failures:
            problems.append(f"{writer.failures} event(s) could not be recorded ({writer.last_error})")
        if stats["failed"]:
            problems.append(f"{stats['failed']} inbox row(s) failed")
        failed_scans = [s for s in last if s["status"] in ("failed",) and s["source_id"] != RUNTIME_SOURCE]
        if failed_scans:
            problems.append("last scan failed for " + ", ".join(s["source_id"] for s in failed_scans))
        labels = {"inbox": stats, "writer": {"written": writer.written, "failures": writer.failures}, "last_scans": last, "open_gaps": open_gaps,
                  "processor": {"applied": processor.applied, "skipped": processor.skipped, "failed": processor.failed} if processor else None}
        detail = f"asset graph; inbox {stats['pending']} pending / {stats['failed']} failed; {open_gaps} open gap(s)"
        return ComponentHealth("intelligence", Status.DEGRADED if problems else Status.OK, detail + ("; " + "; ".join(problems) if problems else ""), category="data", labels=labels)

    def deep() -> ComponentHealth:
        steps: list[dict[str, Any]] = []
        ok = True
        seq: int | None = None
        try:
            with ctx.db.begin() as conn:
                res = conn.execute(insert(inbox).values(type="intelligence.probe", workspace_id=None, payload={}, received_at=now_iso(), attempts=0, status="done"))
                pk = res.inserted_primary_key
                seq = int(pk[0]) if pk else None
            steps.append({"step": "inbox write", "ok": True})
            with ctx.db.connect() as c:
                got = c.execute(select(inbox.c.seq).where(inbox.c.seq == seq)).first()
            steps.append({"step": "inbox read-back", "ok": got is not None})
            ok = got is not None
        except Exception as exc:  # noqa: BLE001
            steps.append({"step": "inbox write", "ok": False, "error": str(exc)[:200]})
            ok = False
        finally:
            if seq is not None:
                try:
                    with ctx.db.begin() as conn:
                        conn.execute(delete(inbox).where(inbox.c.seq == seq))
                except Exception:  # noqa: BLE001
                    pass
        try:
            service.graph.walk([], direction="down", depth=1)
            steps.append({"step": "graph query", "ok": True})
        except Exception as exc:  # noqa: BLE001
            steps.append({"step": "graph query", "ok": False, "error": str(exc)[:200]})
            ok = False
        base = quick()
        return ComponentHealth("intelligence", Status.OK if ok and base.status is Status.OK else (Status.DOWN if not ok else base.status), base.detail if ok else "probe failed: " + ", ".join(s["step"] for s in steps if not s["ok"]),
                               category="data", labels=base.labels, steps=steps, mode="deep")

    return quick, deep


__all__ = ["INBOX_TYPES", "InboxProcessor", "InboxWriter", "LeaderLoop", "health_check", "wanted"]

