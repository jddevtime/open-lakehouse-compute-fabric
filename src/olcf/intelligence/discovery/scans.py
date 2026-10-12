# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The scan runner: the only writer of discovery results.

A scan of one source is

1. **admission** (:meth:`ScanRunner.start`): the source exists and is enabled, the mode is valid (an incremental scan of a source that never completed a scan is promoted to
   ``full``), the in-process concurrency limit holds, and the source's *lease* is taken with one conditional ``UPDATE`` - so there is at most one running scan per source across all
   replicas (a second request gets ``409 Conflict`` naming the running scan). A lease whose owner disappeared is taken over and the abandoned ``running`` scan is closed as ``failed``.
2. **execution** (:meth:`ScanRunner._execute`, on a worker thread or inline with ``wait``): the connector's ``discover`` generator is consumed one batch at a time; each batch is
   applied through the :class:`~olcf.intelligence.engine.Engine` (``apply_facts`` / ``analyze_and_apply``, each fact in its own serialised transaction, so a crash loses at most the
   current batch); gaps reported by the connector are stored after every batch; a heartbeat thread keeps the lease alive; cancellation (this process or any other replica, through the
   ``cancel_scan_id`` flag of the source row) is observed between batches and while the connector waits out a throttle.
3. **outcome**: ``cancelled`` when asked to stop; ``failed`` when the connector could not proceed at all (:class:`~olcf.intelligence.discovery.base.ConnectorFailure`, credentials
   rejected everywhere, an unexpected exception - recorded without a stack trace or secrets); ``partial`` when the scan finished but something was not read (a blocking gap, a fact that
   could not be applied, a request-level workspace filter on a full scan); otherwise ``succeeded``. **Only a ``succeeded`` full scan runs the removal sweep** (the engine enforces
   that), so a permission error or a throttled workspace can never make the graph forget what it simply could not see. A gap marked ``informational`` (``location.informational`` or
   kind ``sampling_disabled``) says "less detail", not "something may be missing", and does not block the sweep.

The runner never raises into a caller thread for problems of the *source* - they become the scan's status and gaps.
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from functools import partial
from typing import Any

from sqlalchemy import select

from olcf.core.errors import BadRequest, Conflict, NotFound
from olcf.core.ids import new_id
from olcf.core.timeutil import now_iso
from olcf.db.util import fetch_all
from olcf.intelligence import settings as rt
from olcf.intelligence.discovery import identities
from olcf.intelligence.discovery.base import (
    CancellationToken,
    ConnectorFailure,
    ScanCancelled,
    ScanContext,
    connector_for,
    facts_counts,
    safe_message,
    scrub,
)
from olcf.intelligence.discovery.sources import SourceStore, holder_id
from olcf.intelligence.engine import RUNTIME_SOURCE, ApplyStats
from olcf.intelligence.model import DefinitionFact, Gap
from olcf.intelligence.schema import scans

log = logging.getLogger("olcf.intelligence.discovery.scans")

SCAN_MODES = ("full", "incremental")
TRIGGERS = ("manual", "schedule", "startup", "api", "import")
DEFAULT_MAX_CONCURRENT = 2


def is_blocking(gap: Gap) -> bool:
    """True when a connector gap means "something may have been missed" (so the scan cannot be ``succeeded`` and must not sweep); informational gaps do not block."""
    return not (gap.kind == "sampling_disabled" or (gap.location or {}).get("informational"))


class ScanRunner:
    """Admits, runs, cancels and reaps scans. One instance per process (``service.discovery.runner``)."""

    def __init__(self, ctx: Any, service: Any, store: SourceStore) -> None:
        self.ctx, self.service, self.store = ctx, service, store
        self.engine = service.engine
        self._pool: ThreadPoolExecutor | None = None
        self._lock = threading.Lock()
        self._active: dict[str, Future[Any] | None] = {}      # None = running inline on the caller's thread (``wait=True``)
        self._tokens: dict[str, CancellationToken] = {}
        #: replaceable in tests: ``(callable)`` that returns the sleep function a connector uses while throttled
        self.sleep: Any = None
        #: replaceable in tests: a factory returning the HTTP client factory for Fabric connectors
        self.http_client_factory: Any = None
        #: replaceable in tests: ``() -> ISO-8601 UTC string`` used as "now" by connectors (scan start, watermarks, request budgets); ``None`` = the system clock
        self.now: Any = None

    # ------------------------------------------------------------------------------------------------------------------------ settings
    def max_concurrent(self) -> int:
        """Most scans running in this process at once (``intelligence.max_concurrent_scans``)."""
        return max(1, int(rt.get(self.ctx, "intelligence.max_concurrent_scans", DEFAULT_MAX_CONCURRENT)))

    def limits(self) -> dict[str, Any]:
        """Runtime limits handed to connectors (from the ``intelligence.*`` settings)."""
        g = self.ctx
        return {"scanner_batch_size": int(rt.get(g, "intelligence.fabric_scanner_batch_size", 100)),
                "getinfo_requests_per_hour": int(rt.get(g, "intelligence.fabric_getinfo_requests_per_hour", 500)),
                "modified_requests_per_hour": int(rt.get(g, "intelligence.fabric_modified_requests_per_hour", 30)),
                "max_wait_seconds": float(rt.get(g, "intelligence.fabric_max_wait_seconds", 120))}

    def active_scan_ids(self) -> list[str]:
        """Scans running in this process."""
        with self._lock:
            return sorted(self._active)

    # ------------------------------------------------------------------------------------------------------------------------ admission
    def start(self, principal: Any, source_ref: str, *, mode: str = "incremental", trigger: str = "manual", workspace_filter: list[str] | None = None,
              wait: bool = False) -> dict[str, Any]:
        """Admit a scan and run it (in the background unless ``wait``). Returns the ``scans`` row as it is after admission (``running``) or, with ``wait``, after the scan ended.

        Raises ``NotFound`` (no such source), ``Conflict`` (disabled source, a scan already running, too many scans in this process), ``BadRequest`` (bad mode / filter / nothing imported).
        """
        if mode not in SCAN_MODES:
            raise BadRequest(f"mode must be one of {list(SCAN_MODES)}")
        if trigger not in TRIGGERS:
            raise BadRequest(f"trigger must be one of {list(TRIGGERS)}")
        source = self.store.find(source_ref)
        if not source["enabled"]:
            raise Conflict(f"discovery source '{source['name']}' is disabled", remediation="enable it first (PATCH /v1/intelligence/sources/{id} with enabled=true)")
        if source["kind"] == "scanner_import" and not source.get("payload"):
            raise BadRequest(f"no scanner result has been imported into '{source['name']}'", remediation="POST the saved scanResult JSON to /v1/intelligence/import/scanner")
        flt = self._clean_filter(workspace_filter)
        promoted = False
        if mode == "incremental" and not (source.get("last_full_at") or source.get("last_incremental_at")):
            mode, promoted = "full", True
        with self._lock:
            if len(self._active) >= self.max_concurrent():
                raise Conflict(f"{len(self._active)} scan(s) are already running in this process (the limit is {self.max_concurrent()})",
                               remediation="wait for one to finish or raise intelligence.max_concurrent_scans")
        token = new_id("scn")
        held = self.store.acquire(source["id"], token)
        if held is not None:
            held = self._settled(held)
            raise Conflict(f"a scan of '{source['name']}' is already running ({held.get('lock_scan_id')})", details={"source_id": source["id"], "scan_id": held.get("lock_scan_id")},
                           remediation="wait for it, or cancel it with POST /v1/intelligence/scans/{id}/cancel")
        try:
            self._close_abandoned(source["id"])
            scan_id = self.engine.begin_scan(source["id"], mode, trigger, principal if getattr(principal, "id", None) not in (None, "usr_system") else None)
            self.store.rebind(source["id"], token, scan_id)
        except Exception:
            self.store.release(source["id"], token)
            raise
        started = now_iso()
        self.store.record_result(source["id"], scan_id=scan_id, status="running", started_at=started, mode=mode, state=None, ok=False)
        job = {"source": self.store.get(source["id"]), "scan_id": scan_id, "token": token, "mode": mode, "principal": principal, "filter": flt, "started": started, "promoted": promoted}
        cancel = CancellationToken(lambda: self.store.cancel_requested(source["id"], scan_id))
        with self._lock:
            self._tokens[scan_id] = cancel
            if wait:
                self._active[scan_id] = None
        if wait:
            try:
                self._execute(job, cancel)
            finally:
                with self._lock:
                    self._active.pop(scan_id, None)
            return self.engine.scan_row(scan_id)
        with self._lock:
            if self._pool is None:
                self._pool = ThreadPoolExecutor(max_workers=max(self.max_concurrent(), 1), thread_name_prefix="olcf-scan")
            fut = self._pool.submit(self._execute, job, cancel)
            self._active[scan_id] = fut
        fut.add_done_callback(partial(self._done_cb, scan_id))
        return self.engine.scan_row(scan_id)

    def _settled(self, held: dict[str, Any]) -> dict[str, Any]:
        """The lease row of the competing scan once it names a real scan: between its ``acquire`` and its ``begin_scan`` the lease carries a pre-scan token, and the conflict
        message must name the scan, so wait (a few milliseconds in practice) for the holder to rebind it."""
        row = held
        for _ in range(40):
            lock = row.get("lock_scan_id")
            if not lock or self.store.is_scan_row(str(lock)):
                return row
            time.sleep(0.05)
            row = self.store.get(held["id"])
        return row

    def _done_cb(self, scan_id: str, _future: Future[Any]) -> None:
        with self._lock:
            self._active.pop(scan_id, None)

    @staticmethod
    def _clean_filter(flt: list[str] | None) -> frozenset[str] | None:
        if flt is None:
            return None
        if not isinstance(flt, (list, tuple, set, frozenset)) or not all(isinstance(x, str) and x.strip() for x in flt):
            raise BadRequest("workspace_ids must be a list of workspace ids")
        if not flt:
            raise BadRequest("workspace_ids must not be empty (omit it to scan everything)")
        return frozenset(x.strip() for x in flt)

    def _close_abandoned(self, source_id: str) -> None:
        """A lease was taken over: the scan its former holder left ``running`` is closed as ``failed``."""
        old = self.store.abandoned(source_id)
        if old is None:
            return
        with self._lock:
            if old in self._active:
                return
        try:
            self.engine.finish_scan(old, "failed", {"abandoned": True}, "interrupted: the process running this scan stopped before it finished")
        except (Conflict, NotFound):
            pass

    # ------------------------------------------------------------------------------------------------------------------------ execution
    def _context(self, job: dict[str, Any], cancel: CancellationToken) -> ScanContext:
        source = job["source"]
        since = None
        if job["mode"] == "incremental":
            marks = [m for m in (source.get("last_full_at"), source.get("last_incremental_at")) if m]
            since = max(marks) if marks else None
        secrets = identities.resolve_secrets(self.ctx, source) if source["kind"] == "fabric" else None
        kwargs: dict[str, Any] = {}
        if secrets is not None:
            kwargs["secrets"] = secrets
        if self.http_client_factory is not None:
            kwargs["http_client_factory"] = self.http_client_factory
        if self.sleep is not None:
            kwargs["sleep"] = self.sleep
        if self.now is not None:
            kwargs["now"] = self.now
        return ScanContext(source=dict(source), config=dict(source.get("config") or {}), mode=job["mode"], cancel=cancel, since=since, workspace_filter=job["filter"],
                           state=dict(source.get("state") or {}), app=self.ctx, payload=source.get("payload"), principal=job["principal"], limits=self.limits(), **kwargs)

    def _execute(self, job: dict[str, Any], cancel: CancellationToken) -> None:
        source, scan_id, token, mode = job["source"], job["scan_id"], job["token"], job["mode"]
        sid = source["id"]
        beat_stop = threading.Event()
        beat = threading.Thread(target=self._heartbeat, args=(sid, scan_id, beat_stop, cancel), name=f"olcf-scan-beat-{scan_id[-6:]}", daemon=True)
        beat.start()
        t0 = time.perf_counter()
        agg = ApplyStats()
        counts = {"assets": 0, "edges": 0, "gaps": 0, "definitions": 0}
        blocking = 0
        gaps_reported = 0
        status, error = "succeeded", None
        sctx: ScanContext | None = None
        try:
            try:
                sctx = self._context(job, cancel)
                connector = connector_for(source["kind"])
                for batch in connector.discover(sctx):
                    cancel.raise_if_cancelled()
                    stats = (self.engine.analyze_and_apply(batch, scan_id=scan_id, source_id=sid) if isinstance(batch, DefinitionFact)
                             else self.engine.apply_facts(batch, scan_id=scan_id, source_id=sid))
                    agg.add(stats)
                    for k, v in facts_counts(batch).items():
                        counts[k] += v
                    n_block, n_all = self._flush_gaps(sctx, scan_id, sid)
                    blocking, gaps_reported = blocking + n_block, gaps_reported + n_all
                cancel.raise_if_cancelled()
                n_block, n_all = self._flush_gaps(sctx, scan_id, sid)
                blocking, gaps_reported = blocking + n_block, gaps_reported + n_all
                status, error = self._outcome(job, agg, blocking)
            except ScanCancelled:
                status, error = "cancelled", "the scan was cancelled"
                if sctx is not None:
                    self._flush_gaps(sctx, scan_id, sid)
            except ConnectorFailure as exc:
                status, error = "failed", scrub(str(exc)) + (f" ({exc.remediation})" if exc.remediation else "")
            except Exception as exc:  # noqa: BLE001 - a connector bug or an unexpected API failure is the scan's failure, never a crash
                log.warning("scan %s of source %s failed: %s: %s", scan_id, source["name"], type(exc).__name__, safe_message(exc))
                status, error = "failed", f"{type(exc).__name__}: {safe_message(exc)}"
            stats_doc = {"apply": agg.as_dict(), "facts": counts, "counters": dict(sctx.counters) if sctx else {}, "gaps_reported": gaps_reported, "blocking_gaps": blocking,
                         "duration_seconds": round(time.perf_counter() - t0, 3), "trigger_mode": mode, "promoted_to_full": job["promoted"],
                         "workspace_filter": sorted(job["filter"]) if job["filter"] else None}
            self._finish(sid, scan_id, status, stats_doc, error, job, sctx)
        finally:
            beat_stop.set()
            beat.join(timeout=5)
            self.store.release(sid, scan_id, token)
            with self._lock:
                self._tokens.pop(scan_id, None)

    def _outcome(self, job: dict[str, Any], agg: ApplyStats, blocking: int) -> tuple[str, str | None]:
        reasons: list[str] = []
        if blocking:
            reasons.append(f"{blocking} part(s) of the source could not be read (see the scan's gaps)")
        if agg.errors:
            reasons.append(f"{agg.errors} fact(s) could not be applied ({'; '.join(agg.error_samples[:2])})")
        # lineage analysis reports its failures as gaps inside the analysed facts, not as exceptions: a parse error, an unsupported construct that lost edges or an analysis cap means that the
        # definition's lineage is incomplete. The engine counts those (``blocking_gaps`` / ``incomplete``); such a scan is partial, never ``succeeded``, so nothing is swept on its strength.
        lost = int(getattr(agg, "blocking_gaps", 0) or 0)
        if lost or getattr(agg, "incomplete", False):
            reasons.append(f"{lost or 'some'} definition(s) could not be analysed completely (see the scan's gaps); their last known lineage was kept")
        if job["mode"] == "full" and job["filter"]:
            reasons.append("the scan was limited to some workspaces, so unseen assets are not removed")
        if reasons:
            return "partial", "; ".join(reasons) + ("; the removal sweep was skipped" if job["mode"] == "full" else "")
        return "succeeded", None

    def _flush_gaps(self, sctx: ScanContext, scan_id: str, source_id: str) -> tuple[int, int]:
        """Store the gaps the connector reported since the last call; returns ``(blocking, all)``."""
        if not sctx.gaps:
            return 0, 0
        batch, sctx.gaps = list(sctx.gaps), []
        try:
            self.engine.record_gaps(scan_id, batch, source_id)
        except Exception as exc:  # noqa: BLE001 - losing a gap row must not lose the scan
            log.warning("could not record %d gap(s) of scan %s: %s", len(batch), scan_id, safe_message(exc))
        return sum(1 for g in batch if is_blocking(g)), len(batch)

    def _finish(self, source_id: str, scan_id: str, status: str, stats: dict[str, Any], error: str | None, job: dict[str, Any], sctx: ScanContext | None) -> None:
        if status in ("succeeded", "partial"):
            # fence: only the holder of the source's lease may finish a scan (a full ``succeeded`` scan sweeps). A holder whose lease was taken over while it was stalled must not
            # sweep behind the new holder's back; confirming (and extending) the lease is one conditional UPDATE.
            try:
                held = self.store.heartbeat(source_id, scan_id)
            except Exception:  # noqa: BLE001 - cannot confirm: treat as lost, the safe side
                held = False
            if not held:
                status, error = "failed", "the lease on the source was lost before the scan finished (another process took it over); nothing was swept"
        try:
            self.engine.finish_scan(scan_id, status, stats, error)
        except Exception as exc:  # noqa: BLE001
            log.error("scan %s could not be closed as %s: %s", scan_id, status, safe_message(exc))
            try:
                self.engine.finish_scan(scan_id, "failed", {}, f"could not record the outcome: {safe_message(exc)}")
            except Exception:  # noqa: BLE001
                log.exception("scan %s could not be closed at all", scan_id)
            status = "failed"
        ok = status in ("succeeded", "partial")
        try:
            self.store.record_result(source_id, scan_id=scan_id, status=status, started_at=job["started"], mode=job["mode"], state=dict(sctx.state) if (sctx and ok) else None, ok=ok,
                                    release=(scan_id, job["token"]))
        except Exception:  # noqa: BLE001
            log.exception("source %s could not be updated after scan %s", source_id, scan_id)

    def _heartbeat(self, source_id: str, scan_id: str, stop: threading.Event, cancel: CancellationToken) -> None:
        interval = max(1.0, self.store.lease_seconds / 3)
        while not stop.wait(interval):
            try:
                if not self.store.heartbeat(source_id, scan_id):
                    log.warning("the lease of scan %s was lost; cancelling it", scan_id)
                    cancel.cancel()
                    return
            except Exception:  # noqa: BLE001 - a failed beat is retried on the next interval
                log.exception("lease heartbeat of scan %s failed", scan_id)

    # ------------------------------------------------------------------------------------------------------------------------ cancel / reap / shutdown
    def cancel(self, principal: Any, scan_id: str) -> dict[str, Any]:
        """Ask a running scan to stop. The scan notices between batches (on any replica) and ends ``cancelled``; its changes so far stay, nothing is swept."""
        row = self.engine.scan_row(scan_id)
        if row["status"] != "running":
            raise Conflict(f"scan '{scan_id}' is already {row['status']}")
        with self._lock:
            tok = self._tokens.get(scan_id)
        flagged = self.store.request_cancel(row["source_id"], scan_id)
        if tok is not None:
            tok.cancel()
            flagged = True
        if not flagged:
            # nobody holds the lease any more: the scan is abandoned, close it here
            try:
                self.engine.finish_scan(scan_id, "cancelled", {"abandoned": True}, "cancelled; the process running this scan had stopped")
            except Conflict:
                pass
            self.store.release(row["source_id"], scan_id)
        self.ctx.audit.record(principal, "intelligence.scan_cancel_requested", resource_type="scan", resource_id=scan_id, details={"source_id": row["source_id"]})
        return {**self.engine.scan_row(scan_id), "cancel_requested": True}

    def reap(self) -> int:
        """Close ``running`` scans whose lease holder disappeared (leader-only housekeeping). Returns how many were closed."""
        with self.ctx.db.connect() as c:
            running = fetch_all(c, select(scans.c.id, scans.c.source_id, scans.c.started_at).where(scans.c.status == "running", scans.c.source_id != RUNTIME_SOURCE))
        n = 0
        for r in running:
            with self._lock:
                if r["id"] in self._active:
                    continue
            try:
                src = self.store.get(r["source_id"])
            except NotFound:
                src = None
            if src is not None and self.store.is_leased(src):
                continue                                       # somebody (this scan on another replica, or a newer scan) is working on the source
            try:
                self.engine.finish_scan(r["id"], "failed", {"abandoned": True}, "interrupted: the process running this scan stopped before it finished")
                n += 1
            except (Conflict, NotFound):
                continue
            if src is not None:
                self.store.release(r["source_id"], r["id"])
        return n

    def shutdown(self, timeout: float = 20.0) -> None:
        """Stop: cancel every scan of this process, wait for them to finish recording and stop the pool."""
        with self._lock:
            tokens = list(self._tokens.values())
            futures = [f for f in self._active.values() if f is not None]
            pool = self._pool
        for t in tokens:
            t.cancel()
        deadline = time.monotonic() + timeout
        for f in futures:
            try:
                f.result(timeout=max(0.1, deadline - time.monotonic()))
            except Exception:  # noqa: BLE001 - includes timeouts; the scan is abandoned and reaped by the next leader
                log.warning("a scan did not stop within %.0f s at shutdown; it will be reaped as abandoned", timeout)
        if pool is not None:
            pool.shutdown(wait=False, cancel_futures=True)
            self._pool = None

    def holder(self) -> str:
        """Identity string of this process for leases."""
        return holder_id()
