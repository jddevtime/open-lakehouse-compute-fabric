# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Scheduled discovery: a leader-only loop that starts the scans that are due.

Only the replica that holds platform leadership runs :meth:`DiscoveryScheduler.tick` (the loop is :class:`olcf.pipelines.common.LeaderLoop`); manual scans run on whichever
replica receives the request, and the per-source lease keeps the two from colliding.

A source's schedule is ``{"incremental_seconds": n, "full_seconds": m}`` on the source (0 = never); a missing key falls back to the platform setting of its kind:

================  ======================================================  =====================================================
kind              incremental                                              full
================  ======================================================  =====================================================
``local``         ``intelligence.local_scan_interval_seconds`` (900)       ``intelligence.local_full_scan_interval_seconds`` (86400)
``catalog_mirror`` same as ``local``                                       same as ``local``
``fabric``        ``intelligence.fabric_incremental_interval_seconds``     ``intelligence.fabric_full_scan_interval_seconds``
                  (3600)                                                   (86400)
``scanner_import`` never (the document changes only when it is imported)   never
================  ======================================================  =====================================================

Rules: a source that never completed a full scan gets one first (when ``intelligence.scan_on_start`` is on; otherwise it waits for a manual scan); a full scan that is due wins over an incremental one; a source whose last scan failed is retried no sooner than
``min(interval, 5 minutes)``; disabled sources and sources with a live lease are skipped. With ``intelligence.scan_on_start`` the first leader tick also starts an incremental
scan of every enabled source (trigger ``startup``) unless one ran in the last minute. Each tick also closes scans abandoned by a replica that died.

In the ``test`` environment the loop is not started (scans there run on demand, and tests call :meth:`DiscoveryScheduler.tick` themselves), so an application under test never
changes the graph in the background.
"""

from __future__ import annotations

import logging
import threading
from datetime import timedelta
from typing import Any

from olcf.core.errors import OlcfError
from olcf.core.timeutil import age_seconds, from_iso, now_iso, to_iso
from olcf.intelligence import settings as rt
from olcf.intelligence.discovery.base import safe_message
from olcf.pipelines.common import LeaderLoop
from olcf.security.rbac import SYSTEM_PRINCIPAL

log = logging.getLogger("olcf.intelligence.discovery.scheduler")

#: (incremental setting, incremental default, full setting, full default) per source kind
INTERVALS: dict[str, tuple[str, int, str, int]] = {
    "local": ("intelligence.local_scan_interval_seconds", 900, "intelligence.local_full_scan_interval_seconds", 86400),
    "catalog_mirror": ("intelligence.local_scan_interval_seconds", 900, "intelligence.local_full_scan_interval_seconds", 86400),
    "fabric": ("intelligence.fabric_incremental_interval_seconds", 3600, "intelligence.fabric_full_scan_interval_seconds", 86400),
}
RETRY_AFTER_FAILURE = 300.0
STARTUP_QUIET_SECONDS = 60.0
TICK_SECONDS = 15.0


class DiscoveryScheduler:
    """Decides what is due and starts it. ``tick()`` is idempotent and safe to call from tests."""

    def __init__(self, ctx: Any, store: Any, runner: Any, *, tick_seconds: float = TICK_SECONDS) -> None:
        self.ctx, self.store, self.runner = ctx, store, runner
        self.tick_seconds = tick_seconds
        self._startup_done = False
        self._started_at: float | None = None
        self.loop = LeaderLoop(ctx, "intelligence-discovery", self._run_tick, tick_seconds, log)
        self._lock = threading.Lock()
        #: last error of a tick (for the health detail)
        self.last_error: str | None = None
        self.last_tick_at: str | None = None

    # ---- lifecycle
    def start(self) -> None:
        """Start the leader loop (a no-op when Intelligence collection is disabled)."""
        if not self.ctx.settings.intelligence.enabled or self.ctx.settings.environment == "test":
            return
        self.loop.start()

    def stop(self) -> None:
        """Stop the loop (running scans are cancelled by the runner's shutdown)."""
        self.loop.stop()

    @property
    def running(self) -> bool:
        """True while the loop thread is alive."""
        t = self.loop._thread
        return bool(t and t.is_alive())

    # ---- schedule
    def intervals(self, source: dict[str, Any]) -> tuple[int, int]:
        """``(incremental_seconds, full_seconds)`` of a source (its own schedule, else the platform settings of its kind; 0 = never)."""
        kind = source["kind"]
        sched = source.get("schedule") or {}
        inc_key, inc_def, full_key, full_def = INTERVALS.get(kind, ("", 0, "", 0))
        inc = sched["incremental_seconds"] if "incremental_seconds" in sched else (int(rt.get(self.ctx, inc_key, inc_def)) if inc_key else 0)
        full = sched["full_seconds"] if "full_seconds" in sched else (int(rt.get(self.ctx, full_key, full_def)) if full_key else 0)
        return int(inc), int(full)

    def due(self, source: dict[str, Any]) -> str | None:
        """``"full"``, ``"incremental"`` or ``None`` for a source right now."""
        if not source["enabled"] or self.store.is_leased(source) or source["kind"] == "scanner_import":
            return None
        inc, full = self.intervals(source)
        last_any = source.get("last_scan_at")
        if source.get("last_status") == "failed" and last_any:
            age = age_seconds(last_any)
            wait = min(float(full or inc or RETRY_AFTER_FAILURE), RETRY_AFTER_FAILURE)
            if age is not None and age < wait:
                return None
        last_full = source.get("last_full_at")
        if not last_full:
            return "full" if (full or inc) and self.ctx.settings.intelligence.scan_on_start else None
        if full > 0 and (age_seconds(last_full) or 0.0) >= full:
            return "full"
        if inc > 0:
            marks = [m for m in (last_full, source.get("last_incremental_at")) if m]
            if (age_seconds(max(marks)) or 0.0) >= inc:
                return "incremental"
        return None

    def next_due(self, source: dict[str, Any]) -> str | None:
        """When the next scan of a source is expected (ISO time) - ``None`` for "only on demand" or a disabled source."""
        if not source["enabled"] or source["kind"] == "scanner_import":
            return None
        inc, full = self.intervals(source)
        cands: list[Any] = []
        last_full = source.get("last_full_at")
        if not last_full:
            return now_iso() if (full or inc) and self.ctx.settings.intelligence.scan_on_start else None
        if full > 0:
            cands.append(from_iso(last_full) + timedelta(seconds=full))
        if inc > 0:
            marks = [m for m in (last_full, source.get("last_incremental_at")) if m]
            cands.append(from_iso(max(marks)) + timedelta(seconds=inc))
        return to_iso(min(cands)) if cands else None

    def _run_tick(self) -> None:
        self.tick()

    # ---- the pass
    def tick(self) -> dict[str, Any]:
        """One pass: reap abandoned scans, start startup scans (once), start due scans. Returns what it started (for tests and the status report)."""
        started: list[dict[str, str]] = []
        with self._lock:
            self.last_tick_at = now_iso()
            try:
                reaped = self.runner.reap()
            except Exception as exc:  # noqa: BLE001
                reaped = 0
                self.last_error = f"reap: {safe_message(exc)}"
                log.exception("closing abandoned scans failed")
            on_start = (not self._startup_done) and bool(self.ctx.settings.intelligence.scan_on_start)
            self._startup_done = True
            for source in self.store.list(enabled=True):
                try:
                    mode = self.due(source)
                    trigger = "schedule"
                    if mode is None and on_start and source["kind"] != "scanner_import" and not self.store.is_leased(source):
                        recent = age_seconds(source.get("last_scan_at"))
                        if recent is None or recent > STARTUP_QUIET_SECONDS:
                            mode, trigger = "incremental", "startup"
                    if mode is None:
                        continue
                    row = self.runner.start(SYSTEM_PRINCIPAL, source["id"], mode=mode, trigger=trigger)
                    started.append({"source_id": source["id"], "scan_id": row["id"], "mode": row["mode"], "trigger": trigger})
                except OlcfError as exc:
                    log.info("scheduled scan of '%s' not started: %s", source["name"], exc.message)
                except Exception as exc:  # noqa: BLE001 - one bad source never stops the others
                    self.last_error = f"{source['name']}: {safe_message(exc)}"
                    log.exception("scheduled scan of '%s' failed to start", source["name"])
        return {"started": started, "reaped": reaped}
