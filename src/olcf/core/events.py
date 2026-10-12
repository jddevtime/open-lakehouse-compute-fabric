# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Persistent event bus.

Events are the integration backbone: job/pipeline/item/worker/security events are persisted in the
``events`` table (so any API replica can serve ``GET /v1/events`` and the SSE stream), fanned out to
in-process subscribers immediately, and delivered to webhooks by the orchestration module.

In-process subscribers are called on a **delivery lane of their own** (one thread, started on demand, per subscriber; order is kept per subscriber). A publisher waits for the
subscribers of its event for at most :data:`WAIT_BUDGET` seconds in total, so a subscriber that is slow, hung or raising can never hold a publisher (a job state change, a SQL
statement, a pipeline run) for longer than that - and a subscriber whose callback has already been running that long is not waited for at all until it returns. Fast subscribers
therefore still look synchronous (the event is delivered when ``publish`` returns); events for a stuck subscriber queue up (bounded; the oldest is dropped and counted) and are
delivered when it recovers. ``publish`` called from inside a subscriber never waits.

Persisting is bounded as well. On SQLite there is a single writer, so the insert can wait behind another module's transaction (a graph scan, a long batch); a publisher waits at most
:data:`PERSIST_BUDGET` seconds for the write lock. If the lock is still taken the event is handed to a background persister (in order, bounded to :data:`LANE_CAPACITY`; the oldest is
dropped and counted if it cannot keep up), subscribers get the event at once - without a ``seq``, which the persister fills in - and ``publish`` returns. PostgreSQL has no
single-writer lock and persists inline. While events are waiting, later events queue behind them so the stored order stays the publish order.

Event type naming: ``<domain>.<noun>.<verb>`` e.g. ``job.state_changed``, ``worker.online``,
``fabric.submission.received``, ``pipeline.run.failed``, ``item.created``.
"""

from __future__ import annotations

import contextvars
import logging
import threading
import time
from collections import deque
from collections.abc import Callable
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.exc import OperationalError

from olcf.core.ids import new_id
from olcf.core.redaction import redact_obj
from olcf.core.timeutil import iso_in, now_iso
from olcf.db import schema as S
from olcf.db.engine import Database
from olcf.db.util import fetch_all

log = logging.getLogger("olcf.events")

Subscriber = Callable[[dict[str, Any]], None]

#: Seconds a publisher waits, in total, for the in-process subscribers of one event (see the module docstring).
WAIT_BUDGET = 0.5
#: Seconds a publisher waits for the SQLite write lock when persisting an event before the event is handed to the background persister.
PERSIST_BUDGET = 0.25
#: Events a stuck subscriber may have queued before the oldest ones are dropped.
LANE_CAPACITY = 10_000
_in_lane = threading.local()


class _Lane:
    """The delivery lane of one subscriber: a queue drained in order by a thread that exists only while there is something to deliver."""

    def __init__(self, callback: Subscriber) -> None:
        self.callback = callback
        self._queue: deque[tuple[dict[str, Any], contextvars.Context, threading.Event]] = deque()
        self._lock = threading.Lock()
        self._running = False
        self._busy_since: float | None = None
        self.dropped = 0

    @property
    def stalled(self) -> bool:
        """True while the current callback has been running for longer than a publisher is willing to wait."""
        since = self._busy_since
        return since is not None and time.monotonic() - since >= WAIT_BUDGET

    def put(self, evt: dict[str, Any]) -> threading.Event:
        done = threading.Event()
        with self._lock:
            if len(self._queue) >= LANE_CAPACITY:
                _, _, old = self._queue.popleft()
                old.set()
                self.dropped += 1
                if self.dropped == 1 or self.dropped % 1000 == 0:
                    log.warning("event subscriber %r is not keeping up: %d events dropped", getattr(self.callback, "__qualname__", self.callback), self.dropped)
            self._queue.append((evt, contextvars.copy_context(), done))
            if not self._running:
                self._running = True
                threading.Thread(target=self._drain, name="olcf-event-lane", daemon=True).start()
        return done

    def _drain(self) -> None:
        _in_lane.active = True
        while True:
            with self._lock:
                if not self._queue:
                    self._running, self._busy_since = False, None
                    return
                evt, ctx, done = self._queue.popleft()
                self._busy_since = time.monotonic()
            try:
                ctx.run(self.callback, evt)
            except Exception:  # noqa: BLE001
                log.exception("event subscriber failed")
            finally:
                done.set()


class EventBus:
    """Publishes platform events: each event is persisted to the database (unless ``persist`` is false) and delivered to in-process subscribers such as the live event stream.
    """
    def __init__(self, db: Database) -> None:
        self.db = db
        self._subs: dict[int, _Lane] = {}
        self._next = 0
        self._lock = threading.Lock()
        self._deferred: deque[dict[str, Any]] = deque()     # events whose insert could not get the SQLite write lock in time
        self._plock = threading.Lock()
        self._persisting = False
        self.deferred_total = 0
        self.dropped = 0

    def publish(self, type_: str, payload: dict[str, Any] | None = None, *, workspace_id: str | None = None,
                subject: str | None = None, persist: bool = True) -> dict[str, Any]:
        evt: dict[str, Any] = {
            "id": new_id("evt"), "ts": now_iso(), "type": type_, "workspace_id": workspace_id, "subject": subject,
            "payload": redact_obj(payload or {}),
        }
        if persist:
            self._persist(evt)
        with self._lock:
            subs = list(self._subs.values())
        tickets = [(lane, lane.put(evt)) for lane in subs]
        if tickets and not getattr(_in_lane, "active", False):
            deadline = time.monotonic() + WAIT_BUDGET
            for lane, done in tickets:
                if not lane.stalled:
                    done.wait(max(0.0, deadline - time.monotonic()))
        return evt

    def _insert(self, evt: dict[str, Any], budget: float | None) -> None:
        """Insert one event. With a ``budget`` on SQLite the write lock is waited for at most that long (``OperationalError`` otherwise)."""
        with self.db.begin() as conn:
            bounded = budget is not None and self.db.is_sqlite
            previous = None
            if bounded:
                previous = conn.exec_driver_sql("PRAGMA busy_timeout").scalar()
                conn.exec_driver_sql(f"PRAGMA busy_timeout={max(1, int(budget * 1000))}")        # type: ignore[operator]
            try:
                res = conn.execute(S.events.insert().values(**evt))
            finally:
                if bounded and previous is not None:
                    conn.exec_driver_sql(f"PRAGMA busy_timeout={int(previous)}")                 # the connection goes back to the pool: restore its normal wait
            pk = res.inserted_primary_key
            evt["seq"] = int(pk[0]) if pk else None

    def _persist(self, evt: dict[str, Any]) -> None:
        with self._plock:
            behind = bool(self._deferred) or self._persisting
        if not behind:
            try:
                self._insert(evt, PERSIST_BUDGET)
                return
            except OperationalError as exc:
                if not self.db.is_sqlite or not ("locked" in str(exc).lower() or "busy" in str(exc).lower()):
                    log.exception("failed to persist event %s", evt["type"])
                    return
            except Exception:  # noqa: BLE001 - never break the caller because the event store hiccuped
                log.exception("failed to persist event %s", evt["type"])
                return
        self._defer(evt)

    def _defer(self, evt: dict[str, Any]) -> None:
        with self._plock:
            if len(self._deferred) >= LANE_CAPACITY:
                self._deferred.popleft()
                self.dropped += 1
                if self.dropped == 1 or self.dropped % 1000 == 0:
                    log.warning("the event store is busy and cannot keep up: %d events were not persisted", self.dropped)
            self._deferred.append(evt)
            self.deferred_total += 1
            if not self._persisting:
                self._persisting = True
                threading.Thread(target=self._drain_deferred, name="olcf-event-persist", daemon=True).start()

    def _drain_deferred(self) -> None:
        while True:
            with self._plock:
                if not self._deferred:
                    self._persisting = False
                    return
                evt = self._deferred[0]
            try:
                self._insert(evt, None)
            except Exception:  # noqa: BLE001 - the full SQLite wait has passed: give up on this event, keep the rest
                log.exception("failed to persist deferred event %s", evt.get("type"))
            with self._plock:
                if self._deferred and self._deferred[0] is evt:
                    self._deferred.popleft()

    def flush_deferred(self, timeout: float = 30.0) -> bool:
        """Wait until the events handed to the background persister are stored; ``True`` when none is left."""
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            with self._plock:
                if not self._deferred and not self._persisting:
                    return True
            time.sleep(0.01)
        return False

    def subscribe(self, callback: Subscriber) -> Callable[[], None]:
        with self._lock:
            self._next += 1
            key = self._next
            self._subs[key] = _Lane(callback)

        def unsubscribe() -> None:
            with self._lock:
                self._subs.pop(key, None)

        return unsubscribe

    def list(self, *, after_seq: int = 0, limit: int = 100, type_prefix: str | None = None, workspace_ids: set[str] | None = None,
             subject: str | None = None, descending: bool = False) -> list[dict[str, Any]]:
        t = S.events.c
        stmt = select(S.events).limit(max(1, min(limit, 1000)))
        stmt = stmt.order_by(t.seq.desc() if descending else t.seq.asc())
        if after_seq:
            stmt = stmt.where(t.seq > after_seq)
        if type_prefix:
            stmt = stmt.where(t.type.like(type_prefix.replace("*", "%") if "*" in type_prefix else type_prefix + "%"))
        if subject:
            stmt = stmt.where(t.subject == subject)
        if workspace_ids is not None:
            stmt = stmt.where(t.workspace_id.in_(list(workspace_ids)))
        with self.db.connect() as conn:
            return fetch_all(conn, stmt)

    def head_seq(self) -> int:
        with self.db.connect() as conn:
            row = conn.execute(select(S.events.c.seq).order_by(S.events.c.seq.desc()).limit(1)).first()
        return int(row[0]) if row else 0

    def purge(self, older_than_days: int) -> int:
        cutoff = iso_in(-older_than_days * 86400)
        with self.db.begin() as conn:
            return conn.execute(delete(S.events).where(S.events.c.ts < cutoff)).rowcount
