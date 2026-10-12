# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Database-lease based leader election for scheduler singletons (spec section 27)."""

from __future__ import annotations

import logging
import time

from sqlalchemy import insert, select, update
from sqlalchemy.exc import IntegrityError

from olcf.core.timeutil import iso_in, now_iso
from olcf.db import schema as S
from olcf.db.engine import Database

log = logging.getLogger("olcf.leader")


class LeaderElector:
    """Renewable lease. ``is_leader()`` is true while this holder owns an unexpired lease.

    Works on SQLite and PostgreSQL using only portable ``UPDATE ... WHERE`` compare-and-set.
    Services call :meth:`acquire` on each tick (every ``ttl/3`` seconds is recommended).

    **Fencing by clock**: ``is_leader()`` is also false once ``ttl`` seconds have passed since the last *successful* acquire/renewal was started, so a node that
    cannot reach the database (partition, long pause, DB outage) stops acting as leader at the same moment the lease expires for everybody else, instead of
    running singleton work next to the node that took over (SECOPS finding, tests/ops/test_ha.py).
    """

    def __init__(self, db: Database, name: str, holder: str, ttl: float = 15.0) -> None:
        self.db, self.name, self.holder, self.ttl = db, name, holder, ttl
        self._leader = False
        self._valid_until = 0.0
        self.term = 0

    def acquire(self) -> bool:
        started = time.monotonic()  # measured before the round trip: latency can only shorten our belief in the lease, never extend it
        now = now_iso()
        new_exp = iso_in(self.ttl)
        t = S.leader_leases.c
        with self.db.begin() as conn:
            res = conn.execute(
                update(S.leader_leases)
                .where(t.name == self.name, (t.holder == self.holder) | (t.expires_at < now))
                .values(holder=self.holder, expires_at=new_exp, updated_at=now, term=t.term + 1)
            )
            if res.rowcount == 1:
                row = conn.execute(select(t.term).where(t.name == self.name)).first()
                self.term = int(row[0]) if row else self.term
                self._leader = True
                self._valid_until = started + self.ttl
                return True
        try:
            with self.db.begin() as conn:
                conn.execute(insert(S.leader_leases).values(name=self.name, holder=self.holder, term=1, expires_at=new_exp, updated_at=now))
            self.term, self._leader = 1, True
            self._valid_until = started + self.ttl
            return True
        except IntegrityError:
            self._leader = False
            return False

    def is_leader(self) -> bool:
        return self._leader and time.monotonic() < self._valid_until

    def release(self) -> None:
        with self.db.begin() as conn:
            conn.execute(update(S.leader_leases).where(S.leader_leases.c.name == self.name, S.leader_leases.c.holder == self.holder)
                         .values(expires_at=now_iso()))
        self._leader = False
        self._valid_until = 0.0

    def current_holder(self) -> dict | None:
        with self.db.connect() as conn:
            row = conn.execute(select(S.leader_leases).where(S.leader_leases.c.name == self.name)).first()
        return dict(row._mapping) if row else None
