# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Database engine wrapper with SQLite hardening and PostgreSQL support."""

from __future__ import annotations

import threading
import zlib
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from sqlalchemy import Connection, Engine, create_engine, event, text
from sqlalchemy.engine import make_url
from sqlalchemy.pool import StaticPool


class Database:
    """Thin wrapper around a SQLAlchemy engine.

    ``with db.begin() as conn:`` yields a transactional :class:`Connection`; the transaction
    commits on success and rolls back on exception. All services use this so tests can swap the
    backing store (``sqlite://`` memory, file SQLite, PostgreSQL) without code changes.
    """

    def __init__(self, url: str, *, echo: bool = False, pool_size: int = 10) -> None:
        self.url = url
        parsed = make_url(url)
        self.dialect = parsed.get_backend_name()
        kwargs: dict[str, Any] = {"echo": echo, "future": True}
        if self.dialect == "sqlite":
            in_memory = parsed.database in (None, "", ":memory:")
            kwargs["connect_args"] = {"check_same_thread": False, "timeout": 30}
            if in_memory:
                kwargs["poolclass"] = StaticPool  # single shared connection for ``:memory:`` tests
        elif self.dialect == "postgresql":
            kwargs.update(pool_size=pool_size, max_overflow=pool_size, pool_pre_ping=True)
            if url.startswith("postgresql://"):
                # prefer the psycopg3 driver shipped in the ``postgres`` extra
                url = url.replace("postgresql://", "postgresql+psycopg://", 1)
        self.engine: Engine = create_engine(url, **kwargs)
        if self.dialect == "sqlite":
            self._install_sqlite_pragmas()
        self._migrated = threading.Event()
        self._serial_locks: dict[str, threading.Lock] = {}
        self._serial_guard = threading.Lock()

    # ------------------------------------------------------------------ helpers
    @property
    def is_sqlite(self) -> bool:
        return self.dialect == "sqlite"

    @property
    def is_postgres(self) -> bool:
        return self.dialect == "postgresql"

    def _install_sqlite_pragmas(self) -> None:
        @event.listens_for(self.engine, "connect")
        def _on_connect(dbapi_conn, _record):  # noqa: ANN001
            cur = dbapi_conn.cursor()
            cur.execute("PRAGMA journal_mode=WAL")
            cur.execute("PRAGMA synchronous=NORMAL")
            cur.execute("PRAGMA busy_timeout=30000")
            cur.execute("PRAGMA foreign_keys=ON")
            cur.close()

    @contextmanager
    def begin(self) -> Iterator[Connection]:
        with self.engine.begin() as conn:
            yield conn

    @contextmanager
    def begin_serialized(self, name: str) -> Iterator[Connection]:
        """A transaction that holds the cluster-wide mutual-exclusion lock ``name`` until it ends (read-check-write sequences, compare-and-swap).

        PostgreSQL: ``pg_advisory_xact_lock`` (released at commit / rollback; works across processes and nodes, and the statements that follow see every
        change committed by the previous holder because READ COMMITTED takes a new snapshot per statement).  SQLite: ``BEGIN IMMEDIATE`` takes the single
        write lock before the first read, so no other process can commit between the read and the write (a deferred transaction would read a snapshot and
        then fail with ``SQLITE_BUSY_SNAPSHOT`` or, worse, write on top of a stale read), plus an in-process lock because a shared in-memory connection
        cannot nest transactions.  Other dialects fall back to a plain transaction.
        """
        with self._serial_guard:
            lock = self._serial_locks.setdefault(name, threading.Lock())
        with lock, self.engine.begin() as conn:
            if self.is_postgres:
                conn.execute(text("SELECT pg_advisory_xact_lock(:k)"), {"k": zlib.crc32(name.encode("utf-8")) & 0x7FFFFFFF})
            elif self.is_sqlite:
                conn.exec_driver_sql("BEGIN IMMEDIATE")  # pysqlite (legacy transaction control) starts transactions lazily; this one is explicit
            yield conn

    @contextmanager
    def connect(self) -> Iterator[Connection]:
        with self.engine.connect() as conn:
            yield conn

    def ping(self) -> float:
        import time

        t0 = time.perf_counter()
        with self.connect() as conn:
            conn.execute(text("SELECT 1"))
        return (time.perf_counter() - t0) * 1000.0

    def dispose(self) -> None:
        self.engine.dispose()

    # ------------------------------------------------------------------ migrations
    def migrate(self) -> dict[str, int]:
        from olcf.db.migrate import migrate_all

        versions = migrate_all(self)
        self._migrated.set()
        return versions
