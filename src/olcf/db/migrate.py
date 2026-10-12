# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Versioned, transactional schema migrations organised in *streams*.

Each package that owns tables registers a stream (``core``, ``compute``, ``fabric`` ...) so
packages evolve independently. Within a stream versions are applied in order; each migration is
applied in its own transaction and recorded in ``schema_migrations``. Migrations may provide a
``down`` callable so ``olcf db rollback`` can reverse them where feasible (spec section 27).

Packages register migrations at import time of their ``schema`` module::

    from olcf.db.migrate import Migration, register, create_tables
    register(Migration("compute", 1, "initial", up=lambda c: create_tables(c, [workers, ...]),
                       down=lambda c: drop_tables(c, [workers, ...])))
"""

from __future__ import annotations

import importlib
import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass

from sqlalchemy import Connection, Table, insert, select, text

from olcf.core.timeutil import now_iso
from olcf.db import schema as core_schema
from olcf.db.engine import Database

log = logging.getLogger("olcf.db.migrate")

#: Modules that may define a ``schema`` submodule with extra streams.
SCHEMA_MODULES = [
    "olcf.db.schema",
    "olcf.security.schema",
    "olcf.storage.schema",
    "olcf.catalog.schema",
    "olcf.intelligence.schema",
    "olcf.intelligence.discovery.schema",
    "olcf.intelligence.governance.schema",
    "olcf.sql.schema",
    "olcf.compute.schema",
    "olcf.notebooks.schema",
    "olcf.pipelines.schema",
    "olcf.fabric.schema",
    "olcf.observability.schema",
    "olcf.ops.schema",
    "olcf.mcp.schema",
]


@dataclass
class Migration:
    """One versioned schema migration of a stream: its number, description, the ``up`` step and an optional ``down`` step."""
    stream: str
    version: int
    description: str
    up: Callable[[Connection], None]
    down: Callable[[Connection], None] | None = None


_REGISTRY: dict[str, dict[int, Migration]] = {}


def register(m: Migration) -> None:
    _REGISTRY.setdefault(m.stream, {})[m.version] = m


def create_tables(conn: Connection, tables: Iterable[Table]) -> None:
    core_schema.metadata.create_all(conn, tables=list(tables), checkfirst=True)


def drop_tables(conn: Connection, tables: Iterable[Table]) -> None:
    core_schema.metadata.drop_all(conn, tables=list(tables), checkfirst=True)


def add_column(conn: Connection, table: str, column_ddl: str) -> None:
    conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column_ddl}"))  # noqa: S608 - internal DDL only


register(Migration("core", 1, "initial core schema", up=lambda c: create_tables(c, core_schema.CORE_TABLES),
                   down=lambda c: drop_tables(c, reversed(core_schema.CORE_TABLES))))

register(Migration("core", 2, "revoked access-token ids (logout / refresh-token reuse)", up=lambda c: create_tables(c, [core_schema.revoked_tokens]),
                   down=lambda c: drop_tables(c, [core_schema.revoked_tokens])))


def discover() -> None:
    for mod in SCHEMA_MODULES:
        try:
            importlib.import_module(mod)
        except ModuleNotFoundError as exc:
            if exc.name and (mod == exc.name or mod.startswith(exc.name + ".")):
                continue  # package has no schema module (yet)
            raise


def _ensure_table(db: Database) -> None:
    with db.begin() as conn:
        core_schema.metadata.create_all(conn, tables=[core_schema.schema_migrations], checkfirst=True)


def current_versions(db: Database) -> dict[str, int]:
    _ensure_table(db)
    with db.connect() as conn:
        rows = conn.execute(select(core_schema.schema_migrations.c.stream, core_schema.schema_migrations.c.version)).all()
    out: dict[str, int] = {}
    for stream, version in rows:
        out[stream] = max(out.get(stream, 0), version)
    return out


def migrate_all(db: Database) -> dict[str, int]:
    """Apply all pending migrations in every registered stream and return stream versions."""
    discover()
    _ensure_table(db)
    applied = current_versions(db)
    # core first, then alphabetical for determinism
    for stream in sorted(_REGISTRY, key=lambda s: (s != "core", s)):
        for version in sorted(_REGISTRY[stream]):
            if version <= applied.get(stream, 0):
                continue
            m = _REGISTRY[stream][version]
            log.info("applying migration %s v%s: %s", stream, version, m.description)
            with db.begin() as conn:
                if db.is_postgres:
                    conn.execute(text("SELECT pg_advisory_xact_lock(727274)"))
                    # another replica may have applied it while we waited for the lock
                    done = conn.execute(
                        select(core_schema.schema_migrations.c.version).where(
                            core_schema.schema_migrations.c.stream == stream,
                            core_schema.schema_migrations.c.version == version,
                        )
                    ).first()
                    if done:
                        continue
                m.up(conn)
                conn.execute(
                    insert(core_schema.schema_migrations).values(
                        stream=stream, version=version, description=m.description, applied_at=now_iso()
                    )
                )
            applied[stream] = version
    return current_versions(db)


def rollback(db: Database, stream: str, to_version: int) -> int:
    """Reverse migrations of ``stream`` above ``to_version`` (only where ``down`` exists)."""
    discover()
    cur = current_versions(db).get(stream, 0)
    for version in range(cur, to_version, -1):
        m = _REGISTRY.get(stream, {}).get(version)
        if m is None or m.down is None:
            raise RuntimeError(f"migration {stream} v{version} is not reversible")
        with db.begin() as conn:
            m.down(conn)
            conn.execute(
                core_schema.schema_migrations.delete().where(
                    core_schema.schema_migrations.c.stream == stream,
                    core_schema.schema_migrations.c.version == version,
                )
            )
        cur = version - 1
    return cur
