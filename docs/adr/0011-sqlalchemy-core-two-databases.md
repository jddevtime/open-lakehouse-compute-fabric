# ADR 0011: SQLAlchemy Core, SQLite and PostgreSQL with one code path

Status: Accepted

## Context

A single user wants `pip install` and no database server. A team wants several replicas, backups and standard operations. Maintaining two persistence layers doubles the bugs; an ORM hides SQL that has to be correct on both.

## Decision

* All persistence goes through **SQLAlchemy Core** with bound parameters: tables are defined in each package's `schema.py` on one shared `metadata`; there is no ORM and no string-built SQL.
* Every statement must run on both **SQLite** and **PostgreSQL**. Where they differ (locking, `RETURNING`, JSON), code branches on `db.dialect`. The leader lease, trigger claims, nonces and idempotency keys are built from portable constructs: unique constraints and compare-and-set `UPDATE ... WHERE`.
* SQLite is hardened on connect (WAL mode, `synchronous=NORMAL`, foreign keys on, a busy timeout of 30 seconds). PostgreSQL uses psycopg 3, pre-ping and a pool; an empty database must have UTF-8 encoding.
* Migrations are versioned in **streams**, one per package, named after it, so packages evolve independently. `olcf db status`, `migrate` and `rollback` operate on the streams. An applied migration is never edited.
* SQLite supports one server process; PostgreSQL is required for `--workers N` and replicas.

## Consequences

* The test suite runs against SQLite by default; PostgreSQL behaviour is covered only by the tests that need a server (high availability and backup), which skip without `OLCF_TEST_POSTGRES_URL`. A dialect-specific bug can therefore pass the default run.
* No database-specific features (for example PostgreSQL row-level security or `LISTEN/NOTIFY`) can be relied on.
* Backups are logical and database-agnostic ([ADR 0013](0013-encrypted-logical-backup.md)).

## Alternatives considered

PostgreSQL only (rejected: install weight for single users); SQLite only (rejected: no HA); an ORM (rejected: opaque SQL on two dialects).

## Verified by

`src/olcf/db/engine.py`, `src/olcf/db/migrate.py`, `tests/ops/test_ha.py`, `tests/ops/test_backup_pg.py`; the [setup guide](../development/setup.md#database-changes).
