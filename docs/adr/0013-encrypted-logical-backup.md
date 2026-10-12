# ADR 0013: Backups are encrypted logical dumps in one container format

Status: Accepted

## Context

State must be restorable on SQLite and PostgreSQL, across minor schema versions, verifiable without a running database, taken while the platform serves requests, and safe to store off-host.

## Decision

* A **logical** backup: the engine walks the SQLAlchemy metadata (every table every package registered) and streams rows as JSON lines through gzip into an authenticated-encryption container. Memory use is constant: one batch of rows (2000) and one 1 MiB frame.
* All tables are read in one read-only snapshot transaction (PostgreSQL `REPEATABLE READ`, SQLite `BEGIN` in WAL mode), so the audit chain, secrets and referenced items are mutually consistent.
* Container `OLCFBAK1`: AES-256-GCM in 1 MiB frames; a frame's nonce is a random 4-byte prefix and a 64-bit counter, and its authenticated data covers the header hash, the counter and a final flag, so any flipped, reordered, dropped, duplicated or truncated frame, or a swapped header, fails authentication.
* Key derivation: **passphrase** mode uses scrypt (N=2^15, r=8, p=1) with a random salt; **master** mode derives the key by HKDF-SHA256 from the platform master key, so unattended scheduled backups are encrypted too.
* Not included, by design: in-flight OIDC logins and leader leases; lakehouse data (it follows its storage provider's strategy); the master key unless included together with an operator passphrase.
* Targets: a local directory, S3-compatible storage, SFTP, or a lakehouse of the platform itself.

## Consequences

* Restoring a `master`-mode backup needs the same master key: a disaster-recovery prerequisite that must be stored separately ([backup and restore](../guides/backup-and-restore.md)).
* A restore replays rows; it is not a block-level copy, so it is slower than a PostgreSQL base backup for very large databases. Use database-native backups too for those.
* The format is versioned by its magic (`OLCFBAK1`); a new version must read the old.

## Alternatives considered

Copying the SQLite file or `pg_dump` (rejected as the only mechanism: not portable across the two databases, and not encrypted).

## Verified by

`src/olcf/ops/backup_core.py`, `src/olcf/ops/crypto.py`, `tests/ops/test_backup_core.py`, `tests/ops/test_crypto.py`, `tests/ops/test_backup_targets.py`, `tests/ops/test_backup_pg.py`.
