# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Operations metadata schema (migration stream ``ops``).

``backups``          one row per backup run (file name, size, sha256, encryption mode, per-table manifest, off-host copies, validation and
                     restore-test results). The backup *file* lives under ``<data_dir>/backups`` and on the configured targets.
``backup_targets``   off-host destinations (local path / S3 or MinIO / SFTP / lakehouse). Credentials are secret-store references.
``update_records``   one row per update manifest seen (available -> staged -> applied / rolled_back / failed) with its signature verdict.
``retention_runs``   report of every retention/housekeeping run (dry-run or real).
``support_bundles``  metadata (never content) of generated support bundles including the leak-scan verdict.
``audit_anchors``    hash-chain anchors written when audit records are purged, so verification stays meaningful after retention.
"""

from __future__ import annotations

from sqlalchemy import JSON, BigInteger, Boolean, Column, Index, Integer, String, Table, Text

from olcf.db.migrate import Migration, create_tables, drop_tables, register
from olcf.db.schema import ID, TS, metadata

backups = Table(
    "backups", metadata,
    Column("id", ID, primary_key=True),
    Column("kind", String(24), nullable=False, default="manual"),  # manual | scheduled | pre-update | pre-restore
    Column("state", String(16), nullable=False, default="running"),  # running | succeeded | failed
    Column("filename", String(255)),
    Column("size_bytes", BigInteger),
    Column("sha256", String(64)),  # of the (encrypted) file
    Column("encryption", String(16)),  # passphrase | master
    Column("key_id", String(64)),  # master key id for ``master`` mode
    Column("includes", JSON, nullable=False, default=dict),
    Column("manifest", JSON, nullable=False, default=dict),
    Column("app_version", String(32)),
    Column("dialect", String(16)),
    Column("migrations", JSON, nullable=False, default=dict),
    Column("targets", JSON, nullable=False, default=list),  # [{name, uri, ok, error, at}]
    Column("error", JSON),
    Column("last_validation", JSON),
    Column("last_restore_test", JSON),
    Column("created_by", ID),
    Column("created_at", TS, nullable=False),
    Column("finished_at", TS),
    Index("ix_backups_created", "created_at"),
)

backup_targets = Table(
    "backup_targets", metadata,
    Column("id", ID, primary_key=True),
    Column("name", String(64), nullable=False, unique=True),
    Column("type", String(16), nullable=False),  # local | s3 | sftp | lakehouse
    Column("config", JSON, nullable=False, default=dict),
    Column("secret_id", ID),
    Column("enabled", Boolean, nullable=False, default=True),
    Column("retention", JSON, nullable=False, default=dict),  # {keep_last, keep_days}
    Column("last_test", JSON),
    Column("created_by", ID),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
)

update_records = Table(
    "update_records", metadata,
    Column("id", ID, primary_key=True),
    Column("version", String(32), nullable=False),
    Column("channel", String(16), nullable=False),
    Column("state", String(16), nullable=False),  # available | staged | applying | handoff | applied | rolled_back | failed
    Column("manifest", JSON, nullable=False),
    Column("verdict", JSON, nullable=False, default=dict),  # signature/trust/rollback verdict + key ids
    Column("source", String(255)),  # url or "offline-bundle:<name>"
    Column("staged_dir", String(512)),
    Column("preflight", JSON),
    Column("backup_id", ID),
    Column("result", JSON),
    Column("created_by", ID),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
    Index("ix_update_records_version", "version"),
)

retention_runs = Table(
    "retention_runs", metadata,
    Column("id", ID, primary_key=True),
    Column("dry_run", Boolean, nullable=False, default=False),
    Column("started_at", TS, nullable=False),
    Column("finished_at", TS),
    Column("report", JSON, nullable=False, default=dict),
    Column("error", Text),
)

support_bundles = Table(
    "support_bundles", metadata,
    Column("id", ID, primary_key=True),
    Column("filename", String(255), nullable=False),
    Column("size_bytes", BigInteger),
    Column("sha256", String(64)),
    Column("options", JSON, nullable=False, default=dict),
    Column("scan", JSON, nullable=False, default=dict),
    Column("created_by", ID),
    Column("created_at", TS, nullable=False),
)

audit_anchors = Table(
    "audit_anchors", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("anchored_at", TS, nullable=False),
    Column("last_purged_seq", BigInteger, nullable=False),
    Column("last_purged_hash", String(64)),
    Column("purged_count", Integer, nullable=False, default=0),
    Column("archive", String(512)),
    Column("archive_sha256", String(64)),
)

OPS_TABLES = [backups, backup_targets, update_records, retention_runs, support_bundles, audit_anchors]

register(Migration("ops", 1, "backup, updates, retention, support bundles", up=lambda c: create_tables(c, OPS_TABLES),
                   down=lambda c: drop_tables(c, reversed(OPS_TABLES))))
