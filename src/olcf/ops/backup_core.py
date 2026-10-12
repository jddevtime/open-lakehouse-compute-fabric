# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Database-agnostic logical backup and restore engine (spec 30).

Why logical: the control-plane state must be restorable on SQLite *and* PostgreSQL, across minor schema versions, and verifiable without
a running database server. The engine therefore walks the SQLAlchemy ``MetaData`` (every table every package registered) and streams rows
as JSON lines through ``gzip`` into the authenticated-encryption container of :mod:`olcf.ops.crypto`. Nothing is ever held in memory beyond
one batch (default 2000 rows) and one 1 MiB encryption frame, so the size of the database is irrelevant for memory use.

Payload (decrypted, uncompressed) = JSON lines::

    {"t":"meta", ...}                                   once, first
    {"t":"table","name":..,"columns":[..],"binary":[..],"pk":[..]}   before each table's rows
    {"t":"row","tb":..,"d":{col: value}}                one per row (LargeBinary columns are base64 strings)
    {"t":"file","path":..,"seq":..,"last":..,"data":b64}            config / key material
    {"t":"end","manifest":{"tables":{name:{rows,sha256}},"files":{path:{size,sha256}},"payload_sha256":..}}

Consistency: all tables are read inside ONE read-only snapshot transaction (PostgreSQL ``REPEATABLE READ``; SQLite ``BEGIN`` in WAL mode), so
the audit hash chain, the secrets table and the items they reference are mutually consistent even while the platform keeps serving requests.

Not included by design
----------------------
* ``EPHEMERAL_TABLES``: in-flight OIDC logins and leader leases (restoring stale handshakes/leases would be wrong and PKCE material has no
  business in backups).
* Secret *values*: the ``secrets`` table is backed up as it is stored - AES-GCM ciphertext rows plus the id of the master key that wraps them.
  Vault-backed secrets are only references. Restoring therefore needs the **master key** (``secrets/master.key`` or ``OLCF_MASTER_KEY``) in the
  target environment; it can be included in the backup (``include_master_key``) only together with an operator passphrase.
* Lakehouse/object data: follows the storage provider's own strategy (spec 30).
"""

from __future__ import annotations

import base64
import gzip
import hashlib
import io
import json
import logging
import os
import stat
from collections.abc import Callable, Collection, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from sqlalchemy import Connection, LargeBinary, Table, inspect, select, text

from olcf.core.errors import BadRequest, Conflict, Unsupported
from olcf.core.timeutil import now_iso
from olcf.db import migrate as dbm
from olcf.db import schema as core_schema
from olcf.db.engine import Database
from olcf.ops.crypto import BackupCorrupt, EncryptedReader, EncryptedWriter, KeySpec
from olcf.version import __version__

log = logging.getLogger("olcf.ops.backup")

#: runtime/handshake state that must not be backed up or restored
EPHEMERAL_TABLES = {"oidc_auth_requests", "oidc_login_codes", "leader_leases"}
#: never wiped/loaded by an *online* restore (the running process owns them while it restores)
ONLINE_RESTORE_KEEP = {"operations", "leader_leases", "schema_migrations"}
BATCH = 2000
FILE_CHUNK = 192 * 1024
Progress = Callable[[int, str], None]


# ----------------------------------------------------------------------------- helpers
def backup_tables(db: Database | None = None) -> list[Table]:
    """Every table of every registered schema (FK-safe order), minus the ephemeral ones."""
    dbm.discover()
    return [t for t in core_schema.metadata.sorted_tables if t.name not in EPHEMERAL_TABLES]


def _pk_cols(t: Table) -> list[Any]:
    cols = list(t.primary_key.columns)
    return cols or list(t.columns)[:1]


def _jline(obj: Any) -> bytes:
    return json.dumps(obj, separators=(",", ":"), sort_keys=True, default=_json_default, allow_nan=False).encode() + b"\n"


def _json_default(v: Any) -> Any:
    from datetime import date, datetime
    from decimal import Decimal

    if isinstance(v, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(v)).decode()
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return str(v)
    raise TypeError(f"cannot serialise {type(v).__name__}")


def _encode_row(t: Table, row: Any) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for c in t.columns:
        v = row._mapping[c.name]
        if v is not None and isinstance(c.type, LargeBinary):
            v = base64.b64encode(bytes(v)).decode()
        out[c.name] = v
    return out


def _decode_row(t: Table, d: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in d.items():
        c = t.columns[k]
        if v is not None and isinstance(c.type, LargeBinary):
            v = base64.b64decode(v)
        out[k] = v
    return out


@contextmanager
def snapshot(db: Database) -> Iterator[Connection]:
    """One consistent read-only snapshot of the whole database."""
    if db.is_postgres:
        conn = db.engine.connect().execution_options(isolation_level="REPEATABLE READ")
        try:
            conn.execute(text("SET TRANSACTION READ ONLY"))
            yield conn
        finally:
            conn.rollback()
            conn.close()
    else:
        conn = db.engine.connect()
        try:
            conn.exec_driver_sql("BEGIN")  # pysqlite does not open a read transaction by itself; WAL gives us snapshot isolation from here on
            yield conn
        finally:
            try:
                conn.exec_driver_sql("ROLLBACK")
            except Exception:  # noqa: BLE001
                pass
            conn.close()


# ----------------------------------------------------------------------------- write
@dataclass
class FileSpec:
    """A file to include: ``path`` inside the archive and either a source file or in-memory bytes."""

    path: str
    source: Path | None = None
    data: bytes | None = None


@dataclass
class BackupResult:
    """Result of writing a backup: manifest, header, row count and size."""
    manifest: dict[str, Any]
    header: dict[str, Any]
    rows: int
    size_bytes: int
    sha256: str
    migrations: dict[str, int]
    tables: list[str] = field(default_factory=list)


class _HashingGzip:
    """Feeds every payload byte into a SHA-256 before it is compressed."""

    def __init__(self, gz: gzip.GzipFile) -> None:
        self.gz, self.h = gz, hashlib.sha256()

    def write(self, b: bytes) -> None:
        self.h.update(b)
        self.gz.write(b)


def write_backup(db: Database, path: Path, keyspec: KeySpec, *, meta: dict[str, Any], files: list[FileSpec] | None = None,
                 progress: Progress | None = None, batch: int = BATCH) -> BackupResult:
    """Stream a complete logical backup of ``db`` into ``path`` (written to ``path.partial`` and renamed when complete and fsynced)."""
    tables = [t for t in backup_tables(db)]
    partial = path.with_name(path.name + ".partial")
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(partial, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    manifest: dict[str, Any] = {"tables": {}, "files": {}}
    total_rows = 0
    versions: dict[str, int] = {}
    try:
        with os.fdopen(fd, "wb") as fh:
            writer = EncryptedWriter(fh, keyspec, {"backup_id": meta.get("backup_id"), "created_at": now_iso(), "app_version": __version__})
            gz = gzip.GzipFile(fileobj=writer, mode="wb", compresslevel=6, mtime=0)
            try:
                out = _HashingGzip(gz)
                with snapshot(db) as conn:
                    present = set(inspect(conn).get_table_names())
                    versions = _versions(conn)
                    tables = [t for t in tables if t.name in present]
                    out.write(_jline({"t": "meta", "format": 1, "app_version": __version__, "dialect": db.dialect, "created_at": now_iso(),
                                      "migrations": versions, "tables": [t.name for t in tables], **meta}))
                    for i, t in enumerate(tables):
                        n, h = _dump_table(conn, t, out, batch)
                        manifest["tables"][t.name] = {"rows": n, "sha256": h}
                        total_rows += n
                        if progress:
                            progress(int(5 + 85 * (i + 1) / len(tables)), f"dumped {t.name} ({n} rows)")
                for spec in files or []:
                    manifest["files"][spec.path] = _dump_file(spec, out)
                manifest["payload_sha256"] = out.h.hexdigest()
                manifest["total_rows"] = total_rows
                out.write(_jline({"t": "end", "manifest": manifest}))
                gz.close()
            except BaseException:
                writer.discard()
                raise
            finally:
                writer.close()
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(partial, path)
    except BaseException:
        partial.unlink(missing_ok=True)
        raise
    from olcf.ops.crypto import read_header, sha256_file

    return BackupResult(manifest=manifest, header=read_header(path), rows=total_rows, size_bytes=path.stat().st_size, sha256=sha256_file(path),
                        migrations=versions, tables=[t.name for t in tables])


def _versions(conn: Connection) -> dict[str, int]:
    out: dict[str, int] = {}
    for stream, version in conn.execute(select(core_schema.schema_migrations.c.stream, core_schema.schema_migrations.c.version)).all():
        out[stream] = max(out.get(stream, 0), int(version))
    return out


def _dump_table(conn: Connection, t: Table, out: _HashingGzip, batch: int) -> tuple[int, str]:
    out.write(_jline({"t": "table", "name": t.name, "columns": [c.name for c in t.columns],
                      "binary": [c.name for c in t.columns if isinstance(c.type, LargeBinary)], "pk": [c.name for c in _pk_cols(t)]}))
    h = hashlib.sha256()
    n = 0
    stmt = select(t).order_by(*_pk_cols(t))
    result = conn.execution_options(stream_results=True, yield_per=batch).execute(stmt)
    try:
        for row in result:
            d = _encode_row(t, row)
            line = _jline({"t": "row", "tb": t.name, "d": d})
            h.update(line)
            out.write(line)
            n += 1
    finally:
        result.close()
    return n, h.hexdigest()


def _file_pieces(spec: FileSpec) -> Iterator[bytes]:
    """The file as pieces of at most ``FILE_CHUNK`` bytes (at least one, possibly empty)."""
    if spec.data is not None:
        data = spec.data
        for i in range(0, max(len(data), 1), FILE_CHUNK):
            yield data[i:i + FILE_CHUNK]
        return
    assert spec.source is not None
    with open(spec.source, "rb") as fh:
        first = True
        while True:
            c = fh.read(FILE_CHUNK)
            if not c:
                if first:
                    yield b""
                return
            first = False
            yield c


def _dump_file(spec: FileSpec, out: _HashingGzip) -> dict[str, Any]:
    h = hashlib.sha256()
    size = 0
    seq = 0
    it = _file_pieces(spec)
    cur = next(it)
    while True:
        nxt = next(it, None)
        h.update(cur)
        size += len(cur)
        out.write(_jline({"t": "file", "path": spec.path, "seq": seq, "last": nxt is None, "data": base64.b64encode(cur).decode()}))
        seq += 1
        if nxt is None:
            break
        cur = nxt
    return {"size": size, "sha256": h.hexdigest()}


# ----------------------------------------------------------------------------- read
def iter_records(path: Path, keyspec: KeySpec) -> Iterator[dict[str, Any]]:
    """Decrypt + decompress and yield every JSON record. Authentication errors surface as :class:`BackupCorrupt`."""
    with open(path, "rb") as fh:
        reader = EncryptedReader(fh, keyspec)
        gz = gzip.GzipFile(fileobj=io.BufferedReader(reader, buffer_size=1024 * 1024), mode="rb")
        try:
            for line in gz:
                if line.strip():
                    yield json.loads(line)
        except (EOFError, gzip.BadGzipFile, OSError) as exc:
            raise BackupCorrupt(f"backup payload is damaged: {exc}") from exc
        except ValueError as exc:
            raise BackupCorrupt("backup payload contains invalid JSON") from exc


@dataclass
class ValidationReport:
    """Result of validating a backup file: whether it is sound, the errors found and its metadata and manifest."""
    ok: bool
    errors: list[str]
    meta: dict[str, Any]
    manifest: dict[str, Any]
    rows: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {"ok": self.ok, "errors": self.errors, "rows": self.rows, "app_version": self.meta.get("app_version"), "dialect": self.meta.get("dialect"),
                "migrations": self.meta.get("migrations"), "tables": len(self.manifest.get("tables", {})), "files": sorted(self.manifest.get("files", {})),
                "at": now_iso()}


def compat_errors(backup_versions: dict[str, int], dbm_registry: dict[str, dict[int, Any]] | None = None) -> list[str]:
    """Schema compatibility of a backup with *this* code: every stream's backup version must be <= the newest migration this code knows."""
    dbm.discover()
    reg = dbm_registry if dbm_registry is not None else dbm._REGISTRY  # noqa: SLF001
    errs = []
    for stream, ver in sorted(backup_versions.items()):
        known = max(reg.get(stream, {0: None}))
        if stream not in reg:
            errs.append(f"stream '{stream}' (v{ver}) is unknown to this version of OLCF (module missing?)")
        elif ver > known:
            errs.append(f"stream '{stream}' is at v{ver} in the backup but this version only knows v{known} (restore with a newer OLCF)")
    return errs


def validate_backup(path: Path, keyspec: KeySpec, *, progress: Progress | None = None) -> ValidationReport:
    """Decrypt and verify everything (frame authentication, per-table row counts and SHA-256, file hashes, payload hash) and check schema
    compatibility, **without applying anything**."""
    errors: list[str] = []
    meta: dict[str, Any] = {}
    manifest: dict[str, Any] = {}
    counts: dict[str, int] = {}
    hashes: dict[str, Any] = {}
    cur: str | None = None
    files: dict[str, dict[str, Any]] = {}
    payload = hashlib.sha256()
    seen_end = False
    rows = 0
    try:
        for rec in iter_records(path, keyspec):
            t = rec.get("t")
            if t == "meta":
                meta = rec
            elif t == "table":
                cur = rec["name"]
                counts[cur] = 0
                hashes[cur] = hashlib.sha256()
            elif t == "row":
                counts[rec["tb"]] = counts.get(rec["tb"], 0) + 1
                hashes[rec["tb"]].update(_jline(rec))
                rows += 1
            elif t == "file":
                f = files.setdefault(rec["path"], {"size": 0, "h": hashlib.sha256()})
                data = base64.b64decode(rec["data"])
                f["size"] += len(data)
                f["h"].update(data)
            elif t == "end":
                manifest = rec["manifest"]
                seen_end = True
            if t != "end":
                payload.update(_jline(rec))
    except BackupCorrupt as exc:
        return ValidationReport(False, [exc.message], meta, manifest, rows)
    if not seen_end:
        errors.append("the backup has no end record (incomplete)")
    else:
        for name, info in manifest.get("tables", {}).items():
            if counts.get(name) != info["rows"]:
                errors.append(f"table {name}: {counts.get(name)} rows in the stream, manifest says {info['rows']}")
            elif name in hashes and hashes[name].hexdigest() != info["sha256"]:
                errors.append(f"table {name}: checksum mismatch")
        for p, info in manifest.get("files", {}).items():
            got = files.get(p)
            if not got or got["size"] != info["size"] or got["h"].hexdigest() != info["sha256"]:
                errors.append(f"file {p}: checksum mismatch")
        if payload.hexdigest() != manifest.get("payload_sha256"):
            errors.append("payload checksum mismatch")
    errors.extend(compat_errors(meta.get("migrations") or {}))
    return ValidationReport(not errors, errors, meta, manifest, rows)


# ----------------------------------------------------------------------------- restore
def migrate_streams(db: Database, versions: dict[str, int]) -> dict[str, int]:
    """Create the schema at the versions recorded in a backup (streams absent from ``versions`` are created later by the normal migrate).

    Restoring data into a schema of the *same* version as the backup and then upgrading with the regular migrations is the only way to be
    sure that old rows meet the table shapes they were written for.
    """
    dbm.discover()
    dbm._ensure_table(db)  # noqa: SLF001
    applied = dbm.current_versions(db)
    for stream in sorted(dbm._REGISTRY, key=lambda s: (s != "core", s)):  # noqa: SLF001
        cap = versions.get(stream, 0)
        for version in sorted(dbm._REGISTRY[stream]):  # noqa: SLF001
            if version > cap or version <= applied.get(stream, 0):
                continue
            m = dbm._REGISTRY[stream][version]  # noqa: SLF001
            with db.begin() as conn:
                if db.is_postgres:
                    conn.execute(text("SELECT pg_advisory_xact_lock(727274)"))
                m.up(conn)
                conn.execute(core_schema.schema_migrations.insert().values(stream=stream, version=version, description=m.description, applied_at=now_iso()))
            applied[stream] = version
    return dbm.current_versions(db)


def drop_everything(db: Database) -> None:
    """Drop all tables found in the database (reflected, so unknown tables are included)."""
    from sqlalchemy import MetaData

    md = MetaData()
    md.reflect(bind=db.engine)
    md.drop_all(bind=db.engine)


def database_is_clean(db: Database) -> bool:
    """True when the control-plane tables hold no user data (only the bootstrap admin may exist)."""
    with db.connect() as conn:
        present = set(inspect(conn).get_table_names())
        for name in ("workspaces", "items", "connections", "secrets"):
            if name in present and conn.execute(text(f"SELECT 1 FROM {name} LIMIT 1")).first():  # noqa: S608 - fixed table names
                return False
    return True


@dataclass
class RestoreReport:
    """Result of a restore: success, rows restored per table and files written and skipped."""
    ok: bool
    tables: dict[str, int] = field(default_factory=dict)
    files_written: list[str] = field(default_factory=list)
    files_skipped: list[str] = field(default_factory=list)
    migrations_before: dict[str, int] = field(default_factory=dict)
    migrations_after: dict[str, int] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    #: what happened to the jobs that were queued/running at backup time (:func:`olcf.ops.restore_reconcile.reconcile_restored_jobs`); ``None`` when not reconciled
    jobs: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        out = {"ok": self.ok, "tables": self.tables, "rows": sum(self.tables.values()), "files_written": self.files_written, "files_skipped": self.files_skipped,
               "migrations_before": self.migrations_before, "migrations_after": self.migrations_after, "warnings": self.warnings}
        if self.jobs is not None:
            out["jobs"] = self.jobs
        return out


def _safe_rel(p: str) -> str:
    q = Path(p)
    if q.is_absolute() or ".." in q.parts or not p or p.startswith(("/", "\\")):
        raise BackupCorrupt(f"unsafe file path in backup: {p!r}")
    return p


def restore_backup(path: Path, keyspec: KeySpec, db: Database, *, online: bool = False, wipe: bool = True, files_dir: Path | None = None,
                   overwrite_files: bool = False, file_prefixes: tuple[str, ...] = ("config/", "keys/"), run_migrations: bool = True,
                   fresh_schema: bool = False, progress: Progress | None = None, batch: int = BATCH, reconcile_jobs: bool = False, live_workers: Collection[str] = (),
                   backup_id: str | None = None) -> RestoreReport:
    """Restore ``path`` into ``db``.

    The backup is **validated completely first** (nothing is touched if it is damaged or incompatible). ``wipe`` empties every control-plane
    table before loading; without it the tables must be empty. ``online`` keeps the tables the running process owns (``ONLINE_RESTORE_KEEP``).
    ``fresh_schema`` first drops **every** table of the database (also tables this code does not know, e.g. from a newer release - used by the
    update rollback) and recreates the schema at the versions recorded in the backup. Files (config, keys) are written under ``files_dir`` only when given, never over existing files unless ``overwrite_files`` (key files get 0600).

    ``reconcile_jobs`` (restore into another environment: the CLI restore, the restore-test, the online restore) fails the jobs that were queued or running at backup time and have
    no live worker attempt (``live_workers`` = the workers connected right now), see :mod:`olcf.ops.restore_reconcile`; the outcome is ``RestoreReport.jobs``. An update rollback
    leaves it off: its agents keep their processes and reconcile themselves when they reconnect.
    """
    report = RestoreReport(ok=False)
    val = validate_backup(path, keyspec)
    if not val.ok:
        raise BackupCorrupt("refusing to restore an invalid backup: " + "; ".join(val.errors[:5]), details={"errors": val.errors})
    versions = val.meta.get("migrations") or {}
    skip = set(ONLINE_RESTORE_KEEP) if online else set()
    if fresh_schema:
        if online:
            raise BadRequest("fresh_schema cannot be combined with an online restore")
        drop_everything(db)
    report.migrations_before = migrate_streams(db, versions) if not online else dbm.current_versions(db)
    tables = {t.name: t for t in backup_tables(db)}
    order = [t for t in core_schema.metadata.sorted_tables if t.name in tables and t.name not in skip]
    with db.begin() as conn:
        if db.is_postgres:
            conn.execute(text("SELECT pg_advisory_xact_lock(727276)"))  # serialise concurrent restores
        if wipe:
            for t in reversed(order):
                conn.execute(t.delete())
        else:
            for t in order:
                if conn.execute(select(t).limit(1)).first():
                    raise Conflict(f"table {t.name} is not empty; restore needs a clean database (or an explicit wipe)")
        cur: Table | None = None
        cols: set[str] = set()
        buf: list[dict[str, Any]] = []
        files: dict[str, list[bytes]] = {}

        def flush() -> None:
            nonlocal buf
            if buf and cur is not None:
                conn.execute(cur.insert(), buf)
                buf = []

        for rec in iter_records(path, keyspec):
            t = rec.get("t")
            if t == "table":
                flush()
                name = rec["name"]
                if name in skip:
                    cur = None
                    continue
                if name not in tables:
                    raise Unsupported(f"the backup contains table '{name}' which this version of OLCF does not know")
                cur = tables[name]
                cols = {c.name for c in cur.columns}
                extra = set(rec["columns"]) - cols
                if extra:
                    raise Unsupported(f"table {name} has columns {sorted(extra)} in the backup that the current schema lacks")
                report.tables[name] = 0
            elif t == "row" and cur is not None and rec["tb"] == cur.name:
                buf.append(_decode_row(cur, rec["d"]))
                report.tables[cur.name] += 1
                if len(buf) >= batch:
                    flush()
            elif t == "file":
                files.setdefault(rec["path"], []).append(base64.b64decode(rec["data"]))
        flush()
        if db.is_postgres:
            _fix_sequences(conn, [tables[n] for n in report.tables])
    if files_dir is not None:
        _write_files(files, files_dir, overwrite_files, file_prefixes, report)
    elif files:
        report.files_skipped = sorted(files)
    if run_migrations:
        report.migrations_after = db.migrate() if not online else dbm.current_versions(db)
    else:
        report.migrations_after = dbm.current_versions(db)
    if reconcile_jobs:
        from olcf.ops.restore_reconcile import reconcile_restored_jobs

        report.jobs = reconcile_restored_jobs(db, live_workers=live_workers, backup_id=backup_id)
    report.ok = True
    return report


def _fix_sequences(conn: Connection, tables: list[Table]) -> None:
    """After loading explicit ids, advance PostgreSQL sequences so new inserts do not collide."""
    for t in tables:
        col = t.autoincrement_column
        if col is None:
            continue
        seq = conn.execute(text("SELECT pg_get_serial_sequence(:t, :c)"), {"t": t.name, "c": col.name}).scalar()
        if seq:
            conn.execute(text(f"SELECT setval(:s, COALESCE((SELECT MAX({col.name}) FROM {t.name}), 1), (SELECT COUNT(*) > 0 FROM {t.name}))"), {"s": seq})  # noqa: S608 - names from metadata


def _write_files(files: dict[str, list[bytes]], files_dir: Path, overwrite: bool, prefixes: tuple[str, ...], report: RestoreReport) -> None:
    root = files_dir.resolve()
    for rel, parts in sorted(files.items()):
        _safe_rel(rel)
        if not rel.startswith(prefixes):
            report.files_skipped.append(rel)
            continue
        # archive path -> data-dir path: config/config.toml -> config.toml ; keys/pki/ca.key -> pki/ca.key ; keys/secrets/master.key -> secrets/master.key
        target_rel = rel.split("/", 1)[1]
        if rel.startswith("config/") and target_rel.startswith("settings."):
            report.files_skipped.append(rel)  # reference dump, not a restorable file
            continue
        dest = (root / target_rel).resolve()
        if root not in dest.parents:
            raise BackupCorrupt(f"unsafe file path in backup: {rel!r}")
        if dest.exists() and not overwrite:
            report.files_skipped.append(rel)
            report.warnings.append(f"{target_rel} already exists and was not overwritten")
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(dest.name + ".restoring")
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "wb") as fh:
            for p in parts:
                fh.write(p)
        os.replace(tmp, dest)
        if not target_rel.endswith((".crt", ".toml")):
            os.chmod(dest, stat.S_IRUSR | stat.S_IWUSR)
        report.files_written.append(target_rel)


def table_counts(db: Database) -> dict[str, int]:
    out: dict[str, int] = {}
    with db.connect() as conn:
        present = set(inspect(conn).get_table_names())
        for t in backup_tables(db):
            if t.name in present:
                out[t.name] = int(conn.execute(text(f"SELECT COUNT(*) FROM {t.name}")).scalar() or 0)  # noqa: S608 - names from metadata
    return out


def require_passphrase_or_master(keyspec: KeySpec) -> None:
    if not keyspec.passphrase and keyspec.master is None:
        raise BadRequest("a backup passphrase is required")
