# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Backup service (service name ``backup``): scheduled/manual encrypted backups, validate, restore, restore-test, targets, retention.

DR prerequisites (also in docs/dev/SECOPS.md): to rebuild a lost control plane from a backup you need
  1. the backup file (any target) and its passphrase, or - for ``master``-mode backups - the platform master key (``secrets/master.key``),
  2. an empty database of the same or a newer OLCF version (``python -m olcf.ops.backup restore ...`` creates the schema at the backup's version first),
  3. the master key, to decrypt the secret rows (include it in the backup with ``include_keys`` + a passphrase, or keep your own copy),
  4. PKI/CA and signing keys if workers are to keep trusting the platform (``include_keys``) - otherwise re-enrol workers.
Lakehouse data is not part of this backup (it follows the storage provider's own strategy).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import tempfile
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import delete, select, text, update

from olcf.core.context import AppContext
from olcf.core.errors import BadRequest, Conflict, NotFound, Unavailable
from olcf.core.ids import new_id
from olcf.core.timeutil import now_iso
from olcf.db import schema as S
from olcf.db.engine import Database
from olcf.db.util import fetch_all, fetch_one, insert_row
from olcf.ops import backup_core as core
from olcf.ops import backup_targets as T
from olcf.ops import schema as O
from olcf.ops.crypto import BackupCorrupt, KeySpec, check_passphrase
from olcf.security.rbac import Principal

log = logging.getLogger("olcf.ops.backup")

CONFIG_KEY = "ops.backup.config"
STATE_KEY = "ops.backup.state"
DEFAULT_CONFIG: dict[str, Any] = {
    "schedule": {"enabled": False, "cron": "17 2 * * *"},
    "retention": {"keep_last": 7, "keep_days": 30},
    "include_keys": False,  # PKI/signing/master keys; requires a stored passphrase
    "targets": [],  # names of enabled targets that get every backup
    "restore_test": {"enabled": False, "cron": "43 4 * * 0"},
    "passphrase_secret_id": None,
}
FILENAME_FMT = "olcf-backup-{ts}-{id}.olcfbak"
KEY_FILES = ("secrets/master.key", "secrets/jwt-signing.key", "secrets/jwt-signing.prev.key", "pki/ca.key", "pki/ca.crt", "pki/server.key", "pki/server.crt")


class BackupService:
    """Creates, lists, validates, restores and prunes backups, and copies them to the configured targets. One backup or restore runs at a time per process.
    """
    def __init__(self, ctx: AppContext) -> None:
        self.ctx = ctx
        self.dir = ctx.settings.path("backups")
        self._lock = threading.Lock()  # one backup/restore at a time per process
        self._active: str | None = None

    # ------------------------------------------------------------------ config
    def get_config(self) -> dict[str, Any]:
        with self.ctx.db.connect() as conn:
            row = fetch_one(conn, select(S.kv).where(S.kv.c.key == CONFIG_KEY))
        cfg = json.loads(json.dumps(DEFAULT_CONFIG))
        if row:
            for k, v in row["value"].items():
                if isinstance(v, dict) and isinstance(cfg.get(k), dict):
                    cfg[k].update(v)
                else:
                    cfg[k] = v
        return cfg

    def public_config(self) -> dict[str, Any]:
        cfg = self.get_config()
        cfg["passphrase_configured"] = bool(cfg.pop("passphrase_secret_id", None))
        return cfg

    def set_config(self, principal: Principal, patch: dict[str, Any], passphrase: str | None = None) -> dict[str, Any]:
        cfg = self.get_config()
        for k, v in patch.items():
            if k not in DEFAULT_CONFIG or k == "passphrase_secret_id":
                raise BadRequest(f"unknown backup setting '{k}'")
            if isinstance(v, dict) and isinstance(cfg.get(k), dict):
                cfg[k].update(v)
            else:
                cfg[k] = v
        self._validate_config(cfg)
        if passphrase is not None:
            check_passphrase(passphrase)
            if cfg.get("passphrase_secret_id"):
                self.ctx.secrets.rotate(cfg["passphrase_secret_id"], passphrase)
            else:
                cfg["passphrase_secret_id"] = self.ctx.secrets.create("backup-passphrase", passphrase, created_by=principal.id, meta={"kind": "backup-passphrase"})["id"]
        if cfg["include_keys"] and not cfg.get("passphrase_secret_id"):
            raise BadRequest("include_keys requires a backup passphrase (key material is never written without one)",
                             remediation="set 'passphrase' together with include_keys")
        self._save(CONFIG_KEY, cfg)
        self.ctx.audit.record(principal, "admin.backup_config_changed", resource_type="backup_config", details={"keys": sorted(patch), "passphrase_changed": passphrase is not None})
        return self.public_config()

    def _validate_config(self, cfg: dict[str, Any]) -> None:
        from croniter import croniter

        for sect in ("schedule", "restore_test"):
            c = cfg[sect]
            if not isinstance(c.get("enabled"), bool) or not croniter.is_valid(str(c.get("cron", ""))):
                raise BadRequest(f"{sect}: 'enabled' must be a boolean and 'cron' a valid 5-field cron expression")
        r = cfg["retention"]
        for k in ("keep_last", "keep_days"):
            if not isinstance(r.get(k), int) or r[k] < 0 or r[k] > 100000:
                raise BadRequest(f"retention.{k} must be an integer >= 0")
        if r["keep_last"] < 1 and r["keep_days"] < 1:
            raise BadRequest("retention must keep at least one backup (keep_last >= 1 or keep_days >= 1)")
        if not isinstance(cfg["targets"], list) or not all(isinstance(t, str) for t in cfg["targets"]):
            raise BadRequest("targets must be a list of target names")
        known = {t["name"] for t in self.list_targets()}
        missing = [t for t in cfg["targets"] if t not in known]
        if missing:
            raise BadRequest(f"unknown backup target(s): {', '.join(missing)}")

    def _save(self, key: str, value: Any) -> None:
        with self.ctx.db.begin() as conn:
            n = conn.execute(update(S.kv).where(S.kv.c.key == key).values(value=value, updated_at=now_iso())).rowcount
            if not n:
                conn.execute(S.kv.insert().values(key=key, value=value, updated_at=now_iso()))

    def _state(self) -> dict[str, Any]:
        with self.ctx.db.connect() as conn:
            row = fetch_one(conn, select(S.kv).where(S.kv.c.key == STATE_KEY))
        return dict(row["value"]) if row else {}

    # ------------------------------------------------------------------ targets
    @staticmethod
    def public_target(r: dict[str, Any]) -> dict[str, Any]:
        return {k: r.get(k) for k in ("id", "name", "type", "config", "enabled", "retention", "last_test", "created_at", "updated_at")} | {"has_secret": bool(r.get("secret_id"))}

    def list_targets(self) -> list[dict[str, Any]]:
        with self.ctx.db.connect() as conn:
            return fetch_all(conn, select(O.backup_targets).order_by(O.backup_targets.c.name))

    def get_target(self, ref: str) -> dict[str, Any]:
        with self.ctx.db.connect() as conn:
            row = fetch_one(conn, select(O.backup_targets).where((O.backup_targets.c.id == ref) | (O.backup_targets.c.name == ref)))
        if not row:
            raise NotFound(f"backup target '{ref}' not found")
        return row

    def create_target(self, principal: Principal, body: dict[str, Any]) -> dict[str, Any]:
        import re

        name, typ = body.get("name", ""), body.get("type", "")
        if not re.match(r"^[a-z0-9][a-z0-9-]{0,62}$", name):
            raise BadRequest("target name must be lowercase letters, digits and dashes")
        if typ not in T.TARGET_TYPES:
            raise BadRequest(f"unknown target type '{typ}'", details={"types": list(T.TARGET_TYPES)})
        secret_id = None
        if body.get("secret") is not None:
            secret_id = self.ctx.secrets.create(f"backup-target-{name}", body["secret"], created_by=principal.id, meta={"kind": "backup-target"})["id"]
        elif body.get("secret_id"):
            self.ctx.secrets.get_meta(body["secret_id"])
            secret_id = body["secret_id"]
        now = now_iso()
        row = {"id": new_id("bkt"), "name": name, "type": typ, "config": body.get("config") or {}, "secret_id": secret_id, "enabled": bool(body.get("enabled", True)),
               "retention": body.get("retention") or {}, "last_test": None, "created_by": principal.id, "created_at": now, "updated_at": now}
        T.build_target(self.ctx, row)  # validates the configuration shape
        with self.ctx.db.begin() as conn:
            if fetch_one(conn, select(O.backup_targets.c.id).where(O.backup_targets.c.name == name)):
                raise Conflict(f"a backup target named '{name}' already exists")
            insert_row(conn, O.backup_targets, row)
        self.ctx.audit.record(principal, "admin.backup_target_created", resource_type="backup_target", resource_id=row["id"], details={"name": name, "type": typ})
        return self.public_target(row)

    def update_target(self, principal: Principal, ref: str, body: dict[str, Any]) -> dict[str, Any]:
        row = self.get_target(ref)
        vals: dict[str, Any] = {"updated_at": now_iso()}
        for k in ("config", "enabled", "retention"):
            if k in body and body[k] is not None:
                vals[k] = body[k]
        if body.get("secret") is not None:
            if row.get("secret_id"):
                self.ctx.secrets.rotate(row["secret_id"], body["secret"])
            else:
                vals["secret_id"] = self.ctx.secrets.create(f"backup-target-{row['name']}", body["secret"], created_by=principal.id, meta={"kind": "backup-target"})["id"]
        merged = {**row, **vals}
        T.build_target(self.ctx, merged)
        vals["last_test"] = None
        with self.ctx.db.begin() as conn:
            conn.execute(update(O.backup_targets).where(O.backup_targets.c.id == row["id"]).values(**vals))
        self.ctx.audit.record(principal, "admin.backup_target_updated", resource_type="backup_target", resource_id=row["id"], details={"fields": sorted(vals)})
        return self.public_target(self.get_target(row["id"]))

    def delete_target(self, principal: Principal, ref: str) -> None:
        row = self.get_target(ref)
        if row["name"] in self.get_config()["targets"]:
            raise Conflict("the target is used by the backup configuration; remove it from the configuration first")
        with self.ctx.db.begin() as conn:
            conn.execute(delete(O.backup_targets).where(O.backup_targets.c.id == row["id"]))
        if row.get("secret_id"):
            try:
                self.ctx.secrets.delete(row["secret_id"])
            except NotFound:
                pass
        self.ctx.audit.record(principal, "admin.backup_target_deleted", resource_type="backup_target", resource_id=row["id"], details={"name": row["name"]})

    def test_target(self, ref: str) -> dict[str, Any]:
        row = self.get_target(ref)
        try:
            res = T.build_target(self.ctx, row).test()
        except Exception as exc:  # noqa: BLE001
            res = {"ok": False, "detail": f"{type(exc).__name__}: {exc}"[:300], "steps": []}
        res["at"] = now_iso()
        with self.ctx.db.begin() as conn:
            conn.execute(update(O.backup_targets).where(O.backup_targets.c.id == row["id"]).values(last_test=res))
        return res

    # ------------------------------------------------------------------ records
    def list(self, limit: int = 100) -> list[dict[str, Any]]:
        with self.ctx.db.connect() as conn:
            rows = fetch_all(conn, select(O.backups).order_by(O.backups.c.created_at.desc(), O.backups.c.id.desc()).limit(limit))
        return [self.public(r) for r in rows]

    def get(self, bid: str) -> dict[str, Any]:
        with self.ctx.db.connect() as conn:
            row = fetch_one(conn, select(O.backups).where(O.backups.c.id == bid))
        if not row:
            raise NotFound(f"backup '{bid}' not found")
        return row

    @staticmethod
    def public(r: dict[str, Any]) -> dict[str, Any]:
        out = {k: r.get(k) for k in ("id", "kind", "state", "filename", "size_bytes", "sha256", "encryption", "key_id", "includes", "app_version", "dialect", "migrations",
                                     "targets", "error", "last_validation", "last_restore_test", "created_by", "created_at", "finished_at")}
        m = r.get("manifest") or {}
        out["rows"] = m.get("total_rows")
        out["tables"] = len(m.get("tables", {}))
        return out

    def file_path(self, rec: dict[str, Any]) -> Path:
        name = T.safe_name(rec.get("filename") or "")
        p = (self.dir / name).resolve()
        if self.dir.resolve() not in p.parents or not p.is_file():
            raise NotFound("the backup file is no longer on the control plane (check the off-host targets)", details={"targets": [t.get("name") for t in rec.get("targets", [])]})
        return p

    def delete(self, principal: Principal, bid: str, *, everywhere: bool = True) -> dict[str, Any]:
        rec = self.get(bid)
        removed = []
        if rec.get("filename"):
            (self.dir / T.safe_name(rec["filename"])).unlink(missing_ok=True)
            removed.append("local")
            if everywhere:
                for t in rec.get("targets") or []:
                    try:
                        T.build_target(self.ctx, self.get_target(t["name"])).delete(rec["filename"])
                        removed.append(t["name"])
                    except Exception as exc:  # noqa: BLE001
                        log.warning("could not delete %s from target %s: %s", rec["filename"], t.get("name"), exc)
        with self.ctx.db.begin() as conn:
            conn.execute(delete(O.backups).where(O.backups.c.id == bid))
        self.ctx.audit.record(principal, "admin.backup_deleted", resource_type="backup", resource_id=bid, details={"removed_from": removed})
        return {"removed_from": removed}

    # ------------------------------------------------------------------ keyspec
    def keyspec(self, passphrase: str | None = None, *, for_write: bool = False) -> tuple[KeySpec, str]:
        """(KeySpec, mode). Writing: explicit passphrase > stored passphrase > master key."""
        pw = passphrase
        if not pw:
            sid = self.get_config().get("passphrase_secret_id")
            if sid:
                pw = str(self.ctx.secrets.resolve(sid))
        if pw:
            return KeySpec(pw, self.ctx.secrets.master), "passphrase"
        return KeySpec(None, self.ctx.secrets.master), "master"

    def _key_files(self) -> list[core.FileSpec]:
        root = self.ctx.settings.data_dir
        files = []
        for rel in KEY_FILES:
            p = root / rel
            if p.is_file():
                files.append(core.FileSpec("keys/" + rel, source=p))
        return files

    def _config_file(self) -> core.FileSpec:
        """Reference copy of the effective (redacted) settings; informational, never applied by restore."""
        from olcf.core.redaction import redact_obj

        data = json.dumps(redact_obj(json.loads(self.ctx.settings.model_dump_json())), indent=2, sort_keys=True).encode()
        return core.FileSpec("config/settings.json", data=data)

    # ------------------------------------------------------------------ create
    def start_backup(self, principal: Principal | None, *, kind: str = "manual", include_keys: bool | None = None, passphrase: str | None = None,
                     targets: list[str] | None = None, wait: bool = False) -> dict[str, Any]:
        """Create a backup record and run it in the background (or inline with ``wait``). Returns ``{operation_id, backup_id}``."""
        cfg = self.get_config()
        include = cfg["include_keys"] if include_keys is None else include_keys
        spec, mode = self.keyspec(passphrase, for_write=True)
        if include and mode != "passphrase":
            raise BadRequest("key material (CA key, signing keys, master key) is only ever included with an operator passphrase")
        names = cfg["targets"] if targets is None else targets
        for n in names:
            self.get_target(n)
        bid = new_id("bkp")
        ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        filename = FILENAME_FMT.format(ts=ts, id=bid.split("_", 1)[1][-10:].lower())
        now = now_iso()
        rec = {"id": bid, "kind": kind, "state": "running", "filename": filename, "encryption": mode, "key_id": self.ctx.secrets.master.backup_id if mode == "master" else None,
               "includes": {"keys": include, "config": True}, "manifest": {}, "migrations": {}, "targets": [], "created_by": principal.id if principal else None, "created_at": now}
        with self.ctx.db.begin() as conn:
            insert_row(conn, O.backups, rec)
        ops = self.ctx.service("operations")
        oid = ops.create("backup.create", principal.id if principal else None, f"backup {bid} started")
        args = (bid, oid, spec, include, names, principal)
        if wait:
            self._run_backup(*args)
        else:
            threading.Thread(target=self._run_backup, args=args, name=f"backup-{bid}", daemon=True).start()
        return {"operation_id": oid, "backup_id": bid, "filename": filename}

    def _run_backup(self, bid: str, oid: str, spec: KeySpec, include_keys: bool, target_names: list[str], principal: Principal | None) -> None:
        ops = self.ctx.service("operations")
        rec = self.get(bid)
        path = self.dir / rec["filename"]
        try:
            with self._lock:
                self._active = bid
                files = [self._config_file()] + (self._key_files() if include_keys else [])
                res = core.write_backup(self.ctx.db, path, spec, meta={"backup_id": bid, "kind": rec["kind"], "includes_keys": include_keys, "node": self.ctx.node_id}, files=files,
                                        progress=lambda p, m: ops.update(oid, progress=p, message=m))
            ops.update(oid, progress=92, message="verifying the backup file")
            val = core.validate_backup(path, spec)
            if not val.ok:
                raise BackupCorrupt("the freshly written backup failed verification: " + "; ".join(val.errors[:3]))
            copies = []
            for i, n in enumerate(target_names):
                ops.update(oid, progress=92 + int(6 * (i + 1) / max(len(target_names), 1)), message=f"copying to target {n}")
                copies.append(self._copy_to_target(n, path, rec["filename"]))
            with self.ctx.db.begin() as conn:
                conn.execute(update(O.backups).where(O.backups.c.id == bid).values(
                    state="succeeded", size_bytes=res.size_bytes, sha256=res.sha256, manifest=res.manifest, app_version=res.header.get("app_version"),
                    dialect=self.ctx.db.dialect, migrations=res.migrations, targets=copies, last_validation=val.to_dict(), finished_at=now_iso()))
            failed = [c for c in copies if not c["ok"]]
            self.ctx.audit.record(principal, "admin.backup_created", resource_type="backup", resource_id=bid, outcome="success" if not failed else "partial",
                                  details={"kind": rec["kind"], "size": res.size_bytes, "rows": res.rows, "keys": include_keys, "targets": [{"name": c["name"], "ok": c["ok"]} for c in copies]})
            self.ctx.events.publish("backup.created", {"id": bid, "size": res.size_bytes, "ok": not failed})
            ops.update(oid, state="succeeded", progress=100, message=f"backup {bid} complete" + (f"; {len(failed)} target(s) failed" if failed else ""),
                       result={"backup_id": bid, "size_bytes": res.size_bytes, "rows": res.rows, "targets": copies})
            self.apply_retention()
        except Exception as exc:  # noqa: BLE001
            log.exception("backup %s failed", bid)
            err = exc.to_dict() if hasattr(exc, "to_dict") else {"code": "internal", "message": str(exc)}
            path.unlink(missing_ok=True)
            with self.ctx.db.begin() as conn:
                conn.execute(update(O.backups).where(O.backups.c.id == bid).values(state="failed", error=err, finished_at=now_iso()))
            self.ctx.audit.record(principal, "admin.backup_created", resource_type="backup", resource_id=bid, outcome="failure", details={"error": err.get("message")})
            self.ctx.events.publish("backup.failed", {"id": bid, "error": err.get("message")})
            ops.update(oid, state="failed", progress=100, message=err.get("message", "backup failed"), error=err)
        finally:
            self._active = None

    def _copy_to_target(self, name: str, path: Path, filename: str) -> dict[str, Any]:
        try:
            uri = T.build_target(self.ctx, self.get_target(name)).put(path, filename)
            return {"name": name, "uri": uri, "ok": True, "at": now_iso()}
        except Exception as exc:  # noqa: BLE001
            log.warning("backup copy to target %s failed: %s", name, exc)
            return {"name": name, "ok": False, "error": f"{type(exc).__name__}: {exc}"[:300], "at": now_iso()}

    # ------------------------------------------------------------------ fetch / import
    def materialise(self, bid: str, tmp_dir: Path) -> Path:
        """Local path of a backup file; downloaded from the first working target when it is no longer on this host."""
        rec = self.get(bid)
        try:
            return self.file_path(rec)
        except NotFound:
            pass
        for t in rec.get("targets") or []:
            if not t.get("ok"):
                continue
            dest = tmp_dir / rec["filename"]
            try:
                T.build_target(self.ctx, self.get_target(t["name"])).get(rec["filename"], dest)
                return dest
            except Exception as exc:  # noqa: BLE001
                log.warning("could not fetch %s from %s: %s", rec["filename"], t["name"], exc)
        raise NotFound("the backup file is not available locally or on any target")

    def import_file(self, principal: Principal, stream: Any, filename: str) -> dict[str, Any]:
        """Register an uploaded backup file (e.g. brought from another installation); it is validated when used."""
        from olcf.ops.crypto import MAGIC, read_header

        bid = new_id("bkp")
        safe = T.safe_name(Path(filename or "upload.olcfbak").name.replace(" ", "_"))
        dest_name = f"imported-{bid.split('_', 1)[1][-8:].lower()}-{safe}"
        if not dest_name.endswith(".olcfbak"):
            dest_name += ".olcfbak"
        self.dir.mkdir(parents=True, exist_ok=True)
        dest = self.dir / dest_name
        size = 0
        with open(dest, "wb") as out:
            while chunk := stream.read(1024 * 1024):
                size += len(chunk)
                if size > 64 * 1024**3:
                    dest.unlink(missing_ok=True)
                    raise BadRequest("backup file too large")
                out.write(chunk)
        try:
            hdr = read_header(dest)
        except Exception:  # noqa: BLE001
            dest.unlink(missing_ok=True)
            raise BackupCorrupt("not an OLCF backup file") from None
        from olcf.ops.crypto import sha256_file

        rec = {"id": bid, "kind": "imported", "state": "succeeded", "filename": dest_name, "size_bytes": size, "sha256": sha256_file(dest), "encryption": "passphrase" if hdr.get("kdf") == "scrypt" else "master",
               "key_id": hdr.get("key_id"), "includes": {}, "manifest": {}, "app_version": hdr.get("app_version"), "migrations": {}, "targets": [],
               "created_by": principal.id, "created_at": now_iso(), "finished_at": now_iso()}
        _ = MAGIC
        with self.ctx.db.begin() as conn:
            insert_row(conn, O.backups, rec)
        self.ctx.audit.record(principal, "admin.backup_imported", resource_type="backup", resource_id=bid, details={"size": size})
        return self.public(rec)

    # ------------------------------------------------------------------ validate / restore-test / restore
    def validate(self, principal: Principal | None, bid: str, passphrase: str | None = None) -> dict[str, Any]:
        rec = self.get(bid)
        spec, _ = self.keyspec(passphrase)
        with tempfile.TemporaryDirectory(dir=self._tmp()) as td:
            path = self.materialise(bid, Path(td))
            rep = core.validate_backup(path, spec).to_dict()
        rep["file_sha256_ok"] = None
        if rec.get("sha256") and rec.get("filename"):
            try:
                from olcf.ops.crypto import sha256_file

                rep["file_sha256_ok"] = sha256_file(self.file_path(rec)) == rec["sha256"]
                rep["ok"] = rep["ok"] and rep["file_sha256_ok"]
            except NotFound:
                pass
        with self.ctx.db.begin() as conn:
            conn.execute(update(O.backups).where(O.backups.c.id == bid).values(last_validation=rep))
        self.ctx.audit.record(principal, "admin.backup_validated", resource_type="backup", resource_id=bid, outcome="success" if rep["ok"] else "failure", details={"errors": rep["errors"][:5]})
        return rep

    def _tmp(self) -> Path:
        d = self.ctx.settings.path("tmp")
        d.mkdir(parents=True, exist_ok=True)
        return d

    def restore_test(self, principal: Principal | None, bid: str, passphrase: str | None = None) -> dict[str, Any]:
        """Restore into a throw-away SQLite database and verify that the result is a working control plane. Never touches the live database."""
        spec, _ = self.keyspec(passphrase)
        t0 = time.perf_counter()
        checks: list[dict[str, Any]] = []
        jobs_report: dict[str, Any] | None = None

        def chk(name: str, ok: bool, detail: str = "") -> bool:
            checks.append({"name": name, "ok": bool(ok), "detail": detail})
            return bool(ok)

        rec = self.get(bid)
        with tempfile.TemporaryDirectory(dir=self._tmp()) as td:
            tdp = Path(td)
            db = None
            try:
                path = self.materialise(bid, tdp)
                val = core.validate_backup(path, spec)
                chk("validate", val.ok, "; ".join(val.errors[:3]) or f"{val.rows} rows verified")
                if val.ok:
                    db = Database(f"sqlite:///{tdp / 'restore-test.db'}")
                    rep = core.restore_backup(path, spec, db, files_dir=tdp / "files", run_migrations=True, reconcile_jobs=True, backup_id=bid)
                    chk("restore", rep.ok, f"{sum(rep.tables.values())} rows into {len(rep.tables)} tables; migrations {rep.migrations_before} -> {rep.migrations_after}")
                    manifest = val.manifest.get("tables", {})
                    counts = core.table_counts(db)
                    added = {"job_events": (rep.jobs or {}).get("events_added", 0)}  # the job reconciliation writes timeline rows of its own
                    bad = [f"{n}: {counts.get(n)} != {m['rows'] + added.get(n, 0)}" for n, m in manifest.items() if n not in core.ONLINE_RESTORE_KEEP and counts.get(n) != m["rows"] + added.get(n, 0)]
                    chk("row-counts", not bad, ", ".join(bad[:5]) or f"{len(manifest)} tables match the manifest")
                    with db.connect() as conn:
                        integ = conn.exec_driver_sql("PRAGMA integrity_check").scalar()
                        fk = conn.exec_driver_sql("PRAGMA foreign_key_check").fetchall()
                    chk("integrity", integ == "ok" and not fk, f"integrity_check={integ}, foreign_key violations={len(fk)}")
                    from olcf.ops.retention import verify_audit_chain

                    av = verify_audit_chain(db)
                    chk("audit-chain", av.get("ok", False), f"{av.get('checked', 0)} audit records verified" if av.get("ok") else str(av)[:200])
                    chk("secrets-decrypt", *self._secrets_check(db))
                    with db.connect() as conn:
                        admins = conn.execute(text("SELECT COUNT(*) FROM role_bindings WHERE role = 'PlatformAdmin'")).scalar() or 0
                        wcount = conn.execute(text("SELECT COUNT(*) FROM workspaces")).scalar() or 0
                    chk("admin-binding", admins > 0, f"{admins} PlatformAdmin binding(s), {wcount} workspace(s)")
                    keys = [f for f in rep.files_written if f.endswith(("ca.key", "master.key"))]
                    jobs = jobs_report = rep.jobs or {}
                    jobs_after = self._open_jobs(db)
                    chk("job-reconcile", rep.jobs is not None and jobs_after == 0,
                        (jobs.get("message") or "no jobs") + ("" if jobs_after == 0 else f"; {jobs_after} unfinished job(s) remain after the reconciliation"))
                    chk("key-material", True, "included: " + ", ".join(sorted(rep.files_written)) if keys else "not included in this backup (expected unless include_keys was set)")
            except Exception as exc:  # noqa: BLE001
                chk("restore-test", False, f"{type(exc).__name__}: {getattr(exc, 'message', exc)}"[:300])
            finally:
                if db is not None:
                    db.dispose()
        ok = all(c["ok"] for c in checks)
        res = {"ok": ok, "checks": checks, "seconds": round(time.perf_counter() - t0, 2), "at": now_iso(), "backup_id": bid, "filename": rec.get("filename"), "jobs": jobs_report}
        with self.ctx.db.begin() as conn:
            conn.execute(update(O.backups).where(O.backups.c.id == bid).values(last_restore_test=res))
        self.ctx.audit.record(principal, "admin.backup_restore_tested", resource_type="backup", resource_id=bid, outcome="success" if ok else "failure",
                              details={"failed": [c["name"] for c in checks if not c["ok"]], "jobs_reconciled": (jobs_report or {}).get("failed", 0)})
        self.ctx.events.publish("backup.restore_test", {"id": bid, "ok": ok})
        return res

    @staticmethod
    def _open_jobs(db: Database) -> int:
        """Jobs that are neither finished nor owned by a provider in ``db`` (the restored throw-away database): none may remain once the restore reconciled them."""
        from sqlalchemy import func, inspect, or_

        from olcf.compute import schema as C
        from olcf.ops.restore_reconcile import AGENT_PROVIDERS, NON_TERMINAL

        with db.connect() as conn:
            if not inspect(conn).has_table("jobs"):
                return 0
            return int(conn.execute(select(func.count()).select_from(C.jobs).where(C.jobs.c.state.in_(NON_TERMINAL), or_(C.jobs.c.provider.is_(None), C.jobs.c.provider.in_(AGENT_PROVIDERS)))).scalar() or 0)

    def _secrets_check(self, db: Database) -> tuple[bool, str]:
        # the key ring is part of the backup (the same snapshot as the ciphertext): read the ring from the *restored* database, with this node's root keys
        master = self.ctx.secrets.master.view_for(db) if hasattr(self.ctx.secrets.master, "view_for") else self.ctx.secrets.master
        with db.connect() as conn:
            rows = fetch_all(conn, select(S.secrets).where(S.secrets.c.provider == "file", S.secrets.c.deleted_at.is_(None)))
        if not rows:
            return True, "no file-provider secrets"
        from olcf.security.secrets import FileSecretProvider

        provider = FileSecretProvider(master)
        bad = []
        for r in rows:
            try:
                provider.fetch(r)
            except Exception:  # noqa: BLE001
                bad.append(r["id"])
        if bad:
            return False, f"{len(bad)} of {len(rows)} secrets cannot be decrypted with the master key available here (key ids {sorted({str(r['key_id']) for r in rows})}, have {sorted(master.keys)})"
        ring = getattr(master, "ring", None)
        return True, f"all {len(rows)} secrets decrypt with the restored key ring (version {ring.version if ring else 0}) and this node's root key"

    def prerequisites(self, bid: str, passphrase: str | None = None) -> dict[str, Any]:
        rec = self.get(bid)
        mk = self.ctx.secrets.master
        pre = [{"name": "backup file", "ok": True, "detail": rec.get("filename")},
               {"name": "decryption", "ok": rec["encryption"] == "master" and (rec.get("key_id") in getattr(mk, "root_keys", mk.keys)) or (rec["encryption"] == "passphrase" and bool(passphrase or self.get_config().get("passphrase_secret_id"))),
                "detail": "master key present" if rec["encryption"] == "master" else "operator passphrase required"},
               {"name": "master key for secrets", "ok": True, "detail": f"this installation holds root master key(s) {sorted(getattr(mk, 'root_keys', mk.keys))}; the backup carries the database key ring (wrapped by the root key that was "
                "current when it was taken) next to the secret rows, so one of those root keys must be present, otherwise restore the key files first (see runbook)"},
               {"name": "effect", "ok": True, "detail": "ALL control-plane data in this database is replaced; sessions/tokens issued after the backup become invalid; "
                "a pre-restore backup is taken automatically"}]
        return {"backup_id": bid, "prerequisites": pre}

    def start_restore(self, principal: Principal, bid: str, *, passphrase: str | None, confirm: str, restore_keys: bool = False, wait: bool = False) -> dict[str, Any]:
        if confirm != "RESTORE":
            raise BadRequest("restoring replaces the control-plane database; confirm by sending confirm='RESTORE'")
        rec = self.get(bid)
        if rec["state"] != "succeeded":
            raise Conflict("only a successful backup can be restored")
        spec, _ = self.keyspec(passphrase)
        ops = self.ctx.service("operations")
        oid = ops.create("backup.restore", principal.id, f"restore of {bid} started")
        args = (bid, oid, spec, principal, restore_keys)
        if wait:
            self._run_restore(*args)
        else:
            threading.Thread(target=self._run_restore, args=args, name=f"restore-{bid}", daemon=True).start()
        return {"operation_id": oid, "backup_id": bid}

    def _run_restore(self, bid: str, oid: str, spec: KeySpec, principal: Principal, restore_keys: bool) -> None:
        ops = self.ctx.service("operations")
        try:
            with tempfile.TemporaryDirectory(dir=self._tmp()) as td:
                path = self.materialise(bid, Path(td))
                ops.update(oid, progress=5, message="validating the backup")
                val = core.validate_backup(path, spec)
                if not val.ok:
                    raise BackupCorrupt("refusing to restore: " + "; ".join(val.errors[:3]))
                ops.update(oid, progress=15, message="taking a pre-restore backup of the current state")
                pre = self.start_backup(principal, kind="pre-restore", wait=True, targets=[])
                if self.get(pre["backup_id"])["state"] != "succeeded":
                    raise Unavailable("the pre-restore backup failed; refusing to restore")
                ops.update(oid, progress=30, message="restoring")
                with self._lock:
                    rep = core.restore_backup(path, spec, self.ctx.db, online=True, wipe=True, files_dir=self.ctx.settings.data_dir if restore_keys else None,
                                              overwrite_files=restore_keys, file_prefixes=("keys/",), reconcile_jobs=True, live_workers=self._connected_workers(), backup_id=bid)
                # the restore replaced the backup history with the one in the file; keep the pre-restore record visible
                self._reinsert_pre_restore(pre["backup_id"])
            self.ctx.audit.record(principal, "admin.backup_restored", resource_type="backup", resource_id=bid, details={"rows": sum(rep.tables.values()), "pre_restore_backup": pre["backup_id"],
                                                                                                                                      "jobs_failed": (rep.jobs or {}).get("failed", 0), "jobs_kept": (rep.jobs or {}).get("kept", 0)})
            ops.update(oid, state="succeeded", progress=100, message="restore complete; restart the platform nodes to reload caches",
                       result={**rep.to_dict(), "pre_restore_backup": pre["backup_id"]})
        except Exception as exc:  # noqa: BLE001
            log.exception("restore %s failed", bid)
            err = exc.to_dict() if hasattr(exc, "to_dict") else {"code": "internal", "message": str(exc)}
            self.ctx.audit.record(principal, "admin.backup_restored", resource_type="backup", resource_id=bid, outcome="failure", details={"error": err.get("message")})
            ops.update(oid, state="failed", progress=100, message=err.get("message", "restore failed"), error=err)

    def _connected_workers(self) -> set[str]:
        """Workers connected to this controller right now (their attempts are alive; everything else of the restored snapshot is not)."""
        compute = self.ctx.maybe_service("compute")
        hub = getattr(compute, "hub", None)
        try:
            return set(hub.connected_ids()) if hub is not None else set()
        except Exception:  # noqa: BLE001
            return set()

    def _reinsert_pre_restore(self, bid: str) -> None:
        # the pre-restore backup row lived in the database that was just replaced; recreate a minimal row for it so the file stays discoverable
        with self.ctx.db.connect() as conn:
            exists = fetch_one(conn, select(O.backups.c.id).where(O.backups.c.id == bid))
        if exists:
            return
        for p in sorted(self.dir.glob("olcf-backup-*.olcfbak")):
            if bid.split("_", 1)[1][-10:].lower() in p.name:
                from olcf.ops.crypto import read_header, sha256_file

                hdr = read_header(p)
                rec = {"id": bid, "kind": "pre-restore", "state": "succeeded", "filename": p.name, "size_bytes": p.stat().st_size, "sha256": sha256_file(p),
                       "encryption": "passphrase" if hdr.get("kdf") == "scrypt" else "master", "key_id": hdr.get("key_id"), "includes": {}, "manifest": {}, "migrations": {},
                       "targets": [], "app_version": hdr.get("app_version"), "created_at": hdr.get("created_at") or now_iso(), "finished_at": now_iso()}
                with self.ctx.db.begin() as conn:
                    insert_row(conn, O.backups, rec)
                return

    # ------------------------------------------------------------------ retention
    @staticmethod
    def _stamp(name: str) -> datetime | None:
        try:
            return datetime.strptime(name.split("-")[2], "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
        except (IndexError, ValueError):
            return None

    @staticmethod
    def _prune_names(names: list[str], keep_last: int, keep_days: int, now: datetime | None = None) -> list[str]:
        """Names to delete: older than ``keep_days`` AND beyond the newest ``keep_last``. Always keeps the newest one."""
        now = now or datetime.now(UTC)
        stamped = sorted(((BackupService._stamp(n), n) for n in names if BackupService._stamp(n)), reverse=True)
        out = []
        for i, (ts, n) in enumerate(stamped):
            if i == 0:
                continue
            by_count = keep_last > 0 and i < keep_last
            by_age = keep_days > 0 and ts > now - timedelta(days=keep_days)
            if not (by_count or by_age):
                out.append(n)
        return out

    def apply_retention(self, *, dry_run: bool = False) -> dict[str, Any]:
        cfg = self.get_config()
        keep_last, keep_days = cfg["retention"]["keep_last"], cfg["retention"]["keep_days"]
        report: dict[str, Any] = {"local": [], "targets": {}}
        with self.ctx.db.connect() as conn:
            recs = fetch_all(conn, select(O.backups).where(O.backups.c.state == "succeeded", O.backups.c.kind.in_(["manual", "scheduled", "pre-update", "pre-restore"])))
        by_name = {r["filename"]: r for r in recs if r.get("filename")}
        for n in self._prune_names(list(by_name), keep_last, keep_days):
            report["local"].append(n)
            if not dry_run:
                (self.dir / n).unlink(missing_ok=True)
                with self.ctx.db.begin() as conn:
                    conn.execute(delete(O.backups).where(O.backups.c.id == by_name[n]["id"]))
        for t in self.list_targets():
            if not t["enabled"]:
                continue
            tr = t.get("retention") or {}
            try:
                tgt = T.build_target(self.ctx, t)
                names = [f["name"] for f in tgt.list()]
                victims = self._prune_names(names, int(tr.get("keep_last", keep_last)), int(tr.get("keep_days", keep_days)))
                report["targets"][t["name"]] = victims
                if not dry_run:
                    for n in victims:
                        tgt.delete(n)
            except Exception as exc:  # noqa: BLE001
                report["targets"][t["name"]] = f"error: {exc}"[:200]
        return report

    # ------------------------------------------------------------------ schedule (leader)
    def tick(self, now: datetime | None = None) -> dict[str, Any]:
        """Run what is due. Called by the module loop on the leader only; idempotent per cron slot."""
        from croniter import croniter

        now = now or datetime.now(UTC)
        cfg, state, did = self.get_config(), self._state(), {}
        for key, sect in (("last_scheduled_backup", "schedule"), ("last_restore_test", "restore_test")):
            c = cfg[sect]
            if not c["enabled"]:
                continue
            last = datetime.fromisoformat(state[key]) if state.get(key) else now - timedelta(seconds=1)
            if not state.get(key):  # first time: anchor, do not fire retroactively
                state[key] = now.isoformat()
                self._save(STATE_KEY, state)
                continue
            due = croniter(c["cron"], last).get_next(datetime)
            if due.tzinfo is None:
                due = due.replace(tzinfo=UTC)
            if due <= now:
                state[key] = now.isoformat()
                self._save(STATE_KEY, state)
                if sect == "schedule":
                    did["backup"] = self.start_backup(None, kind="scheduled", wait=True)
                else:
                    latest = next((b for b in self.list(5) if b["state"] == "succeeded"), None)
                    if latest:
                        did["restore_test"] = self.restore_test(None, latest["id"])["ok"]
        return did

    # ------------------------------------------------------------------ health
    def health(self, deep: bool = False) -> dict[str, Any]:
        cfg = self.get_config()
        recs = self.list(20)
        ok = [b for b in recs if b["state"] == "succeeded"]
        last = ok[0] if ok else None
        out: dict[str, Any] = {"configured": cfg["schedule"]["enabled"], "last_success": last["finished_at"] if last else None, "last_id": last["id"] if last else None}
        if last:
            age = (datetime.now(UTC) - datetime.fromisoformat(last["finished_at"].replace("Z", "+00:00"))).total_seconds()
            out["age_hours"] = round(age / 3600, 1)
        out["last_failed"] = next((b for b in recs if b["state"] == "failed"), None) is not None and (not last or recs.index(next(b for b in recs if b["state"] == "failed")) < recs.index(last))
        out["last_restore_test"] = next(({"ok": b["last_restore_test"]["ok"], "at": b["last_restore_test"]["at"]} for b in recs if b.get("last_restore_test")), None)
        return out


# ===================================================================== CLI: disaster recovery without a running platform
def _cli(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m olcf.ops.backup", description="Offline backup tooling (no running control plane needed)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    v = sub.add_parser("validate", help="decrypt and verify a backup file")
    r = sub.add_parser("restore", help="restore a backup into an EMPTY database (or --wipe an existing one)")
    for p in (v, r):
        p.add_argument("file")
        p.add_argument("--passphrase-env", default="OLCF_BACKUP_PASSPHRASE", help="environment variable holding the passphrase (never pass it on the command line)")
        p.add_argument("--master-key-file", help="master.key to use for master-mode backups (or set OLCF_MASTER_KEY)")
    r.add_argument("--db-url", required=True, help="SQLAlchemy URL of the target database")
    r.add_argument("--data-dir", help="restore key/PKI files into this data directory (when the backup contains them)")
    r.add_argument("--wipe", action="store_true", help="replace existing control-plane data in the target")
    r.add_argument("--fresh-schema", action="store_true", help="drop ALL tables first and recreate the schema at the backup's versions (used by update rollback)")
    ns = ap.parse_args(argv)
    import os

    from olcf.security.secrets import MasterKey

    pw = os.environ.get(ns.passphrase_env) or None
    master = None
    if ns.master_key_file:
        master = MasterKey(Path(ns.master_key_file).parent, explicit_file=ns.master_key_file)
    elif os.environ.get("OLCF_MASTER_KEY"):
        master = MasterKey(Path(tempfile.mkdtemp(prefix="olcf-mk-")))
    elif getattr(ns, "data_dir", None) and (Path(ns.data_dir) / "secrets" / "master.key").is_file():
        master = MasterKey(Path(ns.data_dir) / "secrets")
    spec = KeySpec(pw, master)
    path = Path(ns.file)
    try:
        if ns.cmd == "validate":
            rep = core.validate_backup(path, spec)
            print(json.dumps(rep.to_dict(), indent=2))
            return 0 if rep.ok else 2
        db = Database(ns.db_url)
        out = core.restore_backup(path, spec, db, wipe=ns.wipe or ns.fresh_schema, fresh_schema=ns.fresh_schema, files_dir=Path(ns.data_dir) if ns.data_dir else None)
        print(json.dumps(out.to_dict(), indent=2))
        return 0
    except (BackupCorrupt, BadRequest, Conflict) as exc:
        print(f"error: {exc.message}", file=sys.stderr)
        return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(_cli())
