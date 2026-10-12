# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Operator commands: ``backup``, ``restore``, ``db``, ``audit``, ``settings``, ``health``, ``diagnostics``, ``completion``.

``backup`` / ``restore`` / ``db`` act on the *local* installation (the data directory and its database) with the same engine the server uses
(:mod:`olcf.ops.backup_core`: encrypted, logical, database-agnostic); they work while the server is stopped and refuse to restore into a
running one.  ``diagnostics`` writes a redacted support bundle that contains no secrets, tokens, keys or database contents.
"""

from __future__ import annotations

import contextlib
import json
import os
import platform
import sys
import time
import zipfile
from pathlib import Path
from typing import Any

import click

from olcf.cli import doctor as checks
from olcf.cli.commands.lifecycle import embedded_state_dir, read_pidfile
from olcf.cli.common import confirm, pass_state, write_or_print, yes_option
from olcf.cli.output import human_bytes
from olcf.cli.state import State
from olcf.core.redaction import redact_text
from olcf.version import __version__

BACKUP_SUFFIX = ".olcfbak"


def _passphrase(required: bool, *, stdin: bool) -> str | None:
    if os.environ.get("OLCF_BACKUP_PASSPHRASE"):
        return os.environ["OLCF_BACKUP_PASSPHRASE"]
    if stdin:
        return sys.stdin.readline().rstrip("\n") or None
    if required and sys.stdin.isatty():
        return click.prompt("Backup passphrase", hide_input=True, confirmation_prompt=True)
    return None


def _keyspec(settings: Any, passphrase: str | None, *, for_reading: bool = False) -> Any:
    from olcf.ops.crypto import KeySpec
    from olcf.security.secrets import MasterKey

    master = None
    with contextlib.suppress(Exception):
        master = MasterKey(settings.path("secrets"), use_keyring=settings.security.use_keyring, explicit_file=settings.security.master_key_file)
    return KeySpec(passphrase=passphrase, master=master)


def _progress(state: State) -> Any:
    """Per-table progress lines only with -v; otherwise one line per quarter so long backups do not look hung."""
    last = {"q": -1}

    def cb(pct: int, msg: str) -> None:
        if state.verbose:
            state.out.detail(f"{pct:>3}% {msg}")
        elif pct // 25 != last["q"]:
            last["q"] = pct // 25
            state.out.detail(f"{pct:>3}%")

    return cb


def _db(settings: Any) -> Any:
    from olcf.db.engine import Database

    return Database(settings.database_url, echo=False, pool_size=settings.db.pool_size)


# --------------------------------------------------------------------------------------------- backup / restore
@click.group("backup", help="Encrypted, database-agnostic backups of the control plane (works offline).")
def backup_group() -> None:
    pass


@backup_group.command("create", help="Write an encrypted backup (default: <data>/backups/).  With a passphrase it can also carry the key files.")
@click.option("-o", "--out", "out_file", type=click.Path(dir_okay=False), default=None)
@click.option("--passphrase-stdin", is_flag=True, help="Read the passphrase from stdin (or set OLCF_BACKUP_PASSPHRASE).")
@click.option("--include-keys", is_flag=True, help="Also include master/signing keys (requires a passphrase; restores are self-contained).")
@pass_state
def backup_create(state: State, out_file: str | None, passphrase_stdin: bool, include_keys: bool) -> None:
    from olcf.core.ids import new_id
    from olcf.ops.backup_core import FileSpec, write_backup

    settings = state.settings(log_level="WARNING")
    pw = _passphrase(include_keys, stdin=passphrase_stdin)
    if include_keys and not pw:
        raise click.UsageError("--include-keys needs a passphrase (prompt, --passphrase-stdin or OLCF_BACKUP_PASSPHRASE)")
    if not (settings.data_dir / "olcf.db").exists() and settings.database_url.startswith("sqlite"):
        raise click.ClickException(f"no database in {settings.data_dir}; nothing to back up (run `olcf init` first)")
    path = Path(out_file) if out_file else settings.path("backups", f"olcf-{time.strftime('%Y%m%dT%H%M%S')}-v{__version__}{BACKUP_SUFFIX}")
    files: list[FileSpec] = []
    cfg = settings.data_dir / "config.toml"
    if cfg.is_file():
        files.append(FileSpec("config/config.toml", source=cfg))
    if include_keys:
        # the same set the server's scheduled backups carry: the secrets/* keys and the PKI (pki/ca.*, pki/server.*)
        from olcf.ops.backup import KEY_FILES

        for rel in KEY_FILES:
            f = settings.data_dir / rel
            if f.is_file():
                files.append(FileSpec(f"keys/{rel}", source=f))
    db = _db(settings)
    try:
        state.out.step(f"backing up {settings.database_url.split('@')[-1]} -> {path}")
        res = write_backup(db, path, _keyspec(settings, pw), meta={"backup_id": new_id("bkp"), "host": platform.node(), "kind": "cli"}, files=files,
                           progress=_progress(state))
    finally:
        db.dispose()
    doc = {"path": str(path), "bytes": res.size_bytes, "sha256": res.sha256, "rows": res.rows, "tables": len(res.tables), "encryption": res.header.get("kdf"),
           "files": sorted(res.manifest.get("files", {}))}
    if not pw:
        doc["note"] = "encrypted with this installation's master key (secrets/master.key): keep a copy of that key somewhere safe, or add --include-keys with a passphrase"

    def human(d: dict[str, Any]) -> None:
        state.out.success(f"backup written: {d['path']} ({human_bytes(d['bytes'])}, {d['rows']} rows, {d['tables']} tables)")
        state.out.kv([("sha256", d["sha256"]), ("encryption", d["encryption"]), ("files", ", ".join(d["files"]) or "-")])
        if d.get("note"):
            state.out.warn(d["note"])

    state.out.emit(doc, human)


@backup_group.command("list", help="Backups found in the backup directory.")
@pass_state
def backup_list(state: State) -> None:
    from olcf.ops.crypto import read_header

    settings = state.settings()
    rows = []
    for p in sorted(settings.path("backups").glob(f"*{BACKUP_SUFFIX}"), reverse=True):
        try:
            h = read_header(p)
        except Exception:  # noqa: BLE001 - listing must survive a damaged file
            h = {}
        rows.append({"file": p.name, "size": human_bytes(p.stat().st_size), "created": h.get("created_at"), "version": h.get("app_version"), "encryption": h.get("kdf")})
    state.out.table(rows, [("FILE", "file", 60), ("SIZE", "size"), ("CREATED", "created"), ("VERSION", "version"), ("KEY", "encryption")], empty="no backups yet (olcf backup create)")


@backup_group.command("verify", help="Decrypt and check every table and file hash of a backup, without applying anything.")
@click.argument("file", type=click.Path(exists=True, dir_okay=False))
@click.option("--passphrase-stdin", is_flag=True)
@pass_state
def backup_verify(state: State, file: str, passphrase_stdin: bool) -> None:
    from olcf.ops.backup_core import validate_backup

    settings = state.settings()
    pw = _passphrase(False, stdin=passphrase_stdin)
    rep = validate_backup(Path(file), _keyspec(settings, pw), progress=_progress(state))
    state.out.emit(rep.to_dict(), lambda d: (state.out.success if d["ok"] else state.out.error)(
        f"backup is {'valid' if d['ok'] else 'NOT valid'}: {d['rows']} rows, {d['tables']} tables" + ("" if d["ok"] else "; " + "; ".join(d["errors"][:3]))))
    if not rep.ok:
        raise SystemExit(7)


@click.command("restore", help="Restore a backup into the (stopped) local installation.  Replaces the control-plane database.")
@click.argument("file", type=click.Path(exists=True, dir_okay=False))
@click.option("--passphrase-stdin", is_flag=True)
@click.option("--overwrite-files", is_flag=True, help="Replace config/key files that already exist (default: keep them).")
@yes_option
@pass_state
def restore_cmd(state: State, file: str, passphrase_stdin: bool, overwrite_files: bool, yes: bool) -> None:
    from olcf.ops.backup_core import restore_backup

    settings = state.settings()
    if read_pidfile(settings.data_dir):
        raise click.ClickException("the server is running; stop it first (`olcf stop`) - an offline restore cannot share the database with a live server")
    pw = _passphrase(False, stdin=passphrase_stdin)
    confirm(state, f"Replace ALL control-plane data in {settings.database_url.split('@')[-1]} with the contents of {Path(file).name}?", yes)
    settings.ensure_dirs()
    db = _db(settings)
    try:
        # the archive paths map onto the data directory (config/config.toml -> config.toml, keys/secrets/master.key -> secrets/master.key, keys/pki/ca.key -> pki/ca.key);
        # existing files are kept unless --overwrite-files (the report says which were skipped)
        rep = restore_backup(Path(file), _keyspec(settings, pw), db, online=False, wipe=True, files_dir=settings.data_dir, overwrite_files=overwrite_files, progress=_progress(state),
                             reconcile_jobs=True, backup_id=Path(file).name)  # a stopped server has no live worker attempts: unfinished jobs are failed explicitly
    finally:
        db.dispose()
    placed = [str(settings.data_dir / f) for f in rep.files_written]
    doc = rep.to_dict() | {"files_placed": placed}

    def _restored(d: dict[str, Any]) -> None:
        state.out.success(f"restored {d['rows']} rows")
        for f in d["files_placed"]:
            state.out.detail(f"placed {f}")
        for w in d.get("warnings") or []:
            state.out.warn(w)
        jobs = d.get("jobs") or {}
        if jobs.get("failed"):
            state.out.warn(jobs["message"])
            for j in (jobs.get("jobs") or [])[:10]:
                state.out.detail(f"{j['id']}  {j['name']}  was {j['state_at_backup']}")
            if len(jobs.get("jobs") or []) > 10 or jobs.get("truncated"):
                state.out.detail(f"... and {jobs['failed'] - 10} more (see --json)")
        elif jobs:
            state.out.info(f"jobs: {jobs.get('message')}")
        if any(f.startswith("keys/secrets/master.key") for f in d.get("files_skipped") or []):
            state.out.warn("the backup carries a master key that was NOT applied (this installation has its own): stored secrets stay undecryptable until you re-run with --overwrite-files")
        state.out.info("start the server again with `olcf serve`")

    state.out.emit(doc, _restored)


# --------------------------------------------------------------------------------------------- db
@click.group("db", help="Database schema: status, migrations, rollback (local installation).")
def db_group() -> None:
    pass


@db_group.command("status")
@pass_state
def db_status(state: State) -> None:
    from olcf.db import migrate as dbm

    settings = state.settings()
    db = _db(settings)
    try:
        dbm.discover()
        cur = dbm.current_versions(db)
        rows = [{"stream": s, "current": cur.get(s, 0), "latest": max(v), "pending": max(v) - cur.get(s, 0)} for s, v in sorted(dbm._REGISTRY.items())]  # noqa: SLF001
    finally:
        db.dispose()

    def _streams(d: dict[str, Any]) -> None:
        state.out.info(f"database: {d['database']}")
        state.out.table(d["streams"], [("STREAM", "stream"), ("CURRENT", "current"), ("LATEST", "latest"), ("PENDING", "pending")])

    state.out.emit({"database": settings.database_url.split("@")[-1], "streams": rows}, _streams)


@db_group.command("migrate", help="Apply pending migrations (the server does this on start; use this to do it ahead of time).")
@pass_state
def db_migrate(state: State) -> None:
    settings = state.settings()
    settings.ensure_dirs()
    db = _db(settings)
    try:
        versions = db.migrate()
    finally:
        db.dispose()
    state.out.emit(versions, lambda d: state.out.success("schema is up to date: " + ", ".join(f"{k}=v{v}" for k, v in sorted(d.items()))))


@db_group.command("rollback", help="Reverse migrations of one stream down to VERSION (only where reversible).  Back up first.")
@click.argument("stream")
@click.argument("version", type=int)
@yes_option
@pass_state
def db_rollback(state: State, stream: str, version: int, yes: bool) -> None:
    from olcf.db import migrate as dbm

    settings = state.settings()
    if read_pidfile(settings.data_dir):
        raise click.ClickException("stop the server first (`olcf stop`)")
    confirm(state, f"Roll back schema stream '{stream}' to v{version}? Data in dropped tables is lost - take a backup first.", yes)
    db = _db(settings)
    try:
        to = dbm.rollback(db, stream, version)
    except RuntimeError as exc:
        raise click.ClickException(str(exc)) from None
    finally:
        db.dispose()
    state.out.emit({"stream": stream, "version": to}, lambda d: state.out.success(f"{d['stream']} is now at v{d['version']}"))


# --------------------------------------------------------------------------------------------- audit / settings / health
@click.group("audit", help="The tamper-evident audit log.")
def audit_group() -> None:
    pass


@audit_group.command("list")
@click.option("--action", default=None)
@click.option("--actor", default=None)
@click.option("--limit", type=int, default=50)
@pass_state
def audit_list(state: State, action: str | None, actor: str | None, limit: int) -> None:
    res = state.client.admin.audit(limit=limit, action=action, actor=actor)
    rows = res if isinstance(res, list) else res.get("items", [])
    state.out.table(rows, [("SEQ", "seq"), ("AT", "ts"), ("ACTOR", "actor_name"), ("ACTION", "action"), ("RESOURCE", "resource_id"), ("OUTCOME", "outcome"), ("IP", "ip")])


@audit_group.command("verify", help="Check the hash chain of the log (detects edited or removed records).")
@pass_state
def audit_verify(state: State) -> None:
    res = state.client.admin.verify_audit()
    state.out.emit(res, lambda d: (state.out.success if d.get("ok", True) else state.out.error)(f"audit chain {'intact' if d.get('ok', True) else 'BROKEN'} ({d.get('checked', '?')} records)"))
    if res.get("ok") is False:
        raise SystemExit(1)


@audit_group.command("export")
@click.option("--format", "fmt", type=click.Choice(["jsonl", "csv"]), default="jsonl")
@click.option("-o", "--out", "out_file", type=click.Path(dir_okay=False), default=None)
@pass_state
def audit_export(state: State, fmt: str, out_file: str | None) -> None:
    write_or_print(state, state.client.admin.export_audit(format=fmt), out_file)


@click.group("settings", help="Server-wide settings stored in the database.")
def settings_group() -> None:
    pass


@settings_group.command("list")
@pass_state
def settings_list(state: State) -> None:
    res = state.client.admin.settings()
    items = res.get("settings", res) if isinstance(res, dict) else {}
    rows = [{"setting": k, "current": v.get("value"), "default": v.get("default"), "description": v.get("description")} for k, v in sorted(items.items())]
    state.out.table(rows, [("SETTING", "setting", 38), ("VALUE", "current", 30), ("DEFAULT", "default", 20), ("DESCRIPTION", "description", 60)])


@settings_group.command("set")
@click.argument("key")
@click.argument("value")
@pass_state
def settings_set(state: State, key: str, value: str) -> None:
    from olcf.cli.common import parse_value

    state.out.emit(state.client.admin.set_setting(key, parse_value(value)), lambda d: state.out.success(f"{key} updated"))


@click.command("health", help="Server health checks (storage, database, compute, identity providers ...).")
@click.option("--force", is_flag=True, help="Run the checks now instead of using the cached result.")
@pass_state
def health_cmd(state: State, force: bool) -> None:
    rep = state.client.admin.health_report(force=force)
    rows = rep.get("components") or []

    def _health(d: dict[str, Any]) -> None:
        state.out.info(f"overall: {state.out.state(str(d.get('status', '?')))}")
        state.out.table(rows, [("CHECK", "name", 30), ("STATUS", "status"), ("CATEGORY", "category"), ("DETAIL", "detail", 70)])

    state.out.emit(rep, _health)
    if str(rep.get("status")) in ("down", "failed", "critical", "unhealthy"):
        raise SystemExit(1)


# --------------------------------------------------------------------------------------------- diagnostics / support bundle
def _redacted_config(path: Path) -> str:
    out = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        key = line.split("=", 1)[0].strip().lower()
        out.append(f"{line.split('=', 1)[0]}= \"[redacted]\"" if "=" in line and any(w in key for w in ("password", "secret", "token", "key", "url")) and not line.lstrip().startswith("#") else redact_text(line))
    return "\n".join(out) + "\n"


@click.command("diagnostics", help="Write a support bundle (zip) with versions, checks, redacted config and recent logs - no secrets, keys, tokens or data.")
@click.option("-o", "--out", "out_file", type=click.Path(dir_okay=False), default=None, help="Output file (default ./olcf-support-<time>.zip).")
@click.option("--log-lines", type=int, default=2000, show_default=True)
@pass_state
def diagnostics_cmd(state: State, out_file: str | None, log_lines: int) -> None:
    from importlib.metadata import distributions

    settings = state.settings()
    dd = settings.data_dir
    items = checks.run_local_checks(settings, server_running=bool(read_pidfile(dd)), state_dirs=[embedded_state_dir(dd)])
    out = Path(out_file) if out_file else Path.cwd() / f"olcf-support-{time.strftime('%Y%m%dT%H%M%S')}.zip"
    server: dict[str, Any] = {}
    try:
        c = state.client
        server["health"] = c.health()
        for k, fn in (("info", c.admin.info), ("topology", c.compute.topology)):
            with contextlib.suppress(Exception):
                server[k] = fn()
    except Exception as exc:  # noqa: BLE001
        server["unreachable"] = str(exc)[:200]
    info = {"olcf": __version__, "python": platform.python_version(), "platform": platform.platform(), "machine": platform.machine(), "created": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "data_dir": str(dd), "database": settings.database_url.split("@")[-1].split("://")[0], "env": {k: ("set" if os.environ.get(k) else "unset") for k in sorted(os.environ) if k.startswith("OLCF_") and "TOKEN" not in k and "PASSWORD" not in k and "KEY" not in k}}
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("info.json", json.dumps(info, indent=2, default=str))
        z.writestr("doctor.json", json.dumps([i.to_dict() for i in items], indent=2))
        z.writestr("server.json", redact_text(json.dumps(server, indent=2, default=str)))
        z.writestr("packages.txt", "\n".join(sorted(f"{d.metadata['Name']}=={d.version}" for d in distributions() if d.metadata["Name"])))
        if (dd / "config.toml").is_file():
            z.writestr("config.redacted.toml", _redacted_config(dd / "config.toml"))
        for log in sorted((dd / "logs").glob("*")) if (dd / "logs").is_dir() else []:
            if log.is_file() and log.stat().st_size:
                tail = log.read_bytes()[-2_000_000:].decode("utf-8", "replace").splitlines()[-log_lines:]
                z.writestr(f"logs/{log.name}", redact_text("\n".join(tail)))
    with contextlib.suppress(OSError):
        os.chmod(out, 0o600)

    def _written(d: dict[str, Any]) -> None:
        state.out.success(f"support bundle written: {d['path']} ({human_bytes(d['bytes'])})")
        state.out.info("It contains versions, check results, redacted configuration and recent logs - review it before sharing.")

    state.out.emit({"path": str(out), "bytes": out.stat().st_size}, _written)


# --------------------------------------------------------------------------------------------- completion
@click.command("completion", help="Print a shell completion script.  bash: eval \"$(olcf completion bash)\"   powershell: olcf completion powershell | Out-String | Invoke-Expression")
@click.argument("shell", type=click.Choice(["bash", "zsh", "fish", "powershell"]))
def completion_cmd(shell: str) -> None:
    from click.shell_completion import get_completion_class

    from olcf.cli.main import cli
    from olcf.cli.psfmt import register_powershell

    register_powershell()
    cls = get_completion_class(shell)
    if cls is None:
        raise click.ClickException(f"no completion support for shell '{shell}'")
    click.echo(cls(cli, {}, "olcf", "_OLCF_COMPLETE").source())


def register(group: click.Group) -> None:
    for cmd in (backup_group, restore_cmd, db_group, audit_group, settings_group, health_cmd, diagnostics_cmd, completion_cmd):
        group.add_command(cmd)
    from olcf.cli.commands import security

    security.register(group)
