# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Versioned installation layout and the upgrade / rollback procedure behind ``olcf upgrade``.

Layout written by ``scripts/install.sh`` / ``install.ps1`` (``<prefix>`` is ``~/.local/share/olcf`` or ``%LOCALAPPDATA%\\olcf``)::

    <prefix>/install.json          what is installed (schema 1): version, previous, python, installed_at, data_dir ...
    <prefix>/bin/uv                the uv binary the installer bootstrapped
    <prefix>/python/               uv-managed Python builds (UV_PYTHON_INSTALL_DIR) - never a system / Xcode Python
    <prefix>/versions/<v>/venv/    one virtual environment per installed version
    <prefix>/current               symlink (Windows: junction) to versions/<current>

Upgrade = new venv + wheel-only install + smoke check + pre-upgrade backup + database migration with the *new* code + atomic swap of
``current``.  The previous version stays on disk; ``rollback`` swaps back.  Every step is a subprocess with an argument list (no shell) and
runs with ``UV_PYTHON_PREFERENCE=only-managed`` so uv can never pick the macOS ``/usr/bin/python3`` installer stub.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import click

from olcf.cli.output import Output

INSTALL_JSON = "install.json"
SCHEMA = 1


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


@dataclass
class Layout:
    """The versioned layout of an installation made by the installer: the prefix directory and the install record, with its ``versions`` directory."""
    prefix: Path
    info: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_info(cls, info: dict[str, Any] | None) -> Layout | None:
        if not info or not info.get("prefix"):
            return None
        p = Path(info["prefix"])
        return cls(p, info) if (p / "versions").is_dir() else None

    @property
    def versions_dir(self) -> Path:
        return self.prefix / "versions"

    @property
    def current_link(self) -> Path:
        return self.prefix / "current"

    def venv(self, version: str) -> Path:
        return self.versions_dir / version / "venv"

    def python(self, version: str) -> Path:
        v = self.venv(version)
        return v / "Scripts" / "python.exe" if os.name == "nt" else v / "bin" / "python"

    def uv(self) -> str:
        cand = self.prefix / "bin" / ("uv.exe" if os.name == "nt" else "uv")
        if cand.exists():
            return str(cand)
        found = shutil.which("uv")
        if not found:
            raise click.ClickException(f"uv was not found ({cand} is missing); re-run the installer to repair the installation")
        return found

    def installed(self) -> list[str]:
        return sorted((d.name for d in self.versions_dir.iterdir() if (d / "venv").is_dir()), key=version_key) if self.versions_dir.is_dir() else []

    def current(self) -> str | None:
        try:
            return Path(os.path.realpath(self.current_link)).name if self.current_link.exists() else None
        except OSError:
            return None

    def previous(self) -> str | None:
        prev = self.info.get("previous")
        return prev if prev and (self.versions_dir / prev / "venv").is_dir() else None

    def describe(self) -> list[dict[str, Any]]:
        cur, prev = self.current(), self.previous()
        return [{"version": v, "current": v == cur, "previous": v == prev, "installed_at": _mtime(self.versions_dir / v)} for v in self.installed()]

    def save(self) -> None:
        self.info.update(schema=SCHEMA, prefix=str(self.prefix), version=self.current() or self.info.get("version"))
        tmp = self.prefix / (INSTALL_JSON + ".tmp")
        tmp.write_text(json.dumps(self.info, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(tmp, self.prefix / INSTALL_JSON)


def _mtime(p: Path) -> str:
    try:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(p.stat().st_mtime))
    except OSError:
        return "-"


def version_key(v: str) -> tuple[Any, ...]:
    return tuple((0, int(x)) if x.isdigit() else (1, x) for x in re.split(r"[.\-+]", v))


def set_link(link: Path, target: Path) -> None:
    """Point ``link`` at ``target``: atomic on POSIX (rename over the old link), junction swap on Windows."""
    if os.name == "nt":
        if link.exists() or link.is_symlink():
            with contextlib.suppress(OSError):
                os.rmdir(link)
        subprocess.run(["cmd", "/c", "mklink", "/J", str(link), str(target)], check=True, capture_output=True)  # noqa: S603,S607
        return
    tmp = link.with_name(link.name + ".new")
    with contextlib.suppress(FileNotFoundError):
        tmp.unlink()
    tmp.symlink_to(target)
    os.replace(tmp, link)


def wheel_version(path: str | Path) -> str:
    m = re.match(r"^olcf-([^-]+)-", Path(path).name)
    if not m:
        raise click.ClickException(f"'{Path(path).name}' is not an olcf wheel (expected olcf-<version>-py3-none-any.whl)")
    return m.group(1)


class Upgrader:
    """Performs ``olcf upgrade``: installs a new version beside the current one, backs up and migrates the database, switches over and can roll back.
    """
    def __init__(self, layout: Layout, out: Output, data_dir: Path, *, index_url: str | None = None, find_links: list[str] | None = None) -> None:
        self.layout, self.out, self.data_dir = layout, out, data_dir
        self.index_url, self.find_links = index_url, find_links or []

    # ------------------------------------------------------------------ plumbing
    def env(self) -> dict[str, str]:
        e = dict(os.environ)
        e.update(UV_PYTHON_INSTALL_DIR=str(self.layout.prefix / "python"), UV_CACHE_DIR=os.environ.get("OLCF_CACHE_DIR") or str(self.layout.prefix / "cache"), UV_PYTHON_PREFERENCE="only-managed",
                 UV_NO_PROGRESS="1", OLCF_DATA_DIR=str(self.data_dir))
        return e

    def run(self, argv: list[str], what: str, *, quiet: bool = True) -> subprocess.CompletedProcess[str]:
        self.out.detail(what)
        res = subprocess.run(argv, env=self.env(), capture_output=True, text=True, check=False, stdin=subprocess.DEVNULL)  # noqa: S603
        if res.returncode != 0:
            tail = "\n".join((res.stderr or res.stdout).strip().splitlines()[-12:])
            raise click.ClickException(f"{what} failed (exit {res.returncode}):\n{tail}")
        return res

    def server_info(self) -> dict[str, Any] | None:
        from olcf.cli.commands.lifecycle import read_pidfile

        return read_pidfile(self.data_dir)

    def stop_server(self) -> None:
        from olcf.cli.commands.lifecycle import stop_running

        stop_running(self.data_dir, grace=45.0, force=True)

    def start_server(self, version: str, info: dict[str, Any]) -> None:
        argv = [str(self.layout.python(version)), "-m", "olcf.cli.main", "--data-dir", str(self.data_dir), "-q", "serve", "--daemon", "--host", str(info.get("host", "127.0.0.1")),
                "--port", str(info.get("port", 8470))]
        self.run(argv, "starting the server with the new version")

    # ------------------------------------------------------------------ steps
    def build_version(self, version: str, *, wheel: str | None) -> None:
        dest = self.layout.versions_dir / version
        venv = dest / "venv"
        if venv.is_dir():
            shutil.rmtree(dest)
        dest.mkdir(parents=True)
        uv = self.layout.uv()
        pyver = str(self.layout.info.get("python", "3.13"))
        try:
            self.run([uv, "python", "install", pyver], f"making sure Python {pyver} (uv-managed) is present")
            self.run([uv, "venv", "--python", pyver, "--python-preference", "only-managed", str(venv)], f"creating {venv}")
            spec = [str(wheel)] if wheel else [f"olcf=={version}"]
            argv = [uv, "pip", "install", "--python", str(self.layout.python(version)), "--only-binary", ":all:", *spec]
            if self.index_url:
                argv += ["--index-url", self.index_url]
            for fl in self.find_links:
                argv += ["--find-links", fl]
            self.run(argv, f"installing olcf {version} (wheels only; nothing is compiled)")
            res = self.run([str(self.layout.python(version)), "-m", "olcf.cli.main", "--json", "version"], "checking the new installation")
            got = json.loads(res.stdout).get("olcf")
            if got != version:
                raise click.ClickException(f"the installed package reports version {got}, expected {version}")
        except BaseException:
            shutil.rmtree(dest, ignore_errors=True)
            raise

    def backup(self, version: str) -> str | None:
        if not (self.data_dir / "olcf.db").exists() and not os.environ.get("OLCF_DB__URL"):
            return None
        out = self.data_dir / "backups" / f"pre-upgrade-{self.layout.current()}-to-{version}-{time.strftime('%Y%m%dT%H%M%S')}.olcfbak"
        py = self.layout.python(self.layout.current() or "")
        self.run([str(py), "-m", "olcf.cli.main", "--data-dir", str(self.data_dir), "-q", "backup", "create", "-o", str(out)], "backing up the database before migrating")
        return str(out)

    def migrate(self, version: str) -> None:
        self.run([str(self.layout.python(version)), "-m", "olcf.cli.main", "--data-dir", str(self.data_dir), "-q", "db", "migrate"], "migrating the database with the new version")

    def prune(self, keep: int, protect: set[str]) -> list[str]:
        vs = self.layout.installed()
        removable = [v for v in vs if v not in protect]
        extra = max(0, len(vs) - max(keep, len(protect)))
        removed = []
        for v in removable[:extra]:
            shutil.rmtree(self.layout.versions_dir / v, ignore_errors=True)
            removed.append(v)
        return removed

    # ------------------------------------------------------------------ public
    def upgrade(self, *, version: str | None, wheel: str | None, backup: bool, restart: bool, keep: int) -> dict[str, Any]:
        target = wheel_version(wheel) if wheel else str(version)
        cur = self.layout.current()
        if cur == target:
            return {"changed": False, "version": target, "message": f"already running {target}"}
        running = self.server_info() if restart else None
        self.out.step(f"installing {target} next to {cur}")
        if (self.layout.versions_dir / target / "venv").is_dir() and not wheel:
            self.out.detail(f"{target} is already installed; switching to it")
        else:
            self.build_version(target, wheel=wheel)
        backup_path = None
        if running:
            self.out.step("stopping the server")
            self.stop_server()
        if backup:
            self.out.step("backing up")
            backup_path = self.backup(target)
        try:
            self.out.step("migrating the database")
            self.migrate(target)
        except click.ClickException as exc:
            if running:
                self.start_server(cur or target, running)
            raise click.ClickException(f"{exc.message}\nThe old version ({cur}) stays active." + (f" Backup: {backup_path}" if backup_path else "")) from None
        self.out.step("switching")
        set_link(self.layout.current_link, self.layout.versions_dir / target)
        self.layout.info.update(previous=cur, upgraded_at=now_iso(), last_backup=backup_path)
        self.layout.info.setdefault("history", []).append({"from": cur, "to": target, "at": now_iso(), "backup": backup_path})
        self.layout.save()
        removed = self.prune(keep, {target, cur or ""})
        if running:
            self.out.step("starting the server")
            self.start_server(target, running)
        msg = f"upgraded {cur} -> {target}" + (f" (backup: {backup_path})" if backup_path else "") + "; roll back with `olcf upgrade --rollback`"
        return {"changed": True, "from": cur, "version": target, "backup": backup_path, "removed": removed, "message": msg}

    def rollback(self, *, restart: bool) -> dict[str, Any]:
        prev, cur = self.layout.previous(), self.layout.current()
        if not prev:
            raise click.ClickException("there is no previous version to go back to (see `olcf upgrade --list`)")
        running = self.server_info() if restart else None
        if running:
            self.out.step("stopping the server")
            self.stop_server()
        set_link(self.layout.current_link, self.layout.versions_dir / prev)
        self.layout.info.update(previous=cur, rolled_back_at=now_iso())
        self.layout.save()
        backup = self.layout.info.get("last_backup")
        if running:
            self.start_server(prev, running)
        msg = f"switched back {cur} -> {prev}."
        if backup:
            msg += f" If {cur} migrated the database and {prev} refuses to start, restore the pre-upgrade backup: olcf restore {backup}"
        return {"changed": True, "from": cur, "version": prev, "backup": backup, "message": msg}
