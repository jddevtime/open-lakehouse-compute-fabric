# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``olcf service ...`` (start at login / boot) and ``olcf upgrade`` / ``olcf update``.

Service managers are driven through their own command line tools (``launchctl``, ``systemctl``, ``schtasks``) with argument lists, never a
shell.  Upgrades use the installer's versioned layout (see :mod:`olcf.cli.upgrade`).
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

import click

from olcf.cli import service as svc
from olcf.cli import upgrade as upg
from olcf.cli.commands.compute import launcher_command
from olcf.cli.commands.lifecycle import find_install_info, read_pidfile
from olcf.cli.common import confirm, pass_state, yes_option
from olcf.cli.state import State


def _platform() -> str:
    return "macos" if sys.platform == "darwin" else "windows" if os.name == "nt" else "linux"


def _spec(state: State, host: str | None, port: int | None, system: bool, user: str | None, extra: list[str]) -> svc.ServiceSpec:
    dd = state.data_path()
    return svc.ServiceSpec(command=svc.serve_command(launcher_command(), dd, host=host, port=port, extra=extra), data_dir=dd, log_dir=dd / "logs", system=system,
                           user=user or (os.environ.get("SUDO_USER") if system else None), group=None)


def _run(state: State, argv: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    if state.verbose:
        state.out.detail("$ " + svc.describe(argv))
    res = subprocess.run(argv, capture_output=True, text=True, check=False, stdin=subprocess.DEVNULL, timeout=120)  # noqa: S603
    if check and res.returncode != 0:
        raise click.ClickException(f"`{svc.describe(argv)}` failed ({res.returncode}): {(res.stderr or res.stdout).strip()[:400]}")
    return res


def _tool(name: str, fallback: str | None = None) -> str:
    found = shutil.which(name) or (fallback if fallback and Path(fallback).exists() else None)
    if not found:
        raise click.ClickException(f"`{name}` was not found on this machine")
    return found


@click.group("service", help="Run OLCF automatically at login (macOS LaunchAgent, Windows scheduled task) or at boot (Linux systemd).")
def service_group() -> None:
    pass


_opts = [
    click.option("--host", default=None, help="Bind address for the service."),
    click.option("--port", type=int, default=None),
    click.option("--system", is_flag=True, help="Linux: install a system-wide unit (needs root) instead of a per-user unit."),
    click.option("--user", "run_as", default=None, help="Linux system unit: account to run as (default: $SUDO_USER)."),
]


def _with_opts(f: Any) -> Any:
    for o in reversed(_opts):
        f = o(f)
    return f


@service_group.command("print", help="Show the service definition for this (or --platform) system without installing anything.")
@click.option("--platform", "plat", type=click.Choice(["macos", "linux", "windows"]), default=None)
@_with_opts
@pass_state
def service_print(state: State, plat: str | None, host: str | None, port: int | None, system: bool, run_as: str | None) -> None:
    spec = _spec(state, host, port, system, run_as, [])
    plat = plat or _platform()
    text = svc.render_launchd(spec).decode() if plat == "macos" else svc.render_systemd(spec) if plat == "linux" else svc.render_windows_task(spec)
    state.out.emit({"platform": plat, "definition": text}, lambda d: print(d["definition"], file=state.out.out))


@service_group.command("install", help="Install and (by default) start the service.")
@click.option("--start/--no-start", default=True)
@_with_opts
@pass_state
def service_install(state: State, start: bool, host: str | None, port: int | None, system: bool, run_as: str | None) -> None:
    plat = _platform()
    spec = _spec(state, host, port, system, run_as, [])
    spec.log_dir.mkdir(parents=True, exist_ok=True)
    if read_pidfile(state.data_path()):
        state.out.warn("a server started by `olcf serve --daemon` is running; stop it (`olcf stop`) so the service can take over the port")
    home = Path.home()
    if plat == "macos":
        path = svc.launchd_path(home)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(svc.render_launchd(spec))
        uid = str(os.getuid())
        _run(state, [_tool("launchctl", "/bin/launchctl"), "bootout", f"gui/{uid}/{svc.LABEL}"], check=False)
        _run(state, [_tool("launchctl", "/bin/launchctl"), "bootstrap", f"gui/{uid}", str(path)])
        if start:
            _run(state, [_tool("launchctl", "/bin/launchctl"), "kickstart", "-k", f"gui/{uid}/{svc.LABEL}"], check=False)
        where = str(path)
    elif plat == "linux":
        if system and os.geteuid() != 0:
            raise click.ClickException("--system needs root: re-run with sudo (and --user NAME for the account to run as)")
        if system and not spec.user:
            raise click.ClickException("--system needs --user NAME (an unprivileged account that owns the data directory)")
        path = svc.systemd_path(home, system)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(svc.render_systemd(spec), encoding="utf-8")
        ctl = [_tool("systemctl")] + ([] if system else ["--user"])
        _run(state, [*ctl, "daemon-reload"])
        _run(state, [*ctl, "enable", *(["--now"] if start else []), svc.UNIT_NAME])
        where = str(path)
        if not system:
            state.out.info("To start it at boot without logging in:  loginctl enable-linger $USER   (needs permission once)")
    else:
        xml = svc.render_windows_task(spec, user=os.environ.get("USERNAME", ""))
        with tempfile.NamedTemporaryFile("w", suffix=".xml", delete=False, encoding="utf-16") as fh:
            fh.write(xml)
            tmp = fh.name
        try:
            _run(state, [_tool("schtasks"), "/Create", "/TN", svc.TASK_NAME, "/XML", tmp, "/F"])
        finally:
            Path(tmp).unlink(missing_ok=True)
        if start:
            _run(state, [_tool("schtasks"), "/Run", "/TN", svc.TASK_NAME], check=False)
        where = f"scheduled task '{svc.TASK_NAME}'"
    state.out.emit({"platform": plat, "installed": where, "started": start}, lambda d: state.out.success(f"service installed ({d['installed']}); manage it with `olcf service status|stop|start|uninstall`"))


def _manager_cmd(plat: str, action: str, system: bool = False) -> list[str]:
    if plat == "macos":
        uid = str(os.getuid())
        t = _tool("launchctl", "/bin/launchctl")
        return {"start": [t, "kickstart", f"gui/{uid}/{svc.LABEL}"], "stop": [t, "kill", "SIGTERM", f"gui/{uid}/{svc.LABEL}"], "status": [t, "print", f"gui/{uid}/{svc.LABEL}"],
                "restart": [t, "kickstart", "-k", f"gui/{uid}/{svc.LABEL}"]}[action]
    if plat == "linux":
        ctl = [_tool("systemctl")] + ([] if system else ["--user"])
        return [*ctl, action if action != "status" else "status", svc.UNIT_NAME] + (["--no-pager"] if action == "status" else [])
    t = _tool("schtasks")
    return {"start": [t, "/Run", "/TN", svc.TASK_NAME], "stop": [t, "/End", "/TN", svc.TASK_NAME], "status": [t, "/Query", "/TN", svc.TASK_NAME, "/V", "/FO", "LIST"],
            "restart": [t, "/Run", "/TN", svc.TASK_NAME]}[action]


for _name in ("start", "stop", "restart", "status"):
    def _make(name: str) -> None:
        @service_group.command(name, help=f"{name.capitalize()} the installed service.")
        @click.option("--system", is_flag=True, help="Linux: the system-wide unit.")
        @pass_state
        def cmd(state: State, system: bool) -> None:
            plat = _platform()
            if plat == "windows" and name == "restart":
                _run(state, _manager_cmd(plat, "stop"), check=False)
            res = _run(state, _manager_cmd(plat, name, system), check=(name != "status"))
            text = (res.stdout or res.stderr).strip()
            state.out.emit({"action": name, "ok": res.returncode == 0, "output": text if name == "status" else ""},
                           lambda d: print(d["output"], file=state.out.out) if name == "status" else state.out.success(f"service {name} requested"))
            if name == "status" and res.returncode != 0:
                raise SystemExit(3)
    _make(_name)


@service_group.command("uninstall", help="Stop and remove the service (data is kept).")
@click.option("--system", is_flag=True)
@pass_state
def service_uninstall(state: State, system: bool) -> None:
    plat = _platform()
    home = Path.home()
    if plat == "macos":
        path = svc.launchd_path(home)
        _run(state, [_tool("launchctl", "/bin/launchctl"), "bootout", f"gui/{os.getuid()}/{svc.LABEL}"], check=False)
        path.unlink(missing_ok=True)
    elif plat == "linux":
        ctl = [_tool("systemctl")] + ([] if system else ["--user"])
        _run(state, [*ctl, "disable", "--now", svc.UNIT_NAME], check=False)
        svc.systemd_path(home, system).unlink(missing_ok=True)
        _run(state, [*ctl, "daemon-reload"], check=False)
    else:
        _run(state, [_tool("schtasks"), "/End", "/TN", svc.TASK_NAME], check=False)
        _run(state, [_tool("schtasks"), "/Delete", "/TN", svc.TASK_NAME, "/F"], check=False)
    state.out.emit({"removed": True}, lambda d: state.out.success("service removed"))


# --------------------------------------------------------------------------------------------- upgrade
@click.command("upgrade", help="Install a newer version next to the current one, migrate the database and switch (the previous version stays for rollback).")
@click.option("--version", "version", default=None, help="Version to install from the package index (e.g. 0.2.0).")
@click.option("--wheel", type=click.Path(exists=True, dir_okay=False), default=None, help="Install from a local olcf-*.whl instead.")
@click.option("--index-url", default=None, envvar="OLCF_INDEX_URL")
@click.option("--find-links", multiple=True, help="Directory/URL with wheels (offline or private mirror).")
@click.option("--rollback", is_flag=True, help="Switch back to the previous version.")
@click.option("--list", "list_versions", is_flag=True, help="Show installed versions.")
@click.option("--no-backup", is_flag=True, help="Skip the pre-upgrade backup (not recommended).")
@click.option("--no-restart", is_flag=True, help="Do not stop/start a running server.")
@click.option("--keep", type=int, default=2, show_default=True, help="Versions to keep installed (current + previous are always kept).")
@yes_option
@pass_state
def upgrade_cmd(state: State, version: str | None, wheel: str | None, index_url: str | None, find_links: tuple[str, ...], rollback: bool, list_versions: bool, no_backup: bool,
                no_restart: bool, keep: int, yes: bool) -> None:
    info = find_install_info()
    layout = upg.Layout.from_info(info)
    if layout is None:
        raise click.ClickException("this OLCF was not installed by the installer script (no versioned layout found), so it cannot upgrade itself.\n"
                                   "  pip/uv installs: `uv pip install -U olcf` (then `olcf db migrate`);  Docker: pull the new image tag;  Helm: `helm upgrade`.")
    if list_versions:
        rows = layout.describe()
        state.out.table(rows, [("VERSION", "version"), ("CURRENT", "current"), ("PREVIOUS", "previous"), ("INSTALLED", "installed_at")])
        return
    plan = upg.Upgrader(layout, state.out, state.data_path(), index_url=index_url, find_links=list(find_links))
    if rollback:
        confirm(state, f"Switch back to {layout.previous() or '(no previous version)'}?", yes)
        res = plan.rollback(restart=not no_restart)
    else:
        if not version and not wheel:
            raise click.UsageError("say what to install: --version X.Y.Z or --wheel FILE")
        confirm(state, "Upgrade now? A running server is stopped briefly and a backup is taken first.", yes)
        res = plan.upgrade(version=version, wheel=wheel, backup=not no_backup, restart=not no_restart, keep=keep)
    state.out.emit(res, lambda d: state.out.success(d["message"]))


def register(group: click.Group) -> None:
    group.add_command(service_group)
    group.add_command(upgrade_cmd)
    group.add_command(upgrade_cmd, "update")
