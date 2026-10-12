# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Child process of ``test_windows_startup.py``: behave like an interpreter that has no POSIX-only modules and attributes, then import and start OLCF.

Run as ``python -P windows_sim_child.py {imports|app}`` with ``PYTHONPATH`` containing ``src``.  It prints one JSON object on its last line.

What "Windows-like" means here (all of it is absent on a real Windows Python): the modules ``fcntl resource pwd grp termios pty tty syslog``
(importing one raises ``ModuleNotFoundError``) and the ``os``/``signal`` attributes ``fork killpg getpgid setsid getuid geteuid getloadavg ...`` and
``SIGKILL SIGHUP SIGALRM ...``.  ``psutil`` is imported *before* the blocking starts because on this host it loads its Linux backend, which legitimately
needs ``resource``; on Windows psutil ships a different backend.  This is a focused simulation of the missing names, not a Windows run.
"""

from __future__ import annotations

import importlib
import importlib.abc
import json
import os
import pkgutil
import signal
import sys
import tempfile
from pathlib import Path

import psutil  # noqa: F401 - loaded before the POSIX-only modules are hidden (see the module docstring)

BLOCKED_MODULES = ("fcntl", "resource", "pwd", "grp", "termios", "pty", "tty", "syslog")
MISSING_OS = ("fork", "forkpty", "killpg", "getpgid", "getpgrp", "setsid", "getuid", "geteuid", "getgid", "getegid", "getloadavg", "chown", "mkfifo", "setuid", "setgid", "nice")
MISSING_SIGNALS = ("SIGKILL", "SIGHUP", "SIGALRM", "SIGUSR1", "SIGUSR2", "SIGQUIT", "SIGPIPE", "SIGCHLD", "SIGSTOP", "SIGCONT", "alarm", "setitimer", "pthread_kill")


class _Block(importlib.abc.MetaPathFinder):
    def find_spec(self, name, path=None, target=None):  # noqa: ANN001, ANN201
        if name.split(".")[0] in BLOCKED_MODULES:
            raise ModuleNotFoundError(f"No module named {name!r}", name=name)
        return None


def make_windows_like() -> None:
    for m in list(sys.modules):
        if m.split(".")[0] in BLOCKED_MODULES:
            del sys.modules[m]
    sys.meta_path.insert(0, _Block())
    for a in MISSING_OS:
        if hasattr(os, a):
            delattr(os, a)
    for a in MISSING_SIGNALS:
        if hasattr(signal, a):
            delattr(signal, a)


def import_everything() -> dict[str, str]:
    import olcf

    failures: dict[str, str] = {}
    for mi in pkgutil.walk_packages(olcf.__path__, "olcf."):
        if mi.name.endswith(".__main__"):
            continue  # executing a __main__ would run the CLI
        try:
            importlib.import_module(mi.name)
        except BaseException as exc:  # noqa: BLE001
            failures[mi.name] = f"{type(exc).__name__}: {exc}"
    return failures


def start_app() -> dict[str, object]:
    from fastapi.testclient import TestClient

    from olcf.api.app import create_app
    from olcf.core.config import load_settings

    data = Path(tempfile.mkdtemp(prefix="olcf-winsim-"))
    s = load_settings(data / "data", environment="test")
    s.compute.embedded_worker = False
    app = create_app(s)  # every module of the default list
    with TestClient(app) as c:
        health = c.get("/v1/health").status_code
        ready = c.get("/v1/health/ready").status_code
    return {"modules": len(app.state.ctx.modules), "health": health, "ready": ready, "blocked_imported": sorted(m for m in sys.modules if m.split(".")[0] in BLOCKED_MODULES)}


def control() -> dict[str, object]:
    """Prove the simulation really hides things (a green run is only meaningful if the probes below fail as they would on Windows)."""
    out: dict[str, object] = {}
    for m in BLOCKED_MODULES:
        try:
            importlib.import_module(m)
            out[m] = "importable"
        except ModuleNotFoundError:
            out[m] = "missing"
    out["os_attrs_present"] = [a for a in MISSING_OS if hasattr(os, a)]
    out["signal_attrs_present"] = [a for a in MISSING_SIGNALS if hasattr(signal, a)]
    return out


def main() -> None:
    mode = sys.argv[1]
    if mode != "baseline":
        make_windows_like()
    result = {"baseline": import_everything, "imports": import_everything, "app": start_app, "control": control}[mode]()
    print(json.dumps(result))


if __name__ == "__main__":
    main()
