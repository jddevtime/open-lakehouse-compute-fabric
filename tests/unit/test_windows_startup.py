# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The application starts and every ``olcf`` module imports when the POSIX-only modules are missing (review finding F06).

``fcntl`` was imported unconditionally by ``notebooks/gitsync.py`` and ``notebooks/layer.py``, and the normal module installation imports both, so
the control plane could not start on Windows at all.  A child interpreter hides ``fcntl resource pwd grp termios pty tty syslog`` and the POSIX-only
``os``/``signal`` attributes (see ``windows_sim_child.py``), imports every module under ``olcf``, builds the app with all modules and runs its lifespan.
This is a simulation of the missing names on Linux/macOS, not a run on Windows; the CI matrix additionally runs this file on ``windows-latest``.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CHILD = Path(__file__).with_name("windows_sim_child.py")


def _child(mode: str, timeout: float = 240.0) -> dict:
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(ROOT / "src"), str(ROOT / "sdk" / "fabric"), os.environ.get("PYTHONPATH", "")]), "OLCF_LOG_LEVEL": "WARNING"}
    r = subprocess.run([sys.executable, "-P", str(CHILD), mode], capture_output=True, text=True, env=env, timeout=timeout, check=False, cwd=ROOT)
    assert r.returncode == 0, f"{mode}: exit {r.returncode}\n{r.stderr[-3000:]}"
    return json.loads(r.stdout.strip().splitlines()[-1])


def test_the_simulation_really_hides_the_posix_only_names() -> None:
    if os.name == "nt":
        pytest.skip("already Windows")
    got = _child("control")
    assert {k: v for k, v in got.items() if k in ("fcntl", "resource", "pwd", "grp", "termios", "pty", "tty", "syslog")} == dict.fromkeys(("fcntl", "resource", "pwd", "grp", "termios", "pty", "tty", "syslog"), "missing")
    assert got["os_attrs_present"] == [] and got["signal_attrs_present"] == []


def test_every_olcf_module_imports_like_it_does_normally() -> None:
    baseline = _child("baseline")
    windows_like = _child("imports")
    new_failures = {m: e for m, e in windows_like.items() if m not in baseline}
    assert not new_failures, "modules that import fine here but not without the POSIX-only modules:\n" + "\n".join(f"  {m}: {e}" for m, e in sorted(new_failures.items()))


def test_create_app_with_every_module_starts_and_serves_health() -> None:
    got = _child("app", timeout=300)
    assert got["health"] == 200 and got["ready"] == 200
    assert got["modules"] >= 10
    assert got["blocked_imported"] == [], "nothing may have imported a POSIX-only module while it was hidden"
