# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""A password the operator chose is never echoed or stored by ``olcf init`` (a container prints this output into its log; the container CI job greps for it).

A generated password is still shown once and saved, because the operator has no other way to learn it.
"""

from __future__ import annotations

import json
import subprocess
import sys

from tests.tooling.conftest import cli_env, free_port

CHOSEN = "ci-only-Passw0rd-123"  # noqa: S105 - fixture value, not a secret


def _init(env, data, *extra, stdin=None):
    return subprocess.run([sys.executable, "-m", "olcf.cli.main", "--data-dir", str(data), "init", "--port", str(free_port()), *extra], env=env, input=stdin,
                          capture_output=True, text=True, timeout=180, check=False)


def test_a_password_from_stdin_is_not_echoed_or_written(tmp_path):
    data = tmp_path / "data"
    res = _init(cli_env(tmp_path, data), data, "--admin-password-stdin", stdin=CHOSEN + "\n")
    assert res.returncode == 0, res.stderr
    assert CHOSEN not in res.stdout + res.stderr
    assert "you supplied" in res.stdout + res.stderr
    assert not (data / "secrets" / "bootstrap-admin.txt").exists()
    assert not any(CHOSEN in p.read_text(errors="ignore") for p in data.rglob("*") if p.is_file() and p.stat().st_size < 1_000_000 and p.suffix in {".toml", ".txt", ".json", ".log"})


def test_a_password_from_the_environment_is_not_echoed_or_written_even_as_json(tmp_path):
    data = tmp_path / "data"
    env = {**cli_env(tmp_path, data), "OLCF_BOOTSTRAP_ADMIN_PASSWORD": CHOSEN}
    res = subprocess.run([sys.executable, "-m", "olcf.cli.main", "--json", "--data-dir", str(data), "init", "--port", str(free_port())], env=env, capture_output=True, text=True,
                         timeout=180, check=False)
    assert res.returncode == 0, res.stderr
    assert CHOSEN not in res.stdout + res.stderr
    doc = json.loads(res.stdout)
    assert doc["admin_created"] is True and "admin_password" not in doc
    assert not (data / "secrets" / "bootstrap-admin.txt").exists()


def test_a_generated_password_is_still_shown_once_and_saved(tmp_path):
    data = tmp_path / "data"
    res = _init(cli_env(tmp_path, data), data)
    assert res.returncode == 0, res.stderr
    saved = (data / "secrets" / "bootstrap-admin.txt").read_text()
    pw = saved.split("password=")[1].strip()
    assert pw and pw in res.stdout + res.stderr
