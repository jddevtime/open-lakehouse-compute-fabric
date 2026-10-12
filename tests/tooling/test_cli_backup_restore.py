# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``olcf backup create --include-keys`` carries every key file (secrets and PKI) and ``olcf restore`` puts them (and config.toml) where the installation reads them."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from olcf.cli.main import cli

PASSPHRASE = "correct-horse-battery-1"  # noqa: S105 - test fixture passphrase, not a credential
KEYS = ("secrets/master.key", "secrets/jwt-signing.key", "pki/ca.key", "pki/ca.crt")


def _run(*args: str):
    res = CliRunner().invoke(cli, list(args), env={"OLCF_BACKUP_PASSPHRASE": PASSPHRASE}, catch_exceptions=False)
    assert res.exit_code == 0, res.output
    return res


def test_restore_places_config_and_all_key_files(tmp_path: Path) -> None:
    src, dst = tmp_path / "src", tmp_path / "dst"
    _run("--data-dir", str(src), "init")
    (src / "config.toml").write_text('[server]\nport = 9123\n')
    out = tmp_path / "b.olcfbak"
    made = json.loads(_run("--data-dir", str(src), "--json", "backup", "create", "--include-keys", "-o", str(out)).stdout)
    assert set(made["files"]) >= {"config/config.toml", *("keys/" + k for k in KEYS)}, made["files"]  # the PKI too, not only secrets/*

    _run("--data-dir", str(dst), "init")
    assert (dst / "secrets" / "master.key").read_bytes() != (src / "secrets" / "master.key").read_bytes()  # a fresh installation has its own keys
    rep = json.loads(_run("--data-dir", str(dst), "--json", "restore", str(out), "--yes", "--overwrite-files").stdout)
    assert set(rep["files_placed"]) >= {str(dst / "config.toml"), *(str(dst / k) for k in KEYS)}, rep
    for k in KEYS:
        assert (dst / k).read_bytes() == (src / k).read_bytes(), k
    assert 9123 == __import__("tomllib").loads((dst / "config.toml").read_text())["server"]["port"]
    assert oct((dst / "secrets" / "master.key").stat().st_mode & 0o777) == "0o600"


def test_restore_keeps_existing_files_unless_told_otherwise_and_says_so(tmp_path: Path) -> None:
    src, dst = tmp_path / "src", tmp_path / "dst"
    _run("--data-dir", str(src), "init")
    out = tmp_path / "b.olcfbak"
    _run("--data-dir", str(src), "backup", "create", "--include-keys", "-o", str(out))
    _run("--data-dir", str(dst), "init")
    own = (dst / "secrets" / "master.key").read_bytes()
    res = _run("--data-dir", str(dst), "restore", str(out), "--yes")
    assert (dst / "secrets" / "master.key").read_bytes() == own  # not overwritten
    assert "NOT applied" in res.output and "--overwrite-files" in res.output, res.output
