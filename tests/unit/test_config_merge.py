# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""FIX K: ``load_settings`` merges config.toml, the environment and explicit overrides KEY BY KEY (an env var of a section no longer drops the whole TOML table)."""

from __future__ import annotations

from pathlib import Path

import pytest

from olcf.core.config import load_settings

TOML = """
log_level = "DEBUG"

[server]
host = "10.1.2.3"
port = 9001
trusted_proxies = 2

[compute]
embedded_worker = false
reserve_cores = 3
"""


@pytest.fixture()
def data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    for k in list(__import__("os").environ):
        if k.startswith("OLCF_"):
            monkeypatch.delenv(k)
    (tmp_path / "config.toml").write_text(TOML)
    return tmp_path


def test_file_alone(data_dir):
    s = load_settings(data_dir)
    assert (s.server.host, s.server.port, s.server.trusted_proxies, s.log_level) == ("10.1.2.3", 9001, 2, "DEBUG")
    assert s.data_dir == data_dir.resolve()


def test_an_env_var_overrides_its_key_and_keeps_the_rest_of_the_table(data_dir, monkeypatch):
    monkeypatch.setenv("OLCF_SERVER__PORT", "9100")
    s = load_settings(data_dir)
    assert s.server.port == 9100                                  # environment wins over the file ...
    assert s.server.host == "10.1.2.3" and s.server.trusted_proxies == 2   # ... but only for the keys it names
    assert s.compute.reserve_cores == 3 and s.compute.embedded_worker is False and s.log_level == "DEBUG"


def test_several_sections_and_top_level_keys(data_dir, monkeypatch):
    monkeypatch.setenv("OLCF_SERVER__HOST", "0.0.0.0")  # noqa: S104 - value under test
    monkeypatch.setenv("OLCF_COMPUTE__RESERVE_CORES", "0")
    monkeypatch.setenv("OLCF_LOG_LEVEL", "ERROR")
    monkeypatch.setenv("OLCF_DB__URL", "sqlite:///x.db")
    s = load_settings(data_dir)
    assert (s.server.host, s.server.port) == ("0.0.0.0", 9001)  # noqa: S104
    assert s.compute.reserve_cores == 0 and s.compute.embedded_worker is False
    assert s.log_level == "ERROR" and s.db.url == "sqlite:///x.db"


def test_explicit_overrides_win_over_environment_and_file_key_by_key(data_dir, monkeypatch):
    monkeypatch.setenv("OLCF_SERVER__PORT", "9100")
    s = load_settings(data_dir, server={"port": 9200}, log_level="WARNING")
    assert s.server.port == 9200 and s.server.host == "10.1.2.3" and s.log_level == "WARNING"


def test_data_dir_argument_beats_the_environment(data_dir, monkeypatch, tmp_path_factory):
    monkeypatch.setenv("OLCF_DATA_DIR", str(tmp_path_factory.mktemp("other")))
    assert load_settings(data_dir).data_dir == data_dir.resolve()


def test_defaults_without_file_or_env(tmp_path, monkeypatch):
    for k in list(__import__("os").environ):
        if k.startswith("OLCF_"):
            monkeypatch.delenv(k)
    s = load_settings(tmp_path)
    assert s.server.port == 8470 and s.server.host == "127.0.0.1"
