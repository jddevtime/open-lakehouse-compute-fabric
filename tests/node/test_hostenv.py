# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Host discovery (Python, Java) and the Temurin download with SHA-256 verification against a local fake Adoptium API."""

from __future__ import annotations

import copy
import json
import os
import stat
import sys
from pathlib import Path

import pytest

from olcf.runtime import hostenv
from olcf.runtime.errors import ChecksumMismatch, PrerequisiteMissing
from olcf.runtime.manager import ManagerConfig, RuntimeManager, bundled_manifest_paths
from olcf.runtime.manifest import RuntimeManifest
from tests.node.fakes import FakeAdoptium


def test_temurin_download_is_verified_and_extracted(tmp_path: Path) -> None:
    fake = FakeAdoptium(tmp_path / "srv", major=21)
    with fake.serve() as api:
        info = hostenv.install_temurin(21, tmp_path / "jdk", api_base=api, os_name="linux", arch="x64")
    assert info.major == 21 and info.vendor == "Eclipse Adoptium"
    assert Path(info.java).is_file() and os.access(info.java, os.X_OK)
    meta = json.loads((tmp_path / "jdk" / "21" / "OLCF-JDK.json").read_text())
    assert meta["sha256"] == fake.sha256 and meta["release"] == "jdk-21.0.8+9"
    assert not [p for p in (tmp_path / "jdk").iterdir() if p.name.startswith(".jdk")], "staging directory must be cleaned up"


def test_temurin_checksum_mismatch_fails_closed(tmp_path: Path) -> None:
    fake = FakeAdoptium(tmp_path / "srv", major=21, tamper=True)
    with fake.serve() as api, pytest.raises(ChecksumMismatch) as ei:
        hostenv.install_temurin(21, tmp_path / "jdk", api_base=api, os_name="linux", arch="x64")
    assert fake.sha256 in ei.value.message and "Do not use this download" in ei.value.remediation
    assert not (tmp_path / "jdk" / "21").exists(), "a tampered JDK must never be installed"


def test_temurin_api_unreachable_is_actionable(tmp_path: Path) -> None:
    with pytest.raises(PrerequisiteMissing) as ei:
        hostenv.install_temurin(21, tmp_path / "jdk", api_base="http://127.0.0.1:1", os_name="linux", arch="x64")
    assert "Adoptium" in ei.value.message and "JAVA_HOME" in ei.value.remediation


def test_manager_downloads_jdk_only_with_flag(tmp_path: Path) -> None:
    """RuntimeManager._jdk: a missing Java fails with an actionable error; ``with_jdk`` downloads the verified Temurin build."""
    doc = json.loads(next(p for p in bundled_manifest_paths() if p.stem == "olcf-rt-native-1.0.0").read_text())
    doc = copy.deepcopy(doc)
    doc["java"] = {"major": 99, "supported": [99]}  # no machine has such a JDK (CI runners ship Java 8 to 25)
    m = RuntimeManifest.from_dict(doc, validate=False)
    mgr = RuntimeManager(ManagerConfig(state_dir=tmp_path / "state"))
    with pytest.raises(PrerequisiteMissing) as ei:
        mgr._jdk(m, lambda *a: None)  # noqa: SLF001
    assert "--with-jdk" in ei.value.remediation
    fake = FakeAdoptium(tmp_path / "srv", major=99)
    steps: list[str] = []
    with fake.serve() as api:
        mgr2 = RuntimeManager(ManagerConfig(state_dir=tmp_path / "state", with_jdk=True, adoptium_api=api))
        # the fake serves linux/x64; pin the platform the same way the real call would resolve it
        orig = hostenv._adoptium_os_arch  # noqa: SLF001
        hostenv._adoptium_os_arch = lambda: ("linux", "x64")  # type: ignore[assignment]  # noqa: SLF001
        try:
            info = mgr2._jdk(m, lambda step, detail: steps.append(step))  # noqa: SLF001
        finally:
            hostenv._adoptium_os_arch = orig  # type: ignore[assignment]  # noqa: SLF001
    assert info.major == 99 and "jdk" in steps
    assert Path(info.home).is_relative_to(tmp_path / "state" / "runtimes" / "_jdk")


def test_java_discovery_honours_java_home_and_checks_the_major(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fake = FakeAdoptium(tmp_path / "srv", major=17)
    with fake.serve() as api:
        info = hostenv.install_temurin(17, tmp_path / "jdk", api_base=api, os_name="linux", arch="x64")
    monkeypatch.setenv("JAVA_HOME", info.home)
    assert any(j.major == 17 and j.home == info.home for j in hostenv.discover_javas())
    assert hostenv.find_java([17], 17).home == info.home
    # a JDK of the wrong major is never silently accepted
    only17 = [hostenv.JavaInfo(home=info.home, java=info.java, major=17, version="17.0.8")]
    monkeypatch.setattr(hostenv, "discover_javas", lambda extra_homes=None: only17)
    with pytest.raises(PrerequisiteMissing) as ei:
        hostenv.find_java([8, 11], 11)
    assert "no Java 8/11 found" in ei.value.message and "found: 17" in ei.value.message


def test_java_spec_version_parsing() -> None:
    assert hostenv._java_major("1.8") == 8 and hostenv._java_major("11.0.2") == 11 and hostenv._java_major("21") == 21  # noqa: SLF001


def test_broken_java_is_ignored(tmp_path: Path) -> None:
    fake = tmp_path / "bin" / "java"
    fake.parent.mkdir()
    fake.write_text("#!/bin/sh\necho nonsense >&2\nexit 3\n")
    fake.chmod(fake.stat().st_mode | stat.S_IXUSR)
    assert hostenv.probe_java(fake) is None
    assert hostenv.probe_java(tmp_path / "does-not-exist") is None


def test_python_discovery_finds_current_interpreter_and_rejects_unknown_series() -> None:
    cur = f"{sys.version_info.major}.{sys.version_info.minor}"
    found = hostenv.discover_pythons(cur)
    assert found and all(p.series == cur and p.implementation == "CPython" for p in found)
    info = hostenv.find_python(cur)
    assert info.has_venv
    with pytest.raises(PrerequisiteMissing) as ei:
        hostenv.find_python("2.6")
    assert "no usable CPython 2.6" in ei.value.message and ("uv" in ei.value.remediation or "python.org" in ei.value.remediation)


def test_python_without_venv_module_is_reported_with_the_distro_hint(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = hostenv.PythonInfo("/usr/bin/python3.99", "3.99.0", "3.99", has_venv=False, has_ensurepip=False)
    monkeypatch.setattr(hostenv, "discover_pythons", lambda series, extra=None: [fake])
    monkeypatch.setattr(hostenv.shutil, "which", lambda name: None)
    with pytest.raises(PrerequisiteMissing) as ei:
        hostenv.find_python("3.99")
    assert "python3.99-venv" in ei.value.remediation


def test_uv_python_install_is_opt_in(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []
    monkeypatch.setattr(hostenv, "discover_pythons", lambda series, extra=None: [])
    monkeypatch.setattr(hostenv.shutil, "which", lambda name: "/usr/bin/uv" if name == "uv" else None)

    class R:
        returncode, stdout, stderr = 1, "", "no network"

    monkeypatch.setattr(hostenv, "_run", lambda cmd, **kw: (calls.append(cmd), R())[1])
    with pytest.raises(PrerequisiteMissing) as ei:
        hostenv.find_python("3.99")  # not allowed to install: only a hint
    assert not calls and "allow-uv-python-install" in ei.value.remediation
    with pytest.raises(PrerequisiteMissing, match="uv python install 3.99"):
        hostenv.find_python("3.99", allow_uv_install=True)
    assert calls and calls[0][1:3] == ["python", "install"]
