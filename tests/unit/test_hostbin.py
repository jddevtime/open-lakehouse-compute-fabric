# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""macOS installer-stub protection: ``olcf.core.hostbin`` must never execute ``/usr/bin/{git,java,python3}`` shims.

The tests simulate a Mac without developer tools on any host: the stub directory, ``xcode-select`` and ``java_home`` are replaced
by scripts that record a marker file and exit 99 when they are *executed as a stub* - the assertion is always that no marker exists.
"""

from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

from olcf.core import hostbin

STUB = "#!/bin/sh\ntouch '{marker}'\necho 'STUB EXECUTED' >&2\nexit 99\n"


def _script(path: Path, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return path


@pytest.fixture()
def fake_mac(tmp_path, monkeypatch):
    """A 'Mac' with only installer shims: stub dir /usr/bin -> tmp, xcode-select fails (no CLT), java_home fails (no JDK)."""
    marker = tmp_path / "STUB-WAS-EXECUTED"
    usr_bin = tmp_path / "usr" / "bin"
    for n in ("git", "java", "python3", "python", "javac"):
        _script(usr_bin / n, STUB.format(marker=marker))
    xs = _script(tmp_path / "xcode-select", "#!/bin/sh\necho 'xcode-select: error: unable to get active developer directory' >&2\nexit 2\n")
    jh = _script(tmp_path / "java_home", "#!/bin/sh\necho 'Unable to locate a Java Runtime.' >&2\nexit 1\n")
    monkeypatch.setattr(hostbin, "is_macos", lambda: True)
    monkeypatch.setattr(hostbin, "STUB_DIRS", (str(usr_bin),))
    monkeypatch.setattr(hostbin, "XCODE_SELECT", str(xs))
    monkeypatch.setattr(hostbin, "JAVA_HOME_TOOL", str(jh))
    hostbin.clear_cache()
    yield {"marker": marker, "usr_bin": usr_bin, "tmp": tmp_path, "xcode": xs, "java_home": jh}
    hostbin.clear_cache()


def test_stub_only_path_finds_nothing_and_never_executes(fake_mac):
    path = str(fake_mac["usr_bin"])
    for name in ("git", "java", "python3", "python", "javac"):
        assert hostbin.find_real_binary(name, path=path) is None
        assert hostbin.is_installer_stub(fake_mac["usr_bin"] / name)
    assert not fake_mac["marker"].exists()


def test_search_continues_past_the_stub_to_a_real_binary(fake_mac):
    real = _script(fake_mac["tmp"] / "brew" / "bin" / "git", "#!/bin/sh\necho git version 2.45.0\n")
    path = os.pathsep.join([str(fake_mac["usr_bin"]), str(real.parent)])
    assert hostbin.find_real_binary("git", path=path) == str(real)
    assert not fake_mac["marker"].exists()


def test_developer_tools_make_git_and_python_shims_acceptable(fake_mac):
    dev = fake_mac["tmp"] / "CommandLineTools"
    dev.mkdir()
    _script(fake_mac["xcode"], f"#!/bin/sh\necho '{dev}'\nexit 0\n")
    hostbin.clear_cache()
    assert hostbin.developer_tools_installed()
    assert hostbin.find_real_binary("git", path=str(fake_mac["usr_bin"])) == str(fake_mac["usr_bin"] / "git")
    # java is a different story: the CLT do not contain a JDK, java_home still says "none"
    assert hostbin.find_real_binary("java", path=str(fake_mac["usr_bin"])) is None
    assert not fake_mac["marker"].exists()


def test_java_home_jdk_is_used_without_touching_the_shim(fake_mac):
    jdk = fake_mac["tmp"] / "jdk-21.jdk" / "Contents" / "Home"
    _script(jdk / "bin" / "java", "#!/bin/sh\necho real java\n")
    _script(fake_mac["java_home"], f"#!/bin/sh\necho '{jdk}'\nexit 0\n")
    hostbin.clear_cache()
    assert hostbin.java_home() == str(jdk)
    assert hostbin.find_real_binary("java", path=str(fake_mac["usr_bin"])) == str(jdk / "bin" / "java")
    assert not hostbin.is_installer_stub(fake_mac["usr_bin"] / "java")  # a real JDK exists, so the shim would forward
    assert not fake_mac["marker"].exists()


def test_nothing_is_rejected_off_macos(fake_mac, monkeypatch):
    monkeypatch.setattr(hostbin, "is_macos", lambda: False)
    assert hostbin.find_real_binary("git", path=str(fake_mac["usr_bin"])) == str(fake_mac["usr_bin"] / "git")
    assert not hostbin.is_installer_stub(fake_mac["usr_bin"] / "git")
    assert hostbin.java_home() is None


def test_explain_missing_is_actionable(fake_mac):
    assert "brew install" in hostbin.explain_missing("git")
    assert "JDK" in hostbin.explain_missing("java") and "--with-jdk" in hostbin.explain_missing("java")


def test_hostenv_discovery_never_executes_stubs(fake_mac, monkeypatch):
    from olcf.runtime import hostenv

    monkeypatch.setenv("PATH", str(fake_mac["usr_bin"]))
    monkeypatch.delenv("JAVA_HOME", raising=False)
    # the macOS roots are real directories of the test host: keep discovery inside the sandboxed PATH only
    monkeypatch.setattr(hostenv, "_java_candidates", lambda extra: [Path(hostbin.find_real_binary("java") or "/nonexistent/java"), fake_mac["usr_bin"] / "java"])
    assert hostenv.discover_javas() == []
    assert hostenv.probe_java(fake_mac["usr_bin"] / "java") is None
    assert hostenv.probe_python([str(fake_mac["usr_bin"] / "python3")]) is None
    pys = hostenv.discover_pythons("3.13")
    assert all("usr/bin" not in p.executable for p in pys)
    assert not fake_mac["marker"].exists()
    from olcf.runtime.errors import PrerequisiteMissing

    with pytest.raises(PrerequisiteMissing) as exc:
        hostenv.find_java([21], 21)
    assert "installer shim" in str(exc.value.remediation)
    assert not fake_mac["marker"].exists()


def test_git_capabilities_report_unavailable_on_a_stub_only_mac(fake_mac, monkeypatch):
    from olcf.notebooks import gitrunner

    monkeypatch.setenv("PATH", str(fake_mac["usr_bin"]))
    caps = gitrunner.git_capabilities()
    assert caps["available"] is False and caps["path"] is None
    assert "brew install git" in str(caps["remediation"])
    assert gitrunner.git_binary() is None
    assert not fake_mac["marker"].exists()


def test_git_capabilities_with_real_git(fake_mac, monkeypatch):
    from olcf.notebooks import gitrunner

    real = _script(fake_mac["tmp"] / "bin" / "git", "#!/bin/sh\necho 'git version 2.45.1'\n")
    monkeypatch.setenv("PATH", os.pathsep.join([str(fake_mac["usr_bin"]), str(real.parent)]))
    gitrunner._VERSION_CACHE.clear()
    caps = gitrunner.git_capabilities()
    assert caps["available"] is True and caps["version"] == "2.45.1" and caps["path"] == str(real)
    assert not fake_mac["marker"].exists()
