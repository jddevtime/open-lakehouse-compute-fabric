# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Stub safety: on a Mac without Xcode Command Line Tools, ``/usr/bin/git|java|python3`` are installer dialogs and must never be executed.

macOS is simulated on this Linux host: ``olcf.core.hostbin`` is pointed at a temporary "``/usr/bin``" whose stubs write a marker file when run.
Every test asserts that no marker appeared.
"""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from olcf.core import hostbin

ROOT = Path(__file__).resolve().parents[2]
STUB_NAMES = ("git", "java", "javac", "python3", "python", "pip3", "make", "clang", "gcc", "cc")


def _script(path: Path, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/bin/sh\n" + body + "\n")
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return path


class FakeMac:
    """A fake macOS: a ``usr_bin`` directory of dialog-opening stubs, a failing ``xcode-select`` and a Homebrew directory with real tools."""

    def __init__(self, base: Path) -> None:
        self.base = base
        self.usr_bin = base / "usr" / "bin"
        self.brew = base / "brew" / "bin"
        self.markers = base / "markers"
        self.markers.mkdir()
        for name in STUB_NAMES:
            _script(self.usr_bin / name, f'echo STUB-{name} > "{self.markers}/{name}"; echo "xcode-select: note: no developer tools were found" >&2; exit 1')
        self.xcode_select = _script(base / "xcode-select", "exit 2")
        self.java_home_tool = _script(base / "java_home", "exit 1")

    def ran(self) -> list[str]:
        return sorted(p.name for p in self.markers.iterdir())

    def add_brew(self, name: str, output: str) -> Path:
        return _script(self.brew / name, f'echo "{output}"')

    def activate(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(hostbin, "is_macos", lambda: True)
        monkeypatch.setattr(hostbin, "STUB_DIRS", (str(self.usr_bin),))
        monkeypatch.setattr(hostbin, "XCODE_SELECT", str(self.xcode_select))
        monkeypatch.setattr(hostbin, "JAVA_HOME_TOOL", str(self.java_home_tool))
        hostbin.clear_cache()


@pytest.fixture()
def mac(tmp_path, monkeypatch) -> FakeMac:
    m = FakeMac(tmp_path)
    m.activate(monkeypatch)
    yield m
    hostbin.clear_cache()


def test_stubs_are_recognised_and_never_run(mac):
    for name in STUB_NAMES:
        assert hostbin.is_installer_stub(mac.usr_bin / name), name
    assert hostbin.find_real_binary("git", path=str(mac.usr_bin)) is None
    assert hostbin.find_real_binary("java", path=str(mac.usr_bin)) is None
    assert mac.ran() == []


def test_search_continues_past_stub_to_homebrew(mac):
    real = mac.add_brew("git", "git version 2.45.0")
    path = os.pathsep.join([str(mac.usr_bin), str(mac.brew)])
    assert hostbin.find_real_binary("git", path=path) == str(real)
    assert mac.ran() == []


def test_developer_tools_present_makes_git_real(mac, monkeypatch, tmp_path):
    devdir = tmp_path / "CommandLineTools"
    devdir.mkdir()
    _script(mac.xcode_select, f'echo "{devdir}"')
    hostbin.clear_cache()
    assert not hostbin.is_installer_stub(mac.usr_bin / "git")
    assert hostbin.is_installer_stub(mac.usr_bin / "java")             # java stays a stub unless java_home reports a JDK
    assert mac.ran() == []


def test_explain_missing_mentions_homebrew_and_never_xcode(mac):
    msg = hostbin.explain_missing("git")
    assert "brew install git" in msg and "never runs it" in msg
    assert "--with-jdk" in hostbin.explain_missing("java")


def test_jdk_discovery_reads_release_files_only(mac, tmp_path, monkeypatch):
    from olcf.cli import hostsafe

    home = tmp_path / "jdk-21"
    (home / "bin").mkdir(parents=True)
    _script(home / "bin" / "java", f'echo RAN > "{mac.markers}/jdk-java"')
    (home / "release").write_text('JAVA_VERSION="21.0.5"\nIMPLEMENTOR="Eclipse Adoptium"\n')
    monkeypatch.setenv("OLCF_JAVA_HOME", str(home))
    mine = [j for j in hostsafe.find_jdks() if j.source == "OLCF_JAVA_HOME"]   # the machine may have other JDKs (CI runners ship several)
    assert len(mine) == 1 and mine[0].major == 21 and mine[0].vendor == "Eclipse Adoptium"
    assert mac.ran() == []                                           # not even the good JDK was started: its release file was read


def test_safe_which_skips_stubs_and_guard_shims(mac, tmp_path, monkeypatch):
    from olcf.cli import hostsafe

    shims = hostsafe.ensure_shims(tmp_path / "guard")
    monkeypatch.setenv("OLCF_STUB_GUARD", str(shims))                   # what protect_environment() records
    real = mac.add_brew("python3", "Python 3.13")
    path = os.pathsep.join([str(shims), str(mac.usr_bin), str(mac.brew)])
    assert hostsafe.safe_which("python3", path) == str(real)
    assert hostsafe.safe_which("git", path) is None
    assert mac.ran() == []


def _tools(tmp_path: Path) -> str:
    """A PATH directory holding only what the shims themselves need (dirname, head, grep, cat), so no host git/java can be found by accident."""
    d = tmp_path / "coreutils"
    d.mkdir(exist_ok=True)
    for tool in ("dirname", "head", "grep", "cat", "sh"):
        real = shutil.which(tool)
        assert real, tool
        if not (d / tool).exists():
            (d / tool).symlink_to(real)
    return str(d)


def test_guard_shims_forward_to_real_tool_and_skip_usr_bin(mac, tmp_path):
    from olcf.cli import hostsafe

    shims = hostsafe.ensure_shims(tmp_path / "guard")
    mac.add_brew("git", "REAL-GIT")
    # the shim's PATH search skips the literal /usr/bin; point a stub-like dir at the real one by name instead
    env = {"PATH": os.pathsep.join([str(shims), str(mac.brew), _tools(tmp_path)]), "OLCF_STUB_DIRS": str(mac.usr_bin)}
    out = subprocess.run([str(shims / "git"), "--version"], env=env, capture_output=True, text=True, check=False)
    assert out.stdout.strip() == "REAL-GIT"
    # with no real git anywhere the shim fails quietly with 127 and a helpful message, and the stub dir is never consulted
    env = {"PATH": os.pathsep.join([str(shims), str(mac.usr_bin), _tools(tmp_path)]), "OLCF_STUB_DIRS": str(mac.usr_bin)}
    out = subprocess.run([str(shims / "git"), "--version"], env=env, capture_output=True, text=True, check=False)
    assert out.returncode == 127 and "not available on this Mac" in out.stderr
    assert mac.ran() == []


def test_java_shim_prefers_olcf_jdk(mac, tmp_path):
    from olcf.cli import hostsafe

    shims = hostsafe.ensure_shims(tmp_path / "guard")
    home = tmp_path / "jdk"
    _script(home / "bin" / "java", 'echo "java from $0"')
    env = {"PATH": f"{shims}:{_tools(tmp_path)}", "OLCF_JAVA_HOME": str(home)}
    out = subprocess.run([str(shims / "java"), "-version"], env=env, capture_output=True, text=True, check=False)
    assert out.returncode == 0 and str(home) in out.stdout
    jh = Path("/usr/libexec/java_home")
    host_has_jdk = jh.exists() and subprocess.run([str(jh)], capture_output=True, check=False).returncode == 0   # a real Mac (or CI runner) with a JDK installed: the shim rightly uses it
    if not host_has_jdk:
        out = subprocess.run([str(shims / "java"), "-version"], env={"PATH": f"{shims}:{mac.usr_bin}:{_tools(tmp_path)}", "OLCF_STUB_DIRS": str(mac.usr_bin)}, capture_output=True, text=True, check=False)
        assert out.returncode == 127 and "olcf runtime install --with-jdk" in out.stderr
    assert mac.ran() == []


def test_shims_are_idempotent_and_clean_up(tmp_path):
    from olcf.cli import hostsafe

    d = hostsafe.ensure_shims(tmp_path / "g")
    first = {p.name: p.read_text() for p in d.iterdir()}
    assert set(first) == set(hostsafe.SHIM_NAMES)
    hostsafe.ensure_shims(d)
    assert {p.name: p.read_text() for p in d.iterdir()} == first
    hostsafe.ensure_shims(d, names=("java", "javac"))                 # developer tools appeared: stop shadowing git, python3 ...
    assert sorted(p.name for p in d.iterdir()) == ["java", "javac"]
    assert all(os.access(p, os.X_OK) for p in d.iterdir())


def test_protect_environment_only_on_macos(tmp_path, monkeypatch):
    from olcf.cli import hostsafe

    monkeypatch.setenv("OLCF_CONFIG_DIR", str(tmp_path / "cfg"))
    monkeypatch.setenv("PATH", "/bin")
    monkeypatch.setattr(hostbin, "is_macos", lambda: False)   # say "not macOS" explicitly: the real host may be either
    hostbin.clear_cache()
    assert hostsafe.protect_environment() is None and os.environ["PATH"] == "/bin"           # not macOS: nothing changes
    monkeypatch.setattr(hostbin, "is_macos", lambda: True)
    hostbin.clear_cache()
    monkeypatch.setattr(hostbin, "XCODE_SELECT", str(tmp_path / "missing-xcode-select"))
    monkeypatch.delenv("OLCF_STUB_GUARD", raising=False)
    d = hostsafe.protect_environment()
    assert d is not None and os.environ["PATH"].split(os.pathsep)[0] == str(d)
    assert (d / "git").is_file()
    again = hostsafe.protect_environment()
    assert again == d and os.environ["PATH"].split(os.pathsep).count(str(d)) == 1
    monkeypatch.setenv("OLCF_NO_STUB_GUARD", "1")
    assert hostsafe.protect_environment() is None
    hostbin.clear_cache()


# ---------------------------------------------------------------------------------------------------------------- whole-CLI simulation
CLI_DRIVER = textwrap.dedent(
    """
    import sys
    from olcf.core import hostbin
    hostbin.is_macos = lambda: True
    hostbin.STUB_DIRS = (sys.argv[1],)
    hostbin.XCODE_SELECT = sys.argv[2]
    hostbin.JAVA_HOME_TOOL = sys.argv[3]
    from olcf.cli.main import main
    raise SystemExit(main(sys.argv[4:]))
    """
)


@pytest.fixture()
def mac_env(tmp_path):
    m = FakeMac(tmp_path)
    cfg = tmp_path / "cfg"
    env = {
        "PATH": os.pathsep.join([str(m.usr_bin), "/usr/bin", "/bin"]),     # stubs first: any unguarded lookup would hit them
        "PYTHONPATH": str(ROOT / "src"), "OLCF_CONFIG_DIR": str(cfg), "OLCF_DATA_DIR": str(tmp_path / "data"), "HOME": str(tmp_path / "home"),
        "NO_COLOR": "1", "NO_PROXY": "*", "OLCF_STUB_DIRS": str(m.usr_bin),
    }
    (tmp_path / "home").mkdir()
    return m, env


def _olcf(m: FakeMac, env: dict[str, str], *args: str, timeout: float = 180) -> subprocess.CompletedProcess[str]:
    return subprocess.run([sys.executable, "-c", CLI_DRIVER, str(m.usr_bin), str(m.xcode_select), str(m.java_home_tool), *args], env=env, capture_output=True, text=True,
                          timeout=timeout, check=False)


def test_cli_on_a_mac_without_developer_tools_never_runs_a_stub(mac_env):
    m, env = mac_env
    r = _olcf(m, env, "version")
    assert r.returncode == 0, r.stderr
    r = _olcf(m, env, "-q", "init", "--port", "18999")
    assert r.returncode == 0, r.stderr
    r = _olcf(m, env, "--json", "doctor", "--no-server")
    assert r.returncode in (0, 1), r.stderr
    assert "STUB" not in r.stdout + r.stderr
    r = _olcf(m, env, "--json", "runtime", "list")
    assert r.returncode == 0, r.stderr
    r = _olcf(m, env, "--json", "backup", "create", "-o", str(Path(env["HOME"]) / "b.bak"))
    assert r.returncode == 0, r.stderr
    r = _olcf(m, env, "--json", "diagnostics", "-o", str(Path(env["HOME"]) / "d.zip"))
    assert r.returncode == 0, r.stderr
    assert m.ran() == [], f"an installer stub was executed: {m.ran()}"
    guard = Path(env["OLCF_CONFIG_DIR"]) / "stubguard"
    assert (guard / "git").is_file() and (guard / "java").is_file()        # the guard shims were installed in front of PATH


def test_doctor_reports_missing_java_with_remedy_not_a_dialog(mac_env):
    m, env = mac_env
    _olcf(m, env, "-q", "init", "--port", "18998")
    r = _olcf(m, env, "--json", "doctor", "--no-server")
    text = r.stdout
    assert "java" in text.lower()
    assert "--with-jdk" in text or "runtime install" in text
    assert m.ran() == []


def test_server_start_on_a_mac_without_tools_never_runs_a_stub(mac_env):
    """The server process (and the embedded worker it would start) inherit the guarded PATH."""
    import httpx

    m, env = mac_env
    port = 18997
    _olcf(m, env, "-q", "init", "--port", str(port))
    proc = subprocess.Popen([sys.executable, "-c", CLI_DRIVER, str(m.usr_bin), str(m.xcode_select), str(m.java_home_tool), "serve", "--port", str(port)],
                            env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        import time

        deadline = time.monotonic() + 90
        ok = False
        while time.monotonic() < deadline and proc.poll() is None:
            try:
                ok = httpx.get(f"http://127.0.0.1:{port}/v1/health", timeout=2, trust_env=False).status_code == 200
            except httpx.HTTPError:
                ok = False
            if ok:
                break
            time.sleep(0.5)
        assert ok, "server did not become healthy"
        time.sleep(3)                                                   # let the embedded worker run its inventory probes
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
    assert m.ran() == [], f"an installer stub was executed: {m.ran()}"
