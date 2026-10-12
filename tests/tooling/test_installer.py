# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S603, S607
"""``scripts/install.sh`` and ``scripts/uninstall.sh`` run for real: wheel built with hatchling, installed into a temporary prefix with a private
uv-managed Python, server started from the installed copy, upgraded, rolled back and removed.  A macOS "no developer tools" host is simulated
with a fake ``uname`` and dialog-opening stubs for git/java/python3 that must never run.  The PowerShell installer cannot be executed on this
platform (see ``test_powershell_scripts_are_present``); it is reviewed by eye only."""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

from tests.tooling.conftest import ROOT, free_port

SCRIPTS = ROOT / "scripts"
INSTALL = SCRIPTS / "install.sh"
UNINSTALL = SCRIPTS / "uninstall.sh"
UV = shutil.which("uv")

pytestmark = pytest.mark.skipif(UV is None, reason="uv is required to build the wheel and to run the installer")


# --------------------------------------------------------------------------------------------- static checks
@pytest.mark.parametrize("script", [INSTALL, UNINSTALL], ids=lambda p: p.name)
def test_shell_syntax(script):
    for shell in ("sh", "dash", "bash"):
        exe = shutil.which(shell)
        if exe:
            args = [exe, "--posix", "-n", str(script)] if shell == "bash" else [exe, "-n", str(script)]
            assert subprocess.run(args, capture_output=True, text=True, check=False).returncode == 0, shell


@pytest.mark.parametrize("script", [INSTALL, UNINSTALL], ids=lambda p: p.name)
def test_shellcheck_when_available(script, shellcheck):
    if shellcheck is None:
        pytest.skip("shellcheck is not installed in this sandbox")
    res = subprocess.run([shellcheck, "-s", "sh", "-S", "warning", str(script)], capture_output=True, text=True, check=False)
    assert res.returncode == 0, res.stdout


def test_installer_has_no_bashisms():
    """The one-line bootstrap pipes the script into ``sh``; on macOS that is bash 3.2 in POSIX mode (or dash on Linux)."""
    text = INSTALL.read_text()
    assert text.startswith("#!/bin/sh\n")
    code = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))
    for pat in (r"\[\[", r"^\s*declare\b", r"^\s*local\s", r"<\(", r"^\s*source\s", r"\$\{[A-Za-z_]+,,?\}", r"^\s*function\s+\w+", r"\bread\s+-p\b"):
        assert not re.search(pat, code, re.M), pat


def test_installer_never_names_a_stub_as_a_command():
    """Static guard in addition to the runtime simulation: no line may *execute* git, java, python3, make or a compiler by bare name."""
    forbidden = re.compile(r"^\s*(?:[A-Z_]+=\S+\s+)*(git|java|javac|python3?|pip3?|make|gcc|g\+\+|clang|cc)\b")
    for script in (INSTALL, UNINSTALL):
        for n, line in enumerate(script.read_text().splitlines(), 1):
            code = line.split("#", 1)[0] if not line.lstrip().startswith("#") else ""
            assert not forbidden.match(code), f"{script.name}:{n}: {line}"
            assert not re.search(r"(\$\(|`)\s*(git|java|python3?)\b", code), f"{script.name}:{n}: {line}"
            assert "/usr/bin/python3" not in code and "/usr/bin/java" not in code and "/usr/bin/git" not in code


def test_powershell_scripts_are_present():
    """PowerShell cannot run here.  Check the things a parser would: both files exist, are non-empty and balanced."""
    for name in ("install.ps1", "uninstall.ps1"):
        text = (SCRIPTS / name).read_text(encoding="utf-8")
        assert text.count("{") == text.count("}"), name
        assert text.count("(") == text.count(")"), name
        assert "Set-StrictMode" in text and "$ErrorActionPreference" in text
        assert "-Verb RunAs" not in text and "Start-Process -Verb" not in text                      # never asks for administrator rights
        assert not re.search(r"Invoke-Expression|\biex\b", text, re.I)


def test_powershell_initialises_before_it_installs_the_runtime():
    """`olcf init` writes environment = "development"; the bundled runtime manifests are dev-signed, so init must run first (a fresh data dir is production)."""
    text = (SCRIPTS / "install.ps1").read_text(encoding="utf-8")
    assert text.index("'init'") < text.index("'runtime', 'install'")


def test_powershell_never_passes_an_empty_argument_to_the_install_json_helper():
    """Windows PowerShell 5.1 drops an empty-string argument when it calls a native program; on a fresh install there is no previous version (CI found this on real Windows)."""
    text = (SCRIPTS / "install.ps1").read_text(encoding="utf-8")
    assert "$previousArg = if ($previous) { $previous } else { '-' }" in text
    assert "$helper, $Prefix, $targetVersion, $previousArg," in text and "$targetVersion, $previous, $Python" not in text
    helper = text.split("write_install_json.py'", 1)[1].split("'@", 1)[0]
    assert 'previous not in ("", "-")' in helper


def test_dry_run_initialises_before_the_runtime_install(tmp_path):
    res = subprocess.run(["sh", str(INSTALL), "--dry-run", "--prefix", str(tmp_path / "p"), "--data-dir", str(tmp_path / "d"), "--version", "1.2.3"], env=_env(tmp_path),
                         capture_output=True, text=True, check=False, stdin=subprocess.DEVNULL)
    assert res.returncode == 0, res.stderr
    assert res.stderr.index("would run: olcf init") < res.stderr.index("would run: olcf runtime install")


def test_default_source_is_this_repositorys_github_archive(tmp_path):
    base = ["sh", str(INSTALL), "--dry-run", "--prefix", str(tmp_path / "p"), "--data-dir", str(tmp_path / "d")]
    res = subprocess.run(base, env=_env(tmp_path), capture_output=True, text=True, check=False, stdin=subprocess.DEVNULL)
    assert res.returncode == 0, res.stderr
    assert "github.com/jddevtime/open-lakehouse-compute-fabric/archive/refs/heads/main.tar.gz" in res.stderr
    res = subprocess.run([*base, "--ref", "v0.1.0"], env=_env(tmp_path), capture_output=True, text=True, check=False, stdin=subprocess.DEVNULL)
    assert "archive/refs/tags/v0.1.0.tar.gz" in res.stderr


def test_source_cannot_be_combined_with_a_wheel_or_version(tmp_path):
    for other in (["--version", "1.2.3"], ["--wheel", str(tmp_path / "olcf-1.0.0-py3-none-any.whl")]):
        res = subprocess.run(["sh", str(INSTALL), "--dry-run", "--source", str(tmp_path / "x.tar.gz"), *other], env=_env(tmp_path), capture_output=True, text=True, check=False, stdin=subprocess.DEVNULL)
        assert res.returncode != 0 and "--source cannot be combined" in res.stderr


def test_a_source_archive_that_is_not_olcf_is_refused(tmp_path):
    import tarfile
    bad = tmp_path / "bad.tar.gz"
    (tmp_path / "pkg").mkdir()
    (tmp_path / "pkg" / "README").write_text("nothing here\n")
    with tarfile.open(bad, "w:gz") as tf:
        tf.add(tmp_path / "pkg", arcname="pkg")
    res = run_install(tmp_path, tmp_path / "prefix", tmp_path / "data", "--source", str(bad), env=_env(tmp_path))
    assert res.returncode != 0 and "no pyproject.toml with a version" in res.stderr


# --------------------------------------------------------------------------------------------- building wheels
def _stage(tmp: Path, version: str) -> Path:
    stage = tmp / f"stage-{version}"
    if stage.exists():
        shutil.rmtree(stage)
    stage.mkdir(parents=True)
    shutil.copy(ROOT / "pyproject.toml", stage / "pyproject.toml")
    shutil.copytree(ROOT / "src", stage / "src", ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    shutil.copytree(ROOT / "schemas", stage / "schemas")
    readme = ROOT / "README.md"
    (stage / "README.md").write_text(readme.read_text() if readme.exists() else "# OLCF\n")
    if version != "0.1.0":
        py = stage / "pyproject.toml"
        py.write_text(re.sub(r'(?m)^version = "[^"]+"', f'version = "{version}"', py.read_text(), count=1))
        ver = stage / "src" / "olcf" / "version.py"
        ver.write_text(re.sub(r'__version__ = "[^"]+"', f'__version__ = "{version}"', ver.read_text()))
    return stage


def _current_version() -> str:
    return re.search(r'(?m)^version = "([^"]+)"', (ROOT / "pyproject.toml").read_text()).group(1)  # type: ignore[union-attr]


def _build(tmp: Path, version: str) -> Path:
    stage = _stage(tmp, version)
    out = tmp / f"dist-{version}"
    res = subprocess.run([UV, "build", "--wheel", "--out-dir", str(out)], cwd=stage, capture_output=True, text=True, check=False, timeout=300)
    assert res.returncode == 0, res.stderr[-2000:]
    return next(out.glob("olcf-*.whl"))


@pytest.fixture(scope="module")
def wheel(tmp_path_factory) -> Path:
    """A wheel built by hatchling from the current tree (README.md is replaced by a stub when the docs package has not delivered one yet)."""
    return _build(tmp_path_factory.mktemp("wheel"), _current_version())


@pytest.fixture(scope="module")
def newer_wheel(tmp_path_factory) -> Path:
    cur = _current_version()
    major, minor, patch = (int(x) for x in cur.split("."))
    return _build(tmp_path_factory.mktemp("wheel2"), f"{major}.{minor}.{patch + 1}")


# --------------------------------------------------------------------------------------------- running the installer
_SHARED_CACHE: list[Path] = []     # set by the autouse fixture below; one uv package cache for every install of this module


@pytest.fixture(scope="module", autouse=True)
def _shared_uv_cache(tmp_path_factory):
    """Every installation resolves the same ~600 MB dependency set (pandas, pyarrow, ipykernel, duckdb ...). With one private cache per
    installation each of the five installs downloaded and unpacked it again (3 GB of disk, five times the network exposure); with one shared
    cache only the first is cold and the others link from it (measured: 7.7 s cold, ~3 s warm)."""
    _SHARED_CACHE[:] = [tmp_path_factory.mktemp("uv-cache")]
    yield
    _SHARED_CACHE.clear()


def _env(home: Path, extra_path: list[str] | None = None, **more: str) -> dict[str, str]:
    """A scrubbed environment: no OLCF_*, PATH with only system dirs (plus ``extra_path`` first), proxy/CA settings kept for the network."""
    env = {"HOME": str(home), "PATH": os.pathsep.join([*(extra_path or []), "/usr/bin", "/bin"]), "SHELL": "/bin/bash", "NO_COLOR": "1", "TMPDIR": str(home / "tmp")}
    (home / "tmp").mkdir(parents=True, exist_ok=True)
    for key in ("HTTPS_PROXY", "HTTP_PROXY", "NO_PROXY", "https_proxy", "http_proxy", "no_proxy", "SSL_CERT_FILE", "CURL_CA_BUNDLE", "REQUESTS_CA_BUNDLE"):
        if key in os.environ:
            env[key] = os.environ[key]
    if _SHARED_CACHE:
        env["OLCF_CACHE_DIR"] = str(_SHARED_CACHE[0])
    env.update(more)
    return env


#: where the installer puts the PATH block, by (login shell, operating system) - written out here as the specification, not computed from the script
PROFILE_FILES = {
    ("bash", "Linux"): ".bashrc", ("bash", "Darwin"): ".bash_profile",
    ("zsh", "Linux"): ".zshrc", ("zsh", "Darwin"): ".zshrc",
    ("fish", "Linux"): ".config/fish/conf.d/olcf.fish", ("fish", "Darwin"): ".config/fish/conf.d/olcf.fish",
    ("sh", "Linux"): ".profile", ("sh", "Darwin"): ".profile",
}
ALL_PROFILE_FILES = sorted({v for k, v in PROFILE_FILES.items() if k[0] != "fish"})
HOST_OS = {"darwin": "Darwin", "linux": "Linux"}.get(sys.platform, sys.platform)


def profile_file(home: Path, shell: str, os_name: str = HOST_OS) -> Path:
    return home / PROFILE_FILES[(shell, os_name)]


def _uname_shim(base: Path, os_name: str) -> Path:
    """A directory with only a ``uname`` (and ``sysctl``) that reports ``os_name``/arm64: the installer then takes that operating system's branches without any other change."""
    d = base / f"uname-{os_name}"
    d.mkdir(parents=True, exist_ok=True)
    (d / "uname").write_text(f'#!/bin/sh\ncase "$1" in -s) echo {os_name};; -m) echo arm64;; -r) echo 23.5.0;; *) echo {os_name};; esac\n')
    (d / "sysctl").write_text("#!/bin/sh\necho 0\n")
    for f in d.iterdir():
        f.chmod(0o755)
    return d


def run_install(home: Path, prefix: Path, data: Path, *args: str, env: dict[str, str] | None = None, timeout: float = 600) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["sh", str(INSTALL), "--prefix", str(prefix), "--data-dir", str(data), "--uv-bin", str(UV), "--no-runtime", *args], env=env or _env(home),
                          capture_output=True, text=True, timeout=timeout, check=False, stdin=subprocess.DEVNULL)


def olcf(prefix: Path, home: Path, *args: str, timeout: float = 120, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run([str(prefix / "bin" / "olcf"), *args], env=env or _env(home), capture_output=True, text=True, timeout=timeout, check=False, stdin=subprocess.DEVNULL)


@pytest.fixture(scope="module")
def installed(tmp_path_factory, wheel):
    base = tmp_path_factory.mktemp("install")
    home, prefix, data = base / "home", base / "prefix", base / "data"
    home.mkdir()
    port = free_port()
    res = run_install(home, prefix, data, "--wheel", str(wheel), "--port", str(port))
    assert res.returncode == 0, res.stderr[-3000:]
    ns = type("Installed", (), {})()
    ns.base, ns.home, ns.prefix, ns.data, ns.port, ns.result, ns.version = base, home, prefix, data, port, res, _current_version()
    yield ns
    with contextlib.suppress(Exception):
        olcf(prefix, home, "--data-dir", str(data), "stop")


def test_install_layout_and_metadata(installed):
    p = installed
    assert (p.prefix / "current").is_symlink()
    assert os.path.realpath(p.prefix / "current") == str(Path(os.path.realpath(p.prefix / "versions" / p.version)))
    assert (p.prefix / "versions" / p.version / "venv" / "bin" / "python").exists()
    assert (p.prefix / "bin" / "olcf").exists() and (p.prefix / "python").is_dir()
    info = json.loads((p.prefix / "install.json").read_text())
    assert info["schema"] == 1 and info["version"] == p.version and info["data_dir"] == str(p.data) and info["method"] == "wheel"
    assert info["prefix"] == str(p.prefix) and info["python"] == "3.13"
    assert "wheels only" in p.result.stderr or "only-binary" in p.result.stderr


def test_install_from_a_source_archive(tmp_path):
    """The default route for GitHub users: OLCF itself is built from a ``.tar.gz`` (hatchling, pure Python); every dependency is still a wheel."""
    import tarfile
    stage = _stage(tmp_path, _current_version())
    archive = tmp_path / "olcf-source.tar.gz"
    with tarfile.open(archive, "w:gz") as tf:
        tf.add(stage, arcname=f"open-lakehouse-compute-fabric-{_current_version()}")
    home, prefix, data = tmp_path / "home", tmp_path / "prefix", tmp_path / "data"
    home.mkdir()
    res = run_install(home, prefix, data, "--source", str(archive), "--no-init", "--no-modify-path", env=_env(home))
    assert res.returncode == 0, res.stderr[-3000:]
    info = json.loads((prefix / "install.json").read_text())
    assert info["method"] == "source" and info["version"] == _current_version()
    out = olcf(prefix, home, "version", env=_env(home))
    assert out.returncode == 0 and _current_version() in out.stdout


def test_installed_python_is_the_managed_one(installed):
    cfg = (installed.prefix / "versions" / installed.version / "venv" / "pyvenv.cfg").read_text()
    assert str(installed.prefix / "python") in cfg                      # not /usr/bin/python3, not the host interpreter


def test_installed_cli_runs_and_knows_its_install(installed):
    res = olcf(installed.prefix, installed.home, "--json", "version")
    assert res.returncode == 0, res.stderr
    doc = json.loads(res.stdout)
    assert doc["olcf"] == installed.version and doc["install"]["prefix"] == str(installed.prefix)
    assert str(installed.prefix) in doc["executable"]


def test_init_created_data_dir_without_leaking_the_password(installed):
    assert (installed.data / "config.toml").exists() and (installed.data / "olcf.db").exists()
    assert oct((installed.data / "secrets").stat().st_mode & 0o777) == "0o700"
    password = (installed.data / "secrets" / "bootstrap-admin.txt").read_text().split("password=")[1].strip()
    assert password in installed.result.stderr                          # shown exactly once, on the terminal
    assert not (installed.prefix / "install.json").read_text().count(password)


def test_installed_server_starts_and_answers(installed):
    """``olcf serve`` from the installed copy (data dir and port come from the installer's records), health check, graceful stop."""
    res = olcf(installed.prefix, installed.home, "serve", "--daemon", "--no-embedded-worker", "--wait", "90", timeout=150)
    assert res.returncode == 0, res.stderr + res.stdout
    try:
        r = httpx.get(f"http://127.0.0.1:{installed.port}/v1/health", timeout=5, trust_env=False)
        assert r.status_code == 200 and r.json()["status"] == "ok"
        st = olcf(installed.prefix, installed.home, "--json", "status")
        assert st.returncode == 0 and json.loads(st.stdout)["reachable"] is True
        who = olcf(installed.prefix, installed.home, "--json", "whoami")                 # the 'local' context created by init
        assert who.returncode == 0, who.stderr
    finally:
        stop = olcf(installed.prefix, installed.home, "stop")
    assert stop.returncode == 0, stop.stderr
    with pytest.raises(httpx.HTTPError):
        httpx.get(f"http://127.0.0.1:{installed.port}/v1/health", timeout=2, trust_env=False)


def test_rerun_is_idempotent(installed, wheel):
    before = json.loads((installed.prefix / "install.json").read_text())
    res = run_install(installed.home, installed.prefix, installed.data, "--wheel", str(wheel), "--port", str(installed.port), "--no-init")
    assert res.returncode == 0, res.stderr[-2000:]
    assert "already installed" in res.stderr
    after = json.loads((installed.prefix / "install.json").read_text())
    assert after["version"] == before["version"] and "previous" not in after
    assert (installed.data / "secrets" / "bootstrap-admin.txt").exists()
    rc = profile_file(installed.home, "bash").read_text()                 # _env() says SHELL=/bin/bash: .bashrc on Linux, .bash_profile on macOS
    assert rc.count("# >>> olcf >>>") == 1                               # PATH block is not duplicated


@pytest.mark.parametrize(("shell", "os_name"), sorted(PROFILE_FILES), ids=lambda v: str(v))
def test_path_block_goes_to_the_profile_of_the_login_shell_on_each_os(installed, wheel, tmp_path, shell, os_name):
    """Re-running the installer for a user whose login shell is ``shell`` on ``os_name`` (a ``uname`` shim stands in for the other operating system) writes the PATH block
    to that shell's profile - and to no other file."""
    home = tmp_path / "home"
    home.mkdir()
    env = _env(home, extra_path=[str(_uname_shim(tmp_path, os_name))], SHELL=f"/usr/bin/{shell}")
    res = run_install(home, installed.prefix, installed.data, "--wheel", str(wheel), "--port", str(installed.port), "--no-init", env=env)
    assert res.returncode == 0, res.stderr[-2000:]
    expect = profile_file(home, shell, os_name)
    text = expect.read_text()
    if shell == "fish":
        assert str(installed.prefix) in text
    else:
        assert text.count("# >>> olcf >>>") == 1 and f'. "{installed.prefix}/env.sh"' in text
    written = sorted(str(p.relative_to(home)) for p in home.rglob("*") if p.is_file() and p.name != "uv" and "tmp" not in p.relative_to(home).parts[:1])
    assert written == [str(expect.relative_to(home))], written


def test_path_entry_added_once_and_sourceable(installed):
    env_sh = (installed.prefix / "env.sh").read_text()
    out = subprocess.run(["sh", "-c", f'. "{installed.prefix}/env.sh"; command -v olcf'], env=_env(installed.home), capture_output=True, text=True, check=False)
    assert out.stdout.strip() == str(installed.prefix / "bin" / "olcf"), env_sh


def test_upgrade_backs_up_migrates_and_rollback_works(installed, newer_wheel):
    v_new = re.match(r"olcf-([^-]+)-", newer_wheel.name).group(1)  # type: ignore[union-attr]
    res = run_install(installed.home, installed.prefix, installed.data, "--wheel", str(newer_wheel), "--no-init", "--no-modify-path")
    assert res.returncode == 0, res.stderr[-3000:]
    info = json.loads((installed.prefix / "install.json").read_text())
    assert info["version"] == v_new and info["previous"] == installed.version
    assert Path(os.path.realpath(installed.prefix / "current")).name == v_new
    backups = list((installed.data / "backups").glob(f"pre-upgrade-{installed.version}-to-{v_new}-*.olcfbak"))
    assert len(backups) == 1 and backups[0].stat().st_size > 0
    assert olcf(installed.prefix, installed.home, "--json", "version").stdout.count(f'"olcf": "{v_new}"') == 1
    listing = olcf(installed.prefix, installed.home, "--json", "upgrade", "--list")
    assert listing.returncode == 0 and installed.version in listing.stdout and v_new in listing.stdout
    back = olcf(installed.prefix, installed.home, "upgrade", "--rollback", "--yes", "--no-restart")
    assert back.returncode == 0, back.stderr + back.stdout
    assert Path(os.path.realpath(installed.prefix / "current")).name == installed.version
    assert olcf(installed.prefix, installed.home, "--json", "version").stdout.count(f'"olcf": "{installed.version}"') == 1


def test_bad_wheel_leaves_current_untouched(installed, tmp_path):
    bogus = tmp_path / "olcf-9.9.9-py3-none-any.whl"
    bogus.write_bytes(b"not a zip")
    before = os.path.realpath(installed.prefix / "current")
    res = run_install(installed.home, installed.prefix, installed.data, "--wheel", str(bogus), "--no-init", "--no-modify-path")
    assert res.returncode != 0
    assert os.path.realpath(installed.prefix / "current") == before
    assert not (installed.prefix / "versions" / "9.9.9").exists()      # partial version directory was removed


# --------------------------------------------------------------------------------------------- uninstaller
@pytest.mark.parametrize("shell", ["bash", "zsh"])
def test_uninstall_keeps_data_by_default_and_categories_are_independent(tmp_path, wheel, shell):
    home, prefix, data = tmp_path / "home", tmp_path / "prefix", tmp_path / "data"
    home.mkdir()
    profile = profile_file(home, shell)                                         # where THIS host's installer puts the PATH block for this login shell
    assert run_install(home, prefix, data, "--wheel", str(wheel), "--port", str(free_port()), env=_env(home, SHELL=f"/bin/{shell}")).returncode == 0
    assert profile.is_file() and profile.read_text().count("# >>> olcf >>>") == 1
    (data / "storage" / "lh").mkdir(parents=True)
    (data / "storage" / "lh" / "t.parquet").write_bytes(b"x")
    (data / "backups" / "b.olcfbak").write_bytes(b"x")
    env = _env(home)

    def un(*a: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(["sh", str(UNINSTALL), "--prefix", str(prefix), *a], env=env, capture_output=True, text=True, check=False, stdin=subprocess.DEVNULL)

    dry = un("--dry-run", "--purge")
    assert dry.returncode == 0 and prefix.exists() and (data / "olcf.db").exists()
    refused = un("--keep-program", "--remove-storage")
    assert refused.returncode != 0 and (data / "storage" / "lh" / "t.parquet").exists()      # non-interactive needs --yes
    one = un("--keep-program", "--remove-logs", "--yes")
    assert one.returncode == 0 and not (data / "logs").exists() and (data / "olcf.db").exists() and (data / "secrets").exists() and (data / "storage").exists()
    two = un("--keep-program", "--remove-storage", "--yes")
    assert two.returncode == 0 and not (data / "storage").exists() and (data / "backups" / "b.olcfbak").exists() and (data / "secrets").exists()
    prog = un()
    assert prog.returncode == 0 and not prefix.exists()
    assert (data / "olcf.db").exists() and (data / "secrets" / "master.key").exists() and (data / "backups" / "b.olcfbak").exists()
    assert "# >>> olcf >>>" not in profile.read_text()                                           # removed from the file the installer wrote it to ...
    assert not [f for f in ALL_PROFILE_FILES if (home / f).is_file() and "# >>> olcf >>>" in (home / f).read_text()]   # ... and left in no other profile
    final = un("--data-dir", str(data), "--purge", "--yes")     # the prefix (and its install.json) is gone, so the data directory must be named
    assert final.returncode == 0 and not data.exists()


def test_uninstall_refuses_foreign_directories(tmp_path):
    foreign = tmp_path / "important"
    foreign.mkdir()
    (foreign / "keep.txt").write_text("precious")
    res = subprocess.run(["sh", str(UNINSTALL), "--prefix", str(foreign), "--data-dir", str(foreign), "--purge", "--yes"], env=_env(tmp_path), capture_output=True, text=True,
                         check=False, stdin=subprocess.DEVNULL)
    assert (foreign / "keep.txt").read_text() == "precious"
    assert "does not look like" in res.stderr
    root = subprocess.run(["sh", str(UNINSTALL), "--prefix", "/", "--yes"], env=_env(tmp_path), capture_output=True, text=True, check=False, stdin=subprocess.DEVNULL)
    assert root.returncode != 0


# --------------------------------------------------------------------------------------------- simulated Mac without developer tools
def _fake_mac(base: Path) -> tuple[Path, Path]:
    """``fake/bin`` first on PATH: uname says Darwin/arm64; git, java, javac, python3, python, pip3, make, cc, clang, gcc are dialog stubs that leave a marker."""
    bin_dir, markers = base / "fake" / "bin", base / "fake" / "markers"
    bin_dir.mkdir(parents=True)
    markers.mkdir()
    for name in ("git", "java", "javac", "python3", "python", "pip3", "pip", "make", "cc", "clang", "gcc", "xcodebuild"):
        f = bin_dir / name
        f.write_text(f'#!/bin/sh\necho "{name} $*" >> "{markers}/{name}"\necho "xcode-select: note: no developer tools were found" >&2\nexit 1\n')
        f.chmod(0o755)
    uname = bin_dir / "uname"
    uname.write_text('#!/bin/sh\ncase "$1" in -s) echo Darwin;; -m) echo arm64;; -r) echo 23.5.0;; -a) echo "Darwin mac 23.5.0 arm64";; *) echo Darwin;; esac\n')
    uname.chmod(0o755)
    sysctl = bin_dir / "sysctl"
    sysctl.write_text("#!/bin/sh\necho 0\n")
    sysctl.chmod(0o755)
    return bin_dir, markers


def test_mac_without_developer_tools_never_runs_a_stub(tmp_path, wheel):
    home, prefix, data = tmp_path / "home", tmp_path / "prefix", tmp_path / "data"
    home.mkdir()
    fake_bin, markers = _fake_mac(tmp_path)
    env = _env(home, extra_path=[str(fake_bin)], SHELL="/bin/zsh", OLCF_STUB_DIRS=str(fake_bin))
    res = run_install(home, prefix, data, "--wheel", str(wheel), "--port", str(free_port()), env=env)
    assert res.returncode == 0, res.stderr[-3000:]
    assert "Darwin/arm64" in res.stderr
    assert "no Xcode Command Line Tools" in res.stderr or "developer tools are present" in res.stderr
    assert sorted(p.name for p in markers.iterdir()) == [], f"a stub was executed: {[p.name for p in markers.iterdir()]}"
    # everything installed afterwards, run with the stubs first on PATH, also leaves them alone
    assert olcf(prefix, home, "--json", "version", env=env).returncode == 0
    assert olcf(prefix, home, "--json", "doctor", "--no-server", env=env).returncode in (0, 1)
    assert olcf(prefix, home, "--json", "runtime", "list", env=env).returncode == 0
    assert (home / ".zshrc").read_text().count("# >>> olcf >>>") == 1                              # macOS default shell
    # Starting the server is deliberately NOT part of this shell-level simulation: the Python side runs on Linux here, where a stub directory
    # that is not /usr/bin is (correctly) just a directory, so e.g. the Git capability probe would run the fake git.  The Python-level guard
    # (olcf.core.hostbin, guard shims, server start) is proven in test_hostsafe.py with the platform and stub directories patched.
    assert sorted(p.name for p in markers.iterdir()) == [], f"a stub was executed: {[p.name for p in markers.iterdir()]}"


def test_unsupported_platform_message(tmp_path):
    fake = tmp_path / "bin"
    fake.mkdir()
    (fake / "uname").write_text('#!/bin/sh\ncase "$1" in -s) echo MINGW64_NT-10.0;; *) echo x86_64;; esac\n')
    (fake / "uname").chmod(0o755)
    res = subprocess.run(["sh", str(INSTALL), "--dry-run", "--prefix", str(tmp_path / "p")], env=_env(tmp_path, [str(fake)]), capture_output=True, text=True, check=False)
    assert res.returncode != 0 and "install.ps1" in res.stderr


def test_install_then_runtime_step_sees_the_bundled_manifests(tmp_path, wheel):
    """The REAL order of the installer: `olcf init` first, so the runtime step runs against environment = "development" and finds the bundled (dev-signed) manifests.
    A bogus runtime id keeps the step offline: the error then lists the ids this installation trusts - with the old order (runtime before init) the list was empty."""
    home = tmp_path / "home"
    home.mkdir()
    res = subprocess.run(["sh", str(INSTALL), "--prefix", str(tmp_path / "p"), "--data-dir", str(tmp_path / "d"), "--uv-bin", str(UV), "--wheel", str(wheel), "--port",
                          str(free_port()), "--runtime", "olcf-rt-9.9.9", "--no-jdk", "--no-modify-path"], env=_env(home), capture_output=True, text=True, timeout=600, check=False,
                         stdin=subprocess.DEVNULL)
    assert res.returncode == 0, res.stderr[-3000:]
    assert "olcf-rt-2.0.0" in res.stderr and "valid runtime ids" in res.stderr, res.stderr[-3000:]
    assert "none ship with this build" not in res.stderr


def test_dry_run_changes_nothing(tmp_path):
    prefix = tmp_path / "p"
    res = subprocess.run(["sh", str(INSTALL), "--dry-run", "--prefix", str(prefix), "--data-dir", str(tmp_path / "d"), "--version", "1.2.3"], env=_env(tmp_path), capture_output=True,
                         text=True, check=False, stdin=subprocess.DEVNULL)
    assert res.returncode == 0, res.stderr
    assert not prefix.exists() and not (tmp_path / "d").exists()
    assert "would" in res.stderr


def test_docker_hint_and_help(tmp_path):
    hint = subprocess.run(["sh", str(INSTALL), "--docker"], capture_output=True, text=True, check=False, env=_env(tmp_path))
    assert hint.returncode == 0 and "docker" in hint.stderr and "allinone" in hint.stderr
    helptext = subprocess.run(["sh", str(INSTALL), "--help"], capture_output=True, text=True, check=False, env=_env(tmp_path))
    for opt in ("--version", "--prefix", "--no-runtime", "--offline-bundle", "--with-service", "--docker"):
        assert opt in helptext.stderr
    bad = subprocess.run(["sh", str(INSTALL), "--bogus"], capture_output=True, text=True, check=False, env=_env(tmp_path))
    assert bad.returncode == 2


# --------------------------------------------------------------------------------------------- uv bootstrap (checksum verification) with a fake curl
def host_uv_triple() -> str:
    """The uv release the installer must ask for on THIS machine (the specification of its OS/CPU mapping; Rosetta is not simulated)."""
    import platform

    arch = {"arm64": "aarch64", "aarch64": "aarch64", "x86_64": "x86_64", "amd64": "x86_64"}.get(platform.machine().lower())
    osn = {"darwin": "apple-darwin", "linux": "unknown-linux-musl"}.get(sys.platform)
    if arch is None or osn is None:
        pytest.skip(f"the installer does not support {sys.platform}/{platform.machine()}")
    return f"{arch}-{osn}"


def _fake_curl(base: Path, tarball: Path, sha: str) -> Path:
    bin_dir = base / "curlbin"
    bin_dir.mkdir()
    real_sha = base / "uv.sha256"
    real_sha.write_text(f"{sha}  uv-test.tar.gz\n")
    script = bin_dir / "curl"
    script.write_text(
        '#!/bin/sh\nout=""; url=""\nwhile [ "$#" -gt 0 ]; do case "$1" in -o) out=$2; shift 2;; -*) shift;; *) url=$1; shift;; esac; done\n'
        f'echo "$url" >> "{base}/requested-urls"\n'
        f'case "$url" in *.sha256) cp "{real_sha}" "$out";; *) cp "{tarball}" "$out";; esac\n'
    )
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    return bin_dir


def _uv_tarball(base: Path) -> Path:
    import tarfile

    triple = host_uv_triple()
    pkg = base / "uv-pkg" / f"uv-{triple}"
    pkg.mkdir(parents=True)
    shutil.copy(UV, pkg / "uv")
    tarball = base / "uv-test.tar.gz"
    with tarfile.open(tarball, "w:gz") as tf:
        tf.add(pkg, arcname=f"uv-{triple}")
    return tarball


def test_uv_download_is_checksum_verified(tmp_path, wheel):
    triple = host_uv_triple()                      # x86_64 or arm64/aarch64, Linux or macOS: the archive is named for this machine, and the installer must ask for the same
    tarball = _uv_tarball(tmp_path)
    good = hashlib.sha256(tarball.read_bytes()).hexdigest()
    home = tmp_path / "home"
    home.mkdir()
    # tampered download: refuse to run it
    (tmp_path / "bad").mkdir()
    env = _env(home, [str(_fake_curl(tmp_path / "bad", tarball, "0" * 64))])
    res = subprocess.run(["sh", str(INSTALL), "--prefix", str(tmp_path / "p1"), "--data-dir", str(tmp_path / "d1"), "--no-runtime", "--no-init", "--wheel", str(wheel)], env=env,
                         capture_output=True, text=True, check=False, stdin=subprocess.DEVNULL, timeout=120)
    assert res.returncode != 0 and "SHA-256 mismatch" in res.stderr
    assert not (tmp_path / "p1" / "bin" / "uv").exists()
    # matching checksum: the binary is installed and used
    (tmp_path / "good").mkdir()
    env = _env(home, [str(_fake_curl(tmp_path / "good", tarball, good))])
    res = subprocess.run(["sh", str(INSTALL), "--prefix", str(tmp_path / "p2"), "--data-dir", str(tmp_path / "d2"), "--no-runtime", "--no-init", "--no-modify-path", "--wheel", str(wheel)],
                         env=env, capture_output=True, text=True, check=False, stdin=subprocess.DEVNULL, timeout=600)
    assert res.returncode == 0, res.stderr[-2000:]
    assert "verified and installed" in res.stderr and (tmp_path / "p2" / "bin" / "uv").exists()
    asked = (tmp_path / "good" / "requested-urls").read_text().split()
    assert any(u.endswith(f"/uv-{triple}.tar.gz") for u in asked) and any(u.endswith(f"/uv-{triple}.tar.gz.sha256") for u in asked), asked


def test_install_refuses_plain_http_urls(tmp_path):
    res = subprocess.run(["sh", str(INSTALL), "--prefix", str(tmp_path / "p"), "--no-runtime", "--no-init", "--wheel", "http://example.invalid/olcf-0.1.0-py3-none-any.whl"],
                         env=_env(tmp_path), capture_output=True, text=True, check=False, stdin=subprocess.DEVNULL)
    assert res.returncode != 0


def test_wait_helper_used_by_tests_is_sane():
    start = time.monotonic()
    assert free_port() > 1024 and time.monotonic() - start < 1 and sys.platform
