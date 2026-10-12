# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Discovery of host prerequisites: a matching CPython (for the runtime venv) and a JDK.

* Python: ``python3.13`` / ``python3`` / ``python`` on ``PATH``, ``sys.executable``, the Windows launcher
  (``py -3.13``), ``uv python find``, and, only when the operator allows it, ``uv python install``.
* Java: ``JAVA_HOME``, ``java`` on ``PATH``, JDKs previously downloaded into ``<runtimes>/_jdk``, common install
  locations. With ``--with-jdk`` a Temurin build is downloaded from the Adoptium API and its **SHA-256, as returned
  by the API, is verified before extraction** (see :func:`install_temurin`).

Every failure is an actionable :class:`~olcf.runtime.errors.PrerequisiteMissing`.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from olcf.core import hostbin
from olcf.runtime.errors import ChecksumMismatch, PrerequisiteMissing
from olcf.runtime.manifest import normalise_arch, normalise_os

log = logging.getLogger("olcf.runtime.hostenv")

ADOPTIUM_API = "https://api.adoptium.net"


def venv_python(venv_dir: str | Path) -> Path:
    """Path of the interpreter inside a venv (POSIX ``bin/python`` or Windows ``Scripts/python.exe``)."""
    d = Path(venv_dir)
    return d / "Scripts" / "python.exe" if os.name == "nt" else d / "bin" / "python"


def _run(cmd: list[str], timeout: float = 30.0, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, check=False, env=env)  # noqa: S603


# --------------------------------------------------------------------------------------------- python
@dataclass(frozen=True)
class PythonInfo:
    """A Python interpreter found on the host: executable, version, series and whether it can create virtual environments."""
    executable: str
    version: str  # e.g. 3.13.5
    series: str  # e.g. 3.13
    has_venv: bool
    has_ensurepip: bool
    implementation: str = "CPython"
    arch: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


_PROBE = (
    "import sys, json, platform\n"
    "try:\n    import ensurepip; ep = True\nexcept Exception:\n    ep = False\n"
    "try:\n    import venv; vv = True\nexcept Exception:\n    vv = False\n"
    "print(json.dumps({'exe': sys.executable, 'v': platform.python_version(), 'impl': platform.python_implementation(),"
    " 'ep': ep, 'vv': vv, 'arch': platform.machine()}))\n"
)


def probe_python(cmd: list[str]) -> PythonInfo | None:
    """Run ``cmd -c <probe>``; return ``None`` when the interpreter is unusable (a macOS installer stub is never executed)."""
    if cmd and hostbin.is_installer_stub(cmd[0]):
        return None
    try:
        res = _run([*cmd, "-I", "-c", _PROBE], timeout=20)
    except (OSError, subprocess.SubprocessError):
        return None
    if res.returncode != 0:
        return None
    try:
        d = json.loads(res.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError):
        return None
    v = d["v"]
    return PythonInfo(d["exe"], v, ".".join(v.split(".")[:2]), bool(d["vv"]), bool(d["ep"]), d["impl"], normalise_arch(d["arch"]))


def discover_pythons(series: str, extra: list[str] | None = None) -> list[PythonInfo]:
    """All usable interpreters of version ``series`` ("3.13"), best candidates first."""
    cands: list[list[str]] = [[p] for p in (extra or [])]
    for name in (f"python{series}", "python3", "python"):
        exe = hostbin.find_real_binary(name)  # skips the macOS /usr/bin/python3 installer shim (never executed)
        if exe:
            cands.append([exe])
    cands.append([sys.executable])
    if os.name == "nt" and shutil.which("py"):
        cands.append(["py", f"-{series}"])
    uv = shutil.which("uv")
    if uv:
        res = _run([uv, "python", "find", series], timeout=30)
        if res.returncode == 0 and res.stdout.strip():
            cands.append([res.stdout.strip().splitlines()[-1]])
    found: list[PythonInfo] = []
    seen: set[str] = set()
    for cmd in cands:
        info = probe_python(cmd)
        if info and info.series == series and info.implementation == "CPython" and info.executable not in seen:
            seen.add(info.executable)
            found.append(info)
    return found


def find_python(series: str, *, extra: list[str] | None = None, allow_uv_install: bool = False) -> PythonInfo:
    """Return a CPython ``series`` able to create venvs; optionally ``uv python install`` one."""
    found = discover_pythons(series, extra)
    usable = [p for p in found if p.has_venv and (p.has_ensurepip or shutil.which("uv"))]
    if usable:
        return usable[0]
    uv = shutil.which("uv")
    if uv and allow_uv_install:
        log.info("installing Python %s with uv", series)
        res = _run([uv, "python", "install", series], timeout=900)
        if res.returncode == 0:
            again = [p for p in discover_pythons(series, extra) if p.has_venv]
            if again:
                return again[0]
        raise PrerequisiteMissing(f"`uv python install {series}` failed: {res.stderr.strip()[-400:]}",
                                  remediation=f"Install Python {series} manually (python.org, your package manager or `uv python install {series}`).")
    if found and not usable:
        raise PrerequisiteMissing(
            f"Python {series} found at {found[0].executable} but the `venv`/`ensurepip` module is missing",
            remediation=f"Debian/Ubuntu: `apt install python{series}-venv`; or install `uv` (https://docs.astral.sh/uv/) which can create venvs without ensurepip.")
    hint = ("`uv python install " + series + "` (uv is available: re-run the install with --allow-uv-python-install)") if uv else \
        f"install Python {series} (python.org / package manager) or install uv and re-run with --allow-uv-python-install"
    raise PrerequisiteMissing(f"no usable CPython {series} found on this machine",
                              remediation=f"Please {hint}." + (f" {hostbin.explain_missing('python3')}" if hostbin.is_macos() else ""))


# --------------------------------------------------------------------------------------------- java
@dataclass(frozen=True)
class JavaInfo:
    """A Java installation found on the host: home directory, executable, major version and full version."""
    home: str
    java: str
    major: int
    version: str
    vendor: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _java_major(spec_version: str) -> int:
    # "1.8" -> 8 ; "11.0.2" -> 11 ; "21" -> 21
    parts = spec_version.split(".")
    if parts[0] == "1" and len(parts) > 1:
        return int(parts[1])
    return int(re.sub(r"\D.*", "", parts[0]) or 0)


def probe_java(java_exe: str | Path) -> JavaInfo | None:
    """Return :class:`JavaInfo` for a ``java`` executable (None if it does not run; a macOS installer stub is never executed)."""
    if hostbin.is_installer_stub(java_exe):
        return None
    env = {k: v for k, v in os.environ.items() if k != "JAVA_TOOL_OPTIONS"}  # silence "Picked up JAVA_TOOL_OPTIONS"
    try:
        res = _run([str(java_exe), "-XshowSettings:properties", "-version"], timeout=30, env=env)
    except (OSError, subprocess.SubprocessError):
        return None
    text = res.stderr + res.stdout
    props: dict[str, str] = {}
    for line in text.splitlines():
        m = re.match(r"^\s+([\w.]+) = (.*)$", line)
        if m:
            props[m.group(1)] = m.group(2).strip()
    if "java.specification.version" not in props:
        return None
    home = props.get("java.home", str(Path(java_exe).resolve().parent.parent))
    return JavaInfo(home=home, java=str(java_exe), major=_java_major(props["java.specification.version"]),
                    version=props.get("java.version", ""), vendor=props.get("java.vendor", ""))


def _java_candidates(extra_homes: list[Path]) -> list[Path]:
    exe = "java.exe" if os.name == "nt" else "java"
    cands: list[Path] = []
    for home in extra_homes:
        cands.append(home / "bin" / exe)
    jh = os.environ.get("JAVA_HOME")
    if jh:
        cands.append(Path(jh) / "bin" / exe)
    which = hostbin.find_real_binary("java")  # never the macOS /usr/bin/java installer shim
    if which:
        cands.append(Path(which))
    jhome = hostbin.java_home()  # macOS: asks /usr/libexec/java_home (does not prompt)
    if jhome:
        cands.append(Path(jhome) / "bin" / exe)
    roots = [Path("/usr/lib/jvm"), Path("/Library/Java/JavaVirtualMachines"), Path("C:/Program Files/Eclipse Adoptium"),
             Path("C:/Program Files/Java"), Path.home() / ".sdkman/candidates/java", Path("/opt/homebrew/opt"), Path("/usr/local/opt")]
    for root in roots:
        if root.is_dir():
            for child in sorted(root.iterdir()):
                if root.name == "opt" and not child.name.startswith(("openjdk", "temurin", "zulu", "corretto")):
                    continue  # Homebrew prefix: thousands of formulae, only JDK formulae matter
                for sub in (child / "bin" / exe, child / "Contents" / "Home" / "bin" / exe):
                    cands.append(sub)
    return cands


def discover_javas(extra_homes: list[Path] | None = None) -> list[JavaInfo]:
    """Every working JDK/JRE found (deduplicated by real home)."""
    out: list[JavaInfo] = []
    seen: set[str] = set()
    for c in _java_candidates(extra_homes or []):
        if not c.exists():
            continue
        info = probe_java(c)
        if info and info.home not in seen:
            seen.add(info.home)
            out.append(info)
    return out


def find_java(supported_majors: list[int], preferred_major: int, *, extra_homes: list[Path] | None = None) -> JavaInfo:
    """Pick the best JVM for a runtime: preferred major first, then any other supported major."""
    javas = discover_javas(extra_homes)
    for want in [preferred_major, *[m for m in sorted(supported_majors, reverse=True) if m != preferred_major]]:
        for j in javas:
            if j.major == want:
                return j
    have = ", ".join(f"{j.major} ({j.home})" for j in javas) or "none"
    raise PrerequisiteMissing(
        f"no Java {'/'.join(str(m) for m in sorted(supported_majors))} found (found: {have})",
        details={"required": supported_majors, "found": [j.to_dict() for j in javas]},
        remediation=f"Install a JDK {preferred_major} (e.g. Eclipse Temurin), set JAVA_HOME, or re-run the runtime install with --with-jdk to download a verified Temurin build."
                    + (f" {hostbin.explain_missing('java')}" if hostbin.is_macos() else ""),
    )


# --------------------------------------------------------------------------------------------- Temurin download
def _adoptium_os_arch() -> tuple[str, str]:
    os_n = {"linux": "linux", "macos": "mac", "windows": "windows"}[normalise_os()]
    arch = {"x86_64": "x64", "aarch64": "aarch64"}.get(normalise_arch(), normalise_arch())
    return os_n, arch


def _http_json(url: str, timeout: float = 60.0) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": "olcf-runtime-manager/0.1", "Accept": "application/json"})  # noqa: S310
    with urllib.request.urlopen(req, timeout=timeout) as fh:  # noqa: S310
        return json.loads(fh.read().decode("utf-8"))


def _safe_extract_tar(tf: tarfile.TarFile, dest: Path) -> None:
    root = dest.resolve()
    for m in tf.getmembers():
        target = (dest / m.name).resolve()
        if not str(target).startswith(str(root) + os.sep) and target != root:
            raise PrerequisiteMissing(f"JDK archive contains an unsafe path: {m.name}")
        if m.issym() or m.islnk():
            link = (Path(target).parent / m.linkname).resolve() if m.issym() else (dest / m.linkname).resolve()
            if not str(link).startswith(str(root) + os.sep):
                raise PrerequisiteMissing(f"JDK archive contains an unsafe link: {m.name} -> {m.linkname}")
    try:
        tf.extractall(dest, filter="tar")  # Python >= 3.12 safe filter
    except TypeError:  # pragma: no cover - Python 3.11
        tf.extractall(dest)  # noqa: S202 - members validated above


def install_temurin(major: int, dest_root: str | Path, *, release_name: str | None = None, api_base: str = ADOPTIUM_API,
                    os_name: str | None = None, arch: str | None = None, timeout: float = 600.0) -> JavaInfo:
    """Download Eclipse Temurin ``major`` from the Adoptium API into ``dest_root/<major>`` and return its :class:`JavaInfo`.

    The API response carries the archive's SHA-256 (``binary.package.checksum``); the download is rejected with
    :class:`ChecksumMismatch` if it differs. ``release_name`` (``jdk-21.0.8+9``) pins an exact build; without it the
    newest GA build of the feature release is used and the resolved release name + checksum are logged for the lock file.
    """
    dest_root = Path(dest_root)
    os_n, arch_n = (os_name, arch) if os_name and arch else _adoptium_os_arch()
    base = api_base.rstrip("/")
    try:
        if release_name:
            assets = _http_json(f"{base}/v3/assets/release_name/eclipse/{urllib.parse.quote(release_name)}?architecture={arch_n}&image_type=jdk&os={os_n}")
            assets = assets if isinstance(assets, list) else [assets]
        else:
            assets = _http_json(f"{base}/v3/assets/latest/{major}/hotspot?architecture={arch_n}&image_type=jdk&os={os_n}&vendor=eclipse")
    except (urllib.error.URLError, OSError, ValueError) as exc:
        raise PrerequisiteMissing(f"cannot query the Adoptium API at {base}: {exc}",
                                  remediation="Check outbound access to api.adoptium.net (or set a mirror), or install a JDK manually and set JAVA_HOME.") from exc
    if not assets:
        raise PrerequisiteMissing(f"Adoptium has no JDK {major} for {os_n}/{arch_n}")
    pkg = assets[0]["binary"]["package"]
    link, expected, name = pkg["link"], str(pkg["checksum"]).lower(), pkg["name"]
    release = assets[0].get("release_name", release_name or "unknown")
    log.info("downloading Temurin %s (%s)", release, name)
    target = dest_root / str(major)
    dest_root.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f".jdk{major}-", dir=dest_root))
    try:
        archive = stage / name
        h = hashlib.sha256()
        req = urllib.request.Request(link, headers={"User-Agent": "olcf-runtime-manager/0.1"})  # noqa: S310
        try:
            with urllib.request.urlopen(req, timeout=timeout) as fh, open(archive, "wb") as out:  # noqa: S310
                while chunk := fh.read(1024 * 1024):
                    out.write(chunk)
                    h.update(chunk)
        except (urllib.error.URLError, OSError) as exc:
            raise PrerequisiteMissing(f"JDK download failed: {exc}", remediation="Retry, or install a JDK manually.") from exc
        if h.hexdigest() != expected:
            raise ChecksumMismatch(f"JDK archive sha256 {h.hexdigest()} does not match the Adoptium-published {expected}",
                                   remediation="Do not use this download. Check your proxy/mirror, then retry.")
        extract = stage / "x"
        extract.mkdir()
        if name.endswith(".zip"):
            with zipfile.ZipFile(archive) as zf:
                root = extract.resolve()
                for member in zf.namelist():
                    if not str((extract / member).resolve()).startswith(str(root)):
                        raise PrerequisiteMissing(f"JDK archive contains an unsafe path: {member}")
                zf.extractall(extract)  # noqa: S202 - members validated above
        else:
            with tarfile.open(archive) as tf:
                _safe_extract_tar(tf, extract)
        children = [c for c in extract.iterdir() if c.is_dir()]
        home_dir = children[0] if len(children) == 1 else extract
        if (home_dir / "Contents" / "Home").is_dir():  # macOS layout
            home_dir = home_dir / "Contents" / "Home"
        if target.exists():
            shutil.rmtree(target)
        shutil.move(str(home_dir), str(target))
        (target / "OLCF-JDK.json").write_text(json.dumps({"release": release, "sha256": expected, "source": link, "archive": name}, indent=2))
    finally:
        shutil.rmtree(stage, ignore_errors=True)
    exe = "java.exe" if os.name == "nt" else "java"
    info = probe_java(target / "bin" / exe)
    if not info:
        raise PrerequisiteMissing(f"downloaded JDK at {target} does not run", remediation="Delete the directory and retry, or install a JDK manually.")
    return info
