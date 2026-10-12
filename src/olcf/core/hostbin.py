# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Find host executables without ever running a macOS *installer stub*.  **Standard library only.**

Why this exists: on a Mac that has neither Xcode nor the Command Line Tools, ``/usr/bin/git``, ``/usr/bin/python3`` and
``/usr/bin/java`` are not real programs.  They are tiny shims that pop up a GUI dialog ("The 'git' command requires the command
line developer tools ..." / "No Java runtime present, requesting install") and block until the user reacts.  A background service
that merely *probes* ``git --version`` or ``java -version`` would therefore freeze or spam the desktop of the person who installed
OLCF.  Everything in OLCF that looks up ``java``, ``git`` or ``python`` goes through :func:`find_real_binary` so that

* a stub is **never executed** (not even to see whether it works - only ``xcode-select -p`` and ``/usr/libexec/java_home``, which are
  real programs and never prompt, are run to find out whether the shim would forward to something real);
* the search continues past a rejected stub to the next ``PATH`` entry (Homebrew, python.org, SDKMAN, a managed JDK ...);
* the rejection reason is available (:func:`explain_missing`) so callers can produce actionable remediation text.

On Linux and Windows nothing is rejected: the functions then behave like :func:`shutil.which` (plus the extra candidates the
caller passes in).  The platform / tool locations are module level so tests can simulate macOS on any host.
"""

from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from pathlib import Path

#: Names whose copies in :data:`STUB_DIRS` are installer shims on a Mac without developer tools.
STUB_NAMES = frozenset({
    "git", "java", "javac", "jar", "python", "python3", "pip", "pip3", "make", "gcc", "g++", "cc", "c++", "clang", "clang++", "ld", "svn",
    "swift", "swiftc", "xcrun", "ar", "nm", "otool", "strip", "lldb", "ruby",
})
#: Directories that contain the shims.
STUB_DIRS: tuple[str, ...] = ("/usr/bin",)
#: Names that are only real when a *JDK* exists (``java_home`` decides); everything else in STUB_NAMES needs the developer tools.
JAVA_NAMES = frozenset({"java", "javac", "jar", "jshell", "jcmd", "keytool"})
#: Real, non-prompting helpers (patched by tests).
XCODE_SELECT = "/usr/bin/xcode-select"
JAVA_HOME_TOOL = "/usr/libexec/java_home"

_CACHE_TTL = 30.0
_lock = threading.Lock()
_cache: dict[str, tuple[float, object]] = {}


def is_macos() -> bool:
    """Single point tests monkeypatch to simulate macOS."""
    return sys.platform == "darwin"


def _cached(key: str, fn):  # noqa: ANN001, ANN202
    now = time.monotonic()
    with _lock:
        hit = _cache.get(key)
        if hit and now - hit[0] < _CACHE_TTL:
            return hit[1]
    val = fn()
    with _lock:
        _cache[key] = (now, val)
    return val


def clear_cache() -> None:
    """Forget cached ``xcode-select`` / ``java_home`` answers (after the user installed the tools; used by tests)."""
    with _lock:
        _cache.clear()


def _run_helper(argv: list[str]) -> tuple[int, str]:
    """Run one of the two known-safe helpers; never raises, never blocks for long, never inherits stdin."""
    try:
        res = subprocess.run(argv, capture_output=True, text=True, timeout=10, check=False, stdin=subprocess.DEVNULL)  # noqa: S603
    except (OSError, subprocess.SubprocessError):
        return 127, ""
    return res.returncode, (res.stdout or "").strip()


def developer_dir() -> str | None:
    """The active developer directory (``xcode-select -p``) when the Command Line Tools / Xcode are really installed, else ``None``.

    ``xcode-select -p`` is a real binary: it prints a path or fails with exit status 2 and never opens the installer dialog.
    """
    def probe() -> str | None:
        if not Path(XCODE_SELECT).is_file():
            return None
        rc, out = _run_helper([XCODE_SELECT, "-p"])
        if rc != 0 or not out:
            return None
        return out if Path(out).is_dir() else None

    return _cached("developer_dir", probe)  # type: ignore[return-value]


def developer_tools_installed() -> bool:
    return developer_dir() is not None


def java_home() -> str | None:
    """Home of a real JDK/JRE according to ``/usr/libexec/java_home`` (macOS only), else ``None``.

    ``java_home`` only inspects ``/Library/Java/JavaVirtualMachines`` (and friends); unlike the ``/usr/bin/java`` shim it never prompts.
    """
    if not is_macos():
        return None

    def probe() -> str | None:
        if not Path(JAVA_HOME_TOOL).is_file():
            return None
        rc, out = _run_helper([JAVA_HOME_TOOL])
        if rc != 0 or not out:
            return None
        home = out.splitlines()[-1].strip()
        return home if (Path(home) / "bin" / "java").is_file() else None

    return _cached("java_home", probe)  # type: ignore[return-value]


def _in_stub_dir(candidate: Path) -> bool:
    """True when ``candidate`` lives directly in a stub directory (symlinks are *not* followed: Homebrew links live elsewhere)."""
    try:
        parent = Path(os.path.abspath(candidate.parent))
    except OSError:
        return False
    return any(parent == Path(d) for d in STUB_DIRS)


def is_installer_stub(path: str | os.PathLike[str]) -> bool:
    """Would executing ``path`` risk the macOS "install developer tools" dialog?  Pure inspection, nothing is executed.

    ``False`` on non-macOS hosts, for anything outside the stub directories and for shims whose backing tools exist
    (Command Line Tools installed; for java: a real JDK known to ``java_home``).
    """
    if not is_macos():
        return False
    p = Path(path)
    name = p.name
    if name not in STUB_NAMES and name not in JAVA_NAMES:
        return False
    if not _in_stub_dir(p):
        return False
    if name in JAVA_NAMES:
        return java_home() is None
    return not developer_tools_installed()


def _is_exec(p: Path) -> bool:
    return p.is_file() and os.access(p, os.X_OK)


def _path_entries(path: str | None) -> list[str]:
    raw = os.environ.get("PATH", os.defpath) if path is None else path
    return [d for d in raw.split(os.pathsep) if d]


def _names(name: str) -> list[str]:
    if os.name == "nt" and not Path(name).suffix:
        exts = os.environ.get("PATHEXT", ".EXE;.CMD;.BAT").split(os.pathsep)
        return [name + e.lower() for e in exts] + [name + e for e in exts] + [name]
    return [name]


def find_real_binary(name: str, *, path: str | None = None, extra: list[str | os.PathLike[str]] | None = None) -> str | None:
    """Absolute path of the first *real* executable called ``name``, or ``None``.

    Search order: ``extra`` candidates (explicit user configuration, JAVA_HOME/bin ...), then every ``PATH`` entry in order.  A candidate
    that :func:`is_installer_stub` is skipped without being executed.  On macOS and for java the JDK reported by
    ``/usr/libexec/java_home`` is used as a last resort so a system-installed JDK is found even when ``PATH`` only has the shim.
    """
    cands: list[Path] = [Path(e) for e in (extra or [])]
    for d in _path_entries(path):
        for n in _names(name):
            cands.append(Path(d) / n)
    for c in cands:
        if _is_exec(c) and not is_installer_stub(c):
            if name in JAVA_NAMES and is_macos() and _in_stub_dir(c):
                # the /usr/bin/java shim forwards to the java_home JDK: use that JDK directly (no shim indirection, no surprises)
                home = java_home()
                if home and _is_exec(Path(home) / "bin" / name):
                    return str(Path(home) / "bin" / name)
            return str(c)
    if name in JAVA_NAMES:
        home = java_home()
        if home:
            c = Path(home) / "bin" / name
            if _is_exec(c):
                return str(c)
    return None


def explain_missing(name: str) -> str:
    """Actionable text for "``name`` was not found" that mentions the macOS developer-tools situation when it applies."""
    if is_macos():
        if name in JAVA_NAMES:
            return ("No Java runtime is installed (the /usr/bin/java on this Mac is only an installer shim, OLCF never runs it). "
                    "Install a JDK 17/21, e.g. `brew install --cask temurin@21`, download Eclipse Temurin from adoptium.net, "
                    "or let OLCF download a verified one with `--with-jdk`.")
        if name in {"git", "python", "python3", "pip", "pip3"}:
            return (f"`{name}` is not installed (the /usr/bin/{name} on this Mac is only an installer shim, OLCF never runs it). "
                    "Install it without Xcode: `brew install " + ("git" if name == "git" else "python@3.13") + "`, or download the installer from "
                    + ("git-scm.com" if name == "git" else "python.org") + ".")
    return f"`{name}` was not found on PATH."
