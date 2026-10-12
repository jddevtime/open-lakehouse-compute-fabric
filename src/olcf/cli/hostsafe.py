# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Stub-safe host discovery - the CLI must work on a Mac whose Xcode Command Line Tools **cannot** be installed.

On a stock macOS without the Command Line Tools ``/usr/bin/python3``, ``/usr/bin/git``, ``/usr/bin/java`` (and clang, make, ...) are *stubs*:
executing one pops up a dialog asking to install developer tools (or Java) and then fails.  OLCF never needs them - it brings its own
``uv`` -> python-build-standalone -> wheels-only virtualenv and a checksum-verified Temurin JDK - so nothing in this module (or in the
installers) may ever **execute** one.  What this module offers:

* :func:`is_apple_stub` - recognises the stubs (always for ``java``; for the others unless ``xcode-select -p`` points at a real toolchain).
  ``xcode-select -p`` is safe: it prints a path or an error and never opens a dialog.
* :func:`safe_which` - ``shutil.which`` that skips stubs.
* :func:`find_jdks` - JDK discovery that **reads** ``<home>/release`` instead of running ``java`` and never consults ``/usr/bin/java`` or
  ``/usr/libexec/java_home``.
* :func:`protect_environment` - on macOS without developer tools, prepends a per-user directory of *guard shims* (``git``, ``java``, ``python3``,
  ``python``, ``javac``, ``cc``, ``clang``, ``gcc``, ``make``) to ``PATH`` so that code which still does ``subprocess.run(["python3", ...])`` (including
  child processes and the embedded worker, which inherit the environment) hits a harmless ``exit 127`` instead of the installer dialog.  The ``java`` shim
  forwards to a real JDK (``$OLCF_JAVA_HOME``, ``$JAVA_HOME``, OLCF's downloaded Temurin) when one exists.
"""

from __future__ import annotations

import contextlib
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path

from olcf.core import hostbin

#: Executables that are Apple stubs without a real toolchain (``java`` is a stub even *with* one - see :func:`is_apple_stub`).
SHIM_NAMES = ("git", "java", "javac", "python3", "python", "pip3", "cc", "clang", "gcc", "make")


def is_macos() -> bool:
    return hostbin.is_macos()


def developer_tools_installed() -> bool:
    """True when ``xcode-select -p`` names a real developer directory (never opens a dialog; see :mod:`olcf.core.hostbin`)."""
    return hostbin.developer_tools_installed() if is_macos() else True


def reset_cache() -> None:
    hostbin.clear_cache()


def is_apple_stub(path: str | os.PathLike[str]) -> bool:
    """Would executing ``path`` risk the "install developer tools / Java" dialog?  Pure inspection."""
    return hostbin.is_installer_stub(path)


def safe_which(name: str, path: str | None = None) -> str | None:
    """Like ``shutil.which`` but never returns an Apple stub or one of our own guard shims."""
    raw = os.environ.get("PATH", os.defpath) if path is None else path
    guard = {os.path.abspath(g) for g in (os.environ.get("OLCF_STUB_GUARD"), str(shim_dir())) if g}
    clean = os.pathsep.join(d for d in raw.split(os.pathsep) if d and os.path.abspath(d) not in guard)       # our own shim directory is not "a real tool"
    found = hostbin.find_real_binary(name, path=clean)
    return None if found is None or _is_guard_shim(found) else found


def _is_guard_shim(path: str) -> bool:
    try:
        return "OLCF-GUARD-SHIM" in Path(path).read_text(errors="ignore")[:300]
    except (OSError, UnicodeDecodeError):
        return False


# --------------------------------------------------------------------------------------------- JDK discovery without executing java
@dataclass(frozen=True)
class Jdk:
    """A JDK found on this machine: its home directory, major and full version, vendor and where it was found."""
    home: str
    major: int
    version: str
    vendor: str
    source: str

    def to_dict(self) -> dict[str, object]:
        return {"home": self.home, "major": self.major, "version": self.version, "vendor": self.vendor, "source": self.source}


def _read_release(home: Path) -> Jdk | None:
    rel = home / "release"
    java = home / "bin" / ("java.exe" if os.name == "nt" else "java")
    if not rel.is_file() or not java.exists():
        return None
    props = dict(re.findall(r'^([A-Z_]+)="?([^"\n]*)"?$', rel.read_text(errors="ignore"), flags=re.M))
    ver = props.get("JAVA_VERSION", "")
    m = re.match(r"(?:1\.)?(\d+)", ver)
    if not m:
        return None
    return Jdk(str(home), int(m.group(1)), ver, props.get("IMPLEMENTOR", ""), "")


def jdk_roots(extra_state_dirs: list[Path] | None = None) -> list[tuple[str, Path]]:
    """Directories that may contain JDK homes, with a label for the source."""
    roots: list[tuple[str, Path]] = []
    home = Path.home()
    for d in extra_state_dirs or []:
        roots.append(("olcf", d / "runtimes" / "_jdk"))
    roots.append(("olcf", home / ".olcf-worker" / "runtimes" / "_jdk"))
    roots += [("system", Path("/usr/lib/jvm")), ("system", Path("/Library/Java/JavaVirtualMachines")), ("user", home / "Library" / "Java" / "JavaVirtualMachines"),
              ("homebrew", Path("/opt/homebrew/opt")), ("homebrew", Path("/usr/local/opt")), ("sdkman", home / ".sdkman" / "candidates" / "java"),
              ("system", Path("C:/Program Files/Eclipse Adoptium")), ("system", Path("C:/Program Files/Java")), ("system", Path("C:/Program Files/Microsoft"))]
    return roots


def find_jdks(extra_state_dirs: list[Path] | None = None) -> list[Jdk]:
    """All JDK homes found by *reading* their ``release`` files (nothing is executed)."""
    found: dict[str, Jdk] = {}

    def consider(h: Path, source: str) -> None:
        for cand in (h, h / "Contents" / "Home", h / "libexec" / "openjdk.jdk" / "Contents" / "Home"):
            try:
                j = _read_release(cand)
            except OSError:
                continue
            if j:
                found.setdefault(str(cand.resolve()), Jdk(j.home, j.major, j.version, j.vendor, source))
                return

    for var in ("OLCF_JAVA_HOME", "JAVA_HOME"):
        v = os.environ.get(var)
        if v:
            consider(Path(v), var)
    for label, root in jdk_roots(extra_state_dirs):
        if not root.is_dir():
            continue
        with contextlib.suppress(OSError):
            for child in sorted(root.iterdir()):
                if root.name == "opt" and "openjdk" not in child.name and "temurin" not in child.name:
                    continue
                if child.is_dir():
                    consider(child, label)
    return sorted(found.values(), key=lambda j: -j.major)


# --------------------------------------------------------------------------------------------- guard shims
SHIM_TEMPLATE = """#!/bin/sh
# OLCF-GUARD-SHIM - generated by `olcf`; safe to delete (it is recreated when needed).
# This Mac has no Xcode Command Line Tools. The real /usr/bin/{name} is a stub that would open an installer dialog, and
# OLCF does not need it: it uses its own uv-managed Python and a downloaded JDK. Failing quietly here keeps the dialog away.
{body}
"""

PATH_SEARCH = """self_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
old_ifs=$IFS; IFS=:
for d in $PATH; do
  [ -n "$d" ] || continue
  [ "$d" = "$self_dir" ] && continue
  [ "$d" = "/usr/bin" ] && continue
  case ":$OLCF_STUB_DIRS:" in *":$d:"*) continue;; esac
  if [ -x "$d/{name}" ] && ! {{ head -c 300 "$d/{name}" 2>/dev/null | grep -qa OLCF-GUARD-SHIM; }}; then IFS=$old_ifs; exec "$d/{name}" "$@"; fi
done
IFS=$old_ifs"""

JAVA_BODY = """for h in "$OLCF_JAVA_HOME" "$JAVA_HOME" {olcf_homes}; do
  if [ -n "$h" ] && [ -x "$h/bin/{name}" ]; then exec "$h/bin/{name}" "$@"; fi
done
if [ -x /usr/libexec/java_home ] && h=$(/usr/libexec/java_home 2>/dev/null) && [ -x "$h/bin/{name}" ]; then exec "$h/bin/{name}" "$@"; fi
""" + PATH_SEARCH + """
echo "olcf: no usable Java found ('{name}'). Run: olcf runtime install --with-jdk   (macOS /usr/bin/{name} is an installer stub and is never run)" >&2
exit 127"""

DEFAULT_BODY = PATH_SEARCH + """
echo "olcf: '{name}' is not available on this Mac (developer tools are not installed and OLCF does not need them; install one with Homebrew if you want it)." >&2
exit 127"""


def shim_dir() -> Path:
    from olcf.sdk.contexts import config_dir

    return config_dir() / "stubguard"


def ensure_shims(directory: Path | None = None, names: tuple[str, ...] = SHIM_NAMES) -> Path:
    """Create (or refresh) the guard shims for ``names`` and return their directory."""
    d = directory or shim_dir()
    d.mkdir(parents=True, exist_ok=True)
    homes = " ".join(f'"{j.home}"' for j in find_jdks() if j.source == "olcf")
    for name in names:
        body = JAVA_BODY.format(name=name, olcf_homes=homes) if name in ("java", "javac") else DEFAULT_BODY.format(name=name)
        text = SHIM_TEMPLATE.format(name=name, body=body)
        f = d / name
        if not f.exists() or f.read_text() != text:
            f.write_text(text)
        f.chmod(f.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    for stale in SHIM_NAMES:                          # a toolchain appeared since last time: stop shadowing its tools
        if stale not in names:
            with contextlib.suppress(OSError):
                (d / stale).unlink()
    return d


def protect_environment() -> Path | None:
    """macOS: put the guard shims first on ``PATH`` for this process and everything it spawns.

    Without developer tools every stub is shadowed; with them only ``java``/``javac`` are (``/usr/bin/java`` is a launcher stub either way and
    OLCF only ever starts its own JDK).  Returns the shim directory when protection is active, ``None`` elsewhere (or with ``OLCF_NO_STUB_GUARD=1``).
    Idempotent.
    """
    if not is_macos() or os.environ.get("OLCF_NO_STUB_GUARD") == "1":
        return None
    try:
        d = ensure_shims(names=("java", "javac") if developer_tools_installed() else SHIM_NAMES)
    except OSError:
        return None
    parts = os.environ.get("PATH", "").split(os.pathsep)
    if str(d) not in parts:
        os.environ["PATH"] = os.pathsep.join([str(d), *parts])
    os.environ["OLCF_STUB_GUARD"] = str(d)
    return d
