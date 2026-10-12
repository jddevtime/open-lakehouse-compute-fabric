#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Release gate: inspect the built wheel and sdist before anything is signed or published.  Standard library only.

    python scripts/verify_dist.py dist/                  # checks every olcf-* and olcf_fabric-* artifact in the directory
    python scripts/verify_dist.py dist/ --require-sdk    # ... and fails when the Fabric SDK wheel or source archive is missing
    python scripts/verify_dist.py dist/olcf-0.1.0-py3-none-any.whl

Two sets of rules, because a source archive and an installed wheel are different things:

* **Everywhere** (wheels and source archives): no private keys other than the documented public development key, no real ``.env`` files, databases,
  bytecode, tool caches (``.hypothesis``, ``.pytest_cache``, ...), virtual environments or ``.git``.
* **Installed wheels only**: no ``tests/``, ``.github/`` or ``deploy/.env.example`` either.  A **source archive** (sdist) is meant to be complete, so
  it may carry the public tests, the CI definitions and exactly one environment file, ``deploy/.env.example`` (placeholders, no secrets).

Checks: names and versions agree with ``pyproject.toml`` and ``olcf.version``; required package files are present (CLI, API, web UI, schemas,
entry points); wheel metadata is sane (name, ``Requires-Python``, license, license files); the wheel is pure-python (``py3-none-any``) so it
installs on a Mac without compilers.  The Fabric SDK (``olcf-fabric``, ``sdk/fabric``) has its own gate: same version as the server, stdlib only
(no ``Requires-Dist``), the ``olcf_fabric`` modules, Apache-2.0 metadata, ``LICENSE`` and ``NOTICE``.
"""

from __future__ import annotations

import re
import sys
import tarfile
import tomllib
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REQUIRED_WHEEL = ("olcf/cli/main.py", "olcf/api/app.py", "olcf/sdk/client.py", "olcf/web/static/index.html", "olcf/schemas/", "olcf/runtime/manifests/",
                  "olcf/worker/main.py", "olcf/version.py")
REQUIRED_SDIST = ("pyproject.toml", "LICENSE", "NOTICE", "README.md", "src/olcf/cli/main.py", "schemas/")
#: The Fabric SDK distribution (``sdk/fabric``): import name ``olcf_fabric``, stdlib only, versioned with the server.
SDK_MODULES = tuple(f"olcf_fabric/{m}.py" for m in ("__init__", "_canonical", "_http", "api", "auth", "capsule", "client", "config", "errors", "fabric"))
REQUIRED_SDK_WHEEL = SDK_MODULES
REQUIRED_SDK_SDIST = ("pyproject.toml", "LICENSE", "NOTICE", "README.md", "PKG-INFO", *SDK_MODULES)
#: The documented public development key (src/olcf/runtime/keys/README.md says the private half is public on purpose).
ALLOWED_KEYS = {"olcf/runtime/keys/olcf-dev.key"}
#: Never in any artifact: private keys, real environment files, databases, bytecode, caches, virtual environments, VCS data.
FORBIDDEN = re.compile(r"(^|/)(\.env(\..*)?|.*\.(pem|p12|pfx|key|db|db-wal|db-shm|sqlite3?|pyc|pyo)|__pycache__|\.venv|\.git|\.pytest_cache|\.mypy_cache|\.ruff_cache|\.hypothesis|"
                       r"\.coverage|htmlcov|\.DS_Store|id_(rsa|ed25519))(/|$)", re.I)
#: Not in an installed wheel (a source archive is allowed to carry them).
WHEEL_FORBIDDEN_DIRS = ("tests/", "olcf/tests/", ".github/")
#: The one environment file a source archive may carry: public placeholders only.  Matched exactly, never by pattern.
SDIST_ALLOWED_ENV = frozenset({"deploy/.env.example"})
LICENSE_FILES = ("LICENSE", "NOTICE")


def project_version() -> str:
    return str(tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]["version"])


def module_version() -> str:
    m = re.search(r'__version__\s*=\s*"([^"]+)"', (ROOT / "src" / "olcf" / "version.py").read_text(encoding="utf-8"))
    return m.group(1) if m else "?"


def check_names(names: list[str], required: tuple[str, ...], label: str, strip: str = "", *, source: bool = False) -> list[str]:
    """Required members present, forbidden members absent.  ``source=True`` applies the source-archive allowances (tests, CI files, ``deploy/.env.example``)."""
    errs = []
    rel = [n[len(strip):] if strip and n.startswith(strip) else n for n in names]
    for req in required:
        if not any(n == req or (req.endswith("/") and n.startswith(req)) for n in rel):
            errs.append(f"{label}: missing {req}")
    for n in rel:
        norm = n.rstrip("/")
        if source and norm in SDIST_ALLOWED_ENV:
            continue
        if FORBIDDEN.search(n) and n.removeprefix("src/") not in ALLOWED_KEYS:
            errs.append(f"{label}: must not ship: {n}")
        elif not source and n.startswith(WHEEL_FORBIDDEN_DIRS):
            errs.append(f"{label}: must not ship: {n}")
    return errs


def license_members(names: list[str], dist_info: str) -> list[str]:
    """Problems with the license files inside ``<dist-info>/`` (new layout ``licenses/``, or the older flat one)."""
    errs = []
    for lic in LICENSE_FILES:
        if f"{dist_info}/licenses/{lic}" not in names and f"{dist_info}/{lic}" not in names:
            errs.append(f"missing {dist_info}/licenses/{lic}")
    return errs


def check_wheel(path: Path, version: str) -> list[str]:
    errs: list[str] = []
    if path.name != f"olcf-{version}-py3-none-any.whl":
        errs.append(f"{path.name}: expected olcf-{version}-py3-none-any.whl (pure python, version {version})")
    with zipfile.ZipFile(path) as zf:
        names = zf.namelist()
        errs += check_names(names, REQUIRED_WHEEL, path.name)
        dist_info = f"olcf-{version}.dist-info"
        for need in ("METADATA", "WHEEL", "RECORD", "entry_points.txt"):
            if f"{dist_info}/{need}" not in names:
                errs.append(f"{path.name}: missing {dist_info}/{need}")
        if f"{dist_info}/METADATA" in names:
            meta = zf.read(f"{dist_info}/METADATA").decode("utf-8", "replace")
            for needle in ("Name: olcf", f"Version: {version}", "Requires-Python: >=3.11", "Apache-2.0"):
                if needle not in meta:
                    errs.append(f"{path.name}: METADATA lacks '{needle}'")
            for dep in ("Requires-Dist: packaging", "Requires-Dist: click", "Requires-Dist: fastapi"):
                if dep not in meta:
                    errs.append(f"{path.name}: METADATA lacks '{dep}' (imported at start-up; must be a hard dependency)")
        errs += [f"{path.name}: {e}" for e in license_members(names, dist_info)]
        if f"{dist_info}/entry_points.txt" in names:
            ep = zf.read(f"{dist_info}/entry_points.txt").decode()
            if "olcf = olcf.cli.main:main" not in ep or "olcf-worker" not in ep:
                errs.append(f"{path.name}: console scripts olcf / olcf-worker missing")
        if f"{dist_info}/WHEEL" in names and "Root-Is-Purelib: true" not in zf.read(f"{dist_info}/WHEEL").decode():
            errs.append(f"{path.name}: not a pure-python wheel")
        for name in names:
            if name.endswith((".so", ".dylib", ".pyd", ".dll")):
                errs.append(f"{path.name}: native code in a pure wheel: {name}")
        for name in names:
            if name.endswith(".py"):
                head = zf.read(name)[:200].decode("utf-8", "replace")
                if name.count("/") >= 1 and "SPDX-License-Identifier" not in head and zf.getinfo(name).file_size > 0:
                    errs.append(f"{path.name}: {name} has no SPDX header")
    return errs


def check_sdist(path: Path, version: str) -> list[str]:
    errs: list[str] = []
    if path.name != f"olcf-{version}.tar.gz":
        errs.append(f"{path.name}: expected olcf-{version}.tar.gz")
    with tarfile.open(path) as tf:
        names = tf.getnames()
        if any(n.startswith(("/", "..")) or "/../" in n for n in names):
            errs.append(f"{path.name}: unsafe member paths")
        errs += check_names(names, REQUIRED_SDIST, path.name, strip=f"olcf-{version}/", source=True)
        if not any(n.endswith("PKG-INFO") for n in names):
            errs.append(f"{path.name}: no PKG-INFO")
    return errs


def sdk_version() -> str:
    m = re.search(r'__version__\s*=\s*"([^"]+)"', (ROOT / "sdk" / "fabric" / "olcf_fabric" / "__init__.py").read_text(encoding="utf-8"))
    return m.group(1) if m else "?"


def check_sdk_wheel(path: Path, version: str) -> list[str]:
    """The ``olcf-fabric`` wheel: pure python, stdlib only, the SDK modules and nothing of the server, license and notice shipped."""
    errs: list[str] = []
    label = path.name
    if path.name != f"olcf_fabric-{version}-py3-none-any.whl":
        errs.append(f"{label}: expected olcf_fabric-{version}-py3-none-any.whl (pure python, same version as the server)")
    with zipfile.ZipFile(path) as zf:
        names = zf.namelist()
        errs += check_names(names, REQUIRED_SDK_WHEEL, label)
        dist_info = f"olcf_fabric-{version}.dist-info"
        for need in ("METADATA", "WHEEL", "RECORD", "top_level.txt"):
            if f"{dist_info}/{need}" not in names:
                errs.append(f"{label}: missing {dist_info}/{need}")
        errs += [f"{label}: {e}" for e in license_members(names, dist_info)]
        if f"{dist_info}/METADATA" in names:
            meta = zf.read(f"{dist_info}/METADATA").decode("utf-8", "replace")
            header = meta.split("\n\n", 1)[0]
            for needle in ("Name: olcf-fabric", f"Version: {version}", "Requires-Python: >=3.10", "Apache-2.0", "License-File: LICENSE", "License-File: NOTICE"):
                if needle not in header:
                    errs.append(f"{label}: METADATA lacks '{needle}'")
            if "Requires-Dist:" in header:
                errs.append(f"{label}: METADATA declares dependencies; the SDK is standard library only (it is attached to a Fabric Environment)")
        if f"{dist_info}/WHEEL" in names and "Root-Is-Purelib: true" not in zf.read(f"{dist_info}/WHEEL").decode():
            errs.append(f"{label}: not a pure-python wheel")
        for name in names:
            top = name.split("/", 1)[0]
            if top not in ("olcf_fabric", dist_info):
                errs.append(f"{label}: unexpected top-level member {name} (the SDK wheel holds only olcf_fabric/)")
            if name.endswith((".so", ".dylib", ".pyd", ".dll")):
                errs.append(f"{label}: native code in a pure wheel: {name}")
            if name.endswith(".py") and zf.getinfo(name).file_size > 0:
                head = zf.read(name)[:200].decode("utf-8", "replace")
                if "SPDX-License-Identifier" not in head:
                    errs.append(f"{label}: {name} has no SPDX header")
                if re.search(r"^\s*(from|import) olcf(\.|\s|$)", zf.read(name).decode("utf-8", "replace"), re.M):
                    errs.append(f"{label}: {name} imports the server package 'olcf'")
    return errs


def check_sdk_sdist(path: Path, version: str) -> list[str]:
    """The ``olcf-fabric`` source archive: same rules as the server's (source allowances, no secrets or caches), plus the SDK modules, license and notice."""
    errs: list[str] = []
    if path.name != f"olcf_fabric-{version}.tar.gz":
        errs.append(f"{path.name}: expected olcf_fabric-{version}.tar.gz (same version as the server)")
    with tarfile.open(path) as tf:
        names = tf.getnames()
        if any(n.startswith(("/", "..")) or "/../" in n for n in names):
            errs.append(f"{path.name}: unsafe member paths")
        errs += check_names(names, REQUIRED_SDK_SDIST, path.name, strip=f"olcf_fabric-{version}/", source=True)
    return errs


def main(argv: list[str]) -> int:
    args = [a for a in argv[1:] if not a.startswith("--")]
    require_sdk = "--require-sdk" in argv[1:]
    target = Path(args[0]) if args else ROOT / "dist"
    files = sorted([*target.glob("olcf-*"), *target.glob("olcf_fabric-*")]) if target.is_dir() else [target]
    version, mod, sdk = project_version(), module_version(), sdk_version()
    errs: list[str] = []
    if version != mod:
        errs.append(f"version mismatch: pyproject.toml says {version}, src/olcf/version.py says {mod}")
    if sdk != version:
        errs.append(f"version mismatch: sdk/fabric/olcf_fabric/__init__.py says {sdk}, the server is {version} (the SDK is versioned with the server)")
    wheels = [f for f in files if f.suffix == ".whl" and f.name.startswith("olcf-")]
    sdists = [f for f in files if f.name.endswith(".tar.gz") and f.name.startswith("olcf-")]
    sdk_wheels = [f for f in files if f.suffix == ".whl" and f.name.startswith("olcf_fabric-")]
    sdk_sdists = [f for f in files if f.name.endswith(".tar.gz") and f.name.startswith("olcf_fabric-")]
    if not (wheels or sdists or sdk_wheels or sdk_sdists):
        errs.append(f"no olcf or olcf-fabric wheel or sdist found in {target}")
    if require_sdk and not (sdk_wheels and sdk_sdists):
        errs.append(f"the Fabric SDK wheel and source archive (olcf_fabric-{version}*) are required but not both found in {target}")
    for w in wheels:
        errs += check_wheel(w, version)
    for s in sdists:
        errs += check_sdist(s, version)
    for w in sdk_wheels:
        errs += check_sdk_wheel(w, version)
    for s in sdk_sdists:
        errs += check_sdk_sdist(s, version)
    for e in errs:
        print(f"FAIL {e}", file=sys.stderr)
    print(f"verify_dist: {len(wheels)} wheel(s), {len(sdists)} sdist(s), {len(sdk_wheels)} SDK wheel(s), {len(sdk_sdists)} SDK sdist(s), version {version}: "
          f"{'FAILED' if errs else 'ok'}", file=sys.stderr)
    return 1 if errs else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
