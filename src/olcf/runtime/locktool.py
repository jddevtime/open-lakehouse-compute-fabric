# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Generate hash-pinned runtime manifests (the engine behind ``scripts/lock_runtime.py``).

How a manifest gets its pins:

1. ``uv pip compile --universal`` resolves the top-level requirements to an exact, platform-independent set of
   ``name==version ; marker`` pins for the target Python series (this needs network access to the package index);
2. for every pin the PyPI JSON API (``<index>/pypi/<name>/<version>/json``) lists the release files with their
   sha256; we keep the hash of every wheel that is installable on one of the *target platforms* (linux x86_64 /
   aarch64, macOS arm64 / x86_64, Windows x86_64 / arm64) for the target Python, plus the sdist when some target
   platform has no wheel (so ``pip`` can still build it) - nothing else is allowed to match;
3. the result is written into the manifest (``packages``), and the manifest is signed (Ed25519).

``pip install --require-hashes`` then accepts exactly those artifacts. Re-run the script to bump pins; the diff of the
manifest files is the audit trail.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import tempfile
import urllib.request
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from packaging.utils import InvalidWheelFilename, canonicalize_name, parse_wheel_filename

from olcf.runtime.errors import ManifestInvalid, PrerequisiteMissing

TARGET_PLATFORMS: dict[str, re.Pattern[str]] = {
    "linux-x86_64": re.compile(r"^(any|manylinux[\w.]*_x86_64|linux_x86_64)$"),
    "linux-aarch64": re.compile(r"^(any|manylinux[\w.]*_aarch64|linux_aarch64)$"),
    "macos-arm64": re.compile(r"^(any|macosx_\d+_\d+_(arm64|universal2))$"),
    "macos-x86_64": re.compile(r"^(any|macosx_\d+_\d+_(x86_64|universal2|intel|fat64|fat32))$"),
    "windows-x86_64": re.compile(r"^(any|win_amd64)$"),
    "windows-arm64": re.compile(r"^(any|win_arm64)$"),
}
_PIN_RE = re.compile(r"^(?P<name>[A-Za-z0-9][A-Za-z0-9._-]*)==(?P<version>[^\s;]+)\s*(?:;\s*(?P<marker>.+))?$")


def compile_requirements(top_level: list[str], python_version: str, *, constraints: list[str] | None = None,
                         index_url: str | None = None, uv: str | None = None) -> list[tuple[str, str, str | None]]:
    """Resolve ``top_level`` into exact ``(name, version, marker)`` pins with ``uv pip compile --universal``."""
    uv = uv or shutil.which("uv")
    if not uv:
        raise PrerequisiteMissing("`uv` is required to generate runtime locks", remediation="Install uv (https://docs.astral.sh/uv/) and retry.")
    with tempfile.TemporaryDirectory() as td:
        req = Path(td) / "requirements.in"
        req.write_text("\n".join(top_level) + "\n")
        cmd = [uv, "pip", "compile", str(req), "--universal", "--python-version", python_version, "--no-header", "--no-annotate", "-o", str(Path(td) / "out.txt"), "-q"]
        if constraints:
            cons = Path(td) / "constraints.txt"
            cons.write_text("\n".join(constraints) + "\n")
            cmd += ["-c", str(cons)]
        if index_url:
            cmd += ["--index-url", index_url]
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=900, check=False)  # noqa: S603
        if res.returncode != 0:
            raise ManifestInvalid(f"uv pip compile failed: {res.stderr[-800:]}")
        pins: list[tuple[str, str, str | None]] = []
        for line in (Path(td) / "out.txt").read_text().splitlines():
            line = line.strip()
            if not line or line.startswith(("#", "-")):
                continue
            m = _PIN_RE.match(line)
            if not m:
                raise ManifestInvalid(f"cannot parse locked requirement {line!r}")
            pins.append((m.group("name"), m.group("version"), (m.group("marker") or "").strip() or None))
        return pins


def pypi_release_files(name: str, version: str, index: str = "https://pypi.org/pypi", timeout: float = 60.0) -> list[dict[str, Any]]:
    """Files of a release from the JSON API: ``[{filename, sha256, packagetype}]``."""
    url = f"{index.rstrip('/')}/{canonicalize_name(name)}/{version}/json"
    req = urllib.request.Request(url, headers={"User-Agent": "olcf-lock-tool/0.1"})  # noqa: S310
    last: Exception | None = None
    for _ in range(3):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as fh:  # noqa: S310
                data = json.loads(fh.read().decode())
            return [{"filename": u["filename"], "sha256": u["digests"]["sha256"], "packagetype": u["packagetype"], "yanked": u.get("yanked", False)} for u in data["urls"]]
        except Exception as exc:  # noqa: BLE001
            last = exc
    raise ManifestInvalid(f"cannot fetch release metadata for {name}=={version} from {url}: {last}")


def _wheel_matches(filename: str, series: str, platform_re: re.Pattern[str]) -> bool:
    try:
        _, _, _, tags = parse_wheel_filename(filename)
    except InvalidWheelFilename:
        return False
    minor = int(series.split(".")[1])
    for t in tags:
        interp, abi, plat = t.interpreter, t.abi, t.platform
        py_ok = (
            interp in (f"cp3{minor}", "py3", f"py3{minor}", "py2", "cp3")
            or (abi == "abi3" and re.fullmatch(r"cp3(\d+)", interp) is not None and int(interp[3:]) <= minor)
        )
        abi_ok = abi in ("none", "abi3", f"cp3{minor}")
        if not (py_ok and abi_ok):
            continue
        # a platform tag can be a compressed set "manylinux_2_17_x86_64.manylinux2014_x86_64" -> parse_wheel_filename already splits
        if platform_re.match(plat):
            return True
    return False


def select_hashes(files: list[dict[str, Any]], series: str, targets: dict[str, re.Pattern[str]] | None = None) -> list[str]:
    """Hashes to pin: every wheel usable on some target platform + the sdist if a platform lacks a wheel."""
    targets = targets or TARGET_PLATFORMS
    wheels = [f for f in files if f["packagetype"] == "bdist_wheel" and not f.get("yanked")]
    sdists = [f for f in files if f["packagetype"] == "sdist" and not f.get("yanked")]
    chosen: dict[str, str] = {}
    uncovered = False
    for _plat, rx in targets.items():
        hits = [w for w in wheels if _wheel_matches(w["filename"], series, rx)]
        for w in hits:
            chosen[w["filename"]] = w["sha256"]
        if not hits:
            uncovered = True
    if uncovered or not chosen:
        for s in sdists:
            chosen[s["filename"]] = s["sha256"]
    if not chosen:
        raise ManifestInvalid("release has no usable file for the target platforms")
    return sorted(set(chosen.values()))


def lock_packages(pins: list[tuple[str, str, str | None]], series: str, *, index: str = "https://pypi.org/pypi", workers: int = 8,
                  progress: Callable[[str], None] | None = None) -> list[dict[str, Any]]:
    """Attach the hash sets to resolved pins."""

    def one(pin: tuple[str, str, str | None]) -> dict[str, Any]:
        name, version, marker = pin
        files = pypi_release_files(name, version, index)
        entry: dict[str, Any] = {"name": name, "version": version, "hashes": select_hashes(files, series)}
        if marker:
            entry["marker"] = marker
        if progress:
            progress(f"{name}=={version}: {len(entry['hashes'])} hash(es)")
        return entry

    with ThreadPoolExecutor(max_workers=workers) as ex:
        return sorted(ex.map(one, pins), key=lambda e: (canonicalize_name(e["name"]), e.get("marker") or ""))


def build_manifest(spec: dict[str, Any], *, index: str = "https://pypi.org/pypi", progress: Callable[[str], None] | None = None) -> dict[str, Any]:
    """Turn a lock *spec* (see ``scripts/lock_runtime.py``) into an unsigned manifest dict with real hashes."""
    series = spec["python"]["version"]
    out = {k: v for k, v in spec.items() if k not in ("top_level", "build_top_level", "constraints")}
    out["schema"] = "olcf.runtime.manifest/1"
    build_pins = compile_requirements(spec["build_top_level"], series) if spec.get("build_top_level") else []
    out["build_packages"] = lock_packages(build_pins, series, index=index, progress=progress)
    pins = compile_requirements(spec["top_level"], series, constraints=spec.get("constraints"))
    build_names = {canonicalize_name(n) for n, _, _ in build_pins}
    # packages already installed as build requirements are installed again by the main lock (same pins) - allowed by pip
    out["packages"] = lock_packages(pins, series, index=index, progress=progress)
    _ = build_names
    return out
