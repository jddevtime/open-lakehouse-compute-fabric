# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Per-job layered Python environment, keyed by the dependency hash.  **Standard library only** (runtime-venv safe).

Contract with the node (CONTRACTS section 12 allows only the stdlib in runtime venvs; this module obeys it):

* The control plane publishes an environment as an immutable, hash-pinned lock (:mod:`olcf.notebooks.environments`) and hands the job
  a ``layer`` block (``EnvironmentSpec.layer``): ``{key, python, requirements, pip, jars}``.
* ``requirements`` is a pip requirements file in exactly the format the runtime manager writes for runtime installs
  (``name==version ; marker \\`` + ``    --hash=sha256:<hex>`` lines), so ``pip install --require-hashes --no-deps -r`` accepts only the artifacts
  the lock names.
* :func:`ensure_layer` installs that file with ``pip install --target <cache>/<key>`` once per key and per node (a ``LAYER.json`` marker is
  written last; concurrent callers serialise on a lock file).  Jobs with the same dependency hash share the directory read-only and put it on
  ``PYTHONPATH`` in front of the runtime's site-packages.  Different environments with identical dependencies therefore share one layer.
* :func:`layer_key` is the single definition of the key (sha256 over the sorted pins and hashes plus the python ABI), so the control plane and
  the node always agree.

Nothing here talks to the network except through ``pip`` and only to the index / wheelhouse named in the lock's ``pip`` block.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from olcf.core.filelock import FileLock, LockTimeout

MARKER = "LAYER.json"


class LayerError(Exception):
    """A layer could not be built; ``args[0]`` is an actionable message."""


def canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def requirements_text(packages: list[dict[str, Any]]) -> str:
    """Hash-pinned requirements file (same layout as ``olcf.runtime.manifest.PackagePin.requirement_lines``)."""
    blocks = []
    for p in packages:
        head = f"{p['name']}=={p['version']}"
        if p.get("marker"):
            head += f" ; {p['marker']}"
        blocks.append(" \\\n".join([head] + [f"    --hash=sha256:{h}" for h in p["hashes"]]))
    return "\n".join(blocks) + ("\n" if blocks else "")


def layer_key(packages: list[dict[str, Any]], abi: str, jars: list[str] | None = None) -> str:
    """``sha256`` over the layer's pins (name, version, sorted hashes), the python ABI tag and the pinned jars."""
    doc = {"abi": abi, "packages": sorted([p["name"], p["version"], sorted(p["hashes"]), p.get("marker") or ""] for p in packages), "jars": sorted(jars or [])}
    return "sha256:" + hashlib.sha256(canonical_json(doc).encode()).hexdigest()


def pip_index_args(pip: dict[str, Any]) -> list[str]:
    args: list[str] = []
    if pip.get("no_index"):
        args.append("--no-index")
    elif pip.get("index_url"):
        args += ["--index-url", str(pip["index_url"])]
    for u in pip.get("extra_index_urls") or []:
        args += ["--extra-index-url", str(u)]
    for f in pip.get("find_links") or []:
        args += ["--find-links", str(f)]
    return args


def _installed(target: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for d in target.glob("*.dist-info"):
        stem = d.name[: -len(".dist-info")]
        if "-" in stem:
            name, ver = stem.rsplit("-", 1)
            out[name.lower().replace("_", "-")] = ver
    return out


def ensure_layer(layer: dict[str, Any], cache_dir: str | Path, *, python: str | None = None, timeout: float = 1800.0, env: dict[str, str] | None = None) -> Path:
    """Install ``layer`` (an ``EnvironmentSpec.layer`` block) below ``cache_dir/<key>`` unless it is already there; return the directory.

    The directory is built in a sibling temp dir and renamed into place, so a crash never leaves a half-installed layer that looks complete.
    Raises :class:`LayerError` when pip fails or the installed distributions differ from the lock.
    """
    key = str(layer["key"])
    safe = key.replace(":", "-")
    root = Path(cache_dir)
    root.mkdir(parents=True, exist_ok=True)
    final = root / safe
    if (final / MARKER).is_file():
        return final
    pkgs = layer.get("packages") or []
    if not pkgs:
        final.mkdir(exist_ok=True)
        (final / MARKER).write_text(json.dumps({"key": key, "packages": []}))
        return final
    lock_path = root / f".{safe}.lock"
    # one build per key and node: the cross-platform lock (flock on POSIX, msvcrt byte lock on Windows) is released by the OS if we crash
    lock = FileLock(lock_path)
    try:
        lock.acquire(timeout)
    except LockTimeout:
        raise LayerError(f"timed out waiting for another process that is building layer {key}") from None
    try:
        if (final / MARKER).is_file():
            return final
        stage = Path(tempfile.mkdtemp(prefix=f".{safe}.", dir=root))
        try:
            req = stage / "requirements.lock.txt"
            req.write_text(layer.get("requirements") or requirements_text(pkgs))
            target = stage / "site"
            pip = layer.get("pip") or {}
            cmd = [python or sys.executable, "-m", "pip", "install", "--require-hashes", "--no-deps", "--disable-pip-version-check", "--no-input",
                   "--no-compile", "--target", str(target), "-r", str(req)]
            if not pip.get("allow_sdist"):
                cmd.append("--only-binary=:all:")
            cmd += pip_index_args(pip)
            penv = {k: v for k, v in os.environ.items() if not k.startswith("PIP_") and k not in ("PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV")}
            penv.update({"PIP_CONFIG_FILE": os.devnull, "PIP_NO_INPUT": "1"})
            penv.update(env or {})
            try:
                res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, check=False, env=penv)  # noqa: S603
            except subprocess.TimeoutExpired as exc:
                raise LayerError(f"pip timed out after {timeout:.0f}s while building layer {key}") from exc
            if res.returncode != 0:
                tail = (res.stderr or res.stdout)[-1500:]
                hint = " (an artifact does not match the hash pinned in the environment lock - do not override)" if "HASHES" in tail or "DO NOT MATCH" in tail else ""
                if "No module named pip" in tail:
                    hint = (f" (pip is not installed for {python or sys.executable}: run `python -m ensurepip --upgrade` or `uv pip install pip` in that environment, "
                            "or create it with `uv venv --seed`)")
                raise LayerError(f"pip failed while building layer {key}{hint}: {tail}")
            have = _installed(target)
            for p in pkgs:
                got = have.get(str(p["name"]).lower().replace("_", "-"))
                if got is None or got != str(p["version"]).replace("-", "_") and got != str(p["version"]):
                    raise LayerError(f"layer {key}: {p['name']}=={p['version']} was not installed (found {got})")
            (target / MARKER).write_text(json.dumps({"key": key, "packages": [f"{p['name']}=={p['version']}" for p in pkgs], "built_at": time.time()}))
            if final.exists():
                shutil.rmtree(final, ignore_errors=True)
            os.replace(target, final)
        finally:
            shutil.rmtree(stage, ignore_errors=True)
    finally:
        lock.release()
    return final


def pythonpath_for(layer_dir: str | Path, existing: str | None = None) -> str:
    """``PYTHONPATH`` value that puts the layer in front of whatever the runtime already exports."""
    return os.pathsep.join(p for p in (str(layer_dir), existing) if p)
