# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``olcf-host-python``: the pseudo-runtime that runs Python-engine work on the worker's *own* interpreter.

Why this exists: a Spark runtime is a 500 MB download (PySpark, JARs from Maven Central, a JDK) that can fail on a first run (offline machine, proxy, no
developer tools on macOS).  A notebook that uses ``deltalake``, DuckDB and pandas needs none of that, and the worker agent already runs in an environment
that has them (they are dependencies of OLCF itself).  So every worker offers one extra runtime, built in and always ``ready`` while the interpreter is
healthy:

* id :data:`HOST_RUNTIME_ID`, family :data:`HOST_FAMILY`, **no Spark** (``summary()["no_spark"]``) - it is never chosen for ``engine=spark`` work;
* a *fingerprint* (Python version, implementation, platform, versions of the key libraries) that is recorded in the job record and ``result.json`` exactly
  like the fingerprint of a Spark runtime, so a result can be tied to the environment that produced it;
* a :class:`HostPythonHandle` with the same surface the agent uses for installed runtimes (``python``, ``root``, ``env()``, ``fingerprint`` ...), so job
  launching, kernels, dependency layers and the sandbox wrapper need no special cases beyond :meth:`HostPythonHandle.module_argv`.

Import rules: standard library only (plus ``olcf.version`` / ``olcf.runtime.errors``), like everything the agent loads on every start.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import platform
import site
import sys
import sysconfig
import threading
import time
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path
from typing import Any

from olcf.core.hostruntime import HOST_FAMILY, HOST_RUNTIME_ID, HOST_SCHEMA, NO_SPARK_REMEDIATION
from olcf.runtime.errors import RuntimeNotFound
from olcf.runtime.manager import RuntimeHandle

#: Distribution names whose versions make up the fingerprint (absent ones are simply not part of it: matplotlib is optional).
KEY_PACKAGES = ("duckdb", "deltalake", "pyarrow", "pandas", "numpy", "polars", "matplotlib", "nbformat", "jupyter_client", "ipykernel", "psutil")
#: Modules the batch runner and the kernel host import; without them a job cannot even start, so the runtime is reported ``failed`` with the missing name.
REQUIRED_MODULES = ("ipykernel", "jupyter_client", "nbformat")

_CACHE_TTL = 60.0
_cache_lock = threading.Lock()
_cache: tuple[float, dict[str, Any]] | None = None

#: Source of the bootstrap used when the ``olcf`` package is not installed into the interpreter's own site-packages (a source checkout run with
#: ``PYTHONPATH=src``).  ``python -I`` ignores ``PYTHONPATH``, so the import root is passed as an argument instead; it is also exported for child processes
#: (notebook kernels) which are started without ``-I``.
_BOOT = ("import os,runpy,sys;r=sys.argv[1];m=sys.argv[2];sys.path.append(r);"
         "os.environ['PYTHONPATH']=os.pathsep.join(p for p in (os.environ.get('PYTHONPATH',''),r) if p);"
         "sys.argv=[m]+sys.argv[3:];runpy.run_module(m,run_name='__main__',alter_sys=True)")


def _version(dist: str) -> str | None:
    try:
        return metadata.version(dist)
    except metadata.PackageNotFoundError:
        return None
    except Exception:  # noqa: BLE001 - a broken dist-info must not take the agent down
        return None


def describe(*, refresh: bool = False) -> dict[str, Any]:
    """What this interpreter is: ``{python, implementation, platform, packages, missing, fingerprint}`` (cached for a minute)."""
    global _cache
    with _cache_lock:
        if not refresh and _cache is not None and time.monotonic() - _cache[0] < _CACHE_TTL:
            return dict(_cache[1])
    packages = {name: v for name in KEY_PACKAGES if (v := _version(name))}
    core = {"schema": HOST_SCHEMA, "python": platform.python_version(), "implementation": platform.python_implementation(),
            "platform": f"{platform.system().lower()}-{platform.machine().lower()}", "packages": packages}
    fp = "sha256:" + hashlib.sha256(json.dumps(core, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    missing = [m for m in REQUIRED_MODULES if importlib.util.find_spec(m) is None]
    doc = {**core, "fingerprint": fp, "missing": missing, "executable": sys.executable}
    with _cache_lock:
        _cache = (time.monotonic(), doc)
    return dict(doc)


def detail_text(info: dict[str, Any]) -> str:
    """One line for humans: ``Python 3.13.5 (CPython) on linux-x86_64; duckdb 1.5.6, deltalake 1.6.6 ...; no Spark``."""
    libs = ", ".join(f"{k} {v}" for k, v in info["packages"].items() if k in ("duckdb", "deltalake", "pyarrow", "pandas", "matplotlib"))
    return f"Python {info['python']} ({info['implementation']}) on {info['platform']}" + (f"; {libs}" if libs else "") + "; no Spark"


def summary(*, active: bool = False) -> dict[str, Any]:
    """The heartbeat / ``hello`` entry of this runtime (``RuntimeStatus`` plus the extra keys the catalog and the UI show)."""
    info = describe()
    if info["missing"]:
        return {"id": HOST_RUNTIME_ID, "state": "failed", "verified": False, "family": HOST_FAMILY, "version": info["python"], "fingerprint": info["fingerprint"],
                "spark": None, "python": info["python"], "no_spark": True, "engines": ["python"], "active": False,
                "detail": f"the worker's Python lacks {', '.join(info['missing'])} (needed to run notebooks): pip install {' '.join(info['missing'])}"}
    return {"id": HOST_RUNTIME_ID, "state": "ready", "verified": True, "family": HOST_FAMILY, "version": info["python"], "fingerprint": info["fingerprint"],
            "spark": None, "python": info["python"], "no_spark": True, "engines": ["python"], "active": active, "detail": detail_text(info),
            "packages": info["packages"]}


def _site_dirs() -> set[Path]:
    dirs: set[str] = set()
    for fn in (getattr(site, "getsitepackages", None),):
        if fn:
            try:
                dirs.update(fn())
            except Exception:  # noqa: BLE001, S110
                pass
    try:
        dirs.add(site.getusersitepackages())
    except Exception:  # noqa: BLE001, S110
        pass
    for key in ("purelib", "platlib"):
        p = sysconfig.get_paths().get(key)
        if p:
            dirs.add(p)
    out: set[Path] = set()
    for d in dirs:
        try:
            out.add(Path(d).resolve())
        except OSError:
            continue
    return out


def olcf_import_root() -> str | None:
    """Directory that must be on ``sys.path`` for ``import olcf`` in a *clean* (``-I``) interpreter, or ``None`` when ``olcf`` is installed in its site-packages."""
    import olcf

    root = Path(olcf.__file__).resolve().parent.parent
    return None if root in _site_dirs() else str(root)


@dataclass
class HostPythonHandle(RuntimeHandle):
    """:class:`~olcf.runtime.manager.RuntimeHandle` for the worker's own interpreter (no venv, no Spark, nothing installed)."""

    @property
    def python(self) -> Path:  # type: ignore[override]
        return Path(sys.executable)

    @property
    def has_spark(self) -> bool:
        return False

    @property
    def spark_home(self) -> Path:
        raise RuntimeNotFound("the host Python runtime contains no Spark", remediation=NO_SPARK_REMEDIATION)

    @property
    def fingerprint(self) -> str:  # type: ignore[override]
        return str(describe()["fingerprint"])

    @property
    def family(self) -> str:
        return HOST_FAMILY

    def java_home(self, extra_homes: list[Path] | None = None) -> Path:
        raise RuntimeNotFound("the host Python runtime has no JDK (it has no Spark)", remediation=NO_SPARK_REMEDIATION)

    def env(self, extra_java_homes: list[Path] | None = None) -> dict[str, str]:
        """Environment of processes that run on the host interpreter: its own ``bin`` first on ``PATH`` (``jupyter``, ``pip`` ...), no Java, no Spark."""
        env = {"OLCF_RUNTIME_ID": self.id, "OLCF_HOST_PYTHON": "1", "PYTHONNOUSERSITE": "1", "PYTHONDONTWRITEBYTECODE": "1",
               "PATH": os.pathsep.join(p for p in (str(Path(sys.executable).parent), os.environ.get("PATH", "")) if p)}
        root = olcf_import_root()
        if root:
            env["PYTHONPATH"] = root
        return env

    def module_argv(self, module: str, *args: str) -> list[str]:
        root = olcf_import_root()
        if root is None:
            return [str(self.python), "-I", "-m", module, *args]
        return [str(self.python), "-I", "-c", _BOOT, root, module, *args]

    def summary(self, state: str = "ready", verified: bool = True) -> dict[str, Any]:
        return summary()


def make_handle(runtimes_dir: Path) -> HostPythonHandle:
    """The handle for this process's interpreter; ``root`` is a (created) directory below the runtimes folder, used by the sandbox wrapper."""
    root = Path(runtimes_dir) / "_host-python"
    root.mkdir(parents=True, exist_ok=True)
    info = describe()
    return HostPythonHandle(HOST_RUNTIME_ID, root, {"id": HOST_RUNTIME_ID, "family": HOST_FAMILY, "version": info["python"], "schema": HOST_SCHEMA}, {})


def catalog_manifest() -> dict[str, Any]:
    """The controller-side catalog entry (static: the per-node facts - Python version, packages, fingerprint - arrive with the worker heartbeats)."""
    return {"schema": HOST_SCHEMA, "id": HOST_RUNTIME_ID, "version": "0", "family": HOST_FAMILY, "builtin": True, "no_spark": True, "engines": ["python"],
            "title": "Host Python (no Spark)",
            "description": ("Runs Python notebooks and scripts on the worker's own interpreter: deltalake, DuckDB, pandas and the other libraries OLCF itself uses. "
                            "Built into every worker, nothing to install. There is no Spark here: use a Spark runtime for PySpark, Spark SQL or Delta on Spark."),
            "approved_spark_conf": []}
