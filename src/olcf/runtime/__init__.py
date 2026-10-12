# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Runtime manifests and the node-side runtime manager (CONTRACTS section 4.5).

A *runtime* is a pinned, verified, side-by-side installation of Spark + Delta + Python packages + jars under
``<state>/runtimes/<id>/``. Everything is declared by a signed manifest (:mod:`olcf.runtime.manifest`, :mod:`olcf.runtime.signing`),
resolved with hash pinning (pip ``--require-hashes``, Maven checksums recorded in ``lock.json``) and managed by
:class:`olcf.runtime.manager.RuntimeManager`. Command line: ``python -m olcf.runtime --help``.

The submodules are imported lazily so that importing :mod:`olcf.runtime` (which the worker, the controller's catalog and the runtime
venvs all do) stays cheap and never pulls in network libraries.
"""

from __future__ import annotations

from typing import Any

__all__ = ["ManagerConfig", "RuntimeHandle", "RuntimeManager", "RuntimeManifest", "TrustStore"]

_LAZY = {
    "ManagerConfig": "olcf.runtime.manager", "RuntimeHandle": "olcf.runtime.manager", "RuntimeManager": "olcf.runtime.manager",
    "RuntimeManifest": "olcf.runtime.manifest", "TrustStore": "olcf.runtime.signing",
}


def __getattr__(name: str) -> Any:
    mod = _LAZY.get(name)
    if mod is None:
        raise AttributeError(f"module 'olcf.runtime' has no attribute {name!r}")
    import importlib

    return getattr(importlib.import_module(mod), name)
