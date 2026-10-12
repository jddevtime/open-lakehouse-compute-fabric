# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Resolution of lakehouse-relative paths (``Files/...``, ``Tables/...``) - the ``paths.resolve`` hook.

Microsoft Fabric notebooks address data with relative paths against the *default lakehouse*. This module reproduces
that: ``resolve("Files/raw/a.csv")`` returns the physical URI below the default lakehouse's ``files_uri``.

Hook contract (documented for the FABRIC package): :func:`resolve` first asks ``olcf.runtime_lib.notebookutils`` -
if that module is importable and defines ``resolve_path(path, *, for_, spec)`` returning a string (or ``None`` to
decline) its answer wins; otherwise the built-in rules below apply.

Rules (built-in):

* ``Files/x`` / ``Tables/x`` (optionally ``./`` prefixed) -> ``<files_uri|tables_uri>/x`` of the default lakehouse.
* ``/lakehouse/default/Files/x`` (Fabric's local mount spelling) -> the same; with ``for_="local"`` and a ``file://``
  lakehouse the plain filesystem path is returned. ``/lakehouse/<name>/...`` selects another lakehouse by name.
* ``olcf://<workspace>/<lakehouse>/Files/x`` -> that lakehouse (must be in ``lakehouses``).
* anything with a URI scheme (``abfss://``, ``s3a://``, ``file://``, ``hdfs://``) and absolute paths are returned unchanged.
* ``..`` segments that would escape the lakehouse area raise :class:`ValueError` (fail closed).
"""

from __future__ import annotations

import importlib
import posixpath
import re
import urllib.parse
import urllib.request
from typing import Any

_STATE: dict[str, Any] = {"spec": {}, "default": None, "lakehouses": []}
_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*://")


def configure(spec: dict[str, Any]) -> None:
    """Called once by the runner/kernel host with the job spec."""
    _STATE["spec"] = spec or {}
    _STATE["default"] = (spec or {}).get("default_lakehouse")
    _STATE["lakehouses"] = list((spec or {}).get("lakehouses") or [])


def default_lakehouse() -> dict[str, Any] | None:
    return _STATE["default"]


def set_default_lakehouse(name_or_id: str) -> dict[str, Any]:
    """Switch the default lakehouse (``%%configure`` ``defaultLakehouse``); must be one the job may use."""
    lh = lookup_lakehouse(name_or_id)
    if lh is None:
        raise ValueError(f"lakehouse {name_or_id!r} is not available to this job")
    _STATE["default"] = lh
    return lh


def lookup_lakehouse(ref: str | None) -> dict[str, Any] | None:
    if not ref:
        return _STATE["default"]
    for lh in [_STATE["default"], *_STATE["lakehouses"]]:
        if lh and ref in (lh.get("name"), lh.get("id")):
            return lh
    return None


def _join(base: str, rel: str) -> str:
    norm = posixpath.normpath(rel) if rel else ""
    if norm.startswith("..") or norm == "..":
        raise ValueError(f"path {rel!r} escapes the lakehouse area")
    norm = "" if norm == "." else norm
    return base.rstrip("/") + ("/" + norm if norm else "")


def _to_local(uri: str) -> str:
    if uri.startswith("file:"):
        return urllib.request.url2pathname(urllib.parse.urlparse(uri).path)
    return uri


def _hook() -> Any:
    # ``importlib.import_module`` honours ``sys.modules`` (tests and embedders may substitute the module); a plain
    # ``import a.b.c as x`` would return the attribute already bound on the parent package instead.
    try:
        nbu = importlib.import_module("olcf.runtime_lib.notebookutils")
    except ImportError:
        return None
    return getattr(nbu, "resolve_path", None)


def resolve(path: str, *, for_: str = "spark") -> str:
    """Resolve ``path`` against the default lakehouse. ``for_`` is ``"spark"`` (URI) or ``"local"`` (python I/O)."""
    hook = _hook()
    if hook is not None:
        out = hook(path, for_=for_, spec=_STATE["spec"])
        if out is not None:
            return str(out)
    return _builtin_resolve(path, for_)


def _builtin_resolve(path: str, for_: str) -> str:
    if _SCHEME.match(path):
        if path.startswith("olcf://"):
            u = urllib.parse.urlparse(path)
            parts = [p for p in u.path.split("/") if p]
            lh = lookup_lakehouse(parts[0]) if parts else None
            if lh is None or len(parts) < 2 or parts[1] not in ("Files", "Tables"):
                raise ValueError(f"cannot resolve {path!r}: unknown lakehouse or area (expected olcf://<workspace>/<lakehouse>/Files|Tables/...)")
            return _finish(lh, parts[1], "/".join(parts[2:]), for_)
        return _to_local(path) if for_ == "local" and path.startswith("file:") else path
    p = path
    lh = _STATE["default"]
    m = re.match(r"^/lakehouse/([^/]+)/(Files|Tables)(?:/(.*))?$", p)
    if m:
        lh = _STATE["default"] if m.group(1) == "default" else lookup_lakehouse(m.group(1))
        if lh is None:
            raise ValueError(f"lakehouse {m.group(1)!r} is not available to this job")
        return _finish(lh, m.group(2), m.group(3) or "", for_)
    if p.startswith("./"):
        p = p[2:]
    m = re.match(r"^(Files|Tables)(?:/(.*))?$", p)
    if m:
        if lh is None:
            raise ValueError(f"cannot resolve relative path {path!r}: this job has no default lakehouse")
        return _finish(lh, m.group(1), m.group(2) or "", for_)
    return path


def _finish(lh: dict[str, Any], area: str, rel: str, for_: str) -> str:
    base = lh["files_uri"] if area == "Files" else lh["tables_uri"]
    uri = _join(base, rel)
    return _to_local(uri) if for_ == "local" else uri
