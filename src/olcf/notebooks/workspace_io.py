# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Workspace export / import serializers of the ITEMS item types, and the shared *file tree* used by Git sync and deployment promotion.

Layout (identical to the foundation's ``WorkspaceService.export_zip``)::

    items/<folder>/<name>.<type>/item.json                  logicalId, type, name, description, folder, tags, sensitivity, version, definition
    items/<folder>/<name>.notebook/notebook-content.ipynb   full notebook (or notebook-content.py in Fabric / percent format for Git)
    items/<folder>/<name>.notebook/.platform                Fabric Git style metadata (displayName, logicalId)
    items/<folder>/<name>.environment/locks/v1.lock.json    one immutable lock per publication
    items/<folder>/<name>.spark_job_definition/main.py      inline code of a Python job (definition keeps "code_file")
    items/<folder>/<name>.variable_set/variables.json
    connections/<name>.json                                 metadata only - secrets never

The foundation calls ``export(row) -> {relative file: bytes}``, ``export_definition(definition)`` (what goes into ``item.json``) and
``import(meta, files) -> definition`` for item types that registered them; :func:`register_hooks` does that for ``notebook``, ``environment``,
``spark_job_definition`` and ``variable_set`` so that a workspace round-trips with *identical definitions* (tested over all item types).

:func:`build_tree` produces the same tree from the database for Git sync / promotion, with Git-specific choices (``mode="git"``): notebooks without
outputs, in the configured source format, and ``item.json`` without the volatile ``version`` field so that unchanged items hash identically.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
import uuid
from typing import Any

from sqlalchemy import select

from olcf.api.core.workspaces import _dump, _safe_name
from olcf.core import refs
from olcf.core.context import AppContext
from olcf.core.errors import ValidationFailed
from olcf.db import schema as S
from olcf.db.util import fetch_all
from olcf.notebooks import envspec, formats
from olcf.notebooks import sparkjobs as sj
from olcf.notebooks import variables as var
from olcf.notebooks.service import normalise_definition as nb_normalise

PLATFORM_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json"
NOTEBOOK_FORMATS = ("ipynb", "py", "fabric")
_LOGICAL_NS = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")


def logical_uuid(item_id: str) -> str:
    """A stable UUID for the ``.platform`` ``logicalId`` (derived from the item id; the same item always gets the same one)."""
    return str(uuid.uuid5(_LOGICAL_NS, item_id))


def platform_file(item_type: str, name: str, description: str, item_id: str) -> bytes:
    kind = {"notebook": "Notebook", "environment": "Environment", "spark_job_definition": "SparkJobDefinition", "variable_set": "VariableLibrary"}.get(item_type, item_type)
    return _dump({"$schema": PLATFORM_SCHEMA, "metadata": {"type": kind, "displayName": name, "description": description or ""},
                  "config": {"version": "2.0", "logicalId": logical_uuid(item_id)}})


# ------------------------------------------------------------------------------ notebook
def export_notebook_files(row: dict[str, Any], *, fmt: str = "ipynb", outputs: bool = True, ids: bool = True) -> dict[str, bytes]:
    d = nb_normalise(row["definition"])
    nb = d["nbformat"]
    if fmt == "ipynb":
        name, text = "notebook-content.ipynb", formats.to_ipynb_text(nb, strip=not outputs)
    elif fmt == "py":
        name, text = "notebook-content.py", formats.to_py_percent(nb, ids=ids, metadata_header=True)
    elif fmt == "fabric":
        name, text = "notebook-content.py", formats.to_fabric_git(nb, ids=ids)
    else:
        raise ValidationFailed(f"unknown notebook format '{fmt}'", details={"known": list(NOTEBOOK_FORMATS)})
    return {name: text.encode(), ".platform": platform_file("notebook", row["name"], row.get("description", ""), row["id"])}


def _notebook_export_definition(defn: dict[str, Any]) -> dict[str, Any]:
    d = nb_normalise(defn)
    return {"settings": d["settings"]} if d.get("settings") else {}


def _notebook_import(meta: dict[str, Any], files: dict[str, bytes]) -> dict[str, Any]:
    settings = (meta.get("definition") or {}).get("settings")
    raw = files.get("notebook-content.ipynb")
    fmt = "ipynb"
    if raw is None:
        raw = files.get("notebook-content.py")
        fmt = "py" if raw is None else (formats.sniff_format(raw, "notebook-content.py"))
    if raw is None:
        if isinstance((meta.get("definition") or {}).get("nbformat"), dict):
            return nb_normalise(meta["definition"])
        raise ValidationFailed(f"notebook '{meta.get('name')}' has no notebook-content.ipynb / notebook-content.py")
    nb = formats.normalize_notebook(formats.parse_notebook(raw, fmt))
    return nb_normalise({"nbformat": nb, **({"settings": settings} if settings else {})})


# ------------------------------------------------------------------------------ spark job definition
def _sjd_export(row: dict[str, Any]) -> dict[str, bytes]:
    d = sj.normalise_definition(row["definition"])
    files = {".platform": platform_file("spark_job_definition", row["name"], row.get("description", ""), row["id"])}
    if "code" in d:
        files["main.py"] = d["code"].encode()
    return files


def _sjd_export_definition(defn: dict[str, Any]) -> dict[str, Any]:
    d = sj.normalise_definition(defn)
    if "code" in d:
        d = {k: v for k, v in d.items() if k != "code"}
        d["code_file"] = "main.py"
    return d


def _sjd_import(meta: dict[str, Any], files: dict[str, bytes]) -> dict[str, Any]:
    d = dict(meta.get("definition") or {})
    cf = d.pop("code_file", None)
    if cf:
        if cf != "main.py" or "main.py" not in files:
            raise ValidationFailed(f"spark job definition '{meta.get('name')}' refers to a missing code file")
        d["code"] = files["main.py"].decode("utf-8")
    return sj.normalise_definition(d)


# ------------------------------------------------------------------------------ variable set
def _vs_export(row: dict[str, Any]) -> dict[str, bytes]:
    return {"variables.json": _dump(var.normalise_definition(row["definition"])), ".platform": platform_file("variable_set", row["name"], row.get("description", ""), row["id"])}


def _vs_import(meta: dict[str, Any], files: dict[str, bytes]) -> dict[str, Any]:
    raw = files.get("variables.json")
    if raw is None:
        raise ValidationFailed(f"variable set '{meta.get('name')}' has no variables.json")
    return var.normalise_definition(json.loads(raw))


_SETTINGS_REF_PATHS = ["settings.default_lakehouse.lakehouse_id", "settings.default_lakehouse.id", "settings.environment_id", "settings.variable_set",
                       "settings.runtime.environment_id"]


def remap_settings_refs(defn: dict[str, Any], id_map: dict[str, str], name_map: dict[str, str] | None = None) -> dict[str, Any]:  # noqa: ARG001
    """Import / promotion: the ``settings`` block's lakehouse / environment / variable-set ids follow the imported items (code and outputs are never touched).

    When the default lakehouse was re-pointed, its ``workspace_id`` (the *source* workspace) is dropped so the reference resolves in the target workspace.
    """
    out = refs.remap_paths(defn, id_map, _SETTINGS_REF_PATHS)
    old = (defn.get("settings") or {}).get("default_lakehouse") if isinstance(defn.get("settings"), dict) else None
    new = (out.get("settings") or {}).get("default_lakehouse") if isinstance(out.get("settings"), dict) else None
    if isinstance(old, dict) and isinstance(new, dict) and old != new and "workspace_id" in new:
        new.pop("workspace_id", None)
    return out


def _no_refs(defn: dict[str, Any], id_map: dict[str, str], name_map: dict[str, str] | None = None) -> dict[str, Any]:  # noqa: ARG001
    """Environment definitions reference no items (package names must never be matched against ids)."""
    return copy.deepcopy(defn)


def register_hooks(ctx: AppContext) -> None:
    """Register validate + export / import hooks for the four ITEMS item types (called by the module's ``install``)."""
    envs = ctx.service("environments")
    ctx.register_item_hooks("notebook", validate=nb_normalise, export=lambda row: export_notebook_files(row), export_definition=_notebook_export_definition,
                            remap_ids=remap_settings_refs, **{"import": _notebook_import})
    ctx.register_item_hooks("environment", validate=envs.validate, export=envs.export_files, export_definition=lambda d: envspec.normalise_definition(d),
                            remap_ids=_no_refs, **{"import": envs.import_definition})
    ctx.register_item_hooks("spark_job_definition", validate=sj.normalise_definition, export=_sjd_export, export_definition=_sjd_export_definition,
                            remap_ids=remap_settings_refs, **{"import": _sjd_import})
    ctx.register_item_hooks("variable_set", validate=var.normalise_definition, export=_vs_export, export_definition=lambda d: {}, **{"import": _vs_import})


# ------------------------------------------------------------------------------ file tree (Git sync / promotion)
def item_base(row: dict[str, Any]) -> str:
    folder = "/".join(_safe_name(p) for p in row["folder"].split("/") if p)
    return f"items/{folder + '/' if folder else ''}{_safe_name(row['name'])}.{row['type']}"


def item_files(ctx: AppContext, row: dict[str, Any], *, mode: str = "export", notebook_format: str = "ipynb") -> dict[str, bytes]:
    """All files of one item, keyed by repo-relative path (``items/.../item.json`` included)."""
    base = item_base(row)
    git = mode == "git"
    meta: dict[str, Any] = {"logicalId": row["id"], "type": row["type"], "name": row["name"], "description": row["description"], "folder": row["folder"], "tags": row["tags"],
                            "sensitivity": row["sensitivity"]}
    if not git:
        meta["version"] = row["version"]
    files: dict[str, bytes] = {}
    hooks = ctx.item_hooks.get(row["type"], {})
    if row["type"] == "notebook" and git:
        for fname, data in export_notebook_files(row, fmt=notebook_format, outputs=False, ids=notebook_format != "fabric").items():
            files[f"{base}/{fname}"] = data
        meta["definition"] = _notebook_export_definition(row["definition"])
    elif "export" in hooks:
        for fname, data in hooks["export"](row).items():
            files[f"{base}/{fname}"] = data
        meta["definition"] = hooks["export_definition"](row["definition"]) if "export_definition" in hooks else {}
    else:
        meta["definition"] = row["definition"]
    files[f"{base}/item.json"] = _dump(meta)
    return files


def connection_file(c: dict[str, Any]) -> tuple[str, bytes]:
    """Connection metadata (never the secret: only ``requires_secret``)."""
    return f"connections/{_safe_name(c['name'])}.json", _dump({"logicalId": c["id"], "name": c["name"], "type": c["type"], "description": c["description"], "config": c["config"],
                                                                  "shared": c["shared"], "requires_secret": bool(c["secret_id"]), "tags": c["tags"]})


def build_tree(ctx: AppContext, workspace_id: str, *, mode: str = "git", notebook_format: str = "ipynb", types: list[str] | None = None,
               names: list[str] | None = None, connections: bool = True) -> tuple[dict[str, bytes], dict[str, dict[str, Any]]]:
    """``(files, units)``: the repo tree of the workspace and, per *unit* (an item directory or a connection file), ``{key, kind, type, name, item_id}``."""
    with ctx.db.connect() as conn:
        rows = fetch_all(conn, select(S.items).where(S.items.c.workspace_id == workspace_id, S.items.c.deleted_at.is_(None)).order_by(S.items.c.id))
        conns = fetch_all(conn, select(S.connections).where(S.connections.c.workspace_id == workspace_id, S.connections.c.deleted_at.is_(None))) if connections else []
    files: dict[str, bytes] = {}
    units: dict[str, dict[str, Any]] = {}
    for r in rows:
        if types and r["type"] not in types:
            continue
        if names and r["name"] not in names:
            continue
        for p, data in item_files(ctx, r, mode=mode, notebook_format=notebook_format).items():
            files[p] = data
        units[item_base(r)] = {"key": item_base(r), "kind": "item", "type": r["type"], "name": r["name"], "item_id": r["id"]}
    for c in conns:
        p, data = connection_file(c)
        files[p] = data
        units[p] = {"key": p, "kind": "connection", "type": "connection", "name": c["name"], "item_id": c["id"]}
    return files, units


def unit_of(path: str) -> str | None:
    """The unit key of a repo path: the item directory (``items/<folder>/<name>.<type>``) or the connection file."""
    if path.startswith("connections/") and path.endswith(".json") and path.count("/") == 1:
        return path
    if path.startswith("items/"):
        parts = path.split("/")
        for i, seg in enumerate(parts[1:-1], start=1):
            if re.search(r"\.(notebook|environment|spark_job_definition|variable_set|lakehouse|sql_query|pipeline|schedule|report|extension)$", seg):
                return "/".join(parts[: i + 1])
        if parts[-1] == "item.json":
            return "/".join(parts[:-1])
    return None


def group_units(files: dict[str, bytes]) -> dict[str, dict[str, bytes]]:
    """Group a flat tree into units (relative file name -> bytes); files that belong to no unit are ignored."""
    out: dict[str, dict[str, bytes]] = {}
    for p, data in files.items():
        u = unit_of(p)
        if u is None:
            continue
        out.setdefault(u, {})[p[len(u) + 1:] if p != u else ""] = data
    return out


def canonical_unit(ctx: AppContext, key: str, files: dict[str, bytes], notebook_format: str) -> dict[str, bytes]:
    """The comparable form of a unit: identity that differs between workspaces (``logicalId``, ``.platform``) is dropped and notebooks are parsed and
    re-serialised in the configured format, so that formatting-only edits and a different workspace do not look like changes."""
    out = {n: d for n, d in files.items() if n != ".platform"}
    for fname in ("item.json", ""):  # "" is the connection file itself
        if fname in out:
            try:
                meta = json.loads(out[fname])
                meta.pop("logicalId", None)
                meta.pop("version", None)
                out[fname] = _dump(meta)
            except ValueError:
                pass
    if key.endswith(".notebook"):
        raw_name = next((n for n in ("notebook-content.ipynb", "notebook-content.py") if n in out), None)
        if raw_name is not None:
            try:
                fmt = "ipynb" if raw_name.endswith(".ipynb") else formats.sniff_format(out[raw_name], raw_name)
                nb = formats.normalize_notebook(formats.parse_notebook(out[raw_name], fmt), strip_outputs=True)
            except Exception:  # noqa: BLE001 - an unparsable notebook keeps its raw bytes (and is reported when it is applied)
                return out
            del out[raw_name]
            regen = export_notebook_files({"definition": {"nbformat": nb}, "name": "x", "description": "", "id": "x"}, fmt=notebook_format, outputs=False, ids=notebook_format != "fabric")
            regen.pop(".platform", None)
            out.update(regen)
    return out


def unit_hash(files: dict[str, bytes]) -> str:
    h = hashlib.sha256()
    for name in sorted(files):
        h.update(name.encode() + b"\x00" + hashlib.sha256(files[name]).digest())
    return "sha256:" + h.hexdigest()
