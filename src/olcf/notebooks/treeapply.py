# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Apply repository *units* (item directories and connection files, see :mod:`olcf.notebooks.workspace_io`) to a workspace.

Shared by ``git pull`` and deployment promotion.  The work is done in three phases so that a bad unit never leaves a half-applied tree behind:

1. **parse + validate** every incoming unit (``item.json`` + the type's ``import`` hook, which validates the definition) - nothing is written if any unit is invalid;
2. **apply** creations / updates through :class:`ItemService` (versions, ETags, audit and events are the foundation's), then deletions; an item that was renamed in
   the source (same ``logicalId``, new name) is updated in place and its old unit is not deleted;
3. **remap** references: ids of source items (``itm_...``) found in definitions are replaced by the ids of the matching target items, and connection ids / names by
   the mapping rules, so that a pipeline promoted to another workspace points at the promoted notebook and not at the original.

Connections are metadata only: secrets are never read from or written to a repository, a pulled / promoted connection is created *without* a secret
(``requires_secret`` tells the operator to attach one) and an existing connection keeps its secret.  Connections are never deleted by a sync.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import select, update

from olcf.api.core.workspaces import _reject_inline_secrets
from olcf.core import refs
from olcf.core.context import AppContext
from olcf.core.errors import OlcfError, ValidationFailed
from olcf.core.models import ItemCreate, ItemType, ItemUpdate
from olcf.core.timeutil import now_iso
from olcf.db import schema as S
from olcf.db.util import fetch_all, new_etag
from olcf.security.rbac import Principal

_ID = re.compile(r"^(itm|con|sec|lak)_[A-Za-z0-9]{6,40}$")
_CONN_KEYS = {"connection", "connection_id", "connection_ref", "connection_name"}
MAX_UNIT_BYTES = 64 * 1024 * 1024


@dataclass
class Parsed:
    """An incoming unit after parsing and validation: key, kind (item or connection), metadata and definition."""
    key: str
    kind: str  # item | connection
    meta: dict[str, Any]
    definition: dict[str, Any] = field(default_factory=dict)


def parse_unit(ctx: AppContext, key: str, files: dict[str, bytes]) -> Parsed:
    """Parse and validate one unit; raises :class:`ValidationFailed` naming the unit."""
    try:
        if key.startswith("connections/"):
            meta = json.loads(files[""])
            if not isinstance(meta, dict) or not isinstance(meta.get("name"), str) or not isinstance(meta.get("type"), str):
                raise ValidationFailed("a connection file needs 'name' and 'type'")
            if not isinstance(meta.get("config", {}), dict):
                raise ValidationFailed("connection 'config' must be an object")
            _reject_inline_secrets(meta.get("config", {}))
            return Parsed(key, "connection", meta)
        if "item.json" not in files:
            raise ValidationFailed("unit has no item.json")
        if sum(len(v) for v in files.values()) > MAX_UNIT_BYTES:
            raise ValidationFailed("unit is too large")
        meta = json.loads(files["item.json"])
        if not isinstance(meta, dict) or not isinstance(meta.get("name"), str) or not 1 <= len(meta["name"]) <= 128:
            raise ValidationFailed("item.json needs a 'name' of 1-128 characters")
        try:
            ItemType(meta.get("type"))
        except ValueError as exc:
            raise ValidationFailed(f"unknown item type {str(meta.get('type'))[:40]!r}") from exc
        hooks = ctx.item_hooks.get(meta["type"], {})
        rest = {n: d for n, d in files.items() if n not in ("item.json", ".platform")}
        definition = hooks["import"](meta, rest) if "import" in hooks else meta.get("definition", {})
        if not isinstance(definition, dict):
            raise ValidationFailed("definition must be an object")
        if "validate" in hooks:
            definition = hooks["validate"](definition)
        return Parsed(key, "item", meta, definition)
    except OlcfError as exc:
        raise ValidationFailed(f"{key}: {exc.message}", details={"unit": key}) from exc
    except (ValueError, KeyError, TypeError) as exc:
        raise ValidationFailed(f"{key}: cannot be parsed ({type(exc).__name__}: {str(exc)[:200]})", details={"unit": key}) from exc


def remap_ids(obj: Any, id_map: dict[str, str], name_map: dict[str, str] | None = None, *, _key: str = "", depth: int = 0) -> Any:
    """Return ``obj`` with every string that equals a key of ``id_map`` replaced (ids are unique, so exact matches are safe); strings under connection-like keys are
    additionally looked up in ``name_map``.  Keys and substrings are never rewritten."""
    return refs.remap_exact(obj, id_map, name_map, conn_keys=_CONN_KEYS, _key=_key, _depth=depth)


def collect_connection_refs(obj: Any, *, _key: str = "", depth: int = 0) -> tuple[set[str], set[str]]:
    """``(connection ids, connection names)`` referenced by a definition: ``con_...`` ids anywhere, names under connection-like keys."""
    ids: set[str] = set()
    names: set[str] = set()
    if depth > 40:
        return ids, names
    if isinstance(obj, str):
        if re.fullmatch(r"con_[A-Za-z0-9]{6,40}", obj):
            ids.add(obj)
        elif _key in _CONN_KEYS and obj:
            names.add(obj)
    elif isinstance(obj, list):
        for x in obj:
            a, b = collect_connection_refs(x, _key=_key, depth=depth + 1)
            ids |= a
            names |= b
    elif isinstance(obj, dict):
        for k, v in obj.items():
            a, b = collect_connection_refs(v, _key=str(k), depth=depth + 1)
            ids |= a
            names |= b
    return ids, names


def _remap_item_definition(item_type: str, definition: dict[str, Any], id_map: dict[str, str], name_map: dict[str, str], hooks: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    """Use the owning package's ``remap_ids`` hook (``ctx.item_hooks``) when it has one, else the conservative generic rules."""
    if hooks and "remap_ids" in (hooks.get(item_type) or {}):
        return refs.remap_definition(hooks, item_type, definition, id_map, name_map)
    if item_type == "notebook":  # never rewrite user code / outputs, only the settings block
        out = dict(definition)
        if "settings" in out:
            out["settings"] = remap_ids(out["settings"], id_map, name_map)
        return out
    return remap_ids(definition, id_map, name_map)


@dataclass
class Applied:
    """The outcome for one unit: created, updated, deleted, unchanged, skipped or failed."""
    key: str
    action: str  # created | updated | deleted | unchanged | skipped | failed
    kind: str = "item"
    type: str = ""
    name: str = ""
    item_id: str | None = None
    detail: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if v not in ("", None)}


def apply_units(ctx: AppContext, principal: Principal, workspace_id: str, incoming: dict[str, dict[str, bytes] | None], existing: dict[str, dict[str, Any]], *,
                connection_map: dict[str, str] | None = None, extra_id_map: dict[str, str] | None = None, dry_run: bool = False, comment: str = "git",
                on_unit: Callable[[Applied], None] | None = None) -> dict[str, Any]:
    """Apply ``incoming`` (unit key -> files, or ``None`` = the unit was deleted at the source) to ``workspace_id``.

    ``existing`` is the workspace's current ``units`` map (:func:`olcf.notebooks.workspace_io.build_tree`).  ``connection_map`` maps source connection ids / names
    to target names; ``extra_id_map`` adds explicit source id -> target id replacements (connections that are not part of the applied units).  Returns ``{applied: [...], id_mapping: {source id: target id}}``; with ``dry_run`` nothing is written (parsing and validation still run).
    """
    items = ctx.service("items")
    conns = ctx.service("connections")
    parsed: dict[str, Parsed] = {}
    problems: list[str] = []
    for key, files in incoming.items():
        if files is None:
            continue
        try:
            parsed[key] = parse_unit(ctx, key, files)
        except ValidationFailed as exc:
            problems.append(exc.message)
    if problems:
        raise ValidationFailed(f"{len(problems)} unit(s) are invalid; nothing was applied", details={"problems": problems[:50]})

    results: list[Applied] = []
    id_map: dict[str, str] = dict(extra_id_map or {})
    consumed: set[str] = set()
    by_id = {u["item_id"]: u for u in existing.values() if u["kind"] == "item"}

    def emit(a: Applied) -> None:
        results.append(a)
        if on_unit:
            on_unit(a)

    name_map: dict[str, str] = dict(connection_map or {})
    # ---- connections (metadata only)
    for key in sorted(parsed):
        p = parsed[key]
        if p.kind != "connection":
            continue
        meta = p.meta
        target_name = name_map.get(meta["name"], name_map.get(meta.get("logicalId", ""), meta["name"]))
        try:
            principal.require("connection.manage", workspace_id=workspace_id)
            cur = conns.find(workspace_id, target_name)
            if cur is None:
                if dry_run:
                    emit(Applied(key, "created", "connection", meta["type"], target_name, None, "dry run" + ("; needs a secret" if meta.get("requires_secret") else "")))
                    continue
                row = conns.create(principal, workspace_id=workspace_id, name=target_name, type_=meta["type"], config=meta.get("config", {}), secret_id=None,
                                   description=meta.get("description", ""), shared=False, tags=meta.get("tags", []))
                cur = row
                emit(Applied(key, "created", "connection", meta["type"], target_name, row["id"], "needs a secret" if meta.get("requires_secret") else ""))
            else:
                changed = (cur["config"] != meta.get("config", {}) or cur["description"] != meta.get("description", "") or cur["tags"] != meta.get("tags", []))
                if changed and not dry_run:
                    with ctx.db.begin() as conn:
                        conn.execute(update(S.connections).where(S.connections.c.id == cur["id"]).values(
                            config=meta.get("config", {}), description=meta.get("description", ""), tags=meta.get("tags", []), etag=new_etag(), updated_at=now_iso()))
                    ctx.audit.record(principal, "connection.updated", resource_type="connection", resource_id=cur["id"], workspace_id=workspace_id, details={"via": comment})
                emit(Applied(key, "updated" if changed else "unchanged", "connection", meta["type"], target_name, cur["id"], "dry run" if dry_run and changed else ""))
            if cur is not None:
                for ident in (meta.get("logicalId"), meta["name"]):
                    if isinstance(ident, str):
                        id_map[ident] = cur["id"]
        except OlcfError as exc:
            emit(Applied(key, "failed", "connection", meta["type"], target_name, None, exc.message[:300]))

    # ---- phase 2a: creations and updates (items first so that renamed items keep their identity)
    matches: dict[str, dict[str, Any] | None] = {}
    for key in sorted(parsed):
        p = parsed[key]
        if p.kind != "item":
            continue
        meta = p.meta
        cur = existing.get(key) if existing.get(key, {}).get("kind") == "item" else None
        if cur is None and isinstance(meta.get("logicalId"), str) and meta["logicalId"] in by_id and by_id[meta["logicalId"]]["type"] == meta["type"]:
            cur = by_id[meta["logicalId"]]
        if cur is not None and cur["item_id"] in consumed:
            cur = None
        row = items.get(cur["item_id"]) if cur is not None else items.find(workspace_id, meta["type"], meta["name"])
        if row is not None and (row["id"] in consumed or row["workspace_id"] != workspace_id):
            row = None
        matches[key] = row
        if row is not None:
            consumed.add(row["id"])
            if isinstance(meta.get("logicalId"), str):
                id_map[meta["logicalId"]] = row["id"]
    provisional = {k: v for k, v in id_map.items() if k != v and _ID.match(k)}
    created_ids: set[str] = set()
    for key in sorted(parsed):
        p = parsed[key]
        if p.kind != "item":
            continue
        meta = p.meta
        row = matches[key]
        try:
            if row is not None:
                principal.require("item.update", workspace_id=workspace_id, item_id=row["id"])
                definition = _remap_item_definition(meta["type"], p.definition, provisional, name_map, ctx.item_hooks)
                changes = ItemUpdate(name=meta["name"], description=meta.get("description", ""), folder=meta.get("folder", ""), tags=meta.get("tags", []),
                                     sensitivity=meta.get("sensitivity"), definition=definition, comment=comment)
                same = (row["definition"] == definition and row["name"] == meta["name"] and row["description"] == meta.get("description", "")
                        and row["folder"] == meta.get("folder", "").strip("/") and row["tags"] == meta.get("tags", []) and row["sensitivity"] == meta.get("sensitivity"))
                if same:
                    emit(Applied(key, "unchanged", "item", meta["type"], meta["name"], row["id"]))
                    continue
                if dry_run:
                    emit(Applied(key, "updated", "item", meta["type"], meta["name"], row["id"], "dry run"))
                    continue
                items.update(principal, row["id"], changes, None)
                emit(Applied(key, "updated", "item", meta["type"], meta["name"], row["id"]))
            else:
                principal.require("item.create", workspace_id=workspace_id)
                if dry_run:
                    emit(Applied(key, "created", "item", meta["type"], meta["name"], None, "dry run"))
                    continue
                new = items.create(principal, workspace_id, ItemCreate(type=ItemType(meta["type"]), name=meta["name"], description=meta.get("description", ""),
                                                                         folder=meta.get("folder", ""), tags=meta.get("tags", []), sensitivity=meta.get("sensitivity"),
                                                                         definition=_remap_item_definition(meta["type"], p.definition, provisional, name_map, ctx.item_hooks)))
                consumed.add(new["id"])
                created_ids.add(new["id"])
                if isinstance(meta.get("logicalId"), str):
                    id_map[meta["logicalId"]] = new["id"]
                emit(Applied(key, "created", "item", meta["type"], meta["name"], new["id"]))
        except OlcfError as exc:
            emit(Applied(key, "failed", "item", meta["type"], meta["name"], None, exc.message[:300]))

    # ---- phase 2b: deletions (never of an item that was matched above; connections are never deleted)
    for key, files in sorted(incoming.items()):
        if files is not None:
            continue
        cur = existing.get(key)
        if cur is None:
            continue
        if cur["kind"] == "connection":
            emit(Applied(key, "skipped", "connection", cur["type"], cur["name"], cur["item_id"], "connections are never deleted by a sync"))
            continue
        if cur["item_id"] in consumed:
            continue
        try:
            principal.require("item.delete", workspace_id=workspace_id, item_id=cur["item_id"])
            if not dry_run:
                items.delete(principal, cur["item_id"])
            emit(Applied(key, "deleted", "item", cur["type"], cur["name"], cur["item_id"], "dry run" if dry_run else ""))
        except OlcfError as exc:
            emit(Applied(key, "failed", "item", cur["type"], cur["name"], cur["item_id"], exc.message[:300]))

    # ---- phase 3: remap references inside the applied definitions
    if not dry_run and (id_map or name_map):
        effective = {k: v for k, v in id_map.items() if k != v and _ID.match(k)}
        for a in results:
            if a.kind != "item" or a.action not in ("created", "updated") or not a.item_id:
                continue
            row = items.get(a.item_id)
            new_def = _remap_item_definition(row["type"], row["definition"], effective, name_map, ctx.item_hooks)
            if new_def != row["definition"]:
                try:
                    items.update(principal, a.item_id, ItemUpdate(definition=new_def, comment=f"{comment}: remapped references"), None)
                except OlcfError as exc:
                    a.action, a.detail = "failed", f"remap: {exc.message[:200]}"
    return {"applied": [a.to_dict() for a in results], "id_mapping": {k: v for k, v in id_map.items() if _ID.match(k)}}


def workspace_units(ctx: AppContext, workspace_id: str) -> dict[str, dict[str, Any]]:
    """``units`` of a workspace without building any file content (cheap identity map used by pull / promotion)."""
    from olcf.notebooks.workspace_io import item_base

    with ctx.db.connect() as conn:
        rows = fetch_all(conn, select(S.items).where(S.items.c.workspace_id == workspace_id, S.items.c.deleted_at.is_(None)))
        cons = fetch_all(conn, select(S.connections).where(S.connections.c.workspace_id == workspace_id, S.connections.c.deleted_at.is_(None)))
    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        out[item_base(r)] = {"key": item_base(r), "kind": "item", "type": r["type"], "name": r["name"], "item_id": r["id"]}
    from olcf.api.core.workspaces import _safe_name

    for c in cons:
        k = f"connections/{_safe_name(c['name'])}.json"
        out[k] = {"key": k, "kind": "connection", "type": c["type"], "name": c["name"], "item_id": c["id"]}
    return out
