# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Rewriting item/connection ids inside item definitions (workspace import, Git pull, promotion).

Item definitions of other packages reference each other by id (a report tile's ``lakehouse_id``, a notebook's default lakehouse, a pipeline activity's
notebook, a schedule's target, a lakehouse's connection ...).  When a workspace is imported into another workspace the items get new ids, so every such
reference has to be rewritten or the imported report would still read the *source* workspace's lakehouse.

Each owning package knows where its references live and registers a pure hook::

    ctx.register_item_hooks("report", remap_ids=lambda definition, id_map, name_map=None: ...)

``remap_ids(definition, id_map, name_map=None) -> definition`` must return a new definition (never mutate its argument).  ``id_map`` maps old ids to new ids
(items and connections), ``name_map`` optionally maps connection names.  Types without a hook fall back to :func:`remap_exact` (every string that *equals* a
known id is replaced; ids are unique so exact matches are safe; keys and substrings are never rewritten).

The helpers below are the building blocks for those hooks.  Standard library only.
"""

from __future__ import annotations

import copy
from collections.abc import Callable, Iterable
from typing import Any

MAX_DEPTH = 60

RemapHook = Callable[..., dict[str, Any]]


def remap_exact(obj: Any, id_map: dict[str, str], name_map: dict[str, str] | None = None, *, conn_keys: Iterable[str] = (), _key: str = "", _depth: int = 0) -> Any:
    """Copy of ``obj`` with every string equal to a key of ``id_map`` replaced; strings under ``conn_keys`` are also looked up in ``name_map``."""
    if _depth > MAX_DEPTH:
        return obj
    if isinstance(obj, str):
        if obj in id_map:
            return id_map[obj]
        if name_map and _key in conn_keys and obj in name_map:
            return name_map[obj]
        return obj
    if isinstance(obj, list):
        return [remap_exact(x, id_map, name_map, conn_keys=conn_keys, _key=_key, _depth=_depth + 1) for x in obj]
    if isinstance(obj, dict):
        return {k: remap_exact(v, id_map, name_map, conn_keys=conn_keys, _key=str(k), _depth=_depth + 1) for k, v in obj.items()}
    return obj


def remap_keys(obj: Any, id_map: dict[str, str], keys: Iterable[str], name_map: dict[str, str] | None = None, *, _depth: int = 0) -> Any:
    """Copy of ``obj`` where only values stored under one of ``keys`` (at any depth) are remapped (:func:`remap_exact` applies below such a key)."""
    wanted = frozenset(keys)
    if _depth > MAX_DEPTH:
        return obj
    if isinstance(obj, list):
        return [remap_keys(x, id_map, wanted, name_map, _depth=_depth + 1) for x in obj]
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for k, v in obj.items():
            if k in wanted:
                out[k] = remap_exact(v, id_map, name_map, conn_keys=wanted, _key=str(k))
            else:
                out[k] = remap_keys(v, id_map, wanted, name_map, _depth=_depth + 1)
        return out
    return obj


def remap_paths(definition: dict[str, Any], id_map: dict[str, str], paths: Iterable[str], name_map: dict[str, str] | None = None) -> dict[str, Any]:
    """Copy of ``definition`` with the values at ``paths`` remapped.  A path is dotted keys; a segment ending in ``[]`` iterates a list
    (``"tiles[].lakehouse_id"``, ``"settings.default_lakehouse.lakehouse_id"``).  Missing paths are ignored."""
    out = copy.deepcopy(definition)
    for path in paths:
        _apply_path(out, path.split("."), id_map, name_map)
    return out


def _apply_path(node: Any, segs: list[str], id_map: dict[str, str], name_map: dict[str, str] | None) -> None:
    if not segs or not isinstance(node, dict):
        return
    seg, rest = segs[0], segs[1:]
    many = seg.endswith("[]")
    key = seg[:-2] if many else seg
    if key not in node:
        return
    if many:
        items = node[key]
        if not isinstance(items, list):
            return
        for i, el in enumerate(items):
            if rest:
                _apply_path(el, rest, id_map, name_map)
            else:
                items[i] = remap_exact(el, id_map, name_map, conn_keys=(key,), _key=key)
    elif rest:
        _apply_path(node[key], rest, id_map, name_map)
    else:
        node[key] = remap_exact(node[key], id_map, name_map, conn_keys=(key,), _key=key)


def remap_definition(item_hooks: dict[str, dict[str, Any]], item_type: str, definition: dict[str, Any], id_map: dict[str, str],
                     name_map: dict[str, str] | None = None) -> dict[str, Any]:
    """Apply the type's registered ``remap_ids`` hook, or :func:`remap_exact` when the type has none."""
    hook = (item_hooks.get(item_type) or {}).get("remap_ids")
    if hook is not None:
        return hook(definition, id_map, name_map)
    return remap_exact(definition, id_map, name_map)
