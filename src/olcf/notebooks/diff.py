# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Git-friendly notebook diff: per-cell unified diffs that ignore outputs and execution counts.

Cells are matched by their stable ``id`` first; cells whose ids differ (a notebook re-imported from ``.py`` gets fresh derived ids) are then
matched by identical content and finally by similarity (``difflib`` ratio >= 0.6, in order).  The result lists every cell once with a status
``unchanged | modified | added | removed``, a ``moved`` flag when the cell kept its content but changed its relative position, and a
``diff`` (unified, ``a/cell[i]`` -> ``b/cell[j]``) for modified cells.  :func:`render_text` joins them into one patch-like text.
"""

from __future__ import annotations

import difflib
import json
from typing import Any

SIMILARITY = 0.6


def _sig(cell: dict[str, Any]) -> tuple[str, str]:
    return cell["cell_type"], cell["source"]


def _meta(cell: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in (cell.get("metadata") or {}).items() if k not in ("collapsed", "scrolled", "execution", "olcf_index")}


def _match(a_cells: list[dict[str, Any]], b_cells: list[dict[str, Any]]) -> dict[int, int]:
    """a-index -> b-index pairing (see module docstring)."""
    pairs: dict[int, int] = {}
    used_b: set[int] = set()
    b_by_id = {c["id"]: j for j, c in enumerate(b_cells)}
    for i, c in enumerate(a_cells):
        j = b_by_id.get(c["id"])
        if j is not None and j not in used_b and b_cells[j]["cell_type"] == c["cell_type"]:
            pairs[i] = j
            used_b.add(j)
    free_a = [i for i in range(len(a_cells)) if i not in pairs]
    # identical content
    for i in list(free_a):
        for j, bc in enumerate(b_cells):
            if j not in used_b and _sig(bc) == _sig(a_cells[i]):
                pairs[i] = j
                used_b.add(j)
                free_a.remove(i)
                break
    # similar content, in order
    for i in list(free_a):
        best, best_j = 0.0, None
        for j, bc in enumerate(b_cells):
            if j in used_b or bc["cell_type"] != a_cells[i]["cell_type"]:
                continue
            r = difflib.SequenceMatcher(None, a_cells[i]["source"], bc["source"], autojunk=False).quick_ratio()
            if r >= SIMILARITY:
                r = difflib.SequenceMatcher(None, a_cells[i]["source"], bc["source"], autojunk=False).ratio()
            if r >= SIMILARITY and r > best:
                best, best_j = r, j
        if best_j is not None:
            pairs[i] = best_j
            used_b.add(best_j)
    return pairs


def _lis(seq: list[int]) -> set[int]:
    """Indices (positions in ``seq``) of one longest increasing subsequence."""
    if not seq:
        return set()
    tails: list[int] = []
    tail_idx: list[int] = []
    prev = [-1] * len(seq)
    for k, v in enumerate(seq):
        lo, hi = 0, len(tails)
        while lo < hi:
            mid = (lo + hi) // 2
            if tails[mid] < v:
                lo = mid + 1
            else:
                hi = mid
        if lo == len(tails):
            tails.append(v)
            tail_idx.append(k)
        else:
            tails[lo], tail_idx[lo] = v, k
        prev[k] = tail_idx[lo - 1] if lo else -1
    out: set[int] = set()
    k = tail_idx[-1]
    while k != -1:
        out.add(k)
        k = prev[k]
    return out


def _udiff(a: str, b: str, fa: str, fb: str, context: int) -> str:
    return "\n".join(difflib.unified_diff(a.split("\n") if a else [], b.split("\n") if b else [], fromfile=fa, tofile=fb, n=context, lineterm=""))


def diff_cells(a_nb: dict[str, Any] | None, b_nb: dict[str, Any] | None, *, context: int = 3) -> dict[str, Any]:
    a_cells = list((a_nb or {}).get("cells") or [])
    b_cells = list((b_nb or {}).get("cells") or [])
    pairs = _match(a_cells, b_cells)
    inv = {j: i for i, j in pairs.items()}
    order = sorted(pairs)
    moved_a = {order[k] for k in range(len(order)) if k not in _lis([pairs[i] for i in order])}
    entries: list[dict[str, Any]] = []
    removed_after: dict[int, list[int]] = {}  # b-index (-1 = start) -> removed a cells to show after that b position
    last_partner = -1
    for i in range(len(a_cells)):
        if i in pairs:
            last_partner = max(last_partner, pairs[i]) if i not in moved_a else last_partner
        else:
            removed_after.setdefault(last_partner, []).append(i)
    summary = {"added": 0, "removed": 0, "modified": 0, "moved": 0, "unchanged": 0}

    def removed(i: int) -> dict[str, Any]:
        c = a_cells[i]
        summary["removed"] += 1
        return {"status": "removed", "id": c["id"], "cell_type": c["cell_type"], "index_a": i, "index_b": None,
                "diff": _udiff(c["source"], "", f"a/cell[{i}]:{c['id']}", "/dev/null", context)}

    entries.extend(removed(i) for i in removed_after.get(-1, []))
    for j, bc in enumerate(b_cells):
        i = inv.get(j)
        if i is None:
            summary["added"] += 1
            entries.append({"status": "added", "id": bc["id"], "cell_type": bc["cell_type"], "index_a": None, "index_b": j,
                            "diff": _udiff("", bc["source"], "/dev/null", f"b/cell[{j}]:{bc['id']}", context)})
        else:
            ac = a_cells[i]
            changed_src = ac["source"] != bc["source"]
            type_changed = ac["cell_type"] != bc["cell_type"]
            ta, tb = sorted((ac.get("metadata") or {}).get("tags") or []), sorted((bc.get("metadata") or {}).get("tags") or [])
            meta_changed = _meta(ac) != _meta(bc)
            status = "modified" if (changed_src or type_changed or meta_changed) else "unchanged"
            summary[status] += 1
            e: dict[str, Any] = {"status": status, "id": bc["id"], "cell_type": bc["cell_type"], "index_a": i, "index_b": j}
            if changed_src:
                e["diff"] = _udiff(ac["source"], bc["source"], f"a/cell[{i}]:{ac['id']}", f"b/cell[{j}]:{bc['id']}", context)
            if ta != tb:
                e["tags"] = {"a": ta, "b": tb}
            if type_changed:
                e["cell_type_changed"] = {"a": ac["cell_type"], "b": bc["cell_type"]}
            if meta_changed and not (ta != tb):
                e["metadata_changed"] = True
            if i in moved_a:
                e["moved"] = True
                summary["moved"] += 1
            entries.append(e)
        entries.extend(removed(r) for r in removed_after.get(j, []))
    return {"summary": summary, "cells": entries}


def diff_settings(a: dict[str, Any] | None, b: dict[str, Any] | None, prefix: str = "") -> list[dict[str, Any]]:
    """Flat list of ``{path, a, b}`` for every settings leaf that differs."""
    a, b = a or {}, b or {}
    out: list[dict[str, Any]] = []
    for k in sorted(set(a) | set(b)):
        path = f"{prefix}{k}"
        va, vb = a.get(k), b.get(k)
        if isinstance(va, dict) and isinstance(vb, dict):
            out.extend(diff_settings(va, vb, path + "."))
        elif va != vb:
            out.append({"path": path, "a": va, "b": vb})
    return out


def diff_notebooks(a_def: dict[str, Any] | None, b_def: dict[str, Any] | None, *, context: int = 3) -> dict[str, Any]:
    """Diff two notebook *definitions* (``{"nbformat": ..., "settings": ...}``); outputs and execution counts are ignored."""
    a_nb = (a_def or {}).get("nbformat")
    b_nb = (b_def or {}).get("nbformat")
    res = diff_cells(a_nb, b_nb, context=context)
    am, bm = (a_nb or {}).get("metadata") or {}, (b_nb or {}).get("metadata") or {}
    res["metadata_changed"] = json.dumps(am, sort_keys=True) != json.dumps(bm, sort_keys=True)
    res["settings"] = diff_settings((a_def or {}).get("settings"), (b_def or {}).get("settings"))
    s = res["summary"]
    res["identical"] = not (s["added"] or s["removed"] or s["modified"] or s["moved"] or res["metadata_changed"] or res["settings"])
    return res


def render_text(diff: dict[str, Any], a_label: str = "a", b_label: str = "b") -> str:
    """One patch-like text for terminals and code review tools."""
    lines = [f"--- {a_label}", f"+++ {b_label}"]
    for e in diff["cells"]:
        if e["status"] == "unchanged" and not e.get("moved"):
            continue
        head = f"@@ cell {e['id']} ({e['cell_type']}) {e['status']}" + (" moved" if e.get("moved") else "")
        if e.get("tags"):
            head += f" tags {e['tags']['a']} -> {e['tags']['b']}"
        lines.append(head)
        if e.get("diff"):
            lines.append(e["diff"])
    for s in diff.get("settings", []):
        lines.append(f"@@ settings {s['path']}: {json.dumps(s['a'])} -> {json.dumps(s['b'])}")
    if diff.get("metadata_changed"):
        lines.append("@@ notebook metadata changed")
    return "\n".join(lines) + "\n"
