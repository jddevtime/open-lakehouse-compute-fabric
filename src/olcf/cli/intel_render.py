# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Pure renderers of OLCF Intelligence answers for the terminal (no I/O, no click): the ASCII lineage tree, the column-lineage rows, origin chains,
impact and staleness summaries and connectivity-test lines.

Every function takes the decoded JSON of the REST API and returns text lines or table rows, so the CLI commands stay thin and these shapes are testable on their own.
A ``style(text, role)`` callback may colour confidence words (``verified``/``derived``/``inferred``/``unknown``); the default leaves text untouched.

The lineage documents use ``from``/``to`` on every edge for the direction of the **data flow** (``src``/``dst`` is the storage direction of the edge type), so
"upstream of X" is whatever flows into X and "downstream" whatever X flows into.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

Style = Callable[[str, str], str]

#: A tree line never grows past this many nodes (the server caps lineage answers too; this protects a terminal from a pathological diamond graph)
MAX_TREE_LINES = 2000


def plain(text: str, role: str = "") -> str:
    """The identity style: no colour."""
    return text


def label(node: Mapping[str, Any] | None, node_id: str = "", *, ids: bool = False) -> str:
    """``name (type)`` for a node, flagged when it is an unresolved placeholder or removed; ``ids`` appends the asset id."""
    if not node:
        return node_id or "?"
    out = f"{node.get('name') or node_id} ({node.get('type') or '?'})"
    if node.get("placeholder") or node.get("status") == "placeholder":
        out += " [unresolved]"
    elif node.get("status") == "removed":
        out += " [removed]"
    if ids and node.get("id"):
        out += f"  {node['id']}"
    return out


def node_index(doc: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """Nodes of a lineage / origin / impact document by id (the root included)."""
    out: dict[str, dict[str, Any]] = {}
    root = doc.get("root") or doc.get("asset")
    if isinstance(root, Mapping) and root.get("id"):
        out[str(root["id"])] = dict(root)
    for n in doc.get("nodes") or []:
        if isinstance(n, Mapping) and n.get("id"):
            out.setdefault(str(n["id"]), dict(n))
    return out


def _walk(root_id: str, nxt: Mapping[str, list[tuple[str, Mapping[str, Any]]]], nodes: Mapping[str, Mapping[str, Any]], arrow: str, style: Style, ids: bool,
          budget: list[int]) -> list[str]:
    """Indented tree below ``root_id`` following ``nxt``; a node reached a second time is shown once more with ``(see above)`` and not expanded again."""
    lines: list[str] = []
    expanded: set[str] = {root_id}

    def visit(node_id: str, prefix: str, path: frozenset[str]) -> None:
        children = sorted(nxt.get(node_id, []), key=lambda c: (str((nodes.get(c[0]) or {}).get("name") or c[0]).lower(), c[0]))
        for i, (child, edge) in enumerate(children):
            if budget[0] <= 0:
                return
            budget[0] -= 1
            last = i == len(children) - 1
            branch = "└── " if last else "├── "
            conf = str(edge.get("confidence") or "unknown")
            text = f"{arrow} {label(nodes.get(child), child, ids=ids)}  [{style(conf, conf)}]"
            if edge.get("virtual"):
                text += " (governance)"
            if child in path:
                lines.append(f"{prefix}{branch}{text} (cycle)")
                continue
            if child in expanded and nxt.get(child):
                lines.append(f"{prefix}{branch}{text} (see above)")
                continue
            lines.append(f"{prefix}{branch}{text}")
            expanded.add(child)
            visit(child, prefix + ("    " if last else "│   "), path | {child})

    visit(root_id, "", frozenset({root_id}))
    return lines


def lineage_tree(doc: Mapping[str, Any], *, style: Style = plain, ids: bool = False) -> list[str]:
    """ASCII tree of a table-level lineage document: the asset, then ``upstream`` (what flows into it) and ``downstream`` (what it flows into).

    Each line is ``<- name (type)  [confidence]`` (upstream) or ``-> name (type)  [confidence]``; the confidence is the one of the edge that connects the line to its parent,
    and processes (notebooks, pipelines, queries) appear between the datasets they connect.  Truncation and gaps are summarised at the end.
    """
    nodes = node_index(doc)
    root = doc.get("root") or {}
    root_id = str(root.get("id") or "")
    fwd: dict[str, list[tuple[str, Mapping[str, Any]]]] = {}
    back: dict[str, list[tuple[str, Mapping[str, Any]]]] = {}
    for e in doc.get("edges") or []:
        a, b = str(e.get("from") or e.get("src") or ""), str(e.get("to") or e.get("dst") or "")
        if not a or not b:
            continue
        fwd.setdefault(a, []).append((b, e))
        back.setdefault(b, []).append((a, e))
    direction = str(doc.get("direction") or "both")
    budget = [MAX_TREE_LINES]
    lines = [label(root, root_id, ids=ids) + (f"  {root.get('path')}" if root.get("path") and root.get("path") != root.get("name") else "")]
    sections: list[tuple[str, list[str]]] = []
    if direction in ("up", "both"):
        sections.append(("upstream (flows into it)", _walk(root_id, back, nodes, "<-", style, ids, budget)))
    if direction in ("down", "both"):
        sections.append(("downstream (it flows into)", _walk(root_id, fwd, nodes, "->", style, ids, budget)))
    for i, (title, body) in enumerate(sections):
        last = i == len(sections) - 1
        lines.append(("└── " if last else "├── ") + title + ("" if body else ": none found"))
        pad = "    " if last else "│   "
        lines.extend(pad + b for b in body)
    lines.extend(_notes(doc, budget[0] <= 0))
    return lines


def _notes(doc: Mapping[str, Any], clipped: bool) -> list[str]:
    out: list[str] = []
    if clipped:
        out.append(f"(output cut at {MAX_TREE_LINES} lines; narrow it with --depth, --types or --min-confidence)")
    st = doc.get("stats") or {}
    sw = doc.get("level_switched")
    if sw:
        out.append(f"(the asset is a column, so the answer is at {sw.get('to', 'column')} level)")
    if doc.get("truncated"):
        if "max_impact_nodes" in st:
            out.append(f"(the analysis was cut at {st['max_impact_nodes']} downstream assets, so the totals are a lower bound; lower --depth)")
        else:
            out.append(f"(the answer was cut at the server's node cap of {st.get('max_nodes', '?')}; lower --depth or filter with --types)")
    if doc.get("items_truncated"):
        out.append(f"(only the nearest {st.get('listed', '?')} of {doc.get('total', '?')} assets are listed; the totals above are complete)")
    gaps = doc.get("gaps") or []
    if gaps:
        kinds: dict[str, int] = {}
        for g in gaps:
            kinds[str(g.get("kind"))] = kinds.get(str(g.get("kind")), 0) + 1
        out.append(f"lineage is incomplete: {len(gaps)} gap(s) ({', '.join(f'{k} x{n}' for k, n in sorted(kinds.items()))}); see `olcf intelligence gaps`")
    return out


def column_rows(doc: Mapping[str, Any], table_names: Mapping[str, str] | None = None) -> list[dict[str, Any]]:
    """Rows for the column-level lineage table: one per ``TRANSFORMS`` edge with ``from``/``to`` as ``table.column``, the process in ``via`` and the transformation text.

    ``table_names`` maps a table asset id to its name (the node documents only carry ``parent_id``); unknown parents are shown by column name alone.
    """
    nodes = node_index(doc)
    names = dict(table_names or {})

    def col(node_id: str) -> str:
        n = nodes.get(node_id) or {}
        parent = str(n.get("parent_id") or "")
        table = names.get(parent) or ((nodes.get(parent) or {}).get("name") if parent else None)
        return f"{table}.{n.get('name') or node_id}" if table else str(n.get("name") or node_id)

    rows: list[dict[str, Any]] = []
    for e in doc.get("edges") or []:
        a, b = str(e.get("from") or e.get("src") or ""), str(e.get("to") or e.get("dst") or "")
        na, nb = nodes.get(a) or {}, nodes.get(b) or {}
        side = "upstream" if na.get("direction") == "upstream" else "downstream" if nb.get("direction") == "downstream" else na.get("direction") or nb.get("direction") or ""
        via = nodes.get(str(e.get("via") or "")) or {}
        rows.append({"direction": side, "from": col(a), "to": col(b), "via": via.get("name") or e.get("via") or "", "transformation": e.get("transformation") or "",
                     "confidence": e.get("confidence") or "unknown", "edge_id": e.get("id")})
    rows.sort(key=lambda r: ({"upstream": 0, "downstream": 1}.get(r["direction"], 2), r["from"], r["to"]))
    return rows


def parent_ids(doc: Mapping[str, Any]) -> list[str]:
    """Asset ids of the parents of the column nodes whose parent is not itself part of the document (their names must be fetched to print ``table.column``)."""
    nodes = node_index(doc)
    return sorted({str(n["parent_id"]) for n in nodes.values() if n.get("type") == "column" and n.get("parent_id") and str(n["parent_id"]) not in nodes})


def qualified(node: Mapping[str, Any] | None, nodes: Mapping[str, Mapping[str, Any]], table_names: Mapping[str, str] | None = None) -> str:
    """The name of a node, ``table.column`` for a column whose table name is known (a bare column name such as ``amount`` is ambiguous across tables)."""
    n = node or {}
    name = str(n.get("name") or n.get("id") or "?")
    if n.get("type") != "column" or not n.get("parent_id"):
        return name
    parent = str(n["parent_id"])
    table = (table_names or {}).get(parent) or (nodes.get(parent) or {}).get("name")
    return f"{table}.{name}" if table else name


def origin_roots(doc: Mapping[str, Any], table_names: Mapping[str, str] | None = None) -> list[dict[str, Any]]:
    """Rows for the table of upstream roots of an origin document (columns are named ``table.column`` when ``table_names`` knows their table)."""
    nodes = node_index(doc)
    rows = []
    for r in doc.get("roots") or []:
        a = r.get("asset") or {}
        rows.append({"name": qualified(a, nodes, table_names), "type": a.get("type"), "path": a.get("path") or "", "workspace_id": a.get("workspace_id") or "(organisation)",
                     "source": "yes" if r.get("is_source") else "no", "paths": r.get("path_count"), "confidence": r.get("best_confidence"), "id": a.get("id")})
    return rows


def origin_chains(doc: Mapping[str, Any], *, style: Style = plain, ids: bool = False, table_names: Mapping[str, str] | None = None) -> list[str]:
    """One line per path of an origin document, source first: ``raw -> etl -> clean ... -> sales_report  [inferred, 4 hops via etl]``."""
    nodes = node_index(doc)
    out = []
    for p in doc.get("paths") or []:
        chain = " -> ".join(qualified(nodes.get(str(n)) or {"id": str(n)}, nodes, table_names) for n in p.get("nodes") or [])
        conf = str(p.get("confidence") or "unknown")
        procs = [str((nodes.get(str(x)) or {}).get("name") or x) for x in p.get("processes") or []]
        tail = f"{style(conf, conf)}, {p.get('length', '?')} hop(s)" + (f", via {', '.join(procs)}" if procs else "")
        out.append(f"{chain}  [{tail}]")
    return out


def impact_rows(doc: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Rows of everything downstream of an asset, grouped by type, nearest first; ``owners`` is filled when governance data exists."""
    rows = []
    for typ, group in (doc.get("by_type") or {}).items():
        for a in group.get("items") or []:
            rows.append({"type": typ, "name": a.get("name"), "path": a.get("path") or "", "workspace_id": a.get("workspace_id") or "(organisation)", "hops": a.get("level"),
                         "confidence": a.get("confidence_floor"), "owners": [o.get("name") if isinstance(o, Mapping) else o for o in (a.get("owners") or [])], "id": a.get("id")})
    rows.sort(key=lambda r: (r["hops"] if isinstance(r["hops"], int) else 99, str(r["type"]), str(r["name"])))
    return rows


def staleness_rows(doc: Mapping[str, Any], *, only_stale: bool = False) -> list[dict[str, Any]]:
    """Rows of the producers examined for freshness (the asset itself and what feeds it), the root cause marked."""
    rows = []
    for n in doc.get("nodes") or []:
        if only_stale and not n.get("stale"):
            continue
        a = n.get("asset") or {}
        run = n.get("last_run") or {}
        rows.append({"name": a.get("name"), "type": a.get("type"), "stale": bool(n.get("stale")), "root_cause": bool(n.get("root_cause")), "last_write_at": n.get("last_write_at"),
                     "age_hours": n.get("age_hours"), "last_run": run.get("state") or ("seen" if run else None), "reasons": "; ".join(n.get("reasons") or []), "id": a.get("id")})
    return rows


def step_lines(result: Mapping[str, Any], *, style: Style = plain) -> list[str]:
    """Lines of a connectivity test: ``[ok]``/``[FAIL]``/``[skip]`` per step with its detail and, for a failure, the remediation on the next line."""
    out = []
    steps = result.get("steps") or []
    for i, s in enumerate(steps, 1):
        if s.get("skipped") and s.get("ok"):
            tag = style("[skip]", "warn")
        elif s.get("skipped"):
            tag = style("[----]", "warn")
        elif s.get("ok"):
            tag = style("[ ok ]", "ok")
        else:
            tag = style("[FAIL]", "fail")
        ms = s.get("duration_ms")
        out.append(f"{i}. {tag} {s.get('name')}" + (f" ({ms:g} ms)" if isinstance(ms, (int, float)) and ms and not s.get("skipped") else "") + (f" - {s['detail']}" if s.get("detail") else ""))
        if not s.get("ok") and not s.get("skipped") and s.get("remediation"):
            out.append(f"        fix: {s['remediation']}")
    return out


def count_by(items: Sequence[Mapping[str, Any]], key: str) -> dict[str, int]:
    """``{value: count}`` of ``key`` over ``items`` (for compact summaries)."""
    out: dict[str, int] = {}
    for it in items:
        k = str(it.get(key))
        out[k] = out.get(k, 0) + 1
    return out
