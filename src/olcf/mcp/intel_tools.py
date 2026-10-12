# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""MCP tools of OLCF Intelligence: ask the asset graph where data comes from, what depends on it, whether it is fresh and what the lineage cannot see.

Every tool here is **read-only** (ADR 0014: the ``mcp`` token scope carries ``intelligence.read`` and nothing that writes) and calls the Intelligence service with the
caller's own principal, so the visibility rules of the REST API apply unchanged: an asset outside the caller's workspaces answers ``not_found`` exactly like one that does
not exist.  There is deliberately **no tool for the sample endpoint** (``GET /intelligence/assets/{id}/sample``): sample rows are data, and an agent reaches data through
``run_sql`` under the SQL guard; nothing in this module touches the sampling service, and the column statistics the scans keep (min / max) are left out of the answers too.

Answers are small and shaped for a model, not for a screen:

* assets are named by a compact reference (``id``, ``type``, ``name``, ``path``) - the tools accept an id, a name, a path (``lakehouse.table``) or ``table.column``;
* long lists are cut to a ``max_*`` argument (with hard ceilings) and say so: ``truncated: true`` plus ``omitted`` counts, never a silent cut;
* every lineage statement carries its **confidence** (``verified`` = observed at run time by the platform, ``derived`` = parsed from code or read from a definition, ``inferred`` = matched by name
  or a heuristic, ``unknown``), an **evidence summary** (how often and when it was seen) and the **gaps** (what discovery could not read) that make the picture incomplete;
* ``summary`` is a sentence an agent can quote.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from olcf.core.errors import BadRequest, NotFound, Unavailable
from olcf.mcp.protocol import Tool, obj

if TYPE_CHECKING:
    from olcf.mcp.tools import Env

#: confidence levels, strongest first
CONFIDENCE = ("verified", "derived", "inferred", "unknown")
_RANK = {c: i for i, c in enumerate(CONFIDENCE)}
CONF_SCHEMA = {"type": "string", "enum": list(CONFIDENCE), "description": "Only follow links at least this strong (verified > derived > inferred > unknown)"}
ASSET_ARG = {"type": "string", "minLength": 1, "maxLength": 512,
             "description": "An asset id (ast_...), a name ('sales'), a path ('retail_lakehouse.sales' or 'retail_lakehouse/sales') or 'table.column'. Use intel_search_assets to find it."}
WORKSPACE_ARG = {"type": "string", "description": "Workspace name or id that narrows a name or path (optional)"}
TYPE_ARG = {"type": "string", "maxLength": 32, "description": "Asset type that narrows an ambiguous name: table, view, column, notebook, pipeline, semantic_model, measure, report, dashboard ..."}
_SEP = re.compile(r"[./]")
_SLASHES = re.compile(r"\s*/\s*")


# ----------------------------------------------------------------------------------------------------------------------------------- helpers
def _svc(env: Env) -> Any:
    """The Intelligence service, or ``Unavailable`` when the module is not installed."""
    svc = env.ctx.maybe_service("intelligence")
    if svc is None:
        raise Unavailable("OLCF Intelligence is not enabled on this server", remediation="ask an administrator to enable the intelligence module")
    return svc


def clip(value: Any, n: int = 240) -> Any:
    """``value`` shortened to ``n`` characters with an ellipsis (non-strings are returned unchanged)."""
    return value if not isinstance(value, str) or len(value) <= n else value[: n - 1] + "…"


def cap(n: Any, default: int, ceiling: int) -> int:
    """A list limit: ``n`` if it is an integer within ``1..ceiling``, otherwise ``default`` (the JSON Schema already rejects other values; this is the second line)."""
    return max(1, min(int(n), ceiling)) if isinstance(n, int) and not isinstance(n, bool) else default


def weakest(values: Iterable[str | None]) -> str | None:
    """The weakest confidence among ``values`` (``None`` when there are none)."""
    known = [v for v in values if v in _RANK]
    return max(known, key=_RANK.__getitem__) if known else None


def ref(a: Mapping[str, Any] | None, label: str | None = None) -> dict[str, Any]:
    """Compact reference to an asset: ``{id, type, name, path}`` (``path`` only when it exists, ``label`` when it differs from the name, e.g. ``clean.amount`` for a column)."""
    if not a:
        return {}
    out: dict[str, Any] = {"id": a.get("id"), "type": a.get("type"), "name": a.get("name")}
    if a.get("path"):
        out["path"] = a["path"]
    if label and label not in (a.get("name"), a.get("path")):
        out["label"] = label
    if a.get("placeholder") or a.get("status") == "placeholder":
        out["placeholder"] = True
    return out


def _ws_name(env: Env, cache: dict[str, str | None], workspace_id: str | None) -> str | None:
    """Name of a workspace (``None`` for organisation-level assets), cached for the call."""
    if not workspace_id:
        return None
    if workspace_id not in cache:
        try:
            cache[workspace_id] = env.ctx.service("workspaces").resolve(workspace_id)["name"]
        except Exception:  # noqa: BLE001 - a name is a convenience, never a reason to fail
            cache[workspace_id] = None
    return cache[workspace_id]


def _parent_labels(env: Env, nodes: Iterable[Mapping[str, Any]]) -> dict[str, str]:
    """``{parent_id: parent name}`` for the parents of column nodes the caller can see, so a column reads ``clean.amount`` instead of ``amount``."""
    ids = sorted({n["parent_id"] for n in nodes if n.get("type") == "column" and n.get("parent_id")})
    if not ids:
        return {}
    svc = _svc(env)
    rows = svc.graph.asset_rows(ids)
    return {i: r["name"] for i, r in rows.items() if svc.can_see(env.principal, r["workspace_id"])}


def _label(node: Mapping[str, Any], parents: Mapping[str, str]) -> str:
    """The most readable single name of a node: ``table.column`` for a column, the path for a dataset, the name otherwise."""
    if node.get("type") == "column" and node.get("parent_id") in parents:
        return f"{parents[node['parent_id']]}.{node.get('name')}"
    return str(node.get("path") or node.get("name") or node.get("id"))


def _gaps(doc: Mapping[str, Any], limit: int = 8) -> dict[str, Any]:
    """The open gaps of an analysis, trimmed: what could not be read and how to fix it (``gap_count`` is the full number)."""
    items = list(doc.get("gaps") or [])
    return {"gap_count": len(items), "gaps": [{"kind": g.get("kind"), "subject": clip(g.get("subject"), 120), "detail": clip(g.get("detail"), 200),
                                              "fix": clip(g.get("remediation"), 200)} for g in items[:limit]]}


def _touch(env: Env, *rows: Mapping[str, Any]) -> None:
    """Record the workspaces an answer touched (the audit trail of the call names the workspace when it is exactly one)."""
    for r in rows:
        if r.get("workspace_id"):
            env.workspace_touched.add(r["workspace_id"])


def _candidates(env: Env, rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Compact candidates for an ambiguity or not-found message."""
    parents = _parent_labels(env, rows)
    return [ref(r, _label(r, parents)) for r in rows[:8]]


def resolve_asset(env: Env, text: str, *, type_: str | None = None, workspace: str | None = None) -> dict[str, Any]:
    """Resolve what an agent typed to ONE visible asset (the public form of the row).

    ``ast_...`` ids are looked up directly.  Anything else is matched case-insensitively against the asset name and path (``lakehouse.table``, ``lakehouse/table`` and a
    trailing path both work); ``table.column`` falls back to the column of that table.  Several matches are an error that lists the candidates (pass ``type``,
    ``workspace`` or an id); no match is a ``not_found`` that suggests the closest names.  Assets the caller cannot see are never matched or hinted at.
    """
    svc, p = _svc(env), env.principal
    text = text.strip()
    if not text:
        raise BadRequest("asset must not be empty", remediation="pass an asset id, a name, a path or table.column; intel_search_assets finds them")
    if text.startswith("ast_"):
        row = svc.public(svc.row_for(p, text))
        _touch(env, row)
        return row
    ws_id = env.workspace(workspace, "intelligence.read")["id"] if workspace else None
    key = text.lower()

    def find(q: str, want: str, parts: list[str]) -> list[dict[str, Any]]:
        slash = "/".join(parts)
        full = _SLASHES.sub("/", want).replace(".", "/")
        out = []
        for c in svc.search(p, q=q, type=type_, workspace_id=ws_id, limit=100):
            path = _SLASHES.sub("/", (c.get("path") or "").lower())       # display paths read 'Sales / lakehouse / table'
            if c["name"].lower() == want and (len(parts) <= 1 or path == slash or path.endswith("/" + slash)):
                out.append(c)
            elif path and (path == full or path.endswith("/" + full)):
                out.append(c)
        return out

    exact = find(text, key, [key])
    parts = [x for x in _SEP.split(key) if x]
    if not exact and len(parts) > 1:
        exact = find(parts[-1], parts[-1], parts)
        if not exact:
            parent_text = ".".join(parts[:-1])
            try:
                parent = resolve_asset(env, parent_text, type_=None, workspace=workspace)
            except (NotFound, BadRequest):
                parent = None
            if parent is not None:
                kids, _ = svc.children(p, parent["id"], type="column", limit=500)
                exact = [k for k in kids if k["name"].lower() == parts[-1]]
    if len(exact) == 1:
        _touch(env, exact[0])
        return dict(exact[0])
    if len(exact) > 1:
        raise BadRequest(f"'{text}' matches {len(exact)} assets; candidates: " + "; ".join(f"{c.get('label') or c['name']} ({c['type']}, {c['id']})" for c in _candidates(env, exact)),
                         remediation="pass the id, or narrow with the type and workspace arguments", details={"candidates": _candidates(env, exact)})
    near = svc.search(p, q=parts[-1] if parts else text, type=type_, workspace_id=ws_id, limit=5)
    hint = ("; closest: " + ", ".join(f"{c['name']} ({c['type']})" for c in near)) if near else ""
    raise NotFound(f"no asset named '{text}' is visible to you{hint}", remediation="call intel_search_assets with part of the name to find the asset and its id",
                   details={"closest": _candidates(env, near)} if near else None)


def _as_column(env: Env, row: dict[str, Any], column: str | None) -> tuple[dict[str, Any], str | None]:
    """Lineage and origin of a column are asked on its table with ``column=``; turn a resolved column asset into that pair (``(table row, column name)``)."""
    if row["type"] != "column":
        return row, column
    svc = _svc(env)
    parent = svc.public(svc.row_for(env.principal, row["parent_id"])) if row.get("parent_id") else None
    if parent is None:
        raise BadRequest(f"column '{row['name']}' has no table to trace from", remediation="use the id of the table and the column argument")
    return parent, row["name"]


# ----------------------------------------------------------------------------------------------------------------------------------- intel_search_assets
def intel_search_assets(env: Env, a: dict[str, Any]) -> dict[str, Any]:
    """Ranked search of the asset graph (or a filtered listing when there is no query)."""
    svc, p = _svc(env), env.principal
    limit = cap(a.get("limit"), 15, 50)
    ws = env.workspace(a["workspace"], "intelligence.read")["id"] if a.get("workspace") else None
    filters = {k: a[k] for k in ("domain", "classification", "certification", "owner", "tag") if a.get(k)}
    if a.get("query"):
        rows = svc.search(p, q=a["query"], type=a.get("type"), workspace_id=ws, limit=limit + 1, status=a.get("status"), **filters)
    elif filters or a.get("type") or ws:
        rows, _ = svc.list_assets(p, type=a.get("type"), workspace_id=ws, status=a.get("status"), limit=limit + 1, **filters)
    else:
        raise BadRequest("give a query or at least one filter (type, workspace, domain, classification, certification, owner, tag)",
                         remediation="try intel_search_assets with query='customer' or type='report'")
    more = len(rows) > limit
    rows = rows[:limit]
    parents = _parent_labels(env, rows)
    names: dict[str, str | None] = {}
    items = []
    for r in rows:
        item = ref(r, _label(r, parents))
        item["workspace"] = _ws_name(env, names, r.get("workspace_id"))
        if r.get("description"):
            item["description"] = clip(r["description"], 160)
        if r.get("tags"):
            item["tags"] = r["tags"][:8]
        if r.get("status") not in (None, "active"):
            item["status"] = r["status"]
        if "score" in r:
            item["score"] = r["score"]
        items.append(item)
    _touch(env, *rows)
    out: dict[str, Any] = {"count": len(items), "assets": items, "truncated": more}
    if more:
        out["note"] = f"more than {limit} assets match; narrow the query or add type / workspace filters"
    if not items:
        out["note"] = "nothing matched; try a shorter or different word, drop the type filter, or look at intel_summary for what exists"
    return out


# ----------------------------------------------------------------------------------------------------------------------------------- intel_get_asset
_PROPS = ("format", "location", "row_count", "size_bytes", "num_files", "last_modified", "partition_columns", "table_type")


def intel_get_asset(env: Env, a: dict[str, Any]) -> dict[str, Any]:
    """One asset: what it is, where it lives, its schema, how connected it is in the lineage graph, open gaps and (when governance is installed) governance facts."""
    svc = _svc(env)
    found = resolve_asset(env, a["asset"], type_=a.get("type"), workspace=a.get("workspace"))
    doc = svc.get_asset(env.principal, found["id"])
    attrs = doc.get("attrs") or {}
    names: dict[str, str | None] = {}
    cols = [c for c in attrs.get("columns") or [] if isinstance(c, dict) and c.get("name")]
    max_cols = cap(a.get("max_columns"), 40, 200)
    out: dict[str, Any] = {**ref(doc), "workspace": _ws_name(env, names, doc.get("workspace_id")), "source": doc.get("source_id"), "status": doc.get("status"),
                           "description": clip(doc.get("description") or "", 600), "description_source": doc.get("description_source"), "tags": (doc.get("tags") or [])[:20]}
    if doc.get("parent"):
        out["parent"] = {k: doc["parent"].get(k) for k in ("id", "type", "name")}
    out["child_count"] = doc.get("child_count", 0)
    out["lineage_edges"] = doc.get("lineage_edges") or {}
    out["open_gaps"] = doc.get("open_gaps", 0)
    props = {k: attrs[k] for k in _PROPS if attrs.get(k) not in (None, "", [])}
    if props:
        out["properties"] = props
    if cols:
        # name / type / description / nullability only: the min / max statistics of the scans stay out of MCP answers
        out["columns"] = [{k: clip(c.get(k), 160) for k in ("name", "type", "description", "nullable") if c.get(k) not in (None, "")} for c in cols[:max_cols]]
        out["column_count"] = len(cols)
        out["truncated"] = len(cols) > max_cols
    else:
        out["truncated"] = False
    out["aliases"] = [{"kind": x["kind"], "value": clip(x["value"], 160)} for x in (doc.get("aliases") or [])[:10]]
    if doc.get("governance"):
        out["governance"] = {k: (v[:10] if isinstance(v, list) else v) for k, v in doc["governance"].items()}
    out["next"] = ["intel_lineage", "intel_where_does_this_come_from", "intel_impact_analysis", "intel_staleness", "intel_documentation"]
    return out


# ----------------------------------------------------------------------------------------------------------------------------------- intel_lineage
def _graph_out(env: Env, doc: Mapping[str, Any], root: Mapping[str, Any], *, max_nodes: int, max_edges: int) -> dict[str, Any]:
    """Compact nodes / edges of a lineage document: nearest first, edges only between kept nodes, with confidence and an evidence summary on every edge."""
    nodes = [n for n in doc.get("nodes") or [] if n.get("id") != root.get("id")]
    parents = _parent_labels(env, [root, *nodes])
    nodes.sort(key=lambda n: (n.get("level") if isinstance(n.get("level"), int) else 99, str(n.get("direction")), str(n.get("name"))))
    kept = nodes[:max_nodes]
    keep_ids = {n["id"] for n in kept} | {root["id"]}
    label = {n["id"]: _label(n, parents) for n in [root, *nodes]}
    out_nodes = [{**ref(n, label[n["id"]]), "hops": n.get("level"), "direction": n.get("direction"), "confidence": n.get("confidence_floor")} for n in kept]
    all_edges = list(doc.get("edges") or [])
    edges = []
    for e in all_edges:
        if e.get("from") in keep_ids and e.get("to") in keep_ids:
            row: dict[str, Any] = {"from": e["from"], "to": e["to"], "from_name": label.get(e["from"]), "to_name": label.get(e["to"]), "type": e.get("type"),
                                   "confidence": e.get("confidence"), "evidence_count": e.get("evidence_count"), "last_seen_at": e.get("last_seen_at")}
            if e.get("last_verified_at"):
                row["last_verified_at"] = e["last_verified_at"]
            if e.get("via"):
                row["via"] = label.get(e["via"], e["via"])
            if e.get("transformation"):
                row["transformation"] = clip(e["transformation"], 200)
            edges.append(row)
    omitted_edges = max(0, len(all_edges) - len(edges))
    edges = edges[:max_edges]
    cut = len(nodes) > len(kept) or omitted_edges > 0 or len(all_edges) > 0 and len(edges) < min(len(all_edges), max_edges)
    return {"nodes": out_nodes, "edges": edges, "omitted": {"nodes": max(0, len(nodes) - len(kept)), "edges": max(0, len(all_edges) - len(edges))},
            "truncated": bool(doc.get("truncated")) or cut, "label": label}


def _fmt_counts(counts: Mapping[str, int]) -> str:
    """``{'verified': 1, 'derived': 6}`` as ``1 verified, 6 derived`` (strongest first); ``none`` when empty."""
    parts = [f"{counts[c]} {c}" for c in CONFIDENCE if counts.get(c)]
    return ", ".join(parts) or "none"


def _counts(doc: Mapping[str, Any], root_id: str) -> dict[str, Any]:
    """Counts of the datasets (not the processes) up- and downstream, and of the edges by confidence."""
    up = down = 0
    by_type: dict[str, int] = {}
    for n in doc.get("nodes") or []:
        if n.get("id") == root_id:
            continue
        by_type[n.get("type")] = by_type.get(n.get("type"), 0) + 1
        if n.get("direction") == "upstream":
            up += 1
        elif n.get("direction") == "downstream":
            down += 1
    conf: dict[str, int] = {}
    for e in doc.get("edges") or []:
        conf[e.get("confidence")] = conf.get(e.get("confidence"), 0) + 1
    return {"upstream_nodes": up, "downstream_nodes": down, "nodes_by_type": by_type, "edges_by_confidence": conf}


def intel_lineage(env: Env, a: dict[str, Any]) -> dict[str, Any]:
    """Lineage around an asset, table or column level, up and/or down, with confidence, evidence summaries and gaps."""
    svc = _svc(env)
    found = resolve_asset(env, a["asset"], type_=a.get("type"), workspace=a.get("workspace"))
    row, column = _as_column(env, found, a.get("column"))
    level = "column" if column else a.get("level", "table")
    direction = a.get("direction", "both")
    doc = svc.lineage(env.principal, row["id"], direction=direction, depth=cap(a.get("depth"), 2, 6), level=level, min_confidence=a.get("min_confidence"),
                      types=a.get("types") or None, column=column)
    root = doc.get("root") or row
    g = _graph_out(env, doc, root, max_nodes=cap(a.get("max_nodes"), 40, 200), max_edges=cap(a.get("max_edges"), 80, 400))
    label = g.pop("label")
    counts = _counts(doc, root["id"])
    subject = f"{label.get(root['id'], root.get('name'))}" + (f" (column {column})" if column and root.get("type") != "column" else "")
    out: dict[str, Any] = {"asset": ref(root), "column": column, "level": doc.get("level", level), "direction": direction, "depth": doc.get("depth"), "min_confidence": doc.get("min_confidence"),
                           "summary": (f"{subject}: {counts['upstream_nodes']} upstream and {counts['downstream_nodes']} downstream node(s) within {doc.get('depth')} hop(s); "
                                       f"links: {_fmt_counts(counts['edges_by_confidence'])}."), **counts,
                           "weakest_confidence": weakest(e.get("confidence") for e in doc.get("edges") or []), **g, **_gaps(doc)}
    out["complete"] = not out["truncated"] and out["gap_count"] == 0
    notes = []
    if out["truncated"]:
        notes.append("the graph was cut (node / edge limits); raise max_nodes / max_edges or reduce depth, or ask about one direction")
    if out["gap_count"]:
        notes.append("the lineage is incomplete: see gaps (each says how to fix it); links not seen are not proof that no link exists")
    if not doc.get("edges"):
        notes.append("no lineage was recorded for this asset at this level; try level='table', a larger depth, or intel_coverage_gaps")
    if notes:
        out["notes"] = notes
    return out


# ----------------------------------------------------------------------------------------------------------------------------------- intel_where_does_this_come_from
def intel_where_does_this_come_from(env: Env, a: dict[str, Any]) -> dict[str, Any]:
    """Upstream roots of an asset and every path from them (source first) with the confidence of each path (its weakest link)."""
    svc = _svc(env)
    found = resolve_asset(env, a["asset"], type_=a.get("type"), workspace=a.get("workspace"))
    row, column = _as_column(env, found, a.get("column"))
    doc = svc.origin(env.principal, row["id"], depth=cap(a.get("depth"), 10, 10), min_confidence=a.get("min_confidence"), level="column" if column else "table", column=column)
    subject = doc.get("asset") or row
    pool = {n["id"]: n for n in doc.get("nodes") or []}
    pool.update({r["asset"]["id"]: r["asset"] for r in doc.get("roots") or [] if r.get("asset")})
    pool[subject["id"]] = subject
    parents = _parent_labels(env, pool.values())
    names = {i: _label(n, parents) for i, n in pool.items()}
    max_paths = cap(a.get("max_paths"), 12, 50)
    paths_all = sorted(doc.get("paths") or [], key=lambda x: (_RANK.get(x.get("confidence"), 9), x.get("length") or 0))
    paths = [{"chain": [names.get(i, i) for i in pth.get("nodes") or []], "confidence": pth.get("confidence"), "hops": pth.get("length"),
              "through": [names.get(i, i) for i in pth.get("processes") or []]} for pth in paths_all[:max_paths]]
    edge_stats = {e.get("id"): e for e in doc.get("edges") or []}
    for src, pth in zip(paths_all[:max_paths], paths, strict=True):
        es = [edge_stats[i] for i in src.get("edges") or [] if i in edge_stats]
        if es:
            pth["evidence"] = {"links": len(es), "min_evidence_count": min((e.get("evidence_count") or 0) for e in es), "last_seen_at": max(str(e.get("last_seen_at") or "") for e in es) or None}
    roots = [{**ref(r["asset"], names.get(r["asset"]["id"])), "is_source": bool(r.get("is_source")), "paths": r.get("path_count"), "best_confidence": r.get("best_confidence")}
             for r in doc.get("roots") or []]
    sources = [r["name"] for r in roots if r["is_source"]]
    weak = weakest(p_.get("confidence") for p_ in paths_all)
    label = names.get(subject["id"], subject.get("name"))
    if doc.get("has_upstream"):
        summary = (f"{label} originates from {', '.join(r.get('label') or r['name'] for r in roots[:6]) or 'unknown roots'}" + (f" ({len(roots)} root(s) in all)" if len(roots) > 6 else "")
                   + f" through {len(paths_all)} path(s); the weakest link on any path is {weak or 'unknown'}.")
    else:
        summary = f"no upstream lineage is recorded for {label}" + (f" (column {column})" if column else "") + "; it may be a source itself or its producers have not been scanned."
    out: dict[str, Any] = {"asset": ref(subject, label), "column": column, "has_upstream": bool(doc.get("has_upstream")), "summary": summary, "origins": roots[:max_paths],
                           "source_systems": sources[:10], "paths": paths, "weakest_confidence": weak, "path_count": len(paths_all),
                           "truncated": bool(doc.get("truncated")) or len(paths_all) > len(paths) or len(roots) > max_paths,
                           "omitted": {"paths": max(0, len(paths_all) - len(paths)), "origins": max(0, len(roots) - max_paths)}, **_gaps(doc)}
    out["complete"] = not out["truncated"] and out["gap_count"] == 0
    if not out["complete"]:
        out["notes"] = ["gaps or limits make this a lower bound: there may be more origins than listed"]
    return out


# ----------------------------------------------------------------------------------------------------------------------------------- intel_impact_analysis
_REPORTISH = ("report", "dashboard", "visual", "semantic_model", "measure")


def _dependent_columns(env: Env, table: Mapping[str, Any], column: str, depth: int, min_confidence: str | None, limit: int) -> tuple[list[dict[str, Any]], bool]:
    """Columns that are computed from ``table.column`` (column-level lineage, nearest first) and whether the list was cut at ``limit``."""
    doc = _svc(env).lineage(env.principal, table["id"], direction="down", depth=depth, level="column", min_confidence=min_confidence, column=column)
    nodes = {n["id"]: n for n in doc.get("nodes") or [] if n.get("type") == "column" and n.get("direction") != "root"}
    parents = _parent_labels(env, nodes.values())
    into: dict[str, Mapping[str, Any]] = {}
    for e in doc.get("edges") or []:
        if e.get("to") in nodes and e.get("type") == "TRANSFORMS":
            into.setdefault(e["to"], e)
    rows = []
    for n in sorted(nodes.values(), key=lambda n: (n.get("level") if isinstance(n.get("level"), int) else 99, str(n.get("name")))):
        e = into.get(n["id"]) or {}
        row = {**ref(n, _label(n, parents)), "hops": n.get("level"), "confidence": n.get("confidence_floor")}
        if e.get("transformation"):
            row["transformation"] = clip(e["transformation"], 200)
        if e.get("via") and e["via"] in {x.get("id") for x in doc.get("nodes") or []}:
            via = next(x for x in doc["nodes"] if x.get("id") == e["via"])
            row["via"] = via.get("name")
        rows.append(row)
    return rows[:limit], len(rows) > limit


def intel_impact_analysis(env: Env, a: dict[str, Any]) -> dict[str, Any]:
    """Everything downstream of an asset (tables, models, measures, reports, visuals), nearest first, with owners when governance data exists.

    For a **column** the answer has two parts: ``dependent_columns`` (computed from this very column, from column-level lineage: precise) and ``consumers`` (tables,
    models and reports downstream of the column's table: whether they use this column is not known unless a dependent column leads to them).
    """
    svc = _svc(env)
    found = resolve_asset(env, a["asset"], type_=a.get("type"), workspace=a.get("workspace"))
    depth = cap(a.get("depth"), 5, 10)
    limit = cap(a.get("max_items"), 40, 200)
    deps: list[dict[str, Any]] = []
    deps_cut = False
    is_col = found["type"] == "column"
    if is_col:
        table, column = _as_column(env, found, None)
        deps, deps_cut = _dependent_columns(env, table, column or found["name"], depth, a.get("min_confidence"), limit)
    doc = svc.impact(env.principal, found["id"], depth=depth, min_confidence=a.get("min_confidence"))
    subject = doc.get("asset") or found
    rows = []
    for typ, group in (doc.get("by_type") or {}).items():
        for it in group.get("items") or []:
            rows.append((typ, it))
    parents = _parent_labels(env, [r[1] for r in rows])
    rows.sort(key=lambda r: (r[1].get("level") if isinstance(r[1].get("level"), int) else 99, str(r[0]), str(r[1].get("name"))))
    names: dict[str, str | None] = {}
    consumers = []
    for _typ, it in rows[:limit]:
        row = {**ref(it, _label(it, parents)), "hops": it.get("level"), "confidence": it.get("confidence_floor"), "workspace": _ws_name(env, names, it.get("workspace_id"))}
        if it.get("owners"):
            row["owners"] = [o.get("name") if isinstance(o, Mapping) else o for o in it["owners"]][:5]
        consumers.append(row)
    counts: dict[str, int] = {}
    for typ, _it in rows:
        counts[typ] = counts.get(typ, 0) + 1
    reports = [c for c in consumers if c["type"] in _REPORTISH]
    weak = weakest([*(it.get("confidence_floor") for _, it in rows), *(d["confidence"] for d in deps)])
    subj_label = _label(subject, _parent_labels(env, [subject]))
    n_reports = sum(1 for t, _ in rows if t in ("report", "dashboard"))
    parts = ", ".join(f"{n} {t}" for t, n in sorted(counts.items(), key=lambda kv: -kv[1])) or "none"
    if is_col:
        summary = (f"{subj_label} feeds {len(deps)} column(s) directly or indirectly; the table around it has {len(rows)} downstream asset(s) ({parts})"
                   + (f", {n_reports} of them report/dashboard(s)" if n_reports else "") + "; those reports and tables use this column only if a dependent column leads to them.")
    else:
        summary = f"changing {subj_label} can affect {len(rows)} downstream asset(s): {parts}" + (f"; {n_reports} report/dashboard(s) among them" if n_reports else "") + "."
    out: dict[str, Any] = {"asset": ref(subject, subj_label), "summary": summary, "total": len(rows), "counts_by_type": counts, "weakest_confidence": weak}
    if is_col:
        out["dependent_columns"] = deps
    out.update({"consumers": consumers, "reports_and_models": [{"name": c.get("label") or c["name"], "type": c["type"], "hops": c["hops"], "confidence": c["confidence"]} for c in reports][:25],
                "processes": [clip(x.get("name"), 80) for x in (doc.get("processes") or [])][:15], "owners_known": bool(doc.get("owners_known")),
                "truncated": bool(doc.get("truncated")) or len(rows) > limit or deps_cut, "omitted": {"consumers": max(0, len(rows) - limit)}, **_gaps(doc)})
    out["complete"] = not out["truncated"] and out["gap_count"] == 0
    notes = []
    if not rows and not deps:
        notes.append("no downstream consumer was found; absence of lineage is not proof that nothing reads this - check gaps and intel_coverage_gaps")
    if is_col:
        notes.append("column impact: dependent_columns is column-level lineage (precise); consumers are table-level (the column's table feeds them)")
    if out["gap_count"]:
        notes.append("dynamic or unreadable code (see gaps) can hide consumers; treat the list as a lower bound")
    if notes:
        out["notes"] = notes
    return out


# ----------------------------------------------------------------------------------------------------------------------------------- intel_staleness
def intel_staleness(env: Env, a: dict[str, Any]) -> dict[str, Any]:
    """Is the data fresh?  Last observed write, the producer chain examined, and the most upstream stale producer (the root cause)."""
    svc = _svc(env)
    found = resolve_asset(env, a["asset"], type_=a.get("type"), workspace=a.get("workspace"))
    doc = svc.staleness(env.principal, found["id"], depth=cap(a.get("depth"), 5, 10), stale_after_hours=a.get("stale_after_hours"))
    subject = doc.get("asset") or found
    nodes = list(doc.get("nodes") or [])
    parents = _parent_labels(env, [n.get("asset") or {} for n in nodes])
    limit = cap(a.get("max_items"), 25, 100)
    nodes.sort(key=lambda n: (not n.get("root_cause"), not n.get("stale"), str((n.get("asset") or {}).get("name"))))
    checked = []
    for n in nodes[:limit]:
        asset = n.get("asset") or {}
        run = n.get("last_run") or {}
        row: dict[str, Any] = {**ref(asset, _label(asset, parents)), "stale": bool(n.get("stale")), "root_cause": bool(n.get("root_cause")), "last_write_at": n.get("last_write_at"),
                               "age_hours": n.get("age_hours"), "evidence": n.get("last_write_source")}
        if run:
            row["last_run"] = {"state": run.get("state"), "observed_at": run.get("observed_at"), "by": run.get("producer")}
        if n.get("reasons"):
            row["reasons"] = [clip(r, 200) for r in n["reasons"][:4]]
        checked.append(row)
    causes = [c for c in checked if c["root_cause"]]
    out: dict[str, Any] = {"asset": ref(subject), "stale": bool(doc.get("stale")), "threshold_hours": doc.get("threshold_hours"), "last_write_at": doc.get("last_write_at"),
                           "summary": clip(doc.get("explanation") or "", 400), "root_causes": causes, "checked": checked,
                           "truncated": bool(doc.get("truncated")) or len(nodes) > limit, "omitted": {"checked": max(0, len(nodes) - limit)}, **_gaps(doc)}
    if doc.get("last_write_at") is None:
        out["notes"] = ["no write has been observed for this asset: freshness is unknown (not the same as stale); a scan of the producing job or a run would record it"]
    return out


# ----------------------------------------------------------------------------------------------------------------------------------- intel_documentation
def intel_documentation(env: Env, a: dict[str, Any]) -> dict[str, Any]:
    """Generated Markdown documentation of an asset (description, schema, governance facts, lineage summary, freshness, schema changes), cut to ``max_chars``."""
    svc = _svc(env)
    found = resolve_asset(env, a["asset"], type_=a.get("type"), workspace=a.get("workspace"))
    doc = svc.documentation(env.principal, found["id"])
    limit = cap(a.get("max_chars"), 8000, 30000)
    text = doc.get("markdown") or ""
    cut = len(text) > limit
    if cut:
        text = text[:limit].rsplit("\n", 1)[0] + "\n\n[... cut; raise max_chars or call intel_get_asset / intel_lineage for the parts you need]"
    return {"asset": ref(found), "format": "markdown", "markdown": text, "chars": len(doc.get("markdown") or ""), "truncated": cut, "generated_at": doc.get("generated_at")}


# ----------------------------------------------------------------------------------------------------------------------------------- intel_coverage_gaps
def intel_coverage_gaps(env: Env, a: dict[str, Any]) -> dict[str, Any]:
    """What discovery knows per source and what it could not read (permission denied, dynamic references, unavailable APIs ...), each with the fix."""
    svc, p = _svc(env), env.principal
    disc = svc.discovery
    limit = cap(a.get("limit"), 25, 100)
    asset_id = resolve_asset(env, a["asset"], workspace=a.get("workspace"))["id"] if a.get("asset") else None
    sources: list[dict[str, Any]] = []
    if disc is not None:
        cov = disc.coverage(p)
        for s in cov.get("sources") or []:
            if a.get("source") and a["source"].lower() not in (str(s.get("name")).lower(), str(s.get("source_id"))):
                continue
            sources.append({"name": s.get("name"), "kind": s.get("kind"), "enabled": s.get("enabled"), "active_assets": (s.get("assets") or {}).get("active", 0),
                            "assets_by_type": (s.get("assets") or {}).get("by_type") or {}, "open_gaps": (s.get("open_gaps") or {}).get("total", 0),
                            "gaps_by_kind": (s.get("open_gaps") or {}).get("by_kind") or {}, "last_scan": s.get("last_scan"), "last_status": s.get("last_status"),
                            "last_scan_at": s.get("last_incremental_at") or s.get("last_full_at"), "next_due_at": s.get("next_due_at"), "running": s.get("running")})
        src_id = next((s["source_id"] for s in cov.get("sources") or [] if a.get("source") and a["source"].lower() in (str(s.get("name")).lower(), str(s.get("source_id")))), None)
        rows, nxt = disc.list_gaps(p, source_id=src_id, kind=a.get("kind"), asset_id=asset_id, limit=limit)
    else:
        rows, nxt = svc.gaps(p, limit=limit), None
        if a.get("kind"):
            rows = [g for g in rows if g.get("kind") == a["kind"]]
        if asset_id:
            rows = [g for g in rows if g.get("asset_id") == asset_id]
    if a.get("source") and not sources and disc is not None:
        raise NotFound(f"no discovery source '{a['source']}' is visible to you", remediation="call intel_coverage_gaps without source to list them")
    by_kind: dict[str, int] = {}
    for g in rows:
        by_kind[g.get("kind")] = by_kind.get(g.get("kind"), 0) + 1
    names = {}
    if rows:
        arows = _svc(env).graph.asset_rows([g["asset_id"] for g in rows if g.get("asset_id")])
        names = {i: r["name"] for i, r in arows.items() if _svc(env).can_see(p, r["workspace_id"])}
    gaps = [{"kind": g.get("kind"), "subject": clip(g.get("subject"), 120), "asset": names.get(g.get("asset_id")), "asset_id": g.get("asset_id"), "detail": clip(g.get("detail"), 240),
             "fix": clip(g.get("remediation"), 240), "first_seen_at": g.get("first_seen_at"), "origin": g.get("origin")} for g in rows[:limit]]
    total = max(sum(s["open_gaps"] for s in sources), len(gaps))
    summary = (f"{len(sources)} source(s); " if sources else "") + f"{total} open gap(s)"
    return {"summary": summary + ("; lineage through the assets named in the gaps is incomplete." if gaps else "; nothing known is missing."), "sources": sources, "gaps": gaps, "listed_by_kind": by_kind,
            "truncated": bool(nxt) or len(rows) > limit, "omitted": {"gaps": max(0, len(rows) - limit)} if len(rows) > limit else {}}


# ----------------------------------------------------------------------------------------------------------------------------------- intel_summary
def intel_summary(env: Env, a: dict[str, Any]) -> dict[str, Any]:
    """Overview of the asset graph for what the caller may see: assets by type, links by confidence, placeholders, open gaps and the latest scan per source."""
    doc = _svc(env).summary(env.principal)
    edges = doc.get("edges") or {}
    conf = edges.get("by_confidence") or {}
    total = edges.get("total") or 0
    strong = (conf.get("verified") or 0) + (conf.get("derived") or 0)
    gaps = doc.get("gaps") or {}
    out = {"assets": {"total": (doc.get("assets") or {}).get("total", 0), "by_type": (doc.get("assets") or {}).get("by_type") or {}},
           "edges": {"total": total, "by_type": edges.get("by_type") or {}, "by_confidence": conf, "verified_or_derived_pct": round(100 * strong / total, 1) if total else None},
           "placeholders": doc.get("placeholders", 0), "gaps": {"open": gaps.get("open", 0), "by_kind": gaps.get("by_kind") or {}},
           "last_scans": [{k: s.get(k) for k in ("source_id", "status", "mode", "version", "finished_at")} for s in (doc.get("last_scans") or [])[:10]],
           "discovery_installed": bool(doc.get("discovery")), "governance_installed": bool(doc.get("governance")), "truncated": False}
    out["summary"] = (f"{out['assets']['total']} asset(s), {total} lineage link(s) ({out['edges']['verified_or_derived_pct']}% verified or derived), "
                      f"{out['placeholders']} unresolved placeholder(s), {out['gaps']['open']} open gap(s).")
    return out


# ----------------------------------------------------------------------------------------------------------------------------------- intel_unused_assets
def intel_unused_assets(env: Env, a: dict[str, Any]) -> dict[str, Any]:
    """Tables and views nothing was seen reading within a window (candidates to retire, not proof)."""
    svc = _svc(env)
    limit = cap(a.get("limit"), 25, 100)
    ws = env.workspace(a["workspace"], "intelligence.read")["id"] if a.get("workspace") else None
    doc = svc.unused(env.principal, days=a.get("days"), types=a.get("types") or None, workspace_id=ws, limit=limit)
    names: dict[str, str | None] = {}
    items = [{**ref(i), "workspace": _ws_name(env, names, i.get("workspace_id")), "created_at": i.get("first_seen_at"), "last_consumer_seen_at": i.get("last_consumer_seen_at"),
              "last_read_at": i.get("last_read_at"), "ever_read": bool(i.get("consumer_edges_ever"))} for i in doc.get("items") or []]
    _touch(env, *(doc.get("items") or []))
    return {"days": doc.get("days"), "count": len(items), "unused": items, "truncated": bool(doc.get("next_cursor")),
            "caveat": "unused means no reader was OBSERVED (in scanned code or at run time) within the window; external readers (BI tools, other platforms, ad-hoc SQL) are invisible. "
                      "Check intel_coverage_gaps before retiring anything."}


# ----------------------------------------------------------------------------------------------------------------------------------- registry
def build_intel_tools() -> list[Tool]:
    """The read-only Intelligence tools (listed only to callers with ``intelligence.read`` and only when the intelligence module is installed)."""
    need = ("intelligence.read",)
    asset = {"asset": ASSET_ARG, "type": TYPE_ARG, "workspace": WORKSPACE_ARG}
    limit = lambda default, top: {"type": "integer", "minimum": 1, "maximum": top, "default": default}  # noqa: E731
    tools = [
        Tool("intel_search_assets", "Search the asset graph",
             "Find data assets - tables, views, columns, notebooks, pipelines, semantic models, measures, reports, dashboards - by name, path, description, tag or alias, optionally "
             "filtered by governance facts (domain, classification, certification, owner). Start here when you only have a business word: 'Which tables hold customer data?', "
             "'Is there a Sales report?', 'What tables does the finance domain own?'. Returns compact references; pass an id (or the name) to the other intel_* tools.",
             obj({"query": {"type": "string", "minLength": 1, "maxLength": 200, "description": "Words that must all match the name, path, description or an alias"},
                  "type": {"type": "string", "maxLength": 200, "description": "Asset type(s), comma separated: table, view, column, notebook, pipeline, semantic_model, measure, report ..."},
                  "workspace": WORKSPACE_ARG, "domain": {"type": "string", "maxLength": 128}, "classification": {"type": "string", "maxLength": 128, "description": "e.g. PII"},
                  "certification": {"type": "string", "enum": ["certified", "draft", "deprecated", "none"]}, "owner": {"type": "string", "maxLength": 128}, "tag": {"type": "string", "maxLength": 64},
                  "status": {"type": "string", "enum": ["active", "removed", "placeholder", "any"], "default": "active"}, "limit": limit(15, 50)}), intel_search_assets, needs=need),
        Tool("intel_get_asset", "Describe an asset",
             "What is this asset? Type, path, workspace, description, tags, schema (column names and types, no data values), size and format, how many lineage links it has, open gaps and "
             "governance facts (owners, domain, certification, classifications) when available. Use it for 'What is sales_report and who owns it?' before tracing lineage.",
             obj({**asset, "max_columns": limit(40, 200)}, ["asset"]), intel_get_asset, needs=need),
        Tool("intel_lineage", "Trace lineage of an asset",
             "What feeds this asset and what reads it? Lineage graph around a table, report, model or single column, up and/or down, with the confidence of every link (verified = observed at run time by the platform, "
             "derived = parsed from code or read from a definition, inferred = matched by name, unknown), an evidence summary (count, last seen) and the gaps that make it incomplete. "
             "Questions: 'What feeds the Revenue table?', 'Which columns does clean.amount come from?'. For 'where does X originate?' prefer intel_where_does_this_come_from; for "
             "'what breaks?' prefer intel_impact_analysis.",
             obj({**asset, "direction": {"type": "string", "enum": ["up", "down", "both"], "default": "both", "description": "up = what feeds it, down = what reads it"},
                  "depth": {"type": "integer", "minimum": 1, "maximum": 6, "default": 2, "description": "Dataset hops"}, "level": {"type": "string", "enum": ["table", "column"], "default": "table"},
                  "column": {"type": "string", "maxLength": 256, "description": "Trace one column of the table (implies level=column)"}, "min_confidence": CONF_SCHEMA,
                  "types": {"type": "array", "items": {"type": "string"}, "maxItems": 12, "description": "Only these node types (others are bridged)"},
                  "max_nodes": limit(40, 200), "max_edges": limit(80, 400)}, ["asset"]), intel_lineage, needs=need),
        Tool("intel_where_does_this_come_from", "Where does this data originate?",
             "Trace a table, report, measure or column back to its upstream roots (source systems, raw tables) and list every path source-first with the confidence of each path (its weakest "
             "link). Questions: 'Where does Revenue originate?', 'Which source system feeds the Sales report?', 'Where does the amount column come from?'. Paths marked inferred are "
             "name matches, not proof; gaps list what could not be read.",
             obj({**asset, "column": {"type": "string", "maxLength": 256, "description": "Trace one column of the table"}, "depth": {"type": "integer", "minimum": 1, "maximum": 10, "default": 10},
                  "min_confidence": CONF_SCHEMA, "max_paths": limit(12, 50)}, ["asset"]), intel_where_does_this_come_from, needs=need),
        Tool("intel_impact_analysis", "What breaks if this changes?",
             "Everything downstream of an asset or a column - tables, semantic models, measures, reports, visuals - nearest first, with owners when governance data exists, so you can say who "
             "to warn. Questions: 'What breaks if I change this column?', 'Which reports depend on the orders table?', 'Can I drop this table?'. Lineage that was not observed is not "
             "listed: read the gaps and complete flag.",
             obj({**asset, "depth": {"type": "integer", "minimum": 1, "maximum": 10, "default": 5}, "min_confidence": CONF_SCHEMA, "max_items": limit(40, 200)}, ["asset"]),
             intel_impact_analysis, needs=need),
        Tool("intel_staleness", "Is this data fresh?",
             "Freshness of a table or report: last observed write and run outcome, whether it is past the threshold, and - walking upstream - the most upstream stale producer, which is "
             "the root cause. Questions: 'Why is this dashboard out of date?', 'Is the sales table fresh?', 'Which upstream job stopped?'. Unknown freshness is reported as unknown, never as stale.",
             obj({**asset, "depth": {"type": "integer", "minimum": 1, "maximum": 10, "default": 5}, "stale_after_hours": {"type": "number", "exclusiveMinimum": 0, "maximum": 87600},
                  "max_items": limit(25, 100)}, ["asset"]), intel_staleness, needs=need),
        Tool("intel_documentation", "Generated documentation of an asset",
             "Markdown documentation of an asset: description, tags, governance facts, schema table, properties, lineage summary, freshness, recent schema changes and open gaps. "
             "Questions: 'Explain the customers table', 'Write a data dictionary entry for sales'. Cut to max_chars (the truncated flag says so).",
             obj({**asset, "max_chars": {"type": "integer", "minimum": 500, "maximum": 30000, "default": 8000}}, ["asset"]), intel_documentation, needs=need),
        Tool("intel_coverage_gaps", "Coverage and gaps of the lineage",
             "How complete is the lineage? Per discovery source: assets known, last scan, open gaps by kind; plus the gaps themselves (permission denied, dynamic table names, unavailable APIs ...) "
             "each with how to fix it. Questions: 'What can't the lineage see?', 'Which sources failed to scan?', 'Why is the lineage of the etl notebook incomplete?'. Never reveals credentials.",
             obj({"source": {"type": "string", "maxLength": 128, "description": "Source name or id"}, "asset": {**ASSET_ARG, "description": "Only gaps about this asset"}, "workspace": WORKSPACE_ARG,
                  "kind": {"type": "string", "maxLength": 32, "description": "Gap kind, e.g. dynamic_reference, permission_denied"}, "limit": limit(25, 100)}), intel_coverage_gaps, needs=need),
        Tool("intel_summary", "Overview of the asset graph",
             "What is in the asset graph: assets by type, lineage links by confidence (how much was observed at run time or derived from code and definitions vs inferred), placeholders, open gaps and the latest scan per source, "
             "limited to what you may see. Questions: 'How much of our estate is mapped?', 'How reliable is the lineage?'. A good first call.", obj({}), intel_summary, needs=need),
        Tool("intel_unused_assets", "Unused tables and views",
             "Tables and views no reader was observed for within a window (default 30 days): candidates to retire. Questions: 'Which tables can we retire?', 'What is nobody reading?'. "
             "Observed means seen in scanned code or at run time; readers outside OLCF are invisible, so check intel_coverage_gaps first.",
             obj({"days": {"type": "integer", "minimum": 0, "maximum": 3650}, "types": {"type": "array", "items": {"type": "string"}, "maxItems": 6, "description": "Default table and view"},
                  "workspace": WORKSPACE_ARG, "limit": limit(25, 100)}), intel_unused_assets, needs=need),
    ]
    for t in tools:
        t.service = "intelligence"
    return tools
