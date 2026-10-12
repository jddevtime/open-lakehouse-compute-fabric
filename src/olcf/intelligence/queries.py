# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Read-side analysis over the asset graph: lineage (table and column level), origin, impact, staleness / root cause, unused tables, generated documentation
and the summary. Every function takes the calling principal and applies the visibility rules of contract section 8 (an asset outside the caller's scope does not
exist: ``NotFound``, hidden nodes are skipped as if they were not there).

Response shape of a lineage-like answer::

    {"root": node, "nodes": [{id, type, name, path, workspace_id, status, placeholder, confidence_floor, level, direction, ...}],
     "edges": [{id, src, dst, type, from, to, confidence, via, transformation, evidence_count, ...}], "truncated": bool, "gaps": [...]}

``src``/``dst`` are the edge as stored (``notebook WRITES_TO table``); ``from``/``to`` are the direction the *data* flows.
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from sqlalchemy import exists, func, or_, select

from olcf.core.errors import BadRequest, NotFound
from olcf.core.timeutil import from_iso, now_iso, to_iso, utcnow
from olcf.db import schema as S
from olcf.db.util import fetch_all, paginate
from olcf.intelligence import model as M
from olcf.intelligence.graph import TABLE_LEVEL_EDGES, Walk, weakest
from olcf.intelligence.schema import asset_versions, assets, edges, gaps, scans
from olcf.security.rbac import Principal

if TYPE_CHECKING:
    from olcf.intelligence.service import IntelligenceService

MAX_PATHS = 100
#: Column-level lineage follows the column-to-column ``TRANSFORMS`` edges and the field-level edges that continue them: a measure that uses another measure
#: (``USES_MEASURE``) and a visual that shows a column or measure (``DISPLAYS``). A measure or a visual is therefore a valid root of a column-level query too.
COLUMN_LEVEL_EDGES = ("TRANSFORMS", "USES_MEASURE", "DISPLAYS")
COLUMN_LEVEL_ROOTS = ("column", "measure", "visual")
MAX_COLUMN_ROOTS = 300
MAX_GAPS = 50
_FAILED = {"failed", "timed_out", "cancelled", "error", "dead"}
_OK = {"succeeded", "success", "completed", "ok"}


def _node_out(row: dict[str, Any], *, level: int | None = None, direction: str | None = None, floor: str | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {
        "id": row["id"], "type": row["type"], "name": row["name"], "path": row["path"], "workspace_id": row["workspace_id"], "status": row["status"],
        "placeholder": row["status"] == "placeholder", "parent_id": row["parent_id"], "description": row["description"] or "",
    }
    if level is not None:
        out["level"] = level
    if direction is not None:
        out["direction"] = direction
    if floor is not None:
        out["confidence_floor"] = floor
    return out


def _edge_out(r: dict[str, Any]) -> dict[str, Any]:
    f = M.flow(r["type"], r["src_id"], r["dst_id"]) or (r["src_id"], r["dst_id"])
    out = {"id": r["id"], "type": r["type"], "src": r["src_id"], "dst": r["dst_id"], "from": f[0], "to": f[1], "confidence": r["confidence"], "via": r["via_id"],
            "transformation": r["transformation"], "evidence_count": len(r["evidence"] or []), "observation_count": r["observation_count"], "status": r["status"],
            "last_seen_at": r["last_seen_at"], "last_verified_at": r["last_verified_at"], "virtual": False}
    if (r.get("attrs") or {}).get("stale"):
        out["stale"] = True                      # last-known-good: the latest analysis of its definition was incomplete (see Engine.analyze_and_apply)
    return out


class Queries:
    """The analysis queries of :class:`~olcf.intelligence.service.IntelligenceService` (kept apart so the service stays a thin, readable facade)."""

    def __init__(self, svc: IntelligenceService) -> None:
        self.svc = svc

    # ------------------------------------------------------------------------------------------------------------------------ lineage
    def lineage(self, principal: Principal, asset_id: str, *, direction: str = "both", depth: int = 3, level: str = "table", min_confidence: str | None = None,
                types: list[str] | None = None, column: str | None = None, include_removed: bool = False) -> dict[str, Any]:
        """Lineage around an asset: table level through process nodes (notebook, pipeline ...) or column level over ``TRANSFORMS`` edges (``via`` = the process).
        Cycle-safe; ``depth`` is 1..10; the answer is cut at ``intelligence.max_lineage_nodes`` nodes and says so in ``truncated``."""
        svc = self.svc
        root = svc.row_for(principal, asset_id)
        if direction not in ("up", "down", "both"):
            raise BadRequest("direction must be up, down or both")
        if level not in ("table", "column"):
            raise BadRequest("level must be table or column")
        if not 1 <= int(depth) <= 10:
            raise BadRequest("depth must be between 1 and 10")
        if types:
            bad = [t for t in types if t not in M.ASSET_TYPES]
            if bad:
                raise BadRequest(f"unknown asset type(s) {bad}", details={"supported": list(M.ASSET_TYPES)})
        if column:
            level = "column"
        starts, edge_types = self._starts(principal, root, level, column)
        keep = svc.keep_node(principal)
        cap = svc.max_nodes()
        start_ids = {s["id"] for s in starts}
        walks = self._walks(starts, direction, depth, edge_types, min_confidence, include_removed, cap, keep)
        nodes, edge_out, truncated = self._merge(walks, start_ids, keep)
        nodes, edge_out, cut = self._cap_datasets(nodes, edge_out, start_ids, cap)
        truncated = truncated or cut
        if types:
            nodes, edge_out = _filter_types(nodes, edge_out, set(types), start_ids)
        contracted = 0
        if len(nodes) > cap:
            truncated = True
            nodes, edge_out, contracted = _cap_processes(nodes, edge_out, start_ids, cap)
        gap_rows = self._gaps_for(list(nodes))
        root_node = nodes.get(root["id"]) or _node_out(root, direction="root", floor="verified")
        res: dict[str, Any] = {"root": root_node, "direction": direction, "level": level, "depth": int(depth), "min_confidence": min_confidence,
                               "nodes": sorted(nodes.values(), key=lambda n: (n.get("level", 99), n["name"])), "edges": edge_out, "truncated": truncated, "gaps": gap_rows,
                               "stats": {"nodes": len(nodes), "edges": len(edge_out), "max_nodes": cap, **({"contracted_processes": contracted} if contracted else {})}}
        if level == "column":
            res["roots"] = [s["id"] for s in starts]
        self._decorate_nodes(principal, res["nodes"])
        return res

    def _walks(self, starts: list[dict[str, Any]], direction: str, depth: int, edge_types: tuple[str, ...], min_confidence: str | None, include_removed: bool, cap: int,
               keep: Any) -> dict[str, Walk]:
        """The ``up`` and / or ``down`` traversals of a lineage answer. The cap counts dataset-like nodes (the process nodes in between have a separate, wider
        allowance), and a ``both`` answer shares it between the two directions."""
        graph = self.svc.graph
        ids = [s["id"] for s in starts]
        walks: dict[str, Walk] = {}
        proc_cap = max(cap * 4, 2000)
        if direction in ("up", "both"):
            walks["up"] = graph.walk(ids, direction="up", depth=depth, edge_types=edge_types, min_confidence=min_confidence, include_removed=include_removed,
                                     max_nodes=cap, keep_node=keep, process_cap=proc_cap)
        if direction in ("down", "both"):
            used = sum(1 for n in walks["up"].nodes.values() if n["row"]["type"] not in M.PROCESS_TYPES) if "up" in walks else 0
            walks["down"] = graph.walk(ids, direction="down", depth=depth, edge_types=edge_types, min_confidence=min_confidence, include_removed=include_removed,
                                       max_nodes=max(len(starts) + 1, cap - max(0, used - len(starts))), keep_node=keep, process_cap=proc_cap)
        return walks

    def _merge(self, walks: dict[str, Walk], start_ids: set[str], keep: Any) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]], bool]:
        """Combine the traversals into one node map and edge list; the process named in a ``TRANSFORMS`` edge's ``via`` belongs in the picture."""
        nodes: dict[str, dict[str, Any]] = {}
        edge_rows: dict[str, dict[str, Any]] = {}
        truncated = False
        for d, w in walks.items():
            truncated = truncated or w.truncated
            edge_rows.update(w.edges)
            for nid, info in w.nodes.items():
                label = "root" if nid in start_ids else ("upstream" if d == "up" else "downstream")
                cur = nodes.get(nid)
                if cur is None:
                    nodes[nid] = _node_out(info["row"], level=info["level"], direction=label, floor=info["floor"])
                elif cur["direction"] != label and label != "root" and cur["direction"] != "root":
                    cur["direction"] = "both"
                    cur["confidence_floor"] = M.stronger(cur["confidence_floor"], info["floor"])
        edge_out = [_edge_out(r) for r in edge_rows.values()]
        via_ids = {e["via"] for e in edge_out if e["via"] and e["via"] not in nodes}
        if via_ids:
            for vid, vrow in self.svc.graph.asset_rows(via_ids).items():
                if keep(vrow):
                    n = _node_out(vrow, direction="via")
                    n["via_only"] = True
                    nodes[vid] = n
        return nodes, edge_out, truncated

    @staticmethod
    def _cap_datasets(nodes: dict[str, dict[str, Any]], edge_out: list[dict[str, Any]], start_ids: set[str], cap: int) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]], bool]:
        """Keep at most ``cap`` dataset-like nodes (the nearest first); edges to a dropped node go with it, and so do process nodes left without any edge."""
        datasets = [n for n in nodes.values() if n["type"] not in M.PROCESS_TYPES and n["id"] not in start_ids]
        allowed = max(0, cap - len(start_ids))
        if len(datasets) <= allowed:
            return nodes, edge_out, False
        datasets.sort(key=lambda n: (n.get("level", 99), n["name"], n["id"]))
        dropped = {n["id"] for n in datasets[allowed:]}
        edge_out = [e for e in edge_out if e["from"] not in dropped and e["to"] not in dropped]
        used = {x for e in edge_out for x in (e["from"], e["to"], e.get("via")) if x}
        nodes = {k: v for k, v in nodes.items() if k not in dropped and (k in start_ids or v["type"] not in M.PROCESS_TYPES or k in used)}
        return nodes, edge_out, True

    def _starts(self, principal: Principal, root: dict[str, Any], level: str, column: str | None) -> tuple[list[dict[str, Any]], tuple[str, ...]]:
        svc = self.svc
        if level == "table":
            if root["type"] == "column" and root["parent_id"]:
                parent = svc.graph.asset_rows([root["parent_id"]]).get(root["parent_id"])
                if parent is not None and svc.can_see(principal, parent["workspace_id"]):
                    return [parent], TABLE_LEVEL_EDGES
            return [root], TABLE_LEVEL_EDGES
        if root["type"] in COLUMN_LEVEL_ROOTS:
            return [root], COLUMN_LEVEL_EDGES
        with svc.db.connect() as c:
            stmt = select(assets).where(assets.c.parent_id == root["id"], assets.c.type == "column", assets.c.status != "removed")
            if column:
                stmt = stmt.where(func.lower(assets.c.name) == column.lower())
            cols = fetch_all(c, stmt.order_by(assets.c.name).limit(MAX_COLUMN_ROOTS))
        if column and not cols:
            raise NotFound(f"column '{column}' is not known for asset '{root['name']}'", remediation="column lineage needs a scan of the table's schema first")
        return cols, COLUMN_LEVEL_EDGES

    def _gaps_for(self, ids: list[str]) -> list[dict[str, Any]]:
        if not ids:
            return []
        out: list[dict[str, Any]] = []
        with self.svc.db.connect() as c:
            for i in range(0, len(ids), 300):
                out.extend(fetch_all(c, select(gaps).where(gaps.c.asset_id.in_(ids[i:i + 300]), gaps.c.resolved_at.is_(None)).order_by(gaps.c.id).limit(MAX_GAPS)))
                if len(out) >= MAX_GAPS:
                    break
        return [_gap_out(g) for g in out[:MAX_GAPS]]

    def _decorate_nodes(self, principal: Principal, nodes: list[dict[str, Any]]) -> None:
        info = self.svc.governance_info(principal, [n["id"] for n in nodes])
        if info:
            for n in nodes:
                if n["id"] in info:
                    n["governance"] = info[n["id"]]

    # ------------------------------------------------------------------------------------------------------------------------ origin
    def origin(self, principal: Principal, asset_id: str, *, depth: int = 10, min_confidence: str | None = None, level: str = "table", column: str | None = None) -> dict[str, Any]:
        """Where does this come from: the upstream walk to its roots, with every path (source first) and the confidence of each path (its weakest edge).

        Table-level lineage starts at a column's table and so has no path *of the column*: when the asset is a column the question is answered at column level
        and ``level_switched`` says so (``{"from": "table", "to": "column", "reason": ...}``)."""
        switched: dict[str, str] | None = None
        if level == "table" and not column:
            root_type = self.svc.row_for(principal, asset_id)["type"]
            if root_type == "column":
                level = "column"
                switched = {"from": "table", "to": "column", "reason": "the asset is a column; its origin is traced over the column-level (TRANSFORMS) edges"}
        lin = self.lineage(principal, asset_id, direction="up", depth=depth, level=level, min_confidence=min_confidence, column=column)
        nodes = {n["id"]: n for n in lin["nodes"]}
        into: dict[str, list[dict[str, Any]]] = defaultdict(list)   # node -> edges flowing into it
        for e in lin["edges"]:
            into[e["to"]].append(e)
        starts = lin.get("roots") or [lin["root"]["id"]]
        paths: list[dict[str, Any]] = []
        truncated_paths = False

        def dfs(node: str, trail_nodes: list[str], trail_edges: list[dict[str, Any]], on_path: set[str]) -> None:
            nonlocal truncated_paths
            if len(paths) >= MAX_PATHS:
                truncated_paths = True
                return
            preds = [e for e in into.get(node, []) if e["from"] not in on_path and e["from"] in nodes]
            if not preds:
                if trail_edges:
                    ns = list(reversed(trail_nodes))
                    es = list(reversed(trail_edges))
                    conf = "verified"
                    for e in es:
                        conf = weakest(conf, e["confidence"])
                    paths.append({"nodes": ns, "edges": [e["id"] for e in es], "confidence": conf,
                                  "length": sum(1 for n in ns if nodes[n]["type"] not in M.PROCESS_TYPES) - 1,
                                  "processes": [n for n in ns if nodes[n]["type"] in M.PROCESS_TYPES], "source": ns[0]})
                return
            for e in preds:
                dfs(e["from"], [*trail_nodes, e["from"]], [*trail_edges, e], {*on_path, e["from"]})

        for s in starts:
            dfs(s, [s], [], {s})
        by_root: dict[str, dict[str, Any]] = {}
        for p in paths:
            r = by_root.setdefault(p["source"], {"asset": nodes[p["source"]], "path_count": 0, "best_confidence": "unknown",
                                                 "is_source": nodes[p["source"]]["type"] in ("source_system", "external", "file")})
            r["path_count"] += 1
            r["best_confidence"] = M.stronger(r["best_confidence"], p["confidence"]) if r["path_count"] > 1 else p["confidence"]
        return {"asset": lin["root"], "roots": sorted(by_root.values(), key=lambda r: r["asset"]["name"]), "paths": paths, "nodes": lin["nodes"], "edges": lin["edges"],
                "truncated": lin["truncated"] or truncated_paths, "gaps": lin["gaps"], "has_upstream": bool(paths), "level": lin["level"],
                "level_switched": switched}

    # ------------------------------------------------------------------------------------------------------------------------ impact
    def impact(self, principal: Principal, asset_id: str, *, depth: int = 5, min_confidence: str | None = None) -> dict[str, Any]:
        """Everything downstream that depends on the asset, grouped by type; reports, visuals and dashboards are listed separately and carry their owners
        when governance data exists.

        The counts come from a traversal of their own, bounded by ``intelligence.max_impact_nodes`` and unaffected by the render cap: ``total`` and the
        ``count`` of each ``by_type`` group are real numbers, and ``truncated`` is set only when that bound cut the traversal short. The lists (``items``,
        ``nodes``, ``edges``) stay within ``intelligence.max_lineage_nodes`` (nearest first); ``items_truncated`` says they hold fewer entries than the counts."""
        svc = self.svc
        root = svc.row_for(principal, asset_id)
        starts, edge_types = self._starts(principal, root, "table", None)
        start_ids = {s["id"] for s in starts}
        keep = svc.keep_node(principal)
        cap = svc.max_nodes()
        limit = svc.max_impact_nodes()
        walk = svc.graph.walk(list(start_ids), direction="down", depth=depth, edge_types=edge_types, min_confidence=min_confidence, max_nodes=limit, keep_node=keep,
                              process_cap=limit)
        counts: dict[str, int] = defaultdict(int)
        for nid, info in sorted(walk.nodes.items(), key=lambda kv: (kv[1]["level"], kv[1]["row"]["name"], kv[0])):         # nearest types first
            if nid not in start_ids and info["row"]["type"] not in M.PROCESS_TYPES:
                counts[info["row"]["type"]] += 1
        total = sum(counts.values())
        nodes, edge_out, _ = self._merge({"down": walk}, start_ids, keep)
        nodes, edge_out, _ = self._cap_datasets(nodes, edge_out, start_ids, cap)
        if len(nodes) > cap:
            nodes, edge_out, _ = _cap_processes(nodes, edge_out, start_ids, cap)
        root_node = nodes.get(root["id"]) or _node_out(root, direction="root", floor="verified")
        # the walk starts at the asset itself, or at its table for a column: whatever it starts from is where the data lives, never something that depends on it
        consumers = [n for n in nodes.values() if n["id"] not in start_ids and n["direction"] != "root" and n["type"] not in M.PROCESS_TYPES and not n.get("via_only")]
        consumers.sort(key=lambda n: (n.get("level", 99), n["name"], n["id"]))
        procs = [n for n in nodes.values() if n["type"] in M.PROCESS_TYPES]
        owners = svc.owners_of(principal, [n["id"] for n in consumers])
        for n in consumers:
            if n["id"] in owners:
                n["owners"] = owners[n["id"]]
        by_type: dict[str, dict[str, Any]] = {t: {"count": c, "items": []} for t, c in counts.items()}
        for n in consumers:
            by_type[n["type"]]["items"].append(n)
        ordered = sorted(nodes.values(), key=lambda n: (n.get("level", 99), n["name"]))
        gap_rows = self._gaps_for(list(nodes))
        self._decorate_nodes(principal, ordered)
        return {"asset": root_node, "total": total, "by_type": by_type, "reports": [n for n in consumers if n["type"] in M.CONSUMER_TYPES],
                "processes": procs, "nodes": ordered, "edges": edge_out, "truncated": walk.truncated, "items_truncated": len(consumers) < total, "gaps": gap_rows,
                "owners_known": bool(owners), "stats": {"nodes": len(nodes), "edges": len(edge_out), "max_nodes": cap, "max_impact_nodes": limit, "listed": len(consumers)}}

    # ------------------------------------------------------------------------------------------------------------------------ staleness
    def staleness(self, principal: Principal, asset_id: str, *, depth: int = 5, stale_after_hours: float | None = None) -> dict[str, Any]:
        """Is the data fresh, and if not, which upstream producer is the root cause?

        For the asset and every dataset upstream of it: the last write observed at run time (``WRITES_TO`` edges verified by ``dataset.written`` events), the data's own
        last-modified time (Delta log, when a scan recorded it) and the outcome of the run that wrote last (job / pipeline events). A dataset is *stale* when its last
        run failed after its last good write, when nothing was ever observed writing it although a producer exists, or when its last write is older than the threshold.
        ``root_causes`` are the stale datasets that have no stale dataset upstream of them.
        """
        svc = self.svc
        hours = float(stale_after_hours if stale_after_hours is not None else svc.setting("intelligence.stale_after_hours", 24))
        lin = self.lineage(principal, asset_id, direction="up", depth=depth, level="table")
        root_id = lin["root"]["id"]
        datasets = [n for n in lin["nodes"] if n["type"] not in M.PROCESS_TYPES and not n.get("via_only")]
        ids = [n["id"] for n in datasets]
        writers: dict[str, list[dict[str, Any]]] = defaultdict(list)
        if ids:
            with svc.db.connect() as c:
                for i in range(0, len(ids), 300):
                    for r in fetch_all(c, select(edges).where(edges.c.type == "WRITES_TO", edges.c.dst_id.in_(ids[i:i + 300]), edges.c.status == "active")):
                        writers[r["dst_id"]].append(r)
        rows = svc.graph.asset_rows(ids)
        runs = self._run_states({e.get("ref") for rs in writers.values() for r in rs for e in (r["evidence"] or []) if e.get("kind") == "runtime" and e.get("ref")})
        now = utcnow()
        report: dict[str, dict[str, Any]] = {}
        for n in datasets:
            ws = writers.get(n["id"], [])
            runtime_ev = sorted((e for r in ws for e in (r["evidence"] or []) if e.get("kind") == "runtime"), key=lambda e: e.get("observed_at") or "", reverse=True)
            last_runtime = max((r["last_verified_at"] for r in ws if r["last_verified_at"]), default=None)
            modified = (rows.get(n["id"], {}).get("attrs") or {}).get("last_modified")
            candidates = [(t, src) for t, src in ((last_runtime, "observed write"), (modified, "table modified time")) if t]
            last_at, last_src = max(candidates, key=lambda x: str(x[0])) if candidates else (None, None)
            last_run = None
            if runtime_ev and runtime_ev[0].get("ref"):
                st = runs.get(runtime_ev[0]["ref"])
                last_run = {"ref": runtime_ev[0]["ref"], "observed_at": runtime_ev[0].get("observed_at"), "state": (st or {}).get("state"), "state_at": (st or {}).get("at"),
                            "producer": runtime_ev[0].get("source")}
            reasons: list[str] = []
            if last_run and last_run["state"] in _FAILED and (not last_at or (last_run["state_at"] or "") > str(last_at)):
                reasons.append(f"last run {last_run['ref']} ended {last_run['state']}")
            if ws and not last_at:
                reasons.append("a producer exists but no write was ever observed")
            age = (now - from_iso(str(last_at))).total_seconds() / 3600 if last_at else None
            if age is not None and age > hours:
                reasons.append(f"last write {age:.1f} h ago, threshold {hours:g} h")
            report[n["id"]] = {"asset": n, "is_source": not ws, "producers": sorted({r["src_id"] for r in ws}), "last_write_at": last_at, "last_write_source": last_src,
                               "age_hours": None if age is None else round(age, 2), "last_run": last_run, "stale": bool(reasons), "reasons": reasons, "root_cause": False}
        upstream: dict[str, set[str]] = defaultdict(set)     # dataset -> datasets feeding it (through processes)
        into: dict[str, list[str]] = defaultdict(list)
        for e in lin["edges"]:
            into[e["to"]].append(e["from"])
        typ = {n["id"]: n["type"] for n in lin["nodes"]}

        def feeders(node: str) -> set[str]:
            seen: set[str] = set()
            stack = list(into.get(node, []))
            out: set[str] = set()
            while stack:
                x = stack.pop()
                if x in seen:
                    continue
                seen.add(x)
                if typ.get(x) in M.PROCESS_TYPES:
                    stack.extend(into.get(x, []))
                else:
                    out.add(x)
            return out

        for nid in report:
            upstream[nid] = feeders(nid)
        for nid, r in report.items():
            if r["stale"] and not any(report.get(u, {}).get("stale") for u in _closure(nid, upstream)):
                r["root_cause"] = True
        roots = [r for r in report.values() if r["root_cause"]]
        me = report.get(root_id)
        if me and me["stale"]:
            culprit = [r for r in roots if r["asset"]["id"] != root_id]
            explanation = (f"'{me['asset']['name']}' is stale: {'; '.join(me['reasons'])}." + (f" The most upstream stale producer is '{culprit[0]['asset']['name']}' ({'; '.join(culprit[0]['reasons'])})." if culprit else ""))
        elif me:
            explanation = f"'{me['asset']['name']}' looks fresh" + (f" (last write {me['age_hours']} h ago)." if me["age_hours"] is not None else "; no write time is known.") + (
                f" {sum(1 for r in report.values() if r['stale'])} upstream dataset(s) are stale." if any(r["stale"] for r in report.values()) else "")
        else:
            explanation = "no information"
        return {"asset": lin["root"], "threshold_hours": hours, "stale": bool(me and me["stale"]), "last_write_at": me["last_write_at"] if me else None,
                "nodes": sorted(report.values(), key=lambda r: (r["asset"].get("level", 0), r["asset"]["name"])), "root_causes": [r["asset"]["id"] for r in roots],
                "explanation": explanation, "truncated": lin["truncated"], "gaps": lin["gaps"]}

    def _run_states(self, refs: set[Any]) -> dict[str, dict[str, Any]]:
        """Latest job / pipeline-run outcome per run id from the platform event store (no import of the compute or pipeline modules)."""
        ids = sorted(str(r) for r in refs if r)
        out: dict[str, dict[str, Any]] = {}
        if not ids:
            return out
        t = S.events.c
        with self.svc.db.connect() as c:
            for i in range(0, len(ids), 300):
                for r in fetch_all(c, select(S.events).where(t.subject.in_(ids[i:i + 300]), or_(t.type == "job.state_changed", t.type.like("pipeline.run.%"))).order_by(t.seq)):
                    if r["type"] == "job.state_changed":
                        state = (r["payload"] or {}).get("to")
                    else:
                        state = r["type"].rsplit(".", 1)[-1]
                    if state:
                        out[r["subject"]] = {"state": state, "at": r["ts"], "event_type": r["type"]}
        return out

    # ------------------------------------------------------------------------------------------------------------------------ unused
    def unused(self, principal: Principal, *, days: int | None = None, types: list[str] | None = None, workspace_id: str | None = None, limit: int = 50,
               cursor: str | None = None) -> dict[str, Any]:
        """Tables / views with no downstream consumer observed within ``days`` days: no active ``READS_FROM`` / ``DEPENDS_ON`` / ``USES_MEASURE`` / ``DISPLAYS``
        edge into them was seen (by a scan of the code that reads it, or at run time) in that window, and the asset itself is older than the window."""
        svc = self.svc
        n = int(days if days is not None else svc.setting("intelligence.default_unused_days", 30))
        if n < 0:
            raise BadRequest("days must not be negative")
        kinds = types or ["table", "view"]
        bad = [k for k in kinds if k not in M.ASSET_TYPES]
        if bad:
            raise BadRequest(f"unknown asset type(s) {bad}")
        cutoff = to_iso(utcnow() - timedelta(days=n))
        t, e = assets.c, edges.c
        consumer = exists().where(e.dst_id == t.id, e.type.in_(["READS_FROM", "DEPENDS_ON", "USES_MEASURE", "DISPLAYS"]), e.status == "active", e.last_seen_at >= cutoff)
        stmt = select(assets).where(t.type.in_(kinds), t.status == "active", ~consumer, t.created_at <= cutoff)
        stmt = svc.scope_filter(stmt, principal, workspace_id)
        with svc.db.connect() as c:
            rows, nxt = paginate(c, stmt, assets, limit=limit, cursor=cursor)
            last: dict[str, Any] = {}
            ids = [r["id"] for r in rows]
            if ids:
                for r in c.execute(select(e.dst_id, func.max(e.last_seen_at), func.max(e.last_verified_at), func.count()).where(e.dst_id.in_(ids), e.type == "READS_FROM").group_by(e.dst_id)):
                    last[r[0]] = {"last_consumer_seen_at": r[1], "last_read_at": r[2], "consumer_edges_ever": r[3]}
        items = [{**_node_out(r), **last.get(r["id"], {"last_consumer_seen_at": None, "last_read_at": None, "consumer_edges_ever": 0}), "first_seen_at": r["created_at"]} for r in rows]
        return {"days": n, "items": items, "next_cursor": nxt}

    # ------------------------------------------------------------------------------------------------------------------------ documentation
    def documentation(self, principal: Principal, asset_id: str) -> dict[str, Any]:
        """Generated documentation of an asset as Markdown: description, schema, owners / domain / glossary (when governance data exists), lineage summary, freshness,
        recent schema changes and open gaps."""
        svc = self.svc
        row = svc.row_for(principal, asset_id)
        attrs = row["attrs"] or {}
        gov = svc.governance_info(principal, [row["id"]]).get(row["id"], {})
        lines = [f"# {row['name']}", "", f"*{row['type']}*" + (f" in `{row['path']}`" if row["path"] else "") + (f", status **{row['status']}**" if row["status"] != "active" else ""), ""]
        if row["description"]:
            lines += [row["description"], ""]
        if row["tags"] or attrs.get("source_tags"):
            lines += ["**Tags:** " + ", ".join(sorted({*row["tags"], *(attrs.get("source_tags") or [])})), ""]
        if gov:
            lines += ["## Governance", ""]
            for key in ("domain", "certification", "classifications", "owners", "glossary_terms", "data_product"):
                v = gov.get(key)
                if v:
                    shown = ", ".join(_label(x) for x in v) if isinstance(v, list) else _label(v)
                    lines.append(f"- **{key.replace('_', ' ').capitalize()}:** {shown}")
            lines.append("")
        cols = self._columns(row)
        if cols:
            lines += ["## Schema", "", "| Column | Type | Description |", "|---|---|---|"]
            for c in cols:
                lines.append(f"| `{c['name']}` | {c.get('type') or ''} | {(c.get('description') or '').replace('|', '/')} |")
            lines.append("")
        facts = [(k, attrs[k]) for k in ("format", "location", "row_count", "size_bytes", "last_modified", "partition_columns") if attrs.get(k) not in (None, "", [])]
        if facts:
            lines += ["## Properties", ""] + [f"- **{k.replace('_', ' ')}:** {v}" for k, v in facts] + [""]
        try:
            up = self.lineage(principal, asset_id, direction="up", depth=1)
            down = self.lineage(principal, asset_id, direction="down", depth=1)
            lines += ["## Lineage", ""]
            for title, lin in (("Upstream", up), ("Downstream", down)):
                items = [n for n in lin["nodes"] if n["id"] != lin["root"]["id"] and not n.get("via_only")]
                lines.append(f"**{title}** ({len([n for n in items if n['type'] not in M.PROCESS_TYPES])} datasets):")
                lines += [f"- {n['name']} ({n['type']}, confidence {n.get('confidence_floor', 'unknown')})" for n in items] or ["- none known"]
                lines.append("")
            if up["gaps"] or down["gaps"]:
                lines += ["> Lineage is incomplete: " + "; ".join(sorted({g["detail"] for g in up["gaps"] + down["gaps"]}))[:600], ""]
        except NotFound:
            pass
        fresh = self.staleness(principal, asset_id, depth=1)
        me = next((r for r in fresh["nodes"] if r["asset"]["id"] == row["id"]), None)
        if me and me["last_write_at"]:
            lines += ["## Freshness", "", f"Last write: {me['last_write_at']} ({me['last_write_source']})" + (" - **stale**: " + "; ".join(me["reasons"]) if me["stale"] else ""), ""]
        with svc.db.connect() as c:
            vers = fetch_all(c, select(asset_versions).where(asset_versions.c.asset_id == row["id"]).order_by(asset_versions.c.id.desc()).limit(10))
        schema_changes = [v for v in vers if any(k.startswith("columns_") for k in (v["changes"] or {}))]
        if schema_changes:
            lines += ["## Recent schema changes", ""]
            for v in schema_changes[:5]:
                ch = v["changes"]
                bits = [f"{k.replace('columns_', '')}: {', '.join(x if isinstance(x, str) else x.get('column', '?') for x in ch[k])}" for k in ("columns_added", "columns_removed", "columns_changed") if ch.get(k)]
                lines.append(f"- {v['created_at']}: " + "; ".join(bits))
            lines.append("")
        return {"asset_id": row["id"], "name": row["name"], "markdown": "\n".join(lines).rstrip() + "\n", "generated_at": now_iso()}

    def _columns(self, row: dict[str, Any]) -> list[dict[str, Any]]:
        cols = [c for c in (row["attrs"] or {}).get("columns") or [] if isinstance(c, dict) and c.get("name")]
        if cols:
            return cols
        with self.svc.db.connect() as c:
            kids = fetch_all(c, select(assets).where(assets.c.parent_id == row["id"], assets.c.type == "column", assets.c.status != "removed").order_by(assets.c.name).limit(500))
        return [{"name": k["name"], "type": (k["attrs"] or {}).get("type"), "description": k["description"]} for k in kids]

    # ------------------------------------------------------------------------------------------------------------------------ summary
    def summary(self, principal: Principal) -> dict[str, Any]:
        """Counts for the Intelligence overview, limited to what the caller may see."""
        svc = self.svc
        scope = svc.scope(principal)
        t, e = assets.c, edges.c
        with svc.db.connect() as c:
            base = svc.scope_filter(select(t.type, t.status, func.count()).group_by(t.type, t.status), principal, None)
            by_type: dict[str, int] = defaultdict(int)
            by_status: dict[str, int] = defaultdict(int)
            for typ, st, n in c.execute(base):
                by_status[st] += n
                if st != "removed":
                    by_type[typ] += n
            vis_ids = svc.scope_filter(select(t.id), principal, None)
            emain = select(e.confidence, e.type, func.count()).where(e.status == "active", e.src_id.in_(vis_ids), e.dst_id.in_(vis_ids)).group_by(e.confidence, e.type)
            conf: dict[str, int] = defaultdict(int)
            etype: dict[str, int] = defaultdict(int)
            for cf, et, n in c.execute(emain):
                conf[cf] += n
                etype[et] += n
            gq = select(gaps.c.kind, func.count()).where(gaps.c.resolved_at.is_(None), or_(gaps.c.asset_id.in_(vis_ids), gaps.c.asset_id.is_(None)) if scope is None else gaps.c.asset_id.in_(vis_ids))
            gap_kinds = {k: n for k, n in c.execute(gq.group_by(gaps.c.kind))}
            sources = svc.visible_sources(principal)
            last_scans = []
            for sid in sorted(sources):
                s = fetch_all(c, select(scans).where(scans.c.source_id == sid).order_by(scans.c.version.desc()).limit(1))
                if s:
                    last_scans.append({"source_id": sid, "scan_id": s[0]["id"], "status": s[0]["status"], "mode": s[0]["mode"], "version": s[0]["version"], "finished_at": s[0]["finished_at"]})
        out: dict[str, Any] = {
            "assets": {"total": sum(by_type.values()), "by_type": dict(sorted(by_type.items())), "by_status": dict(by_status)},
            "edges": {"total": sum(etype.values()), "by_type": dict(sorted(etype.items())), "by_confidence": {k: conf.get(k, 0) for k in M.CONFIDENCE_LEVELS}},
            "placeholders": by_status.get("placeholder", 0), "gaps": {"open": sum(gap_kinds.values()), "by_kind": gap_kinds}, "last_scans": last_scans,
            "discovery": svc.discovery is not None, "governance": svc.governance is not None, "organisation_wide": scope is None,
        }
        if scope is None:
            out["inbox"] = svc.inbox_stats()
        return out


def _closure(node: str, upstream: dict[str, set[str]]) -> set[str]:
    seen: set[str] = set()
    stack = list(upstream.get(node, ()))
    while stack:
        x = stack.pop()
        if x in seen:
            continue
        seen.add(x)
        stack.extend(upstream.get(x, ()))
    return seen


def _label(x: Any) -> str:
    if isinstance(x, dict):
        return str(x.get("name") or x.get("label") or x.get("id") or json.dumps(x, default=str))
    return str(x)


def _gap_out(g: dict[str, Any]) -> dict[str, Any]:
    return {"id": g["id"], "kind": g["kind"], "subject": g["subject"], "detail": g["detail"], "remediation": g["remediation"], "asset_id": g["asset_id"],
            "source_id": g["source_id"], "origin": g.get("origin"), "location": g["location"], "first_seen_at": g["first_seen_at"], "last_seen_at": g["last_seen_at"]}


def _cap_processes(nodes: dict[str, dict[str, Any]], edge_list: list[dict[str, Any]], roots: set[str], cap: int) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]], int]:
    """Bring an over-long answer down to ``cap`` nodes by contracting process nodes (dataset nodes are what the reader came for): the ones that connect the
    least first. ``A -> [notebook] -> B`` becomes the virtual edge ``A -> B``. Returns the nodes, the edges and how many process nodes were contracted."""
    excess = len(nodes) - cap
    degree: dict[str, int] = defaultdict(int)
    for e in edge_list:
        degree[e["from"]] += 1
        degree[e["to"]] += 1
    victims = sorted((n for n in nodes.values() if n["type"] in M.PROCESS_TYPES and n["id"] not in roots), key=lambda n: (degree[n["id"]], -n.get("level", 0), n["name"], n["id"]))
    chosen = [n["id"] for n in victims[:max(0, excess)]]
    if not chosen:
        return nodes, edge_list, 0
    nodes, edge_list = _contract(nodes, edge_list, chosen)
    return nodes, edge_list, len(chosen)


def _filter_types(nodes: dict[str, dict[str, Any]], edge_list: list[dict[str, Any]], keep_types: set[str], roots: set[str]) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    """Restrict a lineage answer to some node types. Excluded nodes are contracted: ``A -> [notebook] -> B`` with ``types=table`` becomes ``A -> B`` (a bridged
    edge with ``virtual: true``, the weaker confidence of the two, and ``via`` set to the process it bridges)."""
    nodes, cur = _contract(nodes, edge_list, [n for n, v in nodes.items() if n not in roots and v["type"] not in keep_types])
    # via-only process nodes that no remaining edge names disappear with their edges
    named = {e["via"] for e in cur if e.get("via")}
    nodes = {k: v for k, v in nodes.items() if not v.get("via_only") or k in named or v["type"] in keep_types}
    return nodes, cur


def _contract(nodes: dict[str, dict[str, Any]], edge_list: list[dict[str, Any]], victims: list[str]) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    """Remove the ``victims`` from a lineage picture, bridging each one: every edge into it is joined to every edge out of it by a virtual ``FLOWS_TO`` edge."""
    nodes = dict(nodes)
    cur = list(edge_list)
    for x in victims:
        preds = [e for e in cur if e["to"] == x]
        succs = [e for e in cur if e["from"] == x]
        cur = [e for e in cur if e["to"] != x and e["from"] != x]
        have = {(e["from"], e["to"]) for e in cur}
        for p in preds:
            for s in succs:
                if p["from"] == s["to"] or (p["from"], s["to"]) in have or len(cur) > 5000:
                    continue
                have.add((p["from"], s["to"]))
                is_proc = nodes[x]["type"] in M.PROCESS_TYPES
                cur.append({"id": None, "type": "FLOWS_TO", "src": p["from"], "dst": s["to"], "from": p["from"], "to": s["to"], "confidence": weakest(p["confidence"], s["confidence"]),
                            "via": x if is_proc else (p.get("via") or s.get("via")), "transformation": None, "evidence_count": 0, "observation_count": 0, "status": "active",
                            "last_seen_at": None, "last_verified_at": None, "virtual": True, "bridged_through": [x]})
        nodes.pop(x, None)
    return nodes, cur


__all__ = ["Queries"]

