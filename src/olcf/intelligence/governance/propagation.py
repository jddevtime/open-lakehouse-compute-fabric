# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Classification propagation along lineage: "where does the data of this label end up?".

Approved classification assignments are the *sources*. For each source this module walks the lineage graph **downstream** (the direction data flows, with
:meth:`olcf.intelligence.graph.GraphStore.walk` and :func:`olcf.intelligence.model.flow`) and records an *inherited* item for every dataset, column, measure, visual, report ... that
receives its data. Inherited items are **computed, never stored**; the result is cached per graph version (a stamp made of the newest scan id, the active edge count, the newest edge id and edge
sighting, and the approved assignments) and recomputed when the stamp changes.

How precisely a label is known downstream (``precision``) matters, so every item carries it:

``column``   the label sits on a column (or measure / visual) and was followed over **column-level** edges (``TRANSFORMS``, ``USES_MEASURE``, ``DISPLAYS``, ``DEPENDS_ON``) to
             another column, a measure or a visual. Proven by lineage.
``table``    the label sits on a table-like asset and was followed over **table-level** edges. The label was asserted for the whole table, so it holds for what is read from it.
``contains`` an asset that holds a ``column``-precision item: a table with a classified column, a semantic model, a report with a visual that shows one.

A label on a **column** also reaches, by table-level lineage only, the tables and reports downstream of its table. That a downstream report *may* receive the column is
possible but **not proven** (the column-level link is unknown): such items are marked ``proven: false`` with their confidence capped at ``inferred``, are never used as an
asset's inherited classification, and are only reported by the exposure query and (when a policy asks for it) by policies. ``DEPENDS_ON`` edges that only filter, sort or
format a field are not followed: the values do not appear downstream.

Confidence floor: the weakest edge on the strongest path from the source (``verified`` > ``derived`` > ``inferred`` > ``unknown``), mapped to 0.9 / 0.75 / 0.4 / 0.2. Edges weaker than
the ``governance.propagation_min_confidence`` setting (default ``derived``) are not followed at all. Walks are bounded by ``governance.propagation_depth`` and
``governance.propagation_max_nodes`` per source; a cut-off is reported as ``truncated``.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import func, select

from olcf.intelligence import model as M
from olcf.intelligence.governance import flow
from olcf.intelligence.governance.common import Base
from olcf.intelligence.governance.schema import gov_class_assignments
from olcf.intelligence.graph import TABLE_LEVEL_EDGES
from olcf.intelligence.schema import assets, edges, scans
from olcf.security.rbac import Principal

COLUMN_FLOW_EDGES: tuple[str, ...] = ("TRANSFORMS", "DEPENDS_ON", "USES_MEASURE", "DISPLAYS")
ALL_FLOW_EDGES: tuple[str, ...] = tuple(dict.fromkeys((*TABLE_LEVEL_EDGES, "TRANSFORMS")))
#: Types that hold a "contains" item for something classified inside them.
RECEIVER_TYPES = frozenset({"table", "view", "model_table", "semantic_model", "report", "dashboard", "page"})
#: Types whose parent chain is climbed to find the containers.
CLIMB_TYPES = frozenset({"column", "visual", "page", "measure"})
TABLE_LIKE = frozenset({"table", "view", "model_table"})
MAX_SOURCES = 5000


@dataclass
class Inherited:
    """One computed inherited item: ``class_id`` reaches ``target_id`` from ``source_id`` (assignment ``assignment_id``) over the path ``nodes`` / ``edges``."""

    class_id: str
    target_id: str
    source_id: str
    assignment_id: int
    floor: str
    precision: str
    proven: bool
    nodes: list[str]
    edge_rows: list[dict[str, Any]]
    via: str | None = None             # for "contains": the contained asset (the column / visual) the path ends at

    @property
    def confidence(self) -> float:
        """Numeric confidence of the floor."""
        return flow.CONFIDENCE_VALUE[self.floor]

    def better_than(self, other: Inherited) -> bool:
        """Ordering used when one (target, class) is reached several ways: proven first, then the stronger floor, then the shorter path."""
        return (not self.proven, flow.RANK[self.floor], len(self.nodes)) < (not other.proven, flow.RANK[other.floor], len(other.nodes))


@dataclass
class PropResult:
    """Everything computed for one stamp."""

    stamp: Any
    by_target: dict[str, list[Inherited]] = field(default_factory=dict)
    by_class: dict[str, list[Inherited]] = field(default_factory=dict)
    sources: list[tuple[int, str, str]] = field(default_factory=list)     # (assignment id, class id, asset id)
    truncated: bool = False
    notes: list[str] = field(default_factory=list)


class Propagation(Base):
    """Computes and caches the inherited classifications (see the module docstring)."""

    def __init__(self, gov: Any) -> None:
        super().__init__(gov)
        self._lock = threading.Lock()
        self._cache: PropResult | None = None

    # ------------------------------------------------------------------------------------------------------------------------ parameters
    def params(self) -> dict[str, Any]:
        """Effective propagation settings."""
        return {"depth": int(self.setting("governance.propagation_depth", 6)), "max_nodes": int(self.setting("governance.propagation_max_nodes", 500)),
                "min_confidence": str(self.setting("governance.propagation_min_confidence", "derived"))}

    def stamp(self) -> tuple[Any, ...]:
        """A cheap fingerprint of the graph and the approved assignments; changes whenever the answer may change."""
        a, e, g = assets.c, edges.c, gov_class_assignments.c
        with self.db.connect() as c:
            last_scan = c.execute(select(func.max(scans.c.id))).scalar()
            n_edges, max_edge, max_seen = c.execute(select(func.count(), func.max(e.id), func.max(e.last_seen_at)).where(e.status == "active")).one()
            n_asg, max_rev, max_asg = c.execute(select(func.count(), func.max(g.reviewed_at), func.max(g.id)).where(g.status == "approved")).one()
            n_removed = c.execute(select(func.count()).select_from(assets).where(a.status == "removed")).scalar()
        p = self.params()
        return (last_scan, n_edges, max_edge, max_seen, n_asg, max_rev, max_asg, n_removed, p["depth"], p["max_nodes"], p["min_confidence"])

    # ------------------------------------------------------------------------------------------------------------------------ computation
    def result(self) -> PropResult:
        """The cached result for the current stamp (computed on first use / change)."""
        st = self.stamp()
        with self._lock:
            if self._cache is not None and self._cache.stamp == st:
                return self._cache
            res = self.compute(st=st)
            self._cache = res
            return res

    def invalidate(self) -> None:
        """Drop the cache (tests, and after bulk changes)."""
        with self._lock:
            self._cache = None

    def compute(self, *, st: Any = None, only_class: str | None = None, depth: int | None = None, min_confidence: str | None = None, max_nodes: int | None = None) -> PropResult:
        """Walk downstream from every approved assignment (optionally only those of one class, with explicit bounds) and collect the inherited items."""
        p = self.params()
        depth = int(depth if depth is not None else p["depth"])
        max_nodes = int(max_nodes if max_nodes is not None else p["max_nodes"])
        minc = min_confidence or p["min_confidence"]
        g = gov_class_assignments.c
        stmt = (select(g.id, g.class_id, g.asset_id, assets.c.type, assets.c.status, assets.c.parent_id).join(assets, assets.c.id == g.asset_id)
                .where(g.status == "approved", assets.c.status != "removed").order_by(g.id))
        if only_class:
            stmt = stmt.where(g.class_id == only_class)
        with self.db.connect() as c:
            srcs = [dict(r._mapping) for r in c.execute(stmt.limit(MAX_SOURCES + 1))]
        res = PropResult(stamp=st)
        if len(srcs) > MAX_SOURCES:
            srcs = srcs[:MAX_SOURCES]
            res.truncated = True
            res.notes.append(f"more than {MAX_SOURCES} classified assets: propagation covers the oldest {MAX_SOURCES}")
        by_asset: dict[str, list[dict[str, Any]]] = {}
        for s in srcs:
            by_asset.setdefault(s["asset_id"], []).append(s)
            res.sources.append((s["id"], s["class_id"], s["asset_id"]))
        best: dict[tuple[str, str], Inherited] = {}
        climb_later: list[tuple[Inherited, str]] = []

        def keep(item: Inherited) -> None:
            k = (item.target_id, item.class_id)
            cur = best.get(k)
            if cur is None or item.better_than(cur):
                best[k] = item

        for asset_id, group in by_asset.items():
            stype = group[0]["type"]
            if stype == "column":
                self._from_column(res, group, asset_id, group[0]["parent_id"], depth, max_nodes, minc, keep, climb_later)
            else:
                self._from_asset(res, group, asset_id, stype, depth, max_nodes, minc, keep, climb_later)
        self._climb(climb_later, keep)
        for item in best.values():
            res.by_target.setdefault(item.target_id, []).append(item)
            res.by_class.setdefault(item.class_id, []).append(item)
        return res

    def _walk(self, start: str, edge_types: tuple[str, ...], depth: int, max_nodes: int, minc: str) -> tuple[Any, dict[str, flow.Reach]]:
        w = self.intel.graph.walk([start], direction="down", depth=depth, edge_types=edge_types, min_confidence=minc, max_nodes=max_nodes)
        return w, flow.reach(w.edges.values(), [start], forward=True, skip_edge=flow.is_filter_only)

    def _emit(self, group: list[dict[str, Any]], walk: Any, found: dict[str, flow.Reach], source: str, precision: str, proven: bool, keep: Any, climb: list[tuple[Inherited, str]],
              cap: str | None = None) -> set[str]:
        reached: set[str] = set()
        for nid, rc in found.items():
            if nid == source:
                continue
            row = walk.nodes.get(nid, {}).get("row")
            if row is None or row["type"] in M.PROCESS_TYPES or row["status"] == "removed":
                continue
            nodes, es = flow.path_to(found, nid)
            floor = rc.floor if cap is None else flow.weakest_level(rc.floor, cap)
            for s in group:
                item = Inherited(s["class_id"], nid, source, s["id"], floor, precision, proven, nodes, es)
                keep(item)
                if proven and row["type"] in CLIMB_TYPES:
                    climb.append((item, nid))
            reached.add(nid)
        return reached

    def _from_column(self, res: PropResult, group: list[dict[str, Any]], source: str, table_id: str | None, depth: int, max_nodes: int, minc: str, keep: Any,
                     climb: list[tuple[Inherited, str]]) -> None:
        walk, found = self._walk(source, COLUMN_FLOW_EDGES, depth, max_nodes, minc)
        res.truncated = res.truncated or walk.truncated
        proven_nodes = self._emit(group, walk, found, source, "column", True, keep, climb)
        if not table_id:
            return
        trow = self.intel.graph.asset_rows([table_id]).get(table_id)
        if trow is None or trow["type"] not in TABLE_LIKE:
            return
        twalk, tfound = self._walk(table_id, TABLE_LEVEL_EDGES, depth, max_nodes, minc)
        res.truncated = res.truncated or twalk.truncated
        # possible exposure: whatever the table feeds, minus what the column-level walk already proved
        sub = {nid: rc for nid, rc in tfound.items() if nid not in proven_nodes}
        self._emit(group, twalk, sub, table_id, "table", False, keep, climb, cap="inferred")

    def _from_asset(self, res: PropResult, group: list[dict[str, Any]], source: str, stype: str, depth: int, max_nodes: int, minc: str, keep: Any,
                    climb: list[tuple[Inherited, str]]) -> None:
        precision = "column" if stype in ("measure", "visual") else "table"
        walk, found = self._walk(source, ALL_FLOW_EDGES, depth, max_nodes, minc)
        res.truncated = res.truncated or walk.truncated
        self._emit(group, walk, found, source, precision, True, keep, climb)

    def _climb(self, pending: list[tuple[Inherited, str]], keep: Any) -> None:
        """For items that landed on a column / visual / page / measure add ``contains`` items on the containers (table, semantic model, report ...)."""
        if not pending:
            return
        leaves = sorted({nid for _, nid in pending})
        chains, rows = self.gov.owners.chains(leaves)
        for item, leaf in pending:
            for anc in chains[leaf][1:]:
                arow = rows.get(anc)
                if arow is None:
                    break
                if arow["type"] in RECEIVER_TYPES and arow["status"] != "removed":
                    keep(Inherited(item.class_id, anc, item.source_id, item.assignment_id, item.floor, "contains", True, item.nodes, item.edge_rows, via=leaf))
                elif arow["type"] not in CLIMB_TYPES:
                    break

    # ------------------------------------------------------------------------------------------------------------------------ views for callers
    def inherited_for(self, asset_ids: list[str], *, proven_only: bool = True) -> dict[str, list[Inherited]]:
        """Inherited items per asset id (cached computation)."""
        res = self.result()
        out: dict[str, list[Inherited]] = {}
        for a in asset_ids:
            items = [i for i in res.by_target.get(a, []) if i.proven or not proven_only]
            if items:
                out[a] = sorted(items, key=lambda i: (flow.RANK[i.floor], i.class_id))
        return out

    def shape(self, principal: Principal | None, item: Inherited, refs: dict[str, dict[str, Any]], rows: dict[str, dict[str, Any]]) -> dict[str, Any]:
        """The API form of an inherited item. ``rows`` supplies asset rows by id; assets the caller cannot see are replaced by ``{"hidden": true}``."""

        def ref(aid: str) -> dict[str, Any]:
            return self.ref(principal, rows.get(aid))

        out: dict[str, Any] = {
            "id": None, "source": "inherited", "status": "approved", "classification": refs.get(item.class_id), "confidence": item.confidence, "confidence_floor": item.floor,
            "precision": item.precision, "proven": item.proven,
            "evidence": {"from_asset": ref(item.source_id), "assignment_id": item.assignment_id,
                         "path": {"nodes": [ref(n) for n in item.nodes], "edges": [{"id": e["id"], "type": e["type"], "confidence": e["confidence"]} for e in item.edge_rows]}},
        }
        if item.via:
            out["evidence"]["contains"] = ref(item.via)
        return out

    def rows_for(self, items: list[Inherited]) -> dict[str, dict[str, Any]]:
        """Asset rows for every asset mentioned by the items (sources, targets, path nodes)."""
        ids = sorted({n for i in items for n in (*i.nodes, i.source_id, i.target_id, *( [i.via] if i.via else []))})
        return self.intel.graph.asset_rows(ids) if ids else {}

    def shape_many(self, principal: Principal | None, items: list[Inherited]) -> list[dict[str, Any]]:
        """Shape several items at once (one batch of lookups)."""
        if not items:
            return []
        refs = self.gov.classifications.refs(sorted({i.class_id for i in items}))
        rows = self.rows_for(items)
        return [self.shape(principal, i, refs, rows) for i in items]


__all__ = ["Inherited", "PropResult", "Propagation"]
