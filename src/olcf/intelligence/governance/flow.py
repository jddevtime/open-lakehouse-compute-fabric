# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Path finding over the rows of a graph walk (PURE: no database).

:meth:`olcf.intelligence.graph.GraphStore.walk` returns the nodes and edges it visited; governance needs to know *how* each node was reached (the lineage path that is
shown as evidence) and *which* start it came from. :func:`reach` runs a multi-source shortest-path search over those edge rows in data-flow terms
(:func:`olcf.intelligence.model.flow`) where the cost of a path is lexicographically ``(weakest confidence on it, number of hops)``: the answer for a node is the path whose
weakest edge is the strongest available, with ties broken by the shorter path. That is the same notion of confidence as the lineage queries' "floor", so a governance answer never
claims more certainty than the best lineage path supports.
"""

from __future__ import annotations

import heapq
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from olcf.intelligence import model as M

RANK: dict[str, int] = {c: i for i, c in enumerate(M.CONFIDENCE_LEVELS)}
NAME_OF_RANK: dict[int, str] = {i: c for c, i in RANK.items()}
#: Numeric confidence reported for inherited classifications, by lineage confidence floor.
CONFIDENCE_VALUE: dict[str, float] = {"verified": 0.9, "derived": 0.75, "inferred": 0.4, "unknown": 0.2}


@dataclass
class Reach:
    """How one node was reached: the weakest confidence rank on the path, hop count, the previous node, the edge used and the start it came from."""

    node: str
    rank: int
    hops: int
    prev: str | None
    edge: dict[str, Any] | None
    origin: str

    @property
    def floor(self) -> str:
        """Confidence floor of the path as a level name."""
        return NAME_OF_RANK[self.rank]


def reach(edge_rows: Iterable[dict[str, Any]], starts: Iterable[str], *, forward: bool = True, skip_edge: Callable[[dict[str, Any]], bool] | None = None) -> dict[str, Reach]:
    """Shortest paths from ``starts`` over ``edge_rows`` following the data flow (``forward``) or against it. ``skip_edge`` drops edges (for example filter-only usages)."""
    adj: dict[str, list[tuple[str, dict[str, Any]]]] = {}
    for e in edge_rows:
        if skip_edge is not None and skip_edge(e):
            continue
        f = M.flow(e["type"], e["src_id"], e["dst_id"])
        if f is None:
            continue
        a, b = f if forward else (f[1], f[0])
        adj.setdefault(a, []).append((b, e))
    best: dict[str, Reach] = {}
    heap: list[tuple[int, int, str, str]] = []
    for s in starts:
        best[s] = Reach(s, 0, 0, None, None, s)
        heap.append((0, 0, s, s))
    heapq.heapify(heap)
    done: set[str] = set()
    while heap:
        rank, hops, node, origin = heapq.heappop(heap)
        if node in done:
            continue
        done.add(node)
        for nxt, e in sorted(adj.get(node, []), key=lambda x: (x[1]["id"], x[0])):
            if nxt in done:
                continue
            r2 = max(rank, RANK.get(e["confidence"], 3))
            cur = best.get(nxt)
            if cur is None or (r2, hops + 1) < (cur.rank, cur.hops):
                best[nxt] = Reach(nxt, r2, hops + 1, node, e, best[node].origin)
                heapq.heappush(heap, (r2, hops + 1, nxt, best[node].origin))
    return best


def path_to(found: dict[str, Reach], node: str) -> tuple[list[str], list[dict[str, Any]]]:
    """``(nodes, edges)`` of the path from its start to ``node`` (start first)."""
    nodes = [node]
    edges: list[dict[str, Any]] = []
    cur = found.get(node)
    guard = 0
    while cur is not None and cur.prev is not None and guard < 1000:
        assert cur.edge is not None
        edges.append(cur.edge)
        nodes.append(cur.prev)
        cur = found.get(cur.prev)
        guard += 1
    nodes.reverse()
    edges.reverse()
    return nodes, edges


def weakest_level(a: str, b: str) -> str:
    """The weaker of two confidence level names."""
    return a if RANK[a] >= RANK[b] else b


def is_filter_only(edge: dict[str, Any]) -> bool:
    """True for a ``DEPENDS_ON`` edge that only filters, sorts or formats a field: the field's values do not appear in the consumer (``attrs.role``)."""
    return edge["type"] == "DEPENDS_ON" and str((edge.get("attrs") or {}).get("role") or "") in ("filter", "sort", "format")
