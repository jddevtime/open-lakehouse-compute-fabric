# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The graph store: assets with attribute history, edges with evidence, removal sweeps, traversal and snapshot diffs.

Rules this module enforces (contract section 4):

* **Idempotent apply.** Applying the same facts twice creates no new rows, no duplicate evidence and no new asset version; only ``last_seen_*`` and
  ``observation_count`` move.
* **Edge identity** is ``model.edge_hash(src, dst, type, via)``. A repeat observation merges its evidence (de-duplicated by ``Evidence.key()``, at most 20 kept,
  newest first, always keeping the newest evidence of every confidence level) and bumps ``last_seen_at`` / ``observation_count``.
* **Confidence** is the strongest confidence among the kept evidence. A ``verified`` runtime observation therefore upgrades a ``derived`` static edge, and a later
  static scan never downgrades it while the runtime evidence is younger than the retention window (``verified_days``); older runtime evidence is pruned.
* **Removal sweep** (:meth:`GraphStore.sweep`) only ever runs for a *full*, *succeeded* scan and only touches assets / edges of that scan's source; incremental
  scans touch only what they report. Edges with recent runtime verification survive a sweep. Nothing is deleted - ``status`` moves to ``removed``.
* **Traversal** follows the data-flow direction of ``model.flow``; process nodes (notebook, pipeline ...) do not cost depth, cycles are cut by the visited set,
  and a node cap yields ``truncated`` instead of an unbounded answer.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from sqlalchemy import Connection, String, cast, delete, func, insert, or_, select, update

from olcf.core.errors import BadRequest, NotFound
from olcf.core.ids import new_id
from olcf.core.timeutil import from_iso, iso_in, now_iso, to_iso, utcnow
from olcf.db.engine import Database
from olcf.db.util import fetch_all, fetch_one, new_etag
from olcf.intelligence import model as M
from olcf.intelligence.schema import asset_versions, assets, edges, gaps, scans

#: Evidence kept per edge.
MAX_EVIDENCE = 20
#: Attributes that change on every scan (statistics) and therefore do not create a new asset version.
VOLATILE_ATTRS = frozenset({"row_count", "size_bytes", "num_files", "last_modified", "delta_version", "synced_at", "stats", "profile", "last_run_at", "last_run_state",
                            "last_success_at", "last_scanned_at", "last_scan_status", "source_tags"})
#: Edge types that carry data in the table-level picture.
TABLE_LEVEL_EDGES = ("READS_FROM", "WRITES_TO", "DEPENDS_ON", "USES_MEASURE", "DISPLAYS")
_RANK = {c: i for i, c in enumerate(M.CONFIDENCE_LEVELS)}


# --------------------------------------------------------------------------------------------------------------------------------------- evidence
def weakest(a: str, b: str) -> str:
    """The weaker of two confidence levels."""
    return a if _RANK[a] >= _RANK[b] else b


def allowed_confidences(min_confidence: str | None) -> list[str]:
    """Confidence levels at least as strong as ``min_confidence`` (all of them when ``None``)."""
    if min_confidence is None:
        return list(M.CONFIDENCE_LEVELS)
    if min_confidence not in _RANK:
        raise BadRequest(f"unknown confidence '{min_confidence}'", details={"supported": list(M.CONFIDENCE_LEVELS)})
    return [c for c in M.CONFIDENCE_LEVELS if _RANK[c] <= _RANK[min_confidence]]


def evidence_dict(ev: M.Evidence, now: str, cap_to: str | None = None) -> dict[str, Any]:
    """The stored form of an :class:`~olcf.intelligence.model.Evidence`: ``observed_at`` filled in, snippet clipped, confidence capped to ``cap_to``."""
    conf = ev.confidence if ev.confidence in _RANK else "unknown"
    if cap_to:
        conf = weakest(conf, cap_to)
    return {"kind": ev.kind, "source": ev.source, "confidence": conf, "ref": ev.ref, "location": dict(ev.location or {}), "snippet": M.clip(ev.snippet),
            "observed_at": ev.observed_at or now}


def evidence_key(d: dict[str, Any]) -> tuple[Any, ...]:
    """Identity of a stored evidence record (same as ``Evidence.key()``)."""
    return (d.get("kind"), d.get("source"), d.get("ref"), repr(sorted((d.get("location") or {}).items())))


def merge_evidence(existing: Sequence[dict[str, Any]], new: Sequence[dict[str, Any]], *, cap: int = MAX_EVIDENCE, drop_runtime_before: str | None = None) -> list[dict[str, Any]]:
    """Union of two evidence lists: de-duplicated by key (the observation with the newer ``observed_at`` wins), pruned of runtime evidence older than
    ``drop_runtime_before`` (never below the ``new`` records), capped to ``cap`` newest while keeping the newest record of each confidence level, newest first."""
    by_key: dict[tuple[Any, ...], dict[str, Any]] = {}
    for d in [*existing, *new]:
        k = evidence_key(d)
        cur = by_key.get(k)
        if cur is None or (d.get("observed_at") or "") >= (cur.get("observed_at") or ""):
            by_key[k] = dict(d)
    new_keys = {evidence_key(d) for d in new}
    items = list(by_key.values())
    if drop_runtime_before:
        items = [d for d in items if evidence_key(d) in new_keys or d.get("kind") != "runtime" or (d.get("observed_at") or "") >= drop_runtime_before]
    items.sort(key=lambda d: d.get("observed_at") or "", reverse=True)
    if len(items) > cap:
        keep: list[dict[str, Any]] = []
        seen_levels: set[str] = set()
        for d in items:  # newest of every confidence level first, so the strongest evidence survives the cap
            if d.get("confidence") not in seen_levels:
                seen_levels.add(d.get("confidence") or "unknown")
                keep.append(d)
        ids = {id(d) for d in keep}
        for d in items:
            if len(keep) >= cap:
                break
            if id(d) not in ids:
                keep.append(d)
        keep.sort(key=lambda d: d.get("observed_at") or "", reverse=True)
        items = keep[:cap]
    return items


def strongest_confidence(evidence: Iterable[dict[str, Any]], *fallback: str | None) -> str:
    """The strongest confidence among the evidence records (and the fallbacks when there is none), ``unknown`` when nothing is known."""
    best = "unknown"
    seen = False
    for d in evidence:
        c = d.get("confidence")
        if c in _RANK:
            best = M.stronger(best, c) if seen else c
            seen = True
    if seen:
        return best
    for f in fallback:
        if f in _RANK:
            best = M.stronger(best, f) if seen else f
            seen = True
    return best


# --------------------------------------------------------------------------------------------------------------------------------------- attributes
def attrs_hash(attrs: dict[str, Any]) -> str:
    """Hash of the non-volatile attributes (what a version row records)."""
    stable = {k: v for k, v in (attrs or {}).items() if k not in VOLATILE_ATTRS and k not in ("unresolved", "referenced_as")}
    return hashlib.sha256(json.dumps(stable, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")).hexdigest()


def attr_changes(old: dict[str, Any], new: dict[str, Any]) -> dict[str, Any]:
    """What changed between two attribute documents: added / removed / retyped columns (``columns`` is a list of ``{name, type}``) and the other keys."""
    out: dict[str, Any] = {}

    def cols(a: dict[str, Any]) -> dict[str, str]:
        res: dict[str, str] = {}
        for c in a.get("columns") or []:
            if isinstance(c, dict) and c.get("name") is not None:
                res[str(c["name"]).lower()] = str(c.get("type"))
            elif isinstance(c, str):
                res[c.lower()] = ""
        return res

    oc, nc = cols(old), cols(new)
    if oc or nc:
        added = sorted(set(nc) - set(oc))
        removed = sorted(set(oc) - set(nc))
        changed = sorted(c for c in set(oc) & set(nc) if oc[c] != nc[c])
        if added:
            out["columns_added"] = added
        if removed:
            out["columns_removed"] = removed
        if changed:
            out["columns_changed"] = [{"column": c, "from": oc[c], "to": nc[c]} for c in changed]
    other = sorted(k for k in set(old) | set(new) if k not in VOLATILE_ATTRS and k not in ("columns", "unresolved", "referenced_as") and old.get(k) != new.get(k))
    if other:
        out["attrs_changed"] = other
    return out


@dataclass
class AssetResult:
    """What :meth:`GraphStore.upsert_asset` did: ``created``, ``updated`` or ``unchanged``, and whether the attribute history gained a version."""

    id: str
    kind: str
    versioned: bool = False
    changes: dict[str, Any] = field(default_factory=dict)


@dataclass
class EdgeResult:
    """What :meth:`GraphStore.upsert_edge` did: the edge id, whether it is new, whether anything but counters changed and its confidence before / after."""

    id: str
    created: bool
    changed: bool
    confidence_before: str | None
    confidence_after: str


@dataclass
class Walk:
    """The raw result of a traversal: visited nodes (``{id: {"level", "floor", "row"}}``), accepted edges and whether the node cap cut the walk short."""

    nodes: dict[str, dict[str, Any]] = field(default_factory=dict)
    edges: dict[str, dict[str, Any]] = field(default_factory=dict)
    truncated: bool = False


class GraphStore:
    """Persistence of the asset graph. Every write method takes the caller's connection (an already serialised transaction, see ``AssetRegistry``); the
    read methods open their own. ``verified_days`` is the retention window of runtime evidence, ``max_nodes`` the traversal cap (both callables so live
    settings apply)."""

    def __init__(self, db: Database, *, verified_days: Callable[[], int] | None = None, max_nodes: Callable[[], int] | None = None) -> None:
        self.db = db
        self._verified_days = verified_days or (lambda: 90)
        self._max_nodes = max_nodes or (lambda: 500)

    def runtime_cutoff(self) -> str:
        """ISO time before which runtime evidence is considered expired."""
        return to_iso(utcnow() - timedelta(days=max(1, int(self._verified_days()))))

    # ------------------------------------------------------------------------------------------------------------------------ assets
    def upsert_asset(self, conn: Connection, asset_id: str, fact: M.AssetFact, *, scan_id: str | None, source_id: str | None, parent_id: str | None,
                     created: bool) -> AssetResult:
        """Write what a connector knows about an asset (already identified by the registry): name, path, workspace, parent, attributes (shallow merge),
        description (never over a manual one) and liveness; records an ``asset_versions`` row when the non-volatile attributes changed."""
        t = assets.c
        row = fetch_one(conn, select(assets).where(t.id == asset_id))
        if row is None:
            raise NotFound(f"asset '{asset_id}' does not exist")
        now = now_iso()
        was_placeholder = row["status"] == "placeholder"
        old_attrs = dict(row["attrs"] or {})
        attrs = {**old_attrs, **(fact.attrs or {})}
        if was_placeholder:
            attrs.pop("unresolved", None)
            attrs.pop("referenced_as", None)
        if fact.tags:
            attrs["source_tags"] = sorted({str(x) for x in fact.tags})
        vals: dict[str, Any] = {"name": fact.name[:512], "attrs": attrs, "status": "active", "last_seen_scan": scan_id, "removed_scan": None}
        if fact.path is not None:
            vals["path"] = fact.path[:1024]
        if fact.workspace_id is not None:
            vals["workspace_id"] = fact.workspace_id
        if parent_id is not None:
            vals["parent_id"] = parent_id
        if was_placeholder:
            vals["type"] = fact.ref.type
            vals["source_id"] = source_id or row["source_id"]
        elif not row["source_id"] and source_id:
            vals["source_id"] = source_id
        if row["first_seen_scan"] is None and scan_id:
            vals["first_seen_scan"] = scan_id
        if fact.description is not None and row["description_source"] != "manual" and fact.description != row["description"]:
            vals["description"] = fact.description
        watched = ("name", "path", "workspace_id", "parent_id", "type", "status", "description", "attrs")
        changed = any(k in vals and vals[k] != row[k] for k in watched)
        if changed:
            vals.update(updated_at=now, etag=new_etag())
        conn.execute(update(assets).where(t.id == asset_id).values(**vals))
        h = attrs_hash(attrs)
        last = fetch_one(conn, select(asset_versions.c.attrs_hash, asset_versions.c.attrs).where(asset_versions.c.asset_id == asset_id).order_by(asset_versions.c.id.desc()).limit(1))
        versioned = False
        changes: dict[str, Any] = {}
        if last is None or last["attrs_hash"] != h:
            changes = {"created": True} if last is None else attr_changes(last["attrs"] or {}, attrs)
            conn.execute(insert(asset_versions).values(asset_id=asset_id, scan_id=scan_id, attrs_hash=h, attrs=attrs, changes=changes, created_at=now))
            versioned = True
        kind = "created" if created else ("updated" if changed else "unchanged")
        return AssetResult(asset_id, kind, versioned, changes)

    def touch_assets(self, conn: Connection, ids: Sequence[str], scan_id: str) -> int:
        """Mark assets as seen by a scan without changing them (an analysis skipped because its etag did not change)."""
        n = 0
        for i in range(0, len(ids), 300):
            n += conn.execute(update(assets).where(assets.c.id.in_(list(ids[i:i + 300])), assets.c.status.in_(("active", "placeholder"))).values(last_seen_scan=scan_id)).rowcount
        return n

    def retire_assets(self, conn: Connection, ids: Sequence[str], scan_id: str, source_id: str | None) -> int:
        """Mark assets removed (their definition no longer produces them); only assets that source created are touched."""
        n = 0
        for i in range(0, len(ids), 300):
            stmt = update(assets).where(assets.c.id.in_(list(ids[i:i + 300])), assets.c.status == "active")
            if source_id:
                stmt = stmt.where(assets.c.source_id == source_id)
            n += conn.execute(stmt.values(status="removed", removed_scan=scan_id, updated_at=now_iso(), etag=new_etag())).rowcount
        return n

    # ------------------------------------------------------------------------------------------------------------------------ edges
    def upsert_edge(self, conn: Connection, *, src_id: str, dst_id: str, type_: str, via_id: str | None, evidence: Sequence[M.Evidence], confidence: str | None,
                    transformation: str | None, attrs: dict[str, Any] | None, scan_id: str | None, source_id: str | None) -> EdgeResult:
        """Insert an edge or merge a repeat observation into it (evidence union, confidence recompute, counters)."""
        if type_ not in M.STORED_EDGE_TYPES:
            raise BadRequest(f"edge type '{type_}' is not stored", details={"stored": list(M.STORED_EDGE_TYPES), "virtual": ["GOVERNED_BY", "CLASSIFIED_AS"]})
        e = edges.c
        now = now_iso()
        cap_to = confidence if confidence in _RANK else None
        evs = [evidence_dict(x, now, cap_to) for x in evidence]
        if not evs:
            evs = [evidence_dict(M.Evidence("api", source_id or "engine", confidence or "unknown", ref=scan_id), now)]
        runtime = any(x["kind"] == "runtime" for x in evs)
        h = M.edge_hash(src_id, dst_id, type_, via_id)
        row = fetch_one(conn, select(edges).where(e.edge_hash == h))
        if row is None:
            kept = merge_evidence([], evs)
            conf = strongest_confidence(kept, confidence)
            eid = new_id("edg")
            conn.execute(insert(edges).values(
                id=eid, edge_hash=h, src_id=src_id, dst_id=dst_id, type=type_, via_id=via_id, confidence=conf, transformation=transformation, attrs=dict(attrs or {}),
                evidence=kept, observation_count=1, source_id=source_id, first_seen_scan=scan_id, last_seen_scan=scan_id, removed_scan=None, first_seen_at=now,
                last_seen_at=now, last_verified_at=now if runtime else None, status="active"))
            return EdgeResult(eid, True, True, None, conf)
        kept = merge_evidence(row["evidence"] or [], evs, drop_runtime_before=self.runtime_cutoff())
        conf = strongest_confidence(kept, confidence, row["confidence"] if not kept else None)
        eattrs = dict(row["attrs"] or {})
        history = list(eattrs.get("changes") or [])
        changed = False
        if conf != row["confidence"]:
            history.append({"scan_id": scan_id, "field": "confidence", "from": row["confidence"], "to": conf, "at": now})
            changed = True
        if transformation is not None and transformation != row["transformation"]:
            history.append({"scan_id": scan_id, "field": "transformation", "from": row["transformation"], "to": transformation, "at": now})
            changed = True
        if row["status"] != "active":
            history.append({"scan_id": scan_id, "field": "status", "from": row["status"], "to": "active", "at": now})
            changed = True
        if eattrs.pop("stale", None) is not None:                   # observed again by a complete analysis: no longer last-known-good only
            history.append({"scan_id": scan_id, "field": "stale", "from": True, "to": False, "at": now})
            changed = True
        for k, v in (attrs or {}).items():
            if eattrs.get(k) != v:
                eattrs[k] = v
                changed = True
        if history:
            eattrs["changes"] = history[-MAX_EVIDENCE:]
        if {evidence_key(d) for d in kept} != {evidence_key(d) for d in (row["evidence"] or [])}:
            changed = True
        conn.execute(update(edges).where(e.id == row["id"]).values(
            evidence=kept, confidence=conf, transformation=transformation if transformation is not None else row["transformation"], attrs=eattrs,
            observation_count=(row["observation_count"] or 0) + 1, last_seen_scan=scan_id, removed_scan=None, last_seen_at=now, status="active",
            last_verified_at=now if runtime else row["last_verified_at"], source_id=row["source_id"] or source_id))
        return EdgeResult(row["id"], False, changed, row["confidence"], conf)

    def touch_edges(self, conn: Connection, ids: Sequence[str], scan_id: str) -> int:
        """Mark edges as seen by a scan without adding an observation (their definition was unchanged and its analysis skipped)."""
        n = 0
        now = now_iso()
        for i in range(0, len(ids), 300):
            n += conn.execute(update(edges).where(edges.c.id.in_(list(ids[i:i + 300])), edges.c.status == "active").values(last_seen_scan=scan_id, last_seen_at=now)).rowcount
        return n

    def mark_edges_stale(self, conn: Connection, ids: Sequence[str], scan_id: str, reason: str) -> int:
        """Keep edges that an *incomplete* re-analysis did not find: they count as seen by ``scan_id`` (a sweep leaves them alone) but are flagged ``attrs.stale``
        (``since_scan``, the ``last_confirmed_scan`` / ``last_confirmed_at`` that still observed them, ``reason``). Evidence and its version are untouched; a later complete
        analysis either observes the edge again (the flag is cleared) or retires it. Returns the edges flagged."""
        n = 0
        t = edges.c
        for i in range(0, len(ids), 300):
            for r in fetch_all(conn, select(t.id, t.attrs, t.last_seen_scan, t.last_seen_at).where(t.id.in_(list(ids[i:i + 300])), t.status == "active")):
                attrs = dict(r["attrs"] or {})
                prev: dict[str, Any] = attrs["stale"] if isinstance(attrs.get("stale"), dict) else {}
                attrs["stale"] = {"since_scan": prev.get("since_scan") or scan_id, "last_confirmed_scan": prev.get("last_confirmed_scan") or r["last_seen_scan"],
                                  "last_confirmed_at": prev.get("last_confirmed_at") or r["last_seen_at"], "checked_scan": scan_id, "reason": reason[:300]}
                conn.execute(update(edges).where(t.id == r["id"]).values(attrs=attrs, last_seen_scan=scan_id))
                n += 1
        return n

    def retire_edges(self, conn: Connection, ids: Sequence[str], scan_id: str, source_id: str | None) -> int:
        """Mark edges removed that a re-analysed definition no longer produces (edges with recent runtime verification stay)."""
        n = 0
        cutoff = self.runtime_cutoff()
        for i in range(0, len(ids), 300):
            stmt = update(edges).where(edges.c.id.in_(list(ids[i:i + 300])), edges.c.status == "active", or_(edges.c.last_verified_at.is_(None), edges.c.last_verified_at < cutoff))
            if source_id:
                stmt = stmt.where(edges.c.source_id == source_id)
            n += conn.execute(stmt.values(status="removed", removed_scan=scan_id)).rowcount
        return n

    def recompute_confidence(self, conn: Connection, edge_id: str) -> str:
        """Recompute an edge's confidence from its kept evidence and store it."""
        row = fetch_one(conn, select(edges).where(edges.c.id == edge_id))
        if row is None:
            raise NotFound(f"edge '{edge_id}' does not exist")
        conf = strongest_confidence(row["evidence"] or [], row["confidence"])
        if conf != row["confidence"]:
            conn.execute(update(edges).where(edges.c.id == edge_id).values(confidence=conf))
        return conf

    def prune_runtime_evidence(self, conn: Connection, *, limit: int | None = None) -> int:
        """Drop runtime evidence older than the retention window from every edge that has some and recompute the confidences. Returns the edges changed.
        With ``limit`` at most that many edges are looked at per call (a bounded write transaction); a pruned edge no longer matches, so repeat until the result is below ``limit``."""
        cutoff = self.runtime_cutoff()
        n = 0
        stmt = select(edges).where(edges.c.last_verified_at.is_not(None), edges.c.last_verified_at < cutoff).order_by(edges.c.id)
        for r in fetch_all(conn, stmt.limit(limit) if limit else stmt):
            kept = [d for d in (r["evidence"] or []) if d.get("kind") != "runtime" or (d.get("observed_at") or "") >= cutoff]
            if not kept and r["evidence"]:    # nothing but stale observations: keep the newest as history, but it no longer justifies 'verified'
                newest = max(r["evidence"], key=lambda d: d.get("observed_at") or "")
                kept = [{**newest, "confidence": weakest(newest.get("confidence") or "unknown", "derived"), "stale": True}]
            if kept != (r["evidence"] or []):
                conn.execute(update(edges).where(edges.c.id == r["id"]).values(evidence=kept, confidence=strongest_confidence(kept, "unknown"),
                                                                                last_verified_at=max((d["observed_at"] for d in kept if d.get("kind") == "runtime" and not d.get("stale")), default=None)))
                n += 1
        return n

    # ------------------------------------------------------------------------------------------------------------------------ sweep
    def sweep(self, conn: Connection, source_id: str, scan_id: str, *, limit: int | None = None) -> dict[str, int]:
        """The removal sweep of a *full, succeeded* scan: assets and edges of ``source_id`` that this scan did not see become ``removed``; gaps it did not see are
        resolved. Edges with runtime verification inside the retention window are kept. Nothing is deleted. With ``limit`` at most that many rows of each kind change per
        call (a bounded write transaction): call again until every count is below ``limit``; the sweep is idempotent."""
        now = now_iso()
        a, ed, gp = assets.c, edges.c, gaps.c

        def bounded(table: Any, cond: Any) -> Any:
            return table.c.id.in_(select(table.c.id).where(cond).limit(limit)) if limit else cond

        a_cond = (a.source_id == source_id) & (a.status == "active") & (a.last_seen_scan.is_(None) | (a.last_seen_scan != scan_id))
        e_cond = ((ed.source_id == source_id) & (ed.status == "active") & (ed.last_seen_scan.is_(None) | (ed.last_seen_scan != scan_id))
                  & (ed.last_verified_at.is_(None) | (ed.last_verified_at < self.runtime_cutoff())))
        g_cond = (gp.source_id == source_id) & gp.resolved_at.is_(None) & (gp.last_scan_id.is_(None) | (gp.last_scan_id != scan_id))
        n_assets = conn.execute(update(assets).where(bounded(assets, a_cond)).values(status="removed", removed_scan=scan_id, updated_at=now, etag=new_etag())).rowcount
        n_edges = conn.execute(update(edges).where(bounded(edges, e_cond)).values(status="removed", removed_scan=scan_id)).rowcount
        n_gaps = conn.execute(update(gaps).where(bounded(gaps, g_cond)).values(resolved_at=now)).rowcount
        return {"assets_removed": n_assets, "edges_removed": n_edges, "gaps_resolved": n_gaps}

    # ------------------------------------------------------------------------------------------------------------------------ reads
    def get_edge(self, edge_id: str) -> dict[str, Any]:
        """One edge row with its full evidence."""
        with self.db.connect() as c:
            row = fetch_one(c, select(edges).where(edges.c.id == edge_id))
        if row is None:
            raise NotFound(f"edge '{edge_id}' does not exist")
        return row

    def asset_rows(self, ids: Iterable[str], conn: Connection | None = None) -> dict[str, dict[str, Any]]:
        """``{id: assets row}`` for the ids that exist."""
        ids = list(dict.fromkeys(ids))
        out: dict[str, dict[str, Any]] = {}

        def run(c: Connection) -> None:
            for i in range(0, len(ids), 300):
                for r in fetch_all(c, select(assets).where(assets.c.id.in_(ids[i:i + 300]))):
                    out[r["id"]] = r

        if conn is not None:
            run(conn)
        else:
            with self.db.connect() as c:
                run(c)
        return out

    def _adjacent(self, c: Connection, ids: Sequence[str], direction: str, types: Iterable[str], confs: Sequence[str], include_removed: bool) -> list[tuple[dict[str, Any], str, str]]:
        """Edges leaving (``down``) or entering (``up``) the data flow of ``ids``: ``[(edge row, node we came from, neighbour)]`` in flow terms."""
        types = set(types)
        out_types = [t for t in ("WRITES_TO", "TRANSFORMS") if t in types]    # src -> dst
        in_types = [t for t in ("READS_FROM", "DEPENDS_ON", "USES_MEASURE", "DISPLAYS") if t in types]   # dst -> src
        e = edges.c
        res: list[tuple[dict[str, Any], str, str]] = []
        for i in range(0, len(ids), 300):
            chunk = list(ids[i:i + 300])
            conds = []
            if direction == "down":
                if out_types:
                    conds.append((e.type.in_(out_types)) & (e.src_id.in_(chunk)))
                if in_types:
                    conds.append((e.type.in_(in_types)) & (e.dst_id.in_(chunk)))
            else:
                if out_types:
                    conds.append((e.type.in_(out_types)) & (e.dst_id.in_(chunk)))
                if in_types:
                    conds.append((e.type.in_(in_types)) & (e.src_id.in_(chunk)))
            if not conds:
                continue
            stmt = select(edges).where(or_(*conds), e.confidence.in_(list(confs)))
            if not include_removed:
                stmt = stmt.where(e.status == "active")
            chunk_set = set(chunk)
            for r in fetch_all(c, stmt.order_by(e.id)):
                f = M.flow(r["type"], r["src_id"], r["dst_id"])
                if f is None:
                    continue
                frm, to = f
                if direction == "down" and frm in chunk_set:
                    res.append((r, frm, to))
                elif direction == "up" and to in chunk_set:
                    res.append((r, to, frm))
        return res

    def walk(self, start_ids: Sequence[str], *, direction: str, depth: int, edge_types: Iterable[str] = TABLE_LEVEL_EDGES, min_confidence: str | None = None,
             include_removed: bool = False, max_nodes: int | None = None, keep_node: Callable[[dict[str, Any]], bool] | None = None,
             process_cap: int | None = None) -> Walk:
        """Breadth-first walk from ``start_ids`` along the data flow (``down`` = what the data flows into, ``up`` = what it comes from).

        Depth counts dataset hops: stepping onto a process node (notebook, pipeline ...) is free, so ``A -> [notebook] -> B`` is one hop. ``keep_node`` (visibility)
        decides whether a neighbour may be entered at all; hidden nodes are skipped as if they did not exist. ``Walk.nodes[id]["floor"]`` is the weakest
        confidence on the strongest path from the start (``verified`` for the start itself).

        ``max_nodes`` caps the visited nodes. By default every node counts; with ``process_cap`` set, only dataset-like nodes count against ``max_nodes`` and the
        process nodes (notebooks, pipelines ...) have their own cap, so a table read by thousands of notebooks does not use up the budget meant for the tables.
        ``Walk.truncated`` is set when either cap cut the walk short.
        """
        if direction not in ("up", "down"):
            raise BadRequest("direction must be 'up' or 'down'")
        depth = max(1, min(int(depth), 10))
        cap = max(1, int(max_nodes if max_nodes is not None else self._max_nodes()))
        confs = allowed_confidences(min_confidence)
        walk = Walk()
        n_process = 0
        with self.db.connect() as c:
            rows = self.asset_rows(start_ids, c)
            for sid in start_ids:
                if sid in rows:
                    walk.nodes[sid] = {"level": 0, "floor": "verified", "row": rows[sid]}
            level = 0
            current = [s for s in start_ids if s in walk.nodes]
            links: list[tuple[str, str, str]] = []   # (edge id, from node, to node) in walk direction
            while current and level <= depth:
                stack = list(current)
                nxt: list[str] = []
                seen_expand: set[str] = set()
                while stack:
                    batch = [s for s in stack if s not in seen_expand]
                    stack = []
                    if not batch:
                        break
                    seen_expand.update(batch)
                    adj = self._adjacent(c, batch, direction, edge_types, confs, include_removed)
                    unknown = [n for _, _, n in adj if n not in walk.nodes]
                    new_rows = self.asset_rows(unknown, c) if unknown else {}
                    for r, frm, to in adj:
                        if to not in walk.nodes:
                            nrow = new_rows.get(to)
                            if nrow is None or (keep_node is not None and not keep_node(nrow)):
                                continue
                            free = nrow["type"] in M.PROCESS_TYPES
                            lvl = level if free else level + 1
                            if lvl > depth:
                                continue
                            if process_cap is None:
                                full = len(walk.nodes) >= cap
                            elif free:
                                full = n_process >= process_cap
                            else:
                                full = len(walk.nodes) - n_process >= cap
                            if full:
                                walk.truncated = True
                                continue
                            if free:
                                n_process += 1
                            walk.nodes[to] = {"level": lvl, "floor": None, "row": nrow}
                            (stack if free else nxt).append(to)
                        walk.edges[r["id"]] = r
                        links.append((r["id"], frm, to))
                current = nxt
                level += 1
        self._floors(walk, links, list(start_ids))
        return walk

    @staticmethod
    def _floors(walk: Walk, links: list[tuple[str, str, str]], starts: list[str]) -> None:
        """Confidence floor of every node: the weakest edge on its strongest path from the start (relaxation over the accepted links)."""
        floor: dict[str, str] = {s: "verified" for s in starts if s in walk.nodes}
        for _ in range(len(links) + 1):
            moved = False
            for eid, frm, to in links:
                if frm not in floor:
                    continue
                cand = weakest(floor[frm], walk.edges[eid]["confidence"])
                if to not in floor or _RANK[cand] < _RANK[floor[to]]:
                    floor[to] = cand
                    moved = True
            if not moved:
                break
        for nid, info in walk.nodes.items():
            info["floor"] = floor.get(nid, "unknown")

    # ------------------------------------------------------------------------------------------------------------------------ history and diff
    def history(self, asset_id: str, *, limit: int = 50, cursor: int | None = None) -> tuple[list[dict[str, Any]], int | None]:
        """Attribute versions of an asset, newest first; the cursor is the id of the last row of the previous page."""
        limit = max(1, min(int(limit), 500))
        stmt = select(asset_versions).where(asset_versions.c.asset_id == asset_id)
        if cursor:
            stmt = stmt.where(asset_versions.c.id < cursor)
        with self.db.connect() as c:
            rows = fetch_all(c, stmt.order_by(asset_versions.c.id.desc()).limit(limit + 1))
        nxt = rows[limit - 1]["id"] if len(rows) > limit else None
        return rows[:limit], nxt

    def scan_row(self, scan_id: str) -> dict[str, Any]:
        """The ``scans`` row, ``NotFound`` if missing."""
        with self.db.connect() as c:
            row = fetch_one(c, select(scans).where(scans.c.id == scan_id))
        if row is None:
            raise NotFound(f"scan '{scan_id}' does not exist")
        return row

    def diff(self, from_scan: str, to_scan: str, *, keep: Callable[[str | None], bool] | None = None, limit: int = 500) -> dict[str, Any]:
        """Assets and edges added, removed and changed by the scans after ``from_scan`` up to and including ``to_scan`` (two scans of one source).

        *added*: first seen in that range; *removed*: marked removed in it (by a sweep or a merge); *changed*: an asset with a new attribute version in the range
        (column added / removed / retyped, other attributes) or an edge whose confidence / transformation / status changed. ``keep(workspace_id)`` filters by visibility.
        """
        a, b = self.scan_row(from_scan), self.scan_row(to_scan)
        if a["source_id"] != b["source_id"]:
            raise BadRequest("the two scans belong to different sources", details={"from": a["source_id"], "to": b["source_id"]})
        if a["version"] >= b["version"]:
            raise BadRequest("'from' must be an older scan than 'to'", details={"from_version": a["version"], "to_version": b["version"]})
        src = a["source_id"]
        with self.db.connect() as c:
            ids = [r[0] for r in c.execute(select(scans.c.id).where(scans.c.source_id == src, scans.c.version > a["version"], scans.c.version <= b["version"]))]
            ac, ec = assets.c, edges.c
            # Counting first (id and workspace only), listing second: the summary holds the true totals however long the lists are allowed to get.
            added_keys = [(r[0], r[1]) for r in c.execute(select(ac.id, ac.workspace_id).where(ac.source_id == src, ac.first_seen_scan.in_(ids)).order_by(ac.id))]
            removed_keys = [(r[0], r[1]) for r in c.execute(select(ac.id, ac.workspace_id).where(ac.source_id == src, ac.removed_scan.in_(ids), ac.status == "removed").order_by(ac.id))]
            n_added = [i for i, w in added_keys if keep is None or keep(w)]
            n_removed = [i for i, w in removed_keys if keep is None or keep(w)]
            added = self._rows_in(c, assets, n_added[:limit])
            removed = self._rows_in(c, assets, n_removed[:limit])
            added_ids = set(n_added) | {i for i, _ in added_keys}
            vrows = fetch_all(c, select(asset_versions).where(asset_versions.c.scan_id.in_(ids)).order_by(asset_versions.c.id))
            changed_map: dict[str, dict[str, Any]] = {}
            for v in vrows:
                if v["asset_id"] in added_ids or (v["changes"] or {}).get("created"):
                    continue
                cur = changed_map.setdefault(v["asset_id"], {})
                for k, val in (v["changes"] or {}).items():
                    cur.setdefault(k, [])
                    cur[k].extend(x for x in (val if isinstance(val, list) else [val]) if x not in cur[k])
            changed_rows = self.asset_rows(changed_map)
            changed_rows = {k: r for k, r in changed_rows.items() if r["source_id"] == src}
            e_added_keys = [tuple(r) for r in c.execute(select(ec.id, ec.src_id, ec.dst_id).where(ec.source_id == src, ec.first_seen_scan.in_(ids)).order_by(ec.id))]
            e_removed_keys = [tuple(r) for r in c.execute(select(ec.id, ec.src_id, ec.dst_id).where(ec.source_id == src, ec.removed_scan.in_(ids), ec.status == "removed").order_by(ec.id))]
            e_added_ids = {k[0] for k in e_added_keys}
            e_changed = []
            for r in fetch_all(c, select(edges).where(ec.source_id == src, cast(ec.attrs, String).like("%\"scan_id\"%")).limit(5000)):
                if r["id"] in e_added_ids:
                    continue
                hits = [h for h in (r["attrs"] or {}).get("changes", []) if h.get("scan_id") in ids]
                if hits:
                    e_changed.append({**r, "_changes": hits})
            workspace_of = self._workspaces(c, {x for k in [*e_added_keys, *e_removed_keys] for x in k[1:] if x} | {x for r in e_changed for x in (r["src_id"], r["dst_id"]) if x})

            def edge_visible(src_id: str | None, dst_id: str | None) -> bool:
                return keep is None or all(keep(workspace_of[x]) for x in (src_id, dst_id) if x in workspace_of)

            n_e_added = [k[0] for k in e_added_keys if edge_visible(k[1], k[2])]
            n_e_removed = [k[0] for k in e_removed_keys if edge_visible(k[1], k[2])]
            e_added = self._rows_in(c, edges, n_e_added[:limit])
            e_removed = self._rows_in(c, edges, n_e_removed[:limit])
            e_changed = [r for r in e_changed if edge_visible(r["src_id"], r["dst_id"])]
            endpoint_ids = {x for r in [*e_added, *e_removed, *e_changed[:limit]] for x in (r["src_id"], r["dst_id"], r["via_id"]) if x}
            endpoints = self.asset_rows(endpoint_ids, c)

        def asset_out(r: dict[str, Any]) -> dict[str, Any]:
            return {"id": r["id"], "type": r["type"], "name": r["name"], "path": r["path"], "workspace_id": r["workspace_id"], "status": r["status"]}

        def edge_out(r: dict[str, Any]) -> dict[str, Any]:
            return {"id": r["id"], "type": r["type"], "src": r["src_id"], "dst": r["dst_id"], "via": r["via_id"], "confidence": r["confidence"],
                    "src_name": (endpoints.get(r["src_id"]) or {}).get("name"), "dst_name": (endpoints.get(r["dst_id"]) or {}).get("name")}

        changed_all = [(k, v) for k, v in changed_map.items() if k in changed_rows and (keep is None or keep(changed_rows[k]["workspace_id"]))]
        summary = {"assets_added": len(n_added), "assets_removed": len(n_removed), "assets_changed": len(changed_all),
                   "edges_added": len(n_e_added), "edges_removed": len(n_e_removed), "edges_changed": len(e_changed)}
        return {
            "source_id": src,
            "from": {"scan_id": a["id"], "version": a["version"], "finished_at": a["finished_at"]},
            "to": {"scan_id": b["id"], "version": b["version"], "finished_at": b["finished_at"]},
            "assets": {"added": [asset_out(r) for r in added], "removed": [asset_out(r) for r in removed],
                       "changed": [{**asset_out(changed_rows[k]), "changes": v} for k, v in changed_all[:limit]]},
            "edges": {"added": [edge_out(r) for r in e_added], "removed": [edge_out(r) for r in e_removed],
                      "changed": [{**edge_out(r), "changes": r["_changes"]} for r in e_changed[:limit]]},
            "summary": summary,
            "truncated": any(v > limit for v in summary.values()),
        }

    @staticmethod
    def _rows_in(c: Connection, table: Any, ids: Sequence[str]) -> list[dict[str, Any]]:
        """Full rows of ``table`` for ``ids``, in the order of ``ids``."""
        got: dict[str, dict[str, Any]] = {}
        for i in range(0, len(ids), 300):
            for r in fetch_all(c, select(table).where(table.c.id.in_(list(ids[i:i + 300])))):
                got[r["id"]] = r
        return [got[i] for i in ids if i in got]

    @staticmethod
    def _workspaces(c: Connection, ids: Iterable[str]) -> dict[str, str | None]:
        """``{asset id: workspace id}`` for the ids that exist (a lighter lookup than :meth:`asset_rows`)."""
        wanted = list(dict.fromkeys(ids))
        out: dict[str, str | None] = {}
        for i in range(0, len(wanted), 300):
            for r in c.execute(select(assets.c.id, assets.c.workspace_id).where(assets.c.id.in_(wanted[i:i + 300]))):
                out[r[0]] = r[1]
        return out

    # ------------------------------------------------------------------------------------------------------------------------ maintenance
    def purge_versions(self, keep_per_asset: int = 200, *, batch: int = 50, after_batch: Callable[[], None] | None = None) -> int:
        """Trim very long attribute histories (oldest versions first). Returns rows deleted. Assets are trimmed ``batch`` at a time, each batch in a write transaction of its own,
        and ``after_batch`` is called between batches (the engine's writer pause), so a long history backlog never holds the SQLite writer for long."""
        n = 0
        with self.db.connect() as conn:
            over = [r[0] for r in conn.execute(select(asset_versions.c.asset_id).group_by(asset_versions.c.asset_id).having(func.count() > keep_per_asset)).all()]
        for i in range(0, len(over), max(1, batch)):
            with self.db.begin() as conn:
                for aid in over[i:i + batch]:
                    cut = conn.execute(select(asset_versions.c.id).where(asset_versions.c.asset_id == aid).order_by(asset_versions.c.id.desc()).offset(keep_per_asset).limit(1)).first()
                    if cut:
                        n += conn.execute(delete(asset_versions).where(asset_versions.c.asset_id == aid, asset_versions.c.id <= cut[0])).rowcount
            if after_batch is not None:
                after_batch()
        return n


def cutoff_iso(days: float) -> str:
    """ISO timestamp ``days`` days in the past."""
    return iso_in(-days * 86400)


def parse_iso(value: str | None) -> Any:
    """``from_iso`` that tolerates ``None``."""
    return from_iso(value) if value else None


__all__ = ["MAX_EVIDENCE", "TABLE_LEVEL_EDGES", "VOLATILE_ATTRS", "AssetResult", "EdgeResult", "GraphStore", "Walk", "allowed_confidences", "attr_changes",
           "attrs_hash", "cutoff_iso", "evidence_dict", "evidence_key", "merge_evidence", "parse_iso", "strongest_confidence", "weakest"]
