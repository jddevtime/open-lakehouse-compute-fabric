# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``IntelligenceService``: what other code calls (``ctx.service("intelligence")``) and what the REST routes delegate to.

Authorisation (contract section 8): every asset carries a ``workspace_id``. Reads are filtered with ``principal.visible_workspaces("intelligence.read")``;
an asset with ``workspace_id = None`` (external / tenant level) needs an organisation-wide ``intelligence.read``. An asset outside the caller's scope does not
exist for the caller: ``NotFound``, never ``Forbidden``. A caller without any ``intelligence.read`` binding gets ``Forbidden`` from list-style calls.

Sub-services register themselves on the same object from their package's ``install`` hook: ``service.discovery`` (sources, scans, sampling; ``sample(principal,
asset_row, rows=..., columns=...)`` serves ``GET /intelligence/assets/{id}/sample``) and ``service.governance``. Governance may implement any of these optional
duck-typed methods; the core calls them when present and works without them:

* ``decorate(principal, asset_ids) -> {asset_id: {"domain", "certification", "classifications", "owners", "glossary_terms", "data_product", ...}}``
* ``decorate_compact(principal, asset_ids) -> {asset_id: {"classifications", "certification", "domain", "quality_score", "owners"}}`` (list cards)
* ``owners_of(principal, asset_ids) -> {asset_id: [owner, ...]}``
* ``asset_filter(principal, *, domain, classification, certification, owner) -> set[str]`` (asset ids that match every given filter)
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import String, and_, cast, false, func, or_, select, update

from olcf.core.errors import BadRequest, Forbidden, NotFound, OlcfError, PreconditionFailed, Unsupported
from olcf.core.timeutil import now_iso
from olcf.db.util import decode_cursor, encode_cursor, fetch_all, new_etag, paginate
from olcf.intelligence import model as M
from olcf.intelligence import settings as rt
from olcf.intelligence.engine import RUNTIME_SOURCE, Engine, scan_stats
from olcf.intelligence.graph import GraphStore
from olcf.intelligence.ingest import requeue_failed
from olcf.intelligence.queries import Queries, _gap_out
from olcf.intelligence.registry import AssetRegistry, Resolution
from olcf.intelligence.schema import asset_aliases, assets, edges, gaps, inbox, scans
from olcf.security.rbac import Principal

log = logging.getLogger("olcf.intelligence")

MAX_TAGS = 32
MAX_DESCRIPTION = 8000
ASSET_STATUSES = ("active", "removed", "placeholder")
FILTER_ID_CAP = 20_000
"""Most asset ids a governance filter (domain, classification, certification, owner) may carry into one query; beyond it the response says ``truncated_filter: true``."""
FILTER_SCAN_CAP = 500_000
"""Most candidate ids the id-only pre-scan reads when a governance filter matches a long id list."""
_IN_CHUNK = 500


def _parts(items: list[str], size: int) -> list[list[str]]:
    """``items`` cut into lists of at most ``size`` (keeps each ``IN`` list small)."""
    return [items[i:i + size] for i in range(0, len(items), size)]


def _like(s: str) -> str:
    return "%" + s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"


class IntelligenceService:
    """The Intelligence facade: identity (``registry``), storage and traversal (``graph``), fact ingestion (``engine``), visibility-aware reads and the analysis
    queries. ``discovery`` and ``governance`` are filled in by their packages when they are installed."""

    def __init__(self, ctx: Any, registry: AssetRegistry, graph: GraphStore, engine: Engine) -> None:
        self.ctx, self.db = ctx, ctx.db
        self.registry, self.graph, self.engine = registry, graph, engine
        self.discovery: Any = None
        self.governance: Any = None
        self.inbox_writer: Any = None
        self.inbox_processor: Any = None
        self.queries = Queries(self)

    # ------------------------------------------------------------------------------------------------------------------------ settings
    def setting(self, key: str, default: Any) -> Any:
        """Value of an Intelligence runtime setting."""
        return rt.get(self.ctx, key, default)

    def max_nodes(self) -> int:
        """Node cap of lineage-like answers (``intelligence.max_lineage_nodes``)."""
        return int(self.setting("intelligence.max_lineage_nodes", 500))

    def max_impact_nodes(self) -> int:
        """Cap of the dedicated impact traversal (``intelligence.max_impact_nodes``): real totals up to this many downstream assets."""
        return int(self.setting("intelligence.max_impact_nodes", 20000))

    # ------------------------------------------------------------------------------------------------------------------------ visibility
    def scope(self, principal: Principal) -> set[str] | None:
        """Workspaces whose assets the caller may read (``None`` = organisation-wide, which also covers assets without a workspace). Raises ``Forbidden`` when the
        caller has no ``intelligence.read`` anywhere."""
        vis = principal.visible_workspaces("intelligence.read")
        if vis is not None and not vis:
            raise Forbidden("permission 'intelligence.read' is required", details={"permission": "intelligence.read"})
        return vis

    def can_see(self, principal: Principal, workspace_id: str | None) -> bool:
        """True if an asset of ``workspace_id`` is visible to the caller."""
        vis = principal.visible_workspaces("intelligence.read")
        if vis is None:
            return True
        return workspace_id is not None and workspace_id in vis

    def keep_node(self, principal: Principal) -> Any:
        """A predicate over asset rows (for graph walks) that admits only visible assets."""
        vis = principal.visible_workspaces("intelligence.read")

        def keep(row: dict[str, Any]) -> bool:
            return vis is None or (row["workspace_id"] is not None and row["workspace_id"] in vis)

        return keep

    def scope_filter(self, stmt: Any, principal: Principal, workspace_id: str | None) -> Any:
        """Add the visibility (and an optional ``workspace_id`` filter) to a statement over ``assets``."""
        vis = self.scope(principal)
        col = assets.c.workspace_id
        if workspace_id:
            if vis is not None and workspace_id not in vis:
                return stmt.where(false())
            return stmt.where(col == workspace_id)
        if vis is not None:
            return stmt.where(col.in_(sorted(vis)))
        return stmt

    def row_for(self, principal: Principal, asset_id: str) -> dict[str, Any]:
        """The asset row if the caller may see it; otherwise ``NotFound`` (existence is hidden). A merged-away asset id leads to the asset it was merged into."""
        self.scope(principal)
        row = self.graph.asset_rows([asset_id]).get(asset_id)
        hops = 0
        while row is not None and row["status"] == "removed" and (row["attrs"] or {}).get("merged_into") and hops < 5:
            row = self.graph.asset_rows([row["attrs"]["merged_into"]]).get(row["attrs"]["merged_into"])
            hops += 1
        if row is None or not self.can_see(principal, row["workspace_id"]):
            raise NotFound(f"asset '{asset_id}' does not exist")
        return row

    def visible_asset_ids(self, principal: Principal, ids: list[str] | None = None, *, limit: int = 100_000) -> set[str]:
        """The subset of ``ids`` the caller may see; with ``ids=None`` the ids of (up to ``limit``) visible, non-removed assets."""
        stmt = select(assets.c.id)
        if ids is not None:
            if not ids:
                return set()
            stmt = stmt.where(assets.c.id.in_(ids[:50_000]))
        else:
            stmt = stmt.where(assets.c.status != "removed")
        stmt = self.scope_filter(stmt, principal, None).limit(limit)
        with self.db.connect() as c:
            return {r[0] for r in c.execute(stmt)}

    def visible_sources(self, principal: Principal) -> set[str]:
        """Sources (discovery sources, ``runtime``) that have at least one asset the caller can see; all sources for an organisation-wide reader."""
        vis = self.scope(principal)
        with self.db.connect() as c:
            if vis is None:
                return {r[0] for r in c.execute(select(scans.c.source_id).distinct())}
            return {r[0] for r in c.execute(select(assets.c.source_id).where(assets.c.workspace_id.in_(sorted(vis)), assets.c.source_id.is_not(None)).distinct())}

    # ------------------------------------------------------------------------------------------------------------------------ governance hooks
    def governance_info(self, principal: Principal, ids: list[str]) -> dict[str, dict[str, Any]]:
        """Governance facts per asset (domain, certification, owners ...) when the governance package is installed, else ``{}``."""
        fn = getattr(self.governance, "decorate", None)
        if fn is None or not ids:
            return {}
        try:
            return dict(fn(principal, ids) or {})
        except Exception:  # noqa: BLE001 - governance trouble never breaks a read
            log.exception("governance decoration failed")
            return {}

    def governance_compact(self, principal: Principal, ids: list[str]) -> dict[str, dict[str, Any]]:
        """Compact governance facts for list cards (``classifications``, ``certification``, ``domain``, ``quality_score``, ``owners``) per asset, in one bulk call.

        Uses the optional ``decorate_compact`` of the governance package; ``{}`` when it is not installed or fails.
        """
        fn = getattr(self.governance, "decorate_compact", None)
        if fn is None or not ids:
            return {}
        try:
            return dict(fn(principal, ids) or {})
        except Exception:  # noqa: BLE001 - governance trouble never breaks a read
            log.exception("governance list decoration failed")
            return {}

    def owners_of(self, principal: Principal, ids: list[str]) -> dict[str, list[Any]]:
        """Owners per asset (empty when governance is not installed)."""
        fn = getattr(self.governance, "owners_of", None)
        if fn is not None and ids:
            try:
                return dict(fn(principal, ids) or {})
            except Exception:  # noqa: BLE001
                log.exception("governance owner look-up failed")
                return {}
        return {k: v["owners"] for k, v in self.governance_info(principal, ids).items() if v.get("owners")}

    # ------------------------------------------------------------------------------------------------------------------------ identity
    def resolve(self, ref: M.AssetRef, create: bool = True) -> str:
        """The ``ast_`` id of a reference (see ``AssetRegistry.resolve``); creates a placeholder when nothing matches and ``create`` is true."""
        return self.registry.resolve(ref, create=create)

    def resolve_for(self, principal: Principal, ref: M.AssetRef, *, create: bool = False) -> dict[str, Any]:
        """Resolve a reference for a caller (``POST /intelligence/resolve``): the matching visible asset; ``create`` (a placeholder) needs ``intelligence.manage``."""
        self.scope(principal)
        if create:
            ws = ref.scope_map.get("workspace_id")
            principal.require("intelligence.manage", workspace_id=ws)
            res = self.registry.resolve_ex(ref, create=True, source_id="api")
        else:
            res = self.registry.resolve_ex(ref, create=False)
        row = self.graph.asset_rows([res.asset_id]).get(res.asset_id)
        if row is None or not self.can_see(principal, row["workspace_id"]):
            raise NotFound(f"no asset matches {self.registry._describe(ref)}")  # noqa: SLF001
        return {"asset": self.public(row), "created": res.created, "placeholder": res.placeholder, "via": res.via}

    # ------------------------------------------------------------------------------------------------------------------------ assets
    @staticmethod
    def public(row: dict[str, Any]) -> dict[str, Any]:
        """The API form of an ``assets`` row."""
        return {
            "id": row["id"], "type": row["type"], "name": row["name"], "path": row["path"], "workspace_id": row["workspace_id"], "source_id": row["source_id"],
            "parent_id": row["parent_id"], "description": row["description"] or "", "description_source": row["description_source"], "tags": row["tags"] or [],
            "status": row["status"], "placeholder": row["status"] == "placeholder", "attrs": row["attrs"] or {}, "first_seen_scan": row["first_seen_scan"],
            "last_seen_scan": row["last_seen_scan"], "removed_scan": row["removed_scan"], "created_at": row["created_at"], "updated_at": row["updated_at"], "etag": row["etag"],
        }

    def get_asset(self, principal: Principal, asset_id: str) -> dict[str, Any]:
        """One asset with its aliases, parent, child count, edge counts, open gaps and (when governance is installed) its governance facts."""
        row = self.row_for(principal, asset_id)
        out = self.public(row)
        with self.db.connect() as c:
            out["aliases"] = [{"kind": k, "value": v} for k, v in self.registry.aliases_of(row["id"], c)]
            out["child_count"] = int(c.execute(select(func.count()).select_from(assets).where(assets.c.parent_id == row["id"], assets.c.status != "removed")).scalar() or 0)
            e = edges.c
            flows_in = ["WRITES_TO", "TRANSFORMS"]
            flows_out = ["READS_FROM", "DEPENDS_ON", "USES_MEASURE", "DISPLAYS"]
            up = c.execute(select(func.count()).select_from(edges).where(e.status == "active", or_(and_(e.dst_id == row["id"], e.type.in_(flows_in)), and_(e.src_id == row["id"], e.type.in_(flows_out))))).scalar()
            down = c.execute(select(func.count()).select_from(edges).where(e.status == "active", or_(and_(e.src_id == row["id"], e.type.in_(flows_in)), and_(e.dst_id == row["id"], e.type.in_(flows_out))))).scalar()
            out["lineage_edges"] = {"upstream": int(up or 0), "downstream": int(down or 0)}
            out["open_gaps"] = int(c.execute(select(func.count()).select_from(gaps).where(gaps.c.asset_id == row["id"], gaps.c.resolved_at.is_(None))).scalar() or 0)
        if row["parent_id"]:
            parent = self.graph.asset_rows([row["parent_id"]]).get(row["parent_id"])
            if parent is not None and self.can_see(principal, parent["workspace_id"]):
                out["parent"] = {"id": parent["id"], "type": parent["type"], "name": parent["name"]}
        gov = self.governance_info(principal, [row["id"]]).get(row["id"])
        if gov:
            out["governance"] = gov
        return out

    def _filtered(self, principal: Principal, *, q: str | None = None, type: str | None = None, workspace_id: str | None = None, domain: str | None = None,
                  classification: str | None = None, certification: str | None = None, owner: str | None = None, tag: str | None = None, status: str | None = None,
                  parent_id: str | None = None, source_id: str | None = None, meta: dict[str, Any] | None = None) -> Any:
        t = assets.c
        stmt = select(assets)
        stmt = self.scope_filter(stmt, principal, workspace_id)
        if type:
            kinds = [x for x in type.split(",") if x]
            bad = [k for k in kinds if k not in M.ASSET_TYPES]
            if bad:
                raise BadRequest(f"unknown asset type(s) {bad}", details={"supported": list(M.ASSET_TYPES)})
            stmt = stmt.where(t.type.in_(kinds))
        if status and status != "any":
            if status not in ASSET_STATUSES:
                raise BadRequest(f"status must be one of {[*ASSET_STATUSES, 'any']}")
            stmt = stmt.where(t.status == status)
        elif not status:
            stmt = stmt.where(t.status != "removed")
        if parent_id:
            stmt = stmt.where(t.parent_id == parent_id)
        if source_id:
            stmt = stmt.where(t.source_id == source_id)
        if q:
            for term in [x for x in q.lower().split() if x][:8]:
                like = _like(term)
                stmt = stmt.where(or_(func.lower(t.name).like(like, escape="\\"), func.lower(func.coalesce(t.path, "")).like(like, escape="\\"),
                                      func.lower(t.description).like(like, escape="\\"),
                                      t.id.in_(select(asset_aliases.c.asset_id).where(func.lower(asset_aliases.c.value).like(like, escape="\\")))))
        if tag:
            tg = tag.strip().lower()
            stmt = stmt.where(or_(func.lower(cast(t.tags, String)).like(f'%"{tg}"%'), func.lower(cast(t.attrs, String)).like(f'%"{tg}"%')))
        if any((domain, classification, certification, owner)):
            fn = getattr(self.governance, "asset_filter", None)
            if fn is None:
                raise Unsupported("filtering by domain, classification, certification or owner needs the governance package",
                                  remediation="install a build that includes olcf.intelligence.governance, or drop these filters")
            ids = sorted(fn(principal, domain=domain, classification=classification, certification=certification, owner=owner))
            ids, truncated = self._narrow_ids(stmt, ids)
            if truncated:
                log.warning("governance filter matched more than %d assets; the result is truncated", FILTER_ID_CAP)
                if meta is not None:
                    meta["truncated_filter"] = True
            stmt = stmt.where(or_(*[t.id.in_(part) for part in _parts(ids, _IN_CHUNK)])) if ids else stmt.where(false())
        return stmt

    def _narrow_ids(self, stmt: Any, ids: list[str]) -> tuple[list[str], bool]:
        """Ids from a governance filter, cut down to the ones the other filters can match; ``(ids, truncated)``.

        A short list goes straight into the query. A long one is first intersected with the ids of the assets that pass the scope and every other filter (a cheap id-only query), so
        only matches that exist are carried. ``truncated`` is true when matches remain beyond :data:`FILTER_ID_CAP` (or the id-only scan hit :data:`FILTER_SCAN_CAP`), i.e. when the
        result may be missing assets that match.
        """
        if len(ids) <= _IN_CHUNK:
            return ids, False
        truncated = False
        with self.db.connect() as c:
            seen = {r[0] for r in c.execute(stmt.with_only_columns(assets.c.id).order_by(None).limit(FILTER_SCAN_CAP + 1))}
        if len(seen) > FILTER_SCAN_CAP:
            truncated = True
        ids = [i for i in ids if i in seen]
        if len(ids) > FILTER_ID_CAP:
            ids, truncated = ids[:FILTER_ID_CAP], True
        return ids, truncated

    def list_assets(self, principal: Principal, *, q: str | None = None, type: str | None = None, workspace_id: str | None = None, domain: str | None = None,
                    classification: str | None = None, certification: str | None = None, owner: str | None = None, tag: str | None = None, status: str | None = None,
                    limit: int = 50, cursor: str | None = None, meta: dict[str, Any] | None = None) -> tuple[list[dict[str, Any]], str | None]:
        """Visible assets matching the filters, oldest first, with a keyset cursor. ``q`` matches name, path, description and aliases (every word must match).

        With governance installed each item carries a compact ``governance`` object (one bulk call per page, see :meth:`governance_compact`). ``meta``, when given, receives
        ``truncated_filter: True`` if a governance filter matched more assets than :data:`FILTER_ID_CAP` and the result may therefore be incomplete.
        """
        stmt = self._filtered(principal, q=q, type=type, workspace_id=workspace_id, domain=domain, classification=classification, certification=certification, owner=owner,
                              tag=tag, status=status, meta=meta)
        with self.db.connect() as c:
            rows, nxt = paginate(c, stmt, assets, limit=limit, cursor=cursor)
        items = [self.public(r) for r in rows]
        compact = self.governance_compact(principal, [r["id"] for r in rows])
        for item in items:
            if item["id"] in compact:
                item["governance"] = compact[item["id"]]
        return items, nxt

    def search(self, principal: Principal, *, q: str, type: str | None = None, workspace_id: str | None = None, limit: int = 25, **filters: Any) -> list[dict[str, Any]]:
        """Ranked search: exact alias 90, exact name 100, name prefix 60, name contains 40, path 20, description 10 (summed over the words)."""
        terms = [x for x in q.lower().split() if x][:8]
        if not terms:
            raise BadRequest("q must contain at least one search term")
        stmt = self._filtered(principal, q=q, type=type, workspace_id=workspace_id, **filters).limit(500)
        with self.db.connect() as c:
            rows = fetch_all(c, stmt)
        direct: set[str] = set()
        if len(terms) == 1:
            hits = self.registry.lookup([(k, q.strip()) for k in M.ALIAS_KINDS])
            direct = set(hits.values())
        out = []
        for r in rows:
            name = r["name"].lower()
            score = 90 if r["id"] in direct else 0
            for t in terms:
                score += 100 if name == t else 60 if name.startswith(t) else 40 if t in name else 0
                score += 20 if t in (r["path"] or "").lower() else 0
                score += 10 if t in (r["description"] or "").lower() else 0
            out.append({**self.public(r), "score": score})
        out.sort(key=lambda r: (-r["score"], r["name"]))
        return out[: max(1, min(int(limit), 100))]

    def children(self, principal: Principal, asset_id: str, *, type: str | None = None, status: str | None = None, limit: int = 100,
                 cursor: str | None = None) -> tuple[list[dict[str, Any]], str | None]:
        """Child assets (tables of a lakehouse, columns of a table ...), by name."""
        row = self.row_for(principal, asset_id)
        stmt = self._filtered(principal, type=type, status=status, parent_id=row["id"])
        limit = max(1, min(int(limit), 500))
        cur = decode_cursor(cursor)
        if cur:
            stmt = stmt.where(or_(func.lower(assets.c.name) > cur[0], and_(func.lower(assets.c.name) == cur[0], assets.c.id > cur[1])))
        with self.db.connect() as c:
            rows = fetch_all(c, stmt.order_by(func.lower(assets.c.name), assets.c.id).limit(limit + 1))
        nxt = encode_cursor([rows[limit - 1]["name"].lower(), rows[limit - 1]["id"]]) if len(rows) > limit else None
        return [self.public(r) for r in rows[:limit]], nxt

    @staticmethod
    def _check_if_match(if_match: str | None, etag: str) -> None:
        if if_match is None:
            return
        candidates = {x.strip().strip('"') for x in if_match.split(",")}
        if "*" not in candidates and etag not in candidates:
            raise PreconditionFailed("asset was modified by someone else", details={"current_etag": etag})

    def update_asset(self, principal: Principal, asset_id: str, *, description: str | None = None, tags: list[str] | None = None, if_match: str | None = None) -> dict[str, Any]:
        """Describe / tag an asset (``governance.curate`` in its workspace; ``If-Match`` guards against lost updates). A manual description is never overwritten by a
        scan; an empty description hands the field back to the scans."""
        row = self.row_for(principal, asset_id)
        principal.require("governance.curate", workspace_id=row["workspace_id"])
        self._check_if_match(if_match, row["etag"])
        vals: dict[str, Any] = {}
        if description is not None:
            if len(description) > MAX_DESCRIPTION:
                raise BadRequest(f"description is limited to {MAX_DESCRIPTION} characters")
            if description.strip():
                vals.update(description=description, description_source="manual")
            else:
                vals.update(description="", description_source="scan")
        if tags is not None:
            if not isinstance(tags, list) or not all(isinstance(x, str) for x in tags):
                raise BadRequest("tags must be a list of strings")
            clean = sorted({x.strip().lower() for x in tags if x.strip()})
            if len(clean) > MAX_TAGS or any(len(x) > 64 for x in clean):
                raise BadRequest(f"at most {MAX_TAGS} tags of up to 64 characters")
            vals["tags"] = clean
        if not vals:
            raise BadRequest("nothing to update", remediation="send description and/or tags")
        vals.update(updated_at=now_iso(), etag=new_etag())
        with self.db.begin() as c:
            n = c.execute(update(assets).where(assets.c.id == row["id"], assets.c.etag == row["etag"]).values(**vals)).rowcount
        if n == 0:
            cur = self.graph.asset_rows([row["id"]]).get(row["id"]) or row
            raise PreconditionFailed("asset was modified concurrently", details={"current_etag": cur["etag"]})
        self.ctx.audit.record(principal, "intelligence.asset_updated", resource_type="asset", resource_id=row["id"], workspace_id=row["workspace_id"],
                              details={"fields": sorted(k for k in vals if k not in ("updated_at", "etag", "description_source"))})
        return self.get_asset(principal, row["id"])

    # ------------------------------------------------------------------------------------------------------------------------ analysis
    def lineage(self, principal: Principal, asset_id: str, *, direction: str = "both", depth: int = 3, level: str = "table", min_confidence: str | None = None,
                types: list[str] | None = None, column: str | None = None, include_removed: bool = False) -> dict[str, Any]:
        """Lineage graph around an asset (see :meth:`Queries.lineage`)."""
        return self.queries.lineage(principal, asset_id, direction=direction, depth=depth, level=level, min_confidence=min_confidence, types=types, column=column,
                                    include_removed=include_removed)

    def origin(self, principal: Principal, asset_id: str, **kw: Any) -> dict[str, Any]:
        """Where the asset's data comes from (upstream roots, paths, confidence per path)."""
        return self.queries.origin(principal, asset_id, **kw)

    def impact(self, principal: Principal, asset_id: str, **kw: Any) -> dict[str, Any]:
        """Downstream consumers grouped by type, reports and visuals included."""
        return self.queries.impact(principal, asset_id, **kw)

    def staleness(self, principal: Principal, asset_id: str, **kw: Any) -> dict[str, Any]:
        """Freshness of the asset and the most upstream stale producer."""
        return self.queries.staleness(principal, asset_id, **kw)

    def unused(self, principal: Principal, **kw: Any) -> dict[str, Any]:
        """Tables with no downstream consumer within a window."""
        return self.queries.unused(principal, **kw)

    def documentation(self, principal: Principal, asset_id: str) -> dict[str, Any]:
        """Generated Markdown documentation of an asset."""
        return self.queries.documentation(principal, asset_id)

    def summary(self, principal: Principal) -> dict[str, Any]:
        """Overview counts for what the caller can see."""
        return self.queries.summary(principal)

    def history(self, principal: Principal, asset_id: str, *, limit: int = 50, cursor: str | None = None, schema_only: bool = False) -> dict[str, Any]:
        """Attribute versions of an asset, newest first (``schema_only`` keeps those that added, removed or retyped columns)."""
        row = self.row_for(principal, asset_id)
        cur = decode_cursor(cursor)
        items, nxt = self.graph.history(row["id"], limit=limit * (4 if schema_only else 1), cursor=int(cur[0]) if cur else None)
        if schema_only:
            items = [v for v in items if any(k.startswith("columns_") for k in (v["changes"] or {}))][:limit]
        out = [{"id": v["id"], "scan_id": v["scan_id"], "created_at": v["created_at"], "attrs_hash": v["attrs_hash"], "changes": v["changes"], "attrs": v["attrs"]} for v in items]
        return {"asset_id": row["id"], "items": out, "next_cursor": encode_cursor([nxt]) if nxt else None}

    # ------------------------------------------------------------------------------------------------------------------------ edges, snapshots
    def edge_detail(self, principal: Principal, edge_id: str) -> dict[str, Any]:
        """One edge with all of its (capped) evidence; hidden unless both ends are visible."""
        self.scope(principal)
        r = self.graph.get_edge(edge_id)
        ends = self.graph.asset_rows([x for x in (r["src_id"], r["dst_id"], r["via_id"]) if x])
        if not all(self.can_see(principal, ends[x]["workspace_id"]) for x in (r["src_id"], r["dst_id"]) if x in ends) or r["src_id"] not in ends or r["dst_id"] not in ends:
            raise NotFound(f"edge '{edge_id}' does not exist")
        from olcf.intelligence.queries import _edge_out, _node_out

        out = _edge_out(r)
        out.update(evidence=r["evidence"] or [], attrs={k: v for k, v in (r["attrs"] or {}).items() if k != "changes"}, changes=(r["attrs"] or {}).get("changes", []),
                   first_seen_at=r["first_seen_at"], source_id=r["source_id"], first_seen_scan=r["first_seen_scan"], last_seen_scan=r["last_seen_scan"], removed_scan=r["removed_scan"],
                   src_asset=_node_out(ends[r["src_id"]]), dst_asset=_node_out(ends[r["dst_id"]]))
        if r["via_id"] in ends and self.can_see(principal, ends[r["via_id"]]["workspace_id"]):
            out["via_asset"] = _node_out(ends[r["via_id"]])
        return out

    def snapshots(self, principal: Principal, *, source_id: str | None = None, status: str | None = None, limit: int = 50, cursor: str | None = None,
                  include_retired: bool = False) -> tuple[list[dict[str, Any]], str | None]:
        """Scans (metadata snapshots) of the sources the caller can see, newest first. The closing scan that retires a deleted discovery source (``trigger = "retired"``) is left
        out unless ``include_retired``."""
        allowed = self.visible_sources(principal)
        stmt = select(scans).where(scans.c.source_id.in_(sorted(allowed)) if allowed else false())
        if source_id:
            stmt = stmt.where(scans.c.source_id == source_id)
        if status:
            stmt = stmt.where(scans.c.status == status)
        if not include_retired:
            stmt = stmt.where(scans.c.trigger != "retired")
        with self.db.connect() as c:
            rows, nxt = paginate(c, stmt, scans, limit=limit, cursor=cursor, descending=True)
        return [{**{k: r[k] for k in ("id", "source_id", "mode", "trigger", "status", "version", "started_at", "finished_at", "error")}, "stats": scan_stats(r["stats"])}
                for r in rows], nxt

    def diff(self, principal: Principal, from_scan: str, to_scan: str, *, limit: int = 500) -> dict[str, Any]:
        """Assets and edges added, removed and changed between two scans of one source."""
        allowed = self.visible_sources(principal)
        for sid in (from_scan, to_scan):
            if self.graph.scan_row(sid)["source_id"] not in allowed:
                raise NotFound(f"scan '{sid}' does not exist")
        return self.graph.diff(from_scan, to_scan, keep=lambda ws: self.can_see(principal, ws), limit=limit)

    def gaps(self, principal: Principal, *, limit: int = 100) -> list[dict[str, Any]]:
        """Open gaps about assets the caller can see."""
        vis = self.scope_filter(select(assets.c.id), principal, None)
        with self.db.connect() as c:
            rows = fetch_all(c, select(gaps).where(gaps.c.resolved_at.is_(None), gaps.c.asset_id.in_(vis)).order_by(gaps.c.id.desc()).limit(max(1, min(limit, 500))))
        return [_gap_out(g) for g in rows]

    def identity_conflicts(self, principal: Principal) -> list[dict[str, Any]]:
        """Read-only diagnostic: visible assets that hold two different authoritative identities (conflated by an older build). See :meth:`AssetRegistry.identity_conflicts`."""
        return [r for r in self.registry.identity_conflicts() if self.can_see(principal, r["workspace_id"])]

    # ------------------------------------------------------------------------------------------------------------------------ sampling
    def sample(self, principal: Principal, asset_id: str, *, rows: int | None = None, columns: list[str] | None = None) -> dict[str, Any]:
        """Policy-limited, masked sample rows of an asset, on demand (``intelligence.sample`` in its workspace). Values are never stored; every call is audited
        (counts only). Needs the discovery package's sampling service."""
        row = self.row_for(principal, asset_id)
        principal.require("intelligence.sample", workspace_id=row["workspace_id"])
        fn = getattr(self.discovery, "sample", None)
        if fn is None:
            raise Unsupported("sampling is not available: the discovery package (sampling service) is not installed", remediation="install a build that includes olcf.intelligence.discovery and enable a sampling policy for the workspace")
        try:
            out = fn(principal, row, rows=rows, columns=columns)
        except OlcfError as exc:
            self.ctx.audit.record(principal, "intelligence.sample_read", resource_type="asset", resource_id=row["id"], workspace_id=row["workspace_id"], outcome="denied",
                                  details={"reason": exc.code, "message": exc.message[:200]})
            raise
        n = len(out.get("rows") or []) if isinstance(out, dict) else 0
        self.ctx.audit.record(principal, "intelligence.sample_read", resource_type="asset", resource_id=row["id"], workspace_id=row["workspace_id"],
                              details={"rows": n, "columns": len(out.get("columns") or []) if isinstance(out, dict) else 0, "masking": (out or {}).get("masking") if isinstance(out, dict) else None})
        return dict(out)

    # ------------------------------------------------------------------------------------------------------------------------ operations
    def inbox_stats(self) -> dict[str, Any]:
        """Inbox depth by status and the age of the oldest pending row (seconds)."""
        from olcf.core.timeutil import age_seconds

        with self.db.connect() as c:
            counts = {s: n for s, n in c.execute(select(inbox.c.status, func.count()).group_by(inbox.c.status))}
            oldest = c.execute(select(func.min(inbox.c.received_at)).where(inbox.c.status == "pending")).scalar()
        return {"pending": counts.get("pending", 0), "done": counts.get("done", 0), "failed": counts.get("failed", 0), "skipped": counts.get("skipped", 0),
                "oldest_pending_seconds": age_seconds(oldest)}

    def requeue_inbox(self, principal: Principal) -> dict[str, Any]:
        """Queue every ``failed`` inbox row for a fresh set of attempts (operator action, ``intelligence.manage``); returns the count and the inbox depth afterwards."""
        principal.require("intelligence.manage")
        n = requeue_failed(self.db)
        self.ctx.audit.record(principal, "intelligence.inbox_requeued", resource_type="inbox", resource_id="inbox", details={"requeued": n})
        return {"requeued": n, "inbox": self.inbox_stats()}

    def resolution_of(self, res: Resolution) -> dict[str, Any]:
        """JSON form of a :class:`~olcf.intelligence.registry.Resolution`."""
        return {"asset_id": res.asset_id, "created": res.created, "placeholder": res.placeholder, "via": res.via}


__all__ = ["RUNTIME_SOURCE", "IntelligenceService"]
