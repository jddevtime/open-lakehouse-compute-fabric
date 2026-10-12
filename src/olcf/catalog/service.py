# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The ``catalog`` service: discovered tables, search, statistics, lineage and default-lakehouse resolution.

Design notes
------------
* Storage is the source of truth for physical facts.  :meth:`CatalogService.sync_lakehouse` projects what it finds
  into ``catalog_entries`` (keeping human-written descriptions/tags), marks vanished tables ``missing`` instead of
  deleting them, and records basic statistics.  Nothing here ever moves bulk data.
* Authorisation is always evaluated against the workspace stored on the *row* (``workspace_id`` of the entry / edge),
  never a client-supplied one.
* Lineage edges are deduplicated by a hash of ``(workspace, source, target, via)``; repeated runs bump ``run_count`` and
  ``last_seen_at``.  Column mappings are only stored when proven (see :mod:`olcf.catalog.lineage`).
* When a re-discovery finds that the columns of an already catalogued table differ from the last known entry, the platform event ``dataset.schema_changed``
  (``{workspace_id, lakehouse_id, table, columns_added, columns_removed, columns_changed, version, at}``) is published, best effort (see :mod:`olcf.core.dataset_events`).
  A table seen for the first time, or one whose columns were never read before, produces no event.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections import deque
from typing import TYPE_CHECKING, Any

from sqlalchemy import String, cast, delete, func, insert, or_, select, update

from olcf.catalog import lineage as lin
from olcf.catalog.schema import catalog_entries, lineage_edges, table_stats
from olcf.core import dataset_events as dsev
from olcf.core.errors import BadRequest, Forbidden, NotFound
from olcf.core.ids import new_id
from olcf.core.timeutil import now_iso
from olcf.db import schema as S
from olcf.db.util import fetch_all, fetch_one, new_etag, paginate
from olcf.security.rbac import Principal

if TYPE_CHECKING:
    from olcf.core.context import AppContext
    from olcf.storage.lakehouse import Lakehouse

log = logging.getLogger("olcf.catalog")

MAX_LINEAGE_EDGES = 500
MAX_TAGS = 32
VIA_KINDS = ("sql", "notebook", "job", "pipeline", "report", "query", "api", "manual", "sample")


# --------------------------------------------------------------------------------------------- datasets
def dataset_key(d: dict[str, Any]) -> str:
    """Stable identity of a dataset descriptor (table, file, external location or item such as a report)."""
    kind = d.get("kind")
    if kind == "table":
        return f"table:{d['lakehouse_id']}:{str(d['name']).lower()}"
    if kind == "file":
        return f"file:{d['lakehouse_id']}:{str(d['path']).strip('/')}"
    if kind in ("report", "query", "notebook", "item"):
        return f"item:{d['item_id']}"
    if kind == "external":
        return f"ext:{d.get('uri') or d.get('name')}"
    raise BadRequest(f"unknown dataset kind '{kind}'", details={"supported": ["table", "file", "report", "query", "notebook", "item", "external"]})


def _clean_dataset(d: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(d, dict):
        raise BadRequest("a dataset must be an object such as {kind: 'table', lakehouse_id, name}")
    kind = d.get("kind") or ("table" if d.get("name") and d.get("lakehouse_id") else None)
    out = {**d, "kind": kind}
    need = {"table": ("lakehouse_id", "name"), "file": ("lakehouse_id", "path"), "external": (), "report": ("item_id",), "query": ("item_id",),
            "notebook": ("item_id",), "item": ("item_id",)}.get(kind or "")
    if need is None:
        raise BadRequest(f"unknown dataset kind '{kind}'")
    missing = [k for k in need if not out.get(k)]
    if missing or (kind == "external" and not (out.get("uri") or out.get("name"))):
        raise BadRequest(f"dataset of kind '{kind}' needs {', '.join(need) or 'uri or name'}")
    dataset_key(out)  # validates
    return {k: v for k, v in out.items() if k in ("kind", "lakehouse_id", "name", "path", "item_id", "uri")}


def _has_text(v: Any) -> bool:
    return isinstance(v, str) and bool(v.strip())


def _tags(value: Any) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(t, str) for t in value):
        raise BadRequest("tags must be a list of strings")
    out = sorted({t.strip().lower() for t in value if t.strip()})
    if len(out) > MAX_TAGS or any(len(t) > 64 for t in out):
        raise BadRequest(f"at most {MAX_TAGS} tags of up to 64 characters")
    return out


class CatalogService:
    """Backs ``/v1/catalog*``, the MCP catalog tools and the lineage recording done by SQL, jobs and the sample installer."""

    def __init__(self, ctx: AppContext) -> None:
        self.ctx = ctx

    # ------------------------------------------------------------------ helpers
    @property
    def storage(self):  # noqa: ANN201
        return self.ctx.service("storage")

    def _visible(self, principal: Principal, perm: str = "lakehouse.read") -> set[str] | None:
        return principal.visible_workspaces(perm)

    def _row(self, entry_id: str) -> dict[str, Any]:
        with self.ctx.db.connect() as conn:
            row = fetch_one(conn, select(catalog_entries).where(catalog_entries.c.id == entry_id))
        if not row:
            raise NotFound(f"catalog entry '{entry_id}' does not exist")
        return row

    def _public(self, row: dict[str, Any], *, columns: bool = True) -> dict[str, Any]:
        out = {k: row[k] for k in ("id", "workspace_id", "lakehouse_id", "catalog", "schema_name", "name", "format", "location", "partition_columns",
                                   "description", "tags", "status", "status_detail", "version", "last_modified", "discovered_at", "synced_at",
                                   "created_at", "updated_at")}
        cols = row.get("columns") or []
        out["column_count"] = len(cols)
        if columns:
            out["columns"] = cols
        out["etag"] = row["etag"]
        out["canonical_uri"] = None
        return out

    def _decorate(self, rows: list[dict[str, Any]], *, columns: bool = False) -> list[dict[str, Any]]:
        names = self._lakehouse_names({r["lakehouse_id"] for r in rows})
        out = []
        for r in rows:
            p = self._public(r, columns=columns)
            lh = names.get(r["lakehouse_id"])
            if lh:
                p["lakehouse"] = lh["name"]
                p["workspace"] = lh["workspace"]
                p["canonical_uri"] = f"olcf://{lh['workspace']}/{lh['name']}/Tables/{r['name'].replace('.', '/')}"
            out.append(p)
        return out

    def _lakehouse_names(self, ids: set[str]) -> dict[str, dict[str, str]]:
        if not ids:
            return {}
        with self.ctx.db.connect() as conn:
            rows = fetch_all(conn, select(S.items.c.id, S.items.c.name, S.items.c.workspace_id, S.workspaces.c.name.label("ws"))
                             .select_from(S.items.join(S.workspaces, S.workspaces.c.id == S.items.c.workspace_id))
                             .where(S.items.c.id.in_(list(ids))))
        return {r["id"]: {"name": r["name"], "workspace": r["ws"], "workspace_id": r["workspace_id"]} for r in rows}

    # ------------------------------------------------------------------ discovery sync
    def sync_lakehouse(self, lh: Lakehouse, principal: Principal | None = None, tables: list[str] | None = None) -> dict[str, Any]:
        """Re-discover the tables of ``lh`` (or just ``tables``) and update ``catalog_entries`` + basic ``table_stats``."""
        if principal is not None:
            principal.require("lakehouse.read", workspace_id=lh.workspace_id, item_id=lh.id)
        svc = self.storage
        discovered = {t["name"]: t for t in svc.list_tables(lh)}
        wanted = set(tables) if tables else None
        added, updated, missing, unreadable = [], [], [], []
        now = now_iso()
        with self.ctx.db.connect() as conn:
            existing = {r["name"]: r for r in fetch_all(conn, select(catalog_entries).where(catalog_entries.c.lakehouse_id == lh.id))}
        for name, ref in sorted(discovered.items()):
            if wanted is not None and name not in wanted:
                continue
            try:
                info = svc.table_info(lh, name)
                status, detail = "active", None
            except Exception as exc:  # noqa: BLE001 - a corrupt table must not stop discovery of the others
                info, status, detail = None, "unreadable", f"{type(exc).__name__}: {str(exc)[:300]}"
                unreadable.append(name)
            (updated if name in existing else added).append(name)
            self._upsert(lh, ref, info, existing.get(name), status, detail, now)
        gone = [n for n in existing if n not in discovered and (wanted is None or n in wanted) and existing[n]["status"] != "missing"]
        if gone:
            with self.ctx.db.begin() as conn:
                conn.execute(update(catalog_entries).where(catalog_entries.c.lakehouse_id == lh.id, catalog_entries.c.name.in_(gone))
                             .values(status="missing", status_detail="table no longer found in storage", synced_at=now, updated_at=now,
                                     etag=new_etag()))
            missing = gone
        result = {"lakehouse_id": lh.id, "added": added, "updated": updated, "missing": missing, "unreadable": unreadable,
                  "table_count": len([n for n in discovered if wanted is None or n in wanted]), "synced_at": now}
        if principal is not None:
            self.ctx.audit.record(principal, "catalog.synced", resource_type="lakehouse", resource_id=lh.id, workspace_id=lh.workspace_id,
                                  details={"added": len(added), "updated": len(updated), "missing": len(missing)})
        return result

    def _upsert(self, lh: Lakehouse, ref: dict[str, Any], info: dict[str, Any] | None, old: dict[str, Any] | None, status: str,
                detail: str | None, now: str) -> str:
        cols = []
        if info:
            prev = {c["name"]: c for c in (old or {}).get("columns") or []}
            for c in info.get("schema") or []:
                col = {"name": c["name"], "type": c["type"], "nullable": c.get("nullable", True)}
                if prev.get(c["name"], {}).get("description"):
                    col["description"] = prev[c["name"]]["description"]  # keep human annotations across re-discovery
                cols.append(col)
        elif old:
            cols = old.get("columns") or []
        version = None if not info else (None if info.get("version") is None else str(info["version"]))
        values = {
            "workspace_id": lh.workspace_id, "lakehouse_id": lh.id, "catalog": lh.catalog, "schema_name": ref.get("schema") or "", "name": ref["name"],
            "format": ref["format"], "location": ref.get("location") or "", "columns": cols,
            "partition_columns": [p for p in ((info or {}).get("partition_columns") or []) if p] if info else (old or {}).get("partition_columns") or [],
            "status": status, "status_detail": detail, "version": version, "last_modified": (info or {}).get("last_modified") or (old or {}).get("last_modified"),
            "synced_at": now, "updated_at": now,
        }
        if old:
            eid = old["id"]
            if info and not (old.get("description") or "").strip() and (info.get("description") or "").strip():
                values["description"] = info["description"]
            with self.ctx.db.begin() as conn:
                conn.execute(update(catalog_entries).where(catalog_entries.c.id == eid).values(**values, etag=new_etag()))
        else:
            eid = new_id("cat")
            with self.ctx.db.begin() as conn:
                conn.execute(insert(catalog_entries).values(id=eid, description=(info or {}).get("description") or "", tags=[], owner_id=None,
                                                            discovered_at=now, created_at=now, etag=new_etag(), **values))
        if info:
            self._save_basic_stats(eid, lh, info, version)
            if old is not None:
                self._announce_schema_change(lh, ref["name"], old.get("columns") or [], cols, version)
        return eid

    def _announce_schema_change(self, lh: Lakehouse, table: str, before: list[dict[str, Any]], after: list[dict[str, Any]], version: str | None) -> None:
        """Publish ``dataset.schema_changed`` when ``after`` differs from the previously known ``before`` (best effort; nothing without a known previous schema)."""
        with dsev.best_effort(log, "dataset.schema_changed"):
            added, removed, changed = column_diff(before, after)
            if not (added or removed or changed):
                return
            full = [{"name": c.get("name"), "type": c.get("type"), "nullable": c.get("nullable", True)} for c in after[:SCHEMA_EVENT_MAX_COLUMNS]]
            self.ctx.events.publish("dataset.schema_changed", {"workspace_id": lh.workspace_id, "lakehouse_id": lh.id, "table": table, "columns_added": added,
                                                              "columns_removed": removed, "columns_changed": changed, "columns": full, "version": version, "at": now_iso()},
                                    workspace_id=lh.workspace_id, subject=lh.id)

    def _save_basic_stats(self, eid: str, lh: Lakehouse, info: dict[str, Any], version: str | None) -> None:
        now = now_iso()
        source = {"delta": "delta_log", "iceberg": "iceberg_summary"}.get(info.get("format"), "parquet_footers")
        with self.ctx.db.begin() as conn:
            cur = fetch_one(conn, select(table_stats).where(table_stats.c.entry_id == eid))
            keep_cols = cur["column_stats"] if cur and cur.get("version") == version else None  # stale column stats are dropped
            vals = {"workspace_id": lh.workspace_id, "lakehouse_id": lh.id, "row_count": info.get("num_rows"), "size_bytes": info.get("size_bytes"),
                    "num_files": info.get("num_files"), "version": version, "column_stats": keep_cols, "source": source, "computed_at": now}
            if cur:
                conn.execute(update(table_stats).where(table_stats.c.entry_id == eid).values(**vals))
            else:
                conn.execute(insert(table_stats).values(entry_id=eid, **vals))

    def upsert_table(self, lh: Lakehouse, name: str, *, principal: Principal | None = None) -> dict[str, Any]:
        """Catalog one table (e.g. right after a job wrote it) and return the entry."""
        res = self.sync_lakehouse(lh, principal=principal, tables=[name])
        entry = self.find(lh.id, name)
        if entry is None:
            raise NotFound(f"table '{name}' was not found in lakehouse '{lh.name}'", details=res)
        return entry

    def on_lakehouse_deleted(self, lakehouse_id: str) -> None:
        """Called by the lakehouse delete hook: the catalog forgets the tables (data stays in storage)."""
        with self.ctx.db.begin() as conn:
            ids = [r[0] for r in conn.execute(select(catalog_entries.c.id).where(catalog_entries.c.lakehouse_id == lakehouse_id))]
            if ids:
                conn.execute(delete(table_stats).where(table_stats.c.entry_id.in_(ids)))
                conn.execute(delete(catalog_entries).where(catalog_entries.c.id.in_(ids)))

    # ------------------------------------------------------------------ listing and detail
    def find(self, lakehouse_id: str, name: str) -> dict[str, Any] | None:
        with self.ctx.db.connect() as conn:
            row = fetch_one(conn, select(catalog_entries).where(catalog_entries.c.lakehouse_id == lakehouse_id,
                                                                func.lower(catalog_entries.c.name) == name.lower()))
        return self._public(row) if row else None

    def list_tables(self, lakehouse_id: str | None = None, *, principal: Principal | None = None, workspace_id: str | None = None,
                    catalog: str | None = None, fmt: str | None = None, tag: str | None = None, q: str | None = None,
                    include_missing: bool = False, limit: int = 100, cursor: str | None = None, columns: bool = False) -> tuple[list[dict[str, Any]], str | None]:
        stmt = select(catalog_entries)
        if lakehouse_id:
            stmt = stmt.where(catalog_entries.c.lakehouse_id == lakehouse_id)
        if workspace_id:
            stmt = stmt.where(catalog_entries.c.workspace_id == workspace_id)
        if principal is not None:
            vis = self._visible(principal)
            if vis is not None:
                stmt = stmt.where(catalog_entries.c.workspace_id.in_(sorted(vis) or [""]))
        if catalog:
            stmt = stmt.where(catalog_entries.c.catalog == catalog)
        if fmt:
            stmt = stmt.where(catalog_entries.c.format == fmt)
        if not include_missing:
            stmt = stmt.where(catalog_entries.c.status != "missing")
        if tag:
            stmt = stmt.where(func.lower(cast(catalog_entries.c.tags, String)).like(f'%"{tag.lower()}"%'))
        if q:
            like = f"%{_like_escape(q.lower())}%"
            stmt = stmt.where(or_(func.lower(catalog_entries.c.name).like(like, escape="\\"), func.lower(catalog_entries.c.description).like(like, escape="\\")))
        with self.ctx.db.connect() as conn:
            rows, nxt = paginate(conn, stmt, catalog_entries, limit=limit, cursor=cursor)
        return self._decorate(rows, columns=columns), nxt

    def get(self, principal: Principal, entry_id: str) -> dict[str, Any]:
        row = self._row(entry_id)
        principal.require("lakehouse.read", workspace_id=row["workspace_id"], item_id=row["lakehouse_id"])
        out = self._decorate([row], columns=True)[0]
        out["stats"] = self.stats(principal, entry_id, compute=False)
        out["lineage"] = self._lineage_counts(f"table:{row['lakehouse_id']}:{row['name'].lower()}")
        return out

    def _lineage_counts(self, key: str) -> dict[str, int]:
        with self.ctx.db.connect() as conn:
            up = conn.execute(select(func.count()).select_from(lineage_edges).where(lineage_edges.c.target_key == key)).scalar() or 0
            down = conn.execute(select(func.count()).select_from(lineage_edges).where(lineage_edges.c.source_key == key)).scalar() or 0
        return {"upstream_edges": int(up), "downstream_edges": int(down)}

    def update_entry(self, principal: Principal, entry_id: str, *, description: str | None = None, tags: list[str] | None = None,
                     column_descriptions: dict[str, str] | None = None, if_match: str | None = None) -> dict[str, Any]:
        from olcf.api.deps import check_if_match

        row = self._row(entry_id)
        principal.require("lakehouse.write", workspace_id=row["workspace_id"], item_id=row["lakehouse_id"])
        check_if_match(if_match, row["etag"])
        values: dict[str, Any] = {}
        if description is not None:
            if len(description) > 8000:
                raise BadRequest("description is limited to 8000 characters")
            values["description"] = description
        if tags is not None:
            values["tags"] = _tags(tags)
        if column_descriptions is not None:
            if not isinstance(column_descriptions, dict):
                raise BadRequest("column_descriptions must map column name to text")
            cols = [dict(c) for c in row["columns"] or []]
            by = {c["name"].lower(): c for c in cols}
            unknown = [k for k in column_descriptions if k.lower() not in by]
            if unknown:
                raise BadRequest(f"unknown column(s): {', '.join(sorted(unknown))}", details={"columns": [c["name"] for c in cols]})
            for k, v in column_descriptions.items():
                if v is None or v == "":
                    by[k.lower()].pop("description", None)
                else:
                    by[k.lower()]["description"] = str(v)[:2000]
            values["columns"] = cols
        if not values:
            raise BadRequest("nothing to update", remediation="send description, tags and/or column_descriptions")
        values.update(updated_at=now_iso(), etag=new_etag())
        with self.ctx.db.begin() as conn:
            n = conn.execute(update(catalog_entries).where(catalog_entries.c.id == entry_id, catalog_entries.c.etag == row["etag"]).values(**values)).rowcount
        if n == 0:
            from olcf.core.errors import PreconditionFailed

            raise PreconditionFailed("catalog entry was modified concurrently", details={"current_etag": self._row(entry_id)["etag"]})
        self.ctx.audit.record(principal, "catalog.entry_updated", resource_type="catalog_entry", resource_id=entry_id, workspace_id=row["workspace_id"],
                              details={"fields": sorted(k for k in values if k not in ("updated_at", "etag"))})
        return self._decorate([self._row(entry_id)], columns=True)[0]

    # ------------------------------------------------------------------ catalogs and default resolution
    def list_catalogs(self, principal: Principal) -> list[dict[str, Any]]:
        vis = self._visible(principal)
        stmt = select(S.items.c.id, S.items.c.name, S.items.c.workspace_id, S.items.c.definition).where(S.items.c.type == "lakehouse", S.items.c.deleted_at.is_(None))
        if vis is not None:
            stmt = stmt.where(S.items.c.workspace_id.in_(sorted(vis) or [""]))
        with self.ctx.db.connect() as conn:
            lakes = fetch_all(conn, stmt.limit(5000))
            counts = {r[0]: r[1] for r in conn.execute(select(catalog_entries.c.lakehouse_id, func.count()).where(catalog_entries.c.status != "missing")
                                                       .group_by(catalog_entries.c.lakehouse_id))}
        out: dict[str, dict[str, Any]] = {}
        for lh in lakes:
            name = (lh["definition"] or {}).get("catalog") or "default"
            c = out.setdefault(name, {"name": name, "lakehouses": [], "table_count": 0})
            c["lakehouses"].append({"id": lh["id"], "name": lh["name"], "workspace_id": lh["workspace_id"], "table_count": int(counts.get(lh["id"], 0))})
            c["table_count"] += int(counts.get(lh["id"], 0))
        return sorted(out.values(), key=lambda c: c["name"])

    def default_lakehouse(self, principal: Principal, workspace_id: str, ref: str | None = None, *, catalog: str | None = None) -> Lakehouse:
        """Pick the lakehouse an unqualified table name / relative path refers to.

        Order: explicit ``ref`` (id or name) -> the workspace's ``settings.default_lakehouse`` -> the only lakehouse (of the
        requested ``catalog``) in the workspace.  Anything else is ambiguous and raises an actionable error rather than guessing.
        """
        principal.require("lakehouse.read", workspace_id=workspace_id)
        svc = self.storage
        if ref:
            lh = svc.get_lakehouse(ref, workspace_id=workspace_id)
            if lh.workspace_id != workspace_id:
                raise Forbidden("the lakehouse belongs to a different workspace")
            return lh
        ws = self.ctx.service("workspaces").resolve(workspace_id)
        configured = (ws.get("settings") or {}).get("default_lakehouse")
        if configured:
            return svc.get_lakehouse(configured, workspace_id=workspace_id)
        lakes = [x for x in svc.list_lakehouses(workspace_id) if catalog is None or x.catalog == catalog]
        if len(lakes) == 1:
            return lakes[0]
        if not lakes:
            raise NotFound("this workspace has no lakehouse", remediation="create one (POST /v1/workspaces/{ws}/lakehouses) or install the sample")
        raise BadRequest(f"the workspace has {len(lakes)} lakehouses and no default: {', '.join(sorted(x.name for x in lakes))}",
                         remediation="qualify the table as <lakehouse>.<table>, pass lakehouse=, or set the workspace setting default_lakehouse")

    # ------------------------------------------------------------------ search
    def search(self, principal: Principal, q: str, *, kinds: list[str] | None = None, workspace_id: str | None = None, limit: int = 25) -> list[dict[str, Any]]:
        """Ranked search across tables, columns and (``sql_query``/``report``/``notebook``/``lakehouse``) items.

        Every whitespace-separated term must match somewhere.  Ranking: exact name 100, name prefix 60, name contains 40, tag 30,
        column name 20, description 10 (summed over terms).
        """
        terms = [t for t in q.lower().split() if t][:8]
        if not terms:
            raise BadRequest("q must contain at least one search term")
        kinds = kinds or ["table", "column", "item"]
        bad = [k for k in kinds if k not in ("table", "column", "item")]
        if bad:
            raise BadRequest(f"unknown kinds {bad}", details={"supported": ["table", "column", "item"]})
        limit = max(1, min(int(limit), 100))
        results: list[dict[str, Any]] = []
        if "table" in kinds or "column" in kinds:
            results.extend(self._search_tables(principal, terms, kinds, workspace_id))
        if "item" in kinds:
            results.extend(self._search_items(principal, terms, workspace_id))
        results.sort(key=lambda r: (-r["score"], r["name"]))
        return results[:limit]

    def _search_tables(self, principal: Principal, terms: list[str], kinds: list[str], workspace_id: str | None) -> list[dict[str, Any]]:
        stmt = select(catalog_entries).where(catalog_entries.c.status != "missing")
        vis = self._visible(principal)
        if vis is not None:
            stmt = stmt.where(catalog_entries.c.workspace_id.in_(sorted(vis) or [""]))
        if workspace_id:
            stmt = stmt.where(catalog_entries.c.workspace_id == workspace_id)
        for t in terms:
            like = f"%{_like_escape(t)}%"
            stmt = stmt.where(or_(func.lower(catalog_entries.c.name).like(like, escape="\\"), func.lower(catalog_entries.c.description).like(like, escape="\\"),
                                  func.lower(cast(catalog_entries.c.tags, String)).like(like, escape="\\"),
                                  func.lower(cast(catalog_entries.c.columns, String)).like(like, escape="\\")))
        with self.ctx.db.connect() as conn:
            rows = fetch_all(conn, stmt.limit(2000))
        decorated = {r["id"]: d for r, d in zip(rows, self._decorate(rows), strict=True)}
        out: list[dict[str, Any]] = []
        for r in rows:
            name = r["name"].lower()
            desc = (r["description"] or "").lower()
            tags = [t.lower() for t in r["tags"] or []]
            cols = r["columns"] or []
            score, why = 0, []
            ok = True
            for t in terms:
                s = 0
                if name == t or name.split(".")[-1] == t:
                    s, tag = 100, "name"
                elif name.startswith(t) or name.split(".")[-1].startswith(t):
                    s, tag = 60, "name"
                elif t in name:
                    s, tag = 40, "name"
                elif any(t in x for x in tags):
                    s, tag = 30, "tag"
                else:
                    hits = [c["name"] for c in cols if t in c["name"].lower() or t in (c.get("description") or "").lower()]
                    if hits:
                        s, tag = 20, "column"
                    elif t in desc:
                        s, tag = 10, "description"
                    else:
                        ok = False
                        break
                score += s
                why.append(tag)
            if not ok:
                continue
            d = decorated[r["id"]]
            if "table" in kinds:
                out.append({"kind": "table", "id": r["id"], "name": r["name"], "score": score, "matched": sorted(set(why)), "description": r["description"],
                            "lakehouse": d.get("lakehouse"), "lakehouse_id": r["lakehouse_id"], "workspace_id": r["workspace_id"], "format": r["format"],
                            "canonical_uri": d.get("canonical_uri"), "tags": r["tags"], "columns": [c["name"] for c in cols][:50]})
            if "column" in kinds:
                for c in cols:
                    cn = c["name"].lower()
                    if any(t in cn for t in terms):
                        out.append({"kind": "column", "id": r["id"], "name": f"{r['name']}.{c['name']}", "column": c["name"], "type": c["type"],
                                    "score": 15 + (25 if cn in terms else 0), "matched": ["column"], "description": c.get("description") or "",
                                    "table": r["name"], "lakehouse": d.get("lakehouse"), "lakehouse_id": r["lakehouse_id"], "workspace_id": r["workspace_id"]})
        return out

    def _search_items(self, principal: Principal, terms: list[str], workspace_id: str | None) -> list[dict[str, Any]]:
        stmt = select(S.items.c.id, S.items.c.name, S.items.c.type, S.items.c.description, S.items.c.workspace_id, S.items.c.tags).where(
            S.items.c.deleted_at.is_(None), S.items.c.type.in_(["sql_query", "report", "notebook", "lakehouse"]))
        vis = self._visible(principal, "item.read")
        if vis is not None:
            stmt = stmt.where(S.items.c.workspace_id.in_(sorted(vis) or [""]))
        if workspace_id:
            stmt = stmt.where(S.items.c.workspace_id == workspace_id)
        for t in terms:
            like = f"%{_like_escape(t)}%"
            stmt = stmt.where(or_(func.lower(S.items.c.name).like(like, escape="\\"), func.lower(S.items.c.description).like(like, escape="\\")))
        with self.ctx.db.connect() as conn:
            rows = fetch_all(conn, stmt.limit(500))
        out = []
        for r in rows:
            name = r["name"].lower()
            score = sum(100 if name == t else 60 if name.startswith(t) else 40 if t in name else 10 for t in terms)
            out.append({"kind": r["type"], "id": r["id"], "name": r["name"], "score": score, "matched": ["name" if any(t in name for t in terms) else "description"],
                        "description": r["description"], "workspace_id": r["workspace_id"], "tags": r["tags"]})
        return out

    # ------------------------------------------------------------------ statistics
    def stats(self, principal: Principal, entry_id: str, *, compute: bool = False) -> dict[str, Any]:
        """Row count / size / files (always) and per-column min/max/null counts (``compute=True`` or already cached for this version)."""
        row = self._row(entry_id)
        principal.require("lakehouse.read", workspace_id=row["workspace_id"], item_id=row["lakehouse_id"])
        with self.ctx.db.connect() as conn:
            st = fetch_one(conn, select(table_stats).where(table_stats.c.entry_id == entry_id))
        if compute and (st is None or st.get("column_stats") is None):
            return self.compute_stats(principal, entry_id)
        if st is None:
            return {"entry_id": entry_id, "table": row["name"], "row_count": None, "size_bytes": None, "num_files": None, "version": row["version"],
                    "column_stats": None, "source": None, "computed_at": None}
        return {"entry_id": entry_id, "table": row["name"], **{k: st[k] for k in ("row_count", "size_bytes", "num_files", "version", "column_stats", "source", "computed_at")}}

    def compute_stats(self, principal: Principal, entry_id: str) -> dict[str, Any]:
        """(Re)compute statistics from the Delta log / Parquet footers - metadata only, no data scan."""
        row = self._row(entry_id)
        principal.require("lakehouse.read", workspace_id=row["workspace_id"], item_id=row["lakehouse_id"])
        lh = self.storage.get_lakehouse(row["lakehouse_id"])
        tbl = self.storage.tables
        ref = tbl.locate(lh, row["name"])
        fmt = ref["format"]
        colstats: dict[str, Any] | None
        if fmt == "delta":
            colstats = _delta_column_stats(tbl.open_delta(lh, row["name"]))
        elif fmt == "parquet":
            colstats = _parquet_column_stats(self.storage.filesystem(lh), tbl, ref["path"])
        else:
            colstats = None  # Iceberg: only snapshot-summary totals are available without a data scan
        info = self.storage.table_info(lh, row["name"])
        version = None if info.get("version") is None else str(info["version"])
        self._save_basic_stats(entry_id, lh, info, version)
        if colstats is not None:
            with self.ctx.db.begin() as conn:
                conn.execute(update(table_stats).where(table_stats.c.entry_id == entry_id).values(column_stats=colstats, computed_at=now_iso()))
        with self.ctx.db.begin() as conn:
            conn.execute(update(catalog_entries).where(catalog_entries.c.id == entry_id).values(version=version))
        return self.stats(principal, entry_id)

    # ------------------------------------------------------------------ lineage: recording
    def record_lineage(self, principal: Principal | None, *, workspace_id: str, sources: list[dict[str, Any]], targets: list[dict[str, Any]],
                       via: dict[str, Any] | None = None, columns: list[dict[str, Any]] | None = None, column_status: str | None = None,
                       notes: list[str] | None = None, authorize: bool = True) -> list[dict[str, Any]]:
        """Record ``sources -> targets`` edges (every source to every target), deduplicated, with optional proven column mappings.

        ``columns`` is ``[{"target": col, "sources": [{"source": <index into sources>, "column": col}, ...], "transform": ...}]``.
        Internal callers that run on behalf of a user who already passed their own authorisation (SQL, reports) pass ``authorize=False``.
        """
        if authorize:
            if principal is None:
                raise Forbidden("recording lineage requires an authenticated principal")
            principal.require("lakehouse.write", workspace_id=workspace_id)
        if not sources and not targets:
            raise BadRequest("an edge needs at least one source or target")
        srcs = [_clean_dataset(s) for s in sources]
        tgts = [_clean_dataset(t) for t in targets]
        if not srcs or not tgts:
            return []  # a pure read or a write with no upstream carries no edge
        via = via or {"kind": "manual"}
        vk = via.get("kind") or "manual"
        if vk not in VIA_KINDS:
            raise BadRequest(f"unknown via.kind '{vk}'", details={"supported": list(VIA_KINDS)})
        status = column_status or ("unknown" if not columns else "complete")
        if status not in ("complete", "partial", "unknown", "not_applicable"):
            raise BadRequest("column_status must be complete, partial, unknown or not_applicable")
        out = []
        now = now_iso()
        for tgt in tgts:
            for i, src in enumerate(srcs):
                if dataset_key(src) == dataset_key(tgt):
                    continue  # self-edges (read-modify-write of one table) add nothing but noise to the graph
                mappings = []
                for c in columns or []:
                    picked = [{"column": s["column"]} for s in c.get("sources", []) if s.get("source") == i]
                    if picked or not c.get("sources"):
                        mappings.append({"target": c["target"], "sources": picked, "transform": c.get("transform") or "expression"})
                out.append(self._upsert_edge(workspace_id, src, tgt, vk, via, mappings, status, notes or [], principal, now))
        return out

    def _upsert_edge(self, ws: str, src: dict[str, Any], tgt: dict[str, Any], vk: str, via: dict[str, Any], mappings: list[dict[str, Any]],
                     status: str, notes: list[str], principal: Principal | None, now: str) -> dict[str, Any]:
        sk, tk = dataset_key(src), dataset_key(tgt)
        ref = (via.get("ref") or via.get("name") or "")[:128]
        h = hashlib.sha256(json.dumps([ws, sk, tk, vk, ref], sort_keys=True).encode()).hexdigest()[:40]
        with self.ctx.db.begin() as conn:
            cur = fetch_one(conn, select(lineage_edges).where(lineage_edges.c.edge_hash == h))
            if cur:
                conn.execute(update(lineage_edges).where(lineage_edges.c.id == cur["id"]).values(
                    run_count=cur["run_count"] + 1, last_seen_at=now, column_mappings=mappings, column_status=status, notes=notes[:20],
                    via_name=(via.get("name") or cur["via_name"])))
                edge_id = cur["id"]
            else:
                edge_id = new_id("lin")
                conn.execute(insert(lineage_edges).values(
                    id=edge_id, workspace_id=ws, edge_hash=h, source_key=sk, target_key=tk, source=src, target=tgt, via_kind=vk,
                    via_name=(via.get("name") or None), via_ref=ref or None, column_status=status, column_mappings=mappings, notes=notes[:20],
                    run_count=1, created_by=principal.id if principal else None, first_seen_at=now, last_seen_at=now))
        return {"id": edge_id, "source": sk, "target": tk, "via": vk, "column_status": status}

    # ------------------------------------------------------------------ lineage: from SQL
    def analyze_sql(self, principal: Principal, workspace_id: str, sql: str, *, dialect: str = "duckdb", default_lakehouse_id: str | None = None) -> dict[str, Any]:
        """Parse ``sql`` and resolve its table references to datasets of this workspace (no persistence)."""
        principal.require("lakehouse.read", workspace_id=workspace_id)
        try:
            return self._analyze(workspace_id, sql, dialect, default_lakehouse_id)
        except ValueError as exc:
            raise BadRequest(str(exc)) from None

    def _analyze(self, workspace_id: str, sql: str, dialect: str, default_lakehouse_id: str | None) -> dict[str, Any]:
        lakes = {lh.name.lower(): lh for lh in self.storage.list_lakehouses(workspace_id)}
        default = next((lh for lh in lakes.values() if lh.id == default_lakehouse_id), None) if default_lakehouse_id else None
        with self.ctx.db.connect() as conn:
            entries = fetch_all(conn, select(catalog_entries.c.lakehouse_id, catalog_entries.c.name, catalog_entries.c.columns)
                                .where(catalog_entries.c.workspace_id == workspace_id, catalog_entries.c.status != "missing"))
        cols_by: dict[tuple[str, str], dict[str, str]] = {(e["lakehouse_id"], e["name"].lower()): {c["name"]: c["type"] for c in e["columns"] or []} for e in entries}

        def resolve(ref: lin.TableRef) -> dict[str, Any]:
            if ref.kind == "file":
                return {"kind": "external", "uri": ref.uri, "name": ref.uri}
            parts = [p.lower() for p in ref.parts]
            cand: list[tuple[Any, str]] = []
            if len(parts) == 1 and default is not None:
                cand.append((default, parts[0]))
            elif len(parts) == 1:
                cand.extend((lh, parts[0]) for lh in lakes.values() if (lh.id, parts[0]) in cols_by)
            elif len(parts) == 2:
                if parts[0] in lakes:
                    cand.append((lakes[parts[0]], parts[1]))
                if default is not None and default.schema_enabled:
                    cand.append((default, f"{parts[0]}.{parts[1]}"))  # <schema>.<table> inside a schema-enabled lakehouse
            elif len(parts) >= 3 and parts[-3] in lakes:
                cand.append((lakes[parts[-3]], f"{parts[-2]}.{parts[-1]}"))
            for lh, name in cand:
                if (lh.id, name) in cols_by or lh is default or len(cand) == 1:
                    return {"kind": "table", "lakehouse_id": lh.id, "name": name}
            return {"kind": "external", "name": ref.dotted}

        def schema_for(ref: lin.TableRef) -> dict[str, str] | None:
            d = resolve(ref)
            return cols_by.get((d.get("lakehouse_id"), str(d.get("name", "")).lower())) if d["kind"] == "table" else None

        stmts = lin.analyze(sql, dialect=dialect, schema_for=schema_for)
        out = []
        for st in stmts:
            d = st.to_dict()
            d["source_datasets"] = [resolve(s) for s in st.sources]
            d["target_datasets"] = [resolve(t) for t in st.targets]
            out.append(d)
        return {"statements": out}

    def record_lineage_from_sql(self, principal: Principal | None, workspace_id: str, sql: str, *, dialect: str = "duckdb",
                                default_lakehouse_id: str | None = None, via: dict[str, Any] | None = None, authorize: bool = True) -> list[dict[str, Any]]:
        """Derive lineage from ``sql`` and record an edge for every statement that writes a table (reads alone have no target)."""
        if authorize:
            if principal is None:
                raise Forbidden("recording lineage requires an authenticated principal")
            principal.require("lakehouse.write", workspace_id=workspace_id)
        try:
            analysis = self._analyze(workspace_id, sql, dialect, default_lakehouse_id)
        except ValueError as exc:
            raise BadRequest(str(exc)) from None
        edges: list[dict[str, Any]] = []
        for st in analysis["statements"]:
            if not st["target_datasets"] or not st["source_datasets"]:
                continue
            cols = [{"target": c["target"], "transform": c["transform"],
                     "sources": [{"source": _index_of(st["sources"], s["source"]), "column": s["column"]} for s in c["sources"]]}
                    for c in st["columns"] if c["resolved"]]
            edges += self.record_lineage(principal, workspace_id=workspace_id, sources=st["source_datasets"], targets=st["target_datasets"],
                                         via={"kind": "sql", **(via or {})}, columns=cols, column_status=st["status"], notes=st["notes"], authorize=False)
        return edges

    # ------------------------------------------------------------------ lineage: queries
    def resolve_dataset(self, principal: Principal, ref: str) -> dict[str, Any]:
        """Parse ``?dataset=``: ``olcf://ws/lh/Tables/t`` | ``olcf://ws/lh/Files/p`` | ``<lakehouse-id>/Tables/t`` | ``<catalog-entry-id>`` | ``itm_...``."""
        ref = (ref or "").strip()
        if not ref:
            raise BadRequest("dataset is required", remediation="e.g. dataset=olcf://Sample%20Retail/retail_lakehouse/Tables/fact_sales")
        if ref.startswith("cat_"):
            row = self._row(ref)
            principal.require("lakehouse.read", workspace_id=row["workspace_id"], item_id=row["lakehouse_id"])
            return {"kind": "table", "lakehouse_id": row["lakehouse_id"], "name": row["name"]}
        if ref.startswith("itm_") and "/" not in ref:
            item = self.ctx.service("items").get(ref)
            principal.require("item.read", workspace_id=item["workspace_id"], item_id=item["id"])
            kind = {"report": "report", "sql_query": "query", "notebook": "notebook"}.get(item["type"], "item")
            if item["type"] == "lakehouse":
                raise BadRequest("a lakehouse is not a dataset; pass a table or file path inside it")
            return {"kind": kind, "item_id": item["id"]}
        if ref.startswith("olcf://") or ref.startswith("itm_"):
            from olcf.storage import paths as P

            if ref.startswith("itm_"):
                lh_ref, _, rest = ref.partition("/")
                canon = f"olcf://x/{lh_ref}/{rest}"
                parsed = P.parse_canonical(canon)
                lh = self.storage.get_lakehouse(lh_ref)
            else:
                parsed = P.parse_canonical(ref)
                lh = self.storage.get_lakehouse(f"{parsed.workspace}/{parsed.lakehouse}")
            principal.require("lakehouse.read", workspace_id=lh.workspace_id, item_id=lh.id)
            if parsed.area == "Tables":
                return {"kind": "table", "lakehouse_id": lh.id, "name": parsed.path.replace("/", ".")}
            return {"kind": "file", "lakehouse_id": lh.id, "path": parsed.rel}
        if ref.startswith(("abfss://", "s3://", "file://", "https://")):
            return {"kind": "external", "uri": ref, "name": ref}
        raise BadRequest(f"cannot interpret dataset '{ref}'", remediation="use an olcf:// URI, a catalog entry id (cat_...) or an item id")

    def lineage(self, principal: Principal, dataset: dict[str, Any], *, direction: str = "both", depth: int = 3, column: str | None = None) -> dict[str, Any]:
        """Breadth-first lineage graph around ``dataset`` limited to workspaces the caller can read."""
        if direction not in ("up", "down", "both"):
            raise BadRequest("direction must be up, down or both")
        depth = max(1, min(int(depth), 10))
        vis = self._visible(principal)
        root = _clean_dataset(dataset)
        root_key = dataset_key(root)
        edges: dict[str, dict[str, Any]] = {}
        nodes: dict[str, dict[str, Any]] = {root_key: root}
        truncated = False

        def fetch(col: Any, keys: list[str]) -> list[dict[str, Any]]:
            stmt = select(lineage_edges).where(col.in_(keys))
            if vis is not None:
                stmt = stmt.where(lineage_edges.c.workspace_id.in_(sorted(vis) or [""]))
            with self.ctx.db.connect() as conn:
                return fetch_all(conn, stmt.order_by(lineage_edges.c.last_seen_at.desc()).limit(MAX_LINEAGE_EDGES + 1))

        for want_up in (True, False):
            if (want_up and direction == "down") or (not want_up and direction == "up"):
                continue
            seen = {root_key}
            frontier = deque([(root_key, 0)])
            while frontier:
                batch: list[str] = []
                level = frontier[0][1]
                while frontier and frontier[0][1] == level:
                    batch.append(frontier.popleft()[0])
                if level >= depth:
                    break
                for e in fetch(lineage_edges.c.target_key if want_up else lineage_edges.c.source_key, batch):
                    if len(edges) >= MAX_LINEAGE_EDGES:
                        truncated = True
                        break
                    edges[e["id"]] = e
                    nxt = e["source_key"] if want_up else e["target_key"]
                    nodes.setdefault(e["source_key"], e["source"])
                    nodes.setdefault(e["target_key"], e["target"])
                    if nxt not in seen:
                        seen.add(nxt)
                        frontier.append((nxt, level + 1))
        out_nodes = self._describe_nodes(principal, nodes, root_key)
        out_edges = [{"id": e["id"], "source": e["source_key"], "target": e["target_key"], "via": {"kind": e["via_kind"], "name": e["via_name"], "ref": e["via_ref"]},
                      "column_lineage": {"status": e["column_status"], "mappings": e["column_mappings"], "notes": e["notes"]},
                      "run_count": e["run_count"], "first_seen_at": e["first_seen_at"], "last_seen_at": e["last_seen_at"]}
                     for e in sorted(edges.values(), key=lambda e: e["last_seen_at"], reverse=True)]
        result: dict[str, Any] = {"dataset": out_nodes[root_key], "direction": direction, "depth": depth, "nodes": list(out_nodes.values()), "edges": out_edges,
                                  "truncated": truncated}
        if column:
            result["column_trace"] = self._trace_column(edges, root_key, column)
        return result

    def _trace_column(self, edges: dict[str, dict[str, Any]], key: str, column: str) -> dict[str, Any]:
        """Follow ``column`` of ``key`` upstream through proven mappings; unknown hops are reported, never skipped silently."""
        by_target: dict[str, list[dict[str, Any]]] = {}
        for e in edges.values():
            by_target.setdefault(e["target_key"], []).append(e)
        paths: list[dict[str, Any]] = []
        gaps: list[dict[str, Any]] = []

        def walk(k: str, col: str, trail: list[dict[str, Any]], seen: frozenset[tuple[str, str]]) -> None:
            ups = by_target.get(k, [])
            if not ups:
                if trail:
                    paths.append({"origin": {"dataset": k, "column": col}, "hops": trail})
                return
            for e in ups:
                maps = [m for m in e["column_mappings"] or [] if str(m["target"]).lower() == col.lower()]
                if not maps:
                    if e["column_status"] == "complete":
                        continue  # proven: this source does not feed the column
                    gaps.append({"edge": e["id"], "dataset": e["source_key"], "reason": f"column lineage is {e['column_status']} for this edge", "target_column": col})
                    continue
                for m in maps:
                    if not m["sources"]:
                        paths.append({"origin": {"dataset": e["source_key"], "column": None, "transform": m["transform"]}, "hops": trail + [_hop(e, col, None, m)]})
                    for s in m["sources"]:
                        if (e["source_key"], s["column"].lower()) in seen:
                            continue
                        hop = _hop(e, col, s["column"], m)
                        walk(e["source_key"], s["column"], trail + [hop], seen | {(e["source_key"], s["column"].lower())})

        walk(key, column, [], frozenset({(key, column.lower())}))
        status = "complete" if paths and not gaps else "partial" if paths else "unknown"
        return {"column": column, "status": status, "paths": paths, "unresolved": gaps}

    def _describe_nodes(self, principal: Principal, nodes: dict[str, dict[str, Any]], root_key: str) -> dict[str, dict[str, Any]]:
        lh_ids = {d["lakehouse_id"] for d in nodes.values() if d.get("lakehouse_id")}
        item_ids = {d["item_id"] for d in nodes.values() if d.get("item_id")}
        names = self._lakehouse_names(lh_ids)
        with self.ctx.db.connect() as conn:
            items = {r["id"]: r for r in fetch_all(conn, select(S.items.c.id, S.items.c.name, S.items.c.type, S.items.c.workspace_id).where(S.items.c.id.in_(list(item_ids) or [""])))}
            ents = {(r["lakehouse_id"], r["name"].lower()): r["id"] for r in fetch_all(conn, select(catalog_entries.c.id, catalog_entries.c.lakehouse_id, catalog_entries.c.name)
                                                                                         .where(catalog_entries.c.lakehouse_id.in_(list(lh_ids) or [""])))}
        out: dict[str, dict[str, Any]] = {}
        for k, d in nodes.items():
            n: dict[str, Any] = {"key": k, "kind": d["kind"], "root": k == root_key}
            if d["kind"] in ("table", "file"):
                lh = names.get(d["lakehouse_id"])
                allowed = lh is not None and principal.can("lakehouse.read", workspace_id=lh["workspace_id"], item_id=d["lakehouse_id"])
                n.update(lakehouse_id=d["lakehouse_id"], lakehouse=lh["name"] if allowed else None, restricted=not allowed)
                if d["kind"] == "table":
                    n["name"] = d["name"]
                    n["catalog_entry_id"] = ents.get((d["lakehouse_id"], d["name"].lower())) if allowed else None
                    if allowed and lh:
                        n["canonical_uri"] = f"olcf://{lh['workspace']}/{lh['name']}/Tables/{d['name'].replace('.', '/')}"
                else:
                    n["name"] = d["path"].rsplit("/", 1)[-1]
                    n["path"] = d["path"]
                    if allowed and lh:
                        n["canonical_uri"] = f"olcf://{lh['workspace']}/{lh['name']}/{d['path']}"
            elif d["kind"] == "external":
                n["name"] = d.get("name") or d.get("uri")
                n["uri"] = d.get("uri")
            else:
                it = items.get(d["item_id"])
                allowed = it is not None and principal.can("item.read", workspace_id=it["workspace_id"], item_id=it["id"])
                n.update(item_id=d["item_id"], name=it["name"] if allowed else None, item_type=it["type"] if allowed else None, restricted=not allowed)
            out[k] = n
        return out


#: Upper bound of the full column list carried by ``dataset.schema_changed`` (wider tables are truncated; the diff lists are never truncated).
SCHEMA_EVENT_MAX_COLUMNS = 300


def column_diff(before: list[dict[str, Any]], after: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Difference between two column lists (``[{name, type, nullable}]``) as ``(added, removed, changed)``; names compare case-insensitively (Spark and SQL do).

    ``added`` / ``removed`` are ``{name, type, nullable}``; ``changed`` is ``{name, from, to, nullable_from, nullable_to}`` for a column whose type or nullability differs.
    An empty ``before`` means no schema was known before, which is not a change: all three lists are empty then.
    """
    if not before:
        return [], [], []
    old = {str(c["name"]).lower(): c for c in before}
    new = {str(c["name"]).lower(): c for c in after}

    def desc(c: dict[str, Any]) -> dict[str, Any]:
        return {"name": c["name"], "type": c.get("type"), "nullable": c.get("nullable", True)}

    added = [desc(c) for k, c in new.items() if k not in old]
    removed = [desc(c) for k, c in old.items() if k not in new]
    changed = [{"name": new[k]["name"], "from": old[k].get("type"), "to": new[k].get("type"), "nullable_from": old[k].get("nullable", True), "nullable_to": new[k].get("nullable", True)}
               for k in new if k in old and (old[k].get("type") != new[k].get("type") or old[k].get("nullable", True) != new[k].get("nullable", True))]
    return added, removed, changed


def _hop(e: dict[str, Any], target_col: str, source_col: str | None, m: dict[str, Any]) -> dict[str, Any]:
    return {"edge": e["id"], "from": {"dataset": e["source_key"], "column": source_col}, "to": {"dataset": e["target_key"], "column": target_col},
            "transform": m["transform"], "via": e["via_kind"]}


def _index_of(sources: list[dict[str, Any]], dotted: str) -> int:
    for i, s in enumerate(sources):
        if s["name"] == dotted:
            return i
    return -1


def _like_escape(s: str) -> str:
    return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


# --------------------------------------------------------------------------------------------- statistics helpers
def _delta_column_stats(dt: Any) -> dict[str, Any]:
    """Aggregate per-file Delta stats (min/max/nullCount) across all files - metadata only."""
    import pyarrow as pa

    from olcf.storage.arrowjson import jsonable

    aa = pa.table(dt.get_add_actions(flatten=True))
    cols = [f.name for f in dt.schema().fields]
    out: dict[str, Any] = {}
    for c in cols:
        mn, mx, nc = f"min.{c}", f"max.{c}", f"null_count.{c}"
        entry: dict[str, Any] = {"min": None, "max": None, "null_count": None}
        if mn in aa.column_names:
            vals = [v for v in aa.column(mn).to_pylist() if v is not None]
            try:
                entry["min"] = jsonable(min(vals)) if vals else None
            except TypeError:
                entry["min"] = None
        if mx in aa.column_names:
            vals = [v for v in aa.column(mx).to_pylist() if v is not None]
            try:
                entry["max"] = jsonable(max(vals)) if vals else None
            except TypeError:
                entry["max"] = None
        if nc in aa.column_names:
            vals = aa.column(nc).to_pylist()
            entry["null_count"] = sum(v for v in vals if v is not None) if all(v is not None for v in vals) else None
        out[c] = entry
    return out


def _parquet_column_stats(fs: Any, tbl: Any, path: str, max_files: int = 200) -> dict[str, Any]:
    """Aggregate row-group statistics from Parquet footers (reads only footers via range requests)."""
    import pyarrow.parquet as pq

    from olcf.storage.arrowjson import jsonable

    files = [f for f in fs.list(path, recursive=True) if not f.is_dir and f.name.endswith((".parquet", ".pq"))][:max_files]
    agg: dict[str, dict[str, Any]] = {}
    for f in files:
        md = pq.ParquetFile(tbl._seekable(fs, f.path)).metadata
        for rg in range(md.num_row_groups):
            g = md.row_group(rg)
            for ci in range(g.num_columns):
                col = g.column(ci)
                st = col.statistics
                a = agg.setdefault(col.path_in_schema, {"min": None, "max": None, "null_count": 0, "ok": True})
                if st is None or not st.has_min_max:
                    a["ok"] = False
                    continue
                try:
                    a["min"] = st.min if a["min"] is None else min(a["min"], st.min)
                    a["max"] = st.max if a["max"] is None else max(a["max"], st.max)
                except TypeError:
                    a["ok"] = False
                a["null_count"] += st.null_count or 0
    return {k: {"min": jsonable(v["min"]) if v["ok"] else None, "max": jsonable(v["max"]) if v["ok"] else None, "null_count": v["null_count"]} for k, v in agg.items()}

