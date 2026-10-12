# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Reports (dashboards): validation of ``report`` items and :class:`ReportService` that runs their tiles.

A report is a set of SQL tiles plus filters.  ``ReportService.run`` resolves filter values (defaults, validation against
declared options), runs the tiles **in parallel** (bounded) through :class:`~olcf.sql.service.SqlService` with the caller's
own permissions, and returns typed data per tile with timing.  One failing tile never fails the report: its tile carries
an ``error`` while the others render.  Results are cached for a short TTL; the cache is consulted only *after* the caller
has been authorised for the tile, so it never leaks data between users with different access.

Filter parameters: ``select``/``text`` filters bind ``:<name>``; ``date_range`` filters bind ``:<name>_from`` and ``:<name>_to``
(ISO dates).  ``select`` with ``multiple: true`` binds a list (use ``IN :<name>``).
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

from olcf.core import refs
from olcf.core.errors import BadRequest, NotFound, OlcfError, ValidationFailed
from olcf.core.timeutil import now_iso
from olcf.security.rbac import Principal
from olcf.sql import guard
from olcf.sql.model import ENGINES, SqlRequest
from olcf.sql.service import context_workspace

if TYPE_CHECKING:
    from olcf.core.context import AppContext
    from olcf.sql.service import SqlService

log = logging.getLogger("olcf.sql")
TILE_TYPES = ("kpi", "bar", "line", "area", "donut", "table", "heatmap", "scatter")
FILTER_TYPES = ("select", "date_range", "text")
_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
MAX_TILES = 60
MAX_CACHE = 512


def filter_param_names(filters: list[dict[str, Any]]) -> set[str]:
    out: set[str] = set()
    for f in filters:
        out |= {f"{f['name']}_from", f"{f['name']}_to"} if f["type"] == "date_range" else {f["name"]}
    return out


def validate_report(defn: dict[str, Any]) -> dict[str, Any]:
    """Normalise and validate a ``report`` item definition (raises ``ValidationFailed`` with every problem found)."""
    problems: list[str] = []
    allowed = {"title", "refresh_seconds", "filters", "tiles", "description"}
    unknown = set(defn) - allowed
    if unknown:
        problems.append(f"unknown keys: {', '.join(sorted(unknown))}")
    title = defn.get("title") or ""
    if not isinstance(title, str) or not title.strip():
        problems.append("title is required")
    refresh = defn.get("refresh_seconds")
    if refresh is not None and (not isinstance(refresh, int) or isinstance(refresh, bool) or not 5 <= refresh <= 86400):
        problems.append("refresh_seconds must be null or an integer between 5 and 86400")
    filters_out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for i, f in enumerate(defn.get("filters") or []):
        if not isinstance(f, dict) or not isinstance(f.get("name"), str) or not _NAME.match(f["name"]):
            problems.append(f"filters[{i}]: name must be an identifier")
            continue
        if f["name"] in seen:
            problems.append(f"filters[{i}]: duplicate name '{f['name']}'")
        seen.add(f["name"])
        if f.get("type") not in FILTER_TYPES:
            problems.append(f"filters[{i}] '{f['name']}': type must be one of {', '.join(FILTER_TYPES)}")
            continue
        nf: dict[str, Any] = {"name": f["name"], "label": f.get("label") or f["name"], "type": f["type"], "default": f.get("default")}
        if f["type"] == "select":
            nf["multiple"] = bool(f.get("multiple"))
            if f.get("options") is not None:
                if not isinstance(f["options"], list) or not all(isinstance(o, (str, int, float)) for o in f["options"]):
                    problems.append(f"filters[{i}] '{f['name']}': options must be a list of scalars")
                nf["options"] = f.get("options")
            if f.get("options_sql"):
                nf["options_sql"] = f["options_sql"]
                nf["options_lakehouse_id"] = f.get("options_lakehouse_id")
        if f["type"] == "date_range" and f.get("default") is not None:
            d = f["default"]
            try:
                if not isinstance(d, dict):
                    raise ValueError("default must be {from, to}")
                dt.date.fromisoformat(str(d["from"])[:10])
                dt.date.fromisoformat(str(d["to"])[:10])
            except (KeyError, ValueError) as exc:
                problems.append(f"filters[{i}] '{f['name']}': default must be {{from: YYYY-MM-DD, to: YYYY-MM-DD}} ({exc})")
        filters_out.append(nf)
    names = filter_param_names(filters_out)
    tiles_out: list[dict[str, Any]] = []
    tile_ids: set[str] = set()
    tiles = defn.get("tiles") or []
    if not isinstance(tiles, list) or not tiles:
        problems.append("at least one tile is required")
        tiles = []
    if len(tiles) > MAX_TILES:
        problems.append(f"at most {MAX_TILES} tiles")
    for i, t in enumerate(tiles[:MAX_TILES]):
        if not isinstance(t, dict) or not isinstance(t.get("id"), str) or not _NAME.match(t["id"]):
            problems.append(f"tiles[{i}]: id must be an identifier")
            continue
        if t["id"] in tile_ids:
            problems.append(f"tiles[{i}]: duplicate id '{t['id']}'")
        tile_ids.add(t["id"])
        if t.get("type") not in TILE_TYPES:
            problems.append(f"tiles[{i}] '{t['id']}': type must be one of {', '.join(TILE_TYPES)}")
        eng = t.get("engine") or "duckdb"
        if eng not in ENGINES:
            problems.append(f"tiles[{i}] '{t['id']}': engine must be one of {', '.join(ENGINES)}")
        sql = t.get("sql")
        if not isinstance(sql, str) or not sql.strip():
            problems.append(f"tiles[{i}] '{t['id']}': sql is required")
        else:
            try:
                st = guard.parse_statement(sql, eng if eng in ENGINES else "duckdb", param_names=names)
                if not st.read_only:
                    problems.append(f"tiles[{i}] '{t['id']}': tiles must be read-only (got {st.kind})")
            except OlcfError as exc:
                problems.append(f"tiles[{i}] '{t['id']}': {exc.message}")
        if not t.get("lakehouse_id") or not isinstance(t.get("lakehouse_id"), str):
            problems.append(f"tiles[{i}] '{t['id']}': lakehouse_id is required")
        w, h = t.get("width", 4), t.get("height", 2)
        if not isinstance(w, int) or not 1 <= w <= 12:
            problems.append(f"tiles[{i}] '{t['id']}': width must be 1-12")
        if not isinstance(h, int) or not 1 <= h <= 6:
            problems.append(f"tiles[{i}] '{t['id']}': height must be 1-6")
        nt = {k: t[k] for k in ("id", "title", "type", "sql", "lakehouse_id", "x", "y", "series", "format", "max_rows") if k in t}
        nt.update(engine=eng, width=w, height=h)
        nt.setdefault("title", t.get("id"))
        tiles_out.append(nt)
    if problems:
        raise ValidationFailed("the report definition is invalid", details={"problems": problems})
    out: dict[str, Any] = {"title": title.strip(), "refresh_seconds": refresh, "filters": filters_out, "tiles": tiles_out}
    if defn.get("description"):
        out["description"] = str(defn["description"])[:2000]
    return out


def validate_sql_query(defn: dict[str, Any]) -> dict[str, Any]:
    """Normalise a ``sql_query`` item definition."""
    unknown = set(defn) - {"sql", "engine", "lakehouse_id", "parameters", "description"}
    if unknown:
        raise ValidationFailed(f"unknown keys: {', '.join(sorted(unknown))}", details={"allowed": ["sql", "engine", "lakehouse_id", "parameters"]})
    eng = defn.get("engine") or "duckdb"
    if eng not in ENGINES:
        raise ValidationFailed(f"engine must be one of {', '.join(ENGINES)}")
    try:
        specs = guard.validate_param_specs(defn.get("parameters"))
        guard.parse_statement(defn.get("sql") or "", eng, param_names={s["name"] for s in specs})
    except OlcfError as exc:
        raise ValidationFailed(exc.message, details=exc.details) from None
    lh = defn.get("lakehouse_id")
    if lh is not None and not isinstance(lh, str):
        raise ValidationFailed("lakehouse_id must be a string or null")
    out = {"sql": defn["sql"], "engine": eng, "lakehouse_id": lh, "parameters": specs}
    if defn.get("description"):
        out["description"] = str(defn["description"])[:2000]
    return out


def remap_sql_query_ids(defn: dict[str, Any], id_map: dict[str, str], name_map: dict[str, str] | None = None) -> dict[str, Any]:  # noqa: ARG001
    """Workspace import / promotion: ``lakehouse_id`` follows the imported lakehouse."""
    return refs.remap_paths(defn, id_map, ["lakehouse_id"])


def remap_report_ids(defn: dict[str, Any], id_map: dict[str, str], name_map: dict[str, str] | None = None) -> dict[str, Any]:  # noqa: ARG001
    """Workspace import / promotion: every tile's ``lakehouse_id`` and each select filter's ``options_lakehouse_id`` follow the imported lakehouses."""
    return refs.remap_paths(defn, id_map, ["tiles[].lakehouse_id", "filters[].options_lakehouse_id"])


class ReportService:
    """Runs report tiles in parallel with per-tile errors and a TTL cache."""

    def __init__(self, ctx: AppContext, sql: SqlService) -> None:
        self.ctx, self.sql = ctx, sql
        self._cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self._lock = threading.Lock()

    def _setting(self, key: str, default: int) -> int:
        try:
            v = self.ctx.service("settings").get(key)
            return int(v) if v is not None else default
        except Exception:  # noqa: BLE001
            return default

    # ------------------------------------------------------------------ listing
    def list_reports(self, principal: Principal, workspace_id: str | None = None) -> list[dict[str, Any]]:
        from sqlalchemy import select

        from olcf.db import schema as S
        from olcf.db.util import fetch_all

        vis = principal.visible_workspaces("item.read")
        stmt = select(S.items).where(S.items.c.type == "report", S.items.c.deleted_at.is_(None))
        if workspace_id:
            principal.require("item.read", workspace_id=workspace_id)
            stmt = stmt.where(S.items.c.workspace_id == workspace_id)
        elif vis is not None:
            stmt = stmt.where(S.items.c.workspace_id.in_(sorted(vis) or [""]))
        with self.ctx.db.connect() as conn:
            rows = fetch_all(conn, stmt.order_by(S.items.c.name).limit(500))
        return [{"id": r["id"], "name": r["name"], "workspace_id": r["workspace_id"], "title": (r["definition"] or {}).get("title"), "description": r["description"],
                 "tile_count": len((r["definition"] or {}).get("tiles") or []), "filters": [{"name": f["name"], "label": f.get("label"), "type": f["type"]} for f in (r["definition"] or {}).get("filters") or []],
                 "updated_at": r["updated_at"]} for r in rows]

    # ------------------------------------------------------------------ filters
    def _options(self, principal: Principal, item: dict[str, Any], f: dict[str, Any], fallback_lh: str | None) -> list[Any] | None:
        if f.get("options") is not None:
            return list(f["options"])
        if not f.get("options_sql"):
            return None
        lh_ref = f.get("options_lakehouse_id") or fallback_lh
        req = SqlRequest(sql=f["options_sql"], engine="duckdb", workspace_id=context_workspace(item["workspace_id"], lh_ref), lakehouse=lh_ref,
                         max_rows=500, source="report", item_id=item["id"], label=f"options:{f['name']}")
        key = self._key(item, "opt:" + f["name"], req.sql, {})
        hit = self._get(key)
        if hit is not None:
            self.sql.plan(principal, req)       # still authorise before serving cached options
            return hit["options"]
        res = self.sql.execute(principal, req)
        opts = [r[0] for r in res.rows]
        self._put(key, {"options": opts}, self._setting("sql.report_cache_ttl_seconds", 60))
        return opts

    def resolve_filters(self, principal: Principal, item: dict[str, Any], provided: dict[str, Any] | None) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        defn = item["definition"]
        provided = dict(provided or {})
        declared = {f["name"] for f in defn.get("filters") or []}
        unknown = sorted(set(provided) - declared)
        if unknown:
            raise BadRequest(f"unknown filter(s): {', '.join(unknown)}", details={"declared": sorted(declared)})
        fallback_lh = next((t["lakehouse_id"] for t in defn["tiles"]), None)
        out_filters: list[dict[str, Any]] = []
        params: dict[str, Any] = {}
        for f in defn.get("filters") or []:
            name = f["name"]
            value = provided.get(name, f.get("default"))
            entry = {"name": name, "label": f.get("label"), "type": f["type"], "default": f.get("default"), "value": value}
            if f["type"] == "select":
                opts = self._options(principal, item, f, fallback_lh)
                entry["options"] = opts
                if value is not None:
                    vals = value if isinstance(value, list) else [value]
                    if not f.get("multiple") and isinstance(value, list):
                        raise BadRequest(f"filter '{name}' accepts a single value")
                    if opts is not None:
                        bad = [v for v in vals if v not in opts and str(v) not in {str(o) for o in opts}]
                        if bad:
                            raise BadRequest(f"filter '{name}': {bad!r} is not one of the available options", details={"options": opts[:50]})
                params[name] = value
            elif f["type"] == "date_range":
                if value is None:
                    params[f"{name}_from"] = params[f"{name}_to"] = None
                else:
                    if not isinstance(value, dict) or "from" not in value or "to" not in value:
                        raise BadRequest(f"filter '{name}' needs {{from, to}} dates")
                    a = guard.coerce_param(f"{name}.from", value["from"], "date")
                    b = guard.coerce_param(f"{name}.to", value["to"], "date")
                    if a > b:
                        raise BadRequest(f"filter '{name}': 'from' is after 'to'")
                    params[f"{name}_from"], params[f"{name}_to"] = a, b
                    entry["value"] = {"from": a.isoformat(), "to": b.isoformat()}
            else:  # text
                if value is not None and (not isinstance(value, str) or len(value) > 500):
                    raise BadRequest(f"filter '{name}' must be text of at most 500 characters")
                params[name] = value
            out_filters.append(entry)
        return out_filters, params

    # ------------------------------------------------------------------ cache
    @staticmethod
    def _key(item: dict[str, Any], what: str, sql: str, params: dict[str, Any]) -> str:
        blob = json.dumps([item["id"], item.get("etag"), item.get("version"), what, sql, params], sort_keys=True, default=str)
        return hashlib.sha256(blob.encode()).hexdigest()

    def _get(self, key: str) -> dict[str, Any] | None:
        with self._lock:
            hit = self._cache.get(key)
            if hit and hit[0] > time.monotonic():
                return hit[1]
            self._cache.pop(key, None)
        return None

    def _put(self, key: str, value: dict[str, Any], ttl: float) -> None:
        if ttl <= 0:
            return
        with self._lock:
            if len(self._cache) >= MAX_CACHE:
                now = time.monotonic()
                for k in [k for k, (exp_at, _) in self._cache.items() if exp_at <= now] or [next(iter(self._cache))]:
                    self._cache.pop(k, None)
            self._cache[key] = (time.monotonic() + ttl, value)

    def clear_cache(self) -> None:
        with self._lock:
            self._cache.clear()

    # ------------------------------------------------------------------ run
    def run(self, principal: Principal, report_item_id: str, filters: dict[str, Any] | None = None, *, refresh: bool = False) -> dict[str, Any]:
        """Run every tile of a report and return typed data, per-tile timing and per-tile errors."""
        started = time.perf_counter()
        item = self.ctx.service("items").get(report_item_id)
        if item["type"] != "report":
            raise BadRequest(f"item '{report_item_id}' is a {item['type']}, not a report")
        principal.require("item.read", workspace_id=item["workspace_id"], item_id=item["id"])
        defn = item["definition"]
        resolved, params = self.resolve_filters(principal, item, filters)
        ttl = defn.get("refresh_seconds") or self._setting("sql.report_cache_ttl_seconds", 60)
        workers = max(1, min(self._setting("sql.report_parallelism", 4), len(defn["tiles"])))
        hits = {"n": 0}
        sources: list[dict[str, Any]] = []
        lock = threading.Lock()

        def run_tile(tile: dict[str, Any]) -> dict[str, Any]:
            base = {"id": tile["id"], "title": tile.get("title"), "type": tile["type"],
                    "view": {k: tile[k] for k in ("x", "y", "series", "format", "width", "height") if k in tile}}
            req = SqlRequest(sql=tile["sql"], engine=tile.get("engine") or "duckdb", workspace_id=context_workspace(item["workspace_id"], tile["lakehouse_id"]), lakehouse=tile["lakehouse_id"], parameters=params,
                             max_rows=min(int(tile.get("max_rows") or 5000), 50_000), source="report", item_id=item["id"], label=f"tile:{tile['id']}")
            t0 = time.perf_counter()
            try:
                plan = self.sql.plan(principal, req)       # authorises *this* caller for every table the tile touches
                with lock:
                    sources.extend(plan.datasets)
                key = self._key(item, "tile:" + tile["id"], tile["sql"], params)
                cached = None if refresh else self._get(key)
                if cached is not None:
                    with lock:
                        hits["n"] += 1
                    return {**base, **cached, "elapsed_ms": 0, "from_cache": True, "error": None}
                res = self.sql.execute(principal, req)
                payload = {"columns": res.columns, "rows": res.rows, "row_count": res.row_count, "truncated": res.truncated}
                self._put(key, payload, ttl)
                return {**base, **payload, "elapsed_ms": res.stats.get("elapsed_ms", int((time.perf_counter() - t0) * 1000)), "from_cache": False, "error": None,
                        "query_id": res.query_id}
            except OlcfError as exc:
                return {**base, "columns": [], "rows": [], "row_count": 0, "truncated": False, "elapsed_ms": int((time.perf_counter() - t0) * 1000), "from_cache": False,
                        "error": {"code": exc.code, "message": exc.message, "remediation": exc.remediation}}
            except Exception as exc:  # noqa: BLE001 - a tile bug must not take the report down
                log.exception("report tile %s failed", tile["id"])
                return {**base, "columns": [], "rows": [], "row_count": 0, "truncated": False, "elapsed_ms": int((time.perf_counter() - t0) * 1000), "from_cache": False,
                        "error": {"code": "internal", "message": f"unexpected error: {type(exc).__name__}", "remediation": None}}

        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="olcf-report") as pool:
            tiles = list(pool.map(run_tile, defn["tiles"]))
        self._record_lineage(item, sources)
        errors = sum(1 for t in tiles if t["error"])
        self.ctx.audit.record(principal, "report.run", resource_type="report", resource_id=item["id"], workspace_id=item["workspace_id"],
                              details={"tiles": len(tiles), "errors": errors})
        return {"report": {"id": item["id"], "name": item["name"], "title": defn["title"], "version": item.get("version"), "workspace_id": item["workspace_id"],
                           "refresh_seconds": defn.get("refresh_seconds")},
                "filters": resolved, "tiles": tiles, "generated_at": now_iso(), "elapsed_ms": int((time.perf_counter() - started) * 1000),
                "cache": {"hits": hits["n"], "misses": len(tiles) - hits["n"] - errors}, "errors": errors, "partial": errors > 0}

    def _record_lineage(self, item: dict[str, Any], sources: list[dict[str, Any]]) -> None:
        cat = self.ctx.maybe_service("catalog")
        if cat is None or not sources:
            return
        uniq = {json.dumps(s, sort_keys=True): s for s in sources}
        try:
            cat.record_lineage(None, workspace_id=item["workspace_id"], sources=list(uniq.values()), targets=[{"kind": "report", "item_id": item["id"]}],
                               via={"kind": "report", "name": item["name"], "ref": item["id"]}, column_status="not_applicable", authorize=False)
        except Exception:  # noqa: BLE001
            log.warning("could not record report lineage", exc_info=True)


def report_not_found(item_id: str) -> NotFound:
    return NotFound(f"report '{item_id}' does not exist")
