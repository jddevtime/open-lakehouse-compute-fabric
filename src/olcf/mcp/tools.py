# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The MCP tools.  Every handler is ``fn(env, args) -> dict`` and enforces the caller's own permissions against the *stored* workspace
of whatever it touches (an agent never gets more than the user or token behind it).  Read tools only need the ``mcp`` token scope; the
two job-changing tools (``submit_notebook_job``, ``cancel_job``) need ``mcp:write``.  Results are small and typed: row counts are capped, wide cells truncated.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from olcf.core.errors import BadRequest, Forbidden, NotFound, Unavailable
from olcf.mcp.protocol import Tool, obj
from olcf.security.rbac import Principal
from olcf.sql import guard
from olcf.sql.model import SqlRequest

if TYPE_CHECKING:
    from olcf.core.context import AppContext
    from olcf.storage.lakehouse import Lakehouse

S = {"type": "string"}
LAKEHOUSE = {"type": "string", "description": "Lakehouse name or id (use '<workspace>/<lakehouse>' or the workspace argument when the name is not unique)"}
WORKSPACE = {"type": "string", "description": "Workspace name or id (optional when the lakehouse is given as an id or is unambiguous)"}


class Env:
    """Per-call context: who is calling, through which session, and resolution helpers shared by the tools."""

    def __init__(self, ctx: AppContext, principal: Principal, session_id: str | None = None) -> None:
        self.ctx, self.principal, self.session_id = ctx, principal, session_id
        self.workspace_touched: set[str] = set()

    # ------------------------------------------------------------------ settings
    def cfg(self, key: str, default: int) -> int:
        try:
            v = self.ctx.service("settings").get(key)
            return int(v) if v is not None else default
        except Exception:  # noqa: BLE001
            return default

    # ------------------------------------------------------------------ services
    @property
    def storage(self):  # noqa: ANN201
        return self.ctx.service("storage")

    @property
    def sql(self):  # noqa: ANN201
        return self.ctx.service("sql")

    @property
    def catalog(self):  # noqa: ANN201
        return self.ctx.service("catalog")

    # ------------------------------------------------------------------ resolution
    def visible(self, perm: str = "workspace.read") -> list[dict[str, Any]]:
        rows, _ = self.ctx.service("workspaces").list(self.principal, limit=500, cursor=None)
        vis = self.principal.visible_workspaces(perm)
        return [w for w in rows if vis is None or w["id"] in vis]

    def workspace(self, ref: str, perm: str = "workspace.read") -> dict[str, Any]:
        try:
            ws = self.ctx.service("workspaces").resolve(ref)
        except NotFound:
            raise NotFound(f"workspace '{ref}' not found or not visible to you", remediation="call list_workspaces to see what you can access") from None
        if not self.principal.can(perm, workspace_id=ws["id"]):
            raise NotFound(f"workspace '{ref}' not found or not visible to you", remediation="call list_workspaces to see what you can access")
        self.workspace_touched.add(ws["id"])
        return ws

    def lakehouse(self, ref: str, workspace: str | None = None, perm: str = "lakehouse.read") -> Lakehouse:
        st = self.storage
        try:
            if ref.startswith("itm_") or "/" in ref:
                lh = st.get_lakehouse(ref)
            elif workspace:
                lh = st.get_lakehouse(ref, workspace_id=self.workspace(workspace)["id"])
            else:
                found = [lh for ws in self.visible() for lh in st.list_lakehouses(ws["id"]) if lh.name.lower() == ref.lower()]
                if not found:
                    raise NotFound(f"lakehouse '{ref}' not found")
                if len(found) > 1:
                    raise BadRequest(f"lakehouse name '{ref}' exists in several workspaces: {', '.join(sorted(lh.workspace_name for lh in found))}",
                                     remediation="pass the workspace argument or use '<workspace>/<lakehouse>'")
                lh = found[0]
        except NotFound:
            raise NotFound(f"lakehouse '{ref}' not found or not visible to you", remediation="call list_lakehouses to see what you can access") from None
        if not self.principal.can(perm, workspace_id=lh.workspace_id, item_id=lh.id):
            if not self.principal.can("lakehouse.read", workspace_id=lh.workspace_id, item_id=lh.id):
                raise NotFound(f"lakehouse '{ref}' not found or not visible to you", remediation="call list_lakehouses to see what you can access")
            raise Forbidden(f"permission '{perm}' is required on lakehouse '{lh.name}'", details={"permission": perm})
        self.workspace_touched.add(lh.workspace_id)
        return lh

    def item(self, ref: str, type_: str, workspace: str | None, perm: str = "item.read") -> dict[str, Any]:
        items = self.ctx.service("items")
        if ref.startswith("itm_"):
            row = items.get(ref)
            if row["type"] != type_:
                raise BadRequest(f"'{ref}' is a {row['type']}, not a {type_}")
        else:
            if workspace:
                spaces = [self.workspace(workspace)]
            else:
                spaces = self.visible()
            hits = []
            for ws in spaces:
                f = items.find(ws["id"], type_, ref)
                if f:
                    hits.append(f)
            if not hits:
                raise NotFound(f"{type_} '{ref}' not found or not visible to you")
            if len(hits) > 1:
                raise BadRequest(f"{type_} name '{ref}' exists in several workspaces; pass the workspace argument")
            row = hits[0]
        if not self.principal.can(perm, workspace_id=row["workspace_id"], item_id=row["id"]):
            raise NotFound(f"{type_} '{ref}' not found or not visible to you")
        self.workspace_touched.add(row["workspace_id"])
        return row

    def table_entry(self, lh: Lakehouse, table: str) -> dict[str, Any]:
        """Catalog entry of ``table`` (case-insensitive, with a did-you-mean error), discovering the table first when needed."""
        real = self.sql._real_table_name(lh, table)
        entry = self.catalog.find(lh.id, real)
        if entry is None or entry.get("status") == "missing":
            self.catalog.sync_lakehouse(lh, tables=[real])
            entry = self.catalog.find(lh.id, real)
        if entry is None:
            raise NotFound(f"table '{table}' not found in lakehouse '{lh.name}'", remediation="call list_tables to see the tables")
        return entry


# ----------------------------------------------------------------------------- helpers
def _clip_cell(v: Any, n: int = 500) -> Any:
    return v if not isinstance(v, str) or len(v) <= n else v[:n] + "…"


def _compact(res: dict[str, Any]) -> dict[str, Any]:
    cols = [{"name": c["name"], "type": c["type"]} for c in res["columns"]]
    rows = [[_clip_cell(v) for v in r] for r in res["rows"]]
    out = {"columns": cols, "rows": rows, "row_count": len(rows), "truncated": res["truncated"]}
    if res.get("warnings"):
        out["warnings"] = res["warnings"]
    if res["truncated"]:
        out["note"] = "more rows exist; add a WHERE / LIMIT, aggregate in SQL, or raise max_rows (up to the server limit)"
    return out


def _limit(env: Env, args: dict[str, Any], key: str, default: int, setting: str, hard: int) -> int:
    v = args.get(key, default)
    return max(1, min(int(v), env.cfg(setting, hard), hard))


# ----------------------------------------------------------------------------- tools
def list_workspaces(env: Env, args: dict[str, Any]) -> dict[str, Any]:
    out = []
    items = env.ctx.service("items")
    for ws in env.visible():
        lhs, _ = items.list(ws["id"], type_="lakehouse", limit=500)
        out.append({"id": ws["id"], "name": ws["name"], "description": ws.get("description") or "", "lakehouse_count": len(lhs)})
    return {"workspaces": out, "count": len(out)}


def list_lakehouses(env: Env, args: dict[str, Any]) -> dict[str, Any]:
    spaces = [env.workspace(args["workspace"])] if args.get("workspace") else env.visible()
    out = []
    for ws in spaces:
        for lh in env.storage.list_lakehouses(ws["id"]):
            if not env.principal.can("lakehouse.read", workspace_id=lh.workspace_id, item_id=lh.id):
                continue
            tables, _ = env.catalog.list_tables(lh.id, limit=500)
            out.append({"id": lh.id, "name": lh.name, "workspace": lh.workspace_name, "workspace_id": lh.workspace_id, "catalog": lh.catalog,
                        "tables_known": len(tables), "reference": f"{lh.workspace_name}/{lh.name}"})
    return {"lakehouses": out, "count": len(out)}


def list_tables(env: Env, args: dict[str, Any]) -> dict[str, Any]:
    lh = env.lakehouse(args["lakehouse"], args.get("workspace"))
    limit = _limit(env, args, "limit", 200, "mcp.max_list_items", 500)
    rows, _ = env.catalog.list_tables(lh.id, limit=limit, columns=bool(args.get("include_columns")))
    if not rows:
        env.catalog.sync_lakehouse(lh)
        rows, _ = env.catalog.list_tables(lh.id, limit=limit, columns=bool(args.get("include_columns")))
    out = []
    for r in rows:
        stats = env.catalog.stats(env.principal, r["id"]) if r.get("id") else {}
        t = {"name": r["name"], "reference": f"{lh.name}.{r['name']}", "format": r["format"], "row_count": stats.get("row_count"),
             "size_bytes": stats.get("size_bytes"), "partition_columns": r.get("partition_columns") or [], "description": r.get("description") or "", "tags": r.get("tags") or []}
        if args.get("include_columns"):
            t["columns"] = [{"name": c["name"], "type": c["type"]} for c in r.get("columns") or []]
        out.append(t)
    return {"lakehouse": lh.name, "workspace": lh.workspace_name, "tables": out, "count": len(out),
            "hint": f"query a table with run_sql, e.g. SELECT * FROM {lh.name}.<table> LIMIT 10"}  # noqa: S608


def describe_table(env: Env, args: dict[str, Any]) -> dict[str, Any]:
    lh = env.lakehouse(args["lakehouse"], args.get("workspace"))
    entry = env.table_entry(lh, args["table"])
    full = env.catalog.get(env.principal, entry["id"])
    stats = full.get("stats") or {}
    out = {"reference": f"{lh.name}.{full['name']}", "lakehouse": lh.name, "workspace": lh.workspace_name, "name": full["name"], "format": full["format"],
           "description": full.get("description") or "", "tags": full.get("tags") or [], "partition_columns": full.get("partition_columns") or [],
           "columns": [{"name": c["name"], "type": c["type"], "nullable": c.get("nullable", True), "description": c.get("description") or ""} for c in full["columns"]],
           "stats": {"row_count": stats.get("row_count"), "size_bytes": stats.get("size_bytes"), "num_files": stats.get("num_files"), "version": stats.get("version")},
           "lineage": full.get("lineage"), "canonical_uri": full.get("canonical_uri")}
    if args.get("include_history"):
        try:
            out["history"] = env.storage.tables.history(lh, full["name"], 5)
        except Exception:  # noqa: BLE001
            out["history"] = None
    out["hint"] = f"SELECT * FROM {lh.name}.{full['name']} LIMIT 10"  # noqa: S608
    return out


def sample_rows(env: Env, args: dict[str, Any]) -> dict[str, Any]:
    lh = env.lakehouse(args["lakehouse"], args.get("workspace"))
    real = env.sql._real_table_name(lh, args["table"])
    n = _limit(env, args, "limit", 10, "mcp.max_sample_rows", 100)
    prev = env.storage.tables.preview(lh, real, n, columns=args.get("columns") or None)
    cols = [{"name": c["name"], "type": c["type"]} for c in prev["columns"]]
    rows = [[_clip_cell(v) for v in r] for r in prev["rows"]]
    return {"reference": f"{lh.name}.{real}", "columns": cols, "rows": rows, "row_count": len(rows), "truncated": bool(prev.get("truncated"))}


def table_stats(env: Env, args: dict[str, Any]) -> dict[str, Any]:
    lh = env.lakehouse(args["lakehouse"], args.get("workspace"))
    entry = env.table_entry(lh, args["table"])
    st = env.catalog.stats(env.principal, entry["id"], compute=bool(args.get("column_stats", True)))
    cs = st.get("column_stats") or {}
    return {"reference": f"{lh.name}.{entry['name']}", "row_count": st.get("row_count"), "size_bytes": st.get("size_bytes"), "num_files": st.get("num_files"),
            "version": st.get("version"), "source": st.get("source"), "computed_at": st.get("computed_at"),
            "columns": {k: v for k, v in list(cs.items())[:200]}, "note": "metadata-derived (no data scan); min/max may be absent for some types"}


def run_sql(env: Env, args: dict[str, Any]) -> dict[str, Any]:
    engine = args.get("engine", "duckdb")
    st = guard.parse_statement(args["sql"], engine, param_names=set((args.get("parameters") or {}).keys()))
    if not st.read_only:
        raise BadRequest("run_sql is read-only; INSERT/CREATE/UPDATE/DELETE/MERGE are not available through MCP",
                         remediation="use SELECT / WITH / DESCRIBE / SUMMARIZE / SHOW, or submit a notebook job with submit_notebook_job")
    lh = env.lakehouse(args["lakehouse"], args.get("workspace")) if args.get("lakehouse") else None
    ws = env.workspace(args["workspace"]).get("id") if args.get("workspace") else (lh.workspace_id if lh else None)
    max_rows = _limit(env, args, "max_rows", 200, "mcp.max_rows", 5000)
    res = env.sql.execute(env.principal, SqlRequest(sql=args["sql"], engine=engine, workspace_id=ws, lakehouse=lh.id if lh else None, parameters=args.get("parameters") or {},
                                                    max_rows=max_rows, timeout_seconds=args.get("timeout_seconds") or env.cfg("mcp.sql_timeout_seconds", 30), source="mcp"))
    out = _compact(res.to_dict())
    out.update(query_id=res.query_id, elapsed_ms=res.stats.get("elapsed_ms"), tables=res.stats.get("tables"))
    if ws:
        env.workspace_touched.add(ws)
    return out


def explain_query(env: Env, args: dict[str, Any]) -> dict[str, Any]:
    lh = env.lakehouse(args["lakehouse"], args.get("workspace")) if args.get("lakehouse") else None
    ws = env.workspace(args["workspace"]).get("id") if args.get("workspace") else (lh.workspace_id if lh else None)
    out = env.sql.explain(env.principal, SqlRequest(sql=args["sql"], engine="duckdb", workspace_id=ws, lakehouse=lh.id if lh else None, parameters=args.get("parameters") or {}, source="mcp"))
    return {"tables": out["tables"], "statement": out["statement"], "plan": out["plan"][:20000], "warnings": out.get("warnings") or []}


def search_catalog(env: Env, args: dict[str, Any]) -> dict[str, Any]:
    ws = env.workspace(args["workspace"])["id"] if args.get("workspace") else None
    hits = env.catalog.search(env.principal, args["query"], kinds=args.get("kinds"), workspace_id=ws, limit=_limit(env, args, "limit", 15, "mcp.max_list_items", 50))
    return {"query": args["query"], "results": hits, "count": len(hits)}


def get_lineage(env: Env, args: dict[str, Any]) -> dict[str, Any]:
    ref = args["dataset"]
    cat = env.catalog
    if ref.startswith(("olcf://", "cat_", "itm_")) or "/Tables/" in ref or "/Files/" in ref:
        ds = cat.resolve_dataset(env.principal, ref)
    else:
        lhref, _, table = ref.rpartition(".")
        lh = env.lakehouse(args.get("lakehouse") or lhref, args.get("workspace")) if (args.get("lakehouse") or lhref) else None
        if lh is None:
            raise BadRequest("dataset must be '<lakehouse>.<table>', an olcf:// URI, or a table name together with the lakehouse argument")
        real = env.sql._real_table_name(lh, table if lhref and not args.get("lakehouse") else ref)
        ds = {"kind": "table", "lakehouse_id": lh.id, "name": real}
    g = cat.lineage(env.principal, ds, direction=args.get("direction", "both"), depth=int(args.get("depth", 2)), column=args.get("column"))
    return {"dataset": ds, **g}


def list_reports(env: Env, args: dict[str, Any]) -> dict[str, Any]:
    ws = env.workspace(args["workspace"])["id"] if args.get("workspace") else None
    items = env.ctx.service("reports").list_reports(env.principal, ws)
    return {"reports": items, "count": len(items), "hint": "run one with run_report; filters are optional"}


def run_report(env: Env, args: dict[str, Any]) -> dict[str, Any]:
    item = env.item(args["report"], "report", args.get("workspace"))
    out = env.ctx.service("reports").run(env.principal, item["id"], args.get("filters") or {})
    cap = _limit(env, args, "max_rows_per_tile", 50, "mcp.max_report_rows", 500)
    tiles = []
    for t in out["tiles"]:
        rows = [[_clip_cell(v) for v in r] for r in t["rows"][:cap]]
        tiles.append({"id": t["id"], "title": t["title"], "type": t["type"], "columns": [{"name": c["name"], "type": c["type"]} for c in t["columns"]], "rows": rows,
                      "row_count": t["row_count"], "truncated": t["truncated"] or t["row_count"] > cap, "elapsed_ms": t["elapsed_ms"], "error": t["error"]})
    return {"report": out["report"], "filters": [{"name": f["name"], "type": f["type"], "value": f["value"], "options": (f.get("options") or [])[:50] or None} for f in out["filters"]],
            "tiles": tiles, "errors": out["errors"], "partial": out["partial"]}


# ----------------------------------------------------------------------------- write tools (token scope mcp:write)
def _jobs(env: Env):  # noqa: ANN202
    jobs = env.ctx.maybe_service("jobs")
    if jobs is None:
        raise Unavailable("the job service is not running in this deployment", remediation="enable the compute module and enrol a worker (Admin -> Compute)")
    return jobs


def _job_view(jobs: Any, rec: Any, *, logs: bool = False) -> dict[str, Any]:
    d = rec.model_dump(mode="json") if hasattr(rec, "model_dump") else dict(rec)
    keep = ("id", "kind", "state", "workspace_id", "name", "item_id", "attempt", "created_at", "queued_at", "started_at", "finished_at", "exit_value", "result", "failure", "parameters")
    out = {k: d.get(k) for k in keep}
    if logs:
        try:
            page = jobs.logs_page(d["id"], 0, 8192, tail=True)
            out["log_tail"] = page.get("data", "")[-8000:]
        except Exception:  # noqa: BLE001
            out["log_tail"] = None
    return out


def submit_notebook_job(env: Env, args: dict[str, Any]) -> dict[str, Any]:
    from olcf.core.models import JobKind, JobSubmission, LakehouseRef, Origin, OriginKind

    jobs = _jobs(env)
    nb = env.item(args["notebook"], "notebook", args.get("workspace"))
    env.principal.require("job.submit", workspace_id=nb["workspace_id"])
    default = None
    if args.get("lakehouse"):
        lh = env.lakehouse(args["lakehouse"], args.get("workspace") or nb["workspace_id"])
        default = LakehouseRef(lakehouse_id=lh.id, name=lh.name, workspace_id=lh.workspace_id)
    sub = JobSubmission(kind=JobKind.NOTEBOOK, workspace_id=nb["workspace_id"], name=args.get("name") or f"{nb['name']} (mcp)", item_id=nb["id"],
                        parameters=args.get("parameters") or {}, default_lakehouse=default, idempotency_key=args.get("idempotency_key"),
                        origin=Origin(kind=OriginKind.MCP, details={"session": env.session_id, "principal": env.principal.name}))
    rec = jobs.submit(env.principal, sub)
    wait = min(int(args.get("wait_seconds") or 0), 60)
    if wait:
        rec = jobs.wait(rec.id, timeout=float(wait))
    env.ctx.audit.record(env.principal, "mcp.job_submitted", resource_type="job", resource_id=rec.id, workspace_id=nb["workspace_id"], details={"notebook": nb["id"], "via": "mcp"})
    return {"job": _job_view(jobs, rec, logs=bool(wait)), "hint": "poll with get_job until state is succeeded, failed or cancelled"}


def _job_for(env: Env, job_id: str, perm: str) -> tuple[Any, Any]:
    jobs = _jobs(env)
    try:
        rec = jobs.get(job_id)
    except NotFound:
        raise NotFound(f"job '{job_id}' not found or not visible to you") from None
    if not env.principal.can(perm, workspace_id=rec.workspace_id):
        raise NotFound(f"job '{job_id}' not found or not visible to you")
    env.workspace_touched.add(rec.workspace_id)
    return jobs, rec


def get_job(env: Env, args: dict[str, Any]) -> dict[str, Any]:
    jobs, rec = _job_for(env, args["job_id"], "job.read")
    return {"job": _job_view(jobs, rec, logs=bool(args.get("logs", True)))}


def cancel_job(env: Env, args: dict[str, Any]) -> dict[str, Any]:
    jobs, rec = _job_for(env, args["job_id"], "job.cancel")
    rec = jobs.cancel(env.principal, rec.id, reason=args.get("reason") or "cancelled via MCP")
    env.ctx.audit.record(env.principal, "mcp.job_cancelled", resource_type="job", resource_id=rec.id, workspace_id=rec.workspace_id, details={"via": "mcp"})
    return {"job": _job_view(jobs, rec)}


# ----------------------------------------------------------------------------- registry
def build_tools() -> list[Tool]:
    """All tools: the data tools below, then the read-only Intelligence tools (:mod:`olcf.mcp.intel_tools`) and governance tools (:mod:`olcf.mcp.gov_tools`), which are listed only when
    those modules are installed."""
    from olcf.mcp.gov_tools import build_gov_tools
    from olcf.mcp.intel_tools import build_intel_tools
    return [*_data_tools(), *build_intel_tools(), *build_gov_tools()]


def _data_tools() -> list[Tool]:
    lh_args = {"lakehouse": LAKEHOUSE, "workspace": WORKSPACE}
    return [
        Tool("list_workspaces", "List workspaces", "List the workspaces you can access, with how many lakehouses each has. Start here.", obj({}), list_workspaces, needs=("workspace.read",)),
        Tool("list_lakehouses", "List lakehouses", "List the lakehouses you can read (optionally in one workspace).", obj({"workspace": WORKSPACE}), list_lakehouses, needs=("lakehouse.read",)),
        Tool("list_tables", "List tables", "List the tables of a lakehouse with format, row count, size, partitioning and descriptions. Use the 'reference' value in SQL.",
             obj({**lh_args, "include_columns": {"type": "boolean", "default": False}, "limit": {"type": "integer", "minimum": 1, "maximum": 500, "default": 200}}, ["lakehouse"]),
             list_tables, needs=("lakehouse.read",)),
        Tool("describe_table", "Describe a table", "Columns (name, type, nullability, descriptions), partitioning, row count / size statistics, tags, lineage counts and a ready-to-run example query.",
             obj({**lh_args, "table": S, "include_history": {"type": "boolean", "default": False}}, ["lakehouse", "table"]), describe_table, needs=("lakehouse.read",)),
        Tool("sample_rows", "Sample rows", "Return the first rows of a table (default 10, at most 100) to see what the data looks like.",
             obj({**lh_args, "table": S, "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 10}, "columns": {"type": "array", "items": S, "maxItems": 100}}, ["lakehouse", "table"]),
             sample_rows, needs=("lakehouse.read",)),
        Tool("table_stats", "Table statistics", "Row count, size, file count and per-column min/max/null counts derived from table metadata (no data scan).",
             obj({**lh_args, "table": S, "column_stats": {"type": "boolean", "default": True}}, ["lakehouse", "table"]), table_stats, needs=("lakehouse.read",)),
        Tool("run_sql", "Run a read-only SQL query",
             "Run one read-only SQL statement (SELECT / WITH / DESCRIBE / SUMMARIZE) on DuckDB. Reference tables as <lakehouse>.<table> (or <workspace>.<lakehouse>.<table>) and read Files/ with "
             "read_csv('Files/..'), read_parquet, read_json. Use :name parameters instead of pasting values. Results are capped (default 200 rows); check 'truncated'.",
             obj({"sql": {"type": "string", "minLength": 1, "maxLength": 100000}, "lakehouse": {**LAKEHOUSE, "description": "Default lakehouse for unqualified table names"}, "workspace": WORKSPACE,
                  "parameters": {"type": "object", "description": "Values for :name parameters (strings, numbers, booleans or lists)"},
                  "max_rows": {"type": "integer", "minimum": 1, "maximum": 5000, "default": 200}, "timeout_seconds": {"type": "integer", "minimum": 1, "maximum": 120},
                  "engine": {"type": "string", "enum": ["duckdb", "spark"], "default": "duckdb"}}, ["sql"]), run_sql, needs=("sql.execute",)),
        Tool("explain_query", "Explain a query", "Show how a read-only query would run (tables it touches and the DuckDB plan) without executing it.",
             obj({"sql": {"type": "string", "minLength": 1, "maxLength": 100000}, "lakehouse": LAKEHOUSE, "workspace": WORKSPACE, "parameters": {"type": "object"}}, ["sql"]), explain_query, needs=("sql.execute",)),
        Tool("search_catalog", "Search the data catalog", "Find tables, columns, saved queries, reports and notebooks by name, tag, column or description. Every term must match.",
             obj({"query": {"type": "string", "minLength": 1, "maxLength": 200}, "workspace": WORKSPACE, "kinds": {"type": "array", "items": S, "maxItems": 10},
                  "limit": {"type": "integer", "minimum": 1, "maximum": 50, "default": 15}}, ["query"]), search_catalog, needs=("workspace.read",)),
        Tool("get_lineage", "Get data lineage", "Upstream / downstream lineage of a table (optionally one column): what feeds it and what reads it.",
             obj({"dataset": {"type": "string", "description": "'<lakehouse>.<table>', an olcf:// URI, a catalog entry id (cat_...) or an item id"}, "lakehouse": LAKEHOUSE, "workspace": WORKSPACE,
                  "direction": {"type": "string", "enum": ["up", "down", "both"], "default": "both"}, "depth": {"type": "integer", "minimum": 1, "maximum": 6, "default": 2}, "column": S}, ["dataset"]),
             get_lineage, needs=("lakehouse.read",)),
        Tool("list_reports", "List reports", "List reports (dashboards) with their filters.", obj({"workspace": WORKSPACE}), list_reports, needs=("item.read",)),
        Tool("run_report", "Run a report", "Run every tile of a report with optional filter values and return the data (rows per tile are capped). A failing tile reports its own error.",
             obj({"report": {"type": "string", "description": "Report name or id"}, "workspace": WORKSPACE, "filters": {"type": "object"},
                  "max_rows_per_tile": {"type": "integer", "minimum": 1, "maximum": 500, "default": 50}}, ["report"]), run_report, needs=("item.read", "sql.execute")),
        Tool("submit_notebook_job", "Submit a notebook job", "Run a notebook on the compute fabric (needs the mcp:write token scope). Returns the job; poll it with get_job.",
             obj({"notebook": {"type": "string", "description": "Notebook name or id"}, "workspace": WORKSPACE, "parameters": {"type": "object"}, "lakehouse": LAKEHOUSE, "name": S,
                  "idempotency_key": {"type": "string", "maxLength": 128}, "wait_seconds": {"type": "integer", "minimum": 0, "maximum": 60, "default": 0}}, ["notebook"]),
             submit_notebook_job, write=True, needs=("job.submit",), annotations={"idempotentHint": False}),
        Tool("get_job", "Get a job", "State, result and log tail of a job.", obj({"job_id": S, "logs": {"type": "boolean", "default": True}}, ["job_id"]), get_job, needs=("job.read",)),
        Tool("cancel_job", "Cancel a job", "Request cancellation of a queued or running job (needs the mcp:write token scope).", obj({"job_id": S, "reason": {"type": "string", "maxLength": 200}}, ["job_id"]),
             cancel_job, write=True, needs=("job.cancel",), annotations={"idempotentHint": True, "destructiveHint": True}),
    ]
