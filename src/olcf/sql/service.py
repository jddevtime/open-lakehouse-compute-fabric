# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The ``sql`` service: authorise -> plan -> run -> record.

``SqlService.execute(principal, SqlRequest)`` is the single entry point used by the REST API, saved queries, report tiles,
the MCP ``run_sql`` tool and the sample installer.  The pipeline is the same for every caller:

1. **Parse** exactly one statement (:mod:`olcf.sql.guard`), classify it, bind parameters as typed literals.
2. **Authorise** against the *stored* workspace of every lakehouse the statement touches (``sql.execute`` + ``lakehouse.read``;
   writes additionally ``sql.write`` + ``lakehouse.write``).  Read-only enforcement for DuckDB happens here and in the connection.
3. **Plan** - lakehouse tables become Arrow datasets (Delta via delta-rs, Iceberg via PyIceberg, Parquet) and ``read_csv`` /
   ``read_parquet`` / ``read_json`` on ``Files/`` paths become capped Arrow tables, registered under private names that
   replace the references in the SQL.  DuckDB never sees a path or credential.
4. **Run** under a concurrency slot with a timeout and cancel handle, collecting at most ``max_rows`` (+1 to detect truncation).
5. **Record** history (redacted), and for Spark writes also lineage and a catalog refresh of the target table.
6. **Announce** what ran: a successful statement publishes the platform events ``dataset.read`` / ``dataset.written`` (tables and files touched, from the plan's own table
   extraction) and ``dataset.statement`` (the SQL text, so the control plane can derive column lineage). Publishing is best effort and happens after the history row is final:
   a failing event store or subscriber is logged and swallowed and never changes the result or the status of the statement. Failed statements publish nothing.
"""

from __future__ import annotations

import difflib
import logging
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from sqlglot import exp

from olcf.core import dataset_events as dsev
from olcf.core.errors import BadRequest, Forbidden, NotFound, OlcfError, TooManyRequests, Unsupported
from olcf.core.ids import new_id
from olcf.core.timeutil import now_iso
from olcf.security.rbac import Principal
from olcf.sql import guard
from olcf.sql.engines import (
    DuckDbEngine,
    EngineLimits,
    RunHandle,
    SparkEngine,
    Stream,
    interrupted,
)
from olcf.sql.history import HistoryStore
from olcf.sql.model import ENGINES, QUERY_ID_RE, SqlRequest, SqlResult
from olcf.storage.arrowjson import column_descriptors, table_rows

if TYPE_CHECKING:
    from olcf.core.context import AppContext
    from olcf.storage.lakehouse import Lakehouse

log = logging.getLogger("olcf.sql")
MiB = 1024 * 1024
HARD_ROW_CAP = 1_000_000


@dataclass
class Plan:
    """Everything decided before a statement runs."""

    req: SqlRequest
    query_id: str
    engine: str
    stmt: guard.Statement
    sql: str                                           # final SQL text for the engine
    workspace_id: str | None
    default_lakehouse: Lakehouse | None
    sources: dict[str, Callable[[], Any]] = field(default_factory=dict)   # DuckDB: alias -> loader of an Arrow object
    tables: list[str] = field(default_factory=list)    # human-readable list of datasets touched
    target: tuple[Lakehouse, str] | None = None        # Spark writes: (lakehouse, table)
    warnings: list[str] = field(default_factory=list)
    max_rows: int = 10_000
    timeout: int = 60
    reads: list[Lakehouse] = field(default_factory=list)
    datasets: list[dict[str, Any]] = field(default_factory=list)   # structured lineage sources: {kind: table|file, lakehouse_id, name|path}


class _Resolver:
    """Maps table parts as written to lakehouses of the request workspace (see :meth:`SqlService.resolve_table` for the rules)."""

    def __init__(self, svc: SqlService, principal: Principal, workspace_id: str | None, default: Lakehouse | None) -> None:
        self.svc, self.principal, self.workspace_id, self.default = svc, principal, workspace_id, default
        self._lakes: dict[str, dict[str, Lakehouse]] = {}

    def lakes(self, workspace_id: str) -> dict[str, Lakehouse]:
        if workspace_id not in self._lakes:
            self._lakes[workspace_id] = {lh.name.lower(): lh for lh in self.svc.storage.list_lakehouses(workspace_id)}
        return self._lakes[workspace_id]

    def resolve(self, parts: tuple[str, ...]) -> tuple[Lakehouse, str]:
        n = len(parts)
        ws = self.workspace_id
        lakes = self.lakes(ws) if ws else {}
        low = [p.lower() for p in parts]
        if ws is None and self.default is None and n in (1, 2):
            # without a workspace nothing can be looked up: say what to send instead of blaming the table name
            raise BadRequest(f"cannot resolve table '{'.'.join(parts)}': the request has no workspace_id",
                             remediation="send workspace_id (workspace id or name) in the request body - the SDK, 'olcf sql run --workspace' and the UI do - "
                                         "or fully qualify the table as <workspace>.<lakehouse>.<table>",
                             details={"missing": "workspace_id", "table": ".".join(parts)})
        if n == 1:
            if self.default is not None:
                return self.default, parts[0]
            if len(lakes) == 1:
                return next(iter(lakes.values())), parts[0]
            raise BadRequest(f"table '{parts[0]}' is not qualified and there is no default lakehouse",
                             remediation="write <lakehouse>.<table> or pass lakehouse= in the request" + (f" (lakehouses here: {', '.join(sorted(x.name for x in lakes.values()))})" if lakes else ""),
                             details={"lakehouses": sorted(x.name for x in lakes.values())})
        if n == 2:
            if low[0] in lakes:
                return lakes[low[0]], parts[1]
            if self.default is not None and self.default.schema_enabled:
                return self.default, f"{parts[0]}.{parts[1]}"
            raise NotFound(f"no lakehouse named '{parts[0]}' in this workspace", remediation="use <lakehouse>.<table>",
                           details={"lakehouses": sorted(x.name for x in lakes.values())})
        if n == 3:
            if low[0] in lakes and lakes[low[0]].schema_enabled:
                return lakes[low[0]], f"{parts[1]}.{parts[2]}"
            try:
                other = self.svc.ctx.service("workspaces").resolve(parts[0])
            except OlcfError:
                other = None
            if other is not None and self.principal.can("workspace.read", workspace_id=other["id"]):
                hit = self.lakes(other["id"]).get(low[1])
                if hit is not None:
                    return hit, parts[2]
            raise NotFound(f"cannot resolve '{'.'.join(parts)}' as <lakehouse>.<schema>.<table> or <workspace>.<lakehouse>.<table>")
        raise BadRequest(f"'{'.'.join(parts)}' has too many parts", remediation="use <lakehouse>.<table> (or <workspace>.<lakehouse>.<table>)")


class SqlService:
    """Runs SQL for people, agents and reports."""

    def __init__(self, ctx: AppContext) -> None:
        self.ctx = ctx
        self.history = HistoryStore(ctx)
        self.duckdb = DuckDbEngine()
        self.spark = SparkEngine(ctx)
        self._running: dict[str, tuple[RunHandle, str, str | None]] = {}
        self._run_lock = threading.Lock()
        self._slots = threading.BoundedSemaphore(max(1, self._int("sql.max_concurrent_queries", 4)))

    # ------------------------------------------------------------------ settings
    def _int(self, key: str, default: int) -> int:
        try:
            v = self.ctx.service("settings").get(key)
            return int(v) if v is not None else default
        except Exception:  # noqa: BLE001
            return default

    @property
    def storage(self):  # noqa: ANN201
        return self.ctx.service("storage")

    def limits(self) -> dict[str, int]:
        return {"default_max_rows": self._int("sql.default_max_rows", 10_000), "max_rows_cap": min(self._int("sql.max_rows_cap", 100_000), HARD_ROW_CAP),
                "default_timeout_seconds": self._int("sql.default_timeout_seconds", 60), "max_timeout_seconds": self._int("sql.max_timeout_seconds", 600),
                "duckdb_memory_mb": self._int("sql.duckdb_memory_mb", 1024), "duckdb_threads": self._int("sql.duckdb_threads", 2),
                "max_concurrent_queries": self._int("sql.max_concurrent_queries", 4), "max_file_scan_mb": self._int("sql.max_file_scan_mb", 256),
                "export_max_rows": self._int("sql.export_max_rows", 5_000_000)}

    def _engine_limits(self) -> EngineLimits:
        lim = self.limits()
        return EngineLimits(memory_mb=lim["duckdb_memory_mb"], threads=lim["duckdb_threads"], temp_dir=str(self.ctx.settings.path("tmp", "duckdb")))

    # ------------------------------------------------------------------ engines
    def engines(self) -> list[dict[str, Any]]:
        lim = self.limits()
        d_ok, d_ver = self.duckdb.available()
        s_ok, s_detail = self.spark.available()
        return [
            {"id": "duckdb", "label": self.duckdb.label, "default": True, "available": d_ok, "version": d_ver, "read_only": True,
             "capabilities": {"delta": True, "iceberg": _has_pyiceberg(), "parquet": True, "csv": True, "json": True, "writes": False, "cancel": True},
             "limits": {"memory_mb": lim["duckdb_memory_mb"], "threads": lim["duckdb_threads"], "max_rows_cap": lim["max_rows_cap"], "max_file_scan_mb": lim["max_file_scan_mb"],
                        "max_timeout_seconds": lim["max_timeout_seconds"]},
             "notes": "Embedded, read-only. Reference tables as <lakehouse>.<table>; read Files/ with read_csv/read_parquet/read_json."},
            {"id": "spark", "label": self.spark.label, "default": False, "available": s_ok, "version": None, "detail": s_detail, "read_only": False,
             "capabilities": {"delta": True, "iceberg": True, "parquet": True, "csv": True, "json": True, "writes": True, "cancel": False},
             "limits": {"max_rows_cap": lim["max_rows_cap"], "max_timeout_seconds": lim["max_timeout_seconds"]},
             "notes": "Runs on the compute fabric. Required for INSERT / CREATE TABLE AS / MERGE / UPDATE / DELETE (needs sql.write and lakehouse.write)."},
        ]

    # ------------------------------------------------------------------ planning
    def _context(self, principal: Principal, req: SqlRequest) -> tuple[str | None, Lakehouse | None]:
        default = self.storage.get_lakehouse(req.lakehouse, workspace_id=req.workspace_id) if req.lakehouse else None
        ws = req.workspace_id or (default.workspace_id if default else None)
        if ws:
            principal.require("sql.execute", workspace_id=ws)
        else:
            vis = principal.visible_workspaces("sql.execute")
            if vis is not None and not vis:
                raise Forbidden("permission 'sql.execute' is required", details={"permission": "sql.execute"})
        return ws, default

    def _clamp(self, req: SqlRequest, warnings: list[str]) -> tuple[int, int]:
        lim = self.limits()
        rows = req.max_rows if req.max_rows is not None else lim["default_max_rows"]
        if rows < 1:
            raise BadRequest("max_rows must be at least 1")
        if rows > lim["max_rows_cap"]:
            warnings.append(f"max_rows was reduced to the server limit of {lim['max_rows_cap']}; use the export endpoint for larger results")
            rows = lim["max_rows_cap"]
        t = req.timeout_seconds if req.timeout_seconds is not None else lim["default_timeout_seconds"]
        if t < 1:
            raise BadRequest("timeout_seconds must be at least 1")
        if t > lim["max_timeout_seconds"]:
            warnings.append(f"timeout_seconds was reduced to the server limit of {lim['max_timeout_seconds']}")
            t = lim["max_timeout_seconds"]
        return rows, t

    def plan(self, principal: Principal, req: SqlRequest, *, for_export: bool = False) -> Plan:
        """Parse, bind, authorise and prepare ``req`` without running it.  Raises on any problem (no history row is written)."""
        if req.engine not in ENGINES:
            raise BadRequest(f"unknown engine '{req.engine}'", details={"engines": list(ENGINES)})
        if req.query_id is not None and not QUERY_ID_RE.match(req.query_id):
            raise BadRequest("query_id must be 8-64 characters of letters, digits, '_' or '-'")
        warnings: list[str] = []
        values = guard.resolve_parameter_values(req.parameters, req.param_specs)
        stmt = guard.parse_statement(req.sql, req.engine, param_names=set(values))
        stmt, w = guard.bind_parameters(stmt, values, req.param_specs)
        warnings += w
        guard.require_read_only(stmt, req.engine)
        ws, default = self._context(principal, req)
        max_rows, timeout = self._clamp(req, warnings)
        if for_export:
            max_rows = self.limits()["export_max_rows"]
        plan = Plan(req=req, query_id=req.query_id or new_id("qry"), engine=req.engine, stmt=stmt, sql="", workspace_id=ws, default_lakehouse=default,
                    warnings=warnings, max_rows=max_rows, timeout=timeout)
        resolver = _Resolver(self, principal, ws, default)
        uses = guard.find_uses(stmt.ast, skip_nodes=self._target_nodes(stmt))
        if req.engine == "spark":
            self._plan_spark(principal, plan, resolver, uses)
        else:
            self._plan_duckdb(principal, plan, resolver, uses)
        return plan

    @staticmethod
    def _target_nodes(stmt: guard.Statement) -> set[int]:
        t = stmt.ast.args.get("this") if not stmt.read_only else None
        if isinstance(t, exp.Schema):
            t = t.this
        return {id(t)} if isinstance(t, exp.Table) else set()

    def _authorize_read(self, principal: Principal, lh: Lakehouse) -> None:
        principal.require("lakehouse.read", workspace_id=lh.workspace_id, item_id=lh.id)
        principal.require("sql.execute", workspace_id=lh.workspace_id)

    def _plan_duckdb(self, principal: Principal, plan: Plan, resolver: _Resolver, uses: list[guard.Use]) -> None:
        aliases: dict[Any, str] = {}
        tables_svc = self.storage.tables
        for u in uses:
            if u.kind == "table":
                lh, name = resolver.resolve(u.parts)
                self._authorize_read(principal, lh)
                real = self._real_table_name(lh, name)
                key: Any = ("t", lh.id, real)
                if key not in aliases:
                    alias = f"_olcf_s{len(aliases)}"
                    aliases[key] = alias
                    plan.sources[alias] = (lambda lh=lh, real=real: tables_svc.scan_source(lh, real))
                    plan.tables.append(f"{lh.name}.{real}")
                    plan.datasets.append({"kind": "table", "lakehouse_id": lh.id, "name": real})
                    plan.reads.append(lh)
                self._replace(u, aliases[key], u.alias or u.parts[-1])
            else:
                lh, paths = self._file_location(principal, plan, u)
                self._authorize_read(principal, lh)
                opts = _csv_options(u.options) if u.fmt == "csv" else {}
                key = ("f", lh.id, tuple(paths), u.fmt, tuple(sorted(opts.items())))
                if key not in aliases:
                    alias = f"_olcf_s{len(aliases)}"
                    aliases[key] = alias
                    plan.sources[alias] = (lambda lh=lh, paths=paths, fmt=u.fmt, opts=opts: self._load_files(lh, paths, fmt, opts))
                    plan.tables.append(f"{lh.name}:{paths[0]}" + (f" (+{len(paths) - 1})" if len(paths) > 1 else ""))
                    plan.datasets.extend({"kind": "file", "lakehouse_id": lh.id, "path": p} for p in paths)
                    plan.reads.append(lh)
                self._replace(u, aliases[key], u.alias or "f")
        changed = bool(plan.sources) or bool(plan.stmt.markers) or bool(plan.stmt.explain_prefix)
        # keep the author's text byte-for-byte unless something had to be rewritten (tables, parameters, EXPLAIN)
        plan.sql = guard.render(plan.stmt.ast, "duckdb", plan.stmt.explain_prefix) if changed else plan.req.sql

    @staticmethod
    def _replace(u: guard.Use, alias: str, display_alias: str) -> None:
        u.node.replace(exp.Table(this=exp.to_identifier(alias), alias=exp.TableAlias(this=exp.to_identifier(display_alias))))

    def _plan_spark(self, principal: Principal, plan: Plan, resolver: _Resolver, uses: list[guard.Use]) -> None:
        seen: set[str] = set()
        for u in uses:
            if u.kind != "table":
                lh = resolver.default or (self._only(resolver) if plan.workspace_id else None)
                if lh is None:
                    raise BadRequest("file paths need a default lakehouse", remediation="pass lakehouse= in the request")
                self._authorize_read(principal, lh)
                plan.reads.append(lh)
                continue
            lh, name = resolver.resolve(u.parts)
            self._authorize_read(principal, lh)
            plan.reads.append(lh)
            if f"{lh.id}:{name}" not in seen:
                seen.add(f"{lh.id}:{name}")
                plan.tables.append(f"{lh.name}.{name}")
                plan.datasets.append({"kind": "table", "lakehouse_id": lh.id, "name": name})
        if not plan.stmt.read_only:
            tnode = plan.stmt.ast.args.get("this")
            if isinstance(tnode, exp.Schema):
                tnode = tnode.this
            if isinstance(tnode, exp.Table) and tnode.name:
                parts = tuple(p.name for p in (tnode.args.get("catalog"), tnode.args.get("db"), tnode.this) if isinstance(p, exp.Identifier) and p.name)
                lh, name = resolver.resolve(parts)
                principal.require("sql.write", workspace_id=lh.workspace_id)
                principal.require("lakehouse.write", workspace_id=lh.workspace_id, item_id=lh.id)
                if type(plan.stmt.ast).__name__ in ("Drop", "TruncateTable"):
                    principal.require("lakehouse.destructive", workspace_id=lh.workspace_id, item_id=lh.id)
                plan.target = (lh, name)
                plan.tables.append(f"{lh.name}.{name} (write)")
            else:
                raise BadRequest("the statement's target table could not be determined", remediation="name the target as <lakehouse>.<table>")
        plan.sql = guard.render(plan.stmt.ast, "spark", plan.stmt.explain_prefix)
        guard.check_final_sql(plan.sql, "spark")  # the text the kernel receives: bound parameter values included (F02)

    def _only(self, resolver: _Resolver) -> Lakehouse | None:
        lakes = resolver.lakes(resolver.workspace_id) if resolver.workspace_id else {}
        return next(iter(lakes.values())) if len(lakes) == 1 else None

    def _real_table_name(self, lh: Lakehouse, name: str) -> str:
        tables = self.storage.tables
        try:
            return tables.locate(lh, name)["name"]
        except NotFound:
            pass
        names = [t["name"] for t in tables.discover(lh)]
        ci = [n for n in names if n.lower() == name.lower()]
        if len(ci) == 1:
            return ci[0]
        close = difflib.get_close_matches(name, names, n=3, cutoff=0.5)
        raise NotFound(f"table '{name}' does not exist in lakehouse '{lh.name}'",
                       remediation=("did you mean " + ", ".join(close) + "? " if close else "") + "list tables with GET /v1/sql/schema",
                       details={"tables": names[:50]})

    def _file_location(self, principal: Principal, plan: Plan, u: guard.Use) -> tuple[Lakehouse, list[str]]:
        out: list[str] = []
        lh_found: Lakehouse | None = None
        for p in u.paths:
            if p.startswith("olcf://") or p.startswith(("Files/", "Files\\")) or p == "Files":
                res = self.storage.resolve(p, default_lakehouse=plan.default_lakehouse, workspace_id=plan.workspace_id)
            else:
                raise BadRequest(f"file path '{p}' must be an olcf:// URI or a Files/... path", remediation="e.g. read_csv('Files/raw/sales.csv')")
            if res.area != "Files":
                raise BadRequest(f"'{p}' is not under Files/", remediation="read tables as <lakehouse>.<table>")
            lh = self.storage.get_lakehouse(res.lakehouse_id)
            if lh_found is not None and lh.id != lh_found.id:
                raise BadRequest("all paths of one read_* call must belong to the same lakehouse")
            lh_found = lh
            out.append(res.root_relative)
        if lh_found is None:
            raise BadRequest("the file reader has no path")
        return lh_found, out

    def _load_files(self, lh: Lakehouse, paths: list[str], fmt: str | None, options: dict[str, Any]) -> pa.Table:
        tables = self.storage.tables
        fs = self.storage.filesystem(lh)
        files: list[Any] = []
        for p in paths:
            files += tables._source_files(fs, p, fmt)
        cap = self.limits()["max_file_scan_mb"] * MiB
        total = sum(f.size for f in files)
        if total > cap:
            raise Unsupported(f"the files total {total / MiB:.0f} MiB, above the embedded engine's file-scan limit of {cap // MiB} MiB",
                              remediation="load them into a Delta table first (POST /v1/lakehouses/{id}/tables) and query the table, or use the Spark engine",
                              details={"bytes": total, "limit_bytes": cap})
        return tables._reader(fs, [f.path for f in files], fmt or tables._guess_format(files[0].path), options).read_all()

    # ------------------------------------------------------------------ running
    def _acquire(self, handle: RunHandle, timeout: float) -> None:
        wait = min(30.0, max(1.0, timeout))
        deadline = time.monotonic() + wait
        while not self._slots.acquire(timeout=0.2):
            handle.check()
            if time.monotonic() > deadline:
                raise TooManyRequests(f"the SQL service is busy ({self.limits()['max_concurrent_queries']} concurrent statements)",
                                      remediation="retry in a few seconds", details={"retry_after_seconds": 5})

    def _open(self, principal: Principal, plan: Plan, handle: RunHandle) -> Stream:
        if plan.engine == "spark":
            ws = plan.workspace_id or (plan.target[0].workspace_id if plan.target else (plan.reads[0].workspace_id if plan.reads else None))
            if ws is None:
                raise BadRequest("the Spark engine needs a workspace", remediation="pass workspace_id or lakehouse")
            lh_ref = plan.default_lakehouse.id if plan.default_lakehouse else (plan.target[0].id if plan.target else (plan.reads[0].id if plan.reads else None))
            return self.spark.open(principal, ws, plan.sql, lh_ref, plan.max_rows, plan.timeout, handle)
        return self.duckdb.open(plan.sql, plan.sources, self._engine_limits(), handle)

    def execute(self, principal: Principal, req: SqlRequest) -> SqlResult:
        """Run ``req`` and return a typed, row-capped result.  Every attempt that parses is recorded in the query history."""
        plan = self.plan(principal, req)
        handle = RunHandle(plan.query_id)
        started = time.perf_counter()
        self._begin(principal, plan, handle)
        timer = threading.Timer(plan.timeout, handle.cancel, args=("timeout",))
        timer.daemon = True
        slot = False
        stream: Stream | None = None
        try:
            self._acquire(handle, plan.timeout)
            slot = True
            timer.start()
            stream = self._open(principal, plan, handle)
            table, truncated = _collect(stream, plan.max_rows)
            handle.check()
        except BaseException as exc:
            err = self._classify_error(exc, handle)
            self._finish_error(plan, err, started)
            raise err from None
        finally:
            timer.cancel()
            if stream is not None:
                stream.close()
            if slot:
                self._slots.release()
            self._end(plan.query_id)
        elapsed = int((time.perf_counter() - started) * 1000)
        columns = column_descriptors(table.schema)
        rows = table_rows(table)
        truncated = truncated or bool(stream and stream.info.get("truncated"))
        self.history.finish(plan.query_id, status="ok", rows=len(rows), truncated=truncated, elapsed_ms=elapsed, tables=plan.tables,
                            workspace_id=plan.workspace_id, lakehouse_id=_lh_id(plan), kind=plan.stmt.kind)
        self._after_write(principal, plan)
        self._emit_dataset_events(plan)
        stats = {"elapsed_ms": elapsed, "rows_returned": len(rows), "tables": plan.tables, **({"engine_version": stream.info["engine_version"]} if stream and "engine_version" in stream.info else {})}
        return SqlResult(plan.query_id, plan.engine, plan.stmt.kind, columns, rows, truncated, stats, plan.warnings)

    # ------------------------------------------------------------------ lineage events
    @staticmethod
    def _write_mode(plan: Plan) -> str | None:
        """How the statement changes its target table, or ``None`` when it does not write data (DDL such as DROP / ALTER / TRUNCATE)."""
        kind, ast = plan.stmt.kind, plan.stmt.ast
        if kind == "insert":
            return "overwrite" if ast.args.get("overwrite") else "append"
        if kind in ("ctas", "create_view"):
            return "overwrite" if ast.args.get("replace") else "create"
        return {"update": "update", "delete": "delete", "merge": "merge"}.get(kind)

    def _emit_dataset_events(self, plan: Plan) -> None:
        """Publish ``dataset.read`` / ``dataset.written`` / ``dataset.statement`` for a statement that just succeeded (never raises, never waits for a consumer).

        The tables and files come from :attr:`Plan.datasets` (the table extraction the planner already did to authorise and resolve the statement; no second parser here)
        and ``Plan.target`` for writes. ``statement_hash`` is the sha1 of the whitespace-normalised SQL text; the process is the saved query / report item when there is one.
        """
        ev: dict[str, Any] | None = None
        with dsev.best_effort(log, "dataset events (sql)"):
            req = plan.req
            ws = plan.workspace_id or (plan.target[0].workspace_id if plan.target else (plan.reads[0].workspace_id if plan.reads else None))
            proc = dsev.process_descriptor("report" if req.source == "report" else "sql_query", item_id=req.item_id, query_id=plan.query_id, name=req.label)
            reads = [d for d in (dsev.from_catalog_dataset(x) for x in plan.datasets[:200]) if d is not None]
            mode = self._write_mode(plan) if plan.target else None
            written = [dsev.dataset_descriptor("table", lakehouse_id=plan.target[0].id, name=plan.target[1])] if plan.target and mode else []
            ev = {"ws": ws, "base": {"workspace_id": ws, "process": proc}, "reads": reads, "written": written, "mode": mode, "hash": dsev.statement_hash(req.sql), "at": now_iso()}
        if ev is None:
            return
        base = ev["base"]
        if ev["reads"]:
            with dsev.best_effort(log, "dataset.read"):
                self.ctx.events.publish("dataset.read", {**base, "datasets": ev["reads"], "engine": plan.engine, "mode": None, "statement_hash": ev["hash"], "at": ev["at"]},
                                        workspace_id=ev["ws"], subject=plan.query_id)
        if ev["written"]:
            with dsev.best_effort(log, "dataset.written"):
                self.ctx.events.publish("dataset.written", {**base, "datasets": ev["written"], "engine": plan.engine, "mode": ev["mode"], "statement_hash": ev["hash"], "at": ev["at"]},
                                        workspace_id=ev["ws"], subject=plan.query_id)
        if ev["reads"] or ev["written"]:
            # the SQL text is not persisted in the event table (webhooks, GET /v1/events and SSE read that table and would expose other users' queries): only in-process subscribers see it
            with dsev.best_effort(log, "dataset.statement"):
                self.ctx.events.publish("dataset.statement", {**base, "sql": dsev.clip_statement(plan.req.sql), "dialect": plan.engine, "default_lakehouse_id": _lh_default_id(plan),
                                                                  "statement_hash": ev["hash"], "at": ev["at"]},
                                        workspace_id=ev["ws"], subject=plan.query_id, persist=False)

    def _begin(self, principal: Principal, plan: Plan, handle: RunHandle) -> None:
        with self._run_lock:
            if plan.query_id in self._running:
                raise BadRequest(f"query_id '{plan.query_id}' is already running")
            self._running[plan.query_id] = (handle, principal.id, plan.workspace_id)
        try:
            self.history.start(plan.query_id, principal, engine=plan.engine, source=plan.req.source, sql=plan.req.sql, workspace_id=plan.workspace_id,
                               lakehouse_id=_lh_id(plan), item_id=plan.req.item_id, kind=plan.stmt.kind)
        except Exception:
            self._end(plan.query_id)
            raise

    def _end(self, query_id: str) -> None:
        with self._run_lock:
            self._running.pop(query_id, None)

    def _classify_error(self, exc: BaseException, handle: RunHandle) -> OlcfError:
        if handle.cancelled.is_set() and not isinstance(exc, (Forbidden,)):
            return interrupted(handle.reason)
        if isinstance(exc, OlcfError):
            return exc
        log.exception("unexpected SQL failure")
        return OlcfError(f"unexpected error while running the statement: {type(exc).__name__}")

    def _finish_error(self, plan: Plan, err: OlcfError, started: float) -> None:
        status = {"query_timeout": "timeout", "query_cancelled": "cancelled"}.get(err.code, "denied" if isinstance(err, Forbidden) else "error")
        try:
            self.history.finish(plan.query_id, status=status, elapsed_ms=int((time.perf_counter() - started) * 1000), error_code=err.code, error_message=err.message,
                                tables=plan.tables, workspace_id=plan.workspace_id, lakehouse_id=_lh_id(plan), kind=plan.stmt.kind)
        except Exception:  # noqa: BLE001
            log.exception("could not record query failure")

    def _after_write(self, principal: Principal, plan: Plan) -> None:
        """Spark writes: refresh the catalog entry of the target and record SQL-derived lineage (best effort, never fails the query)."""
        if plan.target is None:
            return
        lh, name = plan.target
        cat = self.ctx.maybe_service("catalog")
        self.ctx.audit.record(principal, "sql.write", resource_type="lakehouse", resource_id=lh.id, workspace_id=lh.workspace_id,
                              details={"table": name, "statement": plan.stmt.kind, "query_id": plan.query_id})
        if cat is None:
            return
        try:
            cat.sync_lakehouse(lh, tables=[name])
            cat.record_lineage_from_sql(principal, lh.workspace_id, plan.req.sql, dialect="spark", default_lakehouse_id=plan.default_lakehouse.id if plan.default_lakehouse else lh.id,
                                        via={"kind": "sql", "name": f"{plan.stmt.kind} {name}", "ref": plan.query_id}, authorize=False)
        except Exception:  # noqa: BLE001
            log.warning("catalog update after SQL write failed", exc_info=True)

    # ------------------------------------------------------------------ cancellation
    def cancel(self, principal: Principal, query_id: str) -> dict[str, Any]:
        """Cancel a running statement (own statements, or any in a workspace the caller administers)."""
        with self._run_lock:
            entry = self._running.get(query_id)
        if entry is None or (entry[1] != principal.id and not self.history.can_see_all(principal, entry[2])):
            raise NotFound(f"no running query '{query_id}'")
        entry[0].cancel("cancelled")
        return {"query_id": query_id, "cancelling": True}

    def running(self, principal: Principal) -> list[dict[str, Any]]:
        with self._run_lock:
            items = [(q, h, pid, ws) for q, (h, pid, ws) in self._running.items()]
        return [{"query_id": q, "workspace_id": ws, "principal_id": pid} for q, _h, pid, ws in items if pid == principal.id or self.history.can_see_all(principal, ws)]

    # ------------------------------------------------------------------ explain / saved queries / schema
    def explain(self, principal: Principal, req: SqlRequest) -> dict[str, Any]:
        """Describe how a read statement would run: resolved tables, statement kind and the engine's plan text (DuckDB)."""
        if req.engine != "duckdb":
            raise Unsupported("EXPLAIN is only available on the DuckDB engine through this API", remediation="use engine='duckdb' or prefix the statement with EXPLAIN in a Spark notebook")
        plan = self.plan(principal, req)
        if plan.stmt.explain_prefix:
            raise BadRequest("send the plain statement; the service adds EXPLAIN itself")
        explain_sql = "EXPLAIN " + plan.sql
        handle = RunHandle(new_id("qry"))
        stream = self.duckdb.open(explain_sql, plan.sources, self._engine_limits(), handle)
        try:
            table = pa.Table.from_batches(list(stream.batches)) if stream.schema else None
        finally:
            stream.close()
        text = "\n".join(str(v) for row in table_rows(table) for v in row[1:]) if table is not None and table.num_columns > 1 else ""
        return {"engine": "duckdb", "statement": plan.stmt.kind, "tables": plan.tables, "plan": text, "warnings": plan.warnings}

    def run_saved(self, principal: Principal, item_id: str, parameters: dict[str, Any] | None = None, *, max_rows: int | None = None,
                  timeout_seconds: int | None = None, query_id: str | None = None) -> SqlResult:
        item = self.ctx.service("items").get(item_id)
        if item["type"] != "sql_query":
            raise BadRequest(f"item '{item_id}' is a {item['type']}, not a saved query")
        principal.require("item.read", workspace_id=item["workspace_id"], item_id=item["id"])
        d = item["definition"] or {}
        return self.execute(principal, SqlRequest(sql=d["sql"], engine=d.get("engine") or "duckdb", workspace_id=context_workspace(item["workspace_id"], d.get("lakehouse_id")),
                                                  lakehouse=d.get("lakehouse_id"),
                                                  parameters=parameters or {}, param_specs=d.get("parameters") or [], max_rows=max_rows, timeout_seconds=timeout_seconds,
                                                  query_id=query_id, source="saved_query", item_id=item_id))

    def schema(self, principal: Principal, workspace_id: str | None = None, lakehouse: str | None = None) -> dict[str, Any]:
        """Lakehouses -> tables -> columns the caller can query, with the exact name to use in SQL (for editors / agents)."""
        vis = principal.visible_workspaces("sql.execute")
        if workspace_id:
            principal.require("sql.execute", workspace_id=workspace_id)
            wss = [workspace_id]
        else:
            with self.ctx.db.connect() as conn:
                from sqlalchemy import select

                from olcf.db import schema as S
                from olcf.db.util import fetch_all

                stmt = select(S.workspaces.c.id).where(S.workspaces.c.deleted_at.is_(None)) if "deleted_at" in S.workspaces.c else select(S.workspaces.c.id)
                if vis is not None:
                    stmt = stmt.where(S.workspaces.c.id.in_(sorted(vis) or [""]))
                wss = [r["id"] for r in fetch_all(conn, stmt)]
        cat = self.ctx.maybe_service("catalog")
        out = []
        for ws in wss:
            for lh in self.storage.list_lakehouses(ws):
                if lakehouse and lakehouse not in (lh.id, lh.name):
                    continue
                if not principal.can("lakehouse.read", workspace_id=lh.workspace_id, item_id=lh.id):
                    continue
                tables: list[dict[str, Any]] = []
                if cat is not None:
                    rows, _ = cat.list_tables(lh.id, limit=500, columns=True)
                    if not rows:                       # never discovered yet: do it now so editors and agents get columns
                        try:
                            cat.sync_lakehouse(lh)
                            rows, _ = cat.list_tables(lh.id, limit=500, columns=True)
                        except OlcfError:
                            log.debug("catalog sync for schema failed", exc_info=True)
                    tables = [{"name": r["name"], "format": r["format"], "reference": f"{lh.name}.{r['name']}", "partition_columns": r["partition_columns"],
                               "columns": [{"name": c["name"], "type": c["type"], "nullable": c.get("nullable", True)} for c in r["columns"]]} for r in rows]
                if not tables:
                    try:
                        tables = [{"name": t["name"], "format": t["format"], "reference": f"{lh.name}.{t['name']}", "columns": None} for t in self.storage.list_tables(lh)]
                    except OlcfError:
                        tables = []
                out.append({"id": lh.id, "name": lh.name, "workspace_id": lh.workspace_id, "workspace": lh.workspace_name, "catalog": lh.catalog, "tables": tables})
        return {"lakehouses": out}

    # ------------------------------------------------------------------ export (streaming)
    def open_export(self, principal: Principal, req: SqlRequest, fmt: str, filename: str | None = None) -> tuple[str, str, Iterator[bytes]]:
        """Authorise and prepare a streaming export; returns ``(content_type, filename, byte iterator)``.  Rows are never buffered whole."""
        from olcf.sql import export

        if fmt not in export.FORMATS:
            raise BadRequest(f"unknown format '{fmt}'", details={"formats": sorted(export.FORMATS)})
        req.source = "export"
        plan = self.plan(principal, req, for_export=True)
        handle = RunHandle(plan.query_id)
        self._begin(principal, plan, handle)
        started = time.perf_counter()
        state = {"rows": 0, "truncated": False}
        timer = threading.Timer(max(plan.timeout, 1) * 10, handle.cancel, args=("timeout",))   # exports get 10x the interactive budget
        timer.daemon = True

        try:
            self._acquire(handle, plan.timeout)
        except BaseException as exc:
            err = self._classify_error(exc, handle)
            self._finish_error(plan, err, started)
            self._end(plan.query_id)
            raise err from None
        try:
            timer.start()
            stream = self._open(principal, plan, handle)
        except BaseException as exc:
            timer.cancel()
            self._slots.release()
            err = self._classify_error(exc, handle)
            self._finish_error(plan, err, started)
            self._end(plan.query_id)
            raise err from None

        def body() -> Iterator[bytes]:
            err: OlcfError | None = None
            try:
                yield from export.encode(stream, fmt, plan.max_rows, state)
                handle.check()
            except GeneratorExit:
                handle.cancel("cancelled")
                raise
            except BaseException as exc:
                err = self._classify_error(exc, handle)
                raise
            finally:
                timer.cancel()
                stream.close()
                self._slots.release()
                self._end(plan.query_id)
                if err is not None:
                    self._finish_error(plan, err, started)
                elif handle.cancelled.is_set():
                    self._finish_error(plan, interrupted(handle.reason), started)
                else:
                    self.history.finish(plan.query_id, status="ok", rows=state["rows"], truncated=state["truncated"], elapsed_ms=int((time.perf_counter() - started) * 1000),
                                        tables=plan.tables, workspace_id=plan.workspace_id, lakehouse_id=_lh_id(plan), kind=plan.stmt.kind)
                    self._emit_dataset_events(plan)

        name = (filename or f"query-{plan.query_id}").rsplit(".", 1)[0]
        safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in name)[:80] or "export"
        return export.FORMATS[fmt][0], f"{safe}.{export.FORMATS[fmt][1]}", body()

    # ------------------------------------------------------------------ health
    def deep_check(self, engine: str) -> tuple[bool, list[dict[str, Any]]]:
        """Real end-to-end probe: ``SELECT 1`` plus a Delta table read through the same registration path real queries use."""
        steps: list[dict[str, Any]] = []

        def step(name: str, fn: Callable[[], str]) -> bool:
            t0 = time.perf_counter()
            try:
                detail = fn()
                steps.append({"name": name, "ok": True, "detail": detail, "ms": round((time.perf_counter() - t0) * 1000, 1)})
                return True
            except Exception as exc:  # noqa: BLE001
                steps.append({"name": name, "ok": False, "detail": f"{type(exc).__name__}: {str(exc)[:300]}", "ms": round((time.perf_counter() - t0) * 1000, 1)})
                return False

        if engine == "duckdb":
            def select_one() -> str:
                con = self.duckdb.connect(self._engine_limits())
                try:
                    return f"SELECT 1 -> {con.execute('SELECT 1').fetchone()[0]} (DuckDB {self.duckdb.available()[1]})"
                finally:
                    con.close()

            def temp_read() -> str:
                import tempfile

                from deltalake import write_deltalake

                with tempfile.TemporaryDirectory(prefix="olcf-sql-probe-") as d:
                    write_deltalake(d, pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]}))
                    from deltalake import DeltaTable

                    h = RunHandle("probe")
                    s = self.duckdb.open("SELECT count(*) AS n, sum(a) AS s FROM _olcf_probe", {"_olcf_probe": lambda: DeltaTable(d).to_pyarrow_dataset()}, self._engine_limits(), h)
                    try:
                        t = pa.Table.from_batches(list(s.batches))
                    finally:
                        s.close()
                    row = t.to_pylist()[0]
                    if (row["n"], row["s"]) != (3, 6):
                        raise RuntimeError(f"unexpected probe result {row}")
                    return "Delta table written and read back through the engine (3 rows)"

            def sample_read() -> str:
                sample = self._sample_lakehouse()
                if sample is None:
                    return "sample lakehouse not installed; skipped (the Delta probe above exercised the table path)"
                lh, table = sample
                h = RunHandle("probe2")
                s = self.duckdb.open("SELECT count(*) AS n FROM _olcf_probe", {"_olcf_probe": lambda: self.storage.tables.scan_source(lh, table)}, self._engine_limits(), h)
                try:
                    n = pa.Table.from_batches(list(s.batches)).to_pylist()[0]["n"]
                finally:
                    s.close()
                return f"{lh.name}.{table}: {n} rows"

            ok = step("select 1", select_one)
            ok = step("delta table read", temp_read) and ok
            ok = step("sample table read", sample_read) and ok
            return ok, steps
        if engine == "spark":
            ok_avail, detail = self.spark.available()
            steps.append({"name": "compute service", "ok": ok_avail, "detail": detail, "ms": 0.0})
            if not ok_avail:
                return False, steps
            compute = self.ctx.service("compute")

            def spark_one() -> str:
                from sqlalchemy import select

                from olcf.db import schema as S

                with self.ctx.db.connect() as conn:
                    ws = conn.execute(select(S.workspaces.c.id).limit(1)).scalar()
                if ws is None:
                    return "no workspace exists yet to run the probe in; skipped"
                res = compute.run_spark_sql(_system_principal(), ws, "SELECT 1 AS one", lakehouse=None, limit=1, timeout=30)
                return f"SELECT 1 -> {res.get('rows')}"

            return step("select 1", spark_one), steps
        raise BadRequest(f"unknown engine '{engine}'")

    def _sample_lakehouse(self) -> tuple[Lakehouse, str] | None:
        from sqlalchemy import select

        from olcf.db import schema as S
        from olcf.db.util import fetch_one

        with self.ctx.db.connect() as conn:
            ws = fetch_one(conn, select(S.workspaces).where(S.workspaces.c.name == "Sample Retail"))
            if not ws:
                return None
            item = fetch_one(conn, select(S.items).where(S.items.c.workspace_id == ws["id"], S.items.c.type == "lakehouse", S.items.c.name == "retail_lakehouse",
                                                         S.items.c.deleted_at.is_(None)))
        if not item:
            return None
        lh = self.storage.lakehouse_from_item(item)
        return (lh, "dim_store") if any(t["name"] == "dim_store" for t in self.storage.list_tables(lh)) else None


def context_workspace(item_workspace: str, lakehouse: str | None) -> str | None:
    """Workspace for a saved query / report tile: a lakehouse *id* names its own workspace (items may read lakehouses of other workspaces the
    caller can access); a lakehouse *name* is resolved inside the item's workspace."""
    return None if lakehouse and lakehouse.startswith("itm_") else item_workspace


def _system_principal() -> Principal:
    from olcf.security.rbac import Binding

    return Principal(id="system", name="system", kind="service", bindings=[Binding("PlatformAdmin")])


def _lh_default_id(plan: Plan) -> str | None:
    """The default lakehouse unqualified names resolved against (``None`` when the request had none)."""
    return plan.default_lakehouse.id if plan.default_lakehouse else None


def _lh_id(plan: Plan) -> str | None:
    if plan.target:
        return plan.target[0].id
    if plan.default_lakehouse:
        return plan.default_lakehouse.id
    return plan.reads[0].id if plan.reads else None


def _has_pyiceberg() -> bool:
    from olcf.storage import iceberg

    return iceberg.pyiceberg_available()


def _csv_options(o: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if "header" in o:
        out["header"] = bool(o["header"])
    for k in ("delim", "delimiter", "sep"):
        if k in o:
            out["delimiter"] = str(o[k])
    return out


def _collect(stream: Stream, max_rows: int) -> tuple[pa.Table, bool]:
    """Read at most ``max_rows`` rows (+1 to detect truncation) from the stream."""
    batches: list[pa.RecordBatch] = []
    n = 0
    truncated = False
    for b in stream.batches:
        if n + b.num_rows > max_rows:
            take = max_rows - n
            if take > 0:
                batches.append(b.slice(0, take))
                n += take
            truncated = True
            break
        batches.append(b)
        n += b.num_rows
    table = pa.Table.from_batches(batches, schema=stream.schema) if batches else stream.schema.empty_table()
    return table, truncated

