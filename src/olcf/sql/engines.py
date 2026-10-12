# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""SQL engines.

* :class:`DuckDbEngine` - embedded, read-only, hardened.  Every query gets a fresh in-memory connection with external
  access disabled and the configuration locked; lakehouse tables and files are handed to DuckDB as Arrow objects
  registered under private names, so DuckDB never touches a path or a credential itself.
* :class:`SparkEngine` - delegates to the compute service's pooled Spark SQL session (``run_spark_sql``); a clear
  ``Unavailable`` error explains how to enable it when no compute service/worker is present.

Both produce a :class:`Stream` of Arrow record batches so row caps, JSON conversion and exports share one code path.
"""

from __future__ import annotations

import datetime as dt
import logging
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pyarrow as pa

from olcf.core.errors import BadRequest, OlcfError, Unavailable, Unsupported

log = logging.getLogger("olcf.sql")


class RunHandle:
    """Cooperative cancellation shared between the service (cancel/timeout) and a running engine."""

    def __init__(self, query_id: str) -> None:
        self.query_id = query_id
        self.cancelled = threading.Event()
        self.reason: str | None = None          # "cancelled" | "timeout"
        self._interrupt: Callable[[], None] | None = None
        self._lock = threading.Lock()

    def set_interrupt(self, fn: Callable[[], None]) -> None:
        with self._lock:
            self._interrupt = fn
        if self.cancelled.is_set():
            self._fire()

    def cancel(self, reason: str = "cancelled") -> None:
        if not self.cancelled.is_set():
            self.reason = reason
            self.cancelled.set()
        self._fire()

    def _fire(self) -> None:
        with self._lock:
            fn = self._interrupt
        if fn:
            try:
                fn()
            except Exception:  # noqa: BLE001
                log.debug("interrupt failed", exc_info=True)

    def check(self) -> None:
        if self.cancelled.is_set():
            raise interrupted(self.reason)


class QueryInterrupted(OlcfError):
    """The statement was cancelled or hit its timeout; build with :func:`interrupted`."""

    status = 408
    code = "query_interrupted"


class QueryTimeout(QueryInterrupted):
    """The query exceeded its time limit (HTTP 408, code ``query_timeout``)."""
    status = 408
    code = "query_timeout"


class QueryCancelled(QueryInterrupted):
    """The query was cancelled (HTTP 499, code ``query_cancelled``)."""
    status = 499
    code = "query_cancelled"


def interrupted(reason: str | None) -> QueryInterrupted:
    if reason == "timeout":
        return QueryTimeout("the statement exceeded its time limit",
                            remediation="add filters, reduce the data scanned, or raise timeout_seconds (up to sql.max_timeout_seconds)")
    return QueryCancelled("the statement was cancelled")


@dataclass
class Stream:
    """Schema plus a lazy iterator of Arrow batches; ``close()`` releases the engine resources."""

    schema: pa.Schema
    batches: Iterator[pa.RecordBatch]
    close: Callable[[], None] = lambda: None
    info: dict[str, Any] = field(default_factory=dict)

    def __iter__(self) -> Iterator[pa.RecordBatch]:
        return self.batches


@dataclass
class EngineLimits:
    """Resource limits for the embedded SQL engine: memory, threads and temporary space."""
    memory_mb: int = 1024
    threads: int = 2
    temp_dir: str | None = None
    temp_max_mb: int = 2048
    batch_rows: int = 8192


def _translate_duckdb(exc: Exception, handle: RunHandle | None) -> Exception:
    import duckdb

    if handle is not None and handle.cancelled.is_set():
        return interrupted(handle.reason)
    msg = str(exc).strip().splitlines()[0][:600] if str(exc).strip() else exc.__class__.__name__
    if isinstance(exc, duckdb.InterruptException):
        return interrupted("cancelled")
    if isinstance(exc, duckdb.OutOfMemoryException):
        return Unsupported("the query exceeded the embedded engine's memory limit", remediation="add filters or aggregation, select fewer columns, or run it on the Spark engine",
                           details={"engine_message": msg})
    if isinstance(exc, duckdb.PermissionException):
        return BadRequest("this operation is not permitted in the embedded SQL engine (file system and network access are disabled)",
                          remediation="query lakehouse tables as <lakehouse>.<table> or read Files/ paths with read_csv/read_parquet/read_json", details={"engine_message": msg})
    if isinstance(exc, duckdb.Error):
        return BadRequest(f"SQL error: {msg}", details={"engine": "duckdb", "kind": exc.__class__.__name__})
    return exc


class DuckDbEngine:
    """Embedded DuckDB with one hardened in-memory connection per query."""

    name = "duckdb"
    label = "DuckDB (embedded)"
    read_only = True

    def available(self) -> tuple[bool, str]:
        try:
            import duckdb

            return True, duckdb.__version__
        except ImportError:
            return False, "duckdb is not installed"

    def connect(self, limits: EngineLimits) -> Any:
        import duckdb

        cfg = {
            "enable_external_access": "false", "autoload_known_extensions": "false", "autoinstall_known_extensions": "false",
            "memory_limit": f"{int(limits.memory_mb)}MB", "threads": str(max(1, int(limits.threads))),
            "max_temp_directory_size": f"{int(limits.temp_max_mb)}MB", "python_enable_replacements": "false",
        }
        if limits.temp_dir:
            Path(limits.temp_dir).mkdir(parents=True, exist_ok=True)
            cfg["temp_directory"] = limits.temp_dir
        con = duckdb.connect(":memory:", config=cfg)
        con.execute("SET lock_configuration = true")   # nothing the SQL does can undo the settings above
        return con

    def open(self, sql: str, sources: dict[str, Callable[[], Any]], limits: EngineLimits, handle: RunHandle) -> Stream:
        con = self.connect(limits)
        handle.set_interrupt(con.interrupt)
        closed = threading.Event()

        def close() -> None:
            if not closed.is_set():
                closed.set()
                try:
                    con.close()
                except Exception:  # noqa: BLE001
                    log.debug("duckdb close failed", exc_info=True)

        try:
            for alias, loader in sources.items():
                handle.check()
                con.register(alias, loader())
            handle.check()
            cur = con.execute(sql)
            reader = cur.to_arrow_reader(limits.batch_rows) if hasattr(cur, "to_arrow_reader") else cur.fetch_record_batch(limits.batch_rows)
        except BaseException as exc:
            close()
            raise _translate_duckdb(exc, handle) from None

        def gen() -> Iterator[pa.RecordBatch]:
            try:
                yield from reader
            except Exception as exc:  # noqa: BLE001
                raise _translate_duckdb(exc, handle) from None
            finally:
                close()

        return Stream(reader.schema, gen(), close, info={"engine_version": self.available()[1]})


_SPARK_TYPES: dict[str, pa.DataType] = {
    "bigint": pa.int64(), "long": pa.int64(), "int": pa.int64(), "integer": pa.int64(), "smallint": pa.int64(), "tinyint": pa.int64(), "short": pa.int64(),
    "double": pa.float64(), "float": pa.float64(), "real": pa.float64(), "boolean": pa.bool_(), "string": pa.string(), "binary": pa.string(),
}


def rows_to_arrow(columns: list[Any], rows: list[Any]) -> pa.Table:
    """Build an Arrow table from a JSON-style result (``columns`` as names or ``{name, type}``; rows as lists or dicts)."""
    names: list[str] = []
    types: list[str] = []
    for c in columns:
        if isinstance(c, dict):
            names.append(str(c.get("name")))
            types.append(str(c.get("type") or "").lower())
        else:
            names.append(str(c))
            types.append("")
    data: list[list[Any]] = [[] for _ in names]
    for r in rows:
        vals = [r.get(n) for n in names] if isinstance(r, dict) else list(r)
        for i in range(len(names)):
            data[i].append(vals[i] if i < len(vals) else None)
    arrays = []
    for i in range(len(names)):
        t = types[i].split("(")[0].strip()
        col = data[i]
        try:
            if t == "date":
                arrays.append(pa.array([None if v is None else dt.date.fromisoformat(str(v)[:10]) for v in col], pa.date32()))
            elif t.startswith("timestamp"):
                arrays.append(pa.array([None if v is None else dt.datetime.fromisoformat(str(v).replace("Z", "+00:00")) for v in col]))
            elif t in _SPARK_TYPES:
                arrays.append(pa.array(col, _SPARK_TYPES[t]))
            else:
                arrays.append(pa.array(col))
        except (pa.ArrowInvalid, pa.ArrowTypeError, ValueError, TypeError):
            arrays.append(pa.array([None if v is None else str(v) for v in col], pa.string()))
    return pa.table(arrays, names=names) if names else pa.table({})


class SparkEngine:
    """Delegates to ``ComputeService.run_spark_sql`` (a pooled Spark SQL session on a worker)."""

    name = "spark"
    label = "Spark SQL (compute fabric)"
    read_only = False

    def __init__(self, ctx: Any) -> None:
        self.ctx = ctx

    def _compute(self) -> Any:
        compute = self.ctx.maybe_service("compute")
        if compute is None or not hasattr(compute, "run_spark_sql"):
            raise Unavailable("the Spark SQL engine needs the compute service, which is not running in this deployment",
                              remediation="enable the compute module and enroll a worker with a Spark runtime (see Admin -> Compute), or use engine='duckdb' for read-only SQL",
                              details={"engine": "spark"})
        return compute

    def available(self) -> tuple[bool, str]:
        compute = self.ctx.maybe_service("compute")
        if compute is None or not hasattr(compute, "run_spark_sql"):
            return False, "compute service not available"
        return True, "delegated to the compute service"

    def open(self, principal: Any, workspace_id: str, sql: str, lakehouse: str | None, limit: int, timeout: int, handle: RunHandle) -> Stream:
        compute = self._compute()
        handle.check()
        done = threading.Event()
        if hasattr(compute, "cancel_spark_sql"):
            handle.set_interrupt(lambda: None if done.is_set() else compute.cancel_spark_sql(principal, handle.query_id))
        try:
            res = compute.run_spark_sql(principal, workspace_id, sql, lakehouse=lakehouse, limit=limit, timeout=timeout)
        except OlcfError:
            raise
        except TimeoutError as exc:
            raise interrupted("timeout") from exc
        except Exception as exc:  # noqa: BLE001
            raise Unavailable(f"the Spark SQL session failed: {str(exc)[:300]}", remediation="check Admin -> Compute -> Runtimes for a healthy Spark runtime") from exc
        finally:
            done.set()
        handle.check()
        if not isinstance(res, dict):
            raise Unavailable("the compute service returned an unexpected Spark SQL result")
        table = rows_to_arrow(res.get("columns") or [], res.get("rows") or [])
        return Stream(table.schema, iter(table.to_batches()), info={"truncated": bool(res.get("truncated")), "spark": res.get("stats") or {}})
