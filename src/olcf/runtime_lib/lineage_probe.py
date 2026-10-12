# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Runtime lineage probe: tells the control plane which datasets a job or notebook really read and wrote (CONTRACTS / INTELLIGENCE contract section 7).

It runs **inside the job process** (runner, notebook kernel, interactive session), next to user code, and reports through the normal ``@@OLCF@@`` event channel
(:func:`olcf.runtime_lib.bootstrap.emit_event`). It is stdlib-only (plus the stdlib-only redaction helper), imports nothing heavy and never imports ``olcf.intelligence``;
the control plane (``JobService.forward_dataset_event``) validates the lines and republishes them as the platform events ``dataset.read`` / ``dataset.written`` /
``dataset.statement``.

Event lines
-----------
::

    @@OLCF@@ {"event":"dataset_read","datasets":[{"kind":"table","lakehouse_id":"itm_..","name":"Silver.sales","format":"delta"}],"at":"2026-01-01T00:00:00.000Z"}
    @@OLCF@@ {"event":"dataset_written","datasets":[{"kind":"file","lakehouse_id":"itm_..","path":"Files/out/x","uri":"abfss://.."}],"mode":"append","at":"..."}
    @@OLCF@@ {"event":"dataset_statement","sql":"INSERT INTO gold.t SELECT ...","dialect":"spark","default_lakehouse_id":"itm_..","at":"..."}
    @@OLCF@@ {"event":"dataset_events_truncated","limit":200}

* A dataset is ``{"kind": "table"|"file", "lakehouse_id", "name"|"path", "uri", "format"}``; keys without a value are omitted. Unqualified table names resolve against the
  job's default lakehouse, ``<lakehouse>.<table>`` against the lakehouses attached to the job, paths under a lakehouse root become lakehouse-relative (``Tables/x`` is the
  table ``x``, anything else a file).
* Events are **de-duplicated** per process (same event, datasets and mode are reported once) and **capped** at :data:`MAX_EVENTS` per process; the first event over the cap
  emits one ``dataset_events_truncated`` marker and everything after it is dropped. All state is guarded by a lock, so threads of user code can read and write freely.
* ``dataset_statement`` carries the SQL text (redacted, at most :data:`MAX_SQL_CHARS` characters) of ``spark.sql(...)`` / DuckDB ``sql`` / ``execute``. The reads and writes
  of a statement are **not** derived here: the control plane parses the statement (sqlglot) and produces verified table and column edges from it.

What is covered, and when it is reported
----------------------------------------
* **PySpark** (patched on the pyspark classes once the session exists, so objects keep their normal classes and behaviour): ``spark.table``, ``spark.read.table / load /
  parquet / csv / json / orc / text`` (``format(..).load(..)`` uses the format set before), ``spark.sql``, ``DataFrameWriter.saveAsTable / insertInto / save / parquet /
  csv / json / orc / text`` and ``DataFrame.writeTo(..).create / createOrReplace / replace / append / overwrite / overwritePartitions``. **Reads are reported when the plan
  is created** (that is when the table or path is resolved), not when rows are materialised: a DataFrame that is built and never used still counts as a read. Writes are
  reported **after the write action succeeded**; every writer method listed above is eager, so nothing is reported for a write that failed.
* **Host Python** (installed lazily when the library is imported by user code, so jobs that never import it pay nothing): ``deltalake.DeltaTable(path)`` (read at
  construction), ``deltalake.write_deltalake`` (written after success), and DuckDB ``duckdb.connect()`` connections plus the module-level ``duckdb.sql / query / execute``
  (statement text). The DuckDB connection is a thin proxy that reports itself as the real connection class.
* Not covered: Spark Connect, streaming, ``DeltaTable.merge / update / delete`` objects, JDBC and other sources without a path or table name, pandas / polars readers.
  Temporary views show up as unresolved table names on the control plane.

Safety
------
Instrumentation errors never reach user code: every hook runs after the wrapped call has returned, inside ``try / except Exception``, and failures are only counted
(:attr:`DatasetProbe.errors`). ``OLCF_DATASET_EVENTS=0`` in the job environment turns the probe off.
"""

from __future__ import annotations

import datetime as _dt
import functools
import hashlib
import importlib.abc
import importlib.util
import os
import re
import sys
import threading
from collections.abc import Callable
from typing import Any

from olcf.core.redaction import redact_text
from olcf.runtime_lib.bootstrap import emit_event

#: Events reported per process (job, kernel); the first one beyond the cap emits ``dataset_events_truncated``.
MAX_EVENTS = 200
#: Datasets per event.
MAX_DATASETS = 20
#: SQL text length in ``dataset_statement``.
MAX_SQL_CHARS = 20_000
_MARK = "__olcf_lineage_probe__"

_LEADING_WORD = re.compile(r"^\s*(?:(?:--[^\n]*(?:\n|$)|/\*.*?\*/)\s*)*\(*\s*([A-Za-z_]+)", re.S)
#: Statements that cannot read or write a dataset (session and catalog housekeeping): not reported, so they do not use the event budget.
_IGNORED_STATEMENTS = frozenset({
    "use", "set", "reset", "show", "describe", "desc", "explain", "refresh", "cache", "uncache", "clear", "analyze", "add", "list", "comment", "msck", "repair",
    "install", "load", "attach", "detach", "pragma", "checkpoint", "call", "begin", "commit", "rollback", "vacuum", "optimize", "export", "import", "summarize",
})
#: A statement with none of these words has no table or file reference (``SELECT 1``, ``SELECT current_date``).
_HAS_REFERENCE = re.compile(r"\b(from|into|join|table|update|merge|view|using|read_\w+|delta_scan)\b", re.I)
_SPLIT_NAME = re.compile(r"`([^`]*)`|\"([^\"]*)\"|([^.`\"]+)")
_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")
_AREA = re.compile(r"^(Files|Tables)(/|$)")
_MODES = {"append": "append", "overwrite": "overwrite", "error": "create", "errorifexists": "create", "default": "create", "ignore": "create", "create": "create",
          "merge": "merge", "update": "update", "delete": "delete"}


def _now() -> str:
    return _dt.datetime.now(_dt.UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _db_name(name: str) -> str:
    """The Spark database name of a lakehouse (the same rule as ``olcf.runtime_lib.spark.db_name_for``)."""
    return re.sub(r"[^0-9A-Za-z_]", "_", name or "lakehouse")


def _canon(uri: str) -> str:
    """A location for prefix comparison: ``file:`` forms reduced to the path, no trailing slash."""
    u = uri.strip().replace("\\", "/")
    if u.startswith("file:"):
        u = re.sub(r"^/{2,}", "/", u[5:])
    return u.rstrip("/")


def _strip_query(uri: str) -> str:
    return re.split(r"[?#]", uri, maxsplit=1)[0]


def _clean(d: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in d.items() if v not in (None, "")}


def leading_keyword(sql: str) -> str | None:
    """The first keyword of a statement (after comments and parentheses), lower-case, or ``None``."""
    m = _LEADING_WORD.match(sql or "")
    return m.group(1).lower() if m else None


def worth_reporting(sql: Any) -> bool:
    """True if ``sql`` is text that can reference a dataset: not session housekeeping (``USE``, ``SET``, ``SHOW`` ...) and mentioning a table-ish keyword."""
    if not isinstance(sql, str) or not sql.strip():
        return False
    word = leading_keyword(sql)
    return word not in _IGNORED_STATEMENTS and bool(_HAS_REFERENCE.search(sql))


class DatasetProbe:
    """Collects, de-duplicates, caps and emits dataset events for one process. Thread-safe; never raises into the caller of :meth:`read` / :meth:`written` / :meth:`statement`.

    ``spec`` is the job spec (the *live* dict: ``%%configure`` may replace its ``default_lakehouse``), used to resolve unqualified table names and paths. ``emit`` is the
    function that writes one event (``emit(event_name, **data)``); tests pass a collector.
    """

    def __init__(self, spec: dict[str, Any] | None = None, emit: Callable[..., None] | None = None, limit: int = MAX_EVENTS) -> None:
        self.spec = spec if spec is not None else {}
        self._emit = emit or emit_event
        self.limit = limit
        self._lock = threading.Lock()
        self._seen: set[str] = set()
        self._count = 0
        self._truncated = False
        self.emitted = 0
        self.duplicates = 0
        self.dropped = 0
        self.errors = 0

    # ------------------------------------------------------------------ resolution
    def lakehouses(self) -> list[dict[str, Any]]:
        """The default lakehouse first, then the other attached lakehouses (descriptors of the job spec)."""
        spec = self.spec or {}
        out: list[dict[str, Any]] = []
        d = spec.get("default_lakehouse")
        if isinstance(d, dict):
            out.append(d)
        out.extend(x for x in (spec.get("lakehouses") or []) if isinstance(x, dict))
        return out

    def default_lakehouse_id(self) -> str | None:
        """Id of the default lakehouse, if the job has one."""
        d = (self.spec or {}).get("default_lakehouse")
        return str(d["id"]) if isinstance(d, dict) and d.get("id") else None

    def table(self, name: Any, fmt: str | None = None) -> dict[str, Any] | None:
        """Descriptor of the table named ``name`` as written in user code (``t``, ``lakehouse.t``, ``lakehouse.schema.t``, ``spark_catalog.lakehouse.t``) or ``None``."""
        if not isinstance(name, str) or not name.strip():
            return None
        parts = [a or b or c for a, b, c in _SPLIT_NAME.findall(name.strip())]
        parts = [p.strip() for p in parts if p.strip()]
        if not parts:
            return None
        if len(parts) > 1 and parts[0].lower() == "spark_catalog":
            parts = parts[1:]
        default, lake = self.default_lakehouse_id(), None
        if len(parts) >= 2:
            lake = next((lh for lh in self.lakehouses() if parts[0].lower() in (_db_name(str(lh.get("name"))).lower(), str(lh.get("name") or "").lower())), None)
        if lake is not None:
            return _clean({"kind": "table", "lakehouse_id": lake.get("id"), "name": ".".join(parts[1:]), "format": fmt})
        return _clean({"kind": "table", "lakehouse_id": default if len(parts) <= 2 else None, "name": ".".join(parts), "format": fmt})

    def path(self, location: Any, fmt: str | None = None) -> dict[str, Any] | None:
        """Descriptor of the dataset at ``location`` (``Files/..``, ``Tables/..``, ``abfss://..``, ``/abs/path``) or ``None`` for something that is not a path."""
        if not isinstance(location, (str, os.PathLike)):
            return None
        raw = str(os.fspath(location)).strip().replace("\\", "/")
        if not raw:
            return None
        raw = _strip_query(raw)
        rel: str | None = None
        lake: dict[str, Any] | None = None
        if _AREA.match(raw):
            lakes = self.lakehouses()
            rel, lake = (raw, lakes[0]) if lakes else (None, None)
        else:
            probe = _canon(raw)
            for lh in self.lakehouses():
                root = _canon(str(lh.get("root_uri") or ""))
                if root and (probe == root or probe.startswith(root + "/")):
                    rel, lake = probe[len(root):].lstrip("/"), lh
                    break
        absolute = _SCHEME.match(raw) is not None or raw.startswith("/")
        uri = raw if absolute else None
        if rel is not None and lake is not None and rel.startswith("Tables/"):
            segs = [s for s in rel.split("/")[1:] if s]
            if segs:
                name = ".".join(segs[:2]) if lake.get("schema_enabled") and len(segs) >= 2 else segs[0]
                return _clean({"kind": "table", "lakehouse_id": lake.get("id"), "name": name, "uri": uri, "format": fmt})
        if rel is not None and lake is not None:
            return _clean({"kind": "file", "lakehouse_id": lake.get("id"), "path": rel, "uri": uri, "format": fmt})
        return _clean({"kind": "file", "path": None if absolute else raw, "uri": uri, "format": fmt})

    # ------------------------------------------------------------------ emission
    def read(self, datasets: list[dict[str, Any] | None]) -> None:
        """Report that ``datasets`` were read (``dataset_read``)."""
        self._dataset_event("dataset_read", datasets, None)

    def written(self, datasets: list[dict[str, Any] | None], mode: str | None = None) -> None:
        """Report that ``datasets`` were written with ``mode`` (``dataset_written``; ``mode`` is a Spark save mode or one of ``create / append / overwrite``)."""
        self._dataset_event("dataset_written", datasets, _MODES.get(str(mode).lower(), "create") if mode is not None else "create")

    def statement(self, sql: Any, dialect: str = "spark") -> None:
        """Report the text of a SQL statement that ran (``dataset_statement``); housekeeping statements and statements without a table reference are skipped."""
        try:
            if not worth_reporting(sql):
                return
            text = redact(str(sql))[:MAX_SQL_CHARS]
            key = "stmt:" + hashlib.sha1(" ".join(text.split()).encode("utf-8", "replace"), usedforsecurity=False).hexdigest()  # noqa: S324 - dedupe key
            self._send(key, "dataset_statement", {"sql": text, "dialect": dialect, "default_lakehouse_id": self.default_lakehouse_id()})
        except Exception:  # noqa: BLE001 - instrumentation must never reach user code
            self.errors += 1

    def _dataset_event(self, event: str, datasets: list[dict[str, Any] | None], mode: str | None) -> None:
        try:
            ds = [d for d in datasets if d][:MAX_DATASETS]
            if not ds:
                return
            key = event + ":" + (mode or "") + ":" + "|".join(sorted(repr(sorted(d.items())) for d in ds))
            self._send(key, event, _clean({"datasets": ds, "mode": mode}))
        except Exception:  # noqa: BLE001
            self.errors += 1

    def _send(self, key: str, event: str, data: dict[str, Any]) -> None:
        action = "event"
        with self._lock:
            if key in self._seen:
                self.duplicates += 1
                return
            if self._count >= self.limit:
                self.dropped += 1
                if self._truncated:
                    return
                self._truncated, action = True, "marker"
            else:
                self._seen.add(key)
                self._count += 1
                self.emitted += 1
        if action == "marker":
            self._emit("dataset_events_truncated", limit=self.limit)
        else:
            self._emit(event, **data, at=_now())

    def stats(self) -> dict[str, int]:
        """Counters (``emitted``, ``duplicates``, ``dropped``, ``errors``) for diagnostics and tests."""
        with self._lock:
            return {"emitted": self.emitted, "duplicates": self.duplicates, "dropped": self.dropped, "errors": self.errors}


def redact(text: str) -> str:
    """Mask secrets in SQL text with the platform redaction rules (``olcf.core.redaction``, stdlib-only)."""
    return redact_text(text)


# --------------------------------------------------------------------------------------------- the process-wide probe
_PROBE: DatasetProbe | None = None
_INSTALL_LOCK = threading.RLock()


def enabled() -> bool:
    """False when the job environment sets ``OLCF_DATASET_EVENTS`` to ``0`` / ``false`` / ``off``."""
    return os.environ.get("OLCF_DATASET_EVENTS", "1").strip().lower() not in ("0", "false", "off", "no")


def current() -> DatasetProbe | None:
    """The installed probe of this process, or ``None``."""
    return _PROBE


def set_probe(probe: DatasetProbe | None) -> DatasetProbe | None:
    """Replace the process-wide probe (tests); returns the previous one."""
    global _PROBE  # noqa: PLW0603
    with _INSTALL_LOCK:
        old, _PROBE = _PROBE, probe
    return old


def install(spec: dict[str, Any] | None = None) -> DatasetProbe | None:
    """Create (once) the process-wide probe for ``spec`` and hook the libraries that are already imported or get imported later. Returns the probe, or ``None`` when disabled.

    Safe to call repeatedly (runner, kernel host and :func:`olcf.runtime_lib.bootstrap.build_namespace` all do); never raises.
    """
    global _PROBE  # noqa: PLW0603
    try:
        if not enabled():
            return None
        with _INSTALL_LOCK:
            if _PROBE is None:
                _PROBE = DatasetProbe(spec)
            elif spec is not None and _PROBE.spec is not spec:
                _PROBE.spec = spec
            probe = _PROBE
        install_hooks()
        return probe
    except Exception:  # noqa: BLE001
        return None


# --------------------------------------------------------------------------------------------- generic wrapping
def _wrap_method(cls: type, name: str, hook: Callable[[DatasetProbe, Any, Any, tuple[Any, ...], dict[str, Any]], None]) -> bool:
    """Replace ``cls.name`` by a wrapper that calls the original, then (after it returned) runs ``hook(probe, self, result, args, kwargs)`` guarded. Idempotent."""
    orig = cls.__dict__.get(name)
    if orig is None or not callable(orig) or getattr(orig, _MARK, False):
        return False

    @functools.wraps(orig)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        result = orig(self, *args, **kwargs)
        probe = _PROBE
        if probe is not None:
            try:
                hook(probe, self, result, args, kwargs)
            except Exception:  # noqa: BLE001 - never reaches user code
                probe.errors += 1
        return result

    setattr(wrapper, _MARK, True)
    setattr(cls, name, wrapper)
    return True


def _wrap_function(owner: Any, name: str, hook: Callable[[DatasetProbe, Any, tuple[Any, ...], dict[str, Any]], None]) -> Any:
    """Same for a module-level function: ``hook(probe, result, args, kwargs)``. Returns the wrapper (or ``None`` when already wrapped / missing)."""
    orig = getattr(owner, name, None)
    if orig is None or not callable(orig) or getattr(orig, _MARK, False):
        return None

    @functools.wraps(orig)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        result = orig(*args, **kwargs)
        probe = _PROBE
        if probe is not None:
            try:
                hook(probe, result, args, kwargs)
            except Exception:  # noqa: BLE001
                probe.errors += 1
        return result

    setattr(wrapper, _MARK, True)
    setattr(owner, name, wrapper)
    return wrapper


def _arg(args: tuple[Any, ...], kwargs: dict[str, Any], index: int, *names: str) -> Any:
    if len(args) > index:
        return args[index]
    for n in names:
        if n in kwargs:
            return kwargs[n]
    return None


def _strings(value: Any) -> list[str]:
    if isinstance(value, (str, os.PathLike)):
        return [str(os.fspath(value))]
    if isinstance(value, (list, tuple)):
        return [str(os.fspath(v)) for v in value if isinstance(v, (str, os.PathLike))]
    return []


# --------------------------------------------------------------------------------------------- PySpark
def _reader_table(p: DatasetProbe, self: Any, result: Any, a: tuple[Any, ...], k: dict[str, Any]) -> None:  # noqa: ARG001
    p.read([p.table(_arg(a, k, 0, "tableName"), getattr(self, "_olcf_fmt", None))])


def _session_table(p: DatasetProbe, self: Any, result: Any, a: tuple[Any, ...], k: dict[str, Any]) -> None:  # noqa: ARG001
    p.read([p.table(_arg(a, k, 0, "tableName"))])


def _session_sql(p: DatasetProbe, self: Any, result: Any, a: tuple[Any, ...], k: dict[str, Any]) -> None:  # noqa: ARG001
    p.statement(_arg(a, k, 0, "sqlQuery", "sql"), "spark")


def _reader_load(p: DatasetProbe, self: Any, result: Any, a: tuple[Any, ...], k: dict[str, Any]) -> None:  # noqa: ARG001
    fmt = _arg(a, k, 1, "format") or getattr(self, "_olcf_fmt", None)
    target = _arg(a, k, 0, "path")
    p.read([p.path(s, fmt) for s in _strings(target)])


def _reader_paths(fmt: str, *, all_positional: bool) -> Callable[..., None]:
    def hook(p: DatasetProbe, self: Any, result: Any, a: tuple[Any, ...], k: dict[str, Any]) -> None:  # noqa: ARG001
        if all_positional:
            targets: list[str] = []
            for v in a:
                targets += _strings(v) if isinstance(v, (str, os.PathLike, list, tuple)) else []
            targets += _strings(k.get("paths") or k.get("path"))
        else:
            targets = _strings(_arg(a, k, 0, "path", "paths"))
        p.read([p.path(s, fmt) for s in targets])

    return hook


def _remember(attr: str) -> Callable[..., None]:
    def hook(p: DatasetProbe, self: Any, result: Any, a: tuple[Any, ...], k: dict[str, Any]) -> None:  # noqa: ARG001
        value = _arg(a, k, 0, "source", "saveMode", "format")
        if isinstance(value, str):
            setattr(self, attr, value)

    return hook


def _writer_save_as_table(p: DatasetProbe, self: Any, result: Any, a: tuple[Any, ...], k: dict[str, Any]) -> None:  # noqa: ARG001
    fmt = _arg(a, k, 1, "format") or getattr(self, "_olcf_fmt", None)
    mode = _arg(a, k, 2, "mode") or getattr(self, "_olcf_mode", None)
    p.written([p.table(_arg(a, k, 0, "name"), fmt)], mode)


def _writer_insert_into(p: DatasetProbe, self: Any, result: Any, a: tuple[Any, ...], k: dict[str, Any]) -> None:  # noqa: ARG001
    overwrite = _arg(a, k, 1, "overwrite")
    recorded = str(getattr(self, "_olcf_mode", "") or "").lower()
    p.written([p.table(_arg(a, k, 0, "tableName"))], "overwrite" if (overwrite or (overwrite is None and recorded == "overwrite")) else "append")


def _writer_save(p: DatasetProbe, self: Any, result: Any, a: tuple[Any, ...], k: dict[str, Any]) -> None:  # noqa: ARG001
    fmt = _arg(a, k, 1, "format") or getattr(self, "_olcf_fmt", None)
    mode = _arg(a, k, 2, "mode") or getattr(self, "_olcf_mode", None)
    p.written([p.path(s, fmt) for s in _strings(_arg(a, k, 0, "path"))], mode)


def _writer_format(fmt: str, *, positional_mode: bool = True) -> Callable[..., None]:
    def hook(p: DatasetProbe, self: Any, result: Any, a: tuple[Any, ...], k: dict[str, Any]) -> None:  # noqa: ARG001
        mode = (_arg(a, k, 1, "mode") if positional_mode else k.get("mode")) or getattr(self, "_olcf_mode", None)
        p.written([p.path(s, fmt) for s in _strings(_arg(a, k, 0, "path"))], mode)

    return hook


def _v2_init(p: DatasetProbe, self: Any, result: Any, a: tuple[Any, ...], k: dict[str, Any]) -> None:  # noqa: ARG001
    name = _arg(a, k, 1, "table")
    if isinstance(name, str):
        self._olcf_table = name


def _v2_write(mode: str) -> Callable[..., None]:
    def hook(p: DatasetProbe, self: Any, result: Any, a: tuple[Any, ...], k: dict[str, Any]) -> None:  # noqa: ARG001
        p.written([p.table(getattr(self, "_olcf_table", None))], mode)

    return hook


def install_spark() -> int:
    """Patch the PySpark classes (``SparkSession``, ``DataFrameReader``, ``DataFrameWriter``, ``DataFrameWriterV2``) once. Returns the number of methods patched (0 when
    PySpark is not imported, already patched or not the expected version). Idempotent and thread-safe; never raises."""
    n = 0
    try:
        session = sys.modules.get("pyspark.sql.session")
        rw = sys.modules.get("pyspark.sql.readwriter")
        with _INSTALL_LOCK:
            if session is not None and hasattr(session, "SparkSession"):
                cls = session.SparkSession
                n += sum([_wrap_method(cls, "table", _session_table), _wrap_method(cls, "sql", _session_sql)])
            if rw is not None:
                if hasattr(rw, "DataFrameReader"):
                    c = rw.DataFrameReader
                    n += sum([_wrap_method(c, "table", _reader_table), _wrap_method(c, "load", _reader_load), _wrap_method(c, "format", _remember("_olcf_fmt")),
                              _wrap_method(c, "parquet", _reader_paths("parquet", all_positional=True)), _wrap_method(c, "orc", _reader_paths("orc", all_positional=True)),
                              _wrap_method(c, "csv", _reader_paths("csv", all_positional=False)), _wrap_method(c, "json", _reader_paths("json", all_positional=False)),
                              _wrap_method(c, "text", _reader_paths("text", all_positional=False))])
                if hasattr(rw, "DataFrameWriter"):
                    c = rw.DataFrameWriter
                    n += sum([_wrap_method(c, "mode", _remember("_olcf_mode")), _wrap_method(c, "format", _remember("_olcf_fmt")),
                              _wrap_method(c, "saveAsTable", _writer_save_as_table), _wrap_method(c, "insertInto", _writer_insert_into), _wrap_method(c, "save", _writer_save),
                              _wrap_method(c, "parquet", _writer_format("parquet")), _wrap_method(c, "csv", _writer_format("csv")),
                              _wrap_method(c, "json", _writer_format("json")), _wrap_method(c, "orc", _writer_format("orc")), _wrap_method(c, "text", _writer_format("text", positional_mode=False))])
                if hasattr(rw, "DataFrameWriterV2"):
                    c = rw.DataFrameWriterV2
                    n += _wrap_method(c, "__init__", _v2_init)
                    for method, mode in (("create", "create"), ("createOrReplace", "overwrite"), ("replace", "overwrite"), ("append", "append"), ("overwrite", "overwrite"),
                                         ("overwritePartitions", "overwrite")):
                        n += _wrap_method(c, method, _v2_write(mode))
    except Exception:  # noqa: BLE001
        if _PROBE is not None:
            _PROBE.errors += 1
    return n


# --------------------------------------------------------------------------------------------- host Python: deltalake, DuckDB
_LOCAL = threading.local()


def _delta_init(p: DatasetProbe, self: Any, result: Any, a: tuple[Any, ...], k: dict[str, Any]) -> None:  # noqa: ARG001
    if getattr(_LOCAL, "writing", 0):
        return          # write_deltalake opens its target table itself (append / overwrite): that is part of the write, not a read by user code
    uri = _arg(a, k, 0, "table_uri")
    p.read([p.path(uri, "delta")])


def _write_deltalake(p: DatasetProbe, result: Any, a: tuple[Any, ...], k: dict[str, Any]) -> None:  # noqa: ARG001
    target = _arg(a, k, 0, "table_or_uri")
    uri = getattr(target, "table_uri", target)
    mode = k.get("mode") or (a[2] if len(a) > 2 and isinstance(a[2], str) else None)
    p.written([p.path(uri, "delta")], mode)


def _wrap_delta_writer(mod: Any) -> Any:
    """Wrap ``write_deltalake`` so that DeltaTable constructions inside the call are not reported as reads, and the write is reported after it succeeded."""
    orig = getattr(mod, "write_deltalake", None)
    if orig is None or not callable(orig) or getattr(orig, _MARK, False):
        return None

    @functools.wraps(orig)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        _LOCAL.writing = getattr(_LOCAL, "writing", 0) + 1
        try:
            result = orig(*args, **kwargs)
        finally:
            _LOCAL.writing -= 1
        probe = _PROBE
        if probe is not None:
            try:
                _write_deltalake(probe, result, args, kwargs)
            except Exception:  # noqa: BLE001 - never reaches user code
                probe.errors += 1
        return result

    setattr(wrapper, _MARK, True)
    mod.write_deltalake = wrapper
    return wrapper


def _patch_deltalake(mod: Any) -> None:
    table_cls = getattr(mod, "DeltaTable", None)
    if table_cls is not None:
        _wrap_method(table_cls, "__init__", _delta_init)
    wrapper = _wrap_delta_writer(mod)
    writer_mod = getattr(mod, "writer", None)
    if wrapper is not None and writer_mod is not None and hasattr(writer_mod, "write_deltalake"):
        try:
            writer_mod.write_deltalake = wrapper
        except (AttributeError, TypeError):
            pass


class _DuckDbConnection:
    """Thin proxy around a DuckDB connection that reports executed statement text; everything else is delegated.

    ``__class__`` is answered by the wrapped connection, so ``isinstance(con, duckdb.DuckDBPyConnection)`` stays true. Methods that return the connection (``execute``) return the
    proxy again so chains such as ``con.execute(..).fetchall()`` and ``with duckdb.connect() as con`` keep working.
    """

    __slots__ = ("_conn",)

    def __init__(self, conn: Any) -> None:
        object.__setattr__(self, "_conn", conn)

    @property  # type: ignore[misc]
    def __class__(self) -> type:  # noqa: PLW0236
        return type(object.__getattribute__(self, "_conn"))

    def __getattr__(self, name: str) -> Any:
        return getattr(object.__getattribute__(self, "_conn"), name)

    def __setattr__(self, name: str, value: Any) -> None:
        setattr(object.__getattribute__(self, "_conn"), name, value)

    def __enter__(self) -> _DuckDbConnection:
        object.__getattribute__(self, "_conn").__enter__()
        return self

    def __exit__(self, *exc: Any) -> Any:
        return object.__getattribute__(self, "_conn").__exit__(*exc)

    def __repr__(self) -> str:
        return repr(object.__getattribute__(self, "_conn"))

    def _report(self, query: Any) -> None:
        probe = _PROBE
        if probe is not None:
            probe.statement(query, "duckdb")

    def sql(self, query: Any, *args: Any, **kwargs: Any) -> Any:
        """``connection.sql(query, ...)``; reports the statement after it ran."""
        result = object.__getattribute__(self, "_conn").sql(query, *args, **kwargs)
        self._report(query)
        return result

    def query(self, query: Any, *args: Any, **kwargs: Any) -> Any:
        """``connection.query(query, ...)``; reports the statement after it ran."""
        result = object.__getattribute__(self, "_conn").query(query, *args, **kwargs)
        self._report(query)
        return result

    def execute(self, query: Any, *args: Any, **kwargs: Any) -> Any:
        """``connection.execute(query, ...)``; reports the statement after it ran and returns this proxy (the real method returns the connection)."""
        conn = object.__getattribute__(self, "_conn")
        result = conn.execute(query, *args, **kwargs)
        self._report(query)
        return self if result is conn else result

    def cursor(self) -> Any:
        """A new cursor, proxied as well."""
        return _DuckDbConnection(object.__getattribute__(self, "_conn").cursor())

    def duplicate(self) -> Any:
        """A duplicate connection, proxied as well."""
        return _DuckDbConnection(object.__getattribute__(self, "_conn").duplicate())


def _patch_duckdb(mod: Any) -> None:
    orig_connect = getattr(mod, "connect", None)
    if callable(orig_connect) and not getattr(orig_connect, _MARK, False):
        @functools.wraps(orig_connect)
        def connect(*args: Any, **kwargs: Any) -> Any:
            conn = orig_connect(*args, **kwargs)
            return _DuckDbConnection(conn) if _PROBE is not None else conn

        setattr(connect, _MARK, True)
        mod.connect = connect
    for name in ("sql", "query", "execute"):
        _wrap_function(mod, name, lambda p, result, a, k: p.statement(_arg(a, k, 0, "query"), "duckdb"))


# --------------------------------------------------------------------------------------------- post-import hooks
class _HookLoader(importlib.abc.Loader):
    """Wraps a module loader: after the module body ran, calls ``callback(module)`` (guarded) and restores the original loader on the module."""

    def __init__(self, inner: Any, callback: Callable[[Any], None]) -> None:
        self._inner, self._callback = inner, callback

    def create_module(self, spec: Any) -> Any:
        """Delegate module creation to the wrapped loader."""
        create = getattr(self._inner, "create_module", None)
        return create(spec) if create is not None else None

    def exec_module(self, module: Any) -> None:
        """Run the wrapped loader, then the callback."""
        self._inner.exec_module(module)
        spec = getattr(module, "__spec__", None)
        if spec is not None:
            spec.loader = self._inner
        try:
            module.__loader__ = self._inner
        except (AttributeError, TypeError):
            pass
        try:
            self._callback(module)
        except Exception:  # noqa: BLE001 - never fails the user's import
            if _PROBE is not None:
                _PROBE.errors += 1

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class _PostImportFinder(importlib.abc.MetaPathFinder):
    """Meta-path finder that lets the normal machinery find the module and only wraps its loader (so the module itself is loaded exactly as before)."""

    def __init__(self) -> None:
        self.hooks: dict[str, Callable[[Any], None]] = {}
        self._busy = threading.local()

    def find_spec(self, fullname: str, path: Any = None, target: Any = None) -> Any:
        """The module's spec with a wrapped loader when ``fullname`` has a registered hook, else ``None`` (not ours)."""
        cb = self.hooks.get(fullname)
        if cb is None or getattr(self._busy, "on", False):
            return None
        self._busy.on = True
        try:
            for finder in sys.meta_path:
                if finder is self or not hasattr(finder, "find_spec"):
                    continue
                spec = finder.find_spec(fullname, path, target)
                if spec is not None:
                    if spec.loader is not None and hasattr(spec.loader, "exec_module"):
                        spec.loader = _HookLoader(spec.loader, cb)
                    return spec
        finally:
            self._busy.on = False
        return None


_FINDER: _PostImportFinder | None = None


def _on_pyspark(module: Any) -> None:  # noqa: ARG001
    install_spark()


_HOOKS: dict[str, Callable[[Any], None]] = {"deltalake": _patch_deltalake, "duckdb": _patch_duckdb, "pyspark.sql": _on_pyspark}


def install_hooks() -> None:
    """Patch ``deltalake``, ``duckdb`` and ``pyspark.sql`` now if they are already imported, and when user code imports them later. Idempotent; never raises."""
    global _FINDER  # noqa: PLW0603
    try:
        with _INSTALL_LOCK:
            if _FINDER is None:
                _FINDER = _PostImportFinder()
                sys.meta_path.insert(0, _FINDER)
            finder = _FINDER
            todo = [(name, cb) for name, cb in _HOOKS.items() if name not in finder.hooks]
            for name, cb in todo:
                finder.hooks[name] = cb
        for name, cb in todo:
            module = sys.modules.get(name)
            if module is not None:
                cb(module)
    except Exception:  # noqa: BLE001
        if _PROBE is not None:
            _PROBE.errors += 1
