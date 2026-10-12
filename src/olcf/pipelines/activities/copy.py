# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``copy`` activity: move data between lakehouse locations (files <-> files, files <-> Delta table), optionally converting csv / jsonl / parquet.

Properties::

    source: {lakehouse, path | table, pattern="*", recursive=true, format=binary|csv|jsonl|parquet|delta, options:{delimiter, header, encoding}}
    sink:   {lakehouse, path | table, format=<source format>, mode=overwrite|skip_existing|fail_if_exists (files) | append|overwrite|error (table),
             flatten=false, options:{header}}
    options:{parallelism=4, verify=size|checksum|none, maxFiles=100000, maxBytes=<50 GiB>, failOnEmpty=false}

Guarantees
----------
* **Authorisation** - the run's principal needs ``lakehouse.read`` on the source and ``lakehouse.write`` on the sink (checked in the lakehouse's own workspace).
* **Resumable & idempotent** - every file is written to a temporary part below ``<sink>/.olcf-tmp/``, verified and only then moved into place; each finished file is recorded
  in the ``copy_manifest`` table keyed by ``(run group, activity, resolved settings)``. A retry, a re-run from failure or a re-execution after a control-plane crash skips the
  files whose manifest row matches the unchanged source (same size / modified time) and whose sink still has the recorded size. Delta appends carry a Delta *application
  transaction id* so a repeated commit is detected and skipped.
* **Verification** - byte copies compare sizes (``verify=size``) or SHA-256 of source and re-read sink (``checksum``); conversions compare the number of rows read and written
  (parquet sinks are re-opened and their footer row count is checked).
* **Limits** - file count, total bytes and in-memory spool size are capped; nothing is read fully into memory unless the source format has no streaming reader (jsonl, parquet),
  in which case a size-bounded spooled temp file is used.
* **Lineage** - on success the catalog service receives ``source -> sink`` edges (``via.kind = pipeline``); failures to record lineage never fail the copy.
"""

from __future__ import annotations

import fnmatch
import hashlib
import io
import json
import logging
import re
import tempfile
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select

from olcf.core import dataset_events as dsev
from olcf.core.errors import Conflict, NotFound, OlcfError
from olcf.core.timeutil import now_iso
from olcf.db.util import fetch_all
from olcf.pipelines.activities.base import (
    Activity,
    ActivityCancelled,
    ActivityContext,
    ActivityError,
    ValidationIssue,
    obj,
    s_bool,
    s_int,
    s_obj,
    s_str,
)
from olcf.pipelines.schema import copy_manifest

log = logging.getLogger("olcf.pipelines.copy")

FORMATS = ("binary", "csv", "jsonl", "parquet", "delta")
FILE_FORMATS = ("binary", "csv", "jsonl", "parquet")
EXT = {"csv": ".csv", "jsonl": ".jsonl", "parquet": ".parquet"}
CHUNK = 1024 * 1024
DEFAULT_MAX_BYTES = 50 * 1024**3
SPOOL_LIMIT = 1024**3
TMP_DIR = ".olcf-tmp"
TABLE_NAME = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$")


@dataclass
class _Plan:
    """The resolved copy: lakehouses, files to copy and where each goes."""

    src_lh: Any
    dst_lh: Any
    src_fmt: str
    dst_fmt: str
    src_table: str | None
    dst_table: str | None
    mode: str
    verify: str
    parallelism: int
    items: list[tuple[Any, str]]      # (FileInfo, destination root-relative path)
    src_opts: dict[str, Any]
    dst_opts: dict[str, Any]
    key: str
    total_bytes: int


def _import_pa() -> Any:
    try:
        import pyarrow as pa
        import pyarrow.csv as pacsv
        import pyarrow.json as pajson
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - pyarrow is a hard dependency
        raise ActivityError(f"pyarrow is required for format conversion: {exc}", category="unsupported", retryable=False) from None
    return pa, pacsv, pajson, pq


class _FileAdapter(io.RawIOBase):
    """File-like wrapper around a storage ``BinaryWriter`` (pyarrow writers want a Python file object)."""

    def __init__(self, writer: Any) -> None:
        super().__init__()
        self._w = writer
        self.bytes = 0

    def writable(self) -> bool:
        return True

    def write(self, b: Any) -> int:  # type: ignore[override]
        data = bytes(b)
        self._w.write(data)
        self.bytes += len(data)
        return len(data)

    def flush(self) -> None:
        return None


class CopyActivity(Activity):
    """See the module docstring."""

    type = "copy"
    title = "Copy data"
    description = "Copy files or a Delta table between lakehouse locations with optional csv/jsonl/parquet conversion; resumable, verified, lineage-recording."
    mode = "blocking"
    resumable = True
    default_timeout_seconds = 6 * 3600
    properties_schema = obj({
        "source": obj({"lakehouse": s_str(minLength=1), "path": s_str(), "table": s_str(), "pattern": s_str(), "recursive": s_bool(), "format": {"enum": list(FORMATS)}, "options": s_obj()},
                      ["lakehouse"]),
        "sink": obj({"lakehouse": s_str(minLength=1), "path": s_str(), "table": s_str(), "format": {"enum": list(FORMATS)}, "mode": {"enum": ["overwrite", "skip_existing", "fail_if_exists", "append", "error"]},
                     "flatten": s_bool(), "options": s_obj()}, ["lakehouse"]),
        "options": obj({"parallelism": s_int(1, 16), "verify": {"enum": ["none", "size", "checksum"]}, "maxFiles": s_int(1, 1_000_000), "maxBytes": s_int(1), "failOnEmpty": s_bool()}),
    }, ["source", "sink"])

    def validate(self, props: dict[str, Any], path: str) -> list[ValidationIssue]:
        out: list[ValidationIssue] = []
        src, snk = props.get("source"), props.get("sink")
        if isinstance(src, dict):
            if not src.get("path") and not src.get("table"):
                out.append(ValidationIssue(f"{path}.source", "source needs 'path' (file or folder) or 'table'", "required"))
            if src.get("path") and src.get("table"):
                out.append(ValidationIssue(f"{path}.source", "use either 'path' or 'table' in source, not both", "conflict"))
        if isinstance(snk, dict):
            if not snk.get("path") and not snk.get("table"):
                out.append(ValidationIssue(f"{path}.sink", "sink needs 'path' (folder or file) or 'table'", "required"))
            if snk.get("path") and snk.get("table"):
                out.append(ValidationIssue(f"{path}.sink", "use either 'path' or 'table' in sink, not both", "conflict"))
            fmt, mode = snk.get("format"), snk.get("mode")
            if snk.get("table") and fmt not in (None, "delta") and not str(fmt).startswith("@"):
                out.append(ValidationIssue(f"{path}.sink.format", "a table sink is always Delta; remove sink.format or use 'delta'", "incompatible"))
            if snk.get("table") and mode in ("skip_existing", "fail_if_exists"):
                out.append(ValidationIssue(f"{path}.sink.mode", f"mode '{mode}' applies to file sinks; table sinks use append, overwrite or error", "incompatible"))
            if snk.get("path") and mode in ("append", "error"):
                out.append(ValidationIssue(f"{path}.sink.mode", f"mode '{mode}' applies to table sinks; file sinks use overwrite, skip_existing or fail_if_exists", "incompatible"))
        if isinstance(src, dict) and isinstance(snk, dict) and src.get("format") == "delta" and not src.get("table") and src.get("path"):
            out.append(ValidationIssue(f"{path}.source.format", "reading a Delta table needs source.table (a table name below Tables/)", "incompatible"))
        return out

    # ------------------------------------------------------------------------------------------------------------------------------ planning
    def _lakehouse(self, actx: ActivityContext, ref: Any, perm: str, what: str) -> Any:
        st = actx.service("storage")
        try:
            lh = st.get_lakehouse(str(ref), workspace_id=actx.workspace_id)
            actx.principal.require(perm, workspace_id=lh.workspace_id, item_id=lh.id)
        except OlcfError as exc:
            raise ActivityError(f"copy {what}: {exc.message}", category=exc.code, retryable=False, details=exc.details) from None
        return lh

    @staticmethod
    def _table_rel(lh: Any, name: str) -> str:
        if not isinstance(name, str) or not TABLE_NAME.match(name) or ".." in name:
            raise ActivityError(f"invalid table name '{name}'", category="configuration", retryable=False)
        if "." in name and getattr(lh, "schema_enabled", False):
            schema, tbl = name.split(".", 1)
            return f"Tables/{schema}/{tbl}"
        return f"Tables/{name}"

    def plan(self, actx: ActivityContext) -> _Plan:
        p = actx.props
        src, snk, opts = dict(p.get("source") or {}), dict(p.get("sink") or {}), dict(p.get("options") or {})
        st = actx.service("storage")
        src_lh = self._lakehouse(actx, src.get("lakehouse"), "lakehouse.read", "source")
        dst_lh = self._lakehouse(actx, snk.get("lakehouse"), "lakehouse.write", "sink")
        src_table, dst_table = src.get("table"), snk.get("table")
        src_fmt = "delta" if src_table else str(src.get("format") or "binary")
        if src_fmt == "delta" and not src_table:
            raise ActivityError("source.format 'delta' needs source.table", category="configuration", retryable=False)
        dst_fmt = "delta" if dst_table else str(snk.get("format") or ("binary" if src_fmt in ("binary",) else src_fmt))
        if dst_fmt == "delta" and not dst_table:
            raise ActivityError("sink.format 'delta' needs sink.table", category="configuration", retryable=False)
        if src_fmt == "binary" and dst_fmt not in ("binary",) and not dst_table:
            raise ActivityError(f"cannot convert binary files to {dst_fmt}; set source.format to csv, jsonl or parquet", category="configuration", retryable=False)
        if dst_fmt == "binary" and src_fmt != "binary":
            raise ActivityError(f"cannot write {src_fmt} data as binary; set sink.format to csv, jsonl or parquet", category="configuration", retryable=False)
        if dst_table and src_fmt == "binary":
            raise ActivityError("a table sink needs a structured source format (csv, jsonl, parquet or a Delta table)", category="configuration", retryable=False)
        if src_table and not dst_table:
            raise ActivityError("copying a Delta table to files is not supported; use a notebook or a sql activity", category="unsupported", retryable=False)
        mode = str(snk.get("mode") or ("append" if dst_table else "overwrite"))
        if dst_table and mode not in ("append", "overwrite", "error"):
            raise ActivityError(f"invalid table sink mode '{mode}'", category="configuration", retryable=False)
        if not dst_table and mode not in ("overwrite", "skip_existing", "fail_if_exists"):
            raise ActivityError(f"invalid file sink mode '{mode}'", category="configuration", retryable=False)
        max_files, max_bytes = int(opts.get("maxFiles") or 100_000), int(opts.get("maxBytes") or DEFAULT_MAX_BYTES)
        items: list[tuple[Any, str]] = []
        total = 0
        if src_table:
            self._table_rel(src_lh, src_table)
        else:
            base = str(src.get("path") or "").strip("/")
            pattern = str(src.get("pattern") or "*")
            recursive = bool(src.get("recursive", True))
            try:
                info = st.stat(src_lh, base) if base else None
            except NotFound:
                raise ActivityError(f"copy source '{base}' does not exist in lakehouse '{src_lh.name}'", category="not_found", retryable=False) from None
            except OlcfError as exc:
                raise ActivityError(f"copy source: {exc.message}", category=exc.code, retryable=False) from None
            if info is not None and not info.is_dir:
                files, root = [info], base.rsplit("/", 1)[0] if "/" in base else ""
                single = True
            else:
                single = False
                root = base
                files = [f for f in st.list_files(src_lh, base, recursive) if not f.is_dir]
            prefix = (root + "/") if root else ""
            for f in sorted(files, key=lambda x: x.path):
                if f"/{TMP_DIR}/" in f"/{f.path}":
                    continue
                rel = f.path[len(prefix):] if f.path.startswith(prefix) else f.path
                if not single and not fnmatch.fnmatchcase(rel, pattern) and not fnmatch.fnmatchcase(rel.rsplit("/", 1)[-1], pattern):
                    continue
                items.append((f, rel))
            if len(items) > max_files:
                raise ActivityError(f"the source matches {len(items)} files; maxFiles is {max_files}", category="limit", retryable=False)
            total = sum(int(f.size or 0) for f, _ in items)
            if total > max_bytes:
                raise ActivityError(f"the source is {total} bytes; maxBytes is {max_bytes}", category="limit", retryable=False)
            if not items and opts.get("failOnEmpty"):
                raise ActivityError(f"no file below '{base}' matches '{pattern}'", category="not_found", retryable=False)
        out_items: list[tuple[Any, str]] = []
        if not dst_table:
            sink_base = str(snk.get("path") or "").strip("/")
            as_file = (not src_table) and len(items) == 1 and single and not str(snk.get("path") or "").endswith("/") and "." in sink_base.rsplit("/", 1)[-1]
            for f, rel in items:
                name = rel.rsplit("/", 1)[-1] if snk.get("flatten") else rel
                if dst_fmt != "binary" and dst_fmt != src_fmt:
                    stem = name.rsplit(".", 1)[0] if "." in name.rsplit("/", 1)[-1] else name
                    name = stem + EXT[dst_fmt]
                dest = sink_base if as_file else (f"{sink_base}/{name}" if sink_base else name)
                out_items.append((f, dest))
            dests = [d for _, d in out_items]
            if len(set(dests)) != len(dests):
                raise ActivityError("two source files map to the same sink path (use flatten=false, or distinct names)", category="configuration", retryable=False)
        key_doc = {"group": actx.run.get("group_id") or actx.run_id, "activity": actx.name, "iteration": actx.row.get("iteration"),
                   "src": [src_lh.id, src.get("path"), src_table, src.get("pattern"), src_fmt], "dst": [dst_lh.id, snk.get("path"), dst_table, dst_fmt, mode]}
        key = hashlib.sha256(json.dumps(key_doc, sort_keys=True, default=str).encode()).hexdigest()[:40]
        return _Plan(src_lh, dst_lh, src_fmt, dst_fmt, src_table, dst_table, mode, str(opts.get("verify") or "size"), int(opts.get("parallelism") or 4),
                     out_items if not dst_table else items, dict(src.get("options") or {}), dict(snk.get("options") or {}), key, total)

    # ------------------------------------------------------------------------------------------------------------------------------ manifest
    def _manifest(self, actx: ActivityContext, key: str) -> dict[str, dict[str, Any]]:
        with actx.ctx.db.connect() as conn:
            rows = fetch_all(conn, select(copy_manifest).where(copy_manifest.c.manifest_key == key))
        return {r["rel_path"]: r for r in rows}

    def _mark(self, actx: ActivityContext, key: str, rel: str, state: str, **vals: Any) -> None:
        t = copy_manifest
        now = now_iso()
        with actx.ctx.db.begin() as conn:
            n = conn.execute(t.update().where(t.c.manifest_key == key, t.c.rel_path == rel).values(state=state, updated_at=now, attempts=t.c.attempts + 1, **vals)).rowcount
            if not n:
                conn.execute(t.insert().values(manifest_key=key, rel_path=rel[:1024], state=state, attempts=1, updated_at=now, **vals))

    # ------------------------------------------------------------------------------------------------------------------------------ execution
    def execute(self, actx: ActivityContext) -> dict[str, Any]:
        plan = self.plan(actx)
        actx.save_state(manifest_key=plan.key)
        if plan.dst_table:
            return self._to_table(actx, plan)
        return self._to_files(actx, plan)

    def _to_files(self, actx: ActivityContext, plan: _Plan) -> dict[str, Any]:
        st = actx.service("storage")
        manifest = self._manifest(actx, plan.key)
        lock = threading.Lock()
        stats = {"filesCopied": 0, "filesSkipped": 0, "filesResumed": 0, "bytesRead": 0, "bytesWritten": 0, "rowsCopied": 0}
        files: list[dict[str, Any]] = []

        def one(item: tuple[Any, str]) -> None:
            actx.check_cancelled()
            info, dest = item
            src_etag = f"{info.size}:{info.modified or ''}:{info.etag or ''}"
            prev = manifest.get(info.path)
            if prev and prev["state"] == "done" and prev.get("source_etag") == src_etag:
                try:
                    if st.stat(plan.dst_lh, dest).size == (prev.get("size") or -1) or plan.dst_fmt != "binary":
                        with lock:
                            stats["filesResumed"] += 1
                            files.append({"source": info.path, "sink": dest, "bytes": prev.get("size"), "rows": prev.get("rows"), "status": "resumed"})
                        return
                except NotFound:
                    pass
            try:
                exists = st.exists(plan.dst_lh, dest)
            except OlcfError:
                exists = False
            if exists and plan.mode == "skip_existing":
                with lock:
                    stats["filesSkipped"] += 1
                    files.append({"source": info.path, "sink": dest, "status": "skipped_existing"})
                return
            if exists and plan.mode == "fail_if_exists":
                raise Conflict(f"sink file '{dest}' already exists (mode fail_if_exists)")
            self._mark(actx, plan.key, info.path, "pending", source_etag=src_etag, error=None)
            tmp = f"{dest.rsplit('/', 1)[0] + '/' if '/' in dest else ''}{TMP_DIR}/{plan.key[:12]}-{hashlib.sha1(dest.encode()).hexdigest()[:10]}.part"  # noqa: S324 - not security relevant
            try:
                if plan.dst_fmt == "binary":
                    nbytes, sha, rows = self._copy_bytes(actx, plan, info, tmp)
                    written = nbytes
                else:
                    rows, written = self._convert(actx, plan, info, tmp)
                    sha = None
                    nbytes = int(info.size or 0)
                self._verify(actx, plan, info, tmp, nbytes, sha, rows, written)
                st.move(plan.dst_lh, tmp, dest, overwrite=True)
            except BaseException as exc:
                try:
                    st.delete(plan.dst_lh, tmp)
                except Exception:  # noqa: BLE001
                    pass
                self._mark(actx, plan.key, info.path, "failed", error=str(exc)[:900])
                raise
            self._mark(actx, plan.key, info.path, "done", size=written, sha256=sha, rows=rows, source_etag=src_etag, error=None)
            with lock:
                stats["filesCopied"] += 1
                stats["bytesRead"] += nbytes
                stats["bytesWritten"] += written
                stats["rowsCopied"] += rows or 0
                if len(files) < 200:
                    files.append({"source": info.path, "sink": dest, "bytes": written, "rows": rows, "sha256": sha, "status": "copied"})

        try:
            if plan.parallelism <= 1 or len(plan.items) <= 1:
                for it in plan.items:
                    one(it)
            else:
                abort = threading.Event()

                def guarded(it: tuple[Any, str]) -> None:
                    if abort.is_set():
                        return
                    try:
                        one(it)
                    except BaseException:
                        abort.set()  # stop the siblings promptly (they check the event between files)
                        raise

                with ThreadPoolExecutor(max_workers=plan.parallelism, thread_name_prefix="olcf-copy") as pool:
                    futs = [pool.submit(guarded, it) for it in plan.items]
                    excs: list[BaseException] = []
                    for f in futs:
                        try:
                            f.result()
                        except BaseException as exc:  # noqa: BLE001
                            excs.append(exc)
                    if excs:
                        real = [e for e in excs if not isinstance(e, ActivityCancelled)]
                        raise (real or excs)[0]
        except OlcfError as exc:
            raise ActivityError(f"copy failed: {exc.message}", category=exc.code, retryable=exc.status >= 500, details=exc.details) from None
        self._cleanup_tmp(actx, plan)
        self._lineage(actx, plan)
        return {**stats, "files": files[:200], "manifestKey": plan.key, "sink": plan.dst_lh.name}

    def _cleanup_tmp(self, actx: ActivityContext, plan: _Plan) -> None:
        # best effort: remove temp parts of this copy that a crash may have left behind
        st = actx.service("storage")
        seen: set[str] = set()
        for _, dest in plan.items:
            d = (dest.rsplit("/", 1)[0] + "/" if "/" in dest else "") + TMP_DIR
            if d in seen:
                continue
            seen.add(d)
            try:
                for f in st.list_files(plan.dst_lh, d, False):
                    if f.path.rsplit("/", 1)[-1].startswith(plan.key[:12]):
                        st.delete(plan.dst_lh, f.path)
            except Exception:  # noqa: BLE001
                pass

    # -- byte copy
    def _copy_bytes(self, actx: ActivityContext, plan: _Plan, info: Any, tmp: str) -> tuple[int, str, int | None]:
        st = actx.service("storage")
        h = hashlib.sha256()
        n = 0
        reader = st.open_read(plan.src_lh, info.path)
        try:
            with st.open_write(plan.dst_lh, tmp, overwrite=True) as w:
                while True:
                    actx.check_cancelled()
                    chunk = reader.read(CHUNK)
                    if not chunk:
                        break
                    w.write(chunk)
                    h.update(chunk)
                    n += len(chunk)
        finally:
            reader.close()
        return n, h.hexdigest(), None

    # -- conversion
    def _spool(self, actx: ActivityContext, plan: _Plan, info: Any) -> Any:
        if int(info.size or 0) > SPOOL_LIMIT:
            raise ActivityError(f"'{info.path}' is {info.size} bytes; {plan.src_fmt} files above {SPOOL_LIMIT // 1024**2} MiB cannot be converted (no streaming reader)", category="limit", retryable=False)
        st = actx.service("storage")
        sp = tempfile.SpooledTemporaryFile(max_size=64 * 1024 * 1024)  # noqa: SIM115 - closed by the caller
        reader = st.open_read(plan.src_lh, info.path)
        try:
            while True:
                actx.check_cancelled()
                chunk = reader.read(CHUNK)
                if not chunk:
                    break
                sp.write(chunk)
        finally:
            reader.close()
        sp.seek(0)
        return sp

    def _batches(self, actx: ActivityContext, plan: _Plan, info: Any) -> tuple[Any, Iterator[Any], Any]:
        """Return ``(schema, batch iterator, closer)`` for one source file."""
        pa, pacsv, pajson, pq = _import_pa()
        o = plan.src_opts
        try:
            if plan.src_fmt == "csv":
                reader_obj = actx.service("storage").open_read(plan.src_lh, info.path)
                ro = pacsv.ReadOptions(encoding=str(o.get("encoding") or "utf8"), autogenerate_column_names=(o.get("header") is False))
                po = pacsv.ParseOptions(delimiter=str(o.get("delimiter") or ","))
                rdr = pacsv.open_csv(reader_obj, read_options=ro, parse_options=po)
                return rdr.schema, iter(rdr), reader_obj
            sp = self._spool(actx, plan, info)
            if plan.src_fmt == "parquet":
                pf = pq.ParquetFile(sp)
                return pf.schema_arrow, pf.iter_batches(batch_size=65536), sp
            if plan.src_fmt == "jsonl":
                tbl = pajson.read_json(sp)
                return tbl.schema, iter(tbl.to_batches(max_chunksize=65536)), sp
        except OlcfError:
            raise
        except ActivityError:
            raise
        except Exception as exc:  # noqa: BLE001 - ArrowInvalid & friends: the data is not what the settings say
            raise ActivityError(f"cannot read '{info.path}' as {plan.src_fmt}: {exc}", category="data", retryable=False) from None
        raise ActivityError(f"unsupported source format {plan.src_fmt}", category="configuration", retryable=False)

    def _convert(self, actx: ActivityContext, plan: _Plan, info: Any, tmp: str) -> tuple[int, int]:
        pa, pacsv, _pajson, pq = _import_pa()
        st = actx.service("storage")
        schema, batches, closer = self._batches(actx, plan, info)
        rows = 0
        try:
            with st.open_write(plan.dst_lh, tmp, overwrite=True) as w:
                adapter = _FileAdapter(w)
                writer: Any
                if plan.dst_fmt == "parquet":
                    writer = pq.ParquetWriter(adapter, schema, compression="zstd")
                elif plan.dst_fmt == "csv":
                    writer = pacsv.CSVWriter(adapter, schema, write_options=pacsv.WriteOptions(include_header=bool(plan.dst_opts.get("header", True))))
                else:
                    writer = None
                try:
                    for b in batches:
                        actx.check_cancelled()
                        rows += b.num_rows
                        if writer is None:
                            lines = "".join(json.dumps(r, default=str, separators=(",", ":")) + "\n" for r in b.to_pylist())
                            adapter.write(lines.encode())
                        else:
                            writer.write_batch(b)
                finally:
                    if writer is not None:
                        writer.close()
                written = adapter.bytes
        except ActivityError:
            raise
        except OlcfError:
            raise
        except Exception as exc:  # noqa: BLE001
            if actx.is_cancelled():
                raise
            raise ActivityError(f"cannot convert '{info.path}' to {plan.dst_fmt}: {exc}", category="data", retryable=False) from None
        finally:
            try:
                closer.close()
            except Exception:  # noqa: BLE001
                pass
        return rows, written

    # -- verification
    def _verify(self, actx: ActivityContext, plan: _Plan, info: Any, tmp: str, nbytes: int, sha: str | None, rows: int | None, written: int) -> None:
        if plan.verify == "none":
            return
        st = actx.service("storage")
        got = st.stat(plan.dst_lh, tmp).size
        if got != written:
            raise ActivityError(f"verification failed for '{info.path}': sink has {got} bytes, expected {written}", category="verification", retryable=True)
        if plan.dst_fmt == "binary":
            if int(info.size or 0) and int(info.size) != nbytes:
                raise ActivityError(f"'{info.path}' changed while it was copied ({info.size} bytes listed, {nbytes} read)", category="verification", retryable=True)
            if plan.verify == "checksum":
                h = hashlib.sha256()
                r = st.open_read(plan.dst_lh, tmp)
                try:
                    for chunk in iter(lambda: r.read(CHUNK), b""):
                        h.update(chunk)
                finally:
                    r.close()
                if h.hexdigest() != sha:
                    raise ActivityError(f"verification failed for '{info.path}': sink checksum differs from the source", category="verification", retryable=True)
        elif plan.dst_fmt == "parquet" and plan.verify in ("size", "checksum"):
            _pa, _c, _j, pq = _import_pa()
            sp = tempfile.SpooledTemporaryFile(max_size=64 * 1024 * 1024)  # noqa: SIM115
            r = st.open_read(plan.dst_lh, tmp)
            try:
                for chunk in iter(lambda: r.read(CHUNK), b""):
                    sp.write(chunk)
            finally:
                r.close()
            sp.seek(0)
            try:
                n = pq.ParquetFile(sp).metadata.num_rows
            finally:
                sp.close()
            if n != rows:
                raise ActivityError(f"verification failed for '{info.path}': wrote {rows} rows but the parquet footer says {n}", category="verification", retryable=True)

    # -- Delta table sink
    def _to_table(self, actx: ActivityContext, plan: _Plan) -> dict[str, Any]:
        pa, *_ = _import_pa()
        from olcf.storage import delta_policy as policy

        try:
            import deltalake as dl
        except ImportError as exc:  # pragma: no cover
            raise ActivityError(f"deltalake is required for table sinks: {exc}", category="unsupported", retryable=False) from None
        st = actx.service("storage")
        rel = self._table_rel(plan.dst_lh, str(plan.dst_table))
        uri = st.physical_uri(plan.dst_lh, rel)
        opts = st.delta_storage_options(plan.dst_lh)
        app_id = f"olcf-copy-{plan.key}"
        exists = False
        try:
            dt = dl.DeltaTable(uri, storage_options=opts)
            exists = True
        except Exception:  # noqa: BLE001 - table does not exist yet
            dt = None
        if exists and dt is not None:
            if dt.transaction_version(app_id) is not None:
                return {"filesCopied": 0, "filesSkipped": 0, "filesResumed": len(plan.items), "rowsCopied": 0, "table": plan.dst_table, "deltaVersion": dt.version(),
                        "resumed": True, "note": "this copy was already committed to the table (application transaction found)"}
            dec = policy.evaluate(dt.protocol(), "write", engine="delta-rs", profile=actx.ctx.service("settings").get("storage.delta_interop_profile"))
            if not dec.allowed:
                raise ActivityError("the Delta interop policy blocks writing this table with the embedded engine: " + "; ".join(dec.reasons()) + " (run the copy with Spark instead)",
                                    category="compatibility", retryable=False)
            if plan.mode == "error":
                raise ActivityError(f"table '{plan.dst_table}' already exists (mode error)", category="conflict", retryable=False)
        counted = {"rows": 0, "files": 0}
        first_schema: list[Any] = []

        def gen() -> Iterator[Any]:
            if plan.src_table:
                srcdt = actx.service("storage").tables.open_delta(plan.src_lh, plan.src_table)
                dec2 = policy.evaluate(srcdt.protocol(), "read", engine="delta-rs")
                if not dec2.allowed:
                    raise ActivityError("the source table uses Delta features the embedded engine cannot read: " + "; ".join(dec2.reasons()), category="compatibility", retryable=False)
                for b in srcdt.to_pyarrow_dataset().scanner(batch_size=65536).to_batches():
                    actx.check_cancelled()
                    counted["rows"] += b.num_rows
                    yield b
                return
            for info, _ in plan.items:
                _schema, batches, closer = self._batches(actx, plan, info)
                try:
                    for b in batches:
                        actx.check_cancelled()
                        if first_schema and b.schema != first_schema[0]:
                            try:
                                b = pa.Table.from_batches([b]).cast(first_schema[0]).to_batches()[0]  # noqa: PLW2901
                            except Exception as exc:  # noqa: BLE001
                                raise ActivityError(f"'{info.path}' has a schema that differs from the first file and cannot be cast: {exc}", category="data", retryable=False) from None
                        counted["rows"] += b.num_rows
                        yield b
                finally:
                    try:
                        closer.close()
                    except Exception:  # noqa: BLE001
                        pass
                counted["files"] += 1

        # peek the schema from the first batch so the RecordBatchReader can be built
        it = gen()
        try:
            first = next(it)
        except StopIteration:
            return {"filesCopied": 0, "rowsCopied": 0, "table": plan.dst_table, "note": "the source produced no rows; nothing was written"}
        except ActivityError:
            raise
        except OlcfError as exc:
            raise ActivityError(exc.message, category=exc.code, retryable=False) from None
        first_schema.append(first.schema)
        if not exists:
            dec = policy.evaluate(policy.protocol_for_schema(first.schema, None), "write", engine="delta-rs", profile=actx.ctx.service("settings").get("storage.delta_interop_profile"))
            if not dec.allowed:
                raise ActivityError("the Delta interop policy blocks creating this table: " + "; ".join(dec.reasons()), category="compatibility", retryable=False)

        def chained() -> Iterator[Any]:
            yield first
            yield from it

        reader = pa.RecordBatchReader.from_batches(first.schema, chained())
        try:
            dl.write_deltalake(uri, reader, mode="overwrite" if plan.mode == "overwrite" else "append" if exists or plan.mode == "append" else "error", storage_options=opts,
                               commit_properties=dl.CommitProperties(custom_metadata={"olcf_run": actx.run_id, "olcf_activity": actx.name},
                                                                     app_transactions=[dl.Transaction(app_id=app_id, version=1)]))
        except ActivityError:
            raise
        except Exception as exc:  # noqa: BLE001
            if actx.is_cancelled():
                raise
            msg = str(exc)
            bad = any(w in msg.lower() for w in ("schema", "mismatch", "cast"))
            raise ActivityError(f"writing table '{plan.dst_table}' failed: {msg}", category="data" if bad else "storage", retryable=not bad) from None
        dt2 = dl.DeltaTable(uri, storage_options=opts)
        cat = actx.ctx.maybe_service("catalog")
        if cat is not None and hasattr(cat, "sync_lakehouse"):
            try:
                cat.sync_lakehouse(plan.dst_lh, tables=[str(plan.dst_table)])
            except Exception:  # noqa: BLE001 - catalog refresh is best effort
                pass
        self._lineage(actx, plan)
        return {"filesCopied": counted["files"], "rowsCopied": counted["rows"], "table": plan.dst_table, "deltaVersion": dt2.version(), "manifestKey": plan.key}

    # -- lineage
    @staticmethod
    def _endpoints(plan: _Plan) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """The source and sink datasets of a finished copy as catalog descriptors (``{kind, lakehouse_id, name | path}``, at most 20 paths each side)."""
        if plan.src_table:
            src_ds: list[dict[str, Any]] = [{"kind": "table", "lakehouse_id": plan.src_lh.id, "name": str(plan.src_table).split(".")[-1]}]
        else:
            paths = sorted({f.path.rsplit("/", 1)[0] if "/" in f.path else f.path for f, _ in plan.items})[:20]
            src_ds = [{"kind": "file", "lakehouse_id": plan.src_lh.id, "path": p} for p in paths]
        if plan.dst_table:
            dst_ds: list[dict[str, Any]] = [{"kind": "table", "lakehouse_id": plan.dst_lh.id, "name": str(plan.dst_table).split(".")[-1]}]
        else:
            paths = sorted({d.rsplit("/", 1)[0] if "/" in d else d for _, d in plan.items})[:20]
            dst_ds = [{"kind": "file", "lakehouse_id": plan.dst_lh.id, "path": p} for p in paths]
        return src_ds, dst_ds

    def _announce(self, actx: ActivityContext, plan: _Plan) -> None:
        """Publish ``dataset.read`` (source) and ``dataset.written`` (sink) for a copy that just succeeded; best effort, never fails the copy (see :mod:`olcf.core.dataset_events`)."""
        ev: dict[str, Any] | None = None
        with dsev.best_effort(log, "dataset events (copy)"):
            src_ds, dst_ds = self._endpoints(plan)
            run = actx.run
            proc = dsev.process_descriptor("pipeline", item_id=run.get("pipeline_id"), name=f"{run.get('pipeline_name')}/{actx.name}", run_id=run.get("id"))
            sink_mode = {"append": "append", "overwrite": "overwrite"}.get(str(plan.mode), "create")
            src_fmt, dst_fmt = ("delta" if plan.src_table else plan.src_fmt), ("delta" if plan.dst_table else plan.dst_fmt)
            ev = {"proc": proc, "mode": sink_mode, "at": now_iso(),
                  "src": [dict(dsev.from_catalog_dataset(d) or {}, format=src_fmt) for d in src_ds], "dst": [dict(dsev.from_catalog_dataset(d) or {}, format=dst_fmt) for d in dst_ds]}
        if ev is None:
            return
        ws = actx.workspace_id
        base = {"workspace_id": ws, "process": ev["proc"], "engine": "python", "statement_hash": None, "at": ev["at"]}
        with dsev.best_effort(log, "dataset.read"):
            actx.ctx.events.publish("dataset.read", {**base, "datasets": ev["src"], "mode": None}, workspace_id=ws, subject=actx.run_id)
        with dsev.best_effort(log, "dataset.written"):
            actx.ctx.events.publish("dataset.written", {**base, "datasets": ev["dst"], "mode": ev["mode"]}, workspace_id=ws, subject=actx.run_id)

    def _lineage(self, actx: ActivityContext, plan: _Plan) -> None:
        self._announce(actx, plan)
        cat = actx.ctx.maybe_service("catalog")
        if cat is None:
            return
        try:
            src_ds, dst_ds = self._endpoints(plan)
            if src_ds and dst_ds:
                cat.record_lineage(None, workspace_id=plan.dst_lh.workspace_id, sources=src_ds, targets=dst_ds,
                                   via={"kind": "pipeline", "name": f"{actx.run.get('pipeline_name')}/{actx.name}", "ref": actx.run_id}, authorize=False)
        except Exception:  # noqa: BLE001 - lineage is best effort
            actx.log("lineage could not be recorded")


COPY_ACTIVITIES: list[type[Activity]] = [CopyActivity]
