# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Lakehouse tables: discovery, inspection, preview, maintenance and managed writes.

Formats: **Delta** (delta-rs), **Iceberg** (metadata JSON + optional PyIceberg) and plain **Parquet** directories.
Every operation that *writes* or *maintains* a Delta table first evaluates the interoperability policy
(:mod:`olcf.storage.delta_policy`) against the table's real ``_delta_log`` protocol and refuses (``CompatibilityBlocked``)
to touch tables whose protocol/features would break Fabric interoperability or exceed what the embedded engine can honour.
Bulk data is never serialised into API responses except by the explicit ``preview`` methods (row-capped).
"""

from __future__ import annotations

import fnmatch
import io
import json
import logging
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from olcf.core.errors import BadRequest, CompatibilityBlocked, Conflict, NotFound, Unsupported
from olcf.storage import delta_policy as policy
from olcf.storage import iceberg as ice
from olcf.storage import paths as P
from olcf.storage.arrowjson import column_descriptors, jsonable, table_rows
from olcf.storage.lakehouse import Lakehouse
from olcf.storage.providers.base import FileSystem, RangeFile

if TYPE_CHECKING:  # pragma: no cover
    from olcf.storage.service import StorageService

log = logging.getLogger("olcf.storage.tables")
MAX_PREVIEW_ROWS_HARD = 5000
SOURCE_SUFFIXES = {"csv": (".csv", ".csv.gz", ".tsv"), "parquet": (".parquet", ".pq"), "json": (".json", ".jsonl", ".ndjson")}


def _import_deltalake() -> Any:
    try:
        import deltalake

        return deltalake
    except ImportError as exc:  # pragma: no cover
        from olcf.core.errors import Unavailable

        raise Unavailable("delta-rs (the 'deltalake' package) is not installed") from exc


class TableService:
    """Reads and maintains the tables of a lakehouse: discovery, protocol report, history, preview, write, optimise, vacuum and drop. Writes go through the Delta policy."""
    def __init__(self, storage: StorageService) -> None:
        self.s = storage

    # ------------------------------------------------------------------ plumbing
    @property
    def ctx(self):  # noqa: ANN201
        return self.s.ctx

    def profile(self) -> str:
        try:
            return self.ctx.service("settings").get("storage.delta_interop_profile") or policy.DEFAULT_PROFILE
        except Exception:  # noqa: BLE001
            return policy.DEFAULT_PROFILE

    def _rel(self, lh: Lakehouse, name: str) -> str:
        return P.table_rel_path(name, schema_enabled=lh.schema_enabled)

    def _uri(self, lh: Lakehouse, rel: str) -> str:
        return self.s.provider(lh).physical_uri(lh.root_uri, rel)

    def _opts(self, lh: Lakehouse) -> dict[str, str] | None:
        return self.s.delta_storage_options(lh) or None

    def open_delta(self, lh: Lakehouse, name: str, *, version: int | None = None, as_of: str | None = None) -> Any:
        dl = _import_deltalake()
        rel = self._rel(lh, name)
        uri = self._uri(lh, rel)
        opts = self._opts(lh)
        try:
            dt = dl.DeltaTable(uri, storage_options=opts, version=version) if version is not None else dl.DeltaTable(uri, storage_options=opts)
        except Exception as exc:  # noqa: BLE001
            if exc.__class__.__name__ == "TableNotFoundError" or "not a delta table" in str(exc).lower() or "no files in log segment" in str(exc).lower():
                raise NotFound(f"Delta table '{name}' does not exist in lakehouse '{lh.name}'") from None
            raise
        if as_of:
            try:
                dt.load_as_version(datetime.fromisoformat(as_of.replace("Z", "+00:00")).astimezone(UTC))
            except ValueError as exc:
                raise BadRequest(f"as_of must be an ISO-8601 timestamp: {exc}") from exc
        return dt

    # ------------------------------------------------------------------ discovery
    def _classify(self, fs: FileSystem, rel: str) -> str | None:
        entries = {f.name: f for f in fs.list(rel, recursive=False)}
        if "_delta_log" in entries and entries["_delta_log"].is_dir:
            return "delta"
        if "metadata" in entries and entries["metadata"].is_dir and ice.is_iceberg_dir(fs, rel):
            return "iceberg"
        if any((not e.is_dir and e.name.endswith((".parquet", ".pq"))) or (e.is_dir and "=" in e.name) for e in entries.values()):
            return "parquet"
        return None

    def discover(self, lh: Lakehouse) -> list[dict[str, Any]]:
        """List the tables below ``Tables/`` (one level, or ``schema/table`` when the lakehouse is schema-enabled)."""
        fs = self.s.filesystem(lh)
        out: list[dict[str, Any]] = []
        for child in sorted((f for f in fs.list("Tables", recursive=False) if f.is_dir), key=lambda f: f.name):
            if child.name.startswith(("_", ".")):
                continue
            fmt = self._classify(fs, child.path)
            if fmt:
                out.append(self._ref(lh, child.name, child.path, fmt, None))
            elif lh.schema_enabled:
                for sub in sorted((f for f in fs.list(child.path, recursive=False) if f.is_dir), key=lambda f: f.name):
                    f2 = self._classify(fs, sub.path)
                    if f2:
                        out.append(self._ref(lh, f"{child.name}.{sub.name}", sub.path, f2, child.name))
        return out

    def _ref(self, lh: Lakehouse, name: str, rel: str, fmt: str, schema: str | None) -> dict[str, Any]:
        return {"name": name, "schema": schema, "table": name.split(".")[-1], "format": fmt, "path": rel, "location": self._uri(lh, rel),
                "lakehouse_id": lh.id}

    def locate(self, lh: Lakehouse, name: str) -> dict[str, Any]:
        rel = self._rel(lh, name)
        fs = self.s.filesystem(lh)
        if not fs.is_dir(rel):
            raise NotFound(f"table '{name}' does not exist in lakehouse '{lh.name}'")
        fmt = self._classify(fs, rel)
        if fmt is None:
            raise NotFound(f"'{rel}' exists but is not a recognised table (Delta, Iceberg or Parquet)")
        return self._ref(lh, name, rel, fmt, name.split(".")[0] if "." in name else None)

    # ------------------------------------------------------------------ inspection
    @staticmethod
    def _delta_fields(dt: Any) -> list[dict[str, Any]]:
        fields = json.loads(dt.schema().to_json())["fields"]
        return [{"name": f["name"], "type": f["type"] if isinstance(f["type"], str) else json.dumps(f["type"]), "nullable": f.get("nullable", True),
                 "metadata": f.get("metadata") or {}} for f in fields]

    def _delta_stats(self, dt: Any) -> dict[str, Any]:
        try:
            aa = pa.table(dt.get_add_actions(flatten=True))
        except Exception:  # noqa: BLE001
            return {"num_files": None, "size_bytes": None, "num_rows": None, "last_modified": None}
        rows = None
        if "num_records" in aa.column_names:
            vals = [v for v in aa.column("num_records").to_pylist() if v is not None]
            rows = sum(vals) if len(vals) == aa.num_rows else None
        mod = None
        if "modification_time" in aa.column_names and aa.num_rows:
            mx = max(v for v in aa.column("modification_time").to_pylist() if v is not None)
            mod = datetime.fromtimestamp(mx / 1000, UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        return {"num_files": aa.num_rows, "size_bytes": sum(aa.column("size_bytes").to_pylist()) if aa.num_rows else 0, "num_rows": rows, "last_modified": mod}

    def protocol_report(self, dt: Any, *, profile: str | None = None) -> dict[str, Any]:
        proto = policy.Protocol.from_any(dt.protocol())
        prof = profile or self.profile()
        return {
            "protocol": proto.to_dict(),
            "effective_reader_features": sorted(proto.effective_reader_features()),
            "effective_writer_features": sorted(proto.effective_writer_features()),
            "profile": prof, "policy_version": policy.POLICY_VERSION,
            "read": {e: policy.evaluate(proto, "read", engine=e, profile=prof).to_dict() for e in ("spark", "delta-rs")},
            "write": {e: policy.evaluate(proto, "write", engine=e, profile=prof).to_dict() for e in ("spark", "delta-rs")},
            "compatibility": policy.compatibility_matrix(proto),
        }

    def protocol(self, lh: Lakehouse, name: str) -> dict[str, Any]:
        ref = self.locate(lh, name)
        if ref["format"] != "delta":
            raise Unsupported(f"'{name}' is a {ref['format']} table; Delta protocol information applies to Delta tables only")
        dt = self.open_delta(lh, name)
        return {"name": name, "version": dt.version(), **self.protocol_report(dt)}

    def history(self, lh: Lakehouse, name: str, limit: int = 30) -> list[dict[str, Any]]:
        ref = self.locate(lh, name)
        if ref["format"] == "iceberg":
            return self._iceberg_summary(lh, name)["snapshots"][::-1][:limit]
        if ref["format"] != "delta":
            raise Unsupported("history is available for Delta and Iceberg tables")
        return self._delta_history(lh, name, limit)

    def _delta_history(self, lh: Lakehouse, name: str, limit: int) -> list[dict[str, Any]]:
        """Newest-first ``commitInfo`` entries read straight from ``_delta_log/*.json`` through the storage layer.

        Reading the log ourselves (instead of ``DeltaTable.history``) works identically on every provider, needs no
        engine credentials plumbing and only fetches the ``limit`` newest commit files.
        """
        fs = self.s.filesystem(lh)
        log = self._rel(lh, name) + "/_delta_log"
        versions: list[tuple[int, str]] = []
        for f in fs.list(log):
            base = f.name
            if not f.is_dir and base.endswith(".json") and base[:-5].isdigit():
                versions.append((int(base[:-5]), f.path))
        versions.sort(reverse=True)
        out: list[dict[str, Any]] = []
        for version, path in versions[:limit]:
            info: dict[str, Any] = {}
            with fs.open_read(path) as fh:
                for line in io.TextIOWrapper(fh, encoding="utf-8"):  # type: ignore[arg-type]
                    if '"commitInfo"' in line:
                        info = json.loads(line).get("commitInfo") or {}
                        break
            ts = info.get("timestamp")
            out.append({"version": version,
                        "timestamp": None if ts is None else datetime.fromtimestamp(ts / 1000, UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                        "operation": info.get("operation"), "operation_parameters": jsonable(info.get("operationParameters") or {}),
                        "operation_metrics": jsonable(info.get("operationMetrics") or {}),
                        "engine": info.get("engineInfo") or info.get("clientVersion"),
                        "user": info.get("userName"), "read_version": info.get("readVersion")})
        return out

    def _iceberg_meta_path(self, lh: Lakehouse, name: str) -> str:
        fs = self.s.filesystem(lh)
        meta = ice.find_metadata_file(fs, self._rel(lh, name))
        if not meta:
            raise NotFound(f"no Iceberg metadata found for '{name}'")
        return meta

    def _iceberg_summary(self, lh: Lakehouse, name: str) -> dict[str, Any]:
        fs = self.s.filesystem(lh)
        return ice.summarize(ice.read_metadata(fs, self._iceberg_meta_path(lh, name)))

    def info(self, lh: Lakehouse, name: str) -> dict[str, Any]:
        ref = self.locate(lh, name)
        base = {**ref, "lakehouse": lh.name, "workspace_id": lh.workspace_id}
        if ref["format"] == "delta":
            dt = self.open_delta(lh, name)
            md = dt.metadata()
            stats = self._delta_stats(dt)
            rep = self.protocol_report(dt)
            try:
                parts = dt.partitions()
            except Exception:  # noqa: BLE001
                parts = []
            return {**base, "version": dt.version(), "schema": self._delta_fields(dt), "partition_columns": list(md.partition_columns),
                    "partitions": {"count": len(parts), "sample": parts[:50]}, "properties": dict(md.configuration or {}),
                    "description": md.description, "name_in_log": md.name,
                    "created_time": None if md.created_time is None else datetime.fromtimestamp(md.created_time / 1000, UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    "protocol": rep["protocol"], "interop": {k: rep[k] for k in ("profile", "policy_version", "read", "write", "compatibility")},
                    "history": self.history(lh, name, 10), **stats}
        if ref["format"] == "iceberg":
            s = self._iceberg_summary(lh, name)
            return {**base, "version": s["current_snapshot_id"], "schema": s["schema"], "partition_columns": [p["source_column"] for p in s["partition_spec"]],
                    "partition_spec": s["partition_spec"], "properties": s["properties"], "format_version": s["format_version"],
                    "current_snapshot_id": s["current_snapshot_id"], "snapshots": s["snapshots"][-20:], "last_modified": s["last_updated"],
                    "num_files": None, "size_bytes": None,
                    "num_rows": _int_or_none((next((x for x in s["snapshots"] if x["snapshot_id"] == s["current_snapshot_id"]), {}).get("summary") or {}).get("total-records")),
                    "pyiceberg_available": ice.pyiceberg_available(),
                    "history": s["snapshots"][::-1][:10]}
        fs = self.s.filesystem(lh)
        files = [f for f in fs.list(ref["path"], recursive=True) if not f.is_dir and f.name.endswith((".parquet", ".pq"))]
        schema = self._parquet_schema(lh, fs, files[0].path) if files else None
        return {**base, "version": None, "schema": [{"name": f.name, "type": str(f.type), "nullable": f.nullable, "metadata": {}} for f in schema] if schema else [],
                "partition_columns": sorted({seg.split("=")[0] for f in files for seg in f.path.split("/") if "=" in seg}), "properties": {},
                "num_files": len(files), "size_bytes": sum(f.size for f in files), "num_rows": None, "history": [],
                "last_modified": max((f.modified for f in files if f.modified), default=None)}

    def _parquet_schema(self, lh: Lakehouse, fs: FileSystem, rel: str) -> pa.Schema:
        import pyarrow.parquet as pq

        return pq.ParquetFile(self._seekable(fs, rel)).schema_arrow

    @staticmethod
    def _seekable(fs: FileSystem, rel: str) -> Any:
        local = fs.local_path(rel)
        return local if local else RangeFile(fs, rel)

    # ------------------------------------------------------------------ data access
    def preview(self, lh: Lakehouse, name: str, rows: int = 100, *, version: int | None = None, as_of: str | None = None,
                snapshot_id: int | None = None, columns: list[str] | None = None) -> dict[str, Any]:
        rows = max(1, min(int(rows), MAX_PREVIEW_ROWS_HARD, self.ctx.settings.storage.max_preview_rows))
        ref = self.locate(lh, name)
        extra: dict[str, Any] = {}
        if ref["format"] == "delta":
            dt = self.open_delta(lh, name, version=version, as_of=as_of)
            self._require_read_allowed(dt)
            ds = dt.to_pyarrow_dataset()
            tbl = ds.scanner(columns=columns or None).head(rows + 1)
            extra["version"] = dt.version()
        elif ref["format"] == "iceberg":
            if version is not None:
                raise BadRequest("use snapshot_id or as_of for Iceberg time travel")
            summary = self._iceberg_summary(lh, name)
            sid = ice.snapshot_for(summary, snapshot_id=snapshot_id, as_of=as_of)
            ice.require_pyiceberg()
            t = ice.load_static_table(self._uri(lh, self._iceberg_meta_path(lh, name)), self._iceberg_props(lh))
            tbl = ice.scan_arrow(t, limit=rows + 1, snapshot_id=sid, columns=tuple(columns) if columns else None)
            extra["snapshot_id"] = sid or summary["current_snapshot_id"]
        else:
            if version is not None or as_of or snapshot_id:
                raise BadRequest("time travel is not available for plain Parquet tables")
            ds = self._parquet_dataset(lh, ref)
            tbl = ds.scanner(columns=columns or None).head(rows + 1)
        more = tbl.num_rows > rows
        tbl = tbl.slice(0, rows)
        return {"name": name, "format": ref["format"], "columns": column_descriptors(tbl.schema), "rows": table_rows(tbl), "row_count": tbl.num_rows,
                "truncated": more, **extra}

    def _iceberg_props(self, lh: Lakehouse) -> dict[str, str]:
        try:
            return self.s.iceberg_properties(lh)
        except Unsupported:
            return {}

    def _parquet_dataset(self, lh: Lakehouse, ref: dict[str, Any]) -> Any:
        import pyarrow.dataset as pads

        fs = self.s.filesystem(lh)
        local = fs.local_path(ref["path"])
        if local:
            return pads.dataset(local, format="parquet", partitioning="hive")
        from pyarrow import fs as pafs

        handler = _PaFs(fs)
        return pads.dataset(ref["path"], filesystem=pafs.PyFileSystem(handler), format="parquet", partitioning="hive")

    def scan_source(self, lh: Lakehouse, name: str) -> Any:
        """An Arrow dataset/table the SQL engine can register for ``name`` (lazy for Delta/Parquet)."""
        ref = self.locate(lh, name)
        if ref["format"] == "delta":
            dt = self.open_delta(lh, name)
            self._require_read_allowed(dt)
            return dt.to_pyarrow_dataset()
        if ref["format"] == "iceberg":
            ice.require_pyiceberg()
            summary = self._iceberg_summary(lh, name)
            total = _int_or_none((next((x for x in summary["snapshots"] if x["snapshot_id"] == summary["current_snapshot_id"]), {}).get("summary") or {}).get("total-files-size"))
            cap = self.ctx.settings.storage.max_upload_mb * 1024 * 1024
            if total is not None and total > cap:
                raise Unsupported(f"Iceberg table '{name}' is {total / 1e6:.0f} MB, above the embedded engine limit of {cap / 1e6:.0f} MB",
                                  remediation="query it with the Spark engine")
            t = ice.load_static_table(self._uri(lh, self._iceberg_meta_path(lh, name)), self._iceberg_props(lh))
            return ice.scan_arrow(t)
        return self._parquet_dataset(lh, ref)

    def _require_read_allowed(self, dt: Any) -> None:
        d = policy.evaluate(dt.protocol(), "read", engine="delta-rs", profile=self.profile())
        if not d.allowed:
            raise CompatibilityBlocked("this table uses Delta features the embedded engine cannot read", details={"policy": d.to_dict()},
                                       remediation="use the Spark engine, or upgrade/downgrade the table with Spark")

    # ------------------------------------------------------------------ policy-guarded writes
    def ensure_write_allowed(self, lh: Lakehouse, name: str, *, engine: str = "delta-rs") -> dict[str, Any]:
        dt = self.open_delta(lh, name)
        d = policy.evaluate(dt.protocol(), "write", engine=engine, profile=self.profile())
        if not d.allowed:
            raise CompatibilityBlocked(f"policy {policy.POLICY_VERSION} blocks writing '{name}' with {engine}: " + "; ".join(d.reasons()),
                                       details={"policy": d.to_dict()},
                                       remediation="run the operation with Spark, or choose a table whose protocol is within the interop policy")
        return d.to_dict()

    def write_table(self, lh: Lakehouse, name: str, data: Any, *, mode: str = "error", partition_by: list[str] | None = None,
                    schema_mode: str | None = None, properties: dict[str, str] | None = None, description: str | None = None) -> dict[str, Any]:
        """Policy-checked Delta write of an Arrow table / RecordBatchReader (managed by OLCF, via delta-rs)."""
        dl = _import_deltalake()
        if mode not in ("error", "append", "overwrite", "ignore"):
            raise BadRequest("mode must be error|append|overwrite|ignore")
        rel = self._rel(lh, name)
        uri = self._uri(lh, rel)
        exists = self.s.filesystem(lh).is_dir(rel) and self._classify(self.s.filesystem(lh), rel) is not None
        schema = data.schema
        if exists:
            ref = self.locate(lh, name)
            if ref["format"] != "delta":
                raise Conflict(f"'{name}' exists and is a {ref['format']} table; the embedded engine only writes Delta")
            decision = self.ensure_write_allowed(lh, name)
            if mode == "error":
                raise Conflict(f"table '{name}' already exists", remediation="use mode=overwrite or mode=append")
        else:
            requested = policy.protocol_for_schema(schema, properties)
            d = policy.evaluate(requested, "write", engine="delta-rs", profile=self.profile())
            if not d.allowed:
                raise CompatibilityBlocked("policy blocks creating this table: " + "; ".join(d.reasons()), details={"policy": d.to_dict()},
                                           remediation="cast timestamp columns to timestamp with time zone (UTC) or create the table with Spark")
            decision = d.to_dict()
        kw: dict[str, Any] = {"mode": mode}
        if partition_by:
            missing = [c for c in partition_by if c not in schema.names]
            if missing:
                raise BadRequest(f"partition columns not in the data: {missing}")
            kw["partition_by"] = partition_by
        if schema_mode:
            kw["schema_mode"] = schema_mode
        if properties:
            kw["configuration"] = {k: str(v) for k, v in properties.items()}
        if description:
            kw["description"] = description
        try:
            dl.write_deltalake(uri, data, storage_options=self._opts(lh), **kw)
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            if "schema" in msg.lower() and ("mismatch" in msg.lower() or "not match" in msg.lower() or "different" in msg.lower()):
                raise BadRequest(f"the data schema does not match table '{name}': {msg}", remediation="pass schema_mode=merge to evolve the schema") from exc
            raise
        dt = self.open_delta(lh, name)
        return {"name": name, "version": dt.version(), "mode": mode, "policy": decision}

    # ------------------------------------------------------------------ maintenance
    def optimize(self, lh: Lakehouse, name: str, *, z_order_by: list[str] | None = None, partition_filters: list[tuple[str, str, Any]] | None = None,
                 target_size: int | None = None) -> dict[str, Any]:
        ref = self.locate(lh, name)
        if ref["format"] != "delta":
            raise Unsupported(f"OPTIMIZE through the embedded engine supports Delta tables only ('{name}' is {ref['format']})",
                              remediation="run compaction with Spark (Iceberg rewrite_data_files)")
        self.ensure_write_allowed(lh, name)
        dt = self.open_delta(lh, name)
        kw: dict[str, Any] = {}
        if partition_filters:
            kw["partition_filters"] = [tuple(f) for f in partition_filters]
        if target_size:
            kw["target_size"] = int(target_size)
        if z_order_by:
            cols = [f["name"] for f in self._delta_fields(dt)]
            bad = [c for c in z_order_by if c not in cols]
            if bad:
                raise BadRequest(f"unknown Z-order columns: {bad}", details={"columns": cols})
            if set(z_order_by) & set(dt.metadata().partition_columns):
                raise BadRequest("cannot Z-order by partition columns")
            metrics = dt.optimize.z_order(z_order_by, **kw)
            kind = "z-order"
        else:
            metrics = dt.optimize.compact(**kw)
            kind = "compact"
        clean: dict[str, Any] = {}
        for k, v in metrics.items():
            if isinstance(v, str) and v.startswith("{"):
                try:
                    v = json.loads(v)
                except ValueError:
                    pass
            clean[k] = v
        return {"name": name, "kind": kind, "version": self.open_delta(lh, name).version(), "metrics": clean}

    def min_vacuum_hours(self) -> int:
        try:
            v = self.ctx.service("settings").get("storage.vacuum_min_retention_hours")
            return int(168 if v is None else v)
        except Exception:  # noqa: BLE001
            return 168

    def vacuum(self, lh: Lakehouse, name: str, *, retention_hours: int | None = None, dry_run: bool = True, max_listed: int = 200) -> dict[str, Any]:
        """Delete unreferenced files. Dry-run by default; the caller must hold ``lakehouse.destructive``."""
        ref = self.locate(lh, name)
        if ref["format"] != "delta":
            raise Unsupported("VACUUM through the embedded engine supports Delta tables only")
        floor = self.min_vacuum_hours()
        hours = floor if retention_hours is None else int(retention_hours)
        if hours < floor:
            raise BadRequest(f"retention of {hours} h is below the safety threshold of {floor} h",
                             remediation="use a longer retention; only a PlatformAdmin may lower the 'storage.vacuum_min_retention_hours' setting",
                             details={"minimum_hours": floor})
        if hours < 0:
            raise BadRequest("retention_hours must be >= 0")
        self.ensure_write_allowed(lh, name)
        dt = self.open_delta(lh, name)
        table_default = 168
        enforce = hours >= table_default  # below the table's own retention only possible when the platform floor was lowered
        files = dt.vacuum(retention_hours=hours, dry_run=dry_run, enforce_retention_duration=enforce)
        return {"name": name, "dry_run": dry_run, "retention_hours": hours, "files_count": len(files), "files": files[:max_listed],
                "files_truncated": len(files) > max_listed, "minimum_hours": floor}

    def drop(self, lh: Lakehouse, name: str) -> int:
        self.locate(lh, name)
        return self.s.filesystem(lh).delete(self._rel(lh, name), recursive=True)

    # ------------------------------------------------------------------ create from files
    def create_from_files(self, lh: Lakehouse, name: str, source: str, *, fmt: str | None = None, mode: str = "error",
                          partition_by: list[str] | None = None, options: dict[str, Any] | None = None) -> dict[str, Any]:
        """Load CSV / Parquet / JSON(L) files under ``Files/`` into a Delta table (streamed, policy-checked)."""
        options = options or {}
        src = P.normalize_rel(source, what="source")
        P.require_area(src, ("Files",), what="source")
        fs = self.s.filesystem(lh)
        files = self._source_files(fs, src, fmt)
        fmt = fmt or self._guess_format(files[0].path)
        total = sum(f.size for f in files)
        cap = int(self.ctx.settings.storage.max_upload_mb) * 4 * 1024 * 1024
        if total > cap:
            raise BadRequest(f"source data is {total / 1e6:.0f} MB, above the embedded ingest limit of {cap / 1e6:.0f} MB",
                             remediation="load large datasets with a Spark notebook or pipeline")
        reader = self._reader(fs, [f.path for f in files], fmt, options)
        if options.get("naive_timestamps", "assume_utc") == "assume_utc":
            reader = _naive_to_utc(reader)
        res = self.write_table(lh, name, reader, mode=mode, partition_by=partition_by)
        return {**res, "source_files": len(files), "source_bytes": total, "format": fmt}

    def _source_files(self, fs: FileSystem, src: str, fmt: str | None) -> list[Any]:
        if any(ch in src for ch in "*?["):
            base = src.rsplit("/", 1)[0] if "/" in src else ""
            found = [f for f in fs.list(base, recursive=True) if not f.is_dir and fnmatch.fnmatch(f.path, src)]
        else:
            info = fs.stat(src)
            if info.is_dir:
                suffixes = SOURCE_SUFFIXES.get(fmt or "", tuple(s for v in SOURCE_SUFFIXES.values() for s in v))
                found = [f for f in fs.list(src, recursive=True) if not f.is_dir and f.path.lower().endswith(suffixes) and not f.name.startswith(("_", "."))]
            else:
                found = [info]
        if not found:
            raise NotFound(f"no source files matched '{src}'")
        return sorted(found, key=lambda f: f.path)

    @staticmethod
    def _guess_format(path: str) -> str:
        low = path.lower()
        for fmt, sfx in SOURCE_SUFFIXES.items():
            if low.endswith(sfx):
                return fmt
        raise BadRequest(f"cannot infer the file format of '{path}'", remediation="pass format=csv|parquet|json")

    def _reader(self, fs: FileSystem, paths: list[str], fmt: str, options: dict[str, Any]) -> pa.RecordBatchReader:
        schema_holder: dict[str, pa.Schema] = {}

        def batches() -> Iterator[pa.RecordBatch]:
            for path in paths:
                for b in self._file_batches(fs, path, fmt, options):
                    if "s" not in schema_holder:
                        schema_holder["s"] = b.schema
                    if b.schema != schema_holder["s"]:
                        try:
                            b = pa.Table.from_batches([b]).cast(schema_holder["s"]).to_batches()[0]
                        except Exception as exc:  # noqa: BLE001
                            raise BadRequest(f"file '{path}' has a different schema than the first file: {exc}") from exc
                    yield b

        it = batches()
        first = next(it, None)
        if first is None:
            raise BadRequest("the source files contain no rows")

        def chain() -> Iterator[pa.RecordBatch]:
            yield first
            yield from it

        return pa.RecordBatchReader.from_batches(first.schema, chain())

    def _file_batches(self, fs: FileSystem, path: str, fmt: str, options: dict[str, Any]) -> Iterator[pa.RecordBatch]:
        if fmt == "csv":
            import pyarrow.csv as pacsv

            delim = options.get("delimiter") or ("\t" if path.lower().endswith(".tsv") else ",")
            ro = pacsv.ReadOptions(autogenerate_column_names=not options.get("header", True), block_size=8 << 20)
            po = pacsv.ParseOptions(delimiter=delim)
            with fs.open_read(path) as fh:
                src: Any = fh
                if path.lower().endswith(".gz"):
                    import gzip

                    src = gzip.GzipFile(fileobj=fh)  # type: ignore[arg-type]
                rd = pacsv.open_csv(src, read_options=ro, parse_options=po)
                yield from rd
        elif fmt == "parquet":
            import pyarrow.parquet as pq

            pf = pq.ParquetFile(self._seekable(fs, path))
            yield from pf.iter_batches(batch_size=65536)
        elif fmt == "json":
            import pyarrow.json as pajson

            size = fs.stat(path).size
            if size > 256 * 1024 * 1024:
                raise BadRequest("JSON sources larger than 256 MB are not supported by the embedded engine; use Parquet/CSV or Spark")
            import io

            tbl = pajson.read_json(io.BytesIO(fs.read_bytes(path)))
            yield from tbl.to_batches()
        else:
            raise BadRequest(f"unsupported source format '{fmt}'", details={"supported": sorted(SOURCE_SUFFIXES)})


def _naive_to_utc(reader: pa.RecordBatchReader) -> pa.RecordBatchReader:
    """Interpret timezone-less timestamps as UTC so new tables stay on the base Delta protocol (no ``timestampNtz`` upgrade)."""
    fields = []
    changed = False
    for f in reader.schema:
        if pa.types.is_timestamp(f.type) and f.type.tz is None:
            fields.append(pa.field(f.name, pa.timestamp(f.type.unit, tz="UTC"), f.nullable))
            changed = True
        else:
            fields.append(f)
    if not changed:
        return reader
    target = pa.schema(fields)

    def gen() -> Iterator[pa.RecordBatch]:
        for b in reader:
            yield pa.Table.from_batches([b]).cast(target).to_batches()[0] if b.num_rows else pa.RecordBatch.from_pylist([], schema=target)

    return pa.RecordBatchReader.from_batches(target, gen())


def _int_or_none(v: Any) -> int | None:
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


class _PaFs:
    """Minimal ``pyarrow.fs.FileSystemHandler`` over an OLCF ``FileSystem`` (read-only) for non-local Parquet datasets."""

    def __new__(cls, fs: FileSystem) -> Any:
        from pyarrow import fs as pafs

        class Handler(pafs.FileSystemHandler):
            def __init__(self) -> None:
                self._fs = fs

            def get_type_name(self) -> str:
                return "olcf"

            def normalize_path(self, path: str) -> str:
                return path

            def get_file_info(self, paths: list[str]) -> list[Any]:
                out = []
                for p in paths:
                    try:
                        i = self._fs.stat(p)
                        out.append(pafs.FileInfo(p, pafs.FileType.Directory if i.is_dir else pafs.FileType.File, size=None if i.is_dir else i.size))
                    except NotFound:
                        out.append(pafs.FileInfo(p, pafs.FileType.NotFound))
                return out

            def get_file_info_selector(self, selector: Any) -> list[Any]:
                out = []
                for i in self._fs.list(selector.base_dir, recursive=selector.recursive):
                    out.append(pafs.FileInfo(i.path, pafs.FileType.Directory if i.is_dir else pafs.FileType.File, size=None if i.is_dir else i.size))
                return out

            def open_input_stream(self, path: str) -> Any:
                return pa.PythonFile(RangeFile(self._fs, path), mode="r")

            def open_input_file(self, path: str) -> Any:
                return pa.PythonFile(RangeFile(self._fs, path), mode="r")

            def create_dir(self, *a: Any, **k: Any) -> None:
                raise Unsupported("read-only")

            delete_dir = delete_dir_contents = delete_root_dir_contents = delete_file = move = copy_file = create_dir
            open_output_stream = open_append_stream = create_dir

        return Handler()
