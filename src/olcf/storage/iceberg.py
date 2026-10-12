# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Apache Iceberg support: discovery, metadata inspection and data access.

*Metadata* (schema, partition spec, snapshot history, properties) is read straight from the table's
``metadata/*.metadata.json`` document, so the Iceberg catalog pages work **without** PyIceberg.  *Data* access
(preview, time travel, DuckDB registration) uses PyIceberg's ``StaticTable``; if it is not installed the API degrades
with a clear, actionable message (``pip install 'olcf[iceberg]'``).  Writes to Iceberg tables are out of scope for the
embedded engine: use Spark.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from typing import Any

from olcf.core.errors import BadRequest, NotFound, Unavailable
from olcf.storage.providers.base import FileSystem

_META_RE = re.compile(r"^(?:(\d+)-[0-9a-fA-F-]+|v(\d+))(?:\.gz)?\.metadata\.json(?:\.gz)?$")
MAX_METADATA_BYTES = 64 * 1024 * 1024


def pyiceberg_available() -> bool:
    try:
        import pyiceberg  # noqa: F401

        return True
    except ImportError:
        return False


def require_pyiceberg() -> None:
    if not pyiceberg_available():
        raise Unavailable("PyIceberg is not installed, so Iceberg table data cannot be read by the embedded engine",
                          remediation="install the optional extra: pip install 'olcf[iceberg]' (schema and snapshot metadata remain available without it)")


def is_iceberg_dir(fs: FileSystem, rel: str) -> bool:
    return find_metadata_file(fs, rel) is not None


def find_metadata_file(fs: FileSystem, table_rel: str) -> str | None:
    """Path (root-relative) of the *current* ``metadata.json``: ``version-hint.text`` if present, else the highest version."""
    meta_dir = f"{table_rel.rstrip('/')}/metadata"
    cands: list[tuple[int, str]] = []
    hint: int | None = None
    for f in fs.list(meta_dir, recursive=False):
        if f.is_dir:
            continue
        if f.name == "version-hint.text":
            try:
                hint = int(fs.read_bytes(f.path, 1024).decode().strip())
            except (ValueError, NotFound):
                hint = None
            continue
        m = _META_RE.match(f.name)
        if m:
            cands.append((int(m.group(1) or m.group(2)), f.path))
    if not cands:
        return None
    if hint is not None:
        for v, path in cands:
            if v == hint:
                return path
    return max(cands)[1]


def _type_str(t: Any) -> str:
    if isinstance(t, str):
        return t
    if isinstance(t, dict):
        kind = t.get("type")
        if kind == "struct":
            return "struct<" + ", ".join(f"{f['name']}: {_type_str(f['type'])}" for f in t.get("fields", [])) + ">"
        if kind == "list":
            return f"list<{_type_str(t['element'])}>"
        if kind == "map":
            return f"map<{_type_str(t['key'])}, {_type_str(t['value'])}>"
    return json.dumps(t)


def _ts(ms: int | None) -> str | None:
    return None if ms is None else datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def read_metadata(fs: FileSystem, meta_path: str) -> dict[str, Any]:
    raw = fs.read_bytes(meta_path, MAX_METADATA_BYTES)
    if raw[:2] == b"\x1f\x8b":
        import gzip

        raw = gzip.decompress(raw)
    return json.loads(raw)


def summarize(meta: dict[str, Any]) -> dict[str, Any]:
    """Schema, partition spec, snapshots and properties from a metadata document (format v1 and v2)."""
    schemas = meta.get("schemas") or ([meta["schema"]] if "schema" in meta else [])
    cur_id = meta.get("current-schema-id", 0)
    schema = next((s for s in schemas if s.get("schema-id", 0) == cur_id), schemas[-1] if schemas else {"fields": []})
    id_to_name = {f["id"]: f["name"] for f in schema.get("fields", [])}
    specs = meta.get("partition-specs") or ([{"spec-id": 0, "fields": meta.get("partition-spec", [])}] if "partition-spec" in meta or "partition-specs" in meta else [])
    spec_id = meta.get("default-spec-id", 0)
    spec = next((s for s in specs if s.get("spec-id", 0) == spec_id), specs[-1] if specs else {"fields": []})
    snaps = meta.get("snapshots") or []
    cur_snap = meta.get("current-snapshot-id")
    return {
        "format_version": meta.get("format-version"), "table_uuid": meta.get("table-uuid"), "location": meta.get("location"),
        "last_updated": _ts(meta.get("last-updated-ms")),
        "schema": [{"id": f["id"], "name": f["name"], "type": _type_str(f["type"]), "nullable": not f.get("required", False), "doc": f.get("doc")}
                   for f in schema.get("fields", [])],
        "partition_spec": [{"name": p.get("name"), "transform": p.get("transform"), "source_column": id_to_name.get(p.get("source-id")),
                            "field_id": p.get("field-id")} for p in spec.get("fields", [])],
        "partition_spec_id": spec_id, "partition_specs_count": len(specs),
        "current_snapshot_id": cur_snap if cur_snap not in (None, -1) else None,
        "snapshots": [{"snapshot_id": s["snapshot-id"], "parent_id": s.get("parent-snapshot-id"), "timestamp": _ts(s.get("timestamp-ms")),
                       "operation": (s.get("summary") or {}).get("operation"), "summary": s.get("summary") or {},
                       "manifest_list": s.get("manifest-list")} for s in snaps],
        "properties": meta.get("properties") or {}, "sort_orders": meta.get("sort-orders") or [],
    }


def load_static_table(physical_metadata_uri: str, properties: dict[str, str] | None = None) -> Any:
    require_pyiceberg()
    from pyiceberg.table import StaticTable

    return StaticTable.from_metadata(physical_metadata_uri, properties=properties or {})


def snapshot_for(summary: dict[str, Any], *, snapshot_id: int | None = None, as_of: str | None = None) -> int | None:
    """Pick the snapshot for time travel: explicit id wins, else the newest snapshot at/before ``as_of`` (ISO timestamp)."""
    ids = {s["snapshot_id"] for s in summary["snapshots"]}
    if snapshot_id is not None:
        if snapshot_id not in ids:
            raise BadRequest(f"snapshot {snapshot_id} does not exist", details={"available": sorted(ids)[-20:]})
        return snapshot_id
    if as_of:
        try:
            cutoff = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
        except ValueError as exc:
            raise BadRequest(f"as_of must be an ISO-8601 timestamp, got '{as_of}'") from exc
        if cutoff.tzinfo is None:
            cutoff = cutoff.replace(tzinfo=UTC)
        eligible = [s for s in summary["snapshots"] if s["timestamp"] and datetime.fromisoformat(s["timestamp"].replace("Z", "+00:00")) <= cutoff]
        if not eligible:
            raise BadRequest(f"no snapshot exists at or before {as_of}")
        return max(eligible, key=lambda s: s["timestamp"])["snapshot_id"]
    return None


def scan_arrow(table: Any, *, limit: int | None = None, snapshot_id: int | None = None, columns: tuple[str, ...] | None = None) -> Any:
    """Materialise (a prefix of) an Iceberg snapshot as an Arrow table."""
    kw: dict[str, Any] = {}
    if snapshot_id is not None:
        kw["snapshot_id"] = snapshot_id
    if limit is not None:
        kw["limit"] = limit
    if columns:
        kw["selected_fields"] = columns
    return table.scan(**kw).to_arrow()
