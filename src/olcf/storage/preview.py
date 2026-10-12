# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""File previews for the storage browser: CSV / JSON / Parquet (first N rows), text (first N bytes) and images."""

from __future__ import annotations

import io
import json
import mimetypes
from typing import Any

import pyarrow as pa

from olcf.core.errors import BadRequest
from olcf.storage.arrowjson import column_descriptors, image_data_uri, jsonable, table_rows
from olcf.storage.providers.base import FileSystem, RangeFile

TEXT_EXT = {".txt", ".md", ".log", ".yaml", ".yml", ".toml", ".ini", ".cfg", ".py", ".sql", ".sh", ".xml", ".html", ".htm", ".css", ".js", ".ts",
            ".ipynb", ".r", ".scala", ".java", ".properties", ".conf", ".json", ".jsonl", ".ndjson", ".csv", ".tsv"}
IMAGE_EXT = {".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".gif": "image/gif", ".webp": "image/webp", ".svg": "image/svg+xml", ".bmp": "image/bmp"}
MAX_IMAGE_BYTES = 2 * 1024 * 1024


def _ext(path: str) -> str:
    n = path.lower()
    if n.endswith(".gz"):
        n = n[:-3]
    return "." + n.rsplit(".", 1)[-1] if "." in n.rsplit("/", 1)[-1] else ""


def _text(data: bytes) -> str:
    return data.decode("utf-8", errors="replace")


def preview_file(fs: FileSystem, rel: str, *, rows: int = 100, max_bytes: int = 1 << 20) -> dict[str, Any]:
    """Return a JSON-able preview. ``kind`` is ``table`` | ``text`` | ``image`` | ``binary``."""
    info = fs.stat(rel)
    if info.is_dir:
        raise BadRequest(f"'{rel}' is a directory")
    ext = _ext(rel)
    base = {"path": rel, "size": info.size, "modified": info.modified, "media_type": mimetypes.guess_type(rel)[0] or "application/octet-stream"}
    max_bytes = max(1024, min(max_bytes, 8 << 20))
    rows = max(1, min(rows, 1000))

    if ext in IMAGE_EXT:
        if info.size > MAX_IMAGE_BYTES:
            return {**base, "kind": "image", "inline": False, "note": f"image is larger than {MAX_IMAGE_BYTES // 1024} KiB; download it to view"}
        return {**base, "kind": "image", "inline": True, "data_uri": image_data_uri(fs.read_bytes(rel, MAX_IMAGE_BYTES), IMAGE_EXT[ext])}

    if ext in (".parquet", ".pq"):
        import pyarrow.parquet as pq

        local = fs.local_path(rel)
        pf = pq.ParquetFile(local if local else RangeFile(fs, rel))
        md = pf.metadata
        head = pa.Table.from_batches([next(pf.iter_batches(batch_size=rows), pa.RecordBatch.from_pylist([], schema=pf.schema_arrow))])
        return {**base, "kind": "table", "format": "parquet", "columns": column_descriptors(pf.schema_arrow), "rows": table_rows(head.slice(0, rows)),
                "total_rows": md.num_rows, "row_groups": md.num_row_groups, "created_by": md.created_by, "truncated": md.num_rows > head.num_rows}

    if ext in (".csv", ".tsv"):
        import pyarrow.csv as pacsv

        with fs.open_read(rel) as fh:
            chunk = fh.read(max_bytes + 1)
        if chunk[:2] == b"\x1f\x8b":
            import gzip

            chunk = gzip.GzipFile(fileobj=io.BytesIO(chunk)).read(max_bytes)
        cut = len(chunk) > max_bytes or info.size > len(chunk)
        if cut and b"\n" in chunk:
            chunk = chunk[: chunk.rindex(b"\n") + 1]
        try:
            tbl = pacsv.read_csv(io.BytesIO(chunk), parse_options=pacsv.ParseOptions(delimiter="\t" if ext == ".tsv" else ","))
        except Exception as exc:  # noqa: BLE001
            return {**base, "kind": "text", "text": _text(chunk[:max_bytes]), "truncated": cut, "note": f"could not parse as CSV: {exc}"}
        return {**base, "kind": "table", "format": "csv", "columns": column_descriptors(tbl.schema), "rows": table_rows(tbl.slice(0, rows)),
                "truncated": cut or tbl.num_rows > rows, "preview_rows": min(rows, tbl.num_rows)}

    if ext in (".jsonl", ".ndjson") or (ext == ".json" and info.size <= max_bytes):
        with fs.open_read(rel) as fh:
            chunk = fh.read(max_bytes + 1)
        truncated = len(chunk) > max_bytes
        text = _text(chunk[:max_bytes])
        try:
            if ext == ".json":
                if truncated:
                    raise ValueError("document larger than the preview window")
                doc = json.loads(text)
                if isinstance(doc, list) and doc and all(isinstance(x, dict) for x in doc):
                    objs = doc[:rows]
                    total = len(doc)
                else:
                    return {**base, "kind": "text", "format": "json", "text": json.dumps(doc, indent=2, default=str)[:max_bytes], "truncated": False}
            else:
                lines = text.splitlines()
                if truncated:
                    lines = lines[:-1]
                objs = [json.loads(line) for line in lines[:rows] if line.strip()]
                total = None
            cols: list[str] = []
            for o in objs:
                for k in o:
                    if k not in cols:
                        cols.append(k)
            return {**base, "kind": "table", "format": "json", "columns": [{"name": c, "type": "json", "kind": "other", "nullable": True} for c in cols],
                    "rows": [[jsonable(o.get(c)) for c in cols] for o in objs], "truncated": truncated or (total is not None and total > rows), "total_rows": total}
        except (ValueError, TypeError):
            return {**base, "kind": "text", "text": text, "truncated": truncated}

    if ext in TEXT_EXT or info.size == 0:
        with fs.open_read(rel) as fh:
            data = fh.read(max_bytes + 1)
        return {**base, "kind": "text", "text": _text(data[:max_bytes]), "truncated": len(data) > max_bytes}

    with fs.open_read(rel) as fh:
        head = fh.read(256)
    if b"\x00" not in head:  # probably text with an unknown extension
        with fs.open_read(rel) as fh:
            data = fh.read(max_bytes + 1)
        return {**base, "kind": "text", "text": _text(data[:max_bytes]), "truncated": len(data) > max_bytes}
    return {**base, "kind": "binary", "hex": head.hex(" "), "note": "binary file; download to inspect"}
