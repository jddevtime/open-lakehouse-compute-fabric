# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Streaming result export: CSV, JSON (array), NDJSON and Parquet.

Batches flow from the engine straight to the HTTP response, so memory stays proportional to one batch no matter how
large the result is.  Values are rendered with the same rules as the JSON API (see :mod:`olcf.storage.arrowjson`);
CSV cells that start with ``= + - @`` are prefixed with a single quote so spreadsheets cannot execute them as formulas.
"""

from __future__ import annotations

import csv
import io
import json
from collections.abc import Iterator
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from olcf.sql.engines import Stream
from olcf.storage.arrowjson import jsonable

#: format -> (content type, file extension)
FORMATS: dict[str, tuple[str, str]] = {
    "csv": ("text/csv; charset=utf-8", "csv"),
    "json": ("application/json", "json"),
    "ndjson": ("application/x-ndjson", "ndjson"),
    "parquet": ("application/vnd.apache.parquet", "parquet"),
}
_FORMULA = ("=", "+", "-", "@", "\t", "\r")


def _slice(stream: Stream, max_rows: int, state: dict[str, Any]) -> Iterator[pa.RecordBatch]:
    """Yield batches until ``max_rows`` rows were sent; records ``rows`` and ``truncated`` in ``state``."""
    for b in stream.batches:
        left = max_rows - state["rows"]
        if left <= 0:
            state["truncated"] = True
            return
        if b.num_rows > left:
            yield b.slice(0, left)
            state["rows"] += left
            state["truncated"] = True
            return
        state["rows"] += b.num_rows
        yield b


def _csv_cell(v: Any) -> Any:
    v = jsonable(v)
    if isinstance(v, str) and v.startswith(_FORMULA):
        return "'" + v
    if v is None:
        return ""
    if isinstance(v, (dict, list)):
        return json.dumps(v, separators=(",", ":"))
    return v


def encode(stream: Stream, fmt: str, max_rows: int, state: dict[str, Any]) -> Iterator[bytes]:
    """Encode ``stream`` as ``fmt``; ``state['rows']`` / ``state['truncated']`` are updated as rows are sent."""
    names = stream.schema.names
    batches = _slice(stream, max_rows, state)
    if fmt == "csv":
        buf = io.StringIO()
        w = csv.writer(buf, lineterminator="\r\n")
        w.writerow(names)
        yield "﻿".encode() + buf.getvalue().encode()   # BOM so Excel detects UTF-8
        for b in batches:
            buf = io.StringIO()
            w = csv.writer(buf, lineterminator="\r\n")
            cols = [b.column(i).to_pylist() for i in range(b.num_columns)]
            for r in range(b.num_rows):
                w.writerow([_csv_cell(c[r]) for c in cols])
            yield buf.getvalue().encode()
    elif fmt in ("json", "ndjson"):
        first = True
        if fmt == "json":
            yield b"["
        for b in batches:
            cols = [b.column(i).to_pylist() for i in range(b.num_columns)]
            out: list[str] = []
            for r in range(b.num_rows):
                obj = json.dumps({n: jsonable(c[r]) for n, c in zip(names, cols, strict=True)}, separators=(",", ":"))
                out.append(obj if fmt == "ndjson" else ("" if first else ",") + obj)
                first = False
            if fmt == "ndjson":
                yield ("\n".join(out) + "\n").encode() if out else b""
            else:
                yield "".join(out).encode()
        if fmt == "json":
            yield b"]"
    else:  # parquet
        sink = _Sink()
        with pq.ParquetWriter(sink, stream.schema, compression="zstd") as writer:
            for b in batches:
                writer.write_batch(b)
                data = sink.drain()
                if data:
                    yield data
        tail = sink.drain()
        if tail:
            yield tail


class _Sink(io.RawIOBase):
    """Write-only file object that hands its buffered bytes to the HTTP generator (Parquet writes sequentially)."""

    def __init__(self) -> None:
        super().__init__()
        self._buf = bytearray()
        self._pos = 0

    def writable(self) -> bool:
        return True

    def write(self, b: Any) -> int:  # type: ignore[override]
        self._buf.extend(b)
        self._pos += len(b)
        return len(b)

    def tell(self) -> int:
        return self._pos

    def drain(self) -> bytes:
        out = bytes(self._buf)
        self._buf.clear()
        return out
