# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Per-job log files, written by the hub in chunks and read in pages.

Requirement (spec section 34): *stream/paginate multi-GB logs; never load a whole log into controller memory.*

Layout below ``<data_dir>/jobs/<job_id>/logs/``: ``stdout.log``, ``stderr.log`` and ``combined.log`` (arrival order).
Offsets exposed by the API are **byte offsets** into one of those files, so a client can resume exactly where it stopped
(`next_offset`) and a page never splits a UTF-8 character. Reading seeks to the offset and reads at most ``limit`` bytes.

Agents may resend chunks after a reconnect; ``job_log`` chunks carry a per-stream ``seq`` and duplicates are dropped.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from olcf.core.errors import BadRequest, NotFound
from olcf.core.redaction import redact_text

log = logging.getLogger("olcf.compute.logs")

_ID_OK = re.compile(r"^[A-Za-z]{2,8}_[0-9A-Za-z]{8,40}$")
STREAMS = ("stdout", "stderr", "combined")
MAX_PAGE_BYTES = 1024 * 1024
MAX_JOB_LOG_BYTES = 2 * 1024 * 1024 * 1024
_MAX_OPEN = 64


@dataclass
class LogPage:
    """One page of a log. ``offset``/``next_offset`` are byte offsets; ``eof`` means no more data *right now*."""

    job_id: str
    stream: str
    offset: int
    next_offset: int
    size: int
    data: str
    eof: bool

    def to_dict(self) -> dict:
        return {"job_id": self.job_id, "stream": self.stream, "offset": self.offset, "next_offset": self.next_offset,
                "size": self.size, "data": self.data, "eof": self.eof}


def _trim_utf8_tail(buf: bytes) -> bytes:
    """Drop a trailing incomplete UTF-8 sequence so a page never ends mid-character."""
    n = len(buf)
    for back in range(1, min(4, n) + 1):
        b = buf[n - back]
        if b & 0xC0 == 0x80:  # continuation byte, keep looking for the lead byte
            continue
        need = 1 if b < 0x80 else 2 if b >> 5 == 0b110 else 3 if b >> 4 == 0b1110 else 4 if b >> 3 == 0b11110 else 1
        return buf if back >= need else buf[: n - back]
    return buf


class LogStore:
    """Job logs on disk: appends chunks as they arrive from the agent, keeps sequence numbers, caps the size per job and serves pages and tails without loading a log into memory.
    """
    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._handles: OrderedDict[tuple[str, str], BinaryIO] = OrderedDict()
        self._seq: dict[tuple[str, str], int] = {}
        self._seq_flush: dict[str, float] = {}
        self._written: dict[str, int] = {}
        self._truncated: set[str] = set()

    # ------------------------------------------------------------------ paths
    def job_dir(self, job_id: str) -> Path:
        if not _ID_OK.match(job_id or ""):
            raise BadRequest("invalid job id")
        d = (self.root / job_id).resolve()
        if self.root.resolve() not in d.parents:  # path traversal guard
            raise BadRequest("invalid job id")
        return d

    def _log_path(self, job_id: str, stream: str) -> Path:
        if stream not in STREAMS:
            raise BadRequest(f"unknown log stream '{stream}'", details={"streams": list(STREAMS)})
        return self.job_dir(job_id) / "logs" / f"{stream}.log"

    # ------------------------------------------------------------------ writing
    def _handle(self, job_id: str, stream: str) -> BinaryIO:
        key = (job_id, stream)
        fh = self._handles.get(key)
        if fh is not None and not fh.closed:
            self._handles.move_to_end(key)
            return fh
        p = self._log_path(job_id, stream)
        p.parent.mkdir(parents=True, exist_ok=True)
        fh = open(p, "ab", buffering=0)  # noqa: SIM115 - handle cached and closed by close()/eviction
        self._handles[key] = fh
        while len(self._handles) > _MAX_OPEN:
            _, old = self._handles.popitem(last=False)
            try:
                old.close()
            except OSError:
                pass
        return fh

    def _load_seq(self, job_id: str, stream: str) -> int:
        key = (job_id, stream)
        if key in self._seq:
            return self._seq[key]
        p = self.job_dir(job_id) / "logs" / "seq.json"
        last = -1
        try:
            last = int(json.loads(p.read_text()).get(stream, -1))
        except (OSError, ValueError):
            pass
        self._seq[key] = last
        return last

    def _flush_seq(self, job_id: str, *, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self._seq_flush.get(job_id, 0) < 2.0:
            return
        self._seq_flush[job_id] = now
        data = {s: self._seq[(job_id, s)] for s in ("stdout", "stderr") if (job_id, s) in self._seq}
        p = self.job_dir(job_id) / "logs" / "seq.json"
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            tmp = p.with_suffix(".tmp")
            tmp.write_text(json.dumps(data))
            os.replace(tmp, p)
        except OSError:
            log.warning("could not persist log sequence for %s", job_id)

    def append(self, job_id: str, stream: str, seq: int | None, data: str) -> int:
        """Append a chunk. Returns the number of bytes written (0 for duplicates / dropped chunks)."""
        if stream not in ("stdout", "stderr"):
            raise BadRequest("stream must be stdout or stderr")
        with self._lock:
            if seq is not None:
                if seq <= self._load_seq(job_id, stream):
                    return 0
                self._seq[(job_id, stream)] = seq
            raw = redact_text(data).encode("utf-8", "replace")
            total = self._written.get(job_id)
            if total is None:
                p = self._log_path(job_id, "combined")
                total = p.stat().st_size if p.exists() else 0
            if total >= MAX_JOB_LOG_BYTES:
                if job_id not in self._truncated:
                    self._truncated.add(job_id)
                    note = b"\n[olcf] log size limit reached; further output is dropped\n"
                    self._handle(job_id, "combined").write(note)
                    self._handle(job_id, stream).write(note)
                return 0
            self._handle(job_id, stream).write(raw)
            self._handle(job_id, "combined").write(raw)
            self._written[job_id] = total + len(raw)
            self._flush_seq(job_id)
            return len(raw)

    def mark(self, job_id: str, text: str) -> None:
        """Write an informational line (e.g. ``--- attempt 2 ---``) to every stream."""
        with self._lock:
            raw = (text.rstrip("\n") + "\n").encode()
            for s in STREAMS:
                self._handle(job_id, s).write(raw)
            self._written[job_id] = self._written.get(job_id, 0) + len(raw)

    def close(self, job_id: str) -> None:
        with self._lock:
            for key in [k for k in self._handles if k[0] == job_id]:
                try:
                    self._handles.pop(key).close()
                except OSError:
                    pass
            if any(k[0] == job_id for k in self._seq):
                self._flush_seq(job_id, force=True)
            for key in [k for k in self._seq if k[0] == job_id]:
                self._seq.pop(key, None)
            self._written.pop(job_id, None)
            self._truncated.discard(job_id)
            self._seq_flush.pop(job_id, None)

    def close_all(self) -> None:
        with self._lock:
            for job_id in {k[0] for k in list(self._handles)} | {k[0] for k in list(self._seq)}:
                self.close(job_id)

    # ------------------------------------------------------------------ reading
    def size(self, job_id: str, stream: str = "combined") -> int:
        p = self._log_path(job_id, stream)
        try:
            return p.stat().st_size
        except FileNotFoundError:
            return 0

    def exists(self, job_id: str) -> bool:
        return (self.job_dir(job_id) / "logs").is_dir()

    def read(self, job_id: str, offset: int = 0, limit: int = 65536, stream: str = "combined", *, tail: bool = False) -> LogPage:
        """Read at most ``limit`` bytes starting at ``offset`` (or the last ``limit`` bytes with ``tail=True``)."""
        limit = max(1, min(int(limit), MAX_PAGE_BYTES))
        p = self._log_path(job_id, stream)
        try:
            size = p.stat().st_size  # writers use unbuffered handles, so appended bytes are immediately visible here
        except FileNotFoundError:
            return LogPage(job_id, stream, 0, 0, 0, "", True)
        offset = max(0, int(offset))
        if tail:
            offset = max(0, size - limit)
        if offset > size:
            raise BadRequest("offset is beyond the end of the log", details={"size": size})
        with open(p, "rb") as fh:
            fh.seek(offset)
            buf = fh.read(limit)
        skipped = 0
        while buf and (buf[0] & 0xC0) == 0x80 and skipped < 3:  # client offset landed inside a multi-byte character
            buf = buf[1:]
            skipped += 1
        at_end = offset + skipped + len(buf) >= size
        if not at_end:
            buf = _trim_utf8_tail(buf)
        nxt = offset + skipped + len(buf)
        return LogPage(job_id, stream, offset, nxt, size, buf.decode("utf-8", "replace"), at_end and nxt >= size)

    def iter_bytes(self, job_id: str, stream: str = "combined", start: int = 0, chunk: int = 65536):
        """Generator for streaming downloads (constant memory)."""
        p = self._log_path(job_id, stream)
        if not p.exists():
            return
        with open(p, "rb") as fh:
            fh.seek(max(0, start))
            while True:
                b = fh.read(chunk)
                if not b:
                    return
                yield b

    # ------------------------------------------------------------------ retention
    def delete(self, job_id: str) -> None:
        self.close(job_id)
        d = self.job_dir(job_id)
        if d.is_dir():
            shutil.rmtree(d, ignore_errors=True)

    def purge_older_than(self, cutoff_epoch: float, *, protect: set[str] | None = None) -> int:
        """Remove job directories untouched since ``cutoff_epoch`` (never those in ``protect``). Returns count."""
        n = 0
        try:
            entries = list(self.root.iterdir())
        except FileNotFoundError:
            return 0
        for d in entries:
            if not d.is_dir() or not _ID_OK.match(d.name) or (protect and d.name in protect):
                continue
            try:
                if d.stat().st_mtime < cutoff_epoch and max((f.stat().st_mtime for f in d.rglob("*") if f.is_file()), default=0) < cutoff_epoch:
                    self.delete(d.name)
                    n += 1
            except OSError:
                continue
        return n

    def require(self, job_id: str) -> None:
        if not self.exists(job_id):
            raise NotFound("no logs recorded for this job")
