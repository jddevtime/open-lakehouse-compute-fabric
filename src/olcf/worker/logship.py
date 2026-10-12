# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Log streaming from on-disk spool files to the controller (CONTRACTS section 4.2 ``job_log``).

Design goals (spec sections 28, 33): logs survive a controller disconnect, are re-sent *in order* after reconnect, and are
never held whole in RAM.

* The job process writes ``stdout.log`` / ``stderr.log`` **directly** (file descriptors, no pipe through the agent), so a log
  never blocks the job, survives an agent restart and *is* the on-disk spool.
* A :class:`StreamShipper` tails one file: it reads at most ``chunk_bytes`` (64 KiB) at a time, cuts at line boundaries, removes
  ``@@OLCF@@`` event lines (forwarded as ``job_event``), redacts secrets (pattern based + the job's exact secret values), assigns
  consecutive ``seq`` numbers and hands frames to the agent only while the connection is up *and* the outbound queue is short
  (back-pressure keeps memory bounded).
* Delivery tracking: a small ring remembers sent-but-unconfirmed chunks (``seq``, raw byte range). A chunk is confirmed by an
  explicit controller ``log_ack`` or, without acks, once the same connection stayed up ``implicit_ack_seconds`` after sending.
  On reconnect the shipper **rewinds to the oldest unconfirmed chunk** and re-sends; consumers dedupe on ``(job_id, stream, seq)``
  (chunks are deterministic, a given ``seq`` always carries the same bytes).
* Confirmed positions are persisted (``logship.json`` in the job dir) so a restarted agent resumes where it left off.
"""

from __future__ import annotations

import json
import os
import re
import time
from collections import deque
from collections.abc import Callable
from pathlib import Path
from typing import Any

from olcf.core.redaction import REDACTED, redact_text

MAX_TEXT_BYTES = 120 * 1024  # controller limit is 128 KiB per job_log chunk
EVENT_PREFIX = b"@@OLCF@@"
_EVENT_LINE = re.compile(rb"^@@OLCF@@ (\{.*\})\s*$")


class StreamShipper:
    """Tails one log file. Not thread-safe: use from the job's single shipping thread (``ack`` may be called from others
    through :meth:`ack`, which only touches ``acked_seq``)."""

    def __init__(self, job_id: str, attempt: int, stream: str, path: Path, *, chunk_bytes: int = 64 * 1024, start_raw: int = 0, start_seq: int = 0,
                 secret_values: list[str] | None = None, max_bytes: int = 50 * 1024 * 1024, implicit_ack_seconds: float = 5.0,
                 on_event: Callable[[dict[str, Any], int], None] | None = None, partial_line_flush_seconds: float = 2.0) -> None:
        self.job_id, self.attempt, self.stream, self.path = job_id, attempt, stream, Path(path)
        self.chunk_bytes = chunk_bytes
        self.raw_pos = start_raw
        self.seq = start_seq
        self.secrets = sorted({s for s in (secret_values or []) if len(s) >= 4}, key=len, reverse=True)
        self.max_bytes = max_bytes
        self.implicit_ack_seconds = implicit_ack_seconds
        self.on_event = on_event
        self.partial_flush = partial_line_flush_seconds
        self.ring: deque[tuple[int, int, int, float, int]] = deque()  # (seq, raw_start, raw_end, sent_at, conn_gen)
        self.shipped_bytes = 0
        self.truncated_notice_sent = False
        self._partial_since: float | None = None
        self.conn_gen = -1
        self.acked_seq = -1

    # ---- state exchange
    def confirmed_position(self) -> tuple[int, int]:
        """``(raw_offset, seq)`` up to which everything is confirmed (what a restarted agent resumes from)."""
        if self.ring:
            seq, raw_start, *_ = self.ring[0]
            return raw_start, seq
        return self.raw_pos, self.seq

    def ack(self, seq: int) -> None:
        self.acked_seq = max(self.acked_seq, seq)

    def _settle(self, now: float, conn_gen: int) -> None:
        while self.ring:
            seq, _, _, sent_at, gen = self.ring[0]
            if seq <= self.acked_seq or (gen == conn_gen and now - sent_at >= self.implicit_ack_seconds):
                self.ring.popleft()
            else:
                break

    def on_reconnect(self, conn_gen: int) -> None:
        """New connection: rewind to the oldest unconfirmed chunk so nothing is lost; duplicates are deduped by seq."""
        if conn_gen == self.conn_gen:
            return
        self.conn_gen = conn_gen
        self._settle(time.monotonic(), -2)  # only explicit acks count across connections
        if self.ring:
            seq, raw_start, *_ = self.ring[0]
            self.raw_pos, self.seq = raw_start, seq
            self.ring.clear()

    def backlog(self) -> int:
        """Bytes on disk not yet read (for diagnostics)."""
        try:
            return max(0, self.path.stat().st_size - self.raw_pos)
        except OSError:
            return 0

    @property
    def drained(self) -> bool:
        return not self.ring and self.backlog() == 0

    # ---- pumping
    def _sanitise(self, data: bytes) -> str:
        return sanitise(data, self.secrets)

    def pump(self, send: Callable[[dict[str, Any]], bool], *, connected: bool, finished: bool, now: float | None = None) -> bool:
        """Read and send at most one chunk. ``send`` returns False to signal back-pressure (stop for now). Returns True if progress was made."""
        now = time.monotonic() if now is None else now
        self._settle(now, self.conn_gen)
        if not connected:
            return False
        try:
            size = self.path.stat().st_size
        except OSError:
            return False
        if self.shipped_bytes >= self.max_bytes:
            if not self.truncated_notice_sent:
                self.truncated_notice_sent = True
                msg = f"\n[olcf: {self.stream} log truncated after {self.max_bytes} bytes; the full log remains in the job directory on the worker]\n"
                self._send_chunk(send, msg, self.raw_pos, self.raw_pos)
            self.raw_pos = size  # drop the rest
            return False
        if size <= self.raw_pos:
            self._partial_since = None
            return False
        with open(self.path, "rb") as fh:
            fh.seek(self.raw_pos)
            data = fh.read(self.chunk_bytes)
        if not data:
            return False
        end = data.rfind(b"\n")
        if end == -1:
            if len(data) < self.chunk_bytes and not finished:
                # an unterminated line: give the writer a moment to finish it
                if self._partial_since is None:
                    self._partial_since = now
                if now - self._partial_since < self.partial_flush:
                    return False
            take = len(data)
        else:
            take = end + 1
        self._partial_since = None
        # the controller refuses frames whose text exceeds 2 x 64 KiB; redaction can lengthen text, so shrink the raw range until it fits
        while True:
            piece = data[:take]
            text, events = self._render(piece)
            if len(text.encode("utf-8", "replace")) <= MAX_TEXT_BYTES or take <= 1024:
                break
            half = take // 2
            cut = data.rfind(b"\n", 0, half)
            take = cut + 1 if cut > 0 else half
        raw_start, raw_end = self.raw_pos, self.raw_pos + take
        if text:
            if not self._send_chunk(send, text, raw_start, raw_end):
                return False  # back-pressure: re-read this range later
            self.shipped_bytes += len(text.encode("utf-8", "replace"))
        else:
            self.raw_pos = raw_end
        for ev, off in events:  # only after the chunk was accepted: the raw offset makes re-delivery after a rewind idempotent
            if self.on_event:
                self.on_event(ev, off)
        return True

    def _render(self, piece: bytes) -> tuple[str, list[tuple[dict[str, Any], int]]]:
        """Split ``piece`` into log text (sanitised) and parsed ``@@OLCF@@`` events with their raw offsets."""
        lines_out: list[bytes] = []
        events: list[tuple[dict[str, Any], int]] = []
        cum = 0
        for line in piece.splitlines(keepends=True):
            m = _EVENT_LINE.match(line) if line.startswith(EVENT_PREFIX) else None
            if m:
                try:
                    events.append((json.loads(m.group(1)), self.raw_pos + cum))
                except ValueError:
                    lines_out.append(line)
            else:
                lines_out.append(line)
            cum += len(line)
        return (self._sanitise(b"".join(lines_out)) if lines_out else ""), events

    def _send_chunk(self, send: Callable[[dict[str, Any]], bool], text: str, raw_start: int, raw_end: int) -> bool:
        frame = {"job_id": self.job_id, "attempt": self.attempt, "stream": self.stream, "seq": self.seq, "offset": raw_start, "data": text}
        if not send(frame):
            return False
        self.ring.append((self.seq, raw_start, raw_end, time.monotonic(), self.conn_gen))
        self.seq += 1
        self.raw_pos = raw_end
        return True


def sanitise(data: bytes, secrets: list[str]) -> str:
    """Decode ``data`` and mask the job's exact secret values plus anything that looks like a credential."""
    text = data.decode("utf-8", "replace")
    for sec in secrets:
        if sec in text:
            text = text.replace(sec, REDACTED)
    return redact_text(text)


def redact_file(path: Path, secrets: list[str], *, block_bytes: int = 1024 * 1024) -> bool:
    """Rewrite a log spool file in place with secrets masked (streaming, bounded memory, atomic replace, mode preserved).

    Called once a job's output is fully delivered: the raw spool is no longer needed for re-sending, and a job that printed one of
    its own secrets must not leave it on the node's disk until the retention window ends. Returns True if the file was rewritten.
    """
    try:
        mode = path.stat().st_mode & 0o777
        tmp = path.with_name(path.name + ".redact")
        changed = False
        with open(path, "rb") as src, open(tmp, "wb") as dst:
            os.chmod(tmp, mode & 0o600 or 0o600)
            pending = b""
            while True:
                block = src.read(block_bytes)
                data = pending + block
                if not block:
                    pending = b""
                else:
                    cut = data.rfind(b"\n")
                    if cut == -1:
                        pending = data
                        continue
                    data, pending = data[: cut + 1], data[cut + 1:]
                if data:
                    clean = sanitise(data, secrets).encode("utf-8")
                    changed = changed or clean != data
                    dst.write(clean)
                if not block:
                    break
        if changed:
            os.replace(tmp, path)
        else:
            tmp.unlink()
        return changed
    except OSError:
        return False


def save_ship_state(path: Path, shippers: dict[str, StreamShipper]) -> None:
    doc = {name: {"raw": s.confirmed_position()[0], "seq": s.confirmed_position()[1], "shipped_bytes": s.shipped_bytes} for name, s in shippers.items()}
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(doc), encoding="utf-8")
    os.replace(tmp, path)


def load_ship_state(path: Path) -> dict[str, dict[str, int]]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
