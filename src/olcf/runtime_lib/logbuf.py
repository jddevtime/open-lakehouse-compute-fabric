# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Bounded text buffers and output budgets (stdlib only).

Job output can be arbitrarily large (a notebook printing in a loop, ``df.show`` on wide tables). The runner must never
hold unbounded text in RAM nor write an unbounded ``output.ipynb`` / ``result.json``; ``limits.max_output_bytes`` of the
job spec is enforced with these helpers. Truncation is always *visible*: a marker states how many bytes were dropped.
"""

from __future__ import annotations

import threading

TRUNCATION_MARK = "\n[... olcf: {n} bytes of output truncated (limits.max_output_bytes) ...]\n"


class BoundedBuffer:
    """Keep the first ``head`` and last ``tail`` bytes of a text stream, counting what was dropped."""

    def __init__(self, max_bytes: int) -> None:
        self.head_limit = max(1, max_bytes // 2)
        self.tail_limit = max(1, max_bytes - self.head_limit)
        self._head: list[str] = []
        self._head_bytes = 0
        self._tail: list[str] = []
        self._tail_bytes = 0
        self.dropped = 0
        self._lock = threading.Lock()

    def write(self, text: str) -> None:
        if not text:
            return
        with self._lock:
            b = len(text.encode("utf-8", "replace"))
            if self._head_bytes < self.head_limit:
                room = self.head_limit - self._head_bytes
                if b <= room:
                    self._head.append(text)
                    self._head_bytes += b
                    return
                cut = _cut_bytes(text, room)
                self._head.append(cut)
                self._head_bytes += len(cut.encode("utf-8", "replace"))
                text = text[len(cut):]
                b = len(text.encode("utf-8", "replace"))
            self._tail.append(text)
            self._tail_bytes += b
            while self._tail_bytes > self.tail_limit and self._tail:
                first = self._tail[0]
                fb = len(first.encode("utf-8", "replace"))
                over = self._tail_bytes - self.tail_limit
                if fb <= over:
                    self._tail.pop(0)
                    self._tail_bytes -= fb
                    self.dropped += fb
                else:
                    keep = first.encode("utf-8", "replace")[over:].decode("utf-8", "ignore")
                    self.dropped += over
                    self._tail[0] = keep
                    self._tail_bytes -= over
                    break

    @property
    def truncated(self) -> bool:
        return self.dropped > 0

    def getvalue(self) -> str:
        with self._lock:
            mid = TRUNCATION_MARK.format(n=self.dropped) if self.dropped else ""
            return "".join(self._head) + mid + "".join(self._tail)


def _cut_bytes(text: str, nbytes: int) -> str:
    data = text.encode("utf-8", "replace")[:nbytes]
    return data.decode("utf-8", "ignore")


def truncate_text(text: str, max_bytes: int) -> tuple[str, bool]:
    """Return ``(text, truncated)`` where text is limited to ``max_bytes`` (head+tail kept, marker inserted)."""
    if len(text.encode("utf-8", "replace")) <= max_bytes:
        return text, False
    buf = BoundedBuffer(max_bytes)
    buf.write(text)
    return buf.getvalue(), True


class OutputBudget:
    """Shared byte budget for all outputs of one job; ``take(n)`` returns how many bytes may still be used."""

    def __init__(self, max_bytes: int) -> None:
        self.max_bytes = max_bytes
        self.used = 0
        self.truncated = False
        self._lock = threading.Lock()

    def take(self, n: int) -> int:
        with self._lock:
            room = max(0, self.max_bytes - self.used)
            got = min(room, n)
            self.used += got
            if got < n:
                self.truncated = True
            return got

    @property
    def exhausted(self) -> bool:
        return self.used >= self.max_bytes
