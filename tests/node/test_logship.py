# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Unit tests for :class:`olcf.worker.logship.StreamShipper`: chunking, redaction, events, back-pressure, rewind/dedupe, persistence."""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from olcf.worker.logship import (
    MAX_TEXT_BYTES,
    StreamShipper,
    load_ship_state,
    redact_file,
    sanitise,
    save_ship_state,
)


class Sink:
    """A fake outbound queue; ``accept`` toggles back-pressure."""

    def __init__(self) -> None:
        self.frames: list[dict] = []
        self.accept = True

    def __call__(self, frame: dict) -> bool:
        if not self.accept:
            return False
        self.frames.append(frame)
        return True

    def text(self) -> str:
        return "".join(f["data"] for f in self.frames)


def make(tmp_path: Path, **kw) -> tuple[StreamShipper, Path]:
    p = tmp_path / "stdout.log"
    p.touch()
    return StreamShipper("job1", 1, "stdout", p, **kw), p


def drain(sh: StreamShipper, sink: Sink, *, finished: bool = True, connected: bool = True, now: float | None = None) -> int:
    n = 0
    while sh.pump(sink, connected=connected, finished=finished, now=now):
        n += 1
        assert n < 10_000
    return n


def test_chunks_are_line_aligned_and_bounded(tmp_path):
    sh, p = make(tmp_path)
    sink = Sink()
    sh.on_reconnect(1)
    line = "x" * 99 + "\n"
    p.write_text(line * 2000)  # 200 000 bytes -> >3 chunks of <=64 KiB
    drain(sh, sink)
    assert len(sink.frames) >= 4
    for f in sink.frames:
        raw = f["data"].encode()
        assert len(raw) <= 64 * 1024
        assert f["data"].endswith("\n")
        assert f["job_id"] == "job1" and f["attempt"] == 1 and f["stream"] == "stdout"
    assert [f["seq"] for f in sink.frames] == list(range(len(sink.frames)))
    assert sink.text() == line * 2000
    offs = [f["offset"] for f in sink.frames]
    assert offs == sorted(offs) and offs[0] == 0


def test_nothing_sent_when_disconnected(tmp_path):
    sh, p = make(tmp_path)
    sink = Sink()
    p.write_text("hello\n")
    assert drain(sh, sink, connected=False) == 0
    assert sink.frames == []
    sh.on_reconnect(1)
    drain(sh, sink)
    assert sink.text() == "hello\n"


def test_partial_line_waits_then_flushes(tmp_path):
    sh, p = make(tmp_path, partial_line_flush_seconds=2.0)
    sink = Sink()
    sh.on_reconnect(1)
    p.write_text("no newline yet")
    assert not sh.pump(sink, connected=True, finished=False, now=100.0)
    assert not sh.pump(sink, connected=True, finished=False, now=101.0)
    assert sh.pump(sink, connected=True, finished=False, now=102.5)  # waited long enough
    assert sink.text() == "no newline yet"


def test_partial_line_flushes_immediately_when_finished(tmp_path):
    sh, p = make(tmp_path)
    sink = Sink()
    sh.on_reconnect(1)
    p.write_text("a\nb-no-newline")
    drain(sh, sink, finished=True)
    assert sink.text() == "a\nb-no-newline"


def test_secrets_redacted_exact_and_pattern(tmp_path):
    sh, p = make(tmp_path, secret_values=["s3cr3t-value-xyz", "ab"])  # values shorter than 4 chars are ignored
    sink = Sink()
    sh.on_reconnect(1)
    p.write_text("token is s3cr3t-value-xyz ok\nab stays\nAuthorization: Bearer abcdefghijklmnopqrstuvwxyz0123456789\nclean\n")
    drain(sh, sink)
    out = sink.text()
    assert "s3cr3t-value-xyz" not in out and "abcdefghijklmnopqrstuvwxyz0123456789" not in out
    assert "ab stays" in out and "clean" in out


def test_redaction_expansion_never_exceeds_frame_limit(tmp_path):
    # many short secrets: each 4-byte secret becomes a longer marker, so 64 KiB of raw text can expand beyond the limit
    sh, p = make(tmp_path, secret_values=["ABCD"])
    sink = Sink()
    sh.on_reconnect(1)
    p.write_text(("ABCD " * 12 + "\n") * 1100)  # ~ 70 KiB raw, every 4 bytes replaced by a longer token
    drain(sh, sink)
    assert all(len(f["data"].encode()) <= MAX_TEXT_BYTES for f in sink.frames)
    assert "ABCD" not in sink.text()
    assert sink.text().count("\n") == 1100  # nothing lost to the shrinking


def test_events_extracted_and_fired_after_chunk_accepted(tmp_path):
    events: list[tuple[dict, int]] = []
    sh, p = make(tmp_path, on_event=lambda ev, off: events.append((ev, off)))
    sink = Sink()
    sh.on_reconnect(1)
    first = "line1\n"
    ev = '@@OLCF@@ {"event": "spark_app_started", "application_id": "app-1"}\n'
    p.write_text(first + ev + "line2\n@@OLCF@@ {not json}\n")
    drain(sh, sink)
    assert sink.text() == "line1\nline2\n@@OLCF@@ {not json}\n"  # event line removed, malformed one kept as text
    assert len(events) == 1 and events[0][0]["event"] == "spark_app_started"
    assert events[0][1] == len(first)  # raw offset = dedupe id

    # back-pressure: the event must NOT fire while the chunk is refused, and fires exactly once when accepted
    events.clear()
    (tmp_path / "b").mkdir()
    sh2, p2 = make(tmp_path / "b", on_event=lambda e, o: events.append((e, o)))
    sh2.on_reconnect(1)
    p2.write_text("text\n" + ev)
    blocked = Sink()
    blocked.accept = False
    sh2.pump(blocked, connected=True, finished=True)
    assert events == []
    drain(sh2, Sink())
    assert [off for _, off in events] == [len("text\n")]


def test_event_only_chunk_advances_without_frame(tmp_path):
    events = []
    sh, p = make(tmp_path, on_event=lambda e, o: events.append(e))
    sink = Sink()
    sh.on_reconnect(1)
    p.write_text('@@OLCF@@ {"event": "x"}\n')
    drain(sh, sink)
    assert sink.frames == [] and len(events) == 1 and sh.backlog() == 0


def test_backpressure_retries_same_range(tmp_path):
    sh, p = make(tmp_path)
    sink = Sink()
    sh.on_reconnect(1)
    p.write_text("one\ntwo\n")
    sink.accept = False
    assert not sh.pump(sink, connected=True, finished=True)
    assert sh.raw_pos == 0 and sh.seq == 0
    sink.accept = True
    drain(sh, sink)
    assert sink.text() == "one\ntwo\n" and [f["seq"] for f in sink.frames] == [0]


def test_rewind_on_reconnect_resends_unconfirmed_with_same_seq(tmp_path):
    sh, p = make(tmp_path, chunk_bytes=16, implicit_ack_seconds=60)
    sink = Sink()
    sh.on_reconnect(1)
    p.write_text("".join(f"line-{i:02d}\n" for i in range(10)))  # 8 bytes per line, 2 lines per chunk
    drain(sh, sink)
    first_pass = list(sink.frames)
    assert len(first_pass) == 5
    sh.ack(1)  # controller confirmed seq 0 and 1 only
    sh.on_reconnect(2)  # connection dropped and came back
    assert sh.seq == 2 and sh.raw_pos == first_pass[2]["offset"]
    sink.frames.clear()
    drain(sh, sink)
    assert [f["seq"] for f in sink.frames] == [2, 3, 4]
    assert sink.frames == first_pass[2:]  # deterministic: same seq carries the same bytes
    assert sh.on_reconnect(2) is None  # same generation again: no second rewind
    assert sh.seq == 5


def test_implicit_ack_on_a_stable_connection(tmp_path):
    sh, p = make(tmp_path, implicit_ack_seconds=5.0)
    sink = Sink()
    sh.on_reconnect(1)
    p.write_text("a\n")
    t0 = time.monotonic()  # the shipper stamps sends with the real monotonic clock
    drain(sh, sink, now=t0)
    assert not sh.drained and sh.ring
    assert not sh.pump(sink, connected=True, finished=True, now=t0 + 1.0)
    assert sh.ring  # not yet
    sh.pump(sink, connected=True, finished=True, now=t0 + 6.0)
    assert not sh.ring and sh.drained
    # a reconnect after implicit confirmation does not resend
    sh.on_reconnect(2)
    sink.frames.clear()
    drain(sh, sink)
    assert sink.frames == []


def test_implicit_ack_does_not_count_across_connections(tmp_path):
    sh, p = make(tmp_path, implicit_ack_seconds=5.0)
    sink = Sink()
    sh.on_reconnect(1)
    p.write_text("a\n")
    t0 = time.monotonic()
    drain(sh, sink, now=t0)
    sh.on_reconnect(2)  # the connection died before the implicit window elapsed
    sink.frames.clear()
    drain(sh, sink, now=t0 + 200.0)
    assert sink.text() == "a\n"  # re-sent


def test_explicit_ack_wins_across_connections(tmp_path):
    sh, p = make(tmp_path)
    sink = Sink()
    sh.on_reconnect(1)
    p.write_text("a\n")
    drain(sh, sink)
    sh.ack(0)
    sh.on_reconnect(2)
    sink.frames.clear()
    drain(sh, sink)
    assert sink.frames == []


def test_truncation_after_max_bytes(tmp_path):
    sh, p = make(tmp_path, max_bytes=1000, chunk_bytes=400)
    sink = Sink()
    sh.on_reconnect(1)
    p.write_text(("y" * 99 + "\n") * 100)
    drain(sh, sink)
    text = sink.text()
    assert "log truncated after 1000 bytes" in text
    assert 1000 <= len(text) < 2500
    assert sh.backlog() == 0  # the remainder is dropped (it stays on disk on the worker)
    assert sum("truncated" in f["data"] for f in sink.frames) == 1  # announced once


def test_missing_file_is_not_an_error(tmp_path):
    sh = StreamShipper("j", 1, "stderr", tmp_path / "nope.log")
    assert not sh.pump(Sink(), connected=True, finished=True)
    assert sh.backlog() == 0


def test_invalid_utf8_is_replaced(tmp_path):
    sh, p = make(tmp_path)
    sink = Sink()
    sh.on_reconnect(1)
    p.write_bytes(b"ok \xff\xfe bytes\n")
    drain(sh, sink)
    assert "ok" in sink.text() and "�" in sink.text()


def test_persistence_roundtrip_and_resume(tmp_path):
    sh, p = make(tmp_path, chunk_bytes=16, implicit_ack_seconds=60)
    sink = Sink()
    sh.on_reconnect(1)
    p.write_text("".join(f"line-{i:02d}\n" for i in range(6)))
    drain(sh, sink)
    sh.ack(1)
    sh._settle(0.0, 1)
    state_path = tmp_path / "logship.json"
    save_ship_state(state_path, {"stdout": sh})
    saved = load_ship_state(state_path)["stdout"]
    assert saved["seq"] == 2 and saved["raw"] == 32  # oldest unconfirmed chunk (16 raw bytes per chunk)
    # "agent restart": a new shipper resumes from the persisted confirmed position
    sh2 = StreamShipper("job1", 1, "stdout", p, chunk_bytes=16, start_raw=saved["raw"], start_seq=saved["seq"])
    sink2 = Sink()
    sh2.on_reconnect(1)
    drain(sh2, sink2)
    assert [f["seq"] for f in sink2.frames] == [2]
    assert sink2.text() == "".join(f"line-{i:02d}\n" for i in range(4, 6))
    assert load_ship_state(tmp_path / "missing.json") == {}
    (tmp_path / "bad.json").write_text("{not json")
    assert load_ship_state(tmp_path / "bad.json") == {}
    assert json.loads(state_path.read_text())["stdout"]["shipped_bytes"] > 0


@pytest.mark.parametrize("chunk", [1, 7, 50])
def test_tiny_chunks_still_reassemble_exactly(tmp_path, chunk):
    sh, p = make(tmp_path, chunk_bytes=chunk, partial_line_flush_seconds=0.0)
    sink = Sink()
    sh.on_reconnect(1)
    body = "".join(f"row {i} " + "z" * (i % 13) + "\n" for i in range(40))
    p.write_text(body)
    drain(sh, sink)
    assert sink.text() == body


def test_redact_file_in_place(tmp_path):
    p = tmp_path / "stdout.log"
    body = "".join(f"row {i} token=top-secret-value-1234 end\n" for i in range(5000)) + "tail without newline top-secret-value-1234"
    p.write_text(body)
    p.chmod(0o640)
    assert redact_file(p, ["top-secret-value-1234"], block_bytes=4096) is True
    out = p.read_text()
    assert "top-secret-value-1234" not in out and out.count("\n") == 5000 and out.endswith("tail without newline " + sanitise(b"top-secret-value-1234", ["top-secret-value-1234"]))
    assert (p.stat().st_mode & 0o077) == 0  # never wider than 0600 afterwards
    assert not list(tmp_path.glob("*.redact"))
    assert redact_file(p, ["top-secret-value-1234"]) is False  # nothing left to mask: file untouched
    assert redact_file(tmp_path / "missing.log", ["x"]) is False


def test_sanitise_masks_exact_values_and_patterns():
    out = sanitise(b"a=hunter2hunter2 Authorization: Bearer abcdefghijklmnopqrstuvwxyz0123456789", ["hunter2hunter2"])
    assert "hunter2hunter2" not in out and "abcdefghijklmnopqrstuvwxyz0123456789" not in out
