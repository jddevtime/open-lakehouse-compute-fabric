# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Backup container: roundtrip, tamper, truncation, reordering, wrong key."""

from __future__ import annotations

import io
import os

import pytest

from olcf.core.errors import BadRequest
from olcf.ops.crypto import (
    BackupCorrupt,
    EncryptedReader,
    EncryptedWriter,
    KeySpec,
    check_passphrase,
    read_header,
)

PW = "correct horse battery staple"


class FakeMaster:
    def __init__(self, keys: dict[str, bytes], current: str) -> None:
        self.keys, self.current_id = keys, current


def _enc(data: bytes, ks: KeySpec, frame: int = 4096) -> bytes:
    bio = io.BytesIO()
    w = EncryptedWriter(bio, ks, {"x": 1}, frame=frame)
    w.write(data)
    w.close()
    return bio.getvalue()


def _dec(blob: bytes, ks: KeySpec) -> bytes:
    return EncryptedReader(io.BytesIO(blob), ks).read()


def test_roundtrip_multi_frame():
    data = os.urandom(50_000)
    blob = _enc(data, KeySpec(PW))
    assert _dec(blob, KeySpec(PW)) == data
    assert data[:100] not in blob


def test_empty_payload():
    assert _dec(_enc(b"", KeySpec(PW)), KeySpec(PW)) == b""


def test_wrong_passphrase_and_missing():
    blob = _enc(b"hello", KeySpec(PW))
    with pytest.raises(BackupCorrupt):
        _dec(blob, KeySpec("another passphrase!!"))
    with pytest.raises(BackupCorrupt):
        _dec(blob, KeySpec())


def test_master_mode_requires_same_key():
    m = FakeMaster({"k1": os.urandom(32)}, "k1")
    blob = _enc(b"data" * 1000, KeySpec(master=m))
    assert _dec(blob, KeySpec(master=m)) == b"data" * 1000
    with pytest.raises(BackupCorrupt) as e:
        _dec(blob, KeySpec(master=FakeMaster({"k2": os.urandom(32)}, "k2")))
    assert e.value.remediation


@pytest.mark.parametrize("where", ["header", "body", "last"])
def test_bitflip_detected(where):
    blob = bytearray(_enc(os.urandom(20_000), KeySpec(PW)))
    idx = {"header": 20, "body": len(blob) // 2, "last": len(blob) - 3}[where]
    blob[idx] ^= 1
    with pytest.raises(BackupCorrupt):
        _dec(bytes(blob), KeySpec(PW))


def test_truncation_at_frame_boundary_detected():
    blob = _enc(os.urandom(20_000), KeySpec(PW), frame=4096)
    # drop the final frame entirely: the previous frame is not flagged final
    import struct

    pos = len(b"OLCFBAK1") + 4 + struct.unpack(">I", blob[8:12])[0]
    frames = []
    while pos < len(blob):
        ln = struct.unpack(">I", blob[pos + 1: pos + 5])[0]
        frames.append((pos, pos + 5 + ln))
        pos += 5 + ln
    assert len(frames) >= 3
    with pytest.raises(BackupCorrupt):
        _dec(blob[: frames[-1][0]], KeySpec(PW))
    # reorder two frames
    swapped = blob[: frames[0][0]] + blob[frames[1][0]: frames[1][1]] + blob[frames[0][0]: frames[0][1]] + blob[frames[2][0]:]
    with pytest.raises(BackupCorrupt):
        _dec(swapped, KeySpec(PW))
    # trailing garbage
    with pytest.raises(BackupCorrupt):
        _dec(blob + b"x", KeySpec(PW))


def test_weak_passphrase_rejected_and_bad_magic():
    with pytest.raises(BadRequest):
        check_passphrase("short")
    with pytest.raises(BackupCorrupt):
        EncryptedReader(io.BytesIO(b"NOTABACKUP" * 4), KeySpec(PW))


def test_header_readable_without_key(tmp_path):
    p = tmp_path / "b.olcfbak"
    p.write_bytes(_enc(b"x", KeySpec(PW)))
    h = read_header(p)
    assert h["kdf"] == "scrypt" and h["x"] == 1 and "salt" in h
