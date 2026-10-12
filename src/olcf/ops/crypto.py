# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Streaming authenticated encryption for backup files (spec 30: "scheduled *encrypted* backups").

File layout::

    "OLCFBAK1" | u32 header_len | header JSON | frame*
    frame = u8 final_flag | u32 ciphertext_len | AES-256-GCM(ciphertext || tag)

* One AES-256-GCM key per file. Each frame uses nonce ``prefix(4 random bytes) || u64 counter`` and the AAD
  ``sha256(header bytes) || u64 counter || u8 final_flag``. Consequently a flipped bit anywhere, a re-ordered / dropped / duplicated frame, a
  truncated file (the last frame must carry ``final_flag=1``) and a swapped header all fail authentication.
* The data is processed in 1 MiB frames, so backups of arbitrary size are written and verified in constant memory.
* **Key derivation**: ``passphrase`` mode uses scrypt (N=2^15, r=8, p=1, 16-byte random salt) - memory-hard, an operator-held secret that makes
  the backup restorable on any machine; ``master`` mode derives the key with HKDF-SHA256 from the platform master key (header records only
  its ``key_id``), so unattended scheduled backups are encrypted too, and restoring them needs the same master key (documented DR prerequisite).
"""

from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import struct
from collections.abc import Callable
from typing import Any, BinaryIO

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from olcf.core.errors import BadRequest, Unavailable

MAGIC = b"OLCFBAK1"
FORMAT = "olcf-backup/1"
FRAME = 1024 * 1024
SCRYPT_N, SCRYPT_R, SCRYPT_P = 2**15, 8, 1
MIN_PASSPHRASE = 12


class BackupCorrupt(BadRequest):
    """The file is not a valid OLCF backup, was modified, truncated, or the key/passphrase is wrong."""

    code = "backup_corrupt"


def _b64(b: bytes) -> str:
    return base64.b64encode(b).decode()


def _unb64(s: str) -> bytes:
    return base64.b64decode(s)


def check_passphrase(passphrase: str) -> str:
    if not isinstance(passphrase, str) or len(passphrase) < MIN_PASSPHRASE:
        raise BadRequest(f"the backup passphrase must be at least {MIN_PASSPHRASE} characters")
    return passphrase


def derive_passphrase_key(passphrase: str, salt: bytes, n: int = SCRYPT_N, r: int = SCRYPT_R, p: int = SCRYPT_P) -> bytes:
    return hashlib.scrypt(passphrase.encode(), salt=salt, n=n, r=r, p=p, maxmem=256 * 1024 * 1024, dklen=32)


def derive_master_key(master_key: bytes, salt: bytes) -> bytes:
    return HKDF(algorithm=hashes.SHA256(), length=32, salt=salt, info=b"olcf-backup-v1").derive(master_key)


class KeySpec:
    """How a backup key is obtained: a passphrase and/or the platform master key (``MasterKey``-like with ``current_id`` and ``keys``)."""

    def __init__(self, passphrase: str | None = None, master: Any = None) -> None:
        self.passphrase, self.master = passphrase, master

    def _keys(self) -> dict[str, bytes]:
        """Keys a backup may be encrypted with: the root keys (plus any other key of a master-like object that has no root/ring split)."""
        m = self.master
        return {**getattr(m, "keys", {}), **getattr(m, "root_keys", {})}

    def for_writing(self) -> tuple[dict[str, Any], bytes]:
        salt = os.urandom(16)
        if self.passphrase:
            check_passphrase(self.passphrase)
            hdr = {"kdf": "scrypt", "n": SCRYPT_N, "r": SCRYPT_R, "p": SCRYPT_P, "salt": _b64(salt)}
            return hdr, derive_passphrase_key(self.passphrase, salt)
        if self.master is None:
            raise Unavailable("no backup passphrase was given and no master key is available")
        # the *root* master key (OLCF_MASTER_KEY / master.key), never a key of the database key ring: those live inside the backup itself, so a restore
        # on a clean machine could not reach them before it had decrypted the backup
        kid = getattr(self.master, "backup_id", None) or self.master.current_id
        return {"kdf": "hkdf-master", "salt": _b64(salt), "key_id": kid}, derive_master_key(self._keys()[kid], salt)

    def for_reading(self, hdr: dict[str, Any]) -> bytes:
        kdf = hdr.get("kdf")
        salt = _unb64(hdr["salt"])
        if kdf == "scrypt":
            if not self.passphrase:
                raise BackupCorrupt("this backup is encrypted with an operator passphrase; supply it", details={"encryption": "passphrase"})
            n, r, p = int(hdr["n"]), int(hdr["r"]), int(hdr["p"])
            if n > 2**20 or r > 16 or p > 4 or n < 2**10:
                raise BackupCorrupt("unreasonable scrypt parameters in the backup header")
            return derive_passphrase_key(self.passphrase, salt, n, r, p)
        if kdf == "hkdf-master":
            kid = hdr.get("key_id")
            if self.master is None or kid not in self._keys():
                raise BackupCorrupt(f"this backup was encrypted with platform master key {kid}, which is not available here",
                                    remediation="restore the master key file (<data_dir>/secrets/master.key) from your key backup first - see the DR runbook",
                                    details={"encryption": "master", "key_id": kid})
            return derive_master_key(self._keys()[kid], salt)
        raise BackupCorrupt(f"unknown key derivation {kdf!r}")


class EncryptedWriter(io.RawIOBase):
    """File-like object: plaintext in, framed AES-GCM out. Close it to write the final frame."""

    def __init__(self, fh: BinaryIO, keyspec: KeySpec, header_extra: dict[str, Any] | None = None, *, frame: int = FRAME) -> None:
        super().__init__()
        self.fh, self.frame = fh, frame
        kdf_hdr, key = keyspec.for_writing()
        self.prefix = os.urandom(4)
        header = {"format": FORMAT, "enc": "aes-256-gcm", "frame": frame, "nonce_prefix": _b64(self.prefix), **kdf_hdr, **(header_extra or {})}
        hb = json.dumps(header, sort_keys=True, separators=(",", ":")).encode()
        self.header = header
        self.aad_root = hashlib.sha256(hb).digest()
        self.aes = AESGCM(key)
        fh.write(MAGIC + struct.pack(">I", len(hb)) + hb)
        self.buf = bytearray()
        self.counter = 0
        self._closed = False
        self._discard = False
        self.bytes_in = 0

    def writable(self) -> bool:
        return True

    def discard(self) -> None:
        """Abandon the stream (error path): later writes are dropped and ``close`` emits no frame, so the file is never a valid backup."""
        self._discard = True
        self.buf.clear()

    def write(self, data: bytes) -> int:  # type: ignore[override]
        if self._discard:
            return len(data)
        if self._closed:
            raise ValueError("write to closed EncryptedWriter")
        self.buf.extend(data)
        self.bytes_in += len(data)
        while len(self.buf) >= self.frame:
            self._emit(bytes(self.buf[: self.frame]), final=False)
            del self.buf[: self.frame]
        return len(data)

    def _emit(self, chunk: bytes, *, final: bool) -> None:
        nonce = self.prefix + struct.pack(">Q", self.counter)
        aad = self.aad_root + struct.pack(">QB", self.counter, 1 if final else 0)
        ct = self.aes.encrypt(nonce, chunk, aad)
        self.fh.write(struct.pack(">BI", 1 if final else 0, len(ct)) + ct)
        self.counter += 1

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            if not self._discard:
                self._emit(bytes(self.buf), final=True)
            self.buf.clear()
            self.fh.flush()
        super().close()


class EncryptedReader(io.RawIOBase):
    """File-like object over an encrypted backup. Raises :class:`BackupCorrupt` on any authentication failure."""

    def __init__(self, fh: BinaryIO, keyspec: KeySpec) -> None:
        super().__init__()
        self.fh = fh
        if fh.read(len(MAGIC)) != MAGIC:
            raise BackupCorrupt("not an OLCF backup file (bad magic)")
        raw = fh.read(4)
        if len(raw) != 4:
            raise BackupCorrupt("truncated backup header")
        (hl,) = struct.unpack(">I", raw)
        if hl > 64 * 1024:
            raise BackupCorrupt("unreasonable header length")
        hb = fh.read(hl)
        if len(hb) != hl:
            raise BackupCorrupt("truncated backup header")
        try:
            self.header = json.loads(hb)
        except ValueError as exc:
            raise BackupCorrupt("backup header is not valid JSON") from exc
        if self.header.get("format") != FORMAT:
            raise BackupCorrupt(f"unsupported backup format {self.header.get('format')!r}")
        self.aad_root = hashlib.sha256(hb).digest()
        self.prefix = _unb64(self.header["nonce_prefix"])
        self.aes = AESGCM(keyspec.for_reading(self.header))
        self.counter = 0
        self.buf = b""
        self.pos = 0
        self.done = False
        self.bytes_out = 0

    def readable(self) -> bool:
        return True

    def _next_frame(self) -> bool:
        if self.done:
            return False
        head = self.fh.read(5)
        if len(head) != 5:
            raise BackupCorrupt("backup is truncated (missing final frame)")
        flag, ln = struct.unpack(">BI", head)
        if ln > 2 * FRAME + 64 or flag not in (0, 1):
            raise BackupCorrupt("invalid frame header")
        ct = self.fh.read(ln)
        if len(ct) != ln:
            raise BackupCorrupt("backup is truncated")
        try:
            pt = self.aes.decrypt(self.prefix + struct.pack(">Q", self.counter), ct, self.aad_root + struct.pack(">QB", self.counter, flag))
        except InvalidTag as exc:
            raise BackupCorrupt("authentication failed: wrong passphrase/key, or the backup was modified") from exc
        self.counter += 1
        self.buf, self.pos = pt, 0
        if flag == 1:
            self.done = True
            if self.fh.read(1):
                raise BackupCorrupt("unexpected data after the final frame")
        return True

    def readinto(self, b: Any) -> int:
        n = len(b)
        while self.pos >= len(self.buf):
            if not self._next_frame():
                return 0
        take = min(n, len(self.buf) - self.pos)
        b[:take] = self.buf[self.pos: self.pos + take]
        self.pos += take
        self.bytes_out += take
        return take


def read_header(path: str | os.PathLike[str]) -> dict[str, Any]:
    """Return the (public, authenticated-on-read) header of a backup without needing a key."""
    with open(path, "rb") as fh:
        if fh.read(len(MAGIC)) != MAGIC:
            raise BackupCorrupt("not an OLCF backup file (bad magic)")
        (hl,) = struct.unpack(">I", fh.read(4))
        return json.loads(fh.read(hl))


def sha256_file(path: str | os.PathLike[str], progress: Callable[[int], None] | None = None) -> str:
    h = hashlib.sha256()
    done = 0
    with open(path, "rb") as fh:
        while chunk := fh.read(1024 * 1024):
            h.update(chunk)
            done += len(chunk)
            if progress:
                progress(done)
    return h.hexdigest()
