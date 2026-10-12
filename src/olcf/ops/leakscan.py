# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Secret-leak scanner for generated artifacts (support bundles; also used by the security test-suite on logs and API output).

Two independent detectors, because redaction is pattern based and patterns miss things:

1. **Live values**: every secret currently in the secret store (decrypted), the platform master key, the JWT signing keys, the CA/server private
   keys, the bootstrap admin password, the stored backup passphrase and the database password are collected as *canaries*. A canary found
   anywhere (also base64-, hex- or URL-encoded) is a leak, whatever it looks like.
2. **Patterns**: JWTs, platform tokens (``olcf_pat_`` / ``olcf_enr_`` / ``olcf_wrk_`` / ``olcf_hook_`` followed by key material), PEM private keys, AWS key ids/secrets, ``Bearer <token>``, unredacted
   ``password=/secret=/api_key=`` assignments and credentials in URLs.

Archives (zip / gzip / tar, nested up to depth 3) are scanned member by member in a streaming fashion with an overlap window, so a canary that
straddles a chunk boundary is still found. Findings never contain the secret itself - only the file, the kind and, for canaries, the secret id/name.
"""

from __future__ import annotations

import base64
import gzip
import io
import json
import logging
import re
import tarfile
import urllib.parse
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, BinaryIO

from sqlalchemy import select

from olcf.core.context import AppContext
from olcf.core.errors import OlcfError

log = logging.getLogger("olcf.ops.leakscan")

CHUNK = 1024 * 1024
MIN_CANARY = 6
MAX_DEPTH = 3


class LeakDetected(OlcfError):
    """A secret was found where none may appear (HTTP 500, code ``secret_leak_detected``)."""
    status = 500
    code = "secret_leak_detected"


PATTERNS: dict[str, re.Pattern[bytes]] = {
    "jwt": re.compile(rb"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{4,}"),
    "platform-token": re.compile(rb"\bolcf_(?:pat|enr|wrk|hook)_[A-Za-z0-9_-]{16,}"),
    "private-key": re.compile(rb"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    "aws-key-id": re.compile(rb"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"),
    "aws-secret": re.compile(rb"(?i)aws_secret_access_key[\"']?\s*[=:]\s*[\"']?[A-Za-z0-9/+=]{30,}"),
    "bearer": re.compile(rb"(?i)\bbearer\s+(?!\*\*\*REDACTED)[A-Za-z0-9._~+/=-]{20,}"),
    "url-credentials": re.compile(rb"://[^/\s:@\"']{1,64}:(?!\*\*\*REDACTED)[^@\s/\"']{3,}@"),
    "secret-assignment": re.compile(rb"(?i)\b(?:password|passwd|client_secret|api[_-]?key|secret[_-]?key|access[_-]?key|private[_-]?key)[\"']?\s*[=:]\s*[\"']?(?!\*\*\*REDACTED)[A-Za-z0-9/+=_@#%!$^&*.-]{8,}"),
}
#: values that look like a pattern hit but are documentation / placeholders produced by the platform itself
ALLOWED = (b"example", b"<token>", b"<password>", b"<secret>", b"changeme", b"xxxxxxxx", b"********")


@dataclass
class Finding:
    """One leak found by the scanner: file, kind, detail and offset. Secret values are never recorded."""
    file: str
    kind: str
    detail: str = ""
    offset: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {"file": self.file, "kind": self.kind, "detail": self.detail, "offset": self.offset}


@dataclass
class ScanResult:
    """Result of a leak scan: whether it is clean, the findings and how many files and bytes were scanned."""
    ok: bool
    findings: list[Finding] = field(default_factory=list)
    files_scanned: int = 0
    bytes_scanned: int = 0
    canaries: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {"ok": self.ok, "files_scanned": self.files_scanned, "bytes_scanned": self.bytes_scanned, "canaries": self.canaries, "findings": [f.to_dict() for f in self.findings[:50]],
                "finding_count": len(self.findings)}


def _variants(value: bytes) -> list[bytes]:
    out = {value, base64.b64encode(value), base64.urlsafe_b64encode(value), base64.b64encode(value).rstrip(b"="), value.hex().encode(), urllib.parse.quote(value.decode("utf-8", "ignore")).encode()}
    return [v for v in out if len(v) >= MIN_CANARY]


class LeakScanner:
    """Scans files and archives for known secret values (canaries) and for credential-shaped patterns. Findings name the secret, never its value.
    """
    def __init__(self, canaries: dict[str, bytes] | None = None, *, patterns: bool = True) -> None:
        """``canaries``: ``{label: secret bytes}``. Labels (secret ids / file names) are reported, values never."""
        self.patterns = patterns
        self._bytes = 0
        self._files = 0
        self._labels: dict[bytes, str] = {}
        for label, val in (canaries or {}).items():
            if len(val) < MIN_CANARY:
                continue
            for v in _variants(val):
                self._labels.setdefault(v, label)
        self._rx = re.compile(b"|".join(re.escape(k) for k in sorted(self._labels, key=len, reverse=True))) if self._labels else None
        self.window = max([len(k) for k in self._labels] + [256]) + 64

    # ------------------------------------------------------------------ canary collection
    @classmethod
    def from_context(cls, ctx: AppContext, extra: dict[str, str] | None = None) -> LeakScanner:
        return cls(collect_canaries(ctx, extra))

    def scrub_text(self, text: str) -> str:
        """Replace every live-secret canary (and encoded variant) in ``text`` by ``***REDACTED***``; used where free text from logs is quoted."""
        if self._rx is None:
            return text
        return self._rx.sub(b"***REDACTED***", text.encode("utf-8", "replace")).decode("utf-8", "replace")

    # ------------------------------------------------------------------ scanning primitives
    def scan_bytes(self, name: str, data: bytes) -> list[Finding]:
        self._bytes = self._files = 0
        return self._scan_stream(name, io.BytesIO(data))

    def _scan_stream(self, name: str, fh: BinaryIO) -> list[Finding]:
        findings: list[Finding] = []
        tail = b""
        base = 0
        while True:
            chunk = fh.read(CHUNK)
            if not chunk:
                break
            buf = tail + chunk
            findings.extend(self._scan_buf(name, buf, base - len(tail), final_skip=len(tail)))
            keep = min(len(buf), self.window)
            tail = buf[-keep:]
            base += len(chunk)
        self._bytes += base
        return findings

    def _scan_buf(self, name: str, buf: bytes, base: int, final_skip: int) -> list[Finding]:
        out: list[Finding] = []
        if self._rx is not None:
            for m in self._rx.finditer(buf):
                if m.end() <= final_skip:  # entirely inside the overlap that was already scanned
                    continue
                out.append(Finding(name, "live-secret", f"value of {self._labels[m.group(0)]}", base + m.start()))
        if self.patterns:
            for kind, rx in PATTERNS.items():
                for m in rx.finditer(buf):
                    if m.end() <= final_skip:
                        continue
                    if any(a in m.group(0).lower() for a in ALLOWED):
                        continue
                    out.append(Finding(name, kind, "", base + m.start()))
        return out

    def scan_file(self, path: Path, label: str | None = None) -> ScanResult:
        self._bytes = 0
        self._files = 0
        findings = self._scan_path(label or path.name, path, 0)
        return ScanResult(not findings, findings, self._files, self._bytes, len(self._labels))

    def _scan_path(self, name: str, path: Path, depth: int) -> list[Finding]:
        with open(path, "rb") as fh:
            magic = fh.read(4)
            fh.seek(0)
            return self._scan_container(name, fh, magic, depth)

    def _scan_container(self, name: str, fh: BinaryIO, magic: bytes, depth: int) -> list[Finding]:
        out: list[Finding] = []
        if depth < MAX_DEPTH and magic[:2] == b"PK":
            try:
                with zipfile.ZipFile(fh) as zf:
                    for info in zf.infolist():
                        if info.is_dir():
                            continue
                        member = f"{name}!{info.filename}"
                        out.extend(self._scan_member(member, zf.open(info), depth + 1))
                        out.extend(self._scan_text(f"{name}!<filename>", info.filename.encode()))
                    return out
            except zipfile.BadZipFile:
                fh.seek(0)
        if depth < MAX_DEPTH and magic[:2] == b"\x1f\x8b":
            try:
                with gzip.GzipFile(fileobj=fh) as gz:
                    peek = gz.read(512)
                    gz.seek(0)
                    if peek[257:262] == b"ustar":
                        gz.seek(0)
                        with tarfile.open(fileobj=gz, mode="r|") as tf:
                            for ti in tf:
                                if ti.isfile():
                                    f = tf.extractfile(ti)
                                    if f is not None:
                                        out.extend(self._scan_member(f"{name}!{ti.name}", f, depth + 1))
                        return out
                    return self._scan_member(f"{name}!gunzip", gz, depth + 1)
            except (OSError, EOFError, tarfile.TarError):
                fh.seek(0)
        self._files += 1
        return self._scan_stream(name, fh)

    def _scan_member(self, name: str, fh: BinaryIO, depth: int) -> list[Finding]:
        head = fh.read(4)
        data = io.BytesIO(head + fh.read())  # members of bundles are bounded (tails/summaries); nested archives need seekability
        data.seek(0)
        return self._scan_container(name, data, head, depth)

    def _scan_text(self, name: str, text: bytes) -> list[Finding]:
        return self._scan_buf(name, text, 0, 0)


def collect_canaries(ctx: AppContext, extra: dict[str, str] | None = None) -> dict[str, bytes]:
    """Every secret value this installation holds, labelled by id/name only."""
    out: dict[str, bytes] = {}

    def add(label: str, value: Any) -> None:
        if isinstance(value, str) and len(value) >= MIN_CANARY:
            out[label] = value.encode()
        elif isinstance(value, bytes) and len(value) >= MIN_CANARY:
            out[label] = value
        elif isinstance(value, dict):
            for k, v in value.items():
                add(f"{label}.{k}", v)
        elif isinstance(value, list):
            for i, v in enumerate(value):
                add(f"{label}[{i}]", v)

    from olcf.db import schema as S

    with ctx.db.connect() as conn:
        rows = conn.execute(select(S.secrets.c.id, S.secrets.c.name).where(S.secrets.c.deleted_at.is_(None))).all()
    for sid, name in rows:
        try:
            v = ctx.secrets.resolve(sid)
        except Exception as exc:  # noqa: BLE001 - external vault down etc.: cannot be a leak we can detect
            log.debug("secret %s not resolvable for the leak scan: %s", name, type(exc).__name__)
            continue
        if isinstance(v, str):
            try:
                parsed = json.loads(v)
                if isinstance(parsed, dict):
                    add(f"secret:{name}", parsed)
                    continue
            except ValueError:
                pass
        add(f"secret:{name}", v)
    master = getattr(ctx.secrets, "master", None)
    if master is not None:
        for kid, key in master.keys.items():
            add(f"master-key:{kid}", base64.b64encode(key))
            add(f"master-key-raw:{kid}", key)
    signing = getattr(getattr(ctx, "tokens", None), "keys", None)
    for kid, key in (signing.verification_keys() if signing is not None and hasattr(signing, "verification_keys") else []):
        add(f"signing-key:ring:{kid}", key)  # also the keys that only exist in the database ring
    for kid, rk in (getattr(master, "root_keys", {}) or {}).items():
        add(f"master-key:root:{kid}", base64.b64encode(rk))
    for fname in ("jwt-signing.key", "jwt-signing.prev.key"):
        p = ctx.settings.path("secrets", fname)
        if p.is_file():
            add(f"signing-key:{fname}", p.read_bytes().strip())
    for fname in ("ca.key", "server.key"):
        p = ctx.settings.path("pki", fname)
        if p.is_file():
            body = p.read_text()
            add(f"pki:{fname}", "".join(line for line in body.splitlines() if "-----" not in line))
    p = ctx.settings.path("secrets", "bootstrap-admin.txt")
    if p.is_file() and "password=" in p.read_text():
        add("bootstrap-admin-password", p.read_text().split("password=")[1].split()[0])
    try:
        from urllib.parse import urlsplit

        pw = urlsplit(ctx.settings.database_url).password
        if pw:
            add("database-password", pw)
    except Exception:  # noqa: BLE001
        pass
    for k, v in (extra or {}).items():
        add(k, v)
    return out
