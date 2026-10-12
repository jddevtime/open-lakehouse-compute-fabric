# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Builds a real, installable ``olcf`` wheel from the working tree with a different version and optional migration changes (for update tests)."""

from __future__ import annotations

import base64
import hashlib
import re
import zipfile
from pathlib import Path

SRC = Path(__file__).resolve().parents[2] / "src" / "olcf"
_CACHE: dict[str, dict[str, bytes]] = {}


def _tree() -> dict[str, bytes]:
    if "t" not in _CACHE:
        files: dict[str, bytes] = {}
        for p in SRC.rglob("*.py"):
            if "__pycache__" in p.parts:
                continue
            files["olcf/" + p.relative_to(SRC).as_posix()] = p.read_bytes()
        _CACHE["t"] = files
    return _CACHE["t"]


EXTRA_MIGRATION = '''

# --- appended by the update test: a new release adds a column
from olcf.db.migrate import add_column as _add_column  # noqa: E402

register(Migration("ops", 2, "test: retention note", up=lambda c: _add_column(c, "retention_runs", "note TEXT")))
'''

BROKEN_MIGRATION = '''

def _boom(conn):
    raise RuntimeError("migration exploded on purpose")


register(Migration("ops", 2, "test: broken", up=_boom))
'''


def build_olcf_wheel(dest: Path, version: str, *, migration: str | None = None, name_version: str | None = None, code_version: str | None = None) -> Path:
    """``migration``: ``'extra'`` (adds ops v2), ``'broken'`` (ops v2 raises) or ``None``. ``name_version`` lets the METADATA lie about the version."""
    files = dict(_tree())
    files["olcf/version.py"] = re.sub(rb'__version__\s*=\s*"[^"]+"', b'__version__ = "' + (code_version or version).encode() + b'"', files["olcf/version.py"])
    if migration:
        files["olcf/ops/schema.py"] += (EXTRA_MIGRATION if migration == "extra" else BROKEN_MIGRATION).encode()
    mv = name_version or version
    dist = f"olcf-{mv}.dist-info"
    files[f"{dist}/METADATA"] = f"Metadata-Version: 2.1\nName: olcf\nVersion: {mv}\n".encode()
    files[f"{dist}/WHEEL"] = b"Wheel-Version: 1.0\nGenerator: olcf-tests\nRoot-Is-Purelib: true\nTag: py3-none-any\n"
    rec = []
    for k, v in files.items():
        h = base64.urlsafe_b64encode(hashlib.sha256(v).digest()).rstrip(b"=").decode()
        rec.append(f"{k},sha256={h},{len(v)}")
    rec.append(f"{dist}/RECORD,,")
    files[f"{dist}/RECORD"] = ("\n".join(rec) + "\n").encode()
    dest.mkdir(parents=True, exist_ok=True)
    out = dest / f"olcf-{mv}-py3-none-any.whl"
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zf:
        for k, v in files.items():
            zf.writestr(k, v)
    return out
