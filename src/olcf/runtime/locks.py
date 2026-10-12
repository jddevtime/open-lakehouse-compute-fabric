# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Cross-process file lock and the deterministic olcf wheel builder used by the runtime manager."""

from __future__ import annotations

import base64
import hashlib
import os
import zipfile
from pathlib import Path

import olcf
from olcf.core.filelock import FileLock as _CoreFileLock
from olcf.core.filelock import LockTimeout
from olcf.runtime.errors import InstallFailed
from olcf.version import __version__


class FileLock(_CoreFileLock):
    """Exclusive advisory runtime-install lock (:class:`olcf.core.filelock.FileLock`) that reports a timeout as :class:`InstallFailed`.

    The runtime manager takes it around every mutating operation so two agents (or an agent and the CLI) sharing
    one state directory can never interleave installs.  The platform primitive (``flock`` / ``msvcrt``) lives in the shared helper.
    """

    def __init__(self, path: str | Path, timeout: float = 3600.0) -> None:
        super().__init__(path, timeout)

    def __enter__(self) -> FileLock:
        try:
            self.acquire()
        except LockTimeout:
            raise InstallFailed(f"timed out waiting for runtime lock {self.path}",
                                remediation="Another install/repair is running on this node; wait for it to finish.") from None
        return self


def _b64_nopad(digest: bytes) -> str:
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def build_olcf_wheel(dest_dir: str | Path) -> Path:
    """Build a pure-python wheel of the *running* ``olcf`` package without any build backend.

    The runtime venv must contain the ``olcf.runtime_lib`` modules (and the stdlib-only helpers they import) at the
    same version as the agent. Building the wheel by hand keeps this working from any layout (source checkout,
    wheel install, zipapp) and makes it byte-for-byte reproducible (sorted entries, fixed timestamps). The private
    development signing key and the web UI assets are deliberately excluded. The wheel is installed with
    ``pip install --no-deps --no-index`` (CONTRACTS section 12).
    """
    pkg_root = Path(olcf.__file__).resolve().parent
    files: list[tuple[str, bytes]] = []
    for path in sorted(pkg_root.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(pkg_root).as_posix()
        if "__pycache__" in rel or rel.endswith((".pyc", ".pyo")) or rel.startswith(("web/", "tests/")) or rel.endswith(".key"):
            continue
        files.append((f"olcf/{rel}", path.read_bytes()))
    dist_info = f"olcf-{__version__}.dist-info"
    metadata = (
        "Metadata-Version: 2.1\nName: olcf\n"
        f"Version: {__version__}\n"
        "Summary: Open Lakehouse Compute Fabric (runtime-side slice)\n"
        "Author: Justin Dodrill\nLicense: Apache-2.0\nRequires-Python: >=3.11\n"
    ).encode()
    wheel_meta = b"Wheel-Version: 1.0\nGenerator: olcf-runtime-manager\nRoot-Is-Purelib: true\nTag: py3-none-any\n"
    files.append((f"{dist_info}/METADATA", metadata))
    files.append((f"{dist_info}/WHEEL", wheel_meta))
    record_lines = [f"{name},sha256={_b64_nopad(hashlib.sha256(data).digest())},{len(data)}" for name, data in files]
    record_lines.append(f"{dist_info}/RECORD,,")
    files.append((f"{dist_info}/RECORD", ("\n".join(record_lines) + "\n").encode()))
    out_dir = Path(dest_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"olcf-{__version__}-py3-none-any.whl"
    tmp = out.with_suffix(".tmp")
    with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for name, data in files:
            zi = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            zi.external_attr = 0o644 << 16
            zi.compress_type = zipfile.ZIP_DEFLATED
            zf.writestr(zi, data)
    os.replace(tmp, out)
    return out
