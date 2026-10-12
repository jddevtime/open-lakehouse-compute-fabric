# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Local filesystem provider (development, single-node installs, NFS/CIFS mounts).

Security model: a lakehouse root is a directory; every operation resolves the *real* path of the target (or of its
deepest existing ancestor for creations) and verifies it stays inside the real root, so ``..`` tricks and symlink
escapes are both refused.  Final path components are opened with ``O_NOFOLLOW``.  Symlinks are never traversed when
listing.  Which roots a lakehouse may use is decided by the lakehouse hook (:mod:`olcf.storage.lakehouse`): only the
platform's local storage root or the root of an administrator-created ``local`` connection.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, BinaryIO

from olcf.core.errors import BadRequest, Conflict, Forbidden, NotFound
from olcf.storage.paths import normalize_rel
from olcf.storage.providers.base import (
    BinaryWriter,
    Capabilities,
    FieldSpec,
    FileInfo,
    FileSystem,
    StorageProvider,
    probe_roundtrip,
    run_steps,
)


def uri_to_path(root_uri: str) -> Path:
    """``file:///a/b`` or ``/a/b`` -> ``Path('/a/b')`` (no percent-decoding by design, see CONTRACTS section 3)."""
    if root_uri.startswith("file://"):
        raw = root_uri[len("file://"):]
        if not raw.startswith("/"):
            raise BadRequest(f"file URI must be absolute: {root_uri}")
        return Path(raw)
    if root_uri.startswith("/"):
        return Path(root_uri)
    raise BadRequest(f"local lakehouse roots must be absolute 'file:///...' URIs, got '{root_uri}'")


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class _LocalWriter(BinaryWriter):
    """Writes to a temp file in the target directory and atomically renames on close."""

    def __init__(self, fs: LocalFileSystem, rel: str, target: Path, overwrite: bool) -> None:
        self._fs, self._rel, self._target, self._overwrite = fs, rel, target, overwrite
        target.parent.mkdir(parents=True, exist_ok=True)
        fs._check_inside(target.parent)
        fd, tmp = tempfile.mkstemp(prefix=".olcf-upload-", dir=target.parent)
        self._tmp = tmp
        self._fh = os.fdopen(fd, "wb")
        self._done = False

    def write(self, data: bytes) -> int:
        return self._fh.write(data)

    def close(self) -> None:
        if self._done:
            return
        self._done = True
        self._fh.close()
        try:
            if not self._overwrite and os.path.lexists(self._target):
                raise Conflict(f"'{self._rel}' already exists")
            if self._target.is_symlink():
                raise Forbidden("refusing to overwrite a symbolic link")
            os.replace(self._tmp, self._target)
        except BaseException:
            Path(self._tmp).unlink(missing_ok=True)
            raise

    def abort(self) -> None:
        if self._done:
            return
        self._done = True
        self._fh.close()
        Path(self._tmp).unlink(missing_ok=True)


class LocalFileSystem(FileSystem):
    """File system over a directory on the host. Paths are checked against traversal and symbolic links."""
    provider = "local"
    capabilities = Capabilities(atomic_rename=True, server_side_copy=False, true_directories=True, delta_commits="rename")

    def __init__(self, root: Path) -> None:
        self.root = root
        self._real_root = Path(os.path.realpath(root))

    # --- safety
    def _check_inside(self, p: Path) -> None:
        real = Path(os.path.realpath(p))
        if real != self._real_root and self._real_root not in real.parents:
            raise Forbidden("path resolves outside the lakehouse root (symbolic link escape refused)")

    def _abs(self, rel: str) -> Path:
        rel = normalize_rel(rel)
        p = self.root / rel if rel else self.root
        # resolve through the deepest existing ancestor so brand-new paths are checked too
        probe = p
        while not os.path.lexists(probe) and probe != self.root:
            probe = probe.parent
        self._check_inside(probe)
        return p

    def local_path(self, path: str) -> str | None:
        return str(self._abs(path))

    def physical_uri(self, path: str = "") -> str:
        rel = normalize_rel(path)
        return "file://" + str(self.root / rel if rel else self.root)

    # --- reads
    def _info(self, rel: str, p: Path, st: os.stat_result | None = None) -> FileInfo:
        st = st or p.stat()
        is_dir = (st.st_mode & 0o170000) == 0o040000
        return FileInfo(path=rel, is_dir=is_dir, size=0 if is_dir else st.st_size, modified=_iso(st.st_mtime),
                        etag=f"{st.st_mtime_ns:x}-{st.st_size:x}")

    def stat(self, path: str) -> FileInfo:
        rel = normalize_rel(path)
        p = self._abs(rel)
        try:
            return self._info(rel, p)
        except (FileNotFoundError, NotADirectoryError):
            raise NotFound(f"'{rel}' does not exist") from None

    def list(self, path: str = "", recursive: bool = False) -> Iterator[FileInfo]:
        rel = normalize_rel(path)
        base = self._abs(rel)
        if not base.is_dir():
            return
        stack = [(rel, base)]
        while stack:
            cur_rel, cur = stack.pop()
            try:
                entries = sorted(os.scandir(cur), key=lambda e: e.name)
            except (FileNotFoundError, NotADirectoryError, PermissionError):
                continue
            for e in entries:
                if e.name.startswith(".olcf-upload-"):
                    continue
                child_rel = f"{cur_rel}/{e.name}" if cur_rel else e.name
                try:
                    if e.is_symlink():
                        continue  # never traverse or expose symlinks
                    st = e.stat(follow_symlinks=False)
                except OSError:
                    continue
                info = self._info(child_rel, Path(e.path), st)
                yield info
                if recursive and info.is_dir:
                    stack.append((child_rel, Path(e.path)))

    def open_read(self, path: str, offset: int = 0, length: int | None = None) -> BinaryIO:
        rel = normalize_rel(path)
        p = self._abs(rel)
        try:
            fd = os.open(p, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        except FileNotFoundError:
            raise NotFound(f"'{rel}' does not exist") from None
        except IsADirectoryError:
            raise BadRequest(f"'{rel}' is a directory") from None
        except OSError as exc:
            if exc.errno in (40, 62):  # ELOOP
                raise Forbidden("refusing to follow a symbolic link") from None
            raise
        fh = os.fdopen(fd, "rb")
        if offset:
            fh.seek(offset)
        if length is None:
            return fh  # type: ignore[return-value]
        return _Limited(fh, length)  # type: ignore[return-value]

    # --- writes
    def open_write(self, path: str, overwrite: bool = True) -> BinaryWriter:
        rel = normalize_rel(path)
        if not rel:
            raise BadRequest("cannot write to the lakehouse root")
        p = self._abs(rel)
        if p.is_dir():
            raise BadRequest(f"'{rel}' is a directory")
        return _LocalWriter(self, rel, p, overwrite)

    def mkdirs(self, path: str) -> None:
        p = self._abs(path)
        p.mkdir(parents=True, exist_ok=True)
        self._check_inside(p)

    def delete(self, path: str, recursive: bool = False) -> int:
        rel = normalize_rel(path)
        if not rel:
            raise BadRequest("refusing to delete the lakehouse root")
        p = self._abs(rel)
        if p.is_symlink():
            p.unlink()
            return 1
        if not p.exists():
            return 0
        if p.is_dir():
            if not recursive and any(p.iterdir()):
                raise Conflict(f"directory '{rel}' is not empty", remediation="pass recursive=true to delete it with its contents")
            n = sum(1 for _ in self.list(rel, recursive=True)) + 1
            shutil.rmtree(p)
            return n
        p.unlink()
        return 1

    def move(self, src: str, dst: str, overwrite: bool = False) -> None:
        s, d = self._abs(src), self._abs(dst)
        if not s.exists():
            raise NotFound(f"'{src}' does not exist")
        if d.exists() and not overwrite:
            raise Conflict(f"destination '{dst}' already exists")
        if str(d).startswith(str(s) + os.sep):
            raise BadRequest("cannot move a directory into itself")
        d.parent.mkdir(parents=True, exist_ok=True)
        self._check_inside(d.parent)
        if d.exists() and d.is_dir() and overwrite:
            shutil.rmtree(d)
        os.replace(s, d)


class _Limited:
    """Read-limiting wrapper (``length`` bytes from the current position)."""

    def __init__(self, fh: BinaryIO, length: int) -> None:
        self._fh, self._left = fh, length

    def read(self, n: int = -1) -> bytes:
        if self._left <= 0:
            return b""
        n = self._left if n is None or n < 0 else min(n, self._left)
        data = self._fh.read(n)
        self._left -= len(data)
        return data

    def close(self) -> None:
        self._fh.close()

    def __enter__(self) -> _Limited:
        return self

    def __exit__(self, *a: object) -> None:
        self.close()


class LocalProvider(StorageProvider):
    """Storage provider for a directory on the control-plane host or a mounted share."""
    id = "local"
    label = "Local filesystem"
    description = "Directory on the control-plane host or a mounted share. Intended for development and single-node installs."
    schemes = ("file",)
    connection_types = ("local",)
    capabilities = LocalFileSystem.capabilities
    requires_connection = False
    connection_fields = (
        FieldSpec("root", "Root directory", required=True, help="Absolute directory under which lakehouses may be created."),
    )

    def validate_root(self, root_uri: str) -> str:
        return "file://" + str(uri_to_path(root_uri)).rstrip("/")

    def validate_config(self, config: dict[str, Any]) -> dict[str, Any]:
        root = config.get("root")
        if root is not None and not str(root).startswith("/"):
            raise BadRequest("local connection 'root' must be an absolute path")
        return config

    def filesystem(self, root_uri: str, config: dict[str, Any], secret: Any) -> LocalFileSystem:
        root = uri_to_path(root_uri)
        root.mkdir(parents=True, exist_ok=True)
        return LocalFileSystem(root)

    def physical_uri(self, root_uri: str, rel: str = "") -> str:
        rel = normalize_rel(rel)
        root = str(uri_to_path(root_uri)).rstrip("/")
        return "file://" + (f"{root}/{rel}" if rel else root)

    def spark_conf(self, root_uri: str, config: dict[str, Any], secret: Any) -> dict[str, str]:
        # the local filesystem needs no credentials; the default Hadoop LocalFileSystem handles file://
        return {"spark.hadoop.fs.file.impl.disable.cache": "false"}

    def test(self, root_uri: str, config: dict[str, Any], secret: Any) -> dict[str, Any]:
        root = uri_to_path(root_uri)
        holder: dict[str, LocalFileSystem] = {}

        def dns() -> str:
            return "local filesystem - no DNS lookup required"

        def connect() -> str:
            root.mkdir(parents=True, exist_ok=True)
            holder["fs"] = LocalFileSystem(root)
            free = shutil.disk_usage(root).free
            return f"root {root} is a directory; {free / 1e9:.1f} GB free"

        def auth() -> str:
            if not os.access(root, os.R_OK | os.W_OK | os.X_OK):
                raise Forbidden(f"the service account lacks read/write access to {root}")
            return "service account has read/write/execute permission"

        steps = [("resolve DNS", dns), ("connect", connect), ("auth", auth)]
        steps += probe_roundtrip(lambda: holder["fs"])
        return run_steps(steps)
