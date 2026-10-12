# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Provider-neutral storage abstractions.

A *provider* (:class:`StorageProvider`) knows a storage technology: how to validate a lakehouse root URI, how to
build a :class:`FileSystem` for it, which Hadoop/Spark settings and delta-rs/PyIceberg options a runtime needs, and
how to run the stepwise connectivity test that powers the "Test connectivity" buttons.  A :class:`FileSystem`
is a *bound* view of one lakehouse root: every method takes a root-relative path that has already been normalised by
:func:`olcf.storage.paths.normalize_rel`.  Third-party providers plug in through the ``olcf.storage_providers``
entry-point group (see :mod:`olcf.storage.providers`).
"""

from __future__ import annotations

import io
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Iterator
from dataclasses import asdict, dataclass
from typing import Any, BinaryIO
from urllib.parse import urlparse

from olcf.core.errors import NotFound, OlcfError, Unsupported


@dataclass(frozen=True)
class FileInfo:
    """One file or directory below a lakehouse root."""

    path: str  # root-relative, '/'-separated, no trailing slash
    is_dir: bool
    size: int = 0
    modified: str | None = None  # ISO-8601 UTC
    etag: str | None = None

    @property
    def name(self) -> str:
        return self.path.rsplit("/", 1)[-1]

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["name"] = self.name
        return d


@dataclass(frozen=True)
class Capabilities:
    """What a provider can do; the UI and the API fail closed on anything not advertised."""

    list: bool = True
    read: bool = True
    write: bool = True
    delete: bool = True
    rename: bool = True
    atomic_rename: bool = False  # rename is a single atomic metadata operation (POSIX, ADLS Gen2)
    server_side_copy: bool = False
    true_directories: bool = True  # False => "directories" are key prefixes (S3)
    range_read: bool = True
    multipart_write: bool = False
    acl: bool = False  # exposes ACL / policy visibility
    delta_commits: str = "rename"  # how delta-rs obtains commit atomicity: rename | conditional_put | lock

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FieldSpec:
    """Describes one connection/lakehouse form field so the UI can render provider forms generically."""

    name: str
    label: str
    type: str = "string"  # string | bool | int | enum | secret | text
    required: bool = False
    help: str = ""
    options: tuple[str, ...] = ()
    default: Any = None
    secret: bool = False  # stored in the secret store, never in connection config
    visible_when: dict[str, Any] | None = None  # e.g. {"auth": ["service_principal"]}

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["options"] = list(self.options)
        return d


class BinaryWriter(ABC):
    """Streaming writer returned by :meth:`FileSystem.open_write`. ``close()`` commits, ``abort()`` discards."""

    @abstractmethod
    def write(self, data: bytes) -> int: ...

    @abstractmethod
    def close(self) -> None: ...

    @abstractmethod
    def abort(self) -> None: ...

    def __enter__(self) -> BinaryWriter:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            self.abort()
        else:
            self.close()


class FileSystem(ABC):
    """A provider bound to one lakehouse root. Paths are root-relative and pre-validated."""

    provider: str = "?"
    capabilities: Capabilities = Capabilities()

    # --- required primitives
    @abstractmethod
    def list(self, path: str = "", recursive: bool = False) -> Iterator[FileInfo]:
        """Yield entries below ``path`` (directories first is *not* guaranteed). Missing path => empty iterator."""

    @abstractmethod
    def stat(self, path: str) -> FileInfo:
        """Return metadata or raise :class:`NotFound`."""

    @abstractmethod
    def open_read(self, path: str, offset: int = 0, length: int | None = None) -> BinaryIO:
        """Return a streaming binary reader positioned at ``offset`` (raises :class:`NotFound`)."""

    @abstractmethod
    def open_write(self, path: str, overwrite: bool = True) -> BinaryWriter:
        """Return a streaming writer; parent directories are created implicitly."""

    @abstractmethod
    def delete(self, path: str, recursive: bool = False) -> int:
        """Delete a file or directory; returns the number of objects removed (0 when it did not exist)."""

    @abstractmethod
    def mkdirs(self, path: str) -> None: ...

    @abstractmethod
    def move(self, src: str, dst: str, overwrite: bool = False) -> None: ...

    # --- derived helpers (providers may override with something faster)
    def exists(self, path: str) -> bool:
        try:
            self.stat(path)
            return True
        except NotFound:
            return False

    def is_dir(self, path: str) -> bool:
        try:
            return self.stat(path).is_dir
        except NotFound:
            return False

    def read_bytes(self, path: str, max_bytes: int | None = None) -> bytes:
        with self.open_read(path) as fh:
            data = fh.read() if max_bytes is None else fh.read(max_bytes + 1)
        if max_bytes is not None and len(data) > max_bytes:
            raise OlcfError(f"{path} is larger than {max_bytes} bytes")
        return data

    def write_bytes(self, path: str, data: bytes, overwrite: bool = True) -> None:
        with self.open_write(path, overwrite=overwrite) as w:
            w.write(data)

    def copy(self, src: str, dst: str, overwrite: bool = False) -> int:
        """Stream-copy a file or a directory tree; returns the number of files copied."""
        info = self.stat(src)
        if not info.is_dir:
            if not overwrite and self.exists(dst):
                from olcf.core.errors import Conflict

                raise Conflict(f"destination '{dst}' already exists")
            with self.open_read(src) as r, self.open_write(dst, overwrite=True) as w:
                for chunk in iter(lambda: r.read(1024 * 1024), b""):
                    w.write(chunk)
            return 1
        n = 0
        for child in self.list(src, recursive=True):
            if child.is_dir:
                continue
            rel = child.path[len(src.rstrip("/")) + 1:]
            n += self.copy(child.path, f"{dst.rstrip('/')}/{rel}", overwrite=overwrite)
        if n == 0:
            self.mkdirs(dst)
        return n

    def glob_files(self, prefix: str, suffixes: tuple[str, ...] = ()) -> list[FileInfo]:
        out = [f for f in self.list(prefix, recursive=True) if not f.is_dir and (not suffixes or f.path.lower().endswith(suffixes))]
        return sorted(out, key=lambda f: f.path)

    def physical_uri(self, path: str = "") -> str:
        raise Unsupported(f"provider {self.provider} cannot express physical URIs")

    def local_path(self, path: str) -> str | None:
        """Absolute local filesystem path when the provider is local, else ``None``."""
        return None

    def close(self) -> None:  # pragma: no cover - default no-op
        return None


class RangeFile(io.RawIOBase):
    """Seekable read-only file over ``FileSystem.open_read(offset, length)`` range requests.

    PyArrow's Parquet reader needs ``seek``/``read``; remote providers only offer streaming ranges.  A 1 MiB
    read-ahead window keeps the number of requests small for footer-first access patterns.
    """

    def __init__(self, fs: FileSystem, path: str, size: int | None = None, window: int = 1 << 20) -> None:
        super().__init__()
        self._fs, self._path = fs, path
        self._size = fs.stat(path).size if size is None else size
        self._pos = 0
        self._window = window
        self._buf_start = 0
        self._buf = b""

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._pos

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            self._pos = offset
        elif whence == io.SEEK_CUR:
            self._pos += offset
        elif whence == io.SEEK_END:
            self._pos = self._size + offset
        self._pos = max(0, self._pos)
        return self._pos

    def _fetch(self, start: int, length: int) -> bytes:
        with self._fs.open_read(self._path, offset=start, length=length) as fh:
            return fh.read(length)

    def read(self, size: int = -1) -> bytes:
        if self._pos >= self._size:
            return b""
        if size is None or size < 0:
            size = self._size - self._pos
        size = min(size, self._size - self._pos)
        end = self._pos + size
        if self._buf_start <= self._pos and end <= self._buf_start + len(self._buf):
            out = self._buf[self._pos - self._buf_start:end - self._buf_start]
        elif size >= self._window:
            out = self._fetch(self._pos, size)
        else:
            want = min(self._window, self._size - self._pos)
            self._buf, self._buf_start = self._fetch(self._pos, want), self._pos
            out = self._buf[:size]
        self._pos += len(out)
        return out

    def readinto(self, b) -> int:  # type: ignore[no-untyped-def]
        data = self.read(len(b))
        b[:len(data)] = data
        return len(data)


# ----------------------------------------------------------------------------- connectivity test helper
def run_steps(steps: Iterable[tuple[str, Callable[[], str | None]]]) -> dict[str, Any]:
    """Run named test steps in order; stop at the first failure and mark the rest ``skipped``.

    Each callable returns an optional detail string.  The result shape is the platform-wide
    ``{ok, steps:[{name, ok, detail, ms}]}`` used by every "Test connectivity" button.
    """
    from olcf.core.redaction import redact_text

    out: list[dict[str, Any]] = []
    failed = False
    for name, fn in steps:
        if failed:
            out.append({"name": name, "ok": False, "skipped": True, "detail": "skipped because an earlier step failed", "ms": 0.0})
            continue
        t0 = time.perf_counter()
        try:
            detail = fn() or ""
            out.append({"name": name, "ok": True, "detail": redact_text(detail), "ms": round((time.perf_counter() - t0) * 1000, 1)})
        except Exception as exc:  # noqa: BLE001 - a failing step is a test result, not an error
            failed = True
            msg = exc.message if isinstance(exc, OlcfError) else f"{type(exc).__name__}: {exc}"
            out.append({"name": name, "ok": False, "detail": redact_text(msg), "ms": round((time.perf_counter() - t0) * 1000, 1)})
    return {"ok": bool(out) and not failed, "steps": out}


STEP_NAMES = ("resolve DNS", "connect", "auth", "list", "write", "read", "delete")


class StorageProvider(ABC):
    """Static description + factory for one storage technology."""

    id: str = "?"
    label: str = "?"
    description: str = ""
    schemes: tuple[str, ...] = ()
    #: connection ``type`` values served by this provider (``minio`` shares the S3 provider)
    connection_types: tuple[str, ...] = ()
    capabilities: Capabilities = Capabilities()
    connection_fields: tuple[FieldSpec, ...] = ()
    #: whether a lakehouse of this provider must reference a connection (credentials)
    requires_connection: bool = True

    # --- configuration
    def claims(self, root_uri: str) -> bool:
        """Does this provider own ``root_uri``?  Default: by URI scheme (OneLake/ADLS refine by host)."""
        return urlparse(root_uri).scheme in self.schemes

    def validate_root(self, root_uri: str) -> str:
        """Validate and normalise a lakehouse root URI (without trailing slash)."""
        return root_uri.rstrip("/")

    def validate_config(self, config: dict[str, Any]) -> dict[str, Any]:
        return config

    @abstractmethod
    def filesystem(self, root_uri: str, config: dict[str, Any], secret: Any) -> FileSystem: ...

    @abstractmethod
    def physical_uri(self, root_uri: str, rel: str = "") -> str:
        """URI a Spark/delta-rs client uses for ``rel`` below ``root_uri``."""

    @abstractmethod
    def spark_conf(self, root_uri: str, config: dict[str, Any], secret: Any) -> dict[str, str]:
        """Spark-ready settings (``spark.hadoop.*``) incl. credentials. Compute-layer only."""

    def delta_storage_options(self, root_uri: str, config: dict[str, Any], secret: Any) -> dict[str, str]:
        return {}

    def env(self, root_uri: str, config: dict[str, Any], secret: Any) -> dict[str, str]:
        """Environment variables for Python-side clients inside jobs (never logged)."""
        return {}

    def iceberg_properties(self, root_uri: str, config: dict[str, Any], secret: Any) -> dict[str, str]:
        """PyIceberg FileIO properties."""
        return {}

    def describe(self) -> dict[str, Any]:
        return {
            "id": self.id, "label": self.label, "description": self.description, "schemes": list(self.schemes),
            "connection_types": list(self.connection_types), "capabilities": self.capabilities.to_dict(),
            "requires_connection": self.requires_connection,
            "fields": [f.to_dict() for f in self.connection_fields],
        }

    def test(self, root_uri: str, config: dict[str, Any], secret: Any) -> dict[str, Any]:
        """Stepwise connectivity test: DNS -> connect -> auth -> list -> write -> read -> delete."""
        raise NotImplementedError


def probe_roundtrip(get_fs: Callable[[], FileSystem], *, prefix: str = "Files/.olcf-connectivity-test") -> list[tuple[str, Callable[[], str | None]]]:
    """The shared list -> write -> read -> delete tail of every provider test.

    ``get_fs`` is called lazily so the tail can be appended to the steps list before the connect step has run.
    """
    import os

    name = f"{prefix}/probe-{os.getpid()}-{int(time.time() * 1000)}.txt"
    payload = b"olcf connectivity probe\n"

    def do_list() -> str:
        n = sum(1 for _ in get_fs().list("", recursive=False))
        return f"listed {n} top-level entries"

    def do_write() -> str:
        get_fs().write_bytes(name, payload)
        return f"wrote {len(payload)} bytes"

    def do_read() -> str:
        got = get_fs().read_bytes(name)
        if got != payload:
            raise OlcfError("read-back content mismatch")
        return f"read back {len(got)} bytes, content identical"

    def do_delete() -> str:
        fs = get_fs()
        fs.delete(name)
        fs.delete(prefix, recursive=True)
        if fs.exists(name):
            raise OlcfError("object still exists after delete")
        return "deleted probe object"

    return [("list", do_list), ("write", do_write), ("read", do_read), ("delete", do_delete)]
