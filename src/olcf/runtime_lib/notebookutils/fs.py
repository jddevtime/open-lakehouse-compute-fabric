# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``notebookutils.fs``: file operations over lakehouse paths (stdlib only; Hadoop FS through the job's Spark session when needed).

Paths go through :func:`olcf.runtime_lib.paths.resolve` first, so ``Files/raw/a.csv``, ``/lakehouse/default/Files/raw/a.csv`` and OneLake ``abfss://`` URLs
mean what they mean in Fabric. Two back-ends:

* :class:`LocalBackend` - plain Python for ``file://`` / local paths (local lakehouses, job directories). No JVM, no Spark session is started.
* :class:`HadoopBackend` - ``org.apache.hadoop.fs.FileSystem`` obtained from the Spark session (``spark._jvm``), so every scheme the session has credentials
  for (OneLake ``abfss://``, S3A, ADLS) works with the job's storage identity.

Not provided: ``mount`` / ``unmount`` / ``mounts`` / ``getMountPath`` - the Fabric ``/synfs`` FUSE mounts do not exist on OLCF workers (:class:`NotSupportedError`
with guidance).
"""

from __future__ import annotations

import shutil
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from olcf.runtime_lib import paths
from olcf.runtime_lib.notebookutils.errors import NotSupportedError

_MOUNT_HELP = ("Fabric mount points (the /synfs FUSE mounts) do not exist on OLCF workers; address data with Spark APIs and relative Files/ or Tables/ paths, "
               "abfss:// URLs, or notebookutils.fs.* instead of mount points / getMountPath.")


@dataclass
class FileInfo:
    """Mirror of Fabric's ``FileInfo`` (attributes ``path``, ``name``, ``size``, ``isDir``, ``isFile``, ``modifyTime``)."""

    path: str
    name: str
    size: int
    isDir: bool  # noqa: N815 - Fabric attribute names
    isFile: bool  # noqa: N815
    modifyTime: int  # noqa: N815 - epoch milliseconds

    def __str__(self) -> str:
        return f"FileInfo(path='{self.path}', name='{self.name}', size={self.size}, modifyTime={self.modifyTime})"


def _local_path(uri: str) -> str:
    if uri.startswith("file:"):
        return urllib.request.url2pathname(urllib.parse.urlparse(uri).path)
    return uri


def _is_local(uri: str) -> bool:
    return uri.startswith("file:") or "://" not in uri


# =============================================================================================== back-ends
class LocalBackend:
    """Pure-Python implementation for local file systems."""

    def ls(self, uri: str) -> list[FileInfo]:
        p = Path(_local_path(uri))
        if not p.exists():
            raise FileNotFoundError(f"path does not exist: {uri}")
        entries = sorted(p.iterdir()) if p.is_dir() else [p]
        out = []
        for e in entries:
            st = e.stat()
            is_dir = e.is_dir()
            out.append(FileInfo(path=e.resolve().as_uri(), name=e.name, size=0 if is_dir else st.st_size, isDir=is_dir, isFile=not is_dir,
                                modifyTime=int(st.st_mtime * 1000)))
        return out

    def mkdirs(self, uri: str) -> bool:
        Path(_local_path(uri)).mkdir(parents=True, exist_ok=True)
        return True

    def exists(self, uri: str) -> bool:
        return Path(_local_path(uri)).exists()

    def cp(self, src: str, dst: str, recurse: bool) -> bool:
        s, d = Path(_local_path(src)), Path(_local_path(dst))
        if not s.exists():
            raise FileNotFoundError(f"source does not exist: {src}")
        if s.is_dir():
            if not recurse:
                raise IsADirectoryError(f"{src} is a directory; pass recurse=True to copy it")
            shutil.copytree(s, d, dirs_exist_ok=True)
            return True
        if d.is_dir():
            d = d / s.name
        d.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(s, d)
        return True

    def mv(self, src: str, dst: str, create_path: bool, overwrite: bool) -> bool:
        s, d = Path(_local_path(src)), Path(_local_path(dst))
        if not s.exists():
            raise FileNotFoundError(f"source does not exist: {src}")
        if d.is_dir() and not s.is_dir():
            d = d / s.name
        if d.exists() and not overwrite:
            raise FileExistsError(f"destination exists: {dst} (pass overwrite=True)")
        if create_path:
            d.parent.mkdir(parents=True, exist_ok=True)
        if d.exists() and overwrite:
            shutil.rmtree(d) if d.is_dir() else d.unlink()
        shutil.move(str(s), str(d))
        return True

    def rm(self, uri: str, recurse: bool) -> bool:
        p = Path(_local_path(uri))
        if not p.exists():
            return False
        if p.is_dir():
            if not recurse:
                raise IsADirectoryError(f"{uri} is a directory; pass recurse=True to delete it")
            shutil.rmtree(p)
        else:
            p.unlink()
        return True

    def head(self, uri: str, max_bytes: int) -> str:
        with open(_local_path(uri), "rb") as fh:
            return fh.read(max_bytes).decode("utf-8", "replace")

    def put(self, uri: str, content: str, overwrite: bool) -> bool:
        p = Path(_local_path(uri))
        if p.exists() and not overwrite:
            raise FileExistsError(f"{uri} exists (pass overwrite=True)")
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")
        return True

    def append(self, uri: str, content: str, create: bool) -> bool:
        p = Path(_local_path(uri))
        if not p.exists() and not create:
            raise FileNotFoundError(f"{uri} does not exist (pass createFileIfNotExists=True)")
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "a", encoding="utf-8") as fh:
            fh.write(content)
        return True


class HadoopBackend:
    """Hadoop ``FileSystem`` through the job's Spark session (py4j)."""

    def __init__(self, spark: Any) -> None:
        self._spark = spark

    # -- plumbing
    def _jvm(self) -> Any:
        return self._spark._jvm  # noqa: SLF001 - py4j gateway of the SparkSession

    def _conf(self) -> Any:
        return self._spark._jsc.hadoopConfiguration()  # noqa: SLF001

    def _path(self, uri: str) -> Any:
        return self._jvm().org.apache.hadoop.fs.Path(uri)

    def _fs(self, uri: str) -> Any:
        return self._path(uri).getFileSystem(self._conf())

    # -- operations
    def ls(self, uri: str) -> list[FileInfo]:
        fs, p = self._fs(uri), self._path(uri)
        if not fs.exists(p):
            raise FileNotFoundError(f"path does not exist: {uri}")
        sts = fs.listStatus(p)
        out = []
        for st in sts:
            out.append(FileInfo(path=str(st.getPath().toString()), name=str(st.getPath().getName()), size=int(st.getLen()), isDir=bool(st.isDirectory()),
                                isFile=bool(st.isFile()), modifyTime=int(st.getModificationTime())))
        return sorted(out, key=lambda f: f.path)

    def mkdirs(self, uri: str) -> bool:
        return bool(self._fs(uri).mkdirs(self._path(uri)))

    def exists(self, uri: str) -> bool:
        return bool(self._fs(uri).exists(self._path(uri)))

    def cp(self, src: str, dst: str, recurse: bool) -> bool:
        sfs, dfs = self._fs(src), self._fs(dst)
        sp, dp = self._path(src), self._path(dst)
        if not sfs.exists(sp):
            raise FileNotFoundError(f"source does not exist: {src}")
        if sfs.getFileStatus(sp).isDirectory() and not recurse:
            raise IsADirectoryError(f"{src} is a directory; pass recurse=True to copy it")
        return bool(self._jvm().org.apache.hadoop.fs.FileUtil.copy(sfs, sp, dfs, dp, False, True, self._conf()))

    def mv(self, src: str, dst: str, create_path: bool, overwrite: bool) -> bool:
        sfs, dfs = self._fs(src), self._fs(dst)
        sp, dp = self._path(src), self._path(dst)
        if not sfs.exists(sp):
            raise FileNotFoundError(f"source does not exist: {src}")
        if dfs.exists(dp):
            if not overwrite:
                raise FileExistsError(f"destination exists: {dst} (pass overwrite=True)")
            dfs.delete(dp, True)
        if create_path:
            dfs.mkdirs(dp.getParent())
        if sfs.getUri().toString() == dfs.getUri().toString():
            return bool(sfs.rename(sp, dp))
        return bool(self._jvm().org.apache.hadoop.fs.FileUtil.copy(sfs, sp, dfs, dp, True, True, self._conf()))

    def rm(self, uri: str, recurse: bool) -> bool:
        fs, p = self._fs(uri), self._path(uri)
        if not fs.exists(p):
            return False
        if fs.getFileStatus(p).isDirectory() and not recurse:
            raise IsADirectoryError(f"{uri} is a directory; pass recurse=True to delete it")
        return bool(fs.delete(p, recurse))

    def head(self, uri: str, max_bytes: int) -> str:
        fs, p = self._fs(uri), self._path(uri)
        stream = fs.open(p)
        try:
            arr = self._spark._sc._gateway.new_array(self._jvm().byte, int(max_bytes))  # noqa: SLF001
            total = 0
            while total < max_bytes:
                n = stream.read(arr, total, max_bytes - total)
                if n < 0:
                    break
                total += n
            return bytes((arr[i] & 0xFF) for i in range(total)).decode("utf-8", "replace")
        finally:
            stream.close()

    def put(self, uri: str, content: str, overwrite: bool) -> bool:
        fs, p = self._fs(uri), self._path(uri)
        if fs.exists(p) and not overwrite:
            raise FileExistsError(f"{uri} exists (pass overwrite=True)")
        stream = fs.create(p, True)
        try:
            stream.write(bytearray(content.encode("utf-8")))
        finally:
            stream.close()
        return True

    def append(self, uri: str, content: str, create: bool) -> bool:
        fs, p = self._fs(uri), self._path(uri)
        if not fs.exists(p):
            if not create:
                raise FileNotFoundError(f"{uri} does not exist (pass createFileIfNotExists=True)")
            return self.put(uri, content, True)
        old = self.head(uri, 1 << 30)  # object stores cannot append in place: rewrite (bounded by 1 GiB)
        return self.put(uri, old + content, True)


# =============================================================================================== namespace
class FS:
    """The ``notebookutils.fs`` namespace."""

    def __init__(self, state: dict[str, Any]) -> None:
        self._state = state
        self._local = LocalBackend()

    # -- resolution
    def _resolve(self, path: str) -> str:
        if not isinstance(path, str) or not path:
            raise ValueError("a path is required")
        return paths.resolve(path, for_="spark")

    def _backend(self, uri: str) -> Any:
        return self._local if _is_local(uri) else HadoopBackend(self._state["spark"])

    # -- Fabric API
    def ls(self, path: str) -> list[FileInfo]:
        """List a directory (or return the single entry for a file)."""
        uri = self._resolve(path)
        return self._backend(uri).ls(uri)

    def mkdirs(self, path: str) -> bool:
        uri = self._resolve(path)
        return self._backend(uri).mkdirs(uri)

    def exists(self, path: str) -> bool:
        uri = self._resolve(path)
        return self._backend(uri).exists(uri)

    def cp(self, src: str, dest: str, recurse: bool = False) -> bool:
        s, d = self._resolve(src), self._resolve(dest)
        if _is_local(s) and _is_local(d):
            return self._local.cp(s, d, recurse)
        return HadoopBackend(self._state["spark"]).cp(s, d, recurse)

    def fastcp(self, src: str, dest: str, recurse: bool = True) -> bool:
        """Fabric's accelerated copy; here an alias of :meth:`cp` (same result, no AzCopy)."""
        return self.cp(src, dest, recurse)

    def mv(self, src: str, dest: str, create_path: bool = False, overwrite: bool = False) -> bool:
        s, d = self._resolve(src), self._resolve(dest)
        if _is_local(s) and _is_local(d):
            return self._local.mv(s, d, create_path, overwrite)
        return HadoopBackend(self._state["spark"]).mv(s, d, create_path, overwrite)

    def rm(self, path: str, recurse: bool = False) -> bool:
        uri = self._resolve(path)
        return self._backend(uri).rm(uri, recurse)

    def head(self, path: str, maxBytes: int = 1048576) -> str:  # noqa: N803 - Fabric parameter name
        uri = self._resolve(path)
        return self._backend(uri).head(uri, int(maxBytes))

    def put(self, path: str, content: str, overwrite: bool = False) -> bool:
        uri = self._resolve(path)
        return self._backend(uri).put(uri, str(content), overwrite)

    def append(self, path: str, content: str, createFileIfNotExists: bool = False) -> bool:  # noqa: N803
        uri = self._resolve(path)
        return self._backend(uri).append(uri, str(content), createFileIfNotExists)

    # -- unsupported
    def mount(self, *args: Any, **kwargs: Any) -> Any:
        raise NotSupportedError("notebookutils.fs.mount", "mount points need the Fabric FUSE layer", _MOUNT_HELP)

    def unmount(self, *args: Any, **kwargs: Any) -> Any:
        raise NotSupportedError("notebookutils.fs.unmount", "there are no mount points", _MOUNT_HELP)

    def mounts(self, *args: Any, **kwargs: Any) -> Any:
        raise NotSupportedError("notebookutils.fs.mounts", "there are no mount points", _MOUNT_HELP)

    def getMountPath(self, *args: Any, **kwargs: Any) -> Any:  # noqa: N802
        raise NotSupportedError("notebookutils.fs.getMountPath", "there are no mount points", _MOUNT_HELP)

    def help(self, method_name: str = "") -> None:
        doc = {"ls": "ls(path) -> list[FileInfo]", "mkdirs": "mkdirs(path) -> bool", "exists": "exists(path) -> bool", "cp": "cp(src, dest, recurse=False) -> bool",
               "fastcp": "fastcp(src, dest, recurse=True) -> bool", "mv": "mv(src, dest, create_path=False, overwrite=False) -> bool", "rm": "rm(path, recurse=False) -> bool",
               "head": "head(path, maxBytes=1048576) -> str", "put": "put(path, content, overwrite=False) -> bool",
               "append": "append(path, content, createFileIfNotExists=False) -> bool"}
        if method_name:
            print(doc.get(method_name, f"{method_name}: not available in OLCF"))
            return
        print("notebookutils.fs (OLCF):")
        for line in doc.values():
            print("  " + line)
        print("  mount / unmount / mounts / getMountPath: not supported in OLCF")

