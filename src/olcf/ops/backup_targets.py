# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Off-host destinations for backups: local directory, S3/MinIO, SFTP and a lakehouse of the platform's own storage layer.

A target moves *already encrypted* files, so none of them has to be trusted with plaintext. Every transfer streams (constant memory). Names are
restricted to ``[A-Za-z0-9._-]`` so a configured or remote-listed name can never address a path outside the target's directory.

``config`` per type (credentials always in the secret store, referenced by ``secret_id``):

* ``local``     ``{"path": "/mnt/nas/olcf-backups"}`` - an absolute directory (created 0700). Paths below the data directory's ``secrets``/``pki`` are refused.
* ``s3``        ``{"bucket", "prefix", "region", "endpoint_url", "path_style": bool, "ca_bundle"}``; secret JSON ``{"access_key_id", "secret_access_key", "session_token"}``
                (without a secret the default AWS credential chain - instance role, env - is used). MinIO: set ``endpoint_url`` and ``path_style``.
* ``sftp``      ``{"host", "port", "username", "path", "host_key_sha256"}`` (``paramiko`` required; the host key fingerprint is **mandatory** - there is no
                trust-on-first-use); secret JSON ``{"password"}`` or ``{"private_key", "passphrase"}``.
* ``lakehouse`` ``{"lakehouse": "<workspace>/<name>", "prefix": "Files/olcf-backups"}`` - written through the storage service, i.e. to whatever provider
                (local, S3, ADLS, OneLake...) that lakehouse uses.
"""

from __future__ import annotations

import base64
import hashlib
import os
import re
import shutil
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from olcf.core.context import AppContext
from olcf.core.errors import BadRequest, NotFound, Unavailable

NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$")
CHUNK = 1024 * 1024
TARGET_TYPES = ("local", "s3", "sftp", "lakehouse")


def safe_name(name: str) -> str:
    if not isinstance(name, str) or not NAME_RE.match(name) or ".." in name:
        raise BadRequest("invalid backup file name")
    return name


_CODE_RE = re.compile(r"\b(NoSuchBucket|AccessDenied|InvalidAccessKeyId|SignatureDoesNotMatch|NoSuchKey|AuthorizationFailure|ContainerNotFound|BlobNotFound|AuthenticationFailed)\b")


def provider_error_code(exc: BaseException) -> str | None:
    """Structured provider error code (e.g. ``NoSuchBucket``), independent of message clipping or file paths.

    Prefers the ``response["Error"]["Code"]`` / ``error_code`` attributes found anywhere on the exception chain;
    falls back to a well-known code token in the full (unclipped) message of any chained exception.
    """
    seen: set[int] = set()
    cur: BaseException | None = exc
    texts: list[str] = []
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        resp = getattr(cur, "response", None)
        if isinstance(resp, dict):
            code = (resp.get("Error") or {}).get("Code")
            if code:
                return str(code)
        code = getattr(cur, "error_code", None)
        if code:
            return str(code)
        texts.append(str(cur))
        cur = cur.__cause__ or cur.__context__
    m = _CODE_RE.search(" ".join(texts))
    return m.group(1) if m else None


class Target(ABC):
    """Base class of backup targets. A target stores, lists, fetches and deletes backup files, and is created from a name, configuration and optional secret.
    """
    type = "?"

    def __init__(self, ctx: AppContext, name: str, config: dict[str, Any], secret: Any = None) -> None:
        self.ctx, self.tname, self.config, self.secret = ctx, name, config, secret

    @abstractmethod
    def put(self, local: Path, name: str) -> str:
        """Upload ``local`` as ``name``; return the target URI."""

    @abstractmethod
    def get(self, name: str, dest: Path) -> None: ...

    @abstractmethod
    def list(self) -> list[dict[str, Any]]:
        """``[{name, size, modified}]`` for backup files (``*.olcfbak``)."""

    @abstractmethod
    def delete(self, name: str) -> None: ...

    def uri(self, name: str) -> str:
        return f"{self.type}://{self.tname}/{name}"

    def test(self) -> dict[str, Any]:
        """Write, list, read back and delete a small probe object; reports each step."""
        steps: list[dict[str, Any]] = []
        probe = f"olcf-probe-{os.getpid()}-{int(time.time() * 1000)}.probe"
        tmp = self.ctx.settings.path("tmp")
        tmp.mkdir(parents=True, exist_ok=True)
        src, dst = tmp / probe, tmp / (probe + ".back")
        payload = os.urandom(2048)
        src.write_bytes(payload)

        def step(name: str, fn: Any) -> bool:
            t0 = time.perf_counter()
            try:
                detail = fn()
                steps.append({"name": name, "ok": True, "detail": detail or "ok", "ms": round((time.perf_counter() - t0) * 1000, 1)})
                return True
            except Exception as exc:  # noqa: BLE001 - reported to the operator
                steps.append({"name": name, "ok": False, "detail": f"{type(exc).__name__}: {exc}"[:300], "ms": round((time.perf_counter() - t0) * 1000, 1),
                              "code": provider_error_code(exc), "remediation": self.remediation(exc)})
                return False

        try:
            ok = step("write", lambda: self.put(src, probe))
            def read_back() -> str:
                self.get(probe, dst)
                if dst.read_bytes() != payload:
                    raise RuntimeError("content read back differs from what was written")
                return "content verified"

            ok = ok and step("read-back", read_back)
            if ok:
                step("delete", lambda: self.delete(probe))
            else:
                try:
                    self.delete(probe)
                except Exception:  # noqa: BLE001, S110
                    pass
        finally:
            src.unlink(missing_ok=True)
            dst.unlink(missing_ok=True)
        good = all(s["ok"] for s in steps)
        return {"ok": good, "steps": steps, "detail": "target is writable and readable" if good else next(s["detail"] for s in steps if not s["ok"])}

    def remediation(self, exc: Exception) -> str:
        return "check the target configuration and credentials"


# ----------------------------------------------------------------------------- local
class LocalTarget(Target):
    """Backup target that writes to a directory on the control-plane host."""
    type = "local"

    def __init__(self, *a: Any, **k: Any) -> None:
        super().__init__(*a, **k)
        raw = self.config.get("path")
        if not raw or not os.path.isabs(str(raw)):
            raise BadRequest("local target needs an absolute 'path'")
        root = Path(raw).resolve()
        data = self.ctx.settings.data_dir.resolve()
        for forbidden in (data / "secrets", data / "pki"):
            if root == forbidden or forbidden in root.parents:
                raise BadRequest("a backup target must not be inside the secrets or pki directory")
        self.root = root

    def _dir(self) -> Path:
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        return self.root

    def put(self, local: Path, name: str) -> str:
        safe_name(name)
        dest = self._dir() / name
        tmp = dest.with_name(dest.name + ".partial")
        shutil.copyfile(local, tmp)
        os.chmod(tmp, 0o600)
        with open(tmp, "rb") as fh:
            os.fsync(fh.fileno())
        os.replace(tmp, dest)
        return f"file://{dest}"

    def get(self, name: str, dest: Path) -> None:
        p = self._dir() / safe_name(name)
        if not p.is_file():
            raise NotFound(f"{name} not found on target '{self.tname}'")
        shutil.copyfile(p, dest)

    def list(self) -> list[dict[str, Any]]:
        out = []
        for p in sorted(self._dir().glob("*.olcfbak")):
            st = p.stat()
            out.append({"name": p.name, "size": st.st_size, "modified": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(st.st_mtime))})
        return out

    def delete(self, name: str) -> None:
        (self._dir() / safe_name(name)).unlink(missing_ok=True)


# ----------------------------------------------------------------------------- s3
class S3Target(Target):
    """Backup target for S3-compatible object storage; needs the optional ``boto3`` package."""
    type = "s3"

    def _client(self) -> Any:
        try:
            import boto3
            from botocore.config import Config
        except ImportError as exc:  # pragma: no cover - extra not installed
            raise Unavailable("boto3 is not installed", remediation="pip install 'olcf[s3]'") from exc
        c = self.config
        if not c.get("bucket"):
            raise BadRequest("s3 target needs 'bucket'")
        kw: dict[str, Any] = {"config": Config(s3={"addressing_style": "path" if c.get("path_style") else "auto"}, retries={"max_attempts": 4, "mode": "standard"},
                                               connect_timeout=10, read_timeout=60)}
        if c.get("endpoint_url"):
            kw["endpoint_url"] = c["endpoint_url"]
        if c.get("region"):
            kw["region_name"] = c["region"]
        verify = c.get("ca_bundle") or self.ctx.settings.tls.ca_bundle
        if verify:
            kw["verify"] = verify
        sec = self.secret if isinstance(self.secret, dict) else {}
        if sec.get("access_key_id"):
            kw.update(aws_access_key_id=sec["access_key_id"], aws_secret_access_key=sec.get("secret_access_key"), aws_session_token=sec.get("session_token"))
        return boto3.client("s3", **kw)

    def _key(self, name: str) -> str:
        prefix = str(self.config.get("prefix") or "").strip("/")
        return f"{prefix}/{safe_name(name)}" if prefix else safe_name(name)

    def put(self, local: Path, name: str) -> str:
        from boto3.s3.transfer import TransferConfig

        self._client().upload_file(str(local), self.config["bucket"], self._key(name), Config=TransferConfig(multipart_threshold=16 * CHUNK, multipart_chunksize=8 * CHUNK),
                                   ExtraArgs={"ServerSideEncryption": "AES256"} if self.config.get("sse") else None)
        return f"s3://{self.config['bucket']}/{self._key(name)}"

    def get(self, name: str, dest: Path) -> None:
        from botocore.exceptions import ClientError

        try:
            self._client().download_file(self.config["bucket"], self._key(name), str(dest))
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
                raise NotFound(f"{name} not found on target '{self.tname}'") from exc
            raise

    def list(self) -> list[dict[str, Any]]:
        c = self._client()
        prefix = str(self.config.get("prefix") or "").strip("/")
        out = []
        for page in c.get_paginator("list_objects_v2").paginate(Bucket=self.config["bucket"], Prefix=(prefix + "/") if prefix else ""):
            for o in page.get("Contents", []):
                n = o["Key"].rsplit("/", 1)[-1]
                if n.endswith(".olcfbak") and NAME_RE.match(n):
                    out.append({"name": n, "size": o["Size"], "modified": o["LastModified"].strftime("%Y-%m-%dT%H:%M:%SZ")})
        return sorted(out, key=lambda x: x["name"])

    def delete(self, name: str) -> None:
        self._client().delete_object(Bucket=self.config["bucket"], Key=self._key(name))

    def remediation(self, exc: Exception) -> str:
        text = str(exc)
        if any(c in text for c in ("AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch", "403")):
            return "check the access key / secret and the bucket policy (needs s3:PutObject, GetObject, DeleteObject, ListBucket)"
        if "NoSuchBucket" in text:
            return "create the bucket or fix the bucket name"
        return "check endpoint_url, region, path_style and the CA bundle for private endpoints"


# ----------------------------------------------------------------------------- sftp
class SftpTarget(Target):
    """Backup target for an SFTP server; needs the optional ``paramiko`` package."""
    type = "sftp"

    def _connect(self) -> Any:
        try:
            import paramiko
        except ImportError as exc:
            raise Unavailable("paramiko is not installed", remediation="pip install paramiko (optional SFTP backup target)") from exc
        c = self.config
        for k in ("host", "username", "path"):
            if not c.get(k):
                raise BadRequest(f"sftp target needs '{k}'")
        if not c.get("host_key_sha256"):
            raise BadRequest("sftp target needs 'host_key_sha256' (SHA256 fingerprint of the server host key); trust-on-first-use is not offered")
        sock_timeout = 15
        import socket

        sock = socket.create_connection((c["host"], int(c.get("port", 22))), timeout=sock_timeout)
        tr = paramiko.Transport(sock)
        tr.start_client(timeout=sock_timeout)
        key = tr.get_remote_server_key()
        fp = base64.b64encode(hashlib.sha256(key.asbytes()).digest()).decode().rstrip("=")
        want = str(c["host_key_sha256"]).removeprefix("SHA256:").rstrip("=")
        if fp != want:
            tr.close()
            raise Unavailable("SFTP host key does not match the configured fingerprint", details={"presented": f"SHA256:{fp}"})
        sec = self.secret if isinstance(self.secret, dict) else {}
        if sec.get("private_key"):
            import io

            pk = None
            for cls in (paramiko.Ed25519Key, paramiko.ECDSAKey, paramiko.RSAKey):
                try:
                    pk = cls.from_private_key(io.StringIO(sec["private_key"]), password=sec.get("passphrase"))
                    break
                except Exception:  # noqa: BLE001, S112
                    continue
            if pk is None:
                tr.close()
                raise BadRequest("could not load the SFTP private key")
            tr.auth_publickey(c["username"], pk)
        else:
            tr.auth_password(c["username"], sec.get("password", ""))
        return tr, paramiko.SFTPClient.from_transport(tr)

    def _path(self, name: str) -> str:
        return str(self.config["path"]).rstrip("/") + "/" + safe_name(name)

    def put(self, local: Path, name: str) -> str:
        tr, sftp = self._connect()
        try:
            final = self._path(name)
            self._mkdirs(sftp, str(self.config["path"]))
            sftp.put(str(local), final + ".partial")
            try:
                sftp.posix_rename(final + ".partial", final)
            except OSError:
                try:
                    sftp.remove(final)
                except OSError:
                    pass
                sftp.rename(final + ".partial", final)
            return f"sftp://{self.config['host']}{final}"
        finally:
            sftp.close()
            tr.close()

    @staticmethod
    def _mkdirs(sftp: Any, path: str) -> None:
        cur = ""
        for part in [p for p in path.split("/") if p]:
            cur += "/" + part
            try:
                sftp.stat(cur)
            except OSError:
                sftp.mkdir(cur)

    def get(self, name: str, dest: Path) -> None:
        tr, sftp = self._connect()
        try:
            try:
                sftp.get(self._path(name), str(dest))
            except FileNotFoundError as exc:
                raise NotFound(f"{name} not found on target '{self.tname}'") from exc
        finally:
            sftp.close()
            tr.close()

    def list(self) -> list[dict[str, Any]]:
        tr, sftp = self._connect()
        try:
            out = []
            for a in sftp.listdir_attr(str(self.config["path"])):
                if a.filename.endswith(".olcfbak") and NAME_RE.match(a.filename):
                    out.append({"name": a.filename, "size": a.st_size, "modified": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(a.st_mtime or 0))})
            return sorted(out, key=lambda x: x["name"])
        except FileNotFoundError:
            return []
        finally:
            sftp.close()
            tr.close()

    def delete(self, name: str) -> None:
        tr, sftp = self._connect()
        try:
            try:
                sftp.remove(self._path(name))
            except FileNotFoundError:
                pass
        finally:
            sftp.close()
            tr.close()


# ----------------------------------------------------------------------------- lakehouse
class LakehouseTarget(Target):
    """Backup target that stores backup files in a lakehouse through the storage service."""
    type = "lakehouse"

    def _lh(self) -> tuple[Any, Any, str]:
        storage = self.ctx.maybe_service("storage")
        if storage is None:
            raise Unavailable("the storage module is not loaded")
        ref = self.config.get("lakehouse")
        if not ref:
            raise BadRequest("lakehouse target needs 'lakehouse' (<workspace>/<name> or item id)")
        prefix = str(self.config.get("prefix") or "Files/olcf-backups").strip("/")
        if ".." in prefix.split("/"):
            raise BadRequest("invalid prefix")
        return storage, storage.get_lakehouse(ref), prefix

    def put(self, local: Path, name: str) -> str:
        storage, lh, prefix = self._lh()
        safe_name(name)
        storage.mkdirs(lh, prefix)
        with open(local, "rb") as src, storage.open_write(lh, f"{prefix}/{name}", overwrite=True) as w:
            while chunk := src.read(CHUNK):
                w.write(chunk)
        return storage.physical_uri(lh, f"{prefix}/{name}")

    def get(self, name: str, dest: Path) -> None:
        storage, lh, prefix = self._lh()
        if not storage.exists(lh, f"{prefix}/{safe_name(name)}"):
            raise NotFound(f"{name} not found on target '{self.tname}'")
        with storage.open_read(lh, f"{prefix}/{name}") as src, open(dest, "wb") as out:
            while chunk := src.read(CHUNK):
                out.write(chunk)

    def list(self) -> list[dict[str, Any]]:
        storage, lh, prefix = self._lh()
        if not storage.exists(lh, prefix):
            return []
        return sorted(({"name": f.name, "size": f.size, "modified": f.modified} for f in storage.list_files(lh, prefix)
                       if not f.is_dir and f.name.endswith(".olcfbak") and NAME_RE.match(f.name)), key=lambda x: x["name"])

    def delete(self, name: str) -> None:
        storage, lh, prefix = self._lh()
        storage.delete(lh, f"{prefix}/{safe_name(name)}")


_TYPES: dict[str, type[Target]] = {"local": LocalTarget, "s3": S3Target, "sftp": SftpTarget, "lakehouse": LakehouseTarget}


def build_target(ctx: AppContext, row: dict[str, Any]) -> Target:
    cls = _TYPES.get(row["type"])
    if cls is None:
        raise BadRequest(f"unknown target type {row['type']!r}", details={"types": list(TARGET_TYPES)})
    secret = None
    if row.get("secret_id"):
        secret = ctx.secrets.resolve(row["secret_id"])
        if isinstance(secret, str):
            import json

            try:
                secret = json.loads(secret)
            except ValueError:
                secret = {"password": secret}
    return cls(ctx, row["name"], row.get("config") or {}, secret)
