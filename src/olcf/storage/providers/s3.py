# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Amazon S3 and S3-compatible object stores (MinIO, Ceph RGW, Cloudflare R2, ...).

Supports custom endpoints, path-style addressing, an optional private CA bundle and either static credentials
(from the connection's secret) or the ambient AWS credential chain (instance profile, IRSA, env).  Object stores
have no real directories, so ``mkdirs`` writes a zero-byte ``dir/`` marker (exactly what the S3 console does).
Large uploads are streamed as multipart uploads so memory stays bounded regardless of file size.
"""

from __future__ import annotations

import io
import socket
import ssl
from collections.abc import Iterator
from typing import Any, BinaryIO
from urllib.parse import urlparse

from olcf.core.errors import BadRequest, Conflict, NotFound, OlcfError, Unavailable
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

PART_SIZE = 8 * 1024 * 1024


def parse_s3_uri(uri: str) -> tuple[str, str]:
    u = urlparse(uri)
    if u.scheme not in ("s3", "s3a", "s3n") or not u.netloc:
        raise BadRequest(f"expected s3://<bucket>/<prefix>, got '{uri}'")
    return u.netloc, u.path.strip("/")


def credentials_from_secret(secret: Any) -> dict[str, str]:
    """Accept ``{"access_key_id","secret_access_key","session_token"?}`` or the compact ``"AKID:SECRET"`` form."""
    if secret in (None, ""):
        return {}
    if isinstance(secret, dict):
        ak = secret.get("access_key_id") or secret.get("aws_access_key_id") or secret.get("access_key")
        sk = secret.get("secret_access_key") or secret.get("aws_secret_access_key") or secret.get("secret_key")
        if not ak or not sk:
            raise BadRequest("S3 secret must contain access_key_id and secret_access_key")
        out = {"access_key_id": str(ak), "secret_access_key": str(sk)}
        if secret.get("session_token"):
            out["session_token"] = str(secret["session_token"])
        return out
    if isinstance(secret, str) and ":" in secret:
        ak, sk = secret.split(":", 1)
        return {"access_key_id": ak, "secret_access_key": sk}
    raise BadRequest("S3 secret must be a structured secret {access_key_id, secret_access_key[, session_token]}")


def _is_http(endpoint: str | None) -> bool:
    return bool(endpoint) and endpoint.lower().startswith("http://")


class _Body(io.RawIOBase):
    """Readable raw-IO wrapper around botocore's StreamingBody (usable by pyarrow, gzip and ``with``)."""

    def __init__(self, body: Any) -> None:
        super().__init__()
        self._b = body

    def readable(self) -> bool:
        return True

    def read(self, n: int = -1) -> bytes:  # type: ignore[override]
        return self._b.read() if n is None or n < 0 else self._b.read(n)

    def readinto(self, buf: Any) -> int:  # type: ignore[override]
        data = self._b.read(len(buf))
        buf[: len(data)] = data
        return len(data)

    def close(self) -> None:
        try:
            self._b.close()
        finally:
            super().close()

    def __iter__(self) -> Iterator[bytes]:  # type: ignore[override]
        return iter(lambda: self.read(1 << 20), b"")


class _S3Writer(BinaryWriter):
    """Buffers up to one part; switches to a multipart upload once the first part overflows."""

    def __init__(self, fs: S3FileSystem, key: str, rel: str) -> None:
        self._fs, self._key, self._rel = fs, key, rel
        self._buf = bytearray()
        self._upload_id: str | None = None
        self._parts: list[dict[str, Any]] = []
        self._done = False

    def _upload_part(self, body: bytes) -> None:
        c = self._fs.client
        if self._upload_id is None:
            self._upload_id = c.create_multipart_upload(Bucket=self._fs.bucket, Key=self._key)["UploadId"]
        n = len(self._parts) + 1
        r = c.upload_part(Bucket=self._fs.bucket, Key=self._key, UploadId=self._upload_id, PartNumber=n, Body=body)
        self._parts.append({"ETag": r["ETag"], "PartNumber": n})

    def write(self, data: bytes) -> int:
        self._buf.extend(data)
        while len(self._buf) >= PART_SIZE:
            part = bytes(self._buf[:PART_SIZE])
            del self._buf[:PART_SIZE]
            self._upload_part(part)
        return len(data)

    def close(self) -> None:
        if self._done:
            return
        self._done = True
        c = self._fs.client
        try:
            if self._upload_id is None:
                c.put_object(Bucket=self._fs.bucket, Key=self._key, Body=bytes(self._buf))
            else:
                if self._buf:
                    self._upload_part(bytes(self._buf))
                c.complete_multipart_upload(Bucket=self._fs.bucket, Key=self._key, UploadId=self._upload_id,
                                            MultipartUpload={"Parts": self._parts})
        except BaseException:
            self.abort_upload()
            raise

    def abort_upload(self) -> None:
        if self._upload_id:
            try:
                self._fs.client.abort_multipart_upload(Bucket=self._fs.bucket, Key=self._key, UploadId=self._upload_id)
            except Exception:  # noqa: BLE001, S110
                pass

    def abort(self) -> None:
        if self._done:
            return
        self._done = True
        self.abort_upload()


class S3FileSystem(FileSystem):
    """File system over an S3 bucket and prefix."""
    provider = "s3"
    capabilities = Capabilities(atomic_rename=False, server_side_copy=True, true_directories=False, multipart_write=True,
                                delta_commits="conditional_put")

    def __init__(self, client: Any, bucket: str, prefix: str) -> None:
        self.client, self.bucket = client, bucket
        self.prefix = prefix.strip("/")

    # --- key mapping
    def _key(self, rel: str) -> str:
        rel = normalize_rel(rel)
        return f"{self.prefix}/{rel}".strip("/") if self.prefix else rel

    def _rel(self, key: str) -> str:
        if self.prefix:
            return key[len(self.prefix) + 1:] if key.startswith(self.prefix + "/") else key
        return key

    def physical_uri(self, path: str = "") -> str:
        k = self._key(path)
        return f"s3://{self.bucket}/{k}" if k else f"s3://{self.bucket}"

    def _not_found(self, exc: Exception) -> bool:
        code = getattr(exc, "response", {}).get("Error", {}).get("Code", "")
        return code in ("404", "NoSuchKey", "NotFound", "NoSuchBucket")

    # --- reads
    def list(self, path: str = "", recursive: bool = False) -> Iterator[FileInfo]:
        rel = normalize_rel(path)
        key = self._key(rel)
        prefix = key + "/" if key else ""
        kwargs: dict[str, Any] = {"Bucket": self.bucket, "Prefix": prefix}
        if not recursive:
            kwargs["Delimiter"] = "/"
        token = None
        seen_dirs: set[str] = set()
        while True:
            if token:
                kwargs["ContinuationToken"] = token
            resp = self.client.list_objects_v2(**kwargs)
            for cp in resp.get("CommonPrefixes", []):
                d = self._rel(cp["Prefix"].rstrip("/"))
                if d not in seen_dirs:
                    seen_dirs.add(d)
                    yield FileInfo(path=d, is_dir=True)
            for obj in resp.get("Contents", []):
                k = obj["Key"]
                if k == prefix:
                    continue  # the directory marker itself
                if recursive:
                    # object stores have implicit directories: synthesise every parent below the listed prefix so
                    # recursive listings match the local provider (directories first, then their children)
                    parts = k[len(prefix):].split("/")[:-1]
                    for i in range(1, len(parts) + 1):
                        d = self._rel(prefix + "/".join(parts[:i]))
                        if d not in seen_dirs:
                            seen_dirs.add(d)
                            yield FileInfo(path=d, is_dir=True)
                if k.endswith("/"):  # explicit directory marker
                    continue
                yield FileInfo(path=self._rel(k), is_dir=False, size=int(obj["Size"]),
                               modified=obj["LastModified"].strftime("%Y-%m-%dT%H:%M:%S.%fZ"), etag=obj.get("ETag", "").strip('"'))
            if not resp.get("IsTruncated"):
                return
            token = resp.get("NextContinuationToken")

    def stat(self, path: str) -> FileInfo:
        rel = normalize_rel(path)
        if not rel:
            return FileInfo(path="", is_dir=True)
        key = self._key(rel)
        try:
            h = self.client.head_object(Bucket=self.bucket, Key=key)
            return FileInfo(path=rel, is_dir=False, size=int(h["ContentLength"]),
                            modified=h["LastModified"].strftime("%Y-%m-%dT%H:%M:%S.%fZ"), etag=h.get("ETag", "").strip('"'))
        except Exception as exc:  # noqa: BLE001
            if not self._not_found(exc):
                raise
        resp = self.client.list_objects_v2(Bucket=self.bucket, Prefix=key + "/", MaxKeys=1)
        if resp.get("KeyCount", 0) > 0:
            return FileInfo(path=rel, is_dir=True)
        raise NotFound(f"'{rel}' does not exist")

    def open_read(self, path: str, offset: int = 0, length: int | None = None) -> BinaryIO:
        rel = normalize_rel(path)
        kwargs: dict[str, Any] = {"Bucket": self.bucket, "Key": self._key(rel)}
        if offset or length is not None:
            end = "" if length is None else str(offset + length - 1)
            kwargs["Range"] = f"bytes={offset}-{end}"
        try:
            return _Body(self.client.get_object(**kwargs)["Body"])  # type: ignore[return-value]
        except Exception as exc:  # noqa: BLE001
            if self._not_found(exc):
                raise NotFound(f"'{rel}' does not exist") from None
            if getattr(exc, "response", {}).get("Error", {}).get("Code") == "InvalidRange":
                return _Body(_Empty())  # type: ignore[return-value]
            raise

    # --- writes
    def open_write(self, path: str, overwrite: bool = True) -> BinaryWriter:
        rel = normalize_rel(path)
        if not rel:
            raise BadRequest("cannot write to the lakehouse root")
        if not overwrite and self.exists(rel):
            raise Conflict(f"'{rel}' already exists")
        return _S3Writer(self, self._key(rel), rel)

    def mkdirs(self, path: str) -> None:
        rel = normalize_rel(path)
        if rel:
            self.client.put_object(Bucket=self.bucket, Key=self._key(rel) + "/", Body=b"")

    def _delete_keys(self, keys: list[str]) -> int:
        n = 0
        for i in range(0, len(keys), 1000):
            batch = keys[i:i + 1000]
            self.client.delete_objects(Bucket=self.bucket, Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True})
            n += len(batch)
        return n

    def delete(self, path: str, recursive: bool = False) -> int:
        rel = normalize_rel(path)
        if not rel:
            raise BadRequest("refusing to delete the lakehouse root")
        key = self._key(rel)
        n = 0
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            self.client.delete_object(Bucket=self.bucket, Key=key)
            n = 1
        except Exception as exc:  # noqa: BLE001
            if not self._not_found(exc):
                raise
        children = [o["Key"] for page in self.client.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=key + "/")
                    for o in page.get("Contents", [])]
        if children:
            real = [k for k in children if not k.endswith("/")]
            if real and not recursive:
                raise Conflict(f"directory '{rel}' is not empty", remediation="pass recursive=true to delete it with its contents")
            n += self._delete_keys(children)
        return n

    def move(self, src: str, dst: str, overwrite: bool = False) -> None:
        info = self.stat(src)
        if self.exists(dst) and not overwrite:
            raise Conflict(f"destination '{dst}' already exists")
        self.copy(src, dst, overwrite=True)
        self.delete(src, recursive=info.is_dir)

    def copy(self, src: str, dst: str, overwrite: bool = False) -> int:
        info = self.stat(src)
        if not overwrite and self.exists(dst):
            raise Conflict(f"destination '{dst}' already exists")
        if not info.is_dir:
            self.client.copy({"Bucket": self.bucket, "Key": self._key(src)}, self.bucket, self._key(dst))
            return 1
        n = 0
        for child in self.list(src, recursive=True):
            if child.is_dir:
                continue
            rel = child.path[len(normalize_rel(src)) + 1:]
            self.client.copy({"Bucket": self.bucket, "Key": self._key(child.path)}, self.bucket, self._key(f"{normalize_rel(dst)}/{rel}"))
            n += 1
        return n


class _Empty:
    def read(self, n: int = -1) -> bytes:
        return b""

    def close(self) -> None:
        return None


class S3Provider(StorageProvider):
    """Storage provider for Amazon S3 and S3-compatible object stores."""
    id = "s3"
    label = "Amazon S3 / S3-compatible (MinIO)"
    description = "AWS S3 and any S3-compatible object store: custom endpoint, path-style addressing, optional private CA."
    schemes = ("s3", "s3a")
    connection_types = ("s3", "minio")
    capabilities = S3FileSystem.capabilities
    connection_fields = (
        FieldSpec("bucket", "Bucket", required=True),
        FieldSpec("prefix", "Key prefix", help="Optional prefix below which lakehouses live."),
        FieldSpec("region", "Region", default="us-east-1"),
        FieldSpec("endpoint_url", "Endpoint URL", help="Leave empty for AWS. For MinIO use e.g. https://minio.example.com:9000"),
        FieldSpec("path_style", "Path-style addressing", type="bool", default=False, help="Required by MinIO and most S3-compatible stores."),
        FieldSpec("ca_bundle", "CA bundle file", help="PEM file trusted for the endpoint's TLS certificate."),
        FieldSpec("auth", "Authentication", type="enum", options=("static", "default"), default="static",
                  help="static = access key from the secret; default = ambient AWS credential chain (instance role, IRSA)."),
        FieldSpec("access_key_id", "Access key id / secret access key / session token", type="secret", secret=True,
                  visible_when={"auth": ["static"]}, help="Stored in the secret store as {access_key_id, secret_access_key, session_token?}."),
    )

    # --- configuration
    def validate_root(self, root_uri: str) -> str:
        bucket, prefix = parse_s3_uri(root_uri)
        return f"s3://{bucket}" + (f"/{prefix}" if prefix else "")

    def validate_config(self, config: dict[str, Any]) -> dict[str, Any]:
        ep = config.get("endpoint_url")
        if ep and urlparse(ep).scheme not in ("http", "https"):
            raise BadRequest("endpoint_url must start with http:// or https://")
        if config.get("auth", "static") not in ("static", "default"):
            raise BadRequest("auth must be 'static' or 'default'")
        return config

    def _client(self, config: dict[str, Any], secret: Any) -> Any:
        try:
            import boto3
            from botocore.config import Config
        except ImportError as exc:  # pragma: no cover
            raise Unavailable("the S3 provider needs boto3", remediation="pip install 'olcf[s3]'") from exc
        creds = credentials_from_secret(secret) if config.get("auth", "static") == "static" else {}
        if config.get("auth", "static") == "static" and not creds:
            raise BadRequest("this S3 connection uses static credentials but has no secret",
                             remediation="attach a secret {access_key_id, secret_access_key} or set auth=default")
        cfg = Config(
            signature_version="s3v4", s3={"addressing_style": "path" if config.get("path_style") else "auto"},
            retries={"max_attempts": 5, "mode": "standard"}, connect_timeout=float(config.get("connect_timeout", 10)),
            read_timeout=float(config.get("read_timeout", 60)),
        )
        kw: dict[str, Any] = {"config": cfg, "region_name": config.get("region") or "us-east-1"}
        if config.get("endpoint_url"):
            kw["endpoint_url"] = config["endpoint_url"]
        if config.get("ca_bundle"):
            kw["verify"] = config["ca_bundle"]
        if creds:
            kw.update(aws_access_key_id=creds["access_key_id"], aws_secret_access_key=creds["secret_access_key"],
                      aws_session_token=creds.get("session_token"))
        return boto3.session.Session().client("s3", **kw)

    def filesystem(self, root_uri: str, config: dict[str, Any], secret: Any) -> S3FileSystem:
        bucket, prefix = parse_s3_uri(root_uri)
        return S3FileSystem(self._client(config, secret), bucket, prefix)

    def physical_uri(self, root_uri: str, rel: str = "") -> str:
        bucket, prefix = parse_s3_uri(root_uri)
        rel = normalize_rel(rel)
        key = "/".join(p for p in (prefix, rel) if p)
        return f"s3://{bucket}/{key}" if key else f"s3://{bucket}"

    # --- runtime settings
    def spark_conf(self, root_uri: str, config: dict[str, Any], secret: Any) -> dict[str, str]:
        """S3A settings (Hadoop 3.4 / AWS SDK v2) for Spark 4.x; never returned by an API."""
        creds = credentials_from_secret(secret) if config.get("auth", "static") == "static" else {}
        c: dict[str, str] = {
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.path.style.access": "true" if config.get("path_style") else "false",
            "spark.hadoop.fs.s3a.endpoint.region": config.get("region") or "us-east-1",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false" if _is_http(config.get("endpoint_url")) else "true",
        }
        if config.get("endpoint_url"):
            ep = urlparse(config["endpoint_url"])
            c["spark.hadoop.fs.s3a.endpoint"] = ep.netloc if ep.scheme else config["endpoint_url"]
        if creds:
            c["spark.hadoop.fs.s3a.access.key"] = creds["access_key_id"]
            c["spark.hadoop.fs.s3a.secret.key"] = creds["secret_access_key"]
            if creds.get("session_token"):
                c["spark.hadoop.fs.s3a.session.token"] = creds["session_token"]
                c["spark.hadoop.fs.s3a.aws.credentials.provider"] = "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
            else:
                c["spark.hadoop.fs.s3a.aws.credentials.provider"] = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        else:
            c["spark.hadoop.fs.s3a.aws.credentials.provider"] = "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider"
        return c

    def delta_storage_options(self, root_uri: str, config: dict[str, Any], secret: Any) -> dict[str, str]:
        creds = credentials_from_secret(secret) if config.get("auth", "static") == "static" else {}
        o: dict[str, str] = {"AWS_REGION": config.get("region") or "us-east-1"}
        if creds:
            o["AWS_ACCESS_KEY_ID"] = creds["access_key_id"]
            o["AWS_SECRET_ACCESS_KEY"] = creds["secret_access_key"]
            if creds.get("session_token"):
                o["AWS_SESSION_TOKEN"] = creds["session_token"]
        if config.get("endpoint_url"):
            o["AWS_ENDPOINT_URL"] = config["endpoint_url"]
            if _is_http(config["endpoint_url"]):
                o["AWS_ALLOW_HTTP"] = "true"
        if config.get("path_style") or config.get("endpoint_url"):
            o["AWS_VIRTUAL_HOSTED_STYLE_REQUEST"] = "false"
        mode = config.get("delta_commit_mode", "conditional_put")
        if mode == "conditional_put":
            o["aws_conditional_put"] = "etag"
        elif mode == "unsafe_rename":  # only safe with a single writer
            o["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"
        if config.get("ca_bundle"):
            o["AWS_CA_BUNDLE"] = config["ca_bundle"]
        return o

    def env(self, root_uri: str, config: dict[str, Any], secret: Any) -> dict[str, str]:
        creds = credentials_from_secret(secret) if config.get("auth", "static") == "static" else {}
        e: dict[str, str] = {"AWS_DEFAULT_REGION": config.get("region") or "us-east-1"}
        if creds:
            e["AWS_ACCESS_KEY_ID"] = creds["access_key_id"]
            e["AWS_SECRET_ACCESS_KEY"] = creds["secret_access_key"]
            if creds.get("session_token"):
                e["AWS_SESSION_TOKEN"] = creds["session_token"]
        if config.get("endpoint_url"):
            e["AWS_ENDPOINT_URL"] = config["endpoint_url"]
        if config.get("ca_bundle"):
            e["AWS_CA_BUNDLE"] = config["ca_bundle"]
        return e

    def iceberg_properties(self, root_uri: str, config: dict[str, Any], secret: Any) -> dict[str, str]:
        creds = credentials_from_secret(secret) if config.get("auth", "static") == "static" else {}
        p: dict[str, str] = {"s3.region": config.get("region") or "us-east-1"}
        if config.get("endpoint_url"):
            p["s3.endpoint"] = config["endpoint_url"]
        if creds:
            p["s3.access-key-id"] = creds["access_key_id"]
            p["s3.secret-access-key"] = creds["secret_access_key"]
            if creds.get("session_token"):
                p["s3.session-token"] = creds["session_token"]
        if config.get("path_style"):
            p["s3.force-virtual-addressing"] = "false"
        return p

    # --- connectivity
    def _endpoint_host(self, root_uri: str, config: dict[str, Any]) -> tuple[str, int, bool]:
        ep = config.get("endpoint_url")
        if ep:
            u = urlparse(ep)
            return u.hostname or "", u.port or (443 if u.scheme == "https" else 80), u.scheme == "https"
        bucket, _ = parse_s3_uri(root_uri)
        region = config.get("region") or "us-east-1"
        host = f"{bucket}.s3.amazonaws.com" if region == "us-east-1" else f"{bucket}.s3.{region}.amazonaws.com"
        return host, 443, True

    def test(self, root_uri: str, config: dict[str, Any], secret: Any) -> dict[str, Any]:
        host, port, tls = self._endpoint_host(root_uri, config)
        holder: dict[str, Any] = {}

        def dns() -> str:
            infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
            return f"{host} -> {', '.join(sorted({i[4][0] for i in infos}))}"

        def connect() -> str:
            with socket.create_connection((host, port), timeout=10) as s:
                if tls:
                    ctx = ssl.create_default_context(cafile=config.get("ca_bundle"))
                    with ctx.wrap_socket(s, server_hostname=host) as ss:
                        return f"TCP+TLS established to {host}:{port} ({ss.version()})"
            return f"TCP connection established to {host}:{port} (plain HTTP - not recommended outside a trusted network)"

        def auth() -> str:
            client = self._client(config, secret)
            bucket, _ = parse_s3_uri(root_uri)
            try:
                client.head_bucket(Bucket=bucket)
            except Exception as exc:  # noqa: BLE001
                code = getattr(exc, "response", {}).get("Error", {}).get("Code", "")
                if code in ("404", "NoSuchBucket"):
                    raise OlcfError(f"bucket '{bucket}' does not exist") from None
                if code in ("403", "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch"):
                    raise OlcfError(f"credentials rejected ({code}); check the access key, secret and bucket policy") from None
                raise
            holder["fs"] = S3FileSystem(client, bucket, parse_s3_uri(root_uri)[1])
            return f"bucket '{bucket}' is reachable and the credentials are accepted"

        steps = [("resolve DNS", dns), ("connect", connect), ("auth", auth)] + probe_roundtrip(lambda: holder["fs"])
        return run_steps(steps)
