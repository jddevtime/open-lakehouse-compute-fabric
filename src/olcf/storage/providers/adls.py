# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Azure Data Lake Storage Gen2 (``abfss://``) via ``azure-storage-file-datalake``.

Authentication modes (connection ``config.auth``): ``account_key``, ``sas``, ``service_principal`` (client secret),
``managed_identity`` (optionally user-assigned via ``client_id``) and ``default`` (``DefaultAzureCredential``).
Secret material lives only in the secret store; this module resolves it just-in-time from the value handed in by the
caller.  OneLake (:mod:`olcf.storage.providers.onelake`) reuses everything here: it is the same API on a different
endpoint with workspace/item addressing.
"""

from __future__ import annotations

import socket
import ssl
import time
from collections.abc import Iterator
from datetime import UTC, datetime
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

APPEND_CHUNK = 4 * 1024 * 1024
STORAGE_SCOPE = "https://storage.azure.com/.default"
AUTH_MODES = ("account_key", "sas", "service_principal", "managed_identity", "default")


def parse_abfss(uri: str) -> tuple[str, str, str]:
    """``abfss://<container>@<host>/<path>`` -> ``(container, host, path)``."""
    u = urlparse(uri)
    if u.scheme not in ("abfss", "abfs") or "@" not in u.netloc:
        raise BadRequest(f"expected abfss://<container>@<account>.dfs.core.windows.net/<path>, got '{uri}'")
    container, host = u.netloc.split("@", 1)
    return container, host, u.path.strip("/")


def _secret_value(secret: Any, *names: str) -> str | None:
    if secret in (None, ""):
        return None
    if isinstance(secret, dict):
        for n in names:
            if secret.get(n):
                return str(secret[n])
        return None
    return str(secret)


class StaticTokenCredential:
    """A pre-acquired bearer token (e.g. a user token pasted for OneLake); never refreshed."""

    def __init__(self, token: str, expires_on: int | None = None) -> None:
        self._token, self._exp = token, expires_on or int(time.time()) + 3000

    def get_token(self, *scopes: str, **kwargs: Any):  # noqa: ANN201
        from azure.core.credentials import AccessToken

        return AccessToken(self._token, self._exp)

    def close(self) -> None:
        return None


def _iso(dt: datetime | None) -> str | None:
    return None if dt is None else dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class _AdlsWriter(BinaryWriter):
    def __init__(self, fs: AdlsFileSystem, rel: str) -> None:
        self._fs, self._rel = fs, rel
        self._fc = fs.fsc.get_file_client(fs._full(rel))
        self._fc.create_file()
        self._pos = 0
        self._buf = bytearray()
        self._done = False

    def _append(self, data: bytes) -> None:
        self._fc.append_data(data, offset=self._pos, length=len(data))
        self._pos += len(data)

    def write(self, data: bytes) -> int:
        self._buf.extend(data)
        while len(self._buf) >= APPEND_CHUNK:
            part = bytes(self._buf[:APPEND_CHUNK])
            del self._buf[:APPEND_CHUNK]
            self._append(part)
        return len(data)

    def close(self) -> None:
        if self._done:
            return
        self._done = True
        try:
            if self._buf:
                self._append(bytes(self._buf))
            self._fc.flush_data(self._pos)
        except BaseException:
            self._discard()
            raise

    def _discard(self) -> None:
        try:
            self._fc.delete_file()
        except Exception:  # noqa: BLE001, S110
            pass

    def abort(self) -> None:
        if self._done:
            return
        self._done = True
        self._discard()


class _Reader:
    def __init__(self, downloader: Any) -> None:
        self._d = downloader
        self._it = downloader.chunks()
        self._left = b""

    def read(self, n: int = -1) -> bytes:
        if n is None or n < 0:
            out = [self._left]
            self._left = b""
            out.extend(self._it)
            return b"".join(out)
        while len(self._left) < n:
            try:
                self._left += next(self._it)
            except StopIteration:
                break
        out, self._left = self._left[:n], self._left[n:]
        return out

    def close(self) -> None:
        return None

    def __enter__(self) -> _Reader:
        return self

    def __exit__(self, *a: object) -> None:
        self.close()


class AdlsFileSystem(FileSystem):
    """File system over an Azure Data Lake Storage Gen2 container and prefix."""
    provider = "adls"
    capabilities = Capabilities(atomic_rename=True, server_side_copy=False, true_directories=True, multipart_write=True, acl=True,
                                delta_commits="rename")

    def __init__(self, service: Any, container: str, prefix: str) -> None:
        self.service, self.container = service, container
        self.prefix = prefix.strip("/")
        self.fsc = service.get_file_system_client(container)

    def _full(self, rel: str) -> str:
        rel = normalize_rel(rel)
        return "/".join(p for p in (self.prefix, rel) if p)

    def _rel(self, full: str) -> str:
        full = full.strip("/")
        if self.prefix and full.startswith(self.prefix + "/"):
            return full[len(self.prefix) + 1:]
        return full if not self.prefix else ("" if full == self.prefix else full)

    def physical_uri(self, path: str = "") -> str:
        host = urlparse(self.service.url).hostname
        full = self._full(path)
        return f"abfss://{self.container}@{host}" + (f"/{full}" if full else "")

    @staticmethod
    def _is_missing(exc: Exception) -> bool:
        return exc.__class__.__name__ in ("ResourceNotFoundError",) or getattr(exc, "status_code", None) == 404

    def list(self, path: str = "", recursive: bool = False) -> Iterator[FileInfo]:
        full = self._full(path)
        try:
            for p in self.fsc.get_paths(path=full or None, recursive=recursive):
                rel = self._rel(p.name)
                if not rel:
                    continue
                yield FileInfo(path=rel, is_dir=bool(p.is_directory), size=0 if p.is_directory else int(p.content_length or 0),
                               modified=_iso(p.last_modified), etag=(p.etag or "").strip('"') or None)
        except Exception as exc:  # noqa: BLE001
            if self._is_missing(exc):
                return
            raise

    def stat(self, path: str) -> FileInfo:
        rel = normalize_rel(path)
        full = self._full(rel)
        if not full:
            return FileInfo(path="", is_dir=True)
        try:
            props = self.fsc.get_file_client(full).get_file_properties()
        except Exception as exc:  # noqa: BLE001
            if self._is_missing(exc):
                raise NotFound(f"'{rel}' does not exist") from None
            raise
        is_dir = bool((props.metadata or {}).get("hdi_isfolder") == "true")
        return FileInfo(path=rel, is_dir=is_dir, size=0 if is_dir else int(props.size or 0), modified=_iso(props.last_modified),
                        etag=(props.etag or "").strip('"') or None)

    def open_read(self, path: str, offset: int = 0, length: int | None = None) -> BinaryIO:
        rel = normalize_rel(path)
        try:
            kw: dict[str, Any] = {}
            if offset or length is not None:
                kw = {"offset": offset, "length": length}
            return _Reader(self.fsc.get_file_client(self._full(rel)).download_file(**kw))  # type: ignore[return-value]
        except Exception as exc:  # noqa: BLE001
            if self._is_missing(exc):
                raise NotFound(f"'{rel}' does not exist") from None
            raise

    def open_write(self, path: str, overwrite: bool = True) -> BinaryWriter:
        rel = normalize_rel(path)
        if not rel:
            raise BadRequest("cannot write to the lakehouse root")
        if not overwrite and self.exists(rel):
            raise Conflict(f"'{rel}' already exists")
        return _AdlsWriter(self, rel)

    def mkdirs(self, path: str) -> None:
        full = self._full(path)
        if full:
            self.fsc.get_directory_client(full).create_directory()

    def delete(self, path: str, recursive: bool = False) -> int:
        rel = normalize_rel(path)
        if not rel:
            raise BadRequest("refusing to delete the lakehouse root")
        try:
            info = self.stat(rel)
        except NotFound:
            return 0
        if info.is_dir:
            n = sum(1 for _ in self.list(rel, recursive=True))
            if n and not recursive:
                raise Conflict(f"directory '{rel}' is not empty", remediation="pass recursive=true to delete it with its contents")
            self.fsc.get_directory_client(self._full(rel)).delete_directory()
            return n + 1
        self.fsc.get_file_client(self._full(rel)).delete_file()
        return 1

    def move(self, src: str, dst: str, overwrite: bool = False) -> None:
        info = self.stat(src)
        if self.exists(dst):
            if not overwrite:
                raise Conflict(f"destination '{dst}' already exists")
            self.delete(dst, recursive=True)
        client = (self.fsc.get_directory_client(self._full(src)) if info.is_dir else self.fsc.get_file_client(self._full(src)))
        client.rename_directory(new_name=f"{self.container}/{self._full(dst)}") if info.is_dir else client.rename_file(
            new_name=f"{self.container}/{self._full(dst)}")


class AdlsProvider(StorageProvider):
    """Storage provider for Azure Data Lake Storage Gen2 (``abfss://``)."""
    id = "adls"
    label = "Azure Data Lake Storage Gen2"
    description = "ADLS Gen2 (abfss://) with account key, SAS, service principal, managed identity or default Azure credential."
    schemes = ("abfss", "abfs")
    connection_types = ("adls",)
    capabilities = AdlsFileSystem.capabilities
    #: DNS suffix of the data lake endpoint (overridable for sovereign clouds via config.endpoint_suffix)
    default_suffix = "dfs.core.windows.net"
    connection_fields = (
        FieldSpec("account_name", "Storage account", required=True),
        FieldSpec("filesystem", "Container (file system)", required=True),
        FieldSpec("root_path", "Root path", help="Optional directory below the container that holds lakehouses."),
        FieldSpec("auth", "Authentication", type="enum", required=True, options=AUTH_MODES, default="service_principal"),
        FieldSpec("tenant_id", "Tenant id", visible_when={"auth": ["service_principal"]}),
        FieldSpec("client_id", "Client id", visible_when={"auth": ["service_principal", "managed_identity"]},
                  help="App registration (service principal) or user-assigned managed identity client id."),
        FieldSpec("endpoint_suffix", "Endpoint suffix", default="dfs.core.windows.net", help="Change for sovereign clouds."),
        FieldSpec("endpoint", "Endpoint override", help="Full base URL; only for emulators / private link test setups."),
        FieldSpec("secret", "Account key / SAS token / client secret", type="secret", secret=True,
                  visible_when={"auth": ["account_key", "sas", "service_principal"]},
                  help="Stored in the secret store: account_key -> {account_key}, sas -> {sas_token}, service_principal -> {client_secret}."),
    )

    # --- helpers
    def _auth(self, config: dict[str, Any]) -> str:
        auth = config.get("auth") or "service_principal"
        if auth not in AUTH_MODES:
            raise BadRequest(f"unknown auth mode '{auth}'", details={"supported": list(AUTH_MODES)})
        return auth

    def _account_host(self, root_uri: str, config: dict[str, Any]) -> str:
        return parse_abfss(root_uri)[1]

    def _account_url(self, root_uri: str, config: dict[str, Any]) -> str:
        if config.get("endpoint"):
            return config["endpoint"].rstrip("/")
        return f"https://{self._account_host(root_uri, config)}"

    def _account_name(self, root_uri: str, config: dict[str, Any]) -> str:
        return config.get("account_name") or self._account_host(root_uri, config).split(".")[0]

    def claims(self, root_uri: str) -> bool:
        u = urlparse(root_uri)
        return u.scheme in self.schemes and "fabric.microsoft.com" not in (u.hostname or u.netloc.split("@")[-1])

    def validate_root(self, root_uri: str) -> str:
        container, host, path = parse_abfss(root_uri)
        return f"abfss://{container}@{host}" + (f"/{path}" if path else "")

    def validate_config(self, config: dict[str, Any]) -> dict[str, Any]:
        auth = self._auth(config)
        if auth == "service_principal" and not (config.get("tenant_id") and config.get("client_id")):
            raise BadRequest("service_principal authentication needs tenant_id and client_id in the connection config")
        ep = config.get("endpoint")
        if ep and urlparse(ep).scheme not in ("http", "https"):
            raise BadRequest("endpoint must start with http:// or https://")
        return config

    def credential(self, root_uri: str, config: dict[str, Any], secret: Any) -> Any:
        """Build the Azure SDK credential object for the configured auth mode."""
        auth = self._auth(config)
        if auth == "account_key":
            key = _secret_value(secret, "account_key", "key")
            if not key:
                raise BadRequest("account_key authentication needs a secret {account_key}")
            return {"account_name": self._account_name(root_uri, config), "account_key": key}
        if auth == "sas":
            sas = _secret_value(secret, "sas_token", "sas")
            if not sas:
                raise BadRequest("sas authentication needs a secret {sas_token}")
            from azure.core.credentials import AzureSasCredential

            return AzureSasCredential(sas.lstrip("?"))
        try:
            import azure.identity as ident
        except ImportError as exc:  # pragma: no cover
            raise Unavailable("Entra authentication needs azure-identity", remediation="pip install 'olcf[azure]'") from exc
        if auth == "service_principal":
            cs = _secret_value(secret, "client_secret", "secret")
            if not cs:
                raise BadRequest("service_principal authentication needs a secret {client_secret}")
            kw: dict[str, Any] = {}
            if config.get("authority_host"):
                kw["authority"] = config["authority_host"]
            if config.get("disable_instance_discovery"):
                kw["disable_instance_discovery"] = True
            return ident.ClientSecretCredential(config["tenant_id"], config["client_id"], cs, **kw)
        if auth == "managed_identity":
            return ident.ManagedIdentityCredential(client_id=config.get("client_id")) if config.get("client_id") else ident.ManagedIdentityCredential()
        return ident.DefaultAzureCredential(exclude_interactive_browser_credential=True)

    def service_client(self, root_uri: str, config: dict[str, Any], secret: Any) -> Any:
        try:
            from azure.storage.filedatalake import DataLakeServiceClient
        except ImportError as exc:  # pragma: no cover
            raise Unavailable("the ADLS provider needs azure-storage-file-datalake", remediation="pip install 'olcf[azure]'") from exc
        kw: dict[str, Any] = {}
        if config.get("ca_bundle"):
            kw["connection_verify"] = config["ca_bundle"]
        return DataLakeServiceClient(self._account_url(root_uri, config), credential=self.credential(root_uri, config, secret), **kw)

    def filesystem(self, root_uri: str, config: dict[str, Any], secret: Any) -> AdlsFileSystem:
        container, _host, path = parse_abfss(root_uri)
        return AdlsFileSystem(self.service_client(root_uri, config, secret), container, path)

    def physical_uri(self, root_uri: str, rel: str = "") -> str:
        container, host, path = parse_abfss(root_uri)
        rel = normalize_rel(rel)
        full = "/".join(p for p in (path, rel) if p)
        return f"abfss://{container}@{host}" + (f"/{full}" if full else "")

    # --- runtime settings
    def _hadoop_host(self, root_uri: str) -> str:
        return parse_abfss(root_uri)[1]

    def spark_conf(self, root_uri: str, config: dict[str, Any], secret: Any) -> dict[str, str]:
        """ABFS driver settings keyed by the account host (``fs.azure.*.<host>``)."""
        auth = self._auth(config)
        host = self._hadoop_host(root_uri)
        p = "spark.hadoop."
        c: dict[str, str] = {}
        if auth == "account_key":
            key = _secret_value(secret, "account_key", "key")
            if not key:
                raise BadRequest("account_key authentication needs a secret {account_key}")
            c[f"{p}fs.azure.account.key.{host}"] = key
        elif auth == "sas":
            sas = _secret_value(secret, "sas_token", "sas")
            if not sas:
                raise BadRequest("sas authentication needs a secret {sas_token}")
            c[f"{p}fs.azure.account.auth.type.{host}"] = "SAS"
            c[f"{p}fs.azure.sas.token.provider.type.{host}"] = "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider"
            c[f"{p}fs.azure.sas.fixed.token.{host}"] = sas.lstrip("?")
        elif auth == "service_principal":
            cs = _secret_value(secret, "client_secret", "secret")
            if not cs:
                raise BadRequest("service_principal authentication needs a secret {client_secret}")
            authority = (config.get("authority_host") or "https://login.microsoftonline.com").rstrip("/")
            c[f"{p}fs.azure.account.auth.type.{host}"] = "OAuth"
            c[f"{p}fs.azure.account.oauth.provider.type.{host}"] = "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
            c[f"{p}fs.azure.account.oauth2.client.id.{host}"] = config["client_id"]
            c[f"{p}fs.azure.account.oauth2.client.secret.{host}"] = cs
            c[f"{p}fs.azure.account.oauth2.client.endpoint.{host}"] = f"{authority}/{config['tenant_id']}/oauth2/token"
        elif auth == "managed_identity":
            c[f"{p}fs.azure.account.auth.type.{host}"] = "OAuth"
            c[f"{p}fs.azure.account.oauth.provider.type.{host}"] = "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider"
            if config.get("tenant_id"):
                c[f"{p}fs.azure.account.oauth2.msi.tenant.{host}"] = config["tenant_id"]
            if config.get("client_id"):
                c[f"{p}fs.azure.account.oauth2.client.id.{host}"] = config["client_id"]
        else:
            from olcf.core.errors import Unsupported

            raise Unsupported("Spark cannot use DefaultAzureCredential: the ABFS driver has no equivalent provider",
                              remediation="configure the connection with auth=service_principal or auth=managed_identity for Spark workloads")
        c[f"{p}fs.abfss.impl"] = "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem"
        return c

    def _acquire_token(self, root_uri: str, config: dict[str, Any], secret: Any) -> str:
        cred = self.credential(root_uri, config, secret)
        return cred.get_token(STORAGE_SCOPE).token

    def delta_storage_options(self, root_uri: str, config: dict[str, Any], secret: Any) -> dict[str, str]:
        auth = self._auth(config)
        o: dict[str, str] = {"azure_storage_account_name": self._account_name(root_uri, config)}
        if config.get("endpoint"):
            o["azure_storage_endpoint"] = config["endpoint"]
            if config["endpoint"].startswith("http://"):
                o["allow_http"] = "true"
        if auth == "account_key":
            o["azure_storage_account_key"] = _secret_value(secret, "account_key", "key") or ""
        elif auth == "sas":
            o["azure_storage_sas_token"] = (_secret_value(secret, "sas_token", "sas") or "").lstrip("?")
        elif auth == "service_principal":
            o.update(azure_storage_client_id=config["client_id"], azure_storage_client_secret=_secret_value(secret, "client_secret", "secret") or "",
                     azure_storage_tenant_id=config["tenant_id"])
        else:  # managed identity / default credential: a short-lived bearer token acquired just-in-time
            o["azure_storage_token"] = self._acquire_token(root_uri, config, secret)
        return o

    def env(self, root_uri: str, config: dict[str, Any], secret: Any) -> dict[str, str]:
        e: dict[str, str] = {"AZURE_STORAGE_ACCOUNT": self._account_name(root_uri, config)}
        auth = self._auth(config)
        if auth == "service_principal":
            e.update(AZURE_TENANT_ID=config["tenant_id"], AZURE_CLIENT_ID=config["client_id"],
                     AZURE_CLIENT_SECRET=_secret_value(secret, "client_secret", "secret") or "")
        elif auth == "account_key":
            e["AZURE_STORAGE_KEY"] = _secret_value(secret, "account_key", "key") or ""
        elif auth == "sas":
            e["AZURE_STORAGE_SAS_TOKEN"] = (_secret_value(secret, "sas_token", "sas") or "").lstrip("?")
        return e

    def iceberg_properties(self, root_uri: str, config: dict[str, Any], secret: Any) -> dict[str, str]:
        auth = self._auth(config)
        p = {"adls.account-name": self._account_name(root_uri, config)}
        if auth == "account_key":
            p["adls.account-key"] = _secret_value(secret, "account_key", "key") or ""
        elif auth == "sas":
            p[f"adls.sas-token.{self._account_name(root_uri, config)}"] = (_secret_value(secret, "sas_token", "sas") or "").lstrip("?")
        elif auth == "service_principal":
            p.update({"adls.tenant-id": config["tenant_id"], "adls.client-id": config["client_id"],
                      "adls.client-secret": _secret_value(secret, "client_secret", "secret") or ""})
        return p

    # --- connectivity
    def _host_port(self, root_uri: str, config: dict[str, Any]) -> tuple[str, int, bool]:
        u = urlparse(self._account_url(root_uri, config))
        return u.hostname or "", u.port or (443 if u.scheme == "https" else 80), u.scheme == "https"

    def test(self, root_uri: str, config: dict[str, Any], secret: Any) -> dict[str, Any]:
        host, port, tls = self._host_port(root_uri, config)
        auth = self._auth(config)
        holder: dict[str, Any] = {}

        def dns() -> str:
            infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
            return f"{host} -> {', '.join(sorted({i[4][0] for i in infos}))}"

        def connect() -> str:
            with socket.create_connection((host, port), timeout=10) as s:
                if tls:
                    sc = ssl.create_default_context(cafile=config.get("ca_bundle"))
                    with sc.wrap_socket(s, server_hostname=host) as ss:
                        return f"TCP+TLS established to {host}:{port} ({ss.version()})"
            return f"TCP connection established to {host}:{port}"

        def authenticate() -> str:
            svc = self.service_client(root_uri, config, secret)
            container, _h, path = parse_abfss(root_uri)
            cred = self.credential(root_uri, config, secret)
            detail = f"{auth} credential constructed"
            if hasattr(cred, "get_token"):
                tok = cred.get_token(STORAGE_SCOPE)
                detail = f"{auth}: Entra token acquired (expires in {max(0, tok.expires_on - int(time.time()))} s)"
            try:
                svc.get_file_system_client(container).get_file_system_properties()
            except Exception as exc:  # noqa: BLE001
                status = getattr(exc, "status_code", None)
                if status in (401, 403):
                    raise OlcfError(f"storage rejected the credentials (HTTP {status}); check role assignments (Storage Blob Data Contributor) or key/SAS validity") from None
                if status == 404:
                    raise OlcfError(f"container '{container}' does not exist") from None
                raise
            holder["fs"] = AdlsFileSystem(svc, container, path)
            return detail + f"; container '{container}' is accessible"

        steps = [("resolve DNS", dns), ("connect", connect), ("auth", authenticate)] + probe_roundtrip(lambda: holder["fs"])
        return run_steps(steps)
