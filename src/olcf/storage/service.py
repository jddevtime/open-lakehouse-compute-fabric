# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``StorageService`` - the platform's single entry point to lakehouse storage (CONTRACTS section 3).

Callers pass a lakehouse as a :class:`~olcf.storage.lakehouse.Lakehouse`, an item row, an item id, or ``"<workspace>/<name>"``.
The service performs **no authorisation** itself (internal trusted callers such as the compute layer need
``spark_conf``); every REST/MCP/SQL entry point authorises with ``principal.require(..., workspace_id=<stored object's
workspace>)`` first - see :func:`StorageService.authorize`.

Credentials are resolved just-in-time from the connection's secret via ``ctx.secrets.resolve`` and are never cached
on disk, never persisted and never returned by an API.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, BinaryIO

from olcf.core.context import AppContext
from olcf.core.errors import BadRequest, Conflict, NotFound
from olcf.security.rbac import Principal
from olcf.storage import paths as P
from olcf.storage.lakehouse import Lakehouse, default_local_root
from olcf.storage.providers import ProviderRegistry
from olcf.storage.providers.base import BinaryWriter, FileInfo, FileSystem, StorageProvider

log = logging.getLogger("olcf.storage")
FS_CACHE_TTL = 120.0


@dataclass(frozen=True)
class ResolvedLocation:
    """Result of :meth:`StorageService.resolve`.

    ``relative_path`` is relative to ``area`` (``olcf://ws/lh/Files/raw/a.csv`` -> area ``Files``, relative_path ``raw/a.csv``);
    ``root_relative`` is the path from the lakehouse root.
    """

    physical_uri: str
    provider: str
    lakehouse_id: str
    area: str
    relative_path: str
    canonical_uri: str = ""

    @property
    def root_relative(self) -> str:
        return f"{self.area}/{self.relative_path}".rstrip("/") if self.area else ""


class StorageService:
    """Resolves lakehouses and connections to file systems through the provider registry and caches them."""
    def __init__(self, ctx: AppContext, registry: ProviderRegistry | None = None) -> None:
        self.ctx = ctx
        self.registry = registry or ProviderRegistry()
        self._fs_cache: dict[tuple[Any, ...], tuple[float, FileSystem]] = {}
        self._lock = threading.Lock()
        from olcf.storage.tables import TableService

        self.tables = TableService(self)

    # ------------------------------------------------------------------ lakehouse lookup
    def lakehouse_from_item(self, item: dict[str, Any]) -> Lakehouse:
        if item.get("type") != "lakehouse":
            raise NotFound(f"item {item.get('id')} is not a lakehouse")
        d = item.get("definition") or {}
        storage = d.get("storage") or {}
        ws = self.ctx.service("workspaces").resolve(item["workspace_id"], include_deleted=True)
        root = storage.get("root_uri") or default_local_root(self.ctx, ws["name"], item["name"])
        conn = None
        if storage.get("connection_id"):
            conn = self.ctx.service("connections").get(storage["connection_id"])
            if conn["workspace_id"] not in (None, item["workspace_id"]):
                raise BadRequest("the lakehouse references a connection that belongs to another workspace")
        provider = self.registry.for_root(root)
        return Lakehouse(id=item["id"], name=item["name"], workspace_id=item["workspace_id"], workspace_name=ws["name"], root_uri=root,
                         provider_id=provider.id, connection=conn, definition=d, item=item)

    def get_lakehouse(self, ref: Any, *, workspace_id: str | None = None) -> Lakehouse:
        """Resolve an id, ``"<workspace>/<name>"``, bare name (needs ``workspace_id``), item row or ``Lakehouse``."""
        if isinstance(ref, Lakehouse):
            return ref
        items = self.ctx.service("items")
        if isinstance(ref, dict):
            return self.lakehouse_from_item(ref)
        if not isinstance(ref, str) or not ref:
            raise BadRequest("lakehouse reference required")
        if ref.startswith("itm_"):
            return self.lakehouse_from_item(items.get(ref))
        if "/" in ref:
            wsref, name = ref.split("/", 1)
            ws = self.ctx.service("workspaces").resolve(wsref)
            return self.lakehouse_from_item(items.resolve(ws["id"], name, "lakehouse"))
        if not workspace_id:
            raise BadRequest(f"lakehouse '{ref}' is ambiguous without a workspace", remediation="use '<workspace>/<lakehouse>' or pass workspace_id")
        return self.lakehouse_from_item(items.resolve(workspace_id, ref, "lakehouse"))

    def authorize(self, principal: Principal, lh: Lakehouse, perm: str = "lakehouse.read") -> Lakehouse:
        """Raise ``Forbidden`` unless ``principal`` holds ``perm`` for the lakehouse's *stored* workspace."""
        principal.require(perm, workspace_id=lh.workspace_id, item_id=lh.id)
        return lh

    def list_lakehouses(self, workspace_id: str) -> list[Lakehouse]:
        rows, _ = self.ctx.service("items").list(workspace_id, type_="lakehouse", limit=500, with_definition=True)
        return [self.lakehouse_from_item(r) for r in rows]

    # ------------------------------------------------------------------ provider plumbing
    def provider(self, lh: Lakehouse) -> StorageProvider:
        return self.registry.get(lh.provider_id)

    def _conn_parts(self, lh: Lakehouse) -> tuple[dict[str, Any], Any]:
        conn = lh.connection
        config = dict((conn or {}).get("config") or {})
        secret = None
        if conn and conn.get("secret_id"):
            secret = self.ctx.secrets.resolve(conn["secret_id"])  # just-in-time; never stored
        return config, secret

    def filesystem(self, lh: Any) -> FileSystem:
        lh = self.get_lakehouse(lh)
        key = lh.cache_key
        now = time.monotonic()
        with self._lock:
            hit = self._fs_cache.get(key)
            if hit and hit[0] > now:
                return hit[1]
            if len(self._fs_cache) > 64:
                self._fs_cache = {k: v for k, v in self._fs_cache.items() if v[0] > now}
        config, secret = self._conn_parts(lh)
        fs = self.provider(lh).filesystem(lh.root_uri, config, secret)
        with self._lock:
            self._fs_cache[key] = (now + FS_CACHE_TTL, fs)
        return fs

    def invalidate(self, lakehouse_id: str | None = None) -> None:
        with self._lock:
            if lakehouse_id is None:
                self._fs_cache.clear()
            else:
                self._fs_cache = {k: v for k, v in self._fs_cache.items() if k[0] != lakehouse_id}

    # ------------------------------------------------------------------ file API (contract)
    def list_files(self, lh: Any, path: str = "", recursive: bool = False) -> list[FileInfo]:
        return list(self.filesystem(lh).list(P.normalize_rel(path), recursive))

    def stat(self, lh: Any, path: str) -> FileInfo:
        return self.filesystem(lh).stat(P.normalize_rel(path))

    def exists(self, lh: Any, path: str) -> bool:
        return self.filesystem(lh).exists(P.normalize_rel(path))

    def read_bytes(self, lh: Any, path: str, max_bytes: int | None = None) -> bytes:
        return self.filesystem(lh).read_bytes(P.normalize_rel(path), max_bytes)

    def open_read(self, lh: Any, path: str, offset: int = 0, length: int | None = None) -> BinaryIO:
        return self.filesystem(lh).open_read(P.normalize_rel(path), offset, length)

    def write_bytes(self, lh: Any, path: str, data: bytes, overwrite: bool = True) -> None:
        self.filesystem(lh).write_bytes(P.normalize_rel(path), data, overwrite)

    def open_write(self, lh: Any, path: str, overwrite: bool = True) -> BinaryWriter:
        return self.filesystem(lh).open_write(P.normalize_rel(path), overwrite)

    def delete(self, lh: Any, path: str, recursive: bool = False) -> int:
        return self.filesystem(lh).delete(P.normalize_rel(path), recursive)

    def move(self, lh: Any, src: str, dst: str, overwrite: bool = False) -> None:
        self.filesystem(lh).move(P.normalize_rel(src), P.normalize_rel(dst), overwrite)

    def copy(self, lh: Any, src: str, dst: str, overwrite: bool = False) -> int:
        return self.filesystem(lh).copy(P.normalize_rel(src), P.normalize_rel(dst), overwrite)

    def mkdirs(self, lh: Any, path: str) -> None:
        self.filesystem(lh).mkdirs(P.normalize_rel(path))

    # ------------------------------------------------------------------ tables (contract)
    def list_tables(self, lh: Any) -> list[dict[str, Any]]:
        return self.tables.discover(self.get_lakehouse(lh))

    def table_info(self, lh: Any, name: str) -> dict[str, Any]:
        return self.tables.info(self.get_lakehouse(lh), name)

    # ------------------------------------------------------------------ runtime settings (compute layer only)
    def spark_conf(self, lh: Any) -> dict[str, str]:
        """Spark-ready (``spark.hadoop.*``) storage settings with credentials resolved now. **Never return via an API.**"""
        lh = self.get_lakehouse(lh)
        config, secret = self._conn_parts(lh)
        return self.provider(lh).spark_conf(lh.root_uri, config, secret)

    def hadoop_conf(self, lh: Any) -> dict[str, str]:
        """Same as :meth:`spark_conf` with the ``spark.hadoop.`` prefix removed (bare Hadoop keys)."""
        return {k[len("spark.hadoop."):] if k.startswith("spark.hadoop.") else k: v for k, v in self.spark_conf(lh).items()}

    def delta_storage_options(self, lh: Any) -> dict[str, str]:
        """``storage_options`` for delta-rs (also usable with object_store based tools). **Never return via an API.**"""
        lh = self.get_lakehouse(lh)
        config, secret = self._conn_parts(lh)
        return self.provider(lh).delta_storage_options(lh.root_uri, config, secret)

    def storage_env(self, lh: Any) -> dict[str, str]:
        """Environment variables (e.g. ``AWS_*``) for Python-side clients inside a job. **Never return via an API.**"""
        lh = self.get_lakehouse(lh)
        config, secret = self._conn_parts(lh)
        return self.provider(lh).env(lh.root_uri, config, secret)

    def iceberg_properties(self, lh: Any) -> dict[str, str]:
        lh = self.get_lakehouse(lh)
        config, secret = self._conn_parts(lh)
        return self.provider(lh).iceberg_properties(lh.root_uri, config, secret)

    def job_storage_conf(self, lh: Any) -> dict[str, dict[str, str]]:
        """``{"spark": {...}, "env": {...}}`` exactly as placed in the (0600, deleted-at-job-end) ``storage_conf`` of a job spec."""
        return {"spark": self.spark_conf(lh), "env": self.storage_env(lh)}

    def job_lakehouse_descriptor(self, lh: Any) -> dict[str, Any]:
        """The credential-free ``default_lakehouse`` / ``lakehouses[]`` block of a job spec (CONTRACTS section 4.3)."""
        lh = self.get_lakehouse(lh)
        p = self.provider(lh)
        return {"id": lh.id, "name": lh.name, "workspace_id": lh.workspace_id, "root_uri": p.physical_uri(lh.root_uri),
                "tables_uri": p.physical_uri(lh.root_uri, "Tables"), "files_uri": p.physical_uri(lh.root_uri, "Files"),
                "format": lh.default_format, "fabric": lh.fabric}

    # ------------------------------------------------------------------ URIs
    def physical_uri(self, lh: Any, rel: str = "") -> str:
        lh = self.get_lakehouse(lh)
        return self.provider(lh).physical_uri(lh.root_uri, P.normalize_rel(rel))

    def resolve(self, uri: str, default_lakehouse: Any = None, *, workspace_id: str | None = None) -> ResolvedLocation:
        """Resolve ``olcf://<ws>/<lakehouse>/Files|Tables/<path>`` (or a relative ``Files/..`` path) to a physical location."""
        ref = P.parse_canonical(uri)
        if ref.lakehouse is None:
            if default_lakehouse is None:
                raise BadRequest(f"'{uri}' is relative but no default lakehouse was provided",
                                 remediation="use a full olcf://<workspace>/<lakehouse>/... URI or attach a default lakehouse")
            lh = self.get_lakehouse(default_lakehouse, workspace_id=workspace_id)
        else:
            ws = self.ctx.service("workspaces").resolve(ref.workspace or "")
            lh = self.lakehouse_from_item(self.ctx.service("items").resolve(ws["id"], ref.lakehouse, "lakehouse"))
        rel = ref.rel
        return ResolvedLocation(physical_uri=self.provider(lh).physical_uri(lh.root_uri, rel), provider=lh.provider_id, lakehouse_id=lh.id,
                                area=ref.area, relative_path=ref.path, canonical_uri=P.canonical_uri(lh.workspace_name, lh.name, rel))

    # ------------------------------------------------------------------ tests / health
    def test(self, target: Any, *, workspace_id: str | None = None) -> dict[str, Any]:
        """Stepwise connectivity test for a lakehouse or a connection row: ``{ok, steps:[{name, ok, detail, ms}]}``."""
        if isinstance(target, dict) and "type" in target and "config" in target and "definition" not in target:
            return self.test_connection(target)
        lh = self.get_lakehouse(target, workspace_id=workspace_id)
        config, secret = self._conn_parts(lh)
        return self.provider(lh).test(lh.root_uri, config, secret)

    def test_connection(self, conn: dict[str, Any], secret: Any = None, *, resolve_secret: bool = True) -> dict[str, Any]:
        from olcf.storage.lakehouse import connection_root

        provider = self.registry.for_connection_type(conn["type"])
        if provider is None:
            return {"ok": False, "steps": [], "detail": f"no storage provider handles connection type '{conn['type']}'"}
        config = dict(conn.get("config") or {})
        if secret is None and resolve_secret and conn.get("secret_id"):
            secret = self.ctx.secrets.resolve(conn["secret_id"])
        try:
            provider.validate_config(config)
            root = connection_root(provider, config)
            if provider.id == "local" and not config.get("root"):
                root = default_local_root(self.ctx, "_connectivity", "probe")
            if not root:
                raise BadRequest("the connection config does not describe a storage root",
                                 remediation="provide bucket (S3), account_name+filesystem (ADLS) or workspace+lakehouse (OneLake)")
            provider.validate_root(root)
        except Exception as exc:  # noqa: BLE001
            msg = exc.message if hasattr(exc, "message") else str(exc)
            return {"ok": False, "detail": "invalid configuration: " + msg,
                    "steps": [{"name": "validate configuration", "ok": False, "detail": msg, "ms": 0.0}]}
        res = provider.test(root, config, secret)
        res["detail"] = ("all steps passed" if res["ok"] else "failed at: " + next((s["name"] + " - " + s["detail"] for s in res["steps"] if not s["ok"] and not s.get("skipped")), "unknown step"))
        return res

    def describe_providers(self) -> list[dict[str, Any]]:
        out = [p.describe() for p in self.registry.all()]
        return out

    def default_root_for(self, workspace_name: str, lakehouse_name: str) -> str:
        return default_local_root(self.ctx, workspace_name, lakehouse_name)

    def ensure_not_exists(self, lh: Any, path: str) -> None:
        if self.exists(lh, path):
            raise Conflict(f"'{path}' already exists")
