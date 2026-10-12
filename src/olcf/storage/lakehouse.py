# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The lakehouse item: definition validation, item hooks and the :class:`Lakehouse` value object.

A lakehouse is an *item* (``type = "lakehouse"``) whose definition points at a storage root::

    {"storage": {"connection_id": str|null, "root_uri": str}, "default_format": "delta",
     "schema_enabled": false, "catalog": "default", "fabric": {"workspace_id": ..., "lakehouse_id": ...}|null}

Security-relevant rules enforced here (the item service calls ``validate`` on every create/update):

* non-local roots **must** reference a connection (no ambient server credentials for arbitrary buckets);
* local roots must lie inside the platform's local storage root, the root of the referenced administrator-created
  ``local`` connection, or a root allow-listed by the ``storage.local_allowed_roots`` platform setting - otherwise any
  contributor could point a lakehouse at ``/etc``;
* the connection must be shared or belong to the lakehouse's workspace (checked when the lakehouse is opened, because
  the validate hook does not know the workspace).
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

from sqlalchemy import update

from olcf.core import refs
from olcf.core.context import AppContext
from olcf.core.errors import Forbidden, NotFound, ValidationFailed
from olcf.core.timeutil import now_iso
from olcf.db import schema as S
from olcf.security.rbac import Principal
from olcf.storage.paths import canonical_uri, slug
from olcf.storage.providers import ProviderRegistry
from olcf.storage.providers.local import uri_to_path

log = logging.getLogger("olcf.storage")

DEFINITION_KEYS = {"storage", "default_format", "schema_enabled", "catalog", "fabric", "description"}
FORMATS = ("delta", "iceberg", "parquet")
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")


@dataclass
class Lakehouse:
    """A resolved lakehouse (item + connection + provider). Contains no secret material."""

    id: str
    name: str
    workspace_id: str
    workspace_name: str
    root_uri: str
    provider_id: str
    connection: dict[str, Any] | None
    definition: dict[str, Any]
    item: dict[str, Any] = field(repr=False, default_factory=dict)

    @property
    def schema_enabled(self) -> bool:
        return bool(self.definition.get("schema_enabled"))

    @property
    def default_format(self) -> str:
        return self.definition.get("default_format") or "delta"

    @property
    def catalog(self) -> str:
        return self.definition.get("catalog") or "default"

    @property
    def fabric(self) -> dict[str, Any] | None:
        return self.definition.get("fabric")

    @property
    def canonical(self) -> str:
        return canonical_uri(self.workspace_name, self.name)

    @property
    def cache_key(self) -> tuple[Any, ...]:
        c = self.connection or {}
        return (self.id, self.item.get("etag"), c.get("id"), c.get("etag"), c.get("secret_id"))

    def to_public(self) -> dict[str, Any]:
        d = self.definition
        return {
            "id": self.id, "name": self.name, "workspace_id": self.workspace_id, "workspace_name": self.workspace_name,
            "root_uri": self.root_uri, "provider": self.provider_id,
            "connection_id": (d.get("storage") or {}).get("connection_id"), "default_format": self.default_format,
            "schema_enabled": self.schema_enabled, "catalog": self.catalog, "fabric": self.fabric, "canonical_uri": self.canonical,
            "description": self.item.get("description", ""), "created_at": self.item.get("created_at"), "updated_at": self.item.get("updated_at"),
            "etag": self.item.get("etag"),
        }


def default_local_root(ctx: AppContext, workspace_name: str, lakehouse_name: str) -> str:
    """``file://<data_dir>/storage/lakehouses/<workspace>/<lakehouse>.Lakehouse`` (names slugged for the filesystem)."""
    base = ctx.settings.local_storage_root / "lakehouses" / slug(workspace_name) / f"{slug(lakehouse_name)}.Lakehouse"
    return "file://" + str(base)


def _inside(path: str, root: str) -> bool:
    p, r = os.path.realpath(path), os.path.realpath(root)
    try:
        return os.path.commonpath([p, r]) == r
    except ValueError:
        return False


def local_allowed_roots(ctx: AppContext, connection: dict[str, Any] | None) -> list[str]:
    roots = [str(ctx.settings.local_storage_root)]
    if connection and connection.get("type") == "local" and (connection.get("config") or {}).get("root"):
        roots.append(str(connection["config"]["root"]))
    try:
        extra = ctx.service("settings").get("storage.local_allowed_roots") or []
        roots += [str(x) for x in extra]
    except Exception:  # noqa: BLE001
        pass
    return roots


def workspace_local_roots(ctx: AppContext, workspace_id: str, connection: dict[str, Any] | None) -> list[str]:
    """Local directories a lakehouse of ``workspace_id`` may use: its own tree under the storage root, the root of the (platform-created) ``local`` connection it
    references and the roots an administrator allow-listed.  **Never** the whole storage root - that holds every other tenant's lakehouses."""
    ws = ctx.service("workspaces").resolve(workspace_id)
    roots = [str(ctx.settings.local_storage_root / "lakehouses" / slug(ws["name"]))]
    if connection and connection.get("type") == "local" and (connection.get("config") or {}).get("root"):
        roots.append(str(connection["config"]["root"]))
    try:
        roots += [str(x) for x in (ctx.service("settings").get("storage.local_allowed_roots") or [])]
    except Exception:  # noqa: BLE001
        pass
    return roots


def _mountable_by(ctx: AppContext, principal: Principal | None, path: str, exclude_id: str | None = None) -> bool:
    """``True`` when ``principal`` may already read **and write** an existing lakehouse whose local root contains ``path`` (export / import round trips of one's own data)."""
    if principal is None:
        return False
    from sqlalchemy import select

    with ctx.db.connect() as conn:
        rows = conn.execute(select(S.items.c.id, S.items.c.workspace_id, S.items.c.definition).where(S.items.c.type == "lakehouse", S.items.c.deleted_at.is_(None))).all()
    for item_id, ws_id, definition in rows:
        root = str(((definition or {}).get("storage") or {}).get("root_uri") or "")
        if item_id != exclude_id and root.startswith("file://") and _inside(path, str(uri_to_path(root))) \
                and principal.can("lakehouse.read", workspace_id=ws_id, item_id=item_id) and principal.can("lakehouse.write", workspace_id=ws_id, item_id=item_id):
            return True
    return False


def check_local_scope(ctx: AppContext, workspace_id: str, storage: dict[str, Any], principal: Principal | None = None, item_id: str | None = None) -> None:
    """Tenant isolation for ``file://`` roots (the definition validator does not know the workspace, this does).

    Allowed: the workspace's own folder, the root of a platform-created ``local`` connection, administrator allow-listed roots, and a directory that ``principal``
    can already read and write through another lakehouse.  Never "anywhere inside the storage root": that holds every other tenant's data.
    """
    root = str(storage.get("root_uri") or "")
    if not root.startswith("file://"):
        return
    conn_id = storage.get("connection_id")
    conn_row = ctx.service("connections").get(conn_id) if conn_id else None
    path = str(uri_to_path(root))
    allowed = workspace_local_roots(ctx, workspace_id, conn_row)
    if not any(_inside(path, r) for r in allowed) and not _mountable_by(ctx, principal, path, item_id):
        raise ValidationFailed("a local lakehouse root must be inside this workspace's own storage folder or an administrator-approved root",
                               remediation="omit root_uri to use the default location, or ask a platform administrator for a 'local' connection / the 'storage.local_allowed_roots' setting",
                               details={"allowed_roots": allowed})


def validate_definition(ctx: AppContext, registry: ProviderRegistry, definition: dict[str, Any]) -> dict[str, Any]:
    """Item ``validate`` hook: normalise and check a lakehouse definition."""
    if not isinstance(definition, dict):
        raise ValidationFailed("lakehouse definition must be an object")
    unknown = set(definition) - DEFINITION_KEYS
    if unknown:
        raise ValidationFailed(f"unknown lakehouse definition fields: {sorted(unknown)}",
                               remediation="allowed: storage, default_format, schema_enabled, catalog, fabric")
    storage = dict(definition.get("storage") or {})
    bad = set(storage) - {"connection_id", "root_uri", "options"}
    if bad:
        raise ValidationFailed(f"unknown storage fields: {sorted(bad)}")
    root = (storage.get("root_uri") or "").strip()
    connection_id = storage.get("connection_id") or None
    conn_row: dict[str, Any] | None = None
    if connection_id:
        try:
            conn_row = ctx.service("connections").get(connection_id)
        except NotFound:
            raise ValidationFailed(f"connection '{connection_id}' does not exist") from None
    out: dict[str, Any] = {
        "storage": {"connection_id": connection_id, "root_uri": ""},
        "default_format": definition.get("default_format") or "delta",
        "schema_enabled": bool(definition.get("schema_enabled", False)),
        "catalog": definition.get("catalog") or "default",
        "fabric": definition.get("fabric") or None,
    }
    if definition.get("description"):
        out["description"] = str(definition["description"])
    if out["default_format"] not in FORMATS:
        raise ValidationFailed(f"default_format must be one of {FORMATS}")
    if not _IDENT.match(out["catalog"]):
        raise ValidationFailed("catalog must be an identifier (letters, digits, underscore)")
    fab = out["fabric"]
    if fab is not None:
        if not isinstance(fab, dict) or not {"workspace_id", "lakehouse_id"} <= set(fab):
            raise ValidationFailed("fabric must be {workspace_id, lakehouse_id} or null")
        out["fabric"] = {"workspace_id": str(fab["workspace_id"]), "lakehouse_id": str(fab["lakehouse_id"])}
    if not root:
        if conn_row is not None and conn_row["type"] != "local":
            provider = registry.for_connection_type(conn_row["type"])
            if provider is None:
                raise ValidationFailed(f"connection type '{conn_row['type']}' is not a storage connection")
            # a connection may carry a ready-made root (onelake: workspace+lakehouse; s3: bucket/prefix)
            root = _root_from_connection(provider, conn_row)
        if not root:
            return out  # default local root is assigned by on_create once workspace and name are known
    if root.startswith("/"):
        root = "file://" + root
    if not urlparse(root).scheme:
        raise ValidationFailed("root_uri must include a scheme (file://, s3://, abfss://)")
    provider = registry.for_root(root)
    root = provider.validate_root(root)
    if provider.requires_connection and not conn_row:
        raise ValidationFailed(f"a {provider.label} lakehouse requires a storage connection",
                               remediation="create a connection (POST /v1/connections) and pass storage.connection_id")
    if conn_row is not None:
        allowed_types = provider.connection_types
        if conn_row["type"] not in allowed_types:
            raise ValidationFailed(f"connection '{conn_row['name']}' has type '{conn_row['type']}' but the root URI needs {list(allowed_types)}")
        provider.validate_config(conn_row.get("config") or {})
    if provider.id == "local":
        path = str(uri_to_path(root))
        if not any(_inside(path, r) for r in local_allowed_roots(ctx, conn_row)):
            raise ValidationFailed(
                "local lakehouse roots must be inside the platform storage root or an administrator-approved root",
                remediation="create a 'local' connection with the desired root (connection.manage) or add the directory to the "
                            "'storage.local_allowed_roots' setting",
                details={"allowed_roots": local_allowed_roots(ctx, conn_row)})
    out["storage"]["root_uri"] = root
    return out


def _root_from_connection(provider: Any, conn_row: dict[str, Any]) -> str:
    cfg = conn_row.get("config") or {}
    if provider.id == "onelake" and cfg.get("workspace") and cfg.get("lakehouse"):
        from olcf.storage.providers.onelake import build_onelake_root

        return build_onelake_root(cfg["workspace"], cfg["lakehouse"])
    if provider.id == "s3" and cfg.get("bucket"):
        return f"s3://{cfg['bucket']}" + (f"/{cfg['prefix'].strip('/')}" if cfg.get("prefix") else "")
    if provider.id == "adls" and cfg.get("filesystem") and cfg.get("account_name"):
        suffix = cfg.get("endpoint_suffix") or "dfs.core.windows.net"
        path = (cfg.get("root_path") or "").strip("/")
        return f"abfss://{cfg['filesystem']}@{cfg['account_name']}.{suffix}" + (f"/{path}" if path else "")
    return ""


def connection_root(provider: Any, config: dict[str, Any]) -> str:
    """Root URI described by a connection's own config (used by the connection testers)."""
    row = {"config": config, "type": provider.connection_types[0]}
    root = _root_from_connection(provider, row)
    if not root and provider.id == "local":
        root = "file://" + str(config.get("root", ""))
    return root


def _owner_principal(ctx: AppContext, owner_id: str | None) -> Principal | None:
    if not owner_id:
        return None
    prow = ctx.service("identity").get(owner_id)
    if not prow or not prow.get("is_active"):
        return None
    return ctx.authz.principal_from_row(prow)


def install_hooks(ctx: AppContext, registry: ProviderRegistry) -> None:
    """Register the lakehouse ``validate`` / ``on_create`` / ``on_delete`` item hooks."""

    def validate(definition: dict[str, Any]) -> dict[str, Any]:
        return validate_definition(ctx, registry, definition)

    def on_create(item: dict[str, Any]) -> None:
        definition = dict(item["definition"])
        storage = dict(definition.get("storage") or {})
        conn_id = storage.get("connection_id")
        # connection.use is checked against the item's creator (hooks do not receive the caller)
        if conn_id:
            conn = ctx.service("connections").get(conn_id)
            owner = _owner_principal(ctx, item.get("owner_id"))
            ok = owner is not None and (
                owner.can("connection.use", workspace_id=item["workspace_id"], connection_id=conn_id)
                or (conn["workspace_id"] is None and owner.can("connection.use", connection_id=conn_id))
                or owner.can("connection.manage", workspace_id=item["workspace_id"]))
            if conn["workspace_id"] not in (None, item["workspace_id"]) or not ok:
                _purge_item(ctx, item)
                raise Forbidden(f"you may not use connection '{conn['name']}' for a lakehouse in this workspace",
                                remediation="ask a workspace administrator for the 'connection.use' permission")
        try:
            check_local_scope(ctx, item["workspace_id"], storage, _owner_principal(ctx, item.get("owner_id")), item["id"])
        except ValidationFailed:
            _purge_item(ctx, item)
            raise
        if not storage.get("root_uri"):
            ws = ctx.service("workspaces").resolve(item["workspace_id"])
            storage["root_uri"] = default_local_root(ctx, ws["name"], item["name"])
            definition["storage"] = storage
            item["definition"] = definition
            with ctx.db.begin() as conn:
                conn.execute(update(S.items).where(S.items.c.id == item["id"]).values(definition=definition))
                conn.execute(update(S.item_versions).where(S.item_versions.c.item_id == item["id"], S.item_versions.c.version == 1)
                             .values(definition=definition))
        try:
            svc = ctx.service("storage")
            lh = svc.lakehouse_from_item(item)
            fs = svc.filesystem(lh)
            fs.mkdirs("Files")
            fs.mkdirs("Tables")
        except Exception as exc:  # noqa: BLE001 - creating the folders is best-effort for remote stores (permissions may come later)
            log.warning("lakehouse %s created but Files/Tables folders could not be initialised: %s", item["id"], exc)
        ctx.events.publish("lakehouse.created", {"name": item["name"], "root_uri": storage.get("root_uri")}, workspace_id=item["workspace_id"], subject=item["id"])

    def on_delete(item: dict[str, Any]) -> None:
        # Soft delete: data stays in storage (the item can be restored); the catalog forgets the tables.
        cat = ctx.maybe_service("catalog")
        if cat is not None:
            try:
                cat.on_lakehouse_deleted(item["id"])
            except Exception:  # noqa: BLE001
                log.exception("catalog cleanup failed for lakehouse %s", item["id"])
        ctx.events.publish("lakehouse.deleted", {"name": item["name"], "data_retained": True}, workspace_id=item["workspace_id"], subject=item["id"])

    def on_update(principal: Principal, old: dict[str, Any], new: dict[str, Any]) -> None:
        """Re-pointing a lakehouse to another connection is *using* that connection: the caller needs ``connection.use`` (or ``connection.manage``) for it."""
        old_conn = ((old.get("definition") or {}).get("storage") or {}).get("connection_id")
        new_storage = (new.get("definition") or {}).get("storage") or {}
        new_conn = new_storage.get("connection_id")
        if new_storage.get("root_uri") != ((old.get("definition") or {}).get("storage") or {}).get("root_uri") or new_conn != old_conn:
            check_local_scope(ctx, old["workspace_id"], new_storage, principal, old.get("id"))
        if not new_conn or new_conn == old_conn:
            return
        conn = ctx.service("connections").get(new_conn)
        ws_id = old["workspace_id"]
        ok = (principal.can("connection.use", workspace_id=ws_id, connection_id=new_conn)
              or (conn["workspace_id"] is None and principal.can("connection.use", connection_id=new_conn))
              or principal.can("connection.manage", workspace_id=ws_id))
        if conn["workspace_id"] not in (None, ws_id) or not ok:
            raise Forbidden(f"you may not use connection '{conn['name']}' for a lakehouse in this workspace",
                            remediation="ask a workspace administrator for the 'connection.use' permission on that connection")

    def remap_ids(definition: dict[str, Any], id_map: dict[str, str], name_map: dict[str, str] | None = None) -> dict[str, Any]:
        """Import / promotion: ``storage.connection_id`` follows the imported (or promoted) connection."""
        return refs.remap_paths(definition, id_map, ["storage.connection_id"], name_map)

    ctx.register_item_hooks("lakehouse", validate=validate, on_create=on_create, on_delete=on_delete, on_update=on_update, remap_ids=remap_ids)


def _purge_item(ctx: AppContext, item: dict[str, Any]) -> None:
    with ctx.db.begin() as conn:
        conn.execute(S.item_versions.delete().where(S.item_versions.c.item_id == item["id"]))
        conn.execute(S.items.delete().where(S.items.c.id == item["id"]))
    log.warning("rolled back lakehouse item %s (%s) at %s", item["id"], item["name"], now_iso())
