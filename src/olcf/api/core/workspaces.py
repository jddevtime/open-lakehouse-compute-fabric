# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Workspaces and items (spec sections 6-7).

The workspace is the primary security/configuration/collaboration boundary. Items are typed, versioned
documents with immutable ids, ETags and soft delete (recycle bin). Item *definitions* are Git-friendly JSON;
feature modules attach validation / export hooks per item type through ``ctx.register_item_hooks``.
"""

from __future__ import annotations

import hashlib
import io
import json
import re
import zipfile
from typing import Any

from fastapi import APIRouter, Query, Response, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import ValidationError as PydanticValidationError
from sqlalchemy import or_, select, update

from olcf.api.deps import Authed, Ctx, IfMatch, Pagination, check_if_match, etag_header
from olcf.core import refs
from olcf.core.context import AppContext
from olcf.core.errors import BadRequest, Conflict, Forbidden, NotFound, ValidationFailed
from olcf.core.ids import new_id
from olcf.core.models import ItemCreate, ItemType, ItemUpdate, OpenBody, Role, WorkspaceCreate, dump
from olcf.core.timeutil import iso_in, now_iso
from olcf.db import schema as S
from olcf.db.util import fetch_all, fetch_one, get_by_id, insert_row, new_etag, paginate
from olcf.security.rbac import Principal

router = APIRouter(tags=["workspaces"])

MAX_DEFINITION_BYTES = 25 * 1024 * 1024
_SLUG = re.compile(r"[^A-Za-z0-9._-]+")


def _safe_name(s: str) -> str:
    return _SLUG.sub("_", s).strip("._") or "unnamed"


def _public_ws(r: dict[str, Any]) -> dict[str, Any]:
    return {k: r[k] for k in ("id", "name", "display_name", "description", "owner_id", "tags", "settings", "quota", "git", "etag", "created_at", "updated_at", "deleted_at")}


def _public_item(r: dict[str, Any], *, with_definition: bool = True) -> dict[str, Any]:
    keys = ["id", "workspace_id", "type", "name", "folder", "description", "owner_id", "tags", "sensitivity", "version", "etag", "created_at", "updated_at", "deleted_at"]
    out = {k: r[k] for k in keys}
    if with_definition:
        out["definition"] = r["definition"]
    return out


class WorkspaceService:
    """Workspaces: create, resolve by id or name, update, soft delete and restore, and export and import of whole workspaces as archives.
    """
    def __init__(self, ctx: AppContext) -> None:
        self.ctx = ctx

    # ------------------------------------------------------------------ resolution
    def resolve(self, ref: str, *, include_deleted: bool = False) -> dict[str, Any]:
        """Resolve a workspace by id or (unique) name."""
        with self.ctx.db.connect() as conn:
            row = get_by_id(conn, S.workspaces, ref, include_deleted=include_deleted) if ref.startswith("wks_") else None
            if row is None:
                stmt = select(S.workspaces).where(S.workspaces.c.name == ref)
                if not include_deleted:
                    stmt = stmt.where(S.workspaces.c.deleted_at.is_(None))
                row = fetch_one(conn, stmt)
        if not row:
            raise NotFound(f"workspace '{ref}' not found")
        return row

    def create(self, principal: Principal, body: WorkspaceCreate) -> dict[str, Any]:
        with self.ctx.db.begin() as conn:
            dup = fetch_one(conn, select(S.workspaces.c.id).where(S.workspaces.c.name == body.name, S.workspaces.c.deleted_at.is_(None)))
            if dup:
                raise Conflict(f"workspace '{body.name}' already exists")
            now = now_iso()
            row = {"id": new_id("wks"), "name": body.name, "display_name": body.display_name or body.name, "description": body.description,
                   "owner_id": principal.id, "tags": body.tags, "settings": body.settings, "quota": body.quota, "git": {},
                   "etag": new_etag(), "created_at": now, "updated_at": now, "deleted_at": None}
            insert_row(conn, S.workspaces, row)
        # creator becomes WorkspaceAdmin (unless already org-level admin)
        if principal.id not in ("usr_system",):
            ident = self.ctx.service("identity")
            ident.bind(principal.id, Role.WORKSPACE_ADMIN.value, scope_type="workspace", scope_id=row["id"], created_by=principal.id, source="system")
        self.ctx.audit.record(principal, "workspace.created", resource_type="workspace", resource_id=row["id"], workspace_id=row["id"], details={"name": body.name})
        self.ctx.events.publish("workspace.created", {"name": body.name}, workspace_id=row["id"], subject=row["id"])
        return row

    def list(self, principal: Principal, *, limit: int, cursor: str | None, deleted: bool = False, q: str | None = None) -> tuple[list[dict[str, Any]], str | None]:
        visible = principal.visible_workspaces("workspace.read")
        stmt = select(S.workspaces)
        stmt = stmt.where(S.workspaces.c.deleted_at.is_not(None) if deleted else S.workspaces.c.deleted_at.is_(None))
        if visible is not None:
            if not visible:
                return [], None
            stmt = stmt.where(S.workspaces.c.id.in_(list(visible)))
        if q:
            stmt = stmt.where(or_(S.workspaces.c.name.like(f"%{q}%"), S.workspaces.c.display_name.like(f"%{q}%")))
        with self.ctx.db.connect() as conn:
            return paginate(conn, stmt, S.workspaces, limit=limit, cursor=cursor)

    def update(self, principal: Principal, ws_id: str, changes: dict[str, Any], if_match: str | None) -> dict[str, Any]:
        with self.ctx.db.begin() as conn:
            row = get_by_id(conn, S.workspaces, ws_id)
            if not row:
                raise NotFound("workspace not found")
            check_if_match(if_match, row["etag"])
            if "name" in changes and changes["name"] != row["name"]:
                dup = fetch_one(conn, select(S.workspaces.c.id).where(S.workspaces.c.name == changes["name"], S.workspaces.c.deleted_at.is_(None)))
                if dup:
                    raise Conflict(f"workspace '{changes['name']}' already exists")
            conn.execute(update(S.workspaces).where(S.workspaces.c.id == ws_id).values(**changes, etag=new_etag(), updated_at=now_iso()))
            new = get_by_id(conn, S.workspaces, ws_id)
        self.ctx.audit.record(principal, "workspace.updated", resource_type="workspace", resource_id=ws_id, workspace_id=ws_id, details={"fields": sorted(changes)})
        return new  # type: ignore[return-value]

    def soft_delete(self, principal: Principal, ws_id: str) -> None:
        with self.ctx.db.begin() as conn:
            n = conn.execute(update(S.workspaces).where(S.workspaces.c.id == ws_id, S.workspaces.c.deleted_at.is_(None)).values(deleted_at=now_iso(), etag=new_etag())).rowcount
        if not n:
            raise NotFound("workspace not found")
        self.ctx.audit.record(principal, "workspace.deleted", resource_type="workspace", resource_id=ws_id, workspace_id=ws_id)
        self.ctx.events.publish("workspace.deleted", {}, workspace_id=ws_id, subject=ws_id)

    def restore(self, principal: Principal, ws_id: str) -> dict[str, Any]:
        row = self.resolve(ws_id, include_deleted=True)
        with self.ctx.db.begin() as conn:
            dup = fetch_one(conn, select(S.workspaces.c.id).where(S.workspaces.c.name == row["name"], S.workspaces.c.deleted_at.is_(None)))
            if dup:
                raise Conflict(f"another workspace named '{row['name']}' exists; rename it before restoring")
            conn.execute(update(S.workspaces).where(S.workspaces.c.id == row["id"]).values(deleted_at=None, etag=new_etag()))
        self.ctx.audit.record(principal, "workspace.restored", resource_type="workspace", resource_id=row["id"], workspace_id=row["id"])
        return self.resolve(row["id"])

    # ------------------------------------------------------------------ export / import
    def export_zip(self, principal: Principal, ws_id: str) -> bytes:
        """Git-friendly export. Connections are exported as metadata only - secret material never leaves the vault."""
        ws = self.resolve(ws_id)
        files: dict[str, bytes] = {}
        files["workspace.json"] = _dump({"schemaVersion": "1.0", "name": ws["name"], "display_name": ws["display_name"], "description": ws["description"],
                                         "tags": ws["tags"], "settings": ws["settings"], "quota": ws["quota"]})
        with self.ctx.db.connect() as conn:
            item_rows = fetch_all(conn, select(S.items).where(S.items.c.workspace_id == ws["id"], S.items.c.deleted_at.is_(None)).order_by(S.items.c.id))
            conn_rows = fetch_all(conn, select(S.connections).where(S.connections.c.workspace_id == ws["id"], S.connections.c.deleted_at.is_(None)))
        id_to_path: dict[str, str] = {}
        for r in item_rows:
            folder = "/".join(_safe_name(p) for p in r["folder"].split("/") if p)
            base = f"items/{folder + '/' if folder else ''}{_safe_name(r['name'])}.{r['type']}"
            id_to_path[r["id"]] = base
            meta = {"logicalId": r["id"], "type": r["type"], "name": r["name"], "description": r["description"], "folder": r["folder"], "tags": r["tags"],
                    "sensitivity": r["sensitivity"], "version": r["version"]}
            hooks = self.ctx.item_hooks.get(r["type"], {})
            if "export" in hooks:
                extra = hooks["export"](r)  # {relative filename: bytes}
                for fname, data in extra.items():
                    files[f"{base}/{fname}"] = data
                meta["definition"] = hooks.get("export_definition", lambda d: d)(r["definition"]) if "export_definition" in hooks else {}
            else:
                meta["definition"] = r["definition"]
            files[f"{base}/item.json"] = _dump(meta)
        for c in conn_rows:
            files[f"connections/{_safe_name(c['name'])}.json"] = _dump({"logicalId": c["id"], "name": c["name"], "type": c["type"], "description": c["description"],
                                                                         "config": c["config"], "shared": c["shared"], "requires_secret": bool(c["secret_id"]), "tags": c["tags"]})
        manifest = {"schemaVersion": "1.0", "exported_at": now_iso(), "exported_by": principal.name, "product": "olcf",
                    "files": {k: hashlib.sha256(v).hexdigest() for k, v in sorted(files.items())}}
        files["manifest.json"] = _dump(manifest)
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
            for name, data in sorted(files.items()):
                z.writestr(name, data)
        self.ctx.audit.record(principal, "workspace.exported", resource_type="workspace", resource_id=ws["id"], workspace_id=ws["id"], details={"files": len(files)})
        return buf.getvalue()

    def import_zip(self, principal: Principal, data: bytes, *, target_workspace: str | None, new_name: str | None, on_conflict: str = "rename") -> dict[str, Any]:
        if on_conflict not in ("rename", "skip", "overwrite"):
            raise BadRequest("on_conflict must be rename|skip|overwrite")
        if target_workspace:  # authorise before looking at (or reporting on) the upload
            principal.require("workspace.import", workspace_id=self.resolve(target_workspace)["id"])
        else:
            principal.require("workspace.create")
        try:
            z = zipfile.ZipFile(io.BytesIO(data))
        except zipfile.BadZipFile as exc:
            raise BadRequest("not a valid zip archive") from exc
        names = z.namelist()
        for n in names:  # zip-slip / absolute path guard
            if n.startswith("/") or ".." in n.split("/") or "\\" in n:
                raise BadRequest(f"unsafe path in archive: {n}")
        if sum(i.file_size for i in z.infolist()) > 2 * 1024**3:
            raise BadRequest("archive too large when expanded")
        if "manifest.json" in names:
            manifest = _zip_json(z, "manifest.json")
            if not isinstance(manifest.get("files", {}), dict):
                raise ValidationFailed("manifest.json: 'files' must be an object")
            for fname, digest in manifest.get("files", {}).items():
                if fname not in names or hashlib.sha256(z.read(fname)).hexdigest() != digest:
                    raise ValidationFailed(f"manifest checksum mismatch for {fname}")
        wsmeta = _zip_json(z, "workspace.json") if "workspace.json" in names else {"name": "imported"}
        if target_workspace:
            ws = self.resolve(target_workspace)
            principal.require("workspace.import", workspace_id=ws["id"])
        else:
            principal.require("workspace.create")
            name = new_name or wsmeta["name"]
            base, i = name, 2
            while True:
                try:
                    self.resolve(name)
                    name = f"{base}-{i}"
                    i += 1
                except NotFound:
                    break
            try:
                spec = WorkspaceCreate(name=name, display_name=wsmeta.get("display_name"), description=wsmeta.get("description", ""),
                                       tags=wsmeta.get("tags", []), settings=wsmeta.get("settings", {}), quota=wsmeta.get("quota", {}))
            except PydanticValidationError as exc:
                raise ValidationFailed("workspace.json is not a valid workspace definition", details={"errors": [e["msg"] for e in exc.errors()][:5]}) from None
            ws = self.create(principal, spec)
        items_svc: ItemService = self.ctx.service("items")
        created, skipped, mapping = [], [], {}
        # 1. connections first: lakehouses (and other items) reference them, and their new ids belong in the id mapping
        conns_created = []
        name_map: dict[str, str] = {}
        csvc: ConnectionService = self.ctx.service("connections")
        for n in sorted(names):
            if n.startswith("connections/") and n.endswith(".json"):
                c = _zip_json(z, n, required=("name", "type"))
                cur = csvc.find(ws["id"], c["name"])
                if not cur:
                    cur = csvc.create(principal, workspace_id=ws["id"], name=c["name"], type_=c["type"], config=c.get("config", {}), secret_id=None,
                                      description=c.get("description", ""), shared=False, tags=c.get("tags", []))
                    conns_created.append({"name": c["name"], "needs_secret": c.get("requires_secret", False)})
                if c.get("logicalId"):
                    mapping[c["logicalId"]] = cur["id"]
                name_map[c["name"]] = cur["name"]
        # 2. items in dependency order (a stable sort: referenced types first); ids of items created later are fixed up in step 3
        parsed: list[tuple[dict[str, Any], dict[str, Any], str]] = []
        for d in sorted({n.rsplit("/", 1)[0] for n in names if n.startswith("items/") and n.endswith("/item.json")}):
            meta = _zip_json(z, f"{d}/item.json", required=("type", "name"))
            if meta["type"] not in _ITEM_TYPES:
                raise ValidationFailed(f"{d}/item.json: unknown item type {str(meta['type'])[:40]!r}")
            files = {n[len(d) + 1:]: z.read(n) for n in names if n.startswith(d + "/") and not n.endswith("/item.json") and n != f"{d}/item.json"}
            hooks = self.ctx.item_hooks.get(meta["type"], {})
            parsed.append((meta, hooks["import"](meta, files) if "import" in hooks else meta.get("definition", {}), d))
        parsed.sort(key=lambda t: _IMPORT_ORDER.get(t[0]["type"], len(_IMPORT_ORDER)))
        applied: list[tuple[str, str, dict[str, Any], dict[str, Any]]] = []  # (item id, type, source definition, definition as written)
        for meta, definition, _d in parsed:
            existing = items_svc.find(ws["id"], meta["type"], meta["name"])
            if existing and on_conflict == "skip":
                skipped.append(meta["name"])
                mapping[meta.get("logicalId", "")] = existing["id"]
                continue
            written = refs.remap_definition(self.ctx.item_hooks, meta["type"], definition, mapping, name_map)
            if existing and on_conflict == "overwrite":
                items_svc.update(principal, existing["id"], ItemUpdate(definition=written, description=meta.get("description"), tags=meta.get("tags"),
                                                                        folder=meta.get("folder"), comment="import"), None)
                mapping[meta.get("logicalId", "")] = existing["id"]
                applied.append((existing["id"], meta["type"], definition, written))
                created.append(meta["name"])
                continue
            name = meta["name"]
            if existing:
                k = 2
                while items_svc.find(ws["id"], meta["type"], f"{meta['name']} ({k})"):
                    k += 1
                name = f"{meta['name']} ({k})"
            it = items_svc.create(principal, ws["id"], ItemCreate(type=ItemType(meta["type"]), name=name, description=meta.get("description", ""),
                                                                  folder=meta.get("folder", ""), tags=meta.get("tags", []), sensitivity=meta.get("sensitivity"),
                                                                  definition=written))
            mapping[meta.get("logicalId", "")] = it["id"]
            applied.append((it["id"], meta["type"], definition, written))
            created.append(name)
        # 3. forward references (a pipeline calling a pipeline imported later, ...): rewrite once the whole mapping is known
        for item_id, typ, definition, written in applied:
            final = refs.remap_definition(self.ctx.item_hooks, typ, definition, mapping, name_map)
            if final != written:
                items_svc.update(principal, item_id, ItemUpdate(definition=final, comment="import: resolve references"), None)
        self.ctx.audit.record(principal, "workspace.imported", resource_type="workspace", resource_id=ws["id"], workspace_id=ws["id"],
                              details={"items": len(created), "skipped": len(skipped), "connections": len(conns_created)})
        return {"workspace": _public_ws(self.resolve(ws["id"])), "items_created": created, "items_skipped": skipped, "connections_created": conns_created,
                "id_mapping": mapping}


#: Import order: types that other types reference come first (the rest keeps archive order).
_ITEM_TYPES = frozenset(t.value for t in ItemType)


def _zip_json(z: zipfile.ZipFile, name: str, required: tuple[str, ...] = ()) -> dict[str, Any]:
    """A JSON object member of an uploaded archive; malformed input is a 422, never a 500."""
    try:
        doc = json.loads(z.read(name))
    except (ValueError, KeyError, zipfile.BadZipFile, RuntimeError) as exc:
        raise ValidationFailed(f"{name} in the archive is not valid JSON") from exc
    if not isinstance(doc, dict):
        raise ValidationFailed(f"{name} in the archive must be a JSON object")
    missing = [k for k in required if not isinstance(doc.get(k), str) or not doc[k]]
    if missing:
        raise ValidationFailed(f"{name} in the archive lacks {missing}")
    return doc


_IMPORT_ORDER = {t: i for i, t in enumerate(("lakehouse", "environment", "variable_set", "notebook", "spark_job_definition", "sql_query", "pipeline", "report", "schedule"))}


def _dump(obj: Any) -> bytes:
    return (json.dumps(obj, indent=2, sort_keys=True, default=str) + "\n").encode()


class ItemService:
    """Items of every type: create, read, update with ETags, soft delete and restore, versions and the per-type validation hooks registered by feature modules.
    """
    def __init__(self, ctx: AppContext) -> None:
        self.ctx = ctx

    def get(self, item_id: str, *, include_deleted: bool = False) -> dict[str, Any]:
        with self.ctx.db.connect() as conn:
            row = get_by_id(conn, S.items, item_id, include_deleted=include_deleted)
        if not row:
            raise NotFound(f"item {item_id} not found")
        return row

    def find(self, workspace_id: str, type_: str, name: str) -> dict[str, Any] | None:
        with self.ctx.db.connect() as conn:
            return fetch_one(conn, select(S.items).where(S.items.c.workspace_id == workspace_id, S.items.c.type == str(type_), S.items.c.name == name, S.items.c.deleted_at.is_(None)))

    def resolve(self, workspace_id: str, ref: str, type_: str | None = None) -> dict[str, Any]:
        """Resolve an item by id or by name within ``workspace_id`` (optionally constrained by type)."""
        if ref.startswith("itm_"):
            row = self.get(ref)
            if row["workspace_id"] != workspace_id:
                raise NotFound(f"item {ref} not found in this workspace")
            if type_ and row["type"] != type_:
                raise NotFound(f"item {ref} is not a {type_}")
            return row
        with self.ctx.db.connect() as conn:
            stmt = select(S.items).where(S.items.c.workspace_id == workspace_id, S.items.c.name == ref, S.items.c.deleted_at.is_(None))
            if type_:
                stmt = stmt.where(S.items.c.type == type_)
            rows = fetch_all(conn, stmt)
        if not rows:
            raise NotFound(f"item '{ref}' not found")
        if len(rows) > 1:
            raise Conflict(f"item name '{ref}' is ambiguous; use the id or specify the type")
        return rows[0]

    def _validate(self, type_: str, definition: dict[str, Any]) -> dict[str, Any]:
        if len(json.dumps(definition, default=str)) > MAX_DEFINITION_BYTES:
            raise ValidationFailed("item definition is too large")
        hook = self.ctx.item_hooks.get(type_, {}).get("validate")
        return hook(definition) if hook else definition

    def create(self, principal: Principal, workspace_id: str, body: ItemCreate) -> dict[str, Any]:
        definition = self._validate(body.type.value, body.definition)
        with self.ctx.db.begin() as conn:
            ws = get_by_id(conn, S.workspaces, workspace_id)
            if not ws:
                raise NotFound("workspace not found")
            dup = fetch_one(conn, select(S.items.c.id).where(S.items.c.workspace_id == workspace_id, S.items.c.type == body.type.value, S.items.c.name == body.name, S.items.c.deleted_at.is_(None)))
            if dup:
                raise Conflict(f"a {body.type.value} named '{body.name}' already exists in this workspace")
            now = now_iso()
            row = {"id": new_id("itm"), "workspace_id": workspace_id, "type": body.type.value, "name": body.name, "folder": body.folder.strip("/"),
                   "description": body.description, "owner_id": principal.id, "tags": body.tags, "sensitivity": body.sensitivity,
                   "definition": definition, "version": 1, "etag": new_etag(), "created_at": now, "updated_at": now, "deleted_at": None}
            insert_row(conn, S.items, row)
            insert_row(conn, S.item_versions, {"id": new_id("itm").replace("itm_", "ver_"), "item_id": row["id"], "version": 1, "definition": definition,
                                               "name": body.name, "comment": "created", "created_at": now, "created_by": principal.id})
        hook = self.ctx.item_hooks.get(row["type"], {}).get("on_create")
        if hook:
            hook(row)
        self.ctx.audit.record(principal, "item.created", resource_type=row["type"], resource_id=row["id"], workspace_id=workspace_id, details={"name": row["name"]})
        self.ctx.events.publish("item.created", {"type": row["type"], "name": row["name"]}, workspace_id=workspace_id, subject=row["id"])
        return row

    def update(self, principal: Principal, item_id: str, body: ItemUpdate, if_match: str | None) -> dict[str, Any]:
        with self.ctx.db.begin() as conn:
            row = get_by_id(conn, S.items, item_id)
            if not row:
                raise NotFound("item not found")
            check_if_match(if_match, row["etag"])
            vals: dict[str, Any] = {}
            if body.name is not None and body.name != row["name"]:
                dup = fetch_one(conn, select(S.items.c.id).where(S.items.c.workspace_id == row["workspace_id"], S.items.c.type == row["type"], S.items.c.name == body.name, S.items.c.deleted_at.is_(None)))
                if dup:
                    raise Conflict(f"a {row['type']} named '{body.name}' already exists in this workspace")
                vals["name"] = body.name
            for f in ("description", "tags", "sensitivity"):
                v = getattr(body, f)
                if v is not None:
                    vals[f] = v
            if body.folder is not None:
                vals["folder"] = body.folder.strip("/")
            now = now_iso()
            definition: dict[str, Any] | None = None
            if body.definition is not None:
                definition = self._validate(row["type"], body.definition)
                if definition != row["definition"]:
                    vals["definition"] = definition
                    vals["version"] = row["version"] + 1
            if not vals:
                return row
            on_update = self.ctx.item_hooks.get(row["type"], {}).get("on_update")
            if on_update:
                # ``on_update(principal, old_row, new_row)`` runs before anything is written: a hook that raises (e.g. Forbidden because the caller may not use the
                # connection a lakehouse is re-pointed to) leaves the item untouched
                on_update(principal, row, {**row, **vals})
            if "definition" in vals:
                insert_row(conn, S.item_versions, {"id": new_id("itm").replace("itm_", "ver_"), "item_id": item_id, "version": vals["version"],
                                                   "definition": vals["definition"], "name": vals.get("name", row["name"]), "comment": body.comment or "", "created_at": now,
                                                   "created_by": principal.id})
            vals.update(etag=new_etag(), updated_at=now)
            conn.execute(update(S.items).where(S.items.c.id == item_id).values(**vals))
            new = get_by_id(conn, S.items, item_id)
        self.ctx.audit.record(principal, "item.updated", resource_type=row["type"], resource_id=item_id, workspace_id=row["workspace_id"], details={"fields": sorted(k for k in vals if k not in ("etag", "updated_at"))})
        self.ctx.events.publish("item.updated", {"type": row["type"], "name": new["name"], "version": new["version"]}, workspace_id=row["workspace_id"], subject=item_id)  # type: ignore[index]
        return new  # type: ignore[return-value]

    def delete(self, principal: Principal, item_id: str) -> dict[str, Any]:
        row = self.get(item_id)
        with self.ctx.db.begin() as conn:
            conn.execute(update(S.items).where(S.items.c.id == item_id).values(deleted_at=now_iso(), etag=new_etag()))
        hook = self.ctx.item_hooks.get(row["type"], {}).get("on_delete")
        if hook:
            hook(row)
        self.ctx.audit.record(principal, "item.deleted", resource_type=row["type"], resource_id=item_id, workspace_id=row["workspace_id"], details={"name": row["name"]})
        self.ctx.events.publish("item.deleted", {"type": row["type"], "name": row["name"]}, workspace_id=row["workspace_id"], subject=item_id)
        return row

    def restore(self, principal: Principal, item_id: str) -> dict[str, Any]:
        row = self.get(item_id, include_deleted=True)
        if row["deleted_at"] is None:
            return row
        with self.ctx.db.begin() as conn:
            dup = fetch_one(conn, select(S.items.c.id).where(S.items.c.workspace_id == row["workspace_id"], S.items.c.type == row["type"], S.items.c.name == row["name"], S.items.c.deleted_at.is_(None)))
            if dup:
                raise Conflict(f"a {row['type']} named '{row['name']}' exists; rename it first")
            conn.execute(update(S.items).where(S.items.c.id == item_id).values(deleted_at=None, etag=new_etag()))
        self.ctx.audit.record(principal, "item.restored", resource_type=row["type"], resource_id=item_id, workspace_id=row["workspace_id"])
        return self.get(item_id)

    def list(self, workspace_id: str, *, type_: str | None = None, folder: str | None = None, q: str | None = None, tag: str | None = None,
             deleted: bool = False, limit: int = 100, cursor: str | None = None, with_definition: bool = False) -> tuple[list[dict[str, Any]], str | None]:
        stmt = select(S.items).where(S.items.c.workspace_id == workspace_id)
        stmt = stmt.where(S.items.c.deleted_at.is_not(None) if deleted else S.items.c.deleted_at.is_(None))
        if type_:
            stmt = stmt.where(S.items.c.type == type_)
        if folder is not None:
            stmt = stmt.where(S.items.c.folder == folder.strip("/"))
        if q:
            stmt = stmt.where(or_(S.items.c.name.like(f"%{q}%"), S.items.c.description.like(f"%{q}%")))
        with self.ctx.db.connect() as conn:
            rows, nxt = paginate(conn, stmt, S.items, limit=limit, cursor=cursor)
        if tag:
            rows = [r for r in rows if tag in (r["tags"] or [])]
        return rows, nxt

    def versions(self, item_id: str) -> list[dict[str, Any]]:
        with self.ctx.db.connect() as conn:
            return fetch_all(conn, select(S.item_versions).where(S.item_versions.c.item_id == item_id).order_by(S.item_versions.c.version.desc()))

    def purge_deleted(self, older_than_days: int) -> int:
        cutoff = iso_in(-older_than_days * 86400)
        n = 0
        with self.ctx.db.begin() as conn:
            ids = [r["id"] for r in fetch_all(conn, select(S.items.c.id).where(S.items.c.deleted_at.is_not(None), S.items.c.deleted_at < cutoff))]
            for i in ids:
                conn.execute(S.item_versions.delete().where(S.item_versions.c.item_id == i))
                conn.execute(S.items.delete().where(S.items.c.id == i))
                n += 1
        return n


# ===================================================================== routes: workspaces
@router.get("/workspaces")
def list_workspaces(principal: Authed, ctx: Ctx, page: Pagination, deleted: bool = False, q: str | None = None) -> dict[str, Any]:
    if deleted:
        principal.require("workspace.delete") if principal.visible_workspaces("workspace.delete") is None else None
    rows, nxt = ctx.service("workspaces").list(principal, limit=page.limit, cursor=page.cursor, deleted=deleted, q=q)
    if deleted:
        rows = [r for r in rows if principal.can("workspace.delete", workspace_id=r["id"])]
    return {"items": [_public_ws(r) for r in rows], "next_cursor": nxt}


@router.post("/workspaces", status_code=201)
def create_workspace(body: WorkspaceCreate, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    principal.require("workspace.create")
    row = ctx.service("workspaces").create(principal, body)
    response.headers["ETag"] = etag_header(row["etag"])
    return _public_ws(row)


@router.get("/workspaces/{ws}")
def get_workspace(ws: str, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    row = ctx.service("workspaces").resolve(ws)
    principal.require("workspace.read", workspace_id=row["id"])
    response.headers["ETag"] = etag_header(row["etag"])
    return _public_ws(row)


class WorkspacePatch(OpenBody):
    """Fields of ``PATCH /workspaces/{ws}``; any other key is refused with 400 ``unsupported fields``."""

    name: str | None = None
    display_name: str | None = None
    description: str | None = None
    tags: list[str] | None = None
    settings: dict[str, Any] | None = None
    quota: dict[str, Any] | None = None
    git: dict[str, Any] | None = None


@router.patch("/workspaces/{ws}")
def patch_workspace(ws: str, patch: WorkspacePatch, principal: Authed, ctx: Ctx, if_match: IfMatch = None) -> dict[str, Any]:
    body = dump(patch)
    row = ctx.service("workspaces").resolve(ws)
    principal.require("workspace.update", workspace_id=row["id"])
    allowed = {"name", "display_name", "description", "tags", "settings", "quota", "git"}
    changes = {k: v for k, v in body.items() if k in allowed}
    if set(body) - allowed:
        raise BadRequest(f"unsupported fields: {sorted(set(body) - allowed)}")
    if "name" in changes and not re.match(r"^[A-Za-z0-9][A-Za-z0-9._ -]*$", str(changes["name"])):
        raise BadRequest("invalid workspace name")
    return _public_ws(ctx.service("workspaces").update(principal, row["id"], changes, if_match))


@router.delete("/workspaces/{ws}", status_code=204)
def delete_workspace(ws: str, principal: Authed, ctx: Ctx) -> None:
    row = ctx.service("workspaces").resolve(ws)
    principal.require("workspace.delete", workspace_id=row["id"])
    ctx.service("workspaces").soft_delete(principal, row["id"])


@router.post("/workspaces/{ws}/restore")
def restore_workspace(ws: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    row = ctx.service("workspaces").resolve(ws, include_deleted=True)
    principal.require("workspace.delete", workspace_id=row["id"])
    return _public_ws(ctx.service("workspaces").restore(principal, row["id"]))


@router.get("/workspaces/{ws}/members")
def workspace_members(ws: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    row = ctx.service("workspaces").resolve(ws)
    principal.require("workspace.read", workspace_id=row["id"])
    with ctx.db.connect() as conn:
        rows = fetch_all(conn, select(S.role_bindings, S.principals.c.name.label("principal_name"), S.principals.c.kind.label("principal_kind"), S.principals.c.display_name)
                         .join(S.principals, S.principals.c.id == S.role_bindings.c.principal_id)
                         .where(S.role_bindings.c.scope_type == "workspace", S.role_bindings.c.scope_id == row["id"]))
    return {"items": rows}


@router.get("/workspaces/{ws}/export")
def export_workspace(ws: str, principal: Authed, ctx: Ctx) -> StreamingResponse:
    row = ctx.service("workspaces").resolve(ws)
    principal.require("workspace.export", workspace_id=row["id"])
    data = ctx.service("workspaces").export_zip(principal, row["id"])
    return StreamingResponse(io.BytesIO(data), media_type="application/zip",
                             headers={"Content-Disposition": f'attachment; filename="{_safe_name(row["name"])}.olcf.zip"'})


@router.post("/workspaces/import", status_code=201)
async def import_workspace(file: UploadFile, principal: Authed, ctx: Ctx, target_workspace: str | None = Query(None), new_name: str | None = Query(None),
                           on_conflict: str = Query("rename")) -> dict[str, Any]:
    data = await file.read()
    if len(data) > ctx.settings.server.request_body_limit_mb * 1024 * 1024:
        raise BadRequest("upload too large")
    from starlette.concurrency import run_in_threadpool

    return await run_in_threadpool(ctx.service("workspaces").import_zip, principal, data, target_workspace=target_workspace, new_name=new_name, on_conflict=on_conflict)


# ===================================================================== routes: items
#: ``GET /workspaces/{ws}/items`` with definitions embeds at most this many bytes of definitions per response; later items carry ``definition_omitted: "size"``
#: (fetch those with ``GET /items/{id}``). The cap protects the server and slow clients from ``limit=200`` x large notebooks.
LIST_DEFINITION_CAP_BYTES = 8 * 1024 * 1024


@router.get("/workspaces/{ws}/items")
def list_items(ws: str, principal: Authed, ctx: Ctx, page: Pagination, type: str | None = None, folder: str | None = None, q: str | None = None,  # noqa: A002
               tag: str | None = None, deleted: bool = False, with_definition: bool = False,
               include: str | None = Query(None, description="comma separated; 'definition' embeds each definition (same as with_definition=true), size-capped")) -> dict[str, Any]:
    wanted = {x.strip() for x in (include or "").split(",") if x.strip()}
    if wanted - {"definition"}:
        raise BadRequest(f"unsupported include value(s) {sorted(wanted - {'definition'})}; supported: definition")
    embed = with_definition or "definition" in wanted
    row = ctx.service("workspaces").resolve(ws)
    principal.require("item.read", workspace_id=row["id"])
    if deleted:
        principal.require("item.delete", workspace_id=row["id"])
    rows, nxt = ctx.service("items").list(row["id"], type_=type, folder=folder, q=q, tag=tag, deleted=deleted, limit=page.limit, cursor=page.cursor)
    items, used = [], 0
    for r in rows:
        out = _public_item(r, with_definition=False)
        if embed:
            size = len(json.dumps(r["definition"], default=str))
            if not principal.can("item.read", workspace_id=row["id"], item_id=r["id"]):
                out["definition"], out["definition_omitted"] = None, "forbidden"
            elif used + size > LIST_DEFINITION_CAP_BYTES:
                out["definition"], out["definition_omitted"] = None, "size"
            else:
                used += size
                out["definition"] = r["definition"]
        items.append(out)
    return {"items": items, "next_cursor": nxt}


@router.post("/workspaces/{ws}/items", status_code=201)
def create_item(ws: str, body: ItemCreate, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    row = ctx.service("workspaces").resolve(ws)
    principal.require("item.create", workspace_id=row["id"])
    item = ctx.service("items").create(principal, row["id"], body)
    response.headers["ETag"] = etag_header(item["etag"])
    return _public_item(item)


def _load_item(ctx: AppContext, principal: Principal, item_id: str, perm: str, *, include_deleted: bool = False) -> dict[str, Any]:
    item = ctx.service("items").get(item_id, include_deleted=include_deleted)
    principal.require(perm, workspace_id=item["workspace_id"], item_id=item["id"])
    return item


@router.get("/items/{item_id}")
def get_item(item_id: str, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    item = _load_item(ctx, principal, item_id, "item.read", include_deleted=True)
    response.headers["ETag"] = etag_header(item["etag"])
    return _public_item(item)


@router.put("/items/{item_id}")
def update_item(item_id: str, body: ItemUpdate, principal: Authed, ctx: Ctx, response: Response, if_match: IfMatch = None) -> dict[str, Any]:
    _load_item(ctx, principal, item_id, "item.update")
    item = ctx.service("items").update(principal, item_id, body, if_match)
    response.headers["ETag"] = etag_header(item["etag"])
    return _public_item(item)


@router.delete("/items/{item_id}", status_code=204)
def delete_item(item_id: str, principal: Authed, ctx: Ctx) -> None:
    _load_item(ctx, principal, item_id, "item.delete")
    ctx.service("items").delete(principal, item_id)


@router.post("/items/{item_id}/restore")
def restore_item(item_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    _load_item(ctx, principal, item_id, "item.delete", include_deleted=True)
    return _public_item(ctx.service("items").restore(principal, item_id))


@router.get("/items/{item_id}/versions")
def item_versions(item_id: str, principal: Authed, ctx: Ctx, with_definition: bool = False) -> dict[str, Any]:
    _load_item(ctx, principal, item_id, "item.read", include_deleted=True)
    rows = ctx.service("items").versions(item_id)
    if not with_definition:
        for r in rows:
            r.pop("definition", None)
    return {"items": rows}


@router.get("/items/{item_id}/versions/{version}")
def item_version(item_id: str, version: int, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    _load_item(ctx, principal, item_id, "item.read", include_deleted=True)
    for r in ctx.service("items").versions(item_id):
        if r["version"] == version:
            return r
    raise NotFound("version not found")


@router.post("/items/{item_id}/versions/{version}/revert")
def revert_item(item_id: str, version: int, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    _load_item(ctx, principal, item_id, "item.update")
    for r in ctx.service("items").versions(item_id):
        if r["version"] == version:
            item = ctx.service("items").update(principal, item_id, ItemUpdate(definition=r["definition"], comment=f"revert to v{version}"), None)
            return _public_item(item)
    raise NotFound("version not found")


@router.get("/search")
def search(principal: Authed, ctx: Ctx, q: str = Query(min_length=1), limit: int = Query(25, ge=1, le=100)) -> dict[str, Any]:
    """Cross-workspace item search limited to workspaces the caller can read."""
    visible = principal.visible_workspaces("item.read")
    stmt = select(S.items.c.id, S.items.c.workspace_id, S.items.c.type, S.items.c.name, S.items.c.folder, S.items.c.description).where(
        S.items.c.deleted_at.is_(None), or_(S.items.c.name.like(f"%{q}%"), S.items.c.description.like(f"%{q}%"))).limit(limit)
    if visible is not None:
        if not visible:
            return {"items": []}
        stmt = stmt.where(S.items.c.workspace_id.in_(list(visible)))
    with ctx.db.connect() as conn:
        return {"items": fetch_all(conn, stmt)}


# ===================================================================== connections
class ConnectionService:
    """Connections: storage and service endpoints whose secret material lives in the secret store, either owned by a workspace or shared across the organisation, with a connectivity test.
    """
    def __init__(self, ctx: AppContext) -> None:
        self.ctx = ctx

    @staticmethod
    def public(r: dict[str, Any]) -> dict[str, Any]:
        out = {k: r[k] for k in ("id", "workspace_id", "name", "type", "description", "config", "shared", "tags", "owner_id", "etag", "last_test", "created_at", "updated_at")}
        out["has_secret"] = bool(r["secret_id"])
        out["secret_id"] = r["secret_id"]  # an opaque reference, not secret material
        return out

    def get(self, cid: str) -> dict[str, Any]:
        with self.ctx.db.connect() as conn:
            row = get_by_id(conn, S.connections, cid)
        if not row:
            raise NotFound(f"connection {cid} not found")
        return row

    def find(self, workspace_id: str | None, name: str) -> dict[str, Any] | None:
        with self.ctx.db.connect() as conn:
            stmt = select(S.connections).where(S.connections.c.name == name, S.connections.c.deleted_at.is_(None))
            stmt = stmt.where(S.connections.c.workspace_id == workspace_id) if workspace_id else stmt.where(S.connections.c.workspace_id.is_(None))
            return fetch_one(conn, stmt)

    def resolve(self, ref: str, workspace_id: str | None = None) -> dict[str, Any]:
        """Resolve by id or name; workspace-scoped connections first, then shared ones."""
        if ref.startswith("con_"):
            return self.get(ref)
        row = self.find(workspace_id, ref) if workspace_id else None
        row = row or self.find(None, ref)
        if not row:
            raise NotFound(f"connection '{ref}' not found")
        return row

    def create(self, principal: Principal, *, workspace_id: str | None, name: str, type_: str, config: dict[str, Any], secret_id: str | None,
               description: str = "", shared: bool = False, tags: list[str] | None = None) -> dict[str, Any]:
        _reject_inline_secrets(config)
        _require_platform_for_local(principal, type_)
        if self.find(workspace_id if not shared else None, name):
            raise Conflict(f"connection '{name}' already exists")
        if secret_id:
            self.ctx.secrets.get_meta(secret_id)  # must exist
        now = now_iso()
        row = {"id": new_id("con"), "workspace_id": None if shared else workspace_id, "name": name, "type": type_, "description": description,
               "config": config, "secret_id": secret_id, "shared": shared or workspace_id is None, "tags": tags or [], "owner_id": principal.id,
               "etag": new_etag(), "last_test": None, "created_at": now, "updated_at": now, "deleted_at": None}
        with self.ctx.db.begin() as conn:
            insert_row(conn, S.connections, row)
        self.ctx.audit.record(principal, "connection.created", resource_type="connection", resource_id=row["id"], workspace_id=row["workspace_id"], details={"name": name, "type": type_})
        return row

    def test(self, principal: Principal, cid: str) -> dict[str, Any]:
        row = self.get(cid)
        tester = self.ctx.connection_testers.get(row["type"])
        import time as _t

        t0 = _t.perf_counter()
        if tester is None:
            result = {"ok": True, "detail": f"no active test is available for connection type '{row['type']}'; configuration was validated only", "steps": [], "validated_only": True}
        else:
            secret = self.ctx.secrets.resolve(row["secret_id"]) if row["secret_id"] else None
            try:
                result = tester(self.ctx, row, secret)
            except Exception as exc:  # noqa: BLE001 - tester failure is a test result
                from olcf.core.redaction import redact_text

                result = {"ok": False, "detail": redact_text(f"{type(exc).__name__}: {exc}"), "steps": []}
        result["latency_ms"] = round((_t.perf_counter() - t0) * 1000, 1)
        result["at"] = now_iso()
        with self.ctx.db.begin() as conn:
            conn.execute(update(S.connections).where(S.connections.c.id == cid).values(last_test=result))
        return result


def _require_platform_for_local(principal: Principal, type_: str) -> None:
    """A ``local`` connection grants access to a directory of the *server*, so only platform-level administrators (``connection.manage`` at organisation scope) may create or change one.

    Workspace administrators hold ``connection.manage`` for their workspace only; letting them point a connection at ``/`` or at the storage root would
    give them every other tenant's files and the server's own secrets (SECOPS finding, see ``tests/security/test_references.py``).
    """
    if type_ == "local":
        principal.require("connection.manage")


_SECRET_KEYS = re.compile(r"(?i)(password|secret|token|key$|accountkey|sas|credential|private)")


def _reject_inline_secrets(config: dict[str, Any], path: str = "config") -> None:
    """Secrets must never be stored in connection metadata; they belong in the secret store."""
    for k, v in config.items():
        if isinstance(v, dict):
            _reject_inline_secrets(v, f"{path}.{k}")
        elif isinstance(v, str) and v and _SECRET_KEYS.search(k) and not re.search(r"(?i)(secret_id|_ref$|_name$|_id$|_url$|file$)", k):
            raise ValidationFailed(f"'{path}.{k}' looks like secret material. Store it with POST /v1/secrets and pass secret_id instead.")


@router.get("/connections")
def list_connections(principal: Authed, ctx: Ctx, page: Pagination, workspace_id: str | None = None, type: str | None = None) -> dict[str, Any]:
    stmt = select(S.connections).where(S.connections.c.deleted_at.is_(None))
    visible = principal.visible_workspaces("connection.read")
    if workspace_id:
        principal.require("connection.read", workspace_id=workspace_id)
        stmt = stmt.where(or_(S.connections.c.workspace_id == workspace_id, S.connections.c.workspace_id.is_(None)))
    elif visible is not None:
        stmt = stmt.where(or_(S.connections.c.workspace_id.in_(list(visible)) if visible else False, S.connections.c.workspace_id.is_(None)))
    if type:
        stmt = stmt.where(S.connections.c.type == type)
    with ctx.db.connect() as conn:
        rows, nxt = paginate(conn, stmt, S.connections, limit=page.limit, cursor=page.cursor)
    return {"items": [ConnectionService.public(r) for r in rows], "next_cursor": nxt}


class ConnectionBody(dict):  # placeholder for typing in signature
    """Typing placeholder for connection request bodies in route signatures; the validated models are ``ConnectionCreate`` and ``ConnectionUpdate``.
    """
    pass


from pydantic import BaseModel, Field  # noqa: E402


class ConnectionCreate(BaseModel):
    """Request body that creates a connection; ``secret_id`` points at a stored secret, never at a value."""
    workspace_id: str | None = None
    name: str = Field(min_length=1, max_length=128)
    type: str = Field(min_length=1, max_length=32)
    description: str = ""
    config: dict[str, Any] = Field(default_factory=dict)
    secret_id: str | None = None
    shared: bool = False
    tags: list[str] = Field(default_factory=list)


class ConnectionUpdate(BaseModel):
    """Request body that changes a connection's name, description, configuration, secret reference or tags."""
    name: str | None = None
    description: str | None = None
    config: dict[str, Any] | None = None
    secret_id: str | None = None
    tags: list[str] | None = None


@router.post("/connections", status_code=201)
def create_connection(body: ConnectionCreate, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    if body.shared or not body.workspace_id:
        principal.require("connection.manage")
    else:
        principal.require("connection.manage", workspace_id=body.workspace_id)
    ws_id = None
    if body.workspace_id:
        ws_id = ctx.service("workspaces").resolve(body.workspace_id)["id"]
    if body.secret_id:
        principal.require("secret.use")
    row = ctx.service("connections").create(principal, workspace_id=ws_id, name=body.name, type_=body.type, config=body.config, secret_id=body.secret_id,
                                            description=body.description, shared=body.shared, tags=body.tags)
    return ConnectionService.public(row)


def _conn_perm(principal: Principal, row: dict[str, Any], perm: str) -> None:
    principal.require(perm, workspace_id=row["workspace_id"], connection_id=row["id"]) if row["workspace_id"] else principal.require(perm, connection_id=row["id"])


@router.get("/connections/{cid}")
def get_connection(cid: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    row = ctx.service("connections").get(cid)
    if row["workspace_id"]:
        _conn_perm(principal, row, "connection.read")
    else:
        if not principal.can("connection.read", connection_id=cid) and not principal.visible_workspaces("connection.read"):
            raise Forbidden("permission 'connection.read' is required")
    return ConnectionService.public(row)


@router.patch("/connections/{cid}")
def update_connection(cid: str, body: ConnectionUpdate, principal: Authed, ctx: Ctx, if_match: IfMatch = None) -> dict[str, Any]:
    row = ctx.service("connections").get(cid)
    _conn_perm(principal, row, "connection.manage")
    _require_platform_for_local(principal, row["type"])
    check_if_match(if_match, row["etag"])
    vals = {k: v for k, v in body.model_dump().items() if v is not None}
    if "config" in vals:
        _reject_inline_secrets(vals["config"])
    if "secret_id" in vals:
        ctx.secrets.get_meta(vals["secret_id"])
    if vals:
        with ctx.db.begin() as conn:
            conn.execute(update(S.connections).where(S.connections.c.id == cid).values(**vals, etag=new_etag(), updated_at=now_iso()))
        ctx.audit.record(principal, "connection.updated", resource_type="connection", resource_id=cid, workspace_id=row["workspace_id"], details={"fields": sorted(vals)})
    return ConnectionService.public(ctx.service("connections").get(cid))


@router.delete("/connections/{cid}", status_code=204)
def delete_connection(cid: str, principal: Authed, ctx: Ctx) -> None:
    row = ctx.service("connections").get(cid)
    _conn_perm(principal, row, "connection.manage")
    with ctx.db.begin() as conn:
        conn.execute(update(S.connections).where(S.connections.c.id == cid).values(deleted_at=now_iso(), etag=new_etag()))
    ctx.audit.record(principal, "connection.deleted", resource_type="connection", resource_id=cid, workspace_id=row["workspace_id"], details={"name": row["name"]})


@router.post("/connections/{cid}/test")
def test_connection(cid: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    row = ctx.service("connections").get(cid)
    _conn_perm(principal, row, "connection.manage")
    return ctx.service("connections").test(principal, cid)
