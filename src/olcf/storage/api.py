# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""REST API for lakehouses: files, tables, maintenance, provider catalogue (mounted under ``/v1``).

Every handler authorises with the permission appropriate to the operation against the *stored* lakehouse's workspace
(never a client-supplied one).  Bulk data only moves through the explicit upload / download / preview endpoints, which
stream, size-limit and never buffer whole files in memory.
"""

from __future__ import annotations

import heapq
import mimetypes
import re
from collections.abc import Iterator
from typing import Any

from fastapi import APIRouter, Query, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import or_, select
from starlette.concurrency import iterate_in_threadpool, run_in_threadpool

from olcf.api.deps import Authed, Ctx, Pagination
from olcf.core.context import AppContext
from olcf.core.errors import BadRequest, NotFound
from olcf.core.models import ItemCreate, ItemType
from olcf.db import schema as S
from olcf.db.util import paginate
from olcf.security.rbac import Principal
from olcf.storage import delta_policy as policy
from olcf.storage import paths as P
from olcf.storage.errors import PayloadTooLarge, storage_errors
from olcf.storage.lakehouse import Lakehouse
from olcf.storage.preview import preview_file
from olcf.storage.service import StorageService

router = APIRouter(tags=["lakehouses"])
CHUNK = 1024 * 1024
MAX_SCAN = 500_000


class _Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


# ----------------------------------------------------------------------------- helpers
def _svc(ctx: AppContext) -> StorageService:
    return ctx.service("storage")


def _lakehouse(ctx: AppContext, principal: Principal, lh_id: str, perm: str = "lakehouse.read") -> Lakehouse:
    item = ctx.service("items").get(lh_id)
    if item["type"] != "lakehouse":
        raise NotFound(f"lakehouse {lh_id} not found")
    principal.require(perm, workspace_id=item["workspace_id"], item_id=item["id"])
    return _svc(ctx).lakehouse_from_item(item)


def _audit(ctx: AppContext, principal: Principal, lh: Lakehouse, action: str, **details: Any) -> None:
    ctx.audit.record(principal, action, resource_type="lakehouse", resource_id=lh.id, workspace_id=lh.workspace_id, details=details)


def _destructive(ctx: AppContext, principal: Principal, lh: Lakehouse, operation: str, **details: Any) -> None:
    ctx.audit.record(principal, "data.destructive", resource_type="lakehouse", resource_id=lh.id, workspace_id=lh.workspace_id,
                     details={"operation": operation, **details})


def _file_path(path: str | None, *, mutating: bool = False, what: str = "path") -> str:
    rel = P.normalize_rel(path, what=what)
    if mutating:
        P.require_area(rel, ("Files",), what=what)
        if rel == "Files":
            raise BadRequest("operate on entries inside Files/, not on the area itself")
    return rel


# ----------------------------------------------------------------------------- providers
@router.get("/storage/providers")
def storage_providers(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Provider types, capabilities and the form fields the UI needs to build connection/lakehouse forms."""
    svc = _svc(ctx)
    return {"items": svc.describe_providers(), "plugin_errors": svc.registry.plugin_errors,
            "interop_profiles": [{"id": k, "label": v["label"]} for k, v in policy.PROFILES.items()]}


# ----------------------------------------------------------------------------- lakehouse CRUD
class LakehouseCreate(_Body):
    """Request body for creating a lakehouse: name, description, storage connection and root location."""
    name: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z][A-Za-z0-9_]*$")
    description: str = ""
    connection_id: str | None = None
    root_uri: str | None = None
    default_format: str = "delta"
    schema_enabled: bool = False
    catalog: str = "default"
    folder: str = ""
    tags: list[str] = Field(default_factory=list)
    fabric: dict[str, str] | None = None


def _list_rows(ctx: AppContext, principal: Principal, *, workspace_id: str | None, q: str | None, limit: int, cursor: str | None) -> tuple[list[dict[str, Any]], str | None]:
    visible = principal.visible_workspaces("lakehouse.read")
    stmt = select(S.items).where(S.items.c.type == "lakehouse", S.items.c.deleted_at.is_(None))
    if workspace_id:
        principal.require("lakehouse.read", workspace_id=workspace_id)
        stmt = stmt.where(S.items.c.workspace_id == workspace_id)
    elif visible is not None:
        if not visible:
            return [], None
        stmt = stmt.where(S.items.c.workspace_id.in_(list(visible)))
    if q:
        stmt = stmt.where(or_(S.items.c.name.like(f"%{q}%"), S.items.c.description.like(f"%{q}%")))
    with ctx.db.connect() as conn:
        return paginate(conn, stmt, S.items, limit=limit, cursor=cursor)


@router.get("/lakehouses")
def list_lakehouses(principal: Authed, ctx: Ctx, page: Pagination, workspace_id: str | None = None, q: str | None = None) -> dict[str, Any]:
    rows, nxt = _list_rows(ctx, principal, workspace_id=workspace_id, q=q, limit=page.limit, cursor=page.cursor)
    return {"items": [_svc(ctx).lakehouse_from_item(r).to_public() for r in rows], "next_cursor": nxt}


@router.get("/workspaces/{ws}/lakehouses")
def list_workspace_lakehouses(ws: str, principal: Authed, ctx: Ctx, page: Pagination, q: str | None = None) -> dict[str, Any]:
    wid = ctx.service("workspaces").resolve(ws)["id"]
    rows, nxt = _list_rows(ctx, principal, workspace_id=wid, q=q, limit=page.limit, cursor=page.cursor)
    return {"items": [_svc(ctx).lakehouse_from_item(r).to_public() for r in rows], "next_cursor": nxt}


@router.post("/workspaces/{ws}/lakehouses", status_code=201)
def create_lakehouse(ws: str, body: LakehouseCreate, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    wid = ctx.service("workspaces").resolve(ws)["id"]
    principal.require("item.create", workspace_id=wid)
    if body.connection_id:
        conn = ctx.service("connections").get(body.connection_id)
        principal.require("connection.use", workspace_id=wid, connection_id=conn["id"]) if conn["workspace_id"] else principal.require("connection.use", connection_id=conn["id"])
    definition: dict[str, Any] = {"storage": {"connection_id": body.connection_id, "root_uri": body.root_uri or ""}, "default_format": body.default_format,
                                  "schema_enabled": body.schema_enabled, "catalog": body.catalog, "fabric": body.fabric}
    item = ctx.service("items").create(principal, wid, ItemCreate(type=ItemType.LAKEHOUSE, name=body.name, description=body.description, folder=body.folder,
                                                                  tags=body.tags, definition=definition))
    response.headers["ETag"] = f'"{item["etag"]}"'
    return _svc(ctx).lakehouse_from_item(item).to_public()


@router.get("/lakehouses/{lh_id}")
def get_lakehouse(lh_id: str, principal: Authed, ctx: Ctx, response: Response) -> dict[str, Any]:
    lh = _lakehouse(ctx, principal, lh_id)
    response.headers["ETag"] = f'"{lh.item["etag"]}"'
    return lh.to_public()


@router.delete("/lakehouses/{lh_id}", status_code=204)
def delete_lakehouse(lh_id: str, principal: Authed, ctx: Ctx) -> None:
    lh = _lakehouse(ctx, principal, lh_id, "item.delete")
    ctx.service("items").delete(principal, lh.id)
    _svc(ctx).invalidate(lh.id)


@router.post("/lakehouses/{lh_id}/test")
def test_lakehouse(lh_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Stepwise connectivity test (resolve DNS, connect, auth, list, write, read, delete) from the control plane."""
    lh = _lakehouse(ctx, principal, lh_id, "lakehouse.write")
    res = _svc(ctx).test(lh)
    _audit(ctx, principal, lh, "lakehouse.tested", ok=res["ok"])
    return res


# ----------------------------------------------------------------------------- files
def _page_of_files(it: Iterator[Any], limit: int, cursor: str | None) -> tuple[list[dict[str, Any]], str | None, bool]:
    """Deterministic page (sorted by path, directories included) using a bounded heap; ``scan_capped`` flags a truncated scan."""
    scanned = 0
    capped = False

    def gen() -> Iterator[Any]:
        nonlocal scanned, capped
        for f in it:
            scanned += 1
            if scanned > MAX_SCAN:
                capped = True
                return
            if cursor is None or f.path > cursor:
                yield f

    best = heapq.nsmallest(limit + 1, gen(), key=lambda f: f.path)
    nxt = best[limit - 1].path if len(best) > limit else None
    return [f.to_dict() for f in best[:limit]], nxt, capped


@router.get("/lakehouses/{lh_id}/files")
def list_files(lh_id: str, principal: Authed, ctx: Ctx, page: Pagination, path: str = "", recursive: bool = False) -> dict[str, Any]:
    lh = _lakehouse(ctx, principal, lh_id)
    rel = _file_path(path)
    fs = _svc(ctx).filesystem(lh)
    with storage_errors():
        if rel and not fs.exists(rel):
            raise NotFound(f"'{rel}' does not exist")
        if not rel:  # lakehouse root: always show the two managed areas
            items = [{"path": a, "name": a, "is_dir": True, "size": 0, "modified": None, "etag": None} for a in P.AREAS]
            return {"items": items, "next_cursor": None, "path": "", "provider": lh.provider_id}
        items, nxt, capped = _page_of_files(fs.list(rel, recursive), page.limit, page.cursor)
    return {"items": items, "next_cursor": nxt, "path": rel, "provider": lh.provider_id, "scan_capped": capped}


@router.get("/lakehouses/{lh_id}/files/stat")
def stat_file(lh_id: str, path: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    lh = _lakehouse(ctx, principal, lh_id)
    with storage_errors():
        return _svc(ctx).filesystem(lh).stat(_file_path(path)).to_dict()


def _limit_bytes(ctx: AppContext) -> int:
    return int(ctx.settings.storage.max_upload_mb) * 1024 * 1024


async def _write_stream(ctx: AppContext, lh: Lakehouse, rel: str, chunks: Any, limit: int, overwrite: bool) -> int:
    """Stream ``chunks`` into the provider via a writer; enforces ``limit`` and aborts (no partial object) on any failure."""
    fs = _svc(ctx).filesystem(lh)
    writer = await run_in_threadpool(fs.open_write, rel, overwrite)
    total = 0
    try:
        async for chunk in chunks:
            if not chunk:
                continue
            total += len(chunk)
            if total > limit:
                raise PayloadTooLarge(f"upload exceeds the limit of {limit // (1024 * 1024)} MiB", remediation="raise storage.max_upload_mb or load large files with a pipeline")
            await run_in_threadpool(writer.write, chunk)
        await run_in_threadpool(writer.close)
    except BaseException:
        await run_in_threadpool(writer.abort)
        raise
    return total


@router.put("/lakehouses/{lh_id}/files/content", status_code=201)
async def upload_stream(lh_id: str, path: str, request: Request, principal: Authed, ctx: Ctx, overwrite: bool = True) -> dict[str, Any]:
    """Streamed upload: the raw request body becomes the file (no buffering of the whole body in memory)."""
    lh = await run_in_threadpool(_lakehouse, ctx, principal, lh_id, "lakehouse.write")
    rel = _file_path(path, mutating=True)
    limit = _limit_bytes(ctx)
    cl = request.headers.get("content-length")
    if cl and cl.isdigit() and int(cl) > limit:
        raise PayloadTooLarge(f"upload exceeds the limit of {limit // (1024 * 1024)} MiB")
    with storage_errors():
        total = await _write_stream(ctx, lh, rel, request.stream(), limit, overwrite)
    _audit(ctx, principal, lh, "lakehouse.file_uploaded", path=rel, bytes=total)
    return {"path": rel, "size": total}


@router.post("/lakehouses/{lh_id}/files/upload", status_code=201)
async def upload_multipart(lh_id: str, request: Request, principal: Authed, ctx: Ctx, path: str = Query("Files", description="target directory (inside Files/)"),
                           overwrite: bool = True) -> dict[str, Any]:
    """Browser-friendly ``multipart/form-data`` upload of one or more files into a directory."""
    lh = await run_in_threadpool(_lakehouse, ctx, principal, lh_id, "lakehouse.write")
    target_dir = _file_path(path, mutating=False)
    P.require_area(target_dir, ("Files",), what="path")
    limit = _limit_bytes(ctx)
    cl = request.headers.get("content-length")
    if not cl or not cl.isdigit():
        raise BadRequest("multipart uploads need a Content-Length header", remediation="use PUT /files/content for chunked/streamed uploads")
    if int(cl) > limit + 1024 * 1024:
        raise PayloadTooLarge(f"upload exceeds the limit of {limit // (1024 * 1024)} MiB")
    form = await request.form(max_files=100, max_fields=20)
    uploaded: list[dict[str, Any]] = []
    try:
        for key, val in form.multi_items():
            if not hasattr(val, "filename"):
                continue
            name = (val.filename or key).replace("\\", "/").rsplit("/", 1)[-1]
            rel = P.join_rel(target_dir, name)
            P.require_area(rel, ("Files",), what="path")

            async def reader(f: Any = val) -> Any:
                while True:
                    b = await f.read(CHUNK)
                    if not b:
                        return
                    yield b

            with storage_errors():
                total = await _write_stream(ctx, lh, rel, reader(), limit, overwrite)
            uploaded.append({"path": rel, "size": total})
    finally:
        await form.close()
    if not uploaded:
        raise BadRequest("no file part found in the multipart body")
    _audit(ctx, principal, lh, "lakehouse.file_uploaded", files=[u["path"] for u in uploaded], bytes=sum(u["size"] for u in uploaded))
    return {"uploaded": uploaded}


_RANGE = re.compile(r"^bytes=(\d*)-(\d*)$")


@router.get("/lakehouses/{lh_id}/files/content")
def download_file(lh_id: str, path: str, request: Request, principal: Authed, ctx: Ctx) -> Response:
    """Streamed download (supports a single ``Range`` for resumable downloads)."""
    lh = _lakehouse(ctx, principal, lh_id)
    rel = _file_path(path)
    fs = _svc(ctx).filesystem(lh)
    with storage_errors():
        info = fs.stat(rel)
        if info.is_dir:
            raise BadRequest(f"'{rel}' is a directory")
        start, length, status = 0, info.size, 200
        rng = request.headers.get("range")
        if rng:
            m = _RANGE.match(rng.strip())
            if not m or (not m.group(1) and not m.group(2)):
                raise BadRequest("unsupported Range header (only a single 'bytes=a-b' range is supported)")
            if m.group(1):
                start = int(m.group(1))
                end = int(m.group(2)) if m.group(2) else info.size - 1
            else:
                start = max(0, info.size - int(m.group(2)))
                end = info.size - 1
            if start >= info.size or end < start:
                return Response(status_code=416, headers={"Content-Range": f"bytes */{info.size}"})
            end = min(end, info.size - 1)
            length, status = end - start + 1, 206
        reader = fs.open_read(rel, offset=start, length=length if status == 206 else None)

    def gen() -> Iterator[bytes]:
        try:
            left = length
            while left > 0:
                b = reader.read(min(CHUNK, left))
                if not b:
                    break
                left -= len(b)
                yield b
        finally:
            reader.close()

    name = rel.rsplit("/", 1)[-1]
    safe = re.sub(r'[^A-Za-z0-9._-]', "_", name)
    headers = {"Content-Disposition": f'attachment; filename="{safe}"', "Content-Length": str(length), "Accept-Ranges": "bytes",
               "Content-Security-Policy": "default-src 'none'; sandbox", "ETag": f'"{info.etag}"' if info.etag else ""}
    if status == 206:
        headers["Content-Range"] = f"bytes {start}-{start + length - 1}/{info.size}"
    if not headers["ETag"]:
        headers.pop("ETag")
    _audit(ctx, principal, lh, "lakehouse.file_downloaded", path=rel, bytes=length)
    return StreamingResponse(iterate_in_threadpool(gen()), status_code=status, media_type=mimetypes.guess_type(name)[0] or "application/octet-stream", headers=headers)


class MkdirBody(_Body):
    """Request body for creating a directory."""
    path: str


class MoveBody(_Body):
    """Request body for moving or renaming a file or directory, optionally overwriting."""
    src: str
    dst: str
    overwrite: bool = False


@router.post("/lakehouses/{lh_id}/files/mkdir", status_code=201)
def mkdir(lh_id: str, body: MkdirBody, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    lh = _lakehouse(ctx, principal, lh_id, "lakehouse.write")
    rel = _file_path(body.path, mutating=True)
    with storage_errors():
        _svc(ctx).filesystem(lh).mkdirs(rel)
    _audit(ctx, principal, lh, "lakehouse.dir_created", path=rel)
    return {"path": rel}


@router.delete("/lakehouses/{lh_id}/files")
def delete_path(lh_id: str, path: str, principal: Authed, ctx: Ctx, recursive: bool = False) -> dict[str, Any]:
    lh = _lakehouse(ctx, principal, lh_id, "lakehouse.write")
    rel = _file_path(path, mutating=True)
    if recursive:
        principal.require("lakehouse.destructive", workspace_id=lh.workspace_id, item_id=lh.id)
    with storage_errors():
        n = _svc(ctx).filesystem(lh).delete(rel, recursive=recursive)
    if n == 0:
        raise NotFound(f"'{rel}' does not exist")
    _audit(ctx, principal, lh, "lakehouse.file_deleted", path=rel, objects=n, recursive=recursive)
    if recursive or n > 1:
        _destructive(ctx, principal, lh, "delete_path", path=rel, objects=n)
    return {"path": rel, "deleted": n}


@router.post("/lakehouses/{lh_id}/files/move")
def move_path(lh_id: str, body: MoveBody, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    lh = _lakehouse(ctx, principal, lh_id, "lakehouse.write")
    src, dst = _file_path(body.src, mutating=True, what="src"), _file_path(body.dst, mutating=True, what="dst")
    if body.overwrite:
        principal.require("lakehouse.destructive", workspace_id=lh.workspace_id, item_id=lh.id)
    with storage_errors():
        _svc(ctx).filesystem(lh).move(src, dst, overwrite=body.overwrite)
    _audit(ctx, principal, lh, "lakehouse.file_moved", src=src, dst=dst)
    return {"src": src, "dst": dst}


@router.post("/lakehouses/{lh_id}/files/copy")
def copy_path(lh_id: str, body: MoveBody, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    lh = _lakehouse(ctx, principal, lh_id, "lakehouse.write")
    src = _file_path(body.src, what="src")
    dst = _file_path(body.dst, mutating=True, what="dst")
    if body.overwrite:
        principal.require("lakehouse.destructive", workspace_id=lh.workspace_id, item_id=lh.id)
    with storage_errors():
        n = _svc(ctx).filesystem(lh).copy(src, dst, overwrite=body.overwrite)
    _audit(ctx, principal, lh, "lakehouse.file_copied", src=src, dst=dst, files=n)
    return {"src": src, "dst": dst, "files_copied": n}


@router.get("/lakehouses/{lh_id}/files/preview")
def preview_path(lh_id: str, path: str, principal: Authed, ctx: Ctx, rows: int = Query(100, ge=1, le=1000), bytes: int = Query(1 << 20, ge=1024, le=8 << 20)) -> dict[str, Any]:
    lh = _lakehouse(ctx, principal, lh_id)
    with storage_errors():
        return preview_file(_svc(ctx).filesystem(lh), _file_path(path), rows=rows, max_bytes=bytes)


# ----------------------------------------------------------------------------- tables
def _tbl(ctx: AppContext) -> Any:
    return _svc(ctx).tables


@router.get("/lakehouses/{lh_id}/tables")
def list_tables(lh_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    lh = _lakehouse(ctx, principal, lh_id)
    with storage_errors():
        return {"items": _tbl(ctx).discover(lh)}


@router.get("/lakehouses/{lh_id}/tables/{name}")
def table_detail(lh_id: str, name: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    lh = _lakehouse(ctx, principal, lh_id)
    with storage_errors():
        return _tbl(ctx).info(lh, name)


@router.get("/lakehouses/{lh_id}/tables/{name}/protocol")
def table_protocol(lh_id: str, name: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Delta protocol, reader/writer features and the interoperability-policy verdict per engine and runtime profile."""
    lh = _lakehouse(ctx, principal, lh_id)
    with storage_errors():
        return _tbl(ctx).protocol(lh, name)


@router.get("/lakehouses/{lh_id}/tables/{name}/history")
def table_history(lh_id: str, name: str, principal: Authed, ctx: Ctx, limit: int = Query(30, ge=1, le=500)) -> dict[str, Any]:
    lh = _lakehouse(ctx, principal, lh_id)
    with storage_errors():
        return {"items": _tbl(ctx).history(lh, name, limit)}


@router.get("/lakehouses/{lh_id}/tables/{name}/preview")
def table_preview(lh_id: str, name: str, principal: Authed, ctx: Ctx, rows: int = Query(100, ge=1, le=5000), version: int | None = None,
                  as_of: str | None = None, snapshot_id: int | None = None, columns: str | None = None) -> dict[str, Any]:
    lh = _lakehouse(ctx, principal, lh_id)
    cols = [c for c in (columns or "").split(",") if c] or None
    with storage_errors():
        return _tbl(ctx).preview(lh, name, rows, version=version, as_of=as_of, snapshot_id=snapshot_id, columns=cols)


class OptimizeBody(_Body):
    """Request body for compacting a Delta table: Z-order columns, partition filters and target file size."""
    z_order_by: list[str] = Field(default_factory=list)
    partition_filters: list[list[Any]] | None = None
    target_size: int | None = Field(None, ge=1024 * 1024)


@router.post("/lakehouses/{lh_id}/tables/{name}/optimize")
def table_optimize(lh_id: str, name: str, body: OptimizeBody, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    lh = _lakehouse(ctx, principal, lh_id, "lakehouse.maintain")
    with storage_errors():
        res = _tbl(ctx).optimize(lh, name, z_order_by=body.z_order_by or None, partition_filters=[tuple(f) for f in body.partition_filters] if body.partition_filters else None,
                                 target_size=body.target_size)
    _audit(ctx, principal, lh, "lakehouse.table_optimized", table=name, kind=res["kind"])
    return res


class VacuumBody(_Body):
    """Request body for vacuuming a Delta table: retention hours and a dry-run switch (on by default)."""
    retention_hours: int | None = Field(None, ge=0)
    dry_run: bool = True


@router.post("/lakehouses/{lh_id}/tables/{name}/vacuum")
def table_vacuum(lh_id: str, name: str, body: VacuumBody, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Remove unreferenced files. Dry-run by default; retention below the platform floor (168 h) is refused."""
    lh = _lakehouse(ctx, principal, lh_id, "lakehouse.destructive")
    with storage_errors():
        res = _tbl(ctx).vacuum(lh, name, retention_hours=body.retention_hours, dry_run=body.dry_run)
    if not body.dry_run:
        _destructive(ctx, principal, lh, "vacuum", table=name, files=res["files_count"], retention_hours=res["retention_hours"])
    else:
        _audit(ctx, principal, lh, "lakehouse.table_vacuum_dry_run", table=name, files=res["files_count"])
    return res


class CreateTableBody(_Body):
    """Request body for creating a table from files: name, source path, format and write mode."""
    name: str
    source: str = Field(description="file, directory or glob below Files/")
    format: str | None = Field(None, pattern="^(csv|parquet|json)$")
    mode: str = Field("error", pattern="^(error|append|overwrite)$")
    partition_by: list[str] = Field(default_factory=list)
    options: dict[str, Any] = Field(default_factory=dict)


@router.post("/lakehouses/{lh_id}/tables", status_code=201)
def create_table(lh_id: str, body: CreateTableBody, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Create (or append to / overwrite) a Delta table from CSV/Parquet/JSON files under Files/."""
    lh = _lakehouse(ctx, principal, lh_id, "lakehouse.write")
    if body.mode == "overwrite":
        principal.require("lakehouse.destructive", workspace_id=lh.workspace_id, item_id=lh.id)
    with storage_errors():
        res = _tbl(ctx).create_from_files(lh, body.name, body.source, fmt=body.format, mode=body.mode, partition_by=body.partition_by or None, options=body.options)
    _audit(ctx, principal, lh, "lakehouse.table_created", table=body.name, mode=body.mode, source=body.source, version=res["version"])
    cat = ctx.maybe_service("catalog")
    if cat is not None:
        cat.sync_lakehouse(lh, principal=principal, tables=[body.name])
        cat.record_lineage(principal, workspace_id=lh.workspace_id, sources=[{"kind": "file", "lakehouse_id": lh.id, "path": body.source}],
                           targets=[{"kind": "table", "lakehouse_id": lh.id, "name": body.name}], via={"kind": "api", "name": "create table from files"})
    return res


@router.delete("/lakehouses/{lh_id}/tables/{name}")
def drop_table(lh_id: str, name: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    lh = _lakehouse(ctx, principal, lh_id, "lakehouse.destructive")
    with storage_errors():
        n = _tbl(ctx).drop(lh, name)
    _destructive(ctx, principal, lh, "drop_table", table=name, objects=n)
    cat = ctx.maybe_service("catalog")
    if cat is not None:
        cat.sync_lakehouse(lh, principal=principal)
    return {"name": name, "dropped": True, "objects_deleted": n}


@router.post("/lakehouses/{lh_id}/refresh")
def refresh_lakehouse(lh_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """Re-discover tables in storage and sync the logical catalog."""
    lh = _lakehouse(ctx, principal, lh_id, "lakehouse.read")
    cat = ctx.maybe_service("catalog")
    with storage_errors():
        if cat is None:
            return {"tables": _tbl(ctx).discover(lh), "catalog": None}
        principal.require("lakehouse.maintain", workspace_id=lh.workspace_id, item_id=lh.id)
        return cat.sync_lakehouse(lh, principal=principal)


# silence "unused" for symbols re-exported for tests
