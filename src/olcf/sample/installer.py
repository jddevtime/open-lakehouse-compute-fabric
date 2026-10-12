# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Installs the sample retail workspace: lakehouse + Delta tables + raw files + catalog metadata + lineage + saved queries + report + notebooks.

:func:`install_sample` is idempotent and safe to call concurrently:

* a fresh install creates everything;
* calling it again changes nothing (data, edited notebooks, queries and the report are left alone) and reports ``action: "unchanged"``;
* when the Intelligence module is installed the sample is also scanned and curated there (glossary, domain, data product, certification, a synthetic Power BI scan; see
  :mod:`olcf.sample.intelligence`); that part never fails the install and reports its outcome under ``intelligence``;
* ``reinstall=True`` rewrites the data (a new Delta commit per table) and resets the notebooks, queries and report to their original definitions;
* a dataset ``VERSION`` newer than the installed one is reported as ``upgrade_available`` and applied only with ``reinstall=True``.

Authorisation: a new workspace needs ``workspace.create`` (the creator becomes its WorkspaceAdmin, exactly as with ``POST /v1/workspaces``); an existing
workspace needs ``item.create`` + ``lakehouse.write`` (+ ``item.update`` when items are changed).  Nothing is read or written without it.
"""

from __future__ import annotations

import dataclasses
import json
import logging
import threading
import time
from typing import Any

from olcf.core.errors import BadRequest, Conflict, NotFound
from olcf.core.models import ItemCreate, ItemType, ItemUpdate, Role, WorkspaceCreate
from olcf.sample import generate, notebooks, reports, spec
from olcf.sample import intelligence as sample_intelligence
from olcf.security.rbac import Binding, Principal

log = logging.getLogger("olcf.sample")

MARKER = "Files/_olcf/sample-install.json"
_LOCKS: dict[str, threading.Lock] = {}
_LOCKS_GUARD = threading.Lock()
_PROGRESS: dict[str, dict[str, Any]] = {}


def _lock_for(key: str) -> threading.Lock:
    with _LOCKS_GUARD:
        return _LOCKS.setdefault(key, threading.Lock())


def _key(workspace_name: str, lakehouse_name: str) -> str:
    return f"{workspace_name}/{lakehouse_name}"


def _step(key: str, step: str, **extra: Any) -> None:
    _PROGRESS[key] = {"running": True, "step": step, "since": time.time(), **extra}
    log.info("sample install %s: %s", key, step)


def _check_names(workspace_name: str, lakehouse_name: str, size: str) -> None:
    import re

    if not re.match(r"^[A-Za-z0-9][A-Za-z0-9._ -]{0,63}$", workspace_name or ""):
        raise BadRequest("invalid workspace name", remediation="use letters, digits, spaces and . _ - (at most 64 characters)")
    if not re.match(r"^[A-Za-z][A-Za-z0-9_]{0,63}$", lakehouse_name or ""):
        raise BadRequest("invalid lakehouse name", remediation="start with a letter; use letters, digits and underscores (SQL references it as <lakehouse>.<table>)")
    if size not in spec.SIZES:
        raise BadRequest(f"unknown size '{size}'", remediation=f"choose one of {sorted(spec.SIZES)}")


def _read_marker(storage: Any, lh: Any) -> dict[str, Any] | None:
    try:
        if not storage.exists(lh, MARKER):
            return None
        data = json.loads(storage.read_bytes(lh, MARKER, max_bytes=1_000_000))
        return data if isinstance(data, dict) else None
    except (NotFound, ValueError):
        return None


def _as_workspace_admin(principal: Principal, workspace_id: str) -> Principal:
    """The principal as a fresh request would see it after it created ``workspace_id`` (the creator is bound as WorkspaceAdmin)."""
    binding = Binding(Role.WORKSPACE_ADMIN.value, "workspace", workspace_id, "system")
    return dataclasses.replace(principal, bindings=[*principal.bindings, binding])


# ----------------------------------------------------------------------------- status
def status(ctx: Any, principal: Principal, *, workspace_name: str = spec.WORKSPACE, lakehouse_name: str = spec.LAKEHOUSE) -> dict[str, Any]:
    """Where the sample stands: not installed / installed (and whether it is up to date) plus what it contains.  Invisible workspaces read as not installed."""
    key = _key(workspace_name, lakehouse_name)
    out: dict[str, Any] = {"workspace": workspace_name, "lakehouse": lakehouse_name, "installed": False, "current_version": spec.VERSION, "installed_version": None,
                           "up_to_date": False, "running": bool(_PROGRESS.get(key, {}).get("running")), "step": _PROGRESS.get(key, {}).get("step"),
                           "sizes": sorted(spec.SIZES)}
    try:
        ws = ctx.service("workspaces").resolve(workspace_name)
    except NotFound:
        return out
    if not principal.can("workspace.read", workspace_id=ws["id"]):
        return out
    out["workspace_id"] = ws["id"]
    items = ctx.service("items")
    lh_item = items.find(ws["id"], "lakehouse", lakehouse_name)
    if lh_item is None:
        return out
    out["lakehouse_id"] = lh_item["id"]
    if principal.can("lakehouse.read", workspace_id=ws["id"], item_id=lh_item["id"]):
        storage = ctx.service("storage")
        marker = _read_marker(storage, storage.lakehouse_from_item(lh_item))
        if marker:
            out.update(installed=True, installed_version=marker.get("version"), up_to_date=marker.get("version") == spec.VERSION, size=marker.get("size"),
                       installed_at=marker.get("installed_at"), tables=[{"name": n, "rows": r} for n, r in (marker.get("rows") or {}).items()])
    names = {t: [r["name"] for r in items.list(ws["id"], type_=t, limit=200, cursor=None)[0]] for t in ("notebook", "sql_query", "report")} if principal.can("item.read", workspace_id=ws["id"]) else {}
    out["items"] = {"notebooks": sorted(names.get("notebook", [])), "queries": sorted(names.get("sql_query", [])), "reports": sorted(names.get("report", []))}
    return out


# ----------------------------------------------------------------------------- install
def install_sample(ctx: Any, principal: Principal, *, workspace_name: str = spec.WORKSPACE, lakehouse_name: str = spec.LAKEHOUSE, reinstall: bool = False,
                   size: str = "standard") -> dict[str, Any]:
    """Install (or verify) the sample workspace; returns a summary ``{action, workspace, lakehouse, tables, items, ...}``."""
    _check_names(workspace_name, lakehouse_name, size)
    key = _key(workspace_name, lakehouse_name)
    lock = _lock_for(key)
    if not lock.acquire(blocking=False):
        raise Conflict("the sample is already being installed", remediation="wait for it to finish (GET /v1/sample/status shows the current step) and retry")
    started = time.monotonic()
    try:
        _step(key, "workspace")
        return _install(ctx, principal, workspace_name, lakehouse_name, reinstall, size, started)
    finally:
        _PROGRESS[key] = {"running": False, "step": None}
        lock.release()


def _install(ctx: Any, principal: Principal, workspace_name: str, lakehouse_name: str, reinstall: bool, size: str, started: float) -> dict[str, Any]:
    key = _key(workspace_name, lakehouse_name)
    workspaces, items, storage, catalog = ctx.service("workspaces"), ctx.service("items"), ctx.service("storage"), ctx.service("catalog")
    summary: dict[str, Any] = {"workspace": workspace_name, "lakehouse": lakehouse_name, "created": [], "updated": [], "unchanged": []}

    # ---- workspace
    try:
        ws = workspaces.resolve(workspace_name)
        principal.require("item.create", workspace_id=ws["id"])
        principal.require("lakehouse.write", workspace_id=ws["id"])
        summary["workspace_created"] = False
    except NotFound:
        principal.require("workspace.create")
        try:
            ws = workspaces.create(principal, WorkspaceCreate(name=workspace_name, description="Sample retail company: Delta tables, notebooks, saved queries and a dashboard.",
                                                              tags=["sample", "retail"]))
            principal = _as_workspace_admin(principal, ws["id"])
            summary["workspace_created"] = True
            summary["created"].append(f"workspace:{workspace_name}")
        except Conflict:                                   # lost a race with another creator
            ws = workspaces.resolve(workspace_name)
            principal.require("item.create", workspace_id=ws["id"])
            principal.require("lakehouse.write", workspace_id=ws["id"])
            summary["workspace_created"] = False
    wid = ws["id"]
    summary["workspace_id"] = wid

    # ---- lakehouse
    _step(key, "lakehouse")
    lh_item = items.find(wid, "lakehouse", lakehouse_name)
    if lh_item is None:
        definition = {"storage": {"connection_id": None, "root_uri": ""}, "default_format": "delta", "schema_enabled": False, "catalog": "default", "fabric": None}
        try:
            lh_item = items.create(principal, wid, ItemCreate(type=ItemType.LAKEHOUSE, name=lakehouse_name, description="Sample retail lakehouse (synthetic data).",
                                                              tags=["sample", "retail"], definition=definition))
            summary["created"].append(f"lakehouse:{lakehouse_name}")
        except Conflict:
            lh_item = items.find(wid, "lakehouse", lakehouse_name)
            if lh_item is None:
                raise
    lh = storage.lakehouse_from_item(lh_item)
    summary["lakehouse_id"] = lh.id

    # ---- data
    marker = _read_marker(storage, lh)
    foreign = marker is None and bool(storage.list_tables(lh))
    if foreign and not reinstall:
        raise Conflict(f"lakehouse '{lakehouse_name}' in workspace '{workspace_name}' already contains tables that are not the sample",
                       remediation="pick another lakehouse_name, or pass reinstall=true to overwrite the sample tables (other tables are not touched)")
    write_data = reinstall or marker is None
    if marker and marker.get("version") != spec.VERSION and not reinstall:
        summary["upgrade_available"] = {"installed_version": marker.get("version"), "current_version": spec.VERSION, "how": "reinstall=true"}
    rows: dict[str, int] = dict(marker.get("rows") or {}) if marker else {}
    if write_data:
        principal.require("lakehouse.write", workspace_id=wid, item_id=lh.id)
        rows = _write_data(storage, lh, size, key)
        summary["created" if marker is None else "updated"].append("data")
    else:
        summary["unchanged"].append("data")
    summary["tables"] = [{"name": n, "rows": r} for n, r in rows.items()]

    # ---- catalog metadata + lineage
    _step(key, "catalog")
    if write_data:
        _describe_catalog(catalog, principal, storage, lh)
    nb_ids = _items(ctx, principal, wid, lh.id, reinstall, summary)

    if write_data:
        _step(key, "lineage")
        _lineage(catalog, principal, wid, lh, nb_ids.get("01_Retail_Medallion_ETL"))

    # ---- workspace default lakehouse
    settings = dict(ws.get("settings") or {})
    if not settings.get("default_lakehouse"):
        workspaces.update(principal, wid, {"settings": {**settings, "default_lakehouse": lh.id}}, None)
        summary["updated"].append("workspace:default_lakehouse")

    # ---- OLCF Intelligence (only when the module is installed; a failure here is reported, never raised)
    if ctx.maybe_service("intelligence") is not None:
        _step(key, "intelligence")
        intel = sample_intelligence.integrate(ctx, principal, workspace_id=wid, workspace_name=workspace_name, lakehouse_id=lh.id, lakehouse_name=lakehouse_name, scan=write_data)
        if intel is not None:
            summary["intelligence"] = intel
            summary["created"].extend(f"intelligence:{c}" for c in intel["created"])

    changed = bool(summary["created"] or summary["updated"])
    summary["action"] = ("installed" if marker is None else "reinstalled") if write_data else ("updated" if changed else "unchanged")
    summary["elapsed_ms"] = int((time.monotonic() - started) * 1000)
    summary["try_next"] = [f"SELECT * FROM {lakehouse_name}.gold_daily_sales ORDER BY sale_date DESC LIMIT 10", "open the report 'Retail Sales Overview'", "run notebook 02_Retail_Explore_Python"]  # noqa: S608
    ctx.audit.record(principal, "sample.install", resource_type="workspace", resource_id=wid, workspace_id=wid,
                     details={"action": summary["action"], "lakehouse": lakehouse_name, "size": size, "reinstall": reinstall})
    return summary


def _write_data(storage: Any, lh: Any, size: str, key: str) -> dict[str, int]:
    _step(key, "generating data")
    tables = generate.generate(size, spec.SEED)
    for name in spec.table_order():
        _step(key, f"writing table {name}")
        t = spec.TABLES[name]
        storage.tables.write_table(lh, name, tables[name], mode="overwrite", partition_by=t["partition_by"] or None, description=t["description"])
    _step(key, "writing raw files")
    for rel, data in generate.raw_csvs(tables).items():
        storage.write_bytes(lh, rel, data)
    storage.write_bytes(lh, "Files/README.md", generate.readme_text(tables, size).encode("utf-8"))
    rows = {k: v.num_rows for k, v in tables.items()}
    marker = {"sample": "retail", "version": spec.VERSION, "seed": spec.SEED, "size": size, "rows": rows,
              "installed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    storage.write_bytes(lh, MARKER, json.dumps(marker, indent=2).encode("utf-8"))
    return rows


def _describe_catalog(catalog: Any, principal: Principal, storage: Any, lh: Any) -> None:
    catalog.sync_lakehouse(lh, None)
    for name, t in spec.TABLES.items():
        entry = catalog.find(lh.id, name)
        if entry is None:
            continue
        catalog.update_entry(principal, entry["id"], description=t["description"], tags=["sample", *t["tags"], f"layer:{t['layer']}"],
                             column_descriptions={c["name"]: c["description"] for c in t["columns"]})


def _lineage(catalog: Any, principal: Principal, wid: str, lh: Any, notebook_id: str | None) -> None:
    def table(name: str) -> dict[str, Any]:
        return {"kind": "table", "lakehouse_id": lh.id, "name": name}

    def file(path: str) -> dict[str, Any]:
        return {"kind": "file", "lakehouse_id": lh.id, "path": path}

    for src, dst in (("Files/raw/sales", "fact_sales"), ("Files/raw/customers", "dim_customer"), ("Files/raw/products", "dim_product"), ("Files/raw/stores", "dim_store")):
        catalog.record_lineage(None, workspace_id=wid, sources=[file(src)], targets=[table(dst)], via={"kind": "sample", "name": "sample generator"}, column_status="not_applicable",
                               authorize=False)
    via = {"kind": "notebook", "name": "01_Retail_Medallion_ETL", **({"ref": notebook_id} if notebook_id else {})}
    for name in ("gold_daily_sales", "gold_category_summary", "gold_customer_ltv"):
        sql = f"CREATE TABLE {name} AS {generate.gold_sql(name, 'duckdb').format(fact='fact_sales', product='dim_product', as_of=spec.AS_OF)}"  # noqa: S608
        catalog.record_lineage_from_sql(None, wid, sql, dialect="duckdb", default_lakehouse_id=lh.id, via=via, authorize=False)


def _items(ctx: Any, principal: Principal, wid: str, lakehouse_id: str, reinstall: bool, summary: dict[str, Any]) -> dict[str, str]:
    """Create the notebooks, saved queries and the report; existing ones are left alone unless ``reinstall``."""
    items = ctx.service("items")
    ids: dict[str, str] = {}
    planned: list[tuple[str, dict[str, Any], str]] = []
    for nb in notebooks.manifest():
        planned.append(("notebook", nb, "sample"))
    for q in reports.saved_queries(lakehouse_id):
        planned.append(("sql_query", q, "sample"))
    planned.append(("report", {"name": reports.REPORT_NAME, "description": "Revenue, orders, customers and AOV with period and category filters.",
                               "tags": ["sample", "retail", "dashboard"], "definition": reports.report_definition(lakehouse_id)}, "sample"))
    for type_, rec, _ in planned:
        name = rec["name"]
        tags = sorted({*rec.get("tags", []), "sample"})
        existing = items.find(wid, type_, name)
        if existing is None:
            try:
                row = items.create(principal, wid, ItemCreate(type=ItemType(type_), name=name, description=rec.get("description", ""), tags=tags, definition=rec["definition"]))
                summary["created"].append(f"{type_}:{name}")
            except Conflict:
                row = items.find(wid, type_, name)
                if row is None:
                    raise
            ids[name] = row["id"]
            continue
        ids[name] = existing["id"]
        if not reinstall:
            continue
        principal.require("item.update", workspace_id=wid, item_id=existing["id"])
        if existing["definition"] != rec["definition"] or existing["description"] != rec.get("description", ""):
            items.update(principal, existing["id"], ItemUpdate(description=rec.get("description", ""), definition=rec["definition"], comment="sample reinstall"), None)
            summary["updated"].append(f"{type_}:{name}")
    summary["items"] = {"notebooks": [n["name"] for n in notebooks.manifest()], "queries": [q["name"] for q in reports.saved_queries(lakehouse_id)], "reports": [reports.REPORT_NAME]}
    summary["item_ids"] = ids
    return ids
