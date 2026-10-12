# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The ``local`` connector: discovery of this OLCF installation itself - no Spark, no worker, no network.

What it reports (all as plain :mod:`olcf.intelligence.model` facts; it never writes to the graph):

* **workspaces** (``workspace`` assets, alias ``olcf_workspace``) and **lakehouses** (alias ``olcf_item``; a lakehouse that records a ``fabric`` mapping also gets the ``fabric`` alias so
  it is one asset with the Fabric lakehouse it mirrors);
* **tables** of every lakehouse - Delta, Iceberg and Parquet - with columns (name, type, nullability, partition flag, description from the catalog), partition columns, Delta table
  properties and protocol, version, history summary, row count / size / file count (Delta log, falling back to the catalog's ``table_stats``) and the metadata profile of
  :mod:`~olcf.intelligence.discovery.profiling` (null counts, safe min/max). Each column is also its own ``column`` asset so column-level lineage has something to attach to;
  ids are the ``lakehouse_table`` / ``lakehouse_column`` aliases plus the ``catalog_entry`` alias and the physical ``delta_location``;
* **items**: notebooks, pipelines, saved SQL queries, dashboards / reports and Spark job definitions as assets *and* as :class:`~olcf.intelligence.model.DefinitionFact` (the item ``etag`` is the
  definition etag, so an unchanged definition is not analysed again), with the workspace's / notebook's default lakehouse in the context so partial table names resolve;
* **files** under ``Files/`` - only the paths a source's ``config.files`` policy lists (a scan never walks a lakehouse's files on its own);
* **recorded catalog lineage** (``config.mirror_catalog_lineage``, default true): the rows of the classic catalog's ``lineage_edges`` of the scanned workspaces, as
  :func:`~olcf.intelligence.discovery.catalog_mirror.lineage_batches` reads them, so the default installation needs no second ``catalog_mirror`` source (which stays available).

Value profiling (``config.value_profiling``) is opt-in and additionally gated by a sampling policy; with the default (no policy) it records a ``sampling_disabled`` gap instead. It never
stores values - see :mod:`~olcf.intelligence.discovery.profiling`.

Gaps that only mean "less detail about an object that was found" (an unreadable table, missing statistics, a workspace of the scope that does not exist) carry
``location["informational"] = True`` and do not stop a full scan from ending ``succeeded``; gaps that mean "objects may have been missed" (a lakehouse whose tables cannot be listed, a
table or file cap, an unreadable listed path) keep the scan ``partial`` and therefore skip the removal sweep.

Incremental scans (``mode = incremental``) use the checkpoint ``ctx.state``: items whose ``etag`` and tables whose version did not change since the previous scan are not reported again.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping
from typing import Any

from sqlalchemy import select

from olcf.core.errors import OlcfError, ValidationFailed
from olcf.core.redaction import redact_text
from olcf.intelligence.discovery import detect, profiling
from olcf.intelligence.discovery.base import (
    Batch,
    ResolvedSecrets,
    ScanContext,
    Step,
    StepFailure,
    StepSkip,
    StepSpec,
    run_steps,
    safe_message,
)
from olcf.intelligence.model import (
    AssetFact,
    AssetRef,
    DefinitionFact,
    Facts,
    Gap,
    fabric_alias,
    normalize_path,
)
from olcf.security.rbac import SYSTEM_PRINCIPAL

log = logging.getLogger("olcf.intelligence.discovery.local")

KIND = "local"
#: OLCF item types that carry a definition for the lineage analyzers: item type -> (asset type, definition kind)
DEFINITION_ITEMS: dict[str, tuple[str, str]] = {
    "notebook": ("notebook", "notebook"),
    "pipeline": ("pipeline", "pipeline_olcf"),
    "sql_query": ("sql_query", "sql_query"),
    "report": ("report", "report_olcf"),
    "spark_job_definition": ("spark_job", "spark_job_olcf"),
}
#: Item types the connector knows it does not model (reported once in the scan statistics, not as gaps).
IGNORED_ITEM_TYPES = frozenset({"environment", "variable_set", "schedule", "extension"})
CONFIG_KEYS = frozenset({"workspace_ids", "files", "value_profiling", "profile_tables", "max_tables_per_lakehouse", "max_files_per_path", "mirror_catalog_lineage"})
DEFAULT_MAX_TABLES = 5000
DEFAULT_MAX_FILES = 500
_PAGE = 200


def lakehouse_ref(lh_id: str, name: str | None = None, workspace_id: str | None = None) -> AssetRef:
    """Reference to a lakehouse asset by its OLCF item id."""
    return AssetRef.make("lakehouse", {"olcf_item": lh_id}, name, {"workspace_id": workspace_id} if workspace_id else None)


def table_aliases(lh_id: str, name: str, location: str | None, catalog_entry: str | None) -> dict[str, str]:
    """Aliases of a lakehouse table: ``lakehouse_table`` (always), ``catalog_entry``, and the normalised physical ``delta_location`` (+ ``onelake_path`` for OneLake URLs)."""
    out = {"lakehouse_table": f"{lh_id}:{name}"}
    if catalog_entry:
        out["catalog_entry"] = catalog_entry
    if location and "://" in location:
        out["delta_location"] = normalize_path(location)
        if "onelake.dfs.fabric.microsoft.com" in location.lower():
            out["onelake_path"] = normalize_path(location)
    return out


class LocalConnector:
    """Discovery of the local platform (see the module docstring). Stateless; one instance per call."""

    kind = KIND
    label = "This OLCF installation: workspaces, lakehouses, tables, notebooks, pipelines, saved queries, dashboards and Spark job definitions"

    # ------------------------------------------------------------------------------------------------------------------------ configuration
    def validate_config(self, config: Mapping[str, Any]) -> dict[str, Any]:
        """Check the non-secret settings: ``workspace_ids`` (list of ids or names; default all), ``files`` (list of ``{"lakehouse", "path", "recursive"}`` policies; default none),
        ``value_profiling`` (bool, default false), ``profile_tables`` (bool, default true), ``max_tables_per_lakehouse`` and ``max_files_per_path``."""
        extra = sorted(set(config) - CONFIG_KEYS)
        if extra:
            raise ValidationFailed(f"unknown setting(s) {extra} for a local source; allowed: {sorted(CONFIG_KEYS)}")
        out: dict[str, Any] = {}
        if "mirror_catalog_lineage" in config:
            if not isinstance(config["mirror_catalog_lineage"], bool):
                raise ValidationFailed("mirror_catalog_lineage must be true or false")
            out["mirror_catalog_lineage"] = config["mirror_catalog_lineage"]
        ws = config.get("workspace_ids")
        if ws is not None:
            if not isinstance(ws, list) or not all(isinstance(x, str) and x for x in ws):
                raise ValidationFailed("workspace_ids must be a list of workspace ids or names")
            out["workspace_ids"] = sorted(set(ws))
        files = config.get("files")
        if files is not None:
            if not isinstance(files, list):
                raise ValidationFailed("files must be a list of {lakehouse, path, recursive} policies")
            cleaned = []
            for i, f in enumerate(files):
                if not isinstance(f, Mapping) or not f.get("lakehouse") or not isinstance(f.get("path"), str):
                    raise ValidationFailed(f"files[{i}] needs lakehouse and path")
                bad = sorted(set(f) - {"lakehouse", "path", "recursive"})
                if bad:
                    raise ValidationFailed(f"files[{i}] has unknown field(s) {bad}")
                path = str(f["path"]).strip().strip("/")
                if not (path == "Files" or path.startswith("Files/")):
                    raise ValidationFailed(f"files[{i}].path must be below 'Files/' (got '{f['path']}')")
                if ".." in path.split("/"):
                    raise ValidationFailed(f"files[{i}].path must not contain '..'")
                cleaned.append({"lakehouse": str(f["lakehouse"]), "path": path, "recursive": bool(f.get("recursive", False))})
            out["files"] = cleaned
        for key in ("value_profiling", "profile_tables"):
            if key in config:
                if not isinstance(config[key], bool):
                    raise ValidationFailed(f"{key} must be true or false")
                out[key] = config[key]
        for key, hi in (("max_tables_per_lakehouse", 100_000), ("max_files_per_path", 10_000)):
            if key in config:
                v = config[key]
                if isinstance(v, bool) or not isinstance(v, int) or not 1 <= v <= hi:
                    raise ValidationFailed(f"{key} must be an integer between 1 and {hi}")
                out[key] = v
        return out

    # ------------------------------------------------------------------------------------------------------------------------ connectivity test
    def test(self, config: Mapping[str, Any], secrets: ResolvedSecrets, *, app: Any = None) -> list[Step]:
        """Steps: 1 platform services present, 2 workspaces readable (count, configured ids exist), 3 every lakehouse's storage answers a listing, 4 catalog present (informational)."""
        state: dict[str, Any] = {}

        def services() -> str:
            if app is None:
                raise StepFailure("the connector was not given the platform context", "this is a programming error; report it")
            missing = [n for n in ("workspaces", "items", "storage") if app.maybe_service(n) is None]
            if missing:
                raise StepFailure(f"the platform service(s) {', '.join(missing)} are not installed", "start the control plane with the core and storage modules")
            return "workspaces, items and storage services are available"

        def workspaces() -> str:
            rows = list(_workspaces(app, config.get("workspace_ids")))
            state["workspaces"] = rows
            wanted = config.get("workspace_ids") or []
            if wanted and len(rows) < len(set(wanted)):
                found = {r["id"] for r in rows} | {r["name"] for r in rows}
                missing = sorted(w for w in set(wanted) if w not in found)
                raise StepFailure(f"workspace(s) not found: {', '.join(missing)}", "fix config.workspace_ids or create the workspaces")
            return f"{len(rows)} workspace(s) in scope"

        def lakehouses() -> str:
            storage = app.service("storage")
            bad: list[str] = []
            total = 0
            for ws in state["workspaces"]:
                for lh in _lakehouses(app, ws["id"]):
                    total += 1
                    try:
                        storage.filesystem(lh).list("", recursive=False)
                    except Exception as exc:  # noqa: BLE001 - reported per lakehouse
                        bad.append(f"{ws['name']}/{lh.name}: {safe_message(exc)}")
            if bad:
                raise StepFailure(f"{len(bad)} of {total} lakehouse(s) cannot be listed: {'; '.join(bad[:3])}", "check the lakehouse's connection with POST /v1/connections/{id}/test")
            return f"{total} lakehouse storage root(s) reachable"

        def catalog() -> str:
            if app.maybe_service("catalog") is None:
                raise StepSkip("the catalog module is not installed; descriptions, tags and catalog statistics are not mirrored")
            return "catalog service available"

        return run_steps([StepSpec("platform services", services), StepSpec("workspaces", workspaces), StepSpec("lakehouse storage", lakehouses), StepSpec("catalog", catalog)])

    # ------------------------------------------------------------------------------------------------------------------------ discovery
    def discover(self, ctx: ScanContext) -> Iterator[Batch]:
        """Stream the installation: per workspace one batch of workspace / lakehouse / item assets, then per lakehouse batches of tables (and listed files), then one
        :class:`DefinitionFact` per notebook / pipeline / query / report / job definition. Cancellation is checked between batches."""
        app = ctx.app
        if app is None:
            raise RuntimeError("the local connector needs the platform context (ScanContext.app)")
        cfg = ctx.config
        seen_items: dict[str, str] = dict(ctx.state.get("items") or {})
        seen_tables: dict[str, Any] = dict(ctx.state.get("tables") or {})
        new_items: dict[str, str] = {}
        new_tables: dict[str, Any] = {}
        selection = _selection(ctx)
        rows = list(_workspaces(app, selection))
        if selection:
            found = {r["id"] for r in rows} | {r["name"] for r in rows}
            for missing in sorted(w for w in selection if w not in found):
                ctx.record_gap("not_inspected", missing, f"workspace '{missing}' of the source scope does not exist", "fix the source's workspace list", location={"workspace": missing, "informational": True})
        for ws in rows:
            ctx.check_cancelled()
            yield from self._workspace(ctx, app, ws, seen_items, seen_tables, new_items, new_tables, cfg)
        if cfg.get("mirror_catalog_lineage", True) and rows and app.maybe_service("catalog") is not None:
            # lineage that components recorded in the classic catalog (pipeline runs, saved queries ...) joins the graph from the same source: no second source is needed
            from olcf.intelligence.discovery.catalog_mirror import lineage_batches

            yield from lineage_batches(ctx, app, frozenset(r["id"] for r in rows))
        ctx.state["items"] = new_items if ctx.is_full or not seen_items else {**seen_items, **new_items}
        ctx.state["tables"] = new_tables if ctx.is_full or not seen_tables else {**seen_tables, **new_tables}

    # ---- one workspace
    def _workspace(self, ctx: ScanContext, app: Any, ws: dict[str, Any], seen_items: dict[str, str], seen_tables: dict[str, Any], new_items: dict[str, str],
                   new_tables: dict[str, Any], cfg: Mapping[str, Any]) -> Iterator[Batch]:
        items_svc = app.service("items")
        ws_ref = AssetRef.make("workspace", {"olcf_workspace": ws["id"]}, ws["name"])
        head = Facts()
        head.assets.append(AssetFact(ws_ref, ws.get("display_name") or ws["name"], {"source_kind": "local", "workspace_name": ws["name"], "owner_id": ws.get("owner_id")},
                                     description=ws.get("description") or None, workspace_id=ws["id"], path=ws["name"], tags=list(ws.get("tags") or [])))
        items = _all_items(items_svc, ws["id"])
        lakehouses = [i for i in items if i["type"] == "lakehouse"]
        by_id = {lh["id"]: lh for lh in lakehouses}
        storage = app.service("storage")
        lh_objs: dict[str, Any] = {}
        for it in lakehouses:
            attrs: dict[str, Any] = {"source_kind": "local", "item_id": it["id"], "item_type": "lakehouse", "item_etag": it["etag"], "updated_at": it.get("updated_at"),
                                     "folder": it.get("folder")}
            aliases = {"olcf_item": it["id"]}
            definition = it.get("definition") or {}
            fab = definition.get("fabric") if isinstance(definition.get("fabric"), dict) else None
            if fab and fab.get("workspace_id") and fab.get("lakehouse_id"):
                aliases["fabric"] = fabric_alias(None, str(fab["workspace_id"]), str(fab["lakehouse_id"]))
                attrs["fabric"] = {"workspace_id": str(fab["workspace_id"]), "lakehouse_id": str(fab["lakehouse_id"])}
            try:
                lh = storage.lakehouse_from_item(it)
                lh_objs[it["id"]] = lh
                attrs.update({"provider": lh.provider_id, "schema_enabled": lh.schema_enabled, "default_format": lh.default_format, "catalog": lh.catalog,
                              "location": redact_text(storage.physical_uri(lh))})
            except Exception as exc:  # noqa: BLE001 - a lakehouse we cannot resolve is a gap, the rest of the workspace is still scanned
                ctx.record_gap("not_inspected", it["name"], f"lakehouse '{it['name']}' could not be resolved: {safe_message(exc)}", "fix the lakehouse's storage settings",
                               location={"workspace_id": ws["id"], "item": it["id"]})
            head.assets.append(AssetFact(AssetRef.make("lakehouse", aliases, it["name"], {"workspace_id": ws["id"]}), it["name"], attrs, parent=ws_ref,
                                         description=it.get("description") or None, workspace_id=ws["id"], path=f"{ws['name']} / {it['name']}", tags=list(it.get("tags") or [])))
        defs: list[DefinitionFact] = []
        ws_default = (ws.get("settings") or {}).get("default_lakehouse")
        for it in items:
            kind = DEFINITION_ITEMS.get(it["type"])
            if it["type"] == "lakehouse":
                continue
            if kind is None:
                ctx.count("items_ignored" if it["type"] in IGNORED_ITEM_TYPES else "items_unknown_type")
                continue
            asset_type, def_kind = kind
            unchanged = not ctx.is_full and seen_items.get(it["id"]) == it["etag"]
            new_items[it["id"]] = it["etag"]
            if unchanged:
                ctx.count("items_unchanged")
                continue
            ref = AssetRef.make(asset_type, {"olcf_item": it["id"]}, it["name"], {"workspace_id": ws["id"]})
            head.assets.append(AssetFact(ref, it["name"], {"source_kind": "local", "item_id": it["id"], "item_type": it["type"], "item_etag": it["etag"],
                                                           "updated_at": it.get("updated_at"), "folder": it.get("folder"), "owner_id": it.get("owner_id")},
                                         parent=AssetRef.make("workspace", {"olcf_workspace": ws["id"]}, ws["name"]), description=it.get("description") or None, workspace_id=ws["id"],
                                         path=f"{ws['name']} / {it['name']}", tags=list(it.get("tags") or [])))
            defs.append(self._definition(it, ref, def_kind, ws, by_id, ws_default, ctx))
        ctx.count("workspaces")
        yield head
        # ---- tables and listed files, one batch per lakehouse
        profiler_open = bool(cfg.get("value_profiling"))
        for it in lakehouses:
            ctx.check_cancelled()
            lh = lh_objs.get(it["id"])
            if lh is None:
                continue
            yield from self._lakehouse(ctx, app, ws, it, lh, seen_tables, new_tables, cfg, profiler_open)
        # ---- definitions last, so the assets they refer to exist when they are analysed
        for d in defs:
            ctx.check_cancelled()
            ctx.count("definitions")
            yield d

    # ---- definitions
    def _definition(self, it: dict[str, Any], ref: AssetRef, kind: str, ws: dict[str, Any], lakehouses: dict[str, dict[str, Any]], ws_default: Any, ctx: ScanContext) -> DefinitionFact:
        definition = it.get("definition") or {}
        context: dict[str, Any] = {"workspace_id": ws["id"], "item_id": it["id"], "item_name": it["name"], "item_type": it["type"], "source": "olcf", "source_id": ctx.source_id,
                                   "dialect": "spark"}
        settings = _dict(definition.get("settings"))
        lh_id = None
        payload: Any = definition
        if it["type"] == "notebook":
            # OLCF stores a notebook item as ``{"nbformat": <nbformat v4 document>, "settings": {...}}`` (CONTRACTS section 10); the analyzers want the nbformat document itself,
            # exactly as they get it from a Fabric notebook. A definition that already is a bare document (``cells`` at the top) is passed through.
            nb = definition.get("nbformat")
            if isinstance(nb, dict):
                payload = nb
            dl = _dict(settings.get("default_lakehouse"))
            lh_id = dl.get("lakehouse_id") or dl.get("id") or dl.get("name")
            if str(settings.get("engine") or "").lower() == "duckdb":
                context["dialect"] = "duckdb"
        elif it["type"] == "sql_query":
            lh_id = definition.get("lakehouse_id")
            context["dialect"] = "spark" if str(definition.get("engine") or "duckdb").lower() == "spark" else "duckdb"
        elif it["type"] == "report":
            lh_id = next((t.get("lakehouse_id") for t in definition.get("tiles") or [] if isinstance(t, dict) and t.get("lakehouse_id")), None)
            context["dialect"] = "duckdb"
        if lh_id is None and isinstance(ws_default, str) and ws_default:
            lh_id = ws_default if ws_default in lakehouses else next((i for i, x in lakehouses.items() if x["name"] == ws_default), None)
        if lh_id is None and len(lakehouses) == 1:
            lh_id = next(iter(lakehouses))
        if isinstance(lh_id, str) and lh_id and lh_id not in lakehouses:
            lh_id = next((i for i, x in lakehouses.items() if x["name"] == lh_id), lh_id)            # a default lakehouse may be given by name
        if lh_id and lh_id in lakehouses:
            context["default_lakehouse"] = {"id": lh_id, "name": lakehouses[lh_id]["name"]}
        elif isinstance(lh_id, str) and lh_id:
            context["default_lakehouse"] = {"name": lh_id}
        return DefinitionFact(kind, ref, it["name"], payload, it["etag"], context)

    # ---- one lakehouse
    def _lakehouse(self, ctx: ScanContext, app: Any, ws: dict[str, Any], item: dict[str, Any], lh: Any, seen_tables: dict[str, Any], new_tables: dict[str, Any],
                   cfg: Mapping[str, Any], value_profiling: bool) -> Iterator[Batch]:
        storage = app.service("storage")
        tables = storage.tables
        lh_ref = lakehouse_ref(item["id"], item["name"], ws["id"])
        try:
            found = tables.discover(lh)
        except Exception as exc:  # noqa: BLE001
            ctx.record_gap("not_inspected", item["name"], f"the tables of lakehouse '{item['name']}' could not be listed: {safe_message(exc)}", "check the lakehouse's storage connection",
                           location={"workspace_id": ws["id"], "item": item["id"]})
            return
        cap = int(cfg.get("max_tables_per_lakehouse") or DEFAULT_MAX_TABLES)
        if len(found) > cap:
            ctx.record_gap("not_inspected", item["name"], f"lakehouse '{item['name']}' has {len(found)} tables; only the first {cap} are scanned", "raise max_tables_per_lakehouse on the source",
                           location={"workspace_id": ws["id"], "item": item["id"]})
            found = found[:cap]
        catalog_rows = _catalog_rows(app, item["id"])
        policy = None
        if value_profiling:
            sampling = getattr(app.maybe_service("intelligence"), "discovery", None)
            policy = getattr(getattr(sampling, "sampling", None), "access", None)     # the shared data-read policy service (datapolicy.py)
        for ref in found:
            ctx.check_cancelled()
            key = f"{item['id']}:{ref['name']}"
            batch = self._table(ctx, app, ws, item, lh, lh_ref, ref, catalog_rows.get(str(ref["name"]).lower()), seen_tables, new_tables, key, cfg, policy)
            if batch is not None:
                ctx.count("tables")
                yield batch
        yield from self._files(ctx, storage, ws, item, lh, lh_ref, cfg)

    def _table(self, ctx: ScanContext, app: Any, ws: dict[str, Any], item: dict[str, Any], lh: Any, lh_ref: AssetRef, ref: dict[str, Any], entry: dict[str, Any] | None,
               seen: dict[str, Any], new: dict[str, Any], key: str, cfg: Mapping[str, Any], policies: Any) -> Facts | None:
        tables = app.service("storage").tables
        name = str(ref["name"])
        loc = {"workspace_id": ws["id"], "item": item["id"], "table": name}
        info: dict[str, Any] = {}
        dt = None
        profile: dict[str, Any] | None = None
        try:
            if ref["format"] == "delta":
                dt = tables.open_delta(lh, name)
                version = int(dt.version())
                if not ctx.is_full and seen.get(key) == version:
                    new[key] = version
                    ctx.count("tables_unchanged")
                    return None
            info = tables.info(lh, name)
            new[key] = info.get("version")
        except Exception as exc:  # noqa: BLE001 - an unreadable table is still a known asset
            ctx.record_gap("not_inspected", f"{item['name']}.{name}", f"table '{name}' of lakehouse '{item['name']}' could not be read: {safe_message(exc)}",
                           "open the table in the data explorer; a Delta protocol the embedded engine cannot read needs the Spark engine", location={**loc, "informational": True})
            ctx.count("tables_unreadable")
        schema = list(info.get("schema") or ((entry or {}).get("columns") if entry else []) or [])
        parts = [str(p) for p in (info.get("partition_columns") or (entry or {}).get("partition_columns") or [])]
        cat_cols = {str(c.get("name", "")).lower(): c for c in ((entry or {}).get("columns") or []) if isinstance(c, dict)}
        columns: list[dict[str, Any]] = []
        for pos, c in enumerate(schema):
            cname = str(c.get("name"))
            col: dict[str, Any] = {"name": cname, "type": str(c.get("type")), "nullable": bool(c.get("nullable", True)), "ordinal": pos, "partition": cname in parts}
            descr = (cat_cols.get(cname.lower()) or {}).get("description")
            if descr:
                col["description"] = str(descr)[:1000]
            columns.append(col)
        if dt is not None and cfg.get("profile_tables", True):
            profile, gaps = profiling.profile_delta_log(dt, columns, subject=f"{item['name']}.{name}", source="discovery:local", location=loc)
            for g in gaps:
                g.location = {**g.location, "informational": True}          # the table is known; only a statistic is missing
                ctx.gaps.append(g)
        stats_rows = info.get("num_rows")
        stats_size = info.get("size_bytes")
        if stats_rows is None and entry is not None:
            ts = _table_stats(app, entry["id"])
            if ts:
                stats_rows = ts.get("row_count") if stats_rows is None else stats_rows
                stats_size = ts.get("size_bytes") if stats_size is None else stats_size
        attrs: dict[str, Any] = {"source_kind": "local", "lakehouse_id": item["id"], "lakehouse": item["name"], "table_name": name, "format": ref["format"], "location": redact_text(str(ref.get("location") or "")),
                                 "columns": columns, "column_count": len(columns), "partition_columns": parts, "properties": dict(info.get("properties") or {}),
                                 "protocol": _protocol(info), "history": _history_summary(info.get("history") or []), "delta_version": info.get("version"),
                                 "row_count": stats_rows, "size_bytes": stats_size, "num_files": info.get("num_files"), "last_modified": info.get("last_modified")}
        if info.get("description"):
            attrs["table_description"] = info["description"]
        if profile is not None:
            attrs["profile"] = profile
        if ref.get("schema"):
            attrs["schema"] = ref["schema"]
        if not info:
            attrs["status"] = "unreadable"
        facts = Facts()
        table_ref = AssetRef.make("table", table_aliases(item["id"], name, ref.get("location"), (entry or {}).get("id")), name, {"workspace_id": ws["id"], "lakehouse_id": item["id"]})
        descr_text = (entry or {}).get("description") or info.get("description") or None
        tags = list((entry or {}).get("tags") or [])
        facts.assets.append(AssetFact(table_ref, name, attrs, parent=lh_ref, description=descr_text, workspace_id=ws["id"], path=f"{ws['name']} / {item['name']} / {name}", tags=tags))
        for col in columns:
            cattrs = {"data_type": col["type"], "nullable": col["nullable"], "ordinal": col["ordinal"], "partition": col["partition"], "source_kind": "local"}
            if col.get("description"):
                cattrs["column_description"] = col["description"]
            cref = AssetRef.make("column", {"lakehouse_column": f"{item['id']}:{name}:{col['name']}"}, col["name"], {"workspace_id": ws["id"], "table": f"lakehouse_table:{item['id']}:{name}"})
            facts.assets.append(AssetFact(cref, col["name"], cattrs, parent=table_ref, description=col.get("description"), workspace_id=ws["id"],
                                          path=f"{ws['name']} / {item['name']} / {name} / {col['name']}"))
        detect.annotate_facts(facts)
        if policies is not None and dt is not None:
            self._value_profile(ctx, app, policies, facts, ws, item, lh, name, columns, table_ref)
        return facts

    def _value_profile(self, ctx: ScanContext, app: Any, access: Any, facts: Facts, ws: dict[str, Any], item: dict[str, Any], lh: Any, name: str, columns: list[dict[str, Any]],
                       table_ref: AssetRef) -> None:
        """Opt-in value profiling of one table. It asks the *same* data-read policy service as the interactive sample (:class:`~olcf.intelligence.discovery.datapolicy.DataReadPolicy`):
        the policy is resolved for the table asset that already exists under the table's authoritative aliases (so a table-level ban holds for a rescanned table), approved governance
        classifications are excluded and a failing classification lookup fails closed. When the decision is "no", **no storage read happens at all**. Only aggregates and suggestions are
        stored, never values; the authorised read is audited (column names and row count, never a value)."""
        loc = {"workspace_id": ws["id"], "item": item["id"], "table": name}
        subject = f"{item['name']}.{name}"
        try:
            existing = access.resolve_assets(table_ref.alias_map)
            decision = access.decide(workspace_id=ws["id"], source_id=ctx.source_id, table_asset_ids=existing)
        except Exception as exc:  # noqa: BLE001 - the policy could not be determined: no read
            ctx.gaps.append(Gap("not_inspected", "discovery:local", subject, f"value profiling was skipped because the sampling policy could not be determined ({type(exc).__name__})",
                                "retry the scan; value profiling never reads data while the policy is unknown", loc))
            return
        eff = decision.policy
        if not decision.allowed:
            if decision.code == "classification_unavailable":
                ctx.gaps.append(Gap("not_inspected", "discovery:local", subject, "value profiling was skipped because the classification lookup failed (failing closed)",
                                    "retry the scan; value profiling never reads data while the excluded columns are unknown", loc))
                return
            ctx.gaps.append(Gap("sampling_disabled", "discovery:local", subject, f"value profiling is enabled on the source but no sampling policy allows rows ({eff.reason})",
                                "Create a sampling policy with max_rows > 0 (PUT /v1/intelligence/sampling-policies) or turn value_profiling off.", loc))
            return
        try:
            skip = decision.excluded_in(columns)
            wanted = [c["name"] for c in columns if c["name"].lower() not in skip]
            if not wanted:
                return
            arrow = app.service("storage").tables.scan_source(lh, name).scanner(columns=wanted).head(eff.max_rows)
            arrow, _dropped = access.project(arrow, decision, declared=columns, requested=wanted)          # exactly the permitted columns, whatever the reader returned
            prof = profiling.profile_values(arrow, columns)
            read_columns = list(arrow.column_names)
            read_rows = int(arrow.num_rows)
        except Exception as exc:  # noqa: BLE001
            ctx.gaps.append(Gap("not_inspected", "discovery:local", subject, f"value profiling failed: {safe_message(exc)}", "check that the table can be read", loc))
            return
        audit = getattr(getattr(app, "audit", None), "record", None)
        if audit is not None:
            try:
                audit(ctx.principal, "intelligence.sample_read", resource_type="table", resource_id=(decision.table_asset_ids or (None,))[0], workspace_id=ws["id"],
                      details={"purpose": "value_profiling", "rows": read_rows, "columns": read_columns, "excluded_columns": len(skip), "masking": "aggregate_only",
                               "policy_scope": eff.scope, "asset_name": name, "source_id": ctx.source_id})
            except Exception:  # noqa: BLE001, S110 - an audit hiccup must not corrupt the scan; the read has happened and nothing was stored
                pass
        table = facts.assets[0]
        merged = profiling.merge_value_profile(table.attrs.get("columns") or [], prof)
        table.attrs["columns"] = merged
        for a in facts.assets[1:]:
            p = prof.get(a.name)
            if p and p["suggested_classifications"]:
                a.attrs["suggested_classifications"] = p["suggested_classifications"]
        table.attrs.setdefault("profile", {})["value_profile"] = {"rows": eff.max_rows, "policy_scope": eff.scope}

    # ---- listed files
    def _files(self, ctx: ScanContext, storage: Any, ws: dict[str, Any], item: dict[str, Any], lh: Any, lh_ref: AssetRef, cfg: Mapping[str, Any]) -> Iterator[Batch]:
        policies = [f for f in (cfg.get("files") or []) if f["lakehouse"] in (item["id"], item["name"], f"{ws['name']}/{item['name']}")]
        cap = int(cfg.get("max_files_per_path") or DEFAULT_MAX_FILES)
        for pol in policies:
            ctx.check_cancelled()
            try:
                entries = storage.list_files(lh, pol["path"], recursive=bool(pol["recursive"]))
            except Exception as exc:  # noqa: BLE001
                ctx.record_gap("not_inspected", f"{item['name']}/{pol['path']}", f"the listed path '{pol['path']}' could not be read: {safe_message(exc)}", "check the path in the file explorer",
                               location={"workspace_id": ws["id"], "item": item["id"], "path": pol["path"]})
                continue
            facts = Facts()
            files = [e for e in entries if not e.is_dir]
            if len(files) > cap:
                ctx.record_gap("not_inspected", f"{item['name']}/{pol['path']}", f"{len(files)} files below '{pol['path']}'; only the first {cap} are reported", "raise max_files_per_path or list a narrower path",
                               location={"workspace_id": ws["id"], "item": item["id"], "path": pol["path"]})
                files = files[:cap]
            for e in files:
                uri = redact_text(storage.physical_uri(lh, e.path))
                aliases = {"source": f"{ctx.source_id}:{item['id']}:{e.path}"}
                if "://" in uri:
                    aliases["onelake_path"] = normalize_path(uri)
                attrs = {"source_kind": "local", "lakehouse_id": item["id"], "path": e.path, "size_bytes": e.size, "last_modified": getattr(e, "modified", None) or getattr(e, "last_modified", None),
                         "location": uri}
                facts.assets.append(AssetFact(AssetRef.make("file", aliases, e.path, {"workspace_id": ws["id"], "lakehouse_id": item["id"]}), e.path, attrs, parent=lh_ref, workspace_id=ws["id"],
                                              path=f"{ws['name']} / {item['name']} / {e.path}"))
            ctx.count("files", len(files))
            if facts.assets:
                yield facts


# --------------------------------------------------------------------------------------------- helpers
def _dict(value: Any) -> dict[str, Any]:
    """``value`` when it is a dict, else an empty dict (definitions are user data; every key may be missing or of the wrong type)."""
    return value if isinstance(value, dict) else {}


def _selection(ctx: ScanContext) -> list[str] | None:
    """Workspace ids / names to scan: the source's ``workspace_ids`` intersected with the request-level filter; ``None`` = all."""
    cfg_ws = list(ctx.config.get("workspace_ids") or [])
    flt = set(ctx.workspace_filter) if ctx.workspace_filter is not None else None
    if flt is None:
        return cfg_ws or None
    if not cfg_ws:
        return sorted(flt)
    return [w for w in cfg_ws if w in flt]


def _workspaces(app: Any, wanted: list[str] | None) -> Iterator[dict[str, Any]]:
    """All non-deleted workspaces (or the wanted ones by id or name), read as the system principal."""
    svc = app.service("workspaces")
    if wanted is not None:
        seen: set[str] = set()
        for ref in wanted:
            try:
                row = svc.resolve(ref)
            except OlcfError:
                continue
            if row["id"] not in seen:
                seen.add(row["id"])
                yield row
        return
    cursor = None
    while True:
        rows, cursor = svc.list(SYSTEM_PRINCIPAL, limit=_PAGE, cursor=cursor)
        yield from rows
        if not cursor:
            return


def _all_items(items_svc: Any, workspace_id: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    cursor = None
    while True:
        rows, cursor = items_svc.list(workspace_id, limit=_PAGE, cursor=cursor)
        out.extend(rows)
        if not cursor:
            return out


def _lakehouses(app: Any, workspace_id: str) -> list[Any]:
    return list(app.service("storage").list_lakehouses(workspace_id))


def _catalog_rows(app: Any, lakehouse_id: str) -> dict[str, dict[str, Any]]:
    """Catalog entries of a lakehouse by lower-cased table name (empty when the catalog module is not installed)."""
    try:
        from olcf.catalog.schema import catalog_entries
    except ImportError:
        return {}
    try:
        with app.db.connect() as c:
            rows = c.execute(select(catalog_entries).where(catalog_entries.c.lakehouse_id == lakehouse_id)).mappings().all()
    except Exception:  # noqa: BLE001 - the catalog tables may not exist in a minimal deployment
        return {}
    return {str(r["name"]).lower(): dict(r) for r in rows}


def _table_stats(app: Any, entry_id: str) -> dict[str, Any] | None:
    try:
        from olcf.catalog.schema import table_stats

        with app.db.connect() as c:
            row = c.execute(select(table_stats).where(table_stats.c.entry_id == entry_id)).mappings().first()
        return dict(row) if row else None
    except Exception:  # noqa: BLE001
        return None


def _protocol(info: Mapping[str, Any]) -> dict[str, Any] | None:
    p = info.get("protocol")
    if not isinstance(p, dict):
        return None
    return {k: p.get(k) for k in ("min_reader_version", "min_writer_version", "reader_features", "writer_features") if k in p} or dict(p)


def _history_summary(history: list[dict[str, Any]]) -> dict[str, Any]:
    """A compact summary of the newest commits: last operation and time, counts per operation (no operation parameters, which may contain predicates)."""
    if not history:
        return {}
    ops: dict[str, int] = {}
    for h in history:
        op = str(h.get("operation") or "UNKNOWN")
        ops[op] = ops.get(op, 0) + 1
    return {"last_operation": history[0].get("operation"), "last_commit_at": history[0].get("timestamp"), "commits_summarised": len(history), "operations": ops}
