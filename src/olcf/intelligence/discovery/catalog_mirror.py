# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The ``catalog_mirror`` connector: the classic catalog's tables and recorded lineage, mirrored into the Intelligence graph. **Read-only** - the catalog module is never written.

``catalog_entries`` become ``table`` assets (aliases ``catalog_entry`` + ``lakehouse_table``, the catalog's description / tags / columns) and every row of ``lineage_edges`` becomes a
process asset with ``READS_FROM`` the source dataset and ``WRITES_TO`` the target dataset, plus column ``TRANSFORMS`` edges for the column mappings the catalog could prove.
All edges are ``derived`` with evidence ``kind=metadata`` and ``source=catalog-mirror`` (the catalog stores what some component *recorded*, not what discovery
verified); when the run-time translator observes the same flow the edge is upgraded to ``verified`` by the graph, not here.

The process of a catalog edge is the OLCF item named by ``via_ref`` when that is an item id (notebook, pipeline, saved query, report ...), otherwise a ``script`` asset keyed by the
catalog edge's own (via kind, ref / name) so repeated recordings of one SQL statement or job land on one process.

The ``local`` source mirrors the same lineage rows by default (``mirror_catalog_lineage``, :func:`lineage_batches`); a standalone ``catalog_mirror`` source additionally mirrors the
catalog's table entries and remains available for installations that want the catalog separate from the local scan.

Incremental scans only look at rows whose ``last_seen_at`` / ``updated_at`` is at or after the previous scan; full scans read everything.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Iterator, Mapping
from typing import Any

from sqlalchemy import select

from olcf.intelligence.discovery.base import (
    Batch,
    ResolvedSecrets,
    ScanContext,
    Step,
    StepFailure,
    StepSpec,
    run_steps,
    safe_message,
)
from olcf.intelligence.discovery.local import DEFINITION_ITEMS, lakehouse_ref, table_aliases
from olcf.intelligence.model import AssetFact, AssetRef, EdgeFact, Evidence, Facts

log = logging.getLogger("olcf.intelligence.discovery.catalog_mirror")

KIND = "catalog_mirror"
SOURCE_NAME = "catalog-mirror"
EDGE_CHUNK = 200
CONFIG_KEYS = frozenset({"workspace_ids"})
_VIA_TYPES = {"notebook": "notebook", "pipeline": "pipeline", "report": "report", "query": "sql_query", "sql": "sql_query", "job": "script", "api": "script", "manual": "script"}


def dataset_ref(ds: Mapping[str, Any], workspace_id: str, source_id: str, storage: Any = None) -> AssetRef | None:
    """The asset reference of a catalog dataset descriptor (``kind`` table | file | report | query | notebook | item | external), or ``None`` when it cannot be named."""
    kind = ds.get("kind")
    if kind == "table" and ds.get("lakehouse_id") and ds.get("name"):
        return AssetRef.make("table", {"lakehouse_table": f"{ds['lakehouse_id']}:{ds['name']}"}, str(ds["name"]), {"workspace_id": workspace_id, "lakehouse_id": str(ds["lakehouse_id"])})
    if kind == "file" and ds.get("lakehouse_id") and ds.get("path"):
        aliases = {"source": f"{source_id}:{ds['lakehouse_id']}:{str(ds['path']).strip('/')}"}
        try:
            if storage is not None:
                from olcf.core.redaction import redact_text
                from olcf.intelligence.model import normalize_path

                uri = redact_text(storage.physical_uri(storage.get_lakehouse(str(ds["lakehouse_id"])), str(ds["path"])))
                if "://" in uri:
                    aliases["onelake_path"] = normalize_path(uri)
        except Exception:  # noqa: BLE001 - the alias is an optimisation; the source alias still identifies the file
            pass
        return AssetRef.make("file", aliases, str(ds["path"]), {"workspace_id": workspace_id, "lakehouse_id": str(ds["lakehouse_id"])})
    if kind in ("report", "query", "notebook", "item") and ds.get("item_id"):
        type_ = {"report": "report", "query": "sql_query", "notebook": "notebook"}.get(str(kind), "external")
        return AssetRef.make(type_, {"olcf_item": str(ds["item_id"])}, ds.get("name"), {"workspace_id": workspace_id})
    if kind == "external" and (ds.get("uri") or ds.get("name")):
        label = str(ds.get("uri") or ds.get("name"))
        return AssetRef.make("external", {"source": f"{source_id}:ext:{label}"}, label, None)
    return None


def lineage_batches(ctx: ScanContext, app: Any, workspace_ids: set[str] | frozenset[str] = frozenset()) -> Iterator[Facts]:
    """The recorded catalog lineage (``lineage_edges``) as batches of facts: process assets with ``READS_FROM`` / ``WRITES_TO`` and column ``TRANSFORMS`` edges.

    Shared by the ``catalog_mirror`` connector and the ``local`` connector (``mirror_catalog_lineage``, default on), so the default installation needs no second source. An empty
    ``workspace_ids`` means every workspace; an incremental scan only reads rows seen since the previous scan. Nothing is written to the catalog.
    """
    from olcf.catalog.schema import lineage_edges

    since = None if ctx.is_full else ctx.since
    with app.db.connect() as c:
        eq = select(lineage_edges).order_by(lineage_edges.c.first_seen_at)
        if workspace_ids:
            eq = eq.where(lineage_edges.c.workspace_id.in_(sorted(workspace_ids)))
        if since:
            eq = eq.where(lineage_edges.c.last_seen_at >= since)
        edge_rows = [dict(r) for r in c.execute(eq).mappings().all()]
    storage = app.maybe_service("storage")
    mirror = CatalogMirrorConnector()
    items_cache: dict[str, str | None] = {}
    for start in range(0, len(edge_rows), EDGE_CHUNK):
        ctx.check_cancelled()
        facts = Facts()
        for row in edge_rows[start:start + EDGE_CHUNK]:
            try:
                mirror._edge(ctx, app, storage, row, facts, items_cache)  # noqa: SLF001 - same module
            except Exception as exc:  # noqa: BLE001 - one bad catalog row is a gap
                ctx.record_gap("parse_error", row["id"], f"catalog lineage edge {row['id']} could not be mirrored: {safe_message(exc)}", "inspect the row in lineage_edges",
                               location={"workspace_id": row["workspace_id"], "catalog_edge": row["id"]})
        ctx.count("catalog_edges", len(edge_rows[start:start + EDGE_CHUNK]))
        yield facts


class CatalogMirrorConnector:
    """Mirror of the classic catalog (see the module docstring)."""

    kind = KIND
    label = "The classic catalog (tables, descriptions and recorded lineage) mirrored into the graph; read-only"

    def validate_config(self, config: Mapping[str, Any]) -> dict[str, Any]:
        """Only ``workspace_ids`` (list of workspace ids to mirror; default all) is accepted."""
        from olcf.core.errors import ValidationFailed

        extra = sorted(set(config) - CONFIG_KEYS)
        if extra:
            raise ValidationFailed(f"unknown setting(s) {extra} for a catalog mirror source; allowed: {sorted(CONFIG_KEYS)}")
        ws = config.get("workspace_ids")
        if ws is None:
            return {}
        if not isinstance(ws, list) or not all(isinstance(x, str) and x for x in ws):
            raise ValidationFailed("workspace_ids must be a list of workspace ids")
        return {"workspace_ids": sorted(set(ws))}

    def test(self, config: Mapping[str, Any], secrets: ResolvedSecrets, *, app: Any = None) -> list[Step]:
        """Steps: 1 the catalog module is installed, 2 its tables are readable (entry and edge counts)."""
        counts: dict[str, int] = {}

        def installed() -> str:
            if app is None or app.maybe_service("catalog") is None:
                raise StepFailure("the catalog module is not installed", "start the control plane with the catalog module, or use the local source instead")
            return "catalog service available"

        def readable() -> str:
            from sqlalchemy import func

            from olcf.catalog.schema import catalog_entries, lineage_edges

            with app.db.connect() as c:
                counts["entries"] = int(c.execute(select(func.count()).select_from(catalog_entries)).scalar() or 0)
                counts["edges"] = int(c.execute(select(func.count()).select_from(lineage_edges)).scalar() or 0)
            return f"{counts['entries']} catalog table(s) and {counts['edges']} recorded lineage edge(s)"

        return run_steps([StepSpec("catalog module", installed), StepSpec("catalog tables", readable)])

    def discover(self, ctx: ScanContext) -> Iterator[Batch]:
        """Yield one batch of table assets per workspace, then the lineage edges in chunks. Reads ``catalog_entries`` / ``lineage_edges`` only."""
        app = ctx.app
        if app is None or app.maybe_service("catalog") is None:
            ctx.record_gap("api_unavailable", "catalog", "the catalog module is not installed", "start the control plane with the catalog module")
            return
        from olcf.catalog.schema import catalog_entries

        wanted = set(ctx.config.get("workspace_ids") or [])
        if ctx.workspace_filter is not None:
            wanted = set(ctx.workspace_filter) if not wanted else wanted & set(ctx.workspace_filter)
        since = None if ctx.is_full else ctx.since
        with app.db.connect() as c:
            q = select(catalog_entries).order_by(catalog_entries.c.workspace_id, catalog_entries.c.name)
            if wanted:
                q = q.where(catalog_entries.c.workspace_id.in_(sorted(wanted)))
            if since:
                q = q.where(catalog_entries.c.updated_at >= since)
            entries = [dict(r) for r in c.execute(q).mappings().all()]
        names = _lakehouse_names(app, {e["lakehouse_id"] for e in entries})
        by_ws: dict[str, list[dict[str, Any]]] = {}
        for e in entries:
            by_ws.setdefault(e["workspace_id"], []).append(e)
        for ws_id, rows in by_ws.items():
            ctx.check_cancelled()
            facts = Facts()
            for e in rows:
                lh_name = names.get(e["lakehouse_id"], e["lakehouse_id"])
                lh_ref = lakehouse_ref(e["lakehouse_id"], lh_name, ws_id)
                cols = [{"name": str(col.get("name")), "type": str(col.get("type")), "nullable": bool(col.get("nullable", True)), "ordinal": i,
                         "partition": str(col.get("name")) in (e["partition_columns"] or []), **({"description": col["description"]} if col.get("description") else {})}
                        for i, col in enumerate(e["columns"] or []) if isinstance(col, dict)]
                attrs = {"source_kind": "catalog", "lakehouse_id": e["lakehouse_id"], "lakehouse": lh_name, "table_name": e["name"], "format": e["format"], "location": e["location"],
                         "columns": cols, "column_count": len(cols), "partition_columns": list(e["partition_columns"] or []), "catalog_status": e["status"], "delta_version": e["version"]}
                facts.assets.append(AssetFact(AssetRef.make("table", table_aliases(e["lakehouse_id"], e["name"], e["location"], e["id"]), e["name"], {"workspace_id": ws_id, "lakehouse_id": e["lakehouse_id"]}),
                                              e["name"], attrs, parent=lh_ref, description=e["description"] or None, workspace_id=ws_id, tags=list(e["tags"] or []),
                                              path=f"{lh_name} / {e['name']}"))
            ctx.count("catalog_tables", len(rows))
            yield facts
        yield from lineage_batches(ctx, app, wanted)

    def _edge(self, ctx: ScanContext, app: Any, storage: Any, row: dict[str, Any], facts: Facts, items_cache: dict[str, str | None]) -> None:
        ws = row["workspace_id"]
        src = dataset_ref(row["source"] or {}, ws, ctx.source_id, storage)
        dst = dataset_ref(row["target"] or {}, ws, ctx.source_id, storage)
        loc = {"workspace_id": ws, "catalog_edge": row["id"]}
        if src is None or dst is None:
            ctx.record_gap("unsupported_construct", row["id"], f"catalog lineage edge {row['id']} names a dataset the mirror cannot identify", "re-record the lineage with a complete dataset descriptor", location=loc)
            return
        for ds, ref in ((row["source"] or {}, src), (row["target"] or {}, dst)):
            fact = _dataset_fact(ds, ref, ws)
            if fact is not None:
                facts.assets.append(fact)
        process = self._process(app, row, ws, ctx.source_id, items_cache)
        facts.assets.append(AssetFact(process, process.name or row["via_kind"], {"source_kind": "catalog", "via_kind": row["via_kind"], "catalog_run_count": row["run_count"]},
                                      workspace_id=ws, path=None))
        ev = Evidence("metadata", SOURCE_NAME, "derived", ref=row["id"], location=loc, observed_at=row["last_seen_at"])
        facts.edges.append(EdgeFact(process, src, "READS_FROM", [ev], "derived"))
        facts.edges.append(EdgeFact(process, dst, "WRITES_TO", [Evidence("metadata", SOURCE_NAME, "derived", ref=row["id"], location=loc, observed_at=row["last_seen_at"])], "derived"))
        if row["column_status"] in ("complete", "partial"):
            for m in row["column_mappings"] or []:
                target = str(m.get("target") or "")
                for s in m.get("sources") or []:
                    if not target or not s.get("column"):
                        continue
                    facts.edges.append(EdgeFact(_column(src, str(s["column"])), _column(dst, target), "TRANSFORMS",
                                                [Evidence("metadata", SOURCE_NAME, "derived", ref=row["id"], location={**loc, "target": target})], "derived",
                                                transformation=str(m.get("transform") or "") or None, via=process))

    def _process(self, app: Any, row: dict[str, Any], ws: str, source_id: str, cache: dict[str, str | None]) -> AssetRef:
        via_ref, via_name, via_kind = row.get("via_ref"), row.get("via_name"), row["via_kind"]
        if via_ref and str(via_ref).startswith("itm_"):
            if via_ref not in cache:
                cache[via_ref] = None
                try:
                    cache[via_ref] = app.service("items").get(via_ref)["type"]
                except Exception:  # noqa: BLE001 - a deleted item falls back to a script
                    cache[via_ref] = None
            itype = cache[via_ref]
            if itype in DEFINITION_ITEMS:
                return AssetRef.make(DEFINITION_ITEMS[itype][0], {"olcf_item": str(via_ref)}, via_name, {"workspace_id": ws})
        ident = hashlib.sha256(f"{via_kind}|{via_ref or ''}|{via_name or ''}".encode()).hexdigest()[:20]
        return AssetRef.make("script", {"source": f"{source_id}:catalog:{ident}"}, via_name or f"{via_kind} {via_ref or ''}".strip(), {"workspace_id": ws})


def _dataset_fact(ds: Mapping[str, Any], ref: AssetRef, workspace_id: str) -> AssetFact | None:
    """An asset for a lineage endpoint that no other connector reports: an ``external`` dataset or a ``file`` (tables are reported by the local scan / catalog entries; a table
    nobody reports stays a placeholder and an ``unresolved_reference`` gap, which is the honest state)."""
    from olcf.core.redaction import redact_text

    if ref.type == "external":
        label = redact_text(str(ds.get("uri") or ds.get("name")))
        return AssetFact(ref, label, {"source_kind": "catalog", "dataset_kind": "external"}, workspace_id=workspace_id)
    if ref.type == "file":
        return AssetFact(ref, str(ds["path"]), {"source_kind": "catalog", "dataset_kind": "file", "path": str(ds["path"]), "lakehouse_id": str(ds["lakehouse_id"])},
                         parent=lakehouse_ref(str(ds["lakehouse_id"]), None, workspace_id), workspace_id=workspace_id)
    return None


def _column(table: AssetRef, name: str) -> AssetRef:
    """Reference to a column of the table reference ``table`` (the registry finds the table through ``scope['table']``)."""
    scope = dict(table.scope_map)
    kind, value = table.aliases[0] if table.aliases else ("", "")
    if kind == "lakehouse_table":
        scope["table"] = f"lakehouse_table:{value}"
    elif table.name:
        scope["table"] = table.name
    scope["table_type"] = table.type if table.type in ("table", "view") else "table"
    return AssetRef.make("column", None, name, scope)


def _lakehouse_names(app: Any, ids: set[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    items = app.maybe_service("items")
    for i in ids:
        try:
            out[i] = items.get(i)["name"] if items is not None else i
        except Exception:  # noqa: BLE001
            out[i] = i
    return out
