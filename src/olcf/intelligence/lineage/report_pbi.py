# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Lineage of a Power BI / Fabric report: which semantic model it is bound to, and which model columns and measures each visual shows.

:func:`analyze_report_pbi` ``(parts, *, context)`` reads a report definition given as ``{path: text}`` parts, in either format:

* **PBIR** (folder format): ``definition.pbir``, ``definition/report.json``, ``definition/pages/pages.json``, ``definition/pages/<page>/page.json``,
  ``definition/pages/<page>/visuals/<visual>/visual.json`` (and ``definition/reportExtensions.json`` for report-level measures);
* **legacy** ``report.json`` (``sections`` / ``visualContainers``, whose ``config`` / ``filters`` are JSON strings) together with ``definition.pbir``.

Emitted: a ``report`` asset (the context's process asset), ``page`` and ``visual`` assets (parent = report / page, ``pbi`` alias ``<workspace>/<report>/<page>[/<visual>]`` when the
GUIDs are known), ``DEPENDS_ON`` report -> semantic model from ``datasetReference`` (``byPath``: the model item next to the report, ``byConnection``: the connection string's
semantic model id / initial catalog), and for every field a visual uses ``DISPLAYS`` visual -> column / measure (fields in the visual's query), or ``DEPENDS_ON`` visual -> column /
measure with ``attrs.role`` ``filter`` / ``sort`` / ``format`` for fields that only filter, sort or format. Page and report level filters give ``DEPENDS_ON`` page / report -> field.
Column and measure references use the same ``pbi`` alias the semantic-model analyzer gives them, so the edges land on the model's assets once the model is known; when the model's
GUID is unknown they carry the model name and table in their scope (``semantic_model`` / ``table``) and an informational gap says so.

Confidence is ``derived`` (the binding is declared in the definition). Not interpreted: report-level measures (reported in a gap, not linked), hierarchy levels (gap), field
parameters, bookmarks, drill-through targets, report themes, custom visuals' data roles beyond what is in the standard query, and a report with a missing or unreadable
``definition.pbir`` (visuals are listed, no field edges are emitted, a gap says why).
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import unquote

from olcf.intelligence.lineage.base import (
    MAX_DEPTH,
    MAX_JSON_BYTES,
    MAX_PARTS,
    REPORT_PBI,
    FactBuilder,
    LineageContext,
    facts_with_gap,
    pbi_alias,
)
from olcf.intelligence.model import AssetFact, AssetRef, Facts, clip, looks_like_guid

_PAGE = re.compile(r"(?:^|/)definition/pages/([^/]+)/page\.json$", re.IGNORECASE)
_VISUAL = re.compile(r"(?:^|/)definition/pages/([^/]+)/visuals/([^/]+)/visual\.json$", re.IGNORECASE)
_GUID_IN = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
_MODEL_SUFFIX = re.compile(r"\.(semanticmodel|dataset)$", re.IGNORECASE)
_FIELD_KINDS = ("Column", "Measure")


def _json(text: Any) -> Any:
    """Parse JSON text (or pass a parsed object through); ``ValueError`` for anything else."""
    if isinstance(text, (dict, list)):
        return text
    if isinstance(text, str):
        return json.loads(text)
    raise ValueError("the part is not JSON text")


def _obj(d: Mapping[str, Any], key: str) -> dict[str, Any]:
    """``d[key]`` when it is a JSON object, else ``{}``."""
    v = d.get(key)
    return v if isinstance(v, dict) else {}


def _maybe_json(value: Any) -> Any:
    """Legacy reports hold JSON documents as strings: parse them, return ``None`` when that fails."""
    try:
        return _json(value) if value else None
    except ValueError:
        return None


class _Target:
    """The semantic model a report is bound to: how model objects are referenced from the report analyzer."""

    def __init__(self, ctx: LineageContext, name: str | None, ws: str | None, ds: str | None, ref: AssetRef | None) -> None:
        self.ctx = ctx
        self.name = name
        self.ws = ws
        self.ds = ds
        self.ref = ref

    def field(self, kind: str, table: str, name: str) -> AssetRef:
        """Reference to column / measure ``name`` of model table ``table``."""
        if self.ws and self.ds:
            return AssetRef.make(kind, {"pbi": pbi_alias(self.ws, self.ds, table, name)}, name, {"semantic_model": self.name or self.ds, "table": table.lower()})
        return AssetRef.make(kind, None, name, {"semantic_model": self.name or "", "table": table.lower()} if self.name else {"table": table.lower()})


class _ReportAnalyzer:
    def __init__(self, parts: Mapping[str, Any], ctx: LineageContext) -> None:
        self.parts = {str(k).replace("\\", "/"): v for k, v in parts.items()}
        self.ctx = ctx
        self.b = FactBuilder(REPORT_PBI, ctx)
        ws = ctx.fabric_workspace if ctx.fabric_workspace and looks_like_guid(ctx.fabric_workspace) else None
        item = ctx.item_id if ctx.item_id and looks_like_guid(ctx.item_id) else None
        self.ids = (ws, item) if ws and item else None
        self.key = ctx.item_name or "report"
        self.report_ref = self._report_ref()
        self.target: _Target | None = None
        self.extensions: set[tuple[str, str]] = set()
        self.hierarchy_seen = False
        self.page_n = 0

    # -- references
    def _report_ref(self) -> AssetRef:
        proc = self.ctx.process
        aliases = dict(proc.alias_map) if proc is not None else {}
        if self.ids is not None:
            aliases["pbi"] = pbi_alias(*self.ids)
        if proc is not None:
            return AssetRef.make(proc.type, aliases, proc.name, proc.scope_map)
        return AssetRef.make("report", aliases or None, self.key, self.ctx.scope())

    def _page_ref(self, page: str) -> AssetRef:
        if self.ids is not None:
            return AssetRef.make("page", {"pbi": pbi_alias(*self.ids, page)}, page, {"report": self.key})
        return AssetRef.make("page", None, page, {"report": self.key})

    def _visual_ref(self, page: str, visual: str) -> AssetRef:
        if self.ids is not None:
            return AssetRef.make("visual", {"pbi": pbi_alias(*self.ids, page, visual)}, visual, {"report": self.key, "page": page})
        return AssetRef.make("visual", None, visual, {"report": self.key, "page": page})

    def loc(self, part: str | None = None, **kw: Any) -> dict[str, Any]:
        """Evidence location: the part path plus ``kw`` (``page``, ``visual``)."""
        return self.ctx.loc(part=part, **kw)

    # -- driver
    def run(self) -> Facts:
        """Analyse all parts and return the facts."""
        name = self.ctx.item_name or "report"
        self.b.asset(AssetFact(ref=self.report_ref, name=name, attrs={"definition_format": "pbir" if self._is_pbir() else "legacy"}, workspace_id=self.ctx.workspace_id))
        self._binding()
        self._extensions()
        if self._is_pbir():
            self._pbir()
        else:
            self._legacy()
        if self.hierarchy_seen and self.target is not None:
            self.b.gap("unsupported_construct", name, "hierarchy fields used by visuals are not linked to model columns", "Informational: the other fields of those visuals are linked.",
                       self.loc(informational=True))
        return self.b.facts()

    def _is_pbir(self) -> bool:
        return any(_PAGE.search(p) or _VISUAL.search(p) for p in self.parts)

    # -- dataset binding
    def _binding(self) -> None:
        part = next((p for p in sorted(self.parts) if p.lower().endswith("definition.pbir")), None)
        name = self.ctx.item_name or "report"
        if part is None:
            self.b.gap("not_inspected", name, "the report has no definition.pbir, so the semantic model it uses is unknown and its fields were not linked",
                       "Export the report definition with all parts.", self.loc())
            return
        try:
            doc = _json(self.parts[part])
        except ValueError as exc:
            self.b.gap("parse_error", name, f"definition.pbir cannot be read: {exc}", None, self.loc(part))
            return
        ref = doc.get("datasetReference") if isinstance(doc, dict) else None
        if not isinstance(ref, dict):
            self.b.gap("not_inspected", name, "definition.pbir has no datasetReference, so the semantic model is unknown and fields were not linked", None, self.loc(part))
            return
        by_path = ref.get("byPath")
        by_conn = ref.get("byConnection")
        ws_guid = self.ids[0] if self.ids else None
        if isinstance(by_path, dict) and isinstance(by_path.get("path"), str):
            path = by_path["path"].replace("\\", "/")
            model_name = _MODEL_SUFFIX.sub("", path.rstrip("/").rsplit("/", 1)[-1])
            guid = self.ctx.semantic_models.get(model_name.lower())
            self._set_target(model_name, ws_guid, guid, "path", path, part)
        elif isinstance(by_conn, dict):
            self._by_connection(by_conn, part)
        else:
            self.b.gap("not_inspected", name, "datasetReference is neither byPath nor byConnection", None, self.loc(part))

    def _by_connection(self, conn: dict[str, Any], part: str) -> None:
        cs = str(conn.get("connectionString") or "")
        ws_name_m = re.search(r"powerbi://[^/]+/v1\.0/[^/]+/([^;\"]+)", cs, re.IGNORECASE)
        ws_name = unquote(ws_name_m.group(1)).strip() if ws_name_m else None
        catalog_m = re.search(r"initial catalog\s*=\s*([^;]+)", cs, re.IGNORECASE)
        catalog = catalog_m.group(1).strip().strip('"') if catalog_m else None
        sm_id = re.search(r"semanticmodelid\s*=\s*(" + _GUID_IN.pattern + ")", cs, re.IGNORECASE)
        db = conn.get("pbiModelDatabaseName")
        guid = (sm_id.group(1) if sm_id else None) or (db if isinstance(db, str) and looks_like_guid(db) else None) or (catalog if catalog and looks_like_guid(catalog) else None)
        model_name = catalog if catalog and not looks_like_guid(catalog) else None
        same_ws = bool(ws_name and self.ctx.fabric_workspace_name and ws_name.lower() == self.ctx.fabric_workspace_name.lower())
        ws = self.ids[0] if self.ids and same_ws else None
        self._set_target(model_name, ws, guid.lower() if guid else None, "connection", ws_name or "", part, ws_name=ws_name)

    def _set_target(self, name: str | None, ws: str | None, guid: str | None, how: str, detail: str, part: str, ws_name: str | None = None) -> None:
        if name is None and guid is None:
            self.b.gap("unresolved_reference", "semantic model", "the dataset reference names neither a model nor an id; fields were not linked", None, self.loc(part))
            return
        scope: dict[str, str] = {}
        if self.ctx.fabric_workspace and (how == "path" or ws):
            scope["fabric_workspace"] = self.ctx.fabric_workspace
        elif ws_name:
            scope["fabric_workspace_name"] = ws_name
        aliases = {"pbi": pbi_alias(ws, guid)} if ws and guid else None
        ref = AssetRef.make("semantic_model", aliases, name or guid, scope)
        self.target = _Target(self.ctx, name, ws, guid, ref)
        self.b.edge(self.report_ref, ref, "DEPENDS_ON", confidence="derived", kind="definition", location=self.loc(part), snippet=f"datasetReference by {how}: {detail}"[:400])
        if not (ws and guid):
            what = f"'{name or guid}'" + (f" in workspace '{ws_name}'" if ws_name else "")
            self.b.gap("unresolved_reference", name or guid or "semantic model",
                       f"the report's semantic model {what} could not be mapped to a model id; field references carry the model name and table instead of ids",
                       "Provide the semantic model GUID (context 'semantic_models') or run discovery so the model is known.", {**self.loc(part), "informational": True})

    def _extensions(self) -> None:
        for path in sorted(self.parts):
            if path.lower().endswith("reportextensions.json"):
                doc = _maybe_json(self.parts[path])
                for ent in doc.get("entities", []) if isinstance(doc, dict) else []:
                    if isinstance(ent, dict):
                        for ms in ent.get("measures", []) if isinstance(ent.get("measures"), list) else []:
                            if isinstance(ms, dict) and isinstance(ent.get("name"), str) and isinstance(ms.get("name"), str):
                                self.extensions.add((ent["name"].lower(), ms["name"].lower()))

    # -- fields
    def _walk(self, node: Any, path: tuple[str, ...], out: list[tuple[str, dict[str, Any], tuple[str, ...]]], aliases: dict[str, str], depth: int = 0) -> None:
        if depth > MAX_DEPTH:
            return
        if isinstance(node, dict):
            for kind in _FIELD_KINDS:
                v = node.get(kind)
                if isinstance(v, dict) and isinstance(v.get("Property"), str) and isinstance(v.get("Expression"), dict):
                    out.append((kind, v, path))
                    return
            if "HierarchyLevel" in node or "Hierarchy" in node:
                self.hierarchy_seen = True
            for k, v in node.items():
                self._walk(v, (*path, str(k)), out, aliases, depth + 1)
        elif isinstance(node, list):
            for i, v in enumerate(node):
                self._walk(v, (*path, str(i)), out, aliases, depth + 1)

    @staticmethod
    def _entity(field: dict[str, Any], aliases: dict[str, str]) -> str | None:
        src = field.get("Expression", {}).get("SourceRef") if isinstance(field.get("Expression"), dict) else None
        if not isinstance(src, dict):
            return None
        if isinstance(src.get("Entity"), str):
            return str(src["Entity"])
        if isinstance(src.get("Source"), str):
            return aliases.get(src["Source"])
        return None

    @staticmethod
    def _role(path: tuple[str, ...]) -> tuple[str, bool]:
        """``(role, displayed)`` for a field found at ``path`` inside a visual / page / report document."""
        low = [p.lower() for p in path]
        if "querystate" in low:
            i = low.index("querystate")
            return (path[i + 1] if i + 1 < len(path) else "values"), True
        if "sortdefinition" in low or "orderby" in low:
            return "sort", False
        if "filterconfig" in low or "filters" in low or "where" in low:
            return "filter", False
        return "format", False

    def _link(self, owner: AssetRef, fields: list[tuple[str, dict[str, Any], tuple[str, ...]]], aliases: dict[str, str], part: str, snippet: str,
              location: dict[str, Any] | None = None) -> None:
        if self.target is None:
            return
        loc = {**self.loc(part), **(location or {})}
        for kind, field, path in fields:
            entity = self._entity(field, aliases)
            prop = field.get("Property")
            if entity is None or not isinstance(prop, str):
                self.b.gap("unresolved_reference", str(prop), "a field's table could not be determined", None, loc)
                continue
            if kind == "Measure" and (entity.lower(), prop.lower()) in self.extensions:
                self.b.gap("unsupported_construct", f"{entity}[{prop}]", "report-level measure: it is defined in the report, not in the semantic model, so it is not linked",
                           "Informational: move the measure into the model to track it.", {**loc, "informational": True})
                continue
            role, shown = self._role(path)
            dst = self.target.field("measure" if kind == "Measure" else "column", entity, prop)
            etype = "DISPLAYS" if shown else "DEPENDS_ON"
            self.b.edge(owner, dst, etype, confidence="derived", kind="definition", location=loc, snippet=snippet, attrs={"role": role})

    # -- PBIR
    def _pbir(self) -> None:
        pages: dict[str, dict[str, Any]] = {}
        order: list[str] = []
        for path in sorted(self.parts):
            if path.lower().endswith("pages/pages.json"):
                doc = _maybe_json(self.parts[path])
                if isinstance(doc, dict) and isinstance(doc.get("pageOrder"), list):
                    order = [str(x) for x in doc["pageOrder"]]
        for path in sorted(self.parts):
            m = _PAGE.search(path)
            if m:
                try:
                    doc = _json(self.parts[path])
                except ValueError as exc:
                    self.b.gap("parse_error", m.group(1), f"page.json cannot be read: {exc}", None, self.loc(path))
                    continue
                pages[m.group(1)] = {"doc": doc if isinstance(doc, dict) else {}, "part": path}
        for path in sorted(self.parts):
            m2 = _VISUAL.search(path)
            if m2:
                pages.setdefault(m2.group(1), {"doc": {}, "part": path})
        ordered = [p for p in order if p in pages] + sorted(p for p in pages if p not in order)
        for idx, pid in enumerate(ordered):
            info = pages[pid]
            self._page(pid, info["doc"], info["part"], idx)
        for path in sorted(self.parts):
            m3 = _VISUAL.search(path)
            if m3:
                self._pbir_visual(m3.group(1), m3.group(2), path)
        rep = next((p for p in sorted(self.parts) if p.lower().endswith("definition/report.json")), None)
        if rep is not None:
            doc = _maybe_json(self.parts[rep])
            if isinstance(doc, dict):
                fields: list[tuple[str, dict[str, Any], tuple[str, ...]]] = []
                self._walk(doc.get("filterConfig"), ("filterConfig",), fields, {})
                self._link(self.report_ref, fields, {}, rep, "report filter")

    def _page(self, pid: str, doc: dict[str, Any], part: str, idx: int) -> None:
        attrs: dict[str, Any] = {"order": idx}
        for k in ("displayName", "displayOption", "width", "height"):
            if k in doc and isinstance(doc[k], (str, int, float)):
                attrs[k] = doc[k]
        ref = self._page_ref(pid)
        self.b.asset(AssetFact(ref=ref, name=str(doc.get("displayName") or pid), attrs=attrs, parent=self.report_ref, workspace_id=self.ctx.workspace_id,
                               path=f"{self.ctx.item_name or 'report'} / {doc.get('displayName') or pid}"))
        fields: list[tuple[str, dict[str, Any], tuple[str, ...]]] = []
        self._walk(doc.get("filterConfig"), ("filterConfig",), fields, {})
        self._link(ref, fields, {}, part, "page filter", location={"page": pid})

    def _pbir_visual(self, pid: str, vid: str, part: str) -> None:
        try:
            doc = _json(self.parts[part])
        except ValueError as exc:
            self.b.gap("parse_error", vid, f"visual.json cannot be read: {exc}", None, self.loc(part, page=pid))
            return
        if not isinstance(doc, dict):
            self.b.gap("parse_error", vid, "visual.json is not a JSON object", None, self.loc(part, page=pid))
            return
        body = _obj(doc, "visual")
        attrs: dict[str, Any] = {"page": pid}
        if isinstance(body.get("visualType"), str):
            attrs["visual_type"] = body["visualType"]
        title = _title(body)
        ref = self._visual_ref(pid, vid)
        self.b.asset(AssetFact(ref=ref, name=title or str(doc.get("name") or vid), attrs={**attrs, **({"title": title} if title else {})}, parent=self._page_ref(pid),
                               workspace_id=self.ctx.workspace_id, path=f"{self.ctx.item_name or 'report'} / {pid} / {title or vid}"))
        if "singleVisualGroup" in doc or "visualGroup" in doc:
            return
        fields: list[tuple[str, dict[str, Any], tuple[str, ...]]] = []
        self._walk(body, (), fields, {})
        self._walk(doc.get("filterConfig"), ("filterConfig",), fields, {})
        self._link(ref, fields, {}, part, f"{attrs.get('visual_type', 'visual')} {title or vid}", location={"page": pid, "visual": vid})

    # -- legacy report.json
    def _legacy(self) -> None:
        part = next((p for p in sorted(self.parts) if p.lower() == "report.json" or p.lower().endswith("/report.json")), None)
        name = self.ctx.item_name or "report"
        if part is None:
            self.b.gap("not_inspected", name, "the definition has neither PBIR pages nor a legacy report.json", "Export the report definition with all parts.", self.loc())
            return
        try:
            doc = _json(self.parts[part])
        except ValueError as exc:
            self.b.gap("parse_error", name, f"report.json cannot be read: {exc}", "Check that the report definition is valid JSON.", self.loc(part))
            return
        if not isinstance(doc, dict):
            self.b.gap("parse_error", name, "report.json is not a JSON object", None, self.loc(part))
            return
        cfg = _maybe_json(doc.get("config"))
        for ent_group in cfg.get("modelExtensions", []) if isinstance(cfg, dict) and isinstance(cfg.get("modelExtensions"), list) else []:
            for ent in ent_group.get("entities", []) if isinstance(ent_group, dict) and isinstance(ent_group.get("entities"), list) else []:
                for ms in ent.get("measures", []) if isinstance(ent, dict) and isinstance(ent.get("measures"), list) else []:
                    if isinstance(ms, dict) and isinstance(ent.get("name"), str) and isinstance(ms.get("name"), str):
                        self.extensions.add((ent["name"].lower(), ms["name"].lower()))
        rfields: list[tuple[str, dict[str, Any], tuple[str, ...]]] = []
        self._walk(_maybe_json(doc.get("filters")), ("filters",), rfields, {})
        self._link(self.report_ref, rfields, {}, part, "report filter")
        for si, sec in enumerate(doc.get("sections", []) if isinstance(doc.get("sections"), list) else []):
            if not isinstance(sec, dict):
                continue
            pid = str(sec.get("name") or f"section{si}")
            pref = self._page_ref(pid)
            attrs: dict[str, Any] = {"order": si}
            for k in ("displayName", "displayOption", "width", "height"):
                if isinstance(sec.get(k), (str, int, float)):
                    attrs[k] = sec[k]
            self.b.asset(AssetFact(ref=pref, name=str(sec.get("displayName") or pid), attrs=attrs, parent=self.report_ref, workspace_id=self.ctx.workspace_id,
                                   path=f"{name} / {sec.get('displayName') or pid}"))
            pf: list[tuple[str, dict[str, Any], tuple[str, ...]]] = []
            self._walk(_maybe_json(sec.get("filters")), ("filters",), pf, {})
            self._link(pref, pf, {}, part, "page filter", location={"page": pid})
            for vi, vc in enumerate(sec.get("visualContainers", []) if isinstance(sec.get("visualContainers"), list) else []):
                if isinstance(vc, dict):
                    self._legacy_visual(pid, vi, vc, part)

    def _legacy_visual(self, pid: str, vi: int, vc: dict[str, Any], part: str) -> None:
        cfg = _maybe_json(vc.get("config"))
        if not isinstance(cfg, dict):
            self.b.gap("parse_error", f"{pid}/visual {vi}", "a visual container's config cannot be read", None, self.loc(part, page=pid))
            return
        vid = str(cfg.get("name") or f"visual{vi}")
        single = _obj(cfg, "singleVisual")
        attrs: dict[str, Any] = {"page": pid}
        if isinstance(single.get("visualType"), str):
            attrs["visual_type"] = single["visualType"]
        ref = self._visual_ref(pid, vid)
        self.b.asset(AssetFact(ref=ref, name=vid, attrs=attrs, parent=self._page_ref(pid), workspace_id=self.ctx.workspace_id, path=f"{self.ctx.item_name or 'report'} / {pid} / {vid}"))
        proto = _obj(single, "prototypeQuery")
        aliases = {str(f["Name"]): str(f["Entity"]) for f in proto.get("From", []) if isinstance(f, dict) and "Name" in f and "Entity" in f} if isinstance(proto.get("From"), list) else {}
        roles: dict[str, str] = {}
        projections = single.get("projections")
        for role, items in projections.items() if isinstance(projections, dict) else []:
            for it in items if isinstance(items, list) else []:
                if isinstance(it, dict) and isinstance(it.get("queryRef"), str):
                    roles.setdefault(it["queryRef"], str(role))
        loc = {"page": pid, "visual": vid}
        snippet = f"{attrs.get('visual_type', 'visual')} {vid}"
        for entry in proto.get("Select", []) if isinstance(proto.get("Select"), list) else []:
            found: list[tuple[str, dict[str, Any], tuple[str, ...]]] = []
            self._walk(entry, (), found, aliases)
            role = roles.get(str(entry.get("Name"))) if isinstance(entry, dict) else None
            for kind, field, _path in found:
                self._link_one(ref, kind, field, aliases, part, snippet, role, loc)
        extra: list[tuple[str, dict[str, Any], tuple[str, ...]]] = []
        self._walk(proto.get("OrderBy"), ("orderby",), extra, aliases)
        self._walk(proto.get("Where"), ("where",), extra, aliases)
        self._walk(single.get("objects"), ("objects",), extra, aliases)
        self._walk(_maybe_json(vc.get("filters")), ("filters",), extra, aliases)
        self._link(ref, extra, aliases, part, snippet, location=loc)

    def _link_one(self, owner: AssetRef, kind: str, field: dict[str, Any], aliases: dict[str, str], part: str, snippet: str, role: str | None, loc: dict[str, Any]) -> None:
        """Link one field of a legacy ``prototypeQuery.Select`` entry: displayed when it has a projection role, otherwise it only supports the query (sort / measure formatting)."""
        if self.target is None:
            return
        entity = self._entity(field, aliases)
        prop = field.get("Property")
        if entity is None or not isinstance(prop, str):
            return
        if kind == "Measure" and (entity.lower(), prop.lower()) in self.extensions:
            self.b.gap("unsupported_construct", f"{entity}[{prop}]", "report-level measure: it is defined in the report, not in the semantic model, so it is not linked",
                       "Informational: move the measure into the model to track it.", {**self.loc(part, **loc), "informational": True})
            return
        dst = self.target.field("measure" if kind == "Measure" else "column", entity, prop)
        self.b.edge(owner, dst, "DISPLAYS" if role else "DEPENDS_ON", confidence="derived", kind="definition", location=self.loc(part, **loc), snippet=snippet,
                    attrs={"role": role or "query"})


def _title(body: dict[str, Any]) -> str | None:
    """The literal title of a PBIR visual (``visualContainerObjects.title[0].properties.text.expr.Literal.Value``), unquoted, or ``None``."""
    try:
        val = body["visualContainerObjects"]["title"][0]["properties"]["text"]["expr"]["Literal"]["Value"]
    except (KeyError, IndexError, TypeError):
        return None
    if isinstance(val, str):
        text = val[1:-1].replace("''", "'") if len(val) >= 2 and val[0] == "'" and val[-1] == "'" else val
        return clip(text, 120)
    return None


def analyze_report_pbi(parts: Any, *, context: Any = None) -> Facts:
    """Analyse a Power BI report definition (PBIR parts or legacy ``report.json`` + ``definition.pbir``), see the module docstring. Never raises."""
    ctx = LineageContext.coerce(context)
    if isinstance(parts, str):
        if len(parts) > MAX_JSON_BYTES:
            return facts_with_gap(REPORT_PBI, "unsupported_construct", ctx.item_name or "report", f"the report definition is larger than {MAX_JSON_BYTES} bytes and was not analysed",
                                  "Export the report definition as separate parts.", ctx.loc())
        try:
            parts = json.loads(parts)
        except ValueError:
            parts = None
    if not isinstance(parts, Mapping) or not parts:
        return facts_with_gap(REPORT_PBI, "not_inspected", ctx.item_name or "report", "the report definition has no parts to analyse", "Export the report definition with all parts.", ctx.loc())
    over = len(parts) > MAX_PARTS
    if over:
        parts = dict(sorted(((str(k), v) for k, v in parts.items()), key=lambda kv: kv[0])[:MAX_PARTS])
    analyzer = _ReportAnalyzer(parts, ctx)
    if over:
        analyzer.b.gap("unsupported_construct", ctx.item_name or "report", f"the report definition has more than {MAX_PARTS} parts; the rest were not analysed", None, ctx.loc())
    return analyzer.run()


__all__ = ["analyze_report_pbi"]
