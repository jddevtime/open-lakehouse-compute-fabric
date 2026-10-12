# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Lineage from a Power BI admin *scanner* result (``GET /admin/workspaces/scanResult/{scanId}``), without opening any model.

:func:`analyze_scanner_result` ``(doc, *, context)`` reads ``{"workspaces": [...], "datasourceInstances": [...]}`` and emits, per workspace:

* ``semantic_model`` / ``report`` / ``dashboard`` / ``dataflow`` assets with a ``fabric`` alias (``<tenant>/<workspace>/<item>``) **and** a ``pbi`` alias, so they merge with the same
  objects found through the Fabric API or from a definition;
* ``DEPENDS_ON`` report -> semantic model (``datasetId``), dashboard -> report / semantic model (tiles), semantic model -> upstream semantic model (``upstreamDatasets``) and ->
  dataflow (``upstreamDataflows``), dataflow -> upstream dataflow;
* ``READS_FROM`` semantic model / dataflow -> ``external`` data source (``datasourceUsages`` joined with ``datasourceInstances``), named ``<type>:<server or url>/<database>``;
* for datasets whose scan carried ``datasetSchema`` / ``datasetExpressions``: the model tables, columns and measures and the same column, measure and M-source lineage the
  semantic-model analyzer produces (the scanner result is turned into a model and analysed by :func:`~olcf.intelligence.lineage.semantic_model.analyze_model`). The data-source
  level ``READS_FROM`` is then dropped for that dataset, because the tables read are known. A column's source name is **assumed** to equal its model name (the scanner does not return
  ``sourceColumn``), so those column edges are ``inferred``.

What the scan did not contain is reported, per dataset and per field, as ``not_inspected`` gaps that name the scanner parameter to switch on: ``datasetSchema`` (tables, columns,
measures), ``datasetExpressions`` (DAX and M text), ``lineage`` (upstream datasets / dataflows / data source usages) and ``datasourceDetails`` (the data source instances). Facts from the
scanner carry evidence kind ``metadata`` when they are read from the scan result itself and ``static`` when they come from analysing the DAX / M text it returned. Not interpreted: users, sensitivity labels, endorsement, refresh schedules, Lakehouse / Warehouse / notebook entries (the Fabric API connector
covers them), paginated report data sources beyond ``datasourceUsages``, and behaviour beyond the documented shape of the scanner result.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from olcf.intelligence.lineage.base import (
    MAX_PARTS,
    SCANNER,
    FactBuilder,
    LineageContext,
    facts_with_gap,
    pbi_alias,
)
from olcf.intelligence.lineage.semantic_model import analyze_model
from olcf.intelligence.lineage.tmdl import ColDef, ExprDef, MeasureDef, Model, PartitionDef, TableDef
from olcf.intelligence.model import AssetFact, AssetRef, Facts, clip, fabric_alias, looks_like_guid


def _d(v: Any) -> dict[str, Any]:
    return v if isinstance(v, dict) else {}


def _list(v: Any) -> list[dict[str, Any]]:
    return [x for x in v if isinstance(x, dict)] if isinstance(v, list) else []


def _str(v: Any) -> str | None:
    return v if isinstance(v, str) and v.strip() else None


class _Scan:
    def __init__(self, doc: dict[str, Any], ctx: LineageContext) -> None:
        self.doc = doc
        self.ctx = ctx
        self.b = FactBuilder(SCANNER, ctx)
        self.instances = {str(d.get("datasourceId") or d.get("id")): d for d in _list(doc.get("datasourceInstances"))}
        self.has_instances = "datasourceInstances" in doc

    # -- references
    def ref(self, kind: str, ws: str, oid: str, name: str | None) -> AssetRef:
        """Reference to a Power BI item: ``fabric`` and ``pbi`` aliases, scoped to the workspace."""
        return AssetRef.make(kind, {"fabric": fabric_alias(self.ctx.tenant, ws, oid), "pbi": pbi_alias(ws, oid)}, name, {"fabric_workspace": ws})

    def unresolved(self, kind: str, ws: str, oid: str) -> AssetRef:
        """Reference to an item whose workspace is not known: only its id is."""
        return AssetRef.make(kind, None, oid, {"fabric_workspace": ws})

    def loc(self, **kw: Any) -> dict[str, Any]:
        """Evidence location for scanner facts."""
        return self.ctx.loc(**kw)

    def datasource_ref(self, inst: dict[str, Any]) -> tuple[AssetRef, str]:
        """The ``external`` asset for a data source instance and its display name."""
        cd = _d(inst.get("connectionDetails"))
        typ = str(inst.get("datasourceType") or "unknown").lower()
        where = _str(cd.get("server")) or _str(cd.get("url")) or _str(cd.get("path")) or _str(cd.get("account")) or _str(cd.get("domain")) or "*"
        db = _str(cd.get("database"))
        name = f"{typ}:{where.lower() if where != '*' else where}" + (f"/{db}" if db else "")
        return AssetRef.make("external", name=name), name

    # -- driver
    def run(self) -> Facts:
        """Walk all workspaces."""
        workspaces = _list(self.doc.get("workspaces"))
        if len(workspaces) > MAX_PARTS:
            self.b.gap("unsupported_construct", "scanResult", f"more than {MAX_PARTS} workspaces: the rest were not analysed", None, self.loc())
            workspaces = workspaces[:MAX_PARTS]
        for ws in workspaces:
            self._workspace(ws)
        return self.b.facts()

    def _workspace(self, ws: dict[str, Any]) -> None:
        wid = _str(ws.get("id"))
        wname = _str(ws.get("name")) or wid or "workspace"
        if wid is None or not looks_like_guid(wid):
            self.b.gap("parse_error", wname, "a workspace in the scan result has no valid id", None, self.loc())
            return
        wid = wid.lower()
        datasets = _list(ws.get("datasets"))
        reports = _list(ws.get("reports"))
        local_datasets = {str(d.get("id")).lower() for d in datasets if d.get("id")}
        local_reports = {str(r.get("id")).lower() for r in reports if r.get("id")}
        for d in datasets:
            self._dataset(wid, wname, d)
        for r in reports:
            self._report(wid, wname, r, local_datasets)
        for dash in _list(ws.get("dashboards")):
            self._dashboard(wid, wname, dash, local_reports, local_datasets)
        for flow in _list(ws.get("dataflows")):
            self._dataflow(wid, wname, flow)

    # -- reports / dashboards
    def _report(self, ws: str, wname: str, r: dict[str, Any], local_datasets: set[str]) -> None:
        rid = _str(r.get("id"))
        if rid is None or not looks_like_guid(rid):
            return
        rname = _str(r.get("name")) or rid
        ref = self.ref("report", ws, rid, rname)
        attrs = {k: r[k] for k in ("reportType", "createdDateTime", "modifiedDateTime", "webUrl") if isinstance(r.get(k), str)}
        attrs["workspace_name"] = wname
        self.b.asset(AssetFact(ref=ref, name=rname, attrs=attrs, workspace_id=self.ctx.workspace_id, path=f"{wname} / {rname}"))
        ds = _str(r.get("datasetId"))
        if ds is None:
            if r.get("reportType") != "PaginatedReport":
                self.b.gap("not_inspected", rname, "the report has no datasetId in the scan result", None, self.loc(workspace=ws, report=rid))
            return
        dws = _str(r.get("datasetWorkspaceId"))
        target_ws = dws.lower() if dws and looks_like_guid(dws) else (ws if ds.lower() in local_datasets else None)
        if target_ws is None:
            self.b.edge(ref, self.unresolved("semantic_model", ws, ds.lower()), "DEPENDS_ON", confidence="derived", kind="metadata", location=self.loc(workspace=ws, report=rid))
            self.b.gap("unresolved_reference", rname, f"the report's dataset {ds} is not in workspace '{wname}' and the scan gives no dataset workspace", None,
                       {**self.loc(workspace=ws, report=rid), "informational": True})
            return
        self.b.edge(ref, self.ref("semantic_model", target_ws, ds.lower(), None), "DEPENDS_ON", confidence="derived", kind="metadata", location=self.loc(workspace=ws, report=rid))

    def _dashboard(self, ws: str, wname: str, d: dict[str, Any], local_reports: set[str], local_datasets: set[str]) -> None:
        did = _str(d.get("id"))
        if did is None or not looks_like_guid(did):
            return
        name = _str(d.get("displayName")) or _str(d.get("name")) or did
        ref = self.ref("dashboard", ws, did, name)
        self.b.asset(AssetFact(ref=ref, name=name, attrs={"workspace_name": wname, "tile_count": len(_list(d.get("tiles")))}, workspace_id=self.ctx.workspace_id, path=f"{wname} / {name}"))
        for t in _list(d.get("tiles")):
            rid, dsid = _str(t.get("reportId")), _str(t.get("datasetId"))
            loc = self.loc(workspace=ws, dashboard=did, tile=_str(t.get("id")))
            if rid and rid.lower() in local_reports:
                self.b.edge(ref, self.ref("report", ws, rid.lower(), None), "DEPENDS_ON", confidence="derived", kind="metadata", location=loc)
            elif dsid and dsid.lower() in local_datasets:
                self.b.edge(ref, self.ref("semantic_model", ws, dsid.lower(), None), "DEPENDS_ON", confidence="derived", kind="metadata", location=loc)
            elif rid or dsid:
                self.b.gap("unresolved_reference", name, f"tile '{_str(t.get('title')) or _str(t.get('id')) or '?'}' refers to {'report ' + rid if rid else 'dataset ' + str(dsid)}, "
                           f"which is not in workspace '{wname}'", None, {**loc, "informational": True})

    # -- dataflows
    def _dataflow(self, ws: str, wname: str, f: dict[str, Any]) -> None:
        fid = _str(f.get("objectId")) or _str(f.get("id"))
        if fid is None or not looks_like_guid(fid):
            return
        name = _str(f.get("name")) or fid
        ref = self.ref("dataflow", ws, fid.lower(), name)
        self.b.asset(AssetFact(ref=ref, name=name, attrs={"workspace_name": wname}, workspace_id=self.ctx.workspace_id, path=f"{wname} / {name}"))
        self._lineage(ref, ws, name, f, fid, expect_datasets=False)

    # -- datasets
    def _dataset(self, ws: str, wname: str, d: dict[str, Any]) -> None:
        did = _str(d.get("id"))
        if did is None or not looks_like_guid(did):
            return
        did = did.lower()
        name = _str(d.get("name")) or did
        ref = self.ref("semantic_model", ws, did, name)
        attrs: dict[str, Any] = {"workspace_name": wname}
        for k in ("configuredBy", "targetStorageMode", "contentProviderType", "createdDate"):
            if isinstance(d.get(k), str):
                attrs[k] = d[k]
        self.b.asset(AssetFact(ref=ref, name=name, attrs=attrs, workspace_id=self.ctx.workspace_id, path=f"{wname} / {name}"))
        analysed = self._model(ws, did, name, ref, d)
        self._lineage(ref, ws, name, d, did, expect_datasets=True, skip_sources=analysed)

    def _model(self, ws: str, did: str, name: str, ref: AssetRef, d: dict[str, Any]) -> bool:
        """Analyse the tables / columns / measures of a dataset when the scan has them. Returns True when partition sources (M text) were analysed."""
        loc = self.loc(workspace=ws, dataset=did)
        if "tables" not in d:
            self.b.gap("not_inspected", name, "tables, columns and measures were not returned: run the scan with datasetSchema=True", "Enable datasetSchema in the metadata scan.", loc)
            return False
        model = Model(name=name)
        missing_expr = False
        for t in _list(d.get("tables")):
            tname = _str(t.get("name"))
            if tname is None:
                continue
            tdef = TableDef(tname, is_hidden=t.get("isHidden") is True, description=_str(t.get("description")))
            calc_table = False
            for c in _list(t.get("columns")):
                cname = _str(c.get("name"))
                if cname is None:
                    continue
                ctype = str(c.get("columnType") or "Data").lower()
                if ctype == "rownumber":
                    continue
                if ctype == "calculatedtablecolumn":
                    calc_table = True
                expr = _str(c.get("expression")) if ctype == "calculated" else None
                if ctype == "calculated":
                    missing_expr = missing_expr or expr is None
                tdef.columns.append(ColDef(cname, expression=expr, data_type=_str(c.get("dataType"))))
            for m in _list(t.get("measures")):
                mname = _str(m.get("name"))
                if mname is None:
                    continue
                missing_expr = missing_expr or not _str(m.get("expression"))
                tdef.measures.append(MeasureDef(mname, str(m.get("expression") or "")))
            src = [s for s in _list(t.get("source")) if _str(s.get("expression"))]
            if src:
                tdef.partitions.append(PartitionDef(tname, "calculated" if calc_table else "m", expression=str(src[0]["expression"])))
            elif "source" not in t:
                missing_expr = True
            model.tables.append(tdef)
        for e in _list(d.get("expressions")):
            en = _str(e.get("name"))
            if en is not None:
                model.expressions.append(ExprDef(en, str(e.get("expression") or "")))
        if missing_expr and any(t.measures or t.columns for t in model.tables):
            self.b.gap("not_inspected", name, "DAX and Power Query expressions were not (all) returned: run the scan with datasetExpressions=True", "Enable datasetExpressions in the metadata scan.", loc)
        mctx = replace(self.ctx, item_id=did, item_name=name, fabric_workspace=ws, process=ref, location={**self.ctx.location, "workspace": ws, "dataset": did})
        facts = analyze_model(model, context=mctx, fmt="scanner")
        self.b.merge(facts)
        return any(p.expression for t in model.tables for p in t.partitions)

    def _lineage(self, ref: AssetRef, ws: str, name: str, obj: dict[str, Any], oid: str, *, expect_datasets: bool, skip_sources: bool = False) -> None:
        loc = self.loc(workspace=ws, item=oid)
        keys = ("upstreamDataflows", "datasourceUsages") + (("upstreamDatasets",) if expect_datasets else ())
        if not any(k in obj for k in keys):
            self.b.gap("not_inspected", name, "upstream datasets / dataflows and data source usages were not returned: run the scan with lineage=True", "Enable lineage in the metadata scan.", loc)
            return
        for u in _list(obj.get("upstreamDatasets")):
            tid, gid = _str(u.get("targetDatasetId")), _str(u.get("groupId"))
            if tid and gid and looks_like_guid(tid) and looks_like_guid(gid):
                self.b.edge(ref, self.ref("semantic_model", gid.lower(), tid.lower(), None), "DEPENDS_ON", confidence="derived", kind="metadata", location=loc)
        for u in _list(obj.get("upstreamDataflows")):
            tid, gid = _str(u.get("targetDataflowId")), _str(u.get("groupId"))
            if tid and gid and looks_like_guid(tid) and looks_like_guid(gid):
                self.b.edge(ref, self.ref("dataflow", gid.lower(), tid.lower(), None), "DEPENDS_ON", confidence="derived", kind="metadata", location=loc)
        for use in _list(obj.get("datasourceUsages")):
            iid = _str(use.get("datasourceInstanceId"))
            if iid is None:
                continue
            inst = self.instances.get(iid)
            if inst is None:
                if not skip_sources:
                    self.b.gap("not_inspected", name, f"data source instance {iid} is not described in the scan result: run the scan with datasourceDetails=True",
                               "Enable datasourceDetails in the metadata scan.", loc)
                continue
            if skip_sources:
                continue
            dref, dname = self.datasource_ref(inst)
            self.b.edge(ref, dref, "READS_FROM", confidence="derived", kind="metadata", location=loc, snippet=clip(dname, 200), attrs={"datasource_type": str(inst.get("datasourceType") or "")})


def analyze_scanner_result(doc: Any, *, context: Any = None) -> Facts:
    """Analyse a Power BI admin scanner result document (see the module docstring); never raises. ``context`` carries the ``tenant``."""
    ctx = LineageContext.coerce(context)
    if not isinstance(doc, dict) or not isinstance(doc.get("workspaces"), list):
        return facts_with_gap(SCANNER, "parse_error", ctx.item_name or "scanResult", "the scanner result is not an object with a 'workspaces' array",
                              "Save the body of GET admin/workspaces/scanResult/{scanId} unchanged.", ctx.loc())
    return _Scan(doc, ctx).run()


__all__ = ["analyze_scanner_result"]
