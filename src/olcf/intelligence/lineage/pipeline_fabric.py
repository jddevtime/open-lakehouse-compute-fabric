# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Lineage of Microsoft Fabric Data Pipelines (``pipeline-content.json``).

:func:`analyze_fabric_pipeline` ``(defn, *, context) -> Facts`` reads ``properties.activities`` (or a top-level ``activities`` array) and produces:

* the pipeline asset (``context.process`` or a ``pipeline`` asset aliased by its Fabric id) and one ``activity`` asset per activity, nested containers included
  (``IfCondition`` branches, ``Switch`` cases, ``ForEach`` / ``Until`` bodies), ``CONTAINS`` and ``DEPENDS_ON`` (``dependsOn`` with ``dependencyConditions``) edges;
* ``EXECUTES`` edges from the activity to the item it runs, identified by Fabric GUIDs (``fabric`` alias ``<tenant>/<workspace>/<item>``, built with
  :func:`~olcf.intelligence.model.fabric_alias`): ``TridentNotebook`` (``notebookId``), ``SparkJobDefinition``, ``ExecutePipeline`` (``pipeline.referenceName`` / ``pipelineId``),
  ``RefreshDataflow`` / ``Dataflow`` (``dataflowId``), ``PBISemanticModelRefresh`` (``pbi`` alias ``<workspace>/<dataset>``); ``DatabricksNotebook`` and
  ``SqlServerStoredProcedure`` point to an ``external`` asset and report a ``not_inspected`` gap because the code lives outside the pipeline;
* ``Copy`` activities: ``READS_FROM`` the source and ``WRITES_TO`` the sink. Lakehouse and Warehouse tables carry the Fabric table alias
  (:func:`~olcf.intelligence.lineage.base.fabric_table_alias`), lakehouse files are ``Files/<folder>/<file>``, other stores are ``external`` assets named
  ``<store type>:<connection id>/<database.schema.table or path>``. An explicit ``translator`` column mapping between two tables gives ``TRANSFORMS`` edges; without a
  translator no column edge is created (Fabric maps by name at run time, which the definition does not show);
* ``Lookup`` (source dataset or query) -> ``READS_FROM``; ``Script`` (T-SQL ``scripts[].text``) and ``sqlReaderQuery`` texts are analysed by the SQL analyzer (T-SQL).

Parameters with default values (``@pipeline().parameters.x`` or ``@{pipeline().parameters.x}``) are substituted and the resulting edges are ``inferred``; other expressions are
``dynamic_reference`` gaps. Activity types that do not move data (``Wait``, ``SetVariable``, ``Fail``, ``Web`` ...) only produce their asset (``WebActivity`` / ``WebHook`` with a literal URL
also ``READS_FROM`` an ``external`` asset); ``Delete``, ``GetMetadata``, ``AzureFunction``, ``Teams`` and ``Office365Outlook`` are not interpreted. The table name ``dbo.<t>`` is
recorded as ``<t>`` because ``dbo`` is the default schema of non-schema-enabled lakehouses and warehouses.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any
from urllib.parse import urlsplit

from olcf.intelligence.lineage.base import (
    PIPELINE_FABRIC,
    FactBuilder,
    LineageContext,
    classify_path,
    column_ref,
    fabric_column_alias,
    fabric_table_alias,
)
from olcf.intelligence.lineage.pipeline_common import (
    Node,
    Template,
    as_dict,
    emit_structure,
    resolve_template,
    walk,
)
from olcf.intelligence.lineage.sql import UNKNOWN_MARK, SqlSession
from olcf.intelligence.model import AssetFact, AssetRef, Facts, clip, fabric_alias, looks_like_guid

_DIALECT_BY_STORE = {"sqlserver": "tsql", "azuresql": "tsql", "datawarehouse": "tsql", "warehouse": "tsql", "synapse": "tsql", "fabricsql": "tsql", "snowflake": "snowflake",
                     "postgresql": "postgres", "mysql": "mysql"}


def _tp(node: Node) -> dict[str, Any]:
    return as_dict(node.raw.get("typeProperties"))


def analyze_fabric_pipeline(defn: dict[str, Any], *, context: LineageContext | None = None) -> Facts:
    """Analyse Fabric DataPipeline content JSON and return its lineage as :class:`~olcf.intelligence.model.Facts`; never raises."""
    ctx = LineageContext.coerce(context)
    b = FactBuilder(PIPELINE_FABRIC, ctx)
    try:
        _analyze(defn, ctx, b)
    except Exception as exc:  # noqa: BLE001 - analyzers never raise
        b.gap("parse_error", "pipeline", f"pipeline analysis failed unexpectedly ({type(exc).__name__}: {str(exc)[:160]})", "Report the definition to the OLCF maintainers.", ctx.loc(), PIPELINE_FABRIC)
    return b.facts()


def _analyze(defn: Any, ctx: LineageContext, b: FactBuilder) -> None:
    if not isinstance(defn, dict):
        b.gap("parse_error", "pipeline", "the pipeline content is not a JSON object", "Provide the pipeline-content.json document.", ctx.loc(), PIPELINE_FABRIC)
        return
    props = as_dict(defn.get("properties")) or defn
    name = ctx.item_name or str(defn.get("name") or "pipeline")
    key = ctx.item_id or name
    pipe = ctx.process
    if pipe is None:
        aliases = {"fabric": fabric_alias(ctx.tenant, ctx.fabric_workspace, ctx.item_id)} if ctx.fabric_workspace and ctx.item_id and looks_like_guid(ctx.item_id) else None
        pipe = AssetRef.make("pipeline", aliases=aliases, name=name, scope=ctx.scope())
        b.asset(AssetFact(ref=pipe, name=name, attrs={"format": "fabric-datapipeline"}, description=str(props.get("description") or "") or None, workspace_id=ctx.workspace_id))
    params = as_dict(props.get("parameters"))
    acts = props.get("activities")
    if not isinstance(acts, list):
        b.gap("parse_error", "pipeline", "the pipeline has no 'activities' array", None, ctx.loc(path="properties.activities"), PIPELINE_FABRIC)
        return
    base = "properties.activities" if "properties" in defn and isinstance(defn.get("properties"), dict) else "activities"
    nodes: list[Node] = []
    problems: list[tuple[str, str, str]] = []
    walk(acts, base, None, nodes, problems, [0])
    for kind, subject, detail in problems:
        b.gap(kind, subject, detail, None, ctx.loc(path=subject if subject.startswith(("activities", "properties")) else base), PIPELINE_FABRIC)
    refs = emit_structure(b, ctx, PIPELINE_FABRIC, pipe, key, name, nodes, lambda n: str(n.raw.get("type") or "unknown"))
    for n in nodes:
        t = str(n.raw.get("type") or "")
        try:
            _dispatch(b, ctx, n, refs[n.path], t, params)
        except Exception as exc:  # noqa: BLE001 - one broken activity must not hide the others
            b.gap("parse_error", n.name, f"activity '{n.name}' ({t}) could not be analysed ({type(exc).__name__}: {str(exc)[:120]})", None, ctx.loc(path=n.path, activity=n.name), PIPELINE_FABRIC)


def _dispatch(b: FactBuilder, ctx: LineageContext, n: Node, aref: AssetRef, t: str, params: dict[str, Any]) -> None:
    if t == "TridentNotebook":
        _item(b, ctx, n, aref, "notebook", "notebookId", params)
    elif t == "SparkJobDefinition":
        _item(b, ctx, n, aref, "spark_job", "sparkJobDefinitionId", params)
    elif t == "ExecutePipeline":
        _execute_pipeline(b, ctx, n, aref, params)
    elif t in ("RefreshDataflow", "Dataflow"):
        _item(b, ctx, n, aref, "dataflow", "dataflowId", params)
    elif t in ("PBISemanticModelRefresh", "SemanticModelRefresh"):
        _model_refresh(b, ctx, n, aref, params)
    elif t in ("DatabricksNotebook", "DatabricksSparkPython", "DatabricksSparkJar"):
        _external_code(b, ctx, n, aref, "databricks", str(_tp(n).get("notebookPath") or _tp(n).get("pythonFile") or _tp(n).get("mainClassName") or ""))
    elif t == "SqlServerStoredProcedure":
        _external_code(b, ctx, n, aref, "stored_procedure", str(_tp(n).get("storedProcedureName") or ""))
    elif t == "Copy":
        _copy(b, ctx, n, aref, params)
    elif t == "Lookup":
        _lookup(b, ctx, n, aref, params)
    elif t == "Script":
        _script(b, ctx, n, aref, params)
    elif t in ("WebActivity", "WebHook"):
        _web(b, ctx, n, aref, params)


def _tpl(b: FactBuilder, ctx: LineageContext, n: Node, value: Any, params: dict[str, Any], subpath: str, what: str) -> Template | None:
    """Resolve a property; a value known only at run time records a ``dynamic_reference`` gap and returns ``None``."""
    t = resolve_template(value, params)
    if not t.all_known:
        b.gap("dynamic_reference", clip(str(value), 100) or what, f"the {what} of activity '{n.name}' is an expression that is only known at run time", "Pass a literal id or a parameter with a default value.",
              ctx.loc(path=f"{n.path}.{subpath}", activity=n.name), PIPELINE_FABRIC)
        return None
    return t


def _workspace_of(ctx: LineageContext, n: Node, b: FactBuilder, params: dict[str, Any]) -> str | None:
    """The Fabric workspace GUID an activity's target item lives in (``typeProperties.workspaceId`` or the pipeline's own workspace)."""
    raw = _tp(n).get("workspaceId") or _tp(n).get("groupId")
    if raw:
        t = resolve_template(raw, params)
        if t.all_known and looks_like_guid(t.text):
            return t.text.lower()
        return None
    return ctx.fabric_workspace.lower() if ctx.fabric_workspace else None


def _item(b: FactBuilder, ctx: LineageContext, n: Node, aref: AssetRef, kind: str, key: str, params: dict[str, Any]) -> None:
    raw = _tp(n).get(key)
    if raw is None:
        b.gap("parse_error", n.name, f"activity '{n.name}' has no typeProperties.{key}", None, ctx.loc(path=f"{n.path}.typeProperties", activity=n.name), PIPELINE_FABRIC)
        return
    tpl = _tpl(b, ctx, n, raw, params, f"typeProperties.{key}", f"{kind} id")
    if tpl is None:
        return
    ws = _workspace_of(ctx, n, b, params)
    loc = ctx.loc(path=f"{n.path}.typeProperties.{key}", activity=n.name)
    if not looks_like_guid(tpl.text):
        b.gap("unresolved_reference", tpl.text, f"activity '{n.name}': '{clip(tpl.text, 60)}' is not a Fabric item id", None, loc, PIPELINE_FABRIC)
        return
    if ws is None:
        b.gap("unresolved_reference", tpl.text, f"activity '{n.name}' runs item {tpl.text} in a workspace that is not given in the activity or the analysis context",
              "Provide the workspace id (typeProperties.workspaceId) or analyse the pipeline with its workspace in the context.", loc, PIPELINE_FABRIC)
        return
    target = AssetRef.make(kind, aliases={"fabric": fabric_alias(ctx.tenant, ws, tpl.text)}, scope=ctx.scope(fabric_workspace=ws))
    b.edge(aref, target, "EXECUTES", confidence="inferred" if tpl.taint else "derived", kind="definition", location=loc, snippet=f"{n.raw.get('type')}: {tpl.text}", analyzer=PIPELINE_FABRIC)


def _execute_pipeline(b: FactBuilder, ctx: LineageContext, n: Node, aref: AssetRef, params: dict[str, Any]) -> None:
    tp = _tp(n)
    ref = as_dict(tp.get("pipeline"))
    raw = tp.get("pipelineId") or ref.get("referenceName") or ref.get("id")
    if raw is None:
        b.gap("parse_error", n.name, f"ExecutePipeline activity '{n.name}' has no pipeline reference", None, ctx.loc(path=f"{n.path}.typeProperties.pipeline", activity=n.name), PIPELINE_FABRIC)
        return
    tpl = _tpl(b, ctx, n, raw, params, "typeProperties.pipeline", "pipeline reference")
    if tpl is None:
        return
    loc = ctx.loc(path=f"{n.path}.typeProperties.pipeline", activity=n.name)
    ws = _workspace_of(ctx, n, b, params)
    if looks_like_guid(tpl.text) and ws is not None:
        target = AssetRef.make("pipeline", aliases={"fabric": fabric_alias(ctx.tenant, ws, tpl.text)}, scope=ctx.scope(fabric_workspace=ws))
    elif looks_like_guid(tpl.text):
        b.gap("unresolved_reference", tpl.text, f"activity '{n.name}' runs pipeline {tpl.text} in a workspace that is not known", None, loc, PIPELINE_FABRIC)
        return
    else:
        target = AssetRef.make("pipeline", name=tpl.text, scope=ctx.scope())
    b.edge(aref, target, "EXECUTES", confidence="inferred" if tpl.taint else "derived", kind="definition", location=loc, snippet=f"ExecutePipeline: {tpl.text}", analyzer=PIPELINE_FABRIC)


def _model_refresh(b: FactBuilder, ctx: LineageContext, n: Node, aref: AssetRef, params: dict[str, Any]) -> None:
    tp = _tp(n)
    raw = tp.get("semanticModelId") or tp.get("datasetId")
    if raw is None:
        return
    tpl = _tpl(b, ctx, n, raw, params, "typeProperties.semanticModelId", "semantic model id")
    ws = _workspace_of(ctx, n, b, params)
    if tpl is None or ws is None or not looks_like_guid(tpl.text):
        if tpl is not None:
            b.gap("unresolved_reference", tpl.text, f"activity '{n.name}': the semantic model or its workspace cannot be identified", None, ctx.loc(path=f"{n.path}.typeProperties", activity=n.name),
                  PIPELINE_FABRIC)
        return
    target = AssetRef.make("semantic_model", aliases={"pbi": f"{ws}/{tpl.text.lower()}"}, scope=ctx.scope(fabric_workspace=ws))
    b.edge(aref, target, "EXECUTES", confidence="inferred" if tpl.taint else "derived", kind="definition", location=ctx.loc(path=f"{n.path}.typeProperties", activity=n.name),
           snippet=f"refresh semantic model {tpl.text}", analyzer=PIPELINE_FABRIC)


def _external_code(b: FactBuilder, ctx: LineageContext, n: Node, aref: AssetRef, kind: str, what: str) -> None:
    loc = ctx.loc(path=f"{n.path}.typeProperties", activity=n.name)
    if not what or what.startswith("@"):
        b.gap("not_inspected", n.name, f"activity '{n.name}' runs code outside the pipeline ({kind}); what it reads and writes is unknown", "Document the dependency manually.", loc, PIPELINE_FABRIC)
        return
    ext = AssetRef.make("external", name=f"{kind}:{what}")
    b.edge(aref, ext, "EXECUTES", confidence="derived", kind="definition", location=loc, snippet=what, analyzer=PIPELINE_FABRIC)
    b.gap("not_inspected", n.name, f"the code of {kind} '{clip(what, 80)}' is not part of the pipeline definition; its data access is unknown", "Document the dependency manually.", loc, PIPELINE_FABRIC)


# ---------------------------------------------------------------------------------------------------------------------- datasets
class _Side:
    """A resolved copy / lookup side: the dataset ref, its kind and the information needed for column mappings."""

    def __init__(self, ref: AssetRef, kind: str, table: str | None, alias: str | None, taint: tuple[str, ...]) -> None:
        self.ref, self.kind, self.table, self.alias, self.taint = ref, kind, table, alias, taint


def _table_name(schema: str | None, table: str | None) -> str | None:
    if not table:
        return None
    if schema and schema.lower() != "dbo":
        return f"{schema}.{table}"
    return table


def _dataset(b: FactBuilder, ctx: LineageContext, n: Node, side: dict[str, Any], which: str, params: dict[str, Any]) -> _Side | None:
    """Resolve ``source`` / ``sink`` ``datasetSettings`` to a dataset reference."""
    base = f"{n.path}.typeProperties.{which}"
    loc = ctx.loc(path=base, activity=n.name)
    ds = as_dict(side.get("datasetSettings"))
    dtype = str(ds.get("type") or "")
    dtp = as_dict(ds.get("typeProperties"))
    ls = as_dict(as_dict(ds.get("linkedService")).get("properties"))
    ls_type = str(ls.get("type") or "")
    lstp = as_dict(ls.get("typeProperties"))
    conn = as_dict(ds.get("externalReferences")).get("connection") or as_dict(as_dict(ds.get("linkedService")).get("properties")).get("connection")
    taint: tuple[str, ...] = ()

    def val(v: Any, what: str) -> str | None:
        nonlocal taint
        t = _tpl(b, ctx, n, v, params, f"typeProperties.{which}.datasetSettings.typeProperties", what)
        if t is None:
            return None
        taint += t.taint
        return t.text

    if ls_type.lower() in ("lakehouse", "datawarehouse", "warehouse") or dtype.lower().startswith(("lakehouse", "datawarehouse")):
        art = val(lstp.get("artifactId"), "lakehouse id") if lstp.get("artifactId") else None
        ws_raw = lstp.get("workspaceId")
        ws = val(ws_raw, "workspace id") if ws_raw else (ctx.fabric_workspace or None)
        if art is None or not looks_like_guid(art) or ws is None or not looks_like_guid(ws):
            b.gap("unresolved_reference", n.name, f"the {ls_type or 'lakehouse'} of the {which} of activity '{n.name}' is not identified by GUIDs (artifactId / workspaceId)",
                  "Provide the Fabric workspace in the analysis context.", loc, PIPELINE_FABRIC)
            return None
        root = str(lstp.get("rootFolder") or "").lower()
        is_table = bool(dtp.get("table")) or root == "tables" or dtype.lower().endswith("table")
        sub = replace(ctx, default_lakehouse={"fabric_id": art.lower()}, fabric_workspace=ws.lower())
        if is_table:
            sch = val(dtp.get("schema"), "schema") if dtp.get("schema") else None
            tb = val(dtp.get("table"), "table") if dtp.get("table") else None
            name = _table_name(sch, tb)
            if name is None:
                b.gap("dynamic_reference", n.name, f"the table of the {which} of activity '{n.name}' is not given or only known at run time", None, loc, PIPELINE_FABRIC)
                return None
            alias = fabric_table_alias(ctx.tenant, ws, art, name)
            ref = AssetRef.make("table", aliases={"fabric": alias}, name=name, scope=sub.scope())
            return _Side(ref, "table", name, alias, taint)
        location = as_dict(dtp.get("location"))
        folder = val(location.get("folderPath"), "folder") if location.get("folderPath") else ""
        fname = val(location.get("fileName"), "file name") if location.get("fileName") else ""
        if folder is None or fname is None:
            return None
        rel = "/".join(p.strip("/") for p in (folder, fname) if p)
        path = f"Files/{rel}" if rel else "Files"
        ref = classify_path(path).ref(sub)
        return _Side(ref, "file", None, None, taint)
    # any other store: an external asset named by store type, connection and object
    store = (ls_type or dtype or str(side.get("type") or "unknown")).lower()
    conn_s = val(conn, "connection") if conn else ""
    if conn_s is None:
        return None
    obj = ""
    if dtp.get("table"):
        sch = val(dtp.get("schema"), "schema") if dtp.get("schema") else None
        tb = val(dtp.get("table"), "table")
        obj = ".".join(p for p in (str(dtp.get("database") or "") or None, sch, tb) if p)
    elif as_dict(dtp.get("location")):
        loc_d = as_dict(dtp["location"])
        parts = [str(loc_d.get(k) or "") for k in ("container", "bucketName", "folderPath", "fileName")]
        obj = "/".join(p.strip("/") for p in parts if p)
    elif dtp.get("relativeUrl") or dtp.get("url"):
        obj = str(dtp.get("relativeUrl") or dtp.get("url"))
    if not obj and not conn_s:
        b.gap("not_inspected", n.name, f"the {which} of activity '{n.name}' ({store}) does not identify a table or path", None, loc, PIPELINE_FABRIC)
        return None
    if UNKNOWN_MARK in obj:
        b.gap("dynamic_reference", n.name, f"the object of the {which} of activity '{n.name}' is only known at run time", None, loc, PIPELINE_FABRIC)
        return None
    ref = AssetRef.make("external", name=f"{store}:{conn_s or '*'}/{obj}".rstrip("/"))
    return _Side(ref, "external", None, None, taint)


def _copy(b: FactBuilder, ctx: LineageContext, n: Node, aref: AssetRef, params: dict[str, Any]) -> None:
    tp = _tp(n)
    src_raw, snk_raw = as_dict(tp.get("source")), as_dict(tp.get("sink"))
    src = _dataset(b, ctx, n, src_raw, "source", params) if src_raw else None
    snk = _dataset(b, ctx, n, snk_raw, "sink", params) if snk_raw else None
    if not src_raw or not snk_raw:
        b.gap("parse_error", n.name, f"Copy activity '{n.name}' has no source or no sink", None, ctx.loc(path=f"{n.path}.typeProperties", activity=n.name), PIPELINE_FABRIC)
    if src is not None:
        _read(b, ctx, n, aref, src, src_raw)
    if snk is not None:
        loc = ctx.loc(path=f"{n.path}.typeProperties.sink", activity=n.name)
        action = str(snk_raw.get("tableActionOption") or snk_raw.get("writeBehavior") or "append").lower()
        mode = "overwrite" if action in ("overwrite", "truncate") else "append"
        b.edge(aref, snk.ref, "WRITES_TO", confidence="inferred" if snk.taint else "derived", kind="definition", location=loc, snippet=clip(str(snk_raw.get("type") or "sink"), 80),
               attrs={"dataset_kind": snk.kind, "mode": mode}, analyzer=PIPELINE_FABRIC)
    if src is not None and snk is not None:
        _translator(b, ctx, n, aref, src, snk, tp.get("translator"))


def _translator(b: FactBuilder, ctx: LineageContext, n: Node, aref: AssetRef, src: _Side, snk: _Side, translator: Any) -> None:
    """Explicit column mappings (``TabularTranslator``) between two tables become ``TRANSFORMS`` edges."""
    if not isinstance(translator, dict):
        return
    loc = ctx.loc(path=f"{n.path}.typeProperties.translator", activity=n.name)
    if src.kind != "table" or snk.kind != "table":
        if translator.get("mappings"):
            b.gap("not_inspected", n.name, f"column mappings of Copy activity '{n.name}' are not recorded because the source or sink is not a Fabric table", None, loc, PIPELINE_FABRIC)
        return
    maps = translator.get("mappings")
    if not isinstance(maps, list):
        return
    for i, m in enumerate(maps):
        s, d = as_dict(as_dict(m).get("source")), as_dict(as_dict(m).get("sink"))
        sc, dc = s.get("name") or s.get("path"), d.get("name") or d.get("path")
        if not isinstance(sc, str) or not isinstance(dc, str) or not sc or not dc:
            b.gap("unsupported_construct", f"{n.name}.mappings[{i}]", "a column mapping without plain source and sink names was not recorded", None, loc, PIPELINE_FABRIC)
            continue
        sref = _column(src, sc)
        dref = _column(snk, dc)
        b.edge(sref, dref, "TRANSFORMS", confidence="derived", kind="definition", location={**loc, "mapping": i}, snippet=f"{sc} -> {dc}", via=aref,
               attrs={"kind": "direct", "mode": "copy"}, analyzer=PIPELINE_FABRIC)


def _column(side: _Side, column: str) -> AssetRef:
    base = column_ref(side.ref, column)
    if side.alias:
        return AssetRef.make("column", aliases={"fabric_column": fabric_column_alias(side.alias, column)}, name=column, scope=base.scope_map)
    return base


def _store_dialect(side: dict[str, Any], default: str) -> str:
    t = str(side.get("type") or "").lower()
    for k, v in _DIALECT_BY_STORE.items():
        if k in t:
            return v
    return default


def _sql_text(b: FactBuilder, ctx: LineageContext, n: Node, aref: AssetRef, sql: str, subpath: str, dialect: str) -> None:
    sub = replace(ctx, default_lakehouse=None)
    session = SqlSession(sub, b, dialect, analyzer=PIPELINE_FABRIC, emit_query_assets=False)
    session.evidence_kind = "definition"
    session.run(sql, process=aref, loc=ctx.loc(path=f"{n.path}.{subpath}", activity=n.name))


def _lookup(b: FactBuilder, ctx: LineageContext, n: Node, aref: AssetRef, params: dict[str, Any]) -> None:
    src_raw = as_dict(_tp(n).get("source"))
    if not src_raw:
        return
    src = _dataset(b, ctx, n, src_raw, "source", params)
    if src is not None:
        _read(b, ctx, n, aref, src, src_raw)
    q = src_raw.get("sqlReaderQuery") or src_raw.get("query")
    if isinstance(q, str) and q.startswith("@"):
        b.gap("dynamic_reference", n.name, f"the query of Lookup activity '{n.name}' is an expression; the tables it reads are unknown", None, ctx.loc(path=f"{n.path}.typeProperties.source", activity=n.name),
              PIPELINE_FABRIC)


def _read(b: FactBuilder, ctx: LineageContext, n: Node, aref: AssetRef, src: _Side, src_raw: dict[str, Any]) -> None:
    """``READS_FROM`` the source of a copy / lookup. A query on a Fabric table store is analysed (T-SQL); a query on an external store is kept as an attribute only, because its
    table names belong to that system, not to the Fabric workspace."""
    loc = ctx.loc(path=f"{n.path}.typeProperties.source", activity=n.name)
    q = src_raw.get("sqlReaderQuery") or src_raw.get("query")
    attrs: dict[str, Any] = {"dataset_kind": src.kind}
    has_query = isinstance(q, str) and q.strip() and not q.startswith("@")
    if has_query and src.kind == "external":
        attrs["query"] = clip(str(q), 500)
    b.edge(aref, src.ref, "READS_FROM", confidence="inferred" if src.taint else "derived", kind="definition", location=loc, snippet=clip(str(src_raw.get("type") or "source"), 80),
           attrs=attrs, analyzer=PIPELINE_FABRIC)
    if has_query and src.kind == "table":
        _sql_text(b, ctx, n, aref, str(q), f"typeProperties.source.{'sqlReaderQuery' if src_raw.get('sqlReaderQuery') else 'query'}", _store_dialect(src_raw, "tsql"))


def _script(b: FactBuilder, ctx: LineageContext, n: Node, aref: AssetRef, params: dict[str, Any]) -> None:
    scripts = _tp(n).get("scripts")
    if not isinstance(scripts, list):
        return
    for i, sc in enumerate(scripts):
        text = as_dict(sc).get("text")
        loc = ctx.loc(path=f"{n.path}.typeProperties.scripts[{i}].text", activity=n.name)
        if not isinstance(text, str) or not text.strip():
            continue
        if text.startswith("@") or "@{" in text:
            tpl = resolve_template(text, params)
            text = tpl.text
        _sql_text(b, ctx, n, aref, text, f"typeProperties.scripts[{i}].text", "tsql")
        if UNKNOWN_MARK in text:
            b.gap("dynamic_reference", n.name, f"script {i} of activity '{n.name}' contains expressions evaluated at run time", None, loc, PIPELINE_FABRIC)


def _web(b: FactBuilder, ctx: LineageContext, n: Node, aref: AssetRef, params: dict[str, Any]) -> None:
    url = _tp(n).get("url")
    loc = ctx.loc(path=f"{n.path}.typeProperties.url", activity=n.name)
    if not isinstance(url, str) or not url:
        return
    tpl = resolve_template(url, params)
    if not tpl.all_known:
        b.gap("dynamic_reference", n.name, f"the URL called by activity '{n.name}' is only known at run time", None, loc, PIPELINE_FABRIC)
        return
    parts = urlsplit(tpl.text)
    if not parts.scheme or not parts.hostname:
        return
    name = f"{parts.scheme}://{parts.hostname}{':' + str(parts.port) if parts.port else ''}{parts.path}"
    b.edge(aref, AssetRef.make("external", name=name), "READS_FROM", confidence="inferred" if tpl.taint else "derived", kind="definition", location=loc,
           snippet=f"{_tp(n).get('method', 'GET')} {name}", attrs={"dataset_kind": "external", "protocol": parts.scheme}, analyzer=PIPELINE_FABRIC)


__all__ = ["analyze_fabric_pipeline"]
