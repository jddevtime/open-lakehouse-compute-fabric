# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Lineage of OLCF pipelines (``olcf.pipeline/1`` definitions).

:func:`analyze_olcf_pipeline` ``(defn, *, context) -> Facts`` reads the activity DAG:

* the pipeline asset (``context.process`` or a ``pipeline`` asset keyed by ``olcf_item``) and one ``activity`` asset per activity, nested containers included
  (``if`` branches, ``switch`` cases, ``for_each`` / ``until`` bodies); ``CONTAINS`` edges pipeline -> activity (or container -> nested activity);
* ``DEPENDS_ON`` edges between activities from ``dependsOn`` (with the dependency conditions in ``attrs.conditions``); a dependency on an unknown activity is an
  ``unresolved_reference`` gap;
* ``EXECUTES`` edges activity -> item for ``notebook`` (``typeProperties.notebook``), ``spark_job`` (``sparkJob``) and ``child_pipeline`` (``pipeline``); the target is a
  literal item name or ``itm_`` id; an expression gives a ``dynamic_reference`` gap unless it is a parameter reference with a default (then the edge is ``inferred``);
* ``copy`` activities: ``READS_FROM`` the source and ``WRITES_TO`` the sink (lakehouse table or ``Files/<path>``), and for table to table copies with a known source schema
  the by-name column mapping as ``TRANSFORMS`` edges;
* ``sql`` activities: the SQL text is analysed by the SQL analyzer (DuckDB or Spark dialect per ``engine``) with the activity as the process and the activity's
  ``lakehouse`` as default lakehouse; ``@{pipeline().parameters.x}`` interpolations use the parameter default (``inferred``), other expressions become dynamic references;
* ``script`` activities with ``language: python``: the code is analysed by the PySpark analyzer; shell scripts are reported as ``unsupported_construct``;
* ``http`` / ``webhook_wait`` activities with a literal URL: ``READS_FROM`` an ``external`` asset named by the URL without query string and credentials.

``wait``, ``set_variable`` and ``fail`` activities only produce their asset. Evidence locations are ``{"path": "activities[2].typeProperties.sink", "activity": name}``.
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlsplit

from olcf.intelligence.lineage.base import (
    PIPELINE_OLCF,
    FactBuilder,
    LineageContext,
    ctx_for_lakehouse,
)
from olcf.intelligence.lineage.pipeline_common import (
    Node,
    Template,
    as_dict,
    emit_structure,
    item_ref,
    resolve_template,
    walk,
)
from olcf.intelligence.lineage.pyspark import PyAnalyzer
from olcf.intelligence.lineage.relation import Dataset, Relation
from olcf.intelligence.lineage.sql import UNKNOWN_MARK, SqlSession
from olcf.intelligence.model import AssetFact, AssetRef, Facts, clip

_ITEM_ACTIVITIES = {"notebook": ("notebook", "notebook"), "spark_job": ("spark_job", "sparkJob"), "child_pipeline": ("pipeline", "pipeline")}


def _tp(node: Node) -> dict[str, Any]:
    return as_dict(node.raw.get("typeProperties"))


def analyze_olcf_pipeline(defn: dict[str, Any], *, context: LineageContext | None = None) -> Facts:
    """Analyse an ``olcf.pipeline/1`` definition and return its lineage as :class:`~olcf.intelligence.model.Facts`; never raises."""
    ctx = LineageContext.coerce(context)
    b = FactBuilder(PIPELINE_OLCF, ctx)
    try:
        _analyze(defn, ctx, b)
    except Exception as exc:  # noqa: BLE001 - analyzers never raise
        b.gap("parse_error", "pipeline", f"pipeline analysis failed unexpectedly ({type(exc).__name__}: {str(exc)[:160]})", "Report the definition to the OLCF maintainers.", ctx.loc(), PIPELINE_OLCF)
    return b.facts()


def _analyze(defn: Any, ctx: LineageContext, b: FactBuilder) -> None:
    if not isinstance(defn, dict):
        b.gap("parse_error", "pipeline", "the pipeline definition is not a JSON object", "Provide an olcf.pipeline/1 document.", ctx.loc(), PIPELINE_OLCF)
        return
    schema = defn.get("schema")
    if isinstance(schema, str) and not schema.startswith("olcf.pipeline/"):
        b.gap("parse_error", "pipeline", f"unknown pipeline schema '{clip(schema, 60)}' (expected olcf.pipeline/1)", None, ctx.loc(path="schema"), PIPELINE_OLCF)
    name = ctx.item_name or str(defn.get("name") or "pipeline")
    key = ctx.item_id or name
    pipe = ctx.process or AssetRef.make("pipeline", aliases={"olcf_item": ctx.item_id} if ctx.item_id else None, name=name, scope=ctx.scope())
    if ctx.process is None:
        b.asset(AssetFact(ref=pipe, name=name, attrs={"schema": schema if isinstance(schema, str) else "olcf.pipeline/1"}, description=str(defn.get("description") or "") or None,
                          workspace_id=ctx.workspace_id))
    params = as_dict(defn.get("parameters"))
    acts = defn.get("activities")
    if not isinstance(acts, list):
        b.gap("parse_error", "pipeline", "the pipeline has no 'activities' array", None, ctx.loc(path="activities"), PIPELINE_OLCF)
        return
    nodes: list[Node] = []
    problems: list[tuple[str, str, str]] = []
    walk(acts, "activities", None, nodes, problems, [0])
    for kind, subject, detail in problems:
        b.gap(kind, subject, detail, None, ctx.loc(path=subject if subject.startswith("activities") else "activities"), PIPELINE_OLCF)
    refs = emit_structure(b, ctx, PIPELINE_OLCF, pipe, key, name, nodes, lambda n: str(n.raw.get("type") or "unknown"))
    for n in nodes:
        t = str(n.raw.get("type") or "")
        try:
            if t in _ITEM_ACTIVITIES:
                _executes(b, ctx, n, refs[n.path], t, params)
            elif t == "copy":
                _copy(b, ctx, n, refs[n.path], params)
            elif t == "sql":
                _sql(b, ctx, n, refs[n.path], params)
            elif t == "script":
                _script(b, ctx, n, refs[n.path], params)
            elif t in ("http", "webhook_wait"):
                _http(b, ctx, n, refs[n.path], params)
        except Exception as exc:  # noqa: BLE001 - one broken activity must not hide the others
            b.gap("parse_error", n.name, f"activity '{n.name}' ({t}) could not be analysed ({type(exc).__name__}: {str(exc)[:120]})", None, ctx.loc(path=n.path, activity=n.name), PIPELINE_OLCF)


def _template(b: FactBuilder, ctx: LineageContext, n: Node, value: Any, params: dict[str, Any], subpath: str, what: str) -> Template | None:
    """Resolve a property; a value that cannot be known at all records a ``dynamic_reference`` gap and returns ``None``."""
    t = resolve_template(value, params)
    if not t.all_known and UNKNOWN_MARK in t.text and re.fullmatch(rf"(?:{re.escape(UNKNOWN_MARK)}\d+__)+", t.text):
        b.gap("dynamic_reference", clip(str(value), 100) or what, f"the {what} of activity '{n.name}' is an expression that is only known at run time", "Pass a literal or a parameter with a default value.",
              ctx.loc(path=f"{n.path}.{subpath}", activity=n.name), PIPELINE_OLCF)
        return None
    return t


def _executes(b: FactBuilder, ctx: LineageContext, n: Node, aref: AssetRef, t: str, params: dict[str, Any]) -> None:
    kind, key = _ITEM_ACTIVITIES[t]
    raw = _tp(n).get(key)
    if raw is None:
        b.gap("parse_error", n.name, f"activity '{n.name}' ({t}) has no typeProperties.{key}", None, ctx.loc(path=f"{n.path}.typeProperties", activity=n.name), PIPELINE_OLCF)
        return
    tpl = _template(b, ctx, n, raw, params, f"typeProperties.{key}", f"{kind} reference")
    if tpl is None:
        return
    if not tpl.all_known:
        b.gap("dynamic_reference", clip(str(raw), 100) or kind, f"the {kind} run by activity '{n.name}' is only partly known at analysis time", "Pass a literal name.",
              ctx.loc(path=f"{n.path}.typeProperties.{key}", activity=n.name), PIPELINE_OLCF)
        return
    known = getattr(ctx, "known_notebooks", {}) if kind == "notebook" else None
    target = item_ref(kind, tpl.text, ctx, known)
    b.edge(aref, target, "EXECUTES", confidence="inferred" if tpl.taint else "derived", kind="definition", location=ctx.loc(path=f"{n.path}.typeProperties.{key}", activity=n.name),
           snippet=f"{t}: {tpl.text}", analyzer=PIPELINE_OLCF)


def _site(session: SqlSession, aref: AssetRef, loc: dict[str, Any], text: str, taint: tuple[str, ...] = ()) -> None:
    session.begin_site(aref, loc, text, taint)


def _side(b: FactBuilder, ctx: LineageContext, n: Node, side: Any, which: str, params: dict[str, Any]) -> tuple[SqlSession, Dataset, str] | None:
    """Resolve one side of a copy activity to a dataset; returns ``(session, dataset, confidence)``."""
    path = f"{n.path}.typeProperties.{which}"
    loc = ctx.loc(path=path, activity=n.name)
    if not isinstance(side, dict):
        b.gap("parse_error", n.name, f"copy {which} of activity '{n.name}' is missing", None, loc, PIPELINE_OLCF)
        return None
    if not side.get("lakehouse"):
        b.gap("parse_error", n.name, f"copy {which} of activity '{n.name}' names no lakehouse (the copy activity requires one), so the dataset cannot be identified", "Add the lakehouse to the copy activity.", loc, PIPELINE_OLCF)
        return None
    lh = resolve_template(side.get("lakehouse"), params)
    if not lh.all_known:
        b.gap("dynamic_reference", clip(str(side.get("lakehouse")), 100) or "lakehouse", f"the lakehouse of the copy {which} of activity '{n.name}' is only known at run time", None, loc, PIPELINE_OLCF)
        return None
    actx = ctx_for_lakehouse(ctx, lh.text or None)
    session = SqlSession(actx, b, "spark", analyzer=PIPELINE_OLCF, emit_query_assets=False)
    session.evidence_kind = "definition"
    taint = lh.taint
    if side.get("table") is not None:
        tb = resolve_template(side.get("table"), params)
        if not tb.all_known:
            b.gap("dynamic_reference", clip(str(side.get("table")), 100) or "table", f"the table of the copy {which} of activity '{n.name}' is only known at run time",
                  "Pass a literal table name or a parameter with a default.", ctx.loc(path=path + ".table", activity=n.name), PIPELINE_OLCF)
            return None
        taint += tb.taint
        _site(session, AssetRef.make("activity"), loc, "")          # site for gaps raised while resolving; the caller sets the real one
        ds = session.table_dataset([p for p in tb.text.split(".") if p])
    elif side.get("path") is not None:
        pt = resolve_template(side.get("path"), params)
        if not pt.all_known:
            b.gap("dynamic_reference", clip(str(side.get("path")), 100) or "path", f"the path of the copy {which} of activity '{n.name}' is only known at run time",
                  "Pass a literal path or a parameter with a default.", ctx.loc(path=path + ".path", activity=n.name), PIPELINE_OLCF)
            return None
        taint += pt.taint
        rel = pt.text.strip("/")
        p = rel if re.match(r"^(Files|Tables)(/|$)", rel) else f"Files/{rel}"
        ds = session.file_dataset(p, str(side.get("format") or "") or None)
    else:
        b.gap("parse_error", n.name, f"copy {which} of activity '{n.name}' names neither a table nor a path", None, loc, PIPELINE_OLCF)
        return None
    return session, ds, "inferred" if taint else "derived"


def _copy(b: FactBuilder, ctx: LineageContext, n: Node, aref: AssetRef, params: dict[str, Any]) -> None:
    tp = _tp(n)
    src = _side(b, ctx, n, tp.get("source"), "source", params)
    dst = _side(b, ctx, n, tp.get("sink"), "sink", params)
    if src is not None:
        s_sess, s_ds, s_conf = src
        _site(s_sess, aref, ctx.loc(path=f"{n.path}.typeProperties.source", activity=n.name), f"copy source of {n.name}")
        s_sess.confidence_cap = "inferred" if s_conf == "inferred" else None
        s_sess.record_read(s_ds)
    if dst is not None:
        d_sess, d_ds, d_conf = dst
        sink = as_dict(tp.get("sink"))
        mode = str(sink.get("mode") or "append")
        _site(d_sess, aref, ctx.loc(path=f"{n.path}.typeProperties.sink", activity=n.name), f"copy sink of {n.name}")
        d_sess.confidence_cap = "inferred" if d_conf == "inferred" else None
        d_sess.record_write(d_ds, mode, {"format": str(sink.get("format") or ("delta" if d_ds.kind == "table" else ""))} if (sink.get("format") or d_ds.kind == "table") else None)
        if src is not None and src[1].kind == "table" and d_ds.kind == "table" and src[1].schema:
            d_sess.datasets[src[1].key] = src[1]
            rel = src[0].base_relation(src[1])
            names = list(src[1].schema)
            tgt_cols = d_ds.schema
            keep = [c for c in names if tgt_cols is None or c.lower() in {x.lower() for x in tgt_cols}]
            if keep:
                sub = Relation(tuple(c for c in rel.cols if c.name in keep), (), rel.datasets)
                d_sess.confidence_cap = "inferred" if (d_conf == "inferred" or src[2] == "inferred") else None
                d_sess.emit_columns(sub, d_ds, keep, mode=mode, note="copy activity maps columns by name")


def _sql(b: FactBuilder, ctx: LineageContext, n: Node, aref: AssetRef, params: dict[str, Any]) -> None:
    tp = _tp(n)
    text = tp.get("sql")
    if not isinstance(text, str) or not text.strip():
        b.gap("parse_error", n.name, f"sql activity '{n.name}' has no SQL text", None, ctx.loc(path=f"{n.path}.typeProperties.sql", activity=n.name), PIPELINE_OLCF)
        return
    lh = resolve_template(tp.get("lakehouse"), params) if tp.get("lakehouse") else Template("", True)
    engine = str(tp.get("engine") or "duckdb").lower()
    actx = ctx_for_lakehouse(ctx, lh.text or None)
    session = SqlSession(actx, b, "spark" if engine == "spark" else "duckdb", analyzer=PIPELINE_OLCF, emit_query_assets=False)
    session.evidence_kind = "definition"
    tpl = resolve_template(text, params)
    sql_text = tpl.text if (text.startswith("@") or "@{" in text) else text
    taint = (*tpl.taint, *lh.taint)
    session.taint = taint
    session.run(sql_text, process=aref, loc=ctx.loc(path=f"{n.path}.typeProperties.sql", activity=n.name), line_key="line")


def _script(b: FactBuilder, ctx: LineageContext, n: Node, aref: AssetRef, params: dict[str, Any]) -> None:
    tp = _tp(n)
    code = tp.get("code")
    lang = str(tp.get("language") or "python").lower()
    loc = ctx.loc(path=f"{n.path}.typeProperties.code", activity=n.name)
    if lang != "python":
        b.gap("unsupported_construct", n.name, f"script activity '{n.name}' is a {lang} script; shell scripts are not analysed for data dependencies", "Use a notebook or python script.", loc, PIPELINE_OLCF)
        return
    if not isinstance(code, str) or not code.strip():
        return
    if code.lstrip().startswith("@"):
        b.gap("dynamic_reference", n.name, f"the code of script activity '{n.name}' is an expression", None, loc, PIPELINE_OLCF)
        return
    from dataclasses import replace

    actx = replace(ctx)
    actx.location = {**ctx.location, "path": f"{n.path}.typeProperties.code", "activity": n.name}
    az = PyAnalyzer(actx, b, aref)
    az.run_cell(code, None)
    az.finish()


def _http(b: FactBuilder, ctx: LineageContext, n: Node, aref: AssetRef, params: dict[str, Any]) -> None:
    url = _tp(n).get("url")
    loc = ctx.loc(path=f"{n.path}.typeProperties.url", activity=n.name)
    if not isinstance(url, str) or not url:
        return
    tpl = resolve_template(url, params)
    if not tpl.all_known:
        b.gap("dynamic_reference", n.name, f"the URL called by activity '{n.name}' is only known at run time", None, loc, PIPELINE_OLCF)
        return
    parts = urlsplit(tpl.text)
    if not parts.scheme or not parts.hostname:
        return
    name = f"{parts.scheme}://{parts.hostname}{':' + str(parts.port) if parts.port else ''}{parts.path}"
    ext = AssetRef.make("external", name=name)
    b.edge(aref, ext, "READS_FROM", confidence="inferred" if tpl.taint else "derived", kind="definition", location=loc, snippet=f"{_tp(n).get('method', 'GET')} {name}",
           attrs={"dataset_kind": "external", "protocol": parts.scheme}, analyzer=PIPELINE_OLCF)


__all__ = ["analyze_olcf_pipeline"]
