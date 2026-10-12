# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Lineage of a Power BI / Fabric semantic model: its tables, columns and measures, what feeds them, and how measures depend on each other.

:func:`analyze_semantic_model` ``(parts, *, context)`` takes the definition parts of a model (``{path: text}``: TMDL files ``definition/**/*.tmdl`` or ``model.bim``; a parsed
``model.bim`` object is accepted too) and emits

* assets: the ``semantic_model``, one ``model_table`` per table, one ``column`` per column and one ``measure`` per measure (parent = table / model). With the Fabric workspace and item
  GUIDs in the context every object carries a ``pbi`` alias ``<workspace>/<dataset>[/<table>[/<column or measure>]]`` (see :func:`~olcf.intelligence.lineage.base.pbi_alias`), which
  is what the scanner analyzer and the report analyzer use too; without GUIDs the objects are identified by name and ``semantic_model`` / ``table`` scope keys;
* ``READS_FROM`` model table -> source dataset, for every dataset the partition query reads (Power Query M, analysed by :mod:`~olcf.intelligence.lineage.mcode`) and for the
  ``entityName`` / ``schemaName`` / ``expressionSource`` binding of a Direct Lake partition (evidence kind ``definition``);
* ``TRANSFORMS`` source column -> model column (``via`` = the model) where the column can be traced through the M steps, native SQL, or the Direct Lake binding; a column the M query
  cannot be traced for is reported in a gap, never guessed;
* ``TRANSFORMS`` model column -> measure and model column -> calculated column for every column a DAX expression refers to, ``TRANSFORMS`` measure -> calculated column,
  ``USES_MEASURE`` consumer measure -> used measure, and ``DEPENDS_ON`` measure / calculated column -> table for table references (``COUNTROWS(Sales)``);
* ``READS_FROM`` calculated table -> the tables its DAX expression refers to, and ``USES_MEASURE`` calculated table -> the measures it refers to.

Confidence: ``derived`` for everything read from the definition. ``inferred`` where a name had to be matched (a column without a ``sourceColumn`` is assumed to carry its source column's name, a bare ``[Name]`` in a measure that is also a column name of another table, a
bare column in a measure, a Direct Lake SQL endpoint id that is not mapped to a lakehouse, DAX that could not be tokenized completely).

Not interpreted: what a DAX function does with a column (``CALCULATE`` filter arguments count as references), calculation items (``SELECTEDMEASURE``), row-level security filters, field
parameters, DAX query views, relationships (kept as attributes of the model asset, they are not data flow), perspectives, the columns of calculated tables, the columns of file / web /
external sources, Power Query functions that change the column set in a way the M analyzer does not track (join, expand, pivot ...), query folding, and incremental refresh policies.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from olcf.intelligence.lineage.base import (
    MAX_JSON_BYTES,
    MAX_PARTS,
    MCODE,
    SEMANTIC_MODEL,
    FactBuilder,
    LineageContext,
    column_ref,
    facts_with_gap,
    pbi_alias,
)
from olcf.intelligence.lineage.dax import DaxRef, DaxRefs, extract_refs, tokenize
from olcf.intelligence.lineage.mcode import MResolver, MResult, MSource, analyze_m, entity_source
from olcf.intelligence.lineage.tmdl import (
    ColDef,
    ExprDef,
    MeasureDef,
    Model,
    PartitionDef,
    RelDef,
    TableDef,
    as_text,
    model_from_tmdl,
)
from olcf.intelligence.model import GAP_KINDS, AssetFact, AssetRef, Facts, clip, looks_like_guid

_MAX_LISTED = 8
_IGNORED_BARE = frozenset({"integer", "double", "string", "boolean", "currency", "datetime", "decimal", "int64", "variant", "text"})


# ---------------------------------------------------------------------------------------------------------------------- model.bim
def _d(v: Any) -> dict[str, Any]:
    return v if isinstance(v, dict) else {}


def _l(v: Any) -> list[Any]:
    return v if isinstance(v, list) else []


def _s(v: Any) -> str | None:
    return v if isinstance(v, str) and v else None


def model_from_bim(data: Any) -> Model:
    """Build a :class:`~olcf.intelligence.lineage.tmdl.Model` from a ``model.bim`` / TMSL document (a parsed object or its JSON text).

    Accepts the database object itself (``{"name", "compatibilityLevel", "model": {...}}``), the bare model object and TMSL scripts wrapped in ``createOrReplace``. Expressions may be
    strings or lists of lines. Shapes that are not understood are reported in ``Model.problems`` and skipped; the function never raises.
    """
    m = Model()
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except ValueError as exc:
            m.problems.append((f"model.bim is not valid JSON: {exc}", None))
            return m
    root = _d(data)
    if not root:
        m.problems.append(("model.bim does not contain a JSON object", None))
        return m
    root = _d(root.get("createOrReplace")) or root
    db = _d(root.get("database")) or root
    model = _d(db.get("model")) or db
    m.name = _s(db.get("name")) or _s(model.get("name"))
    level = db.get("compatibilityLevel", model.get("compatibilityLevel"))
    if isinstance(level, int) and not isinstance(level, bool):
        m.compatibility_level = level
    for i, t in enumerate(_l(model.get("tables"))):
        td = _d(t)
        name = _s(td.get("name"))
        if name is None:
            m.problems.append((f"tables[{i}] has no name", None))
            continue
        table = TableDef(name, description=_s(td.get("description")), is_hidden=td.get("isHidden") is True)
        for c in _l(td.get("columns")):
            cd = _d(c)
            cname = _s(cd.get("name"))
            ctype = str(cd.get("type") or "data").lower()
            if cname is None or ctype == "rownumber":
                continue
            expr = as_text(cd.get("expression")) if ctype == "calculated" else ""
            table.columns.append(ColDef(cname, source_column=_s(cd.get("sourceColumn")), expression=expr or None, data_type=_s(cd.get("dataType"))))
        for ms in _l(td.get("measures")):
            md = _d(ms)
            mname = _s(md.get("name"))
            if mname is not None:
                table.measures.append(MeasureDef(mname, as_text(md.get("expression"))))
        for p in _l(td.get("partitions")):
            pd = _d(p)
            src = _d(pd.get("source"))
            kind = str(src.get("type") or "m").lower()
            part = PartitionDef(_s(pd.get("name")) or name, kind, mode=_s(pd.get("mode")))
            if kind == "entity":
                part.entity, part.schema, part.expression_source = _s(src.get("entityName")), _s(src.get("schemaName")), _s(src.get("expressionSource"))
            else:
                part.expression = as_text(src.get("expression")) or as_text(src.get("query")) or None
            table.partitions.append(part)
        m.tables.append(table)
    for e in _l(model.get("expressions")):
        ed = _d(e)
        ename = _s(ed.get("name"))
        if ename is not None:
            m.expressions.append(ExprDef(ename, as_text(ed.get("expression")), str(ed.get("kind") or "m").lower()))
    for r in _l(model.get("relationships")):
        rd = _d(r)
        ft, fc, tt, tc = _s(rd.get("fromTable")), _s(rd.get("fromColumn")), _s(rd.get("toTable")), _s(rd.get("toColumn"))
        if ft and fc and tt and tc:
            m.relationships.append(RelDef(ft, fc, tt, tc, rd.get("isActive") is not False))
    return m


def load_model(parts: Any) -> tuple[Model | None, str]:
    """Pick the model out of the definition parts: TMDL when any ``*.tmdl`` part exists, else ``model.bim`` (``*.bim`` / ``model.json``). Returns ``(model, "tmdl" | "bim")`` or
    ``(None, reason)``."""
    if isinstance(parts, str):
        if len(parts) > MAX_JSON_BYTES:
            return None, f"the definition is larger than {MAX_JSON_BYTES} bytes"
        try:
            parts = json.loads(parts)
        except ValueError:
            return None, "the definition is neither a set of parts nor JSON"
    if not isinstance(parts, Mapping):
        return None, "the definition is not a mapping of part paths to text"
    named = {str(k): v for k, v in parts.items()}
    over = len(named) > MAX_PARTS
    if over:
        named = dict(sorted(named.items())[:MAX_PARTS])
    tmdl = {k: v for k, v in named.items() if k.lower().endswith(".tmdl")}
    if tmdl:
        model = model_from_tmdl(tmdl)
        if over:
            model.problems.append((f"the definition has more than {MAX_PARTS} parts; the rest were not read", None))
        return model, "tmdl"
    bim = sorted(k for k in named if k.lower().endswith((".bim", "model.json")))
    if bim:
        model = model_from_bim(named[bim[0]])
        if over:
            model.problems.append((f"the definition has more than {MAX_PARTS} parts; the rest were not read", None))
        return model, "bim"
    if "tables" in named or "model" in named or "createOrReplace" in named:
        return model_from_bim(named), "bim"
    return None, "no model.bim or .tmdl part (parts: " + ", ".join(sorted(named)[:6]) + ")"


# ---------------------------------------------------------------------------------------------------------------------- analyzer
class _ModelAnalyzer:
    def __init__(self, model: Model, ctx: LineageContext, fmt: str) -> None:
        self.m = model
        self.ctx = ctx
        self.fmt = fmt
        self.b = FactBuilder(SEMANTIC_MODEL, ctx)
        item = ctx.item_id if ctx.item_id and looks_like_guid(ctx.item_id) else None
        ws = ctx.fabric_workspace if ctx.fabric_workspace and looks_like_guid(ctx.fabric_workspace) else None
        self.ids = (ws, item) if ws and item else None
        self.key = (ctx.item_name or model.name or "model")
        self.model_ref = self._make_model_ref()
        self.tables: dict[str, TableDef] = {t.name.lower(): t for t in model.tables}
        self.measures: dict[str, list[tuple[TableDef, MeasureDef]]] = {}
        self.columns: dict[str, list[tuple[TableDef, ColDef]]] = {}
        for t in model.tables:
            for ms in t.measures:
                self.measures.setdefault(ms.name.lower(), []).append((t, ms))
            for c in t.columns:
                self.columns.setdefault(c.name.lower(), []).append((t, c))
        self.resolver = MResolver(self._expression_text)

    # -- references
    def _make_model_ref(self) -> AssetRef:
        proc = self.ctx.process
        aliases: dict[str, str] = dict(proc.alias_map) if proc is not None else {}
        if self.ids is not None:
            aliases["pbi"] = pbi_alias(self.ids[0], self.ids[1])
        if proc is not None:
            return AssetRef.make(proc.type, aliases, proc.name, proc.scope_map)
        return AssetRef.make("semantic_model", aliases or None, self.key, self.ctx.scope())

    def table_ref(self, t: TableDef) -> AssetRef:
        """Reference to a model table."""
        if self.ids is not None:
            return AssetRef.make("model_table", {"pbi": pbi_alias(*self.ids, t.name)}, t.name, {"semantic_model": self.key})
        return AssetRef.make("model_table", None, t.name, {"semantic_model": self.key})

    def obj_ref(self, kind: str, t: TableDef, name: str) -> AssetRef:
        """Reference to a column (``kind == "column"``) or measure of table ``t``."""
        if self.ids is not None:
            return AssetRef.make(kind, {"pbi": pbi_alias(*self.ids, t.name, name)}, name, {"semantic_model": self.key, "table": t.name.lower()})
        return AssetRef.make(kind, None, name, {"semantic_model": self.key, "table": t.name.lower()})

    def loc(self, t: TableDef | None = None, **parts: Any) -> dict[str, Any]:
        """Evidence location of an object: the part file, table and any ``parts`` (``measure``, ``column``, ``partition``, ``line``)."""
        out = self.ctx.loc(**{k: v for k, v in parts.items() if v})
        if t is not None:
            out["table"] = t.name
            if t.part:
                out["part"] = t.part
        return out

    def _expression_text(self, name: str) -> str | None:
        e = self.m.expression(name)
        return e.expression if e is not None and e.expression.strip() else None

    # -- driver
    def run(self) -> Facts:
        """Emit assets, edges and gaps for the whole model."""
        m = self.m
        for msg, line in m.problems:
            self.b.gap("parse_error", msg.split(":")[0][:80] or "model", msg, "Fix the syntax of the definition part, or re-export the semantic model.", self.ctx.loc(line=line))
        self._assets()
        for t in m.tables:
            for p in t.partitions:
                self._partition(t, p)
        for t in m.tables:
            for c in t.columns:
                if c.expression:
                    self._dax(t, "column", c.name, c.expression, c.line)
            for ms in t.measures:
                self._dax(t, "measure", ms.name, ms.expression, ms.line)
        self._relationships()
        return self.b.facts()

    def _assets(self) -> None:
        m = self.m
        attrs: dict[str, Any] = {"definition_format": self.fmt, "table_count": len(m.tables), "measure_count": sum(len(t.measures) for t in m.tables),
                                 "relationship_count": len(m.relationships)}
        if m.compatibility_level is not None:
            attrs["compatibility_level"] = m.compatibility_level
        if m.relationships:
            attrs["relationships"] = [f"{r.from_table}[{r.from_column}] -> {r.to_table}[{r.to_column}]" + ("" if r.active else " (inactive)") for r in m.relationships[:200]]
        name = self.ctx.item_name or m.name or "semantic model"
        self.b.asset(AssetFact(ref=self.model_ref, name=name, attrs=attrs, workspace_id=self.ctx.workspace_id))
        for t in m.tables:
            tref = self.table_ref(t)
            modes = sorted({p.mode or "import" for p in t.partitions if p.kind != "calculated"})
            kinds = sorted({p.kind for p in t.partitions})
            tattrs: dict[str, Any] = {"column_count": len(t.columns), "measure_count": len(t.measures), "hidden": t.is_hidden, "partition_kinds": kinds, "storage_modes": modes}
            self.b.asset(AssetFact(ref=tref, name=t.name, attrs=tattrs, parent=self.model_ref, description=t.description, workspace_id=self.ctx.workspace_id, path=f"{name} / {t.name}"))
            self.b.edge(self.model_ref, tref, "DEPENDS_ON", confidence="derived", kind="definition", location=self.loc(t), snippet=f"table {t.name}")
            for c in t.columns:
                cattrs: dict[str, Any] = {"calculated": bool(c.expression), "source_column": c.source_column, "data_type": c.data_type}
                if c.expression:
                    cattrs["expression"] = clip(c.expression, 2000)
                self.b.asset(AssetFact(ref=self.obj_ref("column", t, c.name), name=c.name, attrs={k: v for k, v in cattrs.items() if v is not None}, parent=tref,
                                       workspace_id=self.ctx.workspace_id, path=f"{name} / {t.name} / {c.name}"))
            for ms in t.measures:
                self.b.asset(AssetFact(ref=self.obj_ref("measure", t, ms.name), name=ms.name, attrs={"expression": clip(ms.expression, 2000)}, parent=tref,
                                       workspace_id=self.ctx.workspace_id, path=f"{name} / {t.name} / {ms.name}"))

    # -- partitions
    def _partition(self, t: TableDef, p: PartitionDef) -> None:
        loc = self.loc(t, partition=p.name, line=p.line)
        if p.kind == "m":
            self._m_partition(t, p, loc)
        elif p.kind == "entity":
            self._entity_partition(t, p, loc)
        elif p.kind == "calculated":
            self._calculated_table(t, p, loc)
        else:
            self.b.gap("unsupported_construct", f"{t.name}/{p.name}", f"partition source type '{p.kind}' is not analysed; the table's source is unknown",
                       "Use an M (Power Query) or Direct Lake partition, or add the lineage manually.", loc)

    def _report_m(self, r: MResult, subject: str, loc: dict[str, Any]) -> None:
        for msg in r.problems:
            self.b.gap("parse_error", subject, msg, "Check the Power Query expression for syntax errors.", loc, MCODE)
        for msg in r.dynamic:
            self.b.gap("dynamic_reference", subject, msg, "Use a literal, or a parameter with a default value, for the source location.", loc, MCODE)
        for msg in r.unsupported:
            self.b.gap("unsupported_construct", subject, msg, "Column-level lineage stops here; the tables read are still reported.", loc, MCODE)
        for kind, subj, detail in r.sql_gaps:
            self.b.gap(kind if kind in GAP_KINDS else "unsupported_construct", subj or subject, detail, None, loc, MCODE)

    def _read(self, t: TableDef, src: MSource, loc: dict[str, Any], snippet: str, kind: str) -> str:
        conf = "inferred" if src.approx else "derived"
        self.b.edge(self.table_ref(t), src.ref, "READS_FROM", confidence=conf, kind=kind, location=loc, snippet=snippet)
        if src.approx:
            self.b.gap("unresolved_reference", src.label, f"{src.approx}; the dependency is recorded with lower confidence", "Use literal connection details and table names, or parameters with default values.", loc)
        return conf

    def _m_partition(self, t: TableDef, p: PartitionDef, loc: dict[str, Any]) -> None:
        text = p.expression or ""
        subject = f"{t.name}/{p.name}"
        if not text.strip():
            self.b.gap("not_inspected", subject, "the partition has no source expression", None, loc)
            return
        r = analyze_m(text, ctx=self.ctx, resolver=self.resolver)
        self._report_m(r, subject, loc)
        for src in r.sources:
            self._read(t, src, loc, text, "static")
        if not r.sources and not (r.problems or r.dynamic or r.unsupported) and not (r.reason or "").startswith("inline table") and not (r.table and r.table.known):
            self.b.gap("unresolved_reference", subject, f"no data source that can be identified is read by the query of '{t.name}'" + (f" ({r.reason})" if r.reason else ""),
                       "Use a connector this analyzer understands (SQL, Lakehouse, OneLake / Azure storage, files, web), or add the lineage manually.", loc, MCODE)
        if not any(s.kind == "table" for s in r.sources):
            if r.sources and any(not c.expression for c in t.columns):
                kinds = ", ".join(sorted({s.kind for s in r.sources}))
                self.b.gap("not_inspected", subject, f"the columns of '{t.name}' are not traced to source columns because its source is not a lakehouse or warehouse table ({kinds})",
                           "Table-level lineage is available.", {**loc, "informational": True}, MCODE)
            return
        unmapped: list[str] = []
        for c in t.columns:
            if c.expression:
                continue
            origins = r.origins(c.source_column or c.name)
            if origins is None:
                unmapped.append(c.name)
                continue
            for o in origins:
                if o.src.kind != "table":
                    continue
                self.b.edge(column_ref(o.src.ref, o.col), self.obj_ref("column", t, c.name), "TRANSFORMS",
                            confidence="inferred" if o.src.approx or c.source_column is None else "derived", kind="static",
                            location={**loc, "column": c.name}, snippet=text, transformation=clip(o.text, 400), via=self.model_ref)
        if unmapped:
            why = r.reason
            shown = ", ".join(unmapped[:_MAX_LISTED]) + (f" and {len(unmapped) - _MAX_LISTED} more" if len(unmapped) > _MAX_LISTED else "")
            self.b.gap("unsupported_construct" if why else "unresolved_reference", subject,
                       f"column(s) {shown} of '{t.name}' could not be traced to a source column" + (f": {why}" if why else " (not in the query output)"),
                       "Column-level lineage needs a query whose steps keep column names traceable.", loc, MCODE)

    def _entity_partition(self, t: TableDef, p: PartitionDef, loc: dict[str, Any]) -> None:
        subject = f"{t.name}/{p.name}"
        if not p.entity:
            self.b.gap("unresolved_reference", subject, "the Direct Lake partition has no entityName", None, loc)
            return
        expr = self.m.expression(p.expression_source) if p.expression_source else None
        if expr is None:
            self.b.gap("unresolved_reference", subject, f"the Direct Lake partition's expressionSource '{p.expression_source or ''}' is not an expression of the model",
                       "Check the expressionSource name in the table definition.", loc)
            return
        r = analyze_m(expr.expression, ctx=self.ctx, resolver=self.resolver)
        self._report_m(r, f"{p.expression_source}", self.loc(None, expression=expr.name, line=expr.line, part=expr.part))
        src = entity_source(r.connection, p.schema, p.entity, ctx=self.ctx) if r.connection is not None else None
        if src is None:
            self.b.gap("unsupported_construct", subject, f"expression '{expr.name}' does not resolve to a Fabric SQL endpoint or OneLake location, so the Direct Lake source of '{p.entity}' is unknown",
                       "Direct Lake sources are read from Sql.Database(<endpoint>, <lakehouse>) or AzureStorage.DataLake(<OneLake url>) expressions.", loc)
            return
        snippet = f"entity {p.schema + '.' if p.schema else ''}{p.entity} via {expr.name}"
        conf = self._read(t, src, loc, snippet, "definition")
        for c in t.columns:
            if c.expression:
                continue
            self.b.edge(column_ref(src.ref, c.source_column or c.name), self.obj_ref("column", t, c.name), "TRANSFORMS",
                        confidence="inferred" if c.source_column is None else conf, kind="definition",
                        location={**loc, "column": c.name}, snippet=snippet, via=self.model_ref)

    def _calculated_table(self, t: TableDef, p: PartitionDef, loc: dict[str, Any]) -> None:
        text = p.expression or ""
        if not text.strip():
            self.b.gap("not_inspected", f"{t.name}/{p.name}", "the calculated table has no expression", None, loc)
            return
        refs = extract_refs(text)
        for msg in refs.problems:
            self.b.gap("parse_error", f"{t.name}/{p.name}", f"DAX: {msg}", None, loc, MCODE)
        used: dict[str, TableDef] = {}
        measures: dict[tuple[str, str], tuple[TableDef, str, str]] = {}
        for res in self._resolve(refs, t, text, "table", loc, f"{t.name}/{p.name}"):
            if res[0] == "measure":
                measures.setdefault((res[1].name.lower(), res[2].lower()), (res[1], res[2], res[3]))
            elif res[1].name.lower() != t.name.lower():
                used.setdefault(res[1].name.lower(), res[1])
        for mt, mname, mconf in measures.values():
            self.b.edge(self.table_ref(t), self.obj_ref("measure", mt, mname), "USES_MEASURE", confidence=mconf, kind="static", location=loc, snippet=text)
        for other in used.values():
            self.b.edge(self.table_ref(t), self.table_ref(other), "READS_FROM", confidence="inferred" if refs.problems else "derived", kind="static", location=loc, snippet=text)
        if t.columns:
            self.b.gap("not_inspected", t.name, f"the columns of calculated table '{t.name}' are not mapped to the columns they are computed from", "Table-level lineage is available.",
                       {**loc, "informational": True})

    # -- DAX
    def _resolve(self, refs: DaxRefs, home: TableDef, expr: str, owner: str, loc: dict[str, Any], subject: str) -> list[tuple[str, TableDef, str, str]]:
        """Resolve DAX references against the model: ``(kind, table, name, confidence)`` with kind ``column`` | ``measure`` | ``table``; unresolved references become gaps.

        ``owner`` is ``column`` (calculated column: a bare ``[x]`` can be a column of the home table), ``measure`` or ``table``.
        """
        out: list[tuple[str, TableDef, str, str]] = []
        locals_ = {tok.value.lower() for tok in tokenize(expr)[0] if tok.kind == "str"}
        mentioned = {r.table.lower() for r in refs.refs if r.table and r.kind in ("qualified", "table") and r.table.lower() in self.tables}
        base_conf = "inferred" if refs.problems else "derived"
        for r in refs.refs:
            got = self._resolve_one(r, home, owner, mentioned, locals_, refs, base_conf)
            if got is None:
                clash = self.columns.get((r.name or "").lower(), []) if r.kind == "unqualified" else []
                if len(clash) > 1:
                    self.b.gap("unresolved_reference", r.name or subject,
                               f"'{self._ref_text(r)}' in {subject} could be a column of any of the tables {', '.join(sorted(t.name for t, _ in clash))}; it is not attributed to one of them",
                               "Qualify the column with its table name.", loc)
                    continue
                self.b.gap("unresolved_reference", f"{r.table + '.' if r.table else ''}{r.name or ''}".strip(".") or subject,
                           f"'{self._ref_text(r)}' in {subject} does not match any table, column or measure of the model", "Check the name, or whether the model definition is complete.", loc)
                continue
            out.extend(got)
        return out

    @staticmethod
    def _ref_text(r: DaxRef) -> str:
        if r.kind == "table":
            return str(r.table)
        return f"{r.table}[{r.name}]" if r.table else f"[{r.name}]"

    def _resolve_one(self, r: DaxRef, home: TableDef, owner: str, mentioned: set[str], locals_: set[str], refs: DaxRefs,
                     base_conf: str) -> list[tuple[str, TableDef, str, str]] | None:
        if r.kind == "table":
            t = self.tables.get((r.table or "").lower())
            if t is not None:
                return [("table", t, t.name, base_conf)]
            low = (r.table or "").lower()
            return [] if low in _IGNORED_BARE or low in refs.variables else None
        name = (r.name or "").lower()
        if r.kind == "qualified":
            if (r.table or "").lower() in refs.variables:
                return []
            t = self.tables.get((r.table or "").lower())
            if t is None:
                return None
            for c in t.columns:
                if c.name.lower() == name:
                    return [("column", t, c.name, base_conf)]
            for ms in t.measures:
                if ms.name.lower() == name:
                    return [("measure", t, ms.name, base_conf)]
            return None
        # unqualified [name]
        measure_hits = self.measures.get(name, [])
        column_hits = self.columns.get(name, [])
        if measure_hits:
            mt, md = measure_hits[0]
            clash = bool(column_hits) and owner != "column"
            return [("measure", mt, md.name, "inferred" if clash or len(measure_hits) > 1 else base_conf)]
        if owner == "column":
            for c in home.columns:
                if c.name.lower() == name:
                    return [("column", home, c.name, base_conf)]
        cands = [(t, c) for t, c in column_hits]
        if len(cands) > 1:
            narrowed = [(t, c) for t, c in cands if t.name.lower() in mentioned]
            cands = narrowed if narrowed else cands
        if len(cands) == 1:
            t, c = cands[0]
            return [("column", t, c.name, "inferred")]
        if name in locals_:
            return []
        return None

    def _dax(self, t: TableDef, kind: str, name: str, expr: str, line: int) -> None:
        """Emit the edges of one calculated column or measure."""
        if not expr.strip():
            return
        loc = self.loc(t, line=line, **{kind: name})
        refs = extract_refs(expr)
        subject = f"{t.name}[{name}]"
        for msg in refs.problems:
            self.b.gap("parse_error", subject, f"DAX: {msg}", "Fix the DAX expression; references found before the problem were recorded with lower confidence.", loc, MCODE)
        me = self.obj_ref(kind, t, name)
        table_deps: set[str] = set()
        for what, rt, rname, conf in self._resolve(refs, t, expr, kind, loc, subject):
            if what == "column":
                src = self.obj_ref("column", rt, rname)
                if kind == "measure" and rt.name.lower() not in table_deps:
                    # table-level lineage: a measure depends on the tables whose columns it aggregates (column level has the TRANSFORMS edges)
                    table_deps.add(rt.name.lower())
                    self.b.edge(me, self.table_ref(rt), "DEPENDS_ON", confidence=conf, kind="static", location=loc, snippet=expr)
                if src == me:
                    continue
                self.b.edge(src, me, "TRANSFORMS", confidence=conf, kind="static", location=loc, snippet=expr, transformation=clip(expr, 400), via=self.model_ref)
            elif what == "measure":
                src = self.obj_ref("measure", rt, rname)
                if src == me:
                    continue
                if kind == "measure":
                    self.b.edge(me, src, "USES_MEASURE", confidence=conf, kind="static", location=loc, snippet=expr)
                else:
                    self.b.edge(src, me, "TRANSFORMS", confidence=conf, kind="static", location=loc, snippet=expr, transformation=clip(expr, 400), via=self.model_ref)
            elif rt.name.lower() not in table_deps:
                table_deps.add(rt.name.lower())
                self.b.edge(me, self.table_ref(rt), "DEPENDS_ON", confidence=conf, kind="static", location=loc, snippet=expr)

    # -- relationships
    def _relationships(self) -> None:
        for r in self.m.relationships:
            for tname, cname in ((r.from_table, r.from_column), (r.to_table, r.to_column)):
                t = self.tables.get(tname.lower())
                if t is None or all(c.name.lower() != cname.lower() for c in t.columns):
                    self.b.gap("unresolved_reference", f"{tname}[{cname}]", f"relationship refers to {tname}[{cname}], which is not in the model", None,
                               {**self.ctx.loc(), "informational": True})


def analyze_model(model: Model, *, context: Any = None, fmt: str = "model") -> Facts:
    """Emit the facts of an already parsed :class:`~olcf.intelligence.lineage.tmdl.Model` (used by the scanner analyzer, which builds a model from scanner metadata)."""
    return _ModelAnalyzer(model, LineageContext.coerce(context), fmt).run()


def analyze_semantic_model(parts: Any, *, context: Any = None) -> Facts:
    """Analyse a semantic model definition (TMDL parts or ``model.bim``), see the module docstring. ``context`` is a :class:`LineageContext`, its dict form, or ``None``.

    Never raises: unreadable input gives a ``parse_error`` / ``not_inspected`` gap.
    """
    ctx = LineageContext.coerce(context)
    model, fmt = load_model(parts)
    if model is None:
        return facts_with_gap(SEMANTIC_MODEL, "parse_error" if fmt.startswith("the definition is") else "not_inspected", ctx.item_name or "semantic model",
                              f"the semantic model was not analysed: {fmt}", "Export the definition as TMDL or model.bim.", ctx.loc())
    if not model.tables and model.problems:
        facts = facts_with_gap(SEMANTIC_MODEL, "parse_error", ctx.item_name or "semantic model", model.problems[0][0], "Check that the definition is valid TMDL / model.bim.", ctx.loc())
        return facts
    return _ModelAnalyzer(model, ctx, fmt).run()


__all__ = ["analyze_model", "analyze_semantic_model", "load_model", "model_from_bim"]
