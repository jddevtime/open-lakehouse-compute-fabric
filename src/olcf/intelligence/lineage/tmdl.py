# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Parser for TMDL (Tabular Model Definition Language) and the model dataclasses shared with the ``model.bim`` reader.

TMDL is an indentation-based text format: ``keyword name [= expression]`` declares an object, ``property: value`` sets a property, children are indented one level deeper,
expressions (DAX for measures / calculated columns / calculated tables, Power Query M for partitions and shared expressions) may continue on following lines indented two levels
deeper than the object (one level deeper than a property that carries it) or sit between triple back-tick fences. :func:`parse_objects` builds a generic object tree and
:func:`model_from_tmdl` turns the parts of a model definition (``definition/model.tmdl``, ``definition/tables/*.tmdl``, ``definition/expressions.tmdl`` ...) into a :class:`Model`.

What is read: tables (columns incl. calculated ones with ``sourceColumn`` / expression, measures, partitions of kind ``m`` / ``entity`` / ``calculated`` with ``mode``,
``entityName``, ``schemaName``, ``expressionSource``), shared ``expression`` objects (including parameters), relationships (``fromColumn`` / ``toColumn``) and the model name.
Everything else (perspectives, cultures, roles, annotations, hierarchies, calculation items, data sources) is parsed but ignored. Names quoted with ``'...'`` are unquoted (``''`` is
an escaped quote); ``///`` description lines and ``//`` comment lines outside expressions are skipped. Malformed input never raises: lines that cannot be understood are reported in
``Model.problems`` with their line number.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

MAX_TMDL_BYTES = 8_000_000
_OBJECT_KEYWORDS = frozenset({
    "database", "createorreplace", "model", "table", "column", "measure", "partition", "hierarchy", "level", "relationship", "expression", "culture", "perspective", "role", "tablepermission", "annotation",
    "calculationgroup", "calculationitem", "datasource", "function", "querygroup", "linguisticmetadata", "ref", "extendedproperty", "variation", "calendar", "translation",
    "perspectivetable", "perspectivecolumn", "perspectivemeasure", "perspectivehierarchy", "member", "source", "alternateof", "refreshpolicy", "detailrowsdefinition",
    "formatstringdefinition", "kpi", "objecttranslation", "namedexpression", "set",
})
_EXPR_PARTITION_KINDS = frozenset({"m", "calculated", "query"})


@dataclass
class TObj:
    """A TMDL object: ``kind`` (``table``, ``measure`` ...), ``name`` (unquoted), the ``=`` value on the declaration line (``expr``), ``props`` (property name -> raw value or
    expression text), ``children`` and the 1-based ``line`` of the declaration."""

    kind: str
    name: str
    expr: str | None
    props: dict[str, str]
    children: list[TObj]
    line: int


@dataclass
class ColDef:
    """A model column: ``expression`` is set for calculated columns, ``source_column`` for data columns."""

    name: str
    source_column: str | None = None
    expression: str | None = None
    data_type: str | None = None
    line: int = 0


@dataclass
class MeasureDef:
    """A measure and its DAX ``expression``."""

    name: str
    expression: str
    line: int = 0


@dataclass
class PartitionDef:
    """A table partition. ``kind`` is ``m`` | ``entity`` | ``calculated`` | ``query`` | other; ``expression`` holds the M or DAX text; ``entity`` / ``schema`` /
    ``expression_source`` are the Direct Lake binding (``mode`` is then ``directLake``)."""

    name: str
    kind: str
    mode: str | None = None
    expression: str | None = None
    entity: str | None = None
    schema: str | None = None
    expression_source: str | None = None
    line: int = 0


@dataclass
class TableDef:
    """A model table with its columns, measures and partitions (``part`` is the path of the file that declares it)."""

    name: str
    columns: list[ColDef] = field(default_factory=list)
    measures: list[MeasureDef] = field(default_factory=list)
    partitions: list[PartitionDef] = field(default_factory=list)
    description: str | None = None
    is_hidden: bool = False
    line: int = 0
    part: str | None = None


@dataclass
class ExprDef:
    """A shared (named) expression: a Power Query M query or parameter."""

    name: str
    expression: str
    kind: str = "m"
    line: int = 0
    part: str | None = None


@dataclass
class RelDef:
    """A relationship ``from_table[from_column] -> to_table[to_column]``."""

    from_table: str
    from_column: str
    to_table: str
    to_column: str
    active: bool = True


@dataclass
class Model:
    """A semantic model reduced to what lineage needs."""

    name: str | None = None
    tables: list[TableDef] = field(default_factory=list)
    expressions: list[ExprDef] = field(default_factory=list)
    relationships: list[RelDef] = field(default_factory=list)
    compatibility_level: int | None = None
    problems: list[tuple[str, int | None]] = field(default_factory=list)       # (message, line)

    def table(self, name: str) -> TableDef | None:
        """Table by case-insensitive name."""
        low = name.lower()
        return next((t for t in self.tables if t.name.lower() == low), None)

    def expression(self, name: str) -> ExprDef | None:
        """Shared expression by case-insensitive name."""
        low = name.lower()
        return next((e for e in self.expressions if e.name.lower() == low), None)


# ---------------------------------------------------------------------------------------------------------------------- names
def unquote(s: str) -> str:
    """Remove ``'...'`` quoting (``''`` is an escaped quote) from a TMDL name or value."""
    s = s.strip()
    if len(s) >= 2 and s[0] == "'" and s[-1] == "'":
        return s[1:-1].replace("''", "'")
    return s


def _read_name(rest: str) -> tuple[str, str]:
    """Split ``'Quoted name' = expr`` / ``name = expr`` / ``name`` into ``(unquoted name, remainder)``."""
    rest = rest.lstrip()
    if rest.startswith("'"):
        i = 1
        while i < len(rest):
            if rest[i] == "'":
                if i + 1 < len(rest) and rest[i + 1] == "'":
                    i += 2
                    continue
                return rest[1:i].replace("''", "'"), rest[i + 1:]
            i += 1
        return rest[1:], ""
    m = re.match(r"^([^\s=]+)(.*)$", rest, re.DOTALL)
    if m:
        return m.group(1), m.group(2)
    return "", rest


# ---------------------------------------------------------------------------------------------------------------------- tree parser
def _indent_unit(lines: list[str]) -> int:
    units = [len(ln) - len(ln.lstrip(" ")) for ln in lines if ln.strip() and ln.startswith(" ") and not ln.startswith("\t")]
    return min(units) if units else 4


def _level(line: str, unit: int) -> int:
    tabs = 0
    spaces = 0
    for ch in line:
        if ch == "\t":
            tabs += 1
        elif ch == " ":
            spaces += 1
        else:
            break
    return tabs + (spaces // unit if unit else 0)


def parse_objects(text: str) -> tuple[list[TObj], list[tuple[str, int | None]]]:
    """Parse TMDL text into a tree of :class:`TObj`. Returns ``(top-level objects, problems)``."""
    problems: list[tuple[str, int | None]] = []
    if len(text.encode("utf-8", "ignore")) > MAX_TMDL_BYTES:
        return [], [(f"TMDL text larger than {MAX_TMDL_BYTES} bytes was not parsed", None)]
    lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    unit = _indent_unit(lines)
    roots: list[TObj] = []
    stack: list[tuple[int, TObj]] = []        # (level, object)
    i, n = 0, len(lines)
    while i < n:
        raw = lines[i]
        stripped = raw.strip()
        lineno = i + 1
        if not stripped or stripped.startswith("///") or (stripped.startswith("//") and not stack):
            i += 1
            continue
        lvl = _level(raw, unit)
        while stack and stack[-1][0] >= lvl:
            stack.pop()
        parent = stack[-1][1] if stack else None
        if stripped.startswith("//"):
            i += 1
            continue
        prop = re.match(r"^([A-Za-z_][\w.]*)[ \t]*:(?!=)[ \t]?(.*)$", stripped)
        prop_expr = re.match(r"^([A-Za-z_]\w*)[ \t]*=(.*)$", stripped)
        obj_m = re.match(r"^([A-Za-z_]\w*)\b(.*)$", stripped)
        if (prop or prop_expr) and parent is None:
            problems.append((f"property outside any object: {stripped[:60]}", lineno))
            i += 1
            continue
        if prop is not None and parent is not None:
            value = prop.group(2).strip()
            consumed = 1
            if value == "" or value.startswith("```"):
                body, consumed = _expression_body(lines, i, lvl + 1, value, unit)
                if body is not None:
                    value = body
            parent.props[prop.group(1)] = value if value.startswith("```") or "\n" in value else unquote(value)
            i += consumed
            continue
        if prop_expr is not None and parent is not None:
            inline = prop_expr.group(2).strip()
            consumed = 1
            if inline == "" or inline.startswith("```"):
                body, consumed = _expression_body(lines, i, lvl + 1, inline, unit)
                value = body if body is not None else ""
            else:
                extra, consumed = _continuation(lines, i, lvl + 1, unit)
                value = inline + ("\n" + extra if extra else "")
            parent.props[prop_expr.group(1)] = value
            i += consumed
            continue
        if obj_m is not None and obj_m.group(1).lower() in _OBJECT_KEYWORDS:
            kind = obj_m.group(1).lower()
            if kind == "ref":
                i += 1
                continue
            name, remainder = _read_name(obj_m.group(2))
            expr: str | None = None
            consumed = 1
            rem = remainder.strip()
            if rem.startswith("="):
                inline = rem[1:].strip()
                if kind == "partition":
                    expr = inline          # the partition type (m / entity / calculated ...)
                elif inline == "" or inline.startswith("```"):
                    body, consumed = _expression_body(lines, i, lvl + 2, inline, unit)
                    expr = body if body is not None else ""
                else:
                    expr = inline
                    extra, consumed = _continuation(lines, i, lvl + 2, unit)
                    if extra:
                        expr = inline + "\n" + extra
            node = TObj(kind, name, expr, {}, [], lineno)
            if parent is not None:
                parent.children.append(node)
            else:
                roots.append(node)
            stack.append((lvl, node))
            i += consumed
            continue
        problems.append((f"line not understood: {stripped[:60]}", lineno))
        i += 1
    return roots, problems


def _continuation(lines: list[str], i: int, min_level: int, unit: int) -> tuple[str, int]:
    """Lines after ``lines[i]`` that continue an expression: indented at least ``min_level``. Returns ``(text, lines consumed including line i)``."""
    out: list[str] = []
    j = i + 1
    pending_blank: list[str] = []
    while j < len(lines):
        ln = lines[j]
        if not ln.strip():
            pending_blank.append("")
            j += 1
            continue
        if _level(ln, unit) >= min_level:
            out.extend(pending_blank)
            pending_blank = []
            out.append(ln)
            j += 1
        else:
            break
    return _dedent(out), j - i - len(pending_blank)


def _expression_body(lines: list[str], i: int, min_level: int, inline: str, unit: int) -> tuple[str | None, int]:
    """Body of a multi-line expression declared on ``lines[i]``: a fenced block (```), or following lines indented at least ``min_level``."""
    j = i + 1
    if inline.startswith("```"):
        first = inline[3:]
        body = [first] if first.strip() else []
        while j < len(lines):
            if lines[j].strip().startswith("```"):
                return _dedent(body), j - i + 1
            body.append(lines[j])
            j += 1
        return _dedent(body), j - i
    # fence on the next non-blank line?
    k = j
    while k < len(lines) and not lines[k].strip():
        k += 1
    if k < len(lines) and lines[k].strip().startswith("```"):
        body = []
        m = k + 1
        while m < len(lines):
            if lines[m].strip().startswith("```"):
                return _dedent(body), m - i + 1
            body.append(lines[m])
            m += 1
        return _dedent(body), m - i
    text, consumed = _continuation(lines, i, min_level, unit)
    return (text if text.strip() else None), consumed


def _dedent(lines: list[str]) -> str:
    nonblank = [ln for ln in lines if ln.strip()]
    if not nonblank:
        return ""
    strip = min(len(ln) - len(ln.lstrip("\t ")) for ln in nonblank)
    return "\n".join(ln[strip:] if ln.strip() else "" for ln in lines).strip("\n")


# ---------------------------------------------------------------------------------------------------------------------- model
def _bool(v: str | None) -> bool:
    return str(v).strip().lower() == "true"


def _table_from_obj(o: TObj, model: Model) -> TableDef:
    t = TableDef(o.name, description=o.props.get("description"), is_hidden=_bool(o.props.get("isHidden")), line=o.line)
    for c in o.children:
        if c.kind == "column":
            t.columns.append(ColDef(c.name, source_column=unquote(c.props["sourceColumn"]) if "sourceColumn" in c.props else None, expression=c.expr or c.props.get("expression") or None,
                                    data_type=c.props.get("dataType"), line=c.line))
        elif c.kind == "measure":
            t.measures.append(MeasureDef(c.name, c.expr if c.expr is not None else c.props.get("expression", ""), c.line))
        elif c.kind == "partition":
            kind = (c.expr or "m").strip().lower()
            part = PartitionDef(c.name, kind, mode=c.props.get("mode"), line=c.line)
            src_obj = next((x for x in c.children if x.kind == "source"), None)
            if src_obj is not None:
                part.entity = unquote(src_obj.props.get("entityName", "")) or None
                part.schema = unquote(src_obj.props.get("schemaName", "")) or None
                part.expression_source = unquote(src_obj.props.get("expressionSource", "")) or None
                if src_obj.expr:
                    part.expression = src_obj.expr
            if "source" in c.props:
                part.expression = c.props["source"]
            if kind == "entity" and part.entity is None and "entityName" in c.props:
                part.entity = unquote(c.props["entityName"])
                part.schema = unquote(c.props.get("schemaName", "")) or None
                part.expression_source = unquote(c.props.get("expressionSource", "")) or None
            t.partitions.append(part)
    return t


def model_from_objects(roots: list[TObj], model: Model | None = None) -> Model:
    """Add the tables, expressions and relationships of parsed TMDL objects to ``model`` (a new one by default)."""
    m = model or Model()
    for o in roots:
        if o.kind == "model":
            m.name = m.name or o.name or None
            lvl = o.props.get("compatibilityLevel")
            if lvl and lvl.isdigit():
                m.compatibility_level = int(lvl)
            for c in o.children:                      # TMDL files may nest tables below the model object
                if c.kind == "table":
                    m.tables.append(_table_from_obj(c, m))
                elif c.kind == "expression":
                    m.expressions.append(ExprDef(c.name, c.expr or c.props.get("expression", ""), "m", c.line))
        elif o.kind == "table":
            existing = m.table(o.name)
            if existing is not None:
                new = _table_from_obj(o, m)
                existing.columns += new.columns
                existing.measures += new.measures
                existing.partitions += new.partitions
            else:
                m.tables.append(_table_from_obj(o, m))
        elif o.kind == "expression":
            m.expressions.append(ExprDef(o.name, o.expr or o.props.get("expression", ""), "m", o.line))
        elif o.kind == "relationship":
            fr, to = o.props.get("fromColumn", ""), o.props.get("toColumn", "")
            ft, fc = _split_column_ref(fr)
            tt, tc = _split_column_ref(to)
            if ft and tt:
                m.relationships.append(RelDef(ft, fc, tt, tc, _bool(o.props.get("isActive", "true"))))
    return m


def _split_column_ref(s: str) -> tuple[str, str]:
    """``Sales.OrderDate`` / ``'Sales Table'.'Order Date'`` -> ``(table, column)``."""
    s = s.strip()
    if s.startswith("'"):
        name, rest = _read_name(s)
        rest = rest.lstrip()
        return name, unquote(rest[1:]) if rest.startswith(".") else ""
    if "." in s:
        t, _, c = s.partition(".")
        return t, unquote(c)
    return "", ""


def model_from_tmdl(parts: dict[str, str]) -> Model:
    """Build a :class:`Model` from TMDL parts (``{path: text}``), in a deterministic order (sorted paths, ``model.tmdl`` first)."""
    model = Model()
    for path in sorted(parts, key=lambda p: (not p.lower().endswith("model.tmdl"), p.lower())):
        if not path.lower().endswith(".tmdl"):
            continue
        text = parts[path]
        if not isinstance(text, str):
            model.problems.append((f"{path}: part is not text", None))
            continue
        roots, problems = parse_objects(text)
        model.problems += [(f"{path}: {msg}", ln) for msg, ln in problems]
        n_tables, n_exprs = len(model.tables), len(model.expressions)
        model_from_objects(roots, model)
        for t in model.tables[n_tables:]:
            t.part = path
        for e in model.expressions[n_exprs:]:
            e.part = path
    return model


def as_text(v: Any) -> str:
    """``v`` as text: a string, or a list of strings joined by newlines (``model.bim`` stores expressions both ways)."""
    if isinstance(v, str):
        return v
    if isinstance(v, list):
        return "\n".join(str(x) for x in v)
    return ""
