# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""SQL lineage extraction with sqlglot.

Given SQL text this module answers: *which tables does it read, which table does it write, and which source columns
feed which target column?*  It is pure (no database, no storage) so it is cheap to unit-test; the catalog service maps
the table references it returns onto lakehouse datasets.

Honesty rules (the reason this module is careful rather than clever):

* **Table-level** lineage is derived from the parse tree and is reliable for every statement kind we understand
  (SELECT, INSERT, CREATE TABLE/VIEW AS, MERGE, UPDATE, DELETE).
* **Column-level** lineage is reported only where it can be *proven*.  Each output column is traced through CTEs,
  sub-queries, joins and unions with :func:`sqlglot.lineage.lineage`; if a column cannot be resolved (``SELECT *`` over a
  table whose schema is unknown, ambiguous unqualified columns, unsupported constructs) it is marked unresolved and the
  statement status becomes ``partial`` (some columns traced) or ``unknown`` (none).  We never guess.
* A column computed from constants or ``COUNT(*)`` has no source columns; that is a *complete* statement about it
  (``transform = constant | aggregate``), not a gap.

Table references are rewritten to flat canonical names (``t0``, ``t1`` ...) before tracing so that sqlglot sees one
consistent schema regardless of how the author spelled ``lakehouse.table`` / ``schema.table``.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError
from sqlglot.lineage import lineage as sg_lineage

DIALECTS = {"duckdb": "duckdb", "spark": "spark", "sparksql": "spark", "": "duckdb"}


@dataclass(frozen=True)
class TableRef:
    """A table (or file) as written in the SQL text."""

    parts: tuple[str, ...]
    kind: str = "table"           # table | file
    uri: str | None = None        # for file sources such as read_parquet('...')

    @property
    def dotted(self) -> str:
        return self.uri if self.kind == "file" and self.uri else ".".join(self.parts)

    def lowered(self) -> tuple[str, ...]:
        return tuple(p.lower() for p in self.parts)


@dataclass
class ColumnLineage:
    """How one target column is produced."""

    target: str
    sources: list[tuple[int, str]]      # (index into SqlLineage.sources, source column)
    transform: str                      # direct | expression | aggregate | constant | unresolved
    resolved: bool = True


@dataclass
class SqlLineage:
    """Lineage of one SQL statement."""

    statement: str                                  # select | insert | ctas | create_view | merge | update | delete | other
    sources: list[TableRef] = field(default_factory=list)
    targets: list[TableRef] = field(default_factory=list)
    columns: list[ColumnLineage] = field(default_factory=list)
    status: str = "not_applicable"                  # complete | partial | unknown | not_applicable
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "statement": self.statement, "status": self.status, "notes": self.notes,
            "sources": [{"name": s.dotted, "kind": s.kind} for s in self.sources],
            "targets": [{"name": t.dotted, "kind": t.kind} for t in self.targets],
            "columns": [{"target": c.target, "transform": c.transform, "resolved": c.resolved,
                         "sources": [{"source": self.sources[i].dotted, "column": col} for i, col in c.sources]} for c in self.columns],
        }


SchemaFor = Callable[[TableRef], "dict[str, str] | None"]


def _dialect(name: str | None) -> str:
    return DIALECTS.get((name or "").lower(), name or "duckdb")


def _table_ref(t: exp.Table) -> TableRef | None:
    """Convert a Table node to a :class:`TableRef` (``None`` for things that are not data sources)."""
    this = t.this
    if isinstance(this, exp.Func) and not isinstance(this, exp.Table):
        # table function such as read_parquet('path') / read_csv('a.csv') / delta_scan('...')
        lit = this.find(exp.Literal)
        if lit is not None and lit.is_string:
            return TableRef(parts=(this.sql_name().lower(),), kind="file", uri=lit.this)
        return None
    if not t.name:
        return None
    parts = tuple(p.name for p in (t.args.get("catalog"), t.args.get("db"), t.this) if isinstance(p, exp.Identifier) and p.name)
    return TableRef(parts=parts) if parts else None


class _Collector:
    """Finds source tables below an expression and rewrites them to canonical names."""

    def __init__(self) -> None:
        self.refs: list[TableRef] = []
        self._index: dict[tuple[Any, ...], int] = {}

    def index_of(self, ref: TableRef) -> int:
        key = (ref.kind, ref.lowered() if ref.kind == "table" else ref.uri)
        if key not in self._index:
            self._index[key] = len(self.refs)
            self.refs.append(ref)
        return self._index[key]

    def rewrite(self, root: exp.Expression, *, skip: set[int] | None = None) -> exp.Expression:
        """Return a copy of ``root`` whose real tables are renamed ``t<i>`` (aliased to their original name)."""
        root = root.copy()
        cte_names = {c.alias.lower() for c in root.find_all(exp.CTE) if c.alias}
        for t in list(root.find_all(exp.Table)):
            if skip and id(t) in skip:
                continue
            if not t.args.get("db") and not t.args.get("catalog") and isinstance(t.this, exp.Identifier) and t.name.lower() in cte_names:
                continue  # reference to a CTE, not a base table
            ref = _table_ref(t)
            if ref is None:
                continue
            i = self.index_of(ref)
            alias = t.alias or (ref.parts[-1] if ref.kind == "table" else "src")
            t.replace(exp.Table(this=exp.to_identifier(f"t{i}"), alias=exp.TableAlias(this=exp.to_identifier(alias))))
        return root


def _canon_schema(refs: list[TableRef], schema_for: SchemaFor | None) -> dict[str, dict[str, str]] | None:
    if schema_for is None:
        return None
    out: dict[str, dict[str, str]] = {}
    for i, r in enumerate(refs):
        cols = schema_for(r) if r.kind == "table" else None
        if cols:
            out[f"t{i}"] = {k.lower(): v for k, v in cols.items()}
    return out or None


def _leaves(node: Any) -> list[tuple[int, str]] | None:
    """Collect ``(canonical table index, column)`` leaves; ``None`` when a leaf is not a plain table column."""
    out: list[tuple[int, str]] = []
    for n in node.walk():
        if isinstance(n.expression, exp.Table):
            name = n.expression.name.lower()
            if not (name.startswith("t") and name[1:].isdigit()):
                return None
            col = n.name.rsplit(".", 1)[-1]
            if col in ("*", ""):
                return None
            out.append((int(name[1:]), col))
    return out


def _select_expr(select: exp.Expression, name: str) -> exp.Expression | None:
    for s in getattr(select, "selects", []):
        if s.alias_or_name.lower() == name.lower():
            return s
    return None


def _transform_of(select: exp.Expression, name: str) -> str:
    target = _select_expr(select, name)
    if target is None:
        return "expression"
    inner = target.unalias()
    if isinstance(inner, exp.Column):
        return "direct"
    if inner.find(exp.AggFunc) is not None:
        return "aggregate"
    if inner.find(exp.Column) is None:
        return "constant"
    return "expression"


def _first_select(q: exp.Expression) -> exp.Expression:
    while isinstance(q, exp.Subquery):
        q = q.this
    node: exp.Expression = q
    while isinstance(node, exp.SetOperation):
        node = node.left
    return node


def _qualified(query: exp.Expression, schema: dict[str, dict[str, str]] | None, dialect: str) -> exp.Expression:
    """Expand stars (when the schema is known), alias anonymous outputs (``_col_0``) and qualify columns; best effort."""
    from sqlglot.optimizer.qualify import qualify

    try:
        return qualify(query.copy(), schema=schema, dialect=dialect, validate_qualify_columns=False, identify=False)
    except Exception:  # noqa: BLE001 - qualification is an optimisation; fall back to the raw query
        return query


def _trace_columns(query: exp.Expression, schema: dict[str, dict[str, str]] | None, dialect: str,
                   target_names: list[str] | None) -> tuple[list[ColumnLineage], list[str]]:
    notes: list[str] = []
    q = _qualified(query, schema, dialect)
    first = _first_select(q)
    names = [n or f"_col_{i}" for i, n in enumerate(getattr(q, "named_selects", None) or getattr(first, "named_selects", []))]
    if not names or "*" in names:
        notes.append("SELECT * over a table with unknown schema: column lineage is unknown")
        return [], notes
    cols: list[ColumnLineage] = []
    for i, n in enumerate(names):
        tgt = target_names[i] if target_names and i < len(target_names) else n
        try:
            node = sg_lineage(n, q, schema=schema, dialect=dialect)
            leaves = _leaves(node)
            if leaves is None:
                raise SqlglotError("non-table leaf")
            transform = _transform_of(first, n)
            sel = _select_expr(first, n)
            if not leaves and sel is not None and sel.find(exp.Column) is not None:
                raise SqlglotError("column could not be attributed to a source table (ambiguous or unknown schema)")
            cols.append(ColumnLineage(tgt, sorted(set(leaves)), transform))
        except (SqlglotError, KeyError, ValueError, AttributeError, IndexError) as exc:
            cols.append(ColumnLineage(tgt, [], "unresolved", resolved=False))
            notes.append(f"column '{n}' could not be traced: {str(exc).splitlines()[0][:120]}")
    return cols, notes


def _status(cols: list[ColumnLineage], starred_unknown: bool) -> str:
    if not cols:
        return "unknown"
    ok = sum(1 for c in cols if c.resolved)
    if ok == len(cols) and not starred_unknown:
        return "complete"
    return "partial" if ok else "unknown"


def _target_ref(node: exp.Expression | None) -> tuple[TableRef | None, list[str]]:
    """Target table of INSERT/CREATE/MERGE/UPDATE/DELETE plus an explicit column list, if given."""
    cols: list[str] = []
    if isinstance(node, exp.Schema):
        cols = [c.name for c in node.expressions if getattr(c, "name", "")]
        node = node.this
    if isinstance(node, exp.Table):
        return _table_ref(node), cols
    return None, cols


def _analyze_query(stmt: exp.Expression, query: exp.Expression, kind: str, dialect: str, schema_for: SchemaFor | None,
                   target: TableRef | None, target_cols: list[str]) -> SqlLineage:
    col = _Collector()
    rewritten = col.rewrite(query)
    result = SqlLineage(statement=kind, sources=col.refs, targets=[target] if target else [])
    if target is None:
        return result  # plain SELECT: table-level only
    schema = _canon_schema(col.refs, schema_for)
    tnames = target_cols or None
    if kind == "insert" and not target_cols and schema_for is not None and target is not None:
        known = schema_for(target)
        if known:
            tnames = list(known)  # positional mapping onto the target table's declared columns
        else:
            result.notes.append("INSERT without a column list into a table with unknown schema: target column names are taken from the SELECT")
    cols, notes = _trace_columns(rewritten, schema, dialect, tnames)
    result.columns, result.notes = cols, result.notes + notes
    star_unknown = any("SELECT *" in n for n in notes)
    result.status = _status(cols, star_unknown)
    return result


def _analyze_dml(stmt: exp.Expression, kind: str, dialect: str, schema_for: SchemaFor | None) -> SqlLineage:
    """MERGE / UPDATE / DELETE: table-level always; column-level only for ``SET target.col = <source columns>``."""
    target, _ = _target_ref(stmt.args.get("this"))
    tnode = stmt.args.get("this")
    skip = {id(tnode)} if isinstance(tnode, exp.Table) else set()
    col = _Collector()
    scan: list[exp.Expression] = []
    if isinstance(stmt, exp.Merge):
        scan = [x for x in (stmt.args.get("using"), stmt.args.get("on")) if isinstance(x, exp.Expression)]
    else:
        scan = [x for k, x in stmt.args.items() if k in ("from_", "from", "where", "using") and isinstance(x, exp.Expression)]
    aliases: dict[str, int] = {}
    for part in scan:
        for t in part.find_all(exp.Table):
            ref = _table_ref(t)
            if ref is None or id(t) in skip:
                continue
            idx = col.index_of(ref)
            aliases[(t.alias or ref.parts[-1]).lower()] = idx
    res = SqlLineage(statement=kind, sources=col.refs, targets=[target] if target else [])
    tgt_alias = (tnode.alias if isinstance(tnode, exp.Table) else "") or (target.parts[-1] if target else "")
    assigns: list[tuple[str, exp.Expression]] = []
    for eq in stmt.find_all(exp.EQ):
        left = eq.left
        if isinstance(left, exp.Column) and (not left.table or left.table.lower() == tgt_alias.lower()) and _inside_set(eq):
            assigns.append((left.name, eq.right))
    if isinstance(stmt, exp.Merge):
        for w in stmt.args.get("whens", []) or []:
            then = w.args.get("then")
            if isinstance(then, exp.Insert):
                names = [c.name for c in (then.this.expressions if isinstance(then.this, exp.Tuple) else [])]
                vals = list(then.expression.expressions) if isinstance(then.expression, exp.Tuple) else []
                assigns.extend(zip(names, vals, strict=False))
    if kind == "delete" or not assigns:
        if kind != "delete":
            res.notes.append("no column assignments found: only table-level lineage is available")
            res.status = "unknown"
        return res
    seen: dict[str, ColumnLineage] = {}
    for name, rhs in assigns:
        srcs: list[tuple[int, str]] = []
        ok = True
        for c in rhs.find_all(exp.Column):
            q = (c.table or "").lower()
            if q in aliases:
                srcs.append((aliases[q], c.name))
            elif q == tgt_alias.lower() or not q:
                ok = ok and bool(q)  # unqualified refs are ambiguous between target and source
            else:
                ok = False
        transform = "direct" if isinstance(rhs, exp.Column) else ("constant" if rhs.find(exp.Column) is None else "expression")
        seen[name] = ColumnLineage(name, sorted(set(srcs)), transform if ok else "unresolved", resolved=ok)
    res.columns = list(seen.values())
    res.status = _status(res.columns, False)
    if res.status != "complete":
        res.notes.append("some assignments reference unqualified or non-table columns and could not be traced")
    return res


def _inside_set(eq: exp.Expression) -> bool:
    p = eq.parent
    while p is not None:
        if isinstance(p, (exp.Update, exp.Merge, exp.Insert)):
            return True
        p = p.parent
    return False


def analyze(sql: str, *, dialect: str | None = "duckdb", schema_for: SchemaFor | None = None) -> list[SqlLineage]:
    """Analyse every statement in ``sql``; unparseable SQL raises ``ValueError`` with the parser's message."""
    d = _dialect(dialect)
    try:
        stmts = [s for s in sqlglot.parse(sql, read=d) if s is not None]
    except SqlglotError as exc:
        raise ValueError(f"cannot parse SQL: {str(exc).splitlines()[0]}") from exc
    out: list[SqlLineage] = []
    for stmt in stmts:
        out.append(_analyze_statement(stmt, d, schema_for))
    return out


def _analyze_statement(stmt: exp.Expression, dialect: str, schema_for: SchemaFor | None) -> SqlLineage:
    if isinstance(stmt, exp.Insert):
        target, tcols = _target_ref(stmt.args.get("this"))
        query = stmt.args.get("expression")
        if query is None or isinstance(query, exp.Values) or not isinstance(query, exp.Query):
            return SqlLineage("insert", targets=[target] if target else [], notes=["INSERT ... VALUES has no upstream datasets"], status="not_applicable")
        # WITH clauses sit on the insert in some dialects: carry them over to the query
        with_ = stmt.args.get("with_") or stmt.args.get("with")
        if with_ is not None and isinstance(query, exp.Query) and not query.args.get("with_") and not query.args.get("with"):
            query = query.copy()
            query.set("with_", with_.copy()) if "with_" in query.arg_types else query.set("with", with_.copy())
        return _analyze_query(stmt, query, "insert", dialect, schema_for, target, tcols)
    if isinstance(stmt, exp.Create):
        kind = str(stmt.args.get("kind") or "").upper()
        target, tcols = _target_ref(stmt.args.get("this"))
        query = stmt.args.get("expression")
        if isinstance(query, exp.Query):
            return _analyze_query(stmt, query, "create_view" if kind == "VIEW" else "ctas", dialect, schema_for, target, tcols)
        return SqlLineage("other", targets=[target] if target else [], notes=["DDL without a query has no upstream datasets"])
    if isinstance(stmt, exp.Merge):
        return _analyze_dml(stmt, "merge", dialect, schema_for)
    if isinstance(stmt, exp.Update):
        return _analyze_dml(stmt, "update", dialect, schema_for)
    if isinstance(stmt, exp.Delete):
        return _analyze_dml(stmt, "delete", dialect, schema_for)
    if isinstance(stmt, exp.Query):
        return _analyze_query(stmt, stmt, "select", dialect, schema_for, None, [])
    return SqlLineage("other", notes=[f"{type(stmt).__name__} statements carry no lineage"])
