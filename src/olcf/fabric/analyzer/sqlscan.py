# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""SQL analysis for ``%%sql`` cells and ``spark.sql("...")`` literals (sqlglot, Spark dialect).

Two jobs:

* **detect** Fabric-specific SQL (V-Order, T-SQL-only statements) and collect the tables a statement reads/writes (logical data references);
* **rewrite** deterministically what has an exact open-source equivalent: ``OPTIMIZE t [...] VORDER`` loses the Fabric-only ``VORDER`` clause.

Parsing never blocks a notebook: Spark SQL has many dialect extensions and templated SQL, so a statement sqlglot cannot parse produces a
``FAB-SQL-PARSE`` warning and Spark remains the authority at run time.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

from olcf.runtime_lib.magics import split_statements

VORDER_RE = re.compile(r"(?is)(\bOPTIMIZE\b(?:(?!;).)*?)(\bVORDER\b)")
_VORDER_PROP = re.compile(r"(?i)vorder")
_SET_LINE = re.compile(r"(?i)\s*SET\s")
_TSQL_ONLY = re.compile(r"(?is)^\s*(?:COPY\s+INTO\b|.*\bOPENROWSET\s*\()")
_WRITE_KINDS = (exp.Insert, exp.Merge, exp.Update, exp.Delete, exp.Create, exp.Drop, exp.Alter, exp.TruncateTable)
_DELTA_COMMAND = re.compile(r"(?is)^\s*(OPTIMIZE|VACUUM|RESTORE|DESCRIBE\s+HISTORY|DESCRIBE\s+DETAIL|CONVERT\s+TO\s+DELTA|GENERATE)\b")


@dataclass
class SqlFinding:
    """A construct found in SQL text: the rule that matched, a 1-based line, the construct and detail."""
    rule: str
    line: int  # 1-based, relative to the SQL text
    construct: str
    detail: str = ""


@dataclass
class SqlEdit:
    """A text replacement the SQL scanner proposes: absolute start and end offsets, the replacement, the rule and the original text."""
    start: int  # absolute offsets in the SQL text
    end: int
    replacement: str
    rule: str
    before: str
    after: str


@dataclass
class SqlTableRef:
    """A table the SQL reads or writes: the reference as written, whether it is a catalog table or a path, the role (read or write) and the line.
    """
    ref: str  # as written (db.table, catalog.db.table, delta.`path`)
    kind: str  # catalog | path
    role: str  # read | write
    line: int


@dataclass
class SqlResult:
    """Result of scanning SQL text: findings, proposed edits, table references and statement counts."""
    findings: list[SqlFinding] = field(default_factory=list)
    edits: list[SqlEdit] = field(default_factory=list)
    tables: list[SqlTableRef] = field(default_factory=list)
    statements: int = 0
    delta_statements: int = 0
    configs: list[tuple[str, int]] = field(default_factory=list)  # SET key=value statements


def _line_of(text: str, offset: int) -> int:
    return text.count("\n", 0, max(0, offset)) + 1


def analyse_sql(text: str, *, base_line: int = 1) -> SqlResult:
    """Analyse ``text`` (one or more ``;``-separated statements). Lines in the result are ``base_line``-relative (1-based)."""
    res = SqlResult()
    for m in VORDER_RE.finditer(text):
        s, e = m.start(2), m.end(2)
        res.edits.append(SqlEdit(s, e, "", "FAB-SQL-VORDER", text[s:e], ""))
        res.findings.append(SqlFinding("FAB-SQL-VORDER", base_line + _line_of(text, s) - 1, "OPTIMIZE ... VORDER"))
    for m in _VORDER_PROP.finditer(text):
        if any(ed.start <= m.start() < ed.end for ed in res.edits):
            continue
        if _SET_LINE.match(text[text.rfind("\n", 0, m.start()) + 1:]):
            continue  # SET spark.sql.parquet.vorder.*=...: classified as a Spark setting, not as a table property
        res.findings.append(SqlFinding("FAB-SQL-VORDER-PROP", base_line + _line_of(text, m.start()) - 1, "V-Order property or setting"))
    for stmt in split_statements(text):
        res.statements += 1
        off = text.find(stmt)
        line = base_line + _line_of(text, off if off >= 0 else 0) - 1
        if _TSQL_ONLY.match(stmt):
            res.findings.append(SqlFinding("FAB-SQL-FABRIC-FN", line, stmt.strip().split("\n", 1)[0][:80]))
            continue
        sm = re.match(r"(?is)^\s*SET\s+([A-Za-z0-9_.\-]+)\s*=", stmt)
        if sm:
            res.configs.append((sm.group(1), line))
            continue
        if _DELTA_COMMAND.match(stmt):
            res.delta_statements += 1
            tm = re.search(r"(?is)\b(?:OPTIMIZE|VACUUM|RESTORE(?:\s+TABLE)?|DESCRIBE\s+(?:HISTORY|DETAIL)|CONVERT\s+TO\s+DELTA)\s+(?:TABLE\s+)?(\S+)", stmt)
            if tm:
                ref = tm.group(1).strip("();")
                kind = "path" if "`" in ref and re.match(r"(?i)(delta|parquet)\.", ref) else "catalog"
                role = "read" if re.match(r"(?is)^\s*DESCRIBE", stmt) else "write"
                res.tables.append(SqlTableRef(ref.replace("`", "") if kind == "path" else ref, kind, role, line))
            continue
        try:
            tree = sqlglot.parse_one(stmt, read="spark")
        except (SqlglotError, ValueError, RecursionError) as exc:
            res.findings.append(SqlFinding("FAB-SQL-PARSE", line, stmt.strip().split("\n", 1)[0][:80], str(exc).split("\n", 1)[0][:200]))
            continue
        if tree is None:
            continue
        if isinstance(tree, exp.Merge):
            res.delta_statements += 1
        _collect_tables(tree, line, res)
    return res


def _collect_tables(tree: exp.Expression, line: int, res: SqlResult) -> None:
    ctes = {c.alias_or_name.lower() for c in tree.find_all(exp.CTE)}
    target: exp.Expression | None = None
    if isinstance(tree, _WRITE_KINDS):
        t = tree.this
        if isinstance(t, exp.Schema):
            t = t.this
        target = t if isinstance(t, exp.Table) else None
    for tbl in tree.find_all(exp.Table):
        name = tbl.name
        if not name or (not tbl.args.get("db") and name.lower() in ctes):
            continue
        parts = [p.name for p in (tbl.args.get("catalog"), tbl.args.get("db")) if p is not None and p.name] + [name]
        ref = ".".join(parts)
        is_path = parts[0].lower() in ("delta", "parquet", "csv", "json", "orc", "text", "binaryfile") and len(parts) == 2
        res.tables.append(SqlTableRef(parts[1] if is_path else ref, "path" if is_path else "catalog", "write" if tbl is target else "read", line))
