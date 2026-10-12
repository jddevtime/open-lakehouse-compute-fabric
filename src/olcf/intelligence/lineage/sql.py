# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""SQL lineage: statements in, table-level and column-level lineage facts out (Spark SQL, DuckDB and T-SQL through sqlglot).

Public entry point: :func:`analyze_sql` ``(sql, *, dialect, context) -> Facts``. The same engine (:class:`SqlSession`) is used by the PySpark and notebook analyzers
(``spark.sql(...)``, ``%%sql`` cells, ``selectExpr``) and by the report analyzers, so every SQL string in the platform is understood the same way.

What is emitted
---------------
For every statement a *process* asset does the work: the ``context.process`` asset when the caller supplied one (a notebook, a pipeline activity, a saved query),
otherwise a ``sql_query`` asset keyed by the ``query`` alias ``sha1:<hash of the normalised statement>``. Edges:

* ``READS_FROM`` process -> every base table / view / file the statement reads (tables inside CTEs, sub-queries, joins, lateral views and ``IN (SELECT ..)`` included);
* ``WRITES_TO`` process -> the target of ``INSERT``, ``INSERT OVERWRITE``, ``CREATE TABLE .. AS``, ``CREATE VIEW``, ``MERGE``, ``UPDATE``, ``DELETE``, ``SELECT .. INTO``, ``COPY`` (``attrs.mode`` says how);
* ``TRANSFORMS`` source column -> target column (``via`` = process, ``transformation`` = the expression text; for values computed through CTEs / sub-queries the chain
  ``outer <- inner`` is recorded, direct copies carry the column expression). Unions merge the origins of every branch, ``USING`` joins take both sides, window
  functions include their ``PARTITION BY`` / ``ORDER BY`` columns, ``CASE`` / aggregates / casts / UDF calls include every referenced column.

Honesty rules (the reason wrong lineage is avoided rather than guessed)
-----------------------------------------------------------------------
* ``SELECT *`` is expanded only when the columns are known (``context.schemas``, an earlier ``CREATE TABLE`` / CTAS in the same script, a CTE / sub-query /
  temp view with a known select list). Otherwise the statement keeps its table-level edges and a gap ``unsupported_construct`` "SELECT * without known schema" is recorded.
* An unqualified column that could belong to several tables whose schemas are unknown is **not** attributed (gap ``unresolved_reference``).
* ``INSERT INTO t SELECT ..`` without a column list into a table of unknown schema maps by select-list names with confidence ``inferred``; with a known schema it maps
  by position; when the select list is not fully known nothing is mapped.
* Dynamic SQL (``EXEC``, ``IDENTIFIER(..)`` with a non-literal) is reported as a gap, never guessed.

Supported / not supported
-------------------------
Supported: SELECT with joins (``ON`` / ``USING`` / ``NATURAL`` when schemas are known), CTEs (also chained and recursive), derived tables, scalar / ``EXISTS`` / ``IN`` sub-queries
(correlated too), ``UNION [ALL]`` / ``INTERSECT`` / ``EXCEPT``, ``LATERAL VIEW explode``, ``VALUES``, window functions with named windows, ``QUALIFY``, lambda functions, ``SELECT * EXCEPT / EXCLUDE / REPLACE``,
``INSERT [OVERWRITE] [TABLE] .. [(cols)] SELECT | VALUES``, ``CREATE [OR REPLACE] [TEMP] TABLE|VIEW .. AS``, ``CREATE TABLE .. USING DELTA AS``, ``CREATE TABLE .. CLONE`` (reads the source), ``CREATE TABLE .. LIKE`` (writes only: no data flows),
``CACHE TABLE .. AS``, ``MERGE INTO`` (``UPDATE SET col = ..``, ``SET *``, ``INSERT (cols) VALUES``, ``INSERT *``, ``DELETE``), ``UPDATE .. SET .. [FROM ..]``, ``DELETE [.. USING | FROM ..]``,
T-SQL ``SELECT .. INTO``, ``#temp`` tables and ``[bracket]`` quoting, ``USE <db>``, ``COPY INTO``, file readers (``read_parquet``, ``read_csv``, ``delta_scan``, ``'path/file.parquet'``,
Spark ``delta.`path```, ``parquet.`path```), time travel (the same table), Spark temp views and DuckDB replacement scans through the session.
Not supported (reported as gaps or ignored as carrying no lineage): ``PIVOT`` / ``UNPIVOT`` column mapping, ``TRANSFORM(..) USING`` scripts, stored procedure calls (``EXEC``),
dynamic SQL, ``INSERT OVERWRITE DIRECTORY ... USING``, statements sqlglot cannot parse (``parse_error``; text that parses only as a bare expression, such as a misspelt ``SELEC x``, is a ``parse_error`` too),
statements that chain more than :data:`MAX_BARE_JOINS` ``JOIN`` s without ``ON`` / ``USING`` (sqlglot's parser is exponential on them; reported as ``unsupported_construct``, not parsed). ``DROP`` / ``ALTER`` / ``OPTIMIZE`` / ``VACUUM`` / ``SET`` / ``SHOW`` / ``DESCRIBE`` carry no lineage and are skipped silently.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any

import sqlglot
from sqlglot import exp

from olcf.intelligence.lineage.base import (
    MAX_DEPTH,
    MAX_SQL_CHARS,
    MAX_STATEMENTS,
    SQL,
    FactBuilder,
    LineageContext,
    SqlContext,
    classify_path,
    column_ref,
    query_ref,
    table_ref,
    view_ref,
)
from olcf.intelligence.lineage.relation import (
    EMPTY,
    ColInfo,
    Dataset,
    Lookup,
    Origin,
    Relation,
    add_step,
    chain_text,
    group_origins,
    opaque_relation,
)
from olcf.intelligence.model import AssetFact, AssetRef, Facts, clip, normalize_qname

try:  # pragma: no cover - the allowlist is optional knowledge (functions that are *not* built-ins are flagged as UDFs on the edge)
    from olcf.sql.spark_functions import SPARK_ALLOWED_FUNCTIONS as _BUILTIN_FUNCTIONS
except ImportError:  # pragma: no cover
    _BUILTIN_FUNCTIONS = frozenset()


class _QuietFallback(logging.Filter):
    """sqlglot logs a warning for every statement it falls back to a ``Command`` for (``OPTIMIZE t``); that is expected here and handled."""

    def filter(self, record: logging.LogRecord) -> bool:
        return "Falling back to parsing as a 'Command'" not in record.getMessage()


logging.getLogger("sqlglot").addFilter(_QuietFallback())

DIALECTS = {
    "spark": "spark", "sparksql": "spark", "pyspark": "spark", "databricks": "spark", "hive": "spark", "": "spark",
    "duckdb": "duckdb", "tsql": "tsql", "t-sql": "tsql", "mssql": "tsql", "sqlserver": "tsql", "warehouse": "tsql", "fabric": "tsql",
}
#: DuckDB / Spark table functions that read files; the first literal argument is the path.
FILE_FUNCTIONS = {
    "read_parquet": "parquet", "parquet_scan": "parquet", "read_csv": "csv", "read_csv_auto": "csv", "csv_scan": "csv", "read_json": "json", "read_json_auto": "json",
    "read_ndjson": "json", "read_ndjson_auto": "json", "delta_scan": "delta", "iceberg_scan": "iceberg", "read_text": "text", "read_blob": "binary", "read_files": None,
    "read_xlsx": "xlsx", "st_read": None,
}
SPARK_FILE_FORMATS = {"parquet", "csv", "json", "orc", "text", "avro", "delta", "binaryfile", "xml", "iceberg", "hudi"}
_FILE_EXT = re.compile(r"\.(parquet|csv|tsv|json|jsonl|ndjson|orc|avro|txt|xlsx|delta)(\.gz|\.zst|\.snappy)?$", re.IGNORECASE)
#: first words of statements that parse as ``Command`` but carry no lineage
_IGNORED_COMMANDS = {
    "optimize", "vacuum", "analyze", "refresh", "msck", "describe", "desc", "show", "set", "reset", "use", "explain", "grant", "revoke", "deny", "comment", "uncache", "clear",
    "add", "list", "restore", "convert", "generate", "declare", "print", "raiserror", "throw", "begin", "commit", "rollback", "alter", "drop", "truncate", "create", "pragma",
    "install", "load", "call", "cache", "ref", "attach", "detach", "checkpoint", "export", "import", "summarize", "cluster",
}
#: node types sqlglot returns for text that is not a statement at all (a misspelt keyword such as ``SELEC x`` reads as an aliased column; ``[1, 2]`` as an array)
_NOT_A_STATEMENT = (exp.Alias, exp.Condition, exp.Identifier, exp.Placeholder, exp.Parameter, exp.Star, exp.Bracket, exp.Interval, exp.Tuple, exp.Array)
MAX_QUERY_DEPTH = 60

#: JOIN keywords that need no ON / USING (they do not make the parser backtrack) and the tokens that end a run of bare ``JOIN`` s
_JOIN_TOKENS = re.compile(r"\b(CROSS\s+JOIN|NATURAL\s+(?:\w+\s+)?JOIN|JOIN|ON|USING)\b", re.IGNORECASE)
MAX_BARE_JOINS = 10


def parse_hazard(text: str) -> str | None:
    """Why ``text`` must not be handed to the SQL parser, or ``None``.

    sqlglot's parser backtracks exponentially over a chain of ``JOIN`` s that have no ``ON`` / ``USING`` (``FROM a JOIN b JOIN c JOIN d ...``: 16 joins take seconds, 30 never finish), which would let one
    hostile statement stall an analysis. More than :data:`MAX_BARE_JOINS` such joins in a row (``CROSS`` / ``NATURAL`` joins excepted) is reported instead of parsed.
    """
    run = 0
    for m in _JOIN_TOKENS.finditer(text):
        word = m.group(1).upper()
        if word == "JOIN":
            run += 1
            if run > MAX_BARE_JOINS:
                return f"the statement chains more than {MAX_BARE_JOINS} JOINs without ON / USING conditions, which the SQL parser cannot process in reasonable time"
        elif word in ("ON", "USING"):
            run = 0
    return None

#: Substituted by the Python analyzer for the parts of an f-string / format template that are not known statically. A table or file name containing it is a dynamic
#: reference (gap); in a value position it is just an opaque constant.
UNKNOWN_MARK = "__olcf_unknown_"


# ================================================================================================================================== dialect / splitting
def resolve_dialect(name: str | None) -> str:
    """Map a platform dialect name (``spark``, ``duckdb``, ``tsql``, ``sparksql`` ...) to a sqlglot dialect; unknown names that sqlglot knows are passed through."""
    key = (name or "").strip().lower()
    if key in DIALECTS:
        return DIALECTS[key]
    try:
        from sqlglot.dialects.dialect import Dialect

        Dialect.get_or_raise(key)
        return key
    except Exception:  # noqa: BLE001 - unknown dialect name
        return "spark"


def _first_code_offset(text: str) -> int:
    """Offset of the first character that is neither whitespace nor part of a leading comment."""
    i, n = 0, len(text)
    while i < n:
        if text[i].isspace():
            i += 1
        elif text.startswith("--", i):
            j = text.find("\n", i)
            i = n if j < 0 else j + 1
        elif text.startswith("/*", i):
            j = text.find("*/", i + 2)
            i = n if j < 0 else j + 2
        else:
            break
    return i


def split_statements(sql: str, dialect: str = "spark") -> list[tuple[str, int]]:
    """Split a script into ``(statement, first line)`` pairs (1-based lines), ignoring ``;`` inside strings, quoted identifiers and comments.

    Handles ``'..'`` (doubled quote, and backslash escapes for Spark), ``".."``, back-ticks (Spark), ``[..]`` (T-SQL), ``--`` and ``/* */`` comments (nested for T-SQL), ``$$..$$``
    (DuckDB) and T-SQL ``GO`` batch separators. Comment-only fragments are dropped. An unterminated string or comment makes the rest of the script one statement,
    which the parser then reports as a parse error instead of the splitter guessing.
    """
    d = resolve_dialect(dialect)
    out: list[tuple[str, int]] = []
    buf_start, i, n, line = 0, 0, len(sql), 1
    buf_line = 1

    def flush(end: int) -> None:
        nonlocal buf_start, buf_line
        frag = sql[buf_start:end]
        off = _first_code_offset(frag)
        if off < len(frag):
            body = frag[off:].strip()
            if body:
                out.append((body, buf_line + frag.count("\n", 0, off)))

    while i < n:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""
        if ch == "\n":
            line += 1
            i += 1
            continue
        if ch == "'" or ch == '"' or (ch == "`" and d == "spark") or (ch == "[" and d == "tsql"):
            close = "]" if ch == "[" else ch
            j = i + 1
            while j < n:
                c = sql[j]
                if c == "\n":
                    line += 1
                if c == "\\" and ch in ("'", '"') and d == "spark" and j + 1 < n:
                    j += 2
                    continue
                if c == close:
                    if j + 1 < n and sql[j + 1] == close and ch != "[":
                        j += 2
                        continue
                    break
                j += 1
            if j >= n:
                flush(n)
                return out
            i = j + 1
            continue
        if ch == "-" and nxt == "-":
            j = sql.find("\n", i)
            i = n if j < 0 else j
            continue
        if ch == "/" and nxt == "*":
            depth, j = 1, i + 2
            while j < n and depth:
                if d == "tsql" and sql.startswith("/*", j):
                    depth += 1
                    j += 2
                elif sql.startswith("*/", j):
                    depth -= 1
                    j += 2
                else:
                    if sql[j] == "\n":
                        line += 1
                    j += 1
            if depth:
                flush(n)
                return out
            i = j
            continue
        if ch == "$" and nxt == "$" and d == "duckdb":
            j = sql.find("$$", i + 2)
            if j < 0:
                flush(n)
                return out
            line += sql.count("\n", i, j)
            i = j + 2
            continue
        if ch == ";":
            flush(i)
            i += 1
            buf_start, buf_line = i, line
            continue
        if d == "tsql" and ch in "Gg" and (i == 0 or sql[i - 1] == "\n" or sql[max(0, sql.rfind("\n", 0, i) + 1):i].strip() == ""):
            m = re.match(r"GO[ \t]*(?:\d+[ \t]*)?(?=\r?\n|$)", sql[i:], re.IGNORECASE)
            if m:
                flush(i)
                i += m.end()
                buf_start, buf_line = i, line
                continue
        i += 1
    flush(n)
    if d == "tsql":
        out = _strip_tsql_blocks(out)
    return out


_TSQL_BLOCK = re.compile(r"^(?:BEGIN\s+(?:TRY|CATCH|TRAN(?:SACTION)?)|END\s+(?:TRY|CATCH)|BEGIN|END)\b[ \t]*", re.IGNORECASE)


def _strip_tsql_blocks(stmts: list[tuple[str, int]]) -> list[tuple[str, int]]:
    """T-SQL ``BEGIN .. END`` / ``TRY .. CATCH`` blocks hold ``;`` separated statements; drop the block keywords from the fragments that carry them."""
    out: list[tuple[str, int]] = []
    for text, line in stmts:
        rest = text
        while True:
            m = _TSQL_BLOCK.match(rest)
            if not m:
                break
            rest = rest[m.end():]
        rest = rest.strip()
        if rest:
            out.append((rest, line))
    return out


# ================================================================================================================================== scope
@dataclass
class Source:
    """One input of a SELECT: the qualifiers it answers to (aliases or table names, lower-case) and its relation."""

    quals: frozenset[str]
    rel: Relation


@dataclass
class Scope:
    """The inputs visible to the expressions of one SELECT, with its enclosing scope (for correlated sub-queries)."""

    outer: Scope | None = None
    sources: list[Source] = field(default_factory=list)
    using: dict[str, list[Source]] = field(default_factory=dict)
    aliases: dict[str, ColInfo] = field(default_factory=dict)
    windows: dict[str, exp.Expression] = field(default_factory=dict)

    def resolve(self, parts: Sequence[str], name: str) -> Lookup:
        """Resolve ``parts.name`` (``parts`` = optional qualifier parts) against the sources, then select-list aliases, then the enclosing scope."""
        low = name.lower()
        if parts:
            q = ".".join(p.lower() for p in parts)
            last = parts[-1].lower()
            hits = [s for s in self.sources if q in s.quals or last in s.quals]
            if hits:
                sub = hits[0].rel.lookup(name)
                if sub.status != "missing":
                    return sub
                return sub
            # struct field access: ``a.b`` where ``a`` is a column
            first = self.resolve((), parts[0])
            if first.status in ("ok", "opaque"):
                return first
            if self.outer is not None:
                return self.outer.resolve(parts, name)
            return Lookup("missing")
        if low in self.using and len(self.using[low]) > 1:
            merged: set[Origin] = set()
            for s in self.using[low]:
                lk = s.rel.lookup(name)
                if lk.status != "ok":
                    return lk
                merged |= lk.origins
            return Lookup("ok", frozenset(merged), name)
        known = [s for s in self.sources if any(c.name.lower() == low for c in s.rel.cols)]
        if len(known) == 1:
            return known[0].rel.lookup(name)
        if len(known) > 1:
            first_lk = known[0].rel.lookup(name)
            if all(s.rel.lookup(name).origins == first_lk.origins and s.rel.lookup(name).status == "ok" for s in known):
                return first_lk
            return Lookup("ambiguous")
        maybe = [s for s in self.sources if s.rel.opaque or s.rel.tail]
        if len(maybe) == 1:
            return maybe[0].rel.lookup(name)
        if len(maybe) > 1:
            return Lookup("ambiguous")
        if low in self.aliases:
            ci = self.aliases[low]
            return Lookup("ok", ci.origins, ci.name)
        if self.outer is not None:
            return self.outer.resolve(parts, name)
        return Lookup("missing")


@dataclass
class Env:
    """Names visible by name only: the CTEs defined so far (lower-case name -> relation)."""

    ctes: dict[str, Relation] = field(default_factory=dict)


@dataclass
class StatementResult:
    """What one statement did: its ``kind``, the relation of a query (``None`` for DML / DDL), the written dataset and where it started."""

    kind: str
    relation: Relation | None = None
    target: Dataset | None = None
    line: int = 1
    index: int = 1
    process: AssetRef | None = None


@dataclass
class _Stmt:
    """Per-statement working state."""

    process: AssetRef
    loc: dict[str, Any]
    text: str
    index: int
    line: int
    reads: dict[str, Dataset] = field(default_factory=dict)
    star_reported: set[str] = field(default_factory=set)
    depth: int = 0
    exclude_reads: frozenset[str] = frozenset()


def _arg(node: exp.Expression, *names: str) -> Any:
    """First non-empty argument among ``names`` (sqlglot renamed ``from``/``with`` to ``from_``/``with_``; both spellings are accepted)."""
    for n in names:
        v = node.args.get(n)
        if v:
            return v
    return None


def _has_placeholder_qualifier(tbl: exp.Table) -> bool:
    """True when the database / catalog part of a table name is a parameter (``${db}.orders``, ``:schema.orders``) rather than a plain identifier."""
    return any(isinstance(tbl.args.get(k), exp.Expression) and not isinstance(tbl.args.get(k), exp.Identifier) for k in ("db", "catalog"))


def _ident_name(x: Any) -> str:
    """Name of an Identifier / Var / string argument; ``""`` for anything else."""
    if x is None:
        return ""
    if isinstance(x, str):
        return x
    if isinstance(x, exp.Identifier) and x.args.get("temporary") and not str(x.this).startswith("#"):
        return "#" + str(x.this)
    return str(getattr(x, "name", "") or "")


# ================================================================================================================================== session
class SqlSession:
    """State shared by the statements of one script, notebook or Python program: temp views, schemas learned from DDL, the current database and the fact builder.

    ``var_lookup`` (optional) lets DuckDB ``FROM df`` resolve to a Python variable holding a relation (replacement scan). ``analyzer`` is the evidence source name.
    """

    def __init__(self, ctx: LineageContext, builder: FactBuilder, dialect: str, *, analyzer: str = SQL, emit_query_assets: bool = True) -> None:
        self.ctx = ctx
        self.b = builder
        self.dialect = resolve_dialect(dialect)
        self.analyzer = analyzer
        self.views: dict[str, Relation] = {}
        self.table_schemas: dict[str, tuple[str, ...]] = {}
        self.persistent_views: set[str] = set()
        self.datasets: dict[str, Dataset] = {}
        self.current_db: str | None = None
        self.var_lookup: Callable[[str], Relation | None] | None = None
        self.emit_query_assets = emit_query_assets
        self.confidence_cap: str | None = None
        self.taint: tuple[str, ...] = ()
        self.sticky_taint: set[str] = set()
        self.local_tables = False
        self.evidence_kind = "static"
        self.table_ref_factory: Callable[[list[str]], AssetRef] | None = None
        self.st: _Stmt = _Stmt(process=ctx.process or AssetRef.make("sql_query"), loc={}, text="", index=0, line=1)
        self._named_cache: dict[str, Any] = {}

    # ------------------------------------------------------------------------------------------------------------ public driver
    def run(self, sql: str, *, process: AssetRef | None = None, loc: dict[str, Any] | None = None, line_offset: int = 0, label: str = "",
            line_key: str = "line") -> list[StatementResult]:
        """Analyse every statement of ``sql`` (a script); ``process`` overrides ``ctx.process``; ``line_offset`` is added to statement lines (for SQL inside code);
        ``line_key`` names the location field that carries the statement's line (``sql_line`` when the SQL sits inside Python code that has its own ``line``)."""
        results: list[StatementResult] = []
        if len(sql) > MAX_SQL_CHARS:
            self.b.gap("unsupported_construct", label or "sql", f"the SQL text is larger than {MAX_SQL_CHARS} characters and was not analysed",
                       "Split the script into smaller statements.", loc, self.analyzer)
            return results
        stmts = split_statements(sql, self.dialect)
        if len(stmts) > MAX_STATEMENTS:
            self.b.gap("unsupported_construct", label or "sql", f"the script has more than {MAX_STATEMENTS} statements; the rest was not analysed",
                       "Split the script.", loc, self.analyzer)
            stmts = stmts[:MAX_STATEMENTS]
        for idx, (text, line) in enumerate(stmts, 1):
            results.extend(self.statement(text, line + line_offset, idx, process, loc, line_key))
        return results

    def statement(self, text: str, line: int, index: int, process: AssetRef | None, loc: dict[str, Any] | None, line_key: str = "line") -> list[StatementResult]:
        """Analyse one statement text (usually one parsed statement; T-SQL may yield several)."""
        base_loc = dict(loc or {})
        base_loc.update({"statement": index, line_key: line})
        hazard = parse_hazard(text)
        if hazard is not None:
            self.b.gap("unsupported_construct", clip(text, 80) or "sql", hazard, "Add the missing join conditions (or CROSS JOIN) or split the statement.", base_loc, self.analyzer)
            return []
        try:
            parsed = [p for p in sqlglot.parse(text, read=self.dialect) if p is not None]
        except RecursionError:
            self.b.gap("parse_error", clip(text, 80) or "sql", "the statement is nested too deeply to parse", "Simplify the statement.", base_loc, self.analyzer)
            return []
        except Exception as exc:  # noqa: BLE001 - sqlglot raises ParseError / TokenError / assorted errors for malformed input
            self.b.gap("parse_error", clip(text, 80) or "sql", f"cannot parse the {self.dialect} statement: {str(exc).splitlines()[0][:200] if str(exc) else type(exc).__name__}",
                       "Fix the SQL syntax or check that the dialect matches the engine the statement runs on.", base_loc, self.analyzer)
            return []
        out: list[StatementResult] = []
        for node in parsed:
            out.append(self._one(node, text, line, index, process, base_loc))
        return out

    # ------------------------------------------------------------------------------------------------------------ one statement
    def _one(self, node: exp.Expression, text: str, line: int, index: int, process: AssetRef | None, loc: dict[str, Any]) -> StatementResult:
        proc = process or self.ctx.process
        if proc is None:
            norm = node.sql(dialect=self.dialect, comments=False)
            proc = query_ref(norm)
            if self.emit_query_assets and not isinstance(node, exp.Command):
                self.b.asset(AssetFact(ref=proc, name=clip(norm, 80) or "query", attrs={"statement": type(node).__name__.lower(), "dialect": self.dialect, "text": clip(text, 2000)},
                                       workspace_id=self.ctx.workspace_id))
        saved = self.st
        self.st = _Stmt(process=proc, loc=loc, text=text, index=index, line=line)
        result = StatementResult(kind="other", line=line, index=index, process=proc)
        try:
            result = self._dispatch(node, result)
        except RecursionError:
            self.b.gap("parse_error", clip(text, 80) or "sql", "the statement is nested too deeply to analyse", "Simplify the statement.", loc, self.analyzer)
        except Exception as exc:  # noqa: BLE001 - never let an analyzer bug lose the table-level facts
            self.b.gap("unsupported_construct", clip(text, 80) or "sql", f"column-level analysis of this statement failed ({type(exc).__name__}); table-level lineage only",
                       "Report the statement to the OLCF maintainers.", loc, self.analyzer)
            self._fallback_tables(node)
        finally:
            self._flush_reads()
            self.st = saved
        return result

    def _dispatch(self, node: exp.Expression, result: StatementResult) -> StatementResult:
        if isinstance(node, (exp.Select, exp.SetOperation, exp.Subquery, exp.Values)):
            into = node.args.get("into") if isinstance(node, exp.Select) else None
            rel = self._query(node, Env(), None)
            result.kind, result.relation = "select", rel
            if into is not None:
                tgt = into.this
                if isinstance(tgt, exp.Table):
                    names = [c.name for c in rel.cols]
                    result.target = self._write_query(tgt, rel, names, mode="create", kind="table", temp=bool(into.args.get("temporary")) or _ident_name(tgt.this).startswith("#"))
                    result.kind = "select_into"
            return result
        if isinstance(node, exp.Insert):
            return self._insert(node, result)
        if isinstance(node, exp.Create):
            return self._create(node, result)
        if isinstance(node, exp.Merge):
            return self._merge(node, result)
        if isinstance(node, exp.Update):
            return self._update(node, result)
        if isinstance(node, exp.Delete):
            return self._delete(node, result)
        if isinstance(node, exp.Cache):
            return self._cache(node, result)
        if isinstance(node, exp.Copy):
            return self._copy(node, result)
        if isinstance(node, exp.Use):
            name = _ident_name(node.this.this if isinstance(node.this, exp.Table) else node.this)
            if isinstance(node.this, exp.Table) and node.this.args.get("db"):
                name = f"{_ident_name(node.this.args['db'])}.{name}"
            if name:
                self.current_db = name
            result.kind = "use"
            return result
        if isinstance(node, exp.Execute):
            tname = node.this.name if isinstance(node.this, exp.Table) else str(getattr(node.this, "name", "") or "")
            self.b.gap("unsupported_construct", tname or "EXEC", "a stored procedure or dynamic SQL is executed (EXEC); its lineage is in its own definition and cannot be seen here",
                       "Analyse the procedure's definition (Warehouse / SQL database item) or the dynamic statement separately.", self.st.loc, self.analyzer)
            result.kind = "execute"
            return result
        if isinstance(node, exp.Command):
            word = str(node.this or "").strip().split()[0].lower() if str(node.this or "").strip() else ""
            if word not in _IGNORED_COMMANDS:
                self.b.gap("unsupported_construct", clip(self.st.text, 80) or "sql", f"statement type '{word or 'unknown'}' is not understood by the SQL analyzer",
                           "Lineage of this statement is not available.", self.st.loc, self.analyzer)
            result.kind = "command"
            return result
        if isinstance(node, _NOT_A_STATEMENT):
            self.b.gap("parse_error", clip(self.st.text, 80) or "sql", f"cannot parse the {self.dialect} statement: it is not a SQL statement (it reads as a bare expression)",
                       "Fix the SQL syntax (for example a misspelt keyword) or check that the dialect matches the engine the statement runs on.", self.st.loc, self.analyzer)
            result.kind = "invalid"
            return result
        result.kind = type(node).__name__.lower()
        return result

    # ------------------------------------------------------------------------------------------------------------ reads / writes
    def _flush_reads(self) -> None:
        """Emit READS_FROM for every dataset the statement read (once each)."""
        st = self.st
        for key in list(st.reads):
            if key in st.exclude_reads:
                continue
            ds = st.reads[key]
            self._edge(st.process, ds.ref, "READS_FROM", snippet=st.text, attrs={"dataset_kind": ds.kind})
        st.reads.clear()

    def _edge(self, src: AssetRef, dst: AssetRef, etype: str, *, snippet: str | None, confidence: str = "derived", transformation: str | None = None,
              via: AssetRef | None = None, attrs: dict[str, Any] | None = None, extra_loc: dict[str, Any] | None = None) -> None:
        conf = confidence
        if self.confidence_cap and conf in ("verified", "derived") and self.confidence_cap == "inferred":
            conf = "inferred"
        if (self.taint or self.sticky_taint) and conf in ("verified", "derived") and self._tainted(src, dst):
            conf = "inferred"
        loc = dict(self.st.loc)
        if extra_loc:
            loc.update(extra_loc)
        self.b.edge(src, dst, etype, confidence=conf, kind=self.evidence_kind, location=loc, snippet=snippet, transformation=transformation, via=via, attrs=attrs, analyzer=self.analyzer)

    def _tainted(self, *refs: AssetRef) -> bool:
        """True if a name of ``refs`` was built from a notebook parameter value (the edge then only holds for the parameter's default)."""
        names: list[str] = []
        for r in refs:
            for n in (r.name or "", r.scope_map.get("table", "")):
                if n:
                    names.append(n.lower())
        parts = {p for n in names for p in re.split(r"[./\\]", n) if p}
        for t in (*self.taint, *self.sticky_taint):
            tl = t.lower()
            if not tl:
                continue
            if tl in parts or tl in names or (len(tl) >= 4 and any(tl in n for n in names)):
                return True
        return False

    def _write(self, ds: Dataset, mode: str, attrs: dict[str, Any] | None = None) -> None:
        if ds.kind == "dynamic":
            return
        self._flush_reads()
        self._edge(self.st.process, ds.ref, "WRITES_TO", snippet=self.st.text, attrs={"mode": mode, "dataset_kind": ds.kind, **(attrs or {})})

    def begin_site(self, process: AssetRef, loc: dict[str, Any], text: str = "", taint: tuple[str, ...] = ()) -> None:
        """Make ``process`` / ``loc`` / ``text`` the origin of the edges recorded next through :meth:`record_read` / :meth:`record_write` / :meth:`emit_columns` (used by
        analyzers of definitions that are not SQL text, such as pipeline copy activities)."""
        self.st = _Stmt(process=process, loc=dict(loc), text=text, index=0, line=0)
        self.taint = taint

    def record_read(self, ds: Dataset) -> None:
        """Record at the current site that ``ds`` is read (``READS_FROM`` is emitted immediately)."""
        self.note_read(ds)
        self._flush_reads()

    def record_write(self, ds: Dataset, mode: str, attrs: dict[str, Any] | None = None) -> None:
        """Record at the current site that ``ds`` is written (``WRITES_TO`` with the write ``mode``)."""
        self._write(ds, mode, attrs)

    # ------------------------------------------------------------------------------------------------------------ datasets
    def table_dataset(self, parts: Sequence[str], *, kind: str = "table", temp: bool = False) -> Dataset:
        """The dataset for a (possibly partial) table name; unqualified names get the ``USE`` database as prefix."""
        names = [p for p in parts if p]
        if any(UNKNOWN_MARK in n for n in names):
            return self._dynamic(".".join(names))
        if len(names) == 1 and self.current_db and not temp and not names[0].startswith("#"):
            names = [*self.current_db.split("."), *names]
        name = ".".join(names)
        key = "t:" + normalize_qname(name)
        have = self.datasets.get(key)
        if have is not None and (have.kind == kind or kind == "table"):
            return have
        if key in self.persistent_views and kind == "table":
            kind = "view"
        if self.table_ref_factory is not None:
            ref = self.table_ref_factory(names)
        else:
            ref = view_ref(name, self.ctx) if kind == "view" else table_ref(name, self.ctx)
        schema = self._schema_for(name, key)
        ds = Dataset(key=key, kind=kind, name=name, ref=ref, schema=tuple(schema) if schema else None)
        self.datasets[key] = ds
        return ds

    def _dynamic(self, name: str) -> Dataset:
        """A reference whose name is only partly known: a gap is recorded and no edge is ever emitted for it."""
        pattern = re.sub(re.escape(UNKNOWN_MARK) + r"\d+__", "<?>", name)
        self.b.gap("dynamic_reference", clip(pattern, 120) or "?", f"the dataset name is built at run time ({clip(pattern, 120)}); the reference cannot be resolved statically",
                   "Use a literal name, or a notebook parameter with a default value, so that the dependency can be seen.", self.st.loc, self.analyzer)
        return Dataset(key="x:dynamic:" + pattern, kind="dynamic", name=pattern, ref=AssetRef.make("external", name=pattern))

    def file_dataset(self, path: str, fmt: str | None = None) -> Dataset:
        """The dataset for a file / folder / Tables path (a table when the path is ``Tables/<name>``)."""
        if UNKNOWN_MARK in path:
            return self._dynamic(path)
        target = classify_path(path, fmt=fmt)
        ref = target.ref(self.ctx, fmt)
        kind = "table" if ref.type == "table" else "file"
        own = not target.lakehouse or target.lakehouse.lower() == str((self.ctx.default_lakehouse or {}).get("name") or "").lower()
        lh_part = "" if own else str(target.lakehouse) + "/"
        if kind == "table" and target.kind == "table" and not ref.alias_map.get("delta_location"):
            key = "t:" + lh_part + normalize_qname(target.name)
            have = self.datasets.get(key)
            if have is not None:
                return have
            schema = self._schema_for(target.name, key) if own else None
            ds = Dataset(key=key, kind="table", name=target.name, ref=ref, schema=tuple(schema) if schema else None)
        else:
            key = "f:" + (ref.alias_map.get("onelake_path") or ref.alias_map.get("delta_location") or lh_part + target.name)
            ds = self.datasets.get(key) or Dataset(key=key, kind=kind, name=target.name, ref=ref)
        self.datasets[ds.key] = ds
        return ds

    def learn_schema(self, ds: Dataset, columns: tuple[str, ...]) -> None:
        """Remember the columns of ``ds`` (declared by DDL or produced by a query) so that a later ``SELECT *`` / unqualified column over it can be resolved."""
        self.table_schemas[ds.key] = columns
        have = self.datasets.get(ds.key)
        if have is not None and have.schema != columns:
            self.datasets[ds.key] = replace(have, schema=columns)

    def _schema_for(self, name: str, key: str) -> list[str] | None:
        got = self.ctx.lookup_schema(name)
        if got:
            return got
        sch = self.table_schemas.get(key)
        return list(sch) if sch else None

    def base_relation(self, ds: Dataset) -> Relation:
        """The relation of a base dataset: its known columns (origins = themselves) or an unknown tail."""
        if ds.kind == "dynamic":
            return opaque_relation()
        if ds.schema:
            cols = tuple(ColInfo(c, frozenset({Origin(ds.key, c)})) for c in ds.schema)
            return Relation(cols, (), frozenset({ds.key}))
        return Relation((), (ds.key,), frozenset({ds.key}))

    def note_read(self, ds: Dataset) -> None:
        """Remember that the current statement reads ``ds`` (its READS_FROM edge is emitted when the statement ends)."""
        if ds.kind != "dynamic":
            self.st.reads.setdefault(ds.key, ds)

    # ------------------------------------------------------------------------------------------------------------ queries
    def _query(self, node: exp.Expression, env: Env, outer: Scope | None) -> Relation:
        """Relation produced by a query expression (SELECT, set operation, sub-query, VALUES, bare table)."""
        self.st.depth += 1
        try:
            if self.st.depth > MAX_QUERY_DEPTH:
                self.b.gap("unsupported_construct", clip(self.st.text, 80) or "sql", f"queries nested more than {MAX_QUERY_DEPTH} levels deep are not analysed", None, self.st.loc, self.analyzer)
                return opaque_relation()
            while isinstance(node, (exp.Subquery, exp.Paren)) and node.this is not None:
                node = node.this
            if isinstance(node, exp.Select):
                return self._select(node, env, outer)
            if isinstance(node, exp.SetOperation):
                return self._setop(node, env, outer)
            if isinstance(node, exp.Values):
                return self._values(node)
            if isinstance(node, exp.Table):
                scope = Scope(outer)
                scope.sources.append(self._table_source(node, scope, env))
                return self._project_star(scope, [exp.Star()])
            self.b.gap("unsupported_construct", clip(self.st.text, 80) or "sql", f"query form {type(node).__name__} is not understood", None, self.st.loc, self.analyzer)
            return opaque_relation()
        finally:
            self.st.depth -= 1

    def _values(self, node: exp.Values) -> Relation:
        alias = node.args.get("alias")
        names: list[str] = [i.name for i in alias.columns] if alias is not None and alias.columns else []
        first = node.expressions[0] if node.expressions else None
        width = len(first.expressions) if isinstance(first, exp.Tuple) else 1
        if not names:
            names = [f"col{i + 1}" for i in range(width)]
        return Relation(tuple(ColInfo(n) for n in names))

    def _with(self, node: exp.Expression, env: Env, outer: Scope | None) -> Env:
        """Evaluate the WITH clause of ``node`` (in order, later CTEs see earlier ones) and return the extended environment."""
        w = _arg(node, "with_", "with")
        if w is None:
            return env
        new = Env(dict(env.ctes))
        recursive = bool(w.args.get("recursive"))
        for cte in w.expressions:
            name = cte.alias
            if not name:
                continue
            body = cte.this
            alias_cols = [c.name for c in cte.args["alias"].columns] if cte.args.get("alias") is not None and cte.args["alias"].columns else []
            if recursive:
                anchor = body
                while isinstance(anchor, (exp.Subquery, exp.Paren)):
                    anchor = anchor.this
                if isinstance(anchor, exp.SetOperation):
                    branches = _flatten_setop(anchor)
                    pre = self._query(branches[0][1], new, outer)
                    new.ctes[name.lower()] = _rename(pre, alias_cols)
            rel = self._query(body, new, outer)
            new.ctes[name.lower()] = _rename(rel, alias_cols)
        return new

    def _setop(self, node: exp.SetOperation, env: Env, outer: Scope | None) -> Relation:
        env = self._with(node, env, outer)
        branches = _flatten_setop(node)
        rels: list[tuple[str, Relation]] = []
        for op, b in branches:
            rels.append((op, self._query(b, env, outer)))
        first = rels[0][1]
        by_name = bool(node.args.get("by_name"))
        cols = list(first.cols)
        tail = list(first.tail)
        datasets = set(first.datasets)
        opaque = first.opaque
        partial_all = False
        for op, rel in rels[1:]:
            datasets |= rel.datasets
            if op in ("EXCEPT", "INTERSECT"):
                continue                      # rows come from the left side only
            tail.extend(t for t in rel.tail if t not in tail)
            opaque = opaque or rel.opaque
            if by_name:
                for i, c in enumerate(cols):
                    other = [x for x in rel.cols if x.name.lower() == c.name.lower()]
                    if other:
                        cols[i] = ColInfo(c.name, c.origins | other[0].origins, c.partial or other[0].partial, c.expr if c.expr == other[0].expr else None)
                continue
            if not (rel.positional_ok and first.positional_ok) or len(rel.cols) != len(cols):
                partial_all = True
            for i in range(min(len(cols), len(rel.cols))):
                if not first.positional_ok or not rel.positional_ok:
                    break
                c, o = cols[i], rel.cols[i]
                cols[i] = ColInfo(c.name, c.origins | o.origins, c.partial or o.partial, c.expr if c.expr == o.expr else None)
        if partial_all:
            cols = [ColInfo(c.name, c.origins, True, c.expr) for c in cols]
        return Relation(tuple(cols), tuple(tail), frozenset(datasets), (), None, opaque)

    # ------------------------------------------------------------------------------------------------------------ select
    def _select(self, node: exp.Select, env: Env, outer: Scope | None) -> Relation:
        env = self._with(node, env, outer)
        scope = Scope(outer)
        for w in node.args.get("windows") or []:
            wname = _ident_name(w.args.get("alias")) or w.alias_or_name
            if wname:
                scope.windows[wname.lower()] = w
        frm = _arg(node, "from_", "from")
        if frm is not None:
            self._add_source(frm.this, scope, env)
        for j in node.args.get("joins") or []:
            self._add_join(j, scope, env)
        for lat in node.args.get("laterals") or []:
            self._add_lateral(lat, scope, env)
        rel = self._project(node.expressions, scope, env, node)
        skip = {"expressions", "from_", "from", "joins", "laterals", "with_", "with", "into", "windows"}
        for key, val in node.args.items():
            if key in skip or val is None:
                continue
            for item in (val if isinstance(val, list) else [val]):
                if isinstance(item, exp.Expression):
                    self._visit_subqueries(item, scope, env)
        return rel

    def _project(self, projections: Sequence[exp.Expression], scope: Scope, env: Env, node: exp.Expression | None = None) -> Relation:
        """Compute the output relation of a projection list over ``scope``."""
        cols: list[ColInfo] = []
        tail: list[str] = []
        opaque = False
        datasets: set[str] = set()
        for s in scope.sources:
            datasets |= s.rel.datasets
        for i, proj in enumerate(projections):
            inner = proj.this if isinstance(proj, exp.Alias) else proj
            while isinstance(inner, exp.Paren):
                inner = inner.this
            if isinstance(inner, exp.Star) or (isinstance(inner, exp.Column) and isinstance(inner.this, exp.Star)):
                qual = inner.args.get("table") if isinstance(inner, exp.Column) else None
                star = inner.this if isinstance(inner, exp.Column) else inner
                got, t, o = self._expand_star(scope, env, _ident_name(qual) if qual is not None else None, star)
                cols.extend(got)
                tail.extend(k for k in t if k not in tail)
                opaque = opaque or o
                continue
            name = proj.alias if isinstance(proj, exp.Alias) and proj.alias else (inner.name if isinstance(inner, exp.Column) else "")
            if not name:
                name = f"_c{i}"
            origins, partial = self._origins(inner, scope, env)
            text = inner.sql(dialect=self.dialect, comments=False)
            if not isinstance(inner, exp.Column):
                origins = add_step(origins, text)
            ci = ColInfo(name, origins, partial, text)
            cols.append(ci)
            scope.aliases.setdefault(name.lower(), ci)
        return Relation(tuple(cols), tuple(tail), frozenset(datasets), (), None, opaque)

    def _project_star(self, scope: Scope, projections: Sequence[exp.Expression]) -> Relation:
        return self._project(projections, scope, Env())

    def _expand_star(self, scope: Scope, env: Env, qualifier: str | None, star: exp.Expression) -> tuple[list[ColInfo], list[str], bool]:
        """Expand ``*`` / ``t.*`` over the scope: known columns are listed, sources with unknown schema are reported as the tail."""
        excluded = {c.name.lower() for c in (star.args.get("except_") or star.args.get("except") or []) if hasattr(c, "name")}
        replace = {r.alias.lower(): r for r in (star.args.get("replace") or []) if isinstance(r, exp.Alias)}
        cols: list[ColInfo] = []
        tail: list[str] = []
        opaque = False
        using_seen: set[str] = set()
        pool = scope.sources
        if qualifier:
            q = qualifier.lower()
            pool = [s for s in scope.sources if q in s.quals]
            if not pool and scope.outer is not None:
                pool = [s for s in scope.outer.sources if q in s.quals]
        starred: list[str] = []
        for s in pool:
            for c in s.rel.cols:
                low = c.name.lower()
                if low in excluded:
                    continue
                if low in scope.using and low in using_seen:
                    continue
                if low in scope.using:
                    using_seen.add(low)
                    lk = scope.resolve((), c.name)
                    c = ColInfo(c.name, lk.origins if lk.status == "ok" else c.origins, c.partial, c.expr)
                if low in replace:
                    inner = replace[low].this
                    org, part = self._origins(inner, scope, env)
                    if not isinstance(inner, exp.Column):
                        org = add_step(org, inner.sql(dialect=self.dialect, comments=False))
                    c = ColInfo(c.name, org, part, inner.sql(dialect=self.dialect, comments=False))
                cols.append(c)
            if s.rel.tail:
                tail.extend(t for t in s.rel.tail if t not in tail)
                starred.extend(self.datasets[t].name for t in s.rel.tail if t in self.datasets)
            if s.rel.opaque:
                opaque = True
                starred.extend(self.datasets[t].name for t in sorted(s.rel.datasets) if t in self.datasets)
        if (tail or opaque) and "*" not in self.st.star_reported:
            self.st.star_reported.add("*")
            names = ", ".join(sorted(set(starred))) or "the input"
            self.b.gap("unsupported_construct", names, f"SELECT * without known schema: the columns of {names} are not known, so column-level lineage for them is not available "
                       "(the table-level lineage is recorded)", "Provide the table schema (run discovery on the lakehouse) or list the columns explicitly.", self.st.loc, self.analyzer)
        return cols, tail, opaque

    # ------------------------------------------------------------------------------------------------------------ sources
    def _add_source(self, node: exp.Expression, scope: Scope, env: Env) -> Source | None:
        src: Source | None = None
        if isinstance(node, exp.Table):
            src = self._table_source(node, scope, env)
        elif isinstance(node, (exp.Subquery, exp.Select, exp.SetOperation)):
            rel = self._query(node, env, scope)
            alias = node.alias if isinstance(node, exp.Subquery) else ""
            ta = node.args.get("alias") if isinstance(node, exp.Subquery) else None
            rel = _rename(rel, [c.name for c in ta.columns] if ta is not None and ta.columns else [])
            src = Source(frozenset({alias.lower()} if alias else set()), rel)
            pivots = node.args.get("pivots") if isinstance(node, exp.Subquery) else None
            if pivots:
                self._pivot_gap(src)
        elif isinstance(node, exp.Lateral):
            self._add_lateral(node, scope, env)
            return None
        elif isinstance(node, exp.Values):
            rel = self._values(node)
            alias = node.alias
            src = Source(frozenset({alias.lower()} if alias else set()), rel)
        elif isinstance(node, (exp.Unnest, exp.Explode, exp.GenerateSeries)):
            src = self._unnest_source(node, scope, env)
        else:
            self.b.gap("unsupported_construct", clip(self.st.text, 80) or "sql", f"FROM item of type {type(node).__name__} is not understood", None, self.st.loc, self.analyzer)
            src = Source(frozenset(), opaque_relation())
        if src is not None:
            scope.sources.append(src)
        return src

    def _add_join(self, join: exp.Join, scope: Scope, env: Env) -> None:
        node = join.this
        before = len(scope.sources)
        src: Source | None = None
        if isinstance(node, exp.Lateral):
            self._add_lateral(node, scope, env)
        else:
            src = self._add_source(node, scope, env)
        using = join.args.get("using") or []
        if using and src is not None:
            for u in using:
                low = _ident_name(u).lower()
                if not low:
                    continue
                left = [s for s in scope.sources[:before] if any(c.name.lower() == low for c in s.rel.cols) or s.rel.tail or s.rel.opaque]
                scope.using[low] = [*left, src]
        method = str(join.args.get("method") or "").upper()
        if method == "NATURAL" and src is not None:
            names = {c.name.lower() for c in src.rel.cols}
            known_left = [s for s in scope.sources[:before]]
            if all(s.rel.positional_ok for s in known_left) and src.rel.positional_ok:
                for low in sorted(names):
                    left = [s for s in known_left if any(c.name.lower() == low for c in s.rel.cols)]
                    if left:
                        scope.using[low] = [*left, src]
            else:
                self.b.gap("unsupported_construct", clip(self.st.text, 80) or "sql", "NATURAL JOIN over relations with unknown schema: the join columns are not known",
                           "Provide table schemas or use an explicit ON / USING clause.", self.st.loc, self.analyzer)
        on = join.args.get("on")
        if on is not None:
            self._visit_subqueries(on, scope, env)

    def _add_lateral(self, lat: exp.Lateral, scope: Scope, env: Env) -> None:
        """``LATERAL VIEW explode(x) e AS c`` and ``LATERAL (SELECT ..)``: a new source whose columns derive from the expression arguments."""
        inner = lat.this
        alias = lat.args.get("alias")
        qual = _ident_name(alias.this) if alias is not None and alias.this is not None else ""
        names = [c.name for c in alias.columns] if alias is not None and alias.columns else []
        if isinstance(inner, (exp.Subquery, exp.Select, exp.SetOperation)):
            rel = _rename(self._query(inner, env, scope), names)
            scope.sources.append(Source(frozenset({qual.lower()} if qual else set()), rel))
            return
        origins, partial = self._origins(inner, scope, env)
        if not names:
            names = [qual] if qual else ["col"]
        cols = tuple(ColInfo(n, add_step(origins, inner.sql(dialect=self.dialect, comments=False)), partial, inner.sql(dialect=self.dialect, comments=False)) for n in names)
        scope.sources.append(Source(frozenset({qual.lower()} if qual else set()), Relation(cols)))

    def _unnest_source(self, node: exp.Expression, scope: Scope, env: Env) -> Source:
        origins, partial = self._origins(node, scope, env)
        alias = node.args.get("alias")
        qual = _ident_name(alias.this) if alias is not None and alias.this is not None else ""
        names = [c.name for c in alias.columns] if alias is not None and alias.columns else [qual or "col"]
        text = node.sql(dialect=self.dialect, comments=False)
        cols = tuple(ColInfo(n, add_step(origins, text), partial, text) for n in names)
        return Source(frozenset({qual.lower()} if qual else set()), Relation(cols))

    def _pivot_gap(self, src: Source) -> None:
        self.b.gap("unsupported_construct", clip(self.st.text, 80) or "sql", "PIVOT / UNPIVOT: the output columns are computed from the data, so column-level lineage through it is not available "
                   "(table-level lineage is recorded)", None, self.st.loc, self.analyzer)

    def _table_source(self, tbl: exp.Table, scope: Scope, env: Env) -> Source:
        """Resolve a FROM-clause table: CTE, temp view, file reader, replacement-scan variable or base table."""
        this = tbl.this
        alias_node = tbl.args.get("alias")
        alias = _ident_name(alias_node.this) if alias_node is not None and alias_node.this is not None else ""
        alias_cols = [c.name for c in alias_node.columns] if alias_node is not None and alias_node.columns else []
        rel: Relation
        quals: set[str] = set()
        if alias:
            quals.add(alias.lower())
        if not isinstance(this, exp.Identifier):
            rel, default_qual = self._table_function(this, tbl, scope, env)
            if default_qual and not alias:
                quals.add(default_qual)
        elif _has_placeholder_qualifier(tbl):
            rel = self.base_relation(self._dynamic(clip(tbl.sql(dialect=self.dialect), 80) or "?"))
            quals.add(_ident_name(this).lower())
        else:
            catalog, db, name = _ident_name(tbl.args.get("catalog")), _ident_name(tbl.args.get("db")), _ident_name(this)
            if this.args.get("temporary") and not name.startswith("#"):
                name = "#" + name
            low = name.lower()
            full = ".".join(p for p in (catalog, db, name) if p)
            if not alias:
                quals.update({low, full.lower()})
            if db and db.lower() in SPARK_FILE_FORMATS and ("/" in name or "://" in name) and not catalog:
                ds = self.file_dataset(name, db.lower())
                self.note_read(ds)
                rel = self.base_relation(ds)
            elif not db and not catalog and (this.args.get("quoted") and ("/" in name or "://" in name or _FILE_EXT.search(name)) and self.dialect == "duckdb"):
                ds = self.file_dataset(name, None)
                self.note_read(ds)
                rel = self.base_relation(ds)
            elif not db and not catalog and low in env.ctes:
                rel = env.ctes[low]
            elif not catalog and (not db or db.lower() == "global_temp") and (low in self.views or ("#" + low in self.views and this.args.get("temporary"))):
                rel = self.views.get(low) or self.views["#" + low]
            elif not db and not catalog and self.var_lookup is not None and (got := self.var_lookup(name)) is not None:
                rel = got
            else:
                ds = self.table_dataset([p for p in (catalog, db, name) if p])
                self.note_read(ds)
                rel = self.base_relation(ds)
        rel = _rename(rel, alias_cols)
        if tbl.args.get("pivots"):
            src = Source(frozenset(quals), rel)
            self._pivot_gap(src)
            return Source(frozenset(quals), opaque_relation(rel.datasets))
        return Source(frozenset(quals), rel)

    def _table_function(self, fn: exp.Expression, tbl: exp.Table, scope: Scope, env: Env) -> tuple[Relation, str]:
        """Table-valued functions in FROM: file readers, ``IDENTIFIER('t')``, unnest / explode / range / values; anything else is opaque."""
        fname = (fn.name if isinstance(fn, exp.Anonymous) else getattr(fn, "key", type(fn).__name__)).lower()
        sql_name = fn.sql_name().lower() if hasattr(fn, "sql_name") else fname
        for cand in (fname, sql_name):
            if cand in FILE_FUNCTIONS or cand in {"readparquet", "readcsv"}:
                args = [a for a in fn.expressions] + ([fn.this] if isinstance(getattr(fn, "this", None), exp.Expression) else [])
                lit = next((a for a in args if isinstance(a, exp.Literal) and a.is_string), None)
                fmt = FILE_FUNCTIONS.get(cand) or {"readparquet": "parquet", "readcsv": "csv"}.get(cand)
                if lit is None:
                    arr = next((a for a in args if isinstance(a, exp.Array) and a.expressions and isinstance(a.expressions[0], exp.Literal)), None)
                    lit = arr.expressions[0] if arr is not None else None
                if lit is None:
                    self.b.gap("dynamic_reference", (clip(fn.sql(dialect=self.dialect), 80) or "?"), "the file path of a table function is not a string literal", "Use a literal path.", self.st.loc, self.analyzer)
                    return opaque_relation(), ""
                ds = self.file_dataset(str(lit.this), fmt)
                self.note_read(ds)
                return self.base_relation(ds), ""
        if fname == "identifier":
            lit = fn.expressions[0] if fn.expressions else None
            if isinstance(lit, exp.Literal) and lit.is_string:
                ds = self.table_dataset([p for p in re.split(r"\.(?![^`]*`)", str(lit.this)) if p])
                self.note_read(ds)
                return self.base_relation(ds), ds.name.split(".")[-1].lower()
            self.b.gap("dynamic_reference", (clip(fn.sql(dialect=self.dialect), 80) or "?"), "IDENTIFIER() with a non-literal argument names a table at run time", None, self.st.loc, self.analyzer)
            return opaque_relation(), ""
        if isinstance(fn, (exp.Unnest, exp.Explode, exp.GenerateSeries, exp.ExplodeOuter, exp.Posexplode)) or fname in {"range", "unnest", "explode", "generate_series", "posexplode", "inline", "stack", "sequence"}:
            src = self._unnest_source(fn, scope, env)
            return src.rel, ""
        self.b.gap("unsupported_construct", (clip(fn.sql(dialect=self.dialect), 80) or "?"), f"table function {fname or type(fn).__name__}() is not understood: its output columns are unknown",
                   None, self.st.loc, self.analyzer)
        return opaque_relation(), ""

    # ------------------------------------------------------------------------------------------------------------ expressions
    def _visit_subqueries(self, expr: exp.Expression, scope: Scope, env: Env) -> None:
        """Resolve the sub-queries inside a clause so that the tables they read are recorded; plain columns in the clause are ignored."""
        stack: list[exp.Expression] = [expr]
        while stack:
            n = stack.pop()
            if isinstance(n, (exp.Select, exp.SetOperation)) or (isinstance(n, exp.Subquery) and isinstance(n.this, (exp.Select, exp.SetOperation, exp.Subquery))):
                self._query(n, env, scope)
                continue
            stack.extend(n.iter_expressions())

    def _origins(self, expr: exp.Expression, scope: Scope, env: Env) -> tuple[frozenset[Origin], bool]:
        """Origins of every column an expression references (sub-queries resolved, lambda parameters excluded, window specs included)."""
        found: set[Origin] = set()
        partial = False
        stack: list[tuple[exp.Expression, frozenset[str], int]] = [(expr, frozenset(), 0)]
        while stack:
            n, lam, depth = stack.pop()
            if depth > MAX_DEPTH * 5:
                partial = True
                continue
            if isinstance(n, exp.Lambda):
                params = frozenset(i.name.lower() for i in n.expressions if hasattr(i, "name"))
                stack.append((n.this, lam | params, depth + 1))
                continue
            if isinstance(n, exp.Column):
                if isinstance(n.this, exp.Star):
                    continue
                parts = [_ident_name(p) for p in (n.args.get("catalog"), n.args.get("db"), n.args.get("table")) if p is not None and _ident_name(p)]
                if not parts and (n.name.lower() in lam or n.name.startswith(UNKNOWN_MARK)):
                    continue
                lk = scope.resolve(parts, n.name)
                if lk.status == "ok":
                    found |= lk.origins
                else:
                    partial = True
                    if lk.status in ("ambiguous", "missing"):
                        qn = ".".join([*parts, n.name])
                        why = ("is ambiguous between several inputs whose columns are not known" if lk.status == "ambiguous"
                               else "does not match any column of the inputs")
                        self.b.gap("unresolved_reference", qn, f"column {qn} {why}; it is not attributed to a source column",
                                   "Qualify the column with its table alias or provide table schemas.", self.st.loc, self.analyzer)
                continue
            if isinstance(n, (exp.Select, exp.SetOperation)) or (isinstance(n, exp.Subquery) and isinstance(n.this, (exp.Select, exp.SetOperation, exp.Subquery))):
                rel = self._query(n, env, scope)
                for c in rel.cols:
                    found |= c.origins
                    partial = partial or c.partial
                if rel.tail or rel.opaque:
                    partial = True
                continue
            if isinstance(n, exp.Window):
                wname = _ident_name(n.args.get("alias")) if n.args.get("alias") is not None else ""
                w = self._find_window(scope, wname.lower()) if wname else None
                if w is not None:
                    stack.append((w, lam, depth + 1))
            if isinstance(n, exp.Anonymous):
                fname = str(n.name).lower()
                if fname and fname not in _BUILTIN_FUNCTIONS and _BUILTIN_FUNCTIONS:
                    self._udfs.add(fname)
            stack.extend((c, lam, depth + 1) for c in n.iter_expressions())
        return frozenset(found), partial

    @property
    def _udfs(self) -> set[str]:
        return self._named_cache.setdefault("udfs", set())

    @staticmethod
    def _find_window(scope: Scope, name: str) -> exp.Expression | None:
        s: Scope | None = scope
        while s is not None:
            if name in s.windows:
                return s.windows[name]
            s = s.outer
        return None

    # ------------------------------------------------------------------------------------------------------------ edges for a written relation
    def emit_columns(self, rel: Relation, target: Dataset, names: Sequence[str | None], *, mode: str, confidence: str = "derived", note: str | None = None) -> None:
        """TRANSFORMS edges for every output column of ``rel`` that has a target name: source column -> ``target.name`` (``via`` = the statement's process)."""
        if target.kind == "dynamic":
            return
        udfs = sorted(self._udfs)
        for i, ci in enumerate(rel.cols):
            tname = names[i] if i < len(names) else None
            if not tname:
                continue
            for ds_key, col, chains in group_origins(ci.origins):
                src = self.datasets.get(ds_key)
                if src is None or src.kind == "dynamic":
                    continue
                if src.key == target.key and col.lower() == tname.lower():
                    continue                       # a column that feeds itself (UPDATE t SET c = c + 1) is not lineage
                real = [c for c in chains if c]
                text = " | ".join(chain_text(c) for c in real[:3]) if real else ci.expr
                if text == col:
                    text = None
                attrs: dict[str, Any] = {"kind": "expression" if real else "direct", "mode": mode}
                if note:
                    attrs["mapping"] = note
                if udfs and real:
                    attrs["functions_not_builtin"] = udfs
                self._edge(column_ref(src.ref, col), column_ref(target.ref, tname), "TRANSFORMS", snippet=ci.expr or self.st.text, confidence=confidence,
                           transformation=clip(text, 1000), via=self.st.process, attrs=attrs, extra_loc={"column": tname})

    # ------------------------------------------------------------------------------------------------------------ targets
    def _target_parts(self, node: Any) -> tuple[exp.Table | None, list[str]]:
        """``(table node, explicit column list)`` of an INSERT / CREATE target (``Schema(Table, cols)`` or ``Table``)."""
        cols: list[str] = []
        if isinstance(node, exp.Schema):
            cols = [c.name for c in node.expressions if getattr(c, "name", "")]
            node = node.this
        if isinstance(node, exp.Table):
            return node, cols
        return None, cols

    def _target_dataset(self, tbl: exp.Table, *, kind: str = "table", temp: bool = False) -> Dataset:
        this = tbl.this
        if isinstance(this, exp.Identifier) and _has_placeholder_qualifier(tbl):
            return self._dynamic(clip(tbl.sql(dialect=self.dialect), 80) or "?")
        if not isinstance(this, exp.Identifier):
            if isinstance(this, exp.Anonymous) and this.name.lower() == "identifier" and this.expressions and isinstance(this.expressions[0], exp.Literal):
                return self.table_dataset([p for p in str(this.expressions[0].this).split(".") if p], kind=kind)
            return self._dynamic(clip(tbl.sql(dialect=self.dialect), 80) or "?")
        parts = [p for p in (_ident_name(tbl.args.get("catalog")), _ident_name(tbl.args.get("db")), _ident_name(this)) if p]
        if len(parts) == 2 and parts[0].lower() in SPARK_FILE_FORMATS and ("/" in parts[1] or "://" in parts[1]):
            return self.file_dataset(parts[1], parts[0].lower())          # delta.`Tables/t` / parquet.`abfss://...`
        if (this.args.get("temporary") or parts[-1].startswith("#")) and not parts[-1].startswith("#"):
            parts[-1] = "#" + parts[-1]
        return self.table_dataset(parts, kind=kind, temp=temp)

    def _write_query(self, tbl: exp.Table, rel: Relation, names: Sequence[str], *, mode: str, kind: str, temp: bool, explicit_cols: Sequence[str] = ()) -> Dataset | None:
        """Register the result of a query as the content of ``tbl`` (CTAS / view / SELECT INTO): a temp object stays in the session, a persistent one is written."""
        target = self._target_dataset(tbl, kind="view" if kind == "view" else "table", temp=temp)
        if self.local_tables and not temp and kind == "table":
            temp = True                        # a DuckDB connection's own tables are not lakehouse tables
        out_names = list(explicit_cols) if explicit_cols else list(names)
        if len(out_names) < len(rel.cols):
            out_names += [c.name for c in rel.cols[len(out_names):]]
        renamed = Relation(tuple(ColInfo(n, c.origins, c.partial, c.expr) for n, c in zip(out_names, rel.cols, strict=False)), rel.tail, rel.datasets, (), None, rel.opaque)
        if temp:
            key = target.name.lower().replace("global_temp.", "")
            self.views[key] = renamed
            return None
        if kind == "view":
            self.persistent_views.add(target.key)
        if renamed.positional_ok and renamed.cols:
            self.learn_schema(target, tuple(c.name for c in renamed.cols))
            if target.schema is None:
                self.datasets[target.key] = Dataset(target.key, target.kind, target.name, target.ref, tuple(c.name for c in renamed.cols))
        self._write(target, mode)
        self.emit_columns(renamed, target, [c.name for c in renamed.cols], mode=mode)
        return target

    # ------------------------------------------------------------------------------------------------------------ INSERT / CREATE
    def _insert(self, node: exp.Insert, result: StatementResult) -> StatementResult:
        result.kind = "insert"
        this = node.this
        query = node.expression
        if isinstance(this, exp.Directory):
            path = str(this.this.this) if isinstance(this.this, exp.Literal) else ""
            if query is not None and isinstance(query, exp.Expression):
                self._query(query, self._env_for_insert(node), None)
            if path:
                ds = self.file_dataset(path, None)
                self._write(ds, "overwrite" if node.args.get("overwrite") else "append")
            return result
        tbl, cols = self._target_parts(this)
        if tbl is None:
            self.b.gap("unsupported_construct", clip(self.st.text, 80) or "?", "INSERT target is not a table", None, self.st.loc, self.analyzer)
            return result
        target = self._target_dataset(tbl)
        mode = "overwrite" if node.args.get("overwrite") else "append"
        result.target = target
        if query is None or isinstance(query, exp.Values) or not isinstance(query, exp.Expression):
            if isinstance(query, exp.Values):
                self._visit_subqueries(query, Scope(), Env())
            self._write(target, mode)
            return result
        env = self._env_for_insert(node)
        rel = self._query(query, env, None)
        self._write(target, mode)
        self._insert_columns(node, rel, target, cols, mode)
        return result

    def _env_for_insert(self, node: exp.Insert) -> Env:
        """``WITH c AS (..) INSERT INTO t SELECT .. FROM c``: the CTEs sit on the INSERT in some dialects."""
        w = _arg(node, "with_", "with")
        if w is None:
            return Env()
        holder = exp.Select()
        holder.set("with_", w)
        return self._with(holder, Env(), None)

    def _insert_columns(self, node: exp.Insert, rel: Relation, target: Dataset, cols: list[str], mode: str) -> None:
        by_name = bool(node.args.get("by_name"))
        confidence, note = "derived", None
        if cols:
            names: list[str | None] = list(cols)
            if not rel.positional_ok:
                return
        elif by_name:
            names = [c.name for c in rel.cols]
        elif target.schema and rel.positional_ok:
            names = list(target.schema)
        elif rel.positional_ok and rel.cols:
            names = [c.name for c in rel.cols]
            confidence, note = "inferred", "target columns assumed to be named like the SELECT output (target schema unknown)"
        else:
            return
        if cols and len(cols) != len(rel.cols):
            self.b.gap("unsupported_construct", target.name, f"INSERT lists {len(cols)} columns but the query produces {len(rel.cols)}", None, self.st.loc, self.analyzer)
            return
        self.emit_columns(rel, target, names, mode=mode, confidence=confidence, note=note)

    def _create(self, node: exp.Create, result: StatementResult) -> StatementResult:
        kind = str(node.args.get("kind") or "").upper()
        result.kind = "create_" + kind.lower()
        props = node.args.get("properties")
        prop_nodes = list(props.expressions) if props is not None else []
        temp = any(isinstance(p, exp.TemporaryProperty) for p in prop_nodes)
        tbl, cols = self._target_parts(node.this)
        query = node.expression
        clone = node.args.get("clone")
        like = next((p for p in prop_nodes if isinstance(p, exp.LikeProperty)), None)
        if kind not in ("TABLE", "VIEW"):
            return result
        if tbl is None:
            return result
        if isinstance(query, exp.Expression) and isinstance(query, (exp.Select, exp.SetOperation, exp.Subquery, exp.Table, exp.Values)):
            rel = self._query(query, self._env_for_create(node), None)
            result.relation = rel
            result.target = self._write_query(tbl, rel, [c.name for c in rel.cols], mode="create", kind="view" if kind == "VIEW" else "table", temp=temp, explicit_cols=cols)
            return result
        if clone is not None or like is not None:
            holder = clone if clone is not None else like
            src_tbl = holder.this if holder is not None else None
            if isinstance(src_tbl, exp.Table):
                src = self.table_dataset([p for p in (_ident_name(src_tbl.args.get("catalog")), _ident_name(src_tbl.args.get("db")), _ident_name(src_tbl.this)) if p])
                if clone is not None:                      # CLONE copies the data; LIKE copies only the column definitions (no data flows, so no READS_FROM)
                    self.note_read(src)
                target = self._target_dataset(tbl)
                result.target = target
                self._write(target, "create")
                if src.schema:
                    self.learn_schema(target, src.schema)
                    if clone is not None:
                        self.emit_columns(self.base_relation(src), target, list(src.schema), mode="create")
            return result
        # plain DDL: remember the declared columns
        if cols or isinstance(node.this, exp.Schema):
            names = [c.name for c in (node.this.expressions if isinstance(node.this, exp.Schema) else []) if getattr(c, "name", "")]
            if names and not temp:
                target = self._target_dataset(tbl)
                self.learn_schema(target, tuple(names))
        return result

    def _env_for_create(self, node: exp.Create) -> Env:
        w = _arg(node, "with_", "with")
        if w is None:
            return Env()
        holder = exp.Select()
        holder.set("with_", w)
        return self._with(holder, Env(), None)

    def _cache(self, node: exp.Cache, result: StatementResult) -> StatementResult:
        result.kind = "cache"
        tbl = node.this
        query = node.expression
        if isinstance(tbl, exp.Table) and isinstance(query, exp.Expression):
            rel = self._query(query, Env(), None)
            name = _ident_name(tbl.this)
            self.views[name.lower()] = rel
        return result

    def _copy(self, node: exp.Copy, result: StatementResult) -> StatementResult:
        result.kind = "copy"
        files = [str(f.this) for f in node.args.get("files") or [] if isinstance(f, exp.Literal)]
        this = node.this
        if node.args.get("kind"):            # COPY INTO table FROM files
            if isinstance(this, exp.Table):
                target = self._target_dataset(this)
                result.target = target
                for f in files:
                    self.note_read(self.file_dataset(f, None))
                self._write(target, "append")
            return result
        if isinstance(this, exp.Subquery):
            self._query(this, Env(), None)
        elif isinstance(this, exp.Table):
            self.note_read(self.table_dataset([p for p in (_ident_name(this.args.get("db")), _ident_name(this.this)) if p]))
        for f in files:
            self._write(self.file_dataset(f, None), "overwrite")
        return result

    # ------------------------------------------------------------------------------------------------------------ MERGE / UPDATE / DELETE
    def _dml_scope(self, target_tbl: exp.Table, extra: Iterable[exp.Expression], env: Env) -> tuple[Scope, Dataset, Source | None]:
        """Scope for UPDATE / DELETE / MERGE: the FROM / USING sources, plus the target itself when it is not among them."""
        target = self._target_dataset(target_tbl)
        scope = Scope()
        for e in extra:
            if isinstance(e, exp.From):
                self._add_source(e.this, scope, env)
            elif isinstance(e, exp.Join):
                self._add_join(e, scope, env)
            elif isinstance(e, exp.Expression):
                self._add_source(e, scope, env)
        alias = target_tbl.alias.lower() if target_tbl.alias else ""
        tname = _ident_name(target_tbl.this).lower()
        tsrc: Source | None = None
        for s in scope.sources:
            if (alias and alias in s.quals) or (not alias and tname in s.quals and target.key in s.rel.datasets):
                tsrc = s
        if tsrc is None:
            tsrc = Source(frozenset({q for q in (alias, tname, target.name.lower()) if q}), self.base_relation(target))
            scope.sources.insert(0, tsrc)
        return scope, target, tsrc

    def _assignments(self, assigns: Sequence[exp.Expression], scope: Scope, env: Env) -> list[tuple[str, ColInfo]]:
        out: list[tuple[str, ColInfo]] = []
        for a in assigns:
            if not isinstance(a, exp.EQ) or not isinstance(a.this, exp.Column):
                continue
            rhs = a.expression
            origins, partial = self._origins(rhs, scope, env)
            text = rhs.sql(dialect=self.dialect, comments=False)
            if not isinstance(rhs, exp.Column):
                origins = add_step(origins, text)
            out.append((a.this.name, ColInfo(a.this.name, origins, partial, text)))
        return out

    def _update(self, node: exp.Update, result: StatementResult) -> StatementResult:
        result.kind = "update"
        tbl = node.this
        if not isinstance(tbl, exp.Table):
            return result
        extra: list[exp.Expression] = []
        frm = _arg(node, "from_", "from")
        if frm is not None:
            extra.append(frm)
        extra.extend(node.args.get("joins") or [])
        scope, target, _ = self._dml_scope(tbl, extra, Env())
        self.st.exclude_reads = frozenset({target.key})
        pairs = self._assignments(node.expressions, scope, Env())
        where = node.args.get("where")
        if where is not None:
            self._visit_subqueries(where, scope, Env())
        result.target = target
        self._write(target, "update")
        rel = Relation(tuple(ci for _, ci in pairs))
        self.emit_columns(rel, target, [n for n, _ in pairs], mode="update")
        return result

    def _delete(self, node: exp.Delete, result: StatementResult) -> StatementResult:
        result.kind = "delete"
        tbl = node.this
        if not isinstance(tbl, exp.Table):
            tables = node.args.get("tables") or []
            tbl = tables[0] if tables and isinstance(tables[0], exp.Table) else None
        if tbl is None:
            return result
        extra: list[exp.Expression] = []
        for j in tbl.args.get("joins") or []:
            extra.append(j)
        using = node.args.get("using")
        if isinstance(using, list):
            extra.extend(u for u in using if isinstance(u, exp.Expression))
        elif isinstance(using, exp.Expression):
            extra.append(using)
        frm = _arg(node, "from_", "from")
        if frm is not None:
            extra.append(frm)
        scope, target, _ = self._dml_scope(tbl, extra, Env())
        self.st.exclude_reads = frozenset({target.key})
        where = node.args.get("where")
        if where is not None:
            self._visit_subqueries(where, scope, Env())
        result.target = target
        self._write(target, "delete")
        return result

    def _merge(self, node: exp.Merge, result: StatementResult) -> StatementResult:
        result.kind = "merge"
        tbl = node.this
        if not isinstance(tbl, exp.Table):
            return result
        using = node.args.get("using")
        env = Env()
        scope = Scope()
        if using is not None:
            self._add_source(using, scope, env)
        source_scope = Scope(None, list(scope.sources))
        tgt_scope, target, _tsrc = self._dml_scope(tbl, [], env)
        full = Scope(None, [*tgt_scope.sources, *scope.sources])
        self.st.exclude_reads = frozenset({target.key})
        on = node.args.get("on")
        if on is not None:
            self._visit_subqueries(on, full, env)
        result.target = target
        self._write(target, "merge")
        src_rel = source_scope.sources[0].rel if source_scope.sources else EMPTY
        whens = node.args.get("whens")
        for w in (whens.expressions if whens is not None else []):
            cond = w.args.get("condition")
            if cond is not None:
                self._visit_subqueries(cond, full, env)
            then = w.args.get("then")
            if isinstance(then, exp.Update):
                if any(isinstance(e, exp.Star) for e in then.expressions):
                    self._merge_star(src_rel, target, "update", w)
                else:
                    pairs = self._assignments(then.expressions, full, env)
                    self.emit_columns(Relation(tuple(ci for _, ci in pairs)), target, [n for n, _ in pairs], mode="merge")
            elif isinstance(then, exp.Insert):
                if isinstance(then.this, exp.Star):
                    self._merge_star(src_rel, target, "insert", w)
                elif isinstance(then.this, exp.Tuple) and isinstance(then.expression, exp.Tuple):
                    names = [c.name for c in then.this.expressions]
                    cols_out: list[ColInfo] = []
                    for v in then.expression.expressions:
                        origins, partial = self._origins(v, full, env)
                        text = v.sql(dialect=self.dialect, comments=False)
                        cols_out.append(ColInfo("", add_step(origins, text) if not isinstance(v, exp.Column) else origins, partial, text))
                    self.emit_columns(Relation(tuple(cols_out)), target, names, mode="merge")
                elif isinstance(then.expression, exp.Tuple):
                    self.b.gap("unsupported_construct", target.name, "MERGE ... INSERT VALUES without a column list: the target columns are positional and not mapped", None, self.st.loc, self.analyzer)
        return result

    def _merge_star(self, src_rel: Relation, target: Dataset, which: str, when: exp.Expression) -> None:
        """``UPDATE SET *`` / ``INSERT *``: source columns map to target columns of the same name (Delta semantics)."""
        if src_rel.tail or src_rel.opaque or not src_rel.cols:
            names = ", ".join(sorted(self.datasets[t].name for t in src_rel.tail if t in self.datasets)) or "the MERGE source"
            self.b.gap("unsupported_construct", target.name, f"MERGE {which.upper()} * with a source of unknown schema ({names}): the column mapping is not available "
                       "(table-level lineage is recorded)", "Provide the source schema or list the columns explicitly.", self.st.loc, self.analyzer)
            if not src_rel.cols:
                return
        self.emit_columns(src_rel, target, [c.name for c in src_rel.cols], mode="merge")

    # ------------------------------------------------------------------------------------------------------------ robust fallback
    def _fallback_tables(self, node: exp.Expression) -> None:
        """Table-level lineage by a plain walk, used when the detailed analysis of a statement failed."""
        ctes = {c.alias.lower() for c in node.find_all(exp.CTE) if c.alias}
        target: exp.Table | None = None
        if isinstance(node, (exp.Insert, exp.Create, exp.Merge, exp.Update, exp.Delete)):
            t = node.this.this if isinstance(node.this, exp.Schema) else node.this
            target = t if isinstance(t, exp.Table) else None
        for t in node.find_all(exp.Table):
            if not isinstance(t.this, exp.Identifier) or t is target:
                continue
            if not t.args.get("db") and t.name.lower() in ctes:
                continue
            self.note_read(self.table_dataset([p for p in (_ident_name(t.args.get("catalog")), _ident_name(t.args.get("db")), _ident_name(t.this)) if p]))
        if target is not None and isinstance(target.this, exp.Identifier):
            self._write(self._target_dataset(target), "write")

    # ------------------------------------------------------------------------------------------------------------ expressions over a relation (selectExpr / expr)
    def project_expressions(self, exprs: Sequence[str], base: Relation, *, label: str = "", process: AssetRef | None = None,
                            loc: dict[str, Any] | None = None) -> Relation | None:
        """Evaluate SQL expression strings (``selectExpr("a", "b + 1 AS c", "*")``) against ``base``; ``None`` when one cannot be parsed (a gap is recorded)."""
        saved = self.st
        self.st = _Stmt(process=process or self.ctx.process or AssetRef.make("sql_query"), loc=dict(loc or {}), text=label or "; ".join(exprs), index=0, line=0)
        try:
            nodes: list[exp.Expression] = []
            for e in exprs:
                try:
                    if parse_hazard(e) is not None:
                        raise ValueError("the expression chains too many JOINs without conditions")
                    nodes.append(sqlglot.parse_one(e, read=self.dialect))
                except Exception as exc:  # noqa: BLE001
                    self.b.gap("parse_error", clip(e, 80) or "expression", f"cannot parse the expression: {str(exc).splitlines()[0][:160] if str(exc) else type(exc).__name__}",
                               "Fix the SQL expression.", self.st.loc, self.analyzer)
                    return None
            scope = Scope()
            scope.sources.append(Source(frozenset(), base))
            for q, _sub in base.quals:
                scope.sources[0].quals = scope.sources[0].quals | {q}
            if base.alias:
                scope.sources[0].quals = scope.sources[0].quals | {base.alias.lower()}
            rel = self._project(nodes, scope, Env())
            # sub-queries inside the expressions read tables
            return Relation(rel.cols, rel.tail, rel.datasets | base.datasets, base.quals, None, rel.opaque)
        finally:
            self._flush_reads()
            self.st = saved


def _rename(rel: Relation, names: Sequence[str]) -> Relation:
    """Apply a column alias list (``AS t(a, b)`` / ``WITH c(a, b)``) positionally."""
    if not names or not rel.positional_ok:
        return rel
    cols = tuple(ColInfo(names[i] if i < len(names) else c.name, c.origins, c.partial, c.expr) for i, c in enumerate(rel.cols))
    return Relation(cols, rel.tail, rel.datasets, rel.quals, rel.alias, rel.opaque)


def _flatten_setop(node: exp.SetOperation) -> list[tuple[str, exp.Expression]]:
    """``a UNION b UNION ALL c`` as ``[("", a), ("UNION", b), ("UNION", c)]`` without recursion (long chains are common)."""
    pairs: list[tuple[str, exp.Expression]] = []
    cur: exp.Expression = node
    while isinstance(cur, exp.SetOperation):
        op = "UNION" if isinstance(cur, exp.Union) else "INTERSECT" if isinstance(cur, exp.Intersect) else "EXCEPT"
        right = cur.args.get("expression")
        if right is not None:
            pairs.append((op, right))
        cur = cur.this
    pairs.append(("", cur))
    pairs.reverse()
    return pairs


# ================================================================================================================================== public API
def analyze_sql(sql: str, *, dialect: str = "spark", context: SqlContext | LineageContext | Mapping[str, Any] | None = None) -> Facts:
    """Analyse a SQL script and return its lineage as :class:`~olcf.intelligence.model.Facts`; never raises.

    ``dialect`` is ``spark``, ``duckdb`` or ``tsql`` (other sqlglot dialect names are accepted); ``context`` supplies the default lakehouse, the known table schemas
    and, optionally, the process asset (otherwise each statement becomes a ``sql_query`` asset keyed by the hash of its normalised text). See the module docstring for the
    constructs that are understood and the honesty rules that apply.
    """
    ctx = LineageContext.coerce(context)
    builder = FactBuilder(SQL, ctx)
    try:
        if not isinstance(sql, str):
            builder.gap("parse_error", "sql", "the SQL payload is not text", None, ctx.loc(), SQL)
            return builder.facts()
        session = SqlSession(ctx, builder, dialect)
        session.run(sql, process=ctx.process, loc=ctx.loc())
    except Exception as exc:  # noqa: BLE001 - analyzers never raise
        builder.gap("parse_error", "sql", f"SQL analysis failed unexpectedly ({type(exc).__name__}: {str(exc)[:160]})", "Report the statement to the OLCF maintainers.", ctx.loc(), SQL)
    return builder.facts()


@dataclass
class QueryOutputs:
    """The output columns of a query and where they come from, for consumers that present a query result (reports): ``columns`` maps each output name to the
    ``(table reference, column)`` pairs it is computed from; ``tables`` are the datasets read; ``facts`` hold the READS_FROM edges and gaps of the analysis."""

    columns: list[tuple[str, list[tuple[AssetRef, str]]]]
    tables: list[AssetRef]
    facts: Facts
    complete: bool


def query_outputs(sql: str, *, dialect: str = "duckdb", context: LineageContext | Mapping[str, Any] | None = None, process: AssetRef | None = None) -> QueryOutputs:
    """Analyse a (single) query and return its output columns with their source columns; the process is ``process`` (a report, a tile ...)."""
    ctx = LineageContext.coerce(context)
    builder = FactBuilder(SQL, ctx)
    session = SqlSession(ctx, builder, dialect, emit_query_assets=False)
    cols: list[tuple[str, list[tuple[AssetRef, str]]]] = []
    complete = False
    try:
        results = session.run(sql, process=process or ctx.process or AssetRef.make("sql_query"), loc=ctx.loc())
        rel = next((r.relation for r in reversed(results) if r.relation is not None), None)
        if rel is not None:
            complete = rel.positional_ok and not any(c.partial for c in rel.cols)
            for c in rel.cols:
                cols.append((c.name, [(session.datasets[ds].ref, col) for ds, col, _ in group_origins(c.origins) if ds in session.datasets]))
    except Exception as exc:  # noqa: BLE001
        builder.gap("parse_error", "sql", f"SQL analysis failed unexpectedly ({type(exc).__name__})", None, ctx.loc(), SQL)
    facts = builder.facts()
    tables = [e.dst for e in facts.edges if e.type == "READS_FROM"]
    return QueryOutputs(cols, tables, facts, complete)
