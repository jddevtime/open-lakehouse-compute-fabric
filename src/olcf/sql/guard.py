# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Statement parsing, classification, read-only enforcement and parameter binding.

The embedded DuckDB engine is protected twice: here (the statement is parsed and anything that is not a pure read is
rejected with an actionable message) and at the connection (external access disabled, configuration locked), so a
parser gap cannot turn into file-system or network access.

Parameters are bound as *typed literal AST nodes*, never by string concatenation.  ``:name`` cannot be parsed by sqlglot's
DuckDB dialect (it clashes with DuckDB's ``alias: expr`` syntax), so named markers are first swapped for a private
identifier by a small lexer that skips string literals, quoted identifiers and comments.
"""

from __future__ import annotations

import datetime as dt
import math
import re
from dataclasses import dataclass, field
from typing import Any

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError
from sqlglot.tokens import TokenType

from olcf.core.errors import BadRequest, Unsupported
from olcf.sql.model import PARAM_TYPES
from olcf.sql.spark_functions import SPARK_ALLOWED_FUNCTIONS

MAX_SQL_CHARS = 200_000
MAX_PARAM_STRING = 10_000
MAX_PARAM_LIST = 1_000
_DIALECT = {"duckdb": "duckdb", "spark": "spark"}
_MARK = "__olcf_p_"
_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

#: DuckDB table functions that read files; they are served by OLCF (authorised, provider-aware) instead of DuckDB itself.
FILE_FUNCTIONS = {
    "read_csv": "csv", "read_csv_auto": "csv", "read_parquet": "parquet", "parquet_scan": "parquet",
    "read_json": "json", "read_json_auto": "json", "read_ndjson": "json", "read_ndjson_auto": "json",
}
#: Functions that evaluate SQL text or touch the environment: they would bypass table authorisation.
DENIED_FUNCTIONS = {"query", "query_table", "glob", "read_text", "read_blob", "sniff_csv", "duckdb_secrets", "which_secret", "getenv",
                    "iceberg_scan", "delta_scan", "read_xlsx", "st_read", "sqlite_scan", "postgres_scan", "mysql_scan", "http_get", "http_post"}
#: Introspection of the engine / host (settings expose server paths); any ``duckdb_*`` / ``pragma_*`` function is refused, plus these.
DENIED_FUNCTION_PREFIXES = ("duckdb_", "pragma_")
DENIED_FUNCTIONS |= {"current_setting", "current_database", "current_schemas", "version", "getvariable"}
FORBIDDEN_NODES = {
    "Insert", "Update", "Delete", "Merge", "Create", "Drop", "Alter", "AlterTable", "Copy", "Set", "Use", "Attach", "Detach", "Install",
    "Pragma", "Command", "TruncateTable", "Grant", "Revoke", "Export", "Analyze", "LoadData", "Refresh", "Kill", "Commit", "Rollback", "Transaction",
}
WRITE_NODES = {"Insert", "Update", "Delete", "Merge", "Create", "Drop", "Alter", "AlterTable", "TruncateTable", "Copy"}

# --------------------------------------------------------------------------------------------- DuckDB: table functions are an allowlist
#: Table functions the DuckDB engine may use besides the file readers (which OLCF serves itself, see ``FILE_FUNCTIONS``). Everything else - ``parquet_metadata``,
#: ``parquet_schema``, ``glob``, ``sniff_csv``, extension scans - can name a host path and is refused, whether or not it is in ``DENIED_FUNCTIONS``.
DUCKDB_ALLOWED_TABLE_FUNCTIONS = {"range", "generate_series", "unnest", "json_each", "json_tree", "repeat_row", "generate_subscripts"}

# --------------------------------------------------------------------------------------------- Spark: host capabilities (review F02)
#: Spark SQL functions that give a statement more than data: JVM reflection (``reflect`` / ``java_method`` / ``try_reflect`` call any static method of any class on the
#: classpath), indirection that would hide a denied call (``call_function``, ``identifier``), descriptor files read from the host (``from_protobuf``) and
#: service/IO functions of other Spark distributions. These names are *explicitly* refused with a dedicated message; the actual boundary is the finite allowlist
#: :data:`olcf.sql.spark_functions.SPARK_ALLOWED_FUNCTIONS` (review C01): a function that is in neither set is refused too, as is every qualified call (``db.fn(x)``).
SPARK_DENIED_FUNCTIONS = {
    "reflect", "java_method", "try_reflect", "call_function", "try_call_function", "call_udf", "identifier", "from_protobuf", "to_protobuf", "schema_of_protobuf",
    "http_request", "secret", "list_secrets", "remote_sql", "python_udf", "scala_udf", "java_udf", "spark_connect",
}
SPARK_DENIED_FUNCTION_PREFIXES = ("read_", "http_", "ai_", "remote_")
#: The only table-valued functions a statement may select from. ``IDENTIFIER('...')``, ``read_files`` and friends could name any table or path after authorisation.
SPARK_ALLOWED_TABLE_FUNCTIONS = {"range", "explode", "explode_outer", "posexplode", "posexplode_outer", "inline", "inline_outer", "stack", "json_tuple",
                                 "variant_explode", "variant_explode_outer", "sequence"}
#: ``<datasource>.`<path>``` is Spark's file query: it reads any path the kernel's identity can reach, bypassing the lakehouse resolver and its authorisation.
SPARK_FILE_DATASOURCES = {"parquet", "csv", "json", "orc", "text", "avro", "delta", "binaryfile", "jdbc", "xml", "iceberg", "hudi", "kafka", "socket", "rate",
                          "libsvm", "image", "noop", "console", "memory", "arrow", "lance", "paimon"}
#: AST nodes with no place in a statement sent to the shared Spark kernel. ``QueryTransform`` is ``SELECT TRANSFORM(..) USING '<command>'`` (Hive script
#: transformation: runs a program), ``Directory`` is ``INSERT OVERWRITE [LOCAL] DIRECTORY '<path>'`` (writes anywhere), ``LocationProperty`` is ``LOCATION '<path>'``.
SPARK_DENIED_NODES = {"QueryTransform", "Directory", "LocationProperty", "Command", "Pragma", "Install", "Attach", "Detach", "Copy", "Export", "LoadData", "Set", "Use"}
_SPARK_DENIED_TOKEN_TEXT = {"recordreader", "recordwriter"}


@dataclass
class Statement:
    """A parsed, classified statement."""

    ast: exp.Expression
    kind: str                      # select | describe | show | summarize | explain | insert | ctas | create_view | update | delete | merge | ddl | unsupported
    read_only: bool
    dialect: str
    explain_prefix: str | None = None   # "EXPLAIN" / "EXPLAIN ANALYZE" when ast is the inner query
    markers: dict[str, str] = field(default_factory=dict)   # private identifier -> parameter name


# --------------------------------------------------------------------------------------------- lexer for parameter markers
def mark_parameters(sql: str, names: set[str]) -> tuple[str, dict[str, str], set[str]]:
    """Swap ``:name`` / ``$name`` markers for private identifiers.

    Only names in ``names`` (provided or declared) are swapped for ``:name`` (``:`` is ambiguous in some SQL); ``$name`` is
    unambiguous, so an unknown ``$name`` is reported back in the third element.  Returns ``(sql, marker->name, unknown_dollar)``.
    """
    out: list[str] = []
    markers: dict[str, str] = {}
    unknown: set[str] = set()
    i, n = 0, len(sql)
    while i < n:
        c = sql[i]
        if c in ("'", '"', "`"):
            j = i + 1
            while j < n:
                if sql[j] == c:
                    if j + 1 < n and sql[j + 1] == c:  # doubled quote escape
                        j += 2
                        continue
                    break
                if sql[j] == "\\" and c == "'" and j + 1 < n:
                    j += 2
                    continue
                j += 1
            out.append(sql[i:j + 1])
            i = j + 1
            continue
        if c == "-" and sql.startswith("--", i):
            j = sql.find("\n", i)
            j = n if j < 0 else j
            out.append(sql[i:j])
            i = j
            continue
        if c == "/" and sql.startswith("/*", i):
            j = sql.find("*/", i + 2)
            j = n if j < 0 else j + 2
            out.append(sql[i:j])
            i = j
            continue
        if c == "$" and sql.startswith("$$", i):
            j = sql.find("$$", i + 2)
            j = n if j < 0 else j + 2
            out.append(sql[i:j])
            i = j
            continue
        if c in (":", "$"):
            prev = sql[i - 1] if i else " "
            m = _IDENT.match(sql, i + 1)
            if m and not (c == ":" and (prev == ":" or prev.isalnum() or prev in "_])'\"")) and not (c == "$" and (prev.isalnum() or prev == "_")):
                name = m.group(0)
                if c == "$" and name not in names:
                    unknown.add(name)
                    out.append(sql[i:m.end()])
                elif name in names:
                    marker = f"{_MARK}{name}"
                    markers[marker] = name
                    out.append(marker)
                else:
                    out.append(sql[i:m.end()])
                i = m.end()
                continue
            if c == ":" and sql.startswith("::", i):
                out.append("::")
                i += 2
                continue
        out.append(c)
        i += 1
    return "".join(out), markers, unknown


# --------------------------------------------------------------------------------------------- parsing and classification
def parse_statement(sql: str, dialect: str = "duckdb", *, param_names: set[str] | None = None) -> Statement:
    """Parse exactly one statement.  Raises ``BadRequest`` for empty / oversized / multi-statement / unparsable SQL."""
    if not isinstance(sql, str) or not sql.strip():
        raise BadRequest("sql must not be empty")
    if len(sql) > MAX_SQL_CHARS:
        raise BadRequest(f"sql is limited to {MAX_SQL_CHARS} characters")
    d = _DIALECT.get(dialect, dialect)
    text, markers, unknown = mark_parameters(sql, param_names or set())
    if unknown:
        raise BadRequest(f"parameter(s) {', '.join('$' + u for u in sorted(unknown))} have no value", details={"missing": sorted(unknown)},
                         remediation="pass them in 'parameters' (named parameters only; positional ? is not supported)")
    if re.search(r"(?<![\w'\"])\?(?![\w'\"])", re.sub(r"'(?:[^']|'')*'", "''", text)):
        raise BadRequest("positional '?' parameters are not supported", remediation="use named parameters such as :region or $region")
    if d == "spark":
        scan_spark_text(text)
    try:
        stmts = [s for s in sqlglot.parse(text, read=d) if s is not None]
    except (SqlglotError, ValueError, TypeError, IndexError, KeyError, AttributeError) as exc:  # sqlglot's builders raise these (not SqlglotError) for malformed calls such as ``date_trunc()``
        stripped = re.sub(r"'(?:[^']|'')*'", "''", sql)
        loose = sorted(set(re.findall(r"(?<![:\w]):([A-Za-z_]\w*)", stripped)) - set(param_names or ()))
        if loose:
            raise BadRequest(f"cannot parse SQL; {', '.join(':' + x for x in loose)} looks like a named parameter without a value",
                             details={"dialect": d, "missing": loose}, remediation="pass a value for each named parameter in 'parameters'") from None
        raise BadRequest(f"cannot parse SQL: {(str(exc).splitlines() or [type(exc).__name__])[0]}", details={"dialect": d},
                         remediation=("send exactly one complete, supported statement (SELECT / WITH / SHOW / DESCRIBE / EXPLAIN, or INSERT / CREATE TABLE AS / MERGE / UPDATE / DELETE with sql.write); "
                                      "text that cannot be parsed is never forwarded, and session commands (SET, RESET, ADD JAR ...) belong in a notebook or job") if d == "spark" else None) from None
    if not stmts:
        raise BadRequest("sql contains no statement")
    if len(stmts) > 1:
        raise BadRequest("exactly one SQL statement per request is supported", remediation="run the statements one after the other")
    st = classify(stmts[0], d)
    st.markers = markers
    return st


def classify(ast: exp.Expression, dialect: str) -> Statement:
    """Decide what kind of statement this is and whether it is a pure read."""
    if isinstance(ast, exp.Command):
        word = str(ast.this).upper()
        rest = ast.expression.name if isinstance(ast.expression, exp.Literal) else ""
        if word == "EXPLAIN" and rest.strip():
            prefix = "EXPLAIN"
            if rest.upper().startswith("ANALYZE "):
                prefix, rest = "EXPLAIN ANALYZE", rest[8:]
            try:
                inner = sqlglot.parse_one(rest, read=dialect)
            except SqlglotError as exc:
                raise BadRequest(f"cannot parse the EXPLAINed statement: {str(exc).splitlines()[0]}") from None
            ist = classify(inner, dialect)
            return Statement(ist.ast, "explain" if ist.read_only else ist.kind, ist.read_only, dialect, explain_prefix=prefix)
        return Statement(ast, "unsupported", False, dialect)
    name = type(ast).__name__
    check_host_capabilities(ast, dialect)  # every statement kind, reads and writes alike: the engines behind this service are shared
    if isinstance(ast, exp.Query) or name in ("Values",):
        _check_nodes(ast, dialect)
        return Statement(ast, "select", True, dialect)
    if name == "Describe":
        _check_nodes(ast, dialect)
        return Statement(ast, "describe", True, dialect)
    if name == "Summarize":
        _check_nodes(ast, dialect)
        return Statement(ast, "summarize", True, dialect)
    if name == "Show":
        return Statement(ast, "show", True, dialect)
    if isinstance(ast, exp.Insert):
        return Statement(ast, "insert", False, dialect)
    if isinstance(ast, exp.Create):
        kind = str(ast.args.get("kind") or "").upper()
        if isinstance(ast.args.get("expression"), exp.Query):
            return Statement(ast, "create_view" if kind == "VIEW" else "ctas", False, dialect)
        return Statement(ast, "ddl", False, dialect)
    if isinstance(ast, exp.Update):
        return Statement(ast, "update", False, dialect)
    if isinstance(ast, exp.Delete):
        return Statement(ast, "delete", False, dialect)
    if isinstance(ast, exp.Merge):
        return Statement(ast, "merge", False, dialect)
    if name in WRITE_NODES:
        return Statement(ast, "ddl", False, dialect)
    return Statement(ast, "unsupported", False, dialect)


def _check_nodes(ast: exp.Expression, dialect: str = "duckdb") -> None:
    """A read must not contain write / session-changing nodes anywhere in its tree (and, for DuckDB, no host-reaching functions - Spark has its own rules, see ``_check_spark``)."""
    for node in ast.walk():
        n = type(node).__name__
        if n in FORBIDDEN_NODES:
            raise Unsupported(f"{n.upper()} is not allowed inside a read-only query", remediation="run only SELECT / WITH / DESCRIBE / SUMMARIZE / EXPLAIN statements here")
    if dialect == "duckdb":
        _check_duckdb(ast)


def _check_duckdb(ast: exp.Expression) -> None:
    """DuckDB: table functions are an allowlist (OLCF's file readers + generators), scalar functions a denylist of host-reaching ones."""
    for node in ast.walk():
        if isinstance(node, exp.Table) and isinstance(node.this, exp.Func):
            fn = _func_name(node.this)
            if fn in DENIED_FUNCTIONS or fn.startswith(DENIED_FUNCTION_PREFIXES):
                raise Unsupported(f"table function {fn}() is not allowed", remediation="query lakehouse tables as <lakehouse>.<table> or read files with read_csv/read_parquet/read_json on Files/ paths")
            if fn.startswith("read_") and fn not in FILE_FUNCTIONS:
                raise Unsupported(f"table function {fn}() is not supported", remediation=f"supported file readers: {', '.join(sorted(FILE_FUNCTIONS))}")
            if fn not in FILE_FUNCTIONS and fn not in DUCKDB_ALLOWED_TABLE_FUNCTIONS:  # an allowlist: any other table function may name a host path (parquet_metadata, ...)
                raise Unsupported(f"table function {fn}() is not supported", remediation=f"query lakehouse tables as <lakehouse>.<table>; allowed table functions: "
                                  f"{', '.join(sorted(DUCKDB_ALLOWED_TABLE_FUNCTIONS | set(FILE_FUNCTIONS)))}")
        elif isinstance(node, exp.Table) and isinstance(node.this, exp.Literal):
            raise Unsupported("a quoted file name cannot be used as a table", remediation="read files with read_csv('Files/...') / read_parquet('Files/...') / read_json('Files/...')")
        elif isinstance(node, exp.Func) and (_func_name(node) in DENIED_FUNCTIONS or _func_name(node).startswith(DENIED_FUNCTION_PREFIXES)):
            raise Unsupported(f"function {_func_name(node)}() is not allowed")


def check_host_capabilities(ast: exp.Expression, dialect: str) -> None:
    """Refuse constructs that give a statement host capabilities beyond reading the lakehouse tables it was authorised for (review F02).

    Runs for every statement kind and every role: the DuckDB connection and the pooled Spark kernels are shared, so what a statement may *do* must not depend on who sends
    it (people who need reflection or a script run a job). Spark: script transformation, reflection and function indirection, file-query table references
    (``parquet.`/path```), non-allowlisted table functions, ``LOCATION`` / ``DIRECTORY`` targets. DuckDB: see :func:`_check_nodes` (also applied here to writes).
    """
    if dialect == "spark":
        _check_spark(ast)
    else:
        _check_duckdb(ast)


def _spark_function_denied(name: str) -> bool:
    return name in SPARK_DENIED_FUNCTIONS or name.startswith(SPARK_DENIED_FUNCTION_PREFIXES)


_FUNC_HEAD = re.compile(r"\s*([A-Za-z_][A-Za-z0-9_]*)\s*\(")


def _spark_rendered_name(node: exp.Expression) -> str:
    """The Spark spelling a typed sqlglot node renders to (``TryCast`` -> ``cast``, ``TimestampTrunc`` -> ``date_trunc``, ``ApproxDistinct`` -> ``approx_count_distinct``); ``""`` if it
    does not render as ``name(...)``."""
    try:
        m = _FUNC_HEAD.match(node.sql(dialect="spark"))
    except Exception:  # noqa: BLE001 - an unrenderable node is simply not matched; the caller then rejects it
        return ""
    return m.group(1).lower() if m else ""


def _is_qualified_call(node: exp.Expression) -> bool:
    """``mydb.my_udf(x)`` / ``spark_catalog.default.f(x)`` parse as ``Dot(<qualifier>, Func)``: a catalog lookup of a (user-defined) function, never a built-in."""
    parent = node.parent
    return isinstance(parent, exp.Dot) and parent.expression is node


def _check_spark_function(node: exp.Func) -> None:
    """Allowlist check of one function call in a statement for the Spark kernel (review C01).

    Refused: qualified calls (always), the explicitly denied names (dedicated message), and every name that is not in :data:`SPARK_ALLOWED_FUNCTIONS`.  Typed nodes are accepted when
    their own name or the Spark spelling they render to is allowed; table-valued functions are checked against ``SPARK_ALLOWED_TABLE_FUNCTIONS`` by the caller.
    """
    if isinstance(node, exp.Connector):
        return  # AND / OR / XOR are operators that sqlglot models as functions
    fn = _func_name(node)
    if _spark_function_denied(fn):
        raise Unsupported(f"function {fn}() is not allowed in SQL statements", remediation="it reaches the host (JVM reflection, files or services) instead of lakehouse data; use a notebook or job if you need it")
    if _is_qualified_call(node):
        raise Unsupported(f"qualified function call {node.parent.sql(dialect='spark')[:80]} is not allowed in SQL statements",
                          remediation="only Spark's built-in functions can be called, unqualified; user-defined and catalog functions run in a notebook or job")
    if isinstance(node.parent, exp.Table) and node.parent.this is node:
        return  # a table-valued function: judged against SPARK_ALLOWED_TABLE_FUNCTIONS
    if fn in SPARK_ALLOWED_FUNCTIONS:
        return
    shown = fn
    if not isinstance(node, exp.Anonymous):
        rendered = _spark_rendered_name(node)
        if _spark_function_denied(rendered):
            raise Unsupported(f"function {rendered}() is not allowed in SQL statements", remediation="it reaches the host (JVM reflection, files or services) instead of lakehouse data; use a notebook or job if you need it")
        if rendered in SPARK_ALLOWED_FUNCTIONS:
            return
        shown = rendered or fn
    raise Unsupported(f"function {shown}() is not an approved Spark SQL function",
                      remediation="SQL statements may call only the approved built-in functions (olcf.sql.spark_functions.SPARK_ALLOWED_FUNCTIONS); custom, UDF and unlisted functions run in a notebook or job")


def _check_spark(ast: exp.Expression) -> None:
    for node in ast.walk():
        n = type(node).__name__
        if n == "QueryTransform":
            raise Unsupported("script transformation (TRANSFORM / MAP / REDUCE ... USING '<command>') is not allowed", remediation="express the logic in SQL, or run a notebook or job")
        if isinstance(node, (exp.Create, exp.Drop)) and str(node.args.get("kind") or "").upper() in ("FUNCTION", "PROCEDURE", "MACRO", "SECRET", "EXTENSION", "CATALOG", "CONNECTION"):
            verb = "CREATE" if isinstance(node, exp.Create) else "DROP"
            raise Unsupported(f"{verb} {str(node.args.get('kind')).upper()} is not allowed in a SQL statement", remediation="functions and catalogs are managed outside SQL requests; register libraries in the runtime environment")
        if n in SPARK_DENIED_NODES:
            raise Unsupported(f"{n.upper()} is not allowed in a SQL statement", remediation="read lakehouse tables as <lakehouse>.<table>; write through a Delta table, never to a path")
        if isinstance(node, exp.Table):
            if isinstance(node.this, exp.Func):
                fn = _func_name(node.this)
                if node.args.get("db") or node.args.get("catalog"):
                    raise Unsupported(f"qualified table function {fn}() is not allowed", remediation=f"query lakehouse tables as <lakehouse>.<table>; allowed table functions: {', '.join(sorted(SPARK_ALLOWED_TABLE_FUNCTIONS))}")
                if fn not in SPARK_ALLOWED_TABLE_FUNCTIONS and _spark_rendered_name(node.this) not in SPARK_ALLOWED_TABLE_FUNCTIONS:  # sqlglot types ``sequence(..)`` as GenerateSeries
                    raise Unsupported(f"table function {fn}() is not allowed", remediation=f"query lakehouse tables as <lakehouse>.<table>; allowed table functions: {', '.join(sorted(SPARK_ALLOWED_TABLE_FUNCTIONS))}")
            else:
                _check_spark_table(node)
        elif isinstance(node, exp.Func):
            _check_spark_function(node)


def _check_spark_table(t: exp.Table) -> None:
    parts = [p for p in (t.args.get("catalog"), t.args.get("db"), t.this) if isinstance(p, exp.Identifier)]
    for p in parts:
        if "/" in p.name or "\\" in p.name or "://" in p.name:
            raise Unsupported(f"'{p.name}' looks like a path, not a table", remediation="read lakehouse tables as <lakehouse>.<table>; files via Files/ in a notebook or the file readers of the DuckDB engine")
    cat, db = t.args.get("catalog"), t.args.get("db")
    if isinstance(db, exp.Identifier) and not cat and (db.name.lower() in SPARK_FILE_DATASOURCES or "." in db.name):
        raise Unsupported(f"'{db.name}.<path>' is a direct file query, not a lakehouse table", remediation="read lakehouse tables as <lakehouse>.<table> (a lakehouse cannot be named after a data source)")


def scan_spark_text(text: str) -> None:
    """Token-level pre-check of Spark SQL text, independent of the parser (defence in depth, and it also covers what the parser would drop or reject).

    Refuses: ``${...}`` (Spark substitutes ``${env:X}``, ``${system:X}`` and conf values into the statement text *before* parsing), ``USING '<string>'`` (script
    transformation; a join's ``USING (cols)`` and ``CREATE TABLE ... USING parquet`` are not followed by a string), ``RECORDREADER`` / ``RECORDWRITER`` and a call of a
    denied function written in any case, back-quoted, or with comments in between. Unreadable text fails closed.
    """
    if "${" in text:
        raise Unsupported("variable substitution (${...}) is not allowed in SQL statements", remediation="Spark would substitute environment variables, system properties and configuration into the text; pass values as :parameters")
    try:
        tokens = sqlglot.tokenize(text, read="spark")
    except SqlglotError as exc:
        raise BadRequest(f"cannot parse SQL: {str(exc).splitlines()[0]}", details={"dialect": "spark"}) from None
    for i, tok in enumerate(tokens):
        nxt = tokens[i + 1] if i + 1 < len(tokens) else None
        if tok.token_type == TokenType.USING and nxt is not None and nxt.token_type == TokenType.STRING:
            raise Unsupported("script transformation (... USING '<command>') is not allowed", remediation="express the logic in SQL, or run a notebook or job")
        if tok.token_type != TokenType.STRING:
            word = tok.text.lower()
            if word in _SPARK_DENIED_TOKEN_TEXT:
                raise Unsupported(f"{tok.text.upper()} (script transformation) is not allowed")
            if nxt is not None and nxt.token_type == TokenType.L_PAREN and _spark_function_denied(word):
                raise Unsupported(f"function {word}() is not allowed in SQL statements",
                                  remediation="it reaches the host (JVM reflection, files or services) instead of lakehouse data; use a notebook or job if you need it")


def check_final_sql(sql: str, dialect: str) -> None:
    """The text that is really sent to the engine (parameters bound, AST rendered). For Spark the token pre-check runs again on it: a *value* bound into the
    statement (``${env:SECRET}`` as a parameter) is substituted by Spark just like text typed by the author."""
    if dialect == "spark":
        scan_spark_text(sql)


#: What ``check_spark_sql`` lets reach a kernel: reads, plus the DML / DDL the SQL service also routes to the Spark engine (those need ``sql.write``, which the caller checks).
#: Everything else the parser classifies as ``unsupported`` is a *command* - ``ADD JAR`` / ``ADD FILE`` / ``LIST FILE`` (load code or read host files), ``RESET`` / ``SET`` (change the
#: shared session's configuration), ``CACHE TABLE`` / ``UNCACHE`` / ``CLEAR CACHE`` (shared kernel state), ``CALL`` ... - and is refused for EVERY caller, writers included.
SPARK_ALLOWED_KINDS = frozenset({"select", "describe", "summarize", "show", "explain", "insert", "ctas", "create_view", "ddl", "update", "delete", "merge"})


def check_spark_sql(sql: str, *, strict: bool) -> Statement:
    """Guard for callers that hand Spark SQL to a kernel without going through the SQL service (``ComputeService.run_spark_sql``).

    Every caller, ``strict`` or not, needs **one fully parsed, supported statement** (review C01): text the parser cannot read, several statements, a session command (``ADD JAR``,
    ``ADD FILE``, ``RESET``, ``CACHE TABLE``, ... - :data:`SPARK_ALLOWED_KINDS`), a host capability (:func:`scan_spark_text`, :func:`check_host_capabilities`: denied or unapproved or
    qualified functions, file queries, ...) are all refused.  There is no pass-through for text that could not be classified, for any role.  ``strict`` (the caller may not write)
    additionally requires the statement to be a read.  Returns the parsed :class:`Statement`.
    """
    scan_spark_text(sql)
    st = _as_spark_read(parse_statement(sql, "spark"))  # BadRequest: empty / unparsable / several statements
    _refuse_spark_commands(st, sql)
    if strict and not st.read_only:
        raise Unsupported(f"only read-only statements are allowed here ({st.kind})")
    return st


def _as_spark_read(st: Statement) -> Statement:
    """Spark's ``SHOW TABLES|DATABASES|COLUMNS|...`` reach sqlglot as a generic ``Command``; they only read catalog metadata and are what the SQL surface has always offered
    (the historic ``READ_ONLY_SQL`` first-word test), so they are classified as the read ``show`` here - the one command that is."""
    if isinstance(st.ast, exp.Command) and str(st.ast.this).upper() == "SHOW":
        return Statement(st.ast, "show", True, st.dialect, st.explain_prefix, st.markers)
    return st


def _refuse_spark_commands(st: Statement, sql: str) -> None:
    if st.kind not in SPARK_ALLOWED_KINDS:
        try:
            word = sqlglot.tokenize(sql, read="spark")[0].text.upper()
        except (SqlglotError, IndexError):
            word = "this"
        raise Unsupported(f"{word} statements are not allowed in a SQL request (session commands, ADD JAR / ADD FILE, CACHE / UNCACHE, RESET, SET ... change or load into the shared Spark session)",
                          remediation="send SELECT / WITH / SHOW / DESCRIBE / EXPLAIN, or INSERT / CREATE TABLE / MERGE / UPDATE / DELETE with sql.write; use a notebook or job for session or library setup")


def require_read_only(st: Statement, engine: str) -> None:
    """Raise an actionable error unless ``st`` may run on ``engine`` without write permissions."""
    if st.read_only:
        return
    if st.kind == "unsupported":
        raise Unsupported("this statement type is not supported by the SQL service",
                          remediation="supported: SELECT, WITH, VALUES, DESCRIBE, SUMMARIZE, SHOW, EXPLAIN; with the Spark engine also INSERT, CREATE TABLE AS, MERGE, UPDATE, DELETE")
    if engine == "duckdb":
        raise Unsupported(f"the DuckDB engine is read-only and cannot run {st.kind.upper().replace('_', ' ')} statements",
                          remediation="submit the statement with engine='spark' (needs sql.write and lakehouse.write), or write the table from a notebook/job",
                          details={"engine": "duckdb", "statement": st.kind})


# --------------------------------------------------------------------------------------------- sources
def _func_name(fn: exp.Expression) -> str:
    if isinstance(fn, exp.Anonymous):
        return str(fn.name).lower()
    try:
        return str(fn.sql_name()).lower()
    except Exception:  # noqa: BLE001
        return type(fn).__name__.lower()


@dataclass
class Use:
    """A table or file reference found in a statement."""

    node: exp.Table
    kind: str                       # table | file
    parts: tuple[str, ...] = ()     # for tables: catalog.schema.table parts as written
    paths: list[str] = field(default_factory=list)       # for files: literal paths
    fmt: str | None = None
    options: dict[str, Any] = field(default_factory=dict)
    alias: str | None = None

    @property
    def display(self) -> str:
        return ".".join(self.parts) if self.kind == "table" else (self.paths[0] if self.paths else "?")


def find_uses(ast: exp.Expression, *, skip_nodes: set[int] | None = None) -> list[Use]:
    """Table / file references that are not CTE names, in document order."""
    cte_names = {c.alias.lower() for c in ast.find_all(exp.CTE) if c.alias}
    uses: list[Use] = []
    for t in ast.find_all(exp.Table):
        if skip_nodes and id(t) in skip_nodes:
            continue
        this = t.this
        if isinstance(this, exp.Func):
            fn = _func_name(this)
            if fn in FILE_FUNCTIONS:
                paths, options = _file_args(this)
                if not paths:
                    raise BadRequest(f"{fn}() needs a literal path", remediation="e.g. read_csv('Files/raw/sales.csv', header=true)")
                uses.append(Use(t, "file", paths=paths, fmt=FILE_FUNCTIONS[fn], options=options, alias=t.alias or None))
            continue
        if not t.name:
            continue
        parts = tuple(p.name for p in (t.args.get("catalog"), t.args.get("db"), t.this) if isinstance(p, exp.Identifier) and p.name)
        if len(parts) == 1 and parts[0].lower() in cte_names:
            continue
        uses.append(Use(t, "table", parts=parts, alias=t.alias or None))
    return uses


def _file_args(fn: exp.Expression) -> tuple[list[str], dict[str, Any]]:
    paths: list[str] = []
    options: dict[str, Any] = {}
    args = ([fn.args["this"]] if isinstance(fn.args.get("this"), exp.Expression) else []) + list(fn.expressions)
    for a in args:
        if isinstance(a, exp.Literal) and a.is_string and not paths:
            paths.append(a.this)
        elif isinstance(a, (exp.Array, exp.Tuple)) and not paths:
            paths.extend(x.this for x in a.expressions if isinstance(x, exp.Literal) and x.is_string)
        elif isinstance(a, (exp.EQ, exp.PropertyEQ, exp.Kwarg)):
            key = (a.this.name if isinstance(a.this, exp.Expression) else str(a.this)).lower()
            val = a.expression
            options[key] = _literal_value(val)
    return paths, options


def _literal_value(e: exp.Expression) -> Any:
    if isinstance(e, exp.Boolean):
        return bool(e.this)
    if isinstance(e, exp.Literal):
        return e.this if e.is_string else (int(e.this) if re.fullmatch(r"-?\d+", e.this) else float(e.this))
    return e.sql()


# --------------------------------------------------------------------------------------------- parameters
def coerce_param(name: str, value: Any, ptype: str | None) -> Any:
    """Validate/convert one parameter value to its declared (or inferred) type."""
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        if len(value) > MAX_PARAM_LIST:
            raise BadRequest(f"parameter '{name}' has more than {MAX_PARAM_LIST} elements")
        return [coerce_param(name, v, ptype) for v in value]
    try:
        if ptype in (None, ""):
            if isinstance(value, dt.datetime):
                ptype = "timestamp"
            elif isinstance(value, dt.date):
                ptype = "date"
            elif isinstance(value, bool | int | float | str):
                ptype = "boolean" if isinstance(value, bool) else "int" if isinstance(value, int) else "number" if isinstance(value, float) else "string"
            else:
                raise TypeError(type(value).__name__)
        if ptype == "string":
            s = str(value)
            if len(s) > MAX_PARAM_STRING:
                raise ValueError("too long")
            return s
        if ptype == "int":
            if isinstance(value, bool) or (isinstance(value, float) and not value.is_integer()):
                raise ValueError("not an integer")
            return int(value)
        if ptype == "number":
            f = float(value)
            if not math.isfinite(f):
                raise ValueError("not finite")
            return int(f) if isinstance(value, int) and not isinstance(value, bool) else f
        if ptype == "boolean":
            if isinstance(value, str):
                if value.lower() in ("true", "false"):
                    return value.lower() == "true"
                raise ValueError("not a boolean")
            return bool(value)
        if ptype == "date":
            if isinstance(value, dt.datetime):
                return value.date()
            if isinstance(value, dt.date):
                return value
            text = str(value)
            if len(text) < 10:
                raise ValueError("expected YYYY-MM-DD")
            return dt.date.fromisoformat(text[:10])
        if ptype == "timestamp":
            if isinstance(value, dt.datetime):
                return value
            if isinstance(value, dt.date):
                return dt.datetime.combine(value, dt.time())
            return dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise BadRequest(f"parameter '{name}' is not a valid {ptype or 'value'}: {exc}", details={"parameter": name, "type": ptype}) from None
    raise BadRequest(f"unknown parameter type '{ptype}'", details={"supported": list(PARAM_TYPES)})


def _literal(value: Any, *, in_list: bool = False) -> exp.Expression:
    if value is None:
        return exp.Null()
    if isinstance(value, bool):
        return exp.Boolean(this=value)
    if isinstance(value, int):
        return exp.Literal.number(value) if value >= 0 else exp.Neg(this=exp.Literal.number(-value))
    if isinstance(value, float):
        return exp.Literal.number(repr(value)) if value >= 0 else exp.Neg(this=exp.Literal.number(repr(-value)))
    if isinstance(value, dt.datetime):
        return exp.Cast(this=exp.Literal.string(value.isoformat(sep=" ")), to=exp.DataType.build("TIMESTAMP"))
    if isinstance(value, dt.date):
        return exp.Cast(this=exp.Literal.string(value.isoformat()), to=exp.DataType.build("DATE"))
    if isinstance(value, list):
        items = [_literal(v) for v in value]
        return exp.Tuple(expressions=items) if in_list else exp.Array(expressions=items)
    return exp.Literal.string(str(value))


def bind_parameters(st: Statement, values: dict[str, Any], specs: list[dict[str, Any]] | None) -> tuple[Statement, list[str]]:
    """Replace parameter markers in ``st.ast`` with typed literals.  Returns the statement and warnings (e.g. unused parameters)."""
    types = {s["name"]: s.get("type") for s in specs or []}
    used: set[str] = set()
    ast = st.ast
    if st.markers:
        for col in list(ast.find_all(exp.Column)):
            marker = col.name
            if not col.table and marker in st.markers:
                name = st.markers[marker]
                val = coerce_param(name, values[name], types.get(name))
                parent = col.parent
                lit = _literal(val, in_list=isinstance(parent, exp.In) and isinstance(val, list))
                if isinstance(val, list) and isinstance(parent, exp.In):
                    parent.set("field", lit)
                    parent.set("expressions", [])
                    parent.set("query", None)
                else:
                    col.replace(lit)
                used.add(name)
        for ident in list(ast.find_all(exp.Identifier)):  # markers used in other positions (e.g. LIMIT :n)
            if ident.name in st.markers:
                raise BadRequest(f"parameter ':{st.markers[ident.name]}' is used where only an expression is allowed (e.g. as a table or column name)",
                                 remediation="parameters bind values; use them in WHERE, LIMIT, SELECT lists or IN (...)")
    warnings = [f"parameter '{k}' is not used by the statement" for k in values if k not in used and k not in st.markers.values()]
    return st, warnings


def resolve_parameter_values(provided: dict[str, Any], specs: list[dict[str, Any]] | None) -> dict[str, Any]:
    """Apply declared defaults, reject unknown names when specs exist, and require every declared parameter to have a value."""
    out = dict(provided or {})
    if specs:
        declared = {s["name"] for s in specs}
        unknown = sorted(set(out) - declared)
        if unknown:
            raise BadRequest(f"unknown parameter(s): {', '.join(unknown)}", details={"declared": sorted(declared)})
        for s in specs:
            if s["name"] in out:
                continue
            if s.get("default") is not None:
                out[s["name"]] = s["default"]
            elif not s.get("required", "default" not in s):     # an explicit "default": null makes the parameter optional (binds NULL)
                out[s["name"]] = None
        missing = sorted(n for n in declared if n not in out)
        if missing:
            raise BadRequest(f"missing value for parameter(s): {', '.join(missing)}", details={"declared": sorted(declared), "missing": missing})
    for k in out:
        if not isinstance(k, str) or not _IDENT.fullmatch(k):
            raise BadRequest(f"invalid parameter name '{k}'", remediation="use letters, digits and underscores")
    return out


def validate_param_specs(specs: Any) -> list[dict[str, Any]]:
    """Validate the ``parameters`` list of a saved query definition."""
    if specs is None:
        return []
    if not isinstance(specs, list):
        raise BadRequest("parameters must be a list of {name, type, default?, required?}")
    out, seen = [], set()
    for s in specs:
        if not isinstance(s, dict) or not isinstance(s.get("name"), str) or not _IDENT.fullmatch(s["name"]):
            raise BadRequest("each parameter needs an identifier name")
        if s["name"] in seen:
            raise BadRequest(f"duplicate parameter '{s['name']}'")
        seen.add(s["name"])
        t = s.get("type", "string")
        if t not in PARAM_TYPES:
            raise BadRequest(f"parameter '{s['name']}' has unknown type '{t}'", details={"supported": list(PARAM_TYPES)})
        if s.get("default") is not None:
            coerce_param(s["name"], s["default"], t)
        out.append({"name": s["name"], "type": t, "default": s.get("default"), "required": bool(s.get("required", "default" not in s))})
    return out


def render(ast: exp.Expression, dialect: str, prefix: str | None = None) -> str:
    sql = ast.sql(dialect=_DIALECT.get(dialect, dialect))
    return f"{prefix} {sql}" if prefix else sql
