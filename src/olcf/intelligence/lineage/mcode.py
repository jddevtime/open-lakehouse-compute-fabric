# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Static analysis of Power Query M: which data sources a query reads and which source column feeds which output column.

:func:`analyze_m` ``(text, *, ctx, resolver) -> MResult`` parses the query (``let ... in``, records, lists, function calls, ``each`` / lambdas, field and item access,
operators) and *abstractly evaluates* it: every step is a table state that remembers, for each output column, the source columns it comes from. Nothing is executed, no
connection is opened and no credential is read.

Sources recognised
------------------
``Sql.Database`` / ``Sql.Databases`` (with ``{[Schema=..,Item=..]}[Data]`` navigation, the ``Query=`` option and ``Value.NativeQuery`` whose SQL text is analysed with the T-SQL
analyzer), ``Lakehouse.Contents`` (``workspaceId`` / ``lakehouseId`` / ``Id`` navigation), ``AzureStorage.DataLakeContents`` / ``AzureStorage.BlobContents`` / ``AzureStorage.Blobs``,
``File.Contents`` / ``Folder.Files`` / ``Folder.Contents`` wrapped by ``Csv.Document`` / ``Excel.Workbook`` / ``Json.Document`` / ``Parquet.Document``, ``Web.Contents`` / ``Web.Page`` /
``Web.BrowserContents`` / ``OData.Feed``, ``SharePoint.Files`` / ``SharePoint.Contents`` / ``SharePoint.Tables``, ``AnalysisServices.Database``, ``PowerPlatform.Dataflows`` /
``PowerBI.Dataflows`` and the generic database connectors (``Oracle.Database``, ``PostgreSQL.Database``, ``MySQL.Database``, ``Snowflake.Databases``, ``Odbc.DataSource`` ...) as ``external``
assets. Parameters and shared expressions (``let Source = Param``, ``Source = OtherQuery``) are followed through the ``resolver``.

Column tracking
---------------
Through ``Table.RenameColumns``, ``RemoveColumns``, ``SelectColumns``, ``ReorderColumns``, ``AddColumn``, ``TransformColumns``, ``TransformColumnTypes``, ``ReplaceValue``, ``DuplicateColumn``,
``AddIndexColumn``, ``Group``, ``Combine`` (same-name columns), and the row-only functions (``SelectRows``, ``Sort``, ``Distinct``, ``FirstN``, ``Skip``, ``PromoteHeaders`` ...).
``Table.Join`` / ``NestedJoin`` / ``Expand*`` / ``Pivot`` / ``Unpivot`` / ``Transpose`` / ``Split*`` and unknown table functions make the output columns unknown: the tables read are still
reported and :attr:`MResult.reason` says why no column lineage is available. A name that is not a literal (or a parameter with a default) is a ``dynamic`` source that is reported,
never guessed. Query folding is not modelled.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

from olcf.intelligence.lineage.base import (
    MCODE,
    FactBuilder,
    LineageContext,
    classify_path,
    fabric_table_ref,
    redact_secrets,
    strip_uri_secrets,
)
from olcf.intelligence.model import AssetRef, clip, looks_like_guid

MAX_M_CHARS = 1_000_000
MAX_M_DEPTH = 60
MAX_M_STEPS = 20_000


# ---------------------------------------------------------------------------------------------------------------------- tokens / AST
class MParseError(Exception):
    """The M text cannot be parsed; the message says where."""


@dataclass(frozen=True)
class MTok:
    """A token of M: ``kind`` is ``id`` | ``str`` | ``num`` | ``op`` | ``kw``; ``start``/``end`` are offsets in the source text."""

    kind: str
    value: str
    start: int
    end: int


_KEYWORDS = frozenset({"let", "in", "each", "if", "then", "else", "try", "otherwise", "meta", "and", "or", "not", "as", "is", "type", "true", "false", "null", "error", "section",
                       "shared", "_"})
_NUM = re.compile(r"0[xX][0-9a-fA-F]+|\d+\.?\d*(?:[eE][+-]?\d+)?|\.\d+(?:[eE][+-]?\d+)?")
_IDENT = re.compile(r"[^\W\d]\w*(?:\.[^\W\d]\w*)*")  # M identifiers may contain any Unicode letter or digit


def mtokenize(text: str) -> list[MTok]:
    """Tokenize M. Raises :class:`MParseError` for an unterminated string or comment."""
    toks: list[MTok] = []
    i, n = 0, len(text)
    while i < n:
        ch = text[i]
        if ch.isspace():
            i += 1
        elif text.startswith("//", i):
            j = text.find("\n", i)
            i = n if j < 0 else j
        elif text.startswith("/*", i):
            j = text.find("*/", i + 2)
            if j < 0:
                raise MParseError("unterminated comment")
            i = j + 2
        elif ch == '"' or (ch == "#" and text.startswith('#"', i)):
            start = i
            i += 2 if ch == "#" else 1
            buf = []
            while i < n:
                if text[i] == '"':
                    if i + 1 < n and text[i + 1] == '"':
                        buf.append('"')
                        i += 2
                        continue
                    break
                buf.append(text[i])
                i += 1
            else:
                raise MParseError("unterminated string")
            i += 1
            toks.append(MTok("id" if ch == "#" else "str", "".join(buf), start, i))
        elif ch == "#":
            m = re.match(r"#[A-Za-z]+", text[i:])
            if m:
                toks.append(MTok("id", m.group(0), i, i + len(m.group(0))))
                i += len(m.group(0))
            else:
                raise MParseError(f"unexpected '#' at offset {i}")
        elif ch.isalpha() or ch == "_":
            m = _IDENT.match(text, i)
            if m is None:
                raise MParseError(f"unexpected character {ch!r} at offset {i}")
            word = m.group(0)
            toks.append(MTok("kw" if word in _KEYWORDS else "id", word, i, i + len(word)))
            i += len(word)
        elif ch.isdigit() or (ch == "." and i + 1 < n and text[i + 1].isdigit()):
            m = _NUM.match(text, i)
            if m is None:
                raise MParseError(f"unexpected character {ch!r} at offset {i}")
            toks.append(MTok("num", m.group(0), i, i + len(m.group(0))))
            i += len(m.group(0))
        else:
            two = text[i:i + 2]
            if two in ("=>", "<=", ">=", "<>", "..", "??"):
                toks.append(MTok("op", two, i, i + 2))
                i += 2
            elif text.startswith("...", i):
                toks.append(MTok("op", "...", i, i + 3))
                i += 3
            else:
                toks.append(MTok("op", ch, i, i + 1))
                i += 1
    return toks


@dataclass
class N:
    """A node of the (partial) M syntax tree. ``kind``: ``let`` ``call`` ``id`` ``str`` ``num`` ``lit`` ``record`` ``list`` ``index`` ``field`` ``each`` ``lambda`` ``bin`` ``if`` ``other``;
    ``a`` / ``b`` / ``items`` / ``name`` hold the parts, ``start`` / ``end`` the source span."""

    kind: str
    name: str = ""
    a: N | None = None
    b: N | None = None
    items: list[N] = field(default_factory=list)
    fields: list[tuple[str, N]] = field(default_factory=list)
    params: list[str] = field(default_factory=list)
    value: Any = None
    start: int = 0
    end: int = 0


class _Parser:
    def __init__(self, text: str) -> None:
        self.text = text
        self.toks = mtokenize(text)
        self.i = 0
        self.depth = 0

    # -- helpers
    def peek(self, k: int = 0) -> MTok | None:
        return self.toks[self.i + k] if self.i + k < len(self.toks) else None

    def next(self) -> MTok:
        t = self.peek()
        if t is None:
            raise MParseError("unexpected end of query")
        self.i += 1
        return t

    def is_op(self, v: str, k: int = 0) -> bool:
        t = self.peek(k)
        return t is not None and t.kind == "op" and t.value == v

    def is_kw(self, v: str, k: int = 0) -> bool:
        t = self.peek(k)
        return t is not None and t.kind == "kw" and t.value == v

    def expect_op(self, v: str) -> MTok:
        t = self.next()
        if t.kind != "op" or t.value != v:
            raise MParseError(f"expected '{v}' at offset {t.start}, found '{t.value}'")
        return t

    def span(self, node: N, start: int) -> N:
        node.start = start
        node.end = self.toks[self.i - 1].end if self.i else start
        return node

    # -- grammar
    def parse(self) -> N:
        e = self.expr()
        if self.peek() is not None:
            raise MParseError(f"unexpected '{self.peek().value}' at offset {self.peek().start}")  # type: ignore[union-attr]
        return e

    def expr(self) -> N:
        self.depth += 1
        if self.depth > MAX_M_DEPTH:
            raise MParseError("expression nested too deeply")
        try:
            return self._expr()
        finally:
            self.depth -= 1

    def _expr(self) -> N:
        t = self.peek()
        if t is None:
            raise MParseError("unexpected end of query")
        start = t.start
        if t.kind == "kw":
            if t.value == "let":
                return self.let()
            if t.value == "each":
                self.next()
                return self.span(N("each", a=self.expr()), start)
            if t.value == "if":
                self.next()
                c = self.expr()
                if not self.is_kw("then"):
                    raise MParseError("expected 'then'")
                self.next()
                a = self.expr()
                if not self.is_kw("else"):
                    raise MParseError("expected 'else'")
                self.next()
                b = self.expr()
                return self.span(N("if", a=a, b=b, items=[c]), start)
            if t.value == "try":
                self.next()
                body = self.expr()
                node = N("other", name="try", a=body)
                if self.is_kw("otherwise"):
                    self.next()
                    node.b = self.expr()
                return self.span(node, start)
            if t.value == "error":
                self.next()
                return self.span(N("other", name="error", a=self.expr()), start)
            if t.value == "type":
                self.next()
                return self.span(N("other", name="type", a=self.binary()), start)
        left = self.binary()
        return left

    def let(self) -> N:
        start = self.next().start
        binds: list[tuple[str, N]] = []
        while True:
            t = self.peek()
            if t is None:
                raise MParseError("unterminated let")
            if t.kind == "kw" and t.value == "in":
                break
            name = self.ident_name()
            self.expect_op("=")
            binds.append((name, self.expr()))
            if self.is_op(","):
                self.next()
        self.next()
        body = self.expr()
        return self.span(N("let", fields=binds, a=body), start)

    def ident_name(self) -> str:
        t = self.next()
        if t.kind in ("id", "str", "kw") and not (t.kind == "kw" and t.value in ("in", "let")):
            return t.value
        raise MParseError(f"expected a step name at offset {t.start}")

    _BIN_OPS = frozenset({"&", "+", "-", "*", "/", "=", "<", ">", "<=", ">=", "<>", "??"})

    def binary(self) -> N:
        left = self.unary()
        while True:
            t = self.peek()
            if t is None:
                return left
            if t.kind == "op" and t.value in self._BIN_OPS or (t.kind == "kw" and t.value in ("and", "or")):
                self.next()
                right = self.unary()
                left = N("bin", name=t.value, a=left, b=right, start=left.start, end=right.end)
            elif t.kind == "kw" and t.value in ("meta", "as", "is"):
                self.next()
                right = self.unary() if t.value == "meta" else self.type_ref()
                left = N("other", name=t.value, a=left, b=right, start=left.start, end=right.end)
            else:
                return left

    def type_ref(self) -> N:
        start = self.peek().start if self.peek() else 0  # type: ignore[union-attr]
        if self.is_kw("type"):
            self.next()
        if self.peek() is None:
            raise MParseError("expected a type")
        if self.is_kw("null") or self.peek().kind == "id":  # type: ignore[union-attr]
            self.next()
            if self.is_op("["):
                self.skip_group("[", "]")
            return self.span(N("other", name="type"), start)
        return self.unary()

    def skip_group(self, open_: str, close: str) -> None:
        depth = 0
        while True:
            t = self.next()
            if t.kind == "op" and t.value == open_:
                depth += 1
            elif t.kind == "op" and t.value == close:
                depth -= 1
                if depth == 0:
                    return

    def unary(self) -> N:
        t = self.peek()
        if t is not None and ((t.kind == "op" and t.value in ("-", "+", "!")) or (t.kind == "kw" and t.value == "not")):
            self.next()
            inner = self.unary()
            return N("other", name="unary", a=inner, start=t.start, end=inner.end)
        return self.postfix()

    def postfix(self) -> N:
        node = self.primary()
        while True:
            if self.is_op("("):
                self.next()
                args = self.args()
                node = N("call", a=node, items=args, start=node.start, end=self.toks[self.i - 1].end)
            elif self.is_op("{"):
                self.next()
                key = self.expr()
                self.expect_op("}")
                opt = False
                if self.is_op("?"):
                    self.next()
                    opt = True
                node = N("index", a=node, b=key, value=opt, start=node.start, end=self.toks[self.i - 1].end)
            elif self.is_op("["):
                names = self.field_selector()
                node = N("field", a=node, name=names, start=node.start, end=self.toks[self.i - 1].end)
                if self.is_op("?"):
                    self.next()
            else:
                return node

    def field_selector(self) -> str:
        self.expect_op("[")
        parts: list[str] = []
        while True:
            t = self.next()
            if t.kind == "op" and t.value == "]":
                break
            parts.append(t.value)
        return "".join(parts) if len(parts) == 1 else " ".join(parts)

    def args(self) -> list[N]:
        out: list[N] = []
        if self.is_op(")"):
            self.next()
            return out
        while True:
            out.append(self.expr())
            t = self.next()
            if t.kind == "op" and t.value == ")":
                return out
            if not (t.kind == "op" and t.value == ","):
                raise MParseError(f"expected ',' or ')' at offset {t.start}")

    def primary(self) -> N:
        t = self.next()
        start = t.start
        if t.kind == "str":
            return N("str", value=t.value, start=start, end=t.end)
        if t.kind == "num":
            return N("num", value=t.value, start=start, end=t.end)
        if t.kind == "kw":
            if t.value in ("true", "false", "null"):
                return N("lit", value={"true": True, "false": False, "null": None}[t.value], start=start, end=t.end)
            if t.value == "_":
                return N("id", name="_", start=start, end=t.end)
            raise MParseError(f"unexpected keyword '{t.value}' at offset {start}")
        if t.kind == "id":
            if t.value.startswith("#") and t.value in ("#table", "#date", "#datetime", "#time", "#duration", "#datetimezone", "#shared", "#sections", "#binary", "#infinity", "#nan"):
                return N("id", name=t.value, start=start, end=t.end)
            return N("id", name=t.value, start=start, end=t.end)
        if t.kind == "op":
            if t.value == "(":
                # lambda `(a, b) => body` or parenthesised expression
                save = self.i
                params = self.try_params()
                if params is not None and self.is_op("=>"):
                    self.next()
                    body = self.expr()
                    return N("lambda", params=params, a=body, start=start, end=body.end)
                self.i = save
                inner = self.expr()
                self.expect_op(")")
                return N("other", name="paren", a=inner, start=start, end=self.toks[self.i - 1].end)
            if t.value == "{":
                items: list[N] = []
                if self.is_op("}"):
                    self.next()
                    return N("list", items=items, start=start, end=self.toks[self.i - 1].end)
                while True:
                    items.append(self.expr())
                    if self.is_op(".."):
                        self.next()
                        self.expr()
                    u = self.next()
                    if u.kind == "op" and u.value == "}":
                        break
                    if not (u.kind == "op" and u.value == ","):
                        raise MParseError(f"expected ',' or '}}' at offset {u.start}")
                return N("list", items=items, start=start, end=self.toks[self.i - 1].end)
            if t.value == "[":
                flds: list[tuple[str, N]] = []
                if self.is_op("]"):
                    self.next()
                    return N("record", fields=flds, start=start, end=self.toks[self.i - 1].end)
                # a record `[a = 1, b = 2]` or an implicit field access `[col]` (in `each [col] * 2`)
                if self.peek(1) is not None and self.is_op("]", 1) and not self.is_op("=", 1):
                    name = self.next().value
                    self.next()
                    return N("field", a=None, name=name, start=start, end=self.toks[self.i - 1].end)
                while True:
                    fname = self.ident_name_loose()
                    self.expect_op("=")
                    flds.append((fname, self.expr()))
                    u = self.next()
                    if u.kind == "op" and u.value == "]":
                        break
                    if not (u.kind == "op" and u.value == ","):
                        raise MParseError(f"expected ',' or ']' at offset {u.start}")
                return N("record", fields=flds, start=start, end=self.toks[self.i - 1].end)
        raise MParseError(f"unexpected '{t.value}' at offset {start}")

    def ident_name_loose(self) -> str:
        t = self.next()
        if t.kind in ("id", "str", "kw"):
            return t.value
        raise MParseError(f"expected a field name at offset {t.start}")

    def try_params(self) -> list[str] | None:
        params: list[str] = []
        if self.is_op(")"):
            self.next()
            return params
        while True:
            t = self.peek()
            if t is None or t.kind not in ("id", "kw") or (t.kind == "kw" and t.value != "_"):
                return None
            self.next()
            params.append(t.value)
            if self.is_kw("as"):
                self.next()
                try:
                    self.type_ref()
                except MParseError:
                    return None
            u = self.peek()
            if u is None:
                return None
            self.next()
            if u.kind == "op" and u.value == ")":
                return params
            if not (u.kind == "op" and u.value == ","):
                return None


def parse_m(text: str) -> N:
    """Parse M text into a syntax tree. Raises :class:`MParseError`."""
    if len(text) > MAX_M_CHARS:
        raise MParseError(f"query longer than {MAX_M_CHARS} characters")
    try:
        return _Parser(text).parse()
    except RecursionError:
        raise MParseError("expression nested too deeply") from None


# ---------------------------------------------------------------------------------------------------------------------- values
@dataclass(frozen=True)
class MSource:
    """A dataset a query reads: its asset reference, ``kind`` (``table`` | ``file`` | ``external``), a display label and ``approx``, the reason the identity of the
    dataset is only a best guess (``None`` when it is certain)."""

    ref: AssetRef
    kind: str
    label: str
    approx: str | None = None


@dataclass(frozen=True)
class MOrigin:
    """A source column feeding an output column, with the M expression applied on the way (``None`` for a plain copy / rename)."""

    src: MSource
    col: str
    text: str | None = None


@dataclass
class MTable:
    """Abstract table state, see the module docstring: ``known`` columns with origins, ``hidden`` names removed or renamed away, ``closed`` when only ``known`` columns exist,
    ``reason`` when the columns are unknowable, ``parts`` for unions."""

    sources: list[MSource] = field(default_factory=list)
    known: dict[str, list[MOrigin]] = field(default_factory=dict)
    hidden: set[str] = field(default_factory=set)
    closed: bool = False
    reason: str | None = None
    parts: list[MTable] | None = None

    def copy(self) -> MTable:
        return MTable(list(self.sources), dict(self.known), set(self.hidden), self.closed, self.reason, list(self.parts) if self.parts is not None else None)

    @property
    def order(self) -> list[str]:
        """The known columns in output order."""
        return list(self.known)

    def lookup(self, name: str) -> list[MOrigin] | None:
        """Origins of column ``name`` or ``None`` when it is unknown / not available."""
        low = name.lower()
        if self.reason is not None:
            return None
        for k, v in self.known.items():
            if k.lower() == low:
                return v
        if low in self.hidden or self.closed:
            return None
        if self.parts is not None:
            out: list[MOrigin] = []
            for p in self.parts:
                got = p.lookup(name)
                if got is None:
                    return None
                out += got
            return out
        if len(self.sources) == 1:
            return [MOrigin(self.sources[0], name)]
        return None


@dataclass
class MConn:
    """A connection object (``Sql.Database(...)``): ``kind`` of connector, ``server`` / ``database`` / ``url`` as far as they are literals, ``extra`` for ids (Lakehouse.Contents)."""

    kind: str
    server: str | None = None
    database: str | None = None
    url: str | None = None
    workspace: str | None = None
    lakehouse: str | None = None
    schema: str | None = None
    item: str | None = None
    dynamic: bool = False
    root: MConn | None = field(default=None, repr=False, compare=False)   # the connector call this connection was navigated from
    used: bool = field(default=False, repr=False, compare=False)          # a dataset was derived from it (set on the root)


class _Unknown:
    pass


UNKNOWN = _Unknown()
_GENERIC_DB = {"oracle.database": "oracle", "postgresql.database": "postgresql", "mysql.database": "mysql", "snowflake.databases": "snowflake", "odbc.datasource": "odbc",
               "oledb.datasource": "oledb", "googlebigquery.database": "bigquery", "amazonredshift.database": "redshift", "teradata.database": "teradata", "saphana.database": "saphana",
               "db2.database": "db2", "mariadb.database": "mariadb", "sybase.database": "sybase", "azuredatabricks.catalogs": "databricks", "databricks.catalogs": "databricks",
               "azuredatalakestore.contents": "adls", "azuredatalake.contents": "adls", "sap.businessobjects": "sap", "impala.database": "impala", "informix.database": "informix",
               "kusto.contents": "kusto", "azuredataexplorer.contents": "kusto", "sql.databases": "sql"}
_FABRIC_SQL_HOST = re.compile(r"datawarehouse\.(fabric\.microsoft\.com|pbidedicated\.windows\.net)$")
_PASS_THROUGH = frozenset(n.lower() for n in """
Table.SelectRows Table.Sort Table.Distinct Table.FirstN Table.LastN Table.Skip Table.Range Table.RemoveFirstN Table.RemoveLastN Table.Buffer
Table.TransformColumnTypes Table.ReorderColumns Table.FillDown Table.FillUp Table.ReplaceErrorValues Table.RemoveRowsWithErrors Table.SelectRowsWithErrors Table.RemoveMatchingRows
Table.InsertRows Table.ReplaceRows Table.AlternateRows Table.Repeat Table.Reverse Table.MaxN Table.MinN Table.StopFolding 
""".split())
_OPAQUE_FUNCTIONS = frozenset({"table.expandtablecolumn", "table.expandrecordcolumn", "table.expandlistcolumn", "table.pivot", "table.unpivot", "table.unpivotothercolumns",
                              "table.transpose", "table.splitcolumn", "table.combinecolumns", "table.transformcolumnnames", "table.aggregatetablecolumn", "table.split",
                              "table.addjoincolumn", "table.addfuzzyclustercolumn", "table.profile", "table.view", "table.viewfunction", "table.conformtopagereader",
                              "table.fromlist", "table.addrankcolumn"})
_FILE_READERS = {"csv.document": "csv", "excel.workbook": "xlsx", "json.document": "json", "parquet.document": "parquet", "xml.tables": "xml", "pdf.tables": "pdf",
                 "lines.fromBinary": "text", "binary.decompress": "binary", "deltalake.table": "delta"}
_FILE_READERS = {k.lower(): v for k, v in _FILE_READERS.items()}


class MResolver:
    """Supplies shared expressions and table queries by name (``expression(name) -> M text | None``) and the lineage context."""

    def __init__(self, expressions: Callable[[str], str | None] | None = None) -> None:
        self.expressions = expressions or (lambda _n: None)


@dataclass
class MResult:
    """Outcome of :func:`analyze_m`: the datasets read, the final table state, and anything that limited the analysis."""

    sources: list[MSource] = field(default_factory=list)
    table: MTable | None = None
    connection: MConn | None = None
    problems: list[str] = field(default_factory=list)
    dynamic: list[str] = field(default_factory=list)
    unsupported: list[str] = field(default_factory=list)
    sql_gaps: list[tuple[str, str, str]] = field(default_factory=list)

    @property
    def reason(self) -> str | None:
        """Why column lineage is not available (``None`` when it is)."""
        if self.table is None:
            return self.problems[0] if self.problems else "the query result is not a table"
        return self.table.reason

    def origins(self, column: str) -> list[MOrigin] | None:
        """The source columns feeding output column ``column`` (``None`` when unknown)."""
        return self.table.lookup(column) if self.table is not None else None


# ---------------------------------------------------------------------------------------------------------------------- evaluator
_ONELAKE_HTTPS = re.compile(r"^https?://onelake\.(?:dfs|blob)\.fabric\.microsoft\.com/([^/]+)/?(.*)$", re.IGNORECASE)


def _onelake_abfss(url: str) -> str:
    """``https://onelake.dfs.fabric.microsoft.com/<ws>/<item>/...`` as the ``abfss://<ws>@onelake.dfs.fabric.microsoft.com/<item>/...`` form that discovery records."""
    m = _ONELAKE_HTTPS.match(url.strip())
    if not m:
        return url.replace("\\", "/") if re.match(r"^[A-Za-z]:\\|^\\\\", url) else url
    return f"abfss://{m.group(1)}@onelake.dfs.fabric.microsoft.com/{m.group(2)}".rstrip("/")


def _src_text(text: str, node: N) -> str:
    return " ".join(text[node.start:node.end].split())


class _Eval:
    def __init__(self, text: str, ctx: LineageContext, resolver: MResolver, builder: FactBuilder | None, depth: int, stack: tuple[str, ...]) -> None:
        self.text = text
        self.ctx = ctx
        self.resolver = resolver
        self.b = builder
        self.depth = depth
        self.stack = stack
        self.steps: dict[str, N] = {}
        self.cache: dict[str, Any] = {}
        self.evaluating: set[str] = set()
        self.sources: list[MSource] = []
        self.conns: list[MConn] = []
        self.result = MResult()
        self.count = 0

    # -- bookkeeping
    def add_source(self, src: MSource) -> MSource:
        if all(s.ref != src.ref for s in self.sources):
            self.sources.append(src)
        return src

    def note(self, kind: str, msg: str) -> None:
        bucket = {"dynamic": self.result.dynamic, "unsupported": self.result.unsupported}[kind]
        if msg not in bucket:
            bucket.append(msg)

    # -- evaluation
    def ev(self, n: N | None, env: dict[str, Any], row: MTable | None = None) -> Any:
        """Evaluate a node to an abstract value (literal, list, dict, MConn, MTable, UNKNOWN)."""
        if n is None:
            return UNKNOWN
        self.count += 1
        if self.count > MAX_M_STEPS:
            raise MParseError("the query is too large to analyse")
        k = n.kind
        if k == "str":
            return n.value
        if k == "num":
            return n.value
        if k == "lit":
            return n.value
        if k == "list":
            return [self.ev(x, env, row) for x in n.items]
        if k == "record":
            return {name: self.ev(v, env, row) for name, v in n.fields}
        if k == "id":
            return self.ident(n.name, env)
        if k == "let":
            names = {name: v for name, v in n.fields}
            lazy = _LazyEnv(env, names, self)
            # steps the result depends on are evaluated in declaration order (each one then finds its predecessors done), so that a chain of hundreds of steps needs no deep
            # recursion; steps the result does not depend on are never evaluated, so they cannot add sources
            reached = _reachable_steps(names, n.a)
            for step in names:
                if step in reached:
                    lazy[step]
            return self.ev(n.a, lazy, row)
        if k == "call":
            return self.call(n, env, row)
        if k == "index":
            return self.index(n, env, row)
        if k == "field":
            return self.field(n, env, row)
        if k == "bin":
            left, right = self.ev(n.a, env, row), self.ev(n.b, env, row)
            if n.name == "&" and isinstance(left, str) and isinstance(right, str):
                return left + right
            return UNKNOWN
        if k == "other" and n.name in ("paren", "meta", "try"):
            return self.ev(n.a, env, row)
        if k == "if":
            return UNKNOWN
        return UNKNOWN

    def ident(self, name: str, env: dict[str, Any]) -> Any:
        if name in env:
            return env[name]
        # a shared expression / another query / parameter
        txt = self.resolver.expressions(name)
        if txt is not None:
            return self.shared(name, txt)
        return UNKNOWN

    def shared(self, name: str, txt: str) -> Any:
        key = name.lower()
        if key in self.stack or self.depth >= 8:
            return UNKNOWN
        try:
            tree = parse_m(txt)
        except MParseError as exc:
            self.result.problems.append(f"query '{name}' cannot be parsed: {exc}")
            return UNKNOWN
        sub = _Eval(txt, self.ctx, self.resolver, self.b, self.depth + 1, (*self.stack, key))
        sub.result = self.result
        val = sub.ev(tree, {})
        for s in sub.sources:
            self.add_source(s)
        for c in sub.conns:
            if all(c is not x for x in self.conns):
                self.conns.append(c)
        if isinstance(val, dict) or isinstance(val, MConn) or isinstance(val, MTable):
            return val
        if isinstance(val, MTable):
            return val
        return val

    # -- function calls
    def call(self, n: N, env: dict[str, Any], row: MTable | None) -> Any:
        out = self._call(n, env, row)
        if isinstance(out, MConn) and out.root is None and all(out is not c for c in self.conns):
            self.conns.append(out)
        return out

    @staticmethod
    def _use(conn: MConn) -> None:
        """Remember that a dataset was derived from the connector call ``conn`` was navigated from."""
        (conn.root or conn).used = True

    def finish_connections(self) -> None:
        """A connection that was opened but never produced a dataset (the table or file it reads could not be identified) is still a dependency: record the connection itself as an
        approximate source, so the query never looks as if it read nothing."""
        for c in self.conns:
            if c.used or c.kind in ("lakehouse", "lakehouse_ws", "lakehouse_item") or c.kind.startswith("pbi_"):
                continue
            name = self._connection_name(c)
            if name is None:
                self.note("dynamic", f"the {c.kind} connection target is only known at run time")
                continue
            why = f"the {c.kind} connection '{name.split(':', 1)[1]}' is opened but the table or file read from it cannot be identified"
            self.add_source(MSource(AssetRef.make("external", name=name), "external", name, why))

    @staticmethod
    def _connection_name(c: MConn) -> str | None:
        if c.url:
            parts = urlsplit(c.url)
            host = f"{parts.scheme}://{parts.hostname}{':' + str(parts.port) if parts.port else ''}{parts.path}" if parts.scheme and parts.hostname else c.url
            return f"{c.kind}:{host}"
        if c.server:
            return f"{c.kind}:{c.server.lower()}/{c.database or '*'}"
        return None

    def _call(self, n: N, env: dict[str, Any], row: MTable | None) -> Any:
        fn = n.a
        if fn is None or fn.kind != "id":
            if fn is not None and fn.kind == "field":
                self.ev(fn, env, row)
            return UNKNOWN
        name = fn.name
        low = name.lower()
        args = n.items
        if low in ("sql.database",):
            return self.sql_database(args, env, row)
        if low in _GENERIC_DB:
            return self.generic_db(_GENERIC_DB[low], args, env, row)
        if low == "lakehouse.contents" or low == "fabric.lakehouse":
            return MConn("lakehouse")
        if low in ("azurestorage.datalakecontents", "azurestorage.blobcontents", "azurestorage.datalake", "azurestorage.blobs", "azurestorage.tables"):
            url = self.ev(args[0], env, row) if args else UNKNOWN
            return MConn("storage", url=url if isinstance(url, str) else None, dynamic=not isinstance(url, str))
        if low in ("file.contents", "folder.files", "folder.contents"):
            p = self.ev(args[0], env, row) if args else UNKNOWN
            return MConn("file", url=p if isinstance(p, str) else None, dynamic=not isinstance(p, str))
        if low in ("web.contents", "web.page", "web.browsercontents", "odata.feed", "sharepoint.files", "sharepoint.contents", "sharepoint.tables", "http.request"):
            a0 = self.ev(args[0], env, row) if args else UNKNOWN
            if isinstance(a0, MConn):
                return a0
            return MConn("web" if low.startswith(("web", "odata", "http")) else "sharepoint", url=a0 if isinstance(a0, str) else None, dynamic=not isinstance(a0, str))
        if low in ("analysisservices.database", "analysisservices.databases"):
            srv = self.ev(args[0], env, row) if args else UNKNOWN
            db = self.ev(args[1], env, row) if len(args) > 1 else None
            return MConn("analysisservices", server=srv if isinstance(srv, str) else None, database=db if isinstance(db, str) else None, dynamic=not isinstance(srv, str))
        if low in ("powerplatform.dataflows", "powerbi.dataflows", "powerbi.datasets"):
            return MConn("pbi_" + low.split(".")[1])
        if low in _FILE_READERS:
            return self.file_reader(low, args, env, row)
        if low == "value.nativequery":
            return self.native_query(args, env, row)
        if low in ("table.fromrows", "table.fromcolumns", "#table", "table.fromrecords", "table.fromlist"):
            return self.inline_table(low, args, env, row)
        if low in ("excel.currentworkbook", "table.fromvalue"):
            return MTable(closed=True, reason="inline workbook data")
        if low.startswith("table."):
            return self.table_fn(low, name, n, env, row)
        # any other function: evaluate its arguments so that the connections and tables inside them are still recorded as sources
        seen = [self.ev(a, env, row) for a in args[:4]]
        for v in seen:
            if isinstance(v, MTable) and v.sources:
                self.note("unsupported", f"{name}: the function is not understood; the tables it reads are still reported")
                return MTable(sources=list(v.sources), closed=True, reason=f"{name} is not understood")
        return UNKNOWN

    # -- connectors
    def sql_database(self, args: list[N], env: dict[str, Any], row: MTable | None) -> Any:
        srv = self.ev(args[0], env, row) if args else UNKNOWN
        db = self.ev(args[1], env, row) if len(args) > 1 else UNKNOWN
        opts = self.ev(args[2], env, row) if len(args) > 2 else {}
        conn = MConn("sql", server=srv if isinstance(srv, str) else None, database=db if isinstance(db, str) else None, dynamic=not isinstance(srv, str) or not isinstance(db, str))
        if isinstance(opts, dict) and isinstance(opts.get("Query"), str):
            return self.run_sql(conn, opts["Query"], None)
        return conn

    def generic_db(self, kind: str, args: list[N], env: dict[str, Any], row: MTable | None) -> Any:
        srv = self.ev(args[0], env, row) if args else UNKNOWN
        db = self.ev(args[1], env, row) if len(args) > 1 else None
        return MConn(kind, server=srv if isinstance(srv, str) else None, database=db if isinstance(db, str) else None, dynamic=not isinstance(srv, str))

    def file_reader(self, low: str, args: list[N], env: dict[str, Any], row: MTable | None) -> Any:
        v = self.ev(args[0], env, row) if args else UNKNOWN
        fmt = _FILE_READERS[low]
        if isinstance(v, MConn):
            if v.kind in ("file", "storage"):
                return self.dataset_table(self.file_dataset(v, fmt))
            if v.kind == "web":
                return self.dataset_table(self.external_dataset(v))
            if v.kind == "lakehouse_item" and v.item:
                return self.dataset_table(self.lakehouse_file(v))
            if v.kind == "sharepoint":
                return self.dataset_table(self.external_dataset(v))
        if isinstance(v, MTable):
            return v
        if isinstance(v, dict) and v.get("__file__"):
            return v["__file__"]
        self.note("dynamic", f"{low}: the file is not a literal path")
        return MTable(closed=True, reason="the file is only known at run time")

    def native_query(self, args: list[N], env: dict[str, Any], row: MTable | None) -> Any:
        conn = self.ev(args[0], env, row) if args else UNKNOWN
        q = self.ev(args[1], env, row) if len(args) > 1 else UNKNOWN
        if not isinstance(conn, MConn) or conn.kind not in ("sql", "oracle", "postgresql", "mysql", "snowflake", "bigquery", "redshift", "odbc", "databricks"):
            self.note("unsupported", "Value.NativeQuery on a source that is not a database connection")
            return MTable(closed=True, reason="Value.NativeQuery on an unsupported source")
        if not isinstance(q, str):
            self.note("dynamic", "Value.NativeQuery: the query text is not a literal")
            return MTable(closed=True, reason="the native query text is only known at run time")
        return self.run_sql(conn, q, None)

    def run_sql(self, conn: MConn, sql: str, _unused: Any) -> MTable:
        """Analyse the SQL text of a native query on ``conn`` with the SQL analyzer (T-SQL for SQL Server / Fabric endpoints)."""
        from olcf.intelligence.lineage.sql import SqlSession

        self._use(conn)
        dialect = {"sql": "tsql", "postgresql": "postgres", "mysql": "mysql", "snowflake": "snowflake", "oracle": "oracle", "bigquery": "bigquery", "redshift": "redshift"}.get(conn.kind, "tsql")
        scratch = FactBuilder(MCODE, self.ctx)
        session = SqlSession(self.ctx, scratch, dialect, analyzer=MCODE, emit_query_assets=False)
        session.table_ref_factory = lambda parts: self.table_ref(conn, parts[:-1][-1] if len(parts) > 1 else None, parts[-1])[0]
        results = session.run(sql, process=AssetRef.make("sql_query"), loc={})
        facts = scratch.facts()
        for g in facts.gaps:
            self.result.sql_gaps.append((g.kind, g.subject, g.detail))
        rel = next((r.relation for r in reversed(results) if r.relation is not None), None)
        by_key = {k: ds for k, ds in session.datasets.items()}
        table = MTable(closed=True)
        for ds in by_key.values():
            if ds.kind in ("table", "view"):
                src = MSource(ds.ref, "table" if ds.ref.type == "table" else "external", ds.name)
                self.add_source(src)
                table.sources.append(src)
        if rel is None:
            table.reason = "the native query does not return a result set"
            return table
        if rel.opaque or rel.tail:
            table.reason = "the native query selects columns that cannot be listed (SELECT * over an unknown schema)"
            table.closed = True
            return table
        for c in rel.cols:
            origins: list[MOrigin] = []
            for o in sorted(c.origins, key=lambda x: (x.ds, x.col)):
                base_ds = by_key.get(o.ds)
                if base_ds is None:
                    continue
                src = MSource(base_ds.ref, "table" if base_ds.ref.type == "table" else "external", base_ds.name)
                origins.append(MOrigin(src, o.col, " <- ".join(o.steps) if o.steps else None))
            table.known[c.name] = origins
        return table

    # -- datasets
    def table_ref(self, conn: MConn, schema: str | None, item: str) -> tuple[AssetRef, str | None]:
        """The reference of ``schema.item`` on connection ``conn`` and, when the identity is not certain, why."""
        name = f"{schema}.{item}" if schema and schema.lower() != "dbo" else item
        host = (conn.server or "").lower()
        db = conn.database or ""
        if conn.kind == "sql" and _FABRIC_SQL_HOST.search(host):
            if looks_like_guid(db):
                ref = AssetRef.make("table", name=name, scope=self.ctx.scope(lakehouse=None, lakehouse_id=None))
                return ref, f"the SQL endpoint '{db.lower()}' is not mapped to a lakehouse by the query"
            return AssetRef.make("table", name=name, scope=self.ctx.scope(lakehouse=db, lakehouse_id=None, fabric_lakehouse=None)), None
        if conn.kind == "lakehouse_item" and conn.workspace and conn.lakehouse:
            return fabric_table_ref(self.ctx, conn.workspace, conn.lakehouse, name), None
        approx = None
        if not host:
            approx = f"the {conn.kind} server is only known at run time"
        elif conn.kind == "sql" and not db:
            approx = "the database of the sql connection is only known at run time"
        return AssetRef.make("external", name=f"{conn.kind}:{host or '*'}/{db or '*'}/{name}"), approx

    def nav_dataset(self, conn: MConn) -> MSource | None:
        self._use(conn)
        if conn.item is None:
            return None
        ref, approx = self.table_ref(conn, conn.schema, conn.item)
        kind = "table" if ref.type == "table" else "external"
        label = f"{conn.schema + '.' if conn.schema else ''}{conn.item}"
        return self.add_source(MSource(ref, kind, label, approx))

    def file_dataset(self, conn: MConn, fmt: str) -> MSource | None:
        self._use(conn)
        if not conn.url:
            return None
        target = classify_path(_onelake_abfss(conn.url), fmt=fmt)
        ref = target.ref(self.ctx, fmt)
        return self.add_source(MSource(ref, "table" if ref.type == "table" else "file", target.name))

    def lakehouse_file(self, conn: MConn) -> MSource | None:
        self._use(conn)
        if not (conn.item and conn.workspace and conn.lakehouse):
            return None
        path = conn.item
        ref = classify_path(path).ref(self.ctx)
        return self.add_source(MSource(ref, "file", path))

    def external_dataset(self, conn: MConn) -> MSource | None:
        self._use(conn)
        if not conn.url:
            return None
        parts = urlsplit(conn.url)
        if parts.scheme and parts.hostname:
            name = f"{conn.kind}:{parts.scheme}://{parts.hostname}{':' + str(parts.port) if parts.port else ''}{parts.path}"
        else:
            name = f"{conn.kind}:{redact_secrets(strip_uri_secrets(conn.url))}"
        return self.add_source(MSource(AssetRef.make("external", name=name), "external", name))

    def dataset_table(self, src: MSource | None) -> MTable:
        if src is None:
            self.note("dynamic", "a data source location is only known at run time")
            return MTable(closed=True, reason="the data source location is only known at run time")
        return MTable(sources=[src])

    # -- navigation
    def index(self, n: N, env: dict[str, Any], row: MTable | None) -> Any:
        base = self.ev(n.a, env, row)
        key = self.ev(n.b, env, row)
        if isinstance(base, MConn):
            out = self.navigate(base, key)
            if isinstance(out, MConn) and out is not base:
                out.root = base.root or base
            return out
        if isinstance(base, list) and isinstance(key, str):
            return UNKNOWN
        return UNKNOWN

    def navigate(self, conn: MConn, key: Any) -> Any:
        if not isinstance(key, dict):
            return UNKNOWN
        k = {str(a).lower(): b for a, b in key.items()}
        if conn.kind == "lakehouse":
            if "workspaceid" in k or "workspacename" in k:
                ws = k.get("workspaceid")
                return MConn("lakehouse_ws", workspace=ws.lower() if isinstance(ws, str) and looks_like_guid(ws) else None, dynamic=not isinstance(ws, str))
            return UNKNOWN
        if conn.kind == "lakehouse_ws":
            lh = k.get("lakehouseid")
            return MConn("lakehouse_item", workspace=conn.workspace, lakehouse=lh.lower() if isinstance(lh, str) and looks_like_guid(lh) else None, dynamic=not isinstance(lh, str))
        if conn.kind == "lakehouse_item":
            item = k.get("id") or k.get("name") or k.get("item")
            kind = str(k.get("itemkind") or "Table").lower()
            sch = k.get("schema")
            out = MConn("lakehouse_item", workspace=conn.workspace, lakehouse=conn.lakehouse, schema=sch if isinstance(sch, str) else None,
                        item=item if isinstance(item, str) else None, dynamic=not isinstance(item, str))
            out.url = "file" if kind in ("file", "folder") else None
            return out
        if conn.kind in ("sql", "oracle", "postgresql", "mysql", "snowflake", "odbc", "bigquery", "redshift", "databricks", "teradata", "db2", "saphana", "mariadb", "impala"):
            sch, item, name = k.get("schema"), k.get("item"), k.get("name")
            if item is not None or sch is not None:
                return MConn(conn.kind, server=conn.server, database=conn.database, schema=sch if isinstance(sch, str) else None, item=item if isinstance(item, str) else None,
                             dynamic=conn.dynamic or not isinstance(item, str))
            if name is not None:       # Sql.Databases{[Name="db"]}[Data]
                return MConn(conn.kind, server=conn.server, database=name if isinstance(name, str) else None, dynamic=conn.dynamic or not isinstance(name, str))
        if conn.kind in ("storage", "file"):
            name = k.get("name")
            folder = k.get("folder path")
            if isinstance(name, str) and conn.url is not None:
                base = folder if isinstance(folder, str) else conn.url
                return MConn(conn.kind, url=base.rstrip("/\\") + ("\\" if "\\" in base and "/" not in base else "/") + name)
            if name is not None or folder is not None:
                return MConn(conn.kind, dynamic=True)
            return conn
        return UNKNOWN

    def field(self, n: N, env: dict[str, Any], row: MTable | None) -> Any:
        if n.a is None:                          # implicit [col] inside `each`
            return ("col", n.name)
        base = self.ev(n.a, env, row)
        name = n.name
        if isinstance(base, MConn) and name == "Content" and base.kind in ("storage", "file"):
            return base
        if isinstance(base, MConn) and name == "Data":
            if base.kind == "lakehouse_item" and base.item:
                if base.url == "file":
                    return self.dataset_table(self.lakehouse_file(base))
                return self.dataset_table(self.nav_dataset(base))
            if base.item is not None and base.kind not in ("lakehouse", "lakehouse_ws"):
                return self.dataset_table(self.nav_dataset(base))
            return base
        if isinstance(base, dict) and name in base:
            return base[name]
        if isinstance(base, MTable):
            return ("col", name)
        return UNKNOWN

    # -- inline tables
    def inline_table(self, low: str, args: list[N], env: dict[str, Any], row: MTable | None) -> MTable:
        # ``Table.FromList(Json.Document(Web.Contents(url)), ...)``: the data comes from the first argument, so its sources (forced here: steps are evaluated lazily) stay the
        # sources of the table; only the columns are unknown
        first = self.ev(args[0], env, row) if args and low != "#table" else None
        t = MTable(closed=True, sources=self._src_of([first]))
        if low == "#table" and args:
            cols = self.ev(args[0], env, row)
            if isinstance(cols, list):
                for c in cols:
                    if isinstance(c, str):
                        t.known[c] = []
            elif isinstance(cols, dict):
                pass
        elif len(args) > 1:
            cols = self.ev(args[1], env, row)
            if isinstance(cols, list):
                for c in cols:
                    if isinstance(c, str):
                        t.known[c] = []
        if not t.known:
            t.reason = "the columns of a table built from data are not known" if t.sources else "inline table with unknown columns"
        return t

    # -- Table.* functions
    def table_fn(self, low: str, name: str, n: N, env: dict[str, Any], row: MTable | None) -> Any:
        args = n.items
        base = self.ev(args[0], env, row) if args else UNKNOWN
        if low == "table.combine":
            parts = base if isinstance(base, list) else []
            tabs = [p for p in parts if isinstance(p, MTable)]
            if not tabs or len(tabs) != len(parts):
                return MTable(reason="Table.Combine over tables that cannot be determined", closed=True, sources=self._src_of(parts))
            srcs = [s for t in tabs for s in t.sources]
            reason = next((t.reason for t in tabs if t.reason), None)
            if any(t.closed for t in tabs):
                names = [c for c in tabs[0].order]
                out = MTable(sources=srcs, closed=True, reason=reason)
                for c in names:
                    origins: list[MOrigin] = []
                    ok = True
                    for t in tabs:
                        got = t.lookup(c)
                        if got is None:
                            ok = False
                            break
                        origins += got
                    if ok:
                        out.known[c] = origins
                return out
            return MTable(sources=srcs, parts=tabs, reason=reason)
        if not isinstance(base, MTable):
            if base is UNKNOWN or base is None:
                self.note("unsupported", f"{name}: the input table could not be determined")
            return base if isinstance(base, MTable) else MTable(closed=True, reason=f"{name}: the input table could not be determined")
        t = base.copy()
        if base.reason is not None:
            return t
        if low in _PASS_THROUGH:
            return t
        rest = args[1:]
        vals = [self.ev(a, env, t) if a.kind != "each" and a.kind != "lambda" else None for a in rest]
        if low == "table.renamecolumns":
            pairs = vals[0] if vals else None
            if isinstance(pairs, list) and all(isinstance(p, list) and len(p) == 2 and all(isinstance(x, str) for x in p) for p in pairs):
                moves = [(p[0], p[1]) for p in pairs]
                renames = {old.lower(): new for old, new in moves}
                new_known: dict[str, list[MOrigin]] = {renames.get(k.lower(), k): v for k, v in t.known.items()}
                have = {k.lower() for k in t.known}
                for old, new in moves:
                    if old.lower() not in have:
                        got = t.lookup(old)
                        if got is not None:
                            new_known[new] = got
                for old, new in moves:
                    if old.lower() != new.lower():
                        t.hidden.add(old.lower())
                for _old, new in moves:
                    t.hidden.discard(new.lower())
                t.known = new_known
                return t
            return self.opaque(t, name, "column renames are not a literal list")
        if low == "table.removecolumns":
            cols = _names(vals[0] if vals else None)
            if cols is None:
                return self.opaque(t, name, "column names are not literal")
            for c in cols:
                t.known = {k: v for k, v in t.known.items() if k.lower() != c.lower()}
                t.hidden.add(c.lower())
            return t
        if low == "table.selectcolumns":
            cols = _names(vals[0] if vals else None)
            if cols is None:
                return self.opaque(t, name, "column names are not literal")
            out = MTable(sources=list(t.sources), closed=True)
            for c in cols:
                got = t.lookup(c)
                out.known[c] = got if got is not None else []
                if got is None:
                    out.hidden.add(c.lower())
            return out
        if low == "table.addcolumn" and len(rest) >= 2:
            cname = vals[0] if isinstance(vals[0], str) else None
            if cname is None:
                return self.opaque(t, name, "new column name is not literal")
            t.known[cname] = self.each_origins(rest[1], t, env, text=_src_text(self.text, rest[1]))
            t.hidden.discard(cname.lower())
            return t
        if low == "table.addindexcolumn" and rest:
            cname = vals[0] if isinstance(vals[0], str) else None
            if cname:
                t.known[cname] = []
                t.hidden.discard(cname.lower())
                return t
            return self.opaque(t, name, "new column name is not literal")
        if low == "table.duplicatecolumn" and len(rest) >= 2:
            old, new = vals[0], vals[1]
            if isinstance(old, str) and isinstance(new, str):
                got = t.lookup(old)
                t.known[new] = got if got is not None else []
                return t
            return self.opaque(t, name, "column names are not literal")
        if low in ("table.transformcolumns", "table.replacevalue", "table.replacevalue_"):
            return self.transform_columns(t, low, name, rest, vals, env)
        if low == "table.group" and len(rest) >= 2:
            keys = _names(vals[0])
            aggs = rest[1]
            if keys is None or aggs.kind != "list":
                return self.opaque(t, name, "grouping columns or aggregations are not literal")
            out = MTable(sources=list(t.sources), closed=True)
            for k in keys:
                got = t.lookup(k)
                out.known[k] = got if got is not None else []
            for spec in aggs.items:
                if spec.kind == "list" and len(spec.items) >= 2 and spec.items[0].kind == "str":
                    out.known[spec.items[0].value] = self.each_origins(spec.items[1], t, env, text=_src_text(self.text, spec.items[1]))
            return out
        if low in ("table.join", "table.nestedjoin", "table.fuzzyjoin", "table.fuzzynestedjoin", "table.join_"):
            others = [v for v in vals if isinstance(v, MTable)]
            out = MTable(sources=list(t.sources) + [s for o in others for s in o.sources], closed=True)
            out.reason = f"{name}: the columns of a join result are not tracked"
            self.note("unsupported", out.reason)
            return out
        if low in ("table.promoteheaders", "table.demoteheaders"):
            return self.opaque(t, name, "the column names come from the data")
        if low in _OPAQUE_FUNCTIONS:
            return self.opaque(t, name, "changes the columns in a way that is not tracked")
        return self.opaque(t, name, "is not a function the analyzer understands")

    def opaque(self, t: MTable, fn: str, why: str) -> MTable:
        msg = f"{fn}: {why}"
        self.note("unsupported", msg)
        out = t.copy()
        out.reason = msg
        return out

    def _src_of(self, parts: Any) -> list[MSource]:
        out: list[MSource] = []
        for p in parts if isinstance(parts, list) else []:
            if isinstance(p, MTable):
                out += p.sources
        return out

    def transform_columns(self, t: MTable, low: str, name: str, rest: list[N], vals: list[Any], env: dict[str, Any]) -> MTable:
        if low.startswith("table.replacevalue"):
            cols = _names(vals[3]) if len(vals) > 3 else None
            if cols is None:
                return self.opaque(t, name, "replaced columns are not literal")
            for c in cols:
                got = t.lookup(c)
                if got is None:
                    continue
                t.known[c] = [MOrigin(o.src, o.col, name) for o in got]
            return t
        spec = rest[0] if rest else None
        if spec is None:
            return t
        items = spec.items if spec.kind == "list" else []
        if spec.kind == "list" and items and all(i.kind == "list" and i.items and i.items[0].kind == "str" for i in items):
            pairs = items
        elif spec.kind == "list" and items and items[0].kind == "str" and len(rest) > 1:
            pairs = [N("list", items=[i, rest[1]]) for i in items]
        else:
            return self.opaque(t, name, "transformations are not a literal list")
        for p in pairs:
            col = p.items[0].value
            got = t.lookup(col)
            if len(p.items) > 1:
                extra = self.each_origins(p.items[1], t, env, text=_src_text(self.text, p.items[1]), self_col=col)
                t.known[col] = extra if extra else ([MOrigin(o.src, o.col, _src_text(self.text, p.items[1])) for o in (got or [])])
            elif got is not None:
                t.known[col] = got
        return t

    def each_origins(self, node: N, t: MTable, env: dict[str, Any], *, text: str, self_col: str | None = None) -> list[MOrigin]:
        """Origins of a column computed by ``each <expr>`` / ``(r) => <expr>`` over table state ``t``: the columns the expression refers to (``[x]``, ``_[x]``, ``r[x]``, ``Record.Field(_, "x")``);
        ``_`` alone means ``self_col``."""
        body = node.a if node.kind in ("each", "lambda") else node
        refs: list[str] = []
        param = node.params[0] if node.kind == "lambda" and node.params else "_"
        uses_self = [False]
        _collect_cols(body, param, refs, uses_self)
        if uses_self[0] and self_col is not None:
            refs.append(self_col)
        out: list[MOrigin] = []
        seen: set[tuple[str, str]] = set()
        lost: list[str] = []
        for r in refs:
            got = t.lookup(r)
            if got is None:
                lost.append(r)
                continue
            for o in got:
                key = (o.src.label, o.col)
                if key in seen:
                    continue
                seen.add(key)
                out.append(MOrigin(o.src, o.col, text if o.text is None else f"{text} <- {o.text}"))
        if lost and t.sources:
            self.note("unsupported", f"an expression refers to column(s) {', '.join(lost[:4])} that could not be traced to a source column")
        return out


class _LazyEnv(dict[str, Any]):
    """Environment of a ``let``: each step is evaluated the first time it is referenced (cycle-safe, outer names visible)."""

    def __init__(self, base: dict[str, Any], names: dict[str, N], ev: _Eval) -> None:
        super().__init__()
        self.base = base
        self.names = names
        self.ev = ev
        self.done: dict[str, Any] = {}
        self.busy: set[str] = set()

    def __contains__(self, key: object) -> bool:
        return key in self.names or key in self.base

    def __getitem__(self, key: str) -> Any:
        if key in self.done:
            return self.done[key]
        if key in self.names:
            if key in self.busy:
                return UNKNOWN
            self.busy.add(key)
            try:
                val = self.ev.ev(self.names[key], self)
            finally:
                self.busy.discard(key)
            self.done[key] = val
            return val
        return self.base[key]


def _reachable_steps(names: dict[str, N], root: N | None) -> set[str]:
    """Names of the ``let`` steps that ``root`` refers to, directly or through other steps (an over-approximation: shadowing is ignored)."""
    out: set[str] = set()
    stack: list[N | None] = [root]
    while stack:
        node = stack.pop()
        if node is None:
            continue
        if node.kind == "id" and node.name in names and node.name not in out:
            out.add(node.name)
            stack.append(names[node.name])
        stack.extend([node.a, node.b, *node.items, *[v for _, v in node.fields]])
    return out


def _names(v: Any) -> list[str] | None:
    if isinstance(v, str):
        return [v]
    if isinstance(v, list) and all(isinstance(x, str) for x in v):
        return [str(x) for x in v]
    return None


def _collect_cols(n: N | None, param: str, out: list[str], uses_self: list[bool]) -> None:
    """Column names a row expression refers to: implicit ``[x]`` fields, ``param[x]``, ``Record.Field(param, "x")``; a bare ``param`` marks ``uses_self``."""
    if n is None:
        return
    if n.kind == "field":
        if n.a is None or (n.a.kind == "id" and n.a.name == param):
            if n.name and n.name not in out:
                out.append(n.name)
            return
        _collect_cols(n.a, param, out, uses_self)
        return
    if n.kind == "call" and n.a is not None and n.a.kind == "id" and n.a.name.lower() in ("record.field", "record.fieldor") and len(n.items) >= 2 and n.items[1].kind == "str":
        if n.items[1].value not in out:
            out.append(n.items[1].value)
        return
    if n.kind == "id" and n.name == param:
        uses_self[0] = True
        return
    for child in (n.a, n.b, *n.items, *[v for _, v in n.fields]):
        _collect_cols(child, param, out, uses_self)


def analyze_m(text: str, *, ctx: LineageContext, resolver: MResolver | None = None, builder: FactBuilder | None = None) -> MResult:
    """Analyse one Power Query M query. Never raises: a query that cannot be parsed returns a result with ``problems``."""
    res = MResult()
    if not isinstance(text, str) or not text.strip():
        res.problems.append("the query is empty")
        return res
    try:
        tree = parse_m(text)
    except MParseError as exc:
        res.problems.append(f"cannot parse the M query: {exc}")
        return res
    except RecursionError:
        res.problems.append("cannot parse the M query: expression nested too deeply")
        return res
    ev = _Eval(text, ctx, resolver or MResolver(), builder, 0, ())
    ev.result = res
    try:
        val = ev.ev(tree, {})
    except MParseError as exc:
        res.problems.append(str(exc))
        res.sources = list(ev.sources)
        return res
    except RecursionError:
        res.problems.append("the query is nested too deeply to analyse")
        res.sources = list(ev.sources)
        return res
    except Exception as exc:  # noqa: BLE001 - analyzers never raise
        res.problems.append(f"the query could not be analysed ({type(exc).__name__})")
        res.sources = list(ev.sources)
        return res
    ev.finish_connections()
    res.sources = list(ev.sources)
    if isinstance(val, MTable):
        res.table = val
    elif isinstance(val, MConn):
        res.connection = val
        res.table = MTable(sources=list(ev.sources), closed=True, reason="the query returns a connection, not a table")
    else:
        res.table = MTable(sources=list(ev.sources), closed=True, reason="the query result could not be determined")
    return res


def entity_source(conn: MConn, schema: str | None, entity: str, *, ctx: LineageContext) -> MSource | None:
    """The table a Direct Lake ``entityName`` / ``schemaName`` pair denotes on connection ``conn`` (the connection an ``expressionSource`` query returns).

    A Fabric SQL endpoint (``Sql.Database``) gives a lakehouse table (``approx`` set when the database is an endpoint id that cannot be mapped to a lakehouse by name); a OneLake
    ``AzureStorage.DataLake`` URL gives the Delta table below ``Tables/``; any other connection gives ``None``.
    """
    ev = _Eval("", ctx, MResolver(), None, 0, ())
    if conn.kind == "sql":
        ref, approx = ev.table_ref(conn, schema, entity)
        label = f"{schema + '.' if schema else ''}{entity}"
        return MSource(ref, "table" if ref.type == "table" else "external", label, approx)
    if conn.kind == "storage" and conn.url and "onelake.dfs.fabric.microsoft.com" in conn.url.lower():
        base = _onelake_abfss(conn.url or "").rstrip("/")
        path = f"{base}/Tables/{schema + '/' if schema and schema.lower() != 'dbo' else ''}{entity}"
        target = classify_path(path)
        ref = target.ref(ctx)
        return MSource(ref, "table" if ref.type == "table" else "file", target.name)
    return None


__all__ = ["MConn", "MOrigin", "MParseError", "MResolver", "MResult", "MSource", "MTable", "analyze_m", "clip", "entity_source", "parse_m"]
