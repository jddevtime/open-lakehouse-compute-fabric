# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Static analysis of Python / PySpark code: which datasets it reads and writes and which columns feed which target columns. Nothing is executed or imported.

:func:`analyze_python` ``(source, *, context) -> Facts`` parses the code with :mod:`ast` and *abstractly interprets* it (see :mod:`olcf.intelligence.lineage.pyvalues` for the
value domain): constants are propagated through assignments, f-strings, ``str.format`` and ``%``, function definitions are inlined when they are called (depth limited,
defaults honoured), loops over constant collections are unrolled, ``if`` branches are both followed, and the DataFrame API is modelled column by column. Notebooks run their
cells through one :class:`PyAnalyzer` so that variables, functions and temp views are shared across cells exactly like in a kernel.

What is recognised
------------------
Reads: ``spark.table``, ``spark.read.table``, ``spark.read.format(..).load(path)``, ``.csv / .parquet / .json / .orc / .text / .avro``, ``spark.readStream``, ``DeltaTable.forName / forPath``
(``.toDF()``), ``deltalake.DeltaTable(path)`` (data access), pandas ``read_parquet / read_csv / read_json / read_excel``, DuckDB ``con.sql / execute / read_parquet / read_csv`` (SQL strings go
through the SQL analyzer, with DataFrame variables resolved as DuckDB replacement scans and ``con.register`` views), ``notebookutils.fs.cp / mv / head``, JDBC sources
(``external`` dataset). Writes: ``df.write.saveAsTable / insertInto / save / parquet / csv / json / orc``, ``format(..).mode(..).partitionBy(..)`` chains, ``df.writeTo(..).create / append /
createOrReplace``, ``writeStream.toTable / start``, Delta ``merge / update / delete`` builders, ``write_deltalake``, pandas ``to_parquet / to_csv / to_json``, ``notebookutils.fs.put / cp / mv``.
SQL: ``spark.sql``, ``selectExpr``, ``F.expr``, ``DataFrame.filter("..")`` strings, with DataFrame variables available as temp views (``createOrReplaceTempView``).
Control flow between notebooks: ``notebookutils.notebook.run`` / ``runMultiple`` / ``mssparkutils.notebook.run`` -> ``DEPENDS_ON``.

Column lineage is tracked through ``select``, ``selectExpr``, ``withColumn``, ``withColumns``, ``withColumnRenamed``, ``toDF``, ``drop``, ``alias``, ``filter`` (no column change), ``join`` (both sides, ``USING``
columns merged), ``groupBy().agg(F.sum(..).alias(..))`` / dict ``agg`` / ``count`` / ``sum`` ..., ``union`` / ``unionByName``, ``distinct``, ``orderBy``, ``limit``, window functions
(``F.row_number().over(Window.partitionBy(..).orderBy(..))``), ``F.when/otherwise``, ``F.expr``, ``F.col``, ``df["c"]``, ``df.c``, casts, arithmetic, UDF calls, and basic pandas
(``df["c"] = expr``, ``assign``, ``rename``, ``drop``, ``merge``, ``concat``).

Honesty
-------
* A table or path name that is not a compile-time constant gives a ``dynamic_reference`` gap and no edge; a name that depends on a **notebook parameter** is analysed with the
  parameter's default and the edge is ``inferred`` (a pipeline may pass another value).
* A path whose root is computed at run time but whose last segment is a constant (``Path(root) / "orders"``) read as a Delta table gives an ``inferred`` table edge plus a gap.
* A DataFrame whose columns are not statically known (``SELECT *`` over an unknown schema, ``mapInPandas``, ``rdd``, pivot) is written with table-level lineage and a gap; no
  column edge is invented. Functions that are defined but never called in the analysed code are analysed with unknown arguments and their edges are ``inferred``.
* Not modelled: classes and methods, ``eval`` / ``exec`` (reported), decorators other than ``udf``, async code, most of pandas and NumPy, structured-streaming sources other than files / Delta,
  Spark Connect specifics, dynamic attribute access (``getattr(df, name)``).
"""

from __future__ import annotations

import ast
import re
import string
import warnings
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from olcf.intelligence.lineage.base import (
    MAX_PY_CHARS,
    PYSPARK,
    FactBuilder,
    LineageContext,
    PyContext,
    statement_alias,
)
from olcf.intelligence.lineage.pyvalues import (
    COLUMN_METHODS,
    DF,
    DF_ACTIONS,
    DF_PASSTHROUGH,
    FUNC_SPEC,
    HOLE,
    KNOWN_FUNCS,
    NOARG_FUNCS,
    PANDAS_PASSTHROUGH,
    SPARK,
    UNK,
    ColV,
    ConnV,
    DeltaTbl,
    FuncV,
    Grouped,
    MergeB,
    ModuleRef,
    NaV,
    PStr,
    RddV,
    Reader,
    SchemaV,
    SparkV,
    UdfV,
    WinV,
    Writer,
    combine,
    concat_str,
    mk_str,
    render_value,
    str_parts,
)
from olcf.intelligence.lineage.relation import (
    ColInfo,
    Dataset,
    Relation,
    add_step,
    concat,
    opaque_relation,
)
from olcf.intelligence.lineage.sql import UNKNOWN_MARK, SqlSession, _Stmt, parse_hazard, split_statements
from olcf.intelligence.model import AssetFact, AssetRef, Facts, clip

MAX_STEPS = 600_000          # abstract evaluation steps per analysis
MAX_CALL_DEPTH = 8
MAX_UNROLL = 200             # loop iterations followed one by one
MAX_COMP = 2_000             # elements of a comprehension
_PARAM_TAINT_MIN = 1

_PY_DATA_ENGINES = {"pandas", "pd"}


class _Return(Exception):  # noqa: N818 - control flow signal, not an error
    """Control-flow signal for ``return``."""


class _Break(Exception):  # noqa: N818
    """Control-flow signal for ``break``."""


class _Continue(Exception):  # noqa: N818
    """Control-flow signal for ``continue``."""


class _Budget(Exception):  # noqa: N818
    """Raised when the evaluation budget is exhausted."""


@dataclass
class PyEnv:
    """A Python scope: variables and the enclosing scope."""

    vars: dict[str, Any] = field(default_factory=dict)
    parent: PyEnv | None = None
    globals_declared: set[str] = field(default_factory=set)

    def lookup(self, name: str) -> Any:
        """Value of ``name`` (searching enclosing scopes) or the sentinel ``_MISSING``."""
        e: PyEnv | None = self
        while e is not None:
            if name in e.vars:
                return e.vars[name]
            e = e.parent
        return _MISSING

    def root(self) -> PyEnv:
        """The module scope."""
        e = self
        while e.parent is not None:
            e = e.parent
        return e

    def set(self, name: str, value: Any) -> None:
        """Bind ``name`` (in the module scope when declared ``global``)."""
        if name in self.globals_declared:
            self.root().vars[name] = value
        else:
            self.vars[name] = value


_MISSING = object()


@dataclass
class _Src:
    """The source text of the cell a node belongs to (evidence needs its cell index and the line text)."""

    cell: int | None
    lines: list[bytes]


@dataclass
class _Frame:
    """A function call in progress: the values it returned."""

    returns: list[Any] = field(default_factory=list)


# ================================================================================================================================== helpers
def text_of(v: Any) -> str | None:
    """The text of a fully known string value (``str`` or a ``PStr`` without holes), else ``None``."""
    if isinstance(v, str):
        return v
    if isinstance(v, PStr):
        return v.text()
    return None


def _split_dotted(name: str) -> list[str]:
    """Split ``a.b.c`` on dots outside back-ticks / brackets, unquoting each part."""
    parts = [p.strip().strip("`\"[]") for p in re.split(r"\.(?=(?:[^`]*`[^`]*`)*[^`]*$)", name.strip())]
    return [p for p in parts if p]


def _stable(items: Any) -> list[Any]:
    """Deterministic list from a set (sorted by repr) or any iterable."""
    if isinstance(items, (set, frozenset)):
        return sorted(items, key=repr)
    return list(items)


def merge_values(a: Any, b: Any) -> Any:
    """The value of a variable after two branches assigned ``a`` and ``b``: equal values stay, two DataFrames are merged, anything else is unknown."""
    if a is b:
        return a
    try:
        if a == b:
            return a
    except Exception:  # noqa: BLE001 - comparing exotic values
        return UNK
    if isinstance(a, DF) and isinstance(b, DF) and a.kind == b.kind:
        return DF(merge_relations(a.rel, b.rel), a.kind)
    return UNK


def merge_relations(a: Relation, b: Relation) -> Relation:
    """Union of two relations that are alternatives (branches): same column names merge their origins, otherwise the result is opaque."""
    if a == b:
        return a
    if a.positional_ok and b.positional_ok and [c.name.lower() for c in a.cols] == [c.name.lower() for c in b.cols]:
        cols = tuple(ColInfo(x.name, x.origins | y.origins, x.partial or y.partial, x.expr if x.expr == y.expr else None) for x, y in zip(a.cols, b.cols, strict=True))
        return Relation(cols, (), a.datasets | b.datasets)
    return opaque_relation(a.datasets | b.datasets)


class PyAnalyzer:
    """Abstract interpreter for one Python program or one notebook (cells are run in order through :meth:`run_cell`)."""

    def __init__(self, ctx: LineageContext, builder: FactBuilder, process: AssetRef, *, dialect: str = "spark") -> None:
        self.ctx = ctx
        self.b = builder
        self.process = process
        self.session = SqlSession(ctx, builder, dialect, analyzer=PYSPARK, emit_query_assets=False)
        self.session.st = _Stmt(process=process, loc=ctx.loc(), text="", index=0, line=0)
        self.conns: dict[int, SqlSession] = {}
        self.env = PyEnv()
        self.steps = 0
        self.depth = 0
        self.frames: list[_Frame] = []
        self.funcs: list[FuncV] = []
        self.called: set[int] = set()
        self.func_stack: list[int] = []
        self.src = _Src(None, [])
        self.speculative = False
        self.parameters: dict[str, Any] = {}
        self.param_names: set[str] = set()
        self.notebook_calls: list[tuple[str, int | None, int | None]] = []
        self._uid = 0
        self._hole = 0
        self.budget_hit = False
        self._bind_builtins()

    # ============================================================================================================================ setup
    def _bind_builtins(self) -> None:
        g = self.env.vars
        g["spark"] = SPARK
        g["sqlContext"] = SPARK
        g["__name__"] = "__main__"
        for name in ("notebookutils", "mssparkutils", "dbutils"):
            g[name] = ModuleRef("notebookutils" if name != "dbutils" else "dbutils")
        for name in ("sc", "display", "displayHTML", "sparkContext", "get_ipython"):
            g[name] = UNK

    # ============================================================================================================================ gaps / locations
    def loc(self, node: Any = None) -> dict[str, Any]:
        """Evidence location of a node: the notebook cell (if any) and the 1-based line inside it."""
        line = getattr(node, "lineno", None) if node is not None else None
        return self.ctx.loc(cell=self.src.cell, line=line)

    def snippet(self, node: Any) -> str:
        """The source text of a node (first line(s) of it), clipped."""
        if node is None or not hasattr(node, "lineno"):
            return ""
        try:
            first, last = node.lineno - 1, getattr(node, "end_lineno", node.lineno) - 1
            lines = self.src.lines
            if first >= len(lines):
                return ""
            if first == last:
                seg = lines[first][node.col_offset:getattr(node, "end_col_offset", None)]
            else:
                seg = b" ".join([lines[first][node.col_offset:], *lines[first + 1:min(last, first + 6)]])
            return seg.decode("utf-8", "replace")
        except Exception:  # noqa: BLE001
            return ""

    def gap(self, kind: str, subject: str, detail: str, remediation: str | None, node: ast.AST | None) -> None:
        """Record a gap unless the analyzer is following code that was never called (speculative mode)."""
        if self.speculative:
            return
        self.b.gap(kind, clip(subject, 120) or "?", detail, remediation, self.loc(node), PYSPARK)

    def at(self, node: ast.AST | None, taint: tuple[str, ...] = ()) -> None:
        """Make ``node`` the current site for edges emitted through the SQL session (location, snippet)."""
        s = self.session
        s.st = _Stmt(process=self.process, loc=self.loc(node), text=self.snippet(node), index=0, line=getattr(node, "lineno", 0))
        s.taint = taint

    def tick(self) -> None:
        """Charge one evaluation step; abort the analysis when the budget is gone."""
        self.steps += 1
        if self.steps > MAX_STEPS:
            raise _Budget

    # ============================================================================================================================ cells
    def run_cell(self, source: str, cell: int | None, *, parameters: bool = False) -> None:
        """Parse and interpret one block of code in the shared module scope. A syntax error is a ``parse_error`` gap; the rest of the notebook is still analysed."""
        saved = self.src
        self.src = _Src(cell, [ln.encode("utf-8") for ln in source.split("\n")])
        try:
            if len(source) > MAX_PY_CHARS:
                self.b.gap("unsupported_construct", f"cell {cell}" if cell is not None else "python", f"the code is larger than {MAX_PY_CHARS} characters and was not analysed",
                           "Split the notebook.", self.ctx.loc(cell=cell), PYSPARK)
                return
            try:
                with warnings.catch_warnings():      # the code under analysis is not ours: its SyntaxWarnings (invalid escapes ...) must not reach stderr
                    warnings.simplefilter("ignore")
                    tree = ast.parse(source)
            except (SyntaxError, ValueError, MemoryError, RecursionError) as exc:
                line = getattr(exc, "lineno", None)
                self.b.gap("parse_error", f"cell {cell}" if cell is not None else "python", f"the code cannot be parsed as Python: {type(exc).__name__}: {str(exc)[:160]}",
                           "Fix the syntax error; cells that use magics other than %%sql / %%pyspark / %run are not analysed.", self.ctx.loc(cell=cell, line=line), PYSPARK)
                return
            try:
                self.exec_block(tree.body, self.env)
                if parameters:
                    self._mark_parameters(tree)
            except _Budget:
                self.budget_hit = True
                self.b.gap("unsupported_construct", f"cell {cell}" if cell is not None else "python",
                           "the analysis budget was exhausted (very long loops or huge code); the rest of this cell was not analysed", "Split the notebook.",
                           self.ctx.loc(cell=cell), PYSPARK)
            except RecursionError:
                self.b.gap("parse_error", f"cell {cell}" if cell is not None else "python", "the code is nested too deeply to analyse", None, self.ctx.loc(cell=cell), PYSPARK)
            except (_Return, _Break, _Continue):
                pass
        finally:
            self.src = saved

    def _mark_parameters(self, tree: ast.Module) -> None:
        """After a parameters cell: remember its variables, apply caller overrides and mark string values as parameter defaults (edges derived from them are ``inferred``)."""
        names: list[str] = []
        for st in tree.body:
            targets = st.targets if isinstance(st, ast.Assign) else [st.target] if isinstance(st, ast.AnnAssign) else []
            for t in targets:
                if isinstance(t, ast.Name):
                    names.append(t.id)
        override = getattr(self.ctx, "parameters", {}) or {}
        for n in names:
            if n in override:
                self.env.vars[n] = override[n]
            self.env.vars[n] = self._taint(self.env.vars.get(n))
            self.param_names.add(n)
            self.parameters[n] = self.env.vars[n]

    def _taint(self, v: Any) -> Any:
        if isinstance(v, str) and len(v) >= _PARAM_TAINT_MIN:
            return PStr((v,), (v,))
        if isinstance(v, list):
            return [self._taint(x) for x in v]
        if isinstance(v, tuple):
            return tuple(self._taint(x) for x in v)
        if isinstance(v, dict):
            return {k: self._taint(x) for k, x in v.items()}
        return v

    def finish(self) -> None:
        """Analyse functions that were defined but never called with unknown arguments; their edges are ``inferred`` and their gaps are suppressed."""
        for f in list(self.funcs):
            if f.uid in self.called or not isinstance(f.node, ast.FunctionDef):
                continue
            saved_src, saved_cap = self.src, self.session.confidence_cap
            self.speculative = True
            self.session.confidence_cap = "inferred"
            self.b.muted = True
            try:
                if isinstance(f.origin, _Src):
                    self.src = f.origin
                self.call_func(f, [], {}, None, speculative=True)
            except (_Budget, RecursionError, _Return, _Break, _Continue):
                pass
            finally:
                self.speculative = False
                self.b.muted = False
                self.session.confidence_cap = saved_cap
                self.src = saved_src

    # ============================================================================================================================ statements
    def exec_block(self, body: list[ast.stmt], env: PyEnv) -> None:
        """Interpret a list of statements."""
        for st in body:
            self.tick()
            m = getattr(self, "_s_" + type(st).__name__, None)
            if m is not None:
                m(st, env)

    def _s_Expr(self, node: ast.Expr, env: PyEnv) -> None:
        self.ev(node.value, env)

    def _s_Pass(self, node: ast.Pass, env: PyEnv) -> None:
        return None

    def _s_Break(self, node: ast.Break, env: PyEnv) -> None:
        raise _Break

    def _s_Continue(self, node: ast.Continue, env: PyEnv) -> None:
        raise _Continue

    def _s_Assign(self, node: ast.Assign, env: PyEnv) -> None:
        v = self.ev(node.value, env)
        for t in node.targets:
            self.assign(t, v, env)

    def _s_AnnAssign(self, node: ast.AnnAssign, env: PyEnv) -> None:
        if node.value is not None:
            self.assign(node.target, self.ev(node.value, env), env)

    def _s_AugAssign(self, node: ast.AugAssign, env: PyEnv) -> None:
        cur = self.ev(ast.Name(id=node.target.id, ctx=ast.Load()), env) if isinstance(node.target, ast.Name) else self.ev(node.target, env)
        new = self.binop(node.op, cur, self.ev(node.value, env))
        if isinstance(cur, list) and isinstance(node.op, ast.Add):
            cur_list = cur
            addition = self.ev(node.value, env)
            if isinstance(addition, (list, tuple)):
                cur_list.extend(addition)
                new = cur_list
        self.assign(node.target, new, env)

    def _s_Import(self, node: ast.Import, env: PyEnv) -> None:
        for a in node.names:
            if a.asname:
                env.set(a.asname, ModuleRef(a.name))
            else:
                env.set(a.name.split(".")[0], ModuleRef(a.name.split(".")[0]))

    def _s_ImportFrom(self, node: ast.ImportFrom, env: PyEnv) -> None:
        mod = ("." * node.level) + (node.module or "")
        for a in node.names:
            if a.name == "*":
                if mod == "pyspark.sql.functions":
                    for fn in KNOWN_FUNCS:
                        if fn not in ("sum", "max", "min", "round", "filter", "abs", "any", "hash", "exists", "map", "len"):
                            env.set(fn, ModuleRef(f"pyspark.sql.functions.{fn}"))
                    for fn in ("sum", "max", "min", "round", "abs"):
                        env.set(fn, ModuleRef(f"pyspark.sql.functions.{fn}"))
                elif mod == "pyspark.sql.types":
                    for t in ("StructType", "StructField", "StringType", "IntegerType", "LongType", "DoubleType", "FloatType", "BooleanType", "DateType", "TimestampType",
                              "DecimalType", "ArrayType", "MapType", "BinaryType", "ShortType", "ByteType"):
                        env.set(t, ModuleRef(f"pyspark.sql.types.{t}"))
                continue
            env.set(a.asname or a.name, ModuleRef(f"{mod}.{a.name}") if not node.level else UNK)

    def _s_FunctionDef(self, node: ast.FunctionDef, env: PyEnv) -> None:
        defaults = tuple(self.ev(d, env) for d in node.args.defaults)
        kw_defaults = tuple((a.arg, self.ev(d, env)) for a, d in zip(node.args.kwonlyargs, node.args.kw_defaults, strict=True) if d is not None)
        for d in node.decorator_list:
            target = d.func if isinstance(d, ast.Call) else d
            dv = self.ev(target, env)
            if isinstance(dv, ModuleRef) and dv.path.rsplit(".", 1)[-1] in ("udf", "pandas_udf"):
                env.set(node.name, UdfV(node.name))
                return
        self._uid += 1
        f = FuncV(node.name, node, env, defaults, kw_defaults, self.src, self._uid)
        env.set(node.name, f)
        if env is self.env:
            self.funcs.append(f)

    _s_AsyncFunctionDef = _s_FunctionDef  # noqa: N815

    def _s_ClassDef(self, node: ast.ClassDef, env: PyEnv) -> None:
        env.set(node.name, UNK)

    def _s_Return(self, node: ast.Return, env: PyEnv) -> None:
        val = self.ev(node.value, env) if node.value is not None else None
        if self.frames:
            self.frames[-1].returns.append(val)
        raise _Return

    def _s_Global(self, node: ast.Global, env: PyEnv) -> None:
        env.globals_declared.update(node.names)

    _s_Nonlocal = _s_Global  # noqa: N815

    def _s_Delete(self, node: ast.Delete, env: PyEnv) -> None:
        for t in node.targets:
            if isinstance(t, ast.Name):
                env.vars.pop(t.id, None)

    def _s_Assert(self, node: ast.Assert, env: PyEnv) -> None:
        self.ev(node.test, env)

    def _s_Raise(self, node: ast.Raise, env: PyEnv) -> None:
        return None

    def _s_If(self, node: ast.If, env: PyEnv) -> None:
        test = self.ev(node.test, env)
        known = self._truth(test)
        if known is True:
            self.exec_block(node.body, env)
        elif known is False:
            self.exec_block(node.orelse, env)
        else:
            self.exec_branches([node.body, node.orelse], env)

    def _truth(self, v: Any) -> bool | None:
        """Truth value of a fully known value; ``None`` when unknown."""
        if v is UNK or isinstance(v, (ColV, DF)):
            return None
        if isinstance(v, PStr):
            t = v.text()
            return None if t is None else bool(t)
        if isinstance(v, (bool, int, float, str, type(None), list, tuple, dict, set)):
            return bool(v)
        return None

    def exec_branches(self, branches: list[list[ast.stmt]], env: PyEnv) -> None:
        """Follow every branch from the same starting state and merge the variables afterwards (a variable that differs between branches becomes unknown)."""
        before = dict(env.vars)
        outcomes: list[dict[str, Any]] = []
        for body in branches:
            env.vars = dict(before)
            terminated = False
            try:
                self.exec_block(body, env)
            except (_Return, _Break, _Continue):
                terminated = True
            if not terminated:
                outcomes.append(env.vars)
        if not outcomes:
            env.vars = before
            raise _Return if self.frames else _Break
        merged = dict(outcomes[0])
        for other in outcomes[1:]:
            for k in set(merged) | set(other):
                if k in merged and k in other:
                    merged[k] = merge_values(merged[k], other[k])
                else:
                    merged[k] = UNK if k not in before or (k in merged and merged[k] is not before.get(k)) or k in other else merged[k]
        env.vars = merged

    def _s_For(self, node: ast.For, env: PyEnv) -> None:
        it = self.ev(node.iter, env)
        items = self.iterate(it)
        if items is None:
            self.assign(node.target, UNK, env)
            try:
                self.exec_block(node.body, env)
            except (_Break, _Continue):
                pass
            self.exec_block(node.orelse, env)
            return
        done = True
        for i, item in enumerate(items):
            if i >= MAX_UNROLL:
                self.assign(node.target, UNK, env)
                try:
                    self.exec_block(node.body, env)
                except (_Break, _Continue):
                    pass
                break
            self.assign(node.target, item, env)
            try:
                self.exec_block(node.body, env)
            except _Break:
                done = False
                break
            except _Continue:
                continue
        if done:
            self.exec_block(node.orelse, env)

    _s_AsyncFor = _s_For  # noqa: N815

    def _s_While(self, node: ast.While, env: PyEnv) -> None:
        test = self.ev(node.test, env)
        if self._truth(test) is False:
            self.exec_block(node.orelse, env)
            return
        try:
            self.exec_block(node.body, env)
        except (_Break, _Continue):
            pass

    def _s_Try(self, node: ast.Try, env: PyEnv) -> None:
        self.exec_block(node.body, env)
        for h in node.handlers:
            if h.name:
                env.set(h.name, UNK)
            self.exec_block(h.body, env)
        self.exec_block(node.orelse, env)
        self.exec_block(node.finalbody, env)

    _s_TryStar = _s_Try  # noqa: N815

    def _s_With(self, node: ast.With, env: PyEnv) -> None:
        for item in node.items:
            v = self.ev(item.context_expr, env)
            if item.optional_vars is not None:
                self.assign(item.optional_vars, v, env)
        self.exec_block(node.body, env)

    _s_AsyncWith = _s_With  # noqa: N815

    def _s_Match(self, node: ast.Match, env: PyEnv) -> None:
        self.ev(node.subject, env)
        self.exec_branches([c.body for c in node.cases] or [[]], env)

    # ---- assignment
    def assign(self, target: ast.AST, v: Any, env: PyEnv) -> None:
        """Bind ``v`` to an assignment target (names, tuple unpacking, item assignment on known containers and pandas frames)."""
        if isinstance(target, ast.Name):
            env.set(target.id, v)
        elif isinstance(target, (ast.Tuple, ast.List)):
            elts = target.elts
            star = next((i for i, e in enumerate(elts) if isinstance(e, ast.Starred)), None)
            if isinstance(v, (list, tuple)) and star is None and len(v) == len(elts):
                for e, x in zip(elts, v, strict=True):
                    self.assign(e, x, env)
            elif isinstance(v, (list, tuple)) and star is not None and len(v) >= len(elts) - 1:
                for i, e in enumerate(elts[:star]):
                    self.assign(e, v[i], env)
                tail = len(elts) - star - 1
                star_node = elts[star]
                self.assign(star_node.value if isinstance(star_node, ast.Starred) else star_node, list(v[star:len(v) - tail]), env)
                for j, e in enumerate(elts[star + 1:]):
                    self.assign(e, v[len(v) - tail + j], env)
            else:
                for e in elts:
                    self.assign(e.value if isinstance(e, ast.Starred) else e, UNK, env)
        elif isinstance(target, ast.Subscript):
            base = self.ev(target.value, env)
            key = self.ev(target.slice, env)
            if isinstance(base, dict):
                k = text_of(key) if isinstance(key, (str, PStr)) else key
                if k is not None and not isinstance(k, (list, dict)) and k is not UNK:
                    base[k] = v
            elif isinstance(base, list) and isinstance(key, int) and -len(base) <= key < len(base):
                base[key] = v
            elif isinstance(base, DF) and isinstance(target.value, ast.Name):
                name = text_of(key)
                if name is not None:
                    env.set(target.value.id, self.df_with_column(base, name, v, target))
        # attribute targets: object state is not tracked

    # ============================================================================================================================ expressions
    def ev(self, node: ast.AST | None, env: PyEnv) -> Any:
        """Abstractly evaluate an expression."""
        if node is None:
            return None
        self.tick()
        m = getattr(self, "_e_" + type(node).__name__, None)
        if m is None:
            return UNK
        return m(node, env)

    def _e_Constant(self, node: ast.Constant, env: PyEnv) -> Any:
        v = node.value
        return v if isinstance(v, (str, int, float, bool, type(None))) else UNK

    def _e_Name(self, node: ast.Name, env: PyEnv) -> Any:
        v = env.lookup(node.id)
        if v is not _MISSING:
            return v
        if node.id in _BUILTIN_NAMES:
            return ModuleRef("builtins." + node.id)
        return UNK

    def _e_Attribute(self, node: ast.Attribute, env: PyEnv) -> Any:
        base = self.ev(node.value, env)
        return self.attribute(base, node.attr, node)

    def attribute(self, base: Any, name: str, node: ast.AST) -> Any:
        """Value of ``base.name`` when it is not called."""
        if isinstance(base, ModuleRef):
            return ModuleRef(f"{base.path}.{name}")
        if isinstance(base, SparkV):
            if name == "read":
                return Reader()
            if name == "readStream":
                return Reader(stream=True)
            if name in ("catalog", "conf", "udf", "streams"):
                return ModuleRef(f"spark.{name}")
            return UNK
        if isinstance(base, DF):
            if name == "write":
                return Writer(base)
            if name == "writeStream":
                return Writer(base, stream=True)
            if name in ("na", "stat"):
                return NaV(base)
            if name == "rdd":
                return RddV(base.rel.datasets)
            if name == "columns":
                return base.rel.names if base.rel.positional_ok else UNK
            if name in ("schema", "dtypes", "shape", "index", "values", "T", "iloc", "loc", "dtype", "size", "ndim", "empty", "isStreaming"):
                return UNK
            return self.df_col(base, name, node)
        if isinstance(base, ColV):
            return UNK
        if isinstance(base, DeltaTbl):
            return UNK
        return UNK

    def _e_Subscript(self, node: ast.Subscript, env: PyEnv) -> Any:
        base = self.ev(node.value, env)
        if isinstance(node.slice, ast.Slice):
            for part in (node.slice.lower, node.slice.upper, node.slice.step):
                if part is not None:
                    self.ev(part, env)
            if isinstance(base, DF):
                return base
            return UNK
        key = self.ev(node.slice, env)
        if isinstance(base, (list, tuple)) and isinstance(key, int) and not isinstance(key, bool):
            return base[key] if -len(base) <= key < len(base) else UNK
        if isinstance(base, dict):
            k = text_of(key) if isinstance(key, (str, PStr)) else key
            try:
                return base[k] if k in base else UNK
            except TypeError:
                return UNK
        if isinstance(base, DF):
            return self.df_subscript(base, key, node)
        if isinstance(base, ColV):
            return combine(f"{base.text}[{render_value(key)}]", [base, key])
        return UNK

    def _e_Call(self, node: ast.Call, env: PyEnv) -> Any:
        args: list[Any] = []
        arg_nodes: list[ast.AST] = []
        for a in node.args:
            if isinstance(a, ast.Starred):
                v = self.ev(a.value, env)
                if isinstance(v, (list, tuple)):
                    args.extend(v)
                    arg_nodes.extend([a.value] * len(v))
                else:
                    args.append(UNK)
                    arg_nodes.append(a.value)
            else:
                args.append(self.ev(a, env))
                arg_nodes.append(a)
        kwargs: dict[str, Any] = {}
        for k in node.keywords:
            v = self.ev(k.value, env)
            if k.arg is None:
                if isinstance(v, dict):
                    for kk, vv in v.items():
                        if isinstance(kk, str):
                            kwargs[kk] = vv
            else:
                kwargs[k.arg] = v
        fn = node.func
        if isinstance(fn, ast.Attribute):
            recv = self.ev(fn.value, env)
            return self.call_method(recv, fn.attr, args, kwargs, node, arg_nodes, env)
        f = self.ev(fn, env)
        return self.call_value(f, args, kwargs, node, arg_nodes, env)

    def _e_BinOp(self, node: ast.BinOp, env: PyEnv) -> Any:
        return self.binop(node.op, self.ev(node.left, env), self.ev(node.right, env))

    def _e_UnaryOp(self, node: ast.UnaryOp, env: PyEnv) -> Any:
        v = self.ev(node.operand, env)
        if isinstance(v, ColV):
            sym = {ast.Not: "NOT ", ast.USub: "-", ast.Invert: "NOT ", ast.UAdd: "+"}.get(type(node.op), "")
            return combine(f"{sym}{v.text}", [v])
        if isinstance(node.op, ast.Not):
            t = self._truth(v)
            return (not t) if t is not None else UNK
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            if isinstance(node.op, ast.USub):
                return -v
            if isinstance(node.op, ast.UAdd):
                return +v
            if isinstance(node.op, ast.Invert) and isinstance(v, int):
                return ~v
        return UNK

    def _e_BoolOp(self, node: ast.BoolOp, env: PyEnv) -> Any:
        is_and = isinstance(node.op, ast.And)
        last: Any = UNK
        for i, v_node in enumerate(node.values):
            v = self.ev(v_node, env)
            t = self._truth(v)
            if t is None:
                for rest in node.values[i + 1:]:
                    self.ev(rest, env)
                return UNK
            last = v
            if is_and and not t:
                return v
            if not is_and and t:
                return v
        return last

    def _e_Compare(self, node: ast.Compare, env: PyEnv) -> Any:
        left = self.ev(node.left, env)
        result: Any = True
        for op, comp_node in zip(node.ops, node.comparators, strict=True):
            right = self.ev(comp_node, env)
            if isinstance(left, ColV) or isinstance(right, ColV):
                sym = {ast.Eq: "=", ast.NotEq: "<>", ast.Lt: "<", ast.LtE: "<=", ast.Gt: ">", ast.GtE: ">="}.get(type(op), "?")
                return combine(f"({render_value(left)} {sym} {render_value(right)})", [left, right])
            if isinstance(left, DF) and isinstance(right, (int, float, str)):
                return UNK
            lv: Any = text_of(left) if isinstance(left, (str, PStr)) else left
            rv: Any = text_of(right) if isinstance(right, (str, PStr)) else right
            if left is UNK or right is UNK or lv is None and isinstance(left, PStr) or rv is None and isinstance(right, PStr):
                return UNK
            try:
                if isinstance(op, ast.Eq):
                    r = lv == rv
                elif isinstance(op, ast.NotEq):
                    r = lv != rv
                elif isinstance(op, ast.Lt):
                    r = lv < rv
                elif isinstance(op, ast.LtE):
                    r = lv <= rv
                elif isinstance(op, ast.Gt):
                    r = lv > rv
                elif isinstance(op, ast.GtE):
                    r = lv >= rv
                elif isinstance(op, ast.Is):
                    r = lv is rv or (lv is None and rv is None)
                elif isinstance(op, ast.IsNot):
                    r = not (lv is rv or (lv is None and rv is None))
                elif isinstance(op, ast.In):
                    r = lv in rv
                elif isinstance(op, ast.NotIn):
                    r = lv not in rv
                else:
                    return UNK
            except Exception:  # noqa: BLE001 - incomparable known values
                return UNK
            if not r:
                return False
            result = r
            left = right
        return result

    def _e_IfExp(self, node: ast.IfExp, env: PyEnv) -> Any:
        t = self._truth(self.ev(node.test, env))
        if t is True:
            return self.ev(node.body, env)
        if t is False:
            return self.ev(node.orelse, env)
        return merge_values(self.ev(node.body, env), self.ev(node.orelse, env))

    def _e_JoinedStr(self, node: ast.JoinedStr, env: PyEnv) -> Any:
        frags: list[str | None] = []
        taint: tuple[str, ...] = ()
        for part in node.values:
            if isinstance(part, ast.Constant):
                frags.append(str(part.value))
                continue
            if not isinstance(part, ast.FormattedValue):
                frags.append(None)
                continue
            v = self.ev(part.value, env)
            spec_v = self.ev(part.format_spec, env) if part.format_spec is not None else ""
            spec = text_of(spec_v) if spec_v is not UNK else None
            pieces = self.format_value(v, part.conversion, spec)
            frags.extend(pieces[0])
            taint += pieces[1]
        return mk_str(frags, taint)

    def format_value(self, v: Any, conversion: int, spec: str | None) -> tuple[list[str | None], tuple[str, ...]]:
        """Fragments for one replacement field of an f-string / ``str.format``."""
        sp = str_parts(v)
        if sp is not None and conversion in (-1, 115) and not spec:
            return list(sp[0]), sp[1]
        if sp is not None and conversion == 114:
            txt = text_of(v)
            return ([repr(txt)], sp[1]) if txt is not None else ([None], sp[1])
        if isinstance(v, (int, float, bool)) or v is None:
            try:
                base = repr(v) if conversion == 114 else v
                return [format(base, spec or "")], ()
            except (ValueError, TypeError):
                return [None], ()
        if sp is not None and spec:
            txt = text_of(v)
            if txt is not None:
                try:
                    return [format(txt, spec)], sp[1]
                except (ValueError, TypeError):
                    return [None], sp[1]
        return [None], ()

    def _e_FormattedValue(self, node: ast.FormattedValue, env: PyEnv) -> Any:
        return UNK

    def _e_List(self, node: ast.List, env: PyEnv) -> Any:
        return self.seq(node.elts, env)

    def _e_Tuple(self, node: ast.Tuple, env: PyEnv) -> Any:
        return tuple(self.seq(node.elts, env))

    def _e_Set(self, node: ast.Set, env: PyEnv) -> Any:
        out: set[Any] = set()
        for v in self.seq(node.elts, env):
            try:
                out.add(text_of(v) if isinstance(v, PStr) and text_of(v) is not None else v)
            except TypeError:
                return UNK
        return out

    def seq(self, elts: list[ast.expr], env: PyEnv) -> list[Any]:
        """Evaluate list elements (``*x`` expands known lists)."""
        out: list[Any] = []
        for e in elts:
            if isinstance(e, ast.Starred):
                v = self.ev(e.value, env)
                if isinstance(v, (list, tuple)):
                    out.extend(v)
                else:
                    out.append(UNK)
            else:
                out.append(self.ev(e, env))
        return out

    def _e_Dict(self, node: ast.Dict, env: PyEnv) -> Any:
        out: dict[Any, Any] = {}
        for k, v in zip(node.keys, node.values, strict=True):
            val = self.ev(v, env)
            if k is None:
                if isinstance(val, dict):
                    out.update(val)
                continue
            key = self.ev(k, env)
            kk = text_of(key) if isinstance(key, (str, PStr)) else key
            if kk is None or kk is UNK:
                continue
            try:
                out[kk] = val
            except TypeError:
                continue
        return out

    def _e_Lambda(self, node: ast.Lambda, env: PyEnv) -> Any:
        self._uid += 1
        defaults = tuple(self.ev(d, env) for d in node.args.defaults)
        return FuncV("<lambda>", node, env, defaults, (), self.src, self._uid)

    def _e_NamedExpr(self, node: ast.NamedExpr, env: PyEnv) -> Any:
        v = self.ev(node.value, env)
        self.assign(node.target, v, env)
        return v

    def _e_Await(self, node: ast.Await, env: PyEnv) -> Any:
        return self.ev(node.value, env)

    def _e_Starred(self, node: ast.Starred, env: PyEnv) -> Any:
        return self.ev(node.value, env)

    def _comp(self, generators: list[ast.comprehension], env: PyEnv, emit: Callable[[PyEnv], None], idx: int = 0, count: list[int] | None = None) -> None:
        counter = count if count is not None else [0]
        if idx == len(generators):
            counter[0] += 1
            if counter[0] > MAX_COMP:
                raise _Budget
            emit(env)
            return
        g = generators[idx]
        items = self.iterate(self.ev(g.iter, env))
        if items is None:
            items = [UNK]
        for item in items[:MAX_UNROLL]:
            inner = PyEnv({}, env)
            self.assign(g.target, item, inner)
            if all(self._truth(self.ev(c, inner)) is not False for c in g.ifs):
                self._comp(generators, inner, emit, idx + 1, counter)

    def _e_ListComp(self, node: ast.ListComp, env: PyEnv) -> Any:
        out: list[Any] = []
        self._comp(node.generators, PyEnv({}, env), lambda e: out.append(self.ev(node.elt, e)))
        return out

    _e_GeneratorExp = _e_ListComp  # noqa: N815

    def _e_SetComp(self, node: ast.SetComp, env: PyEnv) -> Any:
        out: list[Any] = []
        self._comp(node.generators, PyEnv({}, env), lambda e: out.append(self.ev(node.elt, e)))
        try:
            return {text_of(v) if isinstance(v, PStr) and text_of(v) is not None else v for v in out}
        except TypeError:
            return UNK

    def _e_DictComp(self, node: ast.DictComp, env: PyEnv) -> Any:
        out: dict[Any, Any] = {}

        def emit(e: PyEnv) -> None:
            k = self.ev(node.key, e)
            kk = text_of(k) if isinstance(k, (str, PStr)) else k
            if kk is not None and kk is not UNK:
                try:
                    out[kk] = self.ev(node.value, e)
                except TypeError:
                    pass

        self._comp(node.generators, PyEnv({}, env), emit)
        return out

    def iterate(self, v: Any) -> list[Any] | None:
        """The elements of a known iterable (lists, tuples, sets, dict keys, strings are not iterated), else ``None``."""
        if isinstance(v, (list, tuple)):
            return list(v)
        if isinstance(v, (set, frozenset)):
            return _stable(v)
        if isinstance(v, dict):
            return list(v.keys())
        if isinstance(v, _Range):
            return list(v.items)
        return None

    # ---- operators
    def binop(self, op: ast.operator, a: Any, b: Any) -> Any:
        """Binary operator on abstract values."""
        if isinstance(a, ColV) or isinstance(b, ColV):
            sym = {ast.Add: "+", ast.Sub: "-", ast.Mult: "*", ast.Div: "/", ast.Mod: "%", ast.Pow: "^", ast.BitAnd: "AND", ast.BitOr: "OR", ast.BitXor: "XOR",
                   ast.FloorDiv: "DIV"}.get(type(op), "?")
            return combine(f"({render_value(a)} {sym} {render_value(b)})", [a, b])
        if isinstance(op, ast.Mod) and str_parts(a) is not None:
            return self.percent_format(a, b)
        sa, sb = str_parts(a), str_parts(b)
        if isinstance(op, ast.Add):
            if sa is not None and sb is not None:
                return concat_str(a, b)
            if sa is not None and (b is UNK or isinstance(b, (int, float))) or sb is not None and a is UNK:
                return concat_str(a, b)
            if isinstance(a, list) and isinstance(b, list):
                return a + b
            if isinstance(a, tuple) and isinstance(b, tuple):
                return a + b
            if isinstance(a, (int, float)) and isinstance(b, (int, float)) and not isinstance(a, bool) and not isinstance(b, bool):
                return a + b
            return UNK
        if isinstance(op, ast.Div):
            if (sa is not None and sa[2]) or (sb is not None and sa is None and a is UNK) or (sa is not None and sb is not None and sa[2]):
                return concat_str(a, b, sep="/", path=True)
            if a is UNK and sb is not None:
                return concat_str(UNK, b, sep="/", path=True)
        if isinstance(a, (int, float)) and isinstance(b, (int, float)) and not isinstance(a, bool) and not isinstance(b, bool):
            try:
                if isinstance(op, ast.Sub):
                    return a - b
                if isinstance(op, ast.Mult):
                    return a * b if abs(a) < 1e12 and abs(b) < 1e12 else UNK
                if isinstance(op, ast.Div):
                    return a / b
                if isinstance(op, ast.FloorDiv):
                    return a // b
                if isinstance(op, ast.Mod):
                    return a % b
            except (ZeroDivisionError, OverflowError):
                return UNK
        rep = text_of(a)
        if isinstance(op, ast.Mult) and sa is not None and isinstance(b, int) and 0 <= b <= 100 and rep is not None:
            return mk_str([rep * b], sa[1])
        return UNK

    def percent_format(self, tmpl: Any, args: Any) -> Any:
        """``"..." % args`` for a known template and simple arguments; unknown arguments become holes."""
        t = text_of(tmpl)
        sp = str_parts(tmpl)
        if t is None or sp is None:
            return UNK
        vals = list(args) if isinstance(args, tuple) else [args]
        out: list[str | None] = []
        taint = sp[1]
        i = 0
        k = 0
        n = len(t)
        buf = ""
        while i < n:
            ch = t[i]
            if ch != "%":
                buf += ch
                i += 1
                continue
            m = re.match(r"%(\([^)]*\))?[#0\- +]*(\d+|\*)?(\.\d+)?[hlL]?([sdifrxXeEgGc%])", t[i:])
            if not m:
                return UNK
            conv = m.group(4)
            if conv == "%":
                buf += "%"
            else:
                if m.group(1):
                    v = args.get(m.group(1)[1:-1], UNK) if isinstance(args, dict) else UNK
                else:
                    v = vals[k] if k < len(vals) else UNK
                    k += 1
                if buf:
                    out.append(buf)
                    buf = ""
                pieces = self.format_value(v, 114 if conv == "r" else -1, None)
                out.extend(pieces[0])
                taint += pieces[1]
            i += m.end()
        if buf:
            out.append(buf)
        return mk_str(out, taint)

    # ============================================================================================================================ calls
    def call_value(self, f: Any, args: list[Any], kwargs: dict[str, Any], node: ast.Call, arg_nodes: list[ast.AST], env: PyEnv) -> Any:
        """Call a value (a user function, a module attribute, a UDF ...)."""
        if isinstance(f, FuncV):
            return self.call_func(f, args, kwargs, node)
        if isinstance(f, ModuleRef):
            return self.call_qualified(f.path, args, kwargs, node, arg_nodes, env)
        if isinstance(f, UdfV):
            cols = [self.udf_argument(a) for a in [*args, *kwargs.values()]]
            return combine(f"{f.name}({', '.join(render_value(a) for a in cols)})", cols)
        return UNK

    @staticmethod
    def udf_argument(a: Any) -> Any:
        """A UDF call treats a string argument as a column name (``f("v")`` is ``f(col("v"))``)."""
        if isinstance(a, (str, PStr)):
            t = text_of(a)
            if t is None or t == "*":
                return ColV(frozenset(), (), HOLE, None, False)
            return ColV(frozenset(), (t.strip("`"),), t.strip("`"), t.strip("`").split(".")[-1], True)
        return a

    def call_func(self, f: FuncV, args: list[Any], kwargs: dict[str, Any], node: ast.AST | None, *, speculative: bool = False) -> Any:
        """Inline a call of a user function: bind the arguments, run the body in a fresh scope, return the merged return value."""
        self.called.add(f.uid)
        if self.depth >= MAX_CALL_DEPTH or f.uid in self.func_stack:
            return UNK
        fnode = f.node
        a = fnode.args
        local = PyEnv({}, f.env)
        pos = [*a.posonlyargs, *a.args]
        for i, p in enumerate(pos):
            if i < len(args):
                local.vars[p.arg] = args[i]
            elif p.arg in kwargs:
                local.vars[p.arg] = kwargs[p.arg]
            else:
                d_idx = i - (len(pos) - len(f.defaults))
                local.vars[p.arg] = f.defaults[d_idx] if 0 <= d_idx < len(f.defaults) else UNK
        if a.vararg is not None:
            local.vars[a.vararg.arg] = list(args[len(pos):])
        kw_names = {p.arg for p in a.kwonlyargs}
        for p in a.kwonlyargs:
            local.vars[p.arg] = kwargs.get(p.arg, dict(f.kw_defaults).get(p.arg, UNK))
        if a.kwarg is not None:
            local.vars[a.kwarg.arg] = {k: v for k, v in kwargs.items() if k not in kw_names and k not in {p.arg for p in pos}}
        saved_src = self.src
        if isinstance(f.origin, _Src):
            self.src = f.origin
        self.depth += 1
        self.func_stack.append(f.uid)
        try:
            if isinstance(fnode, ast.Lambda):
                return self.ev(fnode.body, local)
            frame = _Frame()
            self.frames.append(frame)
            try:
                self.exec_block(fnode.body, local)
            except _Return:
                pass
            finally:
                self.frames.pop()
            if not frame.returns:
                return None
            out = frame.returns[0]
            for r in frame.returns[1:]:
                out = merge_values(out, r)
            return out
        finally:
            self.func_stack.pop()
            self.depth -= 1
            self.src = saved_src

    def call_method(self, recv: Any, name: str, args: list[Any], kwargs: dict[str, Any], node: ast.Call, arg_nodes: list[ast.AST], env: PyEnv) -> Any:
        """Call ``recv.name(...)``, dispatching on the kind of receiver."""
        if isinstance(recv, ModuleRef):
            return self.call_qualified(f"{recv.path}.{name}", args, kwargs, node, arg_nodes, env)
        if isinstance(recv, SparkV):
            return self.m_spark(name, args, kwargs, node, arg_nodes)
        if isinstance(recv, Reader):
            return self.m_reader(recv, name, args, kwargs, node, arg_nodes)
        if isinstance(recv, DF):
            return self.m_df(recv, name, args, kwargs, node, arg_nodes, env)
        if isinstance(recv, Writer):
            return self.m_writer(recv, name, args, kwargs, node, arg_nodes)
        if isinstance(recv, Grouped):
            return self.m_grouped(recv, name, args, kwargs, node)
        if isinstance(recv, ColV):
            return self.m_col(recv, name, args, kwargs)
        if isinstance(recv, WinV):
            return self.m_window(recv, name, args)
        if isinstance(recv, DeltaTbl):
            return self.m_delta(recv, name, args, kwargs, node, arg_nodes)
        if isinstance(recv, MergeB):
            return self.m_merge(recv, name, args, kwargs, node)
        if isinstance(recv, ConnV):
            return self.m_conn(recv, name, args, kwargs, node, arg_nodes, env)
        if isinstance(recv, NaV):
            return recv.df
        if isinstance(recv, RddV):
            if name == "toDF":
                names = [text_of(a) for a in args if text_of(a) is not None] or ([text_of(x) for x in args[0]] if args and isinstance(args[0], (list, tuple)) else [])
                cols = tuple(ColInfo(n, partial=True) for n in names if n)
                return DF(Relation(cols, (), recv.datasets, (), None, True))
            return recv
        if isinstance(recv, (str, PStr)):
            return self.m_str(recv, name, args, kwargs)
        if isinstance(recv, list):
            return self.m_list(recv, name, args, kwargs)
        if isinstance(recv, dict):
            return self.m_dict(recv, name, args, kwargs)
        if isinstance(recv, (set, frozenset)):
            return self.m_set(recv, name, args)
        if isinstance(recv, UdfV):
            return UNK
        return UNK

    # ---- builtins and library functions (qualified names)
    def call_qualified(self, path: str, args: list[Any], kwargs: dict[str, Any], node: ast.Call, arg_nodes: list[ast.AST], env: PyEnv) -> Any:
        """Call a function identified by its dotted import path (``pyspark.sql.functions.col``, ``deltalake.write_deltalake``, ``builtins.len`` ...)."""
        head, _, last = path.rpartition(".")
        if path.startswith("builtins."):
            return self.builtin(last, args, kwargs, node)
        if head.endswith("pyspark.sql.functions") or head == "pyspark.sql.functions":
            return self.f_call(last, args, kwargs, node, arg_nodes)
        if head.endswith(".functions") and head.split(".")[0] in ("pyspark", "databricks"):
            return self.f_call(last, args, kwargs, node, arg_nodes)
        if path.startswith("pyspark.sql.SparkSession") or path.startswith("pyspark.sql.session.SparkSession"):
            return SPARK if last == "getOrCreate" else ModuleRef(path)
        if re.search(r"(^|\.)Window\.(partitionBy|orderBy|rowsBetween|rangeBetween)$", path):
            return self.m_window(WinV(), last, args)
        if re.search(r"(^|\.)types\.(StructType|StructField)$", path) or path.endswith(".StructType"):
            return self.struct_type(last, args, kwargs)
        if re.search(r"(^|\.)types\.\w+Type$", path):
            return UNK
        if path.endswith("DeltaTable.forName") or path.endswith("DeltaTable.forPath") or path.endswith("DeltaTable.createIfNotExists") or path.endswith("DeltaTable.createOrReplace"):
            return self.delta_for(last, args, kwargs, node, arg_nodes)
        if path in ("deltalake.DeltaTable", "deltalake.table.DeltaTable"):
            return self.deltalake_open(args, kwargs, node, arg_nodes)
        if path.endswith("write_deltalake"):
            return self.write_deltalake(args, kwargs, node, arg_nodes)
        if path.startswith("notebookutils.") or path.startswith("mssparkutils.") or path.startswith("dbutils."):
            return self.notebookutils(path.split(".", 1)[1], args, kwargs, node, arg_nodes)
        if path.startswith(("sempy.fabric.", "fabric.", "sempy_labs.")):
            return self.sempy(last, args, kwargs, node, arg_nodes)
        if path.startswith(("pandas.", "pd.")):
            return self.pandas_fn(last, args, kwargs, node, arg_nodes)
        if path.startswith("duckdb."):
            return self.duckdb_fn(last, args, kwargs, node, arg_nodes, env)
        if path in ("os.path.join", "posixpath.join"):
            return self.path_join(args)
        if path in ("pathlib.Path", "pathlib.PurePath", "pathlib.PurePosixPath", "pathlib.PosixPath", "os.fspath", "os.path.abspath", "os.path.normpath", "os.path.expanduser"):
            return self.path_join(args[:1]) if args else UNK
        if path in ("os.getenv", "os.environ.get"):
            return UNK
        if path in ("builtins.exec", "builtins.eval", "importlib.import_module", "builtins.__import__", "runpy.run_path", "builtins.compile"):
            self.gap("unsupported_construct", last, f"{last}() runs code that is only known at run time; any data access inside it is not analysed",
                     "Replace dynamic code execution by plain calls so that the dependencies can be seen.", node)
            return UNK
        if path == "functools.partial" or path.endswith("lru_cache"):
            return UNK
        if path == "spark.udf.register" or path.endswith("udf.register"):
            nm = text_of(args[0]) if args else None
            return UdfV(nm or "udf")
        if path.startswith("spark.") or path.startswith("pyspark."):
            return UNK
        return UNK

    def builtin(self, name: str, args: list[Any], kwargs: dict[str, Any], node: ast.AST) -> Any:
        """The pure Python built-ins the interpreter models (on fully known arguments)."""
        a0 = args[0] if args else UNK
        try:
            if name == "len":
                if isinstance(a0, (list, tuple, dict, set, frozenset, str)):
                    return len(a0)
                return UNK
            if name == "str":
                if not args:
                    return ""
                sp = str_parts(a0)
                if sp is not None:
                    return a0
                if isinstance(a0, (int, float, bool)) or a0 is None:
                    return str(a0)
                return UNK
            if name == "int":
                t = text_of(a0) if isinstance(a0, (str, PStr)) else a0
                return int(t) if isinstance(t, (int, float, str)) and not isinstance(t, bool) else UNK
            if name == "float":
                t = text_of(a0) if isinstance(a0, (str, PStr)) else a0
                return float(t) if isinstance(t, (int, float, str)) and not isinstance(t, bool) else UNK
            if name == "bool":
                tv = self._truth(a0)
                return tv if tv is not None else UNK
            if name in ("list", "tuple", "set", "frozenset", "sorted", "reversed"):
                items = self.iterate(a0) if args else []
                if items is None:
                    return UNK
                if name == "sorted":
                    keys: list[Any] = [text_of(i) if isinstance(i, (str, PStr)) and text_of(i) is not None else i for i in items]
                    try:
                        order = sorted(range(len(items)), key=lambda i: keys[i], reverse=bool(kwargs.get("reverse")))
                    except TypeError:
                        return UNK
                    return [items[i] for i in order]
                if name == "reversed":
                    return list(reversed(items))
                if name == "set":
                    return set(items)
                if name == "frozenset":
                    return frozenset(items)
                if name == "tuple":
                    return tuple(items)
                return list(items)
            if name == "dict":
                if not args:
                    return dict(kwargs)
                if isinstance(a0, dict):
                    return {**a0, **kwargs}
                items = self.iterate(a0)
                if items is not None and all(isinstance(i, (list, tuple)) and len(i) == 2 for i in items):
                    return {(text_of(k) if isinstance(k, (str, PStr)) and text_of(k) is not None else k): v for k, v in items}
                return UNK
            if name == "range":
                ints = [a for a in args if isinstance(a, int) and not isinstance(a, bool)]
                if len(ints) == len(args) and 1 <= len(ints) <= 3:
                    r = range(*ints)
                    return _Range(tuple(r)) if len(r) <= 100_000 else UNK
                return UNK
            if name == "enumerate":
                items = self.iterate(a0)
                start = kwargs.get("start", args[1] if len(args) > 1 else 0)
                return [(i + start, x) for i, x in enumerate(items)] if items is not None and isinstance(start, int) else UNK
            if name == "zip":
                lists = [self.iterate(a) for a in args]
                return [tuple(t) for t in zip(*lists, strict=False)] if lists and all(x is not None for x in lists) else UNK
            if name in ("min", "max", "sum", "abs", "round"):
                items = self.iterate(a0) if len(args) == 1 and not isinstance(a0, (int, float)) else list(args)
                if items is None or any(not isinstance(i, (int, float)) or isinstance(i, bool) for i in items):
                    return UNK
                if name == "abs":
                    return abs(items[0])
                if name == "round":
                    return round(items[0], *( [args[1]] if len(args) > 1 and isinstance(args[1], int) else []))
                agg_fn: Any = {"min": min, "max": max, "sum": sum}[name]
                return agg_fn(items) if items else UNK
            if name in ("any", "all"):
                items = self.iterate(a0)
                truths = [self._truth(i) for i in items] if items is not None else None
                if truths is None or any(t is None for t in truths):
                    return UNK
                return any(truths) if name == "any" else all(truths)
            if name == "repr":
                t = text_of(a0)
                return repr(t) if t is not None else UNK
            if name == "isinstance":
                return UNK
            if name == "format":
                pieces = self.format_value(a0, -1, text_of(args[1]) if len(args) > 1 else None)
                return mk_str(pieces[0], pieces[1])
            if name in ("map", "filter"):
                f, items = (args[0], self.iterate(args[1])) if len(args) > 1 else (UNK, None)
                if isinstance(f, FuncV) and items is not None:
                    out = [self.call_func(f, [i], {}, node) for i in items[:MAX_COMP]]
                    if name == "map":
                        return out
                    return [i for i, r in zip(items, out, strict=False) if self._truth(r) is not False]
                return UNK
            if name == "next":
                items = self.iterate(a0)
                return items[0] if items else UNK
            if name == "getattr":
                return UNK
            if name == "open":
                mode = text_of(args[1] if len(args) > 1 else kwargs.get("mode", "r")) or "r"
                arg_nodes = getattr(node, "args", [])
                ds, taint, conf = self.path_from_value(a0, node, None, arg_nodes[0] if arg_nodes else None)
                if ds is not None:
                    if any(ch in mode for ch in "wax+"):
                        self.at(node, taint)
                        saved = self.session.confidence_cap
                        self.session.confidence_cap = "inferred" if conf == "inferred" else saved
                        try:
                            self.session._write(ds, "append" if "a" in mode else "overwrite")
                        finally:
                            self.session.confidence_cap = saved
                    if "r" in mode or "+" in mode:
                        self.read_dataset(ds, node, taint=taint, conf=conf)
                return UNK
        except (ValueError, TypeError, OverflowError):
            return UNK
        return UNK

    def path_join(self, args: list[Any]) -> Any:
        """``os.path.join`` / ``Path(a, b)``: join with ``/`` (unknown parts stay holes)."""
        if not args:
            return UNK
        out = args[0]
        if str_parts(out) is None:
            out = UNK
        for a in args[1:]:
            out = concat_str(out, a, sep="/", path=True)
        if isinstance(out, str):
            return mk_str([out], (), True)
        return out

    # ---- string / container methods
    def m_str(self, recv: Any, name: str, args: list[Any], kwargs: dict[str, Any]) -> Any:
        """Methods of known (or partially known) strings."""
        sp = str_parts(recv)
        assert sp is not None
        frags, taint, is_path = sp
        txt = text_of(recv)
        if name == "format":
            if txt is None:
                return UNK
            return self.str_format(txt, taint, args, kwargs)
        if name == "join":
            items = self.iterate(args[0]) if args else None
            if items is None:
                return UNK
            out: Any = ""
            first = True
            for it in items:
                out = concat_str(out, it, sep=txt) if not first and txt is not None else (concat_str(out, it) if txt is None or first else out)
                first = False
            if txt is None:
                return UNK
            return mk_str([""], taint) if not items else out
        if name in ("lower", "upper", "casefold"):
            f = (lambda s: s.lower()) if name != "upper" else (lambda s: s.upper())
            return mk_str([f(x) if x is not None else None for x in frags], taint, is_path)
        if txt is None:
            if name in ("startswith", "endswith", "split", "strip", "lstrip", "rstrip", "replace"):
                return UNK
            return UNK
        try:
            if name in ("strip", "lstrip", "rstrip"):
                chars = text_of(args[0]) if args else None
                return mk_str([getattr(txt, name)(chars)], taint, is_path)
            if name == "replace":
                a, b = text_of(args[0]) if args else None, text_of(args[1]) if len(args) > 1 else None
                if a is None or b is None:
                    return UNK
                return mk_str([txt.replace(a, b)], taint + (tuple(x for x in [text_of(args[1])] if x) if isinstance(args[1], PStr) else ()), is_path)
            if name in ("startswith", "endswith"):
                p = text_of(args[0]) if args else None
                return getattr(txt, name)(p) if p is not None else UNK
            if name in ("split", "rsplit"):
                sep = text_of(args[0]) if args and args[0] is not None else None
                maxsplit = args[1] if len(args) > 1 and isinstance(args[1], int) else kwargs.get("maxsplit", -1)
                parts = getattr(txt, name)(sep, maxsplit) if isinstance(maxsplit, int) else getattr(txt, name)(sep)
                return [mk_str([p], taint) for p in parts]
            if name in ("removeprefix", "removesuffix"):
                p = text_of(args[0]) if args else None
                return mk_str([getattr(txt, name)(p)], taint) if p is not None else UNK
            if name in ("title", "capitalize", "zfill", "strip"):
                return mk_str([getattr(txt, name)(*[a for a in args if isinstance(a, int)])], taint)
            if name in ("find", "count", "index", "rfind"):
                p = text_of(args[0]) if args else None
                return getattr(txt, name)(p) if p is not None else UNK
            if name == "partition":
                p = text_of(args[0]) if args else None
                return tuple(mk_str([x], taint) for x in txt.partition(p)) if p else UNK
            if name == "isdigit":
                return txt.isdigit()
            if name == "splitlines":
                return [mk_str([x], taint) for x in txt.splitlines()]
            if name == "encode":
                return UNK
        except (ValueError, TypeError, IndexError):
            return UNK
        return UNK

    def str_format(self, template: str, taint: tuple[str, ...], args: list[Any], kwargs: dict[str, Any]) -> Any:
        """``template.format(*args, **kwargs)``: replacement fields with names / indexes (and ``[key]`` lookups) are filled, anything else becomes a hole."""
        out: list[str | None] = []
        auto = 0
        t = taint
        try:
            parsed = list(string.Formatter().parse(template))
        except ValueError:
            return UNK
        for lit, field_name, spec, conv in parsed:
            if lit:
                out.append(lit)
            if field_name is None:
                continue
            m = re.match(r"^([^.\[]*)((?:\[[^\]]*\])*)(.*)$", field_name)
            base_name, subs, rest = (m.group(1), m.group(2), m.group(3)) if m else (field_name, "", "")
            if base_name == "":
                key: Any = auto
                auto += 1
            elif base_name.isdigit():
                key = int(base_name)
            else:
                key = base_name
            if rest:
                out.append(None)
                continue
            v = args[key] if isinstance(key, int) and key < len(args) else (kwargs.get(key, UNK) if isinstance(key, str) else UNK)
            for sub in re.findall(r"\[([^\]]*)\]", subs):
                idx: Any = int(sub) if sub.lstrip("-").isdigit() else sub
                if isinstance(v, (list, tuple)) and isinstance(idx, int) and -len(v) <= idx < len(v):
                    v = v[idx]
                elif isinstance(v, dict) and idx in v:
                    v = v[idx]
                else:
                    v = UNK
            spec_txt = spec or ""
            pieces = self.format_value(v, ord(conv) if conv else -1, spec_txt if spec_txt else None)
            out.extend(pieces[0])
            t += pieces[1]
        return mk_str(out, t)

    def m_list(self, recv: list[Any], name: str, args: list[Any], kwargs: dict[str, Any]) -> Any:
        """Mutating and reading methods of known lists (they mutate the abstract value, never user data)."""
        if name == "append" and args:
            recv.append(args[0])
            return None
        if name == "extend" and args:
            items = self.iterate(args[0])
            recv.extend(items if items is not None else [UNK])
            return None
        if name == "insert" and len(args) == 2 and isinstance(args[0], int):
            recv.insert(args[0], args[1])
            return None
        if name == "pop":
            idx = args[0] if args and isinstance(args[0], int) else -1
            return recv.pop(idx) if recv and -len(recv) <= idx < len(recv) else UNK
        if name == "copy":
            return list(recv)
        if name in ("sort", "reverse"):
            return None
        if name == "index" or name == "count":
            return UNK
        return UNK

    def m_dict(self, recv: dict[Any, Any], name: str, args: list[Any], kwargs: dict[str, Any]) -> Any:
        """Methods of known dictionaries."""
        key = text_of(args[0]) if args and isinstance(args[0], (str, PStr)) and text_of(args[0]) is not None else (args[0] if args else UNK)
        if name == "get":
            try:
                return recv.get(key, args[1] if len(args) > 1 else None) if key is not UNK else UNK
            except TypeError:
                return UNK
        if name == "items":
            return list(recv.items())
        if name == "keys":
            return list(recv.keys())
        if name == "values":
            return list(recv.values())
        if name == "update":
            if args and isinstance(args[0], dict):
                recv.update(args[0])
            recv.update(kwargs)
            return None
        if name == "setdefault" and args:
            try:
                return recv.setdefault(key, args[1] if len(args) > 1 else None)
            except TypeError:
                return UNK
        if name == "pop":
            try:
                return recv.pop(key, args[1] if len(args) > 1 else UNK)
            except TypeError:
                return UNK
        if name == "copy":
            return dict(recv)
        return UNK

    def m_set(self, recv: Any, name: str, args: list[Any]) -> Any:
        """Methods of known sets."""
        if isinstance(recv, set):
            if name == "add" and args:
                try:
                    recv.add(text_of(args[0]) if isinstance(args[0], PStr) and text_of(args[0]) is not None else args[0])
                except TypeError:
                    pass
                return None
            if name == "update" and args:
                items = self.iterate(args[0])
                if items is not None:
                    for i in items:
                        try:
                            recv.add(i)
                        except TypeError:
                            pass
                return None
        return UNK

    # ============================================================================================================================ dataset resolution
    def _hole_text(self, v: Any) -> str | None:
        """SQL / name text of a string value with unknown parts replaced by :data:`UNKNOWN_MARK` markers; ``None`` for a non-string."""
        sp = str_parts(v)
        if sp is None:
            return None
        out = []
        for f in sp[0]:
            if f is None:
                out.append(f"{UNKNOWN_MARK}{self._hole}__")
                self._hole += 1
            else:
                out.append(f)
        return "".join(out)

    def table_from_value(self, v: Any, node: ast.AST, arg_node: ast.AST | None = None) -> tuple[Dataset | None, tuple[str, ...]]:
        """Resolve a table-name argument to a dataset. Returns ``(dataset, taint)``; unresolvable names record a ``dynamic_reference`` gap and return ``None``."""
        sp = str_parts(v)
        if sp is None or not sp[0] or (not sp[0][0] and len(sp[0]) == 1):
            self.gap("dynamic_reference", self.snippet(arg_node or node), "the table name is not a compile-time constant",
                     "Use a literal table name, or a notebook parameter with a default value, so that the dependency can be seen.", node)
            return None, ()
        text = self._hole_text(v) or ""
        if UNKNOWN_MARK in text:
            pattern = re.sub(re.escape(UNKNOWN_MARK) + r"\d+__", HOLE, text)
            self.gap("dynamic_reference", pattern, f"the table name is built at run time ({pattern})",
                     "Use a literal table name, or a notebook parameter with a default value, so that the dependency can be seen.", node)
            return None, ()
        parts = _split_dotted(text)
        if not parts:
            return None, ()
        self.at(node, sp[1])
        return self.session.table_dataset(parts), sp[1]

    def path_from_value(self, v: Any, node: ast.AST, fmt: str | None, arg_node: ast.AST | None = None, *, delta: bool = False) -> tuple[Dataset | None, tuple[str, ...], str]:
        """Resolve a path argument to a dataset: ``(dataset, taint, confidence)``.

        A path with an unknown root but a constant last segment is accepted for Delta locations only, as an ``inferred`` table (and a gap says why).
        """
        sp = str_parts(v)
        if sp is None or not sp[0] or all(f is None for f in sp[0]):
            self.gap("dynamic_reference", self.snippet(arg_node or node), "the path is not a compile-time constant",
                     "Use a literal path, or a notebook parameter with a default value, so that the dependency can be seen.", node)
            return None, (), "derived"
        frags, taint, _ = sp
        self.at(node, taint)
        if all(f is not None for f in frags):
            return self.session.file_dataset("".join(f for f in frags if f is not None), fmt), taint, "derived"
        text = "".join(HOLE if f is None else f for f in frags)
        # only the host / workspace part unknown (notebookutils.lakehouse.get paths): classify with the hole dropped
        m = re.match(r"^(abfss://)<\?>(@onelake\.dfs\.fabric\.microsoft\.com/.*)$", text)
        if m and HOLE not in m.group(2):
            return self.session.file_dataset(text, fmt), taint, "derived"
        tail = frags[-1] if frags[-1] is not None else ""
        last_seg = tail.rsplit("/", 1)[-1] if "/" in tail else ""
        if delta and frags[-1] is not None and "/" in tail and last_seg and HOLE not in last_seg:
            name = last_seg
            ds = self.session.table_dataset([name])
            self.gap("dynamic_reference", text, f"the root of the path is computed at run time ({text}); the table '{name}' was inferred from the last path segment",
                     "Use a literal path so that the table can be identified with certainty.", node)
            return ds, taint, "inferred"
        self.gap("dynamic_reference", text, f"the path is built at run time ({text})",
                 "Use a literal path, or a notebook parameter with a default value, so that the dependency can be seen.", node)
        return None, (), "derived"

    def read_dataset(self, ds: Dataset, node: ast.AST, *, taint: tuple[str, ...] = (), conf: str = "derived") -> None:
        """Record that the current code reads ``ds``."""
        self.at(node, taint)
        s = self.session
        if ds.kind == "dynamic":
            return
        s.sticky_taint.update(t for t in taint if t)
        saved = s.confidence_cap
        if conf == "inferred":
            s.confidence_cap = "inferred"
        try:
            s.note_read(ds)
            s._flush_reads()
        finally:
            s.confidence_cap = saved

    def relation_for_read(self, ds: Dataset, schema: tuple[str, ...] | None = None) -> Relation:
        """The relation of a freshly read dataset; an explicit ``.schema(..)`` provides the column names when none are known."""
        if schema and not ds.schema and ds.kind in ("table", "file"):
            ds = Dataset(ds.key, ds.kind, ds.name, ds.ref, schema)
            self.session.datasets[ds.key] = ds
        return self.session.base_relation(ds)

    # ============================================================================================================================ spark
    def m_spark(self, name: str, args: list[Any], kwargs: dict[str, Any], node: ast.Call, arg_nodes: list[ast.AST]) -> Any:
        """Methods of the Spark session."""
        if name == "table":
            return self.read_table_value(args[0] if args else kwargs.get("tableName", UNK), node, arg_nodes[0] if arg_nodes else None)
        if name == "sql":
            q = args[0] if args else kwargs.get("sqlQuery", kwargs.get("query", UNK))
            return self.run_sql_value(q, node, self.session, arg_nodes[0] if arg_nodes else None)
        if name == "range":
            return DF(Relation((ColInfo("id"),)))
        if name == "createDataFrame":
            return self.create_dataframe(args, kwargs)
        if name in ("newSession", "getActiveSession"):
            return SPARK
        return UNK

    def read_table_value(self, v: Any, node: ast.AST, arg_node: ast.AST | None, schema: tuple[str, ...] | None = None) -> Any:
        """``spark.table(name)``: a temp view resolves to its relation, otherwise the table is read."""
        txt = text_of(v)
        if txt is not None:
            key = txt.strip("`").lower()
            if key.startswith("global_temp."):
                key = key[len("global_temp."):]
            if "." not in key and key in self.session.views:
                return DF(self.session.views[key])
        ds, taint = self.table_from_value(v, node, arg_node)
        if ds is None:
            return DF(opaque_relation())
        self.read_dataset(ds, node, taint=taint)
        return DF(self.relation_for_read(ds, schema))

    def create_dataframe(self, args: list[Any], kwargs: dict[str, Any]) -> Any:
        data = args[0] if args else kwargs.get("data", UNK)
        schema = args[1] if len(args) > 1 else kwargs.get("schema", None)
        if isinstance(data, DF):
            names = self.schema_names(schema)
            if names and data.rel.positional_ok and len(names) == len(data.rel.cols):
                return DF(Relation(tuple(ColInfo(n, c.origins, c.partial, c.expr) for n, c in zip(names, data.rel.cols, strict=True)), (), data.rel.datasets))
            return DF(data.rel)
        names = self.schema_names(schema)
        if names:
            return DF(Relation(tuple(ColInfo(n) for n in names)))
        return DF(opaque_relation())

    def schema_names(self, schema: Any) -> list[str] | None:
        """Column names of a schema argument: a list of names, a ``StructType`` value or a DDL string (``"a INT, b STRING"``)."""
        if isinstance(schema, SchemaV):
            return list(schema.names)
        if isinstance(schema, (list, tuple)) and schema and all(isinstance(x, (str, PStr)) and text_of(x) is not None for x in schema):
            return [str(text_of(x)) for x in schema]
        t = text_of(schema) if isinstance(schema, (str, PStr)) else None
        if t:
            names = [p.strip().strip("`").split()[0] for p in re.split(r",(?![^<(]*[>)])", t) if p.strip()]
            return [n for n in names if re.fullmatch(r"[\w$]+", n)] or None
        return None

    def struct_type(self, last: str, args: list[Any], kwargs: dict[str, Any]) -> Any:
        if last == "StructField":
            nm = text_of(args[0]) if args else None
            return SchemaV((nm,)) if nm else UNK
        fields = args[0] if args else UNK
        if isinstance(fields, (list, tuple)) and all(isinstance(f, SchemaV) for f in fields):
            return SchemaV(tuple(n for f in fields for n in f.names))
        return SchemaV(())

    def m_reader(self, rd: Reader, name: str, args: list[Any], kwargs: dict[str, Any], node: ast.Call, arg_nodes: list[ast.AST]) -> Any:
        """``spark.read`` builder methods and terminal reads."""
        if name == "format":
            return Reader(text_of(args[0]) if args else None, rd.options, rd.schema, rd.stream)
        if name == "option" and len(args) >= 2:
            return Reader(rd.fmt, (*rd.options, (str(text_of(args[0])), args[1])), rd.schema, rd.stream)
        if name == "options":
            return Reader(rd.fmt, (*rd.options, *((k, v) for k, v in kwargs.items())), rd.schema, rd.stream)
        if name == "schema":
            names = self.schema_names(args[0]) if args else None
            return Reader(rd.fmt, rd.options, tuple(names) if names else rd.schema, rd.stream)
        opts = dict(rd.options)
        if name == "table":
            return self.read_table_value(args[0] if args else UNK, node, arg_nodes[0] if arg_nodes else None, rd.schema)
        if name == "load":
            fmt = text_of(kwargs.get("format")) or rd.fmt
            path = args[0] if args else kwargs.get("path", opts.get("path", UNK))
            schema = rd.schema
            return self.read_files(path, fmt, node, arg_nodes[0] if arg_nodes else None, opts, schema, kwargs)
        if name in ("csv", "parquet", "json", "orc", "text", "avro", "xml", "delta"):
            path = args[0] if args else kwargs.get("path", UNK)
            return self.read_files(path, name, node, arg_nodes[0] if arg_nodes else None, opts, rd.schema, kwargs)
        if name == "jdbc":
            return self.read_jdbc(opts, args, kwargs, node)
        return UNK

    def read_files(self, path: Any, fmt: str | None, node: ast.Call, arg_node: ast.AST | None, opts: dict[str, Any], schema: tuple[str, ...] | None,
                   kwargs: dict[str, Any]) -> Any:
        """``read.format(fmt).load(path)`` and the per-format shortcuts (a list of paths reads each)."""
        f = (fmt or "parquet").lower()
        if f == "jdbc":
            return self.read_jdbc(opts, [], kwargs, node)
        if f in ("kafka", "eventhubs", "cosmos.oltp", "kusto", "mongodb", "socket", "rate"):
            self.gap("unsupported_construct", f, f"the '{f}' source is not modelled: its lineage is not recorded", "Document this dependency manually in the catalog.", node)
            return DF(opaque_relation())
        if f == "delta" and path is UNK and "path" not in opts:
            tname = kwargs.get("tableName")
            if tname is not None:
                return self.read_table_value(tname, node, arg_node, schema)
        paths = path if isinstance(path, (list, tuple)) else [path]
        rels: list[Relation] = []
        for p in paths:
            ds, taint, conf = self.path_from_value(p, node, f, arg_node, delta=(f == "delta"))
            if ds is None:
                rels.append(opaque_relation())
                continue
            self.read_dataset(ds, node, taint=taint, conf=conf)
            if ds.kind == "table" and conf == "derived" and "/" not in ds.name:
                rels.append(self.relation_for_read(ds, schema))
            else:
                rels.append(self.relation_for_read(ds, schema))
        if not rels:
            return DF(opaque_relation())
        if len(rels) == 1:
            return DF(rels[0])
        out = rels[0]
        for r in rels[1:]:
            out = Relation((), (*out.tail, *r.tail), out.datasets | r.datasets, (), None, out.opaque or r.opaque)
        return DF(out)

    def read_jdbc(self, opts: dict[str, Any], args: list[Any], kwargs: dict[str, Any], node: ast.AST) -> Any:
        """JDBC source: an ``external`` dataset named after the database host and table (credentials in the URL are dropped)."""
        url = text_of(args[0]) if args else text_of(opts.get("url", kwargs.get("url")))
        table = text_of(args[1]) if len(args) > 1 else text_of(opts.get("dbtable", kwargs.get("table", opts.get("query"))))
        if not url:
            self.gap("dynamic_reference", "jdbc", "the JDBC URL is not a compile-time constant", None, node)
            return DF(opaque_relation())
        clean = re.sub(r"//[^/@]*@", "//", url.split("?")[0].split(";")[0])
        name = f"{clean}/{table}" if table else clean
        ref = AssetRef.make("external", name=name)
        ds = Dataset("x:" + name, "external", name, ref)
        self.session.datasets[ds.key] = ds
        self.read_dataset(ds, node)
        return DF(self.session.base_relation(ds))

    # ============================================================================================================================ columns and functions
    def df_col(self, df: DF, name: str, node: ast.AST) -> ColV:
        """``df["name"]`` / ``df.name``: a column reference bound to this DataFrame (resolved now so joins stay unambiguous)."""
        if name == "*":
            return ColV(text="*", star=True)
        lk = df.rel.lookup(name)
        if lk.status == "ok":
            return ColV(lk.origins, (), name, name, True)
        return ColV(frozenset(), (name,), name, name, True)

    def df_subscript(self, df: DF, key: Any, node: ast.AST) -> Any:
        """``df["c"]`` (column), ``df[["a", "b"]]`` (select), ``df[condition]`` (filter)."""
        t = text_of(key) if isinstance(key, (str, PStr)) else None
        if t is not None:
            return self.df_col(df, t, node)
        if isinstance(key, (list, tuple)):
            return self.select(df, list(key), node, strict=False)
        if isinstance(key, ColV):
            return df
        return UNK

    def resolve_col(self, col: Any, rel: Relation, node: ast.AST | None, *, quiet: bool = False) -> ColInfo:
        """Resolve a column argument (``ColV`` or a column name) against ``rel`` into an output :class:`ColInfo`."""
        if isinstance(col, (str, PStr)):
            t = text_of(col)
            if t is None:
                if not quiet:
                    self.gap("dynamic_reference", "column", "a column name is built at run time; the column is not attributed to a source column",
                             "Use literal column names so that the source columns can be seen.", node)
                return ColInfo(HOLE, frozenset(), True)
            col = ColV(frozenset(), (t,), t, t, True)
        if not isinstance(col, ColV):
            if col is UNK:
                if not quiet:
                    self.gap("unsupported_construct", "column expression", "a column is computed by an expression the analyzer cannot follow (unknown function, variable or object); "
                             "it is created without source columns", "Express the column with Spark functions / SQL so that its inputs can be seen.", node)
                return ColInfo(HOLE, frozenset(), True, "?")
            return ColInfo(render_value(col), frozenset(), False, render_value(col))
        origins = set(col.bound)
        partial = False
        for f in col.free:
            lk = self._lookup_name(rel, f)
            if lk.status == "ok":
                origins |= lk.origins
            else:
                partial = True
                if lk.status in ("ambiguous", "missing") and not quiet:
                    why = "is ambiguous" if lk.status == "ambiguous" else "does not match any known column"
                    self.gap("unresolved_reference", f, f"column '{f}' {why} in this DataFrame; it is not attributed to a source column",
                             "Reference the column through its DataFrame (df['c']) or provide table schemas.", node)
        step = None if col.ref else col.text
        out_origins = add_step(origins, step)
        name = col.name or (col.free[0].split(".")[-1] if col.free else col.text)
        return ColInfo(name, out_origins, partial, col.text)

    def _lookup_name(self, rel: Relation, name: str) -> Any:
        if "." in name and not name.startswith("`"):
            q, rest = name.split(".", 1)
            got = rel.lookup_qualified(q, rest)
            if got is not None:
                return got
            first = rel.lookup(q)           # struct field access
            if first.status == "ok":
                return first
            return rel.lookup(name)
        return rel.lookup(name.strip("`"))

    def _expand_col_args(self, args: list[Any]) -> list[Any]:
        """Flatten ``select(["a", "b"])`` style arguments."""
        out: list[Any] = []
        for a in args:
            if isinstance(a, (list, tuple)):
                out.extend(self._expand_col_args(list(a)))
            else:
                out.append(a)
        return out

    def select(self, df: DF, args: list[Any], node: ast.AST, *, strict: bool = True) -> DF:
        """``df.select(...)``."""
        rel = df.rel
        cols: list[ColInfo] = []
        tail: tuple[str, ...] = ()
        opaque = False
        for a in self._expand_col_args(args):
            if (isinstance(a, ColV) and a.star) or text_of(a) == "*":
                cols.extend(rel.cols)
                tail = rel.tail
                opaque = opaque or rel.opaque
                continue
            t = text_of(a) if isinstance(a, (str, PStr)) else None
            if t is not None and t.endswith(".*"):
                q = t[:-2].lower()
                sub = next((s for k, s in rel.quals if k == q), None)
                if sub is not None:
                    cols.extend(sub.cols)
                    tail = tuple(dict.fromkeys((*tail, *sub.tail)))
                    continue
            cols.append(self.resolve_col(a, rel, node))
        return DF(Relation(tuple(cols), tail, rel.datasets, rel.quals, None, opaque), df.kind)

    def f_call(self, fn: str, args: list[Any], kwargs: dict[str, Any], node: ast.Call, arg_nodes: list[ast.AST]) -> Any:
        """A ``pyspark.sql.functions`` call: a column expression over its column arguments."""
        if fn == "broadcast" and args and isinstance(args[0], DF):
            return args[0]
        if fn in ("udf", "pandas_udf"):
            return UdfV(fn)
        if fn == "expr":
            return self.expr_col(args[0] if args else UNK, node)
        if fn in ("col", "column"):
            t = text_of(args[0]) if args else None
            if t is None:
                return ColV(frozenset(), (), HOLE, None, False)
            if t == "*":
                return ColV(text="*", star=True)
            return ColV(frozenset(), (t.strip("`"),), t.strip("`"), t.strip("`").split(".")[-1], True)
        if fn == "lit":
            return ColV(frozenset(), (), render_value(args[0]) if args else "NULL", None, False, False, True)
        if fn == "when" and args:
            cond = args[0]
            val = args[1] if len(args) > 1 else None
            return combine(f"CASE WHEN {render_value(cond)} THEN {render_value(val)}", [cond, val])
        if fn not in KNOWN_FUNCS:
            return UNK
        spec = FUNC_SPEC.get(fn, "" if fn in NOARG_FUNCS else "c" + "l" * 30)
        parts: list[Any] = []
        texts: list[str] = []
        star = False
        for i, a in enumerate(args):
            kind = spec[min(i, len(spec) - 1)] if spec and not (i >= len(spec) and not spec.endswith("*")) else "l"
            if spec.endswith("*") and i >= len(spec) - 1:
                kind = spec[-2]
            kind = kind if kind != "*" else "c"
            if isinstance(a, ColV):
                parts.append(a)
                texts.append(a.text)
            elif kind == "c" and isinstance(a, (str, PStr)):
                t = text_of(a)
                if t is None:
                    texts.append(HOLE)
                    continue
                if t == "*":
                    star = True
                    texts.append("*")
                    continue
                cv = ColV(frozenset(), (t.strip("`"),), t.strip("`"), t.strip("`").split(".")[-1], True)
                parts.append(cv)
                texts.append(t)
            elif kind == "x":
                parts.append(self.expr_col(a, node))
                texts.append(render_value(a))
            elif kind == "f" and isinstance(a, FuncV):
                texts.append("<lambda>")
                n_params = len(a.node.args.args) if hasattr(a.node, "args") else 0
                body = self.call_func(a, [ColV(text="x", ref=False, literal=False)] * n_params, {}, node)
                if isinstance(body, ColV):
                    parts.append(body)
            else:
                texts.append(render_value(a))
        parts.extend(v for v in kwargs.values() if isinstance(v, ColV))
        text = f"{fn.lower()}({', '.join(texts)})"
        out = combine(text, parts)
        if star and not out.bound and not out.free:
            return ColV(frozenset(), (), text, None, False, False, False)
        return out

    def expr_col(self, v: Any, node: ast.AST) -> Any:
        """``F.expr("...")``: parse the SQL expression; its column names are resolved against the DataFrame it is applied to."""
        t = text_of(v) if isinstance(v, (str, PStr)) else None
        if t is None:
            return ColV(frozenset(), (), HOLE, None, False)
        try:
            import sqlglot
            from sqlglot import exp

            if parse_hazard(t) is not None:
                raise ValueError("too many JOINs without conditions")
            tree = sqlglot.parse_one(t, read="spark")
        except Exception:  # noqa: BLE001
            self.gap("parse_error", t, "cannot parse the SQL expression passed to expr()", "Fix the expression.", node)
            return ColV(frozenset(), (), t, None, False)
        inner = tree.this if isinstance(tree, exp.Alias) else tree
        name = tree.alias if isinstance(tree, exp.Alias) and tree.alias else None
        free: list[str] = []
        for c in inner.find_all(exp.Column):
            parts = [p.name for p in (c.args.get("table"), c.args.get("this")) if p is not None and p.name]
            ref = ".".join(parts)
            if ref and ref not in free and not isinstance(c.this, exp.Star):
                free.append(ref)
        return ColV(frozenset(), tuple(free), t, name, isinstance(inner, exp.Column))

    def m_col(self, c: ColV, name: str, args: list[Any], kwargs: dict[str, Any]) -> Any:
        """Methods of a column expression."""
        if name in ("alias", "name") and args:
            nm = text_of(args[0])
            return c.renamed(nm) if nm else c
        if name in ("cast", "astype") and args:
            return combine(f"CAST({c.text} AS {text_of(args[0]) or render_value(args[0])})", [c], c.name)
        if name == "otherwise":
            return combine(f"{c.text} ELSE {render_value(args[0]) if args else 'NULL'} END", [c, *args], c.name)
        if name == "when" and len(args) >= 2:
            return combine(f"{c.text} WHEN {render_value(args[0])} THEN {render_value(args[1])}", [c, *args], c.name)
        if name == "over":
            win = args[0] if args else None
            if isinstance(win, WinV):
                return combine(f"{c.text} OVER ({win.text()})", [c, *win.cols], c.name)
            return combine(f"{c.text} OVER ()", [c], c.name)
        if name in COLUMN_METHODS or name in ("__getitem__",):
            return combine(f"{c.text}.{name}({', '.join(render_value(a) for a in args)})", [c, *args, *kwargs.values()])
        # pandas Series / any other method: depends on the receiver and every column argument
        return combine(f"{c.text}.{name}({', '.join(render_value(a) for a in args)})", [c, *args, *kwargs.values()])

    def m_window(self, w: WinV, name: str, args: list[Any]) -> Any:
        """``Window.partitionBy(..).orderBy(..)``: remembers the columns."""
        if name in ("partitionBy", "orderBy"):
            cols = self._expand_col_args(args)
            conv = tuple(ColV(frozenset(), (text_of(c) or HOLE,), text_of(c) or HOLE, None, True) if isinstance(c, (str, PStr)) else c for c in cols)
            return WinV(w.part + conv, w.order, w.frame) if name == "partitionBy" else WinV(w.part, w.order + conv, w.frame)
        if name in ("rowsBetween", "rangeBetween"):
            return WinV(w.part, w.order, f"{'ROWS' if name == 'rowsBetween' else 'RANGE'} BETWEEN {', '.join(render_value(a) for a in args)}")
        return w

    # ============================================================================================================================ DataFrame methods
    def df_with_column(self, df: DF, name: str, value: Any, node: ast.AST) -> DF:
        """``withColumn(name, value)`` (also pandas ``df[name] = value``): replace the column of that name or append it."""
        ci0 = self.resolve_col(value, df.rel, node)
        ci = ColInfo(name, ci0.origins, ci0.partial, ci0.expr)
        low = name.lower()
        cols = list(df.rel.cols)
        for i, c in enumerate(cols):
            if c.name.lower() == low:
                cols[i] = ci
                break
        else:
            cols.append(ci)
        r = df.rel
        return DF(Relation(tuple(cols), r.tail, r.datasets, r.quals, r.alias, r.opaque), df.kind)

    def m_df(self, df: DF, name: str, args: list[Any], kwargs: dict[str, Any], node: ast.Call, arg_nodes: list[ast.AST], env: PyEnv) -> Any:
        """DataFrame methods (Spark, with the pandas ones that are modelled)."""
        rel = df.rel
        if name in ("select", "project"):
            return self.select(df, args, node)
        if name == "withColumn" and len(args) >= 2:
            nm = text_of(args[0])
            return self.df_with_column(df, nm, args[1], node) if nm else DF(opaque_relation(rel.datasets), df.kind)
        if name == "withColumns" and args and isinstance(args[0], dict):
            out = df
            for k, v in args[0].items():
                out = self.df_with_column(out, str(k), v, node)
            return out
        if name == "withColumnRenamed" and len(args) >= 2:
            old, new = text_of(args[0]), text_of(args[1])
            if old is None or new is None:
                return DF(opaque_relation(rel.datasets), df.kind)
            return self.rename_columns(df, {old: new}, node)
        if name == "withColumnsRenamed" and args and isinstance(args[0], dict):
            return self.rename_columns(df, {str(k): str(text_of(v)) for k, v in args[0].items() if text_of(v)}, node)
        if name == "rename" and kwargs.get("columns") is not None and isinstance(kwargs["columns"], dict):
            return self.rename_columns(df, {str(k): str(text_of(v)) for k, v in kwargs["columns"].items() if text_of(v)}, node)
        if name == "toDF":
            names = [text_of(a) for a in self._expand_col_args(args)]
            if any(n is None for n in names):
                return DF(opaque_relation(rel.datasets), df.kind)
            if rel.positional_ok and len(rel.cols) == len(names):
                return DF(Relation(tuple(ColInfo(str(n), c.origins, c.partial, c.expr) for n, c in zip(names, rel.cols, strict=True)), (), rel.datasets), df.kind)
            return DF(Relation(tuple(ColInfo(str(n), frozenset(), True) for n in names), (), rel.datasets, (), None, True), df.kind)
        if name == "drop":
            drop_names = {str(text_of(a) if isinstance(a, (str, PStr)) else (a.name if isinstance(a, ColV) else "")).lower() for a in self._expand_col_args(args)}
            drop_names |= {str(text_of(v)).lower() for k, v in kwargs.items() if k == "columns" and text_of(v)}
            if isinstance(kwargs.get("columns"), (list, tuple)):
                drop_names |= {str(text_of(x)).lower() for x in kwargs["columns"] if text_of(x)}
            return DF(Relation(tuple(c for c in rel.cols if c.name.lower() not in drop_names), rel.tail, rel.datasets, rel.quals, rel.alias, rel.opaque), df.kind)
        if name in DF_PASSTHROUGH and df.kind == "spark" or name in PANDAS_PASSTHROUGH and df.kind != "spark":
            return df
        if name in ("alias", "as_"):
            nm = text_of(args[0]) if args else None
            return DF(rel.with_alias(nm), df.kind) if nm else df
        if name in ("join", "merge"):
            return self.join(df, args, kwargs, node)
        if name == "crossJoin" and args and isinstance(args[0], DF):
            return DF(concat(rel, args[0].rel), df.kind)
        if name in ("union", "unionAll", "unionByName", "concat"):
            return self.union(df, args, kwargs, name == "unionByName")
        if name in ("intersect", "intersectAll", "subtract", "exceptAll", "difference"):
            other = args[0] if args and isinstance(args[0], DF) else None
            return DF(Relation(rel.cols, rel.tail, rel.datasets | (other.rel.datasets if other else frozenset()), (), None, rel.opaque), df.kind)
        if name in ("groupBy", "groupby", "rollup", "cube", "groupBy_"):
            keys = [self.resolve_col(a, rel, node) for a in self._expand_col_args(args)]
            return Grouped(df, tuple(keys))
        if name in ("agg", "aggregate") and df.kind == "spark":
            return self.m_grouped(Grouped(df, ()), "agg", args, kwargs, node)
        if name == "selectExpr":
            exprs = [text_of(a) for a in self._expand_col_args(args)]
            if any(e is None for e in exprs):
                self.gap("dynamic_reference", self.snippet(node), "an expression passed to selectExpr is not a compile-time constant", None, node)
                return DF(opaque_relation(rel.datasets), df.kind)
            self.at(node)
            proj = self.session.project_expressions([str(e) for e in exprs], rel, label=self.snippet(node), process=self.process, loc=self.loc(node))
            return DF(proj, df.kind) if proj is not None else DF(opaque_relation(rel.datasets), df.kind)
        if name == "transform" and args and isinstance(args[0], FuncV):
            return self.call_func(args[0], [df, *args[1:]], kwargs, node)
        if name in ("createOrReplaceTempView", "createTempView", "createGlobalTempView", "createOrReplaceGlobalTempView", "registerTempTable"):
            nm = text_of(args[0]) if args else None
            if nm:
                self.session.views[nm.lower()] = rel
            return None
        if name == "toPandas" or name == "pandas_api" or name == "to_pandas_on_spark":
            return DF(rel, "pandas")
        if name in ("to_pyarrow_dataset", "to_pyarrow_table", "to_arrow", "toArrow"):
            return DF(rel, "arrow")
        if name in ("to_spark", "toSpark"):
            return DF(rel, "spark")
        if name in ("mapInPandas", "applyInPandas", "mapInArrow", "unpivot", "melt", "pivot", "stack", "explode"):
            schema_cols = self.schema_names(args[1] if len(args) > 1 else kwargs.get("schema")) if name in ("mapInPandas", "applyInPandas", "mapInArrow") else None
            cols = tuple(ColInfo(n, partial=True) for n in schema_cols) if schema_cols else ()
            return DF(Relation(cols, (), rel.datasets, (), None, True), df.kind)
        if name == "assign" and df.kind != "spark":
            out = df
            for k, v in kwargs.items():
                val = self.call_func(v, [out], {}, node) if isinstance(v, FuncV) else v
                out = self.df_with_column(out, k, val, node)
            return out
        if name.startswith("to_") and df.kind != "spark":
            return self.pandas_write(df, name, args, kwargs, node, arg_nodes)
        if name in DF_ACTIONS:
            return UNK
        if name in ("groupby",) or name in ("drop_duplicates",):
            return df
        if name in ("filter", "where") and df.kind == "spark":
            return df
        if name == "isin" or name == "equals":
            return UNK
        if name == "writeTo":
            return Writer(df, v2_table=args[0] if args else UNK)
        if name == "write":
            return Writer(df)
        if df.kind != "spark" and name in ("merge",):
            return self.join(df, args, kwargs, node)
        if name == "describe":
            return UNK
        return DF(opaque_relation(rel.datasets), df.kind)

    def rename_columns(self, df: DF, mapping: dict[str, str], node: ast.AST) -> DF:
        """``withColumnRenamed`` / pandas ``rename(columns=...)``."""
        rel = df.rel
        low = {k.lower(): v for k, v in mapping.items()}
        cols = [ColInfo(low[c.name.lower()], c.origins, c.partial, c.expr) if c.name.lower() in low else c for c in rel.cols]
        tail = rel.tail
        for k, v in low.items():
            if not any(c.name.lower() == k for c in rel.cols) and (rel.tail or rel.opaque):
                lk = rel.lookup(k)
                if lk.status == "ok":
                    cols.append(ColInfo(v, lk.origins, False, k))
        return DF(Relation(tuple(cols), tail, rel.datasets, rel.quals, rel.alias, rel.opaque), df.kind)

    def join(self, df: DF, args: list[Any], kwargs: dict[str, Any], node: ast.AST) -> Any:
        """``left.join(right, on, how)``: both sides' columns; ``USING`` columns appear once."""
        other = args[0] if args and isinstance(args[0], DF) else kwargs.get("other", kwargs.get("right"))
        if not isinstance(other, DF):
            return DF(opaque_relation(df.rel.datasets), df.kind)
        on = args[1] if len(args) > 1 else kwargs.get("on", kwargs.get("on_cols"))
        how = text_of(args[2] if len(args) > 2 else kwargs.get("how", "inner")) or "inner"
        how = how.lower().replace("_", "")
        left, right = df.rel, other.rel
        names: list[str] = []
        if isinstance(on, (str, PStr)) and text_of(on):
            names = [str(text_of(on))]
        elif isinstance(on, (list, tuple)) and on and all(isinstance(x, (str, PStr)) and text_of(x) for x in on):
            names = [str(text_of(x)) for x in on]
        if df.kind != "spark":
            for key in ("left_on", "right_on"):
                v = kwargs.get(key)
                if isinstance(v, (str, PStr)):
                    names = names
        if how in ("leftsemi", "leftanti", "semi", "anti"):
            return DF(Relation(left.cols, left.tail, left.datasets | right.datasets, concat(left, right).quals, None, left.opaque), df.kind)
        if names:
            low = {n.lower() for n in names}
            using_cols: list[ColInfo] = []
            for n in names:
                lk_l, lk_r = left.lookup(n), right.lookup(n)
                if how in ("right", "rightouter"):
                    origins = lk_r.origins
                elif how in ("full", "outer", "fullouter"):
                    origins = lk_l.origins | lk_r.origins
                else:
                    origins = lk_l.origins | (lk_r.origins if how == "cross" else frozenset())
                if how in ("inner", ""):
                    origins = lk_l.origins | lk_r.origins
                part = lk_l.status != "ok" or lk_r.status != "ok"
                using_cols.append(ColInfo(n, origins, part, n))
            cols = [*using_cols, *[c for c in left.cols if c.name.lower() not in low], *[c for c in right.cols if c.name.lower() not in low]]
            j = concat(left, right)
            return DF(Relation(tuple(cols), left.tail + right.tail, j.datasets, j.quals, None, j.opaque), df.kind)
        j = concat(left, right)
        return DF(j, df.kind)

    def union(self, df: DF, args: list[Any], kwargs: dict[str, Any], by_name: bool) -> Any:
        """``union`` (by position) / ``unionByName`` (by name)."""
        others = [a for a in args if isinstance(a, DF)]
        if not others and args and isinstance(args[0], (list, tuple)):
            others = [a for a in args[0] if isinstance(a, DF)]
        out = df.rel
        for o in others:
            r = o.rel
            datasets = out.datasets | r.datasets
            if by_name or kwargs.get("allowMissingColumns"):
                cols = list(out.cols)
                for i, c in enumerate(cols):
                    m = [x for x in r.cols if x.name.lower() == c.name.lower()]
                    if m:
                        cols[i] = ColInfo(c.name, c.origins | m[0].origins, c.partial or m[0].partial, c.expr if c.expr == m[0].expr else None)
                if kwargs.get("allowMissingColumns"):
                    cols.extend(x for x in r.cols if not any(c.name.lower() == x.name.lower() for c in out.cols))
                out = Relation(tuple(cols), tuple(dict.fromkeys((*out.tail, *r.tail))), datasets, (), None, out.opaque or r.opaque)
            elif out.positional_ok and r.positional_ok and len(out.cols) == len(r.cols):
                cols = [ColInfo(c.name, c.origins | x.origins, c.partial or x.partial, c.expr if c.expr == x.expr else None) for c, x in zip(out.cols, r.cols, strict=True)]
                out = Relation(tuple(cols), (), datasets)
            else:
                out = Relation(tuple(ColInfo(c.name, c.origins, True, c.expr) for c in out.cols), tuple(dict.fromkeys((*out.tail, *r.tail))), datasets, (), None,
                               out.opaque or r.opaque)
        return DF(out, df.kind)

    def m_grouped(self, g: Grouped, name: str, args: list[Any], kwargs: dict[str, Any], node: ast.AST) -> Any:
        """``groupBy(..).agg(..)`` and the shortcut aggregates."""
        rel = g.df.rel
        keys = list(g.keys)
        if name == "pivot":
            return Grouped(g.df, g.keys, True)
        aggs: list[ColInfo] = []
        if name == "agg":
            exprs = self._expand_col_args(args)
            if len(args) == 1 and isinstance(args[0], dict):
                for col_name, fn in args[0].items():
                    ci = self.resolve_col(str(col_name), rel, node)
                    fname = text_of(fn) or "agg"
                    aggs.append(ColInfo(f"{fname}({col_name})", add_step(ci.origins, f"{fname}({col_name})"), ci.partial, f"{fname}({col_name})"))
            else:
                aggs = [self.resolve_col(e, rel, node) for e in exprs]
        elif name == "count":
            aggs = [ColInfo("count", frozenset(), False, "count(1)")]
        elif name in ("sum", "avg", "mean", "min", "max"):
            targets = self._expand_col_args(args) or [c.name for c in rel.cols if c.name.lower() not in {k.name.lower() for k in keys}]
            for t in targets:
                ci = self.resolve_col(t, rel, node)
                nm = f"{name}({ci.name})"
                aggs.append(ColInfo(nm, add_step(ci.origins, nm), ci.partial, nm))
        elif name in ("applyInPandas", "apply", "applyInArrow"):
            names = self.schema_names(args[1] if len(args) > 1 else kwargs.get("schema"))
            return DF(Relation(tuple(ColInfo(n, partial=True) for n in names or ()), (), rel.datasets, (), None, True), g.df.kind)
        else:
            return DF(opaque_relation(rel.datasets), g.df.kind)
        cols = tuple([*keys, *aggs])
        if g.pivot:
            return DF(Relation(tuple(keys), (), rel.datasets, (), None, True), g.df.kind)
        return DF(Relation(cols, (), rel.datasets), g.df.kind)

    # ============================================================================================================================ SQL strings
    def run_sql_value(self, q: Any, node: ast.Call, session: SqlSession, arg_node: ast.AST | None = None) -> Any:
        """``spark.sql(q)`` / ``con.sql(q)``: the string goes through the SQL analyzer; the result is a DataFrame over its output relation."""
        sp = str_parts(q)
        if sp is None or all(f is None for f in sp[0]):
            self.gap("dynamic_reference", self.snippet(arg_node or node), "the SQL text is not a compile-time constant; its table and column dependencies are not analysed",
                     "Use literal SQL (or an f-string over constants / notebook parameters) so that the statement can be analysed.", node)
            return DF(opaque_relation())
        text = self._hole_text(q) or ""
        self.at(node, sp[1])
        saved_cap = session.confidence_cap
        session.taint = sp[1]
        loc = self.loc(node)
        try:
            results = session.run(text, process=self.process, loc=loc, line_key="sql_line")
        finally:
            session.taint = ()
            session.confidence_cap = saved_cap
        rel = next((r.relation for r in reversed(results) if r.relation is not None), None)
        if rel is None:
            return DF(opaque_relation(), "spark" if session is self.session else "duckdb")
        return DF(rel, "spark" if session is self.session else "duckdb")

    # ============================================================================================================================ writers
    def m_writer(self, w: Writer, name: str, args: list[Any], kwargs: dict[str, Any], node: ast.Call, arg_nodes: list[ast.AST]) -> Any:
        """``df.write`` builder methods and terminal writes."""
        if name == "format" or name == "using":
            return Writer(w.df, text_of(args[0]) if args else None, w.mode, w.options, w.stream, w.v2_table)
        if name == "mode" and args:
            return Writer(w.df, w.fmt, text_of(args[0]), w.options, w.stream, w.v2_table)
        if name == "option" and len(args) >= 2:
            return Writer(w.df, w.fmt, w.mode, (*w.options, (str(text_of(args[0])), args[1])), w.stream, w.v2_table)
        if name == "options":
            return Writer(w.df, w.fmt, w.mode, (*w.options, *kwargs.items()), w.stream, w.v2_table)
        if name in ("partitionBy", "bucketBy", "sortBy", "clusterBy", "outputMode", "trigger", "queryName", "partitionedBy", "tableProperty", "withNoOverwrite"):
            if name == "outputMode" and args:
                return Writer(w.df, w.fmt, w.mode or text_of(args[0]), w.options, w.stream, w.v2_table)
            return w
        mode = text_of(kwargs.get("mode")) or w.mode
        fmt = text_of(kwargs.get("format")) or w.fmt
        if name == "saveAsTable" or name == "toTable":
            t = args[0] if args else kwargs.get("name", kwargs.get("tableName", UNK))
            return self.write_table(w.df, t, mode or ("append" if w.stream else "errorifexists"), fmt, node, arg_nodes[0] if arg_nodes else None, by_position=False)
        if name == "insertInto":
            t = args[0] if args else kwargs.get("tableName", UNK)
            ov = args[1] if len(args) > 1 else kwargs.get("overwrite")
            return self.write_table(w.df, t, "overwrite" if ov is True else "append", fmt, node, arg_nodes[0] if arg_nodes else None, by_position=True)
        if name in ("create", "createOrReplace", "replace", "append", "overwrite", "overwritePartitions") and w.v2_table is not None:
            md = {"create": "create", "createOrReplace": "overwrite", "replace": "overwrite", "append": "append", "overwrite": "overwrite", "overwritePartitions": "overwrite"}[name]
            return self.write_table(w.df, w.v2_table, md, fmt, node, arg_nodes[0] if arg_nodes else None, by_position=False)
        if name in ("save", "start") or name in ("csv", "parquet", "json", "orc", "text", "avro", "xml", "delta"):
            p = args[0] if args else kwargs.get("path", dict(w.options).get("path", UNK))
            f = fmt if name in ("save", "start") else name
            if name == "start" and not args and "path" not in kwargs:
                tn = kwargs.get("tableName")
                if tn is not None:
                    return self.write_table(w.df, tn, mode or "append", fmt, node, None, by_position=False)
                return None
            return self.write_path(w.df, p, mode or ("append" if w.stream else "errorifexists"), f, node, arg_nodes[0] if arg_nodes else None)
        if name == "jdbc":
            self.write_external(w.df, dict(w.options), args, kwargs, node)
            return None
        if name == "foreachBatch" and args and isinstance(args[0], FuncV):
            self.call_func(args[0], [w.df, 0], {}, node)
            return w
        if name == "save" or name == "start":
            return None
        return None

    def write_table(self, df: DF, tname: Any, mode: str, fmt: str | None, node: ast.AST, arg_node: ast.AST | None, *, by_position: bool) -> None:
        """Terminal table write: WRITES_TO the table and TRANSFORMS edges for every output column whose origin is known."""
        txt = text_of(tname)
        if txt is not None and "." not in txt and txt.lower() in self.session.views and False:
            return None
        ds, taint = self.table_from_value(tname, node, arg_node)
        if ds is None:
            return None
        self.emit_write(df, ds, mode, fmt, node, taint=taint, by_position=by_position, is_table=True)
        return None

    def write_path(self, df: DF, p: Any, mode: str, fmt: str | None, node: ast.AST, arg_node: ast.AST | None) -> None:
        """Terminal path write (``save(path)``, ``.parquet(path)`` ...): a Tables path is a table, anything else a file."""
        ds, taint, conf = self.path_from_value(p, node, fmt, arg_node, delta=(fmt == "delta"))
        if ds is None:
            return None
        self.emit_write(df, ds, mode, fmt, node, taint=taint, by_position=False, is_table=ds.kind == "table", conf=conf)
        return None

    def write_external(self, df: DF, opts: dict[str, Any], args: list[Any], kwargs: dict[str, Any], node: ast.AST) -> None:
        url = text_of(args[0]) if args else text_of(opts.get("url", kwargs.get("url")))
        table = text_of(args[1]) if len(args) > 1 else text_of(opts.get("dbtable", kwargs.get("table")))
        if not url:
            self.gap("dynamic_reference", "jdbc", "the JDBC URL is not a compile-time constant", None, node)
            return
        clean = re.sub(r"//[^/@]*@", "//", url.split("?")[0].split(";")[0])
        name = f"{clean}/{table}" if table else clean
        ds = Dataset("x:" + name, "external", name, AssetRef.make("external", name=name))
        self.session.datasets[ds.key] = ds
        self.emit_write(df, ds, "append", "jdbc", node, taint=(), by_position=False, is_table=False)

    def emit_write(self, df: DF, ds: Dataset, mode: str, fmt: str | None, node: ast.AST, *, taint: tuple[str, ...], by_position: bool, is_table: bool, conf: str = "derived") -> None:
        """WRITES_TO plus the column mapping of ``df`` onto the written table."""
        self.at(node, taint)
        s = self.session
        saved = s.confidence_cap
        if conf == "inferred":
            s.confidence_cap = "inferred"
        try:
            attrs: dict[str, Any] = {"format": fmt} if fmt else {}
            s._write(ds, mode or "append", attrs)
            rel = df.rel
            if is_table and ds.kind == "table":
                if rel.positional_ok and rel.cols:
                    names: list[str | None] = [c.name for c in rel.cols]
                    note, confidence = None, "derived"
                    if by_position:
                        if ds.schema and len(ds.schema) == len(rel.cols):
                            names = list(ds.schema)
                        elif ds.schema:
                            self.gap("unsupported_construct", ds.name, f"insertInto: the DataFrame has {len(rel.cols)} columns but the table has {len(ds.schema)}", None, node)
                            names = []
                        else:
                            note, confidence = "insertInto maps by position; the table schema is unknown, names taken from the DataFrame", "inferred"
                    s.emit_columns(rel, ds, names, mode=mode or "append", confidence=confidence, note=note)
                    if names and not ds.schema:
                        s.table_schemas[ds.key] = tuple(str(n) for n in names if n)
                elif rel.cols and not rel.positional_ok:
                    known = [c for c in rel.cols if c.origins]
                    if not by_position:
                        s.emit_columns(Relation(tuple(known)), ds, [c.name for c in known], mode=mode or "append")
                    self.gap("unsupported_construct", ds.name, f"the columns of the DataFrame written to '{ds.name}' are only partly known (unknown schema upstream, pivot, rdd or pandas UDF); "
                             "table-level lineage is recorded, column-level lineage only for the known columns", "Provide the source table schemas (run discovery) or select the columns explicitly.", node)
                else:
                    self.gap("unsupported_construct", ds.name, f"the columns of the DataFrame written to '{ds.name}' are not statically known (SELECT * over an unknown schema, rdd, mapInPandas ...); "
                             "table-level lineage is recorded", "Provide the source table schemas (run discovery) or select the columns explicitly.", node)
        finally:
            s.confidence_cap = saved

    # ---- delta lake
    def delta_for(self, last: str, args: list[Any], kwargs: dict[str, Any], node: ast.Call, arg_nodes: list[ast.AST]) -> Any:
        """``DeltaTable.forName(spark, name)`` / ``forPath(spark, path)`` handles."""
        a = [x for x in args if not isinstance(x, SparkV)]
        v = a[0] if a else kwargs.get("tableOrViewName", kwargs.get("path", kwargs.get("identifier", UNK)))
        an = arg_nodes[1] if len(arg_nodes) > 1 else (arg_nodes[0] if arg_nodes else None)
        if last in ("forName", "createIfNotExists", "createOrReplace"):
            ds, taint = self.table_from_value(v, node, an)
            if ds is None:
                return UNK
            return DeltaTbl(ds, None, False, "inferred" if taint else "derived")
        ds2, taint2, conf = self.path_from_value(v, node, "delta", an, delta=True)
        return DeltaTbl(ds2, None, False, "inferred" if (taint2 or conf == "inferred") else "derived") if ds2 is not None else UNK

    def deltalake_open(self, args: list[Any], kwargs: dict[str, Any], node: ast.Call, arg_nodes: list[ast.AST]) -> Any:
        """``deltalake.DeltaTable(path)`` (delta-rs)."""
        v = args[0] if args else kwargs.get("table_uri", UNK)
        ds, taint, conf = self.path_from_value(v, node, "delta", arg_nodes[0] if arg_nodes else None, delta=True)
        if ds is None:
            return UNK
        return DeltaTbl(ds, None, True, "inferred" if (taint or conf == "inferred") else "derived")

    def m_delta(self, t: DeltaTbl, name: str, args: list[Any], kwargs: dict[str, Any], node: ast.Call, arg_nodes: list[ast.AST]) -> Any:
        """Methods of a Delta table handle (Spark ``DeltaTable`` and delta-rs ``DeltaTable``)."""
        if name == "alias" and args:
            return DeltaTbl(t.ds, text_of(args[0]), t.py, t.conf)
        if name in ("toDF", "to_pandas", "to_pyarrow_table", "to_pyarrow_dataset", "load_as_version", "to_pyarrow", "scan", "to_polars", "to_pandas_df"):
            self.read_dataset(t.ds, node, conf=t.conf)
            kind = "spark" if name == "toDF" else "pandas" if name in ("to_pandas", "to_pandas_df") else "arrow"
            return DF(self.session.base_relation(t.ds), kind)
        if name == "merge":
            if t.py:
                src = args[0] if args else kwargs.get("source", UNK)
                cond = args[1] if len(args) > 1 else kwargs.get("predicate", UNK)
                sa = text_of(kwargs.get("source_alias", args[2] if len(args) > 2 else None))
                ta = text_of(kwargs.get("target_alias", args[3] if len(args) > 3 else None))
            else:
                src = args[0] if args else kwargs.get("source", UNK)
                cond = args[1] if len(args) > 1 else kwargs.get("condition", UNK)
                sa, ta = None, t.alias
            source = src if isinstance(src, DF) else DF(opaque_relation())
            return MergeB(t.ds, source, ta, sa or (source.rel.alias), cond)
        if name in ("update", "updateExpr") and not t.py or name == "update" and t.py:
            sets = kwargs.get("set") if kwargs.get("set") is not None else (args[1] if len(args) > 1 else (args[0] if args and isinstance(args[0], dict) else None))
            self.emit_set_clause(t.ds, DF(Relation(), "spark"), t.alias, None, sets if isinstance(sets, dict) else {}, "update", node, t.conf)
            return None
        if name == "delete":
            self.at(node)
            saved = self.session.confidence_cap
            self.session.confidence_cap = "inferred" if t.conf == "inferred" else saved
            try:
                self.session._write(t.ds, "delete")
            finally:
                self.session.confidence_cap = saved
            return None
        return UNK

    def m_merge(self, m: MergeB, name: str, args: list[Any], kwargs: dict[str, Any], node: ast.Call) -> Any:
        """Delta merge builder: ``whenMatched*`` / ``whenNotMatched*`` clauses, applied at ``execute()``."""
        low = name.lower().replace("_", "")
        if low in ("whenmatchedupdate", "whenmatchedupdateall", "whennotmatchedinsert", "whennotmatchedinsertall", "whenmatcheddelete", "whennotmatchedbysourceupdate",
                   "whennotmatchedbysourcedelete", "whenmatchedupdateexpr", "whennotmatchedinsertexpr"):
            sets = kwargs.get("set", kwargs.get("values", kwargs.get("updates", args[1] if len(args) > 1 else (args[0] if args and isinstance(args[0], dict) else None))))
            m.clauses.append((low, sets if isinstance(sets, dict) else None))
            return m
        if low == "execute":
            self.emit_merge(m, node)
            return None
        return m

    def emit_merge(self, m: MergeB, node: ast.AST) -> None:
        """Emit WRITES_TO (merge) and the column mappings of every clause."""
        self.at(node)
        self.session._write(m.target, "merge")
        for kind, sets in m.clauses:
            if kind.endswith("delete") or kind == "whennotmatchedbysourceupdate":
                continue
            if kind in ("whenmatchedupdateall", "whennotmatchedinsertall"):
                src = m.source.rel
                if not src.cols or not src.positional_ok:
                    self.gap("unsupported_construct", m.target.name, f"merge {kind}All: the source DataFrame's columns are not statically known, so the column mapping is not available",
                             "Provide the source table schemas or list the columns in the merge.", node)
                    continue
                self.session.emit_columns(src, m.target, [c.name for c in src.cols], mode="merge")
            elif sets:
                self.emit_set_clause(m.target, m.source, m.target_alias, m.source_alias, sets, "merge", node, "derived", write=False)

    def emit_set_clause(self, target: Dataset, source: DF, t_alias: str | None, s_alias: str | None, sets: dict[Any, Any], mode: str, node: ast.AST, conf: str,
                        write: bool = True) -> None:
        """``{target column: expression}`` mapping of ``update`` / merge clauses: expressions are SQL strings (``"s.price * 2"``) or column objects."""
        self.at(node)
        s = self.session
        if write:
            saved = s.confidence_cap
            s.confidence_cap = "inferred" if conf == "inferred" else saved
            try:
                s._write(target, mode)
            finally:
                s.confidence_cap = saved
        t_rel = s.base_relation(target)
        scope_rel = concat(t_rel.with_alias(t_alias or target.name.split(".")[-1]), source.rel.with_alias(s_alias or "source"))
        cols: list[ColInfo] = []
        names: list[str | None] = []
        for k, v in sets.items():
            tname = text_of(k)
            if tname is None:
                continue
            tname = tname.split(".")[-1]
            if isinstance(v, (str, PStr)):
                expr_v = self.expr_col(v, node)
            else:
                expr_v = v
            ci = self.resolve_col(expr_v, scope_rel, node)
            cols.append(ci)
            names.append(tname)
        if cols:
            s.confidence_cap = "inferred" if conf == "inferred" else s.confidence_cap
            try:
                s.emit_columns(Relation(tuple(cols)), target, names, mode=mode)
            finally:
                s.confidence_cap = None if conf != "inferred" else s.confidence_cap

    def write_deltalake(self, args: list[Any], kwargs: dict[str, Any], node: ast.Call, arg_nodes: list[ast.AST]) -> Any:
        """``write_deltalake(path_or_table, data, mode=...)``."""
        target = args[0] if args else kwargs.get("table_or_uri", UNK)
        data = args[1] if len(args) > 1 else kwargs.get("data", UNK)
        mode = text_of(kwargs.get("mode")) or "error"
        df = data if isinstance(data, DF) else DF(opaque_relation())
        if isinstance(target, DeltaTbl):
            self.emit_write(df, target.ds, mode, "delta", node, taint=(), by_position=False, is_table=True, conf=target.conf)
            return None
        ds, taint, conf = self.path_from_value(target, node, "delta", arg_nodes[0] if arg_nodes else None, delta=True)
        if ds is not None:
            self.emit_write(df, ds, mode, "delta", node, taint=taint, by_position=False, is_table=ds.kind == "table", conf=conf)
        return None

    # ---- pandas
    def pandas_fn(self, last: str, args: list[Any], kwargs: dict[str, Any], node: ast.Call, arg_nodes: list[ast.AST]) -> Any:
        """``pandas`` module functions: file readers, ``concat``, ``merge``, ``DataFrame``."""
        if last.startswith("read_") and last not in ("read_sql", "read_sql_query", "read_sql_table", "read_clipboard", "read_pickle"):
            p = args[0] if args else kwargs.get("path", kwargs.get("filepath_or_buffer", kwargs.get("io", UNK)))
            fmt = {"read_parquet": "parquet", "read_csv": "csv", "read_json": "json", "read_excel": "xlsx", "read_orc": "orc", "read_feather": "feather", "read_table": "csv"}.get(last)
            ds, taint, conf = self.path_from_value(p, node, fmt, arg_nodes[0] if arg_nodes else None)
            if ds is None:
                return DF(opaque_relation(), "pandas")
            self.read_dataset(ds, node, taint=taint, conf=conf)
            return DF(self.session.base_relation(ds), "pandas")
        if last in ("read_sql", "read_sql_query", "read_sql_table"):
            self.gap("unsupported_construct", "pandas.read_sql", "database reads through SQLAlchemy / DBAPI connections are not modelled", "Document the dependency manually.", node)
            return DF(opaque_relation(), "pandas")
        if last == "concat":
            frames = [f for f in (args[0] if args and isinstance(args[0], (list, tuple)) else args) if isinstance(f, DF)]
            if frames:
                first = frames[0]
                return self.union(first, [DF(f.rel) for f in frames[1:]], {"allowMissingColumns": True}, True) if len(frames) > 1 else first
            return DF(opaque_relation(), "pandas")
        if last == "merge" and len(args) >= 2 and isinstance(args[0], DF):
            return self.join(args[0], args[1:], kwargs, node)
        if last == "DataFrame":
            data = args[0] if args else kwargs.get("data")
            if isinstance(data, DF):
                return data
            if isinstance(data, dict) and data and all(isinstance(k, str) for k in data):
                return DF(Relation(tuple(ColInfo(str(k)) for k in data)), "pandas")
            return DF(opaque_relation(), "pandas")
        return UNK

    def pandas_write(self, df: DF, name: str, args: list[Any], kwargs: dict[str, Any], node: ast.Call, arg_nodes: list[ast.AST]) -> Any:
        """``df.to_parquet(path)`` and friends."""
        if name in ("to_sql", "to_clipboard", "to_pickle", "to_dict", "to_numpy", "to_records", "to_string", "to_markdown", "to_html", "to_latex", "to_list", "to_json_str"):
            if name == "to_sql":
                self.gap("unsupported_construct", "pandas.to_sql", "database writes through SQLAlchemy / DBAPI connections are not modelled", "Document the dependency manually.", node)
            return UNK
        fmt = {"to_parquet": "parquet", "to_csv": "csv", "to_json": "json", "to_excel": "xlsx", "to_orc": "orc", "to_feather": "feather"}.get(name)
        if fmt is None:
            return UNK
        p = args[0] if args else kwargs.get("path", kwargs.get("path_or_buf", kwargs.get("excel_writer", UNK)))
        if p is None or p is UNK and not args and "path" not in kwargs and "path_or_buf" not in kwargs:
            return UNK
        ds, taint, conf = self.path_from_value(p, node, fmt, arg_nodes[0] if arg_nodes else None)
        if ds is not None:
            self.emit_write(df, ds, "overwrite", fmt, node, taint=taint, by_position=False, is_table=ds.kind == "table", conf=conf)
        return None

    # ---- duckdb
    def conn_session(self, cid: int) -> SqlSession:
        """The SQL session of a DuckDB connection (own temp views and local tables, shared fact builder)."""
        sess = self.conns.get(cid)
        if sess is None:
            sess = SqlSession(self.ctx, self.b, "duckdb", analyzer=PYSPARK, emit_query_assets=False)
            sess.local_tables = True
            sess.datasets = self.session.datasets
            sess.var_lookup = self._replacement_scan
            self.conns[cid] = sess
        return sess

    def _replacement_scan(self, name: str) -> Relation | None:
        """DuckDB replacement scan: ``FROM df`` reads the Python variable ``df``."""
        for env in (self.env,):
            v = env.lookup(name)
            if isinstance(v, DF):
                return v.rel
        return None

    def duckdb_fn(self, last: str, args: list[Any], kwargs: dict[str, Any], node: ast.Call, arg_nodes: list[ast.AST], env: PyEnv) -> Any:
        """``duckdb.connect()`` and the module-level shortcuts (default in-memory connection)."""
        if last in ("connect", "cursor"):
            self._uid += 1
            return ConnV(self._uid)
        return self.m_conn(ConnV(0), last, args, kwargs, node, arg_nodes, env)

    def m_conn(self, c: ConnV, name: str, args: list[Any], kwargs: dict[str, Any], node: ast.Call, arg_nodes: list[ast.AST], env: PyEnv) -> Any:
        """Methods of a DuckDB connection / relation."""
        sess = self.conn_session(c.cid)
        if name in ("sql", "query", "execute", "executemany"):
            q = args[0] if args else kwargs.get("query", UNK)
            if name == "execute":
                out = self.run_sql_value(q, node, sess, arg_nodes[0] if arg_nodes else None)
                return out
            return self.run_sql_value(q, node, sess, arg_nodes[0] if arg_nodes else None)
        if name == "register" and len(args) >= 2:
            nm = text_of(args[0])
            if nm and isinstance(args[1], DF):
                sess.views[nm.lower()] = args[1].rel
            elif nm:
                sess.views[nm.lower()] = opaque_relation()
            return c
        if name == "unregister" and args:
            nm = text_of(args[0])
            if nm:
                sess.views.pop(nm.lower(), None)
            return c
        if name in ("read_parquet", "read_csv", "read_json", "from_parquet", "from_csv_auto", "read_csv_auto"):
            p = args[0] if args else kwargs.get("path_or_buffer", UNK)
            ds, taint, conf = self.path_from_value(p, node, name.split("_", 1)[-1], arg_nodes[0] if arg_nodes else None)
            if ds is None:
                return DF(opaque_relation(), "duckdb")
            self.read_dataset(ds, node, taint=taint, conf=conf)
            return DF(self.session.base_relation(ds), "duckdb")
        if name == "table" and args:
            nm = text_of(args[0])
            if nm and nm.lower() in sess.views:
                return DF(sess.views[nm.lower()], "duckdb")
            return self.read_table_value(args[0], node, arg_nodes[0] if arg_nodes else None)
        if name in ("from_df", "from_arrow") and args and isinstance(args[0], DF):
            return DF(args[0].rel, "duckdb")
        if name in ("close", "commit", "begin", "rollback", "install_extension", "load_extension", "fetchall", "fetchone", "fetchmany", "fetchnumpy", "description", "pl", "fetch_arrow_table_"):
            return UNK
        if name in ("df", "fetchdf", "to_df"):
            return UNK
        return UNK

    # ---- notebookutils / sempy
    def notebookutils(self, rest: str, args: list[Any], kwargs: dict[str, Any], node: ast.Call, arg_nodes: list[ast.AST]) -> Any:
        """``notebookutils`` / ``mssparkutils`` calls that carry lineage: ``fs.cp/mv/put/head``, ``lakehouse.get``, ``notebook.run/runMultiple``."""
        if rest in ("fs.cp", "fs.mv", "fs.fastcp", "fs.copy", "fs.move"):
            src, dst = (args + [UNK, UNK])[:2]
            sds, st, sc = self.path_from_value(src, node, None, arg_nodes[0] if arg_nodes else None)
            dds, dt, dc = self.path_from_value(dst, node, None, arg_nodes[1] if len(arg_nodes) > 1 else None)
            if sds is not None:
                self.read_dataset(sds, node, taint=st, conf=sc)
            if dds is not None:
                self.at(node, dt)
                saved = self.session.confidence_cap
                self.session.confidence_cap = "inferred" if dc == "inferred" else saved
                try:
                    self.session._write(dds, "copy" if rest.endswith(("cp", "fastcp", "copy")) else "move")
                finally:
                    self.session.confidence_cap = saved
            return True
        if rest in ("fs.put", "fs.append"):
            dds, dt, dc = self.path_from_value(args[0] if args else UNK, node, None, arg_nodes[0] if arg_nodes else None)
            if dds is not None:
                self.at(node, dt)
                self.session._write(dds, "overwrite" if rest == "fs.put" else "append")
            return True
        if rest == "fs.head":
            sds, st, sc = self.path_from_value(args[0] if args else UNK, node, None, arg_nodes[0] if arg_nodes else None)
            if sds is not None:
                self.read_dataset(sds, node, taint=st, conf=sc)
            return UNK
        if rest in ("lakehouse.get", "lakehouse.getWithProperties"):
            nm = text_of(args[0]) if args else None
            if nm is None:
                return UNK
            root = f"abfss://{HOLE}@onelake.dfs.fabric.microsoft.com/{nm}.Lakehouse"
            return {"displayName": nm, "properties": {"oneLakeFilesPath": mk_str(["abfss://", None, f"@onelake.dfs.fabric.microsoft.com/{nm}.Lakehouse/Files"]),
                                                      "oneLakeTablesPath": mk_str(["abfss://", None, f"@onelake.dfs.fabric.microsoft.com/{nm}.Lakehouse/Tables"]),
                                                      "abfsPath": mk_str(["abfss://", None, f"@onelake.dfs.fabric.microsoft.com/{nm}.Lakehouse"])}, "root": root}
        if rest == "notebook.run":
            nm = text_of(args[0]) if args else text_of(kwargs.get("name"))
            self.notebook_dependency(nm, args[0] if args else UNK, node)
            return UNK
        if rest == "notebook.runMultiple":
            dag = args[0] if args else kwargs.get("DAG", kwargs.get("dag", UNK))
            acts = dag.get("activities") if isinstance(dag, dict) else (dag if isinstance(dag, (list, tuple)) else None)
            if acts is None:
                self.gap("dynamic_reference", self.snippet(node), "the list of notebooks passed to runMultiple is not a compile-time constant", None, node)
                return UNK
            for a in acts:
                if isinstance(a, dict):
                    p = a.get("path", a.get("notebook"))
                    self.notebook_dependency(text_of(p), p if p is not None else UNK, node)
                elif isinstance(a, (str, PStr)):
                    self.notebook_dependency(text_of(a), a, node)
            return UNK
        return UNK

    def notebook_dependency(self, name: str | None, raw: Any, node: ast.AST) -> None:
        """``DEPENDS_ON`` from this notebook to the notebook it runs."""
        if not name:
            self.gap("dynamic_reference", self.snippet(node), "the name of the notebook that is run is not a compile-time constant", "Use a literal notebook name.", node)
            return
        self.record_notebook_call(name, node, "notebookutils.notebook.run")

    def record_notebook_call(self, name: str, node: ast.AST | None, how: str, *, loc: dict[str, Any] | None = None, text: str | None = None) -> None:
        """Add the DEPENDS_ON edge for a called notebook (``%run``, ``notebookutils.notebook.run``); ``loc`` / ``text`` override the position taken from ``node``."""
        known = getattr(self.ctx, "known_notebooks", {}) or {}
        aliases = {"olcf_item": known[name]} if name in known else None
        ref = AssetRef.make("notebook", aliases=aliases, name=name, scope=self.ctx.scope())
        self.b.edge(self.process, ref, "DEPENDS_ON", confidence="derived", kind="static", location=loc if loc is not None else self.loc(node),
                    snippet=text if text is not None else (self.snippet(node) or how), attrs={"via": how}, analyzer=PYSPARK)

    def sempy(self, last: str, args: list[Any], kwargs: dict[str, Any], node: ast.Call, arg_nodes: list[ast.AST]) -> Any:
        """``sempy.fabric`` reads of semantic-model tables / measures / DAX: a dependency on the model (no column mapping)."""
        if last in ("read_table", "evaluate_measure", "evaluate_dax", "read_table_cached", "list_measures", "list_columns", "list_tables"):
            ds_name = text_of(args[0] if args else kwargs.get("dataset"))
            if ds_name is None:
                self.gap("dynamic_reference", self.snippet(node), "the semantic model name is not a compile-time constant", None, node)
                return DF(opaque_relation(), "pandas")
            if last.startswith("list_"):
                return UNK
            table = text_of(args[1] if len(args) > 1 else kwargs.get("table")) if last in ("read_table", "read_table_cached") else None
            ref = (AssetRef.make("model_table", name=table, scope=self.ctx.scope(semantic_model=ds_name)) if table
                   else AssetRef.make("semantic_model", name=ds_name, scope=self.ctx.scope()))
            self.b.edge(self.process, ref, "READS_FROM", confidence="derived", kind="static", location=self.loc(node), snippet=self.snippet(node),
                        attrs={"dataset_kind": "model_table" if table else "semantic_model", "api": f"sempy.{last}"}, analyzer=PYSPARK)
            return DF(opaque_relation(), "pandas")
        return UNK


class _Range:
    """A known ``range(...)`` (its elements, as a tuple)."""

    def __init__(self, items: tuple[int, ...]) -> None:
        self.items = items


_BUILTIN_NAMES = frozenset("""len str int float bool list tuple set frozenset dict range enumerate zip sorted reversed min max sum abs round repr any all map filter isinstance format next
getattr exec eval compile __import__ print open type hasattr iter id hash""".split())


# ================================================================================================================================== public API
def analyze_python(source: str, *, context: PyContext | LineageContext | Mapping[str, Any] | None = None) -> Facts:
    """Analyse Python / PySpark source and return its lineage as :class:`~olcf.intelligence.model.Facts`; never raises.

    The process asset is ``context.process`` when given, otherwise a ``script`` asset keyed by the hash of the source. See the module docstring for what is recognised and
    which gaps are reported.
    """
    ctx = LineageContext.coerce(context)
    if isinstance(context, PyContext):
        ctx = context
    builder = FactBuilder(PYSPARK, ctx)
    try:
        if not isinstance(source, str):
            builder.gap("parse_error", "python", "the Python payload is not text", None, ctx.loc(), PYSPARK)
            return builder.facts()
        process = ctx.process or AssetRef.make("script", aliases={"query": statement_alias(source)})
        if ctx.process is None:
            builder.asset(AssetFact(ref=process, name=ctx.item_name or "script", attrs={"language": "python", "lines": source.count("\n") + 1}, workspace_id=ctx.workspace_id))
        interp = PyAnalyzer(ctx, builder, process, dialect=ctx.dialect if ctx.dialect in ("spark", "duckdb") else "spark")
        interp.run_cell(source, None)
        interp.finish()
    except Exception as exc:  # noqa: BLE001 - analyzers never raise
        builder.gap("parse_error", "python", f"Python analysis failed unexpectedly ({type(exc).__name__}: {str(exc)[:160]})", "Report the code to the OLCF maintainers.", ctx.loc(), PYSPARK)
    return builder.facts()


__all__ = ["PyAnalyzer", "analyze_python", "split_statements"]
