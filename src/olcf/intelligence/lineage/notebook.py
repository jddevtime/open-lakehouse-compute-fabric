# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Lineage of a notebook (nbformat 4 documents written by OLCF or by Microsoft Fabric), found by analysing its cells in order in one shared session.

:func:`analyze_notebook` ``(nb, *, context) -> Facts`` walks the code cells top to bottom, like a kernel does: variables, functions and temp views defined in one cell are
visible in the later ones, so ``spark.sql(f"... {table}")`` in cell 7 uses the ``table`` assigned in cell 2. Cells are dispatched on their language:

* plain code and ``%%pyspark`` / ``%%python`` / ``%%spark`` / ``%%time`` / ``%%capture`` cells go to the Python analyzer (:mod:`olcf.intelligence.lineage.pyspark`);
  line magics (``%pip``, ``%matplotlib``, ``!cmd`` ...) are blanked so that line numbers stay exact;
* ``%%sql`` / ``%%sparksql`` cells (and Fabric cells whose language metadata is ``sparksql`` / ``sql``) go to the SQL analyzer; ``%%sql -o VAR`` binds the result to ``VAR``;
* ``%run <notebook>``, ``notebookutils.notebook.run`` / ``runMultiple`` and ``mssparkutils.notebook.run`` add a ``DEPENDS_ON`` edge from this notebook to the called one
  (by name; the ``known_notebooks`` map of the context supplies the exact item);
* ``%%configure`` JSON with ``defaultLakehouse`` and Fabric ``metadata.dependencies.lakehouse`` set the default lakehouse that unqualified table names and relative paths use;
* a cell tagged ``parameters`` defines notebook parameters: names derived from them are analysed with the default value and the resulting edges are ``inferred``
  (a pipeline or job may pass another value), and ``context.parameters`` overrides the defaults;
* markdown / raw cells and outputs are ignored; ``%%scala``, ``%%sparkr``, ``%%csharp`` (and cells whose language metadata says so) are reported as ``unsupported_construct`` gaps,
  ``%%bash`` / ``%%sh`` / ``!`` shell cells and ``%%html`` are skipped silently because they cannot touch lakehouse tables through Spark.

Evidence locations are ``{"item": <item id>, "cell": <0-based index in the cells array>, "line": <1-based line in the cell>}``. A cell that is not valid Python gives a ``parse_error``
gap and the remaining cells are still analysed. Limits: at most :data:`~olcf.intelligence.lineage.base.MAX_CELLS` cells and 8 million characters per cell; larger input is
cut with an ``unsupported_construct`` gap.
"""

from __future__ import annotations

import dataclasses
import io
import json
import re
import tokenize
from collections.abc import Mapping
from typing import Any

from olcf.intelligence.lineage.base import (
    MAX_CELLS,
    MAX_JSON_BYTES,
    NOTEBOOK,
    FactBuilder,
    LineageContext,
    PyContext,
    notebook_ref,
)
from olcf.intelligence.lineage.pyspark import PyAnalyzer
from olcf.intelligence.lineage.pyvalues import DF
from olcf.intelligence.lineage.relation import opaque_relation
from olcf.intelligence.model import AssetFact, Facts, looks_like_guid

_PY_MAGICS = {"pyspark", "python", "spark", "time", "timeit", "capture", "prun", "script", "python3"}
_SQL_MAGICS = {"sql", "sparksql", "spark.sql"}
_UNSUPPORTED_LANG = {"scala": "Scala", "sparkr": "SparkR", "r": "R", "csharp": "C#", "dotnet": "C#", "java": "Java"}
_SKIPPED_MAGICS = {"bash", "sh", "shell", "html", "markdown", "md", "javascript", "js", "latex", "svg", "writefile", "file", "pip", "bigquery", "capture_html", "debug",
                   "load_ext", "perl", "ruby", "cmd", "powershell"}
_IGNORED_LINE_MAGICS = {"pip", "conda", "matplotlib", "load_ext", "reload_ext", "autoreload", "config", "env", "time", "timeit", "who", "whos", "lsmagic", "pwd", "cd", "ls",
                        "alias", "bookmark", "cat", "cp", "mv", "rm", "mkdir", "rehashx", "reset", "sx", "system", "precision", "pprint", "xmode", "unload_ext", "autosave", "history",
                        "magic", "page", "pdb", "pinfo", "pinfo2", "psearch", "quickref", "recall", "save", "store", "sc", "tb", "unalias", "who_ls", "dhist", "dirs", "doctest_mode",
                        "edit", "killbgscripts", "logoff", "logon", "logstart", "logstate", "logstop", "notebook", "paste", "pastebin", "popd", "pushd", "pycat", "pylab", "qtconsole",
                        "rerun", "run_cell", "gui", "colors", "automagic", "macro", "lprun", "mprun", "memit", "load", "loadpy", "matplotlib_inline", "pip3", "set_env", "sql_ignored"}


# the tokenizer reports a lone ``!`` as an error token before Python 3.12 and as an operator since
_BANG_TOKENS = (tokenize.ERRORTOKEN, tokenize.OP)


@dataclasses.dataclass
class _LineMagic:
    """A line magic found in code: ``line`` is 1-based, ``name`` the magic without ``%``, ``args`` the rest of the line."""

    line: int
    name: str
    args: str


def _dict(x: Any) -> dict[str, Any]:
    """``x`` when it is a dict, else an empty dict (notebook JSON is untrusted)."""
    return x if isinstance(x, dict) else {}


def cell_source(cell: dict[str, Any]) -> str:
    """The source text of a cell (a string or a list of lines; nbformat 3 ``input`` accepted)."""
    src = cell.get("source", cell.get("input", ""))
    if isinstance(src, list):
        return "".join(str(x) for x in src)
    return src if isinstance(src, str) else ""


def cell_language(cell: dict[str, Any]) -> str:
    """Language declared in the cell metadata (Fabric ``microsoft.language``, VS Code ``vscode.languageId`` or OLCF ``language``); ``python`` when absent."""
    md = _dict(cell.get("metadata"))
    for path in (("microsoft", "language"), ("language",), ("vscode", "languageId"), ("polyglot_notebook", "kernelName"), ("dotnet_interactive", "language")):
        cur: Any = md
        for p in path:
            cur = cur.get(p) if isinstance(cur, dict) else None
        if isinstance(cur, str) and cur:
            return cur.lower()
    return "python"


def cell_tags(cell: dict[str, Any]) -> list[str]:
    """The tags of a cell."""
    tags = _dict(cell.get("metadata")).get("tags")
    return [t for t in tags if isinstance(t, str)] if isinstance(tags, list) else []


def split_cell_magic(src: str) -> tuple[str | None, str, str]:
    """``(magic name, arguments of the magic line, body)`` for a cell that starts with ``%%name``; ``(None, "", src)`` otherwise. The body keeps the original line structure
    (the magic line itself is removed)."""
    first, _, rest = src.partition("\n")
    m = re.match(r"^\s*%%\s*([A-Za-z_][\w.]*)\b(.*)$", first)
    if not m:
        return None, "", src
    return m.group(1).lower(), m.group(2).strip(), rest


def strip_line_magics(src: str) -> tuple[str, list[_LineMagic]]:
    """Blank IPython line magics (``%run x``, ``%pip install y``, ``!ls``, ``x = !ls``) and return them; the result has exactly the same number of lines.

    Statement starts are found with the tokenizer, so a ``%`` operator at the start of a continuation line inside brackets or a ``!`` inside a string is left alone.
    """
    lines = src.split("\n")
    marks: set[int] = set()
    try:
        new_logical = True
        depth = 0
        for tok in tokenize.generate_tokens(io.StringIO(src).readline):
            tt, ts = tok.type, tok.string
            if tt in (tokenize.ENCODING, tokenize.INDENT, tokenize.DEDENT, tokenize.NL, tokenize.COMMENT):
                continue
            if tt == tokenize.NEWLINE:
                new_logical = True
                depth = 0
                continue
            if new_logical:
                if (tt == tokenize.OP and ts == "%") or (tt in _BANG_TOKENS and ts == "!"):
                    marks.add(tok.start[0])
                new_logical = False
            elif tt in _BANG_TOKENS and ts == "!" and depth == 0:
                marks.add(tok.start[0])
            if tt == tokenize.OP and ts in "([{":
                depth += 1
            elif tt == tokenize.OP and ts in ")]}":
                depth = max(0, depth - 1)
    except (tokenize.TokenError, IndentationError, SyntaxError, ValueError):
        marks = {i for i, ln in enumerate(lines, 1) if re.match(r"^\s*(%[A-Za-z_]|!)", ln)}
    found: list[_LineMagic] = []
    for n in sorted(marks):
        if n > len(lines):
            continue
        ln = lines[n - 1]
        indent = ln[:len(ln) - len(ln.lstrip())]
        m = re.match(r"^\s*%\s*([A-Za-z_][\w.]*)\s*(.*)$", ln)
        asg = re.match(r"^(\s*)([A-Za-z_]\w*)\s*=\s*[%!]", ln)
        if m:
            found.append(_LineMagic(n, m.group(1).lower(), m.group(2).strip()))
        else:
            found.append(_LineMagic(n, "!", ln.lstrip()[1:].strip()))
        if m and m.group(1).lower() in ("time", "timeit") and m.group(2).strip():
            # ``%time expr`` / ``%timeit -n 1 expr`` run the expression: keep it
            expr = re.sub(r"^(?:-[A-Za-z]\s*\d*\s+)*", "", m.group(2).strip())
            lines[n - 1] = f"{indent}{expr}"
            continue
        lines[n - 1] = f"{asg.group(1)}{asg.group(2)} = None" if asg else f"{indent}pass"
    return "\n".join(lines), found


def _run_target(args: str) -> str | None:
    """The notebook named by the arguments of ``%run`` (quotes, a leading ``./`` and ``{...}`` parameter objects are dropped), or ``None`` when the name is not a plain literal
    (``$var``, ``{var}``, ``%`` or shell expansions), which cannot be resolved statically."""
    rest = args.strip()
    while rest.startswith("-"):
        rest = rest.split(None, 1)[1].lstrip() if len(rest.split(None, 1)) > 1 else ""
    if not rest or rest.startswith("{"):
        return None
    if rest[0] in "\"'":
        end = rest.find(rest[0], 1)
        name = rest[1:end] if end > 0 else rest[1:]
    else:
        name = rest.split(None, 1)[0]
    name = re.sub(r"^\./", "", name.strip())
    if not name or re.search(r"[${}<>`%*?|;&]", name):
        return None
    return name


def _default_lakehouse_from_metadata(nb: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    """``(default_lakehouse, workspace GUID)`` from Fabric ``metadata.dependencies.lakehouse``."""
    lh = _dict(_dict(_dict(nb.get("metadata")).get("dependencies")).get("lakehouse"))
    if not lh:
        return None, None
    out: dict[str, Any] = {}
    gid = lh.get("default_lakehouse")
    if isinstance(gid, str) and looks_like_guid(gid):
        out["fabric_id"] = gid.lower()
    name = lh.get("default_lakehouse_name")
    if isinstance(name, str) and name:
        out["name"] = name
    ws = lh.get("default_lakehouse_workspace_id")
    return (out or None), (ws.lower() if isinstance(ws, str) and looks_like_guid(ws) else None)


class _NotebookRun:
    """State of one notebook analysis (the analyzer, the builder, the effective context)."""

    def __init__(self, nb: dict[str, Any], ctx: LineageContext) -> None:
        self.nb = nb
        self.ctx = dataclasses.replace(ctx)
        self.ctx.default_lakehouse = dict(ctx.default_lakehouse) if ctx.default_lakehouse else None
        self.b = FactBuilder(NOTEBOOK, self.ctx)
        self.process = notebook_ref(self.ctx)
        self.az: PyAnalyzer | None = None

    def set_default_lakehouse(self, lh: dict[str, Any] | None, ws: str | None = None) -> None:
        """Change the default lakehouse for the cells that follow."""
        cur = self.ctx.default_lakehouse or {}
        other_lakehouse = any(k in cur and str(cur[k]).lower() != str(v).lower() for k, v in (lh or {}).items() if k in ("name", "fabric_id"))
        other_workspace = bool(ws and self.ctx.fabric_workspace and ws != self.ctx.fabric_workspace.lower())
        if lh:
            # a different lakehouse replaces the previous one entirely (a stale OLCF item id must not stay attached to another lakehouse's name)
            self.ctx.default_lakehouse = dict(lh) if (other_lakehouse or other_workspace) else {**cur, **lh}
        if ws and (not self.ctx.fabric_workspace or other_workspace):
            self.ctx.fabric_workspace = ws

    def analyzer(self) -> PyAnalyzer:
        """The shared Python / SQL analyzer (created lazily so that ``%%configure`` / metadata can set the lakehouse first)."""
        if self.az is None:
            self.az = PyAnalyzer(self.ctx, self.b, self.process)
        return self.az


def _cells_of(nb: dict[str, Any]) -> list[Any] | None:
    cells = nb.get("cells")
    if isinstance(cells, list):
        return cells
    ws = nb.get("worksheets")
    if isinstance(ws, list):
        out: list[Any] = []
        for w in ws:
            if isinstance(w, dict) and isinstance(w.get("cells"), list):
                out.extend(w["cells"])
        return out
    return None


def analyze_notebook(nb: dict[str, Any] | str | bytes, *, context: PyContext | LineageContext | Mapping[str, Any] | None = None) -> Facts:
    """Analyse an nbformat notebook document and return its lineage as :class:`~olcf.intelligence.model.Facts`; never raises.

    The process asset is ``context.process`` when the caller supplies the notebook's own asset, otherwise a ``notebook`` asset (keyed by ``olcf_item`` = ``context.item_id``).
    See the module docstring for the cell languages and magics that are understood.
    """
    ctx = LineageContext.coerce(context)
    run = _NotebookRun(nb if isinstance(nb, dict) else {}, ctx)
    b = run.b
    try:
        if isinstance(nb, (str, bytes)):
            if len(nb) > MAX_JSON_BYTES:
                b.gap("unsupported_construct", "notebook", f"the notebook JSON is larger than {MAX_JSON_BYTES} bytes and was not analysed", "Split the notebook.", ctx.loc(), NOTEBOOK)
                return b.facts()
            try:
                parsed = json.loads(nb)
            except ValueError as exc:
                b.gap("parse_error", "notebook", f"the notebook is not valid JSON ({str(exc)[:120]})", "Re-export the notebook as nbformat 4 JSON.", ctx.loc(), NOTEBOOK)
                return b.facts()
            if isinstance(parsed, dict):
                run.nb = parsed
            nb = parsed
        if not isinstance(nb, dict):
            b.gap("parse_error", "notebook", "the notebook document is not a JSON object", None, ctx.loc(), NOTEBOOK)
            return b.facts()
        cells = _cells_of(nb)
        if cells is None:
            b.gap("parse_error", "notebook", "the notebook document has no 'cells' list", "Provide an nbformat 4 document.", ctx.loc(), NOTEBOOK)
            return b.facts()
        if ctx.process is None:
            b.asset(AssetFact(ref=run.process, name=ctx.item_name or "notebook", attrs={"cells": len(cells), "format": "nbformat"}, workspace_id=ctx.workspace_id))
        lh, ws = _default_lakehouse_from_metadata(nb)
        if lh and not (ctx.default_lakehouse or {}).get("name") and not (ctx.default_lakehouse or {}).get("id"):
            run.set_default_lakehouse(lh, ws)
        elif ws and not ctx.fabric_workspace and not (ctx.default_lakehouse or {}):
            run.ctx.fabric_workspace = ws
        if len(cells) > MAX_CELLS:
            b.gap("unsupported_construct", "notebook", f"the notebook has {len(cells)} cells; only the first {MAX_CELLS} were analysed", "Split the notebook.", ctx.loc(), NOTEBOOK)
            cells = cells[:MAX_CELLS]
        for idx, cell in enumerate(cells):
            if not isinstance(cell, dict) or cell.get("cell_type") not in ("code", None):
                continue
            _run_cell(run, idx, cell)
        if run.az is not None:
            run.az.finish()
    except Exception as exc:  # noqa: BLE001 - analyzers never raise
        b.gap("parse_error", "notebook", f"notebook analysis failed unexpectedly ({type(exc).__name__}: {str(exc)[:160]})", "Report the notebook to the OLCF maintainers.", ctx.loc(), NOTEBOOK)
    return b.facts()


def _run_cell(run: _NotebookRun, idx: int, cell: dict[str, Any]) -> None:
    """Dispatch one code cell on its magic / language."""
    b, ctx = run.b, run.ctx
    src = cell_source(cell)
    if not src.strip():
        return
    lang = cell_language(cell)
    magic, margs, body = split_cell_magic(src)
    loc = ctx.loc(cell=idx)
    if magic is None and lang in _SQL_MAGICS | {"sparksql"}:
        magic, margs, body = "sql", "", src
    elif magic is None and lang in _UNSUPPORTED_LANG:
        magic, margs, body = lang, "", src
    if magic is not None:
        if magic == "configure":
            _configure(run, idx, body, loc)
            return
        if magic in _SQL_MAGICS:
            _sql_cell(run, idx, body, margs, 0 if body is src else 1)
            return
        if magic in _UNSUPPORTED_LANG:
            b.gap("unsupported_construct", f"cell {idx}", f"the cell is written in {_UNSUPPORTED_LANG[magic]}, which the lineage analyzers do not read; its dependencies are unknown",
                  "Port the cell to PySpark or SQL, or document its dependencies manually.", loc, NOTEBOOK)
            return
        if magic in _SKIPPED_MAGICS:
            return
        if magic not in _PY_MAGICS:
            b.gap("unsupported_construct", f"cell {idx}", f"the cell magic %%{magic} is not understood; the cell was not analysed", None, loc, NOTEBOOK)
            return
        # python-flavoured cell magic: keep line numbering by leaving an empty first line
        src = "\n" + body
    code, magics = strip_line_magics(src)
    az = run.analyzer()
    pending: list[_LineMagic] = []
    for m in magics:
        if m.name == "run":
            target = _run_target(m.args)
            if target:
                az.record_notebook_call(target, None, "%run", loc=ctx.loc(cell=idx, line=m.line), text=f"%run {m.args}")
            else:
                b.gap("dynamic_reference", f"%run {m.args}"[:80], "the notebook run with %run is not a plain name", None, ctx.loc(cell=idx, line=m.line), NOTEBOOK)
        elif m.name in _SQL_MAGICS:
            pending.append(m)
        elif m.name in _UNSUPPORTED_LANG or m.name in _PY_MAGICS or m.name in _IGNORED_LINE_MAGICS or m.name == "!" or m.name in _SKIPPED_MAGICS:
            continue
    is_params = "parameters" in cell_tags(cell)
    az.run_cell(code, idx, parameters=is_params)
    for m in pending:
        az.session.run(m.args, process=run.process, loc=ctx.loc(cell=idx, line=m.line), line_key="sql_line")


def _sql_cell(run: _NotebookRun, idx: int, sql: str, args: str, line_offset: int) -> None:
    """``%%sql`` cell: run every statement through the SQL analyzer of the shared session; ``-o VAR`` binds the last result to a Python variable."""
    az = run.analyzer()
    ctx = run.ctx
    results = az.session.run(sql, process=run.process, loc=ctx.loc(cell=idx), line_offset=line_offset)
    m = re.search(r"(?:^|\s)(?:-o|--output)\s+([A-Za-z_]\w*)", args)
    if m:
        rel = next((r.relation for r in reversed(results) if r.relation is not None), None)
        az.env.vars[m.group(1)] = DF(rel if rel is not None else opaque_relation(), "pandas")


def _configure(run: _NotebookRun, idx: int, body: str, loc: dict[str, Any]) -> None:
    """``%%configure`` JSON: only ``defaultLakehouse`` matters for lineage."""
    try:
        cfg = json.loads(body) if body.strip() else {}
    except ValueError:
        run.b.gap("parse_error", f"cell {idx}", "the %%configure cell does not contain valid JSON; the default lakehouse it may set is unknown", "Fix the JSON.", loc, NOTEBOOK)
        return
    if not isinstance(cfg, dict):
        return
    dl = cfg.get("defaultLakehouse")
    if isinstance(dl, dict):
        out: dict[str, Any] = {}
        if isinstance(dl.get("name"), str) and dl["name"]:
            out["name"] = dl["name"]
        if isinstance(dl.get("id"), str) and looks_like_guid(dl["id"]):
            out["fabric_id"] = dl["id"].lower()
        ws = dl.get("workspaceId")
        run.set_default_lakehouse(out or None, ws.lower() if isinstance(ws, str) and looks_like_guid(ws) else None)
        if run.az is not None:
            run.b.gap("unsupported_construct", f"cell {idx}", "%%configure defaultLakehouse after other cells ran: earlier cells were analysed with the previous default lakehouse",
                      "Move %%configure to the first cell.", loc, NOTEBOOK)
    elif dl is not None:
        run.b.gap("parse_error", f"cell {idx}", "defaultLakehouse in %%configure is not an object", None, loc, NOTEBOOK)


__all__ = ["analyze_notebook", "cell_language", "cell_source", "split_cell_magic", "strip_line_magics"]
