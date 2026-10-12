# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The compatibility analyzer: find every Fabric-specific construct in a notebook, classify it, rewrite what can be rewritten, block the rest.

Pipeline (all before anything executes):

1. parse the notebook (``ipynb`` | ``fabric_git_source`` | ``py``) into :class:`olcf.fabric.notebook.Notebook`;
2. inline bundled ``%run`` targets (:mod:`olcf.fabric.analyzer.inline`);
3. scan each code cell - cell magics, IPython line magics, Python (``ast`` with exact positions), SQL (``sqlglot``, Spark dialect);
4. classify each finding through the declarative rule file (:mod:`olcf.fabric.analyzer.rules`);
5. apply the deterministic rewrites (intra-line text edits, so every cell keeps its line numbers);
6. assemble the report: per-class counts, shims required, blockers, rewrites, data references, source map.

Principles: ordinary PySpark is never translated; an unknown ``notebookutils`` API fails closed with cell/line and remediation; analysis
never executes notebook code; a cell that does not parse produces a warning, not a guess.
"""

from __future__ import annotations

import ast
import fnmatch
import json
import re
import shlex
import sys
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

from olcf.fabric import notebook as nbm
from olcf.fabric.analyzer.inline import inline_runs
from olcf.fabric.analyzer.pyfront import LineMagic, cell_magic, split_ipython
from olcf.fabric.analyzer.rules import CLASSES, SEVERITY, Rule, RuleSet, load_ruleset
from olcf.fabric.analyzer.sqlscan import VORDER_RE, SqlResult, analyse_sql
from olcf.runtime_lib.magics import MagicError, configure_to_conf
from olcf.runtime_lib.spark import is_protected
from olcf.version import COMPAT_RULESET_VERSION

ANALYZER_VERSION = "1.0.0"
DEFAULT_FAMILY = "fabric-2.0-compatible"

_GUID = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
_ABFSS = re.compile(r"^abfss?://(?P<ws>[^@/]+)@onelake\.(?:dfs|blob)\.fabric\.microsoft\.com(?:/(?P<path>.*))?$", re.I)
_OTHER_SCHEME = re.compile(r"^(?P<scheme>abfss?|wasbs?|s3a?|s3n|gs|hdfs|adl)://", re.I)
_MOUNT = re.compile(r"^(?:file:/*)?(?P<path>/lakehouse(?:/.*)?)$", re.S)
_MOUNT_LITERAL = re.compile(r"""^(?P<pfx>[rRuUfF]{0,2}(?:'''|\"\"\"|'|"))(?P<file>file:/*)?(?P<mount>/lakehouse/default)(?=[/'"{]|$)""")
_REL = re.compile(r"^(?:Files|Tables)/")
_URL = re.compile(r"^https?://", re.I)
_SAFE_ROOT = re.compile(r"^[A-Za-z0-9_./\-+:@~=, ]+$")
_SHELL_PIP = re.compile(r"^\s*(?:python3?\s+-m\s+)?(?:pip3?|conda|mamba)\b")
_REQ_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*")
_BODY_PY_MAGICS = {"time", "timeit", "capture", "prun", "debug"}
_FABRIC_HOST_SUFFIXES = (".fabric.microsoft.com", "api.powerbi.com", ".analysis.windows.net", ".pbidedicated.windows.net")
_WAREHOUSE_HOST = re.compile(r"\.datawarehouse\.fabric\.microsoft\.com$", re.I)
_PIP_QUIET = {"-q", "--quiet", "-U", "--upgrade", "--no-deps", "--user", "--pre"}
_PIP_BLOCKED_OPTS = {"-r", "--requirement", "-e", "--editable", "-i", "--index-url", "--extra-index-url", "-f", "--find-links", "-c", "--constraint"}
_OS_SHELL = {"os.system", "os.popen", "os.popen2", "os.popen3", "pty.spawn", "commands.getoutput", "commands.getstatusoutput"}
_SUBPROC = {"subprocess.run", "subprocess.Popen", "subprocess.call", "subprocess.check_call", "subprocess.check_output", "subprocess.getoutput",
            "subprocess.getstatusoutput"}
_UDF_NAMES = {"udf", "pandas_udf"}


@dataclass
class AnalysisOptions:
    """Inputs the analyzer cannot derive from the notebook. Defaults are the strict ones (fail closed)."""

    runtime_family: str = DEFAULT_FAMILY
    #: administrator policy: notebook-time ``%pip install`` / pip from code
    allow_pip_install: bool = False
    #: administrator policy: ``!cmd`` / ``%%sh`` / ``os.system`` / ``subprocess(shell=True)``
    allow_shell: bool = False
    #: the runtime's approved Spark configuration globs (``None`` = no restriction)
    approved_conf: list[str] | None = None
    #: default lakehouse as the platform knows it: ``{"kind": "local"|"remote", "root_path": "/abs/path" (local), "name": str}``; ``None`` = none attached
    lakehouse: dict[str, Any] | None = None
    #: notebooks available for ``%run`` inlining: display name -> notebook text (any supported format)
    bundled_notebooks: dict[str, str] = field(default_factory=dict)
    #: display name of the notebook being analysed (used in source maps)
    notebook_name: str | None = None


@dataclass
class _Edit:
    cell: int
    start: int
    end: int
    replacement: str
    rule: str
    line: int
    col: int
    before: str
    after: str


@dataclass
class AnalysisResult:
    """The report plus the rewritten, ``%run``-inlined notebook (what the runner should execute)."""

    report: dict[str, Any]
    notebook: nbm.Notebook

    @property
    def blocked(self) -> bool:
        return bool(self.report["blocked"])

    def source(self, fmt: str = "ipynb") -> str:
        return nbm.serialise(self.notebook, fmt)


def _sev_rank(cls: str) -> int:
    return {"info": 0, "warning": 1, "error": 2}[SEVERITY[cls]]


class _Run:
    """State of one analysis."""

    def __init__(self, nb: nbm.Notebook, opts: AnalysisOptions, rs: RuleSet) -> None:
        self.nb, self.opts, self.rs = nb, opts, rs
        self.findings: list[dict[str, Any]] = []
        self.edits: list[_Edit] = []
        self.paths: dict[tuple[str, str], dict[str, Any]] = {}
        self.tables: dict[tuple[str, str], dict[str, Any]] = {}
        self.imports: set[str] = set()
        self.packages: set[str] = set()
        self.pip_requirements: list[dict[str, Any]] = []
        self.parameters: list[dict[str, Any]] = []
        self.children: list[dict[str, Any]] = []
        self.secrets: list[dict[str, Any]] = []
        self.names: dict[str, str] = {"notebookutils": "notebookutils", "mssparkutils": "notebookutils"}
        self._hosts: set[tuple[int, str]] = set()

    # ------------------------------------------------------------------ emit
    def emit(self, rule_id: str, cell: nbm.Cell, line: int, col: int = 0, construct: str = "", detail: str | None = None, **extra: Any) -> dict[str, Any]:
        r: Rule = self.rs.rule(rule_id)
        f: dict[str, Any] = {"rule": r.id, "class": r.cls, "severity": r.severity, "cell": cell.index, "line": max(1, line), "column": col, "construct": construct,
                             "message": r.message, "remediation": r.remediation, "detail": detail, "shim": r.shim}
        if cell.file_line:
            f["file_line"] = cell.file_line + max(1, line) - 1
        if cell.origin:
            f["origin"] = cell.origin
        f.update(extra)
        self.findings.append(f)
        return f

    def add_path(self, cell: nbm.Cell, line: int, ref: dict[str, Any]) -> None:
        key = (ref["kind"], ref.get("uri") or ref.get("path") or "")
        cur = self.paths.setdefault(key, dict(ref, cells=[]))
        if cell.index not in cur["cells"]:
            cur["cells"].append(cell.index)
        cur.setdefault("first", {"cell": cell.index, "line": line})

    def add_table(self, cell: nbm.Cell, line: int, ref: str, role: str, kind: str = "catalog") -> None:
        parts = ref.strip("`").split(".")
        cur = self.tables.setdefault((kind, ref), {"ref": ref, "kind": kind, "roles": [], "cells": [], "parts": parts})
        if role not in cur["roles"]:
            cur["roles"].append(role)
        if cell.index not in cur["cells"]:
            cur["cells"].append(cell.index)
        cur.setdefault("first", {"cell": cell.index, "line": line})

    # ------------------------------------------------------------------ shared classifiers
    def conf_key(self, cell: nbm.Cell, line: int, col: int, key: str, via: str) -> None:
        if not key.startswith("spark."):
            return
        rule = self.rs.match_conf(key)
        if rule is not None:
            self.emit(rule.id, cell, line, col, key, via)
        elif is_protected(key):
            self.emit("FAB-CONF-PROTECTED", cell, line, col, key, via)
        elif self.opts.approved_conf is not None and not any(fnmatch.fnmatchcase(key, g) for g in self.opts.approved_conf):
            self.emit("FAB-CONF-NOTAPPROVED", cell, line, col, key, via)
        else:
            self.emit("FAB-CONF-OSS", cell, line, col, key, via)

    def pip(self, cell: nbm.Cell, line: int, col: int, requirements: list[str], construct: str, *, from_code: bool) -> None:
        """Classify a pip installation (``%pip install``, ``!pip``, ``pip.main``, ``subprocess pip``)."""
        blocked_opt = next((a for a in requirements if a in _PIP_BLOCKED_OPTS), None)
        reqs = [a for a in requirements if not a.startswith("-")]
        other_opt = next((a for a in requirements if a.startswith("-") and a not in _PIP_QUIET and a not in _PIP_BLOCKED_OPTS), None)
        names = [(_REQ_NAME.match(r).group(0).lower().replace("_", "-") if _REQ_NAME.match(r) else r) for r in reqs]
        for n, r in zip(names, reqs, strict=True):
            self.pip_requirements.append({"name": n, "requirement": r, "cell": cell.index, "line": line})
        if blocked_opt or other_opt:
            self.emit("FAB-PY-PIP" if from_code else "FAB-MAGIC-PIP-BLOCKED", cell, line, col, construct,
                      f"option {blocked_opt or other_opt} is never allowed (the runtime environment is declared, hash-pinned and immutable)")
            return
        if names and all(self._is_baseline_package(n) for n in names):
            self.emit("FAB-MAGIC-PIP-READONLY", cell, line, col, construct,
                      "already part of the runtime baseline; exact version pins are verified when the cell runs")
            return
        if self.opts.allow_pip_install:
            self.emit("FAB-PY-PIP-ALLOWED" if from_code else "FAB-MAGIC-PIP-ALLOWED", cell, line, col, construct, ", ".join(names))
        else:
            self.emit("FAB-PY-PIP" if from_code else "FAB-MAGIC-PIP-BLOCKED", cell, line, col, construct, ", ".join(names) or None)

    def _is_baseline_package(self, dist: str) -> bool:
        mod = {"delta-spark": "delta", "ipython": "IPython", "pyarrow": "pyarrow"}.get(dist, dist.replace("-", "_"))
        return self.rs.match_import(mod) is not None and self.rs.match_import(mod).id == "FAB-IMP-BASELINE"

    def shell(self, cell: nbm.Cell, line: int, col: int, construct: str, code_rule: str, magic_rule: str, detail: str | None = None) -> None:
        if self.opts.allow_shell:
            self.emit("FAB-MAGIC-SHELL-ALLOWED", cell, line, col, construct, detail)
        else:
            self.emit(code_rule if code_rule else magic_rule, cell, line, col, construct, detail)

    def classify_import(self, cell: nbm.Cell, line: int, col: int, module: str, extra: tuple[str, ...] = ()) -> None:
        if not module:
            return
        top = module.split(".")[0]
        cands = [c for c in (module, *extra) if c]
        rules = [(self.rs.match_import(c), c) for c in cands]
        rules = [(r, c) for r, c in rules if r is not None]
        if rules:
            rule, shown = max(rules, key=lambda t: _sev_rank(t[0].cls))
        else:
            rule, shown = None, module
        self.imports.add(top)
        if rule is None:
            if top in sys.stdlib_module_names or top == "__future__":
                return
            self.packages.add(top)
            self.emit("FAB-IMP-THIRDPARTY", cell, line, col, module)
            return
        if rule.id in ("FAB-IMP-FABRIC-PREINSTALLED", "FAB-IMP-SYNAPSEML", "FAB-IMP-COM-MICROSOFT"):
            self.packages.add(top)
        self.emit(rule.id, cell, line, col, shown)


class _PyScanner(ast.NodeVisitor):
    """AST visitor for one Python cell body."""

    def __init__(self, run: _Run, cell: nbm.Cell, body: str, body_off: int, line_off: int) -> None:
        self.run, self.cell, self.rs = run, cell, run.rs
        self.body, self.body_off, self.line_off = body, body_off, line_off
        self.lines = body.split("\n")
        self.starts = [0]
        for ln in self.lines:
            self.starts.append(self.starts[-1] + len(ln) + 1)
        self._udf_cell = False
        self._delta_cell = False
        self._displayhtml_cell = False

    # ------------------------------------------------------------------ positions
    def _line(self, node: ast.AST) -> int:
        return getattr(node, "lineno", 1) + self.line_off

    def _col(self, node: ast.AST) -> int:
        return getattr(node, "col_offset", 0)

    def _off(self, lineno: int, col_bytes: int) -> int:
        line = self.lines[lineno - 1] if 0 < lineno <= len(self.lines) else ""
        return self.body_off + self.starts[lineno - 1] + len(line.encode("utf-8")[:col_bytes].decode("utf-8", "ignore"))

    def _range(self, node: ast.AST) -> tuple[int, int]:
        return (self._off(node.lineno, node.col_offset), self._off(node.end_lineno or node.lineno, node.end_col_offset or 0))

    def emit(self, rule_id: str, node: ast.AST, construct: str, detail: str | None = None, **extra: Any) -> dict[str, Any]:
        return self.run.emit(rule_id, self.cell, self._line(node), self._col(node), construct, detail, **extra)

    # ------------------------------------------------------------------ name resolution
    @staticmethod
    def canon(dotted: str) -> str:
        parts = dotted.split(".")
        if parts[0] == "mssparkutils":
            parts[0] = "notebookutils"
        if parts[0] == "notebookutils" and len(parts) > 1 and parts[1] == "mssparkutils":
            del parts[1]
        return ".".join(parts)

    def chain(self, node: ast.AST) -> list[str] | None:
        parts: list[str] = []
        while isinstance(node, ast.Attribute):
            parts.append(node.attr)
            node = node.value
        if isinstance(node, ast.Name):
            parts.append(node.id)
            return parts[::-1]
        return None

    def resolve(self, node: ast.AST) -> str | None:
        """Fully qualified dotted name of a Name/Attribute chain after import/alias resolution (``None`` if it does not bottom out in a Name)."""
        parts = self.chain(node)
        if parts is None:
            return None
        base = self.run.names.get(parts[0], parts[0])
        return self.canon(".".join([base, *parts[1:]]))

    def _is_nbu(self, dotted: str | None) -> bool:
        return bool(dotted) and (dotted == "notebookutils" or dotted.startswith("notebookutils."))

    # ------------------------------------------------------------------ statements
    def visit_Import(self, node: ast.Import) -> None:
        for a in node.names:
            mod = a.name
            if a.asname:
                self.run.names[a.asname] = self.canon(mod)
            else:
                top = mod.split(".")[0]
                self.run.names[top] = self.canon(top)
            self.run.classify_import(self.cell, self._line(node), self._col(node), mod)
            if self._is_nbu(self.canon(mod)):
                self._classify_nbu(self.canon(mod), node, mod)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.level:
            return
        mod = node.module or ""
        for a in node.names:
            if a.name == "*":
                self.run.classify_import(self.cell, self._line(node), self._col(node), mod)
                if self._is_nbu(self.canon(mod)):
                    self.emit("FAB-NBU-DYNAMIC", node, f"from {mod} import *", "star import of notebookutils")
                continue
            full = self.canon(f"{mod}.{a.name}")
            self.run.names[a.asname or a.name] = full
            self.run.classify_import(self.cell, self._line(node), self._col(node), mod, (f"{mod}.{a.name}",))
            if self._is_nbu(full):
                self._classify_nbu(full, node, f"from {mod} import {a.name}")

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        for d in node.decorator_list:
            if not isinstance(d, ast.Call):
                name = self.resolve(d) or ""
                if name.split(".")[-1] in _UDF_NAMES:
                    self._udf(d)
        self.generic_visit(node)

    visit_AsyncFunctionDef = visit_FunctionDef  # type: ignore[assignment]

    def visit_Assign(self, node: ast.Assign) -> None:
        self.visit(node.value)
        alias: str | None = None
        if isinstance(node.value, (ast.Name, ast.Attribute)):
            parts = self.chain(node.value)
            if parts and parts[0] in self.run.names:
                alias = self.resolve(node.value)
        for t in node.targets:
            if isinstance(t, ast.Name):
                if alias is not None:
                    self.run.names[t.id] = alias
                else:
                    self.run.names.pop(t.id, None)  # shadowed by an unrelated value
            else:
                self.visit(t)
        if self.cell.is_parameters:
            self._param(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if node.value is not None:
            self.visit(node.value)
        if self.cell.is_parameters:
            self._param(node)

    def _param(self, node: ast.stmt) -> None:
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]  # type: ignore[union-attr]
        value = node.value  # type: ignore[union-attr]
        if value is None or len(targets) != 1 or not isinstance(targets[0], ast.Name):
            return
        try:
            default = ast.literal_eval(value)
        except (ValueError, SyntaxError, TypeError, MemoryError, RecursionError):
            default = None
            literal = False
        else:
            literal = True
        self.run.parameters.append({"name": targets[0].id, "default": default if literal else None, "literal": literal, "type": type(default).__name__ if literal else None,
                                    "cell": self.cell.index, "line": self._line(node)})

    # ------------------------------------------------------------------ expressions
    def visit_Name(self, node: ast.Name) -> None:
        if not isinstance(node.ctx, ast.Load):
            return
        canon = self.run.names.get(node.id)
        if not self._is_nbu(canon):
            return
        parent = getattr(node, "_p", None)
        if isinstance(parent, ast.Attribute) and parent.value is node:
            return
        if isinstance(parent, ast.Assign) and parent.value is node:
            return
        if isinstance(parent, ast.Call) and parent.func is node:
            self._classify_nbu(canon, node, node.id)  # type: ignore[arg-type]
            return
        if isinstance(parent, (ast.Import, ast.ImportFrom)):
            return
        if isinstance(parent, ast.Call) and isinstance(parent.func, ast.Name) and parent.func.id == "getattr" and parent.args and parent.args[0] is node:
            return  # classified by visit_Call (constant attribute) or reported there as dynamic
        self.emit("FAB-NBU-DYNAMIC", node, node.id, f"'{node.id}' ({canon}) is used as a value")

    def visit_Attribute(self, node: ast.Attribute) -> None:
        rule = self.rs.match_api(node.attr)
        if rule is not None:
            self.emit(rule.id, node, f".{node.attr}")
        parent = getattr(node, "_p", None)
        outermost = not (isinstance(parent, ast.Attribute) and parent.value is node)
        if outermost:
            parts = self.chain(node)
            if parts is not None:
                dotted = self.resolve(node) or ""
                if self._is_nbu(dotted):
                    self._classify_nbu(dotted, node, ".".join(parts))
                elif "_jvm" in parts:
                    rest = ".".join(parts[parts.index("_jvm") + 1:])
                    if rest.startswith("com.microsoft"):
                        rule = self.rs.match_import(rest) or self.rs.rule("FAB-IMP-COM-MICROSOFT")
                        self.emit(rule.id, node, rest, "accessed through py4j")
                if parts[0] == "DeltaTable" or dotted.startswith("delta.tables"):
                    self._delta(node)
        self.generic_visit(node)

    def _classify_nbu(self, dotted: str, node: ast.AST, shown: str) -> None:
        kind, rule = self.rs.match_call(dotted)
        if kind == "container" or rule is None:
            return
        self.emit(rule.id, node, dotted, f"{dotted} is not part of the OLCF shim surface" if kind == "unknown" else None)

    def _udf(self, node: ast.AST) -> None:
        if not self._udf_cell:
            self._udf_cell = True
            self.emit("FAB-PY-UDF", node, "udf")

    def _delta(self, node: ast.AST) -> None:
        if not self._delta_cell:
            self._delta_cell = True
            self.emit("FAB-PY-DELTA", node, "DeltaTable")

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        full = self.resolve(func) if isinstance(func, (ast.Name, ast.Attribute)) else None
        attr = func.attr if isinstance(func, ast.Attribute) else (func.id if isinstance(func, ast.Name) else "")
        first = node.args[0] if node.args else None
        first_str = first.value if isinstance(first, ast.Constant) and isinstance(first.value, str) else None

        if full:
            self._call_named(node, full, first, first_str)
        if attr in ("set", "config") and first_str and first_str.startswith("spark.") and isinstance(func, ast.Attribute):
            self.run.conf_key(self.cell, self._line(node), self._col(node), first_str, f".{attr}()")
        elif attr == "setAll" and isinstance(first, (ast.List, ast.Tuple)):
            for el in first.elts:
                if isinstance(el, (ast.Tuple, ast.List)) and el.elts and isinstance(el.elts[0], ast.Constant) and isinstance(el.elts[0].value, str):
                    self.run.conf_key(self.cell, self._line(el), self._col(el), el.elts[0].value, ".setAll()")
        if attr == "sql" and first_str is not None and isinstance(func, ast.Attribute):
            self._sql_literal(first, first_str)
        if attr in ("table", "saveAsTable", "insertInto") and first_str and isinstance(func, ast.Attribute):
            self.run.add_table(self.cell, self._line(node), first_str, "read" if attr == "table" else "write")
        if attr in _UDF_NAMES or (isinstance(func, ast.Attribute) and attr == "register" and (self.resolve(func.value) or "").endswith("udf")):
            self._udf(node)
        if attr == "displayHTML" and isinstance(func, ast.Name) and not self._displayhtml_cell:
            self._displayhtml_cell = True
            self.emit("FAB-PY-DISPLAYHTML", node, "displayHTML")
        self.generic_visit(node)

    def _call_named(self, node: ast.Call, full: str, first: ast.expr | None, first_str: str | None) -> None:
        if full in _OS_SHELL or full.startswith(("os.exec", "os.spawn", "os.posix_spawn")):
            self.run.shell(self.cell, self._line(node), self._col(node), full, "FAB-PY-SHELL", "FAB-PY-SHELL")
            return
        if full in _SUBPROC or full.startswith("subprocess."):
            consts = _const_strings(first)
            if "pip" in consts or any(c.startswith(("pip ", "pip3 ")) or " pip " in f" {c} " and "install" in c for c in consts):
                self.run.pip(self.cell, self._line(node), self._col(node), _pip_args_from_consts(consts), full, from_code=True)
                return
            shell_kw = next((k for k in node.keywords if k.arg == "shell"), None)
            if full in ("subprocess.getoutput", "subprocess.getstatusoutput") or (shell_kw is not None and not (isinstance(shell_kw.value, ast.Constant) and shell_kw.value.value is False)):
                self.run.shell(self.cell, self._line(node), self._col(node), full, "FAB-PY-SHELL", "FAB-PY-SHELL", "shell=True")
            else:
                self.emit("FAB-PY-SUBPROCESS", node, full)
            return
        if full in ("pip.main", "pip._internal.main", "pip._internal.cli.main.main"):
            consts = _const_strings(first)
            self.run.pip(self.cell, self._line(node), self._col(node), _pip_args_from_consts(consts, strip_install=True), full, from_code=True)
            return
        if full in ("exec", "eval", "compile") and not isinstance(first, ast.Constant):
            self.emit("FAB-PY-DYNAMIC-EXEC", node, full)
            return
        if full in ("importlib.import_module", "__import__"):
            if first_str is not None:
                self.run.classify_import(self.cell, self._line(node), self._col(node), first_str)
            else:
                self.emit("FAB-PY-DYNAMIC-EXEC", node, full, "module name is computed")
            return
        if full == "getattr" and first is not None and len(node.args) >= 2:
            base = self.resolve(first) if isinstance(first, (ast.Name, ast.Attribute)) else None
            if self._is_nbu(base):
                name = node.args[1]
                if isinstance(name, ast.Constant) and isinstance(name.value, str):
                    self._classify_nbu(f"{base}.{name.value}", node, f"getattr({base}, {name.value!r})")
                else:
                    self.emit("FAB-NBU-DYNAMIC", node, "getattr(notebookutils, ...)", "attribute name is computed")
            return
        if self._is_nbu(full):
            self._nbu_call_facts(node, full)

    def _nbu_call_facts(self, node: ast.Call, full: str) -> None:
        args = node.args
        if full == "notebookutils.notebook.run" and args:
            name = args[0].value if isinstance(args[0], ast.Constant) and isinstance(args[0].value, str) else None
            self.run.children.append({"name": name, "cell": self.cell.index, "line": self._line(node), "dynamic": name is None})
        elif full == "notebookutils.notebook.runMultiple" and args:
            try:
                dag = ast.literal_eval(args[0])
            except (ValueError, SyntaxError, TypeError, MemoryError, RecursionError):
                dag = None
            acts = dag.get("activities") if isinstance(dag, dict) else (dag if isinstance(dag, list) else None)
            if isinstance(acts, list):
                for a in acts:
                    if isinstance(a, dict) and isinstance(a.get("path"), str):
                        self.run.children.append({"name": a["path"], "cell": self.cell.index, "line": self._line(node), "dynamic": False})
            else:
                self.run.children.append({"name": None, "cell": self.cell.index, "line": self._line(node), "dynamic": True})
        elif full == "notebookutils.credentials.getSecret":
            vals = [a.value if isinstance(a, ast.Constant) and isinstance(a.value, str) else None for a in args[:2]]
            kw = {k.arg: k.value.value for k in node.keywords if isinstance(k.value, ast.Constant) and isinstance(k.value.value, str)}
            vault = vals[0] if vals else kw.get("akvName")
            name = vals[1] if len(vals) > 1 else kw.get("secret")
            self.run.secrets.append({"vault": vault, "name": name, "cell": self.cell.index, "line": self._line(node), "dynamic": vault is None or name is None})

    # ------------------------------------------------------------------ SQL inside Python
    def _sql_literal(self, node: ast.expr, value: str) -> None:
        res = analyse_sql(value, base_line=self._line(node))
        start, end = self._range(node)
        seg = self.cell.source[start:end]
        seg_hits = [m for m in VORDER_RE.finditer(seg)]
        can_rewrite = len(seg_hits) == len(res.edits) and all(seg[m.start(2):m.end(2)].upper() == "VORDER" for m in seg_hits)
        applied = 0
        if can_rewrite:
            for m in seg_hits:
                s = start + m.start(2)
                self.run.edits.append(_Edit(self.cell.index, s, start + m.end(2), "", "FAB-SQL-VORDER", self._line(node), self._col(node), "VORDER", ""))
                applied += 1
        for f in res.findings:
            if f.rule == "FAB-SQL-VORDER":
                if can_rewrite:
                    self.run.emit("FAB-SQL-VORDER", self.cell, f.line, 0, f.construct, "removed", rewritten=True)
                else:
                    self.run.emit("FAB-SQL-FABRIC-FN", self.cell, f.line, 0, f.construct,
                                  "the VORDER clause sits in a computed or concatenated SQL string and could not be removed deterministically; remove it from the notebook")
            else:
                self.run.emit(f.rule, self.cell, f.line, 0, f.construct, f.detail or None)
        _sql_facts(self.run, self.cell, res, via="spark.sql()", line=self._line(node))

    # ------------------------------------------------------------------ literals
    def visit_Constant(self, node: ast.Constant) -> None:
        if not isinstance(node.value, str):
            return
        parent = getattr(node, "_p", None)
        if isinstance(parent, ast.Expr):
            return  # docstring / bare string
        self._string(node, node.value, template=False)

    def visit_JoinedStr(self, node: ast.JoinedStr) -> None:
        pieces = [v.value if isinstance(v, ast.Constant) and isinstance(v.value, str) else "{}" for v in node.values]
        if any(isinstance(v, ast.Constant) for v in node.values):
            self._string(node, "".join(pieces), template=True)
        for v in node.values:
            if isinstance(v, ast.FormattedValue):
                self.visit(v)

    def _string(self, node: ast.expr, value: str, *, template: bool) -> None:
        if len(value) > 4096 or not value:
            return
        line, col = self._line(node), self._col(node)
        m = _MOUNT.match(value)
        if m:
            self._mount(node, value, template)
            return
        if "/lakehouse/" in value and not value.startswith(("http", "abfs")):
            self.emit("FAB-PATH-MOUNT-DYNAMIC", node, value[:120], "the Fabric mount path is embedded in a longer string")
        a = _ABFSS.match(value)
        if a:
            self._onelake_url(node, value, a, line)
            return
        o = _OTHER_SCHEME.match(value)
        if o:
            self.run.add_path(self.cell, line, {"kind": o.group("scheme").lower(), "uri": value})
            return
        if _REL.match(value):
            area, _, rest = value.partition("/")
            self.emit("FAB-PATH-REL", node, value[:120])
            self.run.add_path(self.cell, line, {"kind": "relative", "area": area, "path": rest, "uri": value})
            return
        if _URL.match(value):
            self._url(node, value, line, col)
            return
        if value.startswith("com.microsoft.kusto") or value.startswith("com.microsoft.azure.kusto"):
            self.emit("FAB-PY-KUSTO", node, value[:120])
        elif value.startswith("com.microsoft.") and not template:
            rule = self.rs.match_import(value) or self.rs.rule("FAB-IMP-COM-MICROSOFT")
            if rule.id != "FAB-IMP-NBU":
                self.emit(rule.id, node, value[:120], "referenced as a string (data source / class name)")
        elif value.startswith("delta.`") or value.startswith("parquet.`"):
            self.run.add_table(self.cell, line, value.split("`")[1] if "`" in value else value, "read", "path")

    def _mount(self, node: ast.expr, value: str, template: bool) -> None:
        run, cell = self.run, self.cell
        path = _MOUNT.match(value).group("path")  # type: ignore[union-attr]
        is_default = path == "/lakehouse/default" or path.startswith("/lakehouse/default/")
        line, col = self._line(node), self._col(node)
        if not is_default:
            self.emit("FAB-PATH-MOUNT-DYNAMIC", node, value[:120], "only /lakehouse/default is a recognised mount path")
            return
        lh = run.opts.lakehouse
        rel = path[len("/lakehouse/default"):].lstrip("/")
        run.add_path(cell, line, {"kind": "mount", "path": rel, "uri": path, "area": rel.split("/")[0] if rel else None})
        if not lh:
            self.emit("FAB-PATH-MOUNT-NOLAKEHOUSE", node, value[:120])
            return
        if lh.get("kind") != "local" or not lh.get("root_path"):
            self.emit("FAB-PATH-MOUNT-REMOTE", node, value[:120], f"default lakehouse '{lh.get('name') or ''}' is not on a local filesystem")
            return
        root = str(lh["root_path"]).rstrip("/")
        start, end = self._range(node)
        seg = cell.source[start:end]
        m = _MOUNT_LITERAL.match(seg)
        if m is None or not _SAFE_ROOT.match(root) or (m.group("pfx")[-1] in root):
            self.emit("FAB-PATH-MOUNT-DYNAMIC", node, value[:120], "the path literal could not be rewritten in place (computed, concatenated or unusual quoting)")
            return
        s = start + m.start("mount")
        e = start + m.end("mount")
        run.edits.append(_Edit(cell.index, s, e, root, "FAB-PATH-MOUNT-LOCAL", line, col, "/lakehouse/default", root))
        self.emit("FAB-PATH-MOUNT-LOCAL", node, value[:120], f"/lakehouse/default -> {root}", rewritten=True)

    def _onelake_url(self, node: ast.expr, value: str, m: re.Match[str], line: int) -> None:
        ws = m.group("ws")
        path = (m.group("path") or "").strip("/")
        item, _, rest = path.partition("/")
        item_name, _, item_type = item.rpartition(".") if "." in item else (item, "", "")
        area, _, sub = rest.partition("/")
        ref = {"kind": "onelake", "workspace": ws, "workspace_is_id": bool(_GUID.match(ws)), "item": item_name or item, "item_is_id": bool(_GUID.match(item)),
               "item_type": item_type or None, "area": area if area in ("Files", "Tables") else (area or None), "path": sub if area in ("Files", "Tables") else rest, "uri": value}
        self.emit("FAB-PATH-ABFSS", node, value[:160])
        self.run.add_path(self.cell, line, ref)
        if (item_type or "").lower() in ("warehouse", "sqlendpoint", "kqldatabase", "eventhouse"):
            self.emit("FAB-DATA-WAREHOUSE-REF", node, value[:160], f"{item_type} item")

    def _url(self, node: ast.expr, value: str, line: int, col: int) -> None:
        try:
            host = (urlsplit(value).hostname or "").lower()
        except ValueError:
            return
        if (self.cell.index, host) in self.run._hosts:
            return
        self.run._hosts.add((self.cell.index, host))
        self.run.add_path(self.cell, line, {"kind": "http", "uri": value.split("?")[0][:300], "host": host})
        if host.endswith(_FABRIC_HOST_SUFFIXES) or host in ("api.powerbi.com", "login.microsoftonline.com") or host.endswith(".analysis.windows.net"):
            self.emit("FAB-PY-ONELAKE-API", node, host)
            if _WAREHOUSE_HOST.search(host):
                self.emit("FAB-DATA-WAREHOUSE-REF", node, host, "SQL endpoint")
        elif host not in ("localhost", "127.0.0.1", "::1", ""):
            self.emit("FAB-PY-NETWORK", node, host)


def _const_strings(node: ast.expr | None) -> list[str]:
    if node is None:
        return []
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]
    if isinstance(node, (ast.List, ast.Tuple)):
        return [e.value for e in node.elts if isinstance(e, ast.Constant) and isinstance(e.value, str)]
    return []


def _pip_args_from_consts(consts: list[str], *, strip_install: bool = False) -> list[str]:
    """Requirements/options of a pip invocation given as constant strings (``["-m","pip","install","x"]`` or ``"pip install x"``)."""
    toks: list[str] = []
    for c in consts:
        try:
            toks.extend(shlex.split(c))
        except ValueError:
            toks.extend(c.split())
    if "install" in toks:
        toks = toks[toks.index("install") + 1:]
    else:
        toks = [t for t in toks if t not in ("-m", "pip", "pip3", "python", "python3", "uninstall")]
    return [t for t in toks if t not in ("-m", "pip", "pip3")]


def _sql_facts(run: _Run, cell: nbm.Cell, res: SqlResult, *, via: str, line: int = 1) -> None:
    for t in res.tables:
        run.add_table(cell, t.line, t.ref, t.role, t.kind)
    for key, line in res.configs:
        run.conf_key(cell, line, 0, key, f"SET ({via})")
    if res.delta_statements:
        run.emit("FAB-SQL-DELTA", cell, min([t.line for t in res.tables] or [line]), 0, f"{res.delta_statements} Delta statement(s)")


# ================================================================================================= cell dispatch
def _scan_cell(run: _Run, cell: nbm.Cell) -> None:
    if cell.cell_type != "code":
        return
    if cell.magic_prefixed:
        run.emit("FAB-FMT-MAGIC-NORMALISED", cell, 1, 0, "# MAGIC", rewritten=True)
    src = cell.source
    if not src.strip():
        return
    cm = cell_magic(src)
    if cm is not None:
        name, args, nlines = cm
        _cell_magic(run, cell, name.lower(), args, nlines)
        return
    if cell.language == "sql":
        _sql_cell(run, cell, src, 0, 0, "sql")
        return
    if cell.language in ("scala", "r", "csharp"):
        run.emit("FAB-LANG-CELL", cell, 1, 0, cell.language, f"cell language '{cell.language}'")
        return
    _python_body(run, cell, src, 0, 0)


def _body_of(cell: nbm.Cell, nlines: int) -> tuple[str, int]:
    lines = cell.source.split("\n")
    return "\n".join(lines[nlines:]), sum(len(ln) + 1 for ln in lines[:nlines])


def _cell_magic(run: _Run, cell: nbm.Cell, name: str, args: str, nlines: int) -> None:
    rs = run.rs
    body, off = _body_of(cell, nlines)
    construct = f"%%{name}"
    rule = rs.match_magic("cell", name)
    if name == "sql":
        run.emit("FAB-MAGIC-SQL", cell, nlines, 0, construct)
        _sql_cell(run, cell, body, off, nlines, "%%sql")
    elif name in ("pyspark", "python"):
        run.emit("FAB-MAGIC-SQL", cell, nlines, 0, construct)
        _python_body(run, cell, body, off, nlines)
    elif name == "configure":
        _configure(run, cell, args, body, nlines)
    elif rule is not None and rule.id == "FAB-MAGIC-SHELL":
        run.shell(cell, nlines, 0, construct, "FAB-MAGIC-SHELL", "FAB-MAGIC-SHELL")
    elif rule is not None and rule.id == "FAB-MAGIC-LANG":
        run.emit("FAB-MAGIC-LANG", cell, nlines, 0, construct)
    elif rule is not None:
        run.emit(rule.id, cell, nlines, 0, construct)
        if name in _BODY_PY_MAGICS:
            _python_body(run, cell, body, off, nlines)
    else:
        run.emit("FAB-MAGIC-UNKNOWN", cell, nlines, 0, construct)


def _sql_cell(run: _Run, cell: nbm.Cell, body: str, off: int, line_off: int, via: str) -> None:
    res = analyse_sql(body, base_line=line_off + 1)
    for f in res.findings:
        if f.rule == "FAB-SQL-VORDER":
            run.emit("FAB-SQL-VORDER", cell, f.line, 0, f.construct, "VORDER removed", rewritten=True)
        else:
            run.emit(f.rule, cell, f.line, 0, f.construct, f.detail or None)
    for ed in res.edits:
        run.edits.append(_Edit(cell.index, off + ed.start, off + ed.end, ed.replacement, ed.rule, line_off + 1 + body.count("\n", 0, ed.start), 0, ed.before, ed.after))
    _sql_facts(run, cell, res, via=via, line=line_off + 1)


def _configure(run: _Run, cell: nbm.Cell, args: str, body: str, line: int) -> None:
    text = body.strip()
    try:
        cfg = json.loads(text) if text else {}
    except ValueError as exc:
        run.emit("FAB-MAGIC-CONFIGURE-JSON", cell, line, 0, "%%configure", str(exc)[:200])
        return
    if not isinstance(cfg, dict):
        run.emit("FAB-MAGIC-CONFIGURE-JSON", cell, line, 0, "%%configure", "the document is not a JSON object")
        return
    try:
        conf, extras = configure_to_conf(cfg, run.opts.approved_conf)
    except MagicError as exc:
        run.emit("FAB-MAGIC-CONFIGURE-BAD", cell, line, 0, "%%configure", str(exc)[:300])
        return
    run.emit("FAB-MAGIC-CONFIGURE", cell, line, 0, "%%configure")
    for k in sorted(conf):
        run.conf_key(cell, line, 0, k, "%%configure")
    dl = extras.get("defaultLakehouse")
    if isinstance(dl, dict):
        run.add_path(cell, line, {"kind": "default-lakehouse", "uri": f"configure:{dl.get('name') or dl.get('id')}", "item": dl.get("name") or dl.get("id"),
                                  "workspace": dl.get("workspaceId")})


def _python_body(run: _Run, cell: nbm.Cell, body: str, off: int, line_off: int) -> None:
    py, magics = split_ipython(body)
    for lm in magics:
        _line_magic(run, cell, lm, line_off)
    try:
        tree = ast.parse(py)
    except SyntaxError as exc:
        run.emit("FAB-PY-SYNTAX", cell, (exc.lineno or 1) + line_off, exc.offset or 0, "syntax error", f"{exc.msg}")
        return
    except (ValueError, RecursionError, MemoryError) as exc:
        run.emit("FAB-PY-SYNTAX", cell, 1 + line_off, 0, "unparseable", str(exc)[:200])
        return
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            child._p = parent  # type: ignore[attr-defined]
    _PyScanner(run, cell, body, off, line_off).visit(tree)


def _line_magic(run: _Run, cell: nbm.Cell, lm: LineMagic, line_off: int) -> None:
    line = lm.line + line_off
    if lm.kind == "shell":
        if _SHELL_PIP.match(lm.args):
            toks = _safe_split(lm.args)
            sub = next((t for t in toks if t in ("install", "uninstall", "list", "freeze", "show", "remove", "update", "upgrade")), "")
            if sub in ("list", "freeze", "show"):
                run.emit("FAB-MAGIC-PIP-READONLY", cell, line, lm.col, f"!{lm.args[:60]}")
            else:
                run.pip(cell, line, lm.col, toks[toks.index(sub) + 1:] if sub in toks else [], f"!{lm.args[:60]}", from_code=False)
            return
        run.shell(cell, line, lm.col, f"!{lm.args[:80]}", "FAB-MAGIC-SHELL", "FAB-MAGIC-SHELL")
        return
    name = lm.name.lower()
    if name in ("pip", "conda", "mamba"):
        toks = _safe_split(lm.args)
        sub = toks[0] if toks else ""
        if name != "pip":
            run.emit("FAB-MAGIC-PIP-BLOCKED", cell, line, lm.col, f"%{name} {lm.args[:60]}", "only %pip is supported; conda environments are not available")
        elif sub in ("list", "freeze", "show"):
            run.emit("FAB-MAGIC-PIP-READONLY", cell, line, lm.col, f"%pip {sub}")
        elif sub == "install":
            run.pip(cell, line, lm.col, toks[1:], f"%pip install {' '.join(toks[1:])[:80]}", from_code=False)
        else:
            run.emit("FAB-MAGIC-PIP-BLOCKED", cell, line, lm.col, f"%pip {sub}", f"%pip {sub} is not supported: the runtime environment is immutable")
        return
    if name == "run":
        run.emit("FAB-MAGIC-RUN", cell, line, lm.col, f"%run {lm.args[:80]}")
        return
    rule = run.rs.match_magic("line", name)
    if rule is None:
        run.emit("FAB-MAGIC-UNKNOWN", cell, line, lm.col, f"%{name}")
    elif rule.id == "FAB-MAGIC-SHELL":
        run.shell(cell, line, lm.col, f"%{name}", "FAB-MAGIC-SHELL", "FAB-MAGIC-SHELL")
    else:
        run.emit(rule.id, cell, line, lm.col, f"%{name}")


def _safe_split(s: str) -> list[str]:
    try:
        return shlex.split(s)
    except ValueError:
        return s.split()


# ================================================================================================= edits
def _apply_edits(nb: nbm.Notebook, edits: list[_Edit]) -> list[dict[str, Any]]:
    applied: list[dict[str, Any]] = []
    by_cell: dict[int, list[_Edit]] = {}
    for e in edits:
        by_cell.setdefault(e.cell, []).append(e)
    for idx, es in by_cell.items():
        cell = nb.cells[idx]
        es.sort(key=lambda e: (e.start, e.end))
        keep: list[_Edit] = []
        for e in es:
            if keep and e.start < keep[-1].end:
                continue  # overlapping edits cannot both apply; the first wins (deterministic)
            keep.append(e)
        text = cell.source
        before_lines = text.count("\n")
        for e in reversed(keep):
            text = text[:e.start] + e.replacement + text[e.end:]
        if text.count("\n") != before_lines:  # pragma: no cover - defensive: edits are intra-line by construction
            raise AssertionError("analyzer rewrite changed the number of lines")
        cell.source = text
        for e in keep:
            applied.append({"rule": e.rule, "cell": idx, "line": e.line, "column": e.col, "before": e.before, "after": e.after, "origin": cell.origin})
    return applied


# ================================================================================================= entry points
def analyze_text(text: str, *, format: str | None = None, options: AnalysisOptions | None = None) -> AnalysisResult:  # noqa: A002
    """Analyse ``text``; returns the report and the rewritten notebook. Raises ``BadRequest`` only when the document is not a notebook at all."""
    t0 = time.perf_counter()
    opts = options or AnalysisOptions()
    rs = load_ruleset()
    nb = nbm.parse(text, format)
    fmt = nb.format
    n_cells_original = len(nb.cells)
    events: list[dict[str, Any]] = []
    if opts.bundled_notebooks or "%run" in text:
        events = inline_runs(nb, opts.bundled_notebooks, main_name=opts.notebook_name)
    run = _Run(nb, opts, rs)
    for cell in nb.cells:
        _scan_cell(run, cell)
    for ev in events:
        if ev["status"] == "inlined":
            run.emit("FAB-MAGIC-RUN-INLINED", nbm.Cell("code", "", index=int(ev["cell"])), int(ev["line"]), 0, f"%run {ev['target']}", f"{ev['cells']} cell(s) inlined")
    rewrites = _apply_edits(nb, run.edits)
    report = _report(run, nb, text, fmt, events, rewrites, n_cells_original, (time.perf_counter() - t0) * 1000)
    return AnalysisResult(report, nb)


def _report(run: _Run, nb: nbm.Notebook, text: str, fmt: str, events: list[dict[str, Any]], rewrites: list[dict[str, Any]], n_orig: int, ms: float) -> dict[str, Any]:
    findings = sorted(run.findings, key=lambda f: (f["cell"], f["line"], f["column"], f["rule"]))
    counts = {c: 0 for c in CLASSES}
    rule_counts: dict[str, int] = {}
    for f in findings:
        counts[f["class"]] += 1
        rule_counts[f["rule"]] = rule_counts.get(f["rule"], 0) + 1
    # a %run that could not be inlined is reported by the line magic scan; add the reason from the inliner to that finding
    for ev in events:
        if ev["status"] in ("missing", "recursive", "too_deep"):
            for f in findings:
                if f["rule"] == "FAB-MAGIC-RUN" and ev["target"] and ev["target"] in (f.get("construct") or ""):
                    f["detail"] = {"missing": "the referenced notebook was not bundled", "recursive": "recursive %run", "too_deep": "%run nesting too deep"}[ev["status"]]
    blockers = [{k: f[k] for k in ("rule", "cell", "line", "column", "construct", "message", "remediation", "detail") if k in f} for f in findings if f["class"] == "unsupported"]
    inlined = any(e["status"] == "inlined" for e in events)
    source_map: dict[str, Any] = {"identity": not inlined, "line_preserving_rewrites": True}
    if inlined:
        source_map["cells"] = [{"cell": c.index, "origin": c.origin} for c in nb.cells]
    cells_total = len(nb.cells)
    code = [c for c in nb.cells if c.cell_type == "code"]
    shims = sorted({f["shim"] for f in findings if f.get("shim")})
    summary = (f"{len(findings)} construct(s): " + ", ".join(f"{n} {c}" for c, n in counts.items() if n) if findings else "no Fabric-specific constructs found")
    summary += f"; {len(blockers)} blocker(s)" if blockers else ""
    paths = sorted(run.paths.values(), key=lambda p: (p["kind"], p.get("uri") or ""))
    tables = sorted(run.tables.values(), key=lambda t: (t["kind"], t["ref"]))
    for p in paths:
        p.pop("first", None)
    for t in tables:
        t.pop("first", None)
    lakehouse_hint = nb.default_lakehouse_hint()
    return {
        "analyzer": {"name": "olcf-fabric-analyzer", "version": ANALYZER_VERSION, "ruleset_version": run.rs.version},
        "analyzer_version": ANALYZER_VERSION,
        "ruleset_version": COMPAT_RULESET_VERSION,
        "runtime_family": run.opts.runtime_family,
        "source": {"format": fmt, "sha256": nbm.sha256_text(text), "bytes": len(text.encode("utf-8"))},
        "cells": {"total": cells_total, "original": n_orig, "code": len(code), "markdown": sum(1 for c in nb.cells if c.cell_type == "markdown"),
                  "sql": sum(1 for c in code if c.language == "sql" or c.source.lstrip().lower().startswith("%%sql"))},
        "policy": {"allow_pip_install": run.opts.allow_pip_install, "allow_shell": run.opts.allow_shell,
                   "default_lakehouse": (run.opts.lakehouse or {}).get("kind")},
        "counts": counts,
        "rule_counts": dict(sorted(rule_counts.items())),
        "findings": findings,
        "blockers": blockers,
        "blocked": bool(blockers),
        "shims_required": shims,
        "rewrites": rewrites,
        "source_map": source_map,
        "inlined_runs": events,
        "data_references": {"paths": paths, "tables": tables},
        "imports": sorted(run.imports),
        "packages": sorted(run.packages),
        "pip_requirements": run.pip_requirements,
        "parameters": run.parameters,
        "child_notebooks": run.children,
        "secret_refs": run.secrets,
        "notebook_default_lakehouse": lakehouse_hint,
        "summary": summary,
        "duration_ms": round(ms, 2),
    }


def analyze(text: str, *, format: str | None = None, runtime_family: str | None = None, origin: dict[str, Any] | None = None,  # noqa: A002, ARG001
            options: AnalysisOptions | None = None) -> dict[str, Any]:
    """Report-only entry point with the signature ``JobService`` calls (``origin`` is accepted for the adapter and ignored by the analysis itself)."""
    opts = options or AnalysisOptions()
    if runtime_family and opts.runtime_family != runtime_family:
        opts = AnalysisOptions(**{**opts.__dict__, "runtime_family": runtime_family})
    return analyze_text(text, format=format, options=opts).report
