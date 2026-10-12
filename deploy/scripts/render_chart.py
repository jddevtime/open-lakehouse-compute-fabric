#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Render and check the OLCF Helm chart WITHOUT helm.

This is NOT a replacement for ``helm template``/``helm lint``.  It exists because helm is not available in every build environment.  It implements the small, well-defined subset of Go templates and Sprig functions that the chart uses, renders every
template, parses the result as YAML, validates the values against ``values.schema.json`` and optionally runs ``kubeconform`` over the manifests.  An
unsupported construct is a hard error (never silently skipped), so a template that outgrows the subset is noticed immediately.  CI additionally runs the real
``helm lint`` and ``helm template`` (see .github/workflows/container.yml).

usage:
    render_chart.py [CHART_DIR] [-f values.yaml]... [--set a.b=c]... [--release NAME] [--namespace NS] [--out DIR] [--kubeconform] [--quiet]
exit status: 0 = rendered and valid, 1 = template/YAML/schema/kubeconform failure, 2 = usage error
"""

from __future__ import annotations

import argparse
import base64
import copy
import hashlib
import json
import os
import re
import secrets
import shutil
import string
import subprocess  # noqa: S404 - only used to run kubeconform, never with shell=True
import sys
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CHART = Path(__file__).resolve().parents[1] / "helm" / "olcf"


class TemplateError(Exception):
    """A template could not be parsed or evaluated (or called ``fail``)."""


# --------------------------------------------------------------------------------------------------------- lexer / parser
_ACTION = re.compile(r"\{\{(-?)\s*(.*?)\s*(-?)\}\}", re.S)


class Node:
    pass


class Text(Node):
    def __init__(self, text: str) -> None:
        self.text = text


class Action(Node):
    def __init__(self, expr: str) -> None:
        self.expr = expr


class Block(Node):
    def __init__(self, kind: str, expr: str) -> None:
        self.kind, self.expr = kind, expr
        self.body: list[Node] = []
        self.alt: list[Node] = []  # else branch (an `else if` is a nested Block inside it)


def _tokenize(src: str) -> list[tuple[str, str]]:
    """Split into ('text', s) and ('action', expr) tokens, honouring {{- and -}} trim markers; comments vanish."""
    tokens: list[tuple[str, str]] = []
    pos = 0
    trim_next = False
    for m in _ACTION.finditer(src):
        text = src[pos:m.start()]
        if trim_next:
            text = text.lstrip()
        if m.group(1) == "-":
            text = text.rstrip()
        if text:
            tokens.append(("text", text))
        expr = m.group(2)
        trim_next = m.group(3) == "-"
        pos = m.end()
        if expr.startswith("/*") and expr.endswith("*/"):
            continue
        tokens.append(("action", expr))
    tail = src[pos:]
    if trim_next:
        tail = tail.lstrip()
    if tail:
        tokens.append(("text", tail))
    return tokens


def _parse(tokens: list[tuple[str, str]], defines: dict[str, list[Node]]) -> list[Node]:
    root: list[Node] = []
    stack: list[tuple[Block, list[Node], bool]] = []  # (block, enclosing list, is an `else if` link of the block below it)
    target = root

    for kind, value in tokens:
        if kind == "text":
            target.append(Text(value))
            continue
        word = value.split(None, 1)[0] if value else ""
        rest = value[len(word):].strip()
        if word in ("if", "range", "with"):
            blk = Block(word, rest)
            target.append(blk)
            stack.append((blk, target, False))
            target = blk.body
        elif word == "define":
            blk = Block("define", _unquote(rest))
            defines[blk.expr] = blk.body
            stack.append((blk, target, False))
            target = blk.body
        elif word == "else":
            if not stack:
                raise TemplateError("{{ else }} outside a block")
            blk = stack[-1][0]
            if rest.startswith("if "):
                nested = Block("if", rest[3:].strip())
                blk.alt.append(nested)
                stack.append((nested, blk.alt, True))
                target = nested.body
            else:
                target = blk.alt
        elif word == "end":
            if not stack:
                raise TemplateError("unbalanced {{ end }}")
            _blk, outer, chained = stack.pop()
            target = outer
            while chained:  # an `else if` link ends together with the block it hangs from
                _blk, outer, chained = stack.pop()
                target = outer
        elif word in ("template", "block", "break", "continue"):
            raise TemplateError(f"unsupported action '{word}'")
        else:
            target.append(Action(value))
    if stack:
        raise TemplateError("missing {{ end }}")
    return root


def _unquote(s: str) -> str:
    s = s.strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in "\"`":
        return bytes(s[1:-1], "utf-8").decode("unicode_escape") if s[0] == '"' else s[1:-1]
    raise TemplateError(f"expected a string literal, got {s!r}")


# --------------------------------------------------------------------------------------------------------- expression language
_TOKEN = re.compile(
    r"""\s*(?:
      (?P<str>"(?:[^"\\]|\\.)*"|`[^`]*`)
    | (?P<num>-?\d+(?:\.\d+)?)
    | (?P<pipe>\|)
    | (?P<lp>\()
    | (?P<rp>\))
    | (?P<decl>:=|=(?!=))
    | (?P<comma>,)
    | (?P<ref>\$(?:\.[A-Za-z_][A-Za-z0-9_]*)+|\$?[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*|\$|\.(?:[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)?)
    )""", re.X)


def _lex_expr(expr: str) -> list[tuple[str, str]]:
    out, pos = [], 0
    expr = expr.strip()
    while pos < len(expr):
        m = _TOKEN.match(expr, pos)
        if not m or m.end() == pos:
            raise TemplateError(f"cannot parse expression near {expr[pos:pos + 30]!r} in {{{{ {expr} }}}}")
        pos = m.end()
        out.append((m.lastgroup or "", m.group(m.lastgroup or "")))
    return out


class Scope:
    def __init__(self, parent: Scope | None = None) -> None:
        self.vars: dict[str, Any] = {}
        self.parent = parent

    def lookup(self, name: str) -> Scope:
        s: Scope | None = self
        while s is not None:
            if name in s.vars:
                return s
            s = s.parent
        raise TemplateError(f"undefined variable {name}")


def truthy(v: Any) -> bool:
    if v is None or v is False:
        return False
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return v != 0
    if isinstance(v, (str, list, dict, tuple)):
        return len(v) > 0
    return True


def _field(base: Any, path: list[str]) -> Any:
    cur = base
    for name in path:
        if cur is None:
            raise TemplateError(f"nil pointer evaluating .{'.'.join(path)}")
        if isinstance(cur, dict):
            cur = cur.get(name)
        else:
            raise TemplateError(f"cannot read field '{name}' of {type(cur).__name__}")
    return cur


class Engine:
    """Evaluates parsed templates against a root context (``.Values``, ``.Release``, ``.Chart`` ...)."""

    def __init__(self, defines: dict[str, list[Node]]) -> None:
        self.defines = defines
        self.funcs = _functions(self)

    # ---- rendering
    def render(self, nodes: list[Node], dot: Any, root: Any, scope: Scope) -> str:
        out: list[str] = []
        for node in nodes:
            if isinstance(node, Text):
                out.append(node.text)
            elif isinstance(node, Action):
                out.append(self._action(node.expr, dot, root, scope))
            elif isinstance(node, Block):
                out.append(self._block(node, dot, root, scope))
        return "".join(out)

    def _action(self, expr: str, dot: Any, root: Any, scope: Scope) -> str:
        toks = _lex_expr(expr)
        if len(toks) >= 2 and toks[0][0] == "ref" and toks[0][1].startswith("$") and toks[1][0] == "decl":
            name = toks[0][1]
            val = self._pipeline(toks[2:], dot, root, scope)
            if toks[1][1] == ":=":
                scope.vars[name] = val
            else:
                scope.lookup(name).vars[name] = val
            return ""
        return _to_str(self._pipeline(toks, dot, root, scope))

    def _block(self, blk: Block, dot: Any, root: Any, scope: Scope) -> str:
        if blk.kind == "define":
            return ""
        if blk.kind == "if":
            cond = self._pipeline(_lex_expr(blk.expr), dot, root, scope)
            inner = Scope(scope)
            return self.render(blk.body if truthy(cond) else blk.alt, dot, root, inner)
        if blk.kind == "with":
            val = self._pipeline(_lex_expr(blk.expr), dot, root, scope)
            return self.render(blk.body, val, root, Scope(scope)) if truthy(val) else self.render(blk.alt, dot, root, Scope(scope))
        if blk.kind == "range":
            return self._range(blk, dot, root, scope)
        raise TemplateError(f"unknown block {blk.kind}")

    def _range(self, blk: Block, dot: Any, root: Any, scope: Scope) -> str:
        toks = _lex_expr(blk.expr)
        names: list[str] = []
        for i, (k, v) in enumerate(toks):
            if k == "decl" and v == ":=":
                names = [t[1] for t in toks[:i] if t[0] == "ref"]
                toks = toks[i + 1:]
                break
        seq = self._pipeline(toks, dot, root, scope)
        if isinstance(seq, dict):
            items = [(k, seq[k]) for k in sorted(seq)]
        elif isinstance(seq, (list, tuple)):
            items = list(enumerate(seq))
        elif seq is None:
            items = []
        else:
            raise TemplateError(f"cannot range over {type(seq).__name__}")
        if not items:
            return self.render(blk.alt, dot, root, Scope(scope))
        out = []
        for key, val in items:
            inner = Scope(scope)
            if len(names) == 1:
                inner.vars[names[0]] = val
            elif len(names) == 2:
                inner.vars[names[0]], inner.vars[names[1]] = key, val
            out.append(self.render(blk.body, val, root, inner))
        return "".join(out)

    # ---- pipelines
    def _pipeline(self, toks: list[tuple[str, str]], dot: Any, root: Any, scope: Scope) -> Any:
        commands: list[list[tuple[str, str]]] = [[]]
        depth = 0
        for t in toks:
            if t[0] == "lp":
                depth += 1
            elif t[0] == "rp":
                depth -= 1
            if t[0] == "pipe" and depth == 0:
                commands.append([])
            else:
                commands[-1].append(t)
        value: Any = None
        have = False
        for cmd in commands:
            value = self._command(cmd, dot, root, scope, value if have else None, have)
            have = True
        return value

    def _command(self, cmd: list[tuple[str, str]], dot: Any, root: Any, scope: Scope, piped: Any, have_piped: bool) -> Any:
        operands = self._split_operands(cmd)
        if not operands:
            raise TemplateError("empty command")
        first = operands[0]
        if len(first) == 1 and first[0][0] == "ref" and first[0][1] in self.funcs:
            name = first[0][1]
            if name in ("and", "or"):  # short-circuit
                return self._logic(name, operands[1:], dot, root, scope, piped, have_piped)
            args = [self._operand(o, dot, root, scope) for o in operands[1:]]
            if have_piped:
                args.append(piped)
            return self.funcs[name](*args)
        if len(operands) > 1:
            raise TemplateError(f"'{first[0][1]}' is not a function but was given arguments")
        if have_piped:
            raise TemplateError("a value cannot be piped into a non-function")
        return self._operand(first, dot, root, scope)

    def _logic(self, name: str, operands: list[list[tuple[str, str]]], dot: Any, root: Any, scope: Scope, piped: Any, have_piped: bool) -> Any:
        values = [lambda o=o: self._operand(o, dot, root, scope) for o in operands]  # noqa: B023 - bound through the default argument
        if have_piped:
            values.append(lambda: piped)
        result: Any = None
        for i, getter in enumerate(values):
            result = getter()
            if name == "and" and not truthy(result):
                return result
            if name == "or" and truthy(result):
                return result
            if i == len(values) - 1:
                return result
        return result

    @staticmethod
    def _split_operands(cmd: list[tuple[str, str]]) -> list[list[tuple[str, str]]]:
        operands: list[list[tuple[str, str]]] = []
        i = 0
        while i < len(cmd):
            if cmd[i][0] == "lp":
                depth, j = 1, i + 1
                while j < len(cmd) and depth:
                    depth += (cmd[j][0] == "lp") - (cmd[j][0] == "rp")
                    j += 1
                if depth:
                    raise TemplateError("unbalanced parentheses")
                operands.append(cmd[i:j])
                i = j
            else:
                operands.append([cmd[i]])
                i += 1
        return operands

    def _operand(self, operand: list[tuple[str, str]], dot: Any, root: Any, scope: Scope) -> Any:
        if operand[0][0] == "lp":
            return self._pipeline(operand[1:-1], dot, root, scope)
        kind, text = operand[0]
        if kind == "str":
            return _unquote(text)
        if kind == "num":
            return float(text) if "." in text else int(text)
        if kind != "ref":
            raise TemplateError(f"unexpected token {text!r}")
        if text in ("true", "false"):
            return text == "true"
        if text == "nil":
            return None
        if text == ".":
            return dot
        if text == "$":
            return root
        if text.startswith("$"):
            name, _, rest = text.partition(".")
            val = root if name == "$" else scope.lookup(name).vars[name]
            return _field(val, rest.split(".")) if rest else val
        if text.startswith("."):
            return _field(dot, text[1:].split("."))
        if text in self.funcs:  # a bare function name used as a value, e.g. `{{ now }}`
            return self.funcs[text]()
        raise TemplateError(f"unknown identifier '{text}'")


def _to_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float) and v == int(v):
        return str(int(v))
    if isinstance(v, (dict, list)):
        return json.dumps(v)
    return str(v)


# --------------------------------------------------------------------------------------------------------- function library
def _to_yaml(v: Any) -> str:
    if v is None:
        return "null"
    return yaml.safe_dump(v, default_flow_style=False, sort_keys=True).rstrip("\n").rstrip() if v not in ({}, []) else ("{}" if v == {} else "[]")


def _indent(n: int, s: Any) -> str:
    pad = " " * int(n)
    return "\n".join(pad + line if line else line for line in _to_str(s).split("\n"))


def _printf(fmt: str, *args: Any) -> str:
    conv = iter(args)

    def repl(m: re.Match[str]) -> str:
        if m.group(0) == "%%":
            return "%"
        v = next(conv)
        spec = m.group(0)[-1]
        if spec == "d":
            return str(int(v))
        if spec == "q":
            return json.dumps(_to_str(v))
        return _to_str(v)

    return re.sub(r"%%|%[-+ #0-9.]*[a-zA-Z]", repl, fmt)


def _functions(engine: Engine) -> dict[str, Any]:
    def include(name: str, ctx: Any) -> str:
        if name not in engine.defines:
            raise TemplateError(f"include: template '{name}' is not defined")
        root = ctx if isinstance(ctx, dict) else _CURRENT_ROOT[0]
        return engine.render(engine.defines[name], ctx, root, Scope())

    def fail(msg: str) -> None:
        raise TemplateError(f"fail: {msg}")

    def required(msg: str, v: Any) -> Any:
        if not truthy(v):
            raise TemplateError(f"required: {msg}")
        return v

    def default(d: Any, v: Any = None) -> Any:
        return v if truthy(v) else d

    def index(m: Any, *keys: Any) -> Any:
        for k in keys:
            m = None if m is None else (m.get(k) if isinstance(m, dict) else m[k])
        return m

    def cmp(op: str) -> Any:
        import operator
        fn = getattr(operator, op)

        def inner(a: Any, *rest: Any) -> bool:
            if op in ("eq",):
                return any(a == b for b in rest)
            return fn(a, rest[0])

        return inner

    def until(n: Any) -> list[int]:
        return list(range(int(n)))

    def dict_(*kv: Any) -> dict[str, Any]:
        return {kv[i]: kv[i + 1] for i in range(0, len(kv), 2)}

    return {
        "include": include, "fail": fail, "required": required, "default": default, "index": index, "until": until,
        "dict": dict_, "list": lambda *a: list(a), "not": lambda v: not truthy(v), "and": None, "or": None,
        "eq": cmp("eq"), "ne": lambda a, b: a != b, "gt": cmp("gt"), "lt": cmp("lt"), "ge": cmp("ge"), "le": cmp("le"),
        "int": lambda v: int(float(v)) if v not in (None, "") else 0, "quote": lambda v: json.dumps(_to_str(v), ensure_ascii=False),
        "squote": lambda v: "'" + _to_str(v) + "'", "toYaml": _to_yaml, "toJson": lambda v: json.dumps(v, sort_keys=True),
        "nindent": lambda n, s: "\n" + _indent(n, s), "indent": _indent,
        "trunc": lambda n, s: _to_str(s)[: int(n)], "trimSuffix": lambda suf, s: _to_str(s)[: -len(suf)] if suf and _to_str(s).endswith(suf) else _to_str(s),
        "trimPrefix": lambda pre, s: _to_str(s)[len(pre):] if _to_str(s).startswith(pre) else _to_str(s),
        "hasPrefix": lambda pre, s: _to_str(s).startswith(pre), "hasSuffix": lambda suf, s: _to_str(s).endswith(suf),
        "contains": lambda sub, s: sub in _to_str(s), "replace": lambda old, new, s: _to_str(s).replace(old, new),
        "upper": lambda s: _to_str(s).upper(), "lower": lambda s: _to_str(s).lower(), "toString": _to_str,
        "printf": _printf, "print": lambda *a: "".join(_to_str(x) for x in a), "len": lambda v: len(v) if v is not None else 0,
        "b64enc": lambda s: base64.b64encode(_to_str(s).encode()).decode(), "b64dec": lambda s: base64.b64decode(_to_str(s)).decode(),
        "sha256sum": lambda s: hashlib.sha256(_to_str(s).encode()).hexdigest(), "hasKey": lambda m, k: isinstance(m, dict) and k in m,
        "lookup": lambda *a: {},  # helm template / a first install: nothing exists yet
        "randAlphaNum": lambda n: "".join(secrets.choice(string.ascii_letters + string.digits) for _ in range(int(n))),
        "randBytes": lambda n: base64.b64encode(secrets.token_bytes(int(n))).decode(),
        "ternary": lambda a, b, c: a if truthy(c) else b, "join": lambda sep, seq: sep.join(_to_str(x) for x in seq),
        "add": lambda *a: sum(int(x) for x in a), "add1": lambda v: int(v) + 1, "sub": lambda a, b: int(a) - int(b),
    }


_CURRENT_ROOT: list[Any] = [None]


# --------------------------------------------------------------------------------------------------------- chart loading / rendering
def _deep_merge(base: dict[str, Any], over: dict[str, Any]) -> dict[str, Any]:
    out = copy.deepcopy(base)
    for k, v in over.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def _set_path(values: dict[str, Any], assignment: str) -> None:
    key, _, raw = assignment.partition("=")
    if not key or not _:
        raise ValueError(f"--set expects a.b=value, got {assignment!r}")
    cur = values
    parts = key.split(".")
    for p in parts[:-1]:
        cur = cur.setdefault(p, {})
    cur[parts[-1]] = yaml.safe_load(raw) if raw != "" else ""


def render_chart(chart_dir: Path, value_files: list[Path] | None = None, sets: list[str] | None = None, release: str = "olcf",
                 namespace: str = "olcf") -> dict[str, str]:
    """Render all templates; returns ``{template file name: rendered text}`` (NOTES.txt and helpers included under their names)."""
    chart_dir = Path(chart_dir)
    chart_meta = yaml.safe_load((chart_dir / "Chart.yaml").read_text(encoding="utf-8"))
    values = yaml.safe_load((chart_dir / "values.yaml").read_text(encoding="utf-8")) or {}
    for vf in value_files or []:
        values = _deep_merge(values, yaml.safe_load(Path(vf).read_text(encoding="utf-8")) or {})
    for s in sets or []:
        _set_path(values, s)
    schema_path = chart_dir / "values.schema.json"
    if schema_path.exists():
        import jsonschema

        errors = sorted(jsonschema.Draft7Validator(json.loads(schema_path.read_text(encoding="utf-8"))).iter_errors(values), key=lambda e: list(e.path))
        if errors:
            raise TemplateError("values do not match values.schema.json:\n" + "\n".join(f"  {'.'.join(map(str, e.path)) or '<root>'}: {e.message}" for e in errors))
    root = {
        "Values": values,
        "Chart": {"Name": chart_meta["name"], "Version": chart_meta["version"], "AppVersion": str(chart_meta.get("appVersion", "")), **{k[0].upper() + k[1:]: v for k, v in chart_meta.items()}},
        "Release": {"Name": release, "Namespace": namespace, "Service": "Helm", "IsInstall": True, "IsUpgrade": False, "Revision": 1},
        "Capabilities": {"KubeVersion": {"Version": "v1.29.0"}},
        "Template": {"BasePath": f"{chart_meta['name']}/templates"},
    }
    _CURRENT_ROOT[0] = root
    defines: dict[str, list[Node]] = {}
    parsed: dict[str, list[Node]] = {}
    templates = chart_dir / "templates"
    for path in sorted(templates.iterdir()):
        if path.is_file() and (path.suffix in (".yaml", ".yml", ".tpl", ".txt")):
            try:
                parsed[path.name] = _parse(_tokenize(path.read_text(encoding="utf-8")), defines)
            except TemplateError as exc:
                raise TemplateError(f"{path.name}: {exc}") from exc
    engine = Engine(defines)
    out: dict[str, str] = {}
    for name, nodes in parsed.items():
        if name.startswith("_"):
            continue
        try:
            out[name] = engine.render(nodes, root, root, Scope())
        except TemplateError as exc:
            raise TemplateError(f"{name}: {exc}") from exc
    return out


def render_string(template: str, values: dict[str, Any] | None = None, helpers: str = "") -> str:
    """Render one template string (with optional ``define`` helpers) against ``.Values``; used by the renderer's own tests."""
    root = {"Values": values or {}, "Release": {"Name": "rel", "Namespace": "ns"}, "Chart": {"Name": "c", "Version": "1", "AppVersion": "1"}}
    _CURRENT_ROOT[0] = root
    defines: dict[str, list[Node]] = {}
    _parse(_tokenize(helpers), defines)
    nodes = _parse(_tokenize(template), defines)
    return Engine(defines).render(nodes, root, root, Scope())


def manifests(rendered: dict[str, str]) -> list[tuple[str, dict[str, Any]]]:
    """Parse every rendered YAML file into (file, object) pairs; raises ``TemplateError`` for invalid YAML."""
    docs: list[tuple[str, dict[str, Any]]] = []
    for name, text in rendered.items():
        if not name.endswith((".yaml", ".yml")):
            continue
        try:
            for doc in yaml.safe_load_all(text):
                if doc:
                    if not isinstance(doc, dict) or "kind" not in doc:
                        raise TemplateError(f"{name}: document without a kind: {str(doc)[:80]}")
                    docs.append((name, doc))
        except yaml.YAMLError as exc:
            raise TemplateError(f"{name}: rendered output is not valid YAML: {exc}") from exc
    return docs


def find_kubeconform() -> str | None:
    return os.environ.get("KUBECONFORM") or shutil.which("kubeconform")


def run_kubeconform(binary: str, docs: list[tuple[str, dict[str, Any]]], version: str = "1.29.0") -> tuple[int, str]:
    payload = "\n---\n".join(yaml.safe_dump(d) for _, d in docs)
    proc = subprocess.run([binary, "-strict", "-summary", "-ignore-missing-schemas", "-kubernetes-version", version, "-"],  # noqa: S603
                          input=payload, capture_output=True, text=True, timeout=300, check=False)
    return proc.returncode, (proc.stdout + proc.stderr).strip()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("chart", nargs="?", default=str(DEFAULT_CHART))
    ap.add_argument("-f", "--values", action="append", default=[], type=Path)
    ap.add_argument("--set", action="append", default=[], dest="sets")
    ap.add_argument("--release", default="olcf")
    ap.add_argument("--namespace", default="olcf")
    ap.add_argument("--out", type=Path, default=None, help="write one file per template here instead of stdout")
    ap.add_argument("--kubeconform", action="store_true", help="also validate with kubeconform (KUBECONFORM or PATH)")
    ap.add_argument("--quiet", action="store_true", help="print nothing but errors")
    args = ap.parse_args(argv)
    try:
        rendered = render_chart(Path(args.chart), args.values, args.sets, args.release, args.namespace)
        docs = manifests(rendered)
    except (TemplateError, OSError, ValueError, yaml.YAMLError) as exc:
        print(f"render_chart: {exc}", file=sys.stderr)
        return 1
    if args.out:
        args.out.mkdir(parents=True, exist_ok=True)
        for name, text in rendered.items():
            (args.out / name).write_text(text, encoding="utf-8")
    elif not args.quiet:
        for name, text in rendered.items():
            if name.endswith((".yaml", ".yml")) and text.strip():
                print(f"---\n# Source: {name}\n{text.strip()}")
    if args.kubeconform:
        binary = find_kubeconform()
        if not binary:
            print("render_chart: kubeconform not found (set KUBECONFORM or add it to PATH)", file=sys.stderr)
            return 1
        code, text = run_kubeconform(binary, docs)
        print(text, file=sys.stderr)
        if code != 0:
            return 1
    if not args.quiet:
        print(f"render_chart: {len(docs)} objects rendered from {len(rendered)} templates", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
