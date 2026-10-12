# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Notebook parameters: detection, typed coercion and safe injection (papermill semantics).

* **Detection** - the first code cell tagged ``parameters`` is parsed with :mod:`ast`; every top-level ``name = <literal>`` (or
  ``name: int = 3``) becomes a typed parameter (default inferred from the literal, description from a trailing / preceding comment).
  Non-literal right-hand sides (``os.environ.get(...)``) are reported as ``source: "expression"`` and cannot be overridden by type.
* **Coercion** - values coming from pipelines, schedules or the API are checked and coerced against the detected types
  (``"5"`` -> ``5`` for an ``integer``, ``"true"`` -> ``True`` for a ``boolean`` ...).  Anything else fails with a precise message.
* **Injection** - a new code cell tagged ``injected-parameters`` is inserted right after the parameters cell (at the top when the
  notebook has none).  Values are rendered by :func:`py_literal`, a *closed* encoder (str, int, float, bool, None, list, dict with string
  keys): it never calls ``repr`` on arbitrary objects, escapes strings through ``repr(str)`` (which cannot terminate the literal), writes
  non-finite floats as ``float("nan")`` and refuses names that are not plain identifiers.  Injection therefore cannot execute code that the
  caller did not already control.
"""

from __future__ import annotations

import ast
import copy
import io
import json
import keyword
import math
import re
import tokenize
from dataclasses import asdict, dataclass, field
from typing import Any

from olcf.core.errors import ValidationFailed
from olcf.notebooks.formats import derived_cell_id, make_cell

PARAMETERS_TAG = "parameters"
INJECTED_TAG = "injected-parameters"
MAX_PARAM_VALUE_BYTES = 1024 * 1024
MAX_DEPTH = 24


@dataclass
class Parameter:
    """A notebook parameter found in a parameters cell: name, type, default and whether it has a default."""
    name: str
    type: str = "any"  # string | integer | number | boolean | array | object | any
    default: Any = None
    has_default: bool = True
    nullable: bool = False
    source: str = "literal"  # literal | expression
    line: int = 0  # 1-based line inside the parameters cell
    description: str = ""
    annotation: str | None = None
    default_source: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        if d["annotation"] is None:
            d.pop("annotation")
        if d["default_source"] is None:
            d.pop("default_source")
        return d


@dataclass
class Detection:
    """Result of looking for a parameters cell: the parameters, the cell where they were found and warnings."""
    parameters: list[Parameter] = field(default_factory=list)
    cell_id: str | None = None
    cell_index: int | None = None
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {"cell_id": self.cell_id, "cell_index": self.cell_index, "parameters": [p.to_dict() for p in self.parameters], "warnings": self.warnings}


# ============================================================================== detection
_ANNOTATIONS = {"str": "string", "int": "integer", "float": "number", "bool": "boolean", "list": "array", "dict": "object", "tuple": "array", "set": "array"}


def _infer_type(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, (list, tuple, set, frozenset)):
        return "array"
    if isinstance(value, dict):
        return "object"
    return "any"


def _annotation_type(ann: ast.expr | None, src: str) -> tuple[str | None, bool]:
    """(type, nullable) from an annotation node; ``(None, False)`` when unknown."""
    if ann is None:
        return None, False
    text = (ast.get_source_segment(src, ann) or "").replace(" ", "")
    nullable = False
    for opt in ("Optional[", "typing.Optional["):
        if text.startswith(opt) and text.endswith("]"):
            text, nullable = text[len(opt):-1], True
    if text.endswith("|None"):
        text, nullable = text[:-5], True
    elif text.startswith("None|"):
        text, nullable = text[5:], True
    base = re.split(r"[\[.]", text, maxsplit=1)[0] if not text.startswith("typing.") else text.split(".", 1)[1].split("[", 1)[0]
    base = base.lower() if base[:1].isupper() and base.lower() in ("list", "dict", "tuple", "set") else base
    return _ANNOTATIONS.get(base), nullable


def _blank_magics(code: str) -> str:
    """Parameter cells may contain notebook magics; blank them (keeping line numbers) so ``ast`` can parse the rest."""
    out = []
    for ln in code.split("\n"):
        s = ln.lstrip()
        out.append("" if s.startswith(("%", "!")) else ln)
    return "\n".join(out)


def _comments(code: str) -> dict[int, tuple[str, bool]]:
    """line -> (comment text, comment_only_line)"""
    found: dict[int, tuple[str, bool]] = {}
    try:
        for tok in tokenize.generate_tokens(io.StringIO(code).readline):
            if tok.type == tokenize.COMMENT:
                line = tok.line.strip()
                found[tok.start[0]] = (tok.string.lstrip("#").strip(), line.startswith("#"))
    except (tokenize.TokenError, IndentationError, SyntaxError):
        pass
    return found


def find_parameters_cell(nb: dict[str, Any]) -> tuple[int, dict[str, Any]] | None:
    for i, c in enumerate(nb["cells"]):
        if c["cell_type"] == "code" and PARAMETERS_TAG in ((c.get("metadata") or {}).get("tags") or []):
            return i, c
    return None


def detect_parameters(nb: dict[str, Any]) -> Detection:
    """Typed parameter list of the notebook's first ``parameters``-tagged cell (empty when there is none)."""
    det = Detection()
    found = find_parameters_cell(nb)
    if found is None:
        return det
    idx, cell = found
    det.cell_index, det.cell_id = idx, cell["id"]
    code = _blank_magics(cell["source"])
    try:
        tree = ast.parse(code)
    except SyntaxError as exc:
        det.warnings.append(f"the parameters cell has a syntax error (line {exc.lineno}): {exc.msg}; no parameters were detected")
        return det
    comments = _comments(code)
    by_name: dict[str, Parameter] = {}
    for stmt in tree.body:
        targets: list[str] = []
        value_node: ast.expr | None = None
        ann_node: ast.expr | None = None
        if isinstance(stmt, ast.Assign):
            value_node = stmt.value
            for t in stmt.targets:
                if isinstance(t, ast.Name):
                    targets.append(t.id)
                else:
                    det.warnings.append(f"line {stmt.lineno}: only simple 'name = value' assignments define parameters (tuple unpacking ignored)")
        elif isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
            targets.append(stmt.target.id)
            value_node, ann_node = stmt.value, stmt.annotation
        else:
            continue
        for name in targets:
            p = Parameter(name=name, line=stmt.lineno)
            ann_type, ann_nullable = _annotation_type(ann_node, code)
            if ann_node is not None:
                p.annotation = ast.get_source_segment(code, ann_node)
            if value_node is None:
                p.has_default, p.default, p.type = False, None, ann_type or "any"
            else:
                try:
                    lit = ast.literal_eval(value_node)
                    p.default = _jsonable(lit)
                    p.type = _infer_type(lit)
                    if lit is None:
                        p.nullable = True
                        p.type = ann_type or "any"
                    if ann_type == "number" and p.type == "integer":
                        p.type = "number"
                    elif ann_type and p.type in ("any",):
                        p.type = ann_type
                except (ValueError, SyntaxError, TypeError, MemoryError, RecursionError):
                    p.source, p.type, p.default = "expression", ann_type or "any", None
                    p.default_source = ast.get_source_segment(code, value_node)
            p.nullable = p.nullable or ann_nullable
            end = getattr(stmt, "end_lineno", stmt.lineno)
            desc = comments.get(end) or comments.get(stmt.lineno)
            if desc is None:
                prev = comments.get(stmt.lineno - 1)
                if prev and prev[1]:
                    desc = prev
            p.description = desc[0] if desc else ""
            if name in by_name:
                p.line = by_name[name].line  # a re-assignment keeps the first position but the last value (that is what the notebook would see)
            by_name[name] = p
    det.parameters = list(by_name.values())
    return det


def _jsonable(v: Any) -> Any:
    if isinstance(v, (set, frozenset)):
        try:
            return sorted(_jsonable(x) for x in v)
        except TypeError:
            return [_jsonable(x) for x in v]
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    if isinstance(v, dict):
        return {str(k): _jsonable(x) for k, x in v.items()}
    if isinstance(v, float) and not math.isfinite(v):
        return None
    if isinstance(v, (bytes, complex)):
        return repr(v)
    return v


# ============================================================================== values
def validate_name(name: Any) -> str:
    if not isinstance(name, str) or not name.isidentifier() or keyword.iskeyword(name) or (name.startswith("__") and name.endswith("__")):
        raise ValidationFailed(f"parameter name {name!r} is not a valid Python identifier",
                               remediation="use letters, digits and underscores (not starting with a digit) and avoid Python keywords and __dunder__ names")
    if len(name) > 128:
        raise ValidationFailed("parameter names may have at most 128 characters")
    return name


def check_json_value(value: Any, where: str = "value", _depth: int = 0) -> Any:
    """Reject anything outside the closed JSON-like value set (str, int, float (finite), bool, None, list, dict with str keys)."""
    if _depth > MAX_DEPTH:
        raise ValidationFailed(f"{where} is nested deeper than {MAX_DEPTH} levels")
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValidationFailed(f"{where} is not a finite number (NaN / Infinity cannot be passed as notebook parameters)")
        return value
    if isinstance(value, (list, tuple)):
        return [check_json_value(v, f"{where}[{i}]", _depth + 1) for i, v in enumerate(value)]
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            if not isinstance(k, str):
                raise ValidationFailed(f"{where} has a non-string key {k!r}")
            out[k] = check_json_value(v, f"{where}.{k}", _depth + 1)
        return out
    raise ValidationFailed(f"{where} has unsupported type {type(value).__name__}", remediation="parameters must be JSON values")


_INT_RE = re.compile(r"^[+-]?\d+$")


def _coerce(p: Parameter, value: Any) -> Any:
    t = p.type
    if value is None:
        if p.nullable or t == "any" or p.default is None:
            return None
        raise ValidationFailed(f"parameter '{p.name}' does not accept null (type {t})")
    if t == "any":
        return value
    if t == "string":
        if isinstance(value, str):
            return value
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return str(value)
    elif t == "integer":
        if isinstance(value, bool):
            pass
        elif isinstance(value, int):
            return value
        elif isinstance(value, float) and value.is_integer():
            return int(value)
        elif isinstance(value, str) and _INT_RE.match(value.strip()):
            return int(value.strip())
    elif t == "number":
        if isinstance(value, bool):
            pass
        elif isinstance(value, (int, float)):
            return value
        elif isinstance(value, str):
            try:
                f = float(value.strip())
            except ValueError:
                f = math.nan
            if math.isfinite(f):
                return int(f) if _INT_RE.match(value.strip()) else f
    elif t == "boolean":
        if isinstance(value, bool):
            return value
        if isinstance(value, str) and value.strip().lower() in ("true", "false", "1", "0", "yes", "no"):
            return value.strip().lower() in ("true", "1", "yes")
    elif t in ("array", "object"):
        want = list if t == "array" else dict
        if isinstance(value, want):
            return value
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except ValueError:
                parsed = None
            if isinstance(parsed, want):
                return parsed
    raise ValidationFailed(f"parameter '{p.name}' expects {t}, got {type(value).__name__} {_short(value)}",
                           details={"parameter": p.name, "expected": t}, remediation=f"pass a {t} value")


def _short(v: Any) -> str:
    s = json.dumps(v, default=str)
    return s if len(s) <= 40 else s[:37] + "..."


def coerce_parameters(declared: list[Parameter], values: dict[str, Any] | None, *, allow_undeclared: bool = True, require: bool = True) -> dict[str, Any]:
    """Validate ``values`` against the detected parameters and return the coerced mapping (order: declaration order, then extras)."""
    values = dict(values or {})
    if len(json.dumps(values, default=str)) > MAX_PARAM_VALUE_BYTES:
        raise ValidationFailed("parameter values are too large (limit 1 MiB)")
    by = {p.name: p for p in declared}
    out: dict[str, Any] = {}
    for p in declared:
        if p.name in values:
            out[p.name] = _coerce(p, check_json_value(values.pop(p.name), f"parameter '{p.name}'"))
        elif require and not p.has_default and p.source == "literal":
            raise ValidationFailed(f"required parameter '{p.name}' was not provided", details={"parameter": p.name}, remediation="pass a value in 'parameters'")
    for name, v in values.items():
        validate_name(name)
        if name not in by and not allow_undeclared:
            raise ValidationFailed(f"unknown parameter '{name}'", details={"known": sorted(by)}, remediation="declare it in the notebook's parameters cell")
        out[name] = check_json_value(v, f"parameter '{name}'")
    return out


# ============================================================================== injection
def py_literal(value: Any, _depth: int = 0) -> str:
    """Render a JSON-like value as a Python literal.  Total on the closed value set, raises on anything else."""
    if _depth > MAX_DEPTH:
        raise ValidationFailed("parameter value is nested too deeply")
    if value is None:
        return "None"
    if isinstance(value, bool):
        return "True" if value else "False"
    if isinstance(value, int):
        return str(int(value))
    if isinstance(value, float):
        if math.isnan(value):
            return 'float("nan")'
        if math.isinf(value):
            return 'float("inf")' if value > 0 else 'float("-inf")'
        return repr(float(value))
    if isinstance(value, str):
        return repr(str(value))  # repr(str) always yields a self-contained literal with quotes and newlines escaped
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(py_literal(v, _depth + 1) for v in value) + "]"
    if isinstance(value, dict):
        items = []
        for k, v in value.items():
            if not isinstance(k, str):
                raise ValidationFailed(f"cannot inject a mapping with non-string key {k!r}")
            items.append(f"{py_literal(k, _depth + 1)}: {py_literal(v, _depth + 1)}")
        return "{" + ", ".join(items) + "}"
    raise ValidationFailed(f"cannot inject a value of type {type(value).__name__}", remediation="parameters must be JSON values")


def render_parameter_cell(params: dict[str, Any]) -> str:
    lines = ["# Parameters injected by OLCF (papermill-compatible)"]
    for name, value in params.items():
        lines.append(f"{validate_name(name)} = {py_literal(value)}")
    return "\n".join(lines)


def inject_parameters(nb: dict[str, Any], params: dict[str, Any] | None) -> dict[str, Any]:
    """Return a deep copy of ``nb`` with an ``injected-parameters`` cell after the parameters cell (top when there is none).

    An ``injected-parameters`` cell that is already present is replaced, so injecting twice never stacks two cells.
    """
    out = copy.deepcopy(nb)
    out["cells"] = [c for c in out["cells"] if INJECTED_TAG not in ((c.get("metadata") or {}).get("tags") or [])]
    if not params:
        return out
    src = render_parameter_cell(params)
    found = find_parameters_cell(out)
    pos = 0 if found is None else found[0] + 1
    ids = {c["id"] for c in out["cells"]}
    cell = make_cell("code", src, cell_id=derived_cell_id(pos, "code", src), metadata={"tags": [INJECTED_TAG]})
    while cell["id"] in ids:
        cell["id"] += "x"
    out["cells"].insert(pos, cell)
    return out
