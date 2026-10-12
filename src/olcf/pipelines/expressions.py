# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The pipeline expression language (a safe, ``eval``-free subset compatible with Data Factory / Fabric pipeline expressions).

Why this exists
---------------
Pipeline definitions are data that users (and imported Fabric pipelines) author. Their parameters, activity inputs and control-flow
conditions must be computable from run state without ever executing user-supplied Python. This module is a small hand-written
tokenizer + Pratt parser + tree-walking evaluator. The only things an expression can touch are the values handed to it in an
:class:`EvalContext`; there is no attribute access on Python objects, no imports, no I/O and no unbounded loops (evaluation is
step-limited and the result size is capped).

Syntax
------
A *string value* is interpreted as follows:

* ``"@expr"``            - the whole string is an expression; the result keeps its native type (number, bool, list, object...).
* ``"text @{expr} more"`` - string interpolation; every ``@{...}`` is stringified and spliced in.
* ``"@@text"``            - escape: yields the literal string ``@text``.
* anything else           - a literal.
* ``{"value": "@expr", "type": "Expression"}`` (Fabric style) is treated like ``"@expr"``.

Inside an expression: string literals ``'it''s'`` (or ``"..."``), numbers, ``true/false/null``, function calls ``concat('a', 1)``,
member access ``a.b`` / null-safe ``a?.b``, indexing ``a[0]`` / ``a['k']``, and the operators ``! - * / % + - < <= > >= == != && ||``
(plus every operator as a function: ``equals``, ``less``, ``and``...). Function names are case-insensitive.

Run-state accessors: ``pipeline()`` (``.parameters.x``, ``.RunId``, ``.Pipeline``, ``.Workspace``, ``.TriggerType``, ``.TriggerTime``),
``parameters('x')``, ``variables('v')``, ``activity('A')`` (``.output``, ``.status``, ``.error``), ``item()`` / ``itemIndex()`` inside
``for_each``, ``trigger()`` (``.body``, ``.type``, ``.scheduledTime``) and ``triggerBody()``.

The full function catalogue is :data:`FUNCTIONS` (also served by ``GET /v1/pipelines/activity-types``).
"""

from __future__ import annotations

import base64
import json
import math
import re
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from typing import Any
from urllib.parse import quote, unquote
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

MAX_EXPRESSION_CHARS = 16 * 1024
MAX_AST_DEPTH = 64
MAX_STEPS = 50_000
MAX_RESULT_CHARS = 5 * 1024 * 1024
MAX_RANGE = 100_000


class ExpressionError(Exception):
    """Syntax or evaluation error with a precise message and the offending position."""

    def __init__(self, message: str, *, pos: int | None = None, source: str | None = None, code: str = "expression_error") -> None:
        self.message, self.pos, self.source, self.code = message, pos, source, code
        super().__init__(self.render())

    def render(self) -> str:
        if self.pos is None or self.source is None:
            return self.message
        start = max(0, self.pos - 12)
        snippet = self.source[start:self.pos + 20]
        return f"{self.message} (at position {self.pos}, near '{snippet}')"

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.render(), "position": self.pos}


# ---------------------------------------------------------------------------------------------------------------------------------------- tokenizer
_NUM = re.compile(r"\d+(?:\.\d+)?(?:[eE][+-]?\d+)?")
_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_PUNCT3 = ()
_PUNCT2 = ("<=", ">=", "==", "!=", "&&", "||", "?.")
_PUNCT1 = "()[],.!-+*/%<>"


@dataclass(frozen=True)
class Token:
    """A lexical token of the expression language: kind (number, string, identifier, operator or end), value and position."""
    kind: str  # num | str | ident | op | end
    value: Any
    pos: int


def tokenize(src: str) -> list[Token]:
    if len(src) > MAX_EXPRESSION_CHARS:
        raise ExpressionError(f"expression is longer than {MAX_EXPRESSION_CHARS} characters", source=src[:40])
    out: list[Token] = []
    i, n = 0, len(src)
    while i < n:
        c = src[i]
        if c.isspace():
            i += 1
            continue
        if c in "'\"":
            quote_char, j, buf = c, i + 1, []
            while True:
                if j >= n:
                    raise ExpressionError("unterminated string literal", pos=i, source=src)
                ch = src[j]
                if ch == quote_char:
                    if quote_char == "'" and j + 1 < n and src[j + 1] == "'":  # '' -> '
                        buf.append("'")
                        j += 2
                        continue
                    j += 1
                    break
                if ch == "\\" and quote_char == '"' and j + 1 < n:
                    esc = src[j + 1]
                    buf.append({"n": "\n", "t": "\t", "r": "\r", '"': '"', "\\": "\\"}.get(esc, "\\" + esc))
                    j += 2
                    continue
                buf.append(ch)
                j += 1
            out.append(Token("str", "".join(buf), i))
            i = j
            continue
        if c.isdigit():
            m = _NUM.match(src, i)
            assert m
            text = m.group(0)
            out.append(Token("num", float(text) if any(x in text for x in ".eE") else int(text), i))
            i = m.end()
            continue
        if c.isalpha() or c == "_":
            m = _IDENT.match(src, i)
            assert m
            out.append(Token("ident", m.group(0), i))
            i = m.end()
            continue
        two = src[i:i + 2]
        if two in _PUNCT2:
            out.append(Token("op", two, i))
            i += 2
            continue
        if c in _PUNCT1:
            out.append(Token("op", c, i))
            i += 1
            continue
        raise ExpressionError(f"unexpected character {c!r}", pos=i, source=src)
    out.append(Token("end", None, n))
    return out


# ---------------------------------------------------------------------------------------------------------------------------------------- AST + parser
@dataclass(frozen=True)
class Lit:
    """Syntax tree node for a literal value."""
    value: Any


@dataclass(frozen=True)
class Call:
    """Syntax tree node for a function call: name, arguments and position."""
    name: str
    args: tuple[Any, ...]
    pos: int


@dataclass(frozen=True)
class Member:
    """Syntax tree node for member access (``obj.name``), optionally null-safe."""
    obj: Any
    name: str
    optional: bool
    pos: int


@dataclass(frozen=True)
class Index:
    """Syntax tree node for indexing (``obj[index]``)."""
    obj: Any
    index: Any
    pos: int


@dataclass(frozen=True)
class Unary:
    """Syntax tree node for a unary operator applied to one operand."""
    op: str
    operand: Any
    pos: int


@dataclass(frozen=True)
class Binary:
    """Syntax tree node for a binary operator applied to two operands."""
    op: str
    left: Any
    right: Any
    pos: int


_BINARY_PREC: dict[str, int] = {"||": 1, "&&": 2, "==": 3, "!=": 3, "<": 4, "<=": 4, ">": 4, ">=": 4, "+": 5, "-": 5, "*": 6, "/": 6, "%": 6}


class _Parser:
    def __init__(self, src: str) -> None:
        self.src, self.toks, self.i = src, tokenize(src), 0

    def peek(self) -> Token:
        return self.toks[self.i]

    def next(self) -> Token:
        t = self.toks[self.i]
        self.i += 1
        return t

    def expect_op(self, op: str) -> Token:
        t = self.next()
        if t.kind != "op" or t.value != op:
            raise ExpressionError(f"expected '{op}' but found {self._desc(t)}", pos=t.pos, source=self.src)
        return t

    @staticmethod
    def _desc(t: Token) -> str:
        return "end of expression" if t.kind == "end" else repr(t.value)

    def parse(self) -> Any:
        node = self.expr(0, 0)
        t = self.peek()
        if t.kind != "end":
            raise ExpressionError(f"unexpected {self._desc(t)}", pos=t.pos, source=self.src)
        return node

    def expr(self, min_prec: int, depth: int) -> Any:
        if depth > MAX_AST_DEPTH:
            raise ExpressionError("expression is nested too deeply", pos=self.peek().pos, source=self.src)
        left = self.unary(depth + 1)
        while True:
            t = self.peek()
            if t.kind != "op" or t.value not in _BINARY_PREC:
                break
            prec = _BINARY_PREC[t.value]
            if prec < min_prec:
                break
            self.next()
            right = self.expr(prec + 1, depth + 1)
            left = Binary(t.value, left, right, t.pos)
        return left

    def unary(self, depth: int) -> Any:
        t = self.peek()
        if t.kind == "op" and t.value in ("!", "-"):
            self.next()
            return Unary(t.value, self.unary(depth + 1), t.pos)
        return self.postfix(depth)

    def postfix(self, depth: int) -> Any:
        node = self.primary(depth)
        while True:
            t = self.peek()
            if t.kind == "op" and t.value in (".", "?."):
                self.next()
                name = self.next()
                if name.kind != "ident":
                    raise ExpressionError(f"expected a property name after '{t.value}'", pos=name.pos, source=self.src)
                node = Member(node, name.value, t.value == "?.", t.pos)
            elif t.kind == "op" and t.value == "[":
                self.next()
                idx = self.expr(0, depth + 1)
                self.expect_op("]")
                node = Index(node, idx, t.pos)
            else:
                return node

    def primary(self, depth: int) -> Any:
        t = self.next()
        if t.kind in ("num", "str"):
            return Lit(t.value)
        if t.kind == "ident":
            low = t.value.lower()
            nxt = self.peek()
            if nxt.kind == "op" and nxt.value == "(":
                self.next()
                args: list[Any] = []
                if not (self.peek().kind == "op" and self.peek().value == ")"):
                    while True:
                        args.append(self.expr(0, depth + 1))
                        sep = self.peek()
                        if sep.kind == "op" and sep.value == ",":
                            self.next()
                            continue
                        break
                self.expect_op(")")
                return Call(low, tuple(args), t.pos)
            if low == "true":
                return Lit(True)
            if low == "false":
                return Lit(False)
            if low == "null":
                return Lit(None)
            raise ExpressionError(f"unknown identifier '{t.value}' (functions need parentheses, e.g. {t.value}())", pos=t.pos, source=self.src)
        if t.kind == "op" and t.value == "(":
            node = self.expr(0, depth + 1)
            self.expect_op(")")
            return node
        raise ExpressionError(f"unexpected {self._desc(t)}", pos=t.pos, source=self.src)


@lru_cache(maxsize=4096)
def parse(src: str) -> Any:
    """Parse expression source (without the leading ``@``) into an AST. Cached; raises :class:`ExpressionError`."""
    return _Parser(src).parse()


# ---------------------------------------------------------------------------------------------------------------------------------------- templates
@dataclass(frozen=True)
class Template:
    """A parsed string value: either one expression (native result) or literal/expression parts (string result)."""

    parts: tuple[str | Any, ...]
    whole: bool  # single expression -> keeps type
    source: str

    @property
    def is_literal(self) -> bool:
        return not self.whole and all(isinstance(p, str) for p in self.parts)


def _find_interpolation_end(s: str, start: int) -> int:
    """Index of the ``}`` closing an ``@{`` whose body starts at ``start`` (skips quoted strings)."""
    i, n, q = start, len(s), None
    while i < n:
        c = s[i]
        if q:
            if c == q:
                if q == "'" and i + 1 < n and s[i + 1] == "'":
                    i += 2
                    continue
                q = None
        elif c in "'\"":
            q = c
        elif c == "}":
            return i
        i += 1
    raise ExpressionError("unterminated '@{' interpolation", pos=start - 2, source=s)


@lru_cache(maxsize=4096)
def compile_template(s: str) -> Template:
    """Compile a string value (see module docstring). Raises :class:`ExpressionError` on syntax errors."""
    if s.startswith("@") and not s.startswith("@@") and not s.startswith("@{"):
        return Template((parse(s[1:]),), True, s)
    parts: list[str | Any] = []
    buf: list[str] = []
    i, n = 0, len(s)
    while i < n:
        if s.startswith("@@", i):
            buf.append("@")
            i += 2
        elif s.startswith("@{", i):
            end = _find_interpolation_end(s, i + 2)
            if buf:
                parts.append("".join(buf))
                buf = []
            parts.append(parse(s[i + 2:end]))
            i = end + 1
        else:
            buf.append(s[i])
            i += 1
    if buf:
        parts.append("".join(buf))
    return Template(tuple(parts), False, s)


def is_expression_value(v: Any) -> bool:
    """True for strings that contain an expression and for ``{"value": "@...", "type": "Expression"}`` objects."""
    if isinstance(v, str):
        return v.startswith("@") and not v.startswith("@@") or "@{" in v
    return isinstance(v, dict) and set(v) == {"value", "type"} and v.get("type") == "Expression" and isinstance(v.get("value"), str)


def contains_expression(v: Any) -> bool:
    if isinstance(v, str):
        return is_expression_value(v)
    if isinstance(v, dict):
        return is_expression_value(v) or any(contains_expression(x) for x in v.values())
    if isinstance(v, list):
        return any(contains_expression(x) for x in v)
    return False


# ---------------------------------------------------------------------------------------------------------------------------------------- context
@dataclass
class EvalContext:
    """Everything an expression can see. All members are plain JSON-like values or callables returning them."""

    parameters: dict[str, Any] = field(default_factory=dict)
    variables: Callable[[str], Any] | dict[str, Any] = field(default_factory=dict)
    pipeline: dict[str, Any] = field(default_factory=dict)
    trigger: dict[str, Any] = field(default_factory=dict)
    #: ``name -> {"output":..., "status":..., "error":...}`` or ``None`` when the activity is unknown / has not run.
    activity: Callable[[str], dict[str, Any] | None] = lambda name: None
    item: Any = None
    item_index: int | None = None
    has_item: bool = False
    now: Callable[[], datetime] = lambda: datetime.now(UTC)
    workspace: dict[str, Any] = field(default_factory=dict)
    steps: int = 0

    def get_variable(self, name: str) -> Any:
        if callable(self.variables):
            return self.variables(name)
        if name not in self.variables:
            raise ExpressionError(f"variable '{name}' is not defined", code="unknown_variable")
        return self.variables[name]

    def child(self, **over: Any) -> EvalContext:
        d = {k: getattr(self, k) for k in ("parameters", "variables", "pipeline", "trigger", "activity", "item", "item_index", "has_item", "now", "workspace")}
        d.update(over)
        return EvalContext(**d)


# ---------------------------------------------------------------------------------------------------------------------------------------- value helpers
def _type_name(v: Any) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "boolean"
    if isinstance(v, int):
        return "integer"
    if isinstance(v, float):
        return "number"
    if isinstance(v, str):
        return "string"
    if isinstance(v, list):
        return "array"
    if isinstance(v, dict):
        return "object"
    return type(v).__name__


def to_str(v: Any) -> str:
    """Stringification used by ``concat``, interpolation and ``string()``."""
    if v is None:
        return ""
    if isinstance(v, str):
        return v
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float):
        return str(int(v)) if v.is_integer() and abs(v) < 1e15 else repr(v)
    if isinstance(v, (int,)):
        return str(v)
    return json.dumps(v, separators=(",", ":"), ensure_ascii=False, default=str)


def to_number(v: Any, fn: str = "") -> int | float:
    if isinstance(v, bool):
        raise ExpressionError(f"{fn or 'operation'} expects a number but got a boolean", code="type_error")
    if isinstance(v, (int, float)):
        return v
    if isinstance(v, str):
        s = v.strip()
        try:
            return int(s)
        except ValueError:
            try:
                return float(s)
            except ValueError:
                pass
    raise ExpressionError(f"{fn or 'operation'} expects a number but got {_type_name(v)} {to_str(v)[:40]!r}", code="type_error")


def to_bool(v: Any, fn: str = "") -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, str) and v.strip().lower() in ("true", "false"):
        return v.strip().lower() == "true"
    raise ExpressionError(f"{fn or 'condition'} expects a boolean but got {_type_name(v)} {to_str(v)[:40]!r}", code="type_error")


def _need_str(v: Any, fn: str) -> str:
    if isinstance(v, str):
        return v
    if v is None:
        return ""
    if isinstance(v, (int, float, bool)):
        return to_str(v)
    raise ExpressionError(f"{fn} expects a string but got {_type_name(v)}", code="type_error")


def _need_list(v: Any, fn: str) -> list[Any]:
    if isinstance(v, list):
        return v
    raise ExpressionError(f"{fn} expects an array but got {_type_name(v)}", code="type_error")


def _need_dict(v: Any, fn: str) -> dict[str, Any]:
    if isinstance(v, dict):
        return v
    raise ExpressionError(f"{fn} expects an object but got {_type_name(v)}", code="type_error")


def equal(a: Any, b: Any) -> bool:
    if isinstance(a, bool) != isinstance(b, bool):
        return False
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return a == b
    return a == b


def _compare(a: Any, b: Any, fn: str) -> int:
    if isinstance(a, bool) or isinstance(b, bool):
        raise ExpressionError(f"{fn} cannot compare booleans", code="type_error")
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return (a > b) - (a < b)
    if isinstance(a, str) and isinstance(b, str):
        return (a > b) - (a < b)
    if isinstance(a, str) and isinstance(b, (int, float)) or isinstance(b, str) and isinstance(a, (int, float)):
        return (to_number(a, fn) > to_number(b, fn)) - (to_number(a, fn) < to_number(b, fn))
    raise ExpressionError(f"{fn} cannot compare {_type_name(a)} with {_type_name(b)}", code="type_error")


# ---------------------------------------------------------------------------------------------------------------------------------------- date/time
_MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
_DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def parse_dt(v: Any, fn: str = "") -> datetime:
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=UTC)
    if isinstance(v, str):
        s = v.strip()
        try:
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            # tolerate 7-digit fractional seconds (.NET "o" format)
            s = re.sub(r"(\.\d{6})\d+", r"\1", s)
            dt = datetime.fromisoformat(s)
            return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
        except ValueError:
            pass
    raise ExpressionError(f"{fn or 'date function'} expects an ISO 8601 timestamp but got {to_str(v)[:40]!r}", code="type_error")


_STD_FORMATS = {
    "o": "yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'", "O": "yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'", "s": "yyyy-MM-dd'T'HH:mm:ss", "d": "MM/dd/yyyy", "D": "dddd, dd MMMM yyyy",
    "t": "HH:mm", "T": "HH:mm:ss", "g": "MM/dd/yyyy HH:mm", "G": "MM/dd/yyyy HH:mm:ss", "u": "yyyy-MM-dd HH:mm:ss'Z'", "f": "dddd, dd MMMM yyyy HH:mm",
    "F": "dddd, dd MMMM yyyy HH:mm:ss", "m": "MMMM dd", "y": "yyyy MMMM",
}


def format_dt(dt: datetime, fmt: str) -> str:
    """.NET custom date/time format strings (the subset Data Factory expressions use)."""
    fmt = _STD_FORMATS.get(fmt, fmt)
    out: list[str] = []
    i, n = 0, len(fmt)

    def run(ch: str) -> int:
        j = i
        while j < n and fmt[j] == ch:
            j += 1
        return j - i

    h12 = dt.hour % 12 or 12
    while i < n:
        c = fmt[i]
        if c == "'":
            j = fmt.find("'", i + 1)
            if j < 0:
                raise ExpressionError(f"unterminated quote in date format {fmt!r}", code="format_error")
            out.append(fmt[i + 1:j])
            i = j + 1
        elif c == "\\" and i + 1 < n:
            out.append(fmt[i + 1])
            i += 2
        elif c in "yMdHhmsftzK":
            k = run(c)
            if c == "y":
                out.append(f"{dt.year:04d}" if k >= 3 else f"{dt.year % 100:02d}" if k == 2 else str(dt.year % 100))
            elif c == "M":
                out.append(_MONTHS[dt.month - 1] if k >= 4 else _MONTHS[dt.month - 1][:3] if k == 3 else f"{dt.month:02d}" if k == 2 else str(dt.month))
            elif c == "d":
                out.append(_DAYS[dt.weekday()] if k >= 4 else _DAYS[dt.weekday()][:3] if k == 3 else f"{dt.day:02d}" if k == 2 else str(dt.day))
            elif c == "H":
                out.append(f"{dt.hour:02d}" if k >= 2 else str(dt.hour))
            elif c == "h":
                out.append(f"{h12:02d}" if k >= 2 else str(h12))
            elif c == "m":
                out.append(f"{dt.minute:02d}" if k >= 2 else str(dt.minute))
            elif c == "s":
                out.append(f"{dt.second:02d}" if k >= 2 else str(dt.second))
            elif c == "f":
                out.append(f"{dt.microsecond:06d}"[:min(k, 6)].ljust(k, "0"))
            elif c == "t":
                out.append(("AM" if dt.hour < 12 else "PM") if k >= 2 else ("A" if dt.hour < 12 else "P"))
            elif c == "z":
                off = dt.utcoffset() or timedelta(0)
                tot = int(off.total_seconds() // 60)
                sign = "+" if tot >= 0 else "-"
                tot = abs(tot)
                out.append(f"{sign}{tot // 60:02d}:{tot % 60:02d}" if k >= 3 else f"{sign}{tot // 60:02d}" if k == 2 else f"{sign}{tot // 60}")
            elif c == "K":
                off = dt.utcoffset() or timedelta(0)
                tot = int(off.total_seconds() // 60)
                out.append("Z" if tot == 0 else f"{'+' if tot > 0 else '-'}{abs(tot) // 60:02d}:{abs(tot) % 60:02d}")
            i += k
        else:
            out.append(c)
            i += 1
    return "".join(out)


def iso_z(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _tz(name: str, fn: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError, OSError):
        raise ExpressionError(f"{fn}: unknown time zone {name!r} (use an IANA name such as 'Europe/Berlin')", code="type_error") from None


def _dt_out(dt: datetime, fmt: Any, fn: str) -> str:
    return format_dt(dt, _need_str(fmt, fn)) if fmt not in (None, "") else iso_z(dt)


_UNITS = {"second": "seconds", "minute": "minutes", "hour": "hours", "day": "days", "week": "weeks"}


def _add_unit(dt: datetime, n: int | float, unit: str, fn: str) -> datetime:
    u = unit.lower().rstrip("s")
    if u in _UNITS:
        return dt + timedelta(**{_UNITS[u]: n})
    if u in ("month", "year"):
        months = int(n) * (12 if u == "year" else 1)
        y, m = divmod(dt.year * 12 + dt.month - 1 + months, 12)
        import calendar

        d = min(dt.day, calendar.monthrange(y, m + 1)[1])
        return dt.replace(year=y, month=m + 1, day=d)
    raise ExpressionError(f"{fn}: unsupported time unit {unit!r} (second, minute, hour, day, week, month, year)", code="type_error")


# ---------------------------------------------------------------------------------------------------------------------------------------- functions
@dataclass(frozen=True)
class Fn:
    """An entry in the expression function table: the Python callable, its minimum and maximum argument counts (``None`` for variadic) and a description.
    """
    fn: Callable[..., Any]
    min_args: int
    max_args: int | None  # None = variadic
    doc: str = ""
    lazy: bool = False  # receives thunks instead of values (if/and/or/coalesce)
    needs_ctx: bool = False


FUNCTIONS: dict[str, Fn] = {}


def _fn(name: str, min_args: int, max_args: int | None = ..., *, doc: str = "", lazy: bool = False, needs_ctx: bool = False):  # type: ignore[no-untyped-def]
    def deco(f: Callable[..., Any]) -> Callable[..., Any]:
        FUNCTIONS[name.lower()] = Fn(f, min_args, min_args if max_args is ... else max_args, doc or (f.__doc__ or "").strip(), lazy, needs_ctx)  # type: ignore[arg-type]
        return f
    return deco


# --- logic / comparison
@_fn("equals", 2, doc="true when both values are equal (strict about types: 1 != '1')")
def _equals(a: Any, b: Any) -> bool:
    return equal(a, b)


@_fn("not", 1, doc="boolean negation")
def _not(a: Any) -> bool:
    return not to_bool(a, "not")


@_fn("and", 2, None, doc="true when all arguments are true (short-circuit)", lazy=True)
def _and(*thunks: Callable[[], Any]) -> bool:
    return all(to_bool(t(), "and") for t in thunks)


@_fn("or", 2, None, doc="true when any argument is true (short-circuit)", lazy=True)
def _or(*thunks: Callable[[], Any]) -> bool:
    return any(to_bool(t(), "or") for t in thunks)


@_fn("if", 3, doc="if(condition, whenTrue, whenFalse): only the chosen branch is evaluated", lazy=True)
def _if(c: Callable[[], Any], a: Callable[[], Any], b: Callable[[], Any]) -> Any:
    return a() if to_bool(c(), "if") else b()


@_fn("coalesce", 1, None, doc="first argument that is not null", lazy=True)
def _coalesce(*thunks: Callable[[], Any]) -> Any:
    for t in thunks:
        v = t()
        if v is not None:
            return v
    return None


@_fn("less", 2)
def _less(a: Any, b: Any) -> bool:
    return _compare(a, b, "less") < 0


@_fn("lessorequals", 2)
def _le(a: Any, b: Any) -> bool:
    return _compare(a, b, "lessOrEquals") <= 0


@_fn("greater", 2)
def _greater(a: Any, b: Any) -> bool:
    return _compare(a, b, "greater") > 0


@_fn("greaterorequals", 2)
def _ge(a: Any, b: Any) -> bool:
    return _compare(a, b, "greaterOrEquals") >= 0


@_fn("empty", 1, doc="true for null, '', [] and {}")
def _empty(a: Any) -> bool:
    return a is None or (isinstance(a, (str, list, dict)) and len(a) == 0)


# --- math
def _num_result(x: float) -> int | float:
    return int(x) if isinstance(x, float) and x.is_integer() and abs(x) < 1e15 else x


@_fn("add", 2, None)
def _add(*a: Any) -> int | float:
    return _num_result(sum(to_number(x, "add") for x in a))


@_fn("sub", 2)
def _sub(a: Any, b: Any) -> int | float:
    return to_number(a, "sub") - to_number(b, "sub")


@_fn("mul", 2, None)
def _mul(*a: Any) -> int | float:
    r: int | float = 1
    for x in a:
        r *= to_number(x, "mul")
    return r


@_fn("div", 2, doc="division; integer operands divide with truncation toward zero (like .NET)")
def _div(a: Any, b: Any) -> int | float:
    x, y = to_number(a, "div"), to_number(b, "div")
    if y == 0:
        raise ExpressionError("division by zero", code="math_error")
    if isinstance(x, int) and isinstance(y, int):
        q = abs(x) // abs(y)
        return q if (x < 0) == (y < 0) else -q
    return x / y


@_fn("mod", 2)
def _mod(a: Any, b: Any) -> int | float:
    x, y = to_number(a, "mod"), to_number(b, "mod")
    if y == 0:
        raise ExpressionError("modulo by zero", code="math_error")
    r = math.fmod(x, y)
    return int(r) if isinstance(x, int) and isinstance(y, int) else r


@_fn("min", 1, None)
def _min(*a: Any) -> Any:
    vals = a[0] if len(a) == 1 and isinstance(a[0], list) else list(a)
    if not vals:
        raise ExpressionError("min of an empty collection", code="math_error")
    return min(to_number(x, "min") for x in vals)


@_fn("max", 1, None)
def _max(*a: Any) -> Any:
    vals = a[0] if len(a) == 1 and isinstance(a[0], list) else list(a)
    if not vals:
        raise ExpressionError("max of an empty collection", code="math_error")
    return max(to_number(x, "max") for x in vals)


@_fn("abs", 1)
def _abs(a: Any) -> int | float:
    return abs(to_number(a, "abs"))


@_fn("round", 1, 2)
def _round(a: Any, digits: Any = 0) -> int | float:
    r = round(to_number(a, "round"), int(to_number(digits, "round")))
    return _num_result(r)


@_fn("floor", 1)
def _floor(a: Any) -> int:
    return math.floor(to_number(a, "floor"))


@_fn("ceil", 1)
def _ceil(a: Any) -> int:
    return math.ceil(to_number(a, "ceil"))


@_fn("range", 2, doc="range(start, count): integers start .. start+count-1")
def _range(start: Any, count: Any) -> list[int]:
    s, c = int(to_number(start, "range")), int(to_number(count, "range"))
    if c < 0 or c > MAX_RANGE:
        raise ExpressionError(f"range count must be between 0 and {MAX_RANGE}", code="math_error")
    return list(range(s, s + c))


# --- conversion
@_fn("string", 1)
def _string(a: Any) -> str:
    return to_str(a)


@_fn("int", 1)
def _int(a: Any) -> int:
    if isinstance(a, float):
        return int(a)
    return int(to_number(a, "int"))


@_fn("float", 1)
def _float(a: Any) -> float:
    return float(to_number(a, "float"))


@_fn("bool", 1)
def _bool(a: Any) -> bool:
    if isinstance(a, (int, float)) and not isinstance(a, bool):
        return a != 0
    return to_bool(a, "bool")


@_fn("json", 1, doc="parse a JSON string into a value")
def _json(a: Any) -> Any:
    if not isinstance(a, str):
        return a
    try:
        return json.loads(a)
    except ValueError as exc:
        raise ExpressionError(f"json(): invalid JSON ({exc})", code="type_error") from None


@_fn("array", 1)
def _array(a: Any) -> list[Any]:
    return a if isinstance(a, list) else [a]


@_fn("createarray", 1, None)
def _createarray(*a: Any) -> list[Any]:
    return list(a)


@_fn("object", 0, None, doc="object('k1', v1, 'k2', v2) builds an object")
def _object(*a: Any) -> dict[str, Any]:
    if len(a) % 2:
        raise ExpressionError("object() needs key/value pairs", code="type_error")
    return {_need_str(a[i], "object"): a[i + 1] for i in range(0, len(a), 2)}


# --- strings
@_fn("concat", 1, None, doc="concatenates strings (or arrays when every argument is an array)")
def _concat(*a: Any) -> Any:
    if all(isinstance(x, list) for x in a):
        return [y for x in a for y in x]
    s = "".join(to_str(x) for x in a)
    if len(s) > MAX_RESULT_CHARS:
        raise ExpressionError("concat result is too large", code="limit")
    return s


@_fn("substring", 2, 3, doc="substring(text, start[, length])")
def _substring(s: Any, start: Any, length: Any = None) -> str:
    t, i = _need_str(s, "substring"), int(to_number(start, "substring"))
    if i < 0 or i > len(t):
        raise ExpressionError(f"substring start {i} is outside the string (length {len(t)})", code="range_error")
    if length is None:
        return t[i:]
    ln = int(to_number(length, "substring"))
    if ln < 0 or i + ln > len(t):
        raise ExpressionError(f"substring length {ln} from {i} runs past the end of the string (length {len(t)})", code="range_error")
    return t[i:i + ln]


@_fn("replace", 3)
def _replace(s: Any, old: Any, new: Any) -> str:
    return _need_str(s, "replace").replace(_need_str(old, "replace"), _need_str(new, "replace"))


@_fn("tolower", 1)
def _lower(s: Any) -> str:
    return _need_str(s, "toLower").lower()


@_fn("toupper", 1)
def _upper(s: Any) -> str:
    return _need_str(s, "toUpper").upper()


@_fn("trim", 1)
def _trim(s: Any) -> str:
    return _need_str(s, "trim").strip()


@_fn("split", 2)
def _split(s: Any, d: Any) -> list[str]:
    delim = _need_str(d, "split")
    if not delim:
        raise ExpressionError("split delimiter must not be empty", code="range_error")
    return _need_str(s, "split").split(delim)


@_fn("join", 2)
def _join(a: Any, d: Any) -> str:
    return _need_str(d, "join").join(to_str(x) for x in _need_list(a, "join"))


@_fn("startswith", 2, doc="case-insensitive prefix test")
def _startswith(s: Any, p: Any) -> bool:
    return _need_str(s, "startsWith").lower().startswith(_need_str(p, "startsWith").lower())


@_fn("endswith", 2, doc="case-insensitive suffix test")
def _endswith(s: Any, p: Any) -> bool:
    return _need_str(s, "endsWith").lower().endswith(_need_str(p, "endsWith").lower())


@_fn("indexof", 2, doc="case-insensitive index of a substring or -1")
def _indexof(s: Any, p: Any) -> int:
    return _need_str(s, "indexOf").lower().find(_need_str(p, "indexOf").lower())


@_fn("lastindexof", 2)
def _lastindexof(s: Any, p: Any) -> int:
    return _need_str(s, "lastIndexOf").lower().rfind(_need_str(p, "lastIndexOf").lower())


@_fn("base64", 1)
def _base64(s: Any) -> str:
    return base64.b64encode(_need_str(s, "base64").encode()).decode()


@_fn("base64tostring", 1)
def _b64s(s: Any) -> str:
    try:
        return base64.b64decode(_need_str(s, "base64ToString"), validate=True).decode()
    except ValueError:
        raise ExpressionError("base64ToString(): invalid base64 input", code="type_error") from None


@_fn("uricomponent", 1)
def _uricomponent(s: Any) -> str:
    return quote(_need_str(s, "uriComponent"), safe="")


@_fn("uricomponenttostring", 1)
def _uricomponenttostring(s: Any) -> str:
    return unquote(_need_str(s, "uriComponentToString"))


@_fn("guid", 0, 1)
def _guid(fmt: Any = None) -> str:
    u = str(uuid.uuid4())
    f = "D" if fmt is None else _need_str(fmt, "guid").upper()
    if f == "D":
        return u
    if f == "N":
        return u.replace("-", "")
    if f == "B":
        return "{" + u + "}"
    if f == "P":
        return "(" + u + ")"
    raise ExpressionError(f"guid format must be one of N, D, B, P (got '{fmt}')", code="argument_error")


# --- collections
@_fn("length", 1)
def _length(a: Any) -> int:
    if isinstance(a, (str, list, dict)):
        return len(a)
    raise ExpressionError(f"length() expects a string, array or object but got {_type_name(a)}", code="type_error")


@_fn("contains", 2, doc="string: case-insensitive substring; array: membership; object: has key")
def _contains(c: Any, v: Any) -> bool:
    if isinstance(c, str):
        return _need_str(v, "contains").lower() in c.lower()
    if isinstance(c, list):
        return any(equal(x, v) for x in c)
    if isinstance(c, dict):
        return _need_str(v, "contains") in c
    raise ExpressionError(f"contains() expects a string, array or object but got {_type_name(c)}", code="type_error")


@_fn("first", 1)
def _first(a: Any) -> Any:
    if isinstance(a, str):
        return a[:1]
    lst = _need_list(a, "first")
    return lst[0] if lst else None


@_fn("last", 1)
def _last(a: Any) -> Any:
    if isinstance(a, str):
        return a[-1:]
    lst = _need_list(a, "last")
    return lst[-1] if lst else None


@_fn("take", 2)
def _take(a: Any, n: Any) -> Any:
    k = max(0, int(to_number(n, "take")))
    return a[:k] if isinstance(a, (str, list)) else _need_list(a, "take")[:k]


@_fn("skip", 2)
def _skip(a: Any, n: Any) -> Any:
    k = max(0, int(to_number(n, "skip")))
    return a[k:] if isinstance(a, (str, list)) else _need_list(a, "skip")[k:]


@_fn("union", 2, None)
def _union(*a: Any) -> Any:
    if all(isinstance(x, dict) for x in a):
        out: dict[str, Any] = {}
        for x in a:
            out.update(x)
        return out
    res: list[Any] = []
    for x in a:
        for y in _need_list(x, "union"):
            if not any(equal(y, z) for z in res):
                res.append(y)
    return res


@_fn("intersection", 2, None)
def _intersection(*a: Any) -> Any:
    lists = [_need_list(x, "intersection") for x in a]
    return [y for y in lists[0] if all(any(equal(y, z) for z in other) for other in lists[1:])]


@_fn("reverse", 1)
def _reverse(a: Any) -> Any:
    return a[::-1] if isinstance(a, (str, list)) else _need_list(a, "reverse")[::-1]


@_fn("getproperty", 2)
def _getproperty(o: Any, k: Any) -> Any:
    d = _need_dict(o, "getProperty")
    key = _need_str(k, "getProperty")
    if key not in d:
        raise ExpressionError(f"getProperty(): object has no property {key!r}", code="missing_property")
    return d[key]


@_fn("setproperty", 3)
def _setproperty(o: Any, k: Any, v: Any) -> dict[str, Any]:
    d = dict(_need_dict(o, "setProperty"))
    d[_need_str(k, "setProperty")] = v
    return d


@_fn("removeproperty", 2)
def _removeproperty(o: Any, k: Any) -> dict[str, Any]:
    d = dict(_need_dict(o, "removeProperty"))
    d.pop(_need_str(k, "removeProperty"), None)
    return d


@_fn("keys", 1)
def _keys(o: Any) -> list[str]:
    return list(_need_dict(o, "keys"))


@_fn("values", 1)
def _values(o: Any) -> list[Any]:
    return list(_need_dict(o, "values").values())


# --- date/time (functions that read the clock take it from the context, so tests can freeze time)
@_fn("utcnow", 0, 1, doc="current UTC time as ISO 8601 (optionally formatted)", needs_ctx=True)
def _utcnow(ctx: EvalContext, fmt: Any = None) -> str:
    return _dt_out(ctx.now().astimezone(UTC), fmt, "utcNow")


@_fn("formatdatetime", 1, 3, doc="formatDateTime(timestamp[, format[, locale]]) with .NET custom format strings")
def _formatdatetime(ts: Any, fmt: Any = None, locale: Any = None) -> str:
    return _dt_out(parse_dt(ts, "formatDateTime"), fmt, "formatDateTime")


def _adder(name: str, unit: str) -> None:
    @_fn(name, 2, 3, doc=f"{name}(timestamp, amount[, format]) adds {unit}s")
    def _f(ts: Any, n: Any, fmt: Any = None) -> str:
        return _dt_out(_add_unit(parse_dt(ts, name), to_number(n, name), unit, name), fmt, name)


for _n, _u in (("adddays", "day"), ("addhours", "hour"), ("addminutes", "minute"), ("addseconds", "second")):
    _adder(_n, _u)


@_fn("addtotime", 3, 4, doc="addToTime(timestamp, amount, unit[, format])")
def _addtotime(ts: Any, n: Any, unit: Any, fmt: Any = None) -> str:
    return _dt_out(_add_unit(parse_dt(ts, "addToTime"), to_number(n, "addToTime"), _need_str(unit, "addToTime"), "addToTime"), fmt, "addToTime")


@_fn("subtractfromtime", 3, 4)
def _subtractfromtime(ts: Any, n: Any, unit: Any, fmt: Any = None) -> str:
    return _dt_out(_add_unit(parse_dt(ts, "subtractFromTime"), -to_number(n, "subtractFromTime"), _need_str(unit, "subtractFromTime"), "subtractFromTime"), fmt, "subtractFromTime")


@_fn("startofday", 1, 2)
def _startofday(ts: Any, fmt: Any = None) -> str:
    return _dt_out(parse_dt(ts, "startOfDay").replace(hour=0, minute=0, second=0, microsecond=0), fmt, "startOfDay")


@_fn("startofhour", 1, 2)
def _startofhour(ts: Any, fmt: Any = None) -> str:
    return _dt_out(parse_dt(ts, "startOfHour").replace(minute=0, second=0, microsecond=0), fmt, "startOfHour")


@_fn("startofmonth", 1, 2)
def _startofmonth(ts: Any, fmt: Any = None) -> str:
    return _dt_out(parse_dt(ts, "startOfMonth").replace(day=1, hour=0, minute=0, second=0, microsecond=0), fmt, "startOfMonth")


@_fn("dayofweek", 1, doc="0 = Sunday")
def _dayofweek(ts: Any) -> int:
    return (parse_dt(ts, "dayOfWeek").weekday() + 1) % 7


@_fn("dayofmonth", 1)
def _dayofmonth(ts: Any) -> int:
    return parse_dt(ts, "dayOfMonth").day


@_fn("dayofyear", 1)
def _dayofyear(ts: Any) -> int:
    return parse_dt(ts, "dayOfYear").timetuple().tm_yday


@_fn("ticks", 1, doc=".NET ticks (100 ns units since 0001-01-01)")
def _ticks(ts: Any) -> int:
    dt = parse_dt(ts, "ticks").astimezone(UTC)
    delta = dt - datetime(1, 1, 1, tzinfo=UTC)
    return (delta.days * 86400 + delta.seconds) * 10_000_000 + delta.microseconds * 10


@_fn("convertfromutc", 2, 3, doc="convertFromUtc(timestamp, timeZone[, format])")
def _convertfromutc(ts: Any, tz: Any, fmt: Any = None) -> str:
    dt = parse_dt(ts, "convertFromUtc").astimezone(_tz(_need_str(tz, "convertFromUtc"), "convertFromUtc"))
    return format_dt(dt, _need_str(fmt, "convertFromUtc")) if fmt not in (None, "") else dt.isoformat()


@_fn("converttoutc", 2, 3, doc="convertToUtc(localTimestamp, sourceTimeZone[, format])")
def _converttoutc(ts: Any, tz: Any, fmt: Any = None) -> str:
    raw = parse_dt(ts, "convertToUtc")
    local = raw.replace(tzinfo=_tz(_need_str(tz, "convertToUtc"), "convertToUtc"))
    return _dt_out(local.astimezone(UTC), fmt, "convertToUtc")


# --- run state accessors
@_fn("pipeline", 0, doc="run metadata: .parameters, .RunId, .Pipeline, .Workspace, .TriggerType, .TriggerTime, .GroupId", needs_ctx=True)
def _pipeline(ctx: EvalContext) -> dict[str, Any]:
    return {**ctx.pipeline, "parameters": ctx.parameters}


@_fn("parameters", 1, doc="parameters('name')", needs_ctx=True)
def _parameters(ctx: EvalContext, name: Any) -> Any:
    n = _need_str(name, "parameters")
    if n not in ctx.parameters:
        raise ExpressionError(f"parameter '{n}' is not defined", code="unknown_parameter")
    return ctx.parameters[n]


@_fn("variables", 1, doc="variables('name')", needs_ctx=True)
def _variables(ctx: EvalContext, name: Any) -> Any:
    return ctx.get_variable(_need_str(name, "variables"))


@_fn("activity", 1, doc="activity('Name').output / .status / .error of an upstream activity", needs_ctx=True)
def _activity(ctx: EvalContext, name: Any) -> dict[str, Any]:
    n = _need_str(name, "activity")
    a = ctx.activity(n)
    if a is None:
        raise ExpressionError(f"activity '{n}' has not run or is not visible here (it must be upstream of the current activity)", code="unknown_activity")
    return a


@_fn("item", 0, doc="current element inside for_each", needs_ctx=True)
def _item(ctx: EvalContext) -> Any:
    if not ctx.has_item:
        raise ExpressionError("item() is only available inside a for_each activity", code="no_item")
    return ctx.item


@_fn("itemindex", 0, doc="zero-based index of the current for_each element", needs_ctx=True)
def _itemindex(ctx: EvalContext) -> int:
    if not ctx.has_item:
        raise ExpressionError("itemIndex() is only available inside a for_each activity", code="no_item")
    return ctx.item_index or 0


@_fn("trigger", 0, doc="trigger metadata: .type, .name, .scheduledTime, .body, .headers", needs_ctx=True)
def _trigger(ctx: EvalContext) -> dict[str, Any]:
    return ctx.trigger


@_fn("triggerbody", 0, doc="payload of the webhook / event / file-arrival trigger that started the run", needs_ctx=True)
def _triggerbody(ctx: EvalContext) -> Any:
    return ctx.trigger.get("body")


# Documentation of functions whose implementation has no docstring (served by GET /v1/pipelines/activity-types)
_DOCS = {
    "less": "true when the first value is smaller (numbers, strings or datetimes)", "lessorequals": "true when the first value is smaller than or equal to the second",
    "greater": "true when the first value is larger", "greaterorequals": "true when the first value is larger than or equal to the second",
    "add": "sum of the numbers", "sub": "first number minus the second", "mul": "product of the numbers", "mod": "remainder of the division (sign of the dividend)",
    "min": "smallest of the numbers (or of one array)", "max": "largest of the numbers (or of one array)", "abs": "absolute value", "round": "round(x[, digits])",
    "floor": "largest integer not above the number", "ceil": "smallest integer not below the number", "string": "convert a value to its string form",
    "int": "convert a string / number to an integer", "float": "convert a string / number to a floating point number", "bool": "convert 'true' / 'false' / 0 / 1 to a boolean",
    "array": "wrap a value in an array (arrays are returned unchanged)", "createarray": "createArray(a, b, ...) builds an array", "replace": "replace(text, old, new): replace every occurrence",
    "tolower": "lower-case a string", "toupper": "upper-case a string", "trim": "remove leading and trailing whitespace", "split": "split(text, separator) into an array of strings",
    "join": "join(array, separator) into one string", "lastindexof": "position of the last occurrence of a substring (-1 when absent)", "base64": "base64-encode a string (UTF-8)",
    "base64tostring": "decode a base64 string into text", "uricomponent": "percent-encode a string for use in a URL", "uricomponenttostring": "decode a percent-encoded string",
    "guid": "random UUID (optionally formatted: N, D, B, P)", "length": "length of a string, array or object", "first": "first element of an array (or first character of a string)",
    "last": "last element of an array (or last character of a string)", "take": "take(collection, n): the first n elements", "skip": "skip(collection, n): everything after the first n elements",
    "union": "union of arrays (order kept, duplicates removed) or objects (later wins)", "intersection": "elements present in every array", "reverse": "reverse an array or string",
    "getproperty": "getProperty(object, name): read a property by (dynamic) name", "setproperty": "setProperty(object, name, value): copy of the object with the property set",
    "removeproperty": "removeProperty(object, name): copy without the property", "keys": "property names of an object", "values": "property values of an object",
    "subtractfromtime": "subtractFromTime(timestamp, n, unit[, format])", "startofday": "midnight of the day of the timestamp", "startofhour": "start of the hour of the timestamp",
    "startofmonth": "first instant of the month of the timestamp", "dayofmonth": "day of the month (1-31)", "dayofyear": "day of the year (1-366)",
}
for _name, _doc in _DOCS.items():
    if _name in FUNCTIONS and not FUNCTIONS[_name].doc:
        FUNCTIONS[_name] = Fn(FUNCTIONS[_name].fn, FUNCTIONS[_name].min_args, FUNCTIONS[_name].max_args, _doc, FUNCTIONS[_name].lazy, FUNCTIONS[_name].needs_ctx)


# ---------------------------------------------------------------------------------------------------------------------------------------- evaluation
def _evaluate(node: Any, ctx: EvalContext, src: str) -> Any:
    ctx.steps += 1
    if ctx.steps > MAX_STEPS:
        raise ExpressionError("expression exceeded the evaluation step limit", code="limit")
    if isinstance(node, Lit):
        return node.value
    if isinstance(node, Call):
        f = FUNCTIONS.get(node.name)
        if f is None:
            raise ExpressionError(f"unknown function '{node.name}'", pos=node.pos, source=src, code="unknown_function")
        n = len(node.args)
        if n < f.min_args or (f.max_args is not None and n > f.max_args):
            want = f"{f.min_args}" if f.max_args == f.min_args else f"{f.min_args}..{'n' if f.max_args is None else f.max_args}"
            raise ExpressionError(f"function '{node.name}' takes {want} argument(s) but got {n}", pos=node.pos, source=src, code="arity")
        try:
            if f.lazy:
                return f.fn(*[(lambda a=a: _evaluate(a, ctx, src)) for a in node.args])
            vals = [_evaluate(a, ctx, src) for a in node.args]
            return f.fn(ctx, *vals) if f.needs_ctx else f.fn(*vals)
        except ExpressionError as exc:
            if exc.pos is None:
                raise ExpressionError(exc.message, pos=node.pos, source=src, code=exc.code) from None
            raise
        except RecursionError:
            raise ExpressionError("expression is too deeply nested", pos=node.pos, source=src, code="limit") from None
    if isinstance(node, Member):
        obj = _evaluate(node.obj, ctx, src)
        if obj is None and node.optional:
            return None
        if not isinstance(obj, dict):
            raise ExpressionError(f"cannot read property '{node.name}' of {_type_name(obj)}", pos=node.pos, source=src, code="type_error")
        if node.name in obj:
            return obj[node.name]
        low = {str(k).lower(): k for k in obj}
        if node.name.lower() in low and node.name[:1].isupper():  # Data Factory accessors are written .RunId / .Pipeline; accept either case for those
            return obj[low[node.name.lower()]]
        if node.optional:
            return None
        avail = ", ".join(sorted(map(str, obj))[:12]) or "none"
        raise ExpressionError(f"property '{node.name}' does not exist (available: {avail}); use '?.' to read optional properties", pos=node.pos, source=src, code="missing_property")
    if isinstance(node, Index):
        obj = _evaluate(node.obj, ctx, src)
        idx = _evaluate(node.index, ctx, src)
        if isinstance(obj, list):
            if isinstance(idx, bool) or not isinstance(idx, (int, float)) or int(idx) != idx:
                raise ExpressionError(f"array index must be an integer, got {_type_name(idx)}", pos=node.pos, source=src, code="type_error")
            if not 0 <= int(idx) < len(obj):
                raise ExpressionError(f"array index {int(idx)} is out of range (length {len(obj)})", pos=node.pos, source=src, code="range_error")
            return obj[int(idx)]
        if isinstance(obj, dict):
            k = to_str(idx)
            if k not in obj:
                raise ExpressionError(f"key {k!r} does not exist", pos=node.pos, source=src, code="missing_property")
            return obj[k]
        if isinstance(obj, str):
            i = int(to_number(idx, "index"))
            if not 0 <= i < len(obj):
                raise ExpressionError(f"string index {i} is out of range (length {len(obj)})", pos=node.pos, source=src, code="range_error")
            return obj[i]
        raise ExpressionError(f"cannot index into {_type_name(obj)}", pos=node.pos, source=src, code="type_error")
    if isinstance(node, Unary):
        v = _evaluate(node.operand, ctx, src)
        try:
            return (not to_bool(v, "!")) if node.op == "!" else -to_number(v, "unary -")
        except ExpressionError as exc:
            raise ExpressionError(exc.message, pos=node.pos, source=src, code=exc.code) from None
    if isinstance(node, Binary):
        op = node.op
        try:
            if op == "&&":
                return to_bool(_evaluate(node.left, ctx, src), "&&") and to_bool(_evaluate(node.right, ctx, src), "&&")
            if op == "||":
                return to_bool(_evaluate(node.left, ctx, src), "||") or to_bool(_evaluate(node.right, ctx, src), "||")
            a, b = _evaluate(node.left, ctx, src), _evaluate(node.right, ctx, src)
            if op == "==":
                return equal(a, b)
            if op == "!=":
                return not equal(a, b)
            if op in ("<", "<=", ">", ">="):
                c = _compare(a, b, op)
                return {"<": c < 0, "<=": c <= 0, ">": c > 0, ">=": c >= 0}[op]
            if op == "+":
                if isinstance(a, str) or isinstance(b, str):
                    return _concat(a, b)
                return _num_result(to_number(a, "+") + to_number(b, "+"))
            if op == "-":
                return _sub(a, b)
            if op == "*":
                return _mul(a, b)
            if op == "/":
                return _div(a, b)
            return _mod(a, b)
        except ExpressionError as exc:
            if exc.pos is None:
                raise ExpressionError(exc.message, pos=node.pos, source=src, code=exc.code) from None
            raise
    raise ExpressionError(f"internal: unknown node {node!r}")  # pragma: no cover


def evaluate_template(s: str, ctx: EvalContext) -> Any:
    """Evaluate one string value. Raises :class:`ExpressionError`."""
    tpl = compile_template(s)
    if tpl.whole:
        return _check_size(_evaluate(tpl.parts[0], ctx, s[1:]), s)
    if tpl.is_literal:
        return "".join(tpl.parts)  # type: ignore[arg-type]
    out: list[str] = []
    for p in tpl.parts:
        out.append(p if isinstance(p, str) else to_str(_evaluate(p, ctx, s)))
    return _check_size("".join(out), s)


def _check_size(v: Any, src: str) -> Any:
    if isinstance(v, str) and len(v) > MAX_RESULT_CHARS:
        raise ExpressionError("expression result is too large", source=src[:40], code="limit")
    return v


def evaluate_value(v: Any, ctx: EvalContext, *, _depth: int = 0) -> Any:
    """Deep-evaluate a JSON-like value: every string/Expression-object inside is evaluated, structure is preserved."""
    if _depth > 40:
        raise ExpressionError("value is nested too deeply", code="limit")
    if isinstance(v, str):
        return evaluate_template(v, ctx) if (v.startswith("@") or "@{" in v) else v
    if isinstance(v, dict):
        if is_expression_value(v):
            return evaluate_template(v["value"] if v["value"].startswith("@") else "@" + v["value"], ctx)
        return {k: evaluate_value(x, ctx, _depth=_depth + 1) for k, x in v.items()}
    if isinstance(v, list):
        return [evaluate_value(x, ctx, _depth=_depth + 1) for x in v]
    return v


# ---------------------------------------------------------------------------------------------------------------------------------------- static analysis
@dataclass
class References:
    """What an expression reads (only literal arguments can be resolved statically)."""

    activities: set[str] = field(default_factory=set)
    variables: set[str] = field(default_factory=set)
    parameters: set[str] = field(default_factory=set)
    uses_item: bool = False
    uses_trigger: bool = False
    dynamic: bool = False  # non-literal activity/variable/parameter name


def analyze(node: Any, refs: References | None = None, *, src: str = "") -> References:
    """Walk an AST: validate function names/arity (raises :class:`ExpressionError`) and collect :class:`References`."""
    refs = refs or References()
    if isinstance(node, Lit):
        return refs
    if isinstance(node, Call):
        f = FUNCTIONS.get(node.name)
        if f is None:
            raise ExpressionError(f"unknown function '{node.name}'", pos=node.pos, source=src, code="unknown_function")
        n = len(node.args)
        if n < f.min_args or (f.max_args is not None and n > f.max_args):
            want = f"{f.min_args}" if f.max_args == f.min_args else f"{f.min_args}..{'n' if f.max_args is None else f.max_args}"
            raise ExpressionError(f"function '{node.name}' takes {want} argument(s) but got {n}", pos=node.pos, source=src, code="arity")
        for a in node.args:
            analyze(a, refs, src=src)
        lit0 = node.args[0].value if node.args and isinstance(node.args[0], Lit) and isinstance(node.args[0].value, str) else None
        if node.name in ("activity", "variables", "parameters"):
            target = {"activity": refs.activities, "variables": refs.variables, "parameters": refs.parameters}[node.name]
            if lit0 is None:
                refs.dynamic = True
            else:
                target.add(lit0)
        elif node.name in ("item", "itemindex"):
            refs.uses_item = True
        elif node.name in ("trigger", "triggerbody"):
            refs.uses_trigger = True
        return refs
    if isinstance(node, Member):
        analyze(node.obj, refs, src=src)
        if isinstance(node.obj, Call) and node.obj.name == "pipeline" and node.name == "parameters":
            pass  # handled by the Member-of-Member case below
        if isinstance(node.obj, Member) and isinstance(node.obj.obj, Call) and node.obj.obj.name == "pipeline" and node.obj.name == "parameters":
            refs.parameters.add(node.name)
        return refs
    if isinstance(node, Index):
        analyze(node.obj, refs, src=src)
        analyze(node.index, refs, src=src)
        if isinstance(node.obj, Member) and isinstance(node.obj.obj, Call) and node.obj.obj.name == "pipeline" and node.obj.name == "parameters":
            if isinstance(node.index, Lit) and isinstance(node.index.value, str):
                refs.parameters.add(node.index.value)
            else:
                refs.dynamic = True
        return refs
    if isinstance(node, Unary):
        return analyze(node.operand, refs, src=src)
    if isinstance(node, Binary):
        analyze(node.left, refs, src=src)
        return analyze(node.right, refs, src=src)
    return refs


def analyze_value(v: Any, refs: References | None = None) -> References:
    """Static analysis of every expression inside a JSON-like value. Raises :class:`ExpressionError` with the offending string."""
    refs = refs or References()
    if isinstance(v, str):
        if v.startswith("@") or "@{" in v:
            tpl = compile_template(v)
            for p in tpl.parts:
                if not isinstance(p, str):
                    analyze(p, refs, src=v[1:] if tpl.whole else v)
    elif isinstance(v, dict):
        if is_expression_value(v):
            analyze_value(v["value"] if v["value"].startswith("@") else "@" + v["value"], refs)
        else:
            for x in v.values():
                analyze_value(x, refs)
    elif isinstance(v, list):
        for x in v:
            analyze_value(x, refs)
    return refs


__all__ = [
    "FUNCTIONS", "EvalContext", "ExpressionError", "References", "Template", "analyze", "analyze_value", "compile_template", "contains_expression",
    "equal", "evaluate_template", "evaluate_value", "format_dt", "is_expression_value", "iso_z", "parse", "parse_dt", "to_bool", "to_number", "to_str",
]
