# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Reference extraction for DAX expressions (measures, calculated columns, calculated tables).

:func:`extract_refs` ``(expression) -> DaxRefs`` tokenizes DAX (string literals, ``'Quoted Table'`` names, ``[Column or Measure]`` brackets, ``//``, ``--`` and ``/* */`` comments,
identifiers, numbers) and lists what the expression refers to:

* ``Table[Column]`` / ``'Table Name'[Column]`` -> a *qualified* reference (a column, or a measure written with its home table: the model decides);
* ``[Name]`` -> an *unqualified* reference (a measure, or a column of the table in row context);
* ``Table`` / ``'Table Name'`` that is neither followed by ``[`` nor by ``(`` and is not a ``VAR`` name -> a *table* reference (``COUNTROWS(Sales)``, ``FILTER(Sales, ...)``).

``VAR`` names are tracked so that ``RETURN x`` is not mistaken for a table. The extractor never evaluates anything and never fails: unbalanced quotes or brackets produce
``problems`` entries and the references found before the problem are still returned. Resolution against a model (is ``Sales[Total]`` a column or a measure? which table does ``[Qty]``
belong to?) is done by :mod:`olcf.intelligence.lineage.semantic_model`, which knows the model.

Not interpreted: what a function does with its arguments (a column used only inside ``CALCULATE`` filters is still reported as referenced, since changing it changes the result),
``EVALUATE`` query semantics, calculation-group items' ``SELECTEDMEASURE()`` indirection, and references hidden in string literals (``EVALUATE`` of a dynamic string through ``EXPRESSION``).
"""

from __future__ import annotations

from dataclasses import dataclass, field

MAX_DAX_CHARS = 500_000
_KEYWORDS = frozenset({"var", "return", "in", "not", "order", "by", "desc", "asc", "evaluate", "define", "measure", "column", "table", "start", "at", "true", "false", "and", "or", "if",
                       "null", "blank"})


@dataclass(frozen=True)
class Tok:
    """A DAX token: ``kind`` is ``str`` | ``qname`` | ``bracket`` | ``ident`` | ``num`` | ``op``; ``value`` the unquoted text; ``line`` is 1-based."""

    kind: str
    value: str
    line: int
    pos: int


@dataclass(frozen=True)
class DaxRef:
    """One reference found in a DAX expression: ``kind`` is ``qualified`` | ``unqualified`` | ``table``; ``table`` / ``name`` as written (unquoted)."""

    kind: str
    table: str | None
    name: str | None
    line: int


@dataclass
class DaxRefs:
    """Result of :func:`extract_refs`: the de-duplicated references in order of first appearance, the ``VAR`` names and any tokenizer problems."""

    refs: list[DaxRef] = field(default_factory=list)
    variables: set[str] = field(default_factory=set)
    problems: list[str] = field(default_factory=list)


def tokenize(text: str) -> tuple[list[Tok], list[str]]:
    """Split DAX into tokens (comments dropped). Returns ``(tokens, problems)``."""
    toks: list[Tok] = []
    problems: list[str] = []
    i, n, line = 0, len(text), 1
    while i < n:
        ch = text[i]
        if ch == "\n":
            line += 1
            i += 1
        elif ch in " \t\r":
            i += 1
        elif text.startswith("//", i) or text.startswith("--", i):
            j = text.find("\n", i)
            i = n if j < 0 else j
        elif text.startswith("/*", i):
            j = text.find("*/", i + 2)
            if j < 0:
                problems.append(f"unterminated comment at line {line}")
                break
            line += text.count("\n", i, j)
            i = j + 2
        elif ch == '"':
            j, buf = i + 1, []
            while j < n:
                if text[j] == '"':
                    if j + 1 < n and text[j + 1] == '"':
                        buf.append('"')
                        j += 2
                        continue
                    break
                buf.append(text[j])
                j += 1
            else:
                problems.append(f"unterminated string at line {line}")
                break
            toks.append(Tok("str", "".join(buf), line, i))
            line += text.count("\n", i, j)
            i = j + 1
        elif ch == "'":
            j, buf = i + 1, []
            while j < n:
                if text[j] == "'":
                    if j + 1 < n and text[j + 1] == "'":
                        buf.append("'")
                        j += 2
                        continue
                    break
                buf.append(text[j])
                j += 1
            else:
                problems.append(f"unterminated quoted name at line {line}")
                break
            toks.append(Tok("qname", "".join(buf), line, i))
            line += text.count("\n", i, j)
            i = j + 1
        elif ch == "[":
            j, buf = i + 1, []
            while j < n:
                if text[j] == "]":
                    if j + 1 < n and text[j + 1] == "]":
                        buf.append("]")
                        j += 2
                        continue
                    break
                buf.append(text[j])
                j += 1
            else:
                problems.append(f"unterminated [ at line {line}")
                break
            toks.append(Tok("bracket", "".join(buf), line, i))
            line += text.count("\n", i, j)
            i = j + 1
        elif ch.isalpha() or ch == "_":
            j = i + 1
            while j < n and (text[j].isalnum() or text[j] in "_."):
                j += 1
            word = text[i:j].rstrip(".")
            toks.append(Tok("ident", word, line, i))
            i += len(word)
        elif ch.isdigit():
            j = i + 1
            while j < n and (text[j].isalnum() or text[j] == "."):
                j += 1
            toks.append(Tok("num", text[i:j], line, i))
            i = j
        else:
            toks.append(Tok("op", ch, line, i))
            i += 1
    return toks, problems


def extract_refs(expression: str) -> DaxRefs:
    """List the references of a DAX expression, see the module docstring."""
    out = DaxRefs()
    if len(expression) > MAX_DAX_CHARS:
        out.problems.append(f"expression longer than {MAX_DAX_CHARS} characters was not analysed")
        return out
    toks, out.problems = tokenize(expression)
    # VAR names first (a variable may be used before its definition is seen only in invalid DAX, but RETURN-less queries need the set up front)
    for i, t in enumerate(toks[:-1]):
        if t.kind == "ident" and t.value.lower() == "var" and toks[i + 1].kind == "ident":
            out.variables.add(toks[i + 1].value.lower())
    seen: set[tuple[str, str | None, str | None]] = set()

    def add(kind: str, table: str | None, name: str | None, line: int) -> None:
        key = (kind, (table or "").lower() or None, (name or "").lower() or None)
        if key not in seen:
            seen.add(key)
            out.refs.append(DaxRef(kind, table, name, line))

    i, n = 0, len(toks)
    while i < n:
        t = toks[i]
        nxt = toks[i + 1] if i + 1 < n else None
        if t.kind in ("ident", "qname"):
            is_call = nxt is not None and nxt.kind == "op" and nxt.value == "("
            if t.kind == "ident" and (t.value.lower() in _KEYWORDS or is_call):
                i += 1
                continue
            if nxt is not None and nxt.kind == "bracket":
                add("qualified", t.value, nxt.value, t.line)
                i += 2
                continue
            if t.kind == "ident" and t.value.lower() in out.variables:
                i += 1
                continue
            if t.kind == "qname" and is_call:
                i += 1
                continue
            add("table", t.value, None, t.line)
        elif t.kind == "bracket":
            add("unqualified", None, t.value, t.line)
        i += 1
    return out
