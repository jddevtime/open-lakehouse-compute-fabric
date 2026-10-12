# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Front-end for Python cells: split IPython syntax (magics, shell escapes) from Python so ``ast`` can parse the rest with exact positions.

Notebook cells are not valid Python: ``%pip install x``, ``%run other``, ``!ls`` and ``%%sql`` are IPython constructs. :func:`split_ipython`
replaces each such *line* by ``pass`` (keeping indentation and the line count, so every AST position still maps 1:1 onto the original cell)
and returns the removed constructs separately. A tiny scanner tracks strings and brackets so that a ``%`` or ``!`` inside a triple-quoted
string or a multi-line call is never mistaken for a magic.
"""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass
class LineMagic:
    """An IPython line construct removed from the Python text: ``kind`` is ``magic`` (``%name args``) or ``shell`` (``!cmd``)."""

    line: int  # 1-based, relative to the cell
    col: int
    kind: str
    name: str
    args: str
    text: str


_MAGIC = re.compile(r"^(?P<indent>[ \t]*)(?:(?P<target>[A-Za-z_][\w.\[\]'\", ]*?)\s*=\s*)?%(?P<name>[A-Za-z_]\w*)(?P<args>.*)$")
_SHELL = re.compile(r"^(?P<indent>[ \t]*)(?:(?P<target>[A-Za-z_][\w.\[\]'\", ]*?)\s*=\s*)?!(?P<cmd>.*)$")
_CELL_MAGIC = re.compile(r"^\s*%%(?P<name>[A-Za-z_]\w*)(?P<args>[^\n]*)$")


def cell_magic(source: str) -> tuple[str, str, int] | None:
    """If the first non-blank line is ``%%name args`` return ``(name, args, number_of_lines_before_the_body)``."""
    lines = source.split("\n")
    for i, ln in enumerate(lines):
        if not ln.strip():
            continue
        m = _CELL_MAGIC.match(ln)
        return (m.group("name"), m.group("args").strip(), i + 1) if m else None
    return None


class _State:
    """Lexical state carried from line to line: open triple-quote, bracket depth, pending backslash continuation."""

    def __init__(self) -> None:
        self.triple: str | None = None
        self.depth = 0
        self.cont = False

    @property
    def at_statement_start(self) -> bool:
        return self.triple is None and self.depth == 0 and not self.cont

    def feed(self, line: str) -> None:
        i, n = 0, len(line)
        self.cont = False
        while i < n:
            if self.triple:
                j = line.find(self.triple, i)
                if j == -1:
                    return
                i = j + 3
                self.triple = None
                continue
            ch = line[i]
            if ch == "#":
                return
            if ch in "\"'":
                prefix_raw = i > 0 and line[i - 1] in "rRbBfF" and "r" in line[max(0, i - 2):i].lower()
                if line.startswith(ch * 3, i):
                    self.triple = ch * 3
                    i += 3
                    continue
                j = i + 1
                while j < n:
                    if line[j] == "\\" and not prefix_raw:
                        j += 2
                        continue
                    if line[j] == ch:
                        break
                    j += 1
                i = j + 1
                continue
            if ch in "([{":
                self.depth += 1
            elif ch in ")]}":
                self.depth = max(0, self.depth - 1)
            i += 1
        if line.rstrip().endswith("\\"):
            self.cont = True


def split_ipython(source: str) -> tuple[str, list[LineMagic]]:
    """Return ``(python_text, line_magics)``; ``python_text`` has the same number of lines as ``source``."""
    out: list[str] = []
    found: list[LineMagic] = []
    st = _State()
    for n, line in enumerate(source.split("\n"), start=1):
        replaced = None
        if st.at_statement_start:
            m = _MAGIC.match(line)
            if m and not line.lstrip().startswith("%%"):
                found.append(LineMagic(n, len(m.group("indent")), "magic", m.group("name"), m.group("args").strip(), line.strip()))
                tgt = m.group("target")
                replaced = f"{m.group('indent')}{tgt} = None" if tgt else f"{m.group('indent')}pass"
            else:
                s = _SHELL.match(line)
                if s and not line.lstrip().startswith("!="):
                    found.append(LineMagic(n, len(s.group("indent")), "shell", "!", s.group("cmd").strip(), line.strip()))
                    tgt = s.group("target")
                    replaced = f"{s.group('indent')}{tgt} = None" if tgt else f"{s.group('indent')}pass"
        if replaced is not None:
            out.append(replaced)
            # a magic never opens strings/brackets we care about; keep state unchanged
        else:
            out.append(line)
            st.feed(line)
    return "\n".join(out), found
