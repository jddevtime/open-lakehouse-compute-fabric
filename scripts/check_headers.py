#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Check (and optionally add) the licence header of every Python source file.

Every ``*.py`` file under ``src/``, ``sdk/``, ``scripts/``, ``tests/``, ``examples/`` and ``deploy/`` must start with::

    # SPDX-License-Identifier: Apache-2.0
    # Copyright (c) 2026 Justin Dodrill

(after an optional ``#!`` line and an optional encoding comment).  Empty ``__init__.py`` files are not exempt: the header
is two comment lines and costs nothing.

Usage::

    python scripts/check_headers.py              # report files that lack the header, exit 1 when there are any
    python scripts/check_headers.py --fix        # insert the header mechanically (nothing else in the file changes)
    python scripts/check_headers.py PATH ...     # restrict to files or directories

Fabric ``notebook-content.py`` fixtures (``tests/corpus``) are exempt.  ``--fix`` only ever inserts the two comment lines; it never rewrites an existing, different header (those are reported
for a human to look at).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DIRS = ("src", "sdk", "scripts", "tests", "examples", "deploy")
SPDX = "# SPDX-License-Identifier: Apache-2.0"
COPYRIGHT = "# Copyright (c) 2026 Justin Dodrill"
#: Test fixtures whose first line is dictated by an external format (the Fabric ``fabricGitSource`` notebook export starts with
#: ``# Fabric notebook source``); they are data, not OLCF source files.
EXEMPT_NAMES = {"notebook-content.py"}
SKIP_PARTS = {".venv", "__pycache__", ".git", "node_modules", "build", "dist", ".mypy_cache", ".ruff_cache", ".hypothesis", ".pytest_cache"}


def python_files(paths: list[Path]) -> list[Path]:
    """All ``*.py`` files below ``paths`` (files are taken as given), without caches and virtual environments."""
    found: list[Path] = []
    for p in paths:
        if p.is_file():
            if p.suffix == ".py" and p.name not in EXEMPT_NAMES:
                found.append(p)
            continue
        for f in sorted(p.rglob("*.py")):
            if f.name not in EXEMPT_NAMES and not SKIP_PARTS.intersection(f.relative_to(p).parts) and not SKIP_PARTS.intersection(f.parts[: len(p.parts)]):
                found.append(f)
    return found


def header_state(text: str) -> tuple[str, int]:
    """Classify a file: ``ok``, ``missing`` or ``different`` and the line index at which a header would be inserted."""
    lines = text.splitlines()
    i = 0
    if lines and lines[0].startswith("#!"):
        i = 1
    if i < len(lines) and lines[i].startswith("#") and "coding" in lines[i]:
        i += 1
    head = [ln.strip() for ln in lines[i : i + 4]]
    has_spdx = any(ln.startswith("# SPDX-License-Identifier:") for ln in head)
    has_copy = any(ln.startswith("# Copyright") for ln in head)
    if SPDX in head and COPYRIGHT in head:
        return "ok", i
    if has_spdx or has_copy:
        return "different", i
    return "missing", i


def add_header(path: Path, at: int) -> None:
    """Insert the two header lines at line index ``at``, keeping the file's newline style and everything else byte-identical."""
    raw = path.read_bytes()
    text = raw.decode("utf-8")
    nl = "\r\n" if "\r\n" in text else "\n"
    lines = text.splitlines(keepends=True)
    block = [SPDX + nl, COPYRIGHT + nl]
    # a header must not glue itself to a following docstring or code line without a blank line
    if at < len(lines) and lines[at].strip() and not lines[at].startswith("#"):
        block.append(nl)
    elif at < len(lines) and lines[at].startswith("#") and "SPDX" not in lines[at]:
        block.append("#" + nl)
    if at >= 1 and at == len(lines):  # file with only a shebang
        lines[at - 1 :] = [lines[at - 1] if lines[at - 1].endswith(("\n", "\r")) else lines[at - 1] + nl]
    lines[at:at] = block
    path.write_bytes("".join(lines).encode("utf-8"))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("paths", nargs="*", help="files or directories (default: src sdk scripts tests examples deploy)")
    ap.add_argument("--fix", action="store_true", help="insert the header where it is missing")
    args = ap.parse_args(argv)
    targets = [Path(p).resolve() for p in args.paths] or [ROOT / d for d in DEFAULT_DIRS if (ROOT / d).exists()]
    files = python_files(targets)
    missing: list[Path] = []
    different: list[Path] = []
    for f in files:
        try:
            state, at = header_state(f.read_text(encoding="utf-8"))
        except UnicodeDecodeError:
            print(f"{f}: not valid UTF-8", file=sys.stderr)
            return 2
        if state == "missing":
            missing.append(f)
            if args.fix:
                add_header(f, at)
        elif state == "different":
            different.append(f)
    rel = lambda p: p.relative_to(ROOT) if p.is_relative_to(ROOT) else p  # noqa: E731
    for f in missing:
        print(("fixed   " if args.fix else "missing ") + str(rel(f)))
    for f in different:
        print("differs " + str(rel(f)) + "  (has a header that is not the project header; fix by hand)")
    print(f"{len(files)} files checked, {len(missing)} without a header{' (fixed)' if args.fix else ''}, {len(different)} with a different header")
    return 1 if (different or (missing and not args.fix)) else 0


if __name__ == "__main__":
    sys.exit(main())
