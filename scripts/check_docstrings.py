#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Docstring coverage audit (AST based, nothing is imported or executed).

Counted objects, per top-level package below ``src/olcf``:

* **modules**   every ``*.py`` file (including ``__init__.py``) needs a module docstring;
* **classes**   every public class (name not starting with ``_``), at any nesting depth outside of functions;
* **functions** every public module-level function and every public method of a public class.  Dunder methods, property
  setters/deleters, ``@overload`` stubs and nested (closure) functions are not counted.

Usage::

    python scripts/check_docstrings.py                    # coverage table
    python scripts/check_docstrings.py --list             # also list every missing docstring (file:line  kind  name)
    python scripts/check_docstrings.py --min-modules 95 --min-classes 95   # exit 1 below the thresholds (CI gate)
    python scripts/check_docstrings.py --root sdk/fabric  # another source root

The thresholds apply to the *overall* coverage figures, not per package.
"""

from __future__ import annotations

import argparse
import ast
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SKIP_PARTS = {"__pycache__", ".venv", "node_modules"}


@dataclass
class Tally:
    """Counts of documented/total objects of one kind."""

    total: int = 0
    documented: int = 0

    def add(self, has_doc: bool) -> None:
        """Record one object."""
        self.total += 1
        self.documented += bool(has_doc)

    @property
    def pct(self) -> float:
        """Coverage in percent (100 when there is nothing to document)."""
        return 100.0 if not self.total else 100.0 * self.documented / self.total


@dataclass
class PackageStats:
    """Tallies of one package plus the list of missing items."""

    modules: Tally = field(default_factory=Tally)
    classes: Tally = field(default_factory=Tally)
    functions: Tally = field(default_factory=Tally)
    missing: list[tuple[str, int, str, str]] = field(default_factory=list)


def _decorator_names(node: ast.FunctionDef | ast.AsyncFunctionDef) -> set[str]:
    names: set[str] = set()
    for d in node.decorator_list:
        target = d.func if isinstance(d, ast.Call) else d
        if isinstance(target, ast.Attribute):
            names.add(target.attr)
        elif isinstance(target, ast.Name):
            names.add(target.id)
    return names


def _counts_as_function(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    if node.name.startswith("_"):
        return False
    deco = _decorator_names(node)
    return not ({"overload", "setter", "deleter"} & deco)


def audit_file(path: Path, rel: str, stats: PackageStats) -> None:
    """Add the objects of one file to ``stats``."""
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError as exc:
        print(f"{rel}: syntax error: {exc}", file=sys.stderr)
        return
    has = ast.get_docstring(tree) is not None
    stats.modules.add(has)
    if not has:
        stats.missing.append((rel, 1, "module", rel))

    def visit_class(cls: ast.ClassDef, qual: str) -> None:
        if cls.name.startswith("_"):
            return
        ok = ast.get_docstring(cls) is not None
        stats.classes.add(ok)
        if not ok:
            stats.missing.append((rel, cls.lineno, "class", f"{qual}{cls.name}"))
        for item in cls.body:
            if isinstance(item, ast.ClassDef):
                visit_class(item, f"{qual}{cls.name}.")
            elif isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and _counts_as_function(item):
                fok = ast.get_docstring(item) is not None
                stats.functions.add(fok)
                if not fok:
                    stats.missing.append((rel, item.lineno, "method", f"{qual}{cls.name}.{item.name}"))

    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            visit_class(node, "")
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and _counts_as_function(node):
            ok = ast.get_docstring(node) is not None
            stats.functions.add(ok)
            if not ok:
                stats.missing.append((rel, node.lineno, "function", node.name))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--root", default="src/olcf", help="source root (default: src/olcf); its direct sub-packages are reported separately")
    ap.add_argument("--list", action="store_true", help="list every missing docstring")
    ap.add_argument("--min-modules", type=float, default=None, help="fail when module-docstring coverage is below this percentage")
    ap.add_argument("--min-classes", type=float, default=None, help="fail when public-class docstring coverage is below this percentage")
    ap.add_argument("--min-functions", type=float, default=None, help="fail when public-function docstring coverage is below this percentage")
    args = ap.parse_args(argv)
    root = (ROOT / args.root).resolve()
    if not root.is_dir():
        print(f"no such directory: {root}", file=sys.stderr)
        return 2
    per: dict[str, PackageStats] = defaultdict(PackageStats)
    total = PackageStats()
    for f in sorted(root.rglob("*.py")):
        rel_parts = f.relative_to(root).parts
        if SKIP_PARTS.intersection(rel_parts):
            continue
        pkg = rel_parts[0] if len(rel_parts) > 1 else "(top level)"
        rel = str(f.relative_to(ROOT))
        audit_file(f, rel, per[pkg])
    # the overall figures are the sums of the packages (each file is audited once)
    for st in per.values():
        for name in ("modules", "classes", "functions"):
            t: Tally = getattr(total, name)
            s: Tally = getattr(st, name)
            t.total += s.total
            t.documented += s.documented
        total.missing.extend(st.missing)
    width = max([len(p) for p in per] + [10])
    print(f"{'package':<{width}}  {'modules':>14}  {'classes':>14}  {'functions':>16}")
    for pkg in sorted(per):
        s = per[pkg]
        print(f"{pkg:<{width}}  {s.modules.documented:>5}/{s.modules.total:<4}{s.modules.pct:>4.0f}%  "
              f"{s.classes.documented:>5}/{s.classes.total:<4}{s.classes.pct:>4.0f}%  {s.functions.documented:>6}/{s.functions.total:<5}{s.functions.pct:>4.0f}%")
    print(f"{'TOTAL':<{width}}  {total.modules.documented:>5}/{total.modules.total:<4}{total.modules.pct:>4.0f}%  "
          f"{total.classes.documented:>5}/{total.classes.total:<4}{total.classes.pct:>4.0f}%  {total.functions.documented:>6}/{total.functions.total:<5}{total.functions.pct:>4.0f}%")
    if args.list:
        print()
        for rel, line, kind, name in sorted(total.missing):
            print(f"{rel}:{line}  {kind:<8} {name}")
    failed = []
    for label, got, floor in (("modules", total.modules.pct, args.min_modules), ("classes", total.classes.pct, args.min_classes),
                              ("functions", total.functions.pct, args.min_functions)):
        if floor is not None and got < floor:
            failed.append(f"{label} {got:.1f}% < {floor:.1f}%")
    if failed:
        print("FAIL: " + "; ".join(failed), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
