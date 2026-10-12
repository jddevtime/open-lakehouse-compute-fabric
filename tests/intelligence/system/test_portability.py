# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""PostgreSQL parity - what can and cannot be said without a PostgreSQL server.

**Not tested here:** nothing in this suite runs against PostgreSQL (``OLCF_TEST_POSTGRES_URL`` is not set in the environments the suite is developed in; the storage-layer tests of
``tests/intelligence`` carry a ``postgres`` parametrisation that this suite does not extend to the system scenarios or the scale test). Whether the graph behaves identically under
PostgreSQL's concurrency and collation rules is therefore **unverified**.

**Tested here:** that the Intelligence package makes no SQLite-only assumption that can be seen in its source - no hand-written SQL text, no vendor dialect imports, no branch on the
dialect name, no ``sqlite3`` import, no vendor function in a ``func.`` call - so every statement is built by SQLAlchemy Core from the portable schema.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import olcf

PKG = Path(olcf.__file__).resolve().parent / "intelligence"
SQLITE_ONLY_FUNCS = {"strftime", "datetime", "julianday", "ifnull", "group_concat", "json_extract", "json_each", "json_group_array", "randomblob", "last_insert_rowid", "instr", "glob"}


def _modules() -> list[tuple[Path, ast.AST]]:
    return [(f, ast.parse(f.read_text(encoding="utf-8"), filename=str(f))) for f in sorted(PKG.rglob("*.py"))]


def test_the_package_has_no_hand_written_sql_and_no_vendor_imports() -> None:
    offences: list[str] = []
    for path, tree in _modules():
        rel = path.relative_to(PKG)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                offences += [f"{rel}:{node.lineno} import {a.name}" for a in node.names if a.name == "sqlite3" or a.name.startswith("sqlalchemy.dialects")]
            elif isinstance(node, ast.ImportFrom) and (node.module or "").startswith(("sqlalchemy.dialects", "sqlite3")):
                offences.append(f"{rel}:{node.lineno} from {node.module}")
            elif isinstance(node, ast.ImportFrom) and node.module == "sqlalchemy" and any(a.name == "text" for a in node.names):
                offences.append(f"{rel}:{node.lineno} sqlalchemy.text (hand-written SQL)")
            elif isinstance(node, ast.Attribute) and node.attr == "dialect" and isinstance(node.value, ast.Attribute) and node.value.attr in ("engine", "bind", "conn"):
                offences.append(f"{rel}:{node.lineno} branches on the dialect")
    assert offences == []


def test_no_sqlite_only_function_or_pragma_is_called() -> None:
    offences: list[str] = []
    for path, tree in _modules():
        rel = path.relative_to(PKG)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name) and node.func.value.id == "func" and node.func.attr.lower() in SQLITE_ONLY_FUNCS:
                offences.append(f"{rel}:{node.lineno} func.{node.func.attr}")
        if re.search(r"(?i)\bpragma\s+\w+\s*=|sqlite_master|\binsert\s+or\s+(replace|ignore)\b", path.read_text(encoding="utf-8")):
            offences.append(f"{rel}: SQLite statement text")
    assert offences == []


def test_the_search_over_json_columns_is_a_portable_cast_like() -> None:
    """The one place the suite's data scale touches JSON (edge attributes in the snapshot diff, ``search`` in ``attrs``) uses ``cast(..., String).like(...)`` - the schema module says so
    and the graph store really does it."""
    assert "cast(ec.attrs, String).like" in (PKG / "graph.py").read_text(encoding="utf-8")
    assert "never JSON operators" in (PKG / "schema.py").read_text(encoding="utf-8")
