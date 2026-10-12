# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The test tree is a set of real packages and helper code is imported by qualified name (review finding F15).

The old layout had several ``conftest.py`` files without packages: a runtime ``from conftest import Stack`` resolved to whichever conftest
pytest had loaded last, so the whole-tree run (the documented CI command) failed in fixtures that passed when their file ran alone.
These checks make that class of mistake impossible to reintroduce: no bare ``conftest`` imports, no bare imports of sibling helper modules,
every directory with tests is a package, and the whole-module reordering hook (``OLCF_TEST_ORDER``) works.
"""

from __future__ import annotations

import ast
import os
import subprocess
import sys
from pathlib import Path

import pytest

TESTS = Path(__file__).resolve().parents[1]
ROOT = TESTS.parent
#: directories that hold fixture data, not importable test code
DATA_DIRS = {"cases", "negative", "__pycache__", "data"}


def _py_files() -> list[Path]:
    return [p for p in TESTS.rglob("*.py") if "__pycache__" not in p.parts and not (set(p.relative_to(TESTS).parts) & {"cases", "negative"})]


def test_every_directory_with_python_files_is_a_package() -> None:
    missing = sorted({str(p.parent.relative_to(ROOT)) for p in _py_files() if not (p.parent / "__init__.py").exists()})
    assert not missing, f"add __init__.py (tests are imported by qualified package path): {missing}"


def test_no_bare_conftest_or_sibling_helper_imports() -> None:
    sibling_modules: dict[str, Path] = {}
    for p in _py_files():
        if p.name != "__init__.py":
            sibling_modules.setdefault(p.stem, p)
    offenders: list[str] = []
    for path in _py_files():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            mods: list[str] = []
            if isinstance(node, ast.Import):
                mods = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                mods = [node.module]
            elif isinstance(node, ast.Call) and getattr(node.func, "id", "") == "__import__" and node.args and isinstance(node.args[0], ast.Constant):
                mods = [str(node.args[0].value)]
            for m in mods:
                head = m.split(".")[0]
                if head == "conftest" or (head in sibling_modules and head not in {"tests"} and not (ROOT / "src" / head).exists() and head not in sys.stdlib_module_names):
                    # a bare name that is also the stem of a file in the test tree (``fake_agent``, ``test_api_e2e`` ...) resolves only through sys.path accidents
                    offenders.append(f"{path.relative_to(ROOT)}:{node.lineno}: import {m}")
    assert not offenders, "import test helpers by their qualified name (``from tests.<area>.<module> import ...``):\n" + "\n".join(offenders)


def test_collection_in_reverse_module_order_works() -> None:
    """The ``OLCF_TEST_ORDER`` hook reorders whole modules; collecting files of different areas in reverse must find the same tests."""
    files = ["tests/security/test_unauthenticated.py", "tests/compute/test_real_agent.py", "tests/items/test_roundtrip_samples.py"]
    base = [sys.executable, "-m", "pytest", "--collect-only", "-q", "-p", "no:cacheprovider", *files]
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(ROOT / "src"), str(ROOT)])}
    fwd = subprocess.run(base, capture_output=True, text=True, cwd=ROOT, env=env, timeout=240, check=False)
    rev = subprocess.run(base, capture_output=True, text=True, cwd=ROOT, env={**env, "OLCF_TEST_ORDER": "reverse"}, timeout=240, check=False)
    assert fwd.returncode == 0, fwd.stdout[-2000:] + fwd.stderr[-2000:]
    assert rev.returncode == 0, rev.stdout[-2000:] + rev.stderr[-2000:]
    ids = lambda r: [ln for ln in r.stdout.splitlines() if "::" in ln]  # noqa: E731
    assert sorted(ids(fwd)) == sorted(ids(rev)) and ids(fwd)
    assert ids(fwd)[0].split("::")[0] != ids(rev)[0].split("::")[0], "the reverse order hook did not reorder the modules"


def test_bad_order_value_is_rejected() -> None:
    env = {**os.environ, "OLCF_TEST_ORDER": "sideways", "PYTHONPATH": os.pathsep.join([str(ROOT / "src"), str(ROOT)])}
    r = subprocess.run([sys.executable, "-m", "pytest", "--collect-only", "-q", "-p", "no:cacheprovider", "tests/unit/test_ratelimit.py"], capture_output=True, text=True, cwd=ROOT, env=env, timeout=120, check=False)
    assert r.returncode != 0 and "OLCF_TEST_ORDER" in (r.stdout + r.stderr)


@pytest.mark.parametrize("module", ["tests.compute.ctrl_stack", "tests.orch.orch_support", "tests.items.items_support", "tests.fabric.fabric_support"])
def test_support_modules_import_in_a_fresh_interpreter_without_conftest(module: str) -> None:
    """Support modules do not rely on a conftest having been loaded first (nothing named ``conftest`` is on sys.path)."""
    code = f"import sys, importlib; importlib.import_module({module!r}); assert 'conftest' not in sys.modules"
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(ROOT / "src"), str(ROOT), str(ROOT / "sdk" / "fabric")])}
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, cwd=ROOT, env=env, timeout=120, check=False)
    assert r.returncode == 0, r.stderr[-2000:]
