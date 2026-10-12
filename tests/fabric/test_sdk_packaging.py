# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Packaging of the separate ``olcf-fabric`` distribution: stdlib only, Python 3.10 syntax, matching versions, a buildable wheel with no server code."""

from __future__ import annotations

import ast
import os
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
PKG = ROOT / "sdk" / "fabric" / "olcf_fabric"
STDLIB = set(sys.stdlib_module_names)
#: names that exist only in the standard library of Python >= 3.11 (the SDK must run on 3.10)
NEW_IN_311 = {"tomllib", "StrEnum", "ExceptionGroup", "BaseExceptionGroup", "Self", "LiteralString", "assert_never", "reveal_type", "Never", "NotRequired", "Required", "TaskGroup"}


def sources() -> list[Path]:
    return sorted(PKG.glob("*.py"))


def test_versions_agree() -> None:
    import olcf_fabric
    from olcf.fabric.snippet import SDK_VERSION
    from olcf.version import __version__

    assert olcf_fabric.__version__ == SDK_VERSION == __version__


@pytest.mark.parametrize("path", sources(), ids=lambda p: p.name)
def test_module_is_stdlib_only_py310_and_licensed(path: Path) -> None:
    text = path.read_text()
    assert text.startswith("# SPDX-License-Identifier: Apache-2.0\n# Copyright (c) 2026 Justin Dodrill")
    tree = ast.parse(text, feature_version=(3, 10))                      # raises SyntaxError for 3.11+ only syntax (except*, ...)
    for node in ast.walk(tree):
        mods: list[str] = []
        if isinstance(node, ast.Import):
            mods = [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            mods = [node.module]
            assert not ({a.name for a in node.names} & NEW_IN_311), f"{path.name}: {node.module} imports a 3.11+ name"
        for m in mods:
            top = m.split(".")[0]
            assert top in STDLIB or top == "olcf_fabric", f"{path.name} imports non-stdlib module {m}"
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "datetime" and node.attr == "UTC":
            pytest.fail(f"{path.name}: datetime.UTC is 3.11+")
    assert "from olcf." not in text and "import olcf\n" not in text, "the SDK must not import the server package"


def test_no_python311_only_stdlib_use() -> None:
    for path in sources():
        text = path.read_text()
        for needle in ("import tomllib", "enum import StrEnum", "except*", "asyncio.TaskGroup", "datetime import UTC"):
            assert needle not in text, f"{path.name}: {needle}"


def test_wheel_builds_and_contains_only_the_sdk(tmp_path: Path) -> None:
    out = tmp_path / "dist"
    env = {**os.environ, "PYTHON": sys.executable}
    res = subprocess.run(["bash", str(ROOT / "scripts" / "build_fabric_sdk.sh"), str(out)], capture_output=True, text=True, env=env, timeout=110, check=False)
    assert res.returncode == 0, res.stderr + res.stdout
    wheels = list(out.glob("olcf_fabric-*-py3-none-any.whl"))
    assert len(wheels) == 1
    with zipfile.ZipFile(wheels[0]) as z:
        names = z.namelist()
        meta = next(z.read(n).decode() for n in names if n.endswith("METADATA"))
    assert {n.split("/")[0] for n in names if "/" in n and not n.endswith(".dist-info/")} <= {"olcf_fabric", wheels[0].name.split("-")[0] + "-" + wheels[0].name.split("-")[1] + ".dist-info"}
    assert "olcf_fabric/api.py" in names and not any(n.startswith(("olcf/", "tests/")) for n in names)
    assert "Requires-Dist" not in meta and "Requires-Python: >=3.10" in meta and "Name: olcf-fabric" in meta
    assert "Fabric Runtime 2.0 Compatible" in meta                       # README is the long description


def test_wheel_imports_in_isolation(tmp_path: Path) -> None:
    out = tmp_path / "dist"
    subprocess.run(["bash", str(ROOT / "scripts" / "build_fabric_sdk.sh"), str(out)], capture_output=True, text=True, env={**os.environ, "PYTHON": sys.executable}, timeout=110, check=True)
    wheel = next(out.glob("*.whl"))
    code = "import sys, olcf_fabric as o; assert 'olcf' not in sys.modules; print(o.__version__, o.__file__)"
    res = subprocess.run([sys.executable, "-c", f"import sys; sys.path.insert(0, {str(wheel)!r}); {code}"], capture_output=True, text=True, cwd=tmp_path, timeout=60, check=False)
    assert res.returncode == 0, res.stderr
    assert str(wheel) in res.stdout
