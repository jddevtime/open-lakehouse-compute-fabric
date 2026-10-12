# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Unit conversion and platform handling of the peak-RSS helper (``tests/peak_memory.py``, review finding F15): Linux reports KiB, macOS bytes, Windows has no ``resource``."""

from __future__ import annotations

import sys
import types
from typing import Any

import pytest

from tests import peak_memory as pm

GIB = 1024 ** 3


@pytest.mark.parametrize(("platform", "raw", "expected"), [
    ("linux", 1024, 1024 * 1024),                    # KiB -> bytes
    ("linux", 350 * 1024, 350 * 1024 ** 2),
    ("freebsd", 2048, 2048 * 1024),
    ("darwin", 953_856_000, 953_856_000),            # already bytes: NOT multiplied (the review's 930,512 MB false reading)
    ("darwin", 1, 1),
])
def test_maxrss_units(platform: str, raw: int, expected: int) -> None:
    assert pm.maxrss_to_bytes(raw, platform) == expected


def test_windows_has_no_ru_maxrss() -> None:
    with pytest.raises(ValueError, match="Windows"):
        pm.maxrss_to_bytes(1, "win32")


def fake_resource(maxrss: int) -> types.ModuleType:
    mod = types.ModuleType("resource")
    mod.RUSAGE_SELF = 0  # type: ignore[attr-defined]
    mod.getrusage = lambda _who: types.SimpleNamespace(ru_maxrss=maxrss)  # type: ignore[attr-defined]
    return mod


def fake_psutil(**memory_info: int) -> types.ModuleType:
    mod = types.ModuleType("psutil")
    mod.Process = lambda: types.SimpleNamespace(memory_info=lambda: types.SimpleNamespace(**memory_info))  # type: ignore[attr-defined]
    return mod


@pytest.mark.parametrize(("platform", "raw", "expected"), [("linux", 900_000, 900_000 * 1024), ("darwin", 953_856_000, 953_856_000)])
def test_posix_platforms_use_the_resource_module_with_their_own_unit(monkeypatch: pytest.MonkeyPatch, platform: str, raw: int, expected: int) -> None:
    monkeypatch.setitem(sys.modules, "resource", fake_resource(raw))
    monkeypatch.setattr(sys, "platform", platform)
    assert pm.peak_rss_bytes() == expected
    assert pm.peak_rss_bytes() < 1.5 * GIB                      # 909 MiB is below the 1.5 GiB bound on both


def test_windows_never_imports_resource_and_uses_the_psutil_peak_working_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "resource", None)           # `import resource` would raise ImportError, as on Windows
    monkeypatch.setitem(sys.modules, "psutil", fake_psutil(rss=10, peak_wset=123_456_789))
    monkeypatch.setattr(sys, "platform", "win32")
    assert pm.peak_rss_bytes() == 123_456_789


def test_windows_without_a_peak_figure_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "psutil", fake_psutil(rss=10))          # no peak_wset attribute
    assert pm.peak_rss_bytes("win32") is None
    monkeypatch.setitem(sys.modules, "psutil", None)                        # psutil not installed at all
    assert pm.peak_rss_bytes("win32") is None


def test_posix_without_the_resource_module_falls_back_to_psutil(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "resource", None)
    monkeypatch.setitem(sys.modules, "psutil", fake_psutil(rss=1, peak_wset=42))
    assert pm.peak_rss_bytes("linux") == 42


def test_the_real_measurement_is_plausible_here() -> None:
    import os

    got: Any = pm.peak_rss_bytes()
    assert got is None or 10 * 1024 ** 2 < got < 64 * GIB
    if os.name == "posix":
        assert got is not None


def test_no_test_module_imports_a_posix_only_module_at_top_level() -> None:
    """Windows collection fails on a module-level ``import resource`` / ``import fcntl``; import them lazily (or use ``tests/peak_memory.py`` / ``olcf.core.filelock``)."""
    import ast
    from pathlib import Path

    offenders = []
    for path in sorted(Path(__file__).resolve().parents[1].rglob("*.py")):
        if path.name == "peak_memory.py":
            continue
        try:
            body = ast.parse(path.read_text(encoding="utf-8")).body
        except (SyntaxError, UnicodeDecodeError):          # data fixtures (corpus notebooks) are not modules
            continue
        for node in body:
            names = [a.name for a in node.names] if isinstance(node, ast.Import) else [node.module or ""] if isinstance(node, ast.ImportFrom) else []
            if any(n.split(".")[0] in ("resource", "fcntl") for n in names):
                offenders.append(f"{path}:{node.lineno}")
    assert not offenders, offenders
