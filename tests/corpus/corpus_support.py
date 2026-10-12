# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Shared helpers of the corpus: the sandbox-wide Spark lock, the cached Spark runtime, and materialising a case's input data into a scratch directory."""

from __future__ import annotations

import contextlib
import shutil
import sys
from collections.abc import Iterator
from pathlib import Path

from tests.spark_locking import spark_lock_held

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
SPARK_RT = "olcf-rt-test-4.1.1"
SMALL_JVM = {"spark.driver.memory": "512m", "spark.sql.shuffle.partitions": "2", "spark.ui.enabled": "false", "spark.sql.session.timeZone": "UTC"}


@contextlib.contextmanager
def spark_lock() -> Iterator[None]:
    """Exclusive lock around a JVM (two builders share 2 CPUs / 7 GB). Do not nest."""
    with spark_lock_held():
        yield


def spark_runtime() -> tuple[Path, Path]:
    """``(runtime_dir, python)`` of the cached Spark runtime; raises ``RuntimeError`` when it cannot be built (offline)."""
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from tests.node.runtime_fixture import CACHE, ensure_test_runtime  # noqa: PLC0415

    ensure_test_runtime(lambda m: print(m, file=sys.stderr))
    rt = CACHE / "state" / "runtimes" / SPARK_RT
    return rt, rt / "venv" / "bin" / "python"


def materialise(case_dir: Path, extra_lakehouses: list[str], dest: Path) -> dict[str, str]:
    """Copy the case input into ``dest`` (``dest/default/Files``, ``dest/<name>/Files``) and return ``{"default": ..., name: ...}`` roots."""
    roots: dict[str, str] = {}
    for key, src in [("default", case_dir / "input"), *[(n, case_dir / "input" / n) for n in extra_lakehouses]]:
        target = dest / key
        (target / "Files").mkdir(parents=True, exist_ok=True)
        (target / "Tables").mkdir(parents=True, exist_ok=True)
        if (src / "Files").is_dir():
            shutil.copytree(src / "Files", target / "Files", dirs_exist_ok=True)
        roots[key] = str(target)
    return roots
