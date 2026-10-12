# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``scripts/gen_reference.py`` must produce the same ``docs/reference/cli.md`` on every supported Python (review: stale generated CLI reference).

argparse words its help differently on Python 3.11/3.12 and 3.13 (where the positional arguments of a long ``usage:`` go, and ``-o FILE, --output FILE`` versus
``-o, --output FILE``); the generator normalises both.  These tests feed it the two spellings.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def gen() -> ModuleType:
    spec = importlib.util.spec_from_file_location("gen_reference_under_test", ROOT / "scripts" / "gen_reference.py")
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


PY312 = """usage: python -m olcf.runtime [-h] [--state-dir STATE_DIR] [--index-url INDEX_URL]
                              [--find-links FIND_LINKS] [--maven-repository MAVEN_REPOSITORY]
                              [--trust-dir TRUST_DIR] [--json] [-v]
                              {list,doctor,install,verify,repair,retire,rollback,export,import,trust,keygen,sign}
                              ...

OLCF runtime manager

options:
  -h, --help            show this help message and exit
  -o FILE, --output FILE
                        where to write
  --state-dir STATE_DIR
                        a long help text that argparse wraps onto the next line because the option
                        string itself is longer than the help column
"""

PY313 = """usage: python -m olcf.runtime [-h] [--state-dir STATE_DIR] [--index-url INDEX_URL]
                              [--find-links FIND_LINKS] [--maven-repository MAVEN_REPOSITORY]
                              [--trust-dir TRUST_DIR] [--json] [-v]
                              {list,doctor,install,verify,repair,retire,rollback,export,import,trust,keygen,sign} ...

OLCF runtime manager

options:
  -h, --help            show this help message and exit
  -o, --output FILE     where to write
  --state-dir STATE_DIR
                        a long help text that argparse wraps onto the next line because the option
                        string itself is longer than the help column
"""


def test_python_version_spellings_normalise_to_the_same_text(gen: ModuleType) -> None:
    a, b = gen.normalise_help(PY312), gen.normalise_help(PY313)
    assert a == b
    assert "-o, --output FILE" in a and "-o FILE, --output FILE" not in a
    assert gen.normalise_help(a) == a  # idempotent


def test_usage_is_wrapped_at_98_columns_with_a_hanging_indent_and_keeps_choices_with_their_ellipsis(gen: ModuleType) -> None:
    out = gen.normalise_help(PY312)
    usage = out.split("\n\n", 1)[0].splitlines()
    assert all(len(line) <= 98 for line in usage[:-1]), usage  # only the last line may overshoot: it holds one unbreakable token ("{a,b,...} ...")
    assert all(line.startswith(" " * len("usage: python -m olcf.runtime ")) for line in usage[1:])
    assert usage[-1].strip().endswith("sign} ...")


def test_text_that_is_not_a_usage_page_is_left_alone(gen: ModuleType) -> None:
    assert gen.normalise_help("error: something else\n") == "error: something else\n"


def test_a_failing_help_command_is_an_error_not_documentation(gen: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    import subprocess

    def boom(cmd: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(cmd, 1, "", "Traceback (most recent call last): ...")

    monkeypatch.setattr(gen.subprocess, "run", boom)
    with pytest.raises(RuntimeError, match="exited 1"):
        gen.argparse_tree([sys.executable, "-m", "olcf.runtime"])
