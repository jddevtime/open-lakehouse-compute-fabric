#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Fail a CI job whose pytest JUnit report shows that the tests it exists for did not really run.

A green pytest exit status does not prove coverage: when every test of a job is skipped (no browser installed, no runtime cache) pytest still exits 0.  This gate reads
the JUnit XML(s) the job produced and fails when

* fewer than ``--min-tests`` test cases ran (default 1, so "zero tests" always fails),
* any test was skipped (or xfailed) and its reason is not matched by an explicit ``--allow-skip`` regular expression, or
* any test failed or errored (normally pytest already failed the job, this keeps the gate self-contained).

Usage::

    python scripts/check_junit_run.py e2e-junit.xml --min-tests 50
    python scripts/check_junit_run.py junit.xml --min-tests 5 --allow-skip "shellcheck is not installed"
"""

from __future__ import annotations

import argparse
import re
import sys
import xml.etree.ElementTree as ET  # noqa: S405 - the XML is produced by pytest in the same job, not by an untrusted party
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class RunSummary:
    """Counts of one or more JUnit reports; ``skipped`` lists ``(test id, reason)``."""

    total: int = 0
    passed: int = 0
    failed: int = 0
    failures: list[tuple[str, str]] = field(default_factory=list)
    skipped: list[tuple[str, str]] = field(default_factory=list)


def summarise(paths: list[Path]) -> RunSummary:
    """Count the ``<testcase>`` elements of the given JUnit XML files."""
    out = RunSummary()
    for path in paths:
        root = ET.parse(path).getroot()  # noqa: S314
        for case in root.iter("testcase"):
            out.total += 1
            name = f"{case.get('classname', '')}::{case.get('name', '')}"
            skip = case.find("skipped")
            if skip is not None:
                out.skipped.append((name, (skip.get("message") or skip.text or "").strip()))
            elif case.find("failure") is not None or case.find("error") is not None:
                out.failed += 1
                bad = case.find("failure") if case.find("failure") is not None else case.find("error")
                out.failures.append((name, ((bad.get("message") or bad.text or "").strip().splitlines() or [""])[0][:300]))
            else:
                out.passed += 1
    return out


def problems(summary: RunSummary, min_tests: int = 1, allow_skip: list[str] | None = None) -> list[str]:
    """The reasons the run must not count as a pass (empty when it is fine)."""
    found: list[str] = []
    executed = summary.total - len(summary.skipped)
    if summary.total == 0:
        found.append("the report contains no test cases at all: nothing ran")
    if executed < max(1, min_tests):
        found.append(f"only {executed} test(s) actually executed (skipped tests do not count); at least {max(1, min_tests)} are required")
    patterns = [re.compile(p) for p in allow_skip or []]
    for name, reason in summary.skipped:
        if not any(p.search(reason) for p in patterns):
            found.append(f"unexpected skip: {name} ({reason or 'no reason given'})")
    if summary.failed:
        found.append(f"{summary.failed} test(s) failed or errored")
        found.extend(f"failed: {name}: {reason}" for name, reason in summary.failures)
    return found


def main(argv: list[str] | None = None) -> int:
    """CLI entry point; returns the exit status."""
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("reports", nargs="+", type=Path, help="pytest --junitxml files")
    ap.add_argument("--min-tests", type=int, default=1, help="minimum number of executed (not skipped) tests (default 1)")
    ap.add_argument("--allow-skip", action="append", default=[], metavar="REGEX", help="skip reasons that are expected; may be repeated. Everything else fails")
    a = ap.parse_args(argv)
    missing = [str(p) for p in a.reports if not p.is_file()]
    if missing:
        print("JUnit report(s) not found (the test step did not produce them): " + ", ".join(missing), file=sys.stderr)  # noqa: T201
        return 1
    summary = summarise(a.reports)
    print(f"{summary.total} test case(s): {summary.passed} passed, {summary.failed} failed, {len(summary.skipped)} skipped")  # noqa: T201
    bad = problems(summary, a.min_tests, a.allow_skip)
    for line in bad:
        print("FAIL: " + line, file=sys.stderr)  # noqa: T201
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
