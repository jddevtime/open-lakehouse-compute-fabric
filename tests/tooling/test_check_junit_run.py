# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The CI gate that stops a skip-only or empty browser job from passing (``scripts/check_junit_run.py``, review finding F11)."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "check_junit_run.py"
_spec = importlib.util.spec_from_file_location("check_junit_run_under_test", SCRIPT)
assert _spec and _spec.loader
gate = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = gate  # dataclasses resolve their module through sys.modules
_spec.loader.exec_module(gate)

OK_CASE = '<testcase classname="tests.e2e.test_a" name="test_ok" time="0.1"/>'
FAIL = '<testcase classname="tests.e2e.test_a" name="test_bad"><failure message="boom">x</failure></testcase>'
ERROR = '<testcase classname="tests.e2e.test_a" name="test_err"><error message="setup">x</error></testcase>'


def skip(reason: str) -> str:
    return f'<testcase classname="tests.e2e.test_a" name="test_skip"><skipped type="pytest.skip" message="{reason}">/x.py:1: {reason}</skipped></testcase>'


def report(tmp_path: Path, *cases: str, name: str = "junit.xml") -> Path:
    p = tmp_path / name
    p.write_text('<?xml version="1.0"?><testsuites><testsuite name="pytest">' + "".join(cases) + "</testsuite></testsuites>")
    return p


def test_all_passing_run_is_accepted(tmp_path: Path) -> None:
    s = gate.summarise([report(tmp_path, OK_CASE, OK_CASE)])
    assert (s.total, s.passed, s.failed, s.skipped) == (2, 2, 0, [])
    assert gate.problems(s, min_tests=2) == []


def test_zero_tests_fails(tmp_path: Path) -> None:
    s = gate.summarise([report(tmp_path)])
    found = gate.problems(s)
    assert any("nothing ran" in x for x in found)
    assert any("only 0 test" in x for x in found)


def test_skip_only_run_fails_even_though_pytest_exits_zero(tmp_path: Path) -> None:
    s = gate.summarise([report(tmp_path, skip("Chromium not found at /opt/x"), skip("Chromium not found at /opt/x"))])
    found = gate.problems(s)
    assert any("only 0 test" in x for x in found)
    assert sum("unexpected skip" in x for x in found) == 2


def test_one_unexpected_skip_among_passes_fails(tmp_path: Path) -> None:
    found = gate.problems(gate.summarise([report(tmp_path, OK_CASE, skip("no cached Spark runtime"))]))
    assert found == ["unexpected skip: tests.e2e.test_a::test_skip (no cached Spark runtime)"]


def test_an_allowed_skip_reason_passes_but_others_still_fail(tmp_path: Path) -> None:
    s = gate.summarise([report(tmp_path, OK_CASE, skip("no cached Spark runtime"), skip("Chromium not found"))])
    found = gate.problems(s, allow_skip=["no cached Spark runtime"])
    assert len(found) == 1 and "Chromium not found" in found[0]


def test_minimum_count_counts_only_executed_tests(tmp_path: Path) -> None:
    s = gate.summarise([report(tmp_path, OK_CASE, OK_CASE, skip("x"))])
    assert any("only 2 test" in x for x in gate.problems(s, min_tests=3, allow_skip=["x"]))
    assert gate.problems(s, min_tests=2, allow_skip=["x"]) == []


def test_failures_and_errors_fail(tmp_path: Path) -> None:
    s = gate.summarise([report(tmp_path, OK_CASE, FAIL, ERROR)])
    assert s.failed == 2
    assert "2 test(s) failed or errored" in gate.problems(s)


def test_several_reports_are_added_up(tmp_path: Path) -> None:
    s = gate.summarise([report(tmp_path, OK_CASE, name="a.xml"), report(tmp_path, OK_CASE, name="b.xml")])
    assert s.total == 2


def run(*args: object) -> subprocess.CompletedProcess[str]:
    return subprocess.run([sys.executable, str(SCRIPT), *map(str, args)], capture_output=True, text=True, check=False)  # noqa: S603


def test_cli_exit_status(tmp_path: Path) -> None:
    good, skipped = report(tmp_path, OK_CASE, name="good.xml"), report(tmp_path, skip("nope"), name="skipped.xml")
    assert run(good).returncode == 0
    r = run(skipped)
    assert r.returncode == 1 and "unexpected skip" in r.stderr
    assert run(skipped, "--allow-skip", "nope").returncode == 1  # still nothing executed
    assert run(good, "--min-tests", "2").returncode == 1


def test_cli_missing_report_fails(tmp_path: Path) -> None:
    r = run(tmp_path / "absent.xml")
    assert r.returncode == 1 and "not found" in r.stderr


def test_a_real_pytest_junit_file_is_understood(tmp_path: Path) -> None:
    """The format produced by pytest itself (not a hand-written one), with a passing and a skipped test."""
    t = tmp_path / "test_x.py"
    t.write_text("import pytest\n\ndef test_a():\n    pass\n\ndef test_b():\n    pytest.skip('why not')\n")
    xml = tmp_path / "j.xml"
    subprocess.run([sys.executable, "-m", "pytest", str(t), f"--junitxml={xml}", "-q", "-p", "no:cacheprovider", "--rootdir", str(tmp_path)], capture_output=True, check=False, cwd=tmp_path)  # noqa: S603
    s = gate.summarise([xml])
    assert (s.total, s.passed, len(s.skipped)) == (2, 1, 1)
    assert s.skipped[0][1] == "why not"
    assert gate.problems(s)  # the skip is not allowed
    assert gate.problems(s, allow_skip=["why not"]) == []


def test_browser_ci_job_installs_chromium_requires_it_and_gates_on_the_junit_report() -> None:
    import yaml

    doc = yaml.safe_load((SCRIPT.parents[1] / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8"))
    job = doc["jobs"]["ui-e2e"]
    text = "\n".join(str(s.get("run", "")) for s in job["steps"])
    assert job["env"]["OLCF_E2E_REQUIRE_BROWSER"] == "1"
    assert "playwright install --with-deps chromium" in text
    assert "--junitxml=e2e-junit.xml" in text and "OLCF_E2E_CHROMIUM" not in str(doc)
    gate_step = next(s for s in job["steps"] if "check_junit_run.py" in str(s.get("run", "")))
    assert "e2e-junit.xml" in gate_step["run"] and "--allow-skip" not in gate_step["run"]
    assert gate_step["if"] == "always()"
    uploads = [s["with"]["path"] for s in job["steps"] if str(s.get("uses", "")).startswith("actions/upload-artifact")]
    assert "e2e-junit.xml" in uploads and "test-results" in uploads


def test_failures_are_named_in_the_report(tmp_path: Path) -> None:
    found = gate.problems(gate.summarise([report(tmp_path, OK_CASE, FAIL, ERROR)]))
    assert "2 test(s) failed or errored" in found
    assert "failed: tests.e2e.test_a::test_bad: boom" in found
    assert "failed: tests.e2e.test_a::test_err: setup" in found
