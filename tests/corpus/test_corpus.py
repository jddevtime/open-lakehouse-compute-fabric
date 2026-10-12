# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The golden compatibility corpus.

For every case under ``cases/`` this checks, in increasing cost:

1. the committed fixtures are exactly what ``corpus_defs.py`` generates (nothing drifts silently);
2. the analyzer's report (``expected.analysis.json``) is unchanged, for BOTH notebook formats (ipynb and Fabric Git source);
3. the independent ``standalone.py`` reproduces ``expected.data.json`` (pure Python / delta-rs cases always; Spark cases are marked ``spark``);
4. the notebook - analysed, rewritten and executed by the production runner with the notebookutils shim - reproduces the same data (Spark cases marked ``spark``).

``negative/`` holds notebooks that must be rejected with specific blockers. Regenerate goldens with ``python tests/corpus/regen.py`` and review the diff.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
for p in (str(ROOT), str(ROOT / "src")):
    if p not in sys.path:
        sys.path.insert(0, p)

from olcf.fabric.analyzer import analyze_text  # noqa: E402
from tests.corpus import regen  # noqa: E402
from tests.corpus.corpus_defs import (  # noqa: E402
    CASES,
    CASES_DIR,
    NEG_DIR,
    NEGATIVES,
    Case,
    standalone_engine,
)
from tests.corpus.corpus_support import SMALL_JVM, materialise, spark_lock, spark_runtime  # noqa: E402
from tests.fabric.runner_harness import lakehouse, run_job  # noqa: E402

NAMES = sorted(CASES)
FMT = {"ipynb": "ipynb", "fabricGitSource": "fabric_git_source"}
FILE = {"ipynb": "notebook.ipynb", "fabricGitSource": "notebook-content.py"}


def golden(name: str, what: str) -> Any:
    return json.loads((CASES_DIR / name / what).read_text())


# ------------------------------------------------------------------------------------------------ 1. fixtures
def test_the_corpus_covers_the_required_scenarios() -> None:
    required = {"etl_orders", "joins", "windows", "udfs", "spark_sql", "delta_merge", "multi_lakehouse", "parameters", "pipeline_invocation", "magics"}
    assert required <= set(CASES)
    assert {c.primary for c in CASES.values()} == {"ipynb", "fabricGitSource"}                  # both formats are exercised by the data runs
    neg = {n.name for n in NEGATIVES}
    assert {"sempy", "unknown_notebookutils_api", "lakehouse_default_on_object_storage", "pip_install_forbidden", "shell_bang", "shell_os_system"} <= neg


@pytest.mark.parametrize("name", NAMES)
def test_fixtures_are_what_the_definitions_generate(name: str) -> None:
    case = CASES[name]
    d = CASES_DIR / name
    for fname, text in regen.texts(case).items():
        assert (d / fname).read_text() == text, f"{name}/{fname} is stale: run python tests/corpus/regen.py --fixtures"
    for rel, body in case.files.items():
        assert (d / "input" / "Files" / rel).read_text() == body
    assert (d / "standalone.py").is_file() and (d / "expected.analysis.json").is_file()
    assert (d / "expected.data.json").is_file(), "run python tests/corpus/regen.py --data"
    ref = json.loads((d / "case.json").read_text())
    assert ref["engine"] == case.engine and ref["data_run"] == case.data_run


def test_negative_fixtures_are_current() -> None:
    for neg in NEGATIVES:
        assert (NEG_DIR / f"{neg.name}.ipynb").read_text() == regen.build_ipynb(neg.cells), neg.name
    on_disk = {p.stem for p in NEG_DIR.glob("*.ipynb")}
    assert on_disk == {n.name for n in NEGATIVES}


# ------------------------------------------------------------------------------------------------ 2. analysis goldens
@pytest.mark.parametrize("name", NAMES)
@pytest.mark.parametrize("fmt", ["ipynb", "fabricGitSource"])
def test_analysis_matches_the_golden_in_both_formats(name: str, fmt: str) -> None:
    case = CASES[name]
    text = (CASES_DIR / name / FILE[fmt]).read_text()
    rep = analyze_text(text, format=FMT[fmt], options=regen.options(case)).report
    assert regen.golden_view(rep) == golden(name, "expected.analysis.json"), f"analysis of {name} ({fmt}) changed; if intended run python tests/corpus/regen.py --analysis and review"


def test_goldens_record_what_matters() -> None:
    g = golden("multi_lakehouse", "expected.analysis.json")
    assert g["rewrites"] == [["FAB-PATH-MOUNT-LOCAL", 1, 1]] and "fs" in g["shims_required"] and g["imports"] == ["csv", "json"]
    g = golden("magics", "expected.analysis.json")
    assert g["inlined_runs"][0]["target"] == "helpers" and g["inlined_runs"][0]["status"] == "inlined"
    g = golden("parameters", "expected.analysis.json")
    assert [p[0] for p in g["parameters"]] == ["run_date", "limit", "dry_run", "ratio", "tags"]
    g = golden("pipeline_invocation", "expected.analysis.json")
    assert sorted({c["name"] for c in g["child_notebooks"]}) == ["child_load", "child_validate"] and not any(c["dynamic"] for c in g["child_notebooks"])
    assert all(golden(n, "expected.analysis.json")["blocked"] is False for n in NAMES)


# ------------------------------------------------------------------------------------------------ negatives
@pytest.mark.parametrize("neg", NEGATIVES, ids=lambda n: n.name)
def test_negative_notebooks_are_rejected_with_the_expected_blockers(neg) -> None:
    case = Case(neg.name, neg.description, neg.cells, lakehouse=neg.lakehouse, allow_pip=neg.allow_pip, allow_shell=neg.allow_shell)
    res = analyze_text((NEG_DIR / f"{neg.name}.ipynb").read_text(), format="ipynb", options=regen.options(case))
    assert res.blocked is True
    got = sorted({b["rule"] for b in res.report["blockers"]})
    assert got == sorted(neg.blockers)
    assert all(b["remediation"] for b in res.report["blockers"]), "every blocker explains what to do"
    expected = json.loads((NEG_DIR / "expected.json").read_text())[neg.name]
    assert expected["blockers"] == got and expected["rule_counts"] == res.report["rule_counts"]


def test_the_same_negative_passes_when_policy_allows_it() -> None:
    pip = next(n for n in NEGATIVES if n.name == "pip_install_forbidden")
    case = Case(pip.name, "", pip.cells, allow_pip=True)
    res = analyze_text((NEG_DIR / "pip_install_forbidden.ipynb").read_text(), format="ipynb", options=regen.options(case))
    assert not res.blocked and "FAB-MAGIC-PIP-ALLOWED" in res.report["rule_counts"]
    sh = next(n for n in NEGATIVES if n.name == "shell_bang")
    res = analyze_text((NEG_DIR / "shell_bang.ipynb").read_text(), format="ipynb", options=regen.options(Case(sh.name, "", sh.cells, allow_shell=True)))
    assert not res.blocked


# ------------------------------------------------------------------------------------------------ 3. standalone == golden
PY_STANDALONE = [n for n in NAMES if standalone_engine(CASES[n]) != "spark"]
SPARK_STANDALONE = [n for n in NAMES if standalone_engine(CASES[n]) == "spark"]


@pytest.mark.parametrize("name", PY_STANDALONE)
def test_standalone_python_equivalent_reproduces_the_data_golden(name: str) -> None:
    from tests.corpus.standalone_driver import run_one

    assert run_one(name) == golden(name, "expected.data.json")


@pytest.fixture(scope="module")
def spark_standalone() -> dict[str, Any]:
    from tests.corpus.standalone_driver import run_all

    try:
        spark_runtime()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"the Spark test runtime is not available: {exc}")
    return run_all(SPARK_STANDALONE)           # one JVM for all cases, under the sandbox Spark lock


@pytest.mark.spark
@pytest.mark.parametrize("name", SPARK_STANDALONE)
def test_standalone_spark_equivalent_reproduces_the_data_golden(name: str, spark_standalone: dict[str, Any]) -> None:
    assert spark_standalone[name] == golden(name, "expected.data.json")


# ------------------------------------------------------------------------------------------------ 4. the notebook through the real runner
@pytest.fixture(scope="module")
def fake_python(tmp_path_factory: pytest.TempPathFactory) -> Path:
    from tests.node.fakes import make_fake_runtime

    root = make_fake_runtime(tmp_path_factory.mktemp("corpus-rt"), "olcf-rt-fake-corpus", family="fabric-2.0-compatible")
    return root / "venv" / "bin" / "python"


def run_notebook(case: Case, tmp: Path, python: Path, runtime_dir: Path | None = None, timeout: float = 280.0):
    roots = materialise(CASES_DIR / case.name, case.extra_lakehouses, tmp / "data")
    opts = regen.options(case, root=roots["default"])
    text = (CASES_DIR / case.name / FILE[case.primary]).read_text()
    res = analyze_text(text, format=FMT[case.primary], options=opts)
    assert not res.blocked, res.report["blockers"]
    over: dict[str, Any] = {"default_lakehouse": lakehouse(Path(roots["default"]), "bronze"),
                            "lakehouses": [lakehouse(Path(roots[n]), n, lh_id=f"lh-{i + 2:04d}") for i, n in enumerate(case.extra_lakehouses)]}
    if case.engine == "spark":
        over["spark"] = {"master": "local[1]", "conf": dict(SMALL_JVM)}
    return run_job(tmp / "job", res.source(FMT[case.primary]), python=python, fmt=FMT[case.primary], engine=case.engine, parameters=dict(case.parameters), timeout=timeout,
                   runtime_dir=runtime_dir, **over)


PY_NOTEBOOKS = [n for n in NAMES if CASES[n].engine == "python" and CASES[n].data_run == "notebook"]
SPARK_NOTEBOOKS = [n for n in NAMES if CASES[n].engine == "spark" and CASES[n].data_run == "notebook"]


@pytest.mark.parametrize("name", PY_NOTEBOOKS)
def test_notebook_through_the_runner_reproduces_the_data_golden(name: str, tmp_path: Path, fake_python: Path) -> None:
    r = run_notebook(CASES[name], tmp_path, fake_python, timeout=120)
    assert r.state == "succeeded", (r.failure, r.cell_text())
    assert json.loads(r.exit_value) == golden(name, "expected.data.json")


@pytest.fixture(scope="module")
def spark_rt() -> Iterator[tuple[Path, Path]]:
    try:
        rt, python = spark_runtime()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"the Spark test runtime is not available: {exc}")
    yield rt, python


@pytest.mark.spark
@pytest.mark.parametrize("name", SPARK_NOTEBOOKS)
def test_spark_notebook_through_the_runner_reproduces_the_data_golden(name: str, tmp_path: Path, spark_rt: tuple[Path, Path]) -> None:
    rt, python = spark_rt
    with spark_lock():
        r = run_notebook(CASES[name], tmp_path, python, runtime_dir=rt)
    assert r.state == "succeeded", (r.failure, r.cell_text()[-3000:], r.stderr[-2000:])
    assert json.loads(r.exit_value) == golden(name, "expected.data.json")


@pytest.mark.spark
def test_delta_merge_notebook_runs_only_where_the_runtime_has_delta(tmp_path: Path, spark_rt: tuple[Path, Path]) -> None:
    from olcf.runtime_lib.spark import has_delta

    rt, python = spark_rt
    lock = json.loads((rt / "lock.json").read_text())
    if not has_delta(lock):
        pytest.skip(CASES["delta_merge"].why)
    with spark_lock():
        r = run_notebook(CASES["delta_merge"], tmp_path, python, runtime_dir=rt)
    assert r.state == "succeeded", (r.failure, r.cell_text()[-3000:])
    assert json.loads(r.exit_value) == golden("delta_merge", "expected.data.json")


def test_pipeline_invocation_needs_the_control_plane(tmp_path: Path, fake_python: Path) -> None:
    """``notebook.run`` submits a child job through the job's callback token. Without a control plane the runner reports that precisely (the real child run is covered end to end)."""
    r = run_notebook(CASES["pipeline_invocation"], tmp_path, fake_python, timeout=120)
    assert r.state == "failed"
    text = json.dumps(r.failure) + r.cell_text()
    assert "callback" in text.lower() and "NotSupported" in text
