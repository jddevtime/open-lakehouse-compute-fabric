#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Regenerate the corpus fixtures and goldens.

    python tests/corpus/regen.py --fixtures     # notebook.ipynb / notebook-content.py / input data / negative notebooks
    python tests/corpus/regen.py --analysis     # expected.analysis.json (from the analyzer; review the diff)
    python tests/corpus/regen.py --data         # expected.data.json (from standalone.py; Spark cases need the cached Spark runtime and take the sandbox Spark lock)

Without arguments all three run. Needs ``PYTHONPATH=src``. Goldens are produced by code that is independent of the notebook runtime (analyzer; standalone scripts), and the
tests make the notebook run reproduce the data goldens.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
for p in (str(ROOT), str(ROOT / "src")):
    if p not in sys.path:
        sys.path.insert(0, p)

from olcf.fabric import notebook as nbm  # noqa: E402
from olcf.fabric.analyzer import AnalysisOptions, analyze_text  # noqa: E402
from tests.corpus.corpus_defs import ANALYSIS_ROOT, CASES, CASES_DIR, NEG_DIR, NEGATIVES, Case  # noqa: E402

VOLATILE = {"duration_ms", "source"}


def build_ipynb(cells: list[tuple]) -> str:
    out = []
    for c in cells:
        tags = c[2] if len(c) > 2 else []
        cell: dict[str, Any] = {"cell_type": c[0], "metadata": {"tags": tags} if tags else {}, "source": c[1]}
        if c[0] == "code":
            cell.update({"execution_count": None, "outputs": []})
        out.append(cell)
    doc = {"nbformat": 4, "nbformat_minor": 5, "cells": out,
           "metadata": {"kernelspec": {"name": "synapse_pyspark", "display_name": "Synapse PySpark", "language": "Python"}, "language_info": {"name": "python"},
                        "microsoft": {"language": "python", "language_group": "synapse_pyspark"}}}
    return json.dumps(doc, indent=1, sort_keys=True) + "\n"


def build_git(cells: list[tuple]) -> str:
    return nbm.to_git_source(nbm.parse_ipynb(build_ipynb(cells)))


def texts(case: Case) -> dict[str, str]:
    return {"notebook.ipynb": build_ipynb(case.cells), "notebook-content.py": build_git(case.cells)}


def options(case: Case, root: str | None = None) -> AnalysisOptions:
    lake = None if case.lakehouse == "none" else ({"kind": "remote", "name": "gold"} if case.lakehouse == "remote" else {"kind": "local", "root_path": root or ANALYSIS_ROOT, "name": "bronze"})
    bundled = {n: build_ipynb(cells) for n, cells in case.bundled.items()}
    return AnalysisOptions(lakehouse=lake, bundled_notebooks=bundled, allow_pip_install=case.allow_pip, allow_shell=case.allow_shell, notebook_name=case.name)


def golden_view(report: dict[str, Any]) -> dict[str, Any]:
    """The stable part of an analyzer report: what a rule change must be reviewed against."""
    def rw(r: dict[str, Any]) -> list[Any]:
        return [r["rule"], r["cell"], r["line"]]

    refs = report["data_references"]
    return {
        "analyzer_version": report["analyzer_version"], "ruleset_version": report["ruleset_version"], "blocked": report["blocked"], "counts": report["counts"],
        "rule_counts": report["rule_counts"], "blockers": sorted({b["rule"] for b in report["blockers"]}), "shims_required": report["shims_required"],
        "rewrites": [rw(r) for r in report["rewrites"]], "inlined_runs": report["inlined_runs"],
        "data_references": {"paths": sorted([p["kind"], p["uri"]] for p in refs["paths"]), "tables": sorted([t["ref"], t["kind"], sorted(t["roles"])] for t in refs["tables"])},
        "imports": report["imports"], "packages": report["packages"], "pip_requirements": [r["requirement"] for r in report["pip_requirements"]],
        "parameters": [[p["name"], p["default"], p["type"]] for p in report["parameters"]], "child_notebooks": report["child_notebooks"],
        "secret_refs": report["secret_refs"],
    }


def analyze_case(case: Case, fmt: str):
    text = texts(case)["notebook.ipynb" if fmt == "ipynb" else "notebook-content.py"]
    return analyze_text(text, format="ipynb" if fmt == "ipynb" else "fabric_git_source", options=options(case))


def dump(doc: Any) -> str:
    return json.dumps(doc, indent=2, sort_keys=True) + "\n"


def write_fixtures() -> None:
    for case in CASES.values():
        d = CASES_DIR / case.name
        d.mkdir(parents=True, exist_ok=True)
        for name, text in texts(case).items():
            (d / name).write_text(text, encoding="utf-8")
        for rel, body in case.files.items():
            f = d / "input" / "Files" / rel
            f.parent.mkdir(parents=True, exist_ok=True)
            f.write_text(body, encoding="utf-8")
        for lh in case.extra_lakehouses:
            (d / "input" / lh / "Files").mkdir(parents=True, exist_ok=True)
            (d / "input" / lh / "Files" / ".keep").write_text("", encoding="utf-8")
        (d / "case.json").write_text(dump({"name": case.name, "description": case.description, "engine": case.engine, "primary_format": case.primary, "parameters": case.parameters,
                                          "standalone_engine": case.standalone_engine or case.engine, "extra_lakehouses": case.extra_lakehouses, "data_run": case.data_run, "why": case.why, "needs": case.needs}), encoding="utf-8")
    NEG_DIR.mkdir(parents=True, exist_ok=True)
    for neg in NEGATIVES:
        (NEG_DIR / f"{neg.name}.ipynb").write_text(build_ipynb(neg.cells), encoding="utf-8")


def write_analysis() -> None:
    for case in CASES.values():
        rep = analyze_case(case, case.primary).report
        (CASES_DIR / case.name / "expected.analysis.json").write_text(dump(golden_view(rep)), encoding="utf-8")
    from tests.corpus.corpus_defs import Negative  # noqa: PLC0415

    out: dict[str, Any] = {}
    for neg in NEGATIVES:
        assert isinstance(neg, Negative)
        case = Case(neg.name, neg.description, neg.cells, lakehouse=neg.lakehouse, allow_pip=neg.allow_pip, allow_shell=neg.allow_shell)
        res = analyze_text((NEG_DIR / f"{neg.name}.ipynb").read_text(), format="ipynb", options=options(case))
        out[neg.name] = {"description": neg.description, "lakehouse": neg.lakehouse, "blocked": res.report["blocked"], "blockers": sorted({b["rule"] for b in res.report["blockers"]}),
                         "rule_counts": res.report["rule_counts"]}
    (NEG_DIR / "expected.json").write_text(dump(out), encoding="utf-8")


def write_data(only: list[str] | None = None) -> None:
    from tests.corpus.standalone_driver import run_all  # noqa: PLC0415

    got = run_all([c for c in CASES if not only or c in only])
    for name, doc in got.items():
        (CASES_DIR / name / "expected.data.json").write_text(dump(doc), encoding="utf-8")
        print("data golden:", name)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fixtures", action="store_true")
    ap.add_argument("--analysis", action="store_true")
    ap.add_argument("--data", action="store_true")
    ap.add_argument("--only", nargs="*")
    a = ap.parse_args()
    everything = not (a.fixtures or a.analysis or a.data)
    if a.fixtures or everything:
        write_fixtures()
    if a.analysis or everything:
        write_analysis()
    if a.data or everything:
        write_data(a.only)


if __name__ == "__main__":
    main()
