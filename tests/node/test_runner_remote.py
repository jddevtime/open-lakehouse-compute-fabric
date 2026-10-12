# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The runner's remote completion contract (review F03/F07): ``--workdir`` staging, the single ``result`` event, the termination record - and the unchanged agent path."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from olcf.runtime_lib import runner

REPO = Path(__file__).resolve().parents[2]


def write_job(root: Path, source: str, **spec_over: Any) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "source").mkdir(exist_ok=True)
    (root / "source" / "main.py").write_text(source)
    spec = {"schema": "olcf.jobspec/1", "job_id": "job_T1", "attempt": 3, "kind": "script", "engine": "python", "source": {"path": "source/main.py", "format": "py"},
            "parameters": {}, "limits": {"timeout_seconds": 60, "max_output_bytes": 5_000_000}, "callbacks": {"api_url": "", "job_token": ""}, "secrets": {"TOKEN": "s3cret"}, **spec_over}
    (root / "job.json").write_text(json.dumps(spec))
    return root / "job.json"


def run_runner(*args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    full = {"PATH": os.environ.get("PATH", ""), "PYTHONPATH": f"{REPO / 'src'}", "HOME": str(REPO), "LANG": "C.UTF-8", **(env or {})}
    return subprocess.run([sys.executable, "-m", "olcf.runtime_lib.runner", *args], capture_output=True, text=True, env=full, check=False, timeout=120)  # noqa: S603


def events(out: str) -> list[dict[str, Any]]:
    return [json.loads(line[8:]) for line in out.splitlines() if line.startswith("@@OLCF@@")]


def test_agent_mode_is_unchanged_it_writes_result_json_and_emits_no_result_event(tmp_path: Path) -> None:
    """Review F07 evidence, and the guard that the agent path keeps its behaviour: without ``--workdir`` / ``--remote`` only ``runner_finished`` is emitted."""
    job = write_job(tmp_path / "job", "EXIT_VALUE = {'expected': 'value'}\n")
    proc = run_runner(str(job))
    assert proc.returncode == 0, proc.stderr
    assert json.loads((job.parent / "result.json").read_text())["exit_value"] == {"expected": "value"}
    assert [e["event"] for e in events(proc.stdout)] == ["runner_started", "runner_finished"]
    assert (job.parent / "job.json").exists(), "the agent owns its job directory; the runner does not delete job.json there"


def test_remote_mode_stages_the_read_only_input_and_emits_exactly_one_result_event(tmp_path: Path) -> None:
    inp = write_job(tmp_path / "input", "print('hi')\nEXIT_VALUE = [1, 2, 3]\n")
    os.chmod(inp.parent, 0o555)  # noqa: S103 - the input volume is read-only
    work = tmp_path / "work"
    term = tmp_path / "term"
    try:
        proc = run_runner("--workdir", str(work), str(inp), env={"OLCF_TERMINATION_LOG": str(term)})
    finally:
        os.chmod(inp.parent, 0o755)  # noqa: S103
    assert proc.returncode == 0, proc.stderr
    evs = events(proc.stdout)
    assert [e["event"] for e in evs] == ["runner_started", "result", "runner_finished"]
    res = evs[1]
    assert res["job_id"] == "job_T1" and res["attempt"] == 3 and res["result"]["state"] == "succeeded" and res["result"]["exit_value"] == [1, 2, 3]
    assert json.loads((work / "result.json").read_text()) == res["result"], "the event is the persisted document"
    assert not (work / "job.json").exists(), "the staged copy of the spec (job token, secrets) is removed when the run is over"
    assert (inp.parent / "job.json").exists()
    rec = json.loads(term.read_text())
    assert rec["v"] == 1 and rec["job_id"] == "job_T1" and rec["result"]["state"] == "succeeded" and rec["result"]["exit_value"] == [1, 2, 3] and len(term.read_bytes()) <= 4096


def test_remote_failure_is_reported_with_exit_status_and_category(tmp_path: Path) -> None:
    inp = write_job(tmp_path / "in", "raise ValueError('nope')\n")
    term = tmp_path / "t"
    proc = run_runner("--workdir", str(tmp_path / "w"), str(inp), env={"OLCF_TERMINATION_LOG": str(term)})
    assert proc.returncode == 1
    (res,) = [e for e in events(proc.stdout) if e["event"] == "result"]
    assert res["result"]["state"] == "failed" and res["result"]["failure"]["category"] == "user-code" and "nope" in json.dumps(res["result"]["failure"])
    assert json.loads(term.read_text())["result"]["failure"]["category"] == "user-code"


def test_oversized_result_event_is_capped_and_references_the_full_document(tmp_path: Path) -> None:
    inp = write_job(tmp_path / "in", "EXIT_VALUE = {'blob': 'z' * 600_000}\n", limits={"timeout_seconds": 60, "max_output_bytes": 5_000_000, "result_event_bytes": 20_000})
    proc = run_runner("--workdir", str(tmp_path / "w"), str(inp), env={"OLCF_TERMINATION_LOG": str(tmp_path / "t")})
    assert proc.returncode == 0, proc.stderr
    (res,) = [e for e in events(proc.stdout) if e["event"] == "result"]
    line = next(x for x in proc.stdout.splitlines() if '"event": "result"' in x)
    assert len(line.encode()) <= 20_000 + 600, len(line)
    full = (tmp_path / "w" / "result.json").read_bytes()
    ref = res["result"]["result_ref"]
    assert ref["bytes"] == len(full) and ref["sha256"] == hashlib.sha256(full).hexdigest() and ref["complete"] is False
    assert res["result"]["exit_value"]["_truncated"] is True
    assert json.loads((tmp_path / "t").read_text())["result"]["partial"] is True


def test_unreadable_input_still_delivers_a_failure_result(tmp_path: Path) -> None:
    inp = tmp_path / "in"
    inp.mkdir()
    (inp / "job.json").write_text("{not json")
    proc = run_runner("--workdir", str(tmp_path / "w"), str(inp / "job.json"), env={"OLCF_JOB_ID": "job_X", "OLCF_ATTEMPT": "2", "OLCF_TERMINATION_LOG": str(tmp_path / "t")})
    assert proc.returncode == 1
    (res,) = [e for e in events(proc.stdout) if e["event"] == "result"]
    assert res["job_id"] == "job_X" and res["attempt"] == 2 and res["result"]["state"] == "failed" and "job.json" in res["result"]["failure"]["message"]


def test_usage_errors_exit_2() -> None:
    assert run_runner().returncode == 2
    assert run_runner("--workdir").returncode == 2


def test_stage_inputs_skips_kubelet_bookkeeping_and_follows_links(tmp_path: Path) -> None:
    src, dst = tmp_path / "src", tmp_path / "dst"
    (src / "..2026_01_01").mkdir(parents=True)
    (src / "..2026_01_01" / "job.json").write_text("{}")
    (src / "..2026_01_01" / "main.py").write_text("x")
    os.symlink("..2026_01_01", src / "..data")
    os.symlink("..data/job.json", src / "job.json")
    (src / "source").mkdir()
    os.symlink("../..data/main.py", src / "source" / "main.py")
    runner.stage_inputs(src, dst)
    assert sorted(str(p.relative_to(dst)) for p in dst.rglob("*") if p.is_file()) == ["job.json", "source/main.py"]
    assert not (dst / "job.json").is_symlink() and oct((dst / "job.json").stat().st_mode & 0o777) == "0o600"


@pytest.mark.parametrize("name", ["urllib", "hashlib"])
def test_the_runner_keeps_to_the_runtime_library_import_rule(name: str) -> None:
    """Only the standard library and olcf.core.* helpers - the remote contract added urllib and hashlib, nothing else."""
    src = (REPO / "src" / "olcf" / "runtime_lib" / "runner.py").read_text()
    imports = {line.split()[1].split(".")[0] for line in src.splitlines() if line.startswith(("import ", "from "))}
    assert name in imports
    assert not imports - set(sys.stdlib_module_names) - {"olcf", "__future__"}
