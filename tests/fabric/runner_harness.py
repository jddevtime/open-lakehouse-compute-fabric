# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Runs a job through the production runner (``python -I -m olcf.runtime_lib.runner job.json``) in a scratch work directory, exactly as the worker agent spawns it
(own process, scrubbed environment, real kernel for notebooks). Used by the shim tests and the golden corpus."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]

FILE_NAMES = {"ipynb": "notebook.ipynb", "py": "main.py", "fabric_git_source": "notebook-content.py"}


@dataclass
class RunResult:
    returncode: int
    result: dict[str, Any]
    stdout: str
    stderr: str
    workdir: Path
    output: dict[str, Any] | None = None
    events: list[dict[str, Any]] = field(default_factory=list)

    @property
    def state(self) -> str:
        return str(self.result.get("state"))

    @property
    def exit_value(self) -> Any:
        return self.result.get("exit_value")

    @property
    def failure(self) -> dict[str, Any]:
        return self.result.get("failure") or {}

    def cell_text(self) -> str:
        """Everything the notebook printed / returned / raised, from the output notebook."""
        out = []
        for c in (self.output or {}).get("cells", []):
            for o in c.get("outputs", []):
                t = o.get("text") or (o.get("data") or {}).get("text/plain") or o.get("evalue") or ""
                out.append("".join(t) if isinstance(t, list) else str(t))
        return "\n".join(out)


def lakehouse(root: Path, name: str = "bronze", *, lh_id: str = "lh-0001", fabric: dict[str, str] | None = None) -> dict[str, Any]:
    """A local-filesystem lakehouse record shaped like the ones the control plane puts into a job spec."""
    (root / "Files").mkdir(parents=True, exist_ok=True)
    (root / "Tables").mkdir(parents=True, exist_ok=True)
    rec = {"name": name, "id": lh_id, "kind": "local", "root_uri": root.as_uri(), "files_uri": (root / "Files").as_uri(), "tables_uri": (root / "Tables").as_uri(), "format": "delta"}
    if fabric:
        rec["fabric"] = fabric
    return rec


def run_job(workdir: Path, source: str, *, python: str | os.PathLike[str], fmt: str = "ipynb", kind: str = "capsule", engine: str = "python", parameters: dict[str, Any] | None = None,
            timeout: float = 180.0, env: dict[str, str] | None = None, runtime_dir: Path | None = None, **spec_over: Any) -> RunResult:
    workdir.mkdir(parents=True, exist_ok=True)
    for sub in ("source", "tmp", "home", "spark-local"):
        (workdir / sub).mkdir(exist_ok=True)
    rel = "source/" + FILE_NAMES[fmt]
    (workdir / rel).write_text(source, encoding="utf-8")
    spec: dict[str, Any] = {"schema": "olcf.jobspec/1", "job_id": "job_corpus_0001", "attempt": 1, "kind": kind, "engine": engine, "name": "corpus",
                            "workspace": {"id": "wks_1", "name": "w"}, "source": {"format": fmt, "path": rel, "sha256": hashlib.sha256(source.encode()).hexdigest()},
                            "parameters": parameters or {}, "spark": {"master": "local[1]", "conf": {}}, "runtime": {"id": "rt", "family": "fabric-2.0-compatible"},
                            "limits": {"timeout_seconds": int(timeout) - 10}, "env": {}, "secrets": {}, "context": {}}
    spec.update(spec_over)
    job = workdir / "job.json"
    job.write_text(json.dumps(spec))
    job.chmod(0o600)
    base_env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(workdir / "home"), "TMPDIR": str(workdir / "tmp"), "LANG": "C.UTF-8", "PYTHONUNBUFFERED": "1",
                "OLCF_JOB_ID": spec["job_id"], "PYTHONPATH": str(ROOT / "src")}
    if runtime_dir is not None:
        base_env["OLCF_RUNTIME_DIR"] = str(runtime_dir)
    base_env.update(env or {})
    proc = subprocess.run([str(python), "-m", "olcf.runtime_lib.runner", str(job)], cwd=workdir, env=base_env, capture_output=True, text=True, timeout=timeout, check=False)  # noqa: S603
    try:
        result = json.loads((workdir / "result.json").read_text())
    except (OSError, ValueError):
        result = {"state": "no-result"}
    out_nb = None
    if (workdir / "output.ipynb").is_file():
        out_nb = json.loads((workdir / "output.ipynb").read_text())
    events = []
    for line in proc.stdout.splitlines():
        if line.startswith("@@OLCF@@"):
            try:
                events.append(json.loads(line[len("@@OLCF@@"):]))
            except ValueError:
                continue
    return RunResult(proc.returncode, result, proc.stdout, proc.stderr, workdir, out_nb, events)
