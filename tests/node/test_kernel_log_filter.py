# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""FIX F: the benign ``IPKernelApp`` "TCP without encryption" warning must not leak into job / Fabric submission logs - and no other warning may be swallowed."""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

from olcf.runtime_lib.kernel_host import BenignKernelNoise

ROOT = Path(__file__).resolve().parents[2]
TCP_WARNING = ("Kernel is running over TCP without encryption. All communication (including code and outputs) is sent in plain text and is susceptible to eavesdropping.")


def _record(name: str, level: int, msg: str) -> logging.LogRecord:
    return logging.LogRecord(name, level, __file__, 1, msg, None, None)


def test_filter_matches_exactly_the_known_warning():
    f = BenignKernelNoise()
    assert f.filter(_record("IPKernelApp", logging.WARNING, TCP_WARNING)) is False
    # everything else passes: other warnings of the same logger, the same text from another logger or at another level, other kernels' messages
    assert f.filter(_record("IPKernelApp", logging.WARNING, "Failed to load connection file")) is True
    assert f.filter(_record("IPKernelApp", logging.WARNING, "Message signing is disabled.  This is insecure and not recommended!")) is True
    assert f.filter(_record("IPKernelApp", logging.ERROR, TCP_WARNING)) is True
    assert f.filter(_record("other", logging.WARNING, TCP_WARNING)) is True
    assert f.filter(_record("IPKernelApp", logging.WARNING, "x " + TCP_WARNING)) is True


def test_a_real_kernel_host_does_not_print_the_warning(tmp_path):
    """Starts the production ``kernel_host`` with the interpreter that runs the tests (ipykernel is installed there) and reads what it writes to stderr."""
    spec = {"schema": "olcf.jobspec/1", "job_id": "job_kh", "attempt": 1, "kind": "notebook", "engine": "python", "workspace": {"id": "wks_x", "name": "x"}, "name": "kh",
            "source": {"format": "ipynb", "path": "n.ipynb", "sha256": "0"}, "parameters": {}, "spark": {"master": "local[1]", "conf": {}}, "runtime": {}, "env": {}, "limits": {}}
    (tmp_path / "spec.json").write_text(json.dumps(spec))
    env = {**os.environ, "PYTHONPATH": str(ROOT / "src"), "JUPYTER_RUNTIME_DIR": str(tmp_path), "HOME": str(tmp_path)}
    err = tmp_path / "err.txt"
    with open(err, "wb") as ef, subprocess.Popen([sys.executable, "-m", "olcf.runtime_lib.kernel_host", "--spec", str(tmp_path / "spec.json"), "-f", str(tmp_path / "conn.json")],  # noqa: S603
                                                  stdout=subprocess.PIPE, stderr=ef, env=env, cwd=str(tmp_path)) as p:
        try:
            ready = False
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline and not ready:
                line = p.stdout.readline().decode()  # type: ignore[union-attr]
                ready = '"kernel_ready"' in line
                if not line and p.poll() is not None:
                    break
            assert ready, err.read_text()
            time.sleep(0.5)
        finally:
            p.kill()
            p.wait(10)
    assert "without encryption" not in err.read_text()
