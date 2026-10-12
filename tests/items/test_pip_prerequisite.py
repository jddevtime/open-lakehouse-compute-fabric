# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""pip is a prerequisite of environment publication and of the Fabric SDK build (review finding F18).

A virtual environment made with ``uv venv`` has no pip; ``uv pip install -e ".[dev]"`` did not add it, so ``python -m pip`` failed with "No module named pip" and the
environment resolver reported that as "pip could not resolve the requirements".  These tests run the real code against an interpreter that has no pip.
"""

from __future__ import annotations

import os
import subprocess
import sys
import venv
from pathlib import Path

import pytest

from olcf.notebooks import piplock
from tests.items.test_environments import (  # noqa: F401 - fixture reused: a wheelhouse registered as approved
    env_def,
    wh,
)

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def no_pip_python(tmp_path_factory: pytest.TempPathFactory) -> str:
    """The interpreter of a virtual environment created WITHOUT pip (what ``uv venv`` makes by default)."""
    d = tmp_path_factory.mktemp("nopip")
    venv.EnvBuilder(with_pip=False, system_site_packages=False, clear=True, symlinks=os.name != "nt").create(d)
    py = d / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    probe = subprocess.run([str(py), "-m", "pip", "--version"], capture_output=True, text=True, check=False)
    if probe.returncode == 0:
        pytest.skip("this platform's virtual environments always contain pip")
    return str(py)


def test_run_pip_without_pip_raises_an_actionable_error(tmp_path: Path, no_pip_python: str) -> None:
    with pytest.raises(piplock.PipMissing) as ei:
        piplock.run_pip(["--version"], home=tmp_path, timeout=30, python=no_pip_python)
    msg = str(ei.value)
    assert "pip is not installed" in msg and no_pip_python in msg
    assert "ensurepip" in ei.value.remediation and "--seed" in ei.value.remediation


def test_resolve_does_not_blame_the_requirements_when_pip_is_missing(tmp_path: Path, no_pip_python: str) -> None:
    with pytest.raises(piplock.PipMissing):
        piplock.resolve(["olcf-demo-a"], index=piplock.IndexSpec(no_index=True, find_links=[str(tmp_path)]), series="3.13", platforms=["linux-x86_64"], python=no_pip_python, timeout=30)


def test_a_working_pip_is_not_reported_missing(tmp_path: Path) -> None:
    pytest.importorskip("pip")
    res = piplock.run_pip(["--version"], home=tmp_path, timeout=60)
    assert res.returncode == 0 and res.stdout.startswith("pip ")


def test_publishing_an_environment_without_pip_says_how_to_fix_it(ic, wh, monkeypatch, no_pip_python):  # noqa: F811
    ws = ic.workspace()
    env = ic.item(ws["id"], "environment", "needs-pip", env_def(wh, ["olcf-demo-a"]))
    monkeypatch.setattr(piplock.sys, "executable", no_pip_python)
    r = ic.post(f"/v1/environments/{env['id']}/publish", json={}, headers=ic.admin)
    monkeypatch.undo()
    assert r.status_code == 422, r.text
    err = r.json()["error"]
    assert "pip is not installed" in err["message"] and "could not resolve" not in err["message"]
    assert "ensurepip" in err["remediation"]


def test_building_a_layer_without_pip_names_the_missing_tool(ic, wh, tmp_path, no_pip_python):  # noqa: F811
    from olcf.notebooks import layer

    env = ic.item(ic.workspace()["id"], "environment", "layer-needs-pip", env_def(wh, ["olcf-demo-a"]))
    assert ic.post(f"/v1/environments/{env['id']}/publish", json={}, headers=ic.admin).status_code == 201
    spec = ic.ctx.service("environments").resolve(env["id"])
    with pytest.raises(layer.LayerError, match="pip is not installed") as ei:
        layer.ensure_layer(spec["layer"], tmp_path / "layers", python=no_pip_python, timeout=60)
    assert "ensurepip" in str(ei.value)


@pytest.mark.skipif(os.name == "nt", reason="the SDK build script is a bash script")
def test_sdk_build_script_checks_its_prerequisites(tmp_path: Path, no_pip_python: str) -> None:
    res = subprocess.run(["bash", str(ROOT / "scripts" / "build_fabric_sdk.sh"), str(tmp_path / "dist")], capture_output=True, text=True,
                         env={**os.environ, "PYTHON": no_pip_python}, timeout=110, check=False)
    assert res.returncode == 2, res.stdout + res.stderr
    assert "pip is not installed" in res.stderr and "ensurepip" in res.stderr and "--seed" in res.stderr
    assert not (tmp_path / "dist").exists() or not list((tmp_path / "dist").glob("*.whl"))


@pytest.mark.skipif(os.name == "nt", reason="the SDK build script is a bash script")
def test_sdk_build_script_reports_a_missing_interpreter(tmp_path: Path) -> None:
    res = subprocess.run(["bash", str(ROOT / "scripts" / "build_fabric_sdk.sh"), str(tmp_path / "dist")], capture_output=True, text=True,
                         env={**os.environ, "PYTHON": str(tmp_path / "no-such-python")}, timeout=60, check=False)
    assert res.returncode == 2 and "cannot run" in res.stderr


@pytest.mark.skipif(sys.platform == "win32", reason="bash script")
def test_sdk_build_script_still_builds_with_a_seeded_environment(tmp_path: Path) -> None:
    pytest.importorskip("pip")
    try:
        import setuptools  # noqa: F401, PLC0415
    except Exception as exc:  # noqa: BLE001 - an ImportError, but also the AssertionError setuptools' distutils shim raises on some managed interpreters
        pytest.skip(f"setuptools cannot be imported in this interpreter: {exc}")
    res = subprocess.run(["bash", str(ROOT / "scripts" / "build_fabric_sdk.sh"), str(tmp_path / "dist")], capture_output=True, text=True,
                         env={**os.environ, "PYTHON": sys.executable}, timeout=110, check=False)
    assert res.returncode == 0, res.stderr + res.stdout
