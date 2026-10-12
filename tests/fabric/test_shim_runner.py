# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The ``notebookutils`` / ``mssparkutils`` shim exercised through the real runner process and a real Jupyter kernel (Spark-less runtime: ``engine=python``)."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest

from olcf.fabric import notebook as nbm
from olcf.fabric.analyzer import AnalysisOptions, analyze_text
from olcf.fabric.notebook import to_git_source
from olcf.runtime_lib.notebookutils.naming import secret_env_name
from tests.fabric.fabric_support import ipynb
from tests.fabric.runner_harness import RunResult, lakehouse, run_job

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def runtime_python(tmp_path_factory: pytest.TempPathFactory) -> Path:
    sys.path.insert(0, str(ROOT))
    from tests.node.fakes import make_fake_runtime

    root = make_fake_runtime(tmp_path_factory.mktemp("shim-rt"), "olcf-rt-fake-shim", family="fabric-2.0-compatible")
    return root / "venv" / "bin" / "python"


@pytest.fixture()
def run(tmp_path: Path, runtime_python: Path):
    counter = [0]

    def go(cells: list[tuple], **kw: Any) -> RunResult:
        counter[0] += 1
        return run_job(tmp_path / f"job{counter[0]}", ipynb(cells), python=runtime_python, **kw)

    return go


def test_exit_value_stops_the_notebook(run) -> None:
    r = run([("code", "import json\nprint('first')"), ("code", "notebookutils.notebook.exit(json.dumps({'a': 1}))"), ("code", "print('never')")])
    assert r.state == "succeeded" and r.returncode == 0 and json.loads(r.exit_value) == {"a": 1}
    assert "first" in r.cell_text() and "never" not in r.cell_text()
    assert r.result["metrics"]["exit_called"] if "exit_called" in r.result["metrics"] else True


def test_mssparkutils_alias_and_imports(run) -> None:
    r = run([("code", "import notebookutils\nfrom notebookutils import mssparkutils as m1\nimport mssparkutils as m2\nassert m1 is m2 is notebookutils\nprint(notebookutils.__version__)")])
    assert r.state == "succeeded", r.failure


def test_runtime_context_is_rebuilt_from_the_capsule(run, tmp_path: Path) -> None:
    ctx = {"fabric": {"notebook_name": "etl_orders", "notebook_id": "nb-1", "workspace_id": "ws-fabric", "workspace_name": "Sales", "is_for_pipeline": True,
                      "user_name": "alice@contoso.example", "context": {"currentRunId": "fabric-run-9", "rootRunId": "root-1", "activityId": "act-1"}}}
    r = run([("code", "import json\nc = notebookutils.runtime.context\nnotebookutils.notebook.exit(json.dumps({k: c.get(k) for k in "
                      "['currentNotebookName','currentWorkspaceId','isForPipeline','userName','currentRunId','rootRunId','productType','defaultLakehouseName']}))")],
            context=ctx, default_lakehouse=lakehouse(tmp_path / "lh", "bronze"))
    got = json.loads(r.exit_value)
    assert got["currentNotebookName"] == "etl_orders" and got["currentWorkspaceId"] == "ws-fabric" and got["isForPipeline"] is True
    assert got["userName"] == "alice@contoso.example" and got["currentRunId"] == "fabric-run-9" and got["rootRunId"] == "root-1" and got["defaultLakehouseName"] == "bronze"
    assert got["productType"] != "Fabric"                                      # we never pretend to be Microsoft Fabric


def test_fs_against_a_local_lakehouse(run, tmp_path: Path, runtime_python: Path) -> None:
    lh = lakehouse(tmp_path / "lh", "bronze")
    code = """
import json
fs = notebookutils.fs
fs.mkdirs('Files/in')
ok = fs.put('Files/in/a.txt', 'hello', True)
fs.append('Files/in/a.txt', ' world', True)
head = fs.head('Files/in/a.txt')
fs.cp('Files/in/a.txt', 'Files/in/b.txt')
fs.mv('Files/in/b.txt', 'Files/in/c.txt')
names = sorted(f.name for f in fs.ls('Files/in'))
exists = [fs.exists('Files/in/a.txt'), fs.exists('Files/in/b.txt'), fs.exists('Files/in/c.txt')]
with open('/lakehouse/default/Files/in/a.txt') as fh:          # the Fabric FUSE spelling works on a local lakehouse
    fuse = fh.read()
fs.rm('Files/in/c.txt')
after = sorted(f.name for f in fs.ls('Files/in'))
notebookutils.notebook.exit(json.dumps({'head': head, 'names': names, 'exists': exists, 'fuse': fuse, 'after': after}))
"""
    # as in the product, the analyzer rewrites the FUSE spelling /lakehouse/default/... to the lakehouse's real path before the runner sees the notebook
    rewritten = analyze_text(ipynb([("code", code)]), options=AnalysisOptions(lakehouse={"kind": "local", "root_path": str(tmp_path / "lh"), "name": "bronze"}))
    assert any(rw["rule"] for rw in rewritten.report["rewrites"])
    r = run_job(tmp_path / "fs-job", rewritten.source("ipynb"), python=runtime_python, default_lakehouse=lh)
    assert r.state == "succeeded", (r.failure, r.cell_text())
    assert json.loads(r.exit_value) == {"head": "hello world", "names": ["a.txt", "c.txt"], "exists": [True, False, True], "fuse": "hello world", "after": ["a.txt"]}
    assert (tmp_path / "lh" / "Files" / "in" / "a.txt").read_text() == "hello world"


def test_path_escape_is_refused(run, tmp_path: Path) -> None:
    (tmp_path / "secret.txt").write_text("nope")
    r = run([("code", "notebookutils.fs.head('Files/../../secret.txt')")], default_lakehouse=lakehouse(tmp_path / "lh"))
    assert r.state == "failed" and "escapes" in json.dumps(r.failure) + r.cell_text()


def test_lakehouse_on_object_storage_cannot_be_opened_as_a_local_path(run) -> None:
    remote = {"name": "gold", "id": "lh-2", "kind": "remote", "root_uri": "abfss://fs@onelake.dfs.fabric.microsoft.com/gold.Lakehouse", "files_uri": "abfss://fs@onelake.dfs.fabric.microsoft.com/gold.Lakehouse/Files",
              "tables_uri": "abfss://fs@onelake.dfs.fabric.microsoft.com/gold.Lakehouse/Tables"}
    r = run([("code", "open('/lakehouse/default/Files/x.csv').read()")], default_lakehouse=remote)
    assert r.state == "failed" and r.failure["category"] == "storage"          # the analyzer blocks this before it ever gets here (tests/fabric/test_analyzer.py)
    assert r.failure.get("remediation")


def test_lakehouse_namespace(run, tmp_path: Path) -> None:
    lh = lakehouse(tmp_path / "lh", "bronze", fabric={"lakehouse_id": "fab-lh-1", "workspace_id": "fab-ws-1"})
    other = lakehouse(tmp_path / "lh2", "silver", lh_id="lh-0002")
    r = run([("code", "import json\nnames = sorted(x['displayName'] if 'displayName' in x else x['name'] for x in notebookutils.lakehouse.list())\nnotebookutils.notebook.exit(json.dumps(names))")],
            default_lakehouse=lh, lakehouses=[other])
    assert r.state == "succeeded", (r.failure, r.cell_text())
    assert json.loads(r.exit_value) == ["bronze", "silver"]


def test_secrets_only_when_granted(run) -> None:
    vault, name = "https://kv-prod.vault.azure.net/", "api-key"
    env = {secret_env_name(vault, name): "granted-value-123"}
    code = f"print(notebookutils.credentials.getSecret('{vault}', '{name}'))\nnotebookutils.credentials.getSecret('{vault}', 'other')"
    r = run([("code", code)], env=env)
    assert r.state == "failed" and "was not granted" in json.dumps(r.failure) + r.cell_text()
    assert "other" in r.cell_text()
    assert r.failure.get("category") == "user-code"
    r2 = run([("code", f"notebookutils.notebook.exit(notebookutils.credentials.getSecret('kv-prod', '{name}'))")], env=env)    # vault by short name
    assert r2.state == "succeeded" and r2.exit_value == "granted-value-123"


@pytest.mark.parametrize("call,needle", [
    ("notebookutils.warehouse.connect('x')", "warehouse"),
    ("notebookutils.variableLibrary.get('x')", "variableLibrary"),
    ("notebookutils.runtime.restartPython()", "restartPython"),
    ("notebookutils.credentials.getToken('pbi')", "getToken"),
    ("notebookutils.env.getPoolName()", "getPoolName"),
    ("notebookutils.nonexistent.thing()", "nonexistent"),
])
def test_unsupported_areas_fail_loudly_with_remediation(run, call: str, needle: str) -> None:
    r = run([("code", call)])
    assert r.state == "failed"
    blob = json.dumps(r.failure) + r.cell_text()
    assert needle in blob and "NotSupported" in blob
    assert r.failure.get("message")


def test_failure_is_classified_with_cell_and_line(run) -> None:
    r = run([("code", "x = 1"), ("code", "y = 2\nraise ValueError('bad input 42')")])
    assert r.state == "failed" and r.returncode == 1
    assert r.failure["category"] == "user-code" and "bad input 42" in r.failure["message"]
    assert r.failure.get("cell") == 1 and r.failure.get("line") == 2


def test_parameters_override_the_parameters_cell(run) -> None:
    r = run([("code", "n = 3\nname = 'default'", ["parameters"]), ("code", "import json\nnotebookutils.notebook.exit(json.dumps([n, name]))")], parameters={"n": 9})
    assert json.loads(r.exit_value) == [9, "default"]


def test_git_source_format_runs_the_same(run, tmp_path: Path, runtime_python: Path) -> None:
    nb = nbm.parse(ipynb([("code", "n = 2", ["parameters"]), ("code", "notebookutils.notebook.exit(str(n * 21))")]))
    r = run_job(tmp_path / "git", to_git_source(nb), python=runtime_python, fmt="fabric_git_source", parameters={"n": 2})
    assert r.state == "succeeded" and r.exit_value == "42"


def test_pip_magic_is_blocked_at_runtime_by_policy(run) -> None:
    r = run([("code", "%pip install olcf-surely-not-a-package-xyz")])
    assert r.state == "failed" and "blocked by policy" in (json.dumps(r.failure) + r.cell_text())
    ok = run([("code", "%pip install nbformat")])                                  # already part of the declared environment: nothing to install, nothing to block
    assert ok.state == "succeeded" and "already satisfied" in ok.cell_text()


def test_exit_value_is_size_limited_visibly(run) -> None:
    r = run([("code", "notebookutils.notebook.exit('x' * 5000)")], limits={"timeout_seconds": 100, "max_output_bytes": 1024})
    assert r.state == "succeeded"
    assert isinstance(r.exit_value, dict) and r.exit_value.get("_truncated") is True
