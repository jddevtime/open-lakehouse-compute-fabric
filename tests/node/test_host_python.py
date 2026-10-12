# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``olcf-host-python`` on the node: Python-engine work on the worker's own interpreter (real runner and kernel processes, no Spark runtime involved).

Covers: the runtime in the agent's hello, a script and a notebook job (fingerprint recorded in ``result.json``), the helpful error when a job asks for Spark,
a per-job dependency layer on top of the host interpreter, a kernel session (interactive notebooks), ``--no-host-python`` and that nothing can install/repair/retire it.
"""

# ruff: noqa: S101
from __future__ import annotations

import json
import os
import sys

import pytest

from olcf.core.errors import OlcfError  # noqa: E402
from olcf.core.hostruntime import HOST_RUNTIME_ID  # noqa: E402
from olcf.notebooks import layer as layer_mod  # noqa: E402
from olcf.runtime import hostpython  # noqa: E402
from olcf.runtime.manager import ManagerConfig, RuntimeManager  # noqa: E402
from tests.items.wheelbuilder import build_wheel  # noqa: E402
from tests.node.conftest import job_spec, wait_final
from tests.node.test_kernels import events, run

W = "wrk_1"


def host_job(job_id: str, src: str, **over):
    rt = over.pop("runtime", None) or {"id": HOST_RUNTIME_ID, "family": "host-python"}
    payload = job_spec(job_id, src, runtime=HOST_RUNTIME_ID, **over)
    payload["spec"]["runtime"] = rt
    return payload


def test_hello_reports_the_host_runtime(controller, start_agent):
    start_agent()
    _, hello = controller.hellos[-1]
    rt = {r["id"]: r for r in hello["runtimes"]}
    assert rt[HOST_RUNTIME_ID]["state"] == "ready" and rt[HOST_RUNTIME_ID]["verified"] is True
    assert rt[HOST_RUNTIME_ID]["family"] == "host-python" and rt[HOST_RUNTIME_ID]["no_spark"] is True
    info = hostpython.describe()
    assert rt[HOST_RUNTIME_ID]["fingerprint"] == info["fingerprint"] and rt[HOST_RUNTIME_ID]["python"] == info["python"]


def test_script_job_runs_on_the_workers_own_python(controller, start_agent):
    start_agent()
    src = "import sys, duckdb, deltalake, pandas\nprint('exe', sys.executable)\nprint('olcf', __import__('olcf').__file__)\nEXIT_VALUE = {'n': duckdb.sql('select 41+1').fetchone()[0]}\n"
    res = controller.ok(W, "run_job", host_job("hp1", src))
    assert res["state"] == "running"
    fin = wait_final(controller, "hp1")
    assert fin["state"] == "succeeded", fin
    # the job prints the interpreter it ran on; the test may have been started through a relative path or a symlinked venv, so compare the files, not the spellings
    printed = next(line.split(" ", 1)[1] for line in controller.log_text("hp1", "stdout").splitlines() if line.startswith("exe "))
    assert os.path.samefile(printed, sys.executable), (printed, sys.executable)
    assert fin["result"]["exit_value"] == {"n": 42}
    dep = fin["result"]["dependencies"]
    info = hostpython.describe()
    assert dep["runtime_id"] == HOST_RUNTIME_ID and dep["runtime_fingerprint"] == info["fingerprint"]
    assert dep["host_python"]["python"] == info["python"] and dep["host_python"]["packages"]["duckdb"] and dep["host_python"]["no_spark"] is True


def test_notebook_job_with_outputs(controller, start_agent):
    start_agent()
    nb = {"nbformat": 4, "nbformat_minor": 5, "metadata": {"kernelspec": {"name": "python3", "display_name": "Python 3", "language": "python"}},
          "cells": [{"cell_type": "code", "id": "c1", "metadata": {}, "execution_count": None, "outputs": [], "source": "import duckdb\nprint('total', duckdb.sql('select 6*7').fetchone()[0])"},
                    {"cell_type": "code", "id": "c2", "metadata": {}, "execution_count": None, "outputs": [], "source": "import pandas as pd\npd.DataFrame({'a': [1, 2]})"}]}
    controller.ok(W, "run_job", host_job("hpnb", json.dumps(nb), kind="notebook", fmt="ipynb"))
    fin = wait_final(controller, "hpnb")
    assert fin["state"] == "succeeded", fin
    assert fin["result"]["dependencies"]["runtime_id"] == HOST_RUNTIME_ID


def test_spark_is_absent_with_a_helpful_error(controller, start_agent):
    start_agent()
    # a Python-engine script that touches `spark` gets the explanation, not a bare NameError
    controller.ok(W, "run_job", host_job("hp-nospark", "df = spark.range(3)\n"))
    fin = wait_final(controller, "hp-nospark")
    assert fin["state"] == "failed"
    msg = json.dumps(fin.get("failure") or {}) + controller.log_text("hp-nospark", "stderr")
    assert "no Spark" in msg and "olcf runtime install" in msg, msg
    # an engine=spark job that reached the node on the host runtime fails the same way (the controller never places it there)
    controller.ok(W, "run_job", host_job("hp-spark-engine", "spark.range(1).count()\n", engine="spark"))
    fin = wait_final(controller, "hp-spark-engine")
    assert fin["state"] == "failed" and "no Spark" in (json.dumps(fin.get("failure") or {}) + controller.log_text("hp-spark-engine", "stderr"))


def test_dependency_layer_on_top_of_host_python(controller, start_agent, tmp_path):
    import hashlib

    wh = tmp_path / "wheelhouse"
    wheel = build_wheel(wh, "olcf-layer-demo", "1.0")
    pk = [{"name": "olcf-layer-demo", "version": "1.0", "hashes": [hashlib.sha256(wheel.read_bytes()).hexdigest()]}]
    py_abi = f"cp{sys.version_info.major}{sys.version_info.minor}"
    lay = {"key": layer_mod.layer_key(pk, py_abi, []), "python": {"abi": py_abi}, "packages": pk, "requirements": layer_mod.requirements_text(pk),
           "pip": {"no_index": True}, "jars": {"resolved": [], "files": []}}
    start_agent(pip_find_links=[str(wh)], layer_offline=True)
    env = {"item_id": "itm_env", "name": "e", "version": 1, "lock_hash": "sha256:abc", "layer": lay}
    controller.ok(W, "run_job", host_job("hp-layer", "import olcf_layer_demo, duckdb\nprint('layer', olcf_layer_demo.__file__)\n",
                                         runtime={"id": HOST_RUNTIME_ID, "family": "host-python", "environment": env}))
    fin = wait_final(controller, "hp-layer", 180)
    assert fin["state"] == "succeeded", (fin, controller.log_text("hp-layer", "stderr"))
    dep = fin["result"]["dependencies"]
    assert dep["layer_key"] == lay["key"] and dep["runtime_id"] == HOST_RUNTIME_ID and dep["host_python"]["python"]


def test_interactive_kernel_on_host_python(controller, start_agent):
    start_agent()
    sp = job_spec("kern", "", kind="notebook", fmt="ipynb", runtime=HOST_RUNTIME_ID)["spec"]
    sp["runtime"] = {"id": HOST_RUNTIME_ID, "family": "host-python"}
    info = controller.ok(W, "start_kernel", {"session_id": "hs1", "spec": sp, "eager_spark": False}, timeout=120)
    assert info["pid"]
    controller.wait_for(lambda: any(e["content"].get("state") == "idle" for e in events(controller, "hs1", channel="status", msg_type="status")), 90, "idle")
    r = run(controller, "hs1", "import duckdb, olcf\nx = duckdb.sql('select 6*7').fetchone()[0]\nx", "m1")
    assert r["content"]["status"] == "ok"
    res = [e for e in events(controller, "hs1", msg_type="execute_result") if e["parent_msg_id"] == "m1"]
    assert res and res[0]["content"]["data"]["text/plain"] == "42"
    r = run(controller, "hs1", "spark.range(3).count()", "m2")
    assert r["content"]["status"] == "error" and "no Spark" in json.dumps(r["content"]) + "".join(e["content"].get("evalue", "") for e in events(controller, "hs1", msg_type="error"))


def test_host_python_can_be_switched_off_per_node(controller, start_agent):
    start_agent(host_python=False)
    _, hello = controller.hellos[-1]
    assert HOST_RUNTIME_ID not in {r["id"] for r in hello["runtimes"]}
    res = controller.request(W, "run_job", host_job("hp-off", "print('x')\n"))
    assert not res.get("ok") and res["error"]["code"] == "runtime_not_ready" and "host_python" in res["error"]["message"]


def test_host_runtime_cannot_be_installed_repaired_or_retired(tmp_path):
    mgr = RuntimeManager(ManagerConfig(state_dir=tmp_path / "s"))
    assert HOST_RUNTIME_ID in {r["id"] for r in mgr.list()}
    for call in (lambda: mgr.install(HOST_RUNTIME_ID), lambda: mgr.repair(HOST_RUNTIME_ID), lambda: mgr.retire(HOST_RUNTIME_ID), lambda: mgr.export_bundle(HOST_RUNTIME_ID, tmp_path / "x.tgz")):
        with pytest.raises(OlcfError) as ei:
            call()
        assert ei.value.remediation and "Spark runtime" in ei.value.remediation
    assert mgr.verify(HOST_RUNTIME_ID)["ok"] is True
    off = RuntimeManager(ManagerConfig(state_dir=tmp_path / "s2", host_python=False))
    assert HOST_RUNTIME_ID not in {r["id"] for r in off.list()}
