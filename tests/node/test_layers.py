# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S101, S603
"""Per-job dependency layers on the node: hash-keyed cache under the agent state dir, offline-capable builds from a wheelhouse, node index policy, pinned jars
(sha256-verified, Maven mirror or control plane), the layer on ``sys.path`` / ``PYTHONPATH`` for the driver and the kernel, jars in ``spark.jars`` and the
dependency fingerprint in ``result.json``.  Real ``pip``, real runner processes; wheels and the Maven repository are built in the test (no Internet)."""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
from pathlib import Path

import pytest

from olcf.notebooks import layer as layer_mod  # noqa: E402
from olcf.runtime_lib import bootstrap, executor  # noqa: E402
from olcf.runtime_lib.runner import dependency_fingerprint  # noqa: E402
from olcf.runtime_lib.spark import build_spark_conf  # noqa: E402
from olcf.worker.config import AgentConfig  # noqa: E402
from olcf.worker.layers import LayerManager, layer_of  # noqa: E402
from olcf.worker.protocol import CommandError  # noqa: E402
from tests.items.wheelbuilder import build_maven_repo, build_wheel  # noqa: E402
from tests.node.conftest import job_spec, wait_final  # noqa: E402

W = "wrk_1"


def make_layer(wheel: Path, name: str, version: str, *, jars: dict | None = None, pip: dict | None = None) -> dict:
    h = hashlib.sha256(wheel.read_bytes()).hexdigest()
    pk = [{"name": name, "version": version, "hashes": [h]}]
    return {"key": layer_mod.layer_key(pk, "cp313", [j["coordinate"] + "@" + j["sha256"] for j in (jars or {}).get("resolved", [])]), "python": {"abi": "cp313"}, "packages": pk,
            "requirements": layer_mod.requirements_text(pk), "pip": {"no_index": True, **(pip or {})}, "jars": jars or {"resolved": [], "files": []}}


def spec_with(layer: dict, **env) -> dict:
    return {"runtime": {"id": "rt", "fingerprint": "fp-1", "environment": {"item_id": "itm_env", "name": "e", "version": 3, "lock_hash": "sha256:abc", "layer": layer, **env}},
            "spark": {"master": "local[1]", "conf": {}}}


@pytest.fixture()
def wheelhouse(tmp_path):
    d = tmp_path / "wheelhouse"
    return d, build_wheel(d, "olcf-layer-demo", "1.0")


@pytest.fixture()
def cfg(tmp_path, wheelhouse):
    c = AgentConfig(state_dir=tmp_path / "state", pip_find_links=[str(wheelhouse[0])], layer_offline=True).resolve()
    c.ensure_dirs()
    return c


# --------------------------------------------------------------------------------------------- layer build / cache
def test_layer_is_built_once_per_hash_offline_and_reused(cfg, wheelhouse, monkeypatch):
    lm = LayerManager(cfg)
    lay = make_layer(wheelhouse[1], "olcf-layer-demo", "1.0")
    info = lm.prepare(spec_with(lay), sys.executable)
    assert info and info["key"] == lay["key"] and info["lock_hash"] == "sha256:abc"
    path = Path(info["path"])
    assert path.parent == cfg.path("layers") and (path / "olcf_layer_demo" / "__init__.py").is_file() and (path / layer_mod.MARKER).is_file()
    # second job with the same dependency hash: no pip at all (works with the index and the wheelhouse gone)
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: (_ for _ in ()).throw(AssertionError("pip was started for a cached layer")))
    assert lm.prepare(spec_with(lay), sys.executable)["path"] == info["path"]


def test_tampered_hash_is_refused_and_leaves_no_layer(cfg, wheelhouse):
    lay = make_layer(wheelhouse[1], "olcf-layer-demo", "1.0")
    lay["packages"][0]["hashes"] = ["0" * 64]
    lay["requirements"] = layer_mod.requirements_text(lay["packages"])
    with pytest.raises(CommandError) as ei:
        LayerManager(cfg).prepare(spec_with(lay), sys.executable)
    assert ei.value.code == "layer_failed" and "hash" in str(ei.value).lower()
    assert not (cfg.path("layers") / lay["key"].replace(":", "-") / layer_mod.MARKER).exists()


def test_node_index_policy(tmp_path, wheelhouse):
    lay = make_layer(wheelhouse[1], "olcf-layer-demo", "1.0", pip={"no_index": False, "index_url": "https://pypi.example.org/simple"})
    c = AgentConfig(state_dir=tmp_path / "s1", layer_allowed_indexes=["https://mirror.corp.example/"]).resolve()
    with pytest.raises(CommandError) as ei:
        LayerManager(c).prepare(spec_with(lay), sys.executable)
    assert ei.value.code == "index_not_allowed" and "mirror.corp.example" in str(ei.value)
    # an offline node ignores the index entirely (cache + wheelhouse only) and therefore never violates the policy
    c2 = AgentConfig(state_dir=tmp_path / "s2", layer_allowed_indexes=["https://mirror.corp.example/"], layer_offline=True, pip_find_links=[str(wheelhouse[0])]).resolve()
    assert LayerManager(c2).prepare(spec_with(lay), sys.executable)["path"]


def test_bare_dependency_lock_is_turned_into_a_layer():
    assert layer_of({"runtime": {}}) is None
    lay = {"key": "sha256:x", "packages": []}
    assert layer_of({"runtime": {"environment": {"layer": lay}}}) is lay


# --------------------------------------------------------------------------------------------- jars
def test_jars_are_fetched_from_the_maven_mirror_verified_and_merged_into_spark_jars(cfg, tmp_path):
    repo = tmp_path / "m2"
    sha = build_maven_repo(repo, {"org.demo:thing:1.0": []})["org.demo:thing:1.0"]
    cfg.maven_repositories = [repo.as_uri()]
    lay = {"key": "sha256:jarsonly", "packages": [], "jars": {"resolved": [{"coordinate": "org.demo:thing:1.0", "sha256": sha}], "files": []}, "pip": {}}
    spec = spec_with(lay)
    info = LayerManager(cfg).apply(spec, sys.executable)
    assert info and info["path"] is None and len(info["jars"]) == 1 and info["jars"][0].endswith(f"{sha}.jar")
    assert spec["runtime"]["layer"]["jars"] == spec["spark"]["jars"] == info["jars"]
    assert hashlib.sha256(Path(cfg.path("layers", "_jars", f"{sha}.jar")).read_bytes()).hexdigest() == sha
    conf = build_spark_conf(spec, lock={"jars": []}, manifest={}, runtime_root=tmp_path)
    assert info["jars"][0] in conf["spark.jars"].split(",")
    # a jar that differs from the pin is refused (and not cached)
    bad = {"key": "sha256:bad", "packages": [], "jars": {"resolved": [{"coordinate": "org.demo:thing:1.0", "sha256": "1" * 64}], "files": []}, "pip": {}}
    with pytest.raises(CommandError) as ei:
        LayerManager(cfg).prepare(spec_with(bad), sys.executable)
    assert ei.value.code == "jar_corrupt" and not cfg.path("layers", "_jars", f"{'1' * 64}.jar").exists()


def test_offline_node_without_the_jar_fails_with_remediation(cfg):
    cfg.maven_repositories = []
    lay = {"key": "sha256:j2", "packages": [], "jars": {"resolved": [{"coordinate": "org.demo:thing:1.0", "sha256": "2" * 64}], "files": []}, "pip": {}}
    with pytest.raises(CommandError) as ei:
        LayerManager(cfg).prepare(spec_with(lay), sys.executable)
    assert ei.value.code == "jar_unavailable" and "offline" in str(ei.value)


def test_uploaded_jar_files_come_from_the_control_plane(cfg):
    import io
    import zipfile

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("a.txt", "x")
    data = buf.getvalue()
    sha = hashlib.sha256(data).hexdigest()
    calls: list[str] = []

    def fetch(s: str, dest: Path) -> None:
        calls.append(s)
        dest.write_bytes(data)

    lay = {"key": "sha256:j3", "packages": [], "jars": {"resolved": [], "files": [{"uri": "Files/x.jar", "sha256": sha}]}, "pip": {}}
    lm = LayerManager(cfg, fetch_file=fetch)
    assert lm.prepare(spec_with(lay), sys.executable)["jars"][0].endswith(f"{sha}.jar")
    lm.prepare(spec_with(lay), sys.executable)
    assert calls == [sha]                                                                  # cached after the first download
    lay2 = {**lay, "key": "sha256:j4", "jars": {"resolved": [], "files": [{"uri": "Files/y.jar", "sha256": "3" * 64}]}}
    with pytest.raises(CommandError) as ei:
        LayerManager(cfg, fetch_file=lambda s, d: d.write_bytes(data)).prepare(spec_with(lay2), sys.executable)
    assert ei.value.code == "jar_corrupt"


# --------------------------------------------------------------------------------------------- in-runtime pieces
def test_apply_layer_puts_the_directory_in_front_on_sys_path_and_pythonpath(tmp_path, monkeypatch):
    d = tmp_path / "layer"
    d.mkdir()
    monkeypatch.setattr(sys, "path", list(sys.path))
    monkeypatch.setenv("PYTHONPATH", "/opt/x")
    assert bootstrap.apply_layer({"runtime": {"layer": {"path": str(d)}}}) == str(d)
    assert sys.path[0] == str(d) and os.environ["PYTHONPATH"].split(os.pathsep) == [str(d), "/opt/x"]
    bootstrap.apply_layer({"runtime": {"layer": {"path": str(d)}}})                      # idempotent
    assert sys.path.count(str(d)) == 1 and os.environ["PYTHONPATH"].split(os.pathsep).count(str(d)) == 1
    assert bootstrap.apply_layer({"runtime": {}}) is None and bootstrap.apply_layer({"runtime": {"layer": {"path": str(tmp_path / "nope")}}}) is None


def test_dependency_fingerprint():
    spec = spec_with({"key": "sha256:k"})
    spec["runtime"]["layer"] = {"key": "sha256:k", "lock_hash": "sha256:abc", "environment": {"item_id": "itm_env", "version": 3}, "jars": ["file:///c/aa.jar"]}
    fp = dependency_fingerprint(spec)
    assert fp == {"runtime_id": "rt", "runtime_fingerprint": "fp-1", "layer_key": "sha256:k", "environment_lock_hash": "sha256:abc",
                  "environment": {"item_id": "itm_env", "version": 3}, "jars": ["aa.jar"]}
    assert dependency_fingerprint({"runtime": {"id": "r", "fingerprint": "f"}}) == {"runtime_id": "r", "runtime_fingerprint": "f"}
    assert dependency_fingerprint({}) is None


def test_non_finite_parameters_are_injected_as_valid_python():
    src = executor.parameter_source({"a": float("nan"), "b": float("inf"), "c": float("-inf"), "d": [1.5, float("nan")], "e": {"k": None, "t": True}, "s": "x'\ny"})
    ns: dict = {}
    exec(src, ns)  # noqa: S102 - the literal under test, built from fixed values
    import math

    assert math.isnan(ns["a"]) and ns["b"] == math.inf and ns["c"] == -math.inf and math.isnan(ns["d"][1]) and ns["e"] == {"k": None, "t": True} and ns["s"] == "x'\ny"
    for bad in ({"x": object()}, {"x": {1: 2}}, {"x-y": 1}):
        with pytest.raises(Exception):  # noqa: B017, PT011 - Unsupported (olcf error)
            executor.parameter_source(bad)


# --------------------------------------------------------------------------------------------- end to end through the agent
def test_job_imports_from_its_layer_and_the_result_carries_the_fingerprint(controller, start_agent, wheelhouse):
    lay = make_layer(wheelhouse[1], "olcf-layer-demo", "1.0")
    h = start_agent(pip_find_links=[str(wheelhouse[0])], layer_offline=True)
    src = "import olcf_layer_demo, sys\nEXIT_VALUE = {'v': olcf_layer_demo.VERSION, 'path': olcf_layer_demo.__file__}\n"
    p = job_spec("lay1", src, runtime="olcf-rt-fake-1")
    p["spec"]["runtime"]["environment"] = {"item_id": "itm_env", "name": "e", "version": 1, "lock_hash": "sha256:abc", "layer": lay}
    controller.ok(W, "run_job", p)
    fin = wait_final(controller, "lay1")
    assert fin["state"] == "succeeded", fin
    assert fin["result"]["exit_value"]["v"] == "1.0" and str(h.cfg.path("layers")) in fin["result"]["exit_value"]["path"]
    dep = fin["result"]["dependencies"]
    assert dep["layer_key"] == lay["key"] and dep["environment_lock_hash"] == "sha256:abc" and dep["runtime_id"] == "olcf-rt-fake-1"
    # without the environment the package is simply not importable
    controller.ok(W, "run_job", job_spec("lay0", src, runtime="olcf-rt-fake-1"))
    fin0 = wait_final(controller, "lay0")
    assert fin0["state"] == "failed" and fin0["result"]["dependencies"] == {"runtime_id": "olcf-rt-fake-1", "runtime_fingerprint": None}


def test_a_broken_layer_fails_the_run_command_with_a_classified_error(controller, start_agent, wheelhouse):
    lay = make_layer(wheelhouse[1], "olcf-layer-demo", "1.0")
    lay["packages"][0]["hashes"] = ["0" * 64]
    lay["requirements"] = layer_mod.requirements_text(lay["packages"])
    h = start_agent(pip_find_links=[str(wheelhouse[0])], layer_offline=True)
    p = job_spec("lay2", "print(1)", runtime="olcf-rt-fake-1")
    p["spec"]["runtime"]["environment"] = {"layer": lay}
    res = controller.request(W, "run_job", p)
    assert res["ok"] is False and res["error"]["code"] == "layer_failed" and "hash" in res["error"]["message"].lower() and res["error"].get("remediation")
    # nothing is left behind: no job record, no work dir
    assert not (h.cfg.path("work") / "lay2").exists()


def test_interactive_kernel_sees_the_environment_layer(controller, start_agent, wheelhouse):
    from tests.node.test_kernels import kspec, run, start, stream_text

    lay = make_layer(wheelhouse[1], "olcf-layer-demo", "1.0")
    start_agent(pip_find_links=[str(wheelhouse[0])], layer_offline=True)
    spec = kspec()
    spec["runtime"]["environment"] = {"item_id": "itm_env", "lock_hash": "sha256:abc", "layer": lay}
    start(controller, "sl", spec=spec)
    controller.wait_for(lambda: any(e["session_id"] == "sl" and e["content"].get("state") == "idle" for e in list(controller.kernel_events) if e.get("msg_type") == "status"), 60, "idle")
    r = run(controller, "sl", "import olcf_layer_demo, os\nprint(olcf_layer_demo.VERSION, 'layers' in olcf_layer_demo.__file__)", "k1")
    assert r["content"]["status"] == "ok" and stream_text(controller, "sl", "k1").split() == ["1.0", "True"]


def test_interactive_kernel_uses_the_inline_matplotlib_backend_but_batch_jobs_keep_agg(controller, start_agent):
    from tests.node.test_kernels import kspec, run, start, stream_text

    start_agent()
    start(controller, "mpl", spec=kspec())
    controller.wait_for(lambda: any(e["session_id"] == "mpl" and e["content"].get("state") == "idle" for e in list(controller.kernel_events) if e.get("msg_type") == "status"), 60, "idle")
    run(controller, "mpl", "import os\nprint(os.environ['MPLBACKEND'])", "b1")
    assert stream_text(controller, "mpl", "b1").strip() == "module://matplotlib_inline.backend_inline"
    controller.ok(W, "run_job", job_spec("mplb", "import os\nEXIT_VALUE = os.environ['MPLBACKEND']\n"))
    assert wait_final(controller, "mplb")["result"]["exit_value"] == "Agg"
