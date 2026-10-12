# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Real runtime installs with the production RuntimeManager - no Spark needed.

The manifests pin hand-built pure-python wheels (real sha256 hashes) that are served from a local wheelhouse with
``--no-index --find-links``; the venv, ``pip install --require-hashes``, the olcf package install (``--no-deps``), the smoke test, the
``lock.json``/``VERIFIED`` markers, stage-dir + atomic rename, side-by-side runtimes, repair, retire, rollback, verify and offline bundle
export/import are all the production code paths. (The Spark-specific parts are covered by the ``spark`` integration tests.)
"""

from __future__ import annotations

import hashlib
import json
import shutil
import sys
import tarfile
from pathlib import Path

import pytest

from olcf.runtime import hostenv
from olcf.runtime.errors import (
    BundleInvalid,
    InstallFailed,
    PlatformUnsupported,
    RuntimeNotFound,
    SignatureInvalid,
    VerifyFailed,
)
from olcf.runtime.manager import ManagerConfig, RuntimeManager, fingerprint_of
from olcf.runtime.signing import (
    TrustedKey,
    TrustStore,
    generate_keypair,
    key_id_for,
    load_private_key,
    load_public_key,
    sign_manifest_dict,
)
from tests.node.fakes import build_wheel

PY = f"{sys.version_info.major}.{sys.version_info.minor}"


def tiny_manifest(rid: str, version: str, wheels: list[Path], *, family: str = "olcf-native", extra: dict | None = None) -> dict:
    pins = []
    for w in wheels:
        name, ver = w.name.split("-")[:2]
        pins.append({"name": name.replace("_", "-"), "version": ver, "hashes": [hashlib.sha256(w.read_bytes()).hexdigest()]})
    doc = {"schema": "olcf.runtime.manifest/1", "id": rid, "version": version, "family": family, "title": f"tiny runtime {rid}", "spark": {"version": "4.1.1"}, "scala": "2.13",
           "java": {"major": 21, "supported": [17, 21]}, "python": {"version": PY}, "os": ["linux", "macos", "windows"], "arch": ["x86_64", "aarch64"],
           "packages": pins, "provided_jars": [], "spark_conf_defaults": {}, "approved_spark_conf": ["spark.*"], "verify": {"smoke": "import", "imports": [pins[0]["name"].replace("-", "_")]}}
    doc.update(extra or {})
    return doc


@pytest.fixture(scope="module")
def keys(tmp_path_factory: pytest.TempPathFactory):
    priv, pub = generate_keypair()
    pk = load_public_key(pub)
    return load_private_key(priv), TrustStore([TrustedKey(key_id_for(pk), pk, "tests")])


@pytest.fixture(scope="module")
def wheelhouse(tmp_path_factory: pytest.TempPathFactory) -> Path:
    d = tmp_path_factory.mktemp("wheelhouse")
    build_wheel(d, "olcfdummy", "1.0.0")
    build_wheel(d, "olcfdummy", "2.0.0")
    return d


def signed(doc: dict, keys) -> dict:
    return sign_manifest_dict(doc, keys[0])


def mgr_for(state: Path, wheelhouse: Path, keys, **over) -> RuntimeManager:
    cfg = ManagerConfig(state_dir=state, trust=keys[1], pip_find_links=[str(wheelhouse)], pip_no_index=True, smoke_timeout=300, **over)
    return RuntimeManager(cfg)


@pytest.fixture(scope="module")
def installed(tmp_path_factory: pytest.TempPathFactory, wheelhouse: Path, keys):
    """A node with v1 and v2 of one family installed side by side (v2 active). Module scoped: installs are slow (venv + pip)."""
    state = tmp_path_factory.mktemp("node")
    mgr = mgr_for(state, wheelhouse, keys)
    steps: list[str] = []
    m1 = signed(tiny_manifest("olcf-rt-tiny-1", "1.0.0", [wheelhouse / "olcfdummy-1.0.0-py3-none-any.whl"]), keys)
    m2 = signed(tiny_manifest("olcf-rt-tiny-2", "2.0.0", [wheelhouse / "olcfdummy-2.0.0-py3-none-any.whl"]), keys)
    r1 = mgr.install(m1, progress=lambda s, d: steps.append(s))
    r2 = mgr.install(m2)
    return {"state": state, "mgr": mgr, "m1": m1, "m2": m2, "r1": r1, "r2": r2, "steps": steps}


def test_install_layout_markers_and_lock(installed) -> None:
    root = installed["state"] / "runtimes" / "olcf-rt-tiny-1"
    names = {p.name for p in root.iterdir()}
    assert {"VERIFIED", "jars", "lock.json", "manifest.json", "venv"} <= names  # the contract layout (+ the audit copy of the pinned requirements)
    assert names - {"VERIFIED", "jars", "lock.json", "manifest.json", "venv", "requirements.lock.txt", "build.requirements.txt"} == set()
    assert "--hash=sha256:" in (root / "requirements.lock.txt").read_text()
    lock = json.loads((root / "lock.json").read_text())
    assert lock["schema"] == "olcf.runtime.lock/1" and lock["runtime_id"] == "olcf-rt-tiny-1" and lock["python"]["series"] == PY
    assert lock["java"]["major"] in (17, 21) and any(p["name"].lower() == "olcfdummy" and p["version"] == "1.0.0" for p in lock["packages"])
    assert lock["olcf"]["wheel_sha256"] and lock["meta"]["source"] == "online"
    verified = json.loads((root / "VERIFIED").read_text())
    assert verified["fingerprint"] == installed["r1"]["fingerprint"] == fingerprint_of(json.loads((root / "manifest.json").read_text()), lock)
    assert any(s["name"].startswith("import:olcfdummy") and s["ok"] for s in verified["smoke"]["steps"])
    assert not (installed["state"] / "runtimes" / ".stage").exists() or not any((installed["state"] / "runtimes" / ".stage").iterdir()), "stage dir cleaned up"
    assert not (root / "_wheel").exists()
    assert installed["steps"][:2] == ["prerequisites", "venv"] and "pip" in installed["steps"] and installed["steps"][-1] == "done"


def test_runtime_venv_runs_and_has_the_pinned_package_and_olcf(installed) -> None:
    import subprocess

    h = installed["mgr"].handle("olcf-rt-tiny-2")
    out = subprocess.run([str(h.python), "-I", "-c", "import olcfdummy, olcf.runtime_lib.runner as r; print(olcfdummy.__version__)"], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == "2.0.0"
    env = h.env()
    assert env["PYSPARK_PYTHON"] == str(h.python) and env["JAVA_HOME"] and env["PYTHONNOUSERSITE"] == "1" and env["OLCF_RUNTIME_ID"] == "olcf-rt-tiny-2"


def test_side_by_side_and_active_runtime_per_family(installed) -> None:
    mgr: RuntimeManager = installed["mgr"]
    assert any(r["id"] == "olcf-host-python" and r["state"] == "ready" for r in mgr.list())  # the built-in pseudo-runtime is always listed
    ids = {r["id"]: r for r in mgr.list() if r["id"] != "olcf-host-python"}
    assert set(ids) == {"olcf-rt-tiny-1", "olcf-rt-tiny-2"} and all(r["state"] == "ready" and r["verified"] for r in ids.values())
    assert mgr.active_for_family("olcf-native") == "olcf-rt-tiny-2"
    assert ids["olcf-rt-tiny-2"]["active"] and not ids["olcf-rt-tiny-1"]["active"]
    assert ids["olcf-rt-tiny-1"]["fingerprint"] != ids["olcf-rt-tiny-2"]["fingerprint"]


def test_second_install_of_same_runtime_is_a_noop(installed) -> None:
    res = installed["mgr"].install(installed["m1"], activate=False)
    assert res["changed"] is False and res["state"] == "ready"
    assert installed["mgr"].active_for_family("olcf-native") == "olcf-rt-tiny-2"


def test_verify_light_and_deep(installed) -> None:
    res = installed["mgr"].verify("olcf-rt-tiny-1")
    assert res["ok"] and {s["name"] for s in res["steps"]} >= {"markers", "manifest-lock"} or res["ok"]
    deep = installed["mgr"].verify("olcf-rt-tiny-1", deep=True)
    assert any(s["name"] == "smoke" and s["ok"] for s in deep["steps"])


def test_rollback_and_retire(installed) -> None:
    mgr: RuntimeManager = installed["mgr"]
    res = mgr.rollback("olcf-native")
    assert res["previous"] == "olcf-rt-tiny-2" and res["active"] == "olcf-rt-tiny-1"
    assert mgr.active_for_family("olcf-native") == "olcf-rt-tiny-1"
    out = mgr.retire("olcf-rt-tiny-2")
    assert out["state"] == "retired" and any(r["id"] == "olcf-rt-tiny-2" and r["state"] == "retired" for r in mgr.list())
    with pytest.raises(VerifyFailed, match="retired"):
        mgr.light_check("olcf-rt-tiny-2")  # the agent refuses to start anything on a retired runtime
    with pytest.raises(RuntimeNotFound):
        mgr.rollback("olcf-rt-unknown")
    # re-installing a retired runtime brings it back (not purged, files still verified)
    mgr.install(installed["m2"], force=True)
    assert any(r["id"] == "olcf-rt-tiny-2" and r["state"] == "ready" for r in mgr.list())


def test_light_check_detects_jar_tampering_and_removes_verified(tmp_path: Path, wheelhouse: Path, keys) -> None:
    """Jar hashes recorded in lock.json are re-verified at every start (light_check)."""
    from tests.node.fakes import make_fake_runtime

    root = make_fake_runtime(tmp_path, "olcf-rt-fake-1")
    jar = root / "jars" / "dep-1.0.jar"
    jar.write_bytes(b"original jar")
    lock = json.loads((root / "lock.json").read_text())
    lock["jars"] = [{"coordinate": "g:dep:1.0", "file": "dep-1.0.jar", "sha256": hashlib.sha256(b"original jar").hexdigest()}]
    (root / "lock.json").write_text(json.dumps(lock))
    mgr = RuntimeManager(ManagerConfig(state_dir=tmp_path, trust=keys[1]))
    assert mgr.light_check("olcf-rt-fake-1").id == "olcf-rt-fake-1"
    jar.write_bytes(b"swapped by an attacker")
    with pytest.raises(VerifyFailed) as ei:
        mgr.light_check("olcf-rt-fake-1")
    assert "dep-1.0.jar" in ei.value.message and "repair" in ei.value.remediation
    assert not (root / "VERIFIED").exists(), "a runtime that fails verification must be un-verified (the scheduler stops placing jobs on it)"
    with pytest.raises(VerifyFailed, match="not verified"):
        mgr.light_check("olcf-rt-fake-1")


def test_unsigned_and_untrusted_manifests_are_refused_before_anything_is_installed(tmp_path: Path, wheelhouse: Path, keys) -> None:
    mgr = mgr_for(tmp_path, wheelhouse, keys)
    doc = tiny_manifest("olcf-rt-tiny-x", "1.0.0", [wheelhouse / "olcfdummy-1.0.0-py3-none-any.whl"])
    with pytest.raises(SignatureInvalid):
        mgr.install(doc)
    other = sign_manifest_dict(doc, load_private_key(generate_keypair()[0]))
    with pytest.raises(SignatureInvalid, match="untrusted key"):
        mgr.install(other)
    assert not (tmp_path / "runtimes" / "olcf-rt-tiny-x").exists()


def test_platform_gate(tmp_path: Path, wheelhouse: Path, keys) -> None:
    doc = tiny_manifest("olcf-rt-tiny-p", "1.0.0", [wheelhouse / "olcfdummy-1.0.0-py3-none-any.whl"], extra={"os": ["windows"], "arch": ["x86_64"]})
    import platform

    if platform.system() != "Windows":
        with pytest.raises(PlatformUnsupported):
            mgr_for(tmp_path, wheelhouse, keys).install(signed(doc, keys))


def test_hash_mismatch_aborts_atomically_and_leaves_existing_install_untouched(installed, wheelhouse: Path, keys, tmp_path: Path) -> None:
    """The manifest pins hashes: a wheelhouse serving other bytes under the same name must fail, with nothing left behind."""
    evil = tmp_path / "evil-wheelhouse"
    build_wheel(evil, "olcfdummy", "1.0.0", {"olcfdummy/__init__.py": "__version__ = '1.0.0'\nEVIL = True\n"})
    mgr = RuntimeManager(ManagerConfig(state_dir=installed["state"], trust=keys[1], pip_find_links=[str(evil)], pip_no_index=True))
    before = (installed["state"] / "runtimes" / "olcf-rt-tiny-1" / "VERIFIED").read_text()
    with pytest.raises(InstallFailed) as ei:
        mgr.install(installed["m1"], force=True)
    assert "do NOT override" in (ei.value.remediation or "") or "hash" in ei.value.message.lower()
    stage = installed["state"] / "runtimes" / ".stage"
    assert not stage.exists() or not any(stage.iterdir()), "failed install must leave no stage directory"
    assert (installed["state"] / "runtimes" / "olcf-rt-tiny-1" / "VERIFIED").read_text() == before
    assert installed["mgr"].light_check("olcf-rt-tiny-1")  # still usable


def test_repair_reinstalls_a_broken_runtime_transactionally(tmp_path: Path, wheelhouse: Path, keys) -> None:
    mgr = mgr_for(tmp_path, wheelhouse, keys)
    m = signed(tiny_manifest("olcf-rt-tiny-r", "1.0.0", [wheelhouse / "olcfdummy-1.0.0-py3-none-any.whl"]), keys)
    mgr.install(m)
    assert mgr.repair("olcf-rt-tiny-r")["detail"] == "already healthy"
    # damage: delete the installed package inside the venv, then verify must fail and repair must fix it
    h = mgr.handle("olcf-rt-tiny-r")
    site = next((h.root / "venv").rglob("olcfdummy"))
    shutil.rmtree(site)
    with pytest.raises(VerifyFailed):
        mgr.verify("olcf-rt-tiny-r", deep=True)
    assert not (h.root / "VERIFIED").exists()
    res = mgr.repair("olcf-rt-tiny-r")
    assert res.get("repaired") and res["state"] == "ready"
    assert mgr.verify("olcf-rt-tiny-r", deep=True)["ok"]


def test_offline_bundle_export_and_import_uses_no_network(installed, wheelhouse: Path, keys, tmp_path: Path) -> None:
    mgr: RuntimeManager = installed["mgr"]
    bundle = tmp_path / "bundle.tar.gz"
    info = mgr.export_bundle("olcf-rt-tiny-1", bundle)
    assert info["files"] >= 5 and Path(info["path"]).is_file()
    with tarfile.open(bundle) as tf:
        names = tf.getnames()
    assert {"bundle.json", "manifest.json", "lock.json", "signature.json"} <= set(names)
    assert any(n.startswith("wheels/olcfdummy-1.0.0") for n in names) and any(n.startswith("olcf/olcf-") for n in names)
    # a fresh node with NO wheelhouse, NO index (pip_no_index + nothing to find): only the bundle can satisfy the install
    node2 = tmp_path / "node2"
    mgr2 = RuntimeManager(ManagerConfig(state_dir=node2, trust=keys[1], pip_no_index=True, deny_public_install=True))
    res = mgr2.import_bundle(bundle)
    assert res["state"] == "ready" and res["source"] == "bundle"
    lock = json.loads((node2 / "runtimes" / "olcf-rt-tiny-1" / "lock.json").read_text())
    assert lock["meta"]["source"] == "bundle"
    assert mgr2.verify("olcf-rt-tiny-1", deep=True)["ok"]
    # identical content -> identical fingerprint on both nodes (reproducible)
    assert mgr2.handle("olcf-rt-tiny-1").fingerprint == mgr.handle("olcf-rt-tiny-1").fingerprint


def test_bundle_tampering_and_traversal_are_rejected(installed, keys, tmp_path: Path) -> None:
    mgr: RuntimeManager = installed["mgr"]
    bundle = tmp_path / "b.tar.gz"
    mgr.export_bundle("olcf-rt-tiny-1", bundle)
    # 1. flip a byte of a wheel inside the bundle
    tampered = tmp_path / "tampered.tar.gz"
    work = tmp_path / "x"
    work.mkdir()
    with tarfile.open(bundle) as tf:
        tf.extractall(work, filter="data")
    wheel = next((work / "wheels").glob("*.whl"))
    wheel.write_bytes(wheel.read_bytes() + b"\0")
    with tarfile.open(tampered, "w:gz") as tf:
        for p in sorted(work.rglob("*")):
            tf.add(p, arcname=p.relative_to(work).as_posix(), recursive=False)
    with pytest.raises(BundleInvalid, match="does not match bundle.json"):
        RuntimeManager(ManagerConfig(state_dir=tmp_path / "n", trust=keys[1])).import_bundle(tampered)
    # 2. path traversal member
    evil = tmp_path / "evil.tar.gz"
    with tarfile.open(evil, "w:gz") as tf:
        data = b"x"
        ti = tarfile.TarInfo("../../escape.txt")
        ti.size = 1
        import io

        tf.addfile(ti, io.BytesIO(data))
    with pytest.raises(BundleInvalid, match="unsafe entry"):
        RuntimeManager(ManagerConfig(state_dir=tmp_path / "n2", trust=keys[1])).import_bundle(evil)
    assert not (tmp_path / "escape.txt").exists()
    # 3. a bundle whose manifest is signed by a key the node does not trust
    with pytest.raises(SignatureInvalid):
        RuntimeManager(ManagerConfig(state_dir=tmp_path / "n3", trust=TrustStore([]))).import_bundle(bundle)
    # 4. not a bundle at all
    junk = tmp_path / "junk.tar.gz"
    junk.write_bytes(b"not a tarball")
    with pytest.raises(BundleInvalid):
        RuntimeManager(ManagerConfig(state_dir=tmp_path / "n4", trust=keys[1])).import_bundle(junk)


def test_deny_public_install_requires_a_mirror(tmp_path: Path, keys, wheelhouse: Path) -> None:
    from olcf.runtime.errors import PrerequisiteMissing

    mgr = RuntimeManager(ManagerConfig(state_dir=tmp_path, trust=keys[1], deny_public_install=True))
    m = signed(tiny_manifest("olcf-rt-tiny-d", "1.0.0", [wheelhouse / "olcfdummy-1.0.0-py3-none-any.whl"]), keys)
    with pytest.raises(PrerequisiteMissing, match="denied by policy"):
        mgr.install(m)


def test_missing_python_series_is_an_actionable_prerequisite_error(tmp_path: Path, keys, wheelhouse: Path) -> None:
    from olcf.runtime.errors import PrerequisiteMissing

    doc = tiny_manifest("olcf-rt-tiny-py", "1.0.0", [wheelhouse / "olcfdummy-1.0.0-py3-none-any.whl"], extra={"python": {"version": "3.99"}})
    with pytest.raises(PrerequisiteMissing) as ei:
        mgr_for(tmp_path, wheelhouse, keys).install(signed(doc, keys))
    assert "3.99" in ei.value.message and ei.value.remediation
    assert not (tmp_path / "runtimes" / "olcf-rt-tiny-py").exists() and hostenv.discover_pythons("3.99") == []
