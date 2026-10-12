# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Runtime catalog: resolution rules, the Spark-conf allow-list, the ``/v1/runtimes`` API, install/verify/retire operations and offline bundles."""

from __future__ import annotations

import copy
import hashlib
import io
import json
from pathlib import Path
from typing import Any

import pytest

from olcf.compute.runtimes import DELTA_TABLE, OLCF_OWNED_CONF, conf_allowed, filter_spark_conf
from olcf.core.errors import BadRequest, PolicyDenied
from tests.compute.fake_agent import wait_until

MANIFESTS = Path(__file__).resolve().parents[2] / "src" / "olcf" / "runtime" / "manifests"
APPROVED = ["spark.sql.*", "spark.executor.memory", "spark.default.parallelism"]


# ------------------------------------------------------------------------------------------------ pure functions
def test_conf_allow_list_accepts_normalises_and_reports() -> None:
    ok, dropped = filter_spark_conf({"spark.sql.shuffle.partitions": 8, "spark.sql.adaptive.enabled": True, "spark.executor.memory": "2g", "spark.some.other": "x", "not.spark": "y"}, APPROVED)
    assert ok == {"spark.sql.shuffle.partitions": "8", "spark.sql.adaptive.enabled": "true", "spark.executor.memory": "2g"}
    assert {d["key"] for d in dropped} == {"spark.some.other", "not.spark"}
    assert any("not a spark" in d["reason"] for d in dropped) and any("approved" in d["reason"] for d in dropped)


@pytest.mark.parametrize("key", ["spark.master", "spark.driver.host", "spark.ui.proxyBase", "spark.authenticate.secret", "spark.kubernetes.namespace", "spark.jars.packages",
                                 "spark.driver.extraJavaOptions", "spark.executor.extraClassPath", "spark.network.crypto.enabled"])
def test_platform_owned_keys_are_refused_even_if_approved(key: str) -> None:
    with pytest.raises(PolicyDenied, match="managed by the platform"):
        filter_spark_conf({key: "x"}, ["spark.*"])  # a wildcard approval can never open an owned key


def test_nested_conf_values_are_rejected() -> None:
    with pytest.raises(BadRequest, match="scalar"):
        filter_spark_conf({"spark.sql.x": {"a": 1}}, APPROVED)


def test_conf_allowed_is_case_sensitive_glob() -> None:
    assert conf_allowed("spark.sql.shuffle.partitions", APPROVED) and not conf_allowed("SPARK.sql.x", APPROVED) and not conf_allowed("spark.sqlx", APPROVED)
    assert "spark.master" in OLCF_OWNED_CONF


def test_delta_matrix_knowledge_is_consistent() -> None:
    for major, row in DELTA_TABLE.items():
        assert row["min_reader"] >= 1 and row["min_writer"] >= row["min_reader"] and "deletionVectors" in row["features"], major


# ------------------------------------------------------------------------------------------------ resolution through the API
def resolve(st: Any, **runtime: Any) -> Any:
    return st.http.post("/v1/runtimes/resolve", headers=st.admin, json={"runtime": runtime})


def test_builtin_runtimes_are_listed_with_signed_manifests(stack: Any) -> None:
    items = {r["id"]: r for r in stack.http.get("/v1/runtimes", headers=stack.admin).json()["items"]}
    assert {"olcf-rt-1.3.0", "olcf-rt-2.0.0", "olcf-rt-native-1.0.0"} <= set(items)
    assert items["olcf-rt-2.0.0"]["family"] == "fabric-2.0-compatible" and items["olcf-rt-2.0.0"]["state"] == "available"
    full = stack.http.get("/v1/runtimes/olcf-rt-2.0.0", headers=stack.admin).json()
    assert full["id"] == "olcf-rt-2.0.0" and full.get("manifest") and "nodes" in full and full["signed"] is True
    assert "signature" not in full["manifest"] and full["approved_spark_conf"]
    assert stack.http.get("/v1/runtimes/nope", headers=stack.admin).status_code == 404


def test_resolution_rules(stack: Any) -> None:
    d = resolve(stack).json()
    assert d["runtime_id"] == "olcf-rt-2.0.0" and d["family"] == "fabric-2.0-compatible"  # platform default family
    assert resolve(stack, family="fabric-1.3-compatible").json()["runtime_id"] == "olcf-rt-1.3.0"
    assert resolve(stack, family="olcf-native").json()["runtime_id"] == "olcf-rt-native-1.0.0"
    assert resolve(stack, runtime_id="olcf-rt-1.3.0").json()["spark_version"] == "3.5.5"
    assert resolve(stack, spark=">=4.1,<4.2").json()["runtime_id"] in ("olcf-rt-2.0.0", "olcf-rt-native-1.0.0")
    assert resolve(stack, spark="3.5").json()["runtime_id"] == "olcf-rt-1.3.0"  # '3.5' means 3.5.*
    r = resolve(stack, spark="9.9")
    assert r.status_code in (400, 422, 501) and "no registered runtime" in r.text and "available" in r.text
    r = resolve(stack, runtime_id="olcf-rt-1.3.0", spark=">=4.0")
    assert r.status_code in (400, 422, 501) and "does not satisfy" in r.text
    assert resolve(stack, runtime_id="ghost").status_code in (400, 422, 501)
    assert resolve(stack, spark="not-a-spec").status_code == 400
    assert resolve(stack, family="no-such-family").status_code in (400, 422, 501)


def test_resolution_conf_policy(stack: Any) -> None:
    d = resolve(stack, spark_conf={"spark.sql.shuffle.partitions": "7", "spark.totally.unknown": "1"}).json()
    assert d["spark_conf"].get("spark.sql.shuffle.partitions") == "7" or any(x["key"] == "spark.sql.shuffle.partitions" for x in d["rejected_conf"])
    assert any(x["key"] == "spark.totally.unknown" for x in d["rejected_conf"]) and d["notes"]
    assert resolve(stack, spark_conf={"spark.master": "local"}).status_code == 403  # platform-owned
    assert resolve(stack, spark_conf={"spark.ui.reverseProxyUrl": "http://evil"}).status_code == 403


def test_matrix_and_rbac(stack: Any) -> None:
    m = stack.http.get("/v1/runtimes/matrix", headers=stack.admin).json()
    fam = {f["family"]: f for f in m["families"]}
    assert fam["fabric-2.0-compatible"]["fabric_interop"] is True and fam["olcf-native"]["fabric_interop"] is False
    assert fam["fabric-2.0-compatible"]["delta_protocol"]["min_reader"] >= 1
    assert any(f["feature"] == "deletionVectors" and f["affects_readers"] for f in m["delta_features"])
    viewer = stack.user("Viewer")  # organisation-wide read access
    assert stack.http.get("/v1/runtimes", headers=viewer).status_code == 200
    assert stack.http.post("/v1/runtimes/olcf-rt-2.0.0/install", headers=viewer, json={}).status_code == 403
    assert stack.http.get("/v1/runtimes/bundles", headers=viewer).status_code == 403
    assert stack.http.post("/v1/runtimes", headers=viewer, json={"manifest": {"id": "x", "family": "y"}}).status_code == 403


def test_registering_runtimes_needs_a_valid_signature(stack: Any) -> None:
    base = json.loads((MANIFESTS / "olcf-rt-2.0.0.json").read_text())
    unsigned = {k: v for k, v in base.items() if k != "signature"} | {"id": "olcf-rt-custom-1"}
    r = stack.http.post("/v1/runtimes", headers=stack.admin, json={"manifest": unsigned})
    assert r.status_code == 403 and "signed" in r.text
    tampered = copy.deepcopy(base)
    tampered["id"], tampered["version"] = "olcf-rt-custom-2", "9.9.9"
    r = stack.http.post("/v1/runtimes", headers=stack.admin, json={"manifest": tampered})
    assert r.status_code in (400, 422) and "signature" in r.text.lower()
    assert stack.http.post("/v1/runtimes", headers=stack.admin, json={"manifest": {"family": "x"}}).status_code in (400, 422)
    assert stack.http.post("/v1/runtimes", headers=stack.admin, json={"manifest": {"id": "../evil", "family": "x"}}).status_code in (400, 422)
    # re-registering a pristine signed manifest is accepted (idempotent upsert)
    assert stack.http.post("/v1/runtimes", headers=stack.admin, json={"manifest": base}).status_code == 201


# ------------------------------------------------------------------------------------------------ operations on workers
def operation(st: Any, op_id: str, timeout: float = 30) -> dict[str, Any]:
    return wait_until(lambda: (o := st.http.get(f"/v1/operations/{op_id}", headers=st.admin).json())["state"] in ("succeeded", "failed") and o, timeout, 0.1, f"operation {op_id}")


def test_install_verify_retire_rollback_cycle(fleet: Any) -> None:
    st = fleet
    a = st.agent("rt-a", runtimes=[])
    b = st.agent("rt-b", runtimes=[])
    st.wait_worker(a)
    st.wait_worker(b)
    rid = "olcf-rt-1.3.0"
    res = st.http.post(f"/v1/runtimes/{rid}/install", headers=st.admin, json={"worker_ids": ["rt-a"]})
    assert res.status_code == 202, res.text
    assert [t["name"] for t in res.json()["targets"]] == ["rt-a"]
    op = operation(st, res.json()["operation_id"])
    assert op["state"] == "succeeded" and op["result"]["ok"] == 1
    inst = [c for c in a.commands_of("install_runtime")]
    assert len(inst) == 1 and inst[0]["payload"]["runtime_manifest"]["id"] == rid and inst[0]["payload"]["mode"] == "install"
    assert not b.commands_of("install_runtime")
    rt = st.http.get(f"/v1/runtimes/{rid}", headers=st.admin).json()
    assert rt["ready_nodes"] == 1 and rt["nodes"][0]["worker"] == "rt-a" and rt["nodes"][0]["state"] == "ready"
    # verify only touches nodes that have the runtime
    v = st.http.post(f"/v1/runtimes/{rid}/verify", headers=st.admin, json={})
    assert v.status_code == 202 and [t["name"] for t in v.json()["targets"]] == ["rt-a"]
    operation(st, v.json()["operation_id"])
    # a second install on every node
    r2 = st.http.post(f"/v1/runtimes/{rid}/install", headers=st.admin, json={})
    assert {t["name"] for t in r2.json()["targets"]} == {"rt-a", "rt-b"}
    assert operation(st, r2.json()["operation_id"])["result"]["ok"] == 2
    # idempotent POST with the same key replays the first answer
    k1 = st.http.post(f"/v1/runtimes/{rid}/verify", headers={**st.admin, "Idempotency-Key": "rt-verify-1"}, json={})
    k2 = st.http.post(f"/v1/runtimes/{rid}/verify", headers={**st.admin, "Idempotency-Key": "rt-verify-1"}, json={})
    assert k1.json()["operation_id"] == k2.json()["operation_id"]
    operation(st, k1.json()["operation_id"])
    # retire / rollback
    ret = st.http.post(f"/v1/runtimes/{rid}/retire", headers=st.admin, json={})
    assert operation(st, ret.json()["operation_id"])["state"] == "succeeded"
    assert st.http.get(f"/v1/runtimes/{rid}", headers=st.admin).json()["state"] == "retired"
    r = resolve(st, runtime_id=rid)
    assert r.status_code in (400, 422, 501) and "retired" in r.text
    assert resolve(st, family="fabric-1.3-compatible").status_code in (400, 422, 501)  # nothing else satisfies the family
    assert st.http.post(f"/v1/runtimes/{rid}/install", headers=st.admin, json={}).status_code == 409
    rb = st.http.post(f"/v1/runtimes/{rid}/rollback", headers=st.admin, json={})
    assert operation(st, rb.json()["operation_id"])["state"] == "succeeded"
    assert resolve(st, runtime_id=rid).status_code == 200


def test_operation_failures_are_reported_per_node(fleet: Any) -> None:
    st = fleet
    a = st.agent("rt-fail", runtimes=[])
    st.wait_worker(a)
    a.fail_commands["install_runtime"] = "no space left on device"
    res = st.http.post("/v1/runtimes/olcf-rt-native-1.0.0/install", headers=st.admin, json={})
    op = operation(st, res.json()["operation_id"])
    assert op["state"] == "failed" and op["result"]["failed"] == 1 and "no space" in json.dumps(op)
    inst = st.http.get("/v1/runtimes/olcf-rt-native-1.0.0", headers=st.admin).json()["nodes"]
    assert any(i["state"] == "failed" for i in inst)
    # an offline/unknown target is refused up front, and verify without installs has nothing to do
    assert st.http.post("/v1/runtimes/olcf-rt-native-1.0.0/install", headers=st.admin, json={"worker_ids": ["ghost"]}).status_code in (409, 503)
    assert st.http.post("/v1/runtimes/olcf-rt-2.0.0/verify", headers=st.admin, json={"worker_ids": ["rt-fail"]}).status_code in (409, 503)


def test_retire_is_blocked_while_the_runtime_is_in_use(fleet: Any) -> None:
    from tests.compute.fake_agent import JobPlan

    st = fleet
    ws = st.workspace()
    a = st.agent("rt-busy")
    a.job_plan = lambda spec: JobPlan(outcome="hang")
    st.wait_worker(a)
    rid = "olcf-rt-2.0.0"
    st.http.post(f"/v1/runtimes/{rid}/install", headers=st.admin, json={})
    j = st.submit(ws)
    st.wait_job(j["id"], "running")
    r = st.http.post(f"/v1/runtimes/{rid}/retire", headers=st.admin, json={})
    assert r.status_code == 409 and "active job" in r.text
    st.http.post(f"/v1/jobs/{j['id']}/cancel", headers=st.admin)
    st.wait_job(j["id"])


# ------------------------------------------------------------------------------------------------ offline bundles
def test_bundle_upload_download_and_install_descriptor(fleet: Any) -> None:
    st = fleet
    payload = b"PK-fake-bundle-" + bytes(range(256)) * 40
    up = st.http.post("/v1/runtimes/bundles", headers=st.admin, params={"runtime_id": "olcf-rt-2.0.0"}, files={"file": ("../weird name.tar.zst", io.BytesIO(payload))})
    assert up.status_code == 201, up.text
    b = up.json()
    assert b["sha256"] == hashlib.sha256(payload).hexdigest() and b["size"] == len(payload) and "/" not in b["filename"] and ".." not in b["filename"]
    assert b["id"] in {x["id"] for x in st.http.get("/v1/runtimes/bundles", headers=st.admin).json()["items"]}
    dl = st.http.get(f"/v1/runtimes/bundles/{b['id']}/download", headers=st.admin)
    assert dl.status_code == 200 and dl.content == payload and dl.headers["X-Content-SHA256"] == b["sha256"]
    # the install command carries the offline descriptor {bundle_id,url,sha256,size,filename}; the agent downloads it with its worker token
    a = st.agent("rt-offline", runtimes=[])
    st.wait_worker(a)
    res = st.http.post("/v1/runtimes/olcf-rt-2.0.0/install", headers=st.admin, json={"worker_ids": ["rt-offline"], "bundle_id": b["id"]})
    assert res.status_code == 202, res.text
    operation(st, res.json()["operation_id"])
    desc = a.commands_of("install_runtime")[0]["payload"]["offline_bundle"]
    assert set(desc) == {"bundle_id", "url", "sha256", "size", "filename"} and desc["url"].endswith(f"/v1/runtimes/bundles/{b['id']}/download") and desc["sha256"] == b["sha256"]
    tok = st.http.get(f"/v1/runtimes/bundles/{b['id']}/download", headers={"Authorization": f"Bearer {a.token}"})
    assert tok.status_code == 200 and tok.content == payload
    assert st.http.post("/v1/runtimes/olcf-rt-2.0.0/install", headers=st.admin, json={"bundle_id": "bun_missing"}).status_code == 404
    assert st.http.delete(f"/v1/runtimes/bundles/{b['id']}", headers=st.admin).status_code == 204
    assert st.http.get(f"/v1/runtimes/bundles/{b['id']}/download", headers=st.admin).status_code == 404
