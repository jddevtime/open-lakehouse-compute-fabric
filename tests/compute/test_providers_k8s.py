# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Kubernetes provider: manifests, status mapping, cleanup and the full job lifecycle against a test Kubernetes API (and a ``kubectl`` stub).

These tests pin the contract the provider speaks.
"""

from __future__ import annotations

import json
import os
import stat
import sys
import textwrap
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from olcf.compute.errors import ProviderError
from olcf.compute.providers.base import AppHandle, ProviderEnv, SubmitRequest
from olcf.compute.providers.kubernetes import (
    SECRET_LIMIT_BYTES,
    KubectlClient,
    KubernetesProvider,
    build_spark_submit_args,
    k8s_name,
)
from olcf.core.errors import BadRequest, Unsupported
from tests.compute.fake_agent import wait_until
from tests.compute.fake_services import FakeKube

JOB = {"id": "job_01HZZ9ABCDEFGHJKMNPQRSTVWX", "attempt": 2, "workspace_id": "ws_01ABC", "runtime_id": "olcf-rt-2.0.0",
       "resources_granted": {"driver_cores": 2, "driver_memory_mb": 2048}}
SPEC = {"schema": "olcf.jobspec/1", "job_id": JOB["id"], "source": {"path": "source/main.py", "format": "py"}, "spark": {"conf": {"spark.sql.shuffle.partitions": "8"}}}


def provider(kube: FakeKube | None = None, **cfg: Any) -> KubernetesProvider:
    cfg = {"image": "registry.example/olcf-rt:2.0.0", "namespace": "analytics", **cfg}
    p = KubernetesProvider("k8s-test", cfg, ProviderEnv(None, None, None, None), kube=kube)  # type: ignore[arg-type]
    p.validate_config()
    return p


def handle_of(p: KubernetesProvider, kube: FakeKube) -> AppHandle:
    h = p.submit(SubmitRequest(job=JOB, spec=SPEC, source_bytes=b"print(1)"))
    return h


# ------------------------------------------------------------------------------------------------ pure helpers
def test_k8s_name_is_dns1123_and_deterministic() -> None:
    n = k8s_name("job_01HZZ9ABCDEFGHJKMNPQRSTVWX", 2)
    assert n == k8s_name("job_01HZZ9ABCDEFGHJKMNPQRSTVWX", 2) and n != k8s_name("job_01HZZ9ABCDEFGHJKMNPQRSTVWX", 3)
    assert n == n.lower() and "_" not in n and len(n) <= 63 and n[0].isalnum() and n[-1].isalnum()
    assert len(k8s_name("job_" + "X" * 200, 12)) <= 63


def test_config_validation() -> None:
    with pytest.raises(BadRequest, match="image"):
        KubernetesProvider("x", {}, None).validate_config()  # type: ignore[arg-type]
    with pytest.raises(BadRequest, match="submit_mode"):
        KubernetesProvider("x", {"image": "i", "submit_mode": "weird"}, None).validate_config()  # type: ignore[arg-type]
    with pytest.raises(BadRequest, match="kube_backend"):
        KubernetesProvider("x", {"image": "i", "kube_backend": "weird"}, None).validate_config()  # type: ignore[arg-type]
    p = provider(images={"olcf-rt-3": "other:3"})
    assert p.image_for("olcf-rt-3") == "other:3" and p.image_for("olcf-rt-2.0.0") == "registry.example/olcf-rt:2.0.0"
    with pytest.raises(ProviderError):
        KubernetesProvider("x", {"images": {"a": "b"}}, None).image_for("zzz")  # type: ignore[arg-type]


def test_manifests_are_complete_and_safe() -> None:
    p = provider(service_account="olcf-driver", image_pull_secret="regcred", driver_node_selector={"pool": "spark"})  # noqa: S106
    name = k8s_name(JOB["id"], JOB["attempt"])
    secret, key = p.build_secret(JOB, SPEC, b"print(1)", name)
    assert secret["kind"] == "Secret" and secret["metadata"]["namespace"] == "analytics" and key == "source__source__main.py"
    assert json.loads(secret["stringData"]["job.json"])["job_id"] == JOB["id"] and secret["stringData"][key] == "print(1)"
    pod = p.build_pod(JOB, SPEC, name, name, key)
    c = pod["spec"]["containers"][0]
    assert pod["spec"]["restartPolicy"] == "Never" and c["image"] == "registry.example/olcf-rt:2.0.0"
    assert c["command"] == ["python", "-m", "olcf.runtime_lib.runner", "--workdir", "/opt/olcf/work", "/opt/olcf/input/job.json"]
    assert {"name": "OLCF_JOB_ID", "value": JOB["id"]} in c["env"]
    assert pod["metadata"]["labels"]["olcf.io/job-id"] == JOB["id"] and pod["metadata"]["labels"]["olcf.io/attempt"] == "2"
    # driver memory request includes Spark's overhead; secrets are mounted read-only with restrictive modes and the source keeps its path
    assert c["resources"]["requests"] == {"cpu": "2", "memory": "2432Mi"} and c["resources"]["limits"] == {"memory": "2432Mi"}
    vol = pod["spec"]["volumes"][0]["secret"]
    # the Secret is an *input* mounted read-only (group-readable for the fsGroup of the non-root user); the runner works in a separate writable emptyDir (review F03)
    assert vol["defaultMode"] == 0o440 and {"key": key, "path": "source/main.py"} in vol["items"] and c["volumeMounts"][0] == {"name": "input", "mountPath": "/opt/olcf/input", "readOnly": True}
    assert pod["spec"]["serviceAccountName"] == "olcf-driver" and pod["spec"]["imagePullSecrets"] == [{"name": "regcred"}] and pod["spec"]["nodeSelector"] == {"pool": "spark"}
    assert "privileged" not in json.dumps(pod) and "hostNetwork" not in json.dumps(pod)
    svc = p.build_service(JOB, SPEC, name)
    assert svc["spec"]["clusterIP"] == "None" and svc["spec"]["selector"]["olcf.io/job-id"] == JOB["id"]
    assert {pt["port"] for pt in svc["spec"]["ports"]} == {7078, 7079, 4040}


def test_oversized_payload_is_refused_with_remediation() -> None:
    p = provider()
    with pytest.raises(Unsupported) as ei:
        p.build_secret(JOB, SPEC, b"x" * (SECRET_LIMIT_BYTES + 10), "n")
    assert "artifact_uri" in (ei.value.remediation or "")


def test_spark_submit_args() -> None:
    args = build_spark_submit_args({"api_server": "https://k.example:6443", "spark_submit_path": "/opt/spark/bin/spark-submit"}, pod_name="p1", secret_name="s1",  # noqa: S106
                                   conf={"spark.app.name": "app", "spark.a": "1"}, entrypoint="local:///opt/olcf/entrypoint.py")
    assert args[:7] == ["/opt/spark/bin/spark-submit", "--master", "k8s://https://k.example:6443", "--deploy-mode", "cluster", "--name", "app"]
    assert "spark.kubernetes.driver.pod.name=p1" in args and "spark.kubernetes.driver.secrets.s1=/opt/olcf/input" in args and args[-1] == "local:///opt/olcf/entrypoint.py"
    assert all(isinstance(a, str) for a in args)


def test_prepare_builds_executor_pod_conf() -> None:
    from olcf.compute.providers.base import PrepareRequest

    p = provider(service_account="sa", executor_node_selector={"pool": "exec"}, api_server="https://k:6443")
    req = PrepareRequest(job={**JOB, "resources_request": {"dynamic_allocation": True, "min_executors": 1, "max_executors": 6}},
                         placement={"executors": 3, "executor_cores": 2, "executor_memory_mb": 4096, "driver_cores": 2, "driver_memory_mb": 2048},
                         runtime={"manifest": {"spark_conf_defaults": {"spark.sql.adaptive.enabled": "true"}}}, spark_conf={"spark.sql.shuffle.partitions": "8"}, app_name="olcf-app")
    sp = p.prepare(req)
    c = sp["conf"]
    assert sp["master"] == "k8s://https://k:6443" and sp["driver_host"] == f"{k8s_name(JOB['id'], 2)}.analytics.svc"
    assert c["spark.executor.instances"] == "3" and c["spark.executor.memory"] == "4096m" and c["spark.kubernetes.container.image"] == "registry.example/olcf-rt:2.0.0"
    assert c["spark.kubernetes.executor.deleteOnTermination"] == "true" and c["spark.kubernetes.driver.pod.name"] == k8s_name(JOB["id"], 2)
    assert c["spark.kubernetes.authenticate.driver.serviceAccountName"] == "sa" and c["spark.kubernetes.executor.node.selector.pool"] == "exec"
    assert c["spark.dynamicAllocation.enabled"] == "true" and c["spark.dynamicAllocation.maxExecutors"] == "6" and c["spark.sql.adaptive.enabled"] == "true"
    assert c["spark.sql.shuffle.partitions"] == "8"


# ------------------------------------------------------------------------------------------------ provider against the fake API
def test_submit_creates_secret_service_pod_and_status_walks_to_success() -> None:
    kube = FakeKube()
    p = provider(kube)
    h = handle_of(p, kube)
    name = k8s_name(JOB["id"], 2)
    assert kube.kinds("secret") == [name] and kube.kinds("service") == [name] and kube.kinds("pod") == [name]
    assert [o["kind"] for o in kube.created] == ["Secret", "Service", "Pod"]  # the Secret exists before the pod that mounts it
    states = []
    for _ in range(6):
        st = p.status(h)
        states.append(st.state)
        if st.state == "succeeded":
            break
    assert states[0] == "pending" and "running" in states and states[-1] == "succeeded"
    assert st.exit_code == 0 and st.native["pod_ip"] == "10.0.0.7" and st.native["node"] == "node-1"


def test_failed_pod_maps_to_failed_with_exit_code() -> None:
    kube = FakeKube(outcome="fail")
    p = provider(kube)
    h = handle_of(p, kube)
    st = wait_until(lambda: (s := p.status(h)).state == "failed" and s, 5, 0.01, "failed")
    assert st.exit_code == 1 and "Error" in st.detail


def test_missing_pod_is_reported_failed_and_hang_stays_running() -> None:
    kube = FakeKube(outcome="hang")
    p = provider(kube)
    h = handle_of(p, kube)
    for _ in range(5):
        st = p.status(h)
    assert st.state == "running"
    kube.objects.clear()
    assert p.status(h).state == "failed" and "does not exist" in p.status(h).detail


def test_logs_are_paged_by_a_cursor_not_by_a_line_count() -> None:
    kube = FakeKube(log=[f"line {i}" for i in range(10)], emit_result=True)
    p = provider(kube)
    h = handle_of(p, kube)
    wait_until(lambda: p.status(h).state == "succeeded", 5, 0.01, "done")
    page = p.logs(h, 0, 20)  # tiny byte budget -> a few lines
    assert page.data.startswith("line 0\nline 1\n") and 0 < page.next_offset < 11 and not page.eof and page.cursor
    seen, off, cur, guard = "", 0, None, 0
    while guard < 50:
        pg = p.logs(h, off, 40, cursor=cur)
        seen += pg.data
        off, cur = pg.next_offset, pg.cursor
        guard += 1
        if pg.eof:
            break
    assert [x for x in seen.splitlines() if x.startswith("line ")] == [f"line {i}" for i in range(10)]
    assert "@@OLCF@@" in seen  # the result line is part of the log; the controller extracts it
    again = p.logs(h, off, 40, cursor=cur)  # stateless: the cursor alone says where to continue, and nothing is repeated at the end
    assert again.data == "" and again.eof and again.cursor == cur


def test_cancel_deletes_pod_service_secret_and_force_uses_zero_grace() -> None:
    kube = FakeKube(outcome="hang")
    p = provider(kube)
    h = handle_of(p, kube)
    p.cancel(h, grace_seconds=12)
    assert kube.objects == {}
    assert ("pod", "analytics", k8s_name(JOB["id"], 2), 12) in kube.deleted
    p.cancel(h, force=True)  # idempotent
    assert ("pod", "analytics", k8s_name(JOB["id"], 2), 0) in kube.deleted


def test_cleanup_keeps_the_pod_for_log_retrieval() -> None:
    kube = FakeKube()
    p = provider(kube)
    h = handle_of(p, kube)
    p.cleanup(h)
    assert kube.kinds("secret") == [] and kube.kinds("service") == [] and kube.kinds("pod") == [k8s_name(JOB["id"], 2)]


def test_partial_creation_is_rolled_back() -> None:
    kube = FakeKube()
    kube.fail_create_kind = "pod"
    p = provider(kube)
    with pytest.raises(ProviderError, match="quota"):
        handle_of(p, kube)
    assert kube.objects == {}, "secret and service of the failed attempt must not leak"
    assert [d[0] for d in kube.deleted] == ["service", "secret"]


def test_reconcile_and_discover() -> None:
    kube = FakeKube()
    p = provider(kube)
    handle_of(p, kube)
    foreign = {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "other", "namespace": "analytics", "labels": {"app": "something-else"}}}
    kube.create(foreign)
    found = p.reconcile()
    assert len(found) == 1 and found[0].job_id == JOB["id"] and found[0].native["attempt"] == "2" and found[0].state == "pending"
    d = p.discover()
    assert d.ok and "v1.29.0-fake" in d.detail and d.live_verified is False and d.extra["namespace"] == "analytics"
    kube.unreachable = True
    d = p.discover()
    assert not d.ok and "unreachable" in d.detail


def test_scale_and_ui() -> None:
    kube = FakeKube()
    p = provider(kube, ui_url_template="https://ui.example/{namespace}/{pod}")
    h = handle_of(p, kube)
    with pytest.raises(Unsupported, match="dynamic allocation"):
        p.scale(h, {"executors": 4})
    ui = p.ui(h)
    assert ui.available and ui.upstream == f"https://ui.example/analytics/{k8s_name(JOB['id'], 2)}"
    assert provider(kube, ui_url_template="http://{nope}").ui(h).available is False


def test_spark_submit_mode_runs_the_binary_without_a_shell(tmp_path: Path) -> None:
    out = tmp_path / "args.json"
    exe = tmp_path / "spark-submit"
    exe.write_text(f"#!{sys.executable}\nimport json, sys\njson.dump(sys.argv[1:], open({str(out)!r}, 'w'))\n")
    exe.chmod(exe.stat().st_mode | stat.S_IXUSR)
    kube = FakeKube()
    p = provider(kube, submit_mode="spark_submit", spark_submit_path=str(exe), api_server="https://k:6443")
    h = p.submit(SubmitRequest(job=JOB, spec=SPEC, source_bytes=b"print(1)"))
    wait_until(out.exists, 10, 0.05, "spark-submit to run")
    wait_until(lambda: out.read_text().endswith("]"), 5, 0.05, "args written")
    args = json.loads(out.read_text())
    assert "k8s://https://k:6443" in args and f"spark.kubernetes.driver.secrets.{k8s_name(JOB['id'], 2)}=/opt/olcf/input" in args
    assert kube.kinds("secret") and not kube.kinds("pod") and h.native["service"] is None
    # the driver pod does not exist yet: pending during the grace period, failed afterwards
    assert p.status(h).state == "pending"
    h.native["submitted_at"] = 0
    assert p.status(h).state == "failed"


def test_spark_submit_missing_binary_cleans_up() -> None:
    kube = FakeKube()
    p = provider(kube, submit_mode="spark_submit", spark_submit_path="/nonexistent/spark-submit")
    with pytest.raises(ProviderError, match="spark-submit"):
        handle_of(p, kube)
    assert kube.objects == {}


# ------------------------------------------------------------------------------------------------ kubectl backend
FAKE_KUBECTL = textwrap.dedent('''\
    #!{python}
    import json, os, sys
    store = {store!r}
    args = sys.argv[1:]
    db = json.load(open(store)) if os.path.exists(store) else {{"objs": {{}}, "calls": []}}
    db["calls"].append(args)
    if "--kubeconfig" in args:
        i = args.index("--kubeconfig"); db["kubeconfig"] = open(args[i + 1]).read(); del args[i:i + 2]
    if "--context" in args:
        i = args.index("--context"); db["context"] = args[i + 1]; del args[i:i + 2]
    cmd = args[0]
    def out(doc):
        json.dump(db, open(store, "w")); print(json.dumps(doc) if doc is not None else ""); sys.exit(0)
    if cmd == "create":
        m = json.loads(sys.stdin.read()); db["objs"][m["kind"].lower() + "/" + m["metadata"]["name"]] = m; out(m)
    if cmd == "get" and "-l" not in args:
        o = db["objs"].get(args[1] + "/" + args[2]); out(o)
    if cmd == "get":
        out({{"items": [o for k, o in db["objs"].items() if k.startswith(args[1] + "/")]}})
    if cmd == "delete":
        db["objs"].pop(args[1] + "/" + args[2], None); out(None)
    if cmd == "logs":
        json.dump(db, open(store, "w")); print("kubectl log line"); sys.exit(0)
    if cmd == "version":
        out({{"serverVersion": {{"gitVersion": "v1.30.1"}}}})
    sys.exit(2)
''')


def test_kubectl_backend_speaks_argv_only(tmp_path: Path) -> None:
    store = tmp_path / "state.json"
    exe = tmp_path / "kubectl"
    exe.write_text(FAKE_KUBECTL.format(python=sys.executable, store=str(store)))
    exe.chmod(exe.stat().st_mode | stat.S_IXUSR)
    kc = KubectlClient(str(exe), kubeconfig_text="apiVersion: v1\nkind: Config\n", context="prod")
    try:
        pod = {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "p1", "namespace": "ns", "labels": {"app": "olcf-spark-driver"}}}
        kc.create(pod)
        assert kc.get("pod", "ns", "p1")["metadata"]["name"] == "p1" and kc.get("pod", "ns", "missing") is None
        assert [o["metadata"]["name"] for o in kc.list("pod", "ns", "app=olcf-spark-driver")] == ["p1"]
        assert kc.pod_log("ns", "p1").strip() == "kubectl log line"
        assert kc.version() == "v1.30.1"
        kc.delete("pod", "ns", "p1", grace_seconds=0)
        db = json.loads(store.read_text())
        assert db["objs"] == {} and db["context"] == "prod" and db["kubeconfig"].startswith("apiVersion")
        dele = [c for c in db["calls"] if "delete" in c][0]
        assert "--grace-period=0" in dele and "--ignore-not-found" in dele
        kcfg = kc._kubeconfig
        assert kcfg and oct(os.stat(kcfg).st_mode & 0o777) == "0o600"
    finally:
        kc.close()
    assert not os.path.exists(kcfg)
    bad = KubectlClient(str(tmp_path / "nope"))
    with pytest.raises(ProviderError, match="kubectl"):
        bad.version()


# ------------------------------------------------------------------------------------------------ end to end through the control plane
@pytest.fixture()
def k8s(stack: Any) -> Iterator[tuple[str, FakeKube]]:
    kube = FakeKube(emit_result=True)
    name = f"k8s-{uuid.uuid4().hex[:6]}"
    r = stack.http.post("/v1/providers", headers=stack.admin, json={"name": name, "type": "kubernetes", "config": {"image": "registry.example/olcf-rt:2.0.0", "namespace": "analytics"}})
    assert r.status_code == 201, r.text
    stack.compute.providers.get(name)._kube = kube  # inject the fake cluster into the controller's cached instance
    try:
        yield name, kube
    finally:
        stack.http.delete(f"/v1/providers/{name}", headers=stack.admin)


def test_k8s_job_succeeds_and_result_is_scraped_from_the_pod_log(stack: Any, k8s: tuple[str, FakeKube]) -> None:
    name, kube = k8s
    ws = stack.workspace()
    j = stack.submit(ws, engine="spark", policy={"placement": {"provider": name}})
    done = stack.wait_job(j["id"], timeout=60)
    assert done["state"] == "succeeded", done
    assert done["provider"] == name and done["runtime_id"] == "olcf-rt-2.0.0"
    assert done["result"].get("rows") == 3
    assert "driver starting" in stack.logs(j["id"]) and "stage 1 done" in stack.logs(j["id"])
    (pod,) = [o for o in kube.created if o["kind"] == "Pod"]
    assert pod["metadata"]["labels"]["olcf.io/job-id"] == j["id"]
    secret = [o for o in kube.created if o["kind"] == "Secret"][0]
    spec = json.loads(secret["stringData"]["job.json"])
    assert spec["job_id"] == j["id"] and spec["spark"]["master"].startswith("k8s://")
    assert spec["spark"]["conf"]["spark.kubernetes.container.image"] == "registry.example/olcf-rt:2.0.0"
    wait_until(lambda: kube.kinds("secret") == [], 15, 0.1, "the job secret (it holds the job token) to be removed after the job")


def test_k8s_job_failure_is_classified_from_the_result_line(stack: Any, k8s: tuple[str, FakeKube]) -> None:
    name, kube = k8s
    kube.outcome = "fail"
    ws = stack.workspace()
    done = stack.wait_job(stack.submit(ws, engine="spark", policy={"placement": {"provider": name}})["id"], timeout=60)
    assert done["state"] == "failed"
    assert done["failure"]["category"] == "user-code" and "NameError" in done["failure"]["message"]


def test_k8s_cancel_removes_the_driver_pod(stack: Any, k8s: tuple[str, FakeKube]) -> None:
    name, kube = k8s
    kube.outcome = "hang"
    ws = stack.workspace()
    j = stack.submit(ws, engine="spark", policy={"placement": {"provider": name}})
    stack.wait_job(j["id"], "running", timeout=60)
    assert stack.http.post(f"/v1/jobs/{j['id']}/cancel", headers=stack.admin).status_code in (200, 202)
    assert stack.wait_job(j["id"], timeout=60)["state"] == "cancelled"
    wait_until(lambda: kube.kinds("pod") == [], 10, 0.1, "driver pod deletion")
    assert any(d[0] == "pod" for d in kube.deleted)


def test_k8s_lost_cluster_contact_does_not_fail_a_job_immediately(stack: Any, k8s: tuple[str, FakeKube]) -> None:
    name, kube = k8s
    kube.outcome = "hang"
    ws = stack.workspace()
    j = stack.submit(ws, engine="spark", policy={"placement": {"provider": name}})
    stack.wait_job(j["id"], "running", timeout=60)
    kube.unreachable = True
    time.sleep(2.0)  # several monitor polls fail meanwhile
    assert stack.job(j["id"])["state"] == "running", "a transient API outage must not kill the job (the limit is compute.remote_status_error_limit polls)"
    kube.unreachable = False
    kube.outcome = "succeed"
    assert stack.wait_job(j["id"], timeout=60)["state"] == "succeeded"
