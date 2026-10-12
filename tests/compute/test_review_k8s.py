# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Regression tests for the external code review of 2026-10-08, findings F03, F07 and F08 (Kubernetes / remote completion contract).

Nothing here talks to a real cluster. :class:`tests.compute.fake_kube.FakeKube` stands in for the API server *and* the kubelet: in ``runner`` mode it really runs the
pod command (the production runner, ``python -m olcf.runtime_lib.runner ...``) with the pod's volumes mapped to scratch directories, enforces that Secret volumes are
read-only (``EROFS``) and turns the container's output into the pod log and the termination message. What stays unverified is everything a fake cannot show: a real
kubelet's volume ownership, a real container runtime's log rotation, a CNI.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from olcf.compute.providers.base import AppHandle, ProviderEnv, SubmitRequest
from olcf.compute.providers.kubernetes import INPUT_DIR, WORK_DIR, KubernetesProvider, k8s_name
from tests.compute.fake_agent import wait_until
from tests.compute.fake_kube import LAUNCHER, FakeKube

JOB = {"id": "job_01HZZ9ABCDEFGHJKMNPQRSTVWX", "attempt": 1, "workspace_id": "ws_01ABC", "runtime_id": "olcf-rt-2.0.0", "resources_granted": {}}


def spec_for(source_format: str = "py", **over: Any) -> dict[str, Any]:
    return {"schema": "olcf.jobspec/1", "job_id": JOB["id"], "attempt": 1, "kind": "script", "engine": "python", "source": {"path": "source/main.py", "format": source_format},
            "parameters": {}, "limits": {"timeout_seconds": 120, "max_output_bytes": 10_000_000}, "callbacks": {"api_url": "", "job_token": ""}, **over}


def provider(kube: FakeKube, **cfg: Any) -> KubernetesProvider:
    p = KubernetesProvider("k8s-review", {"image": "registry.example/olcf-rt:2.0.0", "namespace": "analytics", **cfg}, ProviderEnv(None, None, None, None), kube=kube)  # type: ignore[arg-type]
    p.validate_config()
    return p


def run_to_end(p: KubernetesProvider, source: bytes, *, spec: dict[str, Any] | None = None, timeout: float = 60) -> tuple[AppHandle, Any, str]:
    h = p.submit(SubmitRequest(job=JOB, spec=spec or spec_for(), source_bytes=source))
    st = wait_until(lambda: (s := p.status(h)).state in ("succeeded", "failed") and s, timeout, 0.1, "the pod to end")
    return h, st, drain(p, h)


def drain(p: KubernetesProvider, h: AppHandle) -> str:
    out, off, cur = "", 0, None
    for _ in range(1000):
        pg = p.logs(h, off, 65536, cursor=cur)
        out += pg.data
        off, cur = pg.next_offset, pg.cursor
        if pg.eof:
            break
    return out


def result_events(text: str) -> list[dict[str, Any]]:
    evs = [json.loads(line[8:]) for line in text.splitlines() if line.startswith("@@OLCF@@")]
    return [e for e in evs if e.get("event") == "result"]


@pytest.fixture()
def kube() -> Iterator[FakeKube]:
    k = FakeKube(runner=True, pending_polls=0)
    try:
        yield k
    finally:
        k.close()


# ================================================================================================ F03 volumes and identity
def test_f03_secret_is_an_input_mount_and_the_work_dir_is_a_separate_writable_volume() -> None:
    p = provider(FakeKube())
    name = k8s_name(JOB["id"], 1)
    secret, key = p.build_secret(JOB, spec_for(), b"print(1)", name)
    pod = p.build_pod(JOB, spec_for(), name, name, key)
    c, vols = pod["spec"]["containers"][0], {v["name"]: v for v in pod["spec"]["volumes"]}
    mounts = {m["mountPath"]: m for m in c["volumeMounts"]}
    assert "secret" in vols[mounts[INPUT_DIR]["name"]] and mounts[INPUT_DIR]["readOnly"] is True
    assert "emptyDir" in vols[mounts[WORK_DIR]["name"]] and not mounts[WORK_DIR].get("readOnly")
    assert INPUT_DIR != WORK_DIR and not WORK_DIR.startswith(INPUT_DIR + "/") and not INPUT_DIR.startswith(WORK_DIR + "/")
    # the runner reads the input directory and writes into the work directory (it stages the files itself)
    assert c["command"][-3:] == ["--workdir", WORK_DIR, f"{INPUT_DIR}/job.json"]
    # a non-root user can read the Secret (mode + fsGroup) and write the work directory (fsGroup); nothing is privileged
    sc = pod["spec"]["securityContext"]
    assert sc["runAsNonRoot"] is True and sc["runAsUser"] > 0 and sc["fsGroup"] > 0 and sc["seccompProfile"]["type"] == "RuntimeDefault"
    assert vols[mounts[INPUT_DIR]["name"]]["secret"]["defaultMode"] & 0o040 and not vols[mounts[INPUT_DIR]["name"]]["secret"]["defaultMode"] & 0o007
    assert c["securityContext"] == {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}, "readOnlyRootFilesystem": False}
    assert "/tmp" in mounts  # noqa: S108 - an emptyDir inside the pod
    assert c["terminationMessagePath"] == "/dev/termination-log" and c["terminationMessagePolicy"] == "File"
    # custom commands may use the placeholders; the work directory can be size-limited and the root filesystem locked
    p2 = provider(FakeKube(), runner_command=["run", "{input_dir}/job.json", "{work_dir}"], work_dir_size_limit="2Gi", read_only_root_filesystem=True, run_as_user=2000)
    pod2 = p2.build_pod(JOB, spec_for(), name, name, key)
    assert pod2["spec"]["containers"][0]["command"] == ["run", f"{INPUT_DIR}/job.json", WORK_DIR]
    assert pod2["spec"]["securityContext"]["runAsUser"] == 2000 and pod2["spec"]["containers"][0]["securityContext"]["readOnlyRootFilesystem"] is True
    assert {v["name"]: v for v in pod2["spec"]["volumes"]}["work"]["emptyDir"] == {"sizeLimit": "2Gi"}


def test_f03_the_fake_cluster_really_keeps_secret_mounts_read_only(tmp_path: Path) -> None:
    """The enforcement itself is tested, so the passing runner tests below prove something: a write below a read-only mount fails with EROFS - also when the mount is
    declared ``readOnly: false``, which a kubelet ignores for Secrets."""
    ro, rw = tmp_path / "ro", tmp_path / "rw"
    ro.mkdir()
    rw.mkdir()
    (ro / "job.json").write_text("{}")
    code = (f"import errno, os\nfor p in ({str(ro / 'job.json')!r}, {str(ro / 'new.txt')!r}):\n"
            f"    try:\n        open(p, 'w').write('x'); print('WROTE', p)\n    except OSError as e:\n        print('ERR', e.errno == errno.EROFS, os.path.basename(p))\n"
            f"open({str(rw / 'ok.txt')!r}, 'w').write('x'); print('RW ok')\n")
    mod = tmp_path / "probe_mod.py"
    mod.write_text(code)
    out = subprocess.run([sys.executable, str(LAUNCHER), "--ro", str(ro), "-m", "probe_mod"], capture_output=True, text=True, cwd=tmp_path, env={**os.environ, "PYTHONPATH": str(tmp_path)}, check=True)  # noqa: S603
    assert out.stdout.split() == ["ERR", "True", "job.json", "ERR", "True", "new.txt", "RW", "ok"], out.stdout + out.stderr


def test_f03_the_stock_runner_completes_in_a_pod_with_a_read_only_secret_and_returns_its_result(kube: FakeKube) -> None:
    p = provider(kube)
    h, st, text = run_to_end(p, b"EXIT_VALUE = {'answer': 42}\nprint('hello from the pod')\n")
    assert st.state == "succeeded" and st.exit_code == 0, text
    assert "hello from the pod" in text
    (pod,) = [o for o in kube.created if o["kind"] == "Pod"]
    assert any(m["mountPath"] == INPUT_DIR and m["readOnly"] for m in pod["spec"]["containers"][0]["volumeMounts"])
    work = kube.workdir(h.native["pod"], WORK_DIR)
    assert json.loads((work / "result.json").read_text())["exit_value"] == {"answer": 42}  # result.json was written to the *work* directory
    # the staged copy of job.json holds the job token and secrets: it is gone when the run is over (the Secret volume itself is deleted with the Secret)
    assert not (work / "job.json").exists()


def test_f03_a_runner_pointed_at_the_read_only_directory_cannot_finish_which_is_what_the_old_default_did(kube: FakeKube) -> None:
    """Reproduction of the review's evidence: with the pre-fix command (no ``--workdir``, job directory = the Secret mount) the pod ends Failed with no result."""
    p = provider(kube, runner_command=["python", "-m", "olcf.runtime_lib.runner", f"{INPUT_DIR}/job.json"])
    _, st, text = run_to_end(p, b"EXIT_VALUE = 1\n")
    assert st.state == "failed" and st.exit_code == 1
    assert not result_events(text)  # no completion data of any kind
    assert "cannot persist result.json" not in text or "Read-only file system" in text


# ================================================================================================ F07 one completion contract
def test_f07_the_runner_emits_the_result_event_exactly_once_after_persisting_result_json(kube: FakeKube) -> None:
    p = provider(kube)
    h, st, text = run_to_end(p, b"EXIT_VALUE = {'rows': [1, 2, 3], 'ok': True}\n")
    (ev,) = result_events(text)
    assert ev["job_id"] == JOB["id"] and ev["attempt"] == 1
    on_disk = json.loads((kube.workdir(h.native["pod"], WORK_DIR) / "result.json").read_text())
    assert ev["result"] == on_disk, "the event carries the persisted document"
    assert ev["result"]["state"] == "succeeded" and ev["result"]["exit_value"] == {"rows": [1, 2, 3], "ok": True} and ev["result"]["metrics"]["duration_seconds"] >= 0
    lines = text.splitlines()
    assert lines.index(next(x for x in lines if '"event": "result"' in x)) < lines.index(next(x for x in lines if '"runner_finished"' in x))
    assert p.find_result(h) == on_disk


def test_f07_a_failure_result_carries_the_traceback_category_and_a_failing_exit_status(kube: FakeKube) -> None:
    p = provider(kube)
    _, st, text = run_to_end(p, b"def boom():\n    raise ValueError('kaput')\nboom()\n")
    assert st.state == "failed" and st.exit_code == 1
    (ev,) = result_events(text)
    assert ev["result"]["state"] == "failed" and ev["result"]["failure"]["category"] == "user-code" and "kaput" in json.dumps(ev["result"]["failure"])
    rec = st.termination["result"]  # the same outcome from the termination record, which does not depend on the log
    assert rec["state"] == "failed" and rec["failure"]["category"] == "user-code" and "kaput" in rec["failure"]["message"]


def test_f07_a_huge_exit_value_is_shortened_in_the_event_and_the_complete_result_is_referenced(kube: FakeKube) -> None:
    p = provider(kube)
    _, st, text = run_to_end(p, b"EXIT_VALUE = {'blob': 'x' * 1_500_000}\nprint('done')\n")
    assert st.state == "succeeded"
    (ev,) = result_events(text)
    line = next(x for x in text.splitlines() if '"event": "result"' in x)
    assert len(line.encode()) < 260 * 1024, "a log line stays small however large the result is"
    r = ev["result"]
    assert r["exit_value"]["_truncated"] is True and r["exit_value"]["original_bytes"] > 1_500_000
    ref = r["result_ref"]
    assert ref["name"] == "result.json" and ref["bytes"] > 1_500_000 and ref["complete"] is False and len(ref["sha256"]) == 64
    assert ref["stored"] is False and "callbacks" in ref["error"]  # this spec has no controller to upload to; the end-to-end test below has one


def test_f07_the_termination_record_is_compact_and_survives_without_the_log(kube: FakeKube) -> None:
    p = provider(kube)
    h, st, _ = run_to_end(p, b"EXIT_VALUE = {'a': 1}\n")
    assert st.termination and st.termination["reason"] == "Completed"
    raw = [c for c in kube.get("pod", "analytics", h.native["pod"])["status"]["containerStatuses"]][0]["state"]["terminated"]["message"]
    assert len(raw.encode()) <= 4096
    assert st.termination["result"]["state"] == "succeeded" and st.termination["result"]["exit_value"] == {"a": 1}
    kube.log_retain_bytes = 1  # the container runtime rotated every line away
    kube._pods[next(iter(kube._pods))].lines.clear()
    assert p.find_result(h) is None and drain(p, h) == ""
    assert p.status(h).termination["result"]["state"] == "succeeded"


def test_f07_oom_kill_is_reported_as_a_resource_failure_without_a_result(kube: FakeKube) -> None:
    p = provider(kube)
    h = p.submit(SubmitRequest(job=JOB, spec=spec_for(), source_bytes=b"import time\ntime.sleep(60)\n"))
    wait_until(lambda: p.status(h).state == "running", 30, 0.1, "running")
    time.sleep(1.0)
    kube.kill(h.native["pod"], reason="OOMKilled", exit_code=137)
    st = wait_until(lambda: (s := p.status(h)).state == "failed" and s, 30, 0.1, "the pod to fail")
    assert st.termination["reason"] == "OOMKilled" and st.termination["failure"]["category"] == "resource" and st.confirmed_gone is True
    assert "result" not in st.termination


def test_f03_cancel_deletes_the_pod_and_the_runner_reports_cancellation(kube: FakeKube) -> None:
    p = provider(kube)
    h = p.submit(SubmitRequest(job=JOB, spec=spec_for(), source_bytes=b"import time\nprint('started', flush=True)\ntime.sleep(60)\n"))
    wait_until(lambda: p.status(h).state == "running" and "\nstarted\n" in drain(p, h), 30, 0.2, "the job to start")
    pod = h.native["pod"]
    p.cancel(h, grace_seconds=10)
    assert kube.kinds("pod") == [] and ("pod", "analytics", pod, 10) in kube.deleted


# ================================================================================================ end to end through the control plane
@pytest.fixture()
def real_k8s(stack: Any) -> Iterator[tuple[str, FakeKube]]:
    kube = FakeKube(runner=True, pending_polls=0)
    name = f"k8s-{uuid.uuid4().hex[:6]}"
    r = stack.http.post("/v1/providers", headers=stack.admin, json={"name": name, "type": "kubernetes", "config": {"image": "registry.example/olcf-rt:2.0.0", "namespace": "analytics"}})
    assert r.status_code == 201, r.text
    stack.compute.providers.get(name)._kube = kube
    try:
        yield name, kube
    finally:
        stack.http.delete(f"/v1/providers/{name}", headers=stack.admin)
        kube.close()


def job_events(stack: Any, job_id: str) -> list[str]:
    from sqlalchemy import select

    from olcf.compute import schema as C  # noqa: N812

    with stack.compute.jobs.db.connect() as conn:
        return [r[0] for r in conn.execute(select(C.job_events.c.event).where(C.job_events.c.job_id == job_id).order_by(C.job_events.c.seq)).all()]


def put_setting(stack: Any, key: str, value: Any) -> None:
    assert stack.http.put(f"/v1/admin/settings/{key}", headers=stack.admin, json={"value": value}).status_code == 200


def submit_on(stack: Any, ws: str, name: str, source: str, **over: Any) -> dict[str, Any]:
    return stack.submit(ws, source=source, engine="spark", policy={"placement": {"provider": name}}, **over)


def test_f07_job_result_reaches_the_api_through_the_stock_runner(stack: Any, real_k8s: tuple[str, FakeKube]) -> None:
    name, kube = real_k8s
    ws = stack.workspace()
    j = submit_on(stack, ws, name, "EXIT_VALUE = {'answer': 42}\nprint('from a pod')\n")
    done = stack.wait_job(j["id"], timeout=90)
    assert done["state"] == "succeeded", done
    assert done["result"]["exit_value"] == {"answer": 42} and done["result"]["metrics"]["duration_seconds"] >= 0
    assert "from a pod" in stack.logs(j["id"])
    wait_until(lambda: kube.kinds("secret") == [], 15, 0.1, "the job secret (it holds the job token) to be removed")


def test_f07_failed_job_reports_the_runner_failure(stack: Any, real_k8s: tuple[str, FakeKube]) -> None:
    name, _ = real_k8s
    done = stack.wait_job(submit_on(stack, stack.workspace(), name, "raise RuntimeError('pod says no')")["id"], timeout=90)
    assert done["state"] == "failed" and done["failure"]["category"] == "user-code" and "pod says no" in json.dumps(done["failure"])


def test_f07_oversized_result_is_uploaded_with_the_job_token_and_referenced(stack: Any, real_k8s: tuple[str, FakeKube]) -> None:
    name, _ = real_k8s
    j = submit_on(stack, stack.workspace(), name, "EXIT_VALUE = {'blob': 'y' * 1_000_000}\n")
    done = stack.wait_job(j["id"], timeout=90)
    assert done["state"] == "succeeded", done
    ref = done["result"]["result_ref"]
    assert ref["stored"] is True and ref["complete"] is False
    r = stack.http.get(f"/v1/jobs/{j['id']}/artifacts/result.json", headers=stack.admin)
    assert r.status_code == 200 and len(r.content) == ref["bytes"] and r.json()["exit_value"]["blob"].startswith("yyyy")


def test_f07_notebook_exit_value_and_output_artifact_reach_the_api(stack: Any, real_k8s: tuple[str, FakeKube]) -> None:
    name, _ = real_k8s
    nb = {"cells": [{"cell_type": "code", "metadata": {}, "source": ["x = 6 * 7\n", "print('nb ran')"], "outputs": [], "execution_count": None},
                    {"cell_type": "code", "metadata": {}, "source": ["from notebookutils import notebook\n", "notebook.exit({'x': x})"], "outputs": [], "execution_count": None}],
          "metadata": {"kernelspec": {"name": "python3", "display_name": "Python 3", "language": "python"}}, "nbformat": 4, "nbformat_minor": 5}
    r = stack.http.post("/v1/jobs", headers=stack.admin, json={"kind": "notebook", "workspace_id": stack.workspace(), "engine": "python", "name": "nb",
                                                              "source": {"format": "ipynb", "content": json.dumps(nb)}, "policy": {"placement": {"provider": name}}})
    assert r.status_code == 202, r.text
    done = stack.wait_job(r.json()["id"], timeout=120)
    assert done["state"] == "succeeded", done
    assert done["result"]["exit_value"] == {"x": 42}
    art = stack.http.get(f"/v1/jobs/{done['id']}/artifacts/output.ipynb", headers=stack.admin)
    assert art.status_code == 200 and "nb ran" in art.text
    stored = [a for a in done["result"]["artifacts"] if a["name"] == "output.ipynb"]
    assert stored and stored[0]["stored"] is True and len(stored[0]["sha256"]) == 64


def test_f07_a_terminal_pod_without_any_result_is_incomplete_then_failed_never_a_synthesised_success(stack: Any, real_k8s: tuple[str, FakeKube]) -> None:
    """The review's reproduction: the pod is Succeeded but nothing ever carried the runner's result. The old poller fabricated ``{state: succeeded}``."""
    name, _ = real_k8s
    scripted = FakeKube(pending_polls=0, running_polls=0, log=["pod ran but the runner never reported"])
    stack.compute.providers.get(name)._kube = scripted
    put_setting(stack, "compute.remote_result_grace_seconds", 2)
    try:
        j = submit_on(stack, stack.workspace(), name, "print(1)")
        done = stack.wait_job(j["id"], timeout=60)
        assert done["state"] == "failed", done
        assert done["failure"]["details"]["code"] == "result_missing" and "never delivered its result" in done["failure"]["message"]
        assert "result_incomplete" in job_events(stack, j["id"])
    finally:
        put_setting(stack, "compute.remote_result_grace_seconds", 120)


def test_f07_a_result_missing_from_the_log_is_recovered_from_the_termination_record(stack: Any, real_k8s: tuple[str, FakeKube]) -> None:
    name, _ = real_k8s
    rec = {"v": 1, "job_id": None, "attempt": 1, "result": {"state": "succeeded", "exit_value": {"from": "termination-record"}, "failure": None, "artifacts": [], "partial": False}}
    scripted = FakeKube(pending_polls=0, running_polls=0, log=["the container log was rotated, the result line is gone"], termination_message=json.dumps(rec))
    stack.compute.providers.get(name)._kube = scripted
    done = stack.wait_job(submit_on(stack, stack.workspace(), name, "print(1)")["id"], timeout=60)
    assert done["state"] == "succeeded" and done["result"]["exit_value"] == {"from": "termination-record"}


# ================================================================================================ F08 log paging and drain
def numbered(n: int, pad: int = 0) -> list[str]:
    return [f"row {i:07d} " + "x" * pad for i in range(n)]


def read_all(p: KubernetesProvider, h: AppHandle, limit: int = 65536) -> tuple[list[str], list[str]]:
    lines: list[str] = []
    gaps: list[str] = []
    off, cur = 0, None
    for _ in range(100_000):
        pg = p.logs(h, off, limit, cursor=cur)
        lines += pg.data.splitlines()
        if pg.gap:
            gaps.append(pg.gap)
        off, cur = pg.next_offset, pg.cursor
        if pg.eof:
            break
    return lines, gaps


def test_f08_review_reproduction_a_bounded_window_made_the_old_line_offset_cursor_lose_a_line() -> None:
    """The evidence of the review replayed against the new reader: the window slides from [line1, line2] to [line2, line3]; the old 'lines consumed' cursor then
    returned nothing and ``line3`` was lost. The timestamp cursor continues exactly after the last line it consumed."""
    kube = FakeKube(pending_polls=0, running_polls=0, outcome="hang", log=[], coarse_clock=False)
    p = provider(kube)
    h = p.submit(SubmitRequest(job=JOB, spec=spec_for(), source_bytes=b"x"))
    p.status(h)
    pod = h.native["pod"]
    kube.append_log(pod, "line1\nline2")
    kube.log_retain_bytes = 2 * len("line1\n")
    page1 = p.logs(h, 0, 65536)
    assert page1.data.splitlines() == ["line1", "line2"] and page1.eof
    kube.append_log(pod, "line3")  # rotation drops line1
    page2 = p.logs(h, page1.next_offset, 65536, cursor=page1.cursor)
    assert page2.data.splitlines() == ["line3"] and page2.gap is None
    assert p.logs(h, page2.next_offset, 65536, cursor=page2.cursor).data == ""


def test_f08_lines_sharing_a_timestamp_are_neither_repeated_nor_skipped_at_any_page_size() -> None:
    kube = FakeKube(pending_polls=0, running_polls=0, outcome="hang", log=numbered(300), coarse_clock=True)  # ~300 lines in the same second
    p = provider(kube)
    h = p.submit(SubmitRequest(job=JOB, spec=spec_for(), source_bytes=b"x"))
    p.status(h)
    for limit in (1, 13, 50, 700, 1_000_000):
        lines, gaps = read_all(p, h, limit)
        assert lines == numbered(300) and not gaps, limit


def test_f08_new_lines_after_a_page_boundary_inside_one_timestamp_group_arrive_once() -> None:
    kube = FakeKube(pending_polls=0, running_polls=0, outcome="hang", log=numbered(10), coarse_clock=True)
    p = provider(kube)
    h = p.submit(SubmitRequest(job=JOB, spec=spec_for(), source_bytes=b"x"))
    p.status(h)
    pg = p.logs(h, 0, 5 * len(numbered(1)[0] + "\n"))
    assert pg.data.splitlines() == numbered(5) and not pg.eof
    kube.append_log(h.native["pod"], "\n".join(numbered(14)[10:]))
    rest, off, cur = [], pg.next_offset, pg.cursor
    for _ in range(50):
        nxt = p.logs(h, off, 1000, cursor=cur)
        rest += nxt.data.splitlines()
        off, cur = nxt.next_offset, nxt.cursor
        if nxt.eof:
            break
    assert rest == numbered(14)[5:]


def test_f08_a_burst_larger_than_the_read_window_inside_one_second_is_still_read_completely() -> None:
    kube = FakeKube(pending_polls=0, running_polls=0, outcome="hang", log=numbered(12_000, pad=100), coarse_clock=True)  # ~1.4 MiB within one second: the 320 KiB read window must grow
    p = provider(kube)
    h = p.submit(SubmitRequest(job=JOB, spec=spec_for(), source_bytes=b"x"))
    p.status(h)
    lines, gaps = read_all(p, h, limit=65536)
    assert lines == numbered(12_000, pad=100) and not gaps


def test_f08_rotation_between_polls_is_reported_as_an_explicit_gap_and_the_rest_is_complete() -> None:
    kube = FakeKube(pending_polls=0, running_polls=0, outcome="hang", log=numbered(5), log_retain_bytes=400)
    p = provider(kube)
    h = p.submit(SubmitRequest(job=JOB, spec=spec_for(), source_bytes=b"x"))
    p.status(h)
    pg = p.logs(h, 0, 65536)
    first = pg.data.splitlines()
    assert first and pg.eof
    time.sleep(1.1)  # the cursor's second ends: a later window cannot contain the anchor line any more
    kube.append_log(h.native["pod"], "\n".join(numbered(200)[5:]))  # rotates everything older than ~400 bytes away, including the anchor
    nxt = p.logs(h, pg.next_offset, 65536, cursor=pg.cursor)
    got = nxt.data.splitlines()
    assert nxt.gap and "no longer contains" in nxt.gap
    assert got == numbered(200)[-len(got):] and got[-1] == numbered(200)[-1]


# -------------------------------------------------------------------------------------------- F08 through the controller
def scripted_k8s(stack: Any, name: str, **kw: Any) -> FakeKube:
    kube = FakeKube(pending_polls=0, running_polls=0, **kw)
    stack.compute.providers.get(name)._kube = kube
    return kube


def job_log(stack: Any, job_id: str) -> str:
    """The complete stored job log (``Stack.logs`` stops after 100 pages of 64 KiB)."""
    out, off = "", 0
    for _ in range(2000):
        r = stack.http.get(f"/v1/jobs/{job_id}/logs", headers=stack.admin, params={"offset": off, "limit": 1_000_000})
        assert r.status_code == 200, r.text
        d = r.json()
        out += d["data"]
        if d.get("eof") or d["next_offset"] == off:
            break
        off = d["next_offset"]
    return out


def test_f08_pod_finished_before_the_first_poll_with_more_than_one_page_of_output_and_a_result_at_the_end(stack: Any, real_k8s: tuple[str, FakeKube]) -> None:
    name, _ = real_k8s
    lines = numbered(12_000, pad=70)  # ~1 MiB: four 256 KiB pages
    scripted_k8s(stack, name, log=lines, emit_result=True, result={"exit_value": {"tail": "reached"}})
    j = submit_on(stack, stack.workspace(), name, "print(1)")
    done = stack.wait_job(j["id"], timeout=90)
    assert done["state"] == "succeeded" and done["result"]["exit_value"] == {"tail": "reached"}
    got = [x for x in job_log(stack, j["id"]).splitlines() if x.startswith("row ")]
    assert got == lines, "the complete retained log arrived, in order, exactly once"


def test_f08_more_than_eight_mebibytes_of_output_is_drained_across_polls_without_loss_or_duplicates(stack: Any, real_k8s: tuple[str, FakeKube]) -> None:
    name, _ = real_k8s
    lines = numbered(110_000, pad=75)  # ~9.9 MiB
    kube = scripted_k8s(stack, name, log=lines, emit_result=True)
    j = submit_on(stack, stack.workspace(), name, "print(1)")
    done = stack.wait_job(j["id"], timeout=240)
    assert done["state"] == "succeeded", done
    got = [x for x in job_log(stack, j["id"]).splitlines() if x.startswith("row ")]
    assert len(got) == len(lines) and got == lines
    assert max(c["limit_bytes"] for c in kube.log_calls) < 1024 * 1024, "every read is bounded; a page never asks for the whole log"


def test_f08_a_controller_stop_between_writing_a_page_and_saving_the_cursor_neither_repeats_nor_loses_lines(stack: Any, real_k8s: tuple[str, FakeKube]) -> None:
    name, _ = real_k8s
    lines = numbered(6000, pad=70)
    kube = scripted_k8s(stack, name, log=lines, emit_result=True)
    jobs = stack.compute.jobs
    state = {"armed": True}
    real_append = jobs.logs.append

    def crashing_append(job_id: str, stream: str, seq: int | None, data: str) -> int:
        n = real_append(job_id, stream, seq, data)
        if state["armed"] and seq and seq >= 2 and "row " in data:
            state["armed"] = False
            kube.append_log(next(iter(kube._pods.values())).key[2], "\n".join(f"late {i}" for i in range(50)))  # the log grows while the controller is 'down'
            raise RuntimeError("controller stopped after the write, before saving the cursor")
        return n

    jobs.logs.append = crashing_append  # type: ignore[method-assign]
    try:
        j = submit_on(stack, stack.workspace(), name, "print(1)")
        done = stack.wait_job(j["id"], timeout=120)
    finally:
        jobs.logs.append = real_append  # type: ignore[method-assign]
    assert done["state"] == "succeeded" and not state["armed"]
    got = [x for x in job_log(stack, j["id"]).splitlines() if x.startswith(("row ", "late "))]
    assert got == [*lines, *[f"late {i}" for i in range(50)]]


def test_f08_output_rotated_away_before_the_first_read_leaves_the_complete_retained_tail(stack: Any, real_k8s: tuple[str, FakeKube]) -> None:
    """Without a cursor there is nothing to compare against, so lines that were rotated before the first read cannot be detected (the remote log simply starts
    later); what is retained arrives completely and once, with the result. The detectable case - rotation *between* reads - is tested above."""
    name, _ = real_k8s
    lines = numbered(12_000, pad=70)
    scripted_k8s(stack, name, log=lines, emit_result=True, log_retain_bytes=64 * 1024)
    j = submit_on(stack, stack.workspace(), name, "print(1)")
    done = stack.wait_job(j["id"], timeout=90)
    assert done["state"] == "succeeded"
    got = [x for x in job_log(stack, j["id"]).splitlines() if x.startswith("row ")]
    assert got and got == lines[-len(got):] and len(got) < len(lines)
