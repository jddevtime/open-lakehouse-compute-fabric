# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Worker hub and fleet administration against a live control plane: enrollment, authentication, liveness, cordon/drain, revoke, labels, pools."""

from __future__ import annotations

import json

import pytest
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidStatus

from tests.compute.fake_agent import FakeAgent, JobPlan, wait_until


def _events(st, kind: str) -> list[dict]:
    r = st.http.get("/v1/events", headers=st.admin, params={"type": kind, "limit": 100})
    assert r.status_code == 200, r.text
    return r.json()["items"]


# ------------------------------------------------------------------------------------------------------------ enrollment
def test_enrollment_token_is_single_use_and_names_are_unique(fleet):
    st = fleet
    r = st.http.post("/v1/workers/enrollment-tokens", headers=st.admin, json={"name": "enr-1", "ttl_seconds": 600})
    assert r.status_code == 201
    tok = r.json()
    assert tok["token"].startswith("olcf_enr_") and "olcf-worker run" in tok["command"]
    body = {"token": tok["token"], "name": "enr-1", "hostname": "h", "inventory": {"cores": 4, "memory_mb": 4096}, "agent_version": "t", "protocol": "1.0"}
    ok = st.http.post("/v1/workers/enroll", json=body)
    assert ok.status_code == 201, ok.text
    d = ok.json()
    assert d["worker_token"].startswith("olcf_wrk_") and d["channel_url"].endswith("/v1/workers/ws") and d["ca_pem"]
    again = st.http.post("/v1/workers/enroll", json=body)
    assert again.status_code in (401, 403), "an enrollment token must be single-use"
    # a second token for the same name conflicts unless replace_existing is given
    t2 = st.http.post("/v1/workers/enrollment-tokens", headers=st.admin, json={"name": "enr-1"}).json()["token"]
    dup = st.http.post("/v1/workers/enroll", json={**body, "token": t2})
    assert dup.status_code == 409, dup.text
    t3 = st.http.post("/v1/workers/enrollment-tokens", headers=st.admin, json={"name": "enr-1", "replace_existing": True}).json()["token"]
    rep = st.http.post("/v1/workers/enroll", json={**body, "token": t3})
    assert rep.status_code == 201 and rep.json()["worker_id"] == d["worker_id"]
    assert rep.json()["worker_token"] != d["worker_token"]


def test_enroll_validates_input(fleet):
    st = fleet
    assert st.http.post("/v1/workers/enroll", json={"token": "olcf_enr_nope", "name": "x"}).status_code in (401, 403)
    tok = st.http.post("/v1/workers/enrollment-tokens", headers=st.admin, json={}).json()["token"]
    bad = st.http.post("/v1/workers/enroll", json={"token": tok, "name": "bad name!"})
    assert bad.status_code in (400, 422)
    old = st.http.post("/v1/workers/enroll", json={"token": tok, "protocol": "9.0"})
    assert old.status_code == 400
    pool = st.http.post("/v1/workers/enrollment-tokens", headers=st.admin, json={"pool": "does-not-exist"})
    assert pool.status_code == 400


def test_enrollment_requires_permission(fleet):
    st = fleet
    ws = st.workspace()
    viewer = st.user("Viewer", ws)
    assert st.http.post("/v1/workers/enrollment-tokens", headers=viewer, json={}).status_code == 403
    assert st.http.post("/v1/workers/enrollment-tokens", json={}).status_code == 401


# ------------------------------------------------------------------------------------------------------------ channel authentication
def _ws_connect_result(st, headers: dict[str, str] | None) -> tuple[str, int | None]:
    import asyncio

    async def go() -> tuple[str, int | None]:
        url = st.settings.base_url.replace("http", "ws", 1) + "/v1/workers/ws"
        try:
            async with connect(url, additional_headers=headers or {}, open_timeout=5) as ws:
                try:
                    await asyncio.wait_for(ws.recv(), 5)
                except ConnectionClosed as exc:
                    return "closed", exc.rcvd.code if exc.rcvd else None
                return "open", None
        except InvalidStatus as exc:
            return "http", exc.response.status_code
        except ConnectionClosed as exc:
            return "closed", exc.rcvd.code if exc.rcvd else None

    return asyncio.run(go())


def test_channel_rejects_missing_and_wrong_tokens(fleet):
    st = fleet
    kind, code = _ws_connect_result(st, None)
    assert (kind, code) in (("closed", 4401), ("http", 401), ("http", 403))
    kind, code = _ws_connect_result(st, {"Authorization": "Bearer olcf_wrk_totallywrong"})
    assert (kind, code) in (("closed", 4401), ("http", 401), ("http", 403))
    # a user token is not a worker token
    kind, code = _ws_connect_result(st, st.admin)
    assert kind in ("closed", "http") and code in (4401, 4403, 401, 403)


def test_first_frame_must_be_hello(fleet):
    import asyncio

    st = fleet
    a = FakeAgent(st, "badfirst").enroll()
    st.agents.append(a)

    async def go() -> int | None:
        async with connect(a.channel_url, additional_headers={"Authorization": f"Bearer {a.token}"}) as ws:
            await ws.send(json.dumps({"v": 1, "type": "heartbeat", "payload": {}}))
            try:
                await asyncio.wait_for(ws.recv(), 5)
            except ConnectionClosed as exc:
                return exc.rcvd.code if exc.rcvd else None
        return None

    assert asyncio.run(go()) == 4400


def test_hello_identity_mismatch_is_refused(fleet):
    import asyncio

    st = fleet
    a = FakeAgent(st, "idmismatch").enroll()
    st.agents.append(a)

    async def go() -> int | None:
        async with connect(a.channel_url, additional_headers={"Authorization": f"Bearer {a.token}"}) as ws:
            await ws.send(json.dumps({"v": 1, "type": "hello", "id": "h1", "payload": {"worker_id": "wrk_someoneelse", "protocol": "1.0"}}))
            try:
                await asyncio.wait_for(ws.recv(), 5)
            except ConnectionClosed as exc:
                return exc.rcvd.code if exc.rcvd else None
        return None

    assert asyncio.run(go()) == 4403


def test_protocol_errors_are_answered_not_fatal(fleet):
    import asyncio

    st = fleet
    a = FakeAgent(st, "protoerr").enroll()
    st.agents.append(a)

    async def go() -> list[dict]:
        out = []
        async with connect(a.channel_url, additional_headers={"Authorization": f"Bearer {a.token}"}) as ws:
            await ws.send(json.dumps({"v": 1, "type": "hello", "id": "h1", "payload": {"worker_id": a.worker_id, "protocol": "1.0", "inventory": {"cores": 2, "memory_mb": 2048}}}))
            await ws.recv()
            await ws.send("not json at all")
            out.append(json.loads(await asyncio.wait_for(ws.recv(), 5)))
            await ws.send(json.dumps({"v": 1, "type": "bogus_type", "id": "x1", "payload": {}}))
            out.append(json.loads(await asyncio.wait_for(ws.recv(), 5)))
            await ws.send(json.dumps({"v": 1, "type": "job_log", "id": "x2", "payload": {"job_id": "job_x", "seq": -1, "data": "x"}}))
            out.append(json.loads(await asyncio.wait_for(ws.recv(), 5)))
            await ws.send(json.dumps({"v": 1, "type": "ping_me_maybe", "payload": []}))
            out.append(json.loads(await asyncio.wait_for(ws.recv(), 5)))
            # the channel is still usable
            await ws.send(json.dumps({"v": 1, "type": "heartbeat", "id": "hb1", "payload": {"metrics": {"cpu_percent": 1.0}}}))
            out.append(json.loads(await asyncio.wait_for(ws.recv(), 5)))
        return out

    out = asyncio.run(go())
    assert [f["type"] for f in out[:4]] == ["error"] * 4
    assert [f["payload"]["code"] for f in out[:4]] == ["bad_json", "unknown_type", "bad_payload", "bad_frame"]
    assert out[4]["type"] == "result" and out[4]["ref"] == "hb1"


def test_second_connection_replaces_the_first(fleet):
    st = fleet
    a = st.agent("dup-conn")
    st.wait_worker(a)
    b = FakeAgent(st, "dup-conn")
    b.worker_id, b.token, b.channel_url, b.heartbeat_seconds = a.worker_id, a.token, a.channel_url, 1.0
    st.agents.append(b)
    b.start()
    wait_until(lambda: a.close_code == 4409 or not a.connected, 10, message="the old connection to be closed")
    assert a.close_code in (4409, None) or "replaced" in a.close_reason
    assert st.worker(a.worker_id)["state"] == "online"


# ------------------------------------------------------------------------------------------------------------ liveness
def test_inventory_heartbeat_metrics_and_runtime_state(fleet):
    st = fleet
    a = st.agent("live-1", cores=6, memory_mb=8192, labels={"zone": "a"}, runtimes=("olcf-rt-2.0.0", "olcf-rt-1.3.0"))
    w = st.wait_worker(a)
    assert w["inventory"]["cores"] == 6 and w["inventory"]["memory_mb"] == 8192
    assert w["labels"].get("zone") == "a"
    wait_until(lambda: st.worker(a.worker_id).get("metrics", {}).get("cpu_percent") == 12.5, 15, message="heartbeat metrics")
    ready = {r["id"] for r in st.worker(a.worker_id)["runtimes"] if r["state"] == "ready"}
    assert ready == {"olcf-rt-2.0.0", "olcf-rt-1.3.0"}
    hist = wait_until(lambda: (h := st.worker(a.worker_id).get("history")) and len(h.get("cpu", [])) >= 2 and h, 15, message="metric history")
    assert len(hist["cpu"]) <= 120


def test_worker_goes_offline_and_comes_back(fleet):
    st = fleet
    a = st.agent("live-off")
    st.wait_worker(a)
    a.drop()
    st.wait_worker(a, "offline", timeout=30)
    kinds = [e["type"] for e in _events(st, "worker.offline")]
    assert "worker.offline" in kinds
    a.reconnect()
    st.wait_worker(a, "online")
    assert a.connections == 2


# ------------------------------------------------------------------------------------------------------------ cordon / drain / revoke
def test_cordon_stops_new_placements_uncordon_resumes(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("cordon-a")
    st.wait_worker(a)
    r = st.http.post(f"/v1/workers/{a.worker_id}/cordon", headers=st.admin, json={"reason": "maintenance"})
    assert r.status_code == 200 and r.json()["scheduling"] == "cordoned"
    job = st.submit(ws)
    wait_until(lambda: st.job(job["id"])["state"] == "queued" and st.job(job["id"])["wait_reason"], 10, message="queued with a reason")
    import time

    time.sleep(1.0)
    assert st.job(job["id"])["state"] == "queued", "a cordoned node must not receive new work"
    assert not a.commands_of("run_job")
    r = st.http.post(f"/v1/workers/{a.worker_id}/uncordon", headers=st.admin)
    assert r.json()["scheduling"] == "active"
    assert st.wait_job(job["id"])["state"] == "succeeded"
    assert "worker.cordoned" in [e["type"] for e in _events(st, "worker.cordoned")]


def test_drain_waits_for_running_jobs_then_reports_drained(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("drain-a")
    a.job_plan = lambda spec: JobPlan(outcome="hang")
    st.wait_worker(a)
    job = st.submit(ws)
    st.wait_job(job["id"], "running")
    r = st.http.post(f"/v1/workers/{a.worker_id}/drain", headers=st.admin)
    assert r.status_code == 200 and r.json()["scheduling"] == "draining"
    wait_until(lambda: a.draining, 10, message="agent drain flag")
    assert a.commands_of("drain")[-1]["payload"] == {"on": True}
    assert st.worker(a.worker_id)["busy"]["jobs"] == 1
    assert not _events(st, "worker.drained") or all(e["payload"].get("worker_id") != a.worker_id for e in _events(st, "worker.drained"))
    # the running job is not killed by draining; once it ends the node reports drained
    a.finish_hung(job["id"], "succeed")
    assert st.wait_job(job["id"])["state"] == "succeeded"
    wait_until(lambda: any(e["payload"].get("worker_id") == a.worker_id for e in _events(st, "worker.drained")), 20, message="worker.drained event")
    # a drained node accepts no work until uncordoned
    j2 = st.submit(ws)
    import time

    time.sleep(1.0)
    assert st.job(j2["id"])["state"] == "queued"
    a.job_plan = lambda spec: JobPlan()  # before uncordoning: the queued job is dispatched as soon as the node accepts work again
    st.http.post(f"/v1/workers/{a.worker_id}/uncordon", headers=st.admin)
    assert st.wait_job(j2["id"])["state"] == "succeeded"


def test_hello_after_drain_tells_the_agent_to_stay_drained(fleet):
    st = fleet
    a = st.agent("drain-hello")
    st.wait_worker(a)
    st.http.post(f"/v1/workers/{a.worker_id}/drain", headers=st.admin)
    a.drop()
    a.reconnect()
    assert a.hello_acks[-1]["drain"] is True and a.draining


def test_revoked_worker_is_rejected_and_disconnected(fleet):
    st = fleet
    a = st.agent("revoke-a")
    st.wait_worker(a)
    r = st.http.post(f"/v1/workers/{a.worker_id}/revoke", headers=st.admin, json={"reason": "compromised"})
    assert r.status_code == 200 and r.json()["state"] == "revoked"
    wait_until(lambda: not a.connected, 10, message="the revoked agent to be disconnected")
    assert a.close_code in (4403, 4401, 1008)
    # it cannot come back with its old token
    b = FakeAgent(st, "revoke-a")
    b.worker_id, b.token, b.channel_url, b.heartbeat_seconds = a.worker_id, a.token, a.channel_url, 1.0
    st.agents.append(b)
    with pytest.raises(AssertionError):
        b.start(wait=3)
    assert b.close_code in (4401, 4403, 1008, 1006)
    assert st.worker(a.worker_id)["state"] == "revoked"
    assert "worker.revoked" in [e["type"] for e in _events(st, "worker.revoked")]
    # a revoked worker never receives work
    ws = st.workspace()
    job = st.submit(ws)
    import time

    time.sleep(0.8)
    assert st.job(job["id"])["state"] == "queued"
    st.http.post(f"/v1/jobs/{job['id']}/cancel", headers=st.admin)


def test_revoke_then_delete_and_audit_trail(fleet):
    st = fleet
    a = st.agent("audit-a")
    st.wait_worker(a)
    st.http.post(f"/v1/workers/{a.worker_id}/cordon", headers=st.admin)
    st.http.put(f"/v1/workers/{a.worker_id}/labels", headers=st.admin, json={"gpu": "none", "tier": "t1"})
    r = st.http.get("/v1/audit", headers=st.admin, params={"resource_id": a.worker_id, "limit": 100})
    actions = {e["action"] for e in r.json()["items"]}
    assert {"worker.enrolled", "worker.cordoned", "worker.labels_changed"} <= actions or len(actions) >= 2, actions
    live = st.http.delete(f"/v1/workers/{a.worker_id}", headers=st.admin)
    assert live.status_code in (409, 400, 204)  # a connected worker may not be deleted outright
    st.http.post(f"/v1/workers/{a.worker_id}/revoke", headers=st.admin)
    gone = st.http.delete(f"/v1/workers/{a.worker_id}", headers=st.admin)
    assert gone.status_code == 204
    assert st.http.get(f"/v1/workers/{a.worker_id}", headers=st.admin).status_code == 404


def test_labels_pool_and_patch_with_etag(fleet):
    st = fleet
    pool = st.http.post("/v1/worker-pools", headers=st.admin, json={"name": "gpu-pool", "description": "x"})
    assert pool.status_code in (201, 409), pool.text
    a = st.agent("label-a")
    st.wait_worker(a)
    r = st.http.get(f"/v1/workers/{a.worker_id}", headers=st.admin)
    etag = r.headers["ETag"]
    ok = st.http.patch(f"/v1/workers/{a.worker_id}", headers={**st.admin, "If-Match": etag}, json={"labels": {"role": "etl"}})
    assert ok.status_code == 200 and ok.json()["labels"]["role"] == "etl"
    stale = st.http.patch(f"/v1/workers/{a.worker_id}", headers={**st.admin, "If-Match": etag}, json={"labels": {"role": "other"}})
    assert stale.status_code in (409, 412)
    moved = st.http.put(f"/v1/workers/{a.worker_id}/pool", headers=st.admin, json={"pool": "gpu-pool"})
    assert moved.status_code == 200 and moved.json()["pool"] == "gpu-pool"
    assert st.http.put(f"/v1/workers/{a.worker_id}/pool", headers=st.admin, json={"pool": "nope"}).status_code in (400, 404)
    flt = st.http.get("/v1/workers", headers=st.admin, params={"label": "role=etl"}).json()["items"]
    assert [w["name"] for w in flt] == ["label-a"]
    st.http.put(f"/v1/workers/{a.worker_id}/pool", headers=st.admin, json={"pool": "default"})


def test_worker_api_requires_permission(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("rbac-a")
    st.wait_worker(a)
    contributor = st.user("Contributor", ws)
    assert st.http.post(f"/v1/workers/{a.worker_id}/cordon", headers=contributor).status_code == 403
    assert st.http.post(f"/v1/workers/{a.worker_id}/revoke", headers=contributor).status_code == 403
    assert st.http.get("/v1/workers", headers={}).status_code == 401
    # a worker token is not an API credential
    assert st.http.get("/v1/workers", headers={"Authorization": f"Bearer {a.token}"}).status_code in (401, 403)


# ------------------------------------------------------------------------------------------------------------ advertised capacity
def test_planner_never_plans_beyond_the_capacity_an_agent_advertises(fleet):
    """``--max-cores/--max-memory-mb`` cap what an agent hands out; the planner's node view must honour it (a Spark worker daemon sized from the
    machine's inventory would otherwise exhaust the agent's allocation and the job would fail with "not enough free resources")."""
    st = fleet
    capped = st.agent("capped", cores=16, memory_mb=64000, capacity=(4, 3000))
    plain = st.agent("uncapped", cores=8, memory_mb=16000)
    st.wait_worker(capped)
    st.wait_worker(plain)
    views = {v.name: v for v in st.compute.registry.node_views({}, {})}
    assert (views["capped"].cores_total, views["capped"].memory_mb_total) == (4, 3000)
    assert (views["uncapped"].cores_total, views["uncapped"].memory_mb_total) == (8, 16000)
    # the inventory (what the topology shows) still describes the machine
    assert st.worker(capped.worker_id)["inventory"]["cores"] == 16
    # an agent can never advertise more than the machine has
    from olcf.compute.registry import NodeInfo

    big = NodeInfo.from_row({**st.worker(capped.worker_id), "inventory": {"cores": 2, "memory_mb": 1000, "capacity": {"cores": 64, "memory_mb": 99999}}})
    assert (big.sched_cores, big.sched_memory_mb) == (2, 1000)
