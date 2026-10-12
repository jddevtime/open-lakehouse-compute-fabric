# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Notebook sessions (CONTRACTS 4.4) and ``run_spark_sql`` against fake kernels: REST, long-poll, the browser WebSocket relay, replay, culling, authz."""

from __future__ import annotations

import asyncio
import json
import time

import pytest
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from tests.compute.fake_agent import wait_until


def _set(st, key: str, value) -> None:
    assert st.http.put(f"/v1/admin/settings/{key}", headers=st.admin, json={"value": value}).status_code == 200


def _create(st, ws, headers=None, *, expect=201, **body):
    r = st.http.post("/v1/sessions", headers=headers or st.admin, json={"workspace_id": ws, "engine": "python", **body})
    assert r.status_code == expect, r.text
    return r.json()


def _wait_state(st, sid, state="idle", headers=None, timeout=20):
    return wait_until(lambda: (s := st.http.get(f"/v1/sessions/{sid}", headers=headers or st.admin).json())["state"] == state and s, timeout, 0.1, f"session {sid} to be {state}")


def _events(st, sid, after=0, wait=0.0, headers=None, replay=False):
    r = st.http.get(f"/v1/sessions/{sid}/events", headers=headers or st.admin, params={"after_seq": after, "wait": wait, "replay": replay}, timeout=40)
    assert r.status_code == 200, r.text
    return r.json()


def _wait_reply(st, sid, cell, after=0, timeout=20):
    seen: list[dict] = []
    pos = after

    def poll():
        nonlocal pos
        d = _events(st, sid, pos, wait=1.0)
        pos = d["next_seq"]
        seen.extend(d["items"])
        return any(m["type"] == "reply" and m.get("cell_id") == cell for m in seen)

    wait_until(poll, timeout, 0.0, f"reply of cell {cell}")
    return [m for m in seen if m.get("cell_id") == cell], pos


def _ws(st, sid, ops=(), until=lambda msgs: False, headers=None, timeout=20, close_ok=False, on_message=None):
    async def go():
        msgs: list[dict] = []
        h = headers or st.admin
        url = st.settings.base_url.replace("http", "ws", 1) + f"/v1/sessions/{sid}/ws"
        async with connect(url, additional_headers=h, open_timeout=10) as ws:
            sent = False
            try:
                end = time.monotonic() + timeout
                while time.monotonic() < end:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), 1.0)
                    except TimeoutError:
                        continue
                    msgs.append(json.loads(raw))
                    if on_message is not None:
                        on_message(msgs[-1])
                    if not sent and msgs[-1]["type"] == "replay_done":
                        for op in ops:
                            await ws.send(json.dumps(op))
                        sent = True
                    if sent and until(msgs):
                        break
            except ConnectionClosed as exc:
                if not close_ok:
                    raise
                msgs.append({"type": "__closed__", "code": exc.rcvd.code if exc.rcvd else None})
        return msgs

    return asyncio.run(go())


# ------------------------------------------------------------------------------------------------------------ lifecycle
def test_create_session_starts_a_kernel_on_a_node(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("ses-a")
    st.wait_worker(a)
    s = _create(st, ws, name="my session")
    assert s["id"].startswith("ses_") and s["state"] in ("starting", "idle") and s["ws_path"].endswith(f"{s['id']}/ws")
    s = _wait_state(st, s["id"])
    assert s["worker_id"] == a.worker_id and s["runtime_id"] == "olcf-rt-2.0.0" and s["engine"] == "python"
    (cmd,) = a.commands_of("start_kernel")
    p = cmd["payload"]
    assert p["session_id"] == s["id"] and p["spec"]["schema"] == "olcf.jobspec/1" and p["runtime_id"] == "olcf-rt-2.0.0"
    assert p["resources"]["cores"] >= 1 and p["idle_timeout_seconds"] > 0 and p["eager_spark"] is False
    assert s["id"] in a.kernels
    lst = st.http.get("/v1/sessions", headers=st.admin, params={"workspace_id": ws}).json()["items"]
    assert [x["id"] for x in lst] == [s["id"]]


def test_execute_rest_events_longpoll_and_execution_count(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("ses-exec")
    st.wait_worker(a)
    s = _create(st, ws)
    _wait_state(st, s["id"])
    r = st.http.post(f"/v1/sessions/{s['id']}/execute", headers=st.admin, json={"cell_id": "c1", "code": "x = 1"})
    assert r.status_code == 202
    msg_id = r.json()["msg_id"]
    assert msg_id.startswith("c1~") and a.commands_of("kernel_execute")[0]["payload"]["msg_id"] == msg_id
    msgs, pos = _wait_reply(st, s["id"], "c1")
    types = [m["type"] for m in msgs]
    assert "output" in types and types[-1] == "reply"
    assert any(m["type"] == "output" and m["output"]["output_type"] == "stream" and "out:x = 1" in m["output"]["text"] for m in msgs)
    assert any(m["type"] == "output" and m["output"]["output_type"] == "execute_result" for m in msgs)
    st.http.post(f"/v1/sessions/{s['id']}/execute", headers=st.admin, json={"cell_id": "c2", "code": "y"})
    _wait_reply(st, s["id"], "c2", after=pos)
    assert _wait_state(st, s["id"])["execution_count"] == 2
    bad = st.http.post(f"/v1/sessions/{s['id']}/execute", headers=st.admin, json={"cell_id": "bad cell!", "code": "1"})
    assert bad.status_code == 400
    # errors travel as nbformat error outputs followed by an error reply
    st.http.post(f"/v1/sessions/{s['id']}/execute", headers=st.admin, json={"cell_id": "c3", "code": "raise ValueError"})
    msgs, _ = _wait_reply(st, s["id"], "c3", after=pos)
    assert any(m["type"] == "output" and m["output"]["output_type"] == "error" and m["output"]["ename"] == "ValueError" for m in msgs)
    assert [m for m in msgs if m["type"] == "reply"][0]["status"] == "error"


def test_websocket_relay_replay_and_multiple_browsers(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("ses-ws")
    st.wait_worker(a)
    s = _create(st, ws)
    _wait_state(st, s["id"])
    sid = s["id"]
    msgs = _ws(st, sid, ops=[{"op": "execute", "cell_id": "w1", "code": "print(1)"}, {"op": "ping"}],
               until=lambda m: any(x["type"] == "reply" for x in m) and any(x["type"] == "pong" for x in m))
    kinds = [m["type"] for m in msgs]
    assert kinds[0] == "status" and "replay_done" in kinds
    assert kinds.index("replay_done") < kinds.index("output") < kinds.index("reply")
    # a second browser attaching later gets the outputs replayed
    again = _ws(st, sid, until=lambda m: any(x["type"] == "replay_done" for x in m))
    rep = [m for m in again if m.get("replay")]
    assert any(m["type"] == "output" and m["cell_id"] == "w1" for m in rep) and any(m["type"] == "reply" for m in rep)
    # operations over the socket: complete, interrupt, restart, bad ops are answered, not fatal
    msgs = _ws(st, sid, ops=[{"op": "complete", "code": "pri", "cursor": 3, "req": "r1"}, {"op": "bogus"}, {"op": "execute", "cell_id": "bad cell", "code": "1"}],
               until=lambda m: sum(1 for x in m if x["type"] in ("complete", "error")) >= 3)
    comp = next(m for m in msgs if m["type"] == "complete")
    assert comp["req"] == "r1" and "print" in comp["matches"]
    assert [m["code"] for m in msgs if m["type"] == "error"][:2] and all(m["type"] != "__closed__" for m in msgs)


def test_replay_is_capped_at_500_outputs(fleet):
    st = fleet
    ws = st.workspace()
    st.agent("ses-cap")
    s = _create(st, ws)
    _wait_state(st, s["id"])
    sessions = st.compute.sessions
    for i in range(650):
        sessions.append(s["id"], "output", "bulk", {"output": {"output_type": "stream", "name": "stdout", "text": f"line {i}\n"}})
    msgs = _ws(st, s["id"], until=lambda m: any(x["type"] == "replay_done" for x in m))
    rep = [m for m in msgs if m["type"] == "output" and m.get("replay")]
    assert len(rep) == 500
    assert rep[-1]["output"]["text"] == "line 649\n" and rep[0]["output"]["text"] == "line 150\n"
    d = _events(st, s["id"], replay=True)
    assert len([m for m in d["items"] if m["type"] == "output"]) == 500


def test_interrupt_restart_and_complete(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("ses-int")
    st.wait_worker(a)
    s = _create(st, ws)
    sid = s["id"]
    _wait_state(st, sid)
    st.http.post(f"/v1/sessions/{sid}/execute", headers=st.admin, json={"cell_id": "long", "code": "sleep-forever"})
    wait_until(lambda: _wait_state(st, sid, "busy", timeout=5) if False else st.http.get(f"/v1/sessions/{sid}", headers=st.admin).json()["state"] == "busy", 10, message="busy")
    assert st.http.post(f"/v1/sessions/{sid}/interrupt", headers=st.admin).status_code == 200
    msgs, _ = _wait_reply(st, sid, "long")
    assert any(m["type"] == "output" and m["output"].get("ename") == "KeyboardInterrupt" for m in msgs)
    assert a.commands_of("kernel_interrupt")
    _wait_state(st, sid, "idle")
    r = st.http.post(f"/v1/sessions/{sid}/restart", headers=st.admin)
    assert r.status_code == 200
    _wait_state(st, sid, "idle")
    assert a.commands_of("kernel_restart")
    c = st.http.post(f"/v1/sessions/{sid}/complete", headers=st.admin, json={"code": "pri", "cursor": 3})
    assert c.status_code == 200 and "print" in c.json()["matches"]


def test_delete_shuts_the_kernel_down_and_ends_the_socket(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("ses-del")
    st.wait_worker(a)
    sid = _create(st, ws)["id"]
    _wait_state(st, sid)
    done = {}

    import threading

    subscribed = threading.Event()

    def watch():
        done["msgs"] = _ws(st, sid, until=lambda m: False, timeout=20, close_ok=True, on_message=lambda m: m["type"] == "replay_done" and subscribed.set())

    t = threading.Thread(target=watch)
    t.start()
    assert subscribed.wait(30), "the socket never reached replay_done"  # deleting before the watcher is subscribed would race its status messages
    r = st.http.delete(f"/v1/sessions/{sid}", headers=st.admin)
    assert r.status_code == 200, r.text
    t.join(25)
    msgs = done["msgs"]
    assert any(m["type"] == "status" and m["state"] == "dead" for m in msgs)
    assert msgs[-1] == {"type": "__closed__", "code": 4410}
    wait_until(lambda: a.commands_of("kernel_shutdown"), 10, message="kernel_shutdown")
    assert sid not in a.kernels
    assert st.http.get(f"/v1/sessions/{sid}", headers=st.admin).json()["state"] == "dead"
    assert st.http.post(f"/v1/sessions/{sid}/execute", headers=st.admin, json={"cell_id": "c", "code": "1"}).status_code == 409


# ------------------------------------------------------------------------------------------------------------ authorisation
def test_session_authorization(fleet):
    st = fleet
    ws, other_ws = st.workspace(), st.workspace()
    st.agent("ses-auth")
    owner = st.user("Contributor", ws)
    mate = st.user("Contributor", ws)
    outsider = st.user("Contributor", other_ws)
    viewer = st.user("Viewer", ws)
    assert _create(st, ws, viewer, expect=403)
    sid = _create(st, ws, owner)["id"]
    _wait_state(st, sid, headers=owner)
    for who in (mate, outsider):  # sessions are private to their owner (and administrators)
        assert st.http.get(f"/v1/sessions/{sid}", headers=who).status_code in (403, 404)
        assert st.http.post(f"/v1/sessions/{sid}/execute", headers=who, json={"cell_id": "c", "code": "1"}).status_code in (403, 404)
        assert st.http.delete(f"/v1/sessions/{sid}", headers=who).status_code in (403, 404)
        assert st.http.get(f"/v1/sessions/{sid}/events", headers=who).status_code in (403, 404)
        assert sid not in [x["id"] for x in st.http.get("/v1/sessions", headers=who).json()["items"]]
    assert _create(st, ws, outsider, expect=403)
    assert st.http.get(f"/v1/sessions/{sid}", headers=st.admin).status_code == 200
    # WebSocket authentication
    for h, code in ((None, 4401), ({"Authorization": "Bearer olcf_pat_bogus"}, 4401), (mate, 4404), (outsider, 4404)):
        async def go(h=h):
            url = st.settings.base_url.replace("http", "ws", 1) + f"/v1/sessions/{sid}/ws"
            try:
                async with connect(url, additional_headers=h or {}, open_timeout=10) as w:
                    await asyncio.wait_for(w.recv(), 5)
            except ConnectionClosed as exc:
                return exc.rcvd.code if exc.rcvd else None
            return None

        assert asyncio.run(go()) in (code, 4403), h
    st.http.delete(f"/v1/sessions/{sid}", headers=owner)


def test_session_limits_and_placement_errors(fleet):
    st = fleet
    ws = st.workspace()
    r = st.http.post("/v1/sessions", headers=st.admin, json={"workspace_id": ws, "engine": "python"})
    assert r.status_code in (503, 409) and (r.json().get("remediation") or r.json().get("error", {}).get("remediation")), r.text  # no node at all
    st.agent("ses-lim")
    user = st.user("Contributor", ws)
    _set(st, "compute.max_sessions_per_user", 2)
    try:
        ids = [_create(st, ws, user)["id"] for _ in range(2)]
        r = st.http.post("/v1/sessions", headers=user, json={"workspace_id": ws, "engine": "python"})
        assert r.status_code in (409, 429), r.text
        for i in ids:
            st.http.delete(f"/v1/sessions/{i}", headers=user)
    finally:
        _set(st, "compute.max_sessions_per_user", 5)
    too_big = st.http.post("/v1/sessions", headers=st.admin, json={"workspace_id": ws, "engine": "python", "resources": {"driver_cores": 64}})
    assert too_big.status_code in (400, 409, 422, 503)


# ------------------------------------------------------------------------------------------------------------ culling / failures
def test_idle_sessions_are_culled(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("ses-cull")
    st.wait_worker(a)
    _set(st, "compute.session_idle_timeout_seconds", 2)
    try:
        sid = _create(st, ws)["id"]
        _wait_state(st, sid)
        s = _wait_state(st, sid, "dead", timeout=40)
        assert "idle" in (s["state_reason"] or "")
        wait_until(lambda: a.commands_of("kernel_shutdown"), 10, message="kernel_shutdown after culling")
    finally:
        _set(st, "compute.session_idle_timeout_seconds", 3600)


def test_kernel_lost_when_the_agent_restarts_without_it(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("ses-lost")
    st.wait_worker(a)
    sid = _create(st, ws)["id"]
    _wait_state(st, sid)
    a.drop()
    a.kernels.clear()  # the agent process restarted: kernels did not survive
    a.reconnect()
    s = _wait_state(st, sid, "dead", timeout=30)
    assert "kernel" in (s["state_reason"] or "").lower()


def test_session_survives_a_reconnect_when_the_kernel_is_still_there(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("ses-keep")
    st.wait_worker(a)
    sid = _create(st, ws)["id"]
    _wait_state(st, sid)
    wait_until(lambda: a.commands_of("start_kernel") and a.kernels.get(sid), 10, 0.05, "the agent to hold the kernel")
    a.drop()
    a.reconnect()
    time.sleep(1.5)
    cur = st.http.get(f"/v1/sessions/{sid}", headers=st.admin).json()
    assert cur["state"] == "idle", (cur["state"], cur.get("state_reason"))
    r = st.http.post(f"/v1/sessions/{sid}/execute", headers=st.admin, json={"cell_id": "after", "code": "1"})
    assert r.status_code == 202, r.text
    _wait_reply(st, sid, "after")


def test_session_whose_start_result_is_lost_with_the_connection_keeps_its_running_kernel(fleet):
    """FINALQA-1: the agent reports ``idle`` (kernel ready) and *then* answers ``start_kernel``; a connection that dies between the two used to fail
    the start request and mark a healthy, already-idle session dead ("the kernel could not be started: worker ... disconnected")."""
    st = fleet
    ws = st.workspace()
    a = st.agent("ses-lost-result")
    st.wait_worker(a)
    a.lose_result_of.add("start_kernel")
    sid = _create(st, ws)["id"]
    _wait_state(st, sid)                                                # idle, from the kernel's own event
    wait_until(lambda: not a.connected, 10, 0.05, "the agent connection to be cut")
    a.reconnect()
    for _ in range(20):                                                 # 2 s: neither the failed request nor the reconcile may end it
        assert (cur := st.http.get(f"/v1/sessions/{sid}", headers=st.admin).json())["state"] == "idle", cur
        time.sleep(0.1)
    assert a.commands_of("kernel_shutdown") == []
    r = st.http.post(f"/v1/sessions/{sid}/execute", headers=st.admin, json={"cell_id": "after", "code": "1"})
    assert r.status_code == 202, r.text
    _wait_reply(st, sid, "after")


def test_session_dies_when_its_node_is_revoked(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("ses-revoked")
    st.wait_worker(a)
    sid = _create(st, ws)["id"]
    _wait_state(st, sid)
    st.http.post(f"/v1/workers/{a.worker_id}/revoke", headers=st.admin)
    s = _wait_state(st, sid, "dead", timeout=40)
    assert s["state_reason"]


def test_agent_refusing_start_kernel_marks_the_session_dead(fleet):
    st = fleet
    ws = st.workspace()
    a = st.agent("ses-refuse")
    a.fail_commands["start_kernel"] = "ipykernel is not installed"
    st.wait_worker(a)
    sid = _create(st, ws)["id"]
    s = _wait_state(st, sid, "dead")
    assert "ipykernel" in s["state_reason"]


# ------------------------------------------------------------------------------------------------------------ run_spark_sql
@pytest.fixture()
def principals(fleet):
    from olcf.api.deps import authenticate

    st = fleet

    def make(headers):
        return authenticate(st.ctx, headers["Authorization"].split()[1], ip="127.0.0.1")

    return make


def test_run_spark_sql_uses_one_pooled_hidden_session(fleet, principals):
    from olcf.core.errors import BadRequest, Forbidden

    st = fleet
    ws = st.workspace()
    a = st.agent("sql-a")
    a.sql_results["select * from t"] = {"columns": [{"name": "id", "type": "int"}, {"name": "name", "type": "string"}], "rows": [[i, f"n{i}"] for i in range(10)]}
    a.sql_results["select 1"] = {"columns": [{"name": "1", "type": "int"}], "rows": [[1]]}
    a.kernel_fail_code.add("select * from missing")
    st.wait_worker(a)
    admin = principals(st.admin)
    res = st.compute.run_spark_sql(admin, ws, "select * from t", limit=1000, timeout=30)
    assert res["columns"] == [{"name": "id", "type": "int"}, {"name": "name", "type": "string"}] and res["row_count"] == 10 and res["truncated"] is False
    assert res["application_id"] and res["session_id"].startswith("ses_")
    res2 = st.compute.run_spark_sql(admin, ws, "select 1", timeout=30)
    assert res2["rows"] == [[1]] and res2["session_id"] == res["session_id"], "the hidden session must be reused"
    assert len(a.commands_of("start_kernel")) == 1
    start = a.commands_of("start_kernel")[0]["payload"]
    assert start["eager_spark"] is True
    # limit truncation
    cut = st.compute.run_spark_sql(admin, ws, "select * from t", limit=4, timeout=30)
    assert cut["row_count"] == 4 and cut["truncated"] is True and cut["rows"][-1] == [3, "n3"]
    # the SQL text never reaches the kernel as code, only as a JSON string literal
    code = a.commands_of("kernel_execute")[0]["payload"]["code"]
    assert "spark.sql(_olcf_json.loads(" in code
    # errors are surfaced as 400 with the Spark message
    with pytest.raises(BadRequest) as ei:
        st.compute.run_spark_sql(admin, ws, "select * from missing", timeout=30)
    assert "AnalysisException" in ei.value.message
    # the pooled session is hidden from normal listings
    assert res["session_id"] not in [x["id"] for x in st.http.get("/v1/sessions", headers=st.admin).json()["items"]]
    assert res["session_id"] in [x["id"] for x in st.http.get("/v1/sessions", headers=st.admin, params={"include_hidden": True, "mine": False, "workspace_id": ws}).json()["items"]]
    # authorisation: viewers may read, but not write
    viewer = principals(st.user("Viewer", ws))
    assert st.compute.run_spark_sql(viewer, ws, "select 1", timeout=30)["rows"] == [[1]]
    with pytest.raises(Forbidden):
        st.compute.run_spark_sql(viewer, ws, "insert into t values (1)", timeout=30)
    outsider = principals(st.user("Contributor", st.workspace()))
    with pytest.raises(Forbidden):
        st.compute.run_spark_sql(outsider, ws, "select 1", timeout=30)
    with pytest.raises(BadRequest):
        st.compute.run_spark_sql(admin, ws, "   ", timeout=30)


def test_run_spark_sql_times_out_and_interrupts(fleet, principals):
    from olcf.core.errors import Unavailable

    st = fleet
    ws = st.workspace()
    a = st.agent("sql-slow")
    st.wait_worker(a)
    admin = principals(st.admin)
    a.fail_commands["kernel_execute"] = "kernel busy"
    a.fail_times["kernel_execute"] = 0  # accepted below; the cell never replies because we hold it
    a.fail_commands.clear()
    st.compute.run_spark_sql(admin, ws, "select 1", timeout=30)
    a.command_delay["kernel_execute"] = 0.0
    original = a._cmd_kernel_execute

    async def never_replies(p):
        return {"msg_id": p["msg_id"]}

    a._cmd_kernel_execute = never_replies  # type: ignore[method-assign]
    try:
        with pytest.raises(Unavailable):
            st.compute.run_spark_sql(admin, ws, "select 2", timeout=2)
        assert a.commands_of("kernel_interrupt"), "a timed-out statement must interrupt the kernel"
    finally:
        a._cmd_kernel_execute = original  # type: ignore[method-assign]
