# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Spec 35.4: long-lived server-sent-event streams (``/v1/events/stream``, ``/v1/jobs/{id}/logs/stream``) against a real uvicorn server.

A stream is authenticated once when it opens - and then lives for hours. These tests check that it (1) needs credentials, (2) only carries what the principal may see,
(3) stops when the account is disabled, its role is withdrawn or the token expires or is revoked.
"""

from __future__ import annotations

import json
import threading
import time

import httpx
import pytest

from tests.security.liveserver import LiveServer
from tests.security.secenv import CANARY_A, CANARY_B, build_env

REVALIDATE_WITHIN = 25.0  # seconds a stream may outlive a revoked credential (the server re-validates every ~10 s)


@pytest.fixture(scope="module")
def live(tmp_path_factory):
    from olcf.api.app import create_app
    from olcf.core.config import load_settings

    s = load_settings(tmp_path_factory.mktemp("streams") / "data", environment="test")
    s.compute.embedded_worker = False
    app = create_app(s)
    with LiveServer(app) as srv:
        with httpx.Client(base_url=srv.base_url, timeout=30, trust_env=False) as c:
            env = build_env(c, app.state.ctx)
            env.live = srv  # type: ignore[attr-defined]
            yield env


class Reader(threading.Thread):
    """Reads an SSE stream in the background; ``events`` collects (event, data) tuples, ``status`` the HTTP status, ``ended`` is set when the server closed it."""

    def __init__(self, base_url: str, path: str, headers: dict[str, str] | None = None, params: dict[str, str] | None = None) -> None:
        super().__init__(daemon=True)
        self.base_url, self.path, self.headers, self.params = base_url, path, headers or {}, params or {}
        self.events: list[tuple[str, str]] = []
        self.status: int | None = None
        self.ended = threading.Event()
        self.opened = threading.Event()
        self.stop = threading.Event()
        self.error: str | None = None

    def run(self) -> None:
        try:
            with httpx.Client(base_url=self.base_url, timeout=httpx.Timeout(5, read=60), trust_env=False) as c:
                with c.stream("GET", self.path, headers=self.headers, params=self.params) as r:
                    self.status = r.status_code
                    self.opened.set()
                    if r.status_code != 200:
                        r.read()
                        self.events.append(("http-error", r.text))
                        return
                    event = "message"
                    for line in r.iter_lines():
                        if self.stop.is_set():
                            return
                        if line.startswith("event:"):
                            event = line[6:].strip()
                        elif line.startswith("data:"):
                            self.events.append((event, line[5:].strip()))
                            event = "message"
        except Exception as exc:  # noqa: BLE001
            self.error = f"{type(exc).__name__}: {exc}"
        finally:
            self.opened.set()
            self.ended.set()

    def text(self) -> str:
        return "\n".join(d for _e, d in self.events)


def open_stream(env, path, headers=None, params=None, wait=True) -> Reader:
    rd = Reader(env.live.base_url, path, headers, params)
    rd.start()
    if wait:
        assert rd.opened.wait(15), "the stream did not open"
        time.sleep(0.7)  # let the generator register its subscription / read its head sequence
    return rd


def wait_for(cond, timeout: float, step: float = 0.2) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(step)
    return cond()


def emit(env, ws: str, name: str) -> None:
    r = env.c.post(f"/v1/workspaces/{ws}/items", json={"type": "variable_set", "name": name, "definition": {"variables": []}}, headers=env.admin)
    assert r.status_code == 201, r.text


def test_streams_require_credentials(live):
    env = live
    for path in ("/v1/events/stream", f"/v1/jobs/{env.tenants['A'].ids['job_id']}/logs/stream"):
        for headers, params in (({}, {}), ({"Authorization": "Bearer nope"}, {}), ({}, {"access_token": "nope"}), ({}, {"access_token": "eyJhbGciOiJub25lIn0.e30."})):  # gitleaks:allow
            rd = open_stream(env, path, headers, params)
            assert rd.status == 401, (path, headers, params, rd.status)
            rd.join(5)


def test_event_stream_only_carries_the_callers_workspaces(live):
    env = live
    A, B = env.tenants["A"], env.tenants["B"]
    rd = open_stream(env, "/v1/events/stream", env.headers("a_contrib"))
    assert rd.status == 200
    emit(env, B.ws, f"evt-{CANARY_B}")
    emit(env, A.ws, f"evt-{CANARY_A}")
    assert wait_for(lambda: CANARY_A in rd.text(), 10), "the stream must deliver the caller's own events"
    time.sleep(1.5)
    assert CANARY_B not in rd.text(), "events of another tenant leaked into the stream"
    rd.stop.set()
    # the same through the query-string credential used by browsers
    tok = env.headers("a_contrib")["Authorization"].split()[1]
    rd2 = open_stream(env, "/v1/events/stream", params={"access_token": tok})
    emit(env, B.ws, f"evt2-{CANARY_B}")
    time.sleep(2)
    assert CANARY_B not in rd2.text()
    rd2.stop.set()
    # a principal without any binding sees nothing at all
    rd3 = open_stream(env, "/v1/events/stream", env.headers("nobody"))
    emit(env, A.ws, f"evt3-{CANARY_A}")
    time.sleep(2)
    assert CANARY_A not in rd3.text() and CANARY_B not in rd3.text()
    rd3.stop.set()


def test_log_stream_of_a_foreign_job_is_refused(live):
    env = live
    rd = open_stream(env, f"/v1/jobs/{env.tenants['B'].ids['job_id']}/logs/stream", env.headers("a_contrib"))
    assert rd.status == 403
    assert CANARY_B not in rd.text()
    rd = open_stream(env, f"/v1/jobs/{env.tenants['A'].ids['job_id']}/logs/stream", env.headers("a_contrib"))
    assert rd.status == 200
    rd.stop.set()
    rd = open_stream(env, f"/v1/jobs/{env.tenants['A'].ids['job_id']}/logs/stream", env.headers("nobody"))
    assert rd.status == 403


def test_event_stream_ends_when_the_account_is_disabled(live):
    env = live
    u = env.make_user("stream_disable", "Contributor", ("workspace", env.tenants["A"].ws))
    rd = open_stream(env, "/v1/events/stream", u.headers)
    assert rd.status == 200
    assert env.c.patch(f"/v1/identity/users/{u.id}", json={"is_active": False}, headers=env.admin).status_code == 200
    ended = rd.ended.wait(REVALIDATE_WITHIN)
    emit(env, env.tenants["A"].ws, "after-disable-evt")
    time.sleep(1.5)
    assert ended or "after-disable-evt" not in rd.text(), "a disabled account kept receiving events"
    rd.stop.set()


def test_event_stream_stops_when_the_role_is_withdrawn(live):
    env = live
    A = env.tenants["A"]
    u = env.make_user("stream_unbind", "Contributor", ("workspace", A.ws))
    rd = open_stream(env, "/v1/events/stream", u.headers)
    assert rd.status == 200
    emit(env, A.ws, "before-unbind-evt")
    assert wait_for(lambda: "before-unbind-evt" in rd.text(), 10)
    binding = [b for b in env.c.get("/v1/identity/bindings", params={"principal_id": u.id}, headers=env.admin).json()["items"] if b["principal_id"] == u.id][0]
    assert env.c.delete(f"/v1/identity/bindings/{binding['id']}", headers=env.admin).status_code == 204
    time.sleep(REVALIDATE_WITHIN / 2 + 3)  # re-validation interval + the authorizer's binding cache
    emit(env, A.ws, "after-unbind-evt")
    time.sleep(2)
    assert "after-unbind-evt" not in rd.text() or rd.ended.is_set(), "events kept flowing after the role was withdrawn"
    rd.stop.set()


def test_event_stream_ends_when_the_token_expires_or_is_revoked(live):
    env = live
    A = env.tenants["A"]
    u = env.make_user("stream_expire", "Contributor", ("workspace", A.ws))
    row = env.ctx.service("identity").get(u.id)
    short, _ = env.ctx.tokens.issue_access_token(row, ttl=4)
    rd = open_stream(env, "/v1/events/stream", params={"access_token": short})
    assert rd.status == 200
    assert rd.ended.wait(REVALIDATE_WITHIN + 15), "a stream outlived its access token"
    # a personal access token that is revoked while a stream is open
    pat = env.c.post("/v1/identity/tokens", json={"name": "stream"}, headers=u.headers).json()
    value, tid = pat.get("token") or pat.get("secret"), pat.get("id") or pat.get("token_id")
    rd = open_stream(env, "/v1/events/stream", {"Authorization": f"Bearer {value}"})
    assert rd.status == 200
    assert env.c.delete(f"/v1/identity/tokens/{tid}", headers=u.headers).status_code == 204
    assert rd.ended.wait(REVALIDATE_WITHIN), "a stream kept running after its token was revoked"


def insert_session(env, owner_id: str, ws_id: str) -> str:
    """A notebook session row without a kernel (the relay and the authorisation do not need one)."""
    from sqlalchemy import insert

    from olcf.compute import schema as C
    from olcf.core.ids import new_id
    from olcf.core.timeutil import now_iso

    sched = env.ctx.service("compute").scheduler
    sched.hooks[:] = [h for h in sched.hooks if getattr(h, "__name__", "") != "maintain"]  # the housekeeper would (rightly) declare a session without a node dead
    sid = new_id("ses")
    now = now_iso()
    with env.ctx.db.begin() as conn:
        conn.execute(insert(C.sessions).values(id=sid, workspace_id=ws_id, kind="notebook", hidden=False, owner_id=owner_id, engine="python", state="idle", resources={}, spec={},
                                               execution_count=0, created_at=now, updated_at=now, last_activity_at=now))
    return sid


def test_session_websocket_is_closed_when_access_is_withdrawn(live):
    """An open notebook-session socket can execute code: it must die with the account / role / token, not live on with connect-time rights."""
    from websockets.exceptions import ConnectionClosed
    from websockets.sync.client import connect

    env = live
    A = env.tenants["A"]
    u = env.make_user("ws_unbind", "Contributor", ("workspace", A.ws))
    sid = insert_session(env, u.id, A.ws)
    token = u.headers["Authorization"].split()[1]
    url = env.live.base_url.replace("http", "ws") + f"/v1/sessions/{sid}/ws"
    with connect(url + f"?access_token={token}", open_timeout=10, proxy=None) as sock:
        first = json.loads(sock.recv(timeout=10))
        assert first["type"] == "status"
        binding = [b for b in env.c.get("/v1/identity/bindings", params={"principal_id": u.id}, headers=env.admin).json()["items"] if b["principal_id"] == u.id][0]
        assert env.c.delete(f"/v1/identity/bindings/{binding['id']}", headers=env.admin).status_code == 204
        code = None
        end = time.time() + REVALIDATE_WITHIN
        try:
            while time.time() < end:
                sock.recv(timeout=max(0.1, end - time.time()))
        except ConnectionClosed as exc:
            code = exc.rcvd.code if exc.rcvd else None
        except TimeoutError:
            pass
        assert code in (4401, 4403, 4404), f"the socket survived the withdrawal of the role (close code {code})"
    # a disabled account
    u2 = env.make_user("ws_disable", "Contributor", ("workspace", A.ws))
    sid2 = insert_session(env, u2.id, A.ws)
    tok2 = u2.headers["Authorization"].split()[1]
    with connect(env.live.base_url.replace("http", "ws") + f"/v1/sessions/{sid2}/ws?access_token={tok2}", open_timeout=10, proxy=None) as sock:
        assert json.loads(sock.recv(timeout=10))["type"] == "status"
        assert env.c.patch(f"/v1/identity/users/{u2.id}", json={"is_active": False}, headers=env.admin).status_code == 200
        code = None
        end = time.time() + REVALIDATE_WITHIN
        try:
            while time.time() < end:
                sock.recv(timeout=max(0.1, end - time.time()))
        except ConnectionClosed as exc:
            code = exc.rcvd.code if exc.rcvd else None
        except TimeoutError:
            pass
        assert code == 4401, f"the socket of a disabled account stayed open (close code {code})"


def test_session_websocket_authentication_and_ownership(live):
    from websockets.exceptions import ConnectionClosed, InvalidStatus
    from websockets.sync.client import connect

    env = live
    A = env.tenants["A"]
    sid = insert_session(env, env.actor("a_contrib").id, A.ws)
    base = env.live.base_url.replace("http", "ws") + f"/v1/sessions/{sid}/ws"

    def attempt(query: str = "", headers: dict | None = None) -> int | None:
        try:
            with connect(base + query, open_timeout=10, proxy=None, additional_headers=headers) as sock:
                sock.recv(timeout=5)
                return 0
        except ConnectionClosed as exc:
            return exc.rcvd.code if exc.rcvd else None
        except InvalidStatus as exc:
            return exc.response.status_code

    assert attempt() == 4401
    assert attempt("?access_token=garbage") == 4401
    assert attempt("", {"Authorization": "Bearer garbage"}) == 4401
    other = env.headers("a_contrib2")["Authorization"].split()[1]  # same workspace, not the owner
    assert attempt(f"?access_token={other}") in (4403, 4404)
    foreign = env.headers("b_contrib")["Authorization"].split()[1]
    assert attempt(f"?access_token={foreign}") in (4403, 4404)
    viewer = env.headers("a_viewer")["Authorization"].split()[1]
    assert attempt(f"?access_token={viewer}") in (4403, 4404)
    owner = env.headers("a_contrib")["Authorization"].split()[1]
    assert attempt(f"?access_token={owner}") == 0
