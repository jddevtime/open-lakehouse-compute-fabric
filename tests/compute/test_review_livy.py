# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Regression tests for the external code review of 2026-10-08, findings F04 and F09-F12 (Livy provider family).

Each test first reproduces the review's evidence against the real code (MockTransport for F04, a tagged ``parameters`` cell for F09, generated Python for F10,
a fake Livy client for F11, the real job-service spec path for F12) and then pins the corrected behaviour.
"""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from olcf.compute.errors import ProviderError, SubmissionUncertain
from olcf.compute.providers.base import AppHandle, ProviderEnv, SubmitRequest
from olcf.compute.providers.external import ExternalProvider
from olcf.compute.providers.livy import LivyClient, flatten_source, livy_name
from olcf.core.errors import Unsupported


# ================================================================================================ F09 notebook parameters
def _nb(cells: list[dict[str, Any]]) -> bytes:
    return json.dumps({"cells": cells, "metadata": {}, "nbformat": 4, "nbformat_minor": 5}).encode()


def _code(src: str | list[str], tags: list[str] | None = None) -> dict[str, Any]:
    return {"cell_type": "code", "source": src, "metadata": {"tags": tags or []}, "outputs": [], "execution_count": None}


def _run(code: str) -> dict[str, Any]:
    ns: dict[str, Any] = {}
    exec(compile(code, "<livy>", "exec"), ns)  # noqa: S102 - executing the flattened job is the point of the test
    return ns


def test_f09_submitted_parameters_win_over_the_tagged_defaults_cell() -> None:
    nb = _nb([_code('region = "default"\nn = 1', ["parameters"]), _code("seen = (region, n)")])
    ns = _run(flatten_source({"source": {"format": "ipynb"}, "parameters": {"region": "requested", "n": 5}}, nb))
    assert ns["seen"] == ("requested", 5), "the defaults cell must not overwrite submitted values (review F09)"


def test_f09_without_a_parameters_cell_values_come_first_and_cell_order_is_kept() -> None:
    nb = _nb([_code(["x = 1\n", "y = x + base"]), _code("z = y * 2")])
    ns = _run(flatten_source({"source": {"format": "ipynb"}, "parameters": {"base": 10}}, nb))
    assert (ns["x"], ns["y"], ns["z"]) == (1, 11, 22)


def test_f09_falsy_structured_and_nonfinite_values_round_trip_and_unsafe_names_fail_closed() -> None:
    nb = _nb([_code("a = 7", ["parameters"]), _code("out = (a, b, c, d, e)")])
    params = {"a": 0, "b": "", "c": {"k": [1, None, False]}, "d": float("nan"), "e": "it's \"quoted\"\nnewline"}
    ns = _run(flatten_source({"source": {"format": "ipynb"}, "parameters": params}, nb))
    a, b, c, d, e = ns["out"]
    assert (a, b, c, e) == (0, "", {"k": [1, None, False]}, "it's \"quoted\"\nnewline") and d != d
    with pytest.raises(Exception, match="(?i)name|identifier"):
        flatten_source({"source": {"format": "ipynb"}, "parameters": {"bad-name": 1}}, nb)
    with pytest.raises(Exception, match="(?i)name|identifier"):
        flatten_source({"source": {"format": "ipynb"}, "parameters": {"__import__('os').system('x')": 1}}, nb)


def test_f09_python_scripts_get_the_same_literals() -> None:
    ns = _run(flatten_source({"source": {"format": "py"}, "parameters": {"n": 5, "s": "x'y"}}, b"out = (n, s, PARAMETERS)"))
    assert ns["out"] == (5, "x'y", {"n": 5, "s": "x'y"})


# ================================================================================================ F10 SQL splitting
class _Spark:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def sql(self, text: str, *a: Any, **k: Any) -> Any:
        self.statements.append(text)

        class _R:
            def show(self, **kw: Any) -> None:
                pass

        return _R()


def _statements(sql: str, params: dict[str, Any] | None = None) -> list[str]:
    spark = _Spark()
    ns: dict[str, Any] = {"spark": spark}
    exec(compile(flatten_source({"source": {"format": "sql"}, "parameters": params or {}}, sql.encode()), "<livy>", "exec"), ns)  # noqa: S102
    return spark.statements


def test_f10_semicolons_inside_literals_and_comments_do_not_split() -> None:
    assert _statements("SELECT 'a;b' AS value;") == ["SELECT 'a;b' AS value"]
    assert _statements("select 1; select 'a;b'") == ["select 1", "select 'a;b'"]
    assert _statements("select 'it''s;fine'; select \"x;y\"") == ["select 'it''s;fine'", 'select "x;y"']
    assert _statements("select 'back\\';slash'; select 2") == ["select 'back\\';slash'", "select 2"]
    assert _statements("-- a; comment\nselect 1; /* b; c */ select 2;") == ["-- a; comment\nselect 1", "/* b; c */ select 2"]
    assert _statements("select 'héllo;wörld' as v") == ["select 'héllo;wörld' as v"]
    assert _statements(" ; ;; ") == []


def test_f10_malformed_input_is_diagnosed_not_split_blindly() -> None:
    with pytest.raises(Unsupported, match="(?i)unterminated"):
        flatten_source({"source": {"format": "sql"}}, b"select 'oops; select 2")
    with pytest.raises(Unsupported, match="(?i)unterminated"):
        flatten_source({"source": {"format": "sql"}}, b"select 1 /* never closed; select 2")


# ================================================================================================ F11 batch arguments
class _FakeClient:
    def __init__(self) -> None:
        self.batches: list[dict[str, Any]] = []
        self.sessions: list[dict[str, Any]] = []

    def create_batch(self, body: dict[str, Any]) -> dict[str, Any]:
        self.batches.append(body)
        return {"id": 11, "appId": None, "state": "starting"}

    def create_session(self, body: dict[str, Any]) -> dict[str, Any]:
        self.sessions.append(body)
        return {"id": 5, "state": "starting"}

    def list_batches(self) -> list[dict[str, Any]]:
        return []

    def list_sessions(self) -> list[dict[str, Any]]:
        return []


def _provider(client: Any) -> ExternalProvider:
    p = ExternalProvider("ext", {"livy_url": "http://livy.invalid"}, ProviderEnv(None, None, None, None))  # type: ignore[arg-type]
    p._livy = client
    return p


JOB = {"id": "job_01HZZ9ABCDEFGHJKMNPQRSTVWX", "attempt": 1, "resources_request": {}}


def test_f11_batch_uses_source_args_and_main_class_from_the_normalised_spec() -> None:
    fake = _FakeClient()
    spec = {"source": {"format": "jar", "artifact_uri": "abfss://c@a.dfs.core.windows.net/app.jar", "main_class": "example.Main", "args": ["one", "two"]},
            "parameters": {"args": ["WRONG"], "main_class": "wrong.Class", "business": 1}, "spark": {"conf": {}}}
    p = _provider(fake)
    p.submit(SubmitRequest(job=JOB, spec=spec))
    (b,) = fake.batches
    assert b["file"].endswith("app.jar") and b["className"] == "example.Main" and b["args"] == ["one", "two"]


def test_f11_python_artifacts_pass_args_without_a_class_and_bad_types_are_rejected() -> None:
    fake = _FakeClient()
    p = _provider(fake)
    p.submit(SubmitRequest(job=JOB, spec={"source": {"format": "py", "artifact_uri": "s3a://b/job.py", "args": [1, "x"]}, "parameters": {}, "spark": {"conf": {}}}))
    assert fake.batches[0]["args"] == ["1", "x"] and "className" not in fake.batches[0]
    with pytest.raises(Unsupported, match="args"):
        p.submit(SubmitRequest(job=JOB, spec={"source": {"format": "py", "artifact_uri": "s3a://b/job.py", "args": "not-a-list"}, "parameters": {}, "spark": {"conf": {}}}))


def test_f11_a_jar_without_main_class_is_refused_with_an_actionable_error() -> None:
    p = _provider(_FakeClient())
    with pytest.raises(Unsupported, match="main_class"):
        p.submit(SubmitRequest(job=JOB, spec={"source": {"format": "jar", "artifact_uri": "s3a://b/app.jar", "args": []}, "parameters": {}, "spark": {"conf": {}}}))


# ================================================================================================ F12 Spark conf
def test_f12_session_and_batch_carry_user_conf_but_never_driver_or_network_keys() -> None:
    fake = _FakeClient()
    p = _provider(fake)
    conf = {"spark.sql.shuffle.partitions": "7", "spark.driver.host": "evil", "spark.driver.port": "1", "spark.blockManager.port": "2", "spark.driver.bindAddress": "0.0.0.0"}  # noqa: S104
    p.submit(SubmitRequest(job=JOB, spec={"source": {"format": "py"}, "parameters": {}, "spark": {"conf": conf}}, source_bytes=b"1"))
    assert fake.sessions[0]["conf"] == {"spark.sql.shuffle.partitions": "7"}
    p.submit(SubmitRequest(job=JOB, spec={"source": {"format": "py", "artifact_uri": "s3a://b/j.py"}, "parameters": {}, "spark": {"conf": conf}}))
    assert fake.batches[0]["conf"] == {"spark.sql.shuffle.partitions": "7"}


# ================================================================================================ F04 retry policy
def _client(handler: Any) -> LivyClient:
    c = LivyClient("http://livy.invalid", sleep=lambda s: None)
    c._http = httpx.Client(transport=httpx.MockTransport(handler))
    return c


def test_f04_a_lost_statement_response_is_not_replayed() -> None:
    """The review's reproduction: the first statement POST is accepted by the server but its response is lost."""
    posts: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        if req.method == "POST":
            posts.append(req.url.path)
            if len(posts) == 1:
                raise httpx.ReadError("connection reset after the server executed the statement", request=req)
            return httpx.Response(201, json={"id": 2, "state": "waiting"})
        return httpx.Response(200, json={})

    c = _client(handler)
    with pytest.raises(ProviderError) as ei:
        c.post_statement(3, "df.write.save()")
    assert len(posts) == 1, "a POST that may have been transmitted must never be replayed"
    assert getattr(ei.value, "uncertain", False) is True


def test_f04_connect_errors_prove_non_transmission_and_may_be_retried() -> None:
    posts: list[int] = []

    def handler(req: httpx.Request) -> httpx.Response:
        posts.append(1)
        if len(posts) < 3:
            raise httpx.ConnectError("refused", request=req)
        return httpx.Response(201, json={"id": 9})

    assert _client(handler).create_session({"kind": "pyspark"})["id"] == 9
    assert len(posts) == 3


def test_f04_throttling_429_proves_rejection_so_posts_may_retry() -> None:
    seen: list[int] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(1)
        return httpx.Response(429, headers={"Retry-After": "0"}, json={"msg": "busy"}) if len(seen) == 1 else httpx.Response(201, json={"id": 4})

    assert _client(handler).create_batch({"file": "x"})["id"] == 4 and len(seen) == 2


@pytest.mark.parametrize("code", [500, 503])
def test_c02_a_post_answered_500_or_503_is_uncertain_and_sent_exactly_once(code: int) -> None:
    """A gateway can answer 503 after the upstream committed the create, so neither 500 nor 503 proves 'not processed'."""
    posts: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        posts.append(req.url.path)
        return httpx.Response(code, headers={"Retry-After": "0"}, text="busy")

    c = _client(handler)
    for call in (lambda: c.create_session({"kind": "pyspark"}), lambda: c.create_batch({"file": "x"}), lambda: c.post_statement(1, "x")):
        posts.clear()
        with pytest.raises(SubmissionUncertain) as ei:
            call()
        assert len(posts) == 1, "exactly one POST must be made"
        assert ei.value.uncertain is True and ei.value.details["status"] == code


def test_c02_get_and_delete_still_retry_on_503() -> None:
    calls: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        calls.append(req.method)
        return httpx.Response(503, headers={"Retry-After": "0"}) if len([m for m in calls if m == req.method]) == 1 else httpx.Response(200, json={"id": 1})

    c = _client(handler)
    assert c.get_session(1) == {"id": 1}
    c.delete_session(1)
    assert calls == ["GET", "GET", "DELETE", "DELETE"]


@pytest.mark.parametrize("code", [502, 504])
def test_f04_gateway_errors_on_a_post_are_ambiguous(code: int) -> None:
    seen: list[int] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(1)
        return httpx.Response(code, text="gateway")

    with pytest.raises(ProviderError) as ei:
        _client(handler).post_statement(1, "x")
    assert len(seen) == 1 and getattr(ei.value, "uncertain", False) is True


def test_f04_reads_and_deletes_are_retried_on_transport_loss_and_5xx() -> None:
    calls: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        calls.append(req.method)
        n = len(calls)
        if n == 1:
            raise httpx.ReadError("reset", request=req)
        if n == 2:
            return httpx.Response(502)
        return httpx.Response(200, json={"id": 1, "state": "idle"})

    assert _client(handler).get_session(1)["state"] == "idle" and calls == ["GET", "GET", "GET"]


# ================================================================================================ F04 submission reconcile (provider level)
class _Server:
    """A Livy that records and commits creates, then loses the response (the review's regression scenario)."""

    def __init__(self) -> None:
        self.sessions: dict[int, dict[str, Any]] = {}
        self.statements: dict[int, list[dict[str, Any]]] = {}
        self.batches: dict[int, dict[str, Any]] = {}
        self.lose_next: list[str] = []
        self.posts: list[str] = []
        self.next_id = 0
        self.list_down = False

    def handler(self, req: httpx.Request) -> httpx.Response:
        path, m = req.url.path, req.method
        body = json.loads(req.content) if req.content else None
        if m == "POST":
            self.posts.append(path)
            self.next_id += 1
            if path == "/sessions":
                self.sessions[self.next_id] = {"id": self.next_id, "name": body.get("name"), "state": "idle", "appId": f"application_{self.next_id}"}
                doc: Any = self.sessions[self.next_id]
            elif path == "/batches":
                self.batches[self.next_id] = {"id": self.next_id, "name": body.get("name"), "state": "running", "appId": None}
                doc = self.batches[self.next_id]
            else:
                sid = int(path.split("/")[2])
                lst = self.statements.setdefault(sid, [])
                lst.append({"id": len(lst), "state": "waiting", "code": body["code"]})
                doc = {"id": len(lst) - 1, "state": "waiting"}
            if self.lose_next and path.endswith(self.lose_next[0]):
                self.lose_next.pop(0)
                raise httpx.ReadError("response lost", request=req)  # the server committed the create
            return httpx.Response(201, json=doc)
        if m == "GET" and path == "/sessions":
            if self.list_down:
                raise httpx.ConnectError("down", request=req)
            return httpx.Response(200, json={"sessions": list(self.sessions.values())})
        if m == "GET" and path == "/batches":
            if self.list_down:
                raise httpx.ConnectError("down", request=req)
            return httpx.Response(200, json={"sessions": list(self.batches.values())})
        if m == "GET" and path.endswith("/statements"):
            sid = int(path.split("/")[2])
            return httpx.Response(200, json={"statements": self.statements.get(sid, []), "total_statements": len(self.statements.get(sid, []))})
        if m == "GET" and path.startswith("/sessions/"):
            s = self.sessions.get(int(path.split("/")[2]))
            return httpx.Response(200, json=s) if s else httpx.Response(404)
        return httpx.Response(404)


def _server_provider(srv: _Server) -> ExternalProvider:
    p = ExternalProvider("ext", {"livy_url": "http://livy.invalid"}, ProviderEnv(None, None, None, None))  # type: ignore[arg-type]
    c = LivyClient("http://livy.invalid", sleep=lambda s: None)
    c._http = httpx.Client(transport=httpx.MockTransport(srv.handler))
    p._livy = c
    return p


def test_f04_session_create_with_lost_response_is_adopted_by_name_not_recreated() -> None:
    srv = _Server()
    srv.lose_next = ["/sessions"]
    p = _server_provider(srv)
    h = p.submit(SubmitRequest(job=JOB, spec={"source": {"format": "py"}, "parameters": {}, "spark": {"conf": {}}}, source_bytes=b"print(1)"))
    assert srv.posts == ["/sessions"], "no second POST /sessions"
    assert h.native["session_id"] == 1 and h.native.get("submission") != "uncertain"
    assert srv.sessions[1]["name"] == livy_name(JOB["id"], 1)


def test_f04_session_create_that_cannot_be_reconciled_returns_an_uncertain_handle_and_never_replays() -> None:
    srv = _Server()
    srv.lose_next = ["/sessions"]
    srv.list_down = True
    p = _server_provider(srv)
    h = p.submit(SubmitRequest(job=JOB, spec={"source": {"format": "py"}, "parameters": {}, "spark": {"conf": {}}}, source_bytes=b"print(1)"))
    assert h.native["submission"] == "uncertain" and h.native["session_name"] == livy_name(JOB["id"], 1)
    st = p.status(h)
    assert st.state == "pending" and st.code == "submission_uncertain"
    srv.list_down = False  # Livy is reachable again: the committed session is found by name
    st = p.status(AppHandle("ext", JOB["id"], 1, native=st.native))
    assert srv.posts == ["/sessions"]
    assert st.native["session_id"] == 1 and st.native.get("submission") != "uncertain"


def test_f04_statement_post_with_lost_response_executes_once() -> None:
    srv = _Server()
    p = _server_provider(srv)
    h = p.submit(SubmitRequest(job=JOB, spec={"source": {"format": "py"}, "parameters": {}, "spark": {"conf": {}}}, source_bytes=b"write()"))
    native = dict(h.native)
    srv.lose_next = ["/statements"]
    for _ in range(6):  # statuses: intent is persisted by the caller between calls
        st = p.status(AppHandle("ext", JOB["id"], 1, native=native))
        native = dict(st.native)
        if native.get("statement_id") is not None:
            break
    assert [x for x in srv.posts if x.endswith("/statements")] == ["/sessions/1/statements"], "exactly one remote execution"
    assert native["statement_id"] == 0
    assert len(srv.statements[1]) == 1


def test_f04_statement_post_is_never_sent_blind_after_a_crash_between_intent_and_result() -> None:
    """The controller died after the POST left but before the handle was saved: the next status call finds the statement instead of re-posting."""
    srv = _Server()
    p = _server_provider(srv)
    h = p.submit(SubmitRequest(job=JOB, spec={"source": {"format": "py"}, "parameters": {}, "spark": {"conf": {}}}, source_bytes=b"write()"))
    st = p.status(AppHandle("ext", JOB["id"], 1, native=dict(h.native)))  # phase 1: persist the intent
    assert st.native.get("statement_state") == "submitting"
    srv.statements[1] = [{"id": 0, "state": "running", "code": "write()"}]  # the POST had been processed before the crash
    st2 = p.status(AppHandle("ext", JOB["id"], 1, native=dict(st.native)))
    assert not [x for x in srv.posts if x.endswith("/statements")]
    assert st2.native["statement_id"] == 0
