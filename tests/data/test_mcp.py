# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""MCP endpoint at the protocol level (TestClient): handshake, sessions, auth scopes, RBAC on every tool, resources, prompts, audit,
limits, streaming and the management routes.  The official-client interoperability test lives in ``test_mcp_client.py``."""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa
import pytest

HOST = {"Host": "localhost"}


class Mcp:
    """Tiny MCP client over the TestClient."""

    def __init__(self, dc, headers: dict[str, str], version: str = "2025-06-18") -> None:
        self.dc, self.auth, self.version, self.sid = dc, headers, version, None
        self.n = 0

    def headers(self, session: bool = True, **extra: str) -> dict[str, str]:
        h = {**self.auth, **HOST, "Content-Type": "application/json", "Accept": "application/json, text/event-stream", **extra}
        if session and self.sid:
            h["Mcp-Session-Id"] = self.sid
        return h

    def post(self, body: Any, *, session: bool = True, **extra: str):
        return self.dc.post("/mcp", content=json.dumps(body), headers=self.headers(session, **extra))

    def rpc(self, method: str, params: dict[str, Any] | None = None, *, session: bool = True, **extra: str):
        self.n += 1
        body: dict[str, Any] = {"jsonrpc": "2.0", "id": self.n, "method": method}
        if params is not None:
            body["params"] = params
        return self.post(body, session=session, **extra)

    def init(self, version: str | None = None, name: str = "pytest-agent"):
        r = self.rpc("initialize", {"protocolVersion": version or self.version, "capabilities": {}, "clientInfo": {"name": name, "version": "1.0"}}, session=False)
        assert r.status_code == 200, r.text
        self.sid = r.headers["Mcp-Session-Id"]
        self.version = r.json()["result"]["protocolVersion"]
        n = self.post({"jsonrpc": "2.0", "method": "notifications/initialized"})
        assert n.status_code == 202 and n.content == b""
        return r.json()["result"]

    def result(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        r = self.rpc(method, params)
        assert r.status_code == 200, r.text
        body = r.json()
        assert "error" not in body, body
        return body["result"]

    def call(self, name: str, expect_error: bool = False, **args: Any) -> dict[str, Any]:
        res = self.result("tools/call", {"name": name, "arguments": args})
        data = json.loads(res["content"][0]["text"])
        assert res["isError"] is expect_error, data
        if self.version >= "2025-06-18":
            assert res["structuredContent"] == data
        return data


@pytest.fixture()
def world(dc):
    ws1, ws2 = dc.workspace("ws1"), dc.workspace("ws2")
    lh1, lh2 = dc.lakehouse("ws1", "lh1"), dc.lakehouse("ws2", "lh2")
    n = 500
    dc.write_table(lh1["id"], "sales", pa.table({"order_id": pa.array(range(n), pa.int64()), "region": ["eu" if i % 2 else "us" for i in range(n)], "amount": pa.array([float(i) for i in range(n)])}))
    dc.write_table(lh1["id"], "customers", pa.table({"customer_id": pa.array([1, 2, 3], pa.int64()), "name": ["a", "b", "c"]}))
    dc.write_table(lh2["id"], "secrets", pa.table({"k": ["top"], "v": pa.array([42], pa.int64())}))
    admin = dc.admin
    q = dc.post(f"/v1/workspaces/{ws1['id']}/items", json={"type": "report", "name": "Sales report", "definition": {
        "title": "Sales", "filters": [{"name": "region", "label": "Region", "type": "select", "default": None, "options": ["eu", "us"]}],
        "tiles": [{"id": "rev", "title": "Revenue", "type": "kpi", "lakehouse_id": lh1["id"], "sql": "SELECT sum(amount) AS revenue FROM lh1.sales WHERE region = coalesce(:region, region)"},
                  {"id": "bad", "title": "Bad", "type": "table", "lakehouse_id": lh1["id"], "sql": "SELECT nope FROM lh1.sales"}]}}, headers=admin)
    assert q.status_code == 201, q.text
    reader = dc.user("agent1", "Contributor", workspace_id=ws1["id"], scopes=["mcp"])
    writer = dc.user("agent2", "Contributor", workspace_id=ws1["id"], scopes=["mcp:write"])
    other = dc.user("agent3", "Contributor", workspace_id=ws2["id"], scopes=["mcp"])
    return {"ws1": ws1, "ws2": ws2, "lh1": lh1, "lh2": lh2, "report": q.json(), "reader": Mcp(dc, reader), "writer": Mcp(dc, writer), "other": Mcp(dc, other), "dc": dc}


# ----------------------------------------------------------------------------- handshake & transport
def test_initialize_negotiates_version_and_creates_a_session(world):
    m = world["reader"]
    res = m.init()
    assert res["protocolVersion"] == "2025-06-18" and res["serverInfo"]["name"] == "olcf" and "title" in res["serverInfo"]
    assert set(res["capabilities"]) == {"tools", "resources", "prompts"} and "run_sql" in res["instructions"]
    assert m.sid.startswith("mcp_") and len(m.sid) > 30
    old = Mcp(world["dc"], world["reader"].auth)
    r = old.init("2025-03-26")
    assert r["protocolVersion"] == "2025-03-26" and "title" not in r["serverInfo"]
    future = Mcp(world["dc"], world["reader"].auth)
    assert future.init("2099-01-01")["protocolVersion"] == "2025-06-18"      # we answer with what we speak; the client decides
    assert world["reader"].result("ping") == {}


def test_authentication_and_scopes(world):
    dc = world["dc"]
    anon = Mcp(dc, {})
    r = anon.rpc("initialize", {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "x", "version": "1"}}, session=False)
    assert r.status_code == 401 and r.headers["www-authenticate"].startswith("Bearer") and r.json()["error"]["message"]
    bad = Mcp(dc, {"Authorization": "Bearer olcf_pat_notarealtoken0123456789"})
    assert bad.rpc("ping", session=False).status_code == 401
    # a token without the mcp scope, and a role without mcp.use, are refused with guidance
    ws1 = world["ws1"]
    readonly = Mcp(dc, dc.user("ro-scope", "Contributor", workspace_id=ws1["id"], scopes=["read"]))
    r = readonly.rpc("ping", session=False)
    assert r.status_code == 403 and "mcp" in json.dumps(r.json())
    viewer = Mcp(dc, dc.user("viewer-x", "Viewer", workspace_id=ws1["id"], scopes=["mcp"]))
    assert viewer.rpc("ping", session=False).status_code == 403
    # an interactive user session (JWT) works too, no token needed
    jwt_user = Mcp(dc, dc.user("jwt-user", "Contributor", workspace_id=ws1["id"]))
    assert jwt_user.init()["protocolVersion"]
    # a token may not be passed in the URL
    r = dc.post("/mcp?access_token=" + world["reader"].auth["Authorization"][7:], content="{}", headers={**HOST, "Content-Type": "application/json"})
    assert r.status_code == 401


def test_origin_and_host_protection(world):
    dc, m = world["dc"], world["reader"]
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "ping"})
    base = {**m.auth, "Content-Type": "application/json", "Host": "localhost"}
    assert dc.post("/mcp", content=body, headers={**base, "Origin": "https://evil.example"}).status_code == 403
    assert dc.post("/mcp", content=body, headers={**base, "Origin": "null"}).status_code == 403
    assert dc.post("/mcp", content=body, headers={**base, "Host": "evil.example", "Origin": "http://evil.example"}).status_code == 403   # DNS rebinding
    assert dc.post("/mcp", content=body, headers={**base, "Host": "evil.example"}).status_code == 403
    ok = dc.post("/mcp", content=body, headers={**base, "Origin": "http://localhost"})
    assert ok.status_code == 400                       # origin accepted; a ping needs a session
    dc.ctx.service("settings").set("mcp.allowed_hosts", ["intranet.example"])
    dc.ctx.service("settings").set("mcp.allowed_origins", ["https://app.intranet.example"])
    assert dc.post("/mcp", content=body, headers={**base, "Host": "intranet.example"}).status_code == 400
    assert dc.post("/mcp", content=body, headers={**base, "Host": "intranet.example", "Origin": "https://app.intranet.example"}).status_code == 400
    assert dc.post("/mcp", content=body, headers={**base, "Host": "intranet.example", "Origin": "https://other.example"}).status_code == 403


def test_session_rules(world):
    dc = world["dc"]
    a, b = world["reader"], world["other"]
    a.init()
    r = a.rpc("tools/list", session=False)
    assert r.status_code == 400 and "Mcp-Session-Id" in r.json()["error"]["message"]
    assert a.rpc("tools/list", **{}).status_code == 200
    bogus = Mcp(dc, a.auth)
    bogus.sid = "mcp_does-not-exist"
    assert bogus.rpc("tools/list").status_code == 404
    # sessions are bound to their principal
    b.sid = a.sid
    assert b.rpc("tools/list").status_code == 404
    # protocol header must match the negotiated version
    assert a.rpc("tools/list", **{"MCP-Protocol-Version": "2025-03-26"}).status_code == 400
    assert a.rpc("tools/list", **{"MCP-Protocol-Version": "1999-01-01"}).status_code == 400
    assert a.rpc("tools/list", **{"MCP-Protocol-Version": "2025-06-18"}).status_code == 200
    # termination
    assert dc.delete("/mcp", headers=a.headers()).status_code == 204
    assert a.rpc("tools/list").status_code == 404
    assert dc.delete("/mcp", headers=a.headers()).status_code == 404
    assert dc.delete("/mcp", headers=b.headers(session=False)).status_code == 400
    # expiry
    c = Mcp(dc, world["reader"].auth)
    c.init()
    from sqlalchemy import update

    from olcf.mcp.schema import mcp_sessions
    with dc.ctx.db.begin() as conn:
        conn.execute(update(mcp_sessions).where(mcp_sessions.c.id == c.sid).values(expires_at="2020-01-01T00:00:00.000000Z"))
    assert c.rpc("tools/list").status_code == 404


def test_malformed_messages_and_unsupported_http_methods(world):
    m = world["reader"]
    m.init()
    h = m.headers()
    dc = world["dc"]
    r = dc.post("/mcp", content=b"{not json", headers=h)
    assert r.status_code == 400 and r.json()["error"]["code"] == -32700
    for body, code in [("[]", -32600), ("42", -32600), ('{"jsonrpc":"1.0","id":1,"method":"ping"}', -32600), ('{"jsonrpc":"2.0","id":true,"method":"ping"}', -32600),
                       ('{"jsonrpc":"2.0","id":1,"method":""}', -32600)]:
        r = dc.post("/mcp", content=body, headers=h)
        assert r.status_code in (200, 400) and r.json()["error"]["code"] == code, (body, r.text)
    assert m.rpc("no/such/method").json()["error"]["code"] == -32601
    assert dc.post("/mcp", content="{}", headers={**h, "Content-Type": "text/plain"}).status_code == 415
    assert dc.get("/mcp", headers=h).status_code == 405 and dc.get("/mcp", headers=h).headers["allow"]
    big = dc.post("/mcp", content=b" " * (1_048_576 + 10), headers=h)
    assert big.status_code == 413
    # notifications and client responses are accepted without a body
    assert m.post({"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": 1}}).status_code == 202
    assert m.post({"jsonrpc": "2.0", "id": 99, "result": {}}).status_code == 202


def test_batches_only_exist_in_the_old_protocol(world):
    dc = world["dc"]
    new, old = world["reader"], Mcp(dc, world["reader"].auth)
    new.init()
    old.init("2025-03-26")
    batch = [{"jsonrpc": "2.0", "id": 1, "method": "ping"}, {"jsonrpc": "2.0", "id": 2, "method": "tools/list"}, {"jsonrpc": "2.0", "method": "notifications/initialized"}]
    assert new.post(batch).status_code == 400
    r = old.post(batch)
    assert r.status_code == 200 and [x["id"] for x in r.json()] == [1, 2] and r.json()[1]["result"]["tools"]
    assert old.post([{"jsonrpc": "2.0", "method": "notifications/initialized"}]).status_code == 202
    assert old.post([]).status_code == 400


# ----------------------------------------------------------------------------- tools
def test_tools_list_is_scoped_and_described(world):
    r, w = world["reader"], world["writer"]
    r.init()
    w.init()
    rt = {t["name"]: t for t in r.result("tools/list")["tools"]}
    wt = {t["name"]: t for t in w.result("tools/list")["tools"]}
    reads = {"list_workspaces", "list_lakehouses", "list_tables", "describe_table", "sample_rows", "table_stats", "run_sql", "explain_query", "search_catalog", "get_lineage",
             "list_reports", "run_report"}
    assert reads <= set(rt) and not ({"submit_notebook_job", "cancel_job"} & set(rt))
    assert reads | {"submit_notebook_job", "get_job", "cancel_job"} <= set(wt) and "get_job" in rt
    t = rt["run_sql"]
    assert t["inputSchema"]["required"] == ["sql"] and t["inputSchema"]["additionalProperties"] is False and t["annotations"]["readOnlyHint"] is True and t["title"]
    assert wt["submit_notebook_job"]["annotations"]["readOnlyHint"] is False
    old = Mcp(world["dc"], r.auth)
    old.init("2025-03-26")
    assert "title" not in next(iter(old.result("tools/list")["tools"]))


def test_read_tools_end_to_end(world):
    m = world["reader"]
    m.init()
    ws = m.call("list_workspaces")
    assert [w["name"] for w in ws["workspaces"]] == ["ws1"] and ws["workspaces"][0]["lakehouse_count"] == 1
    lhs = m.call("list_lakehouses")
    assert [(x["name"], x["workspace"]) for x in lhs["lakehouses"]] == [("lh1", "ws1")]
    tabs = m.call("list_tables", lakehouse="lh1")
    assert {t["name"]: t["row_count"] for t in tabs["tables"]} == {"customers": 3, "sales": 500} and tabs["tables"][0]["reference"].startswith("lh1.")
    assert "columns" not in tabs["tables"][0]
    withcols = m.call("list_tables", lakehouse="lh1", workspace="ws1", include_columns=True)
    assert [c["name"] for c in next(t for t in withcols["tables"] if t["name"] == "sales")["columns"]] == ["order_id", "region", "amount"]
    d = m.call("describe_table", lakehouse="lh1", table="SALES")
    assert d["reference"] == "lh1.sales" and d["format"] == "delta" and d["stats"]["row_count"] == 500 and [c["type"] for c in d["columns"]] == ["long", "string", "double"]
    assert d["hint"].startswith("SELECT")
    s = m.call("sample_rows", lakehouse="lh1", table="customers", limit=2)
    assert s["row_count"] == 2 and s["columns"][0] == {"name": "customer_id", "type": "int64"} and s["truncated"] is True
    st = m.call("table_stats", lakehouse="lh1", table="sales")
    assert st["row_count"] == 500 and "amount" in st["columns"]
    q = m.call("run_sql", sql="SELECT region, count(*) AS n, sum(amount) AS total FROM lh1.sales WHERE amount >= :lo GROUP BY region ORDER BY region", parameters={"lo": 100}, lakehouse="lh1")
    assert q["rows"] == [["eu", 200, float(sum(i for i in range(100, 500) if i % 2))], ["us", 200, float(sum(i for i in range(100, 500) if i % 2 == 0))]] and q["truncated"] is False
    assert q["columns"][1] == {"name": "n", "type": "int64"} and q["query_id"].startswith("qry_") and q["tables"] == ["lh1.sales"]
    big = m.call("run_sql", sql="SELECT order_id FROM lh1.sales", lakehouse="lh1", max_rows=10)
    assert big["row_count"] == 10 and big["truncated"] is True and "note" in big
    x = m.call("explain_query", sql="SELECT count(*) FROM lh1.sales", lakehouse="lh1")
    assert x["tables"] == ["lh1.sales"] and x["plan"]
    found = m.call("search_catalog", query="sales")
    assert any(r.get("name") == "sales" for r in found["results"]) and found["count"] >= 1
    lin = m.call("get_lineage", dataset="lh1.sales")
    assert lin["dataset"]["name"] == "sales"
    reps = m.call("list_reports")
    assert [r["name"] for r in reps["reports"]] == ["Sales report"]
    rr = m.call("run_report", report="Sales report", filters={"region": "eu"}, max_rows_per_tile=5)
    tiles = {t["id"]: t for t in rr["tiles"]}
    assert tiles["rev"]["rows"] == [[float(sum(i for i in range(500) if i % 2))]] and tiles["bad"]["error"]["code"] and rr["partial"] is True and rr["errors"] == 1


def test_run_sql_is_read_only_and_safe_through_mcp(world):
    m = world["reader"]
    m.init()
    for stmt in ("INSERT INTO lh1.customers VALUES (9,'z')", "CREATE TABLE lh1.t AS SELECT 1", "DROP TABLE lh1.sales", "COPY lh1.sales TO '/tmp/x.csv'",
                 "SELECT * FROM read_text('/etc/passwd')", "SELECT getenv('HOME')", "ATTACH '/tmp/x.db'"):
        e = m.call("run_sql", expect_error=True, sql=stmt, lakehouse="lh1")
        assert e["error"]["message"] and "root:" not in json.dumps(e), stmt
    e = m.call("run_sql", expect_error=True, sql="INSERT INTO lh1.customers VALUES (9,'z')", lakehouse="lh1", engine="spark")
    assert "read-only" in e["error"]["message"]
    assert m.call("run_sql", sql="SELECT count(*) FROM lh1.customers", lakehouse="lh1")["rows"] == [[3]]


def test_every_tool_enforces_the_callers_workspaces(world):
    a, o = world["reader"], world["other"]            # a: ws1 only; o: ws2 only
    a.init()
    o.init()
    assert [w["name"] for w in o.call("list_workspaces")["workspaces"]] == ["ws2"]
    probes = [("list_tables", {"lakehouse": "lh2", "workspace": "ws2"}), ("list_tables", {"lakehouse": "ws2/lh2"}), ("describe_table", {"lakehouse": "lh2", "table": "secrets"}),
              ("sample_rows", {"lakehouse": "ws2/lh2", "table": "secrets"}), ("table_stats", {"lakehouse": "ws2/lh2", "table": "secrets"}),
              ("run_sql", {"sql": "SELECT * FROM lh2.secrets", "workspace": "ws2"}), ("run_sql", {"sql": "SELECT * FROM ws2.lh2.secrets", "lakehouse": "lh1"}),
              ("explain_query", {"sql": "SELECT * FROM ws2.lh2.secrets", "lakehouse": "lh1"}), ("get_lineage", {"dataset": "ws2/lh2.secrets"}),
              ("list_lakehouses", {"workspace": "ws2"})]
    for tool, args in probes:
        r = a.call(tool, expect_error=True, **args)
        txt = json.dumps(r)
        assert "top" not in txt and '"v"' not in txt, (tool, txt)
        assert r["error"]["code"] in ("not_found", "forbidden", "bad_request"), (tool, r)
    assert a.call("search_catalog", query="secrets")["count"] == 0
    assert o.call("search_catalog", query="secrets")["count"] >= 0
    # the other workspace's report is not even visible
    assert a.call("run_report", expect_error=True, report="Sales report", workspace="ws2")["error"]["code"] == "not_found"
    assert o.call("list_reports")["reports"] == []
    assert o.call("run_report", expect_error=True, report=world["report"]["id"])["error"]["code"] == "not_found"
    # ambiguity is reported, not guessed
    world["dc"].lakehouse("ws2", "lh1")
    both = world["dc"].user("both", "Contributor", scopes=["mcp"])
    b = Mcp(world["dc"], both)
    b.init()
    amb = b.call("list_tables", expect_error=True, lakehouse="lh1")
    assert amb["error"]["code"] == "bad_request" and "several workspaces" in amb["error"]["message"]
    assert b.call("list_tables", lakehouse="ws1/lh1")["count"] == 2


def test_invalid_arguments_and_unknown_tools(world):
    m = world["reader"]
    m.init()
    assert "'sql' is required" in m.call("run_sql", expect_error=True)["error"]["message"]
    e = m.call("run_sql", expect_error=True, sql="SELECT 1", bogus=1)
    assert "unknown argument" in e["error"]["message"] and "bogus" in e["error"]["message"]
    assert "max_rows" in m.call("run_sql", expect_error=True, sql="SELECT 1", max_rows="lots")["error"]["message"]
    assert "max_rows" in m.call("run_sql", expect_error=True, sql="SELECT 1", max_rows=10**9)["error"]["message"]
    r = m.rpc("tools/call", {"name": "drop_everything", "arguments": {}}).json()
    assert r["error"]["code"] == -32602 and "run_sql" in json.dumps(r["error"]["data"])
    assert m.rpc("tools/call", {"name": "run_sql", "arguments": [1]}).json()["error"]["code"] == -32602
    assert m.rpc("tools/call", {}).json()["error"]["code"] == -32602
    nf = m.call("describe_table", expect_error=True, lakehouse="lh1", table="custmers")
    assert "customers" in json.dumps(nf)                 # did-you-mean


def test_result_size_is_capped_and_flagged(world):
    dc, m = world["dc"], world["reader"]
    m.init()
    dc.ctx.service("settings").set("mcp.max_result_bytes", 4096)
    r = m.call("run_sql", sql="SELECT order_id, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' AS pad FROM lh1.sales", lakehouse="lh1", max_rows=500)
    assert r["truncated"] is True and 0 < r["row_count"] < 500 and "size limit" in r["note"]
    assert len(json.dumps(r)) < 6000


def test_rate_limit_returns_429(world):
    dc, m = world["dc"], world["reader"]
    m.init()
    dc.ctx.service("settings").set("mcp.rate_limit_per_minute", 3)
    codes = [m.rpc("tools/list").status_code for _ in range(5)]
    assert codes[:3] == [200, 200, 200] and codes[3:] == [429, 429]
    assert m.rpc("ping").status_code == 200              # ping is exempt so clients can keep sessions alive
    rows = dc.get("/v1/mcp/calls", params={"status": "denied"}, headers=world["reader"].auth).json()["items"]
    assert rows and rows[0]["error_code"] == "rate_limited"


def test_progress_stream_for_tool_calls(world):
    m = world["reader"]
    m.init()
    m.n += 1
    r = m.post({"jsonrpc": "2.0", "id": m.n, "method": "tools/call", "params": {"name": "run_sql", "arguments": {"sql": "SELECT 42 AS answer", "lakehouse": "lh1"}, "_meta": {"progressToken": "tok-1"}}})
    assert r.status_code == 200 and r.headers["content-type"].startswith("text/event-stream")
    events = [json.loads(line[6:]) for line in r.text.splitlines() if line.startswith("data: ")]
    assert events[0]["method"] == "notifications/progress" and events[0]["params"]["progressToken"] == "tok-1" and events[0]["params"]["progress"] == 1
    final = events[-1]
    assert final["id"] == m.n and json.loads(final["result"]["content"][0]["text"])["rows"] == [[42]]
    # without an event-stream Accept the same call is a plain JSON answer
    m.n += 1
    r = m.post({"jsonrpc": "2.0", "id": m.n, "method": "tools/call", "params": {"name": "run_sql", "arguments": {"sql": "SELECT 1"}, "_meta": {"progressToken": "t"}}}, Accept="application/json")
    assert r.headers["content-type"].startswith("application/json")


# ----------------------------------------------------------------------------- write tools
class FakeJobs:
    def __init__(self) -> None:
        self.jobs: dict[str, Any] = {}
        self.cancelled: list[str] = []

    def submit(self, principal, sub, **kw):  # noqa: ANN001, ANN202
        from olcf.core.models import JobRecord
        principal.require("job.submit", workspace_id=sub.workspace_id)
        jid = f"job_FAKE{len(self.jobs):04d}"
        rec = JobRecord(id=jid, kind=sub.kind, state="queued", workspace_id=sub.workspace_id, name=sub.name, item_id=sub.item_id, submitted_by=principal.id, origin=sub.origin,
                        parameters=sub.parameters, created_at="2026-01-01T00:00:00.000000Z", updated_at="2026-01-01T00:00:00.000000Z")
        self.jobs[jid] = rec
        return rec

    def get(self, jid):  # noqa: ANN001, ANN202
        from olcf.core.errors import NotFound
        if jid not in self.jobs:
            raise NotFound(f"job '{jid}' not found")
        return self.jobs[jid]

    def wait(self, jid, timeout=60.0):  # noqa: ANN001, ANN202
        rec = self.jobs[jid]
        self.jobs[jid] = rec.model_copy(update={"state": "succeeded"})
        return self.jobs[jid]

    def cancel(self, principal, jid, reason=""):  # noqa: ANN001, ANN202
        self.cancelled.append(jid)
        self.jobs[jid] = self.jobs[jid].model_copy(update={"state": "cancelled"})
        return self.jobs[jid]

    def logs_page(self, jid, offset=0, limit=65536, stream="combined", tail=False):  # noqa: ANN001, ANN202
        return {"job_id": jid, "data": "hello from the notebook\n", "eof": True}


def test_job_tools_need_the_write_scope_and_respect_workspaces(world):
    dc = world["dc"]
    fake = FakeJobs()
    dc.ctx.register_service("jobs", fake)
    nb = dc.post(f"/v1/workspaces/{world['ws1']['id']}/items", json={"type": "notebook", "name": "etl", "definition": {"nbformat": {"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}}},
                 headers=dc.admin)
    assert nb.status_code == 201, nb.text
    r, w, o = world["reader"], world["writer"], world["other"]
    for m in (r, w, o):
        m.init()
    denied = r.call("submit_notebook_job", expect_error=True, notebook="etl")
    assert denied["error"]["code"] == "forbidden" and "mcp:write" in denied["error"]["remediation"] and not fake.jobs
    sub = w.call("submit_notebook_job", notebook="etl", parameters={"run_date": "2024-01-01"}, workspace="ws1", wait_seconds=1)
    jid = sub["job"]["id"]
    assert sub["job"]["state"] == "succeeded" and sub["job"]["parameters"] == {"run_date": "2024-01-01"} and "hello" in sub["job"]["log_tail"]
    assert fake.jobs[jid].origin.kind.value == "mcp"
    assert w.call("get_job", job_id=jid)["job"]["id"] == jid
    # another workspace's agent cannot see, submit or cancel it
    assert o.call("get_job", expect_error=True, job_id=jid)["error"]["code"] == "not_found"
    assert o.call("submit_notebook_job", expect_error=True, notebook=nb.json()["id"])["error"]["code"] in ("not_found", "forbidden")
    assert w.call("cancel_job", job_id=jid)["job"]["state"] == "cancelled" and fake.cancelled == [jid]
    assert r.call("cancel_job", expect_error=True, job_id=jid)["error"]["code"] == "forbidden"
    # the platform audit log records write actions
    acts = {e["action"] for e in dc.get("/v1/audit", params={"limit": 200}, headers=dc.admin).json()["items"]}
    assert {"mcp.job_submitted", "mcp.job_cancelled"} <= acts


def test_job_tools_without_a_compute_service_explain_how_to_enable_it(world):
    w = world["writer"]
    w.init()
    world["dc"].ctx.services.pop("jobs", None)
    nb = world["dc"].post(f"/v1/workspaces/{world['ws1']['id']}/items", json={"type": "notebook", "name": "etl2", "definition": {"nbformat": {"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}}},
                          headers=world["dc"].admin)
    assert nb.status_code == 201
    e = w.call("submit_notebook_job", expect_error=True, notebook="etl2")
    assert e["error"]["code"] == "unavailable" and "compute" in e["error"]["remediation"]


# ----------------------------------------------------------------------------- resources & prompts
def test_resources_list_read_and_templates(world):
    m = world["reader"]
    m.init()
    rl = m.result("resources/list")
    uris = {r["uri"]: r for r in rl["resources"]}
    assert "olcf://ws1/lh1/Tables/sales" in uris and "olcf://ws1/lh1/data-dictionary" in uris and not any("lh2" in u for u in uris)
    assert uris["olcf://ws1/lh1/Tables/sales"]["mimeType"] == "application/json"
    sch = json.loads(m.result("resources/read", {"uri": "olcf://ws1/lh1/Tables/sales"})["contents"][0]["text"])
    assert sch["reference"] == "lh1.sales" and [c["name"] for c in sch["columns"]] == ["order_id", "region", "amount"] and sch["stats"]["row_count"] == 500
    md = m.result("resources/read", {"uri": "olcf://ws1/lh1/data-dictionary"})["contents"][0]
    assert md["mimeType"] == "text/markdown" and "## sales" in md["text"] and "| `amount` | double |" in md["text"] and "500 rows" in md["text"]
    tpl = m.result("resources/templates/list")["resourceTemplates"]
    assert {t["name"] for t in tpl} == {"table-schema", "data-dictionary"}
    for bad in ("olcf://ws2/lh2/Tables/secrets", "olcf://ws1/lh1/Tables/nope", "olcf://ws1/lh1/Files/x", "https://example.com/x", "olcf://ws1/lh1/data-dictionary/extra"):
        r = m.rpc("resources/read", {"uri": bad}).json()
        assert r["error"]["code"] in (-32002, -32602), (bad, r)
        assert "top" not in json.dumps(r)
    assert m.rpc("resources/read", {}).json()["error"]["code"] == -32602
    assert m.rpc("resources/list", {"cursor": "garbage!"}).json()["error"]["code"] == -32602


def test_resource_listing_is_paginated(world):
    dc, m = world["dc"], world["reader"]
    m.init()
    lh = dc.lakehouse("ws1", "many")
    for i in range(120):
        dc.write_table(lh["id"], f"t{i:03d}", pa.table({"a": [i]}))
    page1 = m.result("resources/list")
    assert len(page1["resources"]) == 100 and page1["nextCursor"]
    seen = {r["uri"] for r in page1["resources"]}
    page2 = m.result("resources/list", {"cursor": page1["nextCursor"]})
    seen |= {r["uri"] for r in page2["resources"]}
    assert len(page2["resources"]) == 24 and "nextCursor" not in page2 and len(seen) == 124           # (1 + 2 tables) + (1 + 120 tables), no duplicates across pages


def test_resource_uris_percent_encode_names_with_spaces(world):
    dc, m = world["dc"], world["reader"]
    dc.ctx.service("workspaces")  # ensure service present
    ws = dc.workspace("Sales Team")
    lh = dc.lakehouse("Sales Team", "my_lake")
    dc.write_table(lh["id"], "daily_numbers", pa.table({"a": [1]}))
    user = Mcp(dc, dc.user("spacey", "Contributor", workspace_id=ws["id"], scopes=["mcp"]))
    user.init()
    uris = [r["uri"] for r in user.result("resources/list")["resources"]]
    assert "olcf://Sales%20Team/my_lake/data-dictionary" in uris
    t = next(u for u in uris if "Tables" in u)
    assert "%20" in t and json.loads(user.result("resources/read", {"uri": t})["contents"][0]["text"])["workspace"] == "Sales Team"
    m.init()


def test_prompts(world):
    m = world["reader"]
    m.init()
    names = {p["name"] for p in m.result("prompts/list")["prompts"]}
    assert names == {"analyze_table", "explore_lakehouse"}
    p = m.result("prompts/get", {"name": "analyze_table", "arguments": {"lakehouse": "lh1", "table": "sales", "focus": "regional skew"}})
    text = p["messages"][0]["content"]["text"]
    assert p["messages"][0]["role"] == "user" and "lh1.sales" in text and "regional skew" in text and "run_sql" in text and "describe_table" in text
    e = m.result("prompts/get", {"name": "explore_lakehouse", "arguments": {"lakehouse": "lh1"}})["messages"][0]["content"]["text"]
    assert "data-dictionary" in e and "list_tables" in e
    assert m.rpc("prompts/get", {"name": "nope"}).json()["error"]["code"] == -32602
    assert m.rpc("prompts/get", {"name": "analyze_table", "arguments": {"lakehouse": "lh1"}}).json()["error"]["code"] == -32602
    assert m.rpc("prompts/get", {"name": "analyze_table", "arguments": {"lakehouse": "lh2", "table": "secrets"}}).json()["error"]["code"] in (-32602, -32603)
    assert m.rpc("prompts/get", {"name": "explore_lakehouse", "arguments": {"lakehouse": "lh1", "x": 1}}).json()["error"]["code"] == -32602


# ----------------------------------------------------------------------------- audit & management
def test_every_call_is_audited_with_redacted_arguments(world):
    dc, m = world["dc"], world["reader"]
    m.init(name="claude-test")
    m.call("run_sql", sql="SELECT 'password=hunter2hunter2' AS pw", lakehouse="lh1")
    m.call("describe_table", lakehouse="lh1", table="sales")
    m.call("describe_table", expect_error=True, lakehouse="lh2", table="secrets")
    m.call("run_sql", expect_error=True, sql="DELETE FROM lh1.sales", lakehouse="lh1")
    m.result("resources/read", {"uri": "olcf://ws1/lh1/Tables/sales"})
    m.rpc("resources/read", {"uri": "olcf://ws2/lh2/Tables/secrets"})
    m.result("prompts/get", {"name": "explore_lakehouse", "arguments": {"lakehouse": "lh1"}})
    rows = dc.get("/v1/mcp/calls", params={"limit": 100}, headers=m.auth).json()["items"]
    by = [(r["method"], r["tool"], r["status"]) for r in reversed(rows)]
    assert by[0] == ("initialize", None, "ok")
    assert ("tools/call", "run_sql", "ok") in by and ("tools/call", "describe_table", "ok") in by and ("tools/call", "describe_table", "error") in by and ("tools/call", "run_sql", "error") in by
    assert ("resources/read", "olcf://ws1/lh1/Tables/sales", "ok") in by and ("resources/read", "olcf://ws2/lh2/Tables/secrets", "error") in by and ("prompts/get", "explore_lakehouse", "ok") in by
    sqlrow = next(r for r in rows if r["tool"] == "run_sql" and r["status"] == "ok")
    assert "hunter2hunter2" not in json.dumps(sqlrow) and "REDACTED" in json.dumps(sqlrow["arguments"]) and sqlrow["client_name"] == "claude-test"
    assert sqlrow["elapsed_ms"] is not None and sqlrow["result_bytes"] > 0 and sqlrow["workspace_id"] == world["ws1"]["id"] and sqlrow["session_id"] == m.sid
    # visibility: a user sees their own; audit.read holders (admin) see all
    assert dc.get("/v1/mcp/calls", params={"mine": "false"}, headers=world["other"].auth).json()["items"] == []
    assert len(dc.get("/v1/mcp/calls", params={"mine": "false", "limit": 200}, headers=dc.admin).json()["items"]) == len(rows)
    assert all(r["tool"] == "describe_table" for r in dc.get("/v1/mcp/calls", params={"tool": "describe_table"}, headers=m.auth).json()["items"])
    page = dc.get("/v1/mcp/calls", params={"limit": 2}, headers=m.auth).json()
    assert len(page["items"]) == 2 and page["next_cursor"]


def test_info_endpoint_gives_client_configuration(world):
    dc = world["dc"]
    info = dc.get("/v1/mcp/info", headers=world["reader"].auth).json()
    assert info["endpoint"].endswith("/mcp") and info["transport"] == "streamable-http" and info["protocol_versions"] == ["2025-06-18", "2025-03-26"]
    assert "submit_notebook_job" not in {t["name"] for t in info["tools"]} and "run_sql" in {t["name"] for t in info["tools"]}
    cfg = info["client_config"]["claude_desktop"]["mcpServers"]["olcf"]
    assert cfg["args"][:2] == ["-m", "olcf.mcp.stdio"] and cfg["env"]["OLCF_MCP_TOKEN"].startswith("<") and info["client_config"]["http_clients"]["headers"]["Authorization"].startswith("Bearer <")
    assert "olcf_pat" not in json.dumps(info)
    assert dc.get("/v1/mcp/info").status_code == 401
    assert "submit_notebook_job" in {t["name"] for t in dc.get("/v1/mcp/info", headers=world["writer"].auth).json()["tools"]}


def test_disabled_endpoint_and_health(world):
    dc, m = world["dc"], world["reader"]
    m.init()
    h = dc.ctx.health.run("mcp", deep=True)
    assert h.status.value == "ok" and [s["name"] for s in h.steps] == ["initialize", "tools/list", "tools/call list_workspaces", "resources/list", "session cleanup"] and all(s["ok"] for s in h.steps)
    q = dc.ctx.health.run("mcp", force=True)
    assert q.status.value == "ok" and q.labels["tools"] >= 15
    assert len(dc.get("/v1/mcp/calls", params={"mine": "false", "limit": 200}, headers=dc.admin).json()["items"]) == 1     # probes are not audited as agent traffic
    dc.ctx.service("settings").set("mcp.enabled", False)
    assert m.rpc("tools/list").status_code == 503
    assert dc.ctx.health.run("mcp", force=True).status.value == "unknown"


def test_guide_snippets_match_what_the_server_advertises(dc):
    """docs/guides/mcp.md shows the Claude Desktop and HTTP client configs; they must stay in step with GET /v1/mcp/info."""
    import re
    from pathlib import Path

    text = (Path(__file__).resolve().parents[2] / "docs" / "guides" / "mcp.md").read_text(encoding="utf-8")
    blocks = [json.loads(b) for b in re.findall(r"```json\n(.*?)\n```", text, flags=re.S)]
    desktop = next(b for b in blocks if "mcpServers" in b)["mcpServers"]["olcf"]
    http = next(b for b in blocks if b.get("type") == "http")
    info = dc.get("/v1/mcp/info", headers=dc.admin).json()["client_config"]
    adv = info["claude_desktop"]["mcpServers"]["olcf"]
    assert desktop["command"] == adv["command"] and desktop["args"][:3] == adv["args"][:3] and list(desktop["env"]) == list(adv["env"])
    assert list(http["headers"]) == list(info["http_clients"]["headers"]) and http["type"] == info["http_clients"]["type"]
    tools = dc.get("/v1/mcp/info", headers=dc.admin).json()["tools"]
    for t in tools:
        assert f"`{t['name']}`" in text, f"tool {t['name']} is not documented in docs/guides/mcp.md"
