# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""Interoperability: the *official* ``mcp`` Python client against a live uvicorn server (HTTP) and through the stdio bridge.

The server runs on a thread of this process (no child server process); the stdio bridge is a short-lived child that is always reaped.
"""

from __future__ import annotations

import asyncio
import json
import socket
import sys
import threading
import time
from pathlib import Path
from typing import Any

import httpx
import pyarrow as pa
import pytest

mcp = pytest.importorskip("mcp")
uvicorn = pytest.importorskip("uvicorn")

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture()
def live(data_app):
    """Live server + a Contributor with an ``mcp`` PAT, a lakehouse with data, and a second workspace the agent must not see."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(data_app, host="127.0.0.1", port=port, log_level="warning", lifespan="on", access_log=False))
    t = threading.Thread(target=server.run, daemon=True, name="live-uvicorn")
    t.start()
    for _ in range(200):
        if server.started:
            break
        time.sleep(0.05)
    else:
        raise RuntimeError("uvicorn did not start")
    ctx = data_app.state.ctx
    base = f"http://127.0.0.1:{port}"
    pw = ctx.settings.path("secrets", "bootstrap-admin.txt").read_text().split("password=")[1].strip()
    with httpx.Client(base_url=base, timeout=30) as http:
        admin = {"Authorization": "Bearer " + http.post("/v1/identity/login", json={"username": "admin", "password": pw}).json()["access_token"]}
        ws1 = http.post("/v1/workspaces", json={"name": "ws1"}, headers=admin).json()
        ws2 = http.post("/v1/workspaces", json={"name": "ws2"}, headers=admin).json()
        lh1 = http.post("/v1/workspaces/ws1/lakehouses", json={"name": "lh1"}, headers=admin).json()
        lh2 = http.post("/v1/workspaces/ws2/lakehouses", json={"name": "lh2"}, headers=admin).json()
    storage = ctx.service("storage")
    storage.tables.write_table(storage.get_lakehouse(lh1["id"]), "sales", pa.table({"id": pa.array(range(100), pa.int64()), "region": ["eu" if i % 2 else "us" for i in range(100)],
                                                                                    "amount": pa.array([float(i) for i in range(100)])}))
    storage.tables.write_table(storage.get_lakehouse(lh2["id"]), "secrets", pa.table({"k": ["top"]}))
    ident = ctx.service("identity")
    row = ident.create_principal("liveagent", kind="user", password="correct horse battery staple")
    ident.bind(row["id"], "Contributor", scope_type="workspace", scope_id=ws1["id"])
    token, _ = ctx.tokens.create_pat(row["id"], "live", scopes=["mcp"], ttl_days=1)
    try:
        yield {"url": f"{base}/mcp", "token": token, "ctx": ctx, "ws1": ws1, "ws2": ws2}
    finally:
        server.should_exit = True
        t.join(timeout=20)


def _text(result: Any) -> dict[str, Any]:
    return json.loads(result.content[0].text)


async def _exercise(client: Any) -> dict[str, Any]:
    seen: dict[str, Any] = {}
    tools = await client.list_tools()
    seen["tools"] = {t.name: t for t in tools.tools}
    seen["workspaces"] = _text(await client.call_tool("list_workspaces", {}))
    seen["tables"] = _text(await client.call_tool("list_tables", {"lakehouse": "lh1"}))
    seen["describe"] = _text(await client.call_tool("describe_table", {"lakehouse": "lh1", "table": "sales"}))
    seen["sql"] = _text(await client.call_tool("run_sql", {"sql": "SELECT region, count(*) AS n FROM lh1.sales GROUP BY region ORDER BY region", "lakehouse": "lh1"}))
    seen["denied"] = await client.call_tool("run_sql", {"sql": "SELECT * FROM ws2.lh2.secrets", "lakehouse": "lh1"})
    seen["write"] = await client.call_tool("run_sql", {"sql": "DROP TABLE lh1.sales", "lakehouse": "lh1"})
    seen["resources"] = (await client.list_resources()).resources
    seen["resource"] = await client.read_resource("olcf://ws1/lh1/Tables/sales")
    seen["dictionary"] = await client.read_resource("olcf://ws1/lh1/data-dictionary")
    seen["prompts"] = (await client.list_prompts()).prompts
    seen["prompt"] = await client.get_prompt("analyze_table", {"lakehouse": "lh1", "table": "sales"})
    return seen


def _check(seen: dict[str, Any]) -> None:
    names = set(seen["tools"])
    assert {"list_workspaces", "list_tables", "describe_table", "run_sql", "run_report", "search_catalog", "get_lineage"} <= names and "submit_notebook_job" not in names
    run_sql = seen["tools"]["run_sql"]
    assert run_sql.input_schema["required"] == ["sql"] and run_sql.annotations.read_only_hint is True
    assert [w["name"] for w in seen["workspaces"]["workspaces"]] == ["ws1"]
    assert {t["name"]: t["row_count"] for t in seen["tables"]["tables"]} == {"sales": 100}
    assert seen["describe"]["reference"] == "lh1.sales" and [c["name"] for c in seen["describe"]["columns"]] == ["id", "region", "amount"]
    assert seen["sql"]["rows"] == [["eu", 50], ["us", 50]]
    assert seen["denied"].is_error and "top" not in seen["denied"].content[0].text
    assert seen["write"].is_error and "read-only" in seen["write"].content[0].text
    uris = {str(r.uri) for r in seen["resources"]}
    assert "olcf://ws1/lh1/Tables/sales" in uris and not any("lh2" in u for u in uris)
    assert json.loads(seen["resource"].contents[0].text)["stats"]["row_count"] == 100
    assert "## sales" in seen["dictionary"].contents[0].text
    assert {p.name for p in seen["prompts"]} == {"analyze_table", "explore_lakehouse"}
    assert "lh1.sales" in seen["prompt"].messages[0].content.text


def test_official_client_over_streamable_http(live):
    from mcp.client.client import Client
    from mcp.client.streamable_http import streamable_http_client
    from mcp.shared._httpx_utils import create_mcp_http_client

    progress: list[Any] = []

    async def go() -> dict[str, Any]:
        async with create_mcp_http_client(headers={"Authorization": f"Bearer {live['token']}"}) as http:
            async with Client(streamable_http_client(live["url"], http_client=http)) as client:
                assert client.server_info.name == "olcf"
                assert client.protocol_version in ("2025-06-18", "2025-03-26")
                seen = await _exercise(client)

                async def on_progress(p: float, total: float | None, message: str | None) -> None:
                    progress.append((p, message))

                seen["streamed"] = await client.call_tool("run_sql", {"sql": "SELECT 7 AS seven", "lakehouse": "lh1"}, progress_callback=on_progress)
                return seen

    seen = asyncio.run(asyncio.wait_for(go(), timeout=120))
    _check(seen)
    assert _text(seen["streamed"])["rows"] == [[7]]
    assert progress and progress[0][1] == "started"                     # the SSE path delivered progress notifications to the real client
    # the session was terminated by the client on exit, and every call is audited under the agent's own identity
    calls = live["ctx"].service("mcp").stats()
    assert calls.get("ok", 0) >= 9 and calls.get("error", 0) >= 2
    from sqlalchemy import func, select

    from olcf.mcp.schema import mcp_sessions
    with live["ctx"].db.connect() as conn:
        assert conn.execute(select(func.count()).select_from(mcp_sessions).where(mcp_sessions.c.closed_at.is_(None))).scalar() == 0


def test_official_client_rejects_a_bad_token(live):
    from mcp.client.client import Client
    from mcp.client.streamable_http import streamable_http_client
    from mcp.shared._httpx_utils import create_mcp_http_client

    async def go() -> None:
        async with create_mcp_http_client(headers={"Authorization": "Bearer olcf_pat_definitelynotvalid0123456789"}) as http:
            async with Client(streamable_http_client(live["url"], http_client=http)) as client:
                await client.list_tools()

    with pytest.raises(BaseExceptionGroup) as exc:
        asyncio.run(asyncio.wait_for(go(), timeout=60))
    errors = _leaves(exc.value)
    assert any(isinstance(e, mcp.MCPError) and e.code == -32000 and "token" in str(e) for e in errors), errors


def _leaves(e: BaseException) -> list[BaseException]:
    return [x for sub in e.exceptions for x in _leaves(sub)] if isinstance(e, BaseExceptionGroup) else [e]


def test_official_client_through_the_stdio_bridge(live):
    from mcp.client.client import Client
    from mcp.client.stdio import StdioServerParameters

    params = StdioServerParameters(command=sys.executable, args=["-m", "olcf.mcp.stdio", "--url", live["url"]],
                                   env={"OLCF_MCP_TOKEN": live["token"], "PYTHONPATH": str(ROOT / "src"), "PATH": "/usr/bin:/bin"})

    async def go() -> dict[str, Any]:
        async with Client(params) as client:
            return await _exercise(client)

    seen = asyncio.run(asyncio.wait_for(go(), timeout=180))
    _check(seen)


def test_stdio_bridge_reports_errors_without_leaking_the_token(live):
    import io

    from olcf.mcp.stdio import Bridge

    out, err = io.StringIO(), io.StringIO()
    # wrong token: every request gets a JSON-RPC error with the HTTP status, and the token never reaches stdout/stderr
    b = Bridge(live["url"], "olcf_pat_wrongwrongwrongwrong", out=out, err=err)
    b.run(io.StringIO(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t", "version": "1"}}}) + "\n"
                      + "not json\n" + json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}) + "\n"))
    lines = [json.loads(x) for x in out.getvalue().splitlines()]
    answered = next(m for m in lines if m.get("id") == 1)
    assert "error" in answered and answered["error"]["message"].startswith("HTTP 401")        # addressed to the request, not id=null
    assert any(m.get("error", {}).get("code") == -32700 and m["id"] is None for m in lines)
    assert "wrongwrong" not in out.getvalue() + err.getvalue()
    # unreachable server
    out2, err2 = io.StringIO(), io.StringIO()
    Bridge("http://127.0.0.1:9/mcp", "tok", out=out2, err=err2).run(io.StringIO(json.dumps({"jsonrpc": "2.0", "id": 5, "method": "ping"}) + "\n"))
    assert json.loads(out2.getvalue())["error"]["message"].startswith("cannot reach")


def test_stdio_bridge_recovers_from_an_expired_session(live):
    import io

    from sqlalchemy import update

    from olcf.mcp.schema import mcp_sessions
    from olcf.mcp.stdio import Bridge

    out, err = io.StringIO(), io.StringIO()
    b = Bridge(live["url"], live["token"], out=out, err=err)
    init = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t", "version": "1"}}}
    b.forward(init)
    b.forward({"jsonrpc": "2.0", "method": "notifications/initialized"})
    first = b.session_id
    assert first
    with live["ctx"].db.begin() as conn:
        conn.execute(update(mcp_sessions).where(mcp_sessions.c.id == first).values(closed_at="2020-01-01T00:00:00.000000Z"))
    b.forward({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
    msgs = [json.loads(x) for x in out.getvalue().splitlines()]
    assert b.session_id and b.session_id != first
    assert msgs[-1]["id"] == 2 and msgs[-1]["result"]["tools"] and "re-established" in err.getvalue()
    b.close()
