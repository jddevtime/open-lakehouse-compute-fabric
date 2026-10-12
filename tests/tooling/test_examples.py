# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The examples under ``examples/`` are tested, not just shipped: the agent loop against a scripted LLM, the MCP and SDK examples against a
real ``olcf serve`` process, and the static files (JSON, Markdown, notebook cell) for shape and for claims that can drift from the code."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import re
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace as NS
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
EX = ROOT / "examples"


def load(name: str) -> Any:
    """Import ``examples/<name>.py`` as a module (the examples import ``_connect`` from their own directory)."""
    if str(EX) not in sys.path:
        sys.path.insert(0, str(EX))
    spec = importlib.util.spec_from_file_location(f"ex_{name}", EX / f"{name}.py")
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


# --------------------------------------------------------------------------- scripted doubles (test doubles, not product code)
def tool(name: str, desc: str = "d") -> NS:
    return NS(name=name, description=desc, input_schema={"type": "object", "properties": {}})


def text(t: str, is_error: bool = False) -> NS:
    return NS(content=[NS(type="text", text=t)], is_error=is_error)


class FakeSession:
    def __init__(self, tools: list[NS], replies: dict[str, Any]) -> None:
        self.tools, self.replies, self.calls = tools, replies, []

    async def list_tools(self) -> NS:
        return NS(tools=self.tools)

    async def call_tool(self, name: str, args: dict[str, Any]) -> NS:
        self.calls.append((name, args))
        reply = self.replies[name]
        if isinstance(reply, Exception):
            raise reply
        return reply


class FakeLLM:
    """``llm.messages.create`` returns the scripted responses in order and records what it was sent."""

    def __init__(self, script: list[NS]) -> None:
        self.script, self.sent = list(script), []
        self.messages = NS(create=self._create)

    async def _create(self, **kw: Any) -> NS:
        self.sent.append(json.loads(json.dumps(kw, default=str)))
        return self.script.pop(0)


def say(t: str, stop: str = "end_turn") -> NS:
    return NS(content=[NS(type="text", text=t)], stop_reason=stop, usage=NS(input_tokens=10, output_tokens=5))


def use(name: str, args: dict[str, Any], uid: str = "tu1", preamble: str = "") -> NS:
    blocks = ([NS(type="text", text=preamble)] if preamble else []) + [NS(type="tool_use", id=uid, name=name, input=args)]
    return NS(content=blocks, stop_reason="tool_use", usage=NS(input_tokens=10, output_tokens=5))


# --------------------------------------------------------------------------- the agent loop
def test_agent_runs_tool_then_answers():
    agent = load("llm_agent_anthropic")
    sess = FakeSession([tool("run_sql"), tool("cancel_job")], {"run_sql": text('{"rows": [[42]]}')})
    llm = FakeLLM([use("run_sql", {"sql": "select 42"}, preamble="checking"), say("The answer is 42.")])
    events: list[tuple[Any, ...]] = []
    res = asyncio.run(agent.run_agent("what is the answer?", llm=llm, session=sess, on_event=lambda *a: events.append(a)))
    assert res.answer == "The answer is 42." and res.complete and res.turns == 2
    assert sess.calls == [("run_sql", {"sql": "select 42"})]
    assert res.tool_calls == [{"name": "run_sql", "input": {"sql": "select 42"}, "is_error": False}]
    assert (res.input_tokens, res.output_tokens) == (20, 10)
    # the tool result went back to the model as a tool_result block tied to the right id
    last = llm.sent[1]["messages"][-1]
    assert last["role"] == "user" and last["content"][0]["tool_use_id"] == "tu1" and "42" in last["content"][0]["content"]
    assert [e[0] for e in events] == ["thought", "tool_call", "tool_result"]


def test_agent_write_tools_are_hidden_unless_allowed():
    agent = load("llm_agent_anthropic")
    tools = [tool("run_sql"), tool("submit_notebook_job"), tool("cancel_job")]
    assert [t["name"] for t in agent.to_anthropic_tools(tools)] == ["run_sql"]
    assert {t["name"] for t in agent.to_anthropic_tools(tools, allow_write=True)} == {"run_sql", "submit_notebook_job", "cancel_job"}
    # 1.x spells the schema inputSchema
    legacy = NS(name="x", description=None, inputSchema={"type": "object", "properties": {"a": {}}})
    assert agent.to_anthropic_tools([legacy])[0]["input_schema"]["properties"] == {"a": {}}
    llm = FakeLLM([use("cancel_job", {"job_id": "j"}), say("done")])
    sess = FakeSession(tools, {"cancel_job": text("cancelled")})
    res = asyncio.run(agent.run_agent("q", llm=llm, session=sess))
    # the model asked for a tool it was never offered: it is told so, and the server is never called
    assert sess.calls == [] and res.tool_calls[0]["is_error"] is True
    assert "unknown tool" in llm.sent[1]["messages"][-1]["content"][0]["content"]


def test_agent_tool_errors_are_returned_to_the_model():
    agent = load("llm_agent_anthropic")
    sess = FakeSession([tool("run_sql")], {"run_sql": text("Catalog Error: no such table", is_error=True)})
    llm = FakeLLM([use("run_sql", {"sql": "select * from nope"}), say("that table does not exist")])
    res = asyncio.run(agent.run_agent("q", llm=llm, session=sess))
    assert res.complete and res.tool_calls[0]["is_error"] is True
    reply = llm.sent[1]["messages"][-1]["content"][0]
    assert reply["is_error"] is True and "no such table" in reply["content"]

    boom = FakeSession([tool("run_sql")], {"run_sql": ConnectionError("link down")})
    res = asyncio.run(agent.run_agent("q", llm=FakeLLM([use("run_sql", {}), say("sorry")]), session=boom))
    assert res.tool_calls[0]["is_error"] is True and res.complete


def test_agent_stops_at_max_turns_and_truncates_big_results():
    agent = load("llm_agent_anthropic")
    sess = FakeSession([tool("run_sql")], {"run_sql": text("x" * 500)})
    llm = FakeLLM([use("run_sql", {}, uid=f"t{i}") for i in range(3)])
    res = asyncio.run(agent.run_agent("q", llm=llm, session=sess, max_turns=3, tool_result_limit=100))
    assert res.complete is False and res.turns == 3 and "stopped after 3 turns" in res.answer
    content = llm.sent[1]["messages"][-1]["content"][0]["content"]
    assert content.startswith("x" * 100) and "truncated 400 characters" in content and len(content) < 250
    # zero turns must not crash either
    res0 = asyncio.run(agent.run_agent("q", llm=FakeLLM([]), session=sess, max_turns=0))
    assert res0.complete is False


def test_agent_needs_tools():
    agent = load("llm_agent_anthropic")
    with pytest.raises(RuntimeError, match="no usable tools"):
        asyncio.run(agent.run_agent("q", llm=FakeLLM([]), session=FakeSession([tool("cancel_job")], {})))


def test_agent_refuses_without_api_key(monkeypatch):
    agent = load("llm_agent_anthropic")
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    assert agent.main(["hello"]) == 2


# --------------------------------------------------------------------------- explorer example with a scripted session
def test_explore_reports_missing_tools():
    mod = load("agent_mcp_client")
    sess = FakeSession([tool("run_sql")], {})
    with pytest.raises(RuntimeError, match="does not offer the 'list_lakehouses'"):
        asyncio.run(mod.explore(sess, lakehouse=None, table=None, sql=None, max_rows=5))


def test_explore_walks_and_renders():
    mod = load("agent_mcp_client")
    names = ["list_lakehouses", "list_tables", "describe_table", "run_sql"]
    sess = FakeSession([tool(n) for n in names], {
        "list_lakehouses": text('{"lakehouses": [{"name": "sales"}]}'),
        "list_tables": text('{"tables": [{"name": "orders"}]}'),
        "describe_table": text('{"columns": [{"name": "id", "type": "bigint"}]}'),
        "run_sql": text('{"columns": ["id"], "rows": [[1], [2]], "truncated": true}'),
    })
    found = asyncio.run(mod.explore(sess, lakehouse=None, table=None, sql=None, max_rows=5))
    assert found["lakehouse"] == "sales" and found["table"] == "orders"
    assert sess.calls[-1] == ("run_sql", {"sql": "SELECT * FROM sales.orders LIMIT 5", "lakehouse": "sales", "max_rows": 5})
    out = mod.render(found)
    assert "id bigint" in out and "(truncated)" in out and "1" in out


def test_mcp_examples_demand_a_token(monkeypatch, capsys):
    mod = load("_connect")
    monkeypatch.delenv("OLCF_MCP_TOKEN", raising=False)
    with pytest.raises(SystemExit) as exc:
        mod.token_from_env()
    assert "olcf token create" in str(exc.value)
    monkeypatch.setenv("OLCF_URL", "https://olcf.example.com/")
    monkeypatch.delenv("OLCF_MCP_URL", raising=False)
    assert mod.endpoint() == "https://olcf.example.com/mcp" and mod.endpoint("http://x/mcp") == "http://x/mcp"


# --------------------------------------------------------------------------- against a real server
def run_example(live_server, script: str, *args: str, env: dict[str, str] | None = None, timeout: float = 120) -> subprocess.CompletedProcess[str]:
    return subprocess.run([sys.executable, str(EX / script), *args], env={**live_server.env, **(env or {})}, capture_output=True, text=True, timeout=timeout, check=False)


def test_sdk_quickstart_against_live_server(live_server):
    res = run_example(live_server, "sdk_quickstart.py", env={"OLCF_URL": live_server.url, "OLCF_USERNAME": "admin", "OLCF_PASSWORD": live_server.admin_password})
    assert res.returncode == 0, res.stdout + res.stderr
    assert "answer: 42" in res.stdout and "sum of 1..10: 55" in res.stdout and "cleaned up" in res.stdout
    assert live_server.admin_password not in res.stdout + res.stderr
    names = [w["name"] for w in live_server.json("workspace", "list")]
    assert not [n for n in names if n.startswith("sdk-quickstart-")]


def test_sdk_quickstart_without_credentials_explains(live_server):
    res = run_example(live_server, "sdk_quickstart.py", env={"OLCF_URL": live_server.url})
    assert res.returncode != 0 and "OLCF_TOKEN" in (res.stdout + res.stderr)


def test_mcp_explorer_against_live_server(live_server):
    live_server.json("sample", "install")
    tok = live_server.json("token", "create", "examples-mcp", "--scope", "mcp", "--days", "1")
    secret = tok.get("token") or tok.get("secret")
    env = {"OLCF_MCP_TOKEN": secret, "OLCF_MCP_URL": live_server.url + "/mcp"}
    res = run_example(live_server, "agent_mcp_client.py", "--json", "--max-rows", "3", env=env)
    assert res.returncode == 0, res.stdout + res.stderr
    assert secret not in res.stdout + res.stderr
    found = json.loads(res.stdout)
    assert {"list_lakehouses", "run_sql", "describe_table"} <= {t["name"] for t in found["tools"]}
    assert found["lakehouse"] and found["table"] and found["result"]["rows"]
    assert len(found["result"]["rows"]) <= 3
    # a plain-text rendering and an explicit read-only statement
    res = run_example(live_server, "agent_mcp_client.py", "--sql", "SELECT 6*7 AS answer", env=env)
    assert res.returncode == 0, res.stderr
    assert "answer" in res.stdout and "42" in res.stdout
    # without the mcp scope the server refuses; the example reports it and does not hang
    narrow = live_server.json("token", "create", "examples-read", "--scope", "read", "--days", "1")
    bad = run_example(live_server, "agent_mcp_client.py", env={**env, "OLCF_MCP_TOKEN": narrow.get("token") or narrow["secret"]})
    assert bad.returncode != 0


# --------------------------------------------------------------------------- static files
def test_desktop_config_is_what_olcf_generates():
    cfg = json.loads((EX / "claude_desktop_config.json").read_text())
    server = cfg["mcpServers"]["olcf"]
    assert server["args"] == ["mcp", "stdio"] and set(server["env"]) == {"OLCF_MCP_URL", "OLCF_MCP_TOKEN"}
    assert server["env"]["OLCF_MCP_URL"].endswith("/mcp")
    assert "REPLACE" in server["env"]["OLCF_MCP_TOKEN"]  # a placeholder, never a real secret
    assert "/usr/bin/python3" not in json.dumps(cfg)  # stub-safe on a Mac without Xcode tools


def test_every_example_has_header_and_compiles():
    for p in sorted(EX.glob("*.py")):
        src = p.read_text()
        assert "SPDX-License-Identifier: Apache-2.0" in src and "Copyright (c) 2026 Justin Dodrill" in src, p.name
        compile(src, str(p), "exec")
    json.loads((EX / "pipeline_example.json").read_text())


def test_example_docs_name_only_real_commands_and_routes():
    """Commands quoted in the markdown must exist; this guards against documenting an option that was never built."""
    from click.testing import CliRunner

    from olcf.cli.main import cli

    runner = CliRunner()
    cmds = set()
    for md in ("docker_quickstart.md", "rest_curl.md", "claude_desktop_config.md"):
        for m in re.finditer(r"\bolcf((?: (?:--?[a-z][\w-]*(?: \S+)?|[a-z][\w-]*))+)", (EX / md).read_text()):
            words = [w for w in m.group(1).split() if re.fullmatch(r"[a-z][a-z-]*", w)]
            if words:
                cmds.add(tuple(words[:2]))
    assert cmds
    checked = 0
    for words in sorted(cmds):
        top = runner.invoke(cli, [words[0], "--help"])
        if top.exit_code != 0:
            continue  # prose like "olcf is ..." is not a command
        if len(words) > 1 and f"  {words[1]} " in top.output:
            assert runner.invoke(cli, [words[0], words[1], "--help"]).exit_code == 0, words
        checked += 1
    assert checked >= 4
    assert "/v1/openapi.json" in (EX / "rest_curl.md").read_text()
    assert "queue_reason" not in (EX / "rest_curl.md").read_text()
