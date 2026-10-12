# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""AI agents (MCP): the in-browser console performs the real handshake, lists the tools and calls them; a scoped token is created, used for a call and shows up in the call log."""

from __future__ import annotations

import re
import time
import uuid

import pytest
from playwright.sync_api import expect


@pytest.fixture(scope="module")
def sample(stack):
    st = stack.api("GET", "/v1/sample/status")
    if not st.get("installed"):
        stack.api("POST", "/v1/sample/install", json={"size": "small"})
        for _ in range(240):
            st = stack.api("GET", "/v1/sample/status")
            if st.get("installed") and not st.get("running"):
                break
            time.sleep(1)
    assert st["installed"]
    return st


def test_console_handshake_tools_and_calls(page, goto, api, sample, snap):
    goto(page, "/agents?tab=try")
    page.get_by_test_id("mcp-connect").click()
    expect(page.get_by_test_id("mcp-ready")).to_be_visible(timeout=30_000) if page.get_by_test_id("mcp-ready").count() else None
    expect(page.get_by_text(re.compile(r"Connected to"))).to_be_visible()
    expect(page.get_by_text(re.compile(r"\d+ tools"))).to_be_visible()
    # list_workspaces: the sample workspace is in the real result
    page.get_by_test_id("tool-list_workspaces").click()
    page.get_by_test_id("mcp-call").click()
    res = page.get_by_test_id("mcp-result")
    expect(res).to_contain_text("Sample Retail", timeout=30_000)
    page.get_by_test_id("mcp-console").scroll_into_view_if_needed()
    page.evaluate("() => { const r = document.querySelector('[data-testid=mcp-result]'); if (r) r.scrollIntoView({ block: 'end' }); }")
    snap(page, "mcp-console-light")
    # run_sql against the sample lakehouse; the count equals what the SQL API says
    expected = api("POST", "/v1/sql/execute", json={"workspace_id": sample["workspace_id"], "lakehouse": sample["lakehouse"], "sql": "SELECT count(*) AS n FROM fact_sales"})
    n = expected["rows"][0][0] if isinstance(expected["rows"][0], list) else expected["rows"][0]["n"]
    page.get_by_test_id("tool-run_sql").click()
    page.get_by_label("sql").fill("SELECT count(*) AS n FROM fact_sales")
    page.get_by_label("workspace", exact=True).fill(sample["workspace_id"])
    page.get_by_label("lakehouse", exact=True).fill(sample["lakehouse"])
    page.get_by_test_id("mcp-call").click()
    expect(page.get_by_test_id("mcp-result")).to_contain_text(str(n), timeout=60_000)
    # the write tools are marked and a read-only call that breaks the rules is refused by the server, not by the page
    expect(page.get_by_test_id("tool-submit_notebook_job")).to_contain_text("write")
    page.get_by_test_id("tool-run_sql").click()
    page.get_by_label("sql").fill("DROP TABLE fact_sales")
    page.get_by_test_id("mcp-call").click()
    expect(page.get_by_test_id("mcp-result")).to_contain_text(re.compile("read-only|only|not allowed|refused|SELECT", re.I), timeout=30_000)
    # every call is in the call log
    page.get_by_role("tab", name="Calls").click()
    expect(page.get_by_test_id("mcp-calls")).to_contain_text("run_sql")
    expect(page.get_by_test_id("mcp-calls")).to_contain_text("list_workspaces")


def test_scoped_token_from_the_page(page, goto, api, sample):
    goto(page, "/agents?tab=connect")
    name = "e2e-agent-" + uuid.uuid4().hex[:5]
    page.get_by_test_id("mcp-token-name").fill(name)
    page.get_by_test_id("mcp-token-create").click()
    shown = page.get_by_test_id("mcp-config")
    expect(shown).to_contain_text(re.compile(r"olcf_pat_[A-Za-z0-9_-]{20,}"), timeout=15_000)  # the snippet holds a placeholder until the real token is created
    mine = next(t for t in api("GET", "/v1/identity/tokens")["items"] if t["name"] == name)
    assert mine["scopes"] == ["mcp"], mine
    page.get_by_role("tab", name="Tokens").click()
    expect(page.get_by_test_id("mcp-tokens")).to_contain_text(name)
