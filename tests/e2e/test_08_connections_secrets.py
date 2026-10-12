# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Connections and secrets: create a real storage connection in the browser, test it end to end (write, read, delete), store a secret that is never shown again, rotate it."""

from __future__ import annotations

import re
import uuid
from pathlib import Path

from playwright.sync_api import expect


def test_local_connection_is_created_and_tested(page, goto, api, stack, tmp_path_factory, snap):
    root = Path(tmp_path_factory.mktemp("conn-root"))
    name = "e2e-local-" + uuid.uuid4().hex[:5]
    goto(page, "/connections")
    page.get_by_test_id("new-connection").first.click()
    dlg = page.get_by_role("dialog")
    dlg.get_by_role("radio", name=re.compile("Local filesystem")).click()
    dlg.get_by_label("Name").fill(name)
    dlg.get_by_label("Available to").select_option(index=0)
    dlg.get_by_label("Root").fill(str(root))
    dlg.get_by_test_id("conn-save").click()
    expect(page.get_by_test_id("connections")).to_contain_text(name)
    conn = next(c for c in api("GET", "/v1/connections", params={"limit": 200})["items"] if c["name"] == name)
    assert conn["type"] == "local" and conn["config"]["root"] == str(root)
    # the connectivity test runs real I/O against the directory and reports each step
    page.get_by_test_id("test-" + name).click()
    td = page.get_by_role("dialog")
    expect(td).to_contain_text(re.compile("pass|ok|succeeded", re.I), timeout=60_000)
    expect(td.locator(".steps li, li").first).to_be_visible()
    snap(page, "connections-test")
    page.keyboard.press("Escape")
    conn = api("GET", f"/v1/connections/{conn['id']}")
    assert (conn.get("last_test") or {}).get("ok") is True
    expect(page.get_by_test_id("connections").get_by_text("Passed").first).to_be_visible()


def test_secret_value_is_never_displayed(page, goto, api):
    name = "e2e-secret-" + uuid.uuid4().hex[:5]
    value = "S3cr3t-" + uuid.uuid4().hex
    goto(page, "/connections?tab=secrets")
    page.get_by_test_id("new-secret").first.click()
    dlg = page.get_by_role("dialog")
    dlg.get_by_label("Name").fill(name)
    box = dlg.get_by_test_id("secret-value")
    assert box.get_attribute("type") == "password"
    box.fill(value)
    dlg.get_by_test_id("secret-save").click()
    secrets = page.get_by_test_id("secrets")
    expect(secrets).to_contain_text(name)
    assert value not in page.content()
    rec = next(s for s in api("GET", "/v1/secrets", params={"limit": 200})["items"] if s["name"] == name)
    assert value not in str(rec)
    # rotate with a new value; the list still shows no value and the version moves on
    page.get_by_role("button", name=re.compile(f"Actions for {re.escape(name)}")).click()
    page.get_by_role("menuitem", name=re.compile("Rotate")).click()
    d2 = page.get_by_role("dialog")
    d2.get_by_test_id("secret-value").fill(value + "-v2")
    d2.get_by_test_id("secret-save").click()
    expect(page.get_by_role("dialog")).to_have_count(0)
    assert (value + "-v2") not in page.content()
    rec2 = next(s for s in api("GET", "/v1/secrets", params={"limit": 200})["items"] if s["id"] == rec["id"])
    assert rec["version"] == 1 and rec2["version"] == 2 and rec2["rotated_at"]
    assert value not in str(rec2) and (value + "-v2") not in str(rec2)
