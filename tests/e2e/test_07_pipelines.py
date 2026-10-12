# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Pipelines: designer, run graph, schedules with the server-computed cron preview, and a webhook trigger fired from outside the browser."""

from __future__ import annotations

import re
import time
import uuid

import httpx
import pytest
from playwright.sync_api import expect


@pytest.fixture(scope="module")
def sample_ws(stack):
    st = stack.api("GET", "/v1/sample/status")
    if not st.get("installed"):
        stack.api("POST", "/v1/sample/install", json={"size": "small"})
        for _ in range(240):
            st = stack.api("GET", "/v1/sample/status")
            if st.get("installed") and not st.get("running"):
                break
            time.sleep(1)
    assert st["installed"]
    return st["workspace_id"]


def _pick_workspace(page, name="Sample Retail"):
    page.locator(".page-head select").first.select_option(label=name)


def _dismiss_toasts(page):
    for _ in range(3):
        page.wait_for_timeout(500)
        for b in page.locator(".toasts button").all():
            try:
                b.click(timeout=1000)
            except Exception:  # noqa: BLE001, S110 - a toast that timed out on its own is exactly what we want
                pass


def _run_state(api, rid):
    return api("GET", f"/v1/pipeline-runs/{rid}")


def test_design_save_run_and_run_graph(page, goto, api, sample_ws, snap):
    name = "e2e_" + uuid.uuid4().hex[:6]
    goto(page, "/pipelines")
    _pick_workspace(page)
    page.get_by_role("button", name="New pipeline").first.click()
    page.get_by_role("dialog").get_by_label("Name").fill(name)
    page.get_by_role("dialog").get_by_role("button", name="Create pipeline").click()
    # the designer opens with the template graph
    expect(page.get_by_role("heading", name=name)).to_be_visible()
    for act in ("Count orders", "Set message", "Stop on failure"):
        expect(page.locator("svg").get_by_text(act, exact=True)).to_be_visible()
    expect(page.get_by_text("The definition is valid.")).to_be_visible()
    _dismiss_toasts(page)
    snap(page, "pipeline-designer-light")
    # add an activity from the palette, save, and the stored definition has it
    page.get_by_test_id("add-script").click()
    expect(page.get_by_test_id("inspector")).to_contain_text("script")
    page.get_by_test_id("pipeline-save").click()
    pipe = None
    for _ in range(40):
        items = api("GET", f"/v1/workspaces/{sample_ws}/items", params={"type": "pipeline", "limit": 100})["items"]
        pipe = next((i for i in items if i["name"] == name), None)
        d = api("GET", f"/v1/items/{pipe['id']}") if pipe else None
        if d and any(a["type"] == "script" for a in (d.get("definition") or {}).get("activities", [])):
            break
        page.wait_for_timeout(250)
    else:
        pytest.fail("the saved pipeline does not contain the script activity")
    # run it
    page.get_by_test_id("run-pipeline").click()
    page.get_by_test_id("run-confirm").click()
    page.wait_for_url(re.compile(r"#/pipelines/runs/|#/runs/|run_"))
    expect(page.locator(".page-head").get_by_text("Succeeded")).to_be_visible(timeout=90_000)
    expect(page.get_by_role("button", name="Set message, set_variable, succeeded")).to_be_visible()
    expect(page.get_by_role("button", name="Stop on failure, fail, skipped")).to_be_visible()
    page.get_by_role("button", name="Count orders, sql, succeeded").click()
    expect(page.get_by_role("complementary")).to_contain_text("Count orders")
    # the real run: every reachable activity succeeded, the failure branch was skipped
    runs = api("GET", "/v1/pipeline-runs", params={"workspace_id": sample_ws, "limit": 20})["items"]
    run = next(r for r in runs if r["pipeline_name"] == name)
    assert run["state"] == "succeeded"
    page.get_by_role("tab", name="Timeline").click()
    expect(page.get_by_role("tabpanel").filter(has_text="Count orders").first).to_be_visible()
    page.get_by_role("tab", name=re.compile("Parameters and outputs")).click()
    expect(page.get_by_role("tabpanel", name=re.compile("Parameters"))).to_contain_text("year")
    page.get_by_role("tab", name="Run graph").click()
    _dismiss_toasts(page)
    snap(page, "pipeline-run-graph-light")


def test_schedule_with_cron_preview(page, goto, api, sample_ws):
    pname = "sched_" + uuid.uuid4().hex[:6]
    api("POST", f"/v1/workspaces/{sample_ws}/items", json={"type": "pipeline", "name": pname, "definition": {
        "schema": "olcf.pipeline/1", "description": "", "parameters": {}, "variables": {}, "outputs": {},
        "activities": [{"name": "Wait", "type": "wait", "typeProperties": {"waitTimeInSeconds": 1}, "dependsOn": []}]}})
    goto(page, "/pipelines?tab=schedules")
    _pick_workspace(page)
    page.get_by_role("button", name="New schedule").first.click()
    dlg = page.get_by_role("dialog")
    sname = "nightly-" + uuid.uuid4().hex[:4]
    dlg.get_by_label("Name").fill(sname)
    dlg.get_by_label("Which one").select_option(label=pname)
    dlg.get_by_role("button", name="Weekdays at 08:00").click()
    dlg.get_by_label("Time zone").fill("UTC")
    preview = dlg.locator(".cron-editor")
    expect(preview).to_contain_text("Next fire times")
    expect(preview).to_contain_text("T08:00:00Z")
    for line in preview.get_by_text(re.compile(r"\d{4}-\d\d-\d\dT08:00:00Z")).all_inner_texts():
        d = time.strptime(line.split()[-1][:10], "%Y-%m-%d")
        assert d.tm_wday < 5, f"weekday cron previewed a weekend date: {line}"
    dlg.get_by_label("Cron expression").fill("not a cron")
    expect(dlg.get_by_text(re.compile("invalid|cron", re.I)).first).to_be_visible()
    page.expect_problems(r"http (400|422)\].*schedules/preview")
    dlg.get_by_label("Cron expression").fill("*/15 * * * *")
    expect(preview).to_contain_text(re.compile(r"T\d\d:(00|15|30|45):00Z"))
    dlg.get_by_role("button", name="Create schedule").click()
    expect(page.get_by_test_id("schedules")).to_contain_text(sname)
    scheds = api("GET", "/v1/schedules", params={"workspace_id": sample_ws})["items"]
    mine = next(s for s in scheds if s["name"] == sname)
    assert mine["definition"]["cron"] == "*/15 * * * *" and mine["definition"]["timezone"] == "UTC"


def test_webhook_trigger_starts_a_run(page, goto, api, sample_ws, stack):
    pname = "hook_" + uuid.uuid4().hex[:6]
    api("POST", f"/v1/workspaces/{sample_ws}/items", json={"type": "pipeline", "name": pname, "definition": {
        "schema": "olcf.pipeline/1", "description": "", "parameters": {}, "variables": {}, "outputs": {},
        "activities": [{"name": "Wait", "type": "wait", "typeProperties": {"waitTimeInSeconds": 1}, "dependsOn": []}]}})
    goto(page, "/pipelines?tab=triggers")
    _pick_workspace(page)
    page.get_by_role("button", name="New trigger").first.click()
    dlg = page.get_by_role("dialog")
    tname = "on-call-" + uuid.uuid4().hex[:4]
    dlg.get_by_label("Name").fill(tname)
    dlg.get_by_label("Pipeline to run").select_option(label=pname)
    dlg.get_by_label("Authentication").select_option(label="Bearer token")
    dlg.get_by_role("button", name="Create trigger").click()
    reveal = page.get_by_role("dialog")
    expect(reveal).to_contain_text("Copy the token now")
    codes = reveal.locator("pre, code").all_inner_texts()
    url = next(c for c in codes if "/v1/hooks/" in c).strip()
    token = next(c for c in codes if c.strip() != url and "/v1/hooks/" not in c).strip()
    page.keyboard.press("Escape")
    expect(page.get_by_test_id("triggers")).to_contain_text(tname)
    # an outside system calls the URL: no token -> refused, right token -> a run starts and finishes
    assert httpx.post(url, json={"source": "e2e"}).status_code in (401, 403)
    r = httpx.post(url, headers={"Authorization": f"Bearer {token}"}, json={"source": "e2e"})
    assert r.status_code in (200, 201, 202), r.text
    end = time.time() + 60
    run = None
    while time.time() < end:
        runs = api("GET", "/v1/pipeline-runs", params={"workspace_id": sample_ws, "limit": 50})["items"]
        run = next((x for x in runs if x["pipeline_name"] == pname), None)
        if run and run["state"] in ("succeeded", "failed"):
            break
        time.sleep(1)
    assert run and run["state"] == "succeeded", run
    page.get_by_role("tab", name=re.compile("^Runs")).click()
    expect(page.get_by_text(pname).first).to_be_visible()


@pytest.mark.parametrize("route", ["/pipelines", "/environments"])
def test_default_workspace_is_the_chosen_one_else_the_sample(page, goto, api, sample_ws, route):
    """FIX P: with nothing chosen the page starts in the sample workspace (not the alphabetically first one); a choice is remembered across reloads."""
    other = next((w for w in api("GET", "/v1/workspaces", params={"limit": 200})["items"] if w["name"] == "aaa-first"), None)
    other = other or api("POST", "/v1/workspaces", json={"name": "aaa-first"})
    select = page.locator(".page-head select").first
    goto(page, route)
    page.evaluate("localStorage.removeItem('olcf.workspace')")
    page.reload()
    page.wait_for_selector(".page-head select", state="visible")
    expect(select).to_have_value(sample_ws)
    select.select_option(label="aaa-first")
    page.reload()
    page.wait_for_selector(".page-head select", state="visible")
    expect(select).to_have_value(other["id"])
    # a stored workspace that no longer exists falls back to the sample again
    page.evaluate("localStorage.setItem('olcf.workspace', JSON.stringify('wks_gone'))")
    page.reload()
    page.wait_for_selector(".page-head select", state="visible")
    expect(select).to_have_value(sample_ws)
    page.evaluate("localStorage.removeItem('olcf.workspace')")
