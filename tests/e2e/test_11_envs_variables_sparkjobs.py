# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Variable sets with stages, environments, Spark job definitions and the runtime catalog."""

from __future__ import annotations

import re
import time
import uuid

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


def _open(page, goto, route):
    goto(page, route)
    page.locator(".page-head select").first.select_option(label="Sample Retail")


def _item(api, ws, typ, name):
    return next(i for i in api("GET", f"/v1/workspaces/{ws}/items", params={"type": typ, "limit": 200})["items"] if i["name"] == name)


def test_variable_set_per_stage(page, goto, api, sample_ws, snap):
    name = "vars_" + uuid.uuid4().hex[:5]
    _open(page, goto, "/variables")
    page.get_by_test_id("new-item").first.click()
    d = page.get_by_role("dialog")
    d.get_by_label("Name").fill(name)
    d.get_by_role("button", name="Create").click()
    expect(page.get_by_role("heading", name=name)).to_be_visible()
    page.get_by_test_id("add-variable").click()
    d = page.get_by_role("dialog")
    d.get_by_label("Name").fill("region")
    d.get_by_role("button", name="Add").click()
    page.get_by_label("Default of region").fill("eu")
    page.get_by_label("prod value of region").fill("us")
    expect(page.get_by_text("Unsaved changes")).to_be_visible()
    page.get_by_test_id("item-save").click()
    expect(page.get_by_text("Unsaved changes")).to_have_count(0)
    it = api("GET", f"/v1/items/{_item(api, sample_ws, 'variable_set', name)['id']}")
    assert "region" in str(it["definition"]) and "us" in str(it["definition"]) and "eu" in str(it["definition"])
    # what a job sees in each stage
    stage = page.locator("section, .card, div").filter(has_text="Resolved values").locator("select").last
    stage.select_option("prod")
    expect(page.get_by_role("table").last).to_contain_text("us")
    stage.select_option("dev")
    expect(page.get_by_role("table").last).to_contain_text("eu")
    snap(page, "variables-light")


def test_environment_publish(page, goto, api, sample_ws):
    name = "env_" + uuid.uuid4().hex[:5]
    _open(page, goto, "/environments")
    page.get_by_test_id("new-item").first.click()
    d = page.get_by_role("dialog")
    d.get_by_label("Name").fill(name)
    d.get_by_role("button", name="Create").click()
    expect(page.get_by_role("heading", name=name)).to_be_visible()
    expect(page.get_by_test_id("env-publish")).to_be_visible()
    page.get_by_test_id("env-publish").click()
    dlg = page.get_by_role("dialog")
    expect(dlg).to_contain_text(f"Publish {name}")
    dlg.get_by_test_id("publish-confirm").click()
    # an environment with nothing to resolve publishes at once; the page shows the new state either way
    expect(page.get_by_text(re.compile("Published|published|Locked|locked"), exact=False).first).to_be_visible(timeout=60_000)
    pubs = []
    for _ in range(40):
        pubs = api("GET", f"/v1/items/{_item(api, sample_ws, 'environment', name)['id']}")["definition"].get("publications") or []
        if pubs:
            break
        page.wait_for_timeout(250)
    assert pubs and pubs[0]["lock_hash"].startswith("sha256:")


def test_runtime_catalog(page, goto, api, has_runtime):
    goto(page, "/runtimes")
    expect(page.get_by_text("olcf-rt-test-4.1.1").first).to_be_visible()
    items = api("GET", "/v1/runtimes")["items"]
    assert any(r["id"] == "olcf-rt-test-4.1.1" for r in items)
    page.get_by_role("tab", name="Compatibility").click()
    expect(page.get_by_role("tabpanel").first).to_be_visible()


@pytest.mark.spark
@pytest.mark.usefixtures("spark_exclusive")
def test_spark_job_definition_validate_and_run(page, goto, api, sample_ws, has_runtime):
    if not has_runtime:
        pytest.skip("no cached Spark runtime")
    name = "sjd_" + uuid.uuid4().hex[:5]
    _open(page, goto, "/sparkjobs")
    page.get_by_test_id("new-item").first.click()
    d = page.get_by_role("dialog")
    d.get_by_label("Name").fill(name)
    d.get_by_role("button", name="Create").click()
    expect(page.get_by_role("heading", name=name)).to_be_visible()
    page.get_by_test_id("spark-validate").click()
    expect(page.get_by_text(re.compile("valid|passed|ok", re.I)).first).to_be_visible(timeout=30_000)
    page.get_by_test_id("spark-run").click()
    page.get_by_test_id("spark-run-confirm").click()
    page.wait_for_url(re.compile(r"#/jobs/job_"), timeout=30_000)
    expect(page.locator(".page-head")).to_contain_text(re.compile("Succeeded|Failed"), timeout=240_000)
    expect(page.locator(".page-head")).to_contain_text("Succeeded")
