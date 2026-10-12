# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Install the sample lakehouse from the overview, then run its Python notebook end to end on a real worker and see the real outputs and chart."""

from __future__ import annotations

import re
from typing import Any

import pytest


def sample_ws(stack: Any) -> dict[str, Any]:
    st = stack.api("GET", "/v1/sample/status")
    assert st["installed"], st
    return st


def items(stack: Any, ws: str, typ: str) -> list[dict[str, Any]]:
    return stack.api("GET", f"/v1/workspaces/{ws}/items", params={"type": typ, "limit": 200})["items"]


def test_install_sample_from_the_overview(page: Any, stack: Any, snap: Any) -> None:
    page.goto("/")
    page.wait_for_selector(".hrow")
    if stack.api("GET", "/v1/sample/status")["installed"]:
        pytest.skip("the sample is already installed in this stack")
    page.get_by_role("heading", name="Install the sample retail lakehouse").wait_for()
    snap(page, "sample-install-card")
    page.get_by_label("Sample size").select_option("small")
    page.get_by_role("button", name="Install sample lakehouse").click()
    page.get_by_text("Sample retail lakehouse is ready").wait_for(timeout=120_000)
    st = sample_ws(stack)
    names = {t["name"]: t["rows"] for t in st["tables"]}
    assert names["fact_sales"] > 1000 and "dim_customer" in names
    snap(page, "sample-tour")
    # the guided tour links to the notebook, dashboard and agent setup
    hrefs = page.locator("ol.tour a").evaluate_all("els => els.map(e => e.getAttribute('href')).filter(Boolean)")
    assert any("/notebook/" in h for h in hrefs) and any("/dashboards/" in h for h in hrefs) and any("agents" in h for h in hrefs)


def test_python_notebook_runs_all_cells_with_real_output_and_chart(page: Any, stack: Any, has_runtime: bool, snap: Any) -> None:
    if not has_runtime:
        pytest.skip("the cached Spark runtime is not available")
    st = sample_ws(stack)
    nb = next(i for i in items(stack, st["workspace_id"], "notebook") if i["name"] == "02_Retail_Explore_Python")
    page.goto(f"/#/notebook/{nb['id']}")
    page.wait_for_selector(".cell-main")
    assert page.locator(".cell-main").count() >= 5
    page.get_by_test_id("run-all").click()
    # a session starts on one of the three nodes; wait until every code cell has finished: an image arrived, nothing runs and the kernel is idle again
    # (the page's strict CSP forbids string evaluation, so Playwright's wait_for_function is not used: poll with locators instead)
    stable = 0
    for _ in range(480):
        idle = "Idle" in page.get_by_test_id("kernel-state").inner_text()
        done = page.locator(".cell-run-line").count() == 0 and page.locator(".outputs img.out-img").count() >= 1
        stable = stable + 1 if idle and done else 0
        if stable >= 4:
            break
        page.wait_for_timeout(500)
    assert stable >= 4, "the notebook did not finish running"
    assert page.locator(".out-error").count() == 0, page.locator(".out-error").first.inner_text()
    imgs = page.locator(".outputs img.out-img")
    assert imgs.count() >= 1  # the matplotlib figure arrived as a PNG display_data output
    natural = imgs.first.evaluate("i => new Promise(r => { if (i.complete) r([i.naturalWidth, i.naturalHeight]); else i.onload = () => r([i.naturalWidth, i.naturalHeight]); })")
    assert natural[0] > 200 and natural[1] > 100
    text = page.locator(".outputs").all_inner_texts()
    assert any(re.search(r"\d", t) for t in text)
    # the kernel really runs on a node of the fleet
    meta = page.locator(".k-meta").inner_text()
    assert "node-" in meta, meta
    page.locator(".nb-save").filter(has_text=re.compile(r"^Saved")).wait_for(timeout=30_000)  # outputs and execution counts were autosaved
    imgs.first.scroll_into_view_if_needed()
    page.evaluate("document.querySelector('.outputs img.out-img').scrollIntoView({block: 'center'})")
    snap(page, "notebook-python-results")
