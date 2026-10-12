# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""First-run browser smoke test with NO Spark runtime (FIRSTRUN): sign in to the stack that ``olcf quickstart --no-runtime`` created, the home page shows the platform
healthy, the built-in host-Python runtime is on the Runtimes page (flagged "No Spark", nothing to install), the sample dashboard renders, a Spark job that cannot run says why
- all with zero console errors.  The module brings its own stack (the shared 3-agent Spark stack of the other e2e modules is not used)."""

from __future__ import annotations

import re
import time
from collections.abc import Iterator
from typing import Any

import pytest

from tests.e2e.firstrun import FirstRunStack


@pytest.fixture(scope="module")
def stack(tmp_path_factory: pytest.TempPathFactory) -> Iterator[FirstRunStack]:  # overrides the session-wide stack fixture for this module
    st = FirstRunStack(tmp_path_factory.mktemp("e2e-firstrun")).start()
    try:
        yield st
    finally:
        st.stop()


def test_sign_in_and_home_is_healthy_without_spark(new_page: Any, stack: FirstRunStack) -> None:
    pg = new_page(anon=True)
    pg.goto("/")
    pg.wait_for_selector("form")
    pg.get_by_label("Username").fill(stack.username)
    pg.locator("input[type=password]").fill(stack.password)
    pg.get_by_role("button", name="Sign in").click()
    pg.wait_for_selector(".page-head h1")
    pg.wait_for_selector(".hrow")
    pg.wait_for_function("document.querySelectorAll('.hrow .light').length >= 8")
    # the embedded worker registers a moment after the control plane is up (slower on a cold CI runner), so give the lights up to a minute to settle
    deadline = time.monotonic() + 60
    while True:
        pg.wait_for_timeout(1500)  # a health poll settles
        states = pg.locator(".hrow .light").evaluate_all("els => els.map(e => [e.closest('.hrow').querySelector('.name').textContent, e.className])")
        assert len(states) >= 8
        bad = [n for n, cls in states if re.search(r"\b(down|bad|fail)\b", cls)]
        if not bad or time.monotonic() > deadline:
            break
    assert not bad, f"components that are not healthy on a fresh install: {bad} ({states})"
    # the sample is installed: the tour is on the home page, the Spark runtime step says it is optional
    pg.get_by_text("Sample retail lakehouse is ready").wait_for()
    assert pg.get_by_text("Install a Spark runtime (optional)").count() == 1
    assert not pg.problems, "\n".join(pg.problems)


def test_runtimes_page_lists_host_python_as_built_in_and_without_spark(page: Any, stack: FirstRunStack) -> None:
    page.goto("/#/runtimes")
    page.wait_for_selector(".page-head h1")
    card = page.locator("article.rt-card[aria-label='Runtime olcf-host-python']")
    card.wait_for()
    text = card.inner_text()
    assert "No Spark" in text and "Built in" in text and "host-python" in text
    assert card.get_by_role("button", name="Install", exact=True).count() == 0, "nothing to install on the built-in runtime"
    card.get_by_role("button", name=re.compile("Available on 1 of 1 node")).wait_for()
    # the Spark runtimes of the catalog are listed too, each with its Install button
    spark = page.locator("article.rt-card[aria-label='Runtime olcf-rt-2.0.0']")
    spark.wait_for()
    assert spark.get_by_role("button", name="Install", exact=True).count() == 1
    assert "Not installed on any node" in spark.inner_text()


def test_compute_page_shows_the_node_with_its_host_python_runtime(page: Any, stack: FirstRunStack) -> None:
    page.goto("/#/compute")
    page.wait_for_selector(".page-head h1")
    page.get_by_text("host-python").first.wait_for()


def test_sample_dashboard_renders_with_data(page: Any, stack: FirstRunStack) -> None:
    ws = stack.api("GET", "/v1/sample/status")["workspace_id"]
    rep = next(i for i in stack.api("GET", f"/v1/workspaces/{ws}/items", params={"type": "report", "limit": 50})["items"] if i["name"] == "Retail Sales Overview")
    page.goto(f"/#/dashboards/{rep['id']}")
    page.wait_for_selector("[data-tile=revenue] .kpi, [data-tile=revenue]")
    page.wait_for_selector("[data-tile=revenue_by_month] svg", timeout=60_000)
    assert re.search(r"\d", page.locator("[data-tile=revenue]").inner_text())


def test_a_spark_job_without_a_runtime_says_why_it_waits(page: Any, stack: FirstRunStack) -> None:
    ws = stack.api("GET", "/v1/sample/status")["workspace_id"]
    etl = next(i for i in stack.api("GET", f"/v1/workspaces/{ws}/items", params={"type": "notebook", "limit": 50})["items"] if i["name"] == "01_Retail_Medallion_ETL")
    job = stack.api("POST", "/v1/jobs", json={"kind": "notebook", "workspace_id": ws, "item_id": etl["id"], "engine": "spark", "name": "ui-needs-spark"})
    try:
        page.goto(f"/#/jobs/{job['id']}")
        banner = page.locator(".banner", has_text="Waiting in the queue")
        banner.wait_for(timeout=40_000)
        txt = banner.inner_text()
        assert "No runtime" in txt and "olcf runtime install" in txt, txt
        page.get_by_role("tab", name="Timeline").click()
        page.get_by_text("queue_reason").first.wait_for()
    finally:
        stack.api("POST", f"/v1/jobs/{job['id']}/cancel", json={})
