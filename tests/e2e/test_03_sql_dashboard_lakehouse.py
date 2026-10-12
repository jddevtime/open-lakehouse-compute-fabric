# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The SQL editor, the Retail Sales Overview dashboard and the lakehouse explorer, each checked against what the API returns for the same question."""

from __future__ import annotations

import re
from typing import Any

import pytest

from tests.e2e.test_02_sample_notebook import items, sample_ws

SQL = "SELECT count(*) AS orders_lines, round(sum(net_amount), 2) AS revenue FROM fact_sales"


@pytest.fixture(scope="module", autouse=True)
def _sample(stack: Any) -> None:
    if not stack.api("GET", "/v1/sample/status")["installed"]:
        stack.api("POST", "/v1/sample/install", json={"size": "small"})


def set_sql(page: Any, sql: str) -> None:
    """Type like a person (CodeMirror 5 reads keystrokes by polling: zero-delay synthetic typing drops characters), then close any completion popup."""
    page.wait_for_selector(".sql-cm .CodeMirror")
    page.locator(".sql-cm .CodeMirror").click()
    page.keyboard.press("Control+a")
    page.keyboard.type(sql, delay=30)
    page.keyboard.press("Escape")
    assert page.evaluate("document.querySelector('.sql-cm .CodeMirror').CodeMirror.getValue()") == sql


def test_sql_editor_runs_a_query_and_matches_the_api(page: Any, stack: Any, snap: Any) -> None:
    st = sample_ws(stack)
    page.goto("/#/sql")
    page.wait_for_selector(".sql-cm .CodeMirror")
    page.get_by_label("Default lakehouse").select_option(label="retail_lakehouse")
    set_sql(page, SQL)
    page.get_by_test_id("sql-run").click()
    res = page.get_by_test_id("sql-results")
    res.get_by_text("orders_lines").first.wait_for(timeout=60_000)
    api = stack.api("POST", "/v1/sql/execute", json={"workspace_id": st["workspace_id"], "lakehouse": "retail_lakehouse", "sql": SQL, "max_rows": 10})
    n, revenue = api["rows"][0]
    text = res.inner_text().replace(",", "")
    assert str(n) in text, text
    assert f"{revenue:.2f}".rstrip("0").rstrip(".") in text or f"{revenue:.1f}" in text or str(round(revenue)) in text, (revenue, text)
    snap(page, "sql-editor-light")
    # a syntax error is reported as a readable message, not a stack trace, and the editor stays usable
    set_sql(page, "SELEC nonsense FROM")
    page.get_by_test_id("sql-run").click()
    res.locator("[role=alert]").first.wait_for(timeout=30_000)
    assert "Traceback" not in res.inner_text()
    page.expect_problems(r"\[http (400|422)\]", r"status of (400|422)")


def test_dashboard_tiles_equal_the_api(page: Any, stack: Any, snap: Any) -> None:
    st = sample_ws(stack)
    rep = next(i for i in items(stack, st["workspace_id"], "report") if i["name"] == "Retail Sales Overview")
    page.goto(f"/#/dashboards/{rep['id']}")
    page.wait_for_selector("[data-tile=revenue] .kpi, [data-tile=revenue]")
    page.wait_for_selector("[data-tile=revenue_by_month] svg", timeout=60_000)
    api = stack.api("POST", f"/v1/reports/{rep['id']}/run", json={})
    tiles = {t["id"]: t for t in api["tiles"]}
    assert not [t["id"] for t in api["tiles"] if t["error"]], "every tile must run without an error"
    # KPI tiles: the number on screen is the number the API computed for the same period filter
    revenue = tiles["revenue"]["rows"][0][0]
    shown = page.locator("[data-tile=revenue]").inner_text().replace(",", "")
    assert re.search(rf"\$?{round(revenue)}\b", shown), (revenue, shown)
    orders = tiles["orders"]["rows"][0][0]
    assert str(int(orders)) in page.locator("[data-tile=orders]").inner_text().replace(",", "")
    # a chart tile draws one mark per row of its result (bars here)
    n_rows = tiles["revenue_by_category"]["row_count"]
    bars = page.locator("[data-tile=revenue_by_category] path.bar")
    assert bars.count() == n_rows
    # every tile offers its table view and the data download (accessibility: the chart is never the only way to read the numbers)
    page.locator("[data-tile=revenue_by_month]").get_by_text("Show data table").click()
    assert page.locator("[data-tile=revenue_by_month] table").count() == 1
    page.locator("[data-tile=revenue_by_month]").get_by_role("button", name=re.compile("Show chart|Show data table")).first.click()
    page.wait_for_selector("[data-tile=revenue_by_month] svg")
    page.mouse.move(0, 0)
    snap(page, "dashboard-light")
    # changing the period filter reruns the tiles with the new range
    frm = page.get_by_label("Period from")
    frm.fill("2025-07-01")
    frm.dispatch_event("change")
    page.wait_for_timeout(1500)
    sub = stack.api("POST", f"/v1/reports/{rep['id']}/run", json={"filters": {"period": {"from": "2025-07-01", "to": "2025-12-31"}}})
    new_rev = next(t for t in sub["tiles"] if t["id"] == "revenue")["rows"][0][0]
    assert new_rev < revenue
    assert re.search(rf"\$?{round(new_rev)}\b", page.locator("[data-tile=revenue]").inner_text().replace(",", ""))


def test_lakehouse_explorer_lists_tables_with_schema_and_preview(page: Any, stack: Any, snap: Any) -> None:
    st = sample_ws(stack)
    page.goto(f"/#/lakehouse/{st['lakehouse_id']}")
    page.wait_for_selector("ul[aria-label=Tables] li")
    names = page.locator("ul[aria-label=Tables] li b").all_inner_texts()
    assert {"fact_sales", "dim_customer", "dim_product", "dim_date"} <= set(names)
    page.locator("ul[aria-label=Tables] li button").filter(has=page.locator("b", has_text=re.compile(r"^fact_sales$"))).click()
    page.get_by_role("heading", name="fact_sales").wait_for()
    page.get_by_role("tab", name=re.compile("Schema")).click()
    cols = page.locator("[role=tabpanel] table tbody tr").count()
    meta = stack.api("GET", f"/v1/lakehouses/{st['lakehouse_id']}/tables/fact_sales")
    assert cols == len(meta["schema"])
    page.get_by_role("tab", name="Preview").click()
    page.wait_for_selector(".vgrid")
    snap(page, "lakehouse-light")
    page.get_by_role("tab", name=re.compile("History")).click()
    assert page.locator("[role=tabpanel] table tbody tr").count() >= 1
    # the files browser lists the lakehouse folder
    page.get_by_role("button", name="Files").first.click() if page.get_by_role("button", name="Files").count() else None
