# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Intelligence in the browser, against the real stack with the sample workspace and completed scans: overview, catalog search to asset to lineage graph (nodes, edges, an upstream path,
evidence), impact, discovery (test connectivity, scan, gaps, coverage, import, sampling policy, changes), governance, the sample tab being off by default, and an accessibility smoke.

Screenshots are written only when ``OLCF_E2E_SHOTS`` is set (they are for review, not for the documentation)."""

from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Any

import pytest

from tests.e2e.conftest import open_route

DATA_PAGES = ("/intelligence", "/intelligence/catalog", "/intelligence/discovery", "/intelligence/discovery?tab=gaps", "/intelligence/discovery?tab=coverage", "/intelligence/discovery?tab=sampling",
              "/intelligence/lineage")


def shot(page: Any, name: str, full: bool = False) -> None:
    out = os.environ.get("OLCF_E2E_SHOTS")
    if out:
        Path(out).mkdir(parents=True, exist_ok=True)
        page.wait_for_timeout(300)
        page.screenshot(path=str(Path(out) / f"{name}.png"), full_page=full)


FIT_JS = """() => {
  const c = document.querySelector('[data-testid=lineage-svg]').closest('.lx-canvas').getBoundingClientRect();
  const out = [];
  document.querySelectorAll('[data-testid=lineage-svg] [data-node]').forEach((n) => {
    const r = n.getBoundingClientRect();
    if (r.left < c.left - 1 || r.right > c.right + 1 || r.top < c.top - 1 || r.bottom > c.bottom + 1) out.push(n.textContent.slice(0, 24));
  });
  const root = document.querySelector('[data-testid=lineage-svg] .lx-node.root').getBoundingClientRect();
  return { outside: out, rootInside: root.left >= c.left && root.right <= c.right && root.top >= c.top && root.bottom <= c.bottom, hint: !!document.querySelector('.lx-hint b'), total: document.querySelectorAll('[data-testid=lineage-svg] [data-node]').length };
}"""


def settled(dlg: Any) -> Any:
    """Wait until the dialog has moved focus into itself, so that typing is not interrupted by that move."""
    dlg.locator(":focus").first.wait_for()
    return dlg


def _scan(stack: Any, source_id: str, mode: str = "full") -> None:
    for _ in range(60):
        r = stack.http.post(f"/v1/intelligence/sources/{source_id}/scan", params={"wait": "true"}, headers=stack.admin, json={"mode": mode}, timeout=180)
        if r.status_code != 409:                                             # 409: a scheduled scan of that source is running right now
            assert r.status_code in (200, 202), r.text
            return
        time.sleep(1)
    raise AssertionError("the source stayed busy")


@pytest.fixture(scope="module")
def intel(stack: Any) -> dict[str, Any]:
    st = stack.api("GET", "/v1/sample/status")
    if not st.get("installed"):
        stack.api("POST", "/v1/sample/install", json={"size": "small"})
        for _ in range(240):
            st = stack.api("GET", "/v1/sample/status")
            if st.get("installed") and not st.get("running"):
                break
            time.sleep(1)
    assert st["installed"]
    sources = stack.api("GET", "/v1/intelligence/sources")["items"]
    local = next(s for s in sources if s["kind"] == "local")
    mirror = next((s for s in sources if s["kind"] == "catalog_mirror"), None) or stack.api("POST", "/v1/intelligence/sources", json={"name": "catalog", "kind": "catalog_mirror"})
    stack.api("POST", "/v1/catalog/lineage/parse", json={"workspace_id": st["workspace_id"], "default_lakehouse": "retail_lakehouse", "record": True,
                                                         "sql": "CREATE TABLE e2e_margin AS SELECT f.net_amount * 0.9 AS adjusted_revenue, f.order_id AS order_ref FROM fact_sales f"})
    _scan(stack, mirror["id"])                                               # the catalog first, so that the local source ends up owning the tables
    _scan(stack, local["id"])
    _scan(stack, local["id"], "incremental")                                 # two finished scans of one source, so that the changes page has something to compare
    fact = stack.api("GET", "/v1/intelligence/assets", params={"q": "fact_sales", "type": "table", "limit": 5})["items"]
    fact = next(a for a in fact if a["name"] == "fact_sales")
    lin = stack.api("GET", f"/v1/intelligence/assets/{fact['id']}/lineage", params={"direction": "both", "depth": 4, "level": "table"})
    visuals = stack.api("GET", "/v1/intelligence/assets", params={"q": "Revenue (card)", "type": "visual", "limit": 5})["items"]
    revenue = next((a for a in visuals if a["name"] == "Revenue (card)"), None)             # the visual of the sample's synthetic Power BI report
    return {"local": local, "mirror": mirror, "fact": fact, "lineage": lin, "ws": st["workspace_id"], "revenue": revenue}


# ------------------------------------------------------------------------------------------------------------------------------ overview
def test_overview_shows_counts_sources_gaps_and_searches(page: Any, intel: dict[str, Any]) -> None:
    open_route(page, "/intelligence")
    assert page.locator(".page-head h1").inner_text().strip() == "Intelligence"
    page.get_by_text("Assets by type").wait_for()
    tiles = page.locator(".intel-tiles .stat")
    assert tiles.count() >= 4
    assert "Assets" in tiles.first.inner_text()
    n = int("".join(c for c in tiles.first.locator(".stat-value").inner_text() if c.isdigit()))
    assert n > 50
    # each source has a traffic-light badge and the last scan time
    page.get_by_text("Sources and their last scan").wait_for()
    assert page.get_by_text("Scanned", exact=True).count() >= 2
    # honest about what it could not see
    page.get_by_text("What we could not see").wait_for()
    shot(page, "intel-overview", True)
    # quick search goes to the catalog with the query in the address
    page.get_by_placeholder("Search tables, columns, notebooks, reports by name or description").fill("fact_sales")
    page.get_by_role("button", name="Search", exact=True).click()
    page.wait_for_url("**/#/intelligence/catalog?q=fact_sales*")
    page.get_by_role("link", name="fact_sales").first.wait_for()


def test_scan_now_runs_to_the_end_and_reports(page: Any, intel: dict[str, Any]) -> None:
    open_route(page, "/intelligence")
    page.get_by_role("button", name="Scan now").first.click()
    dlg = settled(page.get_by_role("dialog"))
    dlg.get_by_text("Only what changed").wait_for()
    dlg.get_by_role("button", name="Start the scan").click()
    dlg.get_by_text("The scan finished").wait_for(timeout=120_000)
    shot(page, "intel-scan-dialog")
    dlg.get_by_role("button", name="Close", exact=True).click()
    assert page.get_by_role("dialog").count() == 0


# ------------------------------------------------------------------------------------------------------------------------------ catalog -> asset -> lineage
def test_catalog_search_to_asset_to_lineage_graph(page: Any, intel: dict[str, Any], stack: Any) -> None:
    listed = stack.api("GET", "/v1/intelligence/assets", params={"q": "fact_sales", "type": "table", "limit": 5})["items"]
    embedded = any("governance" in i for i in listed)
    per_card: list[str] = []
    page.on("request", lambda r: per_card.append(r.url) if "/v1/governance/assets/" in r.url else None)
    open_route(page, "/intelligence/catalog")
    page.get_by_placeholder("Name, path, description, alias").fill("fact_sales")
    page.get_by_role("link", name="fact_sales").first.wait_for()
    page.wait_for_url("**/#/intelligence/catalog?*q=fact_sales*")              # filters live in the address (after the typing pause)
    page.locator(".facet-types button").filter(has_text=re.compile(r"^Table")).first.click()                     # type facet
    page.wait_for_url("**type=table*")
    page.wait_for_timeout(800)
    if embedded:
        assert not per_card, per_card                                          # the list items carry their governance facts, so the cards need no call of their own
    shot(page, "intel-catalog")
    page.get_by_role("link", name="fact_sales").first.click()
    page.locator(".page-head h1").filter(has_text="fact_sales").wait_for()
    for tab in ("Overview", "Columns", "Lineage", "Impact", "Origin", "Quality", "Documentation", "History"):
        assert page.get_by_role("tab", name=tab).count() == 1, tab
    # columns with types
    page.get_by_role("tab", name="Columns").click()
    page.get_by_role("cell", name="net_amount").first.wait_for()
    # the embedded lineage explorer draws real nodes and edges
    page.get_by_role("tab", name="Lineage").click()
    svg = page.get_by_test_id("lineage-svg")
    svg.wait_for()
    page.wait_for_selector("[data-node]")
    nodes = page.locator("[data-testid=lineage-svg] [data-node]").count()
    edges = page.locator("[data-testid=lineage-svg] path.lx-edge").count()
    api_nodes, api_edges = len(intel["lineage"]["nodes"]), len(intel["lineage"]["edges"])
    assert nodes >= 3 and edges >= 2, (nodes, edges)
    assert nodes <= api_nodes + 1 and edges <= api_edges + 1
    shot(page, "intel-asset-lineage")


def test_lineage_explorer_upstream_evidence_keyboard_table_and_export(page: Any, intel: dict[str, Any]) -> None:
    fid = intel["fact"]["id"]
    ups = [n for n in intel["lineage"]["nodes"] if n.get("direction") == "upstream"]
    assert ups, "the sample has something upstream of fact_sales (the generator script that wrote it)"
    open_route(page, f"/intelligence/lineage/{fid}", "[data-testid=lineage-svg]")
    page.wait_for_selector("[data-node]")
    summary = page.get_by_test_id("lineage-summary").inner_text()
    assert "connection" in summary and "asset" in summary
    # legend of confidence styles
    for word in ("Verified", "Derived", "Inferred", "Unknown"):
        assert page.get_by_text(word, exact=True).count() >= 1
    # direction: upstream only keeps the upstream path and still contains the root
    page.get_by_role("button", name="Upstream", exact=True).click()
    page.wait_for_timeout(600)
    names = page.locator("[data-testid=lineage-svg] [data-node]").evaluate_all("els => els.map(e => e.textContent || '')")
    assert any("fact_sales" in n for n in names)
    assert any(ups[0]["name"] in n for n in names), (ups[0]["name"], names)
    assert "direction=up" in page.url
    page.get_by_role("button", name="Both", exact=True).click()
    # a click on a node opens the side panel with evidence of the connections (kind, source, location)
    page.locator("[data-node]").filter(has_text="fact_sales").first.click()
    panel = page.get_by_test_id("lineage-panel")
    panel.wait_for()
    assert "fact_sales" in panel.inner_text()
    page.get_by_test_id("edge-evidence").first.wait_for()
    page.wait_for_timeout(500)                                                 # let the address update and the picture settle before typing
    # keyboard: the focused node moves with the arrow keys and Escape clears the selection
    page.locator("[data-node]").filter(has_text="fact_sales").first.focus()
    page.wait_for_function("document.activeElement && document.activeElement.hasAttribute('data-node')")
    before = page.evaluate("document.activeElement.getAttribute('data-node')")
    page.keyboard.press("ArrowRight")
    page.wait_for_function("(b) => document.activeElement && document.activeElement.getAttribute('data-node') && document.activeElement.getAttribute('data-node') !== b", arg=before)
    page.keyboard.press("ArrowLeft")
    page.wait_for_function("(b) => document.activeElement && document.activeElement.getAttribute('data-node') === b", arg=before)
    page.keyboard.press("Escape")
    # search in the graph highlights matches
    page.get_by_label("Find in the picture").fill("revenue")
    assert page.locator(".lx-node.match").count() >= 1
    page.get_by_label("Find in the picture").fill("")
    # depth slider is keyboard operable and changes the address
    page.get_by_label("How many steps", exact=False).focus()
    page.keyboard.press("ArrowLeft")
    page.wait_for_timeout(700)
    # the table view is the text alternative: same information as a table
    page.get_by_role("button", name="Table", exact=True).last.click()
    page.locator("[data-testid=lineage-explorer] table").first.wait_for()
    assert page.locator("[data-testid=lineage-explorer] table tbody tr").count() >= 2
    page.get_by_role("button", name="Picture", exact=True).click()
    # export JSON downloads exactly what is on screen
    page.get_by_role("button", name="Export", exact=True).click()
    with page.expect_download() as dl:
        page.get_by_role("menuitem", name="Data (JSON)").click()
    data = json.loads(Path(dl.value.path()).read_text())
    assert data["root"]["id"] == fid and len(data["nodes"]) >= 3 and len(data["edges"]) >= 2
    with page.expect_download() as dl:
        page.get_by_role("button", name="Export", exact=True).click()
        page.get_by_role("menuitem", name="Picture (SVG)").click()
    svg = Path(dl.value.path()).read_text()
    assert svg.startswith("<svg") and "fact_sales" in svg and "stroke-dasharray" in svg
    shot(page, "intel-lineage")


def test_hero_where_does_the_revenue_number_come_from(page: Any, intel: dict[str, Any]) -> None:
    rev = intel["revenue"]
    assert rev, "the sample installs a synthetic Power BI report with a Revenue (card) visual"
    # the sentences: one route per line, visual <- measure <- model table <- gold table <- silver table <- notebook <- raw files
    open_route(page, f"/intelligence/assets/{rev['id']}?tab=origin")
    route = page.get_by_test_id("origin-path").first
    route.wait_for()
    page.wait_for_timeout(600)
    routes = page.get_by_test_id("origin-path").all_inner_texts()
    text = next((r for r in routes if "Files/raw" in r and "fact_sales" in r), "")                       # at least one route is the whole way back to the raw files
    for want in ("Files/raw", "fact_sales", "01_Retail_Medallion_ETL", "gold_daily_sales", "Daily Sales", "Total Revenue", "Revenue (card)"):
        assert want in text, (want, text)
    assert re.search(r"Verified|Derived|Inferred", page.get_by_test_id("origin").inner_text())              # each route says how sure we are
    shot(page, "intel-hero-origin")
    # the picture: upstream only, opened already placed in the window around the visual, with its line styles (solid verified, dashed derived, dotted inferred)
    open_route(page, f"/intelligence/lineage/{rev['id']}?direction=up&depth=8", "[data-testid=lineage-svg]")
    page.wait_for_selector("[data-node]")
    page.wait_for_timeout(900)
    names = page.locator("[data-testid=lineage-svg] [data-node]").evaluate_all("els => els.map(e => e.textContent)")
    for want in ("Revenue (card)", "Total Revenue", "Daily Sales"):
        assert any(want in n for n in names), (want, names)
    fit = page.evaluate(FIT_JS)
    assert fit["rootInside"] and (not fit["outside"] or fit["hint"]), fit
    assert page.locator("[data-testid=lineage-svg] path.lx-edge").count() >= 3
    shot(page, "intel-hero-lineage")


def test_dense_lineage_opens_fitted_or_around_the_starting_asset(page: Any, intel: dict[str, Any]) -> None:
    open_route(page, f"/intelligence/lineage/{intel['fact']['id']}", "[data-testid=lineage-svg]")
    page.wait_for_selector("[data-node]")
    page.wait_for_timeout(900)
    fit = page.evaluate(FIT_JS)
    assert fit["rootInside"], fit
    assert not fit["outside"] or fit["hint"], fit                              # either everything is in view, or the page says that only the area around the asset is
    page.get_by_role("button", name="Fit to window").click()
    assert page.evaluate(FIT_JS)["rootInside"]
    # changing the direction fits the new answer too
    page.get_by_role("button", name="Upstream", exact=True).click()
    page.wait_for_timeout(1200)
    fit = page.evaluate(FIT_JS)
    assert fit["rootInside"] and (not fit["outside"] or fit["hint"]), fit


def test_column_lineage_and_impact_page(page: Any, intel: dict[str, Any]) -> None:
    fid = intel["fact"]["id"]
    open_route(page, f"/intelligence/lineage/{fid}?level=column", "[data-testid=column-lineage], [data-testid=lineage-explorer]")
    page.get_by_text("have a recorded mapping").first.wait_for(timeout=30_000)
    shot(page, "intel-column-lineage")
    open_route(page, f"/intelligence/impact/{fid}")
    page.get_by_test_id("impact").wait_for()
    assert "depend" in page.get_by_test_id("impact-summary").inner_text()
    page.get_by_test_id("staleness").wait_for()
    shot(page, "intel-impact", True)


# ------------------------------------------------------------------------------------------------------------------------------ discovery
def test_discovery_test_connectivity_shows_steps(page: Any, intel: dict[str, Any]) -> None:
    open_route(page, "/intelligence/discovery")
    row = page.get_by_test_id("source-local")
    row.get_by_role("button", name="Test connectivity").click()
    res = row.get_by_test_id("test-result")
    res.wait_for()
    assert res.locator("li.step").count() >= 3
    assert "Every step worked" in res.inner_text()
    shot(page, "intel-discovery-test")


def test_discovery_fabric_source_form_never_shows_secrets_and_tests_stepwise(page: Any, intel: dict[str, Any]) -> None:
    open_route(page, "/intelligence/discovery")
    page.get_by_role("button", name="Add a source").click()
    dlg = settled(page.get_by_role("dialog"))
    # validation first: nothing is sent while the form is incomplete
    dlg.get_by_role("button", name="Test connectivity").click()
    assert dlg.get_by_role("alert").count() >= 1
    dlg.get_by_label(re.compile(r"^Name\b")).fill("fabric-e2e")
    dlg.get_by_label("Tenant id").first.fill("contoso.onmicrosoft.com")
    dlg.get_by_label("Client id").first.fill("11111111-1111-4111-8111-111111111111")
    secret = dlg.get_by_label("Client secret").first
    assert secret.get_attribute("type") == "password"
    secret.fill("not-a-real-secret-value")
    dlg.get_by_label("Use a separate identity for reading sample rows").check()
    assert dlg.get_by_label("Client secret").count() == 2                      # a second, separate secret for the optional sampling identity
    dlg.get_by_label("Client id").nth(1).fill("22222222-2222-4222-8222-222222222222")
    dlg.get_by_label("Client secret").nth(1).fill("another-secret-value")
    dlg.get_by_role("button", name="Test connectivity").click()
    res = dlg.get_by_test_id("test-result")
    res.wait_for(timeout=60_000)
    assert res.locator("li.step").count() >= 4
    assert "resolve credentials" in res.inner_text()                           # the first step needs no network and passes; later steps need Microsoft and fail or are skipped
    assert "not-a-real-secret-value" not in res.inner_text()
    res.scroll_into_view_if_needed()
    shot(page, "intel-discovery-fabric")
    dlg.get_by_role("button", name="Add source").click()
    page.get_by_test_id("source-fabric-e2e").wait_for()
    row = page.get_by_test_id("source-fabric-e2e")
    assert "secret stored" in row.inner_text()
    assert "not-a-real-secret-value" not in page.content() and "another-secret-value" not in page.content()
    # editing never shows the stored secret; the field is empty and says so
    row.get_by_role("button", name="Edit").click()
    dlg = settled(page.get_by_role("dialog"))
    assert dlg.get_by_label("Client secret").first.input_value() == ""
    assert "never shown" in dlg.inner_text()
    dlg.get_by_role("button", name="Cancel").click()
    # delete it again (a source that cannot be reached must not keep failing in the background of the other tests)
    row.get_by_role("button", name="Delete fabric-e2e").click()
    page.get_by_role("dialog").get_by_role("button", name="Delete the source").click()
    page.get_by_test_id("source-fabric-e2e").wait_for(state="detached")


def test_discovery_gaps_coverage_scans_and_changes(page: Any, intel: dict[str, Any]) -> None:
    open_route(page, "/intelligence/discovery?tab=scans")
    page.locator("[data-testid=scans] table tbody tr").first.wait_for()
    assert page.locator("[data-testid=scans] table tbody tr").count() >= 2
    page.locator("[data-testid=scans] table tbody tr").first.click()
    d = page.get_by_test_id("scan-detail")
    d.wait_for()
    assert "What it found" in d.inner_text() and "could not see" in d.inner_text()
    page.keyboard.press("Escape")
    # gaps: kinds, remediation text, open/resolved
    open_route(page, "/intelligence/discovery?tab=gaps")
    gaps = page.get_by_test_id("gaps")
    gaps.get_by_text("Gaps are part of the answer").wait_for()
    gaps.locator(".gap-item").first.wait_for()
    assert gaps.locator(".gap-item").count() >= 1
    assert "What to do" in gaps.inner_text()
    shot(page, "intel-gaps")
    gaps.get_by_label("Kind").select_option(index=1)
    page.wait_for_timeout(500)
    gaps.get_by_role("button", name="Resolved", exact=True).click()
    page.wait_for_timeout(500)
    # coverage: honest bars and the list of what is outside the picture
    open_route(page, "/intelligence/discovery?tab=coverage")
    cov = page.get_by_test_id("coverage")
    cov.get_by_text("What we could NOT see").first.wait_for()
    assert cov.get_by_text("Always outside the picture").count() == 1
    assert cov.locator("[role=meter], .cov-bar").count() >= 3
    cov.get_by_role("button", name="Measure now").click()
    cov.get_by_test_id("lineage-coverage").wait_for(timeout=120_000)
    assert "Columns with a recorded mapping" in cov.inner_text()
    shot(page, "intel-coverage", True)
    # changes between two scans
    open_route(page, "/intelligence/discovery?tab=snapshots")
    snaps = page.get_by_test_id("snapshots")
    snaps.get_by_label("Earlier scan").wait_for()
    snaps.get_by_text("Assets", exact=True).first.wait_for(timeout=15_000)


def test_discovery_import_scanner_file_and_sampling_policy_lifecycle(page: Any, intel: dict[str, Any], stack: Any, tmp_path: Path) -> None:
    from tests.fabric.fabric_fakes import FABRIC_WS, pbi_dataset, pbi_report, pbi_table, scanner_workspace
    ds = pbi_dataset("d1d1d1d1-0000-4000-8000-0000000000e2", "E2E model", [pbi_table("e2e_sales", [("id", "Int64"), ("amount", "Double")])], datasource_ids=[])
    doc = {"workspaces": [scanner_workspace(FABRIC_WS, "E2E", datasets=[ds], reports=[pbi_report("e1e1e1e1-0000-4000-8000-0000000000e2", "E2E report", "d1d1d1d1-0000-4000-8000-0000000000e2")])],
           "datasourceInstances": [], "misconfiguredDatasourceInstances": []}
    f = tmp_path / "e2e-scanner.json"
    f.write_text(json.dumps(doc))
    open_route(page, "/intelligence/discovery?tab=import")
    imp = page.get_by_test_id("import")
    imp.get_by_text("50 MB").first.wait_for()
    assert imp.get_by_role("button", name="Import and scan").is_disabled()
    page.set_input_files("#scanner-file", str(f))
    imp.get_by_role("button", name="Import and scan").click()
    imp.get_by_text("The scan finished").wait_for(timeout=60_000)
    shot(page, "intel-import")
    # a file that is not JSON is refused with a readable message, before anything is stored
    bad = tmp_path / "bad.json"
    bad.write_text("this is not json")
    page.set_input_files("#scanner-file", str(bad))
    imp.get_by_role("button", name="Import and scan").click()
    imp.get_by_text("not valid JSON").wait_for()
    # sampling policies: off by default, with plain warnings when it is switched on
    open_route(page, "/intelligence/discovery?tab=sampling")
    s = page.get_by_test_id("sampling")
    s.get_by_text("switched off everywhere").wait_for()
    shot(page, "intel-sampling-off")
    s.get_by_role("button", name="Add a policy").click()
    dlg = settled(page.get_by_role("dialog"))
    dlg.get_by_label("Applies to").select_option("global")
    dlg.locator("#pol-rows").fill("3")
    assert "real data" in dlg.get_by_test_id("policy-dialog").inner_text()
    dlg.get_by_role("button", name="Save the policy").click()
    page.get_by_role("dialog").last.get_by_role("button", name="Allow it").click()
    s.get_by_text("switched ON").wait_for()
    pid = next(p["id"] for p in stack.api("GET", "/v1/intelligence/sampling-policies")["items"] if p["scope_type"] == "global")
    try:
        # the asset's Sample tab now fetches masked rows on request and keeps them out of browser storage
        open_route(page, f"/intelligence/assets/{intel['fact']['id']}?tab=sample")
        page.get_by_role("button", name="Fetch sample rows").click()
        rows = page.get_by_test_id("sample-rows")
        rows.wait_for()
        assert rows.locator("tbody tr").count() <= 3
        cell = rows.locator("tbody tr td").first.inner_text()
        assert cell
        shot(page, "intel-sample-on")
        stored = page.evaluate("JSON.stringify([Object.entries(localStorage), Object.entries(sessionStorage)])")
        assert cell not in stored or len(cell) < 3
        page.get_by_role("button", name="Hide the values").click()
        assert page.get_by_test_id("sample-rows").count() == 0
    finally:
        stack.api("DELETE", f"/v1/intelligence/sampling-policies/{pid}")
    # and with the policy gone the tab explains that it is off
    open_route(page, f"/intelligence/assets/{intel['fact']['id']}?tab=sample")
    page.reload()                                                              # the same address as before: reload so that the policy is asked again
    page.get_by_test_id("sample-off").wait_for()
    assert "switched off" in page.get_by_test_id("sample-off").inner_text()
    # clean up the imported source
    open_route(page, "/intelligence/discovery")
    page.get_by_test_id("source-e2e-scanner").get_by_role("button", name="Delete e2e-scanner").click()
    page.get_by_role("dialog").get_by_role("button", name="Delete the source").click()
    page.get_by_test_id("source-e2e-scanner").wait_for(state="detached")


# ------------------------------------------------------------------------------------------------------------------------------ governance
def _has_governance(stack: Any) -> bool:
    return any(p.startswith("/v1/governance/") for p in stack.api("GET", "/v1/openapi.json")["paths"])


def test_governance_pages_glossary_review_and_policies(page: Any, intel: dict[str, Any], stack: Any) -> None:
    if not _has_governance(stack):
        pytest.skip("the governance module is not installed")
    stack.api("POST", "/v1/governance/classifications/scan", json={})
    open_route(page, "/intelligence/governance")
    page.get_by_test_id("gov-overview").wait_for()
    for tab in ("Overview", "Glossary", "Domains and products", "Classifications", "Review queue", "Requests", "Policies and violations", "Exposure", "Owners"):
        assert page.get_by_role("tab", name=tab).count() == 1, tab
    shot(page, "intel-governance", True)
    # glossary: add a term and see it
    page.get_by_role("tab", name="Glossary").click()
    page.get_by_role("button", name="New term").click()
    dlg = settled(page.get_by_role("dialog"))
    dlg.get_by_label(re.compile(r"^Name\b")).fill("E2E term")
    dlg.get_by_label("What it means").fill("Made up by the browser test.")
    dlg.get_by_role("button", name="Save").click()
    page.get_by_test_id("term-detail").get_by_text("E2E term").first.wait_for()
    page.get_by_role("button", name="Edit", exact=True).click()
    settled(page.get_by_role("dialog"))
    page.get_by_role("dialog").get_by_label("Other names (comma separated)").fill("e2e, test term")
    page.get_by_role("dialog").get_by_role("button", name="Save").click()
    page.get_by_text("Also called: e2e, test term").wait_for()
    shot(page, "intel-glossary")
    # review queue: approve one suggestion with a comment
    page.get_by_role("tab", name="Review queue").click()
    review = page.get_by_test_id("review")
    review.get_by_text("Nothing here is a fact yet").wait_for()
    if review.get_by_test_id("queue-item").count():
        review.get_by_test_id("queue-item").first.get_by_role("button", name="Approve").click()
        settled(page.get_by_role("dialog")).get_by_label("Comment (optional)").fill("checked by the e2e test")
        page.get_by_role("dialog").get_by_role("button", name="Approve").click()
        page.get_by_text("Approved").first.wait_for()
    shot(page, "intel-review")
    # policies report, they do not block
    page.get_by_role("tab", name="Policies and violations").click()
    page.get_by_text("Policies report, they do not block").wait_for()
    page.get_by_role("button", name="Add a policy").click()
    dlg = settled(page.get_by_role("dialog"))
    dlg.get_by_label(re.compile(r"^Name\b")).fill("E2E owners")
    dlg.get_by_label("The rule").select_option("required_owner")
    dlg.get_by_role("button", name="Preview what it would report").click()
    dlg.get_by_test_id("policy-preview").wait_for()
    assert "Nothing was stored" in dlg.get_by_test_id("policy-preview").inner_text()
    dlg.get_by_role("button", name="Save").click()
    page.get_by_role("tab", name="Policies and violations").wait_for()
    page.get_by_role("button", name="Policies", exact=True).click()
    page.get_by_text("E2E owners").first.wait_for()
    shot(page, "intel-policies")
    # exposure explorer
    page.get_by_role("tab", name="Exposure").click()
    page.get_by_label("Label").select_option(label="PII")
    page.get_by_text("Where the label is").wait_for()
    shot(page, "intel-exposure")
    # the asset's Governance tab
    open_route(page, f"/intelligence/assets/{intel['fact']['id']}?tab=governance")
    page.get_by_test_id("asset-governance").wait_for()
    page.get_by_test_id("certification").wait_for()
    shot(page, "intel-asset-governance", True)


# ------------------------------------------------------------------------------------------------------------------------------ accessibility smoke
A11Y_JS = """() => {
  const bad = [];
  const name = (el) => (el.getAttribute('aria-label') || el.getAttribute('aria-labelledby') || el.getAttribute('title') || el.textContent || '').trim();
  document.querySelectorAll('button, [role=button], a[href]').forEach((el) => { if (el.closest('[aria-hidden=true]')) return; if (!name(el) && !el.querySelector('img[alt]')) bad.push('unnamed ' + el.tagName + ' ' + el.outerHTML.slice(0, 90)); });
  document.querySelectorAll('input:not([type=hidden]), select, textarea').forEach((el) => {
    const id = el.id; const lab = id && document.querySelector('label[for="' + id + '"]');
    if (!lab && !el.getAttribute('aria-label') && !el.getAttribute('aria-labelledby') && !el.closest('label')) bad.push('unlabelled ' + el.outerHTML.slice(0, 90));
  });
  const ids = {}; document.querySelectorAll('[id]').forEach((el) => { ids[el.id] = (ids[el.id] || 0) + 1; });
  Object.entries(ids).forEach(([k, v]) => { if (v > 1) bad.push('duplicate id ' + k); });
  if (document.querySelectorAll('h1').length !== 1) bad.push('h1 count ' + document.querySelectorAll('h1').length);
  document.querySelectorAll('img:not([alt])').forEach((el) => bad.push('img without alt'));
  const main = document.querySelector('.main');
  if (main && main.scrollWidth > main.clientWidth + 1) bad.push('horizontal scroll in the page ' + main.scrollWidth + ' > ' + main.clientWidth);
  return bad;
}"""


@pytest.mark.parametrize("dark", [False, True])
def test_accessibility_smoke_of_every_intelligence_page(new_page: Any, intel: dict[str, Any], stack: Any, dark: bool) -> None:
    pg = new_page(dark=dark)
    routes = [*DATA_PAGES, f"/intelligence/assets/{intel['fact']['id']}", f"/intelligence/assets/{intel['fact']['id']}?tab=columns", f"/intelligence/impact/{intel['fact']['id']}"]
    if _has_governance(stack):
        routes += ["/intelligence/governance", "/intelligence/governance/glossary", "/intelligence/governance/policies"]
    for r in routes:
        open_route(pg, r)
        pg.wait_for_timeout(1200)
        bad = pg.evaluate(A11Y_JS)
        assert not bad, f"{r} ({'dark' if dark else 'light'}): {bad}"
        # the first Tab stop is a real, visible control and focus is visible
        pg.keyboard.press("Tab")
        assert pg.evaluate("document.activeElement && document.activeElement !== document.body")
    # lineage graph: the picture has a text equivalent and keyboard help
    open_route(pg, f"/intelligence/lineage/{intel['fact']['id']}", "[data-testid=lineage-svg]")
    assert pg.locator("[data-testid=lineage-svg]").get_attribute("aria-label")
    assert pg.get_by_test_id("lineage-summary").inner_text()
    assert pg.locator("[data-node][tabindex='0']").count() == 1                  # roving tabindex: one stop in the picture, arrows inside it
    for width in (390, 768):
        pg.set_viewport_size({"width": width, "height": 800})
        # the whole document (not just the page area) must fit: the top bar used to push every page sideways on a phone; "/" and "/jobs" are not Intelligence pages
        for r in ("/", "/jobs", "/intelligence", "/intelligence/catalog", "/intelligence/discovery", f"/intelligence/assets/{intel['fact']['id']}", "/intelligence/governance"):
            open_route(pg, r)
            pg.wait_for_timeout(600)
            assert pg.evaluate("document.scrollingElement.scrollWidth <= window.innerWidth"), f"{r} scrolls sideways at {width}px: {pg.evaluate('document.scrollingElement.scrollWidth')}"
            if r.startswith("/intelligence"):
                assert pg.evaluate("(() => { const m = document.querySelector('.main'); return m.scrollWidth <= m.clientWidth + 1; })()"), f"{r} overflows its page area at {width}px"
    if not dark:
        shot(pg, "intel-phone")
