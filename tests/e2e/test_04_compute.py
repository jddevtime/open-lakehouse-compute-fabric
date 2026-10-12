# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Compute control panel against three real worker agents: fleet map, node tests, cluster matrix, cordon / uncordon / drain, enrollment wizard."""

from __future__ import annotations

import re

from playwright.sync_api import expect


def _nodes(api):
    return {w["name"]: w for w in api("GET", "/v1/workers")["items"]}


def _card(page, name):
    return page.locator(f"article[aria-label^='Node {name},']")


def test_fleet_shows_three_online_nodes(page, goto, api, snap):
    goto(page, "/compute")
    page.get_by_role("tab", name=re.compile("^Machines")).wait_for()
    names = {"node-1", "node-2", "node-3"}
    for n in names:
        expect(page.get_by_role("button", name=f"Actions for {n}")).to_be_visible()
    summary = page.get_by_label("Fleet summary")
    expect(summary).to_contain_text("Nodes online")
    expect(summary).to_contain_text(re.compile(r"3\s*/\s*3"))
    # live numbers on the page agree with the API
    topo = api("GET", "/v1/compute/topology")
    assert topo["summary"]["nodes_online"] >= 3
    assert {n["name"] for n in topo["nodes"]} >= names
    # table view lists the same machines, search narrows it
    page.get_by_role("button", name="Table").click()
    expect(page.locator("table tbody tr")).to_have_count(3)
    page.get_by_label("Search nodes").fill("node-2")
    expect(page.locator("table tbody tr")).to_have_count(1)
    page.get_by_label("Search nodes").fill("")
    page.get_by_role("button", name="Cards").click()
    snap(page, "compute-light")


def test_node_connectivity_test(page, goto):
    goto(page, "/compute")
    page.get_by_role("button", name="Actions for node-1").wait_for()
    _card(page, "node-1").get_by_role("button", name="Test", exact=True).click()
    dlg = page.get_by_role("dialog")
    expect(dlg).to_contain_text("Test connectivity: node-1")
    expect(dlg).to_contain_text(re.compile("Every check passed|checks failed"), timeout=60_000)
    expect(dlg).to_contain_text("Every check passed")
    page.keyboard.press("Escape")
    expect(dlg).to_have_count(0)


def test_cluster_matrix(page, goto):
    goto(page, "/compute")
    page.get_by_role("button", name="Test all nodes").click()
    dlg = page.get_by_role("dialog")
    expect(dlg).to_contain_text("Cluster-wide connectivity test")
    expect(dlg.locator("table.matrix")).to_be_visible(timeout=90_000)
    rows = dlg.locator("table.matrix tbody tr")
    expect(rows).to_have_count(3)
    for n in ("node-1", "node-2", "node-3"):
        expect(dlg.locator("table.matrix").get_by_role("rowheader", name=n)).to_be_visible()
    expect(dlg).to_contain_text("All nodes passed every check")


def test_cordon_uncordon_drain(page, goto, api):
    goto(page, "/compute")
    btn = page.get_by_role("button", name="Actions for node-3")
    btn.wait_for()

    def menu(label):
        page.get_by_role("button", name="Actions for node-3").click()
        page.get_by_role("menuitem", name=re.compile(label)).click()

    menu("Cordon")
    page.get_by_role("dialog").get_by_role("button", name="Cordon", exact=True).click()
    for _ in range(40):
        if _nodes(api)["node-3"]["scheduling"] == "cordoned":
            break
        page.wait_for_timeout(250)
    assert _nodes(api)["node-3"]["scheduling"] == "cordoned"
    expect(_card(page, "node-3")).to_contain_text("Cordoned")
    menu("Uncordon")
    for _ in range(40):
        if _nodes(api)["node-3"]["scheduling"] == "active":
            break
        page.wait_for_timeout(250)
    assert _nodes(api)["node-3"]["scheduling"] == "active"
    menu("Drain")
    page.get_by_role("dialog").get_by_role("button", name="Drain", exact=True).click()
    for _ in range(40):
        if _nodes(api)["node-3"]["scheduling"] in ("draining", "cordoned"):
            break
        page.wait_for_timeout(250)
    assert _nodes(api)["node-3"]["scheduling"] == "draining"
    # leave the shared stack as we found it
    api("POST", f"/v1/workers/{_nodes(api)['node-3']['id']}/uncordon", json={"reason": "e2e cleanup"})
    assert _nodes(api)["node-3"]["scheduling"] == "active"


def test_enrollment_wizard_and_details(page, goto, api):
    goto(page, "/compute")
    page.get_by_role("button", name="Add a machine").click()
    dlg = page.get_by_role("dialog")
    dlg.get_by_label("Node name").fill("e2e-new-node")
    dlg.get_by_role("button", name="Create enrollment token").click()
    expect(dlg).to_contain_text("Token expires")
    expect(dlg.locator("pre, code").first).to_contain_text("e2e-new-node")
    page.keyboard.press("Escape")
    expect(page.get_by_role("dialog")).to_have_count(0)
    # details drawer of a node
    _card(page, "node-2").get_by_role("button", name="Details").click()
    expect(page.get_by_role("dialog")).to_contain_text("node-2")
    page.wait_for_timeout(400)
    page.keyboard.press("Escape")
    expect(page.get_by_role("dialog")).to_have_count(0)
    # other tabs render real data
    for tab in ("Spark clusters", "Control plane", "Queues and pools", "Runtimes"):
        t = page.get_by_role("tab", name=re.compile(tab))
        t.click()
        expect(t).to_have_attribute("aria-selected", "true")
        expect(page.locator("[role=tabpanel]:visible").first).to_be_visible()
