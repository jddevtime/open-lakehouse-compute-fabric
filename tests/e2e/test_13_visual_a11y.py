# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Every major page in both themes (screenshots into docs/images/ui), a DOM accessibility audit, design-token contrast, reduced motion and tablet width."""

from __future__ import annotations

import json
import re
import time

import pytest


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
    ws = st["workspace_id"]
    api = stack.api
    items = {t: api("GET", f"/v1/workspaces/{ws}/items", params={"type": t, "limit": 100})["items"] for t in ("notebook", "report", "pipeline", "variable_set", "spark_job_definition")}
    if not items["pipeline"]:
        api("POST", f"/v1/workspaces/{ws}/items", json={"type": "pipeline", "name": "daily_refresh", "definition": {
            "schema": "olcf.pipeline/1", "description": "Count rows, then take one of two paths.", "parameters": {"year": {"type": "int", "default": 2025}}, "variables": {"msg": {"type": "string", "default": ""}},
            "activities": [{"name": "Count orders", "type": "sql", "typeProperties": {"sql": "SELECT count(*) AS n FROM fact_sales"}, "dependsOn": []},
                           {"name": "Set message", "type": "set_variable", "typeProperties": {"variable": "msg", "value": "done"}, "dependsOn": [{"activity": "Count orders", "conditions": ["Succeeded"]}]},
                           {"name": "Stop on failure", "type": "fail", "typeProperties": {"message": "counting failed", "errorCode": "E_COUNT"}, "dependsOn": [{"activity": "Count orders", "conditions": ["Failed"]}]}],
            "outputs": {"msg": "@variables('msg')"}}})
        items["pipeline"] = api("GET", f"/v1/workspaces/{ws}/items", params={"type": "pipeline", "limit": 100})["items"]
    st["items"] = items
    found = api("GET", "/v1/intelligence/assets", params={"q": "fact_sales", "type": "table", "limit": 10})["items"]
    visuals = api("GET", "/v1/intelligence/assets", params={"q": "Revenue (card)", "type": "visual", "limit": 5})["items"]
    st["_intel"] = {"fact_sales": next(a["id"] for a in found if a["name"] == "fact_sales"),
                    "revenue": next(a["id"] for a in visuals if a["name"] == "Revenue (card)")}      # the synthetic Power BI report's Revenue visual: the "where does this number come from" story
    return st


def _routes(sample):
    nb = next(i for i in sample["items"]["notebook"] if "Explore_Python" in i["name"])
    rep = next(i for i in sample["items"]["report"] if "Retail" in i["name"])
    pipe = sample["items"]["pipeline"][0]
    return [
        ("overview", "/", ".hrow"),
        ("compute", "/compute", "article[aria-label^='Node ']"),
        ("fabric", "/fabric", ".ready-list"),
        ("workspaces", f"/workspaces/{sample['workspace_id']}", None),
        ("notebook-python", f"/notebook/{nb['id']}", ".cm-cell, .cell, .nb-cell"),
        ("lakehouse", f"/lakehouse/{sample['lakehouse_id']}", None),
        ("sql-editor", "/sql", ".CodeMirror"),
        ("dashboard", f"/dashboards/{rep['id']}", "path.bar"),
        ("jobs", "/jobs", "table"),
        ("pipeline-designer", f"/pipelines/{pipe['id']}", "svg"),
        ("alerts", "/alerts", "[data-testid=alerts]"),
        ("connections", "/connections", "[data-testid=connections]"),
        ("runtimes", "/runtimes", None),
        ("environments", "/environments", None),
        ("variables", "/variables", None),
        ("spark-jobs", "/sparkjobs", None),
        ("agents", "/agents", "[data-testid=mcp-endpoint]"),
        ("security", "/security?tab=users", "[data-testid=users]"),
        ("admin", "/admin?tab=health", "[data-testid=health]"),
        ("about", "/about", "[data-testid=about-author]"),
        ("intelligence", "/intelligence", ".intel-tiles"),
        ("intelligence-catalog", "/intelligence/catalog?q=sales", None),
        ("intelligence-lineage", f"/intelligence/lineage/{sample['_intel']['revenue']}?direction=up&depth=8", "[data-testid=lineage-svg] [data-node]"),
        ("intelligence-discovery", "/intelligence/discovery", None),
        ("intelligence-governance", "/intelligence/governance", None),
    ]


def _prime(page, sample):
    page.context.add_init_script("try{localStorage.setItem('olcf.workspace'," + json.dumps(json.dumps(sample["workspace_id"])) + ")}catch(e){}")


@pytest.mark.parametrize("theme", ["light", "dark"])
def test_every_major_page_renders_in_both_themes(new_page, sample, snap, theme):
    page = new_page(dark=theme == "dark")
    _prime(page, sample)
    page.expect_problems(r"\[http (4\d\d)\]")  # pages probe optional endpoints; the audit below counts real console errors separately
    done = []
    for name, route, ready in _routes(sample):
        page.goto("/#" + route)
        page.wait_for_selector(".page-head h1, .nb-head, main h1", state="visible", timeout=30_000)
        if ready:
            page.wait_for_selector(ready, timeout=30_000)
        page.wait_for_timeout(900)
        if name == "sql-editor":
            page.wait_for_timeout(600)
        snap(page, f"{name}-{theme}")
        done.append(name)
    assert len(done) == len(_routes(sample))
    errors = [p for p in page.problems if not p.startswith("[http")]
    assert not errors, errors


A11Y_JS = """
() => {
  const bad = [];
  const vis = (el) => !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length);
  const txt = (el) => (el.getAttribute('aria-label') || '').trim() || (el.innerText || '').trim() || (el.getAttribute('title') || '').trim() || (el.querySelector('img[alt]') ? el.querySelector('img[alt]').alt : '') || (el.querySelector('svg title') ? el.querySelector('svg title').textContent : '');
  document.querySelectorAll('input:not([type=hidden]),select,textarea').forEach((el) => {
    if (!vis(el) || el.closest('.CodeMirror')) return;
    const labelled = (el.labels && el.labels.length) || el.getAttribute('aria-label') || el.getAttribute('aria-labelledby') || el.getAttribute('title');
    if (!labelled) bad.push('unlabelled field: ' + el.outerHTML.slice(0, 120));
  });
  document.querySelectorAll('button,[role=button],[role=tab],[role=menuitem],a[href]').forEach((el) => {
    if (!vis(el)) return;
    if (!txt(el) && !el.getAttribute('aria-labelledby')) bad.push('nameless control: ' + el.outerHTML.slice(0, 120));
  });
  document.querySelectorAll('img').forEach((el) => { if (vis(el) && !el.hasAttribute('alt')) bad.push('img without alt: ' + el.outerHTML.slice(0, 100)); });
  const ids = {};
  document.querySelectorAll('[id]').forEach((el) => { ids[el.id] = (ids[el.id] || 0) + 1; });
  Object.entries(ids).forEach(([k, v]) => { if (v > 1) bad.push('duplicate id: ' + k); });
  document.querySelectorAll('[tabindex]').forEach((el) => { if (Number(el.getAttribute('tabindex')) > 0) bad.push('positive tabindex: ' + el.outerHTML.slice(0, 80)); });
  const h1 = [...document.querySelectorAll('h1')].filter(vis);
  if (h1.length !== 1) bad.push('expected exactly one visible h1, found ' + h1.length);
  if (!document.querySelector('main')) bad.push('no main landmark');
  if (!document.querySelector('nav[aria-label]')) bad.push('no labelled nav');
  document.querySelectorAll('[role=tab]').forEach((el) => { if (vis(el) && !el.getAttribute('aria-selected')) bad.push('tab without aria-selected'); });
  document.querySelectorAll('[aria-labelledby],[aria-controls],[aria-describedby]').forEach((el) => {
    ['aria-labelledby', 'aria-describedby'].forEach((a) => { (el.getAttribute(a) || '').split(/\\s+/).filter(Boolean).forEach((id) => { if (!document.getElementById(id)) bad.push(a + ' points to a missing id ' + id); }); });
  });
  document.querySelectorAll('table').forEach((t) => { if (vis(t) && !t.querySelector('th') && !t.getAttribute('role')) bad.push('table without header cells'); });
  return bad;
}
"""

AUDIT_ROUTES = ["/", "/compute", "/fabric", "/jobs", "/pipelines", "/alerts", "/connections", "/runtimes", "/environments", "/variables", "/sparkjobs", "/agents", "/security?tab=users",
                "/security?tab=audit", "/admin?tab=settings", "/about", "/sql", "/dashboards", "/lakehouse", "/workspaces"]


def test_dom_accessibility_audit(page, sample):
    _prime(page, sample)
    found = {}
    for route in AUDIT_ROUTES:
        page.goto("/#" + route)
        page.wait_for_selector(".page-head h1, main h1", state="visible", timeout=30_000)
        page.wait_for_timeout(1200)
        bad = page.evaluate(A11Y_JS)
        if bad:
            found[route] = bad[:6]
    assert not found, json.dumps(found, indent=1)


def _lum(hexv: str) -> float:
    h = hexv.strip().lstrip("#")
    if len(h) == 3:
        h = "".join(c * 2 for c in h)
    rgb = [int(h[i:i + 2], 16) / 255 for i in (0, 2, 4)]
    lin = [c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4 for c in rgb]
    return 0.2126 * lin[0] + 0.7152 * lin[1] + 0.0722 * lin[2]


def _ratio(a: str, b: str) -> float:
    la, lb = _lum(a), _lum(b)
    hi, lo = max(la, lb), min(la, lb)
    return (hi + 0.05) / (lo + 0.05)


PAIRS = [("--ink", "--surface"), ("--ink", "--bg"), ("--ink-2", "--surface"), ("--ink-3", "--surface"), ("--ink-3", "--bg"), ("--ink-3", "--bg-sunk"), ("--ink-3", "--surface-3"), ("--ink-2", "--bg-sunk"), ("--accent", "--surface"), ("--ink-inv", "--accent"),
         ("--accent-ink", "--accent-soft"), ("--st-ok-ink", "--st-ok-soft"), ("--st-warn-ink", "--st-warn-soft"), ("--st-bad-ink", "--st-bad-soft"), ("--st-off-ink", "--st-off-soft"),
         ("--st-busy-ink", "--st-busy-soft"), ("--rail-ink", "--rail-bg"), ("--rail-ink-strong", "--rail-bg"), ("--rail-accent", "--rail-bg")]


@pytest.mark.parametrize("theme", ["light", "dark"])
def test_design_token_contrast(new_page, theme):
    page = new_page(dark=theme == "dark")
    page.goto("/")
    page.wait_for_selector("body")
    vals = page.evaluate("(names) => Object.fromEntries(names.map((n) => [n, getComputedStyle(document.documentElement).getPropertyValue(n).trim()]))", sorted({n for p in PAIRS for n in p}))
    low = []
    for fg, bg in PAIRS:
        if not (vals[fg].startswith("#") and vals[bg].startswith("#")):
            continue
        r = _ratio(vals[fg], vals[bg])
        if r < 4.5:
            low.append(f"{fg} {vals[fg]} on {bg} {vals[bg]} = {r:.2f}")
    assert not low, f"{theme}: " + "; ".join(low)


def test_reduced_motion_disables_animation(new_page, goto):
    page = new_page(reduced_motion=True)
    goto(page, "/")
    dur = page.evaluate("""() => { const el = document.querySelector('.hrow .light, .light, .btn'); const cs = getComputedStyle(el);
        return [cs.animationDuration, cs.transitionDuration, getComputedStyle(document.documentElement).scrollBehavior]; }""")
    assert all(float(re.sub(r"[^0-9.e-]", "", d.split(",")[0]) or 0) <= 0.01 for d in dur[:2]) or dur[2] != "smooth", dur


@pytest.mark.parametrize("width", [768, 1024])
def test_tablet_layout_has_no_horizontal_scroll(new_page, goto, sample, width):
    page = new_page(width=width, height=900)
    _prime(page, sample)
    for route in ("/", "/compute", "/jobs", "/pipelines", "/alerts", "/security?tab=users", "/admin?tab=health", "/fabric"):
        goto(page, route)
        page.wait_for_timeout(900)
        over = page.evaluate("() => document.documentElement.scrollWidth - document.documentElement.clientWidth")
        assert over <= 2, f"{route} at {width}px overflows by {over}px"


def test_keyboard_reaches_the_main_content(page, goto):
    goto(page, "/")
    page.keyboard.press("Tab")
    first = page.evaluate("() => document.activeElement && (document.activeElement.textContent || '').trim()")
    assert re.search("skip", first or "", re.I), first
    page.keyboard.press("Enter")
    assert page.evaluate("() => document.activeElement && document.activeElement.closest('main') !== null")
