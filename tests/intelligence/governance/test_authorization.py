# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Who may do what: the permission matrix, workspace scoping (a hidden asset does not exist), org-level objects, plus the consistency of audit literals, events and settings."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from olcf.intelligence.governance import SETTINGS
from olcf.pipelines.webhooks import EVENT_CATALOG
from olcf.security.audit import ACTIONS

GOV = Path(__file__).resolve().parents[3] / "src" / "olcf" / "intelligence" / "governance"


# ============================================================================================================================ org-level objects
@pytest.mark.parametrize(("who", "read", "write"), [
    ("admin", 200, 201), ("lead_org", 200, 201), ("cur_org", 200, 201), ("viewer_org", 200, 403), ("viewer_alpha", 200, 403), ("viewer_beta", 200, 403),
    ("cur_alpha", 200, 403), ("secadmin", 200, 403), ("agent", 200, 403), ("nobody", 403, 403),
])
def test_org_level_objects_matrix(gw, who, read, write):
    for path in ("/glossary/terms", "/domains", "/products", "/classifications", "/policies", "/certifications", "/summary", "/changes", "/violations", "/review-queue"):
        assert gw.get(path, who).status_code == read, (path, who)
    for path in ("/glossary/terms", "/domains", "/products"):
        assert gw.post(path, {"name": gw.uniq("m")}, who).status_code == write, (path, who)
    assert gw.post("/classifications", {"name": gw.uniq("c")}, who).status_code == write


def test_unauthenticated_requests_are_rejected(gw):
    for method, path in (("GET", "/governance/summary"), ("POST", "/governance/glossary/terms"), ("GET", "/governance/policies"), ("PUT", f"/governance/assets/{gw.aid('orders')}/domain")):
        r = gw.c.request(method, f"/v1{path}", json={})
        assert r.status_code == 401, path


def test_approval_gated_actions(gw):
    t = gw.term()
    assert gw.patch(f"/glossary/terms/{t['id']}", {"status": "approved"}, "cur_org").status_code == 403
    assert gw.ok(gw.patch(f"/glossary/terms/{t['id']}", {"status": "approved"}, "lead_org"))["status"] == "approved"
    assert gw.delete(f"/glossary/terms/{t['id']}", "cur_org", params={"hard": "true"}).status_code == 403
    assert gw.ok(gw.delete(f"/glossary/terms/{t['id']}", "lead_org", params={"hard": "true"}))["deleted"] is True
    # policies and certification are the approvers' (SecurityAdmin included, curators not)
    body = {"name": gw.uniq("p"), "rule_type": "required_owner", "params": {}}
    assert gw.post("/policies", body, "secadmin").status_code == 201
    assert gw.post("/policies", {**body, "name": gw.uniq("p")}, "cur_org").status_code == 403
    assert gw.post("/policies", {**body, "name": gw.uniq("p")}, "appr_alpha").status_code == 403         # workspace approvers are not policy administrators
    prod = gw.product()
    assert gw.put(f"/products/{prod['id']}/certification", {"state": "certified"}, "secadmin").status_code == 200
    assert gw.put(f"/products/{prod['id']}/certification", {"state": "draft"}, "cur_org").status_code == 200
    assert gw.put(f"/products/{prod['id']}/certification", {"state": "certified"}, "cur_org").status_code == 403


# ============================================================================================================================ asset-level
@pytest.mark.parametrize(("who", "status"), [
    ("admin", 201), ("lead_org", 201), ("cur_org", 201), ("cur_alpha", 201), ("appr_alpha", 201), ("viewer_alpha", 403), ("viewer_org", 403), ("secadmin", 403), ("agent", 403),
    ("cur_beta", 404), ("viewer_beta", 404),
])
def test_asset_level_writes_matrix(gw, who, status):
    aid = gw.aid("raw_dump")
    name = gw.uniq("Own")
    r = gw.post(f"/assets/{aid}/owners", {"role": "contact", "name": name}, who)
    assert r.status_code == status, (who, r.text[:200])
    if status == 201:
        assert gw.delete(f"/assets/{aid}/owners/{r.json()['id']}", who).status_code == 200


@pytest.mark.parametrize(("who", "status"), [("viewer_alpha", 200), ("viewer_org", 200), ("cur_alpha", 200), ("agent", 200), ("viewer_beta", 404), ("nobody", 403)])
def test_asset_level_reads_matrix(gw, who, status):
    aid = gw.aid("customers_clean")
    for path in (f"/assets/{aid}", f"/assets/{aid}/owners", f"/assets/{aid}/classifications", f"/assets/{aid}/certification", f"/assets/{aid}/quality", f"/assets/{aid}/graph"):
        assert gw.get(path, who).status_code == status, (path, who)


def test_hidden_assets_do_not_exist_for_everything(gw):
    beta = gw.aid("beta_src")
    col = gw.aid("beta_src.secret_col")
    for who in ("viewer_alpha", "cur_alpha", "appr_alpha"):
        for path in (f"/assets/{beta}", f"/assets/{beta}/owners", f"/assets/{beta}/classifications", f"/assets/{beta}/certification", f"/assets/{beta}/quality", f"/assets/{beta}/graph",
                     f"/assets/{col}", f"/assets/{col}/classifications"):
            r = gw.get(path, who)
            assert r.status_code == 404 and gw.code(r) == "not_found", (path, who)
        for method, path, body in (("post", f"/assets/{beta}/owners", {"role": "owner", "name": "x"}), ("post", f"/assets/{beta}/classifications", {"classification": "PII"}),
                                   ("put", f"/assets/{beta}/certification", {"state": "draft"}), ("put", f"/assets/{beta}/domain", {"domain_id": "dom_x"}),
                                   ("post", f"/assets/{beta}/terms", {"term_id": "trm_x"}), ("post", "/changes", {"type": "certify", "subject_type": "asset", "subject_id": beta})):
            r = getattr(gw, method)(path, body, who)
            assert r.status_code == 404, (method, path, who, r.status_code)
    # the same message for a hidden asset and a missing one: existence is not leaked
    a, b = gw.get(f"/assets/{beta}", "viewer_alpha"), gw.get("/assets/ast_doesnotexist", "viewer_alpha")
    assert a.status_code == b.status_code == 404
    assert a.json()["error"]["message"].replace(beta, "ID") == b.json()["error"]["message"].replace("ast_doesnotexist", "ID")


def test_agent_token_with_read_scope_is_read_only(gw):
    assert gw.get("/summary", "agent").status_code == 200
    assert gw.post(f"/assets/{gw.aid('raw_dump')}/owners", {"role": "owner", "name": "x"}, "agent").status_code == 403
    assert gw.post("/glossary/terms", {"name": gw.uniq("t")}, "agent").status_code == 403
    assert gw.post("/review-queue/classification:1/approve", {}, "agent").status_code in (403, 404)
    assert gw.post("/classifications/scan", {}, "agent").status_code == 403


def test_pagination_and_cursor_validation(gw):
    for i in range(3):
        gw.term(gw.uniq(f"page{i}"))
    first = gw.ok(gw.get("/glossary/terms", limit=2))
    assert len(first["items"]) == 2 and first["next_cursor"]
    second = gw.ok(gw.get("/glossary/terms", limit=2, cursor=first["next_cursor"]))
    assert {i["id"] for i in first["items"]}.isdisjoint(i["id"] for i in second["items"])
    assert gw.get("/glossary/terms", limit=0).status_code == 422 and gw.get("/glossary/terms", limit=100000).status_code == 422
    bad = gw.get("/glossary/terms", cursor="not-a-cursor")
    assert bad.status_code == 400 and gw.code(bad) == "bad_request"


# ============================================================================================================================ audit literals, events, settings
def _literals(pattern: str) -> set[str]:
    found: set[str] = set()
    for f in GOV.glob("*.py"):
        found |= set(re.findall(pattern, f.read_text(encoding="utf-8")))
    return found


def test_every_audit_action_used_is_a_registered_literal():
    used = _literals(r"""audit\(\s*[^,]+,\s*["'](governance\.[a-z_]+)["']""") | _literals(r"""["'](governance\.[a-z_]+_[a-z]+)["']""")
    used = {u for u in used if not re.match(r"governance\.[a-z]+\.[a-z]+", u)}
    assert used, "no audit actions found"
    missing = sorted(u for u in used if u not in ACTIONS and u.replace("governance.", "") not in {s[0].replace("governance.", "") for s in SETTINGS})
    assert not missing, missing


def test_every_event_emitted_is_in_the_catalog_and_documented():
    emitted = _literals(r"""emit\(\s*["'](governance\.[a-z_.]+)["']""") | _literals(r"""\(\s*["'](governance\.[a-z]+\.[a-z]+)["']""")
    catalog = {e["type"] for e in EVENT_CATALOG if e["type"].startswith("governance.")}
    assert emitted and emitted <= catalog, sorted(emitted - catalog)
    assert catalog <= emitted | {"governance.review.requested"}, sorted(catalog - emitted)
    doc = (GOV.parents[3] / "docs" / "dev" / "GOVERNANCE_API.md").read_text(encoding="utf-8") if (GOV.parents[3] / "docs" / "dev" / "GOVERNANCE_API.md").exists() else ""
    assert all(e in doc for e in catalog)


def test_settings_are_registered_with_help_defaults_and_validation(gw):
    listed = gw.ok(gw.req("GET", "/admin/settings"))["settings"]
    keys = {s[0] for s in SETTINGS} | {f"governance.quality_weight.{n}" for n in ("documentation", "ownership", "classification", "freshness", "certification", "lineage", "violations", "schema_stability")}
    for key in keys:
        assert key in listed, key
        assert len(listed[key]["description"]) > 20 and listed[key]["default"] is not None
    assert gw.req("PUT", "/admin/settings/governance.allow_self_approval", json={"value": "yes"}).status_code == 400
    assert gw.req("PUT", "/admin/settings/governance.propagation_depth", json={"value": 99}).status_code == 400
    assert gw.req("PUT", "/admin/settings/governance.propagation_depth", json={"value": 2.5}).status_code == 400
    assert gw.req("PUT", "/admin/settings/governance.propagation_min_confidence", json={"value": "maybe"}).status_code == 400
    assert gw.req("PUT", "/admin/settings/governance.quality_column_rollup_weight", json={"value": 1.5}).status_code == 400
    assert gw.req("PUT", "/admin/settings/governance.suggestion_min_confidence", json={"value": True}).status_code == 400
    # only administrators change them
    assert gw.req("PUT", "/admin/settings/governance.allow_self_approval", "lead_org", json={"value": True}).status_code == 403


def test_settings_change_behaviour_without_restart(fresh):
    gw = fresh
    gw.setting("governance.auto_classify_on_scan", False)
    gw.tick()
    assert gw.ok(gw.get("/review-queue"))["items"] == []           # no automatic proposals while off
    gw.ok(gw.post("/classifications/scan", {}, "cur_org"))        # an explicit scan still works
    assert len(gw.ok(gw.get("/review-queue"))["items"]) == 2
