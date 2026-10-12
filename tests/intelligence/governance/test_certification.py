# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Certification: states and who may set them, concurrency, subjects (asset / product / term), and the automatic drop to ``draft`` when the schema changes."""

from __future__ import annotations


def cert(gw, aid, who="admin"):
    return gw.ok(gw.get(f"/assets/{aid}/certification", who))


def test_default_state_and_curator_vs_approver(gw):
    aid = gw.aid("orders")
    assert cert(gw, aid, "viewer_alpha")["state"] == "none"
    d = gw.ok(gw.put(f"/assets/{aid}/certification", {"state": "draft", "comment": "WIP"}, "cur_alpha"))
    assert d["state"] == "draft" and d["comment"] == "WIP" and d["etag"]
    # certifying and deprecating need governance.approve
    assert gw.put(f"/assets/{aid}/certification", {"state": "certified"}, "cur_alpha").status_code == 403
    assert gw.put(f"/assets/{aid}/certification", {"state": "deprecated"}, "cur_alpha").status_code == 403
    assert gw.put(f"/assets/{aid}/certification", {"state": "draft"}, "viewer_alpha").status_code == 403
    assert gw.put(f"/assets/{aid}/certification", {"state": "bogus"}, "appr_alpha").status_code == 422
    c = gw.ok(gw.put(f"/assets/{aid}/certification", {"state": "certified", "comment": "reviewed"}, "appr_alpha"))
    assert c["state"] == "certified" and c["certified_by"] and c["certified_at"] and c["schema_known"] is True and c["schema_changed_since"] is False
    # approvers of another workspace and curators of another workspace cannot
    assert gw.put(f"/assets/{aid}/certification", {"state": "deprecated"}, "cur_beta").status_code == 404      # hidden
    gw.ok(gw.put(f"/assets/{aid}/certification", {"state": "none"}, "appr_alpha"))
    assert cert(gw, aid)["state"] == "none"


def test_certification_shows_in_document_filter_and_listing(gw):
    aid = gw.aid("customers_clean")
    gw.ok(gw.put(f"/assets/{aid}/certification", {"state": "certified"}, "lead_org"))
    try:
        assert gw.ok(gw.intel(f"/assets/{aid}", "viewer_alpha"))["governance"]["certification"] == "certified"
        assert aid in [a["id"] for a in gw.ok(gw.intel("/assets", certification="certified", limit=200))["items"]]
        assert aid not in [a["id"] for a in gw.ok(gw.intel("/assets", certification="deprecated", limit=200))["items"]]
        listing = gw.ok(gw.get("/certifications", "viewer_alpha"))["items"]
        assert aid in [i["subject"]["id"] for i in listing if i["subject_type"] == "asset"]
        # beta's viewer does not see alpha's certified asset in the listing
        assert aid not in str(gw.ok(gw.get("/certifications", "viewer_beta")))
        assert gw.get("/certifications", "viewer_alpha", state="nope").status_code == 400
    finally:
        gw.ok(gw.put(f"/assets/{aid}/certification", {"state": "none"}, "lead_org"))


def test_etag_and_if_match(gw):
    aid = gw.aid("raw_dump")
    r = gw.put(f"/assets/{aid}/certification", {"state": "draft"}, "cur_alpha")
    etag = r.headers["etag"]
    assert gw.put(f"/assets/{aid}/certification", {"state": "none"}, "cur_alpha", headers={"If-Match": '"stale"'}).status_code in (409, 412)
    assert gw.put(f"/assets/{aid}/certification", {"state": "none"}, "cur_alpha", headers={"If-Match": etag}).status_code == 200


def test_placeholder_and_removed_assets_cannot_be_certified(gw):
    # a missing asset is a plain 404, hidden ones too
    assert gw.put("/assets/ast_nope/certification", {"state": "certified"}, "lead_org").status_code == 404
    assert gw.put(f"/assets/{gw.aid('beta_src')}/certification", {"state": "certified"}, "appr_alpha").status_code == 404


def test_product_and_term_certification(gw):
    prod = gw.product()
    assert gw.ok(gw.get(f"/products/{prod['id']}/certification", "viewer_org"))["state"] == "none"
    assert gw.put(f"/products/{prod['id']}/certification", {"state": "certified"}, "cur_org").status_code == 403
    assert gw.ok(gw.put(f"/products/{prod['id']}/certification", {"state": "certified"}, "lead_org"))["state"] == "certified"
    # a draft term cannot be certified, an approved one can
    t = gw.term(status="draft")
    assert gw.put(f"/glossary/terms/{t['id']}/certification", {"state": "certified"}, "lead_org").status_code == 400
    ta = gw.term(status="approved")
    assert gw.ok(gw.put(f"/glossary/terms/{ta['id']}/certification", {"state": "certified"}, "lead_org"))["state"] == "certified"
    assert gw.ok(gw.get(f"/glossary/terms/{ta['id']}", "viewer_org"))["certification"] == "certified"
    # workspace-scoped users have no say over org-level subjects
    assert gw.put(f"/products/{prod['id']}/certification", {"state": "draft"}, "cur_alpha").status_code == 403
    assert gw.get(f"/products/{prod['id']}/certification", "nobody").status_code == 403


def test_product_certification_drops_when_members_change(gw):
    prod = gw.product()
    gw.ok(gw.post(f"/products/{prod['id']}/members", {"asset_id": gw.aid("orders")}, "cur_org"), 201)
    gw.ok(gw.put(f"/products/{prod['id']}/certification", {"state": "certified"}, "lead_org"))
    assert gw.gov.certification.invalidate_changed() == []
    gw.ok(gw.post(f"/products/{prod['id']}/members", {"asset_id": gw.aid("customers")}, "cur_org"), 201)
    out = gw.gov.certification.invalidate_changed()
    assert [(o["subject_type"], o["subject_id"], o["reason"]) for o in out] == [("product", prod["id"], "members_changed")]
    got = gw.ok(gw.get(f"/products/{prod['id']}/certification", "viewer_org"))
    assert got["state"] == "draft" and "member set" in got["invalidated_reason"]


# ============================================================================================================================ schema change
def test_schema_change_drops_certification_to_draft(fresh):
    gw = fresh
    aid = gw.aid("customers_clean")
    gw.ok(gw.put(f"/assets/{aid}/certification", {"state": "certified", "comment": "ok"}, "appr_alpha"))
    gw.tick()
    assert cert(gw, aid)["state"] == "certified"      # an unchanged rescan keeps it
    # an additional column arrives with the next scan
    gw.alter_table("customers_clean", [("customer_id", "int"), ("email", "string"), ("region", "string"), ("signup_date", "string")])
    r = gw.tick()
    assert r["invalidated"] >= 1
    got = cert(gw, aid)
    assert got["state"] == "draft" and got["invalidated_at"] and "schema changed" in got["invalidated_reason"]
    assert got["certified_at"]      # history stays
    # audit trail and notification event
    inv = [a for a in gw.audit("governance.certification_invalidated") if a["resource_id"] == aid]
    assert len(inv) == 1 and inv[0]["details"]["previous"] == "certified" and inv[0]["details"]["state"] == "draft"
    evts = gw.events("governance.certification.changed", subject_id=aid, state="draft")
    assert len(evts) == 1 and evts[0]["payload"]["previous"] == "certified" and "schema" in evts[0]["payload"]["reason"]
    # idempotent: a second pass finds nothing to do and emits nothing more
    assert gw.gov.certification.invalidate_changed() == []
    assert len(gw.events("governance.certification.changed", subject_id=aid, state="draft")) == 1
    # recertifying records the new schema
    gw.ok(gw.put(f"/assets/{aid}/certification", {"state": "certified"}, "appr_alpha"))
    assert gw.gov.certification.invalidate_changed() == []
    assert cert(gw, aid)["state"] == "certified" and cert(gw, aid)["invalidated_reason"] is None


def test_schema_change_event_triggers_the_check(fresh):
    gw = fresh
    aid = gw.aid("orders")
    gw.ok(gw.put(f"/assets/{aid}/certification", {"state": "certified"}, "appr_alpha"))
    gw.tick()
    gw.alter_table("orders", [("order_id", "int"), ("customer_id", "int"), ("amount", "decimal")])     # a type change
    gw.tick()
    assert cert(gw, aid)["state"] == "draft"


def test_unrelated_and_draft_subjects_are_left_alone(fresh):
    gw = fresh
    gw.ok(gw.put(f"/assets/{gw.aid('orders')}/certification", {"state": "draft"}, "cur_alpha"))
    gw.ok(gw.put(f"/assets/{gw.aid('customers')}/certification", {"state": "certified"}, "appr_alpha"))
    gw.tick()
    gw.alter_table("orders", [("order_id", "int")])
    gw.tick()
    assert cert(gw, gw.aid("orders"))["state"] == "draft" and cert(gw, gw.aid("customers"))["state"] == "certified"
    assert gw.events("governance.certification.changed", subject_id=gw.aid("customers"), state="draft") == []
