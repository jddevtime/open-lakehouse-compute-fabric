# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Change requests: every type, approval rules (no self-approval), idempotent apply, withdrawal, comments, listing and visibility."""

from __future__ import annotations


def open_change(gw, ctype, stype, sid, payload=None, who="cur_alpha", **extra):
    body = {"type": ctype, "subject_type": stype, "subject_id": sid, "payload": payload or {}, **extra}
    return gw.ok(gw.post("/changes", body, who), 201)


def test_certify_request_flow_with_approval_rules(gw):
    aid = gw.aid("mart_sales")
    chg = open_change(gw, "certify", "asset", aid, comment="please certify")
    assert chg["status"] == "pending" and chg["id"].startswith("chg_") and chg["requester"]["name"]
    assert chg["subject"]["id"] == aid and chg["comments"][0]["text"] == "please certify"
    # nothing happened yet
    assert gw.ok(gw.get(f"/assets/{aid}/certification"))["state"] == "none"
    # an identical pending request is refused
    dup = gw.post("/changes", {"type": "certify", "subject_type": "asset", "subject_id": aid, "payload": {}}, "cur_alpha")
    assert dup.status_code == 409 and dup.json()["error"]["details"]["change_id"] == chg["id"]
    # only approvers decide, and never the requester
    assert gw.post(f"/changes/{chg['id']}/approve", {}, "cur_alpha").status_code == 403
    assert gw.post(f"/changes/{chg['id']}/approve", {}, "viewer_alpha").status_code == 403
    assert gw.post(f"/changes/{chg['id']}/approve", {}, "cur_beta").status_code == 404
    own = open_change(gw, "certify", "asset", gw.aid("orders"), who="appr_alpha")
    r = gw.post(f"/changes/{own['id']}/approve", {}, "appr_alpha")
    assert r.status_code == 403 and "own change request" in r.text
    done = gw.ok(gw.post(f"/changes/{chg['id']}/approve", {"comment": "looks good"}, "appr_alpha"))
    assert done["status"] == "approved" and done["decided_by"]["name"] and done["applied_at"] and done["apply_result"]["ok"] is True
    assert gw.ok(gw.get(f"/assets/{aid}/certification"))["state"] == "certified"
    # already decided
    assert gw.post(f"/changes/{chg['id']}/approve", {}, "appr_alpha").status_code == 409
    assert gw.post(f"/changes/{chg['id']}/reject", {"comment": "no"}, "appr_alpha").status_code == 409
    # applying again changes nothing (idempotent) and records no second application
    before = len(gw.audit("governance.change_applied"))
    again = gw.ok(gw.post(f"/changes/{chg['id']}/apply", {}, "appr_alpha"))
    assert again["applied_at"] == done["applied_at"] and len(gw.audit("governance.change_applied")) == before
    # audit and events
    assert any(a["resource_id"] == chg["id"] for a in gw.audit("governance.change_requested"))
    assert any(a["details"]["decision"] == "approved" for a in gw.audit("governance.change_decided") if a["resource_id"] == chg["id"])
    assert gw.events("governance.change.requested", change_id=chg["id"]) and gw.events("governance.change.decided", change_id=chg["id"])
    gw.ok(gw.put(f"/assets/{aid}/certification", {"state": "none"}, "appr_alpha"))
    gw.ok(gw.post(f"/changes/{own['id']}/withdraw", {}, "appr_alpha"))


def test_self_approval_setting(fresh):
    gw = fresh
    chg = open_change(gw, "certify", "asset", gw.aid("orders"), who="appr_alpha")
    assert gw.post(f"/changes/{chg['id']}/approve", {}, "appr_alpha").status_code == 403
    gw.setting("governance.allow_self_approval", True)
    assert gw.ok(gw.post(f"/changes/{chg['id']}/approve", {}, "appr_alpha"))["status"] == "approved"
    assert gw.ok(gw.get(f"/assets/{gw.aid('orders')}/certification"))["state"] == "certified"


def test_reject_needs_comment_and_applies_nothing(gw):
    aid = gw.aid("raw_dump")
    chg = open_change(gw, "certify", "asset", aid)
    assert gw.post(f"/changes/{chg['id']}/reject", {}, "appr_alpha").status_code == 422
    assert gw.post(f"/changes/{chg['id']}/reject", {"comment": "   "}, "appr_alpha").status_code == 400
    rej = gw.ok(gw.post(f"/changes/{chg['id']}/reject", {"comment": "incomplete documentation"}, "appr_alpha"))
    assert rej["status"] == "rejected" and rej["decision_comment"] == "incomplete documentation" and rej["applied_at"] is None
    assert gw.ok(gw.get(f"/assets/{aid}/certification"))["state"] == "none"
    # a rejected request can be filed again
    again = open_change(gw, "certify", "asset", aid)
    gw.ok(gw.post(f"/changes/{again['id']}/withdraw", {}, "cur_alpha"))


def test_withdraw_comments_and_listing(gw):
    aid = gw.aid("raw_dump")
    chg = open_change(gw, "deprecate", "asset", aid, {"reason": "unused"}, reviewers=["g_aa"])
    assert chg["reviewers"] and chg["payload"] == {"reason": "unused"}
    assert gw.post(f"/changes/{chg['id']}/comments", {"text": "context please"}, "viewer_alpha").status_code == 403
    c = gw.ok(gw.post(f"/changes/{chg['id']}/comments", {"text": "is this still read by the finance team?"}, "appr_alpha"))
    assert [x["action"] for x in c["comments"]] == ["created", "comment"]
    assert gw.post(f"/changes/{chg['id']}/comments", {"text": " "}, "appr_alpha").status_code == 400
    # filters
    assert chg["id"] in [i["id"] for i in gw.ok(gw.get("/changes", "viewer_alpha", status="pending"))["items"]]
    assert chg["id"] in [i["id"] for i in gw.ok(gw.get("/changes", "cur_alpha", requester="me"))["items"]]
    assert chg["id"] in [i["id"] for i in gw.ok(gw.get("/changes", "appr_alpha", reviewer="me"))["items"]]
    assert chg["id"] not in [i["id"] for i in gw.ok(gw.get("/changes", "lead_org", reviewer="me"))["items"]]
    assert chg["id"] in [i["id"] for i in gw.ok(gw.get("/changes", type="deprecate", subject_id=aid))["items"]]
    assert gw.get("/changes", status="bogus").status_code == 400
    # nobody else can see it from the other workspace
    assert chg["id"] not in [i["id"] for i in gw.ok(gw.get("/changes", "viewer_beta"))["items"]]
    assert gw.get(f"/changes/{chg['id']}", "viewer_beta").status_code == 404
    # only the requester withdraws
    assert gw.post(f"/changes/{chg['id']}/withdraw", {}, "appr_alpha").status_code == 403
    w = gw.ok(gw.post(f"/changes/{chg['id']}/withdraw", {}, "cur_alpha"))
    assert w["status"] == "withdrawn"
    assert gw.post(f"/changes/{chg['id']}/approve", {}, "appr_alpha").status_code == 409
    # paging
    p1 = gw.ok(gw.get("/changes", limit=1))
    assert len(p1["items"]) == 1
    if p1["next_cursor"]:
        assert gw.ok(gw.get("/changes", limit=1, cursor=p1["next_cursor"]))["items"][0]["id"] != p1["items"][0]["id"]


def test_deprecate_applies_to_asset_product_and_term(gw):
    aid = gw.aid("raw_dump")
    chg = open_change(gw, "deprecate", "asset", aid, {"reason": "replaced"})
    gw.ok(gw.post(f"/changes/{chg['id']}/approve", {}, "appr_alpha"))
    got = gw.ok(gw.get(f"/assets/{aid}/certification"))
    assert got["state"] == "deprecated" and got["comment"] == "replaced"
    gw.ok(gw.put(f"/assets/{aid}/certification", {"state": "none"}, "appr_alpha"))
    prod, term = gw.product(), gw.term(status="approved")
    for stype, sid, path in (("product", prod["id"], f"/products/{prod['id']}"), ("term", term["id"], f"/glossary/terms/{term['id']}")):
        c = open_change(gw, "deprecate", stype, sid, who="cur_org")
        assert gw.ok(gw.post(f"/changes/{c['id']}/approve", {}, "lead_org2"))["apply_result"]["certification"] == "deprecated"
        assert gw.ok(gw.get(path, "viewer_org"))["status"] == "deprecated"


def test_classification_change(gw):
    aid = gw.aid("orders")
    # remove of something that is not there / add of something unknown is rejected up front
    assert gw.post("/changes", {"type": "classification_change", "subject_type": "asset", "subject_id": aid, "payload": {"add": ["Nope"]}}, "cur_alpha").status_code == 404
    assert gw.post("/changes", {"type": "classification_change", "subject_type": "asset", "subject_id": aid, "payload": {}}, "cur_alpha").status_code == 400
    chg = open_change(gw, "classification_change", "asset", aid, {"add": ["Confidential", "Internal"]})
    done = gw.ok(gw.post(f"/changes/{chg['id']}/approve", {}, "appr_alpha"))
    assert len(done["apply_result"]["added"]) == 2
    assert {d["classification"]["name"] for d in gw.ok(gw.get(f"/assets/{aid}/classifications"))["direct"]} == {"Confidential", "Internal"}
    # a second request that removes one and adds one that is already there: the second is "already done"
    chg2 = open_change(gw, "classification_change", "asset", aid, {"add": ["Internal"], "remove": ["Confidential"]})
    d2 = gw.ok(gw.post(f"/changes/{chg2['id']}/approve", {}, "appr_alpha"))
    assert len(d2["apply_result"]["removed"]) == 1 and len(d2["apply_result"]["already_done"]) == 1
    assert [d["classification"]["name"] for d in gw.ok(gw.get(f"/assets/{aid}/classifications"))["direct"]] == ["Internal"]
    gw.ok(gw.delete(f"/assets/{aid}/classifications/Internal", "cur_alpha"))


def test_classification_change_under_four_eyes_lands_approved(fresh):
    gw = fresh
    gw.setting("governance.classification_four_eyes", True)
    aid = gw.aid("orders")
    chg = open_change(gw, "classification_change", "asset", aid, {"add": ["PII"]})
    gw.ok(gw.post(f"/changes/{chg['id']}/approve", {}, "appr_alpha"))
    assert [d["status"] for d in gw.ok(gw.get(f"/assets/{aid}/classifications"))["direct"]] == ["approved"]


def test_term_link_change(gw):
    aid = gw.aid("orders")
    t = gw.term(status="approved")
    assert gw.post("/changes", {"type": "term_link", "subject_type": "asset", "subject_id": aid, "payload": {"term_id": "trm_nope"}}, "cur_alpha").status_code == 404
    assert gw.post("/changes", {"type": "term_link", "subject_type": "asset", "subject_id": aid, "payload": {"term_id": t["id"], "action": "x"}}, "cur_alpha").status_code == 400
    # linking a term is org-level glossary data, and the request is filed by a curator of the asset's workspace
    chg = open_change(gw, "term_link", "asset", aid, {"term_id": t["id"]})
    gw.ok(gw.post(f"/changes/{chg['id']}/approve", {}, "appr_alpha"))
    assert t["id"] in [x["id"] for x in gw.ok(gw.get(f"/assets/{aid}", "viewer_alpha"))["terms"]]
    un = open_change(gw, "term_link", "asset", aid, {"term_id": t["id"], "action": "unlink"})
    gw.ok(gw.post(f"/changes/{un['id']}/approve", {}, "appr_alpha"))
    assert t["id"] not in [x["id"] for x in gw.ok(gw.get(f"/assets/{aid}", "viewer_alpha"))["terms"]]


def test_ownership_change(gw):
    aid = gw.aid("orders")
    chg = open_change(gw, "ownership_change", "asset", aid, {"add": [{"role": "owner", "user": "g_ca"}, {"role": "steward", "name": "Sam", "email": "sam@example.com"}]})
    done = gw.ok(gw.post(f"/changes/{chg['id']}/approve", {}, "appr_alpha"))
    assert done["apply_result"]["added"] == 2
    direct = gw.ok(gw.get(f"/assets/{aid}/owners"))["direct"]
    assert {o["role"] for o in direct} == {"owner", "steward"}
    rm = open_change(gw, "ownership_change", "asset", aid, {"remove": [{"role": "steward", "name": "Sam", "email": "sam@example.com"}, {"role": "owner", "user": "g_ca"}]})
    d2 = gw.ok(gw.post(f"/changes/{rm['id']}/approve", {}, "appr_alpha"))
    assert d2["apply_result"]["removed"] == 2 and gw.ok(gw.get(f"/assets/{aid}/owners"))["direct"] == []
    assert gw.post("/changes", {"type": "ownership_change", "subject_type": "asset", "subject_id": aid, "payload": {}}, "cur_alpha").status_code == 400
    # a domain / product / term is org-level: curators of one workspace cannot file them
    dom = gw.domain()
    assert gw.post("/changes", {"type": "ownership_change", "subject_type": "domain", "subject_id": dom["id"], "payload": {"add": [{"role": "owner", "user": "g_ca"}]}}, "cur_alpha").status_code == 403
    c3 = open_change(gw, "ownership_change", "domain", dom["id"], {"add": [{"role": "owner", "user": "g_ca"}]}, who="cur_org")
    gw.ok(gw.post(f"/changes/{c3['id']}/approve", {}, "lead_org"))
    assert len(gw.ok(gw.get(f"/domains/{dom['id']}/owners"))["direct"]) == 1


def test_request_validation_and_scoping(gw):
    aid = gw.aid("orders")
    bad = lambda body, who="cur_alpha": gw.post("/changes", body, who).status_code      # noqa: E731
    assert bad({"type": "nope", "subject_type": "asset", "subject_id": aid}) == 422
    assert bad({"type": "certify", "subject_type": "domain", "subject_id": aid}) == 400
    assert bad({"type": "certify", "subject_type": "asset"}) == 422
    assert bad({"type": "certify", "subject_type": "asset", "subject_id": aid, "bogus": 1}) == 422
    assert bad({"type": "certify", "subject_type": "asset", "subject_id": aid, "payload": {"state": "deprecated"}}) == 400
    assert bad({"type": "certify", "subject_type": "asset", "subject_id": aid, "reviewers": ["nobody-here"]}) == 400
    assert bad({"type": "certify", "subject_type": "asset", "subject_id": "ast_nope"}) == 404
    assert bad({"type": "certify", "subject_type": "asset", "subject_id": aid}, "viewer_alpha") == 403
    assert bad({"type": "certify", "subject_type": "asset", "subject_id": gw.aid("beta_src")}) == 404     # hidden
    assert bad({"type": "certify", "subject_type": "asset", "subject_id": gw.aid("beta_src")}, "cur_alpha") == 404
    assert bad({"type": "certify", "subject_type": "asset", "subject_id": aid}, "nobody") in (403, 404)


def test_failed_application_is_recorded_and_can_be_retried(gw):
    t = gw.term(status="draft")
    chg = open_change(gw, "certify", "term", t["id"], who="cur_org")
    done = gw.ok(gw.post(f"/changes/{chg['id']}/approve", {}, "lead_org2"))
    # the decision stands, the application failed (a draft term cannot be certified) and says why
    assert done["status"] == "approved" and done["applied_at"] is None and done["apply_result"]["ok"] is False and "draft" in done["apply_result"]["error"]["message"]
    assert any(a["resource_id"] == chg["id"] and a["outcome"] == "failure" for a in gw.audit("governance.change_applied"))
    gw.ok(gw.patch(f"/glossary/terms/{t['id']}", {"status": "approved"}, "lead_org"))
    retried = gw.ok(gw.post(f"/changes/{chg['id']}/apply", {}, "lead_org2"))
    assert retried["applied_at"] and retried["apply_result"]["ok"] is True
    assert gw.ok(gw.get(f"/glossary/terms/{t['id']}/certification", "viewer_org"))["state"] == "certified"
    assert gw.post(f"/changes/{chg['id']}/apply", {}, "viewer_org").status_code == 403


def test_apply_of_unapproved_change_is_refused(gw):
    chg = open_change(gw, "certify", "asset", gw.aid("raw_dump"))
    assert gw.post(f"/changes/{chg['id']}/apply", {}, "appr_alpha").status_code == 409
    gw.ok(gw.post(f"/changes/{chg['id']}/withdraw", {}, "cur_alpha"))


def test_pending_changes_show_in_review_summary_and_asset_view(fresh):
    gw = fresh
    aid = gw.aid("orders")
    chg = open_change(gw, "certify", "asset", aid)
    assert gw.ok(gw.get("/review-queue/summary", "appr_alpha"))["change_requests"] == 1
    assert gw.ok(gw.get("/review-queue/summary", "viewer_beta"))["change_requests"] == 0
    detail = gw.ok(gw.get(f"/assets/{aid}", "viewer_alpha"))
    assert [c["id"] for c in detail["open_changes"]] == [chg["id"]]
