# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Classification: catalog, manual assignment, suggestions and the review queue, four-eyes approval, lineage propagation, exposure and visibility."""

from __future__ import annotations

from sqlalchemy import update

from olcf.intelligence.governance.classification import BUILTINS
from olcf.intelligence.schema import assets

BUILTIN_NAMES = {b[0] for b in BUILTINS}


def pending(gw, **params):
    return gw.ok(gw.get("/review-queue", **params))["items"]


def by_asset(items, name):
    return [i for i in items if i["asset"]["name"] == name]


# ============================================================================================================================ catalog
def test_builtins_are_seeded_once(gw):
    items = gw.ok(gw.get("/classifications", "viewer_org"))["items"]
    assert BUILTIN_NAMES <= {c["name"] for c in items} and len(BUILTIN_NAMES) == 9
    assert all(c["builtin"] for c in items if c["name"] in BUILTIN_NAMES)
    assert gw.gov.classifications.ensure_builtins() == 0     # idempotent
    assert len([c for c in gw.ok(gw.get("/classifications"))["items"] if c["name"] in BUILTIN_NAMES]) == 9


def test_custom_labels_and_builtin_restrictions(gw):
    name = gw.uniq("Secret")
    made = gw.ok(gw.post("/classifications", {"name": name, "category": "internal", "level": 2, "color": "#112233"}, "cur_org"), 201)
    assert made["builtin"] is False and made["level"] == 2
    assert gw.post("/classifications", {"name": name.upper()}, "cur_org").status_code == 409
    assert gw.post("/classifications", {"name": "x", "bogus": 1}, "cur_org").status_code == 422
    # a workspace-scoped curator cannot edit the org-wide catalog
    assert gw.post("/classifications", {"name": gw.uniq("Nope")}, "cur_alpha").status_code == 403
    got = gw.get(f"/classifications/{made['id']}", "viewer_alpha")
    etag = got.headers["etag"]
    assert gw.patch(f"/classifications/{made['id']}", {"description": "d"}, "cur_org", headers={"If-Match": '"stale"'}).status_code in (409, 412)
    assert gw.ok(gw.patch(f"/classifications/{made['id']}", {"description": "d", "level": 3}, "cur_org", headers={"If-Match": etag}))["level"] == 3
    pii = gw.class_id("PII")
    assert gw.patch(f"/classifications/{pii}", {"name": "Other"}, "cur_org").status_code == 400
    assert gw.patch(f"/classifications/{pii}", {"level": 1}, "cur_org").status_code == 400
    assert gw.ok(gw.patch(f"/classifications/{pii}", {"description": "Personally identifiable information."}, "cur_org"))["builtin"] is True
    assert gw.delete(f"/classifications/{pii}", "lead_org").status_code == 409
    # deleting needs governance.approve, and a label in use cannot go
    gw.ok(gw.classify("raw_dump", made["id"], "cur_alpha"), 201)
    assert gw.delete(f"/classifications/{made['id']}", "cur_org").status_code == 403
    assert gw.delete(f"/classifications/{made['id']}", "lead_org").status_code == 409
    gw.ok(gw.delete(f"/assets/{gw.aid('raw_dump')}/classifications/{made['id']}", "cur_alpha"))
    assert gw.ok(gw.delete(f"/classifications/{made['id']}", "lead_org"))["deleted"] is True
    assert gw.get(f"/classifications/{made['id']}").status_code == 404


# ============================================================================================================================ manual
def test_manual_assignment_roundtrip(gw):
    aid = gw.aid("orders")
    r = gw.ok(gw.classify("orders", "Confidential", "cur_alpha"), 201)
    assert r["status"] == "approved" and r["source"] == "manual"
    assert gw.classify("orders", "Confidential", "cur_alpha").status_code == 409
    assert gw.classify("orders", "Nonexistent", "cur_alpha").status_code == 404
    assert gw.classify("lakehouse_a", "Public", "viewer_alpha").status_code == 403
    assert gw.classify("beta_src", "Public", "cur_alpha").status_code == 404           # hidden asset: not found
    detail = gw.ok(gw.get(f"/assets/{aid}/classifications", "viewer_alpha"))
    assert [d["classification"]["name"] for d in detail["direct"]] == ["Confidential"]
    # the label shows in the Intelligence asset document and filter
    doc = gw.ok(gw.intel(f"/assets/{aid}", "viewer_alpha"))["governance"]
    assert [c["name"] for c in doc["classifications"]] == ["Confidential"] or any(c.get("name") == "Confidential" for c in doc["classifications"])
    ids = [i["id"] for i in gw.ok(gw.intel("/assets", classification="Confidential"))["items"]]
    assert aid in ids
    gw.ok(gw.delete(f"/assets/{aid}/classifications/Confidential", "cur_alpha"))
    assert gw.delete(f"/assets/{aid}/classifications/Confidential", "cur_alpha").status_code == 404
    assert aid not in [i["id"] for i in gw.ok(gw.intel("/assets", classification="Confidential"))["items"]]


# ============================================================================================================================ suggestions
def test_suggestions_appear_only_after_scan_finished_and_are_never_approved(fresh):
    gw = fresh
    # nothing is proposed until the background pass has seen the finished scan
    assert pending(gw) == []
    ran = gw.tick()
    assert ran["signals"] >= 1 and ran["scans"] >= 1
    items = pending(gw, kind="classification")
    assert {i["asset"]["name"] for i in items} == {"email", "full_name"}
    for i in items:
        assert i["source"] == "auto" and i["classification"]["name"] == "PII" and i["requires"] == "governance.curate"
        assert i["evidence"]["rule"] and "value" not in str(i["evidence"]).lower().replace("evidence_kinds", "")
    assert gw.ok(gw.get(f"/assets/{gw.aid('customers.email')}/classifications"))["direct"] == []
    assert all(i["status"] != "approved" for i in gw.ok(gw.get(f"/assets/{gw.aid('customers.email')}/classifications"))["suggestions"])
    # nothing is inherited from a mere suggestion
    assert gw.ok(gw.get(f"/assets/{gw.aid('customers_clean.email')}/classifications"))["inherited"] == []
    summary = gw.ok(gw.get("/review-queue/summary"))
    assert summary["classification"] == 2 and summary["total"] >= 2
    # a second tick and an on-demand scan change nothing (idempotent)
    gw.tick()
    again = gw.ok(gw.post("/classifications/scan", {}, "cur_org"))
    assert again["classification"]["created"] == 0 and len(pending(gw, kind="classification")) == 2


def test_scan_endpoint_needs_curate_and_respects_scope(fresh):
    gw = fresh
    alpha, beta = gw.ws["alpha"]["id"], gw.ws["beta"]["id"]
    assert gw.post("/classifications/scan", {"workspace_id": alpha}, "viewer_alpha").status_code == 403
    assert gw.post("/classifications/scan", {}, "cur_alpha").status_code == 403         # no workspace given: org-wide curate is required
    assert gw.post("/classifications/scan", {"workspace_id": beta}, "cur_alpha").status_code == 403
    assert gw.ok(gw.post("/classifications/scan", {"workspace_id": beta}, "cur_beta"))["classification"]["created"] == 0
    r = gw.ok(gw.post("/classifications/scan", {"workspace_id": alpha}, "cur_alpha"))
    assert r["classification"]["created"] == 2
    audit = gw.ok(gw.req("GET", "/audit", params={"action": "governance.classification_scanned"}))["items"]
    assert audit and audit[0]["details"]["suggested"] in (0, 2)


def test_min_confidence_setting_filters_suggestions(fresh):
    gw = fresh
    gw.setting("governance.suggestion_min_confidence", 0.8)
    gw.tick()
    assert {i["asset"]["name"] for i in pending(gw, kind="classification")} == {"email"}


def test_review_approve_reject_and_not_proposed_again(fresh):
    gw = fresh
    gw.tick()
    items = pending(gw, kind="classification")
    email = by_asset(items, "email")[0]
    name = by_asset(items, "full_name")[0]
    assert gw.post(f"/review-queue/{email['id']}/approve", {}, "viewer_alpha").status_code == 403
    assert gw.post(f"/review-queue/{email['id']}/approve", {}, "cur_beta").status_code == 404
    ok = gw.ok(gw.post(f"/review-queue/{email['id']}/approve", {"comment": "confirmed"}, "cur_alpha"))
    assert ok["decision"] == "approved"
    assert gw.post(f"/review-queue/{email['id']}/approve", {}, "cur_alpha").status_code == 409     # decided already
    rej = gw.ok(gw.post(f"/review-queue/{name['id']}/reject", {"comment": "not a person name column"}, "cur_alpha"))
    assert rej["decision"] == "rejected"
    cls = gw.ok(gw.get(f"/assets/{gw.aid('customers.full_name')}/classifications"))
    assert [r["classification"]["name"] for r in cls["rejected"]] == ["PII"] and cls["direct"] == []
    assert pending(gw, kind="classification") == []
    # a rescan and a scan call never bring the rejected suggestion back, nor touch the approved one
    gw.tick()
    gw.ok(gw.post("/classifications/scan", {}, "cur_org"))
    assert pending(gw, kind="classification") == []
    assert [d["status"] for d in gw.ok(gw.get(f"/assets/{gw.aid('customers.email')}/classifications"))["direct"]] == ["approved"]
    # the approval shows in audit and events
    rows = gw.ok(gw.req("GET", "/audit", params={"action": "governance.classification_reviewed"}))["items"]
    assert {r["details"]["decision"] for r in rows} == {"approved", "rejected"}


def test_bulk_review(fresh):
    gw = fresh
    gw.tick()
    ids = [i["id"] for i in pending(gw, kind="classification")]
    r = gw.ok(gw.post("/review-queue/bulk", {"action": "approve", "ids": [*ids, "classification:99999", ids[0]], "comment": "bulk"}, "cur_alpha"))
    assert r["ok"] == 2 and r["failed"] == 1
    assert [x["ok"] for x in r["results"]] == [True, True, False]
    assert r["results"][2]["error"]["code"] == "not_found"
    assert gw.post("/review-queue/bulk", {"action": "approve", "ids": []}, "cur_alpha").status_code == 422
    assert gw.post("/review-queue/bulk", {"action": "maybe", "ids": ids}, "cur_alpha").status_code == 422
    bogus = gw.ok(gw.post("/review-queue/bulk", {"action": "approve", "ids": ["bogus"]}, "cur_alpha"))
    assert bogus["failed"] == 1 and bogus["results"][0]["error"]["code"] == "bad_request"


def test_suggestion_withdrawn_when_detector_no_longer_suggests(fresh):
    gw = fresh
    gw.tick()
    assert len(pending(gw, kind="classification")) == 2
    with gw.gov.db.begin() as c:
        c.execute(update(assets).where(assets.c.id == gw.aid("customers.full_name")).values(attrs={"suggested_classifications": []}))
    r = gw.ok(gw.post("/classifications/scan", {}, "cur_org"))
    assert r["classification"]["withdrawn"] == 1
    assert {i["asset"]["name"] for i in pending(gw, kind="classification")} == {"email"}


def test_four_eyes_requires_approve_and_other_person(fresh):
    gw = fresh
    gw.setting("governance.classification_four_eyes", True)
    gw.tick()
    item = by_asset(pending(gw, kind="classification"), "email")[0]
    # curators can no longer approve; approvers can
    assert gw.post(f"/review-queue/{item['id']}/approve", {}, "cur_alpha").status_code == 403
    assert gw.post(f"/review-queue/{item['id']}/approve", {}, "viewer_alpha").status_code == 403
    # rejecting stays possible for curators
    other = by_asset(pending(gw, kind="classification"), "full_name")[0]
    assert gw.ok(gw.post(f"/review-queue/{other['id']}/reject", {}, "cur_alpha"))["decision"] == "rejected"
    # a manual assignment by a curator becomes a suggestion that waits for review
    r = gw.ok(gw.classify("orders", "Confidential", "cur_alpha"), 201)
    assert r["status"] == "suggested"
    assert gw.classify("orders", "Confidential", "cur_alpha").status_code == 409
    mine = by_asset(pending(gw, kind="classification"), "orders")[0]
    assert gw.post(f"/review-queue/{mine['id']}/approve", {}, "cur_alpha").status_code == 403
    assert gw.ok(gw.post(f"/review-queue/{mine['id']}/approve", {}, "appr_alpha"))["decision"] == "approved"
    assert gw.ok(gw.post(f"/review-queue/{item['id']}/approve", {}, "appr_alpha"))["decision"] == "approved"
    # an approver who proposes cannot approve their own proposal (self-approval off): it waits for somebody else
    own = gw.ok(gw.classify("raw_dump", "Internal", "appr_alpha"), 201)
    assert own["status"] == "suggested"
    own_item = by_asset(pending(gw, kind="classification"), "raw_dump")[0]
    assert gw.post(f"/review-queue/{own_item['id']}/approve", {}, "appr_alpha").status_code == 403
    assert gw.ok(gw.post(f"/review-queue/{own_item['id']}/approve", {}, "lead_org"))["decision"] == "approved"
    # removing an approved label needs approve while four-eyes is on
    assert gw.delete(f"/assets/{gw.aid('orders')}/classifications/Confidential", "cur_alpha").status_code == 403
    gw.ok(gw.delete(f"/assets/{gw.aid('orders')}/classifications/Confidential", "appr_alpha"))


def test_four_eyes_proposer_cannot_approve_own_suggestion(fresh):
    gw = fresh
    gw.setting("governance.classification_four_eyes", True)
    gw.tick()
    item = by_asset(pending(gw, kind="classification"), "email")[0]
    uid = gw.ok(gw.post(f"/assets/{gw.aid('raw_dump')}/owners", {"role": "owner", "user": "g_aa"}, "cur_alpha"), 201)["user_id"]
    n = int(item["id"].split(":")[1])
    from olcf.intelligence.governance.schema import gov_class_assignments as g
    with gw.gov.db.begin() as c:
        c.execute(update(g).where(g.c.id == n).values(created_by=uid))
    denied = gw.post(f"/review-queue/{item['id']}/approve", {}, "appr_alpha")
    assert denied.status_code == 403 and "proposed" in denied.text
    assert gw.ok(gw.post(f"/review-queue/{item['id']}/approve", {}, "lead_org"))["decision"] == "approved"


def test_four_eyes_proposer_may_approve_with_self_approval_setting(fresh):
    gw = fresh
    gw.setting("governance.classification_four_eyes", True)
    gw.setting("governance.allow_self_approval", True)
    gw.tick()
    item = by_asset(pending(gw, kind="classification"), "email")[0]
    uid = gw.ok(gw.post(f"/assets/{gw.aid('raw_dump')}/owners", {"role": "owner", "user": "g_aa"}, "cur_alpha"), 201)["user_id"]
    from olcf.intelligence.governance.schema import gov_class_assignments as g
    with gw.gov.db.begin() as c:
        c.execute(update(g).where(g.c.id == int(item["id"].split(":")[1])).values(created_by=uid))
    assert gw.ok(gw.post(f"/review-queue/{item['id']}/approve", {}, "appr_alpha"))["decision"] == "approved"


# ============================================================================================================================ propagation
def approve_email(gw):
    gw.tick()
    item = by_asset(pending(gw, kind="classification"), "email")[0]
    gw.ok(gw.post(f"/review-queue/{item['id']}/approve", {}, "cur_alpha"))


def test_propagation_along_column_lineage(fresh):
    gw = fresh
    approve_email(gw)
    clean_email = gw.ok(gw.get(f"/assets/{gw.aid('customers_clean.email')}/classifications"))
    inh = clean_email["inherited"]
    assert len(inh) == 1 and inh[0]["classification"]["name"] == "PII" and inh[0]["source"] == "inherited"
    assert inh[0]["proven"] is True and inh[0]["precision"] == "column"
    assert inh[0]["confidence_floor"] == "derived"
    assert [n["name"] for n in inh[0]["evidence"]["path"]["nodes"]] == ["email", "email"]
    # a column that is not on the email path inherits nothing
    assert gw.ok(gw.get(f"/assets/{gw.aid('customers_clean.region')}/classifications"))["inherited"] == []
    # the source table holds the labelled column: "contained", not its own label; the table downstream "contains" it by lineage
    src = gw.ok(gw.get(f"/assets/{gw.aid('customers')}/classifications"))
    assert src["direct"] == [] and [(c["classification"]["name"], c["columns"]) for c in src["contained"]] == [("PII", 1)]
    tab = gw.ok(gw.get(f"/assets/{gw.aid('customers_clean')}/classifications"))
    assert tab["direct"] == [] and [(i["precision"], i["proven"]) for i in tab["inherited"]] == [("contains", True)]
    # table-level lineage alone only makes a *possible* exposure: nothing is stated on the asset
    assert gw.ok(gw.get(f"/assets/{gw.aid('mart_sales')}/classifications"))["inherited"] == []
    # the visual that displays the inherited column
    vis = gw.ok(gw.get(f"/assets/{gw.aid('email_card')}/classifications"))
    assert [i["classification"]["name"] for i in vis["inherited"]] == ["PII"] and vis["inherited"][0]["proven"] is True


def test_exposure_report(fresh):
    gw = fresh
    approve_email(gw)
    pii = gw.class_id("PII")
    ex = gw.ok(gw.get(f"/classifications/{pii}/exposure"))
    assert [s["asset"]["name"] for s in ex["sources"]] == ["email"]
    kinds = {(e["asset"]["name"], e["kind"]): e for e in ex["exposures"]}
    assert ("email", "column") in kinds and ("email_card", "visual") in kinds
    assert kinds[("customers_clean", "table")]["precision"] == "contains" and kinds[("customers_clean", "table")]["proven"] is True
    assert kinds[("sales_report", "report")]["proven"] is True
    for e in ex["exposures"]:
        assert e["path"]["nodes"] and e["confidence_floor"] in ("verified", "derived", "inferred", "unknown")
    # table-level lineage only: possible, never proven
    possible = [e for e in ex["exposures"] if not e["proven"]]
    assert possible and all(e["precision"] != "column" for e in possible)
    assert ex["summary"]["possible"] == len(possible) and ex["summary"]["proven"] == len(ex["exposures"]) - len(possible)
    assert any("not proof" in n for n in ex["notes"])
    # filters
    only = gw.ok(gw.get(f"/classifications/{pii}/exposure", include_possible="false"))
    assert all(e["proven"] for e in only["exposures"])
    visuals = gw.ok(gw.get(f"/classifications/{pii}/exposure", types="visual"))
    assert {e["kind"] for e in visuals["exposures"]} == {"visual"}
    shallow = gw.ok(gw.get(f"/classifications/{pii}/exposure", depth=1))
    assert len(shallow["exposures"]) < len(ex["exposures"])
    assert gw.get(f"/classifications/{pii}/exposure", depth=0).status_code == 422 or gw.get(f"/classifications/{pii}/exposure", depth=0).status_code == 400
    assert gw.get(f"/classifications/{pii}/exposure", min_confidence="bogus").status_code in (400, 422)
    strict = gw.ok(gw.get(f"/classifications/{pii}/exposure", min_confidence="verified"))
    assert len(strict["exposures"]) <= len(ex["exposures"])
    assert gw.get("/classifications/clsnope/exposure").status_code == 404


def test_classification_asset_listing_and_filter_include_inherited(fresh):
    gw = fresh
    approve_email(gw)
    pii = gw.class_id("PII")
    listing = gw.ok(gw.get(f"/classifications/{pii}/assets"))["items"]
    got = {(i["asset"]["name"], i["source"]) for i in listing}
    assert ("email", "direct") in got and ("email", "inherited") in got
    direct_only = gw.ok(gw.get(f"/classifications/{pii}/assets", include_inherited="false"))["items"]
    assert {i["source"] for i in direct_only} == {"direct"}
    flt = {i["id"] for i in gw.ok(gw.intel("/assets", classification="PII"))["items"]}
    assert gw.aid("customers.email") in flt and gw.aid("customers_clean.email") in flt
    assert gw.aid("customers.region") not in flt


def test_exposure_hides_what_the_caller_cannot_see(gw):
    # beta's secret column flows into alpha: a classified source the alpha viewer cannot see
    src = gw.aid("beta_src.secret_col")
    gw.ok(gw.classify("beta_src.secret_col", "Credentials", "cur_beta"), 201)
    try:
        cred = gw.class_id("Credentials")
        # a viewer with org-wide read sees the whole picture
        full = gw.ok(gw.get(f"/classifications/{cred}/exposure", "viewer_org"))
        assert any(s["asset"].get("name") == "secret_col" for s in full["sources"])
        assert any(e["asset"]["name"] == "secret_col" and e["asset"]["workspace_id"] == gw.ws["alpha"]["id"] for e in full["exposures"])
        # an alpha viewer sees the exposure in its own workspace but the source is hidden
        mine = gw.ok(gw.get(f"/classifications/{cred}/exposure", "viewer_alpha"))
        assert mine["sources"] and all(s["asset"].get("hidden") is True for s in mine["sources"])
        assert src not in str(mine) and "beta_src" not in str(mine) and "beta_nb" not in str(mine)
        inh = gw.ok(gw.get(f"/assets/{gw.aid('alpha_ingest.secret_col')}/classifications", "viewer_alpha"))["inherited"]
        assert inh and inh[0]["evidence"]["from_asset"].get("hidden") is True and "beta_src" not in str(inh)
        assert gw.get(f"/assets/{src}/classifications", "viewer_alpha").status_code == 404
        # the listing for beta's viewer only has beta assets
        lst = gw.ok(gw.get(f"/classifications/{cred}/assets", "viewer_beta"))["items"]
        assert {i["asset"]["workspace_id"] for i in lst} == {gw.ws["beta"]["id"]}
    finally:
        gw.ok(gw.delete(f"/assets/{src}/classifications/Credentials", "cur_beta"))


def test_propagation_cache_follows_classification_changes(fresh):
    gw = fresh
    gw.ok(gw.classify("customers.region", "Internal", "cur_alpha"), 201)
    target = gw.aid("customers_clean.region")
    first = gw.ok(gw.get(f"/assets/{target}/classifications"))["inherited"]
    assert [i["classification"]["name"] for i in first] == ["Internal"]
    gw.ok(gw.delete(f"/assets/{gw.aid('customers.region')}/classifications/Internal", "cur_alpha"))
    assert gw.ok(gw.get(f"/assets/{target}/classifications"))["inherited"] == []
    # a new scan version recomputes as well (the cache is keyed per scan version)
    gw.ok(gw.classify("customers.region", "Internal", "cur_alpha"), 201)
    assert [i["classification"]["name"] for i in gw.ok(gw.get(f"/assets/{target}/classifications"))["inherited"]] == ["Internal"]


# ============================================================================================================================ cache and limits
def test_propagation_is_cached_per_scan_version_and_recomputed_on_change(fresh):
    gw = fresh
    approve_email(gw)
    prop = gw.gov.propagation
    first = prop.result()
    assert prop.result() is first                         # same stamp: no recomputation
    gw.alter_table("orders", [("order_id", "int"), ("customer_id", "int"), ("amount", "double"), ("note", "string")])      # a new scan version
    second = prop.result()
    assert second is not first and second.stamp != first.stamp
    assert prop.result() is second
    gw.ok(gw.delete(f"/assets/{gw.aid('customers.email')}/classifications/PII", "cur_alpha"))
    assert prop.result().by_class.get(gw.class_id("PII"), []) == []
    # nothing about inheritance is stored
    from sqlalchemy import func, select

    from olcf.intelligence.governance.schema import gov_class_assignments as g
    with gw.gov.db.connect() as c:
        assert c.execute(select(func.count()).select_from(g).where(g.c.source == "inherited")).scalar() == 0


def test_propagation_limits_are_settings_and_reported(fresh):
    gw = fresh
    approve_email(gw)
    pii = gw.class_id("PII")
    deep = {e["asset"]["name"] for e in gw.ok(gw.get(f"/classifications/{pii}/exposure"))["exposures"]}
    gw.setting("governance.propagation_depth", 1)
    shallow = gw.ok(gw.get(f"/classifications/{pii}/exposure"))
    assert {e["asset"]["name"] for e in shallow["exposures"]} < deep
    gw.setting("governance.propagation_depth", 6)
    gw.setting("governance.propagation_max_nodes", 10)
    assert gw.ok(gw.get(f"/classifications/{pii}/exposure"))["truncated"] is False          # this lineage is small enough
    # a cut-off walk says so (the setting's own minimum is 10, so the bound is passed to the walk directly)
    assert gw.gov.propagation.compute(only_class=pii, max_nodes=1).truncated is True
    # a weak edge is not followed unless the setting allows it
    gw.setting("governance.propagation_max_nodes", 500)
    gw.setting("governance.propagation_min_confidence", "verified")
    strict = gw.ok(gw.get(f"/classifications/{pii}/exposure"))
    assert strict["exposures"] == [] or all(e["confidence_floor"] == "verified" for e in strict["exposures"])
    gw.setting("governance.propagation_min_confidence", "unknown")
    loose = {e["asset"]["name"] for e in gw.ok(gw.get(f"/classifications/{pii}/exposure"))["exposures"]}
    assert deep <= loose


def test_table_label_reaches_downstream_tables_and_report_chain(fresh):
    gw = fresh
    gw.ok(gw.classify("customers", "Confidential", "cur_alpha"), 201)
    inh = {i["classification"]["name"]: i for i in gw.ok(gw.get(f"/assets/{gw.aid('customers_clean')}/classifications"))["inherited"]}
    assert inh["Confidential"]["precision"] == "table" and inh["Confidential"]["proven"] is True
    # a label stated on a table is an assertion about the whole table; it reaches what is derived from it at table level, no matter how many hops
    mart = gw.ok(gw.get(f"/assets/{gw.aid('mart_sales')}/classifications"))["inherited"]
    assert [i["classification"]["name"] for i in mart] == ["Confidential"] and len(mart[0]["evidence"]["path"]["edges"]) >= 2
    assert mart[0]["confidence_floor"] == "derived"


def test_exposure_lists_data_products_that_receive_the_data(fresh):
    gw = fresh
    approve_email(gw)
    prod = gw.product("Clean customers")
    gw.ok(gw.post(f"/products/{prod['id']}/members", {"asset_id": gw.aid("customers_clean")}, "cur_org"), 201)
    other = gw.product("Orders only")
    gw.ok(gw.post(f"/products/{other['id']}/members", {"asset_id": gw.aid("orders")}, "cur_org"), 201)
    ex = gw.ok(gw.get(f"/classifications/{gw.class_id('PII')}/exposure", "viewer_org"))
    assert [(p["product"]["name"], p["via_assets"]) for p in ex["data_products"]] == [("Clean customers", [gw.aid("customers_clean")])]
    assert ex["summary"]["products"] == 1 and ex["summary"]["by_type"]["table"] >= 1
    assert ex["summary"]["visuals"] == 1 and ex["summary"]["reports"] == 1 and ex["summary"]["proven"] + ex["summary"]["possible"] == len(ex["exposures"])
