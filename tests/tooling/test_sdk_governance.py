# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``client.governance`` (sync and async) against the real application: glossary, domains, products, owners and inheritance, classifications with exposure, the review
queue, certification and change requests, policies and violations, quality and the overview, with ETags, pagination iterators, error mapping, permissions and the
hidden existence of assets the caller cannot see."""

from __future__ import annotations

import pytest

from olcf.sdk import (
    BadRequestError,
    ConflictError,
    NotFoundError,
    PermissionDeniedError,
    PreconditionFailedError,
    UnsupportedError,
    ValidationError,
)
from olcf.sdk._core import Page, Resource
from tests.intelligence.surfaces_support import Stack, intel_stack
from tests.tooling.test_sdk_intelligence import (
    async_api,  # noqa: F401  (fixture: an application with a tiny graph and a logged-in AsyncClient)
)


@pytest.fixture(scope="module")
def stack(tmp_path_factory):
    with intel_stack(tmp_path_factory.mktemp("sdk_gov")) as st:
        yield st


@pytest.fixture()
def gov(stack: Stack):
    return stack.sdk().governance


# ------------------------------------------------------------------------------------------------------------------------------ glossary
def test_term_crud_etag_and_conflicts(stack, gov):
    parent = gov.create_term("Money metrics", definition="Everything measured in currency")
    assert isinstance(parent, Resource) and parent.etag and parent["id"].startswith("trm_") and parent["status"] == "draft" and parent["certification"] == "none"
    child = gov.create_term("Booked revenue", definition="Revenue booked in the period", abbreviation="BKR", synonyms=["bookings"], parent_id=parent["id"])
    assert child["parent_id"] == parent["id"] and child["synonyms"] == ["bookings"]
    full = gov.get_term(child["id"])
    assert [a["name"] for a in full["ancestors"]] == ["Money metrics"] and full["etag"] == child.etag
    assert [c["name"] for c in gov.get_term(parent["id"])["children"]] == ["Booked revenue"]
    with pytest.raises(ConflictError):
        gov.create_term("booked REVENUE")                                      # unique per glossary, case-insensitive
    with pytest.raises(BadRequestError):
        gov.update_term(parent["id"], parent_id=child["id"])                   # a cycle
    changed = gov.update_term(child["id"], child, definition="Revenue booked, net of credit notes")
    assert changed["definition"].endswith("credit notes") and changed.etag != child.etag
    with pytest.raises(PreconditionFailedError):
        gov.update_term(child["id"], child, definition="a stale write")
    with pytest.raises(NotFoundError):
        gov.get_term("trm_NOPE")
    soft = gov.delete_term(child["id"], if_match=changed)
    assert soft["status"] == "deprecated" and soft["deprecated"] is True and soft["deleted"] is False
    assert gov.get_term(child["id"])["status"] == "deprecated"
    assert gov.delete_term(child["id"], hard=True)["deleted"] is True
    with pytest.raises(NotFoundError):
        gov.get_term(child["id"])
    assert [c["name"] for c in gov.get_term(parent["id"])["children"]] == []


def test_term_permissions_curators_create_and_approvers_approve(stack):
    viewer, curator, admin = (stack.sdk(w).governance for w in ("org", "curator_org", "admin"))
    with pytest.raises(PermissionDeniedError):
        viewer.create_term("Viewer term")
    draft = curator.create_term("Curated term")
    assert draft["status"] == "draft"
    with pytest.raises(PermissionDeniedError):
        curator.create_term("Self approved term", status="approved")
    with pytest.raises(PermissionDeniedError):
        curator.update_term(draft["id"], status="approved")
    approved = admin.update_term(draft["id"], draft, status="approved")
    assert approved["status"] == "approved"
    with pytest.raises(PermissionDeniedError):
        curator.delete_term(draft["id"], hard=True)


def test_glossary_search_ranking_and_iteration(gov):
    gov.create_term("Gross margin", definition="Revenue minus cost of goods", abbreviation="GM", synonyms=["gross profit"])
    gov.create_term("Margin call", definition="A demand to add funds")
    gov.create_term("Net margin", definition="Profit after all costs, as a share of the gross margin")
    hits = gov.search_glossary("gross margin")
    assert isinstance(hits, Page) and hits[0]["term"]["name"] == "Gross margin" and hits[0]["score"] == 100 and hits[0]["matched_on"] == "name"
    by_abbr = gov.search_glossary("gm")
    assert by_abbr[0]["term"]["name"] == "Gross margin" and by_abbr[0]["matched_on"] == "abbreviation" and by_abbr[0]["score"] == 80
    syn = gov.search_glossary("gross profit")
    assert syn[0]["matched_on"] == "synonym" and syn[0]["score"] == 90
    assert {h["term"]["name"] for h in gov.search_glossary("margin")} >= {"Gross margin", "Margin call", "Net margin"}
    assert len(gov.search_glossary("margin", limit=1)) == 1 and gov.search_glossary("zzz-nothing-matches") == []
    page = gov.list_terms(q="margin", limit=2)
    assert len(page) == 2 and page.next_cursor
    every = list(gov.iter_terms(q="margin", limit=1))
    assert {t["name"] for t in every} == {"Gross margin", "Margin call", "Net margin"}
    assert [t["name"] for t in gov.list_terms(q="margin", status="approved")] == []
    assert any(t["name"] == "Money metrics" for t in gov.list_terms(parent_id="root", limit=100))


def test_term_links_from_both_sides_and_unlinking(stack, gov):
    ids = stack.ids
    term = gov.create_term("Cleaned amount", definition="Amount after cleansing")
    link = gov.link_term(term["id"], asset_id=ids["clean"], comment="the cleansed table")
    assert link["status"] == "approved" and link["source"] == "manual" and link["asset"]["name"] == "clean" and link["term"]["id"] == term["id"]
    other = gov.add_asset_term(ids["mart"], term["id"])
    assert other["asset"]["name"] == "mart"
    with pytest.raises(BadRequestError):
        gov.link_term(term["id"])                                              # neither asset nor product
    links = gov.term_links(term["id"])
    assert {lk["asset"]["name"] for lk in links} == {"clean", "mart"}
    assert {lk["asset"]["name"] for lk in gov.iter_term_links(term["id"], limit=1)} == {"clean", "mart"}
    assert gov.get_term(term["id"])["link_count"] == 2
    gov.unlink_term(term["id"], link["id"])
    gov.remove_asset_term(ids["mart"], term["id"])
    assert list(gov.term_links(term["id"])) == []
    assert [lk["status"] for lk in gov.term_links(term["id"], status="unlinked")] == ["unlinked", "unlinked"]


# ------------------------------------------------------------------------------------------------------------------------------ domains and products
def test_domains_products_membership_and_consumers(stack, gov):
    ids = stack.ids
    parent = gov.create_domain("Commercial", description="Sales and marketing")
    sub = gov.create_domain("Sales analytics", parent_id=parent["id"])
    assert sub["parent_id"] == parent["id"] and [a["name"] for a in gov.get_domain(sub["id"])["ancestors"]] == ["Commercial"]
    assert [d["name"] for d in gov.list_domains(parent_id="root", q="commercial")] == ["Commercial"]
    assert [d["name"] for d in gov.iter_domains(q="analytics", limit=1)] == ["Sales analytics"]
    renamed = gov.update_domain(sub["id"], sub, name="Sales insight")
    assert renamed["name"] == "Sales insight"
    with pytest.raises(PreconditionFailedError):
        gov.update_domain(sub["id"], sub, name="stale")
    assigned = gov.set_asset_domain(ids["mart"], sub["id"])
    assert assigned["domain"]["id"] == sub["id"]
    in_domain = gov.domain_assets(sub["id"])
    assert [a["name"] for a in in_domain] == ["mart"] and in_domain[0]["domain_source"] == "direct"
    assert [a["name"] for a in gov.iter_domain_assets(parent["id"], limit=1)] == ["mart"]             # through the sub-domain
    assert list(gov.domain_assets(parent["id"], include_descendants=False)) == []
    with pytest.raises(ConflictError):
        gov.delete_domain(sub["id"])                                           # an asset is assigned
    gov.set_asset_domain(ids["mart"], None)
    assert list(gov.domain_assets(sub["id"])) == []

    prod = gov.create_product("Revenue mart", description="Monthly revenue", domain_id=parent["id"], member_asset_ids=[ids["mart"]], sla={"freshness_hours": 24})
    assert prod["id"].startswith("dpr_") and prod["status"] == "draft" and prod["member_count"] == 1 and prod["domain"]["name"] == "Commercial"
    assert [m["name"] for m in gov.get_product(prod["id"])["members"]] == ["mart"]
    consumers = gov.product_consumers(prod["id"])
    assert {c["asset"]["name"] for c in consumers["items"]} == {"sales_model", "sales_report"} and consumers["total"] == 2 and consumers["truncated"] is False
    assert {c["asset"]["name"]: c["hops"] for c in consumers["items"]} == {"sales_model": 1, "sales_report": 2}
    assert [c["asset"]["name"] for c in gov.product_consumers(prod["id"], depth=1)["items"]] == ["sales_model"]
    assert {c["asset"]["name"] for c in gov.iter_product_consumers(prod["id"])} == {"sales_model", "sales_report"}
    added = gov.add_product_member(prod["id"], ids["lonely"])
    assert added is not None and gov.get_product(prod["id"])["member_count"] == 2
    gov.remove_product_member(prod["id"], ids["lonely"])
    gov.set_product_members(prod["id"], [ids["mart"], ids["clean"]])
    assert {m["name"] for m in gov.get_product(prod["id"])["members"]} == {"mart", "clean"}
    assert [p["name"] for p in gov.list_products(domain_id=parent["id"])] == ["Revenue mart"]
    assert [p["name"] for p in gov.iter_products(q="revenue", limit=1)] == ["Revenue mart"]
    with pytest.raises(BadRequestError):
        gov.add_product_member(prod["id"], ids["raw.amount"])                  # a column cannot be a member
    with pytest.raises(PermissionDeniedError):
        stack.sdk("curator_org").governance.create_product("Published by a curator", status="published")
    published = gov.update_product(prod["id"], gov.get_product(prod["id"]), status="published")
    assert published["status"] == "published"
    with pytest.raises(ConflictError):
        gov.delete_product(prod["id"])                                         # deprecate first
    dep = gov.update_product(prod["id"], published, status="deprecated")
    gov.delete_product(prod["id"], if_match=dep)
    with pytest.raises(NotFoundError):
        gov.get_product(prod["id"])
    gov.delete_domain(sub["id"])
    gov.delete_domain(parent["id"])


# ------------------------------------------------------------------------------------------------------------------------------ owners
def test_owners_on_every_kind_of_subject_with_inheritance(stack, gov):
    ids = stack.ids
    first = gov.add_owner("asset", ids["raw"], {"role": "owner", "user": "admin"})
    assert first["role"] == "owner" and first["kind"] == "user" and isinstance(first["id"], int)
    with pytest.raises(ConflictError):
        gov.add_owner("asset", ids["raw"], {"role": "owner", "user": "admin"})
    ext = gov.add_owner("asset", ids["raw"], {"role": "steward", "name": "Data Office", "email": "data-office@example.com"})
    assert ext["kind"] == "external" and ext["email"] == "data-office@example.com"
    got = gov.owners("asset", ids["raw"])
    assert {(o["role"], o["name"]) for o in got["direct"]} == {("owner", first["name"]), ("steward", "Data Office")} and got["subject_type"] == "asset"
    inherited = gov.owners("asset", ids["raw.amount"])                         # a column of raw
    assert inherited["direct"] == [] and {o["role"] for o in inherited["effective"]} == {"owner", "steward"}
    assert all(o["inherited_from"]["name"] == "raw" for o in inherited["effective"])
    left = gov.remove_owner("asset", ids["raw"], ext["id"])
    assert [o["role"] for o in left["direct"]] == ["owner"]
    replaced = gov.set_owners("asset", ids["raw"], [{"role": "custodian", "name": "Platform team"}])
    assert [o["role"] for o in replaced["direct"]] == ["custodian"]
    gov.set_owners("asset", ids["raw"], [])
    assert gov.owners("asset", ids["raw"])["direct"] == []

    term, dom, prod = gov.create_term("Owned term"), gov.create_domain("Owned domain"), gov.create_product("Owned product")
    for kind, subject in (("term", term), ("domain", dom), ("product", prod)):
        o = gov.add_owner(kind, subject["id"], {"role": "owner", "user": "admin"})
        assert gov.owners(kind, subject["id"])["direct"][0]["id"] == o["id"]
        assert gov.set_owners(kind, subject["id"], [])["direct"] == []
    assert [o["name"] for o in gov.get_domain(dom["id"])["owners"]] == []
    with pytest.raises(ValueError, match="subject_type"):
        gov.owners("workspace", "x")                                           # type: ignore[arg-type]
    with pytest.raises(PermissionDeniedError):
        stack.sdk("org").governance.add_owner("asset", ids["raw"], {"role": "owner", "user": "admin"})
    with pytest.raises(NotFoundError):
        stack.sdk("alpha").governance.owners("asset", ids["secret"])           # an asset of workspace beta does not exist for alpha
    with pytest.raises(ValidationError):
        gov.add_owner("asset", ids["raw"], {"role": "janitor", "name": "x"})


# ------------------------------------------------------------------------------------------------------------------------------ classifications
def test_classification_catalog_assignment_inheritance_and_exposure(stack, gov):
    ids = stack.ids
    catalog = gov.list_classifications()
    assert {"Public", "Internal", "Confidential", "PII", "PCI", "Financial", "Credentials", "Restricted"} <= {c["name"] for c in catalog}
    assert [c["name"] for c in gov.iter_classifications()] == [c["name"] for c in catalog]
    pii = next(c for c in catalog if c["name"] == "PII")
    assert pii["builtin"] and gov.get_classification(pii["id"])["name"] == "PII"
    custom = gov.create_classification("Embargoed", "handling", 4, description="Not before the announcement", color="#112233")
    assert custom["builtin"] is False and custom.etag and custom["level"] == 4
    updated = gov.update_classification(custom["id"], custom, color="#445566")
    assert updated["color"] == "#445566"
    with pytest.raises(PreconditionFailedError):
        gov.update_classification(custom["id"], custom, color="#000000")
    with pytest.raises(BadRequestError):
        gov.update_classification(pii["id"], name="Renamed PII")               # only description and colour of a built-in label
    with pytest.raises(ValidationError):
        gov.create_classification("Too strict", "handling", 9)

    assigned = gov.classify_asset(ids["raw.amount"], "PII", comment="contains personal amounts")
    assert assigned["status"] == "approved" and assigned["classification"]["name"] == "PII"
    on_raw = gov.asset_classifications(ids["raw"])
    assert [c["classification"]["name"] for c in on_raw["contained"]] == ["PII"] and on_raw["direct"] == []
    on_clean_amount = gov.asset_classifications(ids["clean.amount"])
    inherited = on_clean_amount["inherited"][0]
    assert inherited["classification"]["name"] == "PII" and inherited["precision"] == "column" and inherited["source"] == "inherited" and inherited["evidence"]["path"]["edges"]
    exposure = gov.classification_exposure(pii["id"])
    assert exposure["sources"][0]["asset"]["name"] == "amount"
    by_name = {e["asset"]["name"]: e for e in exposure["exposures"] if e["asset"]["type"] != "column"}
    assert by_name["clean"]["proven"] is True and by_name["clean"]["precision"] == "contains"
    assert by_name["mart"]["proven"] is False and by_name["mart"]["precision"] == "table" and by_name["mart"]["confidence_floor"] in ("inferred", "unknown")
    assert by_name["sales_model"]["kind"] == "semantic_model" and "sales_report" not in by_name and exposure["summary"]["reports"] == 0    # report -> model is only inferred
    assert exposure["truncated"] is False and exposure["notes"] and exposure["summary"]["proven"] == 2 and exposure["summary"]["possible"] == 3
    weak = gov.classification_exposure(pii["id"], min_confidence="inferred")
    report = next(e for e in weak["exposures"] if e["asset"]["name"] == "sales_report")
    assert report["kind"] == "report" and report["proven"] is False and report["confidence_floor"] == "inferred" and weak["summary"]["reports"] == 1
    assert [n["name"] for n in report["path"]["nodes"]][0] == "raw" and len(report["path"]["edges"]) >= 3
    proven_only = gov.classification_exposure(pii["id"], include_possible=False)
    assert "mart" not in {e["asset"]["name"] for e in proven_only["exposures"]} and "clean" in {e["asset"]["name"] for e in proven_only["exposures"]}
    reports = gov.classification_exposure(pii["id"], types=["report", "semantic_model"], min_confidence="inferred")
    assert {e["asset"]["type"] for e in reports["exposures"]} == {"report", "semantic_model"}
    assert [e["asset"]["name"] for e in gov.classification_exposure(pii["id"], types="report", min_confidence="inferred")["exposures"]] == ["sales_report"]
    assert {a["asset"]["name"] for a in gov.classification_assets(pii["id"])} >= {"amount", "clean"}
    direct_only = gov.classification_assets(pii["id"], include_inherited=False)
    assert [a["asset"]["name"] for a in direct_only] == ["amount"] and direct_only[0]["source"] == "direct"
    assert {a["source"] for a in gov.iter_classification_assets(pii["id"], limit=1)} >= {"direct", "inherited"}

    with pytest.raises(ConflictError):
        gov.delete_classification(pii["id"])                                   # built in
    gov.classify_asset(ids["lonely"], "Embargoed")
    with pytest.raises(ConflictError):
        gov.delete_classification(custom["id"])                                # still assigned
    gov.unclassify_asset(ids["lonely"], "Embargoed")
    gov.unclassify_asset(ids["raw.amount"], pii["id"])
    assert gov.asset_classifications(ids["clean.amount"])["inherited"] == []
    gov.delete_classification(custom["id"], if_match=updated)
    with pytest.raises(NotFoundError):
        gov.get_classification(custom["id"])
    with pytest.raises(PermissionDeniedError):
        stack.sdk("org").governance.classify_asset(ids["raw"], "PII")
    with pytest.raises(NotFoundError):
        gov.classify_asset(ids["raw"], "No such label")


def test_classification_exposure_never_shows_hidden_assets(stack, gov):
    ids = stack.ids
    gov.classify_asset(ids["secret"], "Restricted")                            # a table of workspace beta that feeds beta_nb -> secret_out
    try:
        restricted = next(c for c in gov.list_classifications() if c["name"] == "Restricted")
        full = gov.classification_exposure(restricted["id"])
        assert "secret_out" in {e["asset"]["name"] for e in full["exposures"]}
        seen_by_alpha = stack.sdk("alpha").governance.classification_exposure(restricted["id"])
        flat = repr(seen_by_alpha)
        assert "secret_out" not in flat and "secret" not in {s["asset"].get("name") for s in seen_by_alpha["sources"]}
        assert stack.sdk("alpha").governance.classification_assets(restricted["id"]) == []
        with pytest.raises(NotFoundError):
            stack.sdk("alpha").governance.asset_classifications(ids["secret"])
    finally:
        gov.unclassify_asset(ids["secret"], "Restricted")


# ------------------------------------------------------------------------------------------------------------------------------ review queue
def test_review_queue_suggestions_approve_reject_and_bulk(stack, gov):
    ids = stack.ids
    gov.create_term("lonely", definition="A table nobody uses")
    gov.create_term("fresh", definition="A table that was just created")
    gov.create_term("mart", definition="The reporting mart")
    result = gov.suggest_term_links()
    assert result["suggested"] == 3 and result["examined"] >= 7
    queue = gov.review_queue(kind="term_link")
    assert isinstance(queue, Page) and {i["asset"]["name"] for i in queue} == {"lonely", "fresh", "mart"}
    item = next(i for i in queue if i["asset"]["name"] == "lonely")
    assert item["id"].startswith("term_link:") and item["source"] == "suggested" and item["requires"] == "governance.curate" and item["evidence"]["rule"] == "name-matches-name"
    assert 0 < item["confidence"] < 1 and item["term"]["name"] == "lonely"
    summary = gov.review_summary()
    assert summary["term_link"] == 3 and summary["total"] >= 3 and summary["change_requests"] == 0
    assert [i["asset"]["name"] for i in gov.review_queue(asset_id=ids["fresh"])] == ["fresh"]
    assert {i["id"] for i in gov.iter_review_queue(kind="term_link", limit=1)} >= {i["id"] for i in queue}
    approved = gov.approve_review(item["id"], comment="yes, that is the term")
    assert approved["decision"] == "approved" and approved["asset_id"] == item["asset"]["id"] and approved["term"]["name"] == "lonely"
    assert any(lk["asset"]["name"] == "lonely" for lk in gov.term_links(item["term"]["id"]))
    other = next(i for i in gov.review_queue(asset_id=ids["fresh"]))
    rejected = gov.reject_review(other["id"], comment="not the same thing")
    assert rejected["decision"] == "rejected"
    assert gov.review_queue(asset_id=ids["fresh"]) == []
    gov.suggest_term_links()                                                   # a rejected suggestion is not proposed again
    assert gov.review_queue(asset_id=ids["fresh"]) == []
    left = gov.review_queue(asset_id=ids["mart"])
    bulk = gov.review_bulk("approve", [left[0]["id"], "term_link:999999"], comment="bulk")
    assert bulk["ok"] == 1 and bulk["failed"] == 1 and {r["id"] for r in bulk["results"] if not r["ok"]} == {"term_link:999999"}
    malformed = gov.review_bulk("approve", ["classification-1"])
    assert malformed["ok"] == 0 and malformed["failed"] == 1 and malformed["results"][0]["error"]["code"]
    with pytest.raises(NotFoundError):
        gov.approve_review("term_link:424242")
    with pytest.raises(PermissionDeniedError):
        stack.sdk("org").governance.approve_review("term_link:1")


def test_scan_classifications_ingests_suggestions_without_approving(stack, gov):
    out = gov.scan_classifications()
    assert isinstance(out, Resource) and out["assets_examined"] >= 19 and out["classification"]["created"] == 0 and "term_links" in out
    with pytest.raises(PermissionDeniedError):
        stack.sdk("org").governance.scan_classifications()


# ------------------------------------------------------------------------------------------------------------------------------ certification and changes
def test_certification_states_and_permissions(stack, gov):
    ids = stack.ids
    c0 = gov.certification("asset", ids["fresh"])
    assert c0["state"] == "none" and c0["subject_id"] == ids["fresh"] and c0["certified_by"] is None
    curator = stack.sdk("curator_org").governance
    draft = curator.set_certification("asset", ids["fresh"], "draft", comment="working on it")
    assert draft["state"] == "draft"
    with pytest.raises(PermissionDeniedError):
        curator.set_certification("asset", ids["fresh"], "certified")
    done = gov.set_certification("asset", ids["fresh"], "certified", comment="reviewed", if_match=draft)
    assert done["state"] == "certified" and done["certified_by"] and done["comment"] == "reviewed" and done["schema_changed_since"] is False
    with pytest.raises(PreconditionFailedError):
        gov.set_certification("asset", ids["fresh"], "deprecated", if_match=draft)
    assert any(c["subject_id"] == ids["fresh"] for c in gov.list_certifications())
    assert [c["subject"]["name"] for c in gov.list_certifications(state="certified", subject_type="asset") if c["subject_id"] == ids["fresh"]] == ["fresh"]
    assert list(gov.iter_certifications(state="deprecated", subject_type="product")) == []
    term = gov.create_term("Certifiable term")
    with pytest.raises(BadRequestError):
        gov.set_certification("term", term["id"], "certified")                 # a term must be approved first
    gov.update_term(term["id"], term, status="approved")
    assert gov.set_certification("term", term["id"], "certified")["state"] == "certified"
    prod = gov.create_product("Certifiable product")
    assert gov.set_certification("product", prod["id"], "certified")["state"] == "certified"
    assert gov.certification("product", prod["id"])["state"] == "certified"
    with pytest.raises(ValueError, match="subject_type"):
        gov.certification("domain", "dom_x")                                   # type: ignore[arg-type]
    gov.set_certification("asset", ids["fresh"], "none")


def test_change_request_workflow_comment_approve_reject_withdraw_and_apply(stack, gov):
    ids = stack.ids
    curator = stack.sdk("curator_org").governance
    req = curator.request_change("certify", "asset", ids["lonely"], {"state": "certified"}, comment="please certify")
    assert req["id"].startswith("chg_") and req["status"] == "pending" and req["requester"]["name"] == "c_org" and req.etag
    assert [c["id"] for c in curator.list_changes(requester="me", status="pending")] == [req["id"]]
    commented = curator.comment_change(req["id"], "ping")
    assert commented["comments"][-1]["text"] == "ping"
    assert gov.get_change(req["id"])["comments"][-1]["action"] == "comment"
    with pytest.raises(PermissionDeniedError):
        curator.approve_change(req["id"])                                      # no approval right
    with pytest.raises(ValidationError):
        gov.reject_change(req["id"], "")                                       # a reason is required
    done = gov.approve_change(req["id"], comment="looks right")
    assert done["status"] == "approved" and done["applied_at"] and done["decided_by"]
    assert gov.certification("asset", ids["lonely"])["state"] == "certified"
    assert gov.apply_change(req["id"])["status"] == "approved"                 # idempotent
    with pytest.raises(ConflictError):
        gov.approve_change(req["id"])                                          # already decided

    second = curator.request_change("deprecate", "asset", ids["lonely"])
    rejected = gov.reject_change(second["id"], "still in use")
    assert rejected["status"] == "rejected" and rejected["decision_comment"] == "still in use"
    third = curator.request_change("classification_change", "asset", ids["lonely"], {"add": ["Internal"]})
    assert curator.withdraw_change(third["id"])["status"] == "withdrawn"
    own = gov.request_change("certify", "asset", ids["mart"], {"state": "certified"})
    with pytest.raises(PermissionDeniedError):
        gov.approve_change(own["id"])                                          # not your own request (self approval is off)
    assert {c["id"] for c in gov.iter_changes(subject_id=ids["lonely"], limit=1)} == {req["id"], second["id"], third["id"]}
    assert [c["id"] for c in gov.list_changes(type="certify", status="approved", subject_id=ids["lonely"])] == [req["id"]]
    cls_req = curator.request_change("classification_change", "asset", ids["lonely"], {"add": ["Internal"]})
    gov.approve_change(cls_req["id"])
    assert [c["classification"]["name"] for c in gov.asset_classifications(ids["lonely"])["direct"]] == ["Internal"]
    gov.unclassify_asset(ids["lonely"], "Internal")
    with pytest.raises(BadRequestError):
        curator.request_change("certify", "asset", ids["lonely"], {"state": "bogus"})
    with pytest.raises(NotFoundError):
        gov.get_change("chg_NOPE")


# ------------------------------------------------------------------------------------------------------------------------------ policies and violations
def test_policies_preview_evaluate_violations_and_exceptions(stack, gov):
    ids = stack.ids
    types = {r["rule_type"] for r in gov.rule_types()}
    assert types == {"classification_flow", "required_owner", "required_description", "required_classification", "certified_sources_only", "freshness", "quality_min"}
    preview = gov.preview_policy("required_description", {"types": ["table"], "min_length": 5}, limit=3)
    assert preview["evaluated"] == 7 and preview["would_report"] == 7 and len(preview["items"]) == 3 and preview["items"][0]["message"].endswith("(minimum 5)")
    assert list(gov.list_policies(rule_type="required_description")) == []     # a preview stores nothing
    with pytest.raises(UnsupportedError):
        gov.create_policy("Blocking", "required_owner", {"types": ["table"]}, mode="enforce")
    with pytest.raises(BadRequestError):
        gov.create_policy("Bad params", "required_owner", {"types": ["spaceship"]})
    with pytest.raises(PermissionDeniedError):
        stack.sdk("curator_org").governance.create_policy("Not allowed", "required_owner", {})
    pol = gov.create_policy("Tables have descriptions", "required_description", {"types": ["table"], "min_length": 5}, description="Every table is documented", severity="medium")
    assert pol["id"].startswith("plc_") and pol["mode"] == "observe" and pol["enabled"] and pol["needs_evaluation"] and pol.etag
    res = gov.evaluate_policy(pol["id"])
    assert res["policy_id"] == pol["id"] and res["evaluated"] >= 1 and res["opened"] >= 1
    again = gov.evaluate_policy(pol["id"])
    assert again["opened"] == 0 and again["updated"] >= 1                      # idempotent: the same violations are updated
    all_results = gov.evaluate_policies()
    assert pol["id"] in {r["policy_id"] for r in all_results["results"]}
    violations = list(gov.iter_violations(policy_id=pol["id"], limit=2))
    assert len(violations) >= 3 and all(v["status"] == "open" and v["severity"] == "medium" and v["policy"]["id"] == pol["id"] for v in violations)
    one = gov.get_violation(violations[0]["id"])
    assert one.etag and one["asset"]["name"] and one["message"] and "evidence" in one
    ack = gov.acknowledge_violation(one["id"], comment="known")
    assert ack["status"] == "acknowledged" and ack["acknowledged_at"]
    assert one["id"] in {v["id"] for v in gov.list_violations(policy_id=pol["id"])}                  # acknowledged still reported
    assert gov.reopen_violation(one["id"])["status"] == "open"
    assert [v["asset"]["id"] for v in gov.list_violations(policy_id=pol["id"], asset_id=ids["lonely"])] == [ids["lonely"]]
    assert list(gov.list_violations(policy_id=pol["id"], severity="critical")) == []
    assert list(gov.list_violations(policy_id=pol["id"], status="resolved")) == []

    # descriptions are edited through the intelligence client
    stack.sdk("curator_org").intelligence.update_asset(ids["lonely"], description="An orphan table kept for tests")
    after = gov.evaluate_policy(pol["id"])
    assert after["resolved"] == 1
    assert ids["lonely"] not in {v["asset"]["id"] for v in gov.list_violations(policy_id=pol["id"])}
    assert [v["status"] for v in gov.list_violations(policy_id=pol["id"], status="resolved")] == ["resolved"]

    renamed = gov.update_policy(pol["id"], pol, severity="high", enabled=False)
    assert renamed["severity"] == "high" and renamed["enabled"] is False
    with pytest.raises(PreconditionFailedError):
        gov.update_policy(pol["id"], pol, severity="low")
    assert [p["id"] for p in gov.list_policies(enabled=False)] == [pol["id"]] and pol["id"] in {p["id"] for p in gov.iter_policies(limit=1)}
    assert gov.get_policy(pol["id"])["last_result"]["resolved"] == 1
    gov.delete_policy(pol["id"], if_match=renamed)
    with pytest.raises(NotFoundError):
        gov.get_policy(pol["id"])
    assert list(gov.list_violations(policy_id=pol["id"], status="any")) == []             # the violations went with it
    stack.sdk("curator_org").intelligence.update_asset(ids["lonely"], description="")


def test_policy_exception_through_a_change_request(stack, gov):
    ids = stack.ids
    pol = gov.create_policy("Tables need owners", "required_owner", {"types": ["table"]}, severity="low")
    gov.evaluate_policy(pol["id"])
    vio = next(v for v in gov.list_violations(policy_id=pol["id"], asset_id=ids["fresh"]))
    req = stack.sdk("curator_org").governance.request_change("policy_exception", "violation", vio["id"], {"reason": "scratch table"})
    gov.approve_change(req["id"])
    assert gov.get_violation(vio["id"])["status"] == "excepted" and gov.get_violation(vio["id"])["exception"]["reason"] == "scratch table"
    assert gov.reopen_violation(vio["id"])["status"] == "open"
    gov.delete_policy(pol["id"])


# ------------------------------------------------------------------------------------------------------------------------------ quality, overview, per asset
def test_quality_summary_asset_view_and_graph(stack, gov):
    ids = stack.ids
    q = gov.quality("asset", ids["clean"])
    assert 0 <= q["score"] <= 100 and q["asset_id"] == ids["clean"]
    comps = {c["name"]: c for c in q["components"]}
    assert set(comps) == {"documentation", "ownership", "classification", "freshness", "certification", "lineage", "violations", "schema_stability"}
    assert comps["documentation"]["reasons"][0]["reason"] == "no description"
    assert abs(sum(c["points"] for c in q["components"] if c["applicable"]) - q["own_score"]) < 0.1
    prod = gov.create_product("Quality product", member_asset_ids=[ids["clean"], ids["mart"]])
    pq = gov.quality("product", prod["id"])
    assert pq["product_id"] == prod["id"] and {m["asset"]["name"] for m in pq["members"]} == {"clean", "mart"}
    with pytest.raises(ValueError, match="subject_type"):
        gov.quality("term", "trm_x")                                           # type: ignore[arg-type]

    s = gov.summary()
    assert {"glossary", "domains", "products", "classifications", "review_queue", "violations", "certification", "coverage", "top_unowned"} <= set(s)
    assert s["coverage"]["datasets"] == 7 and s["glossary"]["terms"] >= 5
    ws = gov.summary(workspace_id=stack.world.ws2["id"])
    assert ws["coverage"]["datasets"] == 2
    term = gov.create_term("Mart term")
    gov.link_term(term["id"], asset_id=ids["mart"])
    gov.classify_asset(ids["mart"], "Internal")
    detail = gov.asset(ids["mart"])
    assert detail["asset"]["name"] == "mart" and {t["name"] for t in detail["terms"]} == {"mart", "Mart term"}
    assert [c["classification"]["name"] for c in detail["classifications"]["direct"]] == ["Internal"] and "score" in detail["quality"] and detail["owners"]["direct"] == []
    graph = gov.asset_graph(ids["mart"])
    assert {e["type"] for e in graph["edges"]} >= {"GOVERNED_BY", "CLASSIFIED_AS"} and all(e["virtual"] for e in graph["edges"])
    assert {n["type"] for n in graph["nodes"]} >= {"asset", "glossary_term", "classification"}
    gov.unclassify_asset(ids["mart"], "Internal")
    with pytest.raises(NotFoundError):
        stack.sdk("alpha").governance.asset(ids["secret"])
    with pytest.raises(NotFoundError):
        stack.sdk("alpha").governance.quality("asset", ids["secret"])
    with pytest.raises(NotFoundError):
        stack.sdk("alpha").governance.asset_graph(ids["secret"])
    assert stack.sdk("alpha").governance.asset(ids["clean"])["asset"]["name"] == "clean"
    assert stack.sdk("none").governance is not None
    with pytest.raises(PermissionDeniedError):
        stack.sdk("none").governance.summary()


def test_intelligence_asset_filters_use_governance(stack, gov):
    ids = stack.ids
    dom = gov.create_domain("Filter domain")
    gov.set_asset_domain(ids["mart"], dom["id"])
    gov.classify_asset(ids["mart"], "Confidential")
    gov.set_certification("asset", ids["mart"], "certified")
    api = stack.sdk().intelligence
    assert [a["name"] for a in api.list_assets(domain=dom["id"])] == ["mart"]
    assert {a["name"] for a in api.list_assets(classification="Confidential")} == {"mart", "sales_model"}                 # the label flows downstream
    certified = {a["name"] for a in api.list_assets(certification="certified", type="table")}
    assert "mart" in certified and "raw" not in certified
    shown = api.get_asset(ids["mart"])
    assert shown["governance"]["certification"] == "certified" and shown["governance"]["domain"]["name"] == "Filter domain"
    gov.set_certification("asset", ids["mart"], "none")
    gov.unclassify_asset(ids["mart"], "Confidential")
    gov.set_asset_domain(ids["mart"], None)
    gov.delete_domain(dom["id"])


# ------------------------------------------------------------------------------------------------------------------------------ async client
async def test_async_governance_reads_writes_iterators_and_errors(async_api):    # noqa: F811
    gov = async_api.governance
    intel = async_api.intelligence
    clean = (await intel.list_assets(q="clean", type="table"))[0]
    raw = (await intel.list_assets(q="raw", type="table"))[0]
    term = await gov.create_term("Async term", definition="made over the async client", synonyms=["asynchronous"])
    assert term.etag and term["id"].startswith("trm_")
    assert (await gov.search_glossary("asynchronous"))[0]["matched_on"] == "synonym"
    link = await gov.link_term(term["id"], asset_id=clean["id"])
    assert link["asset"]["name"] == "clean"
    assert [lk["id"] async for lk in gov.iter_term_links(term["id"], limit=1)] == [link["id"]]
    assert [t["name"] async for t in gov.iter_terms(q="async", limit=1)] == ["Async term"]
    updated = await gov.update_term(term["id"], term, definition="changed")
    with pytest.raises(PreconditionFailedError):
        await gov.update_term(term["id"], term, definition="stale")
    assert updated["definition"] == "changed"

    dom = await gov.create_domain("Async domain")
    await gov.set_asset_domain(raw["id"], dom["id"])
    assert [a["name"] async for a in gov.iter_domain_assets(dom["id"])] == ["raw"]
    assert [d["name"] async for d in gov.iter_domains()] == ["Async domain"]
    prod = await gov.create_product("Async product", member_asset_ids=[raw["id"]])
    assert [p["name"] async for p in gov.iter_products()] == ["Async product"]
    assert {c["asset"]["name"] async for c in gov.iter_product_consumers(prod["id"])} == {"clean"}

    assert (await gov.add_owner("asset", clean["id"], {"role": "owner", "name": "Async owner"}))["kind"] == "external"
    assert (await gov.owners("asset", clean["id"]))["direct"][0]["name"] == "Async owner"
    with pytest.raises(ValueError, match="subject_type"):
        await gov.owners("workspace", "x")                                     # type: ignore[arg-type]

    labels = [c["name"] async for c in gov.iter_classifications()]
    assert "PII" in labels
    await gov.classify_asset(raw["id"], "PII")
    pii = next(c for c in await gov.list_classifications() if c["name"] == "PII")
    assert {a["asset"]["name"] async for a in gov.iter_classification_assets(pii["id"])} >= {"raw", "clean"}
    exp = await gov.classification_exposure(pii["id"])
    assert "clean" in {e["asset"]["name"] for e in exp["exposures"]}

    await gov.create_term("raw", definition="The raw layer")
    sug = await gov.suggest_term_links()
    assert sug["suggested"] == 1
    assert (await gov.review_summary())["term_link"] == 1
    queued = [i async for i in gov.iter_review_queue()]
    assert [i["asset"]["name"] for i in queued] == ["raw"]
    assert (await gov.approve_review(queued[0]["id"]))["decision"] == "approved"
    cert = await gov.set_certification("asset", clean["id"], "certified")
    assert cert["state"] == "certified" and [c["subject_id"] async for c in gov.iter_certifications()] == [clean["id"]]
    chg = await gov.request_change("deprecate", "asset", raw["id"])
    assert [c["id"] async for c in gov.iter_changes(status="pending")] == [chg["id"]]
    pol = await gov.create_policy("Async owners", "required_owner", {"types": ["table"]})
    await gov.evaluate_policy(pol["id"])
    assert [p["id"] async for p in gov.iter_policies()] == [pol["id"]]
    assert {v["asset"]["name"] async for v in gov.iter_violations(policy_id=pol["id"])} == {"raw"}
    assert (await gov.summary())["coverage"]["datasets"] == 2
    assert (await gov.quality("asset", clean["id"]))["asset_id"] == clean["id"]
    with pytest.raises(NotFoundError):
        await gov.get_term("trm_NOPE")
    with pytest.raises(UnsupportedError):
        await gov.create_policy("Blocking", "required_owner", {}, mode="enforce")
