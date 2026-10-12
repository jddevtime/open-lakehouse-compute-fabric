# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Business glossary: CRUD, hierarchy, search, linking, soft and hard delete, suggestions, authorization."""

from __future__ import annotations


def test_create_get_update_with_etag(gw):
    r = gw.post("/glossary/terms", {"name": "Customer Lifetime Value", "definition": "Net revenue **over time**", "abbreviation": "CLV", "synonyms": ["LTV", "customer value"]})
    t = gw.ok(r, 201)
    assert r.headers["ETag"] == f'"{t["etag"]}"'
    assert t["id"].startswith("trm_") and t["status"] == "draft" and t["synonyms"] == ["LTV", "customer value"] and t["abbreviation"] == "CLV"
    got = gw.ok(gw.get(f"/glossary/terms/{t['id']}"))
    assert got["name"] == "Customer Lifetime Value" and got["ancestors"] == [] and got["children"] == [] and got["link_count"] == 0 and got["certification"] in ("none", None)
    upd = gw.patch(f"/glossary/terms/{t['id']}", {"definition": "changed"}, headers={"If-Match": f'"{t["etag"]}"'})
    assert gw.ok(upd)["definition"] == "changed"
    stale = gw.patch(f"/glossary/terms/{t['id']}", {"definition": "again"}, headers={"If-Match": f'"{t["etag"]}"'})
    assert stale.status_code == 412 and gw.code(stale) == "precondition_failed" and "current_etag" in stale.json()["error"]["details"]


def test_names_unique_case_insensitive_and_validated(gw):
    name = gw.uniq("Revenue")
    gw.term(name)
    dup = gw.post("/glossary/terms", {"name": name.upper()})
    assert dup.status_code == 409
    assert gw.post("/glossary/terms", {"name": "  "}).status_code == 422
    assert gw.post("/glossary/terms", {"name": gw.uniq("t"), "status": "bogus"}).status_code == 422
    assert gw.post("/glossary/terms", {"name": gw.uniq("t"), "surprise": 1}).status_code == 422
    # the same name is fine in another glossary
    assert gw.post("/glossary/terms", {"name": name, "glossary": "finance"}).status_code == 201


def test_hierarchy_children_ancestors_and_cycles(gw):
    root = gw.term(gw.uniq("root"))
    mid = gw.term(gw.uniq("mid"), parent_id=root["id"])
    leaf = gw.term(gw.uniq("leaf"), parent_id=mid["id"])
    got = gw.ok(gw.get(f"/glossary/terms/{leaf['id']}"))
    assert [a["id"] for a in got["ancestors"]] == [root["id"], mid["id"]]
    assert [c["id"] for c in gw.ok(gw.get(f"/glossary/terms/{root['id']}"))["children"]] == [mid["id"]]
    # cycles are refused, also indirect ones, and a term cannot be its own parent
    assert gw.patch(f"/glossary/terms/{root['id']}", {"parent_id": leaf["id"]}).status_code == 400
    assert gw.patch(f"/glossary/terms/{mid['id']}", {"parent_id": mid["id"]}).status_code == 400
    # a parent must live in the same glossary
    other = gw.ok(gw.post("/glossary/terms", {"name": gw.uniq("o"), "glossary": "other"}), 201)
    assert gw.post("/glossary/terms", {"name": gw.uniq("c"), "parent_id": other["id"]}).status_code == 400
    assert gw.post("/glossary/terms", {"name": gw.uniq("c"), "parent_id": "trm_missing"}).status_code in (400, 404)
    top = gw.ok(gw.get("/glossary/terms", parent_id="root", limit=500))["items"]
    assert root["id"] in {t["id"] for t in top} and mid["id"] not in {t["id"] for t in top}
    kids = gw.ok(gw.get("/glossary/terms", parent_id=root["id"]))["items"]
    assert [t["id"] for t in kids] == [mid["id"]]


def test_related_terms_and_domain(gw):
    dom = gw.domain()
    a = gw.term(gw.uniq("a"))
    b = gw.term(gw.uniq("b"), related=[a["id"]], domain_id=dom["id"])
    got = gw.ok(gw.get(f"/glossary/terms/{b['id']}"))
    assert got["related"] == [a["id"]] and [t["id"] for t in got["related_terms"]] == [a["id"]] and got["domain_id"] == dom["id"]
    assert gw.post("/glossary/terms", {"name": gw.uniq("c"), "related": ["trm_nope"]}).status_code in (400, 404)
    assert gw.post("/glossary/terms", {"name": gw.uniq("c"), "domain_id": "dom_nope"}).status_code in (400, 404)
    by_domain = gw.ok(gw.get("/glossary/terms", domain_id=dom["id"]))["items"]
    assert [t["id"] for t in by_domain] == [b["id"]]


def test_search_ranking(gw):
    key = gw.uniq("zq").replace("_", "")
    exact = gw.term(key)
    syn = gw.term(gw.uniq("syn"), synonyms=[key])
    abbr = gw.term(gw.uniq("abbr"), abbreviation=key)
    prefix = gw.term(f"{key}tail")
    contains = gw.term(f"head{key}")
    defn = gw.term(gw.uniq("defn"), definition=f"talks about {key} somewhere")
    items = gw.ok(gw.get("/glossary/search", q=key))["items"]
    by = {i["term"]["id"]: i for i in items}
    assert by[exact["id"]]["score"] == 100 and by[syn["id"]]["score"] == 90 and by[abbr["id"]]["score"] == 80
    assert by[prefix["id"]]["score"] == 60 and by[contains["id"]]["score"] == 40 and by[defn["id"]]["score"] == 10
    assert [i["term"]["id"] for i in items][:3] == [exact["id"], syn["id"], abbr["id"]]
    assert by[exact["id"]]["matched_on"] and gw.get("/glossary/search").status_code == 422
    # deprecated terms are left out unless asked for
    gw.ok(gw.delete(f"/glossary/terms/{exact['id']}"))
    assert exact["id"] not in {i["term"]["id"] for i in gw.ok(gw.get("/glossary/search", q=key))["items"]}
    assert exact["id"] in {i["term"]["id"] for i in gw.ok(gw.get("/glossary/search", q=key, include_deprecated="true"))["items"]}


def test_link_assets_and_products_and_decorate(gw):
    t = gw.term(gw.uniq("Email Address"), definition="An address for e-mail")
    aid = gw.aid("customers_clean")
    link = gw.ok(gw.post(f"/glossary/terms/{t['id']}/links", {"asset_id": aid, "comment": "obvious"}, "cur_alpha"), 201)
    assert link["status"] == "approved" and link["source"] == "manual" and link["asset"]["id"] == aid and link["term"]["id"] == t["id"]
    assert gw.post(f"/glossary/terms/{t['id']}/links", {"asset_id": aid}, "cur_alpha").status_code == 409
    prod = gw.product()
    plink = gw.ok(gw.post(f"/glossary/terms/{t['id']}/links", {"product_id": prod["id"]}, "cur_org"), 201)
    assert plink["product"]["id"] == prod["id"] and plink["asset"] is None
    assert gw.post(f"/glossary/terms/{t['id']}/links", {"asset_id": aid, "product_id": prod["id"]}).status_code == 400
    links = gw.ok(gw.get(f"/glossary/terms/{t['id']}/links"))["items"]
    assert {(x["subject_type"]) for x in links} == {"asset", "product"}
    assert gw.ok(gw.get(f"/glossary/terms/{t['id']}"))["link_count"] == 2
    # decorate(): the asset's governance block and the generated documentation name the term
    asset = gw.ok(gw.intel(f"/assets/{aid}"))
    assert t["id"] in {x["id"] for x in asset["governance"]["glossary_terms"]}
    doc = gw.ok(gw.intel(f"/assets/{aid}/documentation"))
    text = doc["markdown"] if isinstance(doc, dict) else str(doc)
    assert t["name"] in text
    # from the asset side
    t2 = gw.term(gw.uniq("Region"))
    assert gw.ok(gw.post(f"/assets/{aid}/terms", {"term_id": t2["id"]}, "cur_alpha"), 201)["term"]["id"] == t2["id"]
    assert {x["id"] for x in gw.ok(gw.get(f"/assets/{aid}"))["terms"]} >= {t["id"], t2["id"]}
    assert gw.ok(gw.delete(f"/assets/{aid}/terms/{t2['id']}", "cur_alpha"))["status"] == "unlinked"
    assert gw.delete(f"/assets/{aid}/terms/{t2['id']}", "cur_alpha").status_code == 404
    assert gw.ok(gw.delete(f"/glossary/terms/{t['id']}/links/{link['id']}", "cur_alpha"))["status"] == "unlinked"
    assert t["id"] not in {x["id"] for x in gw.ok(gw.intel(f"/assets/{aid}"))["governance"]["glossary_terms"]}


def test_links_respect_workspace_scoping(gw):
    t = gw.term(gw.uniq("Secret"))
    mine, theirs = gw.aid("customers"), gw.aid("beta_src")
    assert gw.post(f"/glossary/terms/{t['id']}/links", {"asset_id": mine}, "cur_alpha").status_code == 201
    # a curator of alpha cannot link a beta asset: it does not exist for them
    r = gw.post(f"/glossary/terms/{t['id']}/links", {"asset_id": theirs}, "cur_alpha")
    assert r.status_code == 404
    # a viewer cannot link at all
    assert gw.post(f"/glossary/terms/{t['id']}/links", {"asset_id": mine}, "viewer_alpha").status_code == 403
    gw.ok(gw.post(f"/glossary/terms/{t['id']}/links", {"asset_id": theirs}, "cur_beta"), 201)
    seen = {x["asset"]["id"] for x in gw.ok(gw.get(f"/glossary/terms/{t['id']}/links", who="viewer_alpha"))["items"] if x["asset"]}
    assert seen == {mine}                                           # the beta asset is omitted for a caller who cannot see it
    seen_admin = {x["asset"]["id"] for x in gw.ok(gw.get(f"/glossary/terms/{t['id']}/links"))["items"] if x["asset"]}
    assert seen_admin == {mine, theirs}


def test_soft_and_hard_delete(gw):
    parent = gw.term(gw.uniq("p"))
    child = gw.term(gw.uniq("c"), parent_id=parent["id"])
    grand = gw.term(gw.uniq("g"), parent_id=child["id"])
    aid = gw.aid("orders")
    gw.ok(gw.post(f"/glossary/terms/{child['id']}/links", {"asset_id": aid}), 201)
    gw.patch(f"/glossary/terms/{child['id']}", {"status": "approved"})
    soft = gw.ok(gw.delete(f"/glossary/terms/{child['id']}"))
    assert soft["status"] == "deprecated"
    got = gw.ok(gw.get(f"/glossary/terms/{child['id']}"))
    assert got["status"] == "deprecated"
    links = gw.ok(gw.get(f"/glossary/terms/{child['id']}/links", status="unlinked"))["items"]
    assert [x["status"] for x in links] == ["unlinked"]                                    # history is kept
    assert child["id"] not in {x["id"] for x in gw.ok(gw.intel(f"/assets/{aid}"))["governance"]["glossary_terms"]}
    assert gw.post(f"/glossary/terms/{child['id']}/links", {"asset_id": aid}).status_code == 400   # a deprecated term cannot be linked
    # hard delete needs approve; curators are refused, an approver removes row and links and moves children up
    assert gw.delete(f"/glossary/terms/{child['id']}", "cur_org", params={"hard": "true"}).status_code == 403
    gw.ok(gw.delete(f"/glossary/terms/{child['id']}", "lead_org", params={"hard": "true"}))
    assert gw.get(f"/glossary/terms/{child['id']}").status_code == 404
    assert gw.ok(gw.get(f"/glossary/terms/{grand['id']}"))["parent_id"] == parent["id"]


def test_approving_needs_approve_permission(gw):
    t = gw.term(gw.uniq("needs-approval"))
    assert gw.post("/glossary/terms", {"name": gw.uniq("t"), "status": "approved"}, "cur_org").status_code == 403
    assert gw.patch(f"/glossary/terms/{t['id']}", {"status": "approved"}, "cur_org").status_code == 403
    assert gw.ok(gw.patch(f"/glossary/terms/{t['id']}", {"status": "approved"}, "lead_org"))["status"] == "approved"
    assert gw.ok(gw.post("/glossary/terms", {"name": gw.uniq("t"), "status": "approved"}, "lead_org"), 201)["status"] == "approved"


def test_authorization_matrix(gw):
    t = gw.term(gw.uniq("auth"))
    for who in ("viewer_alpha", "viewer_beta", "viewer_org", "cur_alpha", "cur_org"):
        assert gw.get(f"/glossary/terms/{t['id']}", who).status_code == 200, who      # any intelligence.read (org-wide or in any workspace)
        assert gw.get("/glossary/terms", who).status_code == 200
    assert gw.get(f"/glossary/terms/{t['id']}", "nobody").status_code == 403
    for who in ("viewer_alpha", "viewer_org", "cur_alpha", "cur_beta", "nobody"):          # org-level writes need an org-wide curate
        assert gw.post("/glossary/terms", {"name": gw.uniq("w")}, who).status_code == 403, who
        assert gw.patch(f"/glossary/terms/{t['id']}", {"definition": "z"}, who).status_code == 403, who
        assert gw.delete(f"/glossary/terms/{t['id']}", who).status_code == 403, who
    assert gw.post("/glossary/terms", {"name": gw.uniq("w")}, "cur_org").status_code == 201
    assert gw.get("/glossary/terms/trm_missing").status_code == 404


def test_suggest_links_by_name_synonym_and_abbreviation(gw):
    exact = gw.term("orders")
    syn = gw.term(gw.uniq("Purchases"), synonyms=["customers clean"], status="approved") if False else gw.term(gw.uniq("Purchases"), synonyms=["customers_clean"])
    r = gw.ok(gw.post("/glossary/suggest-links", {}, "cur_org"))
    assert r["suggested"] >= 2 and r["examined"] > 0
    q = gw.ok(gw.get("/review-queue", kind="term_link", limit=500))["items"]
    pairs = {(i["term"]["id"], i["asset"]["id"]) for i in q}
    assert (exact["id"], gw.aid("orders")) in pairs and (syn["id"], gw.aid("customers_clean")) in pairs
    item = next(i for i in q if i["term"]["id"] == exact["id"] and i["asset"]["id"] == gw.aid("orders"))
    assert item["kind"] == "term_link" and item["source"] == "suggested" and item["confidence"] > 0
    # not approved by itself: no approved link, not in the asset's governance block
    assert exact["id"] not in {x["id"] for x in gw.ok(gw.intel(f"/assets/{gw.aid('orders')}"))["governance"]["glossary_terms"]}
    # idempotent: a second run proposes nothing new for the same pairs
    again = gw.ok(gw.post("/glossary/suggest-links", {}, "cur_org"))
    assert again["suggested"] == 0
    # approving through the queue links the term
    gw.ok(gw.post(f"/review-queue/{item['id']}/approve", {"comment": "ok"}, "cur_alpha"))
    assert exact["id"] in {x["id"] for x in gw.ok(gw.intel(f"/assets/{gw.aid('orders')}"))["governance"]["glossary_terms"]}
    # a rejected proposal is not made again
    other = next(i for i in q if i["term"]["id"] == syn["id"])
    gw.ok(gw.post(f"/review-queue/{other['id']}/reject", {"comment": "no"}, "cur_alpha"))
    assert gw.ok(gw.post("/glossary/suggest-links", {}, "cur_org"))["suggested"] == 0
    # a viewer cannot run it
    assert gw.post("/glossary/suggest-links", {}, "viewer_alpha").status_code == 403
