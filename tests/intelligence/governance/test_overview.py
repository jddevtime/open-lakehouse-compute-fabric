# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The summary, the per-asset view, the virtual governance edges, the hooks Intelligence calls (``decorate``, ``owners_of``, ``asset_filter``) and the registry merge hook."""

from __future__ import annotations

from sqlalchemy import select

from olcf.intelligence.governance import schema as GS


# ============================================================================================================================ summary
def test_summary_counts_and_coverage(fresh):
    gw = fresh
    s0 = gw.ok(gw.get("/summary"))
    assert s0["coverage"]["datasets"] == 7 and s0["coverage"]["owner_pct"] == 0.0 and s0["coverage"]["certified_pct"] == 0.0 and s0["coverage"]["sampled"] is False
    assert s0["classifications"]["labels"] == 9 and s0["review_queue"]["total"] == 0 and s0["violations"]["open"] == 0 and s0["health"]["tables_available"] is True
    dom, term = gw.domain("Sales"), gw.term(status="approved")
    gw.ok(gw.put(f"/assets/{gw.aid('customers')}/domain", {"domain_id": dom["id"]}, "cur_org"))
    gw.ok(gw.post(f"/assets/{gw.aid('customers')}/terms", {"term_id": term["id"]}, "cur_org"), 201)
    gw.ok(gw.post(f"/assets/{gw.aid('customers')}/owners", {"role": "owner", "name": "Dana"}, "cur_alpha"), 201)
    gw.ok(gw.put(f"/assets/{gw.aid('customers')}/certification", {"state": "certified"}, "lead_org"))
    gw.tick()
    s = gw.ok(gw.get("/summary"))
    cv = s["coverage"]
    assert cv["owner_pct"] == 14.3 and cv["domain_pct"] == 14.3 and cv["term_pct"] == 14.3 and cv["certified_pct"] == 14.3 and cv["classified_pct"] == 0.0
    assert cv["datasets_with_pending_suggestions"] == 1 and cv["classification_reviewed_pct"] == 0.0
    assert s["glossary"]["terms"] >= 1 and s["domains"] == 1 and s["certification"]["by_state"]["certified"] == 1
    assert s["review_queue"] == {"total": 2, "classification": 2, "term_link": 0, "change_requests": 0}
    assert [t["name"] for t in s["top_unclassified"]] == ["customers"] and s["top_unclassified"][0]["pending_suggestions"] == 2
    assert "customers" not in [t["name"] for t in s["top_unowned"]]
    # an approved label makes the dataset "classified"; reviewing every suggestion makes it "reviewed"
    for item in gw.ok(gw.get("/review-queue"))["items"]:
        gw.ok(gw.post(f"/review-queue/{item['id']}/{'approve' if item['asset']['name'] == 'email' else 'reject'}", {}, "cur_alpha"))
    cv2 = gw.ok(gw.get("/summary"))["coverage"]
    assert cv2["classified_pct"] == 14.3 and cv2["classification_reviewed_pct"] == 14.3 and cv2["datasets_with_pending_suggestions"] == 0


def test_summary_is_scoped_to_what_the_caller_can_see(fresh):
    gw = fresh
    assert gw.ok(gw.get("/summary", "viewer_beta"))["coverage"]["datasets"] == 1
    assert gw.ok(gw.get("/summary", "viewer_alpha"))["coverage"]["datasets"] == 6
    assert gw.ok(gw.get("/summary", "viewer_org"))["coverage"]["datasets"] == 7
    by_ws = gw.ok(gw.get("/summary", "viewer_org", workspace_id=gw.ws["beta"]["id"]))
    assert by_ws["coverage"]["datasets"] == 1 and [t["name"] for t in by_ws["top_unowned"]] == ["beta_src"]
    assert "beta_src" not in str(gw.ok(gw.get("/summary", "viewer_alpha")))
    assert gw.get("/summary", "nobody").status_code == 403


# ============================================================================================================================ per asset
def test_asset_detail_has_every_facet(fresh):
    gw = fresh
    aid = gw.aid("customers")
    dom, term, prod = gw.domain(), gw.term(status="approved"), gw.product()
    gw.ok(gw.put(f"/assets/{aid}/domain", {"domain_id": dom["id"]}, "cur_org"))
    gw.ok(gw.post(f"/assets/{aid}/terms", {"term_id": term["id"]}, "cur_org"), 201)
    gw.ok(gw.post(f"/products/{prod['id']}/members", {"asset_id": aid}, "cur_org"), 201)
    gw.ok(gw.post(f"/assets/{aid}/owners", {"role": "owner", "name": "Dana"}, "cur_alpha"), 201)
    gw.ok(gw.classify("customers", "Confidential", "cur_alpha"), 201)
    gw.ok(gw.post("/changes", {"type": "certify", "subject_type": "asset", "subject_id": aid, "payload": {}}, "cur_alpha"), 201)
    d = gw.ok(gw.get(f"/assets/{aid}", "viewer_alpha"))
    assert d["asset"]["id"] == aid and d["domain"]["domain"]["id"] == dom["id"] and d["domain"]["source"] == "direct"
    assert [x["domain"]["id"] for x in d["domains"]] == [dom["id"]] and [p["id"] for p in d["products"]] == [prod["id"]]
    assert [t["id"] for t in d["terms"]] == [term["id"]] and d["owners"]["direct"][0]["name"] == "Dana" and d["owners"]["effective"]
    assert [c["classification"]["name"] for c in d["classifications"]["direct"]] == ["Confidential"]
    assert d["certification"]["state"] == "none" and d["quality"]["score"] is not None and d["violations"] == []
    assert len(d["open_changes"]) == 1 and d["open_changes"][0]["type"] == "certify"
    # a column and a lakehouse work too
    assert gw.ok(gw.get(f"/assets/{gw.aid('customers.email')}", "viewer_alpha"))["asset"]["type"] == "column"
    assert gw.ok(gw.get(f"/assets/{gw.aid('lakehouse_a')}", "viewer_alpha"))["quality"]["score"] is None
    # hidden is not found, missing is not found, no role is forbidden
    assert gw.get(f"/assets/{gw.aid('beta_src')}", "viewer_alpha").status_code == 404
    assert gw.get("/assets/ast_nope").status_code == 404
    assert gw.get(f"/assets/{aid}", "nobody").status_code == 403


def test_domain_through_product_is_listed_with_its_source(fresh):
    gw = fresh
    aid = gw.aid("raw_dump")
    dom = gw.domain()
    prod = gw.product(domain_id=dom["id"])
    gw.ok(gw.post(f"/products/{prod['id']}/members", {"asset_id": aid}, "cur_org"), 201)
    d = gw.ok(gw.get(f"/assets/{aid}", "viewer_org"))
    assert [(x["domain"]["id"], x["source"]) for x in d["domains"]] == [(dom["id"], "product")]


def test_virtual_governance_edges(fresh):
    gw = fresh
    aid = gw.aid("customers_clean")
    dom, term = gw.domain("Core"), gw.term(status="approved")
    gw.ok(gw.put(f"/assets/{aid}/domain", {"domain_id": dom["id"]}, "cur_org"))
    gw.ok(gw.post(f"/assets/{aid}/terms", {"term_id": term["id"]}, "cur_org"), 201)
    prod = gw.product()
    gw.ok(gw.post(f"/products/{prod['id']}/members", {"asset_id": aid}, "cur_org"), 201)
    gw.ok(gw.classify("customers_clean", "Internal", "cur_alpha"), 201)
    gw.tick()
    gw.ok(gw.classify("customers.email", "PII", "cur_alpha"), 201)           # reaches customers_clean.email by lineage, and customers_clean by containment
    g = gw.ok(gw.get(f"/assets/{aid}/graph", "viewer_alpha"))
    kinds = {(e["type"], g_node["type"]) for e in g["edges"] for g_node in g["nodes"] if g_node["id"] == e["to"]}
    assert ("GOVERNED_BY", "domain") in kinds and ("GOVERNED_BY", "glossary_term") in kinds and ("GOVERNED_BY", "data_product") in kinds and ("CLASSIFIED_AS", "classification") in kinds
    assert all(e["virtual"] is True and e["from"] == aid for e in g["edges"])
    cls = {e["attrs"]["source"]: e for e in g["edges"] if e["type"] == "CLASSIFIED_AS"}
    assert set(cls) == {"manual", "inherited"} and cls["inherited"]["attrs"]["precision"] == "contains" and cls["inherited"]["attrs"]["from_asset"]["name"] == "email"
    # nothing was written into the lineage graph
    assert "GOVERNED_BY" not in str(gw.ok(gw.intel(f"/assets/{aid}/lineage", direction="both", depth=3)))
    # hidden origin of an inherited label
    gw.ok(gw.classify("beta_src.secret_col", "Credentials", "cur_beta"), 201)
    g2 = gw.ok(gw.get(f"/assets/{gw.aid('alpha_ingest.secret_col')}/graph", "viewer_alpha"))
    inh = [e for e in g2["edges"] if e["type"] == "CLASSIFIED_AS"]
    assert len(inh) == 1 and inh[0]["attrs"]["from_asset"] == {"hidden": True} and "beta_src" not in str(g2)
    assert gw.get(f"/assets/{gw.aid('beta_src')}/graph", "viewer_alpha").status_code == 404


# ============================================================================================================================ hooks
def test_decorate_adds_governance_to_asset_documents_and_lists(fresh):
    gw = fresh
    aid = gw.aid("customers")
    dom, term, prod = gw.domain("Marketing"), gw.term("Customer", status="approved"), gw.product("Customer 360")
    gw.ok(gw.put(f"/assets/{aid}/domain", {"domain_id": dom["id"]}, "cur_org"))
    gw.ok(gw.post(f"/assets/{aid}/terms", {"term_id": term["id"]}, "cur_org"), 201)
    gw.ok(gw.post(f"/products/{prod['id']}/members", {"asset_id": aid}, "cur_org"), 201)
    gw.ok(gw.classify("customers", "Confidential", "cur_alpha"), 201)
    gw.ok(gw.post(f"/assets/{aid}/owners", {"role": "steward", "name": "Sam"}, "cur_alpha"), 201)
    gov = gw.ok(gw.intel(f"/assets/{aid}", "viewer_alpha"))["governance"]
    assert gov["domain"]["name"] == "Marketing" and gov["certification"] == "none"
    assert [c["name"] for c in gov["classifications"]] == ["Confidential"] and [t["name"] for t in gov["glossary_terms"]] == ["Customer"]
    assert [p["name"] for p in gov["data_product"]] == ["Customer 360"] and gov["owners"][0]["name"] == "Sam" and gov["violations"]["open"] == 0
    # lineage nodes carry the facets of every asset in the graph (empty facets, not errors, for assets without governance data)
    nodes = {n["name"]: n for n in gw.ok(gw.intel(f"/assets/{aid}/lineage", "viewer_alpha", direction="down", depth=2))["nodes"]}
    assert nodes["customers"]["governance"]["domain"]["name"] == "Marketing"
    other = nodes["customers_clean"]["governance"]
    assert other["domain"] is None and other["certification"] == "none" and other["owners"] == []
    # the table label reaches the table downstream by lineage and says so
    assert [(c["name"], c["inherited"], c["source"]) for c in other["classifications"]] == [("Confidential", True, "inherited")]
    assert [(c["name"], c["inherited"]) for c in nodes["customers"]["governance"]["classifications"]] == [("Confidential", False)]
    # the generated documentation has a governance section
    md = gw.ok(gw.intel(f"/assets/{aid}/documentation", "viewer_alpha"))
    text = md["markdown"] if isinstance(md, dict) and "markdown" in md else str(md)
    assert "## Governance" in text and "Marketing" in text and "Customer" in text and "Confidential" in text and "Sam" in text


def test_asset_filters_of_the_intelligence_list(fresh):
    gw = fresh
    dom, d2 = gw.domain("Finance"), gw.domain("Ops")
    gw.ok(gw.put(f"/assets/{gw.aid('orders')}/domain", {"domain_id": dom["id"]}, "cur_org"))
    gw.ok(gw.put(f"/assets/{gw.aid('mart_sales')}/domain", {"domain_id": d2["id"]}, "cur_org"))
    ids = lambda **p: sorted(i["name"] for i in gw.ok(gw.intel("/assets", "viewer_org", type="table", limit=200, **p))["items"])      # noqa: E731
    assert ids(domain="Finance") == ["orders"] and ids(domain=dom["id"]) == ["orders"] and ids(domain="Ops") == ["mart_sales"]
    assert ids(domain="Nonexistent") == []
    gw.ok(gw.classify("raw_dump", "Restricted", "cur_alpha"), 201)
    assert ids(classification="Restricted") == ["raw_dump"] and ids(classification="restricted") == ["raw_dump"] and ids(classification="Credentials") == []
    gw.ok(gw.put(f"/assets/{gw.aid('alpha_ingest')}/certification", {"state": "certified"}, "appr_alpha"))
    assert ids(certification="certified") == ["alpha_ingest"] and "alpha_ingest" not in ids(certification="none") and "orders" in ids(certification="none")
    assert gw.intel("/assets", "viewer_org", certification="nope").status_code == 400
    gw.ok(gw.post(f"/assets/{gw.aid('orders')}/owners", {"role": "owner", "user": "g_co"}, "cur_alpha"), 201)
    assert ids(owner="g_co") == ["orders"]
    # filters combine
    assert ids(domain="Finance", classification="Restricted") == []
    assert ids(domain="Finance", owner="g_co") == ["orders"]


def test_owners_hook_feeds_impact_analysis(fresh):
    gw = fresh
    gw.ok(gw.post(f"/assets/{gw.aid('mart_sales')}/owners", {"role": "owner", "user": "g_co2"}, "cur_alpha"), 201)
    imp = gw.ok(gw.intel(f"/assets/{gw.aid('customers')}/impact", "viewer_alpha", depth=4))
    assert "g_co2" in str(imp) and "mart_sales" in str(imp)


# ============================================================================================================================ merge hook
def test_merge_moves_governance_rows_to_the_survivor(fresh):
    gw = fresh
    keep, gone = gw.aid("orders"), gw.aid("raw_dump")
    dom, term, prod, lab = gw.domain(), gw.term(status="approved"), gw.product(), "Internal"
    # governance on the asset that will be merged away
    gw.ok(gw.put(f"/assets/{gone}/domain", {"domain_id": dom["id"]}, "cur_org"))
    gw.ok(gw.post(f"/assets/{gone}/terms", {"term_id": term["id"]}, "cur_org"), 201)
    gw.ok(gw.post(f"/products/{prod['id']}/members", {"asset_id": gone}, "cur_org"), 201)
    gw.ok(gw.post(f"/assets/{gone}/owners", {"role": "steward", "name": "Sam"}, "cur_alpha"), 201)
    gw.ok(gw.post(f"/assets/{gone}/owners", {"role": "owner", "name": "Olive"}, "cur_alpha"), 201)
    gw.ok(gw.classify("raw_dump", lab, "cur_alpha"), 201)
    gw.ok(gw.put(f"/assets/{gone}/certification", {"state": "draft"}, "cur_alpha"))
    chg = gw.ok(gw.post("/changes", {"type": "deprecate", "subject_type": "asset", "subject_id": gone, "payload": {}}, "cur_alpha"), 201)
    pol = gw.ok(gw.post("/policies", {"name": "desc", "rule_type": "required_description", "params": {"types": ["table"], "selector": {"name_pattern": "raw_dump"}}}, "lead_org"), 201)
    gw.ok(gw.post(f"/policies/{pol['id']}/evaluate", {}, "lead_org"))
    assert len(gw.ok(gw.get("/violations", asset_id=gone))["items"]) == 1
    # and a clashing row on the survivor: the same owner and the same label
    gw.ok(gw.post(f"/assets/{keep}/owners", {"role": "owner", "name": "Olive"}, "cur_alpha"), 201)
    gw.ok(gw.classify("orders", lab, "cur_alpha"), 201)
    gw.svc.registry.merge(keep, gone, reason="test merge")
    d = gw.ok(gw.get(f"/assets/{keep}", "viewer_alpha"))
    assert d["domain"]["domain"]["id"] == dom["id"] and [t["id"] for t in d["terms"]] == [term["id"]] and [p["id"] for p in d["products"]] == [prod["id"]]
    owners = sorted((o["role"], o["name"]) for o in d["owners"]["direct"])
    assert owners == [("owner", "Olive"), ("steward", "Sam")]                # the duplicate owner collapsed
    assert [c["classification"]["name"] for c in d["classifications"]["direct"]] == [lab] and d["certification"]["state"] == "draft"
    assert [c["id"] for c in d["open_changes"]] == [chg["id"]]
    # nothing in governance points at the merged-away asset any more, and its violations are gone
    with gw.gov.db.connect() as c:
        for table, col, _extra in ((GS.gov_term_links, "subject_id", None), (GS.gov_asset_domains, "asset_id", None), (GS.gov_product_members, "asset_id", None), (GS.gov_owners, "subject_id", None),
                                  (GS.gov_class_assignments, "asset_id", None), (GS.gov_certifications, "subject_id", None), (GS.gov_changes, "subject_id", None), (GS.gov_violations, "subject_id", None)):
            assert c.execute(select(table).where(table.c[col] == gone)).first() is None, table.name
    # the policy runs against the survivor on the next evaluation
    gw.ok(gw.post(f"/policies/{pol['id']}/evaluate", {}, "lead_org"))
    assert gw.ok(gw.get("/violations", asset_id=keep))["items"] == []       # 'orders' has no description either, but the policy only selects raw_dump by name


def test_merge_with_governance_tables_missing_does_not_fail(fresh):
    gw = fresh
    from olcf.intelligence.governance.hooks import merge_assets

    with gw.gov.db.begin() as c:
        for tb in reversed(GS.GOVERNANCE_TABLES):
            c.exec_driver_sql(f'DROP TABLE IF EXISTS "{tb.name}"')
        assert merge_assets(c, "ast_a", "ast_b") == {}
    gw.svc.registry.merge(gw.aid("orders"), gw.aid("raw_dump"), reason="no governance storage")
