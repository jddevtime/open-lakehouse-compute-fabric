# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Domains (hierarchy, assignment, inheritance) and data products (SLA, contract, members, consumers, rolled-up quality)."""

from __future__ import annotations


def names(r):
    return {a["name"] for a in r["items"]}


def test_domain_crud_hierarchy_and_etag(gw):
    root = gw.domain(gw.uniq("Finance"), description="money")
    child = gw.domain(gw.uniq("Treasury"), parent_id=root["id"])
    assert root["id"].startswith("dom_") and child["parent_id"] == root["id"]
    got = gw.ok(gw.get(f"/domains/{child['id']}"))
    assert [a["id"] for a in got["ancestors"]] == [root["id"]]
    assert [c["id"] for c in gw.ok(gw.get(f"/domains/{root['id']}"))["children"]] == [child["id"]]
    assert gw.post("/domains", {"name": root["name"].lower()}).status_code == 409
    assert gw.patch(f"/domains/{root['id']}", {"parent_id": child["id"]}).status_code == 400          # a cycle
    r = gw.patch(f"/domains/{root['id']}", {"description": "updated"}, headers={"If-Match": f'"{root["etag"]}"'})
    assert gw.ok(r)["description"] == "updated"
    assert gw.patch(f"/domains/{root['id']}", {"description": "x"}, headers={"If-Match": f'"{root["etag"]}"'}).status_code == 412
    top = gw.ok(gw.get("/domains", parent_id="root", limit=500))["items"]
    assert root["id"] in {d["id"] for d in top} and child["id"] not in {d["id"] for d in top}
    listed = next(d for d in gw.ok(gw.get("/domains", q=root["name"]))["items"] if d["id"] == root["id"])
    assert listed["child_count"] == 1 and listed["asset_count"] == 0 and listed["product_count"] == 0


def test_domain_delete_is_blocked_while_in_use(gw):
    d = gw.domain()
    sub = gw.domain(parent_id=d["id"])
    assert gw.delete(f"/domains/{d['id']}").status_code == 409                          # has a sub-domain
    gw.ok(gw.delete(f"/domains/{sub['id']}"))
    gw.ok(gw.put(f"/assets/{gw.aid('orders')}/domain", {"domain_id": d["id"]}, "cur_alpha"))
    assert gw.delete(f"/domains/{d['id']}").status_code == 409                          # has an assigned asset
    gw.ok(gw.put(f"/assets/{gw.aid('orders')}/domain", {"domain_id": None}, "cur_alpha"))
    p = gw.product(domain_id=d["id"])
    assert gw.delete(f"/domains/{d['id']}").status_code == 409                          # has a product
    gw.ok(gw.delete(f"/products/{p['id']}"))
    t = gw.term(gw.uniq("t"), domain_id=d["id"])
    assert gw.delete(f"/domains/{d['id']}").status_code == 409                          # has a linked term
    gw.ok(gw.delete(f"/glossary/terms/{t['id']}", params={"hard": "true"}, who="lead_org"))
    gw.ok(gw.delete(f"/domains/{d['id']}"))
    assert gw.get(f"/domains/{d['id']}").status_code == 404


def test_assign_domain_direct_inherited_and_override(gw):
    parent = gw.domain(gw.uniq("Sales"))
    other = gw.domain(gw.uniq("Marketing"))
    lh, table, col = gw.aid("lakehouse_a"), gw.aid("customers"), gw.aid("customers.email")
    gw.ok(gw.put(f"/assets/{lh}/domain", {"domain_id": parent["id"]}, "cur_alpha"))
    info = gw.ok(gw.get(f"/assets/{table}"))
    assert info["domain"]["domain"]["id"] == parent["id"] and info["domain"]["source"] == "inherited"
    assert info["domain"]["inherited_from"]["asset_id"] == lh and info["domain"]["inherited_from"]["distance"] >= 1
    assert gw.ok(gw.intel(f"/assets/{col}"))["governance"]["domain"]["id"] == parent["id"]
    # a direct assignment on the table wins for the table and its columns, not for its siblings
    gw.ok(gw.put(f"/assets/{table}/domain", {"domain_id": other["id"]}, "cur_alpha"))
    assert gw.ok(gw.get(f"/assets/{table}"))["domain"]["source"] == "direct"
    assert gw.ok(gw.get(f"/assets/{col}"))["domain"]["domain"]["id"] == other["id"]
    assert gw.ok(gw.get(f"/assets/{gw.aid('orders')}"))["domain"]["domain"]["id"] == parent["id"]
    # filter: the core's /intelligence/assets?domain= honours direct and inherited membership
    in_parent = names(gw.ok(gw.intel("/assets", domain=parent["id"], type="table", limit=500)))
    assert "orders" in in_parent and "customers" not in in_parent
    in_other = names(gw.ok(gw.intel("/assets", domain=other["name"], type="table", limit=500)))
    assert in_other == {"customers"}
    # clearing the assignment
    gw.ok(gw.put(f"/assets/{table}/domain", {"domain_id": None}, "cur_alpha"))
    assert gw.ok(gw.get(f"/assets/{table}"))["domain"]["domain"]["id"] == parent["id"]
    gw.ok(gw.put(f"/assets/{lh}/domain", {"domain_id": None}, "cur_alpha"))
    assert gw.ok(gw.get(f"/assets/{table}"))["domain"] is None
    assert gw.put(f"/assets/{lh}/domain", {"domain_id": "dom_missing"}, "cur_alpha").status_code in (400, 404)


def test_domain_assets_listing_includes_products_and_subdomains(gw):
    top = gw.domain(gw.uniq("Ops"))
    sub = gw.domain(gw.uniq("OpsSub"), parent_id=top["id"])
    gw.ok(gw.put(f"/assets/{gw.aid('orders')}/domain", {"domain_id": top['id']}, "cur_alpha"))
    gw.ok(gw.put(f"/assets/{gw.aid('mart_sales')}/domain", {"domain_id": sub['id']}, "cur_alpha"))
    gw.product(domain_id=top["id"], member_asset_ids=[gw.aid("customers")])
    r = gw.ok(gw.get(f"/domains/{top['id']}/assets", limit=500))
    by = {a["name"]: a["domain_source"] for a in r["items"]}
    assert by["orders"] == "direct" and by["mart_sales"] == "direct" and by["customers"] == "product"
    assert by["orders"] and any(a["name"] == "order_id" and a["domain_source"] == "inherited" for a in r["items"])
    only = gw.ok(gw.get(f"/domains/{top['id']}/assets", include_descendants="false", type="table", limit=500))
    assert names(only) == {"orders", "customers"}
    page1 = gw.ok(gw.get(f"/domains/{top['id']}/assets", limit=2))
    assert len(page1["items"]) == 2 and page1["next_cursor"]
    page2 = gw.ok(gw.get(f"/domains/{top['id']}/assets", limit=500, cursor=page1["next_cursor"]))
    assert {a["id"] for a in page1["items"]}.isdisjoint({a["id"] for a in page2["items"]})
    # a viewer of beta sees no alpha asset in the domain listing
    assert gw.ok(gw.get(f"/domains/{top['id']}/assets", "viewer_beta", limit=500))["items"] == []
    assert names(gw.ok(gw.get(f"/domains/{top['id']}/assets", "viewer_alpha", type="table", limit=500))) == {"orders", "customers", "mart_sales"}


def test_domain_permissions(gw):
    d = gw.domain()
    aid = gw.aid("orders")
    assert gw.post("/domains", {"name": gw.uniq("x")}, "cur_alpha").status_code == 403           # org-level write needs org-wide curate
    assert gw.post("/domains", {"name": gw.uniq("x")}, "viewer_org").status_code == 403
    assert gw.get(f"/domains/{d['id']}", "viewer_beta").status_code == 200
    assert gw.get(f"/domains/{d['id']}", "nobody").status_code == 403
    assert gw.put(f"/assets/{aid}/domain", {"domain_id": d["id"]}, "viewer_alpha").status_code == 403
    assert gw.put(f"/assets/{aid}/domain", {"domain_id": d["id"]}, "cur_beta").status_code == 404   # the asset does not exist for a beta curator
    assert gw.put(f"/assets/{aid}/domain", {"domain_id": d["id"]}, "cur_alpha").status_code == 200


def test_product_crud_sla_contract_and_status(gw):
    d = gw.domain()
    body = {"name": gw.uniq("Customer 360"), "description": "A product", "domain_id": d["id"], "sla": {"freshness_hours": 12, "availability": "99.5%"},
            "contract": {"columns": [{"name": "customer_id", "type": "int", "nullable": False, "classification": "Internal"}, {"name": "email"}],
                         "expectations": [{"name": "unique ids", "rule": "customer_id is unique"}]},
            "member_asset_ids": [gw.aid("customers"), gw.aid("customers_clean")]}
    r = gw.post("/products", body)
    p = gw.ok(r, 201)
    assert r.headers["ETag"] and p["id"].startswith("dpr_") and p["status"] == "draft" and p["member_count"] == 2 and p["domain"]["id"] == d["id"] and p["certification"] == "none"
    got = gw.ok(gw.get(f"/products/{p['id']}"))
    assert {m["name"] for m in got["members"]} == {"customers", "customers_clean"} and got["hidden_members"] == 0
    assert isinstance(got["quality"]["score"], (int, float)) and 0 <= got["quality"]["score"] <= 100
    assert got["sla"]["freshness_hours"] == 12 and got["contract"]["columns"][0]["name"] == "customer_id"
    bad = [{"sla": {"freshness_hours": -1}}, {"sla": {"nope": 1}}, {"contract": {"columns": [{"type": "int"}]}}, {"contract": {"columns": "x"}}, {"status": "weird"},
           {"member_asset_ids": [gw.aid("customers.email")]}, {"member_asset_ids": ["ast_missing"]}, {"domain_id": "dom_missing"}]
    for extra in bad:
        assert gw.post("/products", {"name": gw.uniq("bad"), **extra}).status_code in (400, 404, 422), extra
    assert gw.post("/products", {"name": p["name"]}).status_code == 409
    upd = gw.patch(f"/products/{p['id']}", {"description": "new"}, headers={"If-Match": f'"{p["etag"]}"'})
    assert gw.ok(upd)["description"] == "new"
    assert gw.patch(f"/products/{p['id']}", {"description": "n2"}, headers={"If-Match": f'"{p["etag"]}"'}).status_code == 412


def test_product_publish_needs_approve_and_delete_rules(gw):
    p = gw.product()
    assert gw.patch(f"/products/{p['id']}", {"status": "published"}, "cur_org").status_code == 403
    assert gw.post("/products", {"name": gw.uniq("pp"), "status": "published"}, "cur_org").status_code == 403
    assert gw.ok(gw.patch(f"/products/{p['id']}", {"status": "published"}, "lead_org"))["status"] == "published"
    assert gw.delete(f"/products/{p['id']}").status_code == 409                          # published: deprecate first
    gw.ok(gw.patch(f"/products/{p['id']}", {"status": "deprecated"}))
    gw.ok(gw.delete(f"/products/{p['id']}"))
    assert gw.get(f"/products/{p['id']}").status_code == 404


def test_product_members_set_add_remove(gw):
    p = gw.product()
    r = gw.ok(gw.put(f"/products/{p['id']}/members", {"asset_ids": [gw.aid("orders"), gw.aid("customers")]}, "cur_org"))
    assert r["member_count"] == 2
    assert gw.ok(gw.post(f"/products/{p['id']}/members", {"asset_id": gw.aid("mart_sales")}, "cur_org"), 201)["member_count"] == 3
    assert gw.post(f"/products/{p['id']}/members", {"asset_id": gw.aid("mart_sales")}, "cur_org").status_code == 409
    assert gw.ok(gw.delete(f"/products/{p['id']}/members/{gw.aid('orders')}", "cur_org"))["member_count"] == 2
    assert gw.delete(f"/products/{p['id']}/members/{gw.aid('orders')}", "cur_org").status_code == 404
    assert {m["name"] for m in gw.ok(gw.get(f"/products/{p['id']}"))["members"]} == {"customers", "mart_sales"}
    # members must be visible to the caller who adds them, and of a product-eligible type
    assert gw.post(f"/products/{p['id']}/members", {"asset_id": gw.aid("beta_src")}, "cur_alpha").status_code in (403, 404)
    assert gw.post(f"/products/{p['id']}/members", {"asset_id": gw.aid("etl")}, "cur_org").status_code == 400
    assert gw.ok(gw.put(f"/products/{p['id']}/members", {"asset_ids": []}, "cur_org"))["member_count"] == 0


def test_product_hidden_members_and_membership_in_asset_view(gw):
    p = gw.product(member_asset_ids=[gw.aid("customers"), gw.aid("beta_src")])
    full = gw.ok(gw.get(f"/products/{p['id']}"))
    assert {m["name"] for m in full["members"]} == {"customers", "beta_src"} and full["hidden_members"] == 0
    seen = gw.ok(gw.get(f"/products/{p['id']}", "viewer_alpha"))
    assert {m["name"] for m in seen["members"]} == {"customers"} and seen["hidden_members"] == 1
    assert p["id"] in {x["id"] for x in gw.ok(gw.get(f"/assets/{gw.aid('customers')}"))["products"]}
    assert p["id"] in {x["id"] for x in gw.ok(gw.intel(f"/assets/{gw.aid('customers')}"))["governance"]["data_product"]}
    assert gw.get(f"/products/{p['id']}", "nobody").status_code == 403


def test_product_consumers_come_from_lineage(gw):
    p = gw.product(member_asset_ids=[gw.aid("customers_clean")])
    r = gw.ok(gw.get(f"/products/{p['id']}/consumers", depth=3))
    got = {i["asset"]["name"]: i for i in r["items"]}
    assert "mart_sales" in got and got["mart_sales"]["via_member"]["name"] == "customers_clean" and got["mart_sales"]["confidence"] in ("verified", "derived", "inferred", "unknown")
    assert "customers_clean" not in got and "customers" not in got                         # members and upstream assets are not consumers
    shallow = gw.ok(gw.get(f"/products/{p['id']}/consumers", depth=1))
    assert "mart_sales" in {i["asset"]["name"] for i in shallow["items"]}
    strong = gw.ok(gw.get(f"/products/{p['id']}/consumers", depth=6, min_confidence="derived"))
    assert "sales_report" not in {i["asset"]["name"] for i in strong["items"]}               # that hop is only inferred
    assert "sales_report" in {i["asset"]["name"] for i in gw.ok(gw.get(f"/products/{p['id']}/consumers", depth=6))["items"]}
    # a caller who sees only beta sees no consumers of an alpha product
    assert gw.ok(gw.get(f"/products/{p['id']}/consumers", "viewer_beta"))["items"] == []
    assert gw.get(f"/products/{p['id']}/consumers", depth=9).status_code == 422


def test_filter_by_product_domain_membership(gw):
    d = gw.domain(gw.uniq("Core"))
    gw.product(domain_id=d["id"], member_asset_ids=[gw.aid("raw_dump")])
    r = gw.ok(gw.intel("/assets", domain=d["id"], limit=500))
    assert names(r) == {"raw_dump"}
    assert gw.intel("/assets", domain="no-such-domain").json()["items"] == []
    info = gw.ok(gw.intel(f"/assets/{gw.aid('raw_dump')}"))["governance"]
    assert info["domain"]["id"] == d["id"] and info["domain"]["source"] == "product"


def test_product_listing_filters(gw):
    d = gw.domain()
    a = gw.product(gw.uniq("alpha-prod"), domain_id=d["id"])
    b = gw.product(gw.uniq("beta-prod"))
    assert {p["id"] for p in gw.ok(gw.get("/products", domain_id=d["id"]))["items"]} == {a["id"]}
    assert {p["id"] for p in gw.ok(gw.get("/products", q="beta-prod"))["items"]} >= {b["id"]}
    assert gw.get("/products", status="nope").status_code == 422
    assert all(p["status"] == "draft" for p in gw.ok(gw.get("/products", status="draft", limit=500))["items"])
