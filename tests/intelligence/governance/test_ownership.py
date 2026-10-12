# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Owners and stewards: direct assignment, per-role inheritance with provenance, the ``owners_of`` hook and the ``owner=`` filter."""

from __future__ import annotations


def owners(gw, path, who="admin"):
    return gw.ok(gw.get(f"{path}/owners", who))


def test_add_replace_remove_direct_owners(gw):
    aid = gw.aid("raw_dump")
    r = gw.ok(gw.post(f"/assets/{aid}/owners", {"role": "owner", "user": "g_ca"}, "cur_alpha"), 201)
    assert r["role"] == "owner" and r["kind"] == "user" and r["user_id"] and r["name"]
    assert gw.post(f"/assets/{aid}/owners", {"role": "owner", "user": "g_ca"}, "cur_alpha").status_code == 409
    ext = gw.ok(gw.post(f"/assets/{aid}/owners", {"role": "steward", "name": "Dana Data", "email": "dana@example.com"}, "cur_alpha"), 201)
    assert ext["kind"] == "external" and ext["email"] == "dana@example.com" and ext["user_id"] is None
    got = owners(gw, f"/assets/{aid}")
    assert {o["role"] for o in got["direct"]} == {"owner", "steward"} and len(got["effective"]) == 2
    removed = gw.ok(gw.delete(f"/assets/{aid}/owners/{ext['id']}", "cur_alpha"))
    assert [o["role"] for o in removed["direct"]] == ["owner"]
    assert gw.delete(f"/assets/{aid}/owners/{ext['id']}", "cur_alpha").status_code == 404
    rep = gw.ok(gw.put(f"/assets/{aid}/owners", {"owners": [{"role": "custodian", "name": "Ops team"}, {"role": "contact", "email": "help@example.com", "name": "Help"}]}, "cur_alpha"))
    assert sorted(o["role"] for o in rep["direct"]) == ["contact", "custodian"]
    assert gw.ok(gw.put(f"/assets/{aid}/owners", {"owners": []}, "cur_alpha"))["direct"] == []


def test_owner_validation(gw):
    aid = gw.aid("raw_dump")
    assert gw.post(f"/assets/{aid}/owners", {"role": "king", "user": "g_ca"}, "cur_alpha").status_code == 422
    assert gw.post(f"/assets/{aid}/owners", {"role": "owner", "user": "nobody-like-this"}, "cur_alpha").status_code == 400
    assert gw.post(f"/assets/{aid}/owners", {"role": "owner"}, "cur_alpha").status_code == 400
    assert gw.post(f"/assets/{aid}/owners", {"role": "owner", "user": "g_ca", "name": "x"}, "cur_alpha").status_code == 400
    assert gw.post(f"/assets/{aid}/owners", {"role": "owner", "name": "x", "bogus": 1}, "cur_alpha").status_code == 422
    assert gw.put(f"/assets/{aid}/owners", {"owners": "nope"}, "cur_alpha").status_code == 422


def test_inheritance_per_role_with_provenance(gw):
    lh, table, col = gw.aid("lakehouse_a"), gw.aid("orders"), gw.aid("orders.amount")
    gw.ok(gw.put(f"/assets/{lh}/owners", {"owners": [{"role": "owner", "user": "g_ca"}, {"role": "steward", "name": "Lake Steward"}]}, "cur_alpha"))
    try:
        eff = owners(gw, f"/assets/{table}")["effective"]
        by_role = {o["role"]: o for o in eff}
        assert by_role["owner"]["inherited_from"]["asset_id"] == lh and by_role["owner"]["inherited_from"]["distance"] == 1
        assert by_role["steward"]["inherited_from"]["name"] == "lakehouse_a"
        assert owners(gw, f"/assets/{table}")["direct"] == []
        assert owners(gw, f"/assets/{col}")["effective"][0]["inherited_from"]["distance"] == 2
        # a steward of its own on the table replaces the inherited steward but not the inherited owner
        gw.ok(gw.post(f"/assets/{table}/owners", {"role": "steward", "name": "Table Steward"}, "cur_alpha"), 201)
        eff = {o["role"]: o for o in owners(gw, f"/assets/{table}")["effective"]}
        assert eff["steward"]["name"] == "Table Steward" and eff["steward"]["inherited_from"] is None
        assert eff["owner"]["inherited_from"]["asset_id"] == lh
        # the column inherits the nearest holder per role: steward from the table, owner from the lakehouse
        eff_col = {o["role"]: o for o in owners(gw, f"/assets/{col}")["effective"]}
        assert eff_col["steward"]["inherited_from"]["asset_id"] == table and eff_col["owner"]["inherited_from"]["asset_id"] == lh
    finally:
        gw.put(f"/assets/{lh}/owners", {"owners": []}, "cur_alpha")
        gw.put(f"/assets/{table}/owners", {"owners": []}, "cur_alpha")


def test_decorate_and_owners_hook(gw):
    lh, table = gw.aid("lakehouse_a"), gw.aid("mart_sales")
    gw.ok(gw.post(f"/assets/{lh}/owners", {"role": "owner", "user": "g_co"}, "cur_org"), 201)
    try:
        gov = gw.ok(gw.intel(f"/assets/{table}"))["governance"]
        assert [o["name"] for o in gov["owners"]] and gov["owners"][0]["inherited_from"]["asset_id"] == lh
        # the hook used by impact analysis: consumers carry their owners
        imp = gw.ok(gw.intel(f"/assets/{gw.aid('customers_clean')}/impact", depth=3))
        text = str(imp)
        assert "mart_sales" in text and "g_co" in text
    finally:
        gw.delete(f"/assets/{lh}/owners/{gw.ok(gw.get(f'/assets/{lh}/owners'))['direct'][0]['id']}", "cur_org")


def test_owner_filter_includes_inherited_and_me(gw):
    lh = gw.aid("lakehouse_a")
    gw.ok(gw.post(f"/assets/{lh}/owners", {"role": "owner", "user": "g_co2"}, "cur_org"), 201)
    gw.ok(gw.post(f"/assets/{gw.aid('beta_src')}/owners", {"role": "owner", "user": "g_co2"}, "cur_org"), 201)
    try:
        r = gw.ok(gw.intel("/assets", owner="g_co2", type="table", limit=500))
        got = {a["name"] for a in r["items"]}
        assert {"customers", "orders", "mart_sales", "beta_src"} <= got
        alpha_only = {a["name"] for a in gw.ok(gw.intel("/assets", "viewer_alpha", owner="g_co2", type="table", limit=500))["items"]}
        assert "beta_src" not in alpha_only and "customers" in alpha_only
        mine = {a["name"] for a in gw.ok(gw.intel("/assets", "cur_org2", owner="me", type="table", limit=500))["items"]}
        assert mine == got
        assert gw.ok(gw.intel("/assets", owner="somebody-else"))["items"] == []
    finally:
        for aid in (lh, gw.aid("beta_src")):
            for o in gw.ok(gw.get(f"/assets/{aid}/owners"))["direct"]:
                gw.delete(f"/assets/{aid}/owners/{o['id']}", "cur_org")


def test_filter_role_aware_inheritance(gw):
    lh, table = gw.aid("lakehouse_a"), gw.aid("customers")
    gw.ok(gw.post(f"/assets/{lh}/owners", {"role": "owner", "name": "Zed Owner"}, "cur_org"), 201)
    gw.ok(gw.post(f"/assets/{table}/owners", {"role": "owner", "name": "Ann Owner"}, "cur_org"), 201)
    try:
        zed = {a["name"] for a in gw.ok(gw.intel("/assets", owner="Zed Owner", type="table", limit=500))["items"]}
        assert "customers" not in zed and "orders" in zed          # customers has an owner of its own, so it no longer inherits the lakehouse's
        ann = {a["name"] for a in gw.ok(gw.intel("/assets", owner="Ann Owner", limit=500))["items"]}
        assert {"customers", "email", "customer_id"} <= ann and "orders" not in ann
    finally:
        for aid in (lh, table):
            for o in gw.ok(gw.get(f"/assets/{aid}/owners"))["direct"]:
                gw.delete(f"/assets/{aid}/owners/{o['id']}", "cur_org")


def test_owners_of_domains_products_and_terms(gw):
    d, p, t = gw.domain(), gw.product(), gw.term()
    for prefix, sid in (("domains", d["id"]), ("products", p["id"]), ("glossary/terms", t["id"])):
        base = f"/{prefix}/{sid}"
        o = gw.ok(gw.post(f"{base}/owners", {"role": "owner", "user": "g_co"}, "cur_org"), 201)
        assert gw.ok(gw.get(f"{base}/owners", "viewer_alpha"))["effective"][0]["name"]
        assert gw.post(f"{base}/owners", {"role": "steward", "name": "S"}, "cur_alpha").status_code == 403       # org-level subject: org-wide curate
        assert gw.post(f"{base}/owners", {"role": "owner", "user": "g_co"}, "cur_org").status_code == 409
        assert gw.ok(gw.delete(f"{base}/owners/{o['id']}", "cur_org"))["direct"] == []
    assert gw.get("/domains/dom_missing/owners").status_code == 404
    # owners given at creation
    t2 = gw.term(gw.uniq("withowner"), owners=[{"role": "steward", "name": "Sam"}])
    assert [o["name"] for o in gw.ok(gw.get(f"/glossary/terms/{t2['id']}"))["owners"]] == ["Sam"]


def test_owner_permissions_and_visibility(gw):
    aid = gw.aid("raw_dump")
    assert gw.get(f"/assets/{aid}/owners", "viewer_alpha").status_code == 200
    assert gw.get(f"/assets/{aid}/owners", "viewer_beta").status_code == 404           # hidden asset: not found, not forbidden
    assert gw.get(f"/assets/{aid}/owners", "nobody").status_code == 403
    assert gw.post(f"/assets/{aid}/owners", {"role": "owner", "name": "x"}, "viewer_alpha").status_code == 403
    assert gw.post(f"/assets/{aid}/owners", {"role": "owner", "name": "x"}, "cur_beta").status_code == 404
    assert gw.post(f"/assets/{aid}/owners", {"role": "owner", "name": "x"}, "agent").status_code == 403   # token scope 'read' cannot write
    assert gw.post("/assets/ast_missing/owners", {"role": "owner", "name": "x"}, "cur_alpha").status_code == 404
