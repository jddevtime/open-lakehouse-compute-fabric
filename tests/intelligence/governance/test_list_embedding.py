# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``GET /intelligence/assets`` list cards (compact ``governance`` object), the governance-filter id cap with ``truncated_filter``, review-queue permission order, request-model validation."""

from __future__ import annotations

from olcf.intelligence import service as core_service


def items_by_name(resp):
    return {i["name"]: i for i in resp["items"]}


def pending(gw, **params):
    return gw.ok(gw.get("/review-queue", **params))["items"]


# ============================================================================================================================ list cards
def test_list_items_carry_a_compact_governance_object(fresh):
    gw = fresh
    orders, customers = gw.aid("orders"), gw.aid("customers")
    d = gw.domain()
    gw.ok(gw.put(f"/assets/{orders}/domain", {"domain_id": d["id"]}, "cur_alpha"))
    gw.ok(gw.post(f"/assets/{orders}/owners", {"role": "owner", "user": "g_ca"}, "cur_alpha"), 201)
    gw.ok(gw.put(f"/assets/{orders}/certification", {"state": "certified", "comment": "ok"}, "appr_alpha"))
    gw.ok(gw.classify("orders", "Confidential"), 201)
    got = items_by_name(gw.ok(gw.intel("/assets", "viewer_alpha", type="table", limit=100)))
    card = got["orders"]["governance"]
    assert set(card) == {"classifications", "certification", "domain", "quality_score", "owners"}
    assert card["certification"] == "certified" and card["domain"] == {"id": d["id"], "name": d["name"]} and card["owners"] == 1
    assert [c["name"] for c in card["classifications"]] == ["Confidential"] and card["classifications"][0]["inherited"] is False
    assert isinstance(card["quality_score"], (int, float)) and 0 <= card["quality_score"] <= 100
    # an untouched asset still has a card, with neutral values
    plain = got["customers"]["governance"]
    assert plain["certification"] == "none" and plain["domain"] is None and plain["owners"] == 0
    # the score on the card is the one the quality endpoint reports
    assert card["quality_score"] == gw.ok(gw.get(f"/assets/{orders}/quality", "viewer_alpha"))["score"]
    assert customers in {i["id"] for i in gw.ok(gw.intel("/assets", "viewer_alpha", type="table"))["items"]}


def test_list_card_only_for_visible_assets_and_core_shape_unchanged(gw):
    resp = gw.ok(gw.intel("/assets", "viewer_alpha", limit=500))
    names = {i["name"] for i in resp["items"]}
    assert "beta_src" not in names and all("governance" in i for i in resp["items"] if i["status"] == "active")
    assert "truncated_filter" not in resp and set(resp) == {"items", "next_cursor"}
    # the full asset view still carries the complete object
    full = gw.ok(gw.intel(f"/assets/{gw.aid('orders')}", "viewer_alpha"))["governance"]
    assert "glossary_terms" in full and "violations" in full


def test_list_is_one_bulk_call_per_page(fresh):
    gw = fresh
    calls: list[int] = []
    real = gw.gov.decorate_compact

    def counting(principal, ids):
        calls.append(len(ids))
        return real(principal, ids)

    gw.gov.decorate_compact = counting
    try:
        gw.ok(gw.intel("/assets", "viewer_alpha", limit=500))
    finally:
        del gw.gov.decorate_compact
    assert len(calls) == 1 and calls[0] > 5


def test_list_survives_a_failing_governance_decoration(fresh):
    gw = fresh

    def boom(principal, ids):
        raise RuntimeError("down")

    gw.gov.decorate_compact = boom
    try:
        resp = gw.ok(gw.intel("/assets", "viewer_alpha", limit=50))
    finally:
        del gw.gov.decorate_compact
    assert resp["items"] and all("governance" not in i for i in resp["items"])


# ============================================================================================================================ filter cap
def test_governance_filter_is_not_cut_at_five_thousand_ids(fresh, monkeypatch):
    gw = fresh
    orders, customers = gw.aid("orders"), gw.aid("customers")
    filler = [f"ast_00000{n:06d}" for n in range(6000)]            # sort before every real id; the old code kept only the first 5000 and lost the real ones
    monkeypatch.setattr(gw.gov, "asset_filter", lambda principal, **kw: {*filler, orders, customers})
    resp = gw.ok(gw.intel("/assets", "viewer_alpha", domain="anything", limit=100))
    assert {i["name"] for i in resp["items"]} == {"orders", "customers"} and "truncated_filter" not in resp


def test_chunked_filter_returns_every_match(fresh, monkeypatch):
    gw = fresh
    d = gw.domain()
    names = ["orders", "customers", "customers_clean", "mart_sales", "raw_dump", "alpha_ingest"]
    for n in names:
        gw.ok(gw.put(f"/assets/{gw.aid(n)}/domain", {"domain_id": d["id"]}, "cur_alpha"))
    monkeypatch.setattr(core_service, "_IN_CHUNK", 2)
    resp = gw.ok(gw.intel("/assets", "viewer_alpha", domain=d["id"], type="table", limit=100))
    assert {i["name"] for i in resp["items"]} == set(names) and "truncated_filter" not in resp


def test_truncated_filter_is_reported(fresh, monkeypatch):
    gw = fresh
    d = gw.domain()
    names = ["orders", "customers", "customers_clean", "mart_sales", "raw_dump", "alpha_ingest"]
    for n in names:
        gw.ok(gw.put(f"/assets/{gw.aid(n)}/domain", {"domain_id": d["id"]}, "cur_alpha"))
    monkeypatch.setattr(core_service, "_IN_CHUNK", 2)
    monkeypatch.setattr(core_service, "FILTER_ID_CAP", 4)
    resp = gw.ok(gw.intel("/assets", "viewer_alpha", domain=d["id"], type="table", limit=100))
    assert resp["truncated_filter"] is True and len(resp["items"]) == 4 and {i["name"] for i in resp["items"]} <= set(names)
    # a filter-free list never reports it, and neither does one under the cap
    assert "truncated_filter" not in gw.ok(gw.intel("/assets", "viewer_alpha", type="table", limit=100))
    monkeypatch.setattr(core_service, "FILTER_ID_CAP", 100)
    assert "truncated_filter" not in gw.ok(gw.intel("/assets", "viewer_alpha", domain=d["id"], type="table", limit=100))


def test_filter_of_other_workspace_ids_does_not_leak(fresh, monkeypatch):
    gw = fresh
    monkeypatch.setattr(gw.gov, "asset_filter", lambda principal, **kw: {gw.aid("beta_src"), gw.aid("orders")})
    resp = gw.ok(gw.intel("/assets", "viewer_alpha", domain="x", limit=100))
    assert {i["name"] for i in resp["items"]} == {"orders"}


# ============================================================================================================================ permission order
def test_review_queue_checks_permission_before_state(fresh):
    gw = fresh
    gw.tick()
    ids = [i["id"] for i in pending(gw, kind="classification")]
    assert len(ids) == 2
    gw.ok(gw.post(f"/review-queue/{ids[0]}/approve", {"comment": "fine"}, "cur_alpha"))
    gw.ok(gw.post(f"/review-queue/{ids[1]}/reject", {"comment": "no"}, "cur_alpha"))
    for item in ids:
        for action in ("approve", "reject"):
            # a viewer learns nothing about the item's state, a curator is told it is decided
            assert gw.post(f"/review-queue/{item}/{action}", {}, "viewer_alpha").status_code == 403
            assert gw.post(f"/review-queue/{item}/{action}", {}, "cur_alpha").status_code == 409
            assert gw.post(f"/review-queue/{item}/{action}", {}, "cur_beta").status_code == 404
    r = gw.ok(gw.post("/review-queue/bulk", {"action": "approve", "ids": ids}, "viewer_alpha"))
    assert r["ok"] == 0 and {x["error"]["code"] for x in r["results"]} == {"forbidden"}
    r = gw.ok(gw.post("/review-queue/bulk", {"action": "reject", "ids": ids}, "cur_alpha"))
    assert {x["error"]["code"] for x in r["results"]} == {"conflict"}


# ============================================================================================================================ request models
def test_unknown_fields_and_types_are_422_with_a_field_path(gw):
    aid = gw.aid("orders")
    r = gw.post(f"/assets/{aid}/owners", {"role": "owner", "user": "g_ca", "surprise": 1}, "cur_alpha")
    assert r.status_code == 422 and gw.code(r) == "validation_failed"
    r = gw.put(f"/assets/{aid}/domain", {"domain_id": 5}, "cur_alpha")
    assert r.status_code == 422
    r = gw.put(f"/assets/{aid}/domain", {}, "cur_alpha")
    assert r.status_code == 422                                           # domain_id is required (null clears)
    r = gw.post("/classifications", {"name": "ok", "color": "red"}, "cur_org")
    assert r.status_code == 422
    r = gw.post("/classifications", {"name": "x" * 300}, "cur_org")
    assert r.status_code == 422
    r = gw.post("/glossary/terms", {"name": "t", "definition": "d" * 30000}, "admin")
    assert r.status_code == 422
    # a non-object body is refused too
    assert gw.post("/glossary/terms", [1, 2]).status_code == 422
