# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Quality score: explainable arithmetic, configurable weights, column and member roll-ups, and what is (not) scored."""

from __future__ import annotations

import pytest


def q(gw, name, who="admin"):
    return gw.ok(gw.get(f"/assets/{gw.aid(name)}/quality", who))


def comp(res, name):
    return next(c for c in res["components"] if c["name"] == name)


def test_points_add_up_to_the_score_and_each_loss_is_explained(gw):
    res = q(gw, "customers_clean", "viewer_alpha")
    assert res["type"] == "table" and 0 <= res["score"] <= 100 and 0 <= res["own_score"] <= 100
    applicable = [c for c in res["components"] if c["applicable"]]
    assert abs(sum(c["points"] for c in applicable) - res["own_score"]) <= 0.02
    assert abs(sum(c["max_points"] for c in applicable) - 100) <= 0.05
    for c in applicable:
        assert c["points"] <= c["max_points"] and abs(c["points"] - c["max_points"] * c["fraction"]) <= 0.01
        if c["points"] < c["max_points"]:
            assert c["reasons"] and abs(sum(r["points_lost"] for r in c["reasons"]) - (c["max_points"] - c["points"])) <= 0.02
        else:
            assert c["reasons"] == []
    for c in res["components"]:
        if not c["applicable"]:
            assert c["points"] == 0 and c["max_points"] == 0 and c["fraction"] is None
    assert set(res["weights"]) == {"documentation", "ownership", "classification", "freshness", "certification", "lineage", "violations", "schema_stability"}
    assert res["computed_at"]


def test_component_rules(gw):
    # documentation: none 0, short 0.5, long 1
    assert comp(q(gw, "orders"), "documentation")["fraction"] == 0.0
    short = comp(q(gw, "mart_sales"), "documentation")
    assert short["fraction"] == 0.5 and "only 1 characters" in short["reasons"][0]["reason"]
    assert comp(q(gw, "customers"), "documentation")["fraction"] == 1.0
    # freshness: the 2020 table earns nothing, recent ones everything; no known write time means "not applicable"
    assert comp(q(gw, "orders"), "freshness")["fraction"] == 0.0 and "hours ago" in comp(q(gw, "orders"), "freshness")["reasons"][0]["reason"]
    assert comp(q(gw, "customers"), "freshness")["fraction"] == 1.0
    assert comp(q(gw, "raw_dump"), "freshness")["applicable"] is False
    # lineage: a table with no recorded data flow gets no credit
    assert comp(q(gw, "raw_dump"), "lineage")["fraction"] == 0.0 and comp(q(gw, "customers"), "lineage")["fraction"] == 1.0


def test_ownership_certification_and_violation_components(fresh):
    gw = fresh
    base = q(gw, "orders")
    assert comp(base, "ownership")["fraction"] == 0.0 and comp(base, "certification")["fraction"] == 0.0 and comp(base, "violations")["fraction"] == 1.0
    gw.ok(gw.post(f"/assets/{gw.aid('orders')}/owners", {"role": "steward", "name": "Steve"}, "cur_alpha"), 201)
    assert comp(q(gw, "orders"), "ownership")["fraction"] == 0.5
    gw.ok(gw.post(f"/assets/{gw.aid('orders')}/owners", {"role": "owner", "name": "Olive"}, "cur_alpha"), 201)
    owned = q(gw, "orders")
    assert comp(owned, "ownership")["fraction"] == 1.0 and owned["own_score"] > base["own_score"]
    gw.ok(gw.put(f"/assets/{gw.aid('orders')}/certification", {"state": "draft"}, "cur_alpha"))
    assert comp(q(gw, "orders"), "certification")["fraction"] == 0.5
    gw.ok(gw.put(f"/assets/{gw.aid('orders')}/certification", {"state": "certified"}, "appr_alpha"))
    assert comp(q(gw, "orders"), "certification")["fraction"] == 1.0
    # an open violation costs points; weights per severity (high 0.6)
    pol = gw.ok(gw.post("/policies", {"name": "need desc", "rule_type": "required_description", "severity": "high", "params": {"types": ["table"], "selector": {"name_pattern": "orders"}}}, "lead_org"), 201)
    gw.ok(gw.post(f"/policies/{pol['id']}/evaluate", {}, "lead_org"))
    v = comp(q(gw, "orders"), "violations")
    assert abs(v["fraction"] - 0.4) < 1e-6 and "1 open high violation" in v["reasons"][0]["reason"]
    # a quality_min policy never lowers the score it measures
    qm = gw.ok(gw.post("/policies", {"name": "q min", "rule_type": "quality_min", "severity": "critical", "params": {"min_score": 100, "types": ["table"], "selector": {"name_pattern": "orders"}}}, "lead_org"), 201)
    before = q(gw, "orders")["score"]
    gw.ok(gw.post(f"/policies/{qm['id']}/evaluate", {}, "lead_org"))
    assert any(i["policy"]["rule_type"] == "quality_min" for i in gw.ok(gw.get("/violations", asset_id=gw.aid("orders")))["items"])
    assert q(gw, "orders")["score"] == before


def test_classification_component_counts_reviewed_suggestions(fresh):
    gw = fresh
    assert comp(q(gw, "customers"), "classification")["applicable"] is False         # no activity: no opinion
    gw.tick()
    pending = comp(q(gw, "customers"), "classification")
    assert pending["applicable"] and pending["fraction"] == 0.0 and "waiting for review" in pending["reasons"][0]["reason"]
    for item in gw.ok(gw.get("/review-queue"))["items"]:
        gw.ok(gw.post(f"/review-queue/{item['id']}/reject" if item["asset"]["name"] == "full_name" else f"/review-queue/{item['id']}/approve", {}, "cur_alpha"))
    done = comp(q(gw, "customers"), "classification")
    assert done["fraction"] == 1.0 and done["reasons"] == []


def test_weights_are_settings(fresh):
    gw = fresh
    before = q(gw, "orders")
    assert before["weights"]["ownership"] == 20.0
    gw.setting("governance.quality_weight.ownership", 0)
    after = q(gw, "orders")
    own = comp(after, "ownership")
    assert own["applicable"] is False and own["points"] == 0 and own["max_points"] == 0 and after["weights"]["ownership"] == 0
    assert abs(sum(c["max_points"] for c in after["components"]) - 100) <= 0.05          # the remaining weights are renormalised
    assert after["own_score"] != before["own_score"]
    # doubling documentation raises its share
    gw.setting("governance.quality_weight.documentation", 40)
    assert comp(q(gw, "mart_sales"), "documentation")["max_points"] > comp(after, "documentation")["max_points"]
    # help text and validation
    r = gw.req("PUT", "/admin/settings/governance.quality_weight.ownership", json={"value": -5})
    assert r.status_code == 400
    assert gw.req("PUT", "/admin/settings/governance.quality_weight.ownership", json={"value": "lots"}).status_code == 400
    # all weights zero: nothing applies, the score is unknown rather than zero
    for name in ("documentation", "classification", "freshness", "certification", "lineage", "violations", "schema_stability"):
        gw.setting(f"governance.quality_weight.{name}", 0)
    none = q(gw, "orders")
    assert none["own_score"] is None and none["score"] is None and all(not c["applicable"] for c in none["components"])


def test_column_rollup_blends_the_mean_of_the_columns(fresh):
    gw = fresh
    res = q(gw, "customers_clean")
    roll = res["rollup"]
    assert roll["columns"]["count"] == 3 and roll["columns"]["sampled"] is False and roll["column_weight"] == 0.3
    cols = [q(gw, f"customers_clean.{n}")["score"] for n in ("customer_id", "email", "region")]
    assert roll["columns"]["mean"] == pytest.approx(sum(cols) / 3, abs=0.011)
    expected = round(res["own_score"] * 0.7 + roll["columns"]["mean"] * 0.3, 2)
    assert res["score"] == pytest.approx(expected, abs=0.011)
    gw.setting("governance.quality_column_rollup_weight", 0)
    assert q(gw, "customers_clean")["score"] == pytest.approx(res["own_score"], abs=0.011)
    gw.setting("governance.quality_column_rollup_weight", 1)
    r1 = q(gw, "customers_clean")
    assert r1["score"] == pytest.approx(r1["rollup"]["columns"]["mean"], abs=0.011)
    # columns have their own score; a lakehouse has none
    col = q(gw, "customers.email")
    assert col["type"] == "column" and col["score"] is not None and col["rollup"] is None
    lh = q(gw, "lakehouse_a")
    assert lh["score"] is None and "does not apply" in lh["note"]


def test_visibility_of_quality(gw):
    assert gw.get(f"/assets/{gw.aid('beta_src')}/quality", "viewer_alpha").status_code == 404
    assert gw.get(f"/assets/{gw.aid('orders')}/quality", "nobody").status_code in (403, 404)
    assert gw.get("/assets/ast_nope/quality").status_code == 404
    assert gw.get(f"/assets/{gw.aid('beta_src')}/quality", "viewer_beta").status_code == 200


# ============================================================================================================================ products
def test_product_quality_rolls_up_members(fresh):
    gw = fresh
    prod = gw.product(description="Customer master data for the sales teams")
    for n in ("customers", "orders"):
        gw.ok(gw.post(f"/products/{prod['id']}/members", {"asset_id": gw.aid(n)}, "cur_org"), 201)
    gw.ok(gw.post(f"/products/{prod['id']}/members", {"asset_id": gw.aid("beta_src")}, "cur_org"), 201)
    res = gw.ok(gw.get(f"/products/{prod['id']}/quality", "viewer_org"))
    mean = sum(q(gw, n)["score"] for n in ("customers", "orders", "beta_src")) / 3
    assert res["members_total"] == 3 and res["members_scored"] == 3 and res["member_weight"] == 0.7 and res["member_mean"] == pytest.approx(mean, abs=0.02)
    assert res["score"] == pytest.approx(res["own_score"] * 0.3 + res["member_mean"] * 0.7, abs=0.02)
    assert {c["name"] for c in res["product_components"] if c["applicable"]} == {"documentation", "ownership", "certification"}
    assert comp(res | {"components": res["product_components"]}, "documentation")["fraction"] == 1.0
    # a viewer of alpha sees the scores of what it can see; hidden members are counted but not named
    mine = gw.ok(gw.get(f"/products/{prod['id']}/quality", "viewer_alpha"))
    assert mine["hidden_members"] == 1 and {m["asset"]["name"] for m in mine["members"]} == {"customers", "orders"} and "beta_src" not in str(mine)
    # certifying and owning the product raises its own score
    gw.ok(gw.put(f"/products/{prod['id']}/certification", {"state": "certified"}, "lead_org"))
    gw.ok(gw.post(f"/products/{prod['id']}/owners", {"role": "owner", "name": "Pat"}, "cur_org"), 201)
    better = gw.ok(gw.get(f"/products/{prod['id']}/quality", "viewer_org"))
    assert better["own_score"] > res["own_score"] and better["score"] > res["score"]
    gw.setting("governance.quality_product_member_weight", 0)
    assert gw.ok(gw.get(f"/products/{prod['id']}/quality", "viewer_org"))["score"] == pytest.approx(better["own_score"], abs=0.011)


def test_empty_product_scores_on_its_own_components(gw):
    prod = gw.product()
    res = gw.ok(gw.get(f"/products/{prod['id']}/quality", "viewer_org"))
    assert res["members_total"] == 0 and res["member_mean"] is None and res["member_weight"] == 0.0 and res["score"] == res["own_score"]
    assert gw.get("/products/dpr_nope/quality").status_code == 404
