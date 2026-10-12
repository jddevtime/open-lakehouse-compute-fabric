# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Policies (observe only): validation and permissions, each rule type (positive, negative, idempotent), the violation lifecycle, exceptions, preview and visibility of evidence."""

from __future__ import annotations


def make(gw, rule_type, params, who="lead_org", **body):
    return gw.ok(gw.post("/policies", {"name": gw.uniq("pol"), "rule_type": rule_type, "params": params, **body}, who), 201)


def run(gw, pol, who="lead_org"):
    return gw.ok(gw.post(f"/policies/{pol['id']}/evaluate", {}, who))


def vios(gw, pol=None, who="admin", **params):
    if pol is not None:
        params["policy_id"] = pol["id"]
    return gw.ok(gw.get("/violations", who, **params))["items"]


def names(items):
    return sorted((i["asset"] or {}).get("name") for i in items)


# ============================================================================================================================ definition
def test_rule_types_validation_and_permissions(gw):
    rts = gw.ok(gw.get("/policies/rule-types", "viewer_org"))["items"]
    assert {r["rule_type"] for r in rts} == {"classification_flow", "required_owner", "required_description", "required_classification", "certified_sources_only", "freshness", "quality_min"}
    assert all(r["description"] and r["params"] for r in rts)
    body = {"name": gw.uniq("p"), "rule_type": "required_owner", "params": {}}
    assert gw.post("/policies", body, "cur_org").status_code == 403          # curators cannot administer policies
    assert gw.post("/policies", body, "cur_alpha").status_code == 403
    assert gw.post("/policies", body, "viewer_org").status_code == 403
    assert gw.get("/policies", "nobody").status_code == 403
    pol = gw.ok(gw.post("/policies", body, "lead_org"), 201)
    assert pol["mode"] == "observe" and pol["severity"] == "medium" and pol["enabled"] is True and pol["needs_evaluation"] is True
    assert gw.post("/policies", {**body}, "lead_org").status_code == 409                    # same name
    assert gw.post("/policies", {**body, "name": "n2", "rule_type": "nope"}, "lead_org").status_code == 422
    assert gw.post("/policies", {**body, "name": "n3", "params": {"bogus": 1}}, "lead_org").status_code == 400
    assert gw.post("/policies", {**body, "name": "n4", "severity": "dire"}, "lead_org").status_code == 422
    assert gw.post("/policies", {**body, "name": "n5", "bogus": 1}, "lead_org").status_code == 422
    assert gw.post("/policies", {"name": "n6", "rule_type": "freshness", "params": {}}, "lead_org").status_code == 400
    assert gw.post("/policies", {"name": "n7", "rule_type": "freshness", "params": {"max_age_hours": -1}}, "lead_org").status_code == 400
    assert gw.post("/policies", {"name": "n8", "rule_type": "quality_min", "params": {"min_score": 101}}, "lead_org").status_code == 400
    assert gw.post("/policies", {"name": "n9", "rule_type": "required_classification", "params": {}}, "lead_org").status_code == 400
    assert gw.post("/policies", {"name": "n10", "rule_type": "classification_flow", "params": {"classification": "Nope"}}, "lead_org").status_code == 404
    assert gw.post("/policies", {"name": "n11", "rule_type": "required_owner", "params": {"selector": {"bogus": 1}}}, "lead_org").status_code == 400
    assert gw.post("/policies", {"name": "n12", "rule_type": "required_owner", "params": {"types": ["spaceship"]}}, "lead_org").status_code == 400
    # patch, if-match, rule type is fixed
    got = gw.get(f"/policies/{pol['id']}", "viewer_org")
    assert gw.patch(f"/policies/{pol['id']}", {"severity": "high"}, "lead_org", headers={"If-Match": '"stale"'}).status_code in (409, 412)
    assert gw.ok(gw.patch(f"/policies/{pol['id']}", {"severity": "high"}, "lead_org", headers={"If-Match": got.headers["etag"]}))["severity"] == "high"
    assert gw.patch(f"/policies/{pol['id']}", {"rule_type": "freshness"}, "lead_org").status_code == 422
    assert gw.patch(f"/policies/{pol['id']}", {"severity": "low"}, "cur_org").status_code == 403
    assert pol["id"] in [p["id"] for p in gw.ok(gw.get("/policies", "viewer_org", rule_type="required_owner"))["items"]]
    assert gw.ok(gw.delete(f"/policies/{pol['id']}", "lead_org"))["deleted"] is True
    assert gw.get(f"/policies/{pol['id']}").status_code == 404


def test_enforce_mode_is_refused_with_remediation(gw):
    r = gw.post("/policies", {"name": gw.uniq("e"), "rule_type": "required_owner", "params": {}, "mode": "enforce"}, "lead_org")
    assert r.status_code == 422 and gw.code(r) == "unsupported"
    assert "observe" in r.json()["error"]["remediation"] and "never blocks" in r.json()["error"]["remediation"]
    pol = make(gw, "required_owner", {})
    r2 = gw.patch(f"/policies/{pol['id']}", {"mode": "enforce"}, "lead_org")
    assert r2.status_code == 422 and gw.code(r2) == "unsupported"
    assert gw.post("/policies", {"name": gw.uniq("e"), "rule_type": "required_owner", "mode": "audit"}, "lead_org").status_code == 422
    assert gw.ok(gw.get(f"/policies/{pol['id']}"))["mode"] == "observe"
    gw.delete(f"/policies/{pol['id']}", "lead_org")


# ============================================================================================================================ rules
def test_required_owner_inheritance_and_idempotence(fresh):
    gw = fresh
    pol = make(gw, "required_owner", {"types": ["table"], "selector": {"name_pattern": "customers*"}}, severity="high")
    first = run(gw, pol)
    assert first["opened"] == 2 and first["evaluated"] == 2 and first["resolved"] == 0 and first["truncated"] is False
    items = vios(gw, pol)
    assert names(items) == ["customers", "customers_clean"] and {i["severity"] for i in items} == {"high"}
    assert all("no owner" in i["message"] and i["status"] == "open" for i in items)
    ids = {i["id"]: i["first_seen_at"] for i in items}
    # evaluating again keeps ids and first_seen_at, opens nothing
    second = run(gw, pol)
    assert second["opened"] == 0 and second["updated"] == 2
    again = vios(gw, pol)
    assert {i["id"]: i["first_seen_at"] for i in again} == ids and all(i["last_seen_at"] >= i["first_seen_at"] for i in again)
    # an owner on the lakehouse is inherited by both tables: all resolved
    gw.ok(gw.post(f"/assets/{gw.aid('lakehouse_a')}/owners", {"role": "owner", "name": "Lake Owner"}, "cur_alpha"), 201)
    third = run(gw, pol)
    assert third["resolved"] == 2 and vios(gw, pol) == []
    assert names(vios(gw, pol, status="resolved")) == ["customers", "customers_clean"]
    # removing the owner brings them back with the same ids and a reopen count
    owner = gw.ok(gw.get(f"/assets/{gw.aid('lakehouse_a')}/owners"))["direct"][0]
    gw.ok(gw.delete(f"/assets/{gw.aid('lakehouse_a')}/owners/{owner['id']}", "cur_alpha"))
    fourth = run(gw, pol)
    assert fourth["reopened"] == 2 and fourth["opened"] == 0
    back = vios(gw, pol)
    assert {i["id"] for i in back} == set(ids) and {i["reopen_count"] for i in back} == {1}
    # policy view
    p = gw.ok(gw.get(f"/policies/{pol['id']}"))
    assert p["open_violations"] == 2 and p["needs_evaluation"] is False and p["last_result"]["reopened"] == 2
    # events and audit
    assert len(gw.events("governance.violation.opened", policy_id=pol["id"])) == 4 and len(gw.events("governance.violation.resolved", policy_id=pol["id"])) == 2
    assert len(gw.audit("governance.policy_evaluated")) == 4 and gw.events("governance.policy.evaluated", policy_id=pol["id"])


def test_required_owner_roles_param(fresh):
    gw = fresh
    gw.ok(gw.post(f"/assets/{gw.aid('orders')}/owners", {"role": "steward", "name": "Steve"}, "cur_alpha"), 201)
    p1 = make(gw, "required_owner", {"types": ["table"], "selector": {"name_pattern": "orders"}})
    assert run(gw, p1)["opened"] == 1                                     # a steward is not an owner
    p2 = make(gw, "required_owner", {"types": ["table"], "roles": ["owner", "steward"], "selector": {"name_pattern": "orders"}})
    assert run(gw, p2)["opened"] == 0
    assert gw.post("/policies", {"name": gw.uniq("p"), "rule_type": "required_owner", "params": {"roles": ["king"]}}, "lead_org").status_code == 400


def test_required_description(fresh):
    gw = fresh
    pol = make(gw, "required_description", {"types": ["table"], "min_length": 10, "selector": {"name_pattern": "*"}})
    res = run(gw, pol)
    got = {i["asset"]["name"]: i for i in vios(gw, pol)}
    assert set(got) == {"orders", "mart_sales", "raw_dump", "beta_src"}            # the others have a real description
    assert "only 1 characters" in got["mart_sales"]["message"] and "no description" in got["orders"]["message"]
    assert got["orders"]["evidence"]["length"] == 0 and got["orders"]["evidence"]["min_length"] == 10
    assert res["evaluated"] >= 6
    # describing the table fixes it on the next evaluation
    gw.ok(gw.req("PATCH", f"/intelligence/assets/{gw.aid('orders')}", json={"description": "One row per customer order, written by the order service."}, headers={"If-Match": gw.intel(f"/assets/{gw.aid('orders')}").headers["etag"]}))
    assert run(gw, pol)["resolved"] == 1 and "orders" not in names(vios(gw, pol))


def test_required_classification_for_sensitive_column_names(fresh):
    gw = fresh
    pol = make(gw, "required_classification", {"name_patterns": ["*EMAIL*", "ssn"]})
    run(gw, pol)
    items = {(i["asset"]["name"], i["evidence"]["table"]["name"]): i for i in vios(gw, pol)}
    assert set(items) == {("email", "customers"), ("email", "customers_clean")}
    assert "matches '*email*'" in items[("email", "customers")]["message"] and items[("email", "customers")]["evidence"]["pattern"] == "*email*"
    # a suggestion that is waiting for review is mentioned but is not approval
    gw.tick()
    run(gw, pol)
    assert {i["evidence"].get("suggestions_waiting_for_review") for i in vios(gw, pol) if i["evidence"]["table"]["name"] == "customers"} == {1}
    assert len(vios(gw, pol)) == 2
    # approving it satisfies the source column and, by lineage, the derived column
    item = next(i for i in gw.ok(gw.get("/review-queue"))["items"] if i["asset"]["name"] == "email")
    gw.ok(gw.post(f"/review-queue/{item['id']}/approve", {}, "cur_alpha"))
    res = run(gw, pol)
    assert res["resolved"] == 2 and vios(gw, pol) == []
    # allowed_labels narrows what counts
    strict = make(gw, "required_classification", {"name_patterns": ["*email*"], "allowed_labels": ["Confidential"]})
    run(gw, strict)
    assert len(vios(gw, strict)) == 2 and {v["evidence"]["has_other_labels"] != [] for v in vios(gw, strict)} == {True}


def test_classification_flow(fresh):
    gw = fresh
    gw.ok(gw.classify("customers.email", "PII", "cur_alpha"), 201)
    pol = make(gw, "classification_flow", {"classification": "PII"}, severity="critical")
    run(gw, pol)
    got = {i["asset"]["name"]: i for i in vios(gw, pol)}
    assert set(got) == {"customers_clean", "sales_report"}          # proven only: the mart and the model are merely possible
    one = got["customers_clean"]
    assert one["message"] == "'customers_clean' receives data labelled 'PII'" and one["severity"] == "critical"
    ev = one["evidence"]
    assert ev["proven"] is True and ev["precision"] == "contains" and ev["classification"]["name"] == "PII" and ev["from_asset"]["name"] == "email"
    assert [n["name"] for n in ev["path"]] == ["email", "email"] and ev["edges"][0]["type"] == "TRANSFORMS"
    # possible exposures only when asked for
    pol2 = make(gw, "classification_flow", {"classification": "PII", "include_possible": True})
    run(gw, pol2)
    poss = {i["asset"]["name"]: i for i in vios(gw, pol2)}
    assert {"mart_sales", "sales_model"} <= set(poss) and "possible" in poss["mart_sales"]["message"] and poss["mart_sales"]["evidence"]["proven"] is False
    # an allowed label on the receiver exempts it
    gw.ok(gw.classify("customers_clean", "Confidential", "cur_alpha"), 201)
    pol3 = make(gw, "classification_flow", {"classification": "PII", "allowed_labels": ["Confidential"]})
    run(gw, pol3)
    assert names(vios(gw, pol3)) == ["sales_report"]
    # a masking tag on the path exempts everything downstream of it
    gw.ok(gw.req("PATCH", f"/intelligence/assets/{gw.aid('customers_clean.email')}", json={"tags": ["Masked"]}, headers={"If-Match": gw.intel(f"/assets/{gw.aid('customers_clean.email')}").headers["etag"]}))
    pol4 = make(gw, "classification_flow", {"classification": "PII"})
    run(gw, pol4)
    assert vios(gw, pol4) == []
    pol5 = make(gw, "classification_flow", {"classification": "PII", "masking_tag": ""})
    run(gw, pol5)
    assert len(vios(gw, pol5)) == 2
    # target selector narrows the receivers
    pol6 = make(gw, "classification_flow", {"classification": "PII", "masking_tag": "", "target": {"types": ["report"]}})
    run(gw, pol6)
    assert names(vios(gw, pol6)) == ["sales_report"]
    # removing the source label resolves the findings
    gw.ok(gw.delete(f"/assets/{gw.aid('customers.email')}/classifications/PII", "cur_alpha"))
    assert run(gw, pol)["resolved"] == 2 and vios(gw, pol) == []


def test_classification_flow_without_any_labelled_data_reports_nothing(fresh):
    gw = fresh
    pol = make(gw, "classification_flow", {"classification": "Credentials"})
    r = run(gw, pol)
    assert r["opened"] == 0 and vios(gw, pol) == []


def test_certified_sources_only(fresh):
    gw = fresh
    pol = make(gw, "certified_sources_only", {})
    run(gw, pol)
    # the report -> model dependency is only *inferred*; governance ignores lineage below governance.propagation_min_confidence (derived), so it reports nothing
    assert vios(gw, pol) == []
    gw.setting("governance.propagation_min_confidence", "inferred")
    run(gw, pol)
    got = vios(gw, pol)
    assert names(got) == ["sales_report"]
    ev = got[0]["evidence"]
    assert ev["uncertified_count"] >= 1 and {s["name"] for s in ev["uncertified_sources"]} == {"sales_model"} and ev["uncertified_sources"][0]["certification"] == "none"
    assert "sales_model" not in got[0]["message"]
    # certifying the source resolves it
    gw.ok(gw.put(f"/assets/{gw.aid('sales_model')}/certification", {"state": "certified"}, "appr_alpha"))
    assert run(gw, pol)["resolved"] == 1 and vios(gw, pol) == []
    # deprecating again brings it back; a certified data product that contains the source counts too (default)
    gw.ok(gw.put(f"/assets/{gw.aid('sales_model')}/certification", {"state": "deprecated"}, "appr_alpha"))
    assert run(gw, pol)["reopened"] == 1
    prod = gw.product()
    gw.ok(gw.post(f"/products/{prod['id']}/members", {"asset_id": gw.aid("sales_model")}, "cur_org"), 201)
    gw.ok(gw.put(f"/products/{prod['id']}/certification", {"state": "certified"}, "lead_org"))
    assert run(gw, pol)["resolved"] == 1
    no_products = make(gw, "certified_sources_only", {"include_products": False})
    run(gw, no_products)
    assert names(vios(gw, no_products)) == ["sales_report"]


def test_freshness_unknown_is_not_a_violation(fresh):
    gw = fresh
    pol = make(gw, "freshness", {"max_age_hours": 24, "types": ["table"], "selector": {"name_pattern": "*"}})
    res = run(gw, pol)
    got = {i["asset"]["name"]: i for i in vios(gw, pol)}
    assert set(got) == {"orders"} and got["orders"]["evidence"]["age_hours"] > 24 * 365 and got["orders"]["evidence"]["last_write_source"] == "table modified time"
    assert res["unknown"] >= 1                          # raw_dump has no known write time: unknown, never reported
    assert "raw_dump" not in names(vios(gw, pol, status="any"))
    # a write observed by lineage counts as well as the table's modified time: customers_clean was modified 2 hours ago but written by the ETL just now
    tight = make(gw, "freshness", {"max_age_hours": 0.5, "selector": {"name_pattern": "customers*"}})
    run(gw, tight)
    assert names(vios(gw, tight)) == ["customers"]


def test_quality_min_and_unknown(fresh):
    gw = fresh
    pol = make(gw, "quality_min", {"min_score": 60, "types": ["table"], "selector": {"name_pattern": "*"}})
    res = run(gw, pol)
    items = {i["asset"]["name"]: i for i in vios(gw, pol)}
    assert items and res["opened"] == len(items)
    for name, i in items.items():
        assert i["evidence"]["score"] < 60 and i["evidence"]["largest_losses"] and "quality score" in i["message"]
        # the score of the policy ignores quality_min violations, the asset view counts them (its score can only be lower)
        assert gw.ok(gw.get(f"/assets/{gw.aid(name)}/quality"))["score"] <= i["evidence"]["score"] and "quality_min" in i["evidence"]["note"]
    zero = make(gw, "quality_min", {"min_score": 0})
    assert run(gw, zero)["opened"] == 0
    with_products = make(gw, "quality_min", {"min_score": 100, "types": ["lakehouse"], "include_products": True})
    prod = gw.product()
    gw.ok(gw.post(f"/products/{prod['id']}/members", {"asset_id": gw.aid("raw_dump")}, "cur_org"), 201)
    run(gw, with_products)
    assert any(v["subject_type"] == "product" for v in vios(gw, with_products, who="viewer_org"))


# ============================================================================================================================ lifecycle
def test_acknowledge_reopen_and_authorization(fresh):
    gw = fresh
    pol = make(gw, "required_owner", {"types": ["table"], "selector": {"name_pattern": "orders"}})
    run(gw, pol)
    v = vios(gw, pol)[0]
    assert gw.post(f"/violations/{v['id']}/acknowledge", {}, "viewer_alpha").status_code == 403
    assert gw.post(f"/violations/{v['id']}/acknowledge", {}, "cur_beta").status_code == 404
    ack = gw.ok(gw.post(f"/violations/{v['id']}/acknowledge", {"comment": "known, owner search under way"}, "cur_alpha"))
    assert ack["status"] == "acknowledged" and ack["ack_comment"] and ack["acknowledged_by"]
    assert gw.ok(gw.post(f"/violations/{v['id']}/acknowledge", {}, "cur_alpha"))["status"] == "acknowledged"      # repeat: no change
    assert vios(gw, pol)[0]["status"] == "acknowledged"                    # still reported
    assert run(gw, pol)["opened"] == 0 and vios(gw, pol)[0]["status"] == "acknowledged"
    assert vios(gw, pol, status="open") == [] and len(vios(gw, pol, status="acknowledged,open")) == 1
    back = gw.ok(gw.post(f"/violations/{v['id']}/reopen", {}, "cur_alpha"))
    assert back["status"] == "open" and back["acknowledged_by"] is None
    assert gw.get("/violations", status="bogus").status_code == 400 and gw.get("/violations", severity="bogus").status_code == 400
    assert gw.get(f"/violations/{v['id']}", "viewer_alpha").status_code == 200 and gw.get(f"/violations/{v['id']}", "viewer_beta").status_code == 404
    assert gw.get("/violations/vio_nope").status_code == 404
    # a resolved violation cannot be acknowledged or reopened by hand
    gw.ok(gw.post(f"/assets/{gw.aid('orders')}/owners", {"role": "owner", "name": "O"}, "cur_alpha"), 201)
    run(gw, pol)
    assert gw.post(f"/violations/{v['id']}/acknowledge", {}, "cur_alpha").status_code == 409
    assert gw.post(f"/violations/{v['id']}/reopen", {}, "cur_alpha").status_code == 409
    assert gw.audit("governance.violation_updated")


def test_policy_exception_through_change_request(fresh):
    gw = fresh
    pol = make(gw, "required_owner", {"types": ["table"], "selector": {"name_pattern": "orders"}})
    run(gw, pol)
    v = vios(gw, pol)[0]
    body = {"type": "policy_exception", "subject_type": "violation", "subject_id": v["id"]}
    assert gw.post("/changes", {**body, "payload": {}}, "cur_alpha").status_code == 400                        # a reason is required
    assert gw.post("/changes", {**body, "payload": {"reason": "r", "expires_at": "2020-01-01T00:00:00Z"}}, "cur_alpha").status_code == 400
    assert gw.post("/changes", {**body, "payload": {"reason": "r"}}, "viewer_alpha").status_code == 403
    assert gw.post("/changes", {**body, "payload": {"reason": "r"}}, "cur_beta").status_code == 404
    chg = gw.ok(gw.post("/changes", {**body, "payload": {"reason": "legacy table, retired in Q4", "expires_at": "2099-01-01T00:00:00Z"}}, "cur_alpha"), 201)
    assert vios(gw, pol)[0]["status"] == "open"                      # nothing until approved
    gw.ok(gw.post(f"/changes/{chg['id']}/approve", {}, "appr_alpha"))
    ex = gw.ok(gw.get(f"/violations/{v['id']}"))
    assert ex["status"] == "excepted" and ex["exception"]["reason"].startswith("legacy") and ex["exception"]["change_id"] == chg["id"] and ex["exception"]["until"] == "2099-01-01T00:00:00Z"
    assert vios(gw, pol) == [] and len(vios(gw, pol, status="excepted")) == 1
    run(gw, pol)
    assert gw.ok(gw.get(f"/violations/{v['id']}"))["status"] == "excepted"      # evaluation does not undo it
    assert gw.ok(gw.post(f"/violations/{v['id']}/reopen", {}, "cur_alpha"))["status"] == "open"
    # an exception that expired while the finding still holds reopens it on the next evaluation
    gw.ok(gw.post(f"/changes/{gw.ok(gw.post('/changes', {**body, 'payload': {'reason': 'second'}}, 'cur_alpha'), 201)['id']}/approve", {}, "appr_alpha"))
    from sqlalchemy import update

    from olcf.intelligence.governance.schema import gov_violations as gv
    with gw.gov.db.begin() as c:
        c.execute(update(gv).where(gv.c.id == v["id"]).values(exception={"until": "2020-01-01T00:00:00.000000Z", "reason": "old", "change_id": "chg_x"}))
    out = run(gw, pol)
    assert out["reopened"] == 1 and gw.ok(gw.get(f"/violations/{v['id']}"))["status"] == "open"


def test_disabled_policy_resolves_and_cannot_be_evaluated(fresh):
    gw = fresh
    pol = make(gw, "required_owner", {"types": ["table"], "selector": {"name_pattern": "orders"}})
    run(gw, pol)
    assert len(vios(gw, pol)) == 1
    off = gw.ok(gw.patch(f"/policies/{pol['id']}", {"enabled": False}, "lead_org"))
    assert off["enabled"] is False and vios(gw, pol) == []
    assert gw.post(f"/policies/{pol['id']}/evaluate", {}, "lead_org").status_code == 409
    assert gw.ok(gw.post("/policies/evaluate", {}, "lead_org"))["results"] == [] or all(r["policy_id"] != pol["id"] for r in gw.ok(gw.post("/policies/evaluate", {}, "lead_org"))["results"])
    gw.ok(gw.patch(f"/policies/{pol['id']}", {"enabled": True}, "lead_org"))
    assert run(gw, pol)["reopened"] == 1
    assert gw.post("/policies/evaluate", {}, "cur_org").status_code == 403
    deleted = gw.ok(gw.delete(f"/policies/{pol['id']}", "lead_org"))
    assert deleted["violations_deleted"] == 1 and gw.get("/violations", policy_id=pol["id"]).json()["items"] == []


def test_preview_stores_nothing(fresh):
    gw = fresh
    r = gw.ok(gw.post("/policies/preview", {"rule_type": "required_owner", "params": {"types": ["table"], "selector": {"name_pattern": "customers*"}}, "severity": "low"}, "lead_org"))
    assert r["would_report"] == 2 and r["evaluated"] == 2 and {i["asset"]["name"] for i in r["items"]} == {"customers", "customers_clean"} and r["items"][0]["severity"] == "low"
    assert gw.ok(gw.get("/violations", status="any"))["items"] == [] and gw.ok(gw.get("/policies"))["items"] == []
    assert gw.ok(gw.post("/policies/preview", {"rule_type": "required_owner", "params": {"types": ["table"]}, "limit": 1}, "lead_org"))["items"].__len__() == 1
    assert gw.post("/policies/preview", {"rule_type": "required_owner"}, "cur_org").status_code == 403
    assert gw.post("/policies/preview", {"rule_type": "nope"}, "lead_org").status_code == 422
    assert gw.post("/policies/preview", {"params": {}}, "lead_org").status_code == 422


def test_evaluate_all_runs_every_enabled_policy_and_reports_broken_ones(fresh):
    gw = fresh
    a = make(gw, "required_owner", {"types": ["table"], "selector": {"name_pattern": "orders"}})
    b = make(gw, "required_description", {"types": ["table"], "selector": {"name_pattern": "orders"}})
    dom = gw.domain()
    c = make(gw, "required_owner", {"types": ["table"], "selector": {"domain": dom["id"]}})
    gw.ok(gw.delete(f"/domains/{dom['id']}", "lead_org"))
    res = gw.ok(gw.post("/policies/evaluate", {}, "lead_org"))["results"]
    by = {r["policy_id"]: r for r in res}
    assert by[a["id"]]["opened"] == 1 and by[b["id"]]["opened"] == 1 and "error" in by[c["id"]]
    # the evaluation pass after a scan does the same (and a broken policy does not stop it)
    r = gw.tick()
    assert "evaluated" in r


def test_truncated_evaluation_never_resolves(fresh):
    gw = fresh
    pol = make(gw, "required_description", {"types": ["table"], "min_length": 10, "selector": {"name_pattern": "*"}})
    run(gw, pol)
    n = len(vios(gw, pol))
    assert n >= 3
    # a window smaller than the estate (the setting's own minimum is 100, so the manager is told directly): the result says so and nothing is resolved
    gw.gov.policies.setting = lambda key, default=None: 1 if key == "governance.max_evaluation_assets" else default      # type: ignore[method-assign]
    res = run(gw, pol)
    assert res["truncated"] is True and res["resolved"] == 0 and len(vios(gw, pol)) == n


# ============================================================================================================================ visibility
def test_violations_are_scoped_and_evidence_is_redacted(fresh):
    gw = fresh
    desc = make(gw, "required_description", {"types": ["table"], "selector": {"name_pattern": "beta_src"}})
    run(gw, desc)
    assert names(vios(gw, desc, who="viewer_beta")) == ["beta_src"]
    assert vios(gw, desc, who="viewer_alpha") == [] and vios(gw, desc, who="viewer_org") != []
    assert gw.get("/violations", "viewer_alpha", asset_id=gw.aid("beta_src")).status_code == 404
    # evidence naming an asset the caller cannot see is replaced
    gw.ok(gw.classify("beta_src.secret_col", "Credentials", "cur_beta"), 201)
    flow = make(gw, "classification_flow", {"classification": "Credentials", "target": {"types": ["column"]}})
    run(gw, flow)
    mine = vios(gw, flow, who="viewer_alpha")
    assert names(mine) == ["secret_col"]
    ev = mine[0]["evidence"]
    assert ev["from_asset"] == {"hidden": True} and any(n == {"hidden": True} for n in ev["path"])
    assert "beta_src" not in str(mine) and gw.aid("beta_src.secret_col") not in str(mine)
    full = vios(gw, flow, who="viewer_org")
    assert full[0]["evidence"]["from_asset"]["name"] == "secret_col"
    # counts in the summary follow visibility
    assert gw.ok(gw.get("/summary", "viewer_beta"))["violations"]["open"] == 1
    assert gw.ok(gw.get("/summary", "viewer_alpha"))["violations"]["open"] == 1       # only the alpha column finding, not beta's description finding
