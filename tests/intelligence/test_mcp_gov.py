# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""The read-only governance MCP tools over the real MCP transport (``POST /mcp`` with scoped platform tokens) against the populated two-workspace world.

Proves that agents can ask "What does Revenue mean?", "Who owns this and is it certified?", "Which reports show PII?" and "How good is this data?", that the answers are
compact and bounded, carry confidence and evidence, and respect asset visibility, and - as ADR 0014 promises - that the ``mcp`` scope reads and nothing else: every governance
write route and the intelligence sample endpoint refuse an ``mcp`` token (even one that belongs to a platform administrator whose login token may do all of them), and no tool
reaches them or returns sample values.
"""

from __future__ import annotations

import json
import re

import pytest

from tests.intelligence.surfaces_support import McpClient, intel_stack
from tests.intelligence.test_mcp_intel import Agents

GOV_TOOLS = ["governance_glossary_search", "governance_asset", "governance_exposure", "governance_quality"]


class Gov:
    """The agents of ``test_mcp_intel`` plus governance data that exercises every tool: terms with links, a domain, a product, owners, a label, certification and violations."""

    def __init__(self, ag: Agents) -> None:
        self.ag = ag
        s = ag.s
        ids = s.ids
        g = s.sdk().governance
        self.g = g
        rev = g.create_term("Revenue", definition="Money earned from sales, net of returns. " + "Booked when the order ships. " * 30, abbreviation="REV", synonyms=["sales income", "turnover"])
        for key in ("mart", "clean", "raw", "fresh", "lonely"):
            g.link_term(rev["id"], asset_id=ids[key])
        self.revenue = rev
        old = g.create_term("Churn", definition="Customers who stopped buying")
        g.delete_term(old["id"])                                                   # soft delete: deprecated
        self.churn = old
        secret_term = g.create_term("Secret sauce", definition="The recipe of workspace beta")
        g.link_term(secret_term["id"], asset_id=ids["secret"])
        self.secret_term = secret_term
        dom = g.create_domain("Sales", description="Everything about selling")
        g.set_asset_domain(ids["mart"], dom["id"])
        self.domain = dom
        prod = g.create_product("Revenue mart", description="The reporting mart", domain_id=dom["id"])
        g.add_product_member(prod["id"], ids["mart"])
        g.add_product_member(prod["id"], ids["clean"])
        self.product = prod
        g.set_owners("asset", ids["lakehouse_a"], [{"role": "owner", "name": "Data Office", "email": "data-office@example.com"}])    # inherited by every table of the lakehouse
        g.classify_asset(ids["raw.amount"], "PII", comment="contains personal amounts")
        g.classify_asset(ids["secret"], "Restricted")
        cert = g.set_certification("asset", ids["fresh"], "draft", comment="working on it")
        g.set_certification("asset", ids["fresh"], "certified", comment="reviewed", if_match=cert)
        g.create_policy("Tables have descriptions", "required_description", {"types": ["table"], "min_length": 5}, severity="medium")
        g.create_policy("Tables have owners", "required_owner", {"types": ["table"]}, severity="high")
        g.evaluate_policies()
        pending = g.create_term("profiled", definition="A table with column statistics")
        g.suggest_term_links()                                                      # leaves term-link suggestions waiting in the review queue
        self.pending = [i["id"] for i in g.review_queue(kind="term_link")]
        assert self.pending and pending["id"]


@pytest.fixture(scope="module")
def gv(tmp_path_factory):
    with intel_stack(tmp_path_factory.mktemp("mcp_gov"), extra_modules=("olcf.mcp.module",)) as s:
        yield Gov(Agents(s))


# ============================================================================================================================ catalogue
def test_tool_catalogue_is_read_only_and_asks_the_right_questions(gv):
    tools = {t["name"]: t for t in gv.ag.org.result("tools/list")["tools"]}
    assert [n for n in tools if n.startswith("governance_")] == GOV_TOOLS
    for name in GOV_TOOLS:
        t = tools[name]
        assert t["annotations"]["readOnlyHint"] is True and t["annotations"]["destructiveHint"] is False and t["annotations"]["openWorldHint"] is False
        assert t["inputSchema"]["additionalProperties"] is False and len(t["description"]) > 200 and t["title"]
    for question in ("What does active customer mean?", "Who owns the orders table?", "Which reports show PII?", "How good is the quality of the sales table?"):
        assert question in " ".join(t["description"] for t in tools.values())
    assert tools["governance_exposure"]["inputSchema"]["required"] == ["classification"]
    assert tools["governance_glossary_search"]["inputSchema"]["required"] == ["query"]
    # none of the tools writes, certifies, classifies, evaluates, approves or samples
    forbidden = re.compile(r"sample|update|patch|create|delete|scan|import|set_|put_|curate|approve|reject|certify|classify|evaluate|assign|link|owner|policy")
    assert [n for n in tools if n.startswith("governance_") and forbidden.search(n.replace("governance_", "", 1))] == []
    # the tools that predate governance are exactly as they were
    assert set(tools["search_catalog"]["inputSchema"]["properties"]) == {"query", "workspace", "kinds", "limit"}


def test_tools_are_listed_only_while_governance_is_installed(gv):
    svc = gv.ag.s.ctx.service("intelligence")
    installed = svc.governance
    svc.governance = None
    try:
        names = {t["name"] for t in gv.ag.org.result("tools/list")["tools"]}
        assert not [n for n in names if n.startswith("governance_")] and "intel_summary" in names
        assert gv.ag.org.error("tools/call", {"name": "governance_asset", "arguments": {"asset": "mart"}})["code"] == -32602
    finally:
        svc.governance = installed
    assert gv.ag.org.call("governance_asset", asset="mart")["asset"]["name"] == "mart"


# ============================================================================================================================ writes and samples are unreachable
def test_governance_writes_and_the_sample_endpoint_are_unreachable_with_an_mcp_token(gv):
    ag, ids, http = gv.ag, gv.ag.ids, gv.ag.s.http
    probes = [
        ("POST", "/v1/governance/glossary/terms", {"name": "Hacked", "definition": "x"}),
        ("PATCH", f"/v1/governance/glossary/terms/{gv.revenue['id']}", {"definition": "changed through MCP"}),
        ("POST", f"/v1/governance/glossary/terms/{gv.revenue['id']}/links", {"asset_id": ids["lonely"]}),
        ("POST", "/v1/governance/domains", {"name": "Evil"}),
        ("PUT", f"/v1/governance/assets/{ids['lonely']}/domain", {"domain_id": gv.domain["id"]}),
        ("POST", "/v1/governance/products", {"name": "Evil product"}),
        ("PUT", f"/v1/governance/assets/{ids['lonely']}/owners", {"owners": [{"role": "owner", "name": "Mallory", "email": "m@example.com"}]}),
        ("POST", f"/v1/governance/assets/{ids['lonely']}/classifications", {"classification": "Public"}),
        ("POST", "/v1/governance/classifications", {"name": "Evil label", "category": "handling", "level": 1}),
        ("POST", "/v1/governance/classifications/scan", {}),
        ("PUT", f"/v1/governance/assets/{ids['lonely']}/certification", {"state": "certified"}),
        ("POST", "/v1/governance/changes", {"type": "certify", "subject_type": "asset", "subject_id": ids["lonely"], "payload": {"state": "certified"}}),
        ("POST", "/v1/governance/policies", {"name": "Evil", "rule_type": "required_owner", "params": {}}),
        ("POST", "/v1/governance/policies/evaluate", {}),
        ("POST", f"/v1/governance/review-queue/{gv.pending[0]}/approve", {}),
        ("POST", f"/v1/governance/review-queue/{gv.pending[0]}/reject", {"comment": "no"}),
        ("GET", f"/v1/intelligence/assets/{ids['lonely']}/sample", None),
    ]
    sampled = len(ag.s.ctx.audit.query(action="intelligence.sample_read"))
    for headers in (ag.boss_mcp, ag.boss_write):
        # reads work ...
        assert http.get(f"/v1/governance/assets/{ids['mart']}", headers=headers).status_code == 200
        assert http.get(f"/v1/governance/glossary/terms/{gv.revenue['id']}", headers=headers).status_code == 200
        # ... everything that writes or samples is refused for the very same administrator account, because the token scope has no such permission
        for method, url, body in probes:
            r = http.request(method, url, json=body, headers=headers) if body is not None else http.request(method, url, headers=headers)
            assert r.status_code == 403, (method, url, r.status_code, r.text[:200])
        # the bulk route answers 200 with a result per item, and every item of an mcp token fails on permissions
        bulk = http.post("/v1/governance/review-queue/bulk", json={"action": "approve", "ids": gv.pending}, headers=headers).json()
        assert bulk["ok"] == 0 and bulk["failed"] == len(gv.pending) and {x["error"]["code"] for x in bulk["results"]} == {"forbidden"}
    assert [i["id"] for i in gv.g.review_queue(kind="term_link")] == gv.pending       # nothing was decided
    # the control: the same account with a full login token is allowed, so the 403 above comes from the scope
    r = http.post("/v1/governance/glossary/terms", json={"name": "Control term", "definition": "created with a login token"}, headers=ag.boss_login)
    assert r.status_code == 201, r.text
    r = http.put(f"/v1/governance/assets/{ids['lonely']}/owners", json={"owners": []}, headers=ag.boss_login)
    assert r.status_code == 200, r.text
    assert len(ag.s.ctx.audit.query(action="intelligence.sample_read")) == sampled                    # no mcp probe got as far as the sampling service ...
    http.get(f"/v1/intelligence/assets/{ids['lonely']}/sample", headers=ag.boss_login)                # ... while the login token does (the sampling policy then decides, and audits)
    assert len(ag.s.ctx.audit.query(action="intelligence.sample_read")) == sampled + 1
    # the same holds for a workspace agent (Contributor can curate with a login, never through the mcp scope)
    r = http.post("/v1/governance/glossary/terms", json={"name": "By agent", "definition": "x"}, headers=ag.s.world.h["agent"])
    assert r.status_code == 403


def test_no_tool_reaches_a_write_or_samples_and_arguments_cannot_be_bent_into_one(gv):
    ag = gv.ag
    for name in ("governance_create_term", "governance_link_term", "governance_classify_asset", "governance_certify_asset", "governance_approve", "governance_set_owners",
                 "governance_create_policy", "governance_evaluate_policies", "governance_sample_asset", "governance_scan_classifications", "governance_request_change"):
        err = ag.boss.error("tools/call", {"name": name, "arguments": {}})
        assert err["code"] == -32602 and "unknown tool" in err["message"]
    bad = ag.boss.call("governance_asset", asset="lonely", certification="certified", owners=[{"role": "owner", "name": "x"}], error=True)
    assert bad["code"] == "invalid_arguments" and "unknown argument" in bad["message"]
    sampled = len(ag.s.ctx.audit.query(action="intelligence.sample_read"))
    before = (len(ag.s.ctx.audit.query(action="intelligence.asset_updated")), gv.g.get_term(gv.revenue["id"])["definition"], gv.g.certification("asset", ag.ids["lonely"])["state"])
    tour(ag.boss)
    after = (len(ag.s.ctx.audit.query(action="intelligence.asset_updated")), gv.g.get_term(gv.revenue["id"])["definition"], gv.g.certification("asset", ag.ids["lonely"])["state"])
    assert before == after and len(ag.s.ctx.audit.query(action="intelligence.sample_read")) == sampled
    names = {c["tool"] for c in ag.s.http.get("/v1/mcp/calls", params={"limit": 200, "mine": "false"}, headers=ag.s.headers()).json()["items"] if c.get("tool")}
    assert set(GOV_TOOLS) <= names                                                  # audited like every MCP call


def tour(m: McpClient) -> list[dict]:
    """Call every governance tool once with sensible arguments."""
    return [m.call("governance_glossary_search", query="revenue"), m.call("governance_asset", asset="mart"), m.call("governance_exposure", classification="PII"),
            m.call("governance_quality", asset="clean"), m.call("governance_quality", product="Revenue mart")]


def test_answers_never_carry_sample_values_or_column_statistics(gv):
    ag = gv.ag
    docs = [ag.org.call("governance_asset", asset="profiled"), ag.org.call("governance_quality", asset="profiled"),
            *tour(ag.org)]
    text = json.dumps(docs)
    for leaked in ("12345", "987654", "value_profile", "null_count", '"min"', '"max"', "sample"):
        assert leaked not in text
    # nor e-mail addresses of external contacts
    assert "data-office@example.com" not in text and "data-office@example.com" not in json.dumps(ag.org.call("governance_asset", asset="mart"))


# ============================================================================================================================ glossary
def test_what_does_revenue_mean(gv):
    r = gv.ag.org.call("governance_glossary_search", query="revenue")
    assert r["count"] == 1 and r["truncated"] is False
    t = r["terms"][0]
    assert t["name"] == "Revenue" and t["status"] == "draft" and t["abbreviation"] == "REV" and t["synonyms"] == ["sales income", "turnover"] and t["matched_on"] == "name"
    assert t["definition"].startswith("Money earned from sales") and len(t["definition"]) <= 500 and t["definition"].endswith("…")        # clipped
    assert t["linked_assets"]["count"] == 5 and len(t["linked_assets"]["examples"]) == 3 and t["linked_assets"]["truncated"] is True
    assert all(e["type"] == "table" and e["path"] for e in t["linked_assets"]["examples"])
    assert "'Revenue'" in r["summary"] and "Money earned" in r["summary"]
    wide = gv.ag.org.call("governance_glossary_search", query="revenue", assets_per_term=10)
    assert len(wide["terms"][0]["linked_assets"]["examples"]) == 5 and wide["terms"][0]["linked_assets"]["truncated"] is False
    assert gv.ag.org.call("governance_glossary_search", query="revenue", assets_per_term=11, error=True)["code"] == "invalid_arguments"


def test_glossary_search_by_synonym_abbreviation_and_deprecated_state(gv):
    org = gv.ag.org
    assert org.call("governance_glossary_search", query="turnover")["terms"][0]["matched_on"] == "synonym"
    assert org.call("governance_glossary_search", query="REV")["terms"][0]["name"] == "Revenue"
    assert org.call("governance_glossary_search", query="churn")["count"] == 0
    nothing = org.call("governance_glossary_search", query="churn")
    assert "no glossary term matches 'churn'" in nothing["summary"] and "include_deprecated" in nothing["note"]
    old = org.call("governance_glossary_search", query="churn", include_deprecated=True)
    assert old["count"] == 1 and old["terms"][0]["status"] == "deprecated"
    capped = org.call("governance_glossary_search", query="e", limit=1)
    assert capped["count"] == 1 and capped["truncated"] is True and "narrow" in capped["note"]
    assert org.call("governance_glossary_search", query="", error=True)["code"] == "invalid_arguments"


def test_glossary_links_to_hidden_assets_are_not_shown(gv):
    sauce_org = gv.ag.org.call("governance_glossary_search", query="secret sauce")["terms"][0]["linked_assets"]
    assert [e["name"] for e in sauce_org["examples"]] == ["secret"]
    sauce_alpha = gv.ag.alpha.call("governance_glossary_search", query="secret sauce")["terms"][0]["linked_assets"]
    assert sauce_alpha["examples"] == [] and "secret" not in json.dumps(sauce_alpha)


# ============================================================================================================================ asset
def test_who_owns_this_and_is_it_certified(gv):
    r = gv.ag.org.call("governance_asset", asset="mart")
    assert r["asset"]["path"] == "lakehouse_a/mart" and r["workspace"] == "alpha"
    assert r["domain"]["name"] == "Sales" and [p["name"] for p in r["data_products"]] == ["Revenue mart"] and [t["name"] for t in r["glossary_terms"]] == ["Revenue"]
    assert r["owners"] == [{"name": "Data Office", "role": "owner", "external": True, "inherited_from": "lakehouse_a"}]
    assert r["certification"]["state"] == "none" and isinstance(r["quality_score"], float)
    assert "owned by Data Office" in r["summary"] and "certification: none" in r["summary"] and "domain Sales" in r["summary"]
    assert "governance_exposure" in r["next"]
    done = gv.ag.org.call("governance_asset", asset="fresh")
    assert done["certification"]["state"] == "certified" and done["certification"]["comment"] == "reviewed" and "certification: certified" in done["summary"]
    lonely = gv.ag.org.call("governance_asset", asset="lonely")
    assert lonely["owners"][0]["name"] == "Data Office"                           # the lakehouse owner reaches every table of the lakehouse


def test_classification_direct_inherited_and_contained_with_evidence(gv):
    raw = gv.ag.org.call("governance_asset", asset="raw")
    assert [c["label"] for c in raw["classifications"]["contained_in_columns"]] == ["PII"] and "no classification label" in raw["summary"]
    col = gv.ag.org.call("governance_asset", asset="raw.amount")
    assert [c["label"] for c in col["classifications"]["direct"]] == ["PII"] and col["classifications"]["direct"][0]["status"] == "approved" and "labelled PII" in col["summary"]
    inherited = gv.ag.org.call("governance_asset", asset="clean.amount")["classifications"]["inherited"]
    assert [c["label"] for c in inherited] == ["PII"] and inherited[0]["precision"] == "column" and inherited[0]["from"] == "raw.amount" and inherited[0]["via_hops"] >= 1
    assert "confidence" in inherited[0]
    clean = gv.ag.org.call("governance_asset", asset="clean")
    assert clean["classifications"]["contained_in_columns"] == [] and clean["classifications"]["direct"] == []        # the label sits on a column of raw, reaches clean only by inheritance


def test_violations_are_listed_and_capped(gv):
    r = gv.ag.org.call("governance_asset", asset="lonely")
    assert r["violations"]["count"] >= 1 and r["violations"]["items"][0]["rule_type"] in ("required_description", "required_owner") and "open policy violation" in r["summary"]
    assert all(v["id"].startswith("vio_") and v["policy"] and v["severity"] for v in r["violations"]["items"])
    capped = gv.ag.org.call("governance_asset", asset="lonely", max_violations=1)
    assert len(capped["violations"]["items"]) == 1 and capped["violations"]["count"] == r["violations"]["count"]
    assert capped["truncated"] is (r["violations"]["count"] > 1)
    assert gv.ag.org.call("governance_asset", asset="lonely", max_violations=51, error=True)["code"] == "invalid_arguments"


def test_asset_resolution_errors_and_hidden_assets(gv):
    ag = gv.ag
    assert ag.org.call("governance_asset", asset="dup", error=True)["code"] == "bad_request"
    assert ag.org.call("governance_asset", asset="dup", workspace="alpha")["asset"]["path"] == "lakehouse_fan/dup"
    assert ag.org.call("governance_asset", asset="nothing-here", error=True)["code"] == "not_found"
    hidden = ag.alpha.call("governance_asset", asset=ag.ids["secret"], error=True)
    ghost = "ast_" + "0" * 26
    nothing = ag.alpha.call("governance_asset", asset=ghost, error=True)
    assert hidden["code"] == "not_found" and hidden["message"].replace(ag.ids["secret"], "X") == nothing["message"].replace(ghost, "X")
    assert ag.beta.call("governance_asset", asset="secret")["workspace"] == "beta"
    assert ag.org.call("governance_asset", asset="secret")["classifications"]["direct"][0]["label"] == "Restricted"


# ============================================================================================================================ exposure
def test_which_reports_show_pii(gv):
    org = gv.ag.org
    default = org.call("governance_exposure", classification="PII", types=["report"])
    assert default["exposures"] == [] and any("min_confidence" in n or "lower" in n for n in default["notes"])           # a report behind inferred lineage is not listed by default
    r = org.call("governance_exposure", classification="PII", types=["report"], min_confidence="inferred")
    assert r["classification"]["name"] == "PII" and [e["name"] for e in r["exposures"]] == ["sales_report"]
    e = r["exposures"][0]
    assert e["kind"] == "report" and e["proven"] is False and e["confidence"] == "inferred" and e["precision"] == "table" and e["from"] == "lakehouse_a/raw" and e["hops"] == 6
    assert e["path"] == ["lakehouse_a/raw", "etl", "lakehouse_a/clean", "agg", "lakehouse_a/mart", "sales_model", "sales_report"]
    assert r["counts"]["reports"] == 1 and "Reports: 1" in r["summary"] and "PII" in r["summary"]
    assert r["labelled_sources"][0]["label"] == "raw.amount" and r["labelled_source_count"] == 1
    assert r["truncated"] is False and r["complete"] is True and r["notes"]


def test_where_does_labelled_data_end_up(gv):
    r = gv.ag.org.call("governance_exposure", classification="PII")
    by = {e["name"]: e for e in r["exposures"]}
    assert by["clean"]["proven"] is True and by["clean"]["precision"] == "contains"
    assert by["mart"]["proven"] is False and by["mart"]["precision"] == "table"
    assert by["sales_model"]["kind"] == "semantic_model"
    assert r["counts"]["proven"] == 2 and r["counts"]["possible"] == 3 and r["counts"]["by_type"]["table"] >= 2 and "2 proven and 3 possible" in r["summary"]
    assert {e["label"] for e in r["exposures"] if e["name"] == "amount"} <= {"clean.amount", "raw.amount"}
    proven = gv.ag.org.call("governance_exposure", classification="PII", include_possible=False)
    assert {e["name"] for e in proven["exposures"]} < {e["name"] for e in r["exposures"]} and all(e["proven"] for e in proven["exposures"])
    shallow = gv.ag.org.call("governance_exposure", classification="PII", depth=1)
    assert len(shallow["exposures"]) < len(r["exposures"])
    ids = gv.g.get_classification("PII")["id"]
    assert gv.ag.org.call("governance_exposure", classification=ids)["classification"]["id"] == ids
    assert gv.ag.org.call("governance_exposure", classification="pii")["classification"]["name"] == "PII"


def test_exposure_is_bounded_and_errors_are_clear(gv):
    org = gv.ag.org
    capped = org.call("governance_exposure", classification="PII", max_items=1)
    assert len(capped["exposures"]) == 1 and capped["truncated"] is True and capped["complete"] is False and capped["omitted"]["exposures"] >= 1
    assert capped["counts"]["proven"] + capped["counts"]["possible"] > 1                  # the counts still describe everything
    assert org.call("governance_exposure", classification="PII", max_items=101, error=True)["code"] == "invalid_arguments"
    assert org.call("governance_exposure", classification="No such label", error=True)["code"] == "not_found"
    assert org.call("governance_exposure", classification="PII", min_confidence="maybe", error=True)["code"] == "invalid_arguments"
    assert org.call("governance_exposure", classification="PII", workspace="no-such-workspace", error=True)["code"] == "not_found"
    unlabeled = org.call("governance_exposure", classification="PHI")
    assert unlabeled["exposures"] == [] and unlabeled["labelled_source_count"] == 0 and any("nothing downstream" in n for n in unlabeled["notes"])


def test_exposure_never_names_assets_the_caller_cannot_see(gv):
    ag = gv.ag
    full = ag.org.call("governance_exposure", classification="Restricted")
    assert "secret_out" in {e["name"] for e in full["exposures"]} and full["labelled_sources"][0]["name"] == "secret"
    alpha = ag.alpha.call("governance_exposure", classification="Restricted")
    flat = json.dumps(alpha)
    assert "secret_out" not in flat and "beta_nb" not in flat and "lakehouse_b" not in flat and alpha["exposures"] == []
    assert alpha["labelled_sources"] == [{"hidden": True}]                         # like the REST API: a label exists on something the caller cannot see, nothing more
    # lineage through a hidden asset is shown as (hidden), never named: alpha reads the part of PII exposure that passes the beta notebook
    pii = ag.alpha.call("governance_exposure", classification="PII", min_confidence="inferred")
    assert "beta_nb" not in json.dumps(pii) and "secret" not in {e["name"] for e in pii["exposures"]}
    assert ag.beta.call("governance_exposure", classification="Restricted")["labelled_source_count"] == 1


# ============================================================================================================================ quality
def test_how_good_is_the_quality(gv):
    r = gv.ag.org.call("governance_quality", asset="clean")
    assert r["subject"]["name"] == "clean" and 0 <= r["score"] <= 100 and r["computed_at"]
    comps = {c["component"]: c for c in r["components"]}
    assert {"documentation", "ownership", "classification", "freshness", "certification", "lineage", "violations", "schema_stability"} <= set(comps)
    doc = comps["documentation"]
    assert doc["applicable"] is True and doc["points_lost"] == pytest.approx(doc["max_points"] - doc["points"], abs=0.01) and doc["reasons"]
    assert r["columns"]["count"] == 2 and r["own_score"] is not None
    assert "scores" in r["summary"] and "biggest losses" in r["summary"] and "documentation" in r["summary"]
    assert all(set(c) <= {"component", "applicable", "points", "max_points", "points_lost", "reasons"} for c in r["components"])
    owned = comps["ownership"]
    assert owned["points_lost"] == 0 and owned["reasons"] == []                      # the lakehouse owner is inherited
    one = gv.ag.org.call("governance_quality", asset="lonely", max_reasons=1)
    assert all(len(c.get("reasons", [])) <= 1 for c in one["components"])


def test_quality_of_a_data_product(gv):
    r = gv.ag.org.call("governance_quality", product="Revenue mart")
    assert r["subject"]["type"] == "data_product" and r["subject"]["id"] == gv.product["id"] and r["member_count"] == 2
    assert {m["name"] for m in r["members"]} == {"mart", "clean"} and all(isinstance(m["score"], float) for m in r["members"])
    assert r["score"] is not None and r["truncated"] is False and "scores" in r["summary"]
    by_id = gv.ag.org.call("governance_quality", product=gv.product["id"])
    assert by_id["score"] == r["score"]
    assert gv.ag.org.call("governance_quality", product="Nope", error=True)["code"] == "not_found"


def test_quality_argument_and_visibility_rules(gv):
    ag = gv.ag
    assert ag.org.call("governance_quality", error=True)["code"] == "bad_request"
    assert ag.org.call("governance_quality", asset="clean", product="Revenue mart", error=True)["code"] == "bad_request"
    assert ag.alpha.call("governance_quality", asset=ag.ids["secret"], error=True)["code"] == "not_found"
    assert ag.beta.call("governance_quality", asset="secret")["subject"]["name"] == "secret"


# ============================================================================================================================ who may call
def test_viewers_and_other_scopes_cannot_use_the_tools(gv):
    ag = gv.ag
    viewer = McpClient(ag.s.http, ag.viewer_headers)
    assert viewer.rpc("initialize", {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "v", "version": "1"}}, session=False).status_code == 403
    # an agent of one workspace gets the same answers as its user: org-level data of other workspaces is invisible
    assert ag.alpha.call("governance_glossary_search", query="revenue")["count"] == 1
    assert ag.alpha.call("governance_asset", asset="mart")["asset"]["name"] == "mart"


def test_results_stay_within_the_platform_size_limit(gv):
    ag = gv.ag
    ag.s.ctx.service("settings").set("mcp.max_result_bytes", 4096)
    try:
        r = ag.org.call("governance_exposure", classification="PII", min_confidence="inferred", max_items=100)
        assert len(json.dumps(r, separators=(",", ":")).encode()) <= 4096
    finally:
        ag.s.ctx.service("settings").set("mcp.max_result_bytes", 262_144)
