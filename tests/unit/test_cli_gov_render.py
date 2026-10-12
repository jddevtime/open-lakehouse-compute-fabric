# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The pure terminal renderers of governance answers (``olcf.cli.gov_render``) on hand-built documents: asset names, owners, review / change / violation rows, exposure
tables and paths (including assets the caller cannot see), the quality breakdown and the overview."""

from __future__ import annotations

from olcf.cli import gov_render as G


def ref(i, name, typ="table", path=None):
    return {"id": i, "type": typ, "name": name, "path": path, "workspace_id": "wks_1"}


def test_asset_text_prefers_labels_then_path_then_name_and_never_names_hidden_assets():
    assert G.asset_text(None) == "-" and G.asset_text({}) == "-"
    assert G.asset_text(ref("a", "clean", path="lh/clean")) == "lh/clean"
    assert G.asset_text(ref("a", "amount", "column")) == "amount"
    assert G.asset_text(ref("c", "amount", "column"), {"c": "clean.amount"}) == "clean.amount"
    assert G.asset_text({"hidden": True, "id": "x", "name": "secret"}, {"x": "leak"}) == "(hidden)"
    assert G.asset_text({"id": "ast_1"}) == "ast_1"


def test_column_refs_are_found_anywhere_in_a_document_once_and_hidden_ones_are_skipped():
    doc = {"sources": [{"asset": ref("c1", "amount", "column")}], "exposures": [{"asset": ref("t", "clean"), "path": {"nodes": [ref("c1", "amount", "column"), {"hidden": True}, ref("c2", "amount", "column")]}}]}
    assert sorted(r["id"] for r in G.column_refs(doc)) == ["c1", "c2"]
    assert G.column_refs({"asset": {"hidden": True, "type": "column", "id": "h"}}, []) == []


def test_owners_and_classes_text():
    assert G.owners_text([]) == "none" and G.owners_text(None) == "none"
    assert G.owners_text([{"name": "Ann", "role": "owner"}, {"name": "Data Office", "role": "steward", "inherited_from": {"name": "raw"}}]) == "Ann (owner), Data Office (steward, inherited from raw)"
    assert G.classes_text([{"classification": {"name": "PII"}}, {"classification": {"name": "Internal"}}, {"classification": {"name": "PII"}}]) == "Internal, PII"
    assert G.classes_text(None) == "none"


def test_row_builders():
    assert G.term_rows([{"id": "trm_1", "name": "NR", "definition": "a\n  b", "link_count": 2, "status": "draft", "certification": "none"}])[0] == {
        "id": "trm_1", "name": "NR", "abbreviation": "", "status": "draft", "certification": "none", "links": 2, "definition": "a b"}
    hit = G.search_rows([{"term": {"id": "trm_1", "name": "NR", "status": "approved", "definition": "x"}, "score": 80, "matched_on": "abbreviation"}])[0]
    assert (hit["name"], hit["score"], hit["matched"]) == ("NR", 80, "abbreviation")
    links = G.link_rows([{"id": 1, "term": {"name": "NR"}, "subject_type": "asset", "asset": ref("a", "mart", path="lh/mart"), "status": "approved", "source": "manual"},
                         {"id": 2, "term": {"name": "NR"}, "subject_type": "product", "product": {"name": "Revenue"}, "status": "pending", "source": "suggested", "confidence": 0.7}])
    assert [(r["kind"], r["target"]) for r in links] == [("asset", "lh/mart"), ("product", "Revenue")]
    review = G.review_rows([{"id": "classification:3", "kind": "classification", "asset": ref("a", "email", "column"), "classification": {"name": "PII"}, "confidence": 0.9, "source": "auto",
                             "requires": "governance.curate"}, {"id": "term_link:1", "kind": "term_link", "term": {"name": "NR"}}])
    assert [(r["proposal"], r["asset"]) for r in review] == [("PII", "email"), ("NR", "-")]
    changes = G.change_rows([{"id": "chg_1", "type": "certify", "subject": ref("a", "mart", path="lh/mart"), "status": "pending", "requester": {"name": "c"}},
                             {"id": "chg_2", "type": "policy_exception", "subject": None, "subject_id": "vio_1", "status": "approved", "requester": {}}])
    assert [r["subject"] for r in changes] == ["lh/mart", "vio_1"]
    pol = G.policy_rows([{"id": "plc_1", "name": "P", "rule_type": "freshness", "severity": "low", "mode": "observe", "enabled": True, "open_violations": 3}])[0]
    assert pol["evaluated"] == "never" and pol["open"] == 3


def test_violations_are_sorted_worst_first_then_by_subject():
    vios = [{"id": "3", "severity": "low", "asset": ref("a", "a", path="a")}, {"id": "2", "severity": "critical", "asset": ref("b", "b", path="b")},
            {"id": "1", "severity": "critical", "asset": ref("c", "c", path="a0")}, {"id": "4", "severity": "weird", "product": {"name": "Prod"}}]
    assert [r["id"] for r in G.violation_rows(vios)] == ["1", "2", "3", "4"]
    assert G.violation_rows(vios)[3]["subject"] == "Prod"


def test_exposure_rows_paths_and_headline_with_hidden_members():
    doc = {"classification": {"name": "PII"}, "summary": {"proven": 1, "possible": 1, "reports": 1, "semantic_models": 0, "products": 2},
           "exposures": [{"asset": ref("c2", "amount", "column"), "precision": "column", "proven": True, "confidence_floor": "derived", "from_asset": ref("c1", "amount", "column"),
                          "path": {"nodes": [ref("c1", "amount", "column"), ref("c2", "amount", "column")]}},
                         {"asset": ref("r", "sales_report", "report"), "precision": "table", "proven": False, "confidence_floor": "inferred", "from_asset": {"hidden": True},
                          "path": {"nodes": [{"hidden": True}, ref("e", "etl", "notebook"), ref("r", "sales_report", "report")]}}]}
    labels = {"c1": "raw.amount", "c2": "clean.amount"}
    rows = G.exposure_rows(doc, labels)
    assert [(r["asset"], r["proven"], r["from"], r["hops"]) for r in rows] == [("clean.amount", "yes", "raw.amount", 1), ("sales_report", "possible", "(hidden)", 2)]
    assert G.exposure_paths(doc, labels) == ["raw.amount -> clean.amount  [derived, proven]", "(hidden) -> etl -> sales_report  [inferred, possible]"]
    assert G.exposure_paths(doc)[0] == "amount -> amount  [derived, proven]"
    assert G.exposure_headline(doc) == "Data labelled PII reaches 2 asset(s): 1 proven, 1 possible; 1 report(s), 0 semantic model(s), 2 data product(s)."
    assert G.exposure_headline({}) == "Data labelled the label reaches 0 asset(s): 0 proven, 0 possible; 0 report(s), 0 semantic model(s), 0 data product(s)."


def test_quality_lines_show_reasons_and_inapplicable_components():
    doc = {"components": [{"name": "documentation", "applicable": True, "points": 0.0, "max_points": 22.22, "reasons": [{"points_lost": 22.22, "reason": "no description"}, {"points_lost": 1, "reason": "stale"}]},
                          {"name": "freshness", "applicable": True, "points": 16.67, "max_points": 16.67, "reasons": []},
                          {"name": "classification", "applicable": False}]}
    assert G.quality_lines(doc) == ["  documentation          0 / 22.22  -22.22 no description; -1 stale", "  freshness          16.67 / 16.67", "  classification     n/a (does not apply)"]
    assert G.quality_lines({}) == []


def test_summary_pairs_cover_the_overview_and_tolerate_missing_parts():
    d = {"glossary": {"terms": 4, "by_status": {"draft": 3, "approved": 1}}, "domains": 2, "products": {"total": 1, "by_status": {"draft": 1}},
         "classifications": {"labels": 9, "assignments": {"approved": 2}}, "review_queue": {"total": 3, "classification": 1, "term_link": 2, "change_requests": 1},
         "violations": {"open": 5, "by_severity": {"high": 5}}, "certification": {"by_state": {"certified": 1}},
         "coverage": {"datasets": 7, "owner_pct": 14.3, "description_pct": 0.0, "domain_pct": 1.0, "term_pct": 2.0, "classification_reviewed_pct": 100.0, "certified_pct": 14.3}, "evaluation": {}}
    pairs = dict(G.summary_pairs(d))
    assert pairs["glossary terms"] == "4  (draft 3, approved 1)" and pairs["review queue"].startswith("3 pending  (classifications 1, term links 2); 1 change request(s)")
    assert pairs["open violations"] == "5  (high 5)" and pairs["last policy evaluation"] == "never" and pairs["coverage of datasets"].startswith("7 datasets: owner 14.3%")
    assert dict(G.summary_pairs({}))["domains"] == 0
