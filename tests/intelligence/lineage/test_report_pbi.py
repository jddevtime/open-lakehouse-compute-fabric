# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Power BI report analyzer (PBIR and legacy report.json): dataset binding, visual fields, filters, sorting, report-level measures, and agreement with the model analyzer."""

from __future__ import annotations

import json
from typing import Any

import pytest

from olcf.intelligence.lineage.report_pbi import analyze_report_pbi
from olcf.intelligence.lineage.semantic_model import analyze_semantic_model
from olcf.intelligence.model import Facts
from tests.intelligence.lineage.support import (
    edges,
    find,
    gap_kinds,
    gap_text,
    gaps,
    lab,
    load_parts,
)

WS = "11111111-1111-4111-8111-111111111111"
REP = "44444444-4444-4444-8444-444444444444"
MOD = "22222222-2222-4222-8222-222222222222"
CTX = {"item_id": REP, "fabric_workspace": WS, "item_name": "Retail Report", "fabric_workspace_name": "Retail Workspace", "semantic_models": {"retail": MOD}}
PBIR = load_parts("reports", "pbir_retail")
LEGACY = load_parts("reports", "legacy_retail")


def analyze(parts: Any, ctx: dict[str, Any] | None = None) -> Facts:
    """Analyse report parts with the default test context."""
    return analyze_report_pbi(parts, context=CTX if ctx is None else ctx)


def sub(items: set[str]) -> set[str]:
    """Replace the ``{R}`` / ``{M}`` placeholders by the report / model alias prefixes."""
    return {i.replace("{R}", f"{WS}/{REP}").replace("{M}", f"{WS}/{MOD}") for i in items}


PBIR_EDGES = {
    "DEPENDS_ON page:{R}/pageoverview -> col:customer.country [derived]",
    "DEPENDS_ON report:{R} -> col:date table.year [derived]",
    "DEPENDS_ON report:{R} -> semantic_model:{M} [derived]",
    "DEPENDS_ON visual:{R}/pageoverview/vrevenuebycategory -> col:sales.orderdate [derived]",
    "DEPENDS_ON visual:{R}/pageoverview/vrevenuebycategory -> measure:{M}/sales/margin % [derived]",
    "DEPENDS_ON visual:{R}/pageoverview/vrevenuebycategory -> measure:{M}/sales/total revenue [derived]",
    "DISPLAYS visual:{R}/pagedetail/vslicer -> col:product.category [derived]",
    "DISPLAYS visual:{R}/pagedetail/vtable -> col:customer.customer label [derived]",
    "DISPLAYS visual:{R}/pagedetail/vtable -> col:customer.name [derived]",
    "DISPLAYS visual:{R}/pageoverview/vkpi -> measure:{M}/sales/order count [derived]",
    "DISPLAYS visual:{R}/pageoverview/vrevenuebycategory -> col:product.category [derived]",
    "DISPLAYS visual:{R}/pageoverview/vrevenuebycategory -> measure:{M}/sales/total revenue [derived]",
}


# ===================================================================================================================================== PBIR
def test_pbir_report_exact_edges() -> None:
    assert edges(analyze(PBIR)) == sub(PBIR_EDGES)


def test_pbir_gaps_are_informational_limits() -> None:
    f = analyze(PBIR)
    assert sorted(gaps(f)) == [("unsupported_construct", "Local[Report Measure]"), ("unsupported_construct", "Retail Report")]
    assert all(g.location.get("informational") for g in f.gaps)
    assert "hierarchy" in gap_text(f) and "report-level measure" in gap_text(f)


def test_pbir_assets_carry_pbi_aliases_and_parents() -> None:
    f = analyze(PBIR)
    by_name = {a.name: a for a in f.assets}
    assert dict(by_name["Retail Report"].ref.alias_map) == {"pbi": f"{WS}/{REP}"}
    assert dict(by_name["Overview"].ref.alias_map) == {"pbi": f"{WS}/{REP}/pageoverview"} and by_name["Overview"].parent == by_name["Retail Report"].ref
    v = by_name["vKpi"]
    assert dict(v.ref.alias_map) == {"pbi": f"{WS}/{REP}/pageoverview/vkpi"} and v.parent == by_name["Overview"].ref
    assert by_name["Retail Report"].attrs == {"definition_format": "pbir"}
    assert sorted(a.ref.type for a in f.assets) == ["page", "page", "report", "visual", "visual", "visual", "visual"]


def test_roles_distinguish_displayed_fields_from_filters_sorting_and_formatting() -> None:
    f = analyze(PBIR)
    v = f"visual:{WS}/{REP}/pageoverview/vrevenuebycategory"
    assert find(f, "DISPLAYS", v, "col:product.category").attrs == {"role": "Category"}
    assert find(f, "DISPLAYS", v, f"measure:{WS}/{MOD}/sales/total revenue").attrs == {"role": "Y"}
    assert find(f, "DEPENDS_ON", v, f"measure:{WS}/{MOD}/sales/total revenue").attrs == {"role": "sort"}
    assert find(f, "DEPENDS_ON", v, f"measure:{WS}/{MOD}/sales/margin %").attrs == {"role": "format"}
    assert find(f, "DEPENDS_ON", v, "col:sales.orderdate").attrs == {"role": "filter"}
    assert find(f, "DEPENDS_ON", f"page:{WS}/{REP}/pageoverview", "col:customer.country").attrs == {"role": "filter"}
    assert find(f, "DEPENDS_ON", f"report:{WS}/{REP}", "col:date table.year").attrs == {"role": "filter"}


def test_report_binds_to_the_model_by_path_through_the_context_model_map() -> None:
    f = analyze(PBIR)
    e = find(f, "DEPENDS_ON", f"report:{WS}/{REP}", f"semantic_model:{WS}/{MOD}")
    assert dict(e.dst.alias_map) == {"pbi": f"{WS}/{MOD}"}
    assert e.evidence[0].kind == "definition" and e.evidence[0].location["part"] == "definition.pbir"
    snippet = e.evidence[0].snippet or ""
    assert "byPath" not in snippet and "path" in snippet


def test_without_the_model_guid_fields_carry_name_and_table_scope() -> None:
    f = analyze(PBIR, {"item_id": REP, "fabric_workspace": WS, "item_name": "Retail Report"})
    e = find(f, "DISPLAYS", f"visual:{WS}/{REP}/pageoverview/vkpi", "measure:Order Count")
    assert dict(e.dst.scope) == {"semantic_model": "Retail", "table": "sales"} and not e.dst.alias_map
    assert edges(f) == {x.replace(f"{WS}/{MOD}/sales/total revenue", "Total Revenue").replace(f"{WS}/{MOD}/sales/margin %", "Margin %").replace(f"{WS}/{MOD}/sales/order count", "Order Count")
                        .replace(f"semantic_model:{WS}/{MOD}", "semantic_model:Retail") for x in sub(PBIR_EDGES)}
    info = [g for g in f.gaps if g.kind == "unresolved_reference"]
    assert len(info) == 1 and info[0].location["informational"] is True and "could not be mapped to a model id" in info[0].detail


def test_report_fields_land_on_the_assets_of_the_model_analyzer() -> None:
    """The alias a report uses for ``Sales[Total Revenue]`` is exactly the alias the model analyzer gives that measure (the registry joins them on it)."""
    model = analyze_semantic_model(load_parts("models", "retail"), context={"item_id": MOD, "fabric_workspace": WS, "item_name": "Retail"})
    known = {a.ref.alias_map["pbi"] for a in model.assets if "pbi" in a.ref.alias_map}
    report = analyze(PBIR)
    targets = {e.dst.alias_map["pbi"] for e in report.edges if "pbi" in e.dst.alias_map}
    assert targets and targets <= known, sorted(targets - known)


def test_report_level_measures_are_gaps_not_edges() -> None:
    f = analyze(PBIR)
    assert not [e for e in edges(f) if "report measure" in e.lower()]
    assert any(g.subject == "Local[Report Measure]" for g in f.gaps)


def test_unresolvable_table_alias_is_a_gap() -> None:
    parts = dict(PBIR)
    parts["definition/pages/pageDetail/visuals/vTable/visual.json"] = json.dumps(
        {"visual": {"query": {"queryState": {"V": {"projections": [{"field": {"Column": {"Expression": {"SourceRef": {"Source": "x"}}, "Property": "Foo"}}}]}}}}})
    f = analyze(parts)
    assert ("unresolved_reference", "Foo") in gaps(f)
    assert not [e for e in edges(f) if "foo" in e.lower()]


# ====================================================================================================================== dataset reference
def with_pbir(doc: str) -> dict[str, str]:
    """The retail report with another ``definition.pbir``."""
    return {**PBIR, "definition.pbir": doc}


@pytest.mark.parametrize(
    ("pbir", "kind", "needle"),
    [
        (None, "not_inspected", "no definition.pbir"),
        ("{bad", "parse_error", "definition.pbir cannot be read"),
        ('{"version": "4.0"}', "not_inspected", "no datasetReference"),
        ('{"datasetReference": {"x": 1}}', "not_inspected", "neither byPath nor byConnection"),
        ('{"datasetReference": {"byConnection": {"connectionString": "x"}}}', "unresolved_reference", "names neither a model nor an id"),
    ],
)
def test_unusable_dataset_references_link_no_fields(pbir: str | None, kind: str, needle: str) -> None:
    parts = {k: v for k, v in PBIR.items() if not k.endswith(".pbir")} if pbir is None else with_pbir(pbir)
    f = analyze(parts)
    assert kind in gap_kinds(f) and needle in gap_text(f)
    assert not edges(f, "DISPLAYS", "DEPENDS_ON")
    assert not [a for a in f.assets if a.ref.type == "semantic_model"]


def test_by_connection_in_the_same_workspace_with_a_model_id() -> None:
    conn = f"Data Source=powerbi://api.powerbi.com/v1.0/myorg/Retail Workspace;initial catalog=Retail;semanticmodelid={MOD}"
    f = analyze(with_pbir(json.dumps({"datasetReference": {"byConnection": {"connectionString": conn}}})))
    assert edges(f) == sub(PBIR_EDGES)


def test_by_connection_to_another_workspace_is_kept_by_name() -> None:
    conn = "Data Source=powerbi://api.powerbi.com/v1.0/myorg/Other WS;initial catalog=Sales Model"
    f = analyze(with_pbir(json.dumps({"datasetReference": {"byConnection": {"connectionString": conn}}})))
    e = find(f, "DEPENDS_ON", f"report:{WS}/{REP}", "semantic_model:Sales Model")
    assert dict(e.dst.scope) == {"fabric_workspace_name": "Other WS"}
    assert any("Other WS" in g.detail for g in f.gaps)
    assert len(edges(f, "DISPLAYS")) == 6


# ================================================================================================================================== legacy
def test_legacy_report_exact_edges() -> None:
    f = analyze(LEGACY)
    assert edges(f) == sub({
        "DEPENDS_ON report:{R} -> col:date table.year [derived]",
        "DEPENDS_ON report:{R} -> semantic_model:{M} [derived]",
        "DEPENDS_ON visual:{R}/reportsection1/v1 -> measure:{M}/sales/total revenue [derived]",
        "DISPLAYS visual:{R}/reportsection1/v1 -> col:product.category [derived]",
        "DISPLAYS visual:{R}/reportsection1/v1 -> col:sales.revenue [derived]",
    })
    assert f.gaps == []
    assert next(a for a in f.assets if a.ref.type == "report").attrs == {"definition_format": "legacy"}


def test_legacy_prototype_query_aliases_are_resolved_to_tables() -> None:
    f = analyze(LEGACY)
    assert find(f, "DISPLAYS", f"visual:{WS}/{REP}/reportsection1/v1", "col:sales.revenue").attrs == {"role": "Y"}


# ================================================================================================================================ malformed
@pytest.mark.parametrize("parts", [{}, "x", None, 5, []])
def test_no_parts_is_one_not_inspected_gap(parts: Any) -> None:
    f = analyze(parts)
    assert gaps(f) == [("not_inspected", "Retail Report")] and "no parts" in gap_text(f)


def test_only_a_pbir_file_is_not_enough() -> None:
    f = analyze({"definition.pbir": PBIR["definition.pbir"]})
    assert edges(f) == sub({"DEPENDS_ON report:{R} -> semantic_model:{M} [derived]"})
    assert "neither PBIR pages nor a legacy report.json" in gap_text(f)


def test_a_damaged_visual_does_not_hide_the_others() -> None:
    parts = dict(PBIR)
    parts["definition/pages/pageDetail/visuals/vTable/visual.json"] = "{bad"
    f = analyze(parts)
    assert ("parse_error", "vTable") in gaps(f)
    assert edges(f) == sub({e for e in PBIR_EDGES if "vtable" not in e})


def test_a_damaged_page_file_still_lists_its_visuals() -> None:
    parts = dict(PBIR)
    parts["definition/pages/pageDetail/page.json"] = "[1]"
    assert edges(analyze(parts)) == sub(PBIR_EDGES)


def test_windows_path_separators_are_accepted() -> None:
    parts = {k.replace("/", "\\"): v for k, v in PBIR.items()}
    assert edges(analyze(parts)) == sub(PBIR_EDGES)


def test_deeply_nested_json_is_cut_off_without_failing() -> None:
    nest: Any = {"Column": {"Expression": {"SourceRef": {"Entity": "Sales"}}, "Property": "Revenue"}}
    for _ in range(500):
        nest = {"x": nest}
    parts = dict(PBIR)
    parts["definition/pages/pageDetail/visuals/vSlicer/visual.json"] = json.dumps({"visual": {"query": {"queryState": {"V": {"projections": [{"field": nest}]}}}}})
    f = analyze(parts)
    assert not [e for e in edges(f) if "vslicer" in e]


def test_analysis_is_deterministic_and_order_independent() -> None:
    a = analyze(PBIR)
    b = analyze(dict(reversed(list(PBIR.items()))))
    assert [(e.type, lab(e.src), lab(e.dst)) for e in a.edges] == [(e.type, lab(e.src), lab(e.dst)) for e in b.edges]
