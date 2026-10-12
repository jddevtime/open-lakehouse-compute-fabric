# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Power BI scanner-result analyzer: items and their aliases, report / dashboard / dataset / dataflow dependencies, data sources, embedded model lineage, and missing scan parameters."""

from __future__ import annotations

import copy
from typing import Any

import pytest

from olcf.intelligence.lineage.scanner import analyze_scanner_result
from olcf.intelligence.model import Facts
from tests.intelligence.lineage.support import (
    edges,
    find,
    gap_kinds,
    gap_text,
    gaps,
    load_json,
)

WS = "11111111-1111-4111-8111-111111111111"
WS2 = "99999999-9999-4999-8999-999999999999"
D1 = "22222222-2222-4222-8222-222222222222"
D2 = "33333333-3333-4333-8333-333333333333"
R1 = "44444444-4444-4444-8444-444444444444"
R2 = "66666666-6666-4666-8666-666666666666"
R3 = "77777777-7777-4777-8777-777777777777"
F1 = "88888888-8888-4888-8888-888888888888"
DASH = "f0f0f0f0-f0f0-4f0f-8f0f-f0f0f0f0f0f0"
SCAN: dict[str, Any] = load_json("scanner", "scan_result.json")


def scan(doc: Any = None, **ctx: Any) -> Facts:
    """Analyse a scanner result (the corpus one by default) for tenant ``t1``."""
    return analyze_scanner_result(copy.deepcopy(SCAN) if doc is None else doc, context={"tenant": "t1", **ctx})


def test_scan_result_exact_edges() -> None:
    f = scan()
    assert edges(f) == {
        f"DEPENDS_ON dashboard:{WS}/{DASH} -> report:{WS}/{R1} [derived]",
        f"DEPENDS_ON dashboard:{WS}/{DASH} -> semantic_model:{WS}/{D2} [derived]",
        f"DEPENDS_ON measure:{WS}/{D1}/sales/avg -> model_table:{WS}/{D1}/sales [derived]",
        f"DEPENDS_ON measure:{WS}/{D1}/sales/total -> model_table:{WS}/{D1}/sales [derived]",
        f"DEPENDS_ON report:{WS}/{R1} -> semantic_model:{WS}/{D1} [derived]",
        f"DEPENDS_ON report:{WS}/{R2} -> semantic_model:{WS2}/bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb [derived]",
        f"DEPENDS_ON report:{WS}/{R3} -> semantic_model:cccccccc-cccc-4ccc-8ccc-cccccccccccc [derived]",
        f"DEPENDS_ON semantic_model:{WS}/{D1} -> dataflow:{WS}/{F1} [derived]",
        f"DEPENDS_ON semantic_model:{WS}/{D1} -> model_table:{WS}/{D1}/calendar [derived]",
        f"DEPENDS_ON semantic_model:{WS}/{D1} -> model_table:{WS}/{D1}/sales [derived]",
        f"DEPENDS_ON semantic_model:{WS}/{D2} -> semantic_model:{WS}/{D1} [derived]",
        f"READS_FROM dataflow:{WS}/{F1} -> external:sql:srv.example.com/erp [derived]",
        f"READS_FROM model_table:{WS}/{D1}/sales -> external:sql:srv.example.com/erp/sales [derived]",
        f"READS_FROM semantic_model:{WS}/{D2} -> external:sql:srv.example.com/erp [derived]",
        "TRANSFORMS col:sales.amount -> col:sales.band [derived]",
        f"TRANSFORMS col:sales.amount -> measure:{WS}/{D1}/sales/total [derived]",
        f"USES_MEASURE measure:{WS}/{D1}/sales/avg -> measure:{WS}/{D1}/sales/total [derived]",
    }


def test_scan_result_gaps_name_the_scanner_parameter_to_switch_on() -> None:
    f = scan()
    got = sorted((g.kind, g.subject, bool(g.location.get("informational"))) for g in f.gaps)
    assert got == [
        ("not_inspected", "Bare", False), ("not_inspected", "Bare", False), ("not_inspected", "Broken", False), ("not_inspected", "Calendar", True),
        ("not_inspected", "Marketing", False), ("not_inspected", "Marketing", False), ("not_inspected", "Sales/Sales", True),
        ("unresolved_reference", "KPIs", True), ("unresolved_reference", "Orphan", True),
    ]
    text = gap_text(f)
    for parameter in ("datasetSchema=True", "lineage=True", "datasourceDetails"):
        assert parameter in text


def test_items_carry_fabric_and_pbi_aliases() -> None:
    f = scan()
    sm = next(a for a in f.assets if a.ref.type == "semantic_model" and a.name == "Retail")
    assert dict(sm.ref.alias_map) == {"fabric": f"t1/{WS}/{D1}", "pbi": f"{WS}/{D1}"}
    assert dict(sm.ref.scope) == {"fabric_workspace": WS}
    assert sm.attrs == {"workspace_name": "Retail Workspace", "configuredBy": "a@example.com", "targetStorageMode": "Import", "contentProviderType": "PowerBIReport",
                        "definition_format": "scanner", "table_count": 2, "measure_count": 2, "relationship_count": 0}
    assert sm.path == "Retail Workspace / Retail"
    rep = next(a for a in f.assets if a.ref.type == "report" and a.name == "Sales Overview")
    assert dict(rep.ref.alias_map) == {"fabric": f"t1/{WS}/{R1}", "pbi": f"{WS}/{R1}"}


def test_every_item_of_the_scan_belongs_to_the_olcf_workspace_the_source_maps_it_to() -> None:
    """Regression: the scanner's report / dashboard / dataset / dataflow assets were created without the OLCF workspace of the source while their tables, columns and measures had it.

    A semantic model with no workspace is visible to organisation-wide readers only, so a workspace Viewer saw the model's measures but not the model (and lineage through it was cut).
    """
    f = scan(workspace_id="wks_olcf")
    top = [a for a in f.assets if a.ref.type in ("report", "dashboard", "semantic_model", "dataflow")]
    assert {a.ref.type for a in top} == {"report", "dashboard", "semantic_model", "dataflow"}
    assert {a.workspace_id for a in f.assets if a.ref.type != "external"} == {"wks_olcf"}, [(a.ref.type, a.name) for a in f.assets if a.workspace_id != "wks_olcf" and a.ref.type != "external"]
    assert {a.workspace_id for a in scan().assets} == {None}


def test_tenant_is_part_of_the_fabric_alias_and_defaults_to_a_wildcard() -> None:
    f = analyze_scanner_result(copy.deepcopy(SCAN), context={})
    sm = next(a for a in f.assets if a.ref.type == "semantic_model" and a.name == "Retail")
    assert dict(sm.ref.alias_map)["fabric"] == f"*/{WS}/{D1}"


def test_model_objects_use_the_pbi_alias_of_the_model_analyzer() -> None:
    f = scan()
    aliases = {a.ref.alias_map["pbi"] for a in f.assets if "pbi" in a.ref.alias_map}
    assert {f"{WS}/{D1}/sales", f"{WS}/{D1}/sales/amount", f"{WS}/{D1}/sales/total", f"{WS}/{D1}/sales/avg", f"{WS}/{D1}/calendar"} <= aliases
    assert not [a for a in aliases if "rownumber" in a]


def test_evidence_kinds_metadata_for_the_scan_and_static_for_analysed_text() -> None:
    f = scan()
    assert find(f, "DEPENDS_ON", f"report:{WS}/{R1}", f"semantic_model:{WS}/{D1}").evidence[0].kind == "metadata"
    assert find(f, "READS_FROM", f"semantic_model:{WS}/{D2}", "external:sql:srv.example.com/erp").evidence[0].kind == "metadata"
    assert find(f, "READS_FROM", f"model_table:{WS}/{D1}/sales", "external:sql:srv.example.com/erp/sales").evidence[0].kind == "static"


def test_dataset_level_sources_are_dropped_when_the_tables_are_known() -> None:
    f = scan()
    assert not [e for e in edges(f) if e.startswith(f"READS_FROM semantic_model:{WS}/{D1} ")]
    assert any(e.startswith(f"READS_FROM semantic_model:{WS}/{D2} ") for e in edges(f)), "no schema in the scan: the data source stays"


def test_server_names_are_lowercased_in_external_assets() -> None:
    assert "external:sql:srv.example.com/erp" in " ".join(edges(scan()))


def test_cross_workspace_and_unknown_report_datasets() -> None:
    f = scan()
    cross = find(f, "DEPENDS_ON", f"report:{WS}/{R2}", f"semantic_model:{WS2}/bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
    assert dict(cross.dst.alias_map)["pbi"] == f"{WS2}/bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    orphan = find(f, "DEPENDS_ON", f"report:{WS}/{R3}", "semantic_model:cccccccc-cccc-4ccc-8ccc-cccccccccccc")
    assert not orphan.dst.alias_map, "no workspace known: the target is only identified by its id"


def test_paginated_reports_without_dataset_have_no_gap_but_power_bi_reports_do() -> None:
    f = scan()
    assert ("not_inspected", "Broken") in gaps(f)
    assert not [g for g in f.gaps if g.subject == "Paginated"]


def test_dashboard_tiles_link_reports_first_then_datasets() -> None:
    f = scan()
    assert f"DEPENDS_ON dashboard:{WS}/{DASH} -> report:{WS}/{R1} [derived]" in edges(f)
    assert f"DEPENDS_ON dashboard:{WS}/{DASH} -> semantic_model:{WS}/{D1} [derived]" not in edges(f), "a tile with a local report links the report, not its dataset"
    assert any("tile 'Elsewhere'" in g.detail for g in f.gaps)


def test_missing_datasource_details_are_reported_per_dataset() -> None:
    doc = copy.deepcopy(SCAN)
    del doc["datasourceInstances"]
    f = scan(doc)
    assert not [e for e in edges(f) if e.startswith("READS_FROM") and "external:sql:srv.example.com/erp [" in e]
    assert gap_text(f).count("datasourceDetails=True") >= 2


def test_gaps_are_the_only_loss_when_a_scan_parameter_is_off() -> None:
    doc = copy.deepcopy(SCAN)
    for d in doc["workspaces"][0]["datasets"]:
        d.pop("tables", None)
    f = scan(doc)
    assert not [e for e in edges(f) if e.startswith("TRANSFORMS") or e.startswith("USES_MEASURE")]
    assert "datasetSchema=True" in gap_text(f)


def test_expressions_missing_from_the_scan_are_reported() -> None:
    doc = copy.deepcopy(SCAN)
    for m in doc["workspaces"][0]["datasets"][0]["tables"][0]["measures"]:
        del m["expression"]
    f = scan(doc)
    assert "datasetExpressions=True" in gap_text(f)
    assert not [e for e in edges(f) if "measure:" in e and e.startswith("USES_MEASURE")]


@pytest.mark.parametrize(
    "doc",
    [[], {}, {"workspaces": "x"}, None, "x", 5],
)
def test_documents_without_workspaces_give_one_parse_error(doc: Any) -> None:
    f = analyze_scanner_result(doc, context={})
    assert gaps(f) == [("parse_error", "scanResult")] and not f.assets


def test_empty_workspace_list_is_fine() -> None:
    f = analyze_scanner_result({"workspaces": []}, context={})
    assert not f.assets and not f.gaps


def test_workspace_without_a_guid_is_reported_and_skipped() -> None:
    f = analyze_scanner_result({"workspaces": [{"id": "nope", "name": "W", "datasets": [{"id": D1, "name": "x"}]}]}, context={})
    assert gaps(f) == [("parse_error", "W")] and not f.assets


def test_items_with_invalid_ids_or_wrong_types_are_skipped_quietly() -> None:
    doc = {"workspaces": [{"id": WS, "datasets": [{"id": "x"}, 5, None, {"id": D1, "tables": "bad"}], "reports": [{"id": "x"}, "y"], "dashboards": [3], "dataflows": [{"id": "z"}]}]}
    f = analyze_scanner_result(doc, context={})
    assert [a.ref.type for a in f.assets] == ["semantic_model"]
    assert gap_kinds(f) <= {"not_inspected"}


def test_deterministic() -> None:
    a, b = scan(), scan()
    assert edges(a) == edges(b) and gaps(a) == gaps(b)
    assert [x.name for x in a.assets] == [x.name for x in b.assets]
