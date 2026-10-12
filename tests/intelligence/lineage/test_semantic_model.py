# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Semantic model analyzer (TMDL and model.bim): tables, columns, measures, partitions (M, Direct Lake, calculated), DAX dependencies, identity of the emitted references."""

from __future__ import annotations

import json
from collections import Counter
from typing import Any

import pytest

from olcf.intelligence.lineage.semantic_model import analyze_semantic_model, load_model, model_from_bim
from olcf.intelligence.lineage.tmdl import model_from_tmdl, parse_objects
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
ITEM = "22222222-2222-4222-8222-222222222222"
M = f"{WS}/{ITEM}"
CTX = {"item_id": ITEM, "fabric_workspace": WS, "item_name": "Retail", "default_lakehouse": {"name": "silver"}}
RETAIL = load_parts("models", "retail")
BIM = load_parts("models", "bim")


def analyze(parts: Any, ctx: dict[str, Any] | None = None) -> Facts:
    """Analyse model parts with the default test context."""
    return analyze_semantic_model(parts, context=CTX if ctx is None else ctx)


def sub(edge_set: set[str]) -> set[str]:
    """Replace the ``{M}`` placeholder of expectations by the workspace / item pair."""
    return {e.replace("{M}", M) for e in edge_set}


RETAIL_EDGES = {
    "DEPENDS_ON measure:{M}/sales/margin % -> model_table:{M}/product [derived]",
    "DEPENDS_ON measure:{M}/sales/order count -> model_table:{M}/sales [derived]",
    "DEPENDS_ON measure:{M}/sales/revenue per customer -> model_table:{M}/sales [derived]",
    "DEPENDS_ON measure:{M}/sales/total qty -> model_table:{M}/sales [derived]",
    "DEPENDS_ON measure:{M}/sales/total revenue -> model_table:{M}/sales [derived]",
    "DEPENDS_ON semantic_model:{M} -> model_table:{M}/budget [derived]",
    "DEPENDS_ON semantic_model:{M} -> model_table:{M}/customer [derived]",
    "DEPENDS_ON semantic_model:{M} -> model_table:{M}/date table [derived]",
    "DEPENDS_ON semantic_model:{M} -> model_table:{M}/product [derived]",
    "DEPENDS_ON semantic_model:{M} -> model_table:{M}/sales [derived]",
    "DEPENDS_ON semantic_model:{M} -> model_table:{M}/targets [derived]",
    "DEPENDS_ON semantic_model:{M} -> model_table:{M}/top customers [derived]",
    "READS_FROM model_table:{M}/budget -> external:sql:analytics.example.com/erp [inferred]",
    "READS_FROM model_table:{M}/customer -> external:sql:analytics.example.com/erp/Customers [derived]",
    "READS_FROM model_table:{M}/date table -> external:sql:analytics.example.com/erp/calendar [derived]",
    "READS_FROM model_table:{M}/product -> table:dim_product [derived]",
    "READS_FROM model_table:{M}/sales -> table:orders [derived]",
    "READS_FROM model_table:{M}/targets -> file:C:/Shared/targets.xlsx [derived]",
    "READS_FROM model_table:{M}/top customers -> model_table:{M}/customer [derived]",
    "TRANSFORMS col:customer.country -> col:customer.customer label [derived]",
    "TRANSFORMS col:customer.name -> col:customer.customer label [derived]",
    "TRANSFORMS col:dim_product.category -> col:product.category [derived]",
    "TRANSFORMS col:dim_product.product_id -> col:product.productkey [derived]",
    "TRANSFORMS col:dim_product.unit_cost -> col:product.unitcost [derived]",
    "TRANSFORMS col:orders.customer_id -> col:sales.customerkey [derived]",
    "TRANSFORMS col:orders.net_amount -> col:sales.revenue [derived]",
    "TRANSFORMS col:orders.order_date -> col:sales.orderdate [derived]",
    "TRANSFORMS col:orders.order_id -> col:sales.orderkey [derived]",
    "TRANSFORMS col:orders.product_id -> col:sales.productkey [derived]",
    "TRANSFORMS col:orders.qty -> col:sales.quantity [derived]",
    "TRANSFORMS col:product.unitcost -> measure:{M}/sales/margin % [derived]",
    "TRANSFORMS col:sales.customerkey -> measure:{M}/sales/revenue per customer [derived]",
    "TRANSFORMS col:sales.quantity -> measure:{M}/sales/total qty [derived]",
    "TRANSFORMS col:sales.revenue -> col:sales.revenue band [derived]",
    "TRANSFORMS col:sales.revenue -> measure:{M}/sales/total revenue [derived]",
    "USES_MEASURE measure:{M}/sales/avg price -> measure:{M}/sales/total qty [derived]",
    "USES_MEASURE measure:{M}/sales/avg price -> measure:{M}/sales/total revenue [derived]",
    "USES_MEASURE measure:{M}/sales/margin % -> measure:{M}/sales/total revenue [derived]",
    "USES_MEASURE measure:{M}/sales/revenue per customer -> measure:{M}/sales/total revenue [derived]",
    "USES_MEASURE model_table:{M}/top customers -> measure:{M}/sales/total revenue [derived]",
}


# ====================================================================================================================== retail (TMDL) model
def test_retail_model_exact_edges() -> None:
    f = analyze(RETAIL)
    assert edges(f) == sub(RETAIL_EDGES)


def test_retail_model_gaps_are_the_documented_limits_only() -> None:
    f = analyze(RETAIL)
    assert sorted((g.kind, g.subject, bool(g.location.get("informational"))) for g in f.gaps) == [
        ("not_inspected", "Budget/Budget", True), ("not_inspected", "Customer/Customer", True), ("not_inspected", "Date Table/Date Table", True),
        ("not_inspected", "Targets/Targets", True), ("unresolved_reference", "sql:analytics.example.com/erp", False)]


def test_retail_model_assets() -> None:
    f = analyze(RETAIL)
    assert Counter(a.ref.type for a in f.assets) == {"column": 19, "model_table": 7, "measure": 6, "semantic_model": 1}
    model = next(a for a in f.assets if a.ref.type == "semantic_model")
    assert dict(model.ref.alias_map) == {"pbi": M}
    sales = next(a for a in f.assets if a.ref.type == "model_table" and a.name == "Sales")
    assert dict(sales.ref.alias_map) == {"pbi": f"{M}/sales"} and sales.parent == model.ref
    measure = next(a for a in f.assets if a.ref.type == "measure" and a.name == "Total Revenue")
    assert dict(measure.ref.alias_map) == {"pbi": f"{M}/sales/total revenue"} and dict(measure.ref.scope) == {"semantic_model": "Retail", "table": "sales"}
    assert measure.attrs["expression"] == "SUM(Sales[Revenue])"


def test_retail_model_relationships_are_attributes_not_edges() -> None:
    f = analyze(RETAIL)
    model = next(a for a in f.assets if a.ref.type == "semantic_model")
    assert model.attrs == {"definition_format": "tmdl", "table_count": 7, "measure_count": 6, "relationship_count": 3,
                           "relationships": ["Sales[CustomerKey] -> Customer[CustomerKey]", "Sales[ProductKey] -> Product[ProductKey]", "Sales[OrderDate] -> Date Table[Date]"]}
    assert not [e for e in f.edges if "rel_" in repr(e)]


def test_parameters_and_shared_expressions_are_resolved() -> None:
    f = analyze(RETAIL)
    assert f"READS_FROM model_table:{M}/customer -> external:sql:analytics.example.com/erp/Customers [derived]" in edges(f), "ServerName / DatabaseName parameters"
    assert f"READS_FROM model_table:{M}/sales -> table:orders [derived]" in edges(f), "SilverLakehouse shared expression with GUID navigation"


def test_lakehouse_table_sources_carry_the_fabric_alias_and_scope() -> None:
    f = analyze(RETAIL)
    src = find(f, "READS_FROM", f"model_table:{M}/sales", "table:orders").dst
    assert dict(src.alias_map) == {"fabric": f"*/{WS}/{ITEM}/orders"}
    assert dict(src.scope) == {"fabric_lakehouse": ITEM, "fabric_workspace": WS}


def test_direct_lake_binding_uses_the_expression_source_database() -> None:
    f = analyze(RETAIL)
    e = find(f, "READS_FROM", f"model_table:{M}/product", "table:dim_product")
    assert e.dst.scope_map["lakehouse"] == "silver" and e.evidence[0].kind == "definition"
    col = find(f, "TRANSFORMS", "col:dim_product.unit_cost", "col:product.unitcost")
    assert col.via is not None and col.via.type == "semantic_model" and col.evidence[0].kind == "definition"
    assert col.confidence == "derived"


def test_column_edges_name_the_part_file_table_and_line() -> None:
    f = analyze(RETAIL)
    e = find(f, "TRANSFORMS", "col:sales.revenue", f"measure:{M}/sales/total revenue")
    loc = e.evidence[0].location
    assert loc["part"] == "definition/tables/Sales.tmdl" and loc["table"] == "Sales" and loc["measure"] == "Total Revenue" and loc["line"] >= 1
    assert e.transformation == "SUM(Sales[Revenue])" and e.via is not None


def test_columns_with_expression_are_not_given_source_edges() -> None:
    f = analyze(RETAIL)
    assert not [e for e in edges(f, "TRANSFORMS") if e.endswith("-> col:sales.revenue band [derived]") and "orders." in e]


def test_dax_variables_strings_and_table_functions() -> None:
    f = analyze(RETAIL)
    e = edges(f)
    assert f"USES_MEASURE measure:{M}/sales/revenue per customer -> measure:{M}/sales/total revenue [derived]" in e
    assert f"TRANSFORMS col:sales.customerkey -> measure:{M}/sales/revenue per customer [derived]" in e, "column inside a VAR"
    assert f"DEPENDS_ON measure:{M}/sales/order count -> model_table:{M}/sales [derived]" in e, "COUNTROWS(Sales)"


def test_calculated_table_depends_on_tables_and_measures() -> None:
    f = analyze(RETAIL)
    assert f"READS_FROM model_table:{M}/top customers -> model_table:{M}/customer [derived]" in edges(f)
    assert f"USES_MEASURE model_table:{M}/top customers -> measure:{M}/sales/total revenue [derived]" in edges(f)


def test_without_guids_objects_are_identified_by_name_and_scope() -> None:
    f = analyze(RETAIL, {"item_name": "Retail", "default_lakehouse": {"name": "silver"}})
    assert "READS_FROM model_table:Sales -> table:orders [derived]" in edges(f)
    assert "TRANSFORMS col:sales.revenue -> measure:Total Revenue [derived]" in edges(f)
    assert all(not a.ref.alias_map or a.ref.type == "semantic_model" and not a.ref.alias_map for a in f.assets)
    assert len(edges(f)) == len(RETAIL_EDGES)


def test_part_order_does_not_matter() -> None:
    forward = analyze(RETAIL)
    reverse = analyze(dict(reversed(list(RETAIL.items()))))
    assert edges(forward) == edges(reverse)
    assert gaps(forward) == gaps(reverse)


def test_platform_and_pbism_parts_are_ignored() -> None:
    parts = dict(RETAIL)
    parts[".platform"] = '{"metadata": {"type": "SemanticModel", "displayName": "Retail"}}'
    assert edges(analyze(parts)) == sub(RETAIL_EDGES)


def test_crlf_line_endings_give_the_same_result() -> None:
    parts = {k: v.replace("\n", "\r\n") for k, v in RETAIL.items()}
    assert edges(analyze(parts)) == sub(RETAIL_EDGES)


# ====================================================================================================================== model.bim
def test_bim_model_exact_edges() -> None:
    f = analyze(BIM)
    assert edges(f) == sub({
        "DEPENDS_ON measure:{M}/stock/share -> model_table:{M}/stock [derived]",
        "DEPENDS_ON measure:{M}/stock/stock value -> model_table:{M}/stock [derived]",
        "DEPENDS_ON measure:{M}/stock/units -> model_table:{M}/stock [derived]",
        "DEPENDS_ON semantic_model:{M} -> model_table:{M}/item [derived]",
        "DEPENDS_ON semantic_model:{M} -> model_table:{M}/snapshot [derived]",
        "DEPENDS_ON semantic_model:{M} -> model_table:{M}/stock [derived]",
        "READS_FROM model_table:{M}/item -> table:items [derived]",
        "READS_FROM model_table:{M}/snapshot -> model_table:{M}/stock [derived]",
        "READS_FROM model_table:{M}/stock -> external:sql:wh.example.com/wh/inv.stock [derived]",
        "TRANSFORMS col:item.cost -> col:stock.value [derived]",
        "TRANSFORMS col:items.cost -> col:item.cost [derived]",
        "TRANSFORMS col:items.sku -> col:item.sku [derived]",
        "TRANSFORMS col:stock.onhand -> col:stock.value [derived]",
        "TRANSFORMS col:stock.onhand -> measure:{M}/stock/units [derived]",
        "TRANSFORMS col:stock.value -> measure:{M}/stock/stock value [derived]",
        "USES_MEASURE measure:{M}/stock/share -> measure:{M}/stock/units [derived]",
        "USES_MEASURE model_table:{M}/snapshot -> measure:{M}/stock/units [derived]",
    })
    assert gaps(f) == [("not_inspected", "Stock/Stock")]


def test_bim_direct_lake_table_is_scoped_to_the_expression_source_database() -> None:
    f = analyze(BIM)
    src = find(f, "READS_FROM", f"model_table:{M}/item", "table:items").dst
    assert src.scope_map["lakehouse"] == "gold"


def test_bim_row_number_columns_and_line_list_expressions() -> None:
    f = analyze(BIM)
    names = {a.name for a in f.assets if a.ref.type == "column"}
    assert not [n for n in names if n.startswith("RowNumber")]
    assert f"TRANSFORMS col:stock.value -> measure:{M}/stock/stock value [derived]" in edges(f), "expression stored as a list of lines"


def test_bim_text_and_object_forms_are_equivalent() -> None:
    text = BIM["model.bim"]
    assert edges(analyze({"model.bim": text})) == edges(analyze({"model.bim": json.loads(text)}))
    assert edges(analyze(text, {})) == edges(analyze({"model.bim": text}, {})) != set()   # the JSON text of the model itself is accepted too


def test_model_from_bim_reads_the_tmsl_script_wrapper() -> None:
    doc = {"createOrReplace": {"object": {"database": "x"}, "database": {"name": "X", "model": {"tables": [{"name": "T", "columns": [{"name": "c", "sourceColumn": "c"}]}]}}}}
    m = model_from_bim(doc)
    assert m.name == "X" and [t.name for t in m.tables] == ["T"]


# ====================================================================================================================== DAX resolution
def tm(text: str, ctx: dict[str, Any] | None = None) -> Facts:
    """Analyse a one-file TMDL model."""
    return analyze({"definition/model.tmdl": text}, ctx if ctx is not None else {})


TWO_TABLES = """table A
	column x
		sourceColumn: x
	column shared
		sourceColumn: shared
	measure m1 = SUM(A[x])
	measure m2 = [m1] + [m3]
	measure m3 = [m2] * 2
	measure self = [self] + 1
	measure bad = SUM(A[nope]) + [nomeasure] + COUNTROWS(Nope)
	measure strings = "A[x]" & FORMAT(1, "[m1]") // A[shared]
	measure amb = [shared]
	measure scoped = CALCULATE([m1], B[y]) + [y]
	column calc = A[x] * 2
	partition A = m
		source = Sql.Database("s","d"){[Schema="dbo",Item="A"]}[Data]
table B
	column y
		sourceColumn: y
	column shared
	partition B = m
		source = Sql.Database("s","d"){[Schema="dbo",Item="B"]}[Data]
"""


def test_measure_to_measure_cycles_and_self_references_do_not_loop() -> None:
    e = edges(tm(TWO_TABLES))
    assert {"USES_MEASURE measure:m2 -> measure:m1 [derived]", "USES_MEASURE measure:m2 -> measure:m3 [derived]", "USES_MEASURE measure:m3 -> measure:m2 [derived]"} <= e
    assert not [x for x in e if "measure:self ->" in x]


def test_unknown_references_become_gaps_without_edges() -> None:
    f = tm(TWO_TABLES)
    subjects = {g.subject for g in f.gaps if g.kind == "unresolved_reference"}
    assert {"A.nope", "nomeasure", "Nope"} <= subjects
    assert not [e for e in edges(f) if "nope" in e.lower() or "nomeasure" in e]


def test_references_in_strings_and_comments_are_ignored() -> None:
    e = edges(tm(TWO_TABLES))
    assert not [x for x in e if "measure:strings" in x]


def test_a_bare_column_name_present_in_two_tables_is_not_attributed() -> None:
    f = tm(TWO_TABLES)
    assert not [e for e in edges(f) if "-> measure:amb" in e]
    assert any("could be a column of any of the tables A, B" in g.detail for g in f.gaps)


def test_a_bare_column_name_is_inferred_and_a_qualified_one_is_derived() -> None:
    e = edges(tm(TWO_TABLES))
    assert "TRANSFORMS col:b.y -> measure:scoped [derived]" in e or "TRANSFORMS col:b.y -> measure:scoped [inferred]" in e
    assert "USES_MEASURE measure:scoped -> measure:m1 [derived]" in e


def test_calculated_columns_resolve_bare_names_against_their_own_table() -> None:
    e = edges(tm(TWO_TABLES))
    assert "TRANSFORMS col:a.x -> col:a.calc [derived]" in e


def test_damaged_dax_is_reported_and_the_rest_is_kept_with_lower_confidence() -> None:
    f = tm("table A\n\tcolumn x\n\t\tsourceColumn: x\n\tmeasure broken = SUM(A[x) + 1\n\tmeasure ok = SUM(A[x])\n")
    assert "parse_error" in gap_kinds(f) and "unterminated" in gap_text(f)
    assert "TRANSFORMS col:a.x -> measure:ok [derived]" in edges(f)


# ====================================================================================================================== partitions
def test_direct_lake_without_an_expression_source_or_entity_is_reported() -> None:
    f = tm("table A\n\tcolumn x\n\t\tsourceColumn: x\n\tpartition A = entity\n\t\tmode: directLake\n\t\tschemaName: dbo\n")
    assert gap_kinds(f) == {"unresolved_reference"} and not edges(f, "READS_FROM")


def test_direct_lake_with_a_guid_endpoint_is_inferred() -> None:
    text = ('expression DatabaseQuery =\n\t\tlet\n\t\t    d = Sql.Database("x.datawarehouse.fabric.microsoft.com", "33333333-3333-4333-8333-333333333333")\n\t\tin\n\t\t    d\n'
            "table A\n\tcolumn x\n\t\tsourceColumn: x\n\tpartition A = entity\n\t\tentityName: t\n\t\texpressionSource: DatabaseQuery\n")
    f = tm(text)
    assert "READS_FROM model_table:A -> table:t [inferred]" in edges(f)
    assert "TRANSFORMS col:t.x -> col:a.x [inferred]" in edges(f)
    assert any("not mapped to a lakehouse" in g.detail for g in f.gaps)


def test_import_columns_without_source_column_are_inferred_from_the_name() -> None:
    text = ('table A\n\tcolumn Amount\n\tpartition A = m\n\t\tsource =\n\t\t\t\tlet\n\t\t\t\t    s = Lakehouse.Contents(null),\n'
            f'\t\t\t\t    w = s{{[workspaceId="{WS}"]}}[Data],\n\t\t\t\t    l = w{{[lakehouseId="{ITEM}"]}}[Data],\n\t\t\t\t    t = l{{[Id="sales",ItemKind="Table"]}}[Data]\n\t\t\t\tin\n\t\t\t\t    t\n')
    f = tm(text)
    assert "TRANSFORMS col:sales.amount -> col:a.amount [inferred]" in edges(f)


def test_columns_that_the_m_query_does_not_expose_are_reported_not_guessed() -> None:
    text = ('table A\n\tcolumn kept\n\t\tsourceColumn: kept\n\tcolumn gone\n\t\tsourceColumn: gone\n\tpartition A = m\n\t\tsource =\n\t\t\t\tlet\n\t\t\t\t    s = Lakehouse.Contents(null),\n'
            f'\t\t\t\t    w = s{{[workspaceId="{WS}"]}}[Data],\n\t\t\t\t    l = w{{[lakehouseId="{ITEM}"]}}[Data],\n\t\t\t\t    t = l{{[Id="sales",ItemKind="Table"]}}[Data],\n'
            '\t\t\t\t    k = Table.SelectColumns(t, {"kept"})\n\t\t\t\tin\n\t\t\t\t    k\n')
    f = tm(text)
    assert edges(f, "TRANSFORMS") == {"TRANSFORMS col:sales.kept -> col:a.kept [derived]"}
    assert gaps(f) == [("unresolved_reference", "A/A")] and "gone" in gap_text(f)


def test_m_steps_that_hide_column_names_give_an_unsupported_construct_gap() -> None:
    text = ('table A\n\tcolumn k\n\t\tsourceColumn: k\n\tpartition A = m\n\t\tsource =\n\t\t\t\tlet\n\t\t\t\t    s = Lakehouse.Contents(null),\n'
            f'\t\t\t\t    w = s{{[workspaceId="{WS}"]}}[Data],\n\t\t\t\t    l = w{{[lakehouseId="{ITEM}"]}}[Data],\n\t\t\t\t    t = l{{[Id="sales",ItemKind="Table"]}}[Data],\n'
            '\t\t\t\t    p = Table.UnpivotOtherColumns(t, {"id"}, "a", "v")\n\t\t\t\tin\n\t\t\t\t    p\n')
    f = tm(text)
    assert not edges(f, "TRANSFORMS")
    assert {"unsupported_construct"} <= gap_kinds(f) and "READS_FROM model_table:A -> table:sales [derived]" in edges(f)


def test_unknown_partition_kind_and_empty_expression_are_gaps() -> None:
    f = tm("table A\n\tpartition p = query\n\t\tsource = SELECT 1\ntable B\n\tpartition q = m\n")
    assert gap_kinds(f) <= {"not_inspected", "unsupported_construct"} and f.gaps


# ====================================================================================================================== malformed input
@pytest.mark.parametrize(
    ("parts", "kind", "needle"),
    [
        ({}, "not_inspected", "no model.bim or .tmdl part"),
        ({"readme.md": "x"}, "not_inspected", "no model.bim or .tmdl part"),
        ({"definition/tables/x.tmdl": "garbage line\n\tmore garbage"}, "parse_error", "line not understood"),
        ({"model.bim": "{not json"}, "parse_error", "not valid JSON"),
        ({"model.bim": "[]"}, "parse_error", "does not contain a JSON object"),
        ({"definition/model.tmdl": 5}, "parse_error", "not text"),
        (5, "parse_error", "not a mapping"),
        (None, "parse_error", "not a mapping"),
        ("not json", "parse_error", "neither"),
    ],
)
def test_malformed_definitions_give_one_clear_gap(parts: Any, kind: str, needle: str) -> None:
    f = analyze(parts)
    assert not edges(f, "READS_FROM", "TRANSFORMS")
    assert kind in gap_kinds(f)
    assert needle in gap_text(f)


def test_garbage_between_good_tables_does_not_lose_the_good_tables() -> None:
    parts = {"definition/tables/a.tmdl": "table A\n\tcolumn x\n\t\tsourceColumn: x\n\tmeasure m = SUM(A[x])\n", "definition/tables/b.tmdl": "@@@@\n\tzzz\n"}
    f = analyze(parts, {})
    assert "TRANSFORMS col:a.x -> measure:m [derived]" in edges(f)
    assert "parse_error" in gap_kinds(f)


def test_load_model_prefers_tmdl_over_bim() -> None:
    m, fmt = load_model({"model.bim": "{}", "definition/model.tmdl": "table A\n"})
    assert fmt == "tmdl" and m is not None and [t.name for t in m.tables] == ["A"]


# ====================================================================================================================== TMDL parser
def test_tmdl_parser_reads_quotes_descriptions_fences_and_comments() -> None:
    text = ("/// a description\ntable 'Sales Orders'\n\t// comment\n\tcolumn 'Net ''Amount'''\n\t\tsourceColumn: net\n"
            "\tmeasure M = ```\n\t\t\tSUM('Sales Orders'[Net 'Amount'])\n\t\t\t```\n")
    m = model_from_tmdl({"t.tmdl": text})
    (t,) = m.tables
    assert t.name == "Sales Orders" and [c.name for c in t.columns] == ["Net 'Amount'"] and t.measures[0].expression == "SUM('Sales Orders'[Net 'Amount'])"
    assert not m.problems


def test_tmdl_parser_accepts_space_indentation() -> None:
    text = "table A\n    column x\n        sourceColumn: x\n    measure m = SUM(A[x])\n"
    m = model_from_tmdl({"t.tmdl": text})
    assert [c.name for c in m.tables[0].columns] == ["x"] and m.tables[0].measures[0].name == "m"


def test_tmdl_parse_objects_reports_lines() -> None:
    roots, problems = parse_objects("table A\n\t???\n")
    assert roots and problems and problems[0][1] == 2


def test_database_and_createorreplace_headers_are_accepted() -> None:
    m = model_from_tmdl({"database.tmdl": "database Retail\n\tcompatibilityLevel: 1604\n"})
    assert not m.problems


def test_tmdl_relationships_are_read() -> None:
    m = model_from_tmdl({"r.tmdl": RETAIL["definition/relationships.tmdl"]})
    assert [(r.from_table, r.from_column, r.to_table, r.to_column) for r in m.relationships] == [
        ("Sales", "CustomerKey", "Customer", "CustomerKey"), ("Sales", "ProductKey", "Product", "ProductKey"), ("Sales", "OrderDate", "Date Table", "Date")]


def test_analysis_is_deterministic() -> None:
    a, b = analyze(RETAIL), analyze(RETAIL)
    assert [lab(x.ref) for x in a.assets] == [lab(x.ref) for x in b.assets]
    assert [(e.type, lab(e.src), lab(e.dst), e.confidence) for e in a.edges] == [(e.type, lab(e.src), lab(e.dst), e.confidence) for e in b.edges]
