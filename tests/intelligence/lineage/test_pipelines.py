# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""OLCF and Fabric pipeline analyzers: structure, item execution, copy / SQL / script activities, parameters (inferred), dynamic expressions and malformed definitions."""

from __future__ import annotations

import copy
from typing import Any

import pytest

from olcf.intelligence.lineage.base import LineageContext
from olcf.intelligence.lineage.pipeline_fabric import analyze_fabric_pipeline
from olcf.intelligence.lineage.pipeline_olcf import analyze_olcf_pipeline
from olcf.intelligence.model import Facts
from tests.intelligence.lineage.support import (
    LH,
    asset_labels,
    edges,
    find,
    gap_kinds,
    gap_text,
    gaps,
    load_json,
)

WS = "bbbbbbbb-0000-4000-8000-000000000002"
LAKEHOUSE = "aaaaaaaa-0000-4000-8000-000000000001"
WAREHOUSE = "dddddddd-0000-4000-8000-000000000004"
PIPELINE_ID = "77777777-0000-4000-8000-000000000003"


def olcf(defn: Any, **ctx: Any) -> Facts:
    """Analyse an OLCF pipeline definition with the test lakehouse as default."""
    return analyze_olcf_pipeline(defn, context=LineageContext.coerce({"item_id": "itm_pl", "item_name": "pl", "default_lakehouse": dict(LH), **ctx}))


def fabric(defn: Any, **ctx: Any) -> Facts:
    """Analyse a Fabric pipeline definition in the test workspace."""
    return analyze_fabric_pipeline(defn, context=LineageContext.coerce({"item_id": PIPELINE_ID, "item_name": "Sales Load", "fabric_workspace": WS, "tenant": "t1", **ctx}))


def one(*activities: dict[str, Any], **top: Any) -> dict[str, Any]:
    """An OLCF pipeline with the given activities."""
    return {"schema": "olcf.pipeline/1", "activities": list(activities), **top}


def act(name: str, type_: str, **tp: Any) -> dict[str, Any]:
    """An OLCF activity."""
    return {"name": name, "type": type_, "typeProperties": tp}


# ================================================================================================================================ OLCF
def test_olcf_daily_load_exact_lineage() -> None:
    f = olcf(load_json("pipelines", "olcf_daily_load.json"))
    assert edges(f, "READS_FROM", "WRITES_TO", "EXECUTES", "TRANSFORMS") == {
        "EXECUTES activity:pl/Enrich -> notebook:enrich_orders [derived]",
        "EXECUTES activity:pl/Fan/Child -> pipeline:refresh_dims [derived]",
        "EXECUTES activity:pl/Score -> spark_job:['itm_score_job'] [derived]",
        "READS_FROM activity:pl/Clean -> table:orders [derived]",
        "READS_FROM activity:pl/Fan/Py -> table:silver.orders_clean [derived]",
        "READS_FROM activity:pl/Ingest -> file:Files/landing/2026 [inferred]",
        "READS_FROM activity:pl/Notify -> external:https://hooks.example.com/notify [derived]",
        "READS_FROM activity:pl/Promote -> table:orders_stage [derived]",
        "TRANSFORMS col:orders.order_id -> col:orders_clean.order_id [inferred]",
        "TRANSFORMS col:orders.price -> col:orders_clean.revenue [inferred]",
        "TRANSFORMS col:orders.qty -> col:orders_clean.revenue [inferred]",
        "WRITES_TO activity:pl/Clean -> table:orders_clean [derived]",
        "WRITES_TO activity:pl/Fan/Py -> table:gold.orders_daily [derived]",
        "WRITES_TO activity:pl/Ingest -> table:orders_stage [inferred]",
        "WRITES_TO activity:pl/Promote -> table:orders [derived]",
    }
    assert edges(f, "DEPENDS_ON") == {
        "DEPENDS_ON activity:pl/Clean -> activity:pl/Promote [derived]",
        "DEPENDS_ON activity:pl/Enrich -> activity:pl/Clean [derived]",
        "DEPENDS_ON activity:pl/Fan -> activity:pl/Score [derived]",
        "DEPENDS_ON activity:pl/Notify -> activity:pl/Fan [derived]",
        "DEPENDS_ON activity:pl/Promote -> activity:pl/Ingest [derived]",
        "DEPENDS_ON activity:pl/Score -> activity:pl/Enrich [derived]",
    }
    assert sorted(gaps(f)) == [("dynamic_reference", "@pipeline().parameters.other"), ("unresolved_reference", "Ghost"), ("unsupported_construct", "gold.orders_daily")]


def test_olcf_pipeline_structure_assets_and_aliases() -> None:
    f = olcf(load_json("pipelines", "olcf_daily_load.json"))
    assert asset_labels(f, "pipeline") == {"pipeline:pl"}
    assert asset_labels(f, "activity") == {f"activity:pl/{n}" for n in ("Ingest", "Promote", "Clean", "Enrich", "Score", "Fan", "Fan/Child", "Fan/Py", "Notify", "Dyn", "Pause")}
    by_label = {a.name: a for a in f.assets if a.ref.type == "activity"}
    assert dict(by_label["Ingest"].ref.alias_map) == {"source": "olcf:itm_pl/Ingest"}
    assert dict(by_label["Child"].ref.alias_map) == {"source": "olcf:itm_pl/Fan/Child"}
    assert find(f, "CONTAINS", "activity:pl/Fan", "activity:pl/Fan/Child").confidence == "derived"
    dep = find(f, "DEPENDS_ON", "activity:pl/Promote", "activity:pl/Ingest")
    assert dep.attrs["conditions"] == ["succeeded"]


def test_olcf_evidence_points_at_the_json_path_of_the_activity() -> None:
    f = olcf(load_json("pipelines", "olcf_daily_load.json"))
    e = find(f, "EXECUTES", "activity:pl/Enrich", "notebook:enrich_orders")
    assert e.evidence[0].location == {"item": "itm_pl", "path": "activities[3].typeProperties.notebook", "activity": "Enrich"}
    assert e.evidence[0].kind == "definition"
    inner = find(f, "WRITES_TO", "activity:pl/Fan/Py", "table:gold.orders_daily")
    assert inner.evidence[0].location["path"] == "activities[5].typeProperties.activities[1].typeProperties.code"


def test_olcf_parameter_defaults_are_inferred_and_unknown_parameters_are_gaps() -> None:
    f = olcf(load_json("pipelines", "olcf_daily_load.json"))
    assert find(f, "WRITES_TO", "activity:pl/Ingest", "table:orders_stage").effective_confidence() == "inferred"
    assert [g.subject for g in f.gaps if g.kind == "dynamic_reference"] == ["@pipeline().parameters.other"]
    assert not [e for e in edges(f) if "Dyn" in e and not e.startswith("CONTAINS")]


def test_olcf_item_ids_and_names_both_identify_notebooks_and_jobs() -> None:
    f = olcf(one(act("N", "notebook", notebook="itm_nb1"), act("J", "spark_job", sparkJob="my_job"), act("K", "notebook", notebook="named")), known_notebooks={"named": "itm_known"})
    assert edges(f, "EXECUTES") == {"EXECUTES activity:pl/N -> notebook:['itm_nb1'] [derived]", "EXECUTES activity:pl/J -> spark_job:my_job [derived]",
                                    "EXECUTES activity:pl/K -> notebook:named [derived]"}
    assert dict(find(f, "EXECUTES", "activity:pl/K", "notebook:named").dst.alias_map) == {"olcf_item": "itm_known"}
    assert dict(find(f, "EXECUTES", "activity:pl/N", "notebook:['itm_nb1']").dst.alias_map) == {"olcf_item": "itm_nb1"}


def test_olcf_container_branches_are_walked() -> None:
    f = olcf(one(
        {"name": "Branch", "type": "if", "typeProperties": {"expression": "@true", "ifTrueActivities": [act("T", "notebook", notebook="nbT")], "ifFalseActivities": [act("F", "notebook", notebook="nbF")]}},
        {"name": "Sw", "type": "switch", "typeProperties": {"on": "@x", "cases": [{"value": "a", "activities": [act("C", "notebook", notebook="nbC")]}], "defaultActivities": [act("D", "notebook", notebook="nbD")]}},
        {"name": "Loop", "type": "for_each", "typeProperties": {"items": "@x", "activities": [act("L", "notebook", notebook="nbL")]}},
    ))
    assert edges(f, "EXECUTES") == {f"EXECUTES activity:pl/{p} -> notebook:{n} [derived]" for p, n in
                                    [("Branch/T", "nbT"), ("Branch/F", "nbF"), ("Sw/C", "nbC"), ("Sw/D", "nbD"), ("Loop/L", "nbL")]}
    assert len(edges(f, "CONTAINS")) == 8


def test_olcf_copy_file_to_table_and_table_to_file() -> None:
    f = olcf(one(act("A", "copy", source={"lakehouse": "bronze", "path": "in/x.csv"}, sink={"lakehouse": "bronze", "path": "Files/out/x.parquet", "format": "parquet"}),
                 act("B", "copy", source={"lakehouse": "bronze", "table": "t1"}, sink={"lakehouse": "silver", "table": "t2", "mode": "overwrite"})))
    assert edges(f, "READS_FROM", "WRITES_TO") == {
        "READS_FROM activity:pl/A -> file:Files/in/x.csv [derived]", "WRITES_TO activity:pl/A -> file:Files/out/x.parquet [derived]",
        "READS_FROM activity:pl/B -> table:t1 [derived]", "WRITES_TO activity:pl/B -> table:t2 [derived]",
    }
    assert find(f, "WRITES_TO", "activity:pl/B", "table:t2").attrs["mode"] == "overwrite"
    assert not edges(f, "TRANSFORMS"), "no column edges without a known source schema"


def test_olcf_copy_without_a_lakehouse_is_a_gap_because_the_activity_requires_one() -> None:
    f = olcf(one(act("A", "copy", source={"table": "s"}, sink={"table": "t"})))
    assert not edges(f, "READS_FROM", "WRITES_TO")
    assert gaps(f) == [("parse_error", "A"), ("parse_error", "A")]
    assert "names no lakehouse" in gap_text(f)


@pytest.mark.parametrize(
    ("tp", "kind"),
    [
        ({"source": {"lakehouse": "b"}, "sink": {"table": "t"}}, "parse_error"),
        ({"source": {"lakehouse": "b", "table": "s"}}, "parse_error"),
        ({"source": {"lakehouse": "b", "table": "@pipeline().parameters.nope"}, "sink": {"table": "t"}}, "dynamic_reference"),
        ({"source": {"lakehouse": "@pipeline().parameters.nope", "table": "s"}, "sink": {"table": "t"}}, "dynamic_reference"),
    ],
)
def test_olcf_incomplete_copy_sides_give_gaps_and_no_guessed_edges(tp: dict[str, Any], kind: str) -> None:
    f = olcf(one(act("A", "copy", **tp)))
    assert kind in gap_kinds(f)
    assert all("READS_FROM" not in e or "nope" not in e for e in edges(f))
    bad_side = "source" if "nope" in str(tp.get("source")) else None
    if bad_side:
        assert not edges(f, "READS_FROM")


def test_olcf_sql_activity_uses_the_activity_lakehouse_and_parameter_defaults() -> None:
    f = olcf(one(act("S", "sql", engine="spark", lakehouse="silver", sql="INSERT OVERWRITE TABLE c SELECT a, b + 1 AS b FROM @{pipeline().parameters.tbl}")),
             )
    assert not edges(f, "READS_FROM")                                           # no parameters declared -> the table name is unknown
    assert "dynamic_reference" in gap_kinds(f)
    d = one(act("S", "sql", engine="spark", lakehouse="silver", sql="INSERT OVERWRITE TABLE c SELECT a, b + 1 AS b FROM @{pipeline().parameters.tbl}"),
            parameters={"tbl": {"type": "string", "default": "orders"}})
    g = olcf(d)
    assert edges(g, "READS_FROM", "WRITES_TO") == {"READS_FROM activity:pl/S -> table:orders [inferred]", "WRITES_TO activity:pl/S -> table:c [derived]"}
    assert edges(g, "TRANSFORMS") == {"TRANSFORMS col:orders.a -> col:c.a [inferred]", "TRANSFORMS col:orders.b -> col:c.b [inferred]"}


def test_olcf_sql_activity_literal_sql_is_derived() -> None:
    f = olcf(one(act("S", "sql", sql="CREATE TABLE out AS SELECT id FROM src")))
    assert edges(f, "READS_FROM", "WRITES_TO", "TRANSFORMS") == {
        "READS_FROM activity:pl/S -> table:src [derived]", "WRITES_TO activity:pl/S -> table:out [derived]", "TRANSFORMS col:src.id -> col:out.id [derived]"}


def test_olcf_script_activity_python_is_analysed_and_shell_is_a_gap() -> None:
    f = olcf(one(act("P", "script", language="python", code="spark.table('a').write.saveAsTable('b')"), act("Sh", "script", language="bash", code="cp a b")))
    assert {"READS_FROM activity:pl/P -> table:a [derived]", "WRITES_TO activity:pl/P -> table:b [derived]"} <= edges(f)
    assert ("unsupported_construct", "Sh") in gaps(f)


def test_olcf_http_urls_drop_credentials_query_and_fragment() -> None:
    f = olcf(one(act("A", "http", url="https://user:pw@h.example.com:8443/p?q=1#f"), act("B", "http", url="@concat('a','b')"), act("C", "webhook_wait", url="https://hook.example.com/x")))
    assert edges(f, "READS_FROM") == {"READS_FROM activity:pl/A -> external:https://h.example.com:8443/p [derived]", "READS_FROM activity:pl/C -> external:https://hook.example.com/x [derived]"}
    assert gaps(f) == [("dynamic_reference", "B")]
    assert "pw" not in repr(f) and "q=1" not in repr(f)


def test_olcf_activities_that_move_no_data_only_produce_assets() -> None:
    f = olcf(one(act("W", "wait", seconds=5), {"name": "V", "type": "set_variable", "typeProperties": {"name": "x", "value": "y"}}, act("X", "fail", message="no")))
    assert not edges(f, "READS_FROM", "WRITES_TO", "EXECUTES") and not f.gaps
    assert len(asset_labels(f, "activity")) == 3


@pytest.mark.parametrize(
    ("defn", "kind", "needle"),
    [
        ([], "parse_error", "not a JSON object"),
        ({"activities": "no"}, "parse_error", "no 'activities'"),
        ({"schema": "other/1", "activities": []}, "parse_error", "unknown pipeline schema"),
        (one(act("A", "notebook")), "parse_error", "no typeProperties.notebook"),
        (one({"name": "A", "type": "notebook", "typeProperties": {"notebook": "x"}, "dependsOn": [{"activity": "A"}]}), "unresolved_reference", "depends on 'A'"),
        (one(act("A", "notebook", notebook="x"), act("A", "notebook", notebook="y")), "parse_error", "two activities are named 'A'"),
        (one(act("A", "notebook", notebook="@pipeline().parameters.who")), "dynamic_reference", "only known at run time"),
        ({"activities": ["x", 5]}, "parse_error", "not an object"),
    ],
)
def test_olcf_malformed_definitions_give_specific_gaps(defn: Any, kind: str, needle: str) -> None:
    f = olcf(defn)
    assert kind in gap_kinds(f)
    assert needle in gap_text(f)


def test_olcf_a_broken_activity_does_not_hide_the_others() -> None:
    f = olcf(one(act("Bad", "copy", source=5, sink=[]), act("Good", "notebook", notebook="nb")))
    assert "EXECUTES activity:pl/Good -> notebook:nb [derived]" in edges(f)
    assert "parse_error" in gap_kinds(f)


def test_olcf_context_process_is_used_and_not_duplicated() -> None:
    from olcf.intelligence.model import AssetRef

    proc = AssetRef.make("pipeline", {"olcf_item": "itm_pl"}, "pl")
    f = analyze_olcf_pipeline(one(act("N", "notebook", notebook="x")), context=LineageContext(item_id="itm_pl", item_name="pl", process=proc))
    assert not [a for a in f.assets if a.ref.type == "pipeline"]
    assert find(f, "CONTAINS", "pipeline:pl", "activity:pl/N")


def test_olcf_analysis_does_not_mutate_the_definition() -> None:
    d = load_json("pipelines", "olcf_daily_load.json")
    before = copy.deepcopy(d)
    olcf(d)
    assert d == before


# ============================================================================================================================== Fabric
def test_fabric_sales_load_exact_lineage() -> None:
    f = fabric(load_json("pipelines", "fabric_sales_load.json"))
    assert edges(f, "READS_FROM", "WRITES_TO", "EXECUTES", "TRANSFORMS") == {
        "EXECUTES activity:Sales Load/Child -> pipeline:['t1/bbbbbbbb-0000-4000-8000-000000000002/99999999-0000-4000-8000-000000000007'] [derived]",
        "EXECUTES activity:Sales Load/Loop/Dbx -> external:databricks:/Shared/etl [derived]",
        "EXECUTES activity:Sales Load/Refresh -> semantic_model:bbbbbbbb-0000-4000-8000-000000000002/11111111-0000-4000-8000-000000000008 [derived]",
        "EXECUTES activity:Sales Load/RunJob -> spark_job:['t1/bbbbbbbb-0000-4000-8000-000000000002/ffffffff-0000-4000-8000-000000000006'] [derived]",
        "EXECUTES activity:Sales Load/RunNotebook -> notebook:['t1/bbbbbbbb-0000-4000-8000-000000000002/eeeeeeee-0000-4000-8000-000000000005'] [derived]",
        "READS_FROM activity:Sales Load/CopyFromSql -> external:azuresqltable:cccccccc-0000-4000-8000-000000000009/erp.dbo.sales [derived]",
        "READS_FROM activity:Sales Load/Loop/Merge -> table:mart.sales [derived]",
        "READS_FROM activity:Sales Load/Ping -> external:https://api.example.com/v1/ping [derived]",
        "READS_FROM activity:Sales Load/StageToSilver -> table:sales_fact [derived]",
        "TRANSFORMS col:mart.sales.amount -> col:mart.daily.amount [inferred]",
        "TRANSFORMS col:mart.sales.sale_id -> col:mart.daily.sale_id [inferred]",
        "TRANSFORMS col:sales_fact.amount -> col:mart.sales.amount [derived]",
        "TRANSFORMS col:sales_fact.id -> col:mart.sales.sale_id [derived]",
        "WRITES_TO activity:Sales Load/CopyFromSql -> table:sales_fact [inferred]",
        "WRITES_TO activity:Sales Load/Loop/Merge -> table:mart.daily [derived]",
        "WRITES_TO activity:Sales Load/StageToSilver -> table:mart.sales [derived]",
    }
    assert sorted(gaps(f)) == [("dynamic_reference", "@pipeline().parameters.dynamic_ws"), ("not_inspected", "Dbx")]
    assert edges(f, "DEPENDS_ON") == {
        "DEPENDS_ON activity:Sales Load/RunNotebook -> activity:Sales Load/StageToSilver [derived]",
        "DEPENDS_ON activity:Sales Load/StageToSilver -> activity:Sales Load/CopyFromSql [derived]",
    }


def test_fabric_table_refs_carry_the_fabric_alias_and_scope() -> None:
    f = fabric(load_json("pipelines", "fabric_sales_load.json"))
    sink = find(f, "WRITES_TO", "activity:Sales Load/CopyFromSql", "table:sales_fact").dst
    assert dict(sink.alias_map) == {"fabric": f"t1/{WS}/{LAKEHOUSE}/sales_fact"}
    assert dict(sink.scope) == {"fabric_lakehouse": LAKEHOUSE, "fabric_workspace": WS, "tenant": "t1"}
    wh = find(f, "WRITES_TO", "activity:Sales Load/StageToSilver", "table:mart.sales").dst
    assert dict(wh.alias_map) == {"fabric": f"t1/{WS}/{WAREHOUSE}/mart.sales"}
    col = find(f, "TRANSFORMS", "col:sales_fact.id", "col:mart.sales.sale_id")
    assert dict(col.dst.alias_map) == {"fabric_column": f"t1/{WS}/{WAREHOUSE}/mart.sales:sale_id"}
    assert col.via is not None and col.via.type == "activity"


def test_fabric_activity_assets_have_source_aliases_and_the_pipeline_asset_exists() -> None:
    f = fabric(load_json("pipelines", "fabric_sales_load.json"))
    assert asset_labels(f, "pipeline") == {"pipeline:Sales Load"}
    names = {a.name: dict(a.ref.alias_map) for a in f.assets if a.ref.type == "activity"}
    assert names["CopyFromSql"] == {"source": f"olcf:{PIPELINE_ID}/CopyFromSql"}
    assert len(names) == 12


def test_fabric_dbo_schema_is_dropped_and_other_schemas_are_kept() -> None:
    f = fabric(load_json("pipelines", "fabric_sales_load.json"))
    written = edges(f, "WRITES_TO")
    assert "WRITES_TO activity:Sales Load/CopyFromSql -> table:sales_fact [inferred]" in written
    assert "WRITES_TO activity:Sales Load/StageToSilver -> table:mart.sales [derived]" in written


def test_fabric_translator_mappings_are_the_only_source_of_column_edges() -> None:
    d = load_json("pipelines", "fabric_sales_load.json")
    for a in d["properties"]["activities"]:
        a.get("typeProperties", {}).pop("translator", None)
    f = fabric(d)
    assert not [e for e in edges(f, "TRANSFORMS") if "sales_fact" in e]


def test_fabric_parameter_default_in_a_table_name_is_inferred_not_derived() -> None:
    f = fabric(load_json("pipelines", "fabric_sales_load.json"))
    assert find(f, "WRITES_TO", "activity:Sales Load/CopyFromSql", "table:sales_fact").effective_confidence() == "inferred"
    assert find(f, "READS_FROM", "activity:Sales Load/StageToSilver", "table:sales_fact").effective_confidence() == "derived"


def test_fabric_items_in_an_unknown_workspace_are_unresolved_not_guessed() -> None:
    d = {"properties": {"activities": [{"name": "N", "type": "TridentNotebook", "typeProperties": {"notebookId": "eeeeeeee-0000-4000-8000-000000000005"}}]}}
    f = analyze_fabric_pipeline(d, context=LineageContext.coerce({"item_name": "p"}))
    assert not edges(f, "EXECUTES")
    assert gaps(f) == [("unresolved_reference", "eeeeeeee-0000-4000-8000-000000000005")]
    g = analyze_fabric_pipeline(d, context=LineageContext.coerce({"item_name": "p", "fabric_workspace": WS}))
    assert len(edges(g, "EXECUTES")) == 1


def test_fabric_item_workspace_override_in_the_activity() -> None:
    other = "12121212-0000-4000-8000-000000000099"
    d = {"properties": {"activities": [{"name": "N", "type": "TridentNotebook", "typeProperties": {"notebookId": "eeeeeeee-0000-4000-8000-000000000005", "workspaceId": other}}]}}
    f = fabric(d)
    (e,) = [e for e in f.edges if e.type == "EXECUTES"]
    assert dict(e.dst.alias_map) == {"fabric": f"t1/{other}/eeeeeeee-0000-4000-8000-000000000005"}


def test_fabric_non_guid_item_id_is_an_unresolved_reference() -> None:
    d = {"properties": {"activities": [{"name": "N", "type": "TridentNotebook", "typeProperties": {"notebookId": "not-a-guid"}}]}}
    f = fabric(d)
    assert not edges(f, "EXECUTES") and gaps(f) == [("unresolved_reference", "not-a-guid")]


def test_fabric_lakehouse_files_and_unidentified_lakehouse() -> None:
    ls = {"properties": {"type": "Lakehouse", "typeProperties": {"workspaceId": WS, "artifactId": LAKEHOUSE, "rootFolder": "Files"}}}
    d = {"properties": {"activities": [{"name": "C", "type": "Copy", "typeProperties": {
        "source": {"type": "DelimitedTextSource", "datasetSettings": {"type": "DelimitedText", "typeProperties": {"location": {"folderPath": "in", "fileName": "a.csv"}}, "linkedService": ls}},
        "sink": {"type": "LakehouseTableSink", "datasetSettings": {"type": "LakehouseTable", "typeProperties": {"table": "t"},
                 "linkedService": {"properties": {"type": "Lakehouse", "typeProperties": {"rootFolder": "Tables"}}}}}}}]}}
    f = fabric(d, fabric_workspace=None)
    assert edges(f, "READS_FROM") == {"READS_FROM activity:Sales Load/C -> file:Files/in/a.csv [derived]"}
    assert not edges(f, "WRITES_TO")
    assert gaps(f) == [("unresolved_reference", "C")]


def test_fabric_lookup_and_script_use_the_tsql_dialect() -> None:
    d = {"properties": {"activities": [
        {"name": "L", "type": "Lookup", "typeProperties": {"source": {"type": "DataWarehouseSource", "sqlReaderQuery": "SELECT TOP 1 [id] FROM [mart].[sales] ORDER BY [id]",
            "datasetSettings": {"type": "DataWarehouseTable", "typeProperties": {"schema": "mart", "table": "sales"},
                                "linkedService": {"properties": {"type": "DataWarehouse", "typeProperties": {"workspaceId": WS, "artifactId": WAREHOUSE}}}}}}},
        {"name": "S", "type": "Script", "typeProperties": {"scripts": [{"type": "NonQuery", "text": "UPDATE mart.sales SET amount = 0 WHERE id IN (SELECT id FROM mart.bad)"}, {"text": "@concat('x')"}]}},
    ]}}
    f = fabric(d)
    reads = edges(f, "READS_FROM")
    assert "READS_FROM activity:Sales Load/L -> table:mart.sales [derived]" in reads
    assert any(r.startswith("READS_FROM activity:Sales Load/S -> table:mart.bad") for r in reads)


def test_fabric_unsupported_and_dynamic_things_are_reported() -> None:
    d = {"properties": {"activities": [
        {"name": "Dbx", "type": "DatabricksNotebook", "typeProperties": {"notebookPath": "@pipeline().parameters.p"}},
        {"name": "Proc", "type": "SqlServerStoredProcedure", "typeProperties": {"storedProcedureName": "dbo.refresh"}},
        {"name": "Hook", "type": "WebHook", "typeProperties": {"url": "@concat('https://', pipeline().parameters.h)"}},
    ]}}
    f = fabric(d)
    assert edges(f, "EXECUTES") == {"EXECUTES activity:Sales Load/Proc -> external:stored_procedure:dbo.refresh [derived]"}
    assert sorted(gaps(f)) == [("dynamic_reference", "Hook"), ("not_inspected", "Dbx"), ("not_inspected", "Proc")]


@pytest.mark.parametrize("defn", [[], "text", 5, {}, {"properties": {}}, {"properties": {"activities": "x"}}])
def test_fabric_non_pipeline_documents_give_a_parse_error_or_nothing(defn: Any) -> None:
    f = fabric(defn)
    assert not edges(f, "READS_FROM", "WRITES_TO", "EXECUTES")
    assert f.gaps == [] or gap_kinds(f) == {"parse_error"}


def test_fabric_top_level_activities_array_is_accepted() -> None:
    d = {"activities": [{"name": "W", "type": "Wait", "typeProperties": {}}]}
    assert asset_labels(fabric(d), "activity") == {"activity:Sales Load/W"}


def test_fabric_analysis_does_not_mutate_the_definition() -> None:
    d = load_json("pipelines", "fabric_sales_load.json")
    before = copy.deepcopy(d)
    fabric(d)
    assert d == before
