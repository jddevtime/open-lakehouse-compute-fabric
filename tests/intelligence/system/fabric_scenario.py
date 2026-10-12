# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""A small Fabric tenant for ``FakeFabricWorld``: a medallion lakehouse, the notebooks that move data bronze -> silver -> gold, the pipeline that runs them, a Direct Lake
semantic model over silver and gold, and a report on the model - the content a real tenant has, in the formats the Fabric REST API returns.

``populate(world)`` registers everything through the emulation's public ``add_*`` helpers and its ``pbi_*`` / ``workspace_denied`` switches; nothing about the analyzers or the
connector is replaced. The emulation reproduces documented Fabric behaviour.
"""

from __future__ import annotations

import json
from typing import Any

from tests.fabric.fabric_fakes import (
    FABRIC_WS,
    LAKEHOUSE,
    OTHER_FABRIC_WS,
    FakeFabricWorld,
    scanner_workspace,
)

NB_BRONZE_SILVER = "11111111-aaaa-4aaa-8aaa-000000000001"
NB_SILVER_GOLD = "11111111-aaaa-4aaa-8aaa-000000000002"
PIPELINE = "11111111-aaaa-4aaa-8aaa-000000000003"
MODEL = "11111111-aaaa-4aaa-8aaa-000000000004"
REPORT = "11111111-aaaa-4aaa-8aaa-000000000005"
SECRET_LAKEHOUSE = "11111111-aaaa-4aaa-8aaa-000000000006"  # noqa: S105 - a GUID, not a secret
ENDPOINT = "x6eps4xrq2xudenlfv6naeo3i4-example.datawarehouse.fabric.microsoft.com"

BRONZE = [("order_id", "long"), ("customer_id", "long"), ("amount", "double"), ("ordered_at", "timestamp"), ("channel", "string")]
SILVER = [("order_id", "long"), ("customer_id", "long"), ("amount", "double"), ("ordered_date", "date")]
GOLD = [("day", "date"), ("revenue", "double"), ("orders", "long")]


def _nb(*cells: str, lakehouse: str = "lh_sales") -> str:
    return json.dumps({"nbformat": 4, "nbformat_minor": 5,
                       "metadata": {"language_info": {"name": "python"}, "kernel_info": {"name": "synapse_pyspark"},
                                    "dependencies": {"lakehouse": {"default_lakehouse": LAKEHOUSE, "default_lakehouse_name": lakehouse, "default_lakehouse_workspace_id": FABRIC_WS}}},
                       "cells": [{"cell_type": "code", "metadata": {}, "source": c.splitlines(keepends=True), "outputs": [], "execution_count": None} for c in cells]})


BRONZE_TO_SILVER = _nb(
    "from pyspark.sql import functions as F\n",
    'bronze = spark.read.table("bronze_orders")\n'
    'silver = (bronze.filter(F.col("amount") > 0)\n'
    '                .withColumn("ordered_date", F.to_date("ordered_at"))\n'
    '                .select("order_id", "customer_id", "amount", "ordered_date"))\n'
    'silver.write.mode("overwrite").saveAsTable("silver_orders")\n')

SILVER_TO_GOLD = _nb(
    'spark.sql("""\n'
    "INSERT OVERWRITE TABLE gold_revenue\n"
    "SELECT ordered_date AS day, SUM(amount) AS revenue, COUNT(DISTINCT order_id) AS orders\n"
    "FROM silver_orders GROUP BY ordered_date\n"
    '""")\n')

PIPELINE_CONTENT = json.dumps({"properties": {"activities": [
    {"name": "Bronze to silver", "type": "TridentNotebook", "dependsOn": [], "typeProperties": {"notebookId": NB_BRONZE_SILVER, "workspaceId": FABRIC_WS}},
    {"name": "Silver to gold", "type": "TridentNotebook", "dependsOn": [{"activity": "Bronze to silver", "dependencyConditions": ["Succeeded"]}],
     "typeProperties": {"notebookId": NB_SILVER_GOLD, "workspaceId": FABRIC_WS}}]}})

MODEL_PARTS: dict[str, str | bytes] = {
    ".platform": json.dumps({"metadata": {"type": "SemanticModel", "displayName": "Sales Model"}}),
    "definition/model.tmdl": "model Model\n\tculture: en-US\n\tdefaultPowerBIDataSourceVersion: powerBI_V3\n",
    "definition/expressions.tmdl": f'expression DatabaseQuery =\n\t\tlet\n\t\t    database = Sql.Database("{ENDPOINT}", "lh_sales")\n\t\tin\n\t\t    database\n',
    "definition/tables/Revenue.tmdl": ("table Revenue\n\tcolumn day\n\t\tdataType: dateTime\n\t\tsourceColumn: day\n\tcolumn revenue\n\t\tdataType: double\n\t\tsourceColumn: revenue\n"
                                       "\tcolumn orders\n\t\tdataType: int64\n\t\tsourceColumn: orders\n"
                                       "\tmeasure 'Total Revenue' = SUM(Revenue[revenue])\n"
                                       "\tmeasure 'Average Order Value' = DIVIDE([Total Revenue], SUM(Revenue[orders]))\n"
                                       "\tpartition Revenue = entity\n\t\tmode: directLake\n\t\tsource\n\t\t\tentityName: gold_revenue\n\t\t\tschemaName: dbo\n\t\t\texpressionSource: DatabaseQuery\n"),
    "definition/tables/Orders.tmdl": ("table Orders\n\tcolumn order_id\n\t\tdataType: int64\n\t\tsourceColumn: order_id\n\tcolumn amount\n\t\tdataType: double\n\t\tsourceColumn: amount\n"
                                      "\tcolumn ordered_date\n\t\tdataType: dateTime\n\t\tsourceColumn: ordered_date\n"
                                      "\tmeasure 'Order Lines' = COUNTROWS(Orders)\n"
                                      "\tpartition Orders = entity\n\t\tmode: directLake\n\t\tsource\n\t\t\tentityName: silver_orders\n\t\t\tschemaName: dbo\n\t\t\texpressionSource: DatabaseQuery\n"),
}


def _field(kind: str, table: str, name: str) -> dict[str, Any]:
    return {kind: {"Expression": {"SourceRef": {"Entity": table}}, "Property": name}}


def _visual(name: str, vtype: str, title: str, fields: list[tuple[str, str, str, str]]) -> str:
    return json.dumps({"name": name, "position": {"x": 0, "y": 0, "width": 200, "height": 100},
                       "visual": {"visualType": vtype,
                                  "query": {"queryState": {role: {"projections": [{"field": _field(kind, table, field), "queryRef": f"{table}.{field}"} for r, table, field, kind in fields if r == role]}
                                                           for role in dict.fromkeys(r for r, *_ in fields)}},
                                  "visualContainerObjects": {"title": [{"properties": {"text": {"expr": {"Literal": {"Value": f"'{title}'"}}}}}]}}})


REPORT_PARTS: dict[str, str | bytes] = {
    ".platform": json.dumps({"metadata": {"type": "Report", "displayName": "Sales Report"}}),
    "definition.pbir": json.dumps({"version": "4.0", "datasetReference": {"byConnection": {
        "connectionString": f"Data Source=powerbi://api.powerbi.com/v1.0/myorg/Sales;initial catalog=Sales Model;semanticmodelid={MODEL}", "pbiModelDatabaseName": MODEL,
        "connectionType": "pbiServiceXmlaStyleLive"}}}),
    "definition/version.json": json.dumps({"version": "2.0.0"}),
    "definition/report.json": json.dumps({"themeCollection": {"baseTheme": {"name": "CY24SU10"}}}),
    "definition/pages/pages.json": json.dumps({"pageOrder": ["p1"], "activePageName": "p1"}),
    "definition/pages/p1/page.json": json.dumps({"name": "p1", "displayName": "Overview", "displayOption": "FitToPage", "width": 1280, "height": 720}),
    "definition/pages/p1/visuals/rev/visual.json": _visual("rev", "card", "Revenue", [("Values", "Revenue", "Total Revenue", "Measure")]),
    "definition/pages/p1/visuals/trend/visual.json": _visual("trend", "lineChart", "Revenue by day", [("Category", "Revenue", "day", "Column"), ("Y", "Revenue", "Total Revenue", "Measure")]),
}


def populate(world: FakeFabricWorld, *, denied_workspace: bool = True) -> None:
    """The tenant: workspace *Sales* (readable) and, when ``denied_workspace``, workspace *Finance* the service principal may list but not read."""
    world.workspaces = [{"id": FABRIC_WS, "displayName": "Sales", "type": "Workspace"}, {"id": OTHER_FABRIC_WS, "displayName": "Finance", "type": "Workspace"}]
    world.add_item(FABRIC_WS, LAKEHOUSE, "lh_sales", "Lakehouse")
    world.add_item(FABRIC_WS, NB_BRONZE_SILVER, "nb_bronze_to_silver", "Notebook")
    world.add_item(FABRIC_WS, NB_SILVER_GOLD, "nb_silver_to_gold", "Notebook")
    world.add_notebook(FABRIC_WS, NB_BRONZE_SILVER, ipynb=BRONZE_TO_SILVER)
    world.add_notebook(FABRIC_WS, NB_SILVER_GOLD, ipynb=SILVER_TO_GOLD)
    world.add_item(FABRIC_WS, PIPELINE, "pl_daily_refresh", "DataPipeline", parts={".platform": "{}", "pipeline-content.json": PIPELINE_CONTENT})
    world.add_item(FABRIC_WS, MODEL, "Sales Model", "SemanticModel", parts=MODEL_PARTS)
    world.add_item(FABRIC_WS, REPORT, "Sales Report", "Report", parts=REPORT_PARTS)
    world.add_lakehouse_table(FABRIC_WS, LAKEHOUSE, "bronze_orders", schema=BRONZE, commits=2)
    world.add_lakehouse_table(FABRIC_WS, LAKEHOUSE, "silver_orders", schema=SILVER)
    world.add_lakehouse_table(FABRIC_WS, LAKEHOUSE, "gold_revenue", schema=GOLD)
    world.add_item(OTHER_FABRIC_WS, SECRET_LAKEHOUSE, "lh_finance", "Lakehouse")
    world.pbi_workspaces = {FABRIC_WS: scanner_workspace(FABRIC_WS, "Sales"), OTHER_FABRIC_WS: scanner_workspace(OTHER_FABRIC_WS, "Finance")}
    world.pbi_modified = {FABRIC_WS: "2999-01-01T00:00:00Z", OTHER_FABRIC_WS: "2000-01-01T00:00:00Z"}
    world.throttle_retry_after = "0"
    if denied_workspace:
        world.workspace_denied.add(OTHER_FABRIC_WS)
