# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""MCP prompts: ready-made starting points that tell an agent how to use the tools well."""

from __future__ import annotations

from typing import Any

from olcf.mcp.protocol import INVALID_PARAMS, RpcError
from olcf.mcp.tools import Env

PROMPTS: list[dict[str, Any]] = [
    {"name": "analyze_table", "title": "Analyze a table", "description": "Profile one table: structure, quality checks, distributions and notable findings.",
     "arguments": [{"name": "lakehouse", "description": "Lakehouse name or id", "required": True}, {"name": "table", "description": "Table name", "required": True},
                   {"name": "focus", "description": "Optional question or aspect to focus on", "required": False}]},
    {"name": "explore_lakehouse", "title": "Explore a lakehouse", "description": "Get oriented in a lakehouse: what data exists, how it relates, and what questions it can answer.",
     "arguments": [{"name": "lakehouse", "description": "Lakehouse name or id", "required": True}]},
]


def _msg(text: str) -> dict[str, Any]:
    return {"role": "user", "content": {"type": "text", "text": text}}


def get_prompt(env: Env, name: str, args: dict[str, Any]) -> dict[str, Any]:
    spec = next((p for p in PROMPTS if p["name"] == name), None)
    if spec is None:
        raise RpcError(INVALID_PARAMS, f"unknown prompt '{name}'", {"prompts": [p["name"] for p in PROMPTS]})
    missing = [a["name"] for a in spec["arguments"] if a["required"] and not args.get(a["name"])]
    if missing:
        raise RpcError(INVALID_PARAMS, f"missing argument(s): {', '.join(missing)}")
    extra = set(args) - {a["name"] for a in spec["arguments"]}
    if extra:
        raise RpcError(INVALID_PARAMS, f"unknown argument(s): {', '.join(sorted(extra))}")
    lh = env.lakehouse(str(args["lakehouse"]))      # authorises and fails with a clear message before any text is produced
    if name == "analyze_table":
        table = str(args["table"])
        entry = env.table_entry(lh, table)
        focus = f"\n\nFocus: {str(args['focus'])[:500]}" if args.get("focus") else ""
        ref = f"{lh.name}.{entry['name']}"
        text = (f"Analyze the table `{ref}` (workspace `{lh.workspace_name}`).{focus}\n\n"
                f"1. Call `describe_table` (lakehouse=`{lh.name}`, table=`{entry['name']}`) for columns, types, row count and descriptions.\n"
                f"2. Call `sample_rows` to see real values.\n"
                f"3. Use `run_sql` (read-only DuckDB, tables are referenced as `{ref}`) to check: row count, NULL rate per column, duplicate keys, min/max/distinct counts, "
                f"top values of low-cardinality columns, and time coverage if there is a date column. Aggregate in SQL; never pull whole tables.\n"
                f"4. Call `get_lineage` to see where the table comes from and what depends on it.\n"
                f"5. Summarise: what the table represents, data quality issues (with the SQL that proves each), and 3 questions it could answer well.\n\n"
                f"Rules: results are capped, so check the `truncated` flag; pass values as `:name` parameters instead of pasting them into SQL.")
        return {"description": f"Analyze {ref}", "messages": [_msg(text)]}
    text = (f"Explore the lakehouse `{lh.name}` (workspace `{lh.workspace_name}`).\n\n"
            f"1. Read the resource `olcf://{lh.workspace_name.replace(' ', '%20')}/{lh.name}/data-dictionary` or call `list_tables` (include_columns=true).\n"
            f"2. Group the tables by role (raw, dimension, fact, aggregate/gold) and infer the join keys from column names; confirm with `run_sql` joins on a few rows.\n"
            f"3. Call `list_reports` and `search_catalog` to find existing reports and saved queries worth reusing.\n"
            f"4. Describe the data model in a short diagram (text), list the 5 most useful business questions it can answer, and give one verified SQL query for each.")
    return {"description": f"Explore {lh.name}", "messages": [_msg(text)]}
