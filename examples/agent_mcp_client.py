#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Talk to OLCF as an AI agent would: over MCP, with the official ``mcp`` client.

Lists the tools the server offers, finds a lakehouse and a table, describes the table, runs a read-only SQL query, and prints what it found.

    olcf token create agent --scope mcp          # prints the token once
    export OLCF_MCP_TOKEN=olcf_pat_...
    python examples/agent_mcp_client.py                                   # explores the first lakehouse it can see
    python examples/agent_mcp_client.py --lakehouse sales --table orders --sql "select region, count(*) n from sales.orders group by 1"
    python examples/agent_mcp_client.py --json                            # machine-readable output

``--url`` (or ``OLCF_MCP_URL`` / ``OLCF_URL``) selects the server; ``--ca-bundle`` trusts a private CA.  The agent can only see what its
token's owner can see, and ``run_sql`` is read-only on the server side.  Requires ``pip install mcp``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any

from _connect import endpoint, open_session, result_json, token_from_env


async def explore(session: Any, *, lakehouse: str | None, table: str | None, sql: str | None, max_rows: int) -> dict[str, Any]:
    """Run the discovery walk and return everything it learned (no printing here, so tests can assert on it)."""
    found: dict[str, Any] = {}
    tools = await session.list_tools()
    found["tools"] = [{"name": t.name, "description": (t.description or "").split("\n")[0]} for t in tools.tools]
    names = {t["name"] for t in found["tools"]}
    for needed in ("list_lakehouses", "list_tables", "describe_table", "run_sql"):
        if needed not in names:
            raise RuntimeError(f"the server does not offer the '{needed}' tool (offers: {', '.join(sorted(names))})")

    lakehouses = result_json(await session.call_tool("list_lakehouses", {}))
    found["lakehouses"] = lakehouses.get("lakehouses", lakehouses) if isinstance(lakehouses, dict) else lakehouses
    if lakehouse is None:
        if not found["lakehouses"]:
            raise RuntimeError("this token cannot see any lakehouse (create one, or run `olcf sample install`)")
        lakehouse = found["lakehouses"][0]["name"]
    found["lakehouse"] = lakehouse

    tables = result_json(await session.call_tool("list_tables", {"lakehouse": lakehouse}))
    found["tables"] = tables.get("tables", tables) if isinstance(tables, dict) else tables
    if table is None and found["tables"]:
        table = found["tables"][0]["name"]
    found["table"] = table

    if table:
        found["description"] = result_json(await session.call_tool("describe_table", {"lakehouse": lakehouse, "table": table}))
        statement = sql or f"SELECT * FROM {lakehouse}.{table} LIMIT 5"  # noqa: S608 - names come from the server listing, the statement is read-only and checked server-side
    else:
        statement = sql or "SELECT 42 AS answer"
    found["sql"] = statement
    found["result"] = result_json(await session.call_tool("run_sql", {"sql": statement, "lakehouse": lakehouse, "max_rows": max_rows}))
    return found


def render(found: dict[str, Any]) -> str:
    out = [f"tools ({len(found['tools'])}): " + ", ".join(t["name"] for t in found["tools"]), f"lakehouse: {found['lakehouse']}"]
    out.append("tables: " + (", ".join(t["name"] for t in found["tables"]) or "(none)"))
    desc = found.get("description")
    if desc:
        out.append(f"table {found['table']}: " + ", ".join(f"{c['name']} {c['type']}" for c in desc.get("columns", [])))
    result = found["result"]
    cols = [c["name"] if isinstance(c, dict) else c for c in result.get("columns", [])]
    out.append(f"sql: {found['sql']}")
    out.append("  " + " | ".join(cols))
    for row in result.get("rows", []):
        out.append("  " + " | ".join(str(v) for v in row))
    if result.get("truncated"):
        out.append("  (truncated)")
    return "\n".join(out)


async def amain(args: argparse.Namespace) -> int:
    async with open_session(endpoint(args.url), token_from_env(), ca_bundle=args.ca_bundle) as session:
        found = await explore(session, lakehouse=args.lakehouse, table=args.table, sql=args.sql, max_rows=args.max_rows)
    print(json.dumps(found, indent=2, default=str) if args.json else render(found))
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--url", help="MCP endpoint (default: $OLCF_MCP_URL, else $OLCF_URL/mcp, else http://127.0.0.1:8470/mcp)")
    ap.add_argument("--ca-bundle", help="PEM file with the CA that signed the server certificate")
    ap.add_argument("--lakehouse")
    ap.add_argument("--table")
    ap.add_argument("--sql", help="read-only SQL to run (default: a small SELECT on the table)")
    ap.add_argument("--max-rows", type=int, default=20)
    ap.add_argument("--json", action="store_true", help="print the whole walk as JSON")
    args = ap.parse_args(argv)
    try:
        return asyncio.run(amain(args))
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
