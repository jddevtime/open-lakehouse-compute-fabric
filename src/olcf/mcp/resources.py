# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""MCP resources: ``olcf://`` table schemas (JSON) and a per-lakehouse markdown data dictionary.

URIs use the canonical OLCF form with percent-encoded names, e.g. ``olcf://Sample%20Retail/retail_lakehouse/Tables/fact_sales`` and
``olcf://Sample%20Retail/retail_lakehouse/data-dictionary``.  Listing is paginated; every read re-checks ``lakehouse.read`` on the stored workspace.
"""

from __future__ import annotations

import base64
import json
from typing import Any
from urllib.parse import quote, unquote

from olcf.core.errors import NotFound
from olcf.mcp.protocol import INVALID_PARAMS, RESOURCE_NOT_FOUND, RpcError
from olcf.mcp.tools import Env

PAGE = 100
TEMPLATES = [
    {"uriTemplate": "olcf://{workspace}/{lakehouse}/Tables/{table}", "name": "table-schema", "title": "Table schema",
     "description": "Columns, types, partitioning, statistics and descriptions of one lakehouse table (JSON).", "mimeType": "application/json"},
    {"uriTemplate": "olcf://{workspace}/{lakehouse}/data-dictionary", "name": "data-dictionary", "title": "Data dictionary",
     "description": "Markdown data dictionary of every table in a lakehouse: purpose, columns, types and descriptions.", "mimeType": "text/markdown"},
]


def table_uri(workspace: str, lakehouse: str, table: str) -> str:
    return f"olcf://{quote(workspace, safe='')}/{quote(lakehouse, safe='')}/Tables/{quote(table, safe='')}"


def dictionary_uri(workspace: str, lakehouse: str) -> str:
    return f"olcf://{quote(workspace, safe='')}/{quote(lakehouse, safe='')}/data-dictionary"


def _entries(env: Env) -> list[dict[str, Any]]:
    """Flat list of every resource the caller may see (cheap: names only)."""
    out: list[dict[str, Any]] = []
    for ws in env.visible():
        for lh in env.storage.list_lakehouses(ws["id"]):
            if not env.principal.can("lakehouse.read", workspace_id=lh.workspace_id, item_id=lh.id):
                continue
            out.append({"uri": dictionary_uri(lh.workspace_name, lh.name), "name": f"{lh.workspace_name}/{lh.name} data dictionary", "title": f"{lh.name} data dictionary",
                        "description": f"Markdown data dictionary of lakehouse {lh.name}", "mimeType": "text/markdown"})
            rows, _ = env.catalog.list_tables(lh.id, limit=500)
            if not rows:
                env.catalog.sync_lakehouse(lh)
                rows, _ = env.catalog.list_tables(lh.id, limit=500)
            for r in rows:
                out.append({"uri": table_uri(lh.workspace_name, lh.name, r["name"]), "name": f"{lh.name}.{r['name']}", "title": f"{lh.name}.{r['name']}",
                            "description": (r.get("description") or f"{r['format']} table {r['name']} in {lh.name}")[:300], "mimeType": "application/json"})
    return out


def list_resources(env: Env, cursor: str | None) -> dict[str, Any]:
    try:
        start = int(base64.urlsafe_b64decode(cursor.encode()).decode()) if cursor else 0
        if start < 0:
            raise ValueError
    except (ValueError, UnicodeError):
        raise RpcError(INVALID_PARAMS, "invalid cursor") from None
    allr = _entries(env)
    page = allr[start:start + PAGE]
    out: dict[str, Any] = {"resources": page}
    if start + PAGE < len(allr):
        out["nextCursor"] = base64.urlsafe_b64encode(str(start + PAGE).encode()).decode()
    return out


def _parse(uri: str) -> tuple[str, str, str, str | None]:
    if not uri.startswith("olcf://"):
        raise RpcError(RESOURCE_NOT_FOUND, f"unsupported resource uri '{uri[:200]}'", {"uri": uri[:200]})
    parts = [unquote(p) for p in uri[len("olcf://"):].split("/")]
    if len(parts) == 3 and parts[2] == "data-dictionary":
        return parts[0], parts[1], "dictionary", None
    if len(parts) == 4 and parts[2] == "Tables" and parts[3]:
        return parts[0], parts[1], "table", parts[3]
    raise RpcError(RESOURCE_NOT_FOUND, f"unknown resource '{uri[:200]}'", {"uri": uri[:200]})


def read_resource(env: Env, uri: str) -> dict[str, Any]:
    ws, lhn, kind, table = _parse(uri)
    try:
        lh = env.lakehouse(f"{ws}/{lhn}")
        if kind == "table":
            assert table is not None
            entry = env.table_entry(lh, table)
            full = env.catalog.get(env.principal, entry["id"])
            body = {"reference": f"{lh.name}.{full['name']}", "workspace": lh.workspace_name, "lakehouse": lh.name, "format": full["format"], "description": full.get("description") or "",
                    "tags": full.get("tags") or [], "partition_columns": full.get("partition_columns") or [],
                    "columns": [{"name": c["name"], "type": c["type"], "nullable": c.get("nullable", True), "description": c.get("description") or ""} for c in full["columns"]],
                    "stats": {k: (full.get("stats") or {}).get(k) for k in ("row_count", "size_bytes", "num_files", "version")}}
            return {"contents": [{"uri": uri, "mimeType": "application/json", "text": json.dumps(body, default=str, ensure_ascii=False, indent=2)}]}
        return {"contents": [{"uri": uri, "mimeType": "text/markdown", "text": data_dictionary(env, lh)}]}
    except NotFound as exc:
        raise RpcError(RESOURCE_NOT_FOUND, exc.message, {"uri": uri[:200]}) from None


def data_dictionary(env: Env, lh: Any) -> str:
    rows, _ = env.catalog.list_tables(lh.id, limit=500, columns=True)
    if not rows:
        env.catalog.sync_lakehouse(lh)
        rows, _ = env.catalog.list_tables(lh.id, limit=500, columns=True)
    lines = [f"# Data dictionary: {lh.name}", "", f"Workspace: `{lh.workspace_name}`. Query tables as `{lh.name}.<table>`.", ""]
    for r in sorted(rows, key=lambda r: r["name"]):
        st = env.catalog.stats(env.principal, r["id"])
        meta = [r["format"]] + ([f"{st['row_count']:,} rows"] if st.get("row_count") is not None else []) + ([f"partitioned by {', '.join(r['partition_columns'])}"] if r.get("partition_columns") else [])
        lines += [f"## {r['name']}", "", f"_{'; '.join(meta)}_", ""]
        if r.get("description"):
            lines += [r["description"], ""]
        if r.get("tags"):
            lines += ["Tags: " + ", ".join(f"`{t}`" for t in r["tags"]), ""]
        lines += ["| column | type | nullable | description |", "|---|---|---|---|"]
        for c in r.get("columns") or []:
            desc = (c.get("description") or "").replace("|", "\\|").replace("\n", " ")
            lines.append(f"| `{c['name']}` | {c['type']} | {'yes' if c.get('nullable', True) else 'no'} | {desc} |")
        lines.append("")
    if not rows:
        lines.append("_This lakehouse has no tables yet._")
    return "\n".join(lines)
