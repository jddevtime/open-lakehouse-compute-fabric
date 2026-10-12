# Connect an AI agent to your lakehouse (MCP)

OLCF exposes a **Model Context Protocol (MCP)** endpoint at `/mcp`. An agent such as Claude Desktop can browse the lakehouse catalog, read table schemas and run
**read-only SQL** against the data the person who owns the token is allowed to see. Nothing is copied out of the platform except the (capped) query results.

* Transport: Streamable HTTP (`POST /mcp`, JSON or Server-Sent Events). Protocol versions `2025-06-18` and `2025-03-26`.
* Authentication: a platform token (`Authorization: Bearer ...`) with the `mcp` scope, optionally `mcp:write`.
* Authorisation: every tool call is checked against the token owner's roles on the **stored** workspace/lakehouse. An agent can never see more than its owner.

## 1. Try it with the sample data

```bash
curl -s -X POST https://olcf.example.com/v1/sample/install -H "Authorization: Bearer $OLCF_TOKEN" -H 'Content-Type: application/json' -d '{}'
```

This creates the workspace **Sample Retail** with the lakehouse `retail_lakehouse` (about 80 000 order lines), saved queries, a report and two notebooks.
Then ask the agent: *"Which product category grew fastest between 2024 and 2025?"*

## 2. Create a token

The owner needs a role with `mcp.use` (Contributor, WorkspaceAdmin, service principals; **Viewers cannot use MCP**). Create the token with the API (the console offers the same under your profile):

```bash
curl -s -X POST https://olcf.example.com/v1/identity/tokens -H "Authorization: Bearer $OLCF_TOKEN" -H 'Content-Type: application/json' \
     -d '{"name": "claude-desktop", "scopes": ["mcp"], "ttl_days": 30}'
```

The response contains the token once. Scopes:

| scope | what the token can do through MCP |
|---|---|
| `mcp` | read tools: catalog, schemas, samples, statistics, `run_sql`, reports, lineage, job status, the `intel_*` asset-graph tools and the `governance_*` glossary, ownership, classification and quality tools (read only: no sample endpoint, no writes) |
| `mcp:write` | everything in `mcp` plus `submit_notebook_job` and `cancel_job` |

A token with only these scopes cannot call any other OLCF API. Prefer a dedicated token per agent, a short lifetime, and a service principal bound to the workspaces the agent should see.

## 3. Claude Desktop

Claude Desktop starts MCP servers as local processes, so OLCF ships a small **stdio bridge** (`python -m olcf.mcp.stdio`) that forwards to the HTTP endpoint. It needs the
`olcf` package (the bridge itself only uses `httpx`) in a Python environment on your computer. Add to `claude_desktop_config.json`
(macOS: `~/Library/Application Support/Claude/`, Windows: `%APPDATA%\Claude\`):

```json
{
  "mcpServers": {
    "olcf": {
      "command": "python",
      "args": ["-m", "olcf.mcp.stdio", "--url", "https://olcf.example.com/mcp"],
      "env": { "OLCF_MCP_TOKEN": "olcf_pat_..." }
    }
  }
}
```

`command` must point at the interpreter that has OLCF installed (for example `/path/to/venv/bin/python`). Restart Claude Desktop; the tools appear under the connector icon.

Options of the bridge: `--token-file FILE` (read the token from a file instead of the environment), `--ca-file PEM` (trust a private CA), `--timeout SECONDS`
(default 300), `--insecure` (skip certificate verification, lab use only), `--verbose` (diagnostics on stderr). The token is never printed. The bridge re-opens the session
transparently if the server restarted, and sends `DELETE /mcp` when the client closes.

A running server shows ready-made snippets at `GET /v1/mcp/info` (with your public URL filled in).

### From the command line

`olcf mcp` does the token and configuration steps for you:

```bash
olcf mcp config --client claude-code            # creates a narrowly scoped token and prints the configuration
olcf mcp config --client claude-desktop --write --days 30     # --write adds the mcp:write scope
olcf mcp info                                   # endpoint, protocol versions, the tools, recent calls
```

`--client` is `claude-desktop`, `claude-code` or `generic`. For `claude-code` the output includes the equivalent `claude mcp add --transport http olcf <url>/mcp --header "Authorization: Bearer ..."` command. The token is shown once and listed under `olcf token list` (revoke it with `olcf token revoke ID`). `olcf mcp stdio` is the stdio bridge used in the Claude Desktop configuration above.

## 4. Clients that speak Streamable HTTP natively

```json
{ "type": "http", "url": "https://olcf.example.com/mcp", "headers": { "Authorization": "Bearer olcf_pat_..." } }
```

With the official Python SDK (`mcp` 2.x, the same calls the OLCF test-suite makes against a live server):

```python
import asyncio
from mcp.client.client import Client
from mcp.client.streamable_http import streamable_http_client
from mcp.shared._httpx_utils import create_mcp_http_client


async def main(token: str) -> None:
    async with create_mcp_http_client(headers={"Authorization": f"Bearer {token}"}) as http:
        async with Client(streamable_http_client("https://olcf.example.com/mcp", http_client=http)) as client:
            tools = await client.list_tools()
            result = await client.call_tool("run_sql", {"sql": "SELECT count(*) AS n FROM retail_lakehouse.fact_sales", "workspace": "Sample Retail"})
            print([t.name for t in tools.tools], result.content[0].text)


asyncio.run(main("olcf_pat_..."))
```

## 5. What the agent can do

| tool | purpose |
|---|---|
| `list_workspaces`, `list_lakehouses`, `list_tables` | discover what exists (with descriptions, row counts, partitioning) |
| `describe_table` | columns with types and descriptions, partitioning, statistics, tags, lineage counts |
| `sample_rows`, `table_stats` | look at the data (max 100 rows) and metadata-derived statistics (no scan) |
| `run_sql` | one read-only statement on DuckDB: `SELECT`, `WITH`, `DESCRIBE`, `SUMMARIZE`. Tables are `<lakehouse>.<table>`; files under `Files/` via `read_csv('Files/...')`; typed `:name` parameters |
| `explain_query` | the plan and the tables a query would touch, without running it |
| `search_catalog`, `get_lineage` | find tables/columns/queries/reports/notebooks; upstream and downstream lineage, down to column level where known |
| `list_reports`, `run_report` | run a dashboard with its filters and get every tile's data (a failing tile reports its own error) |
| `submit_notebook_job`, `get_job`, `cancel_job` | run a notebook on the compute fabric and follow it (`submit_notebook_job` and `cancel_job` need `mcp:write`) |

### Intelligence tools: where data comes from and what depends on it

When the Intelligence module is installed, tokens with the `mcp` scope also get these **read-only** tools over the asset graph (they need `intelligence.read`; they are not listed when the module is off).
They take an asset **id** (`ast_...`), a name, a path (`lakehouse.table`) or `table.column`; an unknown or ambiguous name comes back as an error that lists candidates, and an asset outside your
workspaces answers exactly like one that does not exist.

| tool | the question it answers |
|---|---|
| `intel_search_assets` | *Which tables hold customer data? Is there a Sales report?* Ranked search by name, path, description, tag or alias, optionally filtered by domain, classification, certification or owner |
| `intel_get_asset` | *What is this asset?* Type, workspace, description, schema (names and types only), size, how connected it is, open gaps, governance facts |
| `intel_lineage` | *What feeds this table, and what reads it?* Table- or column-level graph, up and/or down, with the confidence of each link and its evidence |
| `intel_where_does_this_come_from` | *Where does Revenue originate?* Upstream roots and every path from them, source first, with the confidence of each path |
| `intel_impact_analysis` | *What breaks if I change this column?* Downstream tables, models, measures and reports, nearest first; for a column also the columns computed from it |
| `intel_staleness` | *Why is this dashboard out of date?* Last observed write and run outcome, and the most upstream stale producer |
| `intel_documentation` | *Explain the customers table.* Generated Markdown documentation |
| `intel_coverage_gaps` | *What can't the lineage see?* Per source coverage and the gaps (permission denied, dynamic table names ...) each with its fix |
| `intel_summary` | *How much of the estate is mapped, and how reliable is it?* |
| `intel_unused_assets` | *Which tables can we retire?* Tables nothing was observed reading |

Answers are built for a model: compact references, a one-sentence `summary`, and for every lineage statement a **confidence** (`verified` = observed at run time, `derived` = parsed from code or read from a stored definition,
metadata or catalog record, `inferred` = matched by name or an assumed default, `unknown`, which is not emitted by any analyzer or connector in this version), an **evidence summary** (how many observations, when last seen) and the **gaps** that make the picture incomplete. `complete: false`
means the answer is a lower bound. Long lists are cut to the `max_*` argument of the tool (a hard ceiling applies) and say so with `truncated: true` and `omitted` counts, on top of the platform-wide
`mcp.max_result_bytes`.

What the tools deliberately cannot do (ADR 0014): there is **no tool for the sample endpoint** (sample rows are data; use `run_sql` under its guard), none that changes an asset, a source, a scan or a
sampling policy, and the min/max statistics that scans keep per column are not returned. The REST write and sample routes refuse an `mcp` token even when its owner is an administrator.

### Governance tools: what it means, who owns it, where sensitive data ends up

When the governance package is installed too, the same tokens get four more **read-only** tools (they need `intelligence.read`; they are not listed while governance is off). They use the asset
addressing and the visibility rules of the Intelligence tools above: an asset outside your workspaces does not exist for you, and evidence that passes through one reads `(hidden)`.

| tool | the question it answers |
|---|---|
| `governance_glossary_search` | *What does "active customer" mean? Which tables implement Revenue?* Ranked search of terms, synonyms and abbreviations with the definition, status, owners and some linked assets |
| `governance_asset` | *Who owns this and is it certified? Does it contain PII?* Domain, data products, glossary terms, effective (inherited) owners, direct / inherited / suggested labels with their evidence, certification, quality score, policy violations |
| `governance_exposure` | *Which reports show PII? Where does credit card data end up?* Reports, visuals, models, tables and products downstream of a label, each proven (seen lineage) or possible (only the column's table feeds it), with the lineage path |
| `governance_quality` | *How good is the quality of the sales table?* The 0-100 score with the points lost per component and why, for an asset or a data product |

Exposure follows lineage at the default minimum confidence `derived`; a report that is only reachable through `inferred` links is listed with `min_confidence: "inferred"` and marked as such. Missing lineage
is never proof that data does not flow somewhere: read `notes` and `complete`. Nothing here approves, certifies, classifies or evaluates anything, and no tool returns sample values: the governance
write routes of the REST API refuse an `mcp` token just like the sample endpoint (ADR 0014).

Resources: `olcf://<workspace>/<lakehouse>/Tables/<table>` (JSON schema) and `olcf://<workspace>/<lakehouse>/data-dictionary` (markdown). Prompts: `explore_lakehouse` and `analyze_table`.
Tools carry MCP annotations (`readOnlyHint`, `destructiveHint`) so clients can ask for confirmation before the write tools.

Table references accept the lakehouse's name or id; pass `workspace` when the same lakehouse name exists in more than one workspace.

## 6. Limits and safety

| | default | setting |
|---|---|---|
| requests per minute per user | 120 | `mcp.rate_limit_per_minute` |
| rows returned by `run_sql` | 200 (up to 5 000 if asked) | `mcp.max_rows` |
| bytes of one tool result (rows are cut to fit; `truncated` says so) | 256 KiB | `mcp.max_result_bytes` |
| `run_sql` timeout | 30 s | `mcp.sql_timeout_seconds` |
| request size | 1 MiB | `mcp.max_request_bytes` |
| idle session expiry | 60 min | `mcp.session_idle_minutes` |
| call audit retention | 90 days | `mcp.calls_retention_days` |

* **Read-only**: `run_sql` rejects every statement that is not a read, plus file-system, network and extension functions (`COPY`, `ATTACH`, `INSTALL`, `read_text`, `glob`, `getenv`, ...). The embedded engine runs
  with external access disabled and a memory limit; data is handed to it as Arrow objects, never as paths or credentials.
* **No secrets**: storage credentials and connection secrets are never returned by any tool, resource or error message. Arguments are redacted before they are written to the call log.
* **Missing equals invisible**: an object you cannot read answers exactly like one that does not exist.
* **DNS-rebinding protection**: on a loopback-only server the `Host` and `Origin` headers must be known. Set `server.public_url`, or add names to `mcp.allowed_hosts` / `mcp.allowed_origins`.
* **Audit**: each call is recorded (tool, redacted arguments, duration, outcome, row count) and visible to the owner at `GET /v1/mcp/calls` (administrators see everyone's). Revoke a token in the UI or with `DELETE /v1/identity/tokens/{id}`; its sessions stop at once.
* Turn the endpoint off with the setting `mcp.enabled = false`. The health check `mcp` (Admin, Health, deep) runs initialize, tools/list, a tool call and resources/list in process.

## 7. Troubleshooting

| symptom | cause and fix |
|---|---|
| `401` with `WWW-Authenticate: Bearer` | missing, expired or revoked token |
| `403` "permission 'mcp.use' is required" | the token owner's role has no `mcp.use` (Viewer) or the token lacks the `mcp` scope |
| `403` about Host/Origin | set `server.public_url` or `mcp.allowed_hosts` |
| `429` | rate limit; the response says when to retry |
| a table "was not found" | it does not exist **or** you cannot read it; run `list_tables` to see what is visible |
| "read-only" error from `run_sql` | the engine is read-only by design; changes go through notebooks (`submit_notebook_job`) |
| tool results end with `truncated: true` | add filters or aggregation, or use `LIMIT`; large extracts belong in `/v1/sql/export`, not in an agent conversation |
