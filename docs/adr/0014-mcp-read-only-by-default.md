# ADR 0014: The MCP endpoint is read-only unless the token says otherwise

Status: Accepted. Extended by [ADR 0017](0017-intelligence-subsystem.md): the endpoint now exposes 29 tools; the figures below are the decision as taken at the time.

## Context

AI agents will be pointed at the platform to browse tables and answer questions. An agent acts on behalf of a user, may be fed untrusted text (prompt injection), and should never be able to do more than a person would knowingly allow.

## Decision

* The endpoint is `/mcp` (Streamable HTTP; protocol versions 2025-06-18 and 2025-03-26) with a stdio bridge for clients that start local processes. It exposes 15 tools, resources and prompts.
* A request needs a platform token with the **`mcp`** scope, owned by an account whose role has `mcp.use` (Viewers do not). The `mcp` scope allows the 13 read tools: catalog, schemas, samples, statistics, SQL, reports, lineage, job status. SQL goes through the read-only guard ([ADR 0006](0006-read-only-sql-engines.md)).
* Two tools change state, `submit_notebook_job` and `cancel_job`, and they require the additional **`mcp:write`** scope and the permissions to submit or cancel jobs. Submission accepts an idempotency key.
* Every call is authorised against the *stored* workspace of whatever it touches, so an agent never gets more than the user or token behind it; every call is audited. Results are small and typed: row counts are capped and wide cells truncated.
* One transport-independent service handles every call, and the deep health check goes through it, so what the check proves is what an agent gets.

## Consequences

* A token without `mcp:write` cannot start work even if its owner could. Token scopes limit what a token can do regardless of the owner's roles.
* The bridge forwards the token; it does not hold a different identity.
* There is no tool to write data, change configuration or read secrets.

## Later additions

[ADR 0017](0017-intelligence-subsystem.md) adds 14 read-only tools to the `mcp` scope (10 `intel_*` and 4 `governance_*`; 27 readable tools in all, plus the two `mcp:write` job tools, 29 in total), under the same rules: no tool changes state, all need `intelligence.read`, the owner's visibility applies, and there is no tool for sample rows. The read-only default of this ADR is unchanged.

## Alternatives considered

One scope for everything (rejected: an agent that only needs to read would be given the ability to run jobs).

## Verified by

`src/olcf/mcp/`, `tests/data/test_mcp.py`, `tests/data/test_mcp_client.py`; the [MCP guide](../guides/mcp.md).
