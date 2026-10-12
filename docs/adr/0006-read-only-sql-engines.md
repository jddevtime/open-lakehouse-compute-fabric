# ADR 0006: SQL is read-only: hardened DuckDB, Spark by delegation

Status: Accepted

## Context

Users, dashboards and AI agents run SQL against lakehouses. The SQL service must not become a way to read files it should not, reach the network, or modify data outside the controlled paths for writes.

## Decision

* SQL submitted through the SQL service (REST, console, MCP) is parsed with sqlglot and classified; anything that is not a pure read is rejected with an actionable message (`olcf.sql.guard`). Parameters are bound as typed literal nodes in the syntax tree, never by string concatenation.
* The embedded engine is DuckDB, hardened twice: the guard above, and the connection itself (a fresh in-memory connection per query, external access disabled, configuration locked). Lakehouse tables and files are registered with DuckDB as Arrow objects under private names, so DuckDB never sees a path or a credential. A parser gap therefore cannot become file-system or network access.
* Spark SQL is a second engine that delegates to the compute service's pooled Spark session on a worker; if no worker is available the error says how to enable it.
* Both engines yield a stream of Arrow record batches, so row caps, JSON conversion and export share one path.
* Writes to tables are done by notebooks and jobs on workers, not by the SQL service.

## Consequences

* The SQL service is safe to expose to read-only roles and to agents. It is not a general SQL front end: DDL and DML are not available through it.
* The DuckDB engine runs in the control plane process, so its resource use is bounded by row caps and statement timeouts (`query_timeout`), not isolated by a process boundary. Heavy queries should use Spark.

## Alternatives considered

Running every query on Spark (rejected: JVM start for a laptop-sized query); trusting a keyword filter (rejected: a parser gap would be fatal, hence the second layer in the connection).

## Verified by

`src/olcf/sql/guard.py`, `src/olcf/sql/engines.py`, `tests/data/test_sql_guard.py`, `tests/data/test_sql_service.py`.
