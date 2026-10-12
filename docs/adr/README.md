# Architecture decision records

An ADR records one decision that shapes the system, the reasons for it, and what it costs. They are written for the next maintainer who wants to change the decision: read the *Consequences* and *Verified by* sections before you do. A decision is never edited away; if it changes, a new ADR supersedes the old one and the old one's status says so.

Format: status, context, decision, consequences, alternatives considered, and where in the code and the tests the decision can be checked. All statements were checked against the code at version 0.1.0.

| ADR | Decision | Status |
|---|---|---|
| [0001](0001-fabric-as-an-adapter.md) | Microsoft Fabric is an adapter, not a re-implementation | Accepted |
| [0002](0002-database-as-state-and-leader-lease.md) | The database holds all state; singletons run on a lease holder | Accepted |
| [0003](0003-outbound-worker-connections.md) | Workers connect outward over one WebSocket | Accepted |
| [0004](0004-signed-immutable-runtimes.md) | Spark runtimes are signed, hash-pinned and immutable | Accepted |
| [0005](0005-execution-capsule.md) | Fabric notebooks are submitted as a signed capsule with a single-use nonce | Accepted |
| [0006](0006-read-only-sql-engines.md) | SQL is read-only: hardened DuckDB, Spark by delegation | Accepted |
| [0007](0007-envelope-encrypted-secrets.md) | Secrets are envelope-encrypted, write-only and resolved at the point of use | Accepted |
| [0008](0008-hash-chained-audit-log.md) | The audit log is a hash chain | Accepted |
| [0009](0009-durable-pipeline-engine.md) | The pipeline engine is durable, with exactly-once trigger claims | Accepted |
| [0010](0010-plugin-entry-points.md) | Extension through Python entry points, three groups | Accepted |
| [0011](0011-sqlalchemy-core-two-databases.md) | SQLAlchemy Core, SQLite and PostgreSQL with one code path | Accepted |
| [0012](0012-console-without-build-step.md) | The web console has no build step and no CDN | Accepted |
| [0013](0013-encrypted-logical-backup.md) | Backups are encrypted logical dumps in one container format | Accepted |
| [0014](0014-mcp-read-only-by-default.md) | The MCP endpoint is read-only unless the token says otherwise | Accepted |
| [0015](0015-built-in-ca-and-optional-mtls.md) | A built-in certificate authority and optional mutual TLS for workers | Accepted |
| [0016](0016-apache-2-and-dco.md) | Apache-2.0 licence and DCO sign-off, no CLA | Accepted |
| [0017](0017-intelligence-subsystem.md) | Intelligence is an optional, observe-only module that builds its own asset graph (extends 0014 for its MCP tools) | Accepted |

## Writing a new ADR

Copy the structure of an existing record, number it with the next free four-digit number, state the **status** (Proposed, Accepted, Superseded by NNNN), add it to the table above, and link it from the page it affects. Open it as a pull request; the discussion on the pull request is the record of the alternatives.
