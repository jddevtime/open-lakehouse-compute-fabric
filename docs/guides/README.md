# Guides

Task-oriented pages. Each one says what was verified by running it and what was not. The exact command, API and setting names are in the generated [reference](../reference/README.md).

## Using the platform

| Guide | Use it to |
|---|---|
| [Command line](cli.md) | sign in, contexts, output formats, exit codes, scripting patterns |
| [Python SDK](sdk.md) | automate OLCF from Python: clients, jobs, SQL, pipelines, waiting and errors |
| [Notebooks, environments and Spark jobs](notebooks.md) | write, parameterise and run notebooks; manage packages, environments, variable sets and Spark job definitions |
| [SQL, lakehouses and dashboards](sql-and-lakehouse.md) | create lakehouses, load files, run SQL on DuckDB or Spark, build dashboards, use the catalog |
| [Pipelines, schedules and triggers](pipelines.md) | orchestrate activities with parameters and expressions; schedules, webhooks, storage events; recover failed runs; import from Fabric |
| [Intelligence: lineage, impact, governance](intelligence.md) | run a scan, read table and column lineage, answer the five questions (where from, what breaks, why stale, what unused, where does PII go), connect Fabric and Power BI, sample safely, govern |
| [Connect an AI agent (MCP)](mcp.md) | let Claude Desktop, Claude Code or another MCP client query the lakehouse with the permissions of its token |

## Microsoft Fabric

| Guide | Use it to |
|---|---|
| [Fabric integration](fabric-integration.md) | configure the extension, install the SDK in a Fabric notebook, run a notebook remotely, diagnose failures |
| [Fabric assumptions](fabric-assumptions.md) | the assumptions about Microsoft behaviour behind the integration, and how to check them against your tenant |

## Running it

| Guide | Use it to |
|---|---|
| [Cluster and the Compute control panel](cluster-and-control-panel.md) | add machines, install runtimes, group workers, cordon and drain, run Spark clusters |
| [Single sign-on and identity providers](sso-and-identity.md) | connect OIDC, Microsoft Entra ID or LDAP; map groups to roles |
| [TLS and mutual TLS](tls-and-mtls.md) | serve HTTPS, use your own certificate, require worker certificates |
| [Backup and restore](backup-and-restore.md) | encrypted backups, targets, retention, restore tests, rebuilding a lost installation |
| [High availability](high-availability.md) | several control-plane replicas on PostgreSQL; failover behaviour and its limits |
| [Observability](observability.md) | health, Prometheus and OTLP, logs, events, webhooks and alerts |
| [Troubleshooting](troubleshooting.md) | symptom-to-cause tables for install, sign-in, jobs, workers, SQL, pipelines, backup |

For operators there is also the [operations section](../operations/README.md): runbook, sizing, hardening, production checklist and Kubernetes.
