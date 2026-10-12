# Frequently asked questions

## General

**Do I need Microsoft Fabric?** No. Workspaces, lakehouses, SQL, notebooks, pipelines, Spark clusters and the MCP endpoint work with no Microsoft service. The Fabric extension is an adapter that you configure only if you want Fabric notebooks to run on your own Spark ([Fabric integration](../guides/fabric-integration.md)).

**Is this Microsoft Fabric on my own hardware?** No. It does not run Microsoft's runtime or engines, does not provide the Fabric Warehouse engine, and claims no performance parity. The local runtime is labelled *Fabric Runtime 2.0 Compatible*. See [TRADEMARKS.md](../../TRADEMARKS.md).

**What does it send to other parties?** Nothing by default. There is no telemetry or usage reporting. The outbound connections that exist are listed, with the setting that enables each, in [ports and networking](ports-and-networking.md#outbound-connections).

**What is the licence?** Apache License 2.0 (`LICENSE`). Third-party components keep their own licences ([NOTICE](../../NOTICE), `THIRD_PARTY_NOTICES.md`).

**How mature is it?** Version 0.1.0, beta. It runs end to end on Linux. The [requirements traceability](../compliance/requirements-traceability.md) maps every requirement to code and tests.

## Installing and running

**Which platforms are supported?** Python 3.11, 3.12 or 3.13 on Linux, macOS and Windows. See [getting started](../getting-started/README.md).

**How much hardware do I need?** A laptop is enough to start. Measured footprints and the reserve each worker keeps for its owner are in [sizing](../operations/sizing.md).

**Can it run without Java?** Yes, until you run Spark. SQL on the embedded DuckDB engine and notebooks on the Python engine do not need a JDK. `olcf runtime install --with-jdk` fetches one.

**Do I have to install Spark to try it?** No. `olcf quickstart --no-runtime` gives a running server, the sample lakehouse, SQL, dashboards, the MCP endpoint and Python notebooks, which run on the worker's own Python (the built-in `olcf-host-python` runtime; charts need `pip install "olcf[plots]"`). Only Spark notebooks, Spark SQL and Spark jobs need `olcf runtime install`, and until then they wait in the queue with a stated reason ([getting started](../getting-started/README.md#quickstart-about-five-minutes)).

**Can it run without Internet access?** Yes, with offline bundles for runtimes, wheelhouses for packages and a local mirror for Maven jars ([runbook](../operations/runbook.md#air-gapped-sites)). Installing a runtime for the first time needs either Internet access or a bundle.

**Where is my data?** Control-plane state (workspaces, items, jobs, audit, encrypted secrets) is in SQLite or PostgreSQL. Lakehouse data stays in the storage you chose and is read and written by workers directly. Runtimes, logs and artifacts are under the data directory ([architecture](../concepts/architecture.md)).

**SQLite or PostgreSQL?** SQLite suits one server process. Use PostgreSQL for several replicas, higher write load or when you want standard database operations ([high availability](../guides/high-availability.md)).

**How do I move to a new machine?** Make a backup with `--include-keys` and restore it with `--overwrite-files` on the new host ([backup and restore](../guides/backup-and-restore.md)). Lakehouse data moves with your storage tools.

## Compute

**Does a worker need an open inbound port?** Not for control: agents connect outward to the control plane. The machines of one Spark cluster must reach each other on the Spark ports ([ports](ports-and-networking.md)).

**Can I add a laptop or a cloud VM to the cluster?** Yes, any machine that can run the agent and reach the control plane. Test your network and machines with `olcf compute smoke` (it starts the test and follows it; exit 0 passed, 10 failed, 9 you stopped waiting, 6 no ready Spark runtime or a test already running).

**Why does my job wait in `queued`?** `olcf job timeline ID` says why: no node with the runtime, a constraint nobody satisfies, a paused queue or no free cores. See [troubleshooting](../guides/troubleshooting.md#jobs-and-notebooks).

**Which Spark versions are available?** The runtimes in `olcf runtime list`: `olcf-rt-2.0.0` (family `fabric-2.0-compatible`), `olcf-rt-1.3.0` (`fabric-1.3-compatible`) and `olcf-rt-native-1.0.0` (`olcf-native`). Use `olcf runtime matrix` for the versions each contains.

**Can I run on Kubernetes?** There is a Helm chart for the control plane and a Kubernetes compute provider with two modes. Both are covered by rendered-manifest and protocol tests; run `helm lint` and `kubeconform` on the chart with your own values before deploying ([Kubernetes](../operations/kubernetes.md)). The Spark ports are configured in one place, `spark.ports` ([ports](ports-and-networking.md#helm-sparkports-and-the-network-policy)).

## Security

**Is it safe to run other people's notebooks?** A notebook is arbitrary code running as the worker's operating-system user. OLCF scrubs the environment, scopes storage access to the workspace and offers a `sandbox_command` hook that wraps every job (for example bubblewrap), which fails closed if it cannot run. It is not a hardened multi-tenant sandbox on its own; read [code execution](../concepts/security-model.md#code-execution) and [hardening](../operations/hardening.md) before you let untrusted users run code.

**The runtime manifests are signed. By whom?** By a development key whose private half is in the source tree. A production installation does not trust it: the key is accepted only when `environment` is not `production` (an unset environment *is* production; `olcf init` and `olcf quickstart` write `environment = "development"`) or when you set `runtime.trust.allow_development_key = true`. For production generate your own key (`python -m olcf.runtime keygen`), add its public half to a trust directory (`python -m olcf.runtime --trust-dir DIR trust add KEY.pub`; `trust list|remove` manage it), re-sign the manifests (`sign --key KEY manifest.json`) and set `runtime.trust.dir` / `OLCF_RUNTIME_TRUST_DIR` ([hardening](../operations/hardening.md#6-supply-chain)). `olcf health` shows the state as the `runtime-trust` check.

**After an upgrade the bundled runtimes are "not trusted". Why?** Your data directory's `config.toml` has no `environment`, which now means `production`. On a laptop or lab machine add `environment = "development"` and restart; on a server follow the answer above.

**Can I rotate the master key or the JWT signing key?** Yes: `olcf security keys` and `olcf security rotate master-key|jwt-signing-key` ([runbook](../operations/runbook.md#key-rotation)). Worker certificates renew themselves; `olcf worker renew` forces it.

**What can a running job do with its token?** At most what the credential it was submitted with could do, in its own workspace, and only until the job ends or that credential is revoked (logging out or revoking the PAT ends the callbacks of its running jobs, so use a service token for long jobs). See [job and kernel credentials](../concepts/security-model.md#job-and-kernel-credentials).

**Which SQL does a Contributor (or an administrator) not get to run?** None of the host-reaching constructs: Spark `REFLECT`/`JAVA_METHOD`, `TRANSFORM ... USING`, queries on file paths, `LOCATION` and `INSERT OVERWRITE DIRECTORY`, `CREATE FUNCTION`, `SET`, `ADD JAR`, `${...}` substitution and table functions outside the allowlist are refused for every role (HTTP 422). See the [SQL guide](../guides/sql-and-lakehouse.md#statements-that-are-always-refused).

**How are secrets stored?** Encrypted with AES-256-GCM under the master key, write-only through the API, and resolved on the worker just in time. Vault is supported as a secret provider ([security model](../concepts/security-model.md#secrets)).

**Can I use my company login?** Yes: OIDC, Microsoft Entra ID, or LDAP/Active Directory through the built-in bridge ([SSO](../guides/sso-and-identity.md)).

**How do I report a vulnerability?** Privately, as described in [SECURITY.md](../../SECURITY.md).

## Intelligence (catalog, lineage, governance)

**What is it?** A map of what data exists and how it flows, with a governance layer on top: a catalog of assets (tables, columns, notebooks, pipelines, reports), lineage down to columns, a business glossary, owners, sensitivity labels, certification, policies and a quality score. See [OLCF Intelligence](../concepts/intelligence.md) and the [guide](../guides/intelligence.md).

**Can it slow down or break my jobs, queries or pipelines?** It is built so that it cannot. Compute, SQL and pipelines publish ordinary events in a best-effort block and never wait for Intelligence; it reads them later from a durable inbox. A test breaks the inbox and the graph and checks that jobs and SQL still succeed. The price: events can be lost, so a verified connection can be missing ([failure isolation](../concepts/intelligence.md#failure-isolation-and-the-no-circular-dependency-rule)).

**Is the lineage complete and correct?** No claim of completeness is made, and none should be inferred. Every edge has a confidence (`verified` is only what was seen running; `derived` is read from code; `inferred` is assumed), and what could not be seen is listed as a gap. Static analysis cannot follow a table name built at run time, many UDFs, or constructs the parsers do not model. Per-analyzer limits: [lineage support](lineage-support.md).

**Does it read my data?** It reads metadata (names, schemas from the Delta log, code, definitions). Values are read only through a sampling policy, which is off by default, limited to 0 to 100 rows, masked, audited, never stored, and unavailable to an AI agent through MCP ([sampling policies](../guides/intelligence.md#sampling-policies)).

**Does it enforce my policies?** No. Governance observes: a policy produces violations and events, never a blocked job. Asking for `enforce` is refused with `422`.

**Does it need Microsoft Fabric or Power BI?** No. It works on an installation with only local data (`olcf quickstart`, then the sample). A Fabric source, or a saved Power BI scanner result, only adds what the tenant contains. A Fabric-only installation (no workers, no Spark) is possible, but lineage there is never `verified` ([deployment shapes](../concepts/intelligence-fabric-and-onprem.md)).

**How was the Fabric and Power BI part built?** From Microsoft's documentation, with contract tests against a local test server in the test suite. Tenant settings, roles and quotas differ between organisations, so run the connection test first, report differences, and see [Fabric assumptions](../guides/fabric-assumptions.md#26-intelligence-discovery-the-scanner-and-the-fabric-and-power-bi-rest-apis).

**Can an AI agent use it?** Yes, through 14 read-only MCP tools (10 `intel_*`, 4 `governance_*`). An agent can ask where data comes from, what a change breaks, who owns a table and which labels reach a report; it cannot change anything or read sample rows ([MCP guide](../guides/mcp.md)).

**Is it a replacement for DataHub, OpenMetadata or Microsoft Purview?** Not claimed. It is a built-in catalog, lineage and governance layer for an OLCF installation, with no exporter to those tools in this release ([ADR 0017](../adr/0017-intelligence-subsystem.md)).

**Why does the lineage name a `spark_job` instead of my notebook?** A job submitted without an OLCF item has no notebook to name, so it is recorded as a job-level process (`olcf-job:<job id>`). A run submitted from a Fabric notebook is different: it is tied to the Fabric notebook asset the tenant scan discovered, with `verified` edges when the tenant, workspace and notebook GUIDs are known and `inferred` ones when only the notebook's name is.

## Fabric

**Which Fabric features are supported?** Whole-notebook remote execution from a Fabric notebook or pipeline notebook activity, with the `notebookutils` subset in the [Fabric extension model](../concepts/fabric-extension-model.md#on-the-worker). Everything the analyzer classifies as unsupported is blocked before it runs, with the cell, line and a remedy ([rules](fabric-rules.md)).

**Does it read OneLake?** Yes, the worker reads and writes OneLake directly with a service principal or token; the data does not pass through the control plane. The test suite covers it through a local DFS endpoint ([assumptions](../guides/fabric-assumptions.md#1-test-coverage)).

**Will OLCF damage my Delta tables for Fabric?** It refuses to upgrade a table's protocol in a way that would make Fabric's engines unable to read it (the interop policy), and removes the Fabric-only `VORDER` clause from SQL it runs rather than failing. Report any case where this fails.

## Development

**How do I add a storage backend, compute provider or pipeline activity?** Through entry points: see [extending OLCF](../development/extending.md).

**How do I run the tests?** See [testing](../development/testing.md).

**How do I contribute?** Read [CONTRIBUTING.md](../../CONTRIBUTING.md) and sign off your commits as it describes.
