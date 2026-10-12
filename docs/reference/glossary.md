# Glossary

Terms as OLCF uses them. Where a term also exists in Microsoft Fabric or Apache Spark, the OLCF meaning is given and the difference is noted.

**Activity.** One step of a pipeline: a notebook, Spark job, script, SQL statement, copy, HTTP call, webhook wait, control-flow node (`if`, `switch`, `for_each`, `until`), `set_variable`, `wait`, `fail` or `child_pipeline`. See [pipelines](../guides/pipelines.md).

**Agent.** Short for worker agent.

**Alert.** A record that a rule's condition held. It is `pending`, `firing` or `resolved`, and can be acknowledged or silenced. See [observability](../guides/observability.md#alerts).

**Alias.** In Intelligence, another name that identifies the same asset: an OLCF item id (`itm_...`), a catalog entry, a lakehouse table or column, a Fabric object id, a OneLake path, a qualified name. An asset has exactly one `ast_` id and any number of aliases; two sources that name the same object through a shared alias end up as one asset. See [stable asset ids](../concepts/intelligence.md#stable-asset-ids-and-aliases-the-ast_-id-problem).

**Analyzer.** The compatibility analyzer: it classifies every construct of a Fabric notebook with one of 73 rules and blocks, before anything runs, what the on-premises runtime cannot do. See [Fabric rules](fabric-rules.md).

**Artifact.** A file produced by a job (the executed notebook, output files, logs) and stored by the control plane.

**Asset.** An object in the Intelligence graph: a workspace, lakehouse, table, column, notebook, pipeline, saved query, dashboard, semantic model, report, process and so on, with a stable `ast_<ULID>` id. Assets are never deleted; one that disappears is marked `removed`. Not the same as an Item (an OLCF workspace object); an item is one kind of thing an asset can be made from.

**Audit log.** The append-only, hash-chained record of security-relevant actions. `olcf audit verify` checks the chain. See the [security model](../concepts/security-model.md#audit).

**Binding.** A grant of a role to a principal at a scope (organisation, workspace, or a narrower scope: item, connection, runtime or pool). Bindings created by an identity provider are tagged with its name and replaced at each login.

**Broker identity.** The Entra service principal, stored as an OLCF `onelake` connection, that the control plane uses to read the Fabric REST API (pull mode) and OneLake. Never given to a notebook.

**Capsule.** The signed document a Fabric notebook submits: source, context, runtime request, parameters, data references and policy. See [Fabric extension model](../concepts/fabric-extension-model.md#the-execution-capsule).

**Catalog.** The index of tables, columns, lineage, saved queries, reports and notebooks derived from lakehouses and items. Used by search, lineage and the MCP tools.

**Confidence.** How well an Intelligence edge is supported: `verified` (observed at run time), `derived` (read from code, definitions or metadata), `inferred` (assumed) or `unknown`. A path is as strong as its weakest edge. See [confidence levels](../concepts/intelligence.md#confidence-levels).

**Control plane.** The OLCF server: REST API, web console, scheduler, worker hub, Fabric adapter and MCP endpoint, in one process or several replicas.

**Cordon.** Stop sending new work to a worker; running work continues. **Drain** cordons, waits for running work to finish and releases the node.

**Correlation id.** An identifier of one request (header `X-Correlation-ID`, error field `correlation_id`, log field `cid=`).

**CA (certificate authority), built-in.** The private EC P-256 CA in `<data dir>/pki`, which issues the optional server certificate and the worker client certificates. See [TLS](../guides/tls-and-mtls.md).

**Data product.** In Intelligence governance, a named set of assets with owners, a domain, an optional freshness SLA and a lifecycle (draft, published). It is documentation and observation; it does not change how the assets are stored or served.

**Default lakehouse.** The lakehouse a notebook or job treats as `/lakehouse/default` and as the target of relative `Files/` and `Tables/` paths.

**Embedded worker.** The worker agent that `olcf serve` starts in the same machine by default, so a single machine is a cluster of one.

**Enrollment token.** A single-use token (`olcf_enr_...`) that lets a machine join the fleet. It is exchanged for a worker token (`olcf_wrk_...`) and, with mTLS, a client certificate.

**Environment.** A workspace item that pins Python packages and Spark settings for notebooks and jobs, published as a hash-locked `olcf.envlock/1` document. See [notebooks](../guides/notebooks.md#environments).

**Event.** A record of a state change, such as `job.failed`. Events are stored, streamed and delivered to webhooks. See [events](events.md).

**Evidence.** The record behind an Intelligence edge: its source (an analyzer and its version, or a run-time event), a location in the code or definition, the time observed and a confidence. Evidence about an asset the viewer may not read is shown as `{"hidden": true}`. See [evidence](../concepts/intelligence.md#evidence).

**Exit value.** The value a notebook passes to `notebookutils.notebook.exit`. It is the job's result and, in a Fabric pipeline, the activity's output.

**Exposure.** The answer to "where does this sensitivity label reach?": the assets downstream, along lineage, of every asset or column that carries an approved label. Computed on demand, never stored.

**Fabric Runtime 2.0 Compatible.** The label of the local runtime family `olcf-rt-2.0.0`. It is not Microsoft's runtime and makes no parity claim.

**Family (runtime family).** A compatibility line of runtimes: `fabric-2.0-compatible`, `fabric-1.3-compatible` or `olcf-native`. A job asks for a family or an exact runtime id.

**Gap.** Something discovery or analysis could not do (a permission refused, an unsupported construct, a dynamic table name, an unavailable API), stored as data with a reason and a fix. A *blocking* gap means objects may have been missed; an *informational* gap means less detail about something found. See [gaps](../concepts/intelligence.md#gaps-why-the-platform-tells-you-what-it-cannot-see).

**Host-Python runtime (`olcf-host-python`).** The built-in pseudo-runtime that every worker offers: the worker's own Python interpreter, with no Spark, Java or Scala, engine `python` only. Python-engine work runs on it when no installed runtime fits. See [compute model](../concepts/compute-model.md#the-host-python-runtime-no-spark).

**Idempotency key.** A client-chosen key; the same key never creates a second job, run or submission.

**Intelligence (OLCF Intelligence).** The module that discovers assets, derives lineage down to columns and layers governance (glossary, domains, labels, certification, policies, quality) on the resulting asset graph. It observes; it does not enforce and never blocks a job. See [OLCF Intelligence](../concepts/intelligence.md).

**Item.** A named object inside a workspace: notebook, lakehouse, pipeline, schedule, SQL query, report, environment, variable set, Spark job definition and others.

**Job.** One unit of execution on the fabric: a notebook run, Spark job, SQL statement, script or Fabric capsule. It moves through the states in the [compute model](../concepts/compute-model.md#job-lifecycle).

**Kernel.** The Jupyter kernel process of an interactive notebook session, running on a worker.

**Lakehouse.** An item that binds a storage location (local, S3, MinIO, ADLS Gen2 or OneLake) holding `Files/` and `Tables/` (Delta, Iceberg, Parquet) to a workspace.

**Leader.** The control-plane replica that holds the `control-plane` lease and therefore runs singleton work (scheduler, pipeline dispatcher, schedules, alerts, retention, scheduled backups). See [high availability](../guides/high-availability.md).

**Lineage.** Which datasets, columns and processes a given dataset or column comes from (upstream) and what depends on it (downstream, also called impact). In Intelligence every edge carries a confidence and evidence. The classic Catalog records a narrower SQL and copy lineage of its own.

**Master key.** The key at `<data dir>/secrets/master.key` that wraps every stored secret and, for unattended backups, the backup key. Lose it and the secrets are unreadable.

**MCP (Model Context Protocol).** The protocol of the `/mcp` endpoint through which AI agents browse the catalog and run read-only SQL. See [MCP](../guides/mcp.md).

**Node.** A machine that runs a worker agent. In the API, a worker record.

**Placeholder.** An Intelligence asset created when code mentions a table or file that no scan has found yet (`status` `placeholder`, shown as unresolved). When a scan finds the real object through a shared alias the placeholder is merged into it.

**Pool.** A named group of workers. A job can ask for a pool; `default` always exists.

**Principal.** An authenticated identity: a user, a service principal, a worker or the internal principal of a Fabric submission.

**Provider (compute provider).** The mechanism that runs a Spark job: `standalone`, `local`, `external`, `kubernetes` or `fabric` (Livy). `compute.default_provider` picks the default.

**Pull mode / push mode.** Two ways the Fabric SDK gets the notebook to OLCF: the notebook text travels in the capsule (push), or only identifiers do and the control plane fetches the definition (pull).

**Queue.** A named, prioritised queue of waiting jobs with limits; it can be paused.

**Queue reason (`queue_reason`).** Why a queued job is not running yet: a `code` (`paused`, `quota`, `no_runtime`, `no_node`, `constraints`, `resources`, `infeasible`, `held`, `backoff`), a message and a remediation. See [compute model](../concepts/compute-model.md#why-is-my-job-still-queued).

**Review queue.** The list of classification suggestions (from column names and types) that wait for a person to approve or reject them. Suggestions never count as classification until approved.

**Role.** A bundle of permissions: PlatformAdmin, SecurityAdmin, RuntimeAdmin, WorkspaceAdmin, Contributor, Operator, Viewer, ServicePrincipal. See [permissions](permissions.md).

**Runtime.** An immutable, signed, versioned environment of Python, PySpark, jars and optionally a JDK, installed side by side with others (`olcf-rt-2.0.0`, `olcf-rt-1.3.0`, `olcf-rt-native-1.0.0`). See [compute model](../concepts/compute-model.md#runtimes).

**Sampling policy.** The rule that decides whether Intelligence may read values from a table: rows (0 to 100), masking mode and whether classified columns are excluded. The default is off. Sample values are fetched on demand, masked and audited, and never stored. See [sampling policies](../guides/intelligence.md#sampling-policies).

**Scan.** One run of an Intelligence source (`scn_<ULID>`), full or incremental, that records the assets and edges it found as a versioned snapshot. Only a full scan that succeeded marks vanished assets as removed.

**Scanner (Power BI admin scanner).** Microsoft's admin metadata API (`getInfo`, `scanStatus`, `scanResult`) that Intelligence can call for a Fabric source, or whose saved JSON result you can import without a tenant connection. Not the same as an Intelligence *scan*.

**Scope (token scope).** A limit on what a token can do regardless of its owner's roles: `read`, `write`, `mcp`, `mcp:write`, `fabric` or `*`.

**Service principal.** A non-human account that authenticates only with tokens.

**Session.** An interactive notebook session: a kernel on a worker with optional Spark, kept until idle for `compute.session_idle_timeout_seconds`.

**Source (discovery source).** Something Intelligence scans: the local installation, the classic catalog mirror, a Microsoft Fabric / Power BI tenant, or an imported scanner result. Each has a connectivity test.

**Spark cluster.** A Spark Standalone master with workers, started on demand per runtime and pool on nodes of the fleet.

**Spark job definition.** A workspace item describing a Spark application (main file, arguments, resources) to run as a job.

**SSRF guard.** The check that refuses outbound requests to private, loopback and metadata addresses unless allow-listed. See [hardening](../operations/hardening.md#7-outbound-requests).

**Trigger.** What starts a pipeline: a schedule, a webhook call, a storage event or an upstream pipeline's result.

**Variable set.** A workspace item of named values (some secret) that resolve per stage and are referenced by notebooks and pipelines.

**Worker.** A machine registered with the control plane that runs jobs, sessions and Spark daemons through its worker agent.

**Worker agent.** The `olcf-worker` process on a machine. It connects outward over a WebSocket, reports its inventory and runs the commands the scheduler sends.

**Workspace.** The unit of organisation and access control: it contains items and has members with roles. Optionally carries a stage (dev, test, prod).
