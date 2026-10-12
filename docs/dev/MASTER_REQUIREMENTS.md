**OPEN LAKEHOUSE COMPUTE FABRIC**

**Master Product, Platform, Architecture & Engineering Requirements**

Open-source lakehouse platform with standalone operation, distributed Spark compute, and Microsoft Fabric compute-extension compatibility

  ---------------------------------------------------------------------------------------------------------
  **Field**                           **Definition**
  ----------------------------------- ---------------------------------------------------------------------
  Purpose                             Complete build specification

  Release intent                      Full platform release; no intentionally crippled interim product

  Modes                               Standalone, Fabric Compute Extension, Hybrid/Federated

  Compute                             Multi-machine Spark with side-by-side immutable runtimes

  Data                                Delta/Iceberg over OneLake, ADLS, S3, MinIO and pluggable storage

  Control plane                       Web UI, REST, SDK, CLI, scheduler, catalog, security, observability

  License intent                      Open-source core; final license requires legal review

  Document date                       October 2026
  ---------------------------------------------------------------------------------------------------------

**Recommended descriptor**

Open Lakehouse Compute Fabric - an open-source lakehouse platform with Microsoft Fabric Compute Extension. The wording communicates standalone independence and Fabric workload extension without implying that the project is Microsoft Fabric or Microsoft-endorsed.

**Non-negotiable architecture**

Microsoft Fabric is an adapter and control-plane integration, not the core. The complete platform must run without Microsoft Fabric.

# 1. Executive Product Definition

Open Lakehouse Compute Fabric (OLCF, working name) is a complete open-source data and analytics platform intended to provide an integrated lakehouse experience using open technologies while also acting as an external compute fabric for Microsoft Fabric. It must deploy on one machine, multiple on-premises machines, cloud VMs, or Kubernetes. Multiple isolated Spark runtime families can coexist on the fleet and workloads are scheduled only onto compatible resources.

Standalone mode supplies workspaces, notebooks, SQL, lakehouses, catalogs, pipelines, environments, connections, scheduling, Spark monitoring, data exploration, governance hooks, secrets, APIs and administration without a Microsoft subscription. Fabric Compute Extension mode lets a Fabric notebook or pipeline submit a compatible workload to OLCF, which reconstructs execution context, runs on external Spark, accesses OneLake directly, and returns status/results.

The platform should reuse mature open-source engines rather than reimplementing them. Differentiation is the coherent control plane, runtime compatibility system, distributed scheduler, adapters, UX and operational packaging.

# 2. Product Principles

-   Open first: core functionality cannot depend on Microsoft Fabric, Azure or a proprietary control plane.

-   Fabric compatible, not Fabric impersonation: reproduce supported semantics without claiming proprietary Microsoft runtime equivalence.

-   One notebook source: Fabric-integrated users do not maintain a separate remote notebook.

-   Compute is truly pooled: eligible machines form real Spark execution clusters.

-   Storage is abstracted: OneLake, ADLS, S3, MinIO and compatible stores sit behind stable interfaces.

-   Runtime is immutable: Spark/Delta/Python/Java/Scala combinations are packaged, signed, tested and side-by-side.

-   Deterministic execution: compatibility translation is rule-based and versioned; no runtime LLM guessing.

-   Fail closed: unsupported semantics produce actionable errors rather than silent behavioral changes.

-   Enterprise-ready: identity, TLS, RBAC, audit, secrets, backup, observability and upgrade are core features.

-   Single-node simplicity, cluster-scale architecture: one machine uses the same contracts as a multi-node deployment.

# 3. Operating Modes

  --------------------------------------------------------------------------------------------------------------------------------------------------------------
  **Mode**                            **Required behavior**
  ----------------------------------- --------------------------------------------------------------------------------------------------------------------------
  Standalone                          OLCF UI/API is primary control plane; storage may be MinIO/S3/ADLS/etc.; no Microsoft Fabric required.

  Fabric Compute Extension            Fabric is authoring/orchestration entry point; adapter captures source/context; OLCF Spark reads/writes OneLake.

  Hybrid                              Author in Fabric or OLCF; share code/catalog/data across OneLake and non-Microsoft stores.

  Federated compute                   Scheduler can choose local workers, Kubernetes, external Spark providers and optionally Fabric Spark.

  Air-gapped                          Standalone with offline runtime/package repositories and no public-cloud dependency, subject to chosen identity/storage.
  --------------------------------------------------------------------------------------------------------------------------------------------------------------

# 4. Current Fabric Compatibility Baseline

  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  **Capability**          **Baseline**                                                                               **Design use**
  ----------------------- ------------------------------------------------------------------------------------------ --------------------------------------------------------------------
  Runtime 2.0             GA: Spark 4.1, Java 21, Scala 2.13, Python 3.13, Delta Lake 4.2.                           Primary compatibility family

  Runtime 1.3             Spark 3.5.5, Java 11, Scala 2.12.17, Python 3.11, Delta 3.2; EOSA.                         Legacy only

  Notebook definition     REST supports ipynb and FabricGitSource; PySpark source can be notebook-content.py.        Source acquisition

  Runtime context         notebookutils.runtime.context exposes notebook/workspace/default Lakehouse/run metadata.   Execution capsule

  Notebook utility        notebookutils.notebook.getDefinition retrieves notebook content inside Fabric.             Push model

  Livy                    Fabric exposes session, batch and high-concurrency Spark APIs.                             Optional Fabric-compute provider; not external executor attachment

  OneLake                 External engines can use supported Entra/storage interfaces.                               Shared data plane

  Fabric optimizations    Fabric adds platform-specific Spark/Delta/native optimizations beyond OSS.                 Do not claim parity
  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# 5. Top-Level Architecture

> CLIENTS: Standalone Web UI \| Fabric Adapter \| Python SDK \| CLI \| REST/OpenAPI \| JDBC/SQL\
> \|\
> API GATEWAY + IDENTITY/RBAC\
> \|\
> CONTROL PLANE: Workspace \| Notebook \| Pipeline \| Catalog \| Environment \| Connection \| Audit \| Scheduler\
> \|\
> COMPUTE BROKER -\> Spark Standalone Provider \| Kubernetes Provider \| External Provider \| Fabric Provider\
> \|\
> WORKER FLEET: Node A/B/C\... with Runtime A/B/C side-by-side\
> \|\
> DATA LAYER: OneLake \| ADLS \| S3 \| MinIO \| local/dev -\> Delta \| Iceberg \| Parquet \| ORC

Fabric-specific logic belongs in the Fabric adapter and compatibility packages, never in the generic scheduler or storage core.

# 6. Core Functional Domains

  ----------------------------------------------------------------------------------------------------------------------------------------------------
  **Domain**                          **Minimum scope**
  ----------------------------------- ----------------------------------------------------------------------------------------------------------------
  Workspace/project                   Multi-tenant workspaces, folders, items, ownership, roles, settings, quotas, tags.

  Notebook                            Jupyter-compatible notebooks, PySpark/Python/SQL/Scala where supported, parameters, schedules, source control.

  Lakehouse                           Files/Tables explorer, managed/external tables, Delta/Iceberg metadata, schema browsing, optimization.

  SQL                                 Interactive SQL integration, history, saved queries, JDBC/ODBC where supported.

  Pipelines                           DAG orchestration, notebook/Spark/SQL/copy/script activities, parameters, triggers, retries.

  Environments                        Runtime, Python/JAR dependencies, variables, Spark configuration, immutable publication.

  Connections                         Storage, SQL, HTTP and extensible connectors; secrets separated from metadata.

  Compute                             Worker enrollment, resource inventory, runtime placement, Spark lifecycle, queues/policies.

  Monitoring                          Jobs, Spark apps/stages, logs, metrics, lineage events, audit, alerts.

  Administration                      Identity, RBAC, runtimes, workers, storage, backup, update, diagnostics.
  ----------------------------------------------------------------------------------------------------------------------------------------------------

# 7. Workspace and Item Model

-   Workspace is the primary security/configuration/collaboration boundary.

-   Items have immutable IDs, display names, type, owner, timestamps, tags, description, folder, version/ETag and optional sensitivity metadata.

-   Initial item types: Notebook, Lakehouse, SQL Query, Pipeline, Environment, Connection, Spark Job Definition, Variable Set, Schedule and extension item.

-   All item types expose CRUD/list APIs with permission checks.

-   Definitions are Git-friendly files: notebook source/ipynb, pipeline YAML/JSON, environment lock, connection metadata without secrets.

-   Whole-workspace export/import is required.

-   Soft delete/recycle bin and retention are required.

-   Git sync supports branches, conflict detection and deployment promotion without secrets in source control.

# 8. Standalone Notebook Experience

-   Browser notebook editor using a Jupyter-compatible model/components.

-   PySpark/Python and SQL first-class; Scala architecturally supported.

-   Notebook metadata declares runtime/environment, default lakehouse/catalog, parameters, timeout and compute policy.

-   Interactive session and batch-run modes.

-   Browser reconnects to existing compute session after refresh/disconnect.

-   Outputs support text, bounded tables, safe HTML, images, charts and DataFrame previews.

-   Typed parameters can be supplied by pipeline, schedule, API or Fabric adapter.

-   Version history and Git diff use source-friendly representation.

-   Import/export ipynb and plain source.

-   Expose Spark application/stage metrics and logs from notebook run.

# 9. Fabric Notebook Compute-Extension Adapter

  -----------------------------------------------------------------------------------------------------------------------------------
  **ID**                  **Requirement**                                                                     **Priority**
  ----------------------- ----------------------------------------------------------------------------------- -----------------------
  FA-01                   Fabric SDK validates/submits current notebook with one high-level call.             Must

  FA-02                   Capture notebook/workspace/default Lakehouse/run context.                           Must

  FA-03                   Acquire source through in-notebook API or Fabric REST pull mode.                    Must

  FA-04                   Prefer FabricGitSource .py for PySpark analysis; retain original definition.        Must

  FA-05                   Build signed/versioned execution capsule and submit by HTTPS.                       Must

  FA-06                   Whole-notebook remote execution is default; partial offload is advanced/optional.   Must

  FA-07                   Map Fabric-relative paths/catalog calls through shims.                              Must

  FA-08                   Return job ID, state, logs, exit value/result references and errors.                Must

  FA-09                   Fabric pipeline invocation propagates remote success/failure/exit value.            Must

  FA-10                   No broker service-principal secret in notebook source/payload.                      Must
  -----------------------------------------------------------------------------------------------------------------------------------

# 10. Execution Capsule Contract

-   schemaVersion, SDK/adapter version, submission ID, idempotency key, timestamps, source hash/signature.

-   Source: notebook/item IDs, names, workspace, source format, normalized source and optional original definition.

-   Context: default Lakehouse, run IDs, pipeline flags, environment/runtime metadata.

-   Standalone context: workspace/project, default catalog/lakehouse, user/service identity and item version.

-   Runtime request: compatibility family, Spark/Python/Java/Scala constraints, dependency lock and approved Spark config.

-   Parameters: explicitly declared primitive/structured values; never arbitrary interpreter dumps.

-   Data references: canonical and unresolved logical references to lakehouses, warehouses, catalogs and object paths.

-   Policy: queue, priority, resource request, timeout, retries, retention and placement constraints.

-   Security: identity claims/correlation only; reusable secrets are references resolved by broker.

-   Compatibility report: analyzer version, transformations, required shims, warnings and blockers.

# 11. Compatibility Analyzer and Shim Runtime

-   Parse Python AST and SQL where feasible; retain cell/line source maps.

-   Detect imports, magics, notebookutils/mssparkutils, Fabric paths, shell calls, dynamic installs, UDFs, native packages and external endpoints.

-   Classify constructs as native-portable, compatibility-shim, deterministic rewrite, warning, Fabric-only fallback or unsupported.

-   Do not translate ordinary PySpark; execute it on matching Spark.

-   Versioned shims cover runtime context, Lakehouse path resolution, supported filesystem helpers, notebook exit, variables, secrets abstraction, logging and selected utilities.

-   Unknown APIs fail closed with line/cell and remediation.

-   Every transformation is deterministic, testable and visible in diagnostics.

-   Maintain a golden compatibility corpus for every supported runtime.

# 12. Runtime System

  ---------------------------------------------------------------------------------------------------------------------------------------------------------
  **Area**                            **Requirement**
  ----------------------------------- ---------------------------------------------------------------------------------------------------------------------
  Manifest                            ID/version, Spark, Delta, Iceberg, Java, Scala, Python, OS compatibility, connectors, libraries, hashes/signatures.

  Fabric-2.0-compatible               Align OSS family to Spark 4.1 / Java 21 / Scala 2.13 / Python 3.13 / Delta 4.2 plus OLCF compatibility packages.

  Fabric-1.3-compatible               Optional legacy family aligned to Spark 3.5.5 / Java 11 / Scala 2.12 / Python 3.11 / Delta 3.2.

  Open runtime                        OLCF-native runtimes can advance independently of Microsoft cadence.

  Isolation                           Global bundle is immutable; per-job dependencies are layered separately.

  Side-by-side                        Worker can host multiple runtimes; scheduler places only compatible work.

  Offline                             Signed runtime/dependency packages can be imported offline.

  Rollback                            New versions install side-by-side; prior bundle remains until retired.
  ---------------------------------------------------------------------------------------------------------------------------------------------------------

# 13. Distributed Compute Fabric

-   Every machine runs a lightweight Worker Agent enrolled with machine identity/certificate.

-   Worker reports cores, usable RAM, disk, network labels, accelerators, installed runtimes, OS, agent version and health.

-   Scheduler allocates real Spark applications; driver and executors may span eligible nodes.

-   Spark Standalone is suitable for simple on-prem deployment; Kubernetes is a first-class scalable/HA provider.

-   Worker labels include site, rack, environment, GPU, data locality, trusted zone and runtime.

-   Drain/cordon prevents new allocations without killing running work.

-   Resource admission reserves OS/broker headroom and rejects unsafe oversubscription.

-   Dynamic allocation may be enabled where provider/runtime supports it.

-   Scheduler supports FIFO, priority queues, quotas, fair-share, per-workspace limits and reserved pools.

-   Cluster/network failures trigger deterministic application state and configurable retry, never silent duplicate writes.

# 14. Compute Provider Interface

  -------------------------------------------------------------------------------------------------------------
  **Operation**                       **Contract**
  ----------------------------------- -------------------------------------------------------------------------
  discover()                          Report provider capabilities, workers, runtime availability and health.

  submit(job)                         Create Spark/session/application and return immutable application ID.

  status(appId)                       Normalized state, resources, progress and provider-native details.

  logs(appId)                         Paged/streamed driver/executor logs.

  cancel(appId)                       Idempotent graceful then forced termination.

  scale(appId, policy)                Optional executor scaling/dynamic allocation controls.

  ui(appId)                           Secure Spark UI/proxy metadata.

  reconcile()                         Enumerate active apps after control-plane restart.
  -------------------------------------------------------------------------------------------------------------

# 15. Lakehouse and Storage Abstraction

-   StorageProvider interface supports OneLake/ADLS Gen2, S3, MinIO/S3-compatible and local development storage; additional providers plug in.

-   Lakehouse contains Files and Tables namespaces plus metadata linking storage root, default format and catalog.

-   Canonical internal URI format decouples notebook code from provider-specific addressing where possible.

-   Credentials are resolved server-side from Connection/Secret references.

-   Support managed and external tables/locations.

-   Delta Lake and Apache Iceberg are first-class; Parquet/ORC/CSV/JSON are file formats.

-   Storage browser supports listing, upload/download, preview, delete, move/copy and ACL/policy visibility subject to provider capability.

-   Never route bulk data through the control-plane API unless explicitly performing upload/download.

# 16. Delta Lake and Iceberg Safety

-   Runtime compatibility matrix includes table protocol/features, not only library versions.

-   Inspect Delta protocol/features before write; block unsupported protocol upgrades that would break Fabric interoperability.

-   Test append, overwrite, merge, delete, update, schema evolution, partitioning, liquid clustering/Z-order where supported, checkpoints and concurrent readers/writers.

-   Iceberg support includes catalog choice, snapshot/time-travel semantics, partition evolution and compaction where provider permits.

-   Cross-engine interoperability tests are release gates.

-   Vacuum/expiration/destructive maintenance requires explicit permissions and safety thresholds.

# 17. Catalog and Metadata

-   Provide a central logical catalog with catalogs/schemas/tables/views/functions and immutable IDs.

-   Support default catalog/lakehouse semantics for unqualified Spark table references.

-   Adapters map to provider catalogs such as Hive-compatible, REST catalog, Glue-compatible or Unity-compatible interfaces where legally/technically appropriate.

-   Metadata service stores logical ownership, tags, descriptions, schema snapshots, statistics pointers and lineage edges.

-   Catalog APIs are independent of the notebook UI.

-   Fabric adapter maps default Lakehouse/catalog behavior into OLCF semantics without contaminating standalone core.

# 18. SQL Experience

-   Provide browser SQL editor with autocomplete, schema explorer, parameters, saved queries, history, cancellation and results export.

-   SQL execution engine is pluggable; Spark SQL is baseline. Trino/StarRocks/DuckDB-like providers may be added without changing workspace APIs.

-   Expose JDBC/ODBC-compatible access through a supported gateway/provider when feasible.

-   SQL endpoints enforce workspace/catalog permissions and resource policies.

-   Query history records user, source, duration, bytes/rows where available, engine, runtime and failure category.

-   Do not pretend Fabric Warehouse semantics equal Spark Lakehouse semantics; Fabric Warehouse adapter is separate and explicit.

# 19. Pipeline and Orchestration Engine

  ---------------------------------------------------------------------------------------------------------------------------------------------------------
  **Area**                            **Requirement**
  ----------------------------------- ---------------------------------------------------------------------------------------------------------------------
  DAG                                 Directed acyclic graph with typed activities, dependencies, conditions and fan-out/fan-in.

  Activities                          Notebook, Spark job, SQL, copy/data movement, script/command, HTTP, child pipeline, wait, variable/control-flow.

  Parameters                          Typed pipeline parameters, variables, secrets references and activity outputs.

  Triggers                            Manual, schedule/cron, event/webhook and API.

  Reliability                         Per-activity retry/backoff, timeout, cancellation, resume/re-run from failure where safe.

  Concurrency                         Pipeline/workspace concurrency limits and queue policies.

  Observability                       Run graph, timings, logs, inputs/outputs metadata, lineage events.

  Portability                         Pipeline definitions stored as versioned YAML/JSON and Git-friendly.

  Fabric compatibility                Import/translate a documented supported subset of Fabric pipeline definitions; unsupported activities are explicit.

  Extensibility                       Plugin SDK for new activity types/connectors.
  ---------------------------------------------------------------------------------------------------------------------------------------------------------

# 20. Connections and Connector Framework

-   Connection metadata and secret material are separate objects.

-   Initial connectors: OneLake/ADLS, S3/MinIO, generic JDBC/ODBC, SQL Server/Azure SQL, PostgreSQL, MySQL, HTTP/REST, local/SFTP where appropriate.

-   Connector SDK defines schema discovery, test connection, read/write capability, credential types and optional copy optimization.

-   Secrets never appear in exported workspace definitions or Git.

-   Connections can be workspace-scoped or centrally shared with explicit permissions.

-   Copy activities support chunking, retries, checksums/row counts where possible and resumability.

# 21. Environment and Dependency Management

-   Environment item selects base runtime and immutable dependency lock.

-   Python dependencies use lock files and approved package indexes/wheelhouses; JARs use pinned coordinates/files and allow-list policy.

-   Per-job environment layer is keyed by dependency hash and may be safely cached.

-   Notebook-time package mutation is converted to a declared environment or blocked by policy.

-   Native packages are validated against worker OS/architecture/Python ABI.

-   Administrators can deny public Internet package installation and require mirrored repositories.

-   Every job records exact environment/runtime fingerprint for reproducibility.

# 22. Identity, Authentication and Authorization

-   Standalone supports local bootstrap admin only for initial setup; production supports OIDC/OAuth2 identity providers and optional LDAP/AD integration through a supported bridge.

-   Fabric adapter supports Microsoft Entra identities/service principals/managed identity where deployment permits.

-   RBAC roles are composable and scoped to organization, workspace, item, connection, runtime, worker pool and administration domains.

-   Minimum roles: PlatformAdmin, SecurityAdmin, RuntimeAdmin, WorkspaceAdmin, Developer/Contributor, Operator, Viewer and ServicePrincipal.

-   Authorization is enforced server-side on every API; UI visibility is not a security boundary.

-   Future ABAC/policy hooks can use tags, sensitivity, network zone and identity claims.

# 23. Secrets and Key Management

-   Secret values stored in OS-protected store for single-node installs or external vault/KMS provider for enterprise deployments.

-   Vault provider interface supports Windows protection, HashiCorp Vault and cloud key vaults as plugins.

-   Secrets are referenced by opaque ID and resolved just-in-time by authorized services.

-   Support client certificates and workload identity to reduce long-lived client secrets.

-   Rotation does not require editing notebooks/pipelines.

-   Logs, support bundles, telemetry and error traces run through secret/token redaction.

# 24. API Gateway, SDK and CLI

-   Versioned REST/OpenAPI is the canonical public control-plane contract.

-   Python SDK is generated/wrapped from API contracts and adds notebook conveniences.

-   CLI supports login/context, workspace/item CRUD, job submit/status/log/cancel, runtime/worker admin, export/import and diagnostics.

-   Idempotency keys on mutating long-running operations.

-   Pagination, filtering, sorting, ETags and optimistic concurrency on list/update APIs.

-   Long-running operations use operation IDs and polling/events rather than long-held HTTP connections.

-   Webhook/event stream API emits job, pipeline, item, worker and security events.

# 25. Administration Web UI

  ------------------------------------------------------------------------------------------------------------------------------------
  **Page**                            **Minimum functions**
  ----------------------------------- ------------------------------------------------------------------------------------------------
  Overview                            Service health, control-plane dependencies, storage, workers, runtimes, queues, jobs, alerts.

  Workspaces                          Create/manage workspaces, quotas, roles, settings, Git integration.

  Fabric Integration                  Tenant/client identity, test auth, allowed workspaces, adapter health, compatibility versions.

  Storage/Lakehouses                  Providers, roots, catalogs, lakehouses, connectivity and permissions tests.

  Runtimes                            Install, verify, repair, update, retire, rollback, uninstall, compatibility matrix.

  Compute                             Worker pools, resources, labels, queues, reservations, concurrency, drain/cordon.

  Jobs                                Search/filter, timeline, source/runtime hash, Spark app/stages, cancel/retry, logs.

  Pipelines                           Runs, schedules, failures, retries, activity logs.

  Security                            Identity providers, RBAC, certificates, secrets providers, allow-lists, audit.

  Updates                             Core/agent/runtime channels, signed manifests, offline import, maintenance windows.

  Diagnostics                         Auth, storage read/write, Spark smoke, network, support bundle.

  Backup/Restore                      Metadata backup schedule, encryption, restore points and validation.
  ------------------------------------------------------------------------------------------------------------------------------------

The primary administration UI is a local/hosted web application, not Tkinter. Desktop shortcuts may open the local URL. This permits the same UI to administer Windows, Linux and remote clusters.

# 26. Installer, Packaging and Service Model

-   Signed Windows installer for single-node/control-plane deployment; Linux packages/container images also required for full platform portability.

-   Windows installs core services as auto-start services under least privilege and creates Manager/Logs/Uninstall shortcuts.

-   Core installer remains relatively small; large runtime bundles are installed separately from signed manifests/packages.

-   Installer can deploy all-in-one mode: control plane + worker + local object store option + runtime.

-   Cluster mode separates control plane and Worker Agent installers.

-   Runtime install/repair/remove is transactional and never downloads unpinned latest components.

-   Offline bundle import is supported.

-   Upgrade supports preflight, database migration, drain, rollback and compatibility checks.

-   Uninstaller offers preservation/removal of metadata, secrets, logs, storage and runtime caches independently.

-   Produce SBOM and third-party license inventory for every release/runtime bundle.

# 27. High Availability and State

-   Single-node mode may use SQLite for easy install, but enterprise mode requires PostgreSQL-compatible durable metadata.

-   HA control plane supports multiple stateless API/service replicas behind a load balancer.

-   Leader election/distributed locks protect scheduler singleton responsibilities.

-   Queue/event backend is abstracted; enterprise deployment can use PostgreSQL, Redis, RabbitMQ or equivalent supported implementation.

-   Worker agents reconnect after controller failover and reconcile active applications.

-   No running Spark application should be killed merely because a web/API node restarts.

-   Metadata schema migrations are transactional, versioned and reversible where feasible.

# 28. Observability and Spark Monitoring

-   Structured logs with correlation IDs and redaction.

-   Metrics: API latency/error, queue depth/wait, job startup/runtime, worker CPU/RAM/disk/network, executor counts, Spark stages/tasks/shuffles/spill/GC, storage/auth failures.

-   Secure Spark UI access through reverse proxy or signed route; do not expose unauthenticated Spark UIs.

-   OpenTelemetry-compatible traces/metrics recommended as standard export.

-   Job timeline correlates workspace/item/run, broker job, provider app, Spark app and source/runtime fingerprints.

-   Alert rules for failed jobs, queue saturation, worker loss, disk pressure, runtime mismatch, auth expiry and control-plane health.

-   Retention by age/size; archive provider optional.

# 29. Lineage, Audit and Governance

-   Emit lineage events for notebook/pipeline/SQL reads and writes when deterministically discoverable.

-   Lineage graph uses stable dataset/item IDs and supports column lineage where parsers can prove it.

-   Audit records login, permission change, secret/config change, item CRUD, runtime install, worker enrollment, job submit/cancel and destructive data actions.

-   Audit records are append-oriented and exportable to SIEM.

-   Tag/classification framework allows external scanners/catalogs to enrich metadata.

-   Do not claim full automatic lineage for dynamic code; mark unknown/partial lineage explicitly.

# 30. Backup, Restore and Disaster Recovery

-   Back up control-plane metadata database, configuration, encrypted secret references/metadata, item definitions and Git state pointers.

-   Object/lakehouse data backup follows storage-provider strategy and is not assumed to be covered by metadata backup.

-   Scheduled encrypted backups with retention and off-host target.

-   Restore supports clean-environment disaster recovery and documented key/certificate prerequisites.

-   Regular automated restore tests are a release/operations requirement.

-   Cluster worker nodes are replaceable; persistent state must not live only on a worker local disk.

# 31. Security Hardening

  ------------------------------------------------------------------------------------------------------------------------------------------------------------------
  **Area**                **Requirement**                                                                                                    **Priority**
  ----------------------- ------------------------------------------------------------------------------------------------------------------ -----------------------
  Transport               TLS everywhere; mTLS or equivalent for worker/controller and production remote administration.                     Must

  Supply chain            Signed installers/images/runtime bundles; hash verification; SBOM; dependency scanning.                            Must

  Execution               Least-privilege service accounts, isolated workdirs, restricted filesystem/network, optional containers.           Must

  Network                 Admin UI localhost by default on desktop; remote access explicit; firewall guidance.                               Must

  Tokens                  Short-lived tokens, server-side refresh, no token logging.                                                         Must

  Replay                  Nonce/timestamp/idempotency/signature controls for capsules and sensitive operations.                              Must

  Tenant isolation        Workspace/resource checks on every request; test horizontal/vertical privilege escalation.                         Must

  Code execution          Notebook code is inherently privileged compute; administrators define sandbox/network/filesystem policy by pool.   Must

  Updates                 No unsigned update path.                                                                                           Must

  Support bundles         Sanitized by default; explicit opt-in for sensitive artifacts.                                                     Must
  ------------------------------------------------------------------------------------------------------------------------------------------------------------------

# 32. Networking

-   Document all ports and outbound destinations for standalone and Fabric-integrated modes.

-   Support enterprise HTTP(S) proxies and custom CA trust.

-   No inbound Internet exposure is required for basic on-prem deployment.

-   Worker traffic is authenticated/encrypted and supports routed multi-subnet environments.

-   Kubernetes deployment supports NetworkPolicy.

-   Fabric mode separately diagnoses DNS, TLS, proxy, Entra authentication, Fabric authorization and OneLake authorization.

-   Storage data paths should be direct worker-to-storage where possible, not hairpinned through controller.

# 33. Job Lifecycle and Reliability

  ---------------------------------------------------------------------------------------------------------
  **State**                           **Meaning**
  ----------------------------------- ---------------------------------------------------------------------
  Accepted                            Schema/auth/idempotency validated.

  Validating                          Compatibility, policy, dependencies and data authorization checked.

  Queued                              Waiting for eligible resources.

  Provisioning                        Environment/cluster/app being created.

  Running                             Spark or activity execution active.

  Succeeded                           Declared outputs committed and job complete.

  Failed                              Terminal error with classified reason.

  CancelRequested                     Termination underway.

  Cancelled                           Execution terminated and cleanup completed.

  TimedOut                            Policy deadline exceeded and termination completed.
  ---------------------------------------------------------------------------------------------------------

-   Persist every transition with timestamp/reason.

-   Retry only when policy and write semantics allow; never blindly replay non-idempotent writes.

-   Classify failures: compatibility, authn, authz, storage, catalog, dependency, resource, Spark, user-code, timeout, cancellation, provider, internal.

-   Broker restart reconciles provider applications.

-   Cleanup is asynchronous but bounded; diagnostics retained per policy.

# 34. Performance and Capacity Requirements

  ---------------------------------------------------------------------------------------------------------------------------------------------------
  **Metric**                 **Target**                                                                                       **Type**
  -------------------------- ------------------------------------------------------------------------------------------------ -----------------------
  Control-plane submit       p95 \< 1s excluding large source upload/staging.                                                 Target

  Compatibility validation   Typical notebook \< 5s after source acquisition.                                                 Target

  Long-job broker overhead   \< 2% of runtime for jobs \> 5 minutes.                                                          Target

  Worker utilization         Configurable reservations; prevent memory/CPU oversubscription by default.                       Requirement

  Job history                At least 100,000 metadata records per production control plane without architectural redesign.   Target

  Logs                       Stream/paginate multi-GB logs; never load whole log into controller memory.                      Requirement

  Recovery                   Controller restart/failover reconciles typical active fleet within 2 minutes.                    Target

  Scale-out                  Adding eligible workers increases available executor capacity without SDK/notebook changes.      Requirement
  ---------------------------------------------------------------------------------------------------------------------------------------------------

Spark performance depends on hardware, data locality and workload. The project must benchmark and publish its own results and must not promise parity with proprietary Fabric optimizations.

# 35. Testing and Release Gates

## 35.1 Unit and Contract Tests

-   AST/SQL compatibility rules and source maps.

-   Capsule schemas, signatures and idempotency.

-   Path/catalog resolution.

-   Scheduler/resource admission and state machines.

-   Secret redaction and RBAC.

-   Runtime manifest/signature verification.

-   Storage and compute provider contract test suites.

## 35.2 Integration Tests

-   Single-node clean install through first notebook.

-   Multi-node worker enrollment and distributed Spark job.

-   Worker loss, controller restart and network interruption.

-   OneLake/ADLS/S3/MinIO read/write.

-   Delta and Iceberg interoperability.

-   Fabric source/context acquisition and remote execution.

-   Runtime side-by-side selection.

-   Pipeline execution and retry/cancel.

-   Git export/import and environment locks.

-   Installer repair/upgrade/rollback/uninstall.

## 35.3 Compatibility Corpus

-   Representative Fabric notebooks: ETL, joins, windows, UDFs, Spark SQL, Delta MERGE, multi-Lakehouse, parameters, pipeline invocation.

-   Standalone equivalents with golden outputs.

-   Negative notebooks verify unsupported APIs fail before execution.

-   Run corpus against every SDK, adapter and runtime release.

-   Data correctness is compared, not merely job success.

## 35.4 Chaos and Security Tests

-   Kill worker/executor/driver/controller during execution.

-   Expire credentials/certificates and rotate them.

-   Fill disk, constrain memory and simulate storage throttling.

-   Tamper with runtime packages/manifests.

-   Privilege escalation, path traversal, malicious notebook filesystem/network behavior and API fuzzing.

-   Secret leakage scans across logs, capsule, telemetry and support bundles.

# 36. Full-Release Acceptance Criteria

-   A new administrator can install an all-in-one standalone instance without manually installing Spark/Java/Python.

-   A user can create workspace, lakehouse, environment and notebook in the web UI and run distributed PySpark.

-   At least two worker machines can contribute executors to the same eligible Spark application.

-   Multiple runtime families can coexist and scheduler prevents cross-runtime placement.

-   Delta data remains interoperable across supported engines/providers and protocol policy is enforced.

-   Pipelines can orchestrate notebooks, Spark, SQL, copy and control-flow activities with schedules/retries/cancel.

-   Fabric notebook can submit itself without a maintained mirror notebook and run against OneLake on external compute.

-   Fabric context/default Lakehouse is reconstructed for supported semantics.

-   Standalone operation works with MinIO/S3-compatible storage and no Fabric dependency.

-   RBAC, OIDC/Entra integration, protected secrets, TLS, audit, backup/restore and signed updates pass production tests.

-   HA control plane survives node loss without losing metadata; active compute is reconciled.

-   Every advertised compatibility feature has automated golden tests.

-   Unsupported behavior fails explicitly; no silent data-semantic substitutions.

# 37. Recommended Technology Baseline

  -------------------------------------------------------------------------------------------------------------------------------------------------------
  **Area**                            **Recommendation**
  ----------------------------------- -------------------------------------------------------------------------------------------------------------------
  Web/API                             FastAPI/Python is pragmatic initially; public contracts remain language-neutral.

  Frontend                            React/TypeScript or equivalent modern web UI; Jupyter components for notebook editing.

  Metadata                            PostgreSQL production; SQLite allowed for all-in-one developer/small install.

  Object storage                      MinIO/S3-compatible default standalone option; pluggable providers.

  Spark                               Apache Spark with Spark Standalone simple provider and Kubernetes provider.

  Table formats                       Delta Lake first-class; Apache Iceberg first-class.

  Catalog                             Open catalog abstraction; REST/Hive-compatible providers as plugins.

  Orchestration                       Native OLCF DAG service or mature embedded orchestration components behind OLCF API; definitions remain portable.

  Identity                            OIDC/OAuth2; Entra adapter; optional LDAP/AD bridge.

  Secrets                             OS protection for small install; Vault/KMS plugins for enterprise.

  Telemetry                           OpenTelemetry-compatible metrics/traces plus structured logs.

  Packaging                           Signed Windows installer, Linux packages/containers, signed runtime bundles and Helm charts for Kubernetes.
  -------------------------------------------------------------------------------------------------------------------------------------------------------

# 38. Open-Source Architecture and Governance

-   Repository boundaries should separate core APIs/models, control-plane services, web UI, worker agent, providers, Fabric adapter, runtime manifests and compatibility tests.

-   Use an OSI-approved license selected with counsel after dependency/license analysis; avoid accidental copyleft/proprietary conflicts in bundled runtimes/connectors.

-   Publish contribution guide, code of conduct, security policy, release process, architecture decision records and compatibility policy.

-   Plugin/provider interfaces are public and versioned so vendors/community can add storage, compute, catalog, identity and pipeline activities.

-   Release artifacts include source, binaries/images, SBOM, signatures, checksums, license notices and reproducible build metadata.

-   Trademark policy must make clear that Microsoft Fabric is a compatibility target/integration and Microsoft trademarks remain Microsoft-owned.

# 39. Naming and Product Positioning

Recommended working name: Open Lakehouse Compute Fabric (OLCF). Recommended descriptor: \"Open-source lakehouse platform with Microsoft Fabric Compute Extension.\" Alternative descriptors: \"Standalone Open Lakehouse + Fabric Compute Extender\" or \"Open Lakehouse Runtime and Compute Fabric for standalone and Microsoft Fabric workloads.\"

-   Avoid names implying the software is Microsoft Fabric, an official Microsoft runtime, or a drop-in reproduction of proprietary features.

-   Use labels such as Fabric Runtime 2.0 Compatible, not Fabric Runtime 2.0, for local runtime bundles.

-   Describe the Fabric feature as compute extension or external compute execution, not physical expansion of the Microsoft-managed Spark cluster.

-   Marketing can state that compatible Fabric notebooks are remotely executed on customer-controlled Spark while OneLake remains the data plane, subject to documented compatibility matrix.

# 40. Source References for Fabric Compatibility Baseline

-   Microsoft Learn - Fabric Runtime 2.0: https://learn.microsoft.com/en-us/fabric/data-engineering/runtime-2-0

-   Microsoft Learn - Apache Spark runtime in Fabric: https://learn.microsoft.com/en-us/fabric/data-engineering/runtime

-   Microsoft Learn - Get Notebook Definition REST API: https://learn.microsoft.com/rest/api/fabric/notebook/items/get-notebook-definition

-   Microsoft Learn - Notebook definition formats: https://learn.microsoft.com/en-us/rest/api/fabric/articles/item-management/definitions/notebook-definition

-   Microsoft Learn - NotebookUtils runtime context: https://learn.microsoft.com/en-us/fabric/data-engineering/notebookutils/notebookutils-runtime

-   Microsoft Learn - NotebookUtils notebook management: https://learn.microsoft.com/en-us/fabric/data-engineering/notebookutils/notebookutils-notebook-management

-   Microsoft Learn - Fabric Livy API overview: https://learn.microsoft.com/en-us/fabric/data-engineering/api-livy-overview

-   Microsoft Learn - Spark monitoring/performance: https://learn.microsoft.com/fabric/data-engineering/spark-monitoring-best-practices

# Appendix A - Required Repository/Service Boundaries

> olcf-core-contracts\
> olcf-control-api\
> olcf-web-ui\
> olcf-workspace-service\
> olcf-notebook-service\
> olcf-pipeline-service\
> olcf-catalog-service\
> olcf-scheduler\
> olcf-compute-broker\
> olcf-worker-agent\
> olcf-runtime-manager\
> olcf-storage-providers\
> olcf-compute-providers\
> olcf-identity-security\
> olcf-observability\
> olcf-fabric-adapter\
> olcf-fabric-compatibility\
> olcf-cli-sdk\
> olcf-installer\
> olcf-compatibility-corpus

# Appendix B - Core API Families

-   /v1/workspaces, /items, /notebooks, /lakehouses, /catalogs, /pipelines, /environments, /connections.

-   /v1/jobs, /sessions, /spark-applications, /logs, /events.

-   /v1/workers, /worker-pools, /providers, /runtimes, /queues.

-   /v1/identity, /roles, /permissions, /secrets, /audit.

-   /v1/admin/health, /diagnostics, /updates, /backup, /restore.

-   /v1/integrations/fabric for adapter configuration and compatibility status.

# Appendix C - Definition of Done

The full platform is done when standalone and Fabric-extension modes use the same secure core control plane; multi-machine Spark compute is genuinely pooled; multiple immutable runtimes are safely scheduled side-by-side; lakehouse/catalog/pipeline/notebook experiences are integrated; OneLake and open object stores are supported; Fabric notebook compatibility is deterministic and tested; installation, HA, security, observability, backup, upgrade and rollback are production-grade; and every advertised feature is backed by automated correctness and failure-mode tests.
