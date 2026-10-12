# Requirements traceability

This page maps every section of the original requirements document ([`docs/dev/MASTER_REQUIREMENTS.md`](../dev/MASTER_REQUIREMENTS.md), October 2026, sections 1 to 40 and appendices A to C) to the code that implements it, the tests that exercise it, and its status. It is the place to look before relying on a capability, and the list of what the project does **not** claim. The answers to the external code review of 8 October 2026 (18 findings, the out-of-scope items and the scope decisions) are in [`CODE_REVIEW_RESPONSE.md`](../dev/CODE_REVIEW_RESPONSE.md); the rows below were re-checked against them.

How it was built: each row was checked against the source tree, the generated references, the automated tests and, for behaviour that can be observed, a running server. A status of *Done* means the row's behaviour is implemented and automated tests for it pass on Linux in the project's build environment (2 CPUs, 7 GB). Nothing here was taken from a design document without looking at the code.

**Changes of this revision (9 October 2026).** Counts moved from 164 / 46 / 52 / 4 / 3 (Done / Acceptance pending / Partial / Gap / Process) to 161 / 50 / 51 / 4 / 3. Row 23.5 (rotation) became Done: key rotation and renewal have entry points and were exercised on a local server and PostgreSQL 16. Rows 27.2, 27.3, 30.4 and 36.11 (HA and clean-environment restore) became Acceptance pending: the evidence covers processes on one host and SQLite for the restore; the earlier wording claimed more than that. No row that needs acceptance on an external system was moved to Done. Rows 12.9, 13.1, 13.4, 14.1, 18.4, 19.5, 22.5, 25.1, 26.1, 26.2, 31.3 and 31.5 were rewritten for what the remediation changed.

## Status vocabulary

| Status | Meaning |
|---|---|
| **Done** | Implemented, and covered by automated tests that pass in the build environment. |
| **Acceptance pending** | Implemented to the documented contract and covered by automated tests (test servers, platform-specific code exercised from Linux, or a single host). The acceptance run on the target system (a Fabric tenant, a Kubernetes cluster, Windows, several physical machines, Maven Central) is listed in [release readiness](../development/release-readiness.md). |
| **Partial** | Part of the requirement is implemented. The row says what is missing. |
| **Gap** | Not implemented. |
| **Process** | A practice or deliverable of engineering rather than product behaviour; the row says where the evidence is. |

## Summary

| Status | Sections 1 to 40 (the original requirements) | Section 41 (OLCF Intelligence) | Total |
|---|---|---|---|
| Done | 161 | 19 | 180 |
| Acceptance pending | 50 | 6 | 56 |
| Partial | 51 | 4 | 55 |
| Gap | 4 | 1 | 5 |
| Process | 3 | 0 | 3 |
| **Total** | **269** | **30** | **299** |

The left column is the count of the 9 October 2026 revision and was not re-judged. Section 41 is new: OLCF Intelligence has its own contract, [`docs/dev/INTELLIGENCE.md`](../dev/INTELLIGENCE.md), and the requirements document of section 1 to 40 does not list it, so its rows are traced to that contract (section 1 of it, the non-negotiables, and the features of its later sections) and not to a numbered source bullet.

The sections below use identifiers such as `13.4` for "section 13, fourth requirement"; the numbering follows the order of the bullets or table rows in the source document, and a row may cover several consecutive bullets.

Where a test path is given as a directory (for example `tests/compute/`), the relevant files are named in the area list in [testing](../development/testing.md#layout). Counts of tests: 5,773 collected by one `pytest --collect-only tests` (re-measured on 10 October 2026, after the Intelligence module and its tests were added; the full suite was run in shards, see [CONTRACTS](../dev/CONTRACTS.md), Integration QA addenda).

---

## 1-3. Product definition, principles, operating modes

| ID | Requirement | Implementation | Tests | Status |
|---|---|---|---|---|
| 1.1 | Runs on one machine; on-premises machines and cloud VMs; on Kubernetes | `olcf serve` (control plane, scheduler and embedded worker in one process); `olcf-worker`; `deploy/helm/olcf`, `deploy/docker*` | `tests/integration/test_product_e2e.py` (one server, two agents); `tests/tooling/test_deploy.py` (Helm and Compose rendered) | Acceptance pending (single host; Docker and Kubernetes covered by rendered-manifest tests) |
| 1.2 | Multiple Spark runtime families on the fleet; work scheduled only onto compatible resources | `compute/planner.py`, `runtime/manager.py` | `tests/compute/test_compute_planner.py`, `tests/node/test_runtime_lifecycle.py` | Done |
| 1.3 | Standalone mode with workspaces, notebooks, SQL, lakehouses, catalog, pipelines, environments, connections, scheduling, Spark monitoring, data exploration, secrets, APIs, administration, without Microsoft | whole platform | `tests/integration/`, `tests/e2e/` (no Fabric configured) | Done |
| 1.4 | Fabric Compute Extension mode: a Fabric notebook or pipeline submits a workload; OLCF reconstructs the context, runs it on external Spark, reads OneLake directly and returns status and results | `fabric/`, `sdk/fabric/` | `tests/fabric/` (local Fabric, Entra and OneLake test servers) | Acceptance pending (tenant acceptance: [Fabric assumptions](../guides/fabric-assumptions.md), R07) |
| 1.5 | Reuse mature open-source engines | Spark, Delta (delta-rs and Spark), DuckDB, sqlglot, Jupyter kernels, pyiceberg | n/a | Done |
| 2.1 | Open first: core does not depend on Fabric, Azure or a proprietary plane | `fabric` is a separate module; `compute`, `storage`, `catalog` do not import it | `tests/integration/`, `tests/e2e/` run without it; [ADR 0001](../adr/0001-fabric-as-an-adapter.md) | Done |
| 2.2 | Fabric compatible, not impersonation; no claim of runtime equivalence | runtime labels "Fabric Runtime 2.0 Compatible"; [TRADEMARKS.md](../../TRADEMARKS.md) | `tests/tooling/` header and label checks | Done |
| 2.3 | One notebook source (no maintained mirror notebook) | SDK `run_remote` | `tests/fabric/test_sdk.py`, `test_e2e_flow.py` | Acceptance pending |
| 2.4 | Compute is truly pooled: eligible machines form real Spark clusters | `compute/providers/standalone.py`, `worker/daemons.py` | `tests/node/test_spark_cluster.py`, `tests/compute/test_spark_smoke_real.py` (two agents on one host, real JVMs) | Acceptance pending (several agents on one host; for several machines run `olcf compute smoke`) |
| 2.5 | Storage abstracted | `storage/providers/` | `tests/data/` | Done |
| 2.6 | Runtimes immutable, signed, side by side | `runtime/` | `tests/node/test_manifest_signing.py`, `test_runtime_lifecycle.py` | Done |
| 2.7 | Deterministic, rule-based, versioned compatibility; no runtime LLM guessing | `fabric/analyzer/` (73 rules) | `tests/fabric/test_analyzer.py`, `tests/corpus/` | Done |
| 2.8 | Fail closed on unsupported semantics | `core/errors.py` (`Unsupported`, `CompatibilityBlocked`); shim raises not-supported | `tests/corpus/` (negative cases), `tests/fabric/test_shim_runner.py` | Done |
| 2.9 | Enterprise-ready: identity, TLS, RBAC, audit, secrets, backup, observability, upgrade | `security/`, `ops/`, `observability/` | `tests/security/`, `tests/ops/` | Done (see Known limitations) |
| 2.10 | Single-node simplicity, cluster-scale architecture, same contracts | database state + leader lease ([ADR 0002](../adr/0002-database-as-state-and-leader-lease.md)) | `tests/ops/test_ha.py` (PostgreSQL) | Done |
| 3.1 | Standalone mode | see 1.3 | | Done |
| 3.2 | Fabric Compute Extension mode | see 1.4 | | Acceptance pending |
| 3.3 | Hybrid mode: author in Fabric or OLCF; share across OneLake and non-Microsoft stores | OneLake and S3/ADLS providers, lakehouse-to-lakehouse copy | `tests/fabric/test_e2e_onelake.py` (local OneLake test server) | Acceptance pending |
| 3.4 | Federated compute: local workers, Kubernetes, external Spark, optionally Fabric Spark | `compute/providers/` (`standalone`, `local`, `external`, `kubernetes`, `fabric` via Livy) | `tests/compute/test_providers_k8s.py`, `test_providers_livy.py`, `test_standalone.py`; `test_review_k8s.py`, `test_review_livy.py` (`FakeKube`, Livy test server) | Acceptance pending (Kubernetes and Livy are covered by protocol tests: an API server and kubelet written for the tests, and a Livy test server; acceptance on a cluster with an enforcing CNI and on your Livy gateway: R05, R06) |
| 3.5 | Air-gapped: offline runtime and package repositories | `olcf runtime` offline export/import, wheelhouses, Maven mirror, `scripts/make_offline_bundle.sh`, `install.sh --offline-bundle` | `tests/node/test_runtime_lifecycle.py::test_offline_bundle_export_and_import_uses_no_network`, `test_maven.py`, `test_layers.py`, `tests/ops/test_updates.py` | Done |

## 4. Compatibility baseline

| ID | Requirement | Implementation | Tests | Status |
|---|---|---|---|---|
| 4.1 | Runtime 2.0 family: Spark 4.1, Java 21, Scala 2.13, Python 3.13, Delta 4.2 | manifest `olcf-rt-2.0.0`: Spark 4.1.1, Delta 4.2.0, Iceberg 1.11.0, Java 21, Scala 2.13, Python 3.13 (`olcf runtime matrix`) | `tests/node/test_runtime_lifecycle.py`, `tests/compute/test_runtimes.py`; real Spark 4.1.1 in `tests/compute/test_spark_smoke_real.py` | Partial (Spark starts; Delta jars for Spark come from Maven Central: `scripts/verify_spark_delta.py` checks them on a networked machine) |
| 4.2 | Runtime 1.3 family (Spark 3.5.5, Java 11, Scala 2.12, Python 3.11, Delta 3.2) | manifest `olcf-rt-1.3.0` | manifest and matrix tests | Acceptance pending |
| 4.3 | Notebook definition via ipynb and FabricGitSource; `notebook-content.py` | `notebooks/formats.py`, `fabric/rest.py` | `tests/items/test_formats.py`, `test_roundtrip_samples.py`, `tests/fabric/test_rest.py` (local Fabric test server) | Acceptance pending for the REST part |
| 4.4 | `notebookutils.runtime.context`; `getDefinition` | shim and SDK | `tests/fabric/test_shim_runner.py`, `test_sdk.py` | Acceptance pending |
| 4.5 | Livy as optional Fabric-compute provider | `compute/providers/livy.py`, `fabric.py`: a per-operation retry policy (an ambiguous `POST` is never repeated, `submission_uncertain` is reconciled by name `olcf-<job>-a<attempt>`), lexical SQL splitting, shared parameter injection, `source.main_class`/`args`, Spark `conf` pass-through | `tests/compute/test_providers_livy.py`, `test_review_livy.py` (Livy test server) | Acceptance pending (how a Livy gateway reports a statement it accepted just before a dropped connection is an assumption, R06) |
| 4.6 | OneLake via supported Entra and storage interfaces | `storage/providers/onelake.py`, `fabric/entra.py` | `tests/fabric/test_onelake.py`, `test_entra.py` (local test servers) | Acceptance pending |
| 4.7 | Do not claim parity with Fabric optimisations | documentation and labels | n/a | Process |

## 5-6. Architecture and functional domains

| ID | Requirement | Implementation | Tests | Status |
|---|---|---|---|---|
| 5.1 | Layered architecture: clients, gateway with identity and RBAC, control plane services, compute broker with providers, worker fleet, data layer | [Architecture](../concepts/architecture.md), [repository map](../development/repository-map.md) | `tests/integration/` | Done |
| 5.2 | Fabric-specific logic only in the adapter and compatibility packages | import scan: `compute`, `storage`, `catalog` do not import `fabric` | repository-map diagram generated from an import scan | Done |
| 6.1 | Workspace and project domain: multi-tenant, folders, items, ownership, roles, settings, quotas, tags | `api/core/workspaces.py` | `tests/unit/test_foundation_smoke.py`, `tests/security/test_authorization.py`, `tests/items/` | Partial (a workspace carries a free-form `quota` document that the scheduler enforces for compute; there are no storage or item-count quotas) |
| 6.2 | Notebook domain | `notebooks/` | `tests/items/`, `tests/e2e/test_02_sample_notebook.py` | Done (Scala and R cells are stored but not executed; see 8.2) |
| 6.3 | Lakehouse domain: Files and Tables explorer, managed and external tables, Delta and Iceberg metadata, optimisation | `storage/` | `tests/data/`, `tests/e2e/test_03_sql_dashboard_lakehouse.py` | Done |
| 6.4 | SQL domain | `sql/` | `tests/data/test_sql_service.py`, `test_sql_guard.py` | Partial (no JDBC or ODBC gateway; see 18.3) |
| 6.5 | Pipelines, environments, connections, compute, monitoring, administration | `pipelines/`, `notebooks/environments.py`, `api/core`, `compute/`, `observability/`, `ops/` | respective directories | Done |

## 7. Workspace and item model

| ID | Requirement | Implementation | Tests | Status |
|---|---|---|---|---|
| 7.1 | Workspace is the security, configuration and collaboration boundary | RBAC checks against the workspace stored with each object | `tests/security/test_authorization.py` (tenant isolation on every route), `test_references.py` | Done |
| 7.2 | Items have immutable ids, names, type, owner, timestamps, tags, description, folder, version and ETag; optional sensitivity metadata | `core/models.py`, `api/core/workspaces.py` | `tests/unit/test_foundation_smoke.py`, `tests/items/` | Done (the item create and update models carry a stored `sensitivity` field; classification labels of tables and columns are a separate model in the Intelligence module, rows 41.17 and 41.18, and are not written back to items) |
| 7.3 | Initial item types: Notebook, Lakehouse, SQL Query, Pipeline, Environment, Connection, Spark Job Definition, Variable Set, Schedule, extension | `ItemType`: notebook, lakehouse, sql_query, pipeline, environment, spark_job_definition, variable_set, schedule, report, extension; **connections are a separate object** (workspace-scoped or shared), not an item type | `tests/items/` | Partial (Connection is not an item; `report` was added) |
| 7.4 | CRUD and list APIs with permission checks for all item types | `/v1/workspaces/{ws}/items`, type-specific routes | `tests/security/test_authorization.py`, `tests/tooling/test_sdk_coverage.py` | Done |
| 7.5 | Git-friendly definitions without secrets | notebook source/ipynb, pipeline JSON, environment lock, connections without secrets | `tests/items/test_git_sync.py`, `tests/unit/test_foundation_smoke.py::test_export_import_roundtrip` | Done |
| 7.6 | Whole-workspace export and import | `/v1/workspaces/{ws}/export`, `/import` | `tests/unit/test_foundation_smoke.py`, `tests/items/test_import_remap.py` | Done |
| 7.7 | Soft delete, recycle bin and retention | `deleted_at`, restore routes, `ops/retention.py` | `tests/ops/test_retention.py`, `tests/items/` | Done |
| 7.8 | Git sync with branches, conflict detection, deployment promotion, no secrets in source control | `notebooks/gitsync.py`, `promotion.py` | `tests/items/test_git_sync.py`, `test_promotion.py` (real `git`) | Done |

## 8. Standalone notebook experience

| ID | Requirement | Implementation | Tests | Status |
|---|---|---|---|---|
| 8.1 | Browser editor on a Jupyter-compatible model | `web/static/js/notebook/`, CodeMirror 5 | `tests/e2e/test_02_sample_notebook.py` | Done |
| 8.2 | PySpark/Python and SQL first class; Scala architecturally supported | Python and Spark SQL cells run; `%%scala`, R and C# cells are preserved and reported as warnings, not executed (`notebooks/formats.py`) | `tests/items/test_formats.py` | Partial (Scala not executable) |
| 8.3 | Notebook metadata declares runtime, environment, default lakehouse, parameters, timeout, compute policy | `notebooks/service.py` | `tests/items/test_notebook_api.py` | Done |
| 8.4 | Interactive session and batch-run modes | `compute/sessions.py`, `jobs.py`. Python-engine notebooks, jobs and sessions also run on the built-in host-Python runtime (the worker's own interpreter, no Spark) when no installed runtime fits; Spark work never does | `tests/compute/test_sessions.py`, `test_jobs_flow.py`, `tests/node/test_kernels.py`, `tests/node/test_host_python.py`, `tests/compute/test_first_run_runtimes.py` | Done |
| 8.5 | Browser reconnects to the compute session after refresh | session WebSocket replays the newest outputs (`compute.session_replay_events`) | `tests/compute/test_sessions.py` | Done |
| 8.6 | Output types: text, bounded tables, safe HTML, images, charts, DataFrame previews | `js/notebook/` (sanitiser for HTML) | `tests/e2e/test_02_sample_notebook.py` | Done |
| 8.7 | Typed parameters from pipeline, schedule, API or Fabric adapter | parameters cell, `notebooks/` | `tests/items/`, `tests/orch/test_activities.py` | Done |
| 8.8 | Version history and Git diff in a source-friendly form | notebook versions, `.py` representation | `tests/items/test_notebook_api.py`, `test_git_sync.py` | Done |
| 8.9 | Import and export of ipynb and plain source | `notebooks/formats.py` | `tests/items/test_formats.py` | Done |
| 8.10 | Spark application and stage metrics and logs from a notebook run | `/v1/spark-applications`, `/v1/spark-ui`, `compute/spark_ui.py` | `tests/compute/test_spark_ui.py` | Done |

## 9. Fabric notebook compute-extension adapter

| ID | Requirement | Implementation | Tests | Status |
|---|---|---|---|---|
| FA-01 | SDK validates and submits the current notebook with one call | `olcf_fabric.run_remote` (`sdk/fabric`) | `tests/fabric/test_sdk.py`, `test_sdk_packaging.py` | Acceptance pending (confirm with `olcf.test_connection()` in your Fabric notebook) |
| FA-02 | Capture notebook, workspace, default lakehouse and run context | SDK context capture via `notebookutils.runtime.context` | `tests/fabric/test_sdk.py` | Acceptance pending (context keys are assumptions; see [assumptions](../guides/fabric-assumptions.md)) |
| FA-03 | Source through the in-notebook API or Fabric REST pull mode | SDK; `fabric/rest.py` | `tests/fabric/test_rest.py`, `test_e2e_pull_and_leaks.py` | Acceptance pending |
| FA-04 | Prefer FabricGitSource `.py`; retain original definition | `fabric/capsule.py` (`original` document) | `tests/fabric/test_capsule.py` | Acceptance pending |
| FA-05 | Signed, versioned capsule over HTTPS | `fabric/capsule.py`, `signing.py`; [ADR 0005](../adr/0005-execution-capsule.md) | `tests/fabric/test_capsule.py`, `test_signing.py`, `test_e2e_security.py` | Done (the signature protects integrity and replay, not authorship) |
| FA-06 | Whole-notebook remote execution is default | `run_remote` | `tests/fabric/test_e2e_flow.py` | Acceptance pending |
| FA-07 | Map Fabric-relative paths and catalog calls through shims | `runtime_lib/notebookutils/`, `runtime_lib/paths.py` | `tests/fabric/test_shim_runner.py`, `tests/node/test_runtime_lib.py` | Done for the implemented subset |
| FA-08 | Return job id, state, logs, exit value, result references, errors | submissions API, SDK `observe` | `tests/fabric/test_e2e_flow.py` | Acceptance pending |
| FA-09 | Fabric pipeline invocation propagates remote success, failure and exit value | notebook activity in Fabric calls the same SDK | `tests/fabric/test_sdk.py` | Acceptance pending (confirm with a pipeline run in your tenant) |
| FA-10 | No broker service-principal secret in the notebook source or payload | broker identity stored server-side; the capsule scanner rejects credentials | `tests/fabric/test_e2e_pull_and_leaks.py` | Done |

## 10. Execution capsule

| ID | Requirement | Implementation | Tests | Status |
|---|---|---|---|---|
| 10.1 | Schema version, SDK version, submission id, idempotency key, timestamps, source hash and signature | `fabric/capsule.py`, `schemas/capsule-v1.json` | `tests/fabric/test_capsule.py` (schema kept in sync with the models) | Done |
| 10.2 | Source, context, standalone context, runtime request, parameters (declared only), data references, policy, security claims, compatibility report | same | same | Done |
| 10.3 | Reusable secrets are references, never values | secret scanner `find_secrets` | `tests/fabric/test_e2e_pull_and_leaks.py` | Done |

## 11. Compatibility analyzer and shim runtime

| ID | Requirement | Implementation | Tests | Status |
|---|---|---|---|---|
| 11.1 | Parse Python AST and SQL; retain cell and line source maps | `fabric/analyzer/` | `tests/fabric/test_analyzer.py` | Done |
| 11.2 | Detect imports, magics, `notebookutils`/`mssparkutils`, Fabric paths, shell calls, dynamic installs, UDFs, native packages, external endpoints | 73 rules in `rules/fabric-2.0.yaml` | `tests/corpus/` (65 tests) | Done |
| 11.3 | Classification into native-portable, shim, rewrite, warning, Fabric-only fallback, unsupported | analyzer classes | `tests/fabric/test_analyzer.py` | Done |
| 11.4 | Do not translate ordinary PySpark | no rewrite for plain PySpark | corpus | Done |
| 11.5 | Versioned shims: context, lakehouse paths, filesystem helpers, notebook exit, variables, secrets, logging, selected utilities | `runtime_lib/notebookutils/` | `tests/fabric/test_shim_runner.py` | Done for the implemented subset |
| 11.6 | Unknown APIs fail closed with line, cell and remediation | analyzer blockers | corpus negative cases | Done |
| 11.7 | Transformations deterministic, testable, visible in diagnostics | report in the capsule and the submission | `tests/fabric/test_analyzer.py` | Done |
| 11.8 | Golden compatibility corpus per supported runtime | `tests/corpus/` | `tests/corpus/test_corpus.py` | Partial (one ruleset, `fabric-2.0`; no per-runtime corpus for 1.3; golden outputs are run on the local Python engine and for a Spark subset, not on a Fabric runtime) |

## 12. Runtime system

| ID | Requirement | Implementation | Tests | Status |
|---|---|---|---|---|
| 12.1 | Manifest with id, version, Spark, Delta, Iceberg, Java, Scala, Python, OS compatibility, connectors, libraries, hashes and signatures | `runtime/manifest.py`, `manifests/*.json` | `tests/node/test_manifest_signing.py` | Done |
| 12.2 | Fabric-2.0-compatible family | `olcf-rt-2.0.0` | see 4.1 | Partial |
| 12.3 | Fabric-1.3-compatible family (optional) | `olcf-rt-1.3.0` | see 4.2 | Acceptance pending |
| 12.4 | Open runtime advancing independently | `olcf-rt-native-1.0.0` (`olcf-native`) | manifest tests | Done |
| 12.5 | Global bundle immutable; per-job dependencies layered separately | `notebooks/layer.py`, `worker/jobs.py` | `tests/node/test_layers.py` | Done |
| 12.6 | Side by side, scheduler places only compatible work | planner constraints; the host-Python runtime (`olcf-host-python`, `engines = [python]`) is never offered to Spark work and a queued job states why it waits (`queue_reason`); a queued Python-engine job whose runtime no online worker has is re-bound to the host runtime (`runtime_fallback` with `rebound: true`) | `tests/compute/test_compute_planner.py`, `tests/compute/test_first_run_runtimes.py`, `tests/compute/test_host_python_rebind.py` | Done |
| 12.7 | Offline import of signed packages | `runtime/manager.py` export/import | `tests/node/test_runtime_lifecycle.py` | Done |
| 12.8 | Rollback; prior bundle remains until retired | `state.json` history, retire and rollback | `tests/node/test_runtime_lifecycle.py` | Done |
| 12.9 | Signed bundles | Ed25519; the shipped manifests are signed with a development key whose private half is in the repository. A production installation (`environment` unset or `production`) does **not** trust that key (`runtime.trust.allow_development_key`, `runtime.trust.dir` / `OLCF_RUNTIME_TRUST_DIR`, `python -m olcf.runtime trust list|add|remove`, health check `runtime-trust`) | `tests/node/test_manifest_signing.py`, `tests/security/test_runtime_trust.py` (25) | Partial (secure default, but the shipped manifests must be re-signed with the operator's own key before they install in production; the check is advisory and does not stop the server; the Helm chart mounts a trust directory (`runtimeTrust.*`) and the Docker `BAKE_RUNTIME` build either trusts the development key in the throwaway build stage only or installs an operator-signed manifest with `RUNTIME_TRUST_DIR`; see [ADR 0004](../adr/0004-signed-immutable-runtimes.md)) |

## 13-14. Distributed compute fabric and provider interface

| ID | Requirement | Implementation | Tests | Status |
|---|---|---|---|---|
| 13.1 | Worker agent on every machine, enrolled with identity and certificate | `worker/`, `compute/hub.py`, `security/pki.py`; certificates and tokens renew over the authenticated channel (`olcf worker renew`, `compute.worker_cert_*`) | `tests/node/test_agent_protocol.py`, `tests/compute/test_workers.py`, `tests/security/test_tls.py`, `tests/compute/test_worker_credentials.py`, `tests/node/test_credential_renewal.py`, `tests/compute/test_real_agent.py` | Done (enrolment and identity; the renewal protocol is covered by test agents and by the real agent over plain HTTP; the mTLS handshake with a renewed certificate is a readiness item (R10); worker tokens still never expire unless `compute.worker_token_max_age_days` is set) |
| 13.2 | Worker reports cores, usable RAM, disk, network labels, accelerators, runtimes, OS, agent version, health | `worker/inventory.py`, `compute/protocol.py` | `tests/node/test_probe.py`, `tests/compute/test_workers.py` | Done (GPU fields are reported; no GPU-aware scheduling was tested) |
| 13.3 | Real Spark applications whose driver and executors may span nodes | `compute/providers/standalone.py` | `tests/node/test_spark_cluster.py`, `tests/compute/test_spark_smoke_real.py` | Acceptance pending (several agents on one host) |
| 13.4 | Spark Standalone for simple on-premises; Kubernetes as first-class provider | `standalone.py`, `kubernetes.py`; the Helm NetworkPolicy now opens exactly the configured Spark ranges (`spark.ports`) | `tests/compute/test_standalone.py`, `test_providers_k8s.py`, `test_review_k8s.py`, `test_review_ports.py`, `tests/tooling/test_deploy.py` | Acceptance pending (Kubernetes covered by `FakeKube`, the chart rendered by a bundled interpreter; run `kubeconform` and `helm lint` on your values and check the policy with your CNI, R05) |
| 13.5 | Labels: site, rack, environment, GPU, locality, zone, runtime | pools and labels | `tests/compute/test_compute_planner.py` | Done |
| 13.6 | Drain and cordon without killing running work | scheduling states | `tests/compute/test_workers.py` | Done |
| 13.7 | Admission reserves OS and broker headroom; rejects unsafe oversubscription | `compute.reserve_cores` (live: a change takes effect within seconds, whole numbers 0 to 64), `compute.reserve_memory_fraction` | `tests/compute/test_compute_planner.py`, `test_fix_reserve_and_detail.py` | Done |
| 13.8 | Dynamic allocation where supported | `providers/base.py::dynamic_allocation_conf`, Kubernetes `scale` | `tests/compute/test_providers_k8s.py` | Acceptance pending (the configuration is produced; confirm on your cluster) |
| 13.9 | FIFO, priority queues, quotas, fair share, per-workspace limits, reserved pools | `compute/scheduler.py`, `planner.py`, queues | `tests/compute/test_queues.py`, `test_compute_planner.py` | Done |
| 13.10 | Failures give deterministic state and configurable retry, never silent duplicate writes | `compute/failures.py` (retry rule), idempotent replay only for idempotent jobs; remote providers never report success without a result (`result_missing`), an uncertain Livy submission is reconciled instead of repeated, restore reconciles unfinished jobs (`restore_reconcile`) | `tests/compute/test_reconcile.py`, `tests/compute/test_review_k8s.py`, `test_review_livy.py`, `tests/ops/test_restore_reconcile.py` | Done |
| 14.1 | Provider operations `discover`, `submit`, `status`, `logs`, `cancel`, `scale`, `ui`, `reconcile` | `compute/providers/base.py`; each provider declares its `result_contract` (`runner` or `provider`) | provider tests; contract in [extending](../development/extending.md) | Done (the interface; the Kubernetes, Livy and Fabric implementations are Acceptance pending, see 13.4 and 4.5; a shared contract test suite for third-party providers is not shipped) |

## 15-16. Storage, Delta and Iceberg

| ID | Requirement | Implementation | Tests | Status |
|---|---|---|---|---|
| 15.1 | StorageProvider for OneLake/ADLS Gen2, S3, MinIO, local; pluggable | `storage/providers/` (local, s3 and minio, adls, onelake) and the `olcf.storage_providers` entry point | `tests/data/test_storage_local.py`, `test_s3_moto.py`, `test_storage_azure.py`, `test_storage_plugins.py` | Acceptance pending for ADLS and OneLake (local test endpoints); S3 against moto; MinIO uses the S3 provider |
| 15.2 | Lakehouse with Files and Tables namespaces and metadata linking root, format and catalog | `storage/lakehouse.py` | `tests/data/` | Done |
| 15.3 | Canonical internal URI decoupling notebooks from provider addressing | `olcf://` URIs and `/lakehouse/default` | `tests/data/`, `tests/fabric/test_shim_runner.py` | Done |
| 15.4 | Credentials resolved server-side from Connection and Secret references | `storage/service.py`, `security/secrets.py` | `tests/security/test_leaks.py` | Done |
| 15.5 | Managed and external tables | lakehouse tables and external locations | `tests/data/test_delta_tables.py` | Done |
| 15.6 | Delta and Iceberg first class; Parquet, ORC, CSV, JSON as file formats | `storage/tables.py`, `iceberg.py`; Parquet, CSV, JSON, NDJSON handled | `tests/data/test_delta_tables.py`, `test_iceberg.py` | Partial (ORC is not handled; Iceberg needs the optional `pyiceberg` package) |
| 15.7 | Storage browser: list, upload and download, preview, delete, move and copy, ACL visibility per capability | storage routes under `/v1/lakehouses`, console lakehouse page | `tests/data/`, `tests/e2e/test_03_sql_dashboard_lakehouse.py` | Partial (the ADLS provider advertises an `acl` capability, but no API route or console view uses it) |
| 15.8 | Bulk data does not pass through the control plane | workers read and write storage directly; only explicit upload and download use the API | `tests/integration/test_product_e2e.py` | Done |
| 16.1 | Runtime compatibility matrix includes table protocol and features | `olcf runtime matrix` (`delta_protocol`) | `tests/data/test_delta_policy.py` | Done |
| 16.2 | Inspect protocol and features before write; block upgrades that would break Fabric interoperability | `storage/delta_policy.py` | `tests/data/test_delta_policy.py` | Done (the policy encodes assumptions about Fabric's supported features) |
| 16.3 | Test append, overwrite, merge, delete, update, schema evolution, partitioning, liquid clustering/Z-order, checkpoints, concurrent readers and writers | delta-rs and DuckDB paths; `scripts/verify_spark_delta.py` for Spark | `tests/data/test_delta_tables.py` | Partial (delta-rs and DuckDB paths tested; the Spark-with-Delta matrix is run by `scripts/verify_spark_delta.py`) |
| 16.4 | Iceberg catalog choice, snapshots, time travel, partition evolution, compaction | `storage/iceberg.py` | `tests/data/test_iceberg.py` | Partial (discovery and read; no compaction or partition evolution management) |
| 16.5 | Cross-engine interoperability tests as release gates | `scripts/verify_spark_delta.py` in the CI `delta` job (pushes only) | none run | Acceptance pending |
| 16.6 | Vacuum and destructive maintenance need permissions and safety thresholds | table maintenance with safeguards | `tests/data/test_delta_tables.py` | Done |

## 17-18. Catalog and SQL

| ID | Requirement | Implementation | Tests | Status |
|---|---|---|---|---|
| 17.1 | Central logical catalog of catalogs, schemas, tables, views, functions with immutable ids | `catalog/service.py` indexes tables and files per lakehouse; entries have immutable ids | `tests/data/test_catalog.py` | Partial (no view or function objects, no schema hierarchy beyond what storage provides) |
| 17.2 | Default catalog and lakehouse semantics for unqualified references | `catalog.resolve`, `/lakehouse/default` | `tests/data/test_sql_service.py` | Done |
| 17.3 | Adapters to Hive-compatible, REST, Glue, Unity catalogs | none beyond Iceberg discovery | | Gap |
| 17.4 | Metadata: ownership, tags, descriptions, schema snapshots, statistics, lineage edges | `catalog/` | `tests/data/test_catalog.py`, `test_catalog_lineage.py` | Done |
| 17.5 | Catalog APIs independent of the notebook UI | `/v1/catalog`, `/v1/catalogs` | `tests/data/` | Done |
| 17.6 | Fabric adapter maps default lakehouse without contaminating the core | shim in `runtime_lib` | `tests/fabric/test_shim_runner.py` | Done |
| 18.1 | Browser SQL editor with autocomplete, schema explorer, parameters, saved queries, history, cancellation, export | `sql/`, console SQL page | `tests/data/test_sql_service.py`, `tests/e2e/test_03_sql_dashboard_lakehouse.py` | Done |
| 18.2 | Pluggable SQL engine; Spark SQL baseline; DuckDB-like providers allowed | DuckDB (embedded) and Spark SQL engines | `tests/data/test_sql_service.py`, `tests/e2e/test_05_spark_smoke.py` | Partial (the engine list is fixed in code; Spark SQL needs a worker with a runtime; no Trino or StarRocks provider) |
| 18.3 | JDBC and ODBC access through a gateway | none | | Gap |
| 18.4 | Endpoints enforce workspace, catalog permissions and resource policies | `sql.execute` permission, per-query caps and concurrency limits; a host guard (`sql/guard.py`) refuses reflection, script transformation, file paths, `LOCATION`, `CREATE FUNCTION`, `SET`, `${...}`, non-allowlisted scalar and table functions and qualified function calls for every role, accepts only one fully parsed statement (no unparsed-text fallback), and re-checks the rendered SQL | `tests/security/test_authorization.py`, `tests/data/test_sql_host_guard.py` (138), `tests/compute/test_sql_session_guard.py` | Done (Spark scalar functions are an allowlist since C01; the Spark door is tested with the planner and test sessions) |
| 18.5 | History records user, source, duration, rows, engine, runtime, failure category | `sql/history.py` | `tests/data/test_sql_service.py` | Done |
| 18.6 | No pretence of Fabric Warehouse semantics | no Warehouse adapter | | Done (by omission) |

## 19-21. Pipelines, connections, environments

| ID | Requirement | Implementation | Tests | Status |
|---|---|---|---|---|
| 19.1 | DAG with typed activities, dependencies, conditions, fan-out and fan-in | `pipelines/model.py`, `engine.py` | `tests/orch/test_engine.py`, `test_validation.py` | Done |
| 19.2 | Activities: notebook, Spark job, SQL, copy, script, HTTP, child pipeline, wait, variable and control flow | 15 built-in types in `pipelines/activities/` | `tests/orch/test_activities.py` | Done |
| 19.3 | Typed parameters, variables, secret references, activity outputs | `pipelines/expressions.py` | `tests/orch/test_expressions.py` | Done |
| 19.4 | Triggers: manual, schedule, event or webhook, API | `pipelines/triggers.py` (schedule, webhook, storage event, dependency), run API | `tests/orch/test_triggers.py`, `test_webhooks.py` | Done |
| 19.5 | Per-activity retry and backoff, timeout, cancellation, resume from failure where safe | engine; [ADR 0009](../adr/0009-durable-pipeline-engine.md); a timeout enters a cancelling phase (`policy.cancelGraceSeconds`) and an attempt that may still run or whose outcome is unknown ends `timeout_uncertain` and is retried only if idempotent (`Idempotency-Key` for `http`) | `tests/orch/test_engine.py`, `test_recovery.py`, `tests/orch/test_timeout_overlap.py` (23) | Done (local activities and a local HTTP server; whether a real remote system honours an idempotency key is outside what was tested) |
| 19.6 | Pipeline and workspace concurrency limits and queue policies | `settings.concurrency`, `queuePolicy` | `tests/orch/test_engine.py` | Done |
| 19.7 | Run graph, timings, logs, inputs and outputs metadata, lineage events | run API and console; copy emits lineage | `tests/orch/test_api_e2e.py`, `tests/e2e/test_07_pipelines.py` | Done |
| 19.8 | Definitions stored as versioned YAML or JSON, Git-friendly | pipeline items (`olcf.pipeline/1` JSON) | `tests/items/test_git_sync.py` | Done (JSON; YAML is not offered) |
| 19.9 | Import a documented subset of Fabric pipelines; unsupported activities explicit | `pipelines/fabric_import.py` | `tests/orch/test_activities.py`, `test_validation.py` | Acceptance pending (against hand-written samples) |
| 19.10 | Plugin SDK for activity types and connectors | `olcf.pipeline_activities` entry point | a test plug-in was installed for the documentation; `tests/orch/test_activities.py` | Partial (activities only; no connector SDK) |
| 20.1 | Connection metadata and secret material are separate objects | `connections` table; secrets by id | `tests/security/test_leaks.py` | Done |
| 20.2 | Connectors for OneLake and ADLS, S3 and MinIO, JDBC and ODBC, SQL Server, PostgreSQL, MySQL, HTTP, local and SFTP | storage connections: `local`, `s3`, `minio`, `adls`, `onelake`; the HTTP activity; SFTP only as a backup target | `tests/data/` | Partial (no JDBC, ODBC, SQL Server, PostgreSQL or MySQL connection types; notebooks may use drivers installed in an environment) |
| 20.3 | Connector SDK: schema discovery, test connection, read and write capability, credential types, copy optimisation | storage provider interface (capabilities, `validate_config`, connection testers) | `tests/data/test_storage_plugins.py` | Partial (storage only) |
| 20.4 | Secrets never in exports or Git | export strips secrets, Git sync scans | `tests/items/test_git_sync.py`, `tests/security/test_leaks.py` | Done |
| 20.5 | Connections workspace-scoped or shared with explicit permissions | org-level `connection.manage` for shared | `tests/security/test_authorization.py` | Done |
| 20.6 | Copy: chunking, retries, checksums or row counts, resumability | `pipelines/activities/copy.py` | `tests/orch/test_activities.py` | Done (lakehouse to lakehouse only) |
| 21.1 | Environment selects a base runtime and an immutable dependency lock | `notebooks/environments.py`, `piplock.py` (`olcf.envlock/1`) | `tests/items/test_environments.py` | Done |
| 21.2 | Lock files and approved indexes or wheelhouses; pinned JARs with allow-list | hash-locked requirements; Maven coordinates pinned in the lock | `tests/items/test_environments.py`, `tests/node/test_maven.py` | Done |
| 21.3 | Per-job layer keyed by dependency hash, cached | `notebooks/layer.py` | `tests/node/test_layers.py` | Done |
| 21.4 | Notebook-time package mutation converted or blocked | analyzer rules (`%pip`, `!pip`) | `tests/corpus/` | Done |
| 21.5 | Native packages validated against worker OS, architecture, Python ABI | wheel tags are resolved for the target platform in the lock | `tests/items/test_environments.py` | Partial (resolution for the lock's platform; no check on each worker) |
| 21.6 | Administrators can deny public package installation and require mirrors | setting `allow_public_package_install` and index settings | `tests/items/test_environments.py` | Done |
| 21.7 | Every job records the environment and runtime fingerprint | job record; for the host-Python runtime the fingerprint the placed node reported (Python, platform, library versions) and the package versions in `result.json` | `tests/compute/test_jobs_flow.py`, `tests/node/test_host_python.py` | Done |

## 22-24. Identity, secrets, API

| ID | Requirement | Implementation | Tests | Status |
|---|---|---|---|---|
| 22.1 | Local bootstrap admin for initial setup; production OIDC/OAuth2 and optional LDAP/AD through a bridge | `security/federation*.py`, `ldap_bridge.py` | `tests/security/test_federation_flow.py`, `test_federation_attacks.py`, `test_ldap_bridge.py` (mock IdP) | Acceptance pending against a real identity provider |
| 22.2 | Entra identities, service principals, managed identity for the Fabric adapter | `fabric/auth.py`, `entra.py` | `tests/fabric/test_entra.py` (local test server) | Acceptance pending |
| 22.3 | Composable RBAC scoped to organisation, workspace, item, connection, runtime, pool, admin domains | bindings can be created at org, workspace, item, connection, runtime and pool scope; permissions per domain | `tests/security/test_authorization.py` | Partial (only routes that pass the item, connection or runtime id to `require` honour a narrower binding; pool scope was not verified in any route) |
| 22.4 | The eight minimum roles | PlatformAdmin, SecurityAdmin, RuntimeAdmin, WorkspaceAdmin, Contributor, Operator, Viewer, ServicePrincipal | `tests/security/test_authorization.py` | Done |
| 22.5 | Authorisation enforced server-side on every API | `principal.require(...)`; sweeps over every operation of the OpenAPI document, including that a session that must change its password is refused everywhere except `POST /v1/identity/password`, and that a logged-out session is refused everywhere; job, kernel and pipeline callback credentials are derived from, and capped by, the submitting credential (ceiling, current bindings, one workspace, one attempt, origin revocation), and `POST /v1/identity/tokens` refuses scope escalation | `tests/security/test_authorization.py`, `test_unauthenticated.py`, `test_session_hardening.py`, `tests/compute/test_job_credentials.py` (11), `tests/orch/test_pipeline_credentials.py` (3) | Done (controller and API with test agents; logout ends running jobs' callbacks by design) |
| 22.6 | ABAC hooks (future) | none | | Gap (the requirement says "future") |
| 23.1 | Secrets in an OS-protected store for single node, or a vault or KMS for enterprise | encrypted store with the master key from environment, OS keyring (optional extra) or a 0600 file; HashiCorp Vault | `tests/security/`, `tests/unit/test_foundation_smoke.py` | Partial (Vault tested against a local test server) |
| 23.2 | Vault provider interface: Windows protection, HashiCorp Vault, cloud key vaults as plugins | `SecretProvider` base; only `file` and `vault` implemented | | Partial (no DPAPI, Azure Key Vault or AWS provider) |
| 23.3 | Opaque ids, just-in-time resolution | `SecretStore.resolve` | `tests/security/test_leaks.py` | Done |
| 23.4 | Client certificates and workload identity to reduce long-lived secrets | mTLS for workers; managed identity for ADLS and OneLake; worker certificates renew automatically | `tests/security/test_tls.py` | Partial (mTLS is optional; the renewed-certificate handshake is a readiness item, R10) |
| 23.5 | Rotation without editing notebooks or pipelines | secrets are referenced by id; `POST /v1/secrets` rotation of a value; master key and JWT signing key rotate through key rings (`olcf security keys|rotate`, `admin.keys`, resumable re-encryption, `key-rotation` health check) | `tests/security/test_key_rotation.py` (30), `tests/ops/test_key_ring_backup.py`, `tests/security/test_keyring_concurrency.py`, `tests/tooling/test_cli_security.py` | Done (single host: threads, processes and one local PostgreSQL 16; no multi-node PostgreSQL deployment) |
| 23.6 | Redaction in logs, support bundles, telemetry, error traces | `core/redaction.py`, leak scan in `ops/leakscan.py` | `tests/security/test_leaks.py`, `tests/ops/test_diagnostics.py` | Done |
| 24.1 | Versioned REST/OpenAPI as the canonical contract | `/v1`, 329 paths and 407 operations in `docs/api/openapi.json` | `tests/unit/test_openapi.py` | Done |
| 24.2 | Python SDK wrapping the API with notebook conveniences | `olcf.sdk`, `olcf-fabric` | `tests/tooling/test_sdk_*.py` | Done |
| 24.3 | CLI: login and context, workspace and item CRUD, jobs, runtime and worker admin, export and import, diagnostics | `olcf` | `tests/tooling/test_cli.py` | Done |
| 24.4 | Idempotency keys on mutating long-running operations | `Idempotency-Key` handling in `api/deps.py` | `tests/compute/test_jobs_api.py` | Done |
| 24.5 | Pagination, filtering, sorting, ETags and optimistic concurrency | cursor pagination, ETags and `If-Match` | `tests/unit/test_foundation_smoke.py`, `tests/items/` | Partial (sorting is available only on some lists) |
| 24.6 | Long-running operations with operation ids and polling or events | `/v1/operations/{id}`, job and run resources, SSE event stream | `tests/ops/test_backup_api.py`, `tests/compute/test_jobs_api.py` | Done |
| 24.7 | Webhook and event stream for job, pipeline, item, worker and security events | `core/events.py`, `/v1/events/stream`, `/v1/webhooks` | `tests/orch/test_webhooks.py` | Done |

## 25-26. Web console, installer and packaging

| ID | Requirement | Implementation | Tests | Status |
|---|---|---|---|---|
| 25.1 | Pages: Overview, Workspaces, Fabric Integration, Storage and Lakehouses, Runtimes, Compute, Jobs, Pipelines, Security, Updates, Diagnostics, Backup and Restore | `web/static/js/pages/` (home, workspaces, lakehouse, runtimes, compute, jobs, pipelines, triggers, alerts, fabric, security, ops, connections, envs, reports, sql, notebook, agents, about); a remembered sign-in survives a reload and a token refresh (`web/static/js/api/client.js`) | `tests/e2e/` (15 files, 55 tests, headless Chromium), `tests/web/test_remember_auth.py` (Node unit test, 4 of 6 scenarios fail on the pre-fix client), `tests/e2e/test_15_remember_me.py` | Done (one browser engine; multi-tab refresh rotation is not addressed) |
| 25.2 | A web application, not a desktop GUI; desktop shortcuts may open the URL | served by the control plane | `tests/e2e/` | Done |
| 26.1 | Signed Windows installer; Linux packages and container images | `scripts/install.ps1`, `install.sh`; Dockerfiles; Helm chart; release signs artifacts with sigstore | `tests/tooling/test_installer.py` (Linux, including a Mac without developer tools), `test_deploy.py` | Partial (no MSI or other signed Windows installer; no deb or rpm; scripts are signed by the release workflow; the Docker `BAKE_RUNTIME` build and the chart's `runtimeTrust` values are tested statically and by rendering) |
| 26.2 | Windows services auto-start under least privilege; Manager, Logs and Uninstall shortcuts | `olcf service install` creates a per-user scheduled task on Windows; no shortcuts; the control plane starts without POSIX-only modules (`olcf.core.filelock` with `msvcrt`) | `tests/tooling/test_service_files.py`, `tests/unit/test_filelock.py`, `test_windows_startup.py` (POSIX modules hidden), CI job `windows` | Partial (the Windows path is covered by Linux-hosted tests and the CI job `windows`; Windows acceptance: R08) |
| 26.3 | Core installer small; large runtimes installed separately from signed manifests | `olcf runtime install`. The Python package itself is not small: with `ipykernel` and `pandas` as base dependencies the installed dependencies measure roughly 650 MB on Linux/CPython 3.13 (about 460 MB before those two, about 200 MB for them; `matplotlib` is the optional `plots` extra, about 90 MB more) | `tests/node/test_runtime_lifecycle.py` | Done (Spark runtimes are separate; sizes on other platforms not measured) |
| 26.4 | All-in-one mode: control plane, worker, local object store option, runtime | `olcf serve` embedded worker; local lakehouse storage | `tests/integration/` | Done (no bundled MinIO) |
| 26.5 | Cluster mode with separate control plane and worker installers | `olcf-worker`, `deploy/` | `tests/tooling/test_deploy.py` | Acceptance pending (Compose and Helm rendered) |
| 26.6 | Runtime install is transactional and never downloads unpinned components | staging and atomic rename; hashes | `tests/node/test_runtime_lifecycle.py` | Done |
| 26.7 | Offline bundle import | see 3.5 | | Done |
| 26.8 | Upgrade with preflight, migration, drain, rollback, compatibility checks | `ops/updates.py`, `ops/upgrade.py`, `olcf upgrade` | `tests/ops/test_updates.py`, `test_updates_apply.py`, `tests/tooling/test_installer.py` | Done on Linux |
| 26.9 | Uninstaller preserves or removes metadata, secrets, logs, storage, caches independently | `scripts/uninstall.sh` and `.ps1` categories | `tests/tooling/test_installer.py` | Done on Linux; `uninstall.ps1` is structure-checked |
| 26.10 | SBOM and third-party licence inventory for every release and runtime bundle | `scripts/build_release.sh` (CycloneDX SBOM for `olcf` and for the `olcf-fabric` SDK, `licenses.py`; a placeholder `License: UNKNOWN` falls through to the classifiers) | `tests/tooling/test_release_artifacts.py`; the whole build script was run locally (both distributions, SBOMs, notices, `SHA256SUMS`) | Partial (release artifact only; runtime bundles are covered by their hash-pinned manifests; vendored JavaScript and the JDK/Maven manifests are not audited beyond the notices, readiness R11) |

## 27. High availability and state

| ID | Requirement | Implementation | Tests | Status |
|---|---|---|---|---|
| 27.1 | SQLite for single node; PostgreSQL for enterprise | `db/engine.py` | `tests/ops/test_ha.py`, `test_backup_pg.py` | Done |
| 27.2 | Multiple stateless API replicas behind a load balancer | `olcf serve --workers N`, Helm | `tests/ops/test_ha.py` (several processes on PostgreSQL) | Acceptance pending (several processes on one PostgreSQL server on one host; no load balancer, no multi-machine drill) |
| 27.3 | Leader election and distributed locks | `core/leader.py` | `tests/ops/test_ha.py` | Acceptance pending (measured takeover 15.9 s after SIGKILL on PostgreSQL 16, processes on one host) |
| 27.4 | Queue and event backend abstracted; PostgreSQL, Redis, RabbitMQ or equivalent | PostgreSQL (or SQLite) only; the `worker_commands` outbox | | Partial (no Redis or RabbitMQ implementation) |
| 27.5 | Workers reconnect after failover and reconcile | `hello` reconcile | `tests/compute/test_reconcile.py`, `tests/ops/test_ha.py` | Done |
| 27.6 | No Spark application is killed because an API node restarts | applications run on workers; controller adopts | `tests/compute/test_reconcile.py`, `tests/node/test_agent_jobs.py` | Done |
| 27.7 | Transactional, versioned, reversible-where-feasible migrations | `db/migrate.py` streams, `olcf db rollback` | `tests/ops/` | Done |

## 28-30. Observability, governance, backup

| ID | Requirement | Implementation | Tests | Status |
|---|---|---|---|---|
| 28.1 | Structured logs with correlation ids and redaction | `core/logging.py`, middleware | `tests/security/test_leaks.py` | Done |
| 28.2 | Metrics for API, queue, jobs, workers, executors, Spark stages and tasks, storage and auth failures | 47 Prometheus families at `/v1/metrics` | `tests/orch/test_obs_metrics.py`, `tests/fabric/test_metrics.py` | Partial (no Spark stage, shuffle, spill or GC metrics; the Spark UI is linked instead) |
| 28.3 | Secure Spark UI access through a proxy or signed route | `/v1/spark-ui`, `compute/spark_ui.py` | `tests/compute/test_spark_ui.py` | Done |
| 28.4 | OpenTelemetry-compatible export | `observability/otel.py` | `tests/orch/` | Done |
| 28.5 | Job timeline correlating workspace, item, run, job, provider app, Spark app, fingerprints | `olcf job timeline` | `tests/compute/test_jobs_api.py` | Done |
| 28.6 | Alert rules: failed jobs, queue saturation, worker loss, disk pressure, runtime mismatch, auth expiry, control-plane health | 9 rule types; default rules (10) | `tests/orch/test_alerts.py` | Partial (no rule covers worker certificate expiry) |
| 28.7 | Retention by age and size | `ops/retention.py` | `tests/ops/test_retention.py` | Partial (retention is by age, per data class; there is no size-based retention) |
| 29.1 | Lineage events for reads and writes where deterministically discoverable; column lineage where provable | `catalog/lineage.py` (sqlglot), copy activity edges; the Intelligence module adds notebook, pipeline, report and run-time lineage down to columns (rows 41.11 to 41.15) | `tests/data/test_catalog_lineage.py`; `tests/intelligence/lineage/`, `tests/intelligence/test_runtime_facts.py` | Partial (the classic catalog's own lineage is SQL and copy only; the broader coverage, with its limits, is in [lineage support](../reference/lineage-support.md) and is rated in section 41) |
| 29.2 | Audit of login, permission change, secret and config change, item CRUD, runtime install, worker enrollment, job submit and cancel, destructive data actions | `security/audit.py` | `tests/unit/test_foundation_smoke.py`, `tests/security/` | Done |
| 29.3 | Audit append-oriented and exportable to a SIEM | hash chain; JSONL and CEF export; [ADR 0008](../adr/0008-hash-chained-audit-log.md) | `tests/ops/test_retention.py` | Done |
| 29.4 | Tag and classification framework for external scanners | tags on items and catalog entries; in Intelligence, classification labels with a review queue, propagation along lineage and a sensitive-data detector (`intelligence/governance/classification.py`, `propagation.py`, `intelligence/discovery/detect.py`) | `tests/data/test_catalog.py`; `tests/intelligence/governance/test_classification.py`, `tests/intelligence/discovery/test_detect.py` | Partial (the framework exists in the Intelligence module, rows 41.17 and 41.18; external scanners can contribute through the scanner import, not through a public plug-in interface, and the classic catalog still has only tags and the item `sensitivity` field) |
| 29.5 | Unknown or partial lineage marked explicitly | lineage edges carry how they were derived | `tests/data/test_catalog_lineage.py` | Done |
| 30.1 | Back up metadata, configuration, encrypted secret rows, item definitions and Git state | logical backup of every table; `config`, key files optional | `tests/ops/test_backup_core.py`, `test_backup_api.py` | Done |
| 30.2 | Object data follows the storage provider's strategy | not included by design | | Done |
| 30.3 | Scheduled encrypted backups with retention and an off-host target | local, S3, SFTP and lakehouse targets | `tests/ops/test_backup_targets.py` (moto, a local SFTP server) | Done (S3 against moto only) |
| 30.4 | Restore in a clean environment with documented key and certificate prerequisites | `restore`, `prerequisites`; unfinished jobs are failed with category `restore_reconcile` (`ops/restore_reconcile.py`), reported by `restore`, the online restore and `restore-test` (`job-reconcile`) | `tests/ops/test_backup_api.py`, `tests/tooling/test_cli_backup_restore.py`, `tests/ops/test_restore_reconcile.py` (9) | Acceptance pending (restores into a second data directory on the same machine, SQLite only; no clean host with real worker agents, no PostgreSQL restore reconcile; sessions and kernels are not reconciled) |
| 30.5 | Regular automated restore tests | `restore-test` endpoint and scheduled run | `tests/ops/test_backup_api.py` | Done (SQLite and a local PostgreSQL; the restore test restores into a scratch database) |
| 30.6 | Workers replaceable; no state only on a worker disk | job records and logs on the controller | `tests/compute/test_reconcile.py` | Done |

## 31-32. Security hardening and networking

| ID | Requirement | Implementation | Tests | Status |
|---|---|---|---|---|
| 31.1 | TLS everywhere; mTLS for workers and production remote administration | built-in or own certificate; optional worker mTLS; reverse proxy supported | `tests/security/test_tls.py` | Partial (TLS is optional and off by default) |
| 31.2 | Signed installers, images and runtime bundles; hashes; SBOM; dependency scanning | release workflow (sigstore, SBOM; the CI, container and security workflows are release gates through `workflow_call`), `security.yml` (pip-audit `--disable-pip --no-deps`, OSV-Scanner, CodeQL, gitleaks with `.gitleaks.toml`) | `tests/tooling/test_release_workflows.py` pins the structure; `actionlint` passes | Acceptance pending (repository dry run: R02) |
| 31.3 | Least-privilege service accounts, isolated workdirs, restricted filesystem and network, optional containers | per-job workdirs and scrubbed environment; `sandbox_command` hook; Kubernetes provider | `tests/node/test_agent_jobs.py`, `tests/security/test_paths.py` | Partial (no built-in network or filesystem policy per pool beyond the hook; the callback token of a job is capped, the code is not isolated from the worker's OS user) |
| 31.4 | Admin UI on localhost by default; remote access explicit | `server.host` defaults to 127.0.0.1 | `tests/unit/test_config_merge.py` | Done |
| 31.5 | Short-lived tokens, server-side refresh, no token logging | `security/tokens.py`; refresh tokens rotate and reuse after a 10 s grace revokes the whole login; logout revokes the login's access and refresh tokens on every node (`revoked_tokens`, honoured within 2 s); job and kernel tokens live until the job timeout plus one hour at most and end with their attempt or session or when the origin credential is revoked | `tests/security/test_tokens.py`, `test_session_hardening.py`, `tests/compute/test_job_credentials.py` | Done |
| 31.6 | Nonce, timestamp, idempotency, signature for capsules and sensitive operations | `fabric/signing.py`, webhook signatures | `tests/fabric/test_signing.py`, `tests/orch/test_webhooks.py` | Done |
| 31.7 | Tenant isolation checks; horizontal and vertical escalation tests | sweeps over every route | `tests/security/test_authorization.py`, `test_references.py`, `test_fuzz.py` | Done |
| 31.8 | Notebook code is privileged; administrators define sandbox policy per pool | `sandbox_command` | `tests/node/test_agent_jobs.py` | Partial (no per-pool policy object; the hook is a setting of each worker agent) |
| 31.9 | No unsigned update path | `ops/updates.py` verifies Ed25519 manifests | `tests/ops/test_updates.py` | Done |
| 31.10 | Support bundles sanitised by default; opt-in for sensitive artefacts | `ops/support_bundle.py`, `leakscan.py` | `tests/ops/test_diagnostics.py` | Done |
| 32.1 | Document ports and outbound destinations | [ports and networking](../reference/ports-and-networking.md): every Spark range is a setting (`compute.*_port_range`, `spark.ports.*` in the chart); `GET /v1/admin/ports` prints them | `tests/tooling/` | Done |
| 32.2 | Enterprise proxies and custom CA trust | `Network` configuration, `ca_bundle` | `tests/node/test_no_proxy.py`, `tests/fabric/` | Done |
| 32.3 | No inbound Internet exposure required | outbound-only workers ([ADR 0003](../adr/0003-outbound-worker-connections.md)) | `tests/node/test_agent_protocol.py` | Done |
| 32.4 | Worker traffic authenticated and encrypted; routed multi-subnet | bearer token and optional mTLS; address selection | `tests/compute/test_topology.py` | Acceptance pending (multi-subnet never tried) |
| 32.5 | Kubernetes NetworkPolicy | `deploy/helm/olcf/templates/networkpolicy.yaml`: control plane and workers may reach each other on every configured Spark range, restrictive egress branch, `workerIngressFrom` | `tests/tooling/test_deploy.py` (rendered; first and last port of every range), `tests/compute/test_review_ports.py` | Acceptance pending (rendered by a bundled interpreter; `kubeconform` and `helm` unavailable; no enforcing CNI) |
| 32.6 | Fabric mode diagnoses DNS, TLS, proxy, Entra, Fabric and OneLake authorisation separately | `fabric/diagnostics.py` | `tests/fabric/test_diagnostics.py` | Acceptance pending (against local test servers) |
| 32.7 | Direct worker-to-storage data paths | see 15.8 | | Done |

## 33-34. Job lifecycle, performance

| ID | Requirement | Implementation | Tests | Status |
|---|---|---|---|---|
| 33.1 | States Accepted, Validating, Queued, Provisioning, Running, Succeeded, Failed, CancelRequested, Cancelled, TimedOut | `core/states.py` (`JobState`) and the transition table; diagram in [compute model](../concepts/compute-model.md) checked by `scripts/check_docs.py` | `tests/compute/test_units.py`, `test_jobs_flow.py` | Done |
| 33.2 | Persist every transition with timestamp and reason | `job_events` | `tests/compute/test_jobs_flow.py` | Done |
| 33.3 | Retry only when policy and write semantics allow | `compute/failures.py` retry rule; idempotent flag; a pipeline timeout that may still be running is `timeout_uncertain` and retried only if idempotent | `tests/compute/test_reconcile.py` | Done |
| 33.4 | Failure classes: compatibility, authn, authz, storage, catalog, dependency, resource, Spark, user-code, timeout, cancellation, provider, internal | `FailureCategory` | `tests/compute/test_units.py` | Done |
| 33.5 | Broker restart reconciles provider applications | provider `reconcile` | `tests/compute/test_reconcile.py` | Done |
| 33.6 | Asynchronous but bounded cleanup; diagnostics retained per policy | job retention settings | `tests/ops/test_retention.py` | Done |
| 34.1 | Submit p95 under 1 s | not measured | | Acceptance pending (no benchmark) |
| 34.2 | Compatibility validation under 5 s for a typical notebook | not measured | | Acceptance pending |
| 34.3 | Long-job broker overhead under 2% | not measured | | Acceptance pending |
| 34.4 | Configurable reservations; prevent oversubscription | see 13.7 | | Done |
| 34.5 | 100,000 job records without redesign | indexed tables, pagination, retention | not tested at that size | Acceptance pending |
| 34.6 | Multi-GB logs streamed and paginated; never loaded whole | `compute/logstore.py` (disk), chunked shipping | `tests/node/test_logship.py` | Done |
| 34.7 | Controller restart or failover reconciles a typical fleet within 2 minutes | measured takeover 15.9 s; reconcile on `hello` | `tests/ops/test_ha.py` | Acceptance pending (small fleets only) |
| 34.8 | Adding workers adds capacity without SDK or notebook changes | planner uses all eligible nodes | `tests/compute/test_standalone.py` | Done |

## 35. Testing and release gates

| ID | Requirement | Implementation | Tests | Status |
|---|---|---|---|---|
| 35.1 | Unit and contract tests for rules, capsules, resolution, scheduler and state machines, redaction and RBAC, manifests, provider suites | see [testing](../development/testing.md) | `tests/` | Partial (no shared provider contract suite that third parties can run) |
| 35.2 | Integration: clean install to first notebook; multi-node enrollment and distributed job; worker loss, restart, network interruption; object stores; Delta and Iceberg interop; Fabric acquisition; side-by-side runtimes; pipelines; Git; installer repair, upgrade, rollback, uninstall | `tests/integration/` (including `test_first_run_no_runtime.py`: `olcf quickstart --no-runtime` on an empty data directory to a finished sample notebook, a Spark notebook that waits with its reason, dashboard tiles, an MCP handshake and SQL), `tests/compute/test_real_agent.py`, `tests/node/test_spark_cluster.py`, `tests/tooling/test_installer.py` | | Partial (see the rows above for what is single-host or acceptance pending) |
| 35.3 | Compatibility corpus with golden outputs, negative notebooks, run on every release, data compared | `tests/corpus/` | 65 tests | Partial (65 tests, one ruleset; golden outputs are compared on the local engines) |
| 35.4 | Chaos and security tests: kills, credential expiry and rotation, disk and memory pressure, tampering, escalation, traversal, fuzzing, leak scans | `tests/compute/test_reconcile.py`, `tests/ops/test_ha.py`, `tests/security/` (fuzz, hypothesis, leaks, paths) | | Partial (no disk-full, memory-pressure or storage-throttling tests; credential rotation is tested for tokens, key rings and worker certificates over plain HTTP, not for an mTLS handshake with a renewed certificate) |

## 36. Full-release acceptance criteria

| ID | Criterion | Evidence | Status |
|---|---|---|---|
| 36.1 | An administrator installs all-in-one without installing Spark, Java or Python by hand | `install.sh` brings Python through `uv`; `olcf runtime install --with-jdk` brings the JDK; Python notebooks need neither (host-Python runtime) | Done on Linux (`tests/tooling/test_installer.py`, `tests/integration/test_first_run_no_runtime.py`); the Spark runtime download needs the network (the tests use a prebuilt runtime); Windows and macOS: R08 |
| 36.2 | Create workspace, lakehouse, environment and notebook in the UI and run distributed PySpark | `tests/e2e/test_05_spark_smoke.py`, `test_11_envs_variables_sparkjobs.py` | Acceptance pending ("distributed" means several agents on one host) |
| 36.3 | At least two worker machines contribute executors to one application | `tests/compute/test_spark_smoke_real.py` (two agents; the result says `single_address: true`) | Acceptance pending (one machine) |
| 36.4 | Multiple runtime families coexist; no cross-runtime placement | see 1.2 | Done |
| 36.5 | Delta interoperable; protocol policy enforced | policy tests; the engine matrix is run by `scripts/verify_spark_delta.py` | Partial |
| 36.6 | Pipelines orchestrate notebooks, Spark, SQL, copy, control flow with schedules, retries, cancel | `tests/orch/`, `tests/integration/` | Done |
| 36.7 | A Fabric notebook submits itself and runs against OneLake on external compute | `tests/fabric/test_e2e_*.py` | Acceptance pending (tenant acceptance, R07) |
| 36.8 | Fabric context and default lakehouse reconstructed for supported semantics | shim and capsule | Acceptance pending |
| 36.9 | Standalone with MinIO or S3-compatible storage and no Fabric | S3 provider against moto | Acceptance pending for MinIO |
| 36.10 | RBAC, OIDC and Entra, protected secrets, TLS, audit, backup and restore, signed updates pass production tests | `tests/security/`, `tests/ops/` | Partial (OIDC and Entra against mock providers) |
| 36.11 | HA control plane survives node loss without losing metadata; compute reconciled | `tests/ops/test_ha.py` (PostgreSQL) | Acceptance pending (several control-plane processes on one PostgreSQL server on one host; no multi-machine drill) |
| 36.12 | Every advertised compatibility feature has automated golden tests | corpus and shim tests | Partial |
| 36.13 | Unsupported behaviour fails explicitly; no silent semantic substitution | see 2.8 | Done |

## 37-40. Technology, governance, naming, references

| ID | Requirement | Implementation | Status |
|---|---|---|---|
| 37.1 | FastAPI/Python; language-neutral contracts | FastAPI; OpenAPI; JSON Schemas in `schemas/` | Done |
| 37.2 | Frontend React/TypeScript or equivalent | Preact and htm without a build step ([ADR 0012](../adr/0012-console-without-build-step.md)); CodeMirror 5 instead of Jupyter components | Partial (a deliberate deviation) |
| 37.3 | PostgreSQL for production; SQLite for small installs | both | Done |
| 37.4 | MinIO or S3-compatible default standalone option | local filesystem is the default; S3 and MinIO are providers | Partial |
| 37.5 | Spark Standalone and Kubernetes providers | both | Acceptance pending for Kubernetes |
| 37.6 | Delta and Iceberg first class | Delta full; Iceberg read and discovery | Partial |
| 37.7 | Open catalog abstraction with REST and Hive-compatible plug-ins | none | Gap |
| 37.8 | Native DAG service with portable definitions | `pipelines/` | Done |
| 37.9 | OIDC, Entra adapter, LDAP bridge | present | Acceptance pending (mock providers) |
| 37.10 | OS secret protection; Vault and KMS plug-ins | keyring, file, Vault | Partial |
| 37.11 | OpenTelemetry metrics and traces plus structured logs | `observability/otel.py` | Done |
| 37.12 | Signed Windows installer, Linux packages and containers, signed runtime bundles, Helm charts | see 26.1 | Partial |
| 38.1 | Repository boundaries separating core, services, UI, agent, providers, adapter, manifests, tests | one repository with packages ([repository map](../development/repository-map.md)); the Fabric SDK is a separate distribution | Partial (a single repository, not many) |
| 38.2 | OSI-approved licence selected with counsel | Apache-2.0 ([ADR 0016](../adr/0016-apache-2-and-dco.md)); counsel review is outside the project | Process |
| 38.3 | Contribution guide, code of conduct, security policy, release process, ADRs, compatibility policy | CONTRIBUTING, CODE_OF_CONDUCT, SECURITY, [release process](../development/release-process.md), [ADRs](../adr/README.md), compatibility policy in the [release process](../development/release-process.md#compatibility-policy) | Done |
| 38.4 | Public, versioned plug-in interfaces for storage, compute, catalog, identity, pipeline activities | storage, compute, activities | Partial (no catalog or identity plug-in interface) |
| 38.5 | Release artefacts: source, binaries and images, SBOM, signatures, checksums, notices, reproducible build metadata | `build_release.sh` (server and Fabric SDK wheels and source archives, gate in `verify_dist.py`), `release.yml` (draft release, `pypi.yml` for optional PyPI upload) | Acceptance pending (the local build is verified, and release identity placeholders block a release, [readiness](../development/release-readiness.md) R01) |
| 38.6 | Trademark policy | [TRADEMARKS.md](../../TRADEMARKS.md) | Done |
| 39.1 | Naming and positioning; labels such as "Fabric Runtime 2.0 Compatible"; no physical-expansion wording | runtime titles, documentation | Done |
| 40.1 | Source references for the baseline | listed in the requirements; the assumptions made from them are in [Fabric assumptions](../guides/fabric-assumptions.md) | Process |

## 41. OLCF Intelligence

Source: [`docs/dev/INTELLIGENCE.md`](../dev/INTELLIGENCE.md). Rows 41.1 to 41.9 are its nine non-negotiable requirements in order; 41.10 onwards are the features and the later hardening. How to read the statuses here: *Done* means the behaviour exists and is covered by automated tests that pass on Linux with SQLite; everything that talks to Microsoft Fabric or Power BI is *Acceptance pending* until it has been confirmed on your tenant; the tests use `FakeFabricWorld`, a local test server that follows Microsoft's documentation. The explanation of the feature is in [OLCF Intelligence](../concepts/intelligence.md), the decision in [ADR 0017](../adr/0017-intelligence-subsystem.md), the readiness items in [release readiness](../development/release-readiness.md) (R15 to R18).

| ID | Requirement | Implementation | Tests | Status |
|---|---|---|---|---|
| 41.1 | Subsystem failure isolation: compute, SQL and pipelines never wait for, call or fail because of Intelligence; events are published best effort and consumed from a durable inbox | `core/dataset_events.py` (publishers), `intelligence/ingest.py` (inbox writer, processor on the leader) | `tests/intelligence/test_isolation.py` (a broken inbox and graph; jobs and SQL still succeed), `tests/intelligence/system/test_failure_isolation.py` (broken inbox, graph, subscriber, loops), `tests/unit/test_event_bus.py`, `test_ingest.py`, `tests/data/test_dataset_events.py`, `tests/compute/test_dataset_events_spark.py` | Done (the event bus gives each subscriber a lane and a publisher waits at most 0.5 s: row 41.30; the price, accepted in the contract: events can be lost, so a verified edge can be missing) |
| 41.2 | No circular runtime dependencies: only `core`, `db`, `security` (and the HTTP helpers) are imported by the module core; no other package imports it | package layout; [repository map](../development/repository-map.md) | `tests/intelligence/test_isolation.py` (import graph over the compute-side packages and the allowed look-ups, and the layers of the core) | Done |
| 41.3 | Optional and independently deployable: works without the module, and the module works without compute and without Fabric | every look-up is `ctx.maybe_service`; scans use delta-rs and stored definitions, never Spark | `tests/intelligence/discovery/test_independence.py`, `tests/intelligence/governance/test_isolation.py` | Done (a Fabric-only installation is covered by the Fabric test server; that shape is Acceptance pending, row 41.13) |
| 41.4 | Observe by default: governance reports, never blocks; `enforce` is refused | `intelligence/governance/policies.py`, `workflow.py` | `tests/intelligence/governance/test_policies.py`, `test_changes.py` | Done (enforcement itself is not implemented: row 41.19) |
| 41.5 | Honest lineage: every edge has a confidence and evidence; gaps are data; complete column lineage is never claimed | `intelligence/model.py`, `engine.py`, `queries.py` (path strength is the weakest edge, gaps, coverage) | `tests/intelligence/test_graph.py`, `test_engine.py`, `test_column_lineage_measures.py`, `lineage/test_robustness.py` | Done (only run-time events produce `verified` edges; authoritative declarations such as a pipeline `dependsOn` are `derived`, and the wording in the code and tool descriptions now says so) |
| 41.6 | Stable asset identity: one `ast_<ULID>` per object, other ids as aliases, placeholders merged, assets never deleted | `intelligence/registry.py`, `graph.py` | `tests/intelligence/test_registry.py`, `test_graph.py` | Done |
| 41.7 | Sampling is opt-in: default off, 0 to 100 rows, masking, classified columns excluded, nothing stored, separate permission and identity | `intelligence/discovery/sampling.py`, `identities.py` | `tests/intelligence/discovery/test_sampling.py`, `test_visibility.py` | Done for local data; the Fabric sampling identity and OneLake reads are Acceptance pending (row 41.13) |
| 41.8 | Existing behaviour does not change: the classic catalog is read, never written | `intelligence/discovery/catalog_mirror.py`; no change to `catalog/` | `tests/intelligence/discovery/test_catalog_mirror.py`, `tests/data/test_catalog.py` (unchanged) | Done |
| 41.9 | Every discovery source has a stepwise connectivity test with a remediation per step | `intelligence/discovery/base.py` (`Step`), `fabric.py` (8 steps), `local.py` | `tests/intelligence/discovery/test_sources_api.py`, `test_fabric_connector.py` | Acceptance pending (local source: Done; the Fabric steps are covered by the Fabric test server) |
| 41.10 | Asset graph of OLCF workspaces, lakehouses, tables, columns, notebooks, pipelines, queries and dashboards; incremental and full scans; sweep of removed objects; snapshots and diff | `intelligence/discovery/local.py`, `scans.py`, `graph.py` | `tests/intelligence/discovery/test_local_connector.py`, `test_scan_runner.py`, `tests/intelligence/test_graph.py` | Done (a scan of tens of thousands of tables was never measured, row 41.26) |
| 41.11 | Static lineage from SQL, PySpark, notebooks, pipelines (OLCF and Fabric), Spark job definitions and OLCF dashboards, to column level where provable | `intelligence/lineage/` (`sql.py`, `pyspark.py`, `notebook.py`, `pipeline_olcf.py`, `pipeline_fabric.py`, `spark_job.py`, `report_olcf.py`) | `tests/intelligence/lineage/`, `tests/intelligence/discovery/test_lineage_e2e.py` | Partial (static analysis: dynamic names, `exec`, UDF bodies, some DataFrame constructs become gaps or `inferred` edges; the per-analyzer matrix is [lineage support](../reference/lineage-support.md)) |
| 41.12 | Verified lineage from what ran: reads and writes observed at run time | `runtime_lib/lineage_probe.py`, `compute/lineage_events.py`, `intelligence/runtime.py` | `tests/intelligence/test_runtime_facts.py`, `tests/data/test_dataset_events.py`, `tests/compute/test_dataset_events_spark.py` | Partial (the probe covers the listed read and write APIs only; Spark Connect, streaming, JDBC, pandas and polars readers and `DeltaTable` merge, update and delete are not observed, and the Delta-on-Spark paths are outside the test runtime, which has no Delta jars, R17) |
| 41.13 | Microsoft Fabric and Power BI discovery with a service principal (workspaces, items, definitions, Delta schemas, admin scanner) | `intelligence/discovery/fabric.py` | `tests/intelligence/discovery/test_fabric_connector.py` (against `FakeFabricWorld`) | Acceptance pending (assumptions in [Fabric assumptions](../guides/fabric-assumptions.md#26-intelligence-discovery-the-scanner-and-the-fabric-and-power-bi-rest-apis), readiness R15) |
| 41.14 | Import of a Power BI scanner result as a source of its own | `intelligence/discovery/scanner_import.py`, `lineage/scanner.py` | `tests/intelligence/discovery/test_scanner_import.py`, `lineage/test_scanner.py` | Acceptance pending (written against the documented response shape; never fed a real export) |
| 41.15 | Lineage through semantic models and reports: TMDL, DAX measures, Power Query M, PBIR and legacy reports | `intelligence/lineage/tmdl.py`, `dax.py`, `mcode.py`, `semantic_model.py`, `report_pbi.py` | `tests/intelligence/lineage/test_dax.py`, `test_mcode.py`, `test_semantic_model.py`, `test_report_pbi.py`, `test_column_lineage_measures.py` | Partial (derived from definitions the sample and the tests provide; constructs outside the parsers become gaps) |
| 41.16 | Governance: business glossary with term links, domains, data products, owners and stewards, certification, comments | `intelligence/governance/glossary.py`, `domains.py`, `ownership.py`, `workflow.py` | `tests/intelligence/governance/test_glossary.py`, `test_domains_products.py`, `test_ownership.py`, `test_certification.py` | Done |
| 41.17 | Classification: labels on assets and columns, suggestions from a sensitive-data detector, a review queue, propagation along lineage (computed, not stored) | `intelligence/governance/classification.py`, `propagation.py`, `intelligence/discovery/detect.py` | `tests/intelligence/governance/test_classification.py`, `tests/intelligence/discovery/test_detect.py` | Done (the detector is name- and pattern-based, so it misses and over-flags; every suggestion needs a person) |
| 41.18 | Policies (7 rule types), quality results, freshness; violations and exposure reports | `intelligence/governance/policies.py`, `quality.py`, `selectors.py` | `tests/intelligence/governance/test_policies.py`, `test_quality.py`, `test_overview.py` | Done |
| 41.19 | Enforcement of a policy in the execution path | none; a policy with `enforce` is refused with `422` | `tests/intelligence/governance/test_policies.py` (the refusal) | Gap (by design for this release, [ADR 0017](../adr/0017-intelligence-subsystem.md)) |
| 41.20 | Visibility and permissions: `intelligence.read`, `.manage`, `.sample`, `governance.curate`, `.approve`; hidden assets answer 404 and are redacted from evidence | `intelligence/api.py`, `governance/common.py`, role table in `security/rbac.py` | `tests/intelligence/test_visibility.py`, `tests/intelligence/governance/test_authorization.py`, `test_api_write.py` | Done |
| 41.21 | Surfaces: REST (129 operations), CLI (`olcf intelligence`, `olcf governance`), SDK, and read-only MCP tools (10 `intel_*`, 4 `governance_*`) | `intelligence/api.py`, `discovery/api.py`, `governance/api.py`, `cli/`, `sdk/`, `mcp/intel_tools.py`, `mcp/gov_tools.py` | `tests/intelligence/test_api.py`, `tests/tooling/test_cli_intelligence.py`, `test_cli_governance.py`, `test_sdk_intelligence.py`, `test_sdk_governance.py`, `tests/intelligence/test_mcp_intel.py`, `test_mcp_gov.py` | Done |
| 41.22 | Web console pages for catalog, asset, lineage, impact, discovery and governance | `web/static` (`#/intelligence/...`) | `tests/web/test_intel_ui.py`, `tests/e2e/test_16_intelligence.py` (headless Chromium) | Partial (no page for the unused-table report, which exists in REST, CLI, SDK and MCP) |
| 41.23 | Audit and events for sources, scans, edits, approvals and samples | `intelligence/module.py`, `governance/hooks.py`; names in [events](../reference/events.md), where the generator finds them | `tests/intelligence/test_api_write.py`, `tests/intelligence/governance/test_changes.py` | Done (the `governance.*` audit actions are written through `self.audit(...)`; `scripts/gen_reference.py` now matches that form, so they appear in the generated list) |
| 41.24 | Sample content that shows Intelligence on the first day (scans, a synthetic and labelled Power BI scan, a 12-term glossary, a domain, a data product with owners and a steward, a PII label on one column, certification of the gold tables; sample `VERSION` 4) | `sample/intelligence.py`, `sample/powerbi.py` | `tests/intelligence/system/test_sample_integration.py`, `test_full_platform.py`, `tests/intelligence/lineage/test_sample_workspace.py` | Done |
| 41.25 | Runs on PostgreSQL, including the scan lease and a replica failover | schema and migrations for 24 tables | the PostgreSQL variants of the intelligence tests (`pytest -m postgres tests/intelligence`, 53 tests) | Acceptance pending (the 53 tests run when `OLCF_TEST_POSTGRES_URL` is set, R16) |
| 41.26 | Performance at scale (scan time, lineage and impact answers, inbox burst) | bounded traversals and caps documented in [lineage support](../reference/lineage-support.md) | none measuring performance | Acceptance pending (one system test on 2,000 tables and 20,000 columns on SQLite, `tests/intelligence/system/test_performance.py`; nothing larger was measured, R17; impact totals are bounded by `intelligence.max_impact_nodes`, lineage by `intelligence.max_lineage_nodes`) |
| 41.27 | Endpoints of a Fabric or Power BI source are SSRF-guarded: https only, no credentials, no loopback, private, link-local or metadata destination, checked on save and before every use, connections pinned to the checked address; `intelligence.allow_private_endpoints` lifts only the loopback and private block | `intelligence/discovery/netguard.py`, `identities.py` | `tests/intelligence/discovery/test_fabric_netguard.py`, `test_fabric_connector.py` | Done (behind a proxy only a local name check is possible; the setting is a deliberate hole while it is on) |
| 41.28 | A run submitted from a Fabric notebook attaches to the notebook and tables the tenant scan found, by Fabric ids (`verified`) or by name (`inferred`), in either order | `compute/lineage_events.py`, `core/dataset_events.py`, `intelligence/runtime.py` | `tests/intelligence/system/test_fabric_runtime_link.py`, `test_runtime_facts.py` | Acceptance pending (covered with a `notebookutils` module, the real SDK, adapter and worker, and the Fabric test server; Fabric notebook acceptance: R17) |
| 41.29 | Inbox operations and bounds: a failed event row is retried once automatically and can be re-queued by an operator; attribute history is trimmed; impact totals have their own bound; the origin of a column is answered at column level | `intelligence/ingest.py` (`requeue_failed`), `service.py`, `api.py` (`POST /intelligence/inbox/requeue`), `queries.py`, migration `intelligence` 2, settings `intelligence.inbox_retry_after_minutes`, `max_versions_per_asset`, `max_impact_nodes` | `tests/intelligence/test_ingest.py`, `test_hardening.py`, `system/test_performance.py`, `tests/tooling/test_cli_intelligence.py`, `test_sdk_intelligence.py` | Done |
| 41.30 | The event bus delivers to each in-process subscriber on its own lane and a publisher waits at most 0.5 s in total, so a hung subscriber cannot hold a job, query or pipeline | `core/events.py` | `tests/unit/test_event_bus.py`, `tests/intelligence/system/test_failure_isolation.py` | Done |

## Appendices

| ID | Item | Evidence | Status |
|---|---|---|---|
| A.1 | Required repository and service boundaries (20 named components) | all components exist as packages of one repository: core contracts (`core/`), control API (`api/`), web UI (`web/`), workspace and notebook services (`api/core`, `notebooks/`), pipelines, catalog, scheduler and compute broker (`compute/`), worker agent (`worker/`), runtime manager (`runtime/`), storage and compute providers, identity and security (`security/`), observability, Fabric adapter (`fabric/`), compatibility (`fabric/analyzer`, `runtime_lib/notebookutils`), CLI and SDK, installer (`scripts/`, `deploy/`), corpus (`tests/corpus`) | Partial (boundaries are packages, not separate repositories) |
| B.1 | API families | present: `/v1/workspaces`, `/items`, `/notebooks`, `/lakehouses`, `/catalogs`, `/pipelines`, `/environments`, `/connections`, `/jobs`, `/sessions`, `/spark-applications`, `/events`, `/workers`, `/worker-pools`, `/providers`, `/runtimes`, `/queues`, `/identity`, `/roles`, `/secrets`, `/audit`, `/admin/health`, `/admin/diagnostics`, `/admin/updates`, `/admin/backup`, `/integrations/fabric`. Differences: logs are sub-resources (`/jobs/{id}/logs`) rather than a `/logs` family; permissions are listed under `/roles`; restore is `/admin/backup/{id}/restore`; there is no `/v1/connections`-independent `/restore` family | Done |
| C.1 | Definition of done: one secure core for both modes; pooled multi-machine Spark; side-by-side runtimes; integrated experiences; OneLake and open object stores; deterministic, tested Fabric compatibility; production-grade install, HA, security, observability, backup, upgrade and rollback; every advertised feature backed by automated tests | See the rows above. The core and single-host behaviour are implemented and tested; the elements listed under Acceptance pending and Gap are tracked in [release readiness](../development/release-readiness.md) | Partial |

## Known limitations

The checklist of what still has to be done and verified before each kind of claim or publication (R01 to R14) is [release readiness](../development/release-readiness.md).

Everything below is true of version 0.1.0 and was confirmed against the code, the FIX and FIRSTRUN addenda of [`docs/dev/CONTRACTS.md`](../dev/CONTRACTS.md), or by running it (a clean `olcf quickstart --no-runtime` on an empty data directory, the sample notebooks, `olcf job status`, the CLI error paths). They are the items to read before a production decision. The review fixes behind several of them are described finding by finding in [`CODE_REVIEW_RESPONSE.md`](../dev/CODE_REVIEW_RESPONSE.md).

**Acceptance work for your environment**

1. Microsoft Fabric, Entra ID and OneLake: the integration follows the documented API shapes and is covered by contract tests against local test servers. The assumptions, and how to confirm each on your tenant, are listed in [Fabric assumptions](../guides/fabric-assumptions.md). Optional tests (`pytest -m live`, 8 tests) run against your own tenant when the `OLCF_LIVE_*` variables are set.
2. Delta Lake on Spark needs jars from Maven Central; `scripts/verify_spark_delta.py` checks them on a machine with network access. Delta is exercised through delta-rs and DuckDB. The Spark runtime download that `olcf quickstart` performs after it has printed the working state needs the network: the tests use a prebuilt runtime, and the failure path (a warning, the exact retry command, exit 0) is tested.
3. The Docker images, the Helm chart, the Kubernetes provider, the Livy and Fabric providers, `install.ps1`, `uninstall.ps1` and the Windows scheduled task are covered by rendered-manifest, protocol and structure tests; the macOS installer is covered by Linux-hosted tests with Command Line Tools stubs. The first-run path (quickstart, host-Python runtime, sample notebooks) is tested on Linux. The tests use a Kubernetes API server and kubelet written for them (`FakeKube`), a Livy test server, test and real agents over plain HTTP, a bundled Go-template interpreter instead of Helm, hidden POSIX modules for Windows and a `uname` shim for macOS. Before you deploy, run `kubeconform` and `helm lint` on your values and check the NetworkPolicy against your CNI. The CI job `windows` and the macOS matrix legs are defined in the workflows.
4. A Spark cluster across several physical machines: "distributed" is shown with several agents on one host (the smoke test reports `single_address: true` there); run `olcf compute smoke --executors 2` on your machines. Spark RPC authentication and encryption are not enabled by OLCF.
5. The GitHub workflows (CI, release, container, security, PyPI) are checked with `actionlint` and structure tests. The CI `tests` job used to fail at collection and the `postgres` job selected no test; both are fixed. The release workflow calls CI, container and security as reusable workflows before it builds, creates a draft release, and leaves PyPI upload to a separate workflow; a tag push still pushes versioned container images before the draft is reviewed (use the protected `release` environment). A repository dry run confirms permissions, nested reusable workflows and action availability ([release readiness](../development/release-readiness.md), R02). The repository owner and name are not set, so the release identity preflight (`scripts/check_release_config.py`) fails by design (R01).
6. No performance benchmarks exist; the targets in section 34 are not measured.

**Security and operations**

7. The shipped runtime manifests are signed with a development key whose private half is in the repository. A production installation does not trust it by default (an unset `environment` is production), so the manifests must be re-signed with the operator's own key and a trust directory provided ([hardening](../operations/hardening.md#6-supply-chain)). The `runtime-trust` health check is advisory: it reports `down` when the development key is explicitly trusted in production but does not stop the server or fail readiness. The Helm chart mounts a trust directory with `runtimeTrust.existingSecret|existingConfigMap` and the Docker images take a `RUNTIME_TRUST_DIR` build argument; both are tested statically and by rendering. Existing data directories without `environment` become production after the upgrade.
8. Key rotation (`olcf security rotate`) and worker credential renewal (`olcf worker renew`) have entry points now. They are tested with threads, processes on one host, a local server and PostgreSQL 16; multi-machine rotation and the mTLS handshake with a renewed certificate are readiness items (R10). The UI has no rotation page (CLI, REST and SDK only). Replacing the *root* key (`OLCF_MASTER_KEY`, keyring entry or `master.key`) is an operator procedure (add, restart nodes, `rewrap`, remove).
9. There is no offline administrator password reset; resetting needs the API and another PlatformAdmin.
10. Worker client certificates renew automatically over the authenticated channel and their expiry is covered by a health check and alert rules, but a worker that is offline when its certificate expires cannot renew and must be enrolled again. Worker tokens do not expire unless `compute.worker_token_max_age_days` is set (default `0`).
11. Organisation-scoped SecurityAdmin, RuntimeAdmin and Operator roles act across all workspaces, by design. The rate limiter is per process (its table is bounded, but with several nodes the effective limit is the limit times the nodes). A password change revokes refresh tokens, not access tokens that were already issued: those stay valid until they expire (30 minutes by default). Logout, in contrast, revokes the whole login.
12. The server-enforced forced password change (`must_change_password`) restricts local login sessions only. Personal access tokens, worker tokens and federated identities are not subject to it, because they do not use the local password.
13. Notebook code runs as the worker's operating-system user. The `sandbox_command` hook is the only isolation provided (there is no per-pool OS or container isolation policy); do not treat OLCF as a multi-tenant sandbox. On the built-in host-Python runtime that code runs in the same Python environment as the worker itself, so it can import everything installed next to OLCF. The callback token a job receives is capped (derived credentials), but the job's input file holds that token and the resolved secrets and is readable by the code.
14. TLS is off by default (the server binds to 127.0.0.1).

**First run and the host-Python runtime**

15. The host-Python fallback is decided when a job or session is created. A **queued job** whose runtime no online worker has is re-bound by the scheduler when a worker that offers the host runtime appears; jobs that pinned a runtime, Spark jobs and placed jobs never are, and an interactive **session** is not re-bound after creation. With `compute.host_python_fallback` off nothing is moved.
16. The libraries of the host-Python runtime are whatever is installed next to OLCF on each worker, so nodes can differ (the per-node fingerprint and the package versions in `result.json` record which). `matplotlib` is optional (the `plots` extra); without it the sample notebook shows tables instead of charts. With `compute.host_python_fallback` switched off a Python-engine job whose runtime is missing waits like a Spark job, and the wait message still suggests "use a Python-engine notebook".
17. `olcf compute smoke` needs a ready Spark runtime on an online worker; without one it answers `409 no_spark_runtime` (exit code 6) at once. The test is asynchronous with a controller-side deadline. A client-side wait timeout exits 9, as the general CLI table says for a `--wait` timeout (it exited 11, which the table reserves for a missing prerequisite, until the residual fixes); a 409 exits 6 (conflict); a test that ran and failed or hit its deadline exits 10.
18. Small CLI wrinkles observed while documenting: `olcf doctor`'s local runtime line counts the built-in host-Python runtime, so it never warns that no Spark runtime is installed once a worker has run (before that it warns); `olcf context list` shows no user name for a context created by `olcf init` or `olcf quickstart`; piping a command into `head` that closes the pipe early prints `error: BrokenPipeError` and exits 1.
19. **An unexplained sandbox observation, not a product finding.** In the build sandbox, a server launched directly from the tool shell once died with a 20 s `faulthandler` "Timeout" stack dump, while the same launch from a script file (`setsid nohup script.sh`) was stable every time. It was not reproduced from scripts, the cause is not known, and nothing in the product was changed for it. It is listed so that nobody reads the absence of the problem as proof that it cannot happen.

**Missing relative to the requirements**

20. Scala, R and C# cells are preserved but not executed.
21. No JDBC or ODBC gateway; connection types are limited to `local`, `s3`/`minio`, `adls` and `onelake`; the copy activity moves data between lakehouses only.
22. The catalog has no views, functions or schema hierarchy, and no Hive, REST, Glue or Unity adapters. The classic catalog has no sensitivity or classification model; labels exist only in the Intelligence module (section 41), which does not write them into the catalog or enforce them.
23. RBAC bindings can be created at org, workspace, item, connection, runtime and pool scope, but only routes that pass the narrower id to the permission check honour them (item, connection and runtime ids are passed at some call sites; pool scope was not verified). Connection is not an item type.
24. No Redis or RabbitMQ backend, no ORC support, no Iceberg compaction or partition-evolution management, no Windows DPAPI or cloud key-vault secret provider.
25. No MSI or other signed Windows installer, and no deb or rpm packages. The console uses Preact without a build step instead of React and TypeScript.
26. The lease `term` increments on every renewal, so it cannot be used to detect a change of leader.
27. `olcf sql explain` takes `--file`, `--engine` and `--workspace` but no `--lakehouse`, so a plan cannot be requested against a named lakehouse from the CLI; qualify the table names in the query instead.

**Housekeeping defects still open** (reported to the maintainers)

28. In `core/config.py` the comment above `reserve_cores` describes a fraction, which is the next field's meaning (`reserve_memory_fraction`).
29. An empty directory `src/olcf/jobs/` is in the working copy (git does not track empty directories, so it is not in the repository).
30. Docstrings and comments in the source name sections of the requirements document ("spec 13") and CONTRACTS entries. The documentation pass removed these where it touched a file, but not everywhere.

**Introduced or documented by the review remediation** (all covered by tests with test doubles unless stated)

31. **Kubernetes and Livy rest on protocol tests.** The pod layout, the runner's remote completion contract (`result` event, artifacts, termination record, `result_missing`), the timestamp-cursor log reading and the Livy retry/reconciliation rules are exercised against `FakeKube` and Livy test transports. Kubelet log formats, non-root file ownership, `readOnlyRootFilesystem`, `spark_submit` mode (its entrypoint must call the runner with `--workdir`) and how a Livy gateway reports an accepted-but-unanswered request are assumptions to confirm on your infrastructure.
32. **Dense log bursts are quadratic.** Each log page re-reads the retained pod log from the second of the cursor, so many MiB written within one second are re-fetched for every page (bounded by a 64 MiB window and the drain timeout). Output rotated away before the first read cannot be detected; lines inside a reported gap are lost.
33. **Job token registration.** The attempt's token id is recorded in `job_attempts.token_id` right after minting, before the spec is built or the provider is called, and a failed launch (including a `WorkerOffline` requeue) revokes it at once. Remaining: a controller that stops in the few statements between minting and the record leaves a token for the job timeout plus one hour (refused once the attempt is no longer live), and an uncertain Livy/Fabric submission keeps its token on purpose.
34. **Logout and PAT revocation end running jobs' callbacks** by design (derived credentials follow their origin). Long jobs need a service token.
35. **The SQL host guard allowlists Spark functions (C01).** Spark scalar functions are checked against the finite `SPARK_ALLOWED_FUNCTIONS` (`sql/spark_functions.py`), qualified calls and unknown/UDF functions are refused, and unparsed text is never forwarded for any role; a genuine built-in missing from the list is refused until reviewed and added (custom functions: use a notebook or job). `check_spark_sql` (the pooled-session door) now refuses the session-command class (`ADD JAR`, `ADD FILE`, `RESET`, `CACHE TABLE`, ...) for every caller, `strict` or not.
36. **Restore reconciliation covers jobs, not sessions or kernels,** and is tested by restoring into a second data directory on one machine with SQLite; PostgreSQL and a clean host with worker agents are readiness items (R09). Job source files in the data directory are not in a backup.
37. **Pipeline timeouts need an operator for non-idempotent work.** `timeout_uncertain` is retried only when the activity is idempotent; otherwise the run fails with `operatorAction` and waits for `olcf pipeline resume`. The idempotency key helps only if the receiving service honours it.
38. **Windows specifics.** `olcf.core.filelock` uses `msvcrt` there and is covered by Linux-hosted tests and the CI job `windows`; Spark runtimes, process groups and signals on Windows differ from POSIX and are a readiness item (R08).
39. **Documentation defects found while verifying** (fixed afterwards, see the CODE_REVIEW_RESPONSE defects list): the `dev_key_notice` health-check route, the pipeline resume route in the engine docstring and CONTRACTS, and two `notebookutils` hints that suggested a "JDBC OLCF connection" that does not exist.

**Fixed since the first version of this list** (each with a regression test; see the Fix and First-run addenda in [`docs/dev/CONTRACTS.md`](../dev/CONTRACTS.md))

* The Spark smoke test (`SMOKE_SCRIPT`) now runs on the real cluster through the normal runtime bootstrap instead of a private `local[*]` session and reports executors per node; `compute.reserve_cores` is live; cluster detail no longer keeps a stale "0/? workers alive".
* Forced password change is enforced by the server for every client; logout revokes the whole login; refresh-token reuse revokes the family; the rate limiter is bounded; a disabled webhook stops failing.
* Agent SIGTERM / SIGINT / SIGHUP run the graceful shutdown (no orphaned Spark daemons unless `--keep-daemons-on-exit`); the kernel "Messages are sent over TCP without encryption" noise is filtered.
* `olcf login` printed "signed in as None"; `olcf fabric status` left the CHECK column empty; the console's certificate banner named `server.tls` instead of `tls.enabled`; `olcf restore` placed no configuration or key files; `olcf backup create --include-keys` omitted the certificate authority; `olcf sql engines` and `olcf search` printed empty columns; the async SDK client lacked `from_env` / `from_context`; the MCP tools docstring said three write tools where there are two.
* `pytest tests` failed at collection (two files named `test_metrics.py`; the orchestration one is now `test_obs_metrics.py`); the CI `postgres` job selected no test; `ruff check src tests scripts` is clean.
* The 18 findings of the external review of 8 October 2026 (derived job credentials, SQL host guard, Kubernetes read-only volume and result delivery, Livy retry safety and request fidelity, Helm Spark ports, Windows imports, log paging, pipeline timeout overlap, remembered sign-in, test layout, type check, macOS test assumptions, CI pip) and the limitations "key rotation without an entry point", "worker certificates never renew", "the late-worker fallback binding", "`olcf compute smoke` without a runtime" and "the development key is trusted in production": each has a regression test; what remains open is above and in [`CODE_REVIEW_RESPONSE.md`](../dev/CODE_REVIEW_RESPONSE.md).
* A first run no longer depends on the Spark runtime download: Python notebooks run on the built-in host-Python runtime, a queued job explains itself, CLI failures are one line with a hint instead of a traceback, and quickstart replaces a stale CLI token and reports the working state before the runtime download.
