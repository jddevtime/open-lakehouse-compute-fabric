# OLCF Intelligence: discovery, catalog graph, lineage and governance (engineering contract)

Author: Justin Dodrill. Status: contract for the implementation; the user-facing description is in `docs/concepts/intelligence.md` and `docs/guides/intelligence.md` (written with the feature).

OLCF Intelligence is the fifth module of the platform (Compute, Data, Orchestration, **Intelligence**, Fabric compatibility, Administration/Security). It discovers every accessible asset
(OLCF workspaces, lakehouses, tables, columns, notebooks, pipelines, queries, dashboards; and from Microsoft Fabric and Power BI: workspaces, lakehouses, notebooks, pipelines,
semantic models, reports), derives end-to-end lineage down to column level, and layers governance (glossary, domains, data products, owners, classifications, certification, policies,
quality) on the resulting metadata graph.

## 1. Non-negotiable requirements

1. **Subsystem failure isolation.** Compute, SQL, pipelines and the Fabric adapter never wait for, call, or fail because of Intelligence. They publish ordinary platform events
   (`ctx.events.publish`, best effort, wrapped so an exception is logged and swallowed). Intelligence consumes those events **asynchronously** through a durable inbox table. If the
   Intelligence service, its tables or its graph are offline, broken, slow or disabled, jobs, queries and pipelines run exactly as before. There is a test that breaks the inbox and
   the graph and proves jobs and SQL still succeed.
2. **No circular runtime dependencies.** Dependency direction is one-way: `olcf.intelligence` may import `olcf.core`, `olcf.db`, `olcf.security`, and may call other modules through
   `ctx.maybe_service(...)`; **no other package imports `olcf.intelligence`** (the only exceptions are the registration lists `api/modules.py`, `db/migrate.py` and the optional
   `ctx.maybe_service("intelligence")` look-ups in the sample installer and MCP/CLI/SDK layers). An import-graph test enforces this for `olcf.compute`, `olcf.worker`, `olcf.runtime_lib`,
   `olcf.sql`, `olcf.pipelines`, `olcf.catalog`, `olcf.storage`, `olcf.notebooks`, `olcf.fabric`.
3. **Optional and independently deployable.** An installation without the module (`create_app(module_names=...)` without `olcf.intelligence.module`) works; every look-up is
   `maybe_service`. The module without compute (no worker, no Spark) works too: discovery reads Delta metadata with delta-rs and stored definitions with analyzers, never starting Spark.
   Scans do not need a runtime, a worker or a Spark cluster. Intelligence can run against an existing Fabric tenant without the compute engine.
4. **Observe by default.** Governance observes and reports (violations, review queues). Enforcement in the execution path is not implemented; a policy asking for `enforce` is refused
   with `Unsupported` and a remediation that says so. Basic lineage collection never makes a healthy job fail.
5. **Honest lineage.** Every edge has a confidence (`verified`, `derived`, `inferred`, `unknown`) and evidence (source, location, time). The platform reports **gaps** (permission failures,
   unsupported constructs, dynamic references, unavailable APIs) as data; it never pretends complete coverage. Complete column-level lineage is not claimed for any source.
6. **Stable asset identity.** Every object gets one `ast_<ULID>`; every other identifier is an alias (see §3). Compute, lineage, governance and discovery all reference the `ast_` id.
7. **Sampling is opt-in.** Sample rows and value-based profiling are never collected merely because the scanning identity can read the data. A sampling policy (default: off) says
   per workspace/asset how many rows (0 to 100), masking mode and whether classified assets are excluded. Sample values are not stored; they are fetched on demand through a permission
   (`intelligence.sample`), policy-limited, masked, and audited. Metadata discovery and data sampling use separate permissions and, for Fabric, may use separate identities.
8. **Existing behaviour does not change.** The existing `catalog` module (tables `catalog_entries`, `table_stats`, `lineage_edges`; routes `/v1/catalog/*`; CLI `olcf catalog`;
   MCP tools `search_catalog`, `describe_table`, `get_lineage`) stays exactly as it is. Intelligence *reads* it (a connector mirrors `lineage_edges` and `catalog_entries` into
   the graph) and never writes to it. The word "catalog" in OLCF's lakehouse settings means the Spark catalog name; the new concept is called the **asset graph** / **Intelligence catalog** in code.
9. **Everything has a "test connectivity".** Each discovery source has a stepwise connectivity test (REST + CLI + UI button) that says which step failed and how to fix it.

## 2. Package layout and ownership

```
src/olcf/intelligence/
  model.py            shared vocabulary + fact records (PURE, written, do not change shape without telling the lead)
  module.py           install(ctx): service, health, settings, subscriber, loops               [core]
  schema.py           tables: assets, asset_aliases, asset_versions, edges, scans, gaps, inbox, analysis_cache   [core]
  registry.py         AssetRegistry: resolve/ensure/merge by alias                              [core]
  graph.py            GraphStore: upsert assets/edges, traversal, snapshots, diff               [core]
  engine.py           Engine: begin_scan / apply_facts / analyze_and_apply / finish_scan        [core]
  service.py          IntelligenceService facade (what other code calls)                        [core]
  queries.py          lineage / origin / impact / staleness / unused / documentation / search   [core]
  ingest.py           inbox writer (event subscriber) + processor loop                         [core]
  api.py              REST: assets, lineage, search, snapshots, summary                         [core]
  runtime.py          PURE translators: platform event -> Facts (verified edges)               [runtime]
  lineage/            PURE analyzers: definition -> Facts                                       [analyzers]
    base.py engine.py(analyze_definition dispatcher) sql.py pyspark.py notebook.py pipeline_olcf.py pipeline_fabric.py
    dax.py mcode.py tmdl.py semantic_model.py report_pbi.py report_olcf.py scanner.py
  discovery/                                                                                  [discovery]
    schema.py (sources, sampling_policies)  base.py (Connector, ScanContext, Step)  local.py  catalog_mirror.py
    fabric.py (Fabric + Power BI scanner connector)  scanner_import.py  profiling.py  sampling.py  detect.py (pure sensitive-data detection)
    scans.py (ScanRunner)  scheduler.py (LeaderLoop)  api.py (sources, scans, gaps, coverage, sampling policies, import)
  governance/                                                                                 [governance]
    schema.py  glossary.py  classification.py  ownership.py  policies.py  quality.py  workflow.py  api.py
```

CLI (`olcf intelligence ...`), SDK (`client.intelligence`), MCP tools (`intel_*`), web pages, sample content and docs are added once the API is in place.

## 3. Identity: assets and aliases

* `ast_<ULID>` is the asset id. Table `assets` (§5) has `id, type, name, path, workspace_id, source_id, parent_id, attrs(JSON), description, status (active|removed|placeholder), first_seen_scan, last_seen_scan,
  removed_scan, created_at, updated_at`. Table `asset_aliases(kind, value) UNIQUE -> asset_id`.
* Alias kinds and normalisers are in `model.py` (`ALIAS_KINDS`, `normalize_qname`, `normalize_path`, `fabric_alias`). Existing OLCF ids become aliases: item `itm_x` is alias
  `("olcf_item", "itm_x")`, a catalog entry `cat_x` is `("catalog_entry", "cat_x")`, a table is `("lakehouse_table", "<lakehouse item id>:<name>")`, a column is
  `("lakehouse_column", "<lakehouse item id>:<table>:<column>")`.
* **Resolution** (`AssetRegistry.resolve(ref: AssetRef, *, create=True) -> str`): (1) the first alias that hits; (2) `ref.name` qualified with `scope.lakehouse_id`/`scope.lakehouse` (the default lakehouse) when
  the name has fewer than two parts; (3) a unique `qname` match inside the scope's workspace; (4) otherwise create a **placeholder** (`status="placeholder"`, `attrs.unresolved=true`) keyed by its `qname` alias.
  A **column** reference uses `type="column"`, `name=<column>` and `scope["table"]=<table qname or alias-resolvable table ref>`; it resolves the table by the rules above and then the child column
  (case-insensitive), creating the child under the table if the table's columns are not yet known.
* **Linking and merging:** when a discovery scan finds an asset whose alias set touches an existing placeholder, the placeholder is merged into the real asset: aliases move, edges are rewired
  (duplicates collapse, evidence is unioned), children merge by name, the placeholder row becomes `status="removed"` with `attrs.merged_into`. Merging two non-placeholder assets
  happens only through an alias collision and is audited (`intelligence.asset_merged`); it never happens silently across different asset types.
* **Authoritative identity beats names (third review, F04).** The alias kinds `fabric`, `fabric_column`, `pbi`, `onelake_path`, `delta_location`, `lakehouse_table`, `olcf_item` and the like name a physical object and decide identity. A
  qname hit or name rule is only a *hint*: it is refused when the target asset carries a different authoritative identity (an unknown Fabric tenant `*` matches any tenant; two known tenants must agree). Two remote workspaces mapped
  to one local workspace with an identically named `Silver.orders` stay two assets with their own columns and lineage; the refusal is an informational `identity_conflict` gap, and a local-name-only reference becomes a placeholder
  rather than guessing. `AssetRegistry.identity_conflicts()` lists assets that already hold two identities of one kind; repair is manual (`move_alias`, then rescan), never an automatic split.
* **Incomplete analyses keep the last known lineage (F10).** `ApplyStats` carries `blocking_gaps` and `incomplete`. A blocking gap is a `parse_error`, an analysis-cap marker, an analyzer exception, an apply error, or an
  `unsupported_construct` while known edges vanished. On an incomplete analysis the previous edges are kept and flagged `attrs.stale`, count as seen (a sweep cannot remove them), are not cached as complete, and the scan
  ends `partial` (no sweep). Only a complete re-analysis retires what really went and clears the flag.
* **The analysis cache key includes a context fingerprint (F09):** analyzer version, kind, name, default lakehouse, tenant and workspace mapping, dialect, source, semantic-model bindings, parameters, known notebooks and the
  schemas of only the tables the definition mentions (`engine.context_fingerprint`, migration `intelligence` v3 adds `analysis_cache.context_hash`). A new `LineageContext` field must be added to `engine.LINEAGE_CONTEXT_KEYS` (a guard test fails otherwise).
* **Write pacing (R01):** `intelligence.write_batch_max_rows`, `intelligence.write_batch_max_ms` and `intelligence.write_yield_ms` bound how long the graph writer holds the shared SQLite writer lock (see release-readiness R19).
* An asset is never deleted: `status="removed"` records when it was last seen (history and lineage of dropped objects stay queryable).

## 4. Flow of data through the system

```
 Connector.discover(scan) --Facts / DefinitionFact--> Engine.apply_facts / analyze_and_apply --> AssetRegistry + GraphStore --> snapshots
   (discovery, PURE per batch)                          lineage.engine.analyze_definition(defn) -> Facts (PURE analyzers)
 platform events (dataset.read, job.state_changed, ...) --> inbox (durable) --> runtime.facts_from_event --> Engine.apply_facts (verified edges)
```

* A **scan** (`scn_<ULID>`) is one metadata snapshot of one source (`full` or `incremental`). `Engine.begin_scan(source_id, mode, trigger, principal) -> scan_id`,
  `apply_facts(facts, *, scan_id, source_id)`, `analyze_and_apply(defn, *, scan_id, source_id)` (skips analysis when `defn.etag` equals the cached etag for that asset and analyzer version),
  `record_gaps(scan_id, gaps)`, `finish_scan(scan_id, status, stats)`: a *full* scan marks assets/edges of that source that were not seen in this scan as `removed`/inactive (history preserved);
  an *incremental* scan only touches what it reports. Scans give a monotonically increasing `version` per source; `asset_versions(asset_id, scan_id, attrs_hash, attrs)` records attribute changes (schema changes among them).
* **Snapshots/diff:** `GET /intelligence/snapshots/diff?from=<scan>&to=<scan>` lists assets and edges added, removed and changed (column added/removed/type changed) between two scans of a source.
* **Edge identity:** `edge_hash(src_id, dst_id, type, via_id)`; a repeat observation merges evidence (de-duplicated by `Evidence.key()`, capped at 20 newest) and bumps `last_seen_at`/`observation_count`.
  Edge confidence is recomputed as the strongest evidence confidence. A `verified` runtime observation upgrades a `derived` static edge; a later scan never downgrades it while the verified evidence is within the retention window.
* **Virtual edges:** `GOVERNED_BY` and `CLASSIFIED_AS` are served from the governance tables through the same graph API (marked `"virtual": true`), never stored in `edges`.

### 4.1 Data-flow direction
`model.flow(edge_type, src, dst)` is the single source of truth: `READS_FROM` flows dst->src; `WRITES_TO`, `TRANSFORMS` flow src->dst; `DEPENDS_ON`, `USES_MEASURE`, `DISPLAYS` flow dst->src.
Lineage "upstream of X" means following flows backwards from X, "downstream" forwards. Process nodes (notebook, pipeline, activity, sql_query, spark_job, script, dataflow) stay in the picture
between datasets (`Bronze.Customers -> [Notebook CleanCustomers] -> Silver.Customers`); the **column level** uses `TRANSFORMS` edges between column assets with `via` = the process asset and `transformation` text.

### 4.2 What each analyzer must emit (contract between analyzers and the engine)
For a notebook/pipeline/query/report/model definition the analyzer returns `Facts` containing: the process asset (`AssetFact`, type notebook/pipeline/...), `READS_FROM` and `WRITES_TO` edges from the
process to every dataset it touches (evidence with `location` = cell/line or JSON path and `snippet`), `TRANSFORMS` edges column->column with `via`=process and `transformation`, and `Gap`s for what it could
not resolve (dynamic table names, UDFs, unsupported constructs, parse errors). Static edges are `derived`; edges the analyzer can only guess (name similarity) are `inferred`; a dependency it knows exists
but cannot map is `unknown`. Analyzers never touch the database, never execute code, never import notebooks, never call the network.

## 5. Tables (SQLAlchemy Core, portable SQLite/PostgreSQL; each stream is its own migration stream)

Stream `intelligence` (core, `olcf.intelligence.schema`): `assets`, `asset_aliases`, `asset_versions`, `edges` (`id edg_, edge_hash UNIQUE, src_id, dst_id, type, via_id, confidence, transformation, attrs JSON,
evidence JSON (list, newest first, max 20), observation_count, source_id, first_seen_scan, last_seen_scan, removed_scan, first_seen_at, last_seen_at, status`), `scans` (`scn_`, `source_id, mode, trigger, status
(running|succeeded|partial|failed|cancelled), version, started_at, finished_at, stats JSON, error`), `gaps` (`id, scan_id, source_id, kind, subject, detail, remediation, location JSON, first_seen_at, last_seen_at,
resolved_at`), `inbox` (`seq BigPK, type, workspace_id, payload JSON, received_at, updated_at, attempts, status (pending|done|failed|skipped), error`; `updated_at` is when the row last left `pending` or was re-queued, added by migration `intelligence` version 2, which adds the column to an existing database and is a no-op on a fresh one), `analysis_cache` (`asset_id, kind, analyzer_version, etag, analyzed_at, edge_count`).
Stream `intelligence_discovery` (`olcf.intelligence.discovery.schema`): `discovery_sources` (`src_`), `sampling_policies` (`smp_`). Stream `governance` (`olcf.intelligence.governance.schema`): see the governance package.
Later schema changes are new versions, never edits of v1. Search over JSON uses `cast(col, String).like(...)` (portable), never JSON operators.

## 6. Platform events (the only coupling to the rest of the platform)

Published by existing modules (best effort), consumed by the inbox. Payloads are redacted by the event bus. All names are string literals (the reference generator scans for them) and are added to
`EVENT_CATALOG` in `olcf/pipelines/webhooks.py`.

* `dataset.read` / `dataset.written`: `{"workspace_id", "process": {"kind": "sql_query|notebook|spark_job|pipeline|report|job|query", "item_id": str|None, "job_id": str|None, "query_id": str|None, "name": str|None, "run_id": str|None, "fabric"?: {"tenant_id"?, "workspace_id"?, "item_id"?, "item_name"?, "item_type": "Notebook", "run_id"?}},
  "datasets": [{"kind": "table|file", "lakehouse_id": str|None, "name": str|None, "path": str|None, "uri": str|None, "format": str|None, "fabric"?: {"workspace_id", "lakehouse_id"}}], "engine": "duckdb|spark|python", "mode": "append|overwrite|merge|create|None",
  "statement_hash": str|None, "at": iso}`. The `fabric` keys are **additive** and optional (consumers that do not know them ignore them): `process.fabric` is present for a run whose job `origin` is Fabric (copied from `jobs.origin.fabric`, which the adapter stored at submission: Entra tenant, Fabric workspace GUID, notebook item GUID and name, Fabric run id) and `datasets[].fabric` for a dataset of an OLCF lakehouse that records a Fabric lakehouse (`definition.fabric`: workspace and lakehouse GUIDs). Both come from the job row and the storage service, never from what the runner printed, and they are labels for lineage, never an authorisation input. Emitted by `SqlService.execute` (after `history.finish`, reads and writes, every caller path), by `JobService.handle_job_event` (from runner events, §7) and by pipeline copy/sql activities.
* `dataset.statement`: `{"workspace_id", "process": {...}, "sql": str (<= 20000 chars), "dialect": "spark|duckdb|tsql", "default_lakehouse_id": str|None, "default_lakehouse_fabric"?: {"workspace_id", "lakehouse_id"}}` emitted by the runtime for `spark.sql(...)`/`%%sql` executed in a job or session: the
  control plane parses it (sqlglot) into verified table and column edges (the statement really ran).
* `dataset.schema_changed`: `{"workspace_id", "lakehouse_id", "table", "columns_added": [...], "columns_removed": [...], "columns_changed": [...]}` from `CatalogService.sync_lakehouse` / table maintenance.
* Existing and also consumed: `job.state_changed`, `pipeline.run.*`, `item.created|updated|deleted`, `lakehouse.created|deleted`, `workspace.created|deleted`.

## 7. Runtime lineage emission (compute side; stdlib only, no Intelligence import)

The runner (`olcf.runtime_lib`) emits `@@OLCF@@` events `dataset_read`, `dataset_written`, `dataset_statement` (de-duplicated per job, capped at 200 per job); `JobService.handle_job_event` forwards them as the platform events above with the job's
workspace/item/job ids. Instrumentation covers: `LazySpark` (`spark.table`, `spark.read.table/load/parquet/csv/json/orc/format(..).load`, `DataFrameWriter.saveAsTable/insertInto/save/parquet/csv/json`, `spark.sql`), the host-Python helpers
(`deltalake.DeltaTable(...)` reads, `write_deltalake`, DuckDB `con.sql/execute` statement text), and the notebook SQL helper (already covered by `SqlService`). Failure of the probe never fails the user's code.

## 8. Python API of the service (`ctx.service("intelligence")`)

`IntelligenceService`: `registry`, `graph`, `engine`, `resolve(ref, create=True) -> str`, `get_asset(principal, asset_id)`, `search(principal, q=..., type=..., ...)`, `lineage(principal, asset_id, *, direction, depth, level, min_confidence, types, column)`,
`origin`, `impact`, `staleness`, `unused`, `documentation`, `history`, `summary(principal)`, `visible_asset_ids(principal)`; governance and discovery register their own sub-services on the same object
(`service.governance`, `service.discovery`) in their package's `install` hook (called from `module.install`). Authorisation: assets carry `workspace_id`; reads filter with `principal.visible_workspaces("intelligence.read")`;
assets with `workspace_id=None` (external/tenant level) need an organisation-wide `intelligence.read` (`visible_workspaces(...) is None`). Existence is hidden (`NotFound`), never `Forbidden`, for assets outside the caller's scope.

## 9. REST surface (all under `/v1`, permission in brackets)

Core: `GET /intelligence/summary`, `GET /intelligence/assets` (q, type, workspace_id, domain, classification, certification, owner, tag, status, limit, cursor) `[intelligence.read]`, `GET /intelligence/assets/{id}`, `PATCH /intelligence/assets/{id}`
(description, tags; `If-Match`) `[governance.curate]`, `GET .../children`, `GET .../lineage` (direction up|down|both, depth 1..10, level table|column, min_confidence, types, column), `GET .../origin`, `GET .../impact`, `GET .../staleness`, `GET .../documentation` (markdown),
`GET .../history`, `GET .../sample` `[intelligence.sample]`, `POST /intelligence/resolve`, `GET /intelligence/edges/{id}`, `GET /intelligence/snapshots`, `GET /intelligence/snapshots/diff`, `GET /intelligence/unused`.
Discovery: `GET|POST /intelligence/sources`, `GET|PATCH|DELETE /intelligence/sources/{id}`, `POST .../test`, `POST .../scan`, `POST /intelligence/import/scanner`, `GET /intelligence/scans`, `GET /intelligence/scans/{id}`, `POST .../cancel`,
`GET /intelligence/gaps`, `GET /intelligence/coverage`, `GET|PUT /intelligence/sampling-policies` `[intelligence.manage]`. Operations: `POST /intelligence/inbox/requeue` `[intelligence.manage]` puts every `failed` inbox row back to `pending` with a fresh set of attempts and answers `{"requeued": n, "inbox": {...counts}}` (CLI `olcf intelligence inbox requeue`, SDK `client.intelligence.requeue_inbox()`). Governance: under `/governance/*` (glossary terms, domains, data products, classifications, owners, certification,
policies, violations, quality, change requests).

## 10. Settings, permissions, health, audit

* Static settings `intelligence.*` in `core/config.py` (`enabled` default true, `scan_on_start`, `inbox_batch_size`, `inbox_poll_seconds`, ...), runtime settings registered in `module.install`
  (`intelligence.local_scan_interval_seconds`, `intelligence.fabric_full_scan_interval_seconds`, `intelligence.fabric_incremental_interval_seconds`, `intelligence.schema_check_interval_seconds`, `intelligence.retention_days`, `intelligence.max_lineage_nodes` ...), among them three added with the housekeeping work: `intelligence.max_impact_nodes` (20,000; the bound of the impact traversal, independent of `max_lineage_nodes`), `intelligence.max_versions_per_asset` (200; attribute versions kept per asset, trimmed hourly) and `intelligence.inbox_retry_after_minutes` (30; a `failed` inbox row is queued once more after that long, 0 = never), and the boolean `intelligence.allow_private_endpoints` (false). Every field has help text so the generated reference passes.
* Permissions: `intelligence.read`, `intelligence.manage`, `intelligence.sample`, `governance.curate`, `governance.approve` (roles: read to every role that has `lakehouse.read`; curate to Contributor and up; approve and manage to WorkspaceAdmin/SecurityAdmin/PlatformAdmin; sample to
  WorkspaceAdmin and up, never in the `read` token scope). MCP scope gets `intelligence.read` only (ADR 0014: MCP is read-only).
* Health: non-critical check `intelligence` (category `data`): last scan per source, inbox depth/lag, failed inbox rows, gaps open. `quick` never touches the network.
* Audit: literal actions `intelligence.source_created|updated|deleted|tested`, `intelligence.scan_started|finished`, `intelligence.asset_updated|merged`, `intelligence.sample_read`, `intelligence.inbox_requeued` (`details.requeued` = the number of rows), governance actions in the governance package.

## 11. Testing contract

* Pure packages (`lineage/`, `runtime.py`, `discovery/detect.py`, connectors' `Facts` output) are unit-tested without a database; each analyzer has a corpus of positive and negative cases with line/column evidence asserted.
* Core: graph invariants (idempotent apply, evidence merge, confidence upgrade, merge of placeholders, removal sweep on full scans only, snapshot diff), authorisation (visibility, hidden existence), SQLite and PostgreSQL (`postgres` marker).
* **Isolation tests** (`tests/intelligence/test_isolation.py`): (a) with a broken inbox table / a graph that raises, jobs/SQL/pipelines succeed and the failure is counted, not raised; (b) the import-graph test of §1.2; (c) an app built without the module and an app built
  with only core+storage+catalog+intelligence both work.
* Fabric connector: tested against the extended `FakeFabricWorld` (a real HTTP server in the test suite that implements the documented Fabric REST and Power BI admin scanner shapes).
* **System tests** (`tests/intelligence/system/`, the real application with every module, the real analyzers and connectors, no stubs; the Fabric tenant is the `FakeFabricWorld` test server and the run-time tests use a real worker agent): `test_sample_integration.py` and `test_full_platform.py` (the first-run sample as one system through REST, MCP, CLI, SDK), `test_runtime_lineage.py` (a notebook, a script and a crashing job run by a real worker become verified edges), `test_schema_change.py` (Delta rewrite -> catalog sync -> event -> inbox -> scan -> diff -> certification), `test_failure_isolation.py` (broken inbox, graph, subscriber, loops), `test_deployment_shapes.py` (no module / no Fabric / Fabric only / Fabric plus on-premises), `test_fabric_runtime_link.py` (a run submitted from a Fabric notebook attaches to the assets the tenant scan found, in both orders), `test_performance.py` (2000 tables, 20000 columns, 5040 edges; bounds documented in its docstring), `test_authz.py` (a Viewer of one workspace and every surface), `test_portability.py` (nothing SQLite-only in the source; PostgreSQL itself is not run). The limits they found and have not fixed are listed in [`CONTRACTS.md`](CONTRACTS.md) INTEG-Intelligence, II-E; the ones that were fixed afterwards (wide fan-out and impact totals, diff summary totals, the command palette, origin of a column) are not listed there any more.
* Every new route is covered by `tests/security/test_authorization.py` expectations; every route has an SDK method (`tests/tooling/test_sdk_coverage.py`); generated docs are regenerated (`gen_*.py`), `check_docs.py`, `check_headers.py`, `check_docstrings.py` and ruff pass.
