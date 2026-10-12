# OLCF Intelligence

Author: Justin Dodrill. Applies to `src/olcf/intelligence/` (core, `lineage/`, `discovery/`, `governance/`), the `intel_*` and `governance_*` MCP tools, `olcf intelligence` and `olcf governance`, and the Intelligence pages of the console. The engineering contract that the code was built against is [docs/dev/INTELLIGENCE.md](../dev/INTELLIGENCE.md); where this page and the contract differ, this page describes what the code does (the differences are listed in [Where the code and the earlier design notes differ](#where-the-code-and-the-earlier-design-notes-differ)).

OLCF Intelligence answers four questions about the data an organisation owns: **what do we have, where does it come from, what depends on it, and who is responsible for it.** It does this by *reading* the platform and, optionally, a Microsoft Fabric or Power BI tenant. It never changes the data and, in this release, it never blocks work.

Everything it says is labelled with how sure it is and how it knows, and everything it could not see is listed. It does not claim complete lineage for any source.

## In plain language

* A **scan** reads the places Intelligence has been pointed at (this installation by default; a Fabric tenant or a saved Power BI scanner file if you add one) and records what exists: workspaces, lakehouses, tables, columns, notebooks, pipelines, saved queries, reports, semantic models.
* The **lineage engine** reads the *code and definitions* it found (notebooks, pipeline JSON, SQL, report and model definitions) without running them, and works out which table feeds which table, and which column feeds which column.
* While jobs, queries and pipelines run, the platform publishes ordinary events saying what was really read and written. Intelligence turns those into **verified** connections.
* **Governance** is a layer of business meaning on top of the same graph: a glossary, domains, data products, owners, sensitivity labels with a review queue, certification, policies and a quality score. It observes and reports. A policy never stops a job.
* Anything that cannot be worked out is recorded as a **gap** with the reason and the fix, instead of being guessed.

## The asset graph

The unit is the **asset**: anything the platform knows about. An asset has a type, a name, a human path (`Sample Retail / retail_lakehouse / fact_sales`), a workspace, attributes and a status. The relationships between assets are **edges**, each with a type, a confidence and its evidence.

Asset types (`olcf.intelligence.model.ASSET_TYPES`):

| Group | Types |
|---|---|
| Containers | `tenant`, `workspace`, `domain_scope`, `lakehouse`, `warehouse`, `schema` |
| Data | `table`, `view`, `column`, `file`, `source_system`, `model_table`, `external` |
| Processes (they run and move data between datasets) | `notebook`, `pipeline`, `activity`, `sql_query`, `spark_job`, `script`, `dataflow` |
| Consumers (they present data) | `semantic_model`, `measure`, `report`, `page`, `visual`, `dashboard` |

Which of these a source actually produces is narrower than the list: the local source produces workspaces, lakehouses, tables, columns, notebooks, pipelines, saved queries (`sql_query`), reports, visuals, Spark job definitions and (for paths you list) files; the Fabric source adds warehouses, semantic models, dataflows and the Power BI objects below the report. `tenant`, `domain_scope` and `schema` are in the vocabulary; no shipped connector emits them.

Edge types (`EDGE_TYPES`) and the direction data flows. Edges are stored the way they read in English (`notebook WRITES_TO table`); the direction of *data* is derived by one function, `model.flow`, so traversal never special-cases a type:

| Edge | Reads as | Data flows | Used for |
|---|---|---|---|
| `READS_FROM` | process reads dataset | dataset to process | table-level lineage |
| `WRITES_TO` | process writes dataset | process to dataset | table-level lineage |
| `TRANSFORMS` | source column feeds target column (`via` = the process, `transformation` = the expression) | source to target | column-level lineage |
| `DEPENDS_ON` | A needs B (notebook calls notebook, report uses model, activity follows activity) | B to A | dependencies |
| `USES_MEASURE` | A uses measure B | B to A | Power BI models |
| `DISPLAYS` | visual shows column or measure | field to visual | reports |
| `CONTAINS` | structure: workspace holds lakehouse holds table holds column | none | the tree |
| `EXECUTES` | an activity runs a notebook or job | none | pipelines |
| `GOVERNED_BY`, `CLASSIFIED_AS` | virtual edges served from the governance tables | none | not stored in the edge table |

"Upstream of X" means following data flow backwards from X, "downstream" forwards. Process nodes (a notebook, a pipeline) stay in the picture between datasets, because "which notebook wrote this" is usually the question: `Bronze.Customers -> [notebook CleanCustomers] -> Silver.Customers`. Column lineage uses `TRANSFORMS` edges between column assets with the process in `via`.

## Stable asset ids and aliases (the `ast_` id problem)

The same table is known to different systems by different names:

| Who calls it | How it is written | Alias kind |
|---|---|---|
| The OLCF item store | the lakehouse is `itm_01...`, the table is a name inside it | `olcf_item`, `lakehouse_table` (`<lakehouse item id>:<table>`) |
| The classic catalog | `cat_01...` | `catalog_entry` |
| Delta on disk or OneLake | `abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Tables/sales` | `delta_location`, `onelake_path` (normalised, case-folded for cloud paths) |
| Microsoft Fabric | `<tenant>/<workspace guid>/<item guid>` | `fabric` |
| Power BI | `<workspace guid>/<dataset guid>/<table>/<column>` | `pbi` |
| Code | `Silver.Sales`, `` `silver`.`sales` ``, `[Silver].[Sales]` | `qname` (normalised: unquoted, lower-case, scoped to a workspace) |
| An ad-hoc statement | the SQL text | `query` (`sha1:<hash of the normalised statement>`) |

If each system kept its own id, "the Silver sales table" would be five unrelated nodes and no lineage could cross a boundary. So every object gets exactly one **`ast_<ULID>`** id, and every other identifier is an **alias** that points at it. `(alias kind, value)` is unique across the whole graph: two sources that name the same object can only ever end up on one asset. The `ast_` id is what compute, lineage, governance, discovery, the REST API, the CLI, the SDK and the MCP tools store and exchange. The CLI and the MCP tools also accept a name, a `lakehouse.table` path or `table.column` and tell you when that is ambiguous.

How a reference is resolved (`AssetRegistry.resolve`), in this order:

1. any alias that already exists;
2. a partial name qualified with the default lakehouse of the notebook or job (`spark.read.table("sales")` in a notebook whose default lakehouse is `Silver`);
3. a unique qualified-name match inside the workspace;
4. otherwise a **placeholder** asset (`status = placeholder`, `attrs.unresolved = true`), keyed by the name it was seen under.

Edges to a placeholder are kept, not dropped, and the reference is reported as an `unresolved_reference` gap. When a later scan finds the real object whose aliases touch the placeholder, the placeholder is **merged** into it: aliases move, edges are rewired (duplicates collapse, evidence is unioned), child columns merge by name, and the placeholder row becomes `removed` with `attrs.merged_into`. Merging two non-placeholder assets happens only on an alias collision, is audited (`intelligence.asset_merged`) and never happens across different asset types.

An asset is **never deleted**: `status` moves `active` to `removed` when a full scan no longer sees it. Lineage and history of dropped objects stay queryable (`--include-removed`).

Qualified names are stored per scope (workspace, or Fabric tenant and workspace) because two workspaces can both contain a lakehouse called `Silver`.

**Identities are authoritative, names are hints.** The aliases that name a real object (`olcf_item`, `fabric`, `fabric_column`, `pbi`, `onelake_path`, `delta_location`, `lakehouse_table` ...) decide which asset a fact belongs to. A qualified name (`qname`) is derived from names, so it can be shared: two remote Fabric workspaces mapped to one OLCF workspace may each hold `Silver.orders`. The registry never joins two objects whose authoritative identities differ (the same kind of alias with a different value; an unknown Fabric tenant `*` matches any tenant, two known tenants must agree), however alike their names are. They stay two assets with their own parents, columns and lineage; the shared local name is removed from both and the second one is reported once as an `identity_conflict` gap (informational), so a reference that only knows the local name stays an unresolved placeholder instead of silently picking one table, while a reference that carries its remote scope (`tenant` and `fabric_workspace`, as the Fabric connector and the analyzers give it) still selects the right one. Names that do not conflict keep joining as before (a local table and the Fabric table of the same lakehouse). Renaming a remote item or rescanning in any order changes no id.

*Repairing assets that an older build conflated.* An asset that already carries two different authoritative identities of one kind was joined before this rule existed. `IntelligenceService.identity_conflicts(principal)` (and `Engine.report_identity_conflicts(scan_id, source_id)`, which records the findings as `identity_conflict` gaps) lists them read-only with the identities that were joined; to split one, remove the wrong alias with `AssetRegistry.move_alias` and re-scan the sources, which re-creates the other object. There is deliberately no automatic split: attached classifications and certifications must be reviewed against the right physical object first.

## Medallion lineage

A medallion layout (bronze raw files, silver cleaned tables, gold marts, then reports) is the common case, and the graph shows it as datasets joined through the processes that move them:

```mermaid
flowchart LR
    raw[(Files/raw/sales.csv<br/>file)] -->|READS_FROM| nb1[[notebook<br/>01_Retail_Medallion_ETL]]
    nb1 -->|WRITES_TO| silver[(silver_sales_enriched<br/>table)]
    silver -->|READS_FROM| nb2[[notebook<br/>build gold]]
    nb2 -->|WRITES_TO| gold[(gold_daily_sales<br/>table)]
    gold -->|READS_FROM| rep[/report tile: Revenue/]
    silver -. column .-> gold
```

At column level the same edges are drawn between the columns of those tables (`silver.net_amount` to `gold.net_revenue`, `via` = the notebook, `transformation` = `ROUND(SUM(net_amount), 2)`). Column lineage is only as good as what the analyzer could prove: a `SELECT *` over a table whose columns were not yet known, a dynamic table name or a user-defined function stops it, and the stop is reported as a gap. The exact list of what each analyzer understands is [lineage support](../reference/lineage-support.md).

## Confidence levels

Every edge carries one of four confidence levels, and an edge's confidence is the **strongest** of the evidence it has. A lineage *path* is only as strong as its **weakest** edge (the `confidence_floor` of a node, the confidence of a path in `where-from`).

| Level | How it arises, exactly | Typical source |
|---|---|---|
| **Verified** | The platform *observed it happen*: a `dataset.read` or `dataset.written` event from a job, query or pipeline activity that really ran, or the SQL text of a statement that really ran (`dataset.statement`), parsed. Only the run-time translator (`olcf.intelligence.runtime`) produces it. | a notebook job wrote `silver.sales` at 02:14 |
| **Derived** | A static reading of code or of a definition: SQL and PySpark parsed, a pipeline's activities, a semantic model's partitions and DAX, a report's model binding, a Power BI scanner result, a row of the classic catalog's recorded lineage. The thing is stated in the artefact; nothing executed. | `INSERT INTO gold.t SELECT ... FROM silver.s` in a notebook cell |
| **Inferred** | The analyzer had to *assume*: a table name that depends on a notebook parameter (analysed with its default, which a pipeline may override), a column assumed to keep its name, an insert into a table of unknown schema mapped by select-list names, a function defined but never called, an M column assumed to have its model name, or a classification possibly reaching a report only through table-level lineage. | `spark.table(f"{layer}.sales")` where `layer` is a parameter |
| **Unknown** | A relationship is known to exist but its content cannot be determined, or an edge arrived with no evidence at all. | see the note below |

**Run-time evidence upgrades static evidence.** If a notebook's code says it writes `gold.t` (derived) and a job then really writes it, the edge becomes verified. A later static scan never downgrades it while the run-time observation is younger than `intelligence.verified_evidence_days` (90 by default); older run-time evidence is pruned, and the edge falls back to what the code shows. An edge that the code no longer shows but that was verified recently is kept through the removal sweep.

**Do not read more into a level than it says.** *Verified* means the platform saw a process touch a dataset. It does not mean every column mapping on the edge is proven: column mappings from a runtime statement are upgraded to verified only if the analyzer derived them (guessed ones keep `inferred`). *Derived* is not a guarantee either: it says "the code says so", and the code may not be what runs.

**Unknown is rare in practice.** `unknown` is part of the vocabulary, of every filter (`--min-confidence unknown`) and of the fallback for an edge that has no evidence. In this release none of the static analyzers or connectors *emits* an edge at `unknown`; what they cannot place they report as a gap instead. This is stated so that a count of "unknown 0" in a summary is not mistaken for completeness: the incompleteness is in the gaps.

The user interface draws the four levels with different line styles and badges so they cannot be confused at a glance, and every lineage view, the impact analysis and the MCP answers can be restricted with a minimum confidence.

## Evidence

An edge keeps a list of evidence records, newest first, at most 20, de-duplicated; the newest record of every confidence level is always kept. A record says:

* **kind**: `runtime`, `definition`, `static`, `metadata`, `naming`, `manual` or `api`;
* **source**: who saw it (`sql-analyzer@1`, `pyspark-analyzer@1`, `runtime:job`, `catalog-mirror`, `fabric-scanner` ...); static evidence names the analyzer *and its version*, so a later scan can tell which version of which analyzer produced an edge;
* **location**: where in the source (`{"item": "itm_...", "cell": 3, "line": 12}` for a notebook, a JSON path for a pipeline, a model part and path for Power BI);
* **snippet**: the code or expression, cut to 400 characters;
* **ref** (job, query, scan or definition etag) and **observed_at**.

Evidence never contains secrets or data values. Snippets, gap texts, asset names and attributes, and URI-shaped aliases are passed through the redactor (`password=`, tokens, SAS signatures, bearer values and the password in `scheme://user:password@host` do not reach the graph).

## Gaps: why the platform tells you what it cannot see

A **gap** is something discovery or analysis could not do, stored as data with a reason, a location and a remediation. Gaps are first-class because the alternative, a lineage graph that looks complete, is worse than useless: someone will rely on it.

| Kind | Meaning | Typical fix |
|---|---|---|
| `permission_denied` | The scanning identity may not read it (a Fabric workspace, an item's definition, OneLake). | Grant the role the gap names; see the [troubleshooting table](../guides/intelligence.md#troubleshooting). |
| `api_unavailable` | The API refused, is not enabled for the tenant, or was throttled past the wait limit. | Enable the tenant setting the gap names, or retry later. |
| `unsupported_construct` | Code uses something an analyzer does not model (Scala cells, `PIVOT`, a table function, `SELECT *` over an unknown schema). | Use a supported form, or accept table-level lineage for that statement. |
| `dynamic_reference` | A table or path is computed at run time and is not a compile-time constant. | Use a literal name, or rely on run-time (verified) lineage. |
| `parse_error` | The text is not valid for its language. | Fix the code. |
| `not_inspected` | Deliberately not looked at: code that lives outside the definition (a Spark job that only names a file), a Databricks notebook, data a scan option did not return. | Often none; the gap says what to switch on. |
| `unresolved_reference` | A name could not be tied to a known asset (a placeholder was created). | Run a scan of the source that contains it. |
| `identity_conflict` | Two different objects share a qualified name (or one asset holds two different identities of one kind). Informational; both are kept apart. | Qualify the reference with its Fabric workspace, or map the workspaces to different OLCF workspaces. |
| `sampling_disabled` | Value profiling was requested but no sampling policy allows it. Informational. | Create a [sampling policy](../guides/intelligence.md#sampling-policies) or ignore. |

A gap marked **informational** says "less detail about something that was found" (an unreadable table, missing statistics). A **blocking** gap says "objects may have been missed". The distinction decides the scan status: a full scan with only informational gaps ends `succeeded`; one with a blocking gap ends `partial` and **does not run the removal sweep**, so a permission error or a throttled workspace can never make the graph forget what it merely could not see.

A definition whose analysis is **incomplete** also makes the scan `partial`: a `parse_error`, an analyzer exception, a hit analysis cap (`more than N edges`, a definition `larger than` its limit, `only the first N` cells) or an `unsupported_construct` next to edges the previous analysis knew and this one no longer finds (`ApplyStats.blocking_gaps` / `incomplete`). The engine does not retire those edges: it keeps the last known lineage, flags the edges `stale` (`attrs.stale` with the scan that last confirmed them; the lineage and edge documents carry `"stale": true`), keeps the old gaps open, does not cache the result (the next scan analyses the definition again) and sweeps nothing on its strength. Only a complete analysis retires the edges that really went, and clears the flag on those it finds again.

Gaps are open until a later scan no longer reports them (then they are resolved, not deleted). The overview, the lineage pages, the impact analysis, the CLI (a final "lineage is incomplete: N gap(s)" line) and every MCP answer (`complete: false`) show them.

## Three sources of lineage

| Source | What it sees | Confidence | Cost and limit |
|---|---|---|---|
| **Run time** (events) | What jobs, SQL queries and pipeline copy activities really touched. Needs OLCF compute. | verified | Only what ran, only through the covered APIs (Spark `table`/`read`/`write`/`sql`, `deltalake`, DuckDB, the SQL service, copy activities). Not covered: Spark Connect, streaming, `DeltaTable.merge/update/delete` objects, JDBC, pandas and polars readers. Capped at 200 events per job. |
| **Static** (definitions) | What stored code and definitions say: notebooks, pipelines, SQL, Spark job definitions, reports, semantic models. Nothing is executed or imported. | derived or inferred | Limited by what the analyzers model ([lineage support](../reference/lineage-support.md)). |
| **API and metadata** | What a platform reports about itself: Delta logs, the classic catalog, Fabric REST, Power BI admin scanner results. | derived (relationships), and the existence of assets | Limited by permissions, quotas and tenant settings; run the connection test against your tenant first. |

The three are complementary. Static analysis sees code that has not run yet, and code paths that run rarely; run-time evidence proves what actually happened, including through dynamic names the static analyzer reports as gaps. The graph merges them on the same edge.

## Scans and snapshots

A **scan** (`scn_<ULID>`) is one metadata snapshot of one source, `full` or `incremental`, with a version that increases per source. Every batch of facts a connector produces (one lakehouse, one Fabric workspace, one scanner batch of at most 100 workspaces) is committed in its own transaction, so a crash loses at most the current batch.

* A **full** scan that ends `succeeded` marks the assets and edges of that source that it did not see as `removed`, and resolves gaps it no longer reports.
* An **incremental** scan, and any scan that ends `partial`, `failed` or `cancelled`, never removes anything. A full scan limited to some workspaces never removes anything either.
* An incremental scan of a source that never finished a scan is promoted to full.
* Definitions are analysed only when they changed: the engine caches the analysis per definition by its etag, the analyzer version and a **fingerprint of the resolution context** (default lakehouse binding, the Fabric tenant and workspace mapping, dialect, semantic-model name to GUID map, parameters and known notebooks, and the schemas of the tables the definition mentions that `SELECT *` is expanded with), and keeps an unchanged definition's results alive through a full scan. Changing the workspace's default lakehouse, renaming a container or adding a column to a table the code reads re-analyses a definition whose text is byte-identical; volatile context the analyzers do not read (evidence locations, timestamps) and schemas of tables it never mentions do not. Rows cached before the fingerprint existed are analysed once more.
* There is at most one running scan per source, across all control-plane replicas (a lease on the source row, with a heartbeat); a second request gets `409 Conflict` naming the running scan.
* **Snapshots and diff**: `olcf intelligence snapshots` lists scans, `olcf intelligence diff` shows the assets and edges added, removed and changed (a column added, removed or retyped) between two of them. Attribute history is kept per asset (`asset_versions`).
* The **run-time scan**: events are applied under one synthetic scan per UTC day, mode incremental, so a full scan of any other source never sweeps run-time edges.

## Governance: observe by default

Governance is the optional package `olcf.intelligence.governance`. It adds, on the same assets:

* a **business glossary** (terms, synonyms, abbreviations, links to assets);
* **domains** (a hierarchy) and **data products** (a governed set of assets with owners, an SLA and a contract);
* **owners, stewards, custodians and contacts**, inherited down the asset tree (column, table, lakehouse, workspace);
* **classifications** (sensitivity labels: built-in `PII`, `PCI`, `Financial`, `Credentials`, `Restricted` and your own) with a **review queue**: column names and types produce *suggestions* with a confidence; a person approves them. Nothing is classified automatically. Approved labels **propagate along lineage** as computed, never stored, inherited assignments, with a depth limit and a minimum edge confidence, and an *exposure* query answers "which reports and tables does PII reach?", separating proven exposure from possible exposure;
* **certification** (none, draft, certified, deprecated) and **change requests** with reviewers, comments and four-eyes approval;
* **policies** with seven rule types and **violations** (idempotent, with an acknowledge and exception workflow);
* an explainable **quality score** (documentation, ownership, classification, freshness, certification, lineage, violations, schema stability), with the points lost and why.

"Observe" is literal. Nothing in the compute, SQL or pipeline path calls governance. A policy has a mode; the only mode that exists is `observe`, and a request for `enforce` is refused with `422 unsupported` and a remediation that says so. A violation is a record and an event (`governance.violation.opened`), not a block. If you need enforcement today it has to be built from those events and webhooks outside the platform; this release does not provide it.

## Who sees what

Visibility is part of the model, because lineage is an information-disclosure channel: a graph that crosses workspaces can tell a reader that a table they cannot open exists, who reads it, and what it contains.

* Assets carry the workspace they belong to. A read is filtered with the caller's `intelligence.read` scope; an asset in a workspace the caller cannot read **does not exist for the caller**: `404 not_found`, never `403`.
* Traversal does not enter such assets. A lineage walk stops at the boundary as if the node were not there; it does not return a redacted placeholder, so a reader cannot count hidden neighbours. Evidence that passes through a hidden asset (inheritance sources, exposure paths) is shown as `{"hidden": true}`; data-product member counts report `hidden_members` as a number.
* Assets with no workspace (tenant-level, external systems) need an organisation-wide `intelligence.read`.
* Snapshots, diffs and edges are filtered the same way. A source's configuration (identities, endpoints, schedule) is shown only to a holder of the organisation-wide `intelligence.manage`.
* The consequence for completeness: a user with narrower access sees *shorter* lineage and an impact analysis that omits what they cannot see. The answer says it is incomplete only for reasons it can disclose (open gaps, truncation); it cannot tell you that something hidden was left out. An administrator's view is the complete one.
* The MCP tools use the caller's visibility, and a token scoped `mcp` is narrowed further (see [the MCP guide](../guides/mcp.md#intelligence-tools-where-data-comes-from-and-what-depends-on-it)).

The role matrix is in [permissions](../reference/permissions.md) and the operating view of it is in [the guide](../guides/intelligence.md#who-can-see-and-do-what).

## Failure isolation and the no-circular-dependency rule

Intelligence is built so that it can fail without anyone noticing.

1. **Compute never waits for Intelligence.** Jobs, SQL, pipelines and the catalog publish ordinary platform events through `core/dataset_events.py`, in a `best_effort` block. The event bus (`core/events.py`) delivers to in-process subscribers, the Intelligence inbox writer among them, on a delivery lane of their own per subscriber (one thread each, order kept per subscriber), and a publisher waits for the subscribers of an event for at most 0.5 seconds in total: a subscriber that is slow or hung costs the publisher at most that, and its events queue (bounded at 10,000; the oldest are dropped and counted, while the persisted event is not lost). An exception is logged once and swallowed, and the event carries identifiers and names, never values. Persisting the event is bounded too: on SQLite the bus waits at most 0.25 seconds for the single write lock and then hands the event to a background persister (in order; subscribers receive it at once, without its `seq`, which is filled in as soon as it is stored). Intelligence consumes them asynchronously through a durable `inbox` table. The subscriber only appends a row; it never raises into the publisher, and after a few consecutive failures it backs off for 30 seconds instead of paying for a broken table on every event. If the module, its tables or its graph are off, broken or slow, jobs and queries behave exactly as before (`tests/intelligence/test_isolation.py`: a broken inbox and a graph that raises, with SQL and jobs still succeeding).
2. **The dependency direction is one-way.** The Intelligence core (`module`, `schema`, `registry`, `graph`, `engine`, `service`, `queries`, `ingest`, `api`, `settings`, `model`, `runtime`) imports only `olcf.core`, `olcf.db` and `olcf.security` (and `olcf.api.deps` for the HTTP helpers every router uses), and a test enforces that. The `discovery` sub-package, which has to read other parts of the platform, additionally imports the classic catalog's table definitions (read-only), the Fabric adapter's token and HTTP code and the pipelines' leader-loop helper; the SQL analyzer (`lineage/sql.py`) imports the SQL function allowlist (`sql/spark_functions.py`); the governance package stays inside Intelligence. All of those edges point *out of* Intelligence. **No compute-side package imports it.** Outside `src/olcf/intelligence/` it is named by the two registration lists (`api/modules.py`, `db/migrate.py`), by the MCP tool modules, which look the service up by name while it is installed, and by the sample installer (`sample/installer.py` looks the service up; `sample/intelligence.py` gives the sample its Intelligence content, and runs only when the module is installed). An import-graph test (`tests/intelligence/test_isolation.py`) fails the build if `compute`, `worker`, `runtime_lib`, `sql`, `pipelines`, `catalog`, `storage`, `notebooks` or `fabric` import it or look it up.
3. **Governance is optional inside Intelligence the same way.** Nothing on the execution path mentions governance; the Intelligence core works without the governance tables or package (`tests/intelligence/governance/test_isolation.py`), and recovers when they return.
4. **Compute never trusts what the job says.** The runner reports dataset events from inside user code, which is untrusted: control-plane validation (`compute/lineage_events.py`) keeps only the documented keys, clips every string, strips query strings from URIs, accepts a `lakehouse_id` only if that lakehouse is attached to the job, takes the process, workspace and engine from the job row rather than the event, and forwards at most 200 events per job attempt. `OLCF_DATASET_EVENTS=0` in the job environment turns the probe off.

5. **Resource boundary (the honest limit).** Exception isolation is not resource isolation. On the default SQLite store the graph writer, the event bus, the job and session state and the audit chain share **one writer lock, one process and one connection pool**. What the code does about it: every graph write is a short transaction (one fact, one definition, one sweep chunk of 5,000 rows, one retirement chunk of 1,000 assets, 500 pruned edges, 50 trimmed histories); a writer thread that has been writing for `intelligence.write_batch_max_ms` (250) or `intelligence.write_batch_max_rows` (500 units) pauses for `intelligence.write_yield_ms` (100, SQLite's longest retry sleep) so that waiting writers get in (before this, a 4,000-asset scan starved one event publication for 4.2 seconds although no transaction was longer than 51 ms); the merge audit record is written on the merge's own connection and notifications are sent after the commit, never inside the lock; and the event bus bounds its own wait as described above. Measured on SQLite while a scan of 3,400 assets runs: worst event publication 0.26 s, worst core row write 0.33 s (`tests/intelligence/system/test_write_isolation.py`, bounds 1.0 s and 1.5 s). **What remains:** other SQLite writers still wait up to roughly `write_batch_max_ms + write_yield_ms` (0.35 s) behind a busy graph writer, a scan is about 40 % slower than unpaced because of the pauses, a second tenant of the database (a separate process) gets no pacing at all, and CPU, memory and connection-pool budgets are not enforced. A shared database means the complete answer is a separate store or process for the graph behind the same stable-id contract (APIs, not a shared lock); **PostgreSQL has no single-writer lock**, nothing is paused there, and it is the recommended store for an estate that scans while jobs run. The release checklist item R19 tracks the remaining acceptance work.

```mermaid
flowchart TB
    subgraph found [Foundation]
        core[core] ~~~ db[db] ~~~ security[security]
    end
    subgraph exec [Execution path: knows nothing of Intelligence]
        compute[compute / worker / runtime_lib]
        sql[sql]
        pipelines[pipelines]
        catalog[catalog]
        storage[storage]
        notebooks[notebooks]
        fabric[fabric adapter]
    end
    subgraph intel [olcf.intelligence]
        corei[core: registry, graph, engine, queries, inbox]
        lin[lineage analyzers: pure]
        disc[discovery: connectors, scans]
        gov[governance: observe only]
        runtimei[runtime: events to facts]
    end
    exec -- "publish events (best effort)" --> bus((event bus))
    bus -- "append only" --> inbox[(inbox table)]
    inbox --> corei
    disc --> corei
    lin --> corei
    runtimei --> corei
    gov -. "optional, reads the graph" .-> corei
    intel --> found
    exec --> found
    disc -. "maybe_service: catalog, storage, connections, secrets, fabric http" .-> exec
```

The dashed arrow from discovery to the execution-path packages points out of Intelligence: it reads the classic catalog's tables and the storage service, and the Fabric connector reuses the adapter's HTTP and token code. None of those packages imports back.

## Module independence

The three supported shapes are described, with a diagram of each, in [Intelligence on-premises, with Fabric, and against Fabric only](intelligence-fabric-and-onprem.md). In one line each:

* **Compute and Data without Intelligence.** Leave the module out (`create_app(module_names=...)`) or set `intelligence.enabled = false`: the platform is unchanged. (With `enabled = false` the REST routes stay; collection stops.)
* **Intelligence without OLCF compute.** Scans need no runtime, worker or Spark: the local connector reads Delta metadata with delta-rs, the Fabric connector speaks REST, sampling uses delta-rs or DuckDB. Pointed at a Fabric tenant, it works with no OLCF compute at all (but then there is no run-time evidence).
* **The full platform.** Local scans, run-time events from OLCF compute, and optionally a Fabric source, in one graph.

## Data flow

```mermaid
flowchart LR
    subgraph sources [Sources]
        loc[local: workspaces, items,<br/>Delta metadata]
        cat[classic catalog<br/>read only]
        fab[Fabric REST,<br/>OneLake, Power BI scanner]
        imp[saved scanner JSON]
        run[job / SQL / pipeline events]
    end
    loc & cat & fab & imp --> conn[Connector.discover<br/>batches of Facts and DefinitionFacts]
    conn --> an[lineage.engine.analyze_definition<br/>pure, deterministic]
    an --> eng[Engine.apply_facts]
    conn --> eng
    run --> ibx[(inbox)] --> rt[runtime.facts_from_event<br/>pure] --> eng
    eng --> reg[AssetRegistry<br/>resolve, ensure, merge]
    eng --> gr[GraphStore<br/>assets, edges, evidence, gaps, versions]
    gr --> q[queries: lineage, origin, impact,<br/>staleness, unused, docs, diff]
    q --> api[REST /v1/intelligence]
    api --> ui[Console] & cli[olcf intelligence] & sdk[SDK] & mcp[MCP intel_* tools]
    gr --> govp[governance: reads, decorates, evaluates]
    govp --> apig[REST /v1/governance]
```

Connectors and analyzers are **pure with respect to the graph**: they return plain records (`Facts`, `DefinitionFact`) and never write to the database. The engine is the only code that turns them into rows, one fact per serialised transaction, so a fact that fails is counted without losing the others and concurrent writers (a scan and the run-time inbox) never interleave inside an edge's read-modify-write. The analyzers touch no database, network or file system, execute and import no user code, and are bounded (maximum edges, text sizes, nesting; hitting a cap records a gap saying what was skipped).

## Naming: "catalog"

The word is overloaded in OLCF. Three things carry it and only one is new:

* The **classic catalog** (`olcf.catalog`, tables `catalog_entries`, `table_stats`, `lineage_edges`, routes `/v1/catalog` and `/v1/catalogs`, `olcf catalog`, the MCP tools `search_catalog`, `describe_table`, `get_lineage`): the index of lakehouse tables with search, statistics and the lineage that SQL and copy activities recorded. **It is unchanged.** Intelligence *reads* it (the local source mirrors `lineage_edges` by default; a `catalog_mirror` source additionally mirrors `catalog_entries`) and never writes to it.
* The **Spark catalog name** that appears in lakehouse settings and in `/v1/catalogs` (the `catalog` field of a lakehouse definition, `default` unless set) and in the data references of jobs. It names a Spark catalog, not an index.
* The **asset graph**, called the *Intelligence catalog* in the code and the **Data catalog** in the console (**Intelligence, Data catalog**). It is the new, larger thing: every asset kind, every alias, edges with confidence and evidence, gaps, history and governance facts.

If you are looking for "the catalog" to feed an old integration, use `/v1/catalog`. If you are looking for lineage with confidence and column level, use `/v1/intelligence`.

## Limits worth knowing before you rely on it

* **Fabric and Power BI follow Microsoft's documented API shapes.** The Fabric connector and the scanner import are covered by the automated suite through a local test server (`tests/fabric/fabric_fakes.py`); tenant settings, roles and quotas differ between organisations, so run the connection test first. See [the guide](../guides/intelligence.md#connecting-microsoft-fabric-and-power-bi) and [release readiness](../development/release-readiness.md).
* **Lineage is incomplete by construction.** Complete column-level lineage is not claimed for any source. Read the gaps.
* **Static analysis is a reading of text.** It does not know what a notebook parameter will be, what a UDF does, or which code path will execute.
* **Observe only.** No enforcement exists.
* **Sampling is off** until an administrator creates a policy, and sample values are never stored.
* **Scale limits** are caps, not guarantees. A lineage answer is cut at `intelligence.max_lineage_nodes` (500) dataset-like nodes and says `truncated`; the process nodes between datasets (notebooks, pipelines) have a wider allowance of their own, so one table read by thousands of notebooks does not use up the budget meant for tables, and the rendered graph prefers datasets. The impact analysis counts with a traversal of its own, bounded by `intelligence.max_impact_nodes` (20,000): its totals are real numbers up to that bound and `truncated` is set only when the bound cut it short, while the lists stay within `max_lineage_nodes`. The origin of a column is answered at column level (the answer says so in `level_switched`), because a table-level walk starts at the column's table and has no path of the column. Origin lists at most 100 paths. Governance evaluations look at no more than `governance.max_evaluation_assets` (20,000); list filters carry at most 20,000 ids. Bounds were exercised by a system test on 2,000 tables and 20,000 columns on SQLite; no performance benchmark exists for large estates.
* **Retention.** Processed inbox rows are purged after `intelligence.retention_days` (30); run-time evidence after `intelligence.verified_evidence_days` (90); resolved policy violations after `governance.violation_retention_days` (365); an asset keeps its newest `intelligence.max_versions_per_asset` (200) attribute versions, older ones are trimmed by the hourly housekeeping. A `failed` inbox row is queued once more after `intelligence.inbox_retry_after_minutes` (30; 0 = never) and, if it fails again, stays failed until an operator re-queues it (`olcf intelligence inbox requeue`). Assets, aliases, edges and gaps are kept; removal marks them, it does not delete them. The tenant root asset of a Fabric source records the outcome of its last scan (`last_scan_status`, `last_scanned_at`), so a source whose scan failed is visible on the asset.
* **PostgreSQL variants** of the Intelligence tests (`postgres` marker) exist and run when `OLCF_TEST_POSTGRES_URL` is set.

## Where the code and the earlier design notes differ

Found while writing this page; the code is right by definition and the notes should be corrected:

* `docs/dev/INTELLIGENCE.md` section 1.2 lists "the sample installer" as allowed to look the service up. While this page was being written the second import-graph test did not yet allow it and failed; the test now allows `sample/installer.py` and `sample/intelligence.py` to look the service up by name (not to import the package) and `tests/intelligence/test_isolation.py` passes. The sample's Intelligence content therefore reaches the module only through the service on the context.
* The scheduler also has `intelligence.local_full_scan_interval_seconds` (86,400), which the contract's list of settings omits.

## Where next

[Guide: using Intelligence](../guides/intelligence.md) (first look, scans, lineage, the five questions, Fabric, governance, troubleshooting) · [Lineage support matrix](../reference/lineage-support.md) · [On-prem, Fabric, or both](intelligence-fabric-and-onprem.md) · [ADR 0017](../adr/0017-intelligence-subsystem.md) · [Security model, Intelligence section](security-model.md#intelligence-discovery-lineage-and-sampling) · [REST](../reference/api.md), [permissions](../reference/permissions.md), [settings](../reference/configuration.md), [events](../reference/events.md), [MCP tools](../reference/mcp-tools.md).
