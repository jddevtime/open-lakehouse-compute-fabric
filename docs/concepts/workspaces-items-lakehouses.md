# Workspaces, items and lakehouses

## Workspaces

A **workspace** is the primary boundary for security, configuration and collaboration. It has an immutable id (`wks_...`), a unique name, display name, description, tags, settings, an optional quota, an ETag for optimistic concurrency, and members: role bindings scoped to the workspace ([security model](security-model.md#authorisation)). Workspaces are soft-deleted: a deleted workspace sits in the recycle bin (`GET /v1/workspaces?deleted=true`) and can be restored until the retention job purges it (`retention.recycle_bin_days`).

Workspace-level features:

* **Export and import.** `GET /v1/workspaces/{ws}/export` produces a ZIP; `POST /v1/workspaces/import` creates a workspace from one (`olcf workspace export|import`). The archive holds `workspace.json`, `items/<name>.<type>/item.json` plus the item's content files, and `connections/<name>.connection.json`. Connections are exported as metadata only: no secret value and no secret id ever enters an archive. On import, connections come first, then items in dependency order (lakehouses, environments, variable sets, notebooks, Spark job definitions, SQL queries, pipelines, reports, schedules) with an id map; references to other items and connections are remapped, with a final pass for references to items created later. A `local` connection (a grant of a host directory) can be created or changed only by an organisation-level `connection.manage` holder, also through an archive.
* **Git sync.** A workspace can be connected to a Git repository (branch and folder). Items are written in the export layout; notebook content can be stored as `ipynb`, percent-format `py` or the Fabric Git source format. Status is a three-way comparison (base = tree at the last synced commit, local = workspace, remote = branch tip) per item: outgoing, incoming or conflict. `commit` refuses when anything is incoming or conflicting and pushes without force; it scans every outgoing file and the message for secrets first. `pull` needs a resolution (`ours` or `theirs`) for each conflict and applies nothing if any item is invalid. Credentials come from a `git` connection and reach git only through `GIT_ASKPASS` files. It needs a real `git` program: without one all Git endpoints answer `503 git_unavailable` ([macOS note](../getting-started/install-macos-no-clt.md#what-does-not-work-without-a-real-git)).
* **Stages and promotion.** A workspace can carry a stage (for example dev, test, prod) that selects the matching values of variable sets. Deployment pipelines (`/v1/deployment-pipelines`) promote items forward from one workspace to the next: items are matched by logical id and then by name, references are remapped, and `dry_run` shows the plan first.

## Items

Everything a user builds is an **item**: a typed, versioned document inside a workspace.

| Type | What it is |
|---|---|
| `notebook` | a Jupyter-compatible notebook plus settings (engine, runtime, default lakehouse, environment, timeout, resources, policy); a cell tagged `parameters` declares parameters |
| `lakehouse` | a storage root with `Files/` and `Tables/` areas, default format and catalog, optional Fabric ids |
| `sql_query` | saved SQL with an engine, default lakehouse and typed parameters |
| `report` | a dashboard: filters and tiles (single number, bar, line, area, donut, table, heatmap, scatter), each a SQL query |
| `pipeline` | a DAG of typed activities (`olcf.pipeline/v1`) |
| `schedule` | a cron schedule bound to a pipeline, notebook or Spark job definition |
| `environment` | a runtime choice plus hash-locked Python and jar dependencies, variables and Spark settings, published immutably |
| `spark_job_definition` | a Python, Scala or Java application: main file or class, arguments, reference files, settings |
| `variable_set` | named variables with per-stage values and secret references |
| `extension` | a generic item type reserved for extensions |

Connections are **not** items: they are separate objects (metadata plus an optional secret reference) so that a secret can never leak through an item definition.

Every item has an immutable id (`itm_...`), a unique name within its workspace and type, a folder, tags, a description, optional sensitivity metadata, an owner, timestamps, a **version** number and an **ETag**. Updates send `If-Match`; a stale ETag is answered with `412`. Every update stores a version, so `GET /v1/items/{id}/versions` and a revert (`.../versions/{version}/revert`) are available (for notebooks the diff compares cells and ignores outputs). Deleting an item is a soft delete into the workspace recycle bin. Definitions are plain JSON that diffs well in Git; the notebook definition is nbformat 4. Each item type validates its own definition through hooks registered by its module.

Item creation and updates always authorise against the workspace the object belongs to, never against a workspace id supplied by the client. References between objects are authorised in the *target* workspace: a job cannot name a secret, item or lakehouse of another workspace.

## Lakehouses and storage

A lakehouse item's definition names a **storage root** (`root_uri`, or a connection that supplies one) and the layout below it:

```
<root>/Files/...                      unmanaged files: raw, staging, models, anything
<root>/Tables/<table>                 Delta, Iceberg or Parquet tables
<root>/Tables/<schema>/<table>        when the lakehouse is schema-enabled
```

The canonical address of a location is `olcf://<workspace>/<lakehouse>/Files/<path>` or `.../Tables/<name>` (segments percent-encoded, so a workspace named `Sample Retail` is `olcf://Sample%20Retail/...`). The storage service resolves it to the provider's physical URI (`file://`, `s3://`, `abfss://`). A relative `Files/...` or `Tables/...` path needs a default lakehouse. This is what lets one notebook run unchanged against local files, MinIO or OneLake.

**Providers.** `local` (the default, under `<data_dir>/storage/lakehouses/<workspace>/<lakehouse>.Lakehouse`), `s3` (AWS S3, MinIO and any S3-compatible store), `adls` (ADLS Gen2) and `onelake` (the ADLS API against OneLake: `abfss://<fabric-workspace-id>@onelake.dfs.fabric.microsoft.com/<fabric-lakehouse-id>`). More can be added through the `olcf.storage_providers` entry-point group. Credentials come from a connection's secret and are resolved server-side when needed; see [components and data flow](components-and-data-flow.md#who-touches-the-data). The local provider is confined: a lakehouse root must lie inside the workspace's own directory, the root of a `local` connection the workspace references, `storage.local_allowed_roots`, or inside an existing lakehouse the caller can read and write; anything else is a 422.

**Formats.** Delta Lake and Apache Iceberg are first-class; Parquet, ORC, CSV and JSON are file formats. The browser shows schema, history, details and, for Delta, the protocol.

**Delta safety.** Before OLCF writes to a Delta table through a managed path, and for the protocol endpoint, it reads `_delta_log` and compares the table's reader and writer versions and features with the versioned **interop policy** (`olcf/storage/delta_policy.py`, `olcf-delta-interop/2026.1`). The default profile `fabric-2.0` allows the features of the Delta release in the Fabric Runtime 2.0 compatible family (Delta 4.x) and blocks upgrades beyond it; `fabric-1.3` models Delta 3.2. Unknown features are blocked (fail closed). The feature tables are OLCF's statement of open-source Delta support, not a Microsoft guarantee.

**Destructive maintenance.** `OPTIMIZE` and `VACUUM` are available per table. `VACUUM` needs the `lakehouse.destructive` permission, defaults to a dry run and refuses a retention below `storage.vacuum_min_retention_hours` (168 hours, a runtime setting that only a PlatformAdmin can lower).

## Catalog and lineage

The logical catalog (`olcf.catalog`) keeps one entry per discovered table (Delta, Iceberg or Parquet) of a lakehouse: columns, partitioning, an editable description and tags, status and the latest statistics (from the Delta log or Parquet footers, without scanning data). The catalog name (default `default`) and schema are name qualifiers; views and functions are not modelled. Storage stays the source of truth for physical facts. Search spans workspaces, items and tables. The lineage graph stores deduplicated edges from source to target datasets (`GET /v1/catalog/lineage`; `POST /v1/catalog/lineage` lets external tools record edges). Lineage is recorded where it can be established deterministically: SQL writes made through the Spark engine (table-level edges, and column-level edges only where sqlglot can prove them; otherwise the statement is marked `partial` or `unknown`), pipeline copy activities, the tiles of dashboards (dataset to report) and pipeline runs. `POST /v1/catalog/lineage/parse` explains the lineage of any SQL text. Notebook code is **not** traced: reads and writes made by arbitrary Python or PySpark appear in the catalog only as tables, not as edges. Lineage is never guessed for dynamic code. The catalog learns about tables that a notebook or job wrote directly into `Tables/` the next time a lakehouse is refreshed (`POST /v1/lakehouses/{id}/refresh`) or a table is described; the SQL service scans storage and sees them at once.
