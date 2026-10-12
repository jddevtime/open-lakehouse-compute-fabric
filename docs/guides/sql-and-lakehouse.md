# SQL, lakehouses and dashboards

This guide covers working with data: browsing lakehouses, running SQL, saving queries, building dashboards and using the catalog. The model behind it (workspaces, items, storage roots, providers) is in [workspaces, items and lakehouses](../concepts/workspaces-items-lakehouses.md); the way requests reach the data is in [components and data flow](../concepts/components-and-data-flow.md#a-sql-query).

## Lakehouses

In the UI: **Data, Lakehouse**. Create one with a name and, optionally, a storage location and a connection that supplies credentials. With no location, OLCF creates a managed folder under `<data dir>/storage/lakehouses/<workspace>/<lakehouse>.Lakehouse`.

```bash
olcf lakehouse create sales --root-uri s3://my-bucket/lakehouses/sales --connection minio -w "Sample Retail"
olcf lakehouse test sales -w "Sample Retail"          # reachable and writable?
olcf lakehouse list -w "Sample Retail"
```

| Storage | `--root-uri` | Credentials |
|---|---|---|
| local (default) | omitted, or an absolute path allowed by `storage.local_allowed_roots` or a `local` connection | none |
| S3, MinIO, any S3-compatible store | `s3://bucket/prefix` | connection type `s3` or `minio`: bucket, prefix, region, `endpoint_url` and path-style addressing for non-AWS stores, authentication `static` (access key id and secret in the connection's secret) or `default` (the environment's credential chain) |
| ADLS Gen2 | `abfss://container@account.dfs.core.windows.net/path` | connection type `adls`: account name, container, authentication `account_key`, `sas`, `service_principal`, `managed_identity` or `default` |
| OneLake | `abfss://<fabric-workspace-id>@onelake.dfs.fabric.microsoft.com/<fabric-lakehouse-id>` | connection type `onelake`: Fabric workspace and lakehouse, authentication `service_principal`, `user_token`, `managed_identity` or `default` |

Create the connection first (**Integrate, Connections & secrets**, or `olcf connection create`); the secret value is write-only and `olcf connection test` checks the whole chain. See [what a connection stores](../concepts/security-model.md#secrets).

**Files.** `Files/` holds anything. Browse, upload, download, move and delete in the UI or with `olcf lakehouse files ls|cat|put|mkdir|mv|rm`. Uploads are capped by `storage.max_upload_mb` (default 512).

```bash
olcf lakehouse files ls retail_lakehouse Files/raw -w "Sample Retail"
olcf lakehouse files put retail_lakehouse ./orders.csv Files/raw/orders.csv -w "Sample Retail"
```

**Tables.** `Tables/` holds Delta, Iceberg or Parquet tables. The table view shows schema, a data preview, history (Delta versions or Iceberg snapshots) and details, including the Delta protocol. A preview can read an older Delta version or timestamp.

```bash
olcf lakehouse tables list retail_lakehouse -w "Sample Retail"
olcf lakehouse tables show retail_lakehouse fact_sales -w "Sample Retail"
olcf lakehouse tables preview retail_lakehouse fact_sales --rows 5 --version 0 -w "Sample Retail"
olcf lakehouse tables history retail_lakehouse dim_store -w "Sample Retail"
olcf lakehouse tables create retail_lakehouse orders --from Files/raw/orders.csv --format csv -w "Sample Retail"
```

`tables create` registers a table from a file under `Files/` or from a SQL query (`--format sql`) with `--mode overwrite|append|error`.

**Maintenance.** `OPTIMIZE` compacts small files; `VACUUM` deletes files no longer referenced. `VACUUM` is destructive: it needs the separate `lakehouse.destructive` permission, is a dry run unless you pass `--apply`, and OLCF refuses a retention below `storage.vacuum_min_retention_hours` (168 hours; only a PlatformAdmin can lower the setting).

```bash
olcf lakehouse tables optimize retail_lakehouse fact_sales --z-order customer_id -w "Sample Retail"
olcf lakehouse tables vacuum retail_lakehouse fact_sales --retention-hours 336 -w "Sample Retail"          # dry run
olcf lakehouse tables vacuum retail_lakehouse fact_sales --retention-hours 336 --apply -w "Sample Retail"
```

**Delta compatibility check.** Before a managed write OLCF compares the table's Delta reader and writer protocol and features with the interop policy selected by `storage.delta_interop_profile` (`fabric-2.0` by default, `fabric-1.3` available). Unknown features are blocked. This keeps a table written here readable by a Fabric Runtime 2.0 Spark and by Fabric's SQL endpoint to the extent that OLCF's table of open-source Delta support says so; it is not a Microsoft guarantee.

## Running SQL

In the UI: **Data, SQL editor**. Pick a workspace and a default lakehouse, write a statement, run. The editor shows the schema tree, runs the statement under your permissions, shows results with a row count and time, offers CSV, JSON, Parquet or Excel export of the full result, and keeps a history.

Two engines exist:

| | DuckDB (default) | Spark SQL |
|---|---|---|
| Where it runs | inside the control plane, embedded | on a worker, in a pooled session |
| Statements | exactly one statement, read-only (queries, `EXPLAIN`, `DESCRIBE`, `SUMMARIZE`, `SHOW`) | reads and writes: `INSERT`, `CREATE TABLE AS`, `MERGE`, `UPDATE`, `DELETE`, DDL |
| Permissions | `sql.execute` and `lakehouse.read` | adds `sql.write` and `lakehouse.write` for writes |
| Needs a runtime | no | yes (install one: `olcf runtime install`) |
| Cancel | yes | no |
| Limits | memory `sql.duckdb_memory_mb` (1024), threads `sql.duckdb_threads` (2), `sql.max_concurrent_queries` (4, others wait then get 429) | the worker's resources |

Common to both: one statement per request, a timeout (`sql.default_timeout_seconds` 60; callers may ask for at most `sql.max_timeout_seconds` 600), a row cap (`sql.default_max_rows` 10,000; at most `sql.max_rows_cap` 100,000, with `truncated: true` when cut), and an export route for more (`sql.export_max_rows` 5,000,000, streamed).

**Referring to tables.** With a default lakehouse, use bare names (`fact_sales`). Without one, qualify: `retail_lakehouse.fact_sales`. DuckDB can also read files below `Files/` with `read_csv`, `read_parquet` and `read_json` (`select count(*) from read_csv('Files/raw/stores/stores.csv')`), capped at `sql.max_file_scan_mb` (256) per call. A table that does not exist answers 404 with a hint to look at the schema (`GET /v1/sql/schema`).

**Parameters.** Use named parameters, `:name` or `$name`; positional `?` is not supported. Values are bound as typed literals after parsing, never concatenated into the text.

```bash
olcf sql run -w "Sample Retail" --lakehouse retail_lakehouse -p cat=Electronics \
  "select count(*) as n from dim_product where category = :cat"
olcf sql explain -w "Sample Retail" "select * from retail_lakehouse.dim_store"
olcf -o csv sql run -f report.sql -w "Sample Retail" --lakehouse retail_lakehouse > out.csv
olcf sql history
```

**Read-only is enforced, not hoped for.** Asking DuckDB to `DELETE` answers `422 unsupported` with the remedy "submit the statement with engine='spark'". The DuckDB connection has external access disabled and its configuration locked, and only Arrow tables registered by the service are visible: the engine never sees a path or credential.

### Statements that are always refused

Both engines run a **host guard** (`olcf.sql.guard`) on the text before anything executes. It applies to **every role**: holding `sql.write`, being a workspace administrator or a platform administrator does not lift it, because the constructs below read or write the worker's file system, run programs or call into the JVM, which no lakehouse permission covers. A refused statement answers `422 unsupported` (or `400` for text that cannot be parsed at all) with the reason and the supported alternative, and is recorded in the history as failed.

| Engine | Refused for everybody | Instead |
|---|---|---|
| Spark SQL | JVM reflection and indirection: `reflect`, `java_method`, `try_reflect`, `call_function`, `identifier()` and similar functions; script transformation `SELECT TRANSFORM(...) USING 'cmd'` (and `RECORDREADER`/`RECORDWRITER`); `INSERT OVERWRITE [LOCAL] DIRECTORY`; `LOCATION '...'` clauses; file queries such as ``parquet.`/path` `` and `USING <datasource>` with a path (including `jdbc`, `kafka`, `delta`, `csv`); `CREATE FUNCTION`; `SET`, `USE`, `LOAD DATA`, `ADD JAR`/`ADD FILE`; `${...}` variable substitution in the text (Spark substitutes before it parses, so it is scanned in the raw text); functions that reach the network or services (`http_request`, `read_*`, `ai_*`, `remote_*`, `from_protobuf`...); **every function that is not an approved Spark built-in** (an unknown or registered UDF, a function of a newer Spark that is not reviewed yet) and **every qualified function call** (`mydb.my_udf(x)`, `default.f(x)`); **every table function except** `range`, `explode`, `explode_outer`, `posexplode`, `posexplode_outer`, `inline`, `inline_outer`, `stack`, `json_tuple`, `variant_explode`, `variant_explode_outer`, `sequence` | refer to lakehouse tables by name; load files with a notebook or a Spark job, which are authorised as code |
| DuckDB | everything that is not a read; `query`, `query_table`, `glob`, `read_text`, `read_blob`, `sniff_csv`, extension scans (`iceberg_scan`, `delta_scan`, `sqlite_scan`...), `getenv`, anything named `duckdb_*` or `pragma_*`, `current_setting`; **every table function except** `range`, `generate_series`, `unnest`, `json_each`, `json_tree`, `repeat_row`, `generate_subscripts`; quoted file names as table names | `read_csv`, `read_parquet` and `read_json` (and their `_auto` / `ndjson` forms) with a path below the lakehouse `Files/` folder: OLCF serves these itself after checking the permission and the path |

How it is applied: the statement is parsed once to classify it, and **every role needs exactly one fully parsed, supported statement**. Text the parser cannot read (answered `400`), several statements, and `RESET conf` or `SELECT 1; ADD JAR x` are refused for administrators and writers exactly as for viewers; there is no pass-through for text that could not be classified, so do not look for a spelling that gets around the parser. Session *commands* that the parser returns as a generic command - `ADD JAR`/`ADD FILE`/`ADD ARCHIVE`, `LIST FILE`, `RESET`, `CACHE TABLE`/`UNCACHE`/`CLEAR CACHE`, `REFRESH`, `CALL`, `OPTIMIZE`/`VACUUM`/`MSCK` - are refused by the guard itself for every caller, `strict` or not, including a direct `ComputeService.run_spark_sql`; writers keep `INSERT`, `CREATE TABLE [AS]`, `CREATE VIEW`, `MERGE`, `UPDATE`, `DELETE`, `DROP`, `ALTER` and `TRUNCATE`, and everybody keeps `SHOW`/`DESCRIBE`/`EXPLAIN`. Then the text is checked for the constructs above; Spark runs the *rendered* statement, so the text is checked again after parameters and the default lakehouse were substituted. Scalar functions follow an **allowlist** (`olcf.sql.spark_functions.SPARK_ALLOWED_FUNCTIONS`: the ordinary scalar, aggregate, window, generator, string, math, date/time, collection, map, JSON/CSV/XML, hash, conditional, bitwise, type-conversion and variant functions), so a function added by a newer Spark stays refused until it was reviewed and added; the known dangerous names keep their own refusal message, and table functions follow their own allowlist. **Custom and UDF functions now fail closed**: a statement that calls one is refused with `422 unsupported`; run a notebook or a Spark job when you need it. If a genuine built-in is refused, report it so it can be reviewed and added with a test. The pooled Spark sessions that execute this SQL run with a read-only API credential of their own, so a statement cannot reach other APIs through the session. This was tested with the planner, the session door and a running server using fakes for Spark; the same statements have not been replayed against a real pooled Spark kernel.

**Saved queries.** A `sql_query` item stores SQL, an engine, a default lakehouse and typed parameters. Run one with `POST /v1/sql/queries/{id}/run` or from the editor. The sample workspace has eight, for example "Revenue by month" and "Top products in a category".

**History.** Every statement is recorded with its state, row count and time, with literals and secrets redacted. History is kept for `sql.history_retention_days` (30) and at most `sql.history_max_rows` (100,000) rows. You see your own history; users with the right to see more can filter by workspace.

## Dashboards

A `report` item is a dashboard: filters and tiles. Each tile is a SQL query with a visual type: `kpi`, `bar`, `line`, `area`, `donut`, `table`, `heatmap` or `scatter`. In the UI: **Data, Dashboards**.

* A `select` or `text` filter binds `:<name>`; a `date_range` filter binds `:<name>_from` and `:<name>_to`; a multi-select binds a list (`IN :name`).
* Tiles run in parallel (`sql.report_parallelism`, 4) with the viewer's own permissions. One failing tile shows its error and the others still render.
* Results are cached for `sql.report_cache_ttl_seconds` (60) per query and parameters. The cache is consulted only after the viewer is authorised for the tile's lakehouse, so a cached result never reaches a user who could not have run the query.
* Dashboards produce lineage from the datasets they read to the report.

Run one from the API with `POST /v1/reports/{id}/run`.

## Catalog, lineage and search

The catalog (`olcf catalog tables`) lists every discovered table with format, version, status, tags and statistics read from the Delta log or Parquet footers; descriptions and tags are editable. Search (`olcf search <text>`, or the search box in the UI) covers workspaces, items and tables. Lineage shows upstream and downstream datasets:

```bash
olcf catalog lineage retail_lakehouse.fact_sales --direction both --depth 3
```

Edges are recorded where they can be established deterministically (Spark SQL writes, pipeline copy activities, dashboards, pipeline runs). Arbitrary notebook code is not traced, so tables written by PySpark appear in the catalog but have no lineage edges unless an external tool posts them (`POST /v1/catalog/lineage`). A table that a notebook wrote directly into `Tables/` is added to the catalog on the next refresh (`POST /v1/lakehouses/{id}/refresh`); the SQL engine sees it immediately because it reads storage.

## From notebooks and jobs

Spark code reads and writes storage directly on the worker, using the credentials of the lakehouse's connection that the controller placed in the job at launch ([who touches the data](../concepts/components-and-data-flow.md#who-touches-the-data)). `spark.read.format("delta").load("Tables/fact_sales")` works with a default lakehouse; `olcf://Sample%20Retail/retail_lakehouse/Tables/fact_sales` works from any job that may read it. See [notebooks](notebooks.md).
