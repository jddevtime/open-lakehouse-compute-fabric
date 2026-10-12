# Lineage support matrix

Author: Justin Dodrill. This page is **written by hand** (it is not produced by a `gen_*.py` script); it is the exact list of what each lineage analyzer and run-time emitter of OLCF Intelligence understands, partly understands and does not understand. It was assembled from the module documentation of `src/olcf/intelligence/lineage/*.py`, `runtime.py`, `core/dataset_events.py` and `runtime_lib/lineage_probe.py`, and spot-checked by running the analyzers on small inputs. The tests named under each heading pin the behaviour; if a row here disagrees with the code, the code is right and this page is a defect.

How to read it:

* **Supported**: the construct is understood; edges are emitted with the confidence in the row.
* **Partial**: understood in some forms; the row says which and what is reported for the rest.
* **Unsupported**: not understood; the row says whether it is reported as a gap (it is, unless the row says it is skipped silently because it carries no lineage).
* **Confidence** is the confidence of the *edges the analyzer emits*; see [confidence levels](../concepts/intelligence.md#confidence-levels). Static analyzers emit `derived` and `inferred`, never `verified`; `verified` comes only from [run-time events](#run-time-events-verified).
* **Gap** kinds are listed in [the concepts page](../concepts/intelligence.md#gaps-why-the-platform-tells-you-what-it-cannot-see). A construct that is "reported" appears in the gap list with the subject, the reason and what to do.

What every analyzer guarantees (`lineage/base.py`, `lineage/engine.py`): it is **pure** (no database, no network, no file reads, no code executed or imported), **deterministic** (the same definition gives byte-identical results), **never raises** (an unknown definition kind gives one `not_inspected` gap; an analyzer that fails unexpectedly gives a `parse_error` gap naming it), **bounded** (hard caps; hitting one records a gap saying what was skipped) and **free of secrets** (evidence snippets, gap texts, names and URI aliases are redacted). Caps: 200,000 edges, 5,000 gaps, 2,000,000 characters of SQL, 5,000 statements per script, 8,000,000 characters of Python, 6,000 notebook cells, 64,000,000 bytes of JSON, 5,000 parts per model or report, 600,000 abstract evaluation steps for Python, 500 report tiles, 5,000 pipeline activities (nesting 12). Evidence keeps at most 20 records per edge.

## Summary

| Analyzer | Input | Table-level | Column-level | Typical confidence | Biggest limits |
|---|---|---|---|---|---|
| [SQL](#sql) | a statement or script (Spark SQL, DuckDB, T-SQL via sqlglot) | yes | yes, where schemas are known | derived; inferred when the target schema is unknown | `SELECT *` over unknown schemas, PIVOT/UNPIVOT, dynamic SQL, stored procedures |
| [PySpark / Python](#pyspark-and-python) | Python source | yes | yes, through the DataFrame API | derived; inferred for parameter-dependent names | non-constant names, classes, UDF internals, most of pandas |
| [Notebook](#notebooks) | nbformat 4 (OLCF or Fabric) | yes | yes | as above | Scala, R, C# cells are gaps |
| [OLCF pipeline](#olcf-pipelines) | `olcf.pipeline/1` | yes | by-name copy mapping only | derived; inferred for parameter defaults | expressions, shell scripts |
| [Fabric pipeline](#fabric-pipelines) | `pipeline-content.json` | yes | only with an explicit translator | derived; inferred for parameter defaults | many activity types not interpreted |
| [Spark job definition](#spark-job-definitions) | `spark_job_definition` item | only for inline Python | only for inline Python | derived | a job that names a file is a gap |
| [OLCF report](#olcf-reports-dashboards) | report with tiles | yes | tile to source column | derived | `SELECT *`, literals, ambiguous names |
| [Semantic model](#semantic-models-tmdl-modelbim-dax-power-query-m) | TMDL or `model.bim` | yes | yes | derived; inferred for name matches | join/expand/pivot in M, calculation items, RLS |
| [Power BI report](#power-bi-reports-pbir-and-legacy) | PBIR or legacy `report.json` | yes (model binding) | visual to column/measure | derived | report-level measures, field parameters |
| [Power BI scanner result](#power-bi-scanner-results) | admin `scanResult` JSON | yes | when `datasetSchema` and expressions were returned | derived (relationships), inferred (assumed source column names) | depends on what the scan returned |
| [Classic catalog mirror](#classic-catalog-mirror-and-the-local-source) | `lineage_edges` | yes | where the catalog proved the mapping | derived | what the catalog recorded |
| [Run-time events](#run-time-events-verified) | `dataset.*` events | yes | only via a statement that ran | **verified** | what the probe covers |

## SQL

`olcf.intelligence.lineage.sql` (`analyze_sql`). The same engine reads every SQL string in the platform: saved queries, `%%sql` cells, `spark.sql(...)`, `selectExpr`, DuckDB `con.sql`, pipeline `sql` activities, report tiles and Power Query native queries. Tests: `tests/intelligence/lineage/test_sql.py` (81), `test_robustness.py` (196).

Emitted: `READS_FROM` process to every base table, view or file read (including inside CTEs, sub-queries, joins, lateral views and `IN (SELECT ...)`); `WRITES_TO` process to the target (`attrs.mode` says how); `TRANSFORMS` source column to target column, with the expression text. The process is the caller's asset (notebook, activity, saved query), or a `sql_query` asset keyed by the hash of the normalised statement.

| Construct | Support | Notes |
|---|---|---|
| `SELECT` with joins (`ON`, `USING`, `NATURAL` when schemas are known) | Supported | `USING` takes both sides. |
| CTEs, chained and recursive; derived tables; scalar, `EXISTS` and `IN` sub-queries, correlated too | Supported | The chain `outer <- inner` is recorded for column mappings. |
| `UNION [ALL]`, `INTERSECT`, `EXCEPT` | Supported | Origins of every branch are merged. |
| `LATERAL VIEW explode`, `VALUES`, lambda functions, named windows, `QUALIFY` | Supported | |
| Window functions | Supported | `PARTITION BY` and `ORDER BY` columns are included in the target's sources. |
| `CASE`, aggregates, casts, UDF calls | Supported | Every referenced column is a source of the result. The UDF's own code is not read. |
| `SELECT * EXCEPT / EXCLUDE / REPLACE` | Supported | |
| `SELECT *` | Partial | Expanded only when the columns are known (a prior `CREATE TABLE`/CTAS in the same script, a CTE or temp view with a known select list, or a schema supplied in the context). Otherwise table-level edges stay and a gap `unsupported_construct` "SELECT * without known schema" is recorded. |
| Unqualified column that could belong to several tables of unknown schema | Not attributed | Gap `unresolved_reference`; nothing is guessed. |
| `INSERT [OVERWRITE] [TABLE] t [(cols)] SELECT / VALUES` | Supported | With a known target schema, mapped by position (derived). With an unknown target schema and no column list, mapped by select-list names with confidence **inferred**. If the select list is not fully known, nothing is mapped. |
| `CREATE [OR REPLACE] [TEMP] TABLE / VIEW ... AS`, `CREATE TABLE ... USING DELTA AS`, `CACHE TABLE ... AS`, T-SQL `SELECT ... INTO`, `#temp` tables | Supported | |
| `CREATE TABLE ... CLONE` | Supported | Reads the source. |
| `CREATE TABLE ... LIKE` | Supported | Writes only: no data flows. |
| `MERGE INTO` (`UPDATE SET col = ...`, `SET *`, `INSERT (cols) VALUES`, `INSERT *`, `DELETE`) | Supported | `SET *` / `INSERT *` with a source of unknown schema: gap, no column mapping. |
| `UPDATE ... SET ... [FROM ...]`, `DELETE [... USING / FROM ...]` | Supported | |
| `COPY INTO`, file readers (`read_parquet`, `read_csv`, `delta_scan`, `'path/file.parquet'`, Spark `` delta.`path` ``, `` parquet.`path` ``) | Supported | The file or path is the dataset. |
| Time travel | Supported | The same table. |
| Spark temp views, DuckDB replacement scans | Supported | Through the session shared with the Python analyzer. |
| `USE <db>`, `[bracket]` quoting | Supported | |
| `PIVOT` / `UNPIVOT` | Partial | Tables read are reported; column mapping through the operator is not available (gap `unsupported_construct`). Checked by running it. |
| `TRANSFORM(...) USING` scripts | Unsupported | Gap. |
| Table functions the analyzer does not know | Unsupported | Gap: output columns unknown. |
| Dynamic SQL (`EXEC`, `IDENTIFIER(...)` with a non-literal) | Unsupported | Gap, never guessed. |
| Stored procedure calls | Unsupported | `EXEC` is reported (`parse_error` or `unsupported_construct`, depending on the dialect). |
| `INSERT OVERWRITE DIRECTORY ... USING` | Unsupported | |
| More than 10 consecutive `JOIN`s without `ON`/`USING` | Unsupported | Not parsed (sqlglot's parser is exponential on them); `unsupported_construct`. |
| Text sqlglot cannot parse (including a misspelt `SELEC x`) | Unsupported | Gap `parse_error`. |
| `DROP`, `ALTER`, `OPTIMIZE`, `VACUUM`, `SET`, `SHOW`, `DESCRIBE` | Skipped | They carry no lineage; skipped silently. |

## PySpark and Python

`lineage/pyspark.py` and `pyvalues.py` (`analyze_python`). The code is parsed with `ast` and *abstractly interpreted*: constants are propagated through assignments, f-strings, `str.format` and `%`; functions are inlined when called (depth 8); loops over constant collections are unrolled (200 iterations); both branches of an `if` are followed; the DataFrame API is modelled column by column. Nothing is executed or imported. Tests: `test_pyspark.py` (47), `test_robustness.py`.

| Construct | Support | Notes |
|---|---|---|
| Reads: `spark.table`, `spark.read.table`, `spark.read.format(..).load(path)`, `.csv/.parquet/.json/.orc/.text/.avro`, `spark.readStream`, `DeltaTable.forName/forPath` (`.toDF()`), `deltalake.DeltaTable(path)` | Supported | A literal or constant-propagated name gives `derived`. |
| pandas `read_parquet/csv/json/excel`; DuckDB `con.sql/execute/read_parquet/read_csv`; `notebookutils.fs.cp/mv/head`; JDBC sources | Supported | DuckDB SQL goes through the SQL analyzer with DataFrame variables resolved as replacement scans and `con.register` views; a JDBC source is an `external` asset. |
| Writes: `df.write.saveAsTable/insertInto/save/parquet/csv/json/orc`, `format(..).mode(..).partitionBy(..)` chains, `df.writeTo(..).create/append/createOrReplace`, `writeStream.toTable/start`, Delta `merge/update/delete` builders, `write_deltalake`, pandas `to_parquet/to_csv/to_json`, `notebookutils.fs.put/cp/mv` | Supported | |
| SQL inside Python: `spark.sql`, `selectExpr`, `F.expr`, `DataFrame.filter("...")` strings | Supported | DataFrame variables are available as temp views (`createOrReplaceTempView`). |
| Notebook-to-notebook control flow: `notebookutils.notebook.run/runMultiple`, `mssparkutils.notebook.run` | Supported | `DEPENDS_ON` edge. |
| Column tracking through `select`, `selectExpr`, `withColumn(s)`, `withColumnRenamed`, `toDF`, `drop`, `alias`, `filter`, `join` (both sides; `USING` merged), `groupBy().agg(...)` (including dict `agg`, `count`, `sum` ...), `union/unionByName`, `distinct`, `orderBy`, `limit`, window functions, `F.when/otherwise`, `F.expr`, `F.col`, `df["c"]`, `df.c`, casts, arithmetic, UDF calls | Supported | A UDF call makes its argument columns the sources; the UDF body is not read. |
| Basic pandas: `df["c"] = expr`, `assign`, `rename`, `drop`, `merge`, `concat` | Partial | Most of pandas and NumPy is not modelled. |
| Table or path name that is not a compile-time constant | Gap | `dynamic_reference`, no edge. |
| Name that depends on a **notebook parameter** | Partial | Analysed with the parameter's default; the edge is **inferred** (a pipeline may pass another value). |
| Path whose root is computed at run time but whose last segment is constant (`Path(root) / "orders"`) read as a Delta table | Partial | An **inferred** table edge plus a gap. |
| DataFrame with unknown columns (`SELECT *` over an unknown schema, `mapInPandas`, `rdd`, pivot) | Partial | Table-level lineage plus a gap; no column edge is invented. |
| Functions defined but never called | Partial | Analysed with unknown arguments; their edges are **inferred**. |
| Classes and methods, `eval`/`exec`, decorators other than `udf`, async code, dynamic attribute access (`getattr(df, name)`) | Unsupported | `eval`/`exec` are reported. |
| Structured-streaming sources other than files and Delta; Spark Connect specifics | Unsupported | |

## Notebooks

`lineage/notebook.py` (`analyze_notebook`). Cells run top to bottom through one shared session, so a variable, function or temp view defined in cell 2 is visible in cell 7. Tests: `test_notebook.py` (60), `test_sample_workspace.py` (19).

| Construct | Support | Notes |
|---|---|---|
| Plain code, `%%pyspark`, `%%python`, `%%spark`, `%%time`, `%%capture` | Supported | To the Python analyzer; line magics (`%pip`, `%matplotlib`, `!cmd`) are blanked so line numbers stay exact. |
| `%%sql`, `%%sparksql`, cells whose Fabric language metadata is `sparksql`/`sql` | Supported | To the SQL analyzer. `%%sql -o VAR` binds the result to `VAR`. |
| `%run <notebook>`, `notebookutils.notebook.run/runMultiple`, `mssparkutils.notebook.run` | Supported | `DEPENDS_ON`, by name unless the context supplies the item. |
| `%%configure` with `defaultLakehouse`; Fabric `metadata.dependencies.lakehouse` | Supported | Sets the default lakehouse for unqualified names and relative paths. Invalid JSON: `parse_error` gap. |
| A cell tagged `parameters` | Supported | Parameter defaults are used and derived edges are **inferred**; `context.parameters` overrides the defaults. |
| `%%scala`, `%%sparkr`, `%%csharp` (and cells whose metadata says so) | Unsupported | `unsupported_construct` gap per cell. Checked by running it. |
| `%%bash`, `%%sh`, `!` shell cells, `%%html` | Skipped | Cannot touch lakehouse tables through Spark; skipped silently. |
| A cell that is not valid Python | Partial | `parse_error` gap; the remaining cells are still analysed. |
| Markdown, raw cells, outputs | Ignored | |
| More than 6,000 cells, or 8,000,000 characters in a cell | Partial | Cut with a gap. |

Evidence location for a notebook: `{"item": <item id>, "cell": <0-based index>, "line": <1-based line in the cell>}`.

## OLCF pipelines

`lineage/pipeline_olcf.py` (`olcf.pipeline/1`). Tests: `test_pipelines.py` (48).

| Construct | Support | Notes |
|---|---|---|
| Pipeline and one `activity` asset per activity, nested containers included (`if`, `switch`, `for_each`, `until`) | Supported | `CONTAINS` edges. |
| `dependsOn` | Supported | `DEPENDS_ON` between activities, conditions in `attrs.conditions`; a dependency on an unknown activity is an `unresolved_reference` gap. |
| `notebook`, `spark_job`, `child_pipeline` activities | Supported | `EXECUTES` to the item named by a literal name or `itm_` id. An expression is a `dynamic_reference` gap unless it is a parameter reference with a default (then **inferred**). |
| `copy` | Supported | `READS_FROM` source, `WRITES_TO` sink (lakehouse table or `Files/<path>`). For table to table copies with a known source schema, by-name column mapping as `TRANSFORMS`. |
| `sql` | Supported | Analysed with the SQL engine (DuckDB or Spark per `engine`) with the activity as the process and the activity's lakehouse as default. `@{pipeline().parameters.x}` uses the default (**inferred**); other expressions are dynamic references. |
| `script` with `language: python` | Supported | PySpark analyzer. Shell scripts: `unsupported_construct`. |
| `http`, `webhook_wait` with a literal URL | Supported | `READS_FROM` an `external` asset named by the URL without query string or credentials. |
| `wait`, `set_variable`, `fail` | Skipped | Produce their asset only. |

Evidence location: `{"path": "activities[2].typeProperties.sink", "activity": name}`.

## Fabric pipelines

`lineage/pipeline_fabric.py` (`pipeline-content.json`). Tests: `test_pipelines.py`. Written to the documented shapes and covered by tests against the local Fabric test server.

| Construct | Support | Notes |
|---|---|---|
| Activities, nesting (`IfCondition`, `Switch`, `ForEach`, `Until`), `dependsOn` with `dependencyConditions` | Supported | `CONTAINS` and `DEPENDS_ON`. |
| `TridentNotebook`, `SparkJobDefinition`, `ExecutePipeline`, `RefreshDataflow`/`Dataflow`, `PBISemanticModelRefresh` | Supported | `EXECUTES`, identified by Fabric GUIDs (`fabric` alias `<tenant>/<workspace>/<item>`; `pbi` alias for models). |
| `DatabricksNotebook`, `SqlServerStoredProcedure` | Partial | `EXECUTES` an `external` asset and a `not_inspected` gap: the code is outside the pipeline. |
| `Copy` | Supported | `READS_FROM`/`WRITES_TO`; Lakehouse and Warehouse tables carry the Fabric table alias, lakehouse files are `Files/<folder>/<file>`, other stores are `external` assets. An explicit `translator` column mapping gives `TRANSFORMS`; **without a translator no column edge is created** (Fabric maps by name at run time, which the definition does not show). |
| `Lookup`, `Script` (T-SQL), `sqlReaderQuery` | Supported | `READS_FROM`; T-SQL goes to the SQL analyzer. |
| `Web`, `WebHook` with a literal URL | Supported | `READS_FROM` an `external` asset. |
| `Wait`, `SetVariable`, `Fail` | Skipped | |
| `Delete`, `GetMetadata`, `AzureFunction`, `Teams`, `Office365Outlook` | Unsupported | Not interpreted. |
| Parameters with defaults | Partial | Substituted; edges **inferred**. Other expressions: `dynamic_reference`. |

The table name `dbo.<t>` is recorded as `<t>` because `dbo` is the default schema of non-schema-enabled lakehouses and warehouses.

## Spark job definitions

`lineage/spark_job.py`. Tests: `test_spark_job.py` (12). A definition says *what runs*, not what the code does.

| Construct | Support | Notes |
|---|---|---|
| The job asset with language, main class and path-looking arguments | Supported | `attrs.path_arguments`. |
| `EXECUTES` main file; `DEPENDS_ON` each reference file | Supported | |
| Inline Python (`code`) | Supported | Full PySpark analysis with `settings.default_lakehouse`. |
| Code that is only a file reference | Gap | `not_inspected`: the analyzers never read storage. |
| Scala or Java | Unsupported | `unsupported_construct`. |
| Arguments that look like paths | Gap | `unresolved_reference`: read or write is unknowable, so no edge is guessed. |

Facts about what such a job read and wrote come from [run-time events](#run-time-events-verified).

## OLCF reports (dashboards)

`lineage/report_olcf.py`. Tests: `test_report_olcf.py` (25).

| Construct | Support | Notes |
|---|---|---|
| Tile SQL | Supported | Analysed by the SQL analyzer (`duckdb` or `spark` per the tile's engine). One `visual` asset per tile. |
| `READS_FROM` tile to each dataset, and report to the same datasets | Supported | |
| `DISPLAYS` tile to source column for each output column that traces to a table column | Supported | `attrs.as` lists the output names. |
| `options_sql` of a `select` filter | Supported | `READS_FROM` report to the table. |
| `:name` bind parameters | Ignored | Not data sources. |
| Output columns that cannot be traced (a literal, `SELECT *` over unknown columns, an ambiguous name) | Gap | No `DISPLAYS` edge; the SQL analyzer's gap says why. |
| A tile without SQL, or with SQL that does not parse | Gap | `not_inspected` / `parse_error`; other tiles continue. At most 500 tiles. |

## Semantic models: TMDL, model.bim, DAX, Power Query M

`lineage/tmdl.py`, `semantic_model.py`, `dax.py`, `mcode.py`. Tests: `test_semantic_model.py` (50), `test_dax.py` (36), `test_mcode.py` (106).

| Construct | Support | Notes |
|---|---|---|
| Tables, columns (including calculated), measures, partitions of kind `m`, `entity`, `calculated` | Supported | Assets: `semantic_model`, `model_table`, `column`, `measure`, with `pbi` aliases when the Fabric workspace and item GUIDs are in the context. |
| Direct Lake binding (`entityName`, `schemaName`, `expressionSource`) | Supported | `READS_FROM` model table to the dataset, evidence kind `definition`. A SQL endpoint id not mapped to a lakehouse is **inferred**. |
| Power Query M sources: `Sql.Database(s)` (with navigation, `Query=`, `Value.NativeQuery` analysed as T-SQL), `Lakehouse.Contents`, `AzureStorage.*`, `File.Contents`/`Folder.*` wrapped by `Csv/Excel/Json/Parquet.Document`, `Web.*`, `OData.Feed`, `SharePoint.*`, `AnalysisServices.Database`, `PowerPlatform.Dataflows`, `PowerBI.Dataflows`, and generic database connectors | Supported | Databases and web sources are `external` assets. Parameters and shared expressions are followed. |
| M column tracking through `RenameColumns`, `RemoveColumns`, `SelectColumns`, `ReorderColumns`, `AddColumn`, `TransformColumns`, `TransformColumnTypes`, `ReplaceValue`, `DuplicateColumn`, `AddIndexColumn`, `Group`, `Combine`, and the row-only functions | Supported | |
| M `Table.Join`, `NestedJoin`, `Expand*`, `Pivot`, `Unpivot`, `Transpose`, `Split*`, unknown table functions | Partial | The tables read are still reported; output columns become unknown and `MResult.reason` says why. |
| M source name that is not a literal (or a parameter with a default) | Gap | `dynamic` source, never guessed. Query folding is not modelled. |
| DAX references: `Table[Column]`, `'Table Name'[Column]`, `[Measure]`, table references (`COUNTROWS(Sales)`) | Supported | `TRANSFORMS` column to measure and to calculated column, measure to calculated column; `USES_MEASURE`; `DEPENDS_ON` to tables. |
| A column used only inside `CALCULATE` filters | Supported | Counted as a reference (changing it changes the result). |
| What a DAX function does with its arguments; `EVALUATE` queries; calculation items (`SELECTEDMEASURE`); row-level security filters; field parameters; relationships (kept as attributes, not data flow); perspectives; the columns of calculated tables; incremental refresh policies | Unsupported | |

Confidence: **derived** for everything read from the definition; **inferred** where a name had to be matched (a column without `sourceColumn` assumed to keep its source column's name; a bare `[Name]` that is also a column name of another table; a bare column in a measure; DAX that could not be tokenised completely).

## Power BI reports: PBIR and legacy

`lineage/report_pbi.py`. Tests: `test_report_pbi.py` (29).

| Construct | Support | Notes |
|---|---|---|
| PBIR (`definition.pbir`, `definition/report.json`, `pages/pages.json`, `pages/<page>/page.json`, `visuals/<visual>/visual.json`) and legacy `report.json` (`sections`, `visualContainers`, with `config`/`filters` as JSON strings) | Supported | `report`, `page`, `visual` assets. |
| Model binding (`datasetReference` by path or by connection) | Supported | `DEPENDS_ON` report to the semantic model, **derived**. |
| Fields a visual shows | Supported | `DISPLAYS` visual to column or measure. Fields that only filter, sort or format: `DEPENDS_ON` with `attrs.role`. Page and report filters: `DEPENDS_ON` page/report to field. |
| Column and measure references | Supported | Use the same `pbi` alias the model analyzer gives them; when the model's GUID is unknown they carry the model name and table in their scope and an informational gap says so. |
| Report-level measures | Gap | Reported, not linked. |
| Hierarchy levels | Gap | |
| Field parameters, bookmarks, drill-through targets, themes, custom visuals' data roles beyond the standard query | Unsupported | |
| Missing or unreadable `definition.pbir` | Partial | Visuals are listed, no field edges, a gap says why. |

## Power BI scanner results

`lineage/scanner.py` and `discovery/scanner_import.py`. Tests: `test_scanner.py` (25), `tests/intelligence/discovery/test_scanner_import.py` (233 lines). Follows the documented shape of the scanner result.

| Construct | Support | Notes |
|---|---|---|
| Per workspace: `semantic_model`, `report`, `dashboard`, `dataflow` assets with `fabric` and `pbi` aliases | Supported | Merge with the same objects found through the Fabric API or a definition. |
| Report to semantic model (`datasetId`); dashboard to report/model (tiles); model to upstream model (`upstreamDatasets`) and dataflow (`upstreamDataflows`); dataflow to upstream dataflow | Supported | `DEPENDS_ON`, evidence kind `metadata`, **derived**. |
| Model or dataflow to data source (`datasourceUsages` joined with `datasourceInstances`) | Supported | `READS_FROM` an `external` asset `<type>:<server or url>/<database>`; dropped for a dataset whose tables are known. |
| Tables, columns, measures when the scan carried `datasetSchema` and `datasetExpressions` | Supported | Turned into a model and analysed like a semantic model. The scanner does not return `sourceColumn`, so a column's source name is **assumed** equal to its model name: those column edges are **inferred**. |
| Anything the scan did not contain | Gap | Per dataset and field, `not_inspected`, naming the scanner parameter to switch on: `datasetSchema`, `datasetExpressions`, `lineage`, `datasourceDetails`. |
| Users, sensitivity labels, endorsement, refresh schedules, Lakehouse/Warehouse/notebook entries, paginated-report data sources beyond `datasourceUsages` | Unsupported | The Fabric API connector covers lakehouses and notebooks. |

A saved scan result is validated (a JSON object whose `workspaces` array holds objects with an `id`; size and workspace count limits) and **refused** if it appears to contain a credential.

## Classic catalog mirror and the local source

`discovery/catalog_mirror.py`, `discovery/local.py`. Tests: `tests/intelligence/discovery/test_catalog_mirror.py`, `test_local_connector.py`.

| Construct | Support | Notes |
|---|---|---|
| `lineage_edges` rows of the classic catalog | Supported | A process asset (the OLCF item named by `via_ref`, or a `script` asset), `READS_FROM` source, `WRITES_TO` target, column `TRANSFORMS` where the catalog proved the mapping. All **derived**, evidence kind `metadata`, source `catalog-mirror`: the catalog stores what some component *recorded*. |
| `catalog_entries` | Supported (`catalog_mirror` source) | Table assets with description, tags, columns. |
| Local workspaces, lakehouses, Delta / Iceberg / Parquet tables with columns, partitions, properties, protocol, version, history summary, row count, size, null counts and safe min/max | Supported | Read with delta-rs; never Spark. Min/max are never stored for text or binary columns, or for columns that look sensitive. |
| Notebooks, pipelines, saved SQL, dashboards, Spark job definitions | Supported | As assets *and* as definitions for the analyzers; the item `etag` skips unchanged definitions. |
| Files under `Files/` | Partial | Only the paths a source's `config.files` policy lists. |
| Value profiling | Opt-in | Needs a [sampling policy](../guides/intelligence.md#sampling-policies); otherwise a `sampling_disabled` (informational) gap. |

## Run-time events (verified)

`runtime.py`, `core/dataset_events.py`, `compute/lineage_events.py`, `runtime_lib/lineage_probe.py`. Tests: `tests/intelligence/test_runtime_facts.py` (388 lines), `test_ingest.py`, `test_isolation.py`, `tests/compute/` for the validation. These are the only source of **verified** edges.

| Event | Emitted by | Becomes |
|---|---|---|
| `dataset.read`, `dataset.written` | the SQL service (every caller path), jobs and notebook kernels (from the runner probe), pipeline copy activities | `READS_FROM` / `WRITES_TO` from the process to each dataset, **verified**, evidence `runtime:job`, `runtime:sql` or `runtime:pipeline` with the job, query or run id. The process is the OLCF item named by the event (notebook, Spark job definition, saved query, report, pipeline), a `sql_query` asset keyed by the statement hash for ad-hoc SQL, or a `spark_job` asset keyed by the job id (`olcf-job:<job id>`) when the job has no item. A run submitted from a Fabric notebook (SDK `run_remote`) is the Fabric notebook asset the tenant scan discovered: its events carry the Entra tenant, the Fabric workspace and notebook GUIDs, the process is resolved by the `fabric` alias `<tenant>/<workspace>/<notebook>` and the edges are **verified**; the tables are the scan's tables (the `fabric` / `onelake_path` aliases of the lakehouse the OLCF lakehouse is mapped to). An event that names only the notebook's name (no GUID) is matched to the notebook of that name in the OLCF workspace the Fabric workspace maps to, with **inferred** edges; when no such notebook exists yet a placeholder notebook of that name is created (the next scan promotes it). |
| `dataset.statement` | jobs and sessions (`spark.sql`, DuckDB `sql`/`execute`) | The statement text that really ran is parsed by the SQL analyzer and its edges are upgraded to **verified**; column mappings the analyzer derived become verified, those it only guessed (`inferred`) or could not determine (`unknown`) keep their level. The SQL text is delivered to in-process subscribers only, not stored in the event table. |
| `dataset.schema_changed` | catalog sync and table maintenance | Table attribute update and column assets (added, retyped; dropped columns are flagged `dropped`, never deleted). |
| `pipeline.run.succeeded|failed|cancelled` | pipelines | The pipeline asset's `last_run_state` (state only, so attribute history records flips, not every run). |
| `job.state_changed` | jobs | Recognised and deliberately empty (it names the job, not the item). Used by staleness for run outcomes. |

What the probe in the job process covers (`lineage_probe.py`):

| Covered | Notes |
|---|---|
| PySpark: `spark.table`, `spark.read.table/load/parquet/csv/json/orc/text`, `spark.sql`, `DataFrameWriter.saveAsTable/insertInto/save/parquet/csv/json/orc/text`, `DataFrame.writeTo(..).create/createOrReplace/replace/append/overwrite/overwritePartitions` | **Reads are reported when the plan is created**, not when rows are read: a DataFrame that is built and never used still counts. Writes are reported after the write succeeded. |
| Host Python: `deltalake.DeltaTable(path)` (at construction), `write_deltalake` (after success), DuckDB `duckdb.connect()` connections and module-level `duckdb.sql/query/execute` (statement text) | |
| Not covered | Spark Connect, streaming, `DeltaTable.merge/update/delete` objects, JDBC and other sources without a path or table name, pandas and polars readers. Temporary views appear as unresolved table names on the control plane. |
| Limits | De-duplicated per process; at most 200 events per process, then one `dataset_events_truncated` marker; a statement is clipped to 20,000 characters; instrumentation errors never reach user code. `OLCF_DATASET_EVENTS=0` turns the probe off. |

Because the runner executes user code, everything it reports is untrusted input; the control plane validates and clips it before it becomes an event (see the [security model](../concepts/security-model.md#intelligence-discovery-lineage-and-sampling)).

## Fabric connector (metadata)

`discovery/fabric.py`. It produces assets, not lineage: workspaces, items (Lakehouse, Warehouse, Notebook, DataPipeline, SemanticModel, Report, PaginatedReport, SparkJobDefinition, Dataflow), lakehouse tables with columns read from the Delta log, and definitions of notebooks, pipelines, semantic models and reports, which go to the analyzers above. Its gaps and limits are in [the guide](../guides/intelligence.md#connecting-microsoft-fabric-and-power-bi).

## Sensitive-data suggestions (not lineage)

`discovery/detect.py` is listed here because it is easy to mistake for a lineage feature. It suggests classifications from column **names**, **types** and (only if a sampling policy allows) values that are inspected in memory and discarded. Labels: `email`, `phone`, `national_id`, `credit_card`, `iban`, `bank_account`, `ip_address`, `person_name`, `address`, `date_of_birth`, `geolocation`, `credential`, `financial`, `special_category`. It is heuristic: name rules are token based (`customerEmail`, `customer_email` and `CUSTOMER EMAIL` all match; `classname` does not match `ssn`; `email_count` is not reported); value rules validate their candidates (Luhn and issuer prefix for card numbers, mod-97 for IBANs, real address parsing for IPs). Misses are expected (for example fully lower-case run-together names such as `customerssn`) and false positives are possible. Nothing is classified unless a person approves the suggestion in the review queue.
