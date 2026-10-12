# Use OLCF Intelligence: find where data comes from and what depends on it

Author: Justin Dodrill. This guide is for the people who use the asset graph (analysts, data owners, engineers, administrators). It is task-oriented: each section starts with the question or the job. The ideas behind the module (assets, aliases, confidence, gaps, evidence) are explained in [OLCF Intelligence](../concepts/intelligence.md); what each lineage analyzer does and does not understand is in the [lineage support matrix](../reference/lineage-support.md). The exact names of every command, endpoint and setting are in the generated [reference](../reference/README.md).

Every command output below was captured from a real server that was running the sample workspace (see [how the outputs were produced](#how-the-outputs-in-this-guide-were-produced)). Counts and ordering will differ on your data and with the version of the sample.

## What was verified, and what was not

| Part | Evidence | Status |
|---|---|---|
| Local source (workspaces, lakehouses, Delta tables, notebooks, pipelines, saved SQL, dashboards), the lineage analyzers, the graph, REST API, CLI, SDK, MCP tools, console pages | automated tests (`tests/intelligence`, `tests/cli`, `tests/sdk`, `tests/mcp`, `tests/web`) and the commands captured below | verified here |
| Run-time lineage from a Spark job | `tests/compute/test_dataset_events_spark.py` runs real PySpark 4.1.1 on parquet tables; the SQL-service, host-Python and DuckDB paths have their own tests. The test runtime has no Delta jars, so Spark's Delta write paths were not run | verified here, with that limit |
| Microsoft Fabric and Power BI connector, scanner import, sampling of Fabric tables | tests and the captures in this guide run against the test suite's Fabric server, which follows Microsoft's documentation | covered by the automated suite; run the connection test against your tenant first |
| PostgreSQL as the platform database | the `postgres`-marked variants of the Intelligence tests exist | SQLite is the default store |
| Performance on a large estate | no benchmark exists; the limits in this guide are caps | size your estate against them |

## A first look with the sample

You need a running OLCF with the sample workspace. If you followed [getting started](../getting-started/README.md) (`olcf quickstart`) it is already installed. Otherwise:

```sh
olcf sample install
olcf sample status
```

```text
$ olcf sample status
workspace:         Sample Retail
lakehouse:         retail_lakehouse
installed:         yes
current_version:   4
installed_version: 4
up_to_date:        yes
running:           no
step:              -
workspace_id:      wks_01M4JC3NQTTSEWY2QNX99VT43J
lakehouse_id:      itm_01M4JC3NR40T1394840Z40D0YJ
size:              standard
installed_at:      2026-10-10T07:40:48Z
```

The sample is the workspace **Sample Retail** with the lakehouse `retail_lakehouse`, the notebooks `01_Retail_Medallion_ETL` and `02_Retail_Explore_Python`, saved SQL queries and the dashboard *Retail Sales Overview*. Installing it also fills Intelligence, when the module is installed, so that nothing is empty on the first day. The installer scans the workspace with the `local` source (the same thing you run yourself in the next section) and imports a **synthetic** Power BI scan (a semantic model and a report that are invented for the sample, labelled as such wherever they appear, and stored as a source of kind `scanner_import`) so that lineage reaches from the files to a report. It then adds a business glossary of 12 terms linked to the columns and measures they describe, the domain *Retail Sales*, the data product *Retail Sales Analytics* (the three gold tables, with a freshness SLA), makes the installing user owner of the domain and the gold tables and steward of `dim_customer`, approves the `PII` label on `dim_customer.email`, and certifies the gold tables. The suggestions it does not decide (the name columns of `dim_customer`, more glossary links) are left in the review queue for you. If a step of this fails the install does not: the problem is reported and the other steps carry on. Run `olcf sample install` again and nothing you changed is overwritten.

**In the console**: open **Intelligence** in the sidebar. The overview shows the assets by type, the sources and their last scan, how sure the connections are, what the scans could not see and what changed since the previous scan. Then open **Data catalog**, search for `fact_sales`, and open it: the asset page has the tabs **Overview**, **Columns**, **Lineage**, **Impact**, **Origin**, **Quality**, **Documentation**, **History**, **Sample** (only if you hold `intelligence.sample`) and **Governance**.

**On the command line**:

```text
$ olcf intelligence summary
assets:       221  (column 152, visual 19, measure 11, table 10, sql_query 9, file 5, model_table 3, notebook 3, page 2, report 2, external 1, lakehouse 1, script 1, semantic_model 1, workspace 1)
edges:        243  (derived 207, inferred 36, verified 0, unknown 0)
placeholders: 27
open gaps:    61  (unresolved_reference 50, dynamic_reference 9, not_inspected 1, unsupported_construct 1)
discovery:    yes
governance:   yes
scope:        organisation
event inbox: 0 pending, 0 failed

SOURCE                          SCAN                            MODE         STATUS     VERSION  FINISHED
src_01M4JC3FTV1GEE50TYSSVB7NM6  scn_01M4JC3RH183XNV33C5C4YY9AB  incremental  succeeded  2        2026-10-10T07:40:49.959225Z
src_01M4JC3SXFX3AA7N5RDTXDDQVV  scn_01M4JC3SXM2B7TSP5W3AQS3NQ8  full         succeeded  1        2026-10-10T07:40:50.316151Z
```

How to read it:

* *assets* are everything the scans found, by type. A **placeholder** is something that other code mentions (for example a table name in a notebook) but no scan has found yet; it is kept so that the link is not lost and is merged into the real asset when it appears.
* *edges* are the links, by [confidence](../concepts/intelligence.md#confidence-levels): **verified** (observed at run time), **derived** (read from code, definitions or metadata), **inferred** (assumed from a name or a default value). A fresh sample has no verified edges because nothing has run since it was written; they appear when a notebook, job or SQL statement runs (see the [staleness](#why-is-this-stale) and [run-time](#what-happens-when-code-runs) sections).
* *open gaps* are things a scan could not read or understand. They are part of the answer, not noise: see [troubleshooting](#troubleshooting).
* *event inbox* is the queue of platform events (`dataset.*`, `pipeline.run.*`) that Intelligence has not processed yet. `0 pending` is the healthy state.

Find things:

```text
$ olcf intelligence search revenue
ID                              TYPE       NAME                            PATH                                   WORKSPACE                     STATUS
ast_01M4JC3RHR4DG7339E7J6HEQWC  sql_query  Revenue by month                Sample Retail / Revenue by month       wks_01M4JC3NQTTSEWY2QNX99VT…  active
ast_01M4JC3RHV76XHC6GVMQMM8XSF  sql_query  Top products in a category      Sample Retail / Top products in a ca…  wks_01M4JC3NQTTSEWY2QNX99VT…  active
ast_01M4JC3RJ1N37BH99XJJVND4AF  sql_query  Daily sales, last N days        Sample Retail / Daily sales, last N …  wks_01M4JC3NQTTSEWY2QNX99VT…  active
ast_01M4JC3RJ6E7H88F2JAFKEFVST  sql_query  Store performance ranking       Sample Retail / Store performance ra…  wks_01M4JC3NQTTSEWY2QNX99VT…  active
ast_01M4JC3RJ9KN4APHKA27YXV4F7  sql_query  Channel mix by quarter          Sample Retail / Channel mix by quart…  wks_01M4JC3NQTTSEWY2QNX99VT…  active
[... 33 more lines]
```

And look at one:

```text
$ olcf intelligence assets show fact_sales
id:            ast_01M4JC3RSEBHG1SPJ3G5HCAWVW
type:          table
name:          fact_sales
path:          Sample Retail / retail_lakehouse / fact_sales
workspace:     wks_01M4JC3NQTTSEWY2QNX99VT43J
source:        src_01M4JC3FTV1GEE50TYSSVB7NM6
status:        active
parent:        retail_lakehouse
description:   One row per order line. Partitioned by year and month of the order date.
tags:          -
children:      20
lineage edges: 1 upstream, 19 downstream
open gaps:     0
first seen:    scn_01M4JC3RH183XNV33C5C4YY9AB
last seen:     scn_01M4JC3RH183XNV33C5C4YY9AB
etag:          db60e5e50ca03c80e844cdd5
[... 29 more lines]
```

The whole output also lists every column with its type and description and the aliases (the names other tools use for the same thing). Add `--json` to any `olcf intelligence` command for the structured form.

## Running a scan

A **scan** reads one *source* and records what it saw as a **snapshot**. A source is where Intelligence looks: `local` (this OLCF installation, created automatically), `catalog_mirror` (also mirrors the classic catalog's table index), `fabric` (a Microsoft Fabric tenant, see [below](#connecting-microsoft-fabric-and-power-bi)) or `scanner_import` (a saved Power BI scanner result, see [below](#importing-a-saved-scanner-result)).

```text
$ olcf intelligence sources list
NAME                     KIND            ENABLED  STATUS     LAST SCAN                 NEXT DUE                  CREDENTIALS  ID
local                    local           yes      succeeded  2026-10-10T07:40:49.962…  2026-10-10T07:55:48.547…  no           src_01M4JC3FTV1GEE50TYS…
Sample Retail (synthet…  scanner_import  yes      succeeded  2026-10-10T07:40:50.318…  -                         no           src_01M4JC3SXFX3AA7N5RD…
```

Scans normally run on a schedule (see [schedules](#schedules)); to run one now you need `intelligence.manage` (Platform administrators, Security administrators and Workspace administrators):

```text
$ olcf intelligence sources scan local --mode incremental --wait
scan:     scn_01M4JC4PRT99V2C1NX88DC1DKY
source:   src_01M4JC3FTV1GEE50TYSSVB7NM6
mode:     incremental
trigger:  api
status:   succeeded
version:  3
started:  2026-10-10T07:41:19.514986Z
finished: 2026-10-10T07:41:19.551536Z
error:    -
assets:   0 created, 0 updated, 2 unchanged
edges:    0 created, 0 updated, 0 unchanged
gaps:     0

==> scan scn_01M4JC4PRT99V2C1NX88DC1DKY started (incremental)
```

* `--mode incremental` (the default) re-reads what may have changed and never marks anything as removed. `--mode full` re-reads everything of that source, and **only a full scan that ends `succeeded` marks the assets and edges it did not see as `removed`**. A scan that ends `partial` (something could not be read) never removes anything, so a permission error cannot make the graph forget what it just could not see.
* Only one scan per source runs at a time. A second request gets `409 Conflict` naming the running scan; wait for it or cancel it (`olcf intelligence scans cancel SCAN_ID`).
* `--wait` follows the scan until it ends. The exit status is 0 for `succeeded` or `partial`, 1 for `failed` or `cancelled`, 9 if the wait timed out.
* In the console: **Intelligence, Discovery** has the tabs **Sources** (with *Scan now* and *Test connectivity*), **Scans** (statistics and gaps per scan), **Gaps**, **Coverage**, **Changes** (snapshots and what changed between them) and, for holders of `intelligence.manage`, **Import** and **Sampling**.

Every scan is a snapshot. List them, and compare any two:

```text
$ olcf intelligence snapshots
SNAPSHOT                        SOURCE                    VERSION  MODE         STATUS     FINISHED  CHANGES
scn_01M4JC4PRT99V2C1NX88DC1DKY  local                     3        incremental  succeeded  1s ago    +0 assets, 0 updated, +0 edges
scn_01M4JC3SXM2B7TSP5W3AQS3NQ8  Sample Retail (syntheti…  1        full         succeeded  30s ago   +55 assets, 2 updated, +68 edges
scn_01M4JC3RH183XNV33C5C4YY9AB  local                     2        incremental  succeeded  30s ago   +138 assets, 15 updated, +175 edges
scn_01M4JC3M5BR8F8PJ7XRBYVGQ0S  local                     1        full         succeeded  36s ago   +0 assets, 0 updated, +0 edges
```

`olcf intelligence diff --source local` shows what was added, removed and changed (a column added, removed or retyped) between the latest two snapshots of a source; `olcf intelligence assets history ASSET` shows the attribute versions of one asset:

```text
$ olcf intelligence assets history fact_sales
WHEN                         SCAN                            CHANGES
2026-10-10T07:40:48.815601Z  scn_01M4JC3RH183XNV33C5C4YY9AB  created
```

## Reading lineage

Lineage answers "what feeds this, and what does it feed". There are two levels.

**Table level.** The graph connects datasets (tables, files, models) *through the thing that moves the data* (a notebook, a pipeline activity, a saved query, a job). That is why the tree has the notebook between two tables, not an arrow from table to table:

```text
$ olcf intelligence lineage gold_daily_sales --direction up --depth 2
gold_daily_sales (table)  Sample Retail / retail_lakehouse / gold_daily_sales
└── upstream (flows into it)
    └── <- 01_Retail_Medallion_ETL (notebook)  [derived]
        ├── <- dim_customer (table)  [derived]
        │   └── <- sample generator (script)  [derived]
        │       ├── <- Files/raw/customers (file)  [derived]
        │       ├── <- Files/raw/products (file)  [derived]
        │       ├── <- Files/raw/sales (file)  [derived]
        │       └── <- Files/raw/stores (file)  [derived]
        ├── <- dim_product (table)  [derived]
        │   └── <- sample generator (script)  [derived] (see above)
        ├── <- dim_store (table)  [derived]
        │   └── <- sample generator (script)  [derived] (see above)
        ├── <- fact_sales (table)  [derived]
        │   └── <- sample generator (script)  [derived] (see above)
        ├── <- Files/raw/sales (file)  [derived]
        ├── <- gold_category_summary (table)  [derived]
        │   └── <- 01_Retail_Medallion_ETL (notebook)  [derived] (cycle)
        ├── <- gold_customer_ltv (table)  [derived]
        │   └── <- 01_Retail_Medallion_ETL (notebook)  [derived] (cycle)
        ├── <- gold_daily_sales (table)  [derived] (cycle)
        └── <- silver_sales_enriched (table) [unresolved]  [derived]
            └── <- 01_Retail_Medallion_ETL (notebook)  [derived] (cycle)
lineage is incomplete: 3 gap(s) (unresolved_reference x2, unsupported_construct x1); see `olcf intelligence gaps`
```

* `<-` marks what flows into the asset (upstream), `->` what it flows into (downstream). The word in brackets is the **confidence of that link**.
* `[unresolved]` marks a placeholder: code mentions it, no scan found it.
* `(cycle)` and `(see above)` mark nodes that were already shown, so a loop or a diamond is shown once. (The sample notebook reads and writes its own gold tables, which is why cycles appear.)
* The last line is the rule that matters most: **"lineage is incomplete: N gap(s)"**. It lists the open gaps that touch this answer. If you see it, treat what you read as a lower bound.

**Column level.** `--level column` (or naming the column) follows how a column is computed from other columns:

```text
$ olcf intelligence lineage gold_daily_sales.net_revenue --level column
Column lineage around gold_daily_sales.net_revenue (9 edge(s), direction both, depth 3)
DIRECTION   FROM                              TO                               VIA                      TRANSFORMATION                   CONFIDENCE
upstream    fact_sales.net_amount             gold_daily_sales.net_revenue     01_Retail_Medallion_ETL  aggregate                        derived
upstream    silver_sales_enriched.net_amount  gold_daily_sales.net_revenue     01_Retail_Medallion_ETL  ROUND(SUM(net_amount), 2)        derived
downstream  Daily Sales.Total Revenue         Daily Sales.Average Order Value                                                            derived
downstream  Daily Sales.Total Revenue         Daily Sales.Discount Amount                                                                derived
downstream  Daily Sales.Total Revenue         Daily Sales.Gross Margin %                                                                 derived
downstream  Daily Sales.Total Revenue         Revenue (card)                                                                             derived
downstream  Daily Sales.Total Revenue         Revenue by day (line)                                                                      derived
downstream  Daily Sales.net_revenue           Daily Sales.Total Revenue        Retail Sales Model       SUM('Daily Sales'[net_revenue])  derived
downstream  gold_daily_sales.net_revenue      Daily Sales.net_revenue          Retail Sales Model                                        inferred
lineage is incomplete: 10 gap(s) (unresolved_reference x9, unsupported_construct x1); see `olcf intelligence gaps`
```

`VIA` is the notebook, query or activity that does the computing; `TRANSFORMATION` is the expression. A column that is the result of an aggregate lists every column the aggregate reads.

**In the console**: the **Lineage** page draws the same graph (zoom, fit to window, export as picture or data), with *Direction* (upstream, downstream, both), a *How sure* filter (show everything, inferred or better, derived or better, verified only), a switch that hides or shows the notebooks and queries between tables, and a **Columns** view. Click a link to see **why we think so**: every observation (which cell of which notebook, which statement, when). Click a node for its details.

**Confidence in one paragraph.** A link has the strength of its strongest evidence; a *path* has the strength of its weakest link. `verified` is only ever produced by something that really ran (a job, an SQL statement, a pipeline copy) and was reported by the runner. `derived` means a definition says so: parsed code, a pipeline definition, a report's model binding, the Power BI scanner, or a catalog record. `inferred` means an assumption was needed (a notebook parameter's default value was used, a column was matched by name). A run-time observation upgrades an existing derived link to verified and keeps it so for `intelligence.verified_evidence_days` (90) days after it was last seen. Use `--min-confidence derived` to drop the assumptions.

**What this does not tell you.** Lineage is incomplete by construction: code that builds table names at run time, Scala cells, stored procedures and pivots are reported as gaps, not guessed. See the [support matrix](../reference/lineage-support.md) for each analyzer.

## The five questions

Each question below shows the console route, the CLI, the Python SDK and MCP (for an AI agent). They read the same graph, so the answers agree.

### Where does this number come from?

The question behind a number on a dashboard. Start at the visual (or report, or measure, or column) and ask for its origin: the upstream *roots*, and every path from them, source first, with the weakest confidence on the path.

* **Console**: Data catalog, search for the visual, open it, **Origin** tab. The roots are listed and each path can be opened.
* **CLI**:

```text
$ olcf intelligence where-from Revenue --type visual
Where Revenue (visual) comes from (5 origin(s), 9 path(s)):
ORIGIN                 TYPE    PATH                                  WORKSPACE                     SOURCE SYSTEM  PATHS  CONFIDENCE
Files/raw/customers    file                                          wks_01M4JC3NQTTSEWY2QNX99VT…  yes            2      derived
Files/raw/products     file                                          wks_01M4JC3NQTTSEWY2QNX99VT…  yes            2      derived
Files/raw/sales        file                                          wks_01M4JC3NQTTSEWY2QNX99VT…  yes            2      derived
Files/raw/stores       file                                          wks_01M4JC3NQTTSEWY2QNX99VT…  yes            2      derived
fact_sales.net_amount  column  Sample Retail / retail_lakehouse / …  wks_01M4JC3NQTTSEWY2QNX99VT…  no             1      derived

Paths (source first; confidence is the weakest edge on the path):
[... 9 more lines]
```

The roots with `SOURCE SYSTEM yes` are where the data enters the platform (files, an external system). The paths show every table and every process on the way; `[derived, 2 hop(s), via sample generator]` is the confidence of the weakest link, the number of dataset hops, and the processes involved.

Note what this particular output teaches: one root is the column `fact_sales.net_amount` (the visual is bound to that column), the other four roots are the raw files, reached through the process that generated the sample tables. That is exactly what the recorded definitions say; whether a given file really contributes to a given number is not something static analysis can prove, which is why every path is `derived` and not `verified`.

Asking for the origin of a **column** (an asset of type column, or `table.column`) is answered at column level automatically: a table-level walk starts at the column's table and has no path of the column, so the answer is traced over the column-level (`TRANSFORMS`) edges and says so (`level_switched` in the JSON: from `table` to `column`). For one column of a table add `--level column` or name the column: `olcf intelligence where-from fact_sales.net_amount --level column`.

* **SDK**: `c.intelligence.origin(asset_id, level="table"|"column", column=None)`. **MCP**: `intel_where_does_this_come_from` (see [the examples below](#the-same-questions-from-python-and-from-an-ai-agent)).

### What breaks if I change this?

* **Console**: the asset's **Impact** tab (or the *Impact analysis* page). It lists, grouped by type, every table, model, measure, report and visual downstream, nearest first, with owners when governance data exists, the code that reads or writes it, and the gaps that may be hiding consumers.
* **CLI**:

```text
$ olcf intelligence impact fact_sales --depth 1
If fact_sales (table) changes, 15 downstream asset(s) are affected (visual 10, table 4, report 1).
TYPE    NAME                           PATH                                  WORKSPACE                     HOPS  CONFIDENCE  OWNERS
report  Retail Sales Overview          Sample Retail / Retail Sales Overvi…  wks_01M4JC3NQTTSEWY2QNX99VT…  1     derived     -
table   gold_category_summary          Sample Retail / retail_lakehouse / …  wks_01M4JC3NQTTSEWY2QNX99VT…  1     derived     Bootstrap Administrator
table   gold_customer_ltv              Sample Retail / retail_lakehouse / …  wks_01M4JC3NQTTSEWY2QNX99VT…  1     derived     Bootstrap Administrator
table   gold_daily_sales               Sample Retail / retail_lakehouse / …  wks_01M4JC3NQTTSEWY2QNX99VT…  1     derived     Bootstrap Administrator
table   silver_sales_enriched                                                wks_01M4JC3NQTTSEWY2QNX99VT…  1     derived     -
visual  Average order value            Retail Sales Overview / Average ord…  wks_01M4JC3NQTTSEWY2QNX99VT…  1     derived     -
visual  Channel mix                    Retail Sales Overview / Channel mix   wks_01M4JC3NQTTSEWY2QNX99VT…  1     derived     -
visual  Customers                      Retail Sales Overview / Customers     wks_01M4JC3NQTTSEWY2QNX99VT…  1     derived     -
visual  Orders                         Retail Sales Overview / Orders        wks_01M4JC3NQTTSEWY2QNX99VT…  1     derived     -
visual  Revenue                        Retail Sales Overview / Revenue       wks_01M4JC3NQTTSEWY2QNX99VT…  1     derived     -
[... 7 more lines]
```

`HOPS` counts dataset hops. For a column, ask about the column; the answer then includes the columns computed from it:

```text
$ olcf intelligence impact fact_sales.net_amount
If net_amount (column) changes, 40 downstream asset(s) are affected (visual 19, measure 11, table 4, model_table 3, report 2, semantic_model 1).
TYPE            NAME                           PATH                            WORKSPACE                     HOPS  CONFIDENCE  OWNERS
report          Retail Sales Overview          Sample Retail / Retail Sales …  wks_01M4JC3NQTTSEWY2QNX99VT…  1     derived     -
table           gold_category_summary          Sample Retail / retail_lakeho…  wks_01M4JC3NQTTSEWY2QNX99VT…  1     derived     Bootstrap Administrator
table           gold_customer_ltv              Sample Retail / retail_lakeho…  wks_01M4JC3NQTTSEWY2QNX99VT…  1     derived     Bootstrap Administrator
table           gold_daily_sales               Sample Retail / retail_lakeho…  wks_01M4JC3NQTTSEWY2QNX99VT…  1     derived     Bootstrap Administrator
table           silver_sales_enriched                                          wks_01M4JC3NQTTSEWY2QNX99VT…  1     derived     -
visual          Average order value            Retail Sales Overview / Avera…  wks_01M4JC3NQTTSEWY2QNX99VT…  1     derived     -
visual          Channel mix                    Retail Sales Overview / Chann…  wks_01M4JC3NQTTSEWY2QNX99VT…  1     derived     -
visual          Customers                      Retail Sales Overview / Custo…  wks_01M4JC3NQTTSEWY2QNX99VT…  1     derived     -
visual          Orders                         Retail Sales Overview / Orders  wks_01M4JC3NQTTSEWY2QNX99VT…  1     derived     -
visual          Revenue                        Retail Sales Overview / Reven…  wks_01M4JC3NQTTSEWY2QNX99VT…  1     derived     -
visual          Revenue by category            Retail Sales Overview / Reven…  wks_01M4JC3NQTTSEWY2QNX99VT…  1     derived     -
visual          Revenue by month               Retail Sales Overview / Reven…  wks_01M4JC3NQTTSEWY2QNX99VT…  1     derived     -
[... 30 more lines]
```

An empty result is **not** proof that nothing depends on the asset. It only says nothing was found in the scanned sources. Things outside them (a notebook that was never scanned, another system reading the files) are invisible. The answer prints the gaps it knows of ("lineage is incomplete ...") and says when the list was cut at `intelligence.max_lineage_nodes` (500) nodes.

* **SDK**: `c.intelligence.impact(asset_id, depth=5, min_confidence=None)`.
* **MCP**: `intel_impact_analysis`, arguments such as `{"asset": "fact_sales.net_amount", "max_items": 50}`.

### Why is this stale?

"Stale" means the newest write Intelligence knows of is older than a threshold (`intelligence.stale_after_hours`, 24; override per call). The staleness analysis walks upstream and names the **most upstream stale producer**, the likeliest root cause, together with the outcome of the run that wrote it.

* **Console**: asset page, **Overview** (the *Freshness* card) and the **Impact** tab (*Is it fresh? What could be holding it up?*).
* **CLI**:

```text
$ olcf intelligence assets staleness gold_daily_sales --stale-only
asset:       gold_daily_sales (table)
stale:       no
last write:  2026-10-10T07:40:47.958000Z
threshold:   24.0 h
explanation: 'gold_daily_sales' looks fresh (last write 0.02 h ago). 1 upstream dataset(s) are stale.

PRODUCER               TYPE   STALE  ROOT CAUSE  LAST WRITE  AGE H  LAST RUN  REASONS
silver_sales_enriched  table  yes    no          -           -      -         a producer exists but no write was ever observed
lineage is incomplete: 3 gap(s) (unresolved_reference x2, unsupported_construct x1); see `olcf intelligence gaps`

$ olcf intelligence assets staleness gold_daily_sales --stale-only --stale-after-hours 0.01
asset:       gold_daily_sales (table)
stale:       yes
last write:  2026-10-10T07:40:47.958000Z
threshold:   0.01 h
explanation: 'gold_daily_sales' is stale: last write 0.0 h ago, threshold 0.01 h. The most upstream stale producer is 'dim_customer' (last write 0.0 h ago, threshold 0.01 h).

PRODUCER               TYPE   STALE  ROOT CAUSE  LAST WRITE                   AGE H  LAST RUN  REASONS
gold_daily_sales       table  yes    no          2026-10-10T07:40:47.958000Z  0.02   -         last write 0.0 h ago, threshold 0.01 h
dim_customer           table  yes    yes         2026-10-10T07:40:47.792000Z  0.02   -         last write 0.0 h ago, threshold 0.01 h
dim_product            table  yes    yes         2026-10-10T07:40:47.800000Z  0.02   -         last write 0.0 h ago, threshold 0.01 h
dim_store              table  yes    yes         2026-10-10T07:40:47.805000Z  0.02   -         last write 0.0 h ago, threshold 0.01 h
fact_sales             table  yes    yes         2026-10-10T07:40:47.909000Z  0.02   -         last write 0.0 h ago, threshold 0.01 h
gold_category_summary  table  yes    no          2026-10-10T07:40:47.963000Z  0.02   -         last write 0.0 h ago, threshold 0.01 h
gold_customer_ltv      table  yes    no          2026-10-10T07:40:47.970000Z  0.02   -         last write 0.0 h ago, threshold 0.01 h
[... 2 more lines]
```

The first answer is honest rather than dramatic: the sample's tables were written a moment ago, so `gold_daily_sales` is fresh against the default 24 hours, and the one upstream finding is that `silver_sales_enriched` is a table nobody has ever been seen writing. To see what a stale answer looks like on a young sample, lower the threshold (`--stale-after-hours`); the second command above uses 0.01 hours (36 seconds) and the analysis then names the most upstream stale producers (`ROOT CAUSE yes`) among everything that feeds the table. The exact ages on your screen will differ from the ones printed here.

`LAST WRITE` comes from two places: the Delta log's last commit time (found by a scan) and observed `dataset.written` events (from jobs, SQL and pipelines). `LAST RUN` is the outcome of the pipeline or job that wrote it, when one was observed (`pipeline.run.*` events). A producer with no known write time is reported as such, never as fresh.

What this cannot see: a table that is written by something outside OLCF and not visible in the Delta log; a write that happened but whose run-time event was lost (events are best-effort by design, see [operating notes](#operating-notes)); freshness of a source system.

* **SDK**: `c.intelligence.staleness(asset_id, depth=5, stale_after_hours=6)`.
* **MCP**: `intel_staleness`.

### What is unused?

Tables and views nothing was seen reading within a window (`--days`, default `intelligence.default_unused_days`, 30): candidates for clean-up, not a verdict.

```text
$ olcf intelligence unused --days 30
no unused tables in that window
```

There is no console page for this report in this version; use the CLI, SDK or MCP.

* **SDK**: `c.intelligence.unused(days=90, type="table")`. **MCP**: `intel_unused_assets`.
* *Read the caveat before you delete anything.* "Unused" means no reader was observed: no consumer edge in the graph and no run-time read inside the window. A table read by a notebook that was not scanned, by an external tool, or only in a month-end job that did not run in the window, looks unused. The report excludes assets that something downstream reads; it cannot know about readers outside OLCF.

### Where does PII end up?

This needs governance data: a label is attached to a column (a person approved it) and then **propagated along lineage** (computed, never stored) to everything downstream. The sample installer has already approved the `PII` label on one column, `dim_customer.email`, so the question has an answer from the start:

```text
$ olcf governance exposure PII
Data labelled PII reaches 29 asset(s): 0 proven, 29 possible; 1 report(s), 1 semantic model(s), 1 data product(s).
Labelled directly: dim_customer.email

ASSET                                         TYPE            PRECISION  PROVEN    CONFIDENCE  FROM                            HOPS
Sample Retail / retail_lakehouse / gold_cat…  table           table      possible  inferred    Sample Retail / retail_lakeho…  2
Sample Retail / retail_lakehouse / gold_cus…  table           table      possible  inferred    Sample Retail / retail_lakeho…  2
[... 29 more lines]
```

Every asset in that list is `possible`, not `proven`: only the table-level link from `dim_customer` onwards is known, and the report does not know which columns travel along it, so it says that PII *may* reach them and claims no more. Reading the sample's ETL notebook, it carries `segment` and `loyalty_tier` out of `dim_customer` and not `email`, so here the true answer is probably that no PII reaches the gold tables; the report cannot tell, and errs on the side of reporting. Two more columns of `dim_customer` look sensitive and are still waiting. Suggestions come from column names and types (`email`, `first_name`, `phone`, `ssn` and similar; see the [support matrix](../reference/lineage-support.md#sensitive-data-suggestions-not-lineage)) and nothing counts until a person approves it. Review them (see [governance workflows](#governance-workflows) for the full cycle); `--kind classification` leaves out the glossary-link suggestions, which have their own queue entries:

```text
$ olcf governance review-queue list --kind classification
ID                KIND            ASSET                                   PROPOSAL  CONFIDENCE  SOURCE  NEEDS              CREATED
classification:1  classification  Sample Retail / retail_lakehouse / di…  PII       0.85        auto    governance.curate  2026-10-10T07:40:50.332241Z
classification:2  classification  Sample Retail / retail_lakehouse / di…  PII       0.85        auto    governance.curate  2026-10-10T07:40:50.332241Z
```

```text
$ olcf governance review-queue approve classification:1 classification:2 --comment reviewed
ITEM              RESULT  REASON
classification:1  ok
classification:2  ok
approved: 2, failed: 0
```

The directly labelled columns now include the two names (the numbers of reached assets do not change, because the table was already reached through `email`):

```text
$ olcf governance exposure PII
Data labelled PII reaches 29 asset(s): 0 proven, 29 possible; 1 report(s), 1 semantic model(s), 1 data product(s).
Labelled directly: dim_customer.first_name, dim_customer.last_name, dim_customer.email

ASSET                                         TYPE            PRECISION  PROVEN    CONFIDENCE  FROM                            HOPS
Sample Retail / retail_lakehouse / gold_cat…  table           table      possible  inferred    Sample Retail / retail_lakeho…  2
Sample Retail / retail_lakehouse / gold_cus…  table           table      possible  inferred    Sample Retail / retail_lakeho…  2
Sample Retail / retail_lakehouse / gold_dai…  table           table      possible  inferred    Sample Retail / retail_lakeho…  2
silver_sales_enriched                         table           table      possible  inferred    Sample Retail / retail_lakeho…  2
[... 27 more lines]
```

* **Proven** exposure follows recorded lineage (`derived` or better, by default `governance.propagation_min_confidence`) from the labelled column; **possible** exposure is where only the column's table feeds the target and column lineage is not known. Both are listed, labelled as such. The path shows how the data gets there.
* **Console**: **Governance, Exposure**: pick a label and see the reports, visuals, models and products it reaches; **Governance, Classifications** and the **Review queue**.
* **SDK**: `c.governance.classification_exposure(label_id)`; **MCP**: `governance_exposure`.
* Missing exposure is not proof that none exists. The report says so ("lineage is incomplete wherever gaps exist").

### The same questions from Python and from an AI agent

The SDK (`olcf.sdk`, see [the SDK guide](sdk.md)) takes asset ids, which `list_assets` finds. This script answers three of the questions above; it used the sample on the capture server and printed what follows it:

```python
from olcf.sdk import Client

c = Client.from_context()                      # what `olcf login` stored; or Client(url, token=...)
fact = c.intelligence.list_assets(q="fact_sales", type="table")[0]
impact = c.intelligence.impact(fact["id"], depth=1)
print(impact["total"], "assets depend on", fact["name"])
for kind, group in impact["by_type"].items():
    print(f"  {kind}: {group['count']}")
visual = c.intelligence.list_assets(q="Revenue", type="visual")[0]
origin = c.intelligence.origin(visual["id"])
for root in origin["roots"]:
    print("root:", root["asset"]["type"], root["asset"]["name"], "-", root["best_confidence"])

pii = next(x for x in c.governance.list_classifications() if x["name"] == "PII")
print(c.governance.classification_exposure(pii["id"])["summary"])
```

```text
15 assets depend on fact_sales
  visual: 10
  report: 1
  table: 4
root: file Files/raw/customers - derived
root: file Files/raw/products - derived
root: file Files/raw/sales - derived
root: file Files/raw/stores - derived
root: column net_amount - derived
{'by_type': {'measure': 11, 'model_table': 3, 'report': 1, 'semantic_model': 1, 'table': 4, 'visual': 9}, 'reports': 1, 'visuals': 9, 'semantic_models': 1, 'products': 1, 'proven': 0, 'possible': 29}
```

For an AI agent, the 10 `intel_*` and 4 `governance_*` tools are read-only (see [Connect an AI agent](mcp.md#intelligence-tools-where-data-comes-from-and-what-depends-on-it)). They take a name, a path, `table.column` or an `ast_` id. The tool `intel_where_does_this_come_from` with the arguments `{"asset": "Revenue"}` answered (abridged: the full answer also holds every path with its evidence summary and the gaps):

```json
{
  "summary": "Retail Sales Overview / Revenue originates from Files/raw/customers, Files/raw/products, Files/raw/sales, Files/raw/stores, fact_sales.net_amount through 9 path(s); the weakest link on any path is derived.",
  "complete": true,
  "origins": [
    {
      "name": "Files/raw/customers",
      "type": "file",
      "paths": 2,
      "best_confidence": "derived"
    },
    {
      "name": "Files/raw/products",
      "type": "file",
      "paths": 2,
      "best_confidence": "derived"
    },
    {
      "name": "Files/raw/sales",
      "type": "file",
      "paths": 2,
      "best_confidence": "derived"
    },
    {
      "name": "Files/raw/stores",
      "type": "file",
      "paths": 2,
      "best_confidence": "derived"
    },
    {
      "name": "net_amount",
      "type": "column",
      "paths": 1,
      "best_confidence": "derived"
    }
  ],
  "first_path": {
    "chain": [
      "fact_sales.net_amount",
      "Retail Sales Overview / Revenue"
    ],
    "confidence": "derived",
    "hops": 1,
    "through": [],
    "evidence": {
      "links": 1,
      "min_evidence_count": 1,
      "last_seen_at": "2026-10-10T07:40:49.700896Z"
    }
  }
}
```

`complete: true` here means no open gap touches those paths; `false` would mean the answer is a lower bound.

## Connecting Microsoft Fabric and Power BI

> **Read this first.** The Fabric and Power BI connector, the scanner import and the sampling of Fabric tables follow Microsoft's documentation and are covered in the test suite by a local HTTP server (`tests/fabric/fabric_fakes.py`) that reproduces the documented behaviour. Every Fabric-side statement in this section (menu names, tenant-setting names, role requirements, quotas, response shapes) is Microsoft's documentation as understood when this was written. Where it differs from your tenant, your tenant is right. The [checklist at the end of the section](#what-to-check-on-first-contact-with-your-tenant) tells you what to look at first.

Intelligence reads a tenant with a **service principal** (an Entra application identity, not a person). It needs two kinds of access, and they are separate on purpose:

* the **metadata identity** reads metadata: workspaces, items, item definitions (notebook code, pipeline JSON, semantic model and report definitions), the lakehouse tables' Delta log (for column names and types), and the Power BI admin scanner's result;
* an optional **sampling identity** is the only thing that can read *data*, and only when a [sampling policy](#sampling-policies) allows it. The scanner never reads data with the metadata identity beyond Delta log metadata.

The control plane makes outbound HTTPS calls to Microsoft (Entra, Fabric, Power BI, OneLake): see [ports and networking](../reference/ports-and-networking.md#outbound-connections). Fabric-only installations are described in [Intelligence on-premises, with Fabric, and against Fabric only](../concepts/intelligence-fabric-and-onprem.md).

### 1. Register an application in Entra

In the Microsoft Entra admin center, **App registrations, New registration**, single tenant. Note the **Directory (tenant) ID** and the **Application (client) ID**. Under **Certificates & secrets** create a client secret (or upload a certificate; OLCF accepts a PEM private key and certificate). Do **not** add Power BI or Fabric API permissions to the registration: according to Microsoft's documentation, access for service principals is granted through *tenant settings* and *workspace roles*, and a service principal for the read-only admin APIs should not have admin-consent-required Power BI permissions configured on the app. (This statement is Microsoft's; confirm it on your tenant.)

### 2. Create a security group

Create an Entra **security group** (for example `olcf-intelligence`) and add the service principal to it. Tenant settings below are enabled for this group only, so the permission is limited to it.

### 3. Enable the tenant settings

In the Fabric admin portal, **Tenant settings**, enable for the security group:

| Setting (Microsoft's current names, which may change) | Needed for |
|---|---|
| *Service principals can use Fabric APIs* (Developer settings) | listing workspaces, items and definitions |
| *Service principals can access read-only admin APIs* (Admin API settings) | the Power BI admin scanner (lineage, data sources, models) |
| *Enhance admin APIs responses with detailed metadata* (Admin API settings) | tables, columns and measures in scanner results (`datasetSchema`) |
| *Enhance admin APIs responses with DAX and mashup expressions* (Admin API settings) | measure and query expressions in scanner results (`datasetExpressions`) |
| *Users can access data stored in OneLake with apps external to Fabric* (OneLake settings) | reading a table's Delta log (columns) through OneLake, and sampling |

Microsoft documents that a change can take **up to 15 minutes** to apply. The connectivity test prints this text as its `fix:` line when a step fails because of it.

### 4. Give the service principal access to workspaces

The admin scanner does not need workspace membership. Listing items, tables and definitions does:

* add the service principal to each workspace you want discovered. **Viewer** lists items;
* reading the *definition* of a notebook, pipeline, semantic model or report (which is where the lineage comes from) needs more: Microsoft documents that `getDefinition` requires read **and write** permission on the item, so the code's remediation says to make it **Contributor** (or higher). OLCF only issues read operations, but the role it needs grants write. If that is not acceptable, leave it at Viewer: you get the inventory and the scanner's lineage, and a `permission_denied` gap for every definition it could not read.
* OneLake: the metadata identity needs OneLake read access to the lakehouse to read the Delta log. Without it the table is listed without columns and a `permission_denied` gap says so.

### 5. A separate identity for sampling (optional)

Repeat 1 to 4 for a second application, give it only what it needs to read the data of the lakehouses you want to sample (a OneLake data access role or the Viewer role), and pass it as `--sampling-client-id` with its secret in `OLCF_FABRIC_SAMPLING_CLIENT_SECRET`. Without it, **no Fabric table can be sampled, whatever a sampling policy says.**

### 6. Add the source and test connectivity

The secret never goes on a command line: it comes from an environment variable (default `OLCF_FABRIC_CLIENT_SECRET`), from standard input (`--secret-stdin`), from a certificate (`--private-key-file`, `--certificate-file`) or from a hidden prompt, and is stored encrypted in the platform secret store and never returned.

```sh
export OLCF_FABRIC_CLIENT_SECRET=...        # the metadata identity's secret
olcf intelligence sources add contoso --kind fabric --tenant-id <tenant-guid> --client-id <app-guid> --test-only
```

`--test-only` runs the connectivity test for this configuration and exits without saving anything. Leave it out to save the source (add `--fabric-workspace GUID` to limit the scan to some workspaces, `--item-type` to limit item types, `--incremental-every` and `--full-every` to set the schedule). The test has eight steps and stops at the first failure. (The captures below use the loopback endpoints of the sample tenant through `--config network=...`; the platform refuses loopback and private endpoints unless an administrator has switched on `intelligence.allow_private_endpoints`, so on the capture server that setting was `true`. Without it, the same command is refused before any request is sent: see the end of this section.) What a good run looks like against the sample tenant:

```text
$ OLCF_FABRIC_CLIENT_SECRET=... olcf intelligence sources add contoso --kind fabric --tenant-id <tenant-guid> --client-id <app-guid> --config network=<endpoints> --test-only
1. [ ok ] resolve credentials - service principal eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee in tenant 11111111-1111-4111-8111-111111111111 (client secret; value not shown)
2. [ ok ] acquire Fabric token (56 ms) - token acquired {"tid": "11111111-1111-4111-8111-111111111111", "appid": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee", "azp": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee", "aud": "https://api.fabric.microsoft.com", "roles": ["Task.Execute"]}
3. [ ok ] list workspaces (38.7 ms) - 2 workspace(s) visible
4. [ ok ] acquire Power BI token (41.2 ms) - token acquired for the Power BI API
5. [ ok ] scanner permission probe (61.8 ms) - the admin metadata API answers (1 workspace(s) modified in the last hour); getInfo/scanStatus/scanResult use the same permission
6. [ ok ] item read probe (38.6 ms) - 3 item(s) in workspace 'Sales'
7. [ ok ] definition read probe (217.9 ms) - definition of Notebook 'nb_load' readable
8. [skip] sampling identity probe - no sampling identity configured; data sampling of Fabric tables stays off

connectivity OK
```

And a failure (here a wrong secret). Every failing step ends with a `fix:` line; the rest are skipped:

```text
$ OLCF_FABRIC_CLIENT_SECRET=... olcf intelligence sources add contoso --kind fabric --tenant-id <tenant-guid> --client-id <app-guid> --config network=<endpoints> --test-only
1. [ ok ] resolve credentials - service principal eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee in tenant 11111111-1111-4111-8111-111111111111 (client secret; value not shown)
2. [FAIL] acquire Fabric token (43.1 ms) - Entra rejected the token request for the Fabric API: Entra rejected the token request: AADSTS7000215: Invalid client secret provided.
        fix: The client secret is wrong or was copied as the secret *id*. Create a new secret and update the broker connection.
3. [----] list workspaces - skipped because an earlier step failed
4. [----] acquire Power BI token - skipped because an earlier step failed
5. [----] scanner permission probe - skipped because an earlier step failed
6. [----] item read probe - skipped because an earlier step failed
7. [----] definition read probe - skipped because an earlier step failed
8. [----] sampling identity probe - skipped because an earlier step failed

connectivity FAILED at step: acquire Fabric token
```

With the fault fixed, save the source (this form also adds the separate sampling identity; its secret comes from `OLCF_FABRIC_SAMPLING_CLIENT_SECRET`):

```text
$ olcf intelligence sources add contoso --kind fabric --tenant-id <tenant-guid> --client-id <app-guid> --sampling-client-id <sampling-app-guid> --incremental-every 900 --full-every 86400
OK: created fabric source contoso (src_01M4JC5XNYC210XB0YEMKCAC9N)
next: `olcf intelligence sources test contoso` to check each connectivity step, then `olcf intelligence sources scan contoso --mode full --wait`
```

What each step proves: 1 the credentials resolve; 2 Entra issues a Fabric token (and the claims are shown, never the token); 3 the principal sees at least one workspace (if it sees none, the tenant setting or the group membership is the usual cause); 4 a Power BI token; 5 the admin API answers (this is the *tenant setting for read-only admin APIs*); 6 items can be listed in the first workspace; 7 one definition is readable (this is the *Contributor* requirement); 8 the sampling identity can list a lakehouse in OneLake (it does not read data).

With the source saved, `olcf intelligence sources test contoso` repeats the test at any time (a good run now also passes step 8), and the console's **Intelligence, Discovery, Sources** has a *Test connectivity* button.

### 7. Scan, and read the result

```text
$ olcf intelligence sources scan contoso --mode full --wait
scan:     scn_01M4JC5YPTCJ0Y0K407JGXM9NP
source:   src_01M4JC5XNYC210XB0YEMKCAC9N
mode:     full
trigger:  api
status:   succeeded
version:  1
started:  2026-10-10T07:42:00.410613Z
finished: 2026-10-10T07:42:06.830863Z
error:    -
assets:   26 created, 4 updated, 0 unchanged
edges:    7 created, 0 updated, 0 unchanged
gaps:     2 (not_inspected 1, unsupported_construct 1)

==> scan scn_01M4JC5YPTCJ0Y0K407JGXM9NP started (full)
```

```text
$ olcf intelligence coverage
SOURCE   KIND    ENABLED  ASSETS  BY TYPE                 OPEN GAPS  GAP KINDS               STATUS     LAST FULL               NEXT DUE
local    local   yes      0       none                    0          none                    succeeded  2026-10-10T07:41:47.8…  2026-10-10T07:56:47.8…
contoso  fabric  yes      26      column 9, table 3, ex…  2          not_inspected 1, unsu…  succeeded  2026-10-10T07:42:00.4…  2026-10-10T07:57:00.4…

sampling: off - sampling is off unless an administrator creates a policy with max_rows > 0
connectors: catalog_mirror, fabric, local, scanner_import
```

```text
$ olcf intelligence gaps
KIND                   SUBJECT       DETAIL                                         FIX                                             SOURCE   LAST SEEN
not_inspected          Sales model   DAX and Power Query expressions were not (al…  Enable datasetExpressions in the metadata sca…  contoso  3s ago
unsupported_construct  orders_clean  the columns of the DataFrame written to 'ord…  Provide the source table schemas (run discove…  contoso  8s ago
```

This is the sample tenant, and it shows what a first scan looks like: assets and edges, and gaps that say exactly what was not read and how to fix it (here, the scanner result carried no DAX or Power Query expressions for the model, and a notebook writes a DataFrame whose columns the analyzer cannot know statically, so only table-level lineage exists for it). The lineage of a Fabric table works like any other:

```text
$ olcf intelligence lineage orders_clean
orders_clean (table)  Sales / lh_sales / orders_clean
├── upstream (flows into it)
│   └── <- nb_load (notebook)  [derived]
│       └── <- orders (table)  [derived]
└── downstream (it flows into): none found
lineage is incomplete: 1 gap(s) (unsupported_construct x1); see `olcf intelligence gaps`
```

With the setting off (the default), the same command is refused before a token is requested, so the credential never leaves:

```text
$ OLCF_FABRIC_CLIENT_SECRET=... olcf intelligence sources add contoso --kind fabric --tenant-id <tenant-guid> --client-id <app-guid> --config network=<loopback endpoints> --test-only
error: 422 validation_failed: config.network.authority_host must be an https URL: the credential would travel in clear text hint: use a public https endpoint, or (for a private-link gateway you control) an administrator can set the runtime setting intelligence.allow_private_endpoints to true (correlation id ddaf0453c9e34464b28f83d85fbf7450)
hint:  use a public https endpoint, or (for a private-link gateway you control) an administrator can set the runtime setting intelligence.allow_private_endpoints to true
```

### Limits of the scanner

* The Power BI admin metadata scan takes **at most 100 workspaces per request** (`intelligence.fabric_scanner_batch_size`) and Microsoft documents **500 requests per hour** (`intelligence.fabric_getinfo_requests_per_hour`) and 16 concurrent scans. OLCF budgets the hour; on a `429` it waits `Retry-After`, but never longer than `intelligence.fabric_max_wait_seconds` (120): beyond that it records an `api_unavailable` gap and ends the scanner phase, and the scan ends `partial`. A first scan of a large tenant can therefore take several hours.
* **Incremental scans** ask Power BI which workspaces changed (*Get Modified Workspaces*) and revisit only those, workspaces never seen, and **workspaces whose earlier read failed** (see below). Changes the modified-workspaces list does not report (table drift inside a workspace it considers unmodified) are found by the next **full** scan; this is documented behaviour of this design, not hidden.
* **`modifiedSince` stays inside the documented window** (30 minutes to 30 days before now). The checkpoint watermark is kept 35 minutes behind the scan start and the value sent is `min(watermark - 2 minutes, now - 35 minutes)`, so scans every minute or every hour all send a valid value and a change the service reports late is still found. If the service answers 400 (the two clocks disagree) the question is repeated once, an hour back. A watermark older than about 30 days cannot be asked about: that one scan reads everything (an informational gap) and the watermark is fresh again afterwards. The endpoint is documented at 30 requests per hour; OLCF keeps a request budget in the database (`intelligence.fabric_modified_requests_per_hour`, default 30) shared by every source of a tenant and every replica. When it is spent, or the API fails temporarily, the scan still reads new and failed workspaces but leaves the watermark where it was, so the next scan asks from the same point and nothing is skipped. A refusal (401/403) still makes the scan read every workspace, with a gap that names the tenant setting.
* **A workspace that could not be read is not forgotten.** Permission, connectivity, throttling, a failed metadata-scanner phase or a cut-off listing put the workspace into a durable retry set stored with the source checkpoint (it survives restarts). Every later incremental scan adds the due members to its selection even when the modified list is empty. Retries back off from 5 minutes, doubling per consecutive failure up to 6 hours, and are never abandoned until the workspace is read or no longer listed.
* **Cut-off listings are incomplete, not empty.** If a paged listing (workspaces, items, tables, OneLake) is still continuing when the page limit is reached, or the service repeats a continuation token, the connector keeps what it received, records a blocking `api_unavailable` gap, ends the scan `partial` (so nothing is swept) and retries the workspace. Response bodies are read as bounded streams (OneLake log files are capped, an excessive or false `Content-Length` and compressed bombs are refused, JSON bodies have a size limit).
* A schema-enabled lakehouse has no *Tables* API: the connector lists `Tables/` in OneLake instead and records an `api_unavailable` gap that says so. Columns always come from the Delta log: only `metaData` and `protocol` actions are read; per-file statistics are skipped unread.
* A workspace the principal cannot read gives a `permission_denied` gap for that workspace only; the scan continues and ends `partial`. A `partial` scan never removes assets.
* Lineage that exists only inside Fabric's own engine (Fabric Spark runs, dataflows, pipelines run by Microsoft) is never **verified**: only the stored definitions and the scanner's relationships are available, so those edges are `derived`.
* The exception is work that Fabric hands to OLCF compute (a Fabric notebook calling `run_remote`): the control plane copies the Entra tenant, the Fabric workspace and notebook GUIDs from the job's origin, and the GUIDs of the Fabric lakehouse an OLCF lakehouse is attached to, into the run-time events, so the verified edges attach to the notebook and tables the tenant scan discovered (in either order: scan first, or run first and the scan promotes the placeholders). With only the notebook's name the match is by name and the edges are `inferred`.
* A source records how its last scan ended on its tenant asset (`last_scan_status`, `last_scanned_at` in the asset's attributes), so a tenant whose scan failed after the first batch is recognisable as such.
* A OneLake URL can name a table by GUID or by friendly name; the two spellings are different strings and do not merge. A mismatch shows up as a placeholder and an `unresolved_reference` gap, not as a wrong merge.
* Network overrides for sovereign clouds, private link and proxies (`config.network`: `authority_host`, `fabric_api`, `powerbi_api`, `onelake_dfs`, `proxy`) need `integration.fabric.manage`. Each URL must be https, must not contain a user name or password and must not point at (or resolve to) a loopback, private, link-local or cloud-metadata address; otherwise the source is refused with a 422 and a remediation, and a stored source that no longer passes fails its test step or scan **before** any request is sent, so the credential never leaves. Connections resolve the name and connect to the address they checked, and redirects are not followed. For a private-link gateway or a test double an administrator can set the runtime setting `intelligence.allow_private_endpoints` to `true` (default `false`; the service principal's credential is then sent to those private endpoints; the metadata address stays blocked). Behind a proxy the proxy resolves the destination, so only a local name check is possible. See the [security model](../concepts/security-model.md#intelligence-discovery-lineage-and-sampling).

### What to check on first contact with your tenant

Tenant settings differ between organisations, so do these in order and stop at the first surprise. Report differences (the connector's expectations are listed in [Fabric assumptions](fabric-assumptions.md)).

1. `olcf intelligence sources add ... --test-only` with a tenant that has **one small workspace** and the least privilege you plan to grant. Compare each step's message with the above; the `fix:` text is Microsoft's documentation, not a tested remedy.
2. Check the names of the tenant settings against your admin portal. If one is named differently, note it: the guide and the code's remediation text both need correcting.
3. Run a **full scan limited to that workspace** (`olcf intelligence sources scan contoso --mode full --workspace-id GUID --wait`). A limited scan never removes anything.
4. Open **Intelligence, Discovery, Gaps** and read every gap before you trust the graph. Expect `permission_denied` (workspaces or definitions), `api_unavailable` (schema-enabled lakehouses, throttling) and `not_inspected` (scanner parameters the tenant did not honour).
5. Compare three things you know by heart (a lakehouse table's columns, a notebook's reads and writes, a report's model) with the asset pages. Check that the *same* table found through Fabric and through OLCF is one asset (an `onelake_path` alias match), not two.
6. Look at how long the scanner phase took, and at the `429` handling, before widening the scope.
7. Only then widen to more workspaces, and set the schedule.
8. Leave sampling off until the metadata side is understood.

## Importing a saved scanner result

If your organisation cannot give a service principal the admin API, or runs the Power BI scan elsewhere (PowerShell, a scheduled job), save the JSON body of `GET /v1.0/myorg/admin/workspaces/scanResult/{scanId}` and import it. Intelligence handles it like any other source: the same gaps, the same lineage analyzers, the same removal sweep on full scans.

```text
$ olcf intelligence import scanner scanresult.json --name 'Finance tenant (saved scan)' --wait
OK: imported 1 workspace(s), 0 data source instance(s) into Finance tenant (saved scan) (src_01M4JC68QFEPYRCAQJY9GWJ641); scan scn_01M4JC68QND58JPETDE331JQ1G succeeded
```

* The file is validated (a JSON object whose `workspaces` array holds objects with an `id`, within size and workspace limits) and **refused if it appears to contain a credential** (a connection string with a password, a client secret, a bearer token). The check is a safety net; do not put secrets in the file.
* Importing again under the same `--name` replaces the stored document and rescans it. `--no-scan` stores it without scanning; `--workspace` puts the assets under an OLCF workspace so that users with access to it can see them (without it they belong to no workspace and need an organisation-wide `intelligence.read`).
* What you get depends on what the scan contained: the lineage of reports to models to upstream models and data sources (`lineage=True`), tables and columns (`datasetSchema=True`), expressions (`datasetExpressions=True`). Anything missing becomes a `not_inspected` gap naming the parameter to switch on.
* It follows the documented shape of the result.

## Sampling policies

Intelligence reads **metadata**. It reads **values** only when you ask for a sample, and only if a policy allows it.

* **Default off.** With no policy, or `max_rows 0`, nothing is ever sampled. Holding the permission `intelligence.sample` is not enough on its own; a policy must also allow it.
* A policy has a **scope**: `global`, `workspace`, `source` or `asset`. The narrowest scope that has a policy wins, so `--max-rows 0` on an asset switches sampling off for that asset even where its workspace allows it.
* **The table decides.** Policies are resolved for the *parent table*: a column asset inherits its table's policy (row limit, masking, exclusions) and can never be sampled where the table cannot. A policy on a column asset can only narrow what the table allows (switch it off, fewer rows, stronger masking). Asking for one column returns exactly that column, whether or not the scan recorded the table's schema; a column the table does not have is an error.
* **Automatic value profiling uses the same rules.** `value_profiling` on a local source asks the same data-read policy service as the sample endpoint: the table's own policy applies to a rescanned table, approved governance classifications are left out, a failing classification lookup stops the read (fail closed), and when the answer is "no" no data is read at all. Only aggregates are stored; each authorised read is recorded as `intelligence.sample_read` with `purpose` `value_profiling` (column names and row count, never a value).
* It limits **rows** (0 to 100, whatever a caller asks for), applies **masking** to every returned cell (`none`; `hash`, salted and stable inside the installation so equal values stay equal; `redact`; `partial`, the first and last characters, the year of a date, the leading digit of a number) and, by default, **leaves out classified columns** and columns whose names or types look sensitive (`--include-classified` turns that off).
* **Nothing is stored.** A sample is read, masked, clipped to a fixed cell length and returned. It is not cached, logged or audited as data. The audit record `intelligence.sample_read` holds the asset, the row count, the column *names*, the masking and the policy scope.
* Local Delta tables are read without Spark or a worker. **Fabric tables** need the source's separate sampling identity.
* The sample endpoint refuses an MCP token and there is no MCP tool for it: an AI agent cannot obtain sample rows through Intelligence.

With no policy:

```text
$ olcf intelligence assets sample dim_customer --rows 3
error: 403 policy_denied: sampling is disabled for 'dim_customer': no sampling policy exists hint: An administrator can allow sampling with PUT /v1/intelligence/sampling-policies (max_rows 1..100, masking, scope); the default is off. (correlation id 5207b8410bde49709beeb6b59d8cbe11)
hint:  An administrator can allow sampling with PUT /v1/intelligence/sampling-policies (max_rows 1..100, masking, scope); the default is off.

$ olcf intelligence sampling-policy list
no sampling policies: sampling is off everywhere
```

Allow a small masked sample for one workspace, then ask again:

```text
$ olcf intelligence sampling-policy set --scope workspace --scope-id 'Sample Retail' --max-rows 5 --masking partial
OK: workspace sampling policy smp_01M4JC4WMQWNAZVNENA9HFMXEM: up to 5 row(s), masking partial, classified columns excluded

$ olcf intelligence assets sample dim_customer --rows 3
customer_id  city     state  country  segment      signup_date  loyalty_tier  is_active
1*****       D*****t  **     **       H*********e  2018-**-**   S****r        ***
1*****       C*****o  **     **       H*********e  2020-**-**   ****          ***
1*****       D****s   **     **       H*********e  2021-**-**   B****e        ***
3 row(s), masking partial, policy scope workspace:wks_01M4JC3NQTTSEWY2QNX99VT43J, excluded columns: email, first_name, last_name
```

Notice what is missing: the sensitive-looking columns of `dim_customer` are not in the sample at all (`--include-classified` would add them), and the others are masked. Manage policies in the console under **Intelligence, Discovery, Sampling** (visible with `intelligence.manage`), or with `olcf intelligence sampling-policy list|set|delete`. `intelligence.manage` is needed to change a policy, `intelligence.sample` to read a sample (Platform and Workspace administrators).

*Value profiling* of local tables (null counts, min and max; `--value-profiling` on a local source) also needs a policy, and the min and max of text, binary and sensitive-looking columns are never stored.

## Governance workflows

Governance sits on the same assets and is **observe-only**: it records and reports, it never blocks a job, a query or a pipeline. A policy with `--mode enforce` is refused. The permissions: `governance.curate` to describe, tag, classify, assign owners and propose terms (Contributors and up); `governance.approve` to approve change requests, certify and manage policies (Workspace, Security and Platform administrators).

```text
$ olcf governance summary
glossary terms:         12  (approved 12)
domains:                1
data products:          1  (published 1)
classifications:        9 labels; assignments approved 3, suggested 0, rejected 0
review queue:           10 pending  (classifications 0, term links 10); 0 change request(s)
open violations:        0  (none)
certification:          draft 0, certified 3, deprecated 0
coverage of datasets:   12 datasets: owner 25.0%, description 100.0%, domain 100.0%, term 25.0%, classification reviewed 8.3%, certified 25.0%
last policy evaluation: never

[... 1 more lines]
```

**Classify sensitive data.** (1) A scan produces suggestions from column names and types; they enter the **review queue** and are never approved automatically (`governance.auto_classify_on_scan`, `governance.suggestion_min_confidence`). (2) A reviewer approves or rejects them: `olcf governance review-queue approve classification:1 --comment ok`; or labels an asset directly: `olcf governance classifications assign lakehouse_a.raw.email PII`. With `governance.classification_four_eyes` on, a curator's label waits in the queue and nobody approves their own proposal (`governance.allow_self_approval` relaxes that). (3) Approved labels propagate along lineage (depth `governance.propagation_depth`, 6; minimum confidence `governance.propagation_min_confidence`, `derived`) and the exposure report shows where they end up. (4) Console: **Governance, Classifications** and **Review queue**.

**Describe and organise.** The glossary (`olcf governance terms create 'Net revenue' --definition 'Revenue after returns' --abbreviation NR --synonym turnover`, then `olcf governance terms link 'Net revenue' lakehouse_a.mart`; the scan proposes links for assets whose name equals a term, synonym or abbreviation), domains (`olcf governance domains create Finance`, `olcf governance domains assign lakehouse_a.mart Finance`), owners, stewards and custodians (`olcf governance owners add asset lakehouse_a.clean --role owner --user alice`; inherited down the asset tree, so an owner on a lakehouse owns its tables) and data products (`olcf governance products create 'Revenue mart' --domain Finance --member lakehouse_a.mart --freshness-hours 24`; a new product is a draft and publishing needs `governance.approve`).

**Certify.** `olcf governance certify lakehouse_a.mart --comment 'reviewed with finance'` sets *certified* (needs `governance.approve`); `--state deprecated` marks the opposite. A schema change on a certified asset drops it back to *draft* (event `governance.certification.changed`). Someone without `governance.approve` opens a **change request** instead: `olcf governance requests create certify lakehouse_a.mart --comment 'please certify'`; a reviewer applies it with `olcf governance requests approve ID`. Requests also carry classification changes, term links, ownership changes and policy exceptions.

**Policies and violations.** Seven rule types exist (`olcf governance policies rule-types` lists them with their parameters): `classification_flow` (labelled data must not reach matching assets), `required_owner`, `required_description`, `required_classification` (columns whose names match patterns must carry a label), `certified_sources_only` (reports must sit on certified sources), `freshness` and `quality_min`.

```sh
olcf governance policies create 'Tables have owners' --rule-type required_owner --param types='["table"]' --severity high
olcf governance policies evaluate
olcf governance violations list --severity high
olcf governance violations acknowledge VIOLATION_ID --comment 'known'
```

Policies are evaluated after every scan (`governance.evaluate_policies_on_scan`) and on demand; evaluating is idempotent (a violation that is still true keeps its id). A violation is a record and an event (`governance.violation.opened`), not a block; if you need enforcement today it must be built from those events and webhooks outside the platform.

**Quality.** `olcf governance quality lakehouse_a.clean` gives a 0 to 100 score with the points lost per component (documentation, ownership, classification, freshness, certification, lineage, violations, schema stability) and why; the weights are the `governance.quality_weight.*` settings. It is a hygiene indicator for metadata, not a measure of the data's correctness.

## Schedules

The scheduler runs on the **leader** replica only. A source's schedule is two intervals, incremental and full, in seconds (0 means never; 60 to 2,592,000 when set), with defaults per kind:

| Kind | Incremental | Full |
|---|---|---|
| `local`, `catalog_mirror` | `intelligence.local_scan_interval_seconds` (900) | `intelligence.local_full_scan_interval_seconds` (86,400) |
| `fabric` | `intelligence.fabric_incremental_interval_seconds` (3,600) | `intelligence.fabric_full_scan_interval_seconds` (86,400) |
| `scanner_import` | never (the document only changes when you import) | never |

Override them per source with `--incremental-every` and `--full-every` (`olcf intelligence sources add`; the API's source update changes them later). A source that never completed a full scan gets one first; a full scan that is due wins over an incremental one; a source whose last scan failed is retried after the shorter of its interval and five minutes; disabled sources and sources with a live scan are skipped. With `intelligence.scan_on_start` (default on) the first leader tick after start-up runs an incremental scan of every enabled source. `intelligence.max_concurrent_scans` (2) limits scans running at the same time in one process. The scheduler does not run in the `test` environment.

## Who can see and do what

Permissions are enforced on the server; the console only hides what you cannot use. The full matrix is in [permissions](../reference/permissions.md).

| Action | Permission | Platform admin | Security admin | Workspace admin | Contributor | Viewer | Service principal | Operator, Runtime admin |
|---|---|---|---|---|---|---|---|---|
| Browse assets, lineage, impact, documentation, coverage, gaps, snapshots | `intelligence.read` | yes | yes | yes | yes | yes | yes | no |
| Add, change, delete or scan a source; import a scanner result; set sampling policies | `intelligence.manage` | yes | yes | yes | no | no | no | no |
| Read sample rows | `intelligence.sample` (and a policy) | yes | no | yes | no | no | no | no |
| Describe, tag, classify, assign owners, propose terms | `governance.curate` | yes | no | yes | yes | no | no | no |
| Approve requests, certify, manage policies | `governance.approve` | yes | yes | yes | no | no | no | no |
| Change network endpoints of a Fabric source | `integration.fabric.manage` | yes | no | no | no | no | no | no |
| Use the `intel_*` and `governance_*` MCP tools | `intelligence.read` and `mcp.use` through a token of scope `mcp` | yes | no | yes | yes | no (no `mcp.use`) | yes | no |

What you can *see* is narrower than what the permission suggests, because lineage is an information-disclosure channel:

* A read is filtered to the workspaces you can read. An asset in a workspace you cannot read **does not exist for you**: `404`, not `403`. A lineage walk stops at the boundary without a placeholder, so you cannot count hidden neighbours; evidence that passes through a hidden asset is shown as hidden. Your lineage and impact answers are therefore *shorter* than an administrator's, and cannot tell you that something was left out.
* Assets with no workspace (a tenant, an external system, an imported scanner result without `--workspace`) need an **organisation-wide** `intelligence.read`.
* A source's configuration (tenant and client ids, endpoints, schedule) is shown only to an organisation-wide `intelligence.manage`. Credentials are never shown to anyone.
* A token of scope `mcp` can read but never write or sample, even when its owner is an administrator.

## Retention

| What | How long | Setting |
|---|---|---|
| Processed, skipped and failed rows of the event inbox (rows still pending after twice as long are dropped too) | 30 days | `intelligence.retention_days` |
| Run-time evidence on an edge; after it, the edge falls back to `derived`, and the newest observation is kept as history | 90 days after last seen | `intelligence.verified_evidence_days` |
| Resolved policy violations | 365 days | `governance.violation_retention_days` |
| MCP call audit rows | 90 days | `mcp.calls_retention_days` |
| Audit records (including `intelligence.*` and `governance.*` actions) | 730 days | `retention.audit_days` |
| Assets, aliases, edges, gaps, scans/snapshots, glossary, owners, classifications | kept; removal marks status `removed`, nothing is deleted | none |
| Attribute versions of an asset (history) | the newest 200 per asset are kept; older ones are trimmed by the hourly housekeeping | `intelligence.max_versions_per_asset` |
| A `failed` inbox row | queued once more after 30 minutes (0 = never); if it fails again it stays `failed` until an operator re-queues it, and is purged with the other rows | `intelligence.inbox_retry_after_minutes` |

Evidence on an edge is capped at 20 records, redacted, and each snippet at 400 characters. Sample values are never stored at all.

## Operating notes

* **Inbox lag.** Compute, SQL and pipelines publish `dataset.*` events and Intelligence applies them from a durable inbox (poll interval `intelligence.inbox_poll_seconds`, 2; batch `intelligence.inbox_batch_size`, 200). Between a run and the appearance of its verified edge there is therefore a short delay; `olcf intelligence summary` shows `event inbox: N pending, M failed`. Above `intelligence.inbox_lag_warn` (5,000) pending rows the health check turns `degraded`.
* **Failed inbox rows.** A row that could not be applied three times is `failed`. The hourly housekeeping queues it once more after `intelligence.inbox_retry_after_minutes` (30; 0 turns this off); after that it stays `failed` until you decide. To re-queue every failed row at once, with a fresh set of attempts (needs `intelligence.manage`; the console has no button for it):

```text
$ olcf intelligence inbox requeue
OK: 0 failed event(s) queued again
pending: 0
done:    0
failed:  0
skipped: 16
```

The same is `POST /v1/intelligence/inbox/requeue`, `client.intelligence.requeue_inbox()` in the SDK, and it is audited as `intelligence.inbox_requeued` with the number of rows. `olcf intelligence summary` shows how many are pending and failed; failed rows are purged after `intelligence.retention_days`, so look at them before then.
* **Health checks.** `olcf health` includes `intelligence` (inbox depth and lag, last scans, open gaps; the deep check does a round trip), `intelligence-discovery` (sources, schedule, last scans, sampling state) and `intelligence-governance`. See [health checks](../reference/health-checks.md).
* **Best effort by design.** If Intelligence is off, broken or slow, jobs, SQL and pipelines behave exactly as before; the price is that an event can be lost and a verified edge missing. `intelligence.enabled = false` stops collection but the read routes stay available.
* **Scan conflicts.** A second scan request for a source with a running scan gets `409` (`conflict`) naming the running scan. A scan whose process died is closed as `failed` when its lease (`intelligence.scan_lease_seconds`, 120) expires and another process takes over.
* **Leader only.** The inbox processor, the scheduler and the governance loop run on the leader. After a failover they resume; nothing is lost because events wait in the inbox.
* **Limits.** A lineage answer is cut at `intelligence.max_lineage_nodes` (500) dataset-like nodes and says `truncated`; the notebooks, pipelines and other process nodes between datasets have a wider allowance of their own (at least 2,000), so one table read by thousands of notebooks does not use up the budget meant for tables, and the drawn graph prefers datasets. The impact analysis counts with its own traversal bounded by `intelligence.max_impact_nodes` (20,000): its totals are real numbers up to that bound, `truncated` is set only when the bound cut it short, and the CLI then says the totals are a lower bound (lower `--depth`); the lists in the answer stay within `max_lineage_nodes`. `origin` lists at most 100 paths. Governance evaluations look at no more than `governance.max_evaluation_assets` (20,000). These are caps, not performance guarantees: the largest estate tried was 2,000 tables and 20,000 columns on SQLite in a system test.
* **Events and audit.** `intelligence.scan.finished` and the `governance.*` events can feed webhooks and alerts ([events](../reference/events.md)); every source, scan, asset edit and sample is audited.
* **Back up** with the platform backup ([backup and restore](backup-and-restore.md)): the graph is in the platform database. Everything in it can also be rebuilt by scanning, except governance decisions (labels, terms, owners, certifications) and run-time evidence, which cannot.

## What happens when code runs

Static analysis shows what code *says*. Run-time evidence shows what it *did*. Jobs and notebook kernels run a small probe that reports the tables and paths that PySpark and host-Python (delta-rs, DuckDB) really read and wrote; the SQL service reports every statement; pipelines report their copy activities. Those events turn derived edges into **verified** ones and add edges for names no static analyzer could resolve. The probe is limited (no Spark Connect, streaming, JDBC, pandas readers; at most 200 events per job); the list is in the [support matrix](../reference/lineage-support.md#run-time-events-verified). Because the runner executes user code, everything it reports is treated as untrusted input and is validated and clipped by the control plane. Turn the probe off with `OLCF_DATASET_EVENTS=0` in the job's environment.

## Troubleshooting

Start with `olcf intelligence gaps` (or **Discovery, Gaps**): every gap has a kind, the subject, what could not be done, and a `fix`. The general tools are in [troubleshooting](troubleshooting.md).

| Symptom | Likely cause | What to do |
|---|---|---|
| Lineage ends at a table, or the tree is empty | The code that reads or writes it was never scanned (a notebook outside the scanned workspace or source), or the scan ended `partial`/`failed`, or the table name is built at run time | `olcf intelligence coverage`, then `olcf intelligence scans list --status partial`. Scan the missing source. Look for `dynamic_reference` gaps on the notebook: use a literal table name or a notebook parameter with a default value |
| "lineage is incomplete: N gap(s)" under an answer | There are open gaps touching it. This is a statement of honesty, not an error | `olcf intelligence gaps --asset ASSET` and follow each `fix`; informational gaps (for example `sampling_disabled`) do not hide anything |
| `permission_denied` gap | The identity a connector uses cannot read that workspace, item, definition or OneLake path | For Fabric: add the service principal to the workspace (Viewer; Contributor for definitions), check the tenant settings, wait up to 15 minutes, then `olcf intelligence sources test SOURCE` and rescan. A `partial` scan changes nothing in the graph; the gap clears when a later scan reads it |
| `permission_denied` from `olcf` itself (`403`) | Your user lacks `intelligence.read`, `intelligence.manage`, `intelligence.sample`, `governance.curate` or `governance.approve` | Ask for the role; see [who can see and do what](#who-can-see-and-do-what). For MCP, the token also needs the `mcp` scope |
| `404 not_found` for an asset you know exists | It is in a workspace you cannot read (a hidden asset answers exactly like a missing one), or the name matched several assets and none exactly, or it was removed | Use the `ast_` id from `olcf intelligence search`, add `--type`, ask an administrator to check your workspace access. `--status any` on `search` includes removed assets |
| `unresolved_reference` gaps and `[unresolved]` placeholders | Code mentions a table or file no scan has found (not created yet, in another source, written under another spelling, a different OneLake URL form) | Scan the source that holds it; if the name is qualified differently, qualify it the same way in the code. A placeholder merges into the real asset on the next scan that finds it |
| `unresolved_reference` for a column | The analyzer could not tell which table an unqualified column belongs to (two tables of unknown schema) | Qualify the column (`t.col`), or scan so that schemas are known |
| `unsupported_construct`: "SELECT * without known schema", pivot, `%%scala`, stored procedure, UDF body | Static analysis cannot see through it | Table-level lineage is kept. Select columns explicitly, or let the code run once: run-time evidence adds verified table edges. See the [support matrix](../reference/lineage-support.md) |
| `parse_error` gap | The text is not valid for the analyzer (a syntax error, or a dialect it does not read) | Fix the statement, or accept the gap: the other statements in the script are still analysed |
| `not_inspected` gap | The definition is stored elsewhere (a Spark job definition that only names a file), or the scanner did not return a part | Inline the code, or switch on the named scanner parameter |
| `api_unavailable` gap | An API answered `404`/`501`, a lakehouse has schemas enabled (no Tables API; OneLake is listed instead), or throttling did not clear within `intelligence.fabric_max_wait_seconds` | Rescan later; for throttling, raise `intelligence.fabric_max_wait_seconds` or scan fewer workspaces at a time |
| A table is listed with no columns | The Delta log could not be read (no OneLake access for the metadata identity, or an empty table) | Check the `permission_denied` gap and the OneLake tenant setting |
| A table you deleted is still shown | Only a **full** scan that **succeeded** marks vanished assets `removed` | `olcf intelligence sources scan SOURCE --mode full --wait` and check that it ended `succeeded` |
| No `verified` edges at all | Nothing has run since the module was enabled, the job's probe is off (`OLCF_DATASET_EVENTS=0`), or the inbox is behind | `olcf intelligence summary` (inbox pending/failed), `olcf health`; run a notebook or an SQL statement against the table and look again |
| A job ran but its lineage names a `spark_job` and not the notebook | A job submitted without an item has no notebook to name, so it is recorded as a job-level process (`olcf-job:<id>`). A run submitted from a Fabric notebook is different: it is tied to the notebook the tenant scan discovered when its events carry the tenant, workspace and notebook GUIDs (`verified` edges), or matched by name inside the mapped OLCF workspace (`inferred` edges). If you see two processes, the Fabric source has not been scanned yet (the run created a placeholder, which the next scan promotes), or the run's lakehouse is not attached to a Fabric lakehouse in its definition. | Scan the Fabric source; check the lakehouse's Fabric mapping |
| `event inbox: N pending` keeps growing; health `degraded` | The processor is behind, or this replica is not the leader and the leader is down | `olcf health`; check the leader; `intelligence.inbox_batch_size`; a `failed` row is retried once automatically after `intelligence.inbox_retry_after_minutes`; `olcf intelligence inbox requeue` queues all failed rows again; failed rows are deleted after `intelligence.retention_days`, so look at them before then |
| `409 conflict` when scanning | A scan of that source is running | Wait, or `olcf intelligence scans cancel SCAN_ID` |
| `sources test` fails at step 2 | Wrong tenant, client id or secret (a secret *id* instead of the secret *value*), or an expired secret | Create a new secret and update the source |
| `sources test` fails at step 3 or 5 | The tenant settings are off, the group does not contain the principal, or the change has not propagated | Enable the settings for the group, wait up to 15 minutes |
| `sources test` fails at step 7 | The principal is a Viewer but `getDefinition` needs write permission on the item | Make it a Contributor of the workspace, or accept the gaps |
| Sampling answers "not allowed" or refuses | No policy, `max_rows 0`, you lack `intelligence.sample`, the column is classified, or (Fabric) no sampling identity is configured | `olcf intelligence sampling-policy list`; see [sampling policies](#sampling-policies) |
| Exposure of a label is empty | Nothing approved carries that label (suggestions do not count), or the lineage from the labelled column is missing or below `governance.propagation_min_confidence` | Approve the suggestions in the review queue; read the gaps along the path |
| `422 unsupported` creating a policy | `--mode enforce` is not supported | Use observe mode |
| A saved scanner result is refused | It looks like it contains a credential, or is not a scanResult document | Remove the credential and export again; check that it has a `workspaces` array |

## How the outputs in this guide were produced

A fresh installation (`olcf init`, `olcf serve --no-embedded-worker`, `olcf sample install`) was used for every output outside the Fabric sections; the Fabric sections ran a second fresh installation against the sample Fabric tenant of the test suite on a loopback port. The commands are exactly the ones shown, with the data directory path shortened and one token replaced. No output was edited except where the text says *abridged*.

## Where next

[Concepts](../concepts/intelligence.md) · [Lineage support matrix](../reference/lineage-support.md) · [On-prem, Fabric, or both](../concepts/intelligence-fabric-and-onprem.md) · [Connect an AI agent (MCP)](mcp.md) · [Fabric assumptions](fabric-assumptions.md) · [Security model](../concepts/security-model.md#intelligence-discovery-lineage-and-sampling) · [ADR 0017](../adr/0017-intelligence-subsystem.md) · [CLI reference](../reference/cli.md) · [REST API reference](../reference/api.md) · [settings](../reference/configuration.md).
