# Pipelines, schedules and triggers

A pipeline is an item of type `pipeline`: a directed acyclic graph of typed activities with parameters, variables and an expression language. The same document can be written by hand, edited in the UI designer (**Develop, Pipelines**), imported from a Microsoft Fabric or Data Factory pipeline, and stored in Git. How runs are driven and what survives a crash is in [components and data flow](../concepts/components-and-data-flow.md#a-pipeline-run).

## A first pipeline

```json
{
  "schema": "olcf.pipeline/1",
  "description": "Row count check, then branch on the result",
  "parameters": {"min_rows": {"type": "int", "default": 1000}},
  "activities": [
    {"name": "CountSales", "type": "sql",
     "typeProperties": {"sql": "select count(*) as n from fact_sales", "engine": "duckdb", "lakehouse": "retail_lakehouse"},
     "policy": {"retries": 1, "retryBackoffSeconds": 5}},
    {"name": "Enough", "type": "if",
     "dependsOn": [{"activity": "CountSales", "conditions": ["succeeded"]}],
     "typeProperties": {
       "expression": "@greaterOrEquals(activity('CountSales').output.firstRow.n, pipeline().parameters.min_rows)",
       "ifTrueActivities": [{"name": "Pause", "type": "wait", "typeProperties": {"waitTimeInSeconds": 1}}],
       "ifFalseActivities": [{"name": "TooFew", "type": "fail",
                              "typeProperties": {"message": "fact_sales has too few rows", "errorCode": "too_few_rows"}}]}}
  ]
}
```

```bash
olcf pipeline validate nightly.json                       # every problem with its path, nothing is saved
olcf pipeline create docs_nightly --file nightly.json -w "Sample Retail"
olcf pipeline run docs_nightly -w "Sample Retail" --wait        # exit 0
olcf pipeline run docs_nightly -w "Sample Retail" -p min_rows=99999999 --wait    # run fails, exit 10
```

This definition was created and run against the sample workspace for this guide.

## Definition format

| Field | Meaning |
|---|---|
| `schema` | `olcf.pipeline/1` |
| `parameters` | typed inputs: `string`, `int`, `float`, `bool`, `array`, `object`, `secret_ref`, each with an optional `default` |
| `variables` | typed run-state values changed by `set_variable`: `string`, `int`, `float`, `bool`, `array`, `object` |
| `settings` | `concurrency`, `queuePolicy` (`queue`, `skip` or `cancel_previous`), `timeoutSeconds`, `failFast` |
| `activities` | the DAG, at most 500 activities and 6 levels of nesting |
| `outputs` | named expressions returned by the run |

Each activity has a `name` (unique), a `type`, `typeProperties` specific to the type, optional `dependsOn` (a list of `{activity, conditions}` where a condition is `succeeded`, `failed`, `skipped` or `completed`), and an optional `policy`: `retries`, `retryBackoffSeconds`, `backoffMultiplier`, `maxBackoffSeconds`, `timeoutSeconds`, `cancelGraceSeconds`, `idempotent`, `reusable`, `secureInput`, `secureOutput`. A definition is at most 5 MiB.

Validation reports every problem with a precise path (`activities[2].typeProperties.items`): structure, unknown activity types or properties, duplicate names, cycles (the cycle is printed), bad dependency conditions, expression syntax, unknown functions, references to non-upstream activities, undeclared variables, `item()` outside `for_each`, and unsafe parallel `set_variable`. The JSON Schema is served at `GET /v1/pipelines/schema`; the activity catalogue and function list at `GET /v1/pipelines/activity-types`.

### Activities

| Type | What it does |
|---|---|
| `notebook` | runs a notebook of the workspace as a job on the compute fabric |
| `spark_job` | runs a Spark job definition as a job |
| `script` | runs inline Python or shell code as a job on a worker |
| `sql` | runs a SQL statement on DuckDB or Spark; output has `rowCount`, `columns`, `firstRow`, `rows` (up to `maxRows`) |
| `copy` | copies files or a Delta table between lakehouse locations, optionally converting between csv, jsonl and parquet; resumable and idempotent (each file is written to a temporary part, verified, then moved; a manifest records finished files) |
| `http` | calls an HTTP(S) endpoint through the SSRF guard; output has `statusCode`, `headers`, `body`; `idempotencyKey` sends an `Idempotency-Key` header that stays the same for every attempt (see *Timeouts and uncertain outcomes*) |
| `webhook_wait` | POSTs to a URL with a `callBackUri` in the body and waits for the callback (or waits for a callback with no URL) |
| `if`, `switch`, `for_each`, `until` | control flow; `for_each` exposes `item()` and `itemIndex()`; `until` is bounded by `maxIterations` |
| `set_variable` | sets a variable or appends to an array variable |
| `wait` | pauses for `waitTimeInSeconds` (up to 7 days) |
| `fail` | fails the run with a message and error code |
| `child_pipeline` | runs another pipeline with parameters (depth limited to 8; recursion is rejected) |

Job-backed activities (`notebook`, `spark_job`, `script`) submit an ordinary job and wait for it; the job carries the pipeline run as its parent, so it is visible in **Operate, Jobs**.

### Expressions

A string value beginning with `@` is an expression and keeps its native type: `"@greater(activity('Load').output.rows, 0)"`. `"text @{expr} more"` interpolates; `"@@x"` is the literal `@x`. The language is a safe subset in the style of Data Factory expressions: literals, member access (`a.b`, null-safe `a?.b`), indexing, the usual operators, and 87 functions (case-insensitive). It cannot call Python, import, or loop without bound; evaluation is step-limited and results are size-capped. Run-state accessors: `pipeline()` (`.parameters`, `.RunId`, `.Pipeline`, `.Workspace`, `.TriggerType`, `.TriggerTime`), `parameters('x')`, `variables('v')`, `activity('A')` (`.output`, `.status`, `.error`), `item()`, `trigger()`, `triggerBody()`.

## Running and recovering

```bash
olcf pipeline runs docs_nightly -w "Sample Retail" --state failed
olcf pipeline run-status <run-id>
olcf pipeline cancel <run-id>
olcf pipeline rerun <run-id>          # same definition snapshot and parameters
olcf pipeline resume <run-id>         # continue a failed run from the failed activities
```

A run executes the definition as it was when the run started (an immutable snapshot). `resume` starts a new run in the same group that re-uses the results of the activities that succeeded (the API takes `rerun`, a list of activity names to execute again together with everything that depends on them, and changed parameters disable reuse); `rerun` starts over with the same parameters. The run graph in the UI shows each activity's state, attempts, inputs, outputs and errors; inputs and outputs of activities marked `secureInput`/`secureOutput` are hidden. At most `pipelines.max_active_runs` (500) runs execute at once; extra runs queue.

**Failure semantics.** An activity that fails is retried per its policy. After a control-plane restart, activities that are asynchronous (a job, a child run, a wait) continue from their saved state; blocking activities are repeated only if they are safe to repeat (`copy`, read-only `sql`, `http` GET, or `policy.idempotent`); all others are failed with code `interrupted` and are never silently run twice. Use `resume` after that.

### Timeouts and uncertain outcomes

A timeout never starts a retry while the timed-out attempt could still be running. When an activity exceeds `timeoutSeconds` the engine moves it to a **cancelling** phase: it is asked to stop (an `http` call is aborted on its socket, a `job` activity cancels the job and keeps polling until the job is confirmed terminal), and the retry policy only applies once the attempt has really stopped. How long the engine waits for that is `policy.cancelGraceSeconds` (default 30, `0` to 3600). The result is one of:

| Outcome | Meaning | Retried? |
|---|---|---|
| `timeout` | the attempt stopped when cancelled (or its late result was discarded) | yes, per `retries` |
| `timeout_uncertain` | the attempt did not confirm that it stopped, or it could not tell whether the other side accepted the work | only if the activity is **idempotent**; otherwise the run fails and an operator decides |

An uncertain outcome raises the event `pipeline.activity.outcome_uncertain` (see the [event reference](../reference/events.md)), increments the metric `olcf_pipeline_activity_outcome_uncertain_total` and carries an `operatorAction` in the error details. After checking the target system, `olcf pipeline resume RUN` (`POST /v1/pipeline-runs/{run_id}/resume`, permission `pipeline.run`) starts a new run in the same group that re-uses the activities that already succeeded and executes the failed one again; nothing does that automatically for non-idempotent work. (The route is `/v1/pipeline-runs/...`; older notes that write `/v1/pipelines/runs/...` are wrong.)

For the `http` activity the same rule decides each failed request. Nothing was sent (DNS failure, connection refused, connect timeout, pool timeout) and the request can always be repeated. If the request *was* sent but no answer arrived (read timeout, connection reset, the pipeline timeout) a `GET`, `HEAD` or `OPTIONS` is repeated, while a `POST`, `PUT`, `PATCH` or `DELETE` is `timeout_uncertain` or `network_uncertain` unless it is idempotent. Mark it with `policy.idempotent: true`, or set `typeProperties.idempotencyKey` (`true` generates `olcf-<activity run id>`, a string is used as is) or an `Idempotency-Key` header: the same key is sent on every attempt, which is what lets the receiving service drop the duplicate. A `408`, `425` or `429` answer means the request was not processed and is retried either way. Any `5xx` answer (a `500`, a `503`, or a `502`/`504` from a gateway; a gateway can answer `503` after the upstream committed the request) does not prove that the server did not act: it is retried for `GET`, `HEAD`, `OPTIONS` and idempotent requests, and for other requests it fails as category `http` with `details.retrySuppressed` instead of being repeated.

## Schedules

A schedule is an item of type `schedule` that binds a cron expression and time zone to a target (a pipeline, a notebook or a Spark job definition):

```bash
curl -s -H "Authorization: Bearer $OLCF_TOKEN" -H 'Content-Type: application/json' "$OLCF_URL/v1/schedules" -d '{
  "workspace_id": "wks_...", "name": "docs_nightly_2am",
  "definition": {"cron": "0 2 * * *", "timezone": "Europe/Berlin",
                 "target": {"type": "pipeline", "ref": "docs_nightly", "parameters": {"min_rows": 500}},
                 "catchup": "skip"}}'
```

| Field | Meaning |
|---|---|
| `cron`, `timezone` | five-field cron in an IANA zone (DST aware); the smallest allowed interval is `pipelines.min_schedule_interval_seconds` (60) |
| `target` | `{type, ref, parameters}`; parameters may contain expressions |
| `catchup` | what to do with fire times missed while the platform was down: `skip`, `run_once` (collapse into the latest) or `run_all`, bounded by `max_catchup_runs` (default 100) |
| `misfire_grace_seconds`, `start_at`, `end_at`, `backfill_from`, `enabled` | window and tolerance. A new schedule starts from now; history is back-filled only when `backfill_from` is set |

`POST /v1/schedules/preview` returns the next fire times without storing anything. Control a schedule with `olcf pipeline schedule enable|disable|pause|resume|run-now|next <schedule-id>`; `olcf pipeline schedules` lists them. In the UI schedules appear under the target item.

## Triggers

Other event sources are triggers (`POST /v1/triggers`, UI: the pipeline's triggers panel):

| Type | Fires when | Notes |
|---|---|---|
| `webhook` | `POST /v1/hooks/pipelines/{trigger_id}` is called | `auth: hmac` (default) or `bearer`. HMAC: send `X-OLCF-Timestamp` and `X-OLCF-Signature: sha256=<hex>` where the hex is HMAC-SHA256 of `<timestamp>.<body>` keyed with the secret shown once at creation. A timestamp more than `tolerance_seconds` (300) away is rejected, and a replayed request is recognised from the signed content. Per-trigger rate limit (`rate_limit_per_minute`, 60) and `max_body_bytes`. `payload_mapping` turns the body into parameters with `$.json.path` or `@expression` |
| `storage_event` | new or changed files appear under a lakehouse path | polls every `poll_seconds` (30), batches files and fires after a quiet period (`debounce_seconds`, 30); `pattern`, `recursive`, `mode` (`batch` or `per_file`), `max_files_per_run` (1000), `include_existing` |
| `dependency` | an upstream pipeline's run ends in a chosen state | `upstream` and `on` (`succeeded`, `failed`, `cancelled`) |

**Exactly once.** Every firing first claims `(trigger_id, fire_key)` with a unique insert, so two control-plane replicas cannot fire the same schedule time, webhook delivery or file batch twice; the started run also carries an idempotency key, so a crash between claim and start is repaired without a second run. Use the same mechanism for your own callers by sending an `Idempotency-Key`.

## Promoting between stages

A workspace can carry a stage (dev, test, prod) and variable sets resolve per stage. A deployment pipeline (`/v1/deployment-pipelines`) promotes items from one workspace to the next: items match by logical id and then by name, references to other items and connections are remapped, and `dry_run` shows the plan. See [workspaces](../concepts/workspaces-items-lakehouses.md#workspaces).

## Importing from Fabric

`olcf pipeline import-fabric <file>` (or `POST /v1/pipelines/import-fabric`, which also takes an `item_map` and can create the item) translates a Microsoft Fabric or Data Factory pipeline document into this format and prints a report: the activity mapping, warnings, unresolved references, and every unsupported activity with the reason and a remedy. The API's `on_unsupported` option (`placeholder`, `skip` or `error`) decides what happens to unsupported activities; the default, `placeholder`, keeps the DAG shape with a `fail` activity that explains what is missing, so the run stops exactly there. On the command line `--strict` selects `error`. Nothing is guessed: an activity OLCF cannot execute is never mapped to something similar.
