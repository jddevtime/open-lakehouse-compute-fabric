# Using the command line

The `olcf` command does two jobs. Local lifecycle commands act on an installation on this machine (`init`, `serve`, `doctor`, `service`, `upgrade`, `backup`, `restore`, `db`). All other commands are clients of the REST API and can drive any server you can reach. The complete option listing, generated from `--help`, is in the [CLI reference](../reference/cli.md); this page explains how the pieces fit and shows the common workflows.

Every command accepts `--help`. `olcf quickstart` prints the shortest path from nothing to a working system.

## Connecting to a server

Client commands need a URL and a credential. They are resolved in this order, first match wins:

1. flags: `--url`, `--token`, `-c/--context`, `--ca-bundle`
2. environment: `OLCF_URL`, `OLCF_TOKEN`, `OLCF_CONTEXT`, `OLCF_CA_BUNDLE`
3. the selected context (or, with no URL given at all, the current context)
4. the default `http://127.0.0.1:8470`

A **context** is a named URL plus credentials, created by `olcf login`:

```bash
olcf login                              # prompts for user name and password
olcf login -u admin --password-stdin < pw.txt
olcf login --token-stdin --name prod --url https://olcf.example.com < token.txt
olcf context list
olcf context use prod
olcf whoami
```

Contexts are stored in `contexts.json` in the per-user configuration directory (`~/.config/olcf` on Linux, `~/Library/Application Support/olcf` on macOS, `%APPDATA%\olcf` on Windows; `OLCF_CONFIG_DIR` overrides it) with mode 0600. With `OLCF_USE_KEYRING=1` and the optional `keyring` package installed, tokens go to the OS keychain instead. A context holds an access token and a refresh token; the CLI refreshes the access token when it expires and writes the new tokens back.

**An explicit URL never borrows stored credentials.** If you pass `--url` or set `OLCF_URL` without also naming a context (`-c` or `OLCF_CONTEXT`), the stored token is not sent to that address. This prevents a typo or a hostile URL from receiving a token. Add `-c <name>` when you want both.

For automation, prefer a personal access token over a password:

```bash
olcf token create ci-nightly --scope write --days 30    # the secret is shown once
export OLCF_TOKEN=olcf_pat_...
export OLCF_URL=https://olcf.example.com
olcf whoami
olcf token list
olcf token revoke <token-id>
```

Scopes narrow what a token may do ([security model](../concepts/security-model.md#authorisation)). Flags like `--token` appear in process lists; the environment variable does not.

For a server with a private CA, pass `--ca-bundle ca.crt` or set `OLCF_CA_BUNDLE` (the bundle to use is `<data dir>/pki/ca.crt` on the server).

## Global options

| Option | Meaning |
|---|---|
| `-o, --output table\|json\|yaml\|csv` | output format; `--json` is a shortcut for `-o json`; `OLCF_OUTPUT` sets a default |
| `-q, --quiet` | print only results and errors |
| `-v, --verbose` | the full traceback of local and unexpected errors instead of the one-line `error:` / `hint:`; per-table progress for `backup`; the commands `service` runs. Counted, but a second `-v` adds nothing today |
| `--no-color` | no colours (also `NO_COLOR`) |
| `--timeout` | HTTP timeout in seconds, default 60 |
| `--data-dir` | the local installation to act on, default `~/.olcf` (`OLCF_DATA_DIR`) |
| `-w, --workspace` (per command) | workspace name or id; default `OLCF_WORKSPACE` or the context's workspace |

Global options go before the command: `olcf -o csv sql run "select 1"`, not `olcf sql run -o csv ...`. Set a default workspace once with `olcf workspace use "Sample Retail"` (stored in the context).

## Exit codes

Scripts can branch on the exit code. They are stable.

| Code | Meaning |
|---|---|
| 0 | success |
| 1 | failure, or `olcf doctor` found a problem |
| 2 | usage error |
| 3 | not authenticated: run `olcf login` |
| 4 | permission denied |
| 5 | not found |
| 6 | conflict or precondition failed (stale `If-Match`, name in use) |
| 7 | request rejected (400 or 422), including fail-closed `unsupported` |
| 8 | server error, service unavailable or unreachable |
| 9 | a `--wait` ran out of time; the work may still be running |
| 10 | the awaited job or run did not end in `succeeded` |
| 11 | prerequisite missing (Python, JDK, uv, disk space, port) |
| 130 | interrupted with Ctrl-C |

### How errors look

Failures are one or two lines on stderr, never a Python traceback:

```text
error: no runtime named 'nope'
hint:  valid runtime ids: olcf-rt-1.3.0, olcf-rt-2.0.0, olcf-rt-native-1.0.0. List them with `olcf runtime list`.
```

`error:` says what went wrong and `hint:` (two spaces after the colon, so the text lines up) says what to do next. The three kinds of failure:

* **The server refused or could not be reached, or an awaited job did not succeed** (an SDK error): `error: <status> <code>: <message> (correlation id ...)`, for example `error: 404 not_found: job job_nope not found (correlation id 411a...)`. The correlation id can be searched for in the server log and the audit log; the `hint:` is the server's remediation when it gave one, otherwise a default for the exit code (for example "start one with `olcf serve --daemon`" when nothing answers). With `--json` the same information is one JSON object on stderr. The exit code is 3 to 10 as in the table above (a job that ends `failed` prints `error: job job_... ended failed: <category message and fix>` and exits 10). The error codes are listed in the [errors reference](../reference/errors.md).
* **A local failure** (the runtime manager, the data directory, configuration): `error: <message>` plus the remediation as `hint:`, exit 1.
* **Anything unexpected** (a bug or an environment problem): `error: <ExceptionType>: <message>` and `hint:  re-run with -v for the full traceback; `olcf doctor` checks the installation`, exit 1.

`-v` (`olcf -v job get ...`, a global option, so before the command) re-raises local and unexpected errors with their traceback, which is what to attach to a bug report. It does not change how the server's errors are printed. Mistakes in the command line itself (an unknown option, a missing argument) are reported by the argument parser as `Error: ...` with the usage text, exit 2.

## Install, start and stop

```bash
olcf quickstart                       # init + start + sample data, then the optional Spark runtime, then open the UI
olcf init                             # create the data directory, config.toml and the database
olcf serve                            # foreground
olcf serve --daemon                   # background, writes a pid file
olcf status                           # is it up; workers, runtimes, jobs
olcf stop                             # graceful: waits for running jobs
olcf doctor                           # checks the machine and the installation; each failure names its fix
olcf service install                  # start at login (macOS LaunchAgent, Windows scheduled task, systemd user unit)
```

`olcf serve` runs the API, UI and scheduler and, unless told otherwise, an embedded worker so that one machine is a complete cluster. See [getting started](../getting-started/README.md) for the walk-through and the [CLI reference](../reference/cli.md#olcf-serve) for options.

### What `olcf quickstart` does, in order

1. **Checks** the machine (Python, disk, port; a hard failure exits 11 with the fix).
2. **Initialises** the data directory (keeps an existing one), creating the `admin` account and a CLI context named `local` that follows `--port`.
3. **Starts the server** in the background (or reuses the one that is running) and **verifies the stored CLI login**: a token left in the `local` context by another data directory on the same URL is replaced by a fresh one.
4. **Installs the sample workspace** (`--no-sample` skips it).
5. **Waits up to 45 s** for the embedded worker to connect and offer the host-Python runtime, then **prints the working state**: URL, the one-time administrator credentials (on later runs, where the credentials file is), a direct link to the sample dashboard, and the next commands.
6. **Only then installs the Spark runtime** (`--no-runtime` skips it; `--runtime ID`, `--no-jdk`). It is a large download from PyPI and Maven Central and the step most likely to fail, so it is a follow-up: on failure quickstart prints a warning with the remediation and the exact retry command (`olcf runtime install <id> [--with-jdk]`), and still exits 0 with everything else running.
7. Opens the browser (`--no-open` skips it).

Run on an empty data directory with `olcf quickstart --no-runtime --no-open --port 8530` it prints this (the password is generated; the data directory is wherever `--data-dir` or `OLCF_DATA_DIR` points):

```text
==> checking this machine
==> initialising
==> starting the server
==> installing the sample workspace

OK: OLCF is running at http://127.0.0.1:8530
sample: Sample Retail (installed)
python: notebooks run on the built-in host-Python runtime (no Spark needed)
data:   /home/me/.olcf

Sign in to the web UI with (shown once - save it now):
  username: admin
  password: <generated-password>

Next steps:
  dashboard:   http://127.0.0.1:8530/#/dashboards/itm_01M4CRQK3MH7N7N7GWZFMG6EV1
  notebook:    olcf notebook run -w "Sample Retail" 02_Retail_Explore_Python --wait
  AI agents:   olcf mcp config --client claude-code      (prints a ready-to-paste MCP client configuration)
  SQL:         olcf sql run -w "Sample Retail" 'select count(*) from retail_lakehouse.fact_sales'
  also:        olcf status      olcf stop

No Spark runtime was requested. Add one any time with: olcf runtime install   (needed only for Spark notebooks, Spark SQL and Spark jobs)
```

Running it again is safe: the sample is reported as `(unchanged)`, no second password is created (the output points to `secrets/bootstrap-admin.txt` instead) and the running server is reused. If the worker has not reported the host-Python runtime within the wait, the `python:` line says so and points to `olcf runtime list --remote`. With `--json`, quickstart prints one JSON document (`url`, `sample`, `runtime`, `python_notebooks_ready`, `next`, and `runtime_retry` after a failed runtime install); the generated password is included once.

## Everyday workflows

All examples use the sample workspace (`olcf sample install`).

**Explore data.**

```bash
olcf workspace list
olcf lakehouse list -w "Sample Retail"
olcf lakehouse tables list retail_lakehouse -w "Sample Retail"
olcf lakehouse tables show retail_lakehouse fact_sales -w "Sample Retail"
olcf lakehouse tables preview retail_lakehouse fact_sales -w "Sample Retail"
olcf lakehouse files ls retail_lakehouse Files -w "Sample Retail"
```

**Run SQL.** DuckDB is the default engine and is read-only; `--engine spark` is needed for writes and needs an installed runtime.

```bash
olcf sql run -w "Sample Retail" --lakehouse retail_lakehouse \
  "select p.category, round(sum(f.net_amount),2) as revenue
   from fact_sales f join dim_product p using (product_id)
   group by 1 order by 2 desc limit 3"
olcf -o csv sql run -f report.sql --lakehouse retail_lakehouse -w "Sample Retail"
olcf sql run -f big.sql --save out.parquet --format parquet --lakehouse retail_lakehouse -w "Sample Retail"
olcf sql history
```

**Run a notebook and follow it.**

```bash
olcf notebook list -w "Sample Retail"
olcf notebook parameters 02_Retail_Explore_Python -w "Sample Retail"
olcf notebook run 02_Retail_Explore_Python -w "Sample Retail" -p top_n=5 --follow
echo $?          # 0 succeeded, 10 failed, 9 --timeout reached
```

`--wait` blocks and prints each state once; `--follow` also streams the log. `--idempotency-key` makes a retry safe: the same key never creates a second job.

**A job that does not start says why.** A queued job carries a reason, and the CLI shows it instead of letting you guess. `olcf job status <id>` (exit 1 while the job is queued or running) and `olcf job get <id>` print, for example, the sample Spark notebook on a fresh install:

```text
state:   queued
runtime: olcf-rt-2.0.0
waiting: [no_runtime] runtime 'olcf-rt-2.0.0' is not installed on any worker; install it: olcf runtime install olcf-rt-2.0.0   (downloads Spark from Maven Central; needs network access), or use a Python-engine notebook, which runs on the built-in host Python runtime
  fix: see the message: install the runtime or use a Python-engine notebook (host Python, no install needed)
```

`--wait` prints a `waiting [code] message` line and the fix the first time each reason appears (`--follow` does not). `olcf job timeline <id>` has the columns `AT`, `EVENT`, `STATE`, `DETAIL`: state changes show `from -> to`, and the events `runtime_fallback` (a Python-engine job moved to the built-in host-Python runtime because its Spark runtime is not installed) and `queue_reason` (one per change of reason) show their sentence. The reason codes are listed in the [compute model](../concepts/compute-model.md#why-is-my-job-still-queued).

**Submit a job from a file.**

```bash
olcf job submit --file etl.py --engine spark --runtime olcf-rt-2.0.0 \
  --param run_date=2026-01-31 --secret DB_PASSWORD=sec_... --wait
olcf job list --state failed
olcf job logs <job-id> --follow
olcf job timeline <job-id>        # every state transition and event
olcf job output <job-id>          # the executed notebook, when there is one
olcf job retry <job-id>
olcf job cancel <job-id>
```

`--secret ENV_NAME=secret-id` is resolved on the worker and never printed. `--body job.json` accepts a complete `JobSubmission` document, with other flags merged over it.

**Pipelines.**

```bash
olcf pipeline validate nightly.json -w "Sample Retail"
olcf pipeline create nightly --file nightly.json -w "Sample Retail"
olcf pipeline run nightly -w "Sample Retail" --wait
olcf pipeline runs nightly -w "Sample Retail" --state failed
olcf pipeline schedules -w "Sample Retail"
olcf pipeline schedule pause <schedule-id>      # enable | disable | pause | resume | run-now | next
```

See the [pipelines guide](pipelines.md) for the definition format.

**Where data comes from, and who is responsible for it (Intelligence and governance).** Assets are addressed by id (`ast_...`), name, path (`lakehouse.table`) or `table.column`; an ambiguous name lists the candidates, and an asset you cannot see answers "not found".

```bash
olcf intelligence search sales
olcf intelligence lineage lakehouse_a.clean                      # what feeds it, what it feeds (tree)
olcf intelligence lineage clean.amount --level column             # column lineage as a table
olcf intelligence where-from sales_report                         # roots, paths and the confidence of each
olcf intelligence impact raw                                      # what breaks if raw changes?
olcf intelligence gaps                                            # what the lineage cannot see, with the fix
olcf governance asset lakehouse_a.clean                           # owners, labels, certification, quality, violations
olcf governance terms search revenue
olcf governance exposure PII --type report --min-confidence inferred --paths
olcf governance quality lakehouse_a.clean
olcf governance review-queue list
olcf governance certify lakehouse_a.mart --comment "reviewed"     # needs governance.approve
```

Every command takes `-o json|yaml|csv` (or `--json`), so `olcf -o json governance exposure PII | jq '.exposures[].asset'` works in a script. Exit codes follow the table above: 4 when the role may not do it, 5 for an unknown name or id, 6 for a conflict or a stale `If-Match`, 7 when the request is rejected (for example a policy in `enforce` mode, which this release does not support). `olcf governance --help` and `olcf intelligence --help` list every command with examples; the full reference is generated in [cli.md](../reference/cli.md).

**Administration.**

```bash
olcf user create alice --role "Contributor@workspace:Sample Retail" --must-change-password
olcf role grant alice Viewer --workspace "Sample Retail"
olcf runtime install olcf-rt-2.0.0
olcf worker enroll-token --name rack2-node1 --pool batch      # on the controller
olcf-worker run --url https://olcf.example.com --token olcf_enr_...   # on the new machine
olcf audit verify
olcf backup create --out nightly.olcfbak
olcf health
olcf diagnostics --out support.zip
olcf settings list
```

`olcf backup` and `olcf restore` work on the data directory and need no running server ([backup and restore](backup-and-restore.md)).

## Shell completion

```bash
eval "$(olcf completion bash)"                         # bash
eval "$(olcf completion zsh)"                          # zsh
olcf completion fish | source                          # fish
olcf completion powershell | Out-String | Invoke-Expression
```

## Scripting tips

* Use `-o json` and parse with `jq`; table output is for people and may change.
* Check the exit code instead of parsing messages. Exit 9 means "still running", not "failed".
* Give long-running submissions an `--idempotency-key` so that a retry after a network error does not run twice.
* Give CI its own service principal and a `write`-scoped token with a short lifetime; revoke it when the pipeline is retired.
* The same operations are available from [Python](sdk.md) and, for any language, from the [REST API](../reference/api.md) (`/v1/docs` on the server serves interactive documentation).
