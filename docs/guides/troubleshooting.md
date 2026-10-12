# Troubleshooting

Start with the information OLCF already gives you, then use the table for the symptom. Operational incidents (a stuck queue, a lost worker, a full disk) are also in the [runbook](../operations/runbook.md#common-incidents); this page is organised by what you see.

## Where to look first

| Tool | What it tells you |
|---|---|
| `olcf doctor` | this machine (Python, `uv`, JDK, disk, port, database, runtime, proxy settings) and, when a server answers, every server health check, each failure with a `fix:` line. Exit code 1 when it found a problem |
| `olcf health` | the server's component health as the Health page shows it |
| `olcf -v <command>` | the full Python traceback of a local or unexpected failure (otherwise one `error:` line and a `hint:`), the per-table progress of `olcf backup`, and the commands `olcf service` runs. It does not log HTTP requests; the correlation id of an API error is in its message |
| `olcf job get ID`, `olcf job timeline ID`, `olcf job logs ID` | the job record with its failure category and remediation, every state change with the reason, and the output |
| The error envelope | `{"error": {"code", "message", "details", "remediation", "correlation_id"}}`. The `correlation_id` is also the `X-Correlation-ID` response header, and appears as `cid=...` on the server log lines of that request. See the [error codes](../reference/errors.md) |
| `olcf diagnostics --out support.zip` | a support bundle (no secrets, keys, tokens or data); review it before you send it |
| `olcf audit list --action auth.login_failed` | who failed to sign in, from where, and why |

CLI exit codes: 0 success, 1 failure (or `doctor` found a problem), 2 usage error, 3 not authenticated, 4 permission denied, 5 not found, 6 conflict or precondition, 7 request rejected, 8 server error or unreachable, 9 `--wait` timed out, 10 the awaited job did not succeed, 11 prerequisite missing, 130 interrupted.

## Install and start

| Symptom | Cause and fix |
|---|---|
| `olcf: command not found` | the console script is not on `PATH`. With `pipx`/`uv tool` run `pipx ensurepath` and open a new shell; from a virtual environment activate it or call `python -m olcf.cli.main` |
| `olcf doctor` reports `port 8470` in use | another process listens there. Choose another with `olcf serve --port N` (or `server.port`) |
| `jdk: no Java 17/21` | `olcf runtime install --with-jdk` (downloads Eclipse Temurin). Behind a proxy set `HTTPS_PROXY` and, for a private CA, `SSL_CERT_FILE` first |
| `olcf doctor` says `OK runtime olcf-host-python verified`, but a Spark notebook or job waits (`waiting: [no_runtime] runtime 'olcf-rt-2.0.0' is not installed on any worker`) | expected on a fresh install: the doctor line counts the built-in host-Python runtime (the worker's own Python, listed as `no Spark`), which runs Python-engine notebooks, SQL on DuckDB and the UI. Spark jobs and Spark notebooks wait with the reason `no_runtime` until `olcf runtime install` has run (`olcf runtime list` shows `not installed`) |
| `runtime: no Spark runtime installed` (a `WARN` of `olcf doctor`) | the doctor found no runtime state directory yet: you ran `olcf init` but the server has never started, so no worker has created one. Start the server (`olcf serve --daemon`), after which the line reads `OK`; `olcf runtime install --with-jdk` adds a Spark runtime |
| `olcf quickstart` warns "the Spark runtime could not be installed" | the platform is already running. The download needs PyPI and Maven Central (`repo1.maven.org`): check the network or proxy (`HTTPS_PROXY`, `SSL_CERT_FILE`), point `OLCF_MAVEN_REPOSITORIES` at a mirror, or install offline with `olcf runtime install --bundle FILE`; then retry the command quickstart printed |
| `error: ...` with a `hint:` line instead of a Python traceback | every CLI failure is one line plus the fix. `olcf runtime install <unknown id>` lists the valid ids. Add `-v` (`olcf -v ...`) to see the traceback when reporting a bug |
| `host-Python runtime lacks ipykernel` | the worker's own Python is missing a module needed to run notebooks: `pip install ipykernel jupyter_client nbformat` into the environment of the worker (they are dependencies of OLCF, so this means a damaged installation). Workers started with `--no-host-python` do not offer the runtime |
| The sample notebook prints `matplotlib is not installed` | charts are optional. Install them with `pip install "olcf[plots]"` (or add matplotlib to the notebook's Environment); the tables are shown either way |
| Server exits at start with `tls_config_invalid` | your certificate or key is unreadable, expired, or not a pair. The message names the file ([TLS](tls-and-mtls.md#your-own-certificate)) |
| `data directory ... does not exist yet` | run `olcf init` (or `olcf quickstart`) first |
| Lost the generated admin password | it is in `<data dir>/secrets/bootstrap-admin.txt` if you kept it. Otherwise another PlatformAdmin runs `olcf user reset-password NAME`. There is no offline reset command in this build ([runbook](../operations/runbook.md#common-incidents)) |
| macOS asks to install developer tools | something started Apple's `java` or `git` stub; see [installing without the Command Line Tools](../getting-started/install-macos-no-clt.md) |

## Signing in and permissions

| Symptom | Cause and fix |
|---|---|
| `401 unauthenticated` from a script | the token is expired or revoked (personal tokens last 90 days unless you chose otherwise). `olcf whoami` shows what the server sees; create a new token |
| `401` for a worker token | the worker was revoked; re-enroll it ([cluster guide](cluster-and-control-panel.md#maintenance)) |
| `403 forbidden` / CLI exit 4 | your roles lack the permission named in the message. **Security, Roles and access** shows your bindings; the [permission matrix](../reference/permissions.md) says which role holds it. Scopes of a token also limit it: a token with scope `read` cannot write |
| `403 password_change_required` | the account has a temporary password. Sign in and choose a new one (`olcf password`); until then only that call is allowed |
| `429 rate_limited` on login | more than `auth.login_rate_limit_per_minute` (20) attempts from one address. Wait a minute |
| SSO users get "no role mapping" | the provider's claims match none of the mapping rules. Use **preview mapping** with the real claims ([SSO](sso-and-identity.md#mapping-identities-to-roles)) |
| SSO redirects back with an error | the redirect URI at the provider is not exactly `{server.public_url}/v1/identity/oidc/{name}/callback`, or `server.public_url` is not the address the browser uses |
| Local login is off and SSO is broken | start the server once with `OLCF_BREAKGLASS_LOCAL_LOGIN=1` |
| The browser says the connection is not private | the built-in CA is not trusted by the browser. Import `<data dir>/pki/ca.crt` or use your own certificate |

## Jobs and notebooks

| Symptom | Cause and fix |
|---|---|
| Job stays `queued` | `olcf job status ID` (and the **Job** page) print the reason as `waiting: [code] message` plus what to do, and `olcf job timeline ID` records each change as a `queue_reason` event. Codes: `no_runtime` (the runtime is not installed on any worker, or only on workers that are offline or cordoned; a Spark notebook on a fresh install says `olcf runtime install olcf-rt-2.0.0`), `no_node` (no worker is online: `olcf worker list`), `constraints` (a pool or label nobody satisfies), `paused` (the queue is paused: resume it under Compute > Queues and pools), `quota` (queue or workspace limit reached), `resources` (all cores or memory are taken), `infeasible`, `held`, `backoff`. A request that can never fit is failed with `resource_unsatisfiable` after `compute.infeasible_grace_seconds` (300) |
| `resource_unsatisfiable` | the executors or memory asked for do not fit any node. Lower `--driver-memory-mb`, `--executor-memory-mb`, `--max-executors`, or add a larger machine |
| Job `failed`, category `dependency` | a package is missing or could not be resolved. Add it to the environment and publish it, or change the environment's `pip_policy` ([notebooks](notebooks.md#python-packages)) |
| Job `failed`, category `user-code` | an exception in your code; `olcf job logs ID` has the traceback with cell and line. Not retried |
| Job `failed`, category `compatibility` | the analyzer blocked a Fabric-only construct before the job ran ([Fabric](fabric-integration.md#2-check-the-analyzer-on-a-notebook)) |
| Job `failed`, category `authz`/`storage` | the job's principal cannot read or write the lakehouse path, or the storage connection's credentials expired. `olcf job get ID` has the failure message and remediation; test the connection under **Connections and secrets** |
| Job `failed`, category `provider` or `timeout` | the worker was lost during the run (`olcf worker list`), or `--job-timeout` / the policy timeout was hit. Retry rules in the [compute model](../concepts/compute-model.md#failures-and-retries) |
| Job `failed`, category `provider`, code `result_missing` | a Kubernetes pod (or other runner-contract provider) ended without delivering its result within `compute.remote_result_grace_seconds` (120). OLCF will not call that a success. Read the pod's driver log (`olcf job logs ID`) and check the target data before you run the job again; it is retried automatically only if the job is `idempotent` ([compute model](../concepts/compute-model.md#remote-providers-and-the-result-contract)) |
| Job log says `log gap` / event `log_gap` | the container log was rotated or truncated, or a one-second burst exceeded the read window, before OLCF read it; those lines are lost, the result is not. Log less per second or write to artifacts ([Kubernetes](../operations/kubernetes.md#completion-result_missing-and-log-gaps)) |
| Livy job `failed` with `submission_unresolved`, `submission_not_created` or event `submission_uncertain` | a create request to Livy lost its response, so OLCF looked for the object by its name `olcf-<job>-a<attempt>`: `submission_adopted` carries on, `submission_not_created` is retried, `submission_unresolved` (unknown after 900 s) is **not** retried automatically. Check the Livy server for a session or batch of that name, stop it if it exists, then `olcf job retry ID` |
| Job `failed`, category `restore_reconcile` | the job was unfinished when the backup you restored was taken; no worker process of it exists in this environment. `olcf job retry ID` ([backup guide](backup-and-restore.md#restoring-into-a-clean-environment)) |
| A running job suddenly gets `401`/`403` from the API | its callback credential ended: the PAT or login it was submitted with was revoked or logged out, its attempt moved on, or its owner lost the role. Submit long jobs with a service token ([security model](../concepts/security-model.md#job-and-kernel-credentials)) |
| `olcf compute smoke` exits 6 / 9 / 10 | 6: `409 no_spark_runtime` (no online worker has a ready Spark runtime: `olcf runtime install --remote`) or `smoke_already_running` (follow it with `--status ID --wait` or stop it with `--cancel ID`); 10: the test ran and failed or hit its deadline (the message and the nodes table say why); 9: only your command stopped waiting (`--timeout`), the test keeps running until `--server-timeout` |
| Job `cancelled` or `timed_out` you did not expect | `olcf job timeline ID` has the reason and the actor |
| Spark executors never start | blocked ports between machines, or a worker advertising a name others cannot resolve. Set `--advertise-host`; compare with [ports](../reference/ports-and-networking.md); `olcf compute smoke` isolates the problem from your code |
| `%pip install` fails in a notebook | the environment is immutable and hash-pinned; install-time pip is only allowed by policy. Declare the package in an environment ([notebooks](notebooks.md#python-packages)) |
| Notebook kernel does not start | `compute.session_start_timeout_seconds` (180) elapsed, usually because no node has a ready runtime or the per-user limit `compute.max_sessions_per_user` (5) is reached. `olcf job timeline` of the session's job and the agent log on the node show the reason |
| Idle session vanished | sessions are culled after `compute.session_idle_timeout_seconds` (3600) |

## Workers and runtimes

| Symptom | Cause and fix |
|---|---|
| Worker `offline` | agent stopped, wrong URL, TLS trust (`--ca-pem`), or revoked (close code 4401). The agent log is in `<state dir>/logs/`; `olcf-worker status` on the machine |
| Enrollment fails with `enrollment_failed` | the enrollment token expired or was used (they are single use) or the URL is not reachable from the machine. Create a new token |
| Worker closes with 4403 | mutual TLS is required and the agent has no valid certificate ([TLS](tls-and-mtls.md#worker-mutual-tls)) |
| Worker is `degraded` | one of its components is red on the Compute page card (agent link, Java, Python, disk, clock). `olcf worker test WORKER` runs the checks |
| Clock skew warnings | enable time synchronisation on the node; tokens and signed requests have time windows |
| `runtime_install_failed`, `checksum_mismatch` | a download was corrupted or a mirror serves different bytes; installs fail closed and discard the staging directory. Retry; check the mirror |
| `manifest_signature_invalid` | the runtime manifest is signed by a key you do not trust. See [hardening](../operations/hardening.md#6-supply-chain) |
| `runtime manifest(s) ... not trusted`, bundled runtimes skipped, `runtime-trust` health check `down` or `degraded` | the shipped manifests are signed with the public development key. A production installation (no `environment` in `config.toml` counts as production) does not trust it. Development machine: add `environment = "development"` and restart. Production: sign the manifests with your own key and set `runtime.trust.dir` / `OLCF_RUNTIME_TRUST_DIR` on the control plane and the agents ([hardening](../operations/hardening.md#6-supply-chain)). `down` means the development key is trusted while `environment` is `production`; this also makes `GET /v1/health/ready` answer 503 (liveness stays 200), so the load balancer or Kubernetes marks the node not ready until you fix it |
| `worker-certificates` health check or the alert `auth-expiry` fires | a worker certificate expires soon (warn under 30 days, fail under 7). Connected workers renew by themselves; `olcf worker renew WORKER` forces it. An offline worker cannot: revoke and enrol it again ([runbook](../operations/runbook.md#worker-certificate-and-token-renewal)) |
| `prerequisite_missing` | Python or a JDK is missing on the node; the remediation says what to install |
| Runtimes differ between nodes | the `runtime_mismatch` alert fires. `olcf runtime verify ID --remote --deep`, then `repair` on the odd node |

## SQL, lakehouses and pipelines

| Symptom | Cause and fix |
|---|---|
| `query_timeout` (408) | the statement exceeded the engine timeout. Narrow it, or run it as a Spark job |
| `429` on SQL | more concurrent statements than `sql.max_concurrent_queries` (4) |
| SQL write refused | the embedded SQL engine is read-only by design; use Spark (`engine: spark`) or a notebook ([SQL guide](sql-and-lakehouse.md#running-sql)) |
| Lakehouse listing fails with a storage error | check the connection's **Test**; for S3 or ADLS verify endpoint, credentials and clock, and the SSRF settings for private endpoints |
| `payload_too_large` (413) | an upload or import exceeds `storage.max_upload_mb` (512) or `server.request_body_limit_mb` (256); behind a proxy also raise its body limit |
| A pipeline validation error with a path such as `activities[2].typeProperties.items` | the path is the exact location. `olcf pipeline validate FILE` lists all problems at once ([pipelines](pipelines.md#definition-format)) |
| Pipeline run failed with `interrupted` | the control plane restarted while a non-repeatable blocking activity was running. It is never re-run silently; use `olcf pipeline resume RUN` |
| Pipeline run failed with `timeout_uncertain` or `details.operatorAction` | an activity hit its timeout and did not stop within `policy.cancelGraceSeconds` (30), or the other side may have done the work. A non-idempotent activity is not retried automatically. Check the target; then `olcf pipeline resume RUN` continues from the failed activities. Mark activities that are safe to repeat with `policy.idempotent`, or send an `Idempotency-Key` in an `http` activity ([pipelines](pipelines.md#timeouts-and-uncertain-outcomes)) |
| SQL statement refused with `unsupported` although it works in a Spark shell | the host guard refuses reflection, script transformation, file-path tables, `LOCATION`, `SET`, `ADD JAR`, `${...}`, table functions outside an allowlist and functions outside the approved built-in list (custom/UDF functions, qualified `db.fn(x)` calls) for every role. Use lakehouse table names, a notebook or a Spark job ([SQL guide](sql-and-lakehouse.md#statements-that-are-always-refused)) |
| Schedule did not fire | the schedule is disabled, outside `start_at`/`end_at`, the server was down and `catchup` is `skip`, or no leader was elected. `olcf health` (leader) and `olcf pipeline schedule next SCHEDULE` |
| Webhook trigger returns 401 | wrong HMAC secret, timestamp more than 300 s off, or the body differs from what was signed ([pipelines](pipelines.md#triggers)) |
| Outbound `http` activity or webhook refused | the SSRF guard blocks private and loopback targets; allow the specific destination in `network.ssrf.allow` ([hardening](../operations/hardening.md#7-outbound-requests)) |

## Backup, restore, upgrade

| Symptom | Cause and fix |
|---|---|
| `backup_corrupt` | wrong passphrase, or the file was modified or truncated. `olcf backup verify` tells which; try another copy |
| Restored secrets cannot be decrypted | the master key of the original installation was not restored. Use `--include-keys` backups, or restore with `--overwrite-files` on a new host ([backup guide](backup-and-restore.md#from-the-command-line-works-with-the-server-stopped)) |
| After a restore, jobs are `failed` with `restore_reconcile` | expected: the worker processes of unfinished jobs do not exist in the new environment. Resubmit with `olcf job retry`. Sessions and kernels are not reconciled ([checklist](backup-and-restore.md#restoring-into-a-clean-environment)) |
| After restoring, `key-rotation` is `degraded` or tokens/secrets fail | a restore must bring the ring and the root key that match: restore the root key (`OLCF_MASTER_KEY` or `secrets/master.key`) first; after a root-key change run `olcf security rotate master-key --action rewrap` ([runbook](../operations/runbook.md#key-rotation)) |
| `olcf upgrade` stops before switching | it prints the failing step; the running version is unchanged and the previous version stays installed. `olcf upgrade --list` shows installed versions and `olcf upgrade --rollback` switches back. See the [runbook](../operations/runbook.md#updates) |

## Intelligence (catalog, lineage, governance)

Start with `olcf intelligence gaps`: every gap says what could not be seen and carries a `fix`. The full table is in the [Intelligence guide](intelligence.md#troubleshooting); the short version:

| Symptom | Cause and fix |
|---|---|
| A lineage tree ends early, or is empty | the code that reads or writes the table was not scanned, or the scan ended `partial` or `failed`, or the name is built at run time. `olcf intelligence coverage`, then `olcf intelligence scans list --status partial` |
| "lineage is incomplete: N gap(s)" under an answer | there are open gaps touching it; this is the honest answer, not an error. `olcf intelligence gaps --asset ASSET` |
| `404 not_found` for an asset that exists | a hidden asset (one in a workspace you cannot read) answers exactly like a missing one. Use the `ast_` id from `olcf intelligence search` and ask an administrator to check your workspace access |
| No `verified` edges | nothing has run since the module was enabled, the job probe is off (`OLCF_DATASET_EVENTS=0`), or the event inbox is behind. `olcf intelligence summary` shows pending and failed inbox rows; `olcf health` shows the check |
| `event inbox: N pending` keeps growing; health `degraded` | the processor is behind, or the leader is down. Intelligence never slows jobs, queries or pipelines, so this affects only how fresh the graph is. A row that failed is retried once automatically; `olcf intelligence inbox requeue` queues every failed row again |
| A deleted table is still listed | only a **full** scan that **succeeded** marks vanished assets `removed`. `olcf intelligence sources scan SOURCE --mode full --wait` |
| `sources test` fails for a Fabric or Power BI source | each of the eight steps names its fix (tenant settings, group membership, workspace role). The Fabric assumptions and how to confirm each on your tenant: see [Fabric assumptions](fabric-assumptions.md#26-intelligence-discovery-the-scanner-and-the-fabric-and-power-bi-rest-apis) |
| Sampling is refused | there is no policy, `max_rows` is 0, you lack `intelligence.sample`, the column is classified, or no sampling identity is configured. There is no MCP route to sample rows |
| `422 unsupported` when creating a policy | `enforce` mode does not exist; governance only observes |

## Fabric

See the table in [Fabric integration](fabric-integration.md#6-troubleshooting).

## Asking for help

Open an issue with the output of `olcf version`, `olcf doctor`, the relevant `correlation_id`, and, if useful, a support bundle (read it first; it is redacted, but it is your data). Use the bug report template of the project (`.github/ISSUE_TEMPLATE/`). For security problems follow [SECURITY.md](../../SECURITY.md) instead of opening a public issue.
