# OLCF Engineering Contracts

**Author:** Justin Dodrill · **Audience:** everyone building a package of OLCF · **Status:** normative

This document is the glue between work packages. If your code needs something from another package, it is
specified here. If you must deviate, change this file in the same change and say so in your final report.

## 0. Ground rules (read first)

1. **Everything must really work.** No stubs, no `pass` placeholders, no "TODO: implement", no faked results, no
   mock data pretending to be live. If a capability needs an external system (e.g. a Microsoft Fabric
   tenant, Maven Central, a Kubernetes cluster), implement it fully against the real API contract, cover it with
   contract tests against a local test server, mark it `live`/`needs-external` in the docs, and say so plainly in your report.
2. **Fail closed.** Unsupported semantics raise an actionable error (`olcf.core.errors.Unsupported` family). Never silently substitute.
3. **Headers.** Every source file starts with `SPDX-License-Identifier: Apache-2.0` and `Copyright (c) 2026 Justin Dodrill`.
   Every module has a docstring explaining *why* it exists; public classes/functions have docstrings. Detailed comments where
   logic is non-obvious. The project author is **Justin Dodrill** (use that name in docs, metadata, banners).
4. **Python ≥ 3.11** (dev on 3.13), type hints everywhere, `from __future__ import annotations`. Lint config is in `pyproject.toml`.
5. **Security defaults**: authorise every request server-side (`principal.require(...)`), validate input, never log secrets
   (everything user-visible passes `olcf.core.redaction`), parameterised SQL only (SQLAlchemy Core), path-traversal checks on every
   file path derived from input, no `shell=True`.
6. **Tests.** Put tests under `tests/<area>/` (unit = fast & hermetic, integration = real processes/sockets allowed).
   Run with `cd /home/claude/Open_Lakehouse_Fabric_Compute_Extender && . .venv/bin/activate && PYTHONPATH=src pytest tests/<area> -x -q`.
   **The sandbox has 2 CPUs / 7 GB RAM shared by several builders.** Anything that starts a JVM/Spark MUST be wrapped in
   `flock /tmp/olcf-spark.lock <command>` (or use the `spark_lock` fixture in `tests/conftest.py`) and be marked `@pytest.mark.spark`;
   keep Spark heaps ≤ 512 MB (`spark.driver.memory=512m`, `spark.executor.memory=512m`, `spark.sql.shuffle.partitions=2`).
7. **File ownership.** Only edit files you own (section 1). Need a change elsewhere? Make the smallest possible change and
   describe it in your final report; for the foundation (`core`, `db`, `security`, `api/core`) additive changes only.
8. **Do not `pip install` into the shared `.venv` without need**; if you must add a dependency, add it to `pyproject.toml` too.
   Runtime (Spark) environments are *separate* venvs created under a data dir (see §6) - never install pyspark into `.venv`
   unless you are the compute-node owner and say so.
9. Final report: what you built, how you verified it (commands + results), what is NOT verified and why, any contract changes.

## 1. Packages and owners

| Package / path | Owner (work package) |
|---|---|
| `core/`, `db/`, `security/` (except federation), `api/core`, `api/{app,deps,errors,middleware,modules}.py`, `docs/dev/CONTRACTS.md` | Foundation (architect) |
| `storage/`, `catalog/`, `sql/`, `mcp/`, `sample/`, `schemas/sample-*` | **DATA** |
| `runtime/`, `worker/`, `runtime_lib/{runner,bootstrap,spark,executor,kernel_host}.py` | **NODE** (compute-node) |
| `compute/` (controller side: registry, WS hub, scheduler, providers, jobs, sessions, runtimes API, topology) | **CTRL** (compute-controller) |
| `fabric/`, `sdk/fabric/olcf_fabric/`, `runtime_lib/notebookutils/`, `schemas/capsule-*`, `tests/corpus/` | **FABRIC** |
| `notebooks/` (service, environments, spark job defs, variable sets, git sync, import/export serializers) | **ITEMS** |
| `pipelines/`, `observability/` | **ORCH** |
| `security/federation.py`, `security/ldap_bridge*`, `ops/` (backup, updates, diagnostics, support bundle, retention), `tests/security/` | **SECOPS** |
| `web/static/**` | **UI** |
| `cli/`, `sdk/` (python client), `deploy/`, `scripts/`, `.github/`, `examples/` | **TOOLING** |
| `docs/**` (except `docs/dev/CONTRACTS.md`), `README.md`, governance files | **DOCS** |

## 2. Module system

Each feature package exposes `olcf.<pkg>.module:install(ctx: AppContext) -> Module` (see `olcf/api/modules.py` for the
canonical list and import paths). `install` registers services with `ctx.register_service(name, obj)`, health checks with
`ctx.health.register(...)`, item hooks (`ctx.register_item_hooks`) and connection testers (`ctx.connection_testers[type] = fn`),
and returns a `Module` whose `.routers = [(APIRouter, prefix)]` are mounted under `/v1` and `.public_routers` at the app root
(e.g. `/mcp`). `Module.start()/stop()` start/stop background threads (always check `ctx.leader.is_leader()` before running a
cluster-singleton loop such as the scheduler, trigger evaluator, alert evaluator, retention).

Schema: each package with tables defines them in `olcf/<pkg>/schema.py` using `olcf.db.schema.metadata` and registers a
migration *stream* named after the package (`register(Migration("compute", 1, ..., up=lambda c: create_tables(c, [...])))`).
`Column` types: `ID` (String(64)), `TS` (ISO-8601 UTC string, use `olcf.core.timeutil.now_iso()`), `JSON`. Primary keys are
prefixed ULIDs from `olcf.core.ids.new_id(prefix)` (add new prefixes to `PREFIXES` if needed). Tables must work on SQLite *and*
PostgreSQL (no dialect-specific SQL without a branch on `db.dialect`).

### Foundation services available to everyone (`ctx.service(name)`)

| name | object | key methods |
|---|---|---|
| `identity` | `IdentityService` | `get(pid)`, `get_by_name`, `create_principal(name, kind=..., ...)`, `bind(principal_id, role, scope_type, scope_id)`, `sync_bindings(pid, source, wanted)` |
| `workspaces` | `WorkspaceService` | `resolve(ref_id_or_name)`, `create`, `list`, `export_zip`, `import_zip` |
| `items` | `ItemService` | `get(id)`, `find(ws_id, type, name)`, `resolve(ws_id, ref, type)`, `create(principal, ws_id, ItemCreate)`, `update(principal, id, ItemUpdate, if_match)`, `delete`, `list(ws_id, type_=...)`, `versions(id)` |
| `connections` | `ConnectionService` | `get(id)`, `resolve(ref, workspace_id)`, `create(...)`, `test(principal, id)`; secret material only via `ctx.secrets.resolve(connection["secret_id"])` |
| `operations` | `OperationService` | `create(kind, principal_id)`, `update(oid, state=, progress=, message=, result=, error=)`, `get` |
| `settings` | `KVSettings` | `register(key, description=, default=, validator=)`, `get(key)`, `set(key, value)` |

Direct context members: `ctx.db` (SQLAlchemy Core via `with ctx.db.begin() as conn`), `ctx.secrets` (`create/resolve/rotate/delete`),
`ctx.audit.record(principal, action, resource_type=, resource_id=, workspace_id=, details=)`, `ctx.events.publish(type, payload, workspace_id=, subject=)`,
`ctx.health`, `ctx.tokens`, `ctx.authz`, `ctx.pki`, `ctx.leader`, `ctx.settings`.

### Services other packages must register (names are contract)

| name | registered by | interface |
|---|---|---|
| `storage` | DATA | `StorageService` - see §3 |
| `catalog` | DATA | `CatalogService` - `upsert_table(...)`, `list_tables(lakehouse_id)`, `record_lineage(...)`, `search(q, principal)` |
| `sql` | DATA | `SqlService.execute(principal, SqlRequest) -> SqlResult`; engines `duckdb` (default) and `spark` |
| `reports` | DATA | `ReportService.run(principal, report_item_id, filters) -> dict` |
| `compute` | CTRL | `ComputeService` - see §4 |
| `jobs` | CTRL | alias of `compute.jobs` (`JobService`): `submit(principal, JobSubmission) -> JobRecord`, `get(id)`, `wait(id, timeout)`, `cancel(principal, id)`, `list(...)`, `logs(id, offset, limit)` |
| `sessions` | CTRL | `SessionService.create(principal, ...)`, `execute(session_id, code)`, `stop(...)` |
| `runtimes` | CTRL | `RuntimeCatalog.resolve(RuntimeRequest) -> RuntimeSelection` |
| `notebooks` | ITEMS | `NotebookService.load(item) -> Notebook`, `parse(content, format) -> Notebook`, `to_ipynb/to_py`, `detect_parameters` |
| `environments` | ITEMS | `EnvironmentService.resolve(item_id|lock) -> EnvironmentSpec` (runtime request, lock, approved spark conf) |
| `pipelines` | ORCH | `PipelineService.run(principal, pipeline_item_id, parameters, trigger)` |
| `metrics` | ORCH | `MetricsRegistry` (`counter/gauge/histogram`) + `record_sample(name, value, labels)` |
| `federation` | SECOPS | `authenticate_bearer(token) -> Principal | None` (external OIDC/Entra JWTs) |
| `fabric` | FABRIC | `FabricService` (`analyze`, `submit_capsule`, `diagnose`) |
| `backup` | SECOPS | `BackupService` |

All service lookups from other packages must tolerate absence (`ctx.maybe_service`) where the feature is optional.

## 3. Storage & lakehouse contract (DATA)

* Lakehouse = item of type `lakehouse`; `definition = {"storage": {"connection_id": str|null, "root_uri": str}, "default_format": "delta",
  "schema_enabled": false, "catalog": "default", "fabric": {"workspace_id":..., "lakehouse_id":...}|null}`.
  Layout below `root_uri`: `Files/…` (unmanaged files) and `Tables/<name>` (or `Tables/<schema>/<name>` when schema_enabled).
* Canonical URI `olcf://<workspace>/<lakehouse>/Files/<path>` | `…/Tables/<name>` (path segments percent-encoded, so a workspace named `Sample Retail` is `olcf://Sample%20Retail/…`).
  `StorageService.resolve(uri, default_lakehouse=None, *, workspace_id=None) -> ResolvedLocation(physical_uri, provider, lakehouse_id, area, relative_path, canonical_uri)`.
  A relative `Files/…` / `Tables/…` path needs `default_lakehouse`, otherwise `BadRequest`. Fields:
  * `area` is `"Files"` or `"Tables"` (empty for the lakehouse root), `physical_uri` is the provider's real URI (`file://…`, `s3://…`, `abfss://…`) and
    `canonical_uri` the `olcf://` spelling of the same location;
  * **`relative_path` is relative to `area`, not to the lakehouse root**: `olcf://ws/lh/Files/raw/a.csv` -> `area="Files"`, `relative_path="raw/a.csv"`;
    `olcf://ws/lh/Tables/sales` -> `area="Tables"`, `relative_path="sales"`. The path from the lakehouse root is the property `root_relative` (`"Files/raw/a.csv"`),
    which is what `StorageService.read_bytes/list_files/...` and `physical_uri(lh, rel)` take. Never join `relative_path` onto `root_uri` directly - it would skip the area directory.
* `StorageService` (all methods take/return plain dicts/dataclasses, raise `OlcfError` subclasses; `lh` is a `Lakehouse` or any ref `get_lakehouse` accepts):
  `get_lakehouse(id|ref)`, `list_files(lh, path, recursive=False)`, `read_bytes/open_read(lh, path)`, `write_bytes/open_write`, `delete`, `move`, `copy`, `mkdirs`,
  `stat`, `list_tables(lh)`, `table_info(lh, name)` (schema, partitioning, format, delta protocol/features, history), `physical_uri(lh, rel="")`,
  `job_lakehouse_descriptor(lh)`, `test(lh|connection) -> {ok, steps:[{name, ok, detail, ms}]}`.
* **Runtime storage settings** (for the compute layer only: credentials are resolved *at call time* from the connection's secret, nothing is cached, and **none of these may ever be
  returned by an API, logged, audited or stored in a job record**). They are derived from the same provider call. A lakehouse that needs no credentials yields no secrets: local files give only
  `{"spark.hadoop.fs.file.impl.disable.cache": "false"}` and `{}`; identity-based modes (AWS default credential chain, Azure managed identity) yield only non-secret hints
  (region, endpoint, credentials-provider class):
  * `spark_conf(lh) -> dict[str, str]` - Spark session settings, **every key prefixed `spark.hadoop.`** (S3A: `spark.hadoop.fs.s3a.{endpoint,access.key,secret.key,session.token,path.style.access,
    aws.credentials.provider,…}`; ABFS: `spark.hadoop.fs.azure.account.{key,auth.type,oauth2.*}.<account-host>` / `fs.azure.sas.fixed.token.<host>`). Put them in the `SparkConf` / `--conf` of the driver.
  * `hadoop_conf(lh) -> dict[str, str]` - the same entries with the `spark.hadoop.` prefix **removed** (bare Hadoop keys such as `fs.s3a.access.key`), for code that builds a
    Hadoop `Configuration` (`sc._jsc.hadoopConfiguration().set(k, v)`) or a PyArrow/fsspec filesystem. `hadoop_conf == {k.removeprefix("spark.hadoop."): v for k, v in spark_conf.items()}`.
  * `storage_env(lh) -> dict[str, str]` - environment variables for Python-side clients inside a job (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`,
    `AWS_DEFAULT_REGION`, `AWS_ENDPOINT_URL`, `AWS_CA_BUNDLE`; `AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_KEY` | `AZURE_STORAGE_SAS_TOKEN` | `AZURE_TENANT_ID`/`AZURE_CLIENT_ID`/`AZURE_CLIENT_SECRET`).
    Values are strings; set them in the job process environment only (never in the control plane's own `os.environ`).
  * `delta_storage_options(lh) -> dict[str, str]` - `storage_options` for delta-rs (`deltalake.DeltaTable(uri, storage_options=…)`); also valid for object_store based tools.
    `iceberg_properties(lh)` - PyIceberg catalog/FileIO properties.
  * `job_storage_conf(lh) -> {"spark": spark_conf(lh), "env": storage_env(lh)}` - **exactly the object the compute controller writes to the job's `storage_conf`** (a 0600 file next
    to `job.json`, deleted when the job ends; section 4.3). The credential-free counterpart that goes into `job.json` itself is `job_lakehouse_descriptor(lh)`
    (`{id, name, workspace_id, root_uri, tables_uri, files_uri, format, fabric}` with physical URIs). A job with several lakehouses gets one `job_storage_conf` merge per lakehouse
    (`spark` and `env` dicts are unioned; keys are account/host-scoped so they do not collide; the compute layer owns the merge).
* Local default: `file://<data_dir>/storage/lakehouses/<workspace-name>/<lakehouse-name>.Lakehouse`.
  OneLake: `abfss://<fabric-workspace-id>@onelake.dfs.fabric.microsoft.com/<fabric-lakehouse-id>`; S3/MinIO: `s3://bucket/prefix`.
* Providers (`olcf/storage/providers/`): `local`, `s3` (AWS S3, MinIO, any S3-compatible), `adls` (ADLS Gen2), `onelake` (ADLS API against OneLake). Pluggable via entry-point group `olcf.storage_providers`.
* Delta safety (spec 16): before writes through OLCF-managed paths and for `/tables/{n}/protocol`, inspect `_delta_log` protocol (minReader/minWriter, reader/writer
  features) and compare with the **interop policy** (`olcf/storage/delta_policy.py`, versioned; default allows features supported by Fabric Runtime 2.0 family and blocks
  upgrades that would break Fabric interop). `vacuum` requires `lakehouse.destructive`, defaults to dry-run, refuses retention < 168 h unless explicitly overridden by a PlatformAdmin setting.

## 4. Compute contract (CTRL ↔ NODE ↔ others)

### 4.1 ComputeService (CTRL) - used by ITEMS, ORCH, FABRIC, DATA, MCP

```python
class ComputeService:
    jobs: JobService          # submit/get/wait/cancel/list/logs  (JobSubmission/JobRecord from olcf.core.models)
    sessions: SessionService
    runtimes: RuntimeCatalog
    def topology(self) -> dict                      # see §4.6
    def run_spark_sql(self, principal, workspace_id, sql, *, lakehouse=None, limit=1000, timeout=120) -> dict   # via a pooled SQL session
```
`JobService.submit` performs: authz (`job.submit` in workspace), idempotency (on `idempotency_key`), resolve item/source, run the compatibility analyzer when the
origin is Fabric or the source is `fabric_git_source`/ipynb with Fabric constructs (`ctx.maybe_service("fabric").analyze(...)`), resolve runtime (`RuntimeCatalog`),
persist `Accepted → Validating → Queued`, emit `job.state_changed` events with `{job_id, from, to, reason}` on every transition and persist each transition (table `job_events`).

### 4.2 Controller ⇄ Worker Agent protocol (wire version `WORKER_PROTOCOL_VERSION = "1.0"`)

* **Enrollment** (HTTP): admin creates an enrollment token: `POST /v1/workers/enrollment-tokens` → `{token: "olcf_enr_…", expires_at, command}`.
  Agent: `POST /v1/workers/enroll` body `{token, name, hostname, csr_pem?, inventory, agent_version, protocol}` → `{worker_id, worker_token: "olcf_wrk_…", ca_pem, client_cert_pem?, channel_url, heartbeat_seconds}`.
  Token is single-use. The agent stores identity under its state dir with 0600 perms.
* **Channel**: agent opens an outbound WebSocket to `GET /v1/workers/ws` (`Authorization: Bearer olcf_wrk_…`; with `tls.require_client_cert_for_workers` the
  client certificate CN must equal the worker id). Frames are JSON objects `{"v":1,"type":..., "id": <uuid for requests>, "ref": <id being answered>, "ts": ..., "payload": {...}}`.
  The agent reconnects with exponential backoff (1 s → 30 s) forever; on every (re)connect it sends `hello` containing its full state so the controller can **reconcile**.
* **Agent → controller**: `hello {worker_id, agent_version, protocol, inventory, active_jobs:[{job_id, pid, state}], kernels:[{session_id,...}], spark_daemons:[{role,runtime_id,pid,url}]}`;
  `heartbeat {ts, metrics{cpu_percent, load1, mem_total_mb, mem_available_mb, disk_total_gb, disk_free_gb, net_rx_bps, net_tx_bps}, runtimes:[{id,state,verified}], components:[{name,status,detail}], active_jobs, spark_daemons}` every 5 s;
  `result {ref, ok, data|error}` (answer to a command); `job_state {job_id, state: running|succeeded|failed|cancelled|timed_out, exit_code, result?, failure?}`;
  `job_log {job_id, stream: stdout|stderr, seq, data}` (chunks ≤ 64 KiB; logs are streamed to disk by the controller, never held whole in memory);
  `job_event {job_id, event, data}` (e.g. `spark_app_started {application_id, ui_url}`); `kernel_event {session_id, channel: iopub|shell|status, content}`; `daemon_state {role, runtime_id, state, detail}`.
* **Controller → agent** (each carries `id`; agent replies `result{ref=id}`): `ping`, `run_job {spec}` (job spec §4.3), `cancel_job {job_id, grace_seconds}`, `start_spark_master {runtime_id, host, port, ui_port, conf}`,
  `stop_spark_master`, `start_spark_worker {runtime_id, master_url, cores, memory_mb, labels, work_dir, port?}`, `stop_spark_worker {runtime_id}`, `start_kernel {session_id, spec}`,
  `kernel_execute {session_id, msg_id, code}`, `kernel_interrupt`, `kernel_restart`, `kernel_shutdown`, `kernel_complete {code, cursor}`, `probe {checks:[{kind: tcp|http|dns|storage|disk|clock|runtime|spark_master, ...}]}`,
  `install_runtime {runtime_manifest, mode: install|repair|verify|retire|rollback, offline_bundle?}`, `drain {on}`, `collect_diagnostics {…}`, `shutdown`.
* **Credential renewal** (additive, same wire version): `renew_cert {csr_pem?, reason: scheduled|requested|startup, cert?: {serial, not_after}}` (agent → controller, carries an `id`) is answered with a `result` that **carries data**
  (the one agent frame whose acknowledgement does): `{ok: true, data: {renewed: ["certificate"?, "token"?], cert_pem?, serial?, ca_pem, cert_expires_at, worker_token?, token_expires_at, token_grace_seconds?, renew_after}}` or
  `{ok: false, error: {code, message, details?}}` with `code` one of `renewal_not_due` (`details.renew_after`), `renewal_too_soon`, `auto_renew_disabled`, `bad_csr`, `forbidden` (revoked worker). The subject of the signed certificate is always
  `CN=<worker id>`. `hello` may carry `cert: {serial, not_after}` (what the node holds); the hello acknowledgement carries `credentials: {cert_expires_at, token_expires_at, renew_after (ISO or null), auto_renew, renew_before_days, token_max_age_days}`
  which tells the agent when to ask. Controller → agent command `renew_credentials {certificate, token}` (operator request; the agent answers after running the `renew_cert` exchange). Details: Review remediation addenda (LIFECYCLE) L2.
* The controller never trusts the agent's claims about identity: worker id comes from the authenticated token (and certificate when mTLS is required).
* **Replay / duplicate protection**: `run_job` is keyed by `(job_id, attempt)`; an agent that already runs it answers `ok` without starting a second process. Jobs are never re-executed after a controller restart unless
  `policy.idempotent` is true **and** the agent confirmed the process is gone.

### 4.3 Job spec (`job.json`) - produced by CTRL, consumed by the runner (NODE)

```jsonc
{
  "schema": "olcf.jobspec/1", "job_id": "job_…", "attempt": 1, "kind": "notebook|spark_job|sql|capsule|script", "engine": "spark|python",
  "workspace": {"id": "wks_…", "name": "…"}, "name": "…",
  "source": {"format": "ipynb|py|fabric_git_source|sql|script|jar", "path": "source/notebook.ipynb", "sha256": "…"},   // file is written next to job.json by the agent from the payload
  "parameters": {"p1": 1},
  "spark": {"master": "spark://host:7077" | "local[*]", "app_name": "…", "driver_host": "…", "driver_port": 41000, "block_manager_port": 41100,
            "conf": {"spark.executor.memory": "1g", "...": "..."}, "jars": ["…"], "packages": []},
  "runtime": {"id": "olcf-rt-2.0.0", "family": "fabric-2.0-compatible", "fingerprint": "sha256:…"},
  "default_lakehouse": {"id": "itm_…", "name": "…", "workspace_id": "wks_…", "root_uri": "…", "tables_uri": "…/Tables", "files_uri": "…/Files", "format": "delta",
                        "fabric": {"workspace_id": "…", "lakehouse_id": "…"}|null},
  "lakehouses": [ /* same shape: other lakehouses the job may use */ ],
  "storage_conf": {"spark": {"spark.hadoop.fs.azure…": "…"}, "env": {"AWS_…": "…"}}, // SENSITIVE: 0600 file, deleted at job end; = StorageService.job_storage_conf (section 3), merged over the job's lakehouses; Spark keys carry the spark.hadoop. prefix (runtime_lib adds it to bare Hadoop keys)
  "env": {"K": "V"}, "secrets": {"ENV_NAME": "value"},                          // SENSITIVE
  "limits": {"timeout_seconds": 3600, "max_output_bytes": 52428800},
  "callbacks": {"api_url": "https://…", "job_token": "olcf_pat_…"},              // short-lived token scoped to this job (child runs, secrets API for notebookutils)
  "context": { /* normalised runtime context (workspace/notebook/lakehouse/run ids, origin, fabric block) */ },
  "capsule": { /* optional, metadata only */ }
}
```
The runner writes `result.json` next to it: `{"state": "succeeded|failed", "exit_value": any, "failure": {category,message,cell,line,traceback,remediation}|null,
"spark": {"application_id","ui_url","app_name"}|null, "artifacts":[{"name","path","media_type"}], "metrics":{…}}` and prints progress as single lines
`@@OLCF@@ {"event": "...", ...}` on stdout, which the agent forwards as `job_event`. Notebook jobs also produce `output.ipynb` (executed notebook, outputs truncated to `max_output_bytes`).
Exit code 0 = succeeded; non-zero + `result.json.failure` classify the failure using `olcf.core.errors.FailureCategory`.

### 4.4 Notebook sessions (browser ⇄ controller)

REST: `POST /v1/sessions {workspace_id, notebook_id?, runtime?, resources?, default_lakehouse?, engine}` → session; `GET /v1/sessions`, `GET /v1/sessions/{id}`, `DELETE /v1/sessions/{id}`,
`POST /v1/sessions/{id}/interrupt|restart`. WebSocket `GET /v1/sessions/{id}/ws?access_token=…` JSON messages:
client→server `{"op":"execute","cell_id":"c1","code":"…"}`, `{"op":"interrupt"}`, `{"op":"restart"}`, `{"op":"complete","code":"…","cursor":3,"req":"r1"}`;
server→client `{"type":"status","state":"starting|idle|busy|dead","detail":"…"}`, `{"type":"output","cell_id":"c1","output":{<nbformat output dict: stream|display_data|execute_result|error>}}`,
`{"type":"reply","cell_id":"c1","status":"ok|error|aborted","execution_count":3}`, `{"type":"spark","application_id":"…","ui_url":"…"}`, `{"type":"complete","req":"r1","matches":[…]}`.
Sessions survive browser reconnects (the kernel lives on the worker; reconnecting clients get `status` + replay of the last 500 outputs). Idle sessions are culled after the configured timeout.

### 4.5 Runtimes (CTRL registry + NODE manager)

A **runtime manifest** (JSON, signed, `olcf/runtime/manifests/*.json`) declares: `id`, `version`, `family`, `spark`, `delta`, `iceberg`, `java`, `scala`, `python`, `os`/`arch` constraints, `packages` (pip pins **with sha256 hashes**),
`jars` (Maven coordinates; hashes recorded in a lock file at first resolution), `spark_conf_defaults`, `approved_spark_conf` (allow-list keys/patterns), `signature`. Runtimes are installed **side-by-side** under
`<node state dir>/runtimes/<id>/` as `{venv/, jars/, manifest.json, lock.json, VERIFIED}`; install/repair/retire/rollback are transactional (stage dir → atomic rename). `fingerprint = sha256(manifest + lock)`.
The scheduler places a job only on nodes where the runtime is `ready`. Spark standalone masters/workers are per runtime id (never mixed).

### 4.6 Topology / health model for the control panel (CTRL provides; UI renders)

`GET /v1/compute/topology` returns (all fields always present, empty lists when none):

```jsonc
{
  "generated_at": "…",
  "summary": {"nodes_total": 3, "nodes_online": 3, "cores_total": 12, "cores_used": 4, "memory_total_mb": 0, "memory_used_mb": 0, "jobs_running": 1, "jobs_queued": 0, "executors": 4},
  "control_plane": {"nodes": [{"node_id": "…", "leader": true, "version": "0.1.0", "uptime_seconds": 12}],
                    "components": [{"name": "database", "status": "ok|degraded|down|unknown", "detail": "…", "latency_ms": 1.2, "category": "control-plane", "checked_at": "…"}]},
  "clusters": [{"id": "clu_…", "runtime_id": "…", "family": "…", "provider": "standalone", "state": "up|degraded|down|stopped", "master": {"url": "spark://…", "ui_url": "…", "state": "ok", "alive_workers": 2,
                "cores_total": 4, "cores_used": 2, "memory_total_mb": 4096, "memory_used_mb": 1024, "applications": [{"id": "app-…", "name": "…", "cores": 2, "state": "RUNNING", "job_id": "job_…", "executors": [{"worker": "wrk_…", "cores": 1}]}]},
                "workers": ["wrk_…"]}],
  "nodes": [{"id": "wrk_…", "name": "…", "hostname": "…", "address": "…", "state": "online|degraded|offline|pending|revoked", "scheduling": "active|cordoned|draining", "pool": "default", "labels": {"site": "…"},
             "agent_version": "…", "os": "…", "arch": "…", "heartbeat_age_seconds": 1.2, "resources": {"cores": 4, "memory_mb": 8192, "disk_gb": 100, "cores_allocated": 1, "memory_allocated_mb": 1024},
             "metrics": {"cpu_percent": 12.3, "mem_percent": 40.1, "disk_percent": 22.0, "load1": 0.4, "net_rx_bps": 0, "net_tx_bps": 0},
             "history": {"cpu": [12, 14], "mem": [40, 41], "ts": ["…", "…"]},
             "components": [{"name": "agent|spark-worker:<runtime>|runtime:<id>|storage:<connection>|disk|clock|control-plane-link", "status": "ok", "detail": "…"}],
             "runtimes": [{"id": "…", "state": "ready|installing|failed|retired", "verified": true}], "running_jobs": [{"job_id": "…", "name": "…", "state": "running"}]}],
  "queues": [{"name": "default", "depth": 0, "running": 1, "oldest_wait_seconds": 0}]
}
```
`POST /v1/workers/{id}/test` → `{ok, steps:[{name, ok, detail, ms}]}` (probe suite executed **from the node's perspective**: DNS/TCP/TLS to control plane, TCP to each Spark master, storage reachability for every registered storage connection,
disk space, clock skew vs controller, runtime verify). `POST /v1/compute/test` runs it for every node concurrently and returns a matrix. `POST /v1/compute/spark-smoke` submits a tiny distributed job and reports which nodes hosted executors.

## 5. Fabric contract (FABRIC ↔ everyone)

Endpoints under `/v1/integrations/fabric` (all require auth; `integration.fabric.*` perms): `GET /` (config + readiness checklist + counters), `PUT /` (configure: allowed tenants/audiences, workspace mapping
Fabric workspace id → OLCF workspace, broker connection id for pull mode & OneLake, policy), `POST /test` (stepwise diagnostics: DNS → TLS → proxy → Entra token → Fabric API authorisation → OneLake authorisation; each step `{name, ok, detail, ms, remediation}`),
`POST /handshake` (SDK `test_connection()`; records what Fabric sent: tenant, workspace, notebook, user, SDK version, latency), `POST /validate`, `POST /submit`, `GET /submissions`, `GET /submissions/{id}`,
`GET /metrics` (counters + latency histogram + error breakdown + last-seen), `POST /analyze` (paste notebook → report), `GET /compat/rules`, `GET /snippet?workspace=…` (ready-to-paste Fabric notebook cell).
`fabric_submissions` rows record: id, received_at, fabric tenant/workspace/notebook ids + names, default lakehouse, run ids, pipeline flag, caller identity, SDK/adapter version, capsule size & sha256, parameters (names only + redacted values),
compatibility summary (counts by class + blockers), job id, final state, round-trip latency, error category. Capsule schema: `schemas/capsule-v1.json`.

## 6. Paths & process model

* Data dir (control plane): `settings.data_dir` (default `~/.olcf`). Sub-dirs: `secrets/ pki/ logs/ jobs/ runtimes/ storage/ backups/ tmp/ workers/ artifacts/`.
* The **embedded worker** (all-in-one mode) is an ordinary worker agent running in a thread/subprocess of the control plane and enrolling itself (`compute.embedded_worker`), using the same protocol - single-node = cluster of one.
* Worker agent state dir: `<data_dir>/workers/<name>/` (embedded) or `~/.olcf-worker/` (remote); contains `identity.json` (0600), `runtimes/`, `work/<job_id>/`, `logs/`.
* Job workdir: `<state>/work/<job_id>/` containing `job.json` (0600), `source/`, `result.json`, `output.ipynb`, `stdout.log`; removed per retention policy; secrets removed at end of job.

## 7. API conventions

* Prefix `/v1`. JSON in/out. Errors use the envelope in `olcf/core/errors.py`/`api/errors.py` (`{"error": {"code","message","details","remediation","correlation_id"}}`).
* Lists: `{"items": [...], "next_cursor": str|null}` with `?limit=&cursor=`; filters as query params.
* Mutations on long-running work return `202` + `{operation_id}` or the job record; support `Idempotency-Key` (use `olcf.api.deps.Idempotency`).
* Updates support `ETag`/`If-Match` (`olcf.api.deps.check_if_match`).
* Every handler: `principal: Authed` + explicit `principal.require("<perm>", workspace_id=...)`. Never rely on UI hiding. Workspace-scoped objects must be re-checked by *their* workspace id, not a client-supplied one.
* Never return secret values, token plaintext (except on creation), ciphertext or storage credentials.

## 8. Health checks for the control panel

Register checks with `ctx.health.register(HealthCheck(name, category, description, quick, deep))`. Categories used by the UI grouping: `control-plane`, `compute`, `storage`, `fabric`, `security`, `agents`.
Names (stable): `database`, `secrets`, `audit`, `leader`, `data-disk`, `pki`, `api` (foundation); `scheduler`, `spark-master:<runtime>`, `worker-fleet`, `runtimes` (CTRL); `storage:<provider|connection>` (DATA); `fabric-adapter`, `fabric-auth`, `onelake` (FABRIC);
`mcp` (DATA); `pipelines-engine` (ORCH); `metrics` (ORCH); `backup` (SECOPS); `identity-providers` (SECOPS). `deep` performs a real active test and returns `steps` with per-step results - this powers every "Test connectivity" button.

## 9. Fabric-compatible runtime baseline (spec §4)

`fabric-2.0-compatible`: Spark 4.1.x (`pyspark==4.1.1` - the newest release satisfying `delta-spark==4.2.0`'s `pyspark<=4.1.1`), Java 21, Scala 2.13, Python 3.13, Delta 4.2.0 (`io.delta:delta-spark_2.13:4.2.0`).
`fabric-1.3-compatible` (legacy, optional): Spark 3.5.5, Java 11, Scala 2.12, Python 3.11, Delta 3.2.0. Labels must always read "Fabric Runtime 2.0 **Compatible**" - never imply the runtime *is* Microsoft's.

## 10. Item definition schemas (owners validate via `ctx.register_item_hooks(type, validate=...)`)

* **notebook** (ITEMS): `{"nbformat": {<nbformat v4 notebook dict: cells[], metadata{}, nbformat, nbformat_minor>},
  "settings": {"engine": "spark|python", "runtime": {<RuntimeRequest fields>}, "default_lakehouse": {"lakehouse_id"|"id"|"name"}|null (both spellings accepted everywhere; stored as `lakehouse_id`), "environment_id": str|null,
  "timeout_seconds": int|null, "resources": {<ResourceRequest>}, "policy": {<PolicySpec>}}}`. Cells tagged `parameters` (`cell.metadata.tags`) are the parameter cell (papermill convention).
  Fabric `%%sql`, `%%pyspark`, `%run`, `%pip` magics are recognised. Source text for cells is always a string (join list-of-lines on load).
* **lakehouse** (DATA): see §3. **sql_query** (DATA): `{"sql": str, "engine": "duckdb|spark", "lakehouse_id": str|null, "parameters": [{"name","type","default"}]}`.
* **report** (DATA): `{"title": str, "refresh_seconds": int|null, "filters": [{"name","label","type": "select|date_range|text","default","options_sql"?}],
  "tiles": [{"id","title","type": "kpi|bar|line|area|donut|table|heatmap|scatter","sql": str,"lakehouse_id": str,"engine":"duckdb","x"?,"y"?,"series"?,"format"?,"width": 1-12,"height": 1-6}]}`.
* **pipeline** (ORCH), **schedule** (ORCH), **environment**, **spark_job_definition**, **variable_set** (ITEMS), **connection** is *not* an item (see `connections` table).

## 11. Process entry points

* Worker agent: `olcf-worker run --url <control-plane-url> [--token <enrollment-token>] [--name N] [--state-dir D] [--ca-pem FILE] [--labels k=v,k=v] [--pool P] [--embedded]`
  (`python -m olcf.worker.main …`). With an existing identity in the state dir the token is optional. Exit code 0 on clean shutdown.
* Job runner (inside the runtime venv): `<runtime>/venv/bin/python -m olcf.runtime_lib.runner <workdir>/job.json` (reads job.json, writes result.json).
* Kernel host: `<runtime>/venv/bin/python -m olcf.runtime_lib.kernel_host --spec <path>` (starts an ipykernel with the OLCF bootstrap preloaded; connection file path printed on stdout as `@@OLCF@@ {"event":"kernel_ready","connection_file":"…"}`).
* Embedded worker: the CTRL module enrols a worker named after the host in-process and starts `python -m olcf.worker.main run --embedded …` as a child process (restarted if it dies).
* Control plane: `olcf serve` (TOOLING) → uvicorn `olcf.api.app:create_app` (factory). `OLCF_DATA_DIR` selects the data directory.

## 12. Runtime-side import rules (`olcf/runtime_lib/**`, `olcf/runtime/**` when used inside runtime venvs)

Code that executes inside a runtime venv (runner, bootstrap, spark helper, executor, kernel host, notebookutils shim, path shims) may import **only** the
standard library, `pyspark`, and optional packages the runtime installs (`ipykernel`, `jupyter_client`, `nbformat`, `pandas`, `pyarrow`, `deltalake`, `requests`/`httpx`), plus `olcf.core.redaction`, `olcf.core.errors`, `olcf.version`
(stdlib-only modules). Never import `fastapi`, `pydantic`, `sqlalchemy` or `olcf.core.models` there. The `olcf` package is installed into runtime venvs with `pip install --no-deps`.

## Protocol addenda (NODE)

Additive clarifications and extensions made while implementing the worker agent (`olcf.worker`), the runtime manager (`olcf.runtime`) and the runtime-side
library (`olcf.runtime_lib`). Nothing above is changed or removed; every item is optional for the controller to consume. Items marked **(hub)** describe how the
controller already behaves (verified against `olcf/compute/hub.py`) and that the agent depends on.

### N1. Frame envelope and acknowledgements
* **(hub)** The hub answers every agent frame that carries an `id` with `result{ref=<id>}` (or `error{ref}` when it refuses it, e.g. `job_log` for a job not assigned to
  the worker). The agent uses this as **delivery confirmation** for `job_log` and the terminal `job_state`; a rejected frame (`error{ref}`) is never re-sent. Frames
  without `id` are fire-and-forget (`heartbeat`, `kernel_event`, `daemon_state`, `job_event`, `result`). `hello` has an `id` and is answered by
  `result{ref, ok, data:{heartbeat_seconds, drain, server_time}}`; the agent applies `heartbeat_seconds`, adopts `drain` (the controller is authoritative) and measures clock skew from `server_time`.
* Additive, optional (not sent by the current hub, accepted by the agent): `log_ack {job_id, stream, seq}` (cumulative) and `job_ack {job_id, attempt}`. Without them the agent
  falls back to the per-frame acks above, and finally to *implicit* confirmation (a chunk is considered delivered when the same connection stayed up `implicit_log_ack_seconds`).
* **Limits**: agent → controller frames ≤ 8 MiB; controller → agent ≤ 24 MiB; `job_log.data` ≤ 128 KiB (the agent sends ≤ 64 KiB raw per chunk and never more than 120 KiB after redaction).
* Unknown frame types that carry an `id` are answered `result{ok:false, error:{code:"unsupported_command"}}`; unknown frames without `id` are ignored (forward compatibility).
* **Command-id cache**: the agent remembers the last 256 command ids with their result; a retransmitted command (same `id`) is answered from the cache and never executed twice.

### N2. `hello` and `heartbeat` additions (extra keys are ignored by older controllers)
* `hello`: `draining`, `capacity {cores, memory_mb}`, `allocated {cores, memory_mb}` (jobs + kernels + Spark Worker offers), `started_at`, `pool`, `labels`, `embedded`, `runtimes:[{id,state,verified,family,version,fingerprint,spark}]`,
  `inventory` (`hostname, fqdn, address(es), os, os_name, os_version, kernel, arch, cpu_model, cores, cores_physical, memory_mb, usable_memory_mb, disk_gb, disk_free_gb, python, java, gpus, uv, boot_time, state_dir`).
* `hello.active_jobs[]` lists running jobs **and terminal jobs whose final `job_state` is not yet confirmed** (`state` = terminal state, `undelivered: true`, `exit_code`, `finished_at`); the terminal `job_state`
  frame follows immediately after `hello`, so the controller never mistakes such an entry for a running job. `hello.kernels[]` = sessions that are alive (kernels do not survive an agent restart, see N8).
  `hello.spark_daemons[]` entries: `{role, runtime_id, pid, url, ui_url, state, detail, ui_port, host, port?, cores?, memory_mb?, master_url?, labels?}`.
* `heartbeat` (every `heartbeat_seconds`): additionally `kernels`, `draining`, `capacity`, `allocated`, `clock_skew_seconds`. Component names: `agent`, `control-plane-link`, `disk`, `clock`, `runtime:<id>`,
  `spark-master:<runtime_id>`, `spark-worker:<runtime_id>` (status `ok|degraded|down|unknown`). A runtime being installed is reported with `state: "installing"` in `runtimes` (no separate frame).

### N3. `run_job`
Payload `{spec, source{path, encoding: "utf-8"|"base64", content, sha256}, files?:[{path, content, encoding}], resources?{cores, memory_mb}}`. The agent verifies `sha256` against both `spec.source.sha256` and `source.sha256`
(`source_hash_mismatch`), writes sources below the job dir (path traversal rejected), and starts the runner in its own session. Result `data`: `{job_id, attempt, state:"running", pid, workdir}`; duplicate `(job_id, attempt)` →
`{duplicate:true, state, pid}`. Errors (`result.error.code`): `bad_request`, `conflict` (another attempt of the job is still active on the node), `draining`, `resource_exhausted` (cores/memory/`max_jobs`; `details.requested/free`),
`runtime_not_ready`, `source_hash_mismatch`, `spawn_failed`. Admission uses `payload.resources` or `spec.spark.conf["spark.driver.cores"|"spark.driver.memory"]`.
* `job_log` payload: `{job_id, attempt, stream, seq, offset, data}`; consumers dedupe on `(job_id, stream, seq)` (a given `seq` always carries the same bytes; `offset` is the raw byte offset in the node's log file).
* `job_event` payload: `{job_id, attempt, event, data, seq}`; `seq` is the raw log offset of the `@@OLCF@@` line and is the **dedupe id** (events are re-sent after a reconnect).
* `job_state` while running: `{job_id, attempt, state:"running", pid, started_at, runtime_id}`. Terminal payload: `{job_id, attempt, state, exit_code, result, failure, started_at, finished_at, runtime_id, spark?}` where
  `result = {state, exit_value, spark, artifacts[], metrics}`. The agent's own verdict wins for `timed_out` and `cancelled` (the runner's error while being stopped is kept in `failure.details.runner_failure`).
* **Inline artifacts**: `result.artifacts[]` entries carry `{name, path, media_type, size, sha256, data_b64?, truncated?}`; at most 2 MiB per artifact and 3 MiB per `job_state` are inlined, larger ones have
  `truncated:true` and can be read with `fetch_artifact`. The whole `job_state` frame is kept under 6 MiB (inline data, then an oversized `exit_value`, are shed first).
* **Cancel/timeout semantics**: `cancel_job {job_id, attempt?, grace_seconds?, force?}` → `{job_id, found, state}`; SIGTERM to the job's process group, SIGKILL after `grace_seconds` (default `kill_grace_seconds`; `force:true` = immediately).
  The deadline is `limits.timeout_seconds` (+ slack); on expiry the same escalation runs and the final state is `timed_out`. After the runner exits the agent kills any process still alive in its process group (e.g. an orphaned Spark driver JVM).
* **Secrets**: `job.json` (credentials, `storage_conf`, `secrets`, callbacks) is `0600` and shredded when the job ends; `job.public.json` is the redacted copy. Exact secret values and credential patterns are masked in shipped logs, events,
  results and kernel output; the raw `stdout.log`/`stderr.log` spool is masked in place once its output has been delivered. Jobs run with an allow-listed environment (never the agent's tokens or cloud credentials).
* Optional spec keys honoured by the runner: `spec.policy.allow_pip_install` (default false; `%pip install` of packages that are not already in the runtime is blocked), `spec.notebooks` (`{name: relative path}` for `%run`),
  `spec.limits.cell_timeout_seconds`, `spec.limits.max_output_bytes`, `spec.limits.max_rows`.

### N4. `fetch_artifact` (new command, not yet in the hub's `COMMAND_TYPES`)
`fetch_artifact {job_id, path, offset?, length?}` → `{job_id, path, size, offset, length, eof, data_b64}` (≤ 8 MiB per call). Confined to the job directory; `job.json`, `agent-job.json` and other control files are refused (`not_found`).

### N5. Kernels (`start_kernel` … `kernel_complete`)
* `start_kernel {session_id, spec, resources?, idle_timeout_seconds?, eager_spark?}` → `{session_id, pid, state, ...}`; `kernel_execute {session_id, msg_id, code, store_history?, stop_on_error?}` → `{accepted:true}` immediately;
  outputs arrive as `kernel_event {session_id, channel, msg_type, parent_msg_id, content}` where `parent_msg_id` is the controller's `msg_id`.
  `channel: "iopub"` → `msg_type` `stream|display_data|update_display_data|execute_result|execute_input|error|clear_output` (content = Jupyter message content); `channel: "shell"` → `execute_reply`;
  `channel: "status"` → `msg_type` `status` (`content.state` `starting|idle|busy|dead`, `detail`), `spark` (`application_id, ui_url, app_name, master`) or `event` (any other `@@OLCF@@` event of the kernel).
* `kernel_complete` is answered synchronously: `{session_id, status, matches, cursor_start, cursor_end, metadata}`. `kernel_interrupt` / `kernel_restart` / `kernel_shutdown` → `{session_id, ...}`; `kernel_shutdown` is idempotent (`found:false`).
* Event content is bounded to 4 MiB and scrubbed like job output. A session spec is shredded when the session ends.

### N6. Runtimes
* Install flow: stage dir → `python -m venv` → `pip install --require-hashes` (lock produced by `scripts/lock_runtime.py` / `olcf.runtime.locktool`) → `olcf` with `--no-deps` → jars resolved from Maven (sha256 pinned, checked on every start by `light_check`) and
  **linked into `pyspark/jars` with relative symlinks** (copied where symlinks are unavailable) → Spark smoke test → `lock.json` + `VERIFIED` → atomic rename. `fingerprint = sha256(manifest + lock)`.
* Manifests are Ed25519-signed (`signature{key_id, alg:"ed25519", sig}`); the trust store is configurable (`--runtime-trust-dir`, default: the bundled development key `8ea01f731883a91d`, which must be replaced for production).
  Unsigned/untrusted/modified manifests are refused (fail closed). `install_runtime {mode:"install|repair|verify|retire|rollback", runtime_manifest|runtime_id, offline_bundle?{url,sha256,size}, force?, deep?, purge?}`
  → `{runtime_id, state, verified, mode, fingerprint, detail, result, steps?}`.
* Offline bundles (`python -m olcf.runtime export|import`): tar.gz containing the signed manifest, the wheelhouse (hash-verified) and the jars; import runs `pip --no-index` and re-verifies every sha256.
* CLI: `python -m olcf.runtime {list,doctor,install,verify,repair,retire,rollback,export,import,trust,keygen,sign}`; `--with-jdk` downloads a SHA-256-verified Temurin JDK from the Adoptium API when no supported Java is installed.

### N7. Runtime-side hooks for FABRIC (`olcf.runtime_lib.notebookutils`)
* The bootstrap calls `olcf.runtime_lib.notebookutils.install(job_spec, spark)` if the module exists (return value, or the module itself, becomes `notebookutils`/`mssparkutils`); otherwise a minimal shim with `notebook.exit` is installed.
  A broken module fails the job loudly (only a missing module falls back).
* `olcf.runtime_lib.paths.resolve(path, for_="spark"|"local")` first calls `notebookutils.resolve_path(path, *, for_, spec)` (return `None` to decline); built-in rules map `Files/…`, `Tables/…`, `/lakehouse/<name|default>/…`, `olcf://<ws>/<lakehouse>/…` to the job's lakehouse URIs and reject `..` escapes.
* Exit values: `notebookutils.notebook.exit(v)` ends the run successfully; in notebooks it travels as display data with MIME `application/vnd.olcf.exit+json` (`olcf.runtime_lib.bootstrap.exit_notebook`), in scripts as an internal exception. Scripts may also set `EXIT_VALUE`.
* Script and `spark_job` sources run with `spark` (lazy), `sc`, `sqlContext`, `display`, `notebookutils`, `mssparkutils`, `PARAMETERS` and each parameter as a global predefined.

### N8. Operational notes
* Kernels are children of the agent (`JPY_PARENT_PID`) and exit with it; after an agent restart `hello.kernels` no longer lists them (jobs and Spark daemons, in contrast, are adopted from their pid/create-time records).
* `olcf-worker leave` is local only (stop agent, optionally jobs/daemons, delete identity/state); there is no controller endpoint for it, so it prints the worker id the operator must revoke.
* `websockets >= 13` (asyncio API) is required by the agent; `pyproject.toml` currently allows `>= 12`.
* Probe `kind: "storage"` against remote URIs (`abfss`, `s3`, `gs`, `http`) proves DNS/TCP/TLS **reachability** only (credentials never reach the probe); local paths are checked with real I/O.
* Diagnostics bundle (`collect_diagnostics`): allow-listed, redacted zip in `<state>/diagnostics/`, never `job.json`, identity or keys.

## Controller addenda (CTRL)

Additive clarifications made while implementing the controller (`olcf.compute`: hub, providers, scheduler, job and session services, runtimes API, topology). Nothing above is changed or removed.
Items that describe behaviour the NODE agent already depends on are marked **(agent)**.

### C1. Frames, acknowledgements and ordering
* **(agent)** Every agent frame that carries an `id` is answered by `result{ref=id, ok:true}`, or `error{ref}` when the controller refuses it (a `job_log` / `job_state` for a job that is not assigned to that worker). Frames without `id` are fire-and-forget.
* Limits: agent → controller ≤ 8 MiB, controller → agent ≤ 24 MiB, `job_log.data` ≤ 64 KiB raw per chunk (the hub tolerates up to 128 KiB after the agent's redaction).
* Commands (`run_job`, `kernel_*`, `probe`, …) **may be sent before the `result` to `hello` has been delivered**; an agent must not block its hello acknowledgement on command handling.
* After `hello`, the agent re-sends the terminal `job_state` of every job listed in `hello.active_jobs[]` with a terminal `state` and `undelivered: true`. The hub accepts a terminal state for a job the controller already considers finished (idempotent).
* Close codes (controller → agent): 4400 protocol violation, 4401 unauthenticated / revoked, 4403 forbidden (certificate CN mismatch, worker not allowed), 4408 timeout (no hello / no frames), 4409 replaced by a newer connection of the same worker.
  Session WebSocket: 4401, 4403, 4404 (unknown session), 4410 (session ended).
* `run_job` is idempotent per `(job_id, attempt)`; an agent that already runs (or already finished) that attempt answers `ok`. `job.source.sha256` is **bare hex** (no `sha256:` prefix); the runtime fingerprint keeps its prefix.
* `cancel_job {job_id, attempt?, grace_seconds?, force?}` → `{job_id, found, state}`. `start_spark_worker` accepts optional `conf` and a nullable `work_dir`.
* `probe {checks:[{kind, …}]}` → `{results:[{kind, ok, detail, ms, …}]}`. Kinds used by the controller: `tcp`, `http`, `dns`, `storage`, `disk`, `clock`, `runtime`, `spark_master`. Inventory keys the controller reads are the canonical ones of N2 (`cores`, `memory_mb`, `disk_gb`, `os`, `arch`, `hostname`, `address`).
* `install_runtime` with an offline bundle sends the descriptor `{bundle_id, url, sha256, size, filename}`; the URL is a controller URL the worker can fetch with its worker token (`GET /v1/runtimes/bundles/{bundle_id}/download`; bundles are uploaded with `POST /v1/runtimes/bundles`).
* Kernels: `start_kernel {session_id, spec, resources?, idle_timeout_seconds?, eager_spark?}`; `kernel_event {session_id, channel, msg_type, parent_msg_id, content}` as in N5. The controller's `msg_id` is `<cell_id>~<hex12>`; `parent_msg_id` is mapped back to the cell.
* `@@OLCF@@ {"event": …}` lines understood by the controller: `spark_app_started`, `executor_hosts` (hostnames hosting executors, used by the spark-smoke test), `smoke_result`, and `result` (terminal result of jobs run by a remote provider, where there is no agent: Kubernetes pods and Livy statements/batches).

### C2. Job artifacts
* Delivery channels, in order of preference: (1) **inline** `result.artifacts[].data_b64` in the terminal `job_state` (agent: ≤ 2 MiB each, ≤ 3 MiB per frame). The controller decodes, verifies `size` and `sha256` and stores the file **before** the job is finished, so `GET /v1/jobs/{id}/output` works the moment the job is terminal;
  the database keeps metadata only (`stored: true`, never the base64). (2) Entries flagged `truncated` are pulled afterwards with `fetch_artifact` (N4; now part of the hub's `COMMAND_TYPES`) in 4 MiB pages on the job pool, size- and sha256-verified, cap `compute.max_artifact_bytes` (default 512 MiB, hard cap 2 GiB);
  progress is recorded as job events `artifact_fetched` / `artifact_failed`. (3) `POST /v1/jobs/{id}/artifacts/{name}` (also `PUT`) for runners that push; authenticated by the driver worker's token or the job-scoped token (`TOKEN_SCOPES["job"]`).
* Names: `[A-Za-z0-9][A-Za-z0-9._-]{0,127}`; an invalid `name` falls back to the basename of `path`; anything else is recorded with `error` and skipped. A damaged artifact never turns a finished job into a failure. Files live under `<jobs_root>/<job_id>/artifacts/`, always resolved below that directory.
* `job_event.seq` is the dedupe id: the controller keeps the highest seen `seq` per job in `provider_handle.event_seq` and ignores repeats (the agent may re-send events after a reconnect).

### C3. Worker lifecycle, retry and recovery semantics
* A worker is `online` while heartbeats arrive, `degraded` after missed beats or failed component checks, `offline` after the heartbeat deadline or disconnect, `revoked` after revoke (its certificate serial is added to the CRL, the connection is closed with 4401, and re-enrollment under the same name is refused).
* **Worker-lost grace** (`compute.worker_lost_grace_seconds`, default 180): jobs on an offline worker stay `running`; if the worker reconnects and its `hello` lists the job, the job continues (no re-run). After the grace period, or when the `hello` does not list a job (`compute.missing_report_grace_seconds`, default 30), the attempt is closed as `provider` (infrastructure loss) and the retry rules apply.
* **Retry** (`failures.retry_decision`): only the categories `provider`, `resource`, `spark`, `storage`, `catalog` and `internal` are retryable; never `user-code`, `dependency`, policy or authorization failures, cancellation or timeouts; a job that had reached `running` is retried only if `policy.idempotent` is true **and** an agent has confirmed the old process is gone (`hello` without the job, or a terminal state); backoff `retry_backoff_seconds · 2^(attempt-1)` (base 30 s) capped at 1 h; `max_attempts = 1 + policy.retries` (default 1). RUNNING → QUEUED is a legal transition for retries (`core.states`).
* Failure categories are those of `olcf.core.errors.FailureCategory`, hyphenated on the wire (`user-code`, `dependency`, `resource`, `provider`, …); the classifier combines the runner's `failure.category`, exit codes, OOM/signal evidence and log rules.
* Recovery on start and on leader change: jobs in `accepted/validating/provisioning/dispatching` for longer than `compute.stuck_state_seconds` (default 300) are repaired (re-queued or failed with a reason); `running` jobs are reconciled against the next `hello`; remote-provider jobs are re-attached by their `provider_handle`.
* The scheduler and the loops run on the leader only (lease in the database); any API node accepts submissions and writes `queued` rows. Retention keeps ≥ 100 000 job rows indexed by (state), (workspace, created), (kind, id), (queue, id), (submitter, id); logs are files with an index row per chunk.

### C4. Spark configuration policy
* Submitted `spark.conf` keys are checked against the runtime manifest's `approved_spark_conf`. Keys the platform owns (`spark.master`, `spark.app.name`, `spark.driver.host|port|bindAddress`, `spark.blockManager.port`, `spark.submit.*`, `spark.authenticate*`, `spark.network.crypto.*`, `spark.kubernetes.*`, `spark.jars*`, `spark.*.extraClassPath`, `spark.*.extraJavaOptions`, …) are rejected with `403 policy_denied`;
  other keys that are not approved are dropped, and the choice is reported as notes of the runtime resolution and the job's `rejected_conf` / warnings. Environment variables with the prefix `OLCF_` are reserved and rejected.
* Provider configuration never contains secrets: `secret_id` references only. Config keys that look like secrets (`password`, `secret`, `token`, `key`, …) are rejected; `token_url` / `token_endpoint` are allowed. A Livy bearer comes from the referenced secret; for `fabric` the secret is `{client_secret}` (Entra client credentials against `token_url`) or `{access_token}`.

### C5. Providers
* Entry-point group `olcf.compute_providers` (name → class implementing `ComputeProvider`). Built in: `standalone`, `local`, `external` (a Spark master URL, or a Livy endpoint with `livy_url`), `kubernetes`, `fabric` (Fabric Livy API).
* `provider: auto` selects `standalone` when ≥ 2 eligible nodes are online with the runtime ready, else `local`. A cluster is per runtime id; masters and workers are never shared between runtimes.
* **Standalone capacity sharing**: nodes offer cores/memory to one Spark Worker per runtime; when several runtimes want the same node, `desired_workers` splits capacity (`_runtimes_sharing`), idle workers of other runtimes are shrunk or stopped to make room (`_make_room`, `_shrink_to_share`), never while they host a running application.
* Remote providers (Kubernetes, Livy) have no agent: the controller polls state and logs (`_monitor_remote`), scrapes the `@@OLCF@@` result line, and cancels by deleting the pod / killing the Livy session or batch.
* Kubernetes: manifests (driver pod, headless Service, Secret with the job spec) are built by the provider and applied through a `KubeClient` (in-cluster service account or kubeconfig via `kubectl`); `spark-submit --deploy-mode cluster` is used when `spark_submit` mode is configured.

### C6. Sessions
* REST additions to 4.4: `POST /v1/sessions/{id}/execute {cell_id, code}` (202), `POST /v1/sessions/{id}/complete {code, cursor}` (200), `GET /v1/sessions/{id}/events?after_seq=&limit=&wait=&replay=` (long-poll relay for non-WebSocket clients; `next_seq` is the cursor).
* WebSocket: on connect the server sends `status`, optional `spark`, the replay (the last `compute.session_replay_events` = 500 outputs, each tagged `"replay": true`), then `{"type":"replay_done","count","seq"}`, then live messages. Extra client op `{"op":"ping"}`. Messages are also persisted in `session_events` (table in `compute/schema.py`) so any API node can relay them.
* Session states: `starting → idle ⇄ busy → dead`; a deleted or culled session also ends as `dead` with `state_reason`. Limits: `compute.max_sessions_per_user` (5), idle culling `compute.session_idle_timeout_seconds` (3600).
* `ComputeService.run_spark_sql(principal, workspace_id, sql, *, lakehouse=None, limit=1000, timeout=120)` returns `{columns, rows, row_count, truncated, elapsed_ms, session_id, application_id}`. It requires `sql.execute` (and `sql.write` when the statement is not plainly read-only) plus `lakehouse.read`;
  rows are capped at `limit` (≤ 100 000) and the statement is interrupted after `timeout`. It runs in a pooled hidden session of purpose `sql` (idle cull `compute.sql_session_idle_timeout_seconds`, 900). Caveat: result columns with duplicate names are disambiguated by position in `rows` (JSON objects cannot hold duplicates).

### C7. Embedded worker
* When `settings.compute.embedded_worker` is enabled (default for a single-node install) the controller supervises a real agent child (`python -m olcf.worker.main run --embedded`). Its worker token is passed in `$OLCF_WORKER_TOKEN` (never on the command line) and `PYTHONPATH` entries are made absolute. With `--embedded` the agent stops its children when it exits,
  so **jobs running on the embedded worker do not survive an API restart** (they are retried according to the retry rules); external agents adopt their jobs across restarts.

### C8. Observations on the foundation and additive foundation edits
* Additive edits: `olcf.core.ids.PREFIXES` gained `que, clu, prv, sap, att, cmd, bun, enr`; `olcf.core.states` allows RUNNING → QUEUED; `olcf.security.rbac.TOKEN_SCOPES["job"]`.
* RBAC notes: `runtime.read` and `compute.read` are org-level permissions in practice; a workspace-scoped Viewer cannot list `/v1/providers` or `/v1/runtimes`. Operators have `compute.read` and `compute.test`, not `compute.manage` / `runtime.manage`.
* `api/middleware.py` appends `Cache-Control: no-store` and `X-Content-Type-Options` to every `/v1` response; endpoints that set those headers themselves therefore emit duplicated values (harmless; clients should treat them as lists).

### C9. Verification status
* Verified in the test suite (`tests/compute`, SQLite; PostgreSQL when `OLCF_TEST_POSTGRES_URL` is set): hub, enrollment, revoke/CRL, reconcile, scheduler and 500-job ordering, standalone provider command sequences, jobs API/RBAC/idempotency, sessions, runtimes API, topology/tests, embedded worker and the controller against the **production** agent and runner (`test_real_agent.py`, with the NODE package's test runtime; no Spark).
* Contract-tested (test servers for external systems): Kubernetes (`FakeKube`, `kubectl` / `spark-submit` stubs), Livy and Fabric (`FakeLivy` with the Fabric path shape and an Entra token endpoint). PostgreSQL DDL is compiled against the PostgreSQL dialect; runs against a server need `OLCF_TEST_POSTGRES_URL`.

## Items addenda (ITEMS)

Package `olcf.notebooks` (module path `olcf.notebooks.module`, installed after `compute`). It owns the item types `notebook`, `environment`, `spark_job_definition` and `variable_set`, Git sync and deployment promotion.

### I1. Services (`ctx.service(name)`)
* `notebooks` (`NotebookService`): parse / serialise ipynb, percent `.py` and Fabric `fabricGitSource`; `normalise` to section 10; parameter detection and typed, injection-safe injection; per-cell diff (outputs ignored); `build_submission` for `run`.
* `environments` (`EnvironmentService`): `resolve(ref=None, *, lock=None, version=None, workspace_id=None) -> EnvironmentSpec` (a dict, see I2); `ref` is `itm_...` or `itm_...@<publication>`; a name is accepted together with `workspace_id`. This is what CTRL's `RuntimeCatalog.resolve` calls. Unpublished environments with dependencies raise `Unsupported` (422).
* `spark_jobs` (`SparkJobService`), `git` (`GitService`), `deployments` (`DeploymentService`).
* `variables` (`VariableService`) for ORCH and run endpoints: `resolve(ref, stage=None, *, workspace_id=None, principal=None, check_secrets=True) -> {id, name, stage, stages, values, secrets, variables}` and `substitute(obj, ref, stage=None, ...)` (`${var:name}`; a bare placeholder keeps its JSON type; a secret variable is never substituted into text and fails closed).
  Stage precedence: explicit `stage` > the workspace's active stage (`workspaces.settings.stage`, `GET/PUT /v1/workspaces/{ws}/stage`) > the set's `default_stage`. `values` never contains secrets; `secrets` maps variable name to secret id for `secret_refs`.

### I2. `EnvironmentSpec` and the layer block (CTRL / NODE)
* `EnvironmentSpec = {item_id, name, workspace_id, version, published, lock_hash, runtime, spark_conf, variables, requirements, pip_policy, dependency_lock, layer}`. `dependency_lock` is the immutable lock (`schema olcf.envlock/1`: pinned `python.packages[].{name,version,hashes,artifacts}`, `jars`, `index`, `spark_conf`, `variables`, `lock_hash` = sha256 over the canonical content, so tampering changes the hash); `layer` is `null` when there is nothing to install.
* `layer = {key, python, requirements, pip, jars}`: `requirements` is a hash-pinned requirements file (`--require-hashes --no-deps`), `key` is `olcf.notebooks.layer.layer_key` (same dependency hash, same layer, shared per node). The node installs it once into `<cache>/<key>` with `olcf.notebooks.layer.ensure_layer` (stdlib only, safe in runtime venvs) and puts the directory on `PYTHONPATH` in front of the runtime's site-packages. Private-index credentials are **not** part of the block (they must exist on the node).
* Publication is append-only: `POST /v1/environments/{id}/publish` creates publication N+1 only when the resolved lock differs; locks live in table `environment_locks` (migration stream `notebooks`). Policy: `settings.security.allow_public_package_install` / `allowed_package_indexes` can only be tightened by the admin KV keys below; native packages (sdists that need compilation) are rejected unless `environments.allow_sdist`.
* Notebook `%pip` policy: `pip_policy` is `block` | `declared` (admin default `environments.pip_policy_default`) | `allow`, capped by `environments.pip_policy_max`; enforced by the control-plane preflight of `notebook run` / `session`; the node's magics default to block.

### I3. Admin settings (KV, `PUT /v1/admin/settings/{key}`)
`environments.wheelhouses` (approved offline wheelhouse directories), `environments.allowed_package_indexes`, `environments.allow_public_package_install`, `environments.allow_sdist`, `environments.maven_repositories`, `environments.pip_policy_default`, `environments.pip_policy_max`; `git.allow_file_urls` (default false; development / tests), `git.allow_insecure_http`, `git.allowed_hosts`, `git.timeout_seconds`.

### I4. Git sync and promotion
* Repository layout = the workspace export layout (`items/<name>.<type>/{.platform,item.json,<content>}`, `connections/<name>.connection.json`), below the connection's `folder`. Notebook content format: `ipynb` | `py` | `fabric` (per connection). Connections are metadata only (no secret, no secret id); they are never deleted by a pull.
* State lives in `workspaces.git` (`repo_url, branch, folder, connection, notebook_format, synced_commit`) and is re-validated on every use because the column can be PATCHed. A bare repository per workspace is kept in `<data_dir>/git/<ws>.git`. Credentials come from a `git` connection (`config.username`, secret = token or `{username,password}`) and reach git only through `GIT_ASKPASS` files (0600, deleted afterwards).
* Three-way comparison per unit (base = tree at `synced_commit`, local = workspace, remote = branch tip): `outgoing`, `incoming`, `conflict`. `commit` refuses when anything is incoming or conflicting (409), scans every outgoing file and the message for secrets (422; `olcf: allow-secret` marks an intentional dummy) and pushes without force. `pull` needs `resolution` / `resolutions` (`ours|theirs`) for conflicts and applies nothing if any unit is invalid.
* Promotion (`/v1/deployment-pipelines`, `POST .../deploy`): forward only, `allow_skip` for jumps; items are matched by `logicalId`, then name; item and connection references are remapped; a connection without a counterpart is a problem; the target workspace gets `settings.stage` so variable-set stage overrides apply; `dry_run` plans only.

### I5. Observations / known limits
* CTRL passes no workspace to `environments.resolve` and does not forward `policy.allow_pip_install`; NODE does not yet read `runtime.dependency_lock` / `runtime.environment.layer` (use `ensure_layer`). `spark.jars/packages` are passed as `[]` by CTRL; environment `variables` are merged into `env` by the notebook / Spark job run endpoints. `SessionCreate` has no environment field, so environment variables are not applied to interactive sessions.
* `WorkspaceService.import_zip` returns an `id_mapping` but does not rewrite item ids inside definitions of other owners' types (for example a report's `lakehouse_id` keeps pointing at the source lakehouse). Git pull and promotion do remap.
* There is no environment / git specific permission in the RBAC catalog; `item.*`, `workspace.update|export|import`, `connection.use`, `job.submit` and `session.use` are reused.
* Verified with real tools: `git` (local bare repositories, `git http-backend` with Basic auth), `pip` (locally built wheelhouse, `--no-index --find-links`), `nbformat`. Not covered: Spark / JVM, Maven Central, public package indexes, ssh remotes (unsupported by design).

## Orchestration and observability addenda (ORCH)

Owners: `olcf/pipelines/**`, `olcf/observability/**`, `tests/orch/**`, `schemas/pipeline-v1.json`, `schemas/fabric-import-support.json` (generated by `python -m olcf.pipelines.gen_schemas`; `--check` runs in `tests/orch/test_validation.py`).

### O1. Services registered on `ctx`
* `pipelines` (`PipelineService`): `run(principal, pipeline_item_id, parameters, trigger)`, `get_run`, `activities`, `graph`, `cancel`, `resume`, `rerun`, `validate`, `stats`. `triggers` (`TriggerService`), `webhooks` (`WebhookService`), `metrics` (`MetricsRegistry`: `counter/gauge/histogram`, `record_sample`), `alerts` (`AlertService`), `otlp` (exporter), `tracer`.
* Item types `pipeline` and `schedule` validate through `ctx.register_item_hooks`. Definition schema id `olcf.pipeline/v1` (`typeProperties`, `dependsOn: [{activity, conditions}]`). Unparseable text gives `{"valid": false, "errors": [{"code": "parse_error"}]}` from `POST /v1/pipelines/validate`.
* Run state lives in the database (lease-token CAS); only the leader dispatches. Crash recovery adopts runs whose lease expired.

### O2. Routes (all under `/v1`, bearer auth unless noted)
* `/pipelines`: `GET /activity-types`, `GET /schema`, `POST /validate`, `POST /import-fabric` (`strict` option), `GET /{id}/definition`, `POST|GET /{id}/runs`, `GET /{id}/triggers`. Run requests accept `Idempotency-Key`.
* `/pipeline-runs`: list, `GET /{id}`, `/graph` (nodes carry `child_runs`; `critical_path_ms`), `/activities[/{aid}]`, `/children`, `POST /{id}/cancel|resume|rerun`.
* `/triggers` (manual, cron, webhook, storage, dependency) CRUD plus `enable|disable|pause|resume|run-now|rotate-secret`, `/{id}/runs`, `/{id}/next`; `/schedules` for `schedule` items (`POST /preview` returns DST-aware fire times).
* Public (no bearer; the credential is in the request): `POST /v1/hooks/pipelines/{trigger_id}` (HMAC `X-OLCF-Signature` or bearer secret, replay window, 413/429 limits) and `POST /v1/hooks/pipelines/callbacks/{activity_run_id}/{token}` (completes a `webhook_wait` activity; the `callback_url` is shown only to principals holding `pipeline.run`).
* `/webhooks`: `GET /event-types`, CRUD (ETag / `If-Match`), `POST /{id}/test|rotate-secret`, `GET /{id}/deliveries`, `GET /deliveries/{id}`, `POST /deliveries/{id}/redeliver`. Needs `webhook.manage` in the subscription's workspace (organisation-wide subscriptions need an organisation-level grant). The signing secret is returned once.
* `/alerts`: `rule-types`, `rules` CRUD (+ `/test`, `/silence`), `channels` CRUD (+ `/test`), alert list/get, `/{id}/ack|silence`, `POST /evaluate`. Writes need `alert.manage`.
* `/metrics` (Prometheus text 0.0.4, requires `platform.admin`), `/metrics/series`, `/metrics/names`, `/metrics/snapshot`, `POST /metrics/otlp/test`.

### O3. Activity plugin API
Entry-point group `olcf.pipeline_activities`: each entry resolves to an `Activity` subclass (`olcf.pipelines.activities.base`) with `type`, `schema`, `start`, `poll`, `cancel`, optional `timeout_for(properties)` and `public_state(state) -> dict` (the part of the persisted state visible to run readers). Plugins that clash with built-ins or fail the schema check are rejected and listed in the health check labels. Built-ins: control flow (`if`, `switch`, `for_each`, `until`, `wait`, `webhook_wait`, `fail`, `set_variable`, `append_variable`, `execute_pipeline`), `http`, `sql` (DuckDB, read only), `copy` (resumable via manifest; Delta and Parquet sinks), and job-backed activities (`script`, `notebook`, `spark_job_definition`) that submit through `ComputeService`. After a cancel an activity that does not raise `ActivityCancelled` is force-concluded after `cancel_grace_seconds` (60).

### O4. Webhook delivery contract (spec 24)
`POST` JSON envelope `{id, type, ts, workspace_id, subject, data}`; headers `X-OLCF-Event`, `-Event-Id`, `-Delivery`, `-Attempt`, `-Timestamp` and `X-OLCF-Signature: sha256=HMAC_SHA256(secret, "<timestamp>.<body>")`. Success is any 2xx; 408, 425, 429, 5xx and network errors retry with exponential backoff (`Retry-After` honoured); other 4xx are dead; 410 from a real delivery disables the subscription. States: `pending|retrying|delivering|succeeded|dead`. Redirects are not followed; every attempt re-checks SSRF rules. Events come from the `events` table; `AuditEventBridge` maps audit records to `security.*` (for example `security.login_failed`) and `fabric.*`. A subscription sees only events created after it.

### O5. Alerts (spec 28)
Rule types: `failed_jobs`, `failed_pipelines`, `queue_saturation`, `worker_loss`, `disk_pressure`, `runtime_mismatch`, `auth_expiry`, `control_plane_health`, `fabric_handshake_stale`. Built-in rules are seeded (`builtin_key`; not deletable). States `pending|firing|resolved`, de-duplicated per (rule, instance); notifications go to `log`, signed `webhook` and `email` channels. Failure categories use the CTRL spelling (`user-code`).

### O6. Observability
* Series are sampled by the leader (`observability.sample_interval_seconds`), compacted and purged by retention. OTLP/HTTP JSON export of metrics and traces is optional (`observability.otlp.*`; headers come from a secret named in `observability.otlp.headers_secret`; the endpoint passes the SSRF guard).
* `ApiMetricsMiddleware` is installed from the lifespan of an empty router returned by the observability module because modules cannot wrap the application. Labels use the full route template; `route_label` reconstructs the prefix because `include_router` leaves only the router-relative template on `scope["route"]`.
* Correlation helpers: `olcf.observability.correlation` (`X-Correlation-Id`, W3C `traceparent`, log record fields).
* Health checks `pipelines-engine` and `metrics` provide quick and deep modes with per-step results.

### O7. Settings keys
`pipelines.max_active_runs`, `pipelines.min_schedule_interval_seconds`, `webhooks.backoff_base_seconds|backoff_max_seconds|disable_after_dead|retention_days`, `alerts.history_days`, `alerts.smtp.*` (`host|port|ssl|starttls|username|password_secret|from|timeout_seconds`), `observability.sample_interval_seconds`, `observability.otlp.*` (`endpoint|metrics|traces|interval_seconds|timeout_seconds|trace_sample_rate|headers_secret`), `network.ssrf.allow|deny|block_private`.

### O8. Known limits
Compute-backed activities are tested against CTRL's test worker agent. No `psycopg` is installed in the shared `.venv`; PostgreSQL runs of `tests/orch` need `OLCF_TEST_POSTGRES_URL` and a driver on `PYTHONPATH`.

## Integration addenda (INTEG)

Decisions taken while joining the seven packages. They refine (never contradict) the sections above; where a section says otherwise, this one wins.

### I1. Item hooks: `on_update` and `remap_ids`
* `ctx.register_item_hooks(type, ...)` accepts two more hooks. `on_update(principal, old_row, new_row)` runs inside `ItemService.update` **before** anything is written; raising (for example `Forbidden`) aborts the update. The lakehouse hook uses it: changing `definition.connection` / the storage root needs `connection.use` (or `connection.manage`) on the new connection, so an editor cannot re-point a lakehouse at a credential he may not use.
* `remap_ids(definition, id_map, name_map=None) -> definition` is a **pure** function (return a copy) that rewrites every reference to another item or connection (`olcf.core.refs`: `remap_exact` for generic definitions, `remap_paths` for known locations such as `tiles[].lakehouse_id`). Registered for `lakehouse` (connection), `report`, `sql_query`, `notebook` (settings: default lakehouse, environment, variable set; the source `workspace_id` is dropped when the lakehouse was re-pointed), `environment`, `spark_job_definition`, `pipeline` (activity targets), `schedule`. Types without a hook fall back to `remap_exact`.
* `WorkspaceService.import_zip` imports connections first, then items in dependency order (`lakehouse, environment, variable_set, notebook, spark_job_definition, sql_query, pipeline, report, schedule`), builds the old-id to new-id map as it goes, and finishes with a fix-up pass that rewrites the definitions that referred to items created later (`items.update(... comment="import: resolve references")`). Git pull and promotion use the same hooks.

### I2. Default lakehouse spelling
`default_lakehouse` is `{"id": ...}` or `{"name": ...}` (workspace optional) everywhere. `lakehouse_id` is accepted as an alias at the API boundary (`LakehouseRef` model) and normalised to `id`; stored definitions and job specs only carry `id`.

### I3. RBAC: `runtime.read`
`runtime.read` is a **baseline** permission (`olcf.security.rbac.BASELINE_PERMISSIONS`): any authenticated principal with a role binding at any scope holds it (the job / session / notebook forms need the runtime catalog). A principal without any binding gets nothing. `GET /v1/providers` and `/v1/providers/{name}` return a **slim** view (`id`, `name`, `type`, `enabled`, `is_default`, `etag`, `live_verified`, `execution`, `capabilities`, `builtin`) to holders of only `runtime.read`; the full record (config, endpoints) still needs `compute.read`/`compute.manage`.

### I4. Pip policy and `allow_pip_install`
* `PolicySpec.allow_pip_install` (job policy) is forwarded by the planner to the runner (`%pip` / `pip install` inside the job). Notebook items: `pip_policy: allow` maps to `true`, `declared` and `block` to `false` (declared packages arrive through the environment layer). Fabric submissions: `true` only when the Fabric integration's default policy **and** the capsule both allow it. Interactive sessions take `allow_pip_install` and `environment_id` on `SessionCreate`.
* A job that names an explicit `runtime.dependency_lock` without an environment is validated like a published environment lock (hashes required, indexes checked against the administrator's policy) and turned into a layer by the node.

### I5. Per-job dependency layers (NODE consumer of `olcf.notebooks.layer`)
* The controller resolves `runtime.environment` (item id, name, version, `lock_hash`, `variables`, `spark_conf`, and the `layer` block: `key`, `packages` with hashes, `pip` index/find-links, `jars{resolved,files}`) into the job spec. Environment `variables` become job environment variables, environment `spark_conf` is merged below the job's own conf, environment and runtime-lock jars are merged into `spark.jars` / `spark.jars.packages`.
* The agent (`olcf.worker.layers.LayerManager`) builds the layer once per node and `key` below `<state>/layers/<key with ':' replaced by '-'>` (`pip install --require-hashes --no-deps --target`, flock-serialised, renamed into place; a layer that exists needs no network). Jars are content-addressed in `<state>/layers/_jars/<sha256>.jar` and sha256-verified (fail closed). Uploaded environment jars (`jars.files`) are fetched from `GET /v1/environments/jars/{sha256}` with the worker credentials.
* Node config (`agent.toml` / flags): `layer_offline` (never contact an index; cache + wheelhouse `<state>/layers/wheelhouse` only), `layer_allowed_indexes` (prefixes; any other index in a lock is refused with `index_not_allowed`), `layer_timeout_seconds`; `pip_index_url` / `pip_find_links` (a node-local mirror replaces the index named in the lock; hashes still pin every artifact).
* The layer directory travels as `spec.runtime.layer = {key, path, jars, lock_hash, environment}`. `runtime_lib.bootstrap.apply_layer` puts `path` in front of `sys.path` in the runner (driver), the kernel host gets it on `PYTHONPATH`; Spark Python workers on the same node inherit `PYTHONPATH`. **Limit:** the layer exists only on the node that built it, so Python packages of a layer are not importable inside executors on *other* nodes (jars are shipped by Spark). Put such dependencies in the runtime or use a node-local mirror on every node.
* `result.json` carries `"dependencies": {runtime_id, runtime_fingerprint, layer_key, environment_lock_hash, environment, jars}`; the job record's `result` shows it.
* Parameter values are injected as self-contained Python literals (`runtime_lib.executor.py_literal`, same rules as `olcf.notebooks.params.py_literal`): non-finite floats become `float('nan')` / `float('inf')` (bare `nan` / `inf` are names, not literals) and anything outside the JSON value set is refused instead of being turned into code. Notebook kernels start with `MPLBACKEND=module://matplotlib_inline.backend_inline` (set by the agent), so figures end up as outputs of the executed notebook.

### I6. Planner `auto` provider and single-node defaults
* `compute.default_provider = auto` (default): a Spark job is placed on **standalone** when some node can host master + worker + driver within its reservations (`standalone_unfit_reason(node)` is `None`: at least 2 usable cores and 2048 MB usable after `compute.reserve_cores` / `reserve_memory_fraction`), otherwise on **local** with the reason in the placement note (job timeline `provisioning` event: `data.note`). An explicit `placement.provider` is never second-guessed.
* A request that the submitter did not size (`resources` absent) is *flexible*: driver/executor defaults are scaled to the machine (`local_default_sizing`: about half of the usable cores up to 4, a quarter of the memory up to 4 GiB) and the job is never refused because the machine is small. Explicit requests keep their numbers and are checked against the cluster as before. Simulated 2/4/8/16-core inventories are covered by `tests/compute/test_compute_planner.py`.
* **Advertised capacity:** an agent started with `max_cores` / `max_memory_mb` sends `capacity` in `hello`; the hub stores it in `inventory.capacity` and the planner (`NodeInfo.sched_cores/sched_memory_mb`, used by `node_views` and the standalone provider's Spark worker sizing) plans with `min(inventory, capacity)`. The topology still reports the machine's inventory. Without this, a Spark worker sized from the machine's RAM exhausted the agent's allowance and the driver launch failed with "not enough free resources".
* Spark Standalone: the master UI JSON endpoint is `/json/` (Spark 4 answers `/json` with a 302). All control-plane and agent probes of cluster-internal addresses use clients that ignore `HTTP(S)_PROXY` (`trust_env=False`; `worker/daemons.py` uses a no-proxy opener): a site proxy must never sit between nodes of the same cluster.

### I7. Host binaries: `olcf.core.hostbin` (stdlib only)
`find_real_binary(name, path=None, extra=None)` is the only way OLCF looks for `java`, `git`, `python`, `python3`, `pip`: on macOS it rejects `/usr/bin/{java,git,python3,python,...}` installer shims unless `xcode-select -p` succeeds (java: unless `/usr/libexec/java_home` reports a JDK, and then that JDK is used), never executes a shim, and continues with the next `PATH` entry. `explain_missing(name)` returns the remediation text. Platform and tool locations are module-level so tests exercise macOS behaviour with `PATH` stubs (`tests/unit/test_hostbin.py`).

### I8. Git availability
When no real `git` exists every Git endpoint of the notebooks module answers **503 `git_unavailable`** with remediation (never 500, git is never run to find out); `GET /v1/workspaces/{ws}/git/capabilities` returns `{available, path, version, backend, remediation, platform, connected}` and works without git. There is no pure-Python fallback (dulwich) yet: **not implemented**.

### I9. Known limits discovered by the product test
* The logical catalog (`list_tables`, search, lineage) learns about tables that a notebook or job wrote directly into `Tables/` on the next `POST /v1/lakehouses/{id}/refresh` (or when `describe_table` / `sql` touches them); `run_sql` scans storage and sees them at once.
* Spark executors of one test machine all report `127.0.0.1` as their host, so the application record cannot tell the two agents apart there; the product test proves the distribution through the executors' work directories (each agent's state directory).
* Delta on Spark needs the Delta jars; the sandbox cannot download them, so the Spark + Delta combination is verified only by `scripts/verify_spark_delta.py` on a networked machine. Delta itself is exercised through delta-rs and DuckDB.
* `tests/integration/test_product_e2e.py` is the whole-product test (see its module docstring). Run it with `PYTHONPATH=src pytest tests/integration -p no:cacheprovider`; the Spark steps take `/tmp/olcf-spark.lock` through the `spark_lock` fixture. It needs the cached runtime (`tests/node/runtime_fixture.py`) and downloads wheels once into `/tmp/olcf-test-wheelhouse-e2e`.

## Security and operations addenda (SECOPS)

Decisions of the security / operations package (`olcf.ops`, `olcf.security.federation*`, `tests/security`, `tests/ops`). They refine the sections above.

### S1. Services, routes and permissions (module `olcf.ops.module`, schema stream `ops`)
* Services: `backup`, `updates`, `diagnostics`, `support`, `retention` (`ctx.service(name)`). Routes (all under `/v1`): `/admin/backup*` (targets, config, import, restore, restore-test, validate, prerequisites, download, retention), `/admin/updates*` (check, import, stage, preflight, apply, cancel, rollback, trust, maintenance), `/admin/diagnostics`, `/admin/support-bundle*`, `/admin/retention*` (run, runs, audit).
* Permissions: `admin.backup` (PlatformAdmin only), `admin.update` and `admin.diagnostics` (PlatformAdmin, RuntimeAdmin), `admin.settings` (retention configuration, PlatformAdmin), `audit.read` (PlatformAdmin, SecurityAdmin). `tests/security/test_authorization.py` proves for every route which role gets 403.
* Settings: `retention.enabled|interval_hours|event_days|audit_days|audit_archive|metric_days|job_history_days|log_days|recycle_bin_days|token_grace_days|operations_days|stale_operation_hours|support_bundle_days|run_history_days` (bounded validators; `audit_days` has a floor), `identity.provider_test_interval_seconds`.
* The audit log is purged only after the records were written to `archive/audit/*.jsonl.gz` (unless `retention.audit_archive` is 0) and a hash anchor was stored; `verify_audit_chain` (`olcf.ops.retention`) also detects prefix truncation, which the stock `AuditLog.verify` cannot see.

### S2. Backup and disaster recovery
* A backup is one `.olcfbak` file (database rows + optionally keys), encrypted with the master key or with a passphrase-derived key (scrypt). To rebuild a lost control plane you need: the backup file, its passphrase (or the master key for `master`-mode backups), an empty database of the same or newer OLCF version, the master key to decrypt secret rows, and the PKI / signing keys if workers are to keep trusting the platform (`include_keys`, otherwise re-enrol workers). Lakehouse data is not part of the backup. `GET /admin/backup/{id}/prerequisites` lists what a given file needs; `restore-test` restores into a throw-away SQLite database and runs a functional check without touching the live system.
* `POST /admin/backup/{id}/restore` needs `confirm: "RESTORE"`, validates the file, takes a `pre-restore` backup first (and refuses if that fails), replaces the database online, and asks for a restart of all nodes.

### S3. Updates (honest story)
* A running Python service cannot replace its own code. `apply` verifies the signed manifest (Ed25519, trusted keys under `/admin/updates/trust`, channel, expiry, anti-rollback), runs the preflight (artifact hashes, compatibility, disk, migration dry-run in a subprocess), takes a `pre-update` backup, drains compute and writes `<data_dir>/updates/handoff.json`; the code swap is `olcf upgrade` / `python -m olcf.ops.upgrade apply` (versioned layout, the previous version stays for `--rollback`) or a new image / `helm upgrade`. pip/uv installs: `uv pip install -U olcf && olcf db migrate`. A configured `restart_command` is the only thing that restarts a node automatically.
* There is no flag or endpoint that applies an unverified update. Rollback after the hand-off restores the pre-update backup (migrations are not reversible in general).

### S4. TLS and mTLS
* `olcf.ops.tlsconfig.build_uvicorn_ssl_kwargs(settings)` returns the uvicorn TLS arguments (certificate, key, CA bundle, `ssl_cert_reqs`). Stock uvicorn does not expose the peer certificate to the application; `olcf.ops.tlsproto` installs a protocol shim that fills `scope["extensions"]["tls"]["client_cert_chain"]`. The worker hub additionally verifies the certificate against the platform CA, binds the CN to the worker id and checks `pki.revoked`.
* Behind a TLS-terminating proxy either use L4 passthrough for the worker port, or set `server.trusted_proxies >= 1` and let the proxy forward the verified certificate in `X-Client-Cert` (it must strip a client-supplied header). A header carries no proof of possession of the private key: with `trusted_proxies > 0` the nodes must be reachable **only** through the proxy.

### S5. No-escalation rule (identity)
A principal may only create, change, bind or issue tokens for roles it **covers** (`Principal.covers_role`) and only for users it **outranks** (`Principal.outranks`). In particular SecurityAdmin cannot create or modify a PlatformAdmin and cannot grant a role containing permissions it does not hold itself. Org-scoped SecurityAdmin / RuntimeAdmin / Operator stay cross-tenant **by design**.

### S6. Streams re-validate credentials
Server-sent events and WebSockets use `olcf.api.deps.StreamGuard(ctx, request|None, principal, interval=10, token=None)`; `await guard.current()` re-authenticates at most every `interval` seconds and returns `None` when the token expired or was revoked or the account was disabled. Generators end with `event: end` (SSE) or the socket closes with 4401 / 4403 / 4404 (session WebSocket) and visibility is recomputed from the fresh principal on every iteration. New stream endpoints must do the same.

### S7. Host-path capabilities
* A `local` connection grants a host directory; creating or changing one (including through a workspace import archive) needs the **org-level** `connection.manage`.
* A lakehouse root (`root_uri`, or the root of its connection) must be inside `<storage>/lakehouses/<workspace slug>`, the root of a local connection the workspace references, `storage.local_allowed_roots`, or inside the root of an existing lakehouse where the principal has `lakehouse.read` and `lakehouse.write`; anything else is a 422 (`olcf.storage.lakehouse.check_local_scope`).
* References between objects are authorised in the *target* workspace: a job cannot name a secret, item, lakehouse, artifact or `parent_run_id` of another workspace (`tests/security/test_references.py`).

### S8. High availability (two or more control-plane processes)
* Requirements: PostgreSQL (SQLite is single node), the same `OLCF_MASTER_KEY` **and** `OLCF_JWT_SIGNING_KEY` on every node (otherwise tokens and stored secrets are not interchangeable).
* Exactly one node holds the lease (`LeaderElector`, ttl 15 s, renewed every ttl/3 by `LeaderLoop`). `is_leader()` also requires that the local monotonic clock is still before `acquired_at + ttl`, so a partitioned node stops acting as leader before another one can take over. Measured on PostgreSQL 16: SIGKILL of the leader, takeover after 15.9 s; graceful stop, hand-over after 3.8 s (`tests/ops/test_ha.py`).
* Load balancer: any node serves reads and writes; health check `GET /v1/health/ready`; do not buffer `/v1/events/stream`, job log streams and session WebSockets, and keep idle timeouts above 60 s; set `server.trusted_proxies` to the number of proxy hops so `X-Forwarded-For` is honoured (the login throttle keys on the client address). The rate limiter is per node: the effective limit is `limit x nodes`.
* A role change is visible on every node within the Authorizer binding-cache ttl (3 s); rotating the JWT signing key needs a restart of the other nodes unless the key comes from the shared environment.

## Fix addenda (FIX)
Defect fixes after integration. Each item names the behaviour that is now contractual and the regression test that pins it.

### F-A. Spark smoke test runs on the real cluster (`compute/topology.py`)
* `SMOKE_SCRIPT` takes the normal runtime bootstrap session (`olcf.runtime_lib.spark`), so the master is whatever the planner chose: `spark://...` on a standalone cluster, `local[n]` when the planner fell back to local mode (never a private `local[*]` session built by the script).
* The work is a `mapPartitionsWithIndex` over `max(partitions, 2 x total cores)` partitions (`total cores` = `sc.defaultParallelism` once `SMOKE_EXECUTORS` executors registered or the wait ended). Every task reports `socket.gethostname()`, pid, working directory, partition and its executor id; executors started by an agent's Worker daemon also carry `OLCF_WORKER_ID` (exported by `worker/daemons.py`).
* `POST /v1/compute/spark-smoke` returns, besides the old fields, `master_is_cluster`, `partitions`, `total_cores`, `executors` (what the tasks reported), `executors_registered`, `single_address`, `wait_reason` (only while the job has not started) and `nodes_with_executors` (`worker_id`, `name`, `hostname`, `address`, `cores`, `tasks`, `executors`). Executor-to-node mapping, most precise first: the node id the executor reports, the master's `/json/` worker registrations plus the application's executor list (matched through the Spark worker id / work dir of each agent), then task host names. `distributed` is true only when the master is a cluster **and** at least two agents hosted executors; `single_address` is true when those agents share one address (one machine) and the `note` says so. A local fallback gives `distributed: false` with the reason (`note`).
* Test: `tests/compute/test_spark_smoke_real.py` (spark-marked; two production agents, real JVMs; sets `compute.reserve_cores=0` through the live setting because the sandbox has 2 CPUs).

### F-B. `compute.reserve_cores` is live
`ComputeRegistry.reserve_cores` reads the setting through the settings service with a 2 s cache (`RESERVE_CORES_CACHE_SECONDS`); the planner (`node_views`) and the standalone provider use it, so `PUT /v1/admin/settings/compute.reserve_cores` takes effect within seconds without a restart. A validator accepts whole numbers 0-64. Test: `tests/compute/test_fix_reserve_and_detail.py`.

### F-C. Cluster detail is authoritative
`StandaloneProvider._set_state` stores the detail with the state (the early return only applies when both are equal) and `refresh_cluster` clears it unless the cluster is degraded, so "0/? workers alive" can no longer survive a refresh that saw `alive_workers: 3`. Same test file.

### F-D. Forced password change is enforced by the server
* `UserCreate.must_change_password` (default `false`); rejected for service accounts (400). `sdk.identity.create_user(..., must_change_password=)`, `olcf user create --must-change-password`, and the UI "Require a new password at first sign-in" box (ticked by default for accounts created in the UI) use it.
* Enforcement lives in `olcf.api.deps.authenticate`: a **local login JWT** of an account whose `principals.must_change_password` is set is refused with 403 `password_change_required` by every caller (REST, SSE/WebSocket guards, the Fabric adapter, the Spark UI proxy, MCP) unless the call is `POST /v1/identity/password`. The decision is taken from the account's current state, not from a claim: tokens issued before an administrator set the flag are restricted too, and the same token works the moment the password was changed. The principal of the one allowed call additionally carries only the pseudo scope `password-change` (`security.rbac.PASSWORD_CHANGE_SCOPE`; it grants no permission). Personal access tokens, worker tokens and federated identities are **not** restricted (they do not know the local password).
* `POST /v1/identity/password` refuses a new password equal to the current one. The UI login page switches to "Choose a new password" when the login response says `must_change_password`, then signs in again with the new password.
* Tests: `tests/security/test_session_hardening.py` (including sweeps over every operation of the full OpenAPI document), `tests/tooling/test_sdk_contract.py`, `tests/tooling/test_cli.py`, `tests/e2e/test_12_security_admin.py`.

### F-E. Agent signals run the graceful shutdown
SIGTERM/SIGINT/SIGHUP (SIGBREAK on Windows) call `Agent.request_stop`: running children are stopped, the Spark daemons the agent started are stopped (bounded wait, then the process groups are killed) and the connection is closed. Daemons survive an agent exit only when `worker.keep_daemons_on_exit` is true (CLI `--keep-daemons-on-exit`; replaces `stop_daemons_on_exit`, whose default was the opposite). The controller's `shutdown` command accepts `stop_daemons`. Test: `tests/node/test_agent_shutdown_daemons.py` (real agent, real Spark daemons).

### F-F. Kernel log noise
`runtime_lib.kernel_host.BenignKernelNoise` drops exactly the IPKernelApp "Messages are sent over TCP without encryption" warning (one logger, one message) and nothing else. Test: `tests/node/test_kernel_log_filter.py`.

### F-G. Header names are not secrets
`core.redaction._SAFE_KEYS` contains `signature_header` (and `timestamp_header`): `GET /v1/triggers` shows `config.signature_header` as configured. Secret values, signature values (`signature`, `x-olcf-signature` header values, `secret`, `token`, ...) stay `***REDACTED***`. Test: `tests/orch/test_fix_signature_header.py`.

### F-H. OpenAPI request and response typing
* No operation has an untyped JSON body any more (`tests/unit/test_openapi.py::test_no_operation_has_an_untyped_json_body`, allow-list `ALLOWED_UNTYPED` is empty). Bodies of triggers, schedules (create, preview, pause), webhooks, alert rules/channels/silence, pipeline validate / import-fabric / runs / cancel / resume / rerun, `PATCH /workspaces/{ws}`, `PUT /admin/settings/{key}` (`{"value": ...}`), `PUT /workers/{id}/pool` and the Fabric integration configuration are pydantic models. Most derive from `olcf.core.models.OpenBody` (`extra="allow"`): the known fields are typed and documented, unknown keys still reach the service layer, which keeps the validation and the error wording it always had; handlers pass `dump(body)` (only the keys the caller sent) so "absent" and "explicit null" keep their meaning.
* `POST /notebooks/{id}/session` documents `SessionRequest` (it was already typed at HEAD; the test now pins it). `ProviderOut` (compute) documents `config`, `secret_id`, `last_discovery`, `capabilities`, `live_verified`, `execution`, `builtin`, timestamps - the operator-only keys are absent for `runtime.read`-only users. Identity providers have the response model `IdentityProviderOut` (`config`, `has_client_secret`, `last_test`, `issuer`, `client_id`, ...).

### F-I. `GET /v1/admin/diagnostics`
Returns 200 always. Before the first run: `{"last_run": null, "id": null, "status": "unknown", "summary": {...zeros}, "checks": [], "message": "...", "remediation": "POST /v1/admin/diagnostics"}`; afterwards the stored report plus `last_run` (its start time). SDK `diagnostics.last()` and the UI (Operations page, which treats `last_run: null` as "no report yet" and no longer needs the 404 workaround) use that shape. Test: `tests/ops/test_diagnostics.py`.

### F-J. Pipeline list and definitions in lists
* `GET /v1/workspaces/{ws}/pipelines?q=&include=definition&limit=&cursor=` (pipelines module): per pipeline `id, name, folder, description, tags, version, etag, created_at, updated_at`, `last_run` (`id, state, state_reason, trigger_type, created_at, started_at, finished_at, duration_seconds, pipeline_version`, or `null`) and `triggers` (`total, enabled, by_type, next_fire` - the earliest next fire time of enabled schedules). Two queries serve a page. Needs `item.read` on the workspace; each definition is further checked with `item.read` on that item.
* `?include=definition` (also accepted as `include` on `GET /workspaces/{ws}/items`, equal to the older `with_definition=true`) is size-capped: at most `LIST_DEFINITION_CAP_BYTES` of definitions per response (2 MiB for the pipeline list, 8 MiB for the items list); items past the cap carry `definition: null` and `definition_omitted: "size"` (`"forbidden"` when the caller may not read the item). Unknown `include` values are 400.
* SDK: `pipelines.list_pipelines(ws, q=, include_definition=, limit=, cursor=)`, `items.list(..., include="definition")`. Tests: `tests/orch/test_fix_pipeline_list.py`.

### F-K. Settings layers merge key by key
`load_settings`: defaults < `config.toml` < environment < explicit overrides, merged per key. `OLCF_SERVER__PORT=9001` replaces `server.port` only; the rest of `[server]` in `config.toml` still applies (the whole table used to be dropped). Test: `tests/unit/test_config_merge.py`. The `olcf init` config template note about this workaround was adjusted.

### F-L. No OpenAPI warnings
`create_app` plus `openapi()` raise no warning (checked under `warnings.simplefilter("error")`; `spark_ui` already registered its methods under distinct operation ids at HEAD). `tests/unit/test_openapi.py` keeps it that way.

### F-M. Logout, revocation and refresh-token reuse
* Table `revoked_tokens(jti, principal_id, reason, revoked_at, expires_at)` (migration **core v2**). Access tokens carry a `jti` and a family id `fam`; `TokenService.verify_access_token` refuses a revoked `jti` or family with 401 "access token revoked". The set is cached for `REVOCATION_CACHE_SECONDS` (2 s) per process and reloaded from the database, so every node that shares the database honours a logout within that time. The retention loop (`tokens` step) deletes rows whose token has expired anyway.
* `POST /v1/identity/logout` (audited as `auth.logout`; also works with the refresh token in the body alone, as before): revokes the refresh token **and the whole login family** (every access token and refresh token issued from that login) - other logins of the same user are untouched. A bearer token alone is enough to end its own session.
* Refresh tokens rotate. Presenting an already-rotated refresh token again within `REUSE_GRACE_SECONDS` (10 s; two tabs refreshing at once) only fails with 401. After the grace window it is treated as theft: the whole family is revoked (descendant refresh tokens and all access tokens) and `auth.refresh_reuse_detected` is audited with the number of tokens revoked (`refresh_revoked`).
* Tests: `tests/security/test_session_hardening.py`.

### F-N. Rate limiter is bounded
`core.ratelimit.RateLimiter(max_keys=10_000, sweep_interval=30)`: an LRU of per-key hit windows; expired keys are swept every `sweep_interval`, and the least recently used key is evicted when `max_keys` is exceeded. Memory is bounded under key-flooding (distinct client addresses / usernames). Limits stay per process (HA: effective limit = limit x nodes). Test: `tests/unit/test_ratelimit.py`.

### F-O. Disabled webhooks stop failing
Delivery re-reads the subscription right before the request: a delivery whose webhook was disabled or deleted meanwhile becomes `dead` ("the webhook was deleted or disabled") without a request and without touching `consecutive_failures`. The failure counter only counts while the subscription is enabled, and the auto-disable at `webhooks.disable_after_dead` happens in the same transaction (concurrent attempts cannot overshoot the limit); disabling dead-letters the queued deliveries of that webhook, so no retry noise follows. Tests: `tests/orch/test_webhooks.py`.

### F-P. Default workspace of UI pages
`pages/common.js` `defaultWorkspaceId`: the workspace chosen last (localStorage `olcf.workspace`, if it still exists), else the sample workspace ("Sample Retail") when installed, else the first one. The fallback is not persisted. Used by every page built on `useWorkspaceId` (Pipelines, Environments, Variables, Spark jobs, SQL, Reports). Test: `tests/e2e/test_07_pipelines.py::test_default_workspace_is_the_chosen_one_else_the_sample`. `OLCF_E2E_SHOTS=<dir>` redirects the e2e screenshots (default `docs/images/ui`).

### F-Q. `POST /v1/sql/execute` and `workspace_id`
`workspace_id` stays optional in the schema (statements without table names, and fully qualified `<workspace>.<lakehouse>.<table>`, work without it; the SDK, `olcf sql run --workspace` and the UI always send it). A table name that cannot be resolved without a workspace now answers 400 "cannot resolve table 'x': the request has no workspace_id" with a remediation and `details.missing = "workspace_id"` instead of blaming the table. Test: `tests/data/test_sql_service.py::test_missing_workspace_id_gets_an_actionable_message`.

### F-R. Security sweeps
`tests/security` enumerates the OpenAPI document of the full application, so new endpoints are swept automatically. Added: `/identity/logout` is in the public allow-list with its new description, a sweep asserting that a must-change session is refused on **every** other operation, and a sweep asserting that a logged-out session is refused (401) on every operation (`test_session_hardening.py`).

### F-S. Lint
`ruff check src tests scripts` is clean. Remaining `noqa` comments are per line with a reason (fixture credentials, deliberate fixed paths of the path-traversal tests, a zip extract after member-name validation, a token-scope name). No rule is disabled in `pyproject.toml`.

### F-T. Found while documenting (reported by DOCS)
* `olcf restore FILE` placed nothing: `backup_core._write_files` maps archive paths onto the data directory (`config/config.toml` -> `config.toml`, `keys/secrets/master.key` -> `secrets/master.key`, `keys/pki/ca.key` -> `pki/ca.key`) but the CLI looked in a different layout. It now restores straight into the data directory (`files_placed` lists the paths), keeps existing files unless `--overwrite-files`, and warns when a carried master key was not applied (stored secrets stay undecryptable until it is).
* `olcf backup create --include-keys` archives the same `KEY_FILES` as the server's backups (`secrets/*.key` and the PKI `pki/ca.*`, `pki/server.*`).
* `olcf sql engines` (column `id`), `olcf search` (columns `type`, `workspace_id`, `folder`), `AsyncClient.from_env` / `from_context`. Tests: `tests/tooling/test_cli_backup_restore.py`, `tests/tooling/test_cli.py`, `tests/tooling/test_sdk_transport.py`.
* Not done: `MasterKey.rotate/drop_old`, `SecretStore.reencrypt_all` and `SigningKeys.rotate` have no API, CLI or UI entry point (a feature, not a defect fix).

## First-run addenda (FIRSTRUN)

What a new user gets from `olcf quickstart --no-runtime`: a running server, the sample lakehouse, a notebook that runs with **no Spark runtime**, the dashboard and the AI-agent (MCP) endpoint. Everything below is
additive; no existing field, route or protocol frame changed meaning.

### FR-A. The `olcf-host-python` pseudo-runtime
* **What:** the worker's *own* interpreter offered as a runtime: id `olcf-host-python` (`olcf.core.hostruntime.HOST_RUNTIME_ID`), family `host-python`, **no Spark / Java / Scala**, engines `["python"]`. Built into every worker; nothing to download, install, verify, repair, retire or roll back
  (`RuntimeManager.install/repair/retire/rollback/export_bundle` raise `Unsupported` with a remediation; `POST /v1/runtimes/olcf-host-python/{install|verify|repair|retire|rollback}` is `409 conflict`).
* **Node side** (`olcf.runtime.hostpython`, `RuntimeManager.host_handle()`): `describe()` = Python version, implementation, platform and the versions of the key libraries (`duckdb, deltalake, pyarrow, pandas, numpy, polars, matplotlib, nbformat, jupyter_client, ipykernel, psutil`; absent ones are simply not part of it)
  and `fingerprint` = `sha256:` of the canonical JSON of those facts. `RuntimeManager.list()` always ends with the host entry (`state ready`, or `failed` naming the missing module of `ipykernel, jupyter_client, nbformat`). Switch off per node with `olcf-worker run --no-host-python` / `host_python = false` in `agent.toml` (`AgentConfig.host_python`, `ManagerConfig.host_python`).
  The entry in the `hello` / `heartbeat` `runtimes` list is `{id, state, verified, family, version, fingerprint, spark: null, python, no_spark: true, engines, active, detail, packages}` (the extra keys are ignored by older controllers).
* **Launching:** `RuntimeHandle.module_argv(module, *args)` is the one way the agent builds the command of a runner / kernel host. Installed runtimes: `[python, -I, -m, module, ...]`. Host runtime: the same, unless `olcf` is not in the interpreter's site-packages (a source checkout run with `PYTHONPATH=src`): then a tiny `-I -c` bootstrap appends the import root to `sys.path` and re-exports it as `PYTHONPATH` for the notebook kernel children. Environment: `OLCF_HOST_PYTHON=1`, the interpreter's `bin` first on `PATH`, no `JAVA_HOME`/`SPARK_HOME`. Per-job dependency layers (`LayerManager`) and the sandbox wrapper work unchanged on top.
* **Import rule (CONTRACTS section 12):** `olcf.runtime_lib.spark` imports `olcf.core.hostruntime` (stdlib-only constants and messages) in addition to `olcf.core.errors` / `olcf.core.redaction`; `tests/node/test_runtime_lib.py` allows exactly that module.
* **Spark is absent, with an explanation:** `runtime_lib.spark.no_spark_error(spec)` replaces a bare `ModuleNotFoundError: pyspark` (`Unsupported`: "this runtime has no Spark ... To fix: olcf runtime install ..."); `failures.py` classifies it `compatibility`. In a kernel `spark` is the same lazy object as everywhere, and the first use raises that error.

### FR-B. Catalog, resolution and the job record (controller)
* `runtimes` row `olcf-host-python` (`source = "builtin"`, always `available`) is created by `RuntimeService.ensure_host_runtime()` at module start. `GET /v1/runtimes` rows gain `builtin`, `no_spark`, `engines`; `state` is `ready` when at least one worker reports it ready, `nodes[]` carry each node's Python in `detail`. `GET /v1/runtimes/matrix` and the `spark-master:<runtime>` health checks leave it out. Topology nodes' `runtimes[]` carry `family`, `python`, `no_spark: true` for it. The `runtimes` health check is `ok` with only the host runtime ready (the detail says no Spark runtime is installed yet).
* **`RuntimeService.resolve(req, *, workspace_id, engine)`** (new keyword; `JobService._prepare` and `SessionService.create` pass the request's engine). Fallback to the host runtime happens only when **all** hold: `engine == "python"`; setting `compute.host_python_fallback` (default `true`) is on; at least one worker whose state is `online`, `degraded` or `offline` (not `pending` or `revoked`) last reported the host runtime `ready` (`RuntimeService.host_python_versions`, read from the workers' stored `runtimes`); the request names no exact `runtime_id` and no Spark / Java / Scala constraint; a `python` constraint, if any, is met by a worker's interpreter; and either no registered runtime satisfies the request or the chosen one is installed on no worker. `engine == "spark"` (or an unknown engine) **never** gets it; naming `olcf-host-python` for a Spark job is `422 unsupported` ("has no Spark").
  The decision is recorded in `RuntimeSelection.fallback = {requested, using, reason}`, in the job event **`runtime_fallback`** (right after `queued`) and in `RuntimeSelection.notes`.
* **Fingerprint:** the catalog row has a static manifest fingerprint; the job's `runtime_fingerprint` is replaced at claim time by the fingerprint the *placed node* reported (`RuntimeService.node_fingerprint`), and the agent overrides the spec's runtime block with its live facts. `result.json` gets `dependencies.runtime_id`, `dependencies.runtime_fingerprint` and `dependencies.host_python = {python, version, packages, detail, no_spark}` (`runtime_lib.runner.dependency_fingerprint`); `GET /v1/jobs/{id}?detail=true` shows the same value as `hashes.runtime_fingerprint`.
* A launch that fails with `WorkerOffline` (the chosen worker is still "online" in the database right after a control-plane restart but not connected yet) re-queues the job (up to 5 attempts, 3 s back-off) instead of failing it.

### FR-C. Queued jobs explain themselves
* Job record (`GET /v1/jobs`, `/v1/jobs/{id}`) gains **`queue_reason`**: `{code, message, remediation}` while the job is `queued` and the scheduler has evaluated it, else `null`. `code` is the planner's `Blocked` code: `paused`, `quota`, `no_runtime`, `no_node`, `constraints`, `resources`, `infeasible`, `held`, `backoff`. It is derived from `wait_reason` (`"<code>: <sentence>"`); `remediation` is the generic fix for the code (`jobs._QUEUE_HINTS`).
* `no_runtime` sentences come from `RuntimeService.explain_missing(runtime_id)`: not installed on any worker / installed only on workers that are offline or cordoned / an install is running / the last install failed (with its first detail line) / the host runtime is offered by no online worker, each with the command that fixes it (`olcf runtime install <id>`; Spark runtimes need PyPI and Maven Central). `no_node` names the ways to get a worker.
* Timeline: one **`queue_reason`** event (`kind = event`, `reason` = the sentence, `data = {code, message}`, where `message` is the whole `"<code>: <sentence>"` wait reason) each time the *code* changes (not on every re-wording). CLI: `olcf job status|get` print `waiting: [code] message` + `fix:`; `--wait` prints each new reason once; `olcf job timeline` shows `at / event / from -> to / detail` (its columns were wrong before). UI: the Job page banner "Waiting in the queue: <Code>" with "What to do", and a banner when the job runs on the host runtime.
* The scheduler no longer asks to auto-install the host runtime.

### FR-D. CLI errors and quickstart
* **No tracebacks:** `OlcfGroup.invoke` renders any `OlcfError` as `error: <message>` + `hint:  <remediation>` (exit 1) and any other unexpected exception as `error: <Type>: <message>` + a `-v` hint (exit 1); `olcf -v ...` re-raises (traceback). Click usage errors and the SDK error mapping (exit codes 2-10) are untouched. `olcf runtime install <unknown>` fails before announcing anything and lists the valid ids (`RuntimeNotFound.details.valid_ids`); `olcf runtime list` shows the host runtime (`SPARK = no Spark`).
* **`olcf quickstart` order:** checks -> init -> start the server -> verify the stored CLI login (a token the database does not know, from another data directory on the same URL, is replaced: `initialise` validates the token with `TokenService.verify_opaque`, and `_ensure_cli_login` rotates on a 401) -> sample install -> wait (<= 45 s) for a *connected* worker offering the host runtime -> print the working state (URL, one-time credentials, direct link `<url>/#/dashboards/<report id>`, `olcf notebook run -w "Sample Retail" 02_Retail_Explore_Python --wait`, `olcf mcp config --client claude-code`, a SQL example) -> **then** the Spark runtime install as a non-fatal follow-up: on failure a warning with the remediation, `Retry later with:  olcf runtime install <id> [--with-jdk]` and the Maven/network hint, exit 0. `--no-runtime` skips the follow-up. The `local` context follows `--port` (a config written earlier by another port) and never replaces another `current` context. Re-running is idempotent (sample `(unchanged)`, no second password, the running server is reused). `--json` keeps one JSON document on stdout (`next`, `python_notebooks_ready`, `runtime`, `runtime_retry`).

### FR-E. Sample notebook, notebook executor, dependencies
* `02_Retail_Explore_Python` needs only `deltalake`, `duckdb` and `pandas`; matplotlib is optional: without it the notebook prints how to get charts (`pip install "olcf[plots]"`) and every cell still shows its table. Notebook `01` stays `engine = spark`. `schemas/sample-retail-notebooks.json` regenerated. An existing installation keeps its old notebook text until `olcf sample install --reinstall`.
* The batch notebook executor starts its kernel with `MPLBACKEND=module://matplotlib_inline.backend_inline` (falls back to `Agg`), so figures are saved as `image/png` outputs of the executed notebook. Script jobs keep `Agg`.
* `pyproject.toml`: `ipykernel>=6.29` and `pandas>=2.2` are base dependencies; new extra `plots = ["matplotlib>=3.8"]` (also in `all`/`dev`). Proven in a clean wheel install (see the report of this task).
* Login screen: the anonymous visitor no longer probes `GET /v1/identity/providers` (administrators' list, answered 401 = a console error on every first visit).

### FR-F. Tests
`tests/unit/test_host_python.py`, `tests/node/test_host_python.py` (agent hello, script / notebook / kernel on the host interpreter, dependency layer on top, no-Spark errors, `--no-host-python`, refusals), `tests/compute/test_first_run_runtimes.py` (catalog, fallback rules, every queue reason; the test agent can report `olcf-host-python`), `tests/tooling/test_first_run_cli.py` (clean errors with a bad-input matrix, queue reason in `job status`/timeline, quickstart idempotence, stale context and token, failed runtime install is a warning), `tests/data/test_sample.py` (notebook without matplotlib), `tests/integration/test_first_run_no_runtime.py` (the whole story via subprocess `olcf quickstart` on an empty data directory: notebook 02 on host-Python with outputs, Spark notebook waits with its reason, dashboard tiles, MCP handshake + `run_sql`, SQL) and `tests/e2e/test_14_first_run.py` (Playwright: sign in, healthy home, Runtimes and Compute pages, dashboard, queued Spark job banner; zero console errors; helper `tests/e2e/firstrun.py`).

## Final QA addenda

Defects found by the last full sharded run; none changes a wire format, a route or a stored field. The only new protocol-visible detail is two agent error codes (FQ-A).

### FQ-A. Kernel sockets have exactly one user (`olcf.worker.kernels`)
* **Defect:** `KernelService` read the `jupyter_client` shell/iopub sockets from the relay threads *and* from command threads (`kernel_restart` called `wait_for_ready`, `kernel_execute` / `kernel_complete` sent on the shell socket, teardown closed the channels). ZeroMQ sockets and the client's `Session` (HMAC `digest_history`) are not thread-safe: the symptoms were `kernel restart failed: Invalid Signature: b'{}'`, `b'<IDS|MSG>' is not in list`, `Context was terminated` and, under load, a libzmq abort of the whole agent process. "Restart kernel" in the notebook UI was affected, not only the test. The connection key was never regenerated (a restart re-uses the connection file and the key); the signature errors were interleaved multipart frames.
* **Fix:** one **I/O thread per session** (`KernelService._io_loop`) owns the client sockets: it polls shell + iopub, relays them (`_on_iopub`, `_on_shell`) and runs every send that a command thread hands over with `_call` (`kernel_execute`, `kernel_complete`, the `kernel_info` handshake after a restart). The id maps (`msg_map`, waiters) are filled *on* that thread before a reply can be read (this also closes a small window in which the first output of a fast cell was dropped). Restart waits with `_wait_ready` (kernel_info reply on shell **and** the matching `idle` on iopub, as `KernelClient.wait_for_ready` does) through the I/O thread; teardown joins the I/O thread before it closes the sockets; an unreadable frame is dropped with a warning instead of killing the relay.
* **Also fixed:** after a restart the session's `create_time` was not re-recorded, so the watch loop compared the *new* pid with the *old* start time and declared the restarted kernel dead about a second later; the watch loop now also stands down while `restarting`.
* **Behaviour:** `kernel_execute` / `kernel_complete` during a restart answer `kernel_restarting` (retry); a second `kernel_restart` (or one during shutdown) answers `kernel_busy`. `kernel_restart` while a cell runs is supported (the cell dies with the old process).
* Tests (`tests/node/test_kernels.py`): `test_restart_twice_interrupt_while_running_and_work_after_each_restart`, `test_commands_racing_a_restart_never_corrupt_the_session` (four threads of executes / completions across three restarts), `test_many_cells_in_a_row_keep_their_output_attribution`. The first two fail on the previous code with `b'<IDS|MSG>' is not in list`.

### FQ-B. A lost `start_kernel` answer no longer kills a running session (`SessionService._start_kernel`)
* **Defect:** the agent announces `idle` (kernel ready) with a `kernel_event` *before* it answers `start_kernel`. A channel that died between the two failed the pending request and `_start_kernel` marked the already-idle session dead ("the kernel could not be started: ... disconnected"), although the kernel was up. `test_session_survives_a_reconnect_when_the_kernel_is_still_there` hit it whenever its `drop()` came before the answer was read.
* **Fix:** when the start request fails with `WorkerOffline` / `WorkerTimeout` and the session is already `idle` / `busy`, the session is kept; the next `hello` reconciles it (kernel reported: stays; kernel missing: finished with "the kernel is gone"). A start that fails while the session is still `starting` ends it as before.
* Tests: `test_session_whose_start_result_is_lost_with_the_connection_keeps_its_running_kernel` (new test-agent option `lose_result_of`), and the reconnect test now reports the state reason and asserts that `execute` answers 202.

### FQ-C. Generated schema
* `schemas/capsule-v1.json` was regenerated with `olcf.fabric.capsule.json_schema()` after the model classes gained docstrings (pydantic turns them into `description`). The schema is identical to the previous one **except for four `description` strings** (checked structurally with every `description` removed): the wire format did not change. The wheel packs `schemas/` as `olcf/schemas/` (`pyproject.toml` force-include), the Fabric SDK carries no copy (it builds documents that validate against it), so there is one source of truth. The two multi-line class docstrings in `capsule.py` were reduced to one line so that the descriptions have no trailing whitespace.

### FQ-D. Installer tests
* `scripts/install.sh` and `olcf upgrade` honour **`OLCF_CACHE_DIR`** (uv's package cache; default `<prefix>/cache`, unchanged). `tests/tooling/test_installer.py` points every installation of the module at one cache: before, each of the five installations (module fixture, uninstall, macOS-stub, uv-download tests ...) downloaded and unpacked the whole ~630 MB dependency set (pandas, numpy, pyarrow, duckdb, deltalake, ipykernel ...) into its own cache, ~3.5 GB of disk and five times the network exposure per run. The macOS-without-developer-tools scenario and every other scenario are unchanged.
* Measured here: one cold installation 7.7 s, whole module 55-57 s (before 67 s), longest single test 14 s; so the 300 s limit is not approached by work, only by a stalled download or a full disk. `/tmp` held 12 GB of leftover pytest directories when this was investigated (15 GB free); clean `/tmp/pytest-of-*` after full runs. The 300 s test timeout was **not** raised.

### FQ-E. Command palette focus (`src/olcf/web/static/js/shell.js`)
* **Defect:** the palette focused its input in a passive `useEffect`, i.e. after the next paint. Keystrokes typed right after Ctrl+K went to the previously focused control and were lost (measured: focus was still on a link in 5 of 12 openings; the typed text was empty and Enter opened the first entry, "Home"). That is a UI defect for keyboard and assistive-technology users, not a test problem, and it was not caused by the login change.
* **Fix:** `useLayoutEffect`. `test_keyboard_navigation_and_landmarks` now asserts that the combobox holds focus the moment the dialog exists and that the typed text arrived. 12 of 12 openings focus the input.

### FQ-F. Housekeeping
* `scripts/gen_reference.py` describes `OLCF_CACHE_DIR`; `docs/reference/configuration.md` regenerated. `ruff check src tests scripts`, `scripts/check_docs.py`, the five `scripts/gen_*.py --check` and `scripts/check_headers.py` are clean.
* The Playwright suite writes its screenshots into `docs/images/ui` unless `OLCF_E2E_SHOTS` points elsewhere; a plain full run therefore modifies 43 curated PNGs (they are modified in the working tree and were not touched by these fixes).

## Review remediation addenda (LIFECYCLE)

Answers to the release-blocking limitations of `docs/dev/CODE_REVIEW_2026-10-08.md` that were "no operable path": key rotation (L1), worker credential renewal and expiry (L2), asynchronous Spark smoke (L3), host-python fallback re-binding (L4),
restore drill (L5). Everything is additive: no existing route, field, frame or setting changed meaning, and the defaults keep existing installs behaving as before.

### L1. Key rotation (master key, JWT signing key)

* **Model.** A *key ring* in the database (`kv` rows `security.keyring.<master|jwt>.<version:08d>`; a write inserts version n+1, so the primary key is the compare-and-swap; older versions are pruned after a successful write). Ring keys are stored AES-GCM wrapped
  by the **root** master key (`OLCF_MASTER_KEY`, the OS keyring or `secrets/master.key`; the environment variable may hold several keys separated by whitespace or commas, newest last). Stages: `primary` (new data), `overlap` (still decrypts / verifies) and
  `retired` (an external key id to stop trusting). Every node re-reads the ring at most every `OLCF_KEYRING_REFRESH_SECONDS` (default 5) and at once when it meets an unknown key id, so a rotation reaches all control-plane processes without a restart; a restored ring
  (same version number, different content) is adopted too (`ring.gen`).
* **API (permission `admin.keys`, role SecurityAdmin and PlatformAdmin).** `GET /v1/admin/security/keys` (key ids, stage, age, source, number of secrets per key, blockers, history, externally-managed flags; **never key material**);
  `POST /v1/admin/security/rotate/{master-key|jwt-signing-key}` body `{action: start|reencrypt|drop-old|rollback|rewrap, reencrypt?, force?, to?}`, query `wait`. Audit: `security.master_key.rotated|reencrypted|dropped|rolled_back`,
  `security.jwt_key.rotated|dropped|rolled_back`, `security.keys.rewrapped`, `*_refused`; events are published for the same names.
* **Master key.** `start` adds a new primary key and (default) starts the **re-encryption** as an operation (`/v1/operations/{id}`, progress in percent): `SecretStore.reencrypt_all` rewrites rows with a compare-and-swap on `(id, key_id, version, not deleted)`, so a concurrent
  write of a secret is never lost; it is resumable (`action: reencrypt` picks up where it stopped, a heartbeat lock row allows takeover of a crashed run), multi-pass, and reports unreadable rows as failed instead of hiding them. `drop-old` is refused while any secret
  still uses the key (409 with the count). `rollback` makes the previous key primary again (before `drop-old` only). `rewrap` re-wraps the ring after the *root* key was replaced.
* **JWT signing key.** Tokens carry the `kid` header; verification picks the key by `kid` (kid-less tokens try all keys). `drop-old` is refused until the longest token lifetime (`access_ttl + 30 s`) has passed, unless `force` (signs users out).
* **Externally managed keys.** With the key in `OLCF_MASTER_KEY` or a file the root key is not rotated by the API: `GET keys` says `externally_managed` and the rotation adds ring keys *under* it; rotating the root key itself is: add the new key to the end of
  `OLCF_MASTER_KEY`, restart nodes one by one, run `rewrap`, then remove the old value.
* **Backup / restore.** A `master`-mode backup is encrypted with the newest **root** key (`MasterKey.backup_id`), never a ring key, so a clean machine needs only the root key; the ring is part of the same database snapshot as the secret ciphertext, so key ids and
  ciphertext always match. `restore-test` decrypts secrets with the restored ring. CLI `olcf security keys`, `olcf security rotate master-key|jwt-signing-key [--action ...] [--no-reencrypt] [--force] [--to KID] [--wait] [--yes]`; SDK `client.security.keys()`,
  `client.security.rotate(kind, action, ...)`, `rotate_master_key`, `rotate_jwt_signing_key`; health check `key-rotation`.
* **Tests.** `tests/security/test_key_rotation.py` (30), `tests/ops/test_key_ring_backup.py` (4), `tests/tooling/test_cli_security.py` (4). Code: `security/keyring.py`, `api/core/rotation.py`, `cli/commands/security.py`.

### L2. Worker credential renewal and expiry

* **Why.** The worker certificate was valid 90 days with no renewal and the worker token never expired. Now: the agent renews its certificate over its authenticated channel before it expires, worker tokens can be given a maximum age, and both are visible.
* **Protocol.** See 4.2 ("Credential renewal"). The agent asks when the hello acknowledgement's `credentials.renew_after` is reached (plus a random delay up to `cert_renew_jitter_seconds`, default 300 s, so a fleet does not ask at the same instant) and retries a
  failure after 5 min, doubling to 1 h. The controller signs only when the worker identity is valid (not revoked) **and** the certificate is inside the window (`compute.worker_cert_renew_before_days`, default 30, never wider than half the lifetime), unless
  the operator asked (`POST /v1/workers/{id}/renew` opens a 120 s grant and sends `renew_credentials`). At most one scheduled renewal per 30 s per worker. The key pair is generated on the node; the subject is always `CN=<worker id>`.
* **No lock-out.** The superseded token and certificate stay valid until the node's next connection authenticates with the new token (and, when a client certificate is presented, the new certificate) or `compute.worker_token_grace_seconds` (default 24 h) passes
  (leader sweep); then they are revoked (certificate: CRL `pki.revoked`). An agent that reconnects with the superseded token after a rotation (the response was lost) is given the current token again. The agent writes `client.crt.next`/`client.key.next`, verifies the
  pair loads, rewrites `identity.json` atomically and only then swaps (key first); `identity.recover_staged` finishes an interrupted swap at start. The next connection builds a fresh TLS context from the new files; the current one stays up.
* **Records.** Columns `workers.cert_expires_at`, `workers.token_expires_at`, `workers.credentials` (JSON: pending rotation, grant, last renewal) - **migration compute v2** (idempotent). `cert_expires_at` / `token_expires_at` / `credentials{cert_days_left, renew_after,
  rotation_pending, last_renewal}` appear in `GET /v1/workers[/{id}]` and `cert_expires_at` / `token_expires_at` on every topology node. A worker enrolled before tracking learns its expiry from the `cert` the agent reports in `hello` (same serial only).
* **Settings** (`PUT /v1/admin/settings/...`): `compute.worker_cert_renew_before_days` (30), `compute.worker_cert_lifetime_days` (90), `compute.worker_cert_auto_renew` (true; false = only `olcf worker renew`), `compute.worker_token_max_age_days` (**0 = tokens never expire and are
  never rotated - the default, so existing installs are unchanged**; >0: new tokens expire after that many days and a token without expiry or inside the window is rotated in the next renewal exchange), `compute.worker_token_grace_seconds` (86400).
  Set a max age only on an agent version that renews: an agent without `renew_cert` cannot rotate and is locked out when its token expires (re-enrol it).
* **Health / alerts.** Health check `worker-certificates` (category `compute`, also in the topology components): ok; **degraded below 30 days**; **down below 7 days or expired**. Alert rules: the builtin `auth-expiry` now also covers worker certificates within 30 days
  (`check_worker_certificates`, `worker_certificate_days`); builtin `worker-certificate-critical` (critical, 7 days). Existing `auth_expiry` coverage of worker tokens that have an `expires_at` is unchanged.
* **Operator / CLI / SDK.** `POST /v1/workers/{id}/renew` body `{certificate?: true, token?: true}` (permission `compute.manage`; 409 when the worker is not connected, 502 with the agent's message when the agent fails) -> `{renewed, worker}`;
  `olcf worker renew NAME [--no-cert] [--no-token]`; SDK `client.compute.renew_worker(id, certificate=, token=)`. `olcf-worker status` shows the certificate expiry and the last renewal / error; `agent-status.json` carries `credentials`.
  Audit: `worker.certificate_renewed`, `worker.token_rotated` (actor: the worker), `worker.credentials_renewal_requested` (actor: the operator), `worker.credentials_rotation_completed`; events `worker.certificate_renewed`, `worker.token_rotated`.
* **Test agent / tests.** `tests/compute/fake_agent.py` enrols with a CSR, reports `hello.cert`, implements `renew()` (`renew_cert` exchange, applies certificate/token for its next connection) and the `renew_credentials` command.
  `tests/compute/test_worker_credentials.py` (22), real agent end to end in `tests/compute/test_real_agent.py::test_credentials_are_renewed_over_the_channel_by_the_real_agent`, swap/recovery `tests/node/test_credential_renewal.py` (8), alert `tests/orch/test_alerts.py`.
* **Not covered by a test.** The mTLS handshake itself (the test stack is plain HTTP, so the hub never sees a client certificate; the serial comparison in `on_authenticated` is exercised through the plain-HTTP path only).


### L3. Asynchronous Spark smoke test

* **Why.** `POST /v1/compute/spark-smoke` held the HTTP request open for the whole test (up to 15 minutes), had no way to be followed or stopped, and queued a job for ever when no node could run Spark.
* **Start.** `POST /v1/compute/spark-smoke` (permission `compute.manage`), body `{runtime_id?, partitions (2-64), executors (1-16), timeout_seconds (10-1800, default 240), pool?}`, answers **202** at once with `Location` and `Retry-After: 2` headers and the status document below.
  The id is the id of the job (`olcf-spark-smoke`, system workspace `wks_system`, visible to org-wide roles only); the deadline (`timeout_seconds`, `deadline_at`) is stored in the job's `origin.details`. Audit `compute.spark_smoke`.
* **Fail fast (409).** `no_spark_runtime` when no *online, connected, schedulable (`scheduling=active`, not revoked)* worker (of `pool` if given) has a **ready Spark runtime** (the host-Python runtime is not one; `runtime_id` must match if given). `details`:
  `online_workers`, `spark_ready_workers`, `spark_runtimes`, `requested_runtime`, `pool`; `remediation`: `olcf worker enroll-token` and `olcf runtime install`. The check is skipped when `compute.default_provider` names a remote provider (Kubernetes, Livy, ...).
  `smoke_already_running` when a smoke test is in progress (`details.id`, `details.poll_url`). No job is created in either case.
* **Poll.** `GET /v1/compute/spark-smoke/{id}` (`compute.read`; 404 for any job that is not a smoke test). Document: `id`, `job_id`, `poll_url`, `finished`, `ok` (**null while running**), `phase` (`queued|starting|running|cancelling|done`), `message`, `state` (job state),
  `timed_out`, `timeout_seconds`, `deadline_at`, `elapsed_seconds`, `wait_reason`, `runtime_id`, `provider`, plus the report fields of the earlier synchronous answer (`master`, `master_is_cluster`, `application_id`, `tasks`, `sum`, `hosts`, `executors`, `nodes_with_executors`,
  `distributed`, `single_address`, `failure`, `note`). A finished report stays readable for as long as the job is retained.
* **Bounded.** `TopologyService.smoke_tick` (leader hook, every 2 s) cancels a smoke job whose age passed `timeout_seconds` - **running or still queued** - with the reason `spark smoke test timed out after N s` (job timeline, `timed_out: true`, message says whether it was queued)
  and audits `compute.spark_smoke_timeout`. The job's own `policy.timeout_seconds` is `timeout_seconds + 60` (backstop only). `POST /v1/compute/spark-smoke/{id}/cancel` (`compute.manage`, idempotent) stops it on request (audit `compute.spark_smoke_cancel`).
* **Callers.** `olcf compute smoke [--runtime] [--partitions] [--executors] [--pool] [--wait/--no-wait] [--timeout CLIENT_SECONDS] [--server-timeout 10..1800] [--status ID] [--cancel ID]`: prints the phase as it changes (stderr), the report at the end; exit 0 = passed,
  10 = failed / timed out / cancelled, 9 = the client stopped waiting (the test keeps running until its deadline; the CLI table's `--wait` timeout; was 11 before the residual fixes), 6 on a 409 (`no_spark_runtime`, `smoke_already_running`) with the remediation. SDK `compute.spark_smoke(...)` (returns the 202 document), `spark_smoke_status(id)`, `spark_smoke_wait(id, timeout=, interval=, on_progress=)`,
  `spark_smoke_cancel(id)`. The UI dialog (`pages/compute_dialogs.js`, `SmokeDialog`) posts, then polls every 2 s and shows the controller's progress message. `olcf diagnostics --spark` still calls the synchronous wrapper `TopologyService.spark_smoke` (start + bounded wait + cancel).
* **Tests.** `tests/compute/test_spark_smoke_async.py` (12, test agents), `tests/compute/test_topology.py` (2), the real-cluster flow in `tests/compute/test_spark_smoke_real.py` (marked `spark`, now polls) and the UI dialog in `tests/e2e/test_05_spark_smoke.py` (needs a cached Spark runtime and a browser).
* The two `spark`-marked tests (a Spark cluster) and the e2e dialog test need a cached Spark runtime and a browser.

### L4. Re-binding of queued Python jobs to the host Python

* **Problem.** A job is bound to a runtime when it is submitted. A Python-engine job submitted before any worker offered the host Python (`olcf-host-python`), or whose Spark runtime later disappeared from every online worker, stayed bound to a runtime nobody had and waited with `no_runtime` for ever.
* **Rule.** `JobService.rebind_host_python(trigger)` re-evaluates every **queued** `engine=python` job whose `runtime_id` is not the host runtime: `RuntimeCatalog.resolve(request, engine="python", online_only=True)` (installed means "ready on a worker that is online now"). When that yields the
  host-Python fallback (setting `compute.host_python_fallback` on, a worker offers the host runtime, the request names no Spark/Java/Scala version and satisfies `python`), the job is re-bound with a compare-and-set on `(state=queued, runtime_id=old)`: `jobs.runtime_id`, `runtime_fingerprint`,
  `spec.{spark_conf, rejected_spark_conf, environment, dependency_lock, runtime_notes, spec_hash}`; `wait_reason` is cleared. Timeline event **`runtime_fallback`** `{requested: <old runtime>, using: "olcf-host-python", reason, rebound: true, trigger}`; audit `job.runtime_rebound`.
* **Never re-bound.** Jobs that pinned a runtime id (`spec.runtime_request.runtime_id`, also via an environment), Spark-engine jobs, jobs that are not queued any more (a placed job keeps its runtime), jobs whose runtime is still ready on an online worker, and everything while the setting is off.
* **When it runs.** On every hello (`hub.on_hello`), when a heartbeat changes a worker's advertised runtimes (`hub.on_inventory_change(fn(worker_id))`) and on every scheduler pass (leader hook), at most 500 jobs per run, identical requests resolved once per run.
* **Tests.** `tests/compute/test_host_python_rebind.py` (4, test agents: queued-before-any-worker, the only Spark node revoked, pinned runtime, Spark job and the setting). Submission-time behaviour (`resolve` without `online_only`) is unchanged: `tests/compute/test_first_run_runtimes.py`.

### L5. Clean-environment restore drill

* **Problem.** A backup holds the control-plane database. The processes of its queued and running jobs live on worker machines; after a restore into a new environment no worker is connected, the job source files (data directory) are not part of the backup, and the restored rows still said `running`.
* **Reconciliation.** `olcf.ops.restore_reconcile.reconcile_restored_jobs(db, live_workers=(), backup_id=None, apply=True)`: every job in `accepted|validating|queued|provisioning|running|cancel_requested` **without a live worker attempt** becomes `failed`, `failure_category = restore_reconcile`
  (new `FailureCategory.RESTORE_RECONCILE`), `failure {category, message ("restored from backup X: the job was <state> when the backup was taken and no worker attempt of it is alive in this environment"), state_at_backup, worker_id, attempt, remediation ("olcf job retry <id>")}`; its open attempts
  become `failed`, its job-scoped token is revoked, its timeline gets the state change and a `restore_reconcile` event. A *live attempt* = the job's driver worker is in `live_workers` and its current attempt is `provisioning|running`. Jobs run by a remote provider (provider not `local|standalone`, handle `sent`)
  are left to the provider monitor (`left_to_provider`). Finished jobs are never touched. Idempotent.
* **Report.** `{applied, category, considered, failed, kept, by_state{state: n}, jobs[{id, name, workspace_id, state_at_backup, attempt, worker_id, queue}] (first 200, `truncated`), left_to_provider[], live_workers, events_added, message}`. `backup_core.restore_backup(..., reconcile_jobs=True, live_workers=, backup_id=)` puts it in
  `RestoreReport.jobs` (default off: an update rollback restores into the same fleet, whose agents reconcile themselves on reconnect).
* **Where it is reported.** `POST /v1/admin/backup/{id}/restore-test` result gets `jobs` (the report) and a check **`job-reconcile`** (ok when nothing unfinished remains); the `row-counts` check accounts for the timeline rows the reconciliation adds. The restore-test runs on a throw-away database: the live jobs are untouched.
  `olcf restore FILE` (offline, stopped server): applies it and prints the warning, the first ten jobs and `--json` carries `jobs`. `POST /v1/admin/backup/{id}/restore` (online) applies it with `live_workers` = the workers connected to this controller; the operation result carries `jobs`; audit `admin.backup_restored` has `jobs_failed`/`jobs_kept`,
  `admin.backup_restore_tested` has `jobs_reconciled`.
* **Checklist for restoring into a clean environment** (drill it with `restore-test` first):
  1. On the source: `olcf backup create --include-keys --passphrase-stdin` (or `OLCF_BACKUP_PASSPHRASE`) or a `master`-mode backup plus the operator-held root key (`OLCF_MASTER_KEY`); keep the file off the machine. `POST /v1/admin/backup/{id}/restore-test?wait=true` must be `ok`, the `job-reconcile` check shows how many jobs will not survive.
  2. On the target: install the same or a newer OLCF, `olcf init`, **do not start the server**. Provide the root key (environment, keyring or `secrets/master.key`); with `--include-keys` the key files come from the backup (`--overwrite-files`).
  3. `olcf restore FILE --yes [--overwrite-files]`: read the `jobs` section - those jobs are `failed` (`restore_reconcile`); resubmit what still matters with `olcf job retry <id>`. Sessions/kernels are not part of this reconciliation.
  4. `olcf serve`; `olcf health` (key-rotation, backup and worker-certificates checks), `olcf audit verify`.
  5. Workers: enrolled workers reconnect with their tokens and certificates if the PKI/token rows were restored (they are); agents that still hold a process of a job the restore failed are told it is an orphan and stop it. Workers whose certificates were revoked or lost: `olcf worker enroll-token` and re-enrol.
  6. `olcf compute smoke --wait` proves the Spark path; `olcf job list --state failed --all-workspaces` shows the reconciled jobs.
* **Tests.** `tests/ops/test_restore_reconcile.py` (9: the rules, live attempts, token revocation, no compute tables, restore-test, a backup without unfinished jobs, online restore, `olcf restore` human + JSON, opt-in).
* **Open.** A restore on a different host with worker agents (the CLI test restores into a second data directory on the same machine); PostgreSQL (SQLite only).


## Review remediation addenda (SEC)

Findings F01 and F02 and the production runtime-trust limitation of `CODE_REVIEW_2026-10-08.md`.

**F01 - derived job / kernel credentials never exceed what the submitter could do**

* A job (or session) records the submitter's **credential ceiling** in `jobs.spec["credential"]` / `sessions.spec["credential"]` (no migration: it lives in the spec JSON). Document: `{"version":1,"permissions":[...]|["*"],"scopes":[...],"workspace_id":"ws_...","origin_token_id":...,"origin_family":...}` (`Principal.credential_ceiling`). `permissions` is `effective_permissions()` of the submitting principal (its token scopes, intersected with the ceiling it inherited if it is itself derived; `["*"]` = unrestricted); `workspace_id` is the lock; `origin_*` identify the PAT row / JWT `jti` + `fam` at the root of the chain.
* Dispatch (`jobs._mint_job_token`, `sessions._mint_token`) mints the callback token with `TokenService.create_derived` (scopes `["job"]`, `meta.derived = <ceiling>` plus `{"job_id","attempt"}` / `{"session_id"}`). On every request `api.deps._bind_derived_credential` rebuilds the principal as: the **current** owner's roles (bindings are re-read, so a demoted or deleted owner loses access at once) INTERSECTED with the `job` scope's permissions and the stored ceiling, LOCKED to the job's workspace (`Principal.workspace_lock`; org-level role bindings do not leak into other workspaces). A malformed ceiling document fails closed (no permissions).
* The derived credential is valid only while its origin and its work are alive: origin PAT not revoked / origin JWT family not in `revoked_tokens`; job in a non-terminal state with `attempt == meta.attempt`, same workspace; session not `dead`. **Logging out or revoking the PAT a job was submitted with therefore ends the running job's callbacks** (state, results, lakehouse access) - long-running jobs should be submitted with a service PAT. Legacy rows (no `credential` in the spec): `LEGACY_JOB_PERMISSIONS`, read-oriented, still workspace-locked.
* A job with referenced secrets re-requires `secret.use` for the *current* owner at dispatch.
* Pooled hidden SQL kernels are shared between users: they get the fixed ceiling `SQL_KERNEL_PERMISSIONS` (`fixed_ceiling(...)`), never the first requester's.
* Pipelines: `create_run` stores `principal_credential` (internal; stripped from `public_run`) when the starter carries a lock; `principal_for` re-applies it and raises `ActivityError(code="credential_revoked")` once the origin is revoked. A child/nested pipeline inherits it.
* `POST /v1/identity/tokens`: a token cannot mint a token whose scopes exceed its own `effective_permissions()` ("a token cannot mint a token with scopes that exceed its own").
* Tests: `tests/compute/test_job_credentials.py`, `tests/orch/test_pipeline_credentials.py`.

**F02 - the read-only SQL guard covers Spark's code-execution and file-access paths**

* The guard (`olcf.sql.guard`) applies to **every role, writers included**: `REFLECT`, `JAVA_METHOD`, `CALL_FUNCTION`/`IDENTIFIER()`, `TRANSFORM ... USING` (script transform), `INSERT OVERWRITE [LOCAL] DIRECTORY`, `LOCATION`/`USING <datasource>` path tables, file queries (`parquet.\`path\``), `CREATE FUNCTION ... USING JAR`, `ADD JAR/FILE`, `SET`/`RESET` of configuration, `${...}` variable substitution (Spark substitutes `${env:..}` / `${sys:..}` on the raw text, before parsing, so the text is scanned for it) and the SPARK_DENIED_FUNCTIONS family (`reflect`, `java_method`, `try_reflect`, `call_function`, `identifier`, protobuf functions, `read_*`, `http_*`, `ai_*`, `remote_*`, ...) are refused with a 422 and a remediation. Table-valued functions are allow-listed per dialect (`SPARK_ALLOWED_TABLE_FUNCTIONS`, `DUCKDB_ALLOWED_TABLE_FUNCTIONS`); DuckDB `read_csv`/`read_parquet`/`glob`/`'file.csv'` quoted file names are refused.
* **C01 - Spark SQL functions are an allowlist, and there is no unparsed-text fallback.** Every function call in a Spark statement (anonymous or a typed sqlglot node such as `Sum`, `Cast`, `TimestampTrunc`; the Spark spelling a typed node renders to is accepted too) must be in the finite, lower-case `olcf.sql.spark_functions.SPARK_ALLOWED_FUNCTIONS`; otherwise the statement is refused with `422 unsupported` ("function x() is not an approved Spark SQL function"). The names in `SPARK_DENIED_FUNCTIONS` / the denied prefixes keep their explicit message and are never in the allowlist (a test asserts the two sets are disjoint). **Qualified calls are always refused** (`mydb.my_udf(x)`, `default.f(x)`, `spark_catalog.default.f(x)`, qualified table functions), as is `CREATE FUNCTION` / `CREATE TEMPORARY FUNCTION` / `CREATE PROCEDURE`. Custom and UDF functions therefore fail closed: run a notebook or job. A function is added to the allowlist only after reviewing the Spark implementation (no file, class-path, network, environment, secret or configuration access) and adding a positive test. Every role needs **exactly one fully parsed, supported statement**: `guard.check_spark_sql(sql, strict=...)` returns the parsed `Statement` (never `None`) and raises `BadRequest` for text sqlglot cannot read or for several statements and `Unsupported` for a session command or host capability; `strict` only adds "must be a read". The former writer fallback (leading-keyword test over unparsed `;`-separated fragments, then `READ_ONLY_SQL`) is removed. The guard is one layer of isolation; it does not make notebooks safe for untrusted tenants.
* Spark executes the **rendered** SQL, not the submitted text, so `sql.service._plan_spark` re-checks the rendered statement (`guard.check_final_sql`). `sessions.run_spark_sql` (pooled SQL session path) uses `guard.check_spark_sql(sql, strict=False)` and requires `sql.write` for statements that parse as non-read-only; it refuses before any session is created.
* Tests: `tests/data/test_sql_host_guard.py`, `tests/compute/test_sql_session_guard.py`.

**Runtime trust - the development key is not trusted in production**

* The shipped runtime manifests are signed with the **development** key `8ea01f731883a91d`; its private half is in the source tree (`runtime/keys/olcf-dev.key`), so a store that trusts it accepts a manifest forged by anybody.
* New setting `runtime.trust.allow_development_key` (`OLCF_RUNTIME__TRUST__ALLOW_DEVELOPMENT_KEY`): `null`/unset = automatic = trusted **only when `environment != "production"`**; `true` opts in explicitly (also in production); `false` refuses it everywhere. New `runtime.trust.dir` (`OLCF_RUNTIME_TRUST_DIR` wins): a directory of `*.pub` files that *replaces* the built-in store (the dev key is then never trusted).
* Resolution (`signing.resolve_dev_key_policy`): setting > legacy `OLCF_RUNTIME_ALLOW_DEV_KEY` (`0`/`1`) > the environment (`Settings.environment`; processes without settings read `OLCF_ENVIRONMENT`, **unset = production**). `TrustStore.default()` follows it; `TrustStore.for_settings(settings)` is what the control plane (`RuntimeCatalog.validate_manifest`), `olcf runtime install`/quickstart and the embedded worker (the control plane exports `OLCF_RUNTIME_ALLOW_DEV_KEY` / `OLCF_RUNTIME_TRUST_DIR` to the child) use. A standalone worker agent resolves it from its own environment: set `OLCF_ENVIRONMENT=development` on a dev machine, or `--runtime-trust-dir` / `OLCF_RUNTIME_TRUST_DIR` in production.
* A dev-signed manifest in a store that does not trust the dev key fails with `SignatureInvalid`: "signed with the OLCF development key (...), which is not trusted here" and the remediation (keygen / sign / trust dir; for a dev machine `OLCF_ENVIRONMENT=development` or `runtime.trust.allow_development_key = true`). The control plane returns it as `422 validation_failed` with the same remediation. Built-in manifests that fail verification are skipped with a warning at start-up (the catalog then holds only the host-python runtime until the operator re-signs).
* Health check `runtime-trust` (`security/runtime_trust.py`, category `security`, **critical**; review C05): `down` = dev key trusted while `environment=production`, which makes `GET /v1/health/ready` answer `503 {"status":"unavailable"}` (the body is the same fixed document as the database-unavailable case, no detail; `GET /v1/health` liveness stays 200; the reason is only in the authenticated `GET /v1/admin/health`); `degraded` = dev key trusted in a non-production environment, or nothing at all is trusted; `ok` = only operator keys are trusted.
* `olcf init` / `olcf quickstart` write `environment = "development"` (with a comment explaining the production switch) so a fresh single-machine install keeps working. **Existing installations whose `config.toml` has no `environment` are "production": they stop trusting the dev key on upgrade** and `olcf runtime install` of shipped manifests fails with the actionable message until they add `environment = "development"` or install their own key.
* `python -m olcf.runtime trust [list|add PUBKEY|remove KEYID] [--trust-dir DIR]` manages the trust directory (`add` validates that the file is an Ed25519 *public* key). It is not exposed as `olcf runtime trust`.
* Tests: `tests/security/test_runtime_trust.py` (policy matrix, production reject / dev accept, forged-with-published-key, operator trust dir, control-plane validation, health states and registration, CLI, init config, embedded worker environment).


## Review remediation addenda (PROV)

Findings F03, F04, F05 and F07-F12 of `CODE_REVIEW_2026-10-08.md` (remote providers: Kubernetes, Livy / external / Fabric; shared notebook parameters; Helm network policy). Additive, except where a bullet says it **supersedes** earlier text (4.2 `result` event, 4.3 job directory, 4.4 "remote providers" and C5).
The Kubernetes behaviour is exercised against `tests/compute/fake_kube.py` (a test API server *and* kubelet that really runs the production runner) and the chart through `deploy/scripts/render_chart.py`; run `helm` and `kubeconform` on your values.

### P1. One remote completion contract (F03, F07, F08)

* **Who reports the result.** `ComputeProvider.result_contract` is `"runner"` (Kubernetes: the runner inside the pod reports) or `"provider"` (default: Livy, external, Fabric - the provider derives it from the remote system and returns it in `AppStatus.result`). The controller never invents a result.
  A terminal application whose result did not arrive is **incomplete**, not successful: `_poll_remote` records the events `result_incomplete`, tries `find_result` (the tail of the retained output, independent of the log cursor) and the termination record (`result_recovered`), waits `compute.remote_result_grace_seconds` (default 120)
  and then fails the attempt with `failure.details.code = "result_missing"` (category `provider`, `infra_loss`, `confirmed_gone`, not retried unless the job is `idempotent`). The earlier "minimal success document" fallback is **removed** (supersedes 4.4).
* **`AppStatus` additions** (`code`, `result`, `termination`, `confirmed_gone`, `reached_running`) and **`LogsPage` additions** (`cursor`, `gap`); `ComputeProvider.logs(handle, offset, limit, *, cursor=None)`, `find_result(handle) -> dict | None`, `submission_intent(req) -> dict | None`. All have defaults, so third-party providers keep working.
* **Draining (F08).** A terminal application is finished only after its output was *drained*: the monitor ingests up to `compute.remote_log_pages_per_poll` (16) pages of 256 KiB per poll and treats the end of ingestion (provider `eof`) as separate from the end of the process. Bounded: after `compute.remote_drain_timeout_seconds` (300) it finishes anyway and
  writes a visible `log_gap` event and a note into the job log. The log offset **and the provider's opaque cursor** are persisted after every page (`provider_handle.log_offset|log_cursor`); a page is persisted as `log_pending {seq, chars}` before it is written, so a stop between "written" and "cursor saved" re-reads *exactly* that page and the log store (which ignores a repeated
  `seq`) cannot duplicate it. Gaps reported by a provider (`LogsPage.gap`) become a job-log line and a `log_gap` event.
* **Runner remote mode** (`python -m olcf.runtime_lib.runner --workdir DIR [--remote] <input>/job.json`). `--workdir` stages the (read-only) input directory into `DIR` (owner-only files, kubelet bookkeeping entries skipped, links followed) and implies `--remote`. Without either flag the runner is byte-for-byte the agent runner (writes `result.json`, emits `runner_finished` only).
  In remote mode, **after** `result.json` was persisted atomically: (1) declared artifacts (`output.ipynb`, ...) are uploaded to `POST /v1/jobs/{id}/artifacts/{name}` with the job token (3 tries; `size`, `sha256`, `stored`, `error` are recorded in the artifact entry *before* `result.json` is written); (2) exactly one line
  `@@OLCF@@ {"event":"result","job_id":...,"attempt":...,"result":{...result.json...}}` is printed, then `runner_finished`; (3) a compact record is written to `OLCF_TERMINATION_LOG` (default `/dev/termination-log`). If `result.json` cannot be persisted the event is still emitted (with a diagnostic on stderr); an unreadable `job.json` still produces a failed `result` (job id/attempt from `OLCF_JOB_ID`/`OLCF_ATTEMPT`).
  The staged `job.json` (job token, secrets) is deleted when the run ends.
* **Size.** The event line is capped at `limits.result_event_bytes` (default 192 KiB). A larger document is shortened (`exit_value` replaced by the usual `{"_truncated":true,"original_bytes":..,"preview":..}`, long failure texts cut, `dependencies`/`spark`/`metrics` dropped last) and gets `result_ref {name:"result.json", bytes, sha256, complete:false, stored, error?}`; the complete document is uploaded as the artifact `result.json`
  (`stored:false` + `error` if no controller URL/token or the upload failed). The termination record is `{"v":1,"job_id","attempt","result":{state, failure{category,code,message,remediation}, artifacts[names], result_bytes, result_sha256, exit_value (only if <= 1 KiB), partial?}}`, at most 3.5 KiB (the kubelet keeps 4 KiB). It survives log rotation and pod log loss; it cannot carry a large result.
* **Kubernetes provider.** Pod layout (supersedes the 4.3 note "job.json in a Secret mounted at the work directory"): Secret **`input`** read-only at `/opt/olcf/input` (`defaultMode 0440`), `emptyDir` **`work`** at `/opt/olcf/work` (optional `work_dir_size_limit`), `emptyDir` `/tmp`; command `python -m olcf.runtime_lib.runner --workdir /opt/olcf/work /opt/olcf/input/job.json`
  (`runner_command` may use `{input_dir}` / `{work_dir}`); pod `securityContext {runAsNonRoot, runAsUser (config `run_as_user`, default 10001), runAsGroup, fsGroup, seccompProfile RuntimeDefault}`, container `{allowPrivilegeEscalation:false, capabilities.drop:[ALL], readOnlyRootFilesystem: config `read_only_root_filesystem` (default false)}`, `terminationMessagePath /dev/termination-log`, policy `File`.
  `submit` is idempotent per attempt: `AlreadyExists` of an object carrying this job's `olcf.io/job-id` and `olcf.io/attempt` labels is adopted, anything else is an error. `status()` returns `termination {reason, exit_code, result?, failure?, message?}` (OOMKilled -> `resource`, Evicted -> `provider`, exit 137 -> `provider`), `confirmed_gone`, `reached_running`; a vanished pod is `failed` with `code: pod_missing`.
  `spark_submit` mode mounts the Secret at `/opt/olcf/input` (read-only) and an `emptyDir` at `/opt/olcf/work` (spark conf `spark.kubernetes.driver.volumes.emptyDir.olcf-work.*`); its entrypoint must call the runner with `--workdir` (not part of the stock image contract tested here).
* **Kubernetes log cursor.** `KubeClient.pod_log(ns, pod, *, limit_bytes, since_time, timestamps, tail_lines)`. A page is read with `--timestamps --since-time=<second of the cursor> --limit-bytes=<limit + 64 KiB>`; the cursor `{"v":1,"s":sec,"ns":nsec,"n":k,"h":hash}` names the last consumed line (its timestamp, how many lines share exactly that timestamp, and a hash of the line). Lines up to the cursor are dropped by timestamp + count + hash (the hash is searched when the count disagrees),
  a missing anchor is a `LogsPage.gap` ("the retained log no longer contains the line at ...") and reading continues after the cursor's second; a window too small to contain the anchor (a burst inside one second) is grown x4 up to 64 MiB, beyond which the second is stepped over with an explicit gap. Without a cursor the retained log is read from its start (`offset` is then a plain line count, kept only for jobs started before cursors existed).
  The python client has no `since_time`: `since_seconds` is derived and over-reads by two seconds (de-duplicated by the cursor). Output rotated away *before the first read* cannot be detected (there is nothing to compare with). A line longer than the runtime's 16 KiB record split may carry timestamps in its middle (kubelet behaviour; handled as continuation only when a record has no prefix) - **to be confirmed on a kubelet**.
* **Livy / external / Fabric** keep `result_contract = "provider"`: the statement output / batch state is turned into `AppStatus.result` by the provider; the same monitor, drain and `result_missing` rules apply. The agent provider is untouched (`tests/compute/test_real_agent.py`, `tests/node/test_agent_jobs.py` pass unchanged).
* **Settings** (PUT `/v1/admin/settings/...`): `compute.remote_log_pages_per_poll` (16), `compute.remote_drain_timeout_seconds` (300), `compute.remote_result_grace_seconds` (120); existing `compute.remote_status_error_limit` unchanged. Events: `result_incomplete`, `result_recovered {source}`, `log_gap {reason, detail}`, `remote_reconcile {code, detail, provider}`.
* **Tests.** `tests/compute/test_review_k8s.py` (25), `tests/compute/test_providers_k8s.py`, `tests/node/test_runner_remote.py` (9). `FakeKube` (`tests/compute/fake_kube.py`, launcher `kube_launcher.py`) mounts Secret volumes read-only whatever the mount says (writes fail with `EROFS` through an audit hook, because the sandbox runs as root), runs the pod command, turns output into timestamped pod logs, honours `sinceTime` (whole seconds) / `limit_bytes` (may cut mid-line) / `tail_lines`, rotates logs (`log_retain_bytes`) and fabricates nothing.

### P2. Livy and the shared notebook parameters (F04, F09-F12)

* **F04 retry policy.** `LivyClient` retries per operation: GET/DELETE on transport errors and 429/502/503/504; POST only when the request provably did not leave (`ConnectError`, `ConnectTimeout`, `PoolTimeout`, `UnsupportedProtocol`) or the answer was `429` (a known rejection). Any other POST failure (read timeout, reset, any 5xx including `500`, `502`, `503`, `504`: a gateway can answer `503` after the upstream committed the create) raises `SubmissionUncertain` (`code submission_uncertain`, `ProviderError.uncertain = True`).
  Sessions/batches are named `olcf-<job_id>-a<attempt>` (`livy_name`); `launch` persists the intent (`provider_handle {kind, session_name|batch_name, code?, sent:false, intent_at}`) before the create request leaves; a control plane that dies in between is reconciled by `recover()` (`submission:"uncertain"`, event `remote_reconcile submission_uncertain`), never re-queued.
  Reconciliation looks the object up by name; a session receives exactly one statement, so `GET /sessions/{id}/statements` shows whether a statement POST was accepted. `AppStatus.code` values: `submission_uncertain` (still asking), `submission_adopted` (found), `submission_not_created` (verified absent for `submission_quiesce_seconds`, default 60 -> failed, `reached_running false`, retried), `submission_unresolved` (unknown for `submission_reconcile_seconds`, default 900 -> failed, `confirmed_gone false`, **never retried automatically**).
  There is no "unknown" job state: uncertainty is an event + code on the existing `provisioning`/`running` states.
* **F09 parameters.** `olcf.notebooks.params` is the one injection helper (`inject_parameters`, `render_parameter_cell`, `py_literal`, `validate_name`, `normalize_notebook`): the injected cell (tag `injected-parameters`) goes **after** the tagged `parameters` cell (or first), exactly as the agent runner does; invalid names raise `ValidationFailed` (never silently dropped); values are Python literals (`nan`, `inf`, nested structures round-trip). Livy flattening labels cells `# --- cell N ---` and `# --- injected parameters ---`.
* **F10 SQL.** `olcf.runtime_lib.magics.split_statements(sql, *, strict=False)` is lexical (quotes, backtick identifiers, `--`/`/* */` comments, escapes); `strict=True` raises `ValueError("unterminated ...")` and Livy refuses such a script instead of splitting blindly. The Livy SQL branch passes parameters as `args=PARAMETERS`.
* **F11 batch arguments.** `source.main_class` and `source.args` (now fields of `JobSource`) are read from the normalised spec; `parameters.main_class/args` are no longer an entry-point channel. A jar without `main_class` is refused with a remediation. `source.args` entries must be strings or numbers.
* **F12 Spark conf.** The validated conf (`runtime.spark_conf` filtered by `filter_spark_conf`, platform-owned keys such as `spark.driver.host|port|bindAddress` and `spark.blockManager.port` -> `403 policy_denied`) travels in `spec.spark.conf` for providers with `prepares_spark = False` and becomes the Livy session/batch `conf`; cluster preparation (master, driver host, ports) stays agent-only.

### P3. Spark ports and the Helm network policy (F05)

* `ComputeSettings` has one range per role: `spark_master_port_base`+`_span` (7077, 16), `spark_master_ui_port_base`+`_span` (18080, 16), `worker_port_range` (41200-41299), `worker_ui_port_range` (41300-41399), `driver_ui_port_range` (4040-4056), `driver_port_range` (41000-41099), `block_manager_port_range` (41100-41199); `port_ranges()` lists them, a validator rejects invalid or overlapping ranges.
  The standalone provider allocates master RPC/UI and the **worker RPC/UI pinned per node** from them (`_worker_ports`; exhausted range = actionable `ProviderError`), and sets `spark.ui.port` to the first driver UI port. `GET /v1/admin/ports` lists every range.
* Chart: `values.yaml spark.ports.{masterRpc,masterUi,workerRpc,workerUi,driverUi,driver,blockManager}.{start,end}` (schema-validated) is the single source for **both** the control-plane settings (ConfigMap `OLCF_COMPUTE__*`) and the NetworkPolicy. Control plane -> workers: every range; worker <-> worker: every range except the driver UI; peers are selected by this release's pod labels (`instance` + `component`).
  Restrictive egress (`networkPolicy.egressAllowAll=false`): DNS (labelled namespace + pods, configurable), database (bundled: pod labels; external: CIDRs + port), HTTPS CIDRs/ports, the Kubernetes API server when `kubernetesProvider.enabled`, control plane <-> workers, `networkPolicy.monitoring.{ingressFrom,egressTo}`, `networkPolicy.egress.extra`, `networkPolicy.workerIngressFrom`. `controlPlane.embeddedWorker` adds driver/block-manager ingress from the workers.
* `render_chart.py` gained the Sprig functions `add`, `add1`, `sub`. `tests/tooling/test_deploy.py` asserts, for defaults and for moved/resized ranges, that the rendered policy covers the first and last port of every configured range and that the ConfigMap round-trips into `ComputeSettings`.
* Spark's documented port table was used (master 7077 / UI 8080, worker UI 8081, worker RPC random, driver UI 4040, `spark.driver.port`, `spark.blockManager.port`); executors have no dedicated listening port setting. **Open: a CNI that enforces the policy, and a two-worker Spark application behind it.**

## Review remediation addenda (PORT)

Findings F06, F13, F14, F15, F16, F17, F18 and the test/documentation details of `docs/dev/CODE_REVIEW_2026-10-08.md`.

**Cross-platform file lock (F06).** `olcf.core.filelock.FileLock` is the only inter-process lock in the code base (standard library only, so it is also safe in runtime venvs). POSIX: `fcntl.flock`; Windows: `msvcrt.locking` on one byte at offset 0 with `LK_NBLCK` and our own polling (the lock file is given one byte first and the file position rewound before every call). A crashed holder releases the lock when the OS closes its handle. `acquire(timeout)` raises `LockTimeout` (a `TimeoutError`); `timeout=None` waits, `0` is one try. `runtime/locks.py` wraps it (its `__enter__` turns the timeout into `InstallFailed` with a remediation), `notebooks/layer.py` and `notebooks/gitsync.py` use it directly. **No module may import `fcntl` or `msvcrt` for locking.** `tests/unit/windows_sim_child.py` + `test_windows_startup.py` start a child interpreter that hides `fcntl resource pwd grp termios pty tty syslog` and the POSIX-only `os`/`signal` names, import every `olcf` module and run `create_app` with all modules; CI runs the same file on `windows-latest` (job `windows`, targeted). Other POSIX-only names found by search are guarded (`resource` in `runtime_lib/runner.py`, `os.getloadavg` in `worker/metrics.py`, `worker/procutil.py`); `worker/jobs.py` used `signal.SIGKILL` unguarded and `compute/embedded.py` called `os.killpg`/`start_new_session` directly - both now go through `getattr`/`worker.procutil`.

**Test layout (F15).** Every directory under `tests/` is a package; helpers are imported by qualified name (`from tests.<area>.<module> import ...`). Fixtures' helper code lives in `*_support.py` modules (`tests/compute/ctrl_stack.py`, `tests/orch/orch_support.py`, `tests/items/items_support.py`, `tests/fabric/fabric_support.py`, `tests/spark_locking.py`), the `conftest.py` files only define fixtures. `tests/unit/test_test_layout.py` fails on a bare `conftest`/sibling import or a directory without `__init__.py`. `OLCF_TEST_ORDER=reverse|shuffle:<seed>` reorders whole test modules (the CI collection step uses `reverse`). Do not add bare helper imports.

**Timeouts never overlap a retry (F13).** When an attempt exceeds its timeout the engine does not conclude it: it enters a *cancelling* phase (`Task.timed_out`), signals cancellation (`ActivityContext.on_cancel(callback)` callbacks run - the `http` activity aborts its socket through the SSRF connection tracker; async activities get the `cancel` hook, repeated every 2 s, and are polled until terminal) and waits up to `policy.cancelGraceSeconds` (default 30, 0..3600; engine constructor `timeout_cancel_grace_seconds`). Only then is the attempt concluded and the retry policy applied:
* it stopped when cancelled -> error `timeout`, retryable per `retries`;
* it did not stop within the grace, or it was cancelled somewhere it cannot know whether the other side did the work (`ActivityCancelled(message, uncertain=True, details=...)`, or an `ActivityError` whose category ends in `_uncertain`), or it completed after the deadline -> `timeout_uncertain` (or the activity's own `*_uncertain` category). **Retryable only if the activity is idempotent** (`policy.idempotent`, an idempotent class, or an `http` request with an `Idempotency-Key`); otherwise the run fails with `details.operatorAction` and the explicit decision is `POST /v1/pipeline-runs/{id}/resume`.
* every uncertain outcome publishes the event `pipeline.activity.outcome_uncertain` {run_id, pipeline, pipeline_id, activity, type, error, attempt, retried}, increments `olcf_pipeline_activity_outcome_uncertain_total{type,category}` and is logged.
`schemas/pipeline-v1.json` (`policy.cancelGraceSeconds`) is generated: run `python -m olcf.pipelines.gen_schemas`. Containers keep their own simple timeout (children are cancelled, no retry).

**`http` activity retry rules.** Not sent (DNS, connect error, connect/pool timeout, unsupported protocol, proxy error): always repeatable. Sent but unanswered (read/write timeout, reset, pipeline timeout abort): `GET/HEAD/OPTIONS` repeatable; `POST/PUT/PATCH/DELETE` -> `timeout_uncertain`/`network_uncertain`, repeated only when idempotent. A `408/425/429` answer means "not processed" and is retried for every method; a `5xx` (`503` included: a gateway can answer it after the upstream committed the request) that may have been processed is not retried for non-safe methods unless idempotent. `typeProperties.idempotencyKey` (`true` = `olcf-<activity run id>`, string, or expression) or an `Idempotency-Key` header marks the request idempotent and is sent unchanged on every attempt. `ssrf.guarded_client(..., tracker=)` returns a `ConnectionTracker` whose `abort()` shuts down the live sockets and then returns: the thread that owns the I/O closes them (closing from the watcher races with a blocked `recv`); a connection registered after the abort is closed by its own thread before anything is sent; a stream without a usable socket (or a failed shutdown) is closed by the watcher as a fallback.

**Browser token storage (F14).** `web/static/js/api/client.js`: the sessionStorage blob is `{access, refresh, exp, remember}`; `loadStoredAuth()` restores `remember` from it (or from the presence of the localStorage `olcf.auth` entry), so the first refresh after a reload keeps the persistent copy and follows the rotated refresh token. Unit test (Node, run from pytest): `tests/web/remember_auth.test.mjs` via `tests/web/test_remember_auth.py` (`OLCF_REQUIRE_NODE=1` makes a missing `node` an error instead of a skip); browser test: `tests/e2e/test_15_remember_me.py`.

**Typing (F16).** CI gate: `mypy src/olcf/cli src/olcf/sdk --ignore-missing-imports --follow-imports=silent` must report zero errors. `Identity.create_user(roles=)` takes `Sequence[Body]`.

**pip is a prerequisite, not an assumption (F18).** `uv venv` makes environments without pip. CI uses `uv venv --seed`; the `dev` extra lists `pip` and `setuptools`; `notebooks/piplock.run_pip` raises `PipMissing` (a `PipError` with `.remediation`) when the interpreter has no pip, `EnvironmentService` turns it into a 422 "cannot publish the environment: pip is not installed ..." with the remediation, `notebooks/layer.ensure_layer` says the same in its `LayerError`, and `scripts/build_fabric_sdk.sh` checks interpreter, pip and setuptools first (exit status 2 with the fix). There is deliberately no `uv pip` fallback: the resolver relies on `pip install --dry-run --report`.

**Portable fixtures (F17).** Wheel tags in environment tests come from the target platform (`PLATFORM_TAGS`), foreign targets are parametrized; source builds are pinned to the host platform and Python series. `tests/e2e/procscan.py` (psutil, scoped by a marker on the command line) replaces `/proc` scans in the e2e stacks and asserts that nothing survives teardown. Installer tests state where the PATH block goes per (shell, OS) (`PROFILE_FILES`: bash is `.bashrc` on Linux but `.bash_profile` on macOS, zsh `.zshrc`, fish a conf.d file, others `.profile`), re-run the installer under a `uname` shim to cover both operating systems, and the uv download test is architecture aware and checks the requested archive name. `test_host_python` compares interpreters with `os.path.samefile`.

**Generated documentation.** `scripts/gen_reference.py` normalises argparse help (usage wrapping and the `-o FILE, --output FILE` spelling differ between Python 3.11/3.12 and 3.13), fails instead of documenting a traceback, and walks nested settings sections (`runtime.trust.*`). Click's help output was compared across 8.1.8, 8.2.1, 8.3.0 and 8.5.0 and is identical for this CLI. `scripts/check_docs.py` knows that `runtime.context` is the Fabric notebookutils attribute, not a setting. After changing a CLI option, a setting, an event or the API run `scripts/gen_reference.py`, `scripts/gen_api_docs.py` and `scripts/gen_sdk_docs.py`.


## Integration QA addenda

The four remediation passes (LIFECYCLE, SEC, PROV, PORT) changed the code at the same time; this is what integrating them found and what the code guarantees now. Everything below is covered by tests.

### Q1. Runtime trust on the real user path

* **Bug found.** `olcf init` / `quickstart` write `environment = "development"`, but the local runtime commands (`olcf runtime list|install|verify`, the Spark step of `quickstart`) built their `RuntimeManager` with `TrustStore.default()`, which reads only the *process* environment (unset = production). A fresh `quickstart` therefore ended with "no runtime manifest ships with this build"
  although the server of the same data directory trusted the manifests. They now use `lifecycle.local_trust_store(state)` = `TrustStore.for_settings(<data dir settings>)`, the trust anchor the server uses. Bare data directories (no `config.toml`) are production: tests that use one say `OLCF_ENVIRONMENT=development` explicitly.
* **Old installations** (config.toml without `environment`, created before the runtime-trust change) are production: the bundled, development-signed manifests are refused. The message is one paragraph, not one bare line per manifest, and says exactly what to do (`signing.dev_key_advice`): development / single machine -> `environment = "development"` in `<data dir>/config.toml` (or `OLCF_ENVIRONMENT=development`) and restart;
  production -> `python -m olcf.runtime keygen`, re-sign with `... sign --key ... <manifest.json>`, put the public key in `runtime.trust.dir` / `OLCF_RUNTIME_TRUST_DIR`. It is shown by: `olcf init` and `olcf serve` (a warning from `lifecycle.runtime_trust_notice`, and `runtime_trust_note` in `olcf --json init`; not shown when the operator set `environment`, a trust directory,
  `allow_development_key` or `OLCF_ENVIRONMENT`), the control plane's start-up log (`RuntimeCatalog.dev_key_notice`, names the config file and whether `environment` is missing or explicit), `RuntimeManager.available_manifests` (one warning, `skipped_dev_signed`), `olcf runtime install` without an id (was "no runtime manifest ships"), and the `SignatureInvalid` remediation. Notebooks keep running on `olcf-host-python`.
* **Verified in a clean environment** (wheel from `pip wheel --no-deps`, `uv venv --python 3.13`, `uv pip install`, run through `setsid nohup` scripts, no `OLCF_*` variables): `olcf quickstart --no-runtime --no-open --port P` -> `environment = "development"` written, "Sample Retail" installed, `olcf notebook run -w "Sample Retail" 02_Retail_Explore_Python --wait` succeeded on `olcf-host-python`,
  `olcf runtime list` shows `olcf-rt-1.3.0`, `olcf-rt-2.0.0`, `olcf-rt-native-1.0.0` as "not installed" (installable), `GET /v1/admin/health` has `runtime-trust` = `degraded` (by design) with overall `degraded`, `GET /v1/health` and `/v1/health/ready` stay 200 (`degraded` is advisory; only `down`, the development key trusted in production, fails readiness with 503), `olcf stop` stops the server.
  Tests: `tests/tooling/test_runtime_trust_cli.py` (init -> installable, old directory -> one actionable warning / error / note, `serve` on an old directory), `tests/integration/test_product_e2e.py::test_topology_is_green` accepts `runtime-trust` = `degraded` and nothing else outside `ok`/`unknown`.

### Q2. The key ring never loses a key (data-loss race)

* **Cause.** `Ring._write` was insert-only compare-and-swap (the version in the primary key) followed by a *separate* pruning of older versions. After pruning, key `n+1` is free again: a writer that read version `n` and was descheduled while others wrote `n+1` and `n+2` inserted `n+1` successfully, and its key was hidden behind `n+2`
  (the rotation "succeeded", the key and everything sealed with it was unreachable). It reproduced in isolation (about 1 run in 4 on SQLite), not only under load. Not SQLite specific: PostgreSQL has the same window.
* **Fix.** `Database.begin_serialized(name)` (`db/engine.py`): one transaction that holds a mutual-exclusion lock - PostgreSQL `pg_advisory_xact_lock(crc32(name))`; SQLite `BEGIN IMMEDIATE` plus an in-process lock (a shared in-memory connection cannot nest transactions). `Ring._mutate` now does load -> `fn` -> version check -> insert -> prune inside it (`_write(conn, doc, expected)` keeps the version check
  and the primary key as a backstop; after `attempts` lost rounds the caller gets the retryable `Conflict`, as before). `fn` runs on a deep copy and must be repeatable.
* **Same pattern fixed:** `RotationService._beat` (the re-encryption status document) was an unlocked read-modify-write written by two threads; a finished operation could still report `operation_id: "pending"`. It uses `begin_serialized("olcf.reencryption.state")`.
* **Tests.** `tests/security/test_keyring_concurrency.py` (ring level, SQLite and - when `OLCF_TEST_POSTGRES_URL` is set - PostgreSQL): 20 rounds x 4 threads on one ring, 4 independent engines (= nodes) x 20 rounds, a deterministic stale-writer test (the old bug), a rotation blocked behind another sees its result, 4 OS processes x 5 rotations released together.
  `tests/security/test_key_rotation.py::test_concurrent_rotations_never_lose_a_key` runs 20 rounds of 4 simultaneous rotations through the secret store. Run against PostgreSQL 16 in the QA environment.

### Q3. Compute registry locks and abandoned launches

* `ComputeRegistry._locks` is a `WeakKeyDictionary` keyed by the `Database` OBJECT (was `id(self.db)`: ids are reused, a later stack could queue behind a stale launch thread of an earlier one). Registries on the same database still share their locks.
* `db_lock(..., cancel=callable)`: polled while waiting; a reason ends the wait with `LockAborted` (a `TimeoutError`).
* `StandaloneProvider.ensure_cluster(..., cancelled=)`: every wait (pool lock, room for the workers, master start, worker registration) ends with `LaunchAborted` (code `launch_aborted`) when the job is no longer `provisioning` (cancelled, failed, finished - `prepare` passes `_job_cancelled(job_id)`) or the provider was closed (`close()` sets `_closed`, which also wakes the sleeps). The in-flight daemon start commands are *not* abandoned half-way (they are bounded by the hub timeout), so a cancelled launch leaves the cluster in a state the next launch adopts.
* Tests: `tests/compute/test_launch_abort.py`; `test_spark_smoke_async.py` 3 consecutive runs, whole compute shard.

### Q4. Flaky tests with real causes

* `tests/data/test_sql_service.py::test_cross_workspace_access_is_denied_and_not_leaked` looked for `"42"` as a substring of the whole error body; the body contains the workspace id (`wks_<ULID>`, random Crockford base32), which contains `42` in about 1.5% of ids. It now scrubs ids and compares tokens, and asserts no `rows`/`columns` in the answer.
* `tests/compute/test_real_agent.py::test_credentials_are_renewed_over_the_channel_by_the_real_agent` waited only for the token in `identity.json`, which the agent rewrites BEFORE it swaps `client.key` / `client.crt`; it now waits for the whole swap (`identity.py` documents the order).

### Q5. Results (QA environment: Linux, 2 CPUs, Python 3.13, SQLite; PostgreSQL 16 for the ring tests; one directory per pytest invocation)

| Shard | Result |
|---|---|
| `pytest tests --collect-only -q` | 3120 collected, no errors |
| unit | 62 passed, 1 skipped |
| integration (`not spark`) | 13 passed |
| data | 409 passed, 2 skipped |
| orch | 392 passed |
| items | 184 passed |
| compute (`not spark`) | 464 passed |
| node (`not spark`) | 343 passed |
| fabric (`not spark`) | 281 passed, 5 skipped |
| corpus (`not spark`) | 54 passed |
| ops | 124 passed, 10 skipped (PostgreSQL / SFTP targets) |
| security | 374 passed, 7 skipped |
| tooling (incl. installer) | 281 passed, 5 skipped |
| web | 1 passed |
| e2e (`not spark`) | 52 passed |
| `-m spark`: compute / node / fabric / corpus / integration | 2 / 18 / 1 / 10 (+1 skipped) / 2 passed |
| `ruff check src tests scripts sdk examples deploy/scripts` | clean |
| `mypy src/olcf/cli src/olcf/sdk ...` | no issues in 30 files |
| `check_docs.py`, `gen_reference/api_docs/sdk_docs --check`, `check_headers.py` | ok (the reference documents were regenerated: new exception classes and settings from the four passes) |

**Open:** Kubernetes cluster / CNI / Livy acceptance (see PROV); the key ring and `begin_serialized` on PostgreSQL are exercised on a local PostgreSQL 16 (single server); the Windows and macOS code paths of the PORT pass.

## Residual fixes

Defects found by the documentation pass, fixed afterwards (details and honest limits in `CODE_REVIEW_RESPONSE.md`, "Defects and corrections").

* **Runtime trust in images and installers.** `scripts/install.sh` / `install.ps1` run `olcf init` (writes `environment = "development"`) *before* `olcf runtime install`. `deploy/docker/bake-runtime.sh` (throwaway `runtime-seed` stage of `deploy/Dockerfile` only) exports `OLCF_ENVIRONMENT=development` for the single install when no `*.pub` key is in the build argument `RUNTIME_TRUST_DIR` (default `deploy/runtime-trust`) and marks the seed `.dev-signed`; with keys it installs an operator-signed manifest (`BAKE_RUNTIME=<name>.json` beside the keys) in production mode. The final image is always `OLCF_ENVIRONMENT=production`, carries the keys at `/etc/olcf/runtime-trust`, and both entrypoints export `OLCF_RUNTIME_TRUST_DIR` to it when it holds a `*.pub` and the operator set none (`OLCF_IMAGE_TRUST_DIR` overrides the directory for tests). Compose passes `RUNTIME_TRUST_DIR` (`OLCF_RUNTIME_TRUST_BUILD_DIR`) and reads `OLCF_ENVIRONMENT` from `.env` (default production). Not built here (no Docker daemon).
* **Helm.** `runtimeTrust.existingSecret` | `existingConfigMap` (exclusive) + `mountPath` (default `/etc/olcf/runtime-trust`): mounted read-only into the control plane and every worker pod, `OLCF_RUNTIME_TRUST_DIR` set on both. `controlPlane.extraVolumes|extraVolumeMounts`, `workers.extraEnv|extraVolumes|extraVolumeMounts` added to the templates and `values.schema.json`.
* **`runtime-trust` visibility.** `security/runtime_trust.py` exports `DEV_KEY_FIX`, `NO_KEY_FIX`, `remediation_for(labels)`; `olcf status` adds `runtime_trust` (JSON) / a warning line when the check is `down` or `degraded`, and `olcf doctor` shows it as `WARN` with the fix even when `down` (advisory; readiness is unchanged). `RuntimeCatalog.dev_key_notice` names `GET /v1/admin/health` / `olcf health`.
* **`olcf compute smoke` exit codes** follow the CLI table: 9 = the client stopped waiting (`--timeout`; the test keeps running), 10 = the test finished and did not pass, 6 = `409` (`no_spark_runtime`, `smoke_already_running`), 11 stays "prerequisite missing". The earlier text in the Controller addenda (11 = the client stopped waiting) is superseded by this.
* **Resume route** is `POST /v1/pipeline-runs/{id}/resume` everywhere (engine docstring, `operatorAction` text, this file).
* **`sql.guard.check_spark_sql`** refuses, for every caller and `strict` or not, every statement that is not a read or one of `SPARK_ALLOWED_KINDS` (insert, ctas, create_view, ddl, update, delete, merge): `ADD JAR|FILE|ARCHIVE`, `LIST`, `RESET`, `SET`, `CACHE|UNCACHE|CLEAR CACHE`, `REFRESH`, `CALL`, `OPTIMIZE`, `MSCK` ... Unparseable text is checked per `;`-fragment (leading keyword allowlist, no `JAR|ARCHIVE|LOCATION|DIRECTORY`). Spark `SHOW ...` (a sqlglot `Command`) is classified as the read `show`.
* **`notebookutils` / analyzer remediations** no longer suggest a "JDBC OLCF connection": connection types are `local`, `s3`/`minio`, `adls`, `onelake`; the hints name storage connections, Spark job definitions and variable sets.
* **Job token registration.** `JobService.launch` records `job_attempts.token_id` immediately after `_mint_job_token` (before `_build_spec` / `provider.submit`; if the record fails the token is revoked), and `_launch_failed` revokes the attempt's token before deciding between requeue and failure. An *uncertain* remote submission returns a handle instead of raising and keeps its token.

## Intelligence integration addenda (INTEG-Intelligence)

Written by the integration pass that proved Intelligence works as one system with the platform. Tests: `tests/intelligence/system/` (see [`INTELLIGENCE.md`](INTELLIGENCE.md) section 11).

### II-A. The sample and Intelligence (`olcf.sample.intelligence`, sample `VERSION = 4`)

* `olcf.sample.installer` asks `ctx.maybe_service("intelligence")`; only when the module is installed does it call `sample.intelligence.integrate(...)`. That scans the sample workspace with the local source, imports the SYNTHETIC Power BI scanner result (`schemas/sample-retail-powerbi-scan.json`, generated by `olcf.sample.powerbi`; every name, id and description says *synthetic*) as a `scanner_import` source and scans it, adds a small business glossary linked to the real columns and measures, the domain *Retail Sales*, the data product *Retail Sales Analytics* (the three gold tables, a freshness SLA), approves the PII label on `dim_customer.email` and certifies the gold tables. The outcome is the `intelligence` section of the install summary.
* It never fails the install: every step runs on its own, an error is logged and listed under `problems`, and a caller without `intelligence.manage` / `governance.curate` gets those steps reported rather than raised (without `governance.approve` the terms and the product stay `draft`). Without the module the summary has no `intelligence` key.
* The model is bound to the **gold tables** (its partitions name `gold_daily_sales`, `gold_category_summary`, `gold_customer_ltv`), so a visual traces to the raw files through `fact_sales` and the ETL notebook by normal name resolution. Everything is get-or-create and idempotent; the data product is created last, so an interrupted install finishes on the next call and a finished one is not touched again (a curator's later change survives).
* A dataset `VERSION` newer than the installed one is reported as `upgrade_available` and applied only with `reinstall=True` (`olcf sample install --reinstall`), as before.

### II-B. `EventBus.publish` and subscriber lanes (`olcf.core.events`)

Contract change, same API: a subscriber runs on its own thread (a *lane*, started on first use), in publish order per subscriber. `publish` waits for the lanes of the event for at most `WAIT_BUDGET` (0.5 s) in total, so a fast subscriber still has run when `publish` returns, and a hung or slow one costs the publisher at most that budget. A lane whose callback has been running for the budget is not waited for. `publish` from inside a subscriber never waits. `LANE_CAPACITY` (10000) bounds each queue; on overflow the oldest queued delivery is dropped and counted (the persisted event is not). Consequence for subscribers: do not rely on being *finished* when `publish` returns (only that fast subscribers are, within the budget); a test that needs a barrier polls for the effect (`wait_until`), as `tests/unit/test_event_bus.py` does.
Proven by `tests/unit/test_event_bus.py` (hung, slow and raising subscribers, order, no wait from inside a lane, overflow) and `tests/intelligence/system/test_failure_isolation.py`.

### II-C. Inbox attempts and the `failed` state

`InboxProcessor.tick()` attempts a row at most once per pass (`MAX_ATTEMPTS = 3` counts *passes*, not tries inside a pass). A row that reaches `failed` is retried **once more automatically**: after `intelligence.inbox_retry_after_minutes` (30; 0 disables) the hourly housekeeping sets it back to `pending` with `attempts = MAX_ATTEMPTS`, so one further failure takes it past the limit for good (`requeue_failed(db, older_than_minutes=...)`). An operator can re-queue every failed row at any time, with a fresh set of attempts: `POST /v1/intelligence/inbox/requeue` (`intelligence.manage`; audit action `intelligence.inbox_requeued` with the count), `olcf intelligence inbox requeue`, `client.intelligence.requeue_inbox()`. `inbox.updated_at` (migration `intelligence` version 2: it adds the column to an existing database; a fresh one has it from version 1) records when a row last left `pending` or was re-queued, and the age of a failed row is measured from it. The health check `intelligence` reports the number of failed rows. Events published while the inbox table is missing or raising are not recorded (the writer counts them, backs off 30 s after 3 consecutive failures, and never raises into the publisher): run-time lineage has a gap for that window, which a local scan closes at the next interval. Rows are purged after `intelligence.retention_days` (30).

### II-D. What the deployment-shape tests pin down

* **Without the module**: `/v1/intelligence/*` and `/v1/governance/*` answer as any unknown route of the platform (`404 not_found` for GET; a write to a path that does not exist is a `405 method_not_allowed` because of the GET-only static fallback - that is platform-wide, not Intelligence), `/v1/openapi.json` lists none of them, the console's `hasRoute('/v1/intelligence/summary')` is false so the Intelligence group is not rendered, `tools/list` offers no `intel_*` / `governance_*` tool, the health report has no `intelligence` component.
* **Fabric only** (core + Intelligence, a service-principal source): all eight connection steps pass, a scan reads workspaces, lakehouse, tables, notebooks, pipeline, semantic model, report and links them (table -> notebook -> table -> model table -> measure -> visual; pipeline `CONTAINS` activities that `EXECUTE` notebooks); a workspace the principal may list but not read is a `permission_denied` gap and the scan is `partial`, never failed; an incremental scan analyses nothing when nothing changed (`analysis_skipped`) and exactly the changed definition when one did. A Fabric source that cannot authenticate fails at the step that failed (`acquire Fabric token`, with the remediation), the scan is `failed`, and the only thing written is the tenant root asset the source names.
* **Fabric and on-premises together**: a OneLake table is **one** asset whichever side is scanned first. Aliases of both worlds (`fabric`, `onelake_path`, `delta_location`, `qname`) are on it; the OLCF notebook's edges attach to it; origin, lineage and impact cross the boundary. When the OLCF side is scanned first the tables are placeholders keyed by their `onelake_path` and the Fabric scan promotes them (`placeholders_merged`).
* **Authorisation** (`test_authz.py`): a node a caller may not see is skipped as if it did not exist (`GraphStore.walk` / `keep_node`). The walk does not hop over it, so an alpha-only viewer sees `alpha_raw` and `alpha_out` as unrelated when the chain between them runs through beta; nothing of beta (name, id, count, gap, edge) is in any answer of REST, MCP, CLI or SDK, and a hidden asset is `404` (not `403`) on every per-asset route, identical to an id that never existed. Granting the viewer the middle workspace shows the chain on the next request (no re-login).

### II-E. Known limits found by the system tests (still open)

The system tests also found a wide fan-out that filled the node cap with process nodes (impact reported `total: 0, truncated: true`), diff summaries that counted only the fetched rows, the origin of a column answered at table level, an unbounded attribute history and a command palette that listed the pages on an installation without the module. Those were fixed afterwards and are not listed here: `impact` has its own bound (`intelligence.max_impact_nodes`, 20,000) with real totals, lineage walks count datasets against `intelligence.max_lineage_nodes` and give process nodes a wider allowance of their own, the diff `summary` holds true totals, the origin of a column switches to column level and says so (`level_switched`), attribute versions are trimmed to `intelligence.max_versions_per_asset`, and the palette follows `hasRoute`. What remains:

* **Origin of a heavily diamond-shaped graph is capped at `MAX_PATHS` (100) paths** and says `truncated`; roots and nodes are complete within the node cap.
* **Column lineage of a `count(*)` has no source column**, and the local snapshot diff does not list a column the run-time scan already added (it is in the table's version history).
* **PostgreSQL**: none of the system scenarios, nor the scale test, was run against PostgreSQL. `tests/intelligence/system/test_portability.py` only proves the package contains no SQLite-only construct in its source.

### II-F. Fabric-originated runs and the tenant scan (addendum)

* **Root cause of the missing link.** A notebook run submitted from Fabric (SDK `run_remote` -> signed capsule -> `FabricService._submit` -> a `capsule` job) has no OLCF item: the process of its dataset events carried only the job id, so `runtime._process` built a job-level `spark_job` / `script` asset (`olcf-job:<job id>`) unrelated to the notebook asset the tenant scan made, and the dataset references (`lakehouse_id` + name) could not meet the scan's `fabric` / `onelake_path` aliases. The Fabric identifiers were stored (`jobs.origin.fabric`, `definition.fabric` of the lakehouse) but not forwarded.
* **Fix.** `dataset.read|written|statement` carry optional, additive `process.fabric` and `datasets[].fabric` / `default_lakehouse_fabric` blocks (section 6 of `INTELLIGENCE.md`), built by `compute/lineage_events.sanitize_runner_event` from the job row and the job spec only. `runtime.facts_from_event` (still pure) resolves the process by `fabric_alias(tenant, workspace, notebook)` and the datasets by `fabric_table_alias(...)` / the OneLake path, so the runtime edges attach to the same `ast_` assets in either order (scan first, or events first with the scan promoting the placeholders).
* **Fallback.** With a workspace and a notebook name but no notebook GUID or tenant, the process is a name reference scoped to the Fabric workspace (registry rule 3: a unique notebook of that name in the mapped OLCF workspace) and its edges are `inferred`, evidence `location.attribution = "name"`. If nothing matches, a placeholder notebook of that name is created and the next scan promotes it; the runtime never names or describes a Fabric notebook.
* **Not covered.** A Fabric run whose default lakehouse is not an OLCF lakehouse item with `definition.fabric` has no trusted lakehouse GUIDs; its table names are resolved the existing way (scope + name) and are not given Fabric aliases. The ids a caller claims in a capsule's default-lakehouse fields are not used for dataset identity.
* **Test.** `tests/intelligence/system/test_fabric_runtime_link.py` (both orders) drives the SDK, the adapter, a real worker and the Fabric source against the sample tenant of the test suite.

### II-G. Fabric / Power BI source endpoints are SSRF-guarded (addendum)

`config.network` URLs (`authority_host`, `fabric_api`, `powerbi_api`, `onelake_dfs`, and a source's own `proxy`) are checked by `intelligence/discovery/netguard.py` with the platform guard (`olcf.pipelines.ssrf`): https only, no credentials in the URL, no loopback / private / link-local / metadata / multicast / reserved destination (every resolved address is checked). Create and update answer 422 with a remediation; every use (`FabricConnector._client`, so the connectivity test, the scan and the sampling probe) checks again before a token is requested and then connects through the resolve-and-pin transport, without redirects. The runtime setting `intelligence.allow_private_endpoints` (default `false`) lifts only the loopback / private block (and allows http to such addresses); metadata and link-local stay blocked, the pipelines' `network.ssrf.allow` list is not honoured and `network.ssrf.deny` is. With a proxy the destination is resolved by the proxy: only a local name check is possible. Tests that point a source at a local test server on 127.0.0.1 call `tests.fabric.fabric_fakes.allow_private_endpoints(ctx)` in their fixture. Out of scope here: the Fabric *integration's* own `network` block (`/v1/integrations/fabric`), which is a separate admin-only configuration.

### II-H. Review-3 contract changes (addendum)

* `AuditLog.record(..., conn=...)` appends to the hash chain inside the caller's transaction (on file SQLite the chain is serialised through `begin_serialized("audit.chain")`); asset merges write their audit row on the merge's own connection, and notifications are delivered after commit from a registry outbox (`discard_events()` after a rollback the caller owns). No second writer is opened inside a graph transaction.
* `EventBus.publish` on SQLite waits at most `PERSIST_BUDGET` (0.25 s) for the persist; past that the event goes to an ordered background persister (`flush_deferred()`, `deferred_total`, `dropped`) and subscribers receive it at once without a `seq`, which is filled in later. PostgreSQL persists inline.
* Sampling and automatic value profiling both go through `DataReadPolicy` (`intelligence/discovery/datapolicy.py`): the policy is resolved for the *parent table*; a column can only narrow it; approved classifications and sensitive names are excluded; a classification-provider error fails closed.
* `FabricApi` paging methods raise `IncompleteInventory` (`reason` is `max_pages` or `repeated_token`) instead of returning a silently truncated list, and bounded reads stream and stop at the cap.
* Backup-target probe steps carry a structured `code` (for example `NoSuchBucket`) next to the human `detail` and `remediation`.
