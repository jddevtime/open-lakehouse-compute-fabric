# Operations runbook

Day-to-day tasks and the first steps for common incidents. Commands assume `olcf` is on the path and you are signed in as an administrator (`olcf login`); local lifecycle commands (`serve`, `stop`, `backup`, `restore`, `doctor`) act on the data directory selected by `--data-dir` or `OLCF_DATA_DIR` (default `~/.olcf`).

## Routine checks

| Task | Command or place |
|---|---|
| Is it up, how many workers and jobs | `olcf status` |
| Everything that can be checked | `olcf doctor` (machine and installation) and `olcf doctor --server` (adds the server's own health checks); UI: Administration, Health. Each failure prints the command that fixes it. The checks are catalogued in the [health checks reference](../reference/health-checks.md) |
| Workers, pools, queues | `olcf compute topology`, `olcf worker list` |
| Is the audit trail intact | `olcf audit verify` |
| Disk use | `olcf doctor` warns when the data volume is low; retention settings below bound growth |
| Backups | `olcf backup list`; schedule and restore tests in the UI ([backup and restore](../guides/backup-and-restore.md)) |
| Metrics | `GET /v1/metrics` (Prometheus text, needs `platform.admin`), or OTLP ([observability](../guides/observability.md)) |

## Where things are

`<data dir>` holds `config.toml`, `olcf.db` (SQLite) , `secrets/`, `pki/`, `storage/`, `runtimes/`, `jobs/<job id>/` (spec, work directory, `logs/`), `artifacts/`, `workers/`, `backups/`, `logs/` and `tmp/`. The server writes `logs/control-plane.log` (and `server.out` when started with `--daemon`; `embedded-worker.log` for the embedded worker). A remote worker keeps its state and log under `--state-dir` (default `~/.olcf-worker`). Job logs are served by the API and are not rotated by a log shipper: they are removed by retention (`retention.log_days`, 90).

## Starting, stopping, restarting

```bash
olcf serve --daemon          # background, pid file in the data directory
olcf stop                    # graceful: waits for running jobs, then stops
olcf service install         # start at login / boot (see olcf service --help)
```

A restart is safe at any time. Jobs run by an embedded worker end with it; jobs on remote workers continue, and the control plane re-adopts them when it returns (the agent's `hello` lists what it runs). Pipeline runs resume from the database ([components and data flow](../concepts/components-and-data-flow.md#a-pipeline-run)).

## Updates

Two mechanisms exist and they are different.

**1. Command-line upgrade of a single installation** (`olcf upgrade`, same as `olcf update`). It installs the new version next to the current one, takes a database backup first (skip with `--no-backup`, not recommended), migrates the database and switches; the previous version is kept for rollback.

```bash
olcf upgrade --list                       # installed versions
olcf upgrade --version 0.2.0              # from the package index (or --index-url / --find-links for a mirror)
olcf upgrade --wheel olcf-0.2.0-py3-none-any.whl
olcf upgrade --rollback                   # back to the previous version
```

**2. Signed updates through the control plane** (UI: Administration, Updates; API `/v1/admin/updates`; permission `admin.update`). A release is a manifest signed with Ed25519 by a key you trust. Nothing unsigned can be stored or applied; there is no flag to skip verification. The flow:

1. **Obtain**: `POST /v1/admin/updates/check` against the configured HTTPS feed and channel, or import an offline bundle (`POST /v1/admin/updates/import`). The manifest must be signed by at least `threshold` trusted keys, not expired, for the right channel and not older than what this installation already accepted (anti-rollback sequence). Trusted keys are managed under `/v1/admin/updates/trust`.
2. **Preflight**: artifact hashes, compatibility (Python, database dialect, worker agent versions), disk space, active jobs, and a real migration dry run: the current database is restored into a throw-away SQLite file and the new release's migrations run against it.
3. **Apply** inside a maintenance window: an automatic `pre-update` backup, compute drained (workers draining, queues paused, running jobs awaited), a hand-off marker written.
4. **Hand-off**: `olcf upgrade` (or `python -m olcf.ops.upgrade apply`) re-verifies signature and wheel hash and installs the wheel; a configured `restart_command` may restart the service. The new version runs migrations at start and the update is marked applied when the target version is seen running. A pip-installed single host needs the process restarted unless `restart_command` is set. Container and Kubernetes installs replace the image.
5. **Rollback**: before hand-off, cancel. After it, `rollback` restores the pre-update backup and reinstalls the previous version; changes made after the update are lost because migrations are not reversible in general.

Upgrade workers with the control plane: agents report their version and the preflight checks the compatibility matrix.

## Common incidents

| Symptom | Look at | Usual cause and fix |
|---|---|---|
| Job stays `queued` | `olcf job get <id>`, `olcf compute queues`, the job's timeline | no worker with a matching runtime (`olcf runtime install`), all cores reserved, queue paused, or the request can never fit (it is failed after `compute.infeasible_grace_seconds`, 300) |
| Job `failed` with category `dependency` | `olcf job logs <id>` | a package is missing from the environment; add it and publish the environment, or change `pip_policy` ([notebooks](../guides/notebooks.md#python-packages)) |
| Job `failed`, category `provider` | worker state, `olcf worker list` | worker lost during the run; check the machine and the agent log; retry rules apply |
| Worker shows `offline` | agent log (`olcf-worker` output, `<state-dir>/`), network path to the control plane | agent stopped, wrong `server.public_url`, TLS trust (`--ca-pem`), token revoked (close code 4401: re-enroll) |
| Spark job cannot start executors | `olcf compute smoke`, [ports](../reference/ports-and-networking.md) | blocked ports between nodes, a worker advertising a hostname others cannot resolve (`--advertise-host`) |
| `401` for a script | `olcf whoami` | token expired or revoked (personal tokens last 90 days by default); create a new one |
| Users cannot sign in with SSO | Administration, Health, identity providers; [SSO guide](../guides/sso-and-identity.md) | clock skew, wrong redirect URI, group mapping; if local login was disabled and the provider is broken, start the server once with `OLCF_BREAKGLASS_LOCAL_LOGIN=1` ([SSO guide](../guides/sso-and-identity.md#disabling-local-login)) |
| `429` from SQL | `sql.max_concurrent_queries` (4) | more embedded statements than slots; wait or raise the setting |
| Database locked (SQLite) | logs | SQLite suits one server process; use PostgreSQL for more load or replicas |
| Disk filling up | `du -sh <data dir>/*` | `jobs/`, `logs/`, `artifacts/`, `runtimes/` and local `storage/`; tune retention |
| Lost the only administrator password | none offline | another PlatformAdmin runs `olcf user reset-password`; if there is none, restore a backup taken before the loss or use the bootstrap file `secrets/bootstrap-admin.txt` if it was kept. There is no offline reset command in this build |
| Health `intelligence` is `degraded`, or `event inbox: N pending` keeps growing | `olcf intelligence summary`, `olcf health`, `intelligence.inbox_batch_size` | the inbox processor runs on the leader only and is behind, or the leader is down. Jobs, SQL and pipelines are not affected; only the freshness of the graph is. [Operating notes](../guides/intelligence.md#operating-notes) |
| A scan ended `partial` or `failed`, or a lineage answer says "incomplete" | `olcf intelligence scans list --status partial`, `olcf intelligence gaps` | permission or API gaps of the identity a source uses; every gap carries its fix. A scan that ended `partial` never removes assets ([troubleshooting](../guides/intelligence.md#troubleshooting)) |
| Support request | `olcf diagnostics --out support.zip` | a zip with versions, checks, redacted configuration and recent logs; no secrets, keys, tokens or data. Review it before sending |

## Retention

A scheduled job (leader only, every `retention.interval_hours`, 24) deletes: finished job history after 365 days, job logs after 90, audit records after 730 (minimum 30; archived to gzip JSONL first unless disabled), events and metrics after 14, query history after 30, soft-deleted workspaces and items after 30, tokens 7 days after they expire or are revoked, support bundles after 14. All are runtime settings (`olcf settings list`, [configuration reference](../reference/configuration.md#runtime-settings-olcf-settings-v1adminsettings)); a dry run is available from Administration, Data retention (`/v1/admin/retention`). The event inbox of OLCF Intelligence is trimmed by `intelligence.retention_days` (30) and run-time evidence by `intelligence.verified_evidence_days` (90); assets, edges, scans and governance records are never deleted ([Intelligence retention](../guides/intelligence.md#retention)).

## Rotating credentials

| What | How |
|---|---|
| A user password | `olcf user reset-password <user>` (shown once; use `--must-change-password` at creation) |
| A personal token | create a new one, switch, `olcf token revoke <id>` |
| A worker | `olcf worker revoke <worker>` then enroll again with `olcf worker enroll-token` |
| A webhook trigger or webhook subscription secret | `POST /v1/triggers/{id}/rotate-secret`, `POST /v1/webhooks/{id}/rotate-secret` |
| A stored secret value | `olcf secret rotate <secret-id>` |
| Master key | `olcf security rotate master-key` (adds a primary key and re-encrypts every secret in the background; `--no-wait` returns at once). When it is done: `olcf security rotate master-key --action drop-old`. Details below |
| JWT signing key | `olcf security rotate jwt-signing-key`, later `--action drop-old` (refused until the longest token lifetime has passed; `--force` signs everybody out) |
| A worker's certificate and token | automatic inside the renewal window (30 days before expiry); on demand `olcf worker renew <worker>` (`--no-cert` / `--no-token` renew only one) |
| Server certificate | automatic for the built-in CA (re-issued when names change or 30 days remain); otherwise replace `tls.cert_file` and `tls.key_file` and restart |

### Key rotation

Needs the permission `admin.keys` (PlatformAdmin, SecurityAdmin). `olcf security keys` lists each key ring (key id, stage `primary`/`overlap`/`retired`, age, source, how many secrets still use each master key, blockers); it never prints key material. The health check `key-rotation` reports `degraded` when a rotation was left unfinished (an old key still in `overlap` for a long time), a re-encryption stopped or failed, or a ring cannot be read.

1. `olcf security rotate master-key` asks for confirmation (`-y` skips it), makes a new key primary and re-encrypts every stored secret. The command follows the operation until it ends (`--wait`, default; `--timeout` seconds). It is resumable: if the controller restarts or the command is interrupted, run `olcf security rotate master-key --action reencrypt`.
2. `olcf security keys` should show no secret on the old key. Then `olcf security rotate master-key --action drop-old` removes it; it is refused with the number of secrets still using it. Until then `--action rollback` (optionally `--to KEY_ID`) makes the previous key primary again.
3. Take a **new backup** afterwards: backups made before the rotation carry the old ring. A backup is encrypted with the newest *root* key, never a ring key, so restoring on a clean machine needs only the root key.
4. JWT signing key: `olcf security rotate jwt-signing-key` makes new tokens use a new key while the old key keeps verifying; after the access-token lifetime (default 30 minutes) run `--action drop-old`.
5. If the root key itself (`OLCF_MASTER_KEY`, the OS keyring entry or `secrets/master.key`) is replaced: add the new value at the end of `OLCF_MASTER_KEY` (whitespace- or comma-separated, newest last), restart the nodes one by one, run `olcf security rotate master-key --action rewrap`, then remove the old value.

Every node picks a new ring up within a few seconds (`OLCF_KEYRING_REFRESH_SECONDS`, default 5) without a restart. Not exercised: a rotation while several controllers on separate machines run against one PostgreSQL server (only processes on one machine were tested).

### Worker certificate and token renewal

`olcf worker get <worker>` and `GET /v1/workers` show `cert_expires_at` and `token_expires_at`; the health check `worker-certificates` and the alert rules `auth-expiry` and `worker-certificate-critical` warn before they pass. Agents renew on their own (setting `compute.worker_cert_auto_renew`, default on) when fewer than `compute.worker_cert_renew_before_days` (30) remain. `olcf worker renew <worker>` asks a connected worker to do it now; the old pair stays valid until the node connects with the new one (at most `compute.worker_token_grace_seconds`, 24 hours). A worker that is offline cannot renew; if its certificate or token expired, revoke it and enrol it again. Worker tokens do not expire unless `compute.worker_token_max_age_days` is greater than zero.

## Air-gapped sites

Everything that normally downloads has an offline route:

* OLCF itself: build an offline bundle (`scripts/make_offline_bundle.sh`) and install with `install.sh --offline-bundle` ([deploy README](../../deploy/README.md#5-air-gapped-install-offline-bundle)).
* Runtimes: on a connected machine `olcf runtime export-bundle olcf-rt-2.0.0 -o rt-2.0.0.tgz`; on the target `olcf runtime import-bundle rt-2.0.0.tgz`. The import re-verifies the signature and every checksum. You can also install from a private index or wheelhouse (`--index-url`, `--find-links`).
* Environments: set `security.allow_public_package_install=false` and `security.allowed_package_indexes` to your mirror, and list wheelhouses in `environments.wheelhouses`.
* Updates: import a signed offline bundle (`POST /v1/admin/updates/import`).
* Fabric notebooks that install packages need their own route to the pip index; the Fabric side is outside OLCF.
