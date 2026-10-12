# Backup and restore

OLCF backs up the **control-plane state**: the database (workspaces, items, notebooks, connections, secrets as ciphertext, jobs and history, audit log, settings) and, if you ask, the key files. It does **not** back up lakehouse data, job logs and artifacts, or the Spark runtimes. Lakehouse data follows the storage provider's own strategy (S3 versioning and replication, ADLS or OneLake features, filesystem snapshots); runtimes can be reinstalled (`olcf runtime install`) or restored from an exported bundle.

## What a backup is

A backup is one file (`*.olcfbak`) that is encrypted and authenticated end to end:

* The database is read in a single consistent snapshot (PostgreSQL `REPEATABLE READ`; SQLite in WAL mode), so the audit chain, the secrets and the items that reference them agree with each other while the platform keeps serving requests. It is written as JSON lines through gzip, in constant memory whatever the database size, so the database engine does not matter: you can back up SQLite and restore into PostgreSQL.
* Encryption is AES-256-GCM in 1 MiB frames with a per-file key. A flipped bit, a missing, reordered or duplicated frame, a truncated file or a swapped header fails authentication.
* The key is derived either from an operator **passphrase** (scrypt, at least 12 characters; restorable on any machine) or, when no passphrase is given, from the platform **master key** (HKDF; unattended scheduled backups are encrypted too, but restoring needs the same master key).
* Every table and file has a SHA-256 in the manifest, so `verify` can prove the content without applying it.

Excluded on purpose: in-flight OIDC login handshakes and leader leases (restoring stale ones would be wrong). Secret values are backed up exactly as stored, as ciphertext wrapped by the master key; Vault-backed secrets are only references.

### What you need to rebuild a lost installation

1. The backup file and its passphrase (or, for a master-key backup, the master key).
2. The master key, because the secret rows can only be decrypted with it. Either include it in the backup (a passphrase is then mandatory: key material is never written without one) or keep your own offline copy of `<data dir>/secrets/master.key`.
3. The CA and signing keys if workers are to keep trusting the rebuilt control plane. Without them, issue new enrollment tokens and re-enroll the workers.
4. An empty database of the same or a newer OLCF version.

Losing the master key makes stored secrets and master-key backups unreadable. Keep an offline copy.

## From the command line (works with the server stopped)

```bash
echo "a long passphrase here" | olcf backup create --passphrase-stdin --include-keys --out nightly.olcfbak
echo "a long passphrase here" | olcf backup verify --passphrase-stdin nightly.olcfbak
olcf backup list                       # backups in <data dir>/backups
olcf stop
echo "a long passphrase here" | olcf restore --passphrase-stdin --yes --overwrite-files nightly.olcfbak
olcf serve
```

`olcf backup create` without `--out` writes to `<data dir>/backups/`. `--include-keys` requires a passphrase. The passphrase can also come from `OLCF_BACKUP_PASSPHRASE` or an interactive prompt. `olcf restore` replaces the control-plane database of the stopped local installation and asks for confirmation unless `--yes` is given. For a PostgreSQL target, or to restore into an empty database from a script:

```bash
OLCF_BP='a long passphrase here' python -m olcf.ops.backup restore --passphrase-env OLCF_BP \
  --db-url postgresql+psycopg://olcf:...@db/olcf --data-dir /var/lib/olcf --wipe nightly.olcfbak
```

> **Restoring onto a new machine.** Existing files in the data directory are kept unless you pass `--overwrite-files`, and `olcf restore` creates a fresh `secrets/master.key` before it starts. On a new host, restore with `--overwrite-files` so the master key and the CA from the backup replace the new ones; without it the command prints a warning ("secrets/master.key already exists and was not overwritten") and the restored secrets cannot be decrypted. A backup made with `--include-keys` carries `config.toml`, `secrets/master.key`, `secrets/jwt-signing.key` and `pki/*`, so workers keep trusting the restored control plane. A backup without keys needs your own copy of `master.key`.

## Restoring into a clean environment

A backup is a snapshot of the control-plane database. The processes of its queued and running jobs live on worker machines and are not in the snapshot, so after a restore into a new environment no worker is connected and the restored job rows would still say `running`. Since the review remediation a restore **reconciles** them (`olcf.ops.restore_reconcile`):

* every job that was `accepted`, `validating`, `queued`, `provisioning`, `running` or `cancel_requested` at backup time and has no live worker attempt becomes `failed` with the failure category **`restore_reconcile`** and the remediation `olcf job retry <id>`; its open attempts are closed, its job credential is revoked and its timeline gets the state change and a `restore_reconcile` event. Finished jobs are never touched. The step is idempotent;
* jobs run by a remote provider (Kubernetes, Livy) whose submission was already sent are left to that provider's monitor (`left_to_provider` in the report);
* the report (`jobs` in the result of `olcf restore --json`, the restore operation and the restore test) lists the first 200 affected jobs by the state they were in. The offline `olcf restore` prints a warning and the first ten; an online restore (`POST /v1/admin/backup/{id}/restore`) treats the workers connected to that controller as live; an update rollback does not reconcile (the same fleet reconnects and reconciles itself);
* **interactive sessions and kernels are not part of the reconciliation** (not verified: stale session rows may remain until the session sweep marks them dead).

The restore test (`POST /v1/admin/backup/{id}/restore-test`, UI or `c.backups.restore_test`) restores into a throw-away database and reports `jobs` and a check `job-reconcile` (ok when nothing unfinished remains), so it tells you in advance how many jobs will not survive. Checklist:

1. On the source: `olcf backup create --include-keys --passphrase-stdin` (or `OLCF_BACKUP_PASSPHRASE`), or a backup without keys plus your own copy of the root key (`OLCF_MASTER_KEY`); keep the file off the machine. A restore test of that backup must be `ok`; its `job-reconcile` check shows how many jobs will be failed. If you rotated keys, take a new backup first (the ring is part of the database snapshot; the file is encrypted with the newest root key).
2. On the target: install the same or a newer OLCF and run `olcf init`, but **do not start the server**. Make the root key available (environment, OS keyring or `secrets/master.key`); with `--include-keys` the key files come from the backup (`--overwrite-files`).
3. `olcf restore FILE --passphrase-stdin --yes [--overwrite-files]`. Read the `jobs` section: those jobs are `failed`; resubmit what still matters with `olcf job retry <id>`. Job source files live in the data directory and are not in the backup.
4. `olcf serve`; then `olcf health` (the `key-rotation`, `backup` and `worker-certificates` checks) and `olcf audit verify`.
5. Workers: enrolled workers reconnect with their tokens and certificates if the PKI and token rows were restored (they are in the database); an agent that still holds a process of a job the restore failed is told it is an orphan and stops it. Workers whose credentials were revoked or lost: `olcf worker enroll-token` and enrol again.
6. `olcf compute smoke` proves the Spark path (needs a ready Spark runtime on an online worker); `olcf job list --state failed --all-workspaces` shows the reconciled jobs.

Not verified: a restore on a different host with real worker agents (the tests restore into a second data directory on the same machine), restore reconciliation against PostgreSQL (SQLite only), and what happens to sessions.

## From the control plane (UI, API)

In the UI: **Administer, Administration, Backup and restore**. This runs inside the server and adds scheduling, off-host targets, retention and restore tests.

* **Create** now: choose whether to include keys (needs a passphrase, which is stored encrypted and write-only for scheduled runs).
* **Schedule**: off by default; default time `17 2 * * *` once enabled. The leader runs it, so a multi-node deployment runs it once.
* **Retention**: keep the last 7 and the last 30 days by default; `POST /v1/admin/backup/retention` previews with `dry_run`.
* **Targets** receive every backup after it is written. Files are already encrypted, so a target needs no trust. Types: `local` (an absolute directory; paths below the data directory's `secrets` or `pki` are refused), `s3` (bucket, prefix, region; MinIO with `endpoint_url` and `path_style`; credentials from the secret store or the default AWS chain), `sftp` (host key fingerprint mandatory, no trust on first use; password or private key) and `lakehouse` (a lakehouse's `Files/` area, i.e. any storage provider). Test a target before relying on it.
* **Validate** decrypts and checks a backup. **Restore test** restores into a scratch location and verifies the result without touching the live installation; it can be scheduled (default `43 4 * * 0`, weekly, off by default). A backup that has never been restore-tested is a hope, not a backup.
* **Restore** over the live installation needs the exact confirmation phrase `RESTORE`, because it replaces the control-plane database. The `prerequisites` call reports what a backup needs (keys, passphrase, versions) and whether it is present.

```python
from olcf.sdk import Client
c = Client.from_context()
c.backups.start(include_keys=True, passphrase="a long passphrase here", wait=True)
latest = c.backups.list()["items"][0]
c.backups.restore_test(latest["id"], passphrase="a long passphrase here", wait=True)
```

API: `POST /v1/admin/backup`, `GET /v1/admin/backup`, `.../{id}/validate`, `.../{id}/restore-test`, `.../{id}/restore`, `.../{id}/download`, `POST /v1/admin/backup/import` and the target and configuration endpoints, all requiring `admin.backup`.

## Practice

* Back up before every upgrade (`olcf upgrade` takes a database backup itself) and on a schedule.
* Keep at least one copy off the machine and test it. Restore into a throw-away data directory regularly; this is what the restore test automates.
* Store the passphrase and an offline copy of `master.key` separately from the backup files.
* Back up your lakehouse storage with your storage provider's tools; a restored control plane only references that data.

## Upgrades and rollback

Upgrades are covered in the [runbook](../operations/runbook.md#updates). `olcf db status`, `olcf db migrate` and `olcf db rollback` manage the schema of a local installation.
