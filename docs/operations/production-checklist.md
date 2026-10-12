# Production checklist

Work through this before putting OLCF in front of a team. Each line says how to check it. Details are in [hardening](hardening.md).

## Deployment

- [ ] PostgreSQL if you want replicas, several API processes or a managed database: `olcf db status` shows the migrations applied.
- [ ] TLS on, with a certificate your clients trust: `olcf --ca-bundle ... status` works over https; the response carries `Strict-Transport-Security`.
- [ ] `server.public_url` is the external address: `olcf worker enroll-token` prints enrolment commands with that URL.
- [ ] A service manager starts the server at boot (systemd unit, Compose with a restart policy, Helm) and restarts it on failure.
- [ ] Workers enrolled from the machines you intend to use; `olcf worker test <worker>` passes on each; `olcf compute smoke` exits 0 (it needs a ready Spark runtime on an online worker; exit 6 means none is, exit 10 that the test ran and failed, exit 9 that the command stopped waiting).
- [ ] The runtimes you need are installed and verified on those workers: `olcf runtime verify <id> --deep --remote`.
- [ ] Time is synchronised on all nodes and the database host (leader leases and token expiry depend on it); diagnostics reports the skew.

## Identity and access

- [ ] SSO configured and tested with a non-admin user; role mapping reviewed ([SSO](../guides/sso-and-identity.md)).
- [ ] At least two named PlatformAdmins; `lockout-guard` health check OK; local login disabled or restricted to break-glass.
- [ ] Bootstrap password changed and `secrets/bootstrap-admin.txt` removed.
- [ ] Service principals use scoped, expiring tokens; no personal tokens in shared scripts.

## Secrets and trust

- [ ] `OLCF_MASTER_KEY` and `OLCF_JWT_SIGNING_KEY` come from a secret manager; an offline copy of the master key exists and was tested by decrypting a restore.
- [ ] `environment` is `production` (it is when `config.toml` and `OLCF_ENVIRONMENT` say nothing) and the development runtime key is not trusted: `olcf health` shows `runtime-trust` as `ok` (`degraded` or `down` means the development key is trusted or no key is; `down` also fails `GET /v1/health/ready` with 503, so the node receives no traffic).
- [ ] Runtime trust directory set to your own key on the control plane (`runtime.trust.dir` / `OLCF_RUNTIME_TRUST_DIR`) **and** on every external worker (`OLCF_RUNTIME_TRUST_DIR` or `--runtime-trust-dir`); `python -m olcf.runtime --trust-dir DIR trust list` lists only your key; the runtime manifests you install were re-signed with your key (the shipped ones are signed with the development key and are refused in production) ([hardening, section 6](hardening.md#6-supply-chain)).
- [ ] Key rotation rehearsed: `olcf security keys` lists the keys and `olcf security rotate master-key` / `jwt-signing-key` were run once in a test installation ([runbook](runbook.md#key-rotation)); the `key-rotation` health check is `ok`.
- [ ] Worker certificates are renewing: `worker-certificates` health check `ok`, and `olcf worker renew <worker>` works on one worker.
- [ ] Update trust store holds only your release keys.
- [ ] Package index policy set (`security.allowed_package_indexes`, `environments.pip_policy_max`).
- [ ] Worker mTLS on (`tls.require_client_cert_for_workers`) where workers are on a shared network.
- [ ] A sandbox command is configured on workers, or you have accepted that jobs run as the agent's user.

## Data and recovery

- [ ] Scheduled backups on, with passphrase, copied off host; a restore test has succeeded in the last month.
- [ ] You know how to restore: the [backup guide](../guides/backup-and-restore.md) steps were rehearsed, including the clean-environment checklist (restored jobs that were unfinished are marked `failed` with the category `restore_reconcile`; interactive sessions are not reconciled).
- [ ] Lakehouse storage has its own backup, versioning or replication.
- [ ] Retention settings reviewed for your disk and compliance needs.

## Observability

- [ ] Metrics scraped (`/v1/metrics`) or OTLP export configured; alerts for failed jobs, worker offline, disk low and failed backups.
- [ ] Audit log exported to your SIEM; `olcf audit verify` scheduled.
- [ ] `olcf doctor --server` has no unexplained WARN or FAIL.

## Compatibility with Microsoft Fabric (if used)

- [ ] The Fabric adapter configured (tenant, audience, workspace map); `olcf fabric test` passes ([Fabric integration](../guides/fabric-integration.md)).
- [ ] You have read [Fabric assumptions](../guides/fabric-assumptions.md) and the list of features that are fail-closed or unsupported.
