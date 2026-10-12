# Hardening

Steps for turning a trial installation into one you can expose to a team or a network. The reasoning behind each control is in the [security model](../concepts/security-model.md). The matching checklist is in [production checklist](production-checklist.md).

## 1. Network exposure

* The server binds to `127.0.0.1` by default. Expose it only through TLS, either built in (`tls.enabled = true`) or at a reverse proxy; set `server.host` and `server.public_url` accordingly. If a proxy sets `X-Forwarded-*`, set `server.trusted_proxies` to the number of proxy hops to trust (default 0: not trusted).
* Allow only the ports you need ([ports and networking](../reference/ports-and-networking.md)). Keep the Spark ports (7077, 18080+, 41000-41199 and the ephemeral executor ports) on a private network between cluster nodes. OLCF does not turn on Spark RPC authentication or encryption.
* Keep `server.cors_origins` empty unless a browser application on another origin must call the API.
* Run the control plane under a dedicated unprivileged account. `deploy/systemd/olcf.service` is a hardened unit (no new privileges, read-only system, private `/tmp`, empty capability set, system-call filter, only `/var/lib/olcf` writable); the Docker images run as uid 10001 with a read-only root file system and all capabilities dropped; the Helm chart satisfies the restricted Pod Security Standard.

## 2. TLS and worker identity

* Use your own certificate for the public name (`tls.cert_file`, `tls.key_file`), or the built-in CA with every name in `server.public_url` and `tls.extra_sans`. Distribute `<data dir>/pki/ca.crt` to clients (`--ca-bundle`).
* Turn on `tls.require_client_cert_for_workers` so that a worker's token alone is not enough: the WebSocket must present the client certificate issued at enrollment, whose common name must match the worker id ([TLS and mTLS](../guides/tls-and-mtls.md)).
* Protect `pki/ca.key`: anyone holding it can mint worker identities. It is in the data directory with mode 0600; back it up only with a passphrase-protected backup.

## 3. Identity

* Use OIDC or Entra ID for people ([SSO and identity](../guides/sso-and-identity.md)); map IdP groups to roles; keep local accounts for break-glass only. When SSO works, set `auth.local_login_enabled = false` and check that diagnostics reports `lockout-guard` OK, which confirms a non-local PlatformAdmin can still sign in.
* Change the bootstrap administrator's password, store it, and delete `secrets/bootstrap-admin.txt` (a password you supply through `OLCF_ADMIN_PASSWORD`, `OLCF_BOOTSTRAP_ADMIN_PASSWORD` or `--admin-password-stdin` is never printed or written to that file). Create named administrators; do not share the `admin` account.
* Grant least privilege: Viewer or Contributor at workspace scope for most people; WorkspaceAdmin for owners; PlatformAdmin for very few. Use `ServicePrincipal` accounts with `read` or `write` scoped tokens for automation, with a short lifetime. A SecurityAdmin can manage users but not create PlatformAdmins.
* Review `olcf token list --all` and role bindings regularly. Tokens default to 90 days.

## 4. Secrets and keys

* Supply `OLCF_MASTER_KEY` and `OLCF_JWT_SIGNING_KEY` from a secret manager (or `*_FILE` variants in the container images) rather than leaving them as files on the data volume, and keep an offline copy of the master key. Consider the Vault provider (`security.secrets_provider = "vault"`, `OLCF_VAULT_ADDR`, `OLCF_VAULT_TOKEN`) when you already run Vault.
* Create credentials for storage as connections with secrets; never put them in notebooks or item definitions. Use short-lived or narrowly scoped cloud credentials.
* There is no operator action for rotating the master key or JWT signing key in this build; plan for the manual procedure in the security model.

## 5. Code execution

User code runs with the rights of the worker's operating-system user.

* Run `olcf-worker` as a dedicated account with no access to other secrets, on machines dedicated to compute, with data directories on separate volumes.
* Configure `sandbox_command` (`--sandbox-command`) with a wrapper that fits your platform, for example bubblewrap or a container runner, with the job work directory writable and the network between driver, executors and master open. Jobs fail if the wrapper is configured and missing. Test it with `olcf compute smoke` and a real workload; OLCF ships no profile.
* Restrict who may submit code: `job.submit` and `session.use` are in Contributor and above; give Viewers and Operators only what they need.
* Approve package sources: set `security.allow_public_package_install = false` and list `security.allowed_package_indexes`; keep `environments.pip_policy_max` at `declared` or `block` so notebooks cannot install arbitrary packages at run time.
* `environments.allow_sdist` stays `false` so no native compilation happens on nodes.

## 6. Supply chain

* The runtime manifests shipped in the package are signed with a **development** key (id `8ea01f731883a91d`) whose private half is in the source tree, so anyone can forge a manifest signed with it. **A production installation does not trust that key.** It is trusted only when the installation's `environment` is not `production`, or when you opt in with `runtime.trust.allow_development_key = true`. The order of precedence is: the setting `runtime.trust.allow_development_key`, then the environment variable `OLCF_RUNTIME_ALLOW_DEV_KEY` (`0`/`1`), then the environment name (`environment` in `config.toml` or `OLCF_ENVIRONMENT`; **unset means `production`**). `olcf init` and `olcf quickstart` write `environment = "development"`; the Helm chart and the Compose file set `OLCF_ENVIRONMENT=production`.
* Consequence for production: the manifests shipped with OLCF are refused ("not trusted") until you sign them yourself. Do this once, offline, on a machine that holds the private key:
  1. Generate a key pair: `python -m olcf.runtime keygen --out /secure/olcf-keys` writes `olcf-signing.key` (private; keep it off the servers) and `olcf-signing.pub` into that directory (`--name` changes the base name).
  2. Create the trust directory the servers will read and add the public key: `python -m olcf.runtime --trust-dir /etc/olcf/trust trust add /secure/olcf-keys/olcf-signing.pub` (the `--trust-dir` option goes **before** `trust`). `trust list` shows the key ids, `trust remove KEY_ID` removes one. A trust directory **replaces** the built-in store, so the development key is not trusted once you use one.
  3. Re-sign each manifest you want to install, one file at a time (`sign` takes exactly one manifest and rewrites its signature): `python -m olcf.runtime sign --key /secure/olcf-keys/olcf-signing.key path/to/manifest.json`. The shipped manifests are in `src/olcf/runtime/manifests/` of the source tree. Install or import your signed manifests (`olcf runtime install`, `olcf runtime import-bundle`).
  4. Point every process at the directory: `runtime.trust.dir` in `config.toml` or `OLCF_RUNTIME_TRUST_DIR` on the control plane (the environment variable wins), `OLCF_RUNTIME_TRUST_DIR` or `--runtime-trust-dir` (`runtime_trust_dir` in `agent.toml`) on every external worker agent. An agent with neither uses the built-in store and applies the same production default, so it refuses development-signed manifests too.
* Verify: `olcf health` (the check `runtime-trust`). `ok` means only your keys are trusted; `degraded` means the development key is trusted in a non-production environment, or no key is trusted at all (nothing signed can be installed); `down` means the development key is trusted **in a production environment**. `runtime-trust` is a **critical** check: a `down` result makes `GET /v1/health/ready` answer `503` (fixed body `{"status":"unavailable"}`, no reason), so a load balancer or Kubernetes readiness probe stops sending traffic to that node. Liveness (`GET /v1/health`) stays `200`, so the process is not restarted and you can still read the reason with `olcf health`, `olcf status` or `GET /v1/admin/health`. Fix it with `runtime.trust.allow_development_key = false` (or by removing `OLCF_RUNTIME_ALLOW_DEV_KEY=1`) and your own trust directory. `degraded` (including a production installation with no trust anchor at all) and development installations (`environment = "development"`, `olcf quickstart`) stay ready. It is in the [health checks](../reference/health-checks.md) reference.
* **Upgrade impact.** An existing data directory whose `config.toml` has no `environment` setting is production after the upgrade, so its development-signed runtime manifests are refused, with one message that says how to fix it (`olcf runtime install`, `olcf serve` and the server log). To keep a laptop or lab working add `environment = "development"` to `config.toml` (or set `OLCF_ENVIRONMENT=development`); do not do this on a server.
* Container images built with `BAKE_RUNTIME` install a runtime during the image build. The final image is always `production`; with no operator key the build trusts the development key for that single install in the throwaway build stage and the result is a development try-out that a production container refuses. For production put your public key and your re-signed manifest in a directory and build with `--build-arg RUNTIME_TRUST_DIR=<dir> --build-arg BAKE_RUNTIME=<manifest>.json`; the keys are copied into the image and exported as `OLCF_RUNTIME_TRUST_DIR` ([deploy README](../../deploy/README.md#2-docker-compose)). On Kubernetes mount the keys with `runtimeTrust.existingSecret` / `runtimeTrust.existingConfigMap`. Keep `compute.allow_unsigned_runtimes` at `false`.
* For updates, trust only your own release key (`/v1/admin/updates/trust`) and keep `threshold` at your policy (default 1).
* Installers and release files carry checksums and sigstore signatures ([deploy README](../../deploy/README.md#1-one-line-installer-macos-linux-windows)). Verify downloads.
* Mirror Maven and PyPI for restricted networks; `environments.maven_repositories` and the host allow-lists keep jars to approved sources.

## 7. Outbound requests

Leave `network.ssrf.block_private` on (`true`). Add the specific internal destinations that pipelines and webhooks must reach to `network.ssrf.allow`; add CIDR ranges that must never be reachable to `network.ssrf.deny`.

## 8. Audit, monitoring, backups

* Export the audit log to your SIEM (`olcf audit export`, JSONL or CEF) and verify the hash chain periodically (`olcf audit verify`).
* Alert on failed logins (`auth.login_failed`), refresh-token reuse (`auth.refresh_reuse_detected`), role changes, `data.destructive` and failed health checks ([observability](../guides/observability.md)). The login limiter is per node and in memory; there is no account lockout.
* Turn on scheduled backups with a passphrase, copy them off the host, and schedule restore tests ([backup and restore](../guides/backup-and-restore.md)).

## 9. Data

* Use the storage provider's encryption at rest, access policies and versioning for lakehouse data; OLCF does not encrypt data files.
* Keep `lakehouse.destructive` (vacuum, drop) with few people. The vacuum retention floor is 168 hours.
* Set sensitivity labels on items where you need to track them; labels are metadata, not enforcement.
